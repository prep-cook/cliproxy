use super::*;
use base64::Engine;

fn wire(records: &[Value]) -> String {
    records.iter().map(|record| format!("data: {record}\n\n")).collect()
}

#[test]
fn audio_fragments_transcript_and_buffered_extension_preserve_exact_payloads() {
    let native = json!({"id":"r","model":"m","status":"completed","output":[],"audio":{"data":"AAECAwQF","transcript":"hello é","mime_type":"audio/wav","expires_at":12}});
    let stream = wire(&[
        json!({"type":"response.created","response":{"id":"r","model":"m"}}),
        json!({"type":"response.audio.transcript.delta","delta":"hello "}),
        json!({"type":"response.audio.delta","delta":"AA"}),
        json!({"type":"response.audio.delta","delta":"EC"}),
        json!({"type":"response.audio.delta","delta":"AwQ="}),
        json!({"type":"response.audio.delta","delta":"BQ=="}),
        json!({"type":"response.audio.done"}),
        json!({"type":"response.audio.transcript.delta","delta":"é"}),
        json!({"type":"response.audio.transcript.done"}),
        json!({"type":"response.completed","response":{"id":"r","model":"m","status":"completed","output":[]}}),
    ]);
    for (input, upstream) in [
        (serde_json::to_string(&native).unwrap(), BodyMode::Buffered),
        (stream, BodyMode::Streaming),
    ] {
        for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
            let (output, canonical) = response(input.as_bytes(), upstream, downstream, Operation::Generate);
            let decoded: Vec<u8> = canonical
                .iter()
                .filter_map(|event| event["bytes"].as_str())
                .flat_map(|value| base64::engine::general_purpose::STANDARD.decode(value).unwrap())
                .collect();
            assert_eq!(decoded, [0, 1, 2, 3, 4, 5]);
            let transcript: String = canonical
                .iter()
                .filter_map(|event| event.get("final").and_then(|_| event["text"].as_str()))
                .collect();
            assert_eq!(transcript, "hello é");
            if downstream == BodyMode::Buffered {
                let output: Value = serde_json::from_slice(&output).unwrap();
                if upstream == BodyMode::Buffered {
                    assert_eq!(output["audio"], native["audio"]);
                } else {
                    let audio = output["x_ck_opaque"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|entry| entry["type"] == "response.audio")
                        .unwrap();
                    assert_eq!(audio["data"], "AAECAwQF");
                    assert_eq!(audio["transcript"], "hello é");
                    assert!(audio.get("mime_type").is_none());
                }
            } else {
                let records: Vec<Value> = std::str::from_utf8(&output)
                    .unwrap()
                    .lines()
                    .filter_map(|line| line.strip_prefix("data: "))
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect();
                let decoded: Vec<u8> = records
                    .iter()
                    .filter(|event| event["type"] == "response.audio.delta")
                    .flat_map(|event| {
                        base64::engine::general_purpose::STANDARD
                            .decode(event["delta"].as_str().unwrap())
                            .unwrap()
                    })
                    .collect();
                assert_eq!(decoded, [0, 1, 2, 3, 4, 5]);
                assert_eq!(
                    records
                        .iter()
                        .filter(|event| event["type"] == "response.audio.done")
                        .count(),
                    1
                );
                assert_eq!(
                    records
                        .iter()
                        .filter(|event| event["type"] == "response.audio.transcript.done")
                        .count(),
                    1
                );
            }
        }
    }
}

#[test]
fn unfinished_audio_and_post_done_data_cannot_complete_response() {
    for records in [
        vec![
            json!({"type":"response.audio.delta","delta":"AA"}),
            json!({"type":"response.audio.done"}),
        ],
        vec![json!({"type":"response.audio.delta","delta":"AAEC"})],
        vec![
            json!({"type":"response.audio.delta","delta":"AAEC"}),
            json!({"type":"response.audio.done"}),
            json!({"type":"response.audio.delta","delta":"AwQF"}),
        ],
        vec![json!({"type":"response.audio.transcript.delta","delta":"unfinished"})],
    ] {
        let mut records = records;
        records.push(
            json!({"type":"response.completed","response":{"id":"r","model":"m","status":"completed","output":[]}}),
        );
        let mut mapped = Mapped::new();
        let mut ingress = responses::response::Ingress::new(
            &mut mapped.writer,
            identity(Direction::Response),
            Operation::Generate,
            BodyMode::Streaming,
            100_000,
        )
        .unwrap();
        let result = ingress
            .advance(&mut mapped.writer, wire(&records).as_bytes())
            .and_then(|()| ingress.finish(&mut mapped.writer));
        assert!(result.is_err());
        let output = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
        assert!(output.contains("event: x_ck.error\n"));
        assert!(!output.contains("event: x_ck.done\n"));
    }
}

#[test]
fn native_audio_request_reaches_typed_media_and_preserves_format() {
    let input = json!({"model":"m","input":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"AAECAwQF","format":"wav","vendor":null},"extra":7}]}]});
    let (output, canonical) = request(&input, Operation::Generate);
    assert_eq!(output["input"][0]["content"], input["input"][0]["content"]);
    assert!(canonical.iter().any(|event| event["kind"] == "audio"));
    assert!(
        canonical
            .iter()
            .any(|event| event["pointer"] == "/messages/0/content/0/type" && event["value"] == "audio")
    );
    let mut mapped = Mapped::new();
    let mut ingress = responses::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        200_000,
    )
    .unwrap();
    ingress
        .advance(
            &mut mapped.writer,
            br#"{"model":"m","input":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"AAEC"#,
        )
        .unwrap();
    assert!(events(&mapped).iter().any(|event| event["bytes"] == "AAEC"));
    ingress
        .advance(&mut mapped.writer, br#"AwQF","format":"wav"}}]}]}"#)
        .unwrap();
    let ready = ingress
        .finish(&mut mapped.writer, &responses::request::ResponsesCapability)
        .unwrap();
    let output = responses::request::Egress {
        destination: "responses",
        upstream_stream: false,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    let output: Value = serde_json::from_slice(&bytes(&output)).unwrap();
    assert_eq!(output["input"][0]["content"][0]["input_audio"]["data"], "AAECAwQF");
}

#[test]
fn transcript_only_buffered_audio_does_not_invent_data() {
    let input = json!({"id":"r","status":"completed","output":[],"audio":{"transcript":"spoken","timing":{"start":1,"end":2,"unit":"seconds"}}});
    let (out, canonical) = response(
        &serde_json::to_vec(&input).unwrap(),
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Generate,
    );
    let out: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(out["audio"], input["audio"]);
    assert!(
        canonical
            .iter()
            .any(|event| event["timing"] == input["audio"]["timing"])
    );
}
