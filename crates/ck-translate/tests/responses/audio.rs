use super::*;
use base64::Engine;

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
                assert_streamed_audio(&output);
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

/// Asserts that a streamed client response carries the audio bytes and one done event each
/// for the audio and its transcript.
/// Opens a buffered Responses ingress and sends `head` of its body.
fn buffered_ingress(m: &mut Mapped, head: &[u8]) -> responses::response::Ingress {
    let mut ingress = responses::response::Ingress::new(
        &mut m.writer,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Buffered,
        1024 * 1024,
    )
    .unwrap();
    ingress.advance(&mut m.writer, head).unwrap();
    ingress
}

/// Data events in order: each offset follows the previous chunk; returns the decoded total.
fn data_bytes(canonical: &[Value]) -> u64 {
    let mut offset = 0;
    for event in canonical.iter().filter(|e| e["bytes"].is_string()) {
        let encoded = event["bytes"].as_str().unwrap();
        assert!(encoded.len() <= 49_152, "chunk over the ceiling");
        assert_eq!(event["offset"], offset);
        offset += base64::engine::general_purpose::STANDARD.decode(encoded).unwrap().len() as u64;
    }
    offset
}

#[test]
fn buffered_audio_publishes_chunks_before_the_body_ends() {
    // 98,304 base64 bytes decode to 73,728 bytes: two full 36,864-byte chunks.
    let payload = "AAEC".repeat(24_576);
    let head = br#"{"id":"r","model":"m","created_at":1,"status":"completed","output":[],"audio":{"id":"a","data":""#;
    let tail = br#"","transcript":"hi","expires_at":12}}"#;
    let mut m = Mapped::new();
    let mut ingress = buffered_ingress(&mut m, head);
    for chunk in payload.as_bytes().chunks(8192) {
        ingress.advance(&mut m.writer, chunk).unwrap();
    }
    let early = events(&m);
    assert!(data_bytes(&early) > 0, "no media data before the body ends: {early:?}");
    assert!(!early.iter().any(|e| e["choices"][0]["finish_reason"].is_string()));
    ingress.advance(&mut m.writer, tail).unwrap();
    ingress.finish(&mut m.writer).unwrap();
    let canonical = events(&m);
    assert_eq!(canonical.iter().filter(|e| e["kind"] == "audio").count(), 1);
    assert_eq!(data_bytes(&canonical), 73_728);
    let end = canonical
        .iter()
        .find(|e| e["status"] == "completed" && e["bytes"].is_u64());
    assert_eq!(end.unwrap()["bytes"], 73_728);
    let mut reader = m.store.reader(m.writer.id(), 0).unwrap();
    let mut egress = responses::egress::Egress::new(
        &mut m.writer,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Buffered,
        |_, _| {},
    )
    .unwrap();
    let mut out = Vec::new();
    while let std::task::Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        egress
            .advance(&mut m.writer, lease, |chunk| {
                out.extend_from_slice(&bytes(&chunk));
                Ok(())
            })
            .unwrap();
    }
    egress.finish().unwrap();
    let out: Value = serde_json::from_slice(&out).unwrap();
    let expected = json!({"id":"a","data":payload,"transcript":"hi","expires_at":12});
    assert_eq!(out["audio"], expected);
    // A payload that ends inside a base64 quartet is refused when its string closes.
    let mut m = Mapped::new();
    let mut ingress = buffered_ingress(&mut m, head);
    ingress.advance(&mut m.writer, b"AAECAA").unwrap();
    let result = ingress
        .advance(&mut m.writer, tail)
        .and_then(|()| ingress.finish(&mut m.writer));
    assert!(result.is_err());
}

fn assert_streamed_audio(output: &[u8]) {
    let records: Vec<Value> = std::str::from_utf8(output)
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
