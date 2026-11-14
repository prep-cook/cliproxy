use super::{codec, response, response_fragmented};
use base64::Engine;
use serde_json::{Value, json};

fn messages(output: &[u8], mode: codec::BodyMode) -> Vec<Value> {
    if mode == codec::BodyMode::Buffered {
        return vec![serde_json::from_slice::<Value>(output).unwrap()["choices"][0]["message"].clone()];
    }
    std::str::from_utf8(output)
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter(|line| *line != "[DONE]")
        .flat_map(|line| {
            serde_json::from_str::<Value>(line).unwrap()["choices"]
                .as_array()
                .unwrap()
                .clone()
        })
        .filter_map(|choice| choice.get("delta").cloned())
        .collect()
}

fn audio_value(messages: &[Value]) -> Value {
    let mut result = serde_json::Map::new();
    let mut bytes = Vec::new();
    let mut has_data = false;
    for audio in messages
        .iter()
        .filter_map(|message| message.get("audio"))
        .filter_map(Value::as_object)
    {
        for (key, value) in audio {
            if key == "data" {
                has_data = true;
                bytes.extend(
                    base64::engine::general_purpose::STANDARD
                        .decode(value.as_str().unwrap())
                        .unwrap(),
                );
            } else if key == "transcript" && value.is_string() {
                let text = result.entry(key.clone()).or_insert_with(|| json!(""));
                *text = json!(format!("{}{}", text.as_str().unwrap_or(""), value.as_str().unwrap()));
            } else {
                result.insert(key.clone(), value.clone());
            }
        }
    }
    if has_data {
        result.insert(
            "data".into(),
            json!(base64::engine::general_purpose::STANDARD.encode(bytes)),
        );
    }
    Value::Object(result)
}

#[test]
fn native_audio_images_annotations_and_ordered_parts_survive_all_modes() {
    let image =
        json!({"type":"image_url","image_url":{"url":"data:image/png;base64,AAEC","detail":"high"},"vendor":{"x":2}});
    let message = json!({"role":"assistant","content":[{"type":"text","text":"before"},image.clone(),{"type":"text","text":"after"}],
        "images":[image],"audio":{"id":"audio-1","data":"AQIDBA==","transcript":"spoken","expires_at":1700000000,"format":"wav","timing":{"start":12}},
        "annotations":[{"type":"url_citation","url_citation":{"start_index":0,"end_index":3,"url":"https://example.invalid","title":"source"},"vendor":7}]});
    for upstream in [codec::BodyMode::Buffered, codec::BodyMode::Streaming] {
        let native = json!({"id":"r","model":"m","created":1,"choices":[{"index":0,if upstream == codec::BodyMode::Buffered { "message" } else { "delta" }:message,"finish_reason":"stop"}]});
        let input = if upstream == codec::BodyMode::Buffered {
            native.to_string()
        } else {
            format!("data: {native}\n\ndata: [DONE]\n\n")
        };
        for downstream in [codec::BodyMode::Buffered, codec::BodyMode::Streaming] {
            let (output, events) = response(input.as_bytes(), upstream, downstream);
            let projected = messages(&output, downstream);
            for field in ["content", "images", "annotations"] {
                let parts: Vec<_> = projected
                    .iter()
                    .filter_map(|part| part.get(field))
                    .flat_map(|array| array.as_array().unwrap().clone())
                    .collect();
                assert_eq!(
                    parts,
                    message[field].as_array().unwrap().clone(),
                    "{upstream:?}/{downstream:?}/{field}"
                );
            }
            assert_eq!(audio_value(&projected), message["audio"]);
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event["encoding"] == "base64" && event["x_ck"].get("media_id").is_some())
                    .count(),
                3
            );
            assert!(events.iter().any(|event| event["text"] == "spoken"
                && event["final"].is_boolean()
                && event["x_ck"].get("media_id").is_some()));
            assert!(
                events
                    .iter()
                    .any(|event| event["kind"] == "chat" && event["value"] == message["annotations"][0])
            );
        }
    }
}

#[test]
fn media_data_event_boundaries_preserve_exact_decoded_bytes() {
    let raw: Vec<u8> = (0..80_003).map(|index| (index % 251) as u8).collect();
    let encoded = base64::engine::general_purpose::STANDARD.encode(&raw);
    let native = json!({"id":"r","model":"m","created":1,"choices":[{"index":0,"message":{"role":"assistant","audio":{"id":"a","data":encoded,"transcript":"whole"}},"finish_reason":"stop"}]});
    let input = native.to_string();
    for downstream in [codec::BodyMode::Buffered, codec::BodyMode::Streaming] {
        let (output, events) =
            response_fragmented(input.as_bytes(), codec::BodyMode::Buffered, downstream, input.len());
        let projected = messages(&output, downstream);
        let audio = audio_value(&projected);
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(audio["data"].as_str().unwrap())
                .unwrap(),
            raw
        );
        let data: Vec<_> = events
            .iter()
            .filter(|event| event["bytes"].is_string() && event["x_ck"].get("media_id").is_some())
            .collect();
        assert!(data.len() > 1);
        let mut recovered = Vec::new();
        for (index, event) in data.iter().enumerate() {
            assert_eq!(event["index"], index);
            assert_eq!(event["offset"], recovered.len());
            assert!(format!("event: x_ck.data\nid: {}\ndata: {}\n\n", event["x_ck"]["seq"], event).len() <= 50_000);
            recovered.extend(
                base64::engine::general_purpose::STANDARD
                    .decode(event["bytes"].as_str().unwrap())
                    .unwrap(),
            );
        }
        assert_eq!(recovered, raw);
    }
}

#[test]
fn empty_structured_fields_remain_present() {
    let input = br#"{"id":"r","model":"m","created":1,"choices":[{"index":0,"message":{"role":"assistant","content":[],"images":[],"annotations":[]},"finish_reason":"stop"}]}"#;
    let (output, _) = response(input, codec::BodyMode::Buffered, codec::BodyMode::Buffered);
    let message = &messages(&output, codec::BodyMode::Buffered)[0];
    for field in ["content", "images", "annotations"] {
        assert_eq!(message[field], json!([]));
    }
}

#[test]
fn native_audio_fragments_reassemble_across_every_base64_boundary() {
    let encoded = "AQIDBA==";
    let mut pairs: Vec<_> = (1..encoded.len())
        .map(|split| (&encoded[..split], &encoded[split..]))
        .collect();
    pairs.push(("AQI=", "AwQ="));
    for (first, second) in pairs {
        let start = json!({"id":"r","model":"m","created":1,"choices":[{"index":0,"delta":{"audio":{"id":"audio-1","data":first,"transcript":"hel"}},"finish_reason":null}]});
        let end = json!({"id":"r","choices":[{"index":0,"delta":{"audio":{"data":second,"transcript":"lo","expires_at":99}},"finish_reason":"stop"}]});
        let wire = format!("data: {start}\n\ndata: {end}\n\ndata: [DONE]\n\n");
        for downstream in [codec::BodyMode::Buffered, codec::BodyMode::Streaming] {
            let (output, events) = response(wire.as_bytes(), codec::BodyMode::Streaming, downstream);
            let starts: Vec<_> = events.iter().filter(|event| event["encoding"] == "base64").collect();
            assert_eq!(starts.len(), 1, "{first}/{second}/{downstream:?}");
            let id = &starts[0]["x_ck"]["media_id"];
            let records: Vec<_> = events.iter().filter(|event| &event["x_ck"]["media_id"] == id).collect();
            let mut offset = 0;
            for (index, data) in records.iter().filter(|event| event["bytes"].is_string()).enumerate() {
                assert_eq!(data["index"], index);
                assert_eq!(data["offset"], offset);
                offset += base64::engine::general_purpose::STANDARD
                    .decode(data["bytes"].as_str().unwrap())
                    .unwrap()
                    .len();
            }
            assert_eq!(offset, 4);
            let ends: Vec<_> = records.iter().filter(|event| event["status"] == "completed").collect();
            assert_eq!(ends.len(), 1);
            assert_eq!(ends[0]["bytes"], 4);
            let messages = messages(&output, downstream);
            let mut bytes = Vec::new();
            let mut transcript = String::new();
            for audio in messages.iter().filter_map(|message| message.get("audio")) {
                if let Some(data) = audio["data"].as_str() {
                    bytes.extend(base64::engine::general_purpose::STANDARD.decode(data).unwrap());
                }
                if let Some(text) = audio["transcript"].as_str() {
                    transcript.push_str(text);
                }
            }
            assert_eq!(bytes, [1, 2, 3, 4], "{first}/{second}/{downstream:?}");
            assert_eq!(transcript, "hello");
        }
    }
}

#[test]
fn unfinished_native_audio_cannot_publish_successful_choice_finish() {
    use super::{Mapped, Operation, identity};
    use codec::{ResponseIngress, event::Direction};
    let mut mapped = Mapped::new();
    let mut ingress = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        codec::BodyMode::Streaming,
        1024 * 1024,
    )
    .unwrap();
    let wire = b"data: {\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{\"index\":0,\"delta\":{\"audio\":{\"data\":\"AQ\"}},\"finish_reason\":\"stop\"}]}\n\n";
    assert!(ingress.advance(&mut mapped.writer, wire).is_err());
    let events = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert!(events.contains("event: x_ck.error\n"));
    assert!(!events.contains("event: x_ck.item.end\n"));
    assert!(!events.contains("event: x_ck.done\n"));
}

#[test]
fn nullable_audio_transcript_stays_native_null() {
    let input = br#"{"id":"r","model":"m","created":1,"choices":[{"index":0,"message":{"audio":{"data":"AQ==","transcript":null}},"finish_reason":"stop"}]}"#;
    for downstream in [codec::BodyMode::Buffered, codec::BodyMode::Streaming] {
        let (output, events) = response(input, codec::BodyMode::Buffered, downstream);
        let messages = messages(&output, downstream);
        let audio = audio_value(&messages);
        assert_eq!(audio.get("transcript"), Some(&Value::Null));
        assert!(
            !events
                .iter()
                .any(|event| event.get("text").is_some() && event.get("final").is_some())
        );
    }
}

#[test]
fn image_part_with_unknown_file_member_keeps_its_image_carrier() {
    let url = format!("data:image/png;base64,{}", "AAEC".repeat(2048));
    let parts = [
        json!({"type":"image_url","image_url":{"url":url},"file":{"vendor":1}}),
        json!({"type":"image_url","file":{"vendor":1},"image_url":{"url":url}}),
    ];
    for part in parts {
        for upstream in [codec::BodyMode::Buffered, codec::BodyMode::Streaming] {
            let native = json!({"id":"r","model":"m","created":1,"choices":[{"index":0,if upstream == codec::BodyMode::Buffered { "message" } else { "delta" }:{"role":"assistant","content":[part.clone()]},"finish_reason":"stop"}]});
            let input = if upstream == codec::BodyMode::Buffered {
                native.to_string()
            } else {
                format!("data: {native}\n\ndata: [DONE]\n\n")
            };
            for downstream in [codec::BodyMode::Buffered, codec::BodyMode::Streaming] {
                let (output, events) = response_fragmented(input.as_bytes(), upstream, downstream, 4096);
                assert!(events.iter().any(|event| event["kind"] == "image"), "{upstream:?}");
                let parts: Vec<_> = messages(&output, downstream)
                    .iter()
                    .filter_map(|message| message.get("content"))
                    .flat_map(|array| array.as_array().unwrap().clone())
                    .collect();
                assert_eq!(parts, std::slice::from_ref(&part), "{upstream:?}/{downstream:?}");
            }
        }
    }
}
