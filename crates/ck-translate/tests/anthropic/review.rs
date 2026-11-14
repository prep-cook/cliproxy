use crate::{
    BodyMode, Direction, Mapped, Operation, RequestEgress, RequestIngress, ResponseEgress, ResponseIngress, Value,
    anthropic, bytes, codec, events, identity, json, response,
};
use std::task::Poll;

#[test]
fn chat_tool_completion_closes_synthetic_native_blocks() {
    for mode in [BodyMode::Buffered, BodyMode::Streaming] {
        let mut mapped = Mapped::new();
        let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
        let mut ingress = codec::chat::response::Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Response),
            Operation::Generate,
            BodyMode::Buffered,
            1024 * 1024,
        )
        .unwrap();
        let mut egress = anthropic::egress::Egress::new(
            &mut mapped.writer,
            identity(Direction::Response),
            Operation::Generate,
            mode,
            |_| {},
        )
        .unwrap();
        ingress.advance(&mut mapped.writer, br#"{"id":"r","model":"m","choices":[{"index":0,"message":{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"f","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}"#).unwrap();
        ingress.finish(&mut mapped.writer).unwrap();
        let mut output = Vec::new();
        while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
            egress
                .advance(&mut mapped.writer, lease, |part| {
                    output.extend_from_slice(&bytes(&part));
                    Ok(())
                })
                .unwrap();
        }
        egress.finish().unwrap();
        if mode == BodyMode::Buffered {
            let output: Value = serde_json::from_slice(&output).unwrap();
            assert_eq!(
                output["content"],
                json!([{"type":"tool_use","id":"call_1","name":"f","input":{}}])
            );
        } else {
            let rows = events(&output);
            assert_eq!(rows.iter().filter(|row| row["type"] == "content_block_stop").count(), 1);
            assert_eq!(rows.last().unwrap()["type"], "message_stop");
            assert!(rows.iter().any(|row| row["content_block"]["id"] == "call_1"));
        }
    }
}

#[test]
fn native_unknown_payload_names_never_become_semantic_text_or_signatures() {
    let body = json!({"id":"r","model":"m","type":"message","role":"assistant","content":[{"type":"future_block","text":"opaque-only"},{"type":"text","text":"visible","thinking":"unknown-thought","signature":"unknown-signature"}],"stop_reason":"end_turn","stop_sequence":null});
    let raw = serde_json::to_vec(&body).unwrap();
    let (output, canonical) = response(
        &raw,
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Generate,
        raw.len(),
    );
    assert_eq!(serde_json::from_slice::<Value>(&output).unwrap(), body);
    assert!(
        canonical
            .iter()
            .all(|row| row["choices"][0]["delta"]["content"] != "opaque-only")
    );
    assert!(
        canonical
            .iter()
            .all(|row| row["choices"][0]["delta"]["reasoning_content"] != "unknown-thought")
    );
    assert!(canonical.iter().all(|row| row["fragment"] != "unknown-signature"));
}

#[test]
fn chat_stop_is_an_array_and_foreign_native_thinking_cannot_cross_fence() {
    for (extra, accepted) in [
        (json!({"stop":"END"}), true),
        (json!({"stop":["A","B"]}), true),
        (json!({"reasoning":{"effort":"high"}}), false),
    ] {
        let mut body = json!({"model":"m","max_tokens":16,"messages":[{"role":"user","content":"x"}]});
        body.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        let mut mapped = Mapped::new();
        let mut ingress = codec::chat::request::Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Request),
            Operation::Generate,
            1024 * 1024,
        )
        .unwrap();
        ingress
            .advance(&mut mapped.writer, &serde_json::to_vec(&body).unwrap())
            .unwrap();
        let result = ingress.finish(&mut mapped.writer, &anthropic::request::AnthropicCapability);
        if accepted {
            let ready = result.unwrap();
            let wire = anthropic::request::Egress {
                destination: "anthropic",
                upstream_stream: false,
            }
            .encode(&ready, &mut mapped.writer)
            .unwrap();
            let value: Value = serde_json::from_slice(&bytes(&wire)).unwrap();
            assert_eq!(
                value["stop_sequences"],
                if extra["stop"].is_array() {
                    extra["stop"].clone()
                } else {
                    json!(["END"])
                }
            );
        } else {
            assert!(matches!(result, Err(codec::Error::Unsupported)));
            assert!(
                !std::str::from_utf8(&mapped.output(&mapped.writer))
                    .unwrap()
                    .contains("event: x_ck.request.end\n")
            );
        }
    }
}

#[test]
fn native_delta_provenance_and_unknown_fields_survive_without_text_duplication() {
    let input = b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"r\",\"model\":\"m\"}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":7,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nid: native-7\nretry: 2000\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":7,\"outer\":false,\"delta\":{\"type\":\"text_delta\",\"text\":\"x\",\"vendor\":7}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":7}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
    let (output, canonical) = response(input, BodyMode::Streaming, BodyMode::Streaming, Operation::Generate, 1);
    let chunks: Vec<_> = canonical
        .iter()
        .filter(|row| row["choices"][0]["delta"]["content"] == "x")
        .collect();
    assert_eq!(chunks.len(), 1);
    let row = chunks[0];
    assert!(
        row["x_ck"]["source"]["sse"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field["name"] == "id" && field["value"] == "native-7")
    );
    assert!(
        row["x_ck"]["original"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field["pointer"] == "/delta/vendor" && field["value"] == 7)
    );
    let rows = events(&output);
    let delta = rows.iter().find(|row| row["delta"]["text"] == "x").unwrap();
    assert_eq!(delta["delta"]["vendor"], 7);
    assert_eq!(delta["outer"], false);
    assert_eq!(delta["index"], 0);
    let wire = std::str::from_utf8(&output).unwrap();
    assert!(wire.contains("retry: 2000\n"));
    assert!(!wire.contains("id: native-7\n"));
}

#[test]
fn streaming_signatures_finish_after_the_last_exact_fragment() {
    let rows = crate::stream();
    let raw = crate::sse(&rows);
    let (_, canonical) = response(&raw, BodyMode::Streaming, BodyMode::Buffered, Operation::Generate, 1);
    let signatures: Vec<_> = canonical
        .iter()
        .filter(|row| row["kind"] == "anthropic" && row.get("fragment").is_some())
        .collect();
    assert_eq!(
        signatures
            .iter()
            .filter_map(|row| row["fragment"].as_str())
            .collect::<String>(),
        "signed/\nopaque=="
    );
    assert_eq!(signatures.last().unwrap()["final"], true);
    assert!(
        signatures[..signatures.len() - 1]
            .iter()
            .all(|row| row["final"] == false)
    );
}

#[test]
fn future_deltas_stay_opaque_and_follow_the_remapped_block() {
    let rows = vec![
        json!({"type":"message_start","message":{"id":"r","model":"m"}}),
        json!({"type":"content_block_start","index":7,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":7,"delta":{"type":"future_delta","text":"not-visible","nested":[null,7]}}),
        json!({"type":"content_block_stop","index":7}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
        json!({"type":"message_stop"}),
    ];
    let raw = crate::sse(&rows);
    let (output, canonical) = response(&raw, BodyMode::Streaming, BodyMode::Streaming, Operation::Generate, 1);
    assert!(
        canonical
            .iter()
            .any(|row| row["format"] == "json" && row["value"] == rows[2])
    );
    assert!(
        canonical
            .iter()
            .all(|row| row["choices"][0]["delta"]["content"] != "not-visible")
    );
    let native = events(&output);
    let delta = native
        .iter()
        .find(|row| row["delta"]["type"] == "future_delta")
        .unwrap();
    assert_eq!(delta["delta"], rows[2]["delta"]);
    assert_eq!(delta["index"], 0);
}
