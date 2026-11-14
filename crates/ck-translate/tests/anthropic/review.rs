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
        (
            json!({"messages":[{"role":"system","content":"first"},{"role":"system","content":"second"},{"role":"user","content":"x"}]}),
            false,
        ),
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

#[test]
fn lifecycle_records_restore_native_metadata_at_its_original_level() {
    let mut rows = vec![
        json!({"type":"message_start","message":{"id":"r","model":"m"}}),
        json!({"type":"content_block_start","index":7,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":7,"delta":{"type":"text_delta","text":"x"}}),
        json!({"type":"content_block_stop","index":7}),
        json!({"type":"message_delta","delta":{"stop_reason":null,"progress":0},"usage":{"output_tokens":1}}),
        json!({"type":"message_delta","delta":{},"usage":{"output_tokens":2}}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null,"future_finish":9}}),
        json!({"type":"message_delta","delta":{},"usage":{"output_tokens":3}}),
        json!({"type":"message_stop"}),
    ];
    let mut input = String::new();
    for (index, row) in rows.iter_mut().enumerate() {
        row["vendor"] = json!(index);
        input.push_str(&format!(
            "retry: 2000\nevent: {}\ndata: {}\n\n",
            row["type"].as_str().unwrap(),
            row
        ));
    }
    let (output, canonical) = response(
        input.as_bytes(),
        BodyMode::Streaming,
        BodyMode::Streaming,
        Operation::Generate,
        1,
    );
    let output_rows = events(&output);
    for source in &rows {
        let native = output_rows
            .iter()
            .find(|row| row["vendor"] == source["vendor"])
            .unwrap();
        assert_eq!(native["vendor"], source["vendor"]);
        if source["type"] == "message_delta" {
            assert_eq!(native["delta"], source["delta"]);
        }
        assert!(
            canonical
                .iter()
                .any(|row| row["x_ck"]["original"].as_array().is_some_and(|entries| entries
                    .iter()
                    .any(|entry| entry["pointer"] == "/vendor" && entry["value"] == source["vendor"])))
        );
    }
    let restored_order: Vec<_> = output_rows
        .iter()
        .filter_map(|row| row.get("vendor").cloned())
        .collect();
    assert_eq!(
        restored_order,
        rows.iter().map(|row| row["vendor"].clone()).collect::<Vec<_>>()
    );
    let finish = output_rows
        .iter()
        .find(|row| row["delta"]["stop_reason"] == "end_turn")
        .unwrap();
    assert_eq!(finish["delta"]["future_finish"], 9);
    assert_eq!(finish["delta"].get("stop_sequence"), Some(&Value::Null));
    assert_eq!(
        std::str::from_utf8(&output).unwrap().matches("retry: 2000\n").count(),
        rows.len()
    );
}

#[test]
fn buffered_conversion_reports_only_unrepresentable_native_metadata() {
    static LOSSES: std::sync::Mutex<Vec<codec::event::Kind>> = std::sync::Mutex::new(Vec::new());
    fn record(diagnostic: anthropic::egress::Diagnostic) {
        LOSSES.lock().unwrap().push(diagnostic.kind);
    }
    let mut rows = vec![
        json!({"type":"message_start","message":{"id":"r","model":"m"},"vendor":1}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"x","vendor":7}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn","future_finish":9}}),
        json!({"type":"message_stop"}),
    ];
    LOSSES.lock().unwrap().clear();
    let input = super::sse(&rows);
    let (output, _) = super::response_with_diagnostic(
        &input,
        BodyMode::Streaming,
        BodyMode::Buffered,
        Operation::Generate,
        1,
        record,
    );
    let body: Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(body["content"][0]["text"], "x");
    assert_eq!(body["future_finish"], 9);
    assert_eq!(
        *LOSSES.lock().unwrap(),
        [codec::event::Kind::Chunk, codec::event::Kind::Chunk]
    );

    rows[0].as_object_mut().unwrap().remove("vendor");
    rows[2]["delta"].as_object_mut().unwrap().remove("vendor");
    LOSSES.lock().unwrap().clear();
    let (output, _) = super::response_with_diagnostic(
        &super::sse(&rows),
        BodyMode::Streaming,
        BodyMode::Buffered,
        Operation::Generate,
        1,
        record,
    );
    assert_eq!(serde_json::from_slice::<Value>(&output).unwrap()["future_finish"], 9);
    assert!(LOSSES.lock().unwrap().is_empty());
}

#[test]
fn native_temperature_and_top_p_reach_chat_together() {
    let input = br#"{"model":"m","max_tokens":100,"temperature":0.1,"top_p":0.9,"messages":[{"role":"user","content":"hello"}]}"#;
    for upstream_stream in [false, true] {
        let mut mapped = Mapped::new();
        let mut ingress = anthropic::request::Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Request),
            Operation::Generate,
            4096,
        )
        .unwrap();
        for chunk in input.chunks(3) {
            ingress.advance(&mut mapped.writer, chunk).unwrap();
        }
        let ready = ingress
            .finish(&mut mapped.writer, &codec::chat::request::ChatCapability)
            .unwrap();
        let output = codec::chat::request::Egress {
            destination: "chat",
            upstream_stream,
        }
        .encode(&ready, &mut mapped.writer)
        .unwrap();
        let body: Value = serde_json::from_slice(&bytes(&output)).unwrap();
        assert_eq!(body["temperature"], 0.1);
        assert_eq!(body["top_p"], 0.9);
        assert_eq!(body["messages"], json!([{"role":"user","content":"hello"}]));
        assert_eq!(body["stream"], upstream_stream);
    }
}
