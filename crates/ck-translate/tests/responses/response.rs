use super::*;

#[test]
fn compact_preserves_opaque_items_and_usage_presence() {
    let input = json!({"id":"c","object":"response.compaction","created_at":4,"model":"m","output":[{"id":"i","type":"compaction","encrypted_content":"secret"}],"usage":{"input_tokens":0,"output_tokens":null,"cache_write_tokens":0,"input_tokens_details":{"cache_write_tokens":9},"cache_write_by_ttl":{"300":7,"3600":9}}});
    let (out, events) = response(
        &serde_json::to_vec(&input).unwrap(),
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Compact,
    );
    let out: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(out["output"], input["output"]);
    assert_eq!(out["usage"], input["usage"]);
    let usage = events.iter().find(|e| e.get("usage").is_some()).unwrap();
    assert_eq!(usage["usage"]["input_tokens"], 0);
    assert!(usage["usage"].get("output_tokens").is_none());
    assert_eq!(usage["usage"]["cache_write_by_ttl"]["3600"], 9);
    assert_eq!(usage["usage"]["cache_write_tokens"], 0);
}

#[test]
fn buffered_response_decomposes_text_tools_and_encrypted_reasoning() {
    let input = json!({"id":"r","model":"m","created_at":42,"status":"completed","output":[{"id":"reason","type":"reasoning","summary":[{"type":"summary_text","text":"hmm"}],"encrypted_content":"opaque=="},{"id":"msg","type":"message","role":"assistant","content":[{"type":"output_text","text":"hello","logprobs":[{"token":"hello","logprob":-0.2}]}]},{"id":"tool","type":"function_call","call_id":"call","name":"f","arguments":"{\"x\":1}"}],"usage":{"input_tokens":3,"output_tokens":2}});
    for mode in [BodyMode::Buffered, BodyMode::Streaming] {
        let (out, events) = response(
            &serde_json::to_vec(&input).unwrap(),
            BodyMode::Buffered,
            mode,
            Operation::Generate,
        );
        assert!(
            events
                .iter()
                .any(|e| e["kind"] == "responses_encrypted" && e["fragment"] == "opaque==")
        );
        let text: String = events
            .iter()
            .filter_map(|e| e["choices"][0]["delta"]["content"].as_str())
            .collect();
        assert_eq!(text, "hello");
        if mode == BodyMode::Buffered {
            let out: Value = serde_json::from_slice(&out).unwrap();
            assert_eq!(out["output"][0]["encrypted_content"], "opaque==");
            assert!(out["output"][0].get("content").is_none());
            assert_eq!(out["output"][1]["content"][0]["text"], "hello");
            assert_eq!(out["output"][2]["arguments"], "{\"x\":1}");
        } else {
            assert!(
                std::str::from_utf8(&out)
                    .unwrap()
                    .lines()
                    .filter_map(|line| line.strip_prefix("data: "))
                    .map(|line| serde_json::from_str::<Value>(line).unwrap())
                    .all(|event| event["type"] != "response.content_part.added" || event["item_id"] != "reason")
            );
            assert!(
                std::str::from_utf8(&out)
                    .unwrap()
                    .contains("response.output_text.delta")
            );
        }
    }
}

#[test]
fn streaming_snapshots_do_not_duplicate_text_or_arguments() {
    let records = [
        json!({"type":"response.created","response":{"id":"r","model":"m","created_at":2}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"i","type":"function_call","call_id":"c","name":"f","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":0,"item_id":"i","delta":"{\"x\":"}),
        json!({"type":"response.function_call_arguments.delta","output_index":0,"item_id":"i","delta":"1}"}),
        json!({"type":"response.function_call_arguments.done","output_index":0,"item_id":"i","arguments":"{\"x\":1}"}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"id":"i","type":"function_call","call_id":"c","name":"f","arguments":"{\"x\":1}","status":"completed"}}),
        json!({"type":"response.completed","response":{"id":"r","model":"m","created_at":2,"status":"completed","output":[{"id":"i","type":"function_call","call_id":"c","name":"f","arguments":"{\"x\":1}","status":"completed"}]}}),
    ];
    let mut wire = String::new();
    for record in records {
        wire.push_str(&format!("data: {record}\n\n"));
    }
    let (out, events) = response(
        wire.as_bytes(),
        BodyMode::Streaming,
        BodyMode::Buffered,
        Operation::Generate,
    );
    let out: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(out["output"][0]["arguments"], "{\"x\":1}");
    let args: String = events
        .iter()
        .filter_map(|e| e["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"].as_str())
        .collect();
    assert_eq!(args, "{\"x\":1}");
}

#[test]
fn malformed_truncated_and_contradictory_responses_never_complete() {
    let cases=[
        "data: \n\n".to_owned(),
        "data: {broken}\n\n".to_owned(),
        "event: response.output_text.delta\ndata: broken\n\n".to_owned(),
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"}}\n\n".to_owned(),
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"}}".to_owned(),
        [json!({"type":"response.output_item.added","output_index":0,"item":{"id":"i","type":"message","role":"assistant","content":[]}}),json!({"type":"response.output_text.delta","output_index":0,"item_id":"i","content_index":0,"delta":"prefix"}),json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[{"id":"i","type":"message","content":[{"type":"output_text","text":"changed"}]}]}})].iter().map(|v|format!("data: {v}\n\n")).collect(),
    ];
    for wire in cases {
        let mut m = Mapped::new();
        let mut ingress = responses::response::Ingress::new(
            &mut m.writer,
            identity(Direction::Response),
            Operation::Generate,
            BodyMode::Streaming,
            1024 * 1024,
        )
        .unwrap();
        let result = ingress
            .advance(&mut m.writer, wire.as_bytes())
            .and_then(|()| ingress.finish(&mut m.writer));
        assert!(result.is_err(), "{wire}");
        let emitted = String::from_utf8(m.output(&m.writer)).unwrap();
        assert!(emitted.contains("event: x_ck.error\n"));
        assert!(!emitted.contains("event: x_ck.done\n"));
        assert_invalid_input(emitted.as_bytes(), "upstream");
    }
}

#[test]
fn unknown_sse_fields_cannot_forge_canonical_records() {
    let input=b"data: first\ndata: second\nid: external\nevent: vendor.unknown\nretry: 12\n: comment\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n";
    let (_, events) = response(input, BodyMode::Streaming, BodyMode::Streaming, Operation::Generate);
    let unknown = events.iter().find(|e| e["format"] == "text").unwrap();
    assert_eq!(unknown["value"], "first\nsecond");
    assert_eq!(unknown["x_ck"]["source"]["event"], "vendor.unknown");
    assert!(
        unknown["x_ck"]["source"]["sse"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f["name"] == "id" && f["value"] == "external")
    );
}

#[test]
fn full_reasoning_and_summary_have_independent_block_identity() {
    let item = json!({"id":"rs","type":"reasoning","content":[{"type":"reasoning_text","text":"full"}],"summary":[{"type":"summary_text","text":"brief"}]});
    let body = json!({"id":"r","model":"m","created_at":1,"status":"completed","output":[item]});
    for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
        let (output, canonical) = response(
            &serde_json::to_vec(&body).unwrap(),
            BodyMode::Buffered,
            downstream,
            Operation::Generate,
        );
        let blocks: Vec<_> = canonical
            .iter()
            .filter(|event| event["kind"] == "thinking" || event["kind"] == "reasoning_summary")
            .collect();
        assert_eq!(blocks.len(), 2);
        assert_ne!(blocks[0]["x_ck"]["block_index"], blocks[1]["x_ck"]["block_index"]);
        if downstream == BodyMode::Buffered {
            let value: Value = serde_json::from_slice(&output).unwrap();
            assert_eq!(value["output"][0]["content"][0]["type"], "reasoning_text");
            assert_eq!(value["output"][0]["content"][0]["text"], "full");
            assert_eq!(value["output"][0]["summary"][0]["text"], "brief");
        } else {
            let text = std::str::from_utf8(&output).unwrap();
            assert!(text.contains("response.reasoning_text.delta"));
            assert!(text.contains("response.reasoning_summary_text.delta"));
            let summary = text
                .lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .find(|event| event["type"] == "response.reasoning_summary_text.delta")
                .unwrap();
            assert_eq!(summary["summary_index"], 0);
        }
    }
}

#[test]
fn native_extension_fields_use_noncontrol_carriers_not_incomplete_native_events() {
    let input=b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"}}\n\nid: source-id\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":41,\"output_index\":0,\"item\":{\"id\":\"i\",\"type\":\"message\"},\"vendor\":{\"x_ck\":{\"seq\":900}}}\n\ndata: {\"type\":\"response.completed\",\"sequence_number\":42,\"vendor\":null,\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[{\"id\":\"i\",\"type\":\"message\",\"content\":[]}]}}\n\n";
    let (output, _) = response(input, BodyMode::Streaming, BodyMode::Streaming, Operation::Generate);
    let events: Vec<Value> = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(
        events
            .iter()
            .filter(|event| event["type"] == "response.output_item.added")
            .all(|event| event["item"].is_object())
    );
    let carrier = events
        .iter()
        .find(|event| event["type"] == "x_ck_opaque" && event["source"]["sequence"] == 41)
        .unwrap();
    assert_eq!(carrier["value"]["vendor"]["x_ck"]["seq"], 900);
    assert_eq!(carrier["source"]["event"], "response.output_item.added");
    let terminal = events
        .iter()
        .find(|event| event["type"] == "x_ck_opaque" && event["source"]["sequence"] == 42)
        .unwrap();
    assert!(terminal["value"].get("vendor").unwrap().is_null());
    assert_eq!(events.last().unwrap()["type"], "response.completed");
}

#[test]
fn interleaved_items_and_content_keep_native_indexes_and_final_order() {
    let first = json!({"id":"first","type":"message","content":[{"type":"output_text","text":"a"},{"type":"output_text","text":"b"}]});
    let second = json!({"id":"second","type":"message","content":[]});
    let events = [
        json!({"type":"response.output_item.added","output_index":1,"item":{"id":"second","type":"message"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"first","type":"message"}}),
        json!({"type":"response.output_text.delta","output_index":0,"item_id":"first","content_index":1,"delta":"b"}),
        json!({"type":"response.output_text.delta","output_index":0,"item_id":"first","content_index":0,"delta":"a"}),
        json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[first,second]}}),
    ];
    let wire = events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect::<String>();
    for mode in [BodyMode::Buffered, BodyMode::Streaming] {
        let (output, _) = response(wire.as_bytes(), BodyMode::Streaming, mode, Operation::Generate);
        let body: Value = if mode == BodyMode::Buffered {
            serde_json::from_slice(&output).unwrap()
        } else {
            let events: Vec<Value> = std::str::from_utf8(&output)
                .unwrap()
                .lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            let starts: Vec<_> = events
                .iter()
                .filter(|event| event["type"] == "response.output_item.added")
                .map(|event| event["output_index"].as_u64().unwrap())
                .collect();
            assert_eq!(starts, [1, 0]);
            let b = events
                .iter()
                .find(|event| event["type"] == "response.output_text.delta" && event["delta"] == "b")
                .unwrap();
            assert_eq!(b["output_index"], 0);
            assert_eq!(b["content_index"], 1);
            events.last().unwrap()["response"].clone()
        };
        assert_eq!(body["output"][0]["id"], "first");
        assert_eq!(body["output"][1]["id"], "second");
        assert_eq!(body["output"][0]["content"], first["content"]);
    }
}

#[test]
fn legacy_redacted_thinking_is_not_reclassified_as_an_encryption_signature() {
    let carrier = "claude-redacted-thinking:opaque\n+/=";
    let body = json!({"id":"r","status":"completed","output":[{"id":"reason","type":"reasoning","encrypted_content":carrier,"summary":[]}]});
    for mode in [BodyMode::Buffered, BodyMode::Streaming] {
        let (output, canonical) = response(
            &serde_json::to_vec(&body).unwrap(),
            BodyMode::Buffered,
            mode,
            Operation::Generate,
        );
        assert!(canonical.iter().any(|event| event["data"] == "opaque\n+/="));
        assert!(!canonical.iter().any(|event| event["kind"] == "responses_encrypted"));
        let projected: Value = if mode == BodyMode::Buffered {
            serde_json::from_slice(&output).unwrap()
        } else {
            std::str::from_utf8(&output)
                .unwrap()
                .lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .next_back()
                .unwrap()["response"]
                .clone()
        };
        assert_eq!(projected["output"][0]["encrypted_content"], carrier);
        assert!(projected["output"][0].get("content").is_none());
    }
}
