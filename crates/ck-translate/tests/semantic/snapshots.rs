use super::{Mapped, Operation, codec, identity, response_fragmented};
use codec::{BodyMode, ResponseIngress, event::Direction};
use serde_json::{Value, json};

fn stream(events: &[Value]) -> Vec<u8> {
    let mut wire = String::new();
    for event in events {
        wire.push_str(&format!("data: {event}\n\n"));
    }
    wire.push_str("data: [DONE]\n\n");
    wire.into_bytes()
}

fn deltas(events: &[Value], choice: u64, field: &str) -> String {
    events
        .iter()
        .flat_map(|e| e["choices"].as_array().into_iter().flatten())
        .filter(|c| c["index"] == choice)
        .filter_map(|c| c["delta"][field].as_str())
        .collect()
}

#[test]
fn snapshots_reconcile_equal_and_suffix_across_records_and_choices() {
    let wire = stream(&[
        json!({"choices":[{"index":9,"delta":{"role":"assistant","content":"aé","refusal":"n","reasoning_content":"r"}},{"index":2,"delta":{"content":"two"}}],"usage":{"prompt_tokens":2}}),
        json!({"choices":[{"index":9,"delta":{"content":"中"},"message":{"role":"assistant","content":"aé中!","refusal":"no","reasoning_content":"reason"}},{"index":2,"message":{"content":"two"}}]}),
        json!({"choices":[{"index":2,"message":{},"finish_reason":"stop"},{"index":9,"message":{"content":"aé中!","refusal":"no","reasoning_content":"reason"},"finish_reason":"stop"}]}),
    ]);
    for fragment in [1, wire.len()] {
        for mode in [BodyMode::Buffered, BodyMode::Streaming] {
            let (output, events) = response_fragmented(&wire, BodyMode::Streaming, mode, fragment);
            assert_eq!(deltas(&events, 9, "content"), "aé中!");
            assert_eq!(deltas(&events, 9, "refusal"), "no");
            assert_eq!(deltas(&events, 9, "reasoning_content"), "reason");
            assert_eq!(deltas(&events, 2, "content"), "two");
            assert_eq!(events.iter().filter(|e| e.get("usage").is_some()).count(), 2);
            if mode == BodyMode::Buffered {
                let output: Value = serde_json::from_slice(&output).unwrap();
                assert_eq!(output["choices"][0]["message"]["content"], "aé中!");
            }
        }
    }
}

#[test]
fn snapshots_match_sparse_tools_by_id_not_array_order() {
    let wire = stream(&[
        json!({"choices":[{"index":4,"delta":{"tool_calls":[{"index":8,"id":"a","type":"function","function":{"name":"f","arguments":"{\"x\":"}},{"index":2,"id":"b","type":"custom","custom":{"name":"g","input":"hi"}}]}},{"index":7,"delta":{"tool_calls":[{"index":8,"id":"other","type":"function","function":{"name":"h","arguments":"{}"}}]}}]}),
        json!({"choices":[{"index":4,"message":{"tool_calls":[{"id":"b","type":"custom","custom":{"name":"g","input":"hi!"}},{"id":"a","type":"function","function":{"name":"f","arguments":"{\"x\":1}"}}]},"finish_reason":"tool_calls"},{"index":7,"message":{"tool_calls":[{"id":"other","function":{"name":"h","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}),
    ]);
    for mode in [BodyMode::Buffered, BodyMode::Streaming] {
        let (output, events) = response_fragmented(&wire, BodyMode::Streaming, mode, wire.len());
        let arguments: String = events
            .iter()
            .flat_map(|e| e["choices"].as_array().into_iter().flatten())
            .filter(|c| c["index"] == 4)
            .flat_map(|c| c["delta"]["tool_calls"].as_array().into_iter().flatten())
            .filter(|t| t["index"] == 8)
            .filter_map(|t| t["function"]["arguments"].as_str())
            .collect();
        assert_eq!(arguments, "{\"x\":1}");
        if mode == BodyMode::Buffered {
            let output: Value = serde_json::from_slice(&output).unwrap();
            assert_eq!(
                output["choices"][0]["message"]["tool_calls"][1]["custom"]["input"],
                "hi!"
            );
        }
    }
}

fn reject(prefix: Value, snapshot: Value) {
    let mut mapped = Mapped::new();
    let mut ingress = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        1_000_000,
    )
    .unwrap();
    ingress
        .advance(&mut mapped.writer, format!("data: {prefix}\n\n").as_bytes())
        .unwrap();
    let before = mapped.output(&mapped.writer).len();
    assert!(
        ingress
            .advance(&mut mapped.writer, format!("data: {snapshot}\n\n").as_bytes())
            .is_err()
    );
    let bytes = mapped.output(&mapped.writer);
    let suffix = std::str::from_utf8(&bytes[before..]).unwrap();
    assert!(suffix.contains("event: x_ck.error\n"));
    assert!(!suffix.contains("event: x_ck.tool.end\n"));
    assert!(!suffix.contains("event: x_ck.item.end\n"));
    assert!(!suffix.contains("event: x_ck.done\n"));
    assert!(!suffix.contains("SHOULD_NOT_ESCAPE"));
}

#[test]
fn snapshots_reject_contradictions_before_any_suffix_or_close() {
    for content in [json!("ab"), json!("xbc"), Value::Null, json!("")] {
        reject(
            json!({"choices":[{"index":0,"delta":{"content":"abc"}}]}),
            json!({"choices":[{"index":3,"message":{"content":"SHOULD_NOT_ESCAPE"},"finish_reason":"stop"},{"index":0,"message":{"content":content},"finish_reason":"stop"}]}),
        );
    }
    for call in [
        json!({"index":5,"id":"changed","function":{"name":"f","arguments":"{}"}}),
        json!({"id":"a","function":{"name":"changed","arguments":"{}"}}),
        json!({"id":"a","function":{"arguments":"[]"}}),
        json!({"id":"a","function":{"arguments":null}}),
    ] {
        reject(
            json!({"choices":[{"index":0,"delta":{"content":"a","tool_calls":[{"index":5,"id":"a","function":{"name":"f","arguments":"{}"}}]}}]}),
            json!({"choices":[{"index":0,"message":{"content":"aSHOULD_NOT_ESCAPE","tool_calls":[call]},"finish_reason":"tool_calls"}]}),
        );
    }
}

#[test]
fn snapshots_compare_decoded_media_and_ordered_content() {
    let image = json!({"type":"image_url","image_url":{"url":"data:image/png;base64,AQIDBA=="},"vendor":null});
    let wire = stream(&[
        json!({"choices":[{"index":0,"delta":{"audio":{"id":"sound","data":"AQID","transcript":"he"},"content":[{"type":"text","text":"first"},image]}}]}),
        json!({"choices":[{"index":0,"delta":{"audio":{"data":"BA==","transcript":"llo"}}}]}),
        json!({"choices":[{"index":0,"message":{"audio":{"id":"sound","data":"AQIDBAU=","transcript":"hello!"},"content":[{"type":"text","text":"first"},image,{"type":"text","text":"last"}]}}]}),
        json!({"choices":[{"index":0,"message":{"audio":{"data":"AQIDBAU=","transcript":"hello!"}},"finish_reason":"stop"}]}),
    ]);
    for mode in [BodyMode::Buffered, BodyMode::Streaming] {
        let (output, _) = response_fragmented(&wire, BodyMode::Streaming, mode, wire.len());
        if mode == BodyMode::Buffered {
            let output: Value = serde_json::from_slice(&output).unwrap();
            assert_eq!(output["choices"][0]["message"]["audio"]["data"], "AQIDBAU=");
            assert_eq!(output["choices"][0]["message"]["audio"]["transcript"], "hello!");
            assert_eq!(
                output["choices"][0]["message"]["content"],
                json!([{"type":"text","text":"first"},image,{"type":"text","text":"last"}])
            );
        }
    }
    reject(
        json!({"choices":[{"index":0,"delta":{"audio":{"data":"AQID"}}}]}),
        json!({"choices":[{"index":0,"message":{"content":"SHOULD_NOT_ESCAPE","audio":{"data":"AQIE"}},"finish_reason":"stop"}]}),
    );
}

#[test]
fn snapshots_preserve_absence_null_empty_and_unknown_metadata() {
    let wire = stream(&[
        json!({"choices":[{"index":0,"delta":{"content":null,"refusal":"","vendor":{"x":1,"nested":[true,null]}}}]}),
        json!({"choices":[{"index":0,"message":{"content":"","refusal":"","tool_calls":[],"vendor":{"x":1,"nested":[true,null]}}}]}),
        json!({"choices":[{"index":0,"message":{},"finish_reason":"stop"}]}),
    ]);
    let (output, _) = response_fragmented(&wire, BodyMode::Streaming, BodyMode::Buffered, wire.len());
    let output: Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(
        output["choices"][0]["message"],
        json!({"content":"","refusal":"","tool_calls":[],"vendor":{"x":1,"nested":[true,null]}})
    );
}

#[test]
fn snapshots_complete_pending_audio_and_reject_incomplete_tool_json_atomically() {
    for prefix in ["A", "AQ", "AQI", "AQIDB", "AQIDBA", "AQIDBA="] {
        let wire = stream(&[
            json!({"choices":[{"index":0,"delta":{"audio":{"data":prefix}}}]}),
            json!({"choices":[{"index":0,"message":{"audio":{"data":"AQIDBA=="}},"finish_reason":"stop"}]}),
        ]);
        let (output, _) = response_fragmented(&wire, BodyMode::Streaming, BodyMode::Buffered, wire.len());
        let output: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(output["choices"][0]["message"]["audio"]["data"], "AQIDBA==", "{prefix}");
    }
    reject(
        json!({"choices":[{"index":0,"delta":{"audio":{"data":"AQ"}}}]}),
        json!({"choices":[{"index":0,"message":{"audio":{"data":"Ag=="},"content":"SHOULD_NOT_ESCAPE"},"finish_reason":"stop"}]}),
    );
    for arguments in ["{", "{\"x\":"] {
        reject(
            json!({"choices":[{"index":0,"delta":{"content":"a","tool_calls":[{"index":0,"id":"a","function":{"name":"f","arguments":"{"}}]}}]}),
            json!({"choices":[{"index":0,"message":{"content":"aSHOULD_NOT_ESCAPE","tool_calls":[{"id":"a","function":{"arguments":arguments}}]},"finish_reason":"tool_calls"}]}),
        );
    }
    reject(
        json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"a","function":{"name":"f","arguments":"{}"}}]}}]}),
        json!({"choices":[{"index":0,"message":{"content":"SHOULD_NOT_ESCAPE","tool_calls":[{"id":"a","custom":{"name":"f","input":"text"}}]},"finish_reason":"tool_calls"}]}),
    );
}

#[test]
fn snapshots_after_choice_finish_remain_errors() {
    reject(
        json!({"choices":[{"index":0,"delta":{"content":"abc"},"finish_reason":"stop"}]}),
        json!({"choices":[{"index":0,"message":{"content":"abc"},"finish_reason":"stop"}]}),
    );
}

#[test]
fn snapshots_reader_rejects_a_foreign_store() {
    let mut mapped = Mapped::new();
    let foreign = Mapped::new();
    assert!(
        codec::chat::response::Ingress::new(
            &mut mapped.writer,
            &foreign.store,
            identity(Direction::Response),
            Operation::Generate,
            BodyMode::Streaming,
            4096,
        )
        .is_err()
    );
    assert!(mapped.output(&mapped.writer).is_empty());
    assert!(foreign.output(&foreign.writer).is_empty());
}

#[test]
fn snapshots_validate_retained_children_of_every_finishing_choice() {
    reject(
        json!({"choices":[{"index":0,"delta":{"content":"a"}},{"index":1,"delta":{"tool_calls":[{"index":0,"id":"t","function":{"name":"f","arguments":"{"}}]}}]}),
        json!({"choices":[{"index":0,"message":{"content":"aSHOULD_NOT_ESCAPE"},"finish_reason":"stop"},{"index":1,"finish_reason":"tool_calls"}]}),
    );
    reject(
        json!({"choices":[{"index":0,"delta":{"content":"a"}},{"index":1,"delta":{"audio":{"data":"AQ"}}}]}),
        json!({"choices":[{"index":0,"message":{"content":"aSHOULD_NOT_ESCAPE"},"finish_reason":"stop"},{"index":1,"message":{},"finish_reason":"stop"}]}),
    );
}
