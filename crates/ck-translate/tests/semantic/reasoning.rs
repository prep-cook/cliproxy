use super::{codec, response_fragmented};
use codec::BodyMode;
use serde_json::{Value, json};

/// 512-byte fragments leave long strings unfinished across reads within the test pool.
fn response(input: &[u8], upstream: BodyMode, downstream: BodyMode) -> (Vec<u8>, Vec<Value>) {
    response_fragmented(input, upstream, downstream, 512)
}

fn recorded(name: &str) -> Value {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/pairs/claude-openai.json");
    let fixtures: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    fixtures["fixtures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|fixture| fixture["name"] == name)
        .unwrap()
        .clone()
}

/// Canonical reasoning text, in publication order, and the thinking block starts.
fn canonical_reasoning(events: &[Value]) -> (String, usize) {
    let mut text = String::new();
    for event in events {
        for choice in event["choices"].as_array().into_iter().flatten() {
            assert!(choice["delta"].get("reasoning").is_none());
            assert!(choice["delta"].get("reasoning_details").is_none());
            if let Some(part) = choice["delta"]["reasoning_content"].as_str() {
                text.push_str(part);
            }
        }
    }
    let starts = events.iter().filter(|event| event["kind"] == "thinking").count();
    (text, starts)
}

fn stream_deltas(output: &[u8]) -> Vec<Value> {
    std::str::from_utf8(output)
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter(|line| *line != "[DONE]")
        .flat_map(|line| {
            let chunk: Value = serde_json::from_str(line).unwrap();
            chunk["choices"].as_array().cloned().unwrap_or_default()
        })
        .map(|choice| choice["delta"].clone())
        .collect()
}

fn stream_reasoning(deltas: &[Value]) -> String {
    deltas
        .iter()
        .filter_map(|delta| delta["reasoning_content"].as_str())
        .collect()
}

#[test]
fn recorded_buffered_reasoning_alias_becomes_canonical_reasoning_in_both_modes() {
    let fixture = recorded("TestNonStream_ReasoningFieldEmitsThinkingBlock:1316");
    let input = fixture["input"].as_str().unwrap();
    let (output, events) = response(input.as_bytes(), BodyMode::Buffered, BodyMode::Buffered);
    assert_eq!(canonical_reasoning(&events), ("Thought process".into(), 1));
    let body: Value = serde_json::from_slice(&output).unwrap();
    let message = &body["choices"][0]["message"];
    assert_eq!(message["content"], "Done");
    assert_eq!(message["reasoning_content"], "Thought process");
    assert!(
        message.get("reasoning").is_none(),
        "consumed alias resurrected: {message}"
    );
    let (output, _) = response(input.as_bytes(), BodyMode::Buffered, BodyMode::Streaming);
    let deltas = stream_deltas(&output);
    assert_eq!(stream_reasoning(&deltas), "Thought process");
    assert!(deltas.iter().all(|delta| delta.get("reasoning").is_none()));
}

#[test]
fn recorded_streaming_alias_shapes_keep_preference_and_nested_text() {
    let fixture = recorded("openai-claude/reasoning-shapes/0");
    let mut wire = String::new();
    for line in fixture["lines"].as_array().unwrap() {
        let line = line.as_str().unwrap();
        if line.contains(r#""reasoning_content":5"#) {
            // A non-text reasoning_content with no alias text remains a schema error.
            continue;
        }
        if line == "data: [DONE]" {
            // The recording ends without a finish; Chat completion requires one.
            wire.push_str(r#"data: {"id":"chatcmpl-1","model":"gpt-x","created":5,"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#);
            wire.push_str("\n\n");
        }
        wire.push_str(line);
        wire.push_str("\n\n");
    }
    let expected = "strd1d2nestedwins";
    for mode in [BodyMode::Streaming, BodyMode::Buffered] {
        let (output, events) = response(wire.as_bytes(), BodyMode::Streaming, mode);
        assert_eq!(canonical_reasoning(&events), (expected.into(), 1));
        if mode == BodyMode::Streaming {
            let deltas = stream_deltas(&output);
            assert_eq!(stream_reasoning(&deltas), expected);
            // Unselected aliases without text stay native unknown data, once.
            let unselected: Vec<_> = deltas.iter().filter_map(|delta| delta.get("reasoning")).collect();
            assert_eq!(unselected, [&json!({"x":1})]);
            assert!(deltas.iter().all(|delta| delta.get("reasoning_details").is_none()));
        } else {
            let body: Value = serde_json::from_slice(&output).unwrap();
            let message = &body["choices"][0]["message"];
            assert_eq!(message["reasoning_content"], expected);
            assert_eq!(message["reasoning"], json!({"x":1}));
            assert!(message.get("reasoning_details").is_none());
        }
    }
}

#[test]
fn final_snapshot_aliases_reconcile_against_streamed_reasoning() {
    let chunk = |body: Value, finish: Value| {
        let chunk = json!({"id":"r","model":"m","created":1,"choices":[{"index":0,"finish_reason":finish}]});
        let mut chunk = chunk;
        let (key, value) = body.as_object().unwrap().iter().next().unwrap();
        chunk["choices"][0][key] = value.clone();
        format!("data: {chunk}\n\n")
    };
    let wire = [
        chunk(json!({"delta":{"role":"assistant","reasoning":"T"}}), Value::Null),
        chunk(json!({"delta":{"reasoning_details":[{"text":"U"}]}}), Value::Null),
        chunk(
            json!({"message":{"role":"assistant","reasoning":"TUV","content":"x"}}),
            json!("stop"),
        ),
        "data: [DONE]\n\n".into(),
    ]
    .concat();
    for mode in [BodyMode::Streaming, BodyMode::Buffered] {
        let (output, events) = response(wire.as_bytes(), BodyMode::Streaming, mode);
        assert_eq!(canonical_reasoning(&events), ("TUV".into(), 1));
        if mode == BodyMode::Buffered {
            let body: Value = serde_json::from_slice(&output).unwrap();
            let message = &body["choices"][0]["message"];
            assert_eq!(message["reasoning_content"], "TUV");
            assert_eq!(message["content"], "x");
            assert!(message.get("reasoning").is_none());
        } else {
            assert_eq!(stream_reasoning(&stream_deltas(&output)), "TUV");
        }
    }
}

#[test]
fn buffered_long_reasoning_content_wins_over_an_unselected_alias() {
    let long = "r".repeat(3000);
    let input = json!({"id":"r","object":"chat.completion","model":"m","created":1,"choices":[{"index":0,"message":{"role":"assistant","reasoning_content":long,"reasoning":"other","content":"x"},"finish_reason":"stop"}]});
    for mode in [BodyMode::Buffered, BodyMode::Streaming] {
        // The long value is published as media text before the body ends, not as a chunk.
        let (output, events) = response(input.to_string().as_bytes(), BodyMode::Buffered, mode);
        assert_eq!(canonical_reasoning(&events), (String::new(), 1));
        let (reasoning, unselected) = if mode == BodyMode::Buffered {
            let body: Value = serde_json::from_slice(&output).unwrap();
            let message = body["choices"][0]["message"].clone();
            (
                message["reasoning_content"].as_str().unwrap().to_owned(),
                vec![message["reasoning"].clone()],
            )
        } else {
            let deltas = stream_deltas(&output);
            let unselected = deltas
                .iter()
                .filter_map(|delta| delta.get("reasoning").cloned())
                .collect();
            (stream_reasoning(&deltas), unselected)
        };
        assert!(reasoning == long, "reasoning has {} bytes", reasoning.len());
        assert_eq!(unselected, [json!("other")]);
    }
}

#[test]
fn buffered_long_reasoning_alias_completes_before_publication() {
    let long = "a".repeat(2000);
    let input = json!({"id":"r","object":"chat.completion","model":"m","created":1,"choices":[{"index":0,"message":{"role":"assistant","reasoning":long,"content":"x"},"finish_reason":"stop"}]});
    let (output, events) = response(input.to_string().as_bytes(), BodyMode::Buffered, BodyMode::Streaming);
    assert_eq!(canonical_reasoning(&events), (long.clone(), 1));
    let deltas = stream_deltas(&output);
    assert_eq!(stream_reasoning(&deltas), long);
    assert!(deltas.iter().all(|delta| delta.get("reasoning").is_none()));
}

#[test]
fn empty_aliases_fall_through_and_give_way_without_empty_chunks() {
    for delta in [
        json!({"reasoning_content":"","reasoning":"T"}),
        json!({"reasoning":"","reasoning_details":[{"text":""},{"text":"T"}]}),
        json!({"reasoning_content":null,"reasoning":[],"reasoning_details":"T"}),
    ] {
        let chunk =
            json!({"id":"r","model":"m","created":1,"choices":[{"index":0,"delta":delta,"finish_reason":null}]});
        let finish =
            json!({"id":"r","model":"m","created":1,"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]});
        let wire = format!("data: {chunk}\n\ndata: {finish}\n\ndata: [DONE]\n\n");
        let (output, events) = response(wire.as_bytes(), BodyMode::Streaming, BodyMode::Streaming);
        assert_eq!(canonical_reasoning(&events), ("T".into(), 1), "{delta}");
        let parts: Vec<_> = stream_deltas(&output)
            .into_iter()
            .filter_map(|delta| delta.get("reasoning_content").cloned())
            .collect();
        assert_eq!(parts, [json!("T")], "{delta}");
    }
}

#[test]
fn reasoning_alias_wins_over_reasoning_details_in_either_source_order() {
    let details = json!([{"text":"details"}]);
    for message in [
        json!({"reasoning":"alias","reasoning_details":details}),
        json!({"reasoning_details":details,"reasoning":"alias"}),
    ] {
        let body =
            json!({"id":"r","model":"m","created":1,"choices":[{"index":0,"message":message,"finish_reason":"stop"}]});
        let mut chunk = body.clone();
        chunk["choices"][0]["delta"] = chunk["choices"][0]["message"].take();
        chunk["choices"][0].as_object_mut().unwrap().remove("message");
        let stream = format!("data: {chunk}\n\ndata: [DONE]\n\n");
        for (upstream, wire) in [(BodyMode::Buffered, body.to_string()), (BodyMode::Streaming, stream)] {
            for mode in [BodyMode::Buffered, BodyMode::Streaming] {
                let case = format!("{message}/{upstream:?}/{mode:?}");
                let (output, events) = response(wire.as_bytes(), upstream, mode);
                assert_eq!(canonical_reasoning(&events), ("alias".into(), 1), "{case}");
                // The unselected alias stays native unknown data, once.
                let (reasoning, unselected) = if mode == BodyMode::Buffered {
                    let body: Value = serde_json::from_slice(&output).unwrap();
                    let message = body["choices"][0]["message"].clone();
                    assert!(message.get("reasoning").is_none(), "{case}");
                    (
                        message["reasoning_content"].as_str().unwrap().to_owned(),
                        vec![message["reasoning_details"].clone()],
                    )
                } else {
                    let deltas = stream_deltas(&output);
                    assert!(deltas.iter().all(|delta| delta.get("reasoning").is_none()), "{case}");
                    let unselected = deltas
                        .iter()
                        .filter_map(|delta| delta.get("reasoning_details").cloned())
                        .collect();
                    (stream_reasoning(&deltas), unselected)
                };
                assert_eq!(reasoning, "alias", "{case}");
                assert_eq!(unselected, std::slice::from_ref(&details), "{case}");
            }
        }
    }
}
