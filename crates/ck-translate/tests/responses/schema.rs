//! What the native schema and its official client require of egress: every `output_text` part
//! carries `annotations`, and a response keeps one gateway-generated id when its source has none.
use super::cross::client_events;
use super::final_review::try_streamed;
use super::*;

fn message(content: Value) -> Value {
    json!({"id":"i","type":"message","role":"assistant","content":content})
}

/// Every output_text part a streaming client sees: announced, closed, in the closed item and in
/// the final response.
fn text_parts(events: &[Value]) -> Vec<Value> {
    let mut parts = Vec::new();
    for event in events {
        let content = match event["type"].as_str().unwrap_or("") {
            "response.content_part.added" | "response.content_part.done" => vec![event["part"].clone()],
            "response.output_item.done" => event["item"]["content"].as_array().cloned().unwrap_or_default(),
            "response.completed" => event["response"]["output"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|item| item["content"].as_array().cloned().unwrap_or_default())
                .collect(),
            _ => Vec::new(),
        };
        parts.extend(content.into_iter().filter(|part| part["type"] == "output_text"));
    }
    parts
}

#[test]
fn output_text_parts_always_carry_annotations() {
    let started = json!({"type":"output_text","text":""});
    let done = json!({"type":"output_text","text":"hi"});
    let records = [
        json!({"type":"response.created","response":{"id":"r","model":"m","status":"in_progress","output":[]}}),
        json!({"type":"response.output_item.added","output_index":0,"item":message(json!([]))}),
        json!({"type":"response.content_part.added","output_index":0,"content_index":0,"item_id":"i","part":started}),
        json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"i","delta":"hi"}),
        json!({"type":"response.content_part.done","output_index":0,"content_index":0,"item_id":"i","part":done}),
        json!({"type":"response.output_item.done","output_index":0,"item":message(json!([done]))}),
        json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[message(json!([done]))]}}),
    ];
    let expected = json!({"type":"output_text","text":"hi","annotations":[]});
    let (out, _) = try_streamed(&records, BodyMode::Streaming).unwrap();
    let parts = text_parts(&client_events(&out));
    assert_eq!(parts.len(), 4);
    assert_eq!(parts[0], json!({"type":"output_text","text":"","annotations":[]}));
    for part in &parts[1..] {
        assert_eq!(part, &expected);
    }
    let (out, _) = try_streamed(&records, BodyMode::Buffered).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&out).unwrap()["output"][0]["content"][0],
        expected
    );
    // A buffered source body, replayed buffered and streaming.
    let body = json!({"id":"r","status":"completed","output":[message(json!([done]))]});
    let (out, _) = response(
        body.to_string().as_bytes(),
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Generate,
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&out).unwrap()["output"][0]["content"][0],
        expected
    );
    let (out, _) = response(
        body.to_string().as_bytes(),
        BodyMode::Buffered,
        BodyMode::Streaming,
        Operation::Generate,
    );
    let parts = text_parts(&client_events(&out));
    assert_eq!(parts.len(), 4);
    assert!(
        parts
            .iter()
            .all(|part| part["annotations"] == json!([]) && part.get("logprobs").is_none())
    );
}
