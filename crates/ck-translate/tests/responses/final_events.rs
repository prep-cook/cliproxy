//! Final codec review, continued: event classification, terminal snapshots and native schemas.
use super::final_review::{try_raw, try_streamed};
use super::*;

const COMPLETED: &str =
    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n";

/// `prefix` followed by an empty completed response, replayed to a buffered client.
fn before_completion(prefix: &str) -> codec::Result<(Value, Vec<Value>)> {
    let (out, canonical) = try_raw(format!("{prefix}{COMPLETED}").as_bytes(), BodyMode::Buffered)?;
    Ok((serde_json::from_slice(&out).unwrap(), canonical))
}

fn opaque(canonical: &[Value]) -> Vec<&Value> {
    canonical
        .iter()
        .filter(|e| e["format"].is_string() && e.get("name").is_none())
        .collect()
}

#[test]
fn unknown_events_never_set_usage_or_identity() {
    let vendor = json!({"type":"vendor.stats","response":{"id":"bogus","model":"x",
        "usage":{"input_tokens":999,"output_tokens":888}}});
    let (body, canonical) = before_completion(&format!("data: {vendor}\n\n")).unwrap();
    assert!(!canonical.iter().any(|e| e.get("usage").is_some()), "{canonical:?}");
    assert!(body.get("usage").is_none(), "{body}");
    assert_eq!(body["id"], "r");
    assert!(!canonical.iter().any(|e| e["id"] == "bogus"));
    assert_eq!(opaque(&canonical)[0]["value"], vendor);
    // A type that is not a string is an unknown event, not a failure.
    let numeric = json!({"type":7,"error":{"code":"x"}});
    let (_, canonical) = before_completion(&format!("data: {numeric}\n\n")).unwrap();
    assert_eq!(opaque(&canonical)[0]["value"], numeric);
}

#[test]
fn unknown_sse_data_stays_opaque() {
    let (_, canonical) =
        before_completion("event: vendor.notice\ndata: {\"error\":{\"detail\":\"diagnostic only\"}}\n\n").unwrap();
    assert_eq!(
        opaque(&canonical)[0]["value"],
        json!({"error":{"detail":"diagnostic only"}})
    );
    let (_, canonical) = before_completion("data: ordinary text\n\n").unwrap();
    assert_eq!(opaque(&canonical)[0]["value"], "ordinary text");
    assert_eq!(opaque(&canonical)[0]["format"], "text");
    // Failures and invalid JSON still end the stream.
    for failing in [
        "data: {\"error\":{\"code\":\"x\"}}\n\n",
        "event: error\ndata: {\"code\":\"x\"}\n\n",
        "event: response.output_text.delta\ndata: not json\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"a\n\n",
    ] {
        assert!(before_completion(failing).is_err(), "{failing}");
    }
}

#[test]
fn a_shorter_terminal_array_contradicts_published_parts() {
    let message = |content: Option<Value>| {
        let mut item = json!({"id":"i","type":"message","role":"assistant","status":"completed"});
        if let Some(content) = content {
            item["content"] = content;
        }
        item
    };
    let reasoning = |summary: Value| json!({"id":"i","type":"reasoning","summary":summary});
    let text = json!([{"type":"output_text","text":"A"}]);
    let summary = json!([{"type":"summary_text","text":"s"}]);
    let run = |done: Value, last: Value| {
        try_streamed(
            &[
                json!({"type":"response.output_item.done","output_index":0,"item":done}),
                json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[last]}}),
            ],
            BodyMode::Buffered,
        )
    };
    assert!(run(message(Some(text.clone())), message(Some(json!([])))).is_err());
    assert!(run(reasoning(summary.clone()), reasoning(json!([]))).is_err());
    // Omission keeps the published parts.
    let (out, _) = run(message(Some(text.clone())), message(None)).unwrap();
    let body: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(body["output"][0]["content"], text);
    let (out, _) = run(reasoning(summary.clone()), reasoning(summary.clone())).unwrap();
    let body: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(body["output"][0]["summary"], summary);
}

/// Buffered Responses `output` replayed to a buffered client; returns the client's output.
fn buffered_output(output: Value) -> Value {
    let body = json!({"id":"r","status":"completed","output":output});
    let (out, _) = response(
        body.to_string().as_bytes(),
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Generate,
    );
    serde_json::from_slice::<Value>(&out).unwrap()["output"].clone()
}

#[test]
fn unknown_and_empty_members_survive() {
    // An unknown item type keeps members that other item types consume.
    let future = json!([{"id":"i","type":"future_tool","status":"completed","input":"payload","arguments":"unknown"}]);
    assert_eq!(buffered_output(future.clone()), future);
    // An unknown summary kind stays opaque instead of readable reasoning.
    let summary = json!([{"id":"i","type":"reasoning","status":"completed","summary":[
        {"type":"future_summary","text":"opaque-text","payload":7}]}]);
    let body = json!({"id":"r","status":"completed","output":summary});
    let (out, canonical) = response(
        body.to_string().as_bytes(),
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Generate,
    );
    assert!(
        !canonical.iter().any(|e| e["kind"] == "reasoning_summary"),
        "{canonical:?}"
    );
    assert!(
        !canonical
            .iter()
            .any(|e| e["choices"][0]["delta"]["reasoning_content"].is_string())
    );
    assert_eq!(serde_json::from_slice::<Value>(&out).unwrap()["output"], summary);
    // Empty collections keep their presence.
    let text = json!([{"id":"i","type":"message","role":"assistant","status":"completed","content":[
        {"type":"output_text","text":"hi","annotations":[],"logprobs":[]}]}]);
    assert_eq!(buffered_output(text.clone()), text);
}

#[test]
fn closing_parts_reconcile_nested_members() {
    let part = |text: &str, vendor: Value| json!({"type":"output_text","text":text,"annotations":[],"vendor":vendor});
    let done = part("hi", json!({"a":1,"b":2}));
    let item = json!({"id":"i","type":"message","role":"assistant","status":"completed","content":[done]});
    let records = [
        json!({"type":"response.output_item.added","output_index":0,
            "item":{"id":"i","type":"message","role":"assistant","content":[]}}),
        json!({"type":"response.content_part.added","output_index":0,"item_id":"i","content_index":0,
            "part":part("", json!({"a":1}))}),
        json!({"type":"response.output_text.delta","output_index":0,"item_id":"i","content_index":0,"delta":"hi"}),
        json!({"type":"response.content_part.done","output_index":0,"item_id":"i","content_index":0,"part":done}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[item]}}),
    ];
    let (out, _) = try_streamed(&records, BodyMode::Buffered).unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&out).unwrap()["output"][0], item);
    // The later value replaces the started one rather than adding a duplicate key.
    assert_eq!(String::from_utf8_lossy(&out).matches("\"vendor\"").count(), 1);
}
