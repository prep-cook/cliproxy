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
