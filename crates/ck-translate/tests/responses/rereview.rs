//! Re-review of the final codec review: closing reconciliation, opaque parts, failures and the
//! lifecycle a streaming client sees.
use super::final_events::buffered_output;
use super::final_review::try_streamed;
use super::*;

/// The part that a streamed message starts with `started` and closes with `done`, as a buffered
/// client receives it.
fn closed_part(started: Value, done: Value) -> Value {
    let item = |content: Value| json!({"id":"i","type":"message","role":"assistant","content":content});
    let records = [
        json!({"type":"response.created","response":{"id":"r","model":"m","status":"in_progress","output":[]}}),
        json!({"type":"response.output_item.added","output_index":0,"item":item(json!([]))}),
        json!({"type":"response.content_part.added","output_index":0,"content_index":0,"item_id":"i","part":started}),
        json!({"type":"response.output_item.done","output_index":0,"item":item(json!([done]))}),
        json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[item(json!([done]))]}}),
    ];
    let (out, _) = try_streamed(&records, BodyMode::Buffered).unwrap();
    serde_json::from_slice::<Value>(&out).unwrap()["output"][0]["content"][0].clone()
}

#[test]
fn closing_snapshots_merge_nested_and_opaque_members() {
    // An omitted nested member keeps its started value.
    assert_eq!(
        closed_part(
            json!({"type":"output_text","text":"","vendor":{"a":1,"b":2}}),
            json!({"type":"output_text","text":"hi","vendor":{"b":3}})
        ),
        json!({"type":"output_text","text":"hi","vendor":{"a":1,"b":3}})
    );
    // The text of an opaque part is an ordinary member.
    assert_eq!(
        closed_part(
            json!({"type":"future_content","text":"","vendor":1}),
            json!({"type":"future_content","text":"opaque","vendor":2})
        ),
        json!({"type":"future_content","text":"opaque","vendor":2})
    );
}

#[test]
fn empty_arrays_first_supplied_at_close_keep_their_presence() {
    let done = json!({"type":"output_text","text":"hi","annotations":[],"logprobs":[]});
    assert_eq!(closed_part(json!({"type":"output_text","text":""}), done.clone()), done);
}

#[test]
fn unknown_items_keep_every_member() {
    let future = json!([{"id":"i","type":"future_tool","status":"completed","content":"opaque-content",
        "summary":"opaque-summary","encrypted_content":"E","input":"payload","arguments":"arg"}]);
    assert_eq!(buffered_output(future.clone()), future);
    // Parts and encryption of an unknown item open no blocks.
    let parts = json!([{"id":"i","type":"future_tool","status":"completed","encrypted_content":"E",
        "content":[{"type":"output_text","text":"x"}],"summary":[{"type":"summary_text","text":"y"}]}]);
    let body = json!({"id":"r","status":"completed","output":parts});
    let (out, canonical) = response(
        body.to_string().as_bytes(),
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Generate,
    );
    let blocks: Vec<_> = canonical
        .iter()
        .filter(|e| e["x_ck"]["block_index"].is_number())
        .collect();
    assert!(blocks.is_empty(), "{blocks:?}");
    assert_eq!(serde_json::from_slice::<Value>(&out).unwrap()["output"], parts);
}
