//! Re-review of the final codec review: closing reconciliation, opaque parts, failures and the
//! lifecycle a streaming client sees.
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
