//! Re-review of the final codec review: closing reconciliation, opaque parts, failures and the
//! lifecycle a streaming client sees.
use super::cross::client_events;
use super::final_events::buffered_output;
use super::final_review::try_raw;
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

/// Client events for buffered Responses `body` replayed to a streaming client.
fn streamed(body: &Value) -> Vec<Value> {
    let (out, _) = response(
        body.to_string().as_bytes(),
        BodyMode::Buffered,
        BodyMode::Streaming,
        Operation::Generate,
    );
    client_events(&out)
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

#[test]
fn opaque_summary_parts_raise_no_text_events() {
    for kind in ["output_text", "refusal"] {
        let part = json!({"type":kind,"text":"opaque-text","payload":7});
        let item = json!({"id":"i","type":"reasoning","status":"completed","summary":[part]});
        let events = streamed(&json!({"id":"r","status":"completed","output":[item]}));
        let types: Vec<_> = events.iter().map(|e| e["type"].as_str().unwrap()).collect();
        assert_eq!(
            types,
            [
                "response.created",
                "response.output_item.added",
                "response.reasoning_summary_part.added",
                "response.reasoning_summary_part.done",
                "response.output_item.done",
                "response.completed"
            ]
        );
        for event in &events[2..4] {
            assert_eq!(event["part"], part);
        }
        assert_eq!(events[5]["response"]["output"][0], item);
    }
}

const COMPLETED: &str =
    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n";

#[test]
fn unnamed_data_is_not_classified_by_its_first_byte() {
    for data in ["{diagnostic}", "  {\"type\":\"response.output_text.delta\""] {
        let (_, canonical) = try_raw(format!("data: {data}\n\n{COMPLETED}").as_bytes(), BodyMode::Buffered).unwrap();
        let opaque: Vec<_> = canonical.iter().filter(|e| e["format"] == "text").collect();
        assert_eq!(opaque[0]["value"], data);
    }
}

#[test]
fn announced_text_parts_carry_their_annotations() {
    let citation = json!({"type":"url_citation","url":"https://e/","title":"t","start_index":0,"end_index":1});
    let part = json!({"type":"output_text","text":"hi","annotations":[citation]});
    let item = json!({"id":"i","type":"message","role":"assistant","status":"completed","content":[part]});
    let events = streamed(&json!({"id":"r","status":"completed","output":[item]}));
    let added = events
        .iter()
        .find(|e| e["type"] == "response.content_part.added")
        .unwrap();
    assert_eq!(added["part"], json!({"type":"output_text","text":"","annotations":[]}));
    assert_eq!(events.last().unwrap()["response"]["output"][0], item);
}
