use super::*;

fn wire(records: &[Value]) -> String {
    records.iter().map(|r| format!("data: {r}\n\n")).collect()
}

fn streamed(records: &[Value]) -> (Value, Vec<Value>) {
    let (out, events) = response(
        wire(records).as_bytes(),
        BodyMode::Streaming,
        BodyMode::Buffered,
        Operation::Generate,
    );
    (serde_json::from_slice(&out).unwrap(), events)
}

fn fails(wire: &str) -> bool {
    let mut m = Mapped::new();
    let mut ingress = responses::response::Ingress::new(
        &mut m.writer,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        1024 * 1024,
    )
    .unwrap();
    ingress
        .advance(&mut m.writer, wire.as_bytes())
        .and_then(|()| ingress.finish(&mut m.writer))
        .is_err()
}

fn message(status: &str, content: Value) -> Value {
    json!({"id":"i","type":"message","role":"assistant","status":status,"content":content})
}

#[test]
fn annotation_events_and_part_snapshots_publish_each_citation_once() {
    let annotation = json!({"type":"url_citation","url":"u","start_index":0,"end_index":2,"title":"t"});
    let part = json!({"type":"output_text","text":"hi","annotations":[annotation]});
    let records = [
        json!({"type":"response.created","response":{"id":"r","model":"m","created_at":1}}),
        json!({"type":"response.output_item.added","output_index":0,"item":message("in_progress", json!([]))}),
        json!({"type":"response.content_part.added","output_index":0,"item_id":"i","content_index":0,"part":{"type":"output_text","text":""}}),
        json!({"type":"response.output_text.delta","output_index":0,"item_id":"i","content_index":0,"delta":"hi"}),
        json!({"type":"response.output_text.annotation.added","output_index":0,"item_id":"i","content_index":0,"annotation_index":0,"annotation":annotation}),
        json!({"type":"response.content_part.done","output_index":0,"item_id":"i","content_index":0,"part":part}),
        json!({"type":"response.output_item.done","output_index":0,"item":message("completed", json!([part]))}),
        json!({"type":"response.completed","response":{"id":"r","status":"completed","metadata":{"k":"v"},"output":[message("completed", json!([part]))]}}),
    ];
    let (out, events) = streamed(&records);
    assert_eq!(out["metadata"], json!({"k":"v"}));
    let citations: Vec<_> = events.iter().filter(|e| e["kind"] == "citation").collect();
    assert_eq!(citations.len(), 1);
    assert_eq!(citations[0]["x_ck"]["source"]["output_index"], 0);
    assert_eq!(out["output"][0]["content"][0]["annotations"], json!([annotation]));
    let mut late = records.to_vec();
    late.swap(4, 5);
    assert!(fails(&wire(&late)));
}

#[test]
fn part_snapshots_publish_logprobs_and_annotations() {
    let annotation = json!({"type":"url_citation","url":"u","start_index":0,"end_index":2,"title":"t"});
    let logprobs = json!([{"token":"hi","logprob":-0.1,"bytes":[104,105],"top_logprobs":[]}]);
    let part = json!({"type":"output_text","text":"hi","annotations":[annotation],"logprobs":logprobs});
    let body = json!({"id":"r","status":"completed","output":[message("completed", json!([part]))]});
    let (_, events) = response(
        &serde_json::to_vec(&body).unwrap(),
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Generate,
    );
    assert!(
        events
            .iter()
            .any(|e| e["kind"] == "citation" && e["value"] == annotation)
    );
    assert!(
        events
            .iter()
            .any(|e| e["choices"][0]["logprobs"]["content"] == logprobs)
    );
}

#[test]
fn interim_usage_incomplete_items_and_part_ends_publish_in_order() {
    let parts = json!([{"type":"output_text","text":"a"},{"type":"output_text","text":"b"}]);
    let records = [
        json!({"type":"response.created","response":{"id":"r","model":"m","created_at":1}}),
        json!({"type":"response.in_progress","response":{"id":"r","usage":{"input_tokens":5,"output_tokens":0}}}),
        json!({"type":"response.output_item.added","output_index":0,"item":message("in_progress", json!([]))}),
        json!({"type":"response.content_part.added","output_index":0,"item_id":"i","content_index":0,"part":{"type":"output_text","text":"a"}}),
        json!({"type":"response.content_part.done","output_index":0,"item_id":"i","content_index":0,"part":parts[0]}),
        json!({"type":"response.content_part.added","output_index":0,"item_id":"i","content_index":1,"part":{"type":"output_text","text":"b"}}),
        json!({"type":"response.content_part.done","output_index":0,"item_id":"i","content_index":1,"part":parts[1]}),
        json!({"type":"response.output_item.done","output_index":0,"item":message("incomplete", parts.clone())}),
        json!({"type":"response.incomplete","response":{"id":"r","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[message("incomplete", parts)]}}),
    ];
    let (_, events) = streamed(&records);
    let interim = events.iter().position(|e| e["usage"]["final"] == false).unwrap();
    assert_eq!(events[interim]["usage"]["input_tokens"], 5);
    let item = events.iter().position(|e| e["kind"] == "message").unwrap();
    assert!(interim < item);
    let first_end = events
        .iter()
        .position(|e| e["x_ck"]["block_index"] == 0 && e.as_object().unwrap().len() == 1)
        .unwrap();
    let second_start = events
        .iter()
        .position(|e| e["x_ck"]["block_index"] == 1 && e["kind"] == "text")
        .unwrap();
    assert!(first_end < second_start);
    let end = events.iter().find(|e| e.get("status").is_some()).unwrap();
    assert_eq!(end["status"], "incomplete");
}

#[test]
fn broken_native_events_and_dropped_items_fail_a_completed_stream() {
    let completed = json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[]}});
    for name in ["response.output_text.delta", "error"] {
        assert!(fails(&format!(
            "event: {name}\ndata: broken\n\n{}",
            wire(std::slice::from_ref(&completed))
        )));
    }
    let item = json!({"id":"f","type":"function_call","call_id":"c","name":"f","arguments":"{}","status":"completed"});
    let dropped = wire(&[
        json!({"type":"response.output_item.added","output_index":0,"item":item}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        completed,
    ]);
    assert!(fails(&dropped));
    assert!(!fails(&wire(&[
        json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[]}})
    ])));
}
