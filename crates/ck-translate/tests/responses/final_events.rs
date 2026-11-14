//! Final codec review, continued: event classification, terminal snapshots and native schemas.
use super::cross::client_events;
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
        "event: response.completed\ndata: {\"type\":\"response.completed\"\n\n",
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
pub(super) fn buffered_output(output: Value) -> Value {
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
    // The terminal response alone closes the part the same way.
    let (out, _) = try_streamed(&[&records[..3], &records[5..]].concat(), BodyMode::Buffered).unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&out).unwrap()["output"][0], item);
}

#[test]
fn failure_and_annotation_events_follow_the_native_schemas() {
    let created =
        json!({"type":"response.created","response":{"id":"r","model":"m","status":"in_progress","output":[]}});
    let records = [
        created.clone(),
        json!({"type":"response.output_item.added","output_index":0,
            "item":{"id":"i","type":"message","role":"assistant","content":[]}}),
        json!({"type":"response.content_part.added","output_index":0,"item_id":"i","content_index":0,
            "part":{"type":"output_text","text":"","annotations":[]}}),
        json!({"type":"response.output_text.delta","output_index":0,"item_id":"i","content_index":0,"delta":"hi"}),
        json!({"type":"response.failed","response":{"id":"r","status":"failed"}}),
    ];
    for mode in [BodyMode::Streaming, BodyMode::Buffered] {
        let mut m = Mapped::new();
        let mut ingress = responses::response::Ingress::new(
            &mut m.writer,
            identity(Direction::Response),
            Operation::Generate,
            BodyMode::Streaming,
            1024 * 1024,
        )
        .unwrap();
        let mut reader = m.store.reader(m.writer.id(), 0).unwrap();
        let mut egress = responses::egress::Egress::new(
            &mut m.writer,
            identity(Direction::Response),
            Operation::Generate,
            mode,
            |_, _| {},
        )
        .unwrap();
        let _ = ingress
            .advance(&mut m.writer, wire(&records).as_bytes())
            .and_then(|()| ingress.finish(&mut m.writer));
        let mut out = Vec::new();
        while let std::task::Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
            egress
                .advance(&mut m.writer, lease, |chunk| {
                    out.extend_from_slice(&bytes(&chunk));
                    Ok(())
                })
                .unwrap();
        }
        let response = match mode {
            BodyMode::Streaming => {
                let events = client_events(&out);
                let failed = events.last().unwrap();
                assert_eq!(failed["type"], "response.failed");
                assert_eq!(failed["sequence_number"], events.len() as u64 - 1);
                failed["response"].clone()
            }
            _ => serde_json::from_slice::<Value>(&out).unwrap(),
        };
        assert_eq!(response["id"], "r");
        assert_eq!(response["object"], "response");
        assert_eq!(response["status"], "failed");
        assert!(response["error"]["code"].is_string() && response["error"]["message"].is_string());
    }
    let citation = |i: u64| json!({"type":"url_citation","url":format!("https://e/{i}"),"title":"t","start_index":0,"end_index":1});
    let text = json!({"id":"r","status":"completed","output":[{"id":"i","type":"message","role":"assistant",
        "content":[{"type":"output_text","text":"hi","annotations":[citation(0),citation(1)]}]}]});
    let (out, _) = response(
        text.to_string().as_bytes(),
        BodyMode::Buffered,
        BodyMode::Streaming,
        Operation::Generate,
    );
    let events = client_events(&out);
    let added: Vec<_> = events
        .iter()
        .filter(|e| e["type"] == "response.output_text.annotation.added")
        .collect();
    assert_eq!(added.len(), 2);
    for (i, event) in added.iter().enumerate() {
        assert_eq!(event["annotation_index"], i as u64);
        assert_eq!(event["annotation"], citation(i as u64));
    }
}

#[test]
fn streaming_clients_see_the_lifecycle_status() {
    let response = |status: &str| json!({"id":"r","model":"m","status":status,"output":[]});
    let records = [
        json!({"type":"response.created","response":response("queued")}),
        json!({"type":"response.in_progress","response":response("in_progress")}),
        json!({"type":"response.completed","response":response("completed")}),
    ];
    let (out, _) = try_streamed(&records, BodyMode::Streaming).unwrap();
    let events = client_events(&out);
    let lifecycle: Vec<_> = events
        .iter()
        .map(|e| (e["type"].as_str().unwrap(), e["response"]["status"].as_str().unwrap()))
        .collect();
    assert_eq!(
        lifecycle,
        [
            ("response.created", "queued"),
            ("response.in_progress", "in_progress"),
            ("response.completed", "completed")
        ]
    );
}

#[test]
fn function_tool_choice_keeps_its_extensions() {
    let input = json!({"model":"m","input":"q","tools":[{"type":"function","name":"f","parameters":{}}],
        "tool_choice":{"type":"function","name":"f","vendor":{"a":null}}});
    let (output, _) = request(&input, Operation::Generate);
    assert_eq!(output["tool_choice"], input["tool_choice"]);
}
