use super::*;

/// The finish_reason of the Chat chunk that closes choice 0.
fn finish(events: &[Value]) -> (Value, Value) {
    let event = events
        .iter()
        .find(|event| event["choices"][0]["finish_reason"].is_string())
        .unwrap();
    (
        event["choices"][0]["finish_reason"].clone(),
        event["x_ck"]["native_finish_reason"].clone(),
    )
}

#[test]
fn incomplete_content_filter_keeps_the_filter_finish() {
    let input = json!({"id":"r","status":"incomplete","incomplete_details":{"reason":"content_filter"},"output":[{"id":"m","type":"message","role":"assistant","content":[{"type":"output_text","text":"x"}]}]});
    let (_, events) = response(
        &serde_json::to_vec(&input).unwrap(),
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Generate,
    );
    assert_eq!(
        finish(&events),
        (json!("content_filter"), input["incomplete_details"].clone())
    );
    let input = json!({"id":"r","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[]});
    let (_, events) = response(
        &serde_json::to_vec(&input).unwrap(),
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Generate,
    );
    assert_eq!(finish(&events).0, "length");
}

#[test]
fn completed_function_call_finishes_with_tool_calls() {
    for item in [
        json!({"id":"f","type":"function_call","call_id":"c","name":"f","arguments":"{}"}),
        json!({"id":"f","type":"custom_tool_call","call_id":"c","name":"f","input":"x"}),
    ] {
        let input = json!({"id":"r","status":"completed","output":[{"id":"m","type":"message","role":"assistant","content":[{"type":"output_text","text":"x"}]},item]});
        let (_, events) = response(
            &serde_json::to_vec(&input).unwrap(),
            BodyMode::Buffered,
            BodyMode::Buffered,
            Operation::Generate,
        );
        assert_eq!(finish(&events), (json!("tool_calls"), json!("completed")));
    }
    let server = json!({"id":"r","status":"completed","output":[{"id":"w","type":"web_search_call","status":"completed","action":{"type":"search","query":"q"}}]});
    let (_, events) = response(
        &serde_json::to_vec(&server).unwrap(),
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Generate,
    );
    assert_eq!(finish(&events).0, "stop", "server tools need no client call");
}

#[test]
fn chat_incomplete_finish_reports_the_incomplete_reason() {
    for (reason, native) in [("length", "max_output_tokens"), ("content_filter", "content_filter")] {
        let input = json!({"id":"r","choices":[{"index":0,"message":{"role":"assistant","content":"x"},"finish_reason":reason}]});
        let mut m = Mapped::new();
        let mut ingress = codec::chat::response::Ingress::new(
            &mut m.writer,
            &m.store,
            identity(Direction::Response),
            Operation::Generate,
            BodyMode::Buffered,
            1_000_000,
        )
        .unwrap();
        let mut reader = m.store.reader(m.writer.id(), 0).unwrap();
        let mut egress = responses::egress::Egress::new(
            &mut m.writer,
            identity(Direction::Response),
            Operation::Generate,
            BodyMode::Buffered,
            |_, _| {},
        )
        .unwrap();
        ingress
            .advance(&mut m.writer, &serde_json::to_vec(&input).unwrap())
            .unwrap();
        ingress.finish(&mut m.writer).unwrap();
        let mut output = Vec::new();
        while let std::task::Poll::Ready(Some(ck_stream::Read::Record(record))) = reader.try_next() {
            egress
                .advance(&mut m.writer, record, |chunk| {
                    output.extend_from_slice(&bytes(&chunk));
                    Ok(())
                })
                .unwrap();
        }
        egress.finish().unwrap();
        let output: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(output["status"], "incomplete");
        assert_eq!(output["incomplete_details"], json!({"reason": native}), "{reason}");
    }
}
