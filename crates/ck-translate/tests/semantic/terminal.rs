use super::{Mapped, Operation, codec, identity};
use codec::{BodyMode, ResponseIngress, event::Direction};

fn rejected(input: &[u8], mode: BodyMode, prior_done: bool) {
    let mut mapped = Mapped::new();
    let mut ingress = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        mode,
        1024 * 1024,
    )
    .unwrap();
    let result = ingress
        .advance(&mut mapped.writer, input)
        .and_then(|()| ingress.finish(&mut mapped.writer));
    assert!(result.is_err(), "accepted {input:?}");
    let published = mapped.output(&mapped.writer);
    let text = std::str::from_utf8(&published).unwrap();
    assert_eq!(text.matches("event: x_ck.done\n").count(), usize::from(prior_done));
    if !prior_done {
        assert_eq!(text.matches("event: x_ck.error\n").count(), 1);
    }
    assert!(ingress.advance(&mut mapped.writer, b"data: [DONE]\n\n").is_err());
    assert!(ingress.finish(&mut mapped.writer).is_err());
    assert_eq!(mapped.output(&mapped.writer), published);
}

#[test]
fn missing_early_duplicate_terminal_and_post_finish_content_are_rejected() {
    let open = "data: {\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"},\"finish_reason\":null}]}\n\n";
    let finish = "data: {\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";
    for input in [
        open.to_owned(),
        format!("{open}data: [DONE]\n\n"),
        finish.to_owned(),
        format!("{finish}{open}"),
        format!("{finish}{finish}"),
        "data: [DONE]\n\n".to_owned(),
    ] {
        rejected(input.as_bytes(), BodyMode::Streaming, false);
    }
    for suffix in ["data: [DONE]\n\n", open] {
        rejected(
            format!("{finish}data: [DONE]\n\n{suffix}").as_bytes(),
            BodyMode::Streaming,
            true,
        );
    }
}

#[test]
fn malformed_transport_json_and_provider_errors_never_finish_successfully() {
    for input in [
        b"data: {\"choices\":[]}\n".as_slice(),
        b"data: {\"choices\":[}\n\n",
        b"data: \xff\n\n",
        b"data: {\"error\":{\"message\":\"provider secret\"}}\n\n",
        b"event: error\ndata: {\"message\":\"failure\"}\n\n",
    ] {
        rejected(input, BodyMode::Streaming, false);
    }
    for input in [
        b"{\"choices\":[}".as_slice(),
        b"{\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{\"index\":0,\"message\":{\"content\":\"a\"}}]}",
        b"{\"error\":{\"message\":\"provider secret\"}}",
        b"{\"choices\":[],\"x\":\"\xff\"}",
    ] {
        rejected(input, BodyMode::Buffered, false);
    }
}

#[test]
fn incomplete_successful_tool_json_and_identity_contradictions_are_rejected() {
    let start = "data: {\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-a\",\"type\":\"function\",\"function\":{\"name\":\"f\",\"arguments\":\"{\"}}]},\"finish_reason\":null}]}\n\n";
    for delta in [
        serde_json::json!({}),
        serde_json::json!({"tool_calls":[{"index":0,"id":"call-b","function":{"arguments":"}"}}]}),
        serde_json::json!({"tool_calls":[{"index":0,"function":{"name":"g","arguments":"}"}}]}),
        serde_json::json!({"tool_calls":[{"index":0,"type":"custom","function":{"arguments":"}"}}]}),
    ] {
        let finish = serde_json::json!({"id":"r","choices":[{"index":0,"delta":delta,"finish_reason":"tool_calls"}]});
        rejected(
            format!("{start}data: {finish}\n\ndata: [DONE]\n\n").as_bytes(),
            BodyMode::Streaming,
            false,
        );
    }
    let incomplete = serde_json::json!({"id":"r","model":"m","created":1,"choices":[{"index":0,"message":{"tool_calls":[{"id":"c","type":"function","function":{"name":"f","arguments":"{"}}]},"finish_reason":"tool_calls"}]});
    rejected(incomplete.to_string().as_bytes(), BodyMode::Buffered, false);
}

/// The data of the first `kind` event; frames carry an `id:` line before `data:`.
fn event_data(text: &str, kind: &str) -> serde_json::Value {
    let data = text
        .lines()
        .skip_while(|line| line.strip_prefix("event: ") != Some(kind))
        .find_map(|line| line.strip_prefix("data: "))
        .unwrap_or_else(|| panic!("no {kind} event in {text}"));
    serde_json::from_str(data).unwrap()
}

fn error_status(text: &str) -> (u64, String) {
    let error = event_data(text, "x_ck.error");
    (
        error["status"].as_u64().unwrap(),
        error["scope"].as_str().unwrap().to_owned(),
    )
}

#[test]
fn invalid_input_status_follows_the_failing_side() {
    use super::{ChatCapability, Ingress, RequestIngress};
    let mut mapped = Mapped::new();
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        1024,
    )
    .unwrap();
    assert!(ingress.advance(&mut mapped.writer, b"{]").is_err());
    assert!(ingress.finish(&mut mapped.writer, &ChatCapability).is_err());
    let published = mapped.output(&mapped.writer);
    assert_eq!(
        error_status(std::str::from_utf8(&published).unwrap()),
        (400, "client".to_owned())
    );

    let mut mapped = Mapped::new();
    let mut ingress = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        1024 * 1024,
    )
    .unwrap();
    assert!(
        ingress
            .advance(&mut mapped.writer, b"data: {\"choices\":[}\n\n")
            .is_err()
    );
    let published = mapped.output(&mapped.writer);
    assert_eq!(
        error_status(std::str::from_utf8(&published).unwrap()),
        (502, "upstream".to_owned())
    );
}

#[test]
fn length_and_content_filter_finish_tolerate_incomplete_tool_json() {
    let start = "data: {\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"c\",\"type\":\"function\",\"function\":{\"name\":\"f\",\"arguments\":\"{\\\"a\\\":\"}}]},\"finish_reason\":null}]}\n\n";
    for reason in ["length", "content_filter"] {
        let finish = serde_json::json!({"id":"r","choices":[{"index":0,"delta":{},"finish_reason":reason}]});
        let mut mapped = Mapped::new();
        let mut ingress = codec::chat::response::Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Response),
            Operation::Generate,
            BodyMode::Streaming,
            1024 * 1024,
        )
        .unwrap();
        ingress
            .advance(
                &mut mapped.writer,
                format!("{start}data: {finish}\n\ndata: [DONE]\n\n").as_bytes(),
            )
            .unwrap();
        ingress.finish(&mut mapped.writer).unwrap();
        let published = mapped.output(&mapped.writer);
        let text = std::str::from_utf8(&published).unwrap();
        assert_eq!(text.matches("event: x_ck.done\n").count(), 1, "{reason}");
        assert!(!text.contains("event: x_ck.error\n"), "{reason}");
        let end = event_data(text, "x_ck.tool.end");
        assert_eq!(end["status"], "incomplete", "{reason}");
        assert_eq!(end["valid_json"], false, "{reason}");
    }
}
