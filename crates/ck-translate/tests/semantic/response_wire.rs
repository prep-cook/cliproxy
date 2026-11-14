//! Chat response ingress and client egress: independent choices, tools, usage, buffered
//! projection and provider identity.
use super::*;

#[test]
fn response_choices_tools_and_usage_remain_independent() {
    let wire = concat!(
        "data: {\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{\"index\":2,\"delta\":{\"role\":\"assistant\",\"content\":\"two\"},\"finish_reason\":null},{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":3,\"id\":\"a\",\"type\":\"function\",\"function\":{\"name\":\"f\",\"arguments\":\"{\\\"a\\\":\"}},{\"index\":7,\"id\":\"b\",\"type\":\"function\",\"function\":{\"name\":\"g\",\"arguments\":\"{}\"}}]},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":3,\"function\":{\"arguments\":\"1}\"}}]},\"finish_reason\":\"tool_calls\"},{\"index\":2,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":0,\"vendor_ttl\":{\"300\":2}}}}\n\n",
        "data: {\"choices\":[],\"usage\":{\"completion_tokens\":3}}\n\n",
        "data: [DONE]\n\n"
    );
    let (result, events) = response(wire.as_bytes(), codec::BodyMode::Streaming, codec::BodyMode::Buffered);
    let result: serde_json::Value = serde_json::from_slice(&result).unwrap();
    assert_eq!(result["choices"][0]["index"], 2);
    assert_eq!(result["choices"][0]["message"]["content"], "two");
    let calls = &result["choices"][1]["message"]["tool_calls"];
    assert_eq!(calls[0]["id"], "a");
    assert_eq!(calls[0]["function"]["arguments"], "{\"a\":1}");
    assert_eq!(calls[1]["function"]["arguments"], "{}");
    assert_eq!(result["usage"]["completion_tokens"], 3);
    assert_eq!(result["usage"]["total_tokens"], 13);
    assert_eq!(result["usage"]["prompt_tokens_details"]["vendor_ttl"]["300"], 2);
    assert!(
        events
            .iter()
            .filter_map(|event| event["choices"].as_array())
            .all(|choices| choices.len() <= 1)
    );
}

#[test]
fn buffered_response_projects_streaming_with_empty_refusal_and_logprobs() {
    let input = br#"{"id":"r","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"","refusal":""},"logprobs":{"content":[{"token":"x","bytes":[120],"top_logprobs":[]}]},"finish_reason":"stop"}],"usage":{"prompt_tokens":0,"completion_tokens":0}}"#;
    let (output, events) = response(input, codec::BodyMode::Buffered, codec::BodyMode::Streaming);
    let text = std::str::from_utf8(&output).unwrap();
    assert_eq!(text.matches("data: [DONE]").count(), 1);
    let logprobs = events
        .iter()
        .find(|event| event["choices"][0].get("logprobs").is_some())
        .unwrap();
    assert_eq!(logprobs["choices"][0]["delta"], serde_json::json!({}));
    assert_eq!(
        logprobs["choices"][0]["logprobs"]["content"][0]["bytes"],
        serde_json::json!([120])
    );
    assert!(text.contains("\"refusal\":\"\""));
}

#[test]
fn tool_arguments_are_published_before_finish_and_truncation_never_succeeds() {
    use codec::ResponseIngress;
    let mut mapped = Mapped::new();
    let mut ingress = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        codec::BodyMode::Streaming,
        1024 * 1024,
    )
    .unwrap();
    ingress.advance(&mut mapped.writer, b"data: {\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"c\",\"type\":\"function\",\"function\":{\"name\":\"f\",\"arguments\":\"{\"}}]},\"finish_reason\":null}]}\n\n").unwrap();
    let output = mapped.output(&mapped.writer);
    assert!(std::str::from_utf8(&output).unwrap().contains("\"arguments\":\"{\""));
    assert!(ingress.finish(&mut mapped.writer).is_err());
    let output = mapped.output(&mapped.writer);
    let text = std::str::from_utf8(&output).unwrap();
    assert!(text.contains("event: x_ck.error\n"));
    assert!(!text.contains("event: x_ck.done\n"));
    assert!(!text.contains("event: x_ck.tool.end\n"));
}

#[test]
fn heartbeat_does_not_replace_provider_identity() {
    let wire = concat!(
        ": ping\n\n",
        "data: {\"id\":\"provider-id\",\"model\":\"provider-model\",\"created\":123,\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n"
    );
    let (bytes, _) = response(wire.as_bytes(), codec::BodyMode::Streaming, codec::BodyMode::Buffered);
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["id"], "provider-id");
    assert_eq!(value["model"], "provider-model");
    assert_eq!(value["created"], 123);
    assert_eq!(value["choices"][0]["message"]["content"], "hello");
}
