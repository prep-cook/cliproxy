#[path = "streams.rs"]
mod streams;

use super::{
    BodyMode, Direction, Mapped, Operation, RequestIngress, Value, anthropic, assert_invalid_input, bytes, codec,
    identity, json, request_fragmented, response,
};
use codec::{RequestEgress, ResponseIngress};

const REQUESTS: &[(&str, &str)] = &[
    (
        "claude-antigravity",
        include_str!("../fixtures/pairs/claude-antigravity.json"),
    ),
    ("claude-codex", include_str!("../fixtures/pairs/claude-codex.json")),
    ("claude-gemini", include_str!("../fixtures/pairs/claude-gemini.json")),
    (
        "claude-interactions",
        include_str!("../fixtures/pairs/claude-interactions.json"),
    ),
    ("claude-openai", include_str!("../fixtures/pairs/claude-openai.json")),
];
const RESPONSES: &[(&str, &str)] = &[
    ("openai-claude", include_str!("../fixtures/pairs/openai-claude.json")),
    (
        "openai-response-claude",
        include_str!("../fixtures/pairs/openai-response-claude.json"),
    ),
    ("gemini-claude", include_str!("../fixtures/pairs/gemini-claude.json")),
    (
        "interactions-claude",
        include_str!("../fixtures/pairs/interactions-claude.json"),
    ),
];
fn input(fixture: &Value) -> Vec<u8> {
    let text = fixture["input"].as_str().unwrap_or_default();
    if fixture["bytes"] == true {
        text.chars().map(|c| c as u32 as u8).collect()
    } else {
        text.as_bytes().to_vec()
    }
}
#[test]
fn recorded_request_inputs_preserve_semantics_or_fail_the_source_schema() {
    for (stem, corpus) in REQUESTS {
        let corpus: Value = serde_json::from_str(corpus).unwrap();
        for (index, fixture) in corpus["fixtures"].as_array().unwrap().iter().enumerate() {
            if fixture["path"] != "request" && fixture["path"] != "request_compat" {
                continue;
            }
            let raw = input(fixture);
            let value = serde_json::from_slice::<Value>(&raw).ok();
            let full = value.as_ref().is_some_and(|v| {
                v["model"].is_string() && v["max_tokens"].as_u64().is_some_and(|n| n > 0) && v["messages"].is_array()
            });
            let bad_choice = value.as_ref().is_some_and(|v| {
                v.get("tool_choice").is_some_and(|c| {
                    c.is_null() || c["type"] == "unknown_future_restriction" || (c["type"] == "tool" && c["name"] == "")
                })
            });
            let id = format!("{stem}.json / {} / occurrence {index}", fixture["name"]);
            if full && !bad_choice {
                let (encoded, canonical) = request_fragmented(&raw, Operation::Generate, raw.len());
                let mut expected = value.unwrap();
                expected["stream"] = json!(true);
                assert_eq!(encoded, expected, "{id}");
                assert!(canonical.iter().any(|event| event["operation"] == "generate"), "{id}");
                assert_eq!(canonical.last().unwrap()["x_ck"]["direction"], "request", "{id}");
            } else {
                let mut mapped = Mapped::new();
                let mut ingress = anthropic::request::Ingress::new(
                    &mut mapped.writer,
                    &mapped.store,
                    identity(Direction::Request),
                    Operation::Generate,
                    1024 * 1024,
                )
                .unwrap();
                let error = match ingress.advance(&mut mapped.writer, &raw) {
                    Err(error) => error,
                    Ok(()) => match ingress.finish(&mut mapped.writer, &anthropic::request::AnthropicCapability) {
                        Err(error) => error,
                        Ok(ready) => {
                            let output = anthropic::request::Egress {
                                destination: "anthropic",
                                upstream_stream: true,
                            }
                            .encode(&ready, &mut mapped.writer)
                            .unwrap();
                            panic!("{id}: invalid source authorized upstream bytes {:?}", bytes(&output));
                        }
                    },
                };
                assert!(
                    !matches!(error, codec::Error::Storage(_)),
                    "{id}: schema failure became storage failure"
                );
                let output = mapped.output(&mapped.writer);
                let text = std::str::from_utf8(&output).unwrap();
                assert!(!text.contains("event: x_ck.request.end\n"), "{id}");
                assert!(!text.contains("event: x_ck.done\n"), "{id}");
                assert_invalid_input(&output, "client");
            }
        }
    }
}
#[test]
fn recorded_complete_native_messages_preserve_blocks_and_usage() {
    for (stem, corpus) in RESPONSES {
        let corpus: Value = serde_json::from_str(corpus).unwrap();
        for (index, fixture) in corpus["fixtures"].as_array().unwrap().iter().enumerate() {
            if fixture["path"] != "non_stream" {
                continue;
            }
            let raw = input(fixture);
            let id = format!("{stem}.json / {} / occurrence {index}", fixture["name"]);
            let value = serde_json::from_slice::<Value>(&raw).ok();
            if !value
                .as_ref()
                .is_some_and(|v| v["content"].is_array() && v["stop_reason"].is_string())
            {
                reject_response(&raw, Operation::Generate, &id);
                continue;
            }
            let mut expected = value.unwrap();
            if expected.get("role").is_none() {
                expected["role"] = json!("assistant");
            }
            let (output, canonical) = response(
                &raw,
                BodyMode::Buffered,
                BodyMode::Buffered,
                Operation::Generate,
                raw.len(),
            );
            let output: Value = serde_json::from_slice(&output).unwrap();
            assert_eq!(output, expected, "{id}");
            let usage = canonical
                .iter()
                .filter_map(|event| event.get("usage"))
                .next_back()
                .unwrap();
            assert_eq!(usage["native"]["value"], expected["usage"], "{id}");
            assert_eq!(usage["final"], true, "{id}");
            let native = &expected["usage"];
            if let (Some(a), Some(b), Some(c)) = (
                native["input_tokens"].as_u64(),
                native["cache_read_input_tokens"].as_u64(),
                native["cache_creation_input_tokens"].as_u64(),
            ) {
                assert_eq!(usage["input_tokens"], a + b + c, "{id}");
            } else {
                assert!(
                    usage.get("input_tokens").is_none(),
                    "{id}: absent cache counts must not become zero"
                );
            }
        }
    }
}

fn reject_response(raw: &[u8], operation: Operation, id: &str) {
    let mut mapped = Mapped::new();
    let mut ingress = anthropic::response::Ingress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        operation,
        BodyMode::Buffered,
        1024 * 1024,
    )
    .unwrap();
    let result = ingress
        .advance(&mut mapped.writer, raw)
        .and_then(|()| ingress.finish(&mut mapped.writer));
    assert!(result.is_err(), "{id}: incomplete adapter input authorized a response");
    assert!(!matches!(result, Err(codec::Error::Storage(_))), "{id}");
    let raw = mapped.output(&mapped.writer);
    let output = std::str::from_utf8(&raw).unwrap();
    assert!(output.contains("event: x_ck.error\n"), "{id}");
    assert!(!output.contains("event: x_ck.done\n"), "{id}");
    assert_invalid_input(&raw, "upstream");
}

#[test]
fn count_fixtures_cannot_inject_counts_outside_the_native_body() {
    for (stem, corpus) in RESPONSES {
        let corpus: Value = serde_json::from_str(corpus).unwrap();
        for (index, fixture) in corpus["fixtures"].as_array().unwrap().iter().enumerate() {
            if fixture["path"] != "token_count" {
                continue;
            }
            let id = format!("{stem}.json / {} / occurrence {index}", fixture["name"]);
            reject_response(&input(fixture), Operation::CountTokens, &id);
        }
    }
}
