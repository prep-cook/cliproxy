use super::*;

const GROUPS: &[(&str, &str)] = &[
    (
        "openai-response-codex",
        include_str!("../fixtures/pairs/openai-response-codex.json"),
    ),
    (
        "openai-response-openai",
        include_str!("../fixtures/pairs/openai-response-openai.json"),
    ),
    (
        "openai-response-claude",
        include_str!("../fixtures/pairs/openai-response-claude.json"),
    ),
    (
        "openai-response-gemini",
        include_str!("../fixtures/pairs/openai-response-gemini.json"),
    ),
    (
        "openai-response-interactions",
        include_str!("../fixtures/pairs/openai-response-interactions.json"),
    ),
    (
        "openai-response-antigravity",
        include_str!("../fixtures/pairs/openai-response-antigravity.json"),
    ),
];

#[test]
fn go_responses_requests_preserve_sampling_roles_and_unknowns() {
    let mut accepted = 0;
    for (group, recording) in GROUPS {
        let corpus: Value = serde_json::from_str(recording).unwrap();
        for fixture in corpus["fixtures"].as_array().unwrap() {
            if fixture["path"] != "request" {
                continue;
            }
            let Some(raw) = fixture["input"].as_str() else { continue };
            let parsed = serde_json::from_str::<Value>(raw);
            let mut m = Mapped::new();
            let mut ingress = responses::request::Ingress::new(
                &mut m.writer,
                &m.store,
                identity(Direction::Request),
                Operation::Generate,
                1024 * 1024,
            )
            .unwrap();
            let result = ingress.advance(&mut m.writer, raw.as_bytes()).and_then(|()| {
                let ready = ingress.finish(&mut m.writer, &responses::request::ResponsesCapability)?;
                responses::request::Egress {
                    destination: "responses",
                    upstream_stream: false,
                }
                .encode(&ready, &mut m.writer)
            });
            let valid_root = parsed.as_ref().is_ok_and(|v| v.is_object() && v["model"].is_string()) && unique_keys(raw);
            if !valid_root {
                assert!(
                    result.is_err(),
                    "{group}/{} accepted malformed or model-less request",
                    fixture["name"]
                );
                assert!(
                    !String::from_utf8(m.output(&m.writer))
                        .unwrap()
                        .contains("event: x_ck.request.end\n")
                );
                continue;
            }
            let input = parsed.unwrap();
            let Ok(encoded) = result else {
                // Historical malformed tool arguments and unlinked results now fail the fence.
                let invalid_history = input["input"].as_array().is_some_and(|items| {
                    items.iter().any(|item| {
                        (item["type"] == "function_call"
                            && item
                                .get("arguments")
                                .is_some_and(|a| a.as_str().is_none_or(|s| serde_json::from_str::<Value>(s).is_err())))
                            || ((item["type"] == "function_call_output" || item["type"] == "custom_tool_call_output")
                                && !items.iter().any(|call| {
                                    (call["type"] == "function_call" || call["type"] == "custom_tool_call")
                                        && call["call_id"] == item["call_id"]
                                }))
                            || ((item["type"] == "function_call_output" || item["type"] == "custom_tool_call_output")
                                && ((!item["output"].is_string() && !item["output"].is_array())
                                    || item["output"].as_array().is_some_and(|parts| {
                                        parts.iter().any(|part| {
                                            !part["type"].is_string()
                                                || (part["type"] == "input_image"
                                                    && part.get("image_url").is_some_and(|url| !url.is_string()))
                                        })
                                    })))
                            || ["content", "output"].iter().any(|field| {
                                item[*field]
                                    .as_array()
                                    .is_some_and(|parts| parts.iter().any(invalid_part))
                            })
                    })
                });
                assert!(
                    invalid_history,
                    "{group}/{} unexpectedly failed: {:?}",
                    fixture["name"],
                    result.err()
                );
                continue;
            };
            let output: Value = serde_json::from_slice(&bytes(&encoded)).unwrap();
            accepted += 1;
            for field in [
                "model",
                "temperature",
                "top_p",
                "metadata",
                "store",
                "user",
                "service_tier",
                "parallel_tool_calls",
                "prompt_cache_retention",
                "instructions",
            ] {
                if let Some(value) = input.get(field) {
                    assert_eq!(output.get(field), Some(value), "{group}/{}: {field}", fixture["name"]);
                }
            }
            if let Some(items) = input["input"].as_array() {
                let roles: Vec<_> = items.iter().filter_map(|item| item.get("role")).collect();
                let output_roles: Vec<_> = output["input"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter_map(|item| item.get("role"))
                    .collect();
                assert_eq!(output_roles, roles, "{group}/{} roles", fixture["name"]);
            }
            let emitted = events(&m);
            assert_eq!(emitted.last().unwrap()["x_ck"]["direction"], "request");
            assert_eq!(output["stream"], false);
        }
    }
    assert!(accepted > 0);
}

fn invalid_part(part: &Value) -> bool {
    use base64::Engine;
    if part["type"] == "input_image" && !part["file_id"].is_string() && !part["image_url"].is_string() {
        return true;
    }
    if part["type"] == "input_file"
        && !["file_id", "file_url", "file_data"]
            .iter()
            .any(|name| part[*name].is_string())
    {
        return true;
    }
    ["image_url", "file_data"].iter().any(|field| {
        let Some(value) = part[*field].as_str() else {
            return false;
        };
        if let Some(uri) = value.strip_prefix("data:") {
            return uri
                .split_once(";base64,")
                .is_none_or(|(_, data)| base64::engine::general_purpose::STANDARD.decode(data).is_err());
        }
        *field == "file_data" && base64::engine::general_purpose::STANDARD.decode(value).is_err()
    })
}

fn unique_keys(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    let mut objects = Vec::<std::collections::HashSet<String>>::new();
    let mut at = 0;
    while at < bytes.len() {
        match bytes[at] {
            b'{' => objects.push(std::collections::HashSet::new()),
            b'}' => {
                objects.pop();
            }
            b'"' => {
                let start = at;
                at += 1;
                while at < bytes.len() && bytes[at] != b'"' {
                    if bytes[at] == b'\\' {
                        at += 1;
                    }
                    at += 1;
                }
                if at == bytes.len() {
                    return false;
                }
                let end = at + 1;
                let mut next = end;
                while next < bytes.len() && bytes[next].is_ascii_whitespace() {
                    next += 1;
                }
                if bytes.get(next) == Some(&b':') {
                    let name: String = serde_json::from_str(&raw[start..end]).unwrap();
                    if !objects.last_mut().unwrap().insert(name) {
                        return false;
                    }
                }
            }
            _ => (),
        }
        at += 1;
    }
    true
}
