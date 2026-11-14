use super::*;

const RECORDING: &str = include_str!("../fixtures/pairs/interactions-openai-response.json");

#[test]
fn recorded_buffered_responses_preserve_items_usage_and_terminal_status() {
    let corpus: Value = serde_json::from_str(RECORDING).unwrap();
    for fixture in corpus["fixtures"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|fixture| fixture["path"] == "non_stream")
    {
        let raw = fixture["input"].as_str().unwrap();
        let parsed = serde_json::from_str::<Value>(raw).ok();
        let valid = parsed.as_ref().is_some_and(|body| {
            body["output"].as_array().is_some_and(|items| {
                items.iter().all(|item| {
                    if item["type"] == "function_call" {
                        item["arguments"]
                            .as_str()
                            .is_some_and(|text| serde_json::from_str::<Value>(text).is_ok())
                    } else {
                        true
                    }
                })
            }) && body.get("usage").is_none_or(|usage| {
                ["input_tokens", "output_tokens", "total_tokens"]
                    .iter()
                    .all(|key| usage.get(*key).is_none_or(|value| value.is_null() || value.is_u64()))
            })
        });
        if valid {
            let body = parsed.unwrap();
            for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
                let (output, canonical) = response(raw.as_bytes(), BodyMode::Buffered, downstream, Operation::Generate);
                let projected: Value = if downstream == BodyMode::Buffered {
                    serde_json::from_slice(&output).unwrap()
                } else {
                    std::str::from_utf8(&output)
                        .unwrap()
                        .lines()
                        .filter_map(|line| line.strip_prefix("data: "))
                        .map(|line| serde_json::from_str::<Value>(line).unwrap())
                        .next_back()
                        .unwrap()["response"]
                        .clone()
                };
                assert_eq!(projected["id"], body["id"], "{} id", fixture["name"]);
                assert_eq!(
                    projected.get("usage"),
                    body.get("usage"),
                    "{} usage presence",
                    fixture["name"]
                );
                assert_eq!(
                    &projected["status"],
                    body.get("status").unwrap_or(&json!("completed")),
                    "{} terminal",
                    fixture["name"]
                );
                for (index, item) in body["output"].as_array().unwrap().iter().enumerate() {
                    for (name, value) in item.as_object().unwrap() {
                        assert_eq!(
                            &projected["output"][index][name], value,
                            "{} item {index}/{name}",
                            fixture["name"]
                        );
                    }
                }
                if let Some(cached) = body["usage"]["input_tokens_details"].get("cached_tokens") {
                    let usage = canonical.iter().find(|event| event["usage"]["final"] == true).unwrap();
                    assert_eq!(
                        &usage["usage"]["cache_read_tokens"], cached,
                        "{} cache accounting",
                        fixture["name"]
                    );
                }
            }
        } else {
            assert_rejected(raw.as_bytes(), BodyMode::Buffered, fixture["name"].as_str().unwrap());
        }
    }
}

#[test]
fn recorded_streams_without_native_terminal_snapshots_never_succeed() {
    let corpus: Value = serde_json::from_str(RECORDING).unwrap();
    for fixture in corpus["fixtures"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|fixture| fixture["path"] == "stream")
    {
        let lines = fixture["lines"].as_array().unwrap();
        let has_terminal = lines.iter().filter_map(Value::as_str).any(|line| {
            let raw = line.strip_prefix("data:").unwrap_or(line).trim();
            serde_json::from_str::<Value>(raw)
                .is_ok_and(|event| event["type"] == "response.completed" || event["type"] == "response.incomplete")
        });
        if has_terminal {
            continue;
        }
        let mut wire = String::new();
        for line in lines.iter().filter_map(Value::as_str) {
            if !line.is_empty() && !line.starts_with("data:") && !line.starts_with("event:") && !line.starts_with(':') {
                wire.push_str("data: ");
            }
            wire.push_str(line);
            wire.push_str("\n\n");
        }
        assert_rejected(wire.as_bytes(), BodyMode::Streaming, fixture["name"].as_str().unwrap());
    }
}

fn assert_rejected(input: &[u8], mode: BodyMode, name: &str) {
    let mut m = Mapped::new();
    let mut ingress = responses::response::Ingress::new(
        &mut m.writer,
        identity(Direction::Response),
        Operation::Generate,
        mode,
        1024 * 1024,
    )
    .unwrap();
    let result = input
        .chunks(13)
        .try_for_each(|chunk| ingress.advance(&mut m.writer, chunk))
        .and_then(|()| ingress.finish(&mut m.writer));
    assert!(result.is_err(), "{name} accepted malformed or incomplete response");
    let wire = String::from_utf8(m.output(&m.writer)).unwrap();
    assert!(wire.contains("event: x_ck.error\n"), "{name} missing terminal error");
    assert!(!wire.contains("event: x_ck.done\n"), "{name} fabricated success");
}
