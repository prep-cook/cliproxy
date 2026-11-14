use super::{RESPONSES, anthropic, codec, identity, json};
use crate::{BodyMode, Direction, Mapped, Operation, ResponseIngress, Value, assert_invalid_input, response};
use std::collections::BTreeMap;

// These fixtures include adapter fragments, not just complete Messages streams.
fn expected(rows: &[Value]) -> Option<(Vec<Value>, Value)> {
    let mut started = false;
    let mut finished = false;
    let mut stopped = false;
    let mut blocks = Vec::new();
    let mut indices = BTreeMap::new();
    let mut arguments: BTreeMap<u64, String> = BTreeMap::new();
    let mut open = std::collections::BTreeSet::new();
    let mut reason = Value::Null;
    for row in rows {
        if stopped {
            return None;
        }
        match row["type"].as_str()? {
            "message_start" => {
                if started {
                    return None;
                }
                started = true;
            }
            "content_block_start" => {
                if !started || finished {
                    return None;
                }
                let index = row["index"].as_u64()?;
                if indices.insert(index, blocks.len()).is_some() {
                    return None;
                }
                let block = row["content_block"].clone();
                if matches!(block["type"].as_str(), Some("tool_use" | "server_tool_use")) {
                    block["id"].as_str()?;
                    block["name"].as_str()?;
                }
                blocks.push(block);
                open.insert(index);
            }
            "content_block_delta" => {
                let index = row["index"].as_u64()?;
                if !open.contains(&index) {
                    return None;
                }
                apply_delta(&mut blocks[*indices.get(&index)?], &mut arguments, index, &row["delta"])?;
            }
            "content_block_stop" => {
                let index = row["index"].as_u64()?;
                if !open.remove(&index) {
                    return None;
                }
                close_block(&mut blocks[*indices.get(&index)?], arguments.remove(&index))?;
            }
            "message_delta" => {
                if !started || !row["delta"].is_object() {
                    return None;
                }
                if row["delta"]["stop_reason"].is_string() {
                    if finished || !open.is_empty() {
                        return None;
                    }
                    reason = row["delta"]["stop_reason"].clone();
                    finished = true;
                }
            }
            "message_stop" => {
                if !started || !finished || !open.is_empty() {
                    return None;
                }
                stopped = true;
            }
            "error" => return None,
            _ => {}
        }
    }
    stopped.then_some((blocks, reason))
}

/// Applies one content block delta to the expected block; `None` for an invalid delta.
fn apply_delta(block: &mut Value, arguments: &mut BTreeMap<u64, String>, index: u64, delta: &Value) -> Option<()> {
    match delta["type"].as_str()? {
        "text_delta" | "thinking_delta" | "signature_delta" => {
            let field = match delta["type"].as_str()? {
                "text_delta" => "text",
                "thinking_delta" => "thinking",
                _ => "signature",
            };
            let mut text = block[field].as_str().unwrap_or_default().to_owned();
            text.push_str(delta[field].as_str()?);
            block[field] = json!(text);
        }
        "input_json_delta" => arguments
            .entry(index)
            .or_default()
            .push_str(delta["partial_json"].as_str()?),
        "citations_delta" => {
            if block.get("citations").is_none() {
                block["citations"] = json!([]);
            }
            block["citations"].as_array_mut()?.push(delta["citation"].clone());
        }
        _ => {}
    }
    Some(())
}

/// Completes a stopped block's tool input; `None` when a tool input is not an object.
fn close_block(block: &mut Value, arguments: Option<String>) -> Option<()> {
    if let Some(text) = arguments {
        block["input"] = serde_json::from_str(&text).ok()?;
    }
    if matches!(block["type"].as_str(), Some("tool_use" | "server_tool_use"))
        && block.get("input").is_some_and(|input| !input.is_object())
    {
        return None;
    }
    Some(())
}

#[test]
fn every_recorded_native_sse_stream_obeys_lifecycle_and_preserves_payloads() {
    for (stem, corpus) in RESPONSES {
        let corpus: Value = serde_json::from_str(corpus).unwrap();
        for (index, fixture) in corpus["fixtures"].as_array().unwrap().iter().enumerate() {
            if fixture["path"] != "stream" {
                continue;
            }
            let id = format!("{stem}.json / {} / occurrence {index}", fixture["name"]);
            let mut input = Vec::new();
            for line in fixture["lines"].as_array().unwrap() {
                input.extend_from_slice(line.as_str().unwrap().as_bytes());
                input.extend_from_slice(b"\n\n");
            }
            let rows: Vec<Value> = std::str::from_utf8(&input)
                .unwrap()
                .lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .map(|line| serde_json::from_str(line).unwrap_or_else(|_| json!({"type":"error"})))
                .collect();
            if let Some((blocks, reason)) = expected(&rows) {
                let (output, _) = response(
                    &input,
                    BodyMode::Streaming,
                    BodyMode::Buffered,
                    Operation::Generate,
                    input.len(),
                );
                let body: Value = serde_json::from_slice(&output).unwrap();
                assert_eq!(body["content"], json!(blocks), "{id}");
                assert_eq!(body["stop_reason"], reason, "{id}");
            } else {
                let mut mapped = Mapped::new();
                let mut ingress = anthropic::response::Ingress::new(
                    &mut mapped.writer,
                    identity(Direction::Response),
                    Operation::Generate,
                    BodyMode::Streaming,
                    1024 * 1024,
                )
                .unwrap();
                let result = ingress
                    .advance(&mut mapped.writer, &input)
                    .and_then(|()| ingress.finish(&mut mapped.writer));
                assert!(result.is_err(), "{id}: invalid source acquired success");
                assert!(
                    !matches!(result, Err(codec::Error::Storage(_))),
                    "{id}: source failure became storage exhaustion"
                );
                let raw = mapped.output(&mapped.writer);
                let output = std::str::from_utf8(&raw).unwrap();
                let terminal_prefix = rows
                    .iter()
                    .position(|row| row["type"] == "message_stop")
                    .is_some_and(|stop| expected(&rows[..=stop]).is_some());
                if terminal_prefix {
                    assert_eq!(output.matches("event: x_ck.done\n").count(), 1, "{id}");
                    assert!(
                        !output.contains("event: x_ck.error\n"),
                        "{id}: a late record added a second terminal"
                    );
                } else {
                    assert!(!output.contains("event: x_ck.done\n"), "{id}");
                    assert!(output.contains("event: x_ck.error\n"), "{id}");
                    assert_invalid_input(&raw, "upstream");
                }
            }
        }
    }
}
