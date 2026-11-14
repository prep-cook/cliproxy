use super::*;

/// Runs Chat `input` through Chat ingress and Responses egress; returns the client bytes.
fn chat_to_responses(input: &[u8], mode: BodyMode, downstream: BodyMode) -> Vec<u8> {
    let mut m = Mapped::new();
    let mut ingress = codec::chat::response::Ingress::new(
        &mut m.writer,
        &m.store,
        identity(Direction::Response),
        Operation::Generate,
        mode,
        1_000_000,
    )
    .unwrap();
    let mut reader = m.store.reader(m.writer.id(), 0).unwrap();
    let mut egress = responses::egress::Egress::new(
        &mut m.writer,
        identity(Direction::Response),
        Operation::Generate,
        downstream,
        |_, _| {},
    )
    .unwrap();
    for fragment in input.chunks(37) {
        if ingress.advance(&mut m.writer, fragment).is_err() {
            break;
        }
    }
    let _ = ingress.finish(&mut m.writer);
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
    output
}

/// Client SSE events as JSON values.
fn client_events(output: &[u8]) -> Vec<Value> {
    std::str::from_utf8(output)
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn sse(records: &[Value]) -> String {
    let mut input: String = records.iter().map(|record| format!("data: {record}\n\n")).collect();
    input.push_str("data: [DONE]\n\n");
    input
}

#[test]
fn chat_stream_projects_native_text_events_and_drops_other_choices() {
    let input = sse(&[
        json!({"id":"c","object":"chat.completion.chunk","model":"m","created":5,"choices":[{"index":0,"delta":{"role":"assistant","content":"hi"},"finish_reason":null}]}),
        json!({"id":"c","object":"chat.completion.chunk","model":"m","created":5,"choices":[{"index":1,"delta":{"role":"assistant","content":"other"},"finish_reason":null}]}),
        json!({"id":"c","object":"chat.completion.chunk","model":"m","created":5,"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
        json!({"id":"c","object":"chat.completion.chunk","model":"m","created":5,"choices":[{"index":1,"delta":{},"finish_reason":"stop"}]}),
    ]);
    let output = chat_to_responses(input.as_bytes(), BodyMode::Streaming, BodyMode::Streaming);
    assert!(
        !String::from_utf8_lossy(&output).contains("other"),
        "choice 1 reached a single-choice client"
    );
    let events = client_events(&output);
    let types: Vec<_> = events.iter().map(|event| event["type"].as_str().unwrap()).collect();
    for expected in [
        "response.created",
        "response.output_item.added",
        "response.content_part.added",
        "response.output_text.delta",
        "response.content_part.done",
        "response.output_item.done",
        "response.completed",
    ] {
        assert!(types.contains(&expected), "{expected} missing from {types:?}");
    }
    let created = &events[0]["response"];
    assert_eq!(
        (created["model"].clone(), created["created_at"].clone()),
        (json!("m"), json!(5))
    );
    let added = events
        .iter()
        .find(|e| e["type"] == "response.output_item.added")
        .unwrap();
    assert_eq!(added["item"]["type"], "message");
    let part = events
        .iter()
        .find(|e| e["type"] == "response.content_part.added")
        .unwrap();
    assert_eq!(part["part"]["type"], "output_text");
    let done = events
        .iter()
        .find(|e| e["type"] == "response.output_item.done")
        .unwrap();
    assert_eq!(done["item"]["status"], "completed");
    let response = &events.last().unwrap()["response"];
    assert_eq!(response["status"], "completed");
    assert_eq!(response["output"][0]["content"][0]["text"], "hi");
}

#[test]
fn failed_chat_stream_ends_with_response_failed() {
    let input = "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"},\"finish_reason\":null}]}\n\ndata: {broken}\n\n";
    let output = chat_to_responses(input.as_bytes(), BodyMode::Streaming, BodyMode::Streaming);
    let last = client_events(&output).pop().unwrap();
    assert_eq!(last["type"], "response.failed");
}

#[test]
fn chat_logprobs_without_a_block_join_the_open_text_part() {
    let logprobs = json!({"content":[{"token":"hi","logprob":-0.5,"bytes":[104,105],"top_logprobs":[]}]});
    let input = sse(&[
        json!({"id":"c","model":"m","created":5,"choices":[{"index":0,"delta":{"role":"assistant","content":"hi"},"logprobs":logprobs,"finish_reason":null}]}),
        json!({"id":"c","model":"m","created":5,"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
    ]);
    let output = chat_to_responses(input.as_bytes(), BodyMode::Streaming, BodyMode::Streaming);
    let events = client_events(&output);
    let delta = events
        .iter()
        .find(|e| e["type"] == "response.output_text.delta" && e.get("logprobs").is_some())
        .unwrap();
    assert_eq!(
        (delta["content_index"].clone(), delta["delta"].clone()),
        (json!(0), json!(""))
    );
    assert_eq!(delta["logprobs"], logprobs["content"]);
    let response = &events.last().unwrap()["response"];
    assert_eq!(response["status"], "completed");
    assert_eq!(response["output"][0]["content"][0]["logprobs"], logprobs["content"]);
}
#[test]
fn chat_tool_call_becomes_a_function_call_item() {
    let input = json!({"id":"r","model":"m","choices":[{"index":0,"message":{"role":"assistant","tool_calls":[{"id":"call-a","type":"function","function":{"name":"first","arguments":"{\"v\":1}"}}]},"finish_reason":"tool_calls"}]});
    let output = chat_to_responses(
        &serde_json::to_vec(&input).unwrap(),
        BodyMode::Buffered,
        BodyMode::Streaming,
    );
    let events = client_events(&output);
    let done = events
        .iter()
        .find(|e| e["type"] == "response.function_call_arguments.done")
        .unwrap();
    assert_eq!(done["arguments"], "{\"v\":1}");
    let output = &events.last().unwrap()["response"]["output"];
    let call = output
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "function_call")
        .unwrap();
    assert_eq!(call["type"], "function_call");
    assert_eq!(call["name"], "first");
    assert_eq!(call["call_id"], "call-a");
    assert_eq!(call["arguments"], "{\"v\":1}");
}

#[test]
fn chat_text_and_multiple_calls_remain_separate_output_items() {
    let input = json!({"id":"r","choices":[{"index":0,"message":{"role":"assistant","content":"lead","tool_calls":[{"id":"call-a","type":"function","function":{"name":"first","arguments":"{}"}},{"id":"call-b","type":"function","function":{"name":"second","arguments":"{\"v\":1}"}}]},"finish_reason":"tool_calls"}]});
    for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
        let output = chat_to_responses(&serde_json::to_vec(&input).unwrap(), BodyMode::Buffered, downstream);
        let out = if downstream == BodyMode::Buffered {
            serde_json::from_slice(&output).unwrap()
        } else {
            let events = client_events(&output);
            let added = events
                .iter()
                .filter(|e| e["type"] == "response.output_item.added" && e["item"]["type"] == "function_call")
                .count();
            let done = events
                .iter()
                .filter(|e| e["type"] == "response.output_item.done" && e["item"]["type"] == "function_call")
                .count();
            assert_eq!((added, done), (2, 2));
            events.last().unwrap()["response"].clone()
        };
        let items = out["output"].as_array().unwrap();
        let message = items.iter().find(|item| item["type"] == "message").unwrap();
        assert_eq!(message["content"][0]["text"], "lead");
        for (id, name, arguments) in [("call-a", "first", "{}"), ("call-b", "second", "{\"v\":1}")] {
            let call = items.iter().find(|item| item["call_id"] == id).unwrap();
            assert_eq!(call["type"], "function_call");
            assert_eq!(call["name"], name);
            assert_eq!(call["arguments"], arguments);
            assert_eq!(call["status"], "completed");
            assert_ne!(call["id"], message["id"]);
            assert!(call.get("content").is_none());
        }
    }
}

/// The final Responses object of a buffered body or a streamed `response.completed`.
fn final_response(output: &[u8], downstream: BodyMode) -> Value {
    if downstream == BodyMode::Buffered {
        return serde_json::from_slice(output).unwrap();
    }
    client_events(output)
        .into_iter()
        .find(|event| event["type"] == "response.completed")
        .unwrap()["response"]
        .clone()
}

#[test]
fn chat_usage_projects_native_token_details_without_control_fields() {
    let usage = json!({"prompt_tokens":0,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":0,"audio_tokens":2},"completion_tokens_details":{"reasoning_tokens":1,"audio_tokens":2,"rejected_prediction_tokens":0}});
    let input = json!({"id":"chat-r","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"x"},"finish_reason":"stop"}],"usage":usage});
    for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
        let output = chat_to_responses(&serde_json::to_vec(&input).unwrap(), BodyMode::Buffered, downstream);
        let out = final_response(&output, downstream);
        assert_eq!(
            out["usage"],
            json!({"input_tokens":0,"output_tokens":3,"total_tokens":3,"input_tokens_details":{"cached_tokens":0,"audio_tokens":2},"output_tokens_details":{"reasoning_tokens":1,"audio_tokens":2}})
        );
        let provenance = out["x_ck_opaque"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["name"] == "usage.snapshot")
            .unwrap();
        assert_eq!(provenance["value"]["native"]["value"], usage);
        assert!(provenance["value"]["final"].is_boolean());
    }
}

#[test]
fn corrected_usage_replaces_counts_and_preserves_native_null() {
    let input = sse(&[
        json!({"id":"r","choices":[{"index":0,"delta":{"content":"x"},"finish_reason":null}]}),
        json!({"id":"r","choices":[],"usage":{"prompt_tokens":7,"completion_tokens":3}}),
        json!({"id":"r","choices":[],"usage":{"prompt_tokens":null}}),
        json!({"id":"r","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
    ]);
    let output = chat_to_responses(input.as_bytes(), BodyMode::Streaming, BodyMode::Buffered);
    let out = final_response(&output, BodyMode::Buffered);
    assert_eq!(out["usage"], json!({"output_tokens":3}));
    let snapshots: Vec<_> = out["x_ck_opaque"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["name"] == "usage.snapshot")
        .collect();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0]["value"]["native"]["value"]["prompt_tokens"], Value::Null);
    assert_eq!(snapshots[0]["value"]["native"]["value"]["completion_tokens"], 3);
    let absent = json!({"choices":[{"index":0,"message":{"content":"x"},"finish_reason":"stop"}]});
    let output = chat_to_responses(
        &serde_json::to_vec(&absent).unwrap(),
        BodyMode::Buffered,
        BodyMode::Buffered,
    );
    assert!(final_response(&output, BodyMode::Buffered).get("usage").is_none());
}
