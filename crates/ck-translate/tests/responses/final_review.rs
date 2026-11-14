//! Reproductions from the final codec review: each test pins one corrected mapping.
use super::cross::{chat_to_responses, client_events, sse};
use super::*;

/// Buffered Responses `body` replayed to a streaming client; returns the client events.
fn streamed(body: &Value) -> Vec<Value> {
    let (out, _) = response(
        body.to_string().as_bytes(),
        BodyMode::Buffered,
        BodyMode::Streaming,
        Operation::Generate,
    );
    client_events(&out)
}

fn named<'a>(events: &'a [Value], name: &str) -> Vec<&'a Value> {
    events.iter().filter(|e| e["type"] == name).collect()
}

#[test]
fn announced_parts_and_calls_carry_their_empty_accumulators() {
    let text = json!({"id":"r","status":"completed","output":[{"id":"i","type":"message","role":"assistant",
        "content":[{"type":"output_text","text":"hi","annotations":[]},{"type":"refusal","refusal":"no"}]}]});
    let events = streamed(&text);
    let parts = named(&events, "response.content_part.added");
    assert_eq!(parts[0]["part"]["text"], "");
    assert_eq!(parts[1]["part"], json!({"type":"refusal","refusal":""}));
    let call = json!({"id":"r","status":"completed","output":[
        {"id":"i","type":"function_call","name":"f","call_id":"c","arguments":"{}"},
        {"id":"j","type":"custom_tool_call","name":"g","call_id":"d","input":"x"}]});
    let events = streamed(&call);
    let added = named(&events, "response.output_item.added");
    assert_eq!(added[0]["item"]["arguments"], "");
    assert_eq!(added[1]["item"]["input"], "");
    let chat = sse(&[
        json!({"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call","type":"function","function":{"name":"f","arguments":"{}"}}]},"finish_reason":null}]}),
        json!({"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}),
    ]);
    let events = client_events(&chat_to_responses(
        chat.as_bytes(),
        BodyMode::Streaming,
        BodyMode::Streaming,
    ));
    let added = named(&events, "response.output_item.added");
    let call = added.iter().find(|e| e["item"]["type"] == "function_call").unwrap();
    assert_eq!(call["item"]["arguments"], "");
    let text = sse(&[
        json!({"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":"hi"},"finish_reason":null}]}),
        json!({"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
    ]);
    let events = client_events(&chat_to_responses(
        text.as_bytes(),
        BodyMode::Streaming,
        BodyMode::Streaming,
    ));
    let part = named(&events, "response.content_part.added");
    assert_eq!(
        part[0]["part"],
        json!({"type":"output_text","text":"","annotations":[]})
    );
}
