use super::{Mapped, Operation, codec, identity, response_fragmented};
use codec::{BodyMode, Error, ResponseIngress, event::Direction};
use serde_json::{Value, json};

/// Runs ingress alone and returns its first error with the canonical records published so far.
fn ingest(input: &[u8], upstream: BodyMode) -> (codec::Result<()>, Vec<Value>) {
    let mut mapped = Mapped::new();
    let mut ingress = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        upstream,
        1024 * 1024,
    )
    .unwrap();
    let mut result = Ok(());
    for chunk in input.chunks(512) {
        result = ingress.advance(&mut mapped.writer, chunk);
        if result.is_err() {
            break;
        }
    }
    if result.is_ok() {
        result = ingress.finish(&mut mapped.writer);
    }
    let output = mapped.output(&mapped.writer);
    let events = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    (result, events)
}

fn stream(events: &[Value]) -> Vec<u8> {
    let mut wire = String::new();
    for event in events {
        wire.push_str(&format!("data: {event}\n\n"));
    }
    wire.push_str("data: [DONE]\n\n");
    wire.into_bytes()
}

fn content(events: &[Value], choice: u64) -> String {
    events
        .iter()
        .flat_map(|e| e["choices"].as_array().into_iter().flatten())
        .filter(|c| c["index"] == choice)
        .filter_map(|c| c["delta"]["content"].as_str())
        .collect()
}

fn finished(events: &[Value]) -> usize {
    events
        .iter()
        .flat_map(|e| e["choices"].as_array().into_iter().flatten())
        .filter(|c| c["finish_reason"].is_string())
        .count()
}

#[test]
fn buffered_choice_without_finish_stops_before_later_choices() {
    for finish in [json!({}), json!({"finish_reason": null})] {
        let mut first = json!({"index":0,"message":{"role":"assistant","content":"first"}});
        first
            .as_object_mut()
            .unwrap()
            .extend(finish.as_object().unwrap().clone());
        let body = json!({"id":"r","choices":[first,{"index":1,"message":{"role":"assistant","content":"second"},"finish_reason":"stop"}]});
        let (result, events) = ingest(body.to_string().as_bytes(), BodyMode::Buffered);
        assert!(matches!(result, Err(Error::Truncated)), "{body}: {result:?}");
        assert_eq!(content(&events, 0), "first");
        assert_eq!(content(&events, 1), "", "{body}");
    }
    let body = json!({"id":"r","choices":[{"index":0,"message":{"content":"first"},"finish_reason":"stop"}]});
    assert!(ingest(body.to_string().as_bytes(), BodyMode::Buffered).0.is_ok());
}

#[test]
fn a_choice_cannot_finish_twice_in_one_record() {
    let streamed = stream(&[
        json!({"id":"r","choices":[{"index":0,"delta":{"role":"assistant","content":"a"},"finish_reason":"stop"},{"index":0,"delta":{},"finish_reason":"length"}]}),
    ]);
    let buffered = json!({"id":"r","choices":[
        {"index":0,"message":{"role":"assistant","content":"a"},"finish_reason":"stop"},
        {"index":0,"message":{"content":"b"},"finish_reason":"length"}
    ]})
    .to_string()
    .into_bytes();
    for (input, mode) in [(streamed, BodyMode::Streaming), (buffered, BodyMode::Buffered)] {
        let (result, events) = ingest(&input, mode);
        assert!(matches!(result, Err(Error::Schema)), "{mode:?}: {result:?}");
        assert_eq!(finished(&events), 1, "{mode:?}");
        assert_eq!(content(&events, 0), "a", "{mode:?}");
    }
}

#[test]
fn snapshot_rejects_a_repeated_choice_index_in_one_record() {
    let wire = stream(&[
        json!({"id":"r","choices":[{"index":0,"delta":{"role":"assistant","content":"a"}}]}),
        json!({"choices":[{"index":0,"message":{"content":"ab"}},{"index":0,"message":{"content":"abc"}}]}),
        json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
    ]);
    let (result, events) = ingest(&wire, BodyMode::Streaming);
    assert!(matches!(result, Err(Error::Schema)), "{result:?}");
    assert_eq!(content(&events, 0), "a");
}

#[test]
fn snapshot_array_member_publishes_only_its_new_entries() {
    let wire = stream(&[
        json!({"id":"r","choices":[{"index":0,"delta":{"role":"assistant","content":"a","vendor":[1]}}]}),
        json!({"choices":[{"index":0,"message":{"content":"a","vendor":[1,2]}}]}),
        json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
    ]);
    let (output, _) = response_fragmented(&wire, BodyMode::Streaming, BodyMode::Buffered, 512);
    let output: Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(output["choices"][0]["message"]["vendor"], json!([1, 2]));
    assert_eq!(output["choices"][0]["message"]["content"], "a");
}

#[test]
fn tool_calls_must_be_an_array_even_when_empty() {
    let streamed_object = stream(&[
        json!({"id":"r","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":{}},"finish_reason":"stop"}]}),
    ]);
    let buffered_object =
        json!({"id":"r","choices":[{"index":0,"message":{"role":"assistant","tool_calls":{}},"finish_reason":"stop"}]})
            .to_string()
            .into_bytes();
    for (input, mode) in [
        (streamed_object, BodyMode::Streaming),
        (buffered_object, BodyMode::Buffered),
    ] {
        let (result, events) = ingest(&input, mode);
        assert!(matches!(result, Err(Error::Schema)), "{result:?}");
        assert_eq!(finished(&events), 0);
    }
}
