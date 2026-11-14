//! Responses ingress into the Chat and Anthropic egresses: a response id the source never sent
//! must not reach clients as the gateway's internal stream id.

// Test-only inclusion until the framework owner declares the production modules.
#[path = "../src/anthropic/mod.rs"]
pub mod anthropic;
#[path = "../src/boundary.rs"]
pub mod boundary;
#[path = "../src/codec/mod.rs"]
pub mod codec;
#[path = "../src/responses/mod.rs"]
pub mod responses;
#[path = "semantic/mod.rs"]
mod semantic;

use codec::{
    BodyMode, Operation, ResponseEgress, ResponseIngress,
    event::{Direction, Identity, LocalId},
};
use semantic::Mapped;
use serde_json::{Value, json};
use std::task::Poll;

fn identity() -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction: Direction::Response,
    }
}

fn bytes(lease: &ck_stream::ScratchLease) -> Vec<u8> {
    let mut out = Vec::new();
    while out.len() < lease.len() {
        out.extend_from_slice(lease.chunk(out.len()).unwrap());
    }
    out
}

/// A completed Responses body, with the response id `id` when given.
fn body(id: Option<&str>) -> Vec<u8> {
    let mut body = json!({"model":"m","status":"completed","output":[{"id":"i","type":"message","role":"assistant","content":[{"type":"output_text","text":"hi","annotations":[]}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}});
    if let Some(id) = id {
        body["id"] = json!(id);
    }
    serde_json::to_vec(&body).unwrap()
}

/// Runs a buffered Responses body through Responses ingress and `egress`; returns the client
/// bytes and the writer id the egress derives its own ids from.
fn translate<E: ResponseEgress>(input: &[u8], egress: impl FnOnce(&mut ck_stream::Writer) -> E) -> (Vec<u8>, u64) {
    let mut m = Mapped::new();
    let mut ingress = responses::response::Ingress::new(
        &mut m.writer,
        identity(),
        Operation::Generate,
        BodyMode::Buffered,
        1024 * 1024,
    )
    .unwrap();
    let mut reader = m.store.reader(m.writer.id(), 0).unwrap();
    let mut egress = egress(&mut m.writer);
    for chunk in input.chunks(13) {
        ingress.advance(&mut m.writer, chunk).unwrap();
    }
    ingress.finish(&mut m.writer).unwrap();
    let mut output = Vec::new();
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        egress
            .advance(&mut m.writer, lease, |part| {
                output.extend_from_slice(&bytes(&part));
                Ok(())
            })
            .unwrap();
    }
    egress.finish().unwrap();
    // The premise: ingress fills the missing id itself and marks it generated.
    let canonical = String::from_utf8(m.output(&m.writer)).unwrap();
    assert_eq!(
        canonical.contains("\"synthetic\":[\"id\""),
        !String::from_utf8_lossy(input).contains("\"id\":\"resp_"),
        "{canonical}"
    );
    (output, m.writer.id())
}

fn chat(input: &[u8], mode: BodyMode) -> (Vec<u8>, u64) {
    translate(input, |writer| {
        codec::chat::egress::Egress::new(writer, identity(), Operation::Generate, mode, true, |_| {}).unwrap()
    })
}

fn anthropic(input: &[u8], mode: BodyMode) -> (Vec<u8>, u64) {
    translate(input, |writer| {
        anthropic::egress::Egress::new(writer, identity(), Operation::Generate, mode, |_| {}).unwrap()
    })
}

/// The JSON values of every SSE `data:` line, skipping the `[DONE]` sentinel.
fn data(output: &[u8]) -> Vec<Value> {
    std::str::from_utf8(output)
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter(|line| *line != "[DONE]")
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// The Chat chunks among the streamed records; source fields without a carrier pass as their
/// own events.
fn chunks(output: &[u8]) -> Vec<Value> {
    let chunks: Vec<_> = data(output)
        .into_iter()
        .filter(|record| record["object"] == "chat.completion.chunk")
        .collect();
    assert!(chunks.len() > 1, "{chunks:?}");
    chunks
}

#[test]
fn chat_buffered_body_gets_its_own_completion_id() {
    let (output, writer) = chat(&body(None), BodyMode::Buffered);
    let completion: Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(completion["id"], format!("chatcmpl-s{writer}"));
    assert_eq!(completion["choices"][0]["message"]["content"], "hi");
}

#[test]
fn chat_stream_chunks_share_one_generated_completion_id() {
    let (output, writer) = chat(&body(None), BodyMode::Streaming);
    for chunk in chunks(&output) {
        assert_eq!(chunk["id"], format!("chatcmpl-s{writer}"), "{chunk}");
    }
    assert!(chunks(&output).iter().any(|chunk| chunk["usage"]["total_tokens"] == 2));
}

#[test]
fn chat_keeps_a_supplied_response_id() {
    let (output, _) = chat(&body(Some("resp_a")), BodyMode::Buffered);
    let completion: Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(completion["id"], "resp_a");
    let (output, _) = chat(&body(Some("resp_a")), BodyMode::Streaming);
    for chunk in chunks(&output) {
        assert_eq!(chunk["id"], "resp_a", "{chunk}");
    }
}

#[test]
fn anthropic_buffered_message_gets_its_own_id() {
    let (output, writer) = anthropic(&body(None), BodyMode::Buffered);
    let message: Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(message["id"], format!("msg_s{writer}"));
    assert_eq!(message["content"][0]["text"], "hi");
}

#[test]
fn anthropic_stream_message_start_gets_its_own_id() {
    let (output, writer) = anthropic(&body(None), BodyMode::Streaming);
    let events = data(&output);
    let start = events.iter().find(|e| e["type"] == "message_start").unwrap();
    assert_eq!(start["message"]["id"], format!("msg_s{writer}"));
    assert!(
        !String::from_utf8_lossy(&output).contains("\"stream\""),
        "internal stream id reached the client"
    );
}

#[test]
fn anthropic_keeps_a_supplied_response_id() {
    let (output, _) = anthropic(&body(Some("resp_a")), BodyMode::Buffered);
    let message: Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(message["id"], "resp_a");
    let (output, _) = anthropic(&body(Some("resp_a")), BodyMode::Streaming);
    let events = data(&output);
    let start = events.iter().find(|e| e["type"] == "message_start").unwrap();
    assert_eq!(start["message"]["id"], "resp_a");
}
