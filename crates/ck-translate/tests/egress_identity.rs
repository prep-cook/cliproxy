//! Response ids at egress: a response id the source never sent must not reach clients as the
//! gateway's internal stream id, and a buffered body carries a source id that arrived late.

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

/// Where the upstream bytes come from.
#[derive(Clone, Copy)]
enum Source {
    /// A Responses API body or event stream.
    Responses(BodyMode),
    /// An Anthropic Messages event stream.
    Anthropic,
    /// A Chat Completions chunk stream.
    Chat,
}

/// Client egress under test.
#[derive(Clone, Copy)]
enum Client {
    Chat,
    Anthropic,
    Responses,
}

/// One translation: client bytes, the writer id each egress derives its own ids from, and the
/// canonical stream between them.
struct Run {
    output: Vec<u8>,
    writer: u64,
    canonical: String,
}

/// Feeds an ingress one fragment, or finishes it on `None`.
type Feed = Box<dyn FnMut(&mut ck_stream::Writer, Option<&[u8]>)>;

fn run(source: Source, input: &[u8], client: Client, mode: BodyMode) -> Run {
    let mut m = Mapped::new();
    let mut ingress: Feed = match source {
        Source::Responses(upstream) => {
            let mut ingress =
                responses::response::Ingress::new(&mut m.writer, identity(), Operation::Generate, upstream, 1 << 20)
                    .unwrap();
            Box::new(move |writer, chunk| match chunk {
                Some(chunk) => ingress.advance(writer, chunk).unwrap(),
                None => ingress.finish(writer).unwrap(),
            })
        }
        Source::Anthropic => {
            let mut ingress = anthropic::response::Ingress::new(
                &mut m.writer,
                identity(),
                Operation::Generate,
                BodyMode::Streaming,
                1 << 20,
            )
            .unwrap();
            Box::new(move |writer, chunk| match chunk {
                Some(chunk) => ingress.advance(writer, chunk).unwrap(),
                None => ingress.finish(writer).unwrap(),
            })
        }
        Source::Chat => {
            let mut ingress = codec::chat::response::Ingress::new(
                &mut m.writer,
                &m.store,
                identity(),
                Operation::Generate,
                BodyMode::Streaming,
                1 << 20,
            )
            .unwrap();
            Box::new(move |writer, chunk| match chunk {
                Some(chunk) => ingress.advance(writer, chunk).unwrap(),
                None => ingress.finish(writer).unwrap(),
            })
        }
    };
    let mut reader = m.store.reader(m.writer.id(), 0).unwrap();
    let mut egress: Box<dyn ResponseEgressDyn> = match client {
        Client::Chat => Box::new(
            codec::chat::egress::Egress::new(&mut m.writer, identity(), Operation::Generate, mode, true, |_| {})
                .unwrap(),
        ),
        Client::Anthropic => Box::new(
            anthropic::egress::Egress::new(&mut m.writer, identity(), Operation::Generate, mode, |_| {}).unwrap(),
        ),
        Client::Responses => {
            Box::new(responses::egress::Egress::new(&mut m.writer, Operation::Generate, mode, |_, _| {}).unwrap())
        }
    };
    for chunk in input.chunks(13) {
        ingress(&mut m.writer, Some(chunk));
    }
    ingress(&mut m.writer, None);
    let mut output = Vec::new();
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        egress.advance(&mut m.writer, lease, &mut output);
    }
    egress.finish();
    Run {
        output,
        writer: m.writer.id(),
        canonical: String::from_utf8(m.output(&m.writer)).unwrap(),
    }
}

/// Object-safe view of a response egress that appends its output to a buffer.
trait ResponseEgressDyn {
    fn advance(&mut self, writer: &mut ck_stream::Writer, lease: ck_stream::Lease, output: &mut Vec<u8>);
    fn finish(&mut self);
}
impl<E: ResponseEgress> ResponseEgressDyn for E {
    fn advance(&mut self, writer: &mut ck_stream::Writer, lease: ck_stream::Lease, output: &mut Vec<u8>) {
        ResponseEgress::advance(self, writer, lease, |part| {
            output.extend_from_slice(&bytes(&part));
            Ok(())
        })
        .unwrap();
    }
    fn finish(&mut self) {
        ResponseEgress::finish(self).unwrap();
    }
}

/// Runs a buffered Responses body through Responses ingress and the `client` egress.
fn translate(input: &[u8], client: Client, mode: BodyMode) -> (Vec<u8>, u64) {
    let result = run(Source::Responses(BodyMode::Buffered), input, client, mode);
    // The premise: ingress fills the missing id itself and marks it generated.
    assert_eq!(
        result.canonical.contains("\"synthetic\":[\"id\""),
        !String::from_utf8_lossy(input).contains("\"id\":\"resp_"),
        "{}",
        result.canonical
    );
    (result.output, result.writer)
}

fn chat(input: &[u8], mode: BodyMode) -> (Vec<u8>, u64) {
    translate(input, Client::Chat, mode)
}

fn anthropic(input: &[u8], mode: BodyMode) -> (Vec<u8>, u64) {
    translate(input, Client::Anthropic, mode)
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

/// A Responses stream whose id arrives only with its terminal snapshot.
fn late_id_stream() -> Vec<u8> {
    let records = [
        json!({"type":"response.created","response":{"model":"m","status":"in_progress","output":[]}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"i","type":"message","role":"assistant","status":"in_progress","content":[]}}),
        json!({"type":"response.content_part.added","item_id":"i","output_index":0,"content_index":0,"part":{"type":"output_text","text":"","annotations":[]}}),
        json!({"type":"response.output_text.delta","item_id":"i","output_index":0,"content_index":0,"delta":"hi"}),
        json!({"type":"response.output_text.done","item_id":"i","output_index":0,"content_index":0,"text":"hi"}),
        json!({"type":"response.content_part.done","item_id":"i","output_index":0,"content_index":0,"part":{"type":"output_text","text":"hi","annotations":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"id":"i","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"hi","annotations":[]}]}}),
        json!({"type":"response.completed","response":{"id":"resp_late","model":"m","status":"completed","output":[{"id":"i","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"hi","annotations":[]}]}]}}),
    ];
    records
        .iter()
        .map(|record| format!("event: {}\ndata: {record}\n\n", record["type"].as_str().unwrap()))
        .collect::<String>()
        .into_bytes()
}

/// The response id of a buffered body from `late_id_stream`.
fn late_id_body(client: Client) -> Value {
    let result = run(
        Source::Responses(BodyMode::Streaming),
        &late_id_stream(),
        client,
        BodyMode::Buffered,
    );
    serde_json::from_slice::<Value>(&result.output).unwrap()["id"].clone()
}

#[test]
fn chat_buffered_body_takes_a_late_source_id_and_streaming_keeps_its_own() {
    assert_eq!(late_id_body(Client::Chat), "resp_late");
    let streamed = run(
        Source::Responses(BodyMode::Streaming),
        &late_id_stream(),
        Client::Chat,
        BodyMode::Streaming,
    );
    for chunk in chunks(&streamed.output) {
        assert_eq!(chunk["id"], format!("chatcmpl-s{}", streamed.writer), "{chunk}");
    }
}

#[test]
fn anthropic_buffered_body_takes_a_late_source_id_and_streaming_keeps_its_own() {
    assert_eq!(late_id_body(Client::Anthropic), "resp_late");
    let streamed = run(
        Source::Responses(BodyMode::Streaming),
        &late_id_stream(),
        Client::Anthropic,
        BodyMode::Streaming,
    );
    let events = data(&streamed.output);
    let start = events.iter().find(|e| e["type"] == "message_start").unwrap();
    assert_eq!(start["message"]["id"], format!("msg_s{}", streamed.writer));
}

#[test]
fn responses_buffered_body_takes_a_late_source_id_and_streaming_keeps_its_own() {
    assert_eq!(late_id_body(Client::Responses), "resp_late");
    let streamed = run(
        Source::Responses(BodyMode::Streaming),
        &late_id_stream(),
        Client::Responses,
        BodyMode::Streaming,
    );
    let events = data(&streamed.output);
    assert!(events.len() > 2, "{events:?}");
    for event in events.iter().filter(|event| event.get("response").is_some()) {
        assert_eq!(event["response"]["id"], format!("resp_s{}", streamed.writer), "{event}");
    }
}

/// An Anthropic stream, after a leading ping when `ping`, whose message carries `id` when given.
fn anthropic_stream(ping: bool, id: Option<&str>) -> Vec<u8> {
    let mut message = json!({"type":"message","role":"assistant","model":"claude","content":[],"usage":{"input_tokens":1,"output_tokens":0}});
    if let Some(id) = id {
        message["id"] = json!(id);
    }
    let mut records = vec![
        json!({"type":"message_start","message":message}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":1}}),
        json!({"type":"message_stop"}),
    ];
    if ping {
        records.insert(0, json!({"type":"ping"}));
    }
    records
        .iter()
        .map(|record| format!("event: {}\ndata: {record}\n\n", record["type"].as_str().unwrap()))
        .collect::<String>()
        .into_bytes()
}

/// The ids a Chat client receives for an Anthropic `input`: the buffered body's, then every
/// streamed chunk's, each with the id the egress would issue in that run.
fn chat_ids_from_anthropic(input: &[u8]) -> ((Value, Value), (Vec<Value>, Value)) {
    let buffered = run(Source::Anthropic, input, Client::Chat, BodyMode::Buffered);
    let body: Value = serde_json::from_slice(&buffered.output).unwrap();
    let streamed = run(Source::Anthropic, input, Client::Chat, BodyMode::Streaming);
    let ids = chunks(&streamed.output)
        .iter()
        .map(|chunk| chunk["id"].clone())
        .collect();
    (
        (body["id"].clone(), json!(format!("chatcmpl-s{}", buffered.writer))),
        (ids, json!(format!("chatcmpl-s{}", streamed.writer))),
    )
}

#[test]
fn anthropic_source_without_an_id_gets_a_chat_completion_id() {
    // The premise: ingress marks the stream id it fills in, on the start and, after a ping, on
    // the chunk that refreshes the message identity.
    let canonical = run(
        Source::Anthropic,
        &anthropic_stream(true, None),
        Client::Chat,
        BodyMode::Buffered,
    )
    .canonical;
    assert!(canonical.contains("\"synthetic\":[\"created\",\"id\"]"), "{canonical}");
    assert!(canonical.contains("\"synthetic\":[\"id\"]"), "{canonical}");
    for ping in [false, true] {
        let ((body, issued), (chunks, streamed)) = chat_ids_from_anthropic(&anthropic_stream(ping, None));
        assert_eq!(body, issued, "ping {ping}");
        assert!(chunks.iter().all(|id| *id == streamed), "ping {ping}: {chunks:?}");
    }
}

#[test]
fn anthropic_source_id_after_a_ping_reaches_chat_clients() {
    let ((body, _), (chunks, _)) = chat_ids_from_anthropic(&anthropic_stream(true, Some("msg_real")));
    assert_eq!(body, "msg_real");
    assert!(chunks.iter().all(|id| *id == "msg_real"), "{chunks:?}");
}

/// A Chat chunk stream whose chunks carry no id.
fn chat_stream_without_id() -> Vec<u8> {
    let records = [
        json!({"object":"chat.completion.chunk","model":"m","created":5,"choices":[{"index":0,"delta":{"role":"assistant","content":"hi"},"finish_reason":null}]}),
        json!({"object":"chat.completion.chunk","model":"m","created":5,"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
    ];
    let mut input: String = records.iter().map(|record| format!("data: {record}\n\n")).collect();
    input.push_str("data: [DONE]\n\n");
    input.into_bytes()
}

#[test]
fn chat_source_without_an_id_gets_native_ids() {
    let input = chat_stream_without_id();
    for (client, prefix) in [(Client::Anthropic, "msg_s"), (Client::Responses, "resp_s")] {
        let buffered = run(Source::Chat, &input, client, BodyMode::Buffered);
        let body: Value = serde_json::from_slice(&buffered.output).unwrap();
        assert_eq!(body["id"], format!("{prefix}{}", buffered.writer));
        let streamed = run(Source::Chat, &input, client, BodyMode::Streaming);
        let events = data(&streamed.output);
        let ids: Vec<_> = events
            .iter()
            .filter_map(|event| event.get("message").or(event.get("response")))
            .map(|carrier| carrier["id"].clone())
            .collect();
        assert!(!ids.is_empty(), "{events:?}");
        assert!(
            ids.iter().all(|id| *id == format!("{prefix}{}", streamed.writer)),
            "{ids:?}"
        );
    }
    // The premise: ingress marks the stream id it fills in.
    let canonical = run(Source::Chat, &input, Client::Anthropic, BodyMode::Buffered).canonical;
    assert!(canonical.contains("\"synthetic\":[\"id\"]"), "{canonical}");
}

/// The review's Chat stream: a first chunk with `first` as its id when given, then a finish
/// chunk with another id and model.
fn chat_stream_with_ids(first: Option<&str>) -> Vec<u8> {
    let mut opening = json!({"model":"m","created":5,"choices":[{"index":0,"delta":{"role":"assistant","content":"hi"},"finish_reason":null}]});
    if let Some(first) = first {
        opening["id"] = json!(first);
    }
    let finish = json!({"id":"chatcmpl_second","model":"m2","created":6,"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]});
    format!("data: {opening}\n\ndata: {finish}\n\ndata: [DONE]\n\n").into_bytes()
}

#[test]
fn chat_stream_keeps_the_first_source_id_it_showed() {
    let streamed = run(
        Source::Chat,
        &chat_stream_with_ids(Some("chatcmpl_first")),
        Client::Chat,
        BodyMode::Streaming,
    );
    let chunks = chunks(&streamed.output);
    assert!(chunks.iter().all(|chunk| chunk["id"] == "chatcmpl_first"), "{chunks:?}");
    assert_eq!(chunks.last().unwrap()["model"], "m2");
}

#[test]
fn responses_buffered_body_takes_a_source_id_from_a_later_chunk() {
    let input = chat_stream_with_ids(None);
    let chat = run(Source::Chat, &input, Client::Chat, BodyMode::Buffered);
    let chat: Value = serde_json::from_slice(&chat.output).unwrap();
    let responses = run(Source::Chat, &input, Client::Responses, BodyMode::Buffered);
    let responses: Value = serde_json::from_slice(&responses.output).unwrap();
    assert_eq!((&chat["id"], &chat["model"]), (&json!("chatcmpl_second"), &json!("m2")));
    assert_eq!((&responses["id"], &responses["model"]), (&chat["id"], &chat["model"]));
}

#[test]
fn responses_body_takes_an_anthropic_message_identity_after_a_ping() {
    let input = anthropic_stream(true, Some("msg_real"));
    let buffered = run(Source::Anthropic, &input, Client::Responses, BodyMode::Buffered);
    let body: Value = serde_json::from_slice(&buffered.output).unwrap();
    assert_eq!((&body["id"], &body["model"]), (&json!("msg_real"), &json!("claude")));
    let streamed = run(Source::Anthropic, &input, Client::Responses, BodyMode::Streaming);
    let events = data(&streamed.output);
    // `response.created` follows the ping, so the client keeps the issued id; the model refreshes.
    let responses: Vec<_> = events.iter().filter_map(|event| event.get("response")).collect();
    assert!(responses.len() > 1, "{events:?}");
    for response in &responses {
        assert_eq!(response["id"], format!("resp_s{}", streamed.writer), "{response}");
    }
    assert_eq!(responses.last().unwrap()["model"], "claude");
}
