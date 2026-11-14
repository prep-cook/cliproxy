#[path = "responses/audio.rs"]
mod audio;
#[path = "responses/cross.rs"]
mod cross;
#[path = "responses/events.rs"]
mod events;
#[path = "responses/execution.rs"]
mod execution;
#[path = "responses/finish.rs"]
mod finish;
#[path = "responses/golden.rs"]
mod golden;
#[path = "responses/incremental.rs"]
mod incremental;
#[path = "responses/media.rs"]
mod media;
#[path = "responses/recordings.rs"]
mod recordings;
#[path = "responses/request.rs"]
mod request;
#[path = "responses/response.rs"]
mod response;
#[path = "responses/review.rs"]
mod review;
// Test-only inclusion until the framework owner declares the production module.
// Shared by every boundary codec; this protocol uses a subset.
#[allow(dead_code)]
#[path = "../src/boundary.rs"]
pub mod boundary;
#[path = "../src/codec/mod.rs"]
pub mod codec;
#[path = "../src/responses/mod.rs"]
pub mod responses;
#[path = "semantic/mod.rs"]
mod semantic;

use codec::{
    BodyMode, Operation, RequestEgress, RequestIngress, ResponseEgress, ResponseIngress,
    event::{Direction, Identity, LocalId},
};
use semantic::Mapped;
use serde_json::{Value, json};
fn identity(direction: Direction) -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction,
    }
}
fn bytes(lease: &ck_stream::ScratchLease) -> Vec<u8> {
    let mut result = Vec::new();
    while result.len() < lease.len() {
        result.extend_from_slice(lease.chunk(result.len()).unwrap());
    }
    result
}
fn events(mapped: &Mapped) -> Vec<Value> {
    let bytes = mapped.output(&mapped.writer);
    std::str::from_utf8(&bytes)
        .unwrap()
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}
/// Asserts that the last event is the boundary error the Chat codec also emits.
fn assert_invalid_input(raw: &[u8], scope: &str) {
    let error: Value = std::str::from_utf8(raw)
        .unwrap()
        .lines()
        .rev()
        .filter_map(|line| line.strip_prefix("data: "))
        .find_map(|line| serde_json::from_str(line).ok())
        .unwrap();
    assert_eq!(error["code"], "stream.invalid_input");
    assert_eq!(error["status"], if scope == "client" { 400 } else { 502 });
    assert_eq!(error["message"], "Invalid boundary input");
    assert_eq!(error["scope"], scope);
}
fn request(input: &Value, operation: Operation) -> (Value, Vec<Value>) {
    let mut m = Mapped::new();
    let mut ingress = responses::request::Ingress::new(
        &mut m.writer,
        &m.store,
        identity(Direction::Request),
        operation,
        1024 * 1024,
    )
    .unwrap();
    for chunk in serde_json::to_vec(input).unwrap().chunks(7) {
        ingress.advance(&mut m.writer, chunk).unwrap();
    }
    let ready = ingress
        .finish(&mut m.writer, &responses::request::ResponsesCapability)
        .unwrap();
    let output = responses::request::Egress {
        destination: "responses",
        upstream_stream: false,
    }
    .encode(&ready, &mut m.writer)
    .unwrap();
    (serde_json::from_slice(&bytes(&output)).unwrap(), events(&m))
}
fn response(input: &[u8], mode: BodyMode, downstream: BodyMode, operation: Operation) -> (Vec<u8>, Vec<Value>) {
    let mut m = Mapped::new();
    let mut ingress = responses::response::Ingress::new(
        &mut m.writer,
        identity(Direction::Response),
        operation,
        mode,
        1024 * 1024,
    )
    .unwrap();
    let mut reader = m.store.reader(m.writer.id(), 0).unwrap();
    let mut egress = responses::egress::Egress::new(
        &mut m.writer,
        identity(Direction::Response),
        operation,
        downstream,
        |_, _| {},
    )
    .unwrap();
    let mut result = Vec::new();
    for chunk in input.chunks(13) {
        ingress.advance(&mut m.writer, chunk).unwrap();
    }
    ingress.finish(&mut m.writer).unwrap();
    while let std::task::Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        egress
            .advance(&mut m.writer, lease, |out| {
                result.extend_from_slice(&bytes(&out));
                Ok(())
            })
            .unwrap();
    }
    egress.finish().unwrap();
    (result, events(&m))
}
/// SSE data lines carrying `records`.
fn wire(records: &[Value]) -> String {
    records.iter().map(|record| format!("data: {record}\n\n")).collect()
}
