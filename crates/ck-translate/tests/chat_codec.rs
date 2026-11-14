#[path = "semantic/bindings.rs"]
mod bindings;
#[path = "semantic/budget.rs"]
mod budget;
#[path = "semantic/coverage_egress.rs"]
mod coverage_egress;
#[path = "semantic/coverage_media.rs"]
mod coverage_media;
#[path = "semantic/coverage_response.rs"]
mod coverage_response;
#[path = "semantic/depth.rs"]
mod depth;
#[path = "semantic/incremental.rs"]
mod incremental;
#[path = "semantic/media_deletion.rs"]
mod media_deletion;
#[path = "semantic/media_lifetime.rs"]
mod media_lifetime;
#[path = "semantic/media_source.rs"]
mod media_source;
#[path = "semantic/request_controls.rs"]
mod request_controls;
#[path = "semantic/mod.rs"]
mod semantic;

#[path = "semantic/request_media.rs"]
mod request_media;
#[path = "semantic/request_reader.rs"]
mod request_reader;

#[path = "semantic/ownership.rs"]
mod ownership;
#[path = "semantic/provenance.rs"]
mod provenance;
#[path = "semantic/quota.rs"]
mod quota;
#[path = "semantic/reasoning.rs"]
mod reasoning;
#[path = "semantic/request_wire.rs"]
mod request_wire;
#[path = "semantic/response_audio.rs"]
mod response_audio;
#[path = "semantic/response_chunks.rs"]
mod response_chunks;
#[path = "semantic/response_media.rs"]
mod response_media;
#[path = "semantic/response_wire.rs"]
mod response_wire;
#[path = "semantic/snapshots.rs"]
mod snapshots;
#[path = "semantic/terminal.rs"]
mod terminal;
#[path = "semantic/usage.rs"]
mod usage;

use ck_translate::codec::{
    self, Operation, RequestEgress, RequestIngress,
    chat::request::{ChatCapability, Egress, Ingress},
    event::{Direction, Identity, LocalId},
};
use semantic::Mapped;

fn identity(direction: Direction) -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction,
    }
}
fn bytes(lease: &ck_stream::ScratchLease) -> Vec<u8> {
    let mut bytes = Vec::new();
    while bytes.len() < lease.len() {
        bytes.extend_from_slice(lease.chunk(bytes.len()).unwrap());
    }
    bytes
}
fn request(input: &[u8]) -> (serde_json::Value, Vec<serde_json::Value>) {
    request_with_frames(input, 64)
}
fn request_with_frames(input: &[u8], frames: usize) -> (serde_json::Value, Vec<serde_json::Value>) {
    let mut mapped = Mapped::configured(std::sync::Arc::new(ck_stream::ManualClock::default()), frames);
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        1024 * 1024,
    )
    .unwrap();
    for chunk in input.chunks(19) {
        ingress.advance(&mut mapped.writer, chunk).unwrap();
    }
    let ready = ingress.finish(&mut mapped.writer, &ChatCapability).unwrap();
    let committed = mapped.writer.committed();
    assert_eq!(
        u64::from(mapped.store.used_frames()),
        committed.frame + u64::from(committed.offset != 0),
        "finished request retained noncanonical scratch"
    );
    let output = Egress {
        destination: "chat",
        upstream_stream: true,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    let value = serde_json::from_slice(&bytes(&output)).unwrap();
    let events = mapped.output(&mapped.writer);
    let events = std::str::from_utf8(&events)
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    (value, events)
}

fn response(input: &[u8], upstream: codec::BodyMode, downstream: codec::BodyMode) -> (Vec<u8>, Vec<serde_json::Value>) {
    response_fragmented(input, upstream, downstream, 31)
}

fn response_fragmented(
    input: &[u8],
    upstream: codec::BodyMode,
    downstream: codec::BodyMode,
    fragment: usize,
) -> (Vec<u8>, Vec<serde_json::Value>) {
    use codec::{ResponseEgress, ResponseIngress};
    use std::task::Poll;
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
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let mut egress = codec::chat::egress::Egress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        Operation::Generate,
        downstream,
        true,
        |_| panic!("unexpected projection loss"),
    )
    .unwrap();
    let mut result = Vec::new();
    for chunk in input.chunks(fragment) {
        ingress.advance(&mut mapped.writer, chunk).unwrap();
    }
    ingress.finish(&mut mapped.writer).unwrap();
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        egress
            .advance(&mut mapped.writer, lease, |lease| {
                result.extend_from_slice(&bytes(&lease));
                Ok(())
            })
            .unwrap();
    }
    egress.finish().unwrap();
    let canonical = mapped.output(&mapped.writer);
    let events = std::str::from_utf8(&canonical)
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    (result, events)
}

fn canonical_values(mapped: &Mapped) -> Vec<serde_json::Value> {
    let output = mapped.output(&mapped.writer);
    std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
