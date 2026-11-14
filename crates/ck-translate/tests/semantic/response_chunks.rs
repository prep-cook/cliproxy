//! Buffered Chat response media collects small network reads into shared data records.
use super::{Direction, Mapped, Operation, canonical_values, codec, identity};
use base64::Engine;
use codec::{BodyMode, ResponseIngress};
use serde_json::{Value, json};

/// Input bytes a data record collects before it publishes (docs/INTERNAL-STREAM.md, §3.4).
const THRESHOLD: usize = 28_672;

fn buffered(mapped: &mut Mapped) -> codec::chat::response::Ingress {
    codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Buffered,
        1024 * 1024,
    )
    .unwrap()
}

/// Data records in publication order, checked for consecutive indexes and offsets, and their
/// decoded bytes.
fn data_records(mapped: &Mapped) -> (usize, Vec<u8>) {
    let events = canonical_values(mapped);
    let data: Vec<&Value> = events.iter().filter(|event| event["bytes"].is_string()).collect();
    let mut bytes = Vec::new();
    for (index, event) in data.iter().enumerate() {
        assert_eq!(event["index"], index);
        assert_eq!(event["offset"], bytes.len());
        bytes.extend(
            base64::engine::general_purpose::STANDARD
                .decode(event["bytes"].as_str().unwrap())
                .unwrap(),
        );
    }
    (data.len(), bytes)
}

fn image_body(encoded: &str) -> String {
    json!({"id":"r","model":"m","created":1,"choices":[{"index":0,"message":{"role":"assistant",
        "content":[{"type":"image_url","image_url":{"url":format!("data:image/png;base64,{encoded}")}}]},
        "finish_reason":"stop"}]})
    .to_string()
}

/// An 80 KB image read 13 bytes at a time fits a 16-frame pool: its reads share data records
/// instead of costing a frame-aligned record each.
#[test]
fn buffered_image_in_small_reads_shares_data_records() {
    let raw: Vec<u8> = (0..80_000).map(|index| (index % 251) as u8).collect();
    let encoded = base64::engine::general_purpose::STANDARD.encode(&raw);
    let mut mapped = Mapped::configured(std::sync::Arc::new(ck_stream::ManualClock::default()), 16);
    let mut ingress = buffered(&mut mapped);
    for piece in image_body(&encoded).as_bytes().chunks(13) {
        ingress.advance(&mut mapped.writer, piece).unwrap();
    }
    ingress.finish(&mut mapped.writer).unwrap();
    let (records, bytes) = data_records(&mapped);
    assert_eq!(bytes, raw);
    assert!(
        records <= encoded.len().div_ceil(THRESHOLD) + 1,
        "{records} data records"
    );
    let end = canonical_values(&mapped)
        .into_iter()
        .find(|event| event["status"] == "completed")
        .unwrap();
    assert_eq!(end["bytes"], raw.len());
}

/// Held media input reports a deadline at most 100 ms after collection starts, which later
/// input does not move; a flush publishes the complete base64 quartets and keeps a split one
/// for the next read.
#[test]
fn held_media_input_has_a_deadline_and_flushes_split_quartets() {
    let raw: Vec<u8> = (0..3000).map(|index| (index % 251) as u8).collect();
    let encoded = base64::engine::general_purpose::STANDARD.encode(&raw);
    let body = image_body(&encoded);
    let start = body.find("base64,").unwrap() + "base64,".len();
    // Stop two characters into a quartet, then one more character, so a quartet stays split.
    let (first, rest) = body.split_at(start + 2002);
    let (second, rest) = rest.split_at(1);
    let mut mapped = Mapped::new();
    let mut ingress = buffered(&mut mapped);
    let began = std::time::Instant::now();
    ingress.advance(&mut mapped.writer, first.as_bytes()).unwrap();
    let deadline = ingress.flush_deadline().unwrap();
    assert!(deadline > began && deadline <= std::time::Instant::now() + std::time::Duration::from_millis(100));
    ingress.advance(&mut mapped.writer, second.as_bytes()).unwrap();
    assert_eq!(ingress.flush_deadline(), Some(deadline));
    assert_eq!(data_records(&mapped).0, 0);
    ingress.flush(&mut mapped.writer).unwrap();
    assert!(ingress.flush_deadline().is_none());
    assert_eq!(data_records(&mapped), (1, raw[..1500].to_vec()));
    // A read after the deadline publishes without an explicit flush.
    let (third, rest) = rest.split_at(5);
    ingress.advance(&mut mapped.writer, third.as_bytes()).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(101));
    let (fourth, rest) = rest.split_at(1);
    ingress.advance(&mut mapped.writer, fourth.as_bytes()).unwrap();
    assert_eq!(data_records(&mapped), (2, raw[..1506].to_vec()));
    // The string's end publishes before the body ends.
    ingress.advance(&mut mapped.writer, rest.as_bytes()).unwrap();
    assert_eq!(data_records(&mapped), (3, raw));
    ingress.finish(&mut mapped.writer).unwrap();
}
