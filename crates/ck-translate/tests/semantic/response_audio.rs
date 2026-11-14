//! Streamed Chat audio holds decoded bytes across SSE records and publishes them in shared data
//! records, keeping its transcript and other blocks in their source order.
use super::{
    Direction, Mapped, Operation, canonical_values, codec, identity,
    response_chunks::{THRESHOLD, data_records},
};
use base64::Engine;
use codec::{BodyMode, ResponseIngress};
use serde_json::{Value, json};

fn streamed(mapped: &mut Mapped) -> codec::chat::response::Ingress {
    codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        1024 * 1024,
    )
    .unwrap()
}

fn encode(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn record(delta: Value, finish: Value) -> String {
    let chunk = json!({"id":"r","model":"m","created":1,"choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    format!("data: {chunk}\n\n")
}

fn audio(data: &str) -> String {
    record(json!({"audio":{"data":data}}), Value::Null)
}

fn end() -> String {
    record(json!({}), json!("stop")) + "data: [DONE]\n\n"
}

fn sample(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

/// Feeds a stream in 512-byte network reads.
fn feed(ingress: &mut codec::chat::response::Ingress, mapped: &mut Mapped, stream: &str) {
    for piece in stream.as_bytes().chunks(512) {
        ingress.advance(&mut mapped.writer, piece).unwrap();
    }
}

fn completed(mapped: &Mapped) -> Option<Value> {
    canonical_values(mapped)
        .into_iter()
        .find(|event| event["status"] == "completed")
}

/// 500 audio deltas of 40 bytes each fit a 16-frame pool: they share data records instead of
/// costing a frame-aligned record each.
#[test]
fn streamed_audio_deltas_share_data_records() {
    let raw = sample(500 * 40);
    let mut stream: String = raw.chunks(40).map(|piece| audio(&encode(piece))).collect();
    stream += &end();
    let mut mapped = Mapped::configured(std::sync::Arc::new(ck_stream::ManualClock::default()), 16);
    let mut ingress = streamed(&mut mapped);
    feed(&mut ingress, &mut mapped, &stream);
    ingress.finish(&mut mapped.writer).unwrap();
    let (records, bytes) = data_records(&mapped);
    assert_eq!(bytes, raw);
    assert!(records <= raw.len().div_ceil(THRESHOLD) + 1, "{records} data records");
    assert_eq!(completed(&mapped).unwrap()["bytes"], raw.len());
}

/// Held audio publishes once a delta brings it to a record's worth, before the stream ends:
/// the tenth 3,000-byte delta reaches the threshold, and all ten publish together.
#[test]
fn held_audio_publishes_when_it_fills_a_record() {
    let raw = sample(12 * 3000);
    let mut mapped = Mapped::new();
    let mut ingress = streamed(&mut mapped);
    let stream: String = raw.chunks(3000).map(|piece| audio(&encode(piece))).collect();
    feed(&mut ingress, &mut mapped, &stream);
    // The tenth delta reaches the threshold; the wall-clock deadline may publish more on a
    // slow host, never less.
    let (records, bytes) = data_records(&mapped);
    assert_eq!(THRESHOLD.div_ceil(3000) * 3000, 30_000);
    assert!(
        bytes.len() >= 30_000 && records <= 2,
        "{records} records, {} bytes",
        bytes.len()
    );
    assert_eq!(bytes, raw[..bytes.len()]);
    feed(&mut ingress, &mut mapped, &end());
    ingress.finish(&mut mapped.writer).unwrap();
    let (records, bytes) = data_records(&mapped);
    assert_eq!(bytes, raw);
    assert!(records <= 3, "{records} data records");
}

/// Base64 quartets split across SSE records decode into one record; a flush publishes only the
/// complete quartets and keeps a split one for the next record.
#[test]
fn audio_quartets_split_across_records_share_a_record() {
    let raw = sample(3000);
    let encoded = encode(&raw);
    let pieces: Vec<&str> = encoded
        .as_bytes()
        .chunks(7)
        .map(|piece| std::str::from_utf8(piece).unwrap())
        .collect();
    let mut mapped = Mapped::new();
    let mut ingress = streamed(&mut mapped);
    // 143 pieces of 7 characters hold 1,001 characters: 250 quartets and one split character.
    let stream: String = pieces[..143].iter().map(|piece| audio(piece)).collect();
    // The 100 ms deadline runs on the wall clock, so a slow host may publish once before the
    // flush; the bytes published at each point are what the split quartet decides.
    feed(&mut ingress, &mut mapped, &stream);
    ingress.flush(&mut mapped.writer).unwrap();
    let (records, bytes) = data_records(&mapped);
    assert_eq!(bytes, raw[..750].to_vec());
    assert!(records <= 2, "{records} data records");
    let stream: String = pieces[143..].iter().map(|piece| audio(piece)).collect::<String>() + &end();
    feed(&mut ingress, &mut mapped, &stream);
    ingress.finish(&mut mapped.writer).unwrap();
    let (records, bytes) = data_records(&mapped);
    assert_eq!(bytes, raw);
    assert!(records <= 4, "{records} data records");
}

/// Held audio reports a deadline at most 100 ms after collection starts, which later records do
/// not move; a flush clears it, and a read after the deadline publishes without a flush.
#[test]
fn held_audio_has_a_flush_deadline() {
    let raw = sample(4 * 30);
    let pieces: Vec<String> = raw.chunks(30).map(|piece| audio(&encode(piece))).collect();
    let mut mapped = Mapped::new();
    let mut ingress = streamed(&mut mapped);
    let began = std::time::Instant::now();
    feed(&mut ingress, &mut mapped, &pieces[0]);
    let deadline = ingress.flush_deadline().unwrap();
    assert!(deadline > began && deadline <= std::time::Instant::now() + std::time::Duration::from_millis(100));
    feed(&mut ingress, &mut mapped, &pieces[1]);
    assert_eq!(ingress.flush_deadline(), Some(deadline));
    assert_eq!(data_records(&mapped).0, 0);
    ingress.flush(&mut mapped.writer).unwrap();
    assert!(ingress.flush_deadline().is_none());
    assert_eq!(data_records(&mapped), (1, raw[..60].to_vec()));
    feed(&mut ingress, &mut mapped, &pieces[2]);
    std::thread::sleep(std::time::Duration::from_millis(101));
    feed(&mut ingress, &mut mapped, &pieces[3]);
    assert!(ingress.flush_deadline().is_none());
    assert_eq!(data_records(&mapped), (2, raw.clone()));
    feed(&mut ingress, &mut mapped, &end());
    ingress.finish(&mut mapped.writer).unwrap();
    assert_eq!(data_records(&mapped), (2, raw));
}

/// Held audio from complete records is published once, without a media close, before a
/// terminal error; bytes of the rejected record are not.
#[test]
fn held_audio_publishes_before_a_terminal_error() {
    let raw = sample(60);
    let terminals = [None, Some("data: {\"choices\"\n\n".to_owned()), Some(audio("AAEC!!!!"))];
    for terminal in terminals {
        let mut mapped = Mapped::new();
        let mut ingress = streamed(&mut mapped);
        let stream: String = raw.chunks(30).map(|piece| audio(&encode(piece))).collect();
        feed(&mut ingress, &mut mapped, &stream);
        assert!(ingress.flush_deadline().is_some());
        let result = match &terminal {
            Some(stream) => ingress.advance(&mut mapped.writer, stream.as_bytes()),
            None => ingress.finish(&mut mapped.writer),
        };
        assert!(result.is_err(), "{terminal:?}");
        assert!(ingress.flush_deadline().is_none());
        assert_eq!(data_records(&mapped), (1, raw.clone()), "{terminal:?}");
        let events = canonical_values(&mapped);
        assert!(completed(&mapped).is_none());
        assert!(events.iter().any(|event| event["code"] == "stream.invalid_input"));
    }
}

/// Audio data, transcript and text records in canonical order: data by byte count, transcript
/// and text by value.
fn audio_order(events: &[Value]) -> Vec<String> {
    let mut order = Vec::new();
    for event in events {
        if let Some(bytes) = event["bytes"].as_str() {
            let len = base64::engine::general_purpose::STANDARD.decode(bytes).unwrap().len();
            order.push(format!("data {len}"));
        } else if event["final"].is_boolean() {
            order.push(format!("transcript {}", event["text"].as_str().unwrap()));
        } else if let Some(text) = event["choices"][0]["delta"]["content"].as_str() {
            order.push(format!("content {text}"));
        }
    }
    order
}

/// A transcript or text delta publishes the audio held before it first, so records keep their
/// source order while consecutive audio deltas share a record.
#[test]
fn held_audio_keeps_its_order_with_transcript_and_text() {
    let stream = [
        audio(&encode(&[1; 30])),
        // A null member opens no block, so the audio held before it stays held.
        record(json!({"content":null,"audio":{"data":encode(&[2; 30])}}), Value::Null),
        record(json!({"audio":{"transcript":"one"}}), Value::Null),
        audio(&encode(&[3; 30])),
        record(json!({"content":"hi"}), Value::Null),
        audio(&encode(&[4; 30])),
        record(
            json!({"audio":{"data":encode(&[5; 30]),"transcript":"two"}}),
            Value::Null,
        ),
        end(),
    ]
    .concat();
    let mut mapped = Mapped::new();
    let mut ingress = streamed(&mut mapped);
    feed(&mut ingress, &mut mapped, &stream);
    ingress.finish(&mut mapped.writer).unwrap();
    assert_eq!(
        audio_order(&canonical_values(&mapped)),
        [
            "data 60",
            "transcript one",
            "data 30",
            "content hi",
            "data 60",
            "transcript two",
            "transcript ",
        ]
    );
}

/// A delta whose base64 fails after its first quartets adds no bytes, even when those quartets
/// would cross the threshold: only the earlier delta's bytes publish before the error.
#[test]
fn rejected_delta_adds_no_bytes_across_the_threshold() {
    let mut mapped = Mapped::new();
    let mut ingress = streamed(&mut mapped);
    feed(&mut ingress, &mut mapped, &audio(&"AQID".repeat(9557)));
    assert_eq!(data_records(&mapped).0, 0);
    let result = ingress.advance(&mut mapped.writer, audio("BAUG!!!!").as_bytes());
    assert!(matches!(result, Err(codec::Error::Schema)), "{result:?}");
    let (records, bytes) = data_records(&mapped);
    assert_eq!((records, bytes.len()), (1, 28_671));
    assert!(bytes == [1, 2, 3].repeat(9557));
    assert!(completed(&mapped).is_none());
    assert!(ingress.flush_deadline().is_none());
}

/// A storage failure disarms the held audio's deadline; the failed ingress is not flushed.
#[test]
fn storage_failure_disarms_the_audio_deadline() {
    let mut mapped = Mapped::configured(std::sync::Arc::new(ck_stream::ManualClock::default()), 16);
    let mut ingress = streamed(&mut mapped);
    let content = json!({"choices":[{"index":1,"delta":{"content":"x".repeat(500_000)}}]});
    ingress
        .advance(
            &mut mapped.writer,
            b"data: {\"choices\":[{\"index\":0,\"delta\":{\"audio\":{\"data\":\"AQID\"}}}]}\n\n",
        )
        .unwrap();
    assert!(ingress.flush_deadline().is_some());
    let result = ingress.advance(&mut mapped.writer, format!("data: {content}\n\n").as_bytes());
    assert!(
        matches!(result, Err(codec::Error::Storage(ck_stream::Error::StorageExhausted))),
        "{result:?}"
    );
    assert!(ingress.flush_deadline().is_none());
    assert!(ingress.flush(&mut mapped.writer).is_err());
    assert!(ingress.flush_deadline().is_none());
}
