//! Unfinished request strings reach the stream before the request ends.
use super::*;
use base64::Engine;

fn ingress(mapped: &mut Mapped) -> gemini::request::Ingress {
    gemini::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        "m",
        1024 * 1024,
    )
    .unwrap()
}
fn decoded(canonical: &[Value]) -> Vec<u8> {
    canonical
        .iter()
        .filter_map(|v| v["bytes"].as_str())
        .flat_map(|s| base64::engine::general_purpose::STANDARD.decode(s).unwrap())
        .collect()
}
fn encode(ready: &codec::ValidatedRequest<'_>, mapped: &mut Mapped) -> Value {
    let output = gemini::request::Egress {
        destination: "gemini",
        upstream_stream: true,
    }
    .encode(ready, &mut mapped.writer)
    .unwrap();
    serde_json::from_slice(&bytes(&output)).unwrap()
}

#[test]
fn unfinished_text_publishes_decoded_data_and_flushes_without_request_end() {
    let mut mapped = Mapped::new();
    let mut ingress = ingress(&mut mapped);
    ingress
        .advance(&mut mapped.writer, br#"{"contents":[{"role":"user","parts":[{"text":""#)
        .unwrap();
    let text = "x".repeat(100_000);
    ingress.advance(&mut mapped.writer, text.as_bytes()).unwrap();
    let deadline = ingress.flush_deadline().unwrap();
    ingress.advance(&mut mapped.writer, b"xx").unwrap();
    assert_eq!(
        ingress.flush_deadline(),
        Some(deadline),
        "later input must not postpone the deadline"
    );
    ingress.flush(&mut mapped.writer).unwrap();
    assert!(ingress.flush_deadline().is_none());
    let raw = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert_eq!(decoded(&events(raw.as_bytes())), format!("{text}xx").as_bytes());
    assert!(!raw.contains("event: x_ck.request.end"));
    ingress.advance(&mut mapped.writer, br#"\uD83D\uDE00"}]}]}"#).unwrap();
    let ready = ingress
        .finish(&mut mapped.writer, &gemini::request::GeminiCapability)
        .unwrap();
    let output = encode(&ready, &mut mapped);
    assert_eq!(output["contents"][0]["parts"][0]["text"], format!("{text}xx\u{1f600}"));
}

/// Input that arrives after the deadline publishes the held bytes even when no flush came.
#[test]
fn input_after_the_deadline_publishes_held_bytes() {
    let mut mapped = Mapped::new();
    let mut ingress = ingress(&mut mapped);
    let open = br#"{"contents":[{"parts":[{"text":""#;
    ingress.advance(&mut mapped.writer, open).unwrap();
    ingress
        .advance(&mut mapped.writer, "a".repeat(2_000).as_bytes())
        .unwrap();
    ingress.advance(&mut mapped.writer, b"bb").unwrap();
    let deadline = ingress.flush_deadline().unwrap();
    std::thread::sleep(deadline.saturating_duration_since(std::time::Instant::now()));
    ingress.advance(&mut mapped.writer, b"c").unwrap();
    assert!(ingress.flush_deadline().is_none());
    let published = decoded(&events(&mapped.output(&mapped.writer)));
    assert_eq!(published, format!("{}bbc", "a".repeat(2_000)).as_bytes());
}

#[test]
fn inline_data_streams_once_its_mime_type_is_known() {
    let payload: Vec<u8> = (0..80_003).map(|n| (n % 251) as u8).collect();
    let encoded = base64::engine::general_purpose::STANDARD.encode(&payload);
    let mut mapped = Mapped::new();
    let mut ingress = ingress(&mut mapped);
    let body = format!(
        r#"{{"contents":[{{"role":"user","parts":[{{"inlineData":{{"mimeType":"image/png","data":"{encoded}"}}}}]}}]}}"#
    );
    let (head, tail) = body.split_at(body.len() - 10);
    ingress.advance(&mut mapped.writer, head.as_bytes()).unwrap();
    ingress.flush(&mut mapped.writer).unwrap();
    let early = events(&mapped.output(&mapped.writer));
    assert!(early.iter().any(|v| v["kind"] == "image" && v["mime"] == "image/png"));
    assert!(!decoded(&early).is_empty(), "inline bytes publish before the body ends");
    ingress.advance(&mut mapped.writer, tail.as_bytes()).unwrap();
    let ready = ingress
        .finish(&mut mapped.writer, &gemini::request::GeminiCapability)
        .unwrap();
    let output = encode(&ready, &mut mapped);
    assert_eq!(
        output["contents"][0]["parts"][0]["inlineData"]["data"],
        encoded.as_str()
    );
    assert_eq!(decoded(&events(&mapped.output(&mapped.writer))), payload);
}

/// The modality comes from the MIME type; data before its MIME type is published whole at the
/// end, never under a guessed kind.
#[test]
fn inline_data_before_its_mime_type_waits_for_the_kind() {
    let payload = vec![7u8; 30_000];
    let encoded = base64::engine::general_purpose::STANDARD.encode(&payload);
    let mut mapped = Mapped::new();
    let mut ingress = ingress(&mut mapped);
    let body =
        format!(r#"{{"contents":[{{"parts":[{{"inlineData":{{"data":"{encoded}","mimeType":"audio/wav"}}}}]}}]}}"#);
    let (head, tail) = body.split_at(body.len() - 40);
    ingress.advance(&mut mapped.writer, head.as_bytes()).unwrap();
    ingress.flush(&mut mapped.writer).unwrap();
    assert!(decoded(&events(&mapped.output(&mapped.writer))).is_empty());
    ingress.advance(&mut mapped.writer, tail.as_bytes()).unwrap();
    ingress
        .finish(&mut mapped.writer, &gemini::request::GeminiCapability)
        .unwrap();
    let canonical = events(&mapped.output(&mapped.writer));
    assert!(
        canonical
            .iter()
            .any(|v| v["kind"] == "audio" && v["mime"] == "audio/wav")
    );
    assert!(!canonical.iter().any(|v| v["kind"] == "document"));
    assert_eq!(decoded(&canonical), payload);
}

#[test]
fn malformed_suffix_after_publication_cannot_open_request_fence() {
    let mut mapped = Mapped::new();
    let mut ingress = ingress(&mut mapped);
    ingress
        .advance(&mut mapped.writer, br#"{"contents":[{"parts":[{"text":""#)
        .unwrap();
    ingress
        .advance(&mut mapped.writer, "x".repeat(20_000).as_bytes())
        .unwrap();
    assert!(
        events(&mapped.output(&mapped.writer))
            .iter()
            .any(|v| v["bytes"].is_string())
    );
    assert!(
        ingress
            .finish(&mut mapped.writer, &gemini::request::GeminiCapability)
            .is_err()
    );
    let raw = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert!(raw.contains("event: x_ck.error"));
    assert!(!raw.contains("event: x_ck.request.end"));
    assert!(!raw.contains("event: x_ck.done"));
}
