//! Media parts: MIME, decoded bytes, time offsets and their round trips.
use super::*;
use base64::Engine;
use gemini::response::Framing;

fn decoded(canonical: &[Value]) -> Vec<u8> {
    canonical
        .iter()
        .filter_map(|v| v["bytes"].as_str())
        .flat_map(|s| base64::engine::general_purpose::STANDARD.decode(s).unwrap())
        .collect()
}

#[test]
fn media_mime_and_decoded_bytes_survive_request_and_response() {
    let payload: Vec<u8> = (0..80_003).map(|n| (n % 251) as u8).collect();
    let encoded = base64::engine::general_purpose::STANDARD.encode(&payload);
    let part = json!({"inlineData":{"mimeType":"audio/pcm;rate=24000","data":encoded}});
    let input = json!({"contents":[{"role":"user","parts":[part]}]});
    let (out, canonical) = request(&input, Operation::Generate);
    assert_eq!(out, input);
    assert_eq!(decoded(&canonical), payload);
    assert!(
        canonical
            .iter()
            .any(|v| v["kind"] == "audio" && v["mime"] == "audio/pcm;rate=24000")
    );
    let body = json!({"candidates":[{"index":0,"content":{"role":"model","parts":[part]},"finishReason":"STOP"}]});
    for upstream in [Framing::Json, Framing::Sse, Framing::JsonArray] {
        let input = match upstream {
            Framing::Json => serde_json::to_vec(&body).unwrap(),
            Framing::Sse => sse(std::slice::from_ref(&body)),
            Framing::JsonArray => serde_json::to_vec(&json!([body])).unwrap(),
        };
        let (out, canonical) = response(&input, upstream, Framing::Json, Operation::Generate);
        let out: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(out["candidates"][0]["content"]["parts"][0], part);
        assert_eq!(decoded(&canonical), payload);
    }
}

/// `videoMetadata` offsets become the canonical media timing; the native object, `fps`
/// included, returns unchanged to a Gemini destination.
#[test]
fn video_offsets_become_media_timing_and_round_trip() {
    let part = json!({"fileData":{"mimeType":"video/mp4","fileUri":"gs://b/v.mp4"},"videoMetadata":{"startOffset":"1.25s","endOffset":"10s","fps":2}});
    let input = json!({"contents":[{"role":"user","parts":[part]}]});
    let (out, canonical) = request(&input, Operation::Generate);
    assert_eq!(out, input);
    let field = |name: &str| {
        canonical
            .iter()
            .find(|v| v["pointer"] == format!("/messages/0/content/0/timing/{name}"))
            .map(|v| v["value"].clone())
    };
    assert_eq!(field("start"), Some(json!(1.25)));
    assert_eq!(field("end"), Some(json!(10)));
    assert_eq!(field("unit"), Some(json!("seconds")));
    // Offsets that are not plain JSON numbers of seconds stay native only.
    let other = json!({"contents":[{"role":"user","parts":[{"fileData":{"fileUri":"gs://b/v.mp4"},"videoMetadata":{"startOffset":"01s","endOffset":"-1s"}}]}]});
    let (out, canonical) = request(&other, Operation::Generate);
    assert_eq!(out, other);
    assert!(
        !canonical
            .iter()
            .any(|v| v["pointer"].as_str().is_some_and(|p| p.contains("timing")))
    );
}

/// An escaped offset has no plain number over its input bytes, so it stays native only.
#[test]
fn escaped_video_offset_stays_native() {
    let escape = ['\\', 'u', '0', '0', '3', '1'].iter().collect::<String>();
    let raw = format!(
        r#"{{"contents":[{{"role":"user","parts":[{{"fileData":{{"fileUri":"gs://b/v.mp4"}},"videoMetadata":{{"startOffset":"{escape}.5s"}}}}]}}]}}"#
    );
    let mut mapped = Mapped::new();
    let mut ingress = gemini::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        "m",
        1024 * 1024,
    )
    .unwrap();
    ingress.advance(&mut mapped.writer, raw.as_bytes()).unwrap();
    ingress
        .finish(&mut mapped.writer, &gemini::request::GeminiCapability)
        .unwrap();
    let canonical = events(&mapped.output(&mapped.writer));
    assert!(
        !canonical
            .iter()
            .any(|v| v["pointer"].as_str().is_some_and(|p| p.contains("timing")))
    );
}

/// Each media lifetime: one start, data in index and offset order, one completed end that
/// counts the decoded bytes. Returns the decoded payloads in start order.
fn lifetimes(canonical: &[Value]) -> Vec<Vec<u8>> {
    let mut ids: Vec<String> = Vec::new();
    for v in canonical.iter().filter(|v| v.get("encoding").is_some()) {
        let id = v["x_ck"]["media_id"].as_str().unwrap().to_owned();
        assert!(!ids.contains(&id), "one start per media: {id}");
        ids.push(id);
    }
    ids.iter()
        .map(|id| {
            let scoped: Vec<_> = canonical
                .iter()
                .filter(|v| v["x_ck"]["media_id"] == id.as_str())
                .collect();
            let mut payload = Vec::new();
            for (index, data) in scoped.iter().filter(|v| v["bytes"].is_string()).enumerate() {
                assert_eq!(data["index"], index, "{id}");
                assert_eq!(data["offset"], payload.len(), "{id}");
                let bytes = base64::engine::general_purpose::STANDARD.decode(data["bytes"].as_str().unwrap());
                payload.extend(bytes.unwrap());
            }
            let ends: Vec<_> = scoped.iter().filter(|v| v.get("status").is_some()).collect();
            assert_eq!(ends.len(), 1, "{id}");
            assert_eq!(ends[0]["status"], "completed", "{id}");
            assert_eq!(ends[0]["bytes"], payload.len(), "{id}");
            payload
        })
        .collect()
}

/// Fragmented input yields one complete lifetime per inline payload, whether its MIME type
/// comes before or after the data and whether it sits in the system instruction.
#[test]
fn every_inline_payload_has_one_complete_lifetime() {
    let first: Vec<u8> = (0..70_001).map(|n| (n % 241) as u8).collect();
    let second = vec![9u8; 5_000];
    let third = vec![3u8; 40_000];
    let b64 = |p: &[u8]| base64::engine::general_purpose::STANDARD.encode(p);
    let input = json!({
        "systemInstruction":{"parts":[{"inlineData":{"mimeType":"image/png","data":b64(&first)}}]},
        "contents":[{"role":"user","parts":[
            {"inlineData":{"data":b64(&second),"mimeType":"audio/wav"}},
            {"fileData":{"mimeType":"video/mp4","fileUri":"gs://b/v.mp4"}},
            {"inlineData":{"mimeType":"application/pdf","data":b64(&third)}}
        ]}]
    });
    let mut mapped = Mapped::new();
    let mut ingress = gemini::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        "m",
        1024 * 1024,
    )
    .unwrap();
    for chunk in serde_json::to_vec(&input).unwrap().chunks(37) {
        ingress.advance(&mut mapped.writer, chunk).unwrap();
    }
    let ready = ingress
        .finish(&mut mapped.writer, &gemini::request::GeminiCapability)
        .unwrap();
    let output = gemini::request::Egress {
        destination: "gemini",
        upstream_stream: true,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    let output: Value = serde_json::from_slice(&bytes(&output)).unwrap();
    assert_eq!(output, input);
    let canonical = events(&mapped.output(&mapped.writer));
    let mut payloads = lifetimes(&canonical);
    payloads.sort_by_key(Vec::len);
    assert_eq!(payloads, [second, third, first]);
}

/// Streamed response media: each inline part is its own completed lifetime.
#[test]
fn streamed_response_media_lifetimes_complete() {
    let image = vec![1u8; 60_000];
    let audio = vec![2u8; 3];
    let b64 = |p: &[u8]| base64::engine::general_purpose::STANDARD.encode(p);
    let rows = [
        json!({"candidates":[{"content":{"role":"model","parts":[{"inlineData":{"mimeType":"image/png","data":b64(&image)}}]}}]}),
        json!({"candidates":[{"content":{"role":"model","parts":[{"inlineData":{"mimeType":"audio/wav","data":b64(&audio)}}]},"finishReason":"STOP"}]}),
    ];
    let (out, canonical) = response(&sse(&rows), Framing::Sse, Framing::Json, Operation::Generate);
    assert_eq!(lifetimes(&canonical), [image, audio]);
    let out: Value = serde_json::from_slice(&out).unwrap();
    let parts = out["candidates"][0]["content"]["parts"].as_array().unwrap();
    assert_eq!(parts.len(), 2);
}
