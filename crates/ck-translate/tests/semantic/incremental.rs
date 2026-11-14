use super::{bytes, canonical_values, identity, semantic::Mapped};
use ck_translate::codec::{
    self, Operation, RequestEgress, RequestIngress,
    chat::request::{ChatCapability, Egress, Ingress},
    event::Direction,
};

#[test]
fn unfinished_string_releases_source_and_survives_unicode_tails() {
    let clock = std::sync::Arc::new(ck_stream::ManualClock::default());
    let mut mapped = Mapped::configured(clock.clone(), 48);
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        200_000,
    )
    .unwrap();
    let mut prefix = br#"{"model":"m","messages":[{"role":"user","content":""#.to_vec();
    prefix.extend(std::iter::repeat_n(b'a', 8192));
    let began = std::time::Instant::now();
    ingress.advance(&mut mapped.writer, &prefix).unwrap();
    assert!(began.elapsed() < std::time::Duration::from_millis(100));
    // The owner flushes held string input by its deadline.
    let deadline = ingress.flush_deadline().unwrap();
    assert!(deadline <= std::time::Instant::now() + std::time::Duration::from_millis(100));
    ingress.flush(&mut mapped.writer).unwrap();
    clock.advance(std::time::Duration::from_millis(100));
    let events = canonical_values(&mapped);
    assert!(
        events
            .iter()
            .any(|e| e["pointer"] == "/messages/0/content" && e["value"]["value_kind"] == "utf8")
    );
    assert!(
        events
            .iter()
            .any(|e| e.get("bytes").is_some_and(serde_json::Value::is_string))
    );
    assert!(
        !String::from_utf8(mapped.output(&mapped.writer))
            .unwrap()
            .contains("event: x_ck.media.end\n")
    );
    let canonical = mapped.writer.committed();
    assert!(
        u64::from(mapped.store.used_frames()) <= canonical.frame + 2,
        "raw prefix remains charged"
    );
    for fragment in [
        b"\\uD83D".as_slice(),
        b"\\uDE80",
        b"\\n",
        b"\xc3",
        b"\xa9",
        b"\\\"",
        b"\\\\",
    ] {
        ingress.advance(&mut mapped.writer, fragment).unwrap();
        ingress.flush(&mut mapped.writer).unwrap();
        assert!(ingress.flush_deadline().is_none());
        assert!(
            !String::from_utf8(mapped.output(&mapped.writer))
                .unwrap()
                .contains("event: x_ck.media.end\n")
        );
    }
    ingress.advance(&mut mapped.writer, br#""}]}"#).unwrap();
    let ready = ingress.finish(&mut mapped.writer, &ChatCapability).unwrap();
    let output = Egress {
        destination: "chat",
        upstream_stream: false,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    let output: serde_json::Value = serde_json::from_slice(&bytes(&output)).unwrap();
    assert_eq!(
        output["messages"][0]["content"],
        format!("{}🚀\né\"\\", "a".repeat(8192))
    );
    let mut pointers = std::collections::HashSet::new();
    for event in canonical_values(&mapped) {
        if let Some(pointer) = event["pointer"].as_str() {
            assert!(pointers.insert(pointer.to_owned()));
        }
    }
}

#[test]
fn late_duplicate_after_streamed_prefix_is_still_decoded_equality() {
    let mut mapped = Mapped::new();
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        100_000,
    )
    .unwrap();
    let mut prefix = br#"{"model":"m","messages":[{"role":"user","content":""#.to_vec();
    prefix.extend(std::iter::repeat_n(b'a', 8192));
    ingress.advance(&mut mapped.writer, &prefix).unwrap();
    let error = ingress
        .advance(&mut mapped.writer, br#"","\u0072ole":"assistant"}]}"#)
        .unwrap_err();
    assert!(matches!(error, codec::Error::DuplicateKey), "{error:?}");
    let output = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert!(output.contains("event: x_ck.error\n"));
    assert!(!output.contains("event: x_ck.request.end\n"));
    assert!(!output.contains("event: x_ck.done\n"));
}

#[test]
fn malformed_streamed_string_tail_has_no_successful_media_end() {
    let mut mapped = Mapped::new();
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        100_000,
    )
    .unwrap();
    let mut prefix = br#"{"model":"m","messages":[{"role":"user","content":""#.to_vec();
    prefix.extend(std::iter::repeat_n(b'a', 8192));
    ingress.advance(&mut mapped.writer, &prefix).unwrap();
    assert!(matches!(
        ingress.advance(&mut mapped.writer, br#"\uD800x"#),
        Err(codec::Error::Json)
    ));
    let output = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert!(output.contains("event: x_ck.error\n"));
    assert!(!output.contains("event: x_ck.media.end\n"));
    assert!(!output.contains("event: x_ck.done\n"));
}

#[test]
fn unfinished_native_media_publishes_one_lifetime_and_exact_bytes() {
    use base64::Engine;
    for (prefix, suffix, carrier, field, header) in [
        (
            r#"{"type":"image_url","image_url":{"url":"data:image/png;base64,"#,
            r#"","ref":"literal","detail":"high"}}"#,
            "image_url",
            "url",
            "data:image/png;base64,",
        ),
        (
            r#"{"type":"input_audio","input_audio":{"data":""#,
            r#"","ref":"literal","format":"wav"}}"#,
            "input_audio",
            "data",
            "",
        ),
        (
            r#"{"type":"file","file":{"file_data":"data:application/pdf;base64,"#,
            r#"","ref":"literal","filename":"f.pdf"}}"#,
            "file",
            "file_data",
            "data:application/pdf;base64,",
        ),
    ] {
        let mut mapped = Mapped::configured(std::sync::Arc::new(ck_stream::ManualClock::default()), 64);
        let mut ingress = Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Request),
            Operation::Generate,
            100_000,
        )
        .unwrap();
        let input = format!(r#"{{"model":"m","messages":[{{"role":"user","content":[{prefix}AAEC"#);
        ingress.advance(&mut mapped.writer, input.as_bytes()).unwrap();
        ingress.flush(&mut mapped.writer).unwrap();
        let events = canonical_values(&mapped);
        assert!(events.iter().any(|e| e["bytes"] == "AAEC"), "{carrier}: {events:?}");
        let wire = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
        assert_eq!(wire.matches("event: x_ck.media.start\n").count(), 1);
        assert!(!wire.contains("event: x_ck.media.end\n"));
        for fragment in [b"A".as_slice(), b"w", b"Q", b"F", b"Bg", b"=="] {
            ingress.advance(&mut mapped.writer, fragment).unwrap();
        }
        ingress
            .advance(&mut mapped.writer, format!("{suffix}]}}]}}").as_bytes())
            .unwrap();
        let ready = ingress.finish(&mut mapped.writer, &ChatCapability).unwrap();
        let output = Egress {
            destination: "chat",
            upstream_stream: false,
        }
        .encode(&ready, &mut mapped.writer)
        .unwrap();
        let output: serde_json::Value = serde_json::from_slice(&bytes(&output)).unwrap();
        let native = &output["messages"][0]["content"][0][carrier];
        let encoded = native[field].as_str().unwrap().strip_prefix(header).unwrap();
        assert_eq!(
            base64::engine::general_purpose::STANDARD.decode(encoded).unwrap(),
            [0, 1, 2, 3, 4, 5, 6]
        );
        assert_eq!(native["ref"], "literal");
        let wire = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
        assert_eq!(wire.matches("event: x_ck.media.start\n").count(), 1);
        assert_eq!(wire.matches("event: x_ck.media.end\n").count(), 1);
    }
}

#[test]
fn unfinished_nested_strings_remain_logical_and_arguments_are_structured() {
    for (prefix, suffix, pointer) in [
        (r#"{"model":""#, r#"","messages":[]}"#, "/model"),
        (
            r#"{"model":"m","messages":[],"prediction":{"type":"content","content":""#,
            r#""}}"#,
            "/prediction/content",
        ),
        (
            r#"{"model":"m","messages":[],"unknown":{"a/b~é":""#,
            r#""}}"#,
            "/unknown/a~1b~0é",
        ),
        (
            r#"{"model":"m","messages":[{"role":"assistant","tool_calls":[{"id":"c","type":"function","function":{"name":"f","arguments":"{\"a\":\""#,
            r#"\"}"}}]}]}"#,
            "/messages/0/tool_calls/0/function/arguments",
        ),
    ] {
        let mut mapped = Mapped::new();
        let mut ingress = Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Request),
            Operation::Generate,
            100_000,
        )
        .unwrap();
        let mut input = prefix.as_bytes().to_vec();
        input.extend(std::iter::repeat_n(b'a', 8192));
        ingress.advance(&mut mapped.writer, &input).unwrap();
        ingress.advance(&mut mapped.writer, suffix.as_bytes()).unwrap();
        let ready = ingress.finish(&mut mapped.writer, &ChatCapability).unwrap();
        let output = Egress {
            destination: "chat",
            upstream_stream: false,
        }
        .encode(&ready, &mut mapped.writer)
        .unwrap();
        let output: serde_json::Value = serde_json::from_slice(&bytes(&output)).unwrap();
        if pointer.ends_with("arguments") {
            let parsed: serde_json::Value =
                serde_json::from_str(output.pointer(pointer).unwrap().as_str().unwrap()).unwrap();
            assert_eq!(parsed["a"], "a".repeat(8192));
            assert!(
                !canonical_values(&mapped)
                    .iter()
                    .any(|e| e["pointer"] == pointer && e["value"]["value_kind"] == "utf8")
            );
        } else {
            assert_eq!(output.pointer(pointer).unwrap(), &serde_json::json!("a".repeat(8192)));
        }
    }
}

#[test]
fn buffered_response_collects_unfinished_media_without_premature_success() {
    for (opening, payload, closing, pointer, expected) in [
        (
            r#"{"audio":{"data":""#,
            "AAEC".repeat(2048),
            r#"","format":"wav","transcript":"spoken","unknown":7}}"#,
            "/choices/0/message/audio/data",
            "AAEC".repeat(2048),
        ),
        (
            r#"{"content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,"#,
            "AAEC".repeat(2048),
            r#"","ref":"literal"},"detail":"high"}]}"#,
            "/choices/0/message/content/0/image_url/url",
            format!("data:image/png;base64,{}", "AAEC".repeat(2048)),
        ),
        (
            r#"{"content":""#,
            "a".repeat(8192),
            r#""}"#,
            "/choices/0/message/content",
            "a".repeat(8192),
        ),
    ] {
        let (output, _) = buffered_unfinished_media(opening, &payload, closing, pointer, &expected);
        if pointer.ends_with("/data") {
            assert_eq!(output["choices"][0]["message"]["audio"]["transcript"], "spoken");
            assert_eq!(output["choices"][0]["message"]["audio"]["unknown"], 7);
        }
        if pointer.ends_with("/url") {
            assert_eq!(
                output["choices"][0]["message"]["content"][0]["image_url"]["ref"],
                "literal"
            );
        }
    }
}

#[test]
fn buffered_response_collects_unfinished_file_data() {
    let payload = "AAEC".repeat(2048);
    for (opening, expected) in [
        (
            r#"{"content":[{"type":"file","file":{"file_data":"data:application/pdf;base64,"#,
            format!("data:application/pdf;base64,{payload}"),
        ),
        (r#"{"content":[{"type":"file","file":{"file_data":""#, payload.clone()),
    ] {
        let pointer = "/choices/0/message/content/0/file/file_data";
        let closing = r#"","filename":"a.pdf"},"vendor":1}]}"#;
        let (output, prefix) = buffered_unfinished_media(opening, &payload, closing, pointer, &expected);
        assert!(
            prefix.iter().any(|e| e["kind"] == "document"
                && e["attributes"]["value"] == serde_json::json!({"file":{"file_data":null}}))
        );
        let part = &output["choices"][0]["message"]["content"][0];
        assert_eq!(part["file"]["filename"], "a.pdf");
        assert_eq!(part["vendor"], 1);
    }
}

/// Feeds a buffered response whose message stops inside a media payload, checks that the
/// payload's data is published before the body ends and that nothing succeeds early, then
/// completes the body. Returns the buffered output and the canonical records of the prefix.
fn buffered_unfinished_media(
    opening: &str,
    payload: &str,
    closing: &str,
    pointer: &str,
    expected: &str,
) -> (serde_json::Value, Vec<serde_json::Value>) {
    use codec::{BodyMode, ResponseIngress};
    let mut mapped = Mapped::new();
    let mut ingress = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Buffered,
        100_000,
    )
    .unwrap();
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let mut egress = codec::chat::egress::Egress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Buffered,
        true,
        |_| panic!("lost metadata"),
    )
    .unwrap();
    let prefix = format!(r#"{{"id":"r","model":"m","choices":[{{"index":0,"message":{opening}{payload}"#);
    let begin = std::time::Instant::now();
    ingress.advance(&mut mapped.writer, prefix.as_bytes()).unwrap();
    assert!(begin.elapsed() < std::time::Duration::from_millis(100));
    let published = canonical_values(&mapped);
    assert!(
        published
            .iter()
            .any(|e| e.get("bytes").is_some_and(serde_json::Value::is_string))
    );
    let mut output = Vec::new();
    drain(&mut mapped, &mut reader, &mut egress, &mut output);
    assert!(output.is_empty());
    let suffix = format!(r#"{closing},"finish_reason":"stop"}}]}}"#);
    ingress.advance(&mut mapped.writer, suffix.as_bytes()).unwrap();
    assert!(
        !String::from_utf8(mapped.output(&mapped.writer))
            .unwrap()
            .contains("event: x_ck.done\n")
    );
    ingress.finish(&mut mapped.writer).unwrap();
    drain(&mut mapped, &mut reader, &mut egress, &mut output);
    codec::ResponseEgress::finish(&mut egress).unwrap();
    let output: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(output.pointer(pointer).unwrap(), &serde_json::json!(expected));
    (output, published)
}

fn drain(
    mapped: &mut Mapped,
    reader: &mut ck_stream::Reader,
    egress: &mut codec::chat::egress::Egress,
    output: &mut Vec<u8>,
) {
    use codec::ResponseEgress;
    while let std::task::Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        egress
            .advance(&mut mapped.writer, lease, |lease| {
                output.extend(bytes(&lease));
                Ok(())
            })
            .unwrap();
    }
}

#[test]
fn buffered_media_malformed_tail_never_closes_successfully() {
    use codec::{BodyMode, ResponseIngress};
    let mut mapped = Mapped::new();
    let mut ingress = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Buffered,
        100_000,
    )
    .unwrap();
    ingress
        .advance(
            &mut mapped.writer,
            br#"{"choices":[{"index":0,"message":{"audio":{"data":"AAEC"#,
        )
        .unwrap();
    assert!(ingress.advance(&mut mapped.writer, br#"\uDC00"#).is_err());
    let output = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert!(output.contains("event: x_ck.data\n"));
    assert!(!output.contains("event: x_ck.media.end\n"));
    assert!(!output.contains("event: x_ck.done\n"));
}

#[test]
fn payload_before_type_and_opaque_media_lookalikes_preserve_native_values() {
    let parts = [
        serde_json::json!({"image_url":{"url":"data:image/png;base64,AAEC","ref":"literal"},"type":"image_url"}),
        serde_json::json!({"input_audio":{"data":"AAEC","format":"wav"},"type":"input_audio"}),
        serde_json::json!({"file":{"file_data":"AAEC","filename":"f"},"type":"file"}),
        serde_json::json!({"type":"future","image_url":{"url":"not base64"},"input_audio":{"data":"invalid"}}),
    ];
    for part in parts {
        let input = serde_json::json!({"model":"m","messages":[{"role":"user","content":[part.clone()]}],"unknown":{"content":[{"type":"input_audio","input_audio":{"data":"not base64"}}]}});
        let (output, _) = super::request(input.to_string().as_bytes());
        assert_eq!(output["messages"][0]["content"][0], part);
        assert_eq!(output["unknown"], input["unknown"]);
    }
}

/// Small network pieces of one streamed string share data records instead of publishing one
/// frame-aligned record per piece.
#[test]
fn small_pieces_of_a_streamed_string_share_data_records() {
    let mut mapped = Mapped::configured(std::sync::Arc::new(ck_stream::ManualClock::default()), 64);
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        100_000,
    )
    .unwrap();
    let message = format!(r#"{{"role":"user","content":"{}"}}"#, "a".repeat(2048));
    let input = format!(r#"{{"model":"m","messages":[{}]}}"#, vec![message; 8].join(","));
    for piece in input.as_bytes().chunks(16) {
        ingress.advance(&mut mapped.writer, piece).unwrap();
    }
    ingress.finish(&mut mapped.writer, &ChatCapability).unwrap();
    let wire = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert_eq!(wire.matches("event: x_ck.data\n").count(), 8);
}

/// Held string input reports a deadline at most 100 ms after collection starts, which later
/// input does not move, and a flush publishes it.
#[test]
fn held_string_input_has_a_deadline_and_flushes() {
    let mut mapped = Mapped::new();
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        100_000,
    )
    .unwrap();
    let mut prefix = br#"{"model":"m","messages":[{"role":"user","content":""#.to_vec();
    prefix.extend(std::iter::repeat_n(b'a', 2048));
    let began = std::time::Instant::now();
    ingress.advance(&mut mapped.writer, &prefix).unwrap();
    let deadline = ingress.flush_deadline().unwrap();
    assert!(deadline > began && deadline <= std::time::Instant::now() + std::time::Duration::from_millis(100));
    ingress.advance(&mut mapped.writer, b"bb").unwrap();
    assert_eq!(ingress.flush_deadline(), Some(deadline));
    let data = |mapped: &Mapped| {
        String::from_utf8(mapped.output(&mapped.writer))
            .unwrap()
            .matches("event: x_ck.data\n")
            .count()
    };
    assert_eq!(data(&mapped), 0);
    ingress.flush(&mut mapped.writer).unwrap();
    assert!(ingress.flush_deadline().is_none());
    assert_eq!(data(&mapped), 1);
}

/// A long string fills data records of at least 28,672 input bytes, each within one frame, and
/// its content survives intact.
#[test]
fn long_streamed_string_fills_data_records() {
    let mut mapped = Mapped::new();
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        1_000_000,
    )
    .unwrap();
    let content = "a".repeat(100_000);
    let input = format!(r#"{{"model":"m","messages":[{{"role":"user","content":"{content}"}}]}}"#);
    for piece in input.as_bytes().chunks(1000) {
        ingress.advance(&mut mapped.writer, piece).unwrap();
    }
    let ready = ingress.finish(&mut mapped.writer, &ChatCapability).unwrap();
    let output = Egress {
        destination: "chat",
        upstream_stream: false,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    let output: serde_json::Value = serde_json::from_slice(&bytes(&output)).unwrap();
    assert_eq!(output["messages"][0]["content"], content);
    let wire = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert_eq!(wire.matches("event: x_ck.data\n").count(), 4);
}
