use super::*;
use base64::Engine;

#[test]
fn unfinished_text_retires_raw_frames_and_preserves_late_unicode() {
    for (opening, closing, pointer) in [
        (r#"{"model":"m","input":""#, r#""}"#, "/input/0/content"),
        (r#"{"model":"m","instructions":""#, r#"","input":[]}"#, "/instructions"),
        (
            r#"{"model":"m","input":[{"role":"user","content":[{"type":"input_text","text":""#,
            r#""}]}]}"#,
            "/input/0/content/0/text",
        ),
        (
            r#"{"model":"m","input":[],"vendor":{"a/b~c":""#,
            r#""}}"#,
            "/vendor/a~1b~0c",
        ),
    ] {
        let mut mapped = Mapped::new();
        let mut ingress = responses::request::Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Request),
            Operation::Generate,
            200_000,
        )
        .unwrap();
        ingress.advance(&mut mapped.writer, opening.as_bytes()).unwrap();
        for chunk in vec![b'a'; 180_003].chunks(7) {
            ingress.advance(&mut mapped.writer, chunk).unwrap();
        }
        let prefix = events(&mapped);
        assert!(
            prefix
                .iter()
                .any(|event| event["pointer"] == "/model" && event["value"] == "m")
        );
        assert!(prefix.iter().any(|event| event.get("bytes").is_some()));
        let raw = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
        assert!(!raw.contains("event: x_ck.request.end\n"));
        assert!(!raw.contains("event: x_ck.media.end\n"));
        // The first frame retains native names; the tail retains unconsumed bytes.
        assert!(
            u64::from(mapped.store.used_frames()) <= mapped.writer.committed().frame + 3,
            "raw payload remains charged"
        );
        for tail in [br#"\uD83D"#.as_slice(), br#"\uDE80"#, b"\xc3", b"\xa9", br#"\n"#] {
            ingress.advance(&mut mapped.writer, tail).unwrap();
        }
        ingress.advance(&mut mapped.writer, closing.as_bytes()).unwrap();
        let ready = ingress
            .finish(&mut mapped.writer, &responses::request::ResponsesCapability)
            .unwrap();
        let output = responses::request::Egress {
            destination: "responses",
            upstream_stream: false,
        }
        .encode(&ready, &mut mapped.writer)
        .unwrap();
        let output: Value = serde_json::from_slice(&bytes(&output)).unwrap();
        assert_eq!(
            output.pointer(pointer).unwrap(),
            &json!(format!("{}🚀é\n", "a".repeat(180_003)))
        );
        let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
        let (document, _) = codec::chat::request::read_request(&mut mapped.writer, &mut reader).unwrap();
        assert!(
            document
                .string_is(document.required(document.root(), "model").unwrap(), "m")
                .unwrap()
        );
        let mut paths = std::collections::HashSet::new();
        for event in events(&mapped) {
            if let Some(path) = event["pointer"].as_str() {
                assert!(paths.insert(path.to_owned()), "duplicate assignment: {path}");
            }
        }
    }
}

#[test]
fn malformed_tail_after_incremental_data_cannot_cross_request_fence() {
    for tail in [br#"\uD800x"#.as_slice(), br#"","\u006dodel":"other"}"#] {
        let mut mapped = Mapped::new();
        let mut ingress = responses::request::Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Request),
            Operation::Generate,
            200_000,
        )
        .unwrap();
        ingress
            .advance(&mut mapped.writer, br#"{"model":"m","input":""#)
            .unwrap();
        ingress.advance(&mut mapped.writer, &vec![b'a'; 100_003]).unwrap();
        assert!(events(&mapped).iter().any(|event| event.get("bytes").is_some()));
        assert!(ingress.advance(&mut mapped.writer, tail).is_err());
        let output = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
        assert!(output.contains("event: x_ck.error\n"));
        assert!(!output.contains("event: x_ck.request.end\n"));
        assert!(!output.contains("event: x_ck.done\n"));
    }
}

#[test]
fn unfinished_native_image_has_one_lifetime_and_exact_decoded_bytes() {
    let payload: Vec<u8> = (0..80_003).map(|index| index as u8).collect();
    let encoded = base64::engine::general_purpose::STANDARD.encode(&payload);
    let mut mapped = Mapped::new();
    let mut ingress = responses::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        200_000,
    )
    .unwrap();
    ingress.advance(&mut mapped.writer, br#"{"model":"m","input":[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,"#).unwrap();
    for chunk in encoded.as_bytes().chunks(7) {
        ingress.advance(&mut mapped.writer, chunk).unwrap();
    }
    let prefix = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert_eq!(prefix.matches("event: x_ck.media.start\n").count(), 1);
    assert!(prefix.contains("event: x_ck.data\n"));
    assert!(!prefix.contains("event: x_ck.media.end\n"));
    ingress
        .advance(&mut mapped.writer, br#"","detail":"high"}]}]}"#)
        .unwrap();
    let ready = ingress
        .finish(&mut mapped.writer, &responses::request::ResponsesCapability)
        .unwrap();
    let output = responses::request::Egress {
        destination: "responses",
        upstream_stream: false,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    let output: Value = serde_json::from_slice(&bytes(&output)).unwrap();
    assert_eq!(
        output["input"][0]["content"][0]["image_url"],
        format!("data:image/png;base64,{encoded}")
    );
    assert_eq!(output["input"][0]["content"][0]["detail"], "high");
    let wire = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert_eq!(wire.matches("event: x_ck.media.start\n").count(), 1);
    assert_eq!(wire.matches("event: x_ck.media.end\n").count(), 1);
    let decoded: Vec<u8> = events(&mapped)
        .iter()
        .filter_map(|event| event["bytes"].as_str())
        .flat_map(|bytes| base64::engine::general_purpose::STANDARD.decode(bytes).unwrap())
        .collect();
    assert_eq!(decoded, payload);
}

#[test]
fn buffered_data_fragment_sets_a_fixed_flush_deadline() {
    let mut mapped = Mapped::new();
    let mut ingress = responses::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        200_000,
    )
    .unwrap();
    let prefix = r#"{"model":"m","input":[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AAAA"#;
    ingress.advance(&mut mapped.writer, prefix.as_bytes()).unwrap();
    assert!(
        ingress.flush_deadline().is_none(),
        "first fragment is published at once"
    );
    ingress.advance(&mut mapped.writer, b"BBBB").unwrap();
    let deadline = ingress.flush_deadline().expect("buffered fragment needs a deadline");
    assert!(deadline <= std::time::Instant::now() + std::time::Duration::from_millis(100));
    ingress.advance(&mut mapped.writer, b"CCCC").unwrap();
    assert_eq!(
        ingress.flush_deadline(),
        Some(deadline),
        "later input postponed the deadline"
    );
    let published = mapped.output(&mapped.writer).len();
    ingress.flush(&mut mapped.writer).unwrap();
    assert!(ingress.flush_deadline().is_none());
    assert!(
        mapped.output(&mapped.writer).len() > published,
        "flush published nothing"
    );
}
