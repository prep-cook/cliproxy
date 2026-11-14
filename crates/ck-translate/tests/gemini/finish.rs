//! Finish reasons in both directions: Chat values, the exact native reason and its message.
use super::*;
use gemini::response::Framing;

fn finished(parts: Value, reason: &str) -> (Value, Value) {
    let body = json!({"candidates":[{"index":0,"content":{"role":"model","parts":parts},"finishReason":reason}]});
    let (out, canonical) = response(&sse(&[body]), Framing::Sse, Framing::Json, Operation::Generate);
    let chunk = canonical
        .iter()
        .find(|v| v["choices"][0]["finish_reason"].is_string())
        .unwrap()
        .clone();
    (chunk, serde_json::from_slice(&out).unwrap())
}

#[test]
fn native_finish_reasons_map_to_chat_and_survive_native_egress() {
    let call = json!([{"functionCall":{"name":"f","args":{}}}]);
    let text = json!([{"text":"a"}]);
    let cases = [
        (&call, "STOP", "tool_calls"),
        (&text, "STOP", "stop"),
        (&text, "MAX_TOKENS", "length"),
        (&call, "MAX_TOKENS", "length"),
        (&text, "SAFETY", "content_filter"),
        (&text, "RECITATION", "content_filter"),
        (&text, "BLOCKLIST", "content_filter"),
        (&text, "PROHIBITED_CONTENT", "content_filter"),
        (&text, "SPII", "content_filter"),
        (&text, "IMAGE_SAFETY", "content_filter"),
        (&text, "OTHER", "stop"),
        (&text, "LANGUAGE", "stop"),
        (&call, "MALFORMED_FUNCTION_CALL", "stop"),
        (&text, "FINISH_REASON_UNSPECIFIED", "stop"),
        (&text, "FUTURE_REASON", "stop"),
    ];
    for (parts, native, chat) in cases {
        let (chunk, out) = finished(parts.clone(), native);
        assert_eq!(chunk["choices"][0]["finish_reason"], chat, "{native}");
        assert_eq!(chunk["x_ck"]["native_finish_reason"], native);
        assert_eq!(out["candidates"][0]["finishReason"], native);
    }
}

#[test]
fn finish_message_travels_with_the_finish() {
    let body = json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"a"}]},"finishReason":"MALFORMED_FUNCTION_CALL","finishMessage":"Malformed function call: x"}]});
    let (out, canonical) = response(
        &sse(std::slice::from_ref(&body)),
        Framing::Sse,
        Framing::Json,
        Operation::Generate,
    );
    let finish = canonical
        .iter()
        .find(|v| v["choices"][0]["finish_reason"].is_string())
        .unwrap();
    assert_eq!(
        finish["x_ck"]["original"],
        json!([{"protocol":"gemini","pointer":"/finishMessage","value":"Malformed function call: x"}])
    );
    assert!(!canonical.iter().any(|v| v["name"] == "gemini.candidate"));
    let out: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(out["candidates"], body["candidates"]);
}

/// A finish from another protocol reaches Gemini under a Gemini reason, never its foreign
/// native spelling.
#[test]
fn foreign_finish_reasons_reach_gemini_as_gemini_reasons() {
    for (chat, native) in [
        ("stop", "STOP"),
        ("tool_calls", "STOP"),
        ("length", "MAX_TOKENS"),
        ("content_filter", "SAFETY"),
        ("future", "OTHER"),
    ] {
        let body = json!({"id":"c","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":chat}]});
        let mut mapped = Mapped::new();
        let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
        let mut ingress = codec::chat::response::Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Response),
            Operation::Generate,
            BodyMode::Buffered,
            1024 * 1024,
        )
        .unwrap();
        ingress
            .advance(&mut mapped.writer, &serde_json::to_vec(&body).unwrap())
            .unwrap();
        ingress.finish(&mut mapped.writer).unwrap();
        let mut egress =
            gemini::egress::Egress::with_framing(&mut mapped.writer, Operation::Generate, Framing::Json, |_| {})
                .unwrap();
        let mut output = Vec::new();
        while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
            egress
                .advance(&mut mapped.writer, lease, |part| {
                    output.extend_from_slice(&bytes(&part));
                    Ok(())
                })
                .unwrap();
        }
        egress.finish().unwrap();
        let out: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(out["candidates"][0]["finishReason"], native, "{chat}");
    }
}
