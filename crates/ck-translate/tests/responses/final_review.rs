//! Reproductions from the final codec review: each test pins one corrected mapping.
use super::cross::{chat_to_responses, client_events, sse};
use super::*;

/// Buffered Responses `body` replayed to a streaming client; returns the client events.
fn streamed(body: &Value) -> Vec<Value> {
    let (out, _) = response(
        body.to_string().as_bytes(),
        BodyMode::Buffered,
        BodyMode::Streaming,
        Operation::Generate,
    );
    client_events(&out)
}

fn named<'a>(events: &'a [Value], name: &str) -> Vec<&'a Value> {
    events.iter().filter(|e| e["type"] == name).collect()
}

#[test]
fn announced_parts_and_calls_carry_their_empty_accumulators() {
    let text = json!({"id":"r","status":"completed","output":[{"id":"i","type":"message","role":"assistant",
        "content":[{"type":"output_text","text":"hi","annotations":[]},{"type":"refusal","refusal":"no"}]}]});
    let events = streamed(&text);
    let parts = named(&events, "response.content_part.added");
    assert_eq!(parts[0]["part"]["text"], "");
    assert_eq!(parts[1]["part"], json!({"type":"refusal","refusal":""}));
    let call = json!({"id":"r","status":"completed","output":[
        {"id":"i","type":"function_call","name":"f","call_id":"c","arguments":"{}"},
        {"id":"j","type":"custom_tool_call","name":"g","call_id":"d","input":"x"}]});
    let events = streamed(&call);
    let added = named(&events, "response.output_item.added");
    assert_eq!(added[0]["item"]["arguments"], "");
    assert_eq!(added[1]["item"]["input"], "");
    let chat = sse(&[
        json!({"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call","type":"function","function":{"name":"f","arguments":"{}"}}]},"finish_reason":null}]}),
        json!({"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}),
    ]);
    let events = client_events(&chat_to_responses(
        chat.as_bytes(),
        BodyMode::Streaming,
        BodyMode::Streaming,
    ));
    let added = named(&events, "response.output_item.added");
    let call = added.iter().find(|e| e["item"]["type"] == "function_call").unwrap();
    assert_eq!(call["item"]["arguments"], "");
    let text = sse(&[
        json!({"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":"hi"},"finish_reason":null}]}),
        json!({"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
    ]);
    let events = client_events(&chat_to_responses(
        text.as_bytes(),
        BodyMode::Streaming,
        BodyMode::Streaming,
    ));
    let part = named(&events, "response.content_part.added");
    assert_eq!(
        part[0]["part"],
        json!({"type":"output_text","text":"","annotations":[]})
    );
}

/// Encodes `input` after the trusted reader rebuilt it from the canonical records; `edit` may
/// change the rebuilt document first.
fn rebuilt(input: &Value, operation: Operation, edit: impl FnOnce(&mut codec::document::Document)) -> Value {
    try_rebuilt(input, operation, edit).unwrap()
}

fn try_rebuilt(
    input: &Value,
    operation: Operation,
    edit: impl FnOnce(&mut codec::document::Document),
) -> codec::Result<Value> {
    let mut m = Mapped::new();
    let mut ingress = responses::request::Ingress::new(
        &mut m.writer,
        &m.store,
        identity(Direction::Request),
        operation,
        1024 * 1024,
    )
    .unwrap();
    ingress
        .advance(&mut m.writer, &serde_json::to_vec(input).unwrap())
        .unwrap();
    ingress
        .finish(&mut m.writer, &responses::request::ResponsesCapability)
        .unwrap();
    let mut reader = m.store.reader(m.writer.id(), 0).unwrap();
    let (mut doc, media) = codec::chat::request::read_request(&mut m.writer, &mut reader).unwrap();
    edit(&mut doc);
    let ready = codec::RequestFence::default().end(
        codec::RequestView {
            document: &doc,
            media: &media,
            operation,
        },
        &responses::request::ResponsesCapability,
    )?;
    let output = responses::request::Egress {
        destination: "responses",
        upstream_stream: false,
    }
    .encode(&ready, &mut m.writer)?;
    Ok(serde_json::from_slice(&bytes(&output)).unwrap())
}

#[test]
fn provenance_survives_the_canonical_records() {
    for (input, operation) in [
        (
            json!({"model":"m","input":[{"type":"compaction","encrypted_content":"E"}],"vendor":7}),
            Operation::Compact,
        ),
        (
            json!({"model":"m","instructions":"be brief","input":[{"role":"developer","content":"d"},
                {"type":"reasoning","id":"rs","summary":[{"type":"summary_text","text":"s"}],"encrypted_content":"E"},
                {"type":"future_item","payload":1},
                {"role":"assistant","content":[{"type":"future_part","x":1}]}],
                "tools":[{"type":"web_search"},{"type":"function","name":"f","parameters":{},"vendor":1}]}),
            Operation::Generate,
        ),
    ] {
        let (direct, _) = request(&input, operation);
        assert_eq!(rebuilt(&input, operation, |_| {}), direct, "{input}");
    }
}

/// Detaches content part `part` of message `message` from a rebuilt document.
fn detach_part(doc: &mut codec::document::Document, message: usize, part: usize) {
    let messages = doc.required(doc.root(), "messages").unwrap();
    let message = doc.children(messages).unwrap().nth(message).unwrap();
    let content = doc.required(message, "content").unwrap();
    let part = doc.children(content).unwrap().nth(part).unwrap();
    doc.detach(content, part).unwrap();
}

#[test]
fn deleted_encrypted_parts_take_their_replay_payload_with_them() {
    let reasoning = |text: &str, data: &str| json!({"type":"reasoning","summary":[{"type":"summary_text","text":text}],"encrypted_content":data});
    let input = json!({"model":"m","input":[reasoning("a","E1"), reasoning("b","E2")]});
    let output = rebuilt(&input, Operation::Generate, |doc| detach_part(doc, 0, 1));
    assert_eq!(
        output["input"][0],
        json!({"type":"reasoning","summary":[{"type":"summary_text","text":"a"}]})
    );
    assert_eq!(output["input"][1], reasoning("b", "E2"));
    let output = rebuilt(&input, Operation::Generate, |doc| {
        let messages = doc.required(doc.root(), "messages").unwrap();
        let first = doc.node(messages).unwrap().first.unwrap();
        doc.detach(messages, first).unwrap();
    });
    assert_eq!(output["input"], json!([reasoning("b", "E2")]));
    // An encrypted part moved to another message keeps naming its own message's payload.
    let moved = try_rebuilt(&input, Operation::Generate, |doc| {
        let messages = doc.required(doc.root(), "messages").unwrap();
        let [first, second] = [0, 1].map(|i| doc.children(messages).unwrap().nth(i).unwrap());
        let [from, to] = [second, first].map(|m| doc.required(m, "content").unwrap());
        let own = doc.children(to).unwrap().nth(1).unwrap();
        doc.detach(to, own).unwrap();
        let part = doc.children(from).unwrap().nth(1).unwrap();
        doc.detach(from, part).unwrap();
        doc.append(to, part).unwrap();
    });
    assert!(moved.is_err());
}

/// Runs a Responses request to the fence; returns whether `request.end` was published.
fn fenced(input: &Value) -> bool {
    let mut m = Mapped::new();
    let mut ingress = responses::request::Ingress::new(
        &mut m.writer,
        &m.store,
        identity(Direction::Request),
        Operation::Generate,
        1024 * 1024,
    )
    .unwrap();
    let advanced = ingress.advance(&mut m.writer, &serde_json::to_vec(input).unwrap());
    let ended = advanced.is_ok()
        && ingress
            .finish(&mut m.writer, &responses::request::ResponsesCapability)
            .is_ok();
    let published = String::from_utf8(m.output(&m.writer))
        .unwrap()
        .contains("event: x_ck.request.end\n");
    assert_eq!(ended, published);
    ended
}

#[test]
fn the_fence_refuses_missing_text_and_null_continuations() {
    for part in [
        json!({"type":"refusal"}),
        json!({"type":"output_text"}),
        json!({"type":"refusal","refusal":7}),
    ] {
        assert!(
            !fenced(&json!({"model":"m","input":[{"role":"assistant","content":[part]}]})),
            "{part}"
        );
    }
    let summary = json!({"type":"reasoning","summary":[{"type":"summary_text"}]});
    assert!(!fenced(&json!({"model":"m","input":[summary]})));
    let output = json!({"type":"function_call_output","call_id":"missing","output":"ok"});
    for continuation in [
        json!({"previous_response_id":null}),
        json!({"conversation":null}),
        json!({"conversation":{"vendor":1}}),
    ] {
        let mut input = json!({"model":"m","input":[output]});
        input
            .as_object_mut()
            .unwrap()
            .extend(continuation.as_object().unwrap().clone());
        assert!(!fenced(&input), "{continuation}");
    }
    for continuation in [
        json!({"previous_response_id":"resp"}),
        json!({"conversation":"conv"}),
        json!({"conversation":{"id":"conv"}}),
    ] {
        let mut input = json!({"model":"m","input":[output]});
        input
            .as_object_mut()
            .unwrap()
            .extend(continuation.as_object().unwrap().clone());
        assert!(fenced(&input), "{continuation}");
    }
}

/// Canonical `records` through Responses egress in `mode`; returns the client bytes and the
/// names of the diagnostics egress reported.
fn egress_of(
    records: &[(codec::event::Kind, codec::event::Scope, &str)],
    mode: BodyMode,
) -> (Vec<u8>, Vec<&'static str>) {
    let mut m = Mapped::new();
    let mut reader = m.store.reader(m.writer.id(), 0).unwrap();
    let mut emitter = codec::event::Emitter::new(identity(Direction::Response));
    for (kind, scope, payload) in records {
        emitter
            .emit(
                &mut m.writer,
                *kind,
                *scope,
                |e| e.bytes(payload.as_bytes()),
                |_| Ok(()),
            )
            .unwrap();
    }
    let diagnostics = std::cell::RefCell::new(Vec::new());
    let mut egress = responses::egress::Egress::new(
        &mut m.writer,
        identity(Direction::Response),
        Operation::Generate,
        mode,
        |name, _| diagnostics.borrow_mut().push(name),
    )
    .unwrap();
    let mut out = Vec::new();
    while let std::task::Poll::Ready(Some(ck_stream::Read::Record(record))) = reader.try_next() {
        egress
            .advance(&mut m.writer, record, |chunk| {
                out.extend_from_slice(&bytes(&chunk));
                Ok(())
            })
            .unwrap();
    }
    egress.finish().unwrap();
    drop(egress);
    (out, diagnostics.into_inner())
}

/// A message item scope with an optional block.
fn scope(block: Option<u64>) -> codec::event::Scope {
    codec::event::Scope {
        choice_index: Some(0),
        item_id: Some(codec::event::LocalId::new("i0").unwrap()),
        block_index: block,
        ..codec::event::Scope::default()
    }
}

#[test]
fn redacted_reasoning_has_no_responses_carrier() {
    use codec::event::{Kind, Scope};
    // Native encrypted content is a signature whatever its spelling.
    let carrier = "claude-redacted-thinking:opaque";
    let body = json!({"id":"r","status":"completed","output":[{"id":"rs","type":"reasoning","summary":[],"encrypted_content":carrier}]});
    let (out, canonical) = response(
        body.to_string().as_bytes(),
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Generate,
    );
    assert!(
        canonical
            .iter()
            .any(|e| e["kind"] == "responses_encrypted" && e["fragment"] == carrier)
    );
    assert!(!canonical.iter().any(|e| e.get("data").is_some()));
    assert_eq!(
        serde_json::from_slice::<Value>(&out).unwrap()["output"][0]["encrypted_content"],
        carrier
    );
    // Redacted reasoning from another protocol has no place in Responses and is dropped.
    let chunk = r#","object":"chat.completion.chunk","id":"r","model":"m","created":1,"choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]"#;
    let finish = r#","object":"chat.completion.chunk","id":"r","model":"m","created":1,"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]"#;
    for start in [
        r#","kind":"redacted""#,
        r#","kind":"thinking","attributes":{"type":"redacted"}"#,
    ] {
        let records = [
            (
                Kind::ResponseStart,
                Scope::default(),
                r#","id":"r","model":"m","created":1"#,
            ),
            (Kind::ItemStart, scope(None), r#","kind":"message","role":"assistant""#),
            (Kind::BlockStart, scope(Some(0)), start),
            (Kind::ReasoningRedacted, scope(Some(0)), r#","data":"opaque""#),
            (Kind::BlockEnd, scope(Some(0)), ""),
            (Kind::BlockStart, scope(Some(1)), r#","kind":"text""#),
            (Kind::Chunk, scope(Some(1)), chunk),
            (Kind::BlockEnd, scope(Some(1)), ""),
            (Kind::ItemEnd, scope(None), r#","status":"completed""#),
            (
                Kind::Chunk,
                Scope {
                    choice_index: Some(0),
                    ..Scope::default()
                },
                finish,
            ),
            (Kind::Done, Scope::default(), ""),
        ];
        for (records, expected) in [(
            &records[..],
            json!([{"type":"output_text","text":"hi","annotations":[]}]),
        )] {
            for mode in [BodyMode::Buffered, BodyMode::Streaming] {
                let (out, diagnostics) = egress_of(records, mode);
                let text = String::from_utf8(out.clone()).unwrap();
                assert!(
                    !text.contains("opaque") && !text.contains("encrypted_content") && !text.contains("reasoning"),
                    "{text}"
                );
                let body = match mode {
                    BodyMode::Buffered => serde_json::from_slice::<Value>(&out).unwrap(),
                    _ => client_events(&out).last().unwrap()["response"].clone(),
                };
                let output = body["output"].as_array().unwrap();
                assert_eq!(
                    output.first().map_or(Value::Null, |item| item["content"].clone()),
                    expected,
                    "{text}"
                );
                assert!(output.len() <= 1);
                assert!(diagnostics.contains(&"redacted reasoning"), "{diagnostics:?}");
            }
        }
    }
}
