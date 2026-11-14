//! Reproductions from the codec review: each test pins one corrected mapping.
use super::*;

/// Runs a Responses request through ingress and the Chat capability fence.
fn to_chat(input: &Value) -> codec::Result<()> {
    let mut m = Mapped::new();
    let mut ingress = responses::request::Ingress::new(
        &mut m.writer,
        &m.store,
        identity(Direction::Request),
        Operation::Generate,
        1024 * 1024,
    )
    .unwrap();
    ingress
        .advance(&mut m.writer, &serde_json::to_vec(input).unwrap())
        .unwrap();
    ingress
        .finish(&mut m.writer, &codec::chat::request::ChatCapability)
        .map(|_| ())
}

#[test]
fn json_schema_format_is_flat_in_responses_and_nested_in_chat() {
    let output = super::request::from_chat(&json!({"model":"m","messages":[{"role":"user","content":"q"}],
        "response_format":{"type":"json_schema","json_schema":{"name":"answer","schema":{"type":"object"},"strict":true}}}));
    assert_eq!(
        output["text"]["format"],
        json!({"type":"json_schema","name":"answer","schema":{"type":"object"},"strict":true})
    );
    let flat = json!({"model":"m","input":"q",
        "text":{"format":{"type":"json_schema","name":"answer","description":"d","schema":{"type":"object"},"strict":true}}});
    assert!(to_chat(&flat).is_ok());
    let (output, _) = request(&flat, Operation::Generate);
    assert_eq!(output["text"], flat["text"]);
}

#[test]
fn continuation_tool_outputs_link_to_the_referenced_state() {
    let output = json!({"type":"function_call_output","call_id":"c","output":"done"});
    for (field, value) in [("previous_response_id", json!("prev")), ("conversation", json!("conv"))] {
        let mut input = json!({"model":"m","input":[output]});
        input[field] = value;
        let (encoded, _) = request(&input, Operation::Generate);
        assert_eq!(encoded[field], input[field]);
        assert_eq!(encoded["input"][0], output);
    }
}

/// Runs `input` through `ingress` and Responses request egress; returns the raw upstream body.
fn encoded(ingress: &mut impl RequestIngress, writer: &mut ck_stream::Writer, input: &Value) -> String {
    ingress.advance(writer, &serde_json::to_vec(input).unwrap()).unwrap();
    let ready = ingress
        .finish(writer, &responses::request::ResponsesCapability)
        .unwrap();
    let output = responses::request::Egress {
        destination: "responses",
        upstream_stream: false,
    }
    .encode(&ready, writer)
    .unwrap();
    String::from_utf8(bytes(&output)).unwrap()
}

/// Encodes a Responses request and returns the raw upstream body.
fn raw_request(input: &Value) -> String {
    let mut m = Mapped::new();
    let mut ingress = responses::request::Ingress::new(
        &mut m.writer,
        &m.store,
        identity(Direction::Request),
        Operation::Generate,
        1024 * 1024,
    )
    .unwrap();
    encoded(&mut ingress, &mut m.writer, input)
}

#[test]
fn text_format_and_verbosity_share_one_text_object() {
    let input = json!({"model":"m","input":"q","text":{"format":{"type":"json_object"},"verbosity":"low"}});
    let raw = raw_request(&input);
    assert_eq!(raw.matches("\"text\":").count(), 1, "{raw}");
    let output: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(output["text"], input["text"]);
}

/// Encodes a Chat request for a Responses upstream and returns the raw body.
fn chat_raw(input: &Value) -> String {
    let mut m = Mapped::new();
    let mut ingress = codec::chat::request::Ingress::new(
        &mut m.writer,
        &m.store,
        identity(Direction::Request),
        Operation::Generate,
        1024 * 1024,
    )
    .unwrap();
    encoded(&mut ingress, &mut m.writer, input)
}

#[test]
fn chat_token_limits_become_max_output_tokens() {
    let messages = json!([{"role":"user","content":"q"}]);
    for (input, limit) in [
        (json!({"model":"m","messages":messages,"max_tokens":17}), 17),
        (
            json!({"model":"m","messages":messages,"max_tokens":17,"max_completion_tokens":9}),
            9,
        ),
    ] {
        let raw = chat_raw(&input);
        assert_eq!(raw.matches("\"max_output_tokens\":").count(), 1, "{raw}");
        let output: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(output["max_output_tokens"], limit, "{output}");
        assert!(output.get("max_tokens").is_none(), "{output}");
    }
}

#[test]
fn tool_declarations_keep_order_and_unknown_fields() {
    let input = json!({"model":"m","input":"q","tools":[
        {"type":"web_search"},
        {"type":"function","name":"f","parameters":{},"namespace":"ns","vendor":{"a":null}},
        {"type":"file_search","vector_store_ids":["v"]},
        {"type":"function","name":"g","parameters":{}},
    ]});
    let (output, _) = request(&input, Operation::Generate);
    assert_eq!(output["tools"], input["tools"]);
}

#[test]
fn incomplete_calls_end_incomplete_with_their_arguments() {
    for (arguments, valid) in [("{\"x\":", false), ("{}", true)] {
        let input = json!({"id":"r","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},
            "output":[{"id":"i","type":"function_call","call_id":"c","name":"f","arguments":arguments,"status":"incomplete"}]});
        let (out, events) = response(
            &serde_json::to_vec(&input).unwrap(),
            BodyMode::Buffered,
            BodyMode::Buffered,
            Operation::Generate,
        );
        let end = events.iter().find(|e| e.get("valid_json").is_some()).unwrap();
        assert_eq!(end["status"], "incomplete", "{end}");
        assert_eq!(end["valid_json"], valid, "{end}");
        let out: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(out["status"], "incomplete");
        assert_eq!(out["output"][0]["arguments"], arguments);
        assert_eq!(out["output"][0]["status"], "incomplete");
    }
}

#[test]
fn chat_reasoning_becomes_a_reasoning_item_before_the_message() {
    use super::cross::{chat_to_responses, client_events, sse};
    // Raw bytes keep the source order: reasoning before content.
    let chunk = |delta: Value, finish: Value| json!({"id":"r","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    let streamed = sse(&[
        chunk(json!({"role":"assistant","reasoning_content":"think"}), Value::Null),
        chunk(json!({"content":"answer"}), Value::Null),
        chunk(json!({}), json!("stop")),
    ]);
    // Chat ingress opens a buffered message's text block before its reasoning block, so only a
    // streamed source puts the reasoning first.
    for (input, mode) in [(streamed.as_bytes(), BodyMode::Streaming)] {
        let body: Value = serde_json::from_slice(&chat_to_responses(input, mode, BodyMode::Buffered)).unwrap();
        let events = client_events(&chat_to_responses(input, mode, BodyMode::Streaming));
        for output in [&body["output"], &events.last().unwrap()["response"]["output"]] {
            let kinds: Vec<_> = output.as_array().unwrap().iter().map(|i| i["type"].clone()).collect();
            assert_eq!(kinds, [json!("reasoning"), json!("message")], "{output}");
            assert_eq!(output[0]["content"][0]["type"], "reasoning_text");
            assert_eq!(output[0]["content"][0]["text"], "think");
            assert_eq!(output[0]["status"], "completed");
            assert_eq!(output[1]["content"].as_array().unwrap().len(), 1);
            assert_eq!(output[1]["content"][0]["text"], "answer");
        }
        // Chat keeps its blocks open until the choice ends, so the two items interleave.
        for (kind, index, item) in [
            ("added", 0, "reasoning"),
            ("added", 1, "message"),
            ("done", 0, "reasoning"),
            ("done", 1, "message"),
        ] {
            let name = format!("response.output_item.{kind}");
            let event = events
                .iter()
                .find(|e| e["type"] == name && e["item"]["type"] == item)
                .unwrap();
            assert_eq!(event["output_index"], index, "{event}");
        }
        let at = |name: &str| events.iter().position(|e| e["type"] == name).unwrap();
        assert!(at("response.output_item.added") < at("response.content_part.added"));
        let message = events
            .iter()
            .position(|e| e["type"] == "response.output_item.added" && e["item"]["type"] == "message");
        assert!(message.unwrap() < at("response.output_text.delta"));
    }
    let only = sse(&[
        chunk(json!({"role":"assistant","reasoning_content":"think"}), Value::Null),
        chunk(json!({}), json!("stop")),
    ]);
    for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
        let raw = chat_to_responses(only.as_bytes(), BodyMode::Streaming, downstream);
        let body = match downstream {
            BodyMode::Buffered => serde_json::from_slice(&raw).unwrap(),
            BodyMode::Streaming => client_events(&raw).last().unwrap()["response"].clone(),
        };
        let kinds: Vec<_> = body["output"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["type"].clone())
            .collect();
        assert_eq!(kinds, [json!("reasoning")], "{body}");
    }
}

#[test]
fn text_parts_publish_their_done_events_before_the_part_ends() {
    let input = json!({"id":"r","status":"completed","output":[
        {"id":"rs","type":"reasoning","summary":[{"type":"summary_text","text":"sum"}],"content":[{"type":"reasoning_text","text":"think"}]},
        {"id":"i","type":"message","role":"assistant","content":[{"type":"output_text","text":"hi"},{"type":"refusal","refusal":"no"}]}]});
    let (out, _) = response(
        &serde_json::to_vec(&input).unwrap(),
        BodyMode::Buffered,
        BodyMode::Streaming,
        Operation::Generate,
    );
    let events = super::cross::client_events(&out);
    for (name, field, value, part) in [
        (
            "response.reasoning_summary_text.done",
            "text",
            "sum",
            "response.reasoning_summary_part.done",
        ),
        (
            "response.reasoning_text.done",
            "text",
            "think",
            "response.content_part.done",
        ),
        ("response.output_text.done", "text", "hi", "response.content_part.done"),
        ("response.refusal.done", "refusal", "no", "response.content_part.done"),
    ] {
        let at = events
            .iter()
            .position(|e| e["type"] == name)
            .unwrap_or_else(|| panic!("{name} missing"));
        assert_eq!(events[at][field], value);
        assert_eq!(events[at + 1]["type"], part, "{name}");
    }
}

#[test]
fn late_part_attributes_reach_the_client() {
    let message = |content: Value| json!({"id":"i","type":"message","role":"assistant","content":content});
    let part = json!({"type":"output_text","text":"hi","vendor":{"a":null}});
    let records = [
        json!({"type":"response.output_item.added","output_index":0,"item":message(json!([]))}),
        json!({"type":"response.output_text.delta","output_index":0,"item_id":"i","content_index":0,"delta":"hi"}),
        json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[message(json!([part]))]}}),
    ];
    let (out, _) = response(
        wire(&records).as_bytes(),
        BodyMode::Streaming,
        BodyMode::Buffered,
        Operation::Generate,
    );
    let raw = String::from_utf8(out).unwrap();
    assert_eq!(raw.matches("\"type\":\"output_text\"").count(), 1, "{raw}");
    let out: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(out["output"][0]["content"][0]["vendor"], part["vendor"], "{out}");
}

#[test]
fn lifecycle_status_and_fields_survive_a_terminal_snapshot_without_them() {
    let records = [
        json!({"type":"response.created","response":{"id":"r","status":"queued","metadata":{"k":"v"},"vendor":7}}),
        json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[]}}),
    ];
    let (out, events) = response(
        wire(&records).as_bytes(),
        BodyMode::Streaming,
        BodyMode::Buffered,
        Operation::Generate,
    );
    assert_eq!(events[0]["status"], "queued", "{}", events[0]);
    let out: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(out["metadata"], json!({"k":"v"}), "{out}");
    assert_eq!(out["vendor"], 7);
    assert_eq!(out["status"], "completed");
}

#[test]
fn late_audio_metadata_reaches_the_client() {
    let audio = json!({"data":"AAEC","mime_type":"audio/wav","expires_at":12,"vendor":7});
    let records = [
        json!({"type":"response.audio.delta","delta":"AAEC"}),
        json!({"type":"response.audio.done"}),
        json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[],"audio":audio}}),
    ];
    let (out, _) = response(
        wire(&records).as_bytes(),
        BodyMode::Streaming,
        BodyMode::Buffered,
        Operation::Generate,
    );
    let out = String::from_utf8(out).unwrap();
    for field in ["\"mime_type\":\"audio/wav\"", "\"expires_at\":12", "\"vendor\":7"] {
        assert!(out.contains(field), "{field}: {out}");
    }
    // Audio that the snapshot starts carries its metadata in `media.start`; no second record.
    let body = json!({"id":"r","status":"completed","output":[],"audio":audio});
    let (_, events) = response(
        &serde_json::to_vec(&body).unwrap(),
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Generate,
    );
    assert!(!events.iter().any(|e| e["name"] == "responses.audio"));
}

#[test]
fn unknown_content_parts_stay_opaque() {
    let part = json!({"type":"future_part","text":"t","payload":{"a":null}});
    let input = json!({"id":"r","status":"completed","output":[
        {"id":"i","type":"message","role":"assistant","content":[part]}]});
    for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
        let (out, events) = response(
            &serde_json::to_vec(&input).unwrap(),
            BodyMode::Buffered,
            downstream,
            Operation::Generate,
        );
        let start = events
            .iter()
            .find(|e| e.get("kind").is_some() && e["x_ck"]["block_index"] == 0)
            .unwrap();
        assert_eq!(start["kind"], "opaque", "{start}");
        assert!(!events.iter().any(|e| e["choices"][0]["delta"].get("content").is_some()));
        let body = match downstream {
            BodyMode::Buffered => serde_json::from_slice(&out).unwrap(),
            BodyMode::Streaming => super::cross::client_events(&out).last().unwrap()["response"].clone(),
        };
        assert_eq!(body["output"][0]["content"][0], part, "{body}");
    }
}

#[test]
fn unknown_events_are_preserved_not_failed() {
    let completed = json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[]}});
    let records = wire(&[json!({"type":"vendor.event","error":null}), completed.clone()]);
    let (_, events) = response(
        records.as_bytes(),
        BodyMode::Streaming,
        BodyMode::Buffered,
        Operation::Generate,
    );
    assert!(events.iter().any(|e| e["value"]["type"] == "vendor.event"));
    let input = format!("event: response.future\ndata: not-json\n\ndata: {completed}\n\n");
    let (_, events) = response(
        input.as_bytes(),
        BodyMode::Streaming,
        BodyMode::Buffered,
        Operation::Generate,
    );
    assert!(events.iter().any(|e| e["format"] == "text" && e["value"] == "not-json"));
    // A known event with broken data and an untyped error object still fail the stream.
    for input in [
        format!("event: response.output_text.delta\ndata: not-json\n\ndata: {completed}\n\n"),
        format!("data: {}\n\ndata: {completed}\n\n", json!({"error":{"message":"m"}})),
    ] {
        let mut m = Mapped::new();
        let mut ingress = responses::response::Ingress::new(
            &mut m.writer,
            identity(Direction::Response),
            Operation::Generate,
            BodyMode::Streaming,
            1024 * 1024,
        )
        .unwrap();
        let result = ingress
            .advance(&mut m.writer, input.as_bytes())
            .and_then(|()| ingress.finish(&mut m.writer));
        assert!(result.is_err(), "{input}");
    }
}

/// Runs streamed `records` through Responses ingress alone; returns the outcome.
fn ingest(records: &[Value]) -> codec::Result<()> {
    let mut m = Mapped::new();
    let mut ingress = responses::response::Ingress::new(
        &mut m.writer,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        1024 * 1024,
    )
    .unwrap();
    ingress
        .advance(&mut m.writer, wire(records).as_bytes())
        .and_then(|()| ingress.finish(&mut m.writer))
}

#[test]
fn closed_blocks_and_items_take_no_new_payload() {
    let message = |content: Value| json!({"id":"i","type":"message","role":"assistant","content":content});
    let annotation = json!({"type":"url_citation","url":"u","start_index":0,"end_index":2,"title":"t"});
    let text = json!({"type":"output_text","text":"hi"});
    let late_annotation = [
        json!({"type":"response.output_item.added","output_index":0,"item":message(json!([]))}),
        json!({"type":"response.content_part.done","output_index":0,"item_id":"i","content_index":0,"part":text}),
        json!({"type":"response.output_text.annotation.added","output_index":0,"item_id":"i","content_index":0,"annotation_index":0,"annotation":annotation}),
        json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[message(json!([{"type":"output_text","text":"hi","annotations":[annotation]}]))]}}),
    ];
    assert!(ingest(&late_annotation).is_err());
    let reasoning = json!({"id":"i","type":"reasoning","summary":[]});
    let late_encryption = [
        json!({"type":"response.output_item.done","output_index":0,"item":reasoning}),
        json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[{"id":"i","type":"reasoning","summary":[],"encrypted_content":"sig"}]}}),
    ];
    assert!(ingest(&late_encryption).is_err());
}

#[test]
fn canonical_usage_counts_win_over_the_native_value() {
    use codec::event::{Emitter, Kind, Scope};
    let mut m = Mapped::new();
    let mut reader = m.store.reader(m.writer.id(), 0).unwrap();
    let mut emitter = Emitter::new(identity(Direction::Response));
    let records: [(Kind, &[u8]); 3] = [
        (Kind::ResponseStart, br#","id":"r","model":"m","created":1"#),
        (
            Kind::Usage,
            br#","usage":{"input_tokens":9,"output_tokens":2,"total_tokens":11,"reasoning_tokens":1,"native":{"protocol":"responses","value":{"input_tokens":3,"output_tokens":2,"total_tokens":5,"input_tokens_details":{"cached_tokens":1,"vendor":null},"output_tokens_details":{"reasoning_tokens":0}}},"final":true}"#,
        ),
        (Kind::Done, b""),
    ];
    for (kind, payload) in records {
        emitter
            .emit(&mut m.writer, kind, Scope::default(), |e| e.bytes(payload), |_| Ok(()))
            .unwrap();
    }
    let mut egress = responses::egress::Egress::new(
        &mut m.writer,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Buffered,
        |_, _| {},
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
    let out: Value = serde_json::from_slice(&out).unwrap();
    // The cached count was removed from the canonical snapshot; the native null stays.
    assert_eq!(
        out["usage"],
        json!({"input_tokens":9,"output_tokens":2,"total_tokens":11,
            "input_tokens_details":{"vendor":null},"output_tokens_details":{"reasoning_tokens":1}}),
    );
    // Unchanged counts round-trip: no derived total, and a nested cache write stays nested.
    let usage = json!({"input_tokens":3,"output_tokens":2,"input_tokens_details":{"cache_write_tokens":4}});
    let body = json!({"id":"r","status":"completed","output":[],"usage":usage});
    let (out, _) = response(
        &serde_json::to_vec(&body).unwrap(),
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Generate,
    );
    assert_eq!(serde_json::from_slice::<Value>(&out).unwrap()["usage"], usage);
}

#[test]
fn generated_response_identity_is_marked_synthetic() {
    for (body, synthetic) in [
        (
            json!({"status":"completed","output":[]}),
            Some(json!(["id", "model", "created"])),
        ),
        (
            json!({"id":"r","status":"completed","output":[]}),
            Some(json!(["model", "created"])),
        ),
        (
            json!({"id":"r","model":"m","created_at":1,"status":"completed","output":[]}),
            None,
        ),
    ] {
        let (_, events) = response(
            &serde_json::to_vec(&body).unwrap(),
            BodyMode::Buffered,
            BodyMode::Buffered,
            Operation::Generate,
        );
        assert_eq!(events[0]["x_ck"].get("synthetic"), synthetic.as_ref(), "{}", events[0]);
    }
}
