#[path = "responses/golden.rs"]
mod golden;
// Test-only inclusion until the framework owner declares the production module.
#[path = "../src/codec/mod.rs"]
pub mod codec;
#[path = "../src/responses/mod.rs"]
pub mod responses;
#[path = "semantic/mod.rs"]
mod semantic;

use codec::{
    BodyMode, Operation, RequestEgress, RequestIngress, ResponseEgress, ResponseIngress,
    event::{Direction, Identity, LocalId},
};
use semantic::Mapped;
use serde_json::{Value, json};
fn identity(direction: Direction) -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction,
    }
}
fn bytes(lease: &ck_stream::ScratchLease) -> Vec<u8> {
    let mut result = Vec::new();
    while result.len() < lease.len() {
        result.extend_from_slice(lease.chunk(result.len()).unwrap());
    }
    result
}
fn events(mapped: &Mapped) -> Vec<Value> {
    let bytes = mapped.output(&mapped.writer);
    std::str::from_utf8(&bytes)
        .unwrap()
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}
fn request(input: &Value, operation: Operation) -> (Value, Vec<Value>) {
    let mut m = Mapped::new();
    let mut ingress = responses::request::Ingress::new(
        &mut m.writer,
        &m.store,
        identity(Direction::Request),
        operation,
        1024 * 1024,
    )
    .unwrap();
    for chunk in serde_json::to_vec(input).unwrap().chunks(7) {
        ingress.advance(&mut m.writer, chunk).unwrap();
    }
    let ready = ingress
        .finish(&mut m.writer, &responses::request::ResponsesCapability)
        .unwrap();
    let output = responses::request::Egress {
        destination: "responses",
        upstream_stream: false,
    }
    .encode(&ready, &mut m.writer)
    .unwrap();
    (serde_json::from_slice(&bytes(&output)).unwrap(), events(&m))
}
fn response(input: &[u8], mode: BodyMode, downstream: BodyMode, operation: Operation) -> (Vec<u8>, Vec<Value>) {
    let mut m = Mapped::new();
    let mut ingress = responses::response::Ingress::new(
        &mut m.writer,
        identity(Direction::Response),
        operation,
        mode,
        1024 * 1024,
    )
    .unwrap();
    let mut reader = m.store.reader(m.writer.id(), 0).unwrap();
    let mut egress = responses::egress::Egress::new(
        &mut m.writer,
        identity(Direction::Response),
        operation,
        downstream,
        |_, _| {},
    )
    .unwrap();
    let mut result = Vec::new();
    for chunk in input.chunks(13) {
        ingress.advance(&mut m.writer, chunk).unwrap();
    }
    ingress.finish(&mut m.writer).unwrap();
    while let std::task::Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        egress
            .advance(&mut m.writer, lease, |out| {
                result.extend_from_slice(&bytes(&out));
                Ok(())
            })
            .unwrap();
    }
    egress.finish().unwrap();
    (result, events(&m))
}
#[test]
fn request_history_preserves_identity_encryption_and_structured_arguments() {
    let input = json!({"model":"m","temperature":0,"top_p":0.8,"input":[{"type":"reasoning","id":"reason","summary":[{"type":"summary_text","text":"think"}],"encrypted_content":"opaque\n+/=","vendor":null},{"type":"function_call","id":"item","call_id":"call","name":"f","arguments":"{\"a\":[0,null]}"},{"type":"function_call_output","call_id":"call","output":"ok"}],"previous_response_id":"previous","x_ck":{"attempt":999}});
    let (out, events) = request(&input, Operation::Generate);
    assert_eq!(
        out["input"][0]["encrypted_content"],
        input["input"][0]["encrypted_content"]
    );
    assert_eq!(out["input"][0]["id"], "reason");
    assert_eq!(out["input"][0]["vendor"], Value::Null);
    assert_eq!(out["input"][1]["id"], "item");
    assert_eq!(out["input"][1]["call_id"], "call");
    assert_eq!(
        serde_json::from_str::<Value>(out["input"][1]["arguments"].as_str().unwrap()).unwrap(),
        json!({"a":[0,null]})
    );
    assert_eq!(out["temperature"], 0);
    assert_eq!(out["top_p"], 0.8);
    assert_eq!(out["previous_response_id"], "previous");
    assert_eq!(out["x_ck"]["attempt"], 999);
    assert!(events.iter().all(|e| e["x_ck"]["attempt"] == 1));
    assert!(
        events
            .iter()
            .any(|e| e["pointer"] == "/messages/1/tool_calls/0/function/arguments/a/0")
    );
}
#[test]
fn compact_preserves_opaque_items_and_usage_presence() {
    let input = json!({"id":"c","object":"response.compaction","created_at":4,"model":"m","output":[{"id":"i","type":"compaction","encrypted_content":"secret"}],"usage":{"input_tokens":0,"output_tokens":null,"cache_write_by_ttl":{"300":7,"3600":9}}});
    let (out, events) = response(
        &serde_json::to_vec(&input).unwrap(),
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Compact,
    );
    let out: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(out["output"], input["output"]);
    assert_eq!(out["usage"], input["usage"]);
    let usage = events.iter().find(|e| e.get("usage").is_some()).unwrap();
    assert_eq!(usage["usage"]["input_tokens"], 0);
    assert!(usage["usage"].get("output_tokens").is_none());
    assert_eq!(usage["usage"]["cache_write_by_ttl"]["3600"], 9);
}
#[test]
fn buffered_response_decomposes_text_tools_and_encrypted_reasoning() {
    let input = json!({"id":"r","model":"m","created_at":42,"status":"completed","output":[{"id":"reason","type":"reasoning","summary":[{"type":"summary_text","text":"hmm"}],"encrypted_content":"opaque=="},{"id":"msg","type":"message","role":"assistant","content":[{"type":"output_text","text":"hello","logprobs":[{"token":"hello","logprob":-0.2}]}]},{"id":"tool","type":"function_call","call_id":"call","name":"f","arguments":"{\"x\":1}"}],"usage":{"input_tokens":3,"output_tokens":2}});
    for mode in [BodyMode::Buffered, BodyMode::Streaming] {
        let (out, events) = response(
            &serde_json::to_vec(&input).unwrap(),
            BodyMode::Buffered,
            mode,
            Operation::Generate,
        );
        assert!(
            events
                .iter()
                .any(|e| e["kind"] == "responses_encrypted" && e["fragment"] == "opaque==")
        );
        let text: String = events
            .iter()
            .filter_map(|e| e["choices"][0]["delta"]["content"].as_str())
            .collect();
        assert_eq!(text, "hello");
        if mode == BodyMode::Buffered {
            let out: Value = serde_json::from_slice(&out).unwrap();
            assert_eq!(out["output"][0]["encrypted_content"], "opaque==");
            assert_eq!(out["output"][1]["content"][0]["text"], "hello");
            assert_eq!(out["output"][2]["arguments"], "{\"x\":1}");
        } else {
            assert!(
                std::str::from_utf8(&out)
                    .unwrap()
                    .contains("response.output_text.delta")
            );
        }
    }
}
#[test]
fn streaming_snapshots_do_not_duplicate_text_or_arguments() {
    let records = [
        json!({"type":"response.created","response":{"id":"r","model":"m","created_at":2}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"i","type":"function_call","call_id":"c","name":"f","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":0,"item_id":"i","delta":"{\"x\":"}),
        json!({"type":"response.function_call_arguments.delta","output_index":0,"item_id":"i","delta":"1}"}),
        json!({"type":"response.function_call_arguments.done","output_index":0,"item_id":"i","arguments":"{\"x\":1}"}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"id":"i","type":"function_call","call_id":"c","name":"f","arguments":"{\"x\":1}","status":"completed"}}),
        json!({"type":"response.completed","response":{"id":"r","model":"m","created_at":2,"status":"completed","output":[{"id":"i","type":"function_call","call_id":"c","name":"f","arguments":"{\"x\":1}","status":"completed"}]}}),
    ];
    let mut wire = String::new();
    for record in records {
        wire.push_str(&format!("data: {record}\n\n"));
    }
    let (out, events) = response(
        wire.as_bytes(),
        BodyMode::Streaming,
        BodyMode::Buffered,
        Operation::Generate,
    );
    let out: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(out["output"][0]["arguments"], "{\"x\":1}");
    let args: String = events
        .iter()
        .filter_map(|e| e["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"].as_str())
        .collect();
    assert_eq!(args, "{\"x\":1}");
}
#[test]
fn compact_streaming_and_count_are_explicit_errors() {
    let mut m = Mapped::new();
    assert!(matches!(
        responses::request::Ingress::new(
            &mut m.writer,
            &m.store,
            identity(Direction::Request),
            Operation::CountTokens,
            100
        ),
        Err(codec::Error::Unsupported)
    ));
    assert!(matches!(
        responses::response::Ingress::new(
            &mut m.writer,
            identity(Direction::Response),
            Operation::Compact,
            BodyMode::Streaming,
            100
        ),
        Err(codec::Error::Unsupported)
    ));
    let mut ingress = responses::request::Ingress::new(
        &mut m.writer,
        &m.store,
        identity(Direction::Request),
        Operation::Compact,
        1000,
    )
    .unwrap();
    ingress
        .advance(&mut m.writer, br#"{"model":"m","input":[],"stream":true}"#)
        .unwrap();
    assert!(
        ingress
            .finish(&mut m.writer, &responses::request::ResponsesCapability)
            .is_err()
    );
    assert!(
        !String::from_utf8(m.output(&m.writer))
            .unwrap()
            .contains("x_ck.request.end")
    );
}

#[test]
fn request_media_uses_canonical_data_without_inline_copy() {
    use base64::Engine;
    let raw: Vec<u8> = (0..80_003).map(|i| (i % 251) as u8).collect();
    let encoded = base64::engine::general_purpose::STANDARD.encode(&raw);
    let input = json!({"model":"m","input":[{"role":"user","content":[{"type":"input_image","image_url":format!("data:image/png;base64,{encoded}"),"detail":"high","ref":"literal"}]}]});
    let (out, events) = request(&input, Operation::Generate);
    assert_eq!(
        out["input"][0]["content"][0]["image_url"],
        input["input"][0]["content"][0]["image_url"]
    );
    assert_eq!(out["input"][0]["content"][0]["ref"], "literal");
    let mut decoded = Vec::new();
    for event in &events {
        if let Some(data) = event["bytes"].as_str() {
            decoded.extend(base64::engine::general_purpose::STANDARD.decode(data).unwrap());
        }
    }
    assert_eq!(decoded, raw);
    assert!(
        events
            .iter()
            .filter(|e| e.get("pointer").is_some())
            .all(|e| e["value"] != encoded)
    );
}

#[test]
fn malformed_truncated_and_contradictory_responses_never_complete() {
    let cases=[
        "data: {broken}\n\n".to_owned(),
        "event: response.output_text.delta\ndata: broken\n\n".to_owned(),
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"}}\n\n".to_owned(),
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"}}".to_owned(),
        [json!({"type":"response.output_item.added","output_index":0,"item":{"id":"i","type":"message","role":"assistant","content":[]}}),json!({"type":"response.output_text.delta","output_index":0,"item_id":"i","content_index":0,"delta":"prefix"}),json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[{"id":"i","type":"message","content":[{"type":"output_text","text":"changed"}]}]}})].iter().map(|v|format!("data: {v}\n\n")).collect(),
    ];
    for wire in cases {
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
            .advance(&mut m.writer, wire.as_bytes())
            .and_then(|()| ingress.finish(&mut m.writer));
        assert!(result.is_err(), "{wire}");
        let emitted = String::from_utf8(m.output(&m.writer)).unwrap();
        assert!(emitted.contains("event: x_ck.error\n"));
        assert!(!emitted.contains("event: x_ck.done\n"));
    }
}

#[test]
fn unknown_sse_fields_cannot_forge_canonical_records() {
    let input=b"data: first\ndata: second\nid: external\nevent: vendor.unknown\nretry: 12\n: comment\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n";
    let (_, events) = response(input, BodyMode::Streaming, BodyMode::Streaming, Operation::Generate);
    let unknown = events.iter().find(|e| e["format"] == "text").unwrap();
    assert_eq!(unknown["value"], "first\nsecond");
    assert_eq!(unknown["x_ck"]["source"]["event"], "vendor.unknown");
    assert!(
        unknown["x_ck"]["source"]["sse"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f["name"] == "id" && f["value"] == "external")
    );
}

#[test]
fn destination_rejection_has_no_request_end_and_cannot_be_retried() {
    struct Reject;
    impl codec::Capability for Reject {
        fn destination(&self) -> &'static str {
            "reject"
        }
        fn validate(&self, _: codec::RequestView<'_>) -> codec::Result<()> {
            Err(codec::Error::Unsupported)
        }
    }
    let mut m = Mapped::new();
    let mut ingress = responses::request::Ingress::new(
        &mut m.writer,
        &m.store,
        identity(Direction::Request),
        Operation::Generate,
        1024,
    )
    .unwrap();
    ingress
        .advance(
            &mut m.writer,
            br#"{"model":"m","input":"hi","previous_response_id":"late"}"#,
        )
        .unwrap();
    assert!(matches!(
        ingress.finish(&mut m.writer, &Reject),
        Err(codec::Error::Unsupported)
    ));
    assert!(matches!(
        ingress.finish(&mut m.writer, &responses::request::ResponsesCapability),
        Err(codec::Error::Closed)
    ));
    let output = String::from_utf8(m.output(&m.writer)).unwrap();
    assert!(!output.contains("x_ck.request.end"));
    assert!(output.contains("x_ck.error"));
}

#[test]
fn full_reasoning_and_summary_have_independent_block_identity() {
    let item = json!({"id":"rs","type":"reasoning","content":[{"type":"reasoning_text","text":"full"}],"summary":[{"type":"summary_text","text":"brief"}]});
    let body = json!({"id":"r","model":"m","created_at":1,"status":"completed","output":[item]});
    for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
        let (output, canonical) = response(
            &serde_json::to_vec(&body).unwrap(),
            BodyMode::Buffered,
            downstream,
            Operation::Generate,
        );
        let blocks: Vec<_> = canonical
            .iter()
            .filter(|event| event["kind"] == "thinking" || event["kind"] == "reasoning_summary")
            .collect();
        assert_eq!(blocks.len(), 2);
        assert_ne!(blocks[0]["x_ck"]["block_index"], blocks[1]["x_ck"]["block_index"]);
        if downstream == BodyMode::Buffered {
            let value: Value = serde_json::from_slice(&output).unwrap();
            assert_eq!(value["output"][0]["content"][0]["text"], "full");
            assert_eq!(value["output"][0]["summary"][0]["text"], "brief");
        } else {
            let text = std::str::from_utf8(&output).unwrap();
            assert!(text.contains("response.reasoning_text.delta"));
            assert!(text.contains("response.reasoning_summary_text.delta"));
        }
    }
}

#[test]
fn typed_tool_results_preserve_text_and_file_data() {
    let input = json!({"model":"m","input":[{"type":"function_call","call_id":"c","name":"f","arguments":"{}"},{"type":"function_call_output","call_id":"c","output":[{"type":"input_text","text":"ok"},{"type":"input_file","filename":"a.pdf","file_data":"data:application/pdf;base64,JVBERi0xLjQK"}]}]});
    let (output, _) = request(&input, Operation::Generate);
    assert_eq!(output["input"][1]["output"], input["input"][1]["output"]);
}
