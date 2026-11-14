#[path = "responses/audio.rs"]
mod audio;
#[path = "responses/execution.rs"]
mod execution;
#[path = "responses/golden.rs"]
mod golden;
#[path = "responses/incremental.rs"]
mod incremental;
#[path = "responses/recordings.rs"]
mod recordings;
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
    let input = json!({"id":"c","object":"response.compaction","created_at":4,"model":"m","output":[{"id":"i","type":"compaction","encrypted_content":"secret"}],"usage":{"input_tokens":0,"output_tokens":null,"cache_write_tokens":0,"input_tokens_details":{"cache_write_tokens":9},"cache_write_by_ttl":{"300":7,"3600":9}}});
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
    assert_eq!(usage["usage"]["cache_write_tokens"], 0);
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
            assert!(out["output"][0].get("content").is_none());
            assert_eq!(out["output"][1]["content"][0]["text"], "hello");
            assert_eq!(out["output"][2]["arguments"], "{\"x\":1}");
        } else {
            assert!(
                std::str::from_utf8(&out)
                    .unwrap()
                    .lines()
                    .filter_map(|line| line.strip_prefix("data: "))
                    .map(|line| serde_json::from_str::<Value>(line).unwrap())
                    .all(|event| event["type"] != "response.content_part.added" || event["item_id"] != "reason")
            );
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
        "data: \n\n".to_owned(),
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
            let summary = text
                .lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .find(|event| event["type"] == "response.reasoning_summary_text.delta")
                .unwrap();
            assert_eq!(summary["summary_index"], 0);
        }
    }
}

#[test]
fn typed_tool_results_preserve_text_and_file_data() {
    let input = json!({"model":"m","input":[{"type":"function_call","call_id":"c","name":"f","arguments":"{}"},{"type":"function_call_output","call_id":"c","output":[{"type":"input_text","text":"ok"},{"type":"input_file","filename":"a.pdf","file_data":"data:application/pdf;base64,JVBERi0xLjQK"}]}]});
    let (output, _) = request(&input, Operation::Generate);
    assert_eq!(output["input"][1]["output"], input["input"][1]["output"]);
}

#[test]
fn generated_images_use_media_data_and_preserve_partial_boundaries() {
    use base64::Engine;
    let raw: Vec<_> = (0..80_003).map(|i| (i % 251) as u8).collect();
    let encoded = base64::engine::general_purpose::STANDARD.encode(&raw);
    let item =
        json!({"id":"img","type":"image_generation_call","status":"completed","result":encoded,"output_format":"png"});
    let body = json!({"id":"r","model":"m","created_at":1,"status":"completed","output":[item],"x_ck_opaque":[]});
    let wire=[
        json!({"type":"response.created","response":{"id":"r","model":"m","created_at":1}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"img","type":"image_generation_call"}}),
        json!({"type":"response.image_generation_call.partial_image","output_index":0,"item_id":"img","partial_image_index":3,"partial_image_b64":"AQID"}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        json!({"type":"response.completed","response":body}),
    ].iter().map(|event|format!("data: {event}\n\n")).collect::<String>();
    for mode in [BodyMode::Buffered, BodyMode::Streaming] {
        let input = if mode == BodyMode::Buffered {
            serde_json::to_vec(&body).unwrap()
        } else {
            wire.as_bytes().to_vec()
        };
        for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
            let (output, canonical) = response(&input, mode, downstream, Operation::Generate);
            let media: Vec<_> = canonical.iter().filter(|event| event["kind"] == "image").collect();
            assert_eq!(media.len(), if mode == BodyMode::Streaming { 2 } else { 1 });
            let final_id = &media.last().unwrap()["x_ck"]["media_id"];
            let reconstructed: Vec<_> = canonical
                .iter()
                .filter(|event| event["x_ck"]["media_id"] == *final_id && event["bytes"].is_string())
                .flat_map(|event| {
                    base64::engine::general_purpose::STANDARD
                        .decode(event["bytes"].as_str().unwrap())
                        .unwrap()
                })
                .collect();
            assert_eq!(reconstructed, raw);
            assert!(
                canonical
                    .iter()
                    .all(|event| event["attributes"].get("result").is_none())
            );
            let projected: Value = if downstream == BodyMode::Buffered {
                serde_json::from_slice(&output).unwrap()
            } else {
                let events: Vec<Value> = std::str::from_utf8(&output)
                    .unwrap()
                    .lines()
                    .filter_map(|line| line.strip_prefix("data: "))
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect();
                if mode == BodyMode::Streaming {
                    let partial = events
                        .iter()
                        .find(|event| event["type"] == "response.image_generation_call.partial_image")
                        .unwrap();
                    assert_eq!(partial["partial_image_index"], 3);
                    assert_eq!(partial["partial_image_b64"], "AQID");
                }
                events.last().unwrap()["response"].clone()
            };
            assert_eq!(projected["output"][0]["result"], encoded);
            assert_eq!(projected["output"][0]["output_format"], "png");
            if mode == BodyMode::Streaming && downstream == BodyMode::Buffered {
                let carrier = projected["x_ck_opaque"].as_array().unwrap();
                let preview = carrier
                    .iter()
                    .find(|entry| entry["type"] == "response.image_generation_call.partial_image")
                    .unwrap();
                assert_eq!(preview["partial_image_index"], 3);
                assert_eq!(preview["partial_image_b64"], "AQID");
                let native = carrier
                    .iter()
                    .find(|entry| entry["name"] == "responses.fields" && entry["value"].get("x_ck_opaque").is_some())
                    .unwrap();
                assert_eq!(native["value"]["x_ck_opaque"], json!([]));
            }
        }
    }
}

#[test]
fn compact_requests_replay_compaction_items_without_generation_controls() {
    let input = json!({"model":"m","instructions":"retain facts","input":[{"id":"c","type":"compaction","encrypted_content":"exact+/=\n","vendor":null}]});
    let (output, _) = request(&input, Operation::Compact);
    assert_eq!(output["input"], input["input"]);
    assert_eq!(output["instructions"], "retain facts");
    assert!(output.get("stream").is_none());
}

#[test]
fn reasoning_request_content_and_empty_reasoning_keep_native_identity() {
    let input = json!({"model":"m","input":[{"id":"r","type":"reasoning","content":[{"type":"reasoning_text","text":"full","vendor":{"x":null}}],"summary":[{"type":"summary_text","text":"brief","vendor":0}]},{"id":"empty","type":"reasoning","summary":[]}]});
    let (output, _) = request(&input, Operation::Generate);
    assert_eq!(output["input"], input["input"]);
}

#[test]
fn chat_requests_use_responses_fence_and_streamed_text_projection() {
    for unsupported in [None, Some(("n", json!(2))), Some(("stop", json!(["end"])))] {
        let text = "word ".repeat(900);
        let mut input =
            json!({"model":"m","messages":[{"role":"user","content":text}],"reasoning_effort":"high","temperature":0});
        if let Some((field, value)) = &unsupported {
            input[*field] = value.clone();
        }
        let mut m = Mapped::new();
        let mut ingress = codec::chat::request::Ingress::new(
            &mut m.writer,
            &m.store,
            identity(Direction::Request),
            Operation::Generate,
            100_000,
        )
        .unwrap();
        for chunk in serde_json::to_vec(&input).unwrap().chunks(64) {
            ingress.advance(&mut m.writer, chunk).unwrap();
        }
        let ready = ingress.finish(&mut m.writer, &responses::request::ResponsesCapability);
        if unsupported.is_some() {
            assert!(matches!(ready, Err(codec::Error::Unsupported)));
            assert!(
                !String::from_utf8(m.output(&m.writer))
                    .unwrap()
                    .contains("event: x_ck.request.end\n")
            );
        } else {
            let encoded = responses::request::Egress {
                destination: "responses",
                upstream_stream: false,
            }
            .encode(&ready.unwrap(), &mut m.writer)
            .unwrap();
            let output: Value = serde_json::from_slice(&bytes(&encoded)).unwrap();
            assert_eq!(output["input"][0]["content"], text);
            assert_eq!(output["reasoning"]["effort"], "high");
            assert_eq!(output["temperature"], 0);
        }
    }
}

#[test]
fn native_extension_fields_use_noncontrol_carriers_not_incomplete_native_events() {
    let input=b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"}}\n\nid: source-id\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":41,\"output_index\":0,\"item\":{\"id\":\"i\",\"type\":\"message\"},\"vendor\":{\"x_ck\":{\"seq\":900}}}\n\ndata: {\"type\":\"response.completed\",\"sequence_number\":42,\"vendor\":null,\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[{\"id\":\"i\",\"type\":\"message\",\"content\":[]}]}}\n\n";
    let (output, _) = response(input, BodyMode::Streaming, BodyMode::Streaming, Operation::Generate);
    let events: Vec<Value> = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(
        events
            .iter()
            .filter(|event| event["type"] == "response.output_item.added")
            .all(|event| event["item"].is_object())
    );
    let carrier = events
        .iter()
        .find(|event| event["type"] == "x_ck_opaque" && event["source"]["sequence"] == 41)
        .unwrap();
    assert_eq!(carrier["value"]["vendor"]["x_ck"]["seq"], 900);
    assert_eq!(carrier["source"]["event"], "response.output_item.added");
    let terminal = events
        .iter()
        .find(|event| event["type"] == "x_ck_opaque" && event["source"]["sequence"] == 42)
        .unwrap();
    assert!(terminal["value"].get("vendor").unwrap().is_null());
    assert_eq!(events.last().unwrap()["type"], "response.completed");
}

#[test]
fn image_previews_require_an_open_image_item() {
    for closed_image in [false, true] {
        let item = if closed_image {
            json!({"id":"i","type":"image_generation_call","result":"AQID"})
        } else {
            json!({"id":"i","type":"message","content":[]})
        };
        let mut records = vec![json!({"type":"response.output_item.added","output_index":0,"item":item})];
        if closed_image {
            records.push(json!({"type":"response.output_item.done","output_index":0,"item":item}));
        }
        records.push(json!({"type":"response.image_generation_call.partial_image","output_index":0,"item_id":"i","partial_image_index":0,"partial_image_b64":"AQID"}));
        records.push(json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[item]}}));
        let wire = records
            .iter()
            .map(|event| format!("data: {event}\n\n"))
            .collect::<String>();
        let mut m = Mapped::new();
        let mut ingress = responses::response::Ingress::new(
            &mut m.writer,
            identity(Direction::Response),
            Operation::Generate,
            BodyMode::Streaming,
            100_000,
        )
        .unwrap();
        let result = ingress
            .advance(&mut m.writer, wire.as_bytes())
            .and_then(|()| ingress.finish(&mut m.writer));
        assert!(result.is_err());
        let canonical = String::from_utf8(m.output(&m.writer)).unwrap();
        assert!(canonical.contains("event: x_ck.error\n"));
        assert!(!canonical.contains("event: x_ck.done\n"));
    }
}

#[test]
fn interleaved_items_and_content_keep_native_indexes_and_final_order() {
    let first = json!({"id":"first","type":"message","content":[{"type":"output_text","text":"a"},{"type":"output_text","text":"b"}]});
    let second = json!({"id":"second","type":"message","content":[]});
    let events = [
        json!({"type":"response.output_item.added","output_index":1,"item":{"id":"second","type":"message"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"first","type":"message"}}),
        json!({"type":"response.output_text.delta","output_index":0,"item_id":"first","content_index":1,"delta":"b"}),
        json!({"type":"response.output_text.delta","output_index":0,"item_id":"first","content_index":0,"delta":"a"}),
        json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[first,second]}}),
    ];
    let wire = events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect::<String>();
    for mode in [BodyMode::Buffered, BodyMode::Streaming] {
        let (output, _) = response(wire.as_bytes(), BodyMode::Streaming, mode, Operation::Generate);
        let body: Value = if mode == BodyMode::Buffered {
            serde_json::from_slice(&output).unwrap()
        } else {
            let events: Vec<Value> = std::str::from_utf8(&output)
                .unwrap()
                .lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            let starts: Vec<_> = events
                .iter()
                .filter(|event| event["type"] == "response.output_item.added")
                .map(|event| event["output_index"].as_u64().unwrap())
                .collect();
            assert_eq!(starts, [1, 0]);
            let b = events
                .iter()
                .find(|event| event["type"] == "response.output_text.delta" && event["delta"] == "b")
                .unwrap();
            assert_eq!(b["output_index"], 0);
            assert_eq!(b["content_index"], 1);
            events.last().unwrap()["response"].clone()
        };
        assert_eq!(body["output"][0]["id"], "first");
        assert_eq!(body["output"][1]["id"], "second");
        assert_eq!(body["output"][0]["content"], first["content"]);
    }
}

#[test]
fn legacy_redacted_thinking_is_not_reclassified_as_an_encryption_signature() {
    let carrier = "claude-redacted-thinking:opaque\n+/=";
    let body = json!({"id":"r","status":"completed","output":[{"id":"reason","type":"reasoning","encrypted_content":carrier,"summary":[]}]});
    for mode in [BodyMode::Buffered, BodyMode::Streaming] {
        let (output, canonical) = response(
            &serde_json::to_vec(&body).unwrap(),
            BodyMode::Buffered,
            mode,
            Operation::Generate,
        );
        assert!(canonical.iter().any(|event| event["data"] == "opaque\n+/="));
        assert!(!canonical.iter().any(|event| event["kind"] == "responses_encrypted"));
        let projected: Value = if mode == BodyMode::Buffered {
            serde_json::from_slice(&output).unwrap()
        } else {
            std::str::from_utf8(&output)
                .unwrap()
                .lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .next_back()
                .unwrap()["response"]
                .clone()
        };
        assert_eq!(projected["output"][0]["encrypted_content"], carrier);
        assert!(projected["output"][0].get("content").is_none());
    }
}
