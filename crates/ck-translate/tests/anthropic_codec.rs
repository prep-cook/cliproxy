#[path = "anthropic/history.rs"]
mod history;

#[path = "anthropic/incremental.rs"]
mod incremental;

#[path = "anthropic/review.rs"]
mod review;

#[path = "anthropic/golden.rs"]
mod golden;

// Test-only inclusion until the framework owner declares the production module.
#[path = "../src/anthropic/mod.rs"]
pub mod anthropic;
#[path = "../src/boundary.rs"]
pub mod boundary;
#[path = "../src/codec/mod.rs"]
pub mod codec;
#[path = "semantic/mod.rs"]
mod semantic;

use codec::{
    BodyMode, Operation, RequestEgress, RequestIngress, ResponseEgress, ResponseIngress,
    event::{Direction, Identity, LocalId},
};
use semantic::Mapped;
use serde_json::{Value, json};
use std::task::Poll;

fn identity(direction: Direction) -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction,
    }
}
fn bytes(lease: &ck_stream::ScratchLease) -> Vec<u8> {
    let mut out = Vec::new();
    while out.len() < lease.len() {
        out.extend_from_slice(lease.chunk(out.len()).unwrap());
    }
    out
}
fn events(raw: &[u8]) -> Vec<Value> {
    std::str::from_utf8(raw)
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}
fn request(input: &[u8], operation: Operation) -> (Value, Vec<Value>) {
    let whole = request_fragmented(input, operation, input.len());
    let fragmented = request_fragmented(input, operation, 7);
    assert_eq!(whole.0, fragmented.0);
    fragmented
}
fn request_fragmented(input: &[u8], operation: Operation, fragment: usize) -> (Value, Vec<Value>) {
    let mut mapped = Mapped::new();
    let mut ingress = anthropic::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        operation,
        1024 * 1024,
    )
    .unwrap();
    for chunk in input.chunks(fragment) {
        ingress.advance(&mut mapped.writer, chunk).unwrap();
    }
    let ready = ingress
        .finish(&mut mapped.writer, &anthropic::request::AnthropicCapability)
        .unwrap();
    let output = anthropic::request::Egress {
        destination: "anthropic",
        upstream_stream: operation == Operation::Generate,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    (
        serde_json::from_slice(&bytes(&output)).unwrap(),
        events(&mapped.output(&mapped.writer)),
    )
}
fn response(
    input: &[u8],
    upstream: BodyMode,
    downstream: BodyMode,
    operation: Operation,
    fragment: usize,
) -> (Vec<u8>, Vec<Value>) {
    response_with_diagnostic(input, upstream, downstream, operation, fragment, |_| {})
}
fn response_with_diagnostic(
    input: &[u8],
    upstream: BodyMode,
    downstream: BodyMode,
    operation: Operation,
    fragment: usize,
    diagnostic: fn(anthropic::egress::Diagnostic),
) -> (Vec<u8>, Vec<Value>) {
    let mut mapped = Mapped::new();
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let mut ingress = anthropic::response::Ingress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        operation,
        upstream,
        1024 * 1024,
    )
    .unwrap();
    let mut egress = anthropic::egress::Egress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        operation,
        downstream,
        diagnostic,
    )
    .unwrap();
    let mut output = Vec::new();
    for chunk in input.chunks(fragment) {
        ingress.advance(&mut mapped.writer, chunk).unwrap();
        while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
            egress
                .advance(&mut mapped.writer, lease, |part| {
                    output.extend_from_slice(&bytes(&part));
                    Ok(())
                })
                .unwrap();
        }
    }
    ingress.finish(&mut mapped.writer).unwrap();
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        egress
            .advance(&mut mapped.writer, lease, |part| {
                output.extend_from_slice(&bytes(&part));
                Ok(())
            })
            .unwrap();
    }
    egress.finish().unwrap();
    (output, events(&mapped.output(&mapped.writer)))
}
fn sse(rows: &[Value]) -> Vec<u8> {
    let mut out = String::new();
    for row in rows {
        out.push_str(&format!("event: {}\ndata: {}\n\n", row["type"].as_str().unwrap(), row));
    }
    out.into_bytes()
}
fn stream() -> Vec<Value> {
    vec![
        json!({"type":"message_start","message":{"id":"msg_a","model":"claude","role":"assistant","content":[],"usage":{"input_tokens":2,"cache_read_input_tokens":3,"cache_creation_input_tokens":7,"output_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":4,"ephemeral_1h_input_tokens":3}}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"consider"}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signed/"}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"\nopaque=="}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_start","index":1,"content_block":{"type":"redacted_thinking","data":"redacted=="}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"call-A","name":"lookup","input":{}}}),
        json!({"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"q\":"}}),
        json!({"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"\"值\"}"}}),
        json!({"type":"content_block_stop","index":2}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":9}}),
        json!({"type":"message_stop"}),
    ]
}
#[test]
fn signed_history_tools_sampling_cache_and_unknowns_roundtrip() {
    let input = json!({"model":"m","max_tokens":100,"temperature":0.1,"top_p":0.9,"top_k":42,"system":[{"type":"text","text":"system","cache_control":{"type":"ephemeral","ttl":"1h"}}],"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"reason","signature":"a\n/=="},{"type":"tool_use","id":"c","name":"f","input":{"nested":[null,0]}},{"type":"text","text":"after"},{"type":"redacted_thinking","data":"secret=="}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"c","content":"ok","is_error":false}]}],"tools":[{"name":"f","input_schema":{"type":"object","extra":[null,0]}}],"vendor":{"nested":[null,0]},"x_ck":{"attempt":999}});
    let (out, canonical) = request(&serde_json::to_vec(&input).unwrap(), Operation::Generate);
    let mut expected = input;
    expected["stream"] = json!(true);
    assert_eq!(out, expected);
    assert!(canonical.iter().all(|v| v["x_ck"]["attempt"] == 1));
    assert!(
        canonical
            .iter()
            .any(|v| v["pointer"] == "/messages/1/tool_calls/0/function/arguments/nested/0" && v["value"].is_null())
    );
}
#[test]
fn response_signatures_tools_and_ttl_survive_every_delivery_mode() {
    let buffered = json!({"id":"msg_a","type":"message","role":"assistant","model":"claude","content":[{"type":"thinking","thinking":"consider","signature":"signed/\nopaque=="},{"type":"redacted_thinking","data":"redacted=="},{"type":"tool_use","id":"call-A","name":"lookup","input":{"q":"值"}}],"stop_reason":"tool_use","stop_sequence":null,"usage":{"input_tokens":2,"cache_read_input_tokens":3,"cache_creation_input_tokens":7,"output_tokens":9,"cache_creation":{"ephemeral_5m_input_tokens":4,"ephemeral_1h_input_tokens":3}}});
    for upstream in [BodyMode::Buffered, BodyMode::Streaming] {
        let input = if upstream == BodyMode::Buffered {
            serde_json::to_vec(&buffered).unwrap()
        } else {
            sse(&stream())
        };
        for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
            let (output, canonical) = response(&input, upstream, downstream, Operation::Generate, 1);
            let usage = canonical.iter().filter_map(|v| v.get("usage")).next_back().unwrap();
            assert_eq!(usage["input_tokens"], 12);
            assert_eq!(usage["total_tokens"], 21);
            assert_eq!(usage["cache_write_by_ttl"], json!({"300":4,"3600":3}));
            if downstream == BodyMode::Buffered {
                assert_eq!(serde_json::from_slice::<Value>(&output).unwrap(), buffered);
            } else {
                let wire = events(&output);
                let signature: String = wire.iter().filter_map(|v| v["delta"]["signature"].as_str()).collect();
                assert_eq!(signature, "signed/\nopaque==");
                assert!(wire.iter().any(|v| v["content_block"]["data"] == "redacted=="));
                let args: String = wire
                    .iter()
                    .filter_map(|v| v["delta"]["partial_json"].as_str())
                    .collect();
                assert_eq!(serde_json::from_str::<Value>(&args).unwrap(), json!({"q":"值"}));
                assert_eq!(wire.last().unwrap()["type"], "message_stop");
            }
        }
    }
}
#[test]
fn tool_arguments_publish_before_block_stop() {
    let mut mapped = Mapped::new();
    let mut ingress = anthropic::response::Ingress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        1024 * 1024,
    )
    .unwrap();
    ingress.advance(&mut mapped.writer, &sse(&stream()[..10])).unwrap();
    let canonical = events(&mapped.output(&mapped.writer));
    assert!(
        canonical
            .iter()
            .any(|v| v["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"] == "{\"q\":")
    );
    assert!(
        !std::str::from_utf8(&mapped.output(&mapped.writer))
            .unwrap()
            .contains("x_ck.tool.end")
    );
}
#[test]
fn count_tokens_is_not_generation_usage_and_compact_is_unsupported() {
    let (out, canonical) = request(
        br#"{"model":"m","messages":[{"role":"user","content":"hello"}]}"#,
        Operation::CountTokens,
    );
    assert!(out.get("stream").is_none());
    assert!(canonical.iter().any(|v| v["operation"] == "count_tokens"));
    let (out, canonical) = response(
        br#"{"input_tokens":0,"detail":{"unknown":null}}"#,
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::CountTokens,
        1,
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&out).unwrap(),
        json!({"input_tokens":0,"detail":{"unknown":null}})
    );
    assert!(canonical.iter().all(|v| v.get("usage").is_none()));
    let mut mapped = Mapped::new();
    assert!(matches!(
        anthropic::request::Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Request),
            Operation::Compact,
            1024
        ),
        Err(codec::Error::Unsupported)
    ));
    assert!(matches!(
        anthropic::response::Ingress::new(
            &mut mapped.writer,
            identity(Direction::Response),
            Operation::CountTokens,
            BodyMode::Streaming,
            1024
        ),
        Err(codec::Error::Unsupported)
    ));
}
#[test]
fn partial_usage_and_null_never_become_zero_or_additive_deltas() {
    let mut rows = stream();
    rows[0]["message"]["usage"] = json!({"input_tokens":4,"output_tokens":0,"cache_read_input_tokens":null});
    rows.insert(13, json!({"type":"message_delta","delta":{},"usage":{"output_tokens":7,"cache_read_input_tokens":0,"cache_creation_input_tokens":2}}));
    let (_, canonical) = response(
        &sse(&rows),
        BodyMode::Streaming,
        BodyMode::Buffered,
        Operation::Generate,
        11,
    );
    let snapshots: Vec<_> = canonical.iter().filter_map(|v| v.get("usage")).collect();
    assert!(snapshots[0].get("input_tokens").is_none());
    assert!(snapshots[0].get("cache_read_tokens").is_none());
    let last = snapshots.last().unwrap();
    assert_eq!(last["input_tokens"], 6);
    assert_eq!(last["output_tokens"], 7);
    assert_eq!(last["total_tokens"], 13);
}
#[test]
fn malformed_truncated_and_post_terminal_responses_never_gain_success() {
    for input in [
        sse(&stream()[..13]),
        b"event: message_start\ndata: {\"type\":\"message_start\"}".to_vec(),
        b"data: {\"type\":\"message_start\",\"type\":\"message_stop\"}\n\n".to_vec(),
        b"data: \xff\n\n".to_vec(),
    ] {
        let mut mapped = Mapped::new();
        let mut ingress = anthropic::response::Ingress::new(
            &mut mapped.writer,
            identity(Direction::Response),
            Operation::Generate,
            BodyMode::Streaming,
            1024 * 1024,
        )
        .unwrap();
        let result = ingress.advance(&mut mapped.writer, &input);
        assert!(result.is_err() || ingress.finish(&mut mapped.writer).is_err());
        let raw = mapped.output(&mapped.writer);
        let raw = std::str::from_utf8(&raw).unwrap();
        assert!(raw.contains("event: x_ck.error\n"));
        assert!(!raw.contains("event: x_ck.done\n"));
    }
}
#[test]
fn invalid_late_request_and_foreign_replay_cannot_cross_fence() {
    for input in [
        br#"{"model":"m","max_tokens":10,"messages":[{"role":"user","content":"ok"}],"temperature":"bad"}"#.as_slice(),
        br#"{"model":"m","max_tokens":10,"messages":[{"role":"user","content":"ok"}],"model":"dup"}"#,
        b"{\"model\":\"\xff\"}",
    ] {
        let mut mapped = Mapped::new();
        let mut ingress = anthropic::request::Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Request),
            Operation::Generate,
            1024 * 1024,
        )
        .unwrap();
        let result = ingress.advance(&mut mapped.writer, input);
        assert!(
            result.is_err()
                || ingress
                    .finish(&mut mapped.writer, &anthropic::request::AnthropicCapability)
                    .is_err()
        );
        let raw = mapped.output(&mapped.writer);
        let raw = std::str::from_utf8(&raw).unwrap();
        assert!(!raw.contains("event: x_ck.request.end\n"));
        assert!(raw.contains("event: x_ck.error\n"));
    }
}
#[test]
fn native_image_bytes_mime_and_unknown_source_fields_survive() {
    let input = json!({"model":"m","max_tokens":10,"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAECAw==","ref":"literal"},"unknown":[0,null]}]}]});
    let (out, canonical) = request(&serde_json::to_vec(&input).unwrap(), Operation::Generate);
    let mut expected = input;
    expected["stream"] = json!(true);
    assert_eq!(out, expected);
    use base64::Engine;
    let decoded: Vec<_> = canonical
        .iter()
        .filter_map(|event| event["bytes"].as_str())
        .flat_map(|value| base64::engine::general_purpose::STANDARD.decode(value).unwrap())
        .collect();
    assert_eq!(decoded, [0, 1, 2, 3]);
    assert!(!canonical.iter().any(|v| v["value"] == "AAECAw=="));
}

#[test]
fn buffered_native_body_preserves_unknowns_without_duplicate_finish_fields() {
    let body = json!({"id":"m1","model":"claude","type":"message","role":"assistant","content":[{"type":"text","text":"hello","citations":[],"vendor":{"x":null}}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":2},"unknown":{"nested":[null,false]}});
    let input = serde_json::to_vec(&body).unwrap();
    let (output, canonical) = response(
        &input,
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Generate,
        input.len(),
    );
    assert_eq!(serde_json::from_slice::<Value>(&output).unwrap(), body);
    let finish = canonical
        .iter()
        .find(|v| v["choices"][0]["finish_reason"] == "stop")
        .unwrap();
    assert!(
        finish["x_ck"]["original"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v["pointer"] != "/content")
    );
}

#[test]
fn whole_request_preserves_signed_tool_history() {
    let input = json!({"model":"m","max_tokens":20,"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"reason","signature":"exact/=="},{"type":"tool_use","id":"c","name":"f","input":{"x":1}},{"type":"text","text":"after"}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"c","content":"ok"}]}]});
    let raw = serde_json::to_vec(&input).unwrap();
    let (output, _) = request_fragmented(&raw, Operation::Generate, raw.len());
    let mut expected = input;
    expected["stream"] = json!(true);
    assert_eq!(output, expected);
}

#[test]
fn native_controls_and_colliding_unknowns_remain_distinct() {
    let input = json!({"model":"m","max_tokens":100,"stop_sequences":["done"],"stop":"literal","response_format":{"foreign":true},"parallel_tool_calls":"literal","output_config":{"effort":"high","format":{"type":"json_schema","schema":{"type":"object"}},"future":null},"cache_control":{"type":"ephemeral"},"tools":[{"type":"custom","name":"f","input_schema":{"type":"object"},"cache_control":{"type":"ephemeral","ttl":"1h"}}],"messages":[{"role":"assistant","tool_calls":"literal","content":[{"type":"thinking","thinking":"real","text":"shadow","signature":"signed=="}]},{"role":"user","content":[{"type":"image","mime":"literal","source":{"type":"base64","media_type":"image/png","data":"AA==","kind":"literal"}}]}]});
    let raw = serde_json::to_vec(&input).unwrap();
    let (out, canonical) = request_fragmented(&raw, Operation::Generate, raw.len());
    let mut expected = input;
    expected["stream"] = json!(true);
    assert_eq!(out, expected);
    assert!(
        canonical
            .iter()
            .any(|v| v["pointer"] == "/response_format/json_schema/schema/type" && v["value"] == "object")
    );
    assert!(
        canonical
            .iter()
            .any(|v| v["pointer"] == "/x_ck/thinking/effort" && v["value"] == "high")
    );
    assert!(
        !canonical
            .iter()
            .any(|v| v["pointer"] == "/messages/0/tool_calls" && v["value"] == "literal")
    );
}

#[test]
fn server_results_share_tool_identity_and_preserve_native_payloads() {
    let body = json!({"id":"msg","model":"m","type":"message","role":"assistant","content":[{"type":"server_tool_use","id":"srv-1","name":"web_search","input":{"query":"x"}},{"type":"web_search_tool_result","tool_use_id":"srv-1","content":[{"type":"web_search_result","url":"https://example.com","title":"x","encrypted_content":"exact=="}]}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":2}});
    let raw = serde_json::to_vec(&body).unwrap();
    let (out, canonical) = response(
        &raw,
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Generate,
        raw.len(),
    );
    assert_eq!(serde_json::from_slice::<Value>(&out).unwrap(), body);
    let call = canonical.iter().find(|v| v["name"] == "web_search").unwrap();
    let result = canonical
        .iter()
        .find(|v| v["value"][0]["encrypted_content"] == "exact==")
        .unwrap();
    assert_eq!(result["x_ck"]["tool_call_id"], call["x_ck"]["tool_call_id"]);
    assert_eq!(result["x_ck"]["source"]["ids"]["tool_use_id"], "srv-1");
}

#[test]
fn chat_choices_select_index_zero_not_the_first_array_element() {
    for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
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
        let mut egress = anthropic::egress::Egress::new(
            &mut mapped.writer,
            identity(Direction::Response),
            Operation::Generate,
            downstream,
            |_| {},
        )
        .unwrap();
        ingress.advance(&mut mapped.writer, br#"{"id":"response","model":"m","choices":[{"index":1,"message":{"role":"assistant","content":"discard"},"finish_reason":"length"},{"index":0,"message":{"role":"assistant","content":"selected"},"finish_reason":"stop"}]}"#).unwrap();
        ingress.finish(&mut mapped.writer).unwrap();
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
        if downstream == BodyMode::Buffered {
            let body: Value = serde_json::from_slice(&output).unwrap();
            assert_eq!(body["content"], json!([{"type":"text","text":"selected"}]));
            assert_eq!(body["stop_reason"], "end_turn");
        } else {
            let rows = events(&output);
            assert!(rows.iter().any(|v| v["delta"]["text"] == "selected"));
            assert!(rows.iter().any(|v| v["delta"]["stop_reason"] == "end_turn"));
            assert!(rows.iter().all(|v| v["delta"]["text"] != "discard"));
        }
    }
}
