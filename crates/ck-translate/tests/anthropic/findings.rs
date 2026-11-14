//! Regression tests for the Anthropic codec review findings.
use crate::{
    BodyMode, Direction, Mapped, Operation, ResponseEgress, ResponseIngress, Value, anthropic, bytes, codec, events,
    identity, json, request, request_fragmented, response, sse,
};
use std::task::Poll;

#[test]
fn media_whose_type_follows_its_data_publishes_one_lifetime() {
    let payload = "AAEC".repeat(400);
    let image =
        format!(r#"{{"source":{{"data":"{payload}","type":"base64","media_type":"image/png"}},"type":"image"}}"#);
    let nested = format!(r#"{{"tool_use_id":"c","content":[{image}],"type":"tool_result"}}"#);
    let raw = format!(
        r#"{{"model":"m","max_tokens":10,"messages":[{{"role":"assistant","content":[{{"type":"tool_use","id":"c","name":"f","input":{{}}}}]}},{{"role":"user","content":[{nested},{image},{image}]}}]}}"#
    );
    let (out, canonical) = request_fragmented(raw.as_bytes(), Operation::Generate, 37);
    let content = &out["messages"][1]["content"];
    assert_eq!(content[0]["content"][0]["source"]["data"], payload);
    assert_eq!(content[2]["source"]["data"], payload);
    let starts = canonical.iter().filter(|v| v.get("encoding").is_some()).count();
    assert_eq!(starts, 3, "one media lifetime per image");
}

#[test]
fn delta_after_choice_finish_stays_stream_scoped() {
    let rows = [
        json!({"type":"message_start","message":{"id":"r","model":"m"}}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
        json!({"type":"message_delta","delta":{"stop_reason":null,"future":1},"usage":{"output_tokens":3}}),
        json!({"type":"message_stop"}),
    ];
    let (out, canonical) = response(
        &sse(&rows),
        BodyMode::Streaming,
        BodyMode::Streaming,
        Operation::Generate,
        7,
    );
    let end = canonical
        .iter()
        .position(|v| v["choices"][0]["finish_reason"] == "stop")
        .unwrap();
    for late in &canonical[end + 1..] {
        assert!(
            late["choices"].as_array().is_none_or(Vec::is_empty),
            "post-finish: {late}"
        );
        assert!(late["x_ck"].get("choice_index").is_none(), "post-finish: {late}");
    }
    let native = events(&out);
    let late = native.iter().filter(|v| v["type"] == "message_delta").nth(1).unwrap();
    assert_eq!(late["delta"], rows[2]["delta"]);
    assert_eq!(late["usage"]["output_tokens"], 3);
}

#[test]
fn declared_server_tool_can_be_chosen_by_name() {
    let body = json!({"model":"m","max_tokens":10,"messages":[{"role":"user","content":"x"}],"tools":[{"type":"web_search_20250305","name":"web_search"}],"tool_choice":{"type":"tool","name":"web_search"}});
    let (out, _) = request(&serde_json::to_vec(&body).unwrap(), Operation::Generate);
    assert_eq!(out["tool_choice"], body["tool_choice"]);
    assert_eq!(out["tools"], body["tools"]);
}

#[test]
fn opaque_request_blocks_keep_their_cache_directive() {
    let body = json!({"model":"m","max_tokens":10,"messages":[{"role":"user","content":[{"type":"text","text":"a"},{"type":"future_block","value":"x","cache_control":{"type":"ephemeral","ttl":"1h"}},{"type":"future_block","value":"y"}]}]});
    let (out, _) = request(&serde_json::to_vec(&body).unwrap(), Operation::Generate);
    assert_eq!(out["messages"], body["messages"]);
}

#[test]
fn named_tool_choice_keeps_unknown_members() {
    let body = json!({"model":"m","max_tokens":10,"messages":[{"role":"user","content":"x"}],"tools":[{"name":"f","input_schema":{"type":"object"}}],"tool_choice":{"type":"tool","name":"f","future":{"x":null},"disable_parallel_tool_use":true}});
    let (out, _) = request(&serde_json::to_vec(&body).unwrap(), Operation::Generate);
    assert_eq!(out["tool_choice"], body["tool_choice"]);
}

#[test]
fn message_unknowns_survive_a_leading_ping() {
    let rows = [
        json!({"type":"ping"}),
        json!({"type":"message_start","message":{"id":"r","model":"m","future":{"x":null}},"outer":1}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
        json!({"type":"message_stop"}),
    ];
    let (out, _) = response(
        &sse(&rows),
        BodyMode::Streaming,
        BodyMode::Buffered,
        Operation::Generate,
        7,
    );
    let body: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(body["id"], "r");
    assert_eq!(body["future"], rows[1]["message"]["future"]);
    let (out, _) = response(
        &sse(&rows),
        BodyMode::Streaming,
        BodyMode::Streaming,
        Operation::Generate,
        7,
    );
    let native = events(&out);
    let start = native.iter().find(|v| v["type"] == "message_start").unwrap();
    assert_eq!(start["message"]["future"], rows[1]["message"]["future"]);
    assert_eq!(start["outer"], 1);
    // Without the ping the members travel on response.start; they must not be written twice.
    let (out, _) = response(
        &sse(&rows[1..]),
        BodyMode::Streaming,
        BodyMode::Streaming,
        Operation::Generate,
        7,
    );
    let raw = String::from_utf8(out).unwrap();
    assert_eq!(raw.matches("\"future\"").count(), 1, "{raw}");
}

#[test]
fn citation_delta_keeps_envelope_unknowns() {
    let rows = [
        json!({"type":"message_start","message":{"id":"r","model":"m"}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"outer":17,"delta":{"type":"citations_delta","citation":{"type":"char_location","start_char_index":0,"end_char_index":1},"future":23}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
        json!({"type":"message_stop"}),
    ];
    let (out, _) = response(
        &sse(&rows),
        BodyMode::Streaming,
        BodyMode::Streaming,
        Operation::Generate,
        7,
    );
    let native = events(&out);
    let citation = native.iter().find(|v| v["delta"]["type"] == "citations_delta").unwrap();
    assert_eq!(citation, &rows[2]);
}

/// Projects one buffered Chat response body through Anthropic response egress.
fn chat_to_anthropic(body: &[u8]) -> Value {
    let mut mapped = Mapped::new();
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let mut ingress = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Buffered,
        10000,
    )
    .unwrap();
    let mut egress = anthropic::egress::Egress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Buffered,
        |_| {},
    )
    .unwrap();
    ingress.advance(&mut mapped.writer, body).unwrap();
    ingress.finish(&mut mapped.writer).unwrap();
    let mut out = Vec::new();
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        egress
            .advance(&mut mapped.writer, lease, |part| {
                out.extend_from_slice(&bytes(&part));
                Ok(())
            })
            .unwrap();
    }
    serde_json::from_slice(&out).unwrap()
}

#[test]
fn chat_usage_with_known_cache_components_projects_uncached_input() {
    for (details, expected) in [
        (json!({"cached_tokens":2,"cache_write_tokens":0}), json!(10)),
        (json!({"cached_tokens":2,"cache_write_tokens":4}), json!(6)),
        // Inconsistent operands are not projected.
        (json!({"cached_tokens":20,"cache_write_tokens":0}), Value::Null),
    ] {
        let body = json!({"id":"r","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"x"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":3,"prompt_tokens_details":details}});
        let out = chat_to_anthropic(&serde_json::to_vec(&body).unwrap());
        assert_eq!(out["usage"]["input_tokens"], expected, "out={out}");
        assert_eq!(out["usage"]["cache_read_input_tokens"], details["cached_tokens"]);
        assert_eq!(out["usage"]["output_tokens"], 3);
    }
}

#[test]
fn mcp_tool_use_publishes_its_server_as_namespace() {
    let body = json!({"id":"r","model":"m","type":"message","role":"assistant","content":[{"type":"mcp_tool_use","id":"c","name":"lookup","server_name":"catalog","input":{}},{"type":"server_tool_use","id":"s","name":"web_search","input":{}}],"stop_reason":"end_turn"});
    let (out, canonical) = response(
        &serde_json::to_vec(&body).unwrap(),
        BodyMode::Buffered,
        BodyMode::Buffered,
        Operation::Generate,
        7,
    );
    let start = canonical.iter().find(|v| v["name"] == "lookup").unwrap();
    assert_eq!(start["namespace"], "catalog", "start={start}");
    let start = canonical.iter().find(|v| v["name"] == "web_search").unwrap();
    assert!(start.get("namespace").is_none(), "start={start}");
    let out: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(out["content"], body["content"]);
}
