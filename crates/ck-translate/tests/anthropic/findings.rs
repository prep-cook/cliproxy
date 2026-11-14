//! Regression tests for the Anthropic codec review findings.
use crate::{BodyMode, Operation, events, json, request, request_fragmented, response, sse};

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
