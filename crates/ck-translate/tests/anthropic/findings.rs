//! Regression tests for the Anthropic codec review findings.
use crate::{Operation, request_fragmented};

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
