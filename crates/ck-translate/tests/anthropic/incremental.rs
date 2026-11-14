use crate::{Mapped, Operation, RequestEgress, RequestIngress, Value, anthropic, bytes, codec, events, identity, json};
use codec::event::Direction;

fn ingress(mapped: &mut Mapped) -> anthropic::request::Ingress {
    anthropic::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        1_000_000,
    )
    .unwrap()
}
fn finish(mapped: &mut Mapped, ingress: &mut anthropic::request::Ingress) -> Value {
    let ready = ingress
        .finish(&mut mapped.writer, &anthropic::request::AnthropicCapability)
        .unwrap();
    let out = anthropic::request::Egress {
        destination: "anthropic",
        upstream_stream: false,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    serde_json::from_slice(&bytes(&out)).unwrap()
}
fn assert_prefix(mapped: &Mapped) {
    let output = mapped.output(&mapped.writer);
    let text = std::str::from_utf8(&output).unwrap();
    let values = events(&output);
    assert!(
        values
            .iter()
            .any(|event| event["pointer"] == "/model" && event["value"] == "m")
    );
    assert!(values.iter().any(|event| event["bytes"].is_string()));
    assert!(!text.contains("event: x_ck.request.end\n"));
    assert!(!text.contains("event: x_ck.media.end\n"));
    assert!(
        u64::from(mapped.store.used_frames()) <= mapped.writer.committed().frame + 3,
        "raw payload remains charged"
    );
}

#[test]
fn unfinished_text_retires_raw_payload_and_preserves_late_system_order() {
    let mut mapped = Mapped::new();
    let mut input = ingress(&mut mapped);
    let prefix = format!(
        r#"{{"model":"m","max_tokens":10,"messages":[{{"role":"user","content":"{}"#,
        "a".repeat(80_000)
    );
    input.advance(&mut mapped.writer, prefix.as_bytes()).unwrap();
    assert_prefix(&mapped);
    for fragment in [b"\\uD83D".as_slice(), b"\\uDE80", b"\\n", b"\xc3", b"\xa9"] {
        input.advance(&mut mapped.writer, fragment).unwrap();
    }
    input.advance(&mut mapped.writer, br#""}],"system":"late"}"#).unwrap();
    let output = finish(&mut mapped, &mut input);
    assert_eq!(output["system"], "late");
    assert_eq!(output["messages"][0]["content"], format!("{}🚀\né", "a".repeat(80_000)));
    let mut pointers = std::collections::HashSet::new();
    for event in events(&mapped.output(&mapped.writer)) {
        if let Some(pointer) = event["pointer"].as_str() {
            assert!(pointers.insert(pointer.to_owned()), "duplicate {pointer}");
        }
    }
}

#[test]
fn unfinished_base64_publishes_decoded_bytes_and_keeps_one_lifetime() {
    use base64::Engine;
    let payload = vec![173u8; 80_003];
    let encoded = base64::engine::general_purpose::STANDARD.encode(&payload);
    let mut mapped = Mapped::new();
    let mut input = ingress(&mut mapped);
    let prefix = format!(
        r#"{{"model":"m","max_tokens":10,"messages":[{{"role":"user","content":[{{"type":"image","source":{{"type":"base64","media_type":"image/png","data":"{encoded}"#
    );
    input.advance(&mut mapped.writer, prefix.as_bytes()).unwrap();
    assert_prefix(&mapped);
    input
        .advance(&mut mapped.writer, br#"","ref":"literal"}}]}]}"#)
        .unwrap();
    let output = finish(&mut mapped, &mut input);
    assert_eq!(output["messages"][0]["content"][0]["source"]["data"], encoded);
    assert_eq!(output["messages"][0]["content"][0]["source"]["ref"], "literal");
    let output = mapped.output(&mapped.writer);
    let text = std::str::from_utf8(&output).unwrap();
    assert_eq!(text.matches("event: x_ck.media.start\n").count(), 1);
    assert_eq!(text.matches("event: x_ck.media.end\n").count(), 1);
    let decoded: Vec<_> = events(&output)
        .iter()
        .filter_map(|e| e["bytes"].as_str())
        .flat_map(|s| base64::engine::general_purpose::STANDARD.decode(s).unwrap())
        .collect();
    assert_eq!(decoded, payload);
}

#[test]
fn buffered_base64_fragment_sets_a_fixed_flush_deadline() {
    let mut mapped = Mapped::new();
    let mut input = ingress(&mut mapped);
    let prefix = r#"{"model":"m","max_tokens":10,"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"#;
    input.advance(&mut mapped.writer, prefix.as_bytes()).unwrap();
    assert!(input.flush_deadline().is_none(), "first fragment is published at once");
    input.advance(&mut mapped.writer, b"BBBB").unwrap();
    let deadline = input.flush_deadline().expect("buffered fragment needs a deadline");
    input.advance(&mut mapped.writer, b"CCCC").unwrap();
    assert_eq!(
        input.flush_deadline(),
        Some(deadline),
        "later input postponed the deadline"
    );
    let published = mapped.output(&mapped.writer).len();
    input.flush(&mut mapped.writer).unwrap();
    assert!(input.flush_deadline().is_none());
    assert!(
        mapped.output(&mapped.writer).len() > published,
        "flush published nothing"
    );
}

#[test]
fn streamed_prefix_never_authorizes_late_invalid_request() {
    for suffix in [
        r#""}],"tool_choice":{"type":"invalid"}}"#,
        r#""}],"mo\u0064el":"duplicate"}"#,
    ] {
        let mut mapped = Mapped::new();
        let mut input = ingress(&mut mapped);
        let prefix = format!(
            r#"{{"model":"m","max_tokens":10,"messages":[{{"role":"user","content":"{}"#,
            "a".repeat(8192)
        );
        input.advance(&mut mapped.writer, prefix.as_bytes()).unwrap();
        assert_prefix(&mapped);
        let failed = input.advance(&mut mapped.writer, suffix.as_bytes()).is_err()
            || input
                .finish(&mut mapped.writer, &anthropic::request::AnthropicCapability)
                .is_err();
        assert!(failed);
        let output = mapped.output(&mapped.writer);
        let text = std::str::from_utf8(&output).unwrap();
        assert!(text.contains("event: x_ck.error\n"));
        assert!(!text.contains("event: x_ck.request.end\n"));
        assert!(!text.contains("event: x_ck.done\n"));
    }
}

#[test]
fn payload_before_type_and_unknown_lookalikes_keep_logical_values() {
    for source in [
        json!({"data":"AAEC".repeat(3000),"type":"base64","media_type":"image/png"}),
        json!({"data":"plain".repeat(3000),"type":"text","media_type":"text/plain"}),
    ] {
        let input = json!({"model":"m","max_tokens":10,"messages":[{"role":"user","content":[{"source":source,"type":"document"}]}],"unknown":{"source":{"data":"AAEC".repeat(3000),"type":"base64"},"type":"image"}});
        let mut mapped = Mapped::new();
        let mut codec = ingress(&mut mapped);
        for chunk in serde_json::to_vec(&input).unwrap().chunks(37) {
            codec.advance(&mut mapped.writer, chunk).unwrap();
        }
        let output = finish(&mut mapped, &mut codec);
        assert_eq!(output["messages"], input["messages"]);
        assert_eq!(output["unknown"], input["unknown"]);
    }
}

#[test]
fn long_signed_history_and_tool_results_survive_incremental_normalization() {
    let input = json!({"model":"m","max_tokens":10,"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"a".repeat(9000),"signature":"signed/".repeat(2000)},{"type":"redacted_thinking","data":"opaque==".repeat(2000)},{"type":"tool_use","id":"c","name":"f","input":{"long":"arg".repeat(3000)}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"c","content":"result".repeat(2000)}]}]});
    let mut mapped = Mapped::new();
    let mut codec = ingress(&mut mapped);
    for chunk in serde_json::to_vec(&input).unwrap().chunks(8192) {
        codec.advance(&mut mapped.writer, chunk).unwrap();
    }
    let output = finish(&mut mapped, &mut codec);
    assert_eq!(output["messages"], input["messages"]);
}

#[test]
fn empty_advance_flushes_idle_payload_without_closing_media() {
    use base64::Engine;
    let mut mapped = Mapped::new();
    let mut input = ingress(&mut mapped);
    input.advance(&mut mapped.writer, br#"{"model":"m","max_tokens":1,"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","data":"AAEC"#).unwrap();
    input.advance(&mut mapped.writer, b"AwQF").unwrap();
    input.advance(&mut mapped.writer, b"").unwrap();
    let raw = mapped.output(&mapped.writer);
    let decoded: Vec<_> = events(&raw)
        .iter()
        .filter_map(|event| event["bytes"].as_str())
        .flat_map(|s| base64::engine::general_purpose::STANDARD.decode(s).unwrap())
        .collect();
    assert_eq!(decoded, [0, 1, 2, 3, 4, 5]);
    let raw = std::str::from_utf8(&raw).unwrap();
    assert!(!raw.contains("event: x_ck.media.end\n"));
    assert!(!raw.contains("event: x_ck.request.end\n"));
    input.advance(&mut mapped.writer, br#""}}]}]}"#).unwrap();
    assert_eq!(
        finish(&mut mapped, &mut input)["messages"][0]["content"][0]["source"]["data"],
        "AAECAwQF"
    );
}

#[test]
fn malformed_streamed_tails_never_close_media_successfully() {
    for (prefix, tail) in [
        (br#"{"model":"m","max_tokens":1,"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","data":"AAEC"#.as_slice(), br#"A"}}]}]}"#.as_slice()),
        (br#"{"model":"m","max_tokens":1,"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","data":"AAEC"#.as_slice(), br#"!"}}]}]}"#.as_slice()),
    ] {
        let mut mapped = Mapped::new();
        let mut input = ingress(&mut mapped);
        input.advance(&mut mapped.writer, prefix).unwrap();
        assert!(input.advance(&mut mapped.writer, tail).is_err());
        let raw = mapped.output(&mapped.writer);
        let raw = std::str::from_utf8(&raw).unwrap();
        assert!(raw.contains("event: x_ck.error\n"));
        assert!(!raw.contains("event: x_ck.media.end\n"));
        assert!(!raw.contains("event: x_ck.request.end\n"));
    }
}
