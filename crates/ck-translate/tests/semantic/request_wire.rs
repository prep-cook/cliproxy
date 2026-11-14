//! Chat request ingress and upstream egress: native semantics, legacy spelling, image
//! extensions, terminals and incremental publication.
use super::*;

#[test]
fn request_preserves_nullable_content_sampling_and_structured_tool_history() {
    let input = br#"{"model":"m","temperature":0,"top_p":0.9,"messages":[{"role":"assistant","content":null,"tool_calls":[{"id":"call","type":"function","function":{"name":"f","arguments":"{\"nested\":[null,0,\"x\"]}"}}]},{"role":"tool","tool_call_id":"call","content":"ok"}],"x_ck":{"attempt":999,"media":[{"media_id":"spoof"}]},"vendor":{"nested":[null,false]}}"#;
    let (output, events) = request(input);
    assert_eq!(output["messages"][0]["content"], serde_json::Value::Null);
    assert_eq!(output["temperature"], 0);
    assert_eq!(output["top_p"], 0.9);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(
            output["messages"][0]["tool_calls"][0]["function"]["arguments"]
                .as_str()
                .unwrap()
        )
        .unwrap(),
        serde_json::json!({"nested":[null,0,"x"]})
    );
    assert_eq!(output["x_ck"]["attempt"], 999);
    assert_eq!(output["vendor"], serde_json::json!({"nested":[null,false]}));
    let argument = events
        .iter()
        .find(|event| event["pointer"] == "/messages/0/tool_calls/0/function/arguments")
        .unwrap();
    assert_eq!(argument["value"], serde_json::json!({}));
    assert!(events.iter().all(|event| event["x_ck"]["attempt"] == 1));
}

#[test]
fn request_media_is_normalized_and_not_copied_inline_in_canonical_fields() {
    let (output, events) = request(br#"{"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"hi"},{"type":"image_url","image_url":{"url":"https://example.test/a.png","detail":"high"}},{"type":"input_audio","input_audio":{"data":"AAECAw==","format":"wav"}}]}]}"#);
    assert_eq!(output["messages"][0]["content"][1]["image_url"]["detail"], "high");
    assert_eq!(output["messages"][0]["content"][2]["input_audio"]["data"], "AAECAw==");
    assert!(
        events
            .iter()
            .any(|event| event["pointer"] == "/messages/0/content/1/type" && event["value"] == "image")
    );
    assert!(
        events
            .iter()
            .any(|event| event["pointer"] == "/messages/0/content/2/type" && event["value"] == "audio")
    );
    assert!(
        events
            .iter()
            .filter(|event| event.get("pointer").is_some())
            .all(|event| event["value"] != "AAECAw==")
    );
    assert!(events.iter().any(|event| event["bytes"] == "AAECAw=="));
}

#[test]
fn request_rejects_duplicate_decoded_keys_inside_argument_json_and_unlinked_results() {
    for input in [
        br#"{"model":"m","messages":[{"role":"assistant","tool_calls":[{"id":"c","type":"function","function":{"name":"f","arguments":"{\"a\":1,\"\\u0061\":2}"}}]}]}"#.as_slice(),
        br#"{"model":"m","messages":[{"role":"tool","tool_call_id":"unknown","content":"x"}]}"#,
    ] {
        let mut mapped = Mapped::new();
        let mut ingress = Ingress::new(&mut mapped.writer, &mapped.store, identity(Direction::Request), Operation::Generate, 1024 * 1024).unwrap();
        let result = ingress.advance(&mut mapped.writer, input);
        assert!(result.is_err() || ingress.finish(&mut mapped.writer, &ChatCapability).is_err());
        let events = mapped.output(&mapped.writer);
        assert!(!std::str::from_utf8(&events).unwrap().contains("event: x_ck.request.end\n"));
    }
}

#[test]
fn unsupported_operations_never_create_a_chat_body() {
    let mut mapped = Mapped::new();
    for operation in [Operation::CountTokens, Operation::Compact] {
        assert!(matches!(
            Ingress::new(
                &mut mapped.writer,
                &mapped.store,
                identity(Direction::Request),
                operation,
                1024
            ),
            Err(codec::Error::Unsupported)
        ));
    }
    assert!(mapped.output(&mapped.writer).is_empty());
}

#[test]
fn legacy_functions_and_selection_keep_native_spelling() {
    let (out, _) =
        request(br#"{"model":"m","messages":[],"functions":[{"name":"a"},{"name":"b"}],"function_call":{"name":"a"}}"#);
    assert_eq!(out["functions"], serde_json::json!([{"name":"a"},{"name":"b"}]));
    assert_eq!(out["function_call"], serde_json::json!({"name":"a"}));
    assert!(out.get("tool_choice").is_none());
}

#[test]
fn wire_legacy_marker_cannot_erase_modern_history() {
    let (out, _) = request(br#"{"model":"m","messages":[{"role":"assistant","tool_calls":[{"id":"a","type":"function","chat_legacy":false,"function":{"name":"a","arguments":"{}"}},{"id":"b","type":"function","function":{"name":"b","arguments":"{}"}}]}]}"#);
    assert_eq!(out["messages"][0]["tool_calls"][1]["id"], "b");
    assert_eq!(out["messages"][0]["tool_calls"][0]["chat_legacy"], false);
    assert!(out["messages"][0].get("function_call").is_none());
}

#[test]
fn mapped_image_extensions_keep_native_locations() {
    let (out, _) = request(br#"{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","vendor_part":1,"image_url":{"url":"https://example.test/image","vendor_image":{"k":2}}}]}]}"#);
    assert_eq!(out["messages"][0]["content"][0]["vendor_part"], 1);
    assert_eq!(
        out["messages"][0]["content"][0]["image_url"]["vendor_image"],
        serde_json::json!({"k":2})
    );
}

#[test]
fn native_image_ref_collision_preserves_url_and_unknown_data() {
    let input = serde_json::json!({"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.invalid/a.png","ref":"literal"}}]}]});
    let (output, _) = request(input.to_string().as_bytes());
    assert_eq!(output["messages"], input["messages"]);
}

#[test]
fn malformed_request_advance_publishes_only_error_terminal() {
    let mut mapped = Mapped::new();
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        1024,
    )
    .unwrap();
    assert!(ingress.advance(&mut mapped.writer, b"{]").is_err());
    assert!(ingress.finish(&mut mapped.writer, &ChatCapability).is_err());
    let output = mapped.output(&mapped.writer);
    let output = std::str::from_utf8(&output).unwrap();
    assert!(output.contains("event: x_ck.error\n"));
    assert!(!output.contains("event: x_ck.request.end\n"));
    assert!(!output.contains("event: x_ck.done\n"));
}

#[test]
fn completed_request_fields_and_messages_publish_before_the_body_ends() {
    let mut mapped = Mapped::configured(std::sync::Arc::new(ck_stream::ManualClock::default()), 256);
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        4096,
    )
    .unwrap();
    ingress
        .advance(
            &mut mapped.writer,
            br#"{"model":"m","messages":[{"role":"user","content":"first"},"#,
        )
        .unwrap();
    let prefix = canonical_values(&mapped);
    assert!(
        prefix
            .iter()
            .any(|event| event["pointer"] == "/model" && event["value"] == "m")
    );
    assert!(
        prefix
            .iter()
            .any(|event| event["pointer"] == "/messages/0/content" && event["value"] == "first")
    );
    assert!(
        !String::from_utf8(mapped.output(&mapped.writer))
            .unwrap()
            .contains("event: x_ck.request.end\n")
    );
    for byte in
        br#"{"role":"user","content":"second"}],"temperature":0,"reasoning_effort":"low","reasoning":{"budget":0}}"#
    {
        ingress.advance(&mut mapped.writer, &[*byte]).unwrap();
    }
    let ready = ingress.finish(&mut mapped.writer, &ChatCapability).unwrap();
    let output = Egress {
        destination: "chat",
        upstream_stream: false,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    let output: serde_json::Value = serde_json::from_slice(&bytes(&output)).unwrap();
    assert_eq!(
        output["messages"],
        serde_json::json!([
            {"role":"user","content":"first"}, {"role":"user","content":"second"}
        ])
    );
    assert_eq!(output["reasoning_effort"], "low");
    assert_eq!(output["reasoning"], serde_json::json!({"budget":0}));
    let mut assigned = std::collections::HashSet::new();
    for event in canonical_values(&mapped) {
        let Some(pointer) = event["pointer"].as_str() else {
            continue;
        };
        assert!(assigned.insert(pointer.to_owned()), "duplicate assignment: {pointer}");
        if let Some((parent, _)) = pointer.rsplit_once('/') {
            assert!(assigned.contains(parent), "missing parent: {parent}");
        }
        if event["value"].is_array() || event["value"].is_object() {
            assert!(
                event["value"]
                    .as_array()
                    .map_or_else(|| event["value"].as_object().unwrap().is_empty(), Vec::is_empty)
            );
        }
    }
}

#[test]
fn late_destination_rejection_keeps_published_prefix_without_success() {
    struct TextOnly;
    impl codec::Capability for TextOnly {
        fn destination(&self) -> &'static str {
            "text-only"
        }
        fn validate(&self, view: codec::RequestView<'_>) -> codec::Result<()> {
            let doc = view.document;
            assert!(doc.member(doc.root(), "model")?.is_some());
            if doc.member(doc.root(), "modalities")?.is_some() {
                Err(codec::Error::Unsupported)
            } else {
                Ok(())
            }
        }
    }
    let mut mapped = Mapped::new();
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        4096,
    )
    .unwrap();
    ingress
        .advance(&mut mapped.writer, br#"{"model":"m","messages":[],"#)
        .unwrap();
    assert!(canonical_values(&mapped).iter().any(|e| e["pointer"] == "/model"));
    ingress
        .advance(&mut mapped.writer, br#""modalities":["audio"]}"#)
        .unwrap();
    assert!(matches!(
        ingress.finish(&mut mapped.writer, &TextOnly),
        Err(codec::Error::Unsupported)
    ));
    assert!(ingress.finish(&mut mapped.writer, &ChatCapability).is_err());
    let output = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert!(output.contains("event: x_ck.error\n"));
    assert!(!output.contains("event: x_ck.request.end\n"));
    assert!(!output.contains("event: x_ck.done\n"));
}

#[test]
fn normalized_alias_collisions_fail_in_either_source_order() {
    for input in [
        br#"{"model":"m","messages":[],"functions":[],"tools":[]}"#.as_slice(),
        br#"{"model":"m","messages":[],"tools":[],"functions":[]}"#,
        br#"{"model":"m","messages":[],"function_call":"auto","tool_choice":"none"}"#,
        br#"{"model":"m","messages":[],"tool_choice":"auto","function_call":"none"}"#,
    ] {
        let mut mapped = Mapped::new();
        let mut ingress = Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Request),
            Operation::Generate,
            4096,
        )
        .unwrap();
        let result = ingress.advance(&mut mapped.writer, input);
        assert!(result.is_err() || ingress.finish(&mut mapped.writer, &ChatCapability).is_err());
        let output = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
        assert!(!output.contains("event: x_ck.request.end\n"));
    }
}

#[test]
fn completed_message_media_is_available_before_later_request_fields() {
    let mut mapped = Mapped::new();
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        4096,
    )
    .unwrap();
    ingress.advance(&mut mapped.writer, br#"{"model":"m","messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"AAECAw==","format":"wav"}}]}],"#).unwrap();
    let prefix = canonical_values(&mapped);
    let reference = prefix
        .iter()
        .find(|event| event["pointer"] == "/messages/0/content/0/source/ref")
        .unwrap();
    assert_eq!(reference["value"]["value_kind"], "bytes");
    let media_id = &reference["value"]["media_id"];
    assert!(
        prefix
            .iter()
            .any(|event| event["x_ck"]["media_id"] == *media_id && event["bytes"] == "AAECAw==")
    );
    let output = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert!(output.contains("event: x_ck.media.end\n"));
    assert!(!output.contains("event: x_ck.request.end\n"));
    ingress.advance(&mut mapped.writer, br#""top_p":0.5}"#).unwrap();
    let ready = ingress.finish(&mut mapped.writer, &ChatCapability).unwrap();
    let output = Egress {
        destination: "chat",
        upstream_stream: true,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    let output: serde_json::Value = serde_json::from_slice(&bytes(&output)).unwrap();
    assert_eq!(output["messages"][0]["content"][0]["input_audio"]["data"], "AAECAw==");
    assert_eq!(output["top_p"], 0.5);
    let events = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert_eq!(events.matches("event: x_ck.media.start\n").count(), 1);
    assert_eq!(events.matches("event: x_ck.media.end\n").count(), 1);
}

#[test]
fn invalid_suffix_cannot_complete_an_incrementally_published_request() {
    for suffix in [
        br#""model":"replacement"}"#.as_slice(),
        br#""late":true}false"#,
        br#""late":"\uD800"}"#,
    ] {
        let mut mapped = Mapped::new();
        let mut ingress = Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Request),
            Operation::Generate,
            4096,
        )
        .unwrap();
        ingress
            .advance(
                &mut mapped.writer,
                br#"{"model":"m","messages":[{"role":"user","content":"kept"}],"#,
            )
            .unwrap();
        assert!(
            canonical_values(&mapped)
                .iter()
                .any(|e| e["pointer"] == "/messages/0/content" && e["value"] == "kept")
        );
        let result = ingress.advance(&mut mapped.writer, suffix);
        assert!(result.is_err() || ingress.finish(&mut mapped.writer, &ChatCapability).is_err());
        let before = mapped.output(&mapped.writer);
        assert!(ingress.advance(&mut mapped.writer, b" ").is_err());
        assert_eq!(mapped.output(&mapped.writer), before);
        let events = String::from_utf8(before).unwrap();
        assert_eq!(events.matches("event: x_ck.error\n").count(), 1);
        assert!(!events.contains("event: x_ck.request.end\n"));
        assert!(!events.contains("event: x_ck.done\n"));
    }
}

#[test]
fn empty_tool_call_arguments_fail_with_one_client_error_terminal() {
    let input = br#"{"model":"m","messages":[{"role":"assistant","tool_calls":[{"id":"c","type":"function","function":{"name":"f","arguments":""}}]}]}"#;
    let mut mapped = Mapped::new();
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        4096,
    )
    .unwrap();
    let advanced = ingress.advance(&mut mapped.writer, input);
    let finished = advanced.and_then(|()| ingress.finish(&mut mapped.writer, &ChatCapability).map(drop));
    let events = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert!(finished.is_err());
    assert_eq!(events.matches("event: x_ck.error\n").count(), 1, "{events}");
    assert!(events.contains("\"client\""), "{events}");
    assert!(!events.contains("event: x_ck.request.end\n"));
}
