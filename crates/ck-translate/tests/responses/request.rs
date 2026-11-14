use super::*;

/// Encodes a Chat request for a Responses upstream.
pub(super) fn from_chat(input: &Value) -> Value {
    let mut m = Mapped::new();
    let mut ingress = codec::chat::request::Ingress::new(
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
    let ready = ingress
        .finish(&mut m.writer, &responses::request::ResponsesCapability)
        .unwrap();
    let encoded = responses::request::Egress {
        destination: "responses",
        upstream_stream: false,
    }
    .encode(&ready, &mut m.writer)
    .unwrap();
    serde_json::from_slice(&bytes(&encoded)).unwrap()
}

#[test]
fn assistant_text_and_reasoning_survive_next_to_tool_calls() {
    let call = json!({"id":"c","type":"function","function":{"name":"f","arguments":"{\"x\":1}"}});
    let output = from_chat(&json!({"model":"m","messages":[
        {"role":"user","content":"q"},
        {"role":"assistant","content":[{"type":"reasoning","text":"think"},{"type":"text","text":"checking"}],"tool_calls":[call]},
        {"role":"tool","tool_call_id":"c","content":"r"},
        {"role":"assistant","content":"","tool_calls":[{"id":"d","type":"function","function":{"name":"f","arguments":"{}"}}]},
        {"role":"tool","tool_call_id":"d","content":"s"},
        {"role":"assistant","content":null,"tool_calls":[{"id":"e","type":"function","function":{"name":"f","arguments":"{}"}}]},
        {"role":"tool","tool_call_id":"e","content":"t"},
    ]}));
    let kinds: Vec<_> = output["input"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["type"].as_str().unwrap())
        .collect();
    assert_eq!(
        kinds,
        [
            "message",
            "reasoning",
            "message",
            "function_call",
            "function_call_output",
            "function_call",
            "function_call_output",
            "function_call",
            "function_call_output"
        ]
    );
    assert_eq!(output["input"][1]["content"][0]["text"], "think");
    assert_eq!(output["input"][2]["role"], "assistant");
    assert_eq!(
        output["input"][2]["content"],
        json!([{"type":"output_text","text":"checking"}])
    );
    assert_eq!(output["input"][3]["call_id"], "c");
}

#[test]
fn chat_fields_without_a_responses_mapping_stay_out_of_the_request() {
    let output =
        from_chat(&json!({"model":"m","messages":[{"role":"user","content":"q","extra":1}],"vendor_field":true}));
    assert_eq!(
        output,
        json!({"input":[{"type":"message","role":"user","content":"q"}],"stream":false,"model":"m"})
    );
}

#[test]
fn native_options_tools_and_custom_outputs_round_trip() {
    let input = json!({"model":"m",
        "input":[
            {"type":"custom_tool_call","call_id":"c","name":"g","input":"raw"},
            {"type":"custom_tool_call_output","call_id":"c","output":"done"},
        ],
        "tools":[{"type":"function","name":"f","parameters":{}},{"type":"web_search"}],
        "text":{"format":{"type":"json_object"}},
        "reasoning":{"effort":"high","summary":"auto"},
        "truncation":"auto",
        "include":["reasoning.encrypted_content"],
    });
    let (output, events) = request(&input, Operation::Generate);
    assert_eq!(output["input"][1]["type"], "custom_tool_call_output");
    for field in ["tools", "text", "reasoning", "truncation", "include"] {
        assert_eq!(output[field], input[field], "{field}");
    }
    assert!(events.iter().any(|e| e["pointer"] == "/x_ck/thinking/effort"));
}

#[test]
fn unknown_roles_and_unlinked_tool_outputs_are_refused() {
    for input in [
        json!({"model":"m","input":[{"type":"message","role":"critic","content":"x"}]}),
        json!({"model":"m","input":[{"type":"function_call_output","call_id":"c","output":"r"}]}),
    ] {
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
            .advance(&mut m.writer, &serde_json::to_vec(&input).unwrap())
            .unwrap();
        assert!(
            ingress
                .finish(&mut m.writer, &responses::request::ResponsesCapability)
                .is_err()
        );
    }
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
    assert_invalid_input(output.as_bytes(), "client");
}

#[test]
fn typed_tool_results_preserve_text_and_file_data() {
    let input = json!({"model":"m","input":[{"type":"function_call","call_id":"c","name":"f","arguments":"{}"},{"type":"function_call_output","call_id":"c","output":[{"type":"input_text","text":"ok"},{"type":"input_file","filename":"a.pdf","file_data":"data:application/pdf;base64,JVBERi0xLjQK"}]}]});
    let (output, _) = request(&input, Operation::Generate);
    assert_eq!(output["input"][1]["output"], input["input"][1]["output"]);
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
