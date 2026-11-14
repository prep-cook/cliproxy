use super::*;

/// Encodes a Chat request for a Responses upstream.
fn from_chat(input: &Value) -> Value {
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
