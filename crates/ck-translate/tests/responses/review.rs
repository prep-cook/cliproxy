//! Reproductions from the codec review: each test pins one corrected mapping.
use super::*;

/// Runs a Responses request through ingress and the Chat capability fence.
fn to_chat(input: &Value) -> codec::Result<()> {
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
        .advance(&mut m.writer, &serde_json::to_vec(input).unwrap())
        .unwrap();
    ingress
        .finish(&mut m.writer, &codec::chat::request::ChatCapability)
        .map(|_| ())
}

#[test]
fn json_schema_format_is_flat_in_responses_and_nested_in_chat() {
    let output = super::request::from_chat(&json!({"model":"m","messages":[{"role":"user","content":"q"}],
        "response_format":{"type":"json_schema","json_schema":{"name":"answer","schema":{"type":"object"},"strict":true}}}));
    assert_eq!(
        output["text"]["format"],
        json!({"type":"json_schema","name":"answer","schema":{"type":"object"},"strict":true})
    );
    let flat = json!({"model":"m","input":"q",
        "text":{"format":{"type":"json_schema","name":"answer","description":"d","schema":{"type":"object"},"strict":true}}});
    assert!(to_chat(&flat).is_ok());
    let (output, _) = request(&flat, Operation::Generate);
    assert_eq!(output["text"], flat["text"]);
}

#[test]
fn continuation_tool_outputs_link_to_the_referenced_state() {
    let output = json!({"type":"function_call_output","call_id":"c","output":"done"});
    for (field, value) in [("previous_response_id", json!("prev")), ("conversation", json!("conv"))] {
        let mut input = json!({"model":"m","input":[output]});
        input[field] = value;
        let (encoded, _) = request(&input, Operation::Generate);
        assert_eq!(encoded[field], input[field]);
        assert_eq!(encoded["input"][0], output);
    }
}
