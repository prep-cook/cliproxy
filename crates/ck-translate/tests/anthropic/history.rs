use crate::codec::event::Direction;
use crate::{
    Mapped, Operation, RequestIngress, Value, anthropic, assert_invalid_input, codec, identity, json, request,
};

#[test]
fn server_and_mcp_history_preserve_semantic_results_and_native_order() {
    for (call_type, result_type, result) in [
        (
            "server_tool_use",
            "web_search_tool_result",
            json!([{"type":"web_search_result","url":"https://example.invalid/","encrypted_content":"opaque/==","title":"title"}]),
        ),
        (
            "server_tool_use",
            "web_search_tool_result",
            json!({"type":"web_search_tool_result_error","error_code":"too_many_requests"}),
        ),
        (
            "mcp_tool_use",
            "mcp_tool_result",
            json!([{"type":"text","text":"result"}]),
        ),
    ] {
        let mut call =
            json!({"type":call_type,"id":"c","name":"lookup","input":{"query":"x"},"future_call":{"x":null}});
        if call_type == "mcp_tool_use" {
            call["server_name"] = json!("catalog");
        }
        let body = json!({"model":"m","max_tokens":10,"messages":[{"role":"assistant","content":[{"type":"text","text":"before"},call,{"type":result_type,"tool_use_id":"c","content":result,"is_error":false,"future_result":[null,0]},{"type":"text","text":"after"}]}]});
        let (output, canonical) = request(&serde_json::to_vec(&body).unwrap(), Operation::Generate);
        assert_eq!(output["messages"], body["messages"]);
        assert!(
            canonical
                .iter()
                .any(|e| e["pointer"] == "/messages/0/tool_calls/0/type" && e["value"] == "server")
        );
        assert!(
            canonical
                .iter()
                .any(|e| e["pointer"] == "/messages/0/content/1/type" && e["value"] == "tool_result")
        );
        assert!(
            canonical
                .iter()
                .any(|e| e["pointer"] == "/messages/0/content/1/tool_call_id" && e["value"] == "c")
        );
        if call_type == "mcp_tool_use" {
            assert!(
                canonical
                    .iter()
                    .any(|e| e["pointer"] == "/messages/0/tool_calls/0/namespace" && e["value"] == "catalog")
            );
        }
    }
}

#[test]
fn unlinked_forward_and_wrong_kind_server_results_fail_the_fence() {
    for blocks in [
        json!([{"type":"mcp_tool_result","tool_use_id":"missing","content":[]}]),
        json!([{"type":"mcp_tool_result","tool_use_id":"c","content":[]},{"type":"mcp_tool_use","server_name":"catalog","id":"c","name":"f","input":{}}]),
        json!([{"type":"tool_use","id":"c","name":"f","input":{}},{"type":"web_search_tool_result","tool_use_id":"c","content":[]}]),
    ] {
        let mut mapped = Mapped::new();
        let mut input = anthropic::request::Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Request),
            Operation::Generate,
            10_000,
        )
        .unwrap();
        input
            .advance(
                &mut mapped.writer,
                &serde_json::to_vec(
                    &json!({"model":"m","max_tokens":10,"messages":[{"role":"assistant","content":blocks}]}),
                )
                .unwrap(),
            )
            .unwrap();
        assert!(
            input
                .finish(&mut mapped.writer, &anthropic::request::AnthropicCapability)
                .is_err()
        );
        let raw = mapped.output(&mapped.writer);
        assert_invalid_input(&raw, "client");
        assert!(!std::str::from_utf8(&raw).unwrap().contains("event: x_ck.request.end\n"));
    }
}

/// Asserts that an Anthropic request with `messages` fails the fence with the client error.
fn assert_rejected(messages: Value) {
    let mut mapped = Mapped::new();
    let mut input = anthropic::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        10_000,
    )
    .unwrap();
    let body = json!({"model":"m","max_tokens":10,"messages":messages});
    input
        .advance(&mut mapped.writer, &serde_json::to_vec(&body).unwrap())
        .unwrap();
    let result = input.finish(&mut mapped.writer, &anthropic::request::AnthropicCapability);
    assert!(matches!(result, Err(codec::Error::Schema)), "{messages}");
    let raw = mapped.output(&mapped.writer);
    assert_invalid_input(&raw, "client");
    assert!(!std::str::from_utf8(&raw).unwrap().contains("event: x_ck.request.end\n"));
}

fn call(id: &str) -> Value {
    json!({"type":"tool_use","id":id,"name":"f","input":{}})
}

fn result(id: &str) -> Value {
    json!({"type":"tool_result","tool_use_id":id,"content":"r"})
}

#[test]
fn duplicate_tool_call_ids_fail_the_fence() {
    assert_rejected(json!([{"role":"assistant","content":[call("a"),call("a")]}]));
    assert_rejected(json!([
        {"role":"assistant","content":[call("a")]},
        {"role":"user","content":[result("a")]},
        {"role":"assistant","content":[call("b"),call("a")]},
    ]));
}

#[test]
fn client_results_need_a_call_in_an_earlier_message() {
    assert_rejected(json!([
        {"role":"user","content":[result("a")]},
        {"role":"assistant","content":[call("a")]},
    ]));
    assert_rejected(json!([{"role":"assistant","content":[call("a"),result("a")]}]));
}

#[test]
fn duplicate_chat_tool_call_ids_fail_the_anthropic_fence() {
    let call = |id: &str| json!({"id":id,"type":"function","function":{"name":"f","arguments":"{}"}});
    let body = json!({"model":"m","max_tokens":10,"messages":[
        {"role":"assistant","content":null,"tool_calls":[call("a")]},
        {"role":"tool","tool_call_id":"a","content":"r"},
        {"role":"assistant","content":null,"tool_calls":[call("a")]},
    ]});
    let mut mapped = Mapped::new();
    let mut input = codec::chat::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        10_000,
    )
    .unwrap();
    input
        .advance(&mut mapped.writer, &serde_json::to_vec(&body).unwrap())
        .unwrap();
    let result = input.finish(&mut mapped.writer, &anthropic::request::AnthropicCapability);
    assert!(matches!(result, Err(codec::Error::Schema)));
    let error: Value = crate::events(&mapped.output(&mapped.writer)).pop().unwrap();
    assert_eq!(error["code"], "stream.invalid_input");
    assert_eq!(error["status"], 400);
}
