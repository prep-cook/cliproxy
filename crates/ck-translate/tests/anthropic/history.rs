use crate::codec::event::Direction;
use crate::{Mapped, Operation, RequestIngress, anthropic, events, identity, json, request};

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
        assert_eq!(events(&raw).last().unwrap()["scope"], "client");
        assert!(!std::str::from_utf8(&raw).unwrap().contains("event: x_ck.request.end\n"));
    }
}
