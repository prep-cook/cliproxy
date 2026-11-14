//! Regressions from the final review of the Gemini codec, one input per finding.
use super::*;
use gemini::response::Framing;

/// Streamed egress writes the finish, its message and the usage with the last part, so every
/// record carries content (the SDK keeps the turn in chat history) and the output re-enters
/// Gemini ingress.
#[test]
fn streamed_finish_and_usage_travel_with_the_last_part() {
    let body = json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"hi"}]},"finishReason":"STOP","finishMessage":"done"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":2,"totalTokenCount":3}});
    let two = json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"a"},{"text":"b"}]},"finishReason":"STOP"}]});
    for downstream in [Framing::Sse, Framing::JsonArray] {
        let (out, _) = response(
            &sse(std::slice::from_ref(&body)),
            Framing::Sse,
            downstream,
            Operation::Generate,
        );
        let rows = match downstream {
            Framing::Sse => events(&out),
            _ => serde_json::from_slice::<Vec<Value>>(&out).unwrap(),
        };
        assert_eq!(rows, vec![body.clone()]);
        let (again, _) = response(&out, downstream, Framing::Json, Operation::Generate);
        assert_eq!(serde_json::from_slice::<Value>(&again).unwrap(), body);
        let (out, _) = response(
            &sse(std::slice::from_ref(&two)),
            Framing::Sse,
            downstream,
            Operation::Generate,
        );
        let rows = match downstream {
            Framing::Sse => events(&out),
            _ => serde_json::from_slice::<Vec<Value>>(&out).unwrap(),
        };
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter()
                .all(|v| v["candidates"][0]["content"]["parts"][0].is_object()),
            "{rows:?}"
        );
        assert_eq!(rows[1]["candidates"][0]["finishReason"], "STOP");
    }
}

/// Chat tool messages become one user turn of function responses linked to their calls.
#[test]
fn chat_tool_results_become_function_responses() {
    use super::regressions::{chat_to_gemini, validate};
    let out = chat_to_gemini(
        br#"{"model":"m","messages":[{"role":"assistant","tool_calls":[{"id":"c","type":"function","function":{"name":"f","arguments":"{}"}},{"id":"d","type":"function","function":{"name":"g","arguments":"{}"}}]},{"role":"tool","tool_call_id":"c","content":"{\"ok\":true}"},{"role":"tool","tool_call_id":"d","content":[{"type":"text","text":"a"},{"type":"text","text":"b"}]},{"role":"user","content":"next"}]}"#,
    );
    assert_eq!(
        out["contents"][1],
        json!({"role":"user","parts":[
            {"functionResponse":{"name":"f","id":"c","response":{"result":"{\"ok\":true}"}}},
            {"functionResponse":{"name":"g","id":"d","response":{"result":"ab"}}}
        ]})
    );
    assert_eq!(out["contents"][2], json!({"role":"user","parts":[{"text":"next"}]}));
    let orphan = json!({"model":"m","messages":[{"role":"user","content":"x"},{"role":"tool","tool_call_id":"c","content":"r"}]});
    assert!(matches!(validate(&orphan), Err(codec::Error::Unsupported)));
}
