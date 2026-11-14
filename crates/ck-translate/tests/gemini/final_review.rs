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
