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

/// Gemini body through Gemini ingress and buffered Chat egress.
fn gemini_to_chat(body: &Value) -> codec::Result<Value> {
    let mut mapped = Mapped::new();
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let mut ingress = gemini::response::Ingress::with_framing(
        &mut mapped.writer,
        identity(Direction::Response),
        Operation::Generate,
        Framing::Json,
        1 << 20,
    )
    .unwrap();
    ingress
        .advance(&mut mapped.writer, &serde_json::to_vec(body).unwrap())
        .unwrap();
    ingress.finish(&mut mapped.writer).unwrap();
    let mut egress = codec::chat::egress::Egress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Buffered,
        true,
        |_| {},
    )
    .unwrap();
    let mut out = Vec::new();
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        egress.advance(&mut mapped.writer, lease, |part| {
            out.extend(bytes(&part));
            Ok(())
        })?;
    }
    Ok(serde_json::from_slice(&out).unwrap())
}

/// A Gemini call without a native id reaches Chat under its canonical call id.
#[test]
fn id_less_gemini_call_reaches_chat() {
    let body =
        json!({"candidates":[{"content":{"parts":[{"functionCall":{"name":"f","args":{}}}]},"finishReason":"STOP"}]});
    let out = gemini_to_chat(&body).unwrap();
    let call = &out["choices"][0]["message"]["tool_calls"][0];
    assert_eq!(call["function"]["name"], "f");
    assert!(call["id"].as_str().is_some_and(|id| !id.is_empty()), "{out}");
    assert_eq!(out["choices"][0]["finish_reason"], "tool_calls");
}
