//! Regressions salvaged from the unfinished tail branch and found while finishing the codec.
use super::*;
use gemini::response::Framing;

fn result_ids(canonical: &[Value], message: usize) -> Vec<Value> {
    let prefix = format!("/messages/{message}/content/");
    canonical
        .iter()
        .filter(|v| {
            v["pointer"]
                .as_str()
                .is_some_and(|p| p.starts_with(&prefix) && p.ends_with("/tool_call_id"))
        })
        .map(|v| v["value"].clone())
        .collect()
}

/// Responses without ids answer their turn's calls in order, per name.
#[test]
fn same_name_function_results_consume_calls_fifo() {
    let input = json!({"contents":[
        {"role":"model","parts":[{"functionCall":{"id":"first","name":"f","args":{"n":1}}},{"functionCall":{"id":"other","name":"g","args":{}}},{"functionCall":{"id":"second","name":"f","args":{"n":2}}}]},
        {"role":"user","parts":[{"functionResponse":{"name":"f","response":{"n":1}}},{"functionResponse":{"name":"g","response":{}}},{"functionResponse":{"name":"f","response":{"n":2}}}]},
        {"role":"model","parts":[{"functionCall":{"id":"third","name":"f","args":{}}}]},
        {"role":"user","parts":[{"functionResponse":{"name":"f","response":{}}}]}
    ]});
    let (out, canonical) = request(&input, Operation::Generate);
    assert_eq!(out, input);
    assert_eq!(
        result_ids(&canonical, 1),
        [json!("first"), json!("other"), json!("second")]
    );
    assert_eq!(result_ids(&canonical, 3), [json!("third")]);
}

/// A response answers the latest model turn, never an unanswered call of an earlier turn.
#[test]
fn function_results_answer_the_latest_call_turn() {
    let input = json!({"contents":[
        {"role":"model","parts":[{"functionCall":{"id":"stale","name":"f","args":{}}}]},
        {"role":"user","parts":[{"text":"never mind"}]},
        {"role":"model","parts":[{"functionCall":{"id":"fresh","name":"f","args":{}}}]},
        {"role":"user","parts":[{"functionResponse":{"name":"f","response":{}}}]}
    ]});
    let (out, canonical) = request(&input, Operation::Generate);
    assert_eq!(out, input);
    assert_eq!(result_ids(&canonical, 3), [json!("fresh")]);
}

#[test]
fn prompt_block_is_a_native_terminal_without_candidates() {
    let body = json!({"promptFeedback":{"blockReason":"SAFETY","safetyRatings":[{"blocked":true}]}});
    let (out, _) = response(
        &sse(std::slice::from_ref(&body)),
        Framing::Sse,
        Framing::Json,
        Operation::Generate,
    );
    let out: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(out["promptFeedback"], body["promptFeedback"]);
}

#[test]
fn native_error_does_not_expose_payload_or_emit_success() {
    let mut mapped = Mapped::new();
    let mut ingress = gemini::response::Ingress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Buffered,
        1024,
    )
    .unwrap();
    ingress
        .advance(
            &mut mapped.writer,
            br#"{"error":{"message":"credential-secret","code":429}}"#,
        )
        .unwrap();
    assert!(ingress.finish(&mut mapped.writer).is_err());
    let raw = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert!(!raw.contains("credential-secret"));
    assert!(!raw.contains("event: x_ck.done"));
}

/// Identity that arrives after the first chunk reaches canonical chunks and the Gemini client;
/// an identity the upstream never sent is not fabricated.
#[test]
fn streaming_unknown_metadata_and_late_identity_reach_egress() {
    let rows = [
        json!({"candidates":[{"index":3,"content":{"parts":[{"text":"pre"}]}}],"usageMetadata":{"promptTokenCount":1}}),
        json!({"responseId":"late-id","modelVersion":"late-model","candidates":[{"index":3,"content":{"parts":[{"text":"hi","futurePart":{"n":null}}],"futureContent":[null,1]},"futureCandidate":{"v":0},"finishReason":"STOP"}],"futureRoot":[null,false],"x_ck":{"attempt":99}}),
    ];
    let (out, canonical) = response(&sse(&rows), Framing::Sse, Framing::Sse, Operation::Generate);
    let out = events(&out);
    assert!(out.iter().any(|v| v["futureRoot"] == json!([null, false])));
    assert!(out.iter().any(|v| v["x_ck"] == json!({"attempt":99})));
    assert!(out.iter().any(|v| {
        v["candidates"]
            .as_array()
            .is_some_and(|a| a.iter().any(|c| c["futureCandidate"] == json!({"v":0})))
    }));
    let chunk = canonical
        .iter()
        .find(|v| v["choices"][0]["delta"]["content"] == "hi")
        .unwrap();
    assert_eq!(chunk["id"], "late-id");
    assert_eq!(chunk["model"], "late-model");
    assert!(out[0].get("responseId").is_none(), "{}", out[0]);
    assert!(out[0].get("modelVersion").is_none(), "{}", out[0]);
    let text = out
        .iter()
        .find(|v| v["candidates"][0]["content"]["parts"][0]["text"] == "hi")
        .unwrap();
    assert_eq!(text["responseId"], "late-id");
    assert_eq!(text["modelVersion"], "late-model");
}
