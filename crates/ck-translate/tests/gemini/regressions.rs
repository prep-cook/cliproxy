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

/// A Gemini error body is a terminal upstream failure: one error with a fixed status and code,
/// none of the provider's message.
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
    let error = raw
        .split("\n\n")
        .find(|event| event.starts_with("event: x_ck.error"))
        .unwrap();
    let error: Value = serde_json::from_str(error.lines().find_map(|l| l.strip_prefix("data: ")).unwrap()).unwrap();
    assert_eq!(error["code"], "stream.invalid_input");
    assert_eq!(error["status"], 502);
    assert_eq!(error["scope"], "upstream");
    assert_eq!(raw.matches("event: x_ck.error").count(), 1, "one terminal: {raw}");
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

/// A null message field is unset in protobuf JSON; it is kept, not refused.
#[test]
fn null_generation_config_is_unset() {
    let input = json!({"generationConfig":null,"contents":[{"role":"user","parts":[{"text":"a"}]}]});
    let (out, _) = request(&input, Operation::Generate);
    assert_eq!(out, input);
}

/// Encodes a Chat request for Gemini.
fn chat_to_gemini(input: &[u8]) -> Value {
    let mut mapped = Mapped::new();
    let mut ingress = codec::chat::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        1024 * 1024,
    )
    .unwrap();
    for chunk in input.chunks(7) {
        ingress.advance(&mut mapped.writer, chunk).unwrap();
    }
    let ready = ingress
        .finish(&mut mapped.writer, &gemini::request::GeminiCapability)
        .unwrap();
    let output = gemini::request::Egress {
        destination: "gemini",
        upstream_stream: true,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    serde_json::from_slice(&bytes(&output)).unwrap()
}

/// Members another protocol kept as its own originals are not Gemini fields.
#[test]
fn foreign_unknown_members_do_not_reach_gemini() {
    let out = chat_to_gemini(
        br#"{"model":"m","future_root":{"a":1},"messages":[{"role":"user","content":[{"type":"text","text":"hi","future_part":2}],"future_message":3}]}"#,
    );
    let text = out.to_string();
    for key in ["future_root", "future_message", "future_part"] {
        assert!(!text.contains(key), "{key} reached Gemini: {text}");
    }
    assert_eq!(out["contents"][0]["parts"][0]["text"], "hi");
}

fn validate(input: &Value) -> codec::Result<()> {
    let mut mapped = Mapped::new();
    let bytes = serde_json::to_vec(input).unwrap();
    let mut source = codec::source::Source::new(&mut mapped.writer).unwrap();
    source.push_bytes(&mut mapped.writer, &bytes).unwrap();
    let document = codec::document::Document::parse(&mut mapped.writer, source, bytes.len() as u64).unwrap();
    let media = codec::media::Registry::new(&mut mapped.writer).unwrap();
    let view = codec::RequestView {
        document: &document,
        media: &media,
        operation: Operation::Generate,
    };
    codec::RequestFence::default()
        .end(view, &gemini::request::GeminiCapability)
        .map(|_| ())
}

/// Another protocol's opaque history and signatures cannot be replayed to Gemini; Gemini's own can.
#[test]
fn foreign_opaque_signatures_fail_the_gemini_fence() {
    let request = |part: Value, replay: Value| json!({"model":"m","messages":[{"role":"assistant","content":[part]}],"x_ck":{"replay":[replay]}});
    let text = json!({"type":"text","text":"a"});
    let gemini = request(text.clone(), json!({"kind":"gemini","signature":"sig"}));
    assert!(validate(&gemini).is_ok());
    let foreign_replay = request(text, json!({"kind":"responses_encrypted","signature":"opaque"}));
    assert!(matches!(validate(&foreign_replay), Err(codec::Error::Unsupported)));
    let foreign_part = request(
        json!({"type":"opaque","protocol":"anthropic","data":"opaque"}),
        json!({"kind":"gemini","signature":"sig"}),
    );
    assert!(matches!(validate(&foreign_part), Err(codec::Error::Unsupported)));
}
