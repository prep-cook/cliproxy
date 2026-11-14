// Test-only inclusion until the framework owner declares the production module.
// The Gemini codec leaves some shared helpers unused.
#[allow(dead_code)]
#[path = "../src/boundary.rs"]
pub mod boundary;
#[path = "../src/codec/mod.rs"]
pub mod codec;
#[path = "gemini/finish.rs"]
mod finish;
#[path = "../src/gemini/boundary.rs"]
pub mod gemini;
#[path = "gemini/incremental.rs"]
mod incremental;
#[path = "semantic/mod.rs"]
mod semantic;
#[path = "gemini/streams.rs"]
mod streams;
#[path = "gemini/usage.rs"]
mod usage;

use codec::{
    BodyMode, Operation, RequestEgress, RequestIngress, ResponseEgress, ResponseIngress,
    event::{Direction, Identity, LocalId},
};
use semantic::Mapped;
use serde_json::{Value, json};
use std::task::Poll;

fn identity(direction: Direction) -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction,
    }
}
fn bytes(lease: &ck_stream::ScratchLease) -> Vec<u8> {
    let mut out = Vec::new();
    while out.len() < lease.len() {
        out.extend_from_slice(lease.chunk(out.len()).unwrap());
    }
    out
}
fn events(raw: &[u8]) -> Vec<Value> {
    std::str::from_utf8(raw)
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
fn request(input: &Value, operation: Operation) -> (Value, Vec<Value>) {
    let mut mapped = Mapped::new();
    let mut ingress = gemini::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        operation,
        "gemini-test",
        1024 * 1024,
    )
    .unwrap();
    for chunk in serde_json::to_vec(input).unwrap().chunks(7) {
        ingress.advance(&mut mapped.writer, chunk).unwrap();
    }
    let ready = ingress
        .finish(&mut mapped.writer, &gemini::request::GeminiCapability)
        .unwrap();
    let output = gemini::request::Egress {
        destination: "gemini",
        upstream_stream: operation == Operation::Generate,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    (
        serde_json::from_slice(&bytes(&output)).unwrap(),
        events(&mapped.output(&mapped.writer)),
    )
}
fn response(
    input: &[u8],
    upstream: gemini::response::Framing,
    downstream: gemini::response::Framing,
    operation: Operation,
) -> (Vec<u8>, Vec<Value>) {
    let mut mapped = Mapped::new();
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let mut ingress = gemini::response::Ingress::with_framing(
        &mut mapped.writer,
        identity(Direction::Response),
        operation,
        upstream,
        1024 * 1024,
    )
    .unwrap();
    let mut egress = gemini::egress::Egress::with_framing(&mut mapped.writer, operation, downstream, |_| {}).unwrap();
    let mut output = Vec::new();
    for chunk in input.chunks(7) {
        ingress.advance(&mut mapped.writer, chunk).unwrap();
        while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
            egress
                .advance(&mut mapped.writer, lease, |part| {
                    output.extend_from_slice(&bytes(&part));
                    Ok(())
                })
                .unwrap();
        }
    }
    ingress.finish(&mut mapped.writer).unwrap();
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        egress
            .advance(&mut mapped.writer, lease, |part| {
                output.extend_from_slice(&bytes(&part));
                Ok(())
            })
            .unwrap();
    }
    egress.finish().unwrap();
    (output, events(&mapped.output(&mapped.writer)))
}
fn sse(rows: &[Value]) -> Vec<u8> {
    rows.iter()
        .map(|v| format!("data: {v}\n\n"))
        .collect::<String>()
        .into_bytes()
}
#[test]
fn history_signatures_tools_sampling_modalities_and_unknowns() {
    let input = json!({"contents":[{"role":"model","parts":[{"text":"reason","thought":true,"thoughtSignature":"sig/\n==","extra":{"a":[null,0]}},{"thoughtSignature":"only=="},{"functionCall":{"id":"a","name":"lookup","args":{"q":"value"}}},{"text":"tail"}]},{"role":"user","parts":[{"functionResponse":{"id":"a","name":"lookup","response":{"ok":true}}}]}],"generationConfig":{"temperature":0.1,"topP":0.9,"responseModalities":["TEXT","AUDIO"],"thinkingConfig":{"thinkingBudget":32,"includeThoughts":true},"topK":8},"cachedContent":"cached/x","vendor":{"x_ck":{"attempt":9}},"x_ck":{"attempt":999}});
    let (out, canonical) = request(&input, Operation::Generate);
    assert_eq!(out, input);
    assert!(canonical.iter().all(|v| v["x_ck"]["attempt"] == 1));
    assert!(canonical.iter().any(|v| v["pointer"] == "/top_p" && v["value"] == 0.9));
    assert!(
        canonical
            .iter()
            .any(|v| v["pointer"] == "/messages/0/tool_calls/0/function/arguments/q" && v["value"] == "value")
    );
}
#[test]
fn every_part_signature_choice_and_finish_survives_all_encodings() {
    use gemini::response::Framing::*;
    let body = json!({"responseId":"r","modelVersion":"gemini","createTime":"2026-11-14T00:00:00.123Z","candidates":[{"index":7,"content":{"role":"model","parts":[{"text":"first"},{"text":"second"},{"text":"think","thought":true,"thoughtSignature":"sig\n/=="},{"thoughtSignature":"only=="},{"functionCall":{"id":"call","name":"f","args":{"a":[1,null]}}},{"executableCode":{"language":"PYTHON","code":"print(1)"}},{"codeExecutionResult":{"outcome":"OUTCOME_OK","output":"1"}}]},"groundingMetadata":{"chunks":[{"x":1}]},"urlContextMetadata":{"urls":["x"]},"safetyRatings":[],"finishReason":"STOP"},{"index":0,"content":{"role":"model","parts":[{"text":"zero"}]},"finishReason":"MAX_TOKENS"}],"usageMetadata":{"promptTokenCount":0,"candidatesTokenCount":4,"thoughtsTokenCount":2,"totalTokenCount":6,"cachedContentTokenCount":null,"future":{"ttl":{"300":4}}},"unknown":{"x_ck":true}});
    for upstream in [Json, Sse, JsonArray] {
        for downstream in [Json, Sse, JsonArray] {
            let input = match upstream {
                Json => serde_json::to_vec(&body).unwrap(),
                Sse => sse(std::slice::from_ref(&body)),
                JsonArray => serde_json::to_vec(&json!([body])).unwrap(),
            };
            let (output, canonical) = response(&input, upstream, downstream, Operation::Generate);
            if downstream == Json {
                let raw = String::from_utf8(output.clone()).unwrap();
                assert_eq!(raw.matches("\"responseId\"").count(), 1, "{raw}");
            }
            let rows = match downstream {
                Json => vec![serde_json::from_slice::<Value>(&output).unwrap()],
                Sse => events(&output),
                JsonArray => serde_json::from_slice::<Vec<Value>>(&output).unwrap(),
            };
            let parts: Vec<_> = rows
                .iter()
                .flat_map(|v| v["candidates"].as_array().into_iter().flatten())
                .filter(|v| v["index"] == 7)
                .flat_map(|v| v["content"]["parts"].as_array().into_iter().flatten())
                .cloned()
                .collect();
            assert_eq!(
                parts,
                body["candidates"][0]["content"]["parts"].as_array().unwrap().clone()
            );
            assert!(rows.iter().any(|v| {
                v["candidates"]
                    .as_array()
                    .is_some_and(|c| c.iter().any(|v| v["index"] == 0 && v["finishReason"] == "MAX_TOKENS"))
            }));
            assert_eq!(
                canonical
                    .iter()
                    .filter(|v| v.get("fragment").is_some())
                    .map(|v| v["fragment"].clone())
                    .collect::<Vec<_>>(),
                vec![json!("sig\n/=="), json!("only==")]
            );
            let usage = canonical.iter().rev().find_map(|v| v.get("usage")).unwrap();
            assert_eq!(usage["input_tokens"], 0);
            assert_eq!(usage["output_tokens"], 6);
            assert!(usage.get("cache_read_tokens").is_none());
            assert_eq!(usage["native"]["value"]["future"]["ttl"]["300"], 4);
        }
    }
}
#[test]
fn count_is_not_usage_and_compact_is_explicitly_unsupported() {
    let input = json!({"generateContentRequest":{"contents":[{"parts":[{"text":"count"}]}],"generationConfig":{"temperature":0.2}},"extra":false});
    let (out, _) = request(&input, Operation::CountTokens);
    assert_eq!(
        out["generateContentRequest"]["contents"][0]["parts"],
        input["generateContentRequest"]["contents"][0]["parts"]
    );
    assert_eq!(out["extra"], false);
    let body = json!({"totalTokens":12,"cachedContentTokenCount":0,"promptTokensDetails":[{"modality":"AUDIO","tokenCount":5}]});
    let (out, canonical) = response(
        &serde_json::to_vec(&body).unwrap(),
        gemini::response::Framing::Json,
        gemini::response::Framing::Json,
        Operation::CountTokens,
    );
    assert_eq!(serde_json::from_slice::<Value>(&out).unwrap(), body);
    assert!(canonical.iter().all(|v| v.get("usage").is_none()));
    let mut mapped = Mapped::new();
    assert!(matches!(
        gemini::response::Ingress::new(
            &mut mapped.writer,
            identity(Direction::Response),
            Operation::Compact,
            BodyMode::Buffered,
            100
        ),
        Err(codec::Error::Unsupported)
    ));
}
#[test]
fn truncated_or_invalid_response_never_succeeds() {
    for input in [
        b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"prefix\"}]}}]}\n\n".as_slice(),
        b"data: {\"x\":1,\"x\":2}\n\n",
        b"data: {\"x\":\"\xff\"}\n\n",
        b"data: {}",
    ] {
        let mut mapped = Mapped::new();
        let mut ingress = gemini::response::Ingress::new(
            &mut mapped.writer,
            identity(Direction::Response),
            Operation::Generate,
            BodyMode::Streaming,
            1024,
        )
        .unwrap();
        let result = ingress
            .advance(&mut mapped.writer, input)
            .and_then(|()| ingress.finish(&mut mapped.writer));
        assert!(result.is_err());
        let raw = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
        assert!(raw.contains("event: x_ck.error"));
        assert!(!raw.contains("event: x_ck.done"));
    }
}
#[test]
fn destination_rejection_precedes_request_end() {
    struct Reject;
    impl codec::Capability for Reject {
        fn destination(&self) -> &'static str {
            "reject"
        }
        fn validate(&self, _: codec::RequestView<'_>) -> codec::Result<()> {
            Err(codec::Error::Unsupported)
        }
    }
    let mut mapped = Mapped::new();
    let mut ingress = gemini::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        "m",
        1024,
    )
    .unwrap();
    ingress
        .advance(
            &mut mapped.writer,
            br#"{"contents":[{"parts":[{"text":"ok"}]}],"generationConfig":{"responseModalities":["AUDIO"]}}"#,
        )
        .unwrap();
    assert!(ingress.finish(&mut mapped.writer, &Reject).is_err());
    let raw = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert!(raw.contains("event: x_ck.error"));
    assert!(!raw.contains("event: x_ck.request.end"));
}
