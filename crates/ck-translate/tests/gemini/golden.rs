//! Recorded Go fixtures: native Gemini bodies through the codec and back to Gemini.
use super::*;
use base64::Engine;
use gemini::response::Framing;

/// Files whose `non_stream` and `token_count` inputs are native Gemini response bodies.
const RESPONSE_FILES: &[(&str, &str)] = &[
    (
        "claude-gemini.json",
        include_str!("../fixtures/pairs/claude-gemini.json"),
    ),
    (
        "openai-gemini.json",
        include_str!("../fixtures/pairs/openai-gemini.json"),
    ),
    (
        "openai-response-gemini.json",
        include_str!("../fixtures/pairs/openai-response-gemini.json"),
    ),
    (
        "interactions-gemini.json",
        include_str!("../fixtures/pairs/interactions-gemini.json"),
    ),
    (
        "gemini-gemini.json",
        include_str!("../fixtures/pairs/gemini-gemini.json"),
    ),
];
/// Files whose `request` inputs are native Gemini request bodies.
const REQUEST_FILES: &[(&str, &str)] = &[
    (
        "gemini-antigravity.json",
        include_str!("../fixtures/pairs/gemini-antigravity.json"),
    ),
    (
        "gemini-claude.json",
        include_str!("../fixtures/pairs/gemini-claude.json"),
    ),
    ("gemini-codex.json", include_str!("../fixtures/pairs/gemini-codex.json")),
    (
        "gemini-gemini.json",
        include_str!("../fixtures/pairs/gemini-gemini.json"),
    ),
    (
        "gemini-interactions.json",
        include_str!("../fixtures/pairs/gemini-interactions.json"),
    ),
    (
        "gemini-openai.json",
        include_str!("../fixtures/pairs/gemini-openai.json"),
    ),
];

fn fixtures<'a>(files: &'a [(&str, &str)], path: &'a str) -> impl Iterator<Item = (String, Value)> + 'a {
    files.iter().flat_map(move |&(file, raw)| {
        let all: Value = serde_json::from_str(raw).unwrap();
        all["fixtures"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .filter(|(_, f)| f["path"] == path)
            .map(|(n, f)| (format!("{file}:{}:{n}", f["name"]), f.clone()))
            .collect::<Vec<_>>()
    })
}
fn parts(body: &Value) -> impl Iterator<Item = &Value> {
    body["candidates"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|c| c["content"]["parts"].as_array().into_iter().flatten())
}
/// Whether a recorded body is one the codec must refuse: no response members, an error body,
/// undecodable inline data or a call without a string name.
fn refused(native: Option<&Value>) -> bool {
    let Some(v) = native else { return true };
    let response = ["candidates", "usageMetadata", "promptFeedback"]
        .iter()
        .any(|k| v.get(k).is_some());
    let bad_media = parts(v).any(|p| {
        p.get("inlineData").is_some_and(|d| {
            d["data"]
                .as_str()
                .is_none_or(|s| base64::engine::general_purpose::STANDARD.decode(s).is_err())
        })
    });
    let bad_call = parts(v).any(|p| p.get("functionCall").is_some_and(|f| !f["name"].is_string()));
    // A generation succeeds only when every candidate finishes or the prompt is blocked.
    let finished = v["candidates"]
        .as_array()
        .is_some_and(|c| !c.is_empty() && c.iter().all(|c| c.get("finishReason").is_some()))
        || v["promptFeedback"].get("blockReason").is_some();
    !response || !finished || v.get("error").is_some() || bad_media || bad_call
}
fn egress_json(mapped: &mut Mapped, reader: &mut ck_stream::Reader, id: &str) -> Value {
    let mut egress =
        gemini::egress::Egress::with_framing(&mut mapped.writer, Operation::Generate, Framing::Json, |_| {}).unwrap();
    let mut out = Vec::new();
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        egress
            .advance(&mut mapped.writer, lease, |part| {
                out.extend_from_slice(&bytes(&part));
                Ok(())
            })
            .unwrap_or_else(|error| panic!("{id}: {error}"));
    }
    egress.finish().unwrap();
    serde_json::from_slice(&out).unwrap()
}

#[test]
fn buffered_native_recordings_keep_parts_signatures_usage_and_unknowns() {
    let mut checked = 0;
    for (id, fixture) in fixtures(RESPONSE_FILES, "non_stream") {
        let input = fixture["input"].as_str().unwrap_or("");
        let native = serde_json::from_str::<Value>(input).ok();
        let mut mapped = Mapped::new();
        let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
        let mut ingress = gemini::response::Ingress::new(
            &mut mapped.writer,
            identity(Direction::Response),
            Operation::Generate,
            BodyMode::Buffered,
            1024 * 1024,
        )
        .unwrap();
        let result = ingress
            .advance(&mut mapped.writer, input.as_bytes())
            .and_then(|()| ingress.finish(&mut mapped.writer));
        // Two recordings repeat a member name, which the JSON scanner refuses.
        let duplicate = [
            "TestGeminiApplyPatchRejectInvalidCompleteArguments:literal#3",
            "TestGeminiApplyPatchRejectInvalidCompleteArguments:49#3",
        ]
        .contains(&fixture["name"].as_str().unwrap_or(""));
        if duplicate || refused(native.as_ref()) {
            assert!(result.is_err(), "{id}");
            let raw = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
            assert!(!raw.contains("event: x_ck.done"), "{id}");
            continue;
        }
        result.unwrap_or_else(|error| panic!("{id}: {error}"));
        let out = egress_json(&mut mapped, &mut reader, &id);
        let native = native.unwrap();
        for key in [
            "responseId",
            "modelVersion",
            "createTime",
            "usageMetadata",
            "promptFeedback",
        ] {
            if let Some(value) = native.get(key) {
                assert_eq!(&out[key], value, "{id}: {key}");
            }
        }
        for (ordinal, candidate) in native["candidates"].as_array().into_iter().flatten().enumerate() {
            let index = candidate["index"].as_u64().unwrap_or(ordinal as u64);
            let output = out["candidates"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["index"].as_u64().unwrap_or(0) == index)
                .unwrap_or_else(|| panic!("{id}: candidate {index}"));
            for (key, value) in candidate.as_object().unwrap() {
                // The egress always writes the model role, which a few recordings omit.
                if key == "content" {
                    assert_eq!(output[key]["parts"], value["parts"], "{id}: parts");
                } else if key != "index" {
                    assert_eq!(&output[key], value, "{id}: candidate {key}");
                }
            }
        }
        checked += 1;
    }
    assert!(checked > 100, "{checked}");
}

/// Recordings with tools, a system instruction or non-text parts belong to other rows.
fn text_only(contents: &[Value]) -> bool {
    contents.iter().all(|m| {
        matches!(m.get("role").and_then(Value::as_str), None | Some("user" | "model"))
            && m["parts"].as_array().is_some_and(|p| {
                p.iter().all(|p| {
                    p["text"].is_string()
                        && !["functionCall", "inlineData", "fileData"]
                            .iter()
                            .any(|k| p.get(*k).is_some())
                })
            })
    })
}

/// Whether the request fence must refuse a recorded text request's controls or thoughts.
fn fence_refuses(input: &Value) -> bool {
    let config = &input["generationConfig"];
    let contents = input["contents"].as_array().unwrap();
    !(config.is_null() || config.is_object())
        || ["maxOutputTokens", "candidateCount", "seed"]
            .iter()
            .any(|k| config.get(k).is_some_and(|v| v.as_u64().is_none()))
        || config.get("thinkingConfig").is_some_and(|v| !v.is_object())
        || config.get("responseModalities").is_some_and(|v| {
            !v.as_array().is_some_and(|a| {
                a.iter()
                    .all(|m| m.as_str().is_some_and(|m| ["TEXT", "IMAGE", "AUDIO"].contains(&m)))
            })
        })
        || ["temperature", "topP", "topK"]
            .iter()
            .any(|k| config.get(k).is_some_and(|v| !v.is_number()))
        || config
            .get("stopSequences")
            .is_some_and(|v| !v.as_array().is_some_and(|a| a.iter().all(Value::is_string)))
        || contents.iter().any(|m| {
            m["parts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|p| p.get("thought").is_some_and(|v| !v.is_boolean()))
        })
}

#[test]
fn recorded_text_requests_keep_native_controls_and_unknown_fields() {
    let mut checked = 0;
    for (id, fixture) in fixtures(REQUEST_FILES, "request") {
        let raw = fixture["input"].as_str().unwrap_or("");
        let Ok(input) = serde_json::from_str::<Value>(raw) else {
            continue;
        };
        let Some(contents) = input["contents"].as_array() else {
            continue;
        };
        if input.get("tools").is_some() || input.get("systemInstruction").is_some() || !text_only(contents) {
            continue;
        }
        let mut mapped = Mapped::new();
        let mut ingress = gemini::request::Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Request),
            Operation::Generate,
            fixture["model"].as_str().filter(|m| !m.is_empty()).unwrap_or("m"),
            1024 * 1024,
        )
        .unwrap_or_else(|e| panic!("{id}: {e} {}", fixture["model"]));
        ingress
            .advance(&mut mapped.writer, raw.as_bytes())
            .unwrap_or_else(|e| panic!("{id}: {e}"));
        let invalid = fence_refuses(&input);
        let ready = ingress.finish(&mut mapped.writer, &gemini::request::GeminiCapability);
        if invalid {
            assert!(ready.is_err(), "{id}");
            let raw = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
            assert!(!raw.contains("event: x_ck.request.end"), "{id}");
            continue;
        }
        let ready = ready.unwrap_or_else(|e| panic!("{id}: {e}"));
        let output = gemini::request::Egress {
            destination: "gemini",
            upstream_stream: true,
        }
        .encode(&ready, &mut mapped.writer)
        .unwrap();
        let output: Value = serde_json::from_slice(&bytes(&output)).unwrap();
        for (index, message) in contents.iter().enumerate() {
            assert_eq!(output["contents"][index]["parts"], message["parts"], "{id}: parts");
        }
        for key in [
            "generationConfig",
            "cachedContent",
            "safetySettings",
            "labels",
            "toolConfig",
        ] {
            if let Some(value) = input.get(key) {
                assert_eq!(&output[key], value, "{id}: {key}");
            }
        }
        checked += 1;
    }
    assert!(checked > 50, "{checked}");
}
