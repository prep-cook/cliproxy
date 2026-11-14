//! Gemini -> canonical -> Gemini round trips for the spec rows marked covered (5.9, 5.10),
//! buffered and streamed.
use super::*;
use gemini::response::Framing;

/// One request that touches every 5.9 row.
fn full_request() -> Value {
    json!({
        "model":"models/gemini-test",
        "systemInstruction":{"role":"system","parts":[{"text":"be brief"}]},
        "contents":[
            {"role":"user","parts":[
                {"text":"look"},
                {"inlineData":{"mimeType":"audio/wav","data":"AAECAwQF"}},
                {"fileData":{"mimeType":"video/mp4","fileUri":"gs://b/v.mp4"},"videoMetadata":{"startOffset":"1.5s","endOffset":"2s"}}
            ]},
            {"role":"model","parts":[
                {"text":"plan","thought":true,"thoughtSignature":"c2ln"},
                {"thoughtSignature":"b25seQ=="},
                {"functionCall":{"id":"call_1","name":"lookup","args":{"q":"v","n":[1,null]}}},
                {"text":"tail"}
            ]},
            {"role":"user","parts":[{"functionResponse":{"id":"call_1","name":"lookup","response":{"ok":true}}}]}
        ],
        "generationConfig":{
            "temperature":0.2,"topP":0.9,"topK":8,"candidateCount":1,"maxOutputTokens":64,
            "stopSequences":["END"],"seed":7,"presencePenalty":0.1,
            "responseMimeType":"application/json","responseSchema":{"type":"OBJECT","properties":{"a":{"type":"STRING"}}},
            "responseModalities":["TEXT","AUDIO"],
            "thinkingConfig":{"thinkingBudget":32,"includeThoughts":true}
        },
        "cachedContent":"cachedContents/c1",
        "tools":[
            {"functionDeclarations":[{"name":"lookup","description":"d","parameters":{"type":"OBJECT","properties":{"q":{"type":"STRING"}}}}]},
            {"googleSearch":{}}
        ],
        "toolConfig":{"functionCallingConfig":{"mode":"ANY","allowedFunctionNames":["lookup"]}},
        "safetySettings":[{"category":"HARM_CATEGORY_HARASSMENT","threshold":"BLOCK_NONE"}],
        "labels":{"team":"a"}
    })
}

fn request_round_trip(input: &Value, chunk: usize, upstream_stream: bool) -> Value {
    let mut mapped = Mapped::new();
    let mut ingress = gemini::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        "gemini-test",
        1024 * 1024,
    )
    .unwrap();
    for piece in serde_json::to_vec(input).unwrap().chunks(chunk) {
        ingress.advance(&mut mapped.writer, piece).unwrap();
    }
    let ready = ingress
        .finish(&mut mapped.writer, &gemini::request::GeminiCapability)
        .unwrap();
    let output = gemini::request::Egress {
        destination: "gemini",
        upstream_stream,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    serde_json::from_slice(&bytes(&output)).unwrap()
}

/// Whole and fragmented bodies, for generateContent and streamGenerateContent.
#[test]
fn every_covered_request_row_round_trips() {
    let input = full_request();
    for chunk in [usize::MAX, 5] {
        for upstream_stream in [false, true] {
            assert_eq!(
                request_round_trip(&input, chunk, upstream_stream),
                input,
                "chunk {chunk}, stream {upstream_stream}"
            );
        }
    }
}

/// One response that touches every 5.10 row.
fn full_response() -> Value {
    let ratings = json!([{"category":"HARM_CATEGORY_HARASSMENT","probability":"NEGLIGIBLE"}]);
    json!({
        "responseId":"resp_1","modelVersion":"gemini-test-001","createTime":"2026-11-14T00:00:00.123456Z",
        "candidates":[{
            "index":1,
            "content":{"role":"model","parts":[
                {"text":"first"},
                {"text":"second"},
                {"text":"think","thought":true,"thoughtSignature":"c2ln"},
                {"thoughtSignature":"b25seQ=="},
                {"functionCall":{"id":"call_1","name":"f","args":{"a":[1,null]}}},
                {"functionResponse":{"id":"call_1","name":"f","response":{"ok":true}}},
                {"inlineData":{"mimeType":"image/png","data":"AAECAw=="}},
                {"fileData":{"mimeType":"video/mp4","fileUri":"gs://b/v.mp4"},"videoMetadata":{"startOffset":"1.5s","endOffset":"2s"}},
                {"executableCode":{"language":"PYTHON","code":"print(1)"}},
                {"codeExecutionResult":{"outcome":"OUTCOME_OK","output":"1"}}
            ]},
            "groundingMetadata":{"groundingChunks":[{"web":{"uri":"https://e.example","title":"e"}}]},
            "citationMetadata":{"citationSources":[{"startIndex":0,"endIndex":5,"uri":"https://e.example"}]},
            "urlContextMetadata":{"urlMetadata":[{"retrievedUrl":"https://e.example","urlRetrievalStatus":"URL_RETRIEVAL_STATUS_SUCCESS"}]},
            "safetyRatings":ratings,
            "finishReason":"STOP",
            "finishMessage":"done"
        }],
        "promptFeedback":{"safetyRatings":ratings},
        "usageMetadata":{
            "promptTokenCount":5,"candidatesTokenCount":4,"thoughtsTokenCount":2,"cachedContentTokenCount":0,"totalTokenCount":11,
            "promptTokensDetails":[{"modality":"TEXT","tokenCount":3},{"modality":"AUDIO","tokenCount":2}],
            "candidatesTokensDetails":[{"modality":"TEXT","tokenCount":4}]
        }
    })
}

fn framed(rows: &[Value], framing: Framing) -> Vec<u8> {
    match framing {
        Framing::Json => serde_json::to_vec(&rows[0]).unwrap(),
        Framing::Sse => sse(rows),
        Framing::JsonArray => serde_json::to_vec(rows).unwrap(),
    }
}

/// What a Gemini client rebuilds from streamed chunks: parts append per candidate index,
/// every other member takes its latest value.
fn merged(rows: &[Value]) -> Value {
    let mut out = serde_json::Map::new();
    let mut candidates: Vec<Value> = Vec::new();
    for row in rows {
        for (key, value) in row.as_object().unwrap() {
            if key != "candidates" {
                out.insert(key.clone(), value.clone());
            }
        }
        for candidate in row["candidates"].as_array().into_iter().flatten() {
            let at = match candidates.iter().position(|c| c["index"] == candidate["index"]) {
                Some(at) => at,
                None => {
                    candidates.push(json!({}));
                    candidates.len() - 1
                }
            };
            let target = &mut candidates[at];
            for (key, value) in candidate.as_object().unwrap() {
                if key != "content" {
                    target[key] = value.clone();
                    continue;
                }
                if let Some(role) = value.get("role") {
                    target["content"]["role"] = role.clone();
                }
                let parts = value["parts"].as_array().cloned().unwrap_or_default();
                match target["content"]["parts"].as_array_mut() {
                    Some(list) => list.extend(parts),
                    None => target["content"]["parts"] = Value::Array(parts),
                }
            }
        }
    }
    if !candidates.is_empty() {
        out.insert("candidates".into(), Value::Array(candidates));
    }
    Value::Object(out)
}

/// A buffered body returns unchanged, whatever framing carried it upstream.
#[test]
fn every_covered_response_row_round_trips_buffered() {
    let body = full_response();
    for (name, upstream) in [
        ("json", Framing::Json),
        ("sse", Framing::Sse),
        ("array", Framing::JsonArray),
    ] {
        let input = framed(std::slice::from_ref(&body), upstream);
        let (out, _) = response(&input, upstream, Framing::Json, Operation::Generate);
        assert_eq!(serde_json::from_slice::<Value>(&out).unwrap(), body, "{name}");
    }
}

/// The same response streamed as one part per chunk, metadata on the chunks a Gemini stream
/// puts it on.
fn streamed_rows(body: &Value) -> Vec<Value> {
    let candidate = &body["candidates"][0];
    let parts = candidate["content"]["parts"].as_array().unwrap();
    let mut rows: Vec<Value> = parts
        .iter()
        .map(|part| {
            json!({"candidates":[{"index":1,"content":{"role":"model","parts":[part]}}],
                   "responseId":body["responseId"],"modelVersion":body["modelVersion"],"createTime":body["createTime"]})
        })
        .collect();
    rows[0]["promptFeedback"] = body["promptFeedback"].clone();
    let last = rows.last_mut().unwrap();
    for key in [
        "groundingMetadata",
        "citationMetadata",
        "urlContextMetadata",
        "safetyRatings",
        "finishReason",
        "finishMessage",
    ] {
        last["candidates"][0][key] = candidate[key].clone();
    }
    last["usageMetadata"] = body["usageMetadata"].clone();
    rows
}

/// A stream aggregates to the buffered body, and streams back to chunks a Gemini client merges
/// into the same body, each carrying the response identity.
#[test]
fn every_covered_response_row_round_trips_streamed() {
    let body = full_response();
    let rows = streamed_rows(&body);
    assert_eq!(merged(&rows), body);
    for (name, upstream) in [("sse", Framing::Sse), ("array", Framing::JsonArray)] {
        let input = framed(&rows, upstream);
        let (out, _) = response(&input, upstream, Framing::Json, Operation::Generate);
        assert_eq!(serde_json::from_slice::<Value>(&out).unwrap(), body, "{name} to json");
        for (downstream, framing) in [("sse", Framing::Sse), ("array", Framing::JsonArray)] {
            let (out, _) = response(&input, upstream, framing, Operation::Generate);
            let out = match framing {
                Framing::Sse => events(&out),
                _ => serde_json::from_slice::<Vec<Value>>(&out).unwrap(),
            };
            assert_eq!(merged(&out), body, "{name} to {downstream}");
            for chunk in &out {
                assert_eq!(chunk["responseId"], body["responseId"], "{chunk}");
                assert_eq!(chunk["modelVersion"], body["modelVersion"], "{chunk}");
            }
        }
    }
}
