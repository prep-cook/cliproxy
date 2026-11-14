use super::{Mapped, Operation, codec, identity, response_fragmented};
use codec::{BodyMode, Error, ResponseIngress, event::Direction};
use serde_json::{Value, json};

fn ingest(wire: &[u8]) -> (codec::Result<()>, Vec<Value>) {
    let mut mapped = Mapped::new();
    let mut ingress = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        1024 * 1024,
    )
    .unwrap();
    let result = ingress
        .advance(&mut mapped.writer, wire)
        .and_then(|()| ingress.finish(&mut mapped.writer));
    let output = mapped.output(&mapped.writer);
    let events = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    (result, events)
}

fn audio_stream(deltas: &[Value]) -> Vec<u8> {
    let mut wire = String::new();
    for (position, audio) in deltas.iter().enumerate() {
        let finish = if position + 1 == deltas.len() {
            json!("stop")
        } else {
            Value::Null
        };
        let event = json!({"id":"r","choices":[{"index":0,"delta":{"audio":audio},"finish_reason":finish}]});
        wire.push_str(&format!("data: {event}\n\n"));
    }
    wire.push_str("data: [DONE]\n\n");
    wire.into_bytes()
}

fn transcripts(events: &[Value]) -> Vec<(String, bool)> {
    events
        .iter()
        .filter(|e| e["text"].is_string() && e["final"].is_boolean())
        .map(|e| (e["text"].as_str().unwrap().to_owned(), e["final"].as_bool().unwrap()))
        .collect()
}

#[test]
fn audio_identity_contradiction_fails_even_when_the_stream_completes() {
    let wire = audio_stream(&[
        json!({"id":"au","format":"wav","data":"AAEC"}),
        json!({"id":"other","data":"AwQF"}),
    ]);
    let (result, events) = ingest(&wire);
    assert!(matches!(result, Err(Error::Schema)), "{result:?}");
    assert!(events.iter().all(|e| e.get("status") != Some(&json!("completed"))));
}

#[test]
fn audio_identity_survives_fragments_that_omit_it() {
    for (id, accepted) in [("au", true), ("other", false)] {
        let wire = audio_stream(&[
            json!({"id":"au","format":"wav","data":"AAEC"}),
            json!({"expires_at":5,"data":"AwQF"}),
            json!({"id":id}),
        ]);
        let (result, _) = ingest(&wire);
        assert_eq!(result.is_ok(), accepted, "{id}: {result:?}");
    }
    let wire = audio_stream(&[
        json!({"id":"au","format":"wav","data":"AAEC"}),
        json!({"expires_at":5,"data":"AwQF"}),
    ]);
    let (output, _) = response_fragmented(&wire, BodyMode::Streaming, BodyMode::Buffered, 512);
    let output: Value = serde_json::from_slice(&output).unwrap();
    let audio = &output["choices"][0]["message"]["audio"];
    assert_eq!((&audio["id"], &audio["expires_at"]), (&json!("au"), &json!(5)));
}

#[test]
fn audio_transcript_fragments_close_with_one_final_empty_fragment() {
    let wire = audio_stream(&[
        json!({"id":"au","format":"wav","data":"AAEC","transcript":"hi"}),
        json!({"data":"AwQF","transcript":" there"}),
    ]);
    let (result, events) = ingest(&wire);
    result.unwrap();
    let expected = [("hi", false), (" there", false), ("", true)].map(|(text, last)| (text.to_owned(), last));
    assert_eq!(transcripts(&events), expected);

    let wire = audio_stream(&[json!({"id":"au","format":"wav","data":"AAEC","transcript":null})]);
    let (result, events) = ingest(&wire);
    result.unwrap();
    assert!(transcripts(&events).is_empty());
    assert!(
        events
            .iter()
            .any(|e| e["choices"][0]["delta"]["audio"] == json!({"transcript":null}))
    );
}

fn long(c: char) -> String {
    std::iter::repeat_n(c, 1500).collect()
}

/// A data URL long enough to span several 512-byte input fragments.
fn image(quad: &str) -> Value {
    json!({"type":"image_url","image_url":{"url":format!("data:image/png;base64,{}", quad.repeat(400))}})
}

fn media_body() -> Value {
    json!({"id":"r","choices":[{"index":0,"message":{
        "role":"assistant",
        "content":long('c'),
        "reasoning_content":long('t'),
        "refusal":long('f'),
        "images":[image("AAAA"),image("AQEB")]
    },"finish_reason":"stop"}]})
}

#[test]
fn buffered_media_blocks_get_their_kinds_and_successive_indexes() {
    let body = media_body().to_string().into_bytes();
    let (_, events) = response_fragmented(&body, BodyMode::Buffered, BodyMode::Buffered, 512);
    let blocks: Vec<_> = events
        .iter()
        .filter(|e| e["kind"].is_string() && e["x_ck"]["block_index"].is_u64() && e.get("encoding").is_none())
        .map(|e| (e["x_ck"]["block_index"].as_u64().unwrap(), e["kind"].as_str().unwrap()))
        .collect();
    assert_eq!(
        blocks,
        [
            (0, "text"),
            (1, "thinking"),
            (2, "refusal"),
            (3, "content"),
            (4, "content")
        ]
    );
}

#[test]
fn buffered_media_fields_return_to_their_own_native_members() {
    let input = media_body();
    let body = input.to_string().into_bytes();
    for mode in [BodyMode::Buffered, BodyMode::Streaming] {
        let (output, _) = response_fragmented(&body, BodyMode::Buffered, mode, 512);
        let message = if mode == BodyMode::Buffered {
            serde_json::from_slice::<Value>(&output).unwrap()["choices"][0]["message"].clone()
        } else {
            let mut message = json!({"content":"","reasoning_content":"","refusal":""});
            for line in std::str::from_utf8(&output).unwrap().lines() {
                let Some(Ok(event)) = line.strip_prefix("data: ").map(serde_json::from_str::<Value>) else {
                    continue;
                };
                for field in ["content", "reasoning_content", "refusal"] {
                    if let Some(text) = event["choices"][0]["delta"][field].as_str() {
                        let joined = message[field].as_str().unwrap().to_owned() + text;
                        message[field] = json!(joined);
                    }
                }
            }
            message
        };
        for field in ["content", "reasoning_content", "refusal"] {
            assert_eq!(
                message[field], input["choices"][0]["message"][field],
                "{mode:?} {field}"
            );
        }
    }
}

#[test]
fn only_root_choices_messages_are_published_as_media() {
    let body = json!({"id":"r","alt":[{"index":0,"message":{"content":long('x')}}],"choices":[{"index":0,"message":{"role":"assistant","content":"short"},"finish_reason":"stop"}]});
    let (output, events) =
        response_fragmented(body.to_string().as_bytes(), BodyMode::Buffered, BodyMode::Buffered, 512);
    let output: Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(output["alt"], body["alt"]);
    assert_eq!(output["choices"][0]["message"]["content"], "short");
    assert!(events.iter().all(|e| e["x_ck"].get("media_id").is_none()));
}
