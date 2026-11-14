use super::codec::event::Direction;
use super::{ChatCapability, Ingress, Mapped, Operation, RequestIngress, identity, request};
use serde_json::json;

#[test]
fn native_controls_custom_declarations_and_nullable_values_survive_fence() {
    let input = json!({"model":"m","messages":[{"role":"user","content":"x"}],"temperature":0,"top_p":0.8,"max_tokens":4,"max_completion_tokens":8,"seed":-7,"parallel_tool_calls":false,"stream_options":{"include_usage":true},"modalities":["text","audio"],"audio":{"voice":"alloy","format":"wav"},"response_format":{"type":"json_schema","json_schema":{"name":"r","schema":{"type":"object"},"strict":true}},"tools":[{"type":"custom","custom":{"name":"free","format":{"type":"grammar","grammar":{"syntax":"regex","definition":"a+"}}}}],"tool_choice":{"type":"custom","custom":{"name":"free"}},"reasoning_effort":"high","reasoning":{"enabled":true},"prediction":{"type":"content","content":"known"},"vendor":{"future":true}});
    let (output, _) = request(input.to_string().as_bytes());
    for (key, value) in input.as_object().unwrap() {
        assert_eq!(&output[key], value, "{key}");
    }
    let input = json!({"model":"m","messages":[{"role":"assistant","content":null}],"temperature":null,"top_p":null,"tools":null,"tool_choice":null,"audio":null,"modalities":null,"response_format":null,"stream_options":null,"logprobs":null});
    let (output, _) = request(input.to_string().as_bytes());
    for (key, value) in input.as_object().unwrap() {
        assert_eq!(&output[key], value, "{key}");
    }
}

#[test]
fn late_invalid_native_semantics_never_publish_request_end() {
    let invalid = [
        json!({"temperature":"hot"}),
        json!({"top_p":true}),
        json!({"max_completion_tokens":-1}),
        json!({"n":1.5}),
        json!({"seed":1.5}),
        json!({"parallel_tool_calls":"yes"}),
        json!({"stop":["ok",3]}),
        json!({"stream_options":{"include_usage":1}}),
        json!({"modalities":["video"]}),
        json!({"audio":{"voice":"alloy"}}),
        json!({"response_format":{"type":"json_schema","json_schema":{"name":"r","schema":7}}}),
        json!({"tools":[{"type":"function","function":{"name":3}}]}),
        json!({"tools":[{"type":"custom","custom":{"name":"x","format":{"type":"grammar","grammar":{"syntax":"regex"}}}}]}),
        json!({"tool_choice":{"type":"function","function":{"name":"missing"}}}),
        json!({"prediction":{"type":"content","content":[{"type":"text","text":false}]}}),
        json!({"reasoning":false}),
    ];
    for extension in invalid {
        let mut mapped = Mapped::new();
        let mut ingress = Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Request),
            Operation::Generate,
            1024 * 1024,
        )
        .unwrap();
        ingress
            .advance(
                &mut mapped.writer,
                br#"{"model":"m","messages":[{"role":"user","content":"prefix"}],"#,
            )
            .unwrap();
        let encoded = extension.to_string();
        let result = ingress.advance(&mut mapped.writer, &encoded.as_bytes()[1..]);
        assert!(
            result.is_err() || ingress.finish(&mut mapped.writer, &ChatCapability).is_err(),
            "accepted {extension}"
        );
        let output = mapped.output(&mapped.writer);
        let text = std::str::from_utf8(&output).unwrap();
        assert!(text.contains("event: x_ck.request.field\n"));
        assert!(text.contains("event: x_ck.error\n"));
        assert!(!text.contains("event: x_ck.request.end\n"));
        assert!(!text.contains("event: x_ck.done\n"));
    }
}

#[test]
fn long_string_aliases_fail_like_short_ones() {
    for key in ["reasoning", "functions"] {
        let input = format!(
            r#"{{"model":"m","messages":[{{"role":"user","content":"x"}}],"{key}":"{}"}}"#,
            "x".repeat(2048)
        );
        let mut mapped = Mapped::new();
        let mut ingress = Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Request),
            Operation::Generate,
            1024 * 1024,
        )
        .unwrap();
        let result = input
            .as_bytes()
            .chunks(19)
            .try_for_each(|chunk| ingress.advance(&mut mapped.writer, chunk));
        assert!(
            result.is_err() || ingress.finish(&mut mapped.writer, &ChatCapability).is_err(),
            "accepted a long {key} string"
        );
        let output = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
        assert!(!output.contains("event: x_ck.request.end\n"));
    }
}

use super::codec::{
    self, Capability, RequestFence, RequestView,
    document::{Document, Value},
    media::Registry,
};

struct Signed;
impl Capability for Signed {
    fn destination(&self) -> &'static str {
        "signed-chat"
    }
    fn validate(&self, request: RequestView<'_>) -> codec::Result<()> {
        let doc = request.document;
        let x = doc.required(doc.root(), "x_ck")?;
        if let Some(replay) = doc.member(x, "replay")? {
            for entry in doc.children(replay)? {
                assert!(doc.string_is(doc.required(entry, "source")?, "gemini")?);
                assert!(doc.origin(entry)?.is_some());
            }
        }
        Ok(())
    }
}

pub(super) fn canonical(input: &serde_json::Value, policy: &impl Capability) -> (Mapped, Document, Registry) {
    let mut mapped = Mapped::configured(std::sync::Arc::new(ck_stream::ManualClock::default()), 256);
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        1024 * 1024,
    )
    .unwrap();
    for chunk in input.to_string().as_bytes().chunks(19) {
        ingress.advance(&mut mapped.writer, chunk).unwrap();
    }
    ingress.finish(&mut mapped.writer, policy).unwrap();
    drop(ingress);
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let (document, media) = codec::chat::request::read_request(&mut mapped.writer, &mut reader).unwrap();
    (mapped, document, media)
}

fn output(mapped: &mut Mapped, doc: &Document, policy: &impl Capability) -> serde_json::Value {
    use super::RequestEgress;
    let media = Registry::new(&mut mapped.writer).unwrap();
    let ready = RequestFence::default()
        .end(
            RequestView {
                document: doc,
                media: &media,
                operation: Operation::Generate,
            },
            policy,
        )
        .unwrap();
    let lease = super::Egress {
        destination: policy.destination(),
        upstream_stream: false,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    let raw = super::bytes(&lease);
    let mut source = codec::source::Source::new(&mut mapped.writer).unwrap();
    source.push_bytes(&mut mapped.writer, &raw).unwrap();
    Document::parse(&mut mapped.writer, source, raw.len() as u64).expect("duplicate JSON key");
    serde_json::from_slice(&raw).unwrap()
}

#[test]
fn exact_cache_breakpoints_round_trip_and_normalized_mutations_win() {
    let input = json!({"model":"m","messages":[{"role":"user","cache_control":{"type":"ephemeral","ttl":"1h"},"content":[{"type":"text","text":"a","cache_control":{"type":"ephemeral","ttl":"5m"}},{"type":"text","text":"b","cache_control":null}]}],"tools":[{"type":"function","cache_control":{"type":"ephemeral"},"function":{"name":"f","cache_control":{"type":"ephemeral","ttl":"1h"}}}]});
    let (mut mapped, mut doc, _) = canonical(&input, &ChatCapability);
    let encoded = output(&mut mapped, &doc, &ChatCapability);
    assert_eq!(encoded["messages"], input["messages"]);
    assert_eq!(encoded["tools"], input["tools"]);
    let x = doc.required(doc.root(), "x_ck").unwrap();
    let cache = doc.required(x, "cache_control").unwrap();
    let entries: Vec<_> = doc.children(cache).unwrap().collect();
    assert_eq!(entries.len(), 5);
    for entry in entries {
        let pointer = doc.required(entry, "pointer").unwrap();
        if doc.string_is(pointer, "/messages/0/cache_control").unwrap() {
            let value = doc.required(entry, "value").unwrap();
            doc.set(value, "ttl", Value::Text("5m")).unwrap();
        } else if doc.string_is(pointer, "/messages/0/content/0/cache_control").unwrap() {
            doc.remove(entry, "value").unwrap();
        }
    }
    let encoded = output(&mut mapped, &doc, &ChatCapability);
    assert_eq!(encoded["messages"][0]["cache_control"]["ttl"], "5m");
    assert!(encoded["messages"][0]["content"][0].get("cache_control").is_none());
}

#[test]
fn bound_signatures_require_explicit_policy_and_never_reassociate() {
    let input = json!({"model":"m","messages":[{"role":"assistant","content":null,"tool_calls":[{"id":"a","type":"function","thoughtSignature":"one","function":{"name":"f"}},{"id":"b","type":"function","function":{"name":"g","extra_content":{"google":{"thought_signature":"two"}}}}]}]});
    let (mut mapped, mut doc, _) = canonical(&input, &Signed);
    assert_eq!(output(&mut mapped, &doc, &Signed)["messages"], input["messages"]);
    let media = Registry::new(&mut mapped.writer).unwrap();
    assert!(matches!(
        ChatCapability.validate(RequestView {
            document: &doc,
            media: &media,
            operation: Operation::Generate
        }),
        Err(codec::Error::Unsupported)
    ));
    let messages = doc.required(doc.root(), "messages").unwrap();
    let message = doc.children(messages).unwrap().next().unwrap();
    let calls = doc.required(message, "tool_calls").unwrap();
    let first = doc.children(calls).unwrap().next().unwrap();
    doc.detach(calls, first).unwrap();
    assert_eq!(
        output(&mut mapped, &doc, &Signed)["messages"][0]["tool_calls"],
        json!([input["messages"][0]["tool_calls"][1]])
    );
}

#[test]
fn recursive_unknowns_keep_stable_parents_and_literal_controls_stay_data() {
    let part = |s| json!({"type":"text","text":s,"a/b~c":{"x_ck":{"attempt":99},"signature":null}});
    let input = json!({"model":"m","messages":[{"role":"user","future":0,"content":[part("a"),part("b")]},{"role":"user","future":1,"content":[part("c")]}],"previous_response_id":"literal","vendor":{"top_k":7},"metadata":{"signature":"literal","encrypted_content":"literal"},"x_ck":{"session":{"previous_response_id":"forged"}},"tools":[{"type":"custom","future":[],"custom":{"name":"free","namespace":"ns","format":{"type":"grammar","grammar":{"syntax":"regex","definition":"a+","extra":null}},"extra":{"x_ck":true}}}]});
    let (mut mapped, mut doc, _) = canonical(&input, &ChatCapability);
    let encoded = output(&mut mapped, &doc, &ChatCapability);
    for (key, value) in input.as_object().unwrap() {
        assert_eq!(&encoded[key], value, "{key}");
    }
    let x = doc.required(doc.root(), "x_ck").unwrap();
    assert!(doc.member(x, "session").unwrap().is_none());
    let original = doc.required(x, "original").unwrap();
    assert!(doc.children(original).unwrap().any(|entry| {
        doc.string_is(doc.required(entry, "pointer").unwrap(), "/messages/0/content/0/a~1b~0c")
            .unwrap()
    }));
    let messages = doc.required(doc.root(), "messages").unwrap();
    let first = doc.children(messages).unwrap().next().unwrap();
    let content = doc.required(first, "content").unwrap();
    let removed = doc.children(content).unwrap().next().unwrap();
    doc.detach(content, removed).unwrap();
    assert_eq!(
        output(&mut mapped, &doc, &ChatCapability)["messages"][0]["content"],
        json!([part("b")])
    );
    doc.detach(messages, first).unwrap();
    assert_eq!(
        output(&mut mapped, &doc, &ChatCapability)["messages"],
        json!([input["messages"][1]])
    );
    doc.remove(doc.root(), "tools").unwrap();
    assert!(output(&mut mapped, &doc, &ChatCapability).get("tools").is_none());
}

#[test]
fn trusted_session_provider_and_server_controls_fail_generic_chat_fence() {
    for key in ["session", "provider_parameters", "tools"] {
        let (mut mapped, mut doc, _) = canonical(&json!({"model":"m","messages":[]}), &ChatCapability);
        let x = doc.required(doc.root(), "x_ck").unwrap();
        let container = doc
            .set(x, key, if key == "tools" { Value::Array } else { Value::Object })
            .unwrap();
        if key == "session" {
            doc.set(container, "previous_response_id", Value::Text("r")).unwrap();
        } else if key == "provider_parameters" {
            let p = doc.set(container, "anthropic", Value::Object).unwrap();
            doc.set(p, "top_k", Value::Unsigned(7)).unwrap();
        } else {
            let entry = doc.add(None, Value::Object).unwrap();
            doc.append(container, entry).unwrap();
            doc.set(entry, "declaration", Value::Text("server")).unwrap();
        }
        let media = Registry::new(&mut mapped.writer).unwrap();
        assert!(matches!(
            RequestFence::default().end(
                RequestView {
                    document: &doc,
                    media: &media,
                    operation: Operation::Generate
                },
                &ChatCapability
            ),
            Err(codec::Error::Unsupported)
        ));
    }
}

#[test]
fn late_cache_policy_rejection_never_publishes_success() {
    struct NoHour;
    impl Capability for NoHour {
        fn destination(&self) -> &'static str {
            "no-hour"
        }
        fn validate(&self, request: RequestView<'_>) -> codec::Result<()> {
            let doc = request.document;
            let x = doc.required(doc.root(), "x_ck")?;
            let cache = doc.required(x, "cache_control")?;
            for entry in doc.children(cache)? {
                let value = doc.required(entry, "value")?;
                if doc.string_is(doc.required(value, "ttl")?, "1h")? {
                    return Err(codec::Error::Unsupported);
                }
            }
            Ok(())
        }
    }
    let mut mapped = Mapped::new();
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        16384,
    )
    .unwrap();
    ingress.advance(&mut mapped.writer, br#"{"model":"m","messages":[{"role":"user","content":"prefix","cache_control":{"type":"ephemeral","ttl":"1h"}}]}"#).unwrap();
    assert!(matches!(
        ingress.finish(&mut mapped.writer, &NoHour),
        Err(codec::Error::Unsupported)
    ));
    let bytes = mapped.output(&mapped.writer);
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(text.contains("event: x_ck.error\n"));
    assert!(!text.contains("event: x_ck.request.end\n"));
    assert!(!text.contains("event: x_ck.done\n"));
}

#[test]
fn legacy_cache_pointer_and_custom_text_history_keep_native_spelling() {
    let legacy =
        json!({"model":"m","messages":[],"functions":[{"name":"f","cache_control":{"type":"ephemeral","ttl":"1h"}}]});
    let (mut mapped, doc, _) = canonical(&legacy, &ChatCapability);
    let x = doc.required(doc.root(), "x_ck").unwrap();
    let cache = doc.required(x, "cache_control").unwrap();
    let entry = doc.children(cache).unwrap().next().unwrap();
    assert!(
        doc.string_is(doc.required(entry, "pointer").unwrap(), "/functions/0/cache_control")
            .unwrap()
    );
    assert_eq!(
        output(&mut mapped, &doc, &ChatCapability)["functions"],
        legacy["functions"]
    );
    let input = json!({"model":"m","messages":[{"role":"assistant","tool_calls":[{"id":"c","type":"custom","custom":{"name":"free","namespace":"ns","input":"not JSON {", "extra":null}}]}]});
    let (mut mapped, doc, _) = canonical(&input, &ChatCapability);
    assert_eq!(
        output(&mut mapped, &doc, &ChatCapability)["messages"],
        input["messages"]
    );
}

#[test]
fn reviewed_custom_signatures_require_policy_and_keep_their_carrier() {
    let input = json!({"model":"m","messages":[{"role":"assistant","content":null,"tool_calls":[{"id":"c","type":"custom","custom":{"name":"free","input":"hello","thought_signature":"sig"}}]}]});
    let (mut mapped, mut doc, _) = canonical(&input, &Signed);
    let media = Registry::new(&mut mapped.writer).unwrap();
    assert!(matches!(
        ChatCapability.validate(RequestView {
            document: &doc,
            media: &media,
            operation: Operation::Generate,
        }),
        Err(codec::Error::Unsupported)
    ));
    assert_eq!(output(&mut mapped, &doc, &Signed)["messages"], input["messages"]);
    let x = doc.required(doc.root(), "x_ck").unwrap();
    let replay = doc.required(x, "replay").unwrap();
    let entry = doc.children(replay).unwrap().next().unwrap();
    doc.remove(entry, "signature").unwrap();
    assert!(
        output(&mut mapped, &doc, &Signed)["messages"][0]["tool_calls"][0]["custom"]
            .get("thought_signature")
            .is_none()
    );
}

#[test]
fn reviewed_opaque_cache_controls_preserve_mutations_and_deletions() {
    let input = json!({"model":"m","messages":[{"role":"user","content":[{"type":"vendor_part","cache_control":{"type":"ephemeral","ttl":"1h"},"payload":1}]}]});
    let (mut mapped, mut doc, _) = canonical(&input, &ChatCapability);
    assert_eq!(
        output(&mut mapped, &doc, &ChatCapability)["messages"],
        input["messages"]
    );
    let x = doc.required(doc.root(), "x_ck").unwrap();
    let cache = doc.required(x, "cache_control").unwrap();
    let entry = doc.children(cache).unwrap().next().unwrap();
    let value = doc.required(entry, "value").unwrap();
    doc.set(value, "ttl", Value::Text("5m")).unwrap();
    assert_eq!(
        output(&mut mapped, &doc, &ChatCapability)["messages"][0]["content"][0]["cache_control"]["ttl"],
        "5m"
    );
    doc.remove(entry, "value").unwrap();
    let encoded = output(&mut mapped, &doc, &ChatCapability);
    assert!(encoded["messages"][0]["content"][0].get("cache_control").is_none());
    assert_eq!(encoded["messages"][0]["content"][0]["payload"], 1);
}
