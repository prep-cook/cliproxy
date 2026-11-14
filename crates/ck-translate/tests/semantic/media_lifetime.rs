use super::{Mapped, Operation, codec, identity};
use codec::{
    BodyMode, ResponseEgress, ResponseIngress,
    document::{Document, Value},
    event::{Direction, Kind},
};
use serde_json::json;
use std::task::Poll;

fn project(wire: &str, mode: BodyMode, mutate: impl Fn(Kind, &mut Document)) -> Vec<u8> {
    let (output, result) = project_result(wire, mode, mutate);
    result.unwrap();
    output
}

fn project_result(wire: &str, mode: BodyMode, mutate: impl Fn(Kind, &mut Document)) -> (Vec<u8>, codec::Result<()>) {
    project_input(wire, BodyMode::Streaming, mode, mutate)
}

pub(super) fn project_input(
    wire: &str,
    upstream: BodyMode,
    mode: BodyMode,
    mutate: impl Fn(Kind, &mut Document),
) -> (Vec<u8>, codec::Result<()>) {
    project_filtered(wire, upstream, mode, |kind, doc| {
        mutate(kind, doc);
        true
    })
}

pub(super) fn project_filtered(
    wire: &str,
    upstream: BodyMode,
    mode: BodyMode,
    transform: impl Fn(Kind, &mut Document) -> bool,
) -> (Vec<u8>, codec::Result<()>) {
    let mut mapped = Mapped::new();
    let mut ingress = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        upstream,
        1_000_000,
    )
    .unwrap();
    ingress.advance(&mut mapped.writer, wire.as_bytes()).unwrap();
    ingress.finish(&mut mapped.writer).unwrap();
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let mut target = mapped.store.create().unwrap();
    let mut seq = 0;
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        let record = lease.record();
        let (kind, mut doc) = codec::internal::decode(&mut mapped.writer, lease).unwrap();
        if !transform(kind, &mut doc) {
            continue;
        }
        let x = doc.required(doc.root(), "x_ck").unwrap();
        doc.set(x, "seq", Value::Unsigned(seq)).unwrap();
        codec::encode::publish(&mut target, record.kind, 0, |e| {
            if let Some(name) = kind.name() {
                e.bytes(b"event: ")?;
                e.bytes(name.as_bytes())?;
                e.bytes(b"\n")?;
            }
            e.bytes(b"id: ")?;
            e.unsigned(seq)?;
            e.bytes(b"\ndata: ")?;
            doc.write(doc.root(), e)?;
            e.bytes(b"\n\n")
        })
        .unwrap();
        seq += 1;
    }
    let mut reader = mapped.store.reader(target.id(), 0).unwrap();
    let mut egress = codec::chat::egress::Egress::new(
        &mut target,
        identity(Direction::Response),
        Operation::Generate,
        mode,
        false,
        |_| {},
    )
    .unwrap();
    let mut output = Vec::new();
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        if let Err(error) = egress.advance(&mut target, lease, |lease| {
            output.extend(super::bytes(&lease));
            Ok(())
        }) {
            assert!(egress.finish().is_err());
            assert!(matches!(
                egress.advance(&mut target, reader_record(&mut reader), |_| Ok(())),
                Err(codec::Error::Closed)
            ));
            return (output, Err(error));
        }
    }
    let result = egress.finish();
    (output, result)
}

#[test]
fn trusted_response_edits_replace_media_text_and_delete_provenance_before_delivery() {
    let wire = concat!(
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"old\",\"vendor\":{\"old\":true},\"audio\":{\"data\":\"AQID\",\"transcript\":\"old\",\"id\":\"a\"}}}]}\n\n",
        "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
    );
    for mode in [BodyMode::Streaming, BodyMode::Buffered] {
        let output = project(wire, mode, |kind, doc| {
            let root = doc.root();
            if kind == Kind::Data {
                doc.set(root, "bytes", Value::Text("BAUG")).unwrap();
            }
            if kind == Kind::MediaTranscript && doc.string_is(doc.required(root, "text").unwrap(), "old").unwrap() {
                doc.set(root, "text", Value::Text("new")).unwrap();
            }
            if kind == Kind::Chunk {
                let choices = doc.required(root, "choices").unwrap();
                let nodes: Vec<_> = doc.children(choices).unwrap().collect();
                for choice in nodes {
                    let delta = doc.required(choice, "delta").unwrap();
                    if doc.member(delta, "content").unwrap().is_some() {
                        doc.set(delta, "content", Value::Text("new")).unwrap();
                    }
                    doc.remove(delta, "vendor").unwrap();
                }
            }
        });
        let messages = output_messages(&output, mode);
        assert_eq!(
            messages
                .iter()
                .filter_map(|m| m["content"].as_str())
                .collect::<String>(),
            "new"
        );
        assert_eq!(
            messages
                .iter()
                .filter_map(|m| m["audio"]["transcript"].as_str())
                .collect::<String>(),
            "new"
        );
        let text = String::from_utf8(output).unwrap();
        assert!(!text.contains("old"));
        assert!(!text.contains("vendor"));
        assert!(text.contains("BAUG"));
        assert!(text.contains("new"));
        let deleted = project(wire, mode, |kind, doc| {
            if kind == Kind::MediaStart {
                let attrs = doc.required(doc.root(), "attributes").unwrap();
                doc.remove(attrs, "value").unwrap();
            }
        });
        let deleted = String::from_utf8(deleted).unwrap();
        assert!(!deleted.contains("\"audio\""));
    }
}

#[test]
fn audio_data_reaches_client_before_finish_with_one_lifetime() {
    let mut mapped = Mapped::new();
    let mut ingress = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        1_000_000,
    )
    .unwrap();
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let mut egress = codec::chat::egress::Egress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        false,
        |_| {},
    )
    .unwrap();
    let mut output = Vec::new();
    for (index, audio) in [
        json!({"data":"AQID","transcript":"he"}),
        json!({"id":"late","data":"BA==","transcript":"llo","format":"wav"}),
    ]
    .iter()
    .enumerate()
    {
        let event = json!({"choices":[{"index":7,"delta":{"audio":audio}}]});
        ingress
            .advance(&mut mapped.writer, format!("data: {event}\n\n").as_bytes())
            .unwrap();
        while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
            egress
                .advance(&mut mapped.writer, lease, |lease| {
                    output.extend(super::bytes(&lease));
                    Ok(())
                })
                .unwrap();
        }
        assert!(String::from_utf8_lossy(&output).contains(if index == 0 { "AQID" } else { "BA==" }));
        assert!(!String::from_utf8_lossy(&mapped.output(&mapped.writer)).contains("event: x_ck.media.end"));
    }
    ingress
        .advance(
            &mut mapped.writer,
            b"data: {\"choices\":[{\"index\":7,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
        )
        .unwrap();
    ingress.finish(&mut mapped.writer).unwrap();
    let canonical = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert_eq!(canonical.matches("event: x_ck.media.start\n").count(), 1);
    assert_eq!(canonical.matches("event: x_ck.media.end\n").count(), 1);
    assert!(canonical.contains("\"offset\":3"));
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        egress
            .advance(&mut mapped.writer, lease, |lease| {
                output.extend(super::bytes(&lease));
                Ok(())
            })
            .unwrap();
    }
    egress.finish().unwrap();
    assert_eq!(String::from_utf8_lossy(&output).matches("AQID").count(), 1);
}

#[test]
fn complete_file_occurrences_stay_distinct_and_kind_mime_edits_win() {
    let part =
        json!({"type":"file","file":{"filename":"old.txt","file_data":"data:text/plain;base64,AQID"},"extra":null});
    let event = json!({"choices":[{"index":0,"delta":{"content":[part.clone(),part]},"finish_reason":"stop"}]});
    let wire = format!("data: {event}\n\ndata: [DONE]\n\n");
    for mode in [BodyMode::Streaming, BodyMode::Buffered] {
        let output = project(&wire, mode, |kind, doc| {
            if kind == Kind::MediaStart {
                doc.set(doc.root(), "mime", Value::Text("application/pdf")).unwrap();
                let attrs = doc.required(doc.root(), "attributes").unwrap();
                let value = doc.required(attrs, "value").unwrap();
                let file = doc.required(value, "file").unwrap();
                doc.set(file, "filename", Value::Text("new.pdf")).unwrap();
            }
        });
        let output = String::from_utf8(output).unwrap();
        assert_eq!(output.matches("new.pdf").count(), 2);
        assert_eq!(output.matches("data:application/pdf;base64,AQID").count(), 2);
        assert!(!output.contains("old.txt"));
        assert_eq!(output.matches("\"extra\":null").count(), 2);
    }
}

pub(super) fn output_messages(output: &[u8], mode: BodyMode) -> Vec<serde_json::Value> {
    let mut mapped = Mapped::new();
    let records: Vec<&[u8]> = if mode == BodyMode::Buffered {
        vec![output]
    } else {
        std::str::from_utf8(output)
            .unwrap()
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter(|line| *line != "[DONE]")
            .map(str::as_bytes)
            .collect()
    };
    records
        .into_iter()
        .flat_map(|raw| {
            let mut source = codec::source::Source::new(&mut mapped.writer).unwrap();
            source.push_bytes(&mut mapped.writer, raw).unwrap();
            Document::parse(&mut mapped.writer, source, 1_000_000).expect("duplicate output key");
            let value: serde_json::Value = serde_json::from_slice(raw).unwrap();
            value["choices"]
                .as_array()
                .unwrap()
                .iter()
                .map(|choice| choice[if mode == BodyMode::Buffered { "message" } else { "delta" }].clone())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn image_and_file_byte_kind_and_filename_replacements_are_authoritative() {
    use base64::Engine;
    let event = json!({"choices":[{"index":3,"delta":{"content":[
        {"type":"image_url","image_url":{"url":"data:image/png;base64,AQID","vendor":false}},
        {"type":"file","file":{"file_data":"data:text/plain;base64,AQID","filename":"old.txt"}}
    ]},"finish_reason":"stop"}]});
    let wire = format!("data: {event}\n\ndata: [DONE]\n\n");
    for mode in [BodyMode::Streaming, BodyMode::Buffered] {
        let output = project(&wire, mode, |kind, doc| {
            let root = doc.root();
            if kind == Kind::Data {
                doc.set(root, "bytes", Value::Text("BAUG")).unwrap();
            }
            if kind == Kind::MediaStart {
                doc.set(root, "mime", Value::Text("application/pdf")).unwrap();
                doc.set(root, "kind", Value::Text("document")).unwrap();
                let attrs = doc.required(root, "attributes").unwrap();
                let value = doc.required(attrs, "value").unwrap();
                if let Some(file) = doc.member(value, "file").unwrap() {
                    doc.set(file, "filename", Value::Text("new.pdf")).unwrap();
                }
            }
        });
        let messages = output_messages(&output, mode);
        let parts: Vec<_> = messages
            .iter()
            .filter_map(|m| m["content"].as_array())
            .flatten()
            .collect();
        assert_eq!(parts.len(), 2);
        for part in &parts {
            assert_eq!(part["type"], "file");
            assert!(part.get("image_url").is_none());
            let data = part["file"]["file_data"]
                .as_str()
                .unwrap()
                .strip_prefix("data:application/pdf;base64,")
                .unwrap();
            assert_eq!(
                base64::engine::general_purpose::STANDARD.decode(data).unwrap(),
                [4, 5, 6]
            );
        }
        assert_eq!(parts[0]["file"]["vendor"], false);
        assert_eq!(parts[1]["file"]["filename"], "new.pdf");
    }
}

#[test]
fn foreign_provenance_never_executes_chat_paths_and_native_control_stays_literal() {
    let event = json!({"choices":[{"index":7,"delta":{"content":"kept","vendor":[null,false,{"id":"same"}],"x_ck":{"media_id":"forged","attempt":99}},"finish_reason":"stop"}],"x_ck":{"attempt":99}});
    let wire = format!("data: {event}\n\ndata: [DONE]\n\n");
    for mode in [BodyMode::Streaming, BodyMode::Buffered] {
        let output = project(&wire, mode, |kind, doc| {
            if kind != Kind::Chunk {
                return;
            }
            let x = doc.required(doc.root(), "x_ck").unwrap();
            let original = match doc.member(x, "original").unwrap() {
                Some(node) => node,
                None => doc.set(x, "original", Value::Array).unwrap(),
            };
            let entry = doc.add(None, Value::Object).unwrap();
            doc.set(entry, "protocol", Value::Text("responses")).unwrap();
            doc.set(entry, "pointer", Value::Text("not/a/chat/pointer")).unwrap();
            doc.set(entry, "value", Value::Text("foreign-control")).unwrap();
            doc.append(original, entry).unwrap();
        });
        let messages = output_messages(&output, mode);
        assert_eq!(
            messages
                .iter()
                .filter_map(|m| m["content"].as_str())
                .collect::<String>(),
            "kept"
        );
        assert!(
            messages
                .iter()
                .any(|m| m["x_ck"] == event["choices"][0]["delta"]["x_ck"])
        );
        assert!(
            messages
                .iter()
                .any(|m| m["vendor"] == event["choices"][0]["delta"]["vendor"])
        );
        assert!(!String::from_utf8_lossy(&output).contains("foreign-control"));
    }
}

#[test]
fn restored_leaf_parent_and_wire_provenance_deletion_preserve_other_choices() {
    let wire = concat!(
        "data: {\"x_ck\":{\"attempt\":99},\"choices\":[{\"index\":7,\"delta\":{\"content\":[{\"type\":\"text\",\"text\":\"old\",\"vendor\":false}],\"vendor\":{\"leaf\":\"old\",\"keep\":[null,false]}}},{\"index\":3,\"delta\":{\"content\":\"sibling\",\"vendor\":[null,3]}}]}\n\n",
        "data: {\"choices\":[{\"index\":3,\"delta\":{},\"finish_reason\":\"stop\"},{\"index\":7,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
    );
    for mode in [BodyMode::Streaming, BodyMode::Buffered] {
        let output = project(wire, mode, |kind, doc| {
            if kind != Kind::Chunk {
                return;
            }
            let x = doc.required(doc.root(), "x_ck").unwrap();
            doc.remove(x, "original").unwrap();
            let choices = doc.required(doc.root(), "choices").unwrap();
            let nodes: Vec<_> = doc.children(choices).unwrap().collect();
            for choice in nodes {
                if doc.unsigned(doc.required(choice, "index").unwrap()).unwrap() != 7 {
                    continue;
                }
                let delta = doc.required(choice, "delta").unwrap();
                doc.remove(delta, "content").unwrap();
                if let Some(vendor) = doc.member(delta, "vendor").unwrap() {
                    doc.remove(vendor, "leaf").unwrap();
                }
            }
        });
        let messages = output_messages(&output, mode);
        assert!(!String::from_utf8_lossy(&output).contains("old"));
        assert!(!String::from_utf8_lossy(&output).contains("attempt"));
        assert!(messages.iter().any(|m| m["vendor"] == json!({"keep":[null,false]})));
        assert!(messages.iter().any(|m| m["vendor"] == json!([null, 3])));
        assert_eq!(
            messages
                .iter()
                .filter_map(|m| m["content"].as_str())
                .collect::<String>(),
            "sibling"
        );
    }
}

#[test]
fn sparse_interleaved_audio_binds_late_identity_and_exact_cumulative_positions() {
    use base64::Engine;
    let events = [
        json!({"choices":[{"index":7,"delta":{"audio":{"data":"AQ","transcript":"a"}}},{"index":3,"delta":{"audio":{"data":"BAUG","id":"same","format":"wav"}}}]}),
        json!({"choices":[{"index":3,"delta":{"audio":{"id":"same","format":"wav","expires_at":1}}},{"index":7,"delta":{"audio":{"data":"ID","id":"same","format":"wav","transcript":"b"}}}]}),
        json!({"choices":[{"index":7,"delta":{"audio":{"data":"Bw==","id":"same","format":"wav","expires_at":2}},"finish_reason":"stop"},{"index":3,"delta":{},"finish_reason":"stop"}]}),
    ];
    let wire = events.iter().map(|e| format!("data: {e}\n\n")).collect::<String>() + "data: [DONE]\n\n";
    for mode in [BodyMode::Streaming, BodyMode::Buffered] {
        let (output, canonical) = super::response(wire.as_bytes(), BodyMode::Streaming, mode);
        let starts: Vec<_> = canonical.iter().filter(|e| e["encoding"] == "base64").collect();
        assert_eq!(starts.len(), 2);
        assert_ne!(starts[0]["x_ck"]["media_id"], starts[1]["x_ck"]["media_id"]);
        for (choice, expected, transcript) in [(7, vec![1, 2, 3, 7], "ab"), (3, vec![4, 5, 6], "")] {
            let start = starts.iter().find(|e| e["x_ck"]["choice_index"] == choice).unwrap();
            let id = &start["x_ck"]["media_id"];
            let records: Vec<_> = canonical.iter().filter(|e| &e["x_ck"]["media_id"] == id).collect();
            let mut bytes = Vec::new();
            for (index, data) in records.iter().filter(|e| e["bytes"].is_string()).enumerate() {
                assert_eq!(data["index"], index);
                assert_eq!(data["offset"], bytes.len());
                bytes.extend(
                    base64::engine::general_purpose::STANDARD
                        .decode(data["bytes"].as_str().unwrap())
                        .unwrap(),
                );
            }
            assert_eq!(bytes, expected);
            let ends: Vec<_> = records.iter().filter(|e| e["status"] == "completed").collect();
            assert_eq!(ends.len(), 1);
            assert_eq!(ends[0]["bytes"], expected.len());
            let projected: Vec<serde_json::Value> = if mode == BodyMode::Buffered {
                vec![serde_json::from_slice::<serde_json::Value>(&output).unwrap()]
            } else {
                String::from_utf8_lossy(&output)
                    .lines()
                    .filter_map(|l| l.strip_prefix("data: "))
                    .filter(|l| *l != "[DONE]")
                    .map(|l| serde_json::from_str(l).unwrap())
                    .collect()
            };
            let audio: Vec<_> = projected
                .iter()
                .flat_map(|v| v["choices"].as_array().unwrap())
                .filter(|c| c["index"] == choice)
                .filter_map(|c| c[if mode == BodyMode::Buffered { "message" } else { "delta" }].get("audio"))
                .collect();
            let bytes: Vec<u8> = audio
                .iter()
                .filter_map(|a| a["data"].as_str())
                .flat_map(|s| base64::engine::general_purpose::STANDARD.decode(s).unwrap())
                .collect();
            assert_eq!(bytes, expected);
            assert_eq!(
                audio
                    .iter()
                    .filter_map(|a| a["transcript"].as_str())
                    .collect::<String>(),
                transcript
            );
            assert!(audio.iter().any(|a| a["id"] == "same" && a["format"] == "wav"));
            assert!(audio.iter().any(|a| a["expires_at"] == if choice == 7 { 2 } else { 1 }));
        }
    }
}

#[test]
fn audio_contradictions_truncation_and_provider_errors_never_close_successfully() {
    for suffix in [
        "data: {\"choices\":[{\"index\":7,\"delta\":{\"audio\":{\"id\":\"other\"}}}]}\n\n",
        "data: {\"choices\":[{\"index\":7,\"delta\":{\"audio\":{\"format\":\"mp3\"}}}]}\n\n",
        "data: {\"choices\":[{\"index\":7,\"delta\":{\"audio\":{\"codec\":\"other\"}}}]}\n\n",
        "data: {\"choices\":[{\"index\":7,\"delta\":{\"audio\":{\"sample_rate\":48000}}}]}\n\n",
        "data: {\"choices\":[{\"index\":7,\"delta\":{\"audio\":{\"channels\":2}}}]}\n\n",
        "data: {\"choices\":[{\"index\":7,\"delta\":{\"audio\":{\"data\":\"AQ\"}},\"finish_reason\":\"stop\"}]}\n\n",
        "data: {\"choices\":[]}",
        "data: {\"error\":{\"message\":\"secret-provider-detail\"}}\n\n",
        "event: error\ndata: {\"message\":\"secret-provider-detail\"}\n\n",
    ] {
        for mode in [BodyMode::Streaming, BodyMode::Buffered] {
            let mut mapped = Mapped::new();
            let mut ingress = codec::chat::response::Ingress::new(
                &mut mapped.writer,
                &mapped.store,
                identity(Direction::Response),
                Operation::Generate,
                BodyMode::Streaming,
                1_000_000,
            )
            .unwrap();
            ingress.advance(&mut mapped.writer, b"data: {\"choices\":[{\"index\":7,\"delta\":{\"audio\":{\"data\":\"AQID\",\"id\":\"a\",\"format\":\"wav\",\"codec\":\"pcm\",\"sample_rate\":24000,\"channels\":1}}}]}\n\n").unwrap();
            assert!(
                ingress
                    .advance(&mut mapped.writer, suffix.as_bytes())
                    .and_then(|()| ingress.finish(&mut mapped.writer))
                    .is_err()
            );
            let canonical = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
            for name in ["media.end", "item.end", "done"] {
                assert!(!canonical.contains(&format!("event: x_ck.{name}\n")), "{suffix}");
            }
            assert_eq!(canonical.matches("event: x_ck.error\n").count(), 1);
            let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
            let mut egress = codec::chat::egress::Egress::new(
                &mut mapped.writer,
                identity(Direction::Response),
                Operation::Generate,
                mode,
                false,
                |_| {},
            )
            .unwrap();
            let mut output = Vec::new();
            while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
                egress
                    .advance(&mut mapped.writer, lease, |lease| {
                        output.extend(super::bytes(&lease));
                        Ok(())
                    })
                    .unwrap();
            }
            egress.finish().unwrap();
            let text = String::from_utf8(output).unwrap();
            assert!(text.contains("\"error\""));
            assert!(!text.contains("[DONE]"));
            assert!(!text.contains("secret-provider-detail"));
            if mode == BodyMode::Buffered {
                assert!(
                    serde_json::from_str::<serde_json::Value>(&text)
                        .unwrap()
                        .get("choices")
                        .is_none()
                );
            }
        }
    }
}

fn reader_record(reader: &mut ck_stream::Reader) -> ck_stream::Lease {
    let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() else {
        panic!("missing next canonical record")
    };
    lease
}

#[test]
fn transformed_media_rejects_invalid_indexes_offsets_sizes_and_base64_without_done() {
    let wire = concat!(
        "data: {\"choices\":[{\"index\":7,\"delta\":{\"audio\":{\"data\":\"AQID\"}}}]}\n\n",
        "data: {\"choices\":[{\"index\":7,\"delta\":{\"audio\":{\"data\":\"BA==\"}},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
    );
    for mode in [BodyMode::Streaming, BodyMode::Buffered] {
        for invalid in ["index", "offset", "bytes", "base64"] {
            let (output, result) = project_result(wire, mode, |kind, doc| {
                let root = doc.root();
                if kind == Kind::Data && invalid != "bytes" {
                    if invalid == "base64" {
                        doc.set(root, "bytes", Value::Text("AQ")).unwrap();
                    } else {
                        doc.set(root, invalid, Value::Unsigned(99)).unwrap();
                    }
                }
                if kind == Kind::MediaEnd && invalid == "bytes" {
                    doc.set(root, "bytes", Value::Unsigned(99)).unwrap();
                }
            });
            assert!(result.is_err(), "{mode:?}/{invalid}");
            assert!(!String::from_utf8_lossy(&output).contains("[DONE]"));
            if mode == BodyMode::Buffered {
                assert!(output.is_empty());
            }
        }
    }
}

#[test]
fn buffered_ingress_mutations_replace_media_and_remove_references_before_replay() {
    use base64::Engine;
    let body = json!({"choices":[{"index":7,"message":{"content":[
        {"type":"image_url","image_url":{"url":"data:image/png;base64,AQID","vendor":"deleted"}},
        {"type":"file","file":{"file_data":"data:text/plain;base64,AQID","filename":"old.txt","vendor":[null,false]}},
        {"type":"text","text":"old"}
    ],"audio":{"id":"a","data":"AQID","transcript":"old"}},"finish_reason":"stop"}]});
    for mode in [BodyMode::Streaming, BodyMode::Buffered] {
        let (output, result) = project_input(&body.to_string(), BodyMode::Buffered, mode, |kind, doc| {
            let root = doc.root();
            if kind == Kind::Data {
                doc.set(root, "bytes", Value::Text("BAUG")).unwrap();
            }
            if kind == Kind::MediaTranscript && doc.string_is(doc.required(root, "text").unwrap(), "old").unwrap() {
                doc.set(root, "text", Value::Text("new")).unwrap();
            }
            if kind == Kind::MediaStart {
                let attrs = doc.required(root, "attributes").unwrap();
                let value = doc.required(attrs, "value").unwrap();
                if doc.member(value, "image_url").unwrap().is_some() {
                    doc.remove(attrs, "value").unwrap();
                }
                if let Some(file) = doc.member(value, "file").unwrap() {
                    doc.set(root, "mime", Value::Text("application/pdf")).unwrap();
                    doc.set(file, "filename", Value::Text("new.pdf")).unwrap();
                }
            }
            if kind == Kind::Chunk {
                let choices = doc.required(root, "choices").unwrap();
                let choices: Vec<_> = doc.children(choices).unwrap().collect();
                for choice in choices {
                    let delta = doc.required(choice, "delta").unwrap();
                    if let Some(content) = doc.member(delta, "content").unwrap() {
                        let parts: Vec<_> = doc.children(content).unwrap().collect();
                        for part in parts {
                            if doc.member(part, "text").unwrap().is_some() {
                                doc.set(part, "text", Value::Text("new")).unwrap();
                            }
                        }
                    }
                }
            }
        });
        result.unwrap();
        let messages = output_messages(&output, mode);
        let parts: Vec<_> = messages
            .iter()
            .filter_map(|m| m["content"].as_array())
            .flatten()
            .cloned()
            .collect();
        assert_eq!(
            parts,
            vec![
                json!({"type":"file","file":{"file_data":"data:application/pdf;base64,BAUG","filename":"new.pdf","vendor":[null,false]}}),
                json!({"type":"text","text":"new"})
            ]
        );
        let audio: Vec<_> = messages.iter().filter_map(|m| m.get("audio")).collect();
        let bytes: Vec<u8> = audio
            .iter()
            .filter_map(|a| a["data"].as_str())
            .flat_map(|s| base64::engine::general_purpose::STANDARD.decode(s).unwrap())
            .collect();
        assert_eq!(bytes, [4, 5, 6]);
        assert_eq!(
            audio
                .iter()
                .filter_map(|a| a["transcript"].as_str())
                .collect::<String>(),
            "new"
        );
        assert!(!String::from_utf8_lossy(&output).contains("old"));
    }
}

#[test]
fn changed_byte_lengths_follow_replacement_offsets_and_end_count() {
    use base64::Engine;
    let wire = concat!(
        "data: {\"choices\":[{\"index\":7,\"delta\":{\"audio\":{\"data\":\"AQID\"}}}]}\n\n",
        "data: {\"choices\":[{\"index\":7,\"delta\":{\"audio\":{\"data\":\"BA==\"}},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
    );
    for mode in [BodyMode::Streaming, BodyMode::Buffered] {
        let output = project(wire, mode, |kind, doc| {
            let root = doc.root();
            if kind == Kind::Data {
                let first = doc.unsigned(doc.required(root, "index").unwrap()).unwrap() == 0;
                doc.set(root, "bytes", Value::Text(if first { "CQg=" } else { "BwYF" }))
                    .unwrap();
                doc.set(root, "offset", Value::Unsigned(if first { 0 } else { 2 }))
                    .unwrap();
            }
            if kind == Kind::MediaEnd {
                doc.set(root, "bytes", Value::Unsigned(5)).unwrap();
            }
        });
        let messages = output_messages(&output, mode);
        let bytes: Vec<u8> = messages
            .iter()
            .filter_map(|m| m["audio"]["data"].as_str())
            .flat_map(|s| base64::engine::general_purpose::STANDARD.decode(s).unwrap())
            .collect();
        assert_eq!(bytes, [9, 8, 7, 6, 5]);
    }
}
