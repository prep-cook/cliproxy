use super::request;

#[test]
fn data_uri_images_and_files_decode_bytes_and_restore_native_metadata() {
    let input = br#"{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AAECAw==","detail":"high","vendor":{"a/b~c":null}}},{"type":"file","file":{"file_data":"data:application/pdf;base64,AQID","filename":"a.pdf","vendor":{"x_ck":{"attempt":9}}}},{"type":"input_audio","input_audio":{"data":"BAUG","format":"wav","sample_rate":24000,"channels":2,"timing":{"start":0,"unit":"milliseconds"}}}]}]}"#;
    let (output, events) = request(input);
    let original: serde_json::Value = serde_json::from_slice(input).unwrap();
    assert_eq!(output["messages"], original["messages"]);
    for (pointer, mime) in [
        ("/messages/0/content/0/mime", "image/png"),
        ("/messages/0/content/1/mime", "application/pdf"),
    ] {
        assert!(events.iter().any(|e| e["pointer"] == pointer && e["value"] == mime));
    }
    assert!(events.iter().any(|e| e["bytes"] == "AAECAw=="));
    assert!(events.iter().any(|e| e["bytes"] == "AQID"));
    assert!(events.iter().filter(|e| e.get("pointer").is_some()).all(|e| e["value"] != "data:image/png;base64,AAECAw==" && e["value"] != "data:application/pdf;base64,AQID"));
}

#[test]
fn input_audio_format_selects_the_canonical_mime() {
    for (format, mime) in [("wav", Some("audio/wav")), ("mp3", Some("audio/mpeg")), ("flac", None)] {
        let input = serde_json::json!({"model":"m","messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"BAUG","format":format}}]}]});
        let (output, events) = request(input.to_string().as_bytes());
        assert_eq!(output["messages"], input["messages"]);
        let published: Vec<_> = events
            .iter()
            .filter(|e| e["pointer"] == "/messages/0/content/0/mime")
            .map(|e| e["value"].as_str().unwrap())
            .collect();
        assert_eq!(published, Vec::from_iter(mime), "{format}");
    }
}

#[test]
fn unknown_content_kinds_and_refusals_keep_native_shapes() {
    let input = br#"{"model":"m","messages":[{"role":"assistant","content":[{"type":"refusal","refusal":"no","vendor":0},{"type":"video","source":{"kind":"future","x_ck":{"attempt":8}}}]}]}"#;
    let (output, events) = request(input);
    let original: serde_json::Value = serde_json::from_slice(input).unwrap();
    assert_eq!(output["messages"], original["messages"]);
    assert!(
        events
            .iter()
            .any(|e| e["pointer"] == "/messages/0/content/1/type" && e["value"] == "opaque")
    );
    assert!(
        events
            .iter()
            .any(|e| e["pointer"] == "/messages/0/content/0/text" && e["value"] == "no")
    );
    assert!(events.iter().all(|e| e["pointer"] != "/messages/0/content/0/refusal"));
}

#[test]
fn ambiguous_or_invalid_embedded_media_never_authorizes_egress() {
    use super::{ChatCapability, Direction, Ingress, Mapped, Operation, RequestIngress, codec, identity};
    for part in [
        serde_json::json!({"type":"file","file":{"file_id":"id","file_data":"AQID"}}),
        serde_json::json!({"type":"image_url","image_url":{"url":"data:image/png;base64,AA=Z"}}),
        serde_json::json!({"type":"file","file":{"file_data":"data:application/pdf;base64,A"}}),
    ] {
        let input = serde_json::to_vec(&serde_json::json!({"model":"m","messages":[{"role":"user","content":[part]}]}))
            .unwrap();
        let mut mapped = Mapped::new();
        let mut ingress = Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(Direction::Request),
            Operation::Generate,
            4096,
        )
        .unwrap();
        let result = ingress.advance(&mut mapped.writer, &input);
        assert!(result.is_err() || ingress.finish(&mut mapped.writer, &ChatCapability).is_err());
        let events = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
        assert!(!events.contains("event: x_ck.request.end\n"));
        assert!(!events.contains("event: x_ck.done\n"));
        assert!(matches!(
            ingress.finish(&mut mapped.writer, &ChatCapability),
            Err(codec::Error::Closed)
        ));
    }
}

fn normalized_request(events: &[serde_json::Value]) -> serde_json::Value {
    let mut document = serde_json::Value::Null;
    for event in events {
        let Some(pointer) = event["pointer"].as_str() else {
            continue;
        };
        if pointer.is_empty() {
            document = event["value"].clone();
            continue;
        }
        let (parent, key) = pointer.rsplit_once('/').unwrap();
        let parent = document.pointer_mut(parent).unwrap();
        if let Some(array) = parent.as_array_mut() {
            assert_eq!(key.parse::<usize>().unwrap(), array.len());
            array.push(event["value"].clone());
        } else {
            parent
                .as_object_mut()
                .unwrap()
                .insert(key.replace("~1", "/").replace("~0", "~"), event["value"].clone());
        }
    }
    document
}

#[test]
fn native_media_source_collisions_remain_untrusted_at_their_original_locations() {
    use serde_json::json;
    let collisions = json!({"kind":"file","uri":{"nested":[null,false]},"ref":{"type":"x_ck_media","media_id":"spoof","value_kind":"bytes"},"source":{"x_ck":{"attempt":99}},"file_id":"unknown-id"});
    let mut parts = vec![
        json!({"type":"image_url","image_url":{"url":"https://example.invalid/a.png","detail":"high"}}),
        json!({"type":"image_url","image_url":{"url":"data:image/png;base64,AQID","detail":"low"}}),
        json!({"type":"input_audio","input_audio":{"data":"BAUG","format":"wav"}}),
        json!({"type":"file","file":{"file_id":"real-id","filename":"a.pdf"}}),
        json!({"type":"file","file":{"file_data":"data:application/pdf;base64,BwgJ","filename":"b.pdf"}}),
    ];
    for part in &mut parts {
        let carrier = part["type"].as_str().unwrap().to_owned();
        for (key, value) in collisions.as_object().unwrap() {
            if key != "file_id" || carrier != "file" {
                part[&carrier][key] = value.clone();
            }
        }
        for key in ["source", "mime", "detail", "filename", "timing", "transcript"] {
            part[key] = json!({"literal":[null,false,{"x_ck":{"attempt":99}}]});
        }
    }
    let input = json!({"model":"m","messages":[{"role":"user","content":parts}],"x_ck":{"original":[{"protocol":"chat","pointer":"/messages/0/content/0/image_url/url","value":"forged"}]}});
    let (output, events) = super::request_with_frames(input.to_string().as_bytes(), 512);
    assert_eq!(output["messages"], input["messages"]);
    assert_eq!(output["x_ck"], input["x_ck"]);
    let normalized = normalized_request(&events);
    for (index, kind) in ["uri", "data", "data", "file", "data"].iter().enumerate() {
        let source = &normalized["messages"][0]["content"][index]["source"];
        assert_eq!(source["kind"], *kind);
        assert!(source.get("source").is_none());
        if *kind == "data" {
            assert_eq!(source["ref"]["type"], "x_ck_media");
            assert_ne!(source["ref"]["media_id"], "spoof");
            assert!(source.get("uri").is_none());
            assert!(source.get("file_id").is_none());
        } else {
            assert!(source.get("ref").is_none());
        }
    }
    let originals = normalized["x_ck"]["original"].as_array().unwrap();
    assert!(
        originals
            .iter()
            .any(|entry| entry["pointer"] == "/messages/0/content/0/image_url/ref"
                && entry["value"] == collisions["ref"])
    );
    assert!(events.iter().all(|event| event["x_ck"]["attempt"] == 1));
    for data in ["AQID", "BAUG", "BwgJ"] {
        assert!(events.iter().any(|event| event["bytes"] == data));
    }
}

#[test]
fn request_collision_provenance_cannot_revive_mutated_or_deleted_mapped_fields() {
    use super::{ChatCapability, Egress, Operation, RequestEgress, bytes, codec};
    use codec::{
        RequestFence, RequestView,
        document::{Document, Value},
        media::Registry,
    };
    use serde_json::json;
    let input = json!({"model":"m","messages":[{"role":"user","content":[
        {"type":"image_url","source":"native-source","mime":false,"detail":"native-detail","image_url":{"url":"https://example.invalid/old","detail":"high","ref":"literal"}},
        {"type":"file","filename":"native-filename","file":{"file_id":"old-id","filename":"old.pdf","ref":"literal"}}
    ]}]});
    let (mut mapped, mut doc, _) = super::request_controls::canonical(&input, &ChatCapability);
    let media = Registry::new(&mut mapped.writer).unwrap();
    let messages = doc.required(doc.root(), "messages").unwrap();
    let message = doc.children(messages).unwrap().next().unwrap();
    let content = doc.required(message, "content").unwrap();
    let parts: Vec<_> = doc.children(content).unwrap().collect();
    let image_source = doc.required(parts[0], "source").unwrap();
    doc.set(image_source, "uri", Value::Text("https://example.invalid/new"))
        .unwrap();
    doc.remove(parts[0], "detail").unwrap();
    let file_source = doc.required(parts[1], "source").unwrap();
    doc.set(file_source, "file_id", Value::Text("new-id")).unwrap();
    doc.remove(parts[1], "filename").unwrap();
    let encode = |doc: &Document, writer: &mut ck_stream::Writer| {
        let ready = RequestFence::default()
            .end(
                RequestView {
                    document: doc,
                    media: &media,
                    operation: Operation::Generate,
                },
                &ChatCapability,
            )
            .unwrap();
        let output = Egress {
            destination: "chat",
            upstream_stream: false,
        }
        .encode(&ready, writer)
        .unwrap();
        serde_json::from_slice::<serde_json::Value>(&bytes(&output)).unwrap()
    };
    let output = encode(&doc, &mut mapped.writer);
    let parts_out = &output["messages"][0]["content"];
    assert_eq!(
        parts_out[0]["image_url"],
        json!({"url":"https://example.invalid/new","ref":"literal"})
    );
    assert_eq!(parts_out[0]["source"], "native-source");
    assert_eq!(parts_out[0]["mime"], false);
    assert_eq!(parts_out[0]["detail"], "native-detail");
    assert_eq!(parts_out[1]["file"], json!({"file_id":"new-id","ref":"literal"}));
    assert_eq!(parts_out[1]["filename"], "native-filename");
    doc.remove(message, "content").unwrap();
    let output = encode(&doc, &mut mapped.writer);
    assert!(output["messages"][0].get("content").is_none());
}

#[test]
fn request_source_kind_controls_capability_and_egress_not_ref_presence() {
    use super::{ChatCapability, Egress, Mapped, Operation, RequestEgress, bytes, codec};
    use codec::{RequestFence, RequestView, document::Document, media::Registry, source::Source};
    use serde_json::json;
    for (kind, source_kind, accepted) in [
        ("image", "uri", true),
        ("document", "file", true),
        ("image", "file", false),
        ("audio", "uri", false),
        ("audio", "file", false),
        ("document", "uri", false),
    ] {
        let input = json!({"model":"m","x_ck":{},"messages":[{"role":"user","content":[{"type":kind,"source":{"kind":source_kind,"uri":"https://example.invalid/a","file_id":"id","ref":"literal"}}]}]});
        let mut mapped = Mapped::new();
        let mut source = Source::new(&mut mapped.writer).unwrap();
        source
            .push_bytes(&mut mapped.writer, input.to_string().as_bytes())
            .unwrap();
        let doc = Document::parse(&mut mapped.writer, source, 4096).unwrap();
        let media = Registry::new(&mut mapped.writer).unwrap();
        let result = RequestFence::default().end(
            RequestView {
                document: &doc,
                media: &media,
                operation: Operation::Generate,
            },
            &ChatCapability,
        );
        if accepted {
            let output = Egress {
                destination: "chat",
                upstream_stream: false,
            }
            .encode(&result.unwrap(), &mut mapped.writer)
            .unwrap();
            let output: serde_json::Value = serde_json::from_slice(&bytes(&output)).unwrap();
            let part = &output["messages"][0]["content"][0];
            if kind == "image" {
                assert_eq!(part["image_url"]["url"], "https://example.invalid/a");
            } else {
                assert_eq!(part["file"]["file_id"], "id");
            }
        } else {
            assert!(matches!(result, Err(codec::Error::Unsupported)));
        }
    }
}

#[test]
fn collision_parent_deletion_keeps_surviving_part_and_message_provenance() {
    collision_mutation(false);
}

#[test]
fn collision_direct_replacement_wins_without_duplicate_keys() {
    collision_mutation(true);
}

fn collision_mutation(replace: bool) {
    use super::{ChatCapability, Egress, Operation, RequestEgress, bytes, codec};
    use codec::{
        RequestFence, RequestView,
        document::{Document, Value},
        media::Registry,
        source::Source,
    };
    use serde_json::json;
    let part = |id: &str| json!({"type":"image_url","transcript":id,"source":id,"image_url":{"url":"https://example.invalid/a","ref":id}});
    let input = json!({"model":"m","messages":[{"role":"user","content":[part("a"),part("b")]},{"role":"user","content":[part("c"),part("d")]}]});
    let (mut mapped, doc, _) = super::request_controls::canonical(&input, &ChatCapability);
    let mut empty = Source::new(&mut mapped.writer).unwrap();
    empty.push_bytes(&mut mapped.writer, b"{}").unwrap();
    let mut imported = Document::parse(&mut mapped.writer, empty, 16384).unwrap();
    let root = imported.absorb(doc).unwrap();
    imported.transplant(imported.root(), root).unwrap();
    let mut doc = imported;
    let media = Registry::new(&mut mapped.writer).unwrap();
    let messages = doc.required(doc.root(), "messages").unwrap();
    let first = doc.children(messages).unwrap().next().unwrap();
    let content = doc.required(first, "content").unwrap();
    let removed = doc.children(content).unwrap().next().unwrap();
    if replace {
        doc.set(removed, "transcript", Value::Text("replacement")).unwrap();
    } else {
        doc.detach(content, removed).unwrap();
    }
    let encode = |doc: &Document, writer: &mut ck_stream::Writer| {
        let ready = RequestFence::default()
            .end(
                RequestView {
                    document: doc,
                    media: &media,
                    operation: Operation::Generate,
                },
                &ChatCapability,
            )
            .unwrap();
        let output = Egress {
            destination: "chat",
            upstream_stream: false,
        }
        .encode(&ready, writer)
        .unwrap();
        let raw = bytes(&output);
        let mut check = Source::new(writer).unwrap();
        check.push_bytes(writer, &raw).unwrap();
        Document::parse(writer, check, 16384).expect("duplicate output key");
        serde_json::from_slice::<serde_json::Value>(&raw).unwrap()
    };
    let output = encode(&doc, &mut mapped.writer);
    if replace {
        assert_eq!(output["messages"][0]["content"][0]["transcript"], "replacement");
    } else {
        assert_eq!(output["messages"][0]["content"][0], part("b"));
        doc.detach(messages, first).unwrap();
        let output = encode(&doc, &mut mapped.writer);
        assert_eq!(output["messages"][0]["content"], json!([part("c"), part("d")]));
    }
}

#[test]
fn trusted_ingress_source_kind_replacement_changes_native_carrier_in_both_modes() {
    use super::{ChatCapability, Egress, Operation, RequestEgress, bytes, codec};
    use codec::{RequestFence, RequestView, document::Value};
    use serde_json::json;
    let input = json!({"model":"m","messages":[{"role":"user","content":[
        {"type":"image_url","image_url":{"url":"https://example.invalid/old","vendor":false}},
        {"type":"file","file":{"file_id":"old-id","filename":"old.pdf","vendor":[null,2]}},
        {"type":"image_url","image_url":{"url":"data:image/png;base64,AQID"}},
        {"type":"file","file":{"file_data":"data:application/pdf;base64,BAUG","filename":"old-data.pdf"}}
    ]}]});
    let (mut mapped, mut doc, registry) = super::request_controls::canonical(&input, &ChatCapability);
    let messages = doc.required(doc.root(), "messages").unwrap();
    let message = doc.children(messages).unwrap().next().unwrap();
    let content = doc.required(message, "content").unwrap();
    let parts: Vec<_> = doc.children(content).unwrap().collect();
    let source = doc.required(parts[0], "source").unwrap();
    doc.set(parts[0], "type", Value::Text("document")).unwrap();
    doc.set(source, "kind", Value::Text("file")).unwrap();
    doc.remove(source, "uri").unwrap();
    doc.set(source, "file_id", Value::Text("new-id")).unwrap();
    doc.set(parts[0], "filename", Value::Text("new.pdf")).unwrap();
    let source = doc.required(parts[1], "source").unwrap();
    doc.set(parts[1], "type", Value::Text("image")).unwrap();
    doc.set(source, "kind", Value::Text("uri")).unwrap();
    doc.remove(source, "file_id").unwrap();
    doc.remove(parts[1], "filename").unwrap();
    doc.set(source, "uri", Value::Text("https://example.invalid/new"))
        .unwrap();
    for (index, kind, key, replacement) in [
        (2, "uri", "uri", "https://example.invalid/replaced"),
        (3, "file", "file_id", "replaced-file"),
    ] {
        let source = doc.required(parts[index], "source").unwrap();
        doc.remove(source, "ref").unwrap().expect("trusted embedded reference");
        doc.set(source, "kind", Value::Text(kind)).unwrap();
        doc.set(source, key, Value::Text(replacement)).unwrap();
        doc.remove(parts[index], "mime").unwrap();
        doc.remove(parts[index], "filename").unwrap();
    }
    let retained = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert!(retained.contains("AQID") && retained.contains("BAUG"));
    for upstream_stream in [false, true] {
        let ready = RequestFence::default()
            .end(
                RequestView {
                    document: &doc,
                    media: &registry,
                    operation: Operation::Generate,
                },
                &ChatCapability,
            )
            .unwrap();
        let output = Egress {
            destination: "chat",
            upstream_stream,
        }
        .encode(&ready, &mut mapped.writer)
        .unwrap();
        let raw = bytes(&output);
        let mut source = codec::source::Source::new(&mut mapped.writer).unwrap();
        source.push_bytes(&mut mapped.writer, &raw).unwrap();
        codec::document::Document::parse(&mut mapped.writer, source, 1_000_000).expect("duplicate output key");
        let output: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        let parts = output["messages"][0]["content"].as_array().unwrap();
        assert_eq!(parts[0]["type"], "file");
        assert_eq!(parts[0]["file"]["file_id"], "new-id");
        assert_eq!(parts[0]["file"]["filename"], "new.pdf");
        assert!(parts[0].get("image_url").is_none());
        assert_eq!(parts[1]["type"], "image_url");
        assert_eq!(parts[1]["image_url"]["url"], "https://example.invalid/new");
        assert!(parts[1].get("file").is_none());
        assert_eq!(
            parts[2],
            json!({"type":"image_url","image_url":{"url":"https://example.invalid/replaced"}})
        );
        assert_eq!(parts[3], json!({"type":"file","file":{"file_id":"replaced-file"}}));
        assert!(!String::from_utf8_lossy(&raw).contains("AQID"));
        assert!(!String::from_utf8_lossy(&raw).contains("BAUG"));
        assert!(!String::from_utf8_lossy(&raw).contains("old"));
    }
}
