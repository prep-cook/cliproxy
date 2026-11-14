//! Trusted request records rebuilt into a Document with registered media: unchanged payloads
//! reach request egress, edits to the current MIME win, and replaced payloads are registered.
use super::{ChatCapability, Egress, Mapped, RequestEgress, bytes, codec, request_with_frames};
use codec::{
    Operation, RequestFence, RequestView,
    document::{Document, Value},
    media::{Encoding, Registry},
};
use serde_json::{Value as Json, json};

fn input() -> Json {
    json!({"model":"m","messages":[{"role":"user","content":[
        {"type":"text","text":"t".repeat(3000)},
        {"type":"image_url","image_url":{"url":"data:image/png;base64,AQID","detail":"low"}},
        {"type":"image_url","image_url":{"url":"data:image/png;base64,BAUG"}},
        {"type":"file","file":{"file_data":"data:application/pdf;base64,BwgJ","filename":"a.pdf"}},
        {"type":"file","file":{"file_data":"CgsM"}},
        {"type":"input_audio","input_audio":{"data":"DQ4P","format":"wav"}}
    ]}],"tools":[{"type":"function","function":{"name":"f","parameters":{"type":"object","properties":{"x":{"type":"x_ck_media","media_id":"m1","value_kind":"utf8"}}}}}]})
}

fn rebuilt(input: &Json) -> (Mapped, Document, Registry) {
    super::request_controls::canonical(input, &ChatCapability)
}

fn encode(mapped: &mut Mapped, doc: &Document, media: &Registry, upstream_stream: bool) -> Json {
    let view = RequestView {
        document: doc,
        media,
        operation: Operation::Generate,
    };
    let ready = RequestFence::default().end(view, &ChatCapability).unwrap();
    let lease = Egress {
        destination: "chat",
        upstream_stream,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    serde_json::from_slice(&bytes(&lease)).unwrap()
}

fn parts(doc: &Document) -> Vec<usize> {
    let messages = doc.required(doc.root(), "messages").unwrap();
    let message = doc.children(messages).unwrap().next().unwrap();
    let content = doc.required(message, "content").unwrap();
    doc.children(content).unwrap().collect()
}

#[test]
fn rebuilt_request_media_reach_egress_like_live_ingress() {
    let input = input();
    let (mut live, _) = request_with_frames(input.to_string().as_bytes(), 512);
    live.as_object_mut().unwrap().remove("stream");
    let (mut mapped, doc, media) = rebuilt(&input);
    for upstream_stream in [false, true] {
        let mut output = encode(&mut mapped, &doc, &media, upstream_stream);
        // A literal reference shape in client data is never adopted as media.
        assert_eq!(output["tools"], input["tools"]);
        assert_eq!(
            output.as_object_mut().unwrap().remove("stream"),
            Some(json!(upstream_stream))
        );
        assert_eq!(output, live, "{upstream_stream}");
    }
}

#[test]
fn rebuilt_request_egress_writes_the_current_mime() {
    for (edit, first, file) in [
        (
            Some("image/jpeg"),
            "data:image/jpeg;base64,AQID",
            "data:text/plain;base64,BwgJ",
        ),
        (None, "data:;base64,AQID", "data:;base64,BwgJ"),
    ] {
        let (mut mapped, mut doc, media) = rebuilt(&input());
        let parts = parts(&doc);
        for (part, replacement) in [(parts[1], edit), (parts[3], edit.map(|_| "text/plain"))] {
            match replacement {
                Some(mime) => {
                    doc.set(part, "mime", Value::Text(mime)).unwrap();
                }
                None => {
                    doc.remove(part, "mime").unwrap().unwrap();
                }
            }
        }
        for upstream_stream in [false, true] {
            let output = encode(&mut mapped, &doc, &media, upstream_stream);
            let content = &output["messages"][0]["content"];
            assert_eq!(content[1]["image_url"]["url"], first, "{edit:?}");
            assert_eq!(content[2]["image_url"]["url"], "data:image/png;base64,BAUG");
            assert_eq!(content[3]["file"]["file_data"], file, "{edit:?}");
            assert_eq!(content[4]["file"]["file_data"], "CgsM");
            assert_eq!(content[5]["input_audio"]["data"], "DQ4P");
        }
    }
}

#[test]
fn replaced_request_payload_is_registered_and_old_bytes_stay_unreachable() {
    let (mut mapped, mut doc, mut media) = rebuilt(&input());
    let replacement = "\"data:image/gif;base64,R0lG\"";
    let mut source = codec::source::Source::new(&mut mapped.writer).unwrap();
    source.push_bytes(&mut mapped.writer, replacement.as_bytes()).unwrap();
    let parsed = Document::parse(&mut mapped.writer, source, replacement.len() as u64).unwrap();
    let string = doc.absorb(parsed).unwrap();
    let Value::String(text) = doc.node(string).unwrap().value else {
        panic!("replacement is not a string");
    };
    let part = parts(&doc)[1];
    let source = doc.required(part, "source").unwrap();
    doc.remove(source, "ref").unwrap().unwrap();
    let reference = doc.set(source, "ref", Value::Null).unwrap();
    let mime = doc.set(part, "mime", Value::Text("image/gif")).unwrap();
    media
        .register(
            &mut doc,
            reference,
            text,
            Encoding::DataUri { skip: 22 },
            "image",
            Some(mime),
        )
        .unwrap();
    for upstream_stream in [false, true] {
        let output = encode(&mut mapped, &doc, &media, upstream_stream);
        let content = &output["messages"][0]["content"];
        assert_eq!(
            content[1]["image_url"],
            json!({"url":"data:image/gif;base64,R0lG","detail":"low"})
        );
        assert_eq!(content[2]["image_url"]["url"], "data:image/png;base64,BAUG");
        assert!(!output.to_string().contains("AQID"));
    }
}

#[test]
fn complete_string_payload_egress_uses_the_current_mime() {
    let (mut mapped, mut doc, mut media) = rebuilt(&input());
    let replacement = "\"data:image/png;base64,AQID\"";
    let mut source = codec::source::Source::new(&mut mapped.writer).unwrap();
    source.push_bytes(&mut mapped.writer, replacement.as_bytes()).unwrap();
    let parsed = Document::parse(&mut mapped.writer, source, replacement.len() as u64).unwrap();
    let string = doc.absorb(parsed).unwrap();
    let Value::String(text) = doc.node(string).unwrap().value else {
        panic!("replacement is not a string");
    };
    let part = parts(&doc)[2];
    let source = doc.required(part, "source").unwrap();
    doc.remove(source, "ref").unwrap().unwrap();
    let reference = doc.set(source, "ref", Value::Null).unwrap();
    let mime = doc.set(part, "mime", Value::Text("image/png")).unwrap();
    media
        .register(
            &mut doc,
            reference,
            text,
            Encoding::DataUri { skip: 22 },
            "image",
            Some(mime),
        )
        .unwrap();
    doc.set(part, "mime", Value::Text("image/jpeg")).unwrap();
    for upstream_stream in [false, true] {
        let output = encode(&mut mapped, &doc, &media, upstream_stream);
        let url = &output["messages"][0]["content"][2]["image_url"]["url"];
        assert_eq!(url, "data:image/jpeg;base64,AQID");
    }
}

#[test]
fn plain_base64_file_data_carries_a_supplied_mime() {
    let (mut mapped, mut doc, media) = rebuilt(&input());
    let parts = parts(&doc);
    doc.set(parts[4], "mime", Value::Text("application/pdf")).unwrap();
    for upstream_stream in [false, true] {
        let output = encode(&mut mapped, &doc, &media, upstream_stream);
        let content = &output["messages"][0]["content"];
        assert_eq!(content[4]["file"]["file_data"], "data:application/pdf;base64,CgsM");
        // Audio carries its media type in `format`, never in the payload.
        assert_eq!(content[5]["input_audio"]["data"], "DQ4P");
    }
}

/// Republishes the canonical records of `input` with `transform` applied, then rebuilds them.
fn rebuilt_with(input: &Json, transform: impl Fn(codec::event::Kind, &mut Document)) -> codec::Result<()> {
    use codec::RequestIngress;
    use std::task::Poll;
    let mut mapped = Mapped::configured(std::sync::Arc::new(ck_stream::ManualClock::default()), 256);
    let mut ingress = super::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        super::identity(codec::event::Direction::Request),
        Operation::Generate,
        1024 * 1024,
    )
    .unwrap();
    ingress
        .advance(&mut mapped.writer, input.to_string().as_bytes())
        .unwrap();
    ingress.finish(&mut mapped.writer, &ChatCapability).unwrap();
    drop(ingress);
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let mut target = mapped.store.create().unwrap();
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        let record = lease.record();
        let (kind, mut doc) = codec::internal::decode(&mut mapped.writer, lease).unwrap();
        transform(kind, &mut doc);
        codec::encode::publish(&mut target, record.kind, 0, |e| {
            if let Some(name) = kind.name() {
                e.bytes(b"event: ")?;
                e.bytes(name.as_bytes())?;
                e.bytes(b"\n")?;
            }
            e.bytes(b"id: ")?;
            doc.write(doc.required(doc.required(doc.root(), "x_ck")?, "seq")?, e)?;
            e.bytes(b"\ndata: ")?;
            doc.write(doc.root(), e)?;
            e.bytes(b"\n\n")
        })
        .unwrap();
    }
    let mut reader = mapped.store.reader(target.id(), 0).unwrap();
    codec::chat::request::read_request(&mut target, &mut reader).map(drop)
}

#[test]
fn rebuilt_request_rejects_media_end_that_does_not_complete_its_data() {
    let input = json!({"model":"m","messages":[{"role":"user","content":[
        {"type":"image_url","image_url":{"url":"data:image/png;base64,AQID"}}]}]});
    rebuilt_with(&input, |_, _| ()).unwrap();
    for (bytes, status) in [(999, "completed"), (3, "incomplete"), (999, "incomplete")] {
        let result = rebuilt_with(&input, |kind, doc| {
            if kind == codec::event::Kind::MediaEnd {
                doc.set(doc.root(), "bytes", Value::Unsigned(bytes)).unwrap();
                doc.set(doc.root(), "status", Value::Text(status)).unwrap();
            }
        });
        assert!(result.is_err(), "{bytes}/{status}");
    }
    let result = rebuilt_with(&input, |kind, doc| {
        if kind == codec::event::Kind::Data {
            doc.set(doc.root(), "offset", Value::Unsigned(1)).unwrap();
        }
    });
    assert!(result.is_err(), "data offset");
}

/// Registers `literal` (a JSON string) as the payload of `part`'s source with `encoding`.
fn register_payload(
    mapped: &mut Mapped,
    doc: &mut Document,
    media: &mut Registry,
    part: usize,
    literal: &str,
    (encoding, mime): (Encoding, &'static str),
) {
    let mut source = codec::source::Source::new(&mut mapped.writer).unwrap();
    source.push_bytes(&mut mapped.writer, literal.as_bytes()).unwrap();
    let parsed = Document::parse(&mut mapped.writer, source, literal.len() as u64).unwrap();
    let string = doc.absorb(parsed).unwrap();
    let Value::String(text) = doc.node(string).unwrap().value else {
        panic!("payload is not a string");
    };
    let source = doc.required(part, "source").unwrap();
    doc.remove(source, "ref").unwrap().unwrap();
    let reference = doc.set(source, "ref", Value::Null).unwrap();
    let mime = doc.set(part, "mime", Value::Text(mime)).unwrap();
    media
        .register(doc, reference, text, encoding, "image", Some(mime))
        .unwrap();
}

#[test]
fn registered_payloads_write_as_plain_base64_in_every_original_form() {
    let (mut mapped, mut doc, mut media) = rebuilt(&input());
    let parts = parts(&doc);
    // Retained records: data URI image, data URI file, plain file and audio.
    let mut expected: Vec<_> = [(1, "AQID"), (3, "BwgJ"), (4, "CgsM"), (5, "DQ4P")]
        .into_iter()
        .map(|(index, data)| (parts[index], data))
        .collect();
    // A complete string registered as a data URI.
    register_payload(
        &mut mapped,
        &mut doc,
        &mut media,
        parts[2],
        "\"data:image/gif;base64,R0lG\"",
        (Encoding::DataUri { skip: 22 }, "image/gif"),
    );
    expected.push((parts[2], "R0lG"));
    let mut references: Vec<_> = expected
        .into_iter()
        .map(|(part, data)| {
            let source = doc.required(part, "source").unwrap();
            (doc.required(source, "ref").unwrap(), data.to_owned())
        })
        .collect();
    // Text is encoded from its UTF-8 bytes.
    use base64::Engine;
    references.push((
        doc.required(parts[0], "text").unwrap(),
        base64::engine::general_purpose::STANDARD.encode("t".repeat(3000)),
    ));
    for (reference, data) in references {
        let lease = codec::encode::scratch(&mut mapped.writer, |e| media.write_base64(&doc, reference, e)).unwrap();
        assert_eq!(serde_json::from_slice::<Json>(&bytes(&lease)).unwrap(), json!(data));
    }
}

#[test]
fn plain_base64_image_with_a_mime_reaches_chat_as_a_data_uri() {
    let (mut mapped, mut doc, mut media) = rebuilt(&input());
    let part = parts(&doc)[2];
    register_payload(
        &mut mapped,
        &mut doc,
        &mut media,
        part,
        "\"R0lG\"",
        (Encoding::Base64, "image/gif"),
    );
    for upstream_stream in [false, true] {
        let output = encode(&mut mapped, &doc, &media, upstream_stream);
        let url = &output["messages"][0]["content"][2]["image_url"]["url"];
        assert_eq!(url, "data:image/gif;base64,R0lG");
    }
}
