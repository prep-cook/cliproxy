//! Media registrations and codecs stay bound to the document and writer that own them.
use super::*;

#[test]
fn media_registration_rejects_replacement_and_another_document() {
    use codec::{
        RequestFence, RequestView,
        document::{Document, Value},
        media::{Encoding, Registry},
        source::Source,
    };
    let mut mapped = Mapped::new();
    let mut source = Source::new(&mut mapped.writer).unwrap();
    source.push_bytes(&mut mapped.writer, br#"{"model":"m","x_ck":{},"messages":[{"role":"user","content":[{"type":"audio","source":{"kind":"data","ref":"AQID","format":"wav"}}]}]}"#).unwrap();
    let mut doc = Document::parse(&mut mapped.writer, source, 4096).unwrap();
    let messages = doc.required(doc.root(), "messages").unwrap();
    let message = doc.children(messages).unwrap().next().unwrap();
    let content = doc.required(message, "content").unwrap();
    let part = doc.children(content).unwrap().next().unwrap();
    let source = doc.required(part, "source").unwrap();
    let reference = doc.required(source, "ref").unwrap();
    let Value::String(text) = doc.node(reference).unwrap().value else {
        panic!("non-string reference")
    };
    let mut media = Registry::new(&mut mapped.writer).unwrap();
    media
        .register(&mut doc, reference, text, Encoding::Base64, "audio", None)
        .unwrap();
    let mut other = Source::new(&mut mapped.writer).unwrap();
    other
        .push_bytes(&mut mapped.writer, br#"{"model":"m","x_ck":{},"messages":[]}"#)
        .unwrap();
    let other = Document::parse(&mut mapped.writer, other, 4096).unwrap();
    assert!(
        RequestFence::default()
            .end(
                RequestView {
                    document: &other,
                    media: &media,
                    operation: Operation::Generate
                },
                &ChatCapability
            )
            .is_err()
    );
    doc.replace(reference, Value::Text("replacement")).unwrap();
    assert!(
        RequestFence::default()
            .end(
                RequestView {
                    document: &doc,
                    media: &media,
                    operation: Operation::Generate,
                },
                &ChatCapability,
            )
            .is_err()
    );
    doc.remove(message, "content").unwrap();
    let ready = RequestFence::default()
        .end(
            RequestView {
                document: &doc,
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
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    let output: serde_json::Value = serde_json::from_slice(&bytes(&output)).unwrap();
    assert!(output["messages"][0].get("content").is_none());
}

#[test]
fn codecs_reject_foreign_owners_without_publishing_errors() {
    use codec::{ResponseEgress, ResponseIngress};
    use std::task::Poll;
    let mut mapped = Mapped::new();
    let mut other = mapped.store.create().unwrap();
    let mut request = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        1024,
    )
    .unwrap();
    let mut response = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        codec::BodyMode::Streaming,
        1024,
    )
    .unwrap();
    let mut egress = codec::chat::egress::Egress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        Operation::Generate,
        codec::BodyMode::Streaming,
        true,
        |_| {},
    )
    .unwrap();
    let before = other.committed();
    assert!(matches!(
        request.advance(&mut other, b"{}"),
        Err(codec::Error::Position)
    ));
    assert!(matches!(
        request.finish(&mut other, &ChatCapability),
        Err(codec::Error::Position)
    ));
    assert!(matches!(
        response.advance(&mut other, b"data: {}\n\n"),
        Err(codec::Error::Position)
    ));
    assert!(matches!(response.finish(&mut other), Err(codec::Error::Position)));
    assert_eq!(other.committed(), before);
    let mut reader = mapped.store.reader(other.id(), 0).unwrap();
    let mut foreign = codec::chat::response::Ingress::new(
        &mut other,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        codec::BodyMode::Streaming,
        1024,
    )
    .unwrap();
    foreign.advance(&mut other, b"data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\n").unwrap();
    let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() else {
        panic!("missing response start")
    };
    assert!(matches!(
        egress.advance(&mut other, lease, |_| panic!("foreign output")),
        Err(codec::Error::Position)
    ));
}
