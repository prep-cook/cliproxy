use super::{Mapped, Operation, codec, identity};
use codec::{
    Capability, RequestFence, RequestView,
    document::Document,
    event::{Direction, Emitter, Kind, Scope},
    media::Registry,
    source::Source,
};

struct Any;
impl Capability for Any {
    fn destination(&self) -> &'static str {
        "bindings-test"
    }
    fn validate(&self, _: RequestView<'_>) -> codec::Result<()> {
        Ok(())
    }
}

const NATIVE: &str = r#"{"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"a"},{"type":"server_tool_result","content":{"type":"web_search_result","url":"u"}}]},{"role":"assistant","content":"b"}],"x_ck":{"anthropic_order":[{"kind":"text"},{"kind":"result"},{"kind":"message"}],"original":[{"protocol":"anthropic","pointer":"/messages/0/content/1/zz","value":1}]}}"#;

fn node(doc: &Document, path: &[&str]) -> usize {
    let mut at = doc.root();
    for segment in path {
        at = match segment.parse::<usize>() {
            Ok(index) => doc.children(at).unwrap().nth(index).unwrap(),
            Err(_) => doc.required(at, segment).unwrap(),
        };
    }
    at
}

/// Serialize bindings with the shared helper, publish the document as canonical request
/// records, then reconstruct a new Document from those records alone.
fn reconstruct(bind: impl Fn(&mut Document)) -> codec::Result<(Mapped, Document)> {
    let mut mapped = Mapped::new();
    let mut source = Source::new(&mut mapped.writer).unwrap();
    source.push_bytes(&mut mapped.writer, NATIVE.as_bytes()).unwrap();
    let mut doc = Document::parse(&mut mapped.writer, source, NATIVE.len() as u64).unwrap();
    bind(&mut doc);
    codec::provenance::bindings(&mut doc, &mut mapped.writer).unwrap();
    let media = Registry::new(&mut mapped.writer).unwrap();
    let view = RequestView {
        document: &doc,
        media: &media,
        operation: Operation::Generate,
    };
    let mut emitter = Emitter::new(identity(Direction::Request));
    emitter.request_start(&mut mapped.writer, view).unwrap();
    emitter
        .emit(
            &mut mapped.writer,
            Kind::RequestField,
            Scope::default(),
            |e| {
                e.bytes(b",\"pointer\":\"\",\"value\":")?;
                doc.write(doc.root(), e)
            },
            |_| Ok(()),
        )
        .unwrap();
    let ready = RequestFence::default().end(view, &Any).unwrap();
    emitter.request_end(&mut mapped.writer, &ready).unwrap();
    drop(doc);
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let (restored, _) = codec::chat::request::read_request(&mut mapped.writer, &mut reader)?;
    Ok((mapped, restored))
}

#[test]
fn anthropic_order_and_shape_bindings_survive_canonical_reconstruction() {
    let (_mapped, doc) = reconstruct(|doc| {
        let x = doc.required(doc.root(), "x_ck").unwrap();
        let order = doc.required(x, "anthropic_order").unwrap();
        let entries: Vec<_> = doc.children(order).unwrap().collect();
        let text = node(doc, &["messages", "0", "content", "0"]);
        let result = node(doc, &["messages", "0", "content", "1", "content"]);
        let message = node(doc, &["messages", "1"]);
        doc.bind_origin(entries[0], text, None).unwrap();
        doc.bind_origin(entries[1], result, Some("shape")).unwrap();
        doc.bind_origin(entries[2], message, None).unwrap();
        let original = doc.required(x, "original").unwrap();
        let entry = doc.children(original).unwrap().next().unwrap();
        doc.bind_origin(entry, node(doc, &["messages", "0", "content", "1"]), None)
            .unwrap();
    })
    .unwrap();
    let x = doc.required(doc.root(), "x_ck").unwrap();
    let order: Vec<_> = doc
        .children(doc.required(x, "anthropic_order").unwrap())
        .unwrap()
        .collect();
    assert_eq!(order.len(), 3);
    for (entry, kind) in order.iter().zip(["text", "result", "message"]) {
        assert!(doc.string_is(doc.required(*entry, "kind").unwrap(), kind).unwrap());
    }
    let expected = [
        (node(&doc, &["messages", "0", "content", "0"]), None),
        (node(&doc, &["messages", "0", "content", "1", "content"]), Some("shape")),
        (node(&doc, &["messages", "1"]), None),
    ];
    for (entry, expected) in order.iter().zip(expected) {
        assert_eq!(doc.origin(*entry).unwrap(), Some(expected));
    }
    let original = doc
        .children(doc.required(x, "original").unwrap())
        .unwrap()
        .next()
        .unwrap();
    let part = node(&doc, &["messages", "0", "content", "1"]);
    assert_eq!(doc.origin(original).unwrap(), Some((part, None)));
    assert!(
        doc.string_is(doc.required(original, "protocol").unwrap(), "anthropic")
            .unwrap()
    );
}

#[test]
fn unsupported_binding_carrier_is_rejected_on_reconstruction() {
    let result = reconstruct(|doc| {
        let x = doc.required(doc.root(), "x_ck").unwrap();
        let order = doc.required(x, "anthropic_order").unwrap();
        let entry = doc.children(order).unwrap().next().unwrap();
        let text = node(doc, &["messages", "0", "content", "0"]);
        doc.bind_origin(entry, text, Some("unknown_carrier")).unwrap();
    });
    assert!(matches!(result, Err(codec::Error::Schema)));
}

#[test]
fn repeated_request_fields_replace_object_members_once_indexed() {
    let mut mapped = Mapped::new();
    let mut source = Source::new(&mut mapped.writer).unwrap();
    source.push_bytes(&mut mapped.writer, b"{}").unwrap();
    let doc = Document::parse(&mut mapped.writer, source, 2).unwrap();
    let media = Registry::new(&mut mapped.writer).unwrap();
    let view = RequestView {
        document: &doc,
        media: &media,
        operation: Operation::Generate,
    };
    let mut emitter = Emitter::new(identity(Direction::Request));
    emitter.request_start(&mut mapped.writer, view).unwrap();
    // Members arrive inside a value, as new fields, as replacements, and again after their
    // object's value is replaced.
    for (pointer, value) in [
        ("", r#"{"x_ck":{},"metadata":{"a":1}}"#),
        ("/metadata/b", "2"),
        ("/metadata/a", "3"),
        ("/metadata/b", "4"),
        ("/metadata", r#"{"c":7}"#),
        ("/metadata/c", "8"),
        ("/metadata/d", "9"),
        ("/metadata/d", "10"),
        ("/metadata/a", "11"),
    ] {
        emitter
            .emit(
                &mut mapped.writer,
                Kind::RequestField,
                Scope::default(),
                |e| {
                    e.bytes(b",\"pointer\":")?;
                    e.string(pointer)?;
                    e.bytes(b",\"value\":")?;
                    e.bytes(value.as_bytes())
                },
                |_| Ok(()),
            )
            .unwrap();
    }
    let ready = RequestFence::default().end(view, &Any).unwrap();
    emitter.request_end(&mut mapped.writer, &ready).unwrap();
    drop(doc);
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let (restored, _) = codec::chat::request::read_request(&mut mapped.writer, &mut reader).unwrap();
    let metadata = restored.required(restored.root(), "metadata").unwrap();
    let members: Vec<_> = restored.children(metadata).unwrap().collect();
    assert_eq!(members.len(), 3);
    for (member, (name, value)) in members.into_iter().zip([("c", 8), ("d", 10), ("a", 11)]) {
        let node = restored.node(member).unwrap();
        assert!(restored.name_is(node.name.unwrap(), name).unwrap());
        assert_eq!(restored.unsigned(member).unwrap(), value);
    }
}
