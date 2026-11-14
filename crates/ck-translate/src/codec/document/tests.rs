use super::{Document, LocalId, Node, Source, Value};

#[test]
fn id_values_keep_nodes_small() {
    assert_eq!(std::mem::size_of::<Value>(), 32);
    assert_eq!(std::mem::size_of::<Node>(), 248);
}

fn text(doc: &Document, node: usize) -> String {
    let mut chars = doc.chars(node).unwrap();
    let mut text = String::new();
    while let Some(c) = chars.next_scalar().unwrap() {
        text.push(c);
    }
    text
}

/// Ids live in the document's id table, charged with its other tables, and keep their text
/// when another document is absorbed.
#[test]
fn ids_are_charged_and_survive_absorb() {
    let directory = std::env::temp_dir().join(format!("ck-document-{}", uuid::Uuid::new_v4()));
    let mut config = ck_stream::Config::new(directory.clone());
    config.pool_bytes = Some(8 * ck_stream::FRAME_BYTES);
    config.max_event_bytes = ck_stream::FRAME_BYTES;
    config.header_slots = 4;
    config.workers = 1;
    let mut store =
        ck_stream::Store::with_clock(config, std::sync::Arc::new(ck_stream::ManualClock::default())).unwrap();
    let mut writer = store.create().unwrap();
    let mut doc = Document::streaming(&mut writer).unwrap();
    let a = doc.set_id(doc.root(), "a", LocalId::new("alpha").unwrap()).unwrap();
    let mut other = Document::streaming(&mut writer).unwrap();
    let b = other.set_id(other.root(), "b", LocalId::new("beta").unwrap()).unwrap();
    assert_eq!(text(&other, b), "beta");
    let root = doc.absorb(other).unwrap();
    let b = doc.member(root, "b").unwrap().unwrap();
    assert_eq!((text(&doc, a), text(&doc, b)), ("alpha".into(), "beta".into()));
    assert!(doc.ids.capacity() >= 2);
    assert_eq!(
        doc.metadata.bytes(),
        doc.nodes.capacity() * std::mem::size_of::<Node>()
            + doc.sources.capacity() * std::mem::size_of::<Source>()
            + doc.ids.capacity() * std::mem::size_of::<LocalId>()
    );
    drop((doc, writer));
    store.shutdown().unwrap();
    std::fs::remove_dir_all(&directory).unwrap();
}
