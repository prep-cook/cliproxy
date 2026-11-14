use super::*;

#[derive(Clone, Copy)]
pub(super) enum Component {
    Name(Name),
    Index(u64),
}
fn pointer(doc: &Document, path: &[Option<Component>], e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"\"")?;
    for component in path.iter().flatten() {
        e.bytes(b"/")?;
        match component {
            Component::Index(i) => e.unsigned(*i)?,
            Component::Name(name) => {
                let mut decoded = doc.name_chars(*name)?;
                while let Some(c) = decoded.next_scalar()? {
                    pointer_scalar(c, e)?;
                }
            }
        }
    }
    e.bytes(b"\"")
}
fn pointer_scalar(c: char, e: &mut Encoder<'_, '_>) -> Result<()> {
    match c {
        '~' => e.bytes(b"~0"),
        '/' => e.bytes(b"~1"),
        c => e.escaped(c.encode_utf8(&mut [0; 4])),
    }
}
pub(super) fn field(
    doc: &mut Document,
    media: &Registry,
    node: usize,
    path: &[Option<Component>],
    emitter: &mut Emitter,
    owner: &mut Writer,
    reader: &mut ck_stream::Reader,
) -> Result<bool> {
    let registered = media.get(doc, node)?;
    if doc.published(node)? {
        return Ok(registered.is_some());
    }
    super::super::request_provenance::capture(doc, owner, node)?;
    if let Some(parent) = doc.node(node)?.parent {
        field(doc, media, parent, &[], emitter, owner, reader)?;
        if matches!(doc.node(parent)?.value, Value::Array) {
            // Published children form a prefix; resume after it instead of rescanning.
            let mut prior = doc.unpublished_from(parent)?;
            while let Some(sibling) = prior {
                if sibling == node {
                    break;
                }
                prior = doc.node(sibling)?.next;
                field(doc, media, sibling, &[], emitter, owner, reader)?;
                doc.mark_published_through(parent, sibling);
            }
        }
    }
    emitter.emit(
        owner,
        Kind::RequestField,
        Scope::default(),
        |e| {
            e.bytes(b",\"pointer\":")?;
            if path.is_empty() || super::super::request_provenance::preserved(doc, node)? {
                crate::codec::provenance::pointer(doc, node, e)?;
            } else {
                pointer(doc, path, e)?;
            }
            e.bytes(b",\"value\":")?;
            if let Some(m) = registered {
                return media::write_reference(m, e);
            }
            match doc.node(node)?.value {
                Value::Object => e.bytes(b"{}"),
                Value::Array => e.bytes(b"[]"),
                _ => doc.write(node, e),
            }
        },
        |_| Ok(()),
    )?;
    let (kind, event) = codec::internal::next(owner, reader)?;
    if kind != Kind::RequestField {
        return Err(Error::Schema);
    }
    doc.bind_field(node, event)?;
    doc.mark_published(node);
    if let Some(parent) = doc.node(node)?.parent
        && matches!(doc.node(parent)?.value, Value::Array)
        && doc.unpublished_from(parent)? == Some(node)
    {
        doc.mark_published_through(parent, node);
    }
    Ok(registered.is_some())
}

pub(super) fn fields(
    doc: &mut Document,
    media: &Registry,
    node: usize,
    (path, depth): (&mut [Option<Component>; 128], usize),
    emitter: &mut Emitter,
    owner: &mut Writer,
    reader: &mut ck_stream::Reader,
) -> Result<()> {
    if depth == path.len() {
        return Err(Error::Limit);
    }
    if field(doc, media, node, &path[..depth], emitter, owner, reader)? {
        return Ok(());
    }
    let mut child = doc.node(node)?.first;
    let mut index = 0;
    while let Some(current) = child {
        child = doc.node(current)?.next;
        path[depth] = Some(if matches!(doc.node(node)?.value, Value::Array) {
            Component::Index(index)
        } else {
            Component::Name(doc.node(current)?.name.ok_or(Error::Schema)?)
        });
        fields(doc, media, current, (path, depth + 1), emitter, owner, reader)?;
        index += 1;
    }
    Ok(())
}
