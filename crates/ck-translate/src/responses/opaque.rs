use crate::codec::{
    Error, Result,
    document::{Document, Value},
    encode::Encoder,
};

pub(super) fn write(doc: &Document, e: &mut Encoder<'_, '_>) -> Result<()> {
    let root = doc.root();
    let x = doc.required(root, "x_ck")?;
    if super::is(doc, root, "name", "responses.fields")? {
        e.bytes(b"event: x_ck_opaque\ndata: {\"type\":\"x_ck_opaque\",\"name\":\"responses.fields\",\"value\":")?;
        doc.write(doc.required(root, "value")?, e)?;
        if let Some(source) = doc.member(x, "source")? {
            e.bytes(b",\"source\":")?;
            doc.write(source, e)?;
        }
        return e.bytes(b"}\n\n");
    }
    if let Some(source) = doc.member(x, "source")?
        && let Some(fields) = doc.member(source, "sse")?
    {
        for field in doc.children(fields)? {
            let name = doc.required(field, "name")?;
            if doc.string_is(name, "data")? {
                continue;
            }
            if doc
                .member(field, "comment")?
                .is_some_and(|n| matches!(doc.node(n).map(|n| n.value), Ok(Value::Bool(true))))
            {
                e.bytes(b":")?;
            } else {
                line(doc, name, e)?;
                e.bytes(b":")?;
            }
            e.bytes(b" ")?;
            line(doc, doc.required(field, "value")?, e)?;
            e.bytes(b"\n")?;
        }
    }
    e.bytes(b"data: ")?;
    let value = doc.required(root, "value")?;
    if super::is(doc, root, "format", "text")? {
        let mut chars = doc.chars(value)?;
        while let Some(c) = chars.next_scalar()? {
            match c {
                '\n' => e.bytes(b"\ndata: ")?,
                '\r' => return Err(Error::Schema),
                c => e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?,
            }
        }
    } else {
        doc.write(value, e)?;
    }
    e.bytes(b"\n\n")
}
fn line(doc: &Document, node: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        if matches!(c, '\r' | '\n') {
            return Err(Error::Schema);
        }
        e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
    }
    Ok(())
}

/// Buffered Responses has no event sequence; the codec defines a non-control carrier.
pub(super) fn retain(doc: &mut Document, incoming: Document) -> Result<()> {
    let event = doc.absorb(incoming)?;
    let entry = doc.add(None, Value::Object)?;
    let array = match doc.member(doc.root(), "x_ck_opaque")? {
        Some(n) => n,
        None => doc.set(doc.root(), "x_ck_opaque", Value::Array)?,
    };
    doc.append(array, entry)?;
    for name in ["name", "format", "value"] {
        super::move_field(doc, event, name, entry, name)?;
    }
    let x = doc.required(event, "x_ck")?;
    super::move_field(doc, x, "source", entry, "source")?;
    Ok(())
}

pub(super) fn preserve_carrier_collision(doc: &mut Document, incoming: usize) -> Result<()> {
    let Some(carrier) = doc.member(doc.root(), "x_ck_opaque")? else {
        return Ok(());
    };
    let Some(native) = doc.remove(incoming, "x_ck_opaque")? else {
        return Ok(());
    };
    let entry = doc.add(None, Value::Object)?;
    doc.append(carrier, entry)?;
    doc.set(entry, "name", Value::Text("responses.fields"))?;
    doc.set(entry, "format", Value::Text("json"))?;
    let value = doc.set(entry, "value", Value::Object)?;
    doc.append(value, native)?;
    let source = doc.set(entry, "source", Value::Object)?;
    doc.set(source, "protocol", Value::Text("responses"))?;
    Ok(())
}
