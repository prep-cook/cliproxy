use crate::codec::{
    Error, Result,
    document::{Document, Name, Value},
    encode::Encoder,
};
use ck_stream::Writer;

pub(super) fn array_tail(
    owner: &mut Writer,
    old: &Document,
    previous: Option<usize>,
    incoming: &Document,
    node: usize,
    snapshot: bool,
) -> Result<Document> {
    if !matches!(incoming.node(node)?.value, Value::Array) {
        return Err(Error::Schema);
    }
    let mut skip = 0;
    if snapshot && let Some(previous) = previous {
        let mut new = incoming.children(node)?;
        for item in old.children(previous)? {
            let next = new.next().ok_or(Error::Schema)?;
            if !equal(old, item, incoming, next)? {
                return Err(Error::Schema);
            }
            skip += 1;
        }
    }
    super::mapped(owner, |e| {
        e.bytes(b"[")?;
        for (i, item) in incoming.children(node)?.skip(skip).enumerate() {
            if i > 0 {
                e.bytes(b",")?;
            }
            incoming.write(item, e)?;
        }
        e.bytes(b"]")
    })
}
pub(super) fn equal(a: &Document, an: usize, b: &Document, bn: usize) -> Result<bool> {
    match (a.node(an)?.value, b.node(bn)?.value) {
        (Value::Object, Value::Object) => {
            if a.children(an)?.count() != b.children(bn)?.count() {
                return Ok(false);
            }
            for af in a.children(an)? {
                let mut found = false;
                for bf in b.children(bn)? {
                    if equal_name(
                        a,
                        a.node(af)?.name.ok_or(Error::Schema)?,
                        b,
                        b.node(bf)?.name.ok_or(Error::Schema)?,
                    )? {
                        if !equal(a, af, b, bf)? {
                            return Ok(false);
                        }
                        found = true;
                        break;
                    }
                }
                if !found {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (Value::Array, Value::Array) => {
            let mut bfields = b.children(bn)?;
            for af in a.children(an)? {
                let Some(bf) = bfields.next() else { return Ok(false) };
                if !equal(a, af, b, bf)? {
                    return Ok(false);
                }
            }
            Ok(bfields.next().is_none())
        }
        (Value::String(_) | Value::Text(_) | Value::Id(_), Value::String(_) | Value::Text(_) | Value::Id(_)) => {
            let mut a = a.chars(an)?;
            let mut b = b.chars(bn)?;
            loop {
                let c = a.next_scalar()?;
                if c != b.next_scalar()? {
                    return Ok(false);
                }
                if c.is_none() {
                    return Ok(true);
                }
            }
        }
        (Value::Number(at), Value::Number(bt)) => {
            let mut ac = a.source(at)?.cursor(at.span)?;
            let mut bc = b.source(bt)?.cursor(bt.span)?;
            loop {
                let c = ac.next_byte()?;
                if c != bc.next_byte()? {
                    return Ok(false);
                }
                if c.is_none() {
                    return Ok(true);
                }
            }
        }
        (Value::Unsigned(a), Value::Unsigned(b)) => Ok(a == b),
        (Value::Bool(a), Value::Bool(b)) => Ok(a == b),
        (Value::Null, Value::Null) => Ok(true),
        _ => Ok(false),
    }
}
fn equal_name(a: &Document, an: Name, b: &Document, bn: Name) -> Result<bool> {
    let mut a = a.name_chars(an)?;
    let mut b = b.name_chars(bn)?;
    loop {
        let c = a.next_scalar()?;
        if c != b.next_scalar()? {
            return Ok(false);
        }
        if c.is_none() {
            return Ok(true);
        }
    }
}
pub(super) fn append_array(doc: &mut Document, parent: usize, name: &'static str, incoming: Document) -> Result<()> {
    let array = doc.absorb(incoming)?;
    let target = match doc.member(parent, name)? {
        Some(n) => n,
        None => doc.set(parent, name, Value::Array)?,
    };
    while let Some(child) = doc.node(array)?.first {
        doc.detach(array, child)?;
        doc.append(target, child)?;
    }
    Ok(())
}
pub(super) fn write_logprobs(doc: &Document, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b",\"logprobs\":{\"content\":")?;
    doc.write(doc.root(), e)?;
    e.bytes(b"}")
}
