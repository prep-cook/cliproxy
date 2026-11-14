//! Responses HTTP boundary codecs over the canonical mapped stream.

mod content;
pub mod egress;
mod image;
mod opaque;
pub mod request;
pub mod response;
mod snapshot;
mod usage;

const REDACTED_PREFIX: &str = "claude-redacted-thinking:";

use crate::codec::{
    Error, Result,
    document::{Document, Value},
    encode::{self, Encoder},
    event::{Emitter, Kind, Scope},
    source::Source,
};
use ck_stream::Writer;

fn mapped(owner: &mut Writer, write: impl Fn(&mut Encoder<'_, '_>) -> Result<()>) -> Result<Document> {
    let lease = encode::scratch(owner, write)?;
    let mut source = Source::new(owner)?;
    source.push_scratch(lease)?;
    let len = source.len();
    Document::parse(owner, source, len)
}

fn string(doc: &Document, node: usize) -> Result<()> {
    if matches!(
        doc.node(node)?.value,
        Value::String(_) | Value::Text(_) | Value::Id(_) | Value::Fragments | Value::Utf8Media
    ) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}

fn is(doc: &Document, parent: usize, name: &str, value: &str) -> Result<bool> {
    doc.member(parent, name)?.map_or(Ok(false), |n| doc.string_is(n, value))
}

fn member(doc: &Document, parent: usize, name: &str, e: &mut Encoder<'_, '_>) -> Result<()> {
    if let Some(node) = doc.member(parent, name)? {
        e.bytes(b",")?;
        e.string(name)?;
        e.bytes(b":")?;
        doc.write(node, e)?;
    }
    Ok(())
}

fn failure(emitter: &mut Emitter, owner: &mut Writer, client: bool) -> Result<()> {
    emitter.emit(owner, Kind::Error, Scope::default(), |e| {
        e.bytes(if client { b",\"code\":\"invalid_request\",\"status\":400,\"scope\":\"client\",\"message\":\"Invalid Responses request\"" } else { b",\"code\":\"invalid_response\",\"status\":502,\"scope\":\"upstream\",\"message\":\"Invalid Responses response\"" })
    }, |_| Ok(()))?;
    Ok(())
}

fn move_field(doc: &mut Document, from: usize, name: &str, to: usize, target: &'static str) -> Result<Option<usize>> {
    let Some(node) = doc.remove(from, name)? else {
        return Ok(None);
    };
    doc.rename(node, target)?;
    doc.append(to, node)?;
    Ok(Some(node))
}

fn merge(doc: &mut Document, target: usize, incoming: usize) -> Result<()> {
    while let Some(field) = doc.node(incoming)?.first {
        doc.detach(incoming, field)?;
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        let mut found = None;
        for child in doc.children(target)? {
            if doc.names_equal(name, doc.node(child)?.name.ok_or(Error::Schema)?)? {
                found = Some(child);
                break;
            }
        }
        if let Some(existing) = found {
            if matches!(doc.node(existing)?.value, Value::Object) && matches!(doc.node(field)?.value, Value::Object) {
                merge(doc, existing, field)?;
            } else {
                doc.transplant(existing, field)?;
            }
        } else {
            doc.append(target, field)?;
        }
    }
    Ok(())
}
