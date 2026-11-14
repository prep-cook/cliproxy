//! Gemini generation and count boundaries over the canonical inference stream.

pub mod egress;
pub mod request;
mod request_content;
mod request_encode;
mod request_schema;
pub mod response;

use crate::boundary::{is, mapped, member, string};
use crate::codec::table::Table;
use crate::codec::{
    Error, Result,
    document::{Document, Value},
    encode::Encoder,
};

/// Writes `,"key":value` for every member of `parent` not named in `skip`.
fn members(doc: &Document, parent: usize, skip: &[&str], e: &mut Encoder<'_, '_>) -> Result<()> {
    for n in doc.children(parent)? {
        let name = doc.node(n)?.name.ok_or(Error::Schema)?;
        if skip
            .iter()
            .try_fold(false, |yes, key| Ok::<_, Error>(yes || doc.name_is(name, key)?))?
        {
            continue;
        }
        e.bytes(b",")?;
        doc.write_name(name, e)?;
        e.bytes(b":")?;
        doc.write(n, e)?;
    }
    Ok(())
}
/// Writes the Gemini source marker and `original` entries for the unknown members of `parent`.
fn provenance(doc: &Document, parent: usize, known: &[&str], e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b",\"source\":{\"protocol\":\"gemini\"}")?;
    crate::boundary::original(doc, "gemini", &[(parent, "", known)], e)
}
fn object(doc: &Document, node: usize) -> Result<()> {
    if matches!(doc.node(node)?.value, Value::Object) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}
fn array(doc: &Document, node: usize) -> Result<()> {
    if matches!(doc.node(node)?.value, Value::Array) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}

fn origin(doc: &Document, parent: usize, carrier: &'static str) -> Result<Option<usize>> {
    for entry in doc.origins(parent)? {
        if doc.origin(entry)?.is_some_and(|(_, kind)| kind == Some(carrier)) {
            return Ok(Some(entry));
        }
    }
    Ok(None)
}
