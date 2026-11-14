//! Responses HTTP boundary codecs over the canonical mapped stream.

mod audio;
mod content;
pub mod egress;
mod image;
mod opaque;
pub mod request;
pub mod response;
mod snapshot;
mod usage;

const REDACTED_PREFIX: &str = "claude-redacted-thinking:";

use crate::boundary::{failure, is, mapped, member, object, string};
use crate::codec::table::Table;
use crate::codec::{
    Error, Result,
    document::{Document, Value},
};

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
