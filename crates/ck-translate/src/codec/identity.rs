//! Response identity at egress. Ingress fills a required canonical field it could not read from
//! the source with a generated value and names it in `x_ck.synthetic` (docs/INTERNAL-STREAM.md,
//! §3.2). An egress never shows such a value to its client as if the source had sent it: for the
//! response id it issues its own native id instead.

use super::{Error, Result, document::Document, event::LocalId, media};

/// Whether the canonical record `node` names `field` as generated rather than supplied.
pub fn synthetic(doc: &Document, node: usize, field: &str) -> Result<bool> {
    let Some(x) = doc.member(node, "x_ck")? else {
        return Ok(false);
    };
    let Some(names) = doc.member(x, "synthetic")? else {
        return Ok(false);
    };
    for name in doc.children(names)? {
        if doc.string_is(name, field)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The response id the source supplied on the canonical start `node`, if any.
pub fn supplied_id(doc: &Document, node: usize) -> Result<Option<usize>> {
    match doc.member(node, "id")? {
        Some(id) if !synthetic(doc, node, "id")? => Ok(Some(id)),
        _ => Ok(None),
    }
}

/// The response id an egress issues when the source supplied none: its native `prefix` and a
/// local id derived from the stream's writer, so every record of one response repeats it.
pub fn response_id(prefix: &str, owner: u64) -> Result<LocalId> {
    let local = media::local_id(b's', owner)?;
    let mut bytes = [0u8; 64];
    let id = bytes
        .get_mut(..prefix.len() + local.as_str().len())
        .ok_or(Error::Limit)?;
    let (head, tail) = id.split_at_mut(prefix.len());
    head.copy_from_slice(prefix.as_bytes());
    tail.copy_from_slice(local.as_str().as_bytes());
    LocalId::new(std::str::from_utf8(id).map_err(|_| Error::Schema)?)
}
