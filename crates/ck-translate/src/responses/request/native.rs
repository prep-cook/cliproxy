//! Which native Responses input a normalized message stands for. Normalization records it in
//! `x_ck.original`, a bound list, so the answer survives the canonical records (§4).
use super::super::is;
use crate::codec::{
    Error, Result,
    document::{Document, Name, Value},
};

/// The value of the Responses original field `name` that normalization kept for `target`.
pub(super) fn native(doc: &Document, target: usize, name: &str) -> Result<Option<usize>> {
    let Some(x) = doc.member(doc.root(), "x_ck")? else {
        return Ok(None);
    };
    let Some(original) = doc.member(x, "original")? else {
        return Ok(None);
    };
    for entry in doc.origins(target)? {
        if doc.node(entry)?.parent == Some(original) && is(doc, entry, "protocol", "responses")? {
            let Value::String(text) = doc.node(doc.required(entry, "pointer")?)?.value else {
                return Err(Error::Schema);
            };
            if doc.name_is(Name::Pointer(text), name)? {
                return Ok(Some(doc.required(entry, "value")?));
            }
        }
    }
    Ok(None)
}
/// The native item type normalization kept for `message`: reasoning, compaction or the type of
/// an item kept opaque; `None` for a message item.
pub(super) fn item_type(doc: &Document, message: usize) -> Result<Option<usize>> {
    let Some(kind) = native(doc, message, "type")? else {
        return Ok(None);
    };
    if matches!(doc.node(kind)?.value, Value::String(_)) && doc.string_is(kind, "message")? {
        return Ok(None);
    }
    Ok(Some(kind))
}
/// Whether the native item type `kind` is the string `name`.
pub(super) fn typed(doc: &Document, kind: Option<usize>, name: &str) -> Result<bool> {
    match kind {
        Some(kind) if matches!(doc.node(kind)?.value, Value::String(_)) => doc.string_is(kind, name),
        _ => Ok(false),
    }
}
