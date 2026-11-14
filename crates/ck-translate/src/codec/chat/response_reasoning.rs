//! Chat reasoning aliases. Providers spell reasoning text as `reasoning_content`, `reasoning`
//! or `reasoning_details`; the first that yields text is canonical `reasoning_content`.
use super::*;

const ALIASES: [&str; 3] = ["reasoning_content", "reasoning", "reasoning_details"];

/// Normalizes the delta and message of every choice before any of them is published.
/// `published` reports a long buffered value whose text was already published.
pub(super) fn normalize_choices(doc: &mut Document, choices: usize, published: impl Fn(usize) -> bool) -> Result<()> {
    let mut next = doc.node(choices)?.first;
    while let Some(choice) = next {
        next = doc.node(choice)?.next;
        for key in ["delta", "message"] {
            if let Some(body) = doc.member(choice, key)? {
                normalize(doc, body, &published)?;
            }
        }
    }
    Ok(())
}

/// Rewrites a message or delta so its reasoning text sits in `reasoning_content`, at the
/// selected alias's position. The selected alias is consumed; a `reasoning_content` without
/// text gives way to it. Unselected aliases stay native unknown members. Text nodes keep their
/// source positions: the canonical value is a fragment list over them, not a copy.
fn normalize(doc: &mut Document, body: usize, published: &impl Fn(usize) -> bool) -> Result<()> {
    if !matches!(doc.node(body)?.value, Value::Object) {
        return Ok(());
    }
    for key in ALIASES {
        let Some(field) = doc.member(body, key)? else {
            continue;
        };
        let primary = key == "reasoning_content";
        if primary {
            // Buffered ingress publishes only non-empty long text before the body ends.
            if published(field) {
                return Ok(());
            }
            if matches!(doc.node(field)?.value, Value::String(_) | Value::Text(_)) {
                if text(doc, field)? {
                    return Ok(());
                }
                continue;
            }
        }
        let fragments = doc.add(None, Value::Fragments)?;
        collect(doc, field, fragments)?;
        if doc.node(fragments)?.first.is_none() {
            continue;
        }
        if !primary {
            if let Some(empty) = doc.member(body, "reasoning_content")? {
                doc.detach(body, empty)?;
            }
            doc.rename(field, "reasoning_content")?;
        }
        return doc.transplant(field, fragments);
    }
    Ok(())
}

/// Appends every non-empty text of `node`: a string, an object's string `text`, or the same
/// forms nested in arrays. Other values carry no reasoning text.
fn collect(doc: &mut Document, node: usize, fragments: usize) -> Result<()> {
    match doc.node(node)?.value {
        Value::Array => {
            let mut next = doc.node(node)?.first;
            while let Some(child) = next {
                next = doc.node(child)?.next;
                collect(doc, child, fragments)?;
            }
            Ok(())
        }
        Value::Object => match doc.member(node, "text")? {
            Some(text) if matches!(doc.node(text)?.value, Value::String(_) | Value::Text(_)) => {
                append(doc, text, fragments)
            }
            _ => Ok(()),
        },
        Value::String(_) | Value::Text(_) => append(doc, node, fragments),
        _ => Ok(()),
    }
}

fn append(doc: &mut Document, node: usize, fragments: usize) -> Result<()> {
    if !text(doc, node)? {
        return Ok(());
    }
    let piece = doc.add(None, Value::Null)?;
    doc.transplant(piece, node)?;
    doc.append(fragments, piece)
}

fn text(doc: &Document, node: usize) -> Result<bool> {
    Ok(doc.chars(node)?.next_scalar()?.is_some())
}
