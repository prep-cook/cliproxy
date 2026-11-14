use crate::codec::{
    Error, Result,
    document::{Document, Value},
};
pub(super) fn aggregate_chunk(doc: &mut Document, incoming: usize) -> Result<()> {
    let target = doc.root();
    let choices = doc.required(incoming, "choices")?;
    let output_choices = doc.required(target, "choices")?;
    let mut next = doc.node(choices)?.first;
    while let Some(choice) = next {
        next = doc.node(choice)?.next;
        let index = doc.unsigned(doc.required(choice, "index")?)?;
        let mut existing = None;
        for candidate in doc.children(output_choices)? {
            if doc.unsigned(doc.required(candidate, "index")?)? == index {
                existing = Some(candidate);
                break;
            }
        }
        let output = if let Some(existing) = existing {
            existing
        } else {
            let output = doc.add(None, Value::Object)?;
            doc.append(output_choices, output)?;
            doc.set(output, "index", Value::Unsigned(index))?;
            doc.set(output, "message", Value::Object)?;
            doc.set(output, "finish_reason", Value::Null)?;
            output
        };
        if let Some(delta) = doc.member(choice, "delta")? {
            let message = doc.required(output, "message")?;
            merge_delta(doc, message, delta)?;
        }
        while let Some(field) = doc.node(choice)?.first {
            doc.detach(choice, field)?;
            let name = doc.node(field)?.name.ok_or(Error::Schema)?;
            if doc.name_is(name, "index")? || doc.name_is(name, "delta")? {
                continue;
            }
            if doc.name_is(name, "finish_reason")? && matches!(doc.node(field)?.value, Value::Null) {
                continue;
            }
            merge_field(doc, output, field)?;
        }
    }
    while let Some(field) = doc.node(incoming)?.first {
        doc.detach(incoming, field)?;
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        if doc.name_is(name, "choices")? || doc.name_is(name, "object")? {
            continue;
        }
        merge_field(doc, target, field)?;
    }
    Ok(())
}
pub(super) fn merge_delta(doc: &mut Document, target: usize, incoming: usize) -> Result<()> {
    while let Some(field) = doc.node(incoming)?.first {
        doc.detach(incoming, field)?;
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        if doc.name_is(name, "tool_calls")? {
            let calls = match doc.member(target, "tool_calls")? {
                Some(c) => c,
                None => doc.set(target, "tool_calls", Value::Array)?,
            };
            while let Some(call) = doc.node(field)?.first {
                doc.detach(field, call)?;
                let index = doc.unsigned(doc.required(call, "index")?)?;
                let mut existing = None;
                for c in doc.children(calls)? {
                    if doc.unsigned(doc.required(c, "index")?)? == index {
                        existing = Some(c);
                        break;
                    }
                }
                if let Some(existing) = existing {
                    merge_delta(doc, existing, call)?;
                } else {
                    doc.append(calls, call)?;
                }
            }
        } else {
            merge_field(doc, target, field)?;
        }
    }
    Ok(())
}
fn merge_field(doc: &mut Document, parent: usize, incoming: usize) -> Result<()> {
    let name = doc.node(incoming)?.name.ok_or(Error::Schema)?;
    let mut existing = None;
    for candidate in doc.children(parent)? {
        if doc.names_equal(name, doc.node(candidate)?.name.ok_or(Error::Schema)?)? {
            existing = Some(candidate);
            break;
        }
    }
    let Some(existing) = existing else {
        return doc.append(parent, incoming);
    };
    if doc.name_is(name, "usage")? {
        return doc.transplant(existing, incoming);
    }
    let a = doc.node(existing)?.value;
    let b = doc.node(incoming)?.value;
    if matches!(a, Value::Object) && matches!(b, Value::Object) {
        return merge_delta(doc, existing, incoming);
    }
    if (matches!(a, Value::Array) && matches!(b, Value::Array))
        || (matches!(a, Value::Base64Fragments) && matches!(b, Value::Base64Fragments))
        || (matches!(a, Value::Utf8Media) && matches!(b, Value::Utf8Media))
    {
        while let Some(child) = doc.node(incoming)?.first {
            doc.detach(incoming, child)?;
            doc.append(existing, child)?;
        }
        return Ok(());
    }
    let mut fragment = false;
    for key in [
        "content",
        "reasoning_content",
        "refusal",
        "arguments",
        "input",
        "data",
        "transcript",
    ] {
        fragment |= doc.name_is(name, key)?;
    }
    if fragment
        && matches!(
            a,
            Value::String(_) | Value::Text(_) | Value::Fragments | Value::Utf8Media
        )
    {
        if matches!(b, Value::Null) {
            return Ok(());
        }
        if !matches!(b, Value::String(_) | Value::Text(_) | Value::Utf8Media) {
            return Err(Error::Schema);
        }
        if !matches!(a, Value::Fragments) {
            let first = doc.add(None, Value::Null)?;
            doc.transplant(first, existing)?;
            doc.replace(existing, Value::Fragments)?;
            doc.append(existing, first)?;
        }
        return doc.append(existing, incoming);
    }
    doc.transplant(existing, incoming)
}
