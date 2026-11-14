use super::super::{is, string};
use crate::codec::{
    Error, Operation, RequestView, Result,
    document::{Document, Value},
};

pub(super) fn validate(view: RequestView<'_>) -> Result<()> {
    if view.operation == Operation::CountTokens {
        return Err(Error::Unsupported);
    }
    let doc = view.document;
    let root = doc.root();
    string(doc, doc.required(root, "model")?)?;
    if view.operation == Operation::Compact
        && doc
            .member(root, "stream")?
            .is_some_and(|n| matches!(doc.node(n).map(|n| n.value), Ok(Value::Bool(true))))
    {
        return Err(Error::Unsupported);
    }
    for unsupported in [
        "n",
        "seed",
        "stop",
        "logit_bias",
        "frequency_penalty",
        "presence_penalty",
        "modalities",
        "audio",
    ] {
        if doc.member(root, unsupported)?.is_some() {
            return Err(Error::Unsupported);
        }
    }
    let messages = doc.required(root, "messages")?;
    if !matches!(doc.node(messages)?.value, Value::Array) {
        return Err(Error::Schema);
    }
    for message in doc.children(messages)? {
        validate_message(view, messages, message)?;
    }
    if let Some(x) = doc.member(root, "x_ck")? {
        validate_extensions(doc, x)?;
    }
    Ok(())
}
fn validate_message(view: RequestView<'_>, messages: usize, message: usize) -> Result<()> {
    let doc = view.document;
    let role = doc.required(message, "role")?;
    let mut valid = false;
    for value in ["system", "developer", "user", "assistant", "tool"] {
        valid |= doc.string_is(role, value)?;
    }
    if !valid {
        return Err(Error::Unsupported);
    }
    // ponytail: the linkage scan is quadratic in history; the 1 MiB metadata default bounds a
    // request to about 10 call turns. Replace it with a metadata-charged call-id index when
    // that bound rises (docs/DEFERRED-OPTIONS.md).
    if is(doc, message, "role", "tool")? {
        let id = doc.required(message, "tool_call_id")?;
        string(doc, id)?;
        let mut found = false;
        for previous in doc.children(messages)? {
            if previous == message {
                break;
            }
            if let Some(calls) = doc.member(previous, "tool_calls")? {
                for call in doc.children(calls)? {
                    found |= doc.equal_strings(doc.required(call, "id")?, id)?;
                }
            }
        }
        if !found {
            return Err(Error::Schema);
        }
    }
    if let Some(calls) = doc.member(message, "tool_calls")? {
        for call in doc.children(calls)? {
            string(doc, doc.required(call, "id")?)?;
            let function = doc.required(call, "function")?;
            string(doc, doc.required(function, "name")?)?;
        }
    }
    if let Some(content) = doc.member(message, "content")? {
        match doc.node(content)?.value {
            Value::Null | Value::String(_) | Value::Text(_) | Value::Fragments | Value::Utf8Media => (),
            Value::Array => {
                for part in doc.children(content)? {
                    validate_part(view, part)?;
                }
            }
            _ => return Err(Error::Schema),
        }
    }
    Ok(())
}
fn validate_part(view: RequestView<'_>, part: usize) -> Result<()> {
    let doc = view.document;
    let kind = doc.required(part, "type")?;
    let mut supported = false;
    for k in [
        "text",
        "reasoning",
        "reasoning_summary",
        "refusal",
        "encrypted",
        "image",
        "audio",
        "document",
        "opaque",
    ] {
        supported |= doc.string_is(kind, k)?;
    }
    if doc.string_is(kind, "audio")? {
        let source = doc.required(part, "source")?;
        let mime = doc.required(part, "mime")?;
        if !is(doc, source, "kind", "data")?
            || (!doc.string_is(mime, "audio/mpeg")? && !doc.string_is(mime, "audio/wav")?)
        {
            return Err(Error::Unsupported);
        }
        view.media
            .get(doc, doc.required(source, "ref")?)?
            .ok_or(Error::Position)?;
    }
    if !supported {
        return Err(Error::Unsupported);
    }
    Ok(())
}
/// Refuses gateway extensions a Responses upstream cannot carry.
fn validate_extensions(doc: &Document, x: usize) -> Result<()> {
    if let Some(cache) = doc.member(x, "cache_control")?
        && doc.node(cache)?.first.is_some()
    {
        return Err(Error::Unsupported);
    }
    if let Some(replay) = doc.member(x, "replay")? {
        for entry in doc.children(replay)? {
            if !is(doc, entry, "kind", "responses_encrypted")? {
                return Err(Error::Unsupported);
            }
        }
    }
    if let Some(session) = doc.member(x, "session")? {
        for field in doc.children(session)? {
            if !doc.name_is(doc.node(field)?.name.ok_or(Error::Schema)?, "previous_response_id")? {
                return Err(Error::Unsupported);
            }
        }
    }
    Ok(())
}
