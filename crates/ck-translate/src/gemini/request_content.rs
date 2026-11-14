use super::request::{capture, preserve};
use crate::codec::{
    Error, Result,
    document::{Document, Value},
    media::{self, Encoding, Registry},
};
use ck_stream::Writer;

pub(super) fn normalize(owner: &mut Writer, doc: &mut Document, message: usize, message_index: u64) -> Result<()> {
    super::object(doc, message)?;
    if let Some(role) = doc.member(message, "role")? {
        if doc.string_is(role, "model")? {
            doc.replace(role, Value::Text("assistant"))?;
        } else if !doc.string_is(role, "user")? && !doc.string_is(role, "system")? {
            return Err(Error::Schema);
        }
    } else {
        doc.set(message, "role", Value::Text("user"))?;
    }
    capture(owner, doc, message, &["role", "parts"], None)?;
    let parts = doc.remove(message, "parts")?.ok_or(Error::Schema)?;
    super::array(doc, parts)?;
    let content = doc.set(message, "content", Value::Array)?;
    let x = doc.required(doc.root(), "x_ck")?;
    let order = match doc.member(x, "gemini_order")? {
        Some(n) => n,
        None => doc.set(x, "gemini_order", Value::Array)?,
    };
    let order_entry = doc.add(None, Value::Object)?;
    doc.append(order, order_entry)?;
    doc.bind_origin(order_entry, message, Some("parts"))?;
    let order_parts = doc.set(order_entry, "parts", Value::Array)?;
    let mut index = 0;
    while let Some(part) = doc.node(parts)?.first {
        doc.detach(parts, part)?;
        super::object(doc, part)?;
        let entry = doc.add(None, Value::Object)?;
        doc.append(order_parts, entry)?;
        let mut payloads = 0;
        for key in [
            "text",
            "inlineData",
            "fileData",
            "functionCall",
            "functionResponse",
            "executableCode",
            "codeExecutionResult",
        ] {
            payloads += usize::from(doc.member(part, key)?.is_some());
        }
        if payloads > 1 {
            return Err(Error::Schema);
        }
        if let Some(signature) = doc.remove(part, "thoughtSignature")? {
            super::string(doc, signature)?;
            let replay = doc.required(x, "replay")?;
            let binding = doc.add(None, Value::Object)?;
            doc.append(replay, binding)?;
            doc.set(binding, "message_index", Value::Unsigned(message_index))?;
            doc.set(binding, "block_index", Value::Unsigned(index))?;
            doc.set_id(
                binding,
                "item_id",
                media::local_id(b'i', doc.children(replay)?.count() as u64)?,
            )?;
            doc.set(binding, "kind", Value::Text("gemini"))?;
            doc.rename(signature, "signature")?;
            doc.append(binding, signature)?;
            let source = doc.set(binding, "source", Value::Object)?;
            doc.set(source, "protocol", Value::Text("gemini"))?;
            doc.bind_origin(binding, part, Some("signature"))?;
        }
        if let Some(call) = doc.remove(part, "functionCall")? {
            let calls = match doc.member(message, "tool_calls")? {
                Some(n) => n,
                None => doc.set(message, "tool_calls", Value::Array)?,
            };
            doc.append(calls, part)?;
            doc.set(part, "type", Value::Text("function"))?;
            doc.set(part, "arguments_format", Value::Text("json"))?;
            if let Some(id) = doc.remove(call, "id")? {
                doc.append(part, id)?;
            } else {
                doc.set_id(
                    part,
                    "id",
                    media::local_id(
                        b't',
                        message_index
                            .checked_mul(1_000_000)
                            .and_then(|n| n.checked_add(index))
                            .ok_or(Error::Limit)?,
                    )?,
                )?;
                doc.set(entry, "synthetic_id", Value::Bool(true))?;
            }
            if let Some(args) = doc.member(call, "args")? {
                doc.rename(args, "arguments")?;
            }
            doc.rename(call, "function")?;
            doc.append(part, call)?;
            doc.set(entry, "kind", Value::Text("call"))?;
        } else if let Some(result) = doc.remove(part, "functionResponse")? {
            doc.append(content, part)?;
            doc.set(part, "type", Value::Text("tool_result"))?;
            let name = doc.required(result, "name")?;
            super::string(doc, name)?;
            let id = doc.remove(result, "id")?;
            if let Some(id) = id {
                doc.rename(id, "tool_call_id")?;
                doc.append(part, id)?;
            } else {
                let linked = find_call(doc, message, name)?.ok_or(Error::Schema)?;
                let id = doc.node(doc.required(linked, "id")?)?.value;
                doc.set(part, "tool_call_id", id)?;
                doc.set(entry, "synthetic_id", Value::Bool(true))?;
            }
            doc.rename(result, "result")?;
            doc.append(part, result)?;
            doc.set(entry, "kind", Value::Text("result"))?;
        } else {
            doc.append(content, part)?;
            doc.set(entry, "kind", Value::Text("content"))?;
            if let Some(text) = doc.member(part, "text")? {
                super::string(doc, text)?;
                let thought = doc.member(part, "thought")?;
                let reasoning = if let Some(n) = thought {
                    match doc.node(n)?.value {
                        Value::Bool(b) => b,
                        _ => return Err(Error::Schema),
                    }
                } else {
                    false
                };
                if let Some(n) = doc.remove(part, "thought")? {
                    preserve(owner, doc, part, n, None)?;
                }
                doc.set(part, "type", Value::Text(if reasoning { "reasoning" } else { "text" }))?;
                capture(owner, doc, part, &["type", "text"], None)?;
            } else if let Some(data) = doc.remove(part, "inlineData")? {
                media_part(owner, doc, part, data, true)?;
            } else if let Some(data) = doc.remove(part, "fileData")? {
                media_part(owner, doc, part, data, false)?;
            } else if payloads == 0 && super::origin(doc, part, "signature")?.is_some() {
                doc.set(part, "type", Value::Text("reasoning"))?;
                doc.set(part, "text", Value::Text(""))?;
                doc.set(entry, "signature_only", Value::Bool(true))?;
                capture(owner, doc, part, &["type", "text"], None)?;
            } else {
                let value = doc.add(None, Value::Object)?;
                while let Some(n) = doc.node(part)?.first {
                    doc.detach(part, n)?;
                    doc.append(value, n)?;
                }
                doc.set(part, "type", Value::Text("opaque"))?;
                doc.set(part, "protocol", Value::Text("gemini"))?;
                doc.rename(value, "value")?;
                doc.append(part, value)?;
            }
        }
        doc.bind_origin(entry, part, Some("part"))?;
        index += 1;
    }
    Ok(())
}
fn find_call(doc: &Document, message: usize, name: usize) -> Result<Option<usize>> {
    let messages = doc.required(doc.root(), "messages")?;
    let mut found = None;
    for previous in doc.children(messages)? {
        if previous == message {
            break;
        }
        if let Some(calls) = doc.member(previous, "tool_calls")? {
            for call in doc.children(calls)? {
                let function = doc.required(call, "function")?;
                if doc.equal_strings(doc.required(function, "name")?, name)? {
                    found = Some(call);
                }
            }
        }
    }
    Ok(found)
}
fn media_part(owner: &mut Writer, doc: &mut Document, part: usize, data: usize, inline: bool) -> Result<()> {
    let mime = doc.member(data, "mimeType")?;
    let kind = if let Some(mime) = mime {
        modality(doc, mime)?
    } else {
        "document"
    };
    doc.set(part, "type", Value::Text(kind))?;
    if let Some(mime) = doc.remove(data, "mimeType")? {
        doc.rename(mime, "mime")?;
        doc.append(part, mime)?;
    }
    let payload = doc
        .remove(data, if inline { "data" } else { "fileUri" })?
        .ok_or(Error::Schema)?;
    super::string(doc, payload)?;
    let source = doc.set(part, "source", Value::Object)?;
    doc.set(source, "kind", Value::Text(if inline { "data" } else { "uri" }))?;
    doc.rename(payload, if inline { "ref" } else { "uri" })?;
    doc.append(source, payload)?;
    while let Some(n) = doc.node(data)?.first {
        doc.detach(data, n)?;
        preserve(owner, doc, source, n, None)?;
    }
    capture(owner, doc, part, &["type", "source", "mime"], None)
}
pub(super) fn modality(doc: &Document, mime: usize) -> Result<&'static str> {
    for (prefix, kind) in [("image/", "image"), ("audio/", "audio"), ("video/", "video")] {
        let mut chars = doc.chars(mime)?;
        let mut matches = true;
        for c in prefix.chars() {
            if chars.next_scalar()? != Some(c) {
                matches = false;
                break;
            }
        }
        if matches {
            return Ok(kind);
        }
    }
    Ok("document")
}
pub(super) fn register(doc: &mut Document, media: &mut Registry) -> Result<()> {
    let messages = doc.required(doc.root(), "messages")?;
    let mut message = doc.node(messages)?.first;
    while let Some(m) = message {
        message = doc.node(m)?.next;
        let parts = doc.required(m, "content")?;
        let mut next = doc.node(parts)?.first;
        while let Some(part) = next {
            next = doc.node(part)?.next;
            if let Some(source) = doc.member(part, "source")?
                && super::is(doc, source, "kind", "data")?
            {
                let node = doc.required(source, "ref")?;
                let Value::String(data) = doc.node(node)?.value else {
                    return Err(Error::Schema);
                };
                let mime = doc.member(part, "mime")?;
                let kind = mime.map(|n| modality(doc, n)).transpose()?.unwrap_or("document");
                media.register(doc, node, data, Encoding::Base64, kind, mime)?;
            }
        }
    }
    Ok(())
}
