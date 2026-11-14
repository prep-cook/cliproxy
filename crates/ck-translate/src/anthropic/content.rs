use crate::codec::{
    Error, Result,
    document::{Document, Name, Value},
    media::{self, Encoding, Registry},
};
use ck_stream::Writer;

pub(super) fn normalize(owner: &mut Writer, doc: &mut Document, message: usize, message_index: u64) -> Result<()> {
    let role = doc.required(message, "role")?;
    if !["user", "assistant", "system"]
        .iter()
        .try_fold(false, |yes, name| Ok::<_, Error>(yes || doc.string_is(role, name)?))?
    {
        return Err(Error::Schema);
    }
    let x = doc.required(doc.root(), "x_ck")?;
    let original = doc.required(x, "original")?;
    super::request::capture(owner, doc, message, &["role", "content", "cache_control"], original)?;
    let content = doc.required(message, "content")?;
    match doc.node(content)?.value {
        Value::String(_) | Value::Utf8Media => (),
        Value::Array => {
            let mut next = doc.node(content)?.first;
            let mut block_index = 0;
            let mut call_index = 0;
            while let Some(part) = next {
                next = doc.node(part)?.next;
                if super::is(doc, part, "type", "tool_use")?
                    || super::is(doc, part, "type", "server_tool_use")?
                    || super::is(doc, part, "type", "mcp_tool_use")?
                {
                    if !doc.string_is(role, "assistant")? {
                        return Err(Error::Schema);
                    }
                    tool_use(
                        owner,
                        doc,
                        (message, content, part),
                        (message_index, block_index, call_index),
                    )?;
                    call_index += 1;
                } else {
                    normalize_part(owner, doc, part, message_index, block_index)?;
                }
                block_index += 1;
            }
        }
        _ => return Err(Error::Schema),
    }
    cache(owner, doc, message)?;
    super::request::capture(owner, doc, message, &["role", "content", "tool_calls"], original)
}
/// Moves a `tool_use`, `server_tool_use` or `mcp_tool_use` block from the content into
/// `tool_calls`, recording its native type and position in `x_ck.anthropic_order`.
fn tool_use(
    owner: &mut Writer,
    doc: &mut Document,
    (message, content, part): (usize, usize, usize),
    (message_index, block_index, call_index): (u64, u64, u64),
) -> Result<()> {
    let x = doc.required(doc.root(), "x_ck")?;
    let original = doc.required(x, "original")?;
    let mcp = super::is(doc, part, "type", "mcp_tool_use")?;
    let server = mcp || super::is(doc, part, "type", "server_tool_use")?;
    super::request::capture(
        owner,
        doc,
        part,
        &["type", "id", "name", "input", "cache_control", "server_name"],
        original,
    )?;
    doc.detach(content, part)?;
    let calls = match doc.member(message, "tool_calls")? {
        Some(n) => n,
        None => doc.set(message, "tool_calls", Value::Array)?,
    };
    doc.append(calls, part)?;
    let kind = doc.required(part, "type")?;
    let native_type = doc.clone_tree(kind)?;
    doc.rename(native_type, "native_type")?;
    doc.replace(kind, Value::Text(if server { "server" } else { "function" }))?;
    let function = doc.set(part, "function", Value::Object)?;
    let name = doc.remove(part, "name")?.ok_or(Error::Schema)?;
    doc.append(function, name)?;
    if let Some(input) = doc.remove(part, "input")? {
        doc.rename(input, "arguments")?;
        doc.append(function, input)?;
    }
    if mcp {
        let namespace = doc.remove(part, "server_name")?.ok_or(Error::Schema)?;
        super::string(doc, namespace)?;
        doc.rename(namespace, "namespace")?;
        doc.append(part, namespace)?;
    }
    doc.set(part, "arguments_format", Value::Text("json"))?;
    let order = doc.required(x, "anthropic_order")?;
    let entry = doc.add(None, Value::Object)?;
    doc.append(order, entry)?;
    doc.set(entry, "message_index", Value::Unsigned(message_index))?;
    doc.set(entry, "block_index", Value::Unsigned(block_index))?;
    doc.set(entry, "call_index", Value::Unsigned(call_index))?;
    doc.append(entry, native_type)?;
    doc.bind_origin(entry, part, None)?;
    cache(owner, doc, part)?;
    super::request::capture(
        owner,
        doc,
        part,
        &["id", "type", "function", "arguments_format", "namespace"],
        original,
    )?;
    Ok(())
}
fn normalize_part(
    owner: &mut Writer,
    doc: &mut Document,
    part: usize,
    message_index: u64,
    block_index: u64,
) -> Result<()> {
    let x = doc.required(doc.root(), "x_ck")?;
    let original = doc.required(x, "original")?;
    cache(owner, doc, part)?;
    let kind = doc.required(part, "type")?;
    if let Some(known) = known_fields(doc, part, kind)? {
        super::request::capture(owner, doc, part, known, original)?;
    }
    if doc.string_is(kind, "text")? {
        super::string(doc, doc.required(part, "text")?)?;
    } else if doc.string_is(kind, "thinking")? {
        doc.replace(kind, Value::Text("reasoning"))?;
        let text = doc.required(part, "thinking")?;
        super::string(doc, text)?;
        doc.rename(text, "text")?;
        if let Some(signature) = doc.remove(part, "signature")? {
            super::string(doc, signature)?;
            let replay = replay(doc, part, message_index, block_index, "anthropic")?;
            doc.append(replay, signature)?;
        }
    } else if doc.string_is(kind, "redacted_thinking")? {
        doc.replace(kind, Value::Text("redacted"))?;
        super::string(doc, doc.required(part, "data")?)?;
        let replay_index = doc.children(doc.required(x, "replay")?)?.count() as u64;
        replay(doc, part, message_index, block_index, "anthropic")?;
        doc.set(part, "replay_index", Value::Unsigned(replay_index))?;
    } else if doc.string_is(kind, "image")? || doc.string_is(kind, "document")? {
        media_part(owner, doc, part, original)?;
    } else if doc.string_is(kind, "tool_result")? || super::server_result(doc, part)? {
        result_part(owner, doc, (part, kind), message_index, block_index)?;
    } else {
        let value = doc.add(Some(Name::Static("value")), Value::Object)?;
        doc.transplant(value, part)?;
        doc.replace(part, Value::Object)?;
        doc.set(part, "type", Value::Text("opaque"))?;
        doc.set(part, "protocol", Value::Text("anthropic"))?;
        doc.append(part, value)?;
        return Ok(());
    }
    super::request::capture(
        owner,
        doc,
        part,
        &[
            "type",
            "text",
            "source",
            "mime",
            "data",
            "replay_index",
            "tool_call_id",
            "content",
            "is_error",
        ],
        original,
    )
}
/// Fields of a block kind that map to canonical fields; `None` for an opaque block.
fn known_fields(doc: &Document, part: usize, kind: usize) -> Result<Option<&'static [&'static str]>> {
    Ok(if doc.string_is(kind, "text")? {
        Some(&["type", "text"])
    } else if doc.string_is(kind, "thinking")? {
        Some(&["type", "thinking", "signature"])
    } else if doc.string_is(kind, "redacted_thinking")? {
        Some(&["type", "data"])
    } else if doc.string_is(kind, "image")? || doc.string_is(kind, "document")? {
        Some(&["type", "source"])
    } else if doc.string_is(kind, "tool_result")? || super::server_result(doc, part)? {
        Some(&["type", "tool_use_id", "content", "is_error"])
    } else {
        None
    })
}
fn media_part(owner: &mut Writer, doc: &mut Document, part: usize, original: usize) -> Result<()> {
    let source = doc.required(part, "source")?;
    let source_kind = doc.required(source, "type")?;
    super::request::capture(
        owner,
        doc,
        source,
        &["type", "data", "url", "file_id", "media_type"],
        original,
    )?;
    let mime = doc.remove(source, "media_type")?;
    if let Some(mime) = mime {
        doc.rename(mime, "mime")?;
        doc.append(part, mime)?;
    }
    if doc.string_is(source_kind, "base64")? {
        doc.rename(source_kind, "kind")?;
        doc.replace(source_kind, Value::Text("data"))?;
        let data = doc.required(source, "data")?;
        doc.rename(data, "ref")?;
    } else if doc.string_is(source_kind, "url")? {
        doc.rename(source_kind, "kind")?;
        doc.replace(source_kind, Value::Text("uri"))?;
        let url = doc.required(source, "url")?;
        doc.rename(url, "uri")?;
    } else if doc.string_is(source_kind, "file")? {
        doc.rename(source_kind, "kind")?;
    } else if doc.string_is(source_kind, "text")? {
        // Plain-text document sources are registered UTF-8 media, not guessed binary.
        doc.rename(source_kind, "kind")?;
        doc.replace(source_kind, Value::Text("data"))?;
        let data = doc.required(source, "data")?;
        doc.rename(data, "ref")?;
        doc.set(source, "anthropic_text", Value::Bool(true))?;
    } else {
        return Err(Error::Unsupported);
    }
    super::request::capture(
        owner,
        doc,
        source,
        &["kind", "ref", "uri", "file_id", "anthropic_text"],
        original,
    )?;
    Ok(())
}
/// Maps a `tool_result` or server tool result block, recording its native type and position.
fn result_part(
    owner: &mut Writer,
    doc: &mut Document,
    (part, kind): (usize, usize),
    message_index: u64,
    block_index: u64,
) -> Result<()> {
    let x = doc.required(doc.root(), "x_ck")?;
    let original = doc.required(x, "original")?;
    let server = super::server_result(doc, part)?;
    let native_type = doc.clone_tree(kind)?;
    doc.rename(native_type, "native_type")?;
    let order = doc.required(x, "anthropic_order")?;
    let entry = doc.add(None, Value::Object)?;
    doc.append(order, entry)?;
    doc.append(entry, native_type)?;
    doc.set(entry, "message_index", Value::Unsigned(message_index))?;
    doc.set(entry, "block_index", Value::Unsigned(block_index))?;
    doc.set(entry, "server_result", Value::Bool(server))?;
    doc.bind_origin(entry, part, None)?;
    doc.replace(kind, Value::Text("tool_result"))?;
    let id = doc.required(part, "tool_use_id")?;
    doc.rename(id, "tool_call_id")?;
    if let Some(content) = doc.member(part, "content")? {
        if matches!(doc.node(content)?.value, Value::String(_) | Value::Utf8Media) {
            doc.detach(part, content)?;
            doc.rename(content, "text")?;
            let array = doc.set(part, "content", Value::Array)?;
            let item = doc.add(None, Value::Object)?;
            doc.append(array, item)?;
            doc.set(item, "type", Value::Text("text"))?;
            doc.append(item, content)?;
            let marker = doc.add(Some(Name::Static("content_shape")), Value::Text("string"))?;
            super::request::preserve(owner, doc, original, part, marker, Some("shape"))?;
        } else if matches!(doc.node(content)?.value, Value::Array) {
            let mut next = doc.node(content)?.first;
            while let Some(child) = next {
                next = doc.node(child)?.next;
                normalize_part(owner, doc, child, message_index, block_index)?;
            }
        } else if server && matches!(doc.node(content)?.value, Value::Object) {
            doc.detach(part, content)?;
            doc.rename(content, "value")?;
            let array = doc.set(part, "content", Value::Array)?;
            let item = doc.add(None, Value::Object)?;
            doc.append(array, item)?;
            doc.set(item, "type", Value::Text("opaque"))?;
            doc.set(item, "protocol", Value::Text("anthropic"))?;
            doc.append(item, content)?;
            let marker = doc.add(Some(Name::Static("content_shape")), Value::Text("object"))?;
            super::request::preserve(owner, doc, original, part, marker, Some("shape"))?;
        } else {
            return Err(Error::Schema);
        }
    }
    Ok(())
}
fn replay(doc: &mut Document, part: usize, message_index: u64, block_index: u64, kind: &'static str) -> Result<usize> {
    let x = doc.required(doc.root(), "x_ck")?;
    let list = doc.required(x, "replay")?;
    let entry = doc.add(None, Value::Object)?;
    doc.append(list, entry)?;
    doc.set(entry, "message_index", Value::Unsigned(message_index))?;
    doc.set(entry, "block_index", Value::Unsigned(block_index))?;
    doc.set(entry, "item_id", Value::Id(media::local_id(b'i', message_index)?))?;
    doc.set(entry, "kind", Value::Text(kind))?;
    let source = doc.set(entry, "source", Value::Object)?;
    doc.set(source, "protocol", Value::Text("anthropic"))?;
    doc.bind_origin(entry, part, None)?;
    Ok(entry)
}
pub(super) fn cache(owner: &mut Writer, doc: &mut Document, parent: usize) -> Result<()> {
    let Some(cache) = doc.remove(parent, "cache_control")? else {
        return Ok(());
    };
    let x = doc.required(doc.root(), "x_ck")?;
    let list = doc.required(x, "cache_control")?;
    super::request::preserve(owner, doc, list, parent, cache, None)
}
pub(super) fn register_media(
    doc: &mut Document,
    registry: &mut Registry,
    owner: &mut Writer,
    emitter: &mut crate::codec::event::Emitter,
    reader: &mut ck_stream::Reader,
    published: &mut usize,
) -> Result<()> {
    let mut pending = MediaRegistration {
        registry,
        owner,
        emitter,
        reader,
        published,
    };
    // Never recognize media-marker look-alikes in provenance or schemas.
    let messages = doc.required(doc.root(), "messages")?;
    let mut message = doc.node(messages)?.first;
    while let Some(node) = message {
        message = doc.node(node)?.next;
        if let Some(content) = doc.member(node, "content")? {
            walk(doc, &mut pending, content)?;
        }
    }
    Ok(())
}
/// Registers inline media in a content array, following tool result content.
fn walk(doc: &mut Document, pending: &mut MediaRegistration<'_>, node: usize) -> Result<()> {
    if matches!(doc.node(node)?.value, Value::Object)
        && (super::is(doc, node, "type", "image")? || super::is(doc, node, "type", "document")?)
    {
        let source = doc.required(node, "source")?;
        if super::is(doc, source, "kind", "data")? {
            return register_source(doc, pending, node, source);
        }
    }
    if matches!(doc.node(node)?.value, Value::Array) {
        let mut child = doc.node(node)?.first;
        while let Some(index) = child {
            child = doc.node(index)?.next;
            walk(doc, pending, index)?;
        }
    } else if matches!(doc.node(node)?.value, Value::Object)
        && super::is(doc, node, "type", "tool_result")?
        && let Some(content) = doc.member(node, "content")?
    {
        walk(doc, pending, content)?;
    }
    Ok(())
}
/// Registers and publishes the inline data of an image or document source.
fn register_source(doc: &mut Document, pending: &mut MediaRegistration<'_>, node: usize, source: usize) -> Result<()> {
    let data = doc.required(source, "ref")?;
    let mime = doc.member(node, "mime")?;
    let encoding = if doc.member(source, "anthropic_text")?.is_some() {
        Encoding::Utf8
    } else {
        Encoding::Base64
    };
    let kind = if super::is(doc, node, "type", "image")? {
        "image"
    } else {
        "document"
    };
    let registered = pending.registry.get(doc, data)?;
    if registered.is_some_and(|media| {
        matches!(
            (media.encoding, encoding),
            (Encoding::Base64, Encoding::Base64) | (Encoding::Utf8, Encoding::Utf8)
        )
    }) {
        pending.registry.update_mime(data, mime);
    } else {
        let Value::String(text) = doc.node(data)?.value else {
            return Err(Error::Schema);
        };
        let media = pending.registry.register(doc, data, text, encoding, kind, mime)?;
        media::publish(doc, media, pending.emitter, pending.owner, Default::default())?;
        pending
            .registry
            .retain(doc, *pending.published, pending.owner, pending.reader)?;
        *pending.published += 1;
    }
    Ok(())
}

struct MediaRegistration<'a> {
    registry: &'a mut Registry,
    owner: &'a mut Writer,
    emitter: &'a mut crate::codec::event::Emitter,
    reader: &'a mut ck_stream::Reader,
    published: &'a mut usize,
}

pub(super) fn history(doc: &Document, node: usize) -> Result<Option<usize>> {
    let x = doc.required(doc.root(), "x_ck")?;
    let Some(order) = doc.member(x, "anthropic_order")? else {
        return Ok(None);
    };
    for entry in doc.origins(node)? {
        if doc.node(entry)?.parent == Some(order) {
            return Ok(Some(entry));
        }
    }
    Ok(None)
}
