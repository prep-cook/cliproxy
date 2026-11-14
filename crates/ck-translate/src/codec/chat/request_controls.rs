use crate::codec::{
    Error, Result,
    document::{Document, Value},
    encode,
    encode::Encoder,
    provenance::{leaf, pointer, write_segment},
};
use ck_stream::Writer;

use super::provenance;

fn list(doc: &mut Document, key: &'static str) -> Result<usize> {
    let x = doc.required(doc.root(), "x_ck")?;
    match doc.member(x, key)? {
        Some(n) => Ok(n),
        None => doc.set(x, key, Value::Array),
    }
}

fn entry(
    doc: &mut Document,
    owner: &mut Writer,
    field: usize,
    key: &'static str,
    value_key: &'static str,
) -> Result<usize> {
    let parent = doc.node(field)?.parent.ok_or(Error::Schema)?;
    let lease = encode::scratch(owner, |e| pointer(doc, field, e))?;
    let pointer = doc.import_json(owner, lease)?;
    let list = list(doc, key)?;
    let entry = doc.add(None, Value::Object)?;
    doc.append(list, entry)?;
    doc.bind_origin(entry, parent, None)?;
    doc.rename(pointer, "pointer")?;
    doc.append(entry, pointer)?;
    doc.detach(parent, field)?;
    doc.rename(field, value_key)?;
    doc.append(entry, field)?;
    Ok(entry)
}

pub(super) fn cache(doc: &mut Document, owner: &mut Writer, parent: usize) -> Result<()> {
    if let Some(value) = doc.member(parent, "cache_control")? {
        if !matches!(doc.node(value)?.value, Value::Object | Value::Null) {
            return Err(Error::Schema);
        }
        entry(doc, owner, value, "cache_control", "value")?;
    }
    Ok(())
}

pub(super) fn message(doc: &mut Document, owner: &mut Writer, message: usize, index: u64) -> Result<()> {
    cache(doc, owner, message)?;
    if let Some(content) = doc.member(message, "content")?
        && matches!(doc.node(content)?.value, Value::Array)
    {
        let mut next = doc.node(content)?.first;
        while let Some(part) = next {
            next = doc.node(part)?.next;
            cache(doc, owner, part)?;
        }
    }
    if let Some(calls) = doc.member(message, "tool_calls")? {
        let mut next = doc.node(calls)?.first;
        let mut block = 0;
        while let Some(call) = next {
            next = doc.node(call)?.next;
            signatures(doc, owner, call, call, index, block)?;
            for carrier in ["function", "custom"] {
                if let Some(function) = doc.member(call, carrier)? {
                    signatures(doc, owner, function, call, index, block)?;
                }
            }
            block += 1;
        }
    }
    Ok(())
}

fn signatures(
    doc: &mut Document,
    owner: &mut Writer,
    parent: usize,
    call: usize,
    message: u64,
    block: u64,
) -> Result<()> {
    for key in ["thoughtSignature", "thought_signature"] {
        if let Some(value) = doc.member(parent, key)? {
            signature(doc, owner, value, call, message, block)?;
        }
    }
    if let Some(extra) = doc.member(parent, "extra_content")?
        && matches!(doc.node(extra)?.value, Value::Object)
        && let Some(google) = doc.member(extra, "google")?
        && matches!(doc.node(google)?.value, Value::Object)
        && let Some(value) = doc.member(google, "thought_signature")?
    {
        signature(doc, owner, value, call, message, block)?;
    }
    Ok(())
}

fn signature(
    doc: &mut Document,
    owner: &mut Writer,
    value: usize,
    call: usize,
    message: u64,
    block: u64,
) -> Result<()> {
    super::request::require_string(doc, value)?;
    let id = doc.required(call, "id")?;
    let id_value = doc.node(id)?.value;
    let entry = entry(doc, owner, value, "replay", "signature")?;
    doc.set(entry, "message_index", Value::Unsigned(message))?;
    doc.set(entry, "block_index", Value::Unsigned(block))?;
    doc.set(entry, "item_id", id_value)?;
    doc.set(entry, "kind", Value::Text("signature"))?;
    doc.set(entry, "source", Value::Text("gemini"))?;
    Ok(())
}

pub(super) fn declarations(doc: &mut Document, owner: &mut Writer) -> Result<()> {
    let root = doc.root();
    let Some(tools) = doc.member(root, "tools")? else {
        return Ok(());
    };
    if matches!(doc.node(tools)?.value, Value::Null) {
        return Ok(());
    }
    let mut next = doc.node(tools)?.first;
    while let Some(tool) = next {
        next = doc.node(tool)?.next;
        cache(doc, owner, tool)?;
        let kind = doc.required(tool, "type")?;
        for key in ["function", "custom"] {
            if let Some(declaration) = doc.member(tool, key)? {
                cache(doc, owner, declaration)?;
            }
        }
        if doc.string_is(kind, "custom")? {
            let declaration = doc.required(tool, "custom")?;
            let lease = encode::scratch(owner, |e| pointer(doc, tool, e))?;
            let pointer = doc.import_json(owner, lease)?;
            let tools = list(doc, "tools")?;
            let item = doc.add(None, Value::Object)?;
            doc.append(tools, item)?;
            doc.bind_origin(item, tool, None)?;
            doc.rename(pointer, "pointer")?;
            doc.append(item, pointer)?;
            // The projection shares mapped scalar storage, not a serialized body copy.
            let projection = doc.clone_tree(declaration)?;
            doc.rename(projection, "declaration")?;
            doc.append(item, projection)?;
            doc.set(item, "arguments_format", Value::Text("text"))?;
            if let Some(namespace) = doc.member(declaration, "namespace")? {
                let namespace = doc.clone_tree(namespace)?;
                doc.append(item, namespace)?;
            }
        }
    }
    Ok(())
}

pub(super) fn destination(doc: &Document) -> Result<()> {
    let x = doc.required(doc.root(), "x_ck")?;
    for key in ["session", "provider_parameters"] {
        if let Some(value) = doc.member(x, key)?
            && doc.node(value)?.first.is_some()
        {
            return Err(Error::Unsupported);
        }
    }
    if let Some(replay) = doc.member(x, "replay")? {
        for entry in doc.children(replay)? {
            let live = match doc.origin(entry)? {
                Some((parent, _)) => attached(doc, parent)?,
                None => true,
            };
            if live {
                return Err(Error::Unsupported);
            }
        }
    }
    if let Some(cache) = doc.member(x, "cache_control")? {
        for entry in doc.children(cache)? {
            if doc.member(entry, "value")?.is_some() && doc.origin(entry)?.is_none() {
                return Err(Error::Unsupported);
            }
        }
    }
    if let Some(tools) = doc.member(doc.root(), "tools")?
        && matches!(doc.node(tools)?.value, Value::Array)
    {
        for tool in doc.children(tools)? {
            let kind = doc.required(tool, "type")?;
            if !doc.string_is(kind, "custom")? && !doc.string_is(kind, "function")? {
                return Err(Error::Unsupported);
            }
        }
    }
    for message in doc.children(doc.required(doc.root(), "messages")?)? {
        if let Some(content) = doc.member(message, "content")?
            && matches!(doc.node(content)?.value, Value::Array)
        {
            for part in doc.children(content)? {
                let kind = doc.required(part, "type")?;
                if doc.string_is(kind, "redacted")? || doc.string_is(kind, "encrypted")? {
                    return Err(Error::Unsupported);
                }
            }
        }
    }
    if let Some(tools) = doc.member(x, "tools")? {
        for tool in doc.children(tools)? {
            if doc.origin(tool)?.is_none() {
                return Err(Error::Unsupported);
            }
        }
    }
    Ok(())
}

pub(super) fn members(doc: &Document, parent: usize, first: &mut bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    let x = doc.required(doc.root(), "x_ck")?;
    for entry in doc.origins(parent)? {
        for (key, value) in [("cache_control", "value"), ("replay", "signature")] {
            if doc.member(x, key)? != doc.node(entry)?.parent {
                continue;
            }
            let Some(value) = doc.member(entry, value)? else {
                continue;
            };
            let pointer = doc.required(entry, "pointer")?;
            let name = leaf(doc.chars(pointer)?)?;
            let mut exists = false;
            for child in doc.children(parent)? {
                exists |= name.matches(doc, doc.node(child)?.name.ok_or(Error::Schema)?)?;
            }
            if exists {
                continue;
            }
            if !*first {
                e.bytes(b",")?;
            }
            *first = false;
            write_segment(name, e)?;
            e.bytes(b":")?;
            write(doc, value, e)?;
        }
    }
    Ok(())
}

pub(super) fn write(doc: &Document, node: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let value = doc.node(node)?;
    if !matches!(value.value, Value::Object | Value::Array) {
        return doc.write(node, e);
    }
    let object = matches!(value.value, Value::Object);
    e.bytes(if object { b"{" } else { b"[" })?;
    let mut first = true;
    for child in doc.children(node)? {
        if !first {
            e.bytes(b",")?;
        }
        first = false;
        if object {
            doc.write_name(doc.node(child)?.name.ok_or(Error::Schema)?, e)?;
            e.bytes(b":")?;
        }
        write(doc, child, e)?;
    }
    if object {
        members(doc, node, &mut first, e)?;
        provenance::request_members_first(doc, node, None, &[], &[], &mut first, e)?;
    }
    e.bytes(if object { b"}" } else { b"]" })
}

pub(super) fn capture_cache(doc: &mut Document, owner: &mut Writer, value: usize) -> Result<usize> {
    if !matches!(doc.node(value)?.value, Value::Object | Value::Null) {
        return Err(Error::Schema);
    }
    entry(doc, owner, value, "cache_control", "value")
}

fn attached(doc: &Document, mut node: usize) -> Result<bool> {
    while node != doc.root() {
        let Some(parent) = doc.node(node)?.parent else {
            return Ok(false);
        };
        if !doc.children(parent)?.any(|child| child == node) {
            return Ok(false);
        }
        node = parent;
    }
    Ok(true)
}
