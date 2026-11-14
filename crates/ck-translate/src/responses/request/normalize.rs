use super::super::{is, move_field, string};
use crate::codec::{
    Error, Operation, Result,
    document::{Document, Value},
    encode,
};
use ck_stream::Writer;

const ROOT_FIELDS: &[&str] = &[
    "model",
    "messages",
    "tools",
    "tool_choice",
    "parallel_tool_calls",
    "temperature",
    "top_p",
    "max_completion_tokens",
    "stream",
    "response_format",
    "store",
    "metadata",
    "service_tier",
    "user",
    "safety_identifier",
    "prompt_cache_key",
    "prompt_cache_retention",
    "x_ck",
];

pub(super) fn normalize(doc: &mut Document, owner: &mut Writer, operation: Operation) -> Result<()> {
    let root = doc.root();
    let wire_x = doc.remove(root, "x_ck")?;
    let x = doc.set(root, "x_ck", Value::Object)?;
    doc.set(x, "v", Value::Unsigned(1))?;
    doc.set(x, "original_protocol", Value::Text("responses"))?;
    doc.set(
        x,
        "operation",
        Value::Text(if operation == Operation::Compact {
            "compact"
        } else {
            "generate"
        }),
    )?;
    let original = doc.set(x, "original", Value::Array)?;
    if let Some(wire_x) = wire_x {
        preserve(doc, owner, original, root, wire_x)?;
    }
    normalize_input(doc, owner, x, original)?;
    normalize_options(doc, owner, x, original)?;
    normalize_tools(doc, x)?;
    preserve_unknown(doc, owner, original, root, ROOT_FIELDS)
}
/// Moves `instructions` and `input` into canonical messages.
fn normalize_input(doc: &mut Document, owner: &mut Writer, x: usize, original: usize) -> Result<()> {
    let root = doc.root();
    let messages = doc.set(root, "messages", Value::Array)?;
    if let Some(instructions) = doc.remove(root, "instructions")? {
        string(doc, instructions)?;
        let message = doc.add(None, Value::Object)?;
        doc.append(messages, message)?;
        doc.set(message, "role", Value::Text("developer"))?;
        doc.rename(instructions, "content")?;
        doc.append(message, instructions)?;
        doc.bind_origin(message, instructions, Some("responses.instructions"))?;
    }
    if let Some(input) = doc.remove(root, "input")? {
        if matches!(doc.node(input)?.value, Value::String(_) | Value::Utf8Media) {
            let m = doc.add(None, Value::Object)?;
            doc.append(messages, m)?;
            doc.set(m, "role", Value::Text("user"))?;
            doc.rename(input, "content")?;
            doc.append(m, input)?;
        } else if matches!(doc.node(input)?.value, Value::Array) {
            while let Some(item) = doc.node(input)?.first {
                doc.detach(input, item)?;
                normalize_item(doc, owner, item, messages, x, original)?;
            }
        } else {
            return Err(Error::Schema);
        }
    }
    Ok(())
}
/// Maps the generation options to canonical fields and keeps the rest as provider parameters.
fn normalize_options(doc: &mut Document, owner: &mut Writer, x: usize, original: usize) -> Result<()> {
    let root = doc.root();
    if let Some(native) = doc.remove(root, "max_completion_tokens")? {
        preserve(doc, owner, original, root, native)?;
    }
    move_field(doc, root, "max_output_tokens", root, "max_completion_tokens")?;
    if let Some(text) = doc.remove(root, "text")? {
        move_field(doc, text, "format", root, "response_format")?;
        if doc.node(text)?.first.is_some() {
            let provider = provider(doc, x)?;
            doc.append(provider, text)?;
        }
    }
    if let Some(thinking) = doc.remove(root, "reasoning")? {
        doc.rename(thinking, "thinking")?;
        doc.append(x, thinking)?;
        move_field(doc, thinking, "summary", thinking, "include")?;
    }
    if let Some(previous) = doc.remove(root, "previous_response_id")? {
        let session = doc.set(x, "session", Value::Object)?;
        doc.append(session, previous)?;
    }
    for name in ["conversation", "background", "truncation", "include", "max_tool_calls"] {
        if let Some(value) = doc.remove(root, name)? {
            let provider = provider(doc, x)?;
            doc.append(provider, value)?;
        }
    }
    Ok(())
}
/// Nests function tools and a function tool choice; other tools move to `x_ck.tools`.
fn normalize_tools(doc: &mut Document, x: usize) -> Result<()> {
    let root = doc.root();
    if let Some(tools) = doc.member(root, "tools")? {
        let mut next = doc.node(tools)?.first;
        while let Some(tool) = next {
            next = doc.node(tool)?.next;
            if is(doc, tool, "type", "function")? {
                let function = doc.set(tool, "function", Value::Object)?;
                for name in ["name", "description", "parameters", "strict"] {
                    move_field(doc, tool, name, function, name)?;
                }
            } else {
                let native = match doc.member(x, "tools")? {
                    Some(n) => n,
                    None => doc.set(x, "tools", Value::Array)?,
                };
                let entry = doc.add(None, Value::Object)?;
                doc.append(native, entry)?;
                doc.set(entry, "pointer", Value::Text("/tools"))?;
                doc.detach(tools, tool)?;
                doc.rename(tool, "declaration")?;
                doc.append(entry, tool)?;
            }
        }
    }
    if let Some(choice) = doc.member(root, "tool_choice")?
        && matches!(doc.node(choice)?.value, Value::Object)
        && is(doc, choice, "type", "function")?
    {
        let function = doc.set(choice, "function", Value::Object)?;
        move_field(doc, choice, "name", function, "name")?;
    }
    Ok(())
}
fn provider(doc: &mut Document, x: usize) -> Result<usize> {
    let p = match doc.member(x, "provider_parameters")? {
        Some(p) => p,
        None => doc.set(x, "provider_parameters", Value::Object)?,
    };
    match doc.member(p, "responses")? {
        Some(p) => Ok(p),
        None => doc.set(p, "responses", Value::Object),
    }
}
fn normalize_item(
    doc: &mut Document,
    owner: &mut Writer,
    item: usize,
    messages: usize,
    x: usize,
    original: usize,
) -> Result<()> {
    let index = doc.children(messages)?.count() as u64;
    if is(doc, item, "type", "function_call")? || is(doc, item, "type", "custom_tool_call")? {
        normalize_call(doc, owner, item, messages, original)
    } else if is(doc, item, "type", "function_call_output")? || is(doc, item, "type", "custom_tool_call_output")? {
        normalize_output(doc, owner, item, messages, original)
    } else if is(doc, item, "type", "reasoning")? || is(doc, item, "type", "compaction")? {
        normalize_reasoning(doc, owner, item, (messages, x, original), index)
    } else if (doc.member(item, "type")?.is_none() && doc.member(item, "role")?.is_some())
        || is(doc, item, "type", "message")?
    {
        normalize_message(doc, owner, item, messages, original)
    } else {
        normalize_opaque(doc, item, messages)
    }
}
/// Wraps a function or custom tool call in an assistant message; the item id is kept as an
/// original field because the canonical call id is the Responses `call_id`.
fn normalize_call(doc: &mut Document, owner: &mut Writer, item: usize, messages: usize, original: usize) -> Result<()> {
    let custom = is(doc, item, "type", "custom_tool_call")?;
    let message = doc.add(None, Value::Object)?;
    doc.append(messages, message)?;
    doc.set(message, "role", Value::Text("assistant"))?;
    let calls = doc.set(message, "tool_calls", Value::Array)?;
    doc.append(calls, item)?;
    if let Some(id) = doc.remove(item, "id")? {
        preserve(doc, owner, original, item, id)?;
    }
    move_field(doc, item, "call_id", item, "id")?;
    doc.set(item, "type", Value::Text(if custom { "custom" } else { "function" }))?;
    doc.set(
        item,
        "arguments_format",
        Value::Text(if custom { "text" } else { "json" }),
    )?;
    let function = doc.set(item, "function", Value::Object)?;
    move_field(doc, item, "name", function, "name")?;
    move_field(doc, item, "namespace", function, "namespace")?;
    if let Some(arguments) = move_field(
        doc,
        item,
        if custom { "input" } else { "arguments" },
        function,
        "arguments",
    )? {
        string(doc, arguments)?;
        if !custom {
            let lease = encode::scratch(owner, |e| {
                let mut chars = doc.chars(arguments)?;
                while let Some(c) = chars.next_scalar()? {
                    e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
                }
                Ok(())
            })?;
            let parsed = doc.import_json(owner, lease)?;
            doc.transplant(arguments, parsed)?;
        }
    }
    preserve_unknown(
        doc,
        owner,
        original,
        item,
        &["id", "type", "arguments_format", "function"],
    )?;
    Ok(())
}
fn normalize_output(
    doc: &mut Document,
    owner: &mut Writer,
    item: usize,
    messages: usize,
    original: usize,
) -> Result<()> {
    let custom = is(doc, item, "type", "custom_tool_call_output")?;
    doc.append(messages, item)?;
    doc.set(item, "role", Value::Text("tool"))?;
    move_field(doc, item, "call_id", item, "tool_call_id")?;
    if let Some(content) = move_field(doc, item, "output", item, "content")?
        && matches!(doc.node(content)?.value, Value::Array)
    {
        let mut next = doc.node(content)?.first;
        while let Some(part) = next {
            next = doc.node(part)?.next;
            normalize_part(doc, owner, part, original)?;
        }
    }
    if custom {
        doc.set(item, "name", Value::Text("custom"))?;
    }
    preserve_unknown(doc, owner, original, item, &["role", "tool_call_id", "content", "name"])?;
    Ok(())
}
/// Turns a reasoning or compaction item into an assistant message of reasoning parts; its
/// encrypted content becomes a replay entry.
fn normalize_reasoning(
    doc: &mut Document,
    owner: &mut Writer,
    item: usize,
    (messages, x, original): (usize, usize, usize),
    index: u64,
) -> Result<()> {
    let compaction = is(doc, item, "type", "compaction")?;
    doc.bind_origin(
        item,
        original,
        Some(if compaction {
            "responses.compaction"
        } else {
            "responses.reasoning"
        }),
    )?;
    doc.append(messages, item)?;
    doc.set(item, "role", Value::Text("assistant"))?;
    let native_content = doc.remove(item, "content")?;
    let content = doc.set(item, "content", Value::Array)?;
    if let Some(parts) = native_content {
        while let Some(part) = doc.node(parts)?.first {
            doc.detach(parts, part)?;
            if is(doc, part, "type", "reasoning_text")? {
                doc.set(part, "type", Value::Text("reasoning"))?;
            }
            preserve_unknown(doc, owner, original, part, &["type", "text"])?;
            doc.append(content, part)?;
        }
    }
    if let Some(summary) = doc.remove(item, "summary")? {
        while let Some(part) = doc.node(summary)?.first {
            doc.detach(summary, part)?;
            doc.set(part, "type", Value::Text("reasoning_summary"))?;
            doc.append(content, part)?;
            preserve_unknown(doc, owner, original, part, &["type", "text"])?;
        }
    }
    if let Some(data) = doc.remove(item, "encrypted_content")? {
        replay_encrypted(doc, item, (x, content), data, index)?;
    }
    preserve_unknown(doc, owner, original, item, &["role", "content"])?;
    Ok(())
}
/// Records encrypted reasoning `data` of message `index` in `x_ck.replay` and marks its place
/// in `content` with an encrypted part.
fn replay_encrypted(
    doc: &mut Document,
    item: usize,
    (x, content): (usize, usize),
    data: usize,
    index: u64,
) -> Result<()> {
    string(doc, data)?;
    let replay = match doc.member(x, "replay")? {
        Some(r) => r,
        None => doc.set(x, "replay", Value::Array)?,
    };
    let replay_index = doc.children(replay)?.count() as u64;
    let entry = doc.add(None, Value::Object)?;
    doc.append(replay, entry)?;
    doc.set(entry, "message_index", Value::Unsigned(index))?;
    doc.set(
        entry,
        "block_index",
        Value::Unsigned(doc.children(content)?.count() as u64),
    )?;
    doc.set(entry, "kind", Value::Text("responses_encrypted"))?;
    doc.set(entry, "source", Value::Text("responses"))?;
    if let Some(id) = doc.member(item, "id")? {
        let id_value = doc.node(id)?.value;
        doc.set(entry, "item_id", id_value)?;
    }
    doc.rename(data, "signature")?;
    doc.append(entry, data)?;
    let part = doc.add(None, Value::Object)?;
    doc.append(content, part)?;
    doc.set(part, "type", Value::Text("encrypted"))?;
    doc.set(part, "replay_index", Value::Unsigned(replay_index))?;
    Ok(())
}
fn normalize_message(
    doc: &mut Document,
    owner: &mut Writer,
    item: usize,
    messages: usize,
    original: usize,
) -> Result<()> {
    doc.append(messages, item)?;
    if let Some(content) = doc.member(item, "content")?
        && matches!(doc.node(content)?.value, Value::Array)
    {
        let mut next = doc.node(content)?.first;
        while let Some(part) = next {
            next = doc.node(part)?.next;
            normalize_part(doc, owner, part, original)?;
        }
    }
    preserve_unknown(doc, owner, original, item, &["role", "content"])?;
    Ok(())
}
/// Keeps an item kind without a canonical form as an opaque part of an assistant message.
fn normalize_opaque(doc: &mut Document, item: usize, messages: usize) -> Result<()> {
    let message = doc.add(None, Value::Object)?;
    doc.append(messages, message)?;
    doc.set(message, "role", Value::Text("assistant"))?;
    let content = doc.set(message, "content", Value::Array)?;
    let part = doc.add(None, Value::Object)?;
    doc.append(content, part)?;
    doc.set(part, "type", Value::Text("opaque"))?;
    doc.set(part, "protocol", Value::Text("responses"))?;
    doc.rename(item, "value")?;
    doc.bind_origin(part, message, Some("responses.item"))?;
    doc.append(part, item)?;
    Ok(())
}
fn normalize_part(doc: &mut Document, owner: &mut Writer, part: usize, original: usize) -> Result<()> {
    let kind = doc.required(part, "type")?;
    if doc.string_is(kind, "input_text")?
        || doc.string_is(kind, "output_text")?
        || doc.string_is(kind, "summary_text")?
    {
        let summary = doc.string_is(kind, "summary_text")?;
        doc.set(
            part,
            "type",
            Value::Text(if summary { "reasoning_summary" } else { "text" }),
        )?;
        string(doc, doc.required(part, "text")?)?;
        preserve_unknown(doc, owner, original, part, &["type", "text"])?;
    } else if doc.string_is(kind, "refusal")? {
        move_field(doc, part, "refusal", part, "text")?;
        preserve_unknown(doc, owner, original, part, &["type", "text"])?;
    } else if doc.string_is(kind, "input_audio")? {
        doc.set(part, "type", Value::Text("audio"))?;
        let source = doc.required(part, "input_audio")?;
        doc.detach(part, source)?;
        doc.rename(source, "source")?;
        doc.append(part, source)?;
        let format = doc.required(source, "format")?;
        let mime = if doc.string_is(format, "mp3")? {
            "audio/mpeg"
        } else if doc.string_is(format, "wav")? {
            "audio/wav"
        } else {
            return Err(Error::Unsupported);
        };
        doc.set(part, "mime", Value::Text(mime))?;
        let data = doc.required(source, "data")?;
        doc.rename(data, "ref")?;
        doc.set(source, "kind", Value::Text("data"))?;
        preserve_unknown(doc, owner, original, source, &["kind", "ref"])?;
        preserve_unknown(doc, owner, original, part, &["type", "source", "mime"])?;
    } else if doc.string_is(kind, "input_image")? || doc.string_is(kind, "input_file")? {
        let image = doc.string_is(kind, "input_image")?;
        doc.set(part, "type", Value::Text(if image { "image" } else { "document" }))?;
        let source = doc.set(part, "source", Value::Object)?;
        if move_field(doc, part, if image { "image_url" } else { "file_url" }, source, "uri")?.is_some() {
            doc.set(source, "kind", Value::Text("uri"))?;
        } else if move_field(doc, part, "file_id", source, "file_id")?.is_some() {
            doc.set(source, "kind", Value::Text("file"))?;
        } else if move_field(doc, part, "file_data", source, "ref")?.is_some() {
            doc.set(source, "kind", Value::Text("data"))?;
        } else {
            return Err(Error::Schema);
        }
        preserve_unknown(doc, owner, original, part, &["type", "source", "detail", "filename"])?;
    } else {
        let value = doc.add(None, Value::Object)?;
        while let Some(child) = doc.node(part)?.first {
            doc.detach(part, child)?;
            doc.append(value, child)?;
        }
        doc.set(part, "type", Value::Text("opaque"))?;
        doc.set(part, "protocol", Value::Text("responses"))?;
        doc.rename(value, "value")?;
        doc.append(part, value)?;
    }
    Ok(())
}
fn preserve(doc: &mut Document, owner: &mut Writer, original: usize, target: usize, value: usize) -> Result<()> {
    let name = doc.node(value)?.name.ok_or(Error::Schema)?;
    let entry = doc.add(None, Value::Object)?;
    doc.append(original, entry)?;
    doc.set(entry, "protocol", Value::Text("responses"))?;
    let lease = encode::scratch(owner, |e| {
        e.bytes(b"\"/")?;
        let mut chars = doc.name_chars(name)?;
        while let Some(c) = chars.next_scalar()? {
            match c {
                '~' => e.bytes(b"~0")?,
                '/' => e.bytes(b"~1")?,
                _ => e.escaped(c.encode_utf8(&mut [0; 4]))?,
            }
        }
        e.bytes(b"\"")
    })?;
    let pointer = doc.import_json(owner, lease)?;
    doc.rename(pointer, "pointer")?;
    doc.append(entry, pointer)?;
    doc.rename(value, "value")?;
    doc.append(entry, value)?;
    doc.bind_origin(entry, target, None)
}
fn preserve_unknown(
    doc: &mut Document,
    owner: &mut Writer,
    original: usize,
    target: usize,
    known: &[&str],
) -> Result<()> {
    let mut next = doc.node(target)?.first;
    while let Some(child) = next {
        next = doc.node(child)?.next;
        let name = doc.node(child)?.name.ok_or(Error::Schema)?;
        let mut keep = false;
        for field in known {
            keep |= doc.name_is(name, field)?;
        }
        if !keep {
            doc.detach(target, child)?;
            preserve(doc, owner, original, target, child)?;
        }
    }
    Ok(())
}
