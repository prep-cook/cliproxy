use crate::codec::{
    Error, Operation, RequestView, Result,
    document::{Document, Value},
    encode::Encoder,
};

pub(super) fn write(view: RequestView<'_>, streaming: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let root = doc.root();
    let x = doc.required(root, "x_ck")?;
    e.bytes(b"{")?;
    if fields(doc, root, e)? {
        e.bytes(b",")?;
    }
    messages(view, root, e)?;
    if view.operation == Operation::Generate {
        e.bytes(b",\"stream\":")?;
        e.bytes(if streaming { b"true" } else { b"false" })?;
    }
    thinking(doc, x, e)?;
    if let Some(provider) = doc.member(x, "provider_parameters")?
        && let Some(native) = doc.member(provider, "anthropic")?
    {
        for field in doc.children(native)? {
            if doc.name_is(doc.node(field)?.name.ok_or(Error::Schema)?, "output_config")? {
                continue;
            }
            e.bytes(b",")?;
            doc.write_name(doc.node(field)?.name.ok_or(Error::Schema)?, e)?;
            e.bytes(b":")?;
            doc.write(field, e)?;
        }
    }
    output_config(doc, x, e)?;
    tools(doc, root, x, e)?;
    tool_choice(doc, root, e)?;
    originals(doc, root, e)?;
    e.bytes(b"}")
}
/// Writes the top-level fields that map one to one, renaming `stop`; returns whether any
/// field was written.
fn fields(doc: &Document, root: usize, e: &mut Encoder<'_, '_>) -> Result<bool> {
    let mut first = true;
    for field in doc.children(root)? {
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        if [
            "x_ck",
            "messages",
            "stream",
            "tools",
            "tool_choice",
            "parallel_tool_calls",
            "response_format",
        ]
        .iter()
        .try_fold(false, |yes, key| Ok::<_, Error>(yes || doc.name_is(name, key)?))?
        {
            continue;
        }
        if !first {
            e.bytes(b",")?;
        }
        first = false;
        if doc.name_is(name, "stop")? {
            e.string("stop_sequences")?;
        } else {
            doc.write_name(name, e)?;
        }
        e.bytes(b":")?;
        if doc.name_is(name, "stop")? && !matches!(doc.node(field)?.value, Value::Array) {
            e.bytes(b"[")?;
            doc.write(field, e)?;
            e.bytes(b"]")?;
        } else {
            doc.write(field, e)?;
        }
    }
    Ok(!first)
}
/// Writes `messages` without the system message, then `system` from it.
fn messages(view: RequestView<'_>, root: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    e.bytes(b"\"messages\":[")?;
    let messages = doc.required(root, "messages")?;
    let x = doc.required(root, "x_ck")?;
    let unbound_replay = match doc.member(x, "replay")? {
        Some(replay) => doc
            .children(replay)?
            .try_fold(false, |any, entry| Ok::<_, Error>(any || doc.origin(entry)?.is_none()))?,
        None => false,
    };
    let mut first_message = true;
    let mut system = None;
    for (message_index, message) in doc.children(messages)?.enumerate() {
        if super::is(doc, message, "role", "system")? {
            if system.is_some() {
                return Err(Error::Unsupported);
            }
            system = Some((message_index as u64, message));
            continue;
        }
        if !first_message {
            e.bytes(b",")?;
        }
        first_message = false;
        let at = At {
            message: message_index as u64,
            unbound_replay,
        };
        message_value(view, message, at, e)?;
    }
    e.bytes(b"]")?;
    if let Some((message, node)) = system {
        e.bytes(b",\"system\":")?;
        let at = At {
            message,
            unbound_replay,
        };
        content(view, doc.required(node, "content")?, at, e)?;
    }
    Ok(())
}
fn thinking(doc: &Document, x: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let Some(thinking) = doc.member(x, "thinking")? else {
        return Ok(());
    };
    if doc.member(thinking, "native")?.is_none()
        && doc.member(thinking, "enabled")?.is_none()
        && doc.member(thinking, "budget_tokens")?.is_none()
    {
        return Ok(());
    }
    e.bytes(b",\"thinking\":")?;
    e.bytes(b"{\"type\":")?;
    let enabled = doc
        .member(thinking, "enabled")?
        .map(|n| doc.node(n))
        .transpose()?
        .is_some_and(|n| matches!(n.value, Value::Bool(true)));
    let native = doc.member(thinking, "native")?;
    let adaptive = native
        .map(|n| super::is(doc, n, "type", "adaptive"))
        .transpose()?
        .unwrap_or(false);
    e.string(if !enabled {
        "disabled"
    } else if adaptive {
        "adaptive"
    } else {
        "enabled"
    })?;
    super::member(doc, thinking, "budget_tokens", e)?;
    if let Some(native) = native {
        for field in doc.children(native)? {
            let name = doc.node(field)?.name.ok_or(Error::Schema)?;
            if doc.name_is(name, "type")? || doc.name_is(name, "budget_tokens")? {
                continue;
            }
            e.bytes(b",")?;
            doc.write_name(name, e)?;
            e.bytes(b":")?;
            doc.write(field, e)?;
        }
    }
    e.bytes(b"}")?;
    Ok(())
}
fn tools(doc: &Document, root: usize, x: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let Some(tools) = doc.member(root, "tools")? else {
        return Ok(());
    };
    e.bytes(b",\"tools\":[")?;
    for (index, tool) in doc.children(tools)?.enumerate() {
        if index != 0 {
            e.bytes(b",")?;
        }
        if super::is(doc, tool, "type", "server")? {
            let declarations = doc.required(x, "tools")?;
            let entry = declaration(doc, declarations, tool)?.ok_or(Error::Schema)?;
            let declaration = doc.required(entry, "declaration")?;
            e.bytes(b"{")?;
            for (index, field) in doc.children(declaration)?.enumerate() {
                if index != 0 {
                    e.bytes(b",")?;
                }
                doc.write_name(doc.node(field)?.name.ok_or(Error::Schema)?, e)?;
                e.bytes(b":")?;
                doc.write(field, e)?;
            }
            originals(doc, tool, e)?;
            e.bytes(b"}")?;
        } else {
            let function = doc.required(tool, "function")?;
            e.bytes(b"{")?;
            for (i, field) in doc.children(function)?.enumerate() {
                if i != 0 {
                    e.bytes(b",")?;
                }
                let name = doc.node(field)?.name.ok_or(Error::Schema)?;
                if doc.name_is(name, "parameters")? {
                    e.string("input_schema")?;
                } else {
                    doc.write_name(name, e)?;
                }
                e.bytes(b":")?;
                doc.write(field, e)?;
            }
            originals(doc, tool, e)?;
            e.bytes(b"}")?;
        }
    }
    e.bytes(b"]")?;
    Ok(())
}
/// The x_ck.tools entry bound to a server tool, found through the tool's own bindings.
pub(super) fn declaration(doc: &Document, declarations: usize, tool: usize) -> Result<Option<usize>> {
    for entry in doc.origins(tool)? {
        if doc.node(entry)?.parent == Some(declarations) {
            return Ok(Some(entry));
        }
    }
    Ok(None)
}
fn tool_choice(doc: &Document, root: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let choice = doc.member(root, "tool_choice")?;
    let parallel = doc.member(root, "parallel_tool_calls")?;
    if choice.is_none() && parallel.is_none() {
        return Ok(());
    }
    e.bytes(b",\"tool_choice\":{\"type\":")?;
    if let Some(choice) = choice {
        if matches!(doc.node(choice)?.value, Value::Object) {
            e.string("tool")?;
            let function = doc.required(choice, "function")?;
            super::member(doc, function, "name", e)?;
            originals(doc, choice, e)?;
        } else {
            e.string(if doc.string_is(choice, "required")? {
                "any"
            } else if doc.string_is(choice, "none")? {
                "none"
            } else {
                "auto"
            })?;
            originals(doc, choice, e)?;
        }
    } else {
        e.string("auto")?;
    }
    if let Some(parallel) = parallel {
        let Value::Bool(value) = doc.node(parallel)?.value else {
            return Err(Error::Schema);
        };
        e.bytes(b",\"disable_parallel_tool_use\":")?;
        e.bytes(if value { b"false" } else { b"true" })?;
    }
    e.bytes(b"}")?;
    Ok(())
}
/// A message's position, and whether any replay entry is matched by position rather than binding.
#[derive(Clone, Copy)]
struct At {
    message: u64,
    unbound_replay: bool,
}
fn message_value(view: RequestView<'_>, message: usize, at: At, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    e.bytes(b"{\"role\":")?;
    let tool_result = super::is(doc, message, "role", "tool")?;
    if tool_result {
        e.string("user")?;
    } else {
        doc.write(doc.required(message, "role")?, e)?;
    }
    e.bytes(b",\"content\":")?;
    if tool_result {
        e.bytes(b"[{\"type\":\"tool_result\",\"tool_use_id\":")?;
        doc.write(doc.required(message, "tool_call_id")?, e)?;
        if let Some(value) = doc.member(message, "content")? {
            e.bytes(b",\"content\":")?;
            content(view, value, at, e)?;
        }
        e.bytes(b"}]")?;
    } else if let Some(calls) = doc.member(message, "tool_calls")? {
        e.bytes(b"[")?;
        let content = doc.member(message, "content")?;
        let mut part = content.and_then(|n| doc.node(n).ok().and_then(|n| n.first));
        let string_content = content.filter(|n| {
            matches!(
                doc.node(*n).map(|n| n.value),
                Ok(Value::String(_) | Value::Text(_) | Value::Fragments | Value::Utf8Media)
            )
        });
        let mut call = doc.node(calls)?.first;
        let mut first = true;
        let mut block_index = 0;
        if let Some(text) = string_content {
            e.bytes(b"{\"type\":\"text\",\"text\":")?;
            doc.write(text, e)?;
            e.bytes(b"}")?;
            first = false;
            block_index += 1;
        }
        while part.is_some() || call.is_some() {
            let use_call = if let Some(call) = call {
                call_position(doc, call)?.is_some_and(|position| position <= block_index) || part.is_none()
            } else {
                false
            };
            if !first {
                e.bytes(b",")?;
            }
            first = false;
            if use_call {
                let node = call.ok_or(Error::Schema)?;
                call = doc.node(node)?.next;
                tool_call(doc, node, e)?;
            } else {
                let node = part.ok_or(Error::Schema)?;
                part = doc.node(node)?.next;
                part_value(view, node, at, block_index, e)?;
            }
            block_index += 1;
        }
        e.bytes(b"]")?;
    } else if let Some(value) = doc.member(message, "content")? {
        content(view, value, at, e)?;
    } else {
        e.bytes(b"[]")?;
    }
    originals(doc, message, e)?;
    e.bytes(b"}")
}
fn call_position(doc: &Document, call: usize) -> Result<Option<u64>> {
    super::content::history(doc, call)?
        .map(|entry| doc.unsigned(doc.required(entry, "block_index")?))
        .transpose()
}
fn tool_call(doc: &Document, call: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{\"type\":")?;
    if let Some(entry) = super::content::history(doc, call)?
        && let Some(native) = doc.member(entry, "native_type")?
    {
        doc.write(native, e)?;
    } else {
        e.string(if super::is(doc, call, "type", "server")? {
            "server_tool_use"
        } else {
            "tool_use"
        })?;
    }
    if let Some(namespace) = doc.member(call, "namespace")? {
        e.bytes(b",\"server_name\":")?;
        doc.write(namespace, e)?;
    }
    super::member(doc, call, "id", e)?;
    let function = doc.required(call, "function")?;
    super::member(doc, function, "name", e)?;
    if let Some(input) = doc.member(function, "arguments")? {
        e.bytes(b",\"input\":")?;
        doc.write(input, e)?;
    }
    originals(doc, call, e)?;
    e.bytes(b"}")
}
fn content(view: RequestView<'_>, node: usize, at: At, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    if !matches!(doc.node(node)?.value, Value::Array) {
        return doc.write(node, e);
    }
    e.bytes(b"[")?;
    for (index, part) in doc.children(node)?.enumerate() {
        if index != 0 {
            e.bytes(b",")?;
        }
        part_value(view, part, at, index as u64, e)?;
    }
    e.bytes(b"]")
}
fn part_value(view: RequestView<'_>, part: usize, at: At, block_index: u64, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let kind = doc.required(part, "type")?;
    if doc.string_is(kind, "opaque")? {
        return opaque_part(doc, part, e);
    }
    e.bytes(b"{\"type\":")?;
    if doc.string_is(kind, "reasoning")? {
        e.string("thinking")?;
        e.bytes(b",\"thinking\":")?;
        doc.write(doc.required(part, "text")?, e)?;
        let x = doc.required(doc.root(), "x_ck")?;
        if let Some(replay) = doc.member(x, "replay")? {
            for entry in doc.origins(part)? {
                if doc.node(entry)?.parent == Some(replay) {
                    super::member(doc, entry, "signature", e)?;
                }
            }
            // Imported documents carry no bindings; their entries match by position.
            if at.unbound_replay {
                for entry in doc.children(replay)? {
                    if doc.origin(entry)?.is_none()
                        && doc.unsigned(doc.required(entry, "message_index")?)? == at.message
                        && doc.unsigned(doc.required(entry, "block_index")?)? == block_index
                    {
                        super::member(doc, entry, "signature", e)?;
                    }
                }
            }
        }
    } else if doc.string_is(kind, "redacted")? {
        e.string("redacted_thinking")?;
        super::member(doc, part, "data", e)?;
    } else if doc.string_is(kind, "image")? || doc.string_is(kind, "document")? {
        media_source(view, part, kind, e)?;
    } else if doc.string_is(kind, "tool_result")? {
        if let Some(entry) = super::content::history(doc, part)?
            && let Some(native) = doc.member(entry, "native_type")?
        {
            doc.write(native, e)?;
        } else {
            e.string("tool_result")?;
        }
        e.bytes(b",\"tool_use_id\":")?;
        doc.write(doc.required(part, "tool_call_id")?, e)?;
        if let Some(value) = doc.member(part, "content")? {
            e.bytes(b",\"content\":")?;
            if shape(doc, part, "string")? {
                let text = doc.node(value)?.first.ok_or(Error::Schema)?;
                doc.write(doc.required(text, "text")?, e)?;
            } else if shape(doc, part, "object")? {
                let item = doc.node(value)?.first.ok_or(Error::Schema)?;
                doc.write(doc.required(item, "value")?, e)?;
            } else {
                content(view, value, at, e)?;
            }
        }
        super::member(doc, part, "is_error", e)?;
    } else {
        doc.write(kind, e)?;
        super::member(doc, part, "text", e)?;
    }
    originals(doc, part, e)?;
    e.bytes(b"}")
}
/// Writes an unknown block with the cache directive normalization moved to x_ck.cache_control.
fn opaque_part(doc: &Document, part: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let value = doc.required(part, "value")?;
    if !matches!(doc.node(value)?.value, Value::Object) || doc.node(value)?.first.is_none() {
        return doc.write(value, e);
    }
    e.bytes(b"{")?;
    for (index, field) in doc.children(value)?.enumerate() {
        if index != 0 {
            e.bytes(b",")?;
        }
        doc.write_name(doc.node(field)?.name.ok_or(Error::Schema)?, e)?;
        e.bytes(b":")?;
        doc.write(field, e)?;
    }
    originals(doc, part, e)?;
    e.bytes(b"}")
}
fn media_source(view: RequestView<'_>, part: usize, kind: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    doc.write(kind, e)?;
    let source = doc.required(part, "source")?;
    e.bytes(b",\"source\":{\"type\":")?;
    if super::is(doc, source, "kind", "data")? {
        let text = doc.member(source, "anthropic_text")?.is_some();
        e.string(if text { "text" } else { "base64" })?;
        if let Some(mime) = doc.member(part, "mime")? {
            e.bytes(b",\"media_type\":")?;
            doc.write(mime, e)?;
        }
        e.bytes(b",\"data\":")?;
        view.media
            .write_value(doc, doc.required(source, "ref")?, (doc.member(part, "mime")?, false), e)?;
    } else if super::is(doc, source, "kind", "uri")? {
        e.string("url")?;
        e.bytes(b",\"url\":")?;
        doc.write(doc.required(source, "uri")?, e)?;
    } else {
        e.string("file")?;
        super::member(doc, source, "file_id", e)?;
    }
    originals(doc, source, e)?;
    e.bytes(b"}")?;
    Ok(())
}
fn shape(doc: &Document, parent: usize, expected: &str) -> Result<bool> {
    let x = doc.required(doc.root(), "x_ck")?;
    let original = doc.member(x, "original")?;
    // Provenance bound to this node, not the whole original list.
    for entry in doc.origins(parent)? {
        if original.is_some()
            && doc.node(entry)?.parent == original
            && doc.origin(entry)? == Some((parent, Some("shape")))
        {
            return doc.string_is(doc.required(entry, "value")?, expected);
        }
    }
    Ok(false)
}
fn originals(doc: &Document, parent: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let x = doc.required(doc.root(), "x_ck")?;
    for field in ["original", "cache_control"] {
        if let Some(original) = doc.member(x, field)? {
            // Provenance bound to this node, not the whole list.
            for entry in doc.origins(parent)? {
                if doc.node(entry)?.parent != Some(original) || doc.origin(entry)? != Some((parent, None)) {
                    continue;
                }
                let pointer = doc.required(entry, "pointer")?;
                let mut chars = doc.chars(pointer)?;
                if chars.next_scalar()? != Some('/') {
                    return Err(Error::Schema);
                }
                e.bytes(b",\"")?;
                while let Some(c) = chars.next_scalar()? {
                    let c = if c == '~' {
                        match chars.next_scalar()? {
                            Some('0') => '~',
                            Some('1') => '/',
                            _ => return Err(Error::Schema),
                        }
                    } else {
                        c
                    };
                    e.escaped(c.encode_utf8(&mut [0; 4]))?;
                }
                e.bytes(b"\":")?;
                doc.write(doc.required(entry, "value")?, e)?;
            }
        }
    }
    Ok(())
}

fn output_config(doc: &Document, x: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let native = match doc.member(x, "provider_parameters")? {
        Some(provider) => match doc.member(provider, "anthropic")? {
            Some(native) => doc.member(native, "output_config")?,
            None => None,
        },
        None => None,
    };
    let effort = match doc.member(x, "thinking")? {
        Some(thinking) => doc.member(thinking, "effort")?,
        None => None,
    };
    let format = doc.member(doc.root(), "response_format")?;
    if native.is_none() && effort.is_none() && format.is_none() {
        return Ok(());
    }
    e.bytes(b",\"output_config\":{")?;
    let mut first = true;
    if let Some(native) = native {
        for field in doc.children(native)? {
            if !first {
                e.bytes(b",")?;
            }
            first = false;
            doc.write_name(doc.node(field)?.name.ok_or(Error::Schema)?, e)?;
            e.bytes(b":")?;
            doc.write(field, e)?;
        }
    }
    if let Some(effort) = effort {
        if !first {
            e.bytes(b",")?;
        }
        first = false;
        e.bytes(b"\"effort\":")?;
        doc.write(effort, e)?;
    }
    if let Some(format) = format {
        if !first {
            e.bytes(b",")?;
        }
        e.bytes(b"\"format\":{\"type\":")?;
        doc.write(doc.required(format, "type")?, e)?;
        let schema = doc.required(format, "json_schema")?;
        for field in doc.children(schema)? {
            e.bytes(b",")?;
            doc.write_name(doc.node(field)?.name.ok_or(Error::Schema)?, e)?;
            e.bytes(b":")?;
            doc.write(field, e)?;
        }
        e.bytes(b"}")?;
    }
    e.bytes(b"}")
}
