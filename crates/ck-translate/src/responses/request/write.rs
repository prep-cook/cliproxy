use super::super::{is, member};
use super::native::{item_type, native, typed};
use crate::codec::{
    Error, Operation, RequestView, Result,
    document::{Document, Name, Value},
    encode::Encoder,
};

/// Writes the Responses fields of `target` that normalization kept in `x_ck.original`; fields
/// another protocol kept there mean nothing to a Responses upstream.
fn originals(doc: &Document, target: usize, e: &mut Encoder<'_, '_>, skip: &[&str]) -> Result<()> {
    let Some(x) = doc.member(doc.root(), "x_ck")? else {
        return Ok(());
    };
    let Some(original) = doc.member(x, "original")? else {
        return Ok(());
    };
    for entry in doc.origins(target)? {
        if doc.node(entry)?.parent != Some(original) || !is(doc, entry, "protocol", "responses")? {
            continue;
        }
        let pointer = doc.required(entry, "pointer")?;
        let Value::String(text) = doc.node(pointer)?.value else {
            return Err(Error::Schema);
        };
        let field_name = Name::Pointer(text);
        let mut skipped = false;
        for name in skip {
            skipped |= doc.name_is(field_name, name)?;
        }
        if skipped {
            continue;
        }
        e.bytes(b",")?;
        doc.write_name(field_name, e)?;
        e.bytes(b":")?;
        doc.write(doc.required(entry, "value")?, e)?;
    }
    Ok(())
}
pub(super) fn write_request(view: RequestView<'_>, streaming: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let root = doc.root();
    e.bytes(b"{\"input\":[")?;
    let mut first = true;
    let mut replay = Replay {
        next: match doc.member(root, "x_ck")? {
            Some(x) => doc
                .member(x, "replay")?
                .map(|r| doc.node(r))
                .transpose()?
                .and_then(|r| r.first),
            None => None,
        },
        index: 0,
    };
    for message in doc.children(doc.required(root, "messages")?)? {
        if !instructions(doc, message)? {
            write_message(view, message, &mut replay, &mut first, e)?;
        }
    }
    e.bytes(b"]")?;
    if view.operation != Operation::Compact {
        e.bytes(b",\"stream\":")?;
        e.bytes(if streaming { b"true" } else { b"false" })?;
    }
    for message in doc.children(doc.required(root, "messages")?)? {
        if instructions(doc, message)? {
            e.bytes(b",\"instructions\":")?;
            doc.write(doc.required(message, "content")?, e)?;
        }
    }
    for field in doc.children(root)? {
        write_field(doc, field, e)?;
    }
    if let Some(x) = doc.member(root, "x_ck")? {
        write_extensions(doc, x, e)?;
    }
    originals(doc, root, e, &[])?;
    e.bytes(b"}")
}
/// Writes a canonical root field under its Responses name and shape.
fn write_field(doc: &Document, field: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let name = doc.node(field)?.name.ok_or(Error::Schema)?;
    if doc.name_is(name, "messages")? || doc.name_is(name, "stream")? || doc.name_is(name, "x_ck")? {
        return Ok(());
    }
    // Chat's `max_tokens` is the older spelling of the same limit; `max_completion_tokens` wins.
    let legacy = doc.name_is(name, "max_tokens")?;
    if legacy && doc.member(doc.root(), "max_completion_tokens")?.is_some() {
        return Ok(());
    }
    e.bytes(b",")?;
    if legacy || doc.name_is(name, "max_completion_tokens")? {
        e.string("max_output_tokens")?;
    } else if doc.name_is(name, "response_format")? {
        e.string("text")?;
    } else {
        doc.write_name(name, e)?;
    }
    e.bytes(b":")?;
    if doc.name_is(name, "response_format")? {
        e.bytes(b"{\"format\":")?;
        write_format(doc, field, e)?;
        if let Some(text) = provider_text(doc)? {
            for f in doc.children(text)? {
                e.bytes(b",")?;
                doc.write_name(doc.node(f)?.name.ok_or(Error::Schema)?, e)?;
                e.bytes(b":")?;
                doc.write(f, e)?;
            }
        }
        e.bytes(b"}")?;
    } else if doc.name_is(name, "tools")? {
        write_tools(doc, field, e)?;
    } else if doc.name_is(name, "tool_choice")?
        && matches!(doc.node(field)?.value, Value::Object)
        && is(doc, field, "type", "function")?
    {
        e.bytes(b"{\"type\":\"function\"")?;
        member(doc, doc.required(field, "function")?, "name", e)?;
        e.bytes(b"}")?;
    } else {
        doc.write(field, e)?;
    }
    Ok(())
}
/// The `text` options other than `format` that ingress kept as Responses provider parameters.
fn provider_text(doc: &Document) -> Result<Option<usize>> {
    let Some(x) = doc.member(doc.root(), "x_ck")? else {
        return Ok(None);
    };
    let Some(providers) = doc.member(x, "provider_parameters")? else {
        return Ok(None);
    };
    match doc.member(providers, "responses")? {
        Some(p) => doc.member(p, "text"),
        None => Ok(None),
    }
}
/// Writes a canonical response format; Responses spells `json_schema` members flat.
fn write_format(doc: &Document, format: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let schema = match doc.node(format)?.value {
        Value::Object => doc.member(format, "json_schema")?,
        _ => None,
    };
    let Some(schema) = schema.filter(|&s| matches!(doc.node(s).map(|n| n.value), Ok(Value::Object))) else {
        return doc.write(format, e);
    };
    e.bytes(b"{")?;
    let fields = doc.children(format)?.filter(|&f| f != schema);
    for (i, f) in fields.chain(doc.children(schema)?).enumerate() {
        if i > 0 {
            e.bytes(b",")?;
        }
        doc.write_name(doc.node(f)?.name.ok_or(Error::Schema)?, e)?;
        e.bytes(b":")?;
        doc.write(f, e)?;
    }
    e.bytes(b"}")
}
/// Writes the function tools and the native tool declarations kept in `x_ck.tools`; a
/// declaration whose pointer names its position returns there, others follow the functions.
fn write_tools(doc: &Document, field: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let native = match doc.member(doc.root(), "x_ck")? {
        Some(x) => doc.member(x, "tools")?,
        None => None,
    };
    let mut natives = native
        .map(|n| doc.children(n))
        .transpose()?
        .into_iter()
        .flatten()
        .peekable();
    let mut functions = doc.children(field)?.peekable();
    e.bytes(b"[")?;
    for position in 0u64.. {
        let at = match natives.peek() {
            Some(&entry) => functions.peek().is_none() || position_of(doc, entry)?.is_some_and(|p| p <= position),
            None => false,
        };
        if position > 0 && (at || functions.peek().is_some()) {
            e.bytes(b",")?;
        }
        if at && let Some(entry) = natives.next() {
            doc.write(doc.required(entry, "declaration")?, e)?;
        } else if let Some(tool) = functions.next() {
            e.bytes(b"{\"type\":\"function\"")?;
            let function = doc.required(tool, "function")?;
            for f in doc.children(function)? {
                e.bytes(b",")?;
                doc.write_name(doc.node(f)?.name.ok_or(Error::Schema)?, e)?;
                e.bytes(b":")?;
                doc.write(f, e)?;
            }
            originals(doc, tool, e, &["type", "function"])?;
            e.bytes(b"}")?;
        } else {
            break;
        }
    }
    e.bytes(b"]")
}
/// The position a `/tools/N` declaration pointer names; `None` for any other pointer.
fn position_of(doc: &Document, entry: usize) -> Result<Option<u64>> {
    let mut chars = doc.chars(doc.required(entry, "pointer")?)?;
    for expected in "/tools/".chars() {
        if chars.next_scalar()? != Some(expected) {
            return Ok(None);
        }
    }
    let mut position = None::<u64>;
    while let Some(c) = chars.next_scalar()? {
        let digit = u64::from(c.to_digit(10).ok_or(Error::Schema)?);
        let value = position.unwrap_or(0).checked_mul(10).and_then(|p| p.checked_add(digit));
        position = Some(value.ok_or(Error::Schema)?);
    }
    Ok(position)
}
/// Writes the reasoning options, the continuation and the Responses provider parameters.
fn write_extensions(doc: &Document, x: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    if let Some(thinking) = doc.member(x, "thinking")? {
        e.bytes(b",\"reasoning\":{")?;
        for (i, f) in doc.children(thinking)?.enumerate() {
            if i > 0 {
                e.bytes(b",")?;
            }
            let name = doc.node(f)?.name.ok_or(Error::Schema)?;
            if doc.name_is(name, "include")? {
                e.string("summary")?;
            } else {
                doc.write_name(name, e)?;
            }
            e.bytes(b":")?;
            doc.write(f, e)?;
        }
        e.bytes(b"}")?;
    }
    if let Some(session) = doc.member(x, "session")? {
        member(doc, session, "previous_response_id", e)?;
    }
    if let Some(providers) = doc.member(x, "provider_parameters")?
        && let Some(p) = doc.member(providers, "responses")?
    {
        let format = doc.member(doc.root(), "response_format")?.is_some();
        for f in doc.children(p)? {
            let name = doc.node(f)?.name.ok_or(Error::Schema)?;
            if format && doc.name_is(name, "text")? {
                // Written beside `format` in the same `text` object.
                continue;
            }
            e.bytes(b",")?;
            doc.write_name(name, e)?;
            e.bytes(b":")?;
            doc.write(f, e)?;
        }
    }
    Ok(())
}
/// True for the developer message that carries the Responses `instructions` field.
fn instructions(doc: &Document, message: usize) -> Result<bool> {
    Ok(native(doc, message, "instructions")?.is_some() && is(doc, message, "role", "developer")?)
}
/// Starts the next input item.
fn separate(first: &mut bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    if !*first {
        e.bytes(b",")?;
    }
    *first = false;
    Ok(())
}
/// Writes the input items of one message: a tool output; or its reasoning item, its message
/// item and its calls, in that order.
fn write_message(
    view: RequestView<'_>,
    message: usize,
    replay: &mut Replay,
    first: &mut bool,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    let doc = view.document;
    if is(doc, message, "role", "tool")? {
        separate(first, e)?;
        return write_tool_output(view, message, e);
    }
    let calls = doc.member(message, "tool_calls")?;
    if let Some(content) = doc.member(message, "content")? {
        let kind = item_type(doc, message)?;
        let native = typed(doc, kind, "reasoning")? || typed(doc, kind, "compaction")?;
        if let Some(part) = doc.node(content)?.first
            && kind.is_some()
            && !native
            && is(doc, part, "type", "opaque")?
        {
            separate(first, e)?;
            return doc.write(doc.required(part, "value")?, e);
        }
        let reasoning = native || any_part(doc, content, true)?;
        if reasoning {
            separate(first, e)?;
            write_reasoning(doc, message, content, replay, e)?;
        }
        let text = match doc.node(content)?.value {
            Value::Array => any_part(doc, content, false)? || (!reasoning && calls.is_none()),
            Value::Null => calls.is_none(),
            _ => calls.is_none() || doc.chars(content)?.next_scalar()?.is_some(),
        };
        if text {
            separate(first, e)?;
            write_message_item(view, message, content, e)?;
        }
    }
    if let Some(calls) = calls {
        for call in doc.children(calls)? {
            separate(first, e)?;
            write_call(doc, call, e)?;
        }
    }
    Ok(())
}
/// True when array `content` has a reasoning part (`reasoning`) or a part of any other kind.
fn any_part(doc: &Document, content: usize, reasoning: bool) -> Result<bool> {
    if !matches!(doc.node(content)?.value, Value::Array) {
        return Ok(false);
    }
    for part in doc.children(content)? {
        let kind = doc.required(part, "type")?;
        let found = doc.string_is(kind, "reasoning")?
            || doc.string_is(kind, "reasoning_summary")?
            || doc.string_is(kind, "encrypted")?;
        if found == reasoning {
            return Ok(true);
        }
    }
    Ok(false)
}
fn write_call(doc: &Document, call: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let custom = is(doc, call, "arguments_format", "text")?;
    e.bytes(b"{\"type\":")?;
    e.string(if custom { "custom_tool_call" } else { "function_call" })?;
    e.bytes(b",\"call_id\":")?;
    doc.write(doc.required(call, "id")?, e)?;
    let function = doc.required(call, "function")?;
    member(doc, function, "name", e)?;
    member(doc, function, "namespace", e)?;
    if let Some(arguments) = doc.member(function, "arguments")? {
        e.bytes(b",")?;
        e.string(if custom { "input" } else { "arguments" })?;
        e.bytes(b":")?;
        if custom {
            doc.write(arguments, e)?;
        } else {
            e.quoted_json(|e| doc.write(arguments, e))?;
        }
    }
    originals(doc, call, e, &[])?;
    e.bytes(b"}")
}
fn write_tool_output(view: RequestView<'_>, message: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    e.bytes(b"{\"type\":")?;
    e.string(if is(doc, message, "name", "custom")? {
        "custom_tool_call_output"
    } else {
        "function_call_output"
    })?;
    e.bytes(b",\"call_id\":")?;
    doc.write(doc.required(message, "tool_call_id")?, e)?;
    e.bytes(b",\"output\":")?;
    write_content(view, doc.required(message, "content")?, false, e)?;
    originals(doc, message, e, &["type"])?;
    e.bytes(b"}")
}
/// Writes string content as is and array content as Responses parts.
fn write_content(view: RequestView<'_>, content: usize, assistant: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    if !matches!(doc.node(content)?.value, Value::Array) {
        return doc.write(content, e);
    }
    e.bytes(b"[")?;
    for (i, part) in doc.children(content)?.enumerate() {
        if i > 0 {
            e.bytes(b",")?;
        }
        write_part(view, part, assistant, e)?;
    }
    e.bytes(b"]")
}
/// Writes a message item with the content parts that are not reasoning.
fn write_message_item(view: RequestView<'_>, message: usize, content: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let assistant = is(doc, message, "role", "assistant")?;
    e.bytes(b"{\"type\":\"message\",\"role\":")?;
    doc.write(doc.required(message, "role")?, e)?;
    e.bytes(b",\"content\":")?;
    if matches!(doc.node(content)?.value, Value::Array) {
        e.bytes(b"[")?;
        let mut first = true;
        for part in doc.children(content)? {
            let kind = doc.required(part, "type")?;
            if doc.string_is(kind, "reasoning")?
                || doc.string_is(kind, "reasoning_summary")?
                || doc.string_is(kind, "encrypted")?
            {
                continue;
            }
            separate(&mut first, e)?;
            write_part(view, part, assistant, e)?;
        }
        e.bytes(b"]")?;
    } else {
        doc.write(content, e)?;
    }
    originals(doc, message, e, &["type", "instructions"])?;
    e.bytes(b"}")
}
/// A cursor over `x_ck.replay`. Encrypted parts name their entries in increasing order, so one
/// walk serves the whole request; entries whose part was deleted are passed over.
struct Replay {
    next: Option<usize>,
    index: u64,
}
impl Replay {
    /// Entry `wanted`, which must be bound to `message`.
    fn seek(&mut self, doc: &Document, wanted: u64, message: usize) -> Result<usize> {
        while self.index < wanted {
            self.next = doc.node(self.next.ok_or(Error::Schema)?)?.next;
            self.index += 1;
        }
        let entry = self.next.filter(|_| self.index == wanted).ok_or(Error::Schema)?;
        if doc.origin(entry)?.map(|(parent, _)| parent) != Some(message) {
            return Err(Error::Schema);
        }
        Ok(entry)
    }
}
/// Writes the reasoning or compaction item of `message` with its summary, full reasoning and
/// the encrypted content its surviving encrypted parts name.
fn write_reasoning(
    doc: &Document,
    message: usize,
    content: usize,
    replay: &mut Replay,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    let compaction = typed(doc, item_type(doc, message)?, "compaction")?;
    e.bytes(b"{\"type\":")?;
    e.string(if compaction { "compaction" } else { "reasoning" })?;
    let has = |kind: &str| -> Result<bool> {
        if !matches!(doc.node(content)?.value, Value::Array) {
            return Ok(false);
        }
        for part in doc.children(content)? {
            if is(doc, part, "type", kind)? {
                return Ok(true);
            }
        }
        Ok(false)
    };
    if !compaction || has("reasoning_summary")? {
        write_reasoning_parts(doc, content, ("summary", "reasoning_summary", "summary_text"), e)?;
    }
    if has("reasoning")? {
        write_reasoning_parts(doc, content, ("content", "reasoning", "reasoning_text"), e)?;
    }
    let parts = matches!(doc.node(content)?.value, Value::Array).then_some(content);
    for part in parts.map(|p| doc.children(p)).transpose()?.into_iter().flatten() {
        if is(doc, part, "type", "encrypted")? {
            let entry = replay.seek(doc, doc.unsigned(doc.required(part, "replay_index")?)?, message)?;
            e.bytes(b",\"encrypted_content\":")?;
            doc.write(doc.required(entry, "signature")?, e)?;
        }
    }
    originals(doc, message, e, &["type"])?;
    e.bytes(b"}")
}
/// Writes member `field` listing the `kind` parts of `content` as `native` parts.
fn write_reasoning_parts(
    doc: &Document,
    content: usize,
    (field, kind, native): (&str, &str, &str),
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    e.bytes(b",")?;
    e.string(field)?;
    e.bytes(b":[")?;
    let mut first = true;
    if matches!(doc.node(content)?.value, Value::Array) {
        for part in doc.children(content)? {
            if is(doc, part, "type", kind)? {
                separate(&mut first, e)?;
                e.bytes(b"{\"type\":")?;
                e.string(native)?;
                e.bytes(b",\"text\":")?;
                doc.write(doc.required(part, "text")?, e)?;
                originals(doc, part, e, &[])?;
                e.bytes(b"}")?;
            }
        }
    }
    e.bytes(b"]")
}
fn write_part(view: RequestView<'_>, part: usize, assistant: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    if is(doc, part, "type", "opaque")? {
        return doc.write(doc.required(part, "value")?, e);
    }
    if is(doc, part, "type", "audio")? {
        let source = doc.required(part, "source")?;
        if !is(doc, source, "kind", "data")? {
            return Err(Error::Unsupported);
        }
        let mime = doc.required(part, "mime")?;
        let format = if doc.string_is(mime, "audio/mpeg")? {
            "mp3"
        } else if doc.string_is(mime, "audio/wav")? {
            "wav"
        } else {
            return Err(Error::Unsupported);
        };
        e.bytes(b"{\"type\":\"input_audio\",\"input_audio\":{\"data\":")?;
        // `format` names the media type, so the data stays plain base64.
        view.media.write_base64(doc, doc.required(source, "ref")?, e)?;
        e.bytes(b",\"format\":")?;
        e.string(format)?;
        originals(doc, source, e, &["format"])?;
        e.bytes(b"}")?;
        originals(doc, part, e, &[])?;
        return e.bytes(b"}");
    }
    if is(doc, part, "type", "image")? || is(doc, part, "type", "document")? {
        let image = is(doc, part, "type", "image")?;
        e.bytes(b"{\"type\":")?;
        e.string(if image { "input_image" } else { "input_file" })?;
        let source = doc.required(part, "source")?;
        if is(doc, source, "kind", "uri")? {
            e.bytes(b",")?;
            e.string(if image { "image_url" } else { "file_url" })?;
            e.bytes(b":")?;
            doc.write(doc.required(source, "uri")?, e)?;
        } else if is(doc, source, "kind", "file")? {
            member(doc, source, "file_id", e)?;
        } else if is(doc, source, "kind", "data")? {
            e.bytes(b",")?;
            e.string(if image { "image_url" } else { "file_data" })?;
            e.bytes(b":")?;
            let mime = doc.member(part, "mime")?;
            view.media
                .write_value(doc, doc.required(source, "ref")?, (mime, true), e)?;
        } else {
            return Err(Error::Unsupported);
        }
        member(doc, part, "detail", e)?;
        member(doc, part, "filename", e)?;
    } else {
        let refusal = is(doc, part, "type", "refusal")?;
        e.bytes(b"{\"type\":")?;
        e.string(if refusal {
            "refusal"
        } else if assistant {
            "output_text"
        } else {
            "input_text"
        })?;
        e.bytes(b",")?;
        e.string(if refusal { "refusal" } else { "text" })?;
        e.bytes(b":")?;
        doc.write(doc.required(part, "text")?, e)?;
    }
    originals(doc, part, e, &[])?;
    e.bytes(b"}")
}
