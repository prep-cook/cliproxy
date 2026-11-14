use super::Table;
use super::request::{capture, preserve};
use crate::codec::{
    Error, Result,
    document::{Document, Text, Value},
    media::{self, Encoding, Registry},
    source::Span,
};
use ck_stream::Writer;
use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hasher},
};

pub(super) fn normalize(
    owner: &mut Writer,
    doc: &mut Document,
    message: usize,
    message_index: u64,
    calls: &mut Calls,
) -> Result<()> {
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
        let place = Place {
            message,
            content,
            entry,
            message_index,
            index,
        };
        normalize_part(owner, doc, part, place, calls)?;
        doc.bind_origin(entry, part, Some("part"))?;
        index += 1;
    }
    Ok(())
}
/// Where a part sits: its message, the message's content array, its order entry and indexes.
#[derive(Clone, Copy)]
struct Place {
    message: usize,
    content: usize,
    entry: usize,
    message_index: u64,
    index: u64,
}
/// Moves one native part to its canonical home: a tool call, a tool result or a content part.
fn normalize_part(owner: &mut Writer, doc: &mut Document, part: usize, place: Place, calls: &mut Calls) -> Result<()> {
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
        replay(doc, part, signature, place)?;
    }
    if let Some(call) = doc.remove(part, "functionCall")? {
        return tool_call(doc, part, call, place, calls);
    }
    if let Some(result) = doc.remove(part, "functionResponse")? {
        return tool_result(doc, part, result, place, calls);
    }
    doc.append(place.content, part)?;
    doc.set(place.entry, "kind", Value::Text("content"))?;
    if let Some(text) = doc.member(part, "text")? {
        super::string(doc, text)?;
        let reasoning = match doc.member(part, "thought")? {
            Some(n) => match doc.node(n)?.value {
                Value::Bool(b) => b,
                _ => return Err(Error::Schema),
            },
            None => false,
        };
        if let Some(n) = doc.remove(part, "thought")? {
            preserve(owner, doc, part, n, None)?;
        }
        doc.set(part, "type", Value::Text(if reasoning { "reasoning" } else { "text" }))?;
        capture(owner, doc, part, &["type", "text"], None)
    } else if let Some(data) = doc.remove(part, "inlineData")? {
        media_part(owner, doc, part, data, true)
    } else if let Some(data) = doc.remove(part, "fileData")? {
        media_part(owner, doc, part, data, false)
    } else if payloads == 0 && super::origin(doc, part, "signature")?.is_some() {
        doc.set(part, "type", Value::Text("reasoning"))?;
        doc.set(part, "text", Value::Text(""))?;
        doc.set(place.entry, "signature_only", Value::Bool(true))?;
        capture(owner, doc, part, &["type", "text"], None)
    } else {
        let value = doc.add(None, Value::Object)?;
        while let Some(n) = doc.node(part)?.first {
            doc.detach(part, n)?;
            doc.append(value, n)?;
        }
        doc.set(part, "type", Value::Text("opaque"))?;
        doc.set(part, "protocol", Value::Text("gemini"))?;
        doc.rename(value, "value")?;
        doc.append(part, value)
    }
}
/// Binds a part's signature into `x_ck.replay` at the part's own message and block index.
fn replay(doc: &mut Document, part: usize, signature: usize, place: Place) -> Result<()> {
    super::string(doc, signature)?;
    let x = doc.required(doc.root(), "x_ck")?;
    let replay = doc.required(x, "replay")?;
    let binding = doc.add(None, Value::Object)?;
    doc.append(replay, binding)?;
    doc.set(binding, "message_index", Value::Unsigned(place.message_index))?;
    doc.set(binding, "block_index", Value::Unsigned(place.index))?;
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
    doc.bind_origin(binding, part, Some("signature"))
}
fn tool_call(doc: &mut Document, part: usize, call: usize, place: Place, turn: &mut Calls) -> Result<()> {
    let calls = match doc.member(place.message, "tool_calls")? {
        Some(n) => n,
        None => doc.set(place.message, "tool_calls", Value::Array)?,
    };
    doc.append(calls, part)?;
    doc.set(part, "type", Value::Text("function"))?;
    doc.set(part, "arguments_format", Value::Text("json"))?;
    if let Some(id) = doc.remove(call, "id")? {
        doc.append(part, id)?;
    } else {
        let id = place
            .message_index
            .checked_mul(1_000_000)
            .and_then(|n| n.checked_add(place.index))
            .ok_or(Error::Limit)?;
        doc.set_id(part, "id", media::local_id(b't', id)?)?;
        doc.set(place.entry, "synthetic_id", Value::Bool(true))?;
    }
    if let Some(args) = doc.member(call, "args")? {
        doc.rename(args, "arguments")?;
    }
    turn.record(doc, place.message, part, doc.required(call, "name")?)?;
    doc.rename(call, "function")?;
    doc.append(part, call)?;
    doc.set(place.entry, "kind", Value::Text("call"))?;
    Ok(())
}
fn tool_result(doc: &mut Document, part: usize, result: usize, place: Place, calls: &mut Calls) -> Result<()> {
    doc.append(place.content, part)?;
    doc.set(part, "type", Value::Text("tool_result"))?;
    let name = doc.required(result, "name")?;
    super::string(doc, name)?;
    if let Some(id) = doc.remove(result, "id")? {
        doc.rename(id, "tool_call_id")?;
        doc.append(part, id)?;
    } else {
        let linked = calls.answer(doc, name)?.ok_or(Error::Schema)?;
        let id = doc.node(doc.required(linked, "id")?)?.value;
        doc.set(part, "tool_call_id", id)?;
        doc.set(place.entry, "synthetic_id", Value::Bool(true))?;
    }
    doc.rename(result, "result")?;
    doc.append(part, result)?;
    doc.set(place.entry, "kind", Value::Text("result"))?;
    Ok(())
}
/// The calls of the latest model turn, for function responses that omit the call id. Gemini
/// answers a turn's calls in order, so a response takes the first unanswered call of its name.
/// Calls sort by a keyed hash of their name: a lookup is a binary search, and the answered
/// calls of a run stay its prefix, which `taken` skips, so matching stays O(n log n).
pub(super) struct Calls {
    turn: Option<usize>,
    entries: Table<Call>,
    sorted: bool,
    hasher: RandomState,
}
#[derive(Clone, Copy)]
struct Call {
    key: u64,
    order: usize,
    part: usize,
    name: usize,
    /// On the first call of a run: how many leading calls of the run are answered.
    taken: usize,
}
impl Calls {
    pub(super) fn new(owner: &mut Writer) -> Result<Self> {
        Ok(Self {
            turn: None,
            entries: Table::new(owner)?,
            sorted: true,
            hasher: RandomState::new(),
        })
    }
    fn key(&self, doc: &Document, name: usize) -> Result<u64> {
        let mut hasher = self.hasher.build_hasher();
        let mut chars = doc.chars(name)?;
        while let Some(c) = chars.next_scalar()? {
            hasher.write_u32(u32::from(c));
        }
        Ok(hasher.finish())
    }
    /// Adds a call; the first call of a new model turn drops the previous turn's calls.
    fn record(&mut self, doc: &Document, message: usize, part: usize, name: usize) -> Result<()> {
        super::string(doc, name)?;
        if self.turn != Some(message) {
            while let Some(last) = self.entries.len().checked_sub(1) {
                self.entries.swap_remove(last);
            }
            self.turn = Some(message);
        }
        let call = Call {
            key: self.key(doc, name)?,
            order: self.entries.len(),
            part,
            name,
            taken: 0,
        };
        self.entries.push(call)?;
        self.sorted = false;
        Ok(())
    }
    /// The first unanswered call named `name`, now answered.
    fn answer(&mut self, doc: &Document, name: usize) -> Result<Option<usize>> {
        if !self.sorted {
            self.entries.sort_unstable_by_key(|c| (c.key, c.order));
            self.sorted = true;
        }
        let key = self.key(doc, name)?;
        let start = self.entries.partition_point(|c| c.key < key);
        let end = start + self.entries[start..].partition_point(|c| c.key == key);
        let taken = self.entries.get(start).map_or(0, |c| c.taken);
        let first = start + taken;
        for index in first..end {
            if doc.equal_strings(self.entries[index].name, name)? {
                // Only a hash collision puts another name first; moving the match to the
                // front keeps the answered calls a prefix and the others in order.
                self.entries[first..=index].rotate_right(1);
                self.entries[start].taken = taken + 1;
                return Ok(Some(self.entries[first].part));
            }
        }
        Ok(None)
    }
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
    // Inline data streamed during ingress is already a media reference; `register` checks
    // that every other inline payload is a string.
    if !inline {
        super::string(doc, payload)?;
    }
    let source = doc.set(part, "source", Value::Object)?;
    doc.set(source, "kind", Value::Text(if inline { "data" } else { "uri" }))?;
    doc.rename(payload, if inline { "ref" } else { "uri" })?;
    doc.append(source, payload)?;
    while let Some(n) = doc.node(data)?.first {
        doc.detach(data, n)?;
        preserve(owner, doc, source, n, None)?;
    }
    timing(doc, part)?;
    capture(owner, doc, part, &["type", "source", "mime", "timing"], None)
}
/// Adds the canonical `timing` for `videoMetadata` offsets. The native object stays an
/// original, so a Gemini destination gets its exact spelling and `fps` back.
fn timing(doc: &mut Document, part: usize) -> Result<()> {
    let Some(metadata) = doc.member(part, "videoMetadata")? else {
        return Ok(());
    };
    if !matches!(doc.node(metadata)?.value, Value::Object) {
        return Ok(());
    }
    let start = match doc.member(metadata, "startOffset")? {
        Some(n) => seconds(doc, n)?,
        None => None,
    };
    let Some(start) = start else {
        return Ok(());
    };
    let end = match doc.member(metadata, "endOffset")? {
        Some(n) => seconds(doc, n)?,
        None => None,
    };
    let timing = doc.set(part, "timing", Value::Object)?;
    doc.set(timing, "start", start)?;
    if let Some(end) = end {
        doc.set(timing, "end", end)?;
    }
    doc.set(timing, "unit", Value::Text("seconds"))?;
    Ok(())
}
/// A protobuf Duration spelled `<seconds>s` without escapes, as a JSON number over the same
/// input bytes. Any other spelling (negative, leading zero, escaped) has no number.
fn seconds(doc: &Document, node: usize) -> Result<Option<Value>> {
    let Value::String(text) = doc.node(node)?.value else {
        return Ok(None);
    };
    let mut buffer = [0u8; 32];
    let mut length = 0;
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        if !c.is_ascii() || length == buffer.len() {
            return Ok(None);
        }
        buffer[length] = c as u8;
        length += 1;
    }
    let Some((b's', number)) = buffer[..length].split_last() else {
        return Ok(None);
    };
    let (whole, fraction) = match number.iter().position(|&b| b == b'.') {
        Some(dot) => (&number[..dot], Some(&number[dot + 1..])),
        None => (number, None),
    };
    let digits = |part: &[u8]| !part.is_empty() && part.iter().all(u8::is_ascii_digit);
    if !digits(whole)
        || (whole.len() > 1 && whole[0] == b'0')
        || fraction.is_some_and(|f| !digits(f))
        || text.span.end - text.span.start != length as u64 + 2
    {
        return Ok(None);
    }
    Ok(Some(Value::Number(Text {
        source: text.source,
        span: Span {
            start: text.span.start + 1,
            end: text.span.end - 2,
        },
    })))
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
                if media.get(doc, node)?.is_some() {
                    continue;
                }
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
