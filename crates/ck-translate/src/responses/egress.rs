use super::{Table, is, response::append_fragment};
use crate::codec::{
    self, BodyMode, Error, Operation, Result,
    document::{Document, Name, Value},
    encode::{self, Encoder},
    event::{Identity, Kind},
    internal,
};
use ck_stream::{ScratchLease, Writer};

mod delta;
mod envelope;
mod item;
mod media;
mod reasoning;

/// The absorbed event and the output item it belongs to, with that item's native index.
#[derive(Clone, Copy)]
struct At {
    node: usize,
    x: usize,
    item: usize,
    index: u64,
}

pub struct Egress<F: FnMut(&'static str, u64)> {
    owner: u64,
    mode: BodyMode,
    document: Document,
    terminal: bool,
    sequence: u64,
    diagnostic: F,
    images: Table<super::image::Projection>,
    audio: Table<super::audio::Projection>,
    /// Value slot of the retained canonical usage snapshot in the opaque carrier.
    usage_snapshot: Option<usize>,
    /// A message item from a source without native items, appended but not yet announced: it
    /// follows any reasoning item split out of it, and is dropped if it stays empty.
    pending: Option<usize>,
    /// `response.created` is owed: it waits for the source's own creation snapshot, or for the
    /// first event that needs a created response before it.
    owed: bool,
}
impl<F: FnMut(&'static str, u64)> Egress<F> {
    pub fn new(
        owner: &mut Writer,
        identity: Identity,
        operation: Operation,
        mode: BodyMode,
        diagnostic: F,
    ) -> Result<Self> {
        if operation == Operation::CountTokens || (operation == Operation::Compact && mode == BodyMode::Streaming) {
            return Err(Error::Unsupported);
        }
        let document = super::mapped(owner, |e| {
            e.bytes(b"{\"id\":")?;
            e.string(identity.stream_id.as_str())?;
            e.bytes(b",\"object\":")?;
            e.string(if operation == Operation::Compact {
                "response.compaction"
            } else {
                "response"
            })?;
            e.bytes(b",\"created_at\":0,\"model\":\"\",\"status\":\"in_progress\",\"output\":[]}")
        })?;
        Ok(Self {
            owner: owner.id(),
            mode,
            document,
            terminal: false,
            sequence: 0,
            diagnostic,
            images: Table::new(owner)?,
            audio: Table::new(owner)?,
            usage_snapshot: None,
            pending: None,
            owed: false,
        })
    }
    fn emit(
        &mut self,
        owner: &mut Writer,
        name: &str,
        payload: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if self.mode == BodyMode::Buffered {
            return Ok(());
        }
        let seq = self.sequence;
        self.sequence = self.sequence.checked_add(1).ok_or(Error::Limit)?;
        output(encode::scratch(owner, |e| {
            e.bytes(b"event: ")?;
            e.bytes(name.as_bytes())?;
            e.bytes(b"\ndata: {\"type\":")?;
            e.string(name)?;
            e.bytes(b",\"sequence_number\":")?;
            e.unsigned(seq)?;
            payload(e)?;
            e.bytes(b"}\n\n")
        })?)
    }
    fn process(
        &mut self,
        owner: &mut Writer,
        kind: Kind,
        incoming: Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let root = incoming.root();
        let x = incoming.required(root, "x_ck")?;
        if let Some(choice) = incoming.member(x, "choice_index")? {
            let index = incoming.unsigned(choice)?;
            if index != 0 {
                (self.diagnostic)("choice", index);
                return Ok(());
            }
        }
        match kind {
            Kind::Heartbeat => return Ok(()),
            Kind::Error | Kind::Cancelled => return self.failed(owner, output),
            Kind::ResponseStart => return self.response_start(incoming),
            Kind::Done => return self.done(owner, output),
            Kind::Opaque => return self.opaque(owner, incoming, output),
            Kind::Usage => {}
            _ => self.created(owner, output)?,
        }
        let native_execution = incoming
            .member(x, "source")?
            .map(|source| is(&incoming, source, "protocol", "responses"))
            .transpose()?
            .unwrap_or(false);
        if matches!(kind, Kind::UrlContext | Kind::Operation)
            || (matches!(kind, Kind::Grounding | Kind::CodeExecution) && !native_execution)
        {
            (self.diagnostic)("unsupported event", 0);
            return Ok(());
        }
        let node = self.document.absorb(incoming)?;
        if kind == Kind::MediaStart && is(&self.document, node, "kind", "audio")? {
            return self.audio_start(node);
        }
        if matches!(kind, Kind::Data | Kind::MediaTranscript | Kind::MediaEnd)
            && let Some(index) = self.audio_index(node)?
        {
            return self.audio_media(owner, kind, node, index, output);
        }
        match kind {
            Kind::Data | Kind::MediaEnd => return self.image_media(owner, kind, node, output),
            Kind::Usage => return self.usage(node),
            Kind::Compact => return self.compact(node),
            Kind::ItemStart => return self.item_start(owner, node, output),
            _ => {}
        }
        let doc = &self.document;
        let x = doc.required(node, "x_ck")?;
        let Some((item, index)) = find_item(doc, doc.required(doc.root(), "output")?, x)? else {
            if kind == Kind::Chunk {
                return self.finish_chunk(node);
            }
            return Err(Error::Schema);
        };
        let at = At { node, x, item, index };
        self.announce_for(owner, (kind, node, item), output)?;
        match kind {
            Kind::MediaStart => self.image_start(at),
            Kind::ToolStart => self.tool_start(owner, at, output),
            Kind::BlockStart => self.block_start(owner, at, output),
            Kind::ToolResult | Kind::Grounding | Kind::CodeExecution => self.execution(owner, kind, at, output),
            Kind::Chunk => self.chunk(owner, at, output),
            Kind::ReasoningSignature => self.signature(at),
            Kind::ReasoningRedacted => self.redacted(),
            Kind::Annotation => self.annotation(owner, at, output),
            Kind::BlockEnd => self.block_end(owner, at, output),
            Kind::ToolEnd => self.tool_end(owner, at, output),
            Kind::ItemEnd => self.item_end(owner, at, output),
            _ => {
                (self.diagnostic)("unsupported event", 0);
                Ok(())
            }
        }
    }
    fn finish_chunk(&mut self, node: usize) -> Result<()> {
        let doc = &mut self.document;
        let choices = doc.required(node, "choices")?;
        for choice in doc.children(choices)? {
            if let Some(reason) = doc.member(choice, "finish_reason")?
                && !matches!(doc.node(reason)?.value, Value::Null)
            {
                let filtered = doc.string_is(reason, "content_filter")?;
                if !filtered && !doc.string_is(reason, "length")? {
                    doc.set(doc.root(), "status", Value::Text("completed"))?;
                    break;
                }
                doc.set(doc.root(), "status", Value::Text("incomplete"))?;
                // A native Responses source restores its own details later, by merge.
                if doc.member(doc.root(), "incomplete_details")?.is_none() {
                    let details = doc.set(doc.root(), "incomplete_details", Value::Object)?;
                    let reason = if filtered {
                        "content_filter"
                    } else {
                        "max_output_tokens"
                    };
                    doc.set(details, "reason", Value::Text(reason))?;
                }
                break;
            }
        }
        Ok(())
    }
}
impl<F: FnMut(&'static str, u64)> codec::ResponseEgress for Egress<F> {
    fn advance(
        &mut self,
        owner: &mut Writer,
        input: ck_stream::Lease,
        mut output: impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        crate::boundary::enter(self.owner, owner, self.terminal)?;
        let (kind, doc) = internal::decode(owner, input)?;
        self.process(owner, kind, doc, &mut output)
    }
    fn finish(&mut self) -> Result<()> {
        if self.terminal { Ok(()) } else { Err(Error::Truncated) }
    }
}
fn write_lease(lease: &ScratchLease, e: &mut Encoder<'_, '_>) -> Result<()> {
    let mut offset = 0;
    while offset < lease.len() {
        let chunk = lease.chunk(offset)?;
        e.bytes(chunk)?;
        offset += chunk.len();
    }
    Ok(())
}
/// Origin carrier of a call item split out of a message item.
const CALL_ITEM: &str = "responses.tool";
/// Origin carrier of a reasoning item split out of a message item.
const REASONING_ITEM: &str = "responses.reasoning";
/// Origin carrier of an opaque part: its members, whatever its type, carry no text semantics.
const OPAQUE_PART: &str = "responses.opaque";

/// Whether block start `node` opens readable, summary, encrypted or redacted reasoning.
fn reasoning(doc: &Document, node: usize) -> Result<bool> {
    Ok(is(doc, node, "kind", "reasoning_summary")?
        || is(doc, node, "kind", "thinking")?
        || is(doc, node, "kind", "reasoning")?)
}
/// Whether block start `node` opens redacted reasoning, which egress drops.
fn redacted(doc: &Document, node: usize) -> Result<bool> {
    Ok(is(doc, node, "kind", "redacted")?
        || doc
            .member(node, "attributes")?
            .map_or(Ok(false), |attributes| is(doc, attributes, "type", "redacted"))?)
}
/// Position of `item` in the response output.
fn position(doc: &Document, item: usize) -> Result<u64> {
    let output = doc.required(doc.root(), "output")?;
    let at = doc.children(output)?.position(|i| i == item).ok_or(Error::Position)?;
    Ok(at as u64)
}

/// The output item of the event with correlation `x`: its own call item when it has one, else
/// the item named by `item_id`.
fn find_item(doc: &Document, output: usize, x: usize) -> Result<Option<(usize, u64)>> {
    let Some(local) = doc.member(x, "item_id")? else {
        return Ok(None);
    };
    if let Some(block) = doc.member(x, "block_index")? {
        let block = doc.unsigned(block)?;
        for (index, item) in doc.children(output)?.enumerate() {
            if let Some((origin, Some(REASONING_ITEM))) = doc.origin(item)?
                && doc.equal_strings(doc.required(origin, "item_id")?, local)?
                && doc.unsigned(doc.required(origin, "block_index")?)? == block
            {
                return Ok(Some((item, index as u64)));
            }
        }
    }
    if let Some(call) = doc.member(x, "tool_call_id")? {
        for (index, item) in doc.children(output)?.enumerate() {
            if let Some((origin, Some(CALL_ITEM))) = doc.origin(item)?
                && doc.equal_strings(doc.required(origin, "tool_call_id")?, call)?
            {
                return Ok(Some((item, index as u64)));
            }
        }
    }
    for (index, item) in doc.children(output)?.enumerate() {
        if let Some((origin, carrier)) = doc.origin(item)?
            && carrier.is_none()
            && doc.equal_strings(doc.required(origin, "item_id")?, local)?
        {
            return Ok(Some((item, native_index(doc, item, "output_index", index as u64)?)));
        }
    }
    Ok(None)
}
fn find_part(doc: &Document, item: usize, x: usize) -> Result<Option<(usize, bool, u64)>> {
    let Some(block) = doc.member(x, "block_index")? else {
        return Ok(None);
    };
    let block = doc.unsigned(block)?;
    for (name, summary) in [("content", false), ("summary", true)] {
        if let Some(parts) = doc.member(item, name)? {
            for (index, part) in doc.children(parts)?.enumerate() {
                if let Some((origin, _)) = doc.origin(part)?
                    && doc.unsigned(doc.required(origin, "block_index")?)? == block
                {
                    return Ok(Some((
                        part,
                        summary,
                        native_index(
                            doc,
                            part,
                            if summary { "summary_index" } else { "content_index" },
                            index as u64,
                        )?,
                    )));
                }
            }
        }
    }
    Ok(None)
}
fn item_fields(doc: &Document, item: usize, index: u64, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b",\"output_index\":")?;
    e.unsigned(index)?;
    e.bytes(b",\"item_id\":")?;
    doc.write(doc.required(item, "id")?, e)
}
fn write_part(doc: &Document, part: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    doc.write(part, e)
}
fn write_item(doc: &Document, item: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{")?;
    let mut first = true;
    for field in doc.children(item)? {
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        if !first {
            e.bytes(b",")?;
        }
        first = false;
        doc.write_name(name, e)?;
        e.bytes(b":")?;
        if (doc.name_is(name, "content")? || doc.name_is(name, "summary")?)
            && matches!(doc.node(field)?.value, Value::Array)
        {
            e.bytes(b"[")?;
            for (i, part) in doc.children(field)?.enumerate() {
                if i > 0 {
                    e.bytes(b",")?;
                }
                write_part(doc, part, e)?;
            }
            e.bytes(b"]")?;
        } else {
            doc.write(field, e)?;
        }
    }
    e.bytes(b"}")
}
fn write_response(doc: &Document, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{")?;
    for (i, field) in doc.children(doc.root())?.enumerate() {
        if i > 0 {
            e.bytes(b",")?;
        }
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        doc.write_name(name, e)?;
        e.bytes(b":")?;
        if doc.name_is(name, "output")? {
            e.bytes(b"[")?;
            for (j, item) in doc.children(field)?.enumerate() {
                if j > 0 {
                    e.bytes(b",")?;
                }
                write_item(doc, item, e)?;
            }
            e.bytes(b"]")?;
        } else {
            doc.write(field, e)?;
        }
    }
    e.bytes(b"}")
}

fn native_index(doc: &Document, node: usize, field: &str, fallback: u64) -> Result<u64> {
    if let Some((origin, _)) = doc.origin(node)?
        && let Some(source) = doc.member(origin, "source")?
        && is(doc, source, "protocol", "responses")?
        && let Some(index) = doc.member(source, field)?
    {
        return doc.unsigned(index);
    }
    Ok(fallback)
}

fn order_array(doc: &mut Document, array: usize, field: &str, owner: &mut Writer) -> Result<()> {
    let mut previous = None;
    let mut unordered = false;
    for (index, node) in doc.children(array)?.enumerate() {
        let index = native_index(doc, node, field, index as u64)?;
        unordered |= previous.is_some_and(|previous| previous >= index);
        previous = Some(index);
    }
    if !unordered {
        return Ok(());
    }
    let mut order = Table::new(owner)?;
    for (index, node) in doc.children(array)?.enumerate() {
        order.push((native_index(doc, node, field, index as u64)?, node))?;
    }
    order.sort_unstable_by_key(|entry| entry.0);
    if order.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(Error::Schema);
    }
    while let Some(node) = doc.node(array)?.first {
        doc.detach(array, node)?;
    }
    for &(_, node) in order.iter() {
        doc.append(array, node)?;
    }
    Ok(())
}

/// Whether block start `node` opens a summary part: a summary, or an unmapped part that a
/// Responses source correlates to a summary index, which returns to the summary it came from.
fn summary_part(doc: &Document, node: usize, x: usize) -> Result<bool> {
    if is(doc, node, "kind", "reasoning_summary")? {
        return Ok(true);
    }
    Ok(match doc.member(x, "source")? {
        Some(source) if is(doc, node, "kind", "opaque")? => {
            is(doc, source, "protocol", "responses")? && doc.member(source, "summary_index")?.is_some()
        }
        _ => false,
    })
}

/// Sets the native type of new part `part` from block start `node`; an opaque part keeps the
/// type its attributes carry.
fn part_type(doc: &mut Document, node: usize, part: usize, summary: bool, thinking: bool) -> Result<()> {
    if is(doc, node, "kind", "opaque")? && doc.member(part, "type")?.is_some() {
        return Ok(());
    }
    let kind = if is(doc, node, "kind", "refusal")? {
        "refusal"
    } else if summary {
        "summary_text"
    } else if thinking {
        "reasoning_text"
    } else {
        "output_text"
    };
    doc.set(part, "type", Value::Text(kind))?;
    Ok(())
}

/// Adds the Responses originals of event `x` to `target`.
fn restore_originals(doc: &mut Document, x: usize, target: usize) -> Result<()> {
    let Some(original) = doc.member(x, "original")? else {
        return Ok(());
    };
    let mut next = doc.node(original)?.first;
    while let Some(entry) = next {
        next = doc.node(entry)?.next;
        if !is(doc, entry, "protocol", "responses")? {
            continue;
        }
        let Value::String(pointer) = doc.node(doc.required(entry, "pointer")?)?.value else {
            return Err(Error::Schema);
        };
        let value = doc.remove(entry, "value")?.ok_or(Error::Schema)?;
        let field = doc.add(Some(Name::Pointer(pointer)), Value::Null)?;
        doc.transplant(field, value)?;
        // A later value of a started member is merged into it: omitted nested members keep
        // their started values (§3.6).
        let carrier = doc.add(None, Value::Object)?;
        doc.append(carrier, field)?;
        super::merge(doc, target, carrier)?;
    }
    Ok(())
}
