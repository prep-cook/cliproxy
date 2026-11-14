use super::{Table, index::Index, is, mapped, member, object, string};

mod block;
mod envelope;
use crate::codec::{
    self, BodyMode, Error, Operation, Result,
    document::{Document, Value},
    encode::{self, Encoder},
    event::{Identity, Kind, LocalId},
    identity, internal,
};
use ck_stream::{Lease, ScratchLease, Writer};

pub struct Diagnostic {
    pub identity: Identity,
    pub kind: Kind,
    pub choice_index: Option<u64>,
    pub reason: &'static str,
    pub destination: &'static str,
}
struct Block {
    item: LocalId,
    canonical: u64,
    index: u64,
    document: Document,
    open: bool,
    started: bool,
    tool: Option<u64>,
    explicit_block: bool,
    start_origin: Option<Document>,
    /// The text kind whose list in `Egress::text_lists` holds this block while it is open.
    text: Option<&'static str>,
    /// The next block started under the same text key, in start order.
    next_text: Option<usize>,
}
/// The open text blocks of one (item, kind), linked through `Block::next_text` in start order.
/// `key` is a block naming the pair. Closed blocks leave the list once they reach `head`, so
/// `head` is always the earliest open block, or `None` when none is open.
struct TextList {
    key: usize,
    head: Option<usize>,
    tail: usize,
}
pub struct Egress {
    owner: u64,
    identity: Identity,
    operation: Operation,
    mode: BodyMode,
    sequence: u64,
    base: Option<Document>,
    /// The base id was issued here because the source supplied none; it stays for the response.
    own_id: bool,
    blocks: Table<Block>,
    /// `blocks` positions by (item, block index) and by tool index, and `text_lists` positions
    /// by (item, kind).
    named: Index,
    tools: Index,
    texts: Index,
    text_lists: Table<TextList>,
    usage: Option<Document>,
    finish: Option<Document>,
    start_origin: Option<Document>,
    finish_origin: Option<Document>,
    count: Option<Document>,
    started: bool,
    finish_sent: bool,
    terminal: bool,
    failed: bool,
    diagnostic: fn(Diagnostic),
}
impl Egress {
    pub fn new(
        owner: &mut Writer,
        identity: Identity,
        operation: Operation,
        mode: BodyMode,
        diagnostic: fn(Diagnostic),
    ) -> Result<Self> {
        if operation == Operation::Compact || (operation == Operation::CountTokens && mode == BodyMode::Streaming) {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            owner: owner.id(),
            identity,
            operation,
            mode,
            sequence: 0,
            base: None,
            own_id: false,
            blocks: Table::new(owner)?,
            named: Index::new(owner)?,
            tools: Index::new(owner)?,
            texts: Index::new(owner)?,
            text_lists: Table::new(owner)?,
            usage: None,
            finish: None,
            start_origin: None,
            finish_origin: None,
            count: None,
            started: false,
            finish_sent: false,
            terminal: false,
            failed: false,
            diagnostic,
        })
    }
    fn drop_event(&self, kind: Kind, choice: Option<u64>, reason: &'static str) {
        (self.diagnostic)(Diagnostic {
            identity: self.identity,
            kind,
            choice_index: choice,
            reason,
            destination: "anthropic",
        });
    }
    fn finish_delta(&mut self, owner: &mut Writer, output: &mut impl FnMut(ScratchLease) -> Result<()>) -> Result<()> {
        if self.finish_sent || self.mode == BodyMode::Buffered {
            return Ok(());
        }
        self.begin(owner, output)?;
        let finish = self.finish.as_ref().ok_or(Error::Schema)?;
        let origin = self.finish_origin.take();
        output(native_event(owner, "message_delta", origin.as_ref(), |e| {
            e.bytes(b",\"delta\":")?;
            finish.write(finish.root(), e)?;
            if let Some(usage) = &self.usage {
                e.bytes(b",\"usage\":")?;
                write_usage(usage, e)?;
            }
            if let Some(doc) = &origin {
                original_members(doc, doc.required(doc.root(), "x_ck")?, e)?;
            }
            Ok(())
        })?)?;
        self.finish_sent = true;
        Ok(())
    }
    fn begin(&mut self, owner: &mut Writer, output: &mut impl FnMut(ScratchLease) -> Result<()>) -> Result<()> {
        if self.started || self.mode == BodyMode::Buffered {
            return Ok(());
        }
        let base = self.base.as_ref().ok_or(Error::Schema)?;
        let origin = self.start_origin.take();
        output(native_event(owner, "message_start", origin.as_ref(), |e| {
            e.bytes(b",\"message\":{")?;
            write_base(base, e)?;
            if let Some(doc) = &origin {
                original_members_at(doc, doc.required(doc.root(), "x_ck")?, "/message", e)?;
            }
            e.bytes(b",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null")?;
            if let Some(usage) = &self.usage {
                e.bytes(b",\"usage\":")?;
                write_usage(usage, e)?;
            }
            e.bytes(b"}")?;
            if let Some(doc) = &origin {
                original_members(doc, doc.required(doc.root(), "x_ck")?, e)?;
            }
            Ok(())
        })?)?;
        self.started = true;
        Ok(())
    }
    fn buffered_envelope_loss(&self, doc: &Document, x: usize, kind: Kind, choice: Option<u64>) -> Result<()> {
        if self.mode != BodyMode::Buffered
            || !matches!(
                kind,
                Kind::Chunk
                    | Kind::BlockStart
                    | Kind::BlockEnd
                    | Kind::Done
                    | Kind::ReasoningSignature
                    | Kind::Heartbeat
            )
        {
            return Ok(());
        }
        let Some(source) = doc.member(x, "source")? else {
            return Ok(());
        };
        if !is(doc, source, "protocol", "anthropic")? {
            return Ok(());
        }
        let Some(fields) = doc.member(source, "sse")? else {
            return Ok(());
        };
        let mut lost = doc.node(fields)?.first.is_some();
        if !lost && let Some(original) = doc.member(x, "original")? {
            let finish = doc.member(x, "native_finish_reason")?.is_some();
            for entry in doc.children(original)? {
                if !is(doc, entry, "protocol", "anthropic")? {
                    continue;
                }
                let mut pointer = doc.chars(doc.required(entry, "pointer")?)?;
                let retained = finish
                    && "/delta/".chars().try_fold(true, |same, c| {
                        Ok::<_, Error>(same && pointer.next_scalar()? == Some(c))
                    })?;
                if !retained {
                    lost = true;
                    break;
                }
            }
        }
        if lost {
            self.drop_event(kind, choice, "native SSE metadata has no buffered carrier");
        }
        Ok(())
    }
    fn accept(
        &mut self,
        owner: &mut Writer,
        lease: Lease,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if self.terminal || self.failed {
            return Err(Error::Closed);
        }
        let (kind, mut doc) = internal::decode(owner, lease)?;
        let root = doc.root();
        let x = doc.required(root, "x_ck")?;
        if doc.unsigned(doc.required(x, "seq")?)? != self.sequence
            || doc.unsigned(doc.required(x, "attempt")?)? != self.identity.attempt
            || !doc.string_is(doc.required(x, "request_id")?, self.identity.request_id.as_str())?
            || !doc.string_is(doc.required(x, "stream_id")?, self.identity.stream_id.as_str())?
            || !doc.string_is(doc.required(x, "direction")?, "response")?
        {
            return Err(Error::Schema);
        }
        self.sequence = self.sequence.checked_add(1).ok_or(Error::Limit)?;
        let choice = doc.member(x, "choice_index")?.map(|n| doc.unsigned(n)).transpose()?;
        if choice.is_some_and(|index| index != 0) {
            self.drop_event(kind, choice, "single-choice destination selects index zero");
            return Ok(());
        }
        self.buffered_envelope_loss(&doc, x, kind, choice)?;
        match kind {
            Kind::ResponseStart => {
                if self.base.is_some() {
                    return Err(Error::Schema);
                }
                let root = doc.root();
                if identity::supplied_id(&doc, root)?.is_none() {
                    doc.set_id(root, "id", identity::response_id("msg_", owner.id())?)?;
                    self.own_id = true;
                }
                self.base = Some(doc);
            }
            Kind::ItemStart | Kind::ItemEnd | Kind::Heartbeat => {}
            Kind::BlockStart => self.block_start(owner, doc, output)?,
            Kind::ToolStart => self.tool_start(owner, doc, output)?,
            Kind::Chunk => self.chunk(owner, doc, kind, output)?,
            Kind::ReasoningSignature => self.signature(owner, doc, kind, choice, output)?,
            Kind::ToolResult => self.tool_result(owner, doc, output)?,
            Kind::ReasoningRedacted => self.redacted(owner, doc, output)?,
            Kind::Annotation => self.annotation(owner, doc, kind, choice, output)?,
            Kind::ToolEnd => self.tool_end(owner, doc, output)?,
            Kind::BlockEnd => {
                let index = self.find(&doc, x)?;
                self.close_block(owner, index, Some(&doc), output)?;
            }
            Kind::Usage => {
                let usage = doc.required(root, "usage")?;
                self.usage = Some(mapped(owner, |e| doc.write(usage, e))?);
            }
            Kind::CountTokens => {
                if self.operation != Operation::CountTokens || self.count.is_some() {
                    return Err(Error::Schema);
                }
                self.count = Some(doc);
            }
            Kind::Opaque => self.opaque(owner, doc, kind, choice, output)?,
            Kind::Done => self.done(owner, doc, output)?,
            Kind::Error | Kind::Cancelled => self.error(owner, output)?,
            _ => self.drop_event(kind, choice, "event has no Anthropic response carrier"),
        }
        Ok(())
    }
}
impl codec::ResponseEgress for Egress {
    fn advance(
        &mut self,
        owner: &mut Writer,
        input: Lease,
        mut output: impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if owner.id() != self.owner {
            return Err(Error::Position);
        }
        let result = self.accept(owner, input, &mut output);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn finish(&mut self) -> Result<()> {
        if self.terminal && !self.failed {
            Ok(())
        } else {
            Err(Error::Truncated)
        }
    }
}
fn local(doc: &Document, node: usize) -> Result<LocalId> {
    let mut bytes = [0u8; 64];
    let mut len = 0;
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        if !c.is_ascii() || len == bytes.len() {
            return Err(Error::Schema);
        }
        bytes[len] = c as u8;
        len += 1;
    }
    LocalId::new(std::str::from_utf8(&bytes[..len]).map_err(|_| Error::Schema)?)
}
fn copy_value(doc: &mut Document, parent: usize, key: &'static str, from: usize) -> Result<()> {
    let target = doc.set(parent, key, Value::Null)?;
    doc.transplant(target, from)
}
fn event(
    owner: &mut Writer,
    name: &'static str,
    fields: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
) -> Result<ScratchLease> {
    native_event(owner, name, None, fields)
}
fn native_event(
    owner: &mut Writer,
    name: &'static str,
    doc: Option<&Document>,
    fields: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
) -> Result<ScratchLease> {
    encode::scratch(owner, |e| {
        e.bytes(b"event: ")?;
        e.bytes(name.as_bytes())?;
        e.bytes(b"\n")?;
        if let Some(doc) = doc
            && let Some(x) = doc.member(doc.root(), "x_ck")?
            && let Some(source) = doc.member(x, "source")?
        {
            native_sse_fields(doc, source, e)?;
        }
        e.bytes(b"data: {\"type\":")?;
        e.string(name)?;
        fields(e)?;
        e.bytes(b"}\n\n")
    })
}

fn native_sse_fields(doc: &Document, source: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    if !is(doc, source, "protocol", "anthropic")? {
        return Ok(());
    }
    if let Some(fields) = doc.member(source, "sse")? {
        for field in doc.children(fields)? {
            let name = doc.required(field, "name")?;
            // Native IDs stay in provenance; only the gateway may issue a resume cursor.
            if doc.string_is(name, "id")? {
                continue;
            }
            raw_string(doc, name, e, false)?;
            e.bytes(b": ")?;
            raw_string(doc, doc.required(field, "value")?, e, false)?;
            e.bytes(b"\n")?;
        }
    }
    Ok(())
}
fn write_base(doc: &Document, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"\"type\":\"message\",\"role\":\"assistant\"")?;
    for key in ["id", "model"] {
        member(doc, doc.root(), key, e)?;
    }
    if let Some(x) = doc.member(doc.root(), "x_ck")? {
        original_members(doc, x, e)?;
    }
    Ok(())
}
fn write_usage(doc: &Document, e: &mut Encoder<'_, '_>) -> Result<()> {
    let root = doc.root();
    if let Some(native) = doc.member(root, "native")?
        && is(doc, native, "protocol", "anthropic")?
    {
        return doc.write(doc.required(native, "value")?, e);
    }
    e.bytes(b"{")?;
    let mut first = true;
    if doc.member(root, "input_uncached_tokens")?.is_none()
        && let Some(uncached) = derived_uncached(doc, root)?
    {
        e.bytes(b"\"input_tokens\":")?;
        e.unsigned(uncached)?;
        first = false;
    }
    for (from, to) in [
        ("input_uncached_tokens", "input_tokens"),
        ("cache_read_tokens", "cache_read_input_tokens"),
        ("cache_write_tokens", "cache_creation_input_tokens"),
        ("output_tokens", "output_tokens"),
    ] {
        if let Some(value) = doc.member(root, from)? {
            if !first {
                e.bytes(b",")?;
            }
            first = false;
            e.string(to)?;
            e.bytes(b":")?;
            doc.write(value, e)?;
        }
    }
    e.bytes(b"}")
}
/// Claude's uncached input, when inclusive input and both cache components are known and consistent.
fn derived_uncached(doc: &Document, root: usize) -> Result<Option<u64>> {
    let mut counts = [0; 3];
    for (count, key) in counts
        .iter_mut()
        .zip(["input_tokens", "cache_read_tokens", "cache_write_tokens"])
    {
        let Some(node) = doc.member(root, key)? else {
            return Ok(None);
        };
        let Ok(value) = doc.unsigned(node) else {
            return Ok(None);
        };
        *count = value;
    }
    Ok(counts[1]
        .checked_add(counts[2])
        .and_then(|cached| counts[0].checked_sub(cached)))
}
fn original_members(doc: &Document, x: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    original_members_at(doc, x, "", e)
}
fn original_members_at(doc: &Document, x: usize, prefix: &str, e: &mut Encoder<'_, '_>) -> Result<()> {
    original_members_with_separator(doc, x, prefix, true, e)
}
fn original_members_with_separator(
    doc: &Document,
    x: usize,
    prefix: &str,
    mut separator: bool,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    if let Some(original) = doc.member(x, "original")? {
        'entry: for entry in doc.children(original)? {
            if !is(doc, entry, "protocol", "anthropic")? {
                continue;
            }
            let pointer = doc.required(entry, "pointer")?;
            let mut chars = doc.chars(pointer)?;
            for expected in prefix.chars().chain(std::iter::once('/')) {
                if chars.next_scalar()? != Some(expected) {
                    continue 'entry;
                }
            }
            let mut probe = doc.chars(pointer)?;
            for _ in 0..prefix.chars().count() + 1 {
                probe.next_scalar()?;
            }
            while let Some(c) = probe.next_scalar()? {
                if c == '/' {
                    continue 'entry;
                }
            }
            if separator {
                e.bytes(b",")?;
            }
            separator = true;
            e.bytes(b"\"")?;
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
    Ok(())
}
fn write_opaque(doc: &Document, x: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let source = doc.required(x, "source")?;
    if let Some(name) = doc.member(source, "event")? {
        e.bytes(b"event: ")?;
        raw_string(doc, name, e, false)?;
        e.bytes(b"\n")?;
    }
    native_sse_fields(doc, source, e)?;
    e.bytes(b"data: ")?;
    let value = doc.required(doc.root(), "value")?;
    if is(doc, doc.root(), "format", "text")? {
        raw_string(doc, value, e, true)?;
    } else {
        doc.write(value, e)?;
    }
    e.bytes(b"\n\n")
}
fn raw_string(doc: &Document, node: usize, e: &mut Encoder<'_, '_>, multiline: bool) -> Result<()> {
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        if c == '\n' && multiline {
            e.bytes(b"\ndata: ")?;
        } else if matches!(c, '\n' | '\r') {
            return Err(Error::Schema);
        } else {
            e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
        }
    }
    Ok(())
}

fn metadata_changed(base: &Document, incoming: &Document, keys: &[&str]) -> Result<bool> {
    for &key in keys {
        let Some(next) = incoming.member(incoming.root(), key)? else {
            continue;
        };
        let Some(current) = base.member(base.root(), key)? else {
            return Ok(true);
        };
        if key == "created" {
            if base.unsigned(current)? != incoming.unsigned(next)? {
                return Ok(true);
            }
        } else {
            let mut a = base.chars(current)?;
            let mut b = incoming.chars(next)?;
            loop {
                let c = a.next_scalar()?;
                if c != b.next_scalar()? {
                    return Ok(true);
                }
                if c.is_none() {
                    break;
                }
            }
        }
    }
    Ok(false)
}
