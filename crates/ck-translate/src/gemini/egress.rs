//! Gemini response egress. Each open part keeps its own small document, so a streamed response
//! holds only its open parts; a buffered response also keeps the body it returns at `done`.

mod candidate;
mod part;

use super::{Table, mapped, response::Framing};
use crate::codec::{
    self, BodyMode, Error, Operation, Result,
    document::{Document, Value},
    encode::{self, Encoder},
    event::{Identity, Kind, LocalId},
    internal,
};
use ck_stream::{Lease, ScratchLease, Writer};

#[derive(Clone, Copy, Debug)]
pub enum Diagnostic {
    /// The event has no Gemini carrier, or carries another protocol's native payload.
    UnsupportedEvent(Kind),
    SseMetadata,
}
pub struct Egress {
    owner: u64,
    operation: Operation,
    framing: Framing,
    /// `responseId`, `modelVersion` and `createTime` from the response start.
    identity: Option<Document>,
    /// The buffered response body; streamed output writes each record as it completes.
    body: Option<Document>,
    choices: Table<Choice>,
    /// Open parts only; a part leaves the table when its block ends.
    blocks: Table<Block>,
    terminal: bool,
    failed: bool,
    array_started: bool,
    diagnostic: fn(Diagnostic),
}
struct Choice {
    index: u64,
    /// The candidate object in the buffered body.
    node: Option<usize>,
    finished: bool,
}
struct Block {
    item: LocalId,
    index: u64,
    choice: u64,
    /// The native part under construction, at the document root.
    document: Document,
    arguments: Option<usize>,
    media: Option<usize>,
    thought: bool,
    /// A streamed part holds its latest text until more text, its signature or its end arrives.
    held: bool,
}
impl Egress {
    pub fn new(
        owner: &mut Writer,
        _identity: Identity,
        operation: Operation,
        mode: BodyMode,
        diagnostic: fn(Diagnostic),
    ) -> Result<Self> {
        Self::with_framing(
            owner,
            operation,
            if mode == BodyMode::Buffered {
                Framing::Json
            } else {
                Framing::Sse
            },
            diagnostic,
        )
    }
    pub fn with_framing(
        owner: &mut Writer,
        operation: Operation,
        framing: Framing,
        diagnostic: fn(Diagnostic),
    ) -> Result<Self> {
        if operation == Operation::Compact || (operation == Operation::CountTokens && framing != Framing::Json) {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            owner: owner.id(),
            operation,
            framing,
            identity: None,
            body: match framing {
                Framing::Json => Some(mapped(owner, |e| e.bytes(b"{\"candidates\":[]}"))?),
                Framing::Sse | Framing::JsonArray => None,
            },
            choices: Table::new(owner)?,
            blocks: Table::new(owner)?,
            terminal: false,
            failed: false,
            array_started: false,
            diagnostic,
        })
    }
    fn accept(
        &mut self,
        owner: &mut Writer,
        kind: Kind,
        event: Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if self.identity.is_none() && !matches!(kind, Kind::ResponseStart | Kind::Error | Kind::Cancelled) {
            return Err(Error::Schema);
        }
        match kind {
            Kind::ResponseStart => self.start(owner, &event)?,
            Kind::ItemStart | Kind::ItemEnd | Kind::Heartbeat | Kind::MediaEnd => (),
            Kind::BlockStart => self.block_start(owner, event)?,
            Kind::Chunk => self.chunk(owner, event, output)?,
            Kind::ReasoningSignature => self.signature(event)?,
            Kind::ToolStart => self.tool_start(event)?,
            Kind::ToolEnd => self.tool_end(owner, &event)?,
            Kind::ToolResult | Kind::CodeExecution | Kind::MediaTranscript => self.part_value(kind, event)?,
            Kind::MediaStart => self.media_start(event)?,
            Kind::Data => self.data(event)?,
            Kind::BlockEnd => self.block_end(owner, &event, output)?,
            Kind::Grounding | Kind::UrlContext | Kind::Annotation => self.annotation(owner, kind, &event, output)?,
            Kind::Usage => self.usage(owner, &event, output)?,
            Kind::CountTokens => self.count(owner, &event)?,
            Kind::Opaque => self.opaque(owner, &event, output)?,
            Kind::Done => self.done(owner, output)?,
            Kind::Error | Kind::Cancelled => self.error(owner, output)?,
            _ => (self.diagnostic)(Diagnostic::UnsupportedEvent(kind)),
        }
        Ok(())
    }
    /// Keeps the native identity of the response; synthesized values have no native field.
    fn start(&mut self, owner: &mut Writer, event: &Document) -> Result<()> {
        if self.identity.is_some() {
            return Err(Error::Schema);
        }
        let root = event.root();
        let x = event.required(root, "x_ck")?;
        let timestamp = match event.member(x, "source")? {
            Some(source) => event.member(source, "timestamp")?,
            None => None,
        };
        let mut fields = [None; 3];
        for (slot, (canonical, native)) in fields.iter_mut().zip([("id", "responseId"), ("model", "modelVersion")]) {
            if !synthetic(event, x, canonical)? {
                *slot = event.member(root, canonical)?.map(|node| (native, node));
            }
        }
        fields[2] = timestamp.map(|node| ("createTime", node));
        self.identity = Some(mapped(owner, |e| {
            e.bytes(b"{")?;
            let mut first = true;
            for &(name, node) in fields.iter().flatten() {
                if !first {
                    e.bytes(b",")?;
                }
                first = false;
                e.string(name)?;
                e.bytes(b":")?;
                event.write(node, e)?;
            }
            e.bytes(b"}")
        })?);
        Ok(())
    }
    /// Takes a native chunk's `id` and `model` into the identity when they differ from it, so
    /// an identity the upstream supplied after the response start reaches the client.
    fn refresh(&mut self, owner: &mut Writer, event: &Document, x: usize) -> Result<()> {
        let root = event.root();
        let identity = self.identity.as_ref().ok_or(Error::Schema)?;
        let mut supplied = [None; 2];
        let mut changed = false;
        for (slot, (canonical, native)) in supplied
            .iter_mut()
            .zip([("id", "responseId"), ("model", "modelVersion")])
        {
            if synthetic(event, x, canonical)? {
                continue;
            }
            *slot = event.member(root, canonical)?;
            if let Some(node) = *slot {
                changed |= match identity.member(identity.root(), native)? {
                    Some(current) => !super::same(identity, current, event, node)?,
                    None => true,
                };
            }
        }
        if !changed {
            return Ok(());
        }
        let refreshed = mapped(owner, |e| {
            e.bytes(b"{")?;
            let mut first = true;
            let names = ["responseId", "modelVersion"];
            for (name, node) in names.into_iter().zip(supplied) {
                if let Some(node) = node {
                    e.bytes(if std::mem::replace(&mut first, false) {
                        b""
                    } else {
                        b","
                    })?;
                    e.string(name)?;
                    e.bytes(b":")?;
                    event.write(node, e)?;
                }
            }
            for node in identity.children(identity.root())? {
                let name = identity.node(node)?.name.ok_or(Error::Schema)?;
                if names.into_iter().zip(supplied).try_fold(false, |found, (key, n)| {
                    Ok::<_, Error>(found || (n.is_some() && identity.name_is(name, key)?))
                })? {
                    continue;
                }
                e.bytes(if std::mem::replace(&mut first, false) {
                    b""
                } else {
                    b","
                })?;
                identity.write_name(name, e)?;
                e.bytes(b":")?;
                identity.write(node, e)?;
            }
            e.bytes(b"}")
        })?;
        self.identity = Some(refreshed);
        Ok(())
    }
    /// Writes one streamed record: the members `write` adds, then the response identity, which
    /// Gemini repeats in every streamed response. `write` clears the flag once it writes a member.
    fn record(
        &mut self,
        owner: &mut Writer,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
        write: impl Fn(&mut bool, &mut Encoder<'_, '_>) -> Result<()>,
    ) -> Result<()> {
        let identity = self.identity.take().ok_or(Error::Schema)?;
        let result = self.emit(owner, output, |e| {
            e.bytes(b"{")?;
            let mut first = true;
            write(&mut first, e)?;
            fields(&identity, identity.root(), &[], &mut first, e)?;
            e.bytes(b"}")
        });
        self.identity = Some(identity);
        result
    }
    /// Writes `write`'s bytes as one output record in the client framing.
    fn emit(
        &mut self,
        owner: &mut Writer,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
        write: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
    ) -> Result<()> {
        let prefix: &[u8] = match (self.framing, self.array_started) {
            (Framing::Sse, _) => b"data: ",
            (Framing::JsonArray, false) => b"[",
            (Framing::JsonArray, true) => b",",
            (Framing::Json, _) => b"",
        };
        let suffix: &[u8] = if self.framing == Framing::Sse { b"\n\n" } else { b"" };
        output(encode::scratch(owner, |e| {
            e.bytes(prefix)?;
            write(e)?;
            e.bytes(suffix)
        })?)?;
        self.array_started = true;
        Ok(())
    }
    fn choice(&mut self, index: u64) -> Result<usize> {
        if let Some(slot) = self.choices.iter().position(|c| c.index == index) {
            return Ok(slot);
        }
        let node = match &mut self.body {
            Some(body) => {
                let node = body.add(None, Value::Object)?;
                body.append(body.required(body.root(), "candidates")?, node)?;
                body.set(node, "index", Value::Unsigned(index))?;
                let content = body.set(node, "content", Value::Object)?;
                body.set(content, "role", Value::Text("model"))?;
                body.set(content, "parts", Value::Array)?;
                Some(node)
            }
            None => None,
        };
        self.choices.push(Choice {
            index,
            node,
            finished: false,
        })
    }
    /// The open choice of a record's `choice_index`.
    fn open_choice(&mut self, index: u64) -> Result<usize> {
        let slot = self.choice(index)?;
        if self.choices[slot].finished {
            return Err(Error::Schema);
        }
        Ok(slot)
    }
    fn done(&mut self, owner: &mut Writer, output: &mut impl FnMut(ScratchLease) -> Result<()>) -> Result<()> {
        if !self.blocks.is_empty()
            || (self.operation == Operation::Generate && self.choices.iter().any(|c| !c.finished))
        {
            return Err(Error::Truncated);
        }
        match self.framing {
            Framing::Json => {
                let body = self.body.take().ok_or(Error::Schema)?;
                if self.operation == Operation::CountTokens {
                    self.emit(owner, output, |e| body.write(body.root(), e))?;
                } else {
                    self.record(owner, output, |first, e| fields(&body, body.root(), &[], first, e))?;
                }
            }
            Framing::JsonArray => {
                let close: &[u8] = if self.array_started { b"]" } else { b"[]" };
                output(encode::scratch(owner, |e| e.bytes(close))?)?;
            }
            Framing::Sse => (),
        }
        self.terminal = true;
        Ok(())
    }
    fn error(&mut self, owner: &mut Writer, output: &mut impl FnMut(ScratchLease) -> Result<()>) -> Result<()> {
        self.emit(owner, output, |e| {
            e.bytes(b"{\"error\":{\"code\":502,\"message\":\"Incomplete upstream response\",\"status\":\"INTERNAL\"}}")
        })?;
        if self.framing == Framing::JsonArray {
            output(encode::scratch(owner, |e| e.bytes(b"]"))?)?;
        }
        self.terminal = true;
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
        if self.terminal || self.failed {
            return Err(Error::Closed);
        }
        let result =
            internal::decode(owner, input).and_then(|(kind, event)| self.accept(owner, kind, event, &mut output));
        self.failed = result.is_err();
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
/// Writes the members of `parent` not named in `skip`, separated by commas; `first` tells
/// whether the enclosing object has no member yet.
fn fields(doc: &Document, parent: usize, skip: &[&str], first: &mut bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    for node in doc.children(parent)? {
        let name = doc.node(node)?.name.ok_or(Error::Schema)?;
        if skip
            .iter()
            .try_fold(false, |found, key| Ok::<_, Error>(found || doc.name_is(name, key)?))?
        {
            continue;
        }
        if !std::mem::replace(first, false) {
            e.bytes(b",")?;
        }
        doc.write_name(name, e)?;
        e.bytes(b":")?;
        doc.write(node, e)?;
    }
    Ok(())
}
fn local(doc: &Document, node: usize) -> Result<LocalId> {
    let mut buffer = [0; 64];
    let mut len = 0;
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        if !c.is_ascii() || len == buffer.len() {
            return Err(Error::Schema);
        }
        buffer[len] = c as u8;
        len += 1;
    }
    LocalId::new(std::str::from_utf8(&buffer[..len]).map_err(|_| Error::Schema)?)
}
/// Whether the record marks its canonical field `name` as synthesized.
fn synthetic(doc: &Document, x: usize, name: &str) -> Result<bool> {
    let Some(fields) = doc.member(x, "synthetic")? else {
        return Ok(false);
    };
    for field in doc.children(fields)? {
        if doc.string_is(field, name)? {
            return Ok(true);
        }
    }
    Ok(false)
}
/// Whether the record's payload came from a Gemini source, so its native value has a carrier.
fn native(doc: &Document, x: usize) -> Result<bool> {
    match doc.member(x, "source")? {
        Some(source) => super::is(doc, source, "protocol", "gemini"),
        None => Ok(false),
    }
}
