use super::{Table, is, object, string, usage::Usage};
use crate::codec::{
    self, BodyMode, Error, Operation, Result,
    document::{Document, Name, Value},
    encode::{self, Encoder},
    event::{Emitter, Identity, Kind, LocalId, Scope},
    json, media,
    source::Source,
    sse,
};
use ck_stream::Writer;

mod block;
mod complete;
mod event;
mod item;
mod tools;
pub(super) use tools::Server;

pub struct Ingress {
    owner: u64,
    source: Option<Source>,
    scanner: Option<sse::Scanner>,
    json: Option<json::Scanner>,
    state: State,
    failed: bool,
}
impl Ingress {
    pub fn new(
        owner: &mut Writer,
        identity: Identity,
        operation: Operation,
        mode: BodyMode,
        limit: u64,
    ) -> Result<Self> {
        if operation == Operation::CountTokens || (operation == Operation::Compact && mode == BodyMode::Streaming) {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            owner: owner.id(),
            source: Some(Source::new(owner)?),
            scanner: if mode == BodyMode::Streaming {
                Some(sse::Scanner::new(owner, limit)?)
            } else {
                None
            },
            json: if mode == BodyMode::Buffered {
                Some(json::Scanner::new(owner, limit)?)
            } else {
                None
            },
            state: State {
                emitter: Emitter::new(identity),
                identity,
                operation,
                base: None,
                items: Table::new(owner)?,
                usage: Usage::default(),
                terminal: false,
                next_media: 0,
                audio: None,
            },
            failed: false,
        })
    }
    fn handle(&mut self, owner: &mut Writer, result: Result<()>) -> Result<()> {
        if let Err(error) = result {
            self.failed = true;
            if !self.state.terminal && !matches!(error, Error::Storage(_)) {
                super::failure(&mut self.state.emitter, owner, false)?;
                self.state.terminal = true;
            }
            return Err(error);
        }
        Ok(())
    }
}
impl codec::ResponseIngress for Ingress {
    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> Result<()> {
        crate::boundary::enter(self.owner, owner, self.failed || self.state.terminal)?;
        let result = (|| {
            let source = self.source.as_mut().ok_or(Error::Closed)?;
            source.push_bytes(owner, bytes)?;
            if let Some(scanner) = &mut self.scanner {
                let state = &mut self.state;
                scanner.advance(source, |event| state.event(owner, source, event))?;
                source.release_before(scanner.consumed())?;
            } else {
                self.json.as_mut().ok_or(Error::Closed)?.advance(source)?;
            }
            Ok(())
        })();
        self.handle(owner, result)
    }
    fn finish(&mut self, owner: &mut Writer) -> Result<()> {
        crate::boundary::enter(self.owner, owner, self.failed)?;
        let result = (|| {
            if let Some(scanner) = &mut self.scanner {
                let source = self.source.as_ref().ok_or(Error::Closed)?;
                let state = &mut self.state;
                scanner.finish(source, |event| state.event(owner, source, event))?;
                if !state.terminal {
                    return Err(Error::Truncated);
                }
            } else {
                let source = self.source.take().ok_or(Error::Closed)?;
                let mut scanner = self.json.take().ok_or(Error::Closed)?;
                scanner.finish(&source)?;
                let doc = Document::from_scanned(owner, source, scanner)?;
                self.state.complete(owner, &doc, doc.root())?;
            }
            Ok(())
        })();
        self.handle(owner, result)
    }
}
struct Block {
    index: u64,
    summary: bool,
    value: usize,
    closed: bool,
    /// A part type without a canonical mapping: its members stay attributes, never content.
    opaque: bool,
}
struct Item {
    index: u64,
    id: LocalId,
    tool: Option<LocalId>,
    doc: Document,
    blocks: Table<Block>,
    closed: bool,
    arguments: Option<usize>,
    tool_closed: bool,
    arguments_done: bool,
    /// The closing snapshot marks the call incomplete: its arguments may be cut off.
    incomplete: bool,
    server: Option<Server>,
    code: Option<usize>,
    code_closed: bool,
    code_null: bool,
    results: Option<usize>,
    encrypted: bool,
    image: Option<[u8; 32]>,
}
struct State {
    emitter: Emitter,
    identity: Identity,
    operation: Operation,
    base: Option<Document>,
    items: Table<Item>,
    usage: Usage,
    terminal: bool,
    next_media: u64,
    audio: Option<super::audio::Ingress>,
}
impl Item {
    fn scope(&self, block: Option<u64>) -> Scope {
        Scope {
            choice_index: Some(0),
            item_id: Some(self.id),
            block_index: block,
            tool_call_id: self.tool,
            tool_index: self.tool.map(|_| self.index),
            ..Scope::default()
        }
    }
    fn custom(&self) -> Result<bool> {
        is(&self.doc, self.doc.root(), "type", "custom_tool_call")
    }
}
impl State {
    fn start(&mut self, owner: &mut Writer, doc: Option<(&Document, usize)>) -> Result<()> {
        let existed = self.base.is_some();
        let base = super::mapped(owner, |e| {
            e.bytes(b"{")?;
            for (i, (name, native)) in [("id", "id"), ("model", "model"), ("created", "created_at")]
                .iter()
                .enumerate()
            {
                if i > 0 {
                    e.bytes(b",")?;
                }
                e.string(name)?;
                e.bytes(b":")?;
                if let Some((d, r)) = doc
                    && let Some(n) = d.member(r, native)?
                {
                    d.write(n, e)?;
                } else if let Some(base) = &self.base {
                    base.write(base.required(base.root(), name)?, e)?;
                } else if *name == "created" {
                    e.unsigned(0)?;
                } else {
                    e.string(if *name == "id" {
                        self.identity.stream_id.as_str()
                    } else {
                        ""
                    })?;
                }
            }
            e.bytes(b"}")
        })?;
        if !existed {
            self.emitter.emit(
                owner,
                Kind::ResponseStart,
                Scope::default(),
                |e| {
                    for name in ["id", "model", "created"] {
                        super::member(&base, base.root(), name, e)?;
                    }
                    match doc {
                        Some((d, r)) => match d.member(r, "status")? {
                            Some(status) if string(d, status).is_ok() => {
                                e.bytes(b",\"status\":")?;
                                d.write(status, e)
                            }
                            _ => Ok(()),
                        },
                        None => Ok(()),
                    }
                },
                |e| e.bytes(b",\"source\":{\"protocol\":\"responses\"}"),
            )?;
        }
        self.base = Some(base);
        Ok(())
    }
    fn unknown(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        known: &[&str],
        source: &Source,
        event: &sse::Event,
    ) -> Result<()> {
        let mut unknown = doc.member(doc.root(), "sequence_number")?.is_some();
        for field in doc.children(doc.root())? {
            let name = doc.node(field)?.name.ok_or(Error::Schema)?;
            unknown |= !known
                .iter()
                .try_fold(false, |found, k| Ok::<_, Error>(found || doc.name_is(name, k)?))?;
        }
        if unknown
            || event.fields().iter().any(|f| {
                f.comment
                    || (!source.equals(f.name, b"data").unwrap_or(false)
                        && !source.equals(f.name, b"event").unwrap_or(false))
            })
        {
            self.emitter.emit(
                owner,
                Kind::Opaque,
                Scope::default(),
                |e| {
                    e.bytes(b",\"name\":\"responses.fields\",\"format\":\"json\",\"value\":")?;
                    object(doc, doc.root(), known, e)
                },
                |e| sse_source(source, event, Some(doc), e),
            )?;
        }
        Ok(())
    }
    fn find_event(&self, doc: &Document) -> Result<usize> {
        let root = doc.root();
        let index = doc.member(root, "output_index")?.map(|n| doc.unsigned(n)).transpose()?;
        for (at, item) in self.items.iter().enumerate() {
            if index == Some(item.index) {
                if let (Some(a), Some(b)) = (doc.member(root, "item_id")?, item.doc.member(item.doc.root(), "id")?)
                    && !equal(doc, a, &item.doc, b)?
                {
                    return Err(Error::Schema);
                }
                return Ok(at);
            }
        }
        Err(Error::Schema)
    }
}
/// Chat finish of a validated terminal response: a filtered or truncated incomplete result, a
/// call the client must run, or a stop. Server tools already ran and need no client call.
fn finish_reason(doc: &Document, root: usize, items: &[Item]) -> Result<&'static str> {
    if is(doc, root, "status", "incomplete")? {
        let filtered = match doc.member(root, "incomplete_details")? {
            Some(details) => is(doc, details, "reason", "content_filter")?,
            None => false,
        };
        return Ok(if filtered { "content_filter" } else { "length" });
    }
    if items.iter().any(|item| item.tool.is_some() && item.server.is_none()) {
        return Ok("tool_calls");
    }
    Ok("stop")
}
fn validate_arguments(owner: &mut Writer, item: &Item) -> Result<()> {
    if item.custom()? {
        return Ok(());
    }
    let Some(args) = item.arguments else {
        return Err(Error::Truncated);
    };
    let lease = encode::scratch(owner, |e| {
        let mut chars = Logical::new(&item.doc, args)?;
        while let Some(c) = chars.next()? {
            e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
        }
        Ok(())
    })?;
    let mut source = Source::new(owner)?;
    source.push_scratch(lease)?;
    let len = source.len();
    Document::parse(owner, source, len)?;
    Ok(())
}
fn provenance(doc: &Document, node: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b",\"source\":{\"protocol\":\"responses\"")?;
    for name in ["sequence_number", "output_index", "content_index", "summary_index"] {
        super::member(doc, node, name, e)?;
    }
    e.bytes(b"}")
}
fn sse_source(source: &Source, event: &sse::Event, native: Option<&Document>, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b",\"source\":{\"protocol\":\"responses\"")?;
    if let Some(doc) = native {
        if let Some(sequence) = doc.member(doc.root(), "sequence_number")? {
            e.bytes(b",\"sequence\":")?;
            doc.write(sequence, e)?;
        }
        for name in ["output_index", "content_index", "summary_index"] {
            super::member(doc, doc.root(), name, e)?;
        }
    }
    if let Some(name) = event.name(source)? {
        e.bytes(b",\"event\":")?;
        e.quoted_json(|e| e.span(source, name))?;
    } else if let Some(doc) = native
        && let Some(kind) = doc.member(doc.root(), "type")?
    {
        e.bytes(b",\"event\":")?;
        doc.write(kind, e)?;
    }
    e.bytes(b",\"sse\":[")?;
    let mut first = true;
    for f in event.fields() {
        if source.equals(f.name, b"data")? {
            continue;
        }
        if !first {
            e.bytes(b",")?;
        }
        first = false;
        e.bytes(b"{\"name\":")?;
        e.quoted_json(|e| e.span(source, f.name))?;
        e.bytes(b",\"value\":")?;
        e.quoted_json(|e| e.span(source, f.value))?;
        e.bytes(b",\"comment\":")?;
        e.bytes(if f.comment { b"true" } else { b"false" })?;
        e.bytes(b"}")?;
    }
    e.bytes(b"]}")
}
fn chunk_base(base: &Document, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b",\"object\":\"chat.completion.chunk\"")?;
    for name in ["id", "model", "created"] {
        super::member(base, base.root(), name, e)?;
    }
    Ok(())
}
pub(super) fn append_fragment(doc: &mut Document, parent: usize, name: &'static str, incoming: usize) -> Result<usize> {
    let target = match doc.member(parent, name)? {
        Some(node) => node,
        None => doc.set(parent, name, Value::Fragments)?,
    };
    if !matches!(doc.node(target)?.value, Value::Fragments) {
        let first = doc.add(None, doc.node(target)?.value)?;
        doc.replace(target, Value::Fragments)?;
        doc.append(target, first)?;
    }
    if let Some(parent) = doc.node(incoming)?.parent {
        doc.detach(parent, incoming)?;
    }
    doc.append(target, incoming)?;
    Ok(target)
}
struct Logical<'a> {
    doc: &'a Document,
    next: Option<usize>,
    chars: Option<crate::codec::document::Characters<'a>>,
}
impl<'a> Logical<'a> {
    fn new(doc: &'a Document, node: usize) -> Result<Self> {
        if matches!(doc.node(node)?.value, Value::Fragments) {
            Ok(Self {
                doc,
                next: doc.node(node)?.first,
                chars: None,
            })
        } else {
            Ok(Self {
                doc,
                next: None,
                chars: Some(doc.chars(node)?),
            })
        }
    }
    fn next(&mut self) -> Result<Option<char>> {
        loop {
            if let Some(chars) = &mut self.chars
                && let Some(c) = chars.next_scalar()?
            {
                return Ok(Some(c));
            }
            let Some(node) = self.next else { return Ok(None) };
            self.next = self.doc.node(node)?.next;
            self.chars = Some(self.doc.chars(node)?);
        }
    }
}
pub(super) fn suffix(
    owner: &mut Writer,
    old: &Document,
    node: Option<usize>,
    incoming: &Document,
    value: usize,
) -> Result<Document> {
    super::mapped(owner, |e| {
        let mut new = Logical::new(incoming, value)?;
        if let Some(node) = node {
            let mut previous = Logical::new(old, node)?;
            while let Some(c) = previous.next()? {
                if new.next()? != Some(c) {
                    return Err(Error::Schema);
                }
            }
        }
        e.bytes(b"\"")?;
        while let Some(c) = new.next()? {
            e.escaped(c.encode_utf8(&mut [0; 4]))?;
        }
        e.bytes(b"\"")
    })
}
fn equal(a: &Document, an: usize, b: &Document, bn: usize) -> Result<bool> {
    let mut a = Logical::new(a, an)?;
    let mut b = Logical::new(b, bn)?;
    loop {
        let c = a.next()?;
        if c != b.next()? {
            return Ok(false);
        }
        if c.is_none() {
            return Ok(true);
        }
    }
}
