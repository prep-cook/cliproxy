use super::{is, string, usage::Usage};
use crate::codec::{
    self, BodyMode, Error, Operation, Result,
    document::{Document, Value},
    encode::{self, Encoder},
    event::{Emitter, Identity, Kind, LocalId, Scope},
    json, media,
    source::Source,
    sse,
};
use ck_stream::{MetadataPermit, Writer};

#[path = "response_tools.rs"]
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
                items: Vec::new(),
                metadata: owner.reserve_metadata(0)?,
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
        if owner.id() != self.owner {
            return Err(Error::Position);
        }
        if self.failed || self.state.terminal {
            return Err(Error::Closed);
        }
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
        if owner.id() != self.owner {
            return Err(Error::Position);
        }
        if self.failed {
            return Err(Error::Closed);
        }
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
}
struct Item {
    index: u64,
    id: LocalId,
    tool: Option<LocalId>,
    doc: Document,
    blocks: Vec<Block>,
    metadata: MetadataPermit,
    closed: bool,
    arguments: Option<usize>,
    tool_closed: bool,
    arguments_done: bool,
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
    items: Vec<Item>,
    metadata: MetadataPermit,
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
                    Ok(())
                },
                |e| e.bytes(b",\"source\":{\"protocol\":\"responses\"}"),
            )?;
        }
        self.base = Some(base);
        Ok(())
    }
    fn event(&mut self, owner: &mut Writer, source: &Source, event: &sse::Event) -> Result<()> {
        if self.terminal {
            return Err(Error::Closed);
        }
        if !event.has_data(source)? {
            if !event.fields().is_empty() {
                if self.base.is_none() {
                    self.start(owner, None)?;
                }
                self.emitter.emit(
                    owner,
                    Kind::Heartbeat,
                    Scope::default(),
                    |_| Ok(()),
                    |e| sse_source(source, event, None, e),
                )?;
            }
            return Ok(());
        }
        let parsed = if event.data_len(source)? == 0 {
            Err(Error::Json)
        } else {
            let lease = encode::scratch(owner, |e| event.write_data(source, |b| e.bytes(b)))?;
            let mut data = Source::new(owner)?;
            data.push_scratch(lease)?;
            let len = data.len();
            Document::parse(owner, data, len)
        };
        let doc = match parsed {
            Ok(doc) => doc,
            Err(Error::Json | Error::Truncated) => {
                let name = event.name(source)?.ok_or(Error::Json)?;
                let mut cursor = source.cursor(name)?;
                let mut known = true;
                for byte in b"response." {
                    known &= cursor.next_byte()? == Some(*byte);
                }
                if known || source.equals(name, b"error")? {
                    return Err(Error::Json);
                }
                if self.base.is_none() {
                    self.start(owner, None)?;
                }
                self.emitter.emit(
                    owner,
                    Kind::Opaque,
                    Scope::default(),
                    |e| {
                        e.bytes(b",\"format\":\"text\",\"value\":")?;
                        e.quoted_json(|e| event.write_data(source, |b| e.bytes(b)))
                    },
                    |e| sse_source(source, event, None, e),
                )?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let root = doc.root();
        if is(&doc, root, "type", "error")?
            || is(&doc, root, "type", "response.failed")?
            || doc.member(root, "error")?.is_some()
        {
            return Err(Error::Schema);
        }
        if let Some(response) = doc.member(root, "response")? {
            self.start(owner, Some((&doc, response)))?;
            if !is(&doc, root, "type", "response.completed")?
                && !is(&doc, root, "type", "response.incomplete")?
                && let Some(usage) = doc.member(response, "usage")?
            {
                self.usage.update(owner, &doc, usage)?;
                self.emitter.emit(
                    owner,
                    Kind::Usage,
                    Scope::default(),
                    |e| {
                        e.bytes(b",\"usage\":")?;
                        self.usage.write(e, false)
                    },
                    |_| Ok(()),
                )?;
            }
        } else if self.base.is_none() {
            self.start(owner, None)?;
        }
        if is(&doc, root, "type", "response.completed")? || is(&doc, root, "type", "response.incomplete")? {
            let response = doc.required(root, "response")?;
            self.unknown(owner, &doc, &["type", "response", "sequence_number"], source, event)?;
            return self.complete(owner, &doc, response);
        }
        if is(&doc, root, "type", "response.created")? || is(&doc, root, "type", "response.in_progress")? {
            self.unknown(owner, &doc, &["type", "response"], source, event)?;
            return Ok(());
        }
        if is(&doc, root, "type", "response.output_item.added")? {
            let index = doc.unsigned(doc.required(root, "output_index")?)?;
            let item = doc.required(root, "item")?;
            self.add_item(owner, &doc, item, index)?;
        } else if is(&doc, root, "type", "response.output_item.done")? {
            let index = doc.unsigned(doc.required(root, "output_index")?)?;
            let item = doc.required(root, "item")?;
            let at = self.find_or_add(owner, &doc, item, index)?;
            self.snapshot_item(owner, at, &doc, item, true)?;
        } else if is(&doc, root, "type", "response.content_part.added")?
            || is(&doc, root, "type", "response.reasoning_summary_part.added")?
        {
            let at = self.find_event(&doc)?;
            let summary = is(&doc, root, "type", "response.reasoning_summary_part.added")?;
            let index = doc.unsigned(doc.required(root, if summary { "summary_index" } else { "content_index" })?)?;
            let part = doc.required(root, "part")?;
            self.block(owner, at, (index, summary), &doc, part, (false, false))?;
        } else if is(&doc, root, "type", "response.content_part.done")?
            || is(&doc, root, "type", "response.reasoning_summary_part.done")?
        {
            let at = self.find_event(&doc)?;
            let summary = is(&doc, root, "type", "response.reasoning_summary_part.done")?;
            let index = doc.unsigned(doc.required(root, if summary { "summary_index" } else { "content_index" })?)?;
            let part = doc.required(root, "part")?;
            self.block(owner, at, (index, summary), &doc, part, (true, true))?;
        } else if self.text_event(owner, &doc)?
            || self.audio_event(owner, &doc)?
            || self.execution_event(owner, &doc)?
        {
        } else if is(&doc, root, "type", "response.image_generation_call.partial_image")? {
            let at = self.find_event(&doc)?;
            let data = doc.required(root, "partial_image_b64")?;
            let partial = doc.unsigned(doc.required(root, "partial_image_index")?)?;
            self.image(owner, at, &doc, data, Some(partial))?;
        } else if is(&doc, root, "type", "response.function_call_arguments.delta")?
            || is(&doc, root, "type", "response.custom_tool_call_input.delta")?
            || is(&doc, root, "type", "response.mcp_call_arguments.delta")?
        {
            let at = self.find_event(&doc)?;
            let delta = doc.required(root, "delta")?;
            self.arguments(owner, at, &doc, delta, false)?;
        } else if is(&doc, root, "type", "response.function_call_arguments.done")?
            || is(&doc, root, "type", "response.custom_tool_call_input.done")?
            || is(&doc, root, "type", "response.mcp_call_arguments.done")?
        {
            let at = self.find_event(&doc)?;
            let custom = self.items[at].custom()?;
            let value = doc.required(root, if custom { "input" } else { "arguments" })?;
            self.arguments(owner, at, &doc, value, true)?;
        } else if is(&doc, root, "type", "response.output_text.annotation.added")? {
            let at = self.find_event(&doc)?;
            let index = doc.unsigned(doc.required(root, "content_index")?)?;
            let annotation = doc.required(root, "annotation")?;
            let local = self.items[at]
                .blocks
                .iter()
                .position(|b| b.index == index && !b.summary)
                .ok_or(Error::Schema)? as u64;
            self.emitter.emit(
                owner,
                Kind::Annotation,
                self.items[at].scope(Some(local)),
                |e| {
                    e.bytes(b",\"kind\":\"citation\",\"value\":")?;
                    doc.write(annotation, e)
                },
                |e| provenance(&doc, root, e),
            )?;
        } else {
            self.emitter.emit(
                owner,
                Kind::Opaque,
                Scope::default(),
                |e| {
                    e.bytes(b",\"format\":\"json\",\"value\":")?;
                    doc.write(root, e)
                },
                |e| sse_source(source, event, Some(&doc), e),
            )?;
            return Ok(());
        }
        self.unknown(
            owner,
            &doc,
            &[
                "type",
                "output_index",
                "item_id",
                "content_index",
                "summary_index",
                "item",
                "part",
                "delta",
                "text",
                "code",
                "refusal",
                "arguments",
                "input",
                "annotation",
                "sequence_number",
                "partial_image_b64",
                "partial_image_index",
            ],
            source,
            event,
        )
    }
    fn audio_event(&mut self, owner: &mut Writer, doc: &Document) -> Result<bool> {
        let root = doc.root();
        let delta = is(doc, root, "type", "response.audio.delta")?;
        let done = is(doc, root, "type", "response.audio.done")?;
        let transcript = is(doc, root, "type", "response.audio.transcript.delta")?;
        let transcript_done = is(doc, root, "type", "response.audio.transcript.done")?;
        if !delta && !done && !transcript && !transcript_done {
            return Ok(false);
        }
        if self.audio.is_none() {
            let id = media::local_id(b'm', self.next_media)?;
            self.next_media = self.next_media.checked_add(1).ok_or(Error::Limit)?;
            self.audio = Some(super::audio::Ingress::start(owner, &mut self.emitter, id, None)?);
        }
        let audio = self.audio.as_mut().ok_or(Error::Schema)?;
        if delta {
            audio.data(owner, &mut self.emitter, doc, doc.required(root, "delta")?)?;
        } else if done {
            audio.audio_done()?;
        } else if transcript {
            audio.transcript(owner, &mut self.emitter, doc, doc.required(root, "delta")?, false)?;
        } else {
            audio.transcript_done(owner, &mut self.emitter)?;
        }
        Ok(true)
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
            let mut recognized = false;
            for k in known {
                recognized |= doc.name_is(name, k)?;
            }
            unknown |= !recognized;
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
                    e.bytes(b",\"name\":\"responses.fields\",\"format\":\"json\",\"value\":{")?;
                    let mut first = true;
                    for field in doc.children(doc.root())? {
                        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
                        let mut recognized = false;
                        for k in known {
                            recognized |= doc.name_is(name, k)?;
                        }
                        if !recognized {
                            if !first {
                                e.bytes(b",")?;
                            }
                            first = false;
                            doc.write_name(name, e)?;
                            e.bytes(b":")?;
                            doc.write(field, e)?;
                        }
                    }
                    e.bytes(b"}")
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
    fn find_or_add(&mut self, owner: &mut Writer, doc: &Document, node: usize, index: u64) -> Result<usize> {
        if let Some(at) = self.items.iter().position(|item| item.index == index) {
            if let (Some(a), Some(b)) = (
                doc.member(node, "id")?,
                self.items[at].doc.member(self.items[at].doc.root(), "id")?,
            ) && !equal(doc, a, &self.items[at].doc, b)?
            {
                return Err(Error::Schema);
            }
            Ok(at)
        } else {
            self.add_item(owner, doc, node, index)
        }
    }
    fn add_item(&mut self, owner: &mut Writer, doc: &Document, node: usize, index: u64) -> Result<usize> {
        if self.items.iter().any(|item| item.index == index) {
            return Err(Error::Schema);
        }
        let native = super::mapped(owner, |e| item_metadata(doc, node, e))?;
        let root = native.root();
        let id = media::local_id(b'i', index)?;
        let server = Server::from_item(&native, root)?;
        let tool = server.is_some()
            || is(&native, root, "type", "function_call")?
            || is(&native, root, "type", "custom_tool_call")?;
        let scope = Scope {
            choice_index: Some(0),
            item_id: Some(id),
            ..Scope::default()
        };
        self.emitter.emit(
            owner,
            Kind::ItemStart,
            scope,
            |e| {
                e.bytes(b",\"kind\":")?;
                if let Some(kind) = native.member(root, "type")? {
                    native.write(kind, e)?;
                } else {
                    e.string("message")?;
                }
                super::member(&native, root, "role", e)?;
                e.bytes(b",\"attributes\":")?;
                native.write(root, e)
            },
            |e| {
                e.bytes(b",\"source\":{\"protocol\":\"responses\",\"output_index\":")?;
                e.unsigned(index)?;
                e.bytes(b",\"ids\":{")?;
                let mut first = true;
                for name in ["id", "call_id"] {
                    if let Some(n) = native.member(root, name)? {
                        if !first {
                            e.bytes(b",")?;
                        }
                        first = false;
                        e.string(name)?;
                        e.bytes(b":")?;
                        native.write(n, e)?;
                    }
                }
                e.bytes(b"}}")
            },
        )?;
        let item = Item {
            index,
            id,
            tool: if tool {
                Some(media::local_id(b't', index)?)
            } else {
                None
            },
            doc: native,
            blocks: Vec::new(),
            metadata: owner.reserve_metadata(0)?,
            closed: false,
            arguments: None,
            arguments_done: false,
            tool_closed: false,
            server,
            code: None,
            code_closed: false,
            code_null: false,
            results: None,
            encrypted: false,
            image: None,
        };
        if tool {
            let custom = item.custom()?;
            self.emitter.emit(
                owner,
                Kind::ToolStart,
                item.scope(None),
                |e| {
                    e.bytes(b",\"name\":")?;
                    if let Some(name) = item.doc.member(root, "name")? {
                        item.doc.write(name, e)?;
                    } else if let Some(server) = server {
                        e.string(server.name())?;
                    } else {
                        return Err(Error::Schema);
                    }
                    e.bytes(b",\"kind\":")?;
                    e.string(if server.is_some() {
                        "server"
                    } else if custom {
                        "custom"
                    } else {
                        "function"
                    })?;
                    e.bytes(b",\"arguments_format\":")?;
                    e.string(if custom { "text" } else { "json" })?;
                    super::member(&item.doc, root, "namespace", e)
                },
                |e| {
                    e.bytes(b",\"source\":{\"protocol\":\"responses\",\"ids\":{")?;
                    let mut first = true;
                    for name in ["id", "call_id"] {
                        if let Some(n) = item.doc.member(root, name)? {
                            if !first {
                                e.bytes(b",")?;
                            }
                            first = false;
                            e.string(name)?;
                            e.bytes(b":")?;
                            item.doc.write(n, e)?;
                        }
                    }
                    e.bytes(b"}}")
                },
            )?;
        }
        reserve(&mut self.items, &mut self.metadata)?;
        let at = self.items.len();
        self.items.push(item);
        self.snapshot_item(owner, at, doc, node, false)?;
        Ok(at)
    }
    fn text_event(&mut self, owner: &mut Writer, doc: &Document) -> Result<bool> {
        let root = doc.root();
        for (delta, done, field, summary) in [
            ("response.output_text.delta", "response.output_text.done", "text", false),
            ("response.refusal.delta", "response.refusal.done", "refusal", false),
            (
                "response.reasoning_summary_text.delta",
                "response.reasoning_summary_text.done",
                "text",
                true,
            ),
            (
                "response.reasoning_text.delta",
                "response.reasoning_text.done",
                "text",
                false,
            ),
        ] {
            let snapshot = is(doc, root, "type", done)?;
            if snapshot || is(doc, root, "type", delta)? {
                let at = self.find_event(doc)?;
                let index =
                    doc.unsigned(doc.required(root, if summary { "summary_index" } else { "content_index" })?)?;
                let part = super::mapped(owner, |e| {
                    e.bytes(b"{\"type\":")?;
                    e.string(if field == "refusal" {
                        "refusal"
                    } else if summary {
                        "summary_text"
                    } else if delta == "response.reasoning_text.delta" {
                        "reasoning_text"
                    } else {
                        "output_text"
                    })?;
                    e.bytes(b",")?;
                    e.string(field)?;
                    e.bytes(b":")?;
                    doc.write(doc.required(root, if snapshot { field } else { "delta" })?, e)?;
                    super::member(doc, root, "logprobs", e)?;
                    e.bytes(b"}")
                })?;
                self.block(owner, at, (index, summary), &part, part.root(), (snapshot, false))?;
                return Ok(true);
            }
        }
        Ok(false)
    }
    fn block(
        &mut self,
        owner: &mut Writer,
        at: usize,
        (index, summary): (u64, bool),
        doc: &Document,
        part: usize,
        (snapshot, close): (bool, bool),
    ) -> Result<()> {
        let item = &mut self.items[at];
        let existing = item
            .blocks
            .iter()
            .position(|b| b.index == index && b.summary == summary);
        if item.closed && existing.is_none() {
            return Err(Error::Schema);
        }
        let block = if let Some(b) = existing {
            b
        } else {
            let lease = encode::scratch(owner, |e| {
                write_except(doc, part, e, &["text", "refusal", "annotations", "logprobs"])
            })?;
            let value = item.doc.import_json(owner, lease)?;
            let scope = item.scope(Some(item.blocks.len() as u64));
            let kind = if summary {
                "reasoning_summary"
            } else if is(doc, part, "type", "reasoning_text")? {
                "thinking"
            } else if is(doc, part, "type", "refusal")? {
                "refusal"
            } else {
                "text"
            };
            self.emitter.emit(
                owner,
                Kind::BlockStart,
                scope,
                |e| {
                    e.bytes(b",\"kind\":")?;
                    e.string(kind)?;
                    e.bytes(b",\"attributes\":")?;
                    item.doc.write(value, e)
                },
                |e| {
                    e.bytes(b",\"source\":{\"protocol\":\"responses\"")?;
                    e.bytes(if summary {
                        b",\"summary_index\":"
                    } else {
                        b",\"content_index\":"
                    })?;
                    e.unsigned(index)?;
                    e.bytes(b"}")
                },
            )?;
            reserve(&mut item.blocks, &mut item.metadata)?;
            let b = item.blocks.len();
            item.blocks.push(Block {
                index,
                summary,
                value,
                closed: false,
            });
            b
        };
        let value = item.blocks[block].value;
        for (native, delta) in [
            (
                "text",
                if summary || is(doc, part, "type", "reasoning_text")? {
                    "reasoning_content"
                } else {
                    "content"
                },
            ),
            ("refusal", "refusal"),
        ] {
            if let Some(incoming) = doc.member(part, native)? {
                string(doc, incoming)?;
                let old = item.doc.member(value, native)?;
                let fragment = if snapshot {
                    suffix(owner, &item.doc, old, doc, incoming)?
                } else {
                    super::mapped(owner, |e| doc.write(incoming, e))?
                };
                let empty = fragment.chars(fragment.root())?.next_scalar()?.is_none();
                if (item.blocks[block].closed || item.closed) && !empty {
                    return Err(Error::Schema);
                }
                if !empty || old.is_none() {
                    let base = self.base.as_ref().ok_or(Error::Schema)?;
                    self.emitter.emit(
                        owner,
                        Kind::Chunk,
                        item.scope(Some(block as u64)),
                        |e| {
                            chunk_base(base, e)?;
                            e.bytes(b",\"choices\":[{\"index\":0,\"delta\":{")?;
                            e.string(delta)?;
                            e.bytes(b":")?;
                            fragment.write(fragment.root(), e)?;
                            e.bytes(b"},\"finish_reason\":null")?;
                            e.bytes(b"}]")
                        },
                        |e| e.bytes(b",\"source\":{\"protocol\":\"responses\"}"),
                    )?;
                    let imported = item.doc.absorb(fragment)?;
                    append_fragment(&mut item.doc, value, native, imported)?;
                }
            }
        }
        if let Some(logprobs) = doc.member(part, "logprobs")? {
            let tail = super::snapshot::array_tail(
                owner,
                &item.doc,
                item.doc.member(value, "logprobs")?,
                doc,
                logprobs,
                snapshot,
            )?;
            if tail.node(tail.root())?.first.is_some() {
                if item.blocks[block].closed || item.closed {
                    return Err(Error::Schema);
                }
                let base = self.base.as_ref().ok_or(Error::Schema)?;
                self.emitter.emit(
                    owner,
                    Kind::Chunk,
                    item.scope(Some(block as u64)),
                    |e| {
                        chunk_base(base, e)?;
                        e.bytes(b",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":null")?;
                        super::snapshot::write_logprobs(&tail, e)?;
                        e.bytes(b"}]")
                    },
                    |_| Ok(()),
                )?;
                super::snapshot::append_array(&mut item.doc, value, "logprobs", tail)?;
            }
        }
        if let Some(annotations) = doc.member(part, "annotations")? {
            let tail = super::snapshot::array_tail(
                owner,
                &item.doc,
                item.doc.member(value, "annotations")?,
                doc,
                annotations,
                snapshot,
            )?;
            for annotation in tail.children(tail.root())? {
                if item.blocks[block].closed || item.closed {
                    return Err(Error::Schema);
                }
                self.emitter.emit(
                    owner,
                    Kind::Annotation,
                    item.scope(Some(block as u64)),
                    |e| {
                        e.bytes(b",\"kind\":\"citation\",\"value\":")?;
                        tail.write(annotation, e)
                    },
                    |_| Ok(()),
                )?;
            }
            super::snapshot::append_array(&mut item.doc, value, "annotations", tail)?;
        }
        if close && !item.blocks[block].closed {
            self.emitter.emit(
                owner,
                Kind::BlockEnd,
                item.scope(Some(block as u64)),
                |_| Ok(()),
                |_| Ok(()),
            )?;
            item.blocks[block].closed = true;
        }
        Ok(())
    }
    fn arguments(&mut self, owner: &mut Writer, at: usize, doc: &Document, node: usize, snapshot: bool) -> Result<()> {
        let item = &mut self.items[at];
        if item.tool.is_none() {
            return Err(Error::Schema);
        }
        string(doc, node)?;
        let fragment = if snapshot {
            suffix(owner, &item.doc, item.arguments, doc, node)?
        } else {
            super::mapped(owner, |e| doc.write(node, e))?
        };
        let empty = fragment.chars(fragment.root())?.next_scalar()?.is_none();
        if (item.tool_closed || item.arguments_done) && !empty {
            return Err(Error::Schema);
        }
        if !empty || item.arguments.is_none() {
            let base = self.base.as_ref().ok_or(Error::Schema)?;
            self.emitter.emit(
                owner,
                Kind::Chunk,
                item.scope(None),
                |e| {
                    chunk_base(base, e)?;
                    e.bytes(b",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":")?;
                    e.unsigned(item.index)?;
                    e.bytes(b",\"function\":{\"arguments\":")?;
                    fragment.write(fragment.root(), e)?;
                    e.bytes(b"}}]},\"finish_reason\":null}]")
                },
                |_| Ok(()),
            )?;
            let imported = item.doc.absorb(fragment)?;
            let root = item.doc.root();
            let field = append_fragment(&mut item.doc, root, "arguments", imported)?;
            item.arguments = Some(field);
        }
        if snapshot && !item.tool_closed {
            validate_arguments(owner, item)?;
            item.arguments_done = true;
            if item.server.is_some() {
                return Ok(());
            }
            self.emitter.emit(
                owner,
                Kind::ToolEnd,
                item.scope(None),
                |e| {
                    e.bytes(if item.custom()? {
                        b",\"status\":\"completed\""
                    } else {
                        b",\"status\":\"completed\",\"valid_json\":true"
                    })
                },
                |_| Ok(()),
            )?;
            item.tool_closed = true;
        }
        Ok(())
    }
    fn snapshot_item(&mut self, owner: &mut Writer, at: usize, doc: &Document, node: usize, close: bool) -> Result<()> {
        let item = &self.items[at];
        for name in ["id", "call_id", "type", "name", "namespace", "role"] {
            if let (Some(previous), Some(incoming)) = (item.doc.member(item.doc.root(), name)?, doc.member(node, name)?)
                && !super::snapshot::equal(&item.doc, previous, doc, incoming)?
            {
                return Err(Error::Schema);
            }
        }
        if is(doc, node, "type", "image_generation_call")?
            && let Some(data) = doc.member(node, "result")?
            && !matches!(doc.node(data)?.value, Value::Null)
        {
            self.image(owner, at, doc, data, None)?;
        }
        for (name, summary) in [("content", false), ("summary", true)] {
            if let Some(parts) = doc.member(node, name)? {
                for (index, part) in doc.children(parts)?.enumerate() {
                    self.block(owner, at, (index as u64, summary), doc, part, (close, close))?;
                }
            }
        }
        let custom = self.items[at].custom()?;
        if self.items[at].server.is_some() {
            self.server_snapshot(owner, at, doc, node, close)?;
        } else if self.items[at].tool.is_some() {
            if let Some(arguments) = doc.member(node, if custom { "input" } else { "arguments" })? {
                self.arguments(owner, at, doc, arguments, close)?;
            } else if close && !self.items[at].tool_closed {
                return Err(Error::Truncated);
            }
        }
        if let Some(encrypted) = doc.member(node, "encrypted_content")? {
            string(doc, encrypted)?;
            let item = &mut self.items[at];
            if !item.encrypted {
                let mut chars = doc.chars(encrypted)?;
                let mut redacted = true;
                for c in super::REDACTED_PREFIX.chars() {
                    redacted &= chars.next_scalar()? == Some(c);
                }
                let index = u64::MAX;
                let scope = item.scope(Some(index));
                self.emitter.emit(
                    owner,
                    Kind::BlockStart,
                    scope,
                    |e| {
                        e.bytes(if redacted {
                            b",\"kind\":\"thinking\",\"attributes\":{\"type\":\"redacted\"}"
                        } else {
                            b",\"kind\":\"thinking\",\"attributes\":{\"type\":\"encrypted\"}"
                        })
                    },
                    |_| Ok(()),
                )?;
                self.emitter.emit(
                    owner,
                    if redacted {
                        Kind::ReasoningRedacted
                    } else {
                        Kind::ReasoningSignature
                    },
                    scope,
                    |e| {
                        if redacted {
                            e.bytes(b",\"data\":")?;
                            return e.quoted_json(|e| {
                                let mut chars = doc.chars(encrypted)?;
                                for _ in super::REDACTED_PREFIX.chars() {
                                    chars.next_scalar()?;
                                }
                                while let Some(c) = chars.next_scalar()? {
                                    e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
                                }
                                Ok(())
                            });
                        }
                        e.bytes(b",\"kind\":\"responses_encrypted\",\"fragment\":")?;
                        doc.write(encrypted, e)?;
                        e.bytes(b",\"final\":true")
                    },
                    |_| Ok(()),
                )?;
                self.emitter
                    .emit(owner, Kind::BlockEnd, scope, |_| Ok(()), |_| Ok(()))?;
                let lease = encode::scratch(owner, |e| doc.write(encrypted, e))?;
                let imported = item.doc.import_json(owner, lease)?;
                item.doc.rename(imported, "encrypted_content")?;
                item.doc.append(item.doc.root(), imported)?;
                item.encrypted = true;
            } else if !equal(
                &item.doc,
                item.doc.required(item.doc.root(), "encrypted_content")?,
                doc,
                encrypted,
            )? {
                return Err(Error::Schema);
            }
        }
        if close && !self.items[at].closed {
            let item = &mut self.items[at];
            for block in &item.blocks {
                if !block.closed {
                    return Err(Error::Truncated);
                }
            }
            self.emitter.emit(
                owner,
                Kind::ItemEnd,
                item.scope(None),
                |e| {
                    e.bytes(b",\"status\":")?;
                    e.string(if is(doc, node, "status", "incomplete")? {
                        "incomplete"
                    } else {
                        "completed"
                    })?;
                    e.bytes(b",\"attributes\":")?;
                    item_metadata(doc, node, e)
                },
                |_| Ok(()),
            )?;
            item.closed = true;
        }
        Ok(())
    }
    fn image(
        &mut self,
        owner: &mut Writer,
        at: usize,
        doc: &Document,
        data: usize,
        partial: Option<u64>,
    ) -> Result<()> {
        let item = &self.items[at];
        if !is(&item.doc, item.doc.root(), "type", "image_generation_call")? || (partial.is_some() && item.closed) {
            return Err(Error::Schema);
        }
        if partial.is_none()
            && let Some(previous) = self.items[at].image
        {
            return if previous == super::image::digest(doc, data)? {
                Ok(())
            } else {
                Err(Error::Schema)
            };
        }
        let id = media::local_id(b'm', self.next_media)?;
        self.next_media = self.next_media.checked_add(1).ok_or(Error::Limit)?;
        let digest = super::image::publish(
            owner,
            &mut self.emitter,
            doc,
            data,
            self.items[at].scope(None),
            id,
            partial,
        )?;
        if partial.is_none() {
            self.items[at].image = Some(digest);
        }
        Ok(())
    }
    fn complete(&mut self, owner: &mut Writer, doc: &Document, root: usize) -> Result<()> {
        if is(doc, root, "status", "failed")?
            || doc
                .member(root, "error")?
                .is_some_and(|n| !matches!(doc.node(n).map(|n| n.value), Ok(Value::Null)))
        {
            return Err(Error::Schema);
        }
        if let Some(status) = doc.member(root, "status")?
            && !doc.string_is(status, "completed")?
            && !doc.string_is(status, "incomplete")?
        {
            return Err(Error::Truncated);
        }
        self.start(owner, Some((doc, root)))?;
        let output = doc.required(root, "output")?;
        if !matches!(doc.node(output)?.value, Value::Array) {
            return Err(Error::Schema);
        }
        if let Some(native) = doc.member(root, "audio")? {
            if self.audio.is_none() {
                let id = media::local_id(b'm', self.next_media)?;
                self.next_media = self.next_media.checked_add(1).ok_or(Error::Limit)?;
                self.audio = Some(super::audio::Ingress::start(
                    owner,
                    &mut self.emitter,
                    id,
                    Some((doc, native)),
                )?);
            }
            self.audio
                .as_mut()
                .ok_or(Error::Schema)?
                .snapshot(owner, &mut self.emitter, doc, native)?;
        }
        if let Some(audio) = &mut self.audio {
            audio.finish(owner, &mut self.emitter)?;
        }
        if self.operation == Operation::Compact {
            for item in doc.children(output)? {
                self.emitter.emit(
                    owner,
                    Kind::Compact,
                    Scope::default(),
                    |e| {
                        e.bytes(b",\"item\":")?;
                        doc.write(item, e)
                    },
                    |_| Ok(()),
                )?;
            }
        } else {
            for (index, item) in doc.children(output)?.enumerate() {
                let at = self.find_or_add(owner, doc, item, index as u64)?;
                self.snapshot_item(owner, at, doc, item, true)?;
            }
            if self.items.iter().any(|i| !i.closed) || self.items.len() != doc.children(output)?.count() {
                return Err(Error::Truncated);
            }
            let base = self.base.as_ref().ok_or(Error::Schema)?;
            self.emitter.emit(
                owner,
                Kind::Chunk,
                Scope {
                    choice_index: Some(0),
                    ..Scope::default()
                },
                |e| {
                    chunk_base(base, e)?;
                    e.bytes(b",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":")?;
                    e.string(if is(doc, root, "status", "incomplete")? {
                        "length"
                    } else {
                        "stop"
                    })?;
                    e.bytes(b"}]")
                },
                |e| {
                    e.bytes(b",\"native_finish_reason\":")?;
                    if let Some(n) = doc.member(root, "incomplete_details")? {
                        doc.write(n, e)
                    } else if let Some(n) = doc.member(root, "status")? {
                        doc.write(n, e)
                    } else {
                        e.string("completed")
                    }
                },
            )?;
        }
        if let Some(usage) = doc.member(root, "usage")? {
            self.usage.update(owner, doc, usage)?;
        }
        if self.usage.seen() {
            self.emitter.emit(
                owner,
                Kind::Usage,
                Scope::default(),
                |e| {
                    e.bytes(b",\"usage\":")?;
                    self.usage.write(e, true)
                },
                |_| Ok(()),
            )?;
        }
        self.emitter.emit(
            owner,
            Kind::Opaque,
            Scope::default(),
            |e| {
                e.bytes(b",\"name\":\"responses.response\",\"format\":\"json\",\"value\":")?;
                write_except(doc, root, e, &["output", "usage", "audio"])
            },
            |_| Ok(()),
        )?;
        self.emitter
            .emit(owner, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(()))?;
        self.terminal = true;
        Ok(())
    }
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
pub(super) fn reserve<T>(values: &mut Vec<T>, metadata: &mut MetadataPermit) -> Result<()> {
    if values.len() == values.capacity() {
        let capacity = values.capacity().max(4).checked_mul(2).ok_or(Error::Limit)?;
        metadata.resize(capacity.checked_mul(std::mem::size_of::<T>()).ok_or(Error::Limit)?)?;
        values
            .try_reserve_exact(capacity - values.len())
            .map_err(|_| Error::Limit)?;
        metadata.resize(values.capacity() * std::mem::size_of::<T>())?;
    }
    Ok(())
}
pub(super) fn write_except(doc: &Document, node: usize, e: &mut Encoder<'_, '_>, skip: &[&str]) -> Result<()> {
    e.bytes(b"{")?;
    let mut first = true;
    for field in doc.children(node)? {
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        let mut skipped = false;
        for s in skip {
            skipped |= doc.name_is(name, s)?;
        }
        if skipped {
            continue;
        }
        if !first {
            e.bytes(b",")?;
        }
        first = false;
        doc.write_name(name, e)?;
        e.bytes(b":")?;
        doc.write(field, e)?;
    }
    e.bytes(b"}")
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

fn item_metadata(doc: &Document, node: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let fields: &[&str] = if let Some(server) = Server::from_item(doc, node)? {
        match server {
            Server::Code => &[
                "content",
                "summary",
                "arguments",
                "input",
                "encrypted_content",
                "outputs",
                "code",
            ],
            Server::Web => &[
                "content",
                "summary",
                "arguments",
                "input",
                "encrypted_content",
                "action",
                "results",
            ],
            Server::File => &[
                "content",
                "summary",
                "arguments",
                "input",
                "encrypted_content",
                "results",
            ],
            Server::Mcp => &[
                "content",
                "summary",
                "arguments",
                "input",
                "encrypted_content",
                "output",
                "error",
            ],
        }
    } else if is(doc, node, "type", "image_generation_call")? {
        &[
            "content",
            "summary",
            "arguments",
            "input",
            "encrypted_content",
            "result",
        ]
    } else {
        &["content", "summary", "arguments", "input", "encrypted_content"]
    };
    write_except(doc, node, e, fields)
}
