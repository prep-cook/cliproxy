use super::{Table, usage::Usage};
use crate::codec::{
    self, BodyMode, Error, Operation, Result,
    document::{Document, Value},
    encode::{self, Encoder},
    event::{Emitter, Identity, Kind, Scope},
    json, media,
    source::Source,
    sse,
};
use ck_stream::Writer;

pub struct Ingress {
    owner: u64,
    source: Option<Source>,
    sse: Option<sse::Scanner>,
    json: Option<json::Scanner>,
    state: State,
    failed: bool,
}
struct Block {
    index: u64,
    scope: Scope,
    kind: &'static str,
    open: bool,
    arguments: Option<(Source, json::Scanner)>,
    initial: Option<Document>,
    fragments: bool,
    source_id: Option<Document>,
}
struct State {
    emitter: Emitter,
    identity: Identity,
    operation: Operation,
    base: Option<Document>,
    blocks: Table<Block>,
    next_tool: u64,
    usage: Usage,
    message_started: bool,
    finished: bool,
    terminal: bool,
}
impl Ingress {
    pub fn new(
        owner: &mut Writer,
        identity: Identity,
        operation: Operation,
        mode: BodyMode,
        limit: u64,
    ) -> Result<Self> {
        if operation == Operation::Compact || (operation == Operation::CountTokens && mode == BodyMode::Streaming) {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            owner: owner.id(),
            source: Some(Source::new(owner)?),
            sse: if mode == BodyMode::Streaming {
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
                blocks: Table::new(owner)?,
                next_tool: 0,
                usage: Usage::default(),
                message_started: false,
                finished: false,
                terminal: false,
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
            if let Some(scanner) = &mut self.sse {
                scanner.advance(source, |event| self.state.event(owner, source, event))?;
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
            if let Some(scanner) = &mut self.sse {
                let source = self.source.as_ref().ok_or(Error::Closed)?;
                scanner.finish(source, |event| self.state.event(owner, source, event))?;
                if !self.state.terminal {
                    return Err(Error::Truncated);
                }
            } else {
                let source = self.source.take().ok_or(Error::Closed)?;
                let mut scanner = self.json.take().ok_or(Error::Closed)?;
                scanner.finish(&source)?;
                let doc = Document::from_scanned(owner, source, scanner)?;
                self.state.buffered(owner, &doc)?;
            }
            Ok(())
        })();
        self.handle(owner, result)
    }
}
impl State {
    fn scope(&self) -> Result<Scope> {
        Ok(Scope {
            choice_index: Some(0),
            item_id: Some(media::local_id(b'i', 0)?),
            ..Scope::default()
        })
    }
    fn start(&mut self, owner: &mut Writer, doc: Option<(&Document, usize)>) -> Result<()> {
        if self.base.is_some() {
            return Ok(());
        }
        let base = super::mapped(owner, |e| {
            e.bytes(b"{\"id\":")?;
            if let Some((d, n)) = doc {
                string_or(d, n, "id", self.identity.stream_id.as_str(), e)?;
            } else {
                e.string(self.identity.stream_id.as_str())?;
            }
            e.bytes(b",\"model\":")?;
            if let Some((d, n)) = doc {
                string_or(d, n, "model", "", e)?;
            } else {
                e.string("")?;
            }
            e.bytes(b",\"created\":0}")
        })?;
        self.emitter.emit(
            owner,
            Kind::ResponseStart,
            Scope::default(),
            |e| {
                for key in ["id", "model", "created"] {
                    super::member(&base, base.root(), key, e)?;
                }
                Ok(())
            },
            |e| {
                e.bytes(b",\"source\":{\"protocol\":\"anthropic\"},\"synthetic\":[\"created\"]")?;
                if let Some((d, n)) = doc {
                    super::unknown(
                        d,
                        n,
                        &[
                            "id",
                            "model",
                            "type",
                            "role",
                            "content",
                            "usage",
                            "stop_reason",
                            "stop_sequence",
                        ],
                        e,
                    )?;
                }
                Ok(())
            },
        )?;
        self.base = Some(base);
        if self.operation == Operation::Generate {
            self.emitter.emit(
                owner,
                Kind::ItemStart,
                self.scope()?,
                |e| e.bytes(b",\"kind\":\"message\",\"role\":\"assistant\""),
                |_| Ok(()),
            )?;
        }
        Ok(())
    }
    fn event(&mut self, owner: &mut Writer, source: &Source, event: &sse::Event) -> Result<()> {
        if self.terminal {
            return Err(Error::Closed);
        }
        if !event.has_data(source)? {
            // Keep source metadata without making a heartbeat client-deliverable.
            self.start(owner, None)?;
            self.emitter.emit(
                owner,
                Kind::Heartbeat,
                Scope::default(),
                |_| Ok(()),
                |e| provenance(source, event, e),
            )?;
            return Ok(());
        }
        let raw = encode::scratch(owner, |e| event.write_data(source, |b| e.bytes(b)))?;
        let mut data = Source::new(owner)?;
        data.push_scratch(raw)?;
        let len = data.len();
        let name = event.name(source)?;
        let recognized = [
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
            "error",
            "ping",
        ];
        let named_known = match name {
            Some(n) => recognized.iter().try_fold(false, |yes, key| {
                Ok::<_, Error>(yes || source.equals(n, key.as_bytes())?)
            })?,
            None => true,
        };
        let doc = match Document::parse(owner, data, len) {
            Ok(doc) => doc,
            Err(Error::Json | Error::Truncated) if !named_known => {
                self.start(owner, None)?;
                self.emitter.emit(
                    owner,
                    Kind::Opaque,
                    Scope::default(),
                    |e| {
                        e.bytes(b",\"format\":\"text\",\"value\":")?;
                        e.quoted_json(|e| event.write_data(source, |b| e.bytes(b)))
                    },
                    |e| provenance(source, event, e),
                )?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let root = doc.root();
        if !named_known {
            self.start(owner, None)?;
            self.emitter.emit(
                owner,
                Kind::Opaque,
                Scope::default(),
                |e| {
                    e.bytes(b",\"format\":\"json\",\"value\":")?;
                    doc.write(root, e)
                },
                |e| provenance(source, event, e),
            )?;
            return Ok(());
        }
        let kind = doc.required(root, "type")?;
        if let Some(name) = name {
            let mut chars = doc.chars(kind)?;
            let mut bytes = source.cursor(name)?;
            while let Some(c) = chars.next_scalar()? {
                if !c.is_ascii() || bytes.next_byte()? != Some(c as u8) {
                    return Err(Error::Schema);
                }
            }
            if bytes.next_byte()?.is_some() {
                return Err(Error::Schema);
            }
        }
        if doc.string_is(kind, "error")? {
            return Err(Error::Schema);
        }
        if doc.string_is(kind, "ping")? {
            self.start(owner, None)?;
            self.emitter.emit(
                owner,
                Kind::Heartbeat,
                Scope::default(),
                |_| Ok(()),
                |e| provenance(source, event, e),
            )?;
        } else if doc.string_is(kind, "message_start")? {
            if self.message_started {
                return Err(Error::Schema);
            }
            self.message_started = true;
            let message = doc.required(root, "message")?;
            if let Some(role) = doc.member(message, "role")?
                && !doc.string_is(role, "assistant")?
            {
                return Err(Error::Schema);
            }
            // A leading heartbeat may have created synthetic identity; replace it with supplied metadata.
            if self.base.is_some() {
                if self.blocks.rows.iter().any(|b| b.open) || self.finished {
                    return Err(Error::Schema);
                }
                self.base = Some(super::mapped(owner, |e| {
                    e.bytes(b"{\"id\":")?;
                    string_or(&doc, message, "id", self.identity.stream_id.as_str(), e)?;
                    e.bytes(b",\"model\":")?;
                    string_or(&doc, message, "model", "", e)?;
                    e.bytes(b",\"created\":0}")
                })?);
            } else {
                self.start(owner, Some((&doc, message)))?;
            }
            let base = self.base.as_ref().ok_or(Error::Schema)?;
            self.emitter.emit(
                owner,
                Kind::Chunk,
                self.scope()?,
                |e| {
                    base_fields(base, e)?;
                    e.bytes(b",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]")
                },
                |e| provenance(source, event, e),
            )?;
            if let Some(content) = doc.member(message, "content")? {
                if !matches!(doc.node(content)?.value, Value::Array) {
                    return Err(Error::Schema);
                }
                for (index, block) in doc.children(content)?.enumerate() {
                    self.block_start(owner, &doc, block, index as u64, true)?;
                    self.block_stop(owner, index as u64)?;
                }
            }
            self.update_usage(owner, &doc, message)?;
        } else if doc.string_is(kind, "content_block_start")? {
            self.require_active()?;
            self.block_start(
                owner,
                &doc,
                doc.required(root, "content_block")?,
                doc.unsigned(doc.required(root, "index")?)?,
                false,
            )?;
        } else if doc.string_is(kind, "content_block_delta")? {
            self.require_active()?;
            self.delta(
                owner,
                &doc,
                doc.required(root, "delta")?,
                doc.unsigned(doc.required(root, "index")?)?,
            )?;
        } else if doc.string_is(kind, "content_block_stop")? {
            self.require_active()?;
            self.block_stop(owner, doc.unsigned(doc.required(root, "index")?)?)?;
        } else if doc.string_is(kind, "message_delta")? {
            self.update_usage(owner, &doc, root)?;
            let delta = doc.required(root, "delta")?;
            if let Some(reason) = doc.member(delta, "stop_reason")?
                && !matches!(doc.node(reason)?.value, Value::Null)
            {
                self.finish_choice(owner, &doc, delta)?;
            }
        } else if doc.string_is(kind, "message_stop")? {
            self.done(owner)?;
        } else {
            self.start(owner, None)?;
            self.emitter.emit(
                owner,
                Kind::Opaque,
                Scope::default(),
                |e| {
                    e.bytes(b",\"format\":\"json\",\"value\":")?;
                    doc.write(root, e)
                },
                |e| provenance(source, event, e),
            )?;
        }
        Ok(())
    }
    fn require_active(&self) -> Result<()> {
        if !self.message_started || self.finished {
            Err(Error::Schema)
        } else {
            Ok(())
        }
    }
    fn buffered(&mut self, owner: &mut Writer, doc: &Document) -> Result<()> {
        let root = doc.root();
        if doc.member(root, "error")?.is_some() || super::is(doc, root, "type", "error")? {
            return Err(Error::Schema);
        }
        self.message_started = true;
        self.start(
            owner,
            if self.operation == Operation::CountTokens {
                None
            } else {
                Some((doc, root))
            },
        )?;
        if self.operation == Operation::CountTokens {
            let count = doc.required(root, "input_tokens")?;
            doc.unsigned(count)?;
            self.emitter.emit(
                owner,
                Kind::CountTokens,
                Scope::default(),
                |e| {
                    e.bytes(b",\"input_tokens\":")?;
                    doc.write(count, e)
                },
                |e| super::unknown(doc, root, &["input_tokens"], e),
            )?;
            self.finished = true;
        } else {
            let content = doc.required(root, "content")?;
            if !matches!(doc.node(content)?.value, Value::Array) {
                return Err(Error::Schema);
            }
            for (index, block) in doc.children(content)?.enumerate() {
                self.block_start(owner, doc, block, index as u64, true)?;
                self.block_stop(owner, index as u64)?;
            }
            self.update_usage(owner, doc, root)?;
            self.finish_choice(owner, doc, root)?;
        }
        self.done(owner)
    }
    fn block_start(&mut self, owner: &mut Writer, doc: &Document, node: usize, index: u64, whole: bool) -> Result<()> {
        if self.blocks.rows.iter().any(|b| b.index == index) {
            return Err(Error::Schema);
        }
        let kind = if super::is(doc, node, "type", "text")? {
            "text"
        } else if super::is(doc, node, "type", "thinking")? {
            "thinking"
        } else if super::is(doc, node, "type", "redacted_thinking")? {
            "redacted"
        } else if super::is(doc, node, "type", "tool_use")? {
            "function"
        } else if super::is(doc, node, "type", "server_tool_use")? || super::is(doc, node, "type", "mcp_tool_use")? {
            "server"
        } else if super::server_result(doc, node)? {
            "tool_result"
        } else {
            "opaque"
        };
        let tool = matches!(kind, "function" | "server");
        let result_scope = if kind == "tool_result" {
            let id = doc.required(node, "tool_use_id")?;
            let mut matched = None;
            for block in &self.blocks.rows {
                if let Some(source) = &block.source_id
                    && super::equal_text(source, source.root(), doc, id)?
                {
                    matched = Some(block.scope);
                    break;
                }
            }
            Some(matched.ok_or(Error::Schema)?)
        } else {
            None
        };
        let scope = Scope {
            block_index: Some(index),
            tool_index: if tool {
                Some(self.next_tool)
            } else {
                result_scope.and_then(|s| s.tool_index)
            },
            tool_call_id: if tool {
                Some(media::local_id(b't', self.next_tool)?)
            } else {
                result_scope.and_then(|s| s.tool_call_id)
            },
            ..self.scope()?
        };
        if tool {
            self.next_tool += 1;
        }
        self.emitter.emit(
            owner,
            Kind::BlockStart,
            scope,
            |e| {
                e.bytes(b",\"kind\":")?;
                e.string(kind)?;
                e.bytes(b",\"attributes\":")?;
                super::object(
                    doc,
                    node,
                    if kind == "tool_result" {
                        &["content", "tool_use_id", "is_error"]
                    } else {
                        &["text", "thinking", "signature", "data", "input", "id", "name"]
                    },
                    e,
                )
            },
            |e| e.bytes(b",\"source\":{\"protocol\":\"anthropic\"}"),
        )?;
        if tool {
            let name = doc.required(node, "name")?;
            super::string(doc, name)?;
            let id = doc.required(node, "id")?;
            super::string(doc, id)?;
            self.emitter.emit(
                owner,
                Kind::ToolStart,
                scope,
                |e| {
                    e.bytes(b",\"kind\":")?;
                    e.string(if kind == "server" { "server" } else { "function" })?;
                    e.bytes(b",\"arguments_format\":\"json\",\"name\":")?;
                    doc.write(name, e)
                },
                |e| {
                    e.bytes(b",\"source\":{\"protocol\":\"anthropic\",\"ids\":{\"id\":")?;
                    doc.write(id, e)?;
                    e.bytes(b"}}")
                },
            )?;
        }
        let initial = if tool {
            doc.member(node, "input")?
                .map(|n| super::mapped(owner, |e| doc.write(n, e)))
                .transpose()?
        } else {
            None
        };
        self.blocks.push(Block {
            index,
            scope,
            kind,
            open: true,
            arguments: if tool {
                Some((Source::new(owner)?, json::Scanner::new(owner, u64::MAX)?))
            } else {
                None
            },
            initial,
            fragments: false,
            source_id: if tool {
                Some(super::mapped(owner, |e| doc.write(doc.required(node, "id")?, e))?)
            } else {
                None
            },
        })?;
        for (field, target) in [("text", "content"), ("thinking", "reasoning_content")] {
            if let Some(value) = doc.member(node, field)? {
                self.chunk(owner, scope, doc, value, target)?;
            }
        }
        if let Some(signature) = doc.member(node, "signature")? {
            self.signature(owner, scope, doc, signature, true)?;
        }
        if kind == "redacted" {
            let data = doc.required(node, "data")?;
            super::string(doc, data)?;
            self.emitter.emit(
                owner,
                Kind::ReasoningRedacted,
                scope,
                |e| {
                    e.bytes(b",\"data\":")?;
                    doc.write(data, e)
                },
                |_| Ok(()),
            )?;
        }
        if kind == "tool_result" {
            self.emitter.emit(
                owner,
                Kind::ToolResult,
                scope,
                |e| {
                    e.bytes(b",\"value\":")?;
                    doc.write(doc.required(node, "content")?, e)?;
                    super::member(doc, node, "is_error", e)
                },
                |e| {
                    e.bytes(b",\"source\":{\"protocol\":\"anthropic\",\"ids\":{\"tool_use_id\":")?;
                    doc.write(doc.required(node, "tool_use_id")?, e)?;
                    e.bytes(b"}}")
                },
            )?;
        }
        if kind == "opaque" {
            self.emitter.emit(
                owner,
                Kind::Opaque,
                scope,
                |e| {
                    e.bytes(b",\"format\":\"json\",\"value\":")?;
                    doc.write(node, e)
                },
                |e| e.bytes(b",\"source\":{\"protocol\":\"anthropic\",\"event\":\"content_block\"}"),
            )?;
        }
        if let Some(citations) = doc.member(node, "citations")? {
            self.emitter.emit(
                owner,
                Kind::Annotation,
                scope,
                |e| {
                    e.bytes(b",\"kind\":\"citations\",\"value\":")?;
                    doc.write(citations, e)
                },
                |_| Ok(()),
            )?;
        }
        if whole && tool {
            let input = doc.member(node, "input")?;
            if let Some(input) = input {
                self.arguments(owner, index, doc, input, false)?;
            }
        }
        Ok(())
    }
    fn delta(&mut self, owner: &mut Writer, doc: &Document, delta: usize, index: u64) -> Result<()> {
        let block = self
            .blocks
            .rows
            .iter()
            .find(|b| b.index == index && b.open)
            .ok_or(Error::Schema)?;
        let scope = block.scope;
        if super::is(doc, delta, "type", "text_delta")? && block.kind == "text" {
            self.chunk(owner, scope, doc, doc.required(delta, "text")?, "content")
        } else if super::is(doc, delta, "type", "thinking_delta")? && block.kind == "thinking" {
            self.chunk(owner, scope, doc, doc.required(delta, "thinking")?, "reasoning_content")
        } else if super::is(doc, delta, "type", "signature_delta")? && block.kind == "thinking" {
            self.signature(owner, scope, doc, doc.required(delta, "signature")?, false)
        } else if super::is(doc, delta, "type", "input_json_delta")? && block.arguments.is_some() {
            self.arguments(owner, index, doc, doc.required(delta, "partial_json")?, true)
        } else if super::is(doc, delta, "type", "citations_delta")? {
            self.emitter.emit(
                owner,
                Kind::Annotation,
                scope,
                |e| {
                    e.bytes(b",\"kind\":\"citation\",\"value\":")?;
                    doc.write(doc.required(delta, "citation")?, e)
                },
                |_| Ok(()),
            )?;
            Ok(())
        } else {
            Err(Error::Schema)
        }
    }
    fn chunk(
        &mut self,
        owner: &mut Writer,
        scope: Scope,
        doc: &Document,
        value: usize,
        field: &'static str,
    ) -> Result<()> {
        super::string(doc, value)?;
        let base = self.base.as_ref().ok_or(Error::Schema)?;
        self.emitter.emit(
            owner,
            Kind::Chunk,
            scope,
            |e| {
                base_fields(base, e)?;
                e.bytes(b",\"choices\":[{\"index\":0,\"delta\":{")?;
                e.string(field)?;
                e.bytes(b":")?;
                doc.write(value, e)?;
                e.bytes(b"},\"finish_reason\":null}]")
            },
            |_| Ok(()),
        )?;
        Ok(())
    }
    fn signature(
        &mut self,
        owner: &mut Writer,
        scope: Scope,
        doc: &Document,
        value: usize,
        final_fragment: bool,
    ) -> Result<()> {
        super::string(doc, value)?;
        self.emitter.emit(
            owner,
            Kind::ReasoningSignature,
            scope,
            |e| {
                e.bytes(b",\"kind\":\"anthropic\",\"fragment\":")?;
                doc.write(value, e)?;
                e.bytes(if final_fragment {
                    b",\"final\":true"
                } else {
                    b",\"final\":false"
                })
            },
            |_| Ok(()),
        )?;
        Ok(())
    }
    fn arguments(
        &mut self,
        owner: &mut Writer,
        index: u64,
        doc: &Document,
        value: usize,
        fragment: bool,
    ) -> Result<()> {
        let block = self
            .blocks
            .rows
            .iter_mut()
            .find(|b| b.index == index && b.open)
            .ok_or(Error::Schema)?;
        let (source, parser) = block.arguments.as_mut().ok_or(Error::Schema)?;
        let lease = encode::scratch(owner, |e| {
            if fragment {
                let mut chars = doc.chars(value)?;
                while let Some(c) = chars.next_scalar()? {
                    e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
                }
                Ok(())
            } else {
                doc.write(value, e)
            }
        })?;
        source.push_scratch(lease)?;
        parser.advance(source)?;
        block.fragments = true;
        block.initial = None;
        let scope = block.scope;
        let base = self.base.as_ref().ok_or(Error::Schema)?;
        self.emitter.emit(
            owner,
            Kind::Chunk,
            scope,
            |e| {
                base_fields(base, e)?;
                e.bytes(b",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":")?;
                e.unsigned(scope.tool_index.ok_or(Error::Schema)?)?;
                e.bytes(b",\"function\":{\"arguments\":")?;
                if fragment {
                    doc.write(value, e)?;
                } else {
                    e.quoted_json(|e| doc.write(value, e))?;
                }
                e.bytes(b"}}]},\"finish_reason\":null}]")
            },
            |_| Ok(()),
        )?;
        Ok(())
    }
    fn block_stop(&mut self, owner: &mut Writer, index: u64) -> Result<()> {
        let position = self
            .blocks
            .rows
            .iter()
            .position(|b| b.index == index && b.open)
            .ok_or(Error::Schema)?;
        if !self.blocks.rows[position].fragments
            && let Some(initial) = self.blocks.rows[position].initial.take()
        {
            self.arguments(owner, index, &initial, initial.root(), false)?;
        }
        let block = &mut self.blocks.rows[position];
        if let Some((source, parser)) = &mut block.arguments {
            if block.fragments {
                parser.finish(source)?;
            }
            self.emitter.emit(
                owner,
                Kind::ToolEnd,
                block.scope,
                |e| {
                    e.bytes(if block.fragments {
                        b",\"status\":\"completed\",\"valid_json\":true"
                    } else {
                        b",\"status\":\"completed\""
                    })
                },
                |_| Ok(()),
            )?;
        }
        self.emitter
            .emit(owner, Kind::BlockEnd, block.scope, |_| Ok(()), |_| Ok(()))?;
        block.open = false;
        block.arguments = None;
        block.initial = None;
        Ok(())
    }
    fn update_usage(&mut self, owner: &mut Writer, doc: &Document, parent: usize) -> Result<()> {
        if let Some(usage) = doc.member(parent, "usage")? {
            self.usage.update(owner, doc, usage)?;
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
        Ok(())
    }
    fn finish_choice(&mut self, owner: &mut Writer, doc: &Document, parent: usize) -> Result<()> {
        self.require_active()?;
        if self.blocks.rows.iter().any(|b| b.open) {
            return Err(Error::Truncated);
        }
        let reason = doc.required(parent, "stop_reason")?;
        super::string(doc, reason)?;
        let mapped = if doc.string_is(reason, "max_tokens")? {
            "length"
        } else if doc.string_is(reason, "tool_use")? {
            "tool_calls"
        } else if doc.string_is(reason, "refusal")? {
            "content_filter"
        } else {
            "stop"
        };
        self.emitter.emit(
            owner,
            Kind::ItemEnd,
            self.scope()?,
            |e| e.bytes(b",\"status\":\"completed\""),
            |_| Ok(()),
        )?;
        let base = self.base.as_ref().ok_or(Error::Schema)?;
        self.emitter.emit(
            owner,
            Kind::Chunk,
            self.scope()?,
            |e| {
                base_fields(base, e)?;
                e.bytes(b",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":")?;
                e.string(mapped)?;
                e.bytes(b"}]")
            },
            |e| {
                e.bytes(b",\"native_finish_reason\":")?;
                doc.write(reason, e)?;
                e.bytes(b",\"source\":{\"protocol\":\"anthropic\"}")?;
                if parent == doc.root() {
                    if let Some(sequence) = doc.member(parent, "stop_sequence")? {
                        e.bytes(
                            b",\"original\":[{\"protocol\":\"anthropic\",\"pointer\":\"/stop_sequence\",\"value\":",
                        )?;
                        doc.write(sequence, e)?;
                        e.bytes(b"}]")?;
                    }
                    Ok(())
                } else {
                    super::unknown(doc, parent, &["stop_reason"], e)
                }
            },
        )?;
        self.finished = true;
        Ok(())
    }
    fn done(&mut self, owner: &mut Writer) -> Result<()> {
        if self.base.is_none() || !self.finished || self.blocks.rows.iter().any(|b| b.open) {
            return Err(Error::Truncated);
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
        self.emitter
            .emit(owner, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(()))?;
        self.terminal = true;
        Ok(())
    }
}
fn string_or(doc: &Document, parent: usize, key: &str, default: &str, e: &mut Encoder<'_, '_>) -> Result<()> {
    if let Some(node) = doc.member(parent, key)? {
        super::string(doc, node)?;
        doc.write(node, e)
    } else {
        e.string(default)
    }
}
fn base_fields(doc: &Document, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b",\"object\":\"chat.completion.chunk\"")?;
    for key in ["id", "model", "created"] {
        super::member(doc, doc.root(), key, e)?;
    }
    Ok(())
}
fn provenance(source: &Source, event: &sse::Event, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b",\"source\":{\"protocol\":\"anthropic\"")?;
    if let Some(name) = event.name(source)? {
        e.bytes(b",\"event\":")?;
        e.quoted_json(|e| e.span(source, name))?;
    }
    e.bytes(b",\"sse\":[")?;
    let mut first = true;
    for field in event.fields() {
        if source.equals(field.name, b"data")? || source.equals(field.name, b"event")? {
            continue;
        }
        if !first {
            e.bytes(b",")?;
        }
        first = false;
        e.bytes(b"{\"name\":")?;
        e.quoted_json(|e| e.span(source, field.name))?;
        e.bytes(b",\"value\":")?;
        e.quoted_json(|e| e.span(source, field.value))?;
        e.bytes(if field.comment {
            b",\"comment\":true}"
        } else {
            b",\"comment\":false}"
        })?;
    }
    e.bytes(b"]}")
}
