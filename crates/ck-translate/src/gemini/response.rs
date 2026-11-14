use super::Table;
use crate::codec::{
    self, BodyMode, Error, Operation, Result,
    document::{Document, Value},
    encode::Encoder,
    event::{Emitter, Identity, Kind, Scope},
    json,
    media::{self, Encoding, Media},
    source::Source,
    sse,
};
use ck_stream::Writer;

/// Native streamGenerateContent supports SSE and a streamed JSON array.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Framing {
    Json,
    Sse,
    JsonArray,
}
pub struct Ingress {
    owner: u64,
    source: Option<Source>,
    json: Option<json::Scanner>,
    sse: Option<sse::Scanner>,
    framing: Framing,
    last_array: Option<usize>,
    state: State,
    closed: bool,
}
struct Choice {
    index: u64,
    finished: bool,
    next_part: u64,
    next_tool: u64,
}
struct State {
    emitter: Emitter,
    identity: Identity,
    operation: Operation,
    base: Option<Document>,
    usage: Option<Document>,
    choices: Table<Choice>,
    next_item: u64,
    blocked: bool,
}
impl Ingress {
    pub fn new(
        owner: &mut Writer,
        identity: Identity,
        operation: Operation,
        mode: BodyMode,
        limit: u64,
    ) -> Result<Self> {
        Self::with_framing(
            owner,
            identity,
            operation,
            if mode == BodyMode::Buffered {
                Framing::Json
            } else {
                Framing::Sse
            },
            limit,
        )
    }
    pub fn with_framing(
        owner: &mut Writer,
        identity: Identity,
        operation: Operation,
        framing: Framing,
        limit: u64,
    ) -> Result<Self> {
        if operation == Operation::Compact || (operation == Operation::CountTokens && framing != Framing::Json) {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            owner: owner.id(),
            source: Some(Source::new(owner)?),
            json: if framing != Framing::Sse {
                Some(json::Scanner::new(owner, limit)?)
            } else {
                None
            },
            sse: if framing == Framing::Sse {
                Some(sse::Scanner::new(owner, limit)?)
            } else {
                None
            },
            framing,
            last_array: None,
            state: State {
                emitter: Emitter::new(identity),
                identity,
                operation,
                base: None,
                usage: None,
                choices: Table::new(owner)?,
                next_item: 0,
                blocked: false,
            },
            closed: false,
        })
    }
    fn result(&mut self, owner: &mut Writer, result: Result<()>) -> Result<()> {
        self.closed = result.is_err();
        crate::boundary::reported(result, &mut self.state.emitter, owner, false)
    }
    fn array_prefix(&mut self, owner: &mut Writer) -> Result<()> {
        let source = self.source.as_ref().ok_or(Error::Closed)?;
        let scanner = self.json.as_ref().ok_or(Error::Closed)?;
        if scanner.nodes().is_empty() {
            return Ok(());
        }
        let root = scanner.pending_node(source, 0)?;
        if root.kind != json::Kind::Array {
            return Err(Error::Schema);
        }
        let mut next = match self.last_array {
            Some(n) => scanner.pending_node(source, n)?.next,
            None => root.first,
        };
        while let Some(n) = next {
            let Some(node) = scanner.completed_node(source, n)? else {
                break;
            };
            let doc = super::mapped(owner, |e| e.span(source, node.span))?;
            self.state.body(owner, &doc)?;
            self.last_array = Some(n);
            next = node.next;
        }
        Ok(())
    }
}
impl codec::ResponseIngress for Ingress {
    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> Result<()> {
        crate::boundary::enter(self.owner, owner, self.closed)?;
        let result = (|| {
            let source = self.source.as_mut().ok_or(Error::Closed)?;
            source.push_bytes(owner, bytes)?;
            if let Some(scanner) = &mut self.sse {
                scanner.advance(source, |event| self.state.event(owner, source, event))?;
                source.release_before(scanner.consumed())?;
            } else {
                self.json.as_mut().ok_or(Error::Closed)?.advance(source)?;
                if self.framing == Framing::JsonArray {
                    self.array_prefix(owner)?;
                }
            }
            Ok(())
        })();
        self.result(owner, result)
    }
    fn finish(&mut self, owner: &mut Writer) -> Result<()> {
        crate::boundary::enter(self.owner, owner, self.closed)?;
        let result = (|| {
            if let Some(scanner) = &mut self.sse {
                let source = self.source.as_ref().ok_or(Error::Closed)?;
                scanner.finish(source, |event| self.state.event(owner, source, event))?;
            } else {
                self.json
                    .as_mut()
                    .ok_or(Error::Closed)?
                    .finish(self.source.as_ref().ok_or(Error::Closed)?)?;
                if self.framing == Framing::JsonArray {
                    self.array_prefix(owner)?;
                } else {
                    let doc = Document::from_scanned(
                        owner,
                        self.source.take().ok_or(Error::Closed)?,
                        self.json.take().ok_or(Error::Closed)?,
                    )?;
                    self.state.body(owner, &doc)?;
                }
            }
            if self.state.base.is_none()
                || (self.state.operation == Operation::Generate
                    && !self.state.blocked
                    && (self.state.choices.is_empty() || self.state.choices.iter().any(|c| !c.finished)))
            {
                return Err(Error::Truncated);
            }
            if let Some(usage) = &self.state.usage {
                emit_usage(&mut self.state.emitter, owner, usage, true)?;
            }
            self.state
                .emitter
                .emit(owner, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(()))?;
            Ok(())
        })();
        self.result(owner, result)?;
        self.closed = true;
        Ok(())
    }
}
impl State {
    fn event(&mut self, owner: &mut Writer, source: &Source, event: &sse::Event) -> Result<()> {
        if !event.has_data(source)? {
            if self.base.is_none() {
                return Ok(());
            }
            self.emitter.emit(
                owner,
                Kind::Heartbeat,
                Scope::default(),
                |_| Ok(()),
                |e| sse_source(source, event, e),
            )?;
            return Ok(());
        }
        let doc = super::mapped(owner, |e| event.write_data(source, |bytes| e.bytes(bytes)))?;
        self.body(owner, &doc)?;
        if event
            .fields()
            .iter()
            .any(|f| f.comment || !source.equals(f.name, b"data").unwrap_or(false))
        {
            self.emitter.emit(
                owner,
                Kind::Opaque,
                Scope::default(),
                |e| e.bytes(b",\"name\":\"gemini.sse\",\"format\":\"json\",\"value\":{}"),
                |e| sse_source(source, event, e),
            )?;
        }
        Ok(())
    }
    fn body(&mut self, owner: &mut Writer, doc: &Document) -> Result<()> {
        let root = doc.root();
        super::object(doc, root)?;
        if doc.member(root, "error")?.is_some() {
            return Err(Error::Schema);
        }
        if self.base.is_none() {
            let base = super::mapped(owner, |e| {
                e.bytes(b"{\"id\":")?;
                if let Some(n) = doc.member(root, "responseId")? {
                    super::string(doc, n)?;
                    doc.write(n, e)?;
                } else {
                    e.string(self.identity.stream_id.as_str())?;
                }
                e.bytes(b",\"model\":")?;
                if let Some(n) = doc.member(root, "modelVersion")? {
                    super::string(doc, n)?;
                    doc.write(n, e)?;
                } else {
                    e.string("")?;
                }
                e.bytes(b",\"created\":0}")
            })?;
            self.emitter.emit(
                owner,
                Kind::ResponseStart,
                Scope::default(),
                |e| super::members(&base, base.root(), &[], e),
                |e| {
                    e.bytes(b",\"source\":{\"protocol\":\"gemini\",\"ids\":{")?;
                    if let Some(n) = doc.member(root, "responseId")? {
                        e.bytes(b"\"responseId\":")?;
                        doc.write(n, e)?;
                    }
                    e.bytes(b"}")?;
                    if let Some(n) = doc.member(root, "createTime")? {
                        e.bytes(b",\"timestamp\":")?;
                        doc.write(n, e)?;
                    }
                    e.bytes(b"},\"synthetic\":[\"created\"]")
                },
            )?;
            self.base = Some(base);
        }
        if self.operation == Operation::CountTokens {
            doc.unsigned(doc.required(root, "totalTokens")?)?;
            self.emitter.emit(
                owner,
                Kind::CountTokens,
                Scope::default(),
                |e| {
                    e.bytes(b",\"total_tokens\":")?;
                    doc.write(doc.required(root, "totalTokens")?, e)?;
                    e.bytes(b",\"details\":")?;
                    doc.write(root, e)
                },
                |e| e.bytes(b",\"source\":{\"protocol\":\"gemini\"}"),
            )?;
            return Ok(());
        }
        if let Some(feedback) = doc.member(root, "promptFeedback")? {
            if let Some(block) = doc.member(feedback, "blockReason")?
                && !doc.string_is(block, "BLOCK_REASON_UNSPECIFIED")?
            {
                self.blocked = true;
            }
            self.named(
                owner,
                doc,
                feedback,
                Kind::Annotation,
                Scope::default(),
                "promptFeedback",
            )?;
        }
        if let Some(candidates) = doc.member(root, "candidates")? {
            super::array(doc, candidates)?;
            for (ordinal, candidate) in doc.children(candidates)?.enumerate() {
                let index = doc
                    .member(candidate, "index")?
                    .map(|n| doc.unsigned(n))
                    .transpose()?
                    .unwrap_or(ordinal as u64);
                let slot = match self.choices.iter().position(|c| c.index == index) {
                    Some(n) => n,
                    None => self.choices.push(Choice {
                        index,
                        finished: false,
                        next_part: 0,
                        next_tool: 0,
                    })?,
                };
                if self.choices[slot].finished {
                    return Err(Error::Schema);
                }
                if let Some(content) = doc.member(candidate, "content")? {
                    if let Some(role) = doc.member(content, "role")?
                        && !doc.string_is(role, "model")?
                    {
                        return Err(Error::Schema);
                    }
                    if let Some(parts) = doc.member(content, "parts")? {
                        super::array(doc, parts)?;
                        for part in doc.children(parts)? {
                            self.part(owner, doc, part, slot)?;
                        }
                    }
                }
                let scope = Scope {
                    choice_index: Some(index),
                    ..Scope::default()
                };
                for (field, kind) in [
                    ("groundingMetadata", Kind::Grounding),
                    ("citationMetadata", Kind::Annotation),
                    ("safetyRatings", Kind::Annotation),
                    ("urlContextMetadata", Kind::UrlContext),
                ] {
                    if let Some(n) = doc.member(candidate, field)? {
                        self.named(owner, doc, n, kind, scope, field)?;
                    }
                }
                if let Some(logprobs) = doc.member(candidate, "logprobsResult")? {
                    self.chunk(
                        owner,
                        scope,
                        |e| e.bytes(b"{}"),
                        None,
                        Some((doc, logprobs)),
                        |_| Ok(()),
                    )?;
                }
                self.emitter.emit(
                    owner,
                    Kind::Opaque,
                    scope,
                    |e| {
                        e.bytes(b",\"name\":\"gemini.candidate\",\"format\":\"json\",\"value\":{")?;
                        e.bytes(b"\"index\":")?;
                        e.unsigned(index)?;
                        super::members(
                            doc,
                            candidate,
                            &[
                                "index",
                                "content",
                                "finishReason",
                                "groundingMetadata",
                                "citationMetadata",
                                "safetyRatings",
                                "urlContextMetadata",
                                "logprobsResult",
                            ],
                            e,
                        )?;
                        e.bytes(b"}")
                    },
                    |e| e.bytes(b",\"source\":{\"protocol\":\"gemini\"}"),
                )?;
                if let Some(finish) = doc.member(candidate, "finishReason")? {
                    super::string(doc, finish)?;
                    if doc.string_is(finish, "FINISH_REASON_UNSPECIFIED")? {
                        return Err(Error::Schema);
                    }
                    self.chunk(
                        owner,
                        scope,
                        |e| e.bytes(b"{}"),
                        Some((doc, finish)),
                        None,
                        |e| {
                            e.bytes(b",\"native_finish_reason\":")?;
                            doc.write(finish, e)
                        },
                    )?;
                    self.choices[slot].finished = true;
                }
            }
        }
        if let Some(usage) = doc.member(root, "usageMetadata")? {
            super::object(doc, usage)?;
            let mut next = super::mapped(owner, |e| doc.write(usage, e))?;
            if let Some(previous) = self.usage.take() {
                let old = next.absorb(previous)?;
                let mut field = next.node(old)?.first;
                while let Some(n) = field {
                    field = next.node(n)?.next;
                    let name = next.node(n)?.name.ok_or(Error::Schema)?;
                    let mut present = false;
                    for current in next.children(next.root())? {
                        present |= next.names_equal(name, next.node(current)?.name.ok_or(Error::Schema)?)?;
                    }
                    if !present {
                        next.detach(old, n)?;
                        next.append(next.root(), n)?;
                    }
                }
            }
            emit_usage(&mut self.emitter, owner, &next, false)?;
            self.usage = Some(next);
        }
        self.emitter.emit(
            owner,
            Kind::Opaque,
            Scope::default(),
            |e| {
                e.bytes(b",\"name\":\"gemini.response\",\"format\":\"json\",\"value\":{\"candidates\":[]")?;
                super::members(doc, root, &["candidates", "usageMetadata", "promptFeedback"], e)?;
                e.bytes(b"}")
            },
            |e| e.bytes(b",\"source\":{\"protocol\":\"gemini\"}"),
        )?;
        Ok(())
    }
    fn named(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        node: usize,
        kind: Kind,
        scope: Scope,
        name: &'static str,
    ) -> Result<()> {
        self.emitter.emit(
            owner,
            kind,
            scope,
            |e| {
                if kind == Kind::Annotation {
                    e.bytes(b",\"kind\":")?;
                    e.string(name)?;
                }
                e.bytes(b",\"value\":")?;
                doc.write(node, e)
            },
            |e| {
                e.bytes(b",\"source\":{\"protocol\":\"gemini\",\"event\":")?;
                e.string(name)?;
                e.bytes(b"}")
            },
        )?;
        Ok(())
    }
    fn part(&mut self, owner: &mut Writer, doc: &Document, part: usize, slot: usize) -> Result<()> {
        super::object(doc, part)?;
        let choice = &mut self.choices[slot];
        let block = choice.next_part;
        choice.next_part += 1;
        let item = self.next_item;
        self.next_item += 1;
        let mut scope = Scope {
            choice_index: Some(choice.index),
            item_id: Some(media::local_id(b'i', item)?),
            block_index: Some(block),
            ..Scope::default()
        };
        let reasoning = match doc.member(part, "thought")? {
            Some(n) => match doc.node(n)?.value {
                Value::Bool(b) => b,
                _ => return Err(Error::Schema),
            },
            None => false,
        };
        let kind = if doc.member(part, "text")?.is_some() {
            if reasoning { "thinking" } else { "text" }
        } else if doc.member(part, "functionCall")?.is_some() {
            "function"
        } else if doc.member(part, "inlineData")?.is_some() {
            "media"
        } else {
            "opaque"
        };
        self.emitter.emit(
            owner,
            Kind::ItemStart,
            scope,
            |e| e.bytes(b",\"kind\":\"message\",\"role\":\"assistant\""),
            |_| Ok(()),
        )?;
        self.emitter.emit(
            owner,
            Kind::BlockStart,
            scope,
            |e| {
                e.bytes(b",\"kind\":")?;
                e.string(kind)
            },
            |e| {
                super::provenance(
                    doc,
                    part,
                    &[
                        "text",
                        "thoughtSignature",
                        "functionCall",
                        "functionResponse",
                        "inlineData",
                        "executableCode",
                        "codeExecutionResult",
                    ],
                    e,
                )
            },
        )?;
        if let Some(text) = doc.member(part, "text")? {
            super::string(doc, text)?;
            self.chunk(
                owner,
                scope,
                |e| {
                    e.bytes(if reasoning {
                        b"{\"reasoning_content\":"
                    } else {
                        b"{\"content\":"
                    })?;
                    doc.write(text, e)?;
                    e.bytes(b"}")
                },
                None,
                None,
                |_| Ok(()),
            )?;
        }
        if let Some(call) = doc.member(part, "functionCall")? {
            let name = doc.required(call, "name")?;
            super::string(doc, name)?;
            let choice = &mut self.choices[slot];
            scope.tool_index = Some(choice.next_tool);
            choice.next_tool += 1;
            scope.tool_call_id = Some(media::local_id(b't', item)?);
            self.emitter.emit(
                owner,
                Kind::ToolStart,
                scope,
                |e| {
                    e.bytes(b",\"kind\":\"function\",\"arguments_format\":\"json\",\"name\":")?;
                    doc.write(name, e)
                },
                |e| {
                    e.bytes(b",\"source\":{\"protocol\":\"gemini\",\"ids\":{")?;
                    if let Some(id) = doc.member(call, "id")? {
                        e.bytes(b"\"id\":")?;
                        doc.write(id, e)?;
                    }
                    e.bytes(b"}}")
                },
            )?;
            if let Some(args) = doc.member(call, "args")? {
                super::object(doc, args)?;
                self.chunk(
                    owner,
                    scope,
                    |e| {
                        e.bytes(b"{\"tool_calls\":[{\"index\":")?;
                        e.unsigned(scope.tool_index.ok_or(Error::Schema)?)?;
                        e.bytes(b",\"function\":{\"arguments\":")?;
                        e.quoted_json(|e| doc.write(args, e))?;
                        e.bytes(b"}}]}")
                    },
                    None,
                    None,
                    |_| Ok(()),
                )?;
            }
            self.emitter.emit(
                owner,
                Kind::ToolEnd,
                scope,
                |e| e.bytes(b",\"status\":\"completed\",\"valid_json\":true"),
                |_| Ok(()),
            )?;
        }
        if let Some(result) = doc.member(part, "functionResponse")? {
            self.emitter.emit(
                owner,
                Kind::ToolResult,
                scope,
                |e| {
                    e.bytes(b",\"value\":")?;
                    doc.write(result, e)
                },
                |_| Ok(()),
            )?;
        }
        if let Some(inline) = doc.member(part, "inlineData")? {
            let data = doc.required(inline, "data")?;
            let Value::String(text) = doc.node(data)?.value else {
                return Err(Error::Schema);
            };
            let mime = doc.member(inline, "mimeType")?;
            let media = Media {
                node: data,
                id: media::local_id(b'm', item)?,
                data: Some(text),
                encoding: Encoding::Base64,
                kind: mime
                    .map(|m| super::request_content::modality(doc, m))
                    .transpose()?
                    .unwrap_or("document"),
                mime,
            };
            media::publish(doc, media, &mut self.emitter, owner, scope)?;
        }
        for (field, phase) in [("executableCode", "code"), ("codeExecutionResult", "result")] {
            if let Some(value) = doc.member(part, field)? {
                self.emitter.emit(
                    owner,
                    Kind::CodeExecution,
                    scope,
                    |e| {
                        e.bytes(b",\"phase\":")?;
                        e.string(phase)?;
                        e.bytes(b",\"value\":")?;
                        doc.write(value, e)
                    },
                    |_| Ok(()),
                )?;
            }
        }
        if let Some(signature) = doc.member(part, "thoughtSignature")? {
            super::string(doc, signature)?;
            self.emitter.emit(
                owner,
                Kind::ReasoningSignature,
                scope,
                |e| {
                    e.bytes(b",\"kind\":\"gemini\",\"final\":true,\"fragment\":")?;
                    doc.write(signature, e)
                },
                |_| Ok(()),
            )?;
        }
        self.emitter
            .emit(owner, Kind::BlockEnd, scope, |_| Ok(()), |_| Ok(()))?;
        self.emitter.emit(
            owner,
            Kind::ItemEnd,
            scope,
            |e| e.bytes(b",\"status\":\"completed\""),
            |_| Ok(()),
        )?;
        Ok(())
    }
    fn chunk(
        &mut self,
        owner: &mut Writer,
        scope: Scope,
        delta: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
        finish: Option<(&Document, usize)>,
        logprobs: Option<(&Document, usize)>,
        provenance: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
    ) -> Result<()> {
        let base = self.base.as_ref().ok_or(Error::Schema)?;
        self.emitter.emit(
            owner,
            Kind::Chunk,
            scope,
            |e| {
                super::members(base, base.root(), &[], e)?;
                e.bytes(b",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":")?;
                e.unsigned(scope.choice_index.ok_or(Error::Schema)?)?;
                e.bytes(b",\"delta\":")?;
                delta(e)?;
                e.bytes(b",\"finish_reason\":")?;
                if let Some((doc, n)) = finish {
                    e.string(if doc.string_is(n, "STOP")? {
                        "stop"
                    } else if doc.string_is(n, "MAX_TOKENS")? {
                        "length"
                    } else {
                        "content_filter"
                    })?;
                } else {
                    e.bytes(b"null")?;
                }
                if let Some((doc, n)) = logprobs {
                    e.bytes(b",\"logprobs\":")?;
                    doc.write(n, e)?;
                }
                e.bytes(b"}]")
            },
            |e| {
                e.bytes(b",\"source\":{\"protocol\":\"gemini\"}")?;
                provenance(e)
            },
        )?;
        Ok(())
    }
}
fn emit_usage(emitter: &mut Emitter, owner: &mut Writer, doc: &Document, final_snapshot: bool) -> Result<()> {
    let root = doc.root();
    for key in [
        "promptTokenCount",
        "candidatesTokenCount",
        "thoughtsTokenCount",
        "totalTokenCount",
        "cachedContentTokenCount",
    ] {
        if let Some(n) = doc.member(root, key)?
            && !matches!(doc.node(n)?.value, Value::Null)
        {
            doc.unsigned(n)?;
        }
    }
    emitter.emit(
        owner,
        Kind::Usage,
        Scope::default(),
        |e| {
            e.bytes(b",\"usage\":{\"final\":")?;
            e.bytes(if final_snapshot { b"true" } else { b"false" })?;
            for (wire, canonical) in [
                ("promptTokenCount", "input_tokens"),
                ("totalTokenCount", "total_tokens"),
                ("cachedContentTokenCount", "cache_read_tokens"),
                ("thoughtsTokenCount", "reasoning_tokens"),
            ] {
                if let Some(n) = doc.member(root, wire)?
                    && !matches!(doc.node(n)?.value, Value::Null)
                {
                    e.bytes(b",")?;
                    e.string(canonical)?;
                    e.bytes(b":")?;
                    doc.write(n, e)?;
                }
            }
            if let (Some(candidates), Some(thoughts)) = (
                doc.member(root, "candidatesTokenCount")?,
                doc.member(root, "thoughtsTokenCount")?,
            ) && !matches!(doc.node(candidates)?.value, Value::Null)
                && !matches!(doc.node(thoughts)?.value, Value::Null)
            {
                e.bytes(b",\"output_tokens\":")?;
                e.unsigned(
                    doc.unsigned(candidates)?
                        .checked_add(doc.unsigned(thoughts)?)
                        .ok_or(Error::Schema)?,
                )?;
                e.bytes(b",\"derived\":[\"output_tokens\"]")?;
            }
            e.bytes(b",\"native\":{\"protocol\":\"gemini\",\"value\":")?;
            doc.write(root, e)?;
            e.bytes(b"}}")
        },
        |_| Ok(()),
    )?;
    Ok(())
}
fn sse_source(source: &Source, event: &sse::Event, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b",\"source\":{\"protocol\":\"gemini\",\"sse\":[")?;
    let mut first = true;
    for field in event.fields() {
        if source.equals(field.name, b"data")? {
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
        e.bytes(b",\"comment\":")?;
        e.bytes(if field.comment { b"true" } else { b"false" })?;
        e.bytes(b"}")?;
    }
    e.bytes(b"]}")
}
