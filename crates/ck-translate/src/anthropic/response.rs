use super::{
    Table, equal_text, failure, is, mapped, member, object, server_result, string, unknown, unknown_at, usage::Usage,
};

mod block;
mod event;
use crate::codec::{
    self, BodyMode, Error, Operation, Result,
    document::{Document, Value},
    encode::{self, Encoder},
    event::{Emitter, Identity, Kind, Scope},
    json, media,
    source::{Source, Span},
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
type Origin<'a> = Option<(&'a Document, &'a Source, &'a sse::Event)>;

struct Block {
    index: u64,
    scope: Scope,
    kind: &'static str,
    open: bool,
    arguments: Option<(Source, json::Scanner)>,
    initial: Option<Document>,
    fragments: bool,
    source_id: Option<Document>,
    signature_final: Option<bool>,
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
                failure(&mut self.state.emitter, owner, false)?;
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
        let base = mapped(owner, |e| {
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
                    member(&base, base.root(), key, e)?;
                }
                Ok(())
            },
            |e| {
                e.bytes(b",\"source\":{\"protocol\":\"anthropic\"},\"synthetic\":[\"created\"]")?;
                if let Some((d, n)) = doc {
                    unknown(
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
    fn require_active(&self) -> Result<()> {
        if !self.message_started || self.finished {
            Err(Error::Schema)
        } else {
            Ok(())
        }
    }
    fn buffered(&mut self, owner: &mut Writer, doc: &Document) -> Result<()> {
        let root = doc.root();
        if doc.member(root, "error")?.is_some() || is(doc, root, "type", "error")? {
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
                |e| unknown(doc, root, &["input_tokens"], e),
            )?;
            self.finished = true;
        } else {
            let content = doc.required(root, "content")?;
            if !matches!(doc.node(content)?.value, Value::Array) {
                return Err(Error::Schema);
            }
            for (index, block) in doc.children(content)?.enumerate() {
                self.block_start(owner, doc, block, index as u64, true, None)?;
                self.block_stop(owner, index as u64, None)?;
            }
            self.update_usage(owner, doc, root)?;
            self.finish_choice(owner, doc, root, None)?;
        }
        self.done(owner, None)
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
    fn finish_choice(&mut self, owner: &mut Writer, doc: &Document, parent: usize, origin: Origin<'_>) -> Result<()> {
        self.require_active()?;
        if self.blocks.iter().any(|b| b.open) {
            return Err(Error::Truncated);
        }
        let reason = doc.required(parent, "stop_reason")?;
        string(doc, reason)?;
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
                if let Some((_, source, event)) = origin {
                    provenance_as(source, event, Some("message_delta"), e)?;
                    unknown_at(
                        doc,
                        &[
                            (doc.root(), "", &["type", "delta", "usage"]),
                            (parent, "/delta", &["stop_reason"]),
                        ],
                        e,
                    )
                } else if parent == doc.root() {
                    e.bytes(b",\"source\":{\"protocol\":\"anthropic\"}")?;
                    if let Some(sequence) = doc.member(parent, "stop_sequence")? {
                        e.bytes(
                            b",\"original\":[{\"protocol\":\"anthropic\",\"pointer\":\"/stop_sequence\",\"value\":",
                        )?;
                        doc.write(sequence, e)?;
                        e.bytes(b"}]")?;
                    }
                    Ok(())
                } else {
                    unknown(doc, parent, &["stop_reason"], e)
                }
            },
        )?;
        self.finished = true;
        Ok(())
    }
    fn done(&mut self, owner: &mut Writer, origin: Origin<'_>) -> Result<()> {
        if self.base.is_none() || !self.finished || self.blocks.iter().any(|b| b.open) {
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
        self.emitter.emit(
            owner,
            Kind::Done,
            Scope::default(),
            |_| Ok(()),
            |e| event_origin(origin, "message_stop", &["type"], e),
        )?;
        self.terminal = true;
        Ok(())
    }
}
fn string_or(doc: &Document, parent: usize, key: &str, default: &str, e: &mut Encoder<'_, '_>) -> Result<()> {
    if let Some(node) = doc.member(parent, key)? {
        string(doc, node)?;
        doc.write(node, e)
    } else {
        e.string(default)
    }
}
fn base_fields(doc: &Document, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b",\"object\":\"chat.completion.chunk\"")?;
    for key in ["id", "model", "created"] {
        member(doc, doc.root(), key, e)?;
    }
    Ok(())
}
fn provenance(source: &Source, event: &sse::Event, e: &mut Encoder<'_, '_>) -> Result<()> {
    provenance_as(source, event, None, e)
}
fn provenance_as(source: &Source, event: &sse::Event, fallback: Option<&str>, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b",\"source\":{\"protocol\":\"anthropic\"")?;
    if let Some(name) = event.name(source)? {
        e.bytes(b",\"event\":")?;
        e.quoted_json(|e| e.span(source, name))?;
    } else if let Some(name) = fallback {
        e.bytes(b",\"event\":")?;
        e.string(name)?;
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

fn delta_origin(
    doc: &Document,
    origin: Option<(&Source, &sse::Event)>,
    field: &str,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    if let Some((source, event)) = origin {
        provenance(source, event, e)?;
        unknown_at(
            doc,
            &[
                (doc.root(), "", &["type", "index", "delta"]),
                (doc.required(doc.root(), "delta")?, "/delta", &["type", field]),
            ],
            e,
        )?;
    }
    Ok(())
}

fn event_origin(origin: Origin<'_>, kind: &str, known: &[&str], e: &mut Encoder<'_, '_>) -> Result<()> {
    if let Some((doc, source, event)) = origin {
        provenance_as(source, event, Some(kind), e)?;
        unknown(doc, doc.root(), known, e)
    } else {
        e.bytes(b",\"source\":{\"protocol\":\"anthropic\"}")
    }
}
