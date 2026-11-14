mod candidate;
mod part;
mod usage;

use super::Table;
use crate::codec::{
    self, BodyMode, Error, Operation, Result,
    document::Document,
    encode::Encoder,
    event::{Emitter, Identity, Kind, Scope},
    json,
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
                usage::emit(&mut self.state.emitter, owner, usage, true)?;
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
