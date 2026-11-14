use super::{aggregate::aggregate_chunk, response::mapped_document};
use crate::codec::{
    Error, Result,
    document::{Document, Value},
    encode::{self, Encoder},
    event::{Kind, LocalId},
    identity,
    table::Table,
};
use ck_stream::Writer;

#[path = "egress_media.rs"]
mod media_events;

struct Tool {
    choice: u64,
    index: u64,
    custom: bool,
    legacy: bool,
}

/// Native topology shared by delivery and reconciliation; payloads remain leased.
pub(super) struct Reduction {
    pub(super) document: Option<Document>,
    /// The completion id this egress issued because the source supplied none; it replaces the
    /// canonical id on every chunk.
    own_id: Option<LocalId>,
    tools: Table<Tool>,
    media: Table<media_events::Projection>,
}

impl Reduction {
    pub(super) fn new(owner: &mut Writer) -> Result<Self> {
        Ok(Self {
            document: None,
            own_id: None,
            tools: Table::new(owner)?,
            media: Table::new(owner)?,
        })
    }

    pub(super) fn has_open_media(&self) -> bool {
        self.media.iter().any(|entry| entry.is_open())
    }

    /// Refresh the metadata-only streaming state without retaining delta payloads.
    pub(super) fn refresh_metadata(&mut self, owner: &mut Writer, incoming: &Document) -> Result<()> {
        let current = self.document.as_ref().ok_or(Error::Schema)?;
        const FIELDS: [&str; 5] = ["id", "model", "created", "system_fingerprint", "service_tier"];
        // An issued id stays for the whole response; a later canonical id does not replace it.
        let own = self.own_id.is_some();
        let supplied = |key: &str| match key {
            "id" if own => Ok(None),
            _ => incoming.member(incoming.root(), key),
        };
        let mut changed = false;
        for key in FIELDS {
            if let Some(value) = supplied(key)? {
                changed |= match current.member(current.root(), key)? {
                    None => true,
                    Some(old) => match (current.node(old)?.value, incoming.node(value)?.value) {
                        (Value::Null, Value::Null) => false,
                        (Value::Null, _) | (_, Value::Null) => true,
                        _ if key == "created" => current.unsigned(old)? != incoming.unsigned(value)?,
                        _ => !super::response::equal(current, old, incoming, value)?,
                    },
                };
            }
        }
        if !changed {
            return Ok(());
        }
        self.document = Some(mapped_document(owner, |e| {
            e.bytes(b"{\"object\":\"chat.completion\",\"choices\":[]")?;
            for key in FIELDS {
                let supplied = supplied(key)?.map(|value| (incoming, value));
                let retained = current.member(current.root(), key)?.map(|value| (current, value));
                if let Some((doc, value)) = supplied.or(retained) {
                    e.bytes(b",")?;
                    e.string(key)?;
                    e.bytes(b":")?;
                    doc.write(value, e)?;
                }
            }
            e.bytes(b"}")
        })?);
        Ok(())
    }

    /// Returns a native delta and whether provenance had no surviving parent.
    pub(super) fn project(
        &mut self,
        owner: &mut Writer,
        kind: Kind,
        mut doc: Document,
    ) -> Result<(Option<Document>, bool)> {
        let root = doc.root();
        let x = doc.required(root, "x_ck")?;
        let mut lost = false;
        let projection = match kind {
            Kind::ResponseStart => {
                if self.document.is_some() {
                    return Err(Error::Schema);
                }
                if identity::supplied_id(&doc, root)?.is_none() {
                    let id = identity::response_id("chatcmpl-", owner.id())?;
                    doc.set_id(root, "id", id)?;
                    self.own_id = Some(id);
                }
                doc.remove(root, "x_ck")?;
                doc.set(root, "object", Value::Text("chat.completion"))?;
                doc.set(root, "choices", Value::Array)?;
                self.document = Some(doc);
                None
            }
            Kind::Chunk => {
                if doc.member(x, "media_id")?.is_some() {
                    let id = crate::codec::media::media_id(&doc, x)?;
                    if self.media.iter().any(|media| media.id == id && !media.reachable()) {
                        return Ok((None, false));
                    }
                }
                doc.remove(root, "x_ck")?;
                if let Some(id) = self.own_id {
                    doc.set_id(root, "id", id)?;
                }
                lost = !super::provenance::restore(&mut doc, x)?;
                self.project_tools(&mut doc, x)?;
                Some(doc)
            }
            Kind::ToolStart => Some(self.project_tool_start(owner, &doc, x)?),
            Kind::Usage => Some(self.project_usage(owner, &mut doc, x)?),
            Kind::Opaque | Kind::MediaStart | Kind::Data | Kind::MediaTranscript | Kind::MediaEnd => {
                self.project_media(owner, kind, doc, x)?
            }
            Kind::Annotation => {
                if !doc.string_is(doc.required(root, "kind")?, "chat")? {
                    return Err(Error::Unsupported);
                }
                let choice = doc.unsigned(doc.required(x, "choice_index")?)?;
                let value = doc.required(root, "value")?;
                Some(self.native_field(owner, doc, value, "annotations", choice)?)
            }
            _ => None,
        };
        Ok((projection, lost))
    }

    /// Publishes a tool call's name and identity as a native tool_calls or function_call delta.
    fn project_tool_start(&mut self, owner: &mut Writer, doc: &Document, x: usize) -> Result<Document> {
        let root = doc.root();
        let choice = doc.unsigned(doc.required(x, "choice_index")?)?;
        let index = doc.unsigned(doc.required(x, "tool_index")?)?;
        if self
            .tools
            .iter()
            .any(|tool| tool.choice == choice && tool.index == index)
        {
            return Err(Error::Schema);
        }
        let custom = doc.string_is(doc.required(root, "arguments_format")?, "text")?;
        let legacy = doc.member(x, "chat_legacy")?.is_some();
        self.tools.push(Tool {
            choice,
            index,
            custom,
            legacy,
        })?;
        let source = doc.required(x, "source")?;
        let ids = doc.required(source, "ids")?;
        let id = doc.required(ids, "id")?;
        let name = doc.required(root, "name")?;
        mapped_document(owner, |e| {
            e.bytes(b"{")?;
            chunk_base(self.document.as_ref().ok_or(Error::Schema)?, e)?;
            e.bytes(b",\"choices\":[{\"index\":")?;
            e.unsigned(choice)?;
            if legacy {
                e.bytes(b",\"delta\":{\"function_call\":{\"name\":")?;
                doc.write(name, e)?;
                e.bytes(b"}},\"finish_reason\":null}]")?;
            } else {
                e.bytes(b",\"delta\":{\"tool_calls\":[{\"index\":")?;
                e.unsigned(index)?;
                e.bytes(b",\"id\":")?;
                doc.write(id, e)?;
                e.bytes(b",\"type\":")?;
                e.string(if custom { "custom" } else { "function" })?;
                e.bytes(b",")?;
                e.string(if custom { "custom" } else { "function" })?;
                e.bytes(b":{\"name\":")?;
                doc.write(name, e)?;
                e.bytes(b"}}]},\"finish_reason\":null}]")?;
            }
            e.bytes(b"}")
        })
    }

    /// Fills the native usage value from canonical counts it lacks and publishes it as a chunk.
    fn project_usage(&self, owner: &mut Writer, doc: &mut Document, x: usize) -> Result<Document> {
        let root = doc.root();
        let usage = doc.required(root, "usage")?;
        let native = doc.required(usage, "native")?;
        let value = doc.required(native, "value")?;
        for (canonical, wire) in [
            ("input_tokens", "prompt_tokens"),
            ("output_tokens", "completion_tokens"),
            ("total_tokens", "total_tokens"),
        ] {
            if !matches!(doc.node(value)?.value, Value::Null)
                && doc.member(value, wire)?.is_none()
                && let Some(n) = doc.member(usage, canonical)?
            {
                doc.set(value, wire, Value::Unsigned(doc.unsigned(n)?))?;
            }
        }
        mapped_document(owner, |e| {
            e.bytes(b"{")?;
            chunk_base(self.document.as_ref().ok_or(Error::Schema)?, e)?;
            e.bytes(b",\"choices\":[")?;
            if let Some(choice) = doc.member(x, "choice_index")? {
                e.bytes(b"{\"index\":")?;
                doc.write(choice, e)?;
                e.bytes(b",\"delta\":{},\"finish_reason\":null,\"usage\":")?;
                doc.write(value, e)?;
                e.bytes(b"}]")?;
            } else {
                e.bytes(b"],\"usage\":")?;
                doc.write(value, e)?;
            }
            e.bytes(b"}")
        })
    }

    /// Projects a media event onto the native field its attributes name.
    fn project_media(&mut self, owner: &mut Writer, kind: Kind, doc: Document, x: usize) -> Result<Option<Document>> {
        Ok(match kind {
            Kind::Opaque => {
                let id = crate::codec::media::media_id(&doc, x)?;
                let native = self
                    .media
                    .iter_mut()
                    .find(|entry| entry.id == id)
                    .ok_or(Error::Schema)?
                    .attributes(doc)?;
                native
                    .map(|(doc, value, field, choice)| self.native_field(owner, doc, value, field, choice))
                    .transpose()?
            }
            Kind::MediaStart => {
                let mut projection = media_events::Projection::start(owner, doc, x)?;
                if self.media.iter().any(|entry| entry.id == projection.id) {
                    return Err(Error::Schema);
                }
                let native = projection.initial()?;
                self.media.push(projection)?;
                native
                    .map(|(doc, value, field, choice)| self.native_field(owner, doc, value, field, choice))
                    .transpose()?
            }
            _ => {
                let id = crate::codec::media::media_id(&doc, x)?;
                let position = self
                    .media
                    .iter()
                    .position(|entry| entry.id == id)
                    .ok_or(Error::Schema)?;
                let native = if kind == Kind::Data {
                    self.media[position].data(doc)?
                } else if kind == Kind::MediaTranscript {
                    self.media[position].transcript(doc)?
                } else {
                    self.media[position].end(&doc)?
                };
                native
                    .map(|(doc, value, field, choice)| self.native_field(owner, doc, value, field, choice))
                    .transpose()?
            }
        })
    }

    pub(super) fn fold(&mut self, projection: Document) -> Result<()> {
        let aggregate = self.document.as_mut().ok_or(Error::Schema)?;
        let incoming = aggregate.absorb(projection)?;
        aggregate_chunk(aggregate, incoming)
    }

    fn project_tools(&self, doc: &mut Document, x: usize) -> Result<()> {
        let Some(choice_node) = doc.member(x, "choice_index")? else {
            return Ok(());
        };
        let Some(tool_node) = doc.member(x, "tool_index")? else {
            return Ok(());
        };
        let choice_index = doc.unsigned(choice_node)?;
        let tool_index = doc.unsigned(tool_node)?;
        let tool = self
            .tools
            .iter()
            .find(|t| t.choice == choice_index && t.index == tool_index)
            .ok_or(Error::Schema)?;
        let choices = doc.required(doc.root(), "choices")?;
        let choice = doc.node(choices)?.first.ok_or(Error::Schema)?;
        let delta = doc.required(choice, "delta")?;
        let Some(calls) = doc.member(delta, "tool_calls")? else {
            return Ok(());
        };
        let call = doc.node(calls)?.first.ok_or(Error::Schema)?;
        let Some(function) = doc.member(call, "function")? else {
            return Ok(());
        };
        if tool.legacy {
            doc.remove(delta, "tool_calls")?;
            doc.detach(call, function)?;
            doc.rename(function, "function_call")?;
            doc.append(delta, function)?;
        } else if tool.custom {
            doc.rename(function, "custom")?;
            if let Some(arguments) = doc.member(function, "arguments")? {
                doc.rename(arguments, "input")?;
            }
        }
        Ok(())
    }
}

fn chunk_base(doc: &Document, e: &mut Encoder<'_, '_>) -> Result<()> {
    let mut first = true;
    for key in ["id", "model", "created", "system_fingerprint", "service_tier"] {
        if let Some(value) = doc.member(doc.root(), key)? {
            if !first {
                e.bytes(b",")?;
            }
            first = false;
            e.string(key)?;
            e.bytes(b":")?;
            doc.write(value, e)?;
        }
    }
    e.bytes(b",\"object\":\"chat.completion.chunk\"")
}
