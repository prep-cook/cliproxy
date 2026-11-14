use super::*;

/// Event members the item, content and tool handlers consume; the rest are kept in an opaque
/// `responses.fields` record.
const CONSUMED: &[&str] = &[
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
];

/// What the codec does with one Responses event.
#[derive(Clone, Copy)]
enum Class {
    Failure,
    Terminal,
    Lifecycle,
    Added,
    Done,
    Part { summary: bool, close: bool },
    Text(Text),
    Audio(Audio),
    Code { snapshot: bool },
    PartialImage,
    Arguments { snapshot: bool },
    Annotation,
}

#[derive(Clone, Copy)]
struct Text {
    part: &'static str,
    field: &'static str,
    summary: bool,
    snapshot: bool,
}

#[derive(Clone, Copy)]
enum Audio {
    Delta,
    Done,
    Transcript,
    TranscriptDone,
}

const fn text(part: &'static str, field: &'static str, summary: bool, snapshot: bool) -> Class {
    Class::Text(Text {
        part,
        field,
        summary,
        snapshot,
    })
}

/// Every event this codec maps. Any other name is an opaque event (§3.7): it updates no
/// identity or usage, and its payload members are never interpreted.
const EVENTS: &[(&str, Class)] = &[
    ("error", Class::Failure),
    ("response.failed", Class::Failure),
    ("response.completed", Class::Terminal),
    ("response.incomplete", Class::Terminal),
    ("response.created", Class::Lifecycle),
    ("response.in_progress", Class::Lifecycle),
    ("response.output_item.added", Class::Added),
    ("response.output_item.done", Class::Done),
    (
        "response.content_part.added",
        Class::Part {
            summary: false,
            close: false,
        },
    ),
    (
        "response.content_part.done",
        Class::Part {
            summary: false,
            close: true,
        },
    ),
    (
        "response.reasoning_summary_part.added",
        Class::Part {
            summary: true,
            close: false,
        },
    ),
    (
        "response.reasoning_summary_part.done",
        Class::Part {
            summary: true,
            close: true,
        },
    ),
    ("response.output_text.delta", text("output_text", "text", false, false)),
    ("response.output_text.done", text("output_text", "text", false, true)),
    ("response.refusal.delta", text("refusal", "refusal", false, false)),
    ("response.refusal.done", text("refusal", "refusal", false, true)),
    (
        "response.reasoning_summary_text.delta",
        text("summary_text", "text", true, false),
    ),
    (
        "response.reasoning_summary_text.done",
        text("summary_text", "text", true, true),
    ),
    (
        "response.reasoning_text.delta",
        text("reasoning_text", "text", false, false),
    ),
    (
        "response.reasoning_text.done",
        text("reasoning_text", "text", false, true),
    ),
    ("response.audio.delta", Class::Audio(Audio::Delta)),
    ("response.audio.done", Class::Audio(Audio::Done)),
    ("response.audio.transcript.delta", Class::Audio(Audio::Transcript)),
    ("response.audio.transcript.done", Class::Audio(Audio::TranscriptDone)),
    (
        "response.code_interpreter_call_code.delta",
        Class::Code { snapshot: false },
    ),
    (
        "response.code_interpreter_call_code.done",
        Class::Code { snapshot: true },
    ),
    ("response.image_generation_call.partial_image", Class::PartialImage),
    (
        "response.function_call_arguments.delta",
        Class::Arguments { snapshot: false },
    ),
    (
        "response.function_call_arguments.done",
        Class::Arguments { snapshot: true },
    ),
    (
        "response.custom_tool_call_input.delta",
        Class::Arguments { snapshot: false },
    ),
    (
        "response.custom_tool_call_input.done",
        Class::Arguments { snapshot: true },
    ),
    (
        "response.mcp_call_arguments.delta",
        Class::Arguments { snapshot: false },
    ),
    ("response.mcp_call_arguments.done", Class::Arguments { snapshot: true }),
    ("response.output_text.annotation.added", Class::Annotation),
];

fn lookup(mut named: impl FnMut(&str) -> Result<bool>) -> Result<Option<Class>> {
    for (name, class) in EVENTS {
        if named(name)? {
            return Ok(Some(*class));
        }
    }
    Ok(None)
}

/// Classifies a JSON event by its `type`, else by its SSE name. Only an event with neither
/// is a provider failure when it carries a non-null `error`.
fn classify(doc: &Document, source: &Source, event: &sse::Event) -> Result<Option<Class>> {
    let root = doc.root();
    if !matches!(doc.node(root)?.value, Value::Object) {
        return Ok(None);
    }
    if let Some(kind) = doc.member(root, "type")? {
        if !matches!(doc.node(kind)?.value, Value::String(_)) {
            return Ok(None);
        }
        return lookup(|name| doc.string_is(kind, name));
    }
    if let Some(name) = event.name(source)? {
        return lookup(|known| source.equals(name, known.as_bytes()));
    }
    let error = doc.member(root, "error")?;
    Ok(match error {
        Some(e) if !matches!(doc.node(e)?.value, Value::Null) => Some(Class::Failure),
        _ => None,
    })
}

impl State {
    pub(super) fn event(&mut self, owner: &mut Writer, source: &Source, event: &sse::Event) -> Result<()> {
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
        let Some(doc) = self.parse(owner, source, event)? else {
            return Ok(());
        };
        let root = doc.root();
        let Some(class) = classify(&doc, source, event)? else {
            if self.base.is_none() {
                self.start(owner, None)?;
            }
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
        };
        match class {
            Class::Failure => Err(Error::Schema),
            Class::Terminal => {
                let response = doc.required(root, "response")?;
                self.start(owner, Some((&doc, response)))?;
                self.unknown(owner, &doc, &["type", "response", "sequence_number"], source, event)?;
                self.complete(owner, &doc, response)
            }
            Class::Lifecycle => {
                let response = doc.required(root, "response")?;
                self.lifecycle(owner, &doc, response)?;
                // Lifecycle fields the terminal snapshot may omit; egress merges the later values over them.
                self.emitter.emit(
                    owner,
                    Kind::Opaque,
                    Scope::default(),
                    |e| {
                        e.bytes(b",\"name\":\"responses.response\",\"format\":\"json\",\"value\":")?;
                        object(&doc, response, &["output", "usage", "audio"], e)
                    },
                    |_| Ok(()),
                )?;
                self.unknown(owner, &doc, &["type", "response"], source, event)
            }
            _ => {
                if self.base.is_none() {
                    self.start(owner, None)?;
                }
                self.item_event(owner, &doc, class)?;
                self.unknown(owner, &doc, CONSUMED, source, event)
            }
        }
    }

    /// Parses the event data. Non-JSON data of an unknown event becomes a text opaque record;
    /// under a mapped name, or as an unnamed object that does not parse, it is invalid JSON.
    fn parse(&mut self, owner: &mut Writer, source: &Source, event: &sse::Event) -> Result<Option<Document>> {
        let parsed = if event.data_len(source)? == 0 {
            Err(Error::Json)
        } else {
            let lease = encode::scratch(owner, |e| event.write_data(source, |b| e.bytes(b)))?;
            let mut data = Source::new(owner)?;
            data.push_scratch(lease)?;
            let len = data.len();
            Document::parse(owner, data, len)
        };
        match parsed {
            Ok(doc) => Ok(Some(doc)),
            Err(Error::Json | Error::Truncated) => {
                let invalid = match event.name(source)? {
                    Some(name) => lookup(|known| source.equals(name, known.as_bytes()))?.is_some(),
                    None => {
                        let mut first = None;
                        event.write_data(source, |b| {
                            first = first.or_else(|| b.iter().copied().find(|c| !c.is_ascii_whitespace()));
                            Ok(())
                        })?;
                        first == Some(b'{')
                    }
                };
                if invalid {
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
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Restores the response identity, and publishes usage reported before the terminal event.
    fn lifecycle(&mut self, owner: &mut Writer, doc: &Document, response: usize) -> Result<()> {
        self.start(owner, Some((doc, response)))?;
        if let Some(usage) = doc.member(response, "usage")? {
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

    /// Applies an item, content, tool or media event.
    fn item_event(&mut self, owner: &mut Writer, doc: &Document, class: Class) -> Result<()> {
        let root = doc.root();
        match class {
            Class::Added => {
                let index = doc.unsigned(doc.required(root, "output_index")?)?;
                self.add_item(owner, doc, doc.required(root, "item")?, index).map(drop)
            }
            Class::Done => {
                let index = doc.unsigned(doc.required(root, "output_index")?)?;
                let item = doc.required(root, "item")?;
                let at = self.find_or_add(owner, doc, item, index)?;
                self.snapshot_item(owner, at, doc, item, true)
            }
            Class::Part { summary, close } => {
                let at = self.find_event(doc)?;
                let index =
                    doc.unsigned(doc.required(root, if summary { "summary_index" } else { "content_index" })?)?;
                let part = doc.required(root, "part")?;
                self.block(owner, at, (index, summary), doc, part, (close, close))
            }
            Class::Text(text) => self.text_event(owner, doc, text),
            Class::Audio(audio) => self.audio_event(owner, doc, audio),
            Class::Code { snapshot } => self.code_event(owner, doc, snapshot),
            Class::PartialImage => {
                let at = self.find_event(doc)?;
                let data = doc.required(root, "partial_image_b64")?;
                let partial = doc.unsigned(doc.required(root, "partial_image_index")?)?;
                self.image(owner, at, doc, data, Some(partial))
            }
            Class::Arguments { snapshot } => {
                let at = self.find_event(doc)?;
                let field = if !snapshot {
                    "delta"
                } else if self.items[at].custom()? {
                    "input"
                } else {
                    "arguments"
                };
                self.arguments(owner, at, doc, doc.required(root, field)?, snapshot)
            }
            Class::Annotation => self.annotation_event(owner, doc),
            Class::Failure | Class::Terminal | Class::Lifecycle => Err(Error::Schema),
        }
    }

    /// Publishes one annotation and records it on its part, so the part's done snapshot does
    /// not publish it again.
    fn annotation_event(&mut self, owner: &mut Writer, doc: &Document) -> Result<()> {
        let root = doc.root();
        let at = self.find_event(doc)?;
        let index = doc.unsigned(doc.required(root, "content_index")?)?;
        let annotation = doc.required(root, "annotation")?;
        let item = &mut self.items[at];
        let local = item
            .blocks
            .iter()
            .position(|b| b.index == index && !b.summary)
            .ok_or(Error::Schema)?;
        if item.blocks[local].closed || item.closed {
            return Err(Error::Schema);
        }
        self.emitter.emit(
            owner,
            Kind::Annotation,
            item.scope(Some(local as u64)),
            |e| {
                e.bytes(b",\"kind\":\"citation\",\"value\":")?;
                doc.write(annotation, e)
            },
            |e| provenance(doc, root, e),
        )?;
        let single = super::super::mapped(owner, |e| {
            e.bytes(b"[")?;
            doc.write(annotation, e)?;
            e.bytes(b"]")
        })?;
        let value = item.blocks[local].value;
        super::super::snapshot::append_array(&mut item.doc, value, "annotations", single)
    }

    fn audio_event(&mut self, owner: &mut Writer, doc: &Document, event: Audio) -> Result<()> {
        let root = doc.root();
        if self.audio.is_none() {
            let id = media::local_id(b'm', self.next_media)?;
            self.next_media = self.next_media.checked_add(1).ok_or(Error::Limit)?;
            self.audio = Some(super::super::audio::Ingress::start(owner, &mut self.emitter, id, None)?);
        }
        let audio = self.audio.as_mut().ok_or(Error::Schema)?;
        match event {
            Audio::Delta => audio.data(owner, &mut self.emitter, doc, doc.required(root, "delta")?),
            Audio::Done => audio.audio_done(),
            Audio::Transcript => audio.transcript(owner, &mut self.emitter, doc, doc.required(root, "delta")?, false),
            Audio::TranscriptDone => audio.transcript_done(owner, &mut self.emitter),
        }
    }

    fn text_event(&mut self, owner: &mut Writer, doc: &Document, text: Text) -> Result<()> {
        let root = doc.root();
        let at = self.find_event(doc)?;
        let index = doc.unsigned(doc.required(root, if text.summary { "summary_index" } else { "content_index" })?)?;
        let part = super::super::mapped(owner, |e| {
            e.bytes(b"{\"type\":")?;
            e.string(text.part)?;
            e.bytes(b",")?;
            e.string(text.field)?;
            e.bytes(b":")?;
            doc.write(doc.required(root, if text.snapshot { text.field } else { "delta" })?, e)?;
            super::super::member(doc, root, "logprobs", e)?;
            e.bytes(b"}")
        })?;
        self.block(
            owner,
            at,
            (index, text.summary),
            &part,
            part.root(),
            (text.snapshot, false),
        )
    }
}
