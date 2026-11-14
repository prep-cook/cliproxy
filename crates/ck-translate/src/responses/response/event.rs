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

/// Event names this codec maps; data that is not JSON fails only under one of these names.
const KNOWN: &[&str] = &[
    "error",
    "response.audio.delta",
    "response.audio.done",
    "response.audio.transcript.delta",
    "response.audio.transcript.done",
    "response.code_interpreter_call_code.delta",
    "response.code_interpreter_call_code.done",
    "response.completed",
    "response.content_part.added",
    "response.content_part.done",
    "response.created",
    "response.custom_tool_call_input.delta",
    "response.custom_tool_call_input.done",
    "response.failed",
    "response.function_call_arguments.delta",
    "response.function_call_arguments.done",
    "response.image_generation_call.partial_image",
    "response.incomplete",
    "response.in_progress",
    "response.mcp_call_arguments.delta",
    "response.mcp_call_arguments.done",
    "response.output_item.added",
    "response.output_item.done",
    "response.output_text.annotation.added",
    "response.output_text.delta",
    "response.output_text.done",
    "response.reasoning_summary_part.added",
    "response.reasoning_summary_part.done",
    "response.reasoning_summary_text.delta",
    "response.reasoning_summary_text.done",
    "response.reasoning_text.delta",
    "response.reasoning_text.done",
    "response.refusal.delta",
    "response.refusal.done",
];

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
        // An untyped body with an error object is a provider failure; a typed unknown event
        // that happens to carry an `error` member is not.
        let untyped_error = doc.member(root, "type")?.is_none()
            && doc
                .member(root, "error")?
                .is_some_and(|e| !matches!(doc.node(e).map(|n| n.value), Ok(Value::Null)));
        if is(&doc, root, "type", "error")? || is(&doc, root, "type", "response.failed")? || untyped_error {
            return Err(Error::Schema);
        }
        self.lifecycle(owner, &doc)?;
        if is(&doc, root, "type", "response.completed")? || is(&doc, root, "type", "response.incomplete")? {
            let response = doc.required(root, "response")?;
            self.unknown(owner, &doc, &["type", "response", "sequence_number"], source, event)?;
            return self.complete(owner, &doc, response);
        }
        if is(&doc, root, "type", "response.created")? || is(&doc, root, "type", "response.in_progress")? {
            // Lifecycle fields the terminal snapshot may omit; egress merges the later values over them.
            self.emitter.emit(
                owner,
                Kind::Opaque,
                Scope::default(),
                |e| {
                    e.bytes(b",\"name\":\"responses.response\",\"format\":\"json\",\"value\":")?;
                    object(&doc, doc.required(root, "response")?, &["output", "usage", "audio"], e)
                },
                |_| Ok(()),
            )?;
            self.unknown(owner, &doc, &["type", "response"], source, event)?;
            return Ok(());
        }
        if self.item_event(owner, &doc)? {
            return self.unknown(owner, &doc, CONSUMED, source, event);
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
        Ok(())
    }

    /// Parses the event data; non-JSON data of an unknown event becomes a text opaque record.
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
                let name = event.name(source)?.ok_or(Error::Json)?;
                for known in KNOWN {
                    if source.equals(name, known.as_bytes())? {
                        return Err(Error::Json);
                    }
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
    fn lifecycle(&mut self, owner: &mut Writer, doc: &Document) -> Result<()> {
        let root = doc.root();
        if let Some(response) = doc.member(root, "response")? {
            self.start(owner, Some((doc, response)))?;
            if !is(doc, root, "type", "response.completed")?
                && !is(doc, root, "type", "response.incomplete")?
                && let Some(usage) = doc.member(response, "usage")?
            {
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
        } else if self.base.is_none() {
            self.start(owner, None)?;
        }
        Ok(())
    }

    /// Applies an item, content, tool or media event; false when `doc` is none of them.
    fn item_event(&mut self, owner: &mut Writer, doc: &Document) -> Result<bool> {
        let root = doc.root();
        if is(doc, root, "type", "response.output_item.added")? {
            let index = doc.unsigned(doc.required(root, "output_index")?)?;
            self.add_item(owner, doc, doc.required(root, "item")?, index)?;
            return Ok(true);
        }
        if is(doc, root, "type", "response.output_item.done")? {
            let index = doc.unsigned(doc.required(root, "output_index")?)?;
            let item = doc.required(root, "item")?;
            let at = self.find_or_add(owner, doc, item, index)?;
            self.snapshot_item(owner, at, doc, item, true)?;
            return Ok(true);
        }
        for (added, done, summary) in [
            ("response.content_part.added", "response.content_part.done", false),
            (
                "response.reasoning_summary_part.added",
                "response.reasoning_summary_part.done",
                true,
            ),
        ] {
            let close = is(doc, root, "type", done)?;
            if close || is(doc, root, "type", added)? {
                let at = self.find_event(doc)?;
                let index =
                    doc.unsigned(doc.required(root, if summary { "summary_index" } else { "content_index" })?)?;
                let part = doc.required(root, "part")?;
                self.block(owner, at, (index, summary), doc, part, (close, close))?;
                return Ok(true);
            }
        }
        if self.text_event(owner, doc)? || self.audio_event(owner, doc)? || self.execution_event(owner, doc)? {
            return Ok(true);
        }
        if is(doc, root, "type", "response.image_generation_call.partial_image")? {
            let at = self.find_event(doc)?;
            let data = doc.required(root, "partial_image_b64")?;
            let partial = doc.unsigned(doc.required(root, "partial_image_index")?)?;
            self.image(owner, at, doc, data, Some(partial))?;
            return Ok(true);
        }
        for (delta, done) in [
            (
                "response.function_call_arguments.delta",
                "response.function_call_arguments.done",
            ),
            (
                "response.custom_tool_call_input.delta",
                "response.custom_tool_call_input.done",
            ),
            ("response.mcp_call_arguments.delta", "response.mcp_call_arguments.done"),
        ] {
            let snapshot = is(doc, root, "type", done)?;
            if snapshot || is(doc, root, "type", delta)? {
                let at = self.find_event(doc)?;
                let field = if !snapshot {
                    "delta"
                } else if self.items[at].custom()? {
                    "input"
                } else {
                    "arguments"
                };
                self.arguments(owner, at, doc, doc.required(root, field)?, snapshot)?;
                return Ok(true);
            }
        }
        if is(doc, root, "type", "response.output_text.annotation.added")? {
            self.annotation_event(owner, doc)?;
            return Ok(true);
        }
        Ok(false)
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
            self.audio = Some(super::super::audio::Ingress::start(owner, &mut self.emitter, id, None)?);
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
                let part = super::super::mapped(owner, |e| {
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
                    super::super::member(doc, root, "logprobs", e)?;
                    e.bytes(b"}")
                })?;
                self.block(owner, at, (index, summary), &part, part.root(), (snapshot, false))?;
                return Ok(true);
            }
        }
        Ok(false)
    }
}
