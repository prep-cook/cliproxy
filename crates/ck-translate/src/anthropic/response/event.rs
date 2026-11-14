use super::*;

impl State {
    pub(super) fn event(&mut self, owner: &mut Writer, source: &Source, event: &sse::Event) -> Result<()> {
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
        let Some(doc) = self.data(owner, source, event)? else {
            return Ok(());
        };
        let root = doc.root();
        let kind = doc.required(root, "type")?;
        if let Some(name) = event.name(source)? {
            names_type(&doc, kind, source, name)?;
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
                |e| event_origin(Some((&doc, source, event)), "ping", &["type"], e),
            )?;
        } else if doc.string_is(kind, "message_start")? {
            self.message_start(owner, (&doc, source, event))?;
        } else if doc.string_is(kind, "content_block_start")? {
            self.require_active()?;
            self.block_start(
                owner,
                &doc,
                doc.required(root, "content_block")?,
                doc.unsigned(doc.required(root, "index")?)?,
                false,
                Some((&doc, source, event)),
            )?;
        } else if doc.string_is(kind, "content_block_delta")? {
            self.require_active()?;
            self.delta(
                owner,
                &doc,
                doc.required(root, "delta")?,
                doc.unsigned(doc.required(root, "index")?)?,
                (source, event),
            )?;
        } else if doc.string_is(kind, "content_block_stop")? {
            self.require_active()?;
            self.block_stop(
                owner,
                doc.unsigned(doc.required(root, "index")?)?,
                Some((&doc, source, event)),
            )?;
        } else if doc.string_is(kind, "message_delta")? {
            self.message_delta(owner, (&doc, source, event))?;
        } else if doc.string_is(kind, "message_stop")? {
            self.done(owner, Some((&doc, source, event)))?;
        } else {
            self.opaque(owner, source, event, |e| {
                e.bytes(b",\"format\":\"json\",\"value\":")?;
                doc.write(root, e)
            })?;
        }
        Ok(())
    }
    /// Parses the event data. Data under an event name this codec does not map is published as
    /// opaque text or JSON instead, and yields `None`.
    fn data(&mut self, owner: &mut Writer, source: &Source, event: &sse::Event) -> Result<Option<Document>> {
        let raw = encode::scratch(owner, |e| event.write_data(source, |b| e.bytes(b)))?;
        let mut data = Source::new(owner)?;
        data.push_scratch(raw)?;
        let len = data.len();
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
        let named_known = match event.name(source)? {
            Some(n) => recognized.iter().try_fold(false, |yes, key| {
                Ok::<_, Error>(yes || source.equals(n, key.as_bytes())?)
            })?,
            None => true,
        };
        let doc = match Document::parse(owner, data, len) {
            Ok(doc) => doc,
            Err(Error::Json | Error::Truncated) if !named_known => {
                self.opaque(owner, source, event, |e| {
                    e.bytes(b",\"format\":\"text\",\"value\":")?;
                    e.quoted_json(|e| event.write_data(source, |b| e.bytes(b)))
                })?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if !named_known {
            self.opaque(owner, source, event, |e| {
                e.bytes(b",\"format\":\"json\",\"value\":")?;
                doc.write(doc.root(), e)
            })?;
            return Ok(None);
        }
        Ok(Some(doc))
    }
    fn opaque(
        &mut self,
        owner: &mut Writer,
        source: &Source,
        event: &sse::Event,
        value: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
    ) -> Result<()> {
        self.start(owner, None)?;
        self.emitter.emit(owner, Kind::Opaque, Scope::default(), value, |e| {
            provenance(source, event, e)
        })?;
        Ok(())
    }
    fn message_start(&mut self, owner: &mut Writer, origin: (&Document, &Source, &sse::Event)) -> Result<()> {
        let doc = origin.0;
        if self.message_started {
            return Err(Error::Schema);
        }
        self.message_started = true;
        let message = doc.required(doc.root(), "message")?;
        if let Some(role) = doc.member(message, "role")?
            && !doc.string_is(role, "assistant")?
        {
            return Err(Error::Schema);
        }
        // A leading heartbeat may have created synthetic identity; replace it with supplied metadata.
        if self.base.is_some() {
            if self.blocks.iter().any(|b| b.open) || self.finished {
                return Err(Error::Schema);
            }
            self.base = Some(mapped(owner, |e| {
                e.bytes(b"{\"id\":")?;
                string_or(doc, message, "id", self.identity.stream_id.as_str(), e)?;
                e.bytes(b",\"model\":")?;
                string_or(doc, message, "model", "", e)?;
                e.bytes(b",\"created\":0}")
            })?);
        } else {
            self.start(owner, Some((doc, message)))?;
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
            |e| event_origin(Some(origin), "message_start", &["type", "message"], e),
        )?;
        if let Some(content) = doc.member(message, "content")? {
            if !matches!(doc.node(content)?.value, Value::Array) {
                return Err(Error::Schema);
            }
            for (index, block) in doc.children(content)?.enumerate() {
                self.block_start(owner, doc, block, index as u64, true, None)?;
                self.block_stop(owner, index as u64, None)?;
            }
        }
        self.update_usage(owner, doc, message)
    }
    fn message_delta(&mut self, owner: &mut Writer, origin: (&Document, &Source, &sse::Event)) -> Result<()> {
        let (doc, source, event) = origin;
        if !self.message_started {
            return Err(Error::Schema);
        }
        let root = doc.root();
        self.update_usage(owner, doc, root)?;
        let delta = doc.required(root, "delta")?;
        if let Some(reason) = doc.member(delta, "stop_reason")?
            && !matches!(doc.node(reason)?.value, Value::Null)
        {
            return self.finish_choice(owner, doc, delta, Some(origin));
        }
        let base = self.base.as_ref().ok_or(Error::Schema)?;
        self.emitter.emit(
            owner,
            Kind::Chunk,
            self.scope()?,
            |e| {
                base_fields(base, e)?;
                e.bytes(b",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":null}]")
            },
            |e| {
                provenance_as(source, event, Some("message_delta"), e)?;
                unknown_at(
                    doc,
                    &[(root, "", &["type", "delta", "usage"]), (delta, "/delta", &[])],
                    e,
                )
            },
        )?;
        Ok(())
    }
}

/// Requires the SSE event name, when present, to equal the data's `type`.
fn names_type(doc: &Document, kind: usize, source: &Source, name: Span) -> Result<()> {
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
    Ok(())
}
