use super::*;

impl State {
    /// Validates the terminal response and reconciles it with what was published, then ends
    /// the stream with the final usage, the native response fields and `done`.
    pub(super) fn complete(&mut self, owner: &mut Writer, doc: &Document, root: usize) -> Result<()> {
        if is(doc, root, "status", "failed")?
            || doc
                .member(root, "error")?
                .is_some_and(|n| !matches!(doc.node(n).map(|n| n.value), Ok(Value::Null)))
        {
            return self.fail(owner, doc, root);
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
        self.complete_audio(owner, doc, root)?;
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
            self.complete_output(owner, doc, root, output)?;
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
                object(doc, root, &["output", "usage", "audio"], e)
            },
            |_| Ok(()),
        )?;
        self.emitter
            .emit(owner, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(()))?;
        self.terminal = true;
        Ok(())
    }

    /// Publishes what failed response `root` may tell the client, its identity and usage, then
    /// fails the stream. Every other member, its error and any nonstandard diagnostic included,
    /// stays upstream; egress writes a sanitized error (crates/AGENTS.md, Security).
    pub(super) fn fail(&mut self, owner: &mut Writer, doc: &Document, root: usize) -> Result<()> {
        if !matches!(doc.node(root)?.value, Value::Object) {
            return Err(Error::Schema);
        }
        self.lifecycle(owner, doc, root)?;
        self.emitter.emit(
            owner,
            Kind::Opaque,
            Scope::default(),
            |e| {
                e.bytes(b",\"name\":\"responses.response\",\"format\":\"json\",\"value\":{")?;
                let mut first = true;
                for name in ["id", "object", "created_at", "model"] {
                    if let Some(node) = doc.member(root, name)? {
                        if !first {
                            e.bytes(b",")?;
                        }
                        first = false;
                        e.string(name)?;
                        e.bytes(b":")?;
                        doc.write(node, e)?;
                    }
                }
                e.bytes(b"}")
            },
            |_| Ok(()),
        )?;
        Err(Error::Schema)
    }

    fn complete_audio(&mut self, owner: &mut Writer, doc: &Document, root: usize) -> Result<()> {
        if let Some(native) = doc.member(root, "audio")? {
            if self.audio.is_none() {
                let id = media::local_id(b'm', self.next_media)?;
                self.next_media = self.next_media.checked_add(1).ok_or(Error::Limit)?;
                self.audio = Some(super::super::audio::Ingress::start(
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
        Ok(())
    }

    /// Closes every output item against the snapshot and publishes the finish chunk.
    fn complete_output(&mut self, owner: &mut Writer, doc: &Document, root: usize, output: usize) -> Result<()> {
        for (index, item) in doc.children(output)?.enumerate() {
            let at = self.find_or_add(owner, doc, item, index as u64)?;
            self.snapshot_item(owner, at, doc, item, true)?;
        }
        if self.items.iter().any(|i| !i.closed) || self.items.len() != doc.children(output)?.count() {
            return Err(Error::Truncated);
        }
        let base = self.base.as_ref().ok_or(Error::Schema)?;
        let reason = finish_reason(doc, root, &self.items)?;
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
                e.string(reason)?;
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
        Ok(())
    }
}
