use super::*;

impl Egress {
    pub(super) fn chunk(
        &mut self,
        owner: &mut Writer,
        doc: Document,
        kind: Kind,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        // Source metadata can replace a synthetic response.start after a heartbeat.
        if !self.started && metadata_changed(self.base.as_ref().ok_or(Error::Schema)?, &doc)? {
            self.refresh_base(owner, &doc)?;
        }
        let mut finished = false;
        for choice in doc.children(doc.required(doc.root(), "choices")?)? {
            finished |= self.chunk_choice(owner, &doc, choice, kind, output)?;
        }
        self.chunk_origin(owner, doc, finished, output)
    }
    fn refresh_base(&mut self, owner: &mut Writer, doc: &Document) -> Result<()> {
        let root = doc.root();
        let base = self.base.as_mut().ok_or(Error::Schema)?;
        let update = mapped(owner, |e| {
            e.bytes(b"{")?;
            for (i, key) in ["id", "model", "created"].iter().enumerate() {
                if i != 0 {
                    e.bytes(b",")?;
                }
                e.string(key)?;
                e.bytes(b":")?;
                if let Some(n) = doc.member(root, key)? {
                    doc.write(n, e)?;
                } else {
                    base.write(base.required(base.root(), key)?, e)?;
                }
            }
            member(base, base.root(), "x_ck", e)?;
            e.bytes(b"}")
        })?;
        *base = update;
        Ok(())
    }
    /// Maps choice 0 of a chunk; returns whether it carried a finish reason.
    fn chunk_choice(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        choice_node: usize,
        kind: Kind,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<bool> {
        let x = doc.required(doc.root(), "x_ck")?;
        let index = doc.unsigned(doc.required(choice_node, "index")?)?;
        if index != 0 {
            self.drop_event(kind, Some(index), "single-choice destination selects index zero");
            return Ok(false);
        }
        let delta = doc.required(choice_node, "delta")?;
        for (field, native, delta_kind) in [
            ("content", "text", "text_delta"),
            ("reasoning_content", "thinking", "thinking_delta"),
            ("refusal", "text", "text_delta"),
        ] {
            if let Some(value) = doc.member(delta, field)?
                && !matches!(doc.node(value)?.value, Value::Null)
            {
                let index = self.find_or_text(owner, doc, x, native, output)?;
                self.fragment(owner, doc, value, index, (native, delta_kind), output)?;
            }
        }
        if let Some(calls) = doc.member(delta, "tool_calls")? {
            for call in doc.children(calls)? {
                let tool = doc.unsigned(doc.required(call, "index")?)?;
                let index = self
                    .blocks
                    .iter()
                    .position(|b| b.tool == Some(tool) && b.open)
                    .ok_or(Error::Schema)?;
                let function = doc.required(call, "function")?;
                if let Some(args) = doc.member(function, "arguments")? {
                    self.fragment(owner, doc, args, index, ("partial_json", "input_json_delta"), output)?;
                }
            }
        }
        let mut finished = false;
        if let Some(reason) = doc.member(choice_node, "finish_reason")?
            && !matches!(doc.node(reason)?.value, Value::Null)
        {
            finished = true;
            if self.finish.is_some() || self.blocks.iter().any(|b| b.open) {
                return Err(Error::Schema);
            }
            self.finish = Some(stop_reason(owner, doc, reason)?);
            self.begin(owner, output)?;
        }
        if doc.member(choice_node, "logprobs")?.is_some() {
            self.drop_event(kind, Some(0), "Anthropic has no logprobs carrier");
        }
        Ok(finished)
    }
    /// Keeps native Anthropic message_start/message_delta envelopes for streaming replay.
    fn chunk_origin(
        &mut self,
        owner: &mut Writer,
        doc: Document,
        finished: bool,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let x = doc.required(doc.root(), "x_ck")?;
        if let Some(source) = doc.member(x, "source")?
            && is(&doc, source, "protocol", "anthropic")?
        {
            if is(&doc, source, "event", "message_start")? {
                if self.mode == BodyMode::Streaming {
                    self.start_origin = Some(doc);
                }
            } else if is(&doc, source, "event", "message_delta")? {
                if finished && self.mode == BodyMode::Streaming {
                    self.finish_origin = Some(doc);
                    self.finish_delta(owner, output)?;
                } else if self.mode == BodyMode::Streaming {
                    self.begin(owner, output)?;
                    output(native_event(owner, "message_delta", Some(&doc), |e| {
                        e.bytes(b",\"delta\":{")?;
                        original_members_with_separator(&doc, x, "/delta", false, e)?;
                        e.bytes(b"}")?;
                        if let Some(usage) = &self.usage {
                            e.bytes(b",\"usage\":")?;
                            write_usage(usage, e)?;
                        }
                        original_members(&doc, x, e)
                    })?)?;
                }
            }
        }
        Ok(())
    }
    pub(super) fn opaque(
        &mut self,
        owner: &mut Writer,
        doc: Document,
        kind: Kind,
        choice: Option<u64>,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let root = doc.root();
        let x = doc.required(root, "x_ck")?;
        let source = doc.member(x, "source")?;
        let native = source
            .map(|s| is(&doc, s, "protocol", "anthropic"))
            .transpose()?
            .unwrap_or(false);
        if native && source.is_some_and(|s| is(&doc, s, "event", "content_block").unwrap_or(false)) {
            let index = self.find(&doc, x)?;
            self.blocks[index].document = mapped(owner, |e| doc.write(doc.required(root, "value")?, e))?;
            self.start_block(owner, index, output)?;
        } else if native && self.mode == BodyMode::Streaming {
            let value = doc.required(root, "value")?;
            if matches!(doc.node(value)?.value, Value::Object) && is(&doc, value, "type", "content_block_delta")? {
                let index = self.find(&doc, x)?;
                output(native_event(owner, "content_block_delta", Some(&doc), |e| {
                    e.bytes(b",\"index\":")?;
                    e.unsigned(self.blocks[index].index)?;
                    for field in doc.children(value)? {
                        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
                        if doc.name_is(name, "type")? || doc.name_is(name, "index")? {
                            continue;
                        }
                        e.bytes(b",")?;
                        doc.write_name(name, e)?;
                        e.bytes(b":")?;
                        doc.write(field, e)?;
                    }
                    Ok(())
                })?)?;
            } else {
                output(encode::scratch(owner, |e| write_opaque(&doc, x, e))?)?;
            }
        } else {
            self.drop_event(kind, choice, "opaque event has no native buffered carrier");
        }
        Ok(())
    }
    pub(super) fn done(
        &mut self,
        owner: &mut Writer,
        doc: Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let root = doc.root();
        let x = doc.required(root, "x_ck")?;
        if self.operation == Operation::CountTokens {
            let count = self.count.as_ref().ok_or(Error::Schema)?;
            output(encode::scratch(owner, |e| {
                e.bytes(b"{\"input_tokens\":")?;
                count.write(count.required(count.root(), "input_tokens")?, e)?;
                original_members(count, count.required(count.root(), "x_ck")?, e)?;
                e.bytes(b"}")
            })?)?;
        } else {
            if self.finish.is_none() || self.blocks.iter().any(|b| b.open) {
                return Err(Error::Truncated);
            }
            self.begin(owner, output)?;
            if self.mode == BodyMode::Streaming {
                self.finish_delta(owner, output)?;
                output(native_event(owner, "message_stop", Some(&doc), |e| {
                    original_members(&doc, x, e)
                })?)?;
            } else {
                output(encode::scratch(owner, |e| self.buffered(e))?)?;
            }
        }
        self.terminal = true;
        Ok(())
    }
    pub(super) fn error(
        &mut self,
        owner: &mut Writer,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if self.mode == BodyMode::Streaming {
            output(event(owner, "error", |e| {
                e.bytes(b",\"error\":{\"type\":\"api_error\",\"message\":\"Response failed\"}")
            })?)?;
        } else {
            output(encode::scratch(owner, |e| {
                e.bytes(b"{\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"Response failed\"}}")
            })?)?;
        }
        self.terminal = true;
        Ok(())
    }
    pub(super) fn buffered(&self, e: &mut Encoder<'_, '_>) -> Result<()> {
        e.bytes(b"{")?;
        write_base(self.base.as_ref().ok_or(Error::Schema)?, e)?;
        e.bytes(b",\"content\":[")?;
        for (index, block) in self.blocks.iter().enumerate() {
            if index != 0 {
                e.bytes(b",")?;
            }
            object(&block.document, block.document.root(), &["partial_json"], e)?;
        }
        e.bytes(b"]")?;
        let finish = self.finish.as_ref().ok_or(Error::Schema)?;
        for field in finish.children(finish.root())? {
            e.bytes(b",")?;
            finish.write_name(finish.node(field)?.name.ok_or(Error::Schema)?, e)?;
            e.bytes(b":")?;
            finish.write(field, e)?;
        }
        if let Some(usage) = &self.usage {
            e.bytes(b",\"usage\":")?;
            write_usage(usage, e)?;
        }
        e.bytes(b"}")
    }
}

fn stop_reason(owner: &mut Writer, doc: &Document, reason: usize) -> Result<Document> {
    let x = doc.required(doc.root(), "x_ck")?;
    mapped(owner, |e| {
        e.bytes(b"{\"stop_reason\":")?;
        let native = doc.member(x, "native_finish_reason")?;
        let anthropic = match doc.member(x, "source")? {
            Some(s) => is(doc, s, "protocol", "anthropic")?,
            None => false,
        };
        if anthropic && let Some(native) = native {
            doc.write(native, e)?;
        } else {
            e.string(if doc.string_is(reason, "length")? {
                "max_tokens"
            } else if doc.string_is(reason, "tool_calls")? {
                "tool_use"
            } else if doc.string_is(reason, "content_filter")? {
                "refusal"
            } else {
                "end_turn"
            })?;
        }
        if let Some(source) = doc.member(x, "source")?
            && is(doc, source, "event", "message_delta")?
        {
            original_members_at(doc, x, "/delta", e)?;
        } else {
            original_members(doc, x, e)?;
        }
        e.bytes(b"}")
    })
}
