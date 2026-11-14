use super::*;

impl<F: FnMut(&'static str, u64)> Egress<F> {
    /// Ends the response as failed: the response with a sanitized error, as the body in buffered
    /// mode and in a `response.failed` event when streaming.
    pub(super) fn failed(
        &mut self,
        owner: &mut Writer,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        // A stream that fails before its start still opens with `response.created`.
        self.owed |= self.sequence == 0;
        self.created(owner, output)?;
        self.terminal = true;
        let doc = &mut self.document;
        let root = doc.root();
        doc.set(root, "status", Value::Text("failed"))?;
        let error = doc.set(root, "error", Value::Object)?;
        doc.set(error, "code", Value::Text("server_error"))?;
        doc.set(error, "message", Value::Text("Response failed"))?;
        order_array(doc, doc.required(root, "output")?, "output_index", owner)?;
        let response = encode::scratch(owner, |e| write_response(doc, e))?;
        if self.mode == BodyMode::Buffered {
            return output(response);
        }
        self.emit(
            owner,
            "response.failed",
            |e| {
                e.bytes(b",\"response\":")?;
                write_lease(&response, e)
            },
            output,
        )
    }

    /// Restores the native identity; `response.created` is owed until `created` publishes it.
    /// Fields the source did not supply keep the response's own values.
    pub(super) fn response_start(&mut self, incoming: Document) -> Result<()> {
        let doc = &mut self.document;
        let target = doc.root();
        let node = doc.absorb(incoming)?;
        for (from, to) in [
            ("id", "id"),
            ("model", "model"),
            ("created", "created_at"),
            ("status", "status"),
        ] {
            if let Some(value) = doc.remove(node, from)?
                && !codec::identity::synthetic(doc, node, from)?
            {
                let target = doc.set(target, to, Value::Null)?;
                doc.transplant(target, value)?;
            }
        }
        self.owed = true;
        Ok(())
    }

    /// Publishes the owed `response.created` with the response as it stands. Every event other
    /// than a heartbeat, usage, start, opaque record or chunk calls it before its own output; a
    /// chunk with output follows the start of its item, and the finish chunk writes nothing.
    pub(super) fn created(
        &mut self,
        owner: &mut Writer,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if !self.owed {
            return Ok(());
        }
        self.owed = false;
        let response = encode::scratch(owner, |e| self.document.write(self.document.root(), e))?;
        self.emit(
            owner,
            "response.created",
            |e| {
                e.bytes(b",\"response\":")?;
                write_lease(&response, e)
            },
            output,
        )
    }

    /// Closes the response: buffered mode writes the body, streaming the terminal event.
    pub(super) fn done(
        &mut self,
        owner: &mut Writer,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if self.images.iter().any(|image| !image.closed) || self.audio.iter().any(|audio| !audio.closed) {
            return Err(Error::Truncated);
        }
        self.created(owner, output)?;
        self.terminal = true;
        let doc = &mut self.document;
        let root = doc.root();
        if is(doc, root, "status", "in_progress")? {
            doc.set(root, "status", Value::Text("completed"))?;
        }
        order_array(doc, doc.required(root, "output")?, "output_index", owner)?;
        let response = encode::scratch(owner, |e| write_response(doc, e))?;
        if self.mode == BodyMode::Buffered {
            return output(response);
        }
        let name = if is(doc, root, "status", "incomplete")? {
            "response.incomplete"
        } else {
            "response.completed"
        };
        self.emit(
            owner,
            name,
            |e| {
                e.bytes(b",\"response\":")?;
                write_lease(&response, e)
            },
            output,
        )
    }

    /// Restores native response fields, or writes or retains an unknown event.
    pub(super) fn opaque(
        &mut self,
        owner: &mut Writer,
        incoming: Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let root = incoming.root();
        if is(&incoming, root, "name", "responses.response")? {
            let progress = match incoming
                .member(root, "x_ck")?
                .map(|x| incoming.member(x, "source"))
                .transpose()?
            {
                Some(Some(source)) => is(&incoming, source, "event", "response.in_progress")?,
                _ => false,
            };
            let doc = &mut self.document;
            let node = doc.absorb(incoming)?;
            let value = doc.required(node, "value")?;
            super::super::opaque::preserve_carrier_collision(doc, value)?;
            // A client that has seen the response keeps its id for the whole response.
            if self.sequence > 0 {
                doc.remove(value, "id")?;
            }
            super::super::merge(doc, doc.root(), value)?;
            // Every snapshot carries the native identity, so the owed `response.created` follows
            // its merge rather than a synthesized start.
            self.created(owner, output)?;
            if !progress || self.mode == BodyMode::Buffered {
                return Ok(());
            }
            let doc = &self.document;
            let response = encode::scratch(owner, |e| write_response(doc, e))?;
            return self.emit(
                owner,
                "response.in_progress",
                |e| {
                    e.bytes(b",\"response\":")?;
                    write_lease(&response, e)
                },
                output,
            );
        }
        if is(&incoming, root, "name", "responses.audio")? {
            let node = self.document.absorb(incoming)?;
            let at = self.audio_index(node)?.ok_or(Error::Schema)?;
            let doc = &mut self.document;
            let value = doc.required(node, "value")?;
            return super::super::merge(doc, self.audio[at].audio, value);
        }
        if self.mode == BodyMode::Streaming {
            return output(encode::scratch(owner, |e| super::super::opaque::write(&incoming, e))?);
        }
        super::super::opaque::retain(&mut self.document, incoming)
    }

    /// Stores the usage object: the native value when the source was Responses, otherwise the
    /// Responses projection of the canonical snapshot, which itself stays in the opaque carrier.
    pub(super) fn usage(&mut self, node: usize) -> Result<()> {
        let doc = &mut self.document;
        let usage = doc.required(node, "usage")?;
        let value = super::super::usage::project(doc, usage, &mut self.usage_snapshot)?;
        let target = doc.set(doc.root(), "usage", Value::Null)?;
        doc.transplant(target, value)
    }

    /// Appends one compact output item.
    pub(super) fn compact(&mut self, node: usize) -> Result<()> {
        let doc = &mut self.document;
        let output_node = doc.required(doc.root(), "output")?;
        let item = doc.required(node, "item")?;
        doc.detach(node, item)?;
        doc.append(output_node, item)?;
        Ok(())
    }
}
