use super::*;

impl<F: FnMut(&'static str, u64)> Egress<F> {
    /// Ends the response as failed: the response with a sanitized error, as the body in buffered
    /// mode and in a `response.failed` event when streaming.
    pub(super) fn failed(
        &mut self,
        owner: &mut Writer,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        self.terminal = true;
        let doc = &mut self.document;
        let root = doc.root();
        doc.set(root, "status", Value::Text("failed"))?;
        let error = doc.set(root, "error", Value::Object)?;
        doc.set(error, "code", Value::Text("upstream_error"))?;
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

    /// Publishes `response.created` with the restored native identity.
    pub(super) fn response_start(
        &mut self,
        owner: &mut Writer,
        incoming: Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let doc = &mut self.document;
        let target = doc.root();
        let node = doc.absorb(incoming)?;
        for (from, to) in [("id", "id"), ("model", "model"), ("created", "created_at")] {
            if let Some(value) = doc.remove(node, from)? {
                let target = doc.set(target, to, Value::Null)?;
                doc.transplant(target, value)?;
            }
        }
        let response = encode::scratch(owner, |e| doc.write(target, e))?;
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
            let doc = &mut self.document;
            let node = doc.absorb(incoming)?;
            let value = doc.required(node, "value")?;
            super::super::opaque::preserve_carrier_collision(doc, value)?;
            super::super::merge(doc, doc.root(), value)?;
            return Ok(());
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
