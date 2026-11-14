use super::*;
use crate::codec::document::Name;

impl State {
    pub(super) fn delta(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        _choice: usize,
        body: usize,
        position: usize,
        buffered: bool,
    ) -> Result<()> {
        if !matches!(doc.node(body)?.value, Value::Object) {
            return Err(Error::Schema);
        }
        // Streamed responses carry reasoning before the answer, so a buffered message opens its
        // reasoning block first whatever its member order.
        // ponytail: a `content` over 1,024 bytes that precedes `reasoning_content` is published,
        // and opens its block, while the body arrives (response_buffered.rs), before reasoning is
        // read. Agreeing there needs that text held to the message end or egress ordering.
        // Reasoning read before such an answer is published ahead of it (`reasoning_first`).
        let reasoning = if buffered {
            doc.member(body, "reasoning_content")?
        } else {
            None
        };
        if let Some(field) = reasoning
            && !self.choices[position].reasoning_first
        {
            self.body_field(owner, doc, field, position, buffered)?;
        }
        for field in doc.children(body)? {
            if Some(field) != reasoning {
                self.body_field(owner, doc, field, position, buffered)?;
            }
        }
        Ok(())
    }

    /// Publishes one member of a message or delta.
    fn body_field(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        field: usize,
        position: usize,
        buffered: bool,
    ) -> Result<()> {
        if buffered && self.finish_published(owner, doc, field)? {
            return Ok(());
        }
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        if doc.name_is(name, "audio")?
            && matches!(doc.node(field)?.value, Value::Object)
            && self.media_value(owner, doc, field, self.scope(position), "audio")?
        {
            return Ok(());
        }
        if !matches!(doc.node(field)?.value, Value::Null) {
            self.publish_audio(owner, position)?;
        }
        if doc.name_is(name, "images")?
            || (doc.name_is(name, "content")? && matches!(doc.node(field)?.value, Value::Array))
        {
            let name = if doc.name_is(name, "images")? {
                "images"
            } else {
                "content"
            };
            self.array_field(owner, doc, field, position, name)?;
            return Ok(());
        }
        if doc.name_is(name, "annotations")? && matches!(doc.node(field)?.value, Value::Array) {
            self.annotations(owner, doc, field, position)?;
            return Ok(());
        }
        if doc.name_is(name, "tool_calls")? {
            self.tool_calls(owner, doc, field, position, buffered)?;
            return Ok(());
        }
        if doc.name_is(name, "function_call")? {
            self.legacy_tool(owner, doc, field, position)?;
            return Ok(());
        }
        let mut known = false;
        for key in [
            "role",
            "content",
            "reasoning_content",
            "refusal",
            "audio",
            "annotations",
        ] {
            known |= doc.name_is(name, key)?;
        }
        if !known {
            self.unknown_field(owner, doc, field, position)?;
            return Ok(());
        }
        self.scalar_field(owner, doc, field, name, position)
    }

    /// Publishes a buffered message's complete `reasoning_content` before ingress starts
    /// publishing a long answer field, so the thinking block opens first as it does when
    /// streamed. Reasoning still to come, or without text, waits for the message end.
    pub(super) fn reasoning_first(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        message: usize,
        position: usize,
    ) -> Result<()> {
        if self.choices[position].reasoning_first {
            return Ok(());
        }
        let Some(field) = doc.member(message, "reasoning_content")? else {
            return Ok(());
        };
        if !matches!(doc.node(field)?.value, Value::String(_)) || doc.chars(field)?.next_scalar()?.is_none() {
            return Ok(());
        }
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        self.scalar_field(owner, doc, field, name, position)?;
        self.choices[position].reasoning_first = true;
        Ok(())
    }

    fn tool_calls(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        field: usize,
        position: usize,
        buffered: bool,
    ) -> Result<()> {
        if !matches!(doc.node(field)?.value, Value::Array) {
            return Err(Error::Schema);
        }
        if doc.node(field)?.first.is_none() {
            self.empty_array(owner, position, "tool_calls")?;
        }
        for (index, call) in doc.children(field)?.enumerate() {
            self.tool(
                owner,
                doc,
                call,
                position,
                if buffered { Some(index as u64) } else { None },
                None,
            )?;
        }
        Ok(())
    }

    /// Publishes an empty delta that carries an unmapped member as provenance.
    fn unknown_field(&mut self, owner: &mut Writer, doc: &Document, field: usize, position: usize) -> Result<()> {
        self.emitter.emit(
            owner,
            Kind::Chunk,
            self.scope(position),
            |e| {
                base(self.base.as_ref().ok_or(Error::Schema)?, e)?;
                e.bytes(b",\"choices\":[{\"index\":")?;
                e.unsigned(self.choices[position].index)?;
                e.bytes(b",\"delta\":{},\"finish_reason\":null}]")
            },
            |e| super::super::provenance::one(doc, field, e),
        )?;
        Ok(())
    }

    /// Publishes a role, text, reasoning, refusal or unconsumed media member, opening its block once.
    fn scalar_field(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        field: usize,
        name: Name,
        position: usize,
    ) -> Result<()> {
        let mut scope = self.scope(position);
        for (block, key) in ["content", "reasoning_content", "refusal"].iter().enumerate() {
            if doc.name_is(name, key)? && !matches!(doc.node(field)?.value, Value::Null) {
                if !matches!(
                    doc.node(field)?.value,
                    Value::String(_) | Value::Text(_) | Value::Fragments
                ) {
                    return Err(Error::Schema);
                }
                scope.block_index = Some(block as u64);
                if !self.choices[position].blocks[block] {
                    self.emitter.emit(
                        owner,
                        Kind::BlockStart,
                        scope,
                        |e| {
                            e.bytes(b",\"kind\":")?;
                            e.string(["text", "thinking", "refusal"][block])
                        },
                        |_| Ok(()),
                    )?;
                    self.choices[position].blocks[block] = true;
                }
            }
        }
        self.emitter.emit(
            owner,
            Kind::Chunk,
            scope,
            |e| {
                base(self.base.as_ref().ok_or(Error::Schema)?, e)?;
                e.bytes(b",\"choices\":[{\"index\":")?;
                e.unsigned(self.choices[position].index)?;
                e.bytes(b",\"delta\":{")?;
                doc.write_name(name, e)?;
                e.bytes(b":")?;
                doc.write(field, e)?;
                e.bytes(b"},\"finish_reason\":null}]")
            },
            |_| Ok(()),
        )?;
        Ok(())
    }
}
