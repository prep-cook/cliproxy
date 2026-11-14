use super::*;

impl<F: FnMut(&'static str, u64)> Egress<F> {
    /// Appends a native output item and publishes `response.output_item.added`.
    pub(super) fn item_start(
        &mut self,
        owner: &mut Writer,
        node: usize,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let doc = &mut self.document;
        let x = doc.required(node, "x_ck")?;
        let output_node = doc.required(doc.root(), "output")?;
        let item = doc.add(None, Value::Object)?;
        doc.append(output_node, item)?;
        if let Some(attributes) = doc.member(node, "attributes")? {
            super::super::merge(doc, item, attributes)?;
        }
        if doc.member(item, "type")?.is_none() {
            doc.set(item, "type", Value::Text("message"))?;
        }
        if doc.member(item, "id")?.is_none() {
            let id = doc.node(doc.required(x, "item_id")?)?.value;
            doc.set(item, "id", id)?;
        }
        doc.set(item, "status", Value::Text("in_progress"))?;
        if is(doc, item, "type", "message")? {
            if doc.member(item, "role")?.is_none() {
                doc.set(item, "role", Value::Text("assistant"))?;
            }
            doc.set(item, "content", Value::Array)?;
        } else if is(doc, item, "type", "reasoning")? {
            doc.set(item, "summary", Value::Array)?;
        }
        doc.bind_origin(item, x, None)?;
        let index = native_index(doc, item, "output_index", doc.children(output_node)?.count() as u64 - 1)?;
        let payload = encode::scratch(owner, |e| {
            e.bytes(b",\"output_index\":")?;
            e.unsigned(index)?;
            e.bytes(b",\"item\":")?;
            write_item(doc, item, e)
        })?;
        self.emit(
            owner,
            "response.output_item.added",
            |e| write_lease(&payload, e),
            output,
        )
    }

    /// Turns the item into a function or custom call with the restored call identity.
    pub(super) fn tool_start(&mut self, at: At) -> Result<()> {
        let At { node, x, item, .. } = at;
        let doc = &mut self.document;
        if is(doc, node, "kind", "server")? && super::super::response::Server::from_item(doc, item)?.is_some() {
            return Ok(());
        }
        if is(doc, node, "kind", "custom")? {
            doc.set(item, "type", Value::Text("custom_tool_call"))?;
        } else {
            doc.set(item, "type", Value::Text("function_call"))?;
        }
        for name in ["name", "namespace"] {
            if let Some(value) = doc.member(node, name)? {
                let target = doc.set(item, name, Value::Null)?;
                doc.transplant(target, value)?;
            }
        }
        if let Some(source) = doc.member(x, "source")?
            && let Some(ids) = doc.member(source, "ids")?
        {
            for name in ["id", "call_id"] {
                if let Some(value) = doc.member(ids, name)? {
                    let target = doc.set(item, name, Value::Null)?;
                    doc.transplant(target, value)?;
                }
            }
        }
        if doc.member(item, "call_id")?.is_none() {
            let value = doc.node(doc.required(x, "tool_call_id")?)?.value;
            doc.set(item, "call_id", value)?;
        }
        Ok(())
    }

    /// Opens a content or summary part and publishes its added event.
    pub(super) fn block_start(
        &mut self,
        owner: &mut Writer,
        at: At,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let At { node, x, item, index } = at;
        let doc = &mut self.document;
        let summary = is(doc, node, "kind", "reasoning_summary")?;
        let thinking = is(doc, node, "kind", "thinking")? || is(doc, node, "kind", "reasoning")?;
        if thinking
            && let Some(attributes) = doc.member(node, "attributes")?
            && (is(doc, attributes, "type", "encrypted")? || is(doc, attributes, "type", "redacted")?)
        {
            return Ok(());
        }
        let parts = match doc.member(item, if summary { "summary" } else { "content" })? {
            Some(p) => p,
            None => doc.set(item, if summary { "summary" } else { "content" }, Value::Array)?,
        };
        let part = doc.add(None, Value::Object)?;
        doc.append(parts, part)?;
        if let Some(attrs) = doc.member(node, "attributes")? {
            super::super::merge(doc, part, attrs)?;
        }
        let refusal = is(doc, node, "kind", "refusal")?;
        doc.set(
            part,
            "type",
            Value::Text(if refusal {
                "refusal"
            } else if summary {
                "summary_text"
            } else if thinking {
                "reasoning_text"
            } else {
                "output_text"
            }),
        )?;
        doc.bind_origin(part, x, None)?;
        let payload = encode::scratch(owner, |e| {
            item_fields(doc, item, index, e)?;
            e.bytes(if summary {
                b",\"summary_index\":"
            } else {
                b",\"content_index\":"
            })?;
            e.unsigned(native_index(
                doc,
                part,
                if summary { "summary_index" } else { "content_index" },
                doc.children(parts)?.count() as u64 - 1,
            )?)?;
            e.bytes(b",\"part\":")?;
            write_part(doc, part, e)
        })?;
        self.emit(
            owner,
            if summary {
                "response.reasoning_summary_part.added"
            } else {
                "response.content_part.added"
            },
            |e| write_lease(&payload, e),
            output,
        )
    }

    /// Restores a native server tool result, code or grounding onto its item.
    pub(super) fn execution(
        &mut self,
        owner: &mut Writer,
        kind: Kind,
        at: At,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let At { node, x, item, index } = at;
        let doc = &mut self.document;
        let source = doc.member(x, "source")?;
        let server = super::super::response::Server::from_item(doc, item)?;
        if let (Some(source), Some(server)) = (source, server)
            && is(doc, source, "protocol", "responses")?
        {
            let value = doc.required(node, "value")?;
            if kind == Kind::CodeExecution
                && server == super::super::response::Server::Code
                && is(doc, node, "phase", "code")?
            {
                if matches!(doc.node(value)?.value, Value::Null) {
                    doc.set(item, "code", Value::Null)?;
                    return Ok(());
                }
                let done = is(doc, source, "event", "response.code_interpreter_call_code.done")?;
                if let Some(code) = doc.member(item, "code")?
                    && matches!(doc.node(code)?.value, Value::Null)
                {
                    doc.remove(item, "code")?;
                }
                doc.detach(node, value)?;
                append_fragment(doc, item, "code", value)?;
                let payload = encode::scratch(owner, |e| {
                    item_fields(doc, item, index, e)?;
                    if done {
                        super::super::member(doc, item, "code", e)
                    } else {
                        e.bytes(b",\"delta\":")?;
                        doc.write(value, e)
                    }
                })?;
                return self.emit(
                    owner,
                    if done {
                        "response.code_interpreter_call_code.done"
                    } else {
                        "response.code_interpreter_call_code.delta"
                    },
                    |e| write_lease(&payload, e),
                    output,
                );
            }
            for field in server.fields() {
                if is(doc, x, "responses_field", field)? {
                    let target = doc.set(item, field, Value::Null)?;
                    doc.transplant(target, value)?;
                    return Ok(());
                }
            }
        }
        (self.diagnostic)("unsupported execution event", 0);
        Ok(())
    }

    /// Appends an encrypted reasoning fragment to the item.
    pub(super) fn signature(&mut self, at: At) -> Result<()> {
        let At { node, item, .. } = at;
        let doc = &mut self.document;
        let fragment = doc.required(node, "fragment")?;
        doc.detach(node, fragment)?;
        append_fragment(doc, item, "encrypted_content", fragment)?;
        Ok(())
    }

    /// Restores a redacted reasoning carrier as encrypted content.
    pub(super) fn redacted(&mut self, owner: &mut Writer, at: At) -> Result<()> {
        let At { node, item, .. } = at;
        let doc = &mut self.document;
        let data = doc.required(node, "data")?;
        let lease = encode::scratch(owner, |e| {
            e.bytes(b"\"")?;
            e.bytes(super::super::REDACTED_PREFIX.as_bytes())?;
            doc.write_contents(data, e)?;
            e.bytes(b"\"")
        })?;
        let carrier = doc.import_json(owner, lease)?;
        let target = doc.set(item, "encrypted_content", Value::Null)?;
        doc.transplant(target, carrier)?;
        Ok(())
    }

    /// Appends an annotation to its part and publishes it.
    pub(super) fn annotation(
        &mut self,
        owner: &mut Writer,
        at: At,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let At { node, x, item, index } = at;
        let doc = &mut self.document;
        let (part, _, block) = find_part(doc, item, x)?.ok_or(Error::Schema)?;
        let annotations = match doc.member(part, "annotations")? {
            Some(a) => a,
            None => doc.set(part, "annotations", Value::Array)?,
        };
        let value = doc.required(node, "value")?;
        doc.detach(node, value)?;
        doc.append(annotations, value)?;
        let payload = encode::scratch(owner, |e| {
            item_fields(doc, item, index, e)?;
            e.bytes(b",\"content_index\":")?;
            e.unsigned(block)?;
            e.bytes(b",\"annotation\":")?;
            doc.write(value, e)
        })?;
        self.emit(
            owner,
            "response.output_text.annotation.added",
            |e| write_lease(&payload, e),
            output,
        )
    }

    /// Publishes the done event of a content or summary part.
    pub(super) fn block_end(
        &mut self,
        owner: &mut Writer,
        at: At,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let At { x, item, index, .. } = at;
        let doc = &mut self.document;
        let Some((part, summary, block)) = find_part(doc, item, x)? else {
            return Ok(());
        };
        let payload = encode::scratch(owner, |e| {
            item_fields(doc, item, index, e)?;
            e.bytes(if summary {
                b",\"summary_index\":"
            } else {
                b",\"content_index\":"
            })?;
            e.unsigned(block)?;
            e.bytes(b",\"part\":")?;
            write_part(doc, part, e)
        })?;
        self.emit(
            owner,
            if summary {
                "response.reasoning_summary_part.done"
            } else {
                "response.content_part.done"
            },
            |e| write_lease(&payload, e),
            output,
        )
    }

    /// Publishes the done event of a call's arguments or input.
    pub(super) fn tool_end(
        &mut self,
        owner: &mut Writer,
        at: At,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let At { item, index, .. } = at;
        let doc = &mut self.document;
        if let Some(server) = super::super::response::Server::from_item(doc, item)?
            && (server != super::super::response::Server::Mcp || doc.member(item, "arguments")?.is_none())
        {
            return Ok(());
        }
        let custom = is(doc, item, "type", "custom_tool_call")?;
        let mcp = is(doc, item, "type", "mcp_call")?;
        let field = if custom { "input" } else { "arguments" };
        let payload = encode::scratch(owner, |e| {
            item_fields(doc, item, index, e)?;
            super::super::member(doc, item, field, e)
        })?;
        self.emit(
            owner,
            if custom {
                "response.custom_tool_call_input.done"
            } else if mcp {
                "response.mcp_call_arguments.done"
            } else {
                "response.function_call_arguments.done"
            },
            |e| write_lease(&payload, e),
            output,
        )
    }

    /// Finishes the item status and order, and publishes `response.output_item.done`.
    pub(super) fn item_end(
        &mut self,
        owner: &mut Writer,
        at: At,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let At { node, item, index, .. } = at;
        let doc = &mut self.document;
        if let Some(attrs) = doc.member(node, "attributes")? {
            super::super::merge(doc, item, attrs)?;
        }
        if doc.member(item, "status")?.is_none() || is(doc, item, "status", "in_progress")? {
            doc.set(
                item,
                "status",
                Value::Text(if is(doc, node, "status", "incomplete")? {
                    "incomplete"
                } else {
                    "completed"
                }),
            )?;
        }
        for (field, index_field) in [("content", "content_index"), ("summary", "summary_index")] {
            if let Some(parts) = doc.member(item, field)? {
                order_array(doc, parts, index_field, owner)?;
            }
        }
        let payload = encode::scratch(owner, |e| {
            e.bytes(b",\"output_index\":")?;
            e.unsigned(index)?;
            e.bytes(b",\"item\":")?;
            write_item(doc, item, e)
        })?;
        self.emit(owner, "response.output_item.done", |e| write_lease(&payload, e), output)
    }
}
