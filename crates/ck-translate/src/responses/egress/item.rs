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
        let native = doc.member(item, "type")?.is_some();
        if !native {
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
        initialize_call(doc, item)?;
        doc.bind_origin(item, x, None)?;
        if !native {
            self.pending = Some(item);
            return Ok(());
        }
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

    /// Turns the item into a function or custom call with the restored call identity. A source
    /// that carries calls inside a message item (Chat, Anthropic) gets one call item per call.
    pub(super) fn tool_start(
        &mut self,
        owner: &mut Writer,
        at: At,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let At { node, x, .. } = at;
        let doc = &mut self.document;
        if is(doc, node, "kind", "server")? && super::super::response::Server::from_item(doc, at.item)?.is_some() {
            return Ok(());
        }
        let separate = is(doc, at.item, "type", "message")?;
        let (item, index) = if separate {
            call_item(doc, x)?
        } else {
            (at.item, at.index)
        };
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
            if separate {
                // The source names its call identifier `id`; the call item has its own id.
                if let Some(value) = doc.member(ids, "call_id")?.or(doc.member(ids, "id")?) {
                    let target = doc.set(item, "call_id", Value::Null)?;
                    doc.transplant(target, value)?;
                }
            } else {
                for name in ["id", "call_id"] {
                    if let Some(value) = doc.member(ids, name)? {
                        let target = doc.set(item, name, Value::Null)?;
                        doc.transplant(target, value)?;
                    }
                }
            }
        }
        if doc.member(item, "call_id")?.is_none() {
            let value = doc.node(doc.required(x, "tool_call_id")?)?.value;
            doc.set(item, "call_id", value)?;
        }
        if !separate {
            return Ok(());
        }
        initialize_call(doc, item)?;
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

    /// Opens a content or summary part and publishes its added event.
    pub(super) fn block_start(
        &mut self,
        owner: &mut Writer,
        at: At,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let At { node, x, .. } = at;
        // An unmapped Responses summary part returns to the summary it came from.
        let summary = is(&self.document, node, "kind", "reasoning_summary")?
            || (is(&self.document, node, "kind", "opaque")? && from_summary(&self.document, x)?);
        let thinking = is(&self.document, node, "kind", "thinking")? || is(&self.document, node, "kind", "reasoning")?;
        if super::redacted(&self.document, node)? {
            // Its data is dropped by `redacted`; no reasoning item is split out for it.
            return Ok(());
        }
        let (item, index) = if (summary || thinking) && is(&self.document, at.item, "type", "message")? {
            self.reasoning_item(owner, at, output)?
        } else {
            (at.item, at.index)
        };
        let doc = &mut self.document;
        if thinking
            && let Some(attributes) = doc.member(node, "attributes")?
            && is(doc, attributes, "type", "encrypted")?
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
        // An opaque part keeps the type its attributes carry.
        if !is(doc, node, "kind", "opaque")? || doc.member(part, "type")?.is_none() {
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
        }
        let native = doc
            .member(x, "source")?
            .map(|s| is(doc, s, "protocol", "responses"))
            .transpose()?;
        initialize_part(doc, part, native != Some(true))?;
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
        // A provisional null from the item's start attributes gives way to the content.
        if let Some(previous) = doc.member(item, "encrypted_content")?
            && matches!(doc.node(previous)?.value, Value::Null)
        {
            doc.remove(item, "encrypted_content")?;
        }
        append_fragment(doc, item, "encrypted_content", fragment)?;
        Ok(())
    }

    /// Drops redacted reasoning: Responses has no place for another provider's redacted data.
    pub(super) fn redacted(&mut self) -> Result<()> {
        (self.diagnostic)("redacted reasoning", 0);
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
        if let Some((part, summary, block)) = find_part(doc, item, x)? {
            restore_originals(doc, x, part)?;
            let text = match text_done(doc, part)? {
                Some((name, field)) => Some((
                    name,
                    encode::scratch(owner, |e| {
                        item_fields(doc, item, index, e)?;
                        e.bytes(if summary {
                            b",\"summary_index\":"
                        } else {
                            b",\"content_index\":"
                        })?;
                        e.unsigned(block)?;
                        e.bytes(b",")?;
                        e.string(field)?;
                        e.bytes(b":")?;
                        match doc.member(part, field)? {
                            Some(value) => doc.write(value, e)?,
                            None => e.bytes(b"\"\"")?,
                        }
                        super::super::member(doc, part, "logprobs", e)
                    })?,
                )),
                None => None,
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
            if let Some((name, done)) = text {
                self.emit(owner, name, |e| write_lease(&done, e), output)?;
            }
            self.emit(
                owner,
                if summary {
                    "response.reasoning_summary_part.done"
                } else {
                    "response.content_part.done"
                },
                |e| write_lease(&payload, e),
                output,
            )?;
        }
        let doc = &mut self.document;
        if !doc
            .origin(item)?
            .is_some_and(|(_, carrier)| carrier == Some(REASONING_ITEM))
        {
            return Ok(());
        }
        doc.set(item, "status", Value::Text("completed"))?;
        let payload = encode::scratch(owner, |e| {
            e.bytes(b",\"output_index\":")?;
            e.unsigned(index)?;
            e.bytes(b",\"item\":")?;
            write_item(doc, item, e)
        })?;
        self.emit(owner, "response.output_item.done", |e| write_lease(&payload, e), output)
    }

    /// Publishes the done event of a call's arguments or input.
    pub(super) fn tool_end(
        &mut self,
        owner: &mut Writer,
        at: At,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let At { node, item, index, .. } = at;
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
        let item_done = if doc.origin(item)?.is_some_and(|(_, carrier)| carrier == Some(CALL_ITEM)) {
            let status = doc.node(doc.required(node, "status")?)?.value;
            doc.set(item, "status", status)?;
            Some(encode::scratch(owner, |e| {
                e.bytes(b",\"output_index\":")?;
                e.unsigned(index)?;
                e.bytes(b",\"item\":")?;
                write_item(doc, item, e)
            })?)
        } else {
            None
        };
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
        )?;
        match item_done {
            Some(done) => self.emit(owner, "response.output_item.done", |e| write_lease(&done, e), output),
            None => Ok(()),
        }
    }

    /// Finishes the item status and order, and publishes `response.output_item.done`.
    pub(super) fn item_end(
        &mut self,
        owner: &mut Writer,
        at: At,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let At { node, item, index, .. } = at;
        if self.pending == Some(item) {
            let doc = &mut self.document;
            if doc.node(doc.required(item, "content")?)?.first.is_none() {
                // Nothing but split-out reasoning: the message item is never announced.
                self.pending = None;
                let output_node = doc.required(doc.root(), "output")?;
                return doc.detach(output_node, item);
            }
            self.announce(owner, output)?;
        }
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

/// Whether event `x` correlates a Responses summary part.
fn from_summary(doc: &Document, x: usize) -> Result<bool> {
    Ok(match doc.member(x, "source")? {
        Some(source) => is(doc, source, "protocol", "responses")? && doc.member(source, "summary_index")?.is_some(),
        None => false,
    })
}

/// Adds the Responses originals of event `x` to `target`.
fn restore_originals(doc: &mut Document, x: usize, target: usize) -> Result<()> {
    let Some(original) = doc.member(x, "original")? else {
        return Ok(());
    };
    let mut next = doc.node(original)?.first;
    while let Some(entry) = next {
        next = doc.node(entry)?.next;
        if !is(doc, entry, "protocol", "responses")? {
            continue;
        }
        let Value::String(pointer) = doc.node(doc.required(entry, "pointer")?)?.value else {
            return Err(Error::Schema);
        };
        let value = doc.remove(entry, "value")?.ok_or(Error::Schema)?;
        // A later value of a started member replaces it.
        let mut child = doc.node(target)?.first;
        while let Some(at) = child {
            child = doc.node(at)?.next;
            if doc.names_equal(doc.node(at)?.name.ok_or(Error::Schema)?, Name::Pointer(pointer))? {
                doc.detach(target, at)?;
            }
        }
        let field = doc.add(Some(Name::Pointer(pointer)), Value::Null)?;
        doc.transplant(field, value)?;
        doc.append(target, field)?;
    }
    Ok(())
}

/// Gives a text part the empty members that native clients append deltas to. A Responses source
/// supplies its own `annotations` or their absence; any other source gets the native empty array.
fn initialize_part(doc: &mut Document, part: usize, annotate: bool) -> Result<()> {
    let (field, annotated) = if is(doc, part, "type", "output_text")? {
        ("text", true)
    } else if is(doc, part, "type", "refusal")? {
        ("refusal", false)
    } else if is(doc, part, "type", "reasoning_text")? || is(doc, part, "type", "summary_text")? {
        ("text", false)
    } else {
        return Ok(());
    };
    if doc.member(part, field)?.is_none() {
        doc.set(part, field, Value::Text(""))?;
    }
    if annotated && annotate && doc.member(part, "annotations")?.is_none() {
        doc.set(part, "annotations", Value::Array)?;
    }
    Ok(())
}

/// Gives a call item the empty arguments or input that native clients append deltas to.
fn initialize_call(doc: &mut Document, item: usize) -> Result<()> {
    let field = if is(doc, item, "type", "function_call")? {
        "arguments"
    } else if is(doc, item, "type", "custom_tool_call")? {
        "input"
    } else {
        return Ok(());
    };
    if doc.member(item, field)?.is_none() {
        doc.set(item, field, Value::Text(""))?;
    }
    Ok(())
}

/// The done event and full-value member that close a text, refusal or reasoning part.
fn text_done(doc: &Document, part: usize) -> Result<Option<(&'static str, &'static str)>> {
    for (kind, name, field) in [
        ("output_text", "response.output_text.done", "text"),
        ("refusal", "response.refusal.done", "refusal"),
        ("reasoning_text", "response.reasoning_text.done", "text"),
        ("summary_text", "response.reasoning_summary_text.done", "text"),
    ] {
        if is(doc, part, "type", kind)? {
            return Ok(Some((name, field)));
        }
    }
    Ok(None)
}

/// Appends a function call output item for the call that `x` starts inside a message item.
fn call_item(doc: &mut Document, x: usize) -> Result<(usize, u64)> {
    let output = doc.required(doc.root(), "output")?;
    let item = doc.add(None, Value::Object)?;
    doc.append(output, item)?;
    let id = doc.node(doc.required(x, "tool_call_id")?)?.value;
    doc.set(item, "id", id)?;
    doc.set(item, "status", Value::Text("in_progress"))?;
    doc.bind_origin(item, x, Some(CALL_ITEM))?;
    Ok((item, doc.children(output)?.count() as u64 - 1))
}
