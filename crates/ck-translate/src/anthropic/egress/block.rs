use super::*;

impl Egress {
    pub(super) fn block_start(
        &mut self,
        owner: &mut Writer,
        doc: Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let root = doc.root();
        let x = doc.required(root, "x_ck")?;
        if self.finish.is_some() {
            return Err(Error::Schema);
        }
        let item = local(&doc, doc.required(x, "item_id")?)?;
        let canonical = doc.unsigned(doc.required(x, "block_index")?)?;
        if self.blocks.iter().any(|b| b.item == item && b.canonical == canonical) {
            return Err(Error::Schema);
        }
        let native = if is(&doc, root, "kind", "text")? {
            "text"
        } else if is(&doc, root, "kind", "thinking")? || is(&doc, root, "kind", "reasoning")? {
            "thinking"
        } else if is(&doc, root, "kind", "redacted")? {
            "redacted_thinking"
        } else if is(&doc, root, "kind", "function")? {
            "tool_use"
        } else if is(&doc, root, "kind", "server")? {
            "server_tool_use"
        } else if is(&doc, root, "kind", "tool_result")? {
            "tool_result"
        } else {
            "opaque"
        };
        let mut body = mapped(owner, |e| {
            if let Some(attrs) = doc.member(root, "attributes")? {
                object(&doc, attrs, &["type"], e)
            } else {
                e.bytes(b"{}")
            }
        })?;
        let body_root = body.root();
        if matches!(native, "tool_result" | "server_tool_use")
            && let Some(attrs) = doc.member(root, "attributes")?
            && let Some(kind) = doc.member(attrs, "type")?
        {
            let from = body.absorb(mapped(owner, |e| doc.write(kind, e))?)?;
            copy_value(&mut body, body_root, "type", from)?;
        } else {
            body.set(body_root, "type", Value::Text(native))?;
        }
        for (k, field) in [("text", "text"), ("thinking", "thinking")] {
            if native == k {
                body.set(body_root, field, Value::Fragments)?;
            }
        }
        let index = self.blocks.len() as u64;
        self.blocks.push(Block {
            item,
            canonical,
            index,
            document: body,
            open: true,
            started: false,
            tool: None,
            explicit_block: true,
            start_origin: Some(doc),
        })?;
        if matches!(native, "text" | "thinking") {
            self.start_block(owner, index as usize, output)?;
        }
        Ok(())
    }
    pub(super) fn tool_start(
        &mut self,
        owner: &mut Writer,
        doc: Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let root = doc.root();
        let x = doc.required(root, "x_ck")?;
        let index = self.find_or_tool(owner, &doc, x)?;
        let block = &mut self.blocks[index];
        if block.tool.is_some() {
            return Err(Error::Schema);
        }
        block.tool = Some(doc.unsigned(doc.required(x, "tool_index")?)?);
        let body = &mut block.document;
        let from = body.absorb(doc)?;
        let x = body.required(from, "x_ck")?;
        let id = if let Some(source) = body.member(x, "source")? {
            if let Some(ids) = body.member(source, "ids")? {
                body.member(ids, "id")?.or(body.member(ids, "call_id")?)
            } else {
                None
            }
        } else {
            None
        };
        if let Some(id) = id {
            copy_value(body, body.root(), "id", id)?;
        } else {
            let id = local(body, body.required(x, "tool_call_id")?)?;
            body.set(body.root(), "id", Value::Id(id))?;
        }
        let name = body.required(from, "name")?;
        copy_value(body, body.root(), "name", name)?;
        if is(body, from, "kind", "server")? && is(body, body.root(), "type", "tool_use")? {
            body.set(body.root(), "type", Value::Text("server_tool_use"))?;
        }
        if !is(body, from, "arguments_format", "json")? {
            return Err(Error::Unsupported);
        }
        self.start_block(owner, index, output)?;
        Ok(())
    }
    pub(super) fn signature(
        &mut self,
        owner: &mut Writer,
        doc: Document,
        kind: Kind,
        choice: Option<u64>,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let root = doc.root();
        let x = doc.required(root, "x_ck")?;
        if !is(&doc, root, "kind", "anthropic")? {
            self.drop_event(
                kind,
                choice,
                "foreign encrypted or signed reasoning has no Anthropic carrier",
            );
            return Ok(());
        }
        let index = self.find(&doc, x)?;
        self.fragment(
            owner,
            &doc,
            doc.required(root, "fragment")?,
            index,
            ("signature", "signature_delta"),
            output,
        )?;
        Ok(())
    }
    pub(super) fn tool_result(
        &mut self,
        owner: &mut Writer,
        doc: Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let root = doc.root();
        let x = doc.required(root, "x_ck")?;
        let index = self.find(&doc, x)?;
        let body = &mut self.blocks[index].document;
        let from = body.absorb(doc)?;
        copy_value(body, body.root(), "content", body.required(from, "value")?)?;
        if let Some(error) = body.member(from, "is_error")? {
            copy_value(body, body.root(), "is_error", error)?;
        }
        let x = body.required(from, "x_ck")?;
        let source = body.required(x, "source")?;
        let ids = body.required(source, "ids")?;
        copy_value(body, body.root(), "tool_use_id", body.required(ids, "tool_use_id")?)?;
        self.start_block(owner, index, output)?;
        Ok(())
    }
    pub(super) fn redacted(
        &mut self,
        owner: &mut Writer,
        doc: Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let root = doc.root();
        let x = doc.required(root, "x_ck")?;
        let index = self.find(&doc, x)?;
        let body = &mut self.blocks[index].document;
        let from = body.absorb(doc)?;
        copy_value(body, body.root(), "data", body.required(from, "data")?)?;
        self.start_block(owner, index, output)?;
        Ok(())
    }
    pub(super) fn annotation(
        &mut self,
        owner: &mut Writer,
        doc: Document,
        kind: Kind,
        choice: Option<u64>,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let root = doc.root();
        let x = doc.required(root, "x_ck")?;
        let index = self.find(&doc, x)?;
        let value = doc.required(root, "value")?;
        let many = is(&doc, root, "kind", "citations")?;
        if !many && !is(&doc, root, "kind", "citation")? {
            self.drop_event(kind, choice, "annotation has no native citation carrier");
            return Ok(());
        }
        if many {
            if self.mode == BodyMode::Buffered {
                let body = &mut self.blocks[index].document;
                if body.member(body.root(), "citations")?.is_none() {
                    body.set(body.root(), "citations", Value::Array)?;
                }
            }
            for citation in doc.children(value)? {
                self.citation(owner, &doc, citation, index, output)?;
            }
        } else {
            self.citation(owner, &doc, value, index, output)?;
        }
        Ok(())
    }
    pub(super) fn tool_end(
        &mut self,
        owner: &mut Writer,
        doc: Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let root = doc.root();
        let x = doc.required(root, "x_ck")?;
        let index = self.find(&doc, x)?;
        let body = &mut self.blocks[index].document;
        if let Some(fragments) = body.remove(body.root(), "partial_json")? {
            let raw = encode::scratch(owner, |e| {
                for fragment in body.children(fragments)? {
                    let mut chars = body.chars(fragment)?;
                    while let Some(c) = chars.next_scalar()? {
                        e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
                    }
                }
                Ok(())
            })?;
            let input = body.import_json(owner, raw)?;
            body.rename(input, "input")?;
            body.append(body.root(), input)?;
        }
        if !self.blocks[index].explicit_block {
            self.close_block(owner, index, None, output)?;
        }
        Ok(())
    }
    pub(super) fn find(&self, doc: &Document, x: usize) -> Result<usize> {
        let item = local(doc, doc.required(x, "item_id")?)?;
        if let Some(index) = doc.member(x, "block_index")? {
            let index = doc.unsigned(index)?;
            self.blocks
                .iter()
                .position(|b| b.item == item && b.canonical == index && b.open)
                .ok_or(Error::Schema)
        } else if let Some(tool) = doc.member(x, "tool_index")? {
            let tool = doc.unsigned(tool)?;
            self.blocks
                .iter()
                .position(|b| b.item == item && b.tool == Some(tool) && b.open)
                .ok_or(Error::Schema)
        } else {
            Err(Error::Schema)
        }
    }
    pub(super) fn find_or_tool(&mut self, owner: &mut Writer, doc: &Document, x: usize) -> Result<usize> {
        if doc.member(x, "block_index")?.is_some() {
            return self.find(doc, x);
        }
        let index = self.blocks.len();
        self.blocks.push(Block {
            item: local(doc, doc.required(x, "item_id")?)?,
            canonical: u64::MAX - index as u64,
            index: index as u64,
            document: mapped(owner, |e| e.bytes(b"{\"type\":\"tool_use\"}"))?,
            open: true,
            started: false,
            tool: None,
            explicit_block: false,
            start_origin: None,
        })?;
        Ok(index)
    }
    pub(super) fn find_or_text(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        x: usize,
        native: &'static str,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<usize> {
        if doc.member(x, "block_index")?.is_some() {
            return self.find(doc, x);
        }
        let item = local(doc, doc.required(x, "item_id")?)?;
        if let Some(index) = self.blocks.iter().position(|b| {
            b.item == item && b.open && is(&b.document, b.document.root(), "type", native).unwrap_or(false)
        }) {
            return Ok(index);
        }
        let index = self.blocks.len();
        let mut body = mapped(owner, |e| e.bytes(b"{}"))?;
        body.set(body.root(), "type", Value::Text(native))?;
        body.set(body.root(), native, Value::Fragments)?;
        self.blocks.push(Block {
            item,
            canonical: u64::MAX - index as u64,
            index: index as u64,
            document: body,
            open: true,
            started: false,
            tool: None,
            explicit_block: false,
            start_origin: None,
        })?;
        self.start_block(owner, index, output)?;
        Ok(index)
    }
    pub(super) fn start_block(
        &mut self,
        owner: &mut Writer,
        index: usize,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        self.begin(owner, output)?;
        let block = &mut self.blocks[index];
        if block.started {
            return Ok(());
        }
        let origin = block.start_origin.take();
        if self.mode == BodyMode::Streaming {
            output(native_event(owner, "content_block_start", origin.as_ref(), |e| {
                e.bytes(b",\"index\":")?;
                e.unsigned(block.index)?;
                e.bytes(b",\"content_block\":")?;
                object(&block.document, block.document.root(), &["partial_json"], e)?;
                if let Some(doc) = &origin {
                    original_members(doc, doc.required(doc.root(), "x_ck")?, e)?;
                }
                Ok(())
            })?)?;
        }
        block.started = true;
        Ok(())
    }
    pub(super) fn close_block(
        &mut self,
        owner: &mut Writer,
        index: usize,
        origin: Option<&Document>,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        self.start_block(owner, index, output)?;
        let block = &mut self.blocks[index];
        if !block.open {
            return Err(Error::Schema);
        }
        block.open = false;
        if self.mode == BodyMode::Streaming {
            output(native_event(owner, "content_block_stop", origin, |e| {
                e.bytes(b",\"index\":")?;
                e.unsigned(block.index)?;
                if let Some(doc) = origin {
                    original_members(doc, doc.required(doc.root(), "x_ck")?, e)?;
                }
                Ok(())
            })?)?;
        }
        Ok(())
    }
    pub(super) fn fragment(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        value: usize,
        index: usize,
        (field, delta): (&'static str, &'static str),
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        string(doc, value)?;
        self.start_block(owner, index, output)?;
        let block = &mut self.blocks[index];
        if self.mode == BodyMode::Streaming {
            output(native_event(owner, "content_block_delta", Some(doc), |e| {
                e.bytes(b",\"index\":")?;
                e.unsigned(block.index)?;
                e.bytes(b",\"delta\":{\"type\":")?;
                e.string(delta)?;
                e.bytes(b",")?;
                e.string(field)?;
                e.bytes(b":")?;
                doc.write(value, e)?;
                let x = doc.required(doc.root(), "x_ck")?;
                original_members_at(doc, x, "/delta", e)?;
                e.bytes(b"}")?;
                original_members(doc, x, e)
            })?)?;
        }
        // Tool JSON still needs validation at its end; other streamed payloads need no second state copy.
        if self.mode == BodyMode::Buffered || field == "partial_json" {
            let incoming = mapped(owner, |e| doc.write(value, e))?;
            let fragment = block.document.absorb(incoming)?;
            let root = block.document.root();
            let fragments = match block.document.member(root, field)? {
                Some(n) => n,
                None => block.document.set(root, field, Value::Fragments)?,
            };
            block.document.append(fragments, fragment)?;
        }
        Ok(())
    }
    pub(super) fn citation(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        value: usize,
        index: usize,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if self.mode == BodyMode::Streaming {
            let block = &self.blocks[index];
            output(native_event(owner, "content_block_delta", Some(doc), |e| {
                e.bytes(b",\"index\":")?;
                e.unsigned(block.index)?;
                e.bytes(b",\"delta\":{\"type\":\"citations_delta\",\"citation\":")?;
                doc.write(value, e)?;
                let x = doc.required(doc.root(), "x_ck")?;
                original_members_at(doc, x, "/delta", e)?;
                e.bytes(b"}")?;
                original_members(doc, x, e)
            })?)?;
        } else {
            let incoming = mapped(owner, |e| doc.write(value, e))?;
            let body = &mut self.blocks[index].document;
            let value = body.absorb(incoming)?;
            let array = match body.member(body.root(), "citations")? {
                Some(n) => n,
                None => body.set(body.root(), "citations", Value::Array)?,
            };
            body.append(array, value)?;
        }
        Ok(())
    }
}
