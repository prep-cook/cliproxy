use super::*;

impl State {
    pub(super) fn block_start(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        node: usize,
        index: u64,
        whole: bool,
        origin: Origin<'_>,
    ) -> Result<()> {
        if self.blocks.iter().any(|b| b.index == index) {
            return Err(Error::Schema);
        }
        let kind = block_kind(doc, node)?;
        let tool = matches!(kind, "function" | "server");
        let scope = self.block_scope(doc, node, kind, index)?;
        if tool {
            self.next_tool += 1;
        }
        self.emitter.emit(
            owner,
            Kind::BlockStart,
            scope,
            |e| {
                e.bytes(b",\"kind\":")?;
                e.string(kind)?;
                e.bytes(b",\"attributes\":")?;
                if kind == "opaque" {
                    return e.bytes(b"{}");
                }
                let skip: &[&str] = match kind {
                    "text" => &["text", "citations"],
                    "thinking" => &["thinking", "signature"],
                    "redacted" => &["data"],
                    "function" | "server" => &["input", "id", "name"],
                    "tool_result" => &["content", "tool_use_id", "is_error"],
                    _ => &[],
                };
                object(doc, node, skip, e)
            },
            |e| event_origin(origin, "content_block_start", &["type", "index", "content_block"], e),
        )?;
        if tool {
            self.tool_start(owner, doc, node, kind, scope)?;
        }
        let initial = if tool {
            doc.member(node, "input")?
                .map(|n| mapped(owner, |e| doc.write(n, e)))
                .transpose()?
        } else {
            None
        };
        self.blocks.push(Block {
            index,
            scope,
            kind,
            open: true,
            arguments: if tool {
                Some((Source::new(owner)?, json::Scanner::new(owner, u64::MAX)?))
            } else {
                None
            },
            initial,
            fragments: false,
            signature_final: None,
            source_id: if tool {
                Some(mapped(owner, |e| doc.write(doc.required(node, "id")?, e))?)
            } else {
                None
            },
        })?;
        self.block_content(owner, doc, node, kind, scope, whole)?;
        if whole && tool {
            let input = doc.member(node, "input")?;
            if let Some(input) = input {
                self.arguments(owner, index, doc, input, false, None)?;
            }
        }
        Ok(())
    }
    /// Scope of a new block: tool blocks take the next tool index, and a server tool result
    /// joins the scope of the tool block whose id it names.
    fn block_scope(&self, doc: &Document, node: usize, kind: &str, index: u64) -> Result<Scope> {
        let tool = matches!(kind, "function" | "server");
        let result_scope = if kind == "tool_result" {
            let id = doc.required(node, "tool_use_id")?;
            let mut matched = None;
            for block in self.blocks.iter() {
                if let Some(source) = &block.source_id
                    && equal_text(source, source.root(), doc, id)?
                {
                    matched = Some(block.scope);
                    break;
                }
            }
            Some(matched.ok_or(Error::Schema)?)
        } else {
            None
        };
        Ok(Scope {
            block_index: Some(index),
            tool_index: if tool {
                Some(self.next_tool)
            } else {
                result_scope.and_then(|s| s.tool_index)
            },
            tool_call_id: if tool {
                Some(media::local_id(b't', self.next_tool)?)
            } else {
                result_scope.and_then(|s| s.tool_call_id)
            },
            ..self.scope()?
        })
    }
    fn tool_start(&mut self, owner: &mut Writer, doc: &Document, node: usize, kind: &str, scope: Scope) -> Result<()> {
        let name = doc.required(node, "name")?;
        string(doc, name)?;
        let id = doc.required(node, "id")?;
        string(doc, id)?;
        // An MCP server name is the call's namespace, as in request history.
        let namespace = if is(doc, node, "type", "mcp_tool_use")? {
            let namespace = doc.required(node, "server_name")?;
            string(doc, namespace)?;
            Some(namespace)
        } else {
            None
        };
        self.emitter.emit(
            owner,
            Kind::ToolStart,
            scope,
            |e| {
                e.bytes(b",\"kind\":")?;
                e.string(if kind == "server" { "server" } else { "function" })?;
                e.bytes(b",\"arguments_format\":\"json\",\"name\":")?;
                doc.write(name, e)?;
                if let Some(namespace) = namespace {
                    e.bytes(b",\"namespace\":")?;
                    doc.write(namespace, e)?;
                }
                Ok(())
            },
            |e| {
                e.bytes(b",\"source\":{\"protocol\":\"anthropic\",\"ids\":{\"id\":")?;
                doc.write(id, e)?;
                e.bytes(b"}}")
            },
        )?;
        Ok(())
    }
    /// Publishes the content a block start carries: text, thinking and its signature, redacted
    /// data, a server tool result, an opaque block or text citations.
    fn block_content(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        node: usize,
        kind: &str,
        scope: Scope,
        whole: bool,
    ) -> Result<()> {
        if matches!(kind, "text" | "thinking") {
            let (field, target) = if kind == "text" {
                ("text", "content")
            } else {
                ("thinking", "reasoning_content")
            };
            if let Some(value) = doc.member(node, field)? {
                self.chunk(owner, scope, doc, value, target, None)?;
            }
        }
        if kind == "thinking"
            && let Some(signature) = doc.member(node, "signature")?
        {
            self.signature(owner, scope, doc, signature, whole, None)?;
        }
        if kind == "redacted" {
            let data = doc.required(node, "data")?;
            string(doc, data)?;
            self.emitter.emit(
                owner,
                Kind::ReasoningRedacted,
                scope,
                |e| {
                    e.bytes(b",\"data\":")?;
                    doc.write(data, e)
                },
                |_| Ok(()),
            )?;
        }
        if kind == "tool_result" {
            self.tool_result(owner, doc, node, scope)?;
        }
        if kind == "opaque" {
            self.emitter.emit(
                owner,
                Kind::Opaque,
                scope,
                |e| {
                    e.bytes(b",\"format\":\"json\",\"value\":")?;
                    doc.write(node, e)
                },
                |e| e.bytes(b",\"source\":{\"protocol\":\"anthropic\",\"event\":\"content_block\"}"),
            )?;
        }
        if kind == "text"
            && let Some(citations) = doc.member(node, "citations")?
        {
            self.emitter.emit(
                owner,
                Kind::Annotation,
                scope,
                |e| {
                    e.bytes(b",\"kind\":\"citations\",\"value\":")?;
                    doc.write(citations, e)
                },
                |_| Ok(()),
            )?;
        }
        Ok(())
    }
    fn tool_result(&mut self, owner: &mut Writer, doc: &Document, node: usize, scope: Scope) -> Result<()> {
        self.emitter.emit(
            owner,
            Kind::ToolResult,
            scope,
            |e| {
                e.bytes(b",\"value\":")?;
                doc.write(doc.required(node, "content")?, e)?;
                member(doc, node, "is_error", e)
            },
            |e| {
                e.bytes(b",\"source\":{\"protocol\":\"anthropic\",\"ids\":{\"tool_use_id\":")?;
                doc.write(doc.required(node, "tool_use_id")?, e)?;
                e.bytes(b"}}")
            },
        )?;
        Ok(())
    }
    pub(super) fn delta(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        delta: usize,
        index: u64,
        origin: (&Source, &sse::Event),
    ) -> Result<()> {
        let block = self
            .blocks
            .iter()
            .find(|b| b.index == index && b.open)
            .ok_or(Error::Schema)?;
        let scope = block.scope;
        if is(doc, delta, "type", "text_delta")? && block.kind == "text" {
            self.chunk(owner, scope, doc, doc.required(delta, "text")?, "content", Some(origin))
        } else if is(doc, delta, "type", "thinking_delta")? && block.kind == "thinking" {
            self.chunk(
                owner,
                scope,
                doc,
                doc.required(delta, "thinking")?,
                "reasoning_content",
                Some(origin),
            )
        } else if is(doc, delta, "type", "signature_delta")? && block.kind == "thinking" {
            self.signature(
                owner,
                scope,
                doc,
                doc.required(delta, "signature")?,
                false,
                Some(origin),
            )
        } else if is(doc, delta, "type", "input_json_delta")? && block.arguments.is_some() {
            self.arguments(
                owner,
                index,
                doc,
                doc.required(delta, "partial_json")?,
                true,
                Some(origin),
            )
        } else if is(doc, delta, "type", "citations_delta")? {
            self.emitter.emit(
                owner,
                Kind::Annotation,
                scope,
                |e| {
                    e.bytes(b",\"kind\":\"citation\",\"value\":")?;
                    doc.write(doc.required(delta, "citation")?, e)
                },
                |e| delta_origin(doc, Some(origin), "citation", e),
            )?;
            Ok(())
        } else if ["text_delta", "thinking_delta", "signature_delta", "input_json_delta"]
            .iter()
            .try_fold(false, |known, kind| {
                Ok::<_, Error>(known || is(doc, delta, "type", kind)?)
            })?
        {
            Err(Error::Schema)
        } else {
            self.emitter.emit(
                owner,
                Kind::Opaque,
                scope,
                |e| {
                    e.bytes(b",\"format\":\"json\",\"value\":")?;
                    doc.write(doc.root(), e)
                },
                |e| provenance(origin.0, origin.1, e),
            )?;
            Ok(())
        }
    }
    fn chunk(
        &mut self,
        owner: &mut Writer,
        scope: Scope,
        doc: &Document,
        value: usize,
        field: &'static str,
        origin: Option<(&Source, &sse::Event)>,
    ) -> Result<()> {
        string(doc, value)?;
        let base = self.base.as_ref().ok_or(Error::Schema)?;
        self.emitter.emit(
            owner,
            Kind::Chunk,
            scope,
            |e| {
                base_fields(base, e)?;
                e.bytes(b",\"choices\":[{\"index\":0,\"delta\":{")?;
                e.string(field)?;
                e.bytes(b":")?;
                doc.write(value, e)?;
                e.bytes(b"},\"finish_reason\":null}]")
            },
            |e| delta_origin(doc, origin, if field == "content" { "text" } else { "thinking" }, e),
        )?;
        Ok(())
    }
    fn signature(
        &mut self,
        owner: &mut Writer,
        scope: Scope,
        doc: &Document,
        value: usize,
        final_fragment: bool,
        origin: Option<(&Source, &sse::Event)>,
    ) -> Result<()> {
        string(doc, value)?;
        let block = self
            .blocks
            .iter_mut()
            .find(|block| block.scope.block_index == scope.block_index)
            .ok_or(Error::Schema)?;
        block.signature_final = Some(final_fragment);
        self.emitter.emit(
            owner,
            Kind::ReasoningSignature,
            scope,
            |e| {
                e.bytes(b",\"kind\":\"anthropic\",\"fragment\":")?;
                doc.write(value, e)?;
                e.bytes(if final_fragment {
                    b",\"final\":true"
                } else {
                    b",\"final\":false"
                })
            },
            |e| delta_origin(doc, origin, "signature", e),
        )?;
        Ok(())
    }
    fn arguments(
        &mut self,
        owner: &mut Writer,
        index: u64,
        doc: &Document,
        value: usize,
        fragment: bool,
        origin: Option<(&Source, &sse::Event)>,
    ) -> Result<()> {
        let block = self
            .blocks
            .iter_mut()
            .find(|b| b.index == index && b.open)
            .ok_or(Error::Schema)?;
        let (source, parser) = block.arguments.as_mut().ok_or(Error::Schema)?;
        let lease = encode::scratch(owner, |e| {
            if fragment {
                let mut chars = doc.chars(value)?;
                while let Some(c) = chars.next_scalar()? {
                    e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
                }
                Ok(())
            } else {
                doc.write(value, e)
            }
        })?;
        source.push_scratch(lease)?;
        parser.advance(source)?;
        block.fragments = true;
        block.initial = None;
        let scope = block.scope;
        let base = self.base.as_ref().ok_or(Error::Schema)?;
        self.emitter.emit(
            owner,
            Kind::Chunk,
            scope,
            |e| {
                base_fields(base, e)?;
                e.bytes(b",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":")?;
                e.unsigned(scope.tool_index.ok_or(Error::Schema)?)?;
                e.bytes(b",\"function\":{\"arguments\":")?;
                if fragment {
                    doc.write(value, e)?;
                } else {
                    e.quoted_json(|e| doc.write(value, e))?;
                }
                e.bytes(b"}}]},\"finish_reason\":null}]")
            },
            |e| delta_origin(doc, origin, "partial_json", e),
        )?;
        Ok(())
    }
    pub(super) fn block_stop(&mut self, owner: &mut Writer, index: u64, origin: Origin<'_>) -> Result<()> {
        let position = self
            .blocks
            .iter()
            .position(|b| b.index == index && b.open)
            .ok_or(Error::Schema)?;
        if !self.blocks[position].fragments
            && let Some(initial) = self.blocks[position].initial.take()
        {
            self.arguments(owner, index, &initial, initial.root(), false, None)?;
        }
        let block = &mut self.blocks[position];
        if let Some((source, parser)) = &mut block.arguments {
            if block.fragments {
                parser.finish(source)?;
                // Native tool input is an object, as request validation also requires.
                if !matches!(parser.pending_node(source, 0)?.kind, json::Kind::Object) {
                    return Err(Error::Schema);
                }
            }
            self.emitter.emit(
                owner,
                Kind::ToolEnd,
                block.scope,
                |e| {
                    e.bytes(if block.fragments {
                        b",\"status\":\"completed\",\"valid_json\":true"
                    } else {
                        b",\"status\":\"completed\""
                    })
                },
                |_| Ok(()),
            )?;
        }
        if block.signature_final == Some(false) {
            self.emitter.emit(
                owner,
                Kind::ReasoningSignature,
                block.scope,
                |e| e.bytes(b",\"kind\":\"anthropic\",\"fragment\":\"\",\"final\":true"),
                |_| Ok(()),
            )?;
            block.signature_final = Some(true);
        }
        self.emitter.emit(
            owner,
            Kind::BlockEnd,
            block.scope,
            |_| Ok(()),
            |e| event_origin(origin, "content_block_stop", &["type", "index"], e),
        )?;
        block.open = false;
        block.arguments = None;
        block.initial = None;
        Ok(())
    }
}

/// Canonical kind of a native content block.
fn block_kind(doc: &Document, node: usize) -> Result<&'static str> {
    Ok(if is(doc, node, "type", "text")? {
        "text"
    } else if is(doc, node, "type", "thinking")? {
        "thinking"
    } else if is(doc, node, "type", "redacted_thinking")? {
        "redacted"
    } else if is(doc, node, "type", "tool_use")? {
        "function"
    } else if is(doc, node, "type", "server_tool_use")? || is(doc, node, "type", "mcp_tool_use")? {
        "server"
    } else if server_result(doc, node)? {
        "tool_result"
    } else {
        "opaque"
    })
}
