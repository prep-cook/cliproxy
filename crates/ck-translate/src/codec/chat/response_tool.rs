use super::*;

impl State {
    pub(super) fn legacy_tool(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        function: usize,
        position: usize,
    ) -> Result<()> {
        let synthetic = media::local_id(b'l', self.choices[position].index)?;
        let call = mapped_document(owner, |e| {
            e.bytes(b"{\"index\":0,\"id\":")?;
            e.string(synthetic.as_str())?;
            e.bytes(b",\"type\":\"function\",\"function\":")?;
            doc.write(function, e)?;
            e.bytes(b"}")
        })?;
        self.tool(owner, &call, call.root(), position, None, Some((doc, function)))
    }
    pub(super) fn tool(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        call: usize,
        position: usize,
        buffered_index: Option<u64>,
        original: Option<(&Document, usize)>,
    ) -> Result<()> {
        let legacy = original.is_some();
        let index = if let Some(node) = doc.member(call, "index")? {
            doc.unsigned(node)?
        } else {
            buffered_index.ok_or(Error::Schema)?
        };
        let existing = self.choices[position].tools.iter().position(|tool| tool.index == index);
        let tool_position = if let Some(existing) = existing {
            existing
        } else {
            self.open_tool(owner, doc, call, position, index, legacy)?
        };
        let mut scope = self.scope(position);
        let tool = &self.choices[position].tools[tool_position];
        if tool.legacy != legacy {
            return Err(Error::Schema);
        }
        scope.tool_index = Some(index);
        scope.tool_call_id = Some(tool.id);
        let custom = tool.custom;
        let function = same_identity(doc, call, tool)?;
        if let Some(function) = function {
            if let Some(arguments) = doc.member(function, if custom { "input" } else { "arguments" })? {
                self.tool_arguments(owner, doc, arguments, scope, (position, tool_position))?;
            }
            self.function_extensions(owner, doc, function, custom, scope, original)?;
        }
        self.call_extensions(owner, doc, call, scope)
    }

    /// Registers a call index seen for the first time and publishes its start.
    fn open_tool(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        call: usize,
        position: usize,
        index: u64,
        legacy: bool,
    ) -> Result<usize> {
        let custom = doc
            .member(call, "type")?
            .map(|n| doc.string_is(n, "custom"))
            .transpose()?
            .unwrap_or(false);
        let function = doc.required(call, if custom { "custom" } else { "function" })?;
        let name = doc.required(function, "name")?;
        let call_id = doc.required(call, "id")?;
        let metadata = mapped_document(owner, |e| {
            e.bytes(b"{\"name\":")?;
            doc.write(name, e)?;
            e.bytes(b",\"id\":")?;
            doc.write(call_id, e)?;
            e.bytes(b",\"custom\":")?;
            e.bytes(if custom { b"true" } else { b"false" })?;
            e.bytes(b"}")
        })?;
        let id = media::local_id(b't', self.serial)?;
        self.serial += 1;
        let mut scope = self.scope(position);
        scope.tool_index = Some(index);
        scope.tool_call_id = Some(id);
        self.emitter.emit(
            owner,
            Kind::ToolStart,
            scope,
            |e| {
                e.bytes(b",\"name\":")?;
                doc.write(name, e)?;
                e.bytes(if custom {
                    b",\"kind\":\"custom\",\"arguments_format\":\"text\""
                } else {
                    b",\"kind\":\"function\",\"arguments_format\":\"json\""
                })
            },
            |e| {
                e.bytes(b",\"source\":{\"protocol\":\"chat\",\"ids\":{\"id\":")?;
                doc.write(call_id, e)?;
                e.bytes(b"}}")?;
                if legacy {
                    e.bytes(b",\"chat_legacy\":true")?;
                }
                Ok(())
            },
        )?;
        self.choices[position].tools.push(Tool {
            index,
            id,
            metadata,
            arguments: None,
            parser: None,
            custom,
            legacy,
        })
    }

    /// Publishes an arguments fragment and feeds a function call's JSON parser.
    fn tool_arguments(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        arguments: usize,
        scope: Scope,
        (position, tool_position): (usize, usize),
    ) -> Result<()> {
        let Value::String(text) = doc.node(arguments)?.value else {
            return Err(Error::Schema);
        };
        self.emitter.emit(
            owner,
            Kind::Chunk,
            scope,
            |e| {
                base(self.base.as_ref().ok_or(Error::Schema)?, e)?;
                e.bytes(b",\"choices\":[{\"index\":")?;
                e.unsigned(scope.choice_index.ok_or(Error::Schema)?)?;
                e.bytes(b",\"delta\":{\"tool_calls\":[{\"index\":")?;
                e.unsigned(scope.tool_index.ok_or(Error::Schema)?)?;
                e.bytes(b",\"function\":{\"arguments\":")?;
                doc.write(arguments, e)?;
                e.bytes(b"}}]},\"finish_reason\":null}]")
            },
            |_| Ok(()),
        )?;
        let tool = &mut self.choices[position].tools[tool_position];
        if !tool.custom {
            if tool.arguments.is_none() {
                tool.arguments = Some(Source::new(owner)?);
                tool.parser = Some(json::Scanner::new(owner, u64::MAX)?);
            }
            let length = text.span.len();
            if length > 2 {
                let lease = encode::scratch(owner, |e| {
                    let mut decoded = doc.chars(arguments)?;
                    while let Some(c) = decoded.next_scalar()? {
                        e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
                    }
                    Ok(())
                })?;
                tool.arguments.as_mut().ok_or(Error::Schema)?.push_scratch(lease)?;
            }
            tool.parser
                .as_mut()
                .ok_or(Error::Schema)?
                .advance(tool.arguments.as_ref().ok_or(Error::Schema)?)?;
        }
        Ok(())
    }

    /// Preserves function members the codec does not map, at their native location.
    fn function_extensions(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        function: usize,
        custom: bool,
        scope: Scope,
        original: Option<(&Document, usize)>,
    ) -> Result<()> {
        for (ordinal, field) in doc.children(function)?.enumerate() {
            let name = doc.node(field)?.name.ok_or(Error::Schema)?;
            if doc.name_is(name, "name")? || doc.name_is(name, if custom { "input" } else { "arguments" })? {
                continue;
            }
            let (source, field) = if let Some((source, function)) = original {
                (source, source.children(function)?.nth(ordinal).ok_or(Error::Schema)?)
            } else {
                (doc, field)
            };
            preserved_tool_field(
                &mut self.emitter,
                owner,
                self.base.as_ref().ok_or(Error::Schema)?,
                source,
                field,
                scope,
                true,
            )?;
        }
        Ok(())
    }

    /// Preserves call members the codec does not map.
    fn call_extensions(&mut self, owner: &mut Writer, doc: &Document, call: usize, scope: Scope) -> Result<()> {
        for field in doc.children(call)? {
            let name = doc.node(field)?.name.ok_or(Error::Schema)?;
            let mut known = false;
            for key in ["index", "id", "type", "function", "custom"] {
                known |= doc.name_is(name, key)?;
            }
            if !known {
                preserved_tool_field(
                    &mut self.emitter,
                    owner,
                    self.base.as_ref().ok_or(Error::Schema)?,
                    doc,
                    field,
                    scope,
                    false,
                )?;
            }
        }
        Ok(())
    }
}

/// Rejects a later delta whose id, type or name differs from the call's first delta, checking
/// id and type before reading its function or custom member, which it returns.
fn same_identity(doc: &Document, call: usize, tool: &Tool) -> Result<Option<usize>> {
    if let Some(id) = doc.member(call, "id")?
        && !equal(
            doc,
            id,
            &tool.metadata,
            tool.metadata.required(tool.metadata.root(), "id")?,
        )?
    {
        return Err(Error::Schema);
    }
    if let Some(kind) = doc.member(call, "type")?
        && !doc.string_is(kind, if tool.custom { "custom" } else { "function" })?
    {
        return Err(Error::Schema);
    }
    let function = doc.member(call, if tool.custom { "custom" } else { "function" })?;
    if let Some(function) = function
        && let Some(name) = doc.member(function, "name")?
        && !equal(
            doc,
            name,
            &tool.metadata,
            tool.metadata.required(tool.metadata.root(), "name")?,
        )?
    {
        return Err(Error::Schema);
    }
    Ok(function)
}

fn preserved_tool_field(
    emitter: &mut Emitter,
    owner: &mut Writer,
    metadata: &Document,
    doc: &Document,
    field: usize,
    scope: Scope,
    inner: bool,
) -> Result<()> {
    emitter.emit(
        owner,
        Kind::Chunk,
        scope,
        |e| {
            base(metadata, e)?;
            e.bytes(b",\"choices\":[{\"index\":")?;
            e.unsigned(scope.choice_index.ok_or(Error::Schema)?)?;
            e.bytes(b",\"delta\":{\"tool_calls\":[{\"index\":")?;
            e.unsigned(scope.tool_index.ok_or(Error::Schema)?)?;
            if inner {
                e.bytes(b",\"function\":{}")?;
            }
            e.bytes(b"}]},\"finish_reason\":null}]")
        },
        |e| super::super::provenance::one(doc, field, e),
    )?;
    Ok(())
}
