use super::*;

/// Top-level fields that are rewritten after import, so they are never streamed or published as
/// they arrive.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Alias {
    XCk,
    Functions,
    FunctionCall,
    Reasoning,
    ReasoningEffort,
}

impl Alias {
    const ALL: [Self; 5] = [
        Self::XCk,
        Self::Functions,
        Self::FunctionCall,
        Self::Reasoning,
        Self::ReasoningEffort,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::XCk => "x_ck",
            Self::Functions => "functions",
            Self::FunctionCall => "function_call",
            Self::Reasoning => "reasoning",
            Self::ReasoningEffort => "reasoning_effort",
        }
    }

    /// The order in which the long-string streaming exclusion compares wire keys.
    const STRING_ORDER: [Self; 5] = [
        Self::ReasoningEffort,
        Self::Reasoning,
        Self::Functions,
        Self::FunctionCall,
        Self::XCk,
    ];

    /// Classifies a wire key against `candidates` in order; `key_is` reads a retained key from
    /// the document, not the input.
    fn of_key(
        scanner: &json::Scanner,
        doc: &Document,
        key: codec::source::Span,
        candidates: [Self; 5],
    ) -> Result<Option<Self>> {
        for alias in candidates {
            if scanner.key_is(doc, key, alias.name())? {
                return Ok(Some(alias));
            }
        }
        Ok(None)
    }

    fn of_name(doc: &Document, name: Name) -> Result<Option<Self>> {
        for alias in Self::ALL {
            if doc.name_is(name, alias.name())? {
                return Ok(Some(alias));
            }
        }
        Ok(None)
    }
}

impl Ingress {
    pub(super) fn consume(&mut self, owner: &mut Writer) -> Result<()> {
        let scanner = self.scanner.as_ref().ok_or(Error::Closed)?;
        if scanner.nodes().is_empty() {
            return Ok(());
        }
        let root = scanner.pending_node(self.document.input(), 0)?;
        if root.kind != json::Kind::Object {
            return Err(Error::Schema);
        }
        if !self.started {
            self.start(owner)?;
        }
        loop {
            let scanner = self.scanner.as_ref().ok_or(Error::Closed)?;
            let next = match self.last_field {
                Some(last) => scanner.pending_node(self.document.input(), last)?.next,
                None => root.first,
            };
            let Some(index) = next else { break };
            let wire = scanner.pending_node(self.document.input(), index)?;
            let key = wire.key.ok_or(Error::Schema)?;
            let done = if self.messages.as_ref().is_some_and(|m| m.wire == index)
                || scanner.key_is(&self.document, key, "messages")?
            {
                self.consume_messages_field(owner, index, wire, key)?
            } else if let Some(done) = self.consume_open_field(owner, index, wire, key)? {
                done
            } else {
                self.consume_completed_field(owner, index, key)?
            };
            if !done {
                break;
            }
            self.last_field = Some(index);
        }
        Ok(())
    }

    fn start(&mut self, owner: &mut Writer) -> Result<()> {
        self.emitter.request_start(
            owner,
            RequestView {
                document: &self.document,
                media: &self.media,
                operation: self.operation,
            },
        )?;
        let (kind, _) = codec::internal::next(owner, &mut self.reader)?;
        if kind != Kind::RequestStart {
            return Err(Error::Schema);
        }
        let root = self.document.root();
        field(
            &mut self.document,
            &self.media,
            root,
            &[],
            &mut self.emitter,
            owner,
            &mut self.reader,
        )?;
        self.started = true;
        Ok(())
    }

    /// Returns whether the messages array has ended.
    fn consume_messages_field(
        &mut self,
        owner: &mut Writer,
        index: usize,
        wire: json::Node,
        key: codec::source::Span,
    ) -> Result<bool> {
        if wire.kind != json::Kind::Array {
            return Err(Error::Schema);
        }
        if self.messages.is_none() {
            self.scanner
                .as_mut()
                .ok_or(Error::Closed)?
                .bind_key(key, Name::Static("messages"));
            let node = self.document.set(self.document.root(), "messages", Value::Array)?;
            field(
                &mut self.document,
                &self.media,
                node,
                &[Some(Component::Name(Name::Static("messages")))],
                &mut self.emitter,
                owner,
                &mut self.reader,
            )?;
            self.messages = Some(PendingMessages {
                wire: index,
                node,
                last: None,
                index: 0,
            });
        }
        self.consume_messages(owner)?;
        let scanner = self.scanner.as_ref().ok_or(Error::Closed)?;
        Ok(scanner.completed_node(self.document.input(), index)?.is_some())
    }

    /// Streams a long string or an open container that is not an alias. Returns `Some(done)`
    /// when it took the field, or `None` when the field must wait until it is complete.
    fn consume_open_field(
        &mut self,
        owner: &mut Writer,
        index: usize,
        wire: json::Node,
        key: codec::source::Span,
    ) -> Result<Option<bool>> {
        let scanner = self.scanner.as_ref().ok_or(Error::Closed)?;
        if self.string.as_ref().is_some_and(|s| s.wire == index)
            || (wire.kind == json::Kind::String
                && wire.span.end == 0
                && wire.validated_end - wire.span.start > 1024
                && Alias::of_key(scanner, &self.document, key, Alias::STRING_ORDER)?.is_none())
        {
            let name = self
                .string
                .as_ref()
                .map(|s| self.document.node(s.node).map(|n| n.name))
                .transpose()?
                .flatten()
                .unwrap_or(Name::Mapped(codec::document::Text { source: 0, span: key }));
            let path = [Some(Component::Name(name))];
            return Ok(Some(
                self.stream_string(owner, index, self.document.root(), &path, true)?.1,
            ));
        }
        if Alias::of_key(scanner, &self.document, key, Alias::ALL)?.is_none()
            && (wire.span.end == 0 || self.trees.get(index).is_some_and(Option::is_some))
        {
            let name = if let Some(state) = self.trees.get(index).and_then(|s| *s) {
                self.document.node(state.node)?.name.ok_or(Error::Schema)?
            } else {
                Name::Mapped(codec::document::Text { source: 0, span: key })
            };
            let mut path = [None; 128];
            path[0] = Some(Component::Name(name));
            return Ok(Some(self.consume_tree(
                owner,
                index,
                self.document.root(),
                &mut path,
                1,
                true,
            )?));
        }
        Ok(None)
    }

    /// Imports a completed field; returns false while it is still incomplete.
    fn consume_completed_field(&mut self, owner: &mut Writer, index: usize, key: codec::source::Span) -> Result<bool> {
        let scanner = self.scanner.as_ref().ok_or(Error::Closed)?;
        if scanner.completed_node(self.document.input(), index)?.is_none() {
            return Ok(false);
        }
        let node = self.document.import_completed(scanner, index)?;
        let name = self.document.node(node)?.name.ok_or(Error::Schema)?;
        let alias = Alias::of_name(&self.document, name)?;
        if let Some(alias) = alias {
            self.scanner
                .as_mut()
                .ok_or(Error::Closed)?
                .bind_key(key, Name::Static(alias.name()));
        }
        if alias == Some(Alias::XCk) {
            let x = self.document.required(self.document.root(), "x_ck")?;
            let original = self.document.required(x, "original")?;
            original_field(&mut self.document, original, "/x_ck", node)?;
        } else {
            self.append_field(owner, node, name, alias)?;
        }
        Ok(true)
    }

    /// Adds a completed field to the root, normalizes aliases and publishes the result.
    fn append_field(&mut self, owner: &mut Writer, node: usize, name: Name, alias: Option<Alias>) -> Result<()> {
        for existing in self.document.children(self.document.root())? {
            if self
                .document
                .names_equal(name, self.document.node(existing)?.name.ok_or(Error::Schema)?)?
            {
                return Err(Error::Schema);
            }
        }
        self.document.append(self.document.root(), node)?;
        if alias == Some(Alias::Functions) {
            let mut next = self.document.node(node)?.first;
            while let Some(function) = next {
                next = self.document.node(function)?.next;
                super::super::request_controls::cache(&mut self.document, owner, function)?;
            }
        }
        normalize_options(&mut self.document)?;
        let published = match alias {
            Some(Alias::Functions) => Some(self.document.required(self.document.root(), "tools")?),
            Some(Alias::FunctionCall) => Some(self.document.required(self.document.root(), "tool_choice")?),
            Some(Alias::Reasoning | Alias::ReasoningEffort) => None,
            Some(Alias::XCk) | None => Some(node),
        };
        if let Some(node) = published {
            let mut path = [None; 128];
            path[0] = Some(Component::Name(self.document.node(node)?.name.ok_or(Error::Schema)?));
            fields(
                &mut self.document,
                &self.media,
                node,
                (&mut path, 1),
                &mut self.emitter,
                owner,
                &mut self.reader,
            )?;
        }
        Ok(())
    }
}

fn normalize_options(doc: &mut Document) -> Result<()> {
    let root = doc.root();
    let x = doc.required(root, "x_ck")?;
    if let Some(reasoning) = doc.remove(root, "reasoning_effort")? {
        let thinking = match doc.member(x, "thinking")? {
            Some(i) => i,
            None => doc.set(x, "thinking", Value::Object)?,
        };
        doc.rename(reasoning, "effort")?;
        doc.append(thinking, reasoning)?;
    }
    if let Some(reasoning) = doc.remove(root, "reasoning")? {
        let thinking = match doc.member(x, "thinking")? {
            Some(i) => i,
            None => doc.set(x, "thinking", Value::Object)?,
        };
        doc.rename(reasoning, "native")?;
        doc.append(thinking, reasoning)?;
    }
    if let Some(functions) = doc.remove(root, "functions")? {
        if doc.member(root, "tools")?.is_some() {
            return Err(Error::Schema);
        }
        require_array(doc, functions)?;
        let tools = doc.set(root, "tools", Value::Array)?;
        while let Some(function) = doc.node(functions)?.first {
            doc.detach(functions, function)?;
            let wrapper = doc.add(None, Value::Object)?;
            doc.set(wrapper, "type", Value::Text("function"))?;
            doc.rename(function, "function")?;
            doc.append(wrapper, function)?;
            doc.append(tools, wrapper)?;
        }
        doc.set(x, "chat_legacy_functions", Value::Bool(true))?;
    }
    if let Some(choice) = doc.remove(root, "function_call")? {
        if doc.member(root, "tool_choice")?.is_some() {
            return Err(Error::Schema);
        }
        doc.rename(choice, "tool_choice")?;
        doc.append(root, choice)?;
        if matches!(doc.node(choice)?.value, Value::Object) {
            let function = doc.add(Some(Name::Static("function")), Value::Object)?;
            if let Some(name) = doc.remove(choice, "name")? {
                doc.append(function, name)?;
            }
            doc.set(choice, "type", Value::Text("function"))?;
            doc.append(choice, function)?;
        }
        doc.set(x, "chat_legacy_choice", Value::Bool(true))?;
    }
    Ok(())
}

fn original_field(doc: &mut Document, original: usize, pointer: &'static str, value: usize) -> Result<()> {
    let entry = doc.add(None, Value::Object)?;
    doc.append(original, entry)?;
    doc.bind_origin(entry, doc.root(), None)?;
    doc.set(entry, "protocol", Value::Text("chat"))?;
    doc.set(entry, "pointer", Value::Text(pointer))?;
    doc.rename(value, "value")?;
    doc.append(entry, value)
}
