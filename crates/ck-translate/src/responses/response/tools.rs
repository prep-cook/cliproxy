use super::*;

#[derive(Clone, Copy, PartialEq)]
pub(in super::super) enum Server {
    Code,
    Web,
    File,
    Mcp,
}
impl Server {
    pub(in super::super) fn from_item(doc: &Document, node: usize) -> Result<Option<Self>> {
        for (name, kind) in [
            ("code_interpreter_call", Self::Code),
            ("web_search_call", Self::Web),
            ("file_search_call", Self::File),
            ("mcp_call", Self::Mcp),
        ] {
            if is(doc, node, "type", name)? {
                return Ok(Some(kind));
            }
        }
        Ok(None)
    }
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Code => "code_interpreter",
            Self::Web => "web_search",
            Self::File => "file_search",
            Self::Mcp => "mcp",
        }
    }
    pub(in super::super) fn fields(self) -> &'static [&'static str] {
        match self {
            Self::Code => &["outputs"],
            Self::Web => &["action", "results"],
            Self::File => &["results"],
            Self::Mcp => &["output", "error"],
        }
    }
}

impl State {
    pub(super) fn execution_event(&mut self, owner: &mut Writer, doc: &Document) -> Result<bool> {
        let root = doc.root();
        let done = is(doc, root, "type", "response.code_interpreter_call_code.done")?;
        if !done && !is(doc, root, "type", "response.code_interpreter_call_code.delta")? {
            return Ok(false);
        }
        let at = self.find_event(doc)?;
        let code = doc.required(root, if done { "code" } else { "delta" })?;
        self.code(owner, at, doc, code, done)?;
        Ok(true)
    }

    fn code(&mut self, owner: &mut Writer, at: usize, doc: &Document, node: usize, snapshot: bool) -> Result<()> {
        let item = &mut self.items[at];
        if item.server != Some(Server::Code) {
            return Err(Error::Schema);
        }
        string(doc, node)?;
        let fragment = if snapshot {
            suffix(owner, &item.doc, item.code, doc, node)?
        } else {
            super::super::mapped(owner, |e| doc.write(node, e))?
        };
        let empty = fragment.chars(fragment.root())?.next_scalar()?.is_none();
        if item.code_closed || item.closed {
            return if snapshot && empty { Ok(()) } else { Err(Error::Schema) };
        }
        self.emitter.emit(
            owner,
            Kind::CodeExecution,
            item.scope(None),
            |e| {
                e.bytes(b",\"phase\":\"code\",\"value\":")?;
                fragment.write(fragment.root(), e)
            },
            |e| {
                e.bytes(b",\"source\":{\"protocol\":\"responses\",\"event\":")?;
                e.string(if snapshot {
                    "response.code_interpreter_call_code.done"
                } else {
                    "response.code_interpreter_call_code.delta"
                })?;
                e.bytes(b"}")
            },
        )?;
        let imported = item.doc.absorb(fragment)?;
        let root = item.doc.root();
        item.code = Some(append_fragment(&mut item.doc, root, "code", imported)?);
        item.code_closed = snapshot;
        Ok(())
    }

    /// Reconciles a server tool item snapshot: its arguments or code, then its result fields;
    /// `close` publishes the results and ends the tool.
    pub(super) fn server_snapshot(
        &mut self,
        owner: &mut Writer,
        at: usize,
        doc: &Document,
        node: usize,
        close: bool,
    ) -> Result<()> {
        let server = self.items[at].server.ok_or(Error::Schema)?;
        self.server_input(owner, (at, server), doc, node, close)?;
        let results = self.server_results(owner, (at, server), doc, node)?;
        if !close || self.items[at].tool_closed {
            return Ok(());
        }
        self.close_server(owner, (at, server), results, doc, node)
    }

    fn server_input(
        &mut self,
        owner: &mut Writer,
        (at, server): (usize, Server),
        doc: &Document,
        node: usize,
        close: bool,
    ) -> Result<()> {
        if server == Server::Mcp
            && let Some(arguments) = doc.member(node, "arguments")?
        {
            self.arguments(owner, at, doc, arguments, close)?;
        }
        if server == Server::Code
            && let Some(code) = doc.member(node, "code")?
        {
            if matches!(doc.node(code)?.value, Value::Null) {
                if self.items[at].code.is_some() {
                    return Err(Error::Schema);
                }
                let item = &mut self.items[at];
                if !item.code_null {
                    self.emitter.emit(
                        owner,
                        Kind::CodeExecution,
                        item.scope(None),
                        |e| e.bytes(b",\"phase\":\"code\",\"value\":null"),
                        |e| e.bytes(b",\"source\":{\"protocol\":\"responses\"}"),
                    )?;
                    item.code_null = true;
                }
            } else {
                self.code(owner, at, doc, code, close)?;
            }
        }
        Ok(())
    }

    /// Retains changed result fields; a change after the tool closed is a contradiction.
    fn server_results(
        &mut self,
        owner: &mut Writer,
        (at, server): (usize, Server),
        doc: &Document,
        node: usize,
    ) -> Result<usize> {
        let item = &mut self.items[at];
        let results = match item.results {
            Some(results) => results,
            None => {
                let results = item.doc.add(None, Value::Object)?;
                item.results = Some(results);
                results
            }
        };
        for field in server.fields() {
            if let Some(incoming) = doc.member(node, field)? {
                if let Some(previous) = item.doc.member(results, field)?
                    && super::super::snapshot::equal(&item.doc, previous, doc, incoming)?
                {
                    continue;
                }
                if item.tool_closed {
                    return Err(Error::Schema);
                }
                let retained = super::super::mapped(owner, |e| doc.write(incoming, e))?;
                let imported = item.doc.absorb(retained)?;
                let target = item.doc.set(results, field, Value::Null)?;
                item.doc.transplant(target, imported)?;
            }
        }
        Ok(results)
    }

    fn close_server(
        &mut self,
        owner: &mut Writer,
        (at, server): (usize, Server),
        results: usize,
        doc: &Document,
        node: usize,
    ) -> Result<()> {
        let item = &mut self.items[at];
        if server == Server::Mcp && item.arguments.is_some() && !item.arguments_done {
            validate_arguments(owner, item)?;
            item.arguments_done = true;
        }
        if server == Server::Code && item.code.is_some() && !item.code_closed {
            return Err(Error::Truncated);
        }
        for field in server.fields() {
            if let Some(value) = item.doc.member(results, field)? {
                let grounding = server == Server::Web && *field == "action";
                self.emitter.emit(
                    owner,
                    if grounding { Kind::Grounding } else { Kind::ToolResult },
                    item.scope(None),
                    |e| {
                        e.bytes(b",\"value\":")?;
                        item.doc.write(value, e)?;
                        if *field == "error" && !matches!(item.doc.node(value)?.value, Value::Null) {
                            e.bytes(b",\"is_error\":true")?;
                        }
                        Ok(())
                    },
                    |e| {
                        e.bytes(b",\"source\":{\"protocol\":\"responses\"},\"responses_field\":")?;
                        e.string(field)
                    },
                )?;
            }
        }
        self.emitter.emit(
            owner,
            Kind::ToolEnd,
            item.scope(None),
            |e| {
                e.bytes(b",\"status\":")?;
                e.string(
                    if is(doc, node, "status", "incomplete")? || is(doc, node, "status", "failed")? {
                        "incomplete"
                    } else {
                        "completed"
                    },
                )
            },
            |_| Ok(()),
        )?;
        item.tool_closed = true;
        Ok(())
    }
}
