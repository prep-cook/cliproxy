use super::*;

impl State {
    pub(super) fn find_or_add(&mut self, owner: &mut Writer, doc: &Document, node: usize, index: u64) -> Result<usize> {
        if let Some(at) = self.items.iter().position(|item| item.index == index) {
            if let (Some(a), Some(b)) = (
                doc.member(node, "id")?,
                self.items[at].doc.member(self.items[at].doc.root(), "id")?,
            ) && !equal(doc, a, &self.items[at].doc, b)?
            {
                return Err(Error::Schema);
            }
            Ok(at)
        } else {
            self.add_item(owner, doc, node, index)
        }
    }

    /// Opens output item `index` with its native identity, and its tool call when it is one.
    pub(super) fn add_item(&mut self, owner: &mut Writer, doc: &Document, node: usize, index: u64) -> Result<usize> {
        if self.items.iter().any(|item| item.index == index) {
            return Err(Error::Schema);
        }
        let native = super::super::mapped(owner, |e| item_metadata(doc, node, e))?;
        let root = native.root();
        let id = media::local_id(b'i', index)?;
        let server = Server::from_item(&native, root)?;
        let tool = server.is_some()
            || is(&native, root, "type", "function_call")?
            || is(&native, root, "type", "custom_tool_call")?;
        let scope = Scope {
            choice_index: Some(0),
            item_id: Some(id),
            ..Scope::default()
        };
        self.emitter.emit(
            owner,
            Kind::ItemStart,
            scope,
            |e| {
                e.bytes(b",\"kind\":")?;
                if let Some(kind) = native.member(root, "type")? {
                    native.write(kind, e)?;
                } else {
                    e.string("message")?;
                }
                super::super::member(&native, root, "role", e)?;
                e.bytes(b",\"attributes\":")?;
                native.write(root, e)
            },
            |e| {
                e.bytes(b",\"source\":{\"protocol\":\"responses\",\"output_index\":")?;
                e.unsigned(index)?;
                e.bytes(b",\"ids\":{")?;
                let mut first = true;
                for name in ["id", "call_id"] {
                    if let Some(n) = native.member(root, name)? {
                        if !first {
                            e.bytes(b",")?;
                        }
                        first = false;
                        e.string(name)?;
                        e.bytes(b":")?;
                        native.write(n, e)?;
                    }
                }
                e.bytes(b"}}")
            },
        )?;
        let item = Item {
            index,
            id,
            tool: if tool {
                Some(media::local_id(b't', index)?)
            } else {
                None
            },
            doc: native,
            blocks: Table::new(owner)?,
            closed: false,
            arguments: None,
            arguments_done: false,
            tool_closed: false,
            incomplete: false,
            server,
            code: None,
            code_closed: false,
            code_null: false,
            results: None,
            encrypted: false,
            image: None,
        };
        if tool {
            tool_start(&mut self.emitter, owner, &item, server)?;
        }
        let at = self.items.push(item)?;
        self.snapshot_item(owner, at, doc, node, false)?;
        Ok(at)
    }

    /// Reconciles an item snapshot with what was published, and closes the item on `close`.
    pub(super) fn snapshot_item(
        &mut self,
        owner: &mut Writer,
        at: usize,
        doc: &Document,
        node: usize,
        close: bool,
    ) -> Result<()> {
        let item = &self.items[at];
        for name in ["id", "call_id", "type", "name", "namespace", "role"] {
            if let (Some(previous), Some(incoming)) = (item.doc.member(item.doc.root(), name)?, doc.member(node, name)?)
                && !super::super::snapshot::equal(&item.doc, previous, doc, incoming)?
            {
                return Err(Error::Schema);
            }
        }
        if is(doc, node, "type", "image_generation_call")?
            && let Some(data) = doc.member(node, "result")?
            && !matches!(doc.node(data)?.value, Value::Null)
        {
            self.image(owner, at, doc, data, None)?;
        }
        for (name, summary) in [("content", false), ("summary", true)] {
            if let Some(parts) = doc.member(node, name)? {
                let mut supplied = 0;
                for (index, part) in doc.children(parts)?.enumerate() {
                    self.block(owner, at, (index as u64, summary), doc, part, (close, close))?;
                    supplied = index as u64 + 1;
                }
                // A supplied array must hold every published part; omission keeps them.
                if self.items[at]
                    .blocks
                    .iter()
                    .any(|b| b.summary == summary && b.index >= supplied)
                {
                    return Err(Error::Schema);
                }
            }
        }
        let custom = self.items[at].custom()?;
        if self.items[at].server.is_some() {
            self.server_snapshot(owner, at, doc, node, close)?;
        } else if self.items[at].tool.is_some() {
            self.items[at].incomplete = close && is(doc, node, "status", "incomplete")?;
            if let Some(arguments) = doc.member(node, if custom { "input" } else { "arguments" })? {
                self.arguments(owner, at, doc, arguments, close)?;
            } else if close && !self.items[at].tool_closed {
                return Err(Error::Truncated);
            }
        }
        self.encrypted(owner, at, doc, node, close)?;
        if close && !self.items[at].closed {
            self.close_item(owner, at, doc, node)?;
        }
        Ok(())
    }

    /// Reconciles `encrypted_content`. A value before the item closes is provisional, since
    /// `output_item.added` may carry an incomplete one: at close the snapshot's value, else the
    /// provisional one, is published once as its own block, and later snapshots must equal it.
    /// Null publishes nothing and stays a native attribute (`item_metadata`).
    fn encrypted(&mut self, owner: &mut Writer, at: usize, doc: &Document, node: usize, close: bool) -> Result<()> {
        let incoming = doc.member(node, "encrypted_content")?;
        let item = &mut self.items[at];
        let root = item.doc.root();
        if item.encrypted {
            let published = item.doc.required(root, "encrypted_content")?;
            return match incoming {
                Some(value) if !equal(&item.doc, published, doc, value)? => Err(Error::Schema),
                _ => Ok(()),
            };
        }
        if let Some(value) = incoming {
            if let Some(provisional) = item.doc.member(root, "encrypted_content")? {
                item.doc.detach(root, provisional)?;
            }
            if !matches!(doc.node(value)?.value, Value::Null) {
                string(doc, value)?;
                if item.closed {
                    return Err(Error::Schema);
                }
                let lease = encode::scratch(owner, |e| doc.write(value, e))?;
                let imported = item.doc.import_json(owner, lease)?;
                item.doc.rename(imported, "encrypted_content")?;
                item.doc.append(root, imported)?;
            }
        }
        let Some(value) = item.doc.member(root, "encrypted_content")?.filter(|_| close) else {
            return Ok(());
        };
        let scope = item.scope(Some(u64::MAX));
        self.emitter.emit(
            owner,
            Kind::BlockStart,
            scope,
            |e| e.bytes(b",\"kind\":\"thinking\",\"attributes\":{\"type\":\"encrypted\"}"),
            |_| Ok(()),
        )?;
        self.emitter.emit(
            owner,
            Kind::ReasoningSignature,
            scope,
            |e| {
                e.bytes(b",\"kind\":\"responses_encrypted\",\"fragment\":")?;
                item.doc.write(value, e)?;
                e.bytes(b",\"final\":true")
            },
            |_| Ok(()),
        )?;
        self.emitter
            .emit(owner, Kind::BlockEnd, scope, |_| Ok(()), |_| Ok(()))?;
        item.encrypted = true;
        Ok(())
    }

    fn close_item(&mut self, owner: &mut Writer, at: usize, doc: &Document, node: usize) -> Result<()> {
        let item = &mut self.items[at];
        for block in item.blocks.iter() {
            if !block.closed {
                return Err(Error::Truncated);
            }
        }
        self.emitter.emit(
            owner,
            Kind::ItemEnd,
            item.scope(None),
            |e| {
                e.bytes(b",\"status\":")?;
                e.string(if is(doc, node, "status", "incomplete")? {
                    "incomplete"
                } else {
                    "completed"
                })?;
                e.bytes(b",\"attributes\":")?;
                item_metadata(doc, node, e)
            },
            |_| Ok(()),
        )?;
        item.closed = true;
        Ok(())
    }

    pub(super) fn image(
        &mut self,
        owner: &mut Writer,
        at: usize,
        doc: &Document,
        data: usize,
        partial: Option<u64>,
    ) -> Result<()> {
        let item = &self.items[at];
        if !is(&item.doc, item.doc.root(), "type", "image_generation_call")? || (partial.is_some() && item.closed) {
            return Err(Error::Schema);
        }
        if partial.is_none()
            && let Some(previous) = self.items[at].image
        {
            return if previous == super::super::image::digest(doc, data)? {
                Ok(())
            } else {
                Err(Error::Schema)
            };
        }
        let id = media::local_id(b'm', self.next_media)?;
        self.next_media = self.next_media.checked_add(1).ok_or(Error::Limit)?;
        let digest = super::super::image::publish(
            owner,
            &mut self.emitter,
            doc,
            data,
            self.items[at].scope(None),
            id,
            partial,
        )?;
        if partial.is_none() {
            self.items[at].image = Some(digest);
        }
        Ok(())
    }
}

fn tool_start(emitter: &mut Emitter, owner: &mut Writer, item: &Item, server: Option<Server>) -> Result<()> {
    let root = item.doc.root();
    let custom = item.custom()?;
    emitter.emit(
        owner,
        Kind::ToolStart,
        item.scope(None),
        |e| {
            e.bytes(b",\"name\":")?;
            if let Some(name) = item.doc.member(root, "name")? {
                item.doc.write(name, e)?;
            } else if let Some(server) = server {
                e.string(server.name())?;
            } else {
                return Err(Error::Schema);
            }
            e.bytes(b",\"kind\":")?;
            e.string(if server.is_some() {
                "server"
            } else if custom {
                "custom"
            } else {
                "function"
            })?;
            e.bytes(b",\"arguments_format\":")?;
            e.string(if custom { "text" } else { "json" })?;
            super::super::member(&item.doc, root, "namespace", e)
        },
        |e| {
            e.bytes(b",\"source\":{\"protocol\":\"responses\",\"ids\":{")?;
            let mut first = true;
            for name in ["id", "call_id"] {
                if let Some(n) = item.doc.member(root, name)? {
                    if !first {
                        e.bytes(b",")?;
                    }
                    first = false;
                    e.string(name)?;
                    e.bytes(b":")?;
                    item.doc.write(n, e)?;
                }
            }
            e.bytes(b"}}")
        },
    )?;
    Ok(())
}

fn item_metadata(doc: &Document, node: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    // Members the item's type consumes; any other member stays a native attribute.
    let consumed: &[&str] = match Server::from_item(doc, node)? {
        Some(Server::Code) => &["arguments", "input", "outputs", "code"],
        Some(Server::Web) => &["arguments", "input", "action", "results"],
        Some(Server::File) => &["arguments", "input", "results"],
        Some(Server::Mcp) => &["arguments", "input", "output", "error"],
        None if is(doc, node, "type", "function_call")? => &["arguments"],
        None if is(doc, node, "type", "custom_tool_call")? => &["input"],
        None if is(doc, node, "type", "image_generation_call")? => &["result"],
        None => &[],
    };
    // A null `encrypted_content` publishes no block, so it stays an attribute.
    let null = match doc.member(node, "encrypted_content")? {
        Some(value) => matches!(doc.node(value)?.value, Value::Null),
        None => false,
    };
    let mut kept = ["content", "summary", "encrypted_content", "", "", "", ""];
    let count = if null { 2 } else { 3 };
    kept[count..count + consumed.len()].copy_from_slice(consumed);
    object(doc, node, &kept[..count + consumed.len()], e)
}
