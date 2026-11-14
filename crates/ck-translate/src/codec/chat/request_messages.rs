use super::super::request_content::normalize_part;
use super::*;

impl Ingress {
    pub(super) fn consume_messages(&mut self, owner: &mut Writer) -> Result<()> {
        loop {
            let pending = self.messages.as_ref().ok_or(Error::Schema)?;
            let scanner = self.scanner.as_ref().ok_or(Error::Closed)?;
            let next = match pending.last {
                Some(last) => scanner.pending_node(self.document.input(), last)?.next,
                None => scanner.pending_node(self.document.input(), pending.wire)?.first,
            };
            let Some(index) = next else { break };
            let wire = scanner.pending_node(self.document.input(), index)?;
            if wire.kind != json::Kind::Object {
                return Err(Error::Schema);
            }
            let message_index = pending.index;
            let mut path = [None; 128];
            path[0] = Some(Component::Name(Name::Static("messages")));
            path[1] = Some(Component::Index(message_index));
            if self.current_message.is_none() {
                let node = self.document.add(None, Value::Object)?;
                self.document.append(pending.node, node)?;
                field(
                    &mut self.document,
                    &self.media,
                    node,
                    &path[..2],
                    &mut self.emitter,
                    owner,
                    &mut self.reader,
                )?;
                self.current_message = Some(PendingMessage {
                    wire: index,
                    node,
                    last: None,
                });
            }
            if !self.consume_members(owner, &mut path)? {
                return Ok(());
            }
            if wire.span.end == 0 {
                break;
            }
            self.finish_message(owner, index, message_index, &mut path)?;
        }
        Ok(())
    }

    /// Consumes the current message's available members; false means the input ran out inside one.
    fn consume_members(&mut self, owner: &mut Writer, path: &mut [Option<Component>; 128]) -> Result<bool> {
        loop {
            let current = self.current_message.as_ref().ok_or(Error::Schema)?;
            let scanner = self.scanner.as_ref().ok_or(Error::Closed)?;
            let next = match current.last {
                Some(last) => scanner.pending_node(self.document.input(), last)?.next,
                None => scanner.pending_node(self.document.input(), current.wire)?.first,
            };
            let Some(child) = next else { break };
            let value = scanner.pending_node(self.document.input(), child)?;
            let key = value.key.ok_or(Error::Schema)?;
            let existing = self
                .string
                .as_ref()
                .filter(|s| s.wire == child)
                .map(|s| s.node)
                .or_else(|| self.trees.get(child).and_then(|s| s.map(|s| s.node)));
            let name = existing
                .map(|node| self.document.node(node).map(|n| n.name))
                .transpose()?
                .flatten()
                .unwrap_or(Name::Mapped(codec::document::Text { source: 0, span: key }));
            path[2] = Some(Component::Name(name));
            if self.string.as_ref().is_some_and(|s| s.wire == child)
                || (value.kind == json::Kind::String
                    && value.span.end == 0
                    && value.validated_end - value.span.start > 1024)
            {
                if !self.stream_string(owner, child, current.node, &path[..3], true)?.1 {
                    return Ok(false);
                }
            } else {
                if value.span.end == 0 || self.trees.get(child).is_some_and(Option::is_some) {
                    let deferred = self.document.name_is(name, "tool_calls")?
                        || self.document.name_is(name, "function_call")?
                        || (self.document.name_is(name, "content")? && value.kind == json::Kind::Array);
                    if !self.consume_tree(owner, child, current.node, path, 3, !deferred)? {
                        return Ok(false);
                    }
                    self.current_message.as_mut().ok_or(Error::Schema)?.last = Some(child);
                    continue;
                }
                if value.span.end == 0 {
                    return Ok(false);
                }
                let node = self.document.import_completed(scanner, child)?;
                self.document.append(current.node, node)?;
                let deferred = self.document.name_is(name, "tool_calls")?
                    || self.document.name_is(name, "function_call")?
                    || (self.document.name_is(name, "content")? && value.kind == json::Kind::Array);
                if !deferred {
                    fields(
                        &mut self.document,
                        &self.media,
                        node,
                        (path, 3),
                        &mut self.emitter,
                        owner,
                        &mut self.reader,
                    )?;
                }
            }
            self.current_message.as_mut().ok_or(Error::Schema)?.last = Some(child);
        }
        Ok(true)
    }

    fn finish_message(
        &mut self,
        owner: &mut Writer,
        index: usize,
        message_index: u64,
        path: &mut [Option<Component>; 128],
    ) -> Result<()> {
        let current = self.current_message.take().ok_or(Error::Schema)?;
        self.media
            .validate_registrations(&self.document, self.relocated_media)?;
        normalize_message(&mut self.document, current.node, message_index, &mut self.media, owner)?;
        self.media.relocate_ingress(&self.document, self.relocated_media)?;
        self.relocated_media = self.media.len();
        fields(
            &mut self.document,
            &self.media,
            current.node,
            (path, 2),
            &mut self.emitter,
            owner,
            &mut self.reader,
        )?;
        while self.published_media < self.media.len() {
            let index = self.published_media;
            media::publish(
                &self.document,
                self.media.at(index),
                &mut self.emitter,
                owner,
                Scope::default(),
            )?;
            self.media.retain(&mut self.document, index, owner, &mut self.reader)?;
            self.published_media += 1;
        }
        let pending = self.messages.as_mut().ok_or(Error::Schema)?;
        pending.last = Some(index);
        pending.index += 1;
        Ok(())
    }
}

pub(super) fn validate(doc: &Document) -> Result<()> {
    let root = doc.root();
    require_string(doc, doc.required(root, "model")?)?;
    let messages = doc.required(root, "messages")?;
    require_array(doc, messages)?;
    doc.with_table(|calls| validate_messages(doc, messages, calls))
}

/// Tool call IDs by hash, then message ordinal, then node: one sorted index replaces a history
/// rescan for every tool result.
type CallIndex = crate::codec::table::Table<(u64, u64, usize)>;
use std::collections::hash_map::RandomState;

fn index_calls(doc: &Document, messages: usize, calls: &mut CallIndex, hasher: &RandomState) -> Result<()> {
    for (ordinal, message) in doc.children(messages)?.enumerate() {
        if let Some(list) = doc.member(message, "tool_calls")?
            && matches!(doc.node(list)?.value, Value::Array)
        {
            for call in doc.children(list)? {
                if let Some(id) = doc.member(call, "id")? {
                    calls.push((string_hash(doc, id, hasher)?, ordinal as u64, id))?;
                }
            }
        }
    }
    calls.sort_unstable();
    Ok(())
}

/// Whether a tool call in a message before `ordinal` has the ID `result`.
fn prior_call(doc: &Document, calls: &CallIndex, hasher: &RandomState, result: usize, ordinal: u64) -> Result<bool> {
    let hash = string_hash(doc, result, hasher)?;
    let first = calls.partition_point(|&(h, _, _)| h < hash);
    for &(_, _, id) in calls[first..]
        .iter()
        .take_while(|&&(h, call_ordinal, _)| h == hash && call_ordinal < ordinal)
    {
        if doc.equal_strings(result, id)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn validate_messages(doc: &Document, messages: usize, calls: &mut CallIndex) -> Result<()> {
    let hasher = RandomState::new();
    index_calls(doc, messages, calls, &hasher)?;
    for (ordinal, message) in doc.children(messages)?.enumerate() {
        let role = doc.required(message, "role")?;
        let mut accepted = false;
        for value in ["system", "developer", "user", "assistant", "tool", "function"] {
            accepted |= doc.string_is(role, value)?;
        }
        if !accepted {
            return Err(Error::Unsupported);
        }
        if let Some(content) = doc.member(message, "content")? {
            match doc.node(content)?.value {
                Value::String(_) | Value::Text(_) | Value::Utf8Media | Value::Null => (),
                Value::Array => {
                    for part in doc.children(content)? {
                        let kind = doc.required(part, "type")?;
                        let mut supported = false;
                        for name in [
                            "text",
                            "reasoning",
                            "reasoning_summary",
                            "refusal",
                            "image",
                            "audio",
                            "video",
                            "document",
                            "tool_result",
                            "opaque",
                            "redacted",
                            "encrypted",
                        ] {
                            supported |= doc.string_is(kind, name)?;
                        }
                        if !supported {
                            return Err(Error::Unsupported);
                        }
                    }
                }
                _ => return Err(Error::Schema),
            }
        }
        if let Some(calls) = doc.member(message, "tool_calls")? {
            require_array(doc, calls)?;
            for call in doc.children(calls)? {
                require_string(doc, doc.required(call, "id")?)?;
                let function = doc.required(call, "function")?;
                require_string(doc, doc.required(function, "name")?)?;
                let format = doc.required(call, "arguments_format")?;
                if !doc.string_is(format, "json")? && !doc.string_is(format, "text")? {
                    return Err(Error::Schema);
                }
            }
        }
        if doc.string_is(role, "tool")? {
            let result_id = doc.required(message, "tool_call_id")?;
            if !prior_call(doc, calls, &hasher, result_id, ordinal as u64)? {
                return Err(Error::Schema);
            }
        }
    }
    Ok(())
}

fn string_hash(doc: &Document, node: usize, state: &impl std::hash::BuildHasher) -> Result<u64> {
    use std::hash::Hasher;
    let mut hasher = state.build_hasher();
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        hasher.write_u32(c as u32);
    }
    Ok(hasher.finish())
}

fn normalize_message(
    doc: &mut Document,
    m: usize,
    current_message: u64,
    registry: &mut Registry,
    owner: &mut Writer,
) -> Result<()> {
    let x = doc.required(doc.root(), "x_ck")?;
    super::super::request_controls::message(doc, owner, m, current_message)?;
    if let Some(content) = doc.member(m, "content")?
        && matches!(doc.node(content)?.value, Value::Array)
    {
        let mut part = doc.node(content)?.first;
        while let Some(p) = part {
            part = doc.node(p)?.next;
            normalize_part(doc, registry, owner, p)?;
        }
    }
    if let Some(function) = doc.remove(m, "function_call")? {
        if doc.member(m, "tool_calls")?.is_some() {
            return Err(Error::Schema);
        }
        let calls = doc.set(m, "tool_calls", Value::Array)?;
        let call = doc.add(None, Value::Object)?;
        doc.append(calls, call)?;
        doc.set_id(call, "id", media::local_id(b'l', m as u64)?)?;
        doc.set(call, "type", Value::Text("function"))?;
        doc.rename(function, "function")?;
        doc.append(call, function)?;
        let legacy = match doc.member(x, "chat_legacy_calls")? {
            Some(n) => n,
            None => doc.set(x, "chat_legacy_calls", Value::Array)?,
        };
        let entry = doc.add(None, Value::Object)?;
        doc.append(legacy, entry)?;
        doc.set(entry, "message_index", Value::Unsigned(current_message))?;
        doc.bind_origin(entry, m, None)?;
    }
    if let Some(calls) = doc.member(m, "tool_calls")? {
        require_array(doc, calls)?;
        let mut call = doc.node(calls)?.first;
        while let Some(c) = call {
            call = doc.node(c)?.next;
            let kind = doc.required(c, "type")?;
            let custom = doc.string_is(kind, "custom")?;
            let function = if custom {
                let custom = doc.remove(c, "custom")?.ok_or(Error::Schema)?;
                doc.rename(custom, "function")?;
                doc.append(c, custom)?;
                if let Some(input) = doc.remove(custom, "input")? {
                    doc.rename(input, "arguments")?;
                    doc.append(custom, input)?;
                }
                custom
            } else {
                doc.required(c, "function")?
            };
            doc.set(c, "arguments_format", Value::Text(if custom { "text" } else { "json" }))?;
            if let Some(arguments) = doc.member(function, "arguments")? {
                require_string(doc, arguments)?;
                if !custom {
                    let lease = encode::scratch(owner, |e| {
                        let mut decoded = doc.chars(arguments)?;
                        while let Some(c) = decoded.next_scalar()? {
                            e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
                        }
                        Ok(())
                    })?;
                    let parsed = doc.import_json(owner, lease)?;
                    doc.transplant(arguments, parsed)?;
                }
            }
        }
    }
    Ok(())
}
