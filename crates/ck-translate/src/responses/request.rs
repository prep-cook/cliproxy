use super::{is, move_field, string};
use crate::codec::{
    self, Capability, Error, Operation, RequestFence, RequestView, Result, ValidatedRequest,
    document::{Document, Name, Value},
    encode::{self, Encoder},
    event::{Emitter, Identity, Kind, Scope},
    media::Registry,
};
use ck_stream::{ScratchLease, Writer};

#[path = "request_input.rs"]
mod input;

pub struct Ingress {
    owner: u64,
    input: input::Input,
    media: Registry,
    emitter: Emitter,
    reader: ck_stream::Reader,
    fence: RequestFence,
    operation: Operation,
    closed: bool,
}
impl Ingress {
    pub fn new(
        owner: &mut Writer,
        store: &ck_stream::Store,
        identity: Identity,
        operation: Operation,
        limit: u64,
    ) -> Result<Self> {
        if operation == Operation::CountTokens {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            owner: owner.id(),
            input: input::Input::new(owner, limit)?,
            media: Registry::new(owner)?,
            emitter: Emitter::new(identity),
            reader: store.reader(owner.id(), 0)?,
            fence: RequestFence::default(),
            operation,
            closed: false,
        })
    }
    fn prepare(&mut self, owner: &mut Writer) -> Result<()> {
        self.input.pending.finish()?;
        self.input.consume(
            owner,
            &mut self.media,
            &mut self.emitter,
            &mut self.reader,
            self.operation,
            true,
        )?;
        let document = &mut self.input.pending.document;
        self.media.validate_registrations(document, 0)?;
        normalize(document, owner, self.operation)?;
        self.media.relocate_ingress(document, 0)?;
        super::content::register(document, owner, &mut self.media)?;
        self.media.relocate_ingress(document, 0)?;
        validate(RequestView {
            document,
            media: &self.media,
            operation: self.operation,
        })?;
        let root = document.root();
        publish_fields(
            document,
            root,
            &mut [None; 128],
            (0, &self.media),
            &mut self.emitter,
            owner,
            &mut self.reader,
        )?;
        for index in self.input.published_media..self.media.len() {
            codec::media::publish(
                document,
                self.media.at(index),
                &mut self.emitter,
                owner,
                Scope::default(),
            )?;
            self.media.retain(document, index, owner, &mut self.reader)?;
        }
        document.retire_scratch()
    }
}
impl codec::RequestIngress for Ingress {
    fn flush_deadline(&self) -> Option<std::time::Instant> {
        self.input.deadline()
    }

    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> Result<()> {
        if owner.id() != self.owner {
            return Err(Error::Position);
        }
        if self.closed {
            return Err(Error::Closed);
        }
        let result = (|| {
            for bytes in bytes.chunks(8192) {
                self.input.pending.advance(owner, bytes)?;
                self.input.consume(
                    owner,
                    &mut self.media,
                    &mut self.emitter,
                    &mut self.reader,
                    self.operation,
                    false,
                )?;
            }
            if bytes.is_empty() {
                self.input.consume(
                    owner,
                    &mut self.media,
                    &mut self.emitter,
                    &mut self.reader,
                    self.operation,
                    true,
                )?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            self.closed = true;
            if !matches!(error, Error::Storage(_)) {
                super::failure(&mut self.emitter, owner, true)?;
            }
            return Err(error);
        }
        Ok(())
    }
    fn finish<'a>(&'a mut self, owner: &mut Writer, destination: &impl Capability) -> Result<ValidatedRequest<'a>> {
        if owner.id() != self.owner {
            return Err(Error::Position);
        }
        if self.closed {
            return Err(Error::Closed);
        }
        self.closed = true;
        if let Err(error) = self.prepare(owner) {
            if !matches!(error, Error::Storage(_)) {
                super::failure(&mut self.emitter, owner, true)?;
            }
            return Err(error);
        }
        let view = RequestView {
            document: &self.input.pending.document,
            media: &self.media,
            operation: self.operation,
        };
        let ready = match self.fence.end(view, destination) {
            Ok(v) => v,
            Err(error) => {
                super::failure(&mut self.emitter, owner, true)?;
                return Err(error);
            }
        };
        self.emitter.request_end(owner, &ready)?;
        self.emitter
            .emit(owner, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(()))?;
        Ok(ready)
    }
}

pub struct ResponsesCapability;
impl Capability for ResponsesCapability {
    fn destination(&self) -> &'static str {
        "responses"
    }
    fn validate(&self, request: RequestView<'_>) -> Result<()> {
        validate(request)
    }
}

pub struct Egress {
    pub destination: &'static str,
    pub upstream_stream: bool,
}
impl codec::RequestEgress for Egress {
    fn encode(&self, request: &ValidatedRequest<'_>, owner: &mut Writer) -> Result<ScratchLease> {
        let view = request.for_destination(self.destination)?;
        view.document.check_owner(owner)?;
        validate(view)?;
        if view.operation == Operation::Compact && self.upstream_stream {
            return Err(Error::Unsupported);
        }
        encode::scratch(owner, |e| write_request(view, self.upstream_stream, e))
    }
}

const ROOT_FIELDS: &[&str] = &[
    "model",
    "messages",
    "tools",
    "tool_choice",
    "parallel_tool_calls",
    "temperature",
    "top_p",
    "max_completion_tokens",
    "stream",
    "response_format",
    "store",
    "metadata",
    "service_tier",
    "user",
    "safety_identifier",
    "prompt_cache_key",
    "prompt_cache_retention",
    "x_ck",
];

fn normalize(doc: &mut Document, owner: &mut Writer, operation: Operation) -> Result<()> {
    let root = doc.root();
    let wire_x = doc.remove(root, "x_ck")?;
    let x = doc.set(root, "x_ck", Value::Object)?;
    doc.set(x, "v", Value::Unsigned(1))?;
    doc.set(x, "original_protocol", Value::Text("responses"))?;
    doc.set(
        x,
        "operation",
        Value::Text(if operation == Operation::Compact {
            "compact"
        } else {
            "generate"
        }),
    )?;
    let original = doc.set(x, "original", Value::Array)?;
    if let Some(wire_x) = wire_x {
        preserve(doc, owner, original, root, wire_x)?;
    }
    let messages = doc.set(root, "messages", Value::Array)?;
    if let Some(instructions) = doc.remove(root, "instructions")? {
        string(doc, instructions)?;
        let message = doc.add(None, Value::Object)?;
        doc.append(messages, message)?;
        doc.set(message, "role", Value::Text("developer"))?;
        doc.rename(instructions, "content")?;
        doc.append(message, instructions)?;
        doc.bind_origin(message, instructions, Some("responses.instructions"))?;
    }
    if let Some(input) = doc.remove(root, "input")? {
        if matches!(doc.node(input)?.value, Value::String(_) | Value::Utf8Media) {
            let m = doc.add(None, Value::Object)?;
            doc.append(messages, m)?;
            doc.set(m, "role", Value::Text("user"))?;
            doc.rename(input, "content")?;
            doc.append(m, input)?;
        } else if matches!(doc.node(input)?.value, Value::Array) {
            while let Some(item) = doc.node(input)?.first {
                doc.detach(input, item)?;
                normalize_item(doc, owner, item, messages, x, original)?;
            }
        } else {
            return Err(Error::Schema);
        }
    }
    if let Some(native) = doc.remove(root, "max_completion_tokens")? {
        preserve(doc, owner, original, root, native)?;
    }
    move_field(doc, root, "max_output_tokens", root, "max_completion_tokens")?;
    if let Some(text) = doc.remove(root, "text")? {
        move_field(doc, text, "format", root, "response_format")?;
        if doc.node(text)?.first.is_some() {
            let provider = provider(doc, x)?;
            doc.append(provider, text)?;
        }
    }
    if let Some(thinking) = doc.remove(root, "reasoning")? {
        doc.rename(thinking, "thinking")?;
        doc.append(x, thinking)?;
        move_field(doc, thinking, "summary", thinking, "include")?;
    }
    if let Some(previous) = doc.remove(root, "previous_response_id")? {
        let session = doc.set(x, "session", Value::Object)?;
        doc.append(session, previous)?;
    }
    for name in [
        "conversation",
        "background",
        "truncation",
        "include",
        "max_tool_calls",
        "text",
    ] {
        if let Some(value) = doc.remove(root, name)? {
            let provider = provider(doc, x)?;
            doc.append(provider, value)?;
        }
    }
    if let Some(tools) = doc.member(root, "tools")? {
        let mut next = doc.node(tools)?.first;
        while let Some(tool) = next {
            next = doc.node(tool)?.next;
            if is(doc, tool, "type", "function")? {
                let function = doc.set(tool, "function", Value::Object)?;
                for name in ["name", "description", "parameters", "strict"] {
                    move_field(doc, tool, name, function, name)?;
                }
            } else {
                let native = match doc.member(x, "tools")? {
                    Some(n) => n,
                    None => doc.set(x, "tools", Value::Array)?,
                };
                let entry = doc.add(None, Value::Object)?;
                doc.append(native, entry)?;
                doc.set(entry, "pointer", Value::Text("/tools"))?;
                doc.detach(tools, tool)?;
                doc.rename(tool, "declaration")?;
                doc.append(entry, tool)?;
            }
        }
    }
    if let Some(choice) = doc.member(root, "tool_choice")?
        && matches!(doc.node(choice)?.value, Value::Object)
        && is(doc, choice, "type", "function")?
    {
        let function = doc.set(choice, "function", Value::Object)?;
        move_field(doc, choice, "name", function, "name")?;
    }
    preserve_unknown(doc, owner, original, root, ROOT_FIELDS)?;
    Ok(())
}
fn provider(doc: &mut Document, x: usize) -> Result<usize> {
    let p = match doc.member(x, "provider_parameters")? {
        Some(p) => p,
        None => doc.set(x, "provider_parameters", Value::Object)?,
    };
    match doc.member(p, "responses")? {
        Some(p) => Ok(p),
        None => doc.set(p, "responses", Value::Object),
    }
}
fn normalize_item(
    doc: &mut Document,
    owner: &mut Writer,
    item: usize,
    messages: usize,
    x: usize,
    original: usize,
) -> Result<()> {
    let index = doc.children(messages)?.count() as u64;
    if is(doc, item, "type", "function_call")? || is(doc, item, "type", "custom_tool_call")? {
        let custom = is(doc, item, "type", "custom_tool_call")?;
        let message = doc.add(None, Value::Object)?;
        doc.append(messages, message)?;
        doc.set(message, "role", Value::Text("assistant"))?;
        let calls = doc.set(message, "tool_calls", Value::Array)?;
        doc.append(calls, item)?;
        if let Some(id) = doc.remove(item, "id")? {
            preserve(doc, owner, original, item, id)?;
        }
        move_field(doc, item, "call_id", item, "id")?;
        doc.set(item, "type", Value::Text(if custom { "custom" } else { "function" }))?;
        doc.set(
            item,
            "arguments_format",
            Value::Text(if custom { "text" } else { "json" }),
        )?;
        let function = doc.set(item, "function", Value::Object)?;
        move_field(doc, item, "name", function, "name")?;
        move_field(doc, item, "namespace", function, "namespace")?;
        if let Some(arguments) = move_field(
            doc,
            item,
            if custom { "input" } else { "arguments" },
            function,
            "arguments",
        )? {
            string(doc, arguments)?;
            if !custom {
                let lease = encode::scratch(owner, |e| {
                    let mut chars = doc.chars(arguments)?;
                    while let Some(c) = chars.next_scalar()? {
                        e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
                    }
                    Ok(())
                })?;
                let parsed = doc.import_json(owner, lease)?;
                doc.transplant(arguments, parsed)?;
            }
        }
        preserve_unknown(
            doc,
            owner,
            original,
            item,
            &["id", "type", "arguments_format", "function"],
        )?;
    } else if is(doc, item, "type", "function_call_output")? || is(doc, item, "type", "custom_tool_call_output")? {
        let custom = is(doc, item, "type", "custom_tool_call_output")?;
        doc.append(messages, item)?;
        doc.set(item, "role", Value::Text("tool"))?;
        move_field(doc, item, "call_id", item, "tool_call_id")?;
        if let Some(content) = move_field(doc, item, "output", item, "content")?
            && matches!(doc.node(content)?.value, Value::Array)
        {
            let mut next = doc.node(content)?.first;
            while let Some(part) = next {
                next = doc.node(part)?.next;
                normalize_part(doc, owner, part, original)?;
            }
        }
        if custom {
            doc.set(item, "name", Value::Text("custom"))?;
        }
        preserve_unknown(doc, owner, original, item, &["role", "tool_call_id", "content", "name"])?;
    } else if is(doc, item, "type", "reasoning")? || is(doc, item, "type", "compaction")? {
        let compaction = is(doc, item, "type", "compaction")?;
        doc.bind_origin(
            item,
            original,
            Some(if compaction {
                "responses.compaction"
            } else {
                "responses.reasoning"
            }),
        )?;
        doc.append(messages, item)?;
        doc.set(item, "role", Value::Text("assistant"))?;
        let native_content = doc.remove(item, "content")?;
        let content = doc.set(item, "content", Value::Array)?;
        if let Some(parts) = native_content {
            while let Some(part) = doc.node(parts)?.first {
                doc.detach(parts, part)?;
                if is(doc, part, "type", "reasoning_text")? {
                    doc.set(part, "type", Value::Text("reasoning"))?;
                }
                preserve_unknown(doc, owner, original, part, &["type", "text"])?;
                doc.append(content, part)?;
            }
        }
        if let Some(summary) = doc.remove(item, "summary")? {
            while let Some(part) = doc.node(summary)?.first {
                doc.detach(summary, part)?;
                doc.set(part, "type", Value::Text("reasoning_summary"))?;
                doc.append(content, part)?;
                preserve_unknown(doc, owner, original, part, &["type", "text"])?;
            }
        }
        if let Some(data) = doc.remove(item, "encrypted_content")? {
            string(doc, data)?;
            let replay = match doc.member(x, "replay")? {
                Some(r) => r,
                None => doc.set(x, "replay", Value::Array)?,
            };
            let replay_index = doc.children(replay)?.count() as u64;
            let entry = doc.add(None, Value::Object)?;
            doc.append(replay, entry)?;
            doc.set(entry, "message_index", Value::Unsigned(index))?;
            doc.set(
                entry,
                "block_index",
                Value::Unsigned(doc.children(content)?.count() as u64),
            )?;
            doc.set(entry, "kind", Value::Text("responses_encrypted"))?;
            doc.set(entry, "source", Value::Text("responses"))?;
            if let Some(id) = doc.member(item, "id")? {
                let id_value = doc.node(id)?.value;
                doc.set(entry, "item_id", id_value)?;
            }
            doc.rename(data, "signature")?;
            doc.append(entry, data)?;
            let part = doc.add(None, Value::Object)?;
            doc.append(content, part)?;
            doc.set(part, "type", Value::Text("encrypted"))?;
            doc.set(part, "replay_index", Value::Unsigned(replay_index))?;
        }
        preserve_unknown(doc, owner, original, item, &["role", "content"])?;
    } else if (doc.member(item, "type")?.is_none() && doc.member(item, "role")?.is_some())
        || is(doc, item, "type", "message")?
    {
        doc.append(messages, item)?;
        if let Some(content) = doc.member(item, "content")?
            && matches!(doc.node(content)?.value, Value::Array)
        {
            let mut next = doc.node(content)?.first;
            while let Some(part) = next {
                next = doc.node(part)?.next;
                normalize_part(doc, owner, part, original)?;
            }
        }
        preserve_unknown(doc, owner, original, item, &["role", "content"])?;
    } else {
        let message = doc.add(None, Value::Object)?;
        doc.append(messages, message)?;
        doc.set(message, "role", Value::Text("assistant"))?;
        let content = doc.set(message, "content", Value::Array)?;
        let part = doc.add(None, Value::Object)?;
        doc.append(content, part)?;
        doc.set(part, "type", Value::Text("opaque"))?;
        doc.set(part, "protocol", Value::Text("responses"))?;
        doc.rename(item, "value")?;
        doc.bind_origin(part, message, Some("responses.item"))?;
        doc.append(part, item)?;
    }
    Ok(())
}
fn normalize_part(doc: &mut Document, owner: &mut Writer, part: usize, original: usize) -> Result<()> {
    let kind = doc.required(part, "type")?;
    if doc.string_is(kind, "input_text")?
        || doc.string_is(kind, "output_text")?
        || doc.string_is(kind, "summary_text")?
    {
        let summary = doc.string_is(kind, "summary_text")?;
        doc.set(
            part,
            "type",
            Value::Text(if summary { "reasoning_summary" } else { "text" }),
        )?;
        string(doc, doc.required(part, "text")?)?;
        preserve_unknown(doc, owner, original, part, &["type", "text"])?;
    } else if doc.string_is(kind, "refusal")? {
        move_field(doc, part, "refusal", part, "text")?;
        preserve_unknown(doc, owner, original, part, &["type", "text"])?;
    } else if doc.string_is(kind, "input_audio")? {
        doc.set(part, "type", Value::Text("audio"))?;
        let source = doc.required(part, "input_audio")?;
        doc.detach(part, source)?;
        doc.rename(source, "source")?;
        doc.append(part, source)?;
        let format = doc.required(source, "format")?;
        let mime = if doc.string_is(format, "mp3")? {
            "audio/mpeg"
        } else if doc.string_is(format, "wav")? {
            "audio/wav"
        } else {
            return Err(Error::Unsupported);
        };
        doc.set(part, "mime", Value::Text(mime))?;
        let data = doc.required(source, "data")?;
        doc.rename(data, "ref")?;
        doc.set(source, "kind", Value::Text("data"))?;
        preserve_unknown(doc, owner, original, source, &["kind", "ref"])?;
        preserve_unknown(doc, owner, original, part, &["type", "source", "mime"])?;
    } else if doc.string_is(kind, "input_image")? || doc.string_is(kind, "input_file")? {
        let image = doc.string_is(kind, "input_image")?;
        doc.set(part, "type", Value::Text(if image { "image" } else { "document" }))?;
        let source = doc.set(part, "source", Value::Object)?;
        if move_field(doc, part, if image { "image_url" } else { "file_url" }, source, "uri")?.is_some() {
            doc.set(source, "kind", Value::Text("uri"))?;
        } else if move_field(doc, part, "file_id", source, "file_id")?.is_some() {
            doc.set(source, "kind", Value::Text("file"))?;
        } else if move_field(doc, part, "file_data", source, "ref")?.is_some() {
            doc.set(source, "kind", Value::Text("data"))?;
        } else {
            return Err(Error::Schema);
        }
        preserve_unknown(doc, owner, original, part, &["type", "source", "detail", "filename"])?;
    } else {
        let value = doc.add(None, Value::Object)?;
        while let Some(child) = doc.node(part)?.first {
            doc.detach(part, child)?;
            doc.append(value, child)?;
        }
        doc.set(part, "type", Value::Text("opaque"))?;
        doc.set(part, "protocol", Value::Text("responses"))?;
        doc.rename(value, "value")?;
        doc.append(part, value)?;
    }
    Ok(())
}
fn preserve(doc: &mut Document, owner: &mut Writer, original: usize, target: usize, value: usize) -> Result<()> {
    let name = doc.node(value)?.name.ok_or(Error::Schema)?;
    let entry = doc.add(None, Value::Object)?;
    doc.append(original, entry)?;
    doc.set(entry, "protocol", Value::Text("responses"))?;
    let lease = encode::scratch(owner, |e| {
        e.bytes(b"\"/")?;
        let mut chars = doc.name_chars(name)?;
        while let Some(c) = chars.next_scalar()? {
            match c {
                '~' => e.bytes(b"~0")?,
                '/' => e.bytes(b"~1")?,
                _ => e.escaped(c.encode_utf8(&mut [0; 4]))?,
            }
        }
        e.bytes(b"\"")
    })?;
    let pointer = doc.import_json(owner, lease)?;
    doc.rename(pointer, "pointer")?;
    doc.append(entry, pointer)?;
    doc.rename(value, "value")?;
    doc.append(entry, value)?;
    doc.bind_origin(entry, target, None)
}
fn preserve_unknown(
    doc: &mut Document,
    owner: &mut Writer,
    original: usize,
    target: usize,
    known: &[&str],
) -> Result<()> {
    let mut next = doc.node(target)?.first;
    while let Some(child) = next {
        next = doc.node(child)?.next;
        let name = doc.node(child)?.name.ok_or(Error::Schema)?;
        let mut keep = false;
        for field in known {
            keep |= doc.name_is(name, field)?;
        }
        if !keep {
            doc.detach(target, child)?;
            preserve(doc, owner, original, target, child)?;
        }
    }
    Ok(())
}
fn validate(view: RequestView<'_>) -> Result<()> {
    if view.operation == Operation::CountTokens {
        return Err(Error::Unsupported);
    }
    let doc = view.document;
    let root = doc.root();
    string(doc, doc.required(root, "model")?)?;
    if view.operation == Operation::Compact
        && doc
            .member(root, "stream")?
            .is_some_and(|n| matches!(doc.node(n).map(|n| n.value), Ok(Value::Bool(true))))
    {
        return Err(Error::Unsupported);
    }
    for unsupported in [
        "n",
        "seed",
        "stop",
        "logit_bias",
        "frequency_penalty",
        "presence_penalty",
        "modalities",
        "audio",
    ] {
        if doc.member(root, unsupported)?.is_some() {
            return Err(Error::Unsupported);
        }
    }
    let messages = doc.required(root, "messages")?;
    if !matches!(doc.node(messages)?.value, Value::Array) {
        return Err(Error::Schema);
    }
    for message in doc.children(messages)? {
        let role = doc.required(message, "role")?;
        let mut valid = false;
        for value in ["system", "developer", "user", "assistant", "tool"] {
            valid |= doc.string_is(role, value)?;
        }
        if !valid {
            return Err(Error::Unsupported);
        }
        if is(doc, message, "role", "tool")? {
            let id = doc.required(message, "tool_call_id")?;
            string(doc, id)?;
            let mut found = false;
            for previous in doc.children(messages)? {
                if previous == message {
                    break;
                }
                if let Some(calls) = doc.member(previous, "tool_calls")? {
                    for call in doc.children(calls)? {
                        found |= doc.equal_strings(doc.required(call, "id")?, id)?;
                    }
                }
            }
            if !found {
                return Err(Error::Schema);
            }
        }
        if let Some(calls) = doc.member(message, "tool_calls")? {
            for call in doc.children(calls)? {
                string(doc, doc.required(call, "id")?)?;
                let function = doc.required(call, "function")?;
                string(doc, doc.required(function, "name")?)?;
            }
        }
        if let Some(content) = doc.member(message, "content")? {
            match doc.node(content)?.value {
                Value::String(_) | Value::Text(_) | Value::Fragments | Value::Utf8Media => (),
                Value::Array => {
                    for part in doc.children(content)? {
                        let kind = doc.required(part, "type")?;
                        let mut supported = false;
                        for k in [
                            "text",
                            "reasoning",
                            "reasoning_summary",
                            "refusal",
                            "encrypted",
                            "redacted",
                            "image",
                            "audio",
                            "document",
                            "opaque",
                        ] {
                            supported |= doc.string_is(kind, k)?;
                        }
                        if doc.string_is(kind, "audio")? {
                            let source = doc.required(part, "source")?;
                            let mime = doc.required(part, "mime")?;
                            if !is(doc, source, "kind", "data")?
                                || (!doc.string_is(mime, "audio/mpeg")? && !doc.string_is(mime, "audio/wav")?)
                            {
                                return Err(Error::Unsupported);
                            }
                            view.media
                                .get(doc, doc.required(source, "ref")?)?
                                .ok_or(Error::Position)?;
                        }
                        if !supported {
                            return Err(Error::Unsupported);
                        }
                    }
                }
                _ => return Err(Error::Schema),
            }
        }
    }
    if let Some(x) = doc.member(root, "x_ck")? {
        if let Some(cache) = doc.member(x, "cache_control")?
            && doc.node(cache)?.first.is_some()
        {
            return Err(Error::Unsupported);
        }
        if let Some(replay) = doc.member(x, "replay")? {
            for entry in doc.children(replay)? {
                if !is(doc, entry, "kind", "responses_encrypted")? {
                    return Err(Error::Unsupported);
                }
            }
        }
        if let Some(session) = doc.member(x, "session")? {
            for field in doc.children(session)? {
                if !doc.name_is(doc.node(field)?.name.ok_or(Error::Schema)?, "previous_response_id")? {
                    return Err(Error::Unsupported);
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Component {
    Name(Name),
    Index(u64),
}
fn publish_fields(
    doc: &mut Document,
    node: usize,
    path: &mut [Option<Component>; 128],
    (depth, media): (usize, &Registry),
    emitter: &mut Emitter,
    owner: &mut Writer,
    reader: &mut ck_stream::Reader,
) -> Result<()> {
    if depth == path.len() {
        return Err(Error::Limit);
    }
    publish_field(doc, node, &path[..depth], media, emitter, owner, reader)?;
    if media.get(doc, node)?.is_some() {
        return Ok(());
    }
    let mut next = doc.node(node)?.first;
    let array = matches!(doc.node(node)?.value, Value::Array);
    let mut index = 0;
    while let Some(child) = next {
        next = doc.node(child)?.next;
        path[depth] = Some(if array {
            Component::Index(index)
        } else {
            Component::Name(doc.node(child)?.name.ok_or(Error::Schema)?)
        });
        publish_fields(doc, child, path, (depth + 1, media), emitter, owner, reader)?;
        index += 1;
    }
    Ok(())
}

fn publish_field(
    doc: &mut Document,
    node: usize,
    path: &[Option<Component>],
    media: &Registry,
    emitter: &mut Emitter,
    owner: &mut Writer,
    reader: &mut ck_stream::Reader,
) -> Result<()> {
    if doc.published(node)? {
        return Ok(());
    }
    let registered = media.get(doc, node)?;
    emitter.emit(
        owner,
        Kind::RequestField,
        Scope::default(),
        |e| {
            e.bytes(b",\"pointer\":\"")?;
            for part in path.iter().flatten() {
                e.bytes(b"/")?;
                match part {
                    Component::Index(i) => e.unsigned(*i)?,
                    Component::Name(name) => {
                        let mut chars = doc.name_chars(*name)?;
                        while let Some(c) = chars.next_scalar()? {
                            match c {
                                '~' => e.bytes(b"~0")?,
                                '/' => e.bytes(b"~1")?,
                                _ => e.escaped(c.encode_utf8(&mut [0; 4]))?,
                            }
                        }
                    }
                }
            }
            e.bytes(b"\",\"value\":")?;
            if let Some(media) = registered {
                return codec::media::write_reference(media, e);
            }
            match doc.node(node)?.value {
                Value::Object => e.bytes(b"{}"),
                Value::Array => e.bytes(b"[]"),
                _ => doc.write(node, e),
            }
        },
        |_| Ok(()),
    )?;
    let (_, event) = codec::internal::next(owner, reader)?;
    doc.bind_field(node, event)?;
    doc.mark_published(node);
    Ok(())
}

fn originals(doc: &Document, target: usize, e: &mut Encoder<'_, '_>, skip: &[&str]) -> Result<()> {
    let Some(x) = doc.member(doc.root(), "x_ck")? else {
        return Ok(());
    };
    let Some(original) = doc.member(x, "original")? else {
        return Ok(());
    };
    for entry in doc.children(original)? {
        if doc.origin(entry)?.map(|(node, _)| node) != Some(target) {
            continue;
        }
        let pointer = doc.required(entry, "pointer")?;
        let Value::String(text) = doc.node(pointer)?.value else {
            return Err(Error::Schema);
        };
        let field_name = Name::Pointer(text);
        let mut skipped = false;
        for name in skip {
            skipped |= doc.name_is(field_name, name)?;
        }
        if skipped {
            continue;
        }
        e.bytes(b",")?;
        doc.write_name(field_name, e)?;
        e.bytes(b":")?;
        doc.write(doc.required(entry, "value")?, e)?;
    }
    Ok(())
}
fn write_request(view: RequestView<'_>, streaming: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let root = doc.root();
    e.bytes(b"{\"input\":[")?;
    let mut first = true;
    for (index, message) in doc.children(doc.required(root, "messages")?)?.enumerate() {
        if doc
            .origin(message)?
            .is_some_and(|(_, carrier)| carrier == Some("responses.instructions"))
            && is(doc, message, "role", "developer")?
        {
            continue;
        }
        if let Some(calls) = doc.member(message, "tool_calls")? {
            for call in doc.children(calls)? {
                if !first {
                    e.bytes(b",")?;
                }
                first = false;
                let custom = is(doc, call, "arguments_format", "text")?;
                e.bytes(b"{\"type\":")?;
                e.string(if custom { "custom_tool_call" } else { "function_call" })?;
                e.bytes(b",\"call_id\":")?;
                doc.write(doc.required(call, "id")?, e)?;
                let function = doc.required(call, "function")?;
                super::member(doc, function, "name", e)?;
                super::member(doc, function, "namespace", e)?;
                if let Some(arguments) = doc.member(function, "arguments")? {
                    e.bytes(b",")?;
                    e.string(if custom { "input" } else { "arguments" })?;
                    e.bytes(b":")?;
                    if custom {
                        doc.write(arguments, e)?;
                    } else {
                        e.quoted_json(|e| doc.write(arguments, e))?;
                    }
                }
                originals(doc, call, e, &[])?;
                e.bytes(b"}")?;
            }
        } else if is(doc, message, "role", "tool")? {
            if !first {
                e.bytes(b",")?;
            }
            first = false;
            e.bytes(b"{\"type\":")?;
            e.string(if is(doc, message, "name", "custom")? {
                "custom_tool_call_output"
            } else {
                "function_call_output"
            })?;
            e.bytes(b",\"call_id\":")?;
            doc.write(doc.required(message, "tool_call_id")?, e)?;
            e.bytes(b",\"output\":")?;
            let content = doc.required(message, "content")?;
            if matches!(doc.node(content)?.value, Value::Array) {
                e.bytes(b"[")?;
                for (index, part) in doc.children(content)?.enumerate() {
                    if index != 0 {
                        e.bytes(b",")?;
                    }
                    write_part(view, part, false, e)?;
                }
                e.bytes(b"]")?;
            } else {
                doc.write(content, e)?;
            }
            originals(doc, message, e, &["type"])?;
            e.bytes(b"}")?;
        } else if let Some(content) = doc.member(message, "content")? {
            if let Some(part) = doc.node(content)?.first
                && doc
                    .origin(part)?
                    .is_some_and(|(_, carrier)| carrier == Some("responses.item"))
                && is(doc, part, "type", "opaque")?
            {
                if !first {
                    e.bytes(b",")?;
                }
                first = false;
                doc.write(doc.required(part, "value")?, e)?;
                continue;
            }
            let reasoning = matches!(doc.node(content)?.value, Value::Array)
                && doc.children(content)?.any(|p| {
                    is(doc, p, "type", "encrypted").unwrap_or(false)
                        || is(doc, p, "type", "reasoning_summary").unwrap_or(false)
                        || is(doc, p, "type", "reasoning").unwrap_or(false)
                })
                || doc.origin(message)?.is_some_and(|(_, carrier)| {
                    matches!(carrier, Some("responses.compaction" | "responses.reasoning"))
                });
            if !first {
                e.bytes(b",")?;
            }
            first = false;
            if reasoning {
                let compaction = doc
                    .origin(message)?
                    .is_some_and(|(_, carrier)| carrier == Some("responses.compaction"));
                e.bytes(b"{\"type\":")?;
                e.string(if compaction { "compaction" } else { "reasoning" })?;
                let summary = !compaction
                    || doc
                        .children(content)?
                        .any(|part| is(doc, part, "type", "reasoning_summary").unwrap_or(false));
                if summary {
                    e.bytes(b",\"summary\":[")?;
                    let mut comma = false;
                    for part in doc.children(content)? {
                        if is(doc, part, "type", "reasoning_summary")? {
                            if comma {
                                e.bytes(b",")?;
                            }
                            comma = true;
                            e.bytes(b"{\"type\":\"summary_text\",\"text\":")?;
                            doc.write(doc.required(part, "text")?, e)?;
                            originals(doc, part, e, &[])?;
                            e.bytes(b"}")?;
                        }
                    }
                    e.bytes(b"]")?;
                }
                if doc
                    .children(content)?
                    .any(|part| is(doc, part, "type", "reasoning").unwrap_or(false))
                {
                    e.bytes(b",\"content\":[")?;
                    let mut comma = false;
                    for part in doc.children(content)? {
                        if is(doc, part, "type", "reasoning")? {
                            if comma {
                                e.bytes(b",")?;
                            }
                            comma = true;
                            e.bytes(b"{\"type\":\"reasoning_text\",\"text\":")?;
                            doc.write(doc.required(part, "text")?, e)?;
                            originals(doc, part, e, &[])?;
                            e.bytes(b"}")?;
                        }
                    }
                    e.bytes(b"]")?;
                }
                if let Some(x) = doc.member(root, "x_ck")?
                    && let Some(replay) = doc.member(x, "replay")?
                {
                    for entry in doc.children(replay)? {
                        if doc.unsigned(doc.required(entry, "message_index")?)? == index as u64 {
                            e.bytes(b",\"encrypted_content\":")?;
                            doc.write(doc.required(entry, "signature")?, e)?;
                        }
                    }
                }
                originals(doc, message, e, &["type"])?;
                e.bytes(b"}")?;
            } else {
                e.bytes(b"{\"type\":\"message\",\"role\":")?;
                doc.write(doc.required(message, "role")?, e)?;
                e.bytes(b",\"content\":")?;
                if matches!(doc.node(content)?.value, Value::Array) {
                    e.bytes(b"[")?;
                    for (i, part) in doc.children(content)?.enumerate() {
                        if i > 0 {
                            e.bytes(b",")?;
                        }
                        write_part(view, part, is(doc, message, "role", "assistant")?, e)?;
                    }
                    e.bytes(b"]")?;
                } else {
                    doc.write(content, e)?;
                }
                originals(doc, message, e, &["type", "instructions"])?;
                e.bytes(b"}")?;
            }
        }
    }
    e.bytes(b"]")?;
    if view.operation != Operation::Compact {
        e.bytes(b",\"stream\":")?;
        e.bytes(if streaming { b"true" } else { b"false" })?;
    }
    for message in doc.children(doc.required(root, "messages")?)? {
        if doc
            .origin(message)?
            .is_some_and(|(_, carrier)| carrier == Some("responses.instructions"))
            && is(doc, message, "role", "developer")?
        {
            e.bytes(b",\"instructions\":")?;
            doc.write(doc.required(message, "content")?, e)?;
        }
    }
    for field in doc.children(root)? {
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        if doc.name_is(name, "messages")? || doc.name_is(name, "stream")? || doc.name_is(name, "x_ck")? {
            continue;
        }
        e.bytes(b",")?;
        if doc.name_is(name, "max_completion_tokens")? {
            e.string("max_output_tokens")?;
        } else if doc.name_is(name, "response_format")? {
            e.string("text")?;
        } else {
            doc.write_name(name, e)?;
        }
        e.bytes(b":")?;
        if doc.name_is(name, "response_format")? {
            e.bytes(b"{\"format\":")?;
            doc.write(field, e)?;
            e.bytes(b"}")?;
        } else if doc.name_is(name, "tools")? {
            e.bytes(b"[")?;
            for (i, tool) in doc.children(field)?.enumerate() {
                if i > 0 {
                    e.bytes(b",")?;
                }
                e.bytes(b"{\"type\":\"function\"")?;
                let function = doc.required(tool, "function")?;
                for f in doc.children(function)? {
                    e.bytes(b",")?;
                    doc.write_name(doc.node(f)?.name.ok_or(Error::Schema)?, e)?;
                    e.bytes(b":")?;
                    doc.write(f, e)?;
                }
                e.bytes(b"}")?;
            }
            if let Some(x) = doc.member(root, "x_ck")?
                && let Some(native) = doc.member(x, "tools")?
            {
                let mut comma = doc.node(field)?.first.is_some();
                for entry in doc.children(native)? {
                    if comma {
                        e.bytes(b",")?;
                    }
                    comma = true;
                    doc.write(doc.required(entry, "declaration")?, e)?;
                }
            }
            e.bytes(b"]")?;
        } else if doc.name_is(name, "tool_choice")?
            && matches!(doc.node(field)?.value, Value::Object)
            && is(doc, field, "type", "function")?
        {
            e.bytes(b"{\"type\":\"function\"")?;
            super::member(doc, doc.required(field, "function")?, "name", e)?;
            e.bytes(b"}")?;
        } else {
            doc.write(field, e)?;
        }
    }
    if let Some(x) = doc.member(root, "x_ck")? {
        if let Some(thinking) = doc.member(x, "thinking")? {
            e.bytes(b",\"reasoning\":{")?;
            for (i, f) in doc.children(thinking)?.enumerate() {
                if i > 0 {
                    e.bytes(b",")?;
                }
                let name = doc.node(f)?.name.ok_or(Error::Schema)?;
                if doc.name_is(name, "include")? {
                    e.string("summary")?;
                } else {
                    doc.write_name(name, e)?;
                }
                e.bytes(b":")?;
                doc.write(f, e)?;
            }
            e.bytes(b"}")?;
        }
        if let Some(session) = doc.member(x, "session")? {
            super::member(doc, session, "previous_response_id", e)?;
        }
        if let Some(providers) = doc.member(x, "provider_parameters")?
            && let Some(p) = doc.member(providers, "responses")?
        {
            for f in doc.children(p)? {
                e.bytes(b",")?;
                doc.write_name(doc.node(f)?.name.ok_or(Error::Schema)?, e)?;
                e.bytes(b":")?;
                doc.write(f, e)?;
            }
        }
    }
    originals(doc, root, e, &[])?;
    e.bytes(b"}")
}
fn write_part(view: RequestView<'_>, part: usize, assistant: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    if is(doc, part, "type", "opaque")? {
        return doc.write(doc.required(part, "value")?, e);
    }
    if is(doc, part, "type", "audio")? {
        let source = doc.required(part, "source")?;
        if !is(doc, source, "kind", "data")? {
            return Err(Error::Unsupported);
        }
        let mime = doc.required(part, "mime")?;
        let format = if doc.string_is(mime, "audio/mpeg")? {
            "mp3"
        } else if doc.string_is(mime, "audio/wav")? {
            "wav"
        } else {
            return Err(Error::Unsupported);
        };
        e.bytes(b"{\"type\":\"input_audio\",\"input_audio\":{\"data\":")?;
        // `format` names the media type, so the data stays plain base64.
        view.media.write_base64(doc, doc.required(source, "ref")?, e)?;
        e.bytes(b",\"format\":")?;
        e.string(format)?;
        originals(doc, source, e, &["format"])?;
        e.bytes(b"}")?;
        originals(doc, part, e, &[])?;
        return e.bytes(b"}");
    }
    if is(doc, part, "type", "image")? || is(doc, part, "type", "document")? {
        let image = is(doc, part, "type", "image")?;
        e.bytes(b"{\"type\":")?;
        e.string(if image { "input_image" } else { "input_file" })?;
        let source = doc.required(part, "source")?;
        if is(doc, source, "kind", "uri")? {
            e.bytes(b",")?;
            e.string(if image { "image_url" } else { "file_url" })?;
            e.bytes(b":")?;
            doc.write(doc.required(source, "uri")?, e)?;
        } else if is(doc, source, "kind", "file")? {
            super::member(doc, source, "file_id", e)?;
        } else if is(doc, source, "kind", "data")? {
            e.bytes(b",")?;
            e.string(if image { "image_url" } else { "file_data" })?;
            e.bytes(b":")?;
            let mime = doc.member(part, "mime")?;
            view.media
                .write_value(doc, doc.required(source, "ref")?, (mime, true), e)?;
        } else {
            return Err(Error::Unsupported);
        }
        super::member(doc, part, "detail", e)?;
        super::member(doc, part, "filename", e)?;
    } else {
        let refusal = is(doc, part, "type", "refusal")?;
        e.bytes(b"{\"type\":")?;
        e.string(if refusal {
            "refusal"
        } else if assistant {
            "output_text"
        } else {
            "input_text"
        })?;
        e.bytes(b",")?;
        e.string(if refusal { "refusal" } else { "text" })?;
        e.bytes(b":")?;
        doc.write(doc.required(part, "text")?, e)?;
    }
    originals(doc, part, e, &[])?;
    e.bytes(b"}")
}
