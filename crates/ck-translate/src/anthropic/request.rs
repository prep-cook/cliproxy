use crate::codec::{
    self, Capability, Error, Operation, RequestFence, RequestView, Result, ValidatedRequest,
    document::{Document, Name, Value, pending::Pending},
    encode,
    event::{Emitter, Identity, Kind, Scope},
    json,
    media::{self, Registry},
};
use ck_stream::{ScratchLease, Writer};

#[path = "request_stream.rs"]
mod stream;

pub struct Ingress {
    owner: u64,
    input: Option<Pending>,
    active: Option<stream::StringState>,
    started: bool,
    last_field: Option<usize>,
    published_media: usize,
    document: Option<Document>,
    media: Registry,
    emitter: Emitter,
    fence: RequestFence,
    operation: Operation,
    reader: ck_stream::Reader,
    closed: bool,
}
impl Ingress {
    pub fn new(
        owner: &mut Writer,
        store: &ck_stream::Store,
        identity: Identity,
        operation: Operation,
        max_bytes: u64,
    ) -> Result<Self> {
        if operation == Operation::Compact {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            owner: owner.id(),
            input: Some(Pending::new(owner, max_bytes)?),
            active: None,
            started: false,
            last_field: None,
            published_media: 0,
            document: None,
            media: Registry::new(owner)?,
            emitter: Emitter::new(identity),
            fence: RequestFence::default(),
            operation,
            reader: store.reader(owner.id(), 0)?,
            closed: false,
        })
    }
    fn prepare(&mut self, owner: &mut Writer) -> Result<()> {
        self.input.as_mut().ok_or(Error::Closed)?.finish()?;
        self.consume(owner)?;
        let mut doc = self.input.take().ok_or(Error::Closed)?.document;
        self.media.validate_registrations(&doc)?;
        normalize(owner, &mut doc, self.operation)?;
        self.media.relocate_ingress(&doc)?;
        super::content::register_media(
            &mut doc,
            &mut self.media,
            owner,
            &mut self.emitter,
            &mut self.reader,
            &mut self.published_media,
        )?;
        super::request_schema::validate(RequestView {
            document: &doc,
            media: &self.media,
            operation: self.operation,
        })?;
        for media in self.media.entries().filter(|media| media.data.is_some()) {
            media::bytes(&doc, media, |_| Ok(()))?;
        }
        let root = doc.root();
        fields(
            &mut doc,
            &self.media,
            root,
            (&mut [None; 128], 0),
            &mut self.emitter,
            owner,
            &mut self.reader,
        )?;
        for index in self.published_media..self.media.len() {
            media::publish(&doc, self.media.at(index), &mut self.emitter, owner, Scope::default())?;
            self.media.retain(&mut doc, index, owner, &mut self.reader)?;
        }
        doc.retire_scratch()?;
        self.document = Some(doc);
        Ok(())
    }
}
impl codec::RequestIngress for Ingress {
    fn flush_deadline(&self) -> Option<std::time::Instant> {
        self.pending_deadline()
    }

    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> Result<()> {
        if owner.id() != self.owner {
            return Err(Error::Position);
        }
        if self.closed {
            return Err(Error::Closed);
        }
        let result = (|| {
            if bytes.is_empty() {
                self.flush_pending(owner)?;
            }
            for chunk in bytes.chunks(8192) {
                self.input.as_mut().ok_or(Error::Closed)?.advance(owner, chunk)?;
                self.consume(owner)?;
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
            document: self.document.as_ref().ok_or(Error::Schema)?,
            media: &self.media,
            operation: self.operation,
        };
        let ready = match self.fence.end(view, destination) {
            Ok(r) => r,
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
pub struct AnthropicCapability;
impl Capability for AnthropicCapability {
    fn destination(&self) -> &'static str {
        "anthropic"
    }
    fn validate(&self, view: RequestView<'_>) -> Result<()> {
        super::request_schema::validate(view)
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
        if view.operation == Operation::Compact || (view.operation == Operation::CountTokens && self.upstream_stream) {
            return Err(Error::Unsupported);
        }
        encode::scratch(owner, |e| super::request_encode::write(view, self.upstream_stream, e))
    }
}
fn normalize(owner: &mut Writer, doc: &mut Document, operation: Operation) -> Result<()> {
    let root = doc.root();
    let collision = doc.remove(root, "x_ck")?;
    let x = doc.set(root, "x_ck", Value::Object)?;
    doc.set(x, "v", Value::Unsigned(1))?;
    doc.set(x, "original_protocol", Value::Text("anthropic"))?;
    doc.set(
        x,
        "operation",
        Value::Text(if operation == Operation::Generate {
            "generate"
        } else {
            "count_tokens"
        }),
    )?;
    let original = doc.set(x, "original", Value::Array)?;
    doc.set(x, "replay", Value::Array)?;
    doc.set(x, "cache_control", Value::Array)?;
    doc.set(x, "anthropic_order", Value::Array)?;
    if let Some(node) = collision {
        preserve(owner, doc, original, root, node, None)?;
    }
    capture(
        owner,
        doc,
        root,
        &[
            "x_ck",
            "model",
            "messages",
            "system",
            "stream",
            "max_tokens",
            "temperature",
            "top_p",
            "top_k",
            "stop_sequences",
            "thinking",
            "output_config",
            "tools",
            "tool_choice",
            "metadata",
            "service_tier",
            "cache_control",
            "container",
            "context_management",
            "mcp_servers",
            "betas",
        ],
        original,
    )?;
    if let Some(stop) = doc.member(root, "stop_sequences")? {
        doc.rename(stop, "stop")?;
    }
    let provider = doc.set(x, "provider_parameters", Value::Object)?;
    let native = doc.set(provider, "anthropic", Value::Object)?;
    for key in ["top_k", "container", "context_management", "mcp_servers", "betas"] {
        if let Some(node) = doc.remove(root, key)? {
            doc.append(native, node)?;
        }
    }
    if let Some(thinking) = doc.remove(root, "thinking")? {
        let normalized = doc.set(x, "thinking", Value::Object)?;
        if let Some(kind) = doc.member(thinking, "type")? {
            if doc.string_is(kind, "enabled")? || doc.string_is(kind, "adaptive")? {
                doc.set(normalized, "enabled", Value::Bool(true))?;
            } else if doc.string_is(kind, "disabled")? {
                doc.set(normalized, "enabled", Value::Bool(false))?;
            } else {
                return Err(Error::Unsupported);
            }
        }
        if let Some(budget) = doc.member(thinking, "budget_tokens")? {
            let value = doc.node(budget)?.value;
            doc.set(normalized, "budget_tokens", value)?;
        }
        doc.rename(thinking, "native")?;
        doc.append(normalized, thinking)?;
    }
    if let Some(output) = doc.remove(root, "output_config")? {
        if let Some(effort) = doc.remove(output, "effort")? {
            let thinking = match doc.member(x, "thinking")? {
                Some(n) => n,
                None => doc.set(x, "thinking", Value::Object)?,
            };
            doc.append(thinking, effort)?;
        }
        if let Some(format) = doc.remove(output, "format")? {
            let response = doc.set(root, "response_format", Value::Object)?;
            let kind = doc.remove(format, "type")?.ok_or(Error::Schema)?;
            doc.append(response, kind)?;
            doc.rename(format, "json_schema")?;
            doc.append(response, format)?;
        }
        doc.append(native, output)?;
    }
    super::content::cache(owner, doc, root)?;
    let messages = doc.required(root, "messages")?;
    if !matches!(doc.node(messages)?.value, Value::Array) {
        return Err(Error::Schema);
    }
    let system = doc.remove(root, "system")?;
    let new_messages = doc.add(None, Value::Array)?;
    if let Some(system) = system {
        let message = doc.add(None, Value::Object)?;
        doc.append(new_messages, message)?;
        doc.set(message, "role", Value::Text("system"))?;
        doc.rename(system, "content")?;
        doc.append(message, system)?;
    }
    while let Some(message) = doc.node(messages)?.first {
        doc.detach(messages, message)?;
        doc.append(new_messages, message)?;
    }
    doc.transplant(messages, new_messages)?;
    let mut current = doc.node(messages)?.first;
    let mut message_index = 0;
    while let Some(message) = current {
        current = doc.node(message)?.next;
        super::content::normalize(owner, doc, message, message_index)?;
        message_index += 1;
    }
    if let Some(tools) = doc.member(root, "tools")? {
        let mut next = doc.node(tools)?.first;
        while let Some(tool) = next {
            next = doc.node(tool)?.next;
            super::content::cache(owner, doc, tool)?;
            if let Some(kind) = doc.member(tool, "type")?
                && !doc.string_is(kind, "custom")?
            {
                let declarations = match doc.member(x, "tools")? {
                    Some(n) => n,
                    None => doc.set(x, "tools", Value::Array)?,
                };
                let entry = doc.add(None, Value::Object)?;
                doc.append(declarations, entry)?;
                doc.set(entry, "pointer", Value::Text("/tools"))?;
                let declaration = doc.add(Some(Name::Static("declaration")), Value::Object)?;
                doc.transplant(declaration, tool)?;
                doc.append(entry, declaration)?;
                doc.replace(tool, Value::Object)?;
                doc.set(tool, "type", Value::Text("server"))?;
                doc.bind_origin(entry, tool, None)?;
                continue;
            }
            if let Some(kind) = doc.remove(tool, "type")? {
                preserve(owner, doc, original, tool, kind, None)?;
            }
            let function = doc.add(Some(Name::Static("function")), Value::Object)?;
            while let Some(field) = doc.node(tool)?.first {
                doc.detach(tool, field)?;
                if doc.name_is(doc.node(field)?.name.ok_or(Error::Schema)?, "input_schema")? {
                    doc.rename(field, "parameters")?;
                }
                doc.append(function, field)?;
            }
            doc.set(tool, "type", Value::Text("function"))?;
            doc.append(tool, function)?;
        }
    }
    if let Some(choice) = doc.member(root, "tool_choice")? {
        if let Some(disable) = doc.remove(choice, "disable_parallel_tool_use")? {
            let Value::Bool(value) = doc.node(disable)?.value else {
                return Err(Error::Schema);
            };
            doc.set(root, "parallel_tool_calls", Value::Bool(!value))?;
        }
        let kind = doc.required(choice, "type")?;
        if doc.string_is(kind, "tool")? {
            let name = doc.remove(choice, "name")?.ok_or(Error::Schema)?;
            doc.replace(kind, Value::Text("function"))?;
            let f = doc.set(choice, "function", Value::Object)?;
            doc.append(f, name)?;
        } else if doc.string_is(kind, "auto")? || doc.string_is(kind, "any")? || doc.string_is(kind, "none")? {
            let value = if doc.string_is(kind, "any")? {
                "required"
            } else if doc.string_is(kind, "none")? {
                "none"
            } else {
                "auto"
            };
            capture(owner, doc, choice, &["type"], original)?;
            doc.replace(choice, Value::Text(value))?;
        } else {
            return Err(Error::Unsupported);
        }
    }
    capture(
        owner,
        doc,
        root,
        &[
            "x_ck",
            "model",
            "messages",
            "tools",
            "tool_choice",
            "parallel_tool_calls",
            "stream",
            "max_tokens",
            "temperature",
            "top_p",
            "stop",
            "metadata",
            "service_tier",
            "response_format",
        ],
        original,
    )?;
    if doc.node(native)?.first.is_none() {
        doc.remove(provider, "anthropic")?;
    }
    if doc.node(provider)?.first.is_none() {
        doc.remove(x, "provider_parameters")?;
    }
    Ok(())
}
pub(super) fn capture(
    owner: &mut Writer,
    doc: &mut Document,
    parent: usize,
    known: &[&str],
    original: usize,
) -> Result<()> {
    let mut next = doc.node(parent)?.first;
    while let Some(field) = next {
        next = doc.node(field)?.next;
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        if known
            .iter()
            .try_fold(false, |yes, key| Ok::<_, Error>(yes || doc.name_is(name, key)?))?
        {
            continue;
        }
        doc.detach(parent, field)?;
        preserve(owner, doc, original, parent, field, None)?;
    }
    Ok(())
}
pub(super) fn preserve(
    owner: &mut Writer,
    doc: &mut Document,
    original: usize,
    parent: usize,
    field: usize,
    carrier: Option<&'static str>,
) -> Result<()> {
    let name = doc.node(field)?.name.ok_or(Error::Schema)?;
    let pointer = encode::scratch(owner, |e| {
        e.bytes(b"\"/")?;
        let mut chars = doc.name_chars(name)?;
        while let Some(c) = chars.next_scalar()? {
            match c {
                '~' => e.bytes(b"~0")?,
                '/' => e.bytes(b"~1")?,
                c => e.escaped(c.encode_utf8(&mut [0; 4]))?,
            }
        }
        e.bytes(b"\"")
    })?;
    let pointer = doc.import_json(owner, pointer)?;
    doc.rename(pointer, "pointer")?;
    let entry = doc.add(None, Value::Object)?;
    doc.append(original, entry)?;
    doc.set(entry, "protocol", Value::Text("anthropic"))?;
    doc.append(entry, pointer)?;
    doc.rename(field, "value")?;
    doc.append(entry, field)?;
    doc.bind_origin(entry, parent, carrier)
}
#[derive(Clone, Copy)]
enum Component {
    Name(Name),
    Index(u64),
}
fn fields(
    doc: &mut Document,
    registry: &Registry,
    node: usize,
    (path, depth): (&mut [Option<Component>; 128], usize),
    emitter: &mut Emitter,
    owner: &mut Writer,
    reader: &mut ck_stream::Reader,
) -> Result<()> {
    if depth == path.len() {
        return Err(Error::Limit);
    }
    let registered = registry.get(doc, node)?;
    if doc.published(node)? {
        if registered.is_some() {
            return Ok(());
        }
    } else {
        emitter.emit(
            owner,
            Kind::RequestField,
            Scope::default(),
            |e| {
                e.bytes(b",\"pointer\":\"")?;
                for component in path[..depth].iter().flatten() {
                    e.bytes(b"/")?;
                    match component {
                        Component::Index(index) => e.unsigned(*index)?,
                        Component::Name(name) => {
                            let mut chars = doc.name_chars(*name)?;
                            while let Some(c) = chars.next_scalar()? {
                                match c {
                                    '~' => e.bytes(b"~0")?,
                                    '/' => e.bytes(b"~1")?,
                                    c => e.escaped(c.encode_utf8(&mut [0; 4]))?,
                                }
                            }
                        }
                    }
                }
                e.bytes(b"\",\"value\":")?;
                if let Some(media) = registered {
                    media::write_reference(media, e)
                } else {
                    match doc.node(node)?.value {
                        Value::Object => e.bytes(b"{}"),
                        Value::Array => e.bytes(b"[]"),
                        _ => doc.write(node, e),
                    }
                }
            },
            |_| Ok(()),
        )?;
        let (_, event) = codec::internal::next(owner, reader)?;
        doc.bind_field(node, event)?;
        doc.mark_published(node);
    }
    if registered.is_some() {
        return Ok(());
    }
    let mut child = doc.node(node)?.first;
    let mut index = 0;
    while let Some(n) = child {
        child = doc.node(n)?.next;
        path[depth] = Some(if matches!(doc.node(node)?.value, Value::Array) {
            Component::Index(index)
        } else {
            Component::Name(doc.node(n)?.name.ok_or(Error::Schema)?)
        });
        fields(doc, registry, n, (path, depth + 1), emitter, owner, reader)?;
        index += 1;
    }
    Ok(())
}
