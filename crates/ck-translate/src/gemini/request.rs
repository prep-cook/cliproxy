use crate::codec::{
    self, Capability, Error, Operation, RequestFence, RequestView, Result, ValidatedRequest,
    document::{Document, Name, Value, pending::Pending},
    encode::{self, Encoder},
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
    published_media: usize,
    document: Option<Document>,
    model: Document,
    media: Registry,
    emitter: Emitter,
    fence: RequestFence,
    operation: Operation,
    reader: ck_stream::Reader,
    closed: bool,
}
impl Ingress {
    /// `model` is the decoded URL model, not an inferred JSON field.
    pub fn new(
        owner: &mut Writer,
        store: &ck_stream::Store,
        identity: Identity,
        operation: Operation,
        model: &str,
        max_bytes: u64,
    ) -> Result<Self> {
        if operation == Operation::Compact {
            return Err(Error::Unsupported);
        }
        if model.is_empty() {
            return Err(Error::Schema);
        }
        Ok(Self {
            owner: owner.id(),
            input: Some(Pending::new(owner, max_bytes)?),
            active: None,
            started: false,
            published_media: 0,
            document: None,
            model: super::mapped(owner, |e| e.string(model))?,
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
        self.media.validate_registrations(&doc, 0)?;
        normalize(owner, &mut doc, self.operation, &self.model)?;
        self.media.relocate_ingress(&doc, 0)?;
        super::request_content::register(&mut doc, &mut self.media)?;
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
        crate::boundary::enter(self.owner, owner, self.closed)?;
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
        self.closed = result.is_err();
        crate::boundary::reported(result, &mut self.emitter, owner, true)
    }
    fn finish<'a>(&'a mut self, owner: &mut Writer, destination: &impl Capability) -> Result<ValidatedRequest<'a>> {
        crate::boundary::enter(self.owner, owner, self.closed)?;
        self.closed = true;
        let prepared = self.prepare(owner);
        crate::boundary::reported(prepared, &mut self.emitter, owner, true)?;
        let view = RequestView {
            document: self.document.as_ref().ok_or(Error::Schema)?,
            media: &self.media,
            operation: self.operation,
        };
        crate::boundary::end_request(&mut self.fence, view, destination, &mut self.emitter, owner)
    }
}
pub struct GeminiCapability;
impl Capability for GeminiCapability {
    fn destination(&self) -> &'static str {
        "gemini"
    }
    fn validate(&self, request: RequestView<'_>) -> Result<()> {
        super::request_schema::validate(request)
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
        encode::scratch(owner, |e| super::request_encode::write(view, e))
    }
}
const CONFIG: &[(&str, &str)] = &[
    ("temperature", "temperature"),
    ("topP", "top_p"),
    ("maxOutputTokens", "max_tokens"),
    ("stopSequences", "stop"),
    ("candidateCount", "n"),
    ("seed", "seed"),
    ("responseModalities", "modalities"),
];
pub(super) fn config() -> &'static [(&'static str, &'static str)] {
    CONFIG
}
fn normalize(owner: &mut Writer, doc: &mut Document, operation: Operation, model: &Document) -> Result<()> {
    let x = envelope(owner, doc, operation, model)?;
    let root = doc.root();
    let provider = doc.set(x, "provider_parameters", Value::Object)?;
    let native = doc.set(provider, "gemini", Value::Object)?;
    for key in ["cachedContent", "safetySettings", "labels", "toolConfig"] {
        if let Some(n) = doc.remove(root, key)? {
            doc.append(native, n)?;
        }
    }
    if let Some(config) = doc.remove(root, "generationConfig")? {
        // A null config is unset in protobuf JSON: it maps nothing and returns as it came.
        if !matches!(doc.node(config)?.value, Value::Null) {
            generation_config(doc, config, x)?;
        }
        doc.append(native, config)?;
    }
    messages(owner, doc)?;
    if let Some(tools) = doc.remove(root, "tools")? {
        normalize_tools(doc, tools, x)?;
    }
    Ok(())
}
/// Starts `x_ck`, unwraps a count request's `generateContentRequest`, preserves unknown root
/// members and sets the URL model. Returns the `x_ck` node.
fn envelope(owner: &mut Writer, doc: &mut Document, operation: Operation, model: &Document) -> Result<usize> {
    let root = doc.root();
    super::object(doc, root)?;
    let collision = doc.remove(root, "x_ck")?;
    let x = doc.set(root, "x_ck", Value::Object)?;
    doc.set(x, "v", Value::Unsigned(1))?;
    doc.set(x, "original_protocol", Value::Text("gemini"))?;
    let operation_name = if operation == Operation::Generate {
        "generate"
    } else {
        "count_tokens"
    };
    doc.set(x, "operation", Value::Text(operation_name))?;
    doc.set(x, "original", Value::Array)?;
    doc.set(x, "replay", Value::Array)?;
    if let Some(n) = collision {
        preserve(owner, doc, root, n, None)?;
    }
    if let Some(wrapper) = doc.remove(root, "generateContentRequest")? {
        if operation != Operation::CountTokens || doc.member(root, "contents")?.is_some() {
            return Err(Error::Schema);
        }
        super::object(doc, wrapper)?;
        doc.set(x, "gemini_count_wrapper", Value::Bool(true))?;
        capture(owner, doc, root, &["x_ck"], None)?;
        while let Some(n) = doc.node(wrapper)?.first {
            doc.detach(wrapper, n)?;
            doc.append(root, n)?;
        }
    }
    capture(owner, doc, root, ROOT_KNOWN, None)?;
    if let Some(n) = doc.remove(root, "model")? {
        preserve(owner, doc, root, n, Some("body_model"))?;
    }
    let value = encode::scratch(owner, |e| model.write(model.root(), e))?;
    let value = doc.import_json(owner, value)?;
    doc.rename(value, "model")?;
    doc.append(root, value)?;
    Ok(x)
}
/// Root members with a canonical home; the rest stay as provenance.
const ROOT_KNOWN: &[&str] = &[
    "x_ck",
    "model",
    "contents",
    "systemInstruction",
    "generationConfig",
    "tools",
    "toolConfig",
    "cachedContent",
    "safetySettings",
    "labels",
];
/// Moves the mapped `generationConfig` members to the root, thinking to `x_ck.thinking` and
/// the response schema to `response_format`; the rest stays in the native config.
fn generation_config(doc: &mut Document, config: usize, x: usize) -> Result<()> {
    let root = doc.root();
    super::object(doc, config)?;
    for &(wire, canonical) in CONFIG {
        if let Some(n) = doc.remove(config, wire)? {
            doc.rename(n, canonical)?;
            doc.append(root, n)?;
        }
    }
    if let Some(thinking) = doc.remove(config, "thinkingConfig")? {
        super::object(doc, thinking)?;
        let normalized = doc.set(x, "thinking", Value::Object)?;
        for (wire, canonical) in [
            ("thinkingBudget", "budget_tokens"),
            ("includeThoughts", "include"),
            ("thinkingLevel", "effort"),
        ] {
            if let Some(n) = doc.remove(thinking, wire)? {
                doc.rename(n, canonical)?;
                doc.append(normalized, n)?;
            }
        }
        doc.rename(thinking, "native")?;
        doc.append(normalized, thinking)?;
    }
    const SCHEMA: [&str; 3] = ["responseMimeType", "responseSchema", "responseJsonSchema"];
    if SCHEMA.iter().try_fold(false, |found, key| {
        Ok::<_, Error>(found || doc.member(config, key)?.is_some())
    })? {
        let format = doc.set(root, "response_format", Value::Object)?;
        doc.set(format, "type", Value::Text("json_schema"))?;
        for key in SCHEMA {
            if let Some(n) = doc.remove(config, key)? {
                doc.append(format, n)?;
            }
        }
    }
    Ok(())
}
/// Turns `systemInstruction` and `contents` into canonical messages, system first.
fn messages(owner: &mut Writer, doc: &mut Document) -> Result<()> {
    let root = doc.root();
    let contents = doc.remove(root, "contents")?.ok_or(Error::Schema)?;
    super::array(doc, contents)?;
    let messages = doc.set(root, "messages", Value::Array)?;
    if let Some(system) = doc.remove(root, "systemInstruction")? {
        super::object(doc, system)?;
        doc.set(system, "role", Value::Text("system"))?;
        doc.append(messages, system)?;
    }
    while let Some(message) = doc.node(contents)?.first {
        doc.detach(contents, message)?;
        doc.append(messages, message)?;
    }
    let mut message = doc.node(messages)?.first;
    let mut index = 0;
    let mut calls = super::request_content::Calls::new(owner)?;
    while let Some(n) = message {
        message = doc.node(n)?.next;
        super::request_content::normalize(owner, doc, n, index, &mut calls)?;
        index += 1;
    }
    Ok(())
}
/// Function declarations become Chat tools; other native tool groups stay in `x_ck.tools`.
fn normalize_tools(doc: &mut Document, tools: usize, x: usize) -> Result<()> {
    super::array(doc, tools)?;
    let root = doc.root();
    let normalized = doc.set(root, "tools", Value::Array)?;
    let native_tools = doc.set(x, "tools", Value::Array)?;
    while let Some(group) = doc.node(tools)?.first {
        doc.detach(tools, group)?;
        if let Some(functions) = doc.remove(group, "functionDeclarations")? {
            super::array(doc, functions)?;
            while let Some(function) = doc.node(functions)?.first {
                doc.detach(functions, function)?;
                let tool = doc.add(None, Value::Object)?;
                doc.append(normalized, tool)?;
                doc.set(tool, "type", Value::Text("function"))?;
                doc.rename(function, "function")?;
                doc.append(tool, function)?;
            }
        }
        if doc.node(group)?.first.is_some() {
            doc.append(native_tools, group)?;
        }
    }
    Ok(())
}
pub(super) fn capture(
    owner: &mut Writer,
    doc: &mut Document,
    parent: usize,
    known: &[&str],
    carrier: Option<&'static str>,
) -> Result<()> {
    let mut next = doc.node(parent)?.first;
    while let Some(n) = next {
        next = doc.node(n)?.next;
        let name = doc.node(n)?.name.ok_or(Error::Schema)?;
        if known
            .iter()
            .try_fold(false, |yes, key| Ok::<_, Error>(yes || doc.name_is(name, key)?))?
        {
            continue;
        }
        doc.detach(parent, n)?;
        preserve(owner, doc, parent, n, carrier)?;
    }
    Ok(())
}
pub(super) fn preserve(
    owner: &mut Writer,
    doc: &mut Document,
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
    let x = doc.required(doc.root(), "x_ck")?;
    let original = doc.required(x, "original")?;
    let entry = doc.add(None, Value::Object)?;
    doc.append(original, entry)?;
    doc.set(entry, "protocol", Value::Text("gemini"))?;
    doc.append(entry, pointer)?;
    doc.rename(field, "value")?;
    doc.append(entry, field)?;
    doc.bind_origin(entry, parent, carrier)
}
pub(super) fn originals(doc: &Document, parent: usize, carrier: Option<&str>, e: &mut Encoder<'_, '_>) -> Result<()> {
    for entry in doc.origins(parent)? {
        let (_, origin_carrier) = doc.origin(entry)?.ok_or(Error::Schema)?;
        // Another protocol's originals are its own fields, not Gemini's.
        if origin_carrier != carrier || !super::is(doc, entry, "protocol", "gemini")? {
            continue;
        }
        let pointer = doc.required(entry, "pointer")?;
        let Value::String(text) = doc.node(pointer)?.value else {
            return Err(Error::Schema);
        };
        let name = Name::Pointer(text);
        let mut shadowed = false;
        for child in doc.children(parent)? {
            if let Some(current) = doc.node(child)?.name {
                if parent == doc.root() && doc.name_is(current, "x_ck")? {
                    continue;
                }
                shadowed |= doc.names_equal(name, current)?;
            }
        }
        if shadowed {
            continue;
        }
        e.bytes(b",")?;
        doc.write_name(name, e)?;
        e.bytes(b":")?;
        doc.write(doc.required(entry, "value")?, e)?;
    }
    Ok(())
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
