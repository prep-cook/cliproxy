use crate::codec::{
    self, Capability, Error, Operation, RequestFence, RequestView, Result, ValidatedRequest,
    document::{Document, Name, Value},
    encode,
    event::{Emitter, Identity, Kind, Scope},
    media::Registry,
};
use ck_stream::{ScratchLease, Writer};

mod input;
mod normalize;
mod validate;
mod write;

use normalize::normalize;
use validate::validate;
use write::write_request;

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
    fn consume(&mut self, owner: &mut Writer, flush: bool) -> Result<()> {
        self.input.consume(
            owner,
            &mut self.media,
            &mut self.emitter,
            &mut self.reader,
            self.operation,
            flush,
        )
    }
    fn prepare(&mut self, owner: &mut Writer) -> Result<()> {
        self.input.pending.finish()?;
        self.consume(owner, true)?;
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
        crate::boundary::enter(self.owner, owner, self.closed)?;
        let result = (|| {
            for chunk in bytes.chunks(8192) {
                self.input.pending.advance(owner, chunk)?;
                self.consume(owner, false)?;
            }
            if bytes.is_empty() {
                self.consume(owner, true)?;
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
            document: &self.input.pending.document,
            media: &self.media,
            operation: self.operation,
        };
        crate::boundary::end_request(&mut self.fence, view, destination, &mut self.emitter, owner)
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
