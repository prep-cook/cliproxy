use super::index::Index;
use crate::codec::{
    Error, Result,
    document::{Document, Name, Text, Value},
    event::{Kind, LocalId},
    internal,
    media::{self, Encoding, Registry},
    provenance,
    table::Table,
};
use ck_stream::{MetadataPermit, Reader, Writer};

/// One media payload's records, from its start to its end.
struct Payload {
    id: LocalId,
    kind: &'static str,
    encoding: Encoding,
    chunks: Option<Table<Text>>,
    next: u64,
    offset: u64,
    ended: bool,
}

/// Rebuild a request from trusted canonical records: its topology, provenance bindings and
/// every published media payload, registered at its reference. A reference is trusted only as
/// the entire value of a request field; the same shape inside other data stays literal.
pub fn read_request(owner: &mut Writer, reader: &mut Reader) -> Result<(Document, Registry)> {
    let mut doc = Document::streaming(owner)?;
    let mut payloads: Table<Payload> = Table::new(owner)?;
    let mut references: Table<(LocalId, usize)> = Table::new(owner)?;
    let mut counts = Counts {
        children: Index::new(),
        objects: Index::new(),
        members: Index::new(),
        builds: 0,
        permit: owner.reserve_metadata(0)?,
    };
    let mut started = false;
    loop {
        let (kind, event) = internal::next(owner, reader)?;
        match kind {
            Kind::RequestStart if !started => started = true,
            Kind::RequestField if started => {
                let node = field(&mut doc, &mut counts, event)?;
                if let Some(id) = reference(&doc, node)? {
                    references.push((id, node))?;
                }
            }
            Kind::MediaStart if started && payloads.last().is_none_or(|open| open.ended) => {
                payloads.push(start(owner, &event)?)?;
            }
            Kind::Data | Kind::MediaEnd if started => {
                let open = payloads.last_mut().filter(|open| !open.ended).ok_or(Error::Schema)?;
                let root = event.root();
                if media::media_id(&event, event.required(root, "x_ck")?)? != open.id {
                    return Err(Error::Schema);
                }
                if kind == Kind::MediaEnd {
                    // Only a completed payload whose data records add up to its count is trusted.
                    if event.unsigned(event.required(root, "bytes")?)? != open.offset
                        || !event.string_is(event.required(root, "status")?, "completed")?
                    {
                        return Err(Error::Schema);
                    }
                    open.ended = true;
                    continue;
                }
                if event.unsigned(event.required(root, "index")?)? != open.next
                    || event.unsigned(event.required(root, "offset")?)? != open.offset
                {
                    return Err(Error::Schema);
                }
                let Value::String(text) = event.node(event.required(root, "bytes")?)?.value else {
                    return Err(Error::Schema);
                };
                let mut decoded = 0u64;
                media::decode_base64(&event, text, |bytes| {
                    decoded += bytes.len() as u64;
                    Ok(())
                })?;
                open.offset = open.offset.checked_add(decoded).ok_or(Error::Limit)?;
                let text = doc.retain_text(event, text)?;
                open.chunks.as_mut().ok_or(Error::Schema)?.push(text)?;
                open.next += 1;
            }
            Kind::RequestEnd if started => {
                provenance::restore_bindings(&mut doc)?;
                let media = register(owner, &mut doc, &mut payloads, &mut references)?;
                return Ok((doc, media));
            }
            _ => return Err(Error::Schema),
        }
    }
}

/// Indexes of the containers that fields fill, so placing a field never walks earlier siblings.
/// An array keeps its child count and last child: fields arrive in order, so the element a field
/// appends or descends into is nearly always the last one. An object maps the hash of each
/// member name to the member, under the build that indexed it; a field whose hash is absent
/// names a new member, and a hash collision falls back to a scan.
struct Counts {
    children: Index<usize, (usize, Option<usize>)>,
    /// The current build of each indexed object. Replacing an object's value drops its build,
    /// so the next field rebuilds the index and entries of earlier builds are never read.
    objects: Index<usize, u32>,
    members: Index<(usize, u32, u64), usize>,
    builds: u32,
    /// The charge of all three indexes.
    permit: MetadataPermit,
}

/// Hashes decoded name characters; equal names, as `Segment::matches` compares them, hash equal.
fn name_hash(mut next: impl FnMut() -> Result<Option<char>>) -> Result<u64> {
    use std::hash::Hasher;
    let mut hasher = std::hash::DefaultHasher::new();
    while let Some(c) = next()? {
        hasher.write_u32(c as u32);
    }
    Ok(hasher.finish())
}

impl Counts {
    /// Forgets the indexes of a container whose value a field replaced.
    fn replaced(&mut self, node: usize) {
        self.children.remove(&node);
        self.objects.remove(&node);
    }

    /// Indexes one member under its object's current build. The first member with a name keeps
    /// the entry, as a scan would find it first.
    fn index_member(&mut self, doc: &Document, object: usize, build: u32, child: usize) -> Result<()> {
        let mut chars = doc.name_chars(doc.node(child)?.name.ok_or(Error::Schema)?)?;
        let key = (object, build, name_hash(|| chars.next_scalar())?);
        if !self.members.contains_key(&key) {
            self.members.insert(&mut self.permit, key, child)?;
        }
        Ok(())
    }

    /// The build of an object's member index, indexing every member on first use.
    fn build(&mut self, doc: &Document, object: usize) -> Result<u32> {
        if let Some(&build) = self.objects.get(&object) {
            return Ok(build);
        }
        self.builds = self.builds.checked_add(1).ok_or(Error::Limit)?;
        let build = self.builds;
        self.objects.insert(&mut self.permit, object, build)?;
        for child in doc.children(object)? {
            self.index_member(doc, object, build, child)?;
        }
        Ok(build)
    }

    /// The member of `object` that `name` names.
    fn member(&mut self, doc: &Document, object: usize, name: &provenance::Segment) -> Result<Option<usize>> {
        let build = self.build(doc, object)?;
        let mut chars = name.clone();
        let Some(&child) = self.members.get(&(object, build, name_hash(|| chars.next())?)) else {
            return Ok(None);
        };
        if name.matches(doc, doc.node(child)?.name.ok_or(Error::Schema)?)? {
            return Ok(Some(child));
        }
        for child in doc.children(object)? {
            if name.matches(doc, doc.node(child)?.name.ok_or(Error::Schema)?)? {
                return Ok(Some(child));
            }
        }
        Ok(None)
    }

    fn get(&mut self, doc: &Document, array: usize) -> Result<&mut (usize, Option<usize>)> {
        if !self.children.contains_key(&array) {
            let (mut count, mut last) = (0, None);
            for child in doc.children(array)? {
                count += 1;
                last = Some(child);
            }
            self.children.insert(&mut self.permit, array, (count, last))?;
        }
        self.children.get_mut(&array).ok_or(Error::Schema)
    }
}

/// Places one request field at its pointer and returns the node that holds its value. Only the
/// record's pointer and value are kept; its envelope nodes are dropped with the record.
fn field(doc: &mut Document, counts: &mut Counts, mut event: Document) -> Result<usize> {
    let root = event.root();
    let Value::String(pointer) = event.node(event.required(root, "pointer")?)?.value else {
        return Err(Error::Schema);
    };
    let value = event.required(root, "value")?;
    let adopted = doc.adopt_record(&mut event)?;
    let pointer = adopted.text(pointer)?;
    if doc.text_chars(pointer)?.next_scalar()?.is_none() {
        let value = doc.copy_adopted(&event, value, adopted, None)?;
        doc.transplant(doc.root(), value)?;
        counts.replaced(doc.root());
        return Ok(doc.root());
    }
    let parent = provenance::resolve_parent_indexed(doc, doc.text_chars(pointer)?, &mut |array, index| {
        let (count, last) = *counts.get(doc, array)?;
        Ok(last.filter(|_| index + 1 == count))
    })?;
    let array = matches!(doc.node(parent)?.value, Value::Array);
    let name = provenance::leaf(doc.text_chars(pointer)?)?;
    let target = if array {
        let index = name.index()?;
        let (count, _) = *counts.get(doc, parent)?;
        if index > count {
            return Err(Error::Schema);
        }
        // Fields arrive in order, so a new element is the common case; replacing one walks.
        // ponytail: the grammar forbids that replacement and Chat ingress never emits it; index
        // elements if a producer starts to (docs/DEFERRED-OPTIONS.md).
        if index == count {
            None
        } else {
            doc.children(parent)?.nth(index)
        }
    } else {
        counts.member(doc, parent, &name)?
    };
    if let Some(target) = target {
        let value = doc.copy_adopted(&event, value, adopted, None)?;
        doc.transplant(target, value)?;
        counts.replaced(target);
        return Ok(target);
    }
    let node = doc.copy_adopted(&event, value, adopted, (!array).then_some(Name::Pointer(pointer)))?;
    doc.append(parent, node)?;
    if let Some((count, last)) = counts.children.get_mut(&parent) {
        *count += 1;
        *last = Some(node);
    }
    if let Some(&build) = counts.objects.get(&parent) {
        counts.index_member(doc, parent, build, node)?;
    }
    Ok(node)
}

/// Returns the media ID of a node holding exactly a media reference.
fn reference(doc: &Document, node: usize) -> Result<Option<LocalId>> {
    if !matches!(doc.node(node)?.value, Value::Object) || doc.children(node)?.count() != 3 {
        return Ok(None);
    }
    let Some(kind) = doc.member(node, "type")? else {
        return Ok(None);
    };
    if !doc.string_is(kind, "x_ck_media")? || doc.member(node, "value_kind")?.is_none() {
        return Ok(None);
    }
    media::media_id(doc, node).map(Some)
}

/// Opens a payload from its media start; the kind and MIME presence select its encoding.
fn start(owner: &mut Writer, event: &Document) -> Result<Payload> {
    let root = event.root();
    let id = media::media_id(event, event.required(root, "x_ck")?)?;
    let kind = event.required(root, "kind")?;
    let (kind, encoding) = if event.string_is(kind, "text")? {
        ("text", Encoding::Utf8)
    } else if event.string_is(kind, "audio")? {
        ("audio", Encoding::Base64)
    } else if event.string_is(kind, "image")? {
        ("image", Encoding::DataUri { skip: 0 })
    } else if event.string_is(kind, "document")? {
        let uri = event.member(root, "mime")?.is_some();
        (
            "document",
            if uri {
                Encoding::DataUri { skip: 0 }
            } else {
                Encoding::Base64
            },
        )
    } else {
        return Err(Error::Schema);
    };
    Ok(Payload {
        id,
        kind,
        encoding,
        chunks: Some(Table::new(owner)?),
        next: 0,
        offset: 0,
        ended: false,
    })
}

/// Registers every payload, in record order, at its single surviving reference.
fn register(
    owner: &mut Writer,
    doc: &mut Document,
    payloads: &mut Table<Payload>,
    references: &mut Table<(LocalId, usize)>,
) -> Result<Registry> {
    let mut registry = Registry::new(owner)?;
    // Sorted references let each payload find its reference without a cross scan.
    references.sort_unstable_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
    let mut matched = 0;
    for payload in payloads.iter_mut() {
        if !payload.ended {
            return Err(Error::Schema);
        }
        let id = payload.id.as_str();
        let first = references.partition_point(|(other, _)| other.as_str() < id);
        let mut nodes = references[first..].iter().take_while(|(other, _)| other.as_str() == id);
        let node = nodes.next().map(|(_, node)| *node);
        if nodes.next().is_some() {
            return Err(Error::Schema);
        }
        matched += usize::from(node.is_some());
        // A later field may have replaced the reference; then the payload is unreachable.
        let node = match node {
            Some(node) if reference(doc, node)? == Some(payload.id) => {
                let utf8 = doc.string_is(doc.required(node, "value_kind")?, "utf8")?;
                if utf8 != matches!(payload.encoding, Encoding::Utf8) {
                    return Err(Error::Schema);
                }
                Some(node)
            }
            _ => None,
        };
        let chunks = payload.chunks.take().ok_or(Error::Schema)?;
        registry.adopt(doc, node, (payload.id, payload.kind, payload.encoding), chunks)?;
    }
    // Every reference names a payload.
    if matched != references.len() {
        return Err(Error::Schema);
    }
    Ok(registry)
}
