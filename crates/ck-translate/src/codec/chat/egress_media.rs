use super::*;
use crate::codec::{event::LocalId, media};

pub(super) struct Projection {
    pub(super) id: LocalId,
    document: Option<Document>,
    value: usize,
    data: usize,
    field: &'static str,
    /// The media kind is a document, so the payload is `file.file_data` rather than
    /// `image_url.url`, whatever other members the part carries.
    document_kind: bool,
    choice: u64,
    next: u64,
    offset: u64,
    live: bool,
    reachable: bool,
    /// A trusted edit gave the part a URI or file source: it is delivered without the bytes.
    external: bool,
    ended: bool,
}

/// Where a projected part's payload goes.
enum Source {
    /// The null member that receives the decoded bytes.
    Bound(usize),
    /// A non-null image URL or document `file_data`, or a document `file_id` without data.
    External,
    /// No carrier or data member: the part is unreachable.
    Missing,
}

/// Classifies the payload member of a supplied attributes value after its carrier matches the
/// media kind. Audio and scalar text accept only a null bound member.
fn source(doc: &Document, value: usize, field: &str, scalar: bool, document_kind: bool) -> Result<Source> {
    let (carrier, data) = if scalar {
        (None, Some(value))
    } else if field == "audio" {
        (None, doc.member(value, "data")?)
    } else if document_kind && let Some(file) = doc.member(value, "file")? {
        (Some(file), doc.member(file, "file_data")?)
    } else if let Some(image) = doc.member(value, "image_url")? {
        (None, doc.member(image, "url")?)
    } else {
        return Ok(Source::Missing);
    };
    Ok(match data {
        Some(data) if matches!(doc.node(data)?.value, Value::Null) => Source::Bound(data),
        Some(_) if !scalar && field != "audio" => Source::External,
        Some(_) => return Err(Error::Schema),
        None if carrier
            .map(|file| doc.member(file, "file_id"))
            .transpose()?
            .flatten()
            .is_some() =>
        {
            Source::External
        }
        None => Source::Missing,
    })
}

/// The member of a part value that holds its payload: audio `data`, document `file_data` or
/// image `url`.
fn payload_member(doc: &Document, value: usize, field: &str, document_kind: bool) -> Result<usize> {
    if field == "audio" {
        doc.required(value, "data")
    } else if document_kind {
        doc.required(doc.required(value, "file")?, "file_data")
    } else {
        doc.required(doc.required(value, "image_url")?, "url")
    }
}

/// Maps the attributes field name onto the native message member it fills.
fn media_field(doc: &Document, name: usize) -> Result<&'static str> {
    Ok(if doc.string_is(name, "audio")? {
        "audio"
    } else if doc.string_is(name, "images")? {
        "images"
    } else if doc.string_is(name, "content")? {
        "content"
    } else if doc.string_is(name, "refusal")? {
        "refusal"
    } else if doc.string_is(name, "reasoning_content")? {
        "reasoning_content"
    } else {
        return Err(Error::Unsupported);
    })
}

/// Moves a part's carrier to the native member for its media kind (file for a document,
/// image_url otherwise) and sets the part type to match. A part that already has its native
/// carrier keeps the other member as an unknown field.
fn native_carrier(doc: &mut Document, value: usize, document_kind: bool) -> Result<()> {
    let (old, new, old_data, new_data, native_type) = if document_kind {
        ("image_url", "file", "url", "file_data", "file")
    } else {
        ("file", "image_url", "file_data", "url", "image_url")
    };
    if doc.member(value, new)?.is_some() {
        return Ok(());
    }
    if let Some(carrier) = doc.remove(value, old)? {
        doc.rename(carrier, new)?;
        if let Some(data) = doc.member(carrier, old_data)? {
            doc.rename(data, new_data)?;
        }
        doc.append(value, carrier)?;
        doc.set(value, "type", Value::Text(native_type))?;
    }
    Ok(())
}

/// Prepares the node that receives decoded media bytes: the data node itself for text, audio
/// and documents without a MIME type, otherwise a base64 child after a `data:` URL prefix.
fn binary_node(
    owner: &mut Writer,
    doc: &mut Document,
    data: usize,
    field: &'static str,
    scalar: bool,
    document_kind: bool,
) -> Result<usize> {
    Ok(if scalar {
        doc.replace(data, Value::Utf8Media)?;
        data
    } else if field == "audio" || (document_kind && doc.member(doc.root(), "mime")?.is_none()) {
        doc.replace(data, Value::Base64Fragments)?;
        data
    } else {
        let mime = doc.member(doc.root(), "mime")?;
        let prefix = encode::scratch(owner, |e| {
            e.bytes(b"\"data:")?;
            if let Some(mime) = mime {
                doc.write_contents(mime, e)?;
            }
            e.bytes(b";base64,\"")
        })?;
        let prefix = doc.import_json(owner, prefix)?;
        doc.replace(data, Value::Fragments)?;
        doc.append(data, prefix)?;
        let binary = doc.add(None, Value::Base64Fragments)?;
        doc.append(data, binary)?;
        binary
    })
}

impl Projection {
    pub(super) fn is_open(&self) -> bool {
        !self.ended
    }
    pub(super) fn reachable(&self) -> bool {
        self.reachable
    }
    pub(super) fn start(owner: &mut Writer, mut doc: Document, x: usize) -> Result<Self> {
        let id = media::media_id(&doc, x)?;
        let choice = doc.unsigned(doc.required(x, "choice_index")?)?;
        let Some(attributes) = doc.member(doc.root(), "attributes")? else {
            // Retain only the lifetime counters after deletion of the entire carrier.
            return Ok(Self {
                id,
                document: None,
                value: 0,
                data: 0,
                field: "content",
                document_kind: false,
                choice,
                next: 0,
                offset: 0,
                live: true,
                reachable: false,
                external: false,
                ended: false,
            });
        };
        let name = doc.required(attributes, "field")?;
        let field = media_field(&doc, name)?;
        let supplied_value = doc.member(attributes, "value")?;
        let value = match supplied_value {
            Some(value) => value,
            None => doc.add(None, Value::Null)?,
        };
        let scalar = doc
            .member(attributes, "scalar")?
            .map(|node| doc.node(node))
            .transpose()?
            .is_some_and(|node| matches!(node.value, Value::Bool(true)));
        let document_kind = doc.string_is(doc.required(doc.root(), "kind")?, "document")?;
        if !scalar && field != "audio" && supplied_value.is_some() {
            native_carrier(&mut doc, value, document_kind)?;
        }
        let source = match supplied_value {
            None => Source::Missing,
            Some(value) => source(&doc, value, field, scalar, document_kind)?,
        };
        let (reachable, external) = (matches!(source, Source::Bound(_)), matches!(source, Source::External));
        let data = match source {
            Source::Bound(data) => data,
            Source::External | Source::Missing => doc.add(None, Value::Null)?,
        };
        let binary = binary_node(owner, &mut doc, data, field, scalar, document_kind)?;
        Ok(Self {
            id,
            document: Some(doc),
            value,
            data: binary,
            field,
            document_kind,
            choice,
            next: 0,
            offset: 0,
            live: field == "audio" || scalar,
            reachable,
            external,
            ended: false,
        })
    }

    pub(super) fn data(&mut self, mut incoming: Document) -> Result<Option<(Document, usize, &'static str, u64)>> {
        let root = incoming.root();
        if incoming.unsigned(incoming.required(root, "index")?)? != self.next
            || incoming.unsigned(incoming.required(root, "offset")?)? != self.offset
        {
            return Err(Error::Schema);
        }
        let bytes = incoming.remove(root, "bytes")?.ok_or(Error::Schema)?;
        let Value::String(text) = incoming.node(bytes)?.value else {
            return Err(Error::Schema);
        };
        let mut count = 0u64;
        media::decode_base64(&incoming, text, |bytes| {
            count += bytes.len() as u64;
            Ok(())
        })?;
        if self.ended {
            return Err(Error::Closed);
        }
        self.next = self.next.checked_add(1).ok_or(Error::Limit)?;
        self.offset = self.offset.checked_add(count).ok_or(Error::Limit)?;
        if self.live {
            if !self.reachable {
                return Ok(None);
            }
            let value = incoming.add(
                None,
                if self.field == "audio" {
                    Value::Object
                } else {
                    Value::Utf8Media
                },
            )?;
            let data = if self.field == "audio" {
                incoming.set(value, "data", Value::Base64Fragments)?
            } else {
                value
            };
            incoming.append(data, bytes)?;
            return Ok(Some((incoming, value, self.field, self.choice)));
        }
        let doc = self.document.as_mut().ok_or(Error::Closed)?;
        let base = doc.absorb(incoming)? - root;
        doc.append(self.data, base + bytes)?;
        Ok(None)
    }

    pub(super) fn transcript(
        &mut self,
        mut incoming: Document,
    ) -> Result<Option<(Document, usize, &'static str, u64)>> {
        let root = incoming.root();
        let text = incoming.remove(root, "text")?.ok_or(Error::Schema)?;
        if self.ended {
            return Err(Error::Closed);
        }
        if self.live {
            if !self.reachable {
                return Ok(None);
            }
            let value = incoming.add(None, Value::Object)?;
            incoming.rename(text, "transcript")?;
            incoming.append(value, text)?;
            return Ok(Some((incoming, value, self.field, self.choice)));
        }
        let doc = self.document.as_mut().ok_or(Error::Closed)?;
        if doc.member(self.value, "transcript")?.is_some() {
            return Err(Error::Schema);
        }
        let text = doc.absorb(incoming)? - root + text;
        doc.rename(text, "transcript")?;
        doc.append(self.value, text)?;
        Ok(None)
    }

    pub(super) fn attributes(
        &mut self,
        mut incoming: Document,
    ) -> Result<Option<(Document, usize, &'static str, u64)>> {
        if !self.reachable {
            return Ok(None);
        }
        let value = incoming.required(incoming.root(), "value")?;
        if !incoming.string_is(incoming.required(incoming.root(), "format")?, "json")? {
            return Err(Error::Schema);
        }
        if self.live {
            if !self.reachable {
                return Ok(None);
            }
            incoming.remove(value, "data")?;
            return Ok(Some((incoming, value, self.field, self.choice)));
        }
        let doc = self.document.as_mut().ok_or(Error::Closed)?;
        let old_data = payload_member(doc, self.value, self.field, self.document_kind)?;
        let root = incoming.root();
        let value = doc.absorb(incoming)? - root + value;
        let data = payload_member(doc, value, self.field, self.document_kind)?;
        if !matches!(doc.node(data)?.value, Value::Null) {
            return Err(Error::Schema);
        }
        doc.transplant(data, old_data)?;
        if self.field == "audio" {
            self.data = data;
        }
        self.value = value;
        Ok(None)
    }

    pub(super) fn end(&mut self, incoming: &Document) -> Result<Option<(Document, usize, &'static str, u64)>> {
        if incoming.unsigned(incoming.required(incoming.root(), "bytes")?)? != self.offset
            || !incoming.string_is(incoming.required(incoming.root(), "status")?, "completed")?
        {
            return Err(Error::Schema);
        }
        if self.ended {
            return Err(Error::Closed);
        }
        self.ended = true;
        if self.live || !(self.reachable || self.external) {
            return Ok(None);
        }
        Ok(Some((
            self.document.take().ok_or(Error::Closed)?,
            self.value,
            self.field,
            self.choice,
        )))
    }

    pub(super) fn initial(&mut self) -> Result<Option<(Document, usize, &'static str, u64)>> {
        if !self.live {
            return Ok(None);
        }
        if !self.reachable || self.field != "audio" {
            return Ok(None);
        }
        let mut doc = self.document.take().ok_or(Error::Closed)?;
        doc.remove(self.value, "data")?;
        if doc.node(self.value)?.first.is_none() {
            return Ok(None);
        }
        Ok(Some((doc, self.value, self.field, self.choice)))
    }
}

impl Reduction {
    pub(super) fn native_field(
        &mut self,
        owner: &mut Writer,
        mut native: Document,
        value: usize,
        field: &'static str,
        choice: u64,
    ) -> Result<Document> {
        if let Some(parent) = native.node(value)?.parent {
            native.detach(parent, value)?;
        }
        let native_root = native.root();
        let mut projection = mapped_document(owner, |e| {
            e.bytes(b"{")?;
            chunk_base(self.document.as_ref().ok_or(Error::Schema)?, e)?;
            e.bytes(b",\"choices\":[{\"index\":")?;
            e.unsigned(choice)?;
            e.bytes(b",\"delta\":{},\"finish_reason\":null}]}")
        })?;
        let imported = projection.absorb(native)? - native_root + value;
        let choices = projection.required(projection.root(), "choices")?;
        let choice = projection.node(choices)?.first.ok_or(Error::Schema)?;
        let delta = projection.required(choice, "delta")?;
        if field == "audio" || matches!(projection.node(imported)?.value, Value::Utf8Media) {
            projection.rename(imported, field)?;
            projection.append(delta, imported)?;
        } else {
            let array = projection.set(delta, field, Value::Array)?;
            projection.append(array, imported)?;
        }
        Ok(projection)
    }
}
