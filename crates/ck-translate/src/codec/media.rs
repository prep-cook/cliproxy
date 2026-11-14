mod decode;
mod retained;

pub use decode::bytes;
pub(crate) use decode::{Base64Fragments, InputDecoder, decode_base64, write_base64_fragments};

use super::{
    Error, Result,
    document::{Document, Text, Value},
    encode::Encoder,
    event::{Emitter, Kind, LocalId, Scope},
    json::Decoded,
};
use ck_stream::{MetadataPermit, Writer};
use decode::{write_encoded_chunks, write_payload};

#[derive(Clone, Copy)]
pub enum Encoding {
    Base64,
    DataUri { skip: u64 },
    Utf8,
}

#[derive(Clone, Copy)]
pub struct Media {
    pub node: usize,
    pub id: LocalId,
    pub data: Option<Text>,
    pub encoding: Encoding,
    pub kind: &'static str,
    pub mime: Option<usize>,
}

struct Entry {
    media: Media,
    lineage: [Option<(usize, u64)>; 128],
    chunks: Option<super::table::Table<Text>>,
    active: bool,
}

pub struct Registry {
    entries: Vec<Entry>,
    /// The active entry registered at each node.
    active: std::collections::HashMap<usize, usize>,
    metadata: MetadataPermit,
    document: Option<u64>,
}

impl Registry {
    pub fn new(owner: &mut Writer) -> Result<Self> {
        Ok(Self {
            entries: Vec::new(),
            active: std::collections::HashMap::new(),
            metadata: owner.reserve_metadata(0)?,
            document: None,
        })
    }

    pub fn validate_document(&self, doc: &Document) -> Result<()> {
        self.metadata.check_live()?;
        if doc.stream_id() != self.metadata.stream_id() || self.document.is_some_and(|id| id != doc.id()) {
            return Err(Error::Position);
        }
        Ok(())
    }
    pub fn get(&self, doc: &Document, node: usize) -> Result<Option<Media>> {
        self.validate_document(doc)?;
        let Some(entry) = self.active.get(&node).map(|&index| &self.entries[index]) else {
            return Ok(None);
        };
        if lineage(doc, node)? != entry.lineage {
            return Err(Error::Position);
        }
        if matches!(entry.media.encoding, Encoding::Utf8) && matches!(doc.node(node)?.value, Value::Utf8Media) {
            return Ok(Some(entry.media));
        }
        let kind = doc.required(node, "type")?;
        let id = doc.required(node, "media_id")?;
        if !doc.string_is(kind, "x_ck_media")? || !doc.string_is(id, entry.media.id.as_str())? {
            return Err(Error::Position);
        }
        Ok(Some(entry.media))
    }
    pub fn register(
        &mut self,
        doc: &mut Document,
        node: usize,
        data: Text,
        encoding: Encoding,
        kind: &'static str,
        mime: Option<usize>,
    ) -> Result<Media> {
        self.validate_document(doc)?;
        self.document = Some(doc.id());
        self.reserve()?;
        let id = local_id(b'm', self.entries.len() as u64)?;
        let media = Media {
            node,
            id,
            data: Some(data),
            encoding,
            kind,
            mime,
        };
        if matches!(encoding, Encoding::Utf8) {
            doc.replace(node, Value::Utf8Media)?;
        } else {
            doc.replace(node, Value::Object)?;
            doc.set(node, "type", Value::Text("x_ck_media"))?;
            doc.set_id(node, "media_id", id)?;
            doc.set(node, "value_kind", Value::Text("bytes"))?;
        }
        self.entries.push(Entry {
            media,
            lineage: lineage(doc, node)?,
            chunks: None,
            active: true,
        });
        self.active.insert(node, self.entries.len() - 1);
        Ok(media)
    }
    /// Grows the entry table and node index, charging their metadata quota, before one more
    /// entry is pushed.
    fn reserve(&mut self) -> Result<()> {
        // A hash table slot holds the pair and one control byte.
        let slot = std::mem::size_of::<(usize, usize)>() + 1;
        if self.entries.len() == self.entries.capacity() {
            let capacity = self.entries.capacity().max(4).checked_mul(2).ok_or(Error::Limit)?;
            let charge = |entries: usize| {
                entries
                    .checked_mul(std::mem::size_of::<Entry>())
                    .and_then(|bytes| bytes.checked_add(entries.checked_mul(2)?.checked_mul(slot)?))
                    .ok_or(Error::Limit)
            };
            self.metadata.resize(charge(capacity)?)?;
            self.entries
                .try_reserve_exact(capacity - self.entries.len())
                .map_err(|_| Error::Limit)?;
            self.active
                .try_reserve(capacity.saturating_sub(self.active.len()))
                .map_err(|_| Error::Limit)?;
            self.metadata
                .resize(charge(self.entries.capacity().max(self.active.capacity() / 2))?)?;
        }
        Ok(())
    }
    pub fn entries(&self) -> impl Iterator<Item = Media> + '_ {
        self.entries
            .iter()
            .filter(|entry| entry.active)
            .map(|entry| entry.media)
    }
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
    pub(crate) fn at(&self, index: usize) -> Media {
        self.entries[index].media
    }

    pub(crate) fn validate_registrations(&self, doc: &Document, from: usize) -> Result<()> {
        for media in self.entries[from.min(self.entries.len())..]
            .iter()
            .filter(|entry| entry.active)
            .map(|entry| entry.media)
        {
            if lineage(doc, media.node)?
                .iter()
                .flatten()
                .any(|(node, _)| *node == doc.root())
            {
                self.get(doc, media.node)?.ok_or(Error::Position)?;
            }
        }
        Ok(())
    }

    /// Only ingress normalization may relocate bindings; ordinary get never refreshes lineage.
    /// Entries before `from` belong to earlier messages, which normalization leaves untouched.
    pub(crate) fn relocate_ingress(&mut self, doc: &Document, from: usize) -> Result<()> {
        self.validate_document(doc)?;
        for (index, entry) in self.entries.iter_mut().enumerate().skip(from) {
            if !entry.active {
                continue;
            }
            if matches!(entry.media.encoding, Encoding::Utf8)
                && !matches!(doc.node(entry.media.node)?.value, Value::Utf8Media)
            {
                entry.active = false;
                if self.active.get(&entry.media.node) == Some(&index) {
                    self.active.remove(&entry.media.node);
                }
            } else {
                entry.lineage = lineage(doc, entry.media.node)?;
            }
        }
        Ok(())
    }

    pub(crate) fn update_mime(&mut self, node: usize, mime: Option<usize>) {
        if let Some(&index) = self.active.get(&node) {
            self.entries[index].media.mime = mime;
        }
    }

    pub(crate) fn retain(
        &mut self,
        doc: &mut Document,
        index: usize,
        owner: &mut Writer,
        reader: &mut ck_stream::Reader,
    ) -> Result<()> {
        if self.retain_record(doc, index, owner, reader)? != Kind::MediaStart {
            return Err(Error::Schema);
        }
        while self.retain_record(doc, index, owner, reader)? != Kind::MediaEnd {}
        Ok(())
    }

    pub(crate) fn retain_record(
        &mut self,
        doc: &mut Document,
        index: usize,
        owner: &mut Writer,
        reader: &mut ck_stream::Reader,
    ) -> Result<Kind> {
        let (kind, event) = super::internal::next(owner, reader)?;
        let entry = &mut self.entries[index];
        match kind {
            Kind::MediaStart => {
                entry.chunks = Some(super::table::Table::new(owner)?);
                entry.media.data = None;
            }
            Kind::Data => {
                let node = event.required(event.root(), "bytes")?;
                let Value::String(text) = event.node(node)?.value else {
                    return Err(Error::Schema);
                };
                let text = doc.retain_text(event, text)?;
                entry.chunks.as_mut().ok_or(Error::Schema)?.push(text)?;
                if matches!(entry.media.encoding, Encoding::Utf8) {
                    let child = doc.add(None, Value::String(text))?;
                    doc.append(entry.media.node, child)?;
                }
            }
            Kind::MediaEnd => (),
            _ => return Err(Error::Schema),
        }
        Ok(kind)
    }

    /// Writes a registered payload in the form its current semantics select. A data URI payload
    /// takes its media type from `mime`, the part's current MIME member; without one the media
    /// type is empty. A plain base64 payload becomes a data URI when `uri_carrier` allows one and
    /// the part has a MIME, so a supplied media type is never dropped.
    pub fn write_value(
        &self,
        doc: &Document,
        node: usize,
        (mime, uri_carrier): (Option<usize>, bool),
        e: &mut Encoder<'_, '_>,
    ) -> Result<()> {
        let media = self.get(doc, node)?.ok_or(Error::Position)?;
        let entry = &self.entries[*self.active.get(&node).ok_or(Error::Position)?];
        if matches!(media.encoding, Encoding::Utf8) {
            let Some(chunks) = &entry.chunks else {
                let text = media.data.ok_or(Error::Position)?;
                return e.span(doc.source(text)?, text.span);
            };
            return e.quoted_json(|e| {
                for text in chunks.iter() {
                    decode_base64(doc, *text, |bytes| e.bytes(bytes))?;
                }
                Ok(())
            });
        }
        e.bytes(b"\"")?;
        if matches!(media.encoding, Encoding::DataUri { .. }) || (uri_carrier && mime.is_some()) {
            e.bytes(b"data:")?;
            if let Some(mime) = mime {
                doc.write_contents(mime, e)?;
            }
            e.bytes(b";base64,")?;
        }
        Self::write_bytes(entry, doc, media, e)?;
        e.bytes(b"\"")
    }

    /// Writes a registered payload as one plain base64 JSON string, whatever its original form:
    /// a data URI loses its prefix and text is encoded from its UTF-8 bytes. Protocol encoders
    /// whose carrier names the media type in a separate field call this.
    pub fn write_base64(&self, doc: &Document, node: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
        let media = self.get(doc, node)?.ok_or(Error::Position)?;
        let entry = &self.entries[*self.active.get(&node).ok_or(Error::Position)?];
        e.bytes(b"\"")?;
        Self::write_bytes(entry, doc, media, e)?;
        e.bytes(b"\"")
    }

    /// Writes the payload's bytes as base64 without quotes, from retained records when the
    /// payload has them and from its string otherwise.
    fn write_bytes(entry: &Entry, doc: &Document, media: Media, e: &mut Encoder<'_, '_>) -> Result<()> {
        match &entry.chunks {
            Some(chunks) => write_encoded_chunks(doc, chunks.iter().copied(), e),
            None => write_payload(doc, media, e),
        }
    }
}

fn lineage(doc: &Document, node: usize) -> Result<[Option<(usize, u64)>; 128]> {
    let mut result = [None; 128];
    let mut at = Some(node);
    for slot in &mut result {
        let Some(index) = at else {
            return Ok(result);
        };
        let node = doc.node(index)?;
        *slot = Some((index, node.revision));
        at = node.parent;
    }
    if at.is_some() {
        return Err(Error::Limit);
    }
    Ok(result)
}

/// Reads the media ID of a canonical record's `x_ck` correlation.
pub(crate) fn media_id(doc: &Document, x: usize) -> Result<LocalId> {
    let mut chars = doc.chars(doc.required(x, "media_id")?)?;
    let mut bytes = [0; 64];
    let mut len = 0;
    while let Some(c) = chars.next_scalar()? {
        if !c.is_ascii() || len == bytes.len() {
            return Err(Error::Schema);
        }
        bytes[len] = c as u8;
        len += 1;
    }
    LocalId::new(std::str::from_utf8(&bytes[..len]).map_err(|_| Error::Schema)?)
}

pub fn local_id(prefix: u8, mut index: u64) -> Result<LocalId> {
    let mut bytes = [0u8; 21];
    let mut start = bytes.len();
    loop {
        start -= 1;
        bytes[start] = b'0' + (index % 10) as u8;
        index /= 10;
        if index == 0 {
            break;
        }
    }
    start -= 1;
    bytes[start] = prefix;
    LocalId::new(std::str::from_utf8(&bytes[start..]).map_err(|_| Error::Schema)?)
}

pub fn publish(doc: &Document, media: Media, emitter: &mut Emitter, owner: &mut Writer, scope: Scope) -> Result<()> {
    publish_with_attributes(doc, media, emitter, owner, (scope, None), None, None)
}

pub(crate) struct Attributes<'a> {
    pub document: &'a Document,
    pub value: usize,
    pub mime: Option<usize>,
}

pub(crate) fn publish_with_attributes(
    doc: &Document,
    media: Media,
    emitter: &mut Emitter,
    owner: &mut Writer,
    (mut scope, fragment): (Scope, Option<&mut Base64Fragments>),
    attributes: Option<Attributes<'_>>,
    transcript: Option<usize>,
) -> Result<()> {
    scope.media_id = Some(media.id);
    let mut publisher = Publisher::start(emitter, owner, scope, |e| {
        e.bytes(b",\"kind\":")?;
        e.string(media.kind)?;
        e.bytes(b",\"encoding\":\"base64\"")?;
        if let Some(attributes) = &attributes {
            if let Some(mime) = attributes.mime {
                e.bytes(b",\"mime\":")?;
                attributes.document.write(mime, e)?;
            }
            e.bytes(b",\"attributes\":")?;
            attributes.document.write(attributes.value, e)?;
        } else if let Some(mime) = media.mime {
            e.bytes(b",\"mime\":")?;
            doc.write(mime, e)?;
        }
        Ok(())
    })?;
    publisher.collect(emitter, owner, |collect| {
        if let Some(fragment) = fragment {
            fragment.push(doc, media.data.ok_or(Error::Position)?, collect)
        } else {
            bytes(doc, media, collect)
        }
    })?;
    if let Some(transcript) = transcript {
        emitter.emit(
            owner,
            Kind::MediaTranscript,
            scope,
            |e| {
                e.bytes(b",\"text\":")?;
                doc.write(transcript, e)?;
                e.bytes(b",\"final\":true")
            },
            |_| Ok(()),
        )?;
    }
    publisher.end(emitter, owner)
}

pub fn write_reference(media: Media, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{\"type\":\"x_ck_media\",\"media_id\":")?;
    e.string(media.id.as_str())?;
    e.bytes(match media.encoding {
        Encoding::Base64 | Encoding::DataUri { .. } => b",\"value_kind\":\"bytes\"}",
        Encoding::Utf8 => b",\"value_kind\":\"utf8\"}",
    })
}

/// One media lifetime; payload is committed immediately, never retained on the heap.
pub(crate) struct Publisher {
    scope: Scope,
    index: u64,
    offset: u64,
    ended: bool,
}

impl Publisher {
    pub(crate) fn begin(doc: &Document, media: Media, emitter: &mut Emitter, owner: &mut Writer) -> Result<Self> {
        let scope = Scope {
            media_id: Some(media.id),
            ..Scope::default()
        };
        Self::start(emitter, owner, scope, |e| {
            e.bytes(b",\"kind\":")?;
            e.string(media.kind)?;
            e.bytes(b",\"encoding\":\"base64\"")?;
            if let Some(mime) = media.mime {
                e.bytes(b",\"mime\":")?;
                doc.write(mime, e)?;
            }
            Ok(())
        })
    }

    pub(crate) fn start(
        emitter: &mut Emitter,
        owner: &mut Writer,
        scope: Scope,
        fields: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
    ) -> Result<Self> {
        emitter.emit(owner, Kind::MediaStart, scope, fields, |_| Ok(()))?;
        Ok(Self {
            scope,
            index: 0,
            offset: 0,
            ended: false,
        })
    }

    pub(crate) fn append(&mut self, bytes: &[u8], emitter: &mut Emitter, owner: &mut Writer) -> Result<()> {
        use base64::Engine;
        if self.ended {
            return Err(Error::Closed);
        }
        if bytes.is_empty() {
            return Ok(());
        }
        if bytes.len() > emitter.data_capacity(self.scope, self.index, self.offset)? {
            return Err(Error::EventTooLarge);
        }
        let mut encoded = [0; 49_152];
        let count = base64::engine::general_purpose::STANDARD
            .encode_slice(bytes, &mut encoded)
            .map_err(|_| Error::Schema)?;
        emitter.emit(
            owner,
            Kind::Data,
            self.scope,
            |e| {
                e.bytes(b",\"index\":")?;
                e.unsigned(self.index)?;
                e.bytes(b",\"offset\":")?;
                e.unsigned(self.offset)?;
                e.bytes(b",\"bytes\":\"")?;
                e.bytes(&encoded[..count])?;
                e.bytes(b"\"")
            },
            |_| Ok(()),
        )?;
        self.index = self.index.checked_add(1).ok_or(Error::Limit)?;
        self.offset = self.offset.checked_add(bytes.len() as u64).ok_or(Error::Limit)?;
        Ok(())
    }

    pub(crate) fn collect(
        &mut self,
        emitter: &mut Emitter,
        owner: &mut Writer,
        decode: impl FnOnce(&mut dyn FnMut(&[u8]) -> Result<()>) -> Result<()>,
    ) -> Result<()> {
        let mut chunk = [0; 36_864];
        let mut len = 0;
        let mut ceiling = emitter.data_capacity(self.scope, self.index, self.offset)?;
        decode(&mut |bytes| {
            for &byte in bytes {
                chunk[len] = byte;
                len += 1;
                if len == ceiling {
                    self.append(&chunk[..len], emitter, owner)?;
                    len = 0;
                    ceiling = emitter.data_capacity(self.scope, self.index, self.offset)?;
                }
            }
            Ok(())
        })?;
        if len != 0 {
            self.append(&chunk[..len], emitter, owner)?;
        }
        Ok(())
    }
    pub(crate) fn end(&mut self, emitter: &mut Emitter, owner: &mut Writer) -> Result<()> {
        if self.ended {
            return Err(Error::Closed);
        }
        emitter.emit(
            owner,
            Kind::MediaEnd,
            self.scope,
            |e| {
                e.bytes(b",\"bytes\":")?;
                e.unsigned(self.offset)?;
                e.bytes(b",\"status\":\"completed\"")
            },
            |_| Ok(()),
        )?;
        self.ended = true;
        Ok(())
    }
}

pub(crate) enum DataUriPrefix {
    Incomplete,
    Plain,
    Encoded {
        encoding: Encoding,
        mime: ck_stream::ScratchLease,
    },
}

pub(crate) fn data_uri_prefix(
    owner: &mut Writer,
    source: &super::source::Source,
    span: super::source::Span,
) -> Result<DataUriPrefix> {
    let mut chars = Decoded::fragment(source, span)?;
    for expected in "data:".chars() {
        let Some(c) = chars.next_scalar()? else {
            return Ok(DataUriPrefix::Incomplete);
        };
        if !c.eq_ignore_ascii_case(&expected) {
            return Ok(DataUriPrefix::Plain);
        }
    }
    let mut count = 0u64;
    let mut suffix = ['\0'; 7];
    loop {
        let Some(c) = chars.next_scalar()? else {
            return Ok(DataUriPrefix::Incomplete);
        };
        if c == ',' {
            break;
        }
        if !c.is_ascii() || c.is_ascii_control() {
            return Err(Error::Schema);
        }
        suffix.rotate_left(1);
        suffix[6] = c;
        count = count.checked_add(1).ok_or(Error::Limit)?;
    }
    if suffix != [';', 'b', 'a', 's', 'e', '6', '4'] {
        return Err(Error::Unsupported);
    }
    let mime_chars = count.checked_sub(7).ok_or(Error::Schema)?;
    let mime = super::encode::scratch(owner, |e| {
        let mut chars = Decoded::fragment(source, span)?;
        for _ in 0..5 {
            chars.next_scalar()?;
        }
        e.bytes(b"\"")?;
        for _ in 0..mime_chars {
            e.escaped(chars.next_scalar()?.ok_or(Error::Schema)?.encode_utf8(&mut [0; 4]))?;
        }
        e.bytes(b"\"")
    })?;
    Ok(DataUriPrefix::Encoded {
        encoding: Encoding::DataUri { skip: count + 6 },
        mime,
    })
}
