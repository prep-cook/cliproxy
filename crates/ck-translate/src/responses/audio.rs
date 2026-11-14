use super::{is, response::append_fragment};
use crate::codec::{
    Error, Result,
    document::{Document, Value},
    event::{Emitter, Kind, LocalId, Scope},
    media::{self, Base64Fragments, Publisher},
};
use ck_stream::Writer;
use sha2::{Digest, Sha256};

pub(super) struct Ingress {
    scope: Scope,
    publisher: Publisher,
    decoder: Base64Fragments,
    hash: Sha256,
    transcript: Document,
    audio_seen: bool,
    audio_done: bool,
    transcript_seen: bool,
    transcript_done: bool,
    closed: bool,
    /// Started from the snapshot's `audio` object, whose metadata `media.start` already carries.
    native: bool,
}

impl Ingress {
    pub fn start(
        owner: &mut Writer,
        emitter: &mut Emitter,
        id: LocalId,
        native: Option<(&Document, usize)>,
    ) -> Result<Self> {
        let scope = Scope {
            choice_index: Some(0),
            media_id: Some(id),
            ..Scope::default()
        };
        let publisher = Publisher::start(emitter, owner, scope, |e| {
            e.bytes(b",\"kind\":\"audio\",\"encoding\":\"base64\"")?;
            if let Some((doc, node)) = native {
                if let Some(mime) = doc.member(node, "mime_type")? {
                    e.bytes(b",\"mime\":")?;
                    doc.write(mime, e)?;
                }
                e.bytes(b",\"attributes\":{\"protocol\":\"responses\",\"field\":\"audio\",\"native\":")?;
                super::object(doc, node, &["data", "transcript"], e)?;
                e.bytes(b",\"data_present\":")?;
                e.bytes(if doc.member(node, "data")?.is_some() {
                    b"true"
                } else {
                    b"false"
                })?;
                e.bytes(b"}")
            } else {
                e.bytes(b",\"attributes\":{\"protocol\":\"responses\",\"field\":\"stream\"}")
            }
        })?;
        Ok(Self {
            scope,
            publisher,
            decoder: Base64Fragments::default(),
            hash: Sha256::new(),
            transcript: super::mapped(owner, |e| e.bytes(b"{}"))?,
            audio_seen: false,
            audio_done: false,
            transcript_seen: false,
            transcript_done: false,
            closed: false,
            native: native.is_some(),
        })
    }

    pub fn data(&mut self, owner: &mut Writer, emitter: &mut Emitter, doc: &Document, node: usize) -> Result<()> {
        if self.closed || self.audio_done {
            return Err(Error::Schema);
        }
        let Value::String(text) = doc.node(node)?.value else {
            return Err(Error::Schema);
        };
        self.audio_seen = true;
        self.publisher.collect(emitter, owner, |emit| {
            self.decoder.push(doc, text, |bytes| {
                self.hash.update(bytes);
                emit(bytes)
            })
        })
    }

    pub fn audio_done(&mut self) -> Result<()> {
        if self.closed || self.audio_done || !self.decoder.complete() {
            return Err(Error::Schema);
        }
        self.audio_seen = true;
        self.audio_done = true;
        Ok(())
    }

    pub fn transcript(
        &mut self,
        owner: &mut Writer,
        emitter: &mut Emitter,
        doc: &Document,
        node: usize,
        snapshot: bool,
    ) -> Result<()> {
        if self.closed {
            return Err(Error::Schema);
        }
        let old = self.transcript.member(self.transcript.root(), "text")?;
        let fragment = if snapshot {
            super::response::suffix(owner, &self.transcript, old, doc, node)?
        } else {
            super::mapped(owner, |e| doc.write(node, e))?
        };
        let empty = fragment.chars(fragment.root())?.next_scalar()?.is_none();
        if self.transcript_done && !empty {
            return Err(Error::Schema);
        }
        if !self.transcript_seen || !empty {
            emitter.emit(
                owner,
                Kind::MediaTranscript,
                self.scope,
                |e| {
                    e.bytes(b",\"text\":")?;
                    fragment.write(fragment.root(), e)?;
                    e.bytes(b",\"final\":false")?;
                    let parent = doc.node(node)?.parent.ok_or(Error::Schema)?;
                    super::member(doc, parent, "timing", e)
                },
                |_| Ok(()),
            )?;
            let imported = self.transcript.absorb(fragment)?;
            let root = self.transcript.root();
            append_fragment(&mut self.transcript, root, "text", imported)?;
        }
        self.transcript_seen = true;
        Ok(())
    }

    pub fn transcript_done(&mut self, owner: &mut Writer, emitter: &mut Emitter) -> Result<()> {
        if self.closed || self.transcript_done {
            return Err(Error::Schema);
        }
        emitter.emit(
            owner,
            Kind::MediaTranscript,
            self.scope,
            |e| e.bytes(b",\"text\":\"\",\"final\":true"),
            |_| Ok(()),
        )?;
        self.transcript_seen = true;
        self.transcript_done = true;
        Ok(())
    }

    pub fn snapshot(&mut self, owner: &mut Writer, emitter: &mut Emitter, doc: &Document, node: usize) -> Result<()> {
        if let Some(data) = doc.member(node, "data")? {
            if self.audio_seen {
                if !self.decoder.complete()
                    || super::image::digest(doc, data)? != <[u8; 32]>::from(self.hash.clone().finalize())
                {
                    return Err(Error::Schema);
                }
            } else {
                self.data(owner, emitter, doc, data)?;
                self.audio_done()?;
            }
        }
        if let Some(text) = doc.member(node, "transcript")? {
            self.transcript(owner, emitter, doc, text, true)?;
            if !self.transcript_done {
                self.transcript_done(owner, emitter)?;
            }
        }
        self.late_metadata(owner, emitter, doc, node)
    }

    /// Publishes the metadata of a snapshot `audio` object that arrives after streamed audio
    /// started the media, as a `responses.audio` record egress merges into the audio.
    fn late_metadata(&mut self, owner: &mut Writer, emitter: &mut Emitter, doc: &Document, node: usize) -> Result<()> {
        let mut late = false;
        for field in doc.children(node)? {
            let name = doc.node(field)?.name.ok_or(Error::Schema)?;
            late |= !doc.name_is(name, "data")? && !doc.name_is(name, "transcript")?;
        }
        if self.native || !late {
            return Ok(());
        }
        emitter.emit(
            owner,
            Kind::Opaque,
            self.scope,
            |e| {
                e.bytes(b",\"name\":\"responses.audio\",\"format\":\"json\",\"value\":")?;
                super::object(doc, node, &["data", "transcript"], e)
            },
            |_| Ok(()),
        )?;
        Ok(())
    }

    pub fn finish(&mut self, owner: &mut Writer, emitter: &mut Emitter) -> Result<()> {
        if self.closed || (self.audio_seen && !self.audio_done) || (self.transcript_seen && !self.transcript_done) {
            return Err(Error::Truncated);
        }
        self.publisher.end(emitter, owner)?;
        self.closed = true;
        Ok(())
    }
}

pub(super) struct Projection {
    pub id: usize,
    pub audio: usize,
    data: Option<usize>,
    next: u64,
    bytes: u64,
    pub closed: bool,
    pub transcript_done: bool,
}
impl Projection {
    pub fn start(doc: &mut Document, event: usize) -> Result<Self> {
        let x = doc.required(event, "x_ck")?;
        let id = doc.required(x, "media_id")?;
        let audio = doc.add(None, Value::Object)?;
        let attributes = doc.member(event, "attributes")?;
        let native = match attributes {
            Some(a) => is(doc, a, "protocol", "responses")? && is(doc, a, "field", "audio")?,
            None => false,
        };
        let data_present = if native {
            let attrs = attributes.ok_or(Error::Schema)?;
            matches!(doc.node(doc.required(attrs, "data_present")?)?.value, Value::Bool(true))
        } else {
            true
        };
        if native {
            let attrs = doc.required(attributes.ok_or(Error::Schema)?, "native")?;
            super::merge(doc, audio, attrs)?;
            doc.rename(audio, "audio")?;
            doc.append(doc.root(), audio)?;
        } else {
            let carrier = match doc.member(doc.root(), "x_ck_opaque")? {
                Some(n) => n,
                None => doc.set(doc.root(), "x_ck_opaque", Value::Array)?,
            };
            doc.set(audio, "type", Value::Text("response.audio"))?;
            doc.append(carrier, audio)?;
            if let Some(mime) = doc.member(event, "mime")? {
                let value = doc.node(mime)?.value;
                doc.set(audio, "mime_type", value)?;
            }
            if let Some(attrs) = attributes {
                doc.detach(event, attrs)?;
                doc.rename(attrs, "attributes")?;
                doc.append(audio, attrs)?;
            }
        }
        let data = if data_present {
            Some(doc.set(audio, "data", Value::Base64Fragments)?)
        } else {
            None
        };
        Ok(Self {
            id,
            audio,
            data,
            next: 0,
            bytes: 0,
            closed: false,
            transcript_done: false,
        })
    }
    pub fn append(&mut self, doc: &mut Document, event: usize) -> Result<()> {
        if self.closed
            || doc.unsigned(doc.required(event, "index")?)? != self.next
            || doc.unsigned(doc.required(event, "offset")?)? != self.bytes
        {
            return Err(Error::Schema);
        }
        let value = doc.required(event, "bytes")?;
        let Value::String(text) = doc.node(value)?.value else {
            return Err(Error::Schema);
        };
        let mut bytes = 0;
        media::decode_base64(doc, text, |part| {
            bytes += part.len() as u64;
            Ok(())
        })?;
        doc.detach(event, value)?;
        let data = match self.data {
            Some(data) => data,
            None => {
                let data = doc.set(self.audio, "data", Value::Base64Fragments)?;
                self.data = Some(data);
                data
            }
        };
        doc.append(data, value)?;
        self.next = self.next.checked_add(1).ok_or(Error::Limit)?;
        self.bytes = self.bytes.checked_add(bytes).ok_or(Error::Limit)?;
        Ok(())
    }
    pub fn transcript(&mut self, doc: &mut Document, event: usize) -> Result<()> {
        if self.closed || self.transcript_done {
            return Err(Error::Schema);
        }
        let text = doc.required(event, "text")?;
        doc.detach(event, text)?;
        append_fragment(doc, self.audio, "transcript", text)?;
        self.transcript_done = matches!(doc.node(doc.required(event, "final")?)?.value, Value::Bool(true));
        if let Some(timing) = doc.remove(event, "timing")? {
            let target = doc.set(self.audio, "timing", Value::Null)?;
            doc.transplant(target, timing)?;
        }
        Ok(())
    }
    pub fn end(&mut self, doc: &Document, event: usize) -> Result<()> {
        if self.closed
            || !is(doc, event, "status", "completed")?
            || doc.unsigned(doc.required(event, "bytes")?)? != self.bytes
        {
            return Err(Error::Schema);
        }
        self.closed = true;
        Ok(())
    }
}
