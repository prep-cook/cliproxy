use super::super::{FLUSH_DELAY, HELD_BYTES};
use super::*;
use crate::codec::{
    document::Text,
    media::{Encoding, Media},
    source::Span,
};
use std::time::Instant;

pub(super) struct Audio {
    publisher: media::Publisher,
    metadata: Document,
    scope: Scope,
    transcript: bool,
    /// Decoded bytes awaiting publication. Each SSE record's source is released after the
    /// record, so the bytes are copied into these scratch frames rather than held as positions.
    held: Source,
    /// Offset in `held` of the first unpublished byte.
    published: u64,
    /// End in `held` of the bytes from deltas that decoded completely; a terminal error
    /// publishes only these.
    complete: u64,
    /// When collection of the held bytes began.
    since: Option<Instant>,
}

impl Audio {
    fn new(owner: &mut Writer, publisher: media::Publisher, metadata: Document, scope: Scope) -> Result<Self> {
        Ok(Self {
            publisher,
            metadata,
            scope,
            transcript: false,
            held: Source::new(owner)?,
            published: 0,
            complete: 0,
            since: None,
        })
    }

    /// Holds a delta's decoded bytes and publishes the held bytes once they fill a data record.
    /// The bytes count only after the whole delta decodes, so a rejected delta adds none; the
    /// hold can therefore exceed the threshold by one delta's bytes.
    fn push(
        &mut self,
        emitter: &mut Emitter,
        owner: &mut Writer,
        fragments: &mut media::Base64Fragments,
        input: &Document,
        data: Text,
    ) -> Result<()> {
        let held = &mut self.held;
        fragments.push(input, data, |bytes| held.push_bytes(owner, bytes).map(drop))?;
        if self.held.len() > self.complete {
            self.since.get_or_insert_with(Instant::now);
        }
        self.complete = self.held.len();
        if self.complete - self.published >= HELD_BYTES {
            self.publish(emitter, owner)?;
        }
        Ok(())
    }

    /// Publishes the held bytes of completely decoded deltas.
    fn publish(&mut self, emitter: &mut Emitter, owner: &mut Writer) -> Result<()> {
        let span = Span {
            start: self.published,
            end: self.complete,
        };
        if !span.is_empty() {
            let held = &self.held;
            self.publisher
                .collect(emitter, owner, |emit| held.visit(span, |bytes| emit(bytes)))?;
            self.held.release_before(span.end)?;
            self.published = span.end;
        }
        self.since = None;
        Ok(())
    }

    pub(super) fn finish(&mut self, emitter: &mut Emitter, owner: &mut Writer) -> Result<()> {
        self.publish(emitter, owner)?;
        if self.transcript {
            emitter.emit(
                owner,
                Kind::MediaTranscript,
                self.scope,
                |e| e.bytes(b",\"text\":\"\",\"final\":true"),
                |_| Ok(()),
            )?;
        }
        self.publisher.end(emitter, owner)
    }
}

/// Whether a media part is a document: its type names a file. Another carrier member, such as
/// an unknown `file` beside an image URL, does not change the kind.
pub(super) fn file_part(doc: &Document, value: usize) -> Result<bool> {
    match doc.member(value, "type")? {
        Some(kind) => doc.string_is(kind, "file"),
        None => Ok(false),
    }
}

impl State {
    pub(super) fn media_value(
        &mut self,
        owner: &mut Writer,
        input: &Document,
        value: usize,
        scope: Scope,
        field: &'static str,
    ) -> Result<bool> {
        if self.finish_published(owner, input, value)? {
            return Ok(true);
        }
        if field == "audio" {
            self.audio_fragment(owner, input, value, scope)?;
            return Ok(true);
        }
        let (carrier, data_key, kind) = if file_part(input, value)? {
            ("file", "file_data", "document")
        } else {
            ("image_url", "url", "image")
        };
        let Some(native) = input.member(value, carrier)? else {
            return Ok(false);
        };
        let Some(native_data) = input.member(native, data_key)? else {
            return Ok(false);
        };
        let Value::String(text) = input.node(native_data)?.value else {
            return Err(Error::Schema);
        };
        let (encoding, mime_lease) = match super::super::request_content::data_uri_metadata(input, owner, native_data)?
        {
            Some((encoding, mime)) => (encoding, Some(mime)),
            None if kind == "document" => (Encoding::Base64, None),
            None => return Ok(false),
        };
        let mut attributes = mapped_document(owner, |e| {
            e.bytes(b"{\"field\":")?;
            e.string(field)?;
            e.bytes(b",\"value\":{")?;
            e.string(carrier)?;
            e.bytes(b":{")?;
            e.string(data_key)?;
            e.bytes(b":null")?;
            members(input, native, e, &[data_key])?;
            e.bytes(b"}")?;
            members(input, value, e, &[carrier])?;
            e.bytes(b"}}")
        })?;
        let mime = mime_lease.map(|mime| attributes.import_json(owner, mime)).transpose()?;
        let media = Media {
            node: native_data,
            id: media::local_id(b'm', self.serial)?,
            data: Some(text),
            encoding,
            kind,
            mime: None,
        };
        self.serial = self.serial.checked_add(1).ok_or(Error::Limit)?;
        media::publish_with_attributes(
            input,
            media,
            &mut self.emitter,
            owner,
            (scope, None),
            Some(media::Attributes {
                document: &attributes,
                value: attributes.root(),
                mime,
            }),
            None,
        )?;
        Ok(true)
    }

    pub(super) fn array_field(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        field: usize,
        position: usize,
        name: &'static str,
    ) -> Result<()> {
        if !matches!(doc.node(field)?.value, Value::Array) {
            return Err(Error::Schema);
        }
        if doc.node(field)?.first.is_none() {
            return self.empty_array(owner, position, name);
        }
        for part in doc.children(field)? {
            let scope = if let Some(scope) = self.published_scope(part) {
                scope
            } else {
                let mut scope = self.scope(position);
                scope.block_index = Some(self.choices[position].next_block);
                self.choices[position].next_block += 1;
                self.emitter.emit(
                    owner,
                    Kind::BlockStart,
                    scope,
                    |e| e.bytes(b",\"kind\":\"content\""),
                    |_| Ok(()),
                )?;
                scope
            };
            if !self.media_value(owner, doc, part, scope, name)? {
                self.emitter.emit(
                    owner,
                    Kind::Chunk,
                    scope,
                    |e| {
                        base(self.base.as_ref().ok_or(Error::Schema)?, e)?;
                        e.bytes(b",\"choices\":[{\"index\":")?;
                        e.unsigned(scope.choice_index.ok_or(Error::Schema)?)?;
                        e.bytes(b",\"delta\":{")?;
                        e.string(name)?;
                        e.bytes(b":[")?;
                        doc.write(part, e)?;
                        e.bytes(b"]},\"finish_reason\":null}]")
                    },
                    |_| Ok(()),
                )?;
            }
            self.emitter
                .emit(owner, Kind::BlockEnd, scope, |_| Ok(()), |_| Ok(()))?;
        }
        Ok(())
    }

    pub(super) fn annotations(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        field: usize,
        position: usize,
    ) -> Result<()> {
        if !matches!(doc.node(field)?.value, Value::Array) {
            return Err(Error::Schema);
        }
        if doc.node(field)?.first.is_none() {
            return self.empty_array(owner, position, "annotations");
        }
        for annotation in doc.children(field)? {
            self.emitter.emit(
                owner,
                Kind::Annotation,
                self.scope(position),
                |e| {
                    e.bytes(b",\"kind\":\"chat\",\"value\":")?;
                    doc.write(annotation, e)
                },
                |_| Ok(()),
            )?;
        }
        Ok(())
    }
}

impl State {
    pub(super) fn empty_array(&mut self, owner: &mut Writer, position: usize, name: &'static str) -> Result<()> {
        let scope = self.scope(position);
        self.emitter.emit(
            owner,
            Kind::Chunk,
            scope,
            |e| {
                base(self.base.as_ref().ok_or(Error::Schema)?, e)?;
                e.bytes(b",\"choices\":[{\"index\":")?;
                e.unsigned(scope.choice_index.ok_or(Error::Schema)?)?;
                e.bytes(b",\"delta\":{")?;
                e.string(name)?;
                e.bytes(b":[]},\"finish_reason\":null}]")
            },
            |_| Ok(()),
        )?;
        Ok(())
    }
}

impl State {
    fn audio_fragment(&mut self, owner: &mut Writer, input: &Document, value: usize, mut scope: Scope) -> Result<()> {
        let position = self
            .choices
            .iter()
            .position(|choice| Some(choice.index) == scope.choice_index)
            .ok_or(Error::Schema)?;
        let data = input.member(value, "data")?;
        let transcript = input.member(value, "transcript")?;
        if let Some(text) = transcript
            && !matches!(input.node(text)?.value, Value::Null)
        {
            input.chars(text)?;
        }
        let data = data
            .map(|node| match input.node(node)?.value {
                Value::String(text) => Ok(text),
                _ => Err(Error::Schema),
            })
            .transpose()?;
        let metadata = self.audio_metadata(owner, input, value, position)?;
        if self.choices[position].audio_output.is_none() {
            scope.media_id = Some(media::local_id(b'm', self.serial)?);
            self.serial = self.serial.checked_add(1).ok_or(Error::Limit)?;
            let publisher = media::Publisher::start(&mut self.emitter, owner, scope, |e| {
                e.bytes(b",\"kind\":\"audio\",\"encoding\":\"base64\",\"attributes\":{\"field\":\"audio\",\"value\":{\"data\":null")?;
                members(&metadata, metadata.root(), e, &[])?;
                e.bytes(b"}}")
            })?;
            self.choices[position].audio_output = Some(Audio::new(owner, publisher, metadata, scope)?);
        } else {
            let audio = self.choices[position].audio_output.as_mut().ok_or(Error::Schema)?;
            audio.metadata = metadata;
            self.emitter.emit(
                owner,
                Kind::Chunk,
                audio.scope,
                |e| {
                    base(self.base.as_ref().ok_or(Error::Schema)?, e)?;
                    e.bytes(b",\"choices\":[{\"index\":")?;
                    e.unsigned(scope.choice_index.ok_or(Error::Schema)?)?;
                    e.bytes(b",\"delta\":{\"audio\":{")?;
                    let mut first = true;
                    for field in input.children(value)? {
                        let name = input.node(field)?.name.ok_or(Error::Schema)?;
                        if input.name_is(name, "data")? || input.name_is(name, "transcript")? {
                            continue;
                        }
                        if !first {
                            e.bytes(b",")?;
                        }
                        first = false;
                        input.write_name(name, e)?;
                        e.bytes(b":")?;
                        input.write(field, e)?;
                    }
                    e.bytes(b"}},\"finish_reason\":null}]")
                },
                |_| Ok(()),
            )?;
        }
        let choice = &mut self.choices[position];
        let audio = choice.audio_output.as_mut().ok_or(Error::Schema)?;
        if let Some(data) = data {
            audio.push(&mut self.emitter, owner, &mut choice.audio, input, data)?;
        }
        if let Some(text) = transcript {
            self.audio_transcript(owner, input, text, position, scope)?;
        }
        Ok(())
    }

    /// Checks that an audio delta keeps the identity members already published and returns its
    /// members other than data and transcript, merged over the earlier metadata.
    fn audio_metadata(&self, owner: &mut Writer, input: &Document, value: usize, position: usize) -> Result<Document> {
        if let Some(audio) = &self.choices[position].audio_output {
            for key in ["id", "format", "codec", "sample_rate", "channels"] {
                if let Some(previous) = audio.metadata.member(audio.metadata.root(), key)?
                    && let Some(incoming) = input.member(value, key)?
                    && !snapshot::exact(&audio.metadata, previous, input, incoming, 0)?
                {
                    return Err(Error::Schema);
                }
            }
        }
        mapped_document(owner, |e| {
            e.bytes(b"{")?;
            let mut first = true;
            for field in input.children(value)? {
                let name = input.node(field)?.name.ok_or(Error::Schema)?;
                if input.name_is(name, "data")? || input.name_is(name, "transcript")? {
                    continue;
                }
                if !first {
                    e.bytes(b",")?;
                }
                first = false;
                input.write_name(name, e)?;
                e.bytes(b":")?;
                input.write(field, e)?;
            }
            if let Some(audio) = &self.choices[position].audio_output {
                for field in audio.metadata.children(audio.metadata.root())? {
                    if snapshot::matching_member(input, value, &audio.metadata, field)?.is_some() {
                        continue;
                    }
                    if !first {
                        e.bytes(b",")?;
                    }
                    first = false;
                    audio
                        .metadata
                        .write_name(audio.metadata.node(field)?.name.ok_or(Error::Schema)?, e)?;
                    e.bytes(b":")?;
                    audio.metadata.write(field, e)?;
                }
            }
            e.bytes(b"}")
        })
    }

    /// Publishes a transcript fragment, or a null transcript as a native chunk.
    fn audio_transcript(
        &mut self,
        owner: &mut Writer,
        input: &Document,
        text: usize,
        position: usize,
        scope: Scope,
    ) -> Result<()> {
        self.publish_audio(owner, position)?;
        let audio = self.choices[position].audio_output.as_mut().ok_or(Error::Schema)?;
        if matches!(input.node(text)?.value, Value::Null) {
            self.emitter.emit(
                owner,
                Kind::Chunk,
                audio.scope,
                |e| {
                    base(self.base.as_ref().ok_or(Error::Schema)?, e)?;
                    e.bytes(b",\"choices\":[{\"index\":")?;
                    e.unsigned(scope.choice_index.ok_or(Error::Schema)?)?;
                    e.bytes(b",\"delta\":{\"audio\":{\"transcript\":null}},\"finish_reason\":null}]")
                },
                |_| Ok(()),
            )?;
        } else {
            audio.transcript = true;
            self.emitter.emit(
                owner,
                Kind::MediaTranscript,
                audio.scope,
                |e| {
                    e.bytes(b",\"text\":")?;
                    input.write(text, e)?;
                    e.bytes(b",\"final\":false")
                },
                |_| Ok(()),
            )?;
        }
        Ok(())
    }
}

impl State {
    /// Publishes a choice's held audio, so a record that follows the audio in the source
    /// follows it in the stream.
    pub(super) fn publish_audio(&mut self, owner: &mut Writer, position: usize) -> Result<()> {
        match &mut self.choices[position].audio_output {
            Some(audio) => audio.publish(&mut self.emitter, owner),
            None => Ok(()),
        }
    }

    /// Publishes every choice's held audio when `force`, otherwise the audio whose collection
    /// delay has passed.
    pub(super) fn flush_audio(&mut self, owner: &mut Writer, force: bool) -> Result<()> {
        for position in 0..self.choices.len() {
            let since = self.choices[position]
                .audio_output
                .as_ref()
                .and_then(|audio| audio.since);
            if since.is_some_and(|since| force || since.elapsed() >= FLUSH_DELAY) {
                self.publish_audio(owner, position)?;
            }
        }
        Ok(())
    }

    /// Deadline of the oldest held audio, under the `ResponseIngress::flush_deadline` contract.
    pub(super) fn audio_deadline(&self) -> Option<Instant> {
        self.choices
            .iter()
            .filter_map(|choice| choice.audio_output.as_ref()?.since)
            .min()
            .map(|since| since + FLUSH_DELAY)
    }
}
