use super::super::{FLUSH_DELAY, HELD_BYTES};
use super::*;
use crate::codec::{
    document::pending::Pending,
    media::{Encoding, InputDecoder, Publisher},
    source::Span,
};

pub(super) struct Buffered {
    input: Pending,
    active: Option<usize>,
}

pub(super) struct Published {
    wire: usize,
    pub(super) value: usize,
    pub(super) scope: Scope,
    field: &'static str,
    scalar: bool,
    start: u64,
    offset: u64,
    decoder: InputDecoder,
    publisher: Publisher,
    complete: bool,
    ended: bool,
    /// When collection of the unpublished input after `offset` began.
    held: Option<std::time::Instant>,
}

impl Buffered {
    pub(super) fn new(owner: &mut Writer, limit: u64) -> Result<Self> {
        Ok(Self {
            input: Pending::new(owner, limit)?,
            active: None,
        })
    }
    pub(super) fn advance(&mut self, owner: &mut Writer, state: &mut State, bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() {
            return self.flush(owner, state);
        }
        for bytes in bytes.chunks(8192) {
            self.scan(owner, state, |input, owner| input.advance(owner, bytes))?;
            if let Some(index) = self.active {
                self.collect(owner, state, index, false)?;
            }
            if self.active.is_none()
                && let Some(wire) = self.input.string
            {
                self.begin(owner, state, wire)?;
                if let Some(index) = self.active {
                    self.collect(owner, state, index, false)?;
                }
            }
        }
        Ok(())
    }
    /// Publishes held media input regardless of its size, as the flush contract requires.
    pub(super) fn flush(&mut self, owner: &mut Writer, state: &mut State) -> Result<()> {
        match self.active {
            Some(index) => self.collect(owner, state, index, true),
            None => Ok(()),
        }
    }
    /// Runs an input step that can poison the scanner. When it fails, held input the scanner
    /// validated before the step is published, without closing its media, before the error
    /// returns; a failed scanner no longer reports positions.
    fn scan(
        &mut self,
        owner: &mut Writer,
        state: &mut State,
        step: impl FnOnce(&mut Pending, &mut Writer) -> Result<()>,
    ) -> Result<()> {
        let held = match self.active {
            Some(index) => {
                let wire = state.published[index].wire;
                Some((
                    index,
                    self.input.scanner.pending_node(self.input.document.input(), wire)?,
                ))
            }
            None => None,
        };
        let result = step(&mut self.input, owner);
        if result.is_err()
            && let Some((index, wire)) = held
        {
            self.publish(owner, state, index, wire)?;
        }
        result
    }
    pub(super) fn flush_deadline(&self, state: &State) -> Option<std::time::Instant> {
        Some(state.published[self.active?].held? + FLUSH_DELAY)
    }
    pub(super) fn finish(mut self, owner: &mut Writer, state: &mut State) -> Result<()> {
        self.scan(owner, state, |input, _| input.finish())?;
        if let Some(index) = self.active {
            self.collect(owner, state, index, true)?;
        }
        state.document(owner, &mut self.input.document, true, None)?;
        state.done(owner, None)
    }

    fn begin(&mut self, owner: &mut Writer, state: &mut State, wire: usize) -> Result<()> {
        let scanned = self.input.scanner.pending_node(self.input.document.input(), wire)?;
        let data = self.input.node(wire)?;
        let doc = &self.input.document;
        let Some((value, field, scalar, index)) = media_target(doc, data, &scanned)? else {
            return Ok(());
        };
        let file = !scalar && field == "content" && super::media_events::file_part(doc, value)?;
        let Some((encoding, mime)) = payload_encoding(owner, doc, &scanned, (field, scalar, file))? else {
            return Ok(());
        };
        let mime = mime
            .map(|lease| self.input.document.import_json(owner, lease))
            .transpose()?;
        let doc = &self.input.document;
        state.ensure_start(owner, Some(doc))?;
        let position = state.choice(owner, index)?;
        let mut scope = state.scope(position);
        state.open_media_block(owner, position, &mut scope, field, scalar)?;
        scope.media_id = Some(media::local_id(b'm', state.serial)?);
        state.serial = state.serial.checked_add(1).ok_or(Error::Limit)?;
        let publisher = Publisher::start(&mut state.emitter, owner, scope, |e| {
            e.bytes(b",\"kind\":")?;
            e.string(if scalar {
                "text"
            } else if field == "audio" {
                "audio"
            } else if file {
                "document"
            } else {
                "image"
            })?;
            e.bytes(b",\"encoding\":\"base64\"")?;
            if let Some(mime) = mime {
                e.bytes(b",\"mime\":")?;
                doc.write(mime, e)?;
            }
            if scalar {
                e.bytes(b",\"mime\":\"text/plain;charset=utf-8\"")?;
            }
            e.bytes(b",\"attributes\":{\"field\":")?;
            e.string(field)?;
            if scalar {
                e.bytes(b",\"scalar\":true,\"value\":null}")
            } else if field == "audio" {
                e.bytes(b",\"value\":{\"data\":null}}")
            } else if file {
                e.bytes(b",\"value\":{\"file\":{\"file_data\":null}}}")
            } else {
                e.bytes(b",\"value\":{\"image_url\":{\"url\":null}}}")
            }
        })?;
        self.input.document.replace(data, Value::Null)?;
        self.active = Some(state.published.push(Published {
            wire,
            value,
            scope,
            field,
            scalar,
            start: scanned.span.start + 1,
            offset: scanned.span.start + 1,
            decoder: InputDecoder::new(encoding),
            publisher,
            complete: false,
            ended: false,
            held: None,
        })?);
        Ok(())
    }

    /// Decodes and publishes the payload input after the published offset. Input stays held, as
    /// positions in the input source, until it fills a record, the string ends, its collection
    /// delay passes or `force` asks for a flush; small network reads then share one record.
    fn collect(&mut self, owner: &mut Writer, state: &mut State, index: usize, force: bool) -> Result<()> {
        let published = &mut state.published[index];
        let wire = self
            .input
            .scanner
            .pending_node(self.input.document.input(), published.wire)?;
        // An incomplete scalar or escape after the validated end decodes with the next step.
        let received = self.input.document.input().len();
        if wire.validated_end > published.offset {
            let since = *published.held.get_or_insert_with(std::time::Instant::now);
            if wire.span.end == 0 && !force && received - published.offset < HELD_BYTES && since.elapsed() < FLUSH_DELAY
            {
                return Ok(());
            }
        }
        self.publish(owner, state, index, wire)
    }

    /// Decodes and publishes the payload input up to the validated end of `wire`, and completes
    /// the media once the string has ended.
    fn publish(&mut self, owner: &mut Writer, state: &mut State, index: usize, wire: json::Node) -> Result<()> {
        let published = &mut state.published[index];
        published.held = None;
        let mut chars = json::Decoded::fragment(
            self.input.document.input(),
            Span {
                start: published.offset,
                end: wire.validated_end,
            },
        )?;
        let mut chunk = [0; 36_864];
        let mut used = 0;
        while let Some(c) = chars.next_scalar()? {
            published.decoder.push(c, |bytes| {
                if used + bytes.len() > chunk.len() {
                    return Err(Error::Limit);
                }
                chunk[used..used + bytes.len()].copy_from_slice(bytes);
                used += bytes.len();
                Ok(())
            })?;
        }
        if used != 0 {
            published.publisher.append(&chunk[..used], &mut state.emitter, owner)?;
        }
        published.offset = wire.validated_end;
        self.input.document.release_input_range(Span {
            start: published.start,
            end: published.offset,
        })?;
        if wire.span.end != 0 {
            if !published.decoder.complete() {
                return Err(Error::Schema);
            }
            published.complete = true;
            self.active = None;
        }
        Ok(())
    }
}

/// Selects how a pending payload string decodes, with its data URI media type. None waits for
/// more of a data URI prefix, or leaves an image URL that is not a data URI unpublished.
fn payload_encoding(
    owner: &mut Writer,
    doc: &Document,
    scanned: &json::Node,
    (field, scalar, file): (&'static str, bool, bool),
) -> Result<Option<(Encoding, Option<ck_stream::ScratchLease>)>> {
    if scalar {
        return Ok(Some((Encoding::Utf8, None)));
    }
    if field == "audio" {
        return Ok(Some((Encoding::Base64, None)));
    }
    let span = Span {
        start: scanned.span.start + 1,
        end: scanned.validated_end,
    };
    Ok(match media::data_uri_prefix(owner, doc.input(), span)? {
        media::DataUriPrefix::Incomplete => None,
        // Plain base64 is valid file data; an image URL must be a data URI.
        media::DataUriPrefix::Plain if file => Some((Encoding::Base64, None)),
        media::DataUriPrefix::Plain => None,
        media::DataUriPrefix::Encoded { encoding, mime } => Some((encoding, Some(mime))),
    })
}

/// Finds the choice member that the pending string `data` publishes as media: a message text
/// field over 1024 bytes, audio data, an image URL or file data. Returns the published value, its field,
/// whether it is a scalar text field, and the choice index.
fn media_target(doc: &Document, data: usize, scanned: &json::Node) -> Result<Option<(usize, &'static str, bool, u64)>> {
    let Some(parent) = doc.node(data)?.parent else {
        return Ok(None);
    };
    let name = doc.node(data)?.name.ok_or(Error::Schema)?;
    let mut target = None;
    if let Some(parent_name) = doc.node(parent)?.name {
        if doc.name_is(parent_name, "message")? {
            for field in ["content", "refusal", "reasoning_content"] {
                if doc.name_is(name, field)? && scanned.validated_end - scanned.span.start > 1024 {
                    target = Some((data, parent, field, true));
                }
            }
        } else if doc.name_is(parent_name, "audio")? && doc.name_is(name, "data")? {
            if let Some(message) = doc.node(parent)?.parent {
                target = Some((parent, message, "audio", false));
            }
        } else if doc.name_is(parent_name, "image_url")?
            && doc.name_is(name, "url")?
            && let Some(part) = doc.node(parent)?.parent
            && let Some(array) = doc.node(part)?.parent
            && matches!(doc.node(array)?.value, Value::Array)
            && let Some(array_name) = doc.node(array)?.name
            && let Some(message) = doc.node(array)?.parent
        {
            if doc.name_is(array_name, "images")? {
                target = Some((part, message, "images", false));
            } else if doc.name_is(array_name, "content")?
                && let Some(kind) = doc.member(part, "type")?
                && doc.string_is(kind, "image_url")?
            {
                target = Some((part, message, "content", false));
            }
        } else if doc.name_is(parent_name, "file")?
            && doc.name_is(name, "file_data")?
            && let Some(part) = doc.node(parent)?.parent
            && let Some(array) = doc.node(part)?.parent
            && matches!(doc.node(array)?.value, Value::Array)
            && let Some(array_name) = doc.node(array)?.name
            && doc.name_is(array_name, "content")?
            && let Some(kind) = doc.member(part, "type")?
            && doc.string_is(kind, "file")?
            && let Some(message) = doc.node(array)?.parent
        {
            target = Some((part, message, "content", false));
        }
    }
    let Some((value, message, field, scalar)) = target else {
        return Ok(None);
    };
    let Some(message_name) = doc.node(message)?.name else {
        return Ok(None);
    };
    if !doc.name_is(message_name, "message")? {
        return Ok(None);
    }
    let Some(choice) = doc.node(message)?.parent else {
        return Ok(None);
    };
    let Some(choices) = doc.node(choice)?.parent else {
        return Ok(None);
    };
    if doc.node(choices)?.parent != Some(doc.root()) || !matches!(doc.node(choices)?.value, Value::Array) {
        return Ok(None);
    }
    if !doc.name_is(doc.node(choices)?.name.ok_or(Error::Schema)?, "choices")? {
        return Ok(None);
    }
    let Some(index) = doc.member(choice, "index")? else {
        return Ok(None);
    };
    let index = doc.unsigned(index)?;
    Ok(Some((value, field, scalar, index)))
}

impl State {
    /// Opens the block that a published text field or image part streams into; audio has none.
    fn open_media_block(
        &mut self,
        owner: &mut Writer,
        position: usize,
        scope: &mut Scope,
        field: &'static str,
        scalar: bool,
    ) -> Result<()> {
        if scalar {
            let block = match field {
                "content" => 0,
                "reasoning_content" => 1,
                _ => 2,
            };
            scope.block_index = Some(block as u64);
            self.emitter.emit(
                owner,
                Kind::BlockStart,
                *scope,
                |e| {
                    e.bytes(b",\"kind\":")?;
                    e.string(["text", "thinking", "refusal"][block])
                },
                |_| Ok(()),
            )?;
            self.choices[position].blocks[block] = true;
        } else if field != "audio" {
            scope.block_index = Some(self.choices[position].next_block);
            self.choices[position].next_block += 1;
            self.emitter.emit(
                owner,
                Kind::BlockStart,
                *scope,
                |e| e.bytes(b",\"kind\":\"content\""),
                |_| Ok(()),
            )?;
        }
        Ok(())
    }
    pub(super) fn published_scope(&self, value: usize) -> Option<Scope> {
        self.published
            .iter()
            .find(|entry| entry.value == value && !entry.ended)
            .map(|entry| entry.scope)
    }
    pub(super) fn finish_published(&mut self, owner: &mut Writer, doc: &Document, value: usize) -> Result<bool> {
        let Some(index) = self
            .published
            .iter()
            .position(|entry| entry.value == value && !entry.ended)
        else {
            return Ok(false);
        };
        let entry = &mut self.published[index];
        if !entry.complete {
            return Err(Error::Truncated);
        }
        if !entry.scalar {
            let transcript = if entry.field == "audio" {
                doc.member(value, "transcript")?
                    .filter(|node| !matches!(doc.node(*node).map(|n| n.value), Ok(Value::Null)))
            } else {
                None
            };
            if let Some(text) = transcript {
                super::super::request::require_string(doc, text)?;
            }
            self.emitter.emit(
                owner,
                Kind::Opaque,
                entry.scope,
                |e| {
                    e.bytes(b",\"name\":\"chat.media.attributes\",\"format\":\"json\",\"value\":")?;
                    if transcript.is_some() {
                        e.bytes(b"{\"data\":null")?;
                        members(doc, value, e, &["data", "transcript"])?;
                        e.bytes(b"}")
                    } else {
                        doc.write(value, e)
                    }
                },
                |_| Ok(()),
            )?;
            if let Some(text) = transcript {
                self.emitter.emit(
                    owner,
                    Kind::MediaTranscript,
                    entry.scope,
                    |e| {
                        e.bytes(b",\"text\":")?;
                        doc.write(text, e)?;
                        e.bytes(b",\"final\":true")
                    },
                    |_| Ok(()),
                )?;
            }
        }
        entry.publisher.end(&mut self.emitter, owner)?;
        entry.ended = true;
        Ok(true)
    }
}
