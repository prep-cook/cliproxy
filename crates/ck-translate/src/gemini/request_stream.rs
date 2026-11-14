//! Incremental publication of unfinished request strings: inline media bytes and long text
//! reach the stream as data records while the request body still arrives.

use super::*;
use crate::codec::{document::Text, media::Encoding, source::Span};

/// Longest time a decoded fragment may wait in scratch before publication.
const FLUSH_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
/// A plain string longer than this is published as text media while it arrives.
const TEXT_THRESHOLD: u64 = 1024;

/// The open string being published, held by its input positions.
pub(super) struct StringState {
    wire: usize,
    media: usize,
    start: u64,
    offset: u64,
    decoder: media::InputDecoder,
    publisher: media::Publisher,
    buffer: Option<ck_stream::ScratchReservation>,
    buffered: usize,
    capacity: usize,
    first: bool,
    since: Option<std::time::Instant>,
}

impl Ingress {
    pub(super) fn consume(&mut self, owner: &mut Writer) -> Result<()> {
        let input = self.input.as_mut().ok_or(Error::Closed)?;
        if input.scanner.nodes().is_empty() {
            return Ok(());
        }
        if !self.started {
            self.start(owner)?;
        }
        if self.active.is_some() {
            self.collect(owner)?;
        }
        if self.active.is_none()
            && let Some(wire) = self.input.as_ref().ok_or(Error::Closed)?.string
        {
            self.begin(owner, wire)?;
            if self.active.is_some() {
                self.collect(owner)?;
            }
        }
        let input = self.input.as_mut().ok_or(Error::Closed)?;
        input.document.rebind_keys(&mut input.scanner)?;
        let floor = input
            .document
            .raw_floor(input.document.root(), input.scanner.raw_floor())?;
        input.document.release_input_before(floor)
    }

    /// Publishes `request.start` and the root object field once the first node is scanned.
    fn start(&mut self, owner: &mut Writer) -> Result<()> {
        let input = self.input.as_mut().ok_or(Error::Closed)?;
        self.emitter.request_start(
            owner,
            RequestView {
                document: &input.document,
                media: &self.media,
                operation: self.operation,
            },
        )?;
        codec::internal::next(owner, &mut self.reader)?;
        self.emitter.emit(
            owner,
            Kind::RequestField,
            Scope::default(),
            |e| e.bytes(b",\"pointer\":\"\",\"value\":{}"),
            |_| Ok(()),
        )?;
        let (_, event) = codec::internal::next(owner, &mut self.reader)?;
        let root = input.document.root();
        input.document.bind_field(root, event)?;
        input.document.mark_published(root);
        self.started = true;
        Ok(())
    }

    /// Registers the open string as media once it is inline media data with a known MIME
    /// type, or plain text past the threshold, and opens its media lifetime.
    fn begin(&mut self, owner: &mut Writer, wire: usize) -> Result<()> {
        let input = self.input.as_mut().ok_or(Error::Closed)?;
        let scanned = input.scanner.pending_node(input.document.input(), wire)?;
        let node = input.node(wire)?;
        let doc = &mut input.document;
        let native = match native_media(doc, node)? {
            // Inline data whose MIME type has not arrived waits in the input until
            // normalization publishes it whole with its modality.
            Carrier::Unresolved => return Ok(()),
            Carrier::Native(kind, mime) => Some((kind, mime)),
            Carrier::Other => None,
        };
        if native.is_none() && scanned.validated_end - scanned.span.start <= TEXT_THRESHOLD {
            return Ok(());
        }
        let (encoding, kind, mime) = match native {
            Some((kind, mime)) => (Encoding::Base64, kind, Some(mime)),
            None => (
                Encoding::Utf8,
                "text",
                Some(doc.add(None, Value::Text("text/plain;charset=utf-8"))?),
            ),
        };
        let index = self.media.len();
        let registered = self.media.register(
            doc,
            node,
            Text {
                source: 0,
                span: scanned.span,
            },
            encoding,
            kind,
            mime,
        )?;
        let publisher = media::Publisher::begin(doc, registered, &mut self.emitter, owner)?;
        self.media.retain_record(doc, index, owner, &mut self.reader)?;
        self.published_media += 1;
        self.active = Some(StringState {
            wire,
            media: index,
            start: scanned.span.start + 1,
            offset: scanned.span.start + 1,
            decoder: media::InputDecoder::new(encoding),
            publisher,
            buffer: None,
            buffered: 0,
            capacity: self.emitter.data_capacity(
                Scope {
                    media_id: Some(registered.id),
                    ..Scope::default()
                },
                u64::MAX,
                u64::MAX,
            )?,
            first: true,
            since: None,
        });
        Ok(())
    }

    /// Decodes the newly validated input of the open string, publishes or holds it, and
    /// closes the media lifetime when the string ends.
    fn collect(&mut self, owner: &mut Writer) -> Result<()> {
        let input = self.input.as_mut().ok_or(Error::Closed)?;
        let pending = self.active.as_mut().ok_or(Error::Schema)?;
        let scanned = input.scanner.pending_node(input.document.input(), pending.wire)?;
        let mut chars = json::Decoded::fragment(
            input.document.input(),
            Span {
                start: pending.offset,
                end: scanned.validated_end,
            },
        )?;
        let mut chunk = [0; 36_864];
        let mut used = 0;
        while let Some(c) = chars.next_scalar()? {
            pending.decoder.push(c, |bytes| {
                if used + bytes.len() > chunk.len() {
                    return Err(Error::Limit);
                }
                chunk[used..used + bytes.len()].copy_from_slice(bytes);
                used += bytes.len();
                Ok(())
            })?;
        }
        let mut sink = Sink {
            doc: &mut input.document,
            registry: &mut self.media,
            emitter: &mut self.emitter,
            owner,
            reader: &mut self.reader,
        };
        if used != 0 {
            if pending.first {
                pending.publisher.append(&chunk[..used], sink.emitter, sink.owner)?;
                sink.registry
                    .retain_record(sink.doc, pending.media, sink.owner, sink.reader)?;
                pending.first = false;
            } else {
                pending.stage(&chunk[..used], &mut sink)?;
            }
        }
        pending.offset = scanned.validated_end;
        sink.doc.release_input_range(Span {
            start: pending.start,
            end: pending.offset,
        })?;
        if scanned.span.end != 0 {
            if !pending.decoder.complete() {
                return Err(Error::Schema);
            }
            pending.flush(&mut sink)?;
            pending.publisher.end(sink.emitter, sink.owner)?;
            sink.registry
                .retain_record(sink.doc, pending.media, sink.owner, sink.reader)?;
            self.active = None;
        } else if pending.since.is_some_and(|since| since.elapsed() >= FLUSH_DELAY) {
            pending.flush(&mut sink)?;
        }
        Ok(())
    }

    /// Deadline of the oldest held fragment; set when the buffer opens, so later input never
    /// postpones it.
    pub(super) fn pending_deadline(&self) -> Option<std::time::Instant> {
        self.active.as_ref()?.since.map(|since| since + FLUSH_DELAY)
    }

    pub(super) fn flush_pending(&mut self, owner: &mut Writer) -> Result<()> {
        if let Some(pending) = &mut self.active {
            let input = self.input.as_mut().ok_or(Error::Closed)?;
            pending.flush(&mut Sink {
                doc: &mut input.document,
                registry: &mut self.media,
                emitter: &mut self.emitter,
                owner,
                reader: &mut self.reader,
            })?;
        }
        Ok(())
    }
}

/// Where held bytes go: the media record publication and its retention in the document.
struct Sink<'a> {
    doc: &'a mut Document,
    registry: &'a mut Registry,
    emitter: &'a mut Emitter,
    owner: &'a mut Writer,
    reader: &'a mut ck_stream::Reader,
}

impl StringState {
    fn flush(&mut self, sink: &mut Sink<'_>) -> Result<()> {
        if let Some(buffer) = self.buffer.take() {
            self.publisher.append(buffer.chunk(0)?, sink.emitter, sink.owner)?;
            sink.registry
                .retain_record(sink.doc, self.media, sink.owner, sink.reader)?;
        }
        self.buffered = 0;
        self.since = None;
        Ok(())
    }
    /// Holds decoded bytes in scratch, publishing whenever the buffer fills one record.
    fn stage(&mut self, mut remaining: &[u8], sink: &mut Sink<'_>) -> Result<()> {
        while !remaining.is_empty() {
            if self.buffer.is_none() {
                self.buffer = Some(sink.owner.begin_scratch(self.capacity)?);
                self.since = Some(std::time::Instant::now());
            }
            let count = remaining.len().min(self.capacity - self.buffered);
            self.buffer.as_mut().ok_or(Error::Schema)?.write(&remaining[..count])?;
            self.buffered += count;
            remaining = &remaining[count..];
            if self.buffered == self.capacity {
                self.flush(sink)?;
            }
        }
        Ok(())
    }
}

enum Carrier {
    /// Inline data with its modality and MIME node.
    Native(&'static str, usize),
    /// Inline data whose MIME type has not arrived yet.
    Unresolved,
    Other,
}

/// Classifies a string node: `data` of an `inlineData` part in `contents` or
/// `systemInstruction`, at the root or inside a count request's `generateContentRequest`.
fn native_media(doc: &Document, node: usize) -> Result<Carrier> {
    let parent = |node: usize| -> Result<Option<usize>> { Ok(doc.node(node)?.parent) };
    let named = |node: usize, name: &str| -> Result<bool> {
        match doc.node(node)?.name {
            Some(key) => doc.name_is(key, name),
            None => Ok(false),
        }
    };
    let Some(inline) = parent(node)? else {
        return Ok(Carrier::Other);
    };
    if !named(node, "data")? || !named(inline, "inlineData")? {
        return Ok(Carrier::Other);
    }
    let Some(parts) = parent(inline)?.map(parent).transpose()?.flatten() else {
        return Ok(Carrier::Other);
    };
    if !named(parts, "parts")? || !matches!(doc.node(parts)?.value, Value::Array) {
        return Ok(Carrier::Other);
    }
    let Some(message) = parent(parts)? else {
        return Ok(Carrier::Other);
    };
    let body = if named(message, "systemInstruction")? {
        parent(message)?
    } else {
        match parent(message)? {
            Some(contents) if named(contents, "contents")? => parent(contents)?,
            _ => None,
        }
    };
    let Some(body) = body else {
        return Ok(Carrier::Other);
    };
    let root = doc.root();
    if body != root && !(parent(body)? == Some(root) && named(body, "generateContentRequest")?) {
        return Ok(Carrier::Other);
    }
    let Some(mime) = doc.member(inline, "mimeType")? else {
        return Ok(Carrier::Unresolved);
    };
    if !matches!(
        doc.node(mime)?.value,
        Value::String(Text {
            span: Span { end: 1.., .. },
            ..
        })
    ) {
        return Ok(Carrier::Unresolved);
    }
    Ok(Carrier::Native(
        super::super::request_content::modality(doc, mime)?,
        mime,
    ))
}
