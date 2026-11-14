use super::*;
use crate::codec::{document::Text, media::Encoding, source::Span};

/// Longest time a decoded fragment may wait in scratch before publication.
const FLUSH_DELAY: std::time::Duration = std::time::Duration::from_millis(100);

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
        // System may follow messages on the wire. Only root fields whose canonical
        // pointers cannot move are published before that ordering is resolved.
        let input = self.input.as_mut().ok_or(Error::Closed)?;
        loop {
            let next = match self.last_field {
                Some(last) => input.scanner.pending_node(input.document.input(), last)?.next,
                None => input.scanner.pending_node(input.document.input(), 0)?.first,
            };
            let Some(wire) = next else { break };
            let scanned = input.scanner.pending_node(input.document.input(), wire)?;
            if scanned.span.end == 0 {
                break;
            }
            let node = input.node(wire)?;
            let name = input.document.node(node)?.name.ok_or(Error::Schema)?;
            if [
                "model",
                "max_tokens",
                "temperature",
                "top_p",
                "stream",
                "metadata",
                "service_tier",
            ]
            .iter()
            .try_fold(false, |found, key| {
                Ok::<_, Error>(found || input.document.name_is(name, key)?)
            })? {
                let mut path = [None; 128];
                path[0] = Some(Component::Name(name));
                fields(
                    &mut input.document,
                    &self.media,
                    node,
                    (&mut path, 1),
                    &mut self.emitter,
                    owner,
                    &mut self.reader,
                )?;
            }
            self.last_field = Some(wire);
        }
        input.document.rebind_keys(&mut input.scanner)?;
        let floor = input
            .document
            .raw_floor(input.document.root(), input.scanner.raw_floor())?;
        input.document.release_input_before(floor)
    }

    fn begin(&mut self, owner: &mut Writer, wire: usize) -> Result<()> {
        let input = self.input.as_mut().ok_or(Error::Closed)?;
        let scanned = input.scanner.pending_node(input.document.input(), wire)?;
        let node = input.node(wire)?;
        let doc = &mut input.document;
        let native = native_media(doc, node)?;
        if native.is_none() && scanned.validated_end - scanned.span.start <= 1024 {
            return Ok(());
        }
        let (encoding, kind, mime) = match native {
            Some(format) => format,
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
        if used != 0 {
            if pending.first {
                pending.publisher.append(&chunk[..used], &mut self.emitter, owner)?;
                self.media
                    .retain_record(&mut input.document, pending.media, owner, &mut self.reader)?;
                pending.first = false;
            } else {
                let mut remaining = &chunk[..used];
                while !remaining.is_empty() {
                    if pending.buffer.is_none() {
                        pending.buffer = Some(owner.begin_scratch(pending.capacity)?);
                        pending.since = Some(std::time::Instant::now());
                    }
                    let count = remaining.len().min(pending.capacity - pending.buffered);
                    pending
                        .buffer
                        .as_mut()
                        .ok_or(Error::Schema)?
                        .write(&remaining[..count])?;
                    pending.buffered += count;
                    remaining = &remaining[count..];
                    if pending.buffered == pending.capacity {
                        pending.flush(
                            &mut input.document,
                            &mut self.media,
                            &mut self.emitter,
                            owner,
                            &mut self.reader,
                        )?;
                    }
                }
            }
        }
        pending.offset = scanned.validated_end;
        input.document.release_input_range(Span {
            start: pending.start,
            end: pending.offset,
        })?;
        if scanned.span.end != 0 {
            if !pending.decoder.complete() {
                return Err(Error::Schema);
            }
            pending.flush(
                &mut input.document,
                &mut self.media,
                &mut self.emitter,
                owner,
                &mut self.reader,
            )?;
            pending.publisher.end(&mut self.emitter, owner)?;
            self.media
                .retain_record(&mut input.document, pending.media, owner, &mut self.reader)?;
            self.active = None;
        } else if pending.since.is_some_and(|since| since.elapsed() >= FLUSH_DELAY) {
            pending.flush(
                &mut input.document,
                &mut self.media,
                &mut self.emitter,
                owner,
                &mut self.reader,
            )?;
        }
        Ok(())
    }

    /// Deadline of the oldest buffered fragment; set when the buffer opens, so later input never postpones it.
    pub(super) fn pending_deadline(&self) -> Option<std::time::Instant> {
        self.active.as_ref()?.since.map(|since| since + FLUSH_DELAY)
    }

    pub(super) fn flush_pending(&mut self, owner: &mut Writer) -> Result<()> {
        if let Some(pending) = &mut self.active {
            let input = self.input.as_mut().ok_or(Error::Closed)?;
            pending.flush(
                &mut input.document,
                &mut self.media,
                &mut self.emitter,
                owner,
                &mut self.reader,
            )?;
        }
        Ok(())
    }
}

impl StringState {
    fn flush(
        &mut self,
        doc: &mut Document,
        registry: &mut Registry,
        emitter: &mut Emitter,
        owner: &mut Writer,
        reader: &mut ck_stream::Reader,
    ) -> Result<()> {
        if let Some(buffer) = self.buffer.take() {
            self.publisher.append(buffer.chunk(0)?, emitter, owner)?;
            registry.retain_record(doc, self.media, owner, reader)?;
        }
        self.buffered = 0;
        self.since = None;
        Ok(())
    }
}

fn native_media(doc: &Document, node: usize) -> Result<Option<(Encoding, &'static str, Option<usize>)>> {
    let Some(source) = doc.node(node)?.parent else {
        return Ok(None);
    };
    let Some(name) = doc.node(node)?.name else {
        return Ok(None);
    };
    if !doc.name_is(name, "data")? || !matches!(doc.node(source)?.value, Value::Object) {
        return Ok(None);
    }
    let Some(name) = doc.node(source)?.name else {
        return Ok(None);
    };
    if !doc.name_is(name, "source")? {
        return Ok(None);
    }
    let Some(part) = doc.node(source)?.parent else {
        return Ok(None);
    };
    if !content_part(doc, part)? {
        return Ok(None);
    }
    let kind = if super::super::is(doc, part, "type", "image")? {
        "image"
    } else if super::super::is(doc, part, "type", "document")? {
        "document"
    } else {
        return Ok(None);
    };
    let encoding = if super::super::is(doc, source, "type", "base64")? {
        Encoding::Base64
    } else if super::super::is(doc, source, "type", "text")? {
        Encoding::Utf8
    } else {
        return Ok(None);
    };
    let mime = doc.member(source, "media_type")?.filter(|&node| {
        matches!(
            doc.node(node).map(|n| n.value),
            Ok(Value::String(Text {
                span: Span { end: 1.., .. },
                ..
            }))
        )
    });
    Ok(Some((encoding, kind, mime)))
}

fn content_part(doc: &Document, part: usize) -> Result<bool> {
    let Some(array) = doc.node(part)?.parent else {
        return Ok(false);
    };
    if !matches!(doc.node(array)?.value, Value::Array) {
        return Ok(false);
    }
    let Some(name) = doc.node(array)?.name else {
        return Ok(false);
    };
    let Some(parent) = doc.node(array)?.parent else {
        return Ok(false);
    };
    if doc.name_is(name, "system")? && parent == doc.root() {
        return Ok(true);
    }
    if !doc.name_is(name, "content")? {
        return Ok(false);
    }
    if let Some(messages) = doc.node(parent)?.parent
        && doc.node(messages)?.parent == Some(doc.root())
        && let Some(name) = doc.node(messages)?.name
        && doc.name_is(name, "messages")?
    {
        return Ok(true);
    }
    Ok(super::super::is(doc, parent, "type", "tool_result")? && content_part(doc, parent)?)
}
