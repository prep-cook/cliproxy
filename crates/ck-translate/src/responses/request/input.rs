use super::*;
use crate::codec::{
    document::{Text, pending::Pending},
    json,
    media::{self, Encoding, InputDecoder, Publisher},
    source::Span,
};
use std::time::{Duration, Instant};

/// Longest time a collected fragment waits for more input before it is published.
const FLUSH_DELAY: Duration = Duration::from_millis(100);

pub(super) struct Input {
    pub pending: Pending,
    pub published_media: usize,
    started: bool,
    active: Option<Active>,
}

struct Active {
    wire: usize,
    media: usize,
    start: u64,
    offset: u64,
    decoder: InputDecoder,
    publisher: Publisher,
    pending_since: Option<Instant>,
}

impl Input {
    pub fn new(owner: &mut Writer, limit: u64) -> Result<Self> {
        Ok(Self {
            pending: Pending::new(owner, limit)?,
            published_media: 0,
            started: false,
            active: None,
        })
    }

    /// Deadline of the oldest collected fragment; set when collection starts, so later input
    /// never postpones it.
    pub fn deadline(&self) -> Option<Instant> {
        self.active.as_ref()?.pending_since.map(|since| since + FLUSH_DELAY)
    }

    /// Publishes what the scanner validated; `flush` publishes a collected fragment before its
    /// deadline.
    pub fn consume(
        &mut self,
        owner: &mut Writer,
        media: &mut Registry,
        emitter: &mut Emitter,
        reader: &mut ck_stream::Reader,
        operation: Operation,
        flush: bool,
    ) -> Result<()> {
        if self.pending.scanner.nodes().is_empty() {
            return Ok(());
        }
        if !self.started {
            emitter.request_start(
                owner,
                RequestView {
                    document: &self.pending.document,
                    media,
                    operation,
                },
            )?;
            codec::internal::next(owner, reader)?;
            let root = self.pending.document.root();
            publish_field(&mut self.pending.document, root, &[], media, emitter, owner, reader)?;
            self.started = true;
        }
        // Model is independent of input/instructions ordering and needs no native rewrite.
        let doc = &mut self.pending.document;
        if let Some(model) = doc.member(doc.root(), "model")?
            && !doc.published(model)?
            && let Value::String(text) = doc.node(model)?.value
            && text.span.end != 0
        {
            publish_field(
                doc,
                model,
                &[Some(Component::Name(Name::Static("model")))],
                media,
                emitter,
                owner,
                reader,
            )?;
            doc.rebind_keys(&mut self.pending.scanner)?;
        }
        if self.active.is_some() {
            self.collect(owner, media, emitter, reader, flush)?;
        }
        if self.active.is_none()
            && let Some(wire) = self.pending.string
        {
            self.begin(owner, media, emitter, reader, wire)?;
            if self.active.is_some() {
                self.collect(owner, media, emitter, reader, flush)?;
            }
        }
        Ok(())
    }

    fn begin(
        &mut self,
        owner: &mut Writer,
        registry: &mut Registry,
        emitter: &mut Emitter,
        reader: &mut ck_stream::Reader,
        wire: usize,
    ) -> Result<()> {
        let scanned = self.pending.scanner.pending_node(self.pending.document.input(), wire)?;
        let node = self.pending.node(wire)?;
        let Some((encoding, kind, mime)) = self.format(owner, node, scanned)? else {
            return Ok(());
        };
        let media_index = registry.len();
        let media = registry.register(
            &mut self.pending.document,
            node,
            Text {
                source: 0,
                span: scanned.span,
            },
            encoding,
            kind,
            mime,
        )?;
        let publisher = Publisher::begin(&self.pending.document, media, emitter, owner)?;
        registry.retain_record(&mut self.pending.document, media_index, owner, reader)?;
        self.published_media += 1;
        self.active = Some(Active {
            wire,
            media: media_index,
            start: scanned.span.start + 1,
            offset: scanned.span.start + 1,
            decoder: InputDecoder::new(encoding),
            publisher,
            pending_since: None,
        });
        Ok(())
    }

    fn format(
        &mut self,
        owner: &mut Writer,
        node: usize,
        wire: json::Node,
    ) -> Result<Option<(Encoding, &'static str, Option<usize>)>> {
        let doc = &mut self.pending.document;
        let value = doc.node(node)?;
        let mut binary = None;
        if let (Some(parent), Some(name)) = (value.parent, value.name) {
            if doc.name_is(name, "data")?
                && let Some(parent_name) = doc.node(parent)?.name
                && doc.name_is(parent_name, "input_audio")?
                && let Some(part) = doc.node(parent)?.parent
                && input_part(doc, part)?
            {
                let Some(kind) = doc.member(part, "type")? else {
                    return Ok(None);
                };
                if doc.string_is(kind, "input_audio")? {
                    return Ok(Some((Encoding::Base64, "audio", None)));
                }
            }
            let image = doc.name_is(name, "image_url")?;
            let file = doc.name_is(name, "file_data")?;
            if image || file {
                // A late type must not turn an unknown literal field into trusted media.
                let Some(kind) = doc.member(parent, "type")? else {
                    return Ok(None);
                };
                if matches!(
                    doc.node(kind)?.value,
                    Value::String(Text {
                        span: Span { end: 0, .. },
                        ..
                    })
                ) {
                    return Ok(None);
                }
                if ((image && doc.string_is(kind, "input_image")?) || (file && doc.string_is(kind, "input_file")?))
                    && input_part(doc, parent)?
                {
                    binary = Some((if image { "image" } else { "document" }, file));
                }
            }
        }
        if let Some((kind, file)) = binary {
            return match media::data_uri_prefix(
                owner,
                doc.input(),
                Span {
                    start: wire.span.start + 1,
                    end: wire.validated_end,
                },
            )? {
                media::DataUriPrefix::Incomplete => Ok(None),
                media::DataUriPrefix::Encoded { encoding, mime } => {
                    let mime = doc.import_json(owner, mime)?;
                    Ok(Some((encoding, kind, Some(mime))))
                }
                media::DataUriPrefix::Plain if file => Ok(Some((Encoding::Base64, kind, None))),
                media::DataUriPrefix::Plain => self.utf8(wire),
            };
        }
        self.utf8(wire)
    }

    fn utf8(&mut self, wire: json::Node) -> Result<Option<(Encoding, &'static str, Option<usize>)>> {
        if wire.validated_end - wire.span.start <= 1024 {
            return Ok(None);
        }
        let mime = self
            .pending
            .document
            .add(None, Value::Text("text/plain;charset=utf-8"))?;
        Ok(Some((Encoding::Utf8, "text", Some(mime))))
    }

    fn collect(
        &mut self,
        owner: &mut Writer,
        registry: &mut Registry,
        emitter: &mut Emitter,
        reader: &mut ck_stream::Reader,
        flush: bool,
    ) -> Result<()> {
        let active = self.active.as_mut().ok_or(Error::Schema)?;
        let wire = self
            .pending
            .scanner
            .pending_node(self.pending.document.input(), active.wire)?;
        if wire.validated_end > active.offset {
            let since = active.pending_since.get_or_insert_with(Instant::now);
            if !flush
                && active.offset != active.start
                && wire.span.end == 0
                && wire.validated_end - active.offset < 8192
                && since.elapsed() < FLUSH_DELAY
            {
                return Ok(());
            }
            let mut chars = json::Decoded::fragment(
                self.pending.document.input(),
                Span {
                    start: active.offset,
                    end: wire.validated_end,
                },
            )?;
            let mut chunk = [0; 36_864];
            let mut used = 0;
            while let Some(c) = chars.next_scalar()? {
                active.decoder.push(c, |bytes| {
                    let end = used + bytes.len();
                    chunk.get_mut(used..end).ok_or(Error::Limit)?.copy_from_slice(bytes);
                    used = end;
                    Ok(())
                })?;
            }
            if used != 0 {
                active.publisher.append(&chunk[..used], emitter, owner)?;
                registry.retain_record(&mut self.pending.document, active.media, owner, reader)?;
            }
            active.offset = wire.validated_end;
            active.pending_since = None;
            self.pending.document.release_input_range(Span {
                start: active.start,
                end: active.offset,
            })?;
        }
        if wire.span.end != 0 {
            if !active.decoder.complete() {
                return Err(Error::Schema);
            }
            active.publisher.end(emitter, owner)?;
            registry.retain_record(&mut self.pending.document, active.media, owner, reader)?;
            self.active = None;
        }
        Ok(())
    }
}

/// True when `part` is a content part of an item in the root `input` array.
fn input_part(doc: &Document, part: usize) -> Result<bool> {
    let Some(content) = doc.node(part)?.parent else {
        return Ok(false);
    };
    let node = doc.node(content)?;
    let named = |name: Option<Name>, value| name.map_or(Ok(false), |name| doc.name_is(name, value));
    if !matches!(node.value, Value::Array) || !named(node.name, "content")? {
        return Ok(false);
    }
    let Some(input) = node
        .parent
        .map(|item| doc.node(item))
        .transpose()?
        .and_then(|item| item.parent)
    else {
        return Ok(false);
    };
    let input = doc.node(input)?;
    Ok(input.parent == Some(doc.root()) && named(input.name, "input")?)
}
