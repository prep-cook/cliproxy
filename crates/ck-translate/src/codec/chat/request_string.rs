use super::*;

/// Input held per data record, counting an incomplete scalar or escape. Advance feeds at most
/// 8,192 bytes between steps, and decoding never grows bytes, so held input plus one step stays
/// within a 36,864-byte decoded chunk.
const HELD_BYTES: u64 = 36_864 - 8_192;
/// Collection delay of held input (docs/INTERNAL-STREAM.md, §3.4).
pub(super) const FLUSH_DELAY: std::time::Duration = std::time::Duration::from_millis(100);

impl Ingress {
    pub(super) fn string_format(
        &mut self,
        owner: &mut Writer,
        parent: usize,
        wire: json::Node,
    ) -> Result<Option<(media::Encoding, &'static str, Option<usize>)>> {
        let parent_node = self.document.node(parent)?;
        let mut carrier = None;
        if let (Some(name), Some(key), Some(part)) = (parent_node.name, wire.key, parent_node.parent) {
            for (native, field, kind) in [
                ("input_audio", "data", "audio"),
                ("image_url", "url", "image"),
                ("file", "file_data", "document"),
            ] {
                if self.document.name_is(name, native)?
                    && json::string_is(self.document.input(), key, field)?
                    && let Some(content) = self.document.node(part)?.parent
                    && matches!(self.document.node(content)?.value, Value::Array)
                    && let Some(name) = self.document.node(content)?.name
                    && self.document.name_is(name, "content")?
                    && let Some(message) = self.document.node(content)?.parent
                    && let Some(messages) = self.document.node(message)?.parent
                    && self.document.node(messages)?.parent == Some(self.document.root())
                    && matches!(self.document.node(messages)?.value, Value::Array)
                    && let Some(name) = self.document.node(messages)?.name
                    && self.document.name_is(name, "messages")?
                {
                    let Some(ty) = self.document.member(part, "type")? else {
                        return Ok(None);
                    };
                    if self.document.string_is(ty, native)? {
                        carrier = Some((native, kind));
                    }
                }
            }
        }
        let Some((native, kind)) = carrier else {
            if wire.validated_end - wire.span.start <= 1024 {
                return Ok(None);
            }
            let mime = self.document.add(None, Value::Text("text/plain;charset=utf-8"))?;
            return Ok(Some((media::Encoding::Utf8, "text", Some(mime))));
        };
        if native == "input_audio" {
            return Ok(Some((media::Encoding::Base64, kind, None)));
        }
        let span = codec::source::Span {
            start: wire.span.start + 1,
            end: wire.validated_end,
        };
        match media::data_uri_prefix(owner, self.document.input(), span)? {
            media::DataUriPrefix::Incomplete => Ok(None),
            media::DataUriPrefix::Plain if native == "file" => Ok(Some((media::Encoding::Base64, kind, None))),
            media::DataUriPrefix::Plain => {
                if wire.validated_end - wire.span.start <= 1024 {
                    return Ok(None);
                }
                let mime = self.document.add(None, Value::Text("text/plain;charset=utf-8"))?;
                Ok(Some((media::Encoding::Utf8, "text", Some(mime))))
            }
            media::DataUriPrefix::Encoded { encoding, mime } => {
                let mime = self.document.import_json(owner, mime)?;
                Ok(Some((encoding, kind, Some(mime))))
            }
        }
    }

    /// Returns the string's node once it is registered as media, and whether the string has ended.
    pub(super) fn stream_string(
        &mut self,
        owner: &mut Writer,
        wire_index: usize,
        parent: usize,
        path: &[Option<Component>],
        publish: bool,
    ) -> Result<(Option<usize>, bool)> {
        let wire = self
            .scanner
            .as_ref()
            .ok_or(Error::Closed)?
            .pending_node(self.document.input(), wire_index)?;
        let opened = self.string.is_none();
        if opened {
            let Some(pending) = self.open_string(owner, wire_index, wire, parent, path, publish)? else {
                return Ok((None, false));
            };
            self.string = Some(pending);
        }
        if self.string.as_ref().ok_or(Error::Schema)?.wire != wire_index {
            return Err(Error::Schema);
        }
        let published = self.append_string(owner, wire, false)?;
        let pending = self.string.as_mut().ok_or(Error::Schema)?;
        // Keys and topology stay fixed while one string streams, and the string node's own span
        // bounds the floor, so the whole-document floor is computed once per string.
        // ponytail: each streamed string still walks the whole document, whose size the
        // default metadata quota limits; 19 us is one sampled shape, not a ceiling. Index key
        // spans and floors if that quota grows (docs/DEFERRED-OPTIONS.md).
        if opened {
            let scanner = self.scanner.as_mut().ok_or(Error::Closed)?;
            self.document.rebind_keys(scanner)?;
            pending.floor = self.document.raw_floor(self.document.root(), u64::MAX)?;
        }
        let node = pending.node;
        if opened || published {
            self.release_string_input(wire)?;
        }
        if wire.span.end != 0 {
            self.string = None;
            return Ok((Some(node), true));
        }
        Ok((Some(node), false))
    }

    fn open_string(
        &mut self,
        owner: &mut Writer,
        wire_index: usize,
        wire: json::Node,
        parent: usize,
        path: &[Option<Component>],
        publish: bool,
    ) -> Result<Option<PendingString>> {
        let Some((encoding, kind, mime)) = self.string_format(owner, parent, wire)? else {
            return Ok(None);
        };
        let text = codec::document::Text {
            source: 0,
            span: wire.span,
        };
        let node = self.document.add(
            wire.key
                .map(|span| Name::Mapped(codec::document::Text { source: 0, span })),
            Value::String(text),
        )?;
        self.document.append(parent, node)?;
        super::super::request_provenance::capture(&mut self.document, owner, node)?;
        let media_index = self.media.len();
        let media = self
            .media
            .register(&mut self.document, node, text, encoding, kind, mime)?;
        if publish {
            field(
                &mut self.document,
                &self.media,
                node,
                path,
                &mut self.emitter,
                owner,
                &mut self.reader,
            )?;
        }
        let publisher = media::Publisher::begin(&self.document, media, &mut self.emitter, owner)?;
        self.media
            .retain_record(&mut self.document, media_index, owner, &mut self.reader)?;
        self.published_media += 1;
        Ok(Some(PendingString {
            wire: wire_index,
            node,
            media: media_index,
            offset: wire.span.start + 1,
            floor: 0,
            held: None,
            publisher,
            decoder: media::InputDecoder::new(encoding),
        }))
    }

    /// Publishes held string input regardless of its size, as the flush contract requires.
    pub(super) fn flush_string(&mut self, owner: &mut Writer) -> Result<()> {
        let Some(pending) = self.string.as_ref() else {
            return Ok(());
        };
        let scanner = self.scanner.as_ref().ok_or(Error::Closed)?;
        let wire = scanner.pending_node(self.document.input(), pending.wire)?;
        if self.append_string(owner, wire, true)? {
            self.release_string_input(wire)?;
        }
        Ok(())
    }

    /// Retires input that the string has published and nothing else still names: everything
    /// before the document and scanner floors, and the string's own published span.
    fn release_string_input(&mut self, wire: json::Node) -> Result<()> {
        let pending = self.string.as_ref().ok_or(Error::Schema)?;
        let scanner = self.scanner.as_ref().ok_or(Error::Closed)?;
        let floor = pending.floor.min(scanner.raw_floor()).min(pending.offset);
        let base = self.document.input().whole().start;
        self.document.release_input_before(floor.max(base))?;
        self.document.release_input_range(codec::source::Span {
            start: wire.span.start + 1,
            end: pending.offset,
        })
    }

    /// Decodes and publishes the string input after the published offset. Input stays held, as
    /// positions in the input source, until it fills a record, the string ends, its collection
    /// delay passes or `force` asks for a flush; small network pieces then share one record.
    /// Returns whether the published offset advanced.
    fn append_string(&mut self, owner: &mut Writer, wire: json::Node, force: bool) -> Result<bool> {
        let pending = self.string.as_mut().ok_or(Error::Schema)?;
        let ended = wire.span.end != 0;
        // An incomplete scalar or escape after the validated end decodes with the next step.
        let received = self.document.input().len();
        if wire.validated_end > pending.offset {
            let since = *pending.held.get_or_insert_with(std::time::Instant::now);
            if !ended && !force && received - pending.offset < HELD_BYTES && since.elapsed() < FLUSH_DELAY {
                return Ok(false);
            }
        }
        pending.held = None;
        let published = wire.validated_end > pending.offset;
        let mut cursor = self.document.input().cursor(codec::source::Span {
            start: pending.offset,
            end: wire.validated_end,
        })?;
        let mut decoder = json::StringDecoder::default();
        let mut chunk = [0; 36_864];
        let mut used = 0;
        while let Some(byte) = cursor.next_byte()? {
            if let Some(c) = decoder.push(byte)? {
                pending.decoder.push(c, |bytes| {
                    if used + bytes.len() > chunk.len() {
                        return Err(Error::Limit);
                    }
                    chunk[used..used + bytes.len()].copy_from_slice(bytes);
                    used += bytes.len();
                    Ok(())
                })?;
            }
        }
        if !decoder.complete() {
            return Err(Error::Json);
        }
        pending.offset = wire.validated_end;
        if used != 0 {
            pending.publisher.append(&chunk[..used], &mut self.emitter, owner)?;
            self.media
                .retain_record(&mut self.document, pending.media, owner, &mut self.reader)?;
        }
        if wire.span.end != 0 {
            if !pending.decoder.complete() {
                return Err(Error::Schema);
            }
            pending.publisher.end(&mut self.emitter, owner)?;
            self.media
                .retain_record(&mut self.document, pending.media, owner, &mut self.reader)?;
        }
        Ok(published)
    }
}
