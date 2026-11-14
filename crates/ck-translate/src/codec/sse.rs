use super::{
    Error, Result,
    json::Utf8,
    source::{Source, Span},
};

/// Every external line is retained, including comments and unknown fields.
#[derive(Clone, Copy, Debug)]
pub struct Field {
    pub name: Span,
    pub value: Span,
    pub comment: bool,
}

/// A complete external SSE record. Spans refer to the supplied `Source`.
/// Multiple data lines are joined with LF by `write_data`; no heap body is built.
pub struct Event {
    source_id: u64,
    stream_id: u64,
    _metadata: Option<ck_stream::MetadataPermit>,
    pub span: Span,
    fields: Vec<Field>,
}

impl Event {
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }
    pub fn try_clone(&self, owner: &mut ck_stream::Writer) -> Result<Self> {
        if owner.id() != self.stream_id {
            return Err(Error::Position);
        }
        let mut metadata = owner.reserve_metadata(self.fields.len() * std::mem::size_of::<Field>())?;
        let fields = self.fields.clone();
        metadata.resize(fields.capacity() * std::mem::size_of::<Field>())?;
        Ok(Self {
            source_id: self.source_id,
            stream_id: self.stream_id,
            _metadata: Some(metadata),
            span: self.span,
            fields,
        })
    }
    fn validate_source(&self, source: &Source) -> Result<()> {
        if self.source_id != source.id() {
            return Err(Error::Position);
        }
        source.check_live()
    }

    pub fn name(&self, source: &Source) -> Result<Option<Span>> {
        self.validate_source(source)?;
        let mut name = None;
        for field in &self.fields {
            if !field.comment && source.equals(field.name, b"event")? {
                name = Some(field.value);
            }
        }
        Ok(name)
    }

    pub fn has_data(&self, source: &Source) -> Result<bool> {
        self.validate_source(source)?;
        for field in &self.fields {
            if !field.comment && source.equals(field.name, b"data")? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn data_len(&self, source: &Source) -> Result<usize> {
        self.validate_source(source)?;
        let mut length = 0usize;
        let mut first = true;
        for field in &self.fields {
            if !field.comment && source.equals(field.name, b"data")? {
                length = length
                    .checked_add(usize::try_from(field.value.len()).map_err(|_| Error::Limit)?)
                    .and_then(|n| n.checked_add(usize::from(!first)))
                    .ok_or(Error::Limit)?;
                first = false;
            }
        }
        Ok(length)
    }

    pub fn write_data(&self, source: &Source, mut write: impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        self.validate_source(source)?;
        let mut first = true;
        for field in &self.fields {
            if !field.comment && source.equals(field.name, b"data")? {
                if !first {
                    write(b"\n")?;
                }
                source.visit(field.value, &mut write)?;
                first = false;
            }
        }
        Ok(())
    }
}

/// Incremental LF/CRLF/CR scanner with UTF-8 state spanning input fragments.
///
/// Callbacks see only blank-line-terminated records, including metadata-only
/// records. A callback error poisons the scanner. EOF never flushes a partial
/// record; the protocol codec must separately validate its terminal marker.
pub struct Scanner {
    offset: u64,
    source_id: Option<u64>,
    line: u64,
    event: u64,
    colon: Option<u64>,
    pending_cr: bool,
    fields: Vec<Field>,
    max_event: u64,
    metadata: ck_stream::MetadataPermit,
    utf8: Utf8,
    failed: bool,
    finished: bool,
}

impl Scanner {
    pub fn new(owner: &mut ck_stream::Writer, max_event: u64) -> Result<Self> {
        Ok(Self {
            offset: 0,
            source_id: None,
            line: 0,
            event: 0,
            colon: None,
            pending_cr: false,
            fields: Vec::new(),
            max_event,
            metadata: owner.reserve_metadata(0)?,
            utf8: Utf8::default(),
            failed: false,
            finished: false,
        })
    }

    pub fn consumed(&self) -> u64 {
        self.event
    }

    pub fn advance(&mut self, source: &Source, mut event: impl FnMut(&Event) -> Result<()>) -> Result<()> {
        if self.failed || self.finished {
            return Err(Error::Closed);
        }
        let result = self.scan(source, &mut event);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn scan(&mut self, source: &Source, emit: &mut impl FnMut(&Event) -> Result<()>) -> Result<()> {
        if source.stream_id() != self.metadata.stream_id() || self.source_id.is_some_and(|id| id != source.id()) {
            return Err(Error::Position);
        }
        self.source_id = Some(source.id());
        let mut cursor = source.cursor(Span {
            start: self.offset,
            end: source.len(),
        })?;
        while let Some(byte) = cursor.next_byte()? {
            self.utf8.push(byte)?;
            let at = self.offset;
            self.offset += 1;
            if at == 2 && source.equals(Span { start: 0, end: 3 }, b"\xef\xbb\xbf")? {
                self.line = 3;
            }
            if self.pending_cr {
                self.pending_cr = false;
                if byte == b'\n' {
                    self.line = self.offset;
                    if self.fields.is_empty() {
                        self.event = self.offset;
                    }
                    continue;
                }
            }
            if self.offset - self.event > self.max_event {
                return Err(Error::EventTooLarge);
            }
            match byte {
                b'\r' | b'\n' => {
                    self.utf8.finish()?;
                    self.end_line(source, at, emit)?;
                    self.line = self.offset;
                    self.pending_cr = byte == b'\r';
                }
                b':' if self.colon.is_none() => self.colon = Some(at),
                _ => {}
            }
        }
        Ok(())
    }

    fn end_line(&mut self, source: &Source, end: u64, emit: &mut impl FnMut(&Event) -> Result<()>) -> Result<()> {
        if self.line == end {
            let fields = std::mem::take(&mut self.fields);
            let span = Span {
                start: self.event,
                end: self.offset,
            };
            self.event = self.offset;
            let mut event = Event {
                source_id: source.id(),
                stream_id: source.stream_id(),
                _metadata: None,
                span,
                fields,
            };
            let result = emit(&event);
            event.fields.clear();
            self.fields = event.fields;
            result?;
        } else {
            if self.fields.len() == self.fields.capacity() {
                let capacity = self.fields.capacity().max(4).checked_mul(2).ok_or(Error::Limit)?;
                self.metadata
                    .resize(capacity.checked_mul(std::mem::size_of::<Field>()).ok_or(Error::Limit)?)?;
                self.fields
                    .try_reserve_exact(capacity - self.fields.len())
                    .map_err(|_| Error::Limit)?;
                self.metadata
                    .resize(self.fields.capacity() * std::mem::size_of::<Field>())?;
            }
            let comment = self.colon == Some(self.line);
            let name = Span {
                start: self.line,
                end: self.colon.unwrap_or(end),
            };
            let mut value = Span {
                start: self.colon.map_or(end, |at| at + 1),
                end,
            };
            if value.start < value.end
                && source.equals(
                    Span {
                        start: value.start,
                        end: value.start + 1,
                    },
                    b" ",
                )?
            {
                value.start += 1;
            }
            self.fields.push(Field { name, value, comment });
        }
        self.colon = None;
        Ok(())
    }

    pub fn finish(&mut self, source: &Source, emit: impl FnMut(&Event) -> Result<()>) -> Result<()> {
        self.advance(source, emit)?;
        let result = self.utf8.finish().and({
            if self.line != self.offset || !self.fields.is_empty() {
                Err(Error::Truncated)
            } else {
                Ok(())
            }
        });
        if result.is_err() {
            self.failed = true;
        } else {
            self.finished = true;
        }
        result
    }
}
