use super::{Error, Result, document::Document, event::Kind, source::Source};
use ck_stream::{Lease, Position, Writer};

const KINDS: &[Kind] = &[
    Kind::Chunk,
    Kind::RequestStart,
    Kind::RequestField,
    Kind::RequestEnd,
    Kind::ResponseStart,
    Kind::ItemStart,
    Kind::ItemEnd,
    Kind::BlockStart,
    Kind::BlockEnd,
    Kind::ReasoningSignature,
    Kind::ReasoningRedacted,
    Kind::ToolStart,
    Kind::ToolEnd,
    Kind::ToolResult,
    Kind::Annotation,
    Kind::Grounding,
    Kind::CodeExecution,
    Kind::UrlContext,
    Kind::Usage,
    Kind::CountTokens,
    Kind::Compact,
    Kind::MediaStart,
    Kind::Data,
    Kind::MediaTranscript,
    Kind::MediaEnd,
    Kind::Operation,
    Kind::Opaque,
    Kind::Heartbeat,
    Kind::Done,
    Kind::Error,
    Kind::Cancelled,
];

/// Decode a trusted canonical lease, retaining its bytes rather than copying SSE data.
/// Wire ingress must never call this function on externally assigned record kinds.
pub fn decode(owner: &mut Writer, lease: Lease) -> Result<(Kind, Document)> {
    let record = lease.record();
    if record.start.generation != owner.id() {
        return Err(Error::Position);
    }
    let kind = *KINDS.get(record.kind as usize).ok_or(Error::Schema)?;
    let mut frame = Frame::new();
    lease.visit(|chunk| {
        for &byte in chunk {
            frame.byte(kind, byte);
        }
    })?;
    let start = frame.data_start.ok_or(Error::Schema)?;
    let end = frame.data_end.ok_or(Error::Truncated)?;
    if frame.invalid || end + 2 != record.len() || frame.name_seen != kind.name().is_some() {
        return Err(Error::Schema);
    }
    let mut source = Source::new(owner)?;
    source.push(lease, advance(record.start, start)?..advance(record.start, end)?)?;
    let mut doc = Document::parse(owner, source, record.len())?;
    let x = doc.required(doc.root(), "x_ck")?;
    if doc.unsigned(doc.required(x, "v")?)? != 1 || Some(doc.unsigned(doc.required(x, "seq")?)?) != frame.sequence {
        return Err(Error::Schema);
    }
    if kind == Kind::Chunk {
        super::chat::prepare_response(&mut doc)?;
    }
    Ok((kind, doc))
}

/// SSE framing of one canonical record, scanned one byte at a time.
struct Frame {
    header: [u8; 96],
    header_len: usize,
    offset: u64,
    data_start: Option<u64>,
    data_end: Option<u64>,
    sequence: Option<u64>,
    name_seen: bool,
    line_start: u64,
    invalid: bool,
}

impl Frame {
    fn new() -> Self {
        Self {
            header: [0; 96],
            header_len: 0,
            offset: 0,
            data_start: None,
            data_end: None,
            sequence: None,
            name_seen: false,
            line_start: 0,
            invalid: false,
        }
    }

    fn byte(&mut self, kind: Kind, byte: u8) {
        if self.data_start.is_some() {
            if byte == b'\n' {
                if let Some(end) = self.data_end {
                    if self.offset != end + 1 {
                        self.invalid = true;
                    }
                } else {
                    self.data_end = Some(self.offset);
                }
            } else if self.data_end.is_some() {
                self.invalid = true;
            }
        } else if byte == b'\n' {
            self.header_line(kind);
            self.header_len = 0;
            self.line_start = self.offset + 1;
        } else {
            if self.header_len == self.header.len() {
                self.invalid = true;
            } else {
                self.header[self.header_len] = byte;
                self.header_len += 1;
            }
            if self.header_len == 6 && &self.header[..6] == b"data: " {
                self.data_start = Some(self.line_start + 6);
            }
        }
        self.offset += 1;
    }

    fn header_line(&mut self, kind: Kind) {
        let line = &self.header[..self.header_len];
        if let Some(name) = line.strip_prefix(b"event: ") {
            if self.name_seen || kind.name().is_none_or(|expected| name != expected.as_bytes()) {
                self.invalid = true;
            }
            self.name_seen = true;
        } else if let Some(id) = line.strip_prefix(b"id: ") {
            let mut parsed = Some(0u64);
            if id.is_empty() {
                self.invalid = true;
            }
            for &b in id {
                parsed = if b.is_ascii_digit() {
                    parsed
                        .and_then(|n| n.checked_mul(10))
                        .and_then(|n| n.checked_add(u64::from(b - b'0')))
                } else {
                    None
                };
            }
            if self.sequence.is_some() || parsed.is_none() {
                self.invalid = true;
            }
            self.sequence = parsed;
        } else {
            self.invalid = true;
        }
    }
}

pub(crate) fn next(owner: &mut Writer, reader: &mut ck_stream::Reader) -> Result<(Kind, Document)> {
    match reader.try_next() {
        std::task::Poll::Ready(Some(ck_stream::Read::Record(lease))) => decode(owner, lease),
        _ => Err(Error::Position),
    }
}

fn advance(position: Position, delta: u64) -> Result<Position> {
    let offset = u64::from(position.offset).checked_add(delta).ok_or(Error::Position)?;
    Ok(Position {
        generation: position.generation,
        frame: position
            .frame
            .checked_add(offset / ck_stream::FRAME_BYTES as u64)
            .ok_or(Error::Position)?,
        offset: (offset % ck_stream::FRAME_BYTES as u64) as u32,
    })
}
