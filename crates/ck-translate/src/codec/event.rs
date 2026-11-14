use ck_stream::{Position, Record, Writer};

use super::{
    Error, Result, ValidatedRequest,
    encode::{self, Encoder},
};

/// Trusted gateway identity. Provider IDs remain position-backed provenance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalId {
    bytes: [u8; 64],
    len: u8,
}
impl LocalId {
    pub fn new(value: &str) -> Result<Self> {
        if value.is_empty() || value.len() > 64 || !value.is_ascii() {
            return Err(Error::Schema);
        }
        let mut bytes = [0; 64];
        bytes[..value.len()].copy_from_slice(value.as_bytes());
        Ok(Self {
            bytes,
            len: value.len() as u8,
        })
    }
    pub fn as_str(&self) -> &str {
        // Construction accepts ASCII only; the remaining bytes are not exposed.
        std::str::from_utf8(&self.bytes[..usize::from(self.len)]).unwrap()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Direction {
    Request,
    Response,
}

#[derive(Clone, Copy, Debug)]
pub struct Identity {
    pub request_id: LocalId,
    pub stream_id: LocalId,
    pub attempt: u64,
    pub direction: Direction,
}

/// Correlation contains only bounded local identities and scalar indexes.
#[derive(Clone, Copy, Debug, Default)]
pub struct Scope {
    pub choice_index: Option<u64>,
    pub item_id: Option<LocalId>,
    pub block_index: Option<u64>,
    pub tool_call_id: Option<LocalId>,
    pub tool_index: Option<u64>,
    pub media_id: Option<LocalId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum Kind {
    Chunk,
    RequestStart,
    RequestField,
    RequestEnd,
    ResponseStart,
    ItemStart,
    ItemEnd,
    BlockStart,
    BlockEnd,
    ReasoningSignature,
    ReasoningRedacted,
    ToolStart,
    ToolEnd,
    ToolResult,
    Annotation,
    Grounding,
    CodeExecution,
    UrlContext,
    Usage,
    CountTokens,
    Compact,
    MediaStart,
    Data,
    MediaTranscript,
    MediaEnd,
    Operation,
    Opaque,
    Heartbeat,
    Done,
    Error,
    Cancelled,
}
impl Kind {
    pub fn name(self) -> Option<&'static str> {
        Some(match self {
            Self::Chunk => return None,
            Self::RequestStart => "x_ck.request.start",
            Self::RequestField => "x_ck.request.field",
            Self::RequestEnd => "x_ck.request.end",
            Self::ResponseStart => "x_ck.response.start",
            Self::ItemStart => "x_ck.item.start",
            Self::ItemEnd => "x_ck.item.end",
            Self::BlockStart => "x_ck.block.start",
            Self::BlockEnd => "x_ck.block.end",
            Self::ReasoningSignature => "x_ck.reasoning.signature",
            Self::ReasoningRedacted => "x_ck.reasoning.redacted",
            Self::ToolStart => "x_ck.tool.start",
            Self::ToolEnd => "x_ck.tool.end",
            Self::ToolResult => "x_ck.tool.result",
            Self::Annotation => "x_ck.annotation",
            Self::Grounding => "x_ck.grounding",
            Self::CodeExecution => "x_ck.code_execution",
            Self::UrlContext => "x_ck.url_context",
            Self::Usage => "x_ck.usage",
            Self::CountTokens => "x_ck.count_tokens",
            Self::Compact => "x_ck.compact",
            Self::MediaStart => "x_ck.media.start",
            Self::Data => "x_ck.data",
            Self::MediaTranscript => "x_ck.media.transcript",
            Self::MediaEnd => "x_ck.media.end",
            Self::Operation => "x_ck.operation",
            Self::Opaque => "x_ck.opaque",
            Self::Heartbeat => "x_ck.heartbeat",
            Self::Done => "x_ck.done",
            Self::Error => "x_ck.error",
            Self::Cancelled => "x_ck.cancelled",
        })
    }
    fn terminal(self) -> bool {
        matches!(self, Self::Done | Self::Error | Self::Cancelled)
    }
}

/// Writes the trusted envelope and complete SSE records without heap payloads.
///
/// One emitter owns one fresh canonical Writer. Raw framing scratch must never
/// be appended to that Writer. Payload and provenance closures are trusted codec
/// code: each writes zero or more comma-prefixed JSON members and must not write
/// envelope identity fields or duplicate keys. Wire fields go through validated
/// spans, not verbatim root-object insertion. Protocol codecs own choice/item/
/// tool/media lifecycle and payload schema validation; this type owns sequence,
/// stream identity, start/end fencing and terminal publication.
pub struct Emitter {
    identity: Identity,
    sequence: u64,
    end: Option<Position>,
    started: bool,
    request_ended: bool,
    request: Option<(u64, super::Operation)>,
    terminal: bool,
}
impl Emitter {
    pub fn new(identity: Identity) -> Self {
        Self {
            identity,
            sequence: 0,
            end: None,
            started: false,
            request_ended: false,
            request: None,
            terminal: false,
        }
    }

    pub fn emit(
        &mut self,
        output: &mut Writer,
        kind: Kind,
        scope: Scope,
        payload: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
        provenance: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
    ) -> Result<Record> {
        if matches!(kind, Kind::RequestStart | Kind::RequestEnd) {
            return Err(Error::Schema);
        }
        self.publish(output, kind, scope, payload, provenance)
    }

    pub fn request_start(&mut self, output: &mut Writer, request: super::RequestView<'_>) -> Result<Record> {
        request.document.check_owner(output)?;
        let record = self.publish(
            output,
            Kind::RequestStart,
            Scope::default(),
            |encoder| {
                encoder.bytes(b",\"operation\":")?;
                encoder.string(match request.operation {
                    super::Operation::Generate => "generate",
                    super::Operation::CountTokens => "count_tokens",
                    super::Operation::Compact => "compact",
                })
            },
            |_| Ok(()),
        )?;
        self.request = Some((request.document.id(), request.operation));
        Ok(record)
    }

    /// Only a successful capability fence can publish request.end.
    pub fn request_end(&mut self, output: &mut Writer, request: &ValidatedRequest<'_>) -> Result<Record> {
        request.view().document.check_owner(output)?;
        if self.request != Some((request.view().document.id(), request.view().operation)) {
            return Err(Error::Position);
        }
        self.publish(output, Kind::RequestEnd, Scope::default(), |_| Ok(()), |_| Ok(()))
    }

    fn publish(
        &mut self,
        output: &mut Writer,
        kind: Kind,
        scope: Scope,
        payload: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
        provenance: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
    ) -> Result<Record> {
        if self.terminal {
            return Err(Error::Closed);
        }
        let position = output.committed();
        if self
            .end
            .map_or(position.frame != 0 || position.offset != 0, |end| end != position)
        {
            return Err(Error::Position);
        }
        let start = match self.identity.direction {
            Direction::Request => Kind::RequestStart,
            Direction::Response => Kind::ResponseStart,
        };
        if kind != Kind::Error && kind != Kind::Cancelled {
            if !self.started && kind != start {
                return Err(Error::Schema);
            }
            if self.started && matches!(kind, Kind::RequestStart | Kind::ResponseStart) {
                return Err(Error::Schema);
            }
            if self.identity.direction == Direction::Response
                && matches!(kind, Kind::RequestStart | Kind::RequestField | Kind::RequestEnd)
            {
                return Err(Error::Schema);
            }
            if self.identity.direction == Direction::Request
                && (kind == Kind::ResponseStart
                    || (kind == Kind::Done && !self.request_ended)
                    || (self.request_ended && kind != Kind::Done))
            {
                return Err(Error::Schema);
            }
        }
        let next = self.sequence.checked_add(1).ok_or(Error::Limit)?;
        let record = encode::publish_record(output, kind as u32, 0, kind == Kind::Data, |encoder| {
            self.encode_event(encoder, kind, scope, &payload, &provenance)
        })?;
        self.sequence = next;
        self.end = Some(record.end);
        self.started |= kind == start;
        self.request_ended |= kind == Kind::RequestEnd;
        self.terminal = kind.terminal();
        Ok(record)
    }

    pub(crate) fn data_capacity(&self, scope: Scope, index: u64, offset: u64) -> Result<usize> {
        let framing = encode::measure(|e| {
            self.encode_event(
                e,
                Kind::Data,
                scope,
                &|e| {
                    e.bytes(b",\"index\":")?;
                    e.unsigned(index)?;
                    e.bytes(b",\"offset\":")?;
                    e.unsigned(offset)?;
                    e.bytes(b",\"bytes\":\"\"")
                },
                &|_| Ok(()),
            )
        })?;
        let available = ck_stream::FRAME_BYTES
            .checked_sub(framing)
            .ok_or(Error::EventTooLarge)?;
        let decoded = (available / 4 * 3).min(36_864);
        if decoded == 0 {
            return Err(Error::EventTooLarge);
        }
        Ok(decoded)
    }

    fn encode_event(
        &self,
        encoder: &mut Encoder<'_, '_>,
        kind: Kind,
        scope: Scope,
        payload: &impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
        provenance: &impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
    ) -> Result<()> {
        if let Some(name) = kind.name() {
            encoder.bytes(b"event: ")?;
            encoder.bytes(name.as_bytes())?;
            encoder.bytes(b"\n")?;
        }
        encoder.bytes(b"id: ")?;
        encoder.unsigned(self.sequence)?;
        encoder.bytes(b"\ndata: {\"x_ck\":{\"v\":1,\"request_id\":")?;
        encoder.string(self.identity.request_id.as_str())?;
        encoder.bytes(b",\"stream_id\":")?;
        encoder.string(self.identity.stream_id.as_str())?;
        encoder.bytes(b",\"attempt\":")?;
        encoder.unsigned(self.identity.attempt)?;
        encoder.bytes(b",\"seq\":")?;
        encoder.unsigned(self.sequence)?;
        encoder.bytes(b",\"direction\":")?;
        encoder.string(match self.identity.direction {
            Direction::Request => "request",
            Direction::Response => "response",
        })?;
        write_scope(encoder, scope)?;
        provenance(encoder)?;
        encoder.bytes(b"}")?;
        payload(encoder)?;
        encoder.bytes(b"}\n\n")
    }
}

fn write_scope(encoder: &mut Encoder<'_, '_>, scope: Scope) -> Result<()> {
    for (name, value) in [
        ("choice_index", scope.choice_index),
        ("block_index", scope.block_index),
        ("tool_index", scope.tool_index),
    ] {
        if let Some(value) = value {
            encoder.bytes(b",")?;
            encoder.string(name)?;
            encoder.bytes(b":")?;
            encoder.unsigned(value)?;
        }
    }
    for (name, value) in [
        ("item_id", scope.item_id),
        ("tool_call_id", scope.tool_call_id),
        ("media_id", scope.media_id),
    ] {
        if let Some(value) = value {
            encoder.bytes(b",")?;
            encoder.string(name)?;
            encoder.bytes(b":")?;
            encoder.string(value.as_str())?;
        }
    }
    Ok(())
}
