//! Boundary-codec registry for the new harness: one arm per protocol and role.
//!
//! Extension point: when a protocol's codecs merge, add a `Protocol` variant, map its
//! fixture names in `parse`, and add its arm to each constructor and `validate_encode`.
//! Fixture names follow the old pair formats: `openai` is Chat Completions, `claude` is
//! Anthropic Messages; `openai-response`/`codex` (Responses), `gemini`, `interactions`
//! and `antigravity` stay unsupported until then, and a workload naming one fails the run.
use crate::anthropic;
use crate::codec::{
    self, BodyMode, Operation, RequestEgress, RequestIngress, ResponseEgress, ResponseIngress, chat,
    event::{Direction, Identity, LocalId},
};
use ck_stream::{Lease, ScratchLease, Store, Writer};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Protocol {
    Chat,
    Anthropic,
}

impl Protocol {
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "openai" => Some(Self::Chat),
            "claude" => Some(Self::Anthropic),
            _ => None,
        }
    }
    pub const SUPPORTED: &[&str] = &["openai", "claude"];
}

pub fn identity(direction: Direction) -> codec::Result<Identity> {
    Ok(Identity {
        request_id: LocalId::new("request")?,
        stream_id: LocalId::new("stream")?,
        attempt: 1,
        direction,
    })
}

/// Client decode: the client protocol's request ingress.
pub enum RequestIn {
    Chat(chat::request::Ingress),
    Anthropic(anthropic::request::Ingress),
}

impl RequestIn {
    pub fn new(client: Protocol, owner: &mut Writer, store: &Store, limit: u64) -> codec::Result<Self> {
        let id = identity(Direction::Request)?;
        Ok(match client {
            Protocol::Chat => Self::Chat(chat::request::Ingress::new(owner, store, id, Operation::Generate, limit)?),
            Protocol::Anthropic => Self::Anthropic(anthropic::request::Ingress::new(
                owner,
                store,
                id,
                Operation::Generate,
                limit,
            )?),
        })
    }
    pub fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> codec::Result<()> {
        match self {
            Self::Chat(ingress) => ingress.advance(owner, bytes),
            Self::Anthropic(ingress) => ingress.advance(owner, bytes),
        }
    }
    /// Upstream encode: the fence validates for the upstream destination, whose egress encodes.
    pub fn encode(&mut self, owner: &mut Writer, upstream: Protocol, stream: bool) -> codec::Result<ScratchLease> {
        match self {
            Self::Chat(ingress) => validate_encode(ingress, owner, upstream, stream),
            Self::Anthropic(ingress) => validate_encode(ingress, owner, upstream, stream),
        }
    }
}

fn validate_encode<I: RequestIngress>(
    ingress: &mut I,
    owner: &mut Writer,
    upstream: Protocol,
    stream: bool,
) -> codec::Result<ScratchLease> {
    match upstream {
        Protocol::Chat => {
            let ready = ingress.finish(owner, &chat::request::ChatCapability)?;
            chat::request::Egress { destination: "chat", upstream_stream: stream }.encode(&ready, owner)
        }
        Protocol::Anthropic => {
            let ready = ingress.finish(owner, &anthropic::request::AnthropicCapability)?;
            anthropic::request::Egress { destination: "anthropic", upstream_stream: stream }.encode(&ready, owner)
        }
    }
}

/// Upstream decode: the upstream protocol's response ingress.
pub enum ResponseIn {
    Chat(chat::response::Ingress),
    Anthropic(anthropic::response::Ingress),
}

impl ResponseIn {
    pub fn new(
        upstream: Protocol,
        owner: &mut Writer,
        store: &Store,
        operation: Operation,
        mode: BodyMode,
        limit: u64,
    ) -> codec::Result<Self> {
        let id = identity(Direction::Response)?;
        Ok(match upstream {
            Protocol::Chat => Self::Chat(chat::response::Ingress::new(owner, store, id, operation, mode, limit)?),
            Protocol::Anthropic => {
                Self::Anthropic(anthropic::response::Ingress::new(owner, id, operation, mode, limit)?)
            }
        })
    }
    pub fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> codec::Result<()> {
        match self {
            Self::Chat(ingress) => ingress.advance(owner, bytes),
            Self::Anthropic(ingress) => ingress.advance(owner, bytes),
        }
    }
    pub fn finish(&mut self, owner: &mut Writer) -> codec::Result<()> {
        match self {
            Self::Chat(ingress) => ingress.finish(owner),
            Self::Anthropic(ingress) => ingress.finish(owner),
        }
    }
}

/// Client encode: the client protocol's response egress.
pub enum ResponseOut {
    Chat(chat::egress::Egress),
    Anthropic(anthropic::egress::Egress),
}

impl ResponseOut {
    pub fn new(
        client: Protocol,
        owner: &mut Writer,
        operation: Operation,
        mode: BodyMode,
        include_usage: bool,
    ) -> codec::Result<Self> {
        let id = identity(Direction::Response)?;
        Ok(match client {
            Protocol::Chat => {
                Self::Chat(chat::egress::Egress::new(owner, id, operation, mode, include_usage, |_| {})?)
            }
            Protocol::Anthropic => {
                Self::Anthropic(anthropic::egress::Egress::new(owner, id, operation, mode, |_| {})?)
            }
        })
    }
    pub fn advance(
        &mut self,
        owner: &mut Writer,
        input: Lease,
        output: impl FnMut(ScratchLease) -> codec::Result<()>,
    ) -> codec::Result<()> {
        match self {
            Self::Chat(egress) => egress.advance(owner, input, output),
            Self::Anthropic(egress) => egress.advance(owner, input, output),
        }
    }
    pub fn finish(&mut self) -> codec::Result<()> {
        match self {
            Self::Chat(egress) => egress.finish(),
            Self::Anthropic(egress) => egress.finish(),
        }
    }
}
