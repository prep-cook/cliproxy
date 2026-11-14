//! Boundary codecs for the S1 internal stream. No provider execution or pair dispatch.
//!
//! # Protocol-worker contract
//!
//! Each protocol implements four independent roles: [`RequestIngress`],
//! [`RequestEgress`], [`ResponseIngress`] and [`ResponseEgress`]. Buffered JSON is
//! a body encoding, not a separate semantic pipeline. [`Operation`] includes
//! generation, count and compact; a protocol without an operation rejects it.
//! Count results never become generation usage, and compact is never a summary
//! substitute. Keep the old pair registry untouched until integration cutover.
//!
//! Ingress consumes leased mapped spans through [`source::Source`]. Scanners keep
//! coordinates and bounded metadata, not body-sized strings or JSON values.
//! Unknown fields retain their source positions, nesting, scalar types and order;
//! unknown SSE records retain all fields. A wire `x_ck` is provenance, never a
//! trusted envelope or media registration. Protocol modules emit the grammar in
//! `docs/INTERNAL-STREAM.md`; only a trusted internal reader interprets envelopes.
//! Use [`event::Emitter`] for canonical publication: it owns trusted identity,
//! sequence, request-end fencing and terminal state. Protocol codecs own payload
//! schema and choice/item/tool/media lifecycle validation.
//!
//! Request ingress validates complete syntax and protocol schema before asking
//! the destination [`Capability`] validator to inspect **every** mapped semantic:
//! model/operation, roles/content, tool declarations/choice/history linkage,
//! replay, cache directives, sampling/limits, output schema/modalities, session
//! controls and provider parameters. Unknown provenance alone is not a rejection.
//! [`ValidatedRequest`] is the execution fence: upstream request encoding accepts
//! no unfinished request. A failed fence emits no `request.end` or success.
//!
//! Response ingress splits choices and parts in source order, keeps independent
//! lifecycle state, forwards tool argument fragments immediately and publishes
//! only complete SSE/JSON records. Usage snapshots replace supplied fields;
//! missing/null counts are not zero. Only canonical `x_ck.usage` is accounting
//! authority. Final snapshots reconcile rather than replay content. EOF is not
//! success: protocol completion plus closed choices/tools/media is required.
//!
//! Response egress alone projects destination losses and decides deliverability.
//! Suppressed metadata produces no bytes. Unknown data reaches this boundary;
//! emit it at its native location/carrier or report a content-free diagnostic.
//! Buffered output and final snapshots read retained positions, never accumulate
//! payloads on the heap. Encoded output uses [`encode::publish`] on mapped storage.
//! No protocol worker edits shared modules/manifests without the framework owner.
//!
//! # Storage integration
//!
//! Sources and scanners bind to one writer, including empty inputs. Raw input
//! and external encoded output use unpublished scratch leases; canonical events
//! alone enter the publication log. All metadata capacity reserves permits from
//! that writer's quota before allocation. Scratch drops independently; never
//! release a response prefix to reclaim parser input.

pub mod chat;
pub mod document;
pub mod encode;
pub mod event;
pub mod internal;
pub mod json;
pub mod media;
pub mod provenance;
pub mod source;
pub mod sse;
pub(crate) mod table;
pub mod usage;

use ck_stream::{ScratchLease, Writer};

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Content-free failures. Boundary owners map client syntax errors to 400,
/// client size errors to 413, upstream syntax errors to terminal upstream errors,
/// and allocation/quota failures to resource errors without provider failover.
#[derive(Debug)]
pub enum Error {
    Storage(ck_stream::Error),
    Position,
    EventTooLarge,
    Limit,
    Utf8,
    Json,
    DuplicateKey,
    Truncated,
    Closed,
    Schema,
    Unsupported,
    Destination,
}

impl From<ck_stream::Error> for Error {
    fn from(error: ck_stream::Error) -> Self {
        Self::Storage(error)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Storage(_) => "codec storage failure",
            Self::Position => "invalid codec position",
            Self::EventTooLarge => "boundary event exceeds its byte limit",
            Self::Limit => "codec limit exceeded",
            Self::Utf8 => "invalid textual UTF-8",
            Self::Json => "invalid JSON",
            Self::DuplicateKey => "duplicate JSON key",
            Self::Truncated => "incomplete boundary input",
            Self::Closed => "codec is closed",
            Self::Schema => "invalid protocol schema",
            Self::Unsupported => "unsupported request semantics",
            Self::Destination => "request validated for another destination",
        })
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    Generate,
    CountTokens,
    Compact,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BodyMode {
    Buffered,
    Streaming,
}

/// The normalized logical request, addressed through mapped JSON positions.
/// Authentication, transport secrets and routing credentials never belong here.
#[derive(Clone, Copy)]
pub struct RequestView<'a> {
    pub document: &'a document::Document,
    pub media: &'a media::Registry,
    pub operation: Operation,
}

/// Destination-specific complete-request validation, not a source allowlist.
pub trait Capability {
    /// Unique key for the destination capability policy; distinct policies must
    /// not share a key, even when they use the same wire protocol.
    fn destination(&self) -> &'static str;
    fn validate(&self, request: RequestView<'_>) -> Result<()>;
}

/// Constructed only after complete syntax and destination capability validation.
/// This token authorizes encoding, not network execution or authentication.
pub struct ValidatedRequest<'a> {
    view: RequestView<'a>,
    destination: &'static str,
}
impl<'a> ValidatedRequest<'a> {
    pub fn view(&self) -> RequestView<'a> {
        self.view
    }
    pub fn for_destination(&self, destination: &str) -> Result<RequestView<'a>> {
        if self.destination != destination {
            return Err(Error::Destination);
        }
        Ok(self.view)
    }
}

/// A failed or completed fence cannot be retried with a weaker destination.
#[derive(Default)]
pub struct RequestFence {
    closed: bool,
}
impl RequestFence {
    pub fn end<'a>(&mut self, request: RequestView<'a>, destination: &impl Capability) -> Result<ValidatedRequest<'a>> {
        if self.closed {
            return Err(Error::Closed);
        }
        self.closed = true;
        request.media.validate_document(request.document)?;
        request.document.check_live()?;
        destination.validate(request)?;
        Ok(ValidatedRequest {
            view: request,
            destination: destination.destination(),
        })
    }
}

pub trait RequestIngress {
    /// Deadline for the oldest ready payload still awaiting publication, or none.
    /// The owner must schedule `flush` even while input is idle, and recheck this
    /// after every advance/flush. Later input must not postpone an existing deadline.
    fn flush_deadline(&self) -> Option<std::time::Instant>;

    /// Publish ready payload without completing the request or an open media object.
    /// May be called before the deadline; empty advance has the same flush contract.
    fn flush(&mut self, owner: &mut Writer) -> Result<()> {
        self.advance(owner, &[])
    }

    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> Result<()>;
    fn finish<'a>(&'a mut self, owner: &mut Writer, destination: &impl Capability) -> Result<ValidatedRequest<'a>>;
}
pub trait RequestEgress {
    fn encode(&self, request: &ValidatedRequest<'_>, output: &mut Writer) -> Result<ScratchLease>;
}
pub trait ResponseIngress {
    /// Deadline for the oldest ready payload still awaiting publication, under the
    /// `RequestIngress::flush_deadline` contract. A codec that publishes every read holds none.
    fn flush_deadline(&self) -> Option<std::time::Instant> {
        None
    }

    /// Publish ready payload without completing the response or an open media object.
    /// May be called before the deadline; empty advance has the same flush contract.
    fn flush(&mut self, owner: &mut Writer) -> Result<()> {
        self.advance(owner, &[])
    }

    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> Result<()>;
    fn finish(&mut self, owner: &mut Writer) -> Result<()>;
}
pub trait ResponseEgress {
    fn advance(
        &mut self,
        owner: &mut Writer,
        input: ck_stream::Lease,
        output: impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}
