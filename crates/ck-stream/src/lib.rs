//! Shared mmap stream bytes, position-based publication, reader leases and retention.
//! No workspace dependencies; no HTTP, credentials, provider dispatch or durable state.
//! Codecs own record syntax. This crate publishes only fully written reservations.

mod cache;
mod grammar;
mod limits;
mod pool;
mod position;
mod publication;
mod request;

pub use cache::{Clock, MaintenanceFailures, ManualClock, MonotonicClock, Store, StoreStats};
pub use grammar::{ByteSpan, EventView};
pub use limits::{Config, Limits, MAX_POOL_BYTES, MAX_STREAM_FRAMES};
pub use position::{Position, Record};
pub use publication::{Lease, MetadataPermit, Read, Reader, Reservation, ScratchLease, ScratchReservation, Writer};
pub use request::RequestFieldView;

pub const FRAME_BYTES: usize = 50_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Terminal {
    Done,
    Cancelled,
    Shutdown,
    UpstreamError,
    StorageExhausted,
    Gone,
    /// The writer was dropped without a terminal, e.g. its producer task aborted or panicked.
    ProducerLost,
}

impl Terminal {
    /// Code of the `error` event for codes INTERNAL-STREAM.md §3.3 registers. An upstream
    /// error carries the producer's own sanitized code, so it has none here.
    pub fn error_code(self) -> Option<&'static str> {
        match self {
            Self::StorageExhausted => Some("stream.storage_exhausted"),
            Self::Gone => Some("stream.gone"),
            Self::ProducerLost => Some("stream.producer_lost"),
            Self::Done | Self::Cancelled | Self::Shutdown | Self::UpstreamError => None,
        }
    }

    /// `reason` of the `cancelled` event (§3.3); cancellation is not an error code.
    pub fn cancel_reason(self) -> Option<&'static str> {
        match self {
            Self::Cancelled => Some("client_cancel"),
            Self::Shutdown => Some("shutdown"),
            Self::Done | Self::UpstreamError | Self::StorageExhausted | Self::Gone | Self::ProducerLost => None,
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Gone,
    StorageExhausted,
    InvalidPosition,
    InvalidRecord,
    IncompleteRecord,
    Closed,
    Io(std::io::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Gone => f.write_str("stream.gone"),
            Self::StorageExhausted => f.write_str("stream.storage_exhausted"),
            Self::InvalidPosition => f.write_str("invalid stream position"),
            Self::InvalidRecord => f.write_str("invalid record length or alignment"),
            Self::IncompleteRecord => f.write_str("record reservation is not completely written"),
            Self::Closed => f.write_str("stream is terminal"),
            Self::Io(error) => write!(f, "stream storage I/O error: {error}"),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}
