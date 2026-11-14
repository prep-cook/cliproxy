use std::{
    sync::{Arc, atomic::Ordering},
    task::Poll,
};

use super::{READER_BYTES, Stream};
use crate::{Error, FRAME_BYTES, Position, Record, Terminal};

pub enum Read {
    Record(Lease),
    Terminal(Terminal),
}

/// Independent complete-record cursor. A terminal is delivered once, followed by EOF.
pub struct Reader {
    stream: Arc<Stream>,
    next: u64,
    available: u64,
    committed: u64,
    terminal_seen: bool,
}

impl Reader {
    pub(crate) fn new(stream: Arc<Stream>, after: u64) -> Result<Self, Error> {
        let result = stream.retention.reading(&stream, |state| {
            if stream.invalidated.load(Ordering::Acquire) || after < state.base_sequence {
                return (Err(Error::Gone), false);
            }
            if after > state.next_sequence {
                return (Err(Error::InvalidPosition), false);
            }
            if READER_BYTES > stream.limits.max_metadata_bytes.saturating_sub(Stream::metadata(state)) {
                return (Err(Error::StorageExhausted), false);
            }
            state.readers += 1;
            (Ok(()), true)
        });
        if matches!(result, Err(Error::StorageExhausted)) {
            stream.finish(Terminal::StorageExhausted);
        }
        result?;
        Ok(Self {
            stream,
            next: after,
            available: 0,
            committed: 0,
            terminal_seen: false,
        })
    }

    pub fn try_next(&mut self) -> Poll<Option<Read>> {
        if self.terminal_seen {
            return Poll::Ready(None);
        }
        let stream = &self.stream;
        // Decide under the lock; build the lease after it, so the critical section moves
        // only a record and its frames.
        let step = stream.retention.reading(stream, |state| {
            if stream.invalidated.load(Ordering::Acquire) || self.next < state.base_sequence {
                return (Step::Terminal(Terminal::Gone), false);
            }
            // Cache only a prefix acquired from the mapped publication header under this lock.
            if self.next >= self.available
                && let Some(slot) = state.slot
            {
                self.committed = stream.pool.header(slot).committed.load(Ordering::Acquire);
                self.available = state.next_sequence;
            }
            let index = state.records.partition_point(|run| run.next_sequence <= self.next);
            if let Some(record) = state
                .records
                .get(index)
                .and_then(|run| run.record(stream.id, self.next))
                && record.end.byte(stream.id).is_ok_and(|end| end <= self.committed)
            {
                return match stream.pin(state, record) {
                    Ok(pinned) => (Step::Record(record, pinned), true),
                    Err(terminal) => (Step::Terminal(terminal), false),
                };
            }
            match state.terminal {
                Some(terminal) => (Step::Terminal(terminal), false),
                None => (Step::Pending, true),
            }
        });
        match step {
            Step::Record(record, pinned) => {
                self.next += 1;
                Poll::Ready(Some(Read::Record(Lease {
                    stream: stream.clone(),
                    record,
                    pinned,
                    acknowledged: false,
                })))
            }
            Step::Terminal(terminal) => {
                // A lease the stream's storage cannot pin fails the stream like any storage
                // failure. A stream that already ended keeps its terminal and its count.
                if terminal == Terminal::StorageExhausted {
                    stream.finish(terminal);
                }
                self.terminal_seen = true;
                Poll::Ready(Some(Read::Terminal(terminal)))
            }
            Step::Pending => Poll::Pending,
        }
    }

    pub async fn next(&mut self) -> Option<Read> {
        if self.terminal_seen {
            return None;
        }
        if self.next < self.available
            && let Poll::Ready(value) = self.try_next()
        {
            return value;
        }
        loop {
            let stream = self.stream.clone();
            let notified = stream.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let _waiting = stream.notify.waiting();
            if let Poll::Ready(value) = self.try_next() {
                return value;
            }
            notified.await;
            if let Poll::Ready(value) = self.try_next() {
                return value;
            }
        }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.stream.retention.reading(&self.stream, |state| {
            state.readers -= 1;
            ((), true)
        });
    }
}

/// What one locked read step found.
enum Step {
    Record(Record, Pinned),
    Terminal(Terminal),
    Pending,
}

/// Pool frames a lease references, in record order. Most records fit one frame.
pub(super) enum Pinned {
    One(u32),
    Many(Box<[u32]>),
}

impl Pinned {
    pub(super) fn frames(&self) -> &[u32] {
        match self {
            Self::One(frame) => std::slice::from_ref(frame),
            Self::Many(frames) => frames,
        }
    }
}

/// Pins immutable published frames, including after invalidation. New access then returns Gone.
pub struct Lease {
    stream: Arc<Stream>,
    record: Record,
    pinned: Pinned,
    acknowledged: bool,
}

impl Lease {
    pub fn record(&self) -> Record {
        self.record
    }

    /// Borrow one frame span at a checked position. The borrow cannot outlive this lease.
    pub fn chunk(&self, position: Position) -> Result<&[u8], Error> {
        let byte = position.byte(self.stream.id)?;
        let begin = self.record.start.byte(self.stream.id)?;
        let end = self.record.end.byte(self.stream.id)?;
        if byte < begin || byte >= end {
            return Err(Error::InvalidPosition);
        }
        if self.stream.invalidated.load(Ordering::Acquire) {
            return Err(Error::Gone);
        }
        let frame = self.pinned.frames()[(position.frame - self.record.start.frame) as usize];
        let len = (end - byte).min((FRAME_BYTES - position.offset as usize) as u64) as usize;
        // SAFETY: this lease pins all record frames until Drop. Publication made this span
        // immutable before lease registration; the pool Arc outlives the returned borrow.
        Ok(unsafe { self.stream.pool.slice(frame, position.offset as usize, len) })
    }

    pub fn visit(&self, mut visitor: impl FnMut(&[u8])) -> Result<(), Error> {
        // The lease's record and frames were validated when it was pinned, so walk them
        // directly; invalidation still ends the walk before each chunk, as `chunk` would.
        let mut offset = self.record.start.offset as usize;
        let mut remaining = self.record.len() as usize;
        for &frame in self.pinned.frames() {
            if self.stream.invalidated.load(Ordering::Acquire) {
                return Err(Error::Gone);
            }
            let len = remaining.min(FRAME_BYTES - offset);
            // SAFETY: as in `chunk`; the span lies within this pinned frame and the record.
            visitor(unsafe { self.stream.pool.slice(frame, offset, len) });
            remaining -= len;
            offset = 0;
        }
        Ok(())
    }

    pub fn acknowledge(mut self) {
        self.acknowledged = true;
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if self.acknowledged {
            self.stream.touch(self.stream.retention.now());
        }
        // Releasing a reference only raises the stream's reclaimable count, so eviction's
        // counted gain stays valid without the state lock or the retention fence.
        for &frame in self.pinned.frames() {
            if self.stream.pool.release(frame) {
                // The stream had detached this frame; its last lease freed it.
                self.stream.detached.fetch_sub(1, Ordering::AcqRel);
            }
        }
        // Single-frame leases, the common case, own no list and take no lock.
        if let Pinned::Many(frames) = &mut self.pinned {
            let bytes = std::mem::size_of_val::<[u32]>(frames);
            drop(std::mem::take(frames));
            self.stream.state.lock().external_metadata -= bytes;
        }
    }
}
