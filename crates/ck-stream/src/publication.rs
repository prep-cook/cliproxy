use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use parking_lot::Mutex;

use crate::{Error, FRAME_BYTES, Limits, Position, Record, Terminal, cache::Retention, pool::Pool};

mod reader;
mod scratch;
mod wakeup;
mod writer;
use reader::Pinned;
pub use reader::{Lease, Read, Reader};
pub use scratch::{MetadataPermit, ScratchLease, ScratchReservation};
use wakeup::Wakeup;
pub use writer::{Reservation, Writer};

pub(crate) const READER_BYTES: usize = std::mem::size_of::<Reader>();

/// A stream's reference to a pool frame; readers' leases hold their own references.
pub(crate) struct Frame {
    pool: Option<u32>,
}

// A run represents exact record boundaries, not a byte range including padding.
#[derive(Clone, Copy)]
pub(crate) struct Index {
    start: u64,
    sequence: u64,
    next_sequence: u64,
    len: u64,
    stride: u64,
    kind: u32,
    correlation: u32,
}

impl Index {
    fn extends(&self, next: &Self) -> bool {
        self.next_sequence == next.sequence
            && self.len == next.len
            && self.stride == next.stride
            && self.kind == next.kind
            && self.correlation == next.correlation
            && (self.next_sequence - self.sequence)
                .checked_mul(self.stride)
                .and_then(|offset| self.start.checked_add(offset))
                == Some(next.start)
    }

    fn record(self, generation: u64, sequence: u64) -> Option<Record> {
        if !(self.sequence..self.next_sequence).contains(&sequence) {
            return None;
        }
        let start = (sequence - self.sequence)
            .checked_mul(self.stride)?
            .checked_add(self.start)?;
        let end = start.checked_add(self.len)?;
        Some(Record {
            sequence,
            start: Position::at(generation, start),
            end: Position::at(generation, end),
            kind: self.kind,
            correlation: self.correlation,
        })
    }
}

/// The fewest entries a metadata table grows by, so small tables do not reallocate per push.
const MIN_GROWTH: usize = 4;

/// Grows `entries` to hold `needed` within `budget` bytes. A step adds a quarter of `needed`,
/// at least [`MIN_GROWTH`]: each step multiplies the capacity by at least 1.25, so pushes stay
/// amortized O(1), while the unused tail stays within a quarter of the use rather than the
/// half of a doubling step, leaving that headroom to the stream's other metadata.
fn reserve_metadata<T>(entries: &mut VecDeque<T>, needed: usize, budget: usize) -> Result<(), Error> {
    if needed <= entries.capacity() {
        return Ok(());
    }
    let maximum = budget / std::mem::size_of::<T>();
    if needed > maximum {
        return Err(Error::StorageExhausted);
    }
    let capacity = needed.saturating_add((needed / 4).max(MIN_GROWTH)).min(maximum);
    entries
        .try_reserve_exact(capacity - entries.len())
        .map_err(|_| Error::StorageExhausted)
}

pub(crate) struct State {
    frames: VecDeque<Frame>,
    base_frame: u64,
    records: VecDeque<Index>,
    base_sequence: u64,
    next_sequence: u64,
    end: u64,
    terminal: Option<Terminal>,
    pub(crate) readers: usize,
    scratch_frames: usize,
    /// The scratch frame small leases pack into, and its first unleased byte. Only leases
    /// reference it; the lease drop that frees it clears this under the same lock.
    scratch_open: Option<(u32, usize)>,
    external_metadata: usize,
    // Taken at invalidation, so an evicted stream frees its header before its last handle drops.
    slot: Option<u32>,
    /// Frames charged when the stream ended with `StorageExhausted`.
    failure_frames: Option<u32>,
}

impl State {
    /// Header slots that invalidation would return now.
    pub(crate) fn slot_count(_: &Stream, state: &Self) -> usize {
        usize::from(state.slot.is_some())
    }
}

#[repr(align(128))]
pub(crate) struct Stream {
    pub id: u64,
    pool: Arc<Pool>,
    retention: Arc<Retention>,
    limits: Limits,
    pub state: Mutex<State>,
    invalidated: AtomicBool,
    /// Clock milliseconds of the last read. Acknowledgements raise it without the state lock.
    last_read: AtomicU64,
    /// Frames the stream gave up while leases still pin them; they still count as live.
    detached: AtomicU64,
    notify: Wakeup,
    stopped: Wakeup,
}

impl Stream {
    pub(crate) fn new(
        id: u64,
        slot: u32,
        pool: Arc<Pool>,
        retention: Arc<Retention>,
        limits: Limits,
        now: Duration,
    ) -> Self {
        Self {
            id,
            pool,
            retention,
            limits,
            notify: Wakeup::new(),
            stopped: Wakeup::new(),
            invalidated: AtomicBool::new(false),
            last_read: AtomicU64::new(millis(now)),
            detached: AtomicU64::new(0),
            state: Mutex::new(State {
                frames: VecDeque::new(),
                base_frame: 0,
                records: VecDeque::new(),
                base_sequence: 0,
                next_sequence: 0,
                end: 0,
                terminal: None,
                readers: 0,
                scratch_frames: 0,
                scratch_open: None,
                external_metadata: 0,
                slot: Some(slot),
                failure_frames: None,
            }),
        }
    }

    pub(crate) fn is_invalidated(&self) -> bool {
        self.invalidated.load(Ordering::Acquire)
    }

    /// Frames reader leases keep pinned: owned ones with another reference, and detached ones
    /// only leases still hold. Every record spans a frame, so this is zero exactly when no lease
    /// on the stream is outstanding. Counted here rather than per lease: an atomic update on
    /// every lease drop cost 5% of small-request throughput.
    pub(crate) fn leased_frames(&self, state: &State) -> u64 {
        let owned = state
            .frames
            .iter()
            .filter(|frame| frame.pool.is_some_and(|index| self.pool.references(index) > 1))
            .count();
        owned as u64 + self.detached.load(Ordering::Acquire)
    }

    pub(crate) fn last_read(&self) -> Duration {
        Duration::from_millis(self.last_read.load(Ordering::Relaxed))
    }

    /// Raise the last-read time. An unlocked acknowledgement may store a sample older than a
    /// read that finished meanwhile, so the time never moves backwards. The time is kept in
    /// whole milliseconds, so reads within one millisecond skip the locked update.
    pub(crate) fn touch(&self, now: Duration) {
        let now = millis(now);
        if now > self.last_read.load(Ordering::Relaxed) {
            self.last_read.fetch_max(now, Ordering::Relaxed);
        }
    }

    /// Frames that invalidation would return to the pool now: owned and unleased.
    pub(crate) fn reclaimable(&self, state: &State) -> usize {
        state
            .frames
            .iter()
            .filter(|frame| frame.pool.is_some_and(|index| self.pool.references(index) == 1))
            .count()
    }

    /// Frames charged to the stream quota: owned, detached but still leased, and scratch.
    pub(super) fn live_frames(&self, state: &State) -> usize {
        state.frames.iter().filter(|frame| frame.pool.is_some()).count()
            + self.detached.load(Ordering::Acquire) as usize
            + state.scratch_frames
    }

    /// Reference every frame of a published record for a new lease, or name the terminal
    /// that ends the reader instead.
    fn pin(&self, state: &mut State, record: Record) -> Result<Pinned, Terminal> {
        let first = (record.start.frame - state.base_frame) as usize;
        let last = (last_frame(record) - state.base_frame) as usize;
        if first == last {
            let index = state.frames[first].pool.ok_or(Terminal::Gone)?;
            if self.pool.saturated(index) {
                return Err(Terminal::StorageExhausted);
            }
            self.pool.retain(index);
            return Ok(Pinned::One(index));
        }
        let frames = (first..=last).map(|index| state.frames[index].pool);
        for frame in frames.clone() {
            match frame {
                None => return Err(Terminal::Gone),
                Some(index) if self.pool.saturated(index) => return Err(Terminal::StorageExhausted),
                Some(_) => {}
            }
        }
        // The lease's frame list outlives its reader, so it is charged until the lease frees it.
        let bytes = (last - first + 1) * std::mem::size_of::<u32>();
        if bytes > self.limits.max_metadata_bytes.saturating_sub(Self::metadata(state)) {
            return Err(Terminal::StorageExhausted);
        }
        // The stream's own references keep every frame allocated while these are added.
        let frames: Box<[u32]> = frames.flatten().collect();
        frames.iter().for_each(|&index| self.pool.retain(index));
        state.external_metadata += bytes;
        Ok(Pinned::Many(frames))
    }

    /// Give up the stream's reference to a frame; leases keep it pinned until they end.
    fn detach(&self, frame: &mut Frame) {
        if let Some(index) = frame.pool.take() {
            // Count before releasing, so a racing last lease never decrements below zero.
            self.detached.fetch_add(1, Ordering::AcqRel);
            if self.pool.release(index) {
                self.detached.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    fn metadata(state: &State) -> usize {
        state.frames.capacity() * std::mem::size_of::<Frame>()
            + state.records.capacity() * std::mem::size_of::<Index>()
            + state.readers * READER_BYTES
            + state.external_metadata
    }

    fn write_reserved(&self, state: &State, start: Position, mut bytes: &[u8]) -> Result<(), Error> {
        let mut frame_index = (start.frame - state.base_frame) as usize;
        let mut offset = start.offset as usize;
        while !bytes.is_empty() {
            let len = bytes.len().min(FRAME_BYTES - offset);
            let frame = state.frames[frame_index].pool.ok_or(Error::Gone)?;
            // SAFETY: the sole Writer reserved this frame; the locked state prevents invalidation,
            // and this span is strictly after the immutable published end and within the frame.
            unsafe {
                self.pool.write(frame, offset, &bytes[..len]);
            }
            frame_index += 1;
            offset = 0;
            bytes = &bytes[len..];
        }
        Ok(())
    }

    fn publish(&self, state: &mut State, index: Index) {
        if let Some(last) = state.records.back_mut().filter(|last| last.extends(&index)) {
            last.next_sequence = index.next_sequence;
        } else {
            state.records.push_back(index);
        }
        state.next_sequence = index.next_sequence;
        state.end = index.start + index.len;
        // Callers publish only after checking invalidation under this lock, so the slot is held.
        if let Some(slot) = state.slot {
            self.pool.header(slot).committed.store(state.end, Ordering::Release);
        }
    }

    pub(crate) fn finish(&self, terminal: Terminal) {
        if terminal == Terminal::Gone {
            self.invalidate();
            return;
        }
        let mut state = self.state.lock();
        // Waiters registered after the first terminal see it when they recheck.
        if state.terminal.is_some() {
            return;
        }
        state.terminal = Some(terminal);
        if terminal == Terminal::StorageExhausted {
            // Every failing path rolls back its partial reservation before it gets here, so this
            // is what the stream still holds, by the formula its frame limit uses. Frame counts
            // are bounded by the pool's u32 capacity.
            let frames = self.live_frames(&state) as u32;
            state.failure_frames = Some(frames);
            self.retention.failures.record(Some(frames));
        }
        drop(state);
        self.notify.wake();
        self.stopped.wake();
    }

    pub(crate) fn invalidate(&self) {
        let (mut state, _fence) = self.retention.lock_stable(self);
        self.invalidate_locked(&mut state);
    }

    pub(crate) fn invalidate_locked(&self, state: &mut State) {
        self.invalidated.store(true, Ordering::Release);
        state.terminal = Some(Terminal::Gone);
        for frame in &mut state.frames {
            self.detach(frame);
        }
        state.records.clear();
        if let Some(slot) = state.slot.take() {
            self.pool.release_slot(slot);
        }
        self.notify.wake();
        self.stopped.wake();
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        let state = self.state.get_mut();
        for frame in &mut state.frames {
            if let Some(index) = frame.pool.take() {
                self.pool.release(index);
            }
        }
        if let Some(slot) = state.slot.take() {
            self.pool.release_slot(slot);
        }
    }
}

fn last_frame(record: Record) -> u64 {
    if record.end.offset == 0 {
        record.end.frame - 1
    } else {
        record.end.frame
    }
}

fn millis(time: Duration) -> u64 {
    u64::try_from(time.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pushes `count` entries one by one and returns every capacity a growth step chose.
    fn steps(count: usize, budget: usize) -> Vec<usize> {
        let mut entries = VecDeque::<u64>::new();
        let mut capacities = Vec::new();
        for value in 0..count as u64 {
            let before = entries.capacity();
            let needed = entries.len() + 1;
            reserve_metadata(&mut entries, needed, budget).unwrap();
            if entries.capacity() != before {
                capacities.push(entries.capacity());
            }
            entries.push_back(value);
        }
        capacities
    }

    #[test]
    fn metadata_tables_grow_by_a_quarter_with_a_floor() {
        let capacities = steps(100_000, usize::MAX);
        // Small tables take the floor rather than one entry per push.
        assert_eq!(capacities[..3], [5, 10, 15]);
        for pair in capacities.windows(2) {
            let (before, after) = (pair[0], pair[1]);
            // `needed` is `before + 1`: a quarter of it, never a doubling.
            assert_eq!(after, before + 1 + ((before + 1) / 4).max(MIN_GROWTH));
        }
        // Geometric growth: about log(100,000) / log(1.25) steps, so pushes stay O(1) amortized.
        assert!(capacities.len() <= 50, "{} steps", capacities.len());
    }

    #[test]
    fn metadata_tables_stop_at_the_budget() {
        let budget = 100 * std::mem::size_of::<u64>();
        assert_eq!(*steps(100, budget).last().unwrap(), 100);
        let mut entries = VecDeque::<u64>::new();
        assert!(matches!(
            reserve_metadata(&mut entries, 101, budget),
            Err(Error::StorageExhausted)
        ));
        assert_eq!(entries.capacity(), 0);
    }
}
