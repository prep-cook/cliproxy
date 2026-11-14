use std::{
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
    time::Duration,
};

use crate::{FRAME_BYTES, MaintenanceFailures, Store};

/// Flow metrics of one store (INTERNAL-STREAM.md §9.2). Counters are monotonic since the
/// store opened and are not persisted.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StoreStats {
    pub capacity_frames: u32,
    pub backed_frames: u32,
    /// Charged frames: retained, scratch, and detached but still leased.
    pub used_frames: u32,
    /// `used_frames` in bytes, not committed payload.
    pub cache_bytes: u64,
    pub cached_streams: usize,
    /// Time since the least recent read of a cached, valid stream; a stream with an attached
    /// reader counts as read now, as expiry treats it. `None` when no such stream is cached.
    pub oldest_read_age: Option<Duration>,
    /// Streams invalidated to free frames, tmpfs space or header slots.
    pub evictions: u64,
    /// Streams invalidated because their last read is older than `cache_ttl`.
    pub expiries: u64,
    /// Streams ended with `StorageExhausted`, plus admissions refused for want of a header slot.
    pub allocation_failures: u64,
    /// Frames charged to the most recent stream that ended with `StorageExhausted`. Concurrent
    /// failures race; `Writer::storage_failure_frames` has each stream's own value.
    pub last_storage_exhausted_frames: Option<u32>,
    /// Frames that reader leases keep pinned, including on streams that left the cache. Zero
    /// exactly when no reader lease is outstanding; a lease spanning several frames counts
    /// each, and leases sharing a frame count it once.
    pub leased_frames: u64,
    pub maintenance: MaintenanceFailures,
}

/// Storage failures since the store opened. Only failure paths write it.
#[repr(align(128))]
pub(crate) struct Failures {
    count: AtomicU64,
    /// `u32::MAX` until the first stream fails.
    last_frames: AtomicU32,
}

impl Default for Failures {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            last_frames: AtomicU32::new(u32::MAX),
        }
    }
}

impl Failures {
    /// Count one failure; `frames` is the failed stream's charge, absent for a refused admission.
    pub(crate) fn record(&self, frames: Option<u32>) {
        self.count.fetch_add(1, Ordering::Relaxed);
        if let Some(frames) = frames {
            self.last_frames.store(frames, Ordering::Relaxed);
        }
    }
}

impl Store {
    /// Read the flow metrics. The cache lock is held only to copy the cached streams' handles
    /// (at most `header_slots`, plus streams whose writers are ending them with `Gone`) and to
    /// swap out the retired-stream list. Each stream's state is then locked on its own, so
    /// admission and attachment never wait behind a busy stream; concurrent scrapes wait for
    /// each other. Each stream costs one walk of its frame table: call this from operator
    /// scrapes, never per request.
    pub fn stats(&self) -> StoreStats {
        let inner = &self.inner;
        let _scrape = inner.scrape.lock();
        let now = inner.retention.now();
        let mut cache = inner.cache.lock();
        let cached: Vec<_> = cache.streams.values().map(|entry| entry.stream.clone()).collect();
        let mut orphans = std::mem::take(&mut cache.orphans);
        let (evictions, expiries) = (cache.evictions, cache.expiries);
        drop(cache);
        // A stream retires only after leaving the cache, so the two lists share no stream.
        let mut oldest: Option<Duration> = None;
        let mut leased_frames = 0;
        for stream in &cached {
            let state = stream.state.lock();
            leased_frames += stream.leased_frames(&state);
            if stream.is_invalidated() {
                continue;
            }
            let read = if state.readers > 0 { now } else { stream.last_read() };
            oldest = Some(oldest.map_or(read, |oldest| oldest.min(read)));
        }
        orphans.retain(|orphan| {
            let Some(stream) = orphan.upgrade() else {
                return false;
            };
            leased_frames += stream.leased_frames(&stream.state.lock());
            true
        });
        inner.cache.lock().restore(orphans);
        let failures = &inner.retention.failures;
        let last_frames = failures.last_frames.load(Ordering::Relaxed);
        let used_frames = inner.pool.used();
        StoreStats {
            capacity_frames: inner.pool.capacity(),
            backed_frames: inner.pool.backed_frames(),
            used_frames,
            cache_bytes: u64::from(used_frames) * FRAME_BYTES as u64,
            cached_streams: cached.len(),
            oldest_read_age: oldest.map(|read| now.saturating_sub(read)),
            evictions,
            expiries,
            allocation_failures: failures.count.load(Ordering::Relaxed),
            last_storage_exhausted_frames: (last_frames != u32::MAX).then_some(last_frames),
            leased_frames,
            maintenance: self.maintenance_failures(),
        }
    }
}
