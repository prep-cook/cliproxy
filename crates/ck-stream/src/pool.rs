//! Process-private frame storage. Publication and lease ownership belong to streams.

use crate::FRAME_BYTES;
use std::cell::Cell;
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering, fence};
use std::sync::{Mutex, MutexGuard};

mod os;
#[cfg(test)]
mod tests;

const EXTENT: u32 = 21;
const WARM: usize = 8;
static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static THREAD_SHARD: Cell<Option<usize>> = const { Cell::new(None) };
}

#[repr(C, align(128))]
pub(crate) struct Header {
    pub committed: AtomicU64,
}

#[repr(align(128))]
struct Padded<T>(T);

/// Per-frame counters, one cache line per frame. Leases on different threads update the
/// counts of neighbouring frames; packed, every update moved a shared line between cores.
#[repr(align(64))]
struct FrameState {
    /// References: the owner's plus one per reader lease. Zero means free.
    refs: AtomicU32,
    /// Bytes from the frame's start whose pages are populated, so writing them cannot
    /// fault on tmpfs exhaustion. Only the frame's current owner changes it.
    ready: AtomicU32,
}

struct FreeList {
    frames: Vec<u32>,
    // The bottom of a LIFO stack below this mark was never popped this interval.
    low_water: usize,
}

impl FreeList {
    fn pop(&mut self) -> Option<u32> {
        let frame = self.frames.pop();
        self.low_water = self.low_water.min(self.frames.len());
        frame
    }
}

/// One worker's free list. `len` mirrors its length so that allocation can pass over
/// empty lists without locking them; every change goes through `ShardGuard`.
struct Shard {
    free: Mutex<FreeList>,
    len: AtomicUsize,
}

impl Shard {
    fn new() -> Self {
        Self {
            free: Mutex::new(FreeList {
                frames: Vec::new(),
                low_water: 0,
            }),
            len: AtomicUsize::new(0),
        }
    }

    fn lock(&self) -> ShardGuard<'_> {
        ShardGuard {
            free: lock(&self.free),
            len: &self.len,
        }
    }

    /// A stale answer costs one lock, or one cold frame taken while a warm one waited.
    fn occupied(&self) -> bool {
        self.len.load(Ordering::Relaxed) != 0
    }
}

struct ShardGuard<'a> {
    free: MutexGuard<'a, FreeList>,
    len: &'a AtomicUsize,
}

impl Deref for ShardGuard<'_> {
    type Target = FreeList;
    fn deref(&self) -> &FreeList {
        &self.free
    }
}

impl DerefMut for ShardGuard<'_> {
    fn deref_mut(&mut self) -> &mut FreeList {
        &mut self.free
    }
}

impl Drop for ShardGuard<'_> {
    fn drop(&mut self) {
        self.len.store(self.free.frames.len(), Ordering::Relaxed);
    }
}

struct Slow {
    backed: u32,
    cold: Vec<u32>,
    candidates: Vec<u32>,
}

struct Mapping {
    base: NonNull<u8>,
    len: usize,
}

impl Mapping {
    /// Map `len` bytes of the file, populate the first `populated` and initialize the
    /// header slots at the start.
    fn new(file: &os::Backing, len: usize, populated: usize, slots: u32) -> io::Result<Self> {
        // SAFETY: fd owns a writable regular file. The entire VA range is reserved
        // once; only fallocate-backed extents are accessed before later growth.
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.fd(),
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let mapping = Mapping {
            base: NonNull::new(base.cast()).ok_or_else(invalid)?,
            len,
        };
        os::populate(mapping.base.as_ptr(), populated)?;
        // SAFETY: fallocate/populate succeeded; the page-aligned header is private
        // during construction. Initialize atomic objects, not references to zero bytes.
        unsafe {
            for slot in 0..slots {
                mapping
                    .base
                    .as_ptr()
                    .add(slot as usize * 128)
                    .cast::<Header>()
                    .write(Header {
                        committed: AtomicU64::new(0),
                    });
            }
        }
        Ok(mapping)
    }
}

/// Per-frame states in private anonymous memory, which stays unbacked until a frame is
/// first used; a heap allocation with this alignment would be written zero in full.
struct FrameStates {
    mapping: Mapping,
    len: usize,
}

impl FrameStates {
    /// States for `len` frames with the first `populated` marked wholly populated.
    fn new(len: usize, populated: u32) -> io::Result<Self> {
        let bytes = len.checked_mul(std::mem::size_of::<FrameState>()).ok_or_else(invalid)?;
        // SAFETY: a fresh private anonymous mapping aliases nothing.
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let states = Self {
            mapping: Mapping {
                base: NonNull::new(base.cast()).ok_or_else(invalid)?,
                len: bytes,
            },
            len,
        };
        for state in &states[..populated as usize] {
            state.ready.store(FRAME_BYTES as u32, Ordering::Relaxed);
        }
        Ok(states)
    }
}

impl Deref for FrameStates {
    type Target = [FrameState];
    fn deref(&self) -> &[FrameState] {
        // SAFETY: the page-aligned mapping holds `len` states for its whole life. Zero
        // bytes are valid atomics, and all access goes through them.
        unsafe { std::slice::from_raw_parts(self.mapping.base.as_ptr().cast(), self.len) }
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: this object owns the sole mapping; Pool's last owner has ended.
        unsafe { libc::munmap(self.base.as_ptr().cast(), self.len) };
    }
}

/// Owners retain this pool (normally via Arc) until all mapped leases end.
#[repr(align(128))]
pub(crate) struct Pool {
    mapping: Mapping,
    file: os::Backing,
    header_bytes: usize,
    page: usize,
    capacity: u32,
    reserve_frames: u32,
    slots: u32,
    free_slots: Mutex<Vec<u32>>,
    states: FrameStates,
    shards: Box<[Padded<Shard>]>,
    slow: Padded<Mutex<Slow>>,
    used: Padded<AtomicU32>,
    backed: Padded<AtomicU32>,
}

// SAFETY: mapping address/lifetime never change. Locks exclusively own free frames;
// streams enforce exclusive unpublished writes and pin immutable spans with leases.
// All shared mapped metadata consists of initialized atomics.
unsafe impl Send for Pool {}
// SAFETY: the same ownership and publication contracts apply across shared references.
unsafe impl Sync for Pool {}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "invalid pool capacity or layout")
}

fn round_up(value: usize, page: usize) -> io::Result<usize> {
    value.checked_add(page - 1).map(|n| n / page * page).ok_or_else(invalid)
}

impl Pool {
    pub(crate) fn open(
        directory: &Path,
        frames: u32,
        slots: u32,
        workers: usize,
        reserve_frames: u32,
    ) -> io::Result<Self> {
        if frames == 0 || slots == 0 || workers == 0 || reserve_frames > frames {
            return Err(invalid());
        }
        // SAFETY: sysconf has no pointer arguments or process-state mutation.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page <= 0 {
            return Err(io::Error::last_os_error());
        }
        let page = page as usize;
        let header_bytes = round_up((slots as usize).checked_mul(128).ok_or_else(invalid)?, page)?;
        let len = round_up(
            (frames as usize)
                .checked_mul(FRAME_BYTES)
                .and_then(|n| n.checked_add(header_bytes))
                .ok_or_else(invalid)?,
            page,
        )?;
        if len > isize::MAX as usize || len > i64::MAX as usize {
            return Err(invalid());
        }
        let file = os::Backing::open(directory)?;
        let initial = frames.min(EXTENT).max(reserve_frames);
        let initial_bytes = round_up(header_bytes + initial as usize * FRAME_BYTES, page)?;
        file.grow(0, initial_bytes)?;
        let mapping = Mapping::new(&file, len, initial_bytes, slots)?;
        let mut shards = Vec::with_capacity(workers);
        for _ in 0..workers {
            shards.push(Padded(Shard::new()));
        }
        // Frames are distributed lazily; a worker steals resident frames from lists that look
        // occupied before taking cold ones.
        shards[0].0.lock().frames.extend((0..initial).rev());
        Ok(Self {
            mapping,
            file,
            header_bytes,
            page,
            capacity: frames,
            reserve_frames,
            slots,
            free_slots: Mutex::new((0..slots).rev().collect()),
            states: FrameStates::new(frames as usize, initial)?,
            shards: shards.into_boxed_slice(),
            slow: Padded(Mutex::new(Slow {
                backed: initial,
                cold: Vec::new(),
                candidates: Vec::new(),
            })),
            used: Padded(AtomicU32::new(0)),
            backed: Padded(AtomicU32::new(initial)),
        })
    }

    fn shard(&self) -> usize {
        THREAD_SHARD.with(|assigned| {
            let id = assigned.get().unwrap_or_else(|| {
                let id = NEXT_SHARD.fetch_add(1, Ordering::Relaxed);
                assigned.set(Some(id));
                id
            });
            id % self.shards.len()
        })
    }

    /// Pop a resident frame, this worker's list first. Unless `every`, lists that look empty
    /// are passed over unlocked.
    fn resident(&self, local: usize, every: bool) -> Option<u32> {
        for step in 0..self.shards.len() {
            let shard = &self.shards[(local + step) % self.shards.len()].0;
            if (every || shard.occupied())
                && let Some(frame) = shard.lock().pop()
            {
                return Some(frame);
            }
        }
        None
    }

    /// Take a free frame whose first `needed` bytes are populated.
    pub(crate) fn allocate(&self, needed: usize) -> io::Result<u32> {
        let local = self.shard();
        let frame = match self.resident(local, false) {
            Some(frame) => frame,
            None => self.reclaim(local)?,
        };
        // Populate outside every lock: the frame is this caller's alone. A resident frame may
        // be populated only for smaller records, a cold one not at all; on refusal it joins
        // the cold list, and eviction may free better ones.
        if let Err(error) = self.prefault(frame, needed) {
            lock(&self.slow.0).cold.push(frame);
            return Err(error);
        }
        self.states[frame as usize].refs.store(1, Ordering::Relaxed);
        self.used.0.fetch_add(1, Ordering::Relaxed);
        Ok(frame)
    }

    /// A frame for an allocation that found no resident one: cold, resident after all, or
    /// grown. The reclaimer holds this lock until every candidate is on the cold list and
    /// its punch completes, so allocation cannot see missing capacity.
    fn reclaim(&self, local: usize) -> io::Result<u32> {
        let mut slow = lock(&self.slow.0);
        if let Some(frame) = slow.cold.pop() {
            return Ok(frame);
        }
        if let Some(frame) = self.resident(local, true) {
            return Ok(frame);
        }
        if slow.backed == self.capacity {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "stream pool capacity exhausted",
            ));
        }
        let old = slow.backed;
        let new = old.saturating_add(EXTENT).min(self.capacity);
        let old_end = round_up(self.header_bytes + old as usize * FRAME_BYTES, self.page)?;
        let new_end = round_up(self.header_bytes + new as usize * FRAME_BYTES, self.page)?;
        self.file.grow(old_end, new_end - old_end)?;
        os::populate(self.address(old_end), new_end - old_end)?;
        for frame in old..new {
            self.states[frame as usize]
                .ready
                .store(FRAME_BYTES as u32, Ordering::Relaxed);
        }
        slow.backed = new;
        self.backed.0.store(new, Ordering::Release);
        self.shards[local].0.lock().frames.extend((old + 1..new).rev());
        Ok(old)
    }

    /// Whether the first `needed` bytes of a frame the caller owns are populated.
    pub(crate) fn is_ready(&self, frame: u32, needed: usize) -> bool {
        needed <= self.states[frame as usize].ready.load(Ordering::Relaxed) as usize
    }

    /// Populate the pages under the first `needed` bytes of a frame the caller owns. Pages
    /// that are already populated cost nothing; a refusal (tmpfs full) leaves them unwritten.
    pub(crate) fn prefault(&self, frame: u32, needed: usize) -> io::Result<()> {
        let ready = self.states[frame as usize].ready.load(Ordering::Relaxed) as usize;
        if needed <= ready {
            return Ok(());
        }
        assert!(frame < self.backed_frames() && needed <= FRAME_BYTES);
        // A frame written in small records grows its prefix by doubling, so filling it costs
        // a few calls rather than one per page. Near exhaustion only `needed` must succeed.
        let ahead = needed.max(ready * 2).min(FRAME_BYTES);
        if ahead > needed && self.populate(frame, ready, ahead).is_ok() {
            return Ok(());
        }
        self.populate(frame, ready, needed)
    }

    /// Populate a frame's bytes from `ready` up to `to` and record the new prefix.
    fn populate(&self, frame: u32, ready: usize, to: usize) -> io::Result<()> {
        let start = self.frame_start(frame);
        let from = (start + ready) / self.page * self.page;
        let to = round_up(start + to, self.page)?;
        os::populate(self.address(from), to - from)?;
        let ready = (to - start).min(FRAME_BYTES) as u32;
        self.states[frame as usize].ready.store(ready, Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn references(&self, frame: u32) -> u32 {
        self.states[frame as usize].refs.load(Ordering::Relaxed)
    }

    /// Whether another reference would overflow the frame's count.
    pub(crate) fn saturated(&self, frame: u32) -> bool {
        self.states[frame as usize].refs.load(Ordering::Relaxed) == u32::MAX
    }

    /// Add a reference to an allocated frame. The caller holds a reference that cannot be
    /// released concurrently and has checked `saturated` since its last increment.
    pub(crate) fn retain(&self, frame: u32) {
        self.states[frame as usize].refs.fetch_add(1, Ordering::Relaxed);
    }

    /// Drop one reference, which the caller holds and gives up exactly once. The last one
    /// returns the frame to this thread's free list; no mapped borrow may survive it.
    /// Returns whether the frame became free.
    pub(crate) fn release(&self, frame: u32) -> bool {
        assert!(frame < self.backed_frames());
        let previous = self.states[frame as usize].refs.fetch_sub(1, Ordering::Release);
        assert_ne!(previous, 0, "frame released without a reference");
        if previous != 1 {
            return false;
        }
        // Every earlier holder's reads happen before the frame is reused.
        fence(Ordering::Acquire);
        self.used.0.fetch_sub(1, Ordering::Relaxed);
        self.shards[self.shard()].0.lock().frames.push(frame);
        true
    }

    fn frame_start(&self, frame: u32) -> usize {
        self.header_bytes + frame as usize * FRAME_BYTES
    }

    fn address(&self, offset: usize) -> *mut u8 {
        assert!(offset <= self.mapping.len);
        // SAFETY: the checked offset is inside or one past the reserved mapping.
        unsafe { self.mapping.base.as_ptr().add(offset) }
    }

    fn span(&self, frame: u32, offset: usize, len: usize) -> *mut u8 {
        assert!(frame < self.backed_frames());
        assert!(offset <= FRAME_BYTES && len <= FRAME_BYTES - offset);
        self.address(self.frame_start(frame) + offset)
    }

    /// # Safety
    /// The caller owns this allocated frame; the span is in bounds and unpublished.
    /// No reader or other writer may access the span until publication synchronizes it.
    pub(crate) unsafe fn write(&self, frame: u32, offset: usize, bytes: &[u8]) {
        let target = self.span(frame, offset, bytes.len());
        // SAFETY: caller owns the unpublished destination; copy permits a mapped
        // source span to overlap, without creating an escaping mutable reference.
        unsafe { std::ptr::copy(bytes.as_ptr(), target, bytes.len()) };
    }

    /// # Safety
    /// The frame is allocated and held by a lease for the returned borrow's lifetime.
    /// The span is in bounds, published with synchronization, and remains immutable.
    pub(crate) unsafe fn slice(&self, frame: u32, offset: usize, len: usize) -> &[u8] {
        let source = self.span(frame, offset, len);
        // SAFETY: caller pins initialized immutable bytes until the borrow ends.
        unsafe { std::slice::from_raw_parts(source, len) }
    }

    pub(crate) fn claim_slot(&self) -> Option<u32> {
        let slot = lock(&self.free_slots).pop()?;
        let header = self.header(slot);
        header.committed.store(0, Ordering::Relaxed);
        Some(slot)
    }

    /// Caller returns a slot exactly once, at invalidation or drop of its stream; the stream
    /// never loads or stores that header afterwards.
    pub(crate) fn release_slot(&self, slot: u32) {
        assert!(slot < self.slots);
        lock(&self.free_slots).push(slot);
    }

    pub(crate) fn header(&self, slot: u32) -> &Header {
        assert!(slot < self.slots);
        // SAFETY: construction initialized each aligned slot; it remains mapped
        // for self's lifetime, and shared access touches atomic fields only.
        unsafe { &*self.address(slot as usize * 128).cast::<Header>() }
    }

    /// Called once per externally scheduled second. Returns reclamation failures.
    pub(crate) fn decay(&self) -> io::Result<()> {
        let mut slow = lock(&self.slow.0);
        let Slow { cold, candidates, .. } = &mut *slow;
        candidates.clear();
        // The warm budget is pool-wide: frames freed on one thread stay warm for any thread,
        // which steals resident frames before taking cold ones.
        let mut warm = WARM * self.shards.len();
        for shard in &self.shards {
            let mut free = shard.0.lock();
            let idle = free.low_water.min(free.frames.len());
            let kept = idle.min(warm);
            warm -= kept;
            let count = idle - kept;
            // The startup prefix stays backed even while free; punch only the excess.
            candidates.extend(free.frames.extract_if(..count, |frame| *frame >= self.reserve_frames));
            free.low_water = free.frames.len();
        }
        for &frame in candidates.iter() {
            self.states[frame as usize].ready.store(0, Ordering::Relaxed);
        }
        candidates.sort_unstable();
        // Publish cold ownership before any destructive syscall, even if a later
        // run fails. Conservative prefault also covers partially successful punches.
        cold.extend_from_slice(candidates);
        let mut first_error = None;
        let mut cursor = 0;
        while cursor < candidates.len() {
            let first = candidates[cursor];
            let mut last = first;
            cursor += 1;
            while cursor < candidates.len() && candidates[cursor] == last + 1 {
                last = candidates[cursor];
                cursor += 1;
            }
            let start = round_up(self.frame_start(first), self.page)?;
            let end = (self.frame_start(last) + FRAME_BYTES) / self.page * self.page;
            if end > start
                && let Err(error) = self.file.punch(start, end - start)
            {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    #[cfg(test)]
    pub(crate) fn cold_frames(&self) -> usize {
        lock(&self.slow.0).cold.len()
    }

    pub(crate) fn capacity(&self) -> u32 {
        self.capacity
    }
    pub(crate) fn used(&self) -> u32 {
        self.used.0.load(Ordering::Relaxed)
    }
    pub(crate) fn backed_frames(&self) -> u32 {
        self.backed.0.load(Ordering::Acquire)
    }
}
