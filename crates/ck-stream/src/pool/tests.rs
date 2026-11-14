use super::*;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt, symlink};
use std::path::PathBuf;
use std::sync::Arc;

static NEXT_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        // st_blocks decay is a tmpfs contract; disk-backed integration tests live in tests/storage.rs.
        let root = std::env::var_os("TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/dev/shm"));
        let path = root.join(format!(
            "ck-stream-pool-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn blocks(file: &os::Backing) -> u64 {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: the backing fd is live and stat is writable.
    assert_eq!(unsafe { libc::fstat(file.fd(), stat.as_mut_ptr()) }, 0);
    // SAFETY: fstat succeeded, initializing st_blocks.
    unsafe { stat.assume_init().st_blocks as u64 }
}

#[test]
fn growth_reuse_and_exact_cap() {
    let directory = Directory::new();
    let pool = Pool::open(&directory.0, 45, 3, 2, 0).unwrap();
    assert_eq!(pool.capacity(), 45);
    assert_eq!(pool.backed_frames(), 21);
    let mut allocated = Vec::new();
    for n in 0..45 {
        let frame = pool.allocate(FRAME_BYTES).unwrap();
        assert!(!allocated.contains(&frame));
        // SAFETY: this test exclusively owns the new frame and has not published it.
        unsafe {
            pool.write(frame, FRAME_BYTES - 2, &[n as u8, 0xa5]);
        }
        allocated.push(frame);
    }
    assert_eq!(pool.backed_frames(), 45);
    assert_eq!(pool.used(), 45);
    assert_eq!(
        pool.allocate(FRAME_BYTES).unwrap_err().kind(),
        io::ErrorKind::OutOfMemory
    );
    for (n, &frame) in allocated.iter().enumerate() {
        // SAFETY: every allocated frame remains pinned and immutable during this read.
        assert_eq!(unsafe { pool.slice(frame, FRAME_BYTES - 2, 2) }, &[n as u8, 0xa5]);
    }
    let frame = allocated.pop().unwrap();
    pool.release(frame);
    assert_eq!(pool.allocate(FRAME_BYTES).unwrap(), frame);
    pool.release(frame);
    for frame in allocated {
        pool.release(frame);
    }
    assert_eq!(pool.used(), 0);
    assert_eq!(
        fs::metadata(format!("/proc/self/fd/{}", pool.file.fd())).unwrap().len() as usize,
        round_up(pool.header_bytes + 45 * FRAME_BYTES, pool.page).unwrap()
    );
}

#[test]
fn slots_are_aligned_exclusive_and_reset() {
    let directory = Directory::new();
    let pool = Pool::open(&directory.0, 4, 40, 1, 0).unwrap();
    assert_eq!(std::mem::align_of::<Pool>(), 128);
    assert_eq!(std::mem::size_of::<Header>(), 128);
    let mut slots = Vec::new();
    for _ in 0..40 {
        let slot = pool.claim_slot().unwrap();
        assert!(!slots.contains(&slot));
        let header = pool.header(slot);
        assert_eq!(header as *const Header as usize % 128, 0);
        header.committed.store(1234, Ordering::Release);
        slots.push(slot);
    }
    assert!(pool.claim_slot().is_none());
    let slot = slots.pop().unwrap();
    pool.release_slot(slot);
    assert_eq!(pool.claim_slot(), Some(slot));
    assert_eq!(pool.header(slot).committed.load(Ordering::Acquire), 0);
    for shard in &pool.shards {
        assert_eq!(shard as *const _ as usize % 128, 0);
    }
}

#[test]
fn decay_preserves_a_leased_neighbor_and_prefaults_reuse() {
    let directory = Directory::new();
    let pool = Arc::new(Pool::open(&directory.0, 64, 2, 1, 0).unwrap());
    let frames: Vec<_> = (0..64).map(|_| pool.allocate(FRAME_BYTES).unwrap()).collect();
    let neighbor = frames[16];
    // SAFETY: this test exclusively owns the allocated neighbor before publication.
    unsafe {
        pool.write(neighbor, 0, &[0x5a; FRAME_BYTES]);
    }
    let lease = Arc::clone(&pool);
    // SAFETY: lease keeps the pool alive and neighbor remains owned/immutable.
    let bytes = unsafe { lease.slice(neighbor, 0, FRAME_BYTES) };
    let before = blocks(&pool.file);
    for &frame in &frames {
        if frame != neighbor {
            pool.release(frame);
        }
    }
    assert_eq!(blocks(&pool.file), before, "release must not punch backing pages");
    pool.decay().unwrap();
    assert_eq!(
        blocks(&pool.file),
        before,
        "first tick only establishes the idle interval"
    );
    pool.decay().unwrap();
    assert!(blocks(&pool.file) < before);
    assert_eq!(pool.shards[0].0.lock().frames.len(), WARM);
    assert_eq!(bytes, &[0x5a; FRAME_BYTES]);
    let mut reused = Vec::new();
    for _ in 0..63 {
        let frame = pool.allocate(FRAME_BYTES).unwrap();
        assert_ne!(frame, neighbor);
        // SAFETY: the newly reserved frame has no readers; cold prefault succeeded.
        unsafe {
            pool.write(frame, 0, &[0x33; FRAME_BYTES]);
        }
        reused.push(frame);
    }
    assert_eq!(bytes, &[0x5a; FRAME_BYTES]);
    assert!(lock(&pool.slow.0).cold.is_empty());
    drop(pool);
    assert_eq!(bytes, &[0x5a; FRAME_BYTES]);
    for frame in reused {
        lease.release(frame);
    }
    lease.release(neighbor);
}

#[test]
fn the_warm_budget_is_pool_wide() {
    let directory = Directory::new();
    let pool = Pool::open(&directory.0, 42, 1, 2, 0).unwrap();
    let frames: Vec<_> = (0..2 * WARM + 4).map(|_| pool.allocate(FRAME_BYTES).unwrap()).collect();
    // One thread frees every frame, so one free list holds almost all of them.
    for frame in frames {
        pool.release(frame);
    }
    pool.decay().unwrap();
    pool.decay().unwrap();
    let kept: usize = pool.shards.iter().map(|shard| shard.0.lock().frames.len()).sum();
    assert_eq!(kept, 2 * WARM);
}

#[test]
fn cold_frames_prefault_only_the_pages_a_record_needs() {
    let directory = Directory::new();
    let pool = Pool::open(&directory.0, 21, 1, 1, 0).unwrap();
    pool.decay().unwrap();
    pool.decay().unwrap();
    let warm: Vec<_> = (0..WARM).map(|_| pool.allocate(FRAME_BYTES).unwrap()).collect();
    let page_blocks = (pool.page / 512) as u64;
    let before = blocks(&pool.file);
    let frame = pool.allocate(100).unwrap();
    // The record's span touches at most two pages, one of which may straddle a neighbour.
    let small = blocks(&pool.file) - before;
    assert!(small <= 2 * page_blocks, "{small} blocks for 100 bytes");
    pool.prefault(frame, FRAME_BYTES).unwrap();
    assert!(blocks(&pool.file) - before >= (FRAME_BYTES / pool.page) as u64 * page_blocks);
    // SAFETY: this test exclusively owns the allocated frame, now populated end to end.
    unsafe { pool.write(frame, 0, &[0x7e; FRAME_BYTES]) };
    // Released first, the frame sits below the eight warm ones and is punched.
    pool.release(frame);
    for frame in warm {
        pool.release(frame);
    }
    pool.decay().unwrap();
    pool.decay().unwrap();
    // Punching forgets the populated prefix, so reuse populates again.
    assert_eq!(pool.states[frame as usize].ready.load(Ordering::Relaxed), 0);
}

#[test]
fn reused_lifo_frames_wait_a_whole_interval() {
    let directory = Directory::new();
    let pool = Pool::open(&directory.0, 21, 1, 1, 0).unwrap();
    pool.decay().unwrap();
    let frame = pool.allocate(FRAME_BYTES).unwrap();
    pool.release(frame);
    pool.decay().unwrap();
    // One popped frame and eight warm frames cannot have been reclaimed.
    assert_eq!(pool.shards[0].0.lock().frames.len(), WARM + 1);
    assert_eq!(pool.allocate(FRAME_BYTES).unwrap(), frame);
    pool.release(frame);
}

#[test]
fn resident_stealing_precedes_cold_prefault() {
    let directory = Directory::new();
    let pool = Pool::open(&directory.0, 21, 1, 2, 0).unwrap();
    pool.decay().unwrap();
    pool.decay().unwrap();
    let cold = lock(&pool.slow.0).cold.len();
    assert!(cold > 0);
    // Empty the calling shard, leaving resident frames only on its neighbor.
    let local = pool.shard();
    let other = (local + 1) % 2;
    let moved = std::mem::take(&mut pool.shards[local].0.lock().frames);
    pool.shards[other].0.lock().frames.extend(moved);
    let frame = pool.allocate(FRAME_BYTES).unwrap();
    assert_eq!(lock(&pool.slow.0).cold.len(), cold);
    pool.release(frame);
}

#[test]
fn allocation_finds_resident_frames_a_stale_length_hid_before_growing() {
    let directory = Directory::new();
    let pool = Pool::open(&directory.0, 42, 1, 2, 0).unwrap();
    // Every frame sits on the neighbor whose length mirror reads empty; nothing is cold.
    let local = pool.shard();
    let other = (local + 1) % 2;
    let moved = std::mem::take(&mut pool.shards[local].0.lock().frames);
    pool.shards[other].0.lock().frames.extend(moved);
    pool.shards[other].0.len.store(0, Ordering::Relaxed);
    let frame = pool.allocate(FRAME_BYTES).unwrap();
    assert_eq!(pool.backed_frames(), 21);
    pool.release(frame);
}

#[test]
fn private_modes_creation_and_symlink_rejection() {
    let directory = Directory::new();
    let child = directory.0.join("private");
    let pool = Pool::open(&child, 1, 1, 1, 0).unwrap();
    assert_eq!(fs::metadata(&child).unwrap().permissions().mode() & 0o7777, 0o700);
    assert_eq!(os::file_mode(&pool.file), 0o600);
    assert_eq!(fs::read_dir(&child).unwrap().count(), 0);
    let fallback = os::Backing::open_exclusive(&child).unwrap();
    assert_eq!(os::file_mode(&fallback), 0o600);
    assert_eq!(fs::read_dir(&child).unwrap().count(), 0);
    let link = directory.0.join("link");
    symlink(&child, &link).unwrap();
    assert!(Pool::open(&link, 1, 1, 1, 0).is_err());
    assert!(Pool::open(&link.join("nested"), 1, 1, 1, 0).is_err());
    fs::set_permissions(&child, fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(
        Pool::open(&child, 1, 1, 1, 0).err().unwrap().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert!(Pool::open(&directory.0, 0, 1, 1, 0).is_err());
    assert!(Pool::open(&directory.0, 1, 0, 1, 0).is_err());
    assert!(Pool::open(&directory.0, 1, 1, 0, 0).is_err());
    assert!(Pool::open(&directory.0, 1, 1, 1, 2).is_err());
}

#[test]
fn concurrent_allocations_and_decay_do_not_alias() {
    let directory = Directory::new();
    let pool = Arc::new(Pool::open(&directory.0, 168, 16, 8, 0).unwrap());
    std::thread::scope(|scope| {
        for worker in 0..8 {
            let pool = &pool;
            scope.spawn(move || {
                for _ in 0..500 {
                    let frame = pool.allocate(FRAME_BYTES).unwrap();
                    // SAFETY: worker exclusively owns this unpublished frame.
                    unsafe {
                        pool.write(frame, 0, &[worker; 128]);
                    }
                    std::thread::yield_now();
                    // SAFETY: worker retains the immutable allocated frame.
                    assert_eq!(unsafe { pool.slice(frame, 0, 128) }, &[worker; 128]);
                    pool.release(frame);
                }
            });
        }
        let pool = &pool;
        scope.spawn(move || {
            for _ in 0..50 {
                pool.decay().unwrap();
            }
        });
    });
    assert_eq!(pool.used(), 0);
    let frames: Vec<_> = (0..168).map(|_| pool.allocate(FRAME_BYTES).unwrap()).collect();
    let mut distinct = frames.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(distinct.len(), 168);
    for frame in frames {
        pool.release(frame);
    }
}

#[test]
#[should_panic]
fn rejects_out_of_bounds_span() {
    let directory = Directory::new();
    let pool = Pool::open(&directory.0, 1, 1, 1, 0).unwrap();
    let frame = pool.allocate(FRAME_BYTES).unwrap();
    // SAFETY: ownership is valid; this deliberately invalid bound must panic before
    // any pointer access, exercising the documented bounds assertion.
    unsafe {
        pool.write(frame, FRAME_BYTES, &[1]);
    }
}

/// Tests that fill the one disposable tmpfs hold this for their whole run.
fn isolated_tmpfs() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    lock(&SERIAL)
}

fn fill_available(path: &Path) -> File {
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: fd is live, stat is writable output storage.
    assert_eq!(unsafe { libc::fstatvfs(file.as_raw_fd(), stat.as_mut_ptr()) }, 0);
    // SAFETY: successful fstatvfs initialized the fields.
    let stat = unsafe { stat.assume_init() };
    assert!(
        stat.f_blocks * stat.f_frsize <= 4 * 1024 * 1024,
        "refusing to fill anything larger than the disposable 4 MiB filesystem"
    );
    let available = stat.f_bavail * stat.f_frsize;
    assert!(available > 0);
    // SAFETY: the newly created filler is private, and the extent fits this tiny tmpfs.
    assert_eq!(
        unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, available as libc::off_t) },
        0
    );
    file
}

#[test]
#[ignore = "requires a disposable private 4 MiB tmpfs supplied as TMPDIR"]
fn isolated_tmpfs_growth_and_cold_prefault_enospc() {
    let _serial = isolated_tmpfs();
    let directory = Directory::new();
    let pool = Pool::open(&directory.0, 42, 1, 1, 0).unwrap();
    let mut frames: Vec<_> = (0..21).map(|_| pool.allocate(FRAME_BYTES).unwrap()).collect();
    let filler_path = directory.0.join("filler");
    let filler = fill_available(&filler_path);
    assert_eq!(
        Pool::open(&directory.0, 42, 1, 1, 42).err().unwrap().raw_os_error(),
        Some(libc::ENOSPC)
    );
    let backed_bytes = fs::metadata(format!("/proc/self/fd/{}", pool.file.fd())).unwrap().len();
    assert_eq!(
        pool.allocate(FRAME_BYTES).unwrap_err().raw_os_error(),
        Some(libc::ENOSPC)
    );
    assert_eq!(pool.used(), 21);
    assert_eq!(pool.backed_frames(), 21);
    assert_eq!(
        fs::metadata(format!("/proc/self/fd/{}", pool.file.fd())).unwrap().len(),
        backed_bytes
    );
    drop(filler);
    fs::remove_file(&filler_path).unwrap();
    frames.extend((21..42).map(|_| pool.allocate(FRAME_BYTES).unwrap()));
    for frame in frames {
        pool.release(frame);
    }
    pool.decay().unwrap();
    pool.decay().unwrap();
    let warm: Vec<_> = (0..WARM).map(|_| pool.allocate(FRAME_BYTES).unwrap()).collect();
    let cold = lock(&pool.slow.0).cold.len();
    let filler = fill_available(&filler_path);
    assert_eq!(
        pool.allocate(FRAME_BYTES).unwrap_err().raw_os_error(),
        Some(libc::EFAULT),
        "cold reservation must fail before any content write"
    );
    assert_eq!(pool.used(), WARM as u32);
    assert_eq!(lock(&pool.slow.0).cold.len(), cold);
    drop(filler);
    fs::remove_file(&filler_path).unwrap();
    let mut restored = Vec::new();
    for _ in 0..cold {
        let frame = pool.allocate(FRAME_BYTES).unwrap();
        // SAFETY: successful prefault restored the exclusively owned cold frame.
        unsafe {
            pool.write(frame, 0, &[0x9b; FRAME_BYTES]);
        }
        restored.push(frame);
    }
    assert_eq!(pool.used(), 42);
    for frame in warm.into_iter().chain(restored) {
        pool.release(frame);
    }
    assert_eq!(pool.used(), 0);
}

#[test]
fn prefault_doubles_the_populated_prefix() {
    let directory = Directory::new();
    let pool = Pool::open(&directory.0, 21, 1, 1, 0).unwrap();
    pool.decay().unwrap();
    pool.decay().unwrap();
    let warm: Vec<_> = (0..WARM).map(|_| pool.allocate(FRAME_BYTES).unwrap()).collect();
    let frame = pool.allocate(100).unwrap();
    let ready = || pool.states[frame as usize].ready.load(Ordering::Relaxed) as usize;
    assert!(ready() <= pool.page);
    // Record by record, a frame fills in a few populate calls rather than one per page.
    let mut calls = 0;
    while ready() < FRAME_BYTES {
        pool.prefault(frame, ready() + 1).unwrap();
        calls += 1;
    }
    assert!(calls <= 6, "{calls} populate calls for one frame");
    for frame in warm.into_iter().chain([frame]) {
        pool.release(frame);
    }
}

#[test]
#[ignore = "requires a disposable private 4 MiB tmpfs supplied as TMPDIR"]
fn isolated_tmpfs_prefault_ahead_falls_back_to_the_needed_page() {
    let _serial = isolated_tmpfs();
    let directory = Directory::new();
    let pool = Pool::open(&directory.0, 21, 1, 1, 0).unwrap();
    pool.decay().unwrap();
    pool.decay().unwrap();
    let warm: Vec<_> = (0..WARM).map(|_| pool.allocate(FRAME_BYTES).unwrap()).collect();
    let frame = pool.allocate(4 * pool.page).unwrap();
    let ready = pool.states[frame as usize].ready.load(Ordering::Relaxed) as usize;
    let filler_path = directory.0.join("filler");
    let filler = fill_available(&filler_path);
    // One free page: populating ahead is refused, the page the record needs is granted.
    filler
        .set_len(filler.metadata().unwrap().len() - pool.page as u64)
        .unwrap();
    pool.prefault(frame, ready + 1).unwrap();
    let after = pool.states[frame as usize].ready.load(Ordering::Relaxed) as usize;
    assert_eq!(after, ready + pool.page);
    assert_eq!(
        pool.prefault(frame, after + 1).unwrap_err().raw_os_error(),
        Some(libc::EFAULT)
    );
    drop(filler);
    fs::remove_file(&filler_path).unwrap();
    for frame in warm.into_iter().chain([frame]) {
        pool.release(frame);
    }
}

#[test]
fn startup_reserve_survives_decay_while_excess_is_reclaimed() {
    let directory = Directory::new();
    let reserve = 25;
    let pool = Pool::open(&directory.0, 64, 1, 1, reserve).unwrap();
    let reserved_bytes = round_up(pool.header_bytes + reserve as usize * FRAME_BYTES, pool.page).unwrap();
    assert!(blocks(&pool.file) * 512 >= reserved_bytes as u64);
    let frames: Vec<_> = (0..64).map(|_| pool.allocate(FRAME_BYTES).unwrap()).collect();
    for &frame in &frames {
        // SAFETY: this test owns every frame exclusively and initializes its whole payload.
        unsafe { pool.write(frame, 0, &[0x5a; FRAME_BYTES]) };
    }
    for frame in frames {
        pool.release(frame);
    }
    let before = blocks(&pool.file);
    pool.decay().unwrap();
    assert_eq!(blocks(&pool.file), before);
    pool.decay().unwrap();
    pool.decay().unwrap();
    assert!(blocks(&pool.file) * 512 >= reserved_bytes as u64);
    assert!(blocks(&pool.file) < before);
    for _ in 0..64 {
        let frame = pool.allocate(FRAME_BYTES).unwrap();
        if frame < reserve {
            // SAFETY: reallocation exclusively owns initialized bytes; a punch would have zeroed them.
            assert_eq!(unsafe { pool.slice(frame, 0, FRAME_BYTES) }, &[0x5a; FRAME_BYTES]);
        }
    }
}
