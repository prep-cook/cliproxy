use ck_stream::{Config, Error, FRAME_BYTES, ManualClock, Store, Terminal};
use std::{
    sync::{Arc, atomic::Ordering},
    task::Poll,
    time::Duration,
};

fn store(frames: usize, slots: u32) -> (tempfile::TempDir, Store, Arc<ManualClock>) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path().join("pool"));
    config.pool_bytes = Some(frames * FRAME_BYTES);
    config.max_frames = Some(4);
    config.max_event_bytes = 4 * FRAME_BYTES;
    config.header_slots = slots;
    config.workers = 1;
    let clock = Arc::new(ManualClock::default());
    let store = Store::with_clock(config, clock.clone()).unwrap();
    (dir, store, clock)
}

#[test]
fn oldest_read_age_counts_attached_readers_as_fresh_and_skips_invalidated_streams() {
    let (_dir, store, clock) = store(8, 32);
    assert_eq!(store.stats().oldest_read_age, None);
    let mut attached = store.create().unwrap();
    attached.append(b"read").unwrap();
    let reader = store.reader(attached.id(), 0).unwrap();
    clock.advance(Duration::from_secs(5));
    let mut unread = store.create().unwrap();
    clock.advance(Duration::from_secs(5));
    // `attached` was last read at 0 but has a reader; `unread` counts from its creation at 5.
    let stats = store.stats();
    assert_eq!(stats.oldest_read_age, Some(Duration::from_secs(5)));
    assert_eq!(stats.cached_streams, 2);
    drop(reader);
    clock.advance(Duration::from_secs(1));
    unread.finish(Terminal::Gone);
    assert_eq!(store.stats().oldest_read_age, Some(Duration::from_secs(1)));
}

#[test]
fn expiry_is_counted_once_per_stream_and_is_not_an_eviction() {
    let (_dir, store, clock) = store(8, 32);
    let mut expiring = store.create().unwrap();
    expiring.append(b"old").unwrap();
    clock.advance(Duration::from_secs(3601));
    let fresh = store.create().unwrap();
    store.tick().unwrap();
    store.tick().unwrap();
    let stats = store.stats();
    assert_eq!((stats.expiries, stats.evictions, stats.cached_streams), (1, 0, 1));
    assert!(store.reader(fresh.id(), 0).is_ok());
    assert!(matches!(store.reader(expiring.id(), 0), Err(Error::Gone)));
}

#[test]
fn frame_and_slot_pressure_count_every_victim() {
    let (_dir, store, clock) = store(4, 4);
    let mut writers = Vec::new();
    for _ in 0..4 {
        let mut writer = store.create().unwrap();
        writer.append(&[1; FRAME_BYTES]).unwrap();
        writers.push(writer);
        clock.advance(Duration::from_secs(1));
    }
    // Two frames free only by invalidating the two least recently read streams.
    let mut allocating = writers.remove(3);
    allocating.append(&[2; 2 * FRAME_BYTES]).unwrap();
    assert_eq!(store.stats().evictions, 2);
    // Two slots are held; two new streams fit, the third evicts one more.
    let _second = store.create().unwrap();
    let _third = store.create().unwrap();
    assert_eq!(store.stats().evictions, 2);
    let _fourth = store.create().unwrap();
    let stats = store.stats();
    assert_eq!((stats.evictions, stats.allocation_failures, stats.expiries), (3, 0, 0));
}

#[test]
fn per_stream_limit_counts_one_failure_with_its_charge_and_evicts_nothing() {
    let (_dir, store, _) = store(8, 32);
    let mut other = store.create().unwrap();
    other.append(b"other").unwrap();
    let mut writer = store.create().unwrap();
    writer.append(&vec![1; 4 * FRAME_BYTES]).unwrap();
    assert_eq!(writer.storage_failure_frames(), None);
    assert!(matches!(writer.append(b"over"), Err(Error::StorageExhausted)));
    assert!(matches!(writer.append(b"after"), Err(Error::Closed)));
    assert_eq!(writer.storage_failure_frames(), Some(4));
    assert_eq!(other.storage_failure_frames(), None);
    let stats = store.stats();
    assert_eq!(stats.allocation_failures, 1);
    assert_eq!(stats.last_storage_exhausted_frames, Some(4));
    assert_eq!(stats.evictions, 0);
    assert_eq!(stats.cache_bytes, u64::from(stats.used_frames) * FRAME_BYTES as u64);
    assert_eq!(stats.used_frames, 5);
}

#[test]
fn failure_charge_includes_scratch_frames() {
    let (_dir, store, _) = store(8, 32);
    let mut writer = store.create().unwrap();
    let mut scratch = writer.begin_scratch(FRAME_BYTES).unwrap();
    scratch.write(&[3; FRAME_BYTES]).unwrap();
    let _scratch = scratch.complete().unwrap();
    writer.append(&[1; FRAME_BYTES]).unwrap();
    // Two more frames would exceed the limit of four; the reservation takes none of them.
    assert!(matches!(
        writer.append(&vec![2; 2 * FRAME_BYTES + 1]),
        Err(Error::StorageExhausted)
    ));
    assert_eq!(writer.storage_failure_frames(), Some(2));
    assert_eq!(store.stats().last_storage_exhausted_frames, Some(2));
}

#[test]
fn failure_charge_excludes_frames_a_failed_reservation_rolled_back() {
    let (_dir, store, _) = store(4, 32);
    let mut pinned = store.create().unwrap();
    pinned.append(&[1; FRAME_BYTES]).unwrap();
    let mut reader = store.reader(pinned.id(), 0).unwrap();
    let Poll::Ready(Some(ck_stream::Read::Record(_lease))) = reader.try_next() else {
        panic!("expected record");
    };
    let mut writer = store.create().unwrap();
    writer.append(&[2; FRAME_BYTES]).unwrap();
    // Two of the three frames are free; the leased stream cannot give the third.
    assert!(matches!(
        writer.append(&vec![3; 2 * FRAME_BYTES + 1]),
        Err(Error::StorageExhausted)
    ));
    assert_eq!(writer.storage_failure_frames(), Some(1));
    let stats = store.stats();
    assert_eq!(
        (stats.allocation_failures, stats.evictions, stats.used_frames),
        (1, 0, 2)
    );
}

fn take(reader: &mut ck_stream::Reader) -> ck_stream::Lease {
    match reader.try_next() {
        Poll::Ready(Some(ck_stream::Read::Record(lease))) => lease,
        _ => panic!("expected record"),
    }
}

#[test]
fn leased_frames_count_each_pinned_frame_once_until_its_last_lease_drops() {
    let (_dir, store, _) = store(8, 32);
    let mut writer = store.create().unwrap();
    let first = writer.append(&[1; FRAME_BYTES]).unwrap();
    writer.append(&[2; FRAME_BYTES / 2]).unwrap();
    // Spans the second and third frames.
    writer.append(&[3; FRAME_BYTES]).unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let (first_lease, second_lease, spanning) = (take(&mut reader), take(&mut reader), take(&mut reader));
    let shared = take(&mut store.reader(writer.id(), 1).unwrap());
    assert_eq!(store.stats().leased_frames, 3);
    // A released prefix frame stays pinned by its lease.
    writer.release_before(first.end).unwrap();
    assert_eq!(store.stats().leased_frames, 3);
    drop(spanning);
    assert_eq!(store.stats().leased_frames, 2);
    drop(first_lease);
    assert_eq!(store.stats().leased_frames, 1);
    second_lease.acknowledge();
    assert_eq!(store.stats().leased_frames, 1);
    drop(shared);
    assert_eq!(store.stats().leased_frames, 0);
}

#[test]
fn leased_frames_of_evicted_expired_and_invalidated_streams_still_count() {
    let (_dir, store, clock) = store(4, 32);
    let mut evicted = store.create().unwrap();
    evicted.append(&[1; FRAME_BYTES]).unwrap();
    evicted.append(&[1; FRAME_BYTES]).unwrap();
    let mut evicted_reader = store.reader(evicted.id(), 0).unwrap();
    let evicted_lease = take(&mut evicted_reader);
    drop(evicted_reader);
    let mut gone = store.create().unwrap();
    gone.append(b"gone").unwrap();
    let gone_lease = take(&mut store.reader(gone.id(), 0).unwrap());
    gone.finish(Terminal::Gone);
    assert_eq!(store.stats().leased_frames, 2);
    // Eviction takes the unleased frame and leaves the leased one pinned.
    clock.advance(Duration::from_secs(1));
    let mut allocating = store.create().unwrap();
    allocating.append(&[2; 2 * FRAME_BYTES]).unwrap();
    assert_eq!(store.stats().evictions, 1);
    assert!(matches!(store.reader(evicted.id(), 0), Err(Error::Gone)));
    assert_eq!(store.stats().leased_frames, 2);
    // Expiry empties the cache; the leased frames of streams that left it still count.
    clock.advance(Duration::from_secs(3601));
    store.tick().unwrap();
    let stats = store.stats();
    assert_eq!((stats.cached_streams, stats.leased_frames), (0, 2));
    drop(evicted_lease);
    assert_eq!(store.stats().leased_frames, 1);
    drop(gone_lease);
    assert_eq!(store.stats().leased_frames, 0);
}

#[test]
fn leased_frames_survive_shutdown() {
    let (_dir, mut store, _) = store(8, 32);
    let mut writer = store.create().unwrap();
    writer.append(b"record").unwrap();
    let lease = take(&mut store.reader(writer.id(), 0).unwrap());
    store.shutdown().unwrap();
    assert_eq!(store.stats().leased_frames, 1);
    drop(lease);
    assert_eq!(store.stats().leased_frames, 0);
}

#[test]
fn concurrent_reads_eviction_and_snapshots_neither_deadlock_nor_leak_leases() {
    let (_dir, store, _) = store(8, 4);
    let store = Arc::new(store);
    let latest = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let taken = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (finished, joined) = std::sync::mpsc::channel();
    let spawn = |work: Box<dyn FnOnce() + Send>| {
        let finished = finished.clone();
        std::thread::spawn(move || {
            work();
            finished.send(()).unwrap();
        })
    };
    let (writer_store, writer_latest, writer_done) = (store.clone(), latest.clone(), done.clone());
    spawn(Box::new(move || {
        for _ in 0..2000 {
            // Slot and frame pressure evict streams that readers may be leasing.
            let Ok(mut writer) = writer_store.create() else {
                continue;
            };
            writer_latest.store(writer.id(), Ordering::Release);
            for _ in 0..4 {
                if writer.append(&[5; FRAME_BYTES / 2]).is_err() {
                    break;
                }
            }
            writer.finish(Terminal::Done);
        }
        writer_done.store(true, Ordering::Release);
    }));
    for _ in 0..3 {
        let (store, latest, done, taken) = (store.clone(), latest.clone(), done.clone(), taken.clone());
        spawn(Box::new(move || {
            while !done.load(Ordering::Acquire) {
                let Ok(mut reader) = store.reader(latest.load(Ordering::Acquire), 0) else {
                    continue;
                };
                let mut held = Vec::new();
                while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
                    held.push(lease);
                }
                taken.fetch_add(held.len() as u64, Ordering::Relaxed);
                held.into_iter().step_by(2).for_each(ck_stream::Lease::acknowledge);
            }
        }));
    }
    let (snapshots, snapshot_done) = (store.clone(), done.clone());
    spawn(Box::new(move || {
        while !snapshot_done.load(Ordering::Acquire) {
            snapshots.stats();
        }
    }));
    for _ in 0..5 {
        joined
            .recv_timeout(Duration::from_secs(60))
            .expect("a thread deadlocked");
    }
    let stats = store.stats();
    assert!(stats.evictions > 0 && taken.load(Ordering::Relaxed) > 0);
    assert_eq!(stats.leased_frames, 0);
}

#[test]
fn streams_ended_gone_leave_the_cache() {
    let (_dir, store, _) = store(4, 1);
    for _ in 0..64 {
        let mut writer = store.create().unwrap();
        writer.append(b"gone").unwrap();
        writer.finish(Terminal::Gone);
    }
    let stats = store.stats();
    assert_eq!((stats.cached_streams, stats.used_frames), (0, 0));
    assert_eq!((stats.evictions, stats.expiries), (0, 0));
}
