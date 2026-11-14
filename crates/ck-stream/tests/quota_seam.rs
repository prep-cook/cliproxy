use ck_stream::{Config, Error, FRAME_BYTES, Read, Reader, ScratchLease, Store, Terminal, Writer};
use std::task::Poll;

fn store(pool_frames: usize, max_frames: u32) -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path().join("pool"));
    config.pool_bytes = Some(pool_frames * FRAME_BYTES);
    config.max_frames = Some(max_frames);
    config.max_event_bytes = max_frames as usize * FRAME_BYTES;
    config.max_metadata_bytes = 4096;
    config.header_slots = 16;
    config.workers = 2;
    let store = Store::open(config).unwrap();
    (dir, store)
}

fn scratch(writer: &mut Writer, frames: usize, byte: u8) -> ScratchLease {
    let mut reservation = writer.begin_scratch(frames * FRAME_BYTES).unwrap();
    for _ in 0..frames {
        reservation.write(&[byte; FRAME_BYTES]).unwrap();
    }
    reservation.complete().unwrap()
}

fn record(reader: &mut Reader) -> ck_stream::Lease {
    match reader.try_next() {
        Poll::Ready(Some(Read::Record(lease))) => lease,
        _ => panic!("expected canonical record"),
    }
}

fn terminal(reader: &mut Reader, expected: Terminal) {
    match reader.try_next() {
        Poll::Ready(Some(Read::Terminal(actual))) => assert_eq!(actual, expected),
        _ => panic!("expected terminal"),
    }
    assert!(matches!(reader.try_next(), Poll::Ready(None)));
}

#[test]
fn scratch_and_canonical_share_frame_quota_in_both_directions() {
    for scratch_first in [false, true] {
        let (_dir, store) = store(8, 4);
        let mut writer = store.create().unwrap();
        let mut reader = store.reader(writer.id(), 0).unwrap();
        let source;
        if scratch_first {
            source = Some(scratch(&mut writer, 3, 7));
            assert!(matches!(
                writer.append(&[1; 2 * FRAME_BYTES]),
                Err(Error::StorageExhausted)
            ));
        } else {
            source = None;
            writer.append(&[1; 3 * FRAME_BYTES]).unwrap();
            assert!(matches!(
                writer.begin_scratch(2 * FRAME_BYTES),
                Err(Error::StorageExhausted)
            ));
            let prefix = record(&mut reader);
            assert_eq!(prefix.chunk(prefix.record().start).unwrap(), &[1; FRAME_BYTES]);
        }
        assert_eq!(store.used_frames(), 3);
        terminal(&mut reader, Terminal::StorageExhausted);
        drop(source);
    }
}

#[test]
fn scratch_is_invisible_and_releases_independently_of_interleaved_response() {
    let (_dir, store) = store(4, 4);
    let mut writer = store.create().unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let mut source = writer.begin_scratch(FRAME_BYTES + 3).unwrap();
    source.write(&[7; FRAME_BYTES - 1]).unwrap();
    source.write(b"abcd").unwrap();
    assert_eq!(writer.committed().frame, 0);
    assert_eq!(writer.committed().offset, 0);
    assert!(matches!(reader.try_next(), Poll::Pending));
    let first = writer.append(b"response").unwrap();
    assert_eq!(first.sequence, 0);
    assert_eq!(first.start.frame, 0);
    assert_eq!(first.start.offset, 0);
    let source = source.complete().unwrap();
    assert_eq!(source.len(), FRAME_BYTES + 3);
    assert!(!source.is_empty());
    assert_eq!(source.chunk(FRAME_BYTES - 1).unwrap(), b"a");
    assert_eq!(source.chunk(FRAME_BYTES).unwrap(), b"bcd");
    assert!(matches!(source.chunk(source.len()), Err(Error::InvalidPosition)));
    let prefix = record(&mut reader);
    assert!(matches!(reader.try_next(), Poll::Pending));
    let second_source = scratch(&mut writer, 1, 8);
    assert_eq!(store.used_frames(), 4);
    drop(source);
    assert_eq!(store.used_frames(), 2);
    let replacement = scratch(&mut writer, 2, 9);
    assert_eq!(store.used_frames(), 4);
    assert_eq!(prefix.chunk(first.start).unwrap(), b"response");
    let second = writer.append(b" tail").unwrap();
    assert_eq!(second.sequence, 1);
    assert_eq!(second.start, first.end);
    assert_eq!(record(&mut reader).chunk(second.start).unwrap(), b" tail");
    drop((second_source, replacement));
    assert_eq!(store.used_frames(), 1);
    writer.finish(Terminal::Done);
    terminal(&mut reader, Terminal::Done);
    let mut resumed = store.reader(writer.id(), 0).unwrap();
    assert_eq!(record(&mut resumed).chunk(first.start).unwrap(), b"response");
}

#[test]
fn released_but_pinned_canonical_frames_still_count_against_scratch() {
    let (_dir, store) = store(8, 4);
    let mut writer = store.create().unwrap();
    let published = writer.append(&[3; 3 * FRAME_BYTES]).unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let pinned = record(&mut reader);
    writer.release_before(published.end).unwrap();
    assert!(matches!(
        writer.begin_scratch(2 * FRAME_BYTES),
        Err(Error::StorageExhausted)
    ));
    assert_eq!(store.used_frames(), 3);
    assert_eq!(pinned.chunk(published.start).unwrap(), &[3; FRAME_BYTES]);
    terminal(&mut reader, Terminal::StorageExhausted);
    drop(pinned);
    assert_eq!(store.used_frames(), 0);
}

#[test]
fn scratch_invalidation_pins_existing_borrows_and_rejects_new_access() {
    let (_dir, store) = store(4, 4);
    let mut writer = store.create().unwrap();
    let source = scratch(&mut writer, 3, 5);
    let borrowed = source.chunk(0).unwrap();
    let mut pending = writer.begin_scratch(1).unwrap();
    pending.write(b"x").unwrap();
    writer.finish(Terminal::Gone);
    assert_eq!(store.used_frames(), 4);
    assert_eq!(borrowed, &[5; FRAME_BYTES]);
    assert!(matches!(source.chunk(FRAME_BYTES), Err(Error::Gone)));
    assert!(matches!(pending.write(b""), Err(Error::Gone)));
    assert!(matches!(pending.complete(), Err(Error::Gone)));
    assert_eq!(store.used_frames(), 3);
    drop(writer);
    drop(source);
    assert_eq!(store.used_frames(), 0);
    let mut replacement = store.create().unwrap();
    let _replacement = scratch(&mut replacement, 4, 9);
    assert_eq!(store.used_frames(), 4);
}

#[test]
fn scratch_outlives_writer_and_store_without_reusing_mapping() {
    let (_dir, mut store) = store(4, 4);
    let mut writer = store.create().unwrap();
    let source = scratch(&mut writer, 2, 6);
    let mut reader = store.reader(writer.id(), 0).unwrap();
    drop(writer);
    terminal(&mut reader, Terminal::ProducerLost);
    store.shutdown().unwrap();
    drop((reader, store));
    assert_eq!(source.chunk(0).unwrap(), &[6; FRAME_BYTES]);
    assert_eq!(source.chunk(FRAME_BYTES).unwrap(), &[6; FRAME_BYTES]);
    drop(source);
}

#[test]
fn scratch_partial_pool_failure_rolls_back_and_reports_terminal() {
    let (_dir, store) = store(4, 4);
    let mut owner = store.create().unwrap();
    let pinned = scratch(&mut owner, 3, 2);
    let borrowed = pinned.chunk(0).unwrap();
    let mut writer = store.create().unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    assert!(matches!(
        writer.begin_scratch(2 * FRAME_BYTES),
        Err(Error::StorageExhausted)
    ));
    // The one free frame was rolled back; the scratch-held owner returns nothing to evict.
    assert_eq!(store.used_frames(), 3);
    assert_eq!(borrowed, &[2; FRAME_BYTES]);
    assert_eq!(pinned.chunk(0).unwrap(), borrowed);
    terminal(&mut reader, Terminal::StorageExhausted);
    drop(pinned);
    assert_eq!(store.used_frames(), 0);
    let mut replacement = store.create().unwrap();
    let _replacement = scratch(&mut replacement, 4, 4);
    assert_eq!(store.used_frames(), 4);
}

#[test]
fn abandoned_and_incomplete_scratch_return_frames_and_metadata() {
    let (_dir, store) = store(4, 4);
    let mut writer = store.create().unwrap();
    assert!(matches!(writer.begin_scratch(0), Err(Error::InvalidRecord)));
    let mut incomplete = writer.begin_scratch(FRAME_BYTES + 1).unwrap();
    incomplete.write(b"partial").unwrap();
    assert!(matches!(incomplete.complete(), Err(Error::IncompleteRecord)));
    assert_eq!(store.used_frames(), 0);
    let mut abandoned = writer.begin_scratch(1).unwrap();
    assert!(matches!(abandoned.write(b"overrun"), Err(Error::InvalidPosition)));
    abandoned.write(b"x").unwrap();
    drop(abandoned);
    assert_eq!(store.used_frames(), 0);
    let all_metadata = writer.reserve_metadata(4096).unwrap();
    assert_eq!(all_metadata.bytes(), 4096);
    drop(all_metadata);
    writer.append(b"still open").unwrap();
}

#[test]
fn external_metadata_and_canonical_growth_share_quota_in_both_directions() {
    for external_first in [false, true] {
        let (_dir, store) = store(4, 4);
        let mut writer = store.create().unwrap();
        let mut reader = store.reader(writer.id(), 0).unwrap();
        let remaining = 4096 - std::mem::size_of::<Reader>();
        if external_first {
            let _permit = writer.reserve_metadata(remaining).unwrap();
            assert!(matches!(writer.append(b"record"), Err(Error::StorageExhausted)));
            assert_eq!(store.used_frames(), 0);
        } else {
            let published = writer.append(b"record").unwrap();
            assert!(matches!(
                writer.reserve_metadata(remaining),
                Err(Error::StorageExhausted)
            ));
            assert_eq!(record(&mut reader).chunk(published.start).unwrap(), b"record");
        }
        terminal(&mut reader, Terminal::StorageExhausted);
    }
}

#[test]
fn external_metadata_and_readers_share_quota_in_both_directions() {
    for external_first in [false, true] {
        let (_dir, store) = store(4, 4);
        let mut writer = store.create().unwrap();
        let mut reader = store.reader(writer.id(), 0).unwrap();
        let reader_bytes = std::mem::size_of::<Reader>();
        if external_first {
            let _permit = writer.reserve_metadata(4096 - reader_bytes).unwrap();
            assert!(matches!(store.reader(writer.id(), 0), Err(Error::StorageExhausted)));
        } else {
            let _second_reader = store.reader(writer.id(), 0).unwrap();
            assert!(matches!(
                writer.reserve_metadata(4096 - reader_bytes),
                Err(Error::StorageExhausted)
            ));
        }
        terminal(&mut reader, Terminal::StorageExhausted);
    }
}

#[test]
fn metadata_resize_is_atomic_and_drop_or_shrink_restores_capacity() {
    let (_dir, store) = store(4, 4);
    let mut writer = store.create().unwrap();
    let mut permit = writer.reserve_metadata(4096).unwrap();
    permit.resize(1024).unwrap();
    let mut other = writer.reserve_metadata(3072).unwrap();
    drop(permit);
    other.resize(4096).unwrap();
    other.resize(0).unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let published = writer.append(b"prefix").unwrap();
    other.resize(1024).unwrap();
    assert!(matches!(other.resize(usize::MAX), Err(Error::StorageExhausted)));
    assert_eq!(other.bytes(), 1024);
    other.resize(512).unwrap();
    assert_eq!(other.bytes(), 512);
    assert_eq!(record(&mut reader).chunk(published.start).unwrap(), b"prefix");
    terminal(&mut reader, Terminal::StorageExhausted);
    drop(other);
}

#[test]
fn scratch_metadata_is_charged_and_external_permits_reject_invalidated_growth() {
    let (_dir, store) = store(4, 4);
    let mut writer = store.create().unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let source = scratch(&mut writer, 1, 1);
    assert!(matches!(
        writer.reserve_metadata(4096 - std::mem::size_of::<Reader>()),
        Err(Error::StorageExhausted)
    ));
    terminal(&mut reader, Terminal::StorageExhausted);
    drop(source);

    let mut writer = store.create().unwrap();
    let mut permit = writer.reserve_metadata(128).unwrap();
    writer.finish(Terminal::Gone);
    assert!(matches!(permit.resize(129), Err(Error::Gone)));
    assert_eq!(permit.bytes(), 128);
    permit.resize(0).unwrap();
    assert!(matches!(writer.reserve_metadata(0), Err(Error::Gone)));
    assert!(matches!(writer.begin_scratch(1), Err(Error::Gone)));
}

#[test]
fn multi_frame_leases_stay_charged_after_their_reader_drops() {
    let (_dir, store) = store(4, 4);
    let mut writer = store.create().unwrap();
    writer.append(&[7; FRAME_BYTES + 1]).unwrap();
    // Each lease keeps its two-frame list after its reader drops, so attaching again must
    // eventually exhaust the ceiling instead of growing uncharged heap.
    let mut leases = Vec::new();
    loop {
        assert!(leases.len() < 4096 / 8, "leases outgrew the metadata ceiling");
        let Ok(mut reader) = store.reader(writer.id(), 0) else {
            break;
        };
        match reader.try_next() {
            Poll::Ready(Some(Read::Record(lease))) => leases.push(lease),
            Poll::Ready(Some(Read::Terminal(Terminal::StorageExhausted))) => break,
            _ => panic!("expected a record or storage exhaustion"),
        }
    }
    drop(leases);
    let mut reader = store.reader(writer.id(), 0).unwrap();
    assert_eq!(record(&mut reader).record().len(), FRAME_BYTES as u64 + 1);
}

#[test]
fn lease_metadata_exhaustion_fails_the_stream_once() {
    let (_dir, store) = store(4, 4);
    let mut writer = store.create().unwrap();
    writer.append(&[7; FRAME_BYTES + 1]).unwrap();
    let mut spare = store.reader(writer.id(), 0).unwrap();
    // Each two-frame lease charges its frame list; a reader's own charge ends when it drops,
    // so the ceiling is reached by a lease, never by attaching.
    let mut leases = Vec::new();
    let _exhausted = loop {
        assert!(leases.len() < 4096 / 8, "leases outgrew the metadata ceiling");
        let mut reader = store.reader(writer.id(), 0).unwrap();
        match reader.try_next() {
            Poll::Ready(Some(Read::Record(lease))) => leases.push(lease),
            Poll::Ready(Some(Read::Terminal(Terminal::StorageExhausted))) => break reader,
            _ => panic!("expected a record or storage exhaustion"),
        }
    };
    assert!(matches!(writer.append(b"after"), Err(Error::Closed)));
    assert_eq!(writer.storage_failure_frames(), Some(2));
    // A second reader meeting the same ceiling does not count the failure again.
    terminal(&mut spare, Terminal::StorageExhausted);
    let stats = store.stats();
    assert_eq!(
        (stats.allocation_failures, stats.last_storage_exhausted_frames),
        (1, Some(2))
    );
}
