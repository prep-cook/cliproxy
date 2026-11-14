use ck_stream::{Config, Error, FRAME_BYTES, ManualClock, Read, Store, Terminal};
use std::{sync::Arc, task::Poll, time::Duration};

fn store(frames: usize) -> (tempfile::TempDir, Store, Arc<ManualClock>) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path().join("pool"));
    config.pool_bytes = Some(frames * FRAME_BYTES);
    config.max_event_bytes = 4 * FRAME_BYTES;
    config.header_slots = 32;
    config.workers = 2;
    let clock = Arc::new(ManualClock::default());
    let store = Store::with_clock(config, clock.clone()).unwrap();
    (dir, store, clock)
}

fn lease(reader: &mut ck_stream::Reader) -> ck_stream::Lease {
    match reader.try_next() {
        Poll::Ready(Some(Read::Record(lease))) => lease,
        _ => panic!("expected record"),
    }
}

fn terminal(reader: &mut ck_stream::Reader, expected: Terminal) {
    match reader.try_next() {
        Poll::Ready(Some(Read::Terminal(actual))) => assert_eq!(actual, expected),
        _ => panic!("expected terminal"),
    }
    assert!(matches!(reader.try_next(), Poll::Ready(None)));
}

#[test]
fn incomplete_record_is_invisible_and_abandoned_frames_are_reusable() {
    let (_dir, store, _) = store(4);
    let mut writer = store.create().unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    {
        let mut record = writer.begin_record(FRAME_BYTES + 8, false, 1, 2).unwrap();
        record.write(b"data: ").unwrap();
        assert!(matches!(reader.try_next(), Poll::Pending));
        assert!(matches!(record.commit(), Err(Error::IncompleteRecord)));
    }
    assert_eq!(store.used_frames(), 0);
    writer.append(b"data: complete\n\n").unwrap();
    let read = lease(&mut reader);
    assert_eq!(read.chunk(read.record().start).unwrap(), b"data: complete\n\n");
    writer.finish(Terminal::Done);
    terminal(&mut reader, Terminal::Done);
}

#[test]
fn incomplete_commit_keeps_the_reservation_writable() {
    let (_dir, store, _) = store(4);
    let mut writer = store.create().unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let mut record = writer.begin_record(12, false, 0, 0).unwrap();
    record.write(b"data: ").unwrap();
    assert!(matches!(record.commit(), Err(Error::IncompleteRecord)));
    record.write(b"done\n\n").unwrap();
    let committed = record.commit().unwrap();
    assert!(matches!(record.commit(), Err(Error::Closed)));
    assert!(matches!(record.write(b""), Err(Error::Closed)));
    drop(record);
    let read = lease(&mut reader);
    assert_eq!(read.record(), committed);
    assert_eq!(read.chunk(committed.start).unwrap(), b"data: done\n\n");
    assert!(matches!(reader.try_next(), Poll::Pending));
}

#[test]
fn cross_frame_record_and_aligned_padding_are_not_mixed() {
    let (_dir, store, _) = store(8);
    let mut writer = store.create().unwrap();
    writer.append(&vec![b'a'; FRAME_BYTES - 3]).unwrap();
    let mut record = writer.begin_record(9, false, 4, 7).unwrap();
    record.write(b"abc").unwrap();
    record.write(b"defghi").unwrap();
    record.commit().unwrap();
    drop(record);
    let mut reservation = writer.begin_record(4, true, 5, 8).unwrap();
    reservation.write(b"last").unwrap();
    let aligned = reservation.commit().unwrap();
    drop(reservation);
    assert_eq!(aligned.start.offset, 0);
    let mut reader = store.reader(writer.id(), 1).unwrap();
    let read = lease(&mut reader);
    let mut bytes = Vec::new();
    read.visit(|span| bytes.extend_from_slice(span)).unwrap();
    assert_eq!(bytes, b"abcdefghi");
    assert_eq!(read.record().kind, 4);
    let last = lease(&mut reader);
    assert_eq!(last.chunk(last.record().start).unwrap(), b"last");
    let mut stale = last.record().start;
    stale.generation += 1;
    assert!(matches!(last.chunk(stale), Err(Error::InvalidPosition)));
}

#[test]
fn disconnect_preserves_complete_prefix_and_terminal_for_resume() {
    let (_dir, store, _) = store(8);
    let mut writer = store.create().unwrap();
    writer.append(b"first").unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    lease(&mut reader).acknowledge();
    drop(reader);
    writer.append(b"second").unwrap();
    writer.finish(Terminal::UpstreamError);
    let mut resumed = store.reader(writer.id(), 1).unwrap();
    let read = lease(&mut resumed);
    assert_eq!(read.chunk(read.record().start).unwrap(), b"second");
    terminal(&mut resumed, Terminal::UpstreamError);
    assert!(matches!(store.reader(writer.id(), 3), Err(Error::InvalidPosition)));
}

#[test]
fn attached_readers_refresh_ttl_but_writes_do_not_and_disconnected_streams_expire() {
    let (_dir, store, clock) = store(8);
    let mut read_writer = store.create().unwrap();
    let mut unread = store.create().unwrap();
    read_writer.append(b"read").unwrap();
    unread.append(b"unread").unwrap();
    clock.advance(Duration::from_secs(3599));
    store.tick().unwrap();
    let mut reader = store.reader(read_writer.id(), 0).unwrap();
    lease(&mut reader).acknowledge();
    unread.append(b"write is not read").unwrap();
    clock.advance(Duration::from_secs(2));
    store.tick().unwrap();
    assert!(matches!(store.reader(unread.id(), 0), Err(Error::Gone)));
    clock.advance(Duration::from_secs(7200));
    store.tick().unwrap();
    assert!(matches!(reader.try_next(), Poll::Pending));
    drop(reader);
    clock.advance(Duration::from_secs(3599));
    store.tick().unwrap();
    // Attach itself restarts the expiry clock, even without acquiring a record.
    let reader = store.reader(read_writer.id(), 1).unwrap();
    drop(reader);
    clock.advance(Duration::from_secs(2));
    store.tick().unwrap();
    let mut reader = store.reader(read_writer.id(), 1).unwrap();
    clock.advance(Duration::from_secs(3599));
    assert!(matches!(reader.try_next(), Poll::Pending));
    drop(reader);
    clock.advance(Duration::from_secs(3601));
    store.tick().unwrap();
    assert!(matches!(store.reader(read_writer.id(), 0), Err(Error::Gone)));
}

#[test]
fn pressure_evicts_oldest_read_not_oldest_created_and_never_reuses_a_lease() {
    let (_dir, store, clock) = store(4);
    let mut first = store.create().unwrap();
    first.append(&vec![1; FRAME_BYTES]).unwrap();
    clock.advance(Duration::from_secs(1));
    let mut second = store.create().unwrap();
    second.append(&vec![2; FRAME_BYTES]).unwrap();
    let mut second_reader = store.reader(second.id(), 0).unwrap();
    let pinned = lease(&mut second_reader);
    let borrowed = pinned.chunk(pinned.record().start).unwrap();
    clock.advance(Duration::from_secs(1));
    let mut first_reader = store.reader(first.id(), 0).unwrap();
    lease(&mut first_reader).acknowledge();
    let mut allocating = store.create().unwrap();
    allocating.append(&vec![3; 3 * FRAME_BYTES]).unwrap();
    // second was read least recently, but its lease means eviction would return nothing.
    assert!(matches!(store.reader(first.id(), 0), Err(Error::Gone)));
    assert_eq!(borrowed, vec![2; FRAME_BYTES]);
    assert_eq!(pinned.chunk(pinned.record().start).unwrap(), borrowed);
    drop(pinned);
    assert!(matches!(second_reader.try_next(), Poll::Pending));
    assert_eq!(store.used_frames(), 4);
}

#[test]
fn pinned_capacity_fails_visibly_without_overwrite_and_keeps_prefix() {
    let (_dir, store, _) = store(4);
    let mut first = store.create().unwrap();
    first.append(&vec![9; 4 * FRAME_BYTES]).unwrap();
    let mut reader = store.reader(first.id(), 0).unwrap();
    let pinned = lease(&mut reader);
    let bytes = pinned.chunk(pinned.record().start).unwrap();
    let mut second = store.create().unwrap();
    let mut failed_reader = store.reader(second.id(), 0).unwrap();
    assert!(matches!(second.append(b"cannot fit"), Err(Error::StorageExhausted)));
    terminal(&mut failed_reader, Terminal::StorageExhausted);
    // The pinned stream would return no frame, so pressure leaves it readable.
    assert!(matches!(reader.try_next(), Poll::Pending));
    assert_eq!(bytes, vec![9; FRAME_BYTES]);
    drop(pinned);
    assert_eq!(store.used_frames(), 4);
    first.finish(Terminal::Done);
    terminal(&mut reader, Terminal::Done);
}

#[test]
fn per_stream_quota_does_not_evict_others_and_error_follows_prefix() {
    let (_dir, store, _) = store(4);
    let mut writer = store.create().unwrap();
    writer.append(&vec![7; 4 * FRAME_BYTES]).unwrap();
    assert!(matches!(writer.append(b"over quota"), Err(Error::StorageExhausted)));
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let read = lease(&mut reader);
    let mut count = 0;
    read.visit(|span| {
        assert!(span.iter().all(|b| *b == 7));
        count += span.len();
    })
    .unwrap();
    assert_eq!(count, 4 * FRAME_BYTES);
    terminal(&mut reader, Terminal::StorageExhausted);
}

#[test]
fn pressure_never_invalidates_streams_that_free_nothing() {
    let (_dir, store, _) = store(4);
    let mut owner = store.create().unwrap();
    let scratch = owner.begin_scratch(4 * FRAME_BYTES).unwrap();
    let mut waiting = store.create().unwrap();
    let mut allocating = store.create().unwrap();
    assert!(matches!(allocating.append(&[1; 10]), Err(Error::StorageExhausted)));
    assert!(store.reader(owner.id(), 0).is_ok());
    assert!(store.reader(waiting.id(), 0).is_ok());
    drop(scratch);
    waiting.append(&[2; 10]).unwrap();
}

#[test]
fn pressure_skips_leased_victims_until_their_frames_can_return() {
    let (_dir, store, _) = store(4);
    let mut pinned = store.create().unwrap();
    for _ in 0..4 {
        pinned.append(&[7; FRAME_BYTES]).unwrap();
    }
    let mut reader = store.reader(pinned.id(), 0).unwrap();
    let leases: Vec<_> = (0..4).map(|_| lease(&mut reader)).collect();
    let mut waiting = store.create().unwrap();
    let mut allocating = store.create().unwrap();
    assert!(matches!(allocating.append(&[1; 10]), Err(Error::StorageExhausted)));
    assert!(store.reader(waiting.id(), 0).is_ok());
    assert_eq!(leases[3].chunk(leases[3].record().start).unwrap(), &[7; FRAME_BYTES]);
    drop(leases);
    // Without leases the least recently read stream returns its frames and is evicted.
    waiting.append(&[2; 10]).unwrap();
    terminal(&mut reader, Terminal::Gone);
}

#[test]
fn pressure_fails_without_eviction_when_victims_cannot_cover_the_reservation() {
    let (_dir, store, _) = store(4);
    let mut pinned = store.create().unwrap();
    pinned.append(&[7; 3 * FRAME_BYTES]).unwrap();
    let mut reader = store.reader(pinned.id(), 0).unwrap();
    let pin = lease(&mut reader);
    let mut small = store.create().unwrap();
    small.append(&[8; 10]).unwrap();
    let mut allocating = store.create().unwrap();
    assert!(matches!(
        allocating.begin_scratch(2 * FRAME_BYTES),
        Err(Error::StorageExhausted)
    ));
    assert!(store.reader(small.id(), 0).is_ok());
    assert!(pin.chunk(pin.record().start).is_ok());
    assert_eq!(store.used_frames(), 4);
}

#[test]
fn header_slot_exhaustion_evicts_the_least_recently_read_stream() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path().join("pool"));
    config.pool_bytes = Some(64 * FRAME_BYTES);
    config.max_event_bytes = FRAME_BYTES;
    config.workers = 1;
    assert_eq!(config.header_slots, 4096);
    let clock = Arc::new(ManualClock::default());
    let store = Store::with_clock(config, clock.clone()).unwrap();
    let finished = || {
        let mut writer = store.create().unwrap();
        writer.finish(Terminal::Done);
        writer.id()
    };
    let first = finished();
    let mut second = store.create().unwrap();
    let kept = second.append(b"kept").unwrap();
    let mut second_reader = store.reader(second.id(), 0).unwrap();
    let rest: Vec<_> = (2..4096).map(|_| finished()).collect();
    clock.advance(Duration::from_secs(1));
    drop(store.reader(first, 0).unwrap());
    let mut admitted = store.create().unwrap();
    admitted.append(b"reuses the freed header slot").unwrap();
    terminal(&mut second_reader, Terminal::Gone);
    assert_eq!(second.committed(), kept.end);
    assert!(matches!(store.reader(second.id(), 0), Err(Error::Gone)));
    assert!(store.reader(first, 0).is_ok());
    assert!(store.reader(rest[0], 0).is_ok());
}

#[tokio::test]
async fn notification_registration_cannot_lose_publication_or_cancellation() {
    let (_dir, store, _) = store(8);
    let mut writer = store.create().unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let task = tokio::spawn(async move {
        for expected in 0..128u8 {
            let Some(Read::Record(read)) = reader.next().await else {
                panic!("missing record")
            };
            assert_eq!(read.chunk(read.record().start).unwrap(), &[expected]);
        }
        assert!(matches!(
            reader.next().await,
            Some(Read::Terminal(Terminal::ProducerLost))
        ));
        assert!(reader.next().await.is_none());
    });
    for byte in 0..128u8 {
        writer.append(&[byte]).unwrap();
        tokio::task::yield_now().await;
    }
    drop(writer);
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn shutdown_preserves_mapping_until_last_borrow_and_refuses_admission() {
    let (_dir, mut store, _) = store(8);
    let mut writer = store.create().unwrap();
    writer.append(b"retained").unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let pinned = lease(&mut reader);
    let bytes = pinned.chunk(pinned.record().start).unwrap();
    store.shutdown().unwrap();
    assert!(matches!(store.create(), Err(Error::Closed)));
    terminal(&mut reader, Terminal::Shutdown);
    drop(store);
    drop(writer);
    drop(reader);
    assert_eq!(bytes, b"retained");
}

#[test]
fn releasing_scratch_reuses_capacity_without_recycling_logical_positions() {
    let (_dir, store, _) = store(4);
    let mut writer = store.create().unwrap();
    let first = writer.append(&vec![1; 4 * FRAME_BYTES]).unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let pinned = lease(&mut reader);
    writer.release_before(first.end).unwrap();
    assert_eq!(store.used_frames(), 4);
    drop(pinned);
    assert_eq!(store.used_frames(), 0);
    writer.release_before(first.end).unwrap();
    let next = writer.append(b"next").unwrap();
    assert_eq!(next.start.frame, 4);
    assert_eq!(next.sequence, 1);
    assert!(matches!(store.reader(writer.id(), 0), Err(Error::Gone)));
}

#[test]
fn partial_scratch_release_cannot_destroy_a_retained_record() {
    let (_dir, store, _) = store(4);
    let mut writer = store.create().unwrap();
    let record = writer.append(&vec![4; FRAME_BYTES * 2]).unwrap();
    let mut middle = record.start;
    middle.frame = 1;
    writer.release_before(middle).unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let read = lease(&mut reader);
    let mut length = 0;
    read.visit(|span| {
        assert!(span.iter().all(|b| *b == 4));
        length += span.len();
    })
    .unwrap();
    assert_eq!(length, FRAME_BYTES * 2);
}

#[test]
fn positions_from_another_pool_are_rejected() {
    let (_first_dir, first_store, _) = store(4);
    let mut first = first_store.create().unwrap();
    let position = first.append(b"old").unwrap().start;
    let (_second_dir, second_store, _) = store(4);
    let mut second = second_store.create().unwrap();
    second.append(b"new").unwrap();
    let mut reader = second_store.reader(second.id(), 0).unwrap();
    assert!(matches!(
        lease(&mut reader).chunk(position),
        Err(Error::InvalidPosition)
    ));
}

#[test]
fn one_mib_of_small_records_replays_with_default_metadata_budget() {
    let (_dir, store, _) = store(32);
    let mut writer = store.create().unwrap();
    for sequence in 0..16_384u32 {
        let payload = [sequence as u8; 64];
        let mut record = writer.begin_record(64, false, sequence % 7, sequence).unwrap();
        record.write(&payload).unwrap();
        record.commit().unwrap();
    }
    writer.finish(Terminal::Done);
    let mut reader = store.reader(writer.id(), 0).unwrap();
    for sequence in 0..16_384u32 {
        let read = lease(&mut reader);
        let record = read.record();
        assert_eq!(record.sequence, u64::from(sequence));
        assert_eq!((record.kind, record.correlation), (sequence % 7, sequence));
        let mut count = 0;
        read.visit(|bytes| {
            assert!(bytes.iter().all(|byte| *byte == sequence as u8));
            count += bytes.len();
        })
        .unwrap();
        assert_eq!(count, 64);
    }
    terminal(&mut reader, Terminal::Done);
}

#[test]
fn invalid_record_parameters_do_not_terminate_a_valid_prefix() {
    let (_dir, store, _) = store(8);
    let mut writer = store.create().unwrap();
    writer.append(b"prefix").unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    drop(lease(&mut reader));
    for (length, aligned) in [(0, false), (4 * FRAME_BYTES + 1, false), (FRAME_BYTES + 1, true)] {
        assert!(matches!(
            writer.begin_record(length, aligned, 0, 0),
            Err(Error::InvalidRecord)
        ));
        assert!(matches!(reader.try_next(), Poll::Pending));
    }
    writer.append(b"suffix").unwrap();
    writer.finish(Terminal::Done);
    let read = lease(&mut reader);
    assert_eq!(read.chunk(read.record().start).unwrap(), b"suffix");
    let mut replay = store.reader(writer.id(), 0).unwrap();
    let read = lease(&mut replay);
    assert_eq!(read.chunk(read.record().start).unwrap(), b"prefix");
    terminal(&mut reader, Terminal::Done);
}
