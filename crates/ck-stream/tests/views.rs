use std::{ops::Range, sync::Arc, task::Poll, time::Duration};

use ck_stream::{
    ByteSpan, Config, Error, EventView, FRAME_BYTES, Lease, ManualClock, Position, Read, RequestFieldView, Store,
};

fn store() -> (tempfile::TempDir, Store, Arc<ManualClock>) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path().join("pool"));
    config.pool_bytes = Some(8 * FRAME_BYTES);
    config.max_event_bytes = 4 * FRAME_BYTES;
    config.header_slots = 8;
    config.workers = 1;
    let clock = Arc::new(ManualClock::default());
    let store = Store::with_clock(config, clock.clone()).unwrap();
    (dir, store, clock)
}

fn lease(reader: &mut ck_stream::Reader) -> Lease {
    match reader.try_next() {
        Poll::Ready(Some(Read::Record(lease))) => lease,
        _ => panic!("expected published record"),
    }
}

fn at(start: Position, offset: usize) -> Position {
    let byte = start.frame * FRAME_BYTES as u64 + u64::from(start.offset) + offset as u64;
    Position {
        generation: start.generation,
        frame: byte / FRAME_BYTES as u64,
        offset: (byte % FRAME_BYTES as u64) as u32,
    }
}

fn range(lease: &Lease, offsets: Range<usize>) -> Range<Position> {
    let start = lease.record().start;
    at(start, offsets.start)..at(start, offsets.end)
}

fn bytes(span: ByteSpan<'_>) -> Vec<u8> {
    let mut bytes = Vec::new();
    span.visit(|chunk| bytes.extend_from_slice(chunk)).unwrap();
    bytes
}

#[test]
fn request_pointer_and_value_cross_frames_without_sse_delimiters() {
    let (_dir, store, _) = store();
    let mut writer = store.create().unwrap();
    let prefix = b"event: x_ck.request.field\ndata: {\"pointer\":\"";
    let pointer = b"/messages/0/content";
    let separator = b"\",\"value\":";
    let mut value = vec![b'v'; FRAME_BYTES + 2];
    value[0] = b'"';
    *value.last_mut().unwrap() = b'"';
    let mut record = prefix.to_vec();
    let pointer_start = record.len();
    record.extend_from_slice(pointer);
    record.extend_from_slice(separator);
    let value_start = record.len();
    record.extend_from_slice(&value);
    record.extend_from_slice(b"}\n\n");
    writer.append(&vec![b'p'; FRAME_BYTES - 3 - pointer_start]).unwrap();
    writer.append(&record).unwrap();
    let mut reader = store.reader(writer.id(), 1).unwrap();
    let lease = lease(&mut reader);
    let event = EventView::new(
        &lease,
        Some(range(&lease, 7..7 + b"x_ck.request.field".len())),
        range(&lease, b"event: x_ck.request.field\ndata: ".len()..record.len() - 2),
    )
    .unwrap();
    let field = RequestFieldView::new(
        &lease,
        range(&lease, pointer_start..pointer_start + pointer.len()),
        range(&lease, value_start..value_start + value.len()),
    )
    .unwrap();

    assert_eq!(bytes(event.name().unwrap()), b"x_ck.request.field");
    assert_eq!(
        bytes(event.data()),
        record[b"event: x_ck.request.field\ndata: ".len()..record.len() - 2]
    );
    assert_eq!(bytes(field.pointer()), pointer);
    assert_eq!(bytes(field.value()), value);
    assert_eq!(field.pointer().chunk(field.pointer().start()).unwrap(), b"/me");
    assert_ne!(field.value().start().frame, field.value().end().frame);
}

#[test]
fn chunks_clip_both_field_boundaries_and_allow_interior_reads() {
    let (_dir, store, _) = store();
    let mut writer = store.create().unwrap();
    writer.append(b"earlier").unwrap();
    writer.append(b"prefixVALUEsuffix").unwrap();
    writer.append(b"later").unwrap();
    let mut reader = store.reader(writer.id(), 1).unwrap();
    let lease = lease(&mut reader);
    let span = ByteSpan::new(&lease, range(&lease, 6..11)).unwrap();
    assert_eq!(span.len(), 5);
    assert!(!span.is_empty());
    assert_eq!(span.start(), at(lease.record().start, 6));
    assert_eq!(span.end(), at(lease.record().start, 11));
    assert_eq!(span.chunk(span.start()).unwrap(), b"VALUE");
    assert_eq!(span.chunk(at(span.start(), 2)).unwrap(), b"LUE");
    assert_eq!(bytes(span), b"VALUE");
    assert!(matches!(span.chunk(lease.record().start), Err(Error::InvalidPosition)));
    assert!(matches!(span.chunk(span.end()), Err(Error::InvalidPosition)));
}

#[test]
fn rejects_wrong_generation_noncanonical_overflowing_and_outside_positions() {
    let (_dir, store, _) = store();
    let mut writer = store.create().unwrap();
    writer.append(b"before").unwrap();
    writer.append(b"record").unwrap();
    writer.append(b"after").unwrap();
    let mut reader = store.reader(writer.id(), 1).unwrap();
    let lease = lease(&mut reader);
    let valid = lease.record().start..lease.record().end;
    let mut foreign = valid.start;
    foreign.generation += 1;
    let mut noncanonical = valid.start;
    noncanonical.offset = FRAME_BYTES as u32;
    let mut overflowing = valid.start;
    overflowing.frame = u64::MAX;
    let mut before = valid.start;
    before.offset -= 1;
    let after = at(valid.end, 1);
    let reversed = valid.end..valid.start;
    for invalid in [
        foreign..valid.end,
        valid.start..foreign,
        noncanonical..valid.end,
        valid.start..noncanonical,
        overflowing..overflowing,
        reversed,
        before..valid.end,
        valid.start..after,
        before..before,
        after..after,
    ] {
        assert!(matches!(ByteSpan::new(&lease, invalid), Err(Error::InvalidPosition)));
    }
    let span = ByteSpan::new(&lease, valid.clone()).unwrap();
    assert!(matches!(span.chunk(foreign), Err(Error::InvalidPosition)));
    assert!(matches!(span.chunk(noncanonical), Err(Error::InvalidPosition)));
    assert!(matches!(span.chunk(overflowing), Err(Error::InvalidPosition)));
    assert!(matches!(
        EventView::new(&lease, Some(foreign..foreign), valid.clone()),
        Err(Error::InvalidPosition)
    ));
    assert!(matches!(
        EventView::new(&lease, None, valid.start..after),
        Err(Error::InvalidPosition)
    ));
    assert!(matches!(
        RequestFieldView::new(&lease, before..valid.start, valid.clone()),
        Err(Error::InvalidPosition)
    ));
    assert!(matches!(
        RequestFieldView::new(&lease, valid.clone(), valid.start..after),
        Err(Error::InvalidPosition)
    ));
}

#[test]
fn empty_spans_at_start_interior_and_exclusive_frame_end_are_safe() {
    let (_dir, store, _) = store();
    let mut writer = store.create().unwrap();
    writer.append(&vec![b'x'; FRAME_BYTES]).unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let lease = lease(&mut reader);
    assert_eq!(lease.record().end.offset, 0);
    for position in [lease.record().start, at(lease.record().start, 3), lease.record().end] {
        let span = ByteSpan::new(&lease, position..position).unwrap();
        assert!(span.is_empty());
        assert_eq!(span.len(), 0);
        assert_eq!(span.chunk(position).unwrap(), b"");
        span.visit(|_| panic!("empty spans must not visit bytes")).unwrap();
    }
    let end = lease.record().end;
    let event = EventView::new(&lease, None, end..end).unwrap();
    assert!(event.name().is_none());
    assert!(event.data().is_empty());
    let field = RequestFieldView::new(&lease, end..end, end..end).unwrap();
    assert_eq!(bytes(field.pointer()), b"");
    assert_eq!(bytes(field.value()), b"");
}

#[test]
fn invalidation_rejects_new_reads_including_empty_spans_but_preserves_borrows() {
    let (_dir, store, clock) = store();
    let mut writer = store.create().unwrap();
    writer.append(b"/pathVALUE").unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let lease = lease(&mut reader);
    drop(reader);
    let event = EventView::new(&lease, None, range(&lease, 0..10)).unwrap();
    let field = RequestFieldView::new(&lease, range(&lease, 0..5), range(&lease, 5..10)).unwrap();
    let end = lease.record().end;
    let empty = ByteSpan::new(&lease, end..end).unwrap();
    let borrowed = field.value().chunk(field.value().start()).unwrap();
    clock.advance(Duration::from_secs(3601));
    store.tick().unwrap();
    assert_eq!(borrowed, b"VALUE");
    for span in [event.data(), field.pointer(), field.value(), empty] {
        assert!(matches!(span.chunk(span.start()), Err(Error::Gone)));
        assert!(matches!(
            span.visit(|_| panic!("invalidated span exposed bytes")),
            Err(Error::Gone)
        ));
    }
    assert!(matches!(ByteSpan::new(&lease, end..end), Err(Error::Gone)));
    assert!(matches!(ByteSpan::new(&lease, range(&lease, 0..1)), Err(Error::Gone)));
}

#[test]
fn invalidation_between_frame_visits_stops_before_the_next_chunk() {
    let (_dir, store, clock) = store();
    let mut writer = store.create().unwrap();
    writer.append(&vec![b'x'; FRAME_BYTES + 1]).unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let lease = lease(&mut reader);
    drop(reader);
    let span = ByteSpan::new(&lease, lease.record().start..lease.record().end).unwrap();
    let mut calls = 0;
    let result = span.visit(|chunk| {
        calls += 1;
        assert_eq!(chunk, &[b'x'; FRAME_BYTES]);
        clock.advance(Duration::from_secs(3601));
        store.tick().unwrap();
    });
    assert!(matches!(result, Err(Error::Gone)));
    assert_eq!(calls, 1);
}

#[test]
fn lease_visits_check_invalidation_before_each_frame() {
    let (_dir, store, clock) = store();
    let mut writer = store.create().unwrap();
    writer.append(b"/pathVALUE").unwrap();
    writer.append(&vec![b'x'; FRAME_BYTES + 1]).unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let small = lease(&mut reader);
    let large = lease(&mut reader);
    drop(reader);
    let mut seen = Vec::new();
    small.visit(|chunk| seen.extend_from_slice(chunk)).unwrap();
    assert_eq!(seen, b"/pathVALUE");
    let mut calls = 0;
    let result = large.visit(|chunk| {
        calls += 1;
        assert_eq!(chunk, &[b'x'; FRAME_BYTES - 10]);
        clock.advance(Duration::from_secs(3601));
        store.tick().unwrap();
    });
    assert!(matches!(result, Err(Error::Gone)));
    assert_eq!(calls, 1);
    assert!(matches!(
        small.visit(|_| panic!("invalidated lease exposed bytes")),
        Err(Error::Gone)
    ));
}

#[test]
fn views_keep_using_the_borrowed_lease_after_other_owners_drop() {
    let (_dir, store, _) = store();
    let mut writer = store.create().unwrap();
    writer.append(b"/pathVALUE").unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let lease = lease(&mut reader);
    let field = RequestFieldView::new(&lease, range(&lease, 0..5), range(&lease, 5..10)).unwrap();
    drop(reader);
    drop(writer);
    drop(store);
    assert_eq!(bytes(field.pointer()), b"/path");
    assert_eq!(bytes(field.value()), b"VALUE");
}
