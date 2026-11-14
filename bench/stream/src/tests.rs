use super::*;

#[test]
fn pattern_rejects_stale_shifted_and_foreign_bytes() {
    let expected = pattern(3, FRAME_BYTES, 4096).to_vec();
    for (id, offset) in [(3, 0), (3, 2 * FRAME_BYTES), (4, FRAME_BYTES), (3, FRAME_BYTES + 64)] {
        let mut check = Check::new(id);
        check.offset = offset;
        check.visit(&expected);
        assert!(!check.ok, "stream {id} offset {offset} accepted foreign bytes");
    }
    let mut check = Check::new(3);
    check.offset = FRAME_BYTES;
    check.visit(&expected);
    assert!(check.ok);
}

#[test]
fn heap_delivers_every_record_byte_exactly() {
    let store = heap::Store::default();
    let mut writer = store.create();
    let id = writer.id();
    let mut reader = store.reader(id).unwrap();
    // Packed records cross frame boundaries, a length change starts a new run,
    // and aligned records skip the frame tail.
    let mut sizes = vec![64; 2000];
    sizes.extend([100, 64, 64]);
    sizes.extend([49152; 3]);
    let mut written = 0;
    for &size in &sizes {
        writer.append(pattern(id, written, size), size == 49152).unwrap();
        written += size;
    }
    writer.finish();
    let mut check = Check::new(id);
    let mut lengths = Vec::new();
    while let Poll::Ready(Some(lease)) = reader.try_next() {
        let before = check.offset;
        lease.visit(|bytes| check.visit(bytes));
        lengths.push(check.offset - before);
    }
    assert_eq!(lengths, sizes);
    check.finish(written).unwrap();
}

#[test]
fn heap_readers_follow_concurrent_publication_and_frame_growth() {
    let store = heap::Store::default();
    let mut writer = store.create();
    let id = writer.id();
    let readers: Vec<_> = (0..4).map(|_| store.reader(id).unwrap()).collect();
    // Aligned records add a frame each; packed ones cross frame boundaries and change runs.
    let sizes: Vec<usize> = (0..3000)
        .map(|i| match i % 7 {
            0 => 49152,
            1 => 1,
            n => 64 * n + 13,
        })
        .collect();
    let total: usize = sizes.iter().sum();
    std::thread::scope(|scope| {
        let handles: Vec<_> = readers
            .into_iter()
            .map(|mut reader| {
                scope.spawn(move || {
                    let mut check = Check::new(id);
                    let mut lengths = Vec::new();
                    loop {
                        match reader.try_next() {
                            Poll::Ready(Some(lease)) => {
                                let before = check.offset;
                                lease.visit(|bytes| check.visit(bytes));
                                lengths.push(check.offset - before);
                            }
                            Poll::Ready(None) => break,
                            Poll::Pending => std::thread::yield_now(),
                        }
                    }
                    (check, lengths)
                })
            })
            .collect();
        let mut written = 0;
        for &size in &sizes {
            writer.append(pattern(id, written, size), size == 49152).unwrap();
            written += size;
        }
        writer.finish();
        for handle in handles {
            let (check, lengths) = handle.join().unwrap();
            assert_eq!(lengths, sizes);
            check.finish(total).unwrap();
        }
    });
}
