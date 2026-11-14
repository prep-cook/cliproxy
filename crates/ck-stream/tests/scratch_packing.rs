use ck_stream::{Config, Error, FRAME_BYTES, ScratchLease, Store, Writer};

const METADATA: usize = 2 * 1024 * 1024;

fn store(pool_frames: usize, max_frames: u32) -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path().join("pool"));
    config.pool_bytes = Some(pool_frames * FRAME_BYTES);
    config.max_frames = Some(max_frames);
    config.max_event_bytes = FRAME_BYTES;
    config.max_metadata_bytes = METADATA;
    config.header_slots = 16;
    config.workers = 2;
    let store = Store::open(config).unwrap();
    (dir, store)
}

fn lease(writer: &mut Writer, bytes: &[u8]) -> Result<ScratchLease, Error> {
    let mut reservation = writer.begin_scratch(bytes.len())?;
    reservation.write(bytes)?;
    reservation.complete()
}

/// Reassemble a lease from its chunks, which never cross a frame boundary.
fn contents(lease: &ScratchLease) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(lease.len());
    while bytes.len() < lease.len() {
        bytes.extend_from_slice(lease.chunk(bytes.len()).unwrap());
    }
    assert!(matches!(lease.chunk(lease.len()), Err(Error::InvalidPosition)));
    bytes
}

fn free(store: &Store) -> u32 {
    store.capacity_frames() - store.used_frames()
}

#[test]
fn small_leases_share_frames_and_read_back_only_their_own_bytes() {
    const LEASES: usize = 10_000;
    let (_dir, store) = store(16, 16);
    let mut writer = store.create().unwrap();
    let body = |index: usize| format!("{index:040}").into_bytes();
    let leases: Vec<_> = (0..LEASES)
        .map(|index| lease(&mut writer, &body(index)).unwrap())
        .collect();
    assert!(store.used_frames() as usize <= (LEASES * 40).div_ceil(FRAME_BYTES) + 1);
    for (index, lease) in leases.iter().enumerate() {
        assert_eq!(lease.chunk(0).unwrap(), body(index));
        assert_eq!(lease.chunk(39).unwrap(), &body(index)[39..]);
        assert!(matches!(lease.chunk(40), Err(Error::InvalidPosition)));
    }
    drop(leases);
    assert_eq!(store.used_frames(), 0);
}

#[test]
fn a_shared_frame_returns_to_the_pool_only_after_its_last_lease_drops() {
    let (_dir, store) = store(8, 4);
    let capacity = store.capacity_frames();
    let mut writer = store.create().unwrap();
    // Five 10,000 B leases fill a frame exactly, so ten take two frames.
    let mut leases: Vec<_> = (0..10u8)
        .map(|byte| Some(lease(&mut writer, &[byte; 10_000]).unwrap()))
        .collect();
    assert_eq!(free(&store), capacity - 2);
    // A reservation in a shared frame exposes only its own written prefix.
    let mut pending = writer.begin_scratch(10).unwrap();
    assert_eq!(free(&store), capacity - 3);
    assert!(matches!(pending.chunk(0), Err(Error::InvalidPosition)));
    pending.write(b"ab").unwrap();
    assert_eq!(pending.chunk(0).unwrap(), b"ab");
    drop(pending);
    assert_eq!(free(&store), capacity - 2);
    for index in [0, 2, 4, 1] {
        leases[index] = None;
        assert_eq!(free(&store), capacity - 2);
    }
    leases[3] = None;
    assert_eq!(free(&store), capacity - 1);
    for index in [9, 5, 7, 6] {
        leases[index] = None;
        assert_eq!(free(&store), capacity - 1);
        assert_eq!(contents(leases[8].as_ref().unwrap()), [8; 10_000]);
    }
    leases[8] = None;
    assert_eq!(free(&store), capacity);
    // The freed open frame is forgotten: the next lease takes a fresh one.
    let fresh = lease(&mut writer, b"fresh").unwrap();
    assert_eq!(free(&store), capacity - 1);
    assert_eq!(contents(&fresh), b"fresh");
    drop(fresh);
    assert_eq!(free(&store), capacity);
}

/// Leases taken in order: small ones pack, big ones keep whole frames.
fn mixed(writer: &mut Writer) -> Vec<(ScratchLease, Vec<u8>)> {
    let mut leases = Vec::new();
    for (len, byte) in [
        (100, 1),
        (FRAME_BYTES + 1, 2),
        (100, 3),
        (49_000, 4),
        (1_000, 5),
        (500, 6),
    ] {
        let bytes = vec![byte; len];
        leases.push((lease(writer, &bytes).unwrap(), bytes));
    }
    leases
}

#[test]
fn mixed_leases_fail_at_the_frame_cap_and_roll_back() {
    let (_dir, store) = store(6, 4);
    let mut writer = store.create().unwrap();
    // 100 + 100 + 49,000 B share one frame, the big lease takes two, 1,000 + 500 B a fourth.
    let leases = mixed(&mut writer);
    assert_eq!(store.used_frames(), 4);
    drop(leases);
    assert_eq!(store.used_frames(), 0);
    // Every lease returned its metadata charge.
    drop(writer.reserve_metadata(METADATA).unwrap());

    let mut leases = mixed(&mut writer);
    // At the cap a small lease still fits in the open frame; a new frame does not.
    let bytes = vec![7; 100];
    leases.push((lease(&mut writer, &bytes).unwrap(), bytes));
    assert_eq!(store.used_frames(), 4);
    assert!(matches!(
        writer.begin_scratch(FRAME_BYTES),
        Err(Error::StorageExhausted)
    ));
    assert_eq!(writer.storage_failure_frames(), Some(4));
    assert!(matches!(writer.begin_scratch(1), Err(Error::Closed)));
    assert_eq!(store.used_frames(), 4);
    for (lease, bytes) in &leases {
        assert_eq!(&contents(lease), bytes);
    }
    drop(leases);
    assert_eq!(store.used_frames(), 0);

    // A big lease the pool cannot finish rolls back its frames; the open frame survives.
    let mut owner = store.create().unwrap();
    let held = lease(&mut owner, &[8; 4 * FRAME_BYTES]).unwrap();
    let mut writer = store.create().unwrap();
    let small = lease(&mut writer, b"small").unwrap();
    assert!(matches!(
        writer.begin_scratch(2 * FRAME_BYTES),
        Err(Error::StorageExhausted)
    ));
    assert_eq!(store.used_frames(), 5);
    assert_eq!(contents(&small), b"small");
    drop(small);
    assert_eq!(store.used_frames(), 4);
    drop(held);
    assert_eq!(store.used_frames(), 0);
}
