//! Storage refusals on a full tmpfs: each must be an error, never SIGBUS. These tests fill
//! the filesystem, so they run only against a private tmpfs supplied as `TMPDIR`.

use ck_stream::{Config, Error, FRAME_BYTES, ManualClock, Read, Store, Terminal};
use std::{sync::Arc, task::Poll, time::Duration};

fn lease(reader: &mut ck_stream::Reader) -> ck_stream::Lease {
    match reader.try_next() {
        Poll::Ready(Some(Read::Record(lease))) => lease,
        _ => panic!("expected record"),
    }
}

/// A directory on the private tmpfs the tests below fill. Any other `TMPDIR`, including a
/// run with `--include-ignored` on the host, is refused before anything is written. Each
/// test fills that tmpfs, so they run one at a time.
struct PrivateTmpfs {
    dir: tempfile::TempDir,
    _serial: std::sync::MutexGuard<'static, ()>,
}

impl PrivateTmpfs {
    fn path(&self) -> &std::path::Path {
        self.dir.path()
    }
}

fn private_tmpfs() -> PrivateTmpfs {
    use std::os::unix::ffi::OsStrExt;
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let serial = SERIAL.lock().unwrap_or_else(|poison| poison.into_inner());
    let root = std::env::temp_dir();
    let path = std::ffi::CString::new(root.as_os_str().as_bytes()).unwrap();
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: path is NUL-terminated and stat is writable output storage.
    assert_eq!(unsafe { libc::statfs(path.as_ptr(), stat.as_mut_ptr()) }, 0);
    // SAFETY: successful statfs initialized the fields.
    let stat = unsafe { stat.assume_init() };
    assert!(
        stat.f_type == libc::TMPFS_MAGIC && stat.f_blocks * stat.f_bsize as u64 <= 8 << 20,
        "refusing to fill {}: run inside `unshare -rm` with a private tmpfs of at most 8 MiB as TMPDIR",
        root.display()
    );
    PrivateTmpfs {
        dir: tempfile::tempdir().unwrap(),
        _serial: serial,
    }
}

/// Fill the tmpfs holding `directory`, which `private_tmpfs` created.
fn fill(directory: &std::path::Path) -> std::fs::File {
    use std::io::Write;
    let mut filler = std::fs::File::create(directory.join("filler")).unwrap();
    for size in [65_536, 4096] {
        while filler.write_all(&vec![0; size]).is_ok() {}
    }
    filler
}

#[test]
#[ignore = "requires a disposable private 8 MiB tmpfs supplied as TMPDIR"]
fn tmpfs_enospc_evicts_once_then_reports_the_os_error() {
    let dir = private_tmpfs();
    let mut config = Config::new(dir.path().join("pool"));
    config.pool_bytes = Some(80 * FRAME_BYTES);
    // Every frame used here is startup reserve, which background decay never punches.
    config.startup_reserve_bytes = Some(21 * FRAME_BYTES);
    config.max_event_bytes = FRAME_BYTES;
    config.workers = 1;
    let store = Store::open(config).unwrap();
    let mut old = store.create().unwrap();
    for _ in 0..21 {
        old.append(&[3; FRAME_BYTES]).unwrap();
    }
    old.finish(Terminal::Done);
    let old = old.id();
    let _filler = fill(dir.path());
    let mut new = store.create().unwrap();
    new.append(&[1; 10]).unwrap();
    assert!(matches!(store.reader(old, 0), Err(Error::Gone)));
    assert_eq!(store.used_frames(), 1);
    let mut last = store.create().unwrap();
    for _ in 0..21 {
        last.append(&[4; FRAME_BYTES]).unwrap();
    }
    assert!(matches!(store.reader(new.id(), 0), Err(Error::Gone)));
    let error = last.append(&[5; FRAME_BYTES]).unwrap_err();
    assert!(matches!(error, Error::Io(ref error) if error.raw_os_error() == Some(libc::ENOSPC)));
    assert_eq!(store.used_frames(), 21);
}

#[test]
#[ignore = "requires a disposable private 8 MiB tmpfs supplied as TMPDIR"]
fn tmpfs_cold_prefault_failure_evicts_once_then_reports_the_os_error() {
    let dir = private_tmpfs();
    let mut config = Config::new(dir.path().join("pool"));
    config.pool_bytes = Some(80 * FRAME_BYTES);
    config.max_event_bytes = FRAME_BYTES;
    config.workers = 1;
    let clock = Arc::new(ManualClock::default());
    let store = Store::with_clock(config, clock.clone()).unwrap();
    let mut old = store.create().unwrap();
    for _ in 0..4 {
        old.append(&[3; FRAME_BYTES]).unwrap();
    }
    let mut expired = store.create().unwrap();
    for _ in 0..30 {
        expired.append(&[4; FRAME_BYTES]).unwrap();
    }
    clock.advance(Duration::from_secs(3601));
    drop(store.reader(old.id(), 0).unwrap());
    // Expiry frees 30 frames; the second decay punches all but the eight warm ones. Extra
    // background decays change nothing: eight warm frames stay and later frees sit above the
    // low-water mark until the test ends.
    for _ in 0..3 {
        store.tick().unwrap();
    }
    assert!(matches!(store.reader(expired.id(), 0), Err(Error::Gone)));
    let _filler = fill(dir.path());
    let mut new = store.create().unwrap();
    for _ in 0..12 {
        new.append(&[5; FRAME_BYTES]).unwrap();
    }
    assert!(matches!(store.reader(old.id(), 0), Err(Error::Gone)));
    let error = new.append(&[6; FRAME_BYTES]).unwrap_err();
    assert!(matches!(error, Error::Io(ref error) if error.raw_os_error() == Some(libc::EFAULT)));
}

/// A stream that has filled eight warm frames and written 100 bytes into a cold ninth,
/// which populated only the pages under those bytes, with a reader attached from the start.
fn continuing_a_cold_frame(dir: &PrivateTmpfs) -> (Store, ck_stream::Writer, ck_stream::Reader) {
    let mut config = Config::new(dir.path().join("pool"));
    config.pool_bytes = Some(80 * FRAME_BYTES);
    config.max_event_bytes = FRAME_BYTES;
    config.workers = 1;
    let clock = Arc::new(ManualClock::default());
    let store = Store::with_clock(config, clock.clone()).unwrap();
    let mut expired = store.create().unwrap();
    for _ in 0..30 {
        expired.append(&[4; FRAME_BYTES]).unwrap();
    }
    clock.advance(Duration::from_secs(3601));
    // Expiry frees 30 frames; the second decay punches all but the eight warm ones.
    for _ in 0..2 {
        store.tick().unwrap();
    }
    let mut new = store.create().unwrap();
    let reader = store.reader(new.id(), 0).unwrap();
    for _ in 0..8 {
        new.append(&[5; FRAME_BYTES]).unwrap();
    }
    new.append(&[6; 100]).unwrap();
    (store, new, reader)
}

#[test]
#[ignore = "requires a disposable private 8 MiB tmpfs supplied as TMPDIR"]
fn tmpfs_refusal_while_continuing_a_cold_frame_is_an_error_not_a_fault() {
    let dir = private_tmpfs();
    let (_store, mut new, mut reader) = continuing_a_cold_frame(&dir);
    let _filler = fill(dir.path());
    let error = new.append(&[7; 20_000]).unwrap_err();
    assert!(matches!(error, Error::Io(ref error) if error.raw_os_error() == Some(libc::EFAULT)));
    for _ in 0..9 {
        lease(&mut reader);
    }
    assert!(matches!(
        reader.try_next(),
        Poll::Ready(Some(Read::Terminal(Terminal::StorageExhausted)))
    ));
}

#[test]
#[ignore = "requires a disposable private 8 MiB tmpfs supplied as TMPDIR"]
fn tmpfs_refusal_while_extending_a_run_into_unpopulated_pages_is_an_error_not_a_fault() {
    let dir = private_tmpfs();
    let (_store, mut new, mut reader) = continuing_a_cold_frame(&dir);
    let _filler = fill(dir.path());
    // Equal records extend the run in place until one reaches a page never populated.
    let mut appended = 0;
    let error = loop {
        match new.append(&[6; 100]) {
            Ok(_) => appended += 1,
            Err(error) => break error,
        }
    };
    assert!(appended < FRAME_BYTES / 100);
    assert!(matches!(error, Error::Io(ref error) if error.raw_os_error() == Some(libc::EFAULT)));
    for _ in 0..9 + appended {
        lease(&mut reader);
    }
    assert!(matches!(
        reader.try_next(),
        Poll::Ready(Some(Read::Terminal(Terminal::StorageExhausted)))
    ));
}
