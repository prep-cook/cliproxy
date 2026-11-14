//! Benchmark-only heap comparator in the S0 shape: uninitialised 50,000-byte
//! frames from the global allocator, lock-free copies of committed bytes and a
//! run-length record index. Locks cover only the frame-table growth, index
//! updates and pointer lookups, never a payload copy or a reader's visitor.
use anyhow::{Result, bail};
use ck_stream::{FRAME_BYTES, Position, Record};
use std::{
    collections::HashMap,
    ptr::NonNull,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::Poll,
};
use tokio::sync::Notify;

const MAX_FRAMES: usize = 5368;
const MAX_EVENT: usize = 1024 * 1024;
/// Frames one record can touch: an unaligned 1 MiB event spans at most 22.
const MAX_SPAN: usize = MAX_EVENT.div_ceil(FRAME_BYTES) + 1;

#[derive(Default)]
#[repr(align(128))]
pub struct Store {
    streams: Mutex<HashMap<u64, Arc<Stream>>>,
    next: AtomicU64,
}

/// Consecutive records of one length at a fixed stride, like the pool's run index.
#[derive(Clone, Copy)]
struct Run {
    start: u64,
    len: u64,
    stride: u64,
    count: u64,
}

#[repr(align(128))]
struct Stream {
    frames: RwLock<Vec<NonNull<u8>>>,
    runs: RwLock<Vec<Run>>,
    records: AtomicU64,
    terminal: AtomicBool,
    notify: Notify,
}

// SAFETY: frames are owned by the stream and freed only on drop. The single
// writer initialises bytes before publishing them with a Release store of the
// record count; readers form slices only over records below an Acquire load
// of that count, so no byte is written while it can be read.
unsafe impl Send for Stream {}
unsafe impl Sync for Stream {}

impl Drop for Stream {
    fn drop(&mut self) {
        for frame in self.frames.get_mut().unwrap().drain(..) {
            let slice =
                std::ptr::slice_from_raw_parts_mut(frame.as_ptr().cast::<std::mem::MaybeUninit<u8>>(), FRAME_BYTES);
            // SAFETY: each pointer came from Box::into_raw of a FRAME_BYTES slice.
            drop(unsafe { Box::from_raw(slice) });
        }
    }
}

pub struct Writer {
    stream: Arc<Stream>,
    id: u64,
    /// Writer-local frame table, so copies take no lock.
    local: Vec<NonNull<u8>>,
    end: usize,
    records: u64,
    finished: bool,
}
// SAFETY: the frame pointers are owned by the shared stream.
unsafe impl Send for Writer {}

pub struct Reader {
    stream: Arc<Stream>,
    id: u64,
    next: u64,
    run: usize,
    in_run: u64,
    done: bool,
}
pub struct Lease {
    stream: Arc<Stream>,
    record: Record,
}

fn position(id: u64, byte: usize) -> Position {
    Position {
        generation: id,
        frame: (byte / FRAME_BYTES) as u64,
        offset: (byte % FRAME_BYTES) as u32,
    }
}
fn byte(position: Position) -> usize {
    position.frame as usize * FRAME_BYTES + position.offset as usize
}

impl Store {
    pub fn create(&self) -> Writer {
        let id = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        let stream = Arc::new(Stream {
            frames: RwLock::new(Vec::new()),
            runs: RwLock::new(Vec::new()),
            records: AtomicU64::new(0),
            terminal: AtomicBool::new(false),
            notify: Notify::new(),
        });
        self.streams.lock().unwrap().insert(id, stream.clone());
        Writer {
            stream,
            id,
            local: Vec::new(),
            end: 0,
            records: 0,
            finished: false,
        }
    }
    pub fn reader(&self, id: u64) -> Result<Reader> {
        let stream = self
            .streams
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("heap stream.gone"))?;
        Ok(Reader {
            stream,
            id,
            next: 0,
            run: 0,
            in_run: 0,
            done: false,
        })
    }
    pub fn expire(&self) {
        self.streams.lock().unwrap().clear();
    }
}

impl Writer {
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn append(&mut self, bytes: &[u8], aligned: bool) -> Result<()> {
        if self.finished {
            bail!("heap stream terminal");
        }
        if bytes.is_empty() || bytes.len() > MAX_EVENT || (aligned && bytes.len() > FRAME_BYTES) {
            bail!("heap event limit");
        }
        let start = if aligned {
            self.end.div_ceil(FRAME_BYTES) * FRAME_BYTES
        } else {
            self.end
        };
        let end = start + bytes.len();
        if end.div_ceil(FRAME_BYTES) > MAX_FRAMES {
            bail!("heap stream.storage_exhausted");
        }
        while self.local.len() < end.div_ceil(FRAME_BYTES) {
            let frame = Box::<[u8]>::new_uninit_slice(FRAME_BYTES);
            let frame = NonNull::new(Box::into_raw(frame).cast::<u8>()).expect("box pointer");
            self.local.push(frame);
            self.stream.frames.write().unwrap().push(frame);
        }
        let mut offset = 0;
        while offset < bytes.len() {
            let byte = start + offset;
            let n = (FRAME_BYTES - byte % FRAME_BYTES).min(bytes.len() - offset);
            // SAFETY: the target bytes lie past every published record, inside an owned frame.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    bytes[offset..].as_ptr(),
                    self.local[byte / FRAME_BYTES].as_ptr().add(byte % FRAME_BYTES),
                    n,
                );
            }
            offset += n;
        }
        {
            let (start, len) = (start as u64, bytes.len() as u64);
            let mut runs = self.stream.runs.write().unwrap();
            match runs.last_mut() {
                Some(run) if run.len == len && run.count == 1 => {
                    run.stride = start - run.start;
                    run.count = 2;
                }
                Some(run) if run.len == len && run.start + run.count * run.stride == start => run.count += 1,
                _ => runs.push(Run {
                    start,
                    len,
                    stride: 0,
                    count: 1,
                }),
            }
        }
        self.end = end;
        self.records += 1;
        self.stream.records.store(self.records, Ordering::Release);
        self.stream.notify.notify_waiters();
        Ok(())
    }
    pub fn finish(&mut self) {
        self.finished = true;
        self.stream.terminal.store(true, Ordering::Release);
        self.stream.notify.notify_waiters();
    }
}

impl Reader {
    pub fn try_next(&mut self) -> Poll<Option<Lease>> {
        if self.done {
            return Poll::Ready(None);
        }
        // Terminal first: once it is visible, the final record count is too.
        let terminal = self.stream.terminal.load(Ordering::Acquire);
        if self.next < self.stream.records.load(Ordering::Acquire) {
            let runs = self.stream.runs.read().unwrap();
            while self.in_run >= runs[self.run].count {
                self.run += 1;
                self.in_run = 0;
            }
            let run = runs[self.run];
            drop(runs);
            let start = (run.start + self.in_run * run.stride) as usize;
            let record = Record {
                sequence: self.next,
                start: position(self.id, start),
                end: position(self.id, start + run.len as usize),
                kind: 0,
                correlation: 0,
            };
            self.in_run += 1;
            self.next += 1;
            return Poll::Ready(Some(Lease {
                stream: self.stream.clone(),
                record,
            }));
        }
        if terminal {
            self.done = true;
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
    pub async fn next(&mut self) -> Option<Lease> {
        loop {
            let stream = self.stream.clone();
            let notified = stream.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Poll::Ready(value) = self.try_next() {
                return value;
            }
            notified.await;
        }
    }
}

impl Lease {
    pub fn visit(&self, mut visitor: impl FnMut(&[u8])) {
        let (start, end) = (byte(self.record.start), byte(self.record.end));
        let first = start / FRAME_BYTES;
        let span = (end - 1) / FRAME_BYTES - first + 1;
        let mut frames = [NonNull::<u8>::dangling(); MAX_SPAN];
        frames[..span].copy_from_slice(&self.stream.frames.read().unwrap()[first..first + span]);
        let mut byte = start;
        while byte < end {
            let n = (FRAME_BYTES - byte % FRAME_BYTES).min(end - byte);
            // SAFETY: the record is published, so these bytes are initialised and immutable.
            visitor(unsafe {
                std::slice::from_raw_parts(frames[byte / FRAME_BYTES - first].as_ptr().add(byte % FRAME_BYTES), n)
            });
            byte += n;
        }
    }
}
