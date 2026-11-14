//! Dispatches each benchmark operation to the pool store or the heap comparator.

use crate::{WORKERS, heap};
use anyhow::{Result, bail};
use ck_stream::{Config, ManualClock, Read, Store, Terminal};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, task::Poll, time::Duration};

pub enum Backend {
    Pool(Store, Arc<ManualClock>),
    Heap(heap::Store),
}
pub enum Writer {
    Pool(ck_stream::Writer),
    Heap(heap::Writer),
}
pub enum Reader {
    Pool(ck_stream::Reader),
    Heap(heap::Reader),
}
pub enum Lease {
    Pool(ck_stream::Lease),
    Heap(heap::Lease),
}

impl Backend {
    pub fn open(kind: &str, directory: &Path, reserve_bytes: Option<usize>) -> Result<Self> {
        match kind {
            "pool" => {
                let clock = Arc::new(ManualClock::default());
                let mut config = Config::new(directory.to_path_buf());
                config.workers = WORKERS;
                config.startup_reserve_bytes = reserve_bytes;
                Ok(Self::Pool(Store::with_clock(config, clock.clone())?, clock))
            }
            "heap" => Ok(Self::Heap(heap::Store::default())),
            _ => bail!("unknown backend {kind}"),
        }
    }
    pub fn create(&self) -> Result<Writer> {
        Ok(match self {
            Self::Pool(store, _) => Writer::Pool(store.create()?),
            Self::Heap(store) => Writer::Heap(store.create()),
        })
    }
    pub fn reader(&self, id: u64) -> Result<Reader> {
        Ok(match self {
            Self::Pool(store, _) => Reader::Pool(store.reader(id, 0)?),
            Self::Heap(store) => Reader::Heap(store.reader(id)?),
        })
    }
    pub fn expire(&self) -> Result<()> {
        match self {
            Self::Pool(store, clock) => {
                clock.advance(Duration::from_secs(3601));
                store.tick()?;
            }
            Self::Heap(store) => store.expire(),
        }
        Ok(())
    }
    pub fn tick(&self) -> Result<()> {
        if let Self::Pool(store, _) = self {
            store.tick()?;
            if let Some(error) = store.take_reclaim_error() {
                return Err(error.into());
            }
        }
        Ok(())
    }
    pub fn counters(&self) -> Value {
        match self {
            Self::Pool(store, _) => {
                json!({"used_frames":store.used_frames(),"backed_frames":store.backed_frames(),"pool_frames":store.limits().pool_frames,"startup_reserve_frames":store.limits().startup_reserve_frames,"max_frames":store.limits().max_frames,"max_metadata_bytes":store.limits().max_metadata_bytes})
            }
            Self::Heap(_) => json!({}),
        }
    }
}
impl Writer {
    pub fn id(&self) -> u64 {
        match self {
            Self::Pool(w) => w.id(),
            Self::Heap(w) => w.id(),
        }
    }
    pub fn append(&mut self, bytes: &[u8], aligned: bool) -> Result<()> {
        match self {
            Self::Pool(w) if aligned => {
                let mut reservation = w.begin_record(bytes.len(), true, 0, 0)?;
                reservation.write(bytes)?;
                reservation.commit()?;
            }
            Self::Pool(w) => {
                w.append(bytes)?;
            }
            Self::Heap(w) => w.append(bytes, aligned)?,
        }
        Ok(())
    }
    pub fn finish(&mut self) {
        match self {
            Self::Pool(w) => w.finish(Terminal::Done),
            Self::Heap(w) => w.finish(),
        }
    }
}
impl Reader {
    pub fn try_next(&mut self) -> Result<Poll<Option<Lease>>> {
        match self {
            Self::Heap(r) => Ok(r.try_next().map(|v| v.map(Lease::Heap))),
            Self::Pool(r) => match r.try_next() {
                Poll::Pending => Ok(Poll::Pending),
                Poll::Ready(Some(Read::Record(lease))) => Ok(Poll::Ready(Some(Lease::Pool(lease)))),
                Poll::Ready(Some(Read::Terminal(Terminal::Done))) => Ok(Poll::Ready(None)),
                Poll::Ready(Some(Read::Terminal(other))) => bail!("reader terminal {other:?}"),
                Poll::Ready(None) => Ok(Poll::Ready(None)),
            },
        }
    }
    pub async fn next(&mut self) -> Result<Option<Lease>> {
        match self {
            Self::Heap(r) => Ok(r.next().await.map(Lease::Heap)),
            Self::Pool(r) => match r.next().await {
                Some(Read::Record(lease)) => Ok(Some(Lease::Pool(lease))),
                Some(Read::Terminal(Terminal::Done)) | None => Ok(None),
                Some(Read::Terminal(other)) => bail!("reader terminal {other:?}"),
            },
        }
    }
}
impl Lease {
    pub fn visit(&self, visitor: impl FnMut(&[u8])) -> Result<()> {
        match self {
            Self::Pool(lease) => lease.visit(visitor)?,
            Self::Heap(lease) => lease.visit(visitor),
        }
        Ok(())
    }
    pub fn acknowledge(self) {
        if let Self::Pool(lease) = self {
            lease.acknowledge();
        }
    }
}
