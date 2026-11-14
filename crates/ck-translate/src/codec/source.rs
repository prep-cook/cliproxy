use std::sync::atomic::{AtomicU64, Ordering};

use ck_stream::{ByteSpan, FRAME_BYTES, Lease, MetadataPermit, Position, ScratchLease, ScratchReservation, Writer};

use super::{Error, Result};

/// Byte offsets into one `Source`, not pointers into the mmap mapping.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Span {
    pub start: u64,
    pub end: u64,
}

impl Span {
    pub fn len(self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    pub fn is_empty(self) -> bool {
        self.start == self.end
    }
}

enum Storage {
    Canonical {
        span: std::ops::Range<Position>,
        lease: Lease,
    },
    Scratch(ScratchLease),
    Pending(ScratchReservation),
}

struct Segment {
    start: u64,
    end: u64,
    storage: Storage,
}

static NEXT_SOURCE: AtomicU64 = AtomicU64::new(1);

fn next_source_id() -> u64 {
    let mut current = NEXT_SOURCE.load(Ordering::Relaxed);
    loop {
        let next = current.checked_add(1).expect("source identity exhausted");
        match NEXT_SOURCE.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return current,
            Err(observed) => current = observed,
        }
    }
}

/// Leased input fragments. Only coordinates and leases are held on the heap.
///
/// Scratch and canonical leases share the owning writer's quota. Releasing input
/// segments never releases a canonical response prefix.
pub struct Source {
    id: u64,
    segments: Vec<Segment>,
    len: u64,
    metadata: MetadataPermit,
    base: u64,
}

impl Source {
    pub fn new(owner: &mut Writer) -> Result<Self> {
        Ok(Self {
            id: next_source_id(),
            segments: Vec::new(),
            len: 0,
            metadata: owner.reserve_metadata(0)?,
            base: 0,
        })
    }

    pub fn stream_id(&self) -> u64 {
        self.metadata.stream_id()
    }

    pub fn check_owner(&self, owner: &Writer) -> Result<()> {
        self.check_live()?;
        if owner.id() != self.stream_id() {
            return Err(Error::Position);
        }
        Ok(())
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn check_live(&self) -> Result<()> {
        self.metadata.check_live()?;
        Ok(())
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn metadata_bytes(&self) -> usize {
        self.segments.capacity() * std::mem::size_of::<Segment>()
    }

    /// Retain a complete canonical record or a subspan under its lease.
    pub fn push(&mut self, lease: Lease, range: std::ops::Range<Position>) -> Result<Span> {
        self.check_live()?;
        if range.start.generation != self.stream_id() {
            return Err(Error::Position);
        }
        let bytes = ByteSpan::new(&lease, range.clone())?.len();
        self.insert(Storage::Canonical { span: range, lease }, bytes)
    }

    pub fn push_scratch(&mut self, lease: ScratchLease) -> Result<Span> {
        self.check_live()?;
        if lease.stream_id() != self.stream_id() {
            return Err(Error::Position);
        }
        let len = lease.len() as u64;
        self.insert(Storage::Scratch(lease), len)
    }

    pub fn push_bytes(&mut self, owner: &mut Writer, mut bytes: &[u8]) -> Result<Span> {
        self.check_owner(owner)?;
        let start = self.len;
        self.len.checked_add(bytes.len() as u64).ok_or(Error::Limit)?;
        while !bytes.is_empty() {
            if let Some(segment) = self.segments.last_mut()
                && segment.end == self.len
                && let Storage::Pending(reservation) = &mut segment.storage
            {
                let available = FRAME_BYTES - (segment.end - segment.start) as usize;
                let count = available.min(bytes.len());
                if count != 0 {
                    reservation.write(&bytes[..count])?;
                    segment.end += count as u64;
                    self.len = segment.end;
                    bytes = &bytes[count..];
                    continue;
                }
            }
            let count = bytes.len().min(FRAME_BYTES);
            let mut reservation = owner.begin_scratch(FRAME_BYTES)?;
            reservation.write(&bytes[..count])?;
            self.insert(Storage::Pending(reservation), count as u64)?;
            bytes = &bytes[count..];
        }
        Ok(Span { start, end: self.len })
    }

    fn insert(&mut self, storage: Storage, bytes: u64) -> Result<Span> {
        let end = self.len.checked_add(bytes).ok_or(Error::Limit)?;
        if bytes != 0 {
            if self.segments.len() == self.segments.capacity() {
                let capacity = self.segments.capacity().max(4).checked_mul(2).ok_or(Error::Limit)?;
                self.metadata.resize(
                    capacity
                        .checked_mul(std::mem::size_of::<Segment>())
                        .ok_or(Error::Limit)?,
                )?;
                self.segments
                    .try_reserve_exact(capacity - self.segments.len())
                    .map_err(|_| Error::Limit)?;
                self.metadata.resize(self.metadata_bytes())?;
            }
            self.segments.push(Segment {
                start: self.len,
                end,
                storage,
            });
        }
        let span = Span { start: self.len, end };
        self.len = end;
        Ok(span)
    }

    /// The caller must have retired every parser/provenance position before this offset.
    pub fn release_before(&mut self, offset: u64) -> Result<()> {
        self.check_live()?;
        if offset < self.base || offset > self.len {
            return Err(Error::Position);
        }
        let count = self.segments.partition_point(|segment| segment.end <= offset);
        self.segments.drain(..count);
        self.base = self.segments.first().map_or(self.len, |segment| segment.start);
        Ok(())
    }

    /// Retire consumed payload leases while earlier, still-live names keep their backing.
    pub(crate) fn release_range(&mut self, span: Span) -> Result<()> {
        self.check_live()?;
        if span.start > span.end || span.end > self.len {
            return Err(Error::Position);
        }
        self.segments
            .retain(|segment| segment.start < span.start || segment.end > span.end);
        self.base = self.segments.first().map_or(self.len, |segment| segment.start);
        Ok(())
    }

    pub(crate) fn retire_scratch(&mut self) -> Result<()> {
        if self
            .segments
            .iter()
            .all(|segment| matches!(segment.storage, Storage::Scratch(_) | Storage::Pending(_)))
        {
            self.release_before(self.len)?;
        }
        Ok(())
    }

    pub fn whole(&self) -> Span {
        Span {
            start: self.base,
            end: self.len,
        }
    }

    pub fn cursor(&self, span: Span) -> Result<Cursor<'_>> {
        self.check_live()?;
        if span.start < self.base || span.start > span.end || span.end > self.len {
            return Err(Error::Position);
        }
        Ok(Cursor {
            source: self,
            offset: span.start,
            end: span.end,
            chunk: &[],
        })
    }

    pub fn visit(&self, span: Span, mut visit: impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        let mut cursor = self.cursor(span)?;
        while cursor.offset < span.end {
            let chunk = self.chunk(cursor.offset, span.end)?;
            visit(chunk)?;
            cursor.offset += chunk.len() as u64;
        }
        Ok(())
    }

    pub fn equals(&self, span: Span, bytes: &[u8]) -> Result<bool> {
        if span.len() != bytes.len() as u64 {
            return Ok(false);
        }
        let mut cursor = self.cursor(span)?;
        for &byte in bytes {
            if cursor.next_byte()? != Some(byte) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn chunk(&self, offset: u64, end: u64) -> Result<&[u8]> {
        let index = self
            .segments
            .partition_point(|segment| segment.start <= offset)
            .checked_sub(1)
            .ok_or(Error::Position)?;
        let segment = &self.segments[index];
        if offset >= segment.end {
            return Err(Error::Position);
        }
        let bytes = match &segment.storage {
            Storage::Canonical { span, lease } => {
                let position = advance(span.start, offset - segment.start)?;
                ByteSpan::new(lease, span.clone())?.chunk(position)?
            }
            Storage::Scratch(lease) => lease.chunk((offset - segment.start) as usize)?,
            Storage::Pending(reservation) => reservation.chunk((offset - segment.start) as usize)?,
        };
        Ok(&bytes[..bytes.len().min((end - offset) as usize)])
    }
}

fn advance(position: Position, bytes: u64) -> Result<Position> {
    let offset = u64::from(position.offset).checked_add(bytes).ok_or(Error::Position)?;
    Ok(Position {
        generation: position.generation,
        frame: position
            .frame
            .checked_add(offset / ck_stream::FRAME_BYTES as u64)
            .ok_or(Error::Position)?,
        offset: (offset % ck_stream::FRAME_BYTES as u64) as u32,
    })
}

/// A cursor caches only a temporary borrowed frame span under `Source`'s lease.
#[derive(Clone)]
pub struct Cursor<'a> {
    source: &'a Source,
    pub(crate) offset: u64,
    end: u64,
    chunk: &'a [u8],
}

impl Cursor<'_> {
    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn next_byte(&mut self) -> Result<Option<u8>> {
        if self.offset == self.end {
            return Ok(None);
        }
        if self.chunk.is_empty() {
            self.chunk = self.source.chunk(self.offset, self.end)?;
        }
        let byte = self.chunk[0];
        self.chunk = &self.chunk[1..];
        self.offset += 1;
        Ok(Some(byte))
    }
}
