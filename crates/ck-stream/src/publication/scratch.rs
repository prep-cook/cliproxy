use super::{Error, FRAME_BYTES, Ordering, Stream, Terminal, Writer};
use std::sync::Arc;

/// Owns a reservation for external metadata capacity, not payload storage.
/// Charge allocated capacity before growing a scanner vector and retain the permit until
/// that capacity is freed. Drop releases the charge; failed growth leaves it unchanged.
pub struct MetadataPermit {
    stream: Arc<Stream>,
    bytes: usize,
}

impl MetadataPermit {
    /// Owning stream generation, used to reject cross-stream codec bookkeeping.
    pub fn stream_id(&self) -> u64 {
        self.stream.id
    }

    /// Validate retained metadata even when its source span is empty.
    /// Completion preserves reads; expiry and eviction invalidate them.
    pub fn check_live(&self) -> Result<(), Error> {
        if self.stream.invalidated.load(Ordering::Acquire) {
            Err(Error::Gone)
        } else {
            Ok(())
        }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Resize the charge. Growth requires a live, nonterminal stream; shrinking is always allowed.
    /// Quota failure terminates the stream with `StorageExhausted`.
    pub fn resize(&mut self, bytes: usize) -> Result<(), Error> {
        let mut state = self.stream.state.lock();
        if bytes > self.bytes {
            if self.stream.invalidated.load(Ordering::Acquire) {
                return Err(Error::Gone);
            }
            if state.terminal.is_some() {
                return Err(Error::Closed);
            }
            let extra = bytes - self.bytes;
            if extra
                > self
                    .stream
                    .limits
                    .max_metadata_bytes
                    .saturating_sub(Stream::metadata(&state))
            {
                drop(state);
                self.stream.finish(Terminal::StorageExhausted);
                return Err(Error::StorageExhausted);
            }
            state.external_metadata += extra;
        } else {
            state.external_metadata -= self.bytes - bytes;
        }
        self.bytes = bytes;
        Ok(())
    }
}

impl Drop for MetadataPermit {
    fn drop(&mut self) {
        self.stream.state.lock().external_metadata -= self.bytes;
    }
}

impl Writer {
    /// Reserve external bookkeeping against the same quota as frame tables, indexes and readers.
    /// The permit owns its charge independently of this writer; zero bytes is valid.
    pub fn reserve_metadata(&mut self, bytes: usize) -> Result<MetadataPermit, Error> {
        {
            let state = self.stream.state.lock();
            if self.stream.invalidated.load(Ordering::Acquire) {
                return Err(Error::Gone);
            }
            if state.terminal.is_some() {
                return Err(Error::Closed);
            }
        }
        let mut permit = MetadataPermit {
            stream: self.stream.clone(),
            bytes: 0,
        };
        permit.resize(bytes)?;
        Ok(permit)
    }

    /// Reserve unpublished source bytes without borrowing the writer for the lease lifetime.
    /// Scratch shares this stream's frame and metadata quotas but has no canonical positions,
    /// records, sequence numbers or committed end. Its frames release independently on drop.
    /// Zero length is invalid; `max_event_bytes` applies to canonical records, not scratch.
    /// Pool/quota failures terminate the stream with `StorageExhausted` and roll back frames.
    pub fn begin_scratch(&mut self, len: usize) -> Result<ScratchReservation, Error> {
        let result = self.reserve_scratch(len);
        if matches!(result, Err(Error::StorageExhausted | Error::Io(_))) {
            self.stream.finish(Terminal::StorageExhausted);
        }
        result
    }

    fn reserve_scratch(&mut self, len: usize) -> Result<ScratchReservation, Error> {
        if len == 0 {
            return Err(Error::InvalidRecord);
        }
        let count = len.div_ceil(FRAME_BYTES);
        let mut state = self.stream.state.lock();
        if self.stream.invalidated.load(Ordering::Acquire) {
            return Err(Error::Gone);
        }
        if state.terminal.is_some() {
            return Err(Error::Closed);
        }
        let live = self.stream.live_frames(&state);
        if count > (self.stream.limits.max_frames as usize).saturating_sub(live) {
            return Err(Error::StorageExhausted);
        }
        let available = self
            .stream
            .limits
            .max_metadata_bytes
            .saturating_sub(Stream::metadata(&state));
        let overhead = std::mem::size_of::<ScratchReservation>();
        let bytes = count
            .checked_mul(std::mem::size_of::<u32>())
            .and_then(|bytes| bytes.checked_add(overhead))
            .ok_or(Error::StorageExhausted)?;
        if bytes > available {
            return Err(Error::StorageExhausted);
        }
        let mut frames = Vec::new();
        frames.try_reserve_exact(count).map_err(|_| Error::StorageExhausted)?;
        let bytes = frames.capacity() * std::mem::size_of::<u32>() + overhead;
        if bytes > available {
            return Err(Error::StorageExhausted);
        }
        state.external_metadata += bytes;
        state.scratch_frames += count;
        drop(state);
        let mut lease = ScratchLease {
            metadata: MetadataPermit {
                stream: self.stream.clone(),
                bytes,
            },
            frames,
            reserved_frames: count,
            len,
        };
        for taken in 0..count {
            // Cache eviction may lock stream state. The quota is reserved, but no lock is held.
            let needed = (len - taken * FRAME_BYTES).min(FRAME_BYTES);
            let frame = self.inner.allocate(self.id(), count - taken, needed)?;
            lease.frames.push(frame);
            let state = self.stream.state.lock();
            if self.stream.invalidated.load(Ordering::Acquire) {
                return Err(Error::Gone);
            }
            if state.terminal.is_some() {
                return Err(Error::Closed);
            }
        }
        Ok(ScratchReservation { lease, written: 0 })
    }
}

/// Exclusive, unpublished scratch storage. Dropping an incomplete reservation returns all frames.
/// Writes go directly into the pool; no payload is retained on the heap.
pub struct ScratchReservation {
    lease: ScratchLease,
    written: usize,
}

impl ScratchReservation {
    /// Borrow only initialized bytes, clipped to their frame boundary.
    /// The borrow excludes appends until it ends; unwritten capacity is never readable.
    pub fn chunk(&self, offset: usize) -> Result<&[u8], Error> {
        let stream = &self.lease.metadata.stream;
        if stream.invalidated.load(Ordering::Acquire) {
            return Err(Error::Gone);
        }
        if offset >= self.written {
            return Err(Error::InvalidPosition);
        }
        let frame_offset = offset % FRAME_BYTES;
        let len = (self.written - offset).min(FRAME_BYTES - frame_offset);
        // SAFETY: the reservation owns these initialized bytes. Rust excludes write's
        // mutable borrow while this slice is live, and the reservation pins the mapping.
        Ok(unsafe {
            stream
                .pool
                .slice(self.lease.frames[offset / FRAME_BYTES], frame_offset, len)
        })
    }

    /// Append bytes within the declared length. Overrun leaves the reservation unchanged.
    pub fn write(&mut self, mut bytes: &[u8]) -> Result<(), Error> {
        let stream = &self.lease.metadata.stream;
        let state = stream.state.lock();
        if stream.invalidated.load(Ordering::Acquire) {
            return Err(Error::Gone);
        }
        if state.terminal.is_some() {
            return Err(Error::Closed);
        }
        if bytes.len() > self.lease.len - self.written {
            return Err(Error::InvalidPosition);
        }
        while !bytes.is_empty() {
            let offset = self.written % FRAME_BYTES;
            let len = bytes.len().min(FRAME_BYTES - offset);
            let frame = self.lease.frames[self.written / FRAME_BYTES];
            // SAFETY: this reservation exclusively owns these frames; mutable access
            // excludes prefix borrows, and writes only extend the initialized prefix.
            unsafe { stream.pool.write(frame, offset, &bytes[..len]) };
            self.written += len;
            bytes = &bytes[len..];
        }
        Ok(())
    }

    /// Freeze exactly the initialized bytes without publishing a canonical record or copying payload.
    /// An incomplete reservation returns `IncompleteRecord` and releases its frames.
    pub fn complete(self) -> Result<ScratchLease, Error> {
        {
            let stream = &self.lease.metadata.stream;
            let state = stream.state.lock();
            if stream.invalidated.load(Ordering::Acquire) {
                return Err(Error::Gone);
            }
            if state.terminal.is_some() {
                return Err(Error::Closed);
            }
            if self.written != self.lease.len {
                return Err(Error::IncompleteRecord);
            }
        }
        Ok(self.lease)
    }
}

/// Immutable source bytes addressed by lease-local byte offsets, never canonical `Position`.
/// The lease pins its frames and mapping through writer/store drop and invalidation.
/// Invalidation rejects new borrows with `Gone`; existing borrows remain valid until they end.
pub struct ScratchLease {
    metadata: MetadataPermit,
    frames: Vec<u32>,
    reserved_frames: usize,
    len: usize,
}

impl ScratchLease {
    /// Owning stream generation; scratch offsets belong only to this lease.
    pub fn stream_id(&self) -> u64 {
        self.metadata.stream_id()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Borrow from `offset` up to the frame boundary or lease end. The borrow pins this lease.
    /// An offset at or beyond the exclusive end returns `InvalidPosition`.
    pub fn chunk(&self, offset: usize) -> Result<&[u8], Error> {
        let stream = &self.metadata.stream;
        if stream.invalidated.load(Ordering::Acquire) {
            return Err(Error::Gone);
        }
        if offset >= self.len {
            return Err(Error::InvalidPosition);
        }
        let frame_offset = offset % FRAME_BYTES;
        let len = (self.len - offset).min(FRAME_BYTES - frame_offset);
        // SAFETY: complete consumed the only writer after initializing the entire span.
        // This lease owns every physical frame until drop and retains the pool mapping.
        Ok(unsafe { stream.pool.slice(self.frames[offset / FRAME_BYTES], frame_offset, len) })
    }
}

impl Drop for ScratchLease {
    fn drop(&mut self) {
        let stream = &self.metadata.stream;
        let mut state = stream.state.lock();
        for frame in self.frames.drain(..) {
            stream.pool.release(frame);
        }
        state.scratch_frames -= self.reserved_frames;
    }
}
