use super::{Error, FRAME_BYTES, Ordering, State, Stream, Terminal, Writer};
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
    /// records, sequence numbers or committed end. A lease shorter than a frame packs into the
    /// stream's open scratch frame; a frame is charged once and returns to the pool when the
    /// last lease in it drops. Longer leases take whole frames of their own.
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
        let mut state = self.stream.state.lock();
        if self.stream.invalidated.load(Ordering::Acquire) {
            return Err(Error::Gone);
        }
        if state.terminal.is_some() {
            return Err(Error::Closed);
        }
        let shared = state
            .scratch_open
            .filter(|&(_, used)| len < FRAME_BYTES && len <= FRAME_BYTES - used);
        let count = if shared.is_some() { 0 } else { len.div_ceil(FRAME_BYTES) };
        let (mut frames, bytes) = self.scratch_quota(&state, count)?;
        if let Some((frame, used)) = shared {
            // The locked owner prefaults the pages this lease reaches; eviction cannot help,
            // as it punches nothing.
            self.inner.pool.prefault(frame, used + len).map_err(Error::Io)?;
            // The leases already in this frame release it only under this lock, and the
            // release that frees it clears `scratch_open`, so it is still allocated. Each
            // lease spans at least one byte, so its references never saturate.
            self.inner.pool.retain(frame);
            frames.push(frame);
            state.scratch_open = Some((frame, used + len));
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
            // The quota check bounded `count` by `max_frames`.
            pending: count as u32,
            base: shared.map_or(0, |(_, used)| used as u32),
            len,
        };
        for taken in 0..count {
            // Cache eviction may lock stream state. The quota is reserved, but no lock is held.
            let needed = (len - taken * FRAME_BYTES).min(FRAME_BYTES);
            let frame = self.inner.allocate(self.id(), count - taken, needed)?;
            // The charge moves from the reservation to the held frame.
            lease.pending -= 1;
            lease.frames.push(frame);
            let mut state = self.stream.state.lock();
            if self.stream.invalidated.load(Ordering::Acquire) {
                return Err(Error::Gone);
            }
            if state.terminal.is_some() {
                return Err(Error::Closed);
            }
            if len < FRAME_BYTES {
                // The previous open frame stays allocated while its own leases hold it.
                state.scratch_open = Some((frame, len));
            }
        }
        Ok(ScratchReservation { lease, written: 0 })
    }

    /// Check that `count` more frames and one lease's bookkeeping fit the stream's quotas, and
    /// allocate the lease's frame list. Charges nothing.
    fn scratch_quota(&self, state: &State, count: usize) -> Result<(Vec<u32>, usize), Error> {
        let live = self.stream.live_frames(state);
        if count > (self.stream.limits.max_frames as usize).saturating_sub(live) {
            return Err(Error::StorageExhausted);
        }
        let available = self
            .stream
            .limits
            .max_metadata_bytes
            .saturating_sub(Stream::metadata(state));
        let overhead = std::mem::size_of::<ScratchReservation>();
        // A shared lease takes no new frame but still lists the one it packs into.
        let listed = count.max(1);
        let bytes = listed
            .checked_mul(std::mem::size_of::<u32>())
            .and_then(|bytes| bytes.checked_add(overhead))
            .ok_or(Error::StorageExhausted)?;
        if bytes > available {
            return Err(Error::StorageExhausted);
        }
        let mut frames = Vec::new();
        frames.try_reserve_exact(listed).map_err(|_| Error::StorageExhausted)?;
        let bytes = frames.capacity() * std::mem::size_of::<u32>() + overhead;
        if bytes > available {
            return Err(Error::StorageExhausted);
        }
        Ok((frames, bytes))
    }
}

/// Exclusive, unpublished scratch storage. Dropping an incomplete reservation releases its frames.
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
        let (frame, frame_offset, len) = self.lease.locate(offset, self.written);
        // SAFETY: the reservation owns these initialized bytes. Rust excludes write's
        // mutable borrow while this slice is live, and the reservation pins the mapping.
        Ok(unsafe { stream.pool.slice(frame, frame_offset, len) })
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
            let (frame, offset, len) = self.lease.locate(self.written, self.written + bytes.len());
            // SAFETY: this reservation exclusively owns its span of these frames, which no other
            // lease overlaps; mutable access excludes prefix borrows, and writes only extend
            // the initialized prefix.
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
/// Leases may share a frame, but each reads and writes only its own span.
pub struct ScratchLease {
    metadata: MetadataPermit,
    frames: Vec<u32>,
    /// Frames charged to the stream but not yet allocated, at most `max_frames`.
    pending: u32,
    /// Offset of byte 0 in the first frame, below `FRAME_BYTES`: nonzero only for a lease
    /// packed into a shared one. Both fit in the word one count took before, so the lease
    /// size, which the metadata quota charges, is unchanged.
    base: u32,
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

    /// The frame, in-frame offset and length of lease bytes `offset..end`, clipped to the
    /// frame boundary. A packed lease lies within one frame, so `base + len <= FRAME_BYTES`.
    fn locate(&self, offset: usize, end: usize) -> (u32, usize, usize) {
        let at = self.base as usize + offset;
        let frame_offset = at % FRAME_BYTES;
        let len = (end - offset).min(FRAME_BYTES - frame_offset);
        (self.frames[at / FRAME_BYTES], frame_offset, len)
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
        let (frame, frame_offset, len) = self.locate(offset, self.len);
        // SAFETY: complete consumed the only writer after initializing the entire span.
        // This lease references every frame it spans until drop and retains the pool mapping.
        Ok(unsafe { stream.pool.slice(frame, frame_offset, len) })
    }
}

impl Drop for ScratchLease {
    fn drop(&mut self) {
        let stream = &self.metadata.stream;
        let mut state = stream.state.lock();
        for frame in self.frames.drain(..) {
            // A shared frame is charged once, until its last lease releases it.
            if stream.pool.release(frame) {
                state.scratch_frames -= 1;
                if state.scratch_open.is_some_and(|(open, _)| open == frame) {
                    state.scratch_open = None;
                }
            }
        }
        state.scratch_frames -= self.pending as usize;
    }
}
