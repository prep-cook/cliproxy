//! Deterministic per-stream payload bytes and the check that verifies them.

use anyhow::{Result, ensure};
use ck_stream::FRAME_BYTES;
use std::sync::LazyLock;

/// Payload byte `i` of stream `id` is `(i + 97 × id) mod 251`. The period is
/// prime and coprime to the frame size, so a stale, shifted or cross-stream
/// frame fails the comparison that a constant byte would pass.
const PERIOD: usize = 251;
static PATTERN: LazyLock<Vec<u8>> = LazyLock::new(|| (0..PERIOD + FRAME_BYTES).map(|i| (i % PERIOD) as u8).collect());
pub fn pattern(id: u64, offset: usize, len: usize) -> &'static [u8] {
    let phase = (offset + (id % PERIOD as u64) as usize * 97) % PERIOD;
    &PATTERN[phase..phase + len]
}
/// Compares every visited byte with the expected pattern at its logical offset.
pub struct Check {
    pub id: u64,
    pub offset: usize,
    pub ok: bool,
}
impl Check {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            offset: 0,
            ok: true,
        }
    }
    pub fn visit(&mut self, bytes: &[u8]) {
        self.ok &= bytes == pattern(self.id, self.offset, bytes.len());
        self.offset += bytes.len();
    }
    pub fn finish(&self, expected: usize) -> Result<()> {
        ensure!(
            self.ok && self.offset == expected,
            "payload mismatch: {} of {expected} bytes, pattern intact: {}",
            self.offset,
            self.ok
        );
        Ok(())
    }
}
