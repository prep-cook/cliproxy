use super::Table;
use crate::codec::{Error, Result};
use ck_stream::Writer;
use std::hash::{BuildHasher, Hash, Hasher};

/// Table positions by key hash, charged to the writer quota, so finding a block probes one
/// cluster instead of rescanning every retained block. Callers confirm the full key.
pub(super) struct Index {
    slots: Table<(u64, usize)>,
    len: usize,
    state: std::collections::hash_map::RandomState,
}
const EMPTY: usize = usize::MAX;
impl Index {
    pub fn new(owner: &mut Writer) -> Result<Self> {
        Ok(Self {
            slots: Table::new(owner)?,
            len: 0,
            state: Default::default(),
        })
    }
    pub fn hash(&self, key: impl Hash) -> u64 {
        self.state.hash_one(key)
    }
    pub fn hasher(&self) -> impl Hasher {
        self.state.build_hasher()
    }
    pub fn insert(&mut self, owner: &mut Writer, hash: u64, position: usize) -> Result<()> {
        if (self.len + 1) * 2 > self.slots.len() {
            let mut slots = Table::new(owner)?;
            for _ in 0..(self.slots.len() * 2).max(8) {
                slots.push((0, EMPTY))?;
            }
            let old = std::mem::replace(&mut self.slots, slots);
            for &(hash, position) in old.iter().filter(|slot| slot.1 != EMPTY) {
                self.place(hash, position);
            }
        }
        self.place(hash, position);
        self.len += 1;
        Ok(())
    }
    fn place(&mut self, hash: u64, position: usize) {
        let mut slot = self.home(hash);
        while self.slots[slot].1 != EMPTY {
            slot = self.next(slot);
        }
        self.slots[slot] = (hash, position);
    }
    /// The smallest position under `hash` that `matches` accepts, as a scan in order would find.
    pub fn find(&self, hash: u64, mut matches: impl FnMut(usize) -> Result<bool>) -> Result<Option<usize>> {
        let mut found: Option<usize> = None;
        if self.slots.is_empty() {
            return Ok(found);
        }
        let mut slot = self.home(hash);
        loop {
            let (h, position) = self.slots[slot];
            if position == EMPTY {
                break;
            }
            if h == hash && found.is_none_or(|f| position < f) && matches(position)? {
                found = Some(position);
            }
            slot = self.next(slot);
        }
        Ok(found)
    }
    /// Drops `position` from `hash`, shifting later cluster entries back so probes stay unbroken.
    pub fn remove(&mut self, hash: u64, position: usize) -> Result<()> {
        if self.slots.is_empty() {
            return Err(Error::Schema);
        }
        let mut hole = self.home(hash);
        while self.slots[hole] != (hash, position) {
            if self.slots[hole].1 == EMPTY {
                return Err(Error::Schema);
            }
            hole = self.next(hole);
        }
        let mut slot = self.next(hole);
        while self.slots[slot].1 != EMPTY {
            let home = self.home(self.slots[slot].0);
            // Move the entry into the hole unless its home lies cyclically in (hole, slot].
            let stays = if hole < slot {
                hole < home && home <= slot
            } else {
                hole < home || home <= slot
            };
            if !stays {
                self.slots[hole] = self.slots[slot];
                hole = slot;
            }
            slot = self.next(slot);
        }
        self.slots[hole] = (0, EMPTY);
        self.len -= 1;
        Ok(())
    }
    fn home(&self, hash: u64) -> usize {
        hash as usize & (self.slots.len() - 1)
    }
    fn next(&self, slot: usize) -> usize {
        (slot + 1) & (self.slots.len() - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Clustered hashes, growth and backward-shift removal agree with a scan in order.
    #[test]
    fn lookups_match_an_ordered_scan() {
        let directory = std::env::temp_dir().join(format!("ck-anthropic-index-{}", uuid::Uuid::new_v4()));
        let mut config = ck_stream::Config::new(directory.clone());
        config.pool_bytes = Some(32 * ck_stream::FRAME_BYTES);
        config.max_event_bytes = 16 * ck_stream::FRAME_BYTES;
        config.max_metadata_bytes = 1024 * 1024;
        config.header_slots = 8;
        config.workers = 1;
        let store =
            ck_stream::Store::with_clock(config, std::sync::Arc::new(ck_stream::ManualClock::default())).unwrap();
        let mut owner = store.create().unwrap();
        let mut index = Index::new(&mut owner).unwrap();
        let mut live: Vec<(u64, usize)> = Vec::new();
        let mut seed = 7u64;
        for position in 0..400 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            // Hashes 0..24 share few home slots, so clusters wrap and interleave.
            let hash = (seed >> 33) % 24 * 8;
            if seed >> 62 == 0 && !live.is_empty() {
                let (hash, position) = live.remove((seed >> 20) as usize % live.len());
                index.remove(hash, position).unwrap();
            } else {
                index.insert(&mut owner, hash, position).unwrap();
                live.push((hash, position));
            }
            for hash in (0..24).map(|h| h * 8) {
                let expected = live.iter().filter(|e| e.0 == hash).map(|e| e.1).min();
                assert_eq!(index.find(hash, |_| Ok(true)).unwrap(), expected);
            }
        }
        assert!(index.remove(1, 0).is_err());
        // Growth reinserts a cluster that wrapped past the last slot in slot order, so later
        // positions can come first in the probe; lookups still return the earliest.
        let mut index = Index::new(&mut owner).unwrap();
        for (hash, position) in [(15, 0), (15, 1), (3, 2), (4, 3), (5, 4)] {
            index.insert(&mut owner, hash, position).unwrap();
        }
        assert_eq!(index.find(15, |_| Ok(true)).unwrap(), Some(0));
        drop(index);
        drop(owner);
        drop(store);
        std::fs::remove_dir_all(directory).ok();
    }
}
