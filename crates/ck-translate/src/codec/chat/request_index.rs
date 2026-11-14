use crate::codec::{Error, Result};
use ck_stream::MetadataPermit;
use std::{collections::HashMap, hash::Hash};

/// A hash map charged at an upper bound of its table's allocation. Growth reserves the new
/// table's bound on the permit before allocating it, so a quota failure allocates nothing.
pub struct Index<K, V> {
    map: HashMap<K, V>,
    /// The bound charged for the current table; zero before the first allocation.
    bytes: usize,
}

impl<K: Eq + Hash, V> Index<K, V> {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            bytes: 0,
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.map.get_mut(key)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    pub fn remove(&mut self, key: &K) {
        self.map.remove(key);
    }

    /// Upper bound of the allocation of a fresh table built for `capacity` entries. hashbrown,
    /// behind `HashMap`, allocates a power of two of buckets: `capacity` × 8/7 rounded up, or at
    /// most 16 below 15 entries. Each bucket holds an entry and a control byte; a trailing group
    /// of at most 16 control bytes follows, after padding of less than 16 bytes.
    fn bound(capacity: usize) -> Option<usize> {
        let buckets = (capacity.checked_mul(8)? / 7).checked_next_power_of_two()?.max(16);
        buckets.checked_mul(std::mem::size_of::<(K, V)>() + 1)?.checked_add(32)
    }

    /// Inserts an entry, first moving a full map into a fresh table for twice its entries. The
    /// permit, which holds this index's charge, grows by the new table's bound before the table
    /// is allocated and drops the old table's bound once that table is freed. `capacity()` is
    /// `len` plus the growth room hashbrown has left after tombstones, so `len == capacity` is
    /// exactly the point where hashbrown would otherwise grow the table itself.
    pub fn insert(&mut self, permit: &mut MetadataPermit, key: K, value: V) -> Result<()> {
        if self.map.len() == self.map.capacity() {
            let capacity = self.map.len().checked_mul(2).ok_or(Error::Limit)?.max(4);
            let bytes = Self::bound(capacity).ok_or(Error::Limit)?;
            let before = permit.bytes();
            permit.resize(before.checked_add(bytes).ok_or(Error::Limit)?)?;
            let mut map = HashMap::new();
            if map.try_reserve(capacity).is_err() {
                permit.resize(before)?;
                return Err(Error::Limit);
            }
            map.extend(self.map.drain());
            self.map = map;
            permit.resize(permit.bytes() - self.bytes)?;
            self.bytes = bytes;
        }
        self.map.insert(key, value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    thread_local! {
        static BYTES: Cell<usize> = const { Cell::new(0) };
    }

    /// Counts the bytes allocated on the current thread, so parallel tests do not disturb it.
    struct Counting;

    // SAFETY: forwards to the system allocator unchanged; the counter is a const thread-local
    // that never allocates.
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let _ = BYTES.try_with(|b| b.set(b.get() + layout.size()));
            unsafe { System.alloc(layout) }
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let _ = BYTES.try_with(|b| b.set(b.get() + new_size));
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    #[global_allocator]
    static GLOBAL: Counting = Counting;

    fn allocated() -> usize {
        BYTES.with(Cell::get)
    }

    struct Pool {
        store: ck_stream::Store,
        directory: std::path::PathBuf,
    }

    impl Pool {
        fn new(metadata: usize) -> Self {
            let directory = std::env::temp_dir().join(format!("ck-index-{}", uuid::Uuid::new_v4()));
            let mut config = ck_stream::Config::new(directory.clone());
            config.pool_bytes = Some(32 * ck_stream::FRAME_BYTES);
            config.max_event_bytes = 16 * ck_stream::FRAME_BYTES;
            config.max_metadata_bytes = metadata;
            config.header_slots = 8;
            config.workers = 1;
            let clock = std::sync::Arc::new(ck_stream::ManualClock::default());
            let store = ck_stream::Store::with_clock(config, clock).unwrap();
            Self { store, directory }
        }
    }

    impl Drop for Pool {
        fn drop(&mut self) {
            self.store.shutdown().unwrap();
            let _ = std::fs::remove_dir_all(&self.directory);
        }
    }

    /// Inserts `entries` keys one by one and checks that every table the index allocates fits
    /// in the bytes it charged, and that the permit holds exactly that charge.
    fn charge_covers_allocation<K: Eq + Hash, V>(entries: usize, entry: impl Fn(usize) -> (K, V)) {
        let pool = Pool::new(64 * 1024 * 1024);
        let mut owner = pool.store.create().unwrap();
        let mut permit = owner.reserve_metadata(0).unwrap();
        let mut index = Index::new();
        let mut grown = 0;
        for key in 0..entries {
            let before = (allocated(), index.map.capacity());
            let (key, value) = entry(key);
            index.insert(&mut permit, key, value).unwrap();
            if index.map.capacity() != before.1 {
                grown += 1;
                let table = allocated() - before.0;
                assert!(table <= index.bytes, "{table} B allocated, {} B charged", index.bytes);
            }
            assert_eq!(permit.bytes(), index.bytes);
        }
        assert!(grown > 4);
    }

    #[test]
    fn charge_covers_every_table_allocation() {
        // The children, objects and members indexes of `read_request`.
        charge_covers_allocation(4096, |i| (i, (i, Some(i))));
        charge_covers_allocation(4096, |i| (i, i as u32));
        charge_covers_allocation(4096, |i| ((i, 1u32, i as u64), i));
    }

    /// Removals can leave tombstones that use growth room without changing `len`; hashbrown
    /// then resizes a table more than half full. Every table allocated while keys churn in a
    /// nearly full index must still fit the charged bound.
    #[test]
    fn charge_covers_tables_grown_after_removals() {
        let pool = Pool::new(64 * 1024 * 1024);
        let mut owner = pool.store.create().unwrap();
        let mut permit = owner.reserve_metadata(0).unwrap();
        let mut index: Index<usize, u32> = Index::new();
        let mut key = 0;
        while key < 1000 || index.map.len() + 1 < index.map.capacity() {
            index.insert(&mut permit, key, 0).unwrap();
            key += 1;
        }
        let oldest = key - index.map.len();
        for step in 0..4096 {
            let before = allocated();
            index.remove(&(oldest + step));
            index.insert(&mut permit, key + step, 0).unwrap();
            let table = allocated() - before;
            assert!(table <= index.bytes, "{table} B allocated, {} B charged", index.bytes);
            assert_eq!(permit.bytes(), index.bytes);
        }
    }

    #[test]
    fn growth_past_the_quota_fails_before_allocating() {
        let pool = Pool::new(1024);
        let mut owner = pool.store.create().unwrap();
        let mut permit = owner.reserve_metadata(0).unwrap();
        let mut index: Index<(usize, u32, u64), usize> = Index::new();
        let mut key = 0;
        let failure = loop {
            let before = (allocated(), permit.bytes(), index.map.capacity());
            match index.insert(&mut permit, (key, 0, 0), key) {
                Ok(()) => key += 1,
                Err(error) => break (error, before),
            }
        };
        let (error, (bytes, charged, capacity)) = failure;
        assert!(matches!(error, Error::Storage(ck_stream::Error::StorageExhausted)));
        assert_eq!(allocated(), bytes);
        assert_eq!(
            (permit.bytes(), index.map.capacity(), index.map.len()),
            (charged, capacity, key)
        );
        assert!(key >= 4);
    }
}
