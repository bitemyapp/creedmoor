use get_size::GetSize;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use sled::{Db, Transactional, Tree};
use sled::transaction::TransactionalTree;

#[derive(Debug)]
struct DiskEntry {
    /// The size in bytes of the stored data on disk.
    size: usize,
}

use thiserror::Error;

pub type Result<T> = core::result::Result<T, CreedmoorError>;

#[derive(Error, Debug)]
pub enum CreedmoorError {
    #[error("Got error on sled operation, was: {0}")]
    SledError(#[from] sled::Error),
    #[error("Got error on sled transaction, was: {0}")]
    SledTransactionError(#[from] sled::transaction::TransactionError),
    #[error("Object size too large: {0}")]
    CacheObjectSizeTooLarge(usize),
}

/// Multi-layer LRU cache: memory + sled (disk).
pub struct MultiLayerCache {
    /// `memory_budget` just gets handed off to sled's [`cache_capacity`]
    memory_budget: AtomicUsize,
    disk_budget: AtomicUsize,
    disk_usage: AtomicUsize,
    /// Sled database for on-disk storage
    db: Db,
}

impl MultiLayerCache {
    /// Create a new multi-layer cache backed by sled on disk.
    ///
    /// * `memory_budget`: max bytes in memory
    /// * `disk_budget`: max bytes on disk
    /// * `sled_path`: path to the sled database
    pub fn new(memory_budget: usize, disk_budget: usize, sled_path: impl AsRef<Path>) -> Result<Self> {
        let path = sled_path.as_ref().to_path_buf();
        // let db = sled::open(&path)?;
        let db =
            sled::Config::new()
                .cache_capacity(memory_budget as u64)
                .use_compression(true)
                .compression_factor(9)
                .path(path).open()?;
        Ok(Self {
            memory_budget: memory_budget.into(),
            disk_budget: disk_budget.into(),
            disk_usage: 0.into(),
            db,
        })
    }

    const OBJECT_LRU: &'static [u8; 10] = b"object_lru";
    const OBJECT_DATA: &'static [u8; 11] = b"object_data";

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let size = value.get_size();
        if size > self.disk_budget.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(CreedmoorError::CacheObjectSizeTooLarge(size));
        }
        let instant = std::time::Instant::now();
        let instant_bytes = instant.elapsed().as_nanos().to_be_bytes();
        // convert key_and_size to bytes
        let mut key_and_size = key.to_vec();
        key_and_size.extend_from_slice(&size.to_be_bytes());
        let disk_usage = self.disk_usage.load(std::sync::atomic::Ordering::Relaxed);
        let target_storage = size + disk_usage;
        let excess = target_storage - disk_usage;
        let mut object_data = self.db.open_tree(Self::OBJECT_DATA)?;
        let mut object_lru = self.db.open_tree(Self::OBJECT_LRU)?;
        let disk_budget = self.disk_budget.load(std::sync::atomic::Ordering::Relaxed);
        (&object_lru, &object_data).transaction(|(lru, data)| {
            if target_storage > disk_budget {
                self.evict_bytes(lru, data, excess);
            }
            self.disk_usage.fetch_add(size, std::sync::atomic::Ordering::Relaxed);
            data.insert(key, value)?;
            // Insert key and size so we don't have to re-compute object size on eviction
            lru.insert(&instant_bytes, key_and_size)?;
            Ok(())
        })?;
        Ok(())
    }

    fn evict_bytes(&mut self, lru_tree: &TransactionalTree, data_tree: &TransactionalTree, excess: usize) {
        let mut total_evicted = 0;
        let mut keys_to_evict = Vec::new();
        for (key, value) in lru_tree.iter()? {
            let size = value.pop();
            total_evicted += size;
            keys_to_evict.push(key.to_vec());
            if total_evicted >= excess {
                break;
            }
        }
        for key in keys_to_evict {
            let size = data_tree.remove(&key).unwrap().unwrap();
        }
        self.disk_usage.fetch_sub(total_evicted, std::sync::atomic::Ordering::Relaxed);
    }
}
