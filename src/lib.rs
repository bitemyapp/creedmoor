use get_size::GetSize;
use std::path::Path;
use sled::{Db, Transactional, Tree};
use sled::transaction::TransactionalTree;

use thiserror::Error;

pub type Result<T> = core::result::Result<T, CreedmoorError>;

#[derive(Error, Debug)]
pub enum CreedmoorError {
    #[error("Got error on sled operation, was: {0}")]
    SledError(#[from] sled::Error),
    #[error("Got error on sled transaction, was: {0}")]
    SledTransactionError(#[from] sled::transaction::TransactionError),
    #[error("Got error on sled unabortable transaction, was: {0}")]
    SledUnabortableTransactionError(#[from] sled::transaction::UnabortableTransactionError),
    #[error("Got error on sled conflictable transaction, was: {0}")]
    SledConflictableTransactionError(#[from] sled::transaction::ConflictableTransactionError),
    #[error("Object size too large: {0}")]
    CacheObjectSizeTooLarge(usize),
}

pub enum Op {
    Add,
    Sub,
}

/// Multi-layer LRU cache: memory + sled (disk).
pub struct MultiLayerCache {
    pub(crate) disk_budget: usize,
    /// Sled database for on-disk storage
    pub(crate) db: Db,
}

impl MultiLayerCache {
    pub(crate) const DISK_USAGE_TREE: &'static [u8; 10] = b"disk_usage";
    pub(crate) const DISK_USAGE_KEY: &'static [u8; 7] = b"current";
    pub(crate) const OBJECT_LRU: &'static [u8; 10] = b"object_lru";
    pub(crate) const OBJECT_DATA: &'static [u8; 11] = b"object_data";

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
            disk_budget,
            db,
        })
    }

    fn ivec_to_u64(ivec: sled::IVec) -> u64 {
        let ivec = ivec.as_ref();
        let ivec = u64::from_be_bytes([ivec[0], ivec[1], ivec[2], ivec[3], ivec[4], ivec[5], ivec[6], ivec[7]]);
        ivec
    }

    fn get_disk_usage_(&self) -> Option<usize> {
        let disk_usage = self.db.open_tree(Self::DISK_USAGE_TREE).unwrap();
        let current = disk_usage.get(Self::DISK_USAGE_KEY).unwrap();
        match current {
            Some(current) => {
                let current = current.as_ref();
                let current = u64::from_be_bytes([current[0], current[1], current[2], current[3], current[4], current[5], current[6], current[7]]);
                Some(current as usize)
            }
            None => None,
        }
    }

    fn populate_disk_usage(&self) -> usize {
        let current = match self.get_disk_usage_() {
            Some(current) => current,
            None => {
                let disk_usage = self.db.open_tree(Self::DISK_USAGE_TREE).unwrap();
                disk_usage.insert(Self::DISK_USAGE_KEY, &0u64.to_be_bytes()).unwrap();
                0
            }
        };
        current
    }

    fn get_disk_usage(&self) -> usize {
        self.populate_disk_usage()
    }

    fn update_disk_usage(&self, disk_usage_tree: &TransactionalTree, operand: Op, value: usize) -> Result<usize> {
        let result: Result<usize> = {
            let current = disk_usage_tree.get(Self::DISK_USAGE_KEY)?;
            let current = match current {
                Some(current) => {
                    let current = Self::ivec_to_u64(current);
                    current as usize
                }
                None => 0,
            };
            let new_value = match operand {
                Op::Add => current + value,
                Op::Sub => current - value,
            };
            disk_usage_tree.insert(Self::DISK_USAGE_KEY, &new_value.to_be_bytes())?;
            Ok(new_value)
        };
        Ok(result?)
    }

    fn fetch_add_disk_usage(&self, disk_usage_tree: &TransactionalTree, add: usize) -> Result<usize> {
        self.update_disk_usage(disk_usage_tree, Op::Add, add)
    }

    fn fetch_sub_disk_usage(&self, disk_usage_tree: &TransactionalTree, sub: usize) -> Result<usize> {
        self.update_disk_usage(disk_usage_tree, Op::Sub, sub)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let size = value.get_size();
        if size > self.disk_budget {
            return Err(CreedmoorError::CacheObjectSizeTooLarge(size));
        }
        let instant = std::time::Instant::now();
        let instant_bytes = instant.elapsed().as_nanos().to_be_bytes();
        // convert key_and_size to bytes
        let mut key_and_size = key.to_vec();
        key_and_size.extend_from_slice(&size.to_be_bytes());
        let disk_usage = self.get_disk_usage();
        let target_storage = size + disk_usage;
        let excess = target_storage - disk_usage;
        let object_data = self.db.open_tree(Self::OBJECT_DATA)?;
        let object_lru = self.db.open_tree(Self::OBJECT_LRU)?;
        let disk_usage = self.db.open_tree(Self::DISK_USAGE_TREE)?;
        let disk_budget = self.disk_budget;
        let (total_to_evict, keys_to_evict) = self.gather_keys_for_eviction(&object_lru, excess)?;
        (&object_lru, &object_data, &disk_usage).transaction(|(lru, data, disk_usage)| {
            // TODO: Fix-up the error types to purge the expect later
            println!("target_storage: {}, disk_budget: {}", target_storage, disk_budget);
            if target_storage > disk_budget {
                self.evict_bytes(disk_usage, data, &keys_to_evict, total_to_evict).expect("Failed to evict bytes");
            }
            self.fetch_add_disk_usage(disk_usage, size).expect("Failed to add disk usage");
            data.insert(key, value)?;
            // Insert key and size so we don't have to re-compute object size on eviction
            lru.insert(&instant_bytes, key_and_size.clone())?;
            Ok(())
        })?;
        Ok(())
    }

    fn gather_keys_for_eviction(&self, lru_tree: &Tree, excess: usize) -> Result<(usize, Vec<Vec<u8>>)> {
        let mut total_evicted = 0;
        let mut keys_to_evict = Vec::new();
        while total_evicted < excess {
            if let Some((key, value)) = lru_tree.pop_min()? {
                let value_vec = value.to_vec();
                // grab the last eight bytes and construct the size: usize, use split
                let (_key_bytes, size_bytes) = value_vec.split_at(value_vec.len() - 8);
                let size = usize::from_be_bytes([size_bytes[0], size_bytes[1], size_bytes[2], size_bytes[3], size_bytes[4], size_bytes[5], size_bytes[6], size_bytes[7]]);
                total_evicted += size;
                keys_to_evict.push(key.to_vec());
            } else {
                break;
            }
        }
        Ok((total_evicted, keys_to_evict))
    }

    fn evict_bytes(&self, disk_usage_tree: &TransactionalTree, data_tree: &TransactionalTree, keys_to_evict: &[Vec<u8>], total_to_evict: usize) -> Result<()> {
        for key in keys_to_evict {
            let _size = data_tree.remove(key.as_slice())?.expect(&format!("Failed to remove key marked for eviction, key was: {:?}", key));
        }
        self.fetch_sub_disk_usage(disk_usage_tree, total_to_evict)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn test_new() {
        let memory_budget = 1024;
        let disk_budget = 1024;
        let sled_path = PathBuf::from("/tmp/sled-test-new");
        let _cache = MultiLayerCache::new(memory_budget, disk_budget, sled_path.clone()).unwrap();
        fs::remove_dir_all(sled_path).unwrap();
    }

    #[test]
    fn test_put() {
        let memory_budget = 1024;
        let disk_budget = 1024;
        let sled_path = PathBuf::from("/tmp/sled-test-put");
        let mut cache = MultiLayerCache::new(memory_budget, disk_budget, sled_path.clone()).unwrap();
        let key = b"key";
        let value = b"value";
        cache.put(key, value).unwrap();
        fs::remove_dir_all(sled_path).unwrap();
    }

    #[test]
    fn test_size_limit() {
        let memory_budget = 1024;
        let disk_budget = 1024;
        let sled_path = PathBuf::from("/tmp/sled-test-size-limit");
        let mut cache = MultiLayerCache::new(memory_budget, disk_budget, sled_path.clone()).unwrap();
        for n in 0..1024u16 {
            let key = n.to_be_bytes();
            let value = n.to_be_bytes();
            cache.put(&key, &value).unwrap();
        }
        let data_tree = cache.db.open_tree(MultiLayerCache::OBJECT_DATA).unwrap();
        let (min_key, _) = data_tree.pop_min().unwrap().unwrap();
        let (max_key, _)  = data_tree.pop_max().unwrap().unwrap();
        let min_key = u16::from_be_bytes([min_key[0], min_key[1]]);
        let max_key = u16::from_be_bytes([max_key[0], max_key[1]]);
        assert_eq!(min_key, 0);
        assert_eq!(max_key, 511);
        fs::remove_dir_all(sled_path).unwrap();
    }
}