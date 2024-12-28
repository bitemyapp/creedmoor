use get_size::GetSize;
use std::collections::{HashMap, BTreeMap};
use std::time::Instant;

pub struct LruBytes<T> {
    // The maximum number of bytes that the cache can hold.
    capacity: usize,
    // The current number of bytes held by the cache.
    usage: usize,
    // The map that holds the data.
    map: HashMap<String, T>,
    // The list of keys in the order they were accessed.
    keys: BTreeMap<Instant, String>,
}

impl <T>LruBytes<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            usage: 0,
            map: HashMap::new(),
            keys: VecDeque::new(),
        }
    }

    pub fn get(&mut self, key: &str) -> Option<&T> {
        if let Some(value) = self.map.get(key) {
            Some(value)
        } else {
            None
        }
    }

    pub fn put(&mut self, key: String, value: T) {
        let size = value.get_size();
        if size > self.capacity {
            return;
        }

        // Evict items until we have enough space.
        while self.usage + size > self.capacity {
            if let Some(k) = self.keys.pop_back() {
                if let Some(v) = self.map.remove(&k) {
                    self.usage -= v.get_size();
                }
            }
        }

        // Add the new item.
        self.map.insert(key.clone(), value);
        self.keys.push_front(key);
        self.usage += size;
    }
}
