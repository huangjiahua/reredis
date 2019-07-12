use std::collections::HashMap;
use crate::object::RobjPtr;

struct DictEntry<K, V> {
    pub key: K,
    pub value: V,
    pub next: Option<Box<DictEntry<K, V>>>,
}

struct DictTable<K, V> {
    pub table: Vec<Option<DictEntry<K, V>>>,
    pub size_mask: i64,
    pub used: i64,
}

impl<K, V> DictTable<K, V> {
    fn new() -> DictTable<K, V> {
        DictTable {
            table: vec![],
            size_mask: 0,
            used: 0,
        }
    }
}

struct HashDict<K, V> {
    ht: [DictTable<K, V>; 2],
    rehash_idx: i32,
    iterators: i32,
    f: fn(K) -> i64,
}

impl<K, V> HashDict<K, V> {
    fn new(f: fn(K) -> i64) -> HashDict<K, V> {
        let table1: DictTable<K, V> = DictTable::new();
        let table2: DictTable<K, V> = DictTable::new();
        HashDict {
            ht: [table1, table2],
            rehash_idx: -1,
            iterators: 0,
            f,
        }
    }

    fn add_raw(&mut self, key: K, value: V) -> Option<&mut Box<DictEntry<K, V>>> {
        let index;
        let ht;
        let mut entry: Box<DictEntry<K, V>>;

        if self.is_rehashing() {
            self.rehash_step();
        }

        match self.key_index(&key) {
            Ok(i) => index = i,
            Err(_) => return None,
        }

        ht = match self.is_rehashing() {
            true => &mut self.ht[1],
            false => &mut self.ht[0],
        };

        entry = Box::new(DictEntry {
            key,
            value,
            next: None,
        });

        // TODO
        if let Some(n) = ht.table[index] {
            entry.next = Some(n);
        }
        ht.table[index] = entry;
        ht.used += 1;

        Some(&mut entry)
    }

    fn is_rehashing(&self) -> bool {
        unimplemented!()
    }

    fn rehash_step(&self) {
        unimplemented!()
    }

    fn key_index(&self, key: &K) -> Result<usize, ()> {}
}

struct HashDictIterator<'a, K, V> {
    d: &'a HashDict<K, V>,
    table: i32,
    index: i32,
    save: bool,
    entry: &'a Option<Box<DictEntry<K, V>>>,
}
