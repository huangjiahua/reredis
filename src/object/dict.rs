use std::collections::HashMap;
use crate::object::RobjPtr;
use core::borrow::Borrow;
use std::ops::Deref;

const DICT_HT_INITIAL_SIZE: usize = 4;

fn next_power(size: usize) -> usize {
    let mut i = DICT_HT_INITIAL_SIZE;

    if size >= std::usize::MAX {
        return std::usize::MAX;
    }

    loop {
        if i >= size {
            return i;
        }
        i *= 2;
    }
}

struct DictEntry<K: Default + PartialEq, V: Default> {
    pub key: K,
    pub value: V,
    pub next: Option<Box<DictEntry<K, V>>>,
}

impl<K, V> DictEntry<K, V>
    where K: Default + PartialEq, V: Default
{
    fn new(key: K, value: V) -> Self {
        DictEntry {
            key,
            value,
            next: None,
        }
    }
}

struct DictEntryIterator<'a, K: Default + PartialEq, V: Default> {
    next: Option<&'a Box<DictEntry<K, V>>>,
}

impl<'a, K, V> Iterator for DictEntryIterator<'a, K, V>
    where K: Default + PartialEq, V: Default
{
    type Item = &'a Box<DictEntry<K, V>>;
    fn next(&mut self) -> Option<Self::Item> {
        let curr = self.next.take();
        if let Some(e) = curr {
            self.next = e.next.as_ref();
            return Some(e);
        }
        None
    }
}


struct DictTable<K: Default + PartialEq, V: Default> {
    pub table: Vec<Option<Box<DictEntry<K, V>>>>,
    pub size: usize,
    pub size_mask: usize,
    pub used: usize,
}

impl<K, V> DictTable<K, V>
    where K: Default + PartialEq, V: Default
{
    fn new() -> DictTable<K, V> {
        DictTable {
            table: vec![],
            size: 0,
            size_mask: 0,
            used: 0,
        }
    }

    fn iter(&self, index: usize) -> DictEntryIterator<K, V> {
        DictEntryIterator { next: self.table[index].as_ref() }
    }

    fn insert_head(&mut self, idx: usize, mut entry: Box<DictEntry<K, V>>) {
        match self.table[idx] {
            None => self.table[idx] = Some(entry),
            Some(_) => {
                let curr_head = self.table[idx].take().unwrap();
                entry.next = Some(curr_head);
                self.table[idx] = Some(entry);
            }
        }
    }
}

struct HashDict<K: Default + PartialEq, V: Default> {
    ht: [DictTable<K, V>; 2],
    rehash_idx: i32,
    iterators: i32,
    dict_can_resize: bool,
    hash_seed: u64,
    hash: fn(&K, u64) -> usize,
}

impl<K, V> HashDict<K, V>
    where K: Default + PartialEq, V: Default
{
    pub fn new(f: fn(&K, u64) -> usize, hash_seed: u64) -> HashDict<K, V> {
        let table1: DictTable<K, V> = DictTable::new();
        let table2: DictTable<K, V> = DictTable::new();
        HashDict {
            ht: [table1, table2],
            rehash_idx: -1,
            iterators: 0,
            dict_can_resize: true,
            hash_seed,
            hash: f,
        }
    }

    pub fn find(&self, key: &K) -> Option<&Box<DictEntry<K, V>>> {
        if self.ht[0].size == 0 {
            return None;
        }

        let h = self.hash_value(key);

        for table in 0..2 {
            let idx = h & self.ht[table].size_mask;

            if let Some(he) = self.ht[table]
                .iter(idx)
                .filter(|e| e.key == *key)
                .next() {
                return Some(he);
            }

            // if the dict is not rehashing, the ht[1]
            // must be empty, there is no need to go there
            if !self.is_rehashing() {
                return None;
            }
        }
        // not found in both ht[0] and ht[1]
        None
    }

    pub fn find_by_mut(&mut self, key: &K) -> Option<&Box<DictEntry<K, V>>> {
        if self.ht[0].size == 0 {
            return None;
        }

        let h = self.hash_value(&key);
        self.rehash_step_if_needed();

        for table in 0..2 {
            let idx = h & self.ht[table].size_mask;

            if let Some(he) = self.ht[table]
                .iter(idx)
                .filter(|e| e.key == *key)
                .next() {
                return Some(he);
            }

            // if the dict is not rehashing, the ht[1]
            // must be empty, there is no need to go there
            if !self.is_rehashing() {
                return None;
            }
        }
        // not found in both ht[0] and ht[1]
        None
    }

    pub fn fetch_value(&mut self, key: &K) -> Option<&V> {
        if let Some(e) = self.find(key) {
            return Some(&e.value);
        }
        None
    }

    pub fn fetch_value_by_mut(&mut self, key: &K) -> Option<&V> {
        if let Some(e) = self.find_by_mut(key) {
            return Some(&e.value);
        }
        None
    }

    pub fn iter(&self) -> HashDictIterator<K, V> {
        let mut table = 0usize;
        let mut index = 0usize;
        let mut break_outer = false;

        for t in 0..2 {
            for (i, v) in self.ht[t].table
                .iter()
                .enumerate() {
                if v.is_some() {
                    table = t;
                    index = i;
                    break_outer = true;
                    break;
                }
            }
            if break_outer || !self.is_rehashing() {
                break;
            }
        }

        HashDictIterator {
            d: self,
            table,
            index,
            save: false,
            entry: self.ht[table].table[index].as_ref(),
        }
    }

    pub fn add(&mut self, key: K, value: V) -> Result<(), ()> {
        let entry = self.add_raw(key);

        match entry {
            Some(entry) => {
                Self::set_val(entry, value);
                Ok(())
            }
            None => Err(()),
        }
    }

    pub fn replace(&mut self, mut key: K, mut value: V) -> bool {
        self.rehash_step_if_needed();
        self.expand_if_needed().unwrap();

        let idx = self.hash_value(&key);
        let mut entry: DictEntry<K, V>;
        let ht: &mut DictTable<K, V>;

        match self.try_update(key, value) {
            None => return false,
            Some((k, v)) => {
                key = k;
                value = v;
            }
        }

        self.rehash_step_if_needed();

        entry = DictEntry::new(key, value);
        let mut entry = Box::new(entry);

        ht = self.get_working_ht();
        let idx = idx & ht.size_mask;

        ht.insert_head(idx, entry);
        ht.used += 1;
        true
    }

    pub fn enable_resize(&mut self) {
        self.dict_can_resize = true;
    }

    pub fn disable_resize(&mut self) {
        self.dict_can_resize = false;
    }

    pub fn set_hash_function_seed(&mut self, seed: u64) {
        self.hash_seed = seed;
    }

    pub fn get_hash_function_seed(&self) -> u64 {
        self.hash_seed
    }

    fn set_val(entry: &mut Box<DictEntry<K, V>>, value: V) {
        entry.value = value;
    }

    fn set_key(entry: &mut Box<DictEntry<K, V>>, key: K) {
        entry.key = key;
    }

    fn add_raw(&mut self, key: K) -> Option<&mut Box<DictEntry<K, V>>> {
        let index;
        let mut entry: DictEntry<K, V>;
        let ht: &mut DictTable<K, V>;

        self.rehash_step_if_needed();

        match self.key_index(&key) {
            Err(_) => return None,
            Ok(i) => index = i,
        }

        ht = self.get_working_ht();

        entry = DictEntry::new(key, Default::default());
        let mut entry = Box::new(entry);

        ht.insert_head(index, entry);
        ht.used += 1;
        ht.table[index].as_mut()
    }

    fn is_rehashing(&self) -> bool {
        self.rehash_idx != -1
    }

    fn rehash_step(&mut self) {
        self.rehash(1);
    }

    fn rehash(&mut self, n: usize) -> bool {
        if !self.is_rehashing() {
            return false;
        }

        for i in 0..n {
            let idx: usize;

            if self.ht[0].used == 0 {
                self.ht.swap(0, 1);
                self.rehash_idx = -1;
                return false;
            }

            assert!((self.rehash_idx as usize) < self.ht[0].size);

            while let None = self.ht[0].table[self.rehash_idx as usize] {
                self.rehash_idx += 1;
            }

            idx = self.rehash_idx as usize;

            while let Some(mut e) = self.ht[0].table[idx].take() {
                let h: usize;
                let next = e.next.take();

                if let Some(de) = next {
                    self.ht[0].table[idx] = Some(de);
                }

                h = self.hash_value(&e.key) & self.ht[1].size_mask;

                self.ht[1].insert_head(h, e);
                self.ht[0].used -= 1;
                self.ht[1].used += 1;
            }
        }

        true
    }

    fn key_index(&mut self, key: &K) -> Result<usize, ()> {
        let h;
        let mut idx = 0;

        self.expand_if_needed()?;

        h = self.hash_value(key);

        for table in 0..2 {
            idx = h & self.ht[table].size_mask;

            if let Some(_) = self.ht[table]
                .iter(idx)
                .filter(|e| e.key == *key)
                .next() {
                return Err(());
            }

            if !self.is_rehashing() {
                break;
            }
        }
        Ok(idx)
    }

    fn expand_if_needed(&mut self) -> Result<(), ()> {
        if self.is_rehashing() {
            return Ok(());
        }

        if self.ht[0].size == 0 {
            return self.expand(DICT_HT_INITIAL_SIZE);
        }

        if self.ht[0].used >= self.ht[0].size && self.dict_can_resize {
            return self.expand(self.ht[0].used * 2);
        }

        Ok(())
    }

    fn expand(&mut self, size: usize) -> Result<(), ()> {
        let real_size = next_power(size);
        let table: usize;
        let mut new_table: Vec<Option<Box<DictEntry<K, V>>>>;

        if self.is_rehashing() || self.ht[0].size >= size {
            return Err(());
        }

        new_table = Vec::with_capacity(real_size);

        for _ in 0..real_size {
            new_table.push(None);
        }

        table = if self.ht[0].size == 0 {
            0
        } else {
            1
        };

        self.ht[table].size = real_size;
        self.ht[table].size_mask = real_size - 1;
        self.ht[table].table = new_table;

        if table == 1 {
            self.rehash_idx = 0;
        }

        Ok(())
    }

    fn hash_value(&self, key: &K) -> usize {
        self.hash.borrow()(key, self.hash_seed)
    }

    fn rehash_step_if_needed(&mut self) {
        if self.is_rehashing() {
            self.rehash_step();
        }
    }

    fn get_working_ht(&mut self) -> &mut DictTable<K, V> {
        if self.is_rehashing() {
            &mut self.ht[1]
        } else {
            &mut self.ht[0]
        }
    }

    fn try_update(&mut self, key: K, value: V) -> Option<(K, V)> {
        let idx = self.hash_value(&key);
        for table in 0..2 {
            let idx = idx & self.ht[table].size_mask;
            let mut he = self.ht[table].table[idx].as_mut();
            while let Some(e) = he {
                if e.key == key {
                    return None;
                }
                he = e.next.as_mut();
            }

            if !self.is_rehashing() {
                break;
            }
        }
        Some((key, value))
    }
}

struct HashDictIterator<'a, K: Default + PartialEq, V: Default> {
    d: &'a HashDict<K, V>,
    table: usize,
    index: usize,
    save: bool,
    entry: Option<&'a Box<DictEntry<K, V>>>,
}

impl<'a, K, V> Iterator for HashDictIterator<'a, K, V>
    where K: Default + PartialEq, V: Default {
    type Item = &'a Box<DictEntry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        let en = self.entry.take();
        let mut ret = None;

        match en {
            None => return None,
            Some(e) => {
                self.entry = e.next.as_ref();
                // the next is set, don't need to worry
                // about it
                if self.entry.is_some() {
                    return Some(e);
                }
                ret = Some(e);
            }
        }

        self.index += 1;

        while self.table < 2 {
            // let's find a valid entry in ht[0]
            if self.index < self.d.ht[self.table].size {
                while let None = self.d.ht[self.table].table[self.index] {
                    self.index += 1;
                    if self.index >= self.d.ht[self.table].size {
                        break;
                    }
                }
            }

            if self.index < self.d.ht[self.table].size {
                // found
                self.entry = self.d.ht[self.table].table[self.index]
                    .as_ref();
                return Some(ret.unwrap());
            }

            // the ht[1] is empty, no need to go there
            if !self.d.is_rehashing() {
                break;
            }

            // let's go to the ht[1]
            self.table += 1;
            self.index = 0;
        }

        Some(ret.unwrap())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn int_hash_func(i: &usize, seed: u64) -> usize {
        i.clone()
    }


    #[test]
    fn create_a_hash_dict() {
        let hd: HashDict<usize, usize> = HashDict::new(int_hash_func, 0);
        let f = hd.hash.borrow();
        assert_eq!(f(&1, 0), 1);
        assert_eq!(hd.iterators, 0);
        assert_eq!(hd.rehash_idx, -1);
    }

    #[test]
    fn add_some_value() {
        let mut hd: HashDict<usize, usize> = HashDict::new(int_hash_func, 0);
        hd.add(3, 1).unwrap();
        let r = hd.add(3, 1);
        if let Ok(_) = r {
            panic!("Wrong")
        }
    }

    #[test]
    fn simple_add_and_find() {
        let mut hd: HashDict<usize, usize> = HashDict::new(int_hash_func, 0);
        hd.add(3, 4).unwrap();
        hd.add(4, 5).unwrap();
        hd.add(5, 6).unwrap();
        hd.add(8, 9).unwrap();
        let entry = hd.find(&3).unwrap();
        assert_eq!(entry.value, 4);
        let entry = hd.find(&4).unwrap();
        assert_eq!(entry.value, 5);
        let entry = hd.find(&5).unwrap();
        assert_eq!(entry.value, 6);
        let entry = hd.find(&8).unwrap();
        assert_eq!(entry.value, 9);

        let entry = hd.find(&2);
        if let Some(_) = entry {
            panic!("Wrong");
        }
    }

    #[test]
    fn ht_should_resize() {
        let mut hd: HashDict<usize, usize> = HashDict::new(int_hash_func, 0);
        // insert 4 to 4
        for i in 0..4 {
            hd.add(i, i + 1).unwrap();
            assert!(!hd.is_rehashing());
        }
        // insert 1 to 5; find 4 >= 4 and do rehashing
        hd.add(7, 8).unwrap();
        assert!(hd.is_rehashing());

        // do rehash for 4 times and there is 0 in the ht[0]
        for i in 0..4 {
            hd.find_by_mut(&3);
            assert!(hd.is_rehashing());
        }
        // ht[0] now has 5
        hd.find_by_mut(&3);
        assert!(!hd.is_rehashing());

        // insert 3 to 8
        for i in 0..3 {
            hd.add(50 + i, i + 1).unwrap();
            assert!(!hd.is_rehashing());
        }
        // insert 1 to 9; find 8 >= 8 and do rehashing
        hd.add(101, 102).unwrap();
        assert!(hd.is_rehashing());

        // do rehash for 1 time and there is still 7 in the ht[0]
        hd.find_by_mut(&3);

        for i in 0..5 {
            hd.add(150 + i, i + 1).unwrap();
            assert!(hd.is_rehashing());
        }
        hd.add(175, 101).unwrap();
        assert!(!hd.is_rehashing());

        hd.add(176, 101).unwrap();
        assert!(!hd.is_rehashing());
        hd.add(177, 101).unwrap();
        assert!(hd.is_rehashing());
    }

    #[test]
    fn simple_replace() {
        let mut hd: HashDict<usize, usize> = HashDict::new(int_hash_func, 0);
        for i in 0..5 {
            assert!(hd.replace(i, i + 1));
        }

        for i in 0..5 {
            let r = hd.find(&i).unwrap();
            assert_eq!(r.value, i + 1);
        }

        let a = 1;
        let b = 2;
        hd.replace(a, b);
    }

    #[test]
    fn simple_iterator_test() {
        let mut hd: HashDict<usize, usize> = HashDict::new(int_hash_func, 0);
        for i in 0..4 {
            hd.add(i, i).unwrap();
        }

        let mut cnt = 0;

        for en in hd.iter() {
            assert_eq!(en.key, en.value);
            cnt += 1;
        }

        assert_eq!(cnt, 4);
    }

    #[test]
    fn iterator_test_when_rehashing() {
        let mut hd: HashDict<usize, usize> = HashDict::new(int_hash_func, 0);
        let mut cnt = 0;

        for i in 0..100 {
            hd.add(i, i).unwrap();
            cnt += 1;
            if hd.is_rehashing() {
                break;
            }
        }

        for en in hd.iter() {
            assert_eq!(en.key, en.value);
            cnt -= 1;
        }

        assert_eq!(cnt, 0);
    }

    #[test]
    fn next_power_test() {
        assert_eq!(next_power(3), 4);
        assert_eq!(next_power(5), 8);
        assert_eq!(next_power(513), 1024);
        assert_eq!(next_power(std::usize::MAX), std::usize::MAX);
    }
}

