use std::ops::IndexMut;
use rand::Rng;

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

pub trait DictPartialEq<RHS: ?Sized = Self> {
    fn eq(&self, other: &RHS) -> bool;
}

struct DictEntry<K: DictPartialEq, V> {
    pub key: K,
    pub value: V,
    next: Option<Box<DictEntry<K, V>>>,
}

impl<K, V> DictEntry<K, V>
    where K: DictPartialEq
{
    fn new(key: K, value: V) -> Self {
        DictEntry {
            key,
            value,
            next: None,
        }
    }
}

struct DictEntryIterator<'a, K: DictPartialEq, V> {
    next: Option<&'a DictEntry<K, V>>,
}

impl<'a, K, V> Iterator for DictEntryIterator<'a, K, V>
    where K: DictPartialEq
{
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        self.next.take().map(|entry| {
            self.next = entry.next
                .as_ref()
                .map(|entry| &**entry);
            (&entry.key, &entry.value)
        })
    }
}

struct DictEntryIteratorMut<'a, K: DictPartialEq, V> {
    next: Option<&'a mut DictEntry<K, V>>,
}

impl<'a, K, V> Iterator for DictEntryIteratorMut<'a, K, V>
    where K: DictPartialEq {
    type Item = (&'a K, &'a mut V);

    fn next(&mut self) -> Option<Self::Item> {
        self.next.take().map(|entry| {
            self.next = entry.next
                .as_mut()
                .map(|entry| &mut **entry);
            (&entry.key, &mut entry.value)
        })
    }
}


struct DictTable<K: DictPartialEq, V> {
    pub table: Vec<Option<Box<DictEntry<K, V>>>>,
    pub size: usize,
    pub size_mask: usize,
    pub used: usize,
}

impl<K, V> DictTable<K, V>
    where K: DictPartialEq
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
        DictEntryIterator {
            next: self.table[index].as_ref().map(|entry| &**entry)
        }
    }

    fn iter_mut(&mut self, index: usize) -> DictEntryIteratorMut<K, V> {
        DictEntryIteratorMut {
            next: self.table.index_mut(index).as_mut().map(|entry| &mut **entry)
        }
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

pub struct Dict<K: DictPartialEq, V> {
    ht: [DictTable<K, V>; 2],
    rehash_idx: i32,
    iterators: i32,
    dict_can_resize: bool,
    hash_seed: u64,
    hash: fn(&K, u64) -> usize,
}

impl<K, V> Dict<K, V>
    where K: DictPartialEq
{
    pub fn new(f: fn(&K, u64) -> usize, hash_seed: u64) -> Dict<K, V> {
        let table1: DictTable<K, V> = DictTable::new();
        let table2: DictTable<K, V> = DictTable::new();
        Dict {
            ht: [table1, table2],
            rehash_idx: -1,
            iterators: 0,
            dict_can_resize: true,
            hash_seed,
            hash: f,
        }
    }

    pub fn len(&self) -> usize {
        self.ht[0].used + self.ht[1].used
    }

    pub fn find(&self, key: &K) -> Option<(&K, &V)> {
        if self.ht[0].size == 0 {
            return None;
        }

        let h = self.hash_value(key);

        for table in 0..2 {
            let idx = h & self.ht[table].size_mask;

            if let Some((k, v)) = self.ht[table]
                .iter(idx)
                .filter(|e| e.0.eq(key))
                .next() {
                return Some((k, v));
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

    pub fn find_by_mut(&mut self, key: &K) -> Option<(&K, &V)> {
        if self.ht[0].size == 0 {
            return None;
        }

        let h = self.hash_value(&key);
        self.rehash_step_if_needed();

        for table in 0..2 {
            let idx = h & self.ht[table].size_mask;

            if let Some((k, v)) = self.ht[table]
                .iter(idx)
                .filter(|e| e.0.eq(key))
                .next() {
                return Some((k, v));
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
            return Some(e.1);
        }
        None
    }

    pub fn fetch_value_by_mut(&mut self, key: &K) -> Option<&V> {
        if let Some(e) = self.find_by_mut(key) {
            return Some(e.1);
        }
        None
    }

    pub fn delete(&mut self, key: &K) -> Result<(K, V), ()> {
        let h: usize;

        if self.ht[0].size == 0 {
            return Err(());
        }

        self.rehash_step_if_needed();

        h = self.hash_value(key);

        for table in 0..2 {
            let idx = h & self.ht[table].size_mask;

            let mut prev: Option<&mut Box<DictEntry<K, V>>> = None;
            let mut he = self.ht[table].table[idx].take();

            while let Some(mut e) = he {
                if e.key.eq(key) {
                    match prev {
                        None => self.ht[table].table[idx] = e.next.take(),
                        Some(p) => p.next = e.next.take(),
                    }
                    self.ht[table].used -= 1;
                    return Ok((e.key, e.value));
                }

                he = e.next.take();
                prev = match prev {
                    None => {
                        self.ht[table].table[idx] = Some(e);
                        self.ht[table].table[idx].as_mut()
                    }
                    Some(k) => {
                        k.next = Some(e);
                        k.next.as_mut()
                    }
                }
            }

            if !self.is_rehashing() {
                break;
            }
        }

        Err(())
    }

    pub fn iter(&self) -> Iter<K, V> {
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

        Iter {
            d: self,
            table,
            index,
            save: false,
            entry: self.ht[table].table[index]
                .as_ref()
                .map(|e| &**e),
        }
    }

    pub fn add(&mut self, key: K, value: V) -> Result<(), ()> {
        let entry = self.add_raw(key, value);

        match entry {
            Some(_) => Ok(()),
            None => Err(()),
        }
    }

    pub fn replace(&mut self, mut key: K, mut value: V) -> bool {
        self.rehash_step_if_needed();
        self.expand_if_needed().unwrap();

        let idx = self.hash_value(&key);
        let entry: DictEntry<K, V>;
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
        let entry = Box::new(entry);

        ht = self.get_working_ht();
        let idx = idx & ht.size_mask;

        ht.insert_head(idx, entry);
        ht.used += 1;
        true
    }

    pub fn random_key_value(&self) -> (&K, &V) {
        assert!(self.len() > 0, "cannot generate random key value on empty dict");
        let mut rng = rand::thread_rng();
        let mut bucket = self.ht[0].size;
        if self.is_rehashing() {
            bucket += self.ht[1].size;
        }

        let mut which: usize = rng.gen_range(0, bucket);

        loop {
            let mut idx: usize = which;
            let mut list_len: usize = 0;

            let ht = if idx >= self.ht[0].size {
                idx -= self.ht[0].size;
                &self.ht[1]
            } else {
                &self.ht[0]
            };

            for _ in ht.iter(idx) {
                list_len += 1;
            }

            if list_len > 0 {
                let n = rng.gen_range(0, list_len);
                let kv = ht.iter(idx)
                    .skip(n)
                    .next()
                    .unwrap();
                return kv;
            }

            which = (which + 1) % bucket;
        }
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

    fn add_raw(&mut self, key: K, value: V) -> Option<&mut Box<DictEntry<K, V>>> {
        let index;
        let entry: DictEntry<K, V>;
        let ht: &mut DictTable<K, V>;

        self.rehash_step_if_needed();

        match self.key_index(&key) {
            Err(_) => return None,
            Ok(i) => index = i,
        }

        ht = self.get_working_ht();

        entry = DictEntry::new(key, value);
        let entry = Box::new(entry);

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

        for _ in 0..n {
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
                .filter(|e| e.0.eq(key))
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
        (&self.hash)(key, self.hash_seed)
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

            if let Some(v) = self.ht[table]
                .iter_mut(idx)
                .filter(|p| p.0.eq(&key))
                .next() {
                *v.1 = value;
                return None;
            }

            if !self.is_rehashing() {
                break;
            }
        }
        Some((key, value))
    }
}

pub struct Iter<'a, K: DictPartialEq, V> {
    d: &'a Dict<K, V>,
    table: usize,
    index: usize,
    save: bool,
    entry: Option<&'a DictEntry<K, V>>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
    where K: DictPartialEq {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let en = self.entry.take();
        let ret;

        match en {
            None => return None,
            Some(e) => {
                self.entry = e.next
                    .as_ref()
                    .map(|e| &**e);
                // the next is set, don't need to worry
                // about it
                if self.entry.is_some() {
                    return Some((&e.key, &e.value));
                }
                ret = Some((&e.key, &e.value));
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
                    .as_ref()
                    .map(|e| &**e);
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

    fn int_hash_func(i: &usize, _seed: u64) -> usize {
        i.clone()
    }

    impl DictPartialEq for usize {
        fn eq(&self, other: &Self) -> bool {
            *self == *other
        }
    }

    #[test]
    fn create_a_hash_dict() {
        let hd: Dict<usize, usize> = Dict::new(int_hash_func, 0);
        let f = &hd.hash;
        assert_eq!(f(&1, 0), 1);
        assert_eq!(hd.iterators, 0);
        assert_eq!(hd.rehash_idx, -1);
    }

    #[test]
    fn add_some_value() {
        let mut hd: Dict<usize, usize> = Dict::new(int_hash_func, 0);
        hd.add(3, 1).unwrap();
        let r = hd.add(3, 1);
        if let Ok(_) = r {
            panic!("Wrong")
        }
    }

    #[test]
    fn simple_add_and_find() {
        let mut hd: Dict<usize, usize> = Dict::new(int_hash_func, 0);
        hd.add(3, 4).unwrap();
        hd.add(4, 5).unwrap();
        hd.add(5, 6).unwrap();
        hd.add(8, 9).unwrap();
        let entry = hd.find(&3).unwrap();
        assert_eq!(*entry.1, 4);
        let entry = hd.find(&4).unwrap();
        assert_eq!(*entry.1, 5);
        let entry = hd.find(&5).unwrap();
        assert_eq!(*entry.1, 6);
        let entry = hd.find(&8).unwrap();
        assert_eq!(*entry.1, 9);

        let entry = hd.find(&2);
        if let Some(_) = entry {
            panic!("Wrong");
        }
    }

    #[test]
    fn ht_should_resize() {
        let mut hd: Dict<usize, usize> = Dict::new(int_hash_func, 0);
        // insert 4 to 4
        for i in 0..4 {
            hd.add(i, i + 1).unwrap();
            assert!(!hd.is_rehashing());
        }
        // insert 1 to 5; find 4 >= 4 and do rehashing
        hd.add(7, 8).unwrap();
        assert!(hd.is_rehashing());

        // do rehash for 4 times and there is 0 in the ht[0]
        for _ in 0..4 {
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
        let mut hd: Dict<usize, usize> = Dict::new(int_hash_func, 0);
        for i in 0..5 {
            assert!(hd.replace(i, i + 1));
        }

        for i in 0..5 {
            let r = hd.find(&i).unwrap();
            assert_eq!(*r.1, i + 1);
        }

        let a = 1;
        let b = 2;
        hd.replace(a, b);
    }

    #[test]
    fn replace_existed() {
        let mut hd: Dict<usize, usize> = Dict::new(int_hash_func, 0);
        for i in 0..5000 {
            hd.replace(i, i + 1);
        }

        for i in 0..5000 {
            let r = hd.find(&i).unwrap();
            assert_eq!(*r.1, i + 1);
        }

        for i in 0..5000 {
            hd.replace(i, i);
        }

        for i in 0..5000 {
            let r = hd.find(&i).unwrap();
            assert_eq!(*r.1, i);
        }
    }

    #[test]
    fn simple_iterator_test() {
        let mut hd: Dict<usize, usize> = Dict::new(int_hash_func, 0);
        for i in 0..4 {
            hd.add(i, i).unwrap();
        }

        let mut cnt = 0;

        for en in hd.iter() {
            assert_eq!(*en.0, *en.1);
            cnt += 1;
        }

        assert_eq!(cnt, 4);
    }

    #[test]
    fn iterator_test_when_rehashing() {
        let mut hd: Dict<usize, usize> = Dict::new(int_hash_func, 0);
        let mut cnt = 0;

        for i in 0..100 {
            hd.add(i, i).unwrap();
            cnt += 1;
            if hd.is_rehashing() {
                break;
            }
        }

        for en in hd.iter() {
            assert_eq!(*en.0, *en.1);
            cnt -= 1;
        }

        assert_eq!(cnt, 0);
    }

    fn delete_items(k: usize) {
        let mut hd: Dict<usize, usize> = Dict::new(int_hash_func, 0);

        for i in 0..100 {
            hd.add(i, i).unwrap();
        }
        assert_eq!(hd.fetch_value(&k).unwrap(), &k);
        let node = hd.delete(&k).unwrap();
        assert_eq!(node.0, k);
        assert!(hd.fetch_value(&k).is_none());

        for i in 0..100 {
            if i == k { continue; }
            assert_eq!(hd.fetch_value(&i).unwrap(), &i);
        }
    }

    #[test]
    fn bug_detect_1() {
        let mut hd: Dict<usize, usize> = Dict::new(int_hash_func, 0);
        hd.add(1, 1).unwrap();
        hd.delete(&1).unwrap();
        hd.find_by_mut(&1);
        hd.add(1, 1).unwrap();
        hd.add(2, 2).unwrap();
        hd.add(3, 3).unwrap();
        hd.find_by_mut(&1);
        hd.find_by_mut(&2);
        hd.find_by_mut(&3);
    }

    #[test]
    fn delete_items_test() {
        for i in 0..100 {
            delete_items(i);
        }
    }

    #[test]
    fn next_power_test() {
        assert_eq!(next_power(3), 4);
        assert_eq!(next_power(5), 8);
        assert_eq!(next_power(513), 1024);
        assert_eq!(next_power(std::usize::MAX), std::usize::MAX);
    }
}

