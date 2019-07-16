use std::collections::HashMap;
use crate::object::RobjPtr;
use core::borrow::Borrow;

const DICT_HT_INITIAL_SIZE: usize = 4;

fn dict_can_resize() -> bool {
    true
}

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
}

struct HashDict<K: Default + PartialEq, V: Default> {
    ht: [DictTable<K, V>; 2],
    rehash_idx: i32,
    iterators: i32,
    f: fn(&K) -> usize,
}

impl<K, V> HashDict<K, V>
    where K: Default + PartialEq, V: Default
{
    pub fn new(f: fn(&K) -> usize) -> HashDict<K, V> {
        let table1: DictTable<K, V> = DictTable::new();
        let table2: DictTable<K, V> = DictTable::new();
        HashDict {
            ht: [table1, table2],
            rehash_idx: -1,
            iterators: 0,
            f,
        }
    }

    pub fn find(&self, key: K) -> Option<&Box<DictEntry<K, V>>> {
        let h;

        if self.ht[0].size == 0 {
            return None;
        }

        h = self.f.borrow()(&key);

        for table in 0..2 {
            let idx = h & self.ht[table].size_mask;

            let mut he = self.ht[table].table[idx].as_ref();
            while let Some(e) = he {
                if e.key == key {
                    return Some(e);
                }
                he = e.next.as_ref();
            }

            if !self.is_rehashing() {
                return None;
            }
        }
        None
    }

    pub fn find_by_mut(&mut self, key: K) -> Option<&Box<DictEntry<K, V>>> {
        let h;

        if self.ht[0].size == 0 {
            return None;
        }

        if self.is_rehashing() {
            self.rehash_step()
        }

        h = self.f.borrow()(&key);

        for table in 0..2 {
            let idx = h & self.ht[table].size_mask;

            let mut he = self.ht[table].table[idx].as_ref();
            while let Some(e) = he {
                if e.key == key {
                    return Some(e);
                }
                he = e.next.as_ref();
            }

            if !self.is_rehashing() {
                return None;
            }
        }
        None
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

        if self.is_rehashing() {
            self.rehash_step();
        }

        match self.key_index(&key) {
            Err(_) => return None,
            Ok(i) => index = i,
        }

        ht = match self.is_rehashing() {
            true => &mut self.ht[1],
            false => &mut self.ht[0],
        };

        entry = DictEntry {
            key,
            value: Default::default(),
            next: None,
        };

        match ht.table[index] {
            None => ht.table[index] = Some(Box::new(entry)),
            Some(_) => {
                let curr_head = ht.table[index].take().unwrap();
                entry.next = Some(curr_head);
                ht.table[index] = Some(Box::new(entry));
            }
        }
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
            if self.ht[0].used == 0 {
                self.ht.swap(0, 1);
                self.rehash_idx = -1;
                return false;
            }

            assert!((self.rehash_idx as usize) < self.ht[0].size);

            while let None = self.ht[0].table[self.rehash_idx as usize] {
                self.rehash_idx += 1;
            }

            let index = self.rehash_idx as usize;

            while let Some(mut e) = self.ht[0].table[index].take() {
                let next = e.next.take();

                if let Some(de) = next {
                    self.ht[0].table[index] = Some(de);
                }

                let h = self.f.borrow()(&e.key) & self.ht[1].size_mask;

                match self.ht[1].table[h] {
                    None => self.ht[1].table[h] = Some(e),
                    Some(_) => {
                        let curr_head = self.ht[1].table[h].take().unwrap();
                        e.next = Some(curr_head);
                        self.ht[1].table[h] = Some(e);
                    }
                }

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

        h = self.f.borrow()(key);

        for table in 0..2 {
            idx = h & self.ht[table].size_mask;
            let mut he: Option<&Box<DictEntry<K, V>>> = self.ht[table].table[idx].as_ref();

            while let Some(e) = he {
                if e.key == *key {
                    return Err(());
                }
                he = e.next.as_ref();
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


        if self.ht[0].used >= self.ht[0].size && dict_can_resize() {
            return self.expand(self.ht[0].used * 2);
        }


        Ok(())
    }

    fn expand(&mut self, size: usize) -> Result<(), ()> {
        let real_size = next_power(size);

        if self.is_rehashing() || self.ht[0].size >= size {
            return Err(());
        }

        let mut new_table: Vec<Option<Box<DictEntry<K, V>>>>
            = Vec::with_capacity(real_size);

        for _ in 0..real_size {
            new_table.push(None);
        }

        let table = if self.ht[0].size == 0 {
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
}

struct HashDictIterator<'a, K: Default + PartialEq, V: Default> {
    d: &'a HashDict<K, V>,
    table: i32,
    index: i32,
    save: bool,
    entry: &'a Option<Box<DictEntry<K, V>>>,
}

#[cfg(test)]
mod test {
    use super::*;

    fn int_hash_func(i: &usize) -> usize {
        i.clone()
    }

    #[test]
    fn create_a_hash_dict() {
        let hd: HashDict<usize, usize> = HashDict::new(int_hash_func);
        let f = hd.f.borrow();
        assert_eq!(f(&1), 1);
        assert_eq!(hd.iterators, 0);
        assert_eq!(hd.rehash_idx, -1);
    }

    #[test]
    fn add_some_value() {
        let mut hd: HashDict<usize, usize> = HashDict::new(int_hash_func);
        hd.add(3, 1).unwrap();
        let r = hd.add(3, 1);
        if let Ok(_) = r {
            panic!("Wrong")
        }
    }

    #[test]
    fn simple_add_and_find() {
        let mut hd: HashDict<usize, usize> = HashDict::new(int_hash_func);
        hd.add(3, 4);
        hd.add(4, 5);
        hd.add(5, 6);
        hd.add(8, 9);
        let entry = hd.find(3).unwrap();
        assert_eq!(entry.value, 4);
        let entry = hd.find(4).unwrap();
        assert_eq!(entry.value, 5);
        let entry = hd.find(5).unwrap();
        assert_eq!(entry.value, 6);
        let entry = hd.find(8).unwrap();
        assert_eq!(entry.value, 9);

        let entry = hd.find(2);
        if let Some(_) = entry {
            panic!("Wrong");
        }
    }

    #[test]
    fn ht_should_resize() {
        let mut hd: HashDict<usize, usize> = HashDict::new(int_hash_func);
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
            hd.find_by_mut(3);
            assert!(hd.is_rehashing());
        }
        // ht[0] now has 5
        hd.find_by_mut(3);
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
        hd.find_by_mut(3);

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
    fn next_power_test() {
        assert_eq!(next_power(3), 4);
        assert_eq!(next_power(5), 8);
        assert_eq!(next_power(513), 1024);
        assert_eq!(next_power(std::usize::MAX), std::usize::MAX);
    }
}

