use crate::server::Server;
use std::io;
use std::io::{Write, BufWriter, BufReader, Read};
use std::fs::{File, OpenOptions};
use crate::db::DB;
use crate::object::{RobjPtr, RobjEncoding, RobjType};
use std::time::SystemTime;
use crate::util::unix_timestamp;
use std::rc::Rc;

const RDB_DB_SELECT_FLAG: u8 = 0xFE;
const RDB_DB_END_FLAG: u8 = 0xFF;
const RDB_KV_EXPIRE_FLAG: u8 = 0xFC;

const RDB_STRING_FLAG: u8 = 0;
const RDB_LIST_FLAG: u8 = 1;
const RDB_SET_FLAG: u8 = 2;
const RDB_ZSET_FLAG: u8 = 3;
const RDB_HASH_FLAG: u8 = 4;
const RDB_ZIPMAP_FLAG: u8 = 9;
const RDB_ZIPLIST_FLAG: u8 = 10;
const RDB_INTSET_FLAG: u8 = 11;
const RDB_ZSET_ZIPLIST_FLAG: u8 = 12;
const RDB_HASH_ZIPLIST_FLAG: u8 = 13;

const RDB_INT_32_FLAG: u8 = 0b1100_0010;
const RDB_INT_16_FLAG: u8 = 0b1100_0001;
const RDB_INT_8_FLAG: u8 = 0b1100_0000;

const RDB_VERSION: &[u8] = b"REDIS0005";
const RDB_SELECT_DB: &[u8] = &[RDB_DB_SELECT_FLAG];
const RDB_END_BUF: &[u8] = &[RDB_DB_END_FLAG];
const RDB_NO_CHECKSUM: &[u8] = &[0, 0, 0, 0, 0, 0, 0, 0];
const RDB_EXPIRE_MS_BUF: &[u8] = &[RDB_KV_EXPIRE_FLAG];

const RDB_INT_32_BUF: &[u8] = &[RDB_INT_32_FLAG];
const RDB_INT_16_BUF: &[u8] = &[RDB_INT_16_FLAG];
const RDB_INT_8_BUF: &[u8] = &[RDB_INT_8_FLAG];

pub fn rdb_save(server: &Server) -> io::Result<()> {
    let file: File = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&server.db_filename)?;

    let mut writer = BufWriter::new(file);

    writer.write_all(RDB_VERSION)?;

    for db in server.db.iter() {
        if db.dict.len() > 0 {
            writer.dump_db(db)?;
        }
    }

    writer.write_all(RDB_END_BUF)?;
    writer.write_all(RDB_NO_CHECKSUM)?;
    writer.flush()?;
    Ok(())
}

trait RdbWriter: io::Write {
    fn dump_db(&mut self, db: &DB) -> io::Result<()> {
        self.write_all(RDB_SELECT_DB)?;
        self.dump_length(db.id)?;
        for (k, v) in db.dict.iter() {
            let exp = db.expires.find(k)
                .map(|p| p.1);
            self.dump_key_value(k, v, exp)?;
        }
        Ok(())
    }

    fn dump_length(&mut self, size: usize) -> io::Result<()> {
        if size < 64 {
            self.write_all(&[size as u8])?;
        } else if size < 16384 {
            let mut bytes: [u8; 2] = (size as u16).to_le_bytes();
            bytes[0] |= 0b0100_0000;
            self.write_all(&bytes)?;
        } else if size < std::u32::MAX as usize {
            let bytes: [u8; 4] = (size as u32).to_le_bytes();
            let encoded: [u8; 5] = [
                0b1000_0000, bytes[0], bytes[1], bytes[2], bytes[3]
            ];
            self.write_all(&encoded)?;
        } else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "cannot be encoded as length",
            ));
        }
        Ok(())
    }

    fn dump_key_value(
        &mut self,
        k: &RobjPtr,
        v: &RobjPtr,
        exp: Option<&SystemTime>,
    ) -> io::Result<()> {
        if let Some(t) = exp {
            self.write_all(RDB_EXPIRE_MS_BUF)?;
            self.dump_timestamp(t)?;
        }

        self.write_all(&[value_type_flag(v)])?;

        self.dump_string(k)?;

        self.dump_object(v)?;

        Ok(())
    }

    fn dump_timestamp(&mut self, t: &SystemTime) -> io::Result<()> {
        let unix_t = unix_timestamp(t);
        self.write_all(&unix_t.to_le_bytes())?;
        Ok(())
    }

    fn dump_object(&mut self, obj: &RobjPtr) -> io::Result<()> {
        use RobjType::*;
        use RobjEncoding::*;
        match (obj.borrow().object_type(), obj.borrow().encoding()) {
            (String, _) => self.dump_string(obj)?,
            (List, LinkedList) => self.dump_list(obj)?,
            (Set, Ht) => self.dump_set(obj)?,
            (Zset, SkipList) => self.dump_zset(obj)?,
            (Hash, Ht) => self.dump_hash(obj)?,
            (Hash, ZipMap) => self.dump_zmap(obj)?,
            (List, ZipList) => self.dump_ziplist(obj)?,
            (Set, IntSet) => self.dump_intset(obj)?,
            (Zset, ZipList) => self.dump_zset_ziplist(obj)?,
            (Hash, ZipList) => self.dump_hash_ziplist(obj)?,
            (_, _) => panic!("no such type-encoding pair"),
        }
        Ok(())
    }

    fn dump_string(&mut self, obj: &RobjPtr) -> io::Result<()> {
        let obj_ref = obj.borrow();
        if let RobjEncoding::Int = obj_ref.encoding() {
            self.dump_integer(obj_ref.integer())?;
            return Ok(());
        }
        self.dump_bytes(obj_ref.string())?;
        Ok(())
    }

    fn dump_bytes(&mut self, s: &[u8]) -> io::Result<()> {
        self.dump_length(s.len())?;
        self.write_all(s)?;
        Ok(())
    }

    fn dump_integer(&mut self, i: i64) -> io::Result<()> {
        if i < std::i32::MIN as i64 || i > std::i32::MAX as i64 {
            self.dump_bytes(i.to_string().as_bytes())?;
        } else if i < std::i16::MIN as i64 || i > std::i16::MAX as i64 {
            self.write_all(RDB_INT_32_BUF)?;
            let bytes: [u8; 4] = (i as i32).to_le_bytes();
            self.write_all(&bytes)?;
        } else if i < std::i8::MIN as i64 || i > std::i8::MAX as i64 {
            self.write_all(RDB_INT_16_BUF)?;
            let bytes: [u8; 2] = (i as i16).to_le_bytes();
            self.write_all(&bytes)?;
        } else {
            self.write_all(RDB_INT_8_BUF)?;
            let bytes: [u8; 1] = (i as i8).to_le_bytes();
            self.write_all(&bytes)?;
        }
        Ok(())
    }

    fn dump_list(&mut self, obj: &RobjPtr) -> io::Result<()> {
        self.dump_linear(obj)
    }

    fn dump_set(&mut self, obj: &RobjPtr) -> io::Result<()> {
        self.dump_linear(obj)
    }

    fn dump_linear(&mut self, obj: &RobjPtr) -> io::Result<()> {
        let obj_ref = obj.borrow();
        self.dump_length(obj_ref.linear_len())?;
        for str_obj in obj_ref.linear_iter() {
            self.dump_string(&str_obj)?;
        }
        Ok(())
    }

    fn dump_zset(&mut self, _obj: &RobjPtr) -> io::Result<()> {
        unimplemented!()
    }

    fn dump_hash(&mut self, _obj: &RobjPtr) -> io::Result<()> {
        unimplemented!()
    }

    fn dump_zmap(&mut self, _obj: &RobjPtr) -> io::Result<()> {
        unimplemented!()
    }

    fn dump_ziplist(&mut self, obj: &RobjPtr) -> io::Result<()> {
        self.dump_bytes(obj.borrow().raw_data())
    }

    fn dump_intset(&mut self, obj: &RobjPtr) -> io::Result<()> {
        self.dump_bytes(obj.borrow().raw_data())
    }

    fn dump_zset_ziplist(&mut self, _obj: &RobjPtr) -> io::Result<()> {
        unimplemented!()
    }

    fn dump_hash_ziplist(&mut self, _obj: &RobjPtr) -> io::Result<()> {
        unimplemented!()
    }
}

impl RdbWriter for BufWriter<File> {}

fn value_type_flag(o: &RobjPtr) -> u8 {
    use RobjEncoding::*;
    use RobjType::*;

    match (o.borrow().object_type(), o.borrow().encoding()) {
        (String, _) => RDB_STRING_FLAG,
        (List, LinkedList) => RDB_LIST_FLAG,
        (Set, Ht) => RDB_SET_FLAG,
        (Zset, SkipList) => RDB_ZSET_FLAG,
        (Hash, Ht) => RDB_HASH_FLAG,
        (Hash, ZipMap) => RDB_ZIPMAP_FLAG,
        (List, ZipList) => RDB_ZIPLIST_FLAG,
        (Set, IntSet) => RDB_INTSET_FLAG,
        (Zset, ZipList) => RDB_ZSET_ZIPLIST_FLAG,
        (Hash, ZipList) => RDB_HASH_ZIPLIST_FLAG,
        (_, _) => panic!("no such type-encoding pair"),
    }
}


pub fn rdb_load(server: &mut Server) -> io::Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .open(&server.db_filename)?;

    let mut buf: [u8; 9] = [0; 9];
    let mut reader = BufReader::new(file);

    reader.read_exact(&mut buf[0..9])?;
    check_magic_number(&buf[0..9])?;
    let first_db_selector = reader.load_u8()?;
    check_db_selector(first_db_selector)?;

    loop {
        let not_end = reader.load_db(server)?;
        if !not_end {
            break;
        }
    }

    Ok(())
}

fn other_io_err(s: &str) -> io::Error {
    io::Error::new(io::ErrorKind::Other, s)
}

fn check_magic_number(buf: &[u8]) -> io::Result<()> {
    if buf != b"REDIS" {
        return Err(other_io_err("Wrong magic number"));
    }
    Ok(())
}

fn check_db_selector(ch: u8) -> io::Result<()> {
    if ch != RDB_DB_SELECT_FLAG {
        return Err(other_io_err("Wrong db selector"));
    }
    Ok(())
}

fn check_db_idx(server: &Server, idx: usize) -> io::Result<()> {
    if idx > server.db.len() {
        return Err(other_io_err("Wrong db selector"));
    }
    if server.db[idx].dict.len() > 0 {
        return Err(other_io_err("duplicate db selector"));
    }
    Ok(())
}

trait RdbReader: io::Read {
    fn load_db(&mut self, server: &mut Server) -> io::Result<bool> {
        let db_idx = self.load_length()?;
        check_db_idx(server, db_idx)?;
        let db = &mut server.db[db_idx];

        loop {
            let stat = self.load_key_value(db)?;
            match stat {
                LoadStatus::Ok => {}
                LoadStatus::EndDB => return Ok(true),
                LoadStatus::EndAll => return Ok(false),
            }
        }
    }

    fn load_length(&mut self) -> io::Result<usize> {
        unimplemented!()
    }

    fn load_u8(&mut self) -> io::Result<u8> {
        let mut buf: [u8; 1] = [0; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn load_key_value(&mut self, db: &mut DB) -> io::Result<LoadStatus> {
        let mut flag = self.load_u8()?;
        let mut expire: Option<SystemTime> = None;

        match flag {
            RDB_DB_END_FLAG => return Ok(LoadStatus::EndAll),
            RDB_DB_SELECT_FLAG => return Ok(LoadStatus::EndDB),
            _ => {}
        }

        if flag == RDB_KV_EXPIRE_FLAG {
            let t = self.load_time()?;
            expire = Some(t);
            flag = self.load_u8()?;
        }

        let key = self.load_string_object()?;
        let value = self.load_object(flag)?;

        if let Some(t) = expire {
            let _ = db.set_expire(Rc::clone(&key), t);
        }

        db.dict.replace(key, value);

        Ok(LoadStatus::Ok)
    }

    fn load_time(&mut self) -> io::Result<SystemTime> {
        unimplemented!()
    }

    fn load_string_object(&mut self) -> io::Result<RobjPtr> {
        unimplemented!()
    }

    fn load_object(&mut self, _flag: u8) -> io::Result<RobjPtr> {
        unimplemented!()
    }
}

impl RdbReader for io::BufReader<File> {}

enum LoadStatus {
    Ok,
    EndDB,
    EndAll,
}