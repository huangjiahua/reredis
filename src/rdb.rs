use crate::server::Server;
use std::io;
use std::io::{Write, BufWriter};
use std::fs::{File, OpenOptions};
use crate::db::DB;
use crate::object::{RobjPtr, RobjEncoding, RobjType};
use std::time::SystemTime;
use crate::util::unix_timestamp;

const RDB_VERSION: &[u8] = b"REDIS0005";
const RDB_SELECT_DB: &[u8] = &[0xF, 0xE];
const RDB_END: &[u8] = &[0xF, 0xF];
const RDB_NO_CHECKSUM: &[u8] = &[0, 0, 0, 0, 0, 0, 0, 0];
const RDB_EXPIRE_SEC: &[u8] = &[0xF, 0xD];

const RDB_INT_32: &[u8] = &[0b1100_0010];
const RDB_INT_16: &[u8] = &[0b1100_0001];
const RDB_INT_8: &[u8] = &[0b1100_0000];

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

    writer.write_all(RDB_END)?;
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
            let mut bytes: [u8; 2] = (size as u16).to_be_bytes();
            bytes[0] |= 0b0100_0000;
            self.write_all(&bytes)?;
        } else if size < std::u32::MAX as usize {
            let bytes: [u8; 4] = (size as u32).to_be_bytes();
            let encoded: [u8; 5] = [
                0b1000_0000, bytes[0], bytes[1], bytes[2], bytes[3]
            ];
            self.write_all(&encoded)?;
        } else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "cannot be encoding as length",
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
            self.write_all(RDB_EXPIRE_SEC)?;
            self.write_all(&unix_timestamp(t).to_be_bytes())?;
        }

        self.write_all(&[value_type_flag(v)])?;

        self.dump_string(k)?;

        self.dump_object(v)?;

        Ok(())
    }

    fn dump_object(&mut self, obj: &RobjPtr) -> io::Result<()> {
        use RobjType::*;
        use RobjEncoding::*;
        match (obj.borrow().object_type(), obj.borrow().encoding()) {
            (String, _) => self.dump_string(obj)?,
            (List, LinkedList) => self.dump_list(obj)?,
            (Set, Ht) => self.dump_hash(obj)?,
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
            self.write_all(RDB_INT_32)?;
            let bytes: [u8; 4] = (i as i32).to_be_bytes();
            self.write_all(&bytes)?;
        } else if i < std::i8::MIN as i64 || i > std::i8::MAX as i64 {
            self.write_all(RDB_INT_16)?;
            let bytes: [u8; 2] = (i as i16).to_be_bytes();
            self.write_all(&bytes)?;
        } else {
            self.write_all(RDB_INT_8)?;
            let bytes: [u8; 1] = (i as i8).to_be_bytes();
            self.write_all(&bytes)?;
        }
        Ok(())
    }

    fn dump_list(&mut self, _obj: &RobjPtr) -> io::Result<()> {
        unimplemented!()
    }

    fn dump_set(&mut self, _obj: &RobjPtr) -> io::Result<()> {
        unimplemented!()
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

    fn dump_ziplist(&mut self, _obj: &RobjPtr) -> io::Result<()> {
        unimplemented!()
    }

    fn dump_intset(&mut self, _obj: &RobjPtr) -> io::Result<()> {
        unimplemented!()
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
        (String, _) => 0,
        (List, LinkedList) => 1,
        (Set, Ht) => 2,
        (Zset, SkipList) => 3,
        (Hash, Ht) => 4,
        (Hash, ZipMap) => 9,
        (List, ZipList) => 10,
        (Set, IntSet) => 11,
        (Zset, ZipList) => 12,
        (Hash, ZipList) => 13,
        (_, _) => panic!("no such type-encoding pair"),
    }
}