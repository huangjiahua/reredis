use crate::server::Server;
use std::io;
use std::io::{Write, BufWriter};
use std::fs::{File, OpenOptions};
use crate::db::DB;
use crate::object::RobjPtr;
use std::time::SystemTime;

const RDB_VERSION: &[u8] = b"redis0001";
const RDB_SELECT_DB: &[u8] = &[0xF, 0xE];
const RDB_END: &[u8] = &[0xF, 0xF];
const RDB_NO_CHECKSUM: &[u8] = &[0, 0, 0, 0, 0, 0, 0, 0];

pub fn rdb_save(server: &Server) -> io::Result<()> {
    let file: File = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&server.db_filename)?;

    let mut writer = BufWriter::new(file);

    writer.write_all(RDB_VERSION)?;

    for db in server.db.iter() {
        writer.dump_db(db)?;
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

    fn dump_length(&mut self, _size: usize) -> io::Result<()> {
        unimplemented!()
    }

    fn dump_key_value(
        &mut self,
        _k: &RobjPtr,
        _v: &RobjPtr,
        _exp: Option<&SystemTime>,
    ) -> io::Result<()> {
        unimplemented!()
    }

    fn dump_object(&mut self, _obj: &RobjPtr) -> io::Result<()> {
        unimplemented!()
    }
}

impl RdbWriter for BufWriter<File> {}