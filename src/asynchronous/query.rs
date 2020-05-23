use crate::env::REREIDS_IO_BUF_LEN;
use crate::util::{bytes_to_i64, bytes_to_usize};
use tokio::net::tcp::ReadHalf;
use tokio::prelude::*;

#[derive(Debug, Clone)]
pub enum QueryError {
    NotEnough,
    MalFormed,
    EOF,
    IO(tokio::io::ErrorKind),
    Protocol(usize, String),
}

pub struct QueryBuilder {
    buf: Vec<u8>,
    is_buf_useful: bool,
    bulk_size: usize,
    built_args: Vec<Vec<u8>>,
}

impl QueryBuilder {
    pub fn new() -> QueryBuilder {
        QueryBuilder {
            buf: vec![],
            is_buf_useful: false,
            bulk_size: 0,
            built_args: vec![],
        }
    }

    pub fn reset(&mut self) {
        self.buf.clear();
        self.is_buf_useful = false;
        self.bulk_size = 0;
        self.built_args.clear();
    }

    pub async fn build_query(
        &mut self,
        reader: &mut ReadHalf<'_>,
    ) -> Result<Vec<Vec<u8>>, QueryError> {
        if self.bulk_size == 0 && self.buf.len() > 0 && self.buf[0] != b'*' {
            return self.build_inline_query(reader).await;
        }
        self.build_bulk_query(reader).await
    }

    async fn build_inline_query(
        &mut self,
        reader: &mut ReadHalf<'_>,
    ) -> Result<Vec<Vec<u8>>, QueryError> {
        unimplemented!()
    }

    async fn build_bulk_query(
        &mut self,
        reader: &mut ReadHalf<'_>,
    ) -> Result<Vec<Vec<u8>>, QueryError> {
        // test if there is complete args in buf
        match self.build_bulk_query_from_buf() {
            Ok(()) => return Ok(self.export_args()),
            Err(QueryError::NotEnough) => (),
            Err(e) => return Err(e),
        }

        // read from the stream
        let buf_len = self.buf.len();
        self.buf.resize(buf_len + REREIDS_IO_BUF_LEN, 0u8);

        let n_read = match reader.read(&mut self.buf[buf_len..]).await {
            Ok(n) => n,
            Err(e) => return Err(QueryError::IO(e.kind())),
        };
        if n_read == 0 {
            return Err(QueryError::EOF);
        }
        self.buf.resize(buf_len + n_read, 0u8);
        self.is_buf_useful = true;

        self.build_bulk_query_from_buf()?;

        Ok(self.export_args())
    }

    fn build_bulk_query_from_buf(&mut self) -> Result<(), QueryError> {
        if self.is_buf_useful == false {
            return Err(QueryError::NotEnough);
        }

        self.is_buf_useful = false;
        let mut processed = 0;
        let mut new_line = None;

        if self.bulk_size == 0 {
            assert_eq!(self.built_args.len(), 0);
            new_line = self
                .buf
                .iter()
                .enumerate()
                .find(|(_, ch)| **ch == b'\r')
                .map(|x| x.0);

            processed = new_line.ok_or(QueryError::NotEnough)?;

            if processed > self.buf.len() - 2 {
                return Err(QueryError::NotEnough);
            }

            assert_eq!(self.buf[0], b'*');

            let ll = bytes_to_i64(&self.buf[1..processed])
                .map_err(|_| ())
                .and_then(|x| if x > 1024 * 1024 { Err(()) } else { Ok(x) })
                .map_err(|_| {
                    QueryError::Protocol(
                        1,
                        "-ERR Protocol Error: invalid bulk length\r\n".to_string(),
                    )
                })?;

            processed += 2;

            if ll <= 0 {
                self.buf.drain(0..processed);
                self.is_buf_useful = true;
                return Ok(());
            }

            self.bulk_size = ll as usize;
            assert_eq!(self.built_args.len(), 0);
            self.built_args.reserve(self.bulk_size);
        }

        assert!(self.bulk_size > 0);

        while self.bulk_size > 0 {
            new_line = self
                .buf
                .iter()
                .enumerate()
                .skip(processed)
                .find(|(_, ch)| **ch == b'\r')
                .map(|x| x.0);

            let new_line_pos = new_line.ok_or(QueryError::NotEnough)?;

            if processed > self.buf.len() - 2 {
                break;
            }

            if self.buf[processed] != b'$' {
                return Err(QueryError::Protocol(
                    processed,
                    format!(
                        "-ERR Protocol Error: expected '$', got {}\r\n",
                        self.buf[processed] as char
                    ),
                ));
            }

            let ll = bytes_to_usize(&self.buf[processed + 1..new_line_pos])
                .map_err(|_| ())
                .and_then(|x| {
                    if x > 512 * 1024 * 1024 {
                        Err(())
                    } else {
                        Ok(x)
                    }
                })
                .map_err(|_| {
                    QueryError::Protocol(
                        processed,
                        "-ERR Protocol Error: invalid bulk length\r\n".to_string(),
                    )
                })?;

            let begin_pos = new_line_pos + 2;

            if self.buf.len() - begin_pos < ll + 2 {
                break;
            } else {
                let arg = self.buf[begin_pos..begin_pos + ll].to_vec();
                processed = begin_pos + ll + 2;
                self.bulk_size -= 1;
                self.built_args.push(arg);
            }
        }

        if processed > 0 {
            self.buf.drain(0..processed);
        }

        if self.bulk_size == 0 {
            self.is_buf_useful = true;
            return Ok(());
        }

        Err(QueryError::NotEnough)
    }

    fn export_args(&mut self) -> Vec<Vec<u8>> {
        let mut args = vec![];
        std::mem::swap(&mut self.built_args, &mut args);
        args
    }
}
