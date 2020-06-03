use crate::env::REREIDS_IO_BUF_LEN;
use crate::util::{bytes_to_i64, bytes_to_usize};
use std::time::Duration;
use tokio::net::tcp::ReadHalf;
use tokio::prelude::*;
use tokio::time::timeout;

#[derive(Debug, Clone)]
pub enum QueryError {
    NotEnough,
    MalFormed,
    EOF,
    ReadTimeOut,
    IO(tokio::io::ErrorKind),
    Protocol(usize, String),
}

pub struct QueryBuilder {
    buf: Vec<u8>,
    is_buf_useful: bool,
    bulk_size: usize,
    built_args: Vec<Vec<u8>>,
    read_timeout: Option<usize>,
}

impl QueryBuilder {
    pub fn new(read_timeout: Option<usize>) -> QueryBuilder {
        QueryBuilder {
            buf: vec![],
            is_buf_useful: false,
            bulk_size: 0,
            built_args: vec![],
            read_timeout,
        }
    }

    pub fn reset(&mut self) {
        self.buf.clear();
        self.is_buf_useful = false;
        self.bulk_size = 0;
        self.built_args.clear();
    }

    async fn build_inline_query(&mut self) -> Result<Vec<Vec<u8>>, QueryError> {
        let new_line = self
            .buf
            .iter()
            .enumerate()
            .find(|(_, ch)| **ch == b'\r')
            .map(|x| x.0);

        let n = match new_line {
            None => return Err(QueryError::NotEnough),
            Some(n) => n,
        };

        if n + 1 >= self.buf.len() {
            return Err(QueryError::NotEnough);
        }

        let command = match std::str::from_utf8(&self.buf[..n]) {
            Ok(s) => s,
            Err(_) => return Err(QueryError::MalFormed),
        };

        let args = command
            .split_ascii_whitespace()
            .map(|x| x.as_bytes().to_vec())
            .collect();

        self.buf.drain(..n + 2);

        Ok(args)
    }

    pub async fn build_query(
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

        let n_read =
            match Self::read_from_reader(reader, &mut self.buf[buf_len..], self.read_timeout).await
            {
                Ok(n) => n,
                Err(e) => return Err(e),
            };
        if n_read == 0 {
            return Err(QueryError::EOF);
        }
        self.buf.resize(buf_len + n_read, 0u8);
        self.is_buf_useful = true;
        if self.bulk_size == 0 && self.buf[0] != b'*' {
            return self.build_inline_query().await;
        }

        self.build_bulk_query_from_buf()?;

        Ok(self.export_args())
    }

    fn build_bulk_query_from_buf(&mut self) -> Result<(), QueryError> {
        if self.is_buf_useful == false {
            return Err(QueryError::NotEnough);
        }

        self.is_buf_useful = false;
        let mut processed = 0;
        let mut new_line;

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

            let new_line_pos = match new_line {
                None => break,
                Some(n) => n,
            };

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
                if self.buf[begin_pos + ll] != b'\r' || self.buf[begin_pos + ll + 1] != b'\n' {
                    return Err(QueryError::Protocol(
                        begin_pos,
                        "-Err Protocol Error: invalid break line, expect \"\\r\\n\"".to_string(),
                    ));
                }
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

    async fn read_from_reader(
        reader: &mut ReadHalf<'_>,
        buf: &mut [u8],
        read_timeout: Option<usize>,
    ) -> Result<usize, QueryError> {
        let fut = reader.read(buf);
        match read_timeout {
            None => fut.await.map_err(|e| QueryError::IO(e.kind())),
            Some(t) => match timeout(Duration::from_secs(t as u64), fut).await {
                Ok(r) => r.map_err(|e| QueryError::IO(e.kind())),
                Err(_) => Err(QueryError::ReadTimeOut),
            },
        }
    }
}
