use crate::asynchronous::query;
use crate::asynchronous::query::{QueryBuilder, QueryError};
use crate::asynchronous::server;
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::prelude::*;

pub async fn read_query_from_client(
    builder: &mut QueryBuilder,
    reader: &mut ReadHalf<'_>,
) -> Result<Vec<Vec<u8>>, QueryError> {
    loop {
        let res = builder.build_query(reader).await;

        match res {
            Ok(args) => return Ok(args),
            Err(query::QueryError::NotEnough) => continue,
            Err(e) => {
                builder.reset();
                return Err(e)
            },
        }
    }
}

pub async fn send_reply_to_client(
    writer: &mut WriteHalf<'_>,
    reply: std::result::Result<server::Reply, server::Error>,
) -> Result<(), ()> {
    match reply {
        Ok(r) => {
            for v in r.reply {
                writer.write_all(&v).await.map_err(|_| ())?
            }
        }
        Err(e) => {
            writer.write_all(e.err_msg.as_bytes()).await.map_err(|_| ())?
        }
    }
    Ok(())
}
