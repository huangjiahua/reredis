use crate::asynchronous::query;
use crate::asynchronous::query::{QueryBuilder, QueryError};
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
    reply: std::result::Result<(), ()>,
) -> Result<(), ()> {
    writer.write_all("-ERR Not Implemented\r\n".as_bytes()).await.unwrap();
    Ok(())
}
