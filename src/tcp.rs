use std::pin::Pin;

use futures::future::FutureExt;
use rkyv::{rancor, Archive, Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use uuid::Uuid;

use crate::{Database, EventId, Row, Sender};

pub async fn request(
    mut tcp_stream: TcpStream,
    database: &Database,
    create_table_sender: Box<
        dyn Sender<(Uuid, Pin<Box<dyn Future<Output = ()> + Send + 'static>>)>,
    >,
) {
    let mut request_len_bytes: [u8; 4] = [0; 4];
    tcp_stream.read_exact(&mut request_len_bytes).await.unwrap();
    let request_len = usize::try_from(u32::from_be_bytes(request_len_bytes)).unwrap();
    let mut request_bytes = vec![0; request_len];
    tcp_stream.read_exact(&mut request_bytes).await.unwrap();
    let request = rkyv::from_bytes::<Request, rancor::Error>(&request_bytes).unwrap();
    match request {
        Request::CreateTable(table) => {
            create_table_sender
                .send((
                    table,
                    async move {
                        write_response(Response::CreateTable, tcp_stream).await;
                    }
                    .boxed(),
                ))
                .await;
        }
        Request::Request(request) => {
            let response = crate::request(request, database).await;
            let response: Response = response.into();
            write_response(response, tcp_stream).await;
        }
    }
}

#[derive(Archive, Serialize, Deserialize)]
pub enum Request {
    CreateTable(Uuid),
    Request(crate::Request),
}

#[derive(Archive, Serialize, Deserialize)]
pub enum Response {
    AppendRow(EventId),
    Rows(Rows),
    CreateTable,
}

impl<'a> From<crate::Response<'a>> for Response {
    fn from(value: crate::Response<'a>) -> Self {
        match value {
            crate::Response::AppendRow(event_id) => Self::AppendRow(event_id),
            crate::Response::Rows(rows) => Self::Rows(Rows {
                rows: (*rows.borrow_rows()).to_owned(),
            }),
        }
    }
}

#[derive(Archive, Serialize, Deserialize)]
pub struct Rows {
    pub rows: Vec<Row>,
}

async fn write_response(response: Response, mut tcp_stream: TcpStream) {
    let response = rkyv::to_bytes::<rancor::Error>(&response).unwrap();
    let response_len = u32::try_from(response.len()).unwrap();
    let response_len_bytes = response_len.to_be_bytes();
    tcp_stream.write_all(&response_len_bytes).await.unwrap();
    tcp_stream.write_all(&response).await.unwrap();
}
