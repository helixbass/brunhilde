use std::time::Duration;

use rkyv::{rancor, string::ArchivedString, vec::ArchivedVec};
use smol_str::ToSmolStr;
use tokio::{
    net::TcpStream,
    time::{sleep, Instant},
};
use uuid::Uuid;

use brunhilde::{client, tcp, AppendRow, RowWithoutEventId, RowsRequest};

#[tokio::main]
async fn main() {
    let client_tcp_stream = connect().await;

    let table_id = Uuid::new_v4();

    let response = client::request(tcp::Request::CreateTable(table_id), client_tcp_stream).await;
    assert_eq!(response, tcp::Response::CreateTable);

    let row_id = Uuid::new_v4();
    let payload: Vec<String> = vec!["foo".to_owned(), "bar".to_owned()];

    let client_tcp_stream = connect().await;

    let response = client::request(
        tcp::Request::Request(
            AppendRow::new(
                table_id,
                RowWithoutEventId::new(
                    Some(row_id),
                    "INSERT_FOO".to_smolstr(),
                    rkyv::to_bytes::<rancor::Error>(&payload)
                        .unwrap()
                        .into_vec(),
                ),
            )
            .into(),
        ),
        client_tcp_stream,
    )
    .await;
    let event_id = response.as_append_row();

    let client_tcp_stream = connect().await;

    let response = client::request(
        tcp::Request::Request(RowsRequest::new(table_id).into()),
        client_tcp_stream,
    )
    .await;

    let response = response.as_rows();
    let rows = &response.rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, Some(row_id));
    assert_eq!(rows[0].type_, "INSERT_FOO".to_smolstr());
    assert_eq!(
        rkyv::access::<ArchivedVec<ArchivedString>, rancor::Error>(&rows[0].payload).unwrap(),
        &payload
    );
    assert_eq!(rows[0].event_id, event_id);

    println!("finished reading");
}

async fn connect() -> TcpStream {
    let timeout = Instant::now() + Duration::from_millis(5000);
    loop {
        if Instant::now() > timeout {
            panic!("failed to connect");
        }
        match TcpStream::connect("127.0.0.1:8422").await {
            Ok(tcp_stream) => {
                return tcp_stream;
            }
            Err(_) => {
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
}
