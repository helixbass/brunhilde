use std::process::Command;
use std::time::Duration;

use rkyv::{rancor, string::ArchivedString, vec::ArchivedVec};
use smol_str::ToSmolStr;
use tempfile::tempdir;
use tokio::{net::TcpStream, time::timeout};
use uuid::Uuid;

use brunhilde::{client, tcp, AppendRow, RowWithoutEventId, RowsRequest};

#[tokio::test]
async fn test_tcp() {
    let path_to_server_binary = env!("CARGO_BIN_EXE_brunhilde");
    let db_dir = tempdir().unwrap();
    let mut running_server = Command::new(path_to_server_binary)
        .arg(db_dir.path().to_str().unwrap())
        .spawn()
        .unwrap();

    let client_tcp_stream = timeout(
        Duration::from_millis(5000),
        TcpStream::connect("127.0.0.1:8421"),
    )
    .await
    .unwrap()
    .unwrap();

    let table_id = Uuid::new_v4();

    let response = client::request(tcp::Request::CreateTable(table_id), client_tcp_stream).await;
    assert_eq!(response, tcp::Response::CreateTable);

    let row_uuid = Uuid::new_v4();
    let payload: Vec<String> = vec!["foo".to_owned(), "bar".to_owned()];

    let client_tcp_stream = TcpStream::connect("127.0.0.1:8421").await.unwrap();

    let response = client::request(
        tcp::Request::Request(
            AppendRow::new(
                table_id,
                RowWithoutEventId::new(
                    row_uuid,
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

    let client_tcp_stream = TcpStream::connect("127.0.0.1:8421").await.unwrap();

    let response = client::request(
        tcp::Request::Request(RowsRequest::new(table_id).into()),
        client_tcp_stream,
    )
    .await;

    let response = response.as_rows();
    let rows = &response.rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].uuid, row_uuid);
    assert_eq!(rows[0].type_, "INSERT_FOO".to_smolstr());
    assert_eq!(
        rkyv::access::<ArchivedVec<ArchivedString>, rancor::Error>(&rows[0].payload).unwrap(),
        &payload
    );
    assert_eq!(rows[0].event_id, event_id);

    running_server.kill().unwrap();
}
