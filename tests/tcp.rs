use std::process::Command;

use tempfile::tempdir;
use tokio::net::TcpStream;
use uuid::Uuid;

use brunhilde::{client, tcp};

#[tokio::test]
async fn test_tcp() {
    let path_to_server_binary = env!("CARGO_BIN_EXE_brunhilde");
    let db_dir = tempdir().unwrap();
    let mut running_server = Command::new(path_to_server_binary)
        .arg(db_dir.path().to_str().unwrap())
        .spawn()
        .unwrap();

    let client_tcp_stream = TcpStream::connect("127.0.0.1:8421").await.unwrap();

    let table_id = Uuid::new_v4();

    let response = client::request(tcp::Request::CreateTable(table_id), client_tcp_stream).await;

    let row_uuid = Uuid::new_v4();
    let payload: Vec<String> = vec!["foo".to_owned(), "bar".to_owned()];
    let event_id = request(
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
        &database,
    )
    .await
    .as_append_row();
    let rows = request(RowsRequest::new(table_id).into(), &database).await;
    let rows = rows.as_rows();
    let rows = rows.borrow_rows();
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
