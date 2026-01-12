use rkyv::{rancor, string::ArchivedString, vec::ArchivedVec};
use smol_str::ToSmolStr;
use tempfile::tempdir;
use uuid::Uuid;

use brunhilde::{request, AppendRow, Database, RowWithoutEventId, RowsRequest};

#[tokio::test]
async fn test_create_table() {
    let db_dir = tempdir().unwrap();
    let mut database = Database::new(db_dir.path().to_owned()).await;
    assert!(database.table_locks.is_empty());
    let table_id = Uuid::new_v4();
    database.create_table(table_id).await;
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
}
