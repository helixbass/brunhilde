use rkyv::{rancor, string::ArchivedString, vec::ArchivedVec};
use smol_str::ToSmolStr;
use tempfile::{tempdir, TempDir};
use uuid::Uuid;

use brunhilde::{request, AppendRow, AppendRows, Database, RowWithoutEventId, RowsRequest};

#[tokio::test]
async fn test_create_table() {
    let DbAndTable {
        table: table_id,
        database,
        db_dir: _db_dir,
    } = create_db_and_table().await;
    let row_id = Uuid::new_v4();
    let payload: Vec<String> = vec!["foo".to_owned(), "bar".to_owned()];
    let event_id = request(
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
        &database,
    )
    .await
    .as_append_row();
    let rows = request(RowsRequest::new(table_id).into(), &database).await;
    let rows = rows.as_rows();
    let rows = rows.borrow_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, Some(row_id));
    assert_eq!(rows[0].type_, "INSERT_FOO".to_smolstr());
    assert_eq!(
        rkyv::access::<ArchivedVec<ArchivedString>, rancor::Error>(&rows[0].payload).unwrap(),
        &payload
    );
    assert_eq!(rows[0].event_id, event_id);
}

#[tokio::test]
async fn test_append_rows() {
    let DbAndTable {
        table: table_id,
        database,
        db_dir: _db_dir,
    } = create_db_and_table().await;
    let rows = vec![
        RowWithoutEventId::new(
            None,
            "INSERT_FOO".to_smolstr(),
            rkyv::to_bytes::<rancor::Error>(&vec!["foo".to_owned(), "bar".to_owned()])
                .unwrap()
                .into_vec(),
        ),
        RowWithoutEventId::new(
            None,
            "UPDATE_FOO".to_smolstr(),
            rkyv::to_bytes::<rancor::Error>(&vec!["baz".to_owned(), "quux".to_owned()])
                .unwrap()
                .into_vec(),
        ),
        RowWithoutEventId::new(
            Some(Uuid::new_v4()),
            "SOMETHING_ELSE".to_smolstr(),
            rkyv::to_bytes::<rancor::Error>(&"whee".to_owned())
                .unwrap()
                .into_vec(),
        ),
    ];
    let event_ids = request(AppendRows::new(table_id, rows.clone()).into(), &database).await;
    let event_ids = event_ids.as_append_rows();
    let response_rows = request(RowsRequest::new(table_id).into(), &database).await;
    let response_rows = response_rows.as_rows();
    let response_rows = response_rows.borrow_rows();
    assert_eq!(response_rows.len(), 3);
    assert_eq!(response_rows[0].id, None);
    assert_eq!(response_rows[0].type_, "INSERT_FOO".to_smolstr());
    assert_eq!(
        rkyv::access::<ArchivedVec<ArchivedString>, rancor::Error>(&response_rows[0].payload)
            .unwrap(),
        &vec!["foo".to_owned(), "bar".to_owned()],
    );
    assert_eq!(response_rows[0].event_id, event_ids[0]);
    assert_eq!(response_rows[2].id, Some(rows[2].id.unwrap()));
}

async fn create_db_and_table() -> DbAndTable {
    let db_dir = tempdir().unwrap();
    let mut database = Database::new(db_dir.path().to_owned()).await;
    assert!(database.table_locks.is_empty());
    let table_id = Uuid::new_v4();
    database.create_table(table_id).await;
    DbAndTable {
        db_dir,
        table: table_id,
        database,
    }
}

struct DbAndTable {
    pub db_dir: TempDir,
    pub table: Uuid,
    pub database: Database,
}
