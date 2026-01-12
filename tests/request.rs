use rkyv::rancor;
use smol_str::ToSmolStr;
use tempfile::tempdir;
use uuid::Uuid;

use brunhilde::{request, AppendRow, Database, RowWithoutEventId};

#[tokio::test]
async fn test_create_table() {
    let db_dir = tempdir().unwrap();
    let mut database = Database::new(db_dir.path().to_owned()).await;
    assert!(database.table_locks.is_empty());
    let table_id = Uuid::new_v4();
    database.create_table(table_id).await;
    let payload: Vec<String> = vec!["foo".to_owned(), "bar".to_owned()];
    request(
        AppendRow::new(
            table_id,
            RowWithoutEventId::new(
                Uuid::new_v4(),
                "INSERT_FOO".to_smolstr(),
                rkyv::to_bytes::<rancor::Error>(&payload)
                    .unwrap()
                    .into_vec(),
            ),
        )
        .into(),
        &database,
    )
    .await;
}
