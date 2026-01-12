use std::collections::HashMap;
use std::path::{Path, PathBuf};

use smol_str::SmolStr;
use tokio::sync::RwLock;
use uuid::Uuid;

pub enum Request {
    AppendRow(AppendRow),
    Rows(RowsRequest),
    CreateTable(CreateTable),
}

pub struct AppendRow {
    pub table: Uuid,
    pub row: RowWithoutEventId,
}

pub struct RowWithoutEventId {
    pub uuid: Uuid,
    pub type_: SmolStr,
    pub payload: Vec<u8>,
}

pub struct Row {
    pub uuid: Uuid,
    pub type_: SmolStr,
    pub payload: Vec<u8>,
    pub event_id: EventId,
}

pub type EventId = u32;

pub struct RowsRequest {
    pub table: Uuid,
}

pub struct CreateTable {
    pub table: Uuid,
}

pub async fn request(request: Request, database: &Database) -> Response {
    match request {
        Request::AppendRow(append_row) => {
            let table = database.lock_table_for_writing(append_row.table).await;
            let event_id = table.get_next_event_id().await;
            table.append(add_event_id(append_row.row, event_id)).await;
            Response::AppendRow(event_id)
        }
        Request::Rows(rows) => Response::Rows(Rows::new(
            database
                .table_for_reading(rows.table)
                .await
                .read_all()
                .await,
        )),
        Request::CreateTable(create_table) => {
            database.create_table(create_table.table).await;
            Response::CreateTable
        }
    }
}

pub enum Response {
    AppendRow(EventId),
    Rows(Rows),
    CreateTable,
}

pub struct Rows {
    pub rows: Vec<Row>,
}

impl Rows {
    pub fn new(rows: Vec<Row>) -> Self {
        Self { rows }
    }
}

pub type TableLocks = HashMap<Uuid, RwLock<Table>>;

pub struct Database {
    pub table_locks: TableLocks,
    pub directory: PathBuf,
}

impl Database {
    pub async fn new(directory: PathBuf) -> Self {
        assert!(directory.is_dir());
        Self {
            table_locks: create_table_locks(&directory).await,
            directory,
        }
    }

    pub async fn lock_table_for_writing(&self, table: Uuid) -> TableWriteLock<'_> {
        unimplemented!()
    }

    pub async fn table_for_reading(&self, table: Uuid) -> TableRead<'_> {
        unimplemented!()
    }
}

async fn create_table_locks(directory: &Path) -> TableLocks {
    let tables_dir = directory.join("tables");
}
