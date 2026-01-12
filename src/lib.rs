use std::collections::HashMap;
use std::path::{Path, PathBuf};

use smol_str::SmolStr;
use squalid::_d;
use tokio::{
    fs::{self, DirEntry},
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};
use uuid::Uuid;

pub enum Request {
    AppendRow(AppendRow),
    Rows(RowsRequest),
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

pub async fn request(request: Request, database: &Database) -> Response<'_> {
    match request {
        Request::AppendRow(append_row) => {
            let table = database.lock_table_for_writing(append_row.table).await;
            let event_id = table.get_next_event_id();
            table.append(add_event_id(append_row.row, event_id)).await;
            Response::AppendRow(event_id)
        }
        Request::Rows(rows) => Response::Rows({
            let table = database.table_for_reading(rows.table).await;
            Rows::new(table.read_all(), table)
        }),
    }
}

pub enum Response<'a> {
    AppendRow(EventId),
    Rows(Rows<'a>),
}

pub struct Rows<'a> {
    pub rows: &'a [Row],
    pub read_guard: RwLockReadGuard<'a, Table>,
}

impl<'a> Rows<'a> {
    pub fn new(rows: &'a [Row], read_guard: RwLockReadGuard<'a, Table>) -> Self {
        Self { rows, read_guard }
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

    pub async fn lock_table_for_writing(&self, table: Uuid) -> RwLockWriteGuard<'_, Table> {
        self.table_locks[&table].write().await
    }

    pub async fn table_for_reading(&self, table: Uuid) -> RwLockReadGuard<'_, Table> {
        self.table_locks[&table].read().await
    }

    pub async fn create_table(&mut self, table: Uuid) {
        if self.table_locks.contains_key(&table) {
            panic!("tried to create existing table");
        }
        self.table_locks
            .insert(table, RwLock::new(Table::brand_new(&self.directory).await));
    }
}

async fn create_table_locks(directory: &Path) -> TableLocks {
    let tables_dir = directory.join("tables");
    if !fs::try_exists(&tables_dir).await.unwrap() {
        fs::create_dir(&tables_dir).await.unwrap();
        return _d();
    }
    let mut ret: TableLocks = _d();
    let mut dir_entries = fs::read_dir(&tables_dir).await.unwrap();
    while let Some(table_file) = dir_entries.next_entry().await.unwrap() {
        let table_uuid = Uuid::try_parse(table_file.file_name().to_str().unwrap()).unwrap();
        ret.insert(
            table_uuid,
            RwLock::new(Table::new_from_disk(table_uuid, &table_file).await),
        );
    }
    ret
}

pub struct Table {
    pub uuid: Uuid,
    pub next_event_id: EventId,
    pub rows: Vec<Row>,
}

impl Table {
    pub async fn new_from_disk(uuid: Uuid, dir_entry: &DirEntry) -> Self {
        Self { uuid }
    }

    pub fn get_next_event_id(&self) -> EventId {
        self.next_event_id
    }

    pub fn read_all(&self) -> &[Row] {
        &self.rows
    }

    pub async fn brand_new(directory: &Path) -> Self {
        unimplemented!()
    }
}
