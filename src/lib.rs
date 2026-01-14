use std::collections::HashMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use ouroboros::self_referencing;
use rkyv::{rancor, Archive, Deserialize, Serialize};
use smol_str::SmolStr;
use squalid::_d;
use tokio::{
    fs::{self, DirEntry},
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};
use uuid::Uuid;

pub mod client;
pub mod tcp;

#[derive(Archive, Serialize, Deserialize)]
pub enum Request {
    AppendRow(AppendRow),
    AppendRows(AppendRows),
    Rows(RowsRequest),
}

impl From<AppendRow> for Request {
    fn from(value: AppendRow) -> Self {
        Self::AppendRow(value)
    }
}

impl From<AppendRows> for Request {
    fn from(value: AppendRows) -> Self {
        Self::AppendRows(value)
    }
}

impl From<RowsRequest> for Request {
    fn from(value: RowsRequest) -> Self {
        Self::Rows(value)
    }
}

#[derive(Archive, Serialize, Deserialize)]
pub struct AppendRow {
    pub table: Uuid,
    pub row: RowWithoutEventId,
}

impl AppendRow {
    pub fn new(table: Uuid, row: RowWithoutEventId) -> Self {
        Self { table, row }
    }
}

#[derive(Archive, Serialize, Deserialize)]
pub struct AppendRows {
    pub table: Uuid,
    pub rows: Vec<RowWithoutEventId>,
}

impl AppendRows {
    pub fn new(table: Uuid, rows: Vec<RowWithoutEventId>) -> Self {
        Self { table, rows }
    }
}

#[derive(Clone, Archive, Serialize, Deserialize)]
pub struct RowWithoutEventId {
    pub id: Option<Uuid>,
    pub type_: SmolStr,
    pub payload: Vec<u8>,
}

impl RowWithoutEventId {
    pub fn new(id: Option<Uuid>, type_: SmolStr, payload: Vec<u8>) -> Self {
        Self { id, type_, payload }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct Row {
    pub id: Option<Uuid>,
    pub type_: SmolStr,
    pub payload: Vec<u8>,
    pub event_id: EventId,
}

pub fn add_event_id(row: RowWithoutEventId, event_id: EventId) -> Row {
    Row {
        id: row.id,
        type_: row.type_,
        payload: row.payload,
        event_id,
    }
}

pub type EventId = u32;

#[derive(Archive, Serialize, Deserialize)]
pub struct RowsRequest {
    pub table: Uuid,
}

impl RowsRequest {
    pub fn new(table: Uuid) -> Self {
        Self { table }
    }
}

pub struct CreateTable {
    pub table: Uuid,
}

pub async fn request(request: Request, database: &Database) -> Response<'_> {
    match request {
        Request::AppendRow(append_row) => {
            let mut table = database.lock_table_for_writing(append_row.table).await;
            let event_id = table.append(append_row.row, &database.directory).await;
            Response::AppendRow(event_id)
        }
        Request::AppendRows(append_rows) => {
            let mut table = database.lock_table_for_writing(append_rows.table).await;
            let event_ids = table
                .append_multiple(append_rows.rows, &database.directory)
                .await;
            Response::AppendRows(event_ids)
        }
        Request::Rows(rows) => Response::Rows(
            RowsBuilder {
                read_guard: database.table_for_reading(rows.table).await,
                rows_builder: |read_guard: &RwLockReadGuard<'_, Table>| read_guard.read_all(),
            }
            .build(),
        ),
    }
}

pub enum Response<'a> {
    AppendRow(EventId),
    AppendRows(Vec<EventId>),
    Rows(Rows<'a>),
}

impl<'a> Response<'a> {
    pub fn as_append_row(&self) -> EventId {
        match self {
            Self::AppendRow(event_id) => *event_id,
            _ => panic!("expected append row"),
        }
    }

    pub fn as_append_rows(&self) -> &[EventId] {
        match self {
            Self::AppendRows(event_ids) => event_ids,
            _ => panic!("expected append rows"),
        }
    }

    pub fn as_rows(&self) -> &Rows<'a> {
        match self {
            Self::Rows(rows) => rows,
            _ => panic!("expected rows"),
        }
    }
}

#[self_referencing]
pub struct Rows<'a> {
    pub read_guard: RwLockReadGuard<'a, Table>,
    #[borrows(read_guard)]
    pub rows: &'this [Row],
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
        self.table_locks.insert(
            table,
            RwLock::new(Table::brand_new(table, &self.directory).await),
        );
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
        let table_id = Uuid::try_parse(table_file.file_name().to_str().unwrap()).unwrap();
        ret.insert(
            table_id,
            RwLock::new(Table::new_from_disk(table_id, &table_file).await),
        );
    }
    ret
}

pub struct Table {
    pub id: Uuid,
    pub next_event_id: EventId,
    pub rows: Vec<Row>,
}

impl Table {
    pub async fn new_from_disk(id: Uuid, dir_entry: &DirEntry) -> Self {
        let rows = read_table_contents(dir_entry).await;
        Self {
            id,
            next_event_id: match rows.is_empty() {
                true => 1,
                false => rows[rows.len() - 1].event_id + 1,
            },
            rows,
        }
    }

    pub fn get_next_event_id(&self) -> EventId {
        self.next_event_id
    }

    pub fn read_all(&self) -> &[Row] {
        &self.rows
    }

    pub async fn brand_new(id: Uuid, directory: &Path) -> Self {
        let rows: Vec<Row> = _d();
        write_table_contents(&rows, id, directory).await;
        Self {
            id,
            next_event_id: 1,
            rows,
        }
    }

    pub async fn append(&mut self, row: RowWithoutEventId, directory: &Path) -> EventId {
        let event_id = self.next_event_id;
        let row = add_event_id(row, event_id);
        self.rows.push(row);
        write_table_contents(&self.rows, self.id, directory).await;
        self.next_event_id += 1;
        event_id
    }

    pub async fn append_multiple(
        &mut self,
        rows: Vec<RowWithoutEventId>,
        directory: &Path,
    ) -> Vec<EventId> {
        let mut ret: Vec<EventId> = _d();
        for row in rows {
            let event_id = self.next_event_id;
            let row = add_event_id(row, event_id);
            self.rows.push(row);
            self.next_event_id += 1;
            ret.push(event_id);
        }
        write_table_contents(&self.rows, self.id, directory).await;
        ret
    }
}

async fn write_table_contents(rows: &Vec<Row>, table: Uuid, directory: &Path) {
    fs::write(
        &directory.join(table.to_string()),
        rkyv::to_bytes::<rancor::Error>(rows).unwrap(),
    )
    .await
    .unwrap()
}

async fn read_table_contents(dir_entry: &DirEntry) -> Vec<Row> {
    rkyv::from_bytes::<_, rancor::Error>(&fs::read(&dir_entry.path()).await.unwrap()).unwrap()
}

#[async_trait]
pub trait Sender<TValue>: Send + Sync {
    async fn send(&self, value: TValue);
    fn box_clone(&self) -> Box<dyn Sender<TValue>>;
}
