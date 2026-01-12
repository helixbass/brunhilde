use smol_str::SmolStr;
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

pub fn request(request: Request, database: &Database) -> Response {
    match request {
        Request::AppendRow(append_row) => {
            let table = database.lock_table_for_writing(append_row.table);
            let event_id = table.get_next_event_id();
            table.append(add_event_id(append_row.row, event_id));
            Response::AppendRow(event_id)
        }
        Request::Rows(rows) => {
            Response::Rows(Rows::new(database.table_for_reading(rows.table).read_all()))
        }
    }
}

pub enum Response {
    AppendRow(EventId),
    Rows(Rows),
}

pub struct Rows {
    pub rows: Vec<Row>,
}

impl Rows {
    pub fn new(rows: Vec<Row>) -> Self {
        Self { rows }
    }
}
