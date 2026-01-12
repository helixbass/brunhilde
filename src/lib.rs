use smol_str::SmolStr;
use uuid::Uuid;

pub enum Request {
    AppendRow(AppendRow),
    Rows(Rows),
}

pub struct AppendRow {
    pub table: Uuid,
    pub row: Row,
}

pub struct Row {
    pub uuid: Uuid,
    pub type_: SmolStr,
    pub payload: Vec<u8>,
}

pub struct Rows {
    pub table: Uuid,
}
