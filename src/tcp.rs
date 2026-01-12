use tokio::net::TcpStream;
use uuid::Uuid;

use crate::{Database, Sender};

pub async fn request(
    tcp_stream: TcpStream,
    database: &Database,
    create_table_sender: Box<dyn Sender<Uuid>>,
) {
    unimplemented!()
}
