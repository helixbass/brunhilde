use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use clap::Parser;
use futures::future::join_all;
use squalid::_d;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, channel},
    task::JoinHandle,
};
use uuid::Uuid;

use brunhilde::{tcp, Database, Sender};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    let (sender, mut receiver) = channel::<World>(100);

    let listener = TcpListener::bind("0.0.0.0:8421").await.unwrap();

    tokio::spawn({
        let sender = sender.clone();
        async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                sender.send(socket.into()).await.unwrap();
            }
        }
    });

    let mut database = Arc::new(Database::new(args.db_dir).await);

    let mut tcp_requests: Vec<JoinHandle<()>> = _d();

    loop {
        match receiver.recv().await.unwrap() {
            World::TcpConnected(tcp_stream) => {
                let join_handle = tokio::spawn({
                    let sender: Box<
                        dyn Sender<(Uuid, Pin<Box<dyn Future<Output = ()> + Send + 'static>>)>,
                    > = Box::new(CreateTableSender::from(sender.clone()));
                    let database = database.clone();
                    async move {
                        tcp::request(tcp_stream, &database, sender).await;
                    }
                });
                tcp_requests.push(join_handle);
            }
            World::CreateTable((table, callback)) => {
                let _ = join_all(tcp_requests).await;
                Arc::get_mut(&mut database)
                    .unwrap()
                    .create_table(table)
                    .await;
                callback.await;
                tcp_requests = _d();
            }
        }
    }
}

#[derive(Parser)]
struct Args {
    db_dir: PathBuf,
}

enum World {
    TcpConnected(TcpStream),
    CreateTable((Uuid, Pin<Box<dyn Future<Output = ()> + Send + 'static>>)),
}

impl From<TcpStream> for World {
    fn from(value: TcpStream) -> Self {
        Self::TcpConnected(value)
    }
}

#[derive(Clone)]
struct CreateTableSender {
    pub sender: mpsc::Sender<World>,
}

impl From<mpsc::Sender<World>> for CreateTableSender {
    fn from(value: mpsc::Sender<World>) -> Self {
        Self { sender: value }
    }
}

#[async_trait]
impl Sender<(Uuid, Pin<Box<dyn Future<Output = ()> + Send + 'static>>)> for CreateTableSender {
    async fn send(&self, value: (Uuid, Pin<Box<dyn Future<Output = ()> + Send + 'static>>)) {
        self.sender.send(World::CreateTable(value)).await.unwrap();
    }

    fn box_clone(
        &self,
    ) -> Box<dyn Sender<(Uuid, Pin<Box<dyn Future<Output = ()> + Send + 'static>>)>> {
        Box::new(self.clone())
    }
}
