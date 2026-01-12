use rkyv::rancor;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::tcp;

pub async fn request(request: tcp::Request, mut tcp: TcpStream) -> tcp::Response {
    let request_bytes = rkyv::to_bytes::<rancor::Error>(&request).unwrap();
    let request_len = u32::try_from(request_bytes.len()).unwrap();
    let request_len_bytes = request_len.to_be_bytes();
    tcp.write_all(&request_len_bytes).await.unwrap();
    tcp.write_all(&request_bytes).await.unwrap();

    let mut response_len_bytes: [u8; 4] = [0; 4];
    tcp.read_exact(&mut response_len_bytes).await.unwrap();
    let request_len = usize::try_from(u32::from_be_bytes(response_len_bytes)).unwrap();
    let mut response_bytes = vec![0; request_len];
    tcp.read_exact(&mut response_bytes).await.unwrap();
    rkyv::from_bytes::<tcp::Response, rancor::Error>(&response_bytes).unwrap()
}
