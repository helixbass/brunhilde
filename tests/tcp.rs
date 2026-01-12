use std::process::Command;

use tempfile::tempdir;

#[tokio::test]
async fn test_tcp() {
    let path_to_server_binary = env!("CARGO_BIN_EXE_brunhilde");
    let db_dir = tempdir().unwrap();
    let mut running_server = Command::new(path_to_server_binary)
        .arg(db_dir.path().to_str().unwrap())
        .spawn()
        .unwrap();

    running_server.kill().unwrap();
}
