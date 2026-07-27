use cairn_device::io::FileDevice;
use cairn_single_node::{HttpNode, SingleNodeConfig, SingleNodeStore};
use serde_json::json;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::mpsc,
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

fn paths() -> (PathBuf, PathBuf) {
    let suffix = format!(
        "{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    (
        std::env::temp_dir().join(format!("cairn-http-{suffix}.db")),
        std::env::temp_dir().join(format!("cairn-http-{suffix}.dag")),
    )
}

fn request(address: std::net::SocketAddr, method: &str, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
    let mut stream = TcpStream::connect(address).unwrap();
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .unwrap();
    stream.write_all(body).unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap();
    let headers = String::from_utf8_lossy(&response[..header_end]);
    let status = headers.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, response[header_end + 4..].to_vec())
}

#[test]
fn http_daemon_publishes_and_reads_a_single_node_version() {
    let (catalog_path, data_path) = paths();
    let config = SingleNodeConfig::new(&catalog_path, &data_path, 1);
    FileDevice::create_preallocated(&data_path, 4 * 1024 * 1024).unwrap();
    let ids = SingleNodeStore::bootstrap(&config, "docs", "readme").unwrap();
    let store = SingleNodeStore::open(config.clone()).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = mpsc::channel();
    let thread = thread::spawn(move || HttpNode::new(store).run(listener, stop_rx));

    let (status, body) = request(
        address,
        "POST",
        &format!("/v1/files/{}/writes", ids.file.get()),
        json!({
            "operation_id": 41,
            "expected_generation": 0,
            "expected_version": null
        })
        .to_string()
        .as_bytes(),
    );
    assert_eq!(status, 201);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["operation_id"],
        41
    );

    let (status, _) = request(address, "PUT", "/v1/writes/41/data?offset=0", b"hello");
    assert_eq!(status, 204);
    let (status, body) = request(address, "POST", "/v1/writes/41/commit", b"");
    assert_eq!(status, 200);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["version"],
        1
    );

    let (status, body) = request(
        address,
        "GET",
        &format!("/v1/files/{}/data", ids.file.get()),
        b"",
    );
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");

    let (status, _) = request(
        address,
        "POST",
        &format!("/v1/files/{}/writes", ids.file.get()),
        json!({
            "operation_id": 42,
            "expected_generation": 1,
            "expected_version": 1
        })
        .to_string()
        .as_bytes(),
    );
    assert_eq!(status, 201);
    let (status, _) = request(address, "PUT", "/v1/writes/42/data?offset=0", b"discarded");
    assert_eq!(status, 204);
    let (status, _) = request(address, "DELETE", "/v1/writes/42", b"");
    assert_eq!(status, 204);
    let (status, body) = request(
        address,
        "GET",
        &format!("/v1/files/{}/data", ids.file.get()),
        b"",
    );
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");

    let (status, _) = request(address, "POST", "/v1/writes/99/commit", b"");
    assert_eq!(status, 404);
    stop_tx.send(()).unwrap();
    thread.join().unwrap().unwrap();

    let reopened = SingleNodeStore::open(config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = mpsc::channel();
    let thread = thread::spawn(move || HttpNode::new(reopened).run(listener, stop_rx));
    let (status, body) = request(
        address,
        "GET",
        &format!("/v1/files/{}/data", ids.file.get()),
        b"",
    );
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");
    stop_tx.send(()).unwrap();
    thread.join().unwrap().unwrap();

    let _ = std::fs::remove_file(&catalog_path);
    let _ = std::fs::remove_file(catalog_path.with_extension("db-wal"));
    let _ = std::fs::remove_file(catalog_path.with_extension("db-shm"));
    let _ = std::fs::remove_file(&data_path);
    let _ = std::fs::remove_file(data_path.with_extension("dag.lock"));
}
