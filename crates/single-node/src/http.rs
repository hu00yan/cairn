use crate::{SingleNodeError, SingleNodeStore, WriteHandle};
use cairn_catalog::{FileId, Head, OperationId, VersionId};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::HashMap,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{mpsc::Receiver, Arc, Mutex},
    thread,
    time::Duration,
};

const MAX_REQUEST_BODY: usize = 64 * 1024 * 1024;
const MAX_HEADER_BYTES: usize = 32 * 1024;

#[derive(Clone)]
pub struct HttpNode {
    store: SingleNodeStore,
    writes: Arc<Mutex<HashMap<u64, WriteHandle>>>,
}

#[derive(Debug, Deserialize)]
struct NamedRequest {
    name: String,
}

#[derive(Debug, Deserialize)]
struct BeginWriteRequest {
    operation_id: u64,
    expected_generation: u64,
    expected_version: Option<u64>,
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
}

impl HttpResponse {
    fn json(status: u16, body: serde_json::Value) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: serde_json::to_vec(&body).expect("JSON response serialization cannot fail"),
        }
    }

    fn empty(status: u16) -> Self {
        Self {
            status,
            content_type: "text/plain",
            body: Vec::new(),
        }
    }
}

impl HttpNode {
    pub fn new(store: SingleNodeStore) -> Self {
        Self {
            store,
            writes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Runs the single-node HTTP daemon until the stop channel receives a value.
    ///
    /// The listener is deliberately injected so tests and a future Unix-socket
    /// adapter can reuse the same request handling seam.
    pub fn run(self, listener: TcpListener, stop: Receiver<()>) -> io::Result<()> {
        listener.set_nonblocking(true)?;
        let mut workers: Vec<thread::JoinHandle<()>> = Vec::new();
        loop {
            if stop.try_recv().is_ok() {
                for worker in workers {
                    let _ = worker.join();
                }
                return Ok(());
            }
            let mut active_workers = Vec::with_capacity(workers.len());
            for worker in workers.drain(..) {
                if worker.is_finished() {
                    let _ = worker.join();
                } else {
                    active_workers.push(worker);
                }
            }
            workers = active_workers;
            match listener.accept() {
                Ok((stream, _)) => {
                    let node = self.clone();
                    workers.push(thread::spawn(move || {
                        let _ = node.handle_connection(stream);
                    }));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(2));
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn handle_connection(&self, mut stream: TcpStream) -> io::Result<()> {
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        let (method, target, body) = read_request(&mut stream)?;
        let response = self.dispatch(&method, &target, &body);
        write_response(&mut stream, response)
    }

    fn dispatch(&self, method: &str, target: &str, body: &[u8]) -> HttpResponse {
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        let parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();

        if method == "GET" && path == "/healthz" {
            return HttpResponse {
                status: 200,
                content_type: "text/plain",
                body: b"ok\n".to_vec(),
            };
        }

        match (method, parts.as_slice()) {
            ("POST", ["v1", "collections"]) => {
                let request = match json_body::<NamedRequest>(body) {
                    Ok(request) => request,
                    Err(response) => return response,
                };
                match self.store.create_collection(&request.name) {
                    Ok(id) => HttpResponse::json(201, json!({ "id": id })),
                    Err(error) => error_response(error),
                }
            }
            ("POST", ["v1", "collections", collection, "files"]) => {
                let collection = match parse_id(collection) {
                    Ok(id) => id,
                    Err(response) => return response,
                };
                let request = match json_body::<NamedRequest>(body) {
                    Ok(request) => request,
                    Err(response) => return response,
                };
                match self.store.create_file(collection, &request.name) {
                    Ok(file) => HttpResponse::json(
                        201,
                        json!({
                            "id": file.id,
                            "collection": file.collection_id,
                            "name": file.name,
                        }),
                    ),
                    Err(error) => error_response(error),
                }
            }
            ("POST", ["v1", "files", file, "writes"]) => {
                let file = match parse_id(file) {
                    Ok(id) => FileId::from_raw(id),
                    Err(response) => return response,
                };
                let request = match json_body::<BeginWriteRequest>(body) {
                    Ok(request) => request,
                    Err(response) => return response,
                };
                let operation = OperationId::from_raw(request.operation_id);
                let head = Head {
                    version_id: request.expected_version.map(VersionId::from_raw),
                    generation: request.expected_generation,
                };
                match self.store.begin_write(file, head, operation) {
                    Ok(write) => match self.writes.lock() {
                        Ok(mut writes) => {
                            writes.insert(request.operation_id, write);
                            HttpResponse::json(201, json!({ "operation_id": request.operation_id }))
                        }
                        Err(_) => error_response(SingleNodeError::Poisoned),
                    },
                    Err(error) => error_response(error),
                }
            }
            ("PUT", ["v1", "writes", operation, "data"]) => {
                let operation = match parse_id(operation) {
                    Ok(id) => id,
                    Err(response) => return response,
                };
                let offset = match query_value(query, "offset") {
                    Some(value) => match value.parse::<u64>() {
                        Ok(value) => value,
                        Err(_) => return HttpResponse::empty(400),
                    },
                    None => return HttpResponse::empty(400),
                };
                match self.writes.lock() {
                    Ok(mut writes) => match writes.get_mut(&operation) {
                        Some(write) => match write.write_at(offset, body) {
                            Ok(()) => HttpResponse::empty(204),
                            Err(error) => error_response(error),
                        },
                        None => HttpResponse::empty(404),
                    },
                    Err(_) => error_response(SingleNodeError::Poisoned),
                }
            }
            ("POST", ["v1", "writes", operation, "commit"]) => {
                let operation = match parse_id(operation) {
                    Ok(id) => id,
                    Err(response) => return response,
                };
                match self.writes.lock() {
                    Ok(mut writes) => match writes.get_mut(&operation) {
                        Some(write) => match write.commit() {
                            Ok(result) => HttpResponse::json(
                                200,
                                json!({
                                    "operation_id": operation,
                                    "version": result.version.get(),
                                    "size": result.info.len,
                                    "digest": hex_digest(&result.info.digest),
                                }),
                            ),
                            Err(error) => error_response(error),
                        },
                        None => HttpResponse::empty(404),
                    },
                    Err(_) => error_response(SingleNodeError::Poisoned),
                }
            }
            ("DELETE", ["v1", "writes", operation]) => {
                let operation = match parse_id(operation) {
                    Ok(id) => id,
                    Err(response) => return response,
                };
                match self.writes.lock() {
                    Ok(mut writes) => match writes.remove(&operation) {
                        Some(mut write) => match write.abort() {
                            Ok(()) => HttpResponse::empty(204),
                            Err(error) => error_response(error),
                        },
                        None => HttpResponse::empty(404),
                    },
                    Err(_) => error_response(SingleNodeError::Poisoned),
                }
            }
            ("GET", ["v1", "files", file, "data"]) => {
                let file = match parse_id(file) {
                    Ok(id) => FileId::from_raw(id),
                    Err(response) => return response,
                };
                match self.store.open_snapshot(file, None) {
                    Ok(snapshot) => match snapshot.read_range(0..snapshot.len()) {
                        Ok(body) => HttpResponse {
                            status: 200,
                            content_type: "application/octet-stream",
                            body,
                        },
                        Err(error) => error_response(error),
                    },
                    Err(error) => error_response(error),
                }
            }
            _ => HttpResponse::empty(404),
        }
    }
}

fn parse_id(value: &str) -> Result<u64, HttpResponse> {
    value.parse().map_err(|_| HttpResponse::empty(400))
}

fn query_value<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|part| {
        let (name, value) = part.split_once('=')?;
        (name == key).then_some(value)
    })
}

fn json_body<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, HttpResponse> {
    serde_json::from_slice(body).map_err(|_| HttpResponse::empty(400))
}

fn hex_digest(digest: &[u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn error_response(error: SingleNodeError) -> HttpResponse {
    let status = match error {
        SingleNodeError::NoVersion => 404,
        SingleNodeError::OutOfBounds | SingleNodeError::UnsupportedWrite => 400,
        SingleNodeError::Unauthorized => 403,
        SingleNodeError::Unavailable
        | SingleNodeError::Poisoned
        | SingleNodeError::CatalogUnavailable
        | SingleNodeError::DeviceUnavailable
        | SingleNodeError::DagUnavailable
        | SingleNodeError::Corrupt
        | SingleNodeError::MetadataMismatch
        | SingleNodeError::NotPublished => 409,
    };
    HttpResponse::json(status, json!({ "error": format!("{error:?}") }))
}

fn read_request(stream: &mut TcpStream) -> io::Result<(String, String, Vec<u8>)> {
    let mut buffer = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 4096];
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "request ended early",
            ));
        }
        buffer.extend_from_slice(&chunk[..count]);
        if buffer.len() > MAX_HEADER_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request headers too large",
            ));
        }
        if let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break position;
        }
    };
    let header = String::from_utf8(buffer[..header_end].to_vec())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "request headers are not UTF-8"))?;
    let mut lines = header.lines();
    let request_line = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request line"))?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing method"))?;
    let target = request_parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing target"))?;
    let content_length = lines
        .filter_map(|line| line.split_once(':'))
        .find_map(|(name, value)| {
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    if content_length > MAX_REQUEST_BODY {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "request body too large",
        ));
    }
    let body_start = header_end + 4;
    while buffer.len() < body_start + content_length {
        let mut chunk = [0_u8; 8192];
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "request body ended early",
            ));
        }
        buffer.extend_from_slice(&chunk[..count]);
    }
    Ok((
        method.to_owned(),
        target.to_owned(),
        buffer[body_start..body_start + content_length].to_vec(),
    ))
}

fn write_response(stream: &mut TcpStream, response: HttpResponse) -> io::Result<()> {
    let reason = match response.status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        _ => "Error",
    };
    write!(
        stream,
        "HTTP/1.1 {} {reason}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        response.status,
        response.content_type,
        response.body.len()
    )?;
    stream.write_all(&response.body)
}
