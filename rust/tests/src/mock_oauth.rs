//! Mock Unity Catalog OAuth token endpoint for integration tests.
//!
//! Stands up a loopback HTTP/1.1 server answering `POST /oidc/v1/token` — the
//! endpoint the SDK's `DefaultTokenFactory` mints against. Tests script the
//! replies (token value, `expires_in`, HTTP status, or an indefinite hang) to
//! drive the OAuth token-cache behavior end to end through the SDK's real
//! `reqwest` client, rather than a stubbed `HeadersProvider`.
//!
//! It mirrors the loopback pattern of `mock_grpc.rs`: bind `127.0.0.1:0`, spawn
//! the server on a background task, and hand the caller back the base URL to
//! point the SDK at. The gRPC mock speaks HTTP/2 via tonic; this endpoint is a
//! plain HTTP/1.1 JSON POST, so it is hand-rolled on a raw `TcpListener` and
//! needs no extra dependency.
//!
//! Shared by `oauth_token_tests` and `federated_token_tests`; each uses only
//! part of it.
#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tracing::{debug, error};

/// A scripted reply the mock returns for one token request.
#[derive(Clone)]
pub enum MockTokenResponse {
    /// `200 OK` with a JSON body carrying `access_token` and, optionally,
    /// `expires_in`.
    Ok {
        access_token: String,
        /// The `expires_in` value as a raw JSON fragment inserted verbatim, so a
        /// test can send an integer (`3600`) or a quoted integer (`"3600"`);
        /// `None` omits the field entirely (the SDK then treats the token as
        /// uncacheable).
        expires_in: Option<String>,
        /// Wall-clock delay before replying, to model a slow issuer. Used to make
        /// a token dead-on-arrival: a delay longer than `expires_in` means the
        /// (start-anchored) token has already expired by the time it arrives.
        delay: Duration,
    },
    /// Reply with the given HTTP status code and body. 5xx classifies as a
    /// retryable `TokenFetchError`, 4xx as a non-retryable `InvalidUCTokenError`.
    Error { status: u16, body: String },
    /// Accept the connection but never reply, so the caller's own timeout fires.
    Hang,
}

impl MockTokenResponse {
    /// `200 OK` returning `access_token` with a one-hour integer `expires_in`.
    pub fn ok(access_token: impl Into<String>) -> Self {
        MockTokenResponse::Ok {
            access_token: access_token.into(),
            expires_in: Some("3600".to_string()),
            delay: Duration::ZERO,
        }
    }

    /// Overrides the `expires_in` fragment (see [`MockTokenResponse::Ok`]).
    pub fn with_expires_in(mut self, raw: Option<&str>) -> Self {
        if let MockTokenResponse::Ok { expires_in, .. } = &mut self {
            *expires_in = raw.map(str::to_string);
        }
        self
    }

    /// Delays a `200 OK` reply by `delay` (see [`MockTokenResponse::Ok`]).
    pub fn with_delay(mut self, delay: Duration) -> Self {
        if let MockTokenResponse::Ok { delay: d, .. } = &mut self {
            *d = delay;
        }
        self
    }

    /// An error reply with the given HTTP status and body.
    pub fn error(status: u16, body: impl Into<String>) -> Self {
        MockTokenResponse::Error {
            status,
            body: body.into(),
        }
    }
}

/// One token request as the mock received it, for asserting the wire contract.
#[derive(Clone, Debug)]
pub struct RecordedRequest {
    pub method: String,
    pub path: String,
    /// Header names lower-cased; a repeated header keeps its last value.
    pub headers: HashMap<String, String>,
    /// The `application/x-www-form-urlencoded` body, decoded. A repeated key
    /// keeps its last value.
    pub form: HashMap<String, String>,
}

impl RecordedRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }

    pub fn form_value(&self, key: &str) -> Option<&str> {
        self.form.get(key).map(String::as_str)
    }
}

/// A mock Unity Catalog OAuth token endpoint bound to loopback.
///
/// The returned handle shares state with the background server task: tests set
/// the scripted replies and read [`mint_count`](Self::mint_count) to assert how
/// many times the SDK actually hit the endpoint (i.e. minted rather than served
/// from cache).
pub struct MockOAuthServer {
    /// Replies consumed in order; once drained, the server task falls back to a
    /// defensive default so an unexpected extra mint fails an assertion cleanly
    /// instead of hanging.
    responses: Arc<Mutex<VecDeque<MockTokenResponse>>>,
    /// Number of token requests fully received (mints observed by the server).
    mint_count: Arc<AtomicUsize>,
    /// Every fully received request, in arrival order.
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
}

impl MockOAuthServer {
    /// Replaces the scripted replies, consumed front to back by later mints.
    pub async fn set_responses(&self, responses: Vec<MockTokenResponse>) {
        let mut queue = self.responses.lock().await;
        queue.clear();
        queue.extend(responses);
    }

    /// Number of token requests the endpoint has received so far.
    pub fn mint_count(&self) -> usize {
        self.mint_count.load(Ordering::SeqCst)
    }

    /// Every token request received so far, in arrival order.
    pub async fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().await.clone()
    }
}

/// Starts the mock endpoint and returns the handle plus its base URL
/// (`http://127.0.0.1:<port>`). Pass the base URL to `unity_catalog_url`; the
/// SDK appends `/oidc/v1/token`.
pub async fn start_mock_oauth_server() -> (MockOAuthServer, String) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock OAuth server");
    let base_url = format!("http://{}", listener.local_addr().expect("local_addr"));

    let responses = Arc::new(Mutex::new(VecDeque::new()));
    let default_response = Arc::new(MockTokenResponse::ok("mock-default-token"));
    let mint_count = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));

    let server = MockOAuthServer {
        responses: Arc::clone(&responses),
        mint_count: Arc::clone(&mint_count),
        requests: Arc::clone(&requests),
    };

    tokio::spawn(async move {
        loop {
            let socket = match listener.accept().await {
                Ok((socket, _)) => socket,
                Err(e) => {
                    error!("mock OAuth accept error: {e}");
                    return;
                }
            };
            let responses = Arc::clone(&responses);
            let default_response = Arc::clone(&default_response);
            let mint_count = Arc::clone(&mint_count);
            let requests = Arc::clone(&requests);
            tokio::spawn(handle_connection(
                socket,
                responses,
                default_response,
                mint_count,
                requests,
            ));
        }
    });

    (server, base_url)
}

/// Reads one HTTP/1.1 request to completion, counts it as a mint, and writes the
/// next scripted reply.
async fn handle_connection(
    mut socket: TcpStream,
    responses: Arc<Mutex<VecDeque<MockTokenResponse>>>,
    default_response: Arc<MockTokenResponse>,
    mint_count: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
) {
    let Some(request) = read_request(&mut socket).await else {
        return;
    };

    // The request arrived in full: record it and count it as a mint before
    // replying, so a test that observes the reply also observes the request.
    requests.lock().await.push(request);
    mint_count.fetch_add(1, Ordering::SeqCst);

    let response = match responses.lock().await.pop_front() {
        Some(response) => response,
        None => (*default_response).clone(),
    };

    match response {
        // Hold the connection open without replying so the caller's own timeout
        // is what fires (used to model a hung token endpoint).
        MockTokenResponse::Hang => std::future::pending::<()>().await,
        MockTokenResponse::Ok {
            access_token,
            expires_in,
            delay,
        } => {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let body = token_body(&access_token, expires_in.as_deref());
            let _ = write_response(&mut socket, 200, "OK", &body).await;
        }
        MockTokenResponse::Error { status, body } => {
            let _ = write_response(&mut socket, status, reason_phrase(status), &body).await;
        }
    }
}

/// Reads until the header terminator, then reads `Content-Length` body bytes.
/// Returns `None` if the peer closed before a full request arrived.
async fn read_request(socket: &mut TcpStream) -> Option<RecordedRequest> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];

    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    };

    let content_length = parse_content_length(&buf[..header_end]).unwrap_or(0);
    while buf.len() < header_end + content_length {
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    debug!("mock OAuth received a token request");

    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next().unwrap_or_default().split(' ');
    let method = request_line.next().unwrap_or_default().to_string();
    let path = request_line.next().unwrap_or_default().to_string();
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    let body = &buf[header_end..header_end + content_length];
    Some(RecordedRequest {
        method,
        path,
        headers,
        form: parse_form(body),
    })
}

/// Decodes an `application/x-www-form-urlencoded` body.
fn parse_form(body: &[u8]) -> HashMap<String, String> {
    body.split(|&b| b == b'&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let mut kv = pair.splitn(2, |&b| b == b'=');
            let key = form_decode(kv.next().unwrap_or_default());
            let value = form_decode(kv.next().unwrap_or_default());
            (key, value)
        })
        .collect()
}

/// Decodes one form component: `+` is a space and `%XX` is a byte.
fn form_decode(raw: &[u8]) -> String {
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        match raw[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < raw.len() => {
                let hex = std::str::from_utf8(&raw[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(byte) => {
                        out.push(byte);
                        i += 2;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            byte => out.push(byte),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Builds the JSON token body, inserting `expires_in` verbatim when present.
fn token_body(access_token: &str, expires_in: Option<&str>) -> String {
    match expires_in {
        Some(raw) => format!(
            r#"{{"access_token":"{access_token}","token_type":"Bearer","expires_in":{raw}}}"#
        ),
        None => format!(r#"{{"access_token":"{access_token}","token_type":"Bearer"}}"#),
    }
}

/// Writes a minimal HTTP/1.1 response and closes the connection.
async fn write_response(
    socket: &mut TcpStream,
    status: u16,
    reason: &str,
    body: &str,
) -> std::io::Result<()> {
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    );
    socket.write_all(response.as_bytes()).await?;
    socket.flush().await?;
    let _ = socket.shutdown().await;
    Ok(())
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Status",
    }
}

fn parse_content_length(headers: &[u8]) -> Option<usize> {
    let text = String::from_utf8_lossy(headers);
    for line in text.lines() {
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("content-length") {
                return value.trim().parse().ok();
            }
        }
    }
    None
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
