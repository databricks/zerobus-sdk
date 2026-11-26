//! Fetching a table's schema, and a token to read it, from the Unity Catalog
//! REST API.

use std::error::Error as _;
use std::fmt::Write as _;
use std::net::IpAddr;
use std::time::Duration;

use reqwest::header::ACCEPT;
use reqwest::{StatusCode, Url};

use super::UcTableSchema;
use crate::{DefaultTokenFactory, ZerobusError, ZerobusResult};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Longest error-response body, in characters, kept in an error message.
const MAX_ERROR_BODY_CHARS: usize = 4096;

/// Options for [`fetch_uc_table_schema`] and [`fetch_uc_read_token`].
///
/// Start from `SchemaFetchOptions::default()` and adjust it with the `with_*`
/// setters; the struct is `#[non_exhaustive]` so options can be added without
/// breaking callers.
///
/// Requests honor the `HTTPS_PROXY`, `HTTP_PROXY`, and `NO_PROXY` environment
/// variables and trust the operating system's certificate store.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SchemaFetchOptions {
    /// Deadline for each HTTP request, from connecting to reading the whole
    /// response. Must be greater than zero. Defaults to 30 seconds.
    pub timeout: Duration,
}

impl Default for SchemaFetchOptions {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
        }
    }
}

impl SchemaFetchOptions {
    /// Sets the deadline for each HTTP request.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

/// Fetches a table's schema from the Unity Catalog REST API.
///
/// Sends `GET {workspace_url}/api/2.1/unity-catalog/tables/{table_name}` with
/// `token` as a bearer credential. Convert the returned [`UcTableSchema`] with
/// [`descriptor_from_uc_schema`](super::descriptor_from_uc_schema) for a
/// dynamic protobuf stream, or with `arrow_schema_from_uc_schema` for Arrow
/// Flight (requires the `arrow-flight` feature).
///
/// `workspace_url` is the Databricks workspace URL. It must use `https`, since
/// the token would otherwise travel in plaintext; `http` is accepted only for a
/// loopback IP address, for local testing.
///
/// `token` needs permission to read the table's metadata: `USE CATALOG` and
/// `USE SCHEMA` on its parents and `SELECT` on the table.
/// [`fetch_uc_read_token`] mints a token limited to exactly that. The token a
/// Zerobus stream authenticates with (OAuth or a `zerobus`-scoped personal
/// access token) is scoped to Zerobus writes and is rejected here.
///
/// # Errors
///
/// Invalid input fails before anything is sent:
/// [`ZerobusError::InvalidTableName`] unless `table_name` is
/// `catalog.schema.table`, [`ZerobusError::InvalidArgument`] for an empty
/// `token` or a zero timeout, and [`ZerobusError::InvalidUCEndpointError`] for
/// a malformed or non-`https` `workspace_url`.
///
/// Anything else returns [`ZerobusError::SchemaFetchError`]. Its `status`
/// carries the HTTP status of a non-success response, and `retryable` is set
/// for transient failures: network errors, timeouts, and 5xx (except 501), 408,
/// or 429 responses. A response without columns is a permanent failure.
///
/// # Example
///
/// ```no_run
/// use databricks_zerobus_ingest_sdk::message_descriptor;
/// use databricks_zerobus_ingest_sdk::schema::{
///     descriptor_from_uc_schema, fetch_uc_read_token, fetch_uc_table_schema, SchemaFetchOptions,
/// };
///
/// # async fn example(
/// #     workspace_url: &str,
/// #     client_id: &str,
/// #     client_secret: &str,
/// # ) -> Result<(), Box<dyn std::error::Error>> {
/// let table = "main.analytics.orders";
/// let options = SchemaFetchOptions::default();
///
/// let token = fetch_uc_read_token(workspace_url, table, client_id, client_secret, &options).await?;
/// let schema = fetch_uc_table_schema(workspace_url, table, &token, &options).await?;
///
/// // Resolve a descriptor for a dynamic protobuf stream.
/// let descriptor = message_descriptor(&descriptor_from_uc_schema(&schema)?)?;
/// # let _ = descriptor;
/// # Ok(())
/// # }
/// ```
pub async fn fetch_uc_table_schema(
    workspace_url: &str,
    table_name: &str,
    token: &str,
    options: &SchemaFetchOptions,
) -> ZerobusResult<UcTableSchema> {
    DefaultTokenFactory::parse_table_name(table_name)?;
    if token.trim().is_empty() {
        return Err(ZerobusError::InvalidArgument(
            "token must not be empty".into(),
        ));
    }
    let url = workspace_endpoint(
        workspace_url,
        &["api", "2.1", "unity-catalog", "tables", table_name],
    )?;

    let response = http_client(options)?
        .get(url)
        .bearer_auth(token)
        .header(ACCEPT, "application/json")
        .send()
        .await
        .map_err(transport_error)?;

    let status = response.status();
    if !status.is_success() {
        let body: String = match response.text().await {
            Ok(body) => body.trim().chars().take(MAX_ERROR_BODY_CHARS).collect(),
            Err(e) => format!("<failed to read body: {}>", describe(&e)),
        };
        return Err(fetch_error(
            format!("Unity Catalog returned {status}: {body}"),
            Some(status.as_u16()),
            is_retryable_status(status),
        ));
    }

    // Read the body before parsing it, so a dropped connection stays retryable
    // and only malformed JSON is permanent.
    let body = response.bytes().await.map_err(transport_error)?;
    let schema: UcTableSchema = serde_json::from_slice(&body)
        .map_err(|e| fetch_error(format!("failed to parse response: {e}"), None, false))?;
    if schema.columns.is_empty() {
        return Err(fetch_error(
            format!("Unity Catalog returned no columns for {table_name}"),
            None,
            false,
        ));
    }
    Ok(schema)
}

/// Mints an OAuth access token that can only read `table_name`, for
/// [`fetch_uc_table_schema`].
///
/// Runs the OAuth client-credentials grant against the workspace's token
/// endpoint and downscopes the token to `USE CATALOG`, `USE SCHEMA`, and
/// `SELECT` on the table. The SDK does not cache it.
///
/// `workspace_url` must use `https`, as for [`fetch_uc_table_schema`].
///
/// # Errors
///
/// Invalid input fails before anything is sent, with
/// [`ZerobusError::InvalidTableName`], [`ZerobusError::InvalidUCEndpointError`],
/// or [`ZerobusError::InvalidArgument`] as for [`fetch_uc_table_schema`]. A
/// failed token request returns [`ZerobusError::TokenFetchError`] (retryable)
/// or [`ZerobusError::InvalidUCTokenError`], as the SDK's other OAuth paths do.
pub async fn fetch_uc_read_token(
    workspace_url: &str,
    table_name: &str,
    client_id: &str,
    client_secret: &str,
    options: &SchemaFetchOptions,
) -> ZerobusResult<String> {
    let params = DefaultTokenFactory::table_read_form_params(table_name)?;
    let url = workspace_endpoint(workspace_url, &["oidc", "v1", "token"])?;
    let fetched = DefaultTokenFactory::send_token_request(
        &http_client(options)?,
        url.as_str(),
        &params,
        Some((client_id, client_secret)),
    )
    .await?;
    Ok(fetched.token)
}

/// Appends `path` to the workspace URL, escaping each segment. Requires `https`
/// unless the host is a loopback IP address, so credentials never cross the
/// network in plaintext.
#[allow(clippy::result_large_err)]
fn workspace_endpoint(workspace_url: &str, path: &[&str]) -> ZerobusResult<Url> {
    let invalid =
        |reason: String| ZerobusError::InvalidUCEndpointError(format!("{workspace_url}: {reason}"));

    let mut url = Url::parse(workspace_url).map_err(|e| invalid(e.to_string()))?;
    let loopback = url
        .host_str()
        .map(|host| host.trim_start_matches('[').trim_end_matches(']'))
        .and_then(|host| host.parse::<IpAddr>().ok())
        .is_some_and(|ip| ip.is_loopback());
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err(invalid("must use https".into()));
    }
    url.path_segments_mut()
        .map_err(|()| invalid("cannot be a base URL".into()))?
        .pop_if_empty()
        .extend(path);
    Ok(url)
}

/// Builds the HTTP client for one request: bounded by `options.timeout` and not
/// following redirects, so credentials are never re-sent to another URL.
#[allow(clippy::result_large_err)]
fn http_client(options: &SchemaFetchOptions) -> ZerobusResult<reqwest::Client> {
    if options.timeout.is_zero() {
        return Err(ZerobusError::InvalidArgument(
            "schema fetch timeout must be greater than zero".into(),
        ));
    }
    reqwest::Client::builder()
        .timeout(options.timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| {
            fetch_error(
                format!("failed to build HTTP client: {}", describe(&e)),
                None,
                false,
            )
        })
}

fn fetch_error(message: String, status: Option<u16>, retryable: bool) -> ZerobusError {
    ZerobusError::SchemaFetchError {
        message,
        status,
        retryable,
    }
}

/// A failure to send the request or read the response. Transient unless the
/// request itself was malformed (e.g. a token that isn't a valid header value).
fn transport_error(error: reqwest::Error) -> ZerobusError {
    fetch_error(describe(&error), None, !error.is_builder())
}

/// The error followed by its causes, which carry the actual reason (e.g.
/// "connection refused") that reqwest's own message leaves out.
fn describe(error: &reqwest::Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let _ = write!(message, ": {cause}");
        source = cause.source();
    }
    message
}

/// 5xx (except 501 Not Implemented), 408 Request Timeout, and 429 Too Many
/// Requests are transient; every other status is permanent.
fn is_retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
    ) || (status.is_server_error() && status != StatusCode::NOT_IMPLEMENTED)
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Write as _};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;
    use std::thread;

    use super::*;

    const TABLE: &str = "main.my_schema.my-table";
    const SCHEMA: &str = r#"{
        "name":"my-table","catalog_name":"main","schema_name":"my_schema",
        "columns":[{"name":"id","type_name":"LONG","position":0,"nullable":false}]
    }"#;

    /// Answers one HTTP request on a background thread with `status` and
    /// `body`, returning the base URL and a receiver for the raw request.
    fn serve_once(status: u16, body: impl Into<String>) -> (String, mpsc::Receiver<String>) {
        let body = body.into();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = tx.send(read_request(&mut stream));
            let _ = write!(
                stream,
                "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        });
        (base_url, rx)
    }

    /// Reads the request head, then as many body bytes as `Content-Length`
    /// declares.
    fn read_request(stream: &mut TcpStream) -> String {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut request = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = stream.read(&mut buf).unwrap_or(0);
            if n == 0 {
                break;
            }
            request.extend_from_slice(&buf[..n]);
            let Some(head_len) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
                continue;
            };
            let head = String::from_utf8_lossy(&request[..head_len]).to_ascii_lowercase();
            let body_len = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            if request.len() >= head_len + 4 + body_len {
                break;
            }
        }
        String::from_utf8_lossy(&request).into_owned()
    }

    fn has_header(request: &str, header: &str) -> bool {
        request
            .lines()
            .any(|line| line.eq_ignore_ascii_case(header))
    }

    async fn fetch(base_url: &str) -> ZerobusResult<UcTableSchema> {
        fetch_uc_table_schema(base_url, TABLE, "tok", &SchemaFetchOptions::default()).await
    }

    #[test]
    fn workspace_endpoint_appends_escaped_segments() {
        let path = ["api", "2.1", "unity-catalog", "tables", TABLE];
        for base in ["https://host", "https://host/"] {
            assert_eq!(
                workspace_endpoint(base, &path).unwrap().as_str(),
                "https://host/api/2.1/unity-catalog/tables/main.my_schema.my-table"
            );
        }
        // A path prefix is kept, and only characters that would change the
        // URL's structure are escaped.
        assert_eq!(
            workspace_endpoint("https://host/prefix/", &["tables", "a.b_c-d?e#f/g"])
                .unwrap()
                .as_str(),
            "https://host/prefix/tables/a.b_c-d%3Fe%23f%2Fg"
        );
    }

    #[test]
    fn workspace_endpoint_requires_https_except_on_loopback() {
        for url in [
            "http://example.com",
            "http://localhost:8080",
            "ftp://host",
            "not a url",
        ] {
            assert!(
                matches!(
                    workspace_endpoint(url, &[]),
                    Err(ZerobusError::InvalidUCEndpointError(_))
                ),
                "{url}"
            );
        }
        for url in ["http://127.0.0.1:8080", "http://[::1]:8080"] {
            assert!(workspace_endpoint(url, &[]).is_ok(), "{url}");
        }
    }

    #[test]
    fn retryable_statuses_are_transient() {
        for code in [500, 502, 503, 504, 408, 429] {
            assert!(
                is_retryable_status(StatusCode::from_u16(code).unwrap()),
                "{code}"
            );
        }
        for code in [501, 400, 401, 403, 404] {
            assert!(
                !is_retryable_status(StatusCode::from_u16(code).unwrap()),
                "{code}"
            );
        }
    }

    #[tokio::test]
    async fn fetch_sends_bearer_token_and_parses_schema() {
        let (base_url, request) = serve_once(200, SCHEMA);

        let schema = fetch(&base_url).await.unwrap();
        assert_eq!(schema.name, "my-table");
        assert_eq!(schema.columns.len(), 1);

        let request = request.recv().unwrap();
        assert!(
            request.starts_with("GET /api/2.1/unity-catalog/tables/main.my_schema.my-table "),
            "{request}"
        );
        assert!(
            has_header(&request, "authorization: Bearer tok"),
            "{request}"
        );
        assert!(
            has_header(&request, "accept: application/json"),
            "{request}"
        );
    }

    #[tokio::test]
    async fn fetch_reports_http_status() {
        let (base_url, _request) = serve_once(404, "missing");
        let err = fetch(&base_url).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Failed to fetch table schema: Unity Catalog returned 404 Not Found: missing"
        );
        assert!(
            matches!(
                err,
                ZerobusError::SchemaFetchError {
                    status: Some(404),
                    retryable: false,
                    ..
                }
            ),
            "{err:?}"
        );

        let (base_url, _request) = serve_once(503, "busy");
        let err = fetch(&base_url).await.unwrap_err();
        assert!(
            matches!(
                err,
                ZerobusError::SchemaFetchError {
                    status: Some(503),
                    retryable: true,
                    ..
                }
            ),
            "{err:?}"
        );
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn fetch_caps_error_body() {
        let (base_url, _request) = serve_once(500, "z".repeat(MAX_ERROR_BODY_CHARS + 100));
        let err = fetch(&base_url).await.unwrap_err();
        assert_eq!(err.to_string().matches('z').count(), MAX_ERROR_BODY_CHARS);
    }

    #[tokio::test]
    async fn fetch_rejects_unusable_body_permanently() {
        let no_columns = r#"{"name":"t","catalog_name":"c","schema_name":"s","columns":[]}"#;
        for (body, expected) in [
            ("not json", "failed to parse response"),
            (no_columns, "no columns"),
        ] {
            let (base_url, _request) = serve_once(200, body);
            let err = fetch(&base_url).await.unwrap_err();
            assert!(err.to_string().contains(expected), "{err}");
            assert!(
                matches!(
                    err,
                    ZerobusError::SchemaFetchError {
                        status: None,
                        retryable: false,
                        ..
                    }
                ),
                "{err:?}"
            );
        }
    }

    #[tokio::test]
    async fn fetch_treats_transport_failures_as_retryable() {
        // Connection refused: nothing listens on the port any more.
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let err = fetch(&format!("http://127.0.0.1:{port}"))
            .await
            .unwrap_err();
        let message = err.to_string();
        assert!(
            message.starts_with("Failed to fetch table schema: error sending request"),
            "{message}"
        );
        assert_eq!(
            message.matches("fetch table schema").count(),
            1,
            "{message}"
        );
        assert!(
            matches!(
                err,
                ZerobusError::SchemaFetchError {
                    status: None,
                    retryable: true,
                    ..
                }
            ),
            "{err:?}"
        );

        // Timeout: the listener accepts connections but never answers.
        let silent = TcpListener::bind("127.0.0.1:0").unwrap();
        let options = SchemaFetchOptions::default().with_timeout(Duration::from_millis(100));
        let err = fetch_uc_table_schema(
            &format!("http://{}", silent.local_addr().unwrap()),
            TABLE,
            "tok",
            &options,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                err,
                ZerobusError::SchemaFetchError {
                    status: None,
                    retryable: true,
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn fetch_validates_inputs_before_sending() {
        // Nothing listens here, so reaching the network would fail differently.
        let url = "https://127.0.0.1:1";
        let options = SchemaFetchOptions::default();

        for table in ["", "main.my_schema", "main..t"] {
            assert!(
                matches!(
                    fetch_uc_table_schema(url, table, "tok", &options).await,
                    Err(ZerobusError::InvalidTableName(_))
                ),
                "{table:?}"
            );
        }
        assert!(matches!(
            fetch_uc_table_schema(url, TABLE, " ", &options).await,
            Err(ZerobusError::InvalidArgument(_))
        ));
        assert!(matches!(
            fetch_uc_table_schema("http://example.com", TABLE, "tok", &options).await,
            Err(ZerobusError::InvalidUCEndpointError(_))
        ));
        let zero_timeout = SchemaFetchOptions::default().with_timeout(Duration::ZERO);
        assert!(matches!(
            fetch_uc_table_schema(url, TABLE, "tok", &zero_timeout).await,
            Err(ZerobusError::InvalidArgument(_))
        ));
    }

    #[tokio::test]
    async fn read_token_uses_client_credentials() {
        let (base_url, request) =
            serve_once(200, r#"{"access_token":"read-token","expires_in":3600}"#);

        let token = fetch_uc_read_token(
            &base_url,
            TABLE,
            "id",
            "secret",
            &SchemaFetchOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(token, "read-token");

        let request = request.recv().unwrap();
        assert!(request.starts_with("POST /oidc/v1/token "), "{request}");
        // HTTP Basic credentials: base64("id:secret").
        assert!(
            has_header(&request, "authorization: Basic aWQ6c2VjcmV0"),
            "{request}"
        );
        assert!(
            request.contains("grant_type=client_credentials"),
            "{request}"
        );
        assert!(!request.contains("resource="), "{request}");
    }
}
