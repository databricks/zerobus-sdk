use crate::default_token_factory::{DefaultTokenFactory, FetchedToken, MintReason};
use crate::token_cache::{TokenCache, DEFAULT_REFRESH_BUFFER};
use crate::ZerobusResult;
use async_trait::async_trait;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// A trait for providing custom headers for gRPC requests.
///
/// This trait allows you to implement custom logic for generating authentication headers,
/// such as fetching tokens from different OAuth providers or using alternative
/// authentication mechanisms.
///
/// The HTTP `user-agent` header is set by the SDK on the underlying tonic
/// `Endpoint` and cannot be overridden by values returned from `get_headers`.
/// Use [`ZerobusSdkBuilder::application_name`](crate::ZerobusSdkBuilder::application_name)
/// to customize it.
///
/// # Examples
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::{HeadersProvider, ZerobusResult};
/// # use std::collections::HashMap;
/// # use async_trait::async_trait;
///
/// struct MyCustomAuthProvider;
///
/// #[async_trait]
/// impl HeadersProvider for MyCustomAuthProvider {
///     async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
///         let mut headers = HashMap::new();
///         headers.insert("some_key", "some_value".to_string());
///         Ok(headers)
///     }
/// }
/// ```
#[async_trait]
pub trait HeadersProvider: Send + Sync {
    /// Asynchronously gets the headers for a request.
    ///
    /// # Returns
    ///
    /// A `ZerobusResult` containing a `HashMap` of header names and values.
    ///
    /// # Errors
    ///
    /// Returns a `ZerobusError` if header generation fails (e.g., token request fails).
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>>;

    /// Invalidates cached authentication state that the server just rejected.
    ///
    /// The SDK calls this when the server rejects the supplied credentials with an
    /// authentication error during stream creation. The default is a no-op, which is
    /// correct for providers that hold no cache. The built-in OAuth provider clears
    /// the rejected token from its cache, so the next `get_headers` re-mints — unless
    /// a concurrent refresh already replaced it with a newer token, which is kept and
    /// served without re-minting.
    async fn invalidate(&self) {}
}

/// Selects how a token provider keys the shared [`TokenCache`]: OAuth keys by
/// `(client_id, client_secret)`; federation keys by its precomputed
/// `cache_identity` under a separate scheme with no secret. Centralizing the
/// choice here is what lets both providers share one `get_headers` /
/// `invalidate` body via [`headers_from_cache`] — the only other difference is
/// the mint closure.
#[derive(Clone, Copy)]
enum CacheAccess<'a> {
    Oauth {
        client_id: &'a str,
        client_secret: &'a str,
    },
    Federated {
        cache_identity: &'a str,
    },
}

impl CacheAccess<'_> {
    /// Reads the cached token, refreshing it (a proactive refresh bounded at
    /// `refresh_timeout` when `Some`) if it is due, via the cache methods that
    /// match this access's scheme.
    async fn get_or_refresh<F, Fut>(
        &self,
        cache: &TokenCache,
        table_name: &str,
        refresh_timeout: Option<Duration>,
        fetch: F,
    ) -> ZerobusResult<(String, u64)>
    where
        F: FnOnce(MintReason) -> Fut,
        Fut: Future<Output = ZerobusResult<FetchedToken>>,
    {
        match (*self, refresh_timeout) {
            (
                CacheAccess::Oauth {
                    client_id,
                    client_secret,
                },
                Some(refresh_timeout),
            ) => {
                cache
                    .get_or_fetch_within(
                        client_id,
                        client_secret,
                        table_name,
                        refresh_timeout,
                        fetch,
                    )
                    .await
            }
            (
                CacheAccess::Oauth {
                    client_id,
                    client_secret,
                },
                None,
            ) => {
                cache
                    .get_or_fetch(client_id, client_secret, table_name, fetch)
                    .await
            }
            (CacheAccess::Federated { cache_identity }, Some(refresh_timeout)) => {
                cache
                    .get_or_fetch_within_federated(
                        cache_identity,
                        table_name,
                        refresh_timeout,
                        fetch,
                    )
                    .await
            }
            (CacheAccess::Federated { cache_identity }, None) => {
                cache
                    .get_or_fetch_federated(cache_identity, table_name, fetch)
                    .await
            }
        }
    }

    /// Records a server rejection of the token served at `rejected_generation`.
    async fn invalidate(&self, cache: &TokenCache, table_name: &str, rejected_generation: u64) {
        match *self {
            CacheAccess::Oauth {
                client_id,
                client_secret,
            } => {
                cache
                    .invalidate(client_id, client_secret, table_name, rejected_generation)
                    .await
            }
            CacheAccess::Federated { cache_identity } => {
                cache
                    .invalidate_federated(cache_identity, table_name, rejected_generation)
                    .await
            }
        }
    }
}

/// Shared `get_headers` body for both token providers: read (or refresh) the
/// cached token, record the served generation so a later `invalidate` can reject
/// exactly that token, and build the `authorization` + table-name headers. Only
/// the [`CacheAccess`] and the `fetch` mint closure differ between providers, so a
/// change to the cache read or the header shape is made here once.
async fn headers_from_cache<F, Fut>(
    cache: &TokenCache,
    access: CacheAccess<'_>,
    table_name: &str,
    refresh_timeout: Option<Duration>,
    last_served_generation: &AtomicU64,
    fetch: F,
) -> ZerobusResult<HashMap<&'static str, String>>
where
    F: FnOnce(MintReason) -> Fut,
    Fut: Future<Output = ZerobusResult<FetchedToken>>,
{
    let (token, generation) = access
        .get_or_refresh(cache, table_name, refresh_timeout, fetch)
        .await?;
    // Remember the highest served generation so invalidate() rejects it.
    last_served_generation.fetch_max(generation, Ordering::SeqCst);
    let mut headers = HashMap::new();
    headers.insert("authorization", format!("Bearer {}", token));
    headers.insert("x-databricks-zerobus-table-name", table_name.to_string());
    Ok(headers)
}

/// The default headers provider that uses OAuth 2.0 with Unity Catalog.
///
/// This provider implements the OAuth 2.0 client credentials flow to obtain
/// access tokens for authenticating with the Zerobus service.
pub struct OAuthHeadersProvider {
    client_id: String,
    client_secret: String,
    table_name: String,
    workspace_id: String,
    unity_catalog_url: String,
    token_cache: Arc<TokenCache>,
    /// How long a proactive refresh may run before it's treated as failed and the
    /// cached token is served; `None` leaves it unbounded.
    refresh_timeout: Option<Duration>,
    /// Highest token generation `get_headers` has served, so `invalidate` rejects a
    /// token this provider actually used. Stays 0 until a cacheable token is served,
    /// and `invalidate` treats 0 as nothing to reject.
    last_served_generation: AtomicU64,
}

impl OAuthHeadersProvider {
    /// Creates a new `OAuthHeadersProvider`.
    ///
    /// This standalone constructor caches tokens for the lifetime of the
    /// returned provider only. When streams are created via
    /// [`ZerobusSdk::stream_builder`](crate::ZerobusSdk::stream_builder) the SDK
    /// supplies a shared cache so tokens are reused across streams; see
    /// [`with_cache`](Self::with_cache).
    pub fn new(
        client_id: String,
        client_secret: String,
        table_name: String,
        workspace_id: String,
        unity_catalog_url: String,
    ) -> Self {
        Self::with_cache(
            client_id,
            client_secret,
            table_name,
            workspace_id,
            unity_catalog_url,
            Arc::new(TokenCache::new(true, DEFAULT_REFRESH_BUFFER)),
            None,
        )
    }

    /// Creates a new `OAuthHeadersProvider` backed by a shared token cache.
    ///
    /// Used internally so all streams created from one `ZerobusSdk` reuse cached
    /// tokens rather than minting a fresh one per stream.
    pub(crate) fn with_cache(
        client_id: String,
        client_secret: String,
        table_name: String,
        workspace_id: String,
        unity_catalog_url: String,
        token_cache: Arc<TokenCache>,
        refresh_timeout: Option<Duration>,
    ) -> Self {
        Self {
            client_id,
            client_secret,
            table_name,
            workspace_id,
            unity_catalog_url,
            token_cache,
            refresh_timeout,
            last_served_generation: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl HeadersProvider for OAuthHeadersProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        let fetch = |reason| {
            DefaultTokenFactory::fetch_token(
                &self.unity_catalog_url,
                &self.table_name,
                &self.client_id,
                &self.client_secret,
                &self.workspace_id,
                reason,
            )
        };
        headers_from_cache(
            &self.token_cache,
            CacheAccess::Oauth {
                client_id: &self.client_id,
                client_secret: &self.client_secret,
            },
            &self.table_name,
            self.refresh_timeout,
            &self.last_served_generation,
            fetch,
        )
        .await
    }

    async fn invalidate(&self) {
        let rejected_generation = self.last_served_generation.load(Ordering::SeqCst);
        CacheAccess::Oauth {
            client_id: &self.client_id,
            client_secret: &self.client_secret,
        }
        .invalidate(&self.token_cache, &self.table_name, rejected_generation)
        .await;
    }
}

/// An async callback that yields the current external IdP token (for example an
/// Entra ID / OIDC JWT).
///
/// The federated auth mode takes a supplier callback rather than a bare token
/// on purpose: an external IdP token is short-lived (typically ~1 hour), so a
/// bare token would strand the stream at the token's expiry with no way to
/// refresh. The supplier is invoked only when a fresh Databricks token must be
/// minted (a cache miss or a proactive refresh), never on every request, so a
/// cache hit incurs neither the callback nor the exchange. A caller that truly
/// holds a static token can wrap it in a trivial closure.
pub type IdpTokenCallback =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ZerobusResult<String>> + Send>> + Send + Sync>;

/// A source of external IdP tokens, paired with a stable **cache identity**.
///
/// The identity is what partitions the account-level token cache (workload
/// identity federation keys by its service principal `client_id` instead). It is
/// deliberately a stable string, never a raw pointer: an `Arc`'s address can be
/// reused by the allocator once its last clone is dropped, so pointer identity
/// would let a freshly built supplier at a reused address inherit a previous
/// identity's cached token without ever being called. A monotonic auto id (for
/// pure-Rust callers) or an explicit id supplied by a wrapper SDK (e.g. a
/// per-`FederatedToken` uuid) cannot be reused, so it is safe to key on.
///
/// Cloning shares the identity, so cloning one supplier across streams shares its
/// cached token; a distinct supplier (or an explicit distinct identity) isolates.
#[derive(Clone)]
pub struct IdpTokenSupplier {
    call: IdpTokenCallback,
    identity: String,
}

/// Monotonic source of auto-assigned supplier identities. Monotonic (never
/// reused), unlike an `Arc` address, which is the whole point.
static NEXT_SUPPLIER_ID: AtomicU64 = AtomicU64::new(0);

impl IdpTokenSupplier {
    /// Builds a supplier with an auto-assigned, process-unique stable identity.
    /// Use this for pure-Rust callers; clone the returned supplier to share its
    /// cached token across streams, or build a separate one to isolate.
    pub fn new(call: IdpTokenCallback) -> Self {
        let id = NEXT_SUPPLIER_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            call,
            identity: format!("auto:{id}"),
        }
    }

    /// Builds a supplier with an explicit stable identity. Wrapper SDKs pass a
    /// per-logical-identity id (e.g. a `FederatedToken`'s uuid) so that rebuilding
    /// the native callback per call still shares one cache entry, and two distinct
    /// logical identities never collide.
    pub fn with_identity(call: IdpTokenCallback, identity: String) -> Self {
        Self { call, identity }
    }

    /// The stable cache identity used to partition the account-level token cache.
    pub(crate) fn identity(&self) -> &str {
        &self.identity
    }

    /// Invokes the callback to obtain the current external IdP token.
    pub(crate) fn invoke(&self) -> Pin<Box<dyn Future<Output = ZerobusResult<String>> + Send>> {
        (self.call)()
    }
}

/// A headers provider that federates an external IdP token into a Zerobus-scoped
/// Databricks token via the RFC 8693 token-exchange grant.
///
/// This is the first-class implementation of external-IdP (e.g. Entra ID)
/// federation. It supports the two supported federation modes through a single
/// `client_id` toggle:
///
/// * **Account-level federation** (`client_id = None`): no Databricks-managed
///   service principal. The exchanged token's subject is resolved to an
///   identity synced into Databricks via Automatic Identity Management (SCIM).
/// * **Workload identity federation** (`client_id = Some(sp_id)`): a Databricks
///   service principal with a client_id and no secret, with a federation policy
///   attached. The exchange request names the service principal via `client_id`.
///
/// It obtains the current IdP token from an [`IdpTokenSupplier`], performs the
/// exchange with the same request shaping as the client-credentials path, and
/// caches the exchanged Databricks token in the shared [`TokenCache`]. The
/// cache key is namespaced by identity so entries never collide: workload keys
/// by `fed-wif:{client_id}` (distinct service principals cache independently),
/// and account-level keys by `fed-al:{supplier-identity}` (the supplier's stable
/// id, so distinct suppliers cache independently while streams sharing one
/// supplier share its token). The cache's `AuthScheme::Federated` additionally
/// keeps these entries in a separate key space from `.oauth()`.
pub struct FederatedTokenProvider {
    /// The Databricks service principal client_id for workload identity
    /// federation, or `None` for account-level federation.
    client_id: Option<String>,
    idp_token_supplier: IdpTokenSupplier,
    table_name: String,
    workspace_id: String,
    unity_catalog_url: String,
    token_cache: Arc<TokenCache>,
    /// The shared cache's client-id component, precomputed and namespaced so
    /// federation entries never collide with each other:
    /// `fed-wif:{client_id}` for workload identity federation, and
    /// `fed-al:{supplier-identity}` for account-level (keyed by the supplier's
    /// stable id — never a reusable pointer — so two distinct identities used
    /// from one SDK instance do not collide on the otherwise-empty
    /// `("", "", table)` slot, while streams sharing one supplier share its
    /// cached token). Collisions with `.oauth()` are prevented by the cache's
    /// `AuthScheme`, not by this string.
    cache_identity: String,
    /// How long a proactive refresh may run before it's treated as failed and the
    /// cached token is served; `None` leaves it unbounded.
    refresh_timeout: Option<Duration>,
    /// Highest token generation `get_headers` has served, so `invalidate` rejects a
    /// token this provider actually used. Stays 0 until a cacheable token is served,
    /// and `invalidate` treats 0 as nothing to reject.
    last_served_generation: AtomicU64,
}

impl FederatedTokenProvider {
    /// Creates a new `FederatedTokenProvider`.
    ///
    /// This standalone constructor caches tokens for the lifetime of the
    /// returned provider only. When streams are created via
    /// [`ZerobusSdk::stream_builder`](crate::ZerobusSdk::stream_builder) the SDK
    /// supplies a shared cache so tokens are reused across streams; see
    /// [`with_cache`](Self::with_cache).
    pub fn new(
        client_id: Option<String>,
        idp_token_supplier: IdpTokenSupplier,
        table_name: String,
        workspace_id: String,
        unity_catalog_url: String,
    ) -> Self {
        Self::with_cache(
            client_id,
            idp_token_supplier,
            table_name,
            workspace_id,
            unity_catalog_url,
            Arc::new(TokenCache::new(true, DEFAULT_REFRESH_BUFFER)),
            None,
        )
    }

    /// Creates a new `FederatedTokenProvider` backed by a shared token cache.
    ///
    /// Used internally so all streams created from one `ZerobusSdk` reuse cached
    /// exchanged tokens rather than re-exchanging per stream.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_cache(
        client_id: Option<String>,
        idp_token_supplier: IdpTokenSupplier,
        table_name: String,
        workspace_id: String,
        unity_catalog_url: String,
        token_cache: Arc<TokenCache>,
        refresh_timeout: Option<Duration>,
    ) -> Self {
        // Precompute the namespaced cache identity once. Workload keys by the
        // service principal id; account-level keys by the supplier's stable
        // identity (a monotonic auto id, or a wrapper-supplied uuid) — never a
        // raw pointer, which the allocator can reuse after the last clone drops.
        // Cloning one supplier across streams shares its cached token; a distinct
        // supplier (or distinct explicit identity) isolates. The `fed-wif:` /
        // `fed-al:` prefixes distinguish the two modes; the cache's
        // `AuthScheme::Federated` keeps them from ever colliding with `.oauth()`.
        let cache_identity = match &client_id {
            Some(client_id) => format!("fed-wif:{client_id}"),
            None => format!("fed-al:{}", idp_token_supplier.identity()),
        };
        Self {
            client_id,
            idp_token_supplier,
            table_name,
            workspace_id,
            unity_catalog_url,
            token_cache,
            cache_identity,
            refresh_timeout,
            last_served_generation: AtomicU64::new(0),
        }
    }

    /// The cache key's client-id component: the precomputed, namespaced
    /// [`cache_identity`](Self::cache_identity). There is no secret in either
    /// federation mode, so the secret component of the key is always empty.
    fn cache_client_id(&self) -> &str {
        &self.cache_identity
    }
}

#[async_trait]
impl HeadersProvider for FederatedTokenProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        let fetch = |reason| async move {
            // Only reached on a cache miss/refresh: fetch the current IdP token,
            // then exchange it for a Zerobus-scoped Databricks token.
            let idp_token = self.idp_token_supplier.invoke().await?;
            DefaultTokenFactory::fetch_exchanged_token(
                &self.unity_catalog_url,
                &self.table_name,
                self.client_id.as_deref(),
                &idp_token,
                &self.workspace_id,
                reason,
            )
            .await
        };
        headers_from_cache(
            &self.token_cache,
            CacheAccess::Federated {
                cache_identity: self.cache_client_id(),
            },
            &self.table_name,
            self.refresh_timeout,
            &self.last_served_generation,
            fetch,
        )
        .await
    }

    async fn invalidate(&self) {
        let rejected_generation = self.last_served_generation.load(Ordering::SeqCst);
        CacheAccess::Federated {
            cache_identity: self.cache_client_id(),
        }
        .invalidate(&self.token_cache, &self.table_name, rejected_generation)
        .await;
    }
}

/// A headers provider that returns no headers.
///
/// Intended only for local testing against a Zerobus endpoint not enforcing authentication.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "testing")] {
/// use databricks_zerobus_ingest_sdk::NoAuthHeadersProvider;
/// use std::sync::Arc;
///
/// // Pass directly to `headers_provider()`, or use the `.no_auth()` shorthand on `StreamBuilder`.
/// let _provider: Arc<NoAuthHeadersProvider> = Arc::new(NoAuthHeadersProvider);
/// # }
/// ```
#[cfg(feature = "testing")]
pub struct NoAuthHeadersProvider;

#[cfg(feature = "testing")]
#[async_trait]
impl HeadersProvider for NoAuthHeadersProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        Ok(HashMap::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A minimal blocking HTTP mock of the UC `/oidc/v1/token` endpoint. It runs
    /// on its own OS thread (blocking std IO) so the async test can drive the
    /// reqwest-based exchange against it. Each request is answered with a fresh
    /// `dbx-token-<n>` so tests can tell a real mint from a cache hit, and every
    /// request body is captured for assertions on the request shape.
    struct MockTokenEndpoint {
        base_url: String,
        request_bodies: Arc<std::sync::Mutex<Vec<String>>>,
        mint_count: Arc<AtomicUsize>,
    }

    impl MockTokenEndpoint {
        fn start() -> Self {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let base_url = format!("http://{}", listener.local_addr().unwrap());
            let request_bodies = Arc::new(std::sync::Mutex::new(Vec::new()));
            let mint_count = Arc::new(AtomicUsize::new(0));

            let bodies = Arc::clone(&request_bodies);
            let count = Arc::clone(&mint_count);
            // Detached daemon thread: it serves connections for the lifetime of
            // the test process. Bounded accept is deliberately avoided so that a
            // caching regression (too many mints) fails an assertion rather than
            // deadlocking on a missing connection.
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { continue };
                    let body = read_http_body(&mut stream);
                    let n = count.fetch_add(1, Ordering::SeqCst);
                    bodies.lock().unwrap().push(body);
                    write_json_response(&mut stream, &format!("dbx-token-{n}"));
                }
            });

            Self {
                base_url,
                request_bodies,
                mint_count,
            }
        }

        fn mint_count(&self) -> usize {
            self.mint_count.load(Ordering::SeqCst)
        }

        fn last_request_body(&self) -> String {
            self.request_bodies.lock().unwrap().last().cloned().unwrap()
        }
    }

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    fn read_http_body(stream: &mut std::net::TcpStream) -> String {
        use std::io::Read;
        let mut buf = Vec::new();
        let mut tmp = [0u8; 2048];
        loop {
            let n = stream.read(&mut tmp).unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                let header_str = String::from_utf8_lossy(&buf[..pos]).to_string();
                let content_length = header_str
                    .lines()
                    .find_map(|line| {
                        let lower = line.to_ascii_lowercase();
                        lower
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                    })
                    .unwrap_or(0);
                let body_start = pos + 4;
                while buf.len() < body_start + content_length {
                    let n = stream.read(&mut tmp).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
                let end = (body_start + content_length).min(buf.len());
                return String::from_utf8_lossy(&buf[body_start..end]).to_string();
            }
        }
        String::new()
    }

    fn write_json_response(stream: &mut std::net::TcpStream, access_token: &str) {
        use std::io::Write;
        let body = format!(r#"{{"access_token":"{access_token}","expires_in":3600}}"#);
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    }

    /// Builds an [`IdpTokenSupplier`] that returns `token` and counts its calls,
    /// so tests can assert the supplier is invoked only on a real mint.
    fn counting_supplier(token: &'static str, calls: Arc<AtomicUsize>) -> IdpTokenSupplier {
        IdpTokenSupplier::new(Arc::new(move || {
            let calls = Arc::clone(&calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(token.to_string())
            })
        }))
    }

    const TOKEN_EXCHANGE_GRANT_ENCODED: &str =
        "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange";

    #[tokio::test]
    async fn account_level_exchanges_and_returns_headers() {
        let mock = MockTokenEndpoint::start();
        let idp_calls = Arc::new(AtomicUsize::new(0));
        let provider = FederatedTokenProvider::new(
            None,
            counting_supplier("entra-jwt", Arc::clone(&idp_calls)),
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            mock.base_url.clone(),
        );

        let headers = provider.get_headers().await.unwrap();

        assert_eq!(headers.get("authorization").unwrap(), "Bearer dbx-token-0");
        assert_eq!(
            headers.get("x-databricks-zerobus-table-name").unwrap(),
            "cat.sch.tbl"
        );
        assert_eq!(mock.mint_count(), 1);
        assert_eq!(idp_calls.load(Ordering::SeqCst), 1);

        // The exchange request carried the RFC 8693 grant and the IdP token, and
        // omitted client_id (account-level federation).
        let body = mock.last_request_body();
        assert!(body.contains(TOKEN_EXCHANGE_GRANT_ENCODED), "body: {body}");
        assert!(body.contains("subject_token=entra-jwt"), "body: {body}");
        assert!(!body.contains("client_id="), "body: {body}");
    }

    #[tokio::test]
    async fn workload_identity_sends_client_id() {
        let mock = MockTokenEndpoint::start();
        let idp_calls = Arc::new(AtomicUsize::new(0));
        let provider = FederatedTokenProvider::new(
            Some("sp-uuid".to_string()),
            counting_supplier("entra-jwt", Arc::clone(&idp_calls)),
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            mock.base_url.clone(),
        );

        provider.get_headers().await.unwrap();

        let body = mock.last_request_body();
        assert!(body.contains(TOKEN_EXCHANGE_GRANT_ENCODED), "body: {body}");
        assert!(body.contains("client_id=sp-uuid"), "body: {body}");
    }

    #[tokio::test]
    async fn caches_exchanged_token_across_calls() {
        let mock = MockTokenEndpoint::start();
        let idp_calls = Arc::new(AtomicUsize::new(0));
        let provider = FederatedTokenProvider::new(
            None,
            counting_supplier("entra-jwt", Arc::clone(&idp_calls)),
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            mock.base_url.clone(),
        );

        let first = provider.get_headers().await.unwrap();
        let second = provider.get_headers().await.unwrap();

        assert_eq!(first.get("authorization"), second.get("authorization"));
        assert_eq!(mock.mint_count(), 1, "second call must reuse cached token");
        assert_eq!(
            idp_calls.load(Ordering::SeqCst),
            1,
            "IdP supplier must not be called on a cache hit"
        );
    }

    #[tokio::test]
    async fn invalidate_forces_remint() {
        let mock = MockTokenEndpoint::start();
        let idp_calls = Arc::new(AtomicUsize::new(0));
        let provider = FederatedTokenProvider::new(
            None,
            counting_supplier("entra-jwt", Arc::clone(&idp_calls)),
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            mock.base_url.clone(),
        );

        let first = provider.get_headers().await.unwrap();
        provider.invalidate().await;
        let second = provider.get_headers().await.unwrap();

        assert_eq!(first.get("authorization").unwrap(), "Bearer dbx-token-0");
        assert_eq!(second.get("authorization").unwrap(), "Bearer dbx-token-1");
        assert_eq!(mock.mint_count(), 2, "invalidate must force a re-mint");
        assert_eq!(idp_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn account_level_and_workload_cache_independently() {
        let mock = MockTokenEndpoint::start();
        // A shared cache, as the SDK supplies to every stream from one instance.
        let cache = Arc::new(TokenCache::new(true, DEFAULT_REFRESH_BUFFER));

        let account_level = FederatedTokenProvider::with_cache(
            None,
            counting_supplier("entra-jwt", Arc::new(AtomicUsize::new(0))),
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            mock.base_url.clone(),
            Arc::clone(&cache),
            None,
        );
        let workload = FederatedTokenProvider::with_cache(
            Some("sp-uuid".to_string()),
            counting_supplier("entra-jwt", Arc::new(AtomicUsize::new(0))),
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            mock.base_url.clone(),
            Arc::clone(&cache),
            None,
        );

        // Same table, same shared cache, but different client_id => two mints.
        account_level.get_headers().await.unwrap();
        workload.get_headers().await.unwrap();
        assert_eq!(
            mock.mint_count(),
            2,
            "account-level and workload modes must key independently"
        );

        // Each then serves its own cached token.
        account_level.get_headers().await.unwrap();
        workload.get_headers().await.unwrap();
        assert_eq!(mock.mint_count(), 2, "both must now be cache hits");
    }

    #[tokio::test]
    async fn account_level_isolates_by_supplier_identity() {
        let mock = MockTokenEndpoint::start();
        // One shared SDK cache, account-level identities (no client_id) on the
        // SAME table. With no client_id the key would collapse to ("", "", table)
        // and collide, so account-level partitions by the supplier's stable
        // identity: a cloned supplier shares the cached token, a distinct supplier
        // isolates.
        let cache = Arc::new(TokenCache::new(true, DEFAULT_REFRESH_BUFFER));

        let supplier_a = counting_supplier("entra-jwt-a", Arc::new(AtomicUsize::new(0)));
        let supplier_b = counting_supplier("entra-jwt-b", Arc::new(AtomicUsize::new(0)));

        let make = |supplier: IdpTokenSupplier| {
            FederatedTokenProvider::with_cache(
                None,
                supplier,
                "cat.sch.tbl".to_string(),
                "12345".to_string(),
                mock.base_url.clone(),
                Arc::clone(&cache),
                None,
            )
        };

        // Two providers built from the SAME supplier (cloned) => one mint, shared.
        let identity_a = make(supplier_a.clone());
        let identity_a_again = make(supplier_a.clone());
        identity_a.get_headers().await.unwrap();
        identity_a_again.get_headers().await.unwrap();
        assert_eq!(
            mock.mint_count(),
            1,
            "streams sharing one supplier must share its cached token"
        );

        // A DIFFERENT supplier is a different identity => a second, independent mint.
        let identity_b = make(supplier_b.clone());
        identity_b.get_headers().await.unwrap();
        assert_eq!(
            mock.mint_count(),
            2,
            "a distinct supplier must not be served another identity's token"
        );

        // Workload federation (client_id = "x") must not share with account-level
        // even on the same table, thanks to the fed-wif:/fed-al: namespacing.
        let workload = FederatedTokenProvider::with_cache(
            Some("x".to_string()),
            supplier_a.clone(),
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            mock.base_url.clone(),
            Arc::clone(&cache),
            None,
        );
        workload.get_headers().await.unwrap();
        assert_eq!(
            mock.mint_count(),
            3,
            "workload federation must key independently of account-level"
        );
    }

    #[tokio::test]
    async fn supplier_error_propagates_and_is_not_cached() {
        // A supplier whose token fetch fails (e.g. the external IdP rejected the
        // credentials). The error must surface from get_headers, and because
        // nothing was cached, a subsequent call must invoke the supplier again
        // rather than serving a stale/absent token. No network is used: the
        // supplier fails before the exchange is ever attempted.
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in_cb = Arc::clone(&calls);
        let supplier = IdpTokenSupplier::new(Arc::new(move || {
            let calls = Arc::clone(&calls_in_cb);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(crate::ZerobusError::InvalidUCTokenError(
                    "external IdP token fetch failed".to_string(),
                ))
            })
        }));
        let provider = FederatedTokenProvider::new(
            None,
            supplier,
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            "http://127.0.0.1:1".to_string(),
        );

        assert!(
            provider.get_headers().await.is_err(),
            "first call must error"
        );
        assert!(
            provider.get_headers().await.is_err(),
            "second call must error too"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "a failed mint must not be cached; the supplier is retried each call"
        );
    }
}
