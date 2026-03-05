use hyper_http_proxy::{Intercept, Proxy, ProxyConnector};
use hyper_util::client::legacy::connect::HttpConnector;
use tracing::info;

pub(crate) type ProxiedConnector = ProxyConnector<HttpConnector>;

/// Reads proxy environment variables and returns a `ProxyConnector`
/// if one is configured, or `None` for direct connections.
///
/// Follows gRPC core precedence: `grpc_proxy` → `https_proxy` → `http_proxy`.
/// For each name the lowercase variant is checked first, then uppercase
/// (matching standard convention and gRPC core behavior).
///
/// The proxy connector uses HTTP CONNECT tunneling for HTTPS targets;
/// TLS is not applied at the proxy layer — tonic handles TLS on top.
pub(crate) fn create_proxy_connector() -> Option<ProxiedConnector> {
    let proxy_url = std::env::var("grpc_proxy")
        .or_else(|_| std::env::var("GRPC_PROXY"))
        .or_else(|_| std::env::var("https_proxy"))
        .or_else(|_| std::env::var("HTTPS_PROXY"))
        .or_else(|_| std::env::var("http_proxy"))
        .or_else(|_| std::env::var("HTTP_PROXY"))
        .ok()?;

    if proxy_url.is_empty() {
        return None;
    }

    let proxy_uri = match proxy_url.parse() {
        Ok(uri) => uri,
        Err(e) => {
            tracing::warn!("Failed to parse proxy URL '{}': {}", proxy_url, e);
            return None;
        }
    };

    info!("Using HTTP proxy: {}", proxy_url);

    let proxy = Proxy::new(Intercept::All, proxy_uri);
    let mut http_connector = HttpConnector::new();
    http_connector.enforce_http(false);

    // Use unsecured: the CONNECT tunnel returns a raw TCP stream,
    // and tonic adds TLS on top via its own endpoint TLS config.
    let proxy_connector = ProxyConnector::from_proxy_unsecured(http_connector, proxy);

    Some(proxy_connector)
}

/// Checks whether a given host should bypass the proxy.
///
/// Follows gRPC core precedence: `no_grpc_proxy` → `no_proxy`.
/// For each name the lowercase variant is checked first, then uppercase.
/// A wildcard `*` matches all hosts. Otherwise entries are matched as
/// suffix of the target host (e.g. `example.com` matches `foo.example.com`).
pub(crate) fn is_no_proxy(host: &str) -> bool {
    let no_proxy = std::env::var("no_grpc_proxy")
        .or_else(|_| std::env::var("NO_GRPC_PROXY"))
        .or_else(|_| std::env::var("no_proxy"))
        .or_else(|_| std::env::var("NO_PROXY"))
        .unwrap_or_default();

    if no_proxy.is_empty() {
        return false;
    }

    if no_proxy.trim() == "*" {
        return true;
    }

    no_proxy.split(',').any(|entry| {
        let entry = entry.trim().trim_start_matches('.');
        host == entry || host.ends_with(&format!(".{}", entry))
    })
}
