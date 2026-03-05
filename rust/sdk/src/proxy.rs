use hyper_http_proxy::{Intercept, Proxy, ProxyConnector};
use hyper_util::client::legacy::connect::HttpConnector;
use tracing::info;

pub(crate) type ProxiedConnector = ProxyConnector<HttpConnector>;

/// Env var names checked for proxy URL, in gRPC core precedence order.
const PROXY_ENV_VARS: &[&str] = &[
    "grpc_proxy",
    "GRPC_PROXY",
    "https_proxy",
    "HTTPS_PROXY",
    "http_proxy",
    "HTTP_PROXY",
];

/// Env var names checked for no-proxy list, in gRPC core precedence order.
const NO_PROXY_ENV_VARS: &[&str] = &["no_grpc_proxy", "NO_GRPC_PROXY", "no_proxy", "NO_PROXY"];

/// Reads the first non-empty value from the given env var names.
fn read_first_env(names: &[&str]) -> Option<String> {
    for name in names {
        if let Ok(val) = std::env::var(name) {
            if !val.is_empty() {
                return Some(val);
            }
        }
    }
    None
}

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
    let proxy_url = read_first_env(PROXY_ENV_VARS)?;

    let proxy_uri = match proxy_url.parse() {
        Ok(uri) => uri,
        Err(e) => {
            tracing::warn!("Failed to parse proxy URL '{}': {}", proxy_url, e);
            return None;
        }
    };

    info!("Using HTTP proxy: {}", proxy_url);

    let mut proxy = Proxy::new(Intercept::All, proxy_uri);
    // gRPC uses HTTP/2 which cannot traverse a regular HTTP/1 forward proxy.
    // Force CONNECT tunneling for all targets, matching gRPC core behavior.
    proxy.force_connect();
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
    let no_proxy = read_first_env(NO_PROXY_ENV_VARS).unwrap_or_default();
    host_matches_no_proxy(host, &no_proxy)
}

/// Pure logic for no-proxy matching, separated for testability.
fn host_matches_no_proxy(host: &str, no_proxy: &str) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_proxy_empty_returns_false() {
        assert!(!host_matches_no_proxy("example.com", ""));
    }

    #[test]
    fn no_proxy_wildcard_matches_everything() {
        assert!(host_matches_no_proxy("anything.com", "*"));
        assert!(host_matches_no_proxy("localhost", " * "));
    }

    #[test]
    fn no_proxy_exact_match() {
        assert!(host_matches_no_proxy("example.com", "example.com"));
        assert!(!host_matches_no_proxy("other.com", "example.com"));
    }

    #[test]
    fn no_proxy_suffix_match() {
        assert!(host_matches_no_proxy(
            "workspace.cloud.databricks.com",
            "databricks.com"
        ));
        assert!(host_matches_no_proxy("foo.example.com", "example.com"));
        // Must be a subdomain, not just a string suffix
        assert!(!host_matches_no_proxy("notexample.com", "example.com"));
    }

    #[test]
    fn no_proxy_leading_dot_stripped() {
        assert!(host_matches_no_proxy("foo.example.com", ".example.com"));
        assert!(host_matches_no_proxy("example.com", ".example.com"));
    }

    #[test]
    fn no_proxy_comma_separated() {
        let no_proxy = "localhost, 127.0.0.1, .internal.corp";
        assert!(host_matches_no_proxy("localhost", no_proxy));
        assert!(host_matches_no_proxy("127.0.0.1", no_proxy));
        assert!(host_matches_no_proxy("service.internal.corp", no_proxy));
        assert!(!host_matches_no_proxy("external.com", no_proxy));
    }

    #[test]
    fn no_proxy_whitespace_handling() {
        assert!(host_matches_no_proxy("example.com", "  example.com  "));
        assert!(host_matches_no_proxy(
            "example.com",
            "other.com , example.com , more.com"
        ));
    }
}
