#ifndef ZEROBUS_TOKEN_HPP
#define ZEROBUS_TOKEN_HPP

#include <chrono>
#include <optional>
#include <string>

namespace zerobus {

/// A Zerobus-scoped Databricks OAuth access token, returned by `get_token()`.
struct OAuthToken {
  /// The bearer token.
  std::string access_token;
  /// When the token expires, measured from when the request was sent. Empty
  /// if the server reported no lifetime.
  std::optional<std::chrono::system_clock::time_point> expires_at;
};

/// Fetch a Zerobus-scoped OAuth token for use outside the SDK's own streams.
///
/// Mints the same token the SDK uses for its streams (OAuth client
/// credentials, scoped to writing `table_name`), for clients that take an
/// OAuth token callback, e.g. a Kafka producer using SASL/OAUTHBEARER. Each
/// call fetches a new token; there is no caching, so call it from the client's
/// refresh hook. Blocks until the token endpoint responds, for at most 30
/// seconds.
///
/// `table_name` is `catalog.schema.table`; `workspace_id` is the numeric
/// workspace ID. Throws `ZerobusException`: `is_retryable()` is true if the
/// token endpoint was unreachable or failed, false if the input is invalid or
/// the endpoint rejected the request (for example, bad credentials or missing
/// table privileges).
OAuthToken get_token(const std::string& workspace_url,
                     const std::string& table_name,
                     const std::string& client_id,
                     const std::string& client_secret,
                     const std::string& workspace_id);

}  // namespace zerobus

#endif  // ZEROBUS_TOKEN_HPP
