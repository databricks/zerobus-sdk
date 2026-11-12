package com.databricks.zerobus;

import java.util.Map;
import java.util.Objects;

/**
 * Internal carrier for a {@link FederatedToken}.
 *
 * <p>Streams keep their authentication as {@code (clientId, clientSecret, headersProvider)} so that
 * {@code recreateStream} can reopen them. Wrapping the token in this provider lets federated auth
 * reuse that path unchanged. It is never invoked as a headers provider: {@link ZerobusSdk} detects
 * it and creates the stream through the native federated-auth path, where the Rust core performs
 * the token exchange, caching, and refresh.
 */
final class FederatedAuthHeadersProvider implements HeadersProvider {

  private final FederatedToken token;

  FederatedAuthHeadersProvider(FederatedToken token) {
    this.token = Objects.requireNonNull(token, "token");
  }

  FederatedToken token() {
    return token;
  }

  @Override
  public Map<String, String> getHeaders() {
    throw new IllegalStateException("Federated authentication is handled by the native SDK");
  }
}
