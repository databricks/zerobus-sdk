package com.databricks.zerobus;

import java.time.Instant;
import java.util.Optional;

/**
 * A Zerobus-scoped OAuth 2.0 access token and its expiry, returned by {@link
 * ZerobusSdk#getToken(String, String, String, String, String)}.
 *
 * <p>{@link #toString()} redacts the token so it can't leak into logs.
 */
public final class OAuthToken {
  private final String accessToken;
  private final Instant expiresAt;

  OAuthToken(String accessToken, Instant expiresAt) {
    this.accessToken = accessToken;
    this.expiresAt = expiresAt;
  }

  /**
   * Returns the OAuth access token, sent as {@code Bearer <accessToken>}.
   *
   * @return the access token
   */
  public String getAccessToken() {
    return accessToken;
  }

  /**
   * Returns when the token expires, or empty if the server reported no lifetime. Measured from when
   * the request was sent, so it never overstates the token's validity.
   *
   * @return the expiry, if known
   */
  public Optional<Instant> getExpiresAt() {
    return Optional.ofNullable(expiresAt);
  }

  @Override
  public String toString() {
    return "OAuthToken{accessToken=<redacted>, expiresAt=" + expiresAt + "}";
  }
}
