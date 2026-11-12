package com.databricks.zerobus;

/**
 * Supplies the current token from an external identity provider (for example a Microsoft Entra ID
 * or other OIDC JWT) for {@link FederatedToken} authentication.
 *
 * <p>The SDK exchanges the returned token for a Zerobus-scoped Databricks token (RFC 8693 token
 * exchange), caches the exchanged token, and refreshes it before it expires. The supplier is called
 * only when a fresh Databricks token must be minted (on a cache miss or a proactive refresh), not
 * on every request, so it may perform a network call to the identity provider.
 *
 * <p>Implementations must be thread-safe. The SDK calls this method from internal SDK threads,
 * including during stream recovery, and may call it concurrently for streams on different tables.
 *
 * @see FederatedToken
 */
@FunctionalInterface
public interface IdpTokenSupplier {

  /**
   * Returns the current external IdP token.
   *
   * <p>Exceptions are treated as retryable, like a failed OAuth token request. Throw {@link
   * NonRetriableException} for permanent failures that should stop recovery immediately. Instances
   * of {@link Error} are always treated as non-retryable.
   *
   * @return the current IdP token; must not be {@code null} or empty
   * @throws Exception if the token cannot be obtained
   */
  String getToken() throws Exception;
}
