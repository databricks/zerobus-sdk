package com.databricks.zerobus;

import java.util.Objects;
import java.util.UUID;

/**
 * Authenticates a stream by federating an external identity provider token (for example from
 * Microsoft Entra ID) instead of a Databricks OAuth client secret. Pass it to {@link
 * StreamBuilder#federatedAuth(FederatedToken)}.
 *
 * <p>The SDK obtains the current IdP token from an {@link IdpTokenSupplier}, exchanges it for a
 * Zerobus-scoped Databricks token using the RFC 8693 token-exchange grant, caches the exchanged
 * token, and refreshes it before it expires. The federation policy must already be configured in
 * Databricks. Two modes are supported:
 *
 * <ul>
 *   <li>{@link #accountLevel(IdpTokenSupplier) Account-level federation}: no Databricks service
 *       principal. The token's subject resolves to an identity synced into Databricks, for example
 *       through automatic identity management.
 *   <li>{@link #workloadIdentity(IdpTokenSupplier, String) Workload identity federation}: a
 *       Databricks service principal with a federation policy and no secret, named by its client
 *       ID.
 * </ul>
 *
 * <pre>{@code
 * FederatedToken auth = FederatedToken.workloadIdentity(() -> fetchEntraIdToken(), spClientId);
 * ZerobusJsonStream stream = sdk.streamBuilder()
 *     .table("catalog.schema.table")
 *     .federatedAuth(auth)
 *     .json()
 *     .build()
 *     .join();
 * }</pre>
 *
 * <p>Instances are immutable. Each instance has its own token cache entry: streams created with the
 * same {@code FederatedToken} share one exchanged token, while two instances never share a token,
 * even with the same supplier. Reuse one instance across streams for the same identity. To switch
 * identities, create a new instance.
 */
public final class FederatedToken {

  private final IdpTokenSupplier idpTokenSupplier;
  private final String clientId;
  private final String cacheIdentity;

  private FederatedToken(IdpTokenSupplier idpTokenSupplier, String clientId) {
    this.idpTokenSupplier = Objects.requireNonNull(idpTokenSupplier, "idpTokenSupplier");
    this.clientId = clientId;
    // A random id fixed at construction partitions the SDK's token cache per instance. It is never
    // derived from an object address, which the JVM or allocator can reuse.
    this.cacheIdentity = "java:" + UUID.randomUUID();
  }

  /**
   * Creates a {@code FederatedToken} for account-level federation, with no Databricks service
   * principal.
   *
   * @param idpTokenSupplier supplies the current external IdP token
   * @return a new {@code FederatedToken}
   * @throws NullPointerException if {@code idpTokenSupplier} is {@code null}
   */
  public static FederatedToken accountLevel(IdpTokenSupplier idpTokenSupplier) {
    return new FederatedToken(idpTokenSupplier, null);
  }

  /**
   * Creates a {@code FederatedToken} for workload identity federation, which authenticates as the
   * given Databricks service principal.
   *
   * @param idpTokenSupplier supplies the current external IdP token
   * @param clientId the client ID of the Databricks service principal that has the federation
   *     policy
   * @return a new {@code FederatedToken}
   * @throws NullPointerException if an argument is {@code null}
   * @throws IllegalArgumentException if {@code clientId} is blank
   */
  public static FederatedToken workloadIdentity(
      IdpTokenSupplier idpTokenSupplier, String clientId) {
    Objects.requireNonNull(clientId, "clientId");
    if (clientId.trim().isEmpty()) {
      throw new IllegalArgumentException("clientId must not be blank");
    }
    return new FederatedToken(idpTokenSupplier, clientId);
  }

  /** Returns the supplier of external IdP tokens. */
  IdpTokenSupplier getIdpTokenSupplier() {
    return idpTokenSupplier;
  }

  /**
   * Returns the Databricks service principal client ID for workload identity federation, or {@code
   * null} for account-level federation.
   */
  String getClientId() {
    return clientId;
  }

  /** Returns the stable id that partitions the SDK's token cache for this instance. */
  String getCacheIdentity() {
    return cacheIdentity;
  }

  @Override
  public String toString() {
    return clientId == null
        ? "FederatedToken{accountLevel}"
        : "FederatedToken{workloadIdentity, clientId=" + clientId + "}";
  }
}
