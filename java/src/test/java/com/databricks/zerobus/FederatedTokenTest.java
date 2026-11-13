package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Unit tests for {@link FederatedToken} and {@link StreamBuilder#federatedAuth}. */
public class FederatedTokenTest {

  private static final IdpTokenSupplier SUPPLIER = () -> "idp-token";

  @Test
  void accountLevelHasNoClientId() {
    FederatedToken token = FederatedToken.accountLevel(SUPPLIER);
    assertNull(token.getClientId());
    assertSame(SUPPLIER, token.getIdpTokenSupplier());
    assertEquals("FederatedToken{accountLevel}", token.toString());
  }

  @Test
  void workloadIdentityKeepsClientId() {
    FederatedToken token = FederatedToken.workloadIdentity(SUPPLIER, "sp-client-id");
    assertEquals("sp-client-id", token.getClientId());
    assertSame(SUPPLIER, token.getIdpTokenSupplier());
    assertEquals("FederatedToken{workloadIdentity, clientId=sp-client-id}", token.toString());
  }

  @Test
  void factoriesRejectInvalidArguments() {
    assertThrows(NullPointerException.class, () -> FederatedToken.accountLevel(null));
    assertThrows(
        NullPointerException.class, () -> FederatedToken.workloadIdentity(null, "sp-client-id"));
    assertThrows(NullPointerException.class, () -> FederatedToken.workloadIdentity(SUPPLIER, null));
    assertThrows(
        IllegalArgumentException.class, () -> FederatedToken.workloadIdentity(SUPPLIER, ""));
    assertThrows(
        IllegalArgumentException.class, () -> FederatedToken.workloadIdentity(SUPPLIER, "   "));
  }

  @Test
  void cacheIdentityIsStablePerInstanceAndDistinctAcrossInstances() {
    FederatedToken first = FederatedToken.accountLevel(SUPPLIER);
    FederatedToken second = FederatedToken.accountLevel(SUPPLIER);

    assertTrue(first.getCacheIdentity().startsWith("java:"));
    assertEquals(first.getCacheIdentity(), first.getCacheIdentity());
    // Same supplier, distinct instances: they must not share a cached token.
    assertNotEquals(first.getCacheIdentity(), second.getCacheIdentity());
  }

  @Test
  void federatedAuthSatisfiesValidation() {
    StreamBuilder b =
        new StreamBuilder(null)
            .table("catalog.schema.table")
            .federatedAuth(FederatedToken.accountLevel(SUPPLIER));
    assertDoesNotThrow(b::validateRequired);
  }

  @Test
  void federatedAuthRejectsNull() {
    assertThrows(NullPointerException.class, () -> new StreamBuilder(null).federatedAuth(null));
  }

  @Test
  void laterAuthCallReplacesFederatedAuth() {
    StreamBuilder b =
        new StreamBuilder(null)
            .table("catalog.schema.table")
            .federatedAuth(FederatedToken.accountLevel(SUPPLIER))
            .oauth("client-id", "client-secret");
    assertDoesNotThrow(b::validateRequired);
  }

  @Test
  void validationMessageMentionsFederatedAuth() {
    StreamBuilder b = new StreamBuilder(null).table("catalog.schema.table");
    IllegalStateException ex = assertThrows(IllegalStateException.class, b::validateRequired);
    assertTrue(ex.getMessage().contains("federatedAuth()"));
  }

  @Test
  void carrierIsNeverUsedAsAHeadersProvider() {
    FederatedAuthHeadersProvider carrier =
        new FederatedAuthHeadersProvider(FederatedToken.accountLevel(SUPPLIER));
    assertThrows(IllegalStateException.class, carrier::getHeaders);
  }
}
