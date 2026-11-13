package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/** Native tests for {@link IdpTokenSupplier} and federated stream creation over JNI. */
class FederatedTokenNativeTest {

  // Never reached: each supplier below fails before the SDK contacts the network.
  private static final String UNREACHABLE = "http://127.0.0.1:9";

  @Test
  void supplierCallbacksPreserveRetryability() {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    AtomicInteger calls = new AtomicInteger();

    IdpTokenSupplier supplier =
        () -> {
          switch (calls.getAndIncrement()) {
            case 0:
              throw new IllegalStateException("transient failure");
            case 1:
              return "idp-token";
            case 2:
              throw new NonRetriableException("permanent failure");
            case 3:
              throw new AssertionError("fatal failure");
            default:
              return null;
          }
        };

    assertEquals("OK", NativeTestHelper.nativeTestIdpTokenSupplierCallbacks(supplier));
    assertEquals(5, calls.get());
  }

  @Test
  void federatedProviderSurfacesSupplierError() {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    AtomicInteger calls = new AtomicInteger();

    IdpTokenSupplier supplier =
        () -> {
          calls.incrementAndGet();
          throw new NonRetriableException("idp unavailable");
        };

    assertEquals("OK", NativeTestHelper.nativeTestFederatedProviderSupplierError(supplier));
    assertEquals(1, calls.get());
  }

  @Test
  void federatedJsonStreamCallsSupplierThroughJni() throws Exception {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    AtomicInteger calls = new AtomicInteger();
    FederatedToken token = FederatedToken.workloadIdentity(failingSupplier(calls), "sp-client-id");

    try (ZerobusSdk sdk = new ZerobusSdk(UNREACHABLE, UNREACHABLE)) {
      ExecutionException ex =
          assertThrows(
              ExecutionException.class,
              () ->
                  sdk.streamBuilder()
                      .table("catalog.schema.table")
                      .federatedAuth(token)
                      .recovery(false)
                      .json()
                      .build()
                      .get(30, TimeUnit.SECONDS));
      assertSupplierFailure(ex, calls);
    }
  }

  @Test
  void federatedArrowStreamCallsSupplierThroughJni() throws Exception {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    AtomicInteger calls = new AtomicInteger();
    FederatedToken token = FederatedToken.accountLevel(failingSupplier(calls));
    Schema schema =
        new Schema(Collections.singletonList(Field.nullable("id", new ArrowType.Int(64, true))));

    try (ZerobusSdk sdk = new ZerobusSdk(UNREACHABLE, UNREACHABLE)) {
      ExecutionException ex =
          assertThrows(
              ExecutionException.class,
              () ->
                  sdk.streamBuilder()
                      .table("catalog.schema.table")
                      .federatedAuth(token)
                      .recovery(false)
                      .arrow(schema)
                      .build()
                      .get(30, TimeUnit.SECONDS));
      assertSupplierFailure(ex, calls);
    }
  }

  private static IdpTokenSupplier failingSupplier(AtomicInteger calls) {
    return () -> {
      calls.incrementAndGet();
      throw new NonRetriableException("idp unavailable");
    };
  }

  private static void assertSupplierFailure(ExecutionException ex, AtomicInteger calls) {
    // Reaching the supplier proves the JNI signature and routing; the error must carry its detail.
    assertTrue(calls.get() >= 1, "IdP token supplier was never called");
    Throwable cause = ex.getCause();
    assertNotNull(cause);
    assertTrue(
        String.valueOf(cause.getMessage()).contains("idp unavailable"),
        "unexpected error: " + cause);
  }
}
