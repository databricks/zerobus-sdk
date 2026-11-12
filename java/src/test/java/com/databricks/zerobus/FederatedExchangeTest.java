package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.ref.WeakReference;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Federation tests through the public API and JNI, against a mock token endpoint.
 *
 * <p>The SDK's Unity Catalog URL points at {@link MockTokenServer}, which records every exchange
 * request. The gRPC endpoint is a refused port with recovery off: the SDK exchanges the token
 * before it connects, so each stream creation reaches the token endpoint and then fails to connect.
 * Assertions are on what reached the token endpoint and how often the supplier ran.
 */
class FederatedExchangeTest {

  private static final String TABLE = "catalog.schema.table";
  private static final String TABLE_2 = "catalog.schema.table_2";
  private static final String SP_ID = "11111111-2222-3333-4444-555555555555";
  private static final SecureRandom RANDOM = new SecureRandom();

  private MockTokenServer tokens;
  private ZerobusSdk sdk;

  @BeforeEach
  void setUp() throws Exception {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    startMocks();
  }

  private void startMocks(MockTokenServer.Reply... script) throws Exception {
    closeMocks();
    tokens = new MockTokenServer(script);
    sdk = new ZerobusSdk("http://127.0.0.1:" + refusedPort(), tokens.url());
  }

  @AfterEach
  void closeMocks() {
    if (sdk != null) {
      sdk.close();
      sdk = null;
    }
    if (tokens != null) {
      tokens.close();
      tokens = null;
    }
  }

  // ==================== The exchange request ====================

  @Test
  void accountLevelExchangeRequest() throws Exception {
    String jwt = fakeJwt("alice");
    failToConnect(FederatedToken.accountLevel(() -> jwt), TABLE);

    MockTokenServer.Request r = onlyRequest();
    assertExchangeRequest(r, jwt);
    assertFalse(r.form.containsKey("client_id"), "account-level exchange must not send client_id");
  }

  @Test
  void workloadIdentityExchangeRequest() throws Exception {
    String jwt = fakeJwt("wif");
    failToConnect(FederatedToken.workloadIdentity(() -> jwt, SP_ID), TABLE);

    MockTokenServer.Request r = onlyRequest();
    assertExchangeRequest(r, jwt);
    assertEquals(Collections.singletonList(SP_ID), r.form.get("client_id"));
  }

  // ==================== Caching ====================

  @Test
  void oneTokenIsSharedAcrossStreams() throws Exception {
    CountingSupplier supplier = new CountingSupplier(fakeJwt("a"));
    FederatedToken token = FederatedToken.accountLevel(supplier);
    failToConnect(token, TABLE);
    failToConnect(token, TABLE);

    assertEquals(1, supplier.calls.get(), "the second stream must hit the cache");
    assertEquals(1, tokens.count());
  }

  @Test
  void distinctAccountLevelTokensNeverShare() throws Exception {
    String jwtA = fakeJwt("alice");
    String jwtB = fakeJwt("bob");
    failToConnect(FederatedToken.accountLevel(() -> jwtA), TABLE);
    failToConnect(FederatedToken.accountLevel(() -> jwtB), TABLE);

    List<MockTokenServer.Request> reqs = tokens.requests();
    assertEquals(2, reqs.size(), "each token needs its own exchange");
    assertSameToken(jwtA, reqs.get(0).formValue("subject_token"), "first exchange");
    assertSameToken(jwtB, reqs.get(1).formValue("subject_token"), "second exchange");
  }

  @Test
  void workloadIdentityIsKeyedByClientId() throws Exception {
    String jwt = fakeJwt("wif");
    failToConnect(FederatedToken.workloadIdentity(() -> jwt, SP_ID), TABLE);
    failToConnect(FederatedToken.workloadIdentity(() -> jwt, SP_ID), TABLE);
    assertEquals(1, tokens.count(), "the same client ID shares one exchanged token");

    failToConnect(
        FederatedToken.workloadIdentity(() -> jwt, "99999999-8888-7777-6666-555555555555"), TABLE);
    assertEquals(2, tokens.count(), "a different client ID needs its own exchange");
  }

  @Test
  void tokensArePerTable() throws Exception {
    FederatedToken token = FederatedToken.accountLevel(new CountingSupplier(fakeJwt("a")));
    failToConnect(token, TABLE);
    failToConnect(token, TABLE_2);
    assertEquals(2, tokens.count());
  }

  @Test
  void concurrentCreatesMintOnce() throws Exception {
    CountingSupplier supplier = new CountingSupplier(500, fakeJwt("a"));
    FederatedToken token = FederatedToken.accountLevel(supplier);

    ExecutorService pool = Executors.newFixedThreadPool(8);
    try {
      List<Future<Throwable>> results = new ArrayList<>();
      for (int i = 0; i < 8; i++) {
        results.add(pool.submit(() -> createJson(token, TABLE)));
      }
      for (Future<Throwable> f : results) {
        assertConnectFailure(f.get(60, TimeUnit.SECONDS));
      }
    } finally {
      pool.shutdownNow();
    }
    assertEquals(1, supplier.calls.get());
    assertEquals(1, tokens.count());
  }

  @Test
  void recreateStreamKeepsTheFederatedIdentity() throws Exception {
    CountingSupplier supplier = new CountingSupplier(fakeJwt("a"));
    FederatedToken token = FederatedToken.accountLevel(supplier);
    ZerobusJsonStream closed =
        new ZerobusJsonStream(
            0,
            TABLE,
            StreamConfigurationOptions.builder().setRecovery(false).build(),
            null,
            null,
            new FederatedAuthHeadersProvider(token));

    // A recreate goes through the federated path: the supplier runs and the token is exchanged.
    assertConnectFailure(recreate(closed));
    assertEquals(1, supplier.calls.get());
    assertEquals(1, tokens.count());

    // A second recreate, and a fresh stream on the same token, reuse that exchanged token.
    assertConnectFailure(recreate(closed));
    failToConnect(token, TABLE);
    assertEquals(1, supplier.calls.get());
    assertEquals(1, tokens.count());
  }

  // ==================== Supplier errors ====================

  @Test
  void supplierExceptionIsRetryableAndNotCached() throws Exception {
    String jwt = fakeJwt("a");
    AtomicInteger calls = new AtomicInteger();
    FederatedToken token =
        FederatedToken.accountLevel(
            () -> {
              if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("idp throttled");
              }
              return jwt;
            });

    Throwable cause = createJson(token, TABLE);
    assertRetryable(cause, "idp throttled");
    assertFalse(String.valueOf(cause.getMessage()).contains(jwt));
    assertEquals(0, tokens.count());

    failToConnect(token, TABLE); // not cached: the supplier runs again
    assertEquals(2, calls.get());
    assertEquals(1, tokens.count());
  }

  @Test
  void nonRetriableExceptionStaysPermanent() throws Exception {
    Throwable cause =
        createJson(
            FederatedToken.accountLevel(
                () -> {
                  throw new NonRetriableException("idp unavailable");
                }),
            TABLE);
    assertPermanent(cause, "idp unavailable");
  }

  @Test
  void nullTokenIsRejectedAndNeverSent() throws Exception {
    assertPermanent(createJson(FederatedToken.accountLevel(() -> null), TABLE), "null");
    assertEquals(0, tokens.count());
  }

  @Test
  void emptyTokenIsRejectedAndNeverSent() throws Exception {
    assertPermanent(createJson(FederatedToken.accountLevel(() -> ""), TABLE), "empty");
    assertEquals(0, tokens.count(), "an empty subject_token must never be sent");
  }

  @Test
  void errorThrownBySupplierIsContained() throws Exception {
    Throwable cause =
        createJson(
            FederatedToken.accountLevel(
                () -> {
                  throw new AssertionError("supplier bug");
                }),
            TABLE);
    assertPermanent(cause, "supplier bug");

    // The SDK and JNI stay usable: no pending exception is left behind.
    failToConnect(FederatedToken.accountLevel(new CountingSupplier(fakeJwt("a"))), TABLE);
    assertEquals(1, tokens.count());
  }

  // ==================== Refresh and exchange failures ====================

  @Test
  void refreshReinvokesTheSupplier() throws Exception {
    // A short-lived token is refreshed once less than min(300 s, TTL / 2) remains: after 1 s here.
    startMocks(MockTokenServer.Reply.ok(2));
    String jwt1 = fakeJwt("first");
    String jwt2 = fakeJwt("second");
    CountingSupplier supplier = new CountingSupplier(jwt1, jwt2);
    FederatedToken token = FederatedToken.accountLevel(supplier);

    failToConnect(token, TABLE);
    Thread.sleep(1500);
    failToConnect(token, TABLE);
    waitFor(() -> tokens.count() >= 2);

    List<MockTokenServer.Request> reqs = tokens.requests();
    assertSameToken(jwt1, reqs.get(0).formValue("subject_token"), "first exchange");
    assertSameToken(jwt2, reqs.get(1).formValue("subject_token"), "refresh must use the new token");
    assertTrue(supplier.calls.get() >= 2);
  }

  @Test
  void exchangeClientErrorIsPermanentWithTheServersReason() throws Exception {
    for (int status : new int[] {401, 400}) {
      startMocks(errors(status));
      String jwt = fakeJwt("a");
      Throwable cause = createJson(FederatedToken.accountLevel(() -> jwt), TABLE);
      assertPermanent(cause, "federation policy mismatch");
      assertFalse(String.valueOf(cause.getMessage()).contains(jwt), "error leaks the IdP token");
    }
  }

  @Test
  void exchangeServerErrorIsRetryable() throws Exception {
    startMocks(errors(500));
    String jwt = fakeJwt("a");
    Throwable cause = createJson(FederatedToken.accountLevel(() -> jwt), TABLE);
    assertRetryable(cause, null);
    assertFalse(String.valueOf(cause.getMessage()).contains(jwt), "error leaks the IdP token");
  }

  // ==================== Arrow entry point ====================

  @Test
  void arrowStreamsSendTheRightExchangeAndShareTheToken() throws Exception {
    Schema schema =
        new Schema(Collections.singletonList(Field.nullable("id", new ArrowType.Int(64, true))));
    for (boolean workload : new boolean[] {false, true}) {
      startMocks();
      String jwt = fakeJwt(workload ? "arrow-wif" : "arrow-al");
      CountingSupplier supplier = new CountingSupplier(jwt);
      FederatedToken token =
          workload
              ? FederatedToken.workloadIdentity(supplier, SP_ID)
              : FederatedToken.accountLevel(supplier);
      for (int i = 0; i < 2; i++) {
        assertConnectFailure(
            causeOf(
                sdk.streamBuilder()
                    .table(TABLE)
                    .federatedAuth(token)
                    .recovery(false)
                    .arrow(schema)
                    .build()));
      }

      MockTokenServer.Request r = onlyRequest();
      assertEquals(1, supplier.calls.get(), "Arrow streams must share the token");
      assertExchangeRequest(r, jwt);
      if (workload) {
        assertEquals(Collections.singletonList(SP_ID), r.form.get("client_id"));
      } else {
        assertFalse(r.form.containsKey("client_id"));
      }
    }
  }

  // ==================== JNI references ====================

  @Test
  void supplierIsReleasedAfterStreams() throws Exception {
    WeakReference<IdpTokenSupplier> released = createAndDropFederatedStreams();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (released.get() != null && System.nanoTime() < deadline) {
      System.gc();
      Thread.sleep(20);
    }
    assertNull(released.get(), "the JNI global reference to the supplier was never released");
  }

  private WeakReference<IdpTokenSupplier> createAndDropFederatedStreams() throws Exception {
    IdpTokenSupplier supplier = new CountingSupplier(fakeJwt("a"));
    FederatedToken token = FederatedToken.accountLevel(supplier);
    failToConnect(token, TABLE);
    failToConnect(token, TABLE);
    return new WeakReference<>(supplier);
  }

  // ==================== Helpers ====================

  /** A realistic ~1.5 KB JWT-shaped token that expires in 2 h. */
  private static String fakeJwt(String subject) {
    Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
    byte[] pad = new byte[700];
    byte[] sig = new byte[256];
    RANDOM.nextBytes(pad);
    RANDOM.nextBytes(sig);
    long exp = System.currentTimeMillis() / 1000 + 7200;
    String header = "{\"alg\":\"RS256\",\"typ\":\"JWT\"}";
    String payload =
        "{\"sub\":\""
            + subject
            + "\",\"exp\":"
            + exp
            + ",\"pad\":\""
            + enc.encodeToString(pad)
            + "\"}";
    return enc.encodeToString(header.getBytes(StandardCharsets.UTF_8))
        + "."
        + enc.encodeToString(payload.getBytes(StandardCharsets.UTF_8))
        + "."
        + enc.encodeToString(sig);
  }

  /** Returns its tokens in order (the last one repeats), optionally after a delay. */
  private static final class CountingSupplier implements IdpTokenSupplier {
    final AtomicInteger calls = new AtomicInteger();
    private final long delayMs;
    private final String[] tokens;

    CountingSupplier(String... tokens) {
      this(0, tokens);
    }

    CountingSupplier(long delayMs, String... tokens) {
      this.delayMs = delayMs;
      this.tokens = tokens;
    }

    @Override
    public String getToken() throws Exception {
      int n = calls.getAndIncrement();
      if (delayMs > 0) {
        Thread.sleep(delayMs);
      }
      return tokens[Math.min(n, tokens.length - 1)];
    }
  }

  private static MockTokenServer.Reply[] errors(int status) {
    MockTokenServer.Reply[] script = new MockTokenServer.Reply[20];
    for (int i = 0; i < script.length; i++) {
      script[i] = MockTokenServer.Reply.error(status, "{\"error\":\"federation policy mismatch\"}");
    }
    return script;
  }

  private static int refusedPort() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  /** Creates a JSON stream, which must fail, and returns the failure's cause. */
  private Throwable createJson(FederatedToken token, String table) {
    return causeOf(
        sdk.streamBuilder().table(table).federatedAuth(token).recovery(false).json().build());
  }

  /** Creates a JSON stream that gets past authentication and fails only to connect. */
  private void failToConnect(FederatedToken token, String table) {
    assertConnectFailure(createJson(token, table));
  }

  private Throwable recreate(ZerobusJsonStream closed) {
    return causeOf(sdk.recreateStream(closed));
  }

  private static Throwable causeOf(CompletableFuture<?> future) {
    ExecutionException ex =
        assertThrows(ExecutionException.class, () -> future.get(60, TimeUnit.SECONDS));
    assertNotNull(ex.getCause());
    return ex.getCause();
  }

  private static void assertConnectFailure(Throwable cause) {
    assertTrue(
        String.valueOf(cause.getMessage()).contains("connect"),
        "expected a gRPC connect failure after authentication, got: " + cause);
  }

  private static void assertRetryable(Throwable cause, String detail) {
    assertTrue(cause instanceof ZerobusException, "unexpected error: " + cause);
    assertFalse(cause instanceof NonRetriableException, "expected retryable, got: " + cause);
    if (detail != null) {
      assertTrue(String.valueOf(cause.getMessage()).contains(detail), "unexpected error: " + cause);
    }
  }

  private static void assertPermanent(Throwable cause, String detail) {
    assertTrue(cause instanceof NonRetriableException, "expected non-retryable, got: " + cause);
    assertTrue(String.valueOf(cause.getMessage()).contains(detail), "unexpected error: " + cause);
  }

  private MockTokenServer.Request onlyRequest() {
    List<MockTokenServer.Request> reqs = tokens.requests();
    assertEquals(1, reqs.size(), "exchanges");
    return reqs.get(0);
  }

  private static void assertExchangeRequest(MockTokenServer.Request r, String jwt) {
    assertEquals("POST", r.method);
    assertEquals("/oidc/v1/token", r.path);
    assertTrue(
        String.valueOf(r.header("Content-Type")).startsWith("application/x-www-form-urlencoded"));
    assertNull(r.header("Authorization"), "the exchange must not carry client credentials");
    assertSameToken(
        jwt, r.formValue("subject_token"), "subject_token must be the supplier's token");
    assertEquals("urn:ietf:params:oauth:grant-type:token-exchange", r.formValue("grant_type"));
    assertEquals("urn:ietf:params:oauth:token-type:jwt", r.formValue("subject_token_type"));
  }

  /** Compares tokens without dumping ~1.5 KB of random JWT into the failure message. */
  private static void assertSameToken(String expected, String actual, String what) {
    assertTrue(
        expected.equals(actual),
        what
            + ": token differs (expected length "
            + expected.length()
            + ", got "
            + (actual == null ? "null" : "length " + actual.length())
            + ")");
  }

  private static void waitFor(java.util.function.BooleanSupplier cond) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!cond.getAsBoolean()) {
      assertTrue(System.nanoTime() < deadline, "timed out");
      Thread.sleep(20);
    }
  }
}
