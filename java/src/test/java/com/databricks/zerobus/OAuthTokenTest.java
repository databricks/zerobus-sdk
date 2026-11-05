package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OAuthTokenTest {
  private HttpServer server;
  private volatile int status;
  private volatile String body;

  @BeforeEach
  void startServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/oidc/v1/token",
        exchange -> {
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(status, bytes.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
          }
        });
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  private OAuthToken getToken(String tableName) throws ZerobusException {
    String url = "http://127.0.0.1:" + server.getAddress().getPort();
    return ZerobusSdk.getToken(url, tableName, "id", "secret", "ws");
  }

  @Test
  void returnsTokenAndExpiry() throws Exception {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    status = 200;
    body = "{\"access_token\":\"secret-token-value\",\"expires_in\":3600}";

    Instant before = Instant.now();
    OAuthToken token = getToken("c.s.t");
    Instant after = Instant.now();

    assertEquals("secret-token-value", token.getAccessToken());
    Instant expiresAt = token.getExpiresAt().get();
    Duration ttl = Duration.ofHours(1);
    // Millisecond truncation can put expiresAt just under before + ttl.
    assertFalse(expiresAt.isBefore(before.plus(ttl).minusMillis(1)));
    assertFalse(expiresAt.isAfter(after.plus(ttl)));
    assertFalse(token.toString().contains("secret-token-value"));
  }

  @Test
  void missingExpiresInYieldsNoExpiry() throws Exception {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    status = 200;
    body = "{\"access_token\":\"tok\"}";

    OAuthToken token = getToken("c.s.t");

    assertEquals("tok", token.getAccessToken());
    assertFalse(token.getExpiresAt().isPresent());
  }

  @Test
  void rejectedRequestIsNonRetriable() {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    status = 401;
    body = "{\"error\":\"invalid_client\"}";

    NonRetriableException e = assertThrows(NonRetriableException.class, () -> getToken("c.s.t"));
    assertTrue(e.getMessage().contains("401"), e.getMessage());
  }

  @Test
  void serverErrorIsRetryable() {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    status = 503;
    body = "unavailable";

    ZerobusException e = assertThrows(ZerobusException.class, () -> getToken("c.s.t"));
    assertFalse(e instanceof NonRetriableException);
  }

  @Test
  void invalidTableNameIsNonRetriable() {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");

    assertThrows(NonRetriableException.class, () -> getToken("not-a-table"));
  }

  @Test
  void nullArgumentIsNonRetriable() {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");

    assertThrows(
        NonRetriableException.class,
        () -> ZerobusSdk.getToken(null, "c.s.t", "id", "secret", "ws"));
  }
}
