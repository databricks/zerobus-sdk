package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class ZerobusSdkBuilderTest {

  private static final String SERVER_ENDPOINT =
      "https://1234567890.zerobus.us-west-2.cloud.databricks.com";
  private static final String UNITY_CATALOG_ENDPOINT =
      "https://test-workspace.cloud.databricks.com";
  // A self-signed certificate used only as a CA in these tests. It is not a credential.
  private static final String VALID_CA_PEM =
      "-----BEGIN CERTIFICATE-----\n"
          + "MIIBijCCATGgAwIBAgIULdA2zf8G+VK3Wf8QmXW27k+0+UwwCgYIKoZIzj0EAwIw\n"
          + "GjEYMBYGA1UEAwwPemVyb2J1cy10ZXN0LWNhMCAXDTI2MTEyNjEzNTAyM1oYDzIx\n"
          + "MjYxMTAyMTM1MDIzWjAaMRgwFgYDVQQDDA96ZXJvYnVzLXRlc3QtY2EwWTATBgcq\n"
          + "hkjOPQIBBggqhkjOPQMBBwNCAAT1vSiadorL5o9rLgjZxGxEFI1o4eLkzMYni+zF\n"
          + "L+pDl/F+n+64Eq+kllE3jFBSxE6UHqTdOG+e75yB6TPzl84co1MwUTAdBgNVHQ4E\n"
          + "FgQUz3f2YGQtCDbuXg9zd1Ree9hwLBUwHwYDVR0jBBgwFoAUz3f2YGQtCDbuXg9z\n"
          + "d1Ree9hwLBUwDwYDVR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNHADBEAiAiiQgi\n"
          + "KaRWuIn67vu3ut9cuNRcbjNkmLAXUztczRFZgQIgGVn7GjmspRr1Eq1aTUBr9iek\n"
          + "Lg6TvR5cTLtS9eP78CY=\n"
          + "-----END CERTIFICATE-----";
  private static final String NON_CERTIFICATE_PEM =
      "-----BEGIN PUBLIC KEY-----\nnot-a-real-key\n-----END PUBLIC KEY-----";
  // First byte of a TLS handshake record, which the client sends first.
  private static final int TLS_HANDSHAKE_RECORD = 0x16;

  private static void assumeNativeLibrary() {
    boolean available;
    try {
      NativeLoader.ensureLoaded();
      available = true;
    } catch (UnsatisfiedLinkError | ExceptionInInitializerError e) {
      available = false;
    }
    assumeTrue(available, "Native library required to construct ZerobusSdk");
  }

  @Test
  void buildWithoutServerEndpointFails() {
    ZerobusSdkBuilder builder = new ZerobusSdkBuilder();

    IllegalStateException error = assertThrows(IllegalStateException.class, builder::build);
    assertTrue(error.getMessage().contains("serverEndpoint"), error.getMessage());
  }

  @Test
  void rejectsBlankEndpoints() {
    ZerobusSdkBuilder builder = new ZerobusSdkBuilder();

    assertThrows(IllegalArgumentException.class, () -> builder.serverEndpoint(" "));
    assertThrows(IllegalArgumentException.class, () -> builder.unityCatalogEndpoint(""));
    assertThrows(NullPointerException.class, () -> builder.serverEndpoint(null));
  }

  @Test
  void applicationNameTrailingNewlineIsTrimmedOnBothEntryPoints() {
    assumeNativeLibrary();
    try (ZerobusSdk fromBuilder =
            ZerobusSdk.builder()
                .serverEndpoint(SERVER_ENDPOINT)
                .applicationName("my-app\n")
                .build();
        ZerobusSdk fromConstructor =
            new ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT, "my-app\n")) {
      assertNotNull(fromBuilder);
      assertNotNull(fromConstructor);
    }
  }

  @Test
  void applicationNameInteriorControlCharacterFailsTheSameWayOnBothEntryPoints() {
    assumeNativeLibrary();
    for (String name : new String[] {"my\napp", "bad\u0007name"}) {
      NonRetriableException fromBuilder =
          assertThrows(
              NonRetriableException.class,
              () ->
                  ZerobusSdk.builder()
                      .serverEndpoint(SERVER_ENDPOINT)
                      .applicationName(name)
                      .build());
      NonRetriableException fromConstructor =
          assertThrows(
              NonRetriableException.class,
              () -> new ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT, name));
      assertEquals(fromConstructor.getMessage(), fromBuilder.getMessage(), name);
    }
  }

  @Test
  void rejectsNullTlsOptions() {
    ZerobusSdkBuilder builder = new ZerobusSdkBuilder();

    assertThrows(NullPointerException.class, () -> builder.tlsOptions(null));
  }

  @Test
  void buildsSdkWithDefaultTls() {
    assumeNativeLibrary();
    try (ZerobusSdk sdk =
        ZerobusSdk.builder()
            .serverEndpoint(SERVER_ENDPOINT)
            .unityCatalogEndpoint(UNITY_CATALOG_ENDPOINT)
            .build()) {
      assertNotNull(sdk);
    }
  }

  @Test
  void buildsSdkWithoutUnityCatalogEndpointForHeadersProviderUse() {
    assumeNativeLibrary();
    try (ZerobusSdk sdk = ZerobusSdk.builder().serverEndpoint(SERVER_ENDPOINT).build()) {
      assertNotNull(sdk);
    }
  }

  @Test
  void buildsSdkWithAllOptions() {
    assumeNativeLibrary();
    TlsOptions tls =
        TlsOptions.builder()
            .trustSystemRoots(false)
            .addCaCertificatePem(
                "-----BEGIN CERTIFICATE-----\nnot-a-real-cert\n-----END CERTIFICATE-----")
            .clientIdentityPem("cert-pem", "key-pem")
            .domainName("zerobus.example.com")
            .build();
    try (ZerobusSdk sdk =
        ZerobusSdk.builder()
            .serverEndpoint(SERVER_ENDPOINT)
            .unityCatalogEndpoint(UNITY_CATALOG_ENDPOINT)
            .applicationName("my-app/1.0")
            .connectionPerStream(false)
            .tlsOptions(tls)
            .build()) {
      assertNotNull(sdk);
    }
  }

  @Test
  void buildDoesNotReadCertificateFiles() {
    assumeNativeLibrary();
    // Files are read when a channel opens, so a missing file does not fail SDK creation.
    TlsOptions tls =
        TlsOptions.builder()
            .trustSystemRoots(false)
            .addCaCertificateFile("/nonexistent/zerobus-test/ca.pem")
            .clientIdentityFiles(
                "/nonexistent/zerobus-test/tls.crt", "/nonexistent/zerobus-test/tls.key")
            .build();
    try (ZerobusSdk sdk =
        ZerobusSdk.builder().serverEndpoint(SERVER_ENDPOINT).tlsOptions(tls).build()) {
      assertNotNull(sdk);
    }
  }

  @Test
  void missingCaFileFailsStreamCreationWithItsPath() {
    assumeNativeLibrary();
    // The core reads the file when it opens the channel, so the failure comes from the native
    // side and names the path it was given. No server is contacted.
    String missing = "/nonexistent/zerobus-test/ca.pem";
    TlsOptions tls =
        TlsOptions.builder().trustSystemRoots(false).addCaCertificateFile(missing).build();
    String message = streamCreationFailure(tls).getMessage();
    assertTrue(message.contains(missing), message);
  }

  @Test
  void invalidBinaryHeaderValueIsRejectedByTheCoreDecoder() {
    assumeNativeLibrary();
    // The core decodes -bin values before it opens the RPC, so no server is contacted. The
    // diagnostic comes from the core's decoder, not from the JNI name check.
    ExecutionException error = createStreamWithBinaryHeader(SERVER_ENDPOINT, "not base64!");
    assertTrue(error.getCause() instanceof NonRetriableException, String.valueOf(error.getCause()));
    assertTrue(
        error.getCause().getMessage().contains("Invalid metadata for header 'x-context-bin'"),
        error.getCause().getMessage());
  }

  @Test
  void validBinaryHeaderValuePassesHeaderHandling() {
    assumeNativeLibrary();
    // Nothing is expected to succeed on port 1. The point is that the core's decoder does not
    // reject the value; that the JNI layer passes it through unchanged is covered by the native
    // callbacks test.
    ExecutionException error = createStreamWithBinaryHeader("https://127.0.0.1:1", "AAE=");
    String message = String.valueOf(error.getCause().getMessage());
    assertFalse(message.contains("Invalid metadata"), message);
  }

  private static ExecutionException createStreamWithBinaryHeader(
      String serverEndpoint, String value) {
    try (ZerobusSdk sdk = ZerobusSdk.builder().serverEndpoint(serverEndpoint).build()) {
      return assertThrows(
          ExecutionException.class,
          () ->
              sdk.streamBuilder()
                  .table("catalog.schema.table")
                  .headersProvider(
                      () -> {
                        Map<String, String> headers = new HashMap<>();
                        headers.put("authorization", "Bearer t");
                        headers.put("x-context-bin", value);
                        return headers;
                      })
                  .recovery(false)
                  .json()
                  .build()
                  .get(60, TimeUnit.SECONDS));
    }
  }

  @Test
  void missingClientIdentityFileFailsStreamCreationWithItsPath() {
    assumeNativeLibrary();
    String missing = "/nonexistent/zerobus-test/tls.crt";
    TlsOptions tls =
        TlsOptions.builder()
            .clientIdentityFiles(missing, "/nonexistent/zerobus-test/tls.key")
            .build();
    String message = streamCreationFailure(tls).getMessage();
    assertTrue(message.contains(missing), message);
  }

  @Test
  void invalidClientIdentityPemFailsStreamCreationWithoutRetry() {
    assumeNativeLibrary();
    // The PEM values reach the core, which rejects them when it opens the channel. A PEM
    // value that the core cannot use fails the same way on every attempt, so it is not retryable.
    TlsOptions tls = TlsOptions.builder().clientIdentityPem("not a cert", "not a key").build();
    String message = streamCreationFailure(tls).getMessage();
    assertTrue(message.contains("private key"), message);
  }

  @Test
  void trustSystemRootsFalseReachesTheCore() {
    assumeNativeLibrary();
    // With system roots off, a CA PEM that holds no certificate leaves the core trusting nothing.
    TlsOptions tls =
        TlsOptions.builder()
            .trustSystemRoots(false)
            .addCaCertificatePem(NON_CERTIFICATE_PEM)
            .build();
    String message = streamCreationFailure(tls).getMessage();
    assertTrue(message.contains("trust no CA certificates"), message);
  }

  @Test
  void caCertificatePemReachesTheCore() throws Exception {
    assumeNativeLibrary();
    // A valid CA is accepted, so the handshake starts. A dropped PEM would fail before connecting.
    TlsOptions tls =
        TlsOptions.builder().trustSystemRoots(false).addCaCertificatePem(VALID_CA_PEM).build();
    LocalProbe probe = probeLocalServer(tls);
    assertTrue(probe.connected, probe.failure);
    assertEquals(TLS_HANDSHAKE_RECORD, probe.firstByte);
  }

  @Test
  void domainNameReachesTheCoreBeforeTheHandshake() throws Exception {
    assumeNativeLibrary();
    LocalProbe control = probeLocalServer(TlsOptions.builder().build());
    assertTrue(control.connected, control.failure);
    assertEquals(TLS_HANDSHAKE_RECORD, control.firstByte);
    // The Java check accepts this name. The core rejects it before connecting, and its error does
    // not name the domain.
    LocalProbe probe =
        probeLocalServer(TlsOptions.builder().domainName("zerobus..example.com").build());
    assertFalse(probe.connected, "the client should not connect");
    assertEquals("Failed to establish TLS connection.", probe.failure);
  }

  /** What the local server observed during one stream creation attempt. */
  private static final class LocalProbe {
    private final boolean connected;
    private final int firstByte;
    private final String failure;

    private LocalProbe(boolean connected, int firstByte, String failure) {
      this.connected = connected;
      this.firstByte = firstByte;
      this.failure = failure;
    }
  }

  /**
   * Creates one stream against a local listener on 127.0.0.1 and reports whether a client
   * connected, the first byte it sent, and the stream's failure message. The listener is served
   * after the attempt, so a connection that was made is still in the accept backlog.
   */
  private static LocalProbe probeLocalServer(TlsOptions tls) throws Exception {
    assumeLoopbackBypassesProxy();
    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
      try (ZerobusSdk sdk =
          ZerobusSdk.builder()
              .serverEndpoint("https://127.0.0.1:" + server.getLocalPort())
              .tlsOptions(tls)
              .build()) {
        CompletableFuture<ZerobusJsonStream> stream =
            sdk.streamBuilder()
                .table("catalog.schema.table")
                .headersProvider(() -> Collections.singletonMap("authorization", "Bearer t"))
                .recovery(false)
                .json()
                .build();
        String failure = null;
        try {
          stream.get(2, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
          failure = String.valueOf(e.getCause().getMessage());
        } catch (TimeoutException e) {
          // The handshake is waiting for the server, so the client has connected.
        }
        Socket client = acceptOrNull(server, failure == null ? 10_000 : 1_000);
        int firstByte = -1;
        if (client != null) {
          try (Socket c = client) {
            c.setSoTimeout(5_000);
            firstByte = c.getInputStream().read();
          }
        }
        if (failure == null) {
          failure = failureOf(stream);
        }
        return new LocalProbe(client != null, firstByte, failure);
      }
    }
  }

  private static Socket acceptOrNull(ServerSocket server, int timeoutMillis) throws IOException {
    server.setSoTimeout(timeoutMillis);
    try {
      return server.accept();
    } catch (SocketTimeoutException e) {
      return null;
    }
  }

  private static String failureOf(CompletableFuture<ZerobusJsonStream> stream) throws Exception {
    try {
      stream.get(10, TimeUnit.SECONDS);
      return null;
    } catch (ExecutionException e) {
      return String.valueOf(e.getCause().getMessage());
    }
  }

  // The core sends traffic through a proxy named by these variables unless the no-proxy list
  // covers the host. The local-socket tests dial 127.0.0.1, so they need that host bypassed.
  private static void assumeLoopbackBypassesProxy() {
    boolean proxied = false;
    for (String name :
        new String[] {
          "grpc_proxy", "GRPC_PROXY", "https_proxy", "HTTPS_PROXY", "http_proxy", "HTTP_PROXY"
        }) {
      proxied |= nonEmpty(System.getenv(name));
    }
    if (!proxied) {
      return;
    }
    String noProxy = null;
    for (String name : new String[] {"no_grpc_proxy", "NO_GRPC_PROXY", "no_proxy", "NO_PROXY"}) {
      if (nonEmpty(System.getenv(name))) {
        noProxy = System.getenv(name);
        break;
      }
    }
    boolean bypassed = false;
    if (noProxy != null) {
      bypassed = noProxy.trim().equals("*");
      for (String entry : noProxy.split(",")) {
        bypassed |= entry.trim().replaceFirst("^\\.+", "").equals("127.0.0.1");
      }
    }
    assumeTrue(bypassed, "a proxy variable is set and 127.0.0.1 is not in no_proxy");
  }

  private static boolean nonEmpty(String value) {
    return value != null && !value.isEmpty();
  }

  private static NonRetriableException streamCreationFailure(TlsOptions tls) {
    try (ZerobusSdk sdk =
        ZerobusSdk.builder().serverEndpoint(SERVER_ENDPOINT).tlsOptions(tls).build()) {
      ExecutionException error = assertThrows(ExecutionException.class, () -> createStream(sdk));
      assertTrue(
          error.getCause() instanceof NonRetriableException, String.valueOf(error.getCause()));
      return (NonRetriableException) error.getCause();
    }
  }

  private static ZerobusJsonStream createStream(ZerobusSdk sdk) throws Exception {
    return sdk.streamBuilder()
        .table("catalog.schema.table")
        .headersProvider(() -> Collections.singletonMap("authorization", "Bearer t"))
        .json()
        .build()
        .get(60, TimeUnit.SECONDS);
  }
}
