package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ZerobusSdkBuilderTest {

  private static final String SERVER_ENDPOINT =
      "https://1234567890.zerobus.us-west-2.cloud.databricks.com";
  private static final String UNITY_CATALOG_ENDPOINT =
      "https://test-workspace.cloud.databricks.com";

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
  void applicationNameRejectsControlCharactersBeforeTheNativeCall() {
    ZerobusSdkBuilder builder = new ZerobusSdkBuilder();

    assertThrows(IllegalArgumentException.class, () -> builder.applicationName("bad\nname"));
    assertThrows(IllegalArgumentException.class, () -> builder.applicationName("bad\u0007name"));
    assertThrows(IllegalArgumentException.class, () -> builder.applicationName("bad\u007fname"));
    // A tab is allowed, and a blank value counts as unset.
    builder.applicationName("my-app/1.0\t");
    builder.applicationName("   ");
    builder.applicationName(null);
  }

  @Test
  void rejectsNullTlsOptions() {
    ZerobusSdkBuilder builder = new ZerobusSdkBuilder();

    assertThrows(NullPointerException.class, () -> builder.tlsOptions(null));
  }

  @Test
  void sdkBuilderIsReachableFromTheSdkClass() {
    // Checked by reflection so the test does not load the native library.
    try {
      Method builder = ZerobusSdk.class.getMethod("builder");
      assertTrue(Modifier.isStatic(builder.getModifiers()));
      assertEquals(ZerobusSdkBuilder.class, builder.getReturnType());
    } catch (NoSuchMethodException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  void preservesNativeCreationSignatures() throws Exception {
    assertNotNull(
        ZerobusSdk.class.getDeclaredMethod(
            "nativeCreateWithOptions",
            String.class,
            String.class,
            String.class,
            boolean.class,
            TlsOptions.class));
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
    try (ZerobusSdk sdk =
        ZerobusSdk.builder().serverEndpoint(SERVER_ENDPOINT).tlsOptions(tls).build()) {
      ExecutionException error =
          assertThrows(
              ExecutionException.class,
              () ->
                  sdk.streamBuilder()
                      .table("catalog.schema.table")
                      .headersProvider(() -> Collections.singletonMap("authorization", "Bearer t"))
                      .json()
                      .build()
                      .get(60, TimeUnit.SECONDS));
      assertTrue(
          error.getCause() instanceof NonRetriableException, String.valueOf(error.getCause()));
      assertTrue(error.getCause().getMessage().contains(missing), error.getCause().getMessage());
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
    try (ZerobusSdk sdk =
        ZerobusSdk.builder().serverEndpoint(SERVER_ENDPOINT).tlsOptions(tls).build()) {
      ExecutionException error =
          assertThrows(
              ExecutionException.class,
              () ->
                  sdk.streamBuilder()
                      .table("catalog.schema.table")
                      .headersProvider(() -> Collections.singletonMap("authorization", "Bearer t"))
                      .json()
                      .build()
                      .get(60, TimeUnit.SECONDS));
      assertTrue(
          error.getCause() instanceof NonRetriableException, String.valueOf(error.getCause()));
      assertTrue(error.getCause().getMessage().contains(missing), error.getCause().getMessage());
    }
  }

  @Test
  void invalidClientIdentityPemFailsStreamCreationWithoutRetry() {
    assumeNativeLibrary();
    // The PEM values reach the core, which rejects them when it opens the channel. A PEM
    // value that the core cannot use fails the same way on every attempt, so it is not retryable.
    TlsOptions tls = TlsOptions.builder().clientIdentityPem("not a cert", "not a key").build();
    try (ZerobusSdk sdk =
        ZerobusSdk.builder().serverEndpoint(SERVER_ENDPOINT).tlsOptions(tls).build()) {
      ExecutionException error = assertThrows(ExecutionException.class, () -> createStream(sdk));
      assertTrue(
          error.getCause() instanceof NonRetriableException, String.valueOf(error.getCause()));
      assertTrue(
          error.getCause().getMessage().contains("private key"), error.getCause().getMessage());
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
