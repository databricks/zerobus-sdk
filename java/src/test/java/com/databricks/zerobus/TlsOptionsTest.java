package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class TlsOptionsTest {

  @Test
  void defaultsTrustSystemRootsWithoutIdentity() {
    TlsOptions options = TlsOptions.builder().build();

    assertTrue(options.isTrustSystemRoots());
    assertTrue(options.getCaCertificatePems().isEmpty());
    assertTrue(options.getCaCertificateFiles().isEmpty());
    assertNull(options.getClientCertificatePem());
    assertNull(options.getClientCertificateFile());
    assertNull(options.getDomainName());
    assertTrue(options.toString().contains("clientIdentity=none"), options.toString());
  }

  @Test
  void withoutSystemRootsRequiresAtLeastOneCaCertificate() {
    TlsOptions.Builder builder = TlsOptions.builder().trustSystemRoots(false);

    IllegalStateException error = assertThrows(IllegalStateException.class, builder::build);
    assertTrue(error.getMessage().contains("addCaCertificatePem"), error.getMessage());
  }

  @Test
  void acceptsCaCertificatesFromPemOrFiles() {
    TlsOptions pemOnly =
        TlsOptions.builder().trustSystemRoots(false).addCaCertificatePem("ca-pem").build();
    TlsOptions fileOnly =
        TlsOptions.builder().trustSystemRoots(false).addCaCertificateFile("/etc/ca.pem").build();

    assertEquals(Collections.singletonList("ca-pem"), pemOnly.getCaCertificatePems());
    assertEquals(Collections.singletonList("/etc/ca.pem"), fileOnly.getCaCertificateFiles());
  }

  @Test
  void keepsCaCertificatesInTheOrderAdded() {
    TlsOptions options =
        TlsOptions.builder()
            .addCaCertificatePem("first")
            .addCaCertificatePem("second")
            .addCaCertificateFile("/a.pem")
            .addCaCertificateFile("/b.pem")
            .build();

    assertEquals(Arrays.asList("first", "second"), options.getCaCertificatePems());
    assertEquals(Arrays.asList("/a.pem", "/b.pem"), options.getCaCertificateFiles());
  }

  @Test
  void clientIdentityPemReplacesFileIdentity() {
    TlsOptions options =
        TlsOptions.builder()
            .clientIdentityFiles("/etc/tls.crt", "/etc/tls.key")
            .clientIdentityPem("cert-pem", "key-pem")
            .build();

    assertEquals("cert-pem", options.getClientCertificatePem());
    assertEquals("key-pem", options.getClientKeyPem());
    assertNull(options.getClientCertificateFile());
    assertNull(options.getClientKeyFile());
    assertTrue(options.toString().contains("clientIdentity=pem"), options.toString());
  }

  @Test
  void clientIdentityFilesReplacesPemIdentity() {
    TlsOptions options =
        TlsOptions.builder()
            .clientIdentityPem("cert-pem", "key-pem")
            .clientIdentityFiles("/etc/tls.crt", "/etc/tls.key")
            .build();

    assertNull(options.getClientCertificatePem());
    assertNull(options.getClientKeyPem());
    assertEquals("/etc/tls.crt", options.getClientCertificateFile());
    assertEquals("/etc/tls.key", options.getClientKeyFile());
    assertTrue(options.toString().contains("clientIdentity=files"), options.toString());
  }

  @Test
  void toStringNeverIncludesCertificateOrKeyMaterial() {
    TlsOptions options =
        TlsOptions.builder()
            .addCaCertificatePem("PEM-MARKER-CA")
            .clientIdentityPem("PEM-MARKER-CERT", "PEM-MARKER-KEY")
            .domainName("zerobus.example.com")
            .build();

    String text = options.toString();

    assertFalse(text.contains("PEM-MARKER"), text);
    assertTrue(text.contains("caCertificatePems=1"), text);
    assertTrue(text.contains("zerobus.example.com"), text);
  }

  @Test
  void builtOptionsAreNotChangedByLaterBuilderCalls() {
    TlsOptions.Builder builder = TlsOptions.builder().trustSystemRoots(false);
    TlsOptions first = builder.addCaCertificatePem("first").build();
    builder.addCaCertificatePem("second");

    assertEquals(Collections.singletonList("first"), first.getCaCertificatePems());
    assertThrows(UnsupportedOperationException.class, () -> first.getCaCertificatePems().add("x"));
  }

  @Test
  void rejectsNullAndBlankValues() {
    TlsOptions.Builder builder = TlsOptions.builder();

    assertThrows(NullPointerException.class, () -> builder.addCaCertificatePem(null));
    assertThrows(IllegalArgumentException.class, () -> builder.addCaCertificatePem("  "));
    assertThrows(IllegalArgumentException.class, () -> builder.addCaCertificateFile(""));
    assertThrows(NullPointerException.class, () -> builder.clientIdentityPem(null, "key"));
    assertThrows(IllegalArgumentException.class, () -> builder.clientIdentityPem("cert", " "));
    assertThrows(NullPointerException.class, () -> builder.clientIdentityFiles("cert", null));
    assertThrows(IllegalArgumentException.class, () -> builder.clientIdentityFiles("", "key"));
    assertThrows(IllegalArgumentException.class, () -> builder.domainName("\t"));
  }

  @Test
  void rejectedValuesLeaveTheBuilderUnchanged() {
    TlsOptions.Builder builder = TlsOptions.builder().clientIdentityPem("cert", "key");

    assertThrows(IllegalArgumentException.class, () -> builder.clientIdentityFiles("", "key"));

    TlsOptions options = builder.build();
    assertEquals("cert", options.getClientCertificatePem());
    assertNull(options.getClientCertificateFile());
  }
}
