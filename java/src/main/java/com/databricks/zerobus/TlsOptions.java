package com.databricks.zerobus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * TLS settings for the connection to the Zerobus endpoint. By default the SDK trusts the operating
 * system's CA certificates. Use {@link #builder()} to trust additional or only supplied CA
 * certificates, present a client certificate for mutual TLS, or check the server certificate
 * against a different domain name. The TLS Configuration section of the README describes how
 * certificate files are read and rotated.
 *
 * <p>These settings apply to the gRPC connection to the Zerobus endpoint only. OAuth and federated
 * token requests to the workspace URL trust the operating system's CA certificates and the SDK's
 * bundled public roots, ignore TlsOptions, and present no client certificate.
 *
 * <pre>{@code
 * TlsOptions tls = TlsOptions.builder()
 *     .trustSystemRoots(false)
 *     .addCaCertificateFile("/etc/zerobus/ca.pem")
 *     .clientIdentityFiles("/etc/zerobus/tls.crt", "/etc/zerobus/tls.key")
 *     .build();
 * }</pre>
 *
 * <p>Instances are immutable and safe to share between threads. {@link #toString()} never includes
 * certificate or key material.
 *
 * @see ZerobusSdkBuilder#tlsOptions(TlsOptions)
 */
public final class TlsOptions {

  // Brackets are URL syntax for IPv6. A single colon is a port; several colons are an IPv6 literal.
  private static final Pattern PASTED_URL_MISTAKE =
      Pattern.compile("[/?#\\[\\]\\p{Z}\\p{Cc}]|^[^:]*:[^:]*$");

  private final boolean trustSystemRoots;
  private final List<String> caCertificatePems;
  private final List<String> caCertificateFiles;
  private final String clientCertificatePem;
  private final String clientKeyPem;
  private final String clientCertificateFile;
  private final String clientKeyFile;
  private final String domainName;

  private TlsOptions(Builder builder) {
    this.trustSystemRoots = builder.trustSystemRoots;
    this.caCertificatePems = Collections.unmodifiableList(new ArrayList<>(builder.caPems));
    this.caCertificateFiles = Collections.unmodifiableList(new ArrayList<>(builder.caFiles));
    this.clientCertificatePem = builder.clientCertificatePem;
    this.clientKeyPem = builder.clientKeyPem;
    this.clientCertificateFile = builder.clientCertificateFile;
    this.clientKeyFile = builder.clientKeyFile;
    this.domainName = builder.domainName;
  }

  /**
   * Returns a new builder with the default settings: system CA certificates are trusted and no
   * client identity or domain name override is set.
   *
   * @return a new TLS options builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns whether the operating system's CA certificates are trusted.
   *
   * @return true if system roots are trusted, false if only the added CA certificates are trusted
   */
  boolean isTrustSystemRoots() {
    return trustSystemRoots;
  }

  /**
   * Returns the CA certificates supplied as PEM text, in the order they were added.
   *
   * @return an unmodifiable list of PEM strings
   */
  List<String> getCaCertificatePems() {
    return caCertificatePems;
  }

  /**
   * Returns the CA certificate files, in the order they were added.
   *
   * @return an unmodifiable list of file paths
   */
  List<String> getCaCertificateFiles() {
    return caCertificateFiles;
  }

  String getClientCertificatePem() {
    return clientCertificatePem;
  }

  String getClientKeyPem() {
    return clientKeyPem;
  }

  String getClientCertificateFile() {
    return clientCertificateFile;
  }

  String getClientKeyFile() {
    return clientKeyFile;
  }

  String getDomainName() {
    return domainName;
  }

  /**
   * Returns a summary of these options. Certificate and key material is never included.
   *
   * @return a human-readable summary
   */
  @Override
  public String toString() {
    return "TlsOptions{trustSystemRoots="
        + trustSystemRoots
        + ", caCertificatePems="
        + caCertificatePems.size()
        + ", caCertificateFiles="
        + caCertificateFiles.size()
        + ", clientIdentity="
        + clientIdentityKind()
        + ", domainName="
        + domainName
        + "}";
  }

  private String clientIdentityKind() {
    if (clientCertificatePem != null) {
      return "pem";
    }
    if (clientCertificateFile != null) {
      return "files";
    }
    return "none";
  }

  /**
   * Builder for {@link TlsOptions}.
   *
   * <p>Setters validate their arguments immediately and replace earlier values where a setting
   * holds only one value. A builder is not thread-safe and is intended to be configured from a
   * single thread.
   */
  public static final class Builder {

    private boolean trustSystemRoots = true;
    private final List<String> caPems = new ArrayList<>();
    private final List<String> caFiles = new ArrayList<>();
    private String clientCertificatePem;
    private String clientKeyPem;
    private String clientCertificateFile;
    private String clientKeyFile;
    private String domainName;

    private Builder() {}

    /**
     * Sets whether the operating system's CA certificates are trusted. Defaults to true. When
     * false, the server certificate must chain to a CA certificate added with {@link
     * #addCaCertificatePem(String)} or {@link #addCaCertificateFile(String)}.
     *
     * @param trustSystemRoots true to also trust the operating system's CA certificates
     * @return this builder for method chaining
     */
    public Builder trustSystemRoots(boolean trustSystemRoots) {
      this.trustSystemRoots = trustSystemRoots;
      return this;
    }

    /**
     * Trusts one more CA certificate, given as PEM text. Call this method more than once to trust
     * several CA certificates.
     *
     * @param caCertificatePem the CA certificate in PEM format
     * @return this builder for method chaining
     * @throws NullPointerException if {@code caCertificatePem} is null
     * @throws IllegalArgumentException if {@code caCertificatePem} is blank
     */
    public Builder addCaCertificatePem(String caCertificatePem) {
      caPems.add(ArgumentChecks.requireNonBlank(caCertificatePem, "caCertificatePem"));
      return this;
    }

    /**
     * Trusts the CA certificates in a PEM file. The file is read each time the SDK opens a
     * connection. Call this method more than once to trust certificates from several files.
     *
     * @param caCertificateFile the path to a PEM file with one or more CA certificates
     * @return this builder for method chaining
     * @throws NullPointerException if {@code caCertificateFile} is null
     * @throws IllegalArgumentException if {@code caCertificateFile} is blank
     */
    public Builder addCaCertificateFile(String caCertificateFile) {
      caFiles.add(ArgumentChecks.requireNonBlank(caCertificateFile, "caCertificateFile"));
      return this;
    }

    /**
     * Sets the client certificate chain and private key for mutual TLS, given as PEM text. Replaces
     * any client identity set earlier.
     *
     * @param certificatePem the client certificate chain in PEM format
     * @param keyPem the client private key in PEM format
     * @return this builder for method chaining
     * @throws NullPointerException if either argument is null
     * @throws IllegalArgumentException if either argument is blank
     */
    public Builder clientIdentityPem(String certificatePem, String keyPem) {
      String cert = ArgumentChecks.requireNonBlank(certificatePem, "certificatePem");
      String key = ArgumentChecks.requireNonBlank(keyPem, "keyPem");
      this.clientCertificatePem = cert;
      this.clientKeyPem = key;
      this.clientCertificateFile = null;
      this.clientKeyFile = null;
      return this;
    }

    /**
     * Sets the client certificate chain and private key for mutual TLS, read from two PEM files.
     * The files are read each time the SDK opens a connection. Replaces any client identity set
     * earlier.
     *
     * @param certificateFile the path to the client certificate chain in PEM format
     * @param keyFile the path to the client private key in PEM format
     * @return this builder for method chaining
     * @throws NullPointerException if either argument is null
     * @throws IllegalArgumentException if either argument is blank
     */
    public Builder clientIdentityFiles(String certificateFile, String keyFile) {
      String cert = ArgumentChecks.requireNonBlank(certificateFile, "certificateFile");
      String key = ArgumentChecks.requireNonBlank(keyFile, "keyFile");
      this.clientCertificateFile = cert;
      this.clientKeyFile = key;
      this.clientCertificatePem = null;
      this.clientKeyPem = null;
      return this;
    }

    /**
     * Sets the domain name the server certificate is checked against. By default the host name of
     * the endpoint is used. Pass a host name or IP address, for example {@code
     * "zerobus.example.com"}, not a URL: a value with a scheme, a port, a path or query, brackets,
     * whitespace, or a control character is rejected. Other malformed names can pass this check and
     * fail when the SDK sets up TLS for the connection; whether recovery retries that failure
     * depends on the rest of the TLS configuration.
     *
     * @param domainName the expected server certificate name
     * @return this builder for method chaining
     * @throws NullPointerException if {@code domainName} is null
     * @throws IllegalArgumentException if {@code domainName} is blank or looks like a URL
     */
    public Builder domainName(String domainName) {
      ArgumentChecks.requireNonBlank(domainName, "domainName");
      if (PASTED_URL_MISTAKE.matcher(domainName).find()) {
        throw new IllegalArgumentException(
            "domainName must be a host name or IP address with no scheme, port, path, brackets or"
                + " whitespace, for example \"zerobus.example.com\"");
      }
      this.domainName = domainName;
      return this;
    }

    /**
     * Builds the TLS options.
     *
     * @return the immutable TLS options
     * @throws IllegalStateException if system roots are not trusted and no CA certificate was added
     */
    public TlsOptions build() {
      if (!trustSystemRoots && caPems.isEmpty() && caFiles.isEmpty()) {
        throw new IllegalStateException(
            "at least one CA certificate is required when system roots are not trusted: call"
                + " addCaCertificatePem() or addCaCertificateFile()");
      }
      return new TlsOptions(this);
    }
  }
}
