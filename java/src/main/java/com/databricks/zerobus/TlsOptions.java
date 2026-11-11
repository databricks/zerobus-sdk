package com.databricks.zerobus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * TLS settings for the connection to the Zerobus endpoint.
 *
 * <p>By default the SDK trusts the operating system's CA certificates. Use {@link #builder()} to
 * trust additional CA certificates, to trust only the CA certificates you supply, to present a
 * client certificate for mutual TLS, or to check the server certificate against a different domain
 * name. Pass the result to {@link ZerobusSdkBuilder#tlsOptions(TlsOptions)}.
 *
 * <p>Certificate and key material can be given as PEM text or as paths to PEM files. When you pass
 * paths, the SDK reads the files each time it opens a connection, so replaced files take effect
 * without a restart. Use absolute paths, because a relative path is resolved when the connection is
 * opened. Replace a client certificate and its key together, for example by switching a symbolic
 * link to a directory that holds both new files.
 *
 * <p>Instances are immutable and safe to share between threads. {@link #toString()} never includes
 * certificate or key material.
 *
 * <pre>{@code
 * TlsOptions tls = TlsOptions.builder()
 *     .trustSystemRoots(false)
 *     .addCaCertificateFile("/etc/zerobus/ca.pem")
 *     .clientIdentityFiles("/etc/zerobus/tls.crt", "/etc/zerobus/tls.key")
 *     .build();
 * }</pre>
 *
 * @see ZerobusSdkBuilder#tlsOptions(TlsOptions)
 */
public final class TlsOptions {

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
     * @throws IllegalArgumentException if {@code caCertificatePem} is null or blank
     */
    public Builder addCaCertificatePem(String caCertificatePem) {
      caPems.add(requireNonBlank(caCertificatePem, "caCertificatePem"));
      return this;
    }

    /**
     * Trusts the CA certificates in a PEM file. The file is read each time the SDK opens a
     * connection. Call this method more than once to trust certificates from several files.
     *
     * @param caCertificateFile the path to a PEM file with one or more CA certificates
     * @return this builder for method chaining
     * @throws IllegalArgumentException if {@code caCertificateFile} is null or blank
     */
    public Builder addCaCertificateFile(String caCertificateFile) {
      caFiles.add(requireNonBlank(caCertificateFile, "caCertificateFile"));
      return this;
    }

    /**
     * Sets the client certificate chain and private key for mutual TLS, given as PEM text. Replaces
     * any client identity set earlier.
     *
     * @param certificatePem the client certificate chain in PEM format
     * @param keyPem the client private key in PEM format
     * @return this builder for method chaining
     * @throws IllegalArgumentException if either argument is null or blank
     */
    public Builder clientIdentityPem(String certificatePem, String keyPem) {
      String cert = requireNonBlank(certificatePem, "certificatePem");
      String key = requireNonBlank(keyPem, "keyPem");
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
     * @throws IllegalArgumentException if either argument is null or blank
     */
    public Builder clientIdentityFiles(String certificateFile, String keyFile) {
      String cert = requireNonBlank(certificateFile, "certificateFile");
      String key = requireNonBlank(keyFile, "keyFile");
      this.clientCertificateFile = cert;
      this.clientKeyFile = key;
      this.clientCertificatePem = null;
      this.clientKeyPem = null;
      return this;
    }

    /**
     * Sets the domain name the server certificate is checked against. By default the host name of
     * the server endpoint is used.
     *
     * @param domainName the expected server certificate name
     * @return this builder for method chaining
     * @throws IllegalArgumentException if {@code domainName} is null or blank
     */
    public Builder domainName(String domainName) {
      this.domainName = requireNonBlank(domainName, "domainName");
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

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.trim().isEmpty()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
