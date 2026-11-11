package com.databricks.zerobus;

import java.util.Objects;

/**
 * Fluent builder for {@link ZerobusSdk}.
 *
 * <p>Obtain a builder with {@link ZerobusSdk#builder()}. Use it to set TLS options with {@link
 * #tlsOptions(TlsOptions)} or to change any other setting that the {@link ZerobusSdk} constructors
 * accept. The constructors remain available and create an SDK with the same defaults.
 *
 * <pre>{@code
 * try (ZerobusSdk sdk = ZerobusSdk.builder()
 *         .serverEndpoint(serverEndpoint)
 *         .unityCatalogEndpoint(workspaceUrl)
 *         .applicationName("my-app/1.0")
 *         .tlsOptions(TlsOptions.builder()
 *                 .addCaCertificateFile("/etc/zerobus/ca.pem")
 *                 .build())
 *         .build()) {
 *     // Create streams with sdk.streamBuilder()
 * }
 * }</pre>
 *
 * <h3>Thread Safety</h3>
 *
 * <p>A {@code ZerobusSdkBuilder} is not thread-safe and is intended to be configured and built from
 * a single thread.
 *
 * @see ZerobusSdk#builder()
 * @see TlsOptions
 */
public final class ZerobusSdkBuilder {

  private String serverEndpoint;
  private String unityCatalogEndpoint;
  private String applicationName;
  private boolean connectionPerStream = true;
  private TlsOptions tlsOptions;

  /** Package-private constructor. Use {@link ZerobusSdk#builder()} to create instances. */
  ZerobusSdkBuilder() {}

  /**
   * Sets the gRPC endpoint URL of the Zerobus service. Required.
   *
   * <p>Find cloud-specific endpoint formats in the <a
   * href="https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id">connection
   * URL table</a>.
   *
   * @param serverEndpoint the Zerobus endpoint URL
   * @return this builder for method chaining
   * @throws NullPointerException if {@code serverEndpoint} is null
   * @throws IllegalArgumentException if {@code serverEndpoint} is blank
   */
  public ZerobusSdkBuilder serverEndpoint(String serverEndpoint) {
    this.serverEndpoint = requireNonBlank(serverEndpoint, "serverEndpoint");
    return this;
  }

  /**
   * Sets the Databricks workspace URL, used for Unity Catalog and OAuth. Required for OAuth
   * streams; streams that use a {@link HeadersProvider} may omit it.
   *
   * @param unityCatalogEndpoint the workspace URL
   * @return this builder for method chaining
   * @throws NullPointerException if {@code unityCatalogEndpoint} is null
   * @throws IllegalArgumentException if {@code unityCatalogEndpoint} is blank
   */
  public ZerobusSdkBuilder unityCatalogEndpoint(String unityCatalogEndpoint) {
    this.unityCatalogEndpoint = requireNonBlank(unityCatalogEndpoint, "unityCatalogEndpoint");
    return this;
  }

  /**
   * Sets an application identifier appended to the HTTP {@code user-agent} header, conventionally
   * {@code "<product>/<version>"} (for example {@code "my-app/1.0"}). Pass {@code null}, or a value
   * that is blank after trimming, to omit it. Leading and trailing whitespace is removed.
   *
   * @param applicationName the application identifier, or {@code null}
   * @return this builder for method chaining
   * @throws IllegalArgumentException if {@code applicationName} contains a control character other
   *     than tab, or the DEL character. Such a value is not a valid HTTP header value.
   */
  public ZerobusSdkBuilder applicationName(String applicationName) {
    if (applicationName == null) {
      this.applicationName = null;
      return this;
    }
    String trimmed = applicationName.trim();
    if (trimmed.isEmpty()) {
      this.applicationName = null;
      return this;
    }
    for (int i = 0; i < trimmed.length(); i++) {
      char c = trimmed.charAt(i);
      if (c != '\t' && (c < 0x20 || c == 0x7f)) {
        throw new IllegalArgumentException(
            "applicationName must not contain control characters other than tab");
      }
    }
    this.applicationName = trimmed;
    return this;
  }

  /**
   * Sets whether every JSON and protobuf stream gets a dedicated gRPC connection. Defaults to
   * {@code true}. Pass {@code false} to share one connection across streams.
   *
   * @param connectionPerStream true for a dedicated connection per stream
   * @return this builder for method chaining
   */
  public ZerobusSdkBuilder connectionPerStream(boolean connectionPerStream) {
    this.connectionPerStream = connectionPerStream;
    return this;
  }

  /**
   * Sets the TLS options for the connection. When not set, the SDK trusts the operating system's CA
   * certificates and presents no client certificate.
   *
   * @param tlsOptions the TLS options
   * @return this builder for method chaining
   * @throws NullPointerException if {@code tlsOptions} is null
   */
  public ZerobusSdkBuilder tlsOptions(TlsOptions tlsOptions) {
    this.tlsOptions = Objects.requireNonNull(tlsOptions, "tlsOptions");
    return this;
  }

  /**
   * Creates the SDK. Close it with {@link ZerobusSdk#close()} or try-with-resources.
   *
   * @return a new SDK instance
   * @throws IllegalStateException if the server endpoint has not been set
   */
  public ZerobusSdk build() {
    if (serverEndpoint == null) {
      throw new IllegalStateException("serverEndpoint is required: call serverEndpoint()");
    }
    String workspaceUrl = unityCatalogEndpoint == null ? "" : unityCatalogEndpoint;
    return new ZerobusSdk(
        serverEndpoint, workspaceUrl, applicationName, connectionPerStream, tlsOptions);
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.trim().isEmpty()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
