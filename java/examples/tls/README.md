# TLS Examples

This directory shows how to configure TLS with `TlsOptions` and `ZerobusSdk.builder()`.

## Building and Running

Find the endpoint values in the [connection URL table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id).

```bash
cd examples
SDK_JAR=$(ls ../target/zerobus-ingest-sdk-*-jar-with-dependencies.jar | head -n 1)

# Compile
javac -d . -cp "$SDK_JAR" tls/TlsConfigurationExample.java

# Set environment variables
export ZEROBUS_SERVER_ENDPOINT="<your-zerobus-server-endpoint>"
export DATABRICKS_WORKSPACE_URL="<your-databricks-workspace-url>"
export ZEROBUS_TABLE_NAME="catalog.schema.table"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"

# Optional: trust only a private CA and present a client certificate
export ZEROBUS_CA_CERT_FILE="/etc/zerobus/ca.pem"
export ZEROBUS_CLIENT_CERT_FILE="/etc/zerobus/tls.crt"
export ZEROBUS_CLIENT_KEY_FILE="/etc/zerobus/tls.key"

java -cp ".:$SDK_JAR" com.databricks.zerobus.examples.tls.TlsConfigurationExample
```

Leave the `ZEROBUS_CA_CERT_FILE`, `ZEROBUS_CLIENT_CERT_FILE`, and `ZEROBUS_CLIENT_KEY_FILE`
variables unset to use the operating system's CA certificates.

## API Overview

```java
TlsOptions tls = TlsOptions.builder()
    .trustSystemRoots(false)
    .addCaCertificateFile("/etc/zerobus/ca.pem")
    .clientIdentityFiles("/etc/zerobus/tls.crt", "/etc/zerobus/tls.key")
    .build();

try (ZerobusSdk sdk = ZerobusSdk.builder()
        .serverEndpoint(serverEndpoint)
        .unityCatalogEndpoint(workspaceUrl)
        .tlsOptions(tls)
        .build()) {
    // Create streams with sdk.streamBuilder()
}
```

Certificate files are read each time the SDK opens a connection, so rotated files take effect
without a restart. See the [TLS Configuration](../../README.md#tls-configuration) section of the
SDK README.
