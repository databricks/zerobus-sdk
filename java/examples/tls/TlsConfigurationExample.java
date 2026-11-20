package com.databricks.zerobus.examples.tls;

import com.databricks.zerobus.TlsOptions;
import com.databricks.zerobus.ZerobusJsonStream;
import com.databricks.zerobus.ZerobusSdk;

/**
 * Connects with custom TLS settings and ingests JSON records.
 *
 * <p>When {@code ZEROBUS_CA_CERT_FILE} is set, only that CA certificate is trusted. When
 * {@code ZEROBUS_CLIENT_CERT_FILE} and {@code ZEROBUS_CLIENT_KEY_FILE} are set, the client presents
 * that identity for mutual TLS. Without them the SDK uses the operating system's CA certificates.
 *
 * <p>Run with: {@code java -cp <classpath> com.databricks.zerobus.examples.tls.TlsConfigurationExample}
 */
public class TlsConfigurationExample {

    public static void main(String[] args) throws Exception {
        String serverEndpoint = System.getenv("ZEROBUS_SERVER_ENDPOINT");
        String workspaceUrl = System.getenv("DATABRICKS_WORKSPACE_URL");
        String tableName = System.getenv("ZEROBUS_TABLE_NAME");
        String clientId = System.getenv("DATABRICKS_CLIENT_ID");
        String clientSecret = System.getenv("DATABRICKS_CLIENT_SECRET");

        if (serverEndpoint == null || workspaceUrl == null || tableName == null
                || clientId == null || clientSecret == null) {
            System.err.println("Error: Required environment variables not set.");
            System.err.println("Set: ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,");
            System.err.println("     DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET");
            System.exit(1);
        }

        TlsOptions tls = buildTlsOptions();
        System.out.println("=== TLS Configuration Example ===\n");
        System.out.println("TLS options: " + tls);

        try (ZerobusSdk sdk = ZerobusSdk.builder()
                .serverEndpoint(serverEndpoint)
                .unityCatalogEndpoint(workspaceUrl)
                .tlsOptions(tls)
                .build()) {

            ZerobusJsonStream stream = sdk.streamBuilder()
                    .table(tableName)
                    .oauth(clientId, clientSecret)
                    .json()
                    .build()
                    .join();

            try {
                for (int i = 0; i < 10; i++) {
                    stream.ingestRecordOffset(String.format(
                            "{\"device_name\": \"tls-example-%d\", \"temp\": %d, \"humidity\": %d}",
                            i, 20 + i, 50 + i));
                }
                stream.flush();
                System.out.println("10 records queued and flushed");
            } finally {
                stream.close();
            }
        }
    }

    private static TlsOptions buildTlsOptions() {
        String caCertFile = System.getenv("ZEROBUS_CA_CERT_FILE");
        String clientCertFile = System.getenv("ZEROBUS_CLIENT_CERT_FILE");
        String clientKeyFile = System.getenv("ZEROBUS_CLIENT_KEY_FILE");

        TlsOptions.Builder builder = TlsOptions.builder();
        if (caCertFile != null) {
            builder.trustSystemRoots(false).addCaCertificateFile(caCertFile);
        }
        if (clientCertFile != null && clientKeyFile != null) {
            builder.clientIdentityFiles(clientCertFile, clientKeyFile);
        }
        return builder.build();
    }
}
