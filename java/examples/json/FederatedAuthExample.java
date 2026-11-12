package com.databricks.zerobus.examples.json;

import com.databricks.zerobus.*;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * External-IdP federation example (Microsoft Entra ID).
 *
 * <p>Authenticates with a token from Entra ID instead of a Databricks OAuth secret. The SDK
 * exchanges the Entra ID token for a Zerobus-scoped Databricks token (RFC 8693), caches it, and
 * refreshes it before it expires. A federation policy must already be configured in Databricks.
 *
 * <p>Two federation modes, selected by {@code DATABRICKS_SP_CLIENT_ID}:
 * <ul>
 *   <li>Unset: account-level federation. No Databricks service principal; the identity is synced
 *       into Databricks through automatic identity management.</li>
 *   <li>Set: workload identity federation as that Databricks service principal, which has a
 *       federation policy and no secret.</li>
 * </ul>
 *
 * <p>Environment variables: {@code ZEROBUS_SERVER_ENDPOINT}, {@code DATABRICKS_WORKSPACE_URL},
 * {@code ZEROBUS_TABLE_NAME}, {@code ENTRA_TENANT_ID}, {@code ENTRA_CLIENT_ID},
 * {@code ENTRA_CLIENT_SECRET}, and optionally {@code ENTRA_SCOPE} and
 * {@code DATABRICKS_SP_CLIENT_ID}.
 *
 * <p>Run with: {@code java -cp <classpath> com.databricks.zerobus.examples.json.FederatedAuthExample}
 */
public class FederatedAuthExample {

    private static final Pattern ACCESS_TOKEN = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");

    public static void main(String[] args) throws Exception {
        String serverEndpoint = requiredEnv("ZEROBUS_SERVER_ENDPOINT");
        String workspaceUrl = requiredEnv("DATABRICKS_WORKSPACE_URL");
        String tableName = requiredEnv("ZEROBUS_TABLE_NAME");

        // Entra ID app registration used to mint the IdP token.
        String tenantId = requiredEnv("ENTRA_TENANT_ID");
        String entraClientId = requiredEnv("ENTRA_CLIENT_ID");
        String entraClientSecret = requiredEnv("ENTRA_CLIENT_SECRET");
        // The audience your Databricks federation policy expects.
        String scope = System.getenv().getOrDefault(
                "ENTRA_SCOPE", "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default");
        String spClientId = System.getenv("DATABRICKS_SP_CLIENT_ID");

        // Called only when the SDK must mint or refresh the Databricks token, not per record.
        IdpTokenSupplier entraToken =
                () -> fetchEntraToken(tenantId, entraClientId, entraClientSecret, scope);

        FederatedToken auth = (spClientId == null || spClientId.isEmpty())
                ? FederatedToken.accountLevel(entraToken)
                : FederatedToken.workloadIdentity(entraToken, spClientId);
        System.out.println("=== JSON Federated Auth Example (" + auth + ") ===\n");

        try (ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
                ZerobusJsonStream stream = sdk.streamBuilder()
                        .table(tableName)
                        .federatedAuth(auth)
                        .json()
                        .build()
                        .join()) {
            for (int i = 0; i < 100; i++) {
                String json = String.format(
                        "{\"device_name\": \"sensor-%d\", \"temp\": %d, \"humidity\": %d}",
                        i % 10, 20 + (i % 15), 50 + (i % 40));
                stream.ingestRecordOffset(json);
            }
            stream.flush();
            System.out.println("100 records queued and flushed");
        }
    }

    /**
     * Requests an Entra ID access token with the client credentials grant. Production code would
     * typically use MSAL or a managed identity and a JSON parser instead.
     */
    private static String fetchEntraToken(
            String tenantId, String clientId, String clientSecret, String scope) throws Exception {
        URL url = new URL("https://login.microsoftonline.com/" + tenantId + "/oauth2/v2.0/token");
        String body = "grant_type=client_credentials"
                + "&client_id=" + URLEncoder.encode(clientId, "UTF-8")
                + "&client_secret=" + URLEncoder.encode(clientSecret, "UTF-8")
                + "&scope=" + URLEncoder.encode(scope, "UTF-8");

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setConnectTimeout(15_000);
        conn.setReadTimeout(15_000);
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        try (OutputStream out = conn.getOutputStream()) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }

        int status = conn.getResponseCode();
        String response = readAll(status < 400 ? conn.getInputStream() : conn.getErrorStream());
        if (status != 200) {
            // Retryable: the SDK retries a failed mint like a failed OAuth request.
            throw new IllegalStateException("Entra ID token request failed: HTTP " + status);
        }
        Matcher m = ACCESS_TOKEN.matcher(response);
        if (!m.find()) {
            throw new NonRetriableException("Entra ID response has no access_token");
        }
        return m.group(1);
    }

    private static String readAll(InputStream in) throws Exception {
        if (in == null) {
            return "";
        }
        try (InputStream input = in) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] chunk = new byte[4096];
            int n;
            while ((n = input.read(chunk)) != -1) {
                buf.write(chunk, 0, n);
            }
            return new String(buf.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            System.err.println("Error: environment variable " + name + " is not set.");
            System.exit(1);
        }
        return value;
    }
}
