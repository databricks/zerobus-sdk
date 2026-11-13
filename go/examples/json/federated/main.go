// Command federated ingests JSON records using external-IdP token federation
// (Microsoft Entra ID) instead of a Databricks OAuth client secret.
//
// The SDK exchanges the Entra ID token for a Zerobus-scoped Databricks token
// (RFC 8693), caches it, and refreshes it before it expires. A federation policy
// must already be configured in Databricks.
//
// Two federation modes, selected by DATABRICKS_SP_CLIENT_ID:
//   - Unset: account-level federation. No Databricks service principal; the
//     identity is synced into Databricks through automatic identity management.
//   - Set: workload identity federation as that Databricks service principal,
//     which has a federation policy and no secret.
//
// Environment variables: ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL,
// ZEROBUS_TABLE_NAME, ENTRA_TENANT_ID, ENTRA_CLIENT_ID, ENTRA_CLIENT_SECRET, and
// optionally ENTRA_SCOPE and DATABRICKS_SP_CLIENT_ID.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	zerobus "github.com/databricks/zerobus-sdk/go"
)

// The default audience for Azure Databricks; use the audience your federation
// policy expects.
const defaultScope = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"

func main() {
	serverEndpoint := requiredEnv("ZEROBUS_SERVER_ENDPOINT")
	workspaceURL := requiredEnv("DATABRICKS_WORKSPACE_URL")
	tableName := requiredEnv("ZEROBUS_TABLE_NAME")

	// Entra ID app registration used to mint the IdP token.
	tenantID := requiredEnv("ENTRA_TENANT_ID")
	entraClientID := requiredEnv("ENTRA_CLIENT_ID")
	entraClientSecret := requiredEnv("ENTRA_CLIENT_SECRET")
	scope := os.Getenv("ENTRA_SCOPE")
	if scope == "" {
		scope = defaultScope
	}

	// Called only when the SDK must mint or refresh the Databricks token, not per record.
	supplier := func() (string, error) {
		return fetchEntraIDToken(tenantID, entraClientID, entraClientSecret, scope)
	}

	var opts []zerobus.FederatedTokenOption
	if spClientID := os.Getenv("DATABRICKS_SP_CLIENT_ID"); spClientID != "" {
		opts = append(opts, zerobus.WithClientID(spClientID))
	}
	token, err := zerobus.NewFederatedToken(supplier, opts...)
	if err != nil {
		log.Fatalf("Failed to create federated token: %v", err)
	}
	log.Printf("=== JSON Federated Auth Example (%v) ===", token)

	sdk, err := zerobus.NewZerobusSdk(serverEndpoint, workspaceURL)
	if err != nil {
		log.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	options := zerobus.DefaultStreamConfigurationOptions()
	options.RecordType = zerobus.RecordTypeJson

	stream, err := sdk.CreateStreamWithFederatedToken(
		zerobus.TableProperties{TableName: tableName},
		token,
		options,
	)
	if err != nil {
		log.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Close()

	// Ingest in a loop, then flush once. IngestRecordOffset returns as soon as the
	// record is queued.
	for i := 0; i < 100; i++ {
		// Change this record to match the schema of your table.
		record := fmt.Sprintf(`{"device_name": "sensor-%d", "temp": %d, "humidity": %d}`,
			i%10, 20+i%15, 50+i%40)
		if _, err := stream.IngestRecordOffset(record); err != nil {
			log.Fatalf("Failed to ingest record %d: %v", i, err)
		}
	}
	if err := stream.Flush(); err != nil {
		log.Fatalf("Failed to flush stream: %v", err)
	}
	log.Println("100 records queued and flushed")
}

// fetchEntraIDToken requests an Entra ID access token with the client
// credentials grant. Production code would typically use the Azure SDK's
// azidentity package (for example a managed identity) instead.
func fetchEntraIDToken(tenantID, clientID, clientSecret, scope string) (string, error) {
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
		"scope":         {scope},
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Post(
		"https://login.microsoftonline.com/"+url.PathEscape(tenantID)+"/oauth2/v2.0/token",
		"application/x-www-form-urlencoded",
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		// Returned errors are retryable: the SDK retries a failed mint like a
		// failed OAuth request.
		return "", fmt.Errorf("entra ID token request: %w", err)
	}
	defer resp.Body.Close()

	var body struct {
		AccessToken string `json:"access_token"`
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("entra ID token request failed: HTTP %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.AccessToken == "" {
		// A malformed response will not fix itself: fail without retrying.
		return "", &zerobus.ZerobusError{Message: "entra ID response has no access_token"}
	}
	return body.AccessToken, nil
}

func requiredEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("Missing required environment variable %s", name)
	}
	return value
}
