//go:build avro

package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	zerobus "github.com/databricks/zerobus-sdk/go"
)

func main() {
	endpoint := flag.String("zerobus-endpoint", os.Getenv("ZEROBUS_ENDPOINT"), "Zerobus endpoint URL")
	catalogURL := flag.String("catalog-url", os.Getenv("UNITY_CATALOG_URL"), "Unity Catalog URL")
	clientID := flag.String("client-id", os.Getenv("DATABRICKS_CLIENT_ID"), "OAuth2 client ID")
	clientSecret := flag.String("client-secret", os.Getenv("DATABRICKS_CLIENT_SECRET"), "OAuth2 client secret")
	tableName := flag.String("table", "catalog.schema.table", "Target table name")
	flag.Parse()

	// Validate inputs
	if *endpoint == "" || *catalogURL == "" || *clientID == "" || *clientSecret == "" {
		log.Fatal("Missing required parameters. Please set ZEROBUS_ENDPOINT, UNITY_CATALOG_URL, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET or pass as flags.")
	}

	// Create SDK instance
	sdk, err := zerobus.NewZerobusSdk(*endpoint, *catalogURL)
	if err != nil {
		log.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	// Avro schema: a record with id (int) and name (string) fields
	schemaJSON := `{
		"type": "record",
		"name": "SimpleRecord",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`

	// Create Avro stream
	tableProps := zerobus.AvroTableProperties{
		TableName:  *tableName,
		SchemaJSON: schemaJSON,
	}

	opts := zerobus.DefaultStreamConfigurationOptions()
	opts.RecordType = zerobus.RecordTypeAvro

	stream, err := sdk.CreateAvroStream(tableProps, *clientID, *clientSecret, opts)
	if err != nil {
		log.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Close()

	fmt.Println("Avro stream created successfully (Beta - requires server Avro support)")

	// Define records as AvroRecord (native encoding with hamba/avro)
	records := []zerobus.AvroRecord{
		{"id": int32(1), "name": "Alice"},
		{"id": int32(2), "name": "Bob"},
		{"id": int32(3), "name": "Charlie"},
	}

	fmt.Printf("Ingesting %d Avro records...\n", len(records))

	// Queue records in a loop; SDK encodes each to binary Avro
	var lastOffset int64
	for _, rec := range records {
		offset, err := stream.IngestAvroRecordOffset(rec)
		if err != nil {
			log.Printf("Failed to ingest record: %v", err)
			continue
		}
		lastOffset = offset
		fmt.Printf("Record %v queued with offset %d\n", rec, offset)
	}

	// Flush to confirm all records are acknowledged
	fmt.Println("Flushing pending records...")
	if err := stream.Flush(); err != nil {
		log.Fatalf("Flush failed: %v", err)
	}

	fmt.Printf("Successfully ingested and flushed %d Avro records (last offset: %d)\n", len(records), lastOffset)

	// Alternative: Ingest pre-encoded Avro bytes (if you already have encoded data)
	// Use IngestAvroBytesOffset() and IngestAvroBytesBatchOffset() for pre-encoded datums
}
