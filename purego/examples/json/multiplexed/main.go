// Multiplexed JSON ingestion example (Beta).
//
// Opens a multiplexed stream with 4 lanes, queues records with IngestRecord
// in a loop, and calls Flush once at the end. Each record goes to the next
// lane.
//
// Set these environment variables before running:
//
//	ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//	DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
//
//	go run ./json/multiplexed
//
// Target table:
//
//	orders(id INT, customer_name STRING, product_name STRING, quantity INT,
//	       price DOUBLE, status STRING, created_at TIMESTAMP, updated_at TIMESTAMP)
package main

import (
	"context"
	"log"

	"github.com/databricks/zerobus-sdk/purego/examples/config"
	"github.com/databricks/zerobus-sdk/purego/examples/internal/exampleutil"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

func main() {
	cfg := config.Load()

	// 1. Create SDK.
	sdk, err := zerobus.New(cfg.ServerEndpoint, cfg.WorkspaceURL,
		zerobus.WithApplicationName("json-multiplexed"))
	if err != nil {
		log.Fatalf("create SDK: %v", err)
	}
	defer sdk.Close()

	// 2. Open a multiplexed JSON stream with 4 lanes. It returns after every
	// lane is open.
	mux, err := sdk.CreateMultiplexedStream(context.Background(), cfg.TableName,
		zerobus.OAuth(cfg.ClientID, cfg.ClientSecret), 4, zerobus.WithJSON())
	if err != nil {
		log.Fatalf("create multiplexed stream: %v", err)
	}
	defer mux.Close()

	now := exampleutil.NowMicros()

	// 3. Queue records without per-record waits. Records go to the lanes
	// round-robin.
	customers := []string{"Alice Smith", "Bob Johnson", "Carol Williams", "Dave Brown"}
	for i := range 100 {
		rec := exampleutil.MakeOrderJSON(i+1, customers[i%len(customers)],
			"Wireless Mouse", 1+i%3, 25.99, "pending", now)
		if _, err := mux.IngestRecord([]byte(rec)); err != nil {
			log.Fatalf("ingest record %d: %v", i+1, err)
		}
	}

	// 4. Flush once: wait until every lane has acknowledged its records.
	if err := mux.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}

	// 5. Close every lane.
	if err := mux.Close(); err != nil {
		log.Fatalf("close: %v", err)
	}
	log.Printf("Multiplexed stream with %d lanes closed. 100 records acknowledged.", mux.StreamCount())
}
