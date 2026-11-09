//! Single-record Avro ingestion (Beta), demonstrating both ways to pass a record.
//!
//! Note on throughput: `ingest_record_offset()` returns as soon as the record is queued.
//! This example ingests several records and then calls `flush()` ONCE to confirm them.
//! Do not call `wait_for_offset()` after every record in a real workload — that forces a
//! server round-trip per record and collapses throughput. For high volume, prefer the
//! batch API in `batch.rs`.

use std::error::Error;

use apache_avro::{writer::datum::GenericDatumWriter, Schema};
use databricks_zerobus_ingest_sdk::{AvroBytes, AvroRecord, AvroValue, ZerobusSdk};

#[path = "../config.rs"]
mod config;
use config::ExampleConfig;

// The Avro writer schema (JSON), declared once at stream creation.
const AVRO_SCHEMA: &str = r#"{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "customer_name", "type": "string"}
  ]
}"#;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = ExampleConfig::from_env()?;
    let sdk = ZerobusSdk::builder()
        .endpoint(&config.server_endpoint)
        .unity_catalog_url(&config.workspace_url)
        .build()?;

    let mut stream = sdk
        .stream_builder()
        .table(&config.table_name)
        .oauth(&config.client_id, &config.client_secret)
        .avro(AVRO_SCHEMA)
        .build()
        .await?;

    // 1. Record object: build an AvroValue and the stream encodes it against the schema.
    let record = AvroValue::Record(vec![
        ("id".to_string(), AvroValue::Long(1)),
        (
            "customer_name".to_string(),
            AvroValue::String("Alice".to_string()),
        ),
    ]);
    let offset_id = stream.ingest_record_offset(AvroRecord(record)).await?;
    println!("[Record object] Record queued with offset ID: {offset_id}");

    // 2. Pre-encoded: encode a record against AVRO_SCHEMA yourself and pass the raw datum.
    //    Use this path when you already hold Avro bytes (e.g. produced by an upstream system).
    let schema = Schema::parse_str(AVRO_SCHEMA)?;
    let order = AvroValue::Record(vec![
        ("id".to_string(), AvroValue::Long(2)),
        (
            "customer_name".to_string(),
            AvroValue::String("Bob".to_string()),
        ),
    ]);
    let datum = GenericDatumWriter::builder(&schema)
        .build()?
        .write_value_to_vec(order.resolve(&schema)?)?;
    let offset_id = stream.ingest_record_offset(AvroBytes(datum)).await?;
    println!("[Pre-encoded] Record queued with offset ID: {offset_id}");

    // Confirm all queued records at once. flush() waits for every pending acknowledgment;
    // this is the right place to wait, not after each individual ingest above.
    stream.flush().await?;
    println!("All records acknowledged");

    stream.close().await?;
    println!("Stream closed successfully");

    Ok(())
}
