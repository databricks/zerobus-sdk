//! Batch Avro ingestion (Beta), demonstrating both ways to pass records.
//!
//! `ingest_records_offset()` queues the whole batch under a single logical offset and
//! returns immediately; call `flush()` once at the end to confirm durability.

use std::error::Error;

use apache_avro::{to_avro_datum, Schema};
use databricks_zerobus_ingest_sdk::{AvroBytes, AvroRecord, AvroValue, ZerobusSdk};

// Change constants to match your data.
const TABLE_NAME: &str = "<your_table_name>";
const DATABRICKS_CLIENT_ID: &str = "<your_databricks_client_id>";
const DATABRICKS_CLIENT_SECRET: &str = "<your_databricks_client_secret>";
const DATABRICKS_WORKSPACE_URL: &str = "https://<your-workspace>.cloud.databricks.com";
const SERVER_ENDPOINT: &str = "https://<your-shard-id>.zerobus.<region>.cloud.databricks.com";

// The Avro writer schema (JSON), declared once at stream creation.
const AVRO_SCHEMA: &str = r#"{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "customer_name", "type": "string"}
  ]
}"#;

/// Builds an `Order` Avro value for the schema above.
fn order(id: i64, customer_name: &str) -> AvroValue {
    AvroValue::Record(vec![
        ("id".to_string(), AvroValue::Long(id)),
        (
            "customer_name".to_string(),
            AvroValue::String(customer_name.to_string()),
        ),
    ])
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let sdk = ZerobusSdk::builder()
        .endpoint(SERVER_ENDPOINT)
        .unity_catalog_url(DATABRICKS_WORKSPACE_URL)
        .build()?;

    let mut stream = sdk
        .stream_builder()
        .table(TABLE_NAME)
        .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
        .avro(AVRO_SCHEMA)
        .build()
        .await?;

    // 1. Record objects: a batch of AvroValues the stream encodes against the schema.
    let records: Vec<AvroRecord> = vec![AvroRecord(order(1, "Alice")), AvroRecord(order(2, "Bob"))];
    if let Some(offset_id) = stream.ingest_records_offset(records).await? {
        println!("[Record objects] Batch queued with offset ID: {offset_id}");
    }

    // 2. Pre-encoded: encode records against AVRO_SCHEMA yourself and pass the raw datums.
    //    Use this path when you already hold Avro bytes (e.g. produced by an upstream system).
    let schema = Schema::parse_str(AVRO_SCHEMA)?;
    let mut pre_encoded_batch: Vec<AvroBytes> = Vec::new();
    for value in [order(3, "Carol"), order(4, "Dan")] {
        let datum = to_avro_datum(&schema, value.resolve(&schema)?)?;
        pre_encoded_batch.push(AvroBytes(datum));
    }
    if let Some(offset_id) = stream.ingest_records_offset(pre_encoded_batch).await? {
        println!("[Pre-encoded] Batch queued with offset ID: {offset_id}");
    }

    // Confirm all queued batches at once.
    stream.flush().await?;
    println!("All offset-API batches acknowledged");

    stream.close().await?;
    println!("Stream closed successfully");

    Ok(())
}
