//! Multiplexed Avro ingestion (Beta).
//!
//! Records are queued across managed sub-streams and acknowledged together with one
//! `flush()`. Do not wait for each `MessageId` in the ingest loop; doing so serializes
//! ingestion on server round trips.

use std::error::Error;

use databricks_zerobus_ingest_sdk::{AvroRecord, AvroValue, ZerobusSdk};

#[path = "../config.rs"]
mod config;
use config::ExampleConfig;

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
        .multiplexed(4)
        .build()
        .await?;

    for id in 0..100_000 {
        let record = AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Long(id)),
            (
                "customer_name".to_string(),
                AvroValue::String(format!("Customer {id}")),
            ),
        ]);
        let _message_id = stream.ingest_record(AvroRecord(record)).await?;
    }

    // Wait once for all records queued across every sub-stream.
    stream.flush().await?;
    stream.close().await?;
    Ok(())
}
