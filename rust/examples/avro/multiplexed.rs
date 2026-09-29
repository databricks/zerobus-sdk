//! Multiplexed Avro ingestion (Beta).
//!
//! Records are queued across managed sub-streams and acknowledged together with one
//! `flush()`. Do not wait for each `MessageId` in the ingest loop; doing so serializes
//! ingestion on server round trips.

use std::error::Error;

use databricks_zerobus_ingest_sdk::{AvroRecord, AvroValue, ZerobusSdk};

// Change constants to match your data.
const TABLE_NAME: &str = "<your_table_name>";
const DATABRICKS_CLIENT_ID: &str = "<your_databricks_client_id>";
const DATABRICKS_CLIENT_SECRET: &str = "<your_databricks_client_secret>";
const DATABRICKS_WORKSPACE_URL: &str = "https://<your-workspace>.cloud.databricks.com";
const SERVER_ENDPOINT: &str = "https://<your-shard-id>.zerobus.<region>.cloud.databricks.com";

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
    let sdk = ZerobusSdk::builder()
        .endpoint(SERVER_ENDPOINT)
        .unity_catalog_url(DATABRICKS_WORKSPACE_URL)
        .build()?;

    let mut stream = sdk
        .stream_builder()
        .table(TABLE_NAME)
        .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
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
