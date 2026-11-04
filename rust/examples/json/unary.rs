//! Unary JSON ingestion: each call sends one batch and returns once it is durable.
//!
//! A unary client opens no stream, so it suits sparse traffic, such as a few
//! batches per minute. Collect the records you have and send them in one call.
//! For continuous ingestion, use a stream (see `batch.rs`).

use std::error::Error;

use databricks_zerobus_ingest_sdk::{JsonValue, ZerobusSdk};
use serde::Serialize;

#[path = "../config.rs"]
mod config;
use config::ExampleConfig;

#[derive(Serialize)]
struct Order {
    id: i32,
    customer_name: String,
    product_name: String,
    quantity: i32,
    price: f64,
    status: String,
    created_at: i64,
    updated_at: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = ExampleConfig::from_env()?;
    let sdk = ZerobusSdk::builder()
        .endpoint(&config.server_endpoint)
        .unity_catalog_url(&config.workspace_url)
        .build()?;

    // Build the client once and reuse it for every call to this table.
    let client = sdk
        .unary_client_builder()
        .table(&config.table_name)
        .oauth(&config.client_id, &config.client_secret)
        .json()
        .build()
        .await?;

    // Delta TIMESTAMP is int64 microseconds since epoch UTC.
    let now = chrono::Utc::now().timestamp_micros();
    let batch: Vec<JsonValue<Order>> = (1..=3)
        .map(|id| {
            JsonValue(Order {
                id,
                customer_name: format!("customer-{id}"),
                product_name: "Wireless Mouse".to_string(),
                quantity: 1,
                price: 25.99,
                status: "pending".to_string(),
                created_at: now,
                updated_at: now,
            })
        })
        .collect();

    // One call for the whole batch. It returns once all records are durable,
    // so there is nothing to flush or close afterwards.
    client.ingest_records(batch).await?;
    println!("Batch of 3 records is durable");

    Ok(())
}
