//! Dynamic protobuf ingestion with the schema fetched from Unity Catalog.
//!
//! Mints a token that can only read the table, fetches the table's schema with
//! `schema::fetch_uc_table_schema`, builds the descriptor from it, and fills
//! records field-by-field with `DynamicRecord` — no `.proto` file and no
//! hand-written columns.
//!
//! Throughput: ingest in a loop, then `flush()` once. Waiting per record forces a
//! server round-trip each time and collapses throughput.

use std::error::Error;

use databricks_zerobus_ingest_sdk::schema::{
    descriptor_from_uc_schema, fetch_uc_read_token, fetch_uc_table_schema, SchemaFetchOptions,
};
use databricks_zerobus_ingest_sdk::{message_descriptor, ProtoBytes, ZerobusSdk};

#[path = "../../config.rs"]
mod config;
use config::ExampleConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = ExampleConfig::from_env()?;

    // The stream's own OAuth token is scoped to Zerobus writes and can't read
    // table metadata, so mint a separate token limited to reading this table.
    let options = SchemaFetchOptions::default();
    let token = fetch_uc_read_token(
        &config.workspace_url,
        &config.table_name,
        &config.client_id,
        &config.client_secret,
        &options,
    )
    .await?;
    let schema =
        fetch_uc_table_schema(&config.workspace_url, &config.table_name, &token, &options).await?;
    for column in &schema.columns {
        println!("Column {}: {}", column.name, column.type_name);
    }
    let descriptor = message_descriptor(&descriptor_from_uc_schema(&schema)?)?;

    let sdk_handle = ZerobusSdk::builder()
        .endpoint(&config.server_endpoint)
        .unity_catalog_url(&config.workspace_url)
        .build()?;

    let mut stream = sdk_handle
        .stream_builder()
        .table(&config.table_name)
        .oauth(&config.client_id, &config.client_secret)
        .dynamic_proto(descriptor)
        .max_inflight_requests(100)
        .build()
        .await?;

    // (customer_name, quantity, price)
    let orders = [
        ("Alice Smith", 2i32, 25.99f64),
        ("Bob Johnson", 1, 89.99),
        ("Carol Williams", 3, 45.00),
    ];

    for (i, (customer_name, quantity, price)) in orders.iter().enumerate() {
        // set()'s value must match the fetched column's type (INT -> i32).
        let mut record = stream.new_record()?;
        record
            .set("id", i as i32)?
            .set("customer_name", *customer_name)?
            .set("quantity", *quantity)?
            .set("price", *price)?;

        // Queues without waiting for the ack.
        let offset_id = stream
            .ingest_record_offset(ProtoBytes(record.encode()?))
            .await?;
        println!("Record {i} queued with offset ID: {offset_id}");
    }

    // Wait once for all pending acks — not after each ingest.
    stream.flush().await?;
    println!("All records acknowledged");

    stream.close().await?;
    println!("Stream closed successfully");

    Ok(())
}
