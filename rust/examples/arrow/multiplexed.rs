//! Queue whole batches across managed Arrow Flight lanes, then flush once.
//!
//! Requires a Delta table with nullable `id BIGINT` and `customer_name STRING` columns.
use std::error::Error;
use std::sync::Arc;

use arrow_array::{Int64Array, LargeStringArray, RecordBatch};
use databricks_zerobus_ingest_sdk::{ArrowSchema, DataType, Field, ZerobusSdk};

#[path = "../config.rs"]
mod config;
use config::ExampleConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = ExampleConfig::from_env()?;
    let sdk = ZerobusSdk::builder()
        .endpoint(&config.server_endpoint)
        .unity_catalog_url(&config.workspace_url)
        .build()?;
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("customer_name", DataType::LargeUtf8, true),
    ]));
    let mut stream = sdk
        .stream_builder()
        .table(&config.table_name)
        .oauth(&config.client_id, &config.client_secret)
        .arrow(schema.clone())
        .max_inflight_batches(100) // Mux-wide budget: 25 pending batches per lane.
        .multiplexed(4)
        .build_arrow()
        .await?;

    for batch_index in 0..100 {
        let start = batch_index * 1_000;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(start..start + 1_000)),
                Arc::new(LargeStringArray::from_iter_values(
                    (0..1_000).map(|_| "Customer"),
                )),
            ],
        )?;
        // The ID identifies a lane and batch offset; queuing does not await durability.
        let _message_id = stream.ingest_batch(batch).await?;
    }
    stream.flush().await?;
    stream.close().await?;
    Ok(())
}
