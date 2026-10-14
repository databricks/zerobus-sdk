//! Queue whole batches across managed Arrow Flight lanes, then flush once.
use std::error::Error;
use std::sync::Arc;

use arrow_array::{Int64Array, LargeStringArray, RecordBatch};
use databricks_zerobus_ingest_sdk::{ArrowSchema, DataType, Field, ZerobusSdk};

// Update these values and create a Delta table with id BIGINT, customer_name STRING.
const TABLE_NAME: &str = "<your_table_name>";
const DATABRICKS_CLIENT_ID: &str = "<your_databricks_client_id>";
const DATABRICKS_CLIENT_SECRET: &str = "<your_databricks_client_secret>";
const DATABRICKS_WORKSPACE_URL: &str = "https://<your-workspace>.cloud.databricks.com";
const SERVER_ENDPOINT: &str = "https://<your-shard-id>.zerobus.<region>.cloud.databricks.com";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let sdk = ZerobusSdk::builder()
        .endpoint(SERVER_ENDPOINT)
        .unity_catalog_url(DATABRICKS_WORKSPACE_URL)
        .build()?;
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("customer_name", DataType::LargeUtf8, true),
    ]));
    let mut stream = sdk
        .stream_builder()
        .table(TABLE_NAME)
        .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
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
