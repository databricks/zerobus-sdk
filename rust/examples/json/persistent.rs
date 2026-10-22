//! Persistent JSON ingestion with create and resume support.
//!
//! On the first run, leave `ZEROBUS_PERSISTENT_STREAM_ID` unset. The server
//! creates a durable stream and this example prints its ID. Persist that ID and
//! provide it through `ZEROBUS_PERSISTENT_STREAM_ID` on later runs to resume
//! from the server's last committed offset.
//!
//! Each run writes new records. Their ids continue after the last committed
//! offset, so you can tell the rows of each run apart in the table.
//!
//! Records are queued without waiting for individual acknowledgements. A
//! single `flush()` after the loop confirms the complete run.

use std::env;
use std::error::Error;

use databricks_zerobus_ingest_sdk::ZerobusSdk;

// Change these constants to match your workspace and table.
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

    let builder = sdk
        .persistent_stream_builder()
        .table(TABLE_NAME)
        .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
        .json()
        .max_inflight_requests(100);

    let mut stream = match env::var("ZEROBUS_PERSISTENT_STREAM_ID") {
        Ok(stream_id) => {
            println!("Resuming persistent stream {stream_id}");
            builder.resume(stream_id).await?
        }
        Err(env::VarError::NotPresent) => {
            let stream = builder.build().await?;
            println!(
                "Created persistent stream {}. Save this ID to resume later.",
                stream.stream_id().unwrap_or("<missing stream ID>")
            );
            stream
        }
        Err(error) => return Err(error.into()),
    };

    // Every ingest call uses one offset. A new stream starts at offset 0. A resumed
    // stream continues after the committed offset. The record id is the offset plus
    // one, so each run writes rows with new ids.
    let first_offset = stream
        .resume_watermark()
        .map_or(0, |watermark| watermark + 1);
    let now = chrono::Utc::now().timestamp_micros();
    let records: Vec<String> = (first_offset..first_offset + 2)
        .map(|offset| {
            let id = offset + 1;
            format!(
                r#"{{"id":{id},"customer_name":"customer-{id}","product_name":"Mouse","quantity":1,"price":25.99,"status":"pending","created_at":{now},"updated_at":{now}}}"#
            )
        })
        .collect();

    for record in records {
        let offset = stream.ingest_record_offset(record).await?;
        println!("Queued record at offset {offset}");
    }
    stream.flush().await?;
    println!("All queued records are durable");

    // Closing ends this connection. The durable stream stays available, and you
    // can resume it later with the same stream ID.
    stream.close().await?;
    Ok(())
}
