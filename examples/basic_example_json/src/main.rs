use std::error::Error;

use databricks_zerobus_ingest_sdk::ZerobusSdk;

// Change constants to match your data.
const TABLE_NAME: &str = "<your_table_name>";
const DATABRICKS_CLIENT_ID: &str = "<your_databricks_client_id>";
const DATABRICKS_CLIENT_SECRET: &str = "<your_databricks_client_secret>";

// Uncomment the appropriate lines for your cloud.

// For AWS:
const DATABRICKS_WORKSPACE_URL: &str = "https://<your-workspace>.cloud.databricks.com";
const SERVER_ENDPOINT: &str = "<your-shard-id>.zerobus.<region>.cloud.databricks.com";

// For Azure:
// const DATABRICKS_WORKSPACE_URL: &str = "https://<your-workspace>.azuredatabricks.net";
// const SERVER_ENDPOINT: &str = "<your-shard-id>.zerobus.<region>.azuredatabricks.net";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let sdk = ZerobusSdk::builder()
        .endpoint(SERVER_ENDPOINT)
        .unity_catalog_url(DATABRICKS_WORKSPACE_URL)
        .build()?;

    let mut stream = sdk
        .stream_builder(TABLE_NAME)
        .client_credentials(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
        .max_inflight_requests(100)
        .json()
        .build()
        .await?;

    // Change the values to match your data.
    let now = chrono::Utc::now().timestamp();
    let json_record = format!(
        r#"{{
            "id": 1,
            "customer_name": "Alice Smith",
            "product_name": "Wireless Mouse",
            "quantity": 2,
            "price": 25.99,
            "status": "pending",
            "created_at": {},
            "updated_at": {}
        }}"#,
        now, now
    );

    // Example 1: ingest_record_offset returns the offset immediately.
    let offset_id = stream.ingest_record_offset(json_record).await.unwrap();
    println!("Record sent with offset Id: {}", offset_id);
    // Wait for acknowledgment.
    stream.wait_for_offset(offset_id).await.unwrap();
    println!("Record acknowledged with offset Id: {}", offset_id);

    let json_record_2 = format!(
        r#"{{
            "id": 2,
            "customer_name": "Bob Jones",
            "product_name": "USB Cable",
            "quantity": 5,
            "price": 9.99,
            "status": "shipped",
            "created_at": {},
            "updated_at": {}
        }}"#,
        now, now
    );
    // Example 2: ingest_record returns a future that resolves to the offset.
    let ack_future = stream.ingest_record(json_record_2).await.unwrap();
    let offset_id_2 = ack_future.await.unwrap();
    println!("Record acknowledged with offset Id: {}", offset_id_2);

    let close_future = stream.close();
    close_future.await?;
    println!("Stream closed successfully");
    Ok(())
}
