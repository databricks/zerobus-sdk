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

    // Create a batch of JSON records
    let now = chrono::Utc::now().timestamp();
    let batch: Vec<String> = vec![
        format!(
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
        ),
        format!(
            r#"{{
                "id": 2,
                "customer_name": "Bob Johnson",
                "product_name": "Mechanical Keyboard",
                "quantity": 1,
                "price": 89.99,
                "status": "shipped",
                "created_at": {},
                "updated_at": {}
            }}"#,
            now, now
        ),
        format!(
            r#"{{
                "id": 3,
                "customer_name": "Carol Williams",
                "product_name": "USB-C Hub",
                "quantity": 3,
                "price": 45.00,
                "status": "delivered",
                "created_at": {},
                "updated_at": {}
            }}"#,
            now, now
        ),
    ];

    // Example 1: ingest_records_offset returns the offset immediately.
    let offset_id = stream.ingest_records_offset(batch).await.unwrap();
    if let Some(offset_id) = offset_id {
        println!("Batch sent with offset Id: {}", offset_id);
        // Wait for acknowledgment.
        stream.wait_for_offset(offset_id).await.unwrap();
        println!(
            "Batch of 3 records acknowledged with offset Id: {}",
            offset_id
        );
    }

    let batch_2: Vec<String> = vec![
        format!(
            r#"{{"id": 4, "customer_name": "David Green", "product_name": "Monitor", "quantity": 1, "price": 299.99, "status": "delivered", "created_at": {}, "updated_at": {}}}"#,
            now, now
        ),
        format!(
            r#"{{"id": 5, "customer_name": "Emma White", "product_name": "Webcam", "quantity": 2, "price": 59.99, "status": "pending", "created_at": {}, "updated_at": {}}}"#,
            now, now
        ),
    ];

    // Example 2: ingest_records returns a future that resolves to the offset (deprecated).
    let ack_future = stream.ingest_records(batch_2).await.unwrap();
    let offset_id = ack_future.await.unwrap();
    println!(
        "Batch of 2 records acknowledged with offset Id: {:?}",
        offset_id
    );

    let close_future = stream.close();
    close_future.await?;
    println!("Stream closed successfully");
    Ok(())
}
