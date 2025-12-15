use std::error::Error;

use databricks_zerobus_ingest_sdk::{
    databricks::zerobus::RecordType, StreamConfigurationOptions, TableProperties, ZerobusSdk,
};

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
    let table_properties = TableProperties {
        table_name: TABLE_NAME.to_string(),
        // descriptor_proto is not needed for JSON ingestion
        descriptor_proto: None,
    };
    let stream_configuration_options = StreamConfigurationOptions {
        max_inflight_requests: 100,
        record_type: RecordType::Json,
        ..Default::default()
    };
    let sdk_handle = ZerobusSdk::new(
        SERVER_ENDPOINT.to_string(),
        DATABRICKS_WORKSPACE_URL.to_string(),
    )?;

    let mut stream = sdk_handle
        .create_stream(
            table_properties.clone(),
            DATABRICKS_CLIENT_ID.to_string(),
            DATABRICKS_CLIENT_SECRET.to_string(),
            Some(stream_configuration_options),
        )
        .await
        .expect("Failed to create a stream.");

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

    // Ingest the batch
    let ack_future = stream.ingest_records(batch).await.unwrap();

    let _ack = ack_future.await.unwrap();
    println!("Batch of 3 records acknowledged with offset Id: 0");
    let close_future = stream.close();
    close_future.await?;
    println!("Stream closed successfully");
    Ok(())
}
