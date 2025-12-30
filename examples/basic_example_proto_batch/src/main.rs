use std::error::Error;
use std::fs;

use prost::Message;
use prost_reflect::prost_types;

use databricks_zerobus_ingest_sdk::{StreamConfigurationOptions, TableProperties, ZerobusSdk};
pub mod orders {
    include!("../output/orders.rs");
}
use crate::orders::TableOrders;

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
    let descriptor_proto =
        load_descriptor_proto("output/orders.descriptor", "orders.proto", "table_Orders"); //<your_descriptor_file>, <your_proto_file>, <your_proto_message_name>
    let table_properties = TableProperties {
        table_name: TABLE_NAME.to_string(),
        descriptor_proto: Some(descriptor_proto),
    };
    let stream_configuration_options = StreamConfigurationOptions {
        max_inflight_requests: 100,
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

    // Create a batch of records
    let now = chrono::Utc::now().timestamp();
    let batch: Vec<Vec<u8>> = vec![
        TableOrders {
            id: Some(1),
            customer_name: Some("Alice Smith".to_string()),
            product_name: Some("Wireless Mouse".to_string()),
            quantity: Some(2),
            price: Some(25.99),
            status: Some("pending".to_string()),
            created_at: Some(now),
            updated_at: Some(now),
        }
        .encode_to_vec(),
        TableOrders {
            id: Some(2),
            customer_name: Some("Bob Johnson".to_string()),
            product_name: Some("Mechanical Keyboard".to_string()),
            quantity: Some(1),
            price: Some(89.99),
            status: Some("shipped".to_string()),
            created_at: Some(now),
            updated_at: Some(now),
        }
        .encode_to_vec(),
        TableOrders {
            id: Some(3),
            customer_name: Some("Carol Williams".to_string()),
            product_name: Some("USB-C Hub".to_string()),
            quantity: Some(3),
            price: Some(45.00),
            status: Some("delivered".to_string()),
            created_at: Some(now),
            updated_at: Some(now),
        }
        .encode_to_vec(),
    ];

    // Example 1: Using v1 API, ingest_records returns a future.
    let ack_future = stream.ingest_records(batch).await.unwrap();
    let offset_id = ack_future.await.unwrap();
    println!(
        "Batch of 3 records acknowledged with offset Id: {:?}",
        offset_id
    );

    let batch_2: Vec<Vec<u8>> = vec![
        TableOrders {
            id: Some(4),
            customer_name: Some("David Green".to_string()),
            product_name: Some("Monitor".to_string()),
            quantity: Some(1),
            price: Some(299.99),
            status: Some("delivered".to_string()),
            created_at: Some(now),
            updated_at: Some(now),
        }
        .encode_to_vec(),
        TableOrders {
            id: Some(5),
            customer_name: Some("Emma White".to_string()),
            product_name: Some("Webcam".to_string()),
            quantity: Some(2),
            price: Some(59.99),
            status: Some("pending".to_string()),
            created_at: Some(now),
            updated_at: Some(now),
        }
        .encode_to_vec(),
    ];

    // Example 2: Using v2 API, ingest_records_v2 returns offset immediately.
    let offset_id = stream.ingest_records_v2(batch_2).await.unwrap();
    if let Some(offset_id) = offset_id {
        println!("Batch sent with offset Id: {}", offset_id);
        // Wait for acknowledgment.
        stream.wait_for_offset(offset_id).await.unwrap();
        println!(
            "Batch of 2 records acknowledged with offset Id: {}",
            offset_id
        );
    }

    let close_future = stream.close();
    close_future.await?;
    println!("Stream closed successfully");
    Ok(())
}

fn load_descriptor_proto(
    path: &str,
    file_name: &str,
    message_name: &str,
) -> prost_types::DescriptorProto {
    let descriptor_bytes = fs::read(path).expect("Failed to read proto descriptor file");
    let file_descriptor_set =
        prost_types::FileDescriptorSet::decode(descriptor_bytes.as_ref()).unwrap();

    let file_descriptor_proto = file_descriptor_set
        .file
        .into_iter()
        .find(|f| f.name.as_deref() == Some(file_name))
        .unwrap();

    file_descriptor_proto
        .message_type
        .into_iter()
        .find(|m| m.name.as_deref() == Some(message_name))
        .unwrap()
}
