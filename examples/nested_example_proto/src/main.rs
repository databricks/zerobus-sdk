use std::error::Error;
use std::fs;

use prost::Message;
use prost_reflect::prost_types;

use databricks_zerobus_ingest_sdk::{StreamConfigurationOptions, TableProperties, ZerobusSdk};
pub mod orders {
    include!("../output/orders.rs");
}
use crate::orders::{table_orders, TableOrders};

// Update this to match your table with nested STRUCT fields
// First, create a table with the following SQL:
//
// CREATE TABLE catalog.schema.orders (
//   id INT,
//   product_name STRING,
//   quantity INT,
//   price DOUBLE,
//   status STRING,
//   created_at BIGINT,
//   updated_at BIGINT,
//   customer STRUCT<
//     name STRING,
//     email STRING,
//     address STRUCT<
//       street STRING,
//       city STRING,
//       state STRING,
//       zip_code STRING,
//       country STRING
//     >
//   >,
//   shipping STRUCT<
//     method STRING,
//     tracking_number STRING,
//     estimated_delivery BIGINT,
//     delivery_address STRUCT<
//       street STRING,
//       city STRING,
//       state STRING,
//       zip_code STRING,
//       country STRING
//     >
//   >
// );

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
        load_descriptor_proto("output/orders.descriptor", "orders.proto", "table_Orders");
    let table_properties = TableProperties {
        table_name: TABLE_NAME.to_string(),
        descriptor_proto: Some(descriptor_proto),
    };
    let stream_configuration_options = StreamConfigurationOptions {
        max_inflight_records: 100,
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

    // Example with nested STRUCT fields
    // This demonstrates how to create records with nested Customer, Address, and Shipping structures
    let ack_future = stream
        .ingest_record(
            TableOrders {
                id: Some(1),
                product_name: Some("Wireless Mouse".to_string()),
                quantity: Some(2),
                price: Some(25.99),
                status: Some("pending".to_string()),
                created_at: Some(chrono::Utc::now().timestamp()),
                updated_at: Some(chrono::Utc::now().timestamp()),
                // Nested Customer STRUCT
                customer: Some(table_orders::Customer {
                    name: Some("Alice Smith".to_string()),
                    email: Some("alice.smith@example.com".to_string()),
                    // Nested Address STRUCT within Customer
                    address: Some(table_orders::Address {
                        street: Some("123 Main Street".to_string()),
                        city: Some("San Francisco".to_string()),
                        state: Some("CA".to_string()),
                        zip_code: Some("94102".to_string()),
                        country: Some("USA".to_string()),
                    }),
                }),
                // Nested Shipping STRUCT
                shipping: Some(table_orders::Shipping {
                    method: Some("Standard Shipping".to_string()),
                    tracking_number: Some("TRACK123456789".to_string()),
                    estimated_delivery: Some(chrono::Utc::now().timestamp() + 86400 * 5), // 5 days from now
                    // Nested Address STRUCT within Shipping (different address)
                    delivery_address: Some(table_orders::Address {
                        street: Some("456 Oak Avenue".to_string()),
                        city: Some("Oakland".to_string()),
                        state: Some("CA".to_string()),
                        zip_code: Some("94601".to_string()),
                        country: Some("USA".to_string()),
                    }),
                }),
            }
            .encode_to_vec(),
        )
        .await
        .unwrap();

    let _ack = ack_future.await.unwrap();
    println!("Record with nested STRUCT fields acknowledged with offset Id: 0");
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
