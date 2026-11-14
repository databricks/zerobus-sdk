//! Single-record protobuf ingestion, demonstrating each record wrapper type.
//!
//! Note on throughput: `ingest_record_offset()` returns as soon as the record is queued.
//! This example ingests several records and then calls `flush()` ONCE to confirm them.
//! Do not call `wait_for_offset()` after every record in a real workload — that forces a
//! server round-trip per record and collapses throughput. For high volume, prefer the
//! batch API in `batch.rs`.

use std::error::Error;

use prost::Message;
use prost_reflect::prost_types;

use databricks_zerobus_ingest_sdk::{ProtoBytes, ProtoMessage, ZerobusSdk, ZerobusStream};

#[path = "../../config.rs"]
mod config;
use config::ExampleConfig;

pub mod orders {
    include!("output/orders.rs");
}
use crate::orders::TableOrders;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = ExampleConfig::from_env()?;
    // Embed the descriptor at compile time (like the generated `orders.rs`
    // above), so it needs no runtime file read.
    let descriptor_proto = load_descriptor_proto(
        include_bytes!("output/orders.descriptor"),
        "orders.proto",
        "table_Orders",
    );
    let sdk_handle = ZerobusSdk::builder()
        .endpoint(&config.server_endpoint)
        .unity_catalog_url(&config.workspace_url)
        .build()?;

    let mut stream = sdk_handle
        .stream_builder()
        .table(&config.table_name)
        .oauth(&config.client_id, &config.client_secret)
        .compiled_proto(descriptor_proto)
        .max_inflight_requests(100)
        .build()
        .await?;

    ingest_with_offset_api(&mut stream).await?;

    stream.close().await?;
    println!("Stream closed successfully");

    Ok(())
}

/// Recommended API: returns offset directly after queuing.
async fn ingest_with_offset_api(stream: &mut ZerobusStream) -> Result<(), Box<dyn Error>> {
    println!("=== Offset-based API (Recommended) ===");

    // Delta TIMESTAMP is int64 microseconds since epoch UTC.
    let now = chrono::Utc::now().timestamp_micros();

    // 1. Auto-encoding: ProtoMessage - pass message directly, SDK handles encoding.
    let order = TableOrders {
        id: Some(1),
        customer_name: Some("Alice Smith".to_string()),
        product_name: Some("Wireless Mouse".to_string()),
        quantity: Some(2),
        price: Some(25.99),
        status: Some("pending".to_string()),
        created_at: Some(now),
        updated_at: Some(now),
    };

    // Queue the record; the call returns immediately without waiting for the ack.
    let offset_id = stream.ingest_record_offset(ProtoMessage(order)).await?;
    println!(
        "[Auto-encoding] Record queued with offset ID: {}",
        offset_id
    );

    // 2. Pre-encoded: ProtoBytes - pass bytes with explicit wrapper.
    let order = TableOrders {
        id: Some(2),
        customer_name: Some("Bob Johnson".to_string()),
        product_name: Some("Mechanical Keyboard".to_string()),
        quantity: Some(1),
        price: Some(89.99),
        status: Some("shipped".to_string()),
        created_at: Some(now),
        updated_at: Some(now),
    };
    let bytes = order.encode_to_vec();

    let offset_id = stream.ingest_record_offset(ProtoBytes(bytes)).await?;
    println!("[Pre-encoded] Record queued with offset ID: {}", offset_id);

    // 3. Backward-compatible: raw Vec<u8> - no wrapper needed, works the same as ProtoBytes.
    let order = TableOrders {
        id: Some(3),
        customer_name: Some("Carol Williams".to_string()),
        product_name: Some("USB-C Hub".to_string()),
        quantity: Some(3),
        price: Some(45.00),
        status: Some("delivered".to_string()),
        created_at: Some(now),
        updated_at: Some(now),
    };
    let raw_bytes = order.encode_to_vec();

    let offset_id = stream.ingest_record_offset(raw_bytes).await?;
    println!(
        "[Backward-compatible] Record queued with offset ID: {}",
        offset_id
    );

    // Confirm all queued records at once. flush() waits for every pending acknowledgment;
    // this is the right place to wait, not after each individual ingest above.
    stream.flush().await?;
    println!("All records acknowledged");

    Ok(())
}

fn load_descriptor_proto(
    descriptor_bytes: &[u8],
    file_name: &str,
    message_name: &str,
) -> prost_types::DescriptorProto {
    let file_descriptor_set = prost_types::FileDescriptorSet::decode(descriptor_bytes).unwrap();

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
