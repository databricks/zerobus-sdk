use std::error::Error;
use std::sync::Arc;

use databricks_zerobus_ingest_sdk::{AckCallback, MessageId, ProtoMessage, ZerobusSdk};
use prost::Message;
use prost_reflect::prost_types;

pub mod orders {
    include!("output/orders.rs");
}
use crate::orders::TableOrders;

#[path = "../../config.rs"]
mod config;
use config::ExampleConfig;

struct AckLogger;

impl AckCallback<MessageId> for AckLogger {
    fn on_ack(&self, message_id: MessageId) {
        println!("Acknowledged {message_id}");
    }

    fn on_error(&self, message_id: MessageId, error_message: &str) {
        eprintln!("Error for {message_id}: {error_message}");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = ExampleConfig::from_env()?;
    let descriptor = load_descriptor_proto(
        include_bytes!("output/orders.descriptor"),
        "orders.proto",
        "table_Orders",
    );
    let sdk = ZerobusSdk::builder()
        .endpoint(&config.server_endpoint)
        .unity_catalog_url(&config.workspace_url)
        .build()?;

    let mut stream = sdk
        .stream_builder()
        .table(&config.table_name)
        .oauth(&config.client_id, &config.client_secret)
        .compiled_proto(descriptor)
        .max_inflight_requests(10_000)
        .multiplexed_ack_callback(Arc::new(AckLogger))
        .multiplexed(4)
        .build()
        .await?;

    for id in 0..10_000 {
        let record = TableOrders {
            id: Some(id),
            customer_name: Some("Alice Smith".to_string()),
            ..Default::default()
        };
        let _message_id = stream.ingest_record(ProtoMessage(record)).await?;

        if (id + 1) % 1_000 == 0 {
            stream.flush().await?;
        }
    }

    stream.flush().await?;
    stream.close().await?;
    Ok(())
}

fn load_descriptor_proto(
    descriptor_bytes: &[u8],
    file_name: &str,
    message_name: &str,
) -> prost_types::DescriptorProto {
    let file_descriptor_set = prost_types::FileDescriptorSet::decode(descriptor_bytes).unwrap();
    let file = file_descriptor_set
        .file
        .into_iter()
        .find(|file| file.name.as_deref() == Some(file_name))
        .unwrap();
    file.message_type
        .into_iter()
        .find(|message| message.name.as_deref() == Some(message_name))
        .unwrap()
}
