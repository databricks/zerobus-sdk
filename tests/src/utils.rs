use std::collections::HashMap;
use std::sync::{Arc, Once};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use async_trait::async_trait;
use databricks_zerobus_ingest_sdk::HeadersProvider;
use databricks_zerobus_ingest_sdk::ZerobusResult;
use prost_reflect::prost_types;
use tracing_subscriber::EnvFilter;

static SETUP: Once = Once::new();

#[derive(Default)]
pub struct TestHeadersProvider {}

#[async_trait]
impl HeadersProvider for TestHeadersProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        let mut headers = HashMap::new();
        headers.insert("authorization", "Bearer test_token".to_string());
        headers.insert("x-databricks-zerobus-table-name", "test_table".to_string());
        Ok(headers)
    }
}

/// Helper function to create a simple descriptor proto for testing.
pub fn create_test_descriptor_proto() -> Option<prost_types::DescriptorProto> {
    Some(prost_types::DescriptorProto {
        name: Some("TestMessage".to_string()),
        field: vec![
            prost_types::FieldDescriptorProto {
                name: Some("id".to_string()),
                number: Some(1),
                r#type: Some(prost_types::field_descriptor_proto::Type::Int64 as i32),
                ..Default::default()
            },
            prost_types::FieldDescriptorProto {
                name: Some("message".to_string()),
                number: Some(2),
                r#type: Some(prost_types::field_descriptor_proto::Type::String as i32),
                ..Default::default()
            },
        ],
        ..Default::default()
    })
}

/// Create a test Arrow schema matching the protobuf descriptor.
pub fn create_test_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("message", DataType::Utf8, true),
    ]))
}

/// Create a test RecordBatch with the given data.
pub fn create_test_record_batch(
    schema: Arc<ArrowSchema>,
    ids: Vec<i64>,
    messages: Vec<Option<&str>>,
) -> RecordBatch {
    let id_array = Int64Array::from(ids);
    let message_array = StringArray::from(messages);

    RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(message_array)])
        .expect("Failed to create RecordBatch")
}

/// Setup tracing for tests.
pub fn setup_tracing() {
    SETUP.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_writer(std::io::stdout)
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
            )
            .try_init()
            .ok();
    });
}
