#[path = "../../../tests/src/mock_grpc.rs"]
mod mock_grpc;

use std::sync::Arc;

use databricks_zerobus_ingest_sdk::databricks::zerobus::{
    ingest_record_request::Record, RecordType,
};
use databricks_zerobus_ingest_sdk::{AvroRecord, AvroValue, NoTlsConfig, ZerobusSdk};
use mock_grpc::{start_mock_server, MockResponse};

const TABLE: &str = "avro.schema.multiplexed";
const AVRO_SCHEMA: &str = r#"{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "id", "type": "long"}
  ]
}"#;

#[tokio::test]
async fn multiplexed_avro_propagates_schema_and_ingests_across_lanes(
) -> Result<(), Box<dyn std::error::Error>> {
    let (mock_server, server_url) = start_mock_server().await?;
    mock_server
        .inject_responses(
            TABLE,
            vec![
                MockResponse::CreateStream {
                    stream_id: "avro-mux".to_string(),
                    delay_ms: 0,
                },
                // Each lane starts its own offset sequence at zero and consumes its own
                // clone of this scripted response list.
                MockResponse::RecordAck {
                    ack_up_to_offset: 0,
                    delay_ms: 0,
                },
            ],
        )
        .await;

    let sdk = ZerobusSdk::builder()
        .endpoint(&server_url)
        .unity_catalog_url("https://mock-uc.example")
        .tls_config(Arc::new(NoTlsConfig))
        .build()?;
    let mut stream = sdk
        .stream_builder()
        .table(TABLE)
        .no_auth()
        .avro(AVRO_SCHEMA)
        .max_inflight_requests(2)
        .multiplexed(2)
        .build()
        .await?;

    let create_requests = mock_server.get_create_requests().await;
    assert_eq!(create_requests.len(), 2);
    for request in create_requests {
        assert_eq!(request.record_type, Some(RecordType::Avro.into()));
        assert_eq!(request.avro_schema_json.as_deref(), Some(AVRO_SCHEMA));
    }

    for id in 0..2 {
        let record = AvroRecord(AvroValue::Record(vec![(
            "id".to_string(),
            AvroValue::Long(id),
        )]));
        let message_id = stream.ingest_record(record).await?;
        assert_eq!(message_id.stream_index(), id as usize);
    }
    stream.flush().await?;
    assert_eq!(mock_server.get_write_count().await, 2);
    let ingest_requests = mock_server.get_ingest_record_requests().await;
    assert_eq!(ingest_requests.len(), 2);
    assert!(ingest_requests
        .iter()
        .all(|request| matches!(request.record, Some(Record::AvroEncodedRecord(_)))));
    stream.close().await?;
    Ok(())
}
