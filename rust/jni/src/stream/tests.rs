use super::*;
use databricks_zerobus_ingest_sdk::{EncodedRecord, NoTlsConfig, ZerobusSdk};
use prost_types::{field_descriptor_proto, DescriptorProto, FieldDescriptorProto};

use zerobus_test_support::mock_grpc::{start_mock_server, MockResponse};

#[tokio::test]
async fn close_keeps_unacked_proto_records_available_for_java_recovery(
) -> Result<(), Box<dyn std::error::Error>> {
    const TABLE_NAME: &str = "test_catalog.test_schema.test_table";

    let (mock_server, server_url) = start_mock_server().await?;
    mock_server
        .inject_responses(
            TABLE_NAME,
            vec![
                MockResponse::CreateStream {
                    stream_id: "proto-stream".to_string(),
                    delay_ms: 0,
                },
                MockResponse::Error {
                    status: tonic::Status::invalid_argument("permanent failure"),
                    delay_ms: 50,
                },
            ],
        )
        .await;

    let sdk = ZerobusSdk::builder()
        .endpoint(server_url)
        .unity_catalog_url("https://mock-uc.com")
        .tls_config(Arc::new(NoTlsConfig))
        .build()?;
    let stream = sdk
        .stream_builder()
        .table(TABLE_NAME)
        .no_auth()
        .compiled_proto(DescriptorProto {
            name: Some("TestMessage".to_string()),
            field: vec![
                FieldDescriptorProto {
                    name: Some("id".to_string()),
                    number: Some(1),
                    r#type: Some(field_descriptor_proto::Type::Int64 as i32),
                    ..Default::default()
                },
                FieldDescriptorProto {
                    name: Some("message".to_string()),
                    number: Some(2),
                    r#type: Some(field_descriptor_proto::Type::String as i32),
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
        .recovery(false)
        .build()
        .await?;
    let record = vec![
        0x08, 0x01, 0x12, 0x07, b'u', b'n', b'a', b'c', b'k', b'e', b'd',
    ];

    stream.ingest_record_offset(record.clone()).await?;

    let handle = NativeStreamHandle::new(
        stream,
        databricks_zerobus_ingest_sdk::RecordType::Proto,
        String::new(),
        String::new(),
    );
    assert!(close_native_stream(&handle).await.is_err());

    let guard = handle.stream.lock().await;
    let closed_stream = guard
        .as_ref()
        .expect("native close must retain the Rust stream until Java retrieves unacked data");
    let unacked: Vec<_> = closed_stream.get_unacked_records().await?.collect();
    assert_eq!(unacked.len(), 1);
    assert!(matches!(&unacked[0], EncodedRecord::Proto(payload) if payload == &record));

    Ok(())
}

#[test]
fn encoded_record_from_maps_record_types() {
    use databricks_zerobus_ingest_sdk::RecordType;

    // Proto: bytes pass through.
    assert!(matches!(
        encoded_record_from(RecordType::Proto, vec![1, 2, 3]),
        Ok(EncodedRecord::Proto(b)) if b == vec![1, 2, 3]
    ));

    // JSON: valid UTF-8 becomes a Json string; invalid UTF-8 is an error.
    assert!(matches!(
        encoded_record_from(RecordType::Json, b"{}".to_vec()),
        Ok(EncodedRecord::Json(s)) if s == "{}"
    ));
    assert!(encoded_record_from(RecordType::Json, vec![0xff, 0xfe]).is_err());

    // Avro: bytes pass through when the feature is enabled.
    #[cfg(feature = "avro")]
    assert!(matches!(
        encoded_record_from(RecordType::Avro, vec![9]),
        Ok(EncodedRecord::Avro(b)) if b == vec![9]
    ));
}
