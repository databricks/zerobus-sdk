//! Logical record -> SDK-native record conversion. The only place this harness
//! understands record bodies.
//!
//! Pipeline: `CreateStreamRequest.descriptor_proto_bytes` -> [`descriptor_from_bytes`]
//! -> `MessageDescriptor`; `IngestRecordRequest.record_json` + descriptor ->
//! [`record_from_json`] -> `DynamicMessage`; then [`sdk_input`] wraps it as
//! `ProtoMessage` so the SDK's own encoder serializes it on ingest.

use databricks_zerobus_ingest_sdk::{
    message_descriptor, DynamicMessage, MessageDescriptor, ProtoMessage,
};
use prost::Message as _;
use prost_reflect::DeserializeOptions;

/// Decode a serialized `google.protobuf.DescriptorProto` and resolve it into a
/// dynamic message descriptor usable by the SDK.
pub fn descriptor_from_bytes(bytes: &[u8]) -> Result<MessageDescriptor, String> {
    let descriptor_proto = prost_types::DescriptorProto::decode(bytes)
        .map_err(|e| format!("descriptor_proto_bytes is not a DescriptorProto: {e}"))?;
    message_descriptor(&descriptor_proto).map_err(|e| e.to_string())
}

/// Parse a canonical protobuf-JSON object (`record_json`) into a `DynamicMessage`
/// of `descriptor`. Unknown fields are rejected (prost-reflect's default), so a
/// fixture typo fails loudly instead of being silently dropped.
pub fn record_from_json(
    descriptor: &MessageDescriptor,
    record_json: &str,
) -> Result<DynamicMessage, String> {
    let value: serde_json::Value = serde_json::from_str(record_json)
        .map_err(|e| format!("record_json is not valid JSON: {e}"))?;
    if !value.is_object() {
        return Err("record_json must be a JSON object".to_string());
    }
    DynamicMessage::deserialize_with_options(descriptor.clone(), value, &DeserializeOptions::new())
        .map_err(|e| format!("record_json does not match the schema: {e}"))
}

/// Wrap a parsed record as SDK input. The SDK serializes it with its own encoder.
pub fn sdk_input(message: DynamicMessage) -> ProtoMessage<DynamicMessage> {
    ProtoMessage(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_types::field_descriptor_proto::{Label, Type};
    use prost_types::{DescriptorProto, FieldDescriptorProto};

    /// `SimpleRecord { int64 id = 1; string message = 2; }`, the schema the scenarios use.
    fn simple_record() -> MessageDescriptor {
        let field = |name: &str, number: i32, ty: Type| FieldDescriptorProto {
            name: Some(name.to_string()),
            number: Some(number),
            r#type: Some(ty as i32),
            label: Some(Label::Optional as i32),
            ..Default::default()
        };
        let proto = DescriptorProto {
            name: Some("SimpleRecord".to_string()),
            field: vec![
                field("id", 1, Type::Int64),
                field("message", 2, Type::String),
            ],
            ..Default::default()
        };
        message_descriptor(&proto).expect("valid descriptor")
    }

    #[test]
    fn json_maps_to_proto_wire_bytes() {
        let desc = simple_record();
        let msg = record_from_json(&desc, r#"{"id": 1, "message": "hello"}"#).unwrap();
        // id=1 -> tag 0x08, varint 0x01; message="hello" -> tag 0x12, len 5, bytes.
        assert_eq!(
            msg.encode_to_vec(),
            vec![0x08, 0x01, 0x12, 0x05, b'h', b'e', b'l', b'l', b'o']
        );
    }

    #[test]
    fn unknown_field_is_rejected() {
        let desc = simple_record();
        assert!(record_from_json(&desc, r#"{"nope": 1}"#).is_err());
    }

    #[test]
    fn non_object_is_rejected() {
        let desc = simple_record();
        assert!(record_from_json(&desc, "[1, 2]").is_err());
    }
}
