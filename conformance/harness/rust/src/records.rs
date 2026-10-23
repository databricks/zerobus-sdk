//! Logical record -> SDK-native record conversion. The only place this harness
//! understands record bodies.
//!
//! Pipeline: `CreateStreamRequest.descriptor_proto_bytes` -> [`descriptor_from_bytes`]
//! -> `MessageDescriptor`; `IngestRecordRequest.record_json` + descriptor ->
//! [`record_from_json`] -> `DynamicMessage`; then [`sdk_input`] wraps it as
//! `ProtoMessage` so the SDK's own encoder serializes it on ingest.

use databricks_zerobus_ingest_sdk::{
    message_descriptor, AvroRecord, DynamicMessage, MessageDescriptor, ProtoMessage,
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

/// Parse the Avro writer schema (`CreateStreamRequest.avro_schema_json`).
pub fn avro_schema_from_json(schema_json: &str) -> Result<apache_avro::Schema, String> {
    apache_avro::Schema::parse_str(schema_json)
        .map_err(|e| format!("avro_schema_json is not a valid Avro schema: {e}"))
}

/// Parse a canonical JSON object (`record_json`) into an Avro value shaped by
/// `schema`. The SDK encodes this value against the same writer schema on ingest.
///
/// Rules (the logical-record JSON subset this harness understands):
/// - `int`/`long`/`float`/`double`/`boolean`/`string`/`null` map to their JSON forms.
/// - `bytes`/`fixed` take a JSON string whose chars are bytes (code points < 256).
/// - `enum` takes a symbol name; `array` a JSON array; `map` a JSON object.
/// - `record` takes a JSON object. Unknown keys are rejected; a missing field takes
///   its schema default, or is an error if it has none.
/// - `union` takes the bare value (no `{"type": ...}` wrapper). The first branch
///   that accepts the value wins, and JSON `null` selects a `null` branch.
/// - Logical types and named references are not supported yet and are rejected.
pub fn avro_value_from_json(
    schema: &apache_avro::Schema,
    record_json: &str,
) -> Result<apache_avro::types::Value, String> {
    let value: serde_json::Value = serde_json::from_str(record_json)
        .map_err(|e| format!("record_json is not valid JSON: {e}"))?;
    if !value.is_object() {
        return Err("record_json must be a JSON object".to_string());
    }
    json_to_avro(schema, &value, "$")
}

/// Wrap a converted Avro value as SDK input. The SDK encodes it against the writer
/// schema on ingest (`AvroRecord` -> `PreparedInput::AvroObject`).
pub fn avro_sdk_input(value: apache_avro::types::Value) -> AvroRecord {
    AvroRecord(value)
}

/// Schema-directed JSON -> Avro value. `path` names the position for error messages.
fn json_to_avro(
    schema: &apache_avro::Schema,
    json: &serde_json::Value,
    path: &str,
) -> Result<apache_avro::types::Value, String> {
    use apache_avro::types::Value;
    use apache_avro::Schema;
    use serde_json::Value as J;

    let mismatch = |want: &str| format!("{path}: expected {want}, got {json}");
    match schema {
        Schema::Null => match json {
            J::Null => Ok(Value::Null),
            _ => Err(mismatch("null")),
        },
        Schema::Boolean => json
            .as_bool()
            .map(Value::Boolean)
            .ok_or_else(|| mismatch("boolean")),
        Schema::Int => json
            .as_i64()
            .and_then(|v| i32::try_from(v).ok())
            .map(Value::Int)
            .ok_or_else(|| mismatch("int")),
        Schema::Long => json
            .as_i64()
            .map(Value::Long)
            .ok_or_else(|| mismatch("long")),
        Schema::Float => json
            .as_f64()
            .map(|v| Value::Float(v as f32))
            .ok_or_else(|| mismatch("float")),
        Schema::Double => json
            .as_f64()
            .map(Value::Double)
            .ok_or_else(|| mismatch("double")),
        Schema::String => json
            .as_str()
            .map(|s| Value::String(s.to_string()))
            .ok_or_else(|| mismatch("string")),
        Schema::Bytes => latin1_bytes(json, path).map(Value::Bytes),
        Schema::Fixed(fixed) => {
            let bytes = latin1_bytes(json, path)?;
            if bytes.len() != fixed.size {
                return Err(format!(
                    "{path}: fixed expects {} bytes, got {}",
                    fixed.size,
                    bytes.len()
                ));
            }
            Ok(Value::Fixed(fixed.size, bytes))
        }
        Schema::Enum(en) => {
            let symbol = json.as_str().ok_or_else(|| mismatch("enum symbol"))?;
            let index = en
                .symbols
                .iter()
                .position(|s| s == symbol)
                .ok_or_else(|| format!("{path}: '{symbol}' is not a symbol of the enum"))?;
            Ok(Value::Enum(index as u32, symbol.to_string()))
        }
        Schema::Array(array) => {
            let items = json.as_array().ok_or_else(|| mismatch("array"))?;
            items
                .iter()
                .enumerate()
                .map(|(i, item)| json_to_avro(&array.items, item, &format!("{path}[{i}]")))
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array)
        }
        Schema::Map(map) => {
            let entries = json.as_object().ok_or_else(|| mismatch("map"))?;
            entries
                .iter()
                .map(|(k, v)| {
                    json_to_avro(&map.types, v, &format!("{path}.{k}")).map(|v| (k.clone(), v))
                })
                .collect::<Result<_, _>>()
                .map(Value::Map)
        }
        Schema::Record(record) => {
            let object = json.as_object().ok_or_else(|| mismatch("record object"))?;
            if let Some(unknown) = object
                .keys()
                .find(|k| !record.fields.iter().any(|f| &f.name == *k))
            {
                return Err(format!("{path}: unknown field '{unknown}'"));
            }
            record
                .fields
                .iter()
                .map(|field| {
                    let field_path = format!("{path}.{}", field.name);
                    let value = match object.get(&field.name) {
                        Some(v) => json_to_avro(&field.schema, v, &field_path)?,
                        None => match &field.default {
                            // Schema defaults are JSON; convert them like any other value.
                            Some(default) => json_to_avro(&field.schema, default, &field_path)?,
                            None => return Err(format!("{field_path}: missing required field")),
                        },
                    };
                    Ok((field.name.clone(), value))
                })
                .collect::<Result<Vec<_>, String>>()
                .map(Value::Record)
        }
        Schema::Union(union) => {
            // First branch that accepts the value wins.
            for (index, branch) in union.variants().iter().enumerate() {
                if let Ok(value) = json_to_avro(branch, json, path) {
                    return Ok(Value::Union(index as u32, Box::new(value)));
                }
            }
            Err(format!("{path}: value matches no union branch: {json}"))
        }
        other => Err(format!(
            "{path}: schema type {other:?} is not supported by the harness yet"
        )),
    }
}

/// Avro JSON byte strings: each char is one byte (code point < 256).
fn latin1_bytes(json: &serde_json::Value, path: &str) -> Result<Vec<u8>, String> {
    let s = json
        .as_str()
        .ok_or_else(|| format!("{path}: expected a string of bytes, got {json}"))?;
    s.chars()
        .map(|c| u8::try_from(c as u32).map_err(|_| format!("{path}: char {c:?} is not a byte")))
        .collect()
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

    const SIMPLE_RECORD_AVSC: &str = r#"{
        "type": "record", "name": "SimpleRecord", "namespace": "zerobus.conformance.fixtures",
        "fields": [{"name": "id", "type": "long"}, {"name": "message", "type": "string"}]
    }"#;

    #[test]
    fn avro_json_encodes_to_golden_datum() {
        // Same datum the SDK's encoder must produce: long 1 -> zigzag 0x02,
        // string "hello" -> length 0x0a, then the bytes.
        let schema = avro_schema_from_json(SIMPLE_RECORD_AVSC).unwrap();
        let value = avro_value_from_json(&schema, r#"{"id": 1, "message": "hello"}"#).unwrap();
        let datum = apache_avro::writer::datum::GenericDatumWriter::builder(&schema)
            .build()
            .unwrap()
            .write_value_to_vec(value)
            .unwrap();
        assert_eq!(datum, vec![0x02, 0x0a, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn avro_unknown_field_and_wrong_type_are_rejected() {
        let schema = avro_schema_from_json(SIMPLE_RECORD_AVSC).unwrap();
        assert!(avro_value_from_json(&schema, r#"{"id": 1, "nope": 2, "message": "x"}"#).is_err());
        assert!(avro_value_from_json(&schema, r#"{"id": "1", "message": "x"}"#).is_err());
        assert!(avro_value_from_json(&schema, r#"{"id": 1}"#).is_err());
    }
}
