//! Workaround for apache-avro's lossy handling of `fixed(16)` + `logicalType: "uuid"`.
//!
//! apache-avro collapses `fixed(16)` + `uuid` into `Schema::Uuid` and encodes it
//! as a 37-byte string, which a server expecting a 16-byte `fixed(16)` cannot
//! decode. Since the fixed backing is gone once parsed, we act on the JSON before
//! parsing: [`strip_fixed_uuid_logical_type`] makes such nodes parse as
//! `Schema::Fixed(16)`, and [`normalize_fixed_uuid`] adjusts the value so
//! `AvroValue::Uuid` still works. `string` + `uuid` is left untouched.

use crate::AvroValue;
use apache_avro::Schema;
use serde_json::Value as JsonValue;

/// Removes `logicalType: "uuid"` from every `fixed` node in the schema JSON, in place.
pub(crate) fn strip_fixed_uuid_logical_type(node: &mut JsonValue) {
    match node {
        JsonValue::Object(map) => {
            let is_fixed_uuid = map.get("type").and_then(JsonValue::as_str) == Some("fixed")
                && map.get("logicalType").and_then(JsonValue::as_str) == Some("uuid");
            if is_fixed_uuid {
                map.remove("logicalType");
            }
            for child in map.values_mut() {
                strip_fixed_uuid_logical_type(child);
            }
        }
        JsonValue::Array(items) => {
            for item in items.iter_mut() {
                strip_fixed_uuid_logical_type(item);
            }
        }
        _ => {}
    }
}

/// Rewrites `AvroValue::Uuid` to `AvroValue::Fixed(16, ..)` wherever the (stripped)
/// `schema` is a `fixed(16)`. Runs before `resolve`, so unresolved values are fine.
pub(crate) fn normalize_fixed_uuid(value: &mut AvroValue, schema: &Schema) {
    match schema {
        Schema::Fixed(fixed) if fixed.size == 16 => {
            if let AvroValue::Uuid(uuid) = value {
                let bytes = uuid.into_bytes().to_vec();
                *value = AvroValue::Fixed(16, bytes);
            }
        }
        Schema::Union(union) => match value {
            // Already wrapped: recurse into the selected branch.
            AvroValue::Union(index, inner) => {
                if let Some(branch) = union.variants().get(*index as usize) {
                    normalize_fixed_uuid(inner, branch);
                }
            }
            // Bare UUID (e.g. nullable field): convert if a fixed(16) branch exists.
            AvroValue::Uuid(uuid) => {
                let has_fixed16 = union
                    .variants()
                    .iter()
                    .any(|branch| matches!(branch, Schema::Fixed(f) if f.size == 16));
                if has_fixed16 {
                    let bytes = uuid.into_bytes().to_vec();
                    *value = AvroValue::Fixed(16, bytes);
                }
            }
            _ => {}
        },
        Schema::Record(record) => {
            if let AvroValue::Record(fields) = value {
                for (name, field_value) in fields.iter_mut() {
                    if let Some(field) = record.fields.iter().find(|f| &f.name == name) {
                        normalize_fixed_uuid(field_value, &field.schema);
                    }
                }
            }
        }
        Schema::Array(array) => {
            if let AvroValue::Array(items) = value {
                for item in items.iter_mut() {
                    normalize_fixed_uuid(item, &array.items);
                }
            }
        }
        Schema::Map(map) => {
            if let AvroValue::Map(entries) = value {
                for entry in entries.values_mut() {
                    normalize_fixed_uuid(entry, &map.types);
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::Uuid;

    const UUID_STR: &str = "550e8400-e29b-41d4-a716-446655440000";

    fn encode(schema_json: &str, value: AvroValue) -> Vec<u8> {
        let mut stripped: JsonValue = serde_json::from_str(schema_json).unwrap();
        strip_fixed_uuid_logical_type(&mut stripped);
        let schema = Schema::parse_str(&stripped.to_string()).unwrap();
        let mut value = value;
        normalize_fixed_uuid(&mut value, &schema);
        let resolved = value.resolve(&schema).unwrap();
        apache_avro::to_avro_datum(&schema, resolved).unwrap()
    }

    #[test]
    fn strip_removes_logical_type_only_from_fixed() {
        let mut schema: JsonValue = serde_json::from_str(
            r#"{
                "type": "record",
                "name": "R",
                "fields": [
                    {"name": "f", "type": {"type": "fixed", "size": 16, "name": "U", "logicalType": "uuid"}},
                    {"name": "s", "type": {"type": "string", "logicalType": "uuid"}}
                ]
            }"#,
        )
        .unwrap();
        strip_fixed_uuid_logical_type(&mut schema);

        let fields = schema["fields"].as_array().unwrap();
        // fixed node: logicalType removed, size/name kept.
        assert!(fields[0]["type"].get("logicalType").is_none());
        assert_eq!(fields[0]["type"]["size"], 16);
        // string node: logicalType preserved.
        assert_eq!(fields[1]["type"]["logicalType"], "uuid");
    }

    #[test]
    fn fixed_uuid_encodes_as_16_raw_bytes() {
        // fixed(16) + uuid must encode as 16 raw bytes, not the 37-byte string.
        let schema = r#"{"type": "fixed", "size": 16, "name": "U", "logicalType": "uuid"}"#;
        let uuid = Uuid::parse_str(UUID_STR).unwrap();
        let datum = encode(schema, AvroValue::Uuid(uuid));
        assert_eq!(datum, uuid.into_bytes().to_vec());
        assert_eq!(datum.len(), 16);
    }

    #[test]
    fn string_uuid_still_encodes_as_string() {
        // string + uuid is untouched and keeps its hyphenated-string encoding.
        let schema = r#"{"type": "string", "logicalType": "uuid"}"#;
        let uuid = Uuid::parse_str(UUID_STR).unwrap();
        let datum = encode(schema, AvroValue::Uuid(uuid));
        // 1-byte length prefix (0x48 = 36 << 1) + 36 ASCII chars.
        assert_eq!(datum.len(), 37);
        assert_eq!(&datum[1..], UUID_STR.as_bytes());
    }

    #[test]
    fn nullable_fixed_uuid_encodes_as_16_raw_bytes() {
        // ["null", fixed(16)+uuid] with a bare (unwrapped) Uuid value.
        let schema = r#"{
            "type": "record",
            "name": "R",
            "fields": [
                {"name": "id", "type": ["null", {"type": "fixed", "size": 16, "name": "U", "logicalType": "uuid"}]}
            ]
        }"#;
        let uuid = Uuid::parse_str(UUID_STR).unwrap();
        let record = AvroValue::Record(vec![("id".to_string(), AvroValue::Uuid(uuid))]);
        let datum = encode(schema, record);
        // union branch index 1 (long-encoded as 0x02) + 16 raw bytes.
        assert_eq!(datum.len(), 17);
        assert_eq!(datum[0], 2);
        assert_eq!(&datum[1..], uuid.into_bytes());
    }

    #[test]
    fn nullable_fixed_uuid_null_branch() {
        let schema = r#"{
            "type": "record",
            "name": "R",
            "fields": [
                {"name": "id", "type": ["null", {"type": "fixed", "size": 16, "name": "U", "logicalType": "uuid"}]}
            ]
        }"#;
        let record = AvroValue::Record(vec![("id".to_string(), AvroValue::Null)]);
        let datum = encode(schema, record);
        // union branch index 0 (null), no payload.
        assert_eq!(datum, vec![0]);
    }

    #[test]
    fn fixed_uuid_nested_in_record_and_array() {
        let schema = r#"{
            "type": "record",
            "name": "R",
            "fields": [
                {"name": "ids", "type": {"type": "array", "items": {"type": "fixed", "size": 16, "name": "U", "logicalType": "uuid"}}}
            ]
        }"#;
        let uuid = Uuid::parse_str(UUID_STR).unwrap();
        let record = AvroValue::Record(vec![(
            "ids".to_string(),
            AvroValue::Array(vec![AvroValue::Uuid(uuid), AvroValue::Uuid(uuid)]),
        )]);
        let datum = encode(schema, record);
        // Array framing: block count 2 (zigzag 0x04), then 2 x 16 raw bytes, then
        // the 0x00 end-of-blocks marker.
        assert_eq!(datum.len(), 1 + 32 + 1);
        assert_eq!(datum[0], 4);
        assert_eq!(&datum[1..17], uuid.into_bytes());
        assert_eq!(&datum[17..33], uuid.into_bytes());
        assert_eq!(datum[33], 0);
    }
}
