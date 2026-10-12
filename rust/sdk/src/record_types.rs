//! Record types and wrappers for the Zerobus SDK.
//!
//! This module contains all the types related to encoding records for ingestion:
//! - [`EncodedRecord`] - The core enum for encoded records (JSON, Protobuf, or Avro)
//! - [`EncodedBatch`] - A batch of encoded records, held as [`BatchRecords`]
//! - Wrapper types for ergonomic record creation:
//!   - [`ProtoBytes`] - For pre-serialized protobuf bytes (you handle serialization)
//!   - [`JsonString`] - For pre-serialized JSON strings (you handle serialization)
//!   - [`ProtoMessage`] - For protobuf messages (SDK handles serialization automatically)
//!   - [`JsonValue`] - For JSON-serializable objects (SDK handles serialization automatically)
//!   - [`AvroRecord`] - For an Avro value the SDK encodes against the writer schema (feature-gated)

use std::borrow::Borrow;
use std::fmt;
use std::iter::FusedIterator;
use std::ops::Deref;

use prost::Message;
use smallvec::{smallvec, SmallVec};

#[cfg(feature = "avro")]
use crate::databricks::zerobus::AvroRecordBatch;
use crate::databricks::zerobus::{
    ephemeral_stream_request::Payload as RequestPayload,
    ingest_record_batch_request::Batch as IngestRequestBatch,
    ingest_record_request::Record as IngestRequestRecord, IngestRecordBatchRequest,
    IngestRecordRequest, JsonRecordBatch, ProtoEncodedRecordBatch,
};
use crate::{OffsetId, RecordType, ZerobusError, ZerobusResult};

use crate::databricks::zerobus::persistent_stream_request::Payload as PersistentRequestPayload;

/// A type alias for a protobuf-encoded record.
pub type ProtoEncodedRecord = Vec<u8>;

/// A type alias for a JSON-encoded record.
pub type JsonEncodedRecord = String;

/// A type alias for an Avro-encoded record.
#[cfg(feature = "avro")]
pub type AvroEncodedRecord = Vec<u8>;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum EncodedRecord {
    Json(JsonEncodedRecord),
    Proto(ProtoEncodedRecord),
    #[cfg(feature = "avro")]
    Avro(AvroEncodedRecord),
}

impl From<ProtoEncodedRecord> for EncodedRecord {
    fn from(v: ProtoEncodedRecord) -> Self {
        EncodedRecord::Proto(v)
    }
}

impl From<JsonEncodedRecord> for EncodedRecord {
    fn from(s: JsonEncodedRecord) -> Self {
        EncodedRecord::Json(s)
    }
}

/// Wrapper for pre-serialized protobuf bytes.
///
/// Use this when you've already serialized the protobuf data yourself.
/// This is optional - you can also pass `Vec<u8>` directly.
///
/// # Examples
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::{ZerobusStream, ProtoBytes};
/// # async fn example(stream: &ZerobusStream) -> Result<(), Box<dyn std::error::Error>> {
/// let proto_bytes = vec![1, 2, 3, 4];
/// let _offset = stream.ingest_record_offset(ProtoBytes(proto_bytes)).await?;
/// // Ingest queues the record; flush() once when done waits for all pending acks.
/// stream.flush().await?;
/// # Ok(())
/// # }
/// ```
pub struct ProtoBytes(pub Vec<u8>);

impl From<ProtoBytes> for EncodedRecord {
    fn from(bytes: ProtoBytes) -> Self {
        EncodedRecord::Proto(bytes.0)
    }
}

/// Wrapper for pre-serialized JSON strings.
///
/// Use this when you've already serialized the JSON data yourself.
/// This is optional - you can also pass `String` directly.
///
/// # Examples
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::{ZerobusStream, JsonString};
/// # async fn example(stream: &ZerobusStream) -> Result<(), Box<dyn std::error::Error>> {
/// let json_str = r#"{"name":"test","value":42}"#.to_string();
/// let _offset = stream.ingest_record_offset(JsonString(json_str)).await?;
/// // Ingest queues the record; flush() once when done waits for all pending acks.
/// stream.flush().await?;
/// # Ok(())
/// # }
/// ```
pub struct JsonString(pub String);

impl From<JsonString> for EncodedRecord {
    fn from(s: JsonString) -> Self {
        EncodedRecord::Json(s.0)
    }
}

/// Wrapper for a pre-encoded Avro binary datum (Beta).
///
/// Encode against the stream's writer schema yourself; pass the raw bytes.
#[cfg(feature = "avro")]
pub struct AvroBytes(pub Vec<u8>);

#[cfg(feature = "avro")]
impl From<AvroBytes> for EncodedRecord {
    fn from(bytes: AvroBytes) -> Self {
        EncodedRecord::Avro(bytes.0)
    }
}

/// Wrapper for protobuf messages with automatic serialization.
///
/// Use this when you want the SDK to handle serialization for you.
/// Pass any protobuf message that implements `prost::Message` and it will be
/// automatically serialized to bytes.
///
/// # Examples
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::{ZerobusStream, ProtoMessage};
/// # async fn example(stream: &ZerobusStream, my_proto_msg: impl prost::Message) -> Result<(), Box<dyn std::error::Error>> {
/// // Ingest a protobuf message - it will be automatically serialized
/// let _offset = stream.ingest_record_offset(ProtoMessage(my_proto_msg)).await?;
/// // Ingest queues the record; flush() once when done waits for all pending acks.
/// stream.flush().await?;
/// # Ok(())
/// # }
/// ```
pub struct ProtoMessage<T: Message>(pub T);

impl<T: Message> From<ProtoMessage<T>> for EncodedRecord {
    fn from(msg: ProtoMessage<T>) -> Self {
        EncodedRecord::Proto(msg.0.encode_to_vec())
    }
}

/// Wrapper for JSON-serializable objects with automatic serialization.
///
/// Use this when you want the SDK to handle serialization for you.
/// Pass any Rust struct that implements `serde::Serialize` and it will be
/// automatically serialized to a JSON string.
///
/// # Examples
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::{ZerobusStream, JsonValue};
/// # use serde::Serialize;
/// # async fn example(stream: &ZerobusStream) -> Result<(), Box<dyn std::error::Error>> {
/// #[derive(Serialize)]
/// struct MyData {
///     name: String,
///     value: i32,
/// }
///
/// let my_data = MyData { name: "test".into(), value: 42 };
/// // Ingest a JSON object - it will be automatically serialized
/// let _offset = stream.ingest_record_offset(JsonValue(my_data)).await?;
/// // Ingest queues the record; flush() once when done waits for all pending acks.
/// stream.flush().await?;
/// # Ok(())
/// # }
/// ```
pub struct JsonValue<T: serde::Serialize>(pub T);

impl<T: serde::Serialize> From<JsonValue<T>> for EncodedRecord {
    fn from(obj: JsonValue<T>) -> Self {
        let json_string = serde_json::to_string(&obj.0).expect(
            "Failed to serialize to JSON - ensure your type implements serde::Serialize correctly",
        );
        EncodedRecord::Json(json_string)
    }
}

/// Wrapper for an Avro record the stream encodes against its writer schema (Beta).
///
/// Hold the record as an [`AvroValue`](crate::AvroValue) (`apache_avro`'s value type,
/// re-exported); the stream encodes it against the declared writer schema. `AvroValue`
/// can represent any Avro type, including unions, `fixed`, `decimal`, and logical types.
/// For data you have already encoded, use [`AvroBytes`](crate::AvroBytes).
///
/// # Examples
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::{ZerobusStream, AvroRecord, AvroValue};
/// # async fn example(stream: &ZerobusStream) -> Result<(), Box<dyn std::error::Error>> {
/// let record = AvroValue::Record(vec![
///     ("id".to_string(), AvroValue::Long(1)),
///     ("customer_name".to_string(), AvroValue::String("Alice".to_string())),
/// ]);
/// // The stream encodes the value against its writer schema.
/// let _offset = stream.ingest_record_offset(AvroRecord(record)).await?;
/// // Ingest queues the record; flush() once when done waits for all pending acks.
/// stream.flush().await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "avro")]
pub struct AvroRecord(pub apache_avro::types::Value);

/// Opaque payload accepted by the `ingest_*` methods; not meant to be named by callers
/// (hence `#[doc(hidden)]`).
///
/// A blanket `From<T: Into<EncodedRecord>>` keeps every existing wrapper working; only
/// `AvroRecord` maps to the `AvroObject` variant, encoded once the writer schema is known.
#[doc(hidden)]
#[non_exhaustive]
pub enum PreparedInput {
    /// Already encoded; enqueue as-is.
    Ready(EncodedRecord),
    /// An Avro value to encode against the stream's writer schema.
    #[cfg(feature = "avro")]
    AvroObject(apache_avro::types::Value),
}

impl<T: Into<EncodedRecord>> From<T> for PreparedInput {
    fn from(value: T) -> Self {
        PreparedInput::Ready(value.into())
    }
}

#[cfg(feature = "avro")]
impl From<AvroRecord> for PreparedInput {
    fn from(record: AvroRecord) -> Self {
        PreparedInput::AvroObject(record.0)
    }
}

/// Records of one encoding held by an [`EncodedBatch`].
///
/// `BatchRecords` is read-only. It derefs to `[T]`, iterates by value or by reference, and
/// converts to a `Vec` with [`into_vec`](Self::into_vec). To change the records, convert them
/// to a `Vec`, edit it, and convert it back with `.into()`.
///
/// # Examples
///
/// ```
/// use databricks_zerobus_ingest_sdk::{BatchRecords, EncodedBatch};
///
/// // A batch as returned by `get_unacked_batches()`.
/// let batch = EncodedBatch::Json(
///     vec![r#"{"id":1}"#.to_string(), r#"{"id":2}"#.to_string()].into(),
/// );
///
/// if let EncodedBatch::Json(records) = batch {
///     assert_eq!(records.len(), 2);
///     for record in &records {
///         assert!(record.starts_with('{'));
///     }
///
///     let mut edited = records.into_vec();
///     edited.truncate(1);
///     let records: BatchRecords<String> = edited.into();
///     assert_eq!(records[0], r#"{"id":1}"#);
/// }
/// ```
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BatchRecords<T>(SmallVec<[T; 1]>);

impl<T> BatchRecords<T> {
    /// Converts the records into a `Vec`.
    pub fn into_vec(self) -> Vec<T> {
        self.0.into_vec()
    }
}

impl<T> Deref for BatchRecords<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        &self.0
    }
}

impl<T> AsRef<[T]> for BatchRecords<T> {
    fn as_ref(&self) -> &[T] {
        self
    }
}

impl<T> Borrow<[T]> for BatchRecords<T> {
    fn borrow(&self) -> &[T] {
        self
    }
}

// Prints as a list, like the `SmallVec` the variants held before, so batches log the same.
impl<T: fmt::Debug> fmt::Debug for BatchRecords<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<T> From<Vec<T>> for BatchRecords<T> {
    fn from(records: Vec<T>) -> Self {
        Self(SmallVec::from_vec(records))
    }
}

impl<T> From<BatchRecords<T>> for Vec<T> {
    fn from(records: BatchRecords<T>) -> Self {
        records.into_vec()
    }
}

impl<T> FromIterator<T> for BatchRecords<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl<T> IntoIterator for BatchRecords<T> {
    type Item = T;
    type IntoIter = BatchRecordsIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        BatchRecordsIntoIter(self.0.into_iter())
    }
}

impl<'a, T> IntoIterator for &'a BatchRecords<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// Owning iterator over [`BatchRecords`].
#[derive(Clone)]
pub struct BatchRecordsIntoIter<T>(smallvec::IntoIter<[T; 1]>);

impl<T> BatchRecordsIntoIter<T> {
    /// Returns the records not yet yielded.
    pub fn as_slice(&self) -> &[T] {
        self.0.as_slice()
    }
}

impl<T: fmt::Debug> fmt::Debug for BatchRecordsIntoIter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("BatchRecordsIntoIter")
            .field(&self.as_slice())
            .finish()
    }
}

impl<T> Iterator for BatchRecordsIntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<T> DoubleEndedIterator for BatchRecordsIntoIter<T> {
    fn next_back(&mut self) -> Option<T> {
        self.0.next_back()
    }
}

impl<T> ExactSizeIterator for BatchRecordsIntoIter<T> {}

impl<T> FusedIterator for BatchRecordsIntoIter<T> {}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum EncodedBatch {
    Proto(BatchRecords<ProtoEncodedRecord>),
    Json(BatchRecords<JsonEncodedRecord>),
    #[cfg(feature = "avro")]
    Avro(BatchRecords<AvroEncodedRecord>),
}

impl EncodedBatch {
    /// Try to convert a single record into an encoded batch of the provided type.
    /// If the record type does not match the provided type, None is returned.
    pub(crate) fn try_from_record<T: Into<EncodedRecord>>(
        value: T,
        record_type: RecordType,
    ) -> Option<Self> {
        match (value.into(), record_type) {
            (EncodedRecord::Json(s), RecordType::Json) => {
                Some(EncodedBatch::Json(BatchRecords(smallvec![s])))
            }
            (EncodedRecord::Proto(v), RecordType::Proto) => {
                Some(EncodedBatch::Proto(BatchRecords(smallvec![v])))
            }
            #[cfg(feature = "avro")]
            (EncodedRecord::Avro(v), RecordType::Avro) => {
                Some(EncodedBatch::Avro(BatchRecords(smallvec![v])))
            }
            _ => None,
        }
    }

    /// Folds an iterator of already-encoded (and possibly fallible) records into a
    /// batch of the given type, in a single pass.
    ///
    /// Returns the first per-record error, or `InvalidArgument` if a record's encoding
    /// does not match `record_type`. An empty iterator yields an empty batch.
    pub(crate) fn try_from_encoded_iter<I>(iter: I, record_type: RecordType) -> ZerobusResult<Self>
    where
        I: IntoIterator<Item = ZerobusResult<EncodedRecord>>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        let size_hint = upper.unwrap_or(lower);
        let mismatch = || {
            ZerobusError::InvalidArgument(
                "Record type does not match stream configuration".to_string(),
            )
        };

        match record_type {
            RecordType::Json => {
                let mut vec = SmallVec::with_capacity(size_hint);
                for record in iter {
                    match record? {
                        EncodedRecord::Json(value) => vec.push(value),
                        _ => return Err(mismatch()),
                    }
                }
                Ok(EncodedBatch::Json(BatchRecords(vec)))
            }
            RecordType::Proto => {
                let mut vec = SmallVec::with_capacity(size_hint);
                for record in iter {
                    match record? {
                        EncodedRecord::Proto(value) => vec.push(value),
                        _ => return Err(mismatch()),
                    }
                }
                Ok(EncodedBatch::Proto(BatchRecords(vec)))
            }
            #[cfg(feature = "avro")]
            RecordType::Avro => {
                let mut vec = SmallVec::with_capacity(size_hint);
                for record in iter {
                    match record? {
                        EncodedRecord::Avro(value) => vec.push(value),
                        _ => return Err(mismatch()),
                    }
                }
                Ok(EncodedBatch::Avro(BatchRecords(vec)))
            }
        }
    }

    pub(crate) fn into_request_payload(self, offset_id: OffsetId) -> RequestPayload {
        match self {
            EncodedBatch::Proto(records) if records.len() == 1 => {
                RequestPayload::IngestRecord(IngestRecordRequest {
                    record: Some(IngestRequestRecord::ProtoEncodedRecord(
                        records.into_iter().next().unwrap(),
                    )),
                    offset_id: Some(offset_id),
                })
            }
            EncodedBatch::Proto(records) => {
                RequestPayload::IngestRecordBatch(IngestRecordBatchRequest {
                    batch: Some(IngestRequestBatch::ProtoEncodedBatch(
                        ProtoEncodedRecordBatch {
                            records: records.into_vec(),
                        },
                    )),
                    offset_id: Some(offset_id),
                })
            }
            EncodedBatch::Json(records) if records.len() == 1 => {
                RequestPayload::IngestRecord(IngestRecordRequest {
                    record: Some(IngestRequestRecord::JsonRecord(
                        records.into_iter().next().unwrap(),
                    )),
                    offset_id: Some(offset_id),
                })
            }
            EncodedBatch::Json(records) => {
                RequestPayload::IngestRecordBatch(IngestRecordBatchRequest {
                    batch: Some(IngestRequestBatch::JsonBatch(JsonRecordBatch {
                        records: records.into_vec(),
                    })),
                    offset_id: Some(offset_id),
                })
            }
            #[cfg(feature = "avro")]
            EncodedBatch::Avro(records) if records.len() == 1 => {
                RequestPayload::IngestRecord(IngestRecordRequest {
                    record: Some(IngestRequestRecord::AvroEncodedRecord(
                        records.into_iter().next().unwrap(),
                    )),
                    offset_id: Some(offset_id),
                })
            }
            #[cfg(feature = "avro")]
            EncodedBatch::Avro(records) => {
                RequestPayload::IngestRecordBatch(IngestRecordBatchRequest {
                    batch: Some(IngestRequestBatch::AvroBatch(AvroRecordBatch {
                        records: records.into_vec(),
                    })),
                    offset_id: Some(offset_id),
                })
            }
        }
    }

    pub(crate) fn into_persistent_request_payload(
        self,
        offset_id: OffsetId,
    ) -> PersistentRequestPayload {
        match self.into_request_payload(offset_id) {
            RequestPayload::IngestRecord(record) => PersistentRequestPayload::IngestRecord(record),
            RequestPayload::IngestRecordBatch(batch) => {
                PersistentRequestPayload::IngestRecordBatch(batch)
            }
            RequestPayload::CreateStream(_) => {
                unreachable!("encoded batches only produce ingest payloads")
            }
        }
    }

    /// Returns the number of records in this batch.
    pub fn get_record_count(&self) -> usize {
        match self {
            EncodedBatch::Proto(records) => records.len(),
            EncodedBatch::Json(records) => records.len(),
            #[cfg(feature = "avro")]
            EncodedBatch::Avro(records) => records.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.get_record_count() == 0
    }

    /// Returns the total encoded byte size of all records in this batch.
    pub fn total_byte_size(&self) -> usize {
        match self {
            EncodedBatch::Proto(records) => records.iter().map(|r| r.len()).sum(),
            EncodedBatch::Json(records) => records.iter().map(|s| s.len()).sum(),
            #[cfg(feature = "avro")]
            EncodedBatch::Avro(records) => records.iter().map(|r| r.len()).sum(),
        }
    }
}

impl IntoIterator for EncodedBatch {
    type Item = EncodedRecord;
    type IntoIter = EncodedBatchIter;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            EncodedBatch::Proto(records) => EncodedBatchIter::Proto(records.into_iter()),
            EncodedBatch::Json(records) => EncodedBatchIter::Json(records.into_iter()),
            #[cfg(feature = "avro")]
            EncodedBatch::Avro(records) => EncodedBatchIter::Avro(records.into_iter()),
        }
    }
}

#[non_exhaustive]
pub enum EncodedBatchIter {
    Proto(BatchRecordsIntoIter<ProtoEncodedRecord>),
    Json(BatchRecordsIntoIter<JsonEncodedRecord>),
    #[cfg(feature = "avro")]
    Avro(BatchRecordsIntoIter<AvroEncodedRecord>),
}

impl Iterator for EncodedBatchIter {
    type Item = EncodedRecord;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            EncodedBatchIter::Proto(iter) => iter.next().map(EncodedRecord::Proto),
            EncodedBatchIter::Json(iter) => iter.next().map(EncodedRecord::Json),
            #[cfg(feature = "avro")]
            EncodedBatchIter::Avro(iter) => iter.next().map(EncodedRecord::Avro),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            EncodedBatchIter::Proto(iter) => iter.size_hint(),
            EncodedBatchIter::Json(iter) => iter.size_hint(),
            #[cfg(feature = "avro")]
            EncodedBatchIter::Avro(iter) => iter.size_hint(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message as ProstMessage;
    use serde::Serialize;

    #[derive(Clone, PartialEq, ProstMessage)]
    struct TestMessage {
        #[prost(string, tag = "1")]
        name: String,
        #[prost(int32, tag = "2")]
        value: i32,
    }

    #[derive(Serialize, PartialEq, Debug)]
    struct TestData {
        name: String,
        value: i32,
    }

    mod encoded_record_conversions {
        use super::*;

        #[test]
        fn test_vec_u8_to_encoded_record() {
            let bytes = vec![1, 2, 3, 4, 5];
            let record: EncodedRecord = bytes.clone().into();

            match record {
                EncodedRecord::Proto(data) => assert_eq!(data, bytes),
                _ => panic!("Expected Proto variant"),
            }
        }

        #[test]
        fn test_proto_bytes_to_encoded_record() {
            let bytes = vec![1, 2, 3, 4, 5];
            let proto_bytes = ProtoBytes(bytes.clone());
            let record: EncodedRecord = proto_bytes.into();

            match record {
                EncodedRecord::Proto(data) => assert_eq!(data, bytes),
                _ => panic!("Expected Proto variant"),
            }
        }

        #[test]
        fn test_proto_message_to_encoded_record() {
            let message = TestMessage {
                name: "test".to_string(),
                value: 42,
            };
            let expected_bytes = message.encode_to_vec();

            let proto_message = ProtoMessage(message.clone());
            let record: EncodedRecord = proto_message.into();

            match record {
                EncodedRecord::Proto(data) => {
                    assert_eq!(data, expected_bytes);
                    let decoded = TestMessage::decode(&data[..]).unwrap();
                    assert_eq!(decoded, message);
                }
                _ => panic!("Expected Proto variant"),
            }
        }

        #[test]
        fn test_string_to_encoded_record() {
            let json_str = r#"{"name":"test","value":42}"#.to_string();
            let record: EncodedRecord = json_str.clone().into();

            match record {
                EncodedRecord::Json(data) => assert_eq!(data, json_str),
                _ => panic!("Expected Json variant"),
            }
        }

        #[test]
        fn test_json_string_to_encoded_record() {
            let json_str = r#"{"name":"test","value":42}"#.to_string();
            let json_string = JsonString(json_str.clone());
            let record: EncodedRecord = json_string.into();

            match record {
                EncodedRecord::Json(data) => assert_eq!(data, json_str),
                _ => panic!("Expected Json variant"),
            }
        }

        #[test]
        fn test_json_value_to_encoded_record() {
            let test_data = TestData {
                name: "test".to_string(),
                value: 42,
            };

            let json_value = JsonValue(test_data);
            let record: EncodedRecord = json_value.into();

            match record {
                EncodedRecord::Json(data) => {
                    let parsed: serde_json::Value = serde_json::from_str(&data).unwrap();
                    assert_eq!(parsed["name"], "test");
                    assert_eq!(parsed["value"], 42);
                }
                _ => panic!("Expected Json variant"),
            }
        }

        #[test]
        fn test_wrapper_types_are_zero_cost() {
            use std::mem::size_of;

            assert_eq!(size_of::<ProtoBytes>(), size_of::<Vec<u8>>());
            assert_eq!(size_of::<JsonString>(), size_of::<String>());
        }
    }

    #[cfg(feature = "avro")]
    mod avro {
        use super::*;

        #[test]
        fn avro_bytes_to_encoded_record() {
            let bytes = vec![1, 2, 3];
            match EncodedRecord::from(AvroBytes(bytes.clone())) {
                EncodedRecord::Avro(data) => assert_eq!(data, bytes),
                _ => panic!("Expected Avro variant"),
            }
        }

        #[test]
        fn try_from_record_matches_avro() {
            let batch =
                EncodedBatch::try_from_record(AvroBytes(vec![9]), RecordType::Avro).unwrap();
            assert!(matches!(batch, EncodedBatch::Avro(_)));
            // Wrong record_type yields None.
            assert!(EncodedBatch::try_from_record(AvroBytes(vec![9]), RecordType::Json).is_none());
        }

        #[test]
        fn single_avro_record_uses_avro_encoded_record() {
            let batch = EncodedBatch::Avro(vec![vec![1, 2, 3]].into());
            match batch.into_request_payload(7) {
                RequestPayload::IngestRecord(req) => {
                    assert_eq!(req.offset_id, Some(7));
                    assert!(matches!(
                        req.record,
                        Some(IngestRequestRecord::AvroEncodedRecord(_))
                    ));
                }
                _ => panic!("Expected IngestRecord"),
            }
        }

        #[test]
        fn multi_avro_records_use_avro_batch() {
            let records = vec![vec![1], vec![2]];
            let batch = EncodedBatch::Avro(records.clone().into());
            assert_eq!(batch.get_record_count(), 2);
            assert_eq!(batch.total_byte_size(), 2);
            match batch.into_request_payload(3) {
                RequestPayload::IngestRecordBatch(req) => match req.batch {
                    Some(IngestRequestBatch::AvroBatch(avro)) => assert_eq!(avro.records, records),
                    _ => panic!("Expected AvroBatch"),
                },
                _ => panic!("Expected IngestRecordBatch"),
            }
        }

        #[test]
        fn avro_record_becomes_prepared_avro_object() {
            let value = apache_avro::types::Value::Record(vec![
                ("id".to_string(), apache_avro::types::Value::Long(1)),
                (
                    "name".to_string(),
                    apache_avro::types::Value::String("Alice".to_string()),
                ),
            ]);
            match PreparedInput::from(AvroRecord(value.clone())) {
                PreparedInput::AvroObject(v) => assert_eq!(v, value),
                _ => panic!("Expected AvroObject variant"),
            }
        }

        #[test]
        fn avro_value_resolves_and_encodes_against_schema() {
            // The exhaustive object path: an AvroValue resolved against the writer schema
            // and encoded to a datum (the pipeline `encode_prepared` runs on the stream).
            let schema_str = r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;
            let schema = apache_avro::Schema::parse_str(schema_str).unwrap();
            let value = apache_avro::types::Value::Record(vec![
                ("id".to_string(), apache_avro::types::Value::Long(123)),
                (
                    "name".to_string(),
                    apache_avro::types::Value::String("Alice".to_string()),
                ),
            ]);

            let resolved = value.resolve(&schema).unwrap();
            let datum = apache_avro::writer::datum::GenericDatumWriter::builder(&schema)
                .build()
                .unwrap()
                .write_value_to_vec(resolved)
                .unwrap();
            assert!(!datum.is_empty());
        }

        #[test]
        fn avro_fixed_uuid_encodes_as_16_raw_bytes() {
            // Regression for the fixed(16)+uuid encoding bug: apache-avro <= 0.21 collapsed
            // fixed(16)+uuid into a string and emitted 37 bytes. 0.22 preserves the fixed
            // backing, so `AvroValue::Uuid` encodes as 16 raw bytes.
            let schema_str = r#"{"type":"fixed","size":16,"name":"U","logicalType":"uuid"}"#;
            let schema = apache_avro::Schema::parse_str(schema_str).unwrap();
            let uuid =
                apache_avro::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
            let value = apache_avro::types::Value::Uuid(uuid);

            let resolved = value.resolve(&schema).unwrap();
            let datum = apache_avro::writer::datum::GenericDatumWriter::builder(&schema)
                .build()
                .unwrap()
                .write_value_to_vec(resolved)
                .unwrap();
            assert_eq!(datum, uuid.into_bytes().to_vec());
            assert_eq!(datum.len(), 16);
        }

        #[test]
        fn test_avro_bytes_becomes_ready_via_blanket_impl() {
            let bytes = vec![1, 2, 3, 4, 5];

            match PreparedInput::from(AvroBytes(bytes.clone())) {
                PreparedInput::Ready(EncodedRecord::Avro(data)) => assert_eq!(data, bytes),
                _ => panic!("Expected Ready(Avro) variant"),
            }
        }
    }

    mod encoded_batch_try_from {
        use super::*;

        /// Adapts an iterator of infallible records into the fallible
        /// [`EncodedBatch::try_from_encoded_iter`] API these tests exercise.
        fn from_batch<B, R>(batch: B, record_type: RecordType) -> ZerobusResult<EncodedBatch>
        where
            B: IntoIterator<Item = R>,
            R: Into<EncodedRecord>,
        {
            EncodedBatch::try_from_encoded_iter(
                batch.into_iter().map(|r| Ok(r.into())),
                record_type,
            )
        }

        #[test]
        fn test_try_from_record_json_with_json_type() {
            let json_str = r#"{"id": 1}"#.to_string();
            let batch = EncodedBatch::try_from_record(json_str.clone(), RecordType::Json);

            assert!(batch.is_some());
            let batch = batch.unwrap();
            assert_eq!(batch.get_record_count(), 1);
            match batch {
                EncodedBatch::Json(records) => assert_eq!(records[0], json_str),
                _ => panic!("Expected Json batch"),
            }
        }

        #[test]
        fn test_try_from_record_keeps_single_record_inline() {
            let batch = EncodedBatch::try_from_record(vec![1u8, 2, 3], RecordType::Proto).unwrap();
            match batch {
                EncodedBatch::Proto(records) => assert!(!records.0.spilled()),
                _ => panic!("Expected Proto batch"),
            }
        }

        #[test]
        fn test_try_from_record_proto_with_proto_type() {
            let bytes = vec![1, 2, 3];
            let batch = EncodedBatch::try_from_record(bytes.clone(), RecordType::Proto);

            assert!(batch.is_some());
            let batch = batch.unwrap();
            assert_eq!(batch.get_record_count(), 1);
            match batch {
                EncodedBatch::Proto(records) => assert_eq!(records[0], bytes),
                _ => panic!("Expected Proto batch"),
            }
        }

        #[test]
        fn test_try_from_record_json_with_proto_type_fails() {
            let json_str = r#"{"id": 1}"#.to_string();
            let batch = EncodedBatch::try_from_record(json_str, RecordType::Proto);

            assert!(batch.is_none());
        }

        #[test]
        fn test_try_from_record_proto_with_json_type_fails() {
            let bytes = vec![1, 2, 3];
            let batch = EncodedBatch::try_from_record(bytes, RecordType::Json);

            assert!(batch.is_none());
        }

        #[test]
        fn test_try_from_record_with_json_string_wrapper() {
            let json_str = JsonString(r#"{"id": 1}"#.to_string());
            let batch = EncodedBatch::try_from_record(json_str, RecordType::Json);

            assert!(batch.is_some());
            assert_eq!(batch.unwrap().get_record_count(), 1);
        }

        #[test]
        fn test_try_from_record_with_proto_bytes_wrapper() {
            let proto_bytes = ProtoBytes(vec![1, 2, 3]);
            let batch = EncodedBatch::try_from_record(proto_bytes, RecordType::Proto);

            assert!(batch.is_some());
            assert_eq!(batch.unwrap().get_record_count(), 1);
        }

        #[test]
        fn test_try_from_record_with_json_value_wrapper() {
            let test_data = TestData {
                name: "test".to_string(),
                value: 42,
            };
            let batch = EncodedBatch::try_from_record(JsonValue(test_data), RecordType::Json);

            assert!(batch.is_some());
            let batch = batch.unwrap();
            match batch {
                EncodedBatch::Json(records) => {
                    let parsed: serde_json::Value = serde_json::from_str(&records[0]).unwrap();
                    assert_eq!(parsed["name"], "test");
                }
                _ => panic!("Expected Json batch"),
            }
        }

        #[test]
        fn test_try_from_record_with_proto_message_wrapper() {
            let message = TestMessage {
                name: "test".to_string(),
                value: 42,
            };
            let batch =
                EncodedBatch::try_from_record(ProtoMessage(message.clone()), RecordType::Proto);

            assert!(batch.is_some());
            let batch = batch.unwrap();
            match batch {
                EncodedBatch::Proto(records) => {
                    let decoded = TestMessage::decode(&records[0][..]).unwrap();
                    assert_eq!(decoded, message);
                }
                _ => panic!("Expected Proto batch"),
            }
        }

        #[test]
        fn test_try_from_batch_json_records() {
            let records = vec![
                r#"{"id": 1}"#.to_string(),
                r#"{"id": 2}"#.to_string(),
                r#"{"id": 3}"#.to_string(),
            ];
            let batch = from_batch(records.clone(), RecordType::Json);

            assert!(batch.is_ok());
            let batch = batch.unwrap();
            assert_eq!(batch.get_record_count(), 3);
            match batch {
                EncodedBatch::Json(batch_records) => {
                    assert_eq!(&batch_records[..], records.as_slice());
                }
                _ => panic!("Expected Json batch"),
            }
        }

        #[test]
        fn test_try_from_batch_proto_records() {
            let records = vec![vec![1, 2], vec![3, 4], vec![5, 6]];
            let batch = from_batch(records.clone(), RecordType::Proto);

            assert!(batch.is_ok());
            let batch = batch.unwrap();
            assert_eq!(batch.get_record_count(), 3);
            match batch {
                EncodedBatch::Proto(batch_records) => {
                    assert_eq!(&batch_records[..], records.as_slice());
                }
                _ => panic!("Expected Proto batch"),
            }
        }

        #[test]
        fn test_try_from_batch_empty() {
            let records: Vec<String> = vec![];
            let batch = from_batch(records, RecordType::Json);

            assert!(batch.is_ok());
            let batch = batch.unwrap();
            assert!(batch.is_empty());
        }

        #[test]
        fn test_try_from_batch_json_with_proto_type_fails() {
            let records = vec![r#"{"id": 1}"#.to_string(), r#"{"id": 2}"#.to_string()];
            let batch = from_batch(records, RecordType::Proto);

            assert!(batch.is_err());
        }

        #[test]
        fn test_try_from_batch_proto_with_json_type_fails() {
            let records = vec![vec![1, 2], vec![3, 4]];
            let batch = from_batch(records, RecordType::Json);

            assert!(batch.is_err());
        }

        #[test]
        fn test_try_from_batch_with_json_string_wrappers() {
            let records = vec![
                JsonString(r#"{"id": 1}"#.to_string()),
                JsonString(r#"{"id": 2}"#.to_string()),
            ];
            let batch = from_batch(records, RecordType::Json);

            assert!(batch.is_ok());
            assert_eq!(batch.unwrap().get_record_count(), 2);
        }

        #[test]
        fn test_try_from_batch_with_proto_bytes_wrappers() {
            let records = vec![ProtoBytes(vec![1, 2]), ProtoBytes(vec![3, 4])];
            let batch = from_batch(records, RecordType::Proto);

            assert!(batch.is_ok());
            assert_eq!(batch.unwrap().get_record_count(), 2);
        }

        #[test]
        fn test_try_from_batch_with_json_value_wrappers() {
            let records = vec![
                JsonValue(TestData {
                    name: "a".to_string(),
                    value: 1,
                }),
                JsonValue(TestData {
                    name: "b".to_string(),
                    value: 2,
                }),
            ];
            let batch = from_batch(records, RecordType::Json);

            assert!(batch.is_ok());
            assert_eq!(batch.unwrap().get_record_count(), 2);
        }

        #[test]
        fn test_try_from_batch_with_proto_message_wrappers() {
            let records = vec![
                ProtoMessage(TestMessage {
                    name: "a".to_string(),
                    value: 1,
                }),
                ProtoMessage(TestMessage {
                    name: "b".to_string(),
                    value: 2,
                }),
            ];
            let batch = from_batch(records, RecordType::Proto);

            assert!(batch.is_ok());
            assert_eq!(batch.unwrap().get_record_count(), 2);
        }
    }

    mod encoded_batch_methods {
        use super::*;

        #[test]
        fn test_get_record_count() {
            let proto_batch = EncodedBatch::Proto(vec![vec![1], vec![2], vec![3]].into());
            assert_eq!(proto_batch.get_record_count(), 3);

            let json_batch = EncodedBatch::Json(vec!["a".to_string(), "b".to_string()].into());
            assert_eq!(json_batch.get_record_count(), 2);

            let empty_batch = EncodedBatch::Proto(vec![].into());
            assert_eq!(empty_batch.get_record_count(), 0);
        }

        #[test]
        fn test_total_byte_size_proto() {
            let batch = EncodedBatch::Proto(vec![vec![1, 2, 3], vec![4, 5]].into());
            assert_eq!(batch.total_byte_size(), 5);
        }

        #[test]
        fn test_total_byte_size_json() {
            let batch = EncodedBatch::Json(vec!["hello".to_string(), "world!".to_string()].into());
            assert_eq!(batch.total_byte_size(), 11);
        }

        #[test]
        fn test_total_byte_size_empty() {
            let batch = EncodedBatch::Proto(vec![].into());
            assert_eq!(batch.total_byte_size(), 0);
        }

        #[test]
        fn test_is_empty() {
            let non_empty = EncodedBatch::Proto(vec![vec![1]].into());
            assert!(!non_empty.is_empty());

            let empty = EncodedBatch::Json(vec![].into());
            assert!(empty.is_empty());
        }

        #[test]
        fn test_into_request_payload_single_proto_record() {
            let record = vec![1, 2, 3];
            let batch = EncodedBatch::Proto(vec![record.clone()].into());
            let payload = batch.into_request_payload(42);

            match payload {
                RequestPayload::IngestRecord(req) => {
                    assert_eq!(req.offset_id, Some(42));
                    match req.record {
                        Some(IngestRequestRecord::ProtoEncodedRecord(data)) => {
                            assert_eq!(data, record);
                        }
                        _ => panic!("Expected ProtoEncodedRecord"),
                    }
                }
                _ => panic!("Expected IngestRecord payload"),
            }
        }

        #[test]
        fn test_into_request_payload_single_json_record() {
            let record = r#"{"id": 1}"#.to_string();
            let batch = EncodedBatch::Json(vec![record.clone()].into());
            let payload = batch.into_request_payload(123);

            match payload {
                RequestPayload::IngestRecord(req) => {
                    assert_eq!(req.offset_id, Some(123));
                    match req.record {
                        Some(IngestRequestRecord::JsonRecord(data)) => {
                            assert_eq!(data, record);
                        }
                        _ => panic!("Expected JsonRecord"),
                    }
                }
                _ => panic!("Expected IngestRecord payload"),
            }
        }

        #[test]
        fn test_into_request_payload_batch_proto() {
            let records = vec![vec![1, 2, 3], vec![4, 5, 6]];
            let batch = EncodedBatch::Proto(records.clone().into());
            let payload = batch.into_request_payload(99);

            match payload {
                RequestPayload::IngestRecordBatch(req) => {
                    assert_eq!(req.offset_id, Some(99));
                    match req.batch {
                        Some(IngestRequestBatch::ProtoEncodedBatch(proto_batch)) => {
                            assert_eq!(proto_batch.records, records);
                        }
                        _ => panic!("Expected ProtoEncodedBatch"),
                    }
                }
                _ => panic!("Expected IngestRecordBatch payload"),
            }
        }

        #[test]
        fn test_into_request_payload_batch_json() {
            let records = vec![r#"{"id": 1}"#.to_string(), r#"{"id": 2}"#.to_string()];
            let batch = EncodedBatch::Json(records.clone().into());
            let payload = batch.into_request_payload(77);

            match payload {
                RequestPayload::IngestRecordBatch(req) => {
                    assert_eq!(req.offset_id, Some(77));
                    match req.batch {
                        Some(IngestRequestBatch::JsonBatch(json_batch)) => {
                            assert_eq!(json_batch.records, records);
                        }
                        _ => panic!("Expected JsonBatch"),
                    }
                }
                _ => panic!("Expected IngestRecordBatch payload"),
            }
        }

        #[test]
        fn test_into_persistent_request_payload() {
            let record = r#"{"id": 1}"#.to_string();
            let batch = EncodedBatch::Json(vec![record.clone()].into());

            match batch.into_persistent_request_payload(42) {
                PersistentRequestPayload::IngestRecord(req) => {
                    assert_eq!(req.offset_id, Some(42));
                    assert_eq!(req.record, Some(IngestRequestRecord::JsonRecord(record)));
                }
                _ => panic!("Expected persistent IngestRecord payload"),
            }
        }
    }

    mod encoded_batch_iter {
        use super::*;

        #[test]
        fn test_iter_proto_batch() {
            let records = vec![vec![1, 2], vec![3, 4], vec![5, 6]];
            let batch = EncodedBatch::Proto(records.clone().into());

            let collected: Vec<EncodedRecord> = batch.into_iter().collect();
            assert_eq!(collected.len(), 3);

            for (i, record) in collected.iter().enumerate() {
                match record {
                    EncodedRecord::Proto(data) => assert_eq!(data, &records[i]),
                    _ => panic!("Expected Proto variant"),
                }
            }
        }

        #[test]
        fn test_iter_json_batch() {
            let records = vec!["a".to_string(), "b".to_string(), "c".to_string()];
            let batch = EncodedBatch::Json(records.clone().into());

            let collected: Vec<EncodedRecord> = batch.into_iter().collect();
            assert_eq!(collected.len(), 3);

            for (i, record) in collected.iter().enumerate() {
                match record {
                    EncodedRecord::Json(data) => assert_eq!(data, &records[i]),
                    _ => panic!("Expected Json variant"),
                }
            }
        }

        #[test]
        fn test_iter_empty_batch() {
            let batch = EncodedBatch::Proto(vec![].into());
            let collected: Vec<EncodedRecord> = batch.into_iter().collect();
            assert!(collected.is_empty());
        }

        #[test]
        fn test_iter_size_hint() {
            let batch = EncodedBatch::Proto(vec![vec![1], vec![2], vec![3]].into());
            let iter = batch.into_iter();
            assert_eq!(iter.size_hint(), (3, Some(3)));
        }

        #[test]
        fn test_iter_size_hint_decreases() {
            let batch = EncodedBatch::Json(vec!["a".to_string(), "b".to_string()].into());
            let mut iter = batch.into_iter();

            assert_eq!(iter.size_hint(), (2, Some(2)));
            iter.next();
            assert_eq!(iter.size_hint(), (1, Some(1)));
            iter.next();
            assert_eq!(iter.size_hint(), (0, Some(0)));
        }
    }

    mod batch_records {
        use super::*;

        #[test]
        fn test_conversions_and_slice_access() {
            let from_vec: BatchRecords<String> = vec!["a".to_string(), "b".to_string()].into();
            let collected: BatchRecords<String> =
                ["a", "b"].iter().map(|s| s.to_string()).collect();
            assert_eq!(from_vec, collected);
            assert_eq!(from_vec.len(), 2);
            assert_eq!(from_vec[1], "b");
            assert_eq!((&from_vec).into_iter().count(), 2);
            assert_eq!(
                Vec::from(from_vec.clone()),
                vec!["a".to_string(), "b".to_string()]
            );
            assert_eq!(from_vec.into_vec(), vec!["a".to_string(), "b".to_string()]);
        }

        #[test]
        fn test_slice_traits() {
            fn len_of<R: AsRef<[String]>>(records: R) -> usize {
                records.as_ref().len()
            }

            let records: BatchRecords<String> = vec!["a".to_string(), "b".to_string()].into();
            assert_eq!(len_of(&records), 2);

            // Borrow<[T]> lookups only work if Hash and Eq agree with the slice's.
            let set = std::collections::HashSet::from([records]);
            assert!(set.contains(&["a".to_string(), "b".to_string()][..]));
        }

        #[test]
        fn test_debug_prints_records() {
            let batch = EncodedBatch::Proto(vec![vec![1u8, 2], vec![3]].into());
            assert_eq!(format!("{batch:?}"), "Proto([[1, 2], [3]])");

            let mut iter = BatchRecords::from(vec![1, 2, 3]).into_iter();
            iter.next();
            assert_eq!(format!("{iter:?}"), "BatchRecordsIntoIter([2, 3])");
        }

        #[test]
        fn test_owning_iterator() {
            let records: BatchRecords<Vec<u8>> = vec![vec![1], vec![2], vec![3]].into();
            let mut iter = records.into_iter();
            assert_eq!(iter.len(), 3);
            assert_eq!(iter.next_back(), Some(vec![3]));
            assert_eq!(iter.next(), Some(vec![1]));
            assert_eq!(iter.as_slice(), [vec![2]]);
            assert_eq!(iter.clone().collect::<Vec<_>>(), vec![vec![2]]);
            assert_eq!(iter.collect::<Vec<_>>(), vec![vec![2]]);
        }
    }

    mod batch_conversions {
        use super::*;

        #[test]
        fn test_batch_with_proto_messages() {
            let msg1 = TestMessage {
                name: "msg1".to_string(),
                value: 1,
            };
            let msg2 = TestMessage {
                name: "msg2".to_string(),
                value: 2,
            };

            let batch: Vec<ProtoMessage<TestMessage>> =
                vec![ProtoMessage(msg1.clone()), ProtoMessage(msg2.clone())];

            let records: Vec<EncodedRecord> = batch.into_iter().map(|m| m.into()).collect();

            assert_eq!(records.len(), 2);
            for (i, record) in records.iter().enumerate() {
                match record {
                    EncodedRecord::Proto(_) => {}
                    _ => panic!("Expected Proto variant at index {}", i),
                }
            }
        }

        #[test]
        fn test_batch_with_json_values() {
            let data1 = TestData {
                name: "data1".to_string(),
                value: 1,
            };
            let data2 = TestData {
                name: "data2".to_string(),
                value: 2,
            };

            let batch: Vec<JsonValue<TestData>> = vec![JsonValue(data1), JsonValue(data2)];

            let records: Vec<EncodedRecord> = batch.into_iter().map(|m| m.into()).collect();

            assert_eq!(records.len(), 2);
            for (i, record) in records.iter().enumerate() {
                match record {
                    EncodedRecord::Json(data) => {
                        let parsed: serde_json::Value = serde_json::from_str(data).unwrap();
                        assert!(parsed["name"].as_str().unwrap().starts_with("data"));
                    }
                    _ => panic!("Expected Json variant at index {}", i),
                }
            }
        }

        #[test]
        fn test_batch_with_proto_bytes() {
            let bytes1 = vec![1, 2, 3];
            let bytes2 = vec![4, 5, 6];

            let batch = vec![ProtoBytes(bytes1.clone()), ProtoBytes(bytes2.clone())];
            let records: Vec<EncodedRecord> = batch.into_iter().map(|b| b.into()).collect();

            assert_eq!(records.len(), 2);
            match &records[0] {
                EncodedRecord::Proto(data) => assert_eq!(data, &bytes1),
                _ => panic!("Expected Proto variant"),
            }
            match &records[1] {
                EncodedRecord::Proto(data) => assert_eq!(data, &bytes2),
                _ => panic!("Expected Proto variant"),
            }
        }

        #[test]
        fn test_batch_with_json_strings() {
            let json1 = r#"{"id":1}"#.to_string();
            let json2 = r#"{"id":2}"#.to_string();

            let batch = vec![JsonString(json1.clone()), JsonString(json2.clone())];
            let records: Vec<EncodedRecord> = batch.into_iter().map(|s| s.into()).collect();

            assert_eq!(records.len(), 2);
            match &records[0] {
                EncodedRecord::Json(data) => assert_eq!(data, &json1),
                _ => panic!("Expected Json variant"),
            }
            match &records[1] {
                EncodedRecord::Json(data) => assert_eq!(data, &json2),
                _ => panic!("Expected Json variant"),
            }
        }

        #[test]
        fn test_batch_backward_compat_vec_u8() {
            let bytes1 = vec![1, 2, 3];
            let bytes2 = vec![4, 5, 6];

            let batch: Vec<Vec<u8>> = vec![bytes1.clone(), bytes2.clone()];
            let records: Vec<EncodedRecord> = batch.into_iter().map(|b| b.into()).collect();

            assert_eq!(records.len(), 2);
            match &records[0] {
                EncodedRecord::Proto(data) => assert_eq!(data, &bytes1),
                _ => panic!("Expected Proto variant"),
            }
            match &records[1] {
                EncodedRecord::Proto(data) => assert_eq!(data, &bytes2),
                _ => panic!("Expected Proto variant"),
            }
        }

        #[test]
        fn test_batch_backward_compat_string() {
            let json1 = r#"{"id":1}"#.to_string();
            let json2 = r#"{"id":2}"#.to_string();

            let batch: Vec<String> = vec![json1.clone(), json2.clone()];
            let records: Vec<EncodedRecord> = batch.into_iter().map(|s| s.into()).collect();

            assert_eq!(records.len(), 2);
            match &records[0] {
                EncodedRecord::Json(data) => assert_eq!(data, &json1),
                _ => panic!("Expected Json variant"),
            }
            match &records[1] {
                EncodedRecord::Json(data) => assert_eq!(data, &json2),
                _ => panic!("Expected Json variant"),
            }
        }
    }
}
