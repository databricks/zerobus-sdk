# Version changelog

## Release v0.2.0

### Native Rust Backend (JNI Migration)
- The SDK now uses JNI (Java Native Interface) to call the Zerobus Rust SDK instead of pure Java gRPC calls
- Native library is automatically loaded from the classpath or system library path
- Token management and background processing handled by native code

### New Stream Classes

**ZerobusProtoStream** - Protocol Buffer ingestion with method-level generics:
- `ingestRecordOffset(T record)` - Auto-encoded: SDK encodes Message to bytes
- `ingestRecordOffset(byte[] bytes)` - Pre-encoded: User provides encoded bytes
- `ingestRecordsOffset(List<T> records)` - Batch auto-encoded ingestion
- `ingestRecordsOffset(List<byte[]> bytes)` - Batch pre-encoded ingestion
- `getUnackedRecords()` - Returns `List<byte[]>` of unacked records
- `getUnackedRecords(Parser<T>)` - Returns parsed `List<T>` of unacked records
- `getUnackedBatches()` - Returns `List<EncodedBatch>` preserving batch grouping

**ZerobusJsonStream** - JSON ingestion without Protocol Buffer dependency:
- `ingestRecordOffset(String json)` - Pre-serialized: User provides JSON string
- `ingestRecordOffset(T obj, JsonSerializer<T>)` - Auto-serialized: SDK serializes object
- `ingestRecordsOffset(List<String> jsons)` - Batch pre-serialized ingestion
- `ingestRecordsOffset(List<T> objs, JsonSerializer<T>)` - Batch auto-serialized ingestion
- `getUnackedRecords()` - Returns `List<String>` of unacked JSON records
- `getUnackedRecords(JsonDeserializer<T>)` - Returns parsed `List<T>` of unacked records
- `getUnackedBatches()` - Returns `List<EncodedBatch>` preserving batch grouping

### New Factory Methods

- `ZerobusSdk.createProtoStream(tableName, descriptorProto, clientId, clientSecret)` - Create proto stream
- `ZerobusSdk.createProtoStream(tableName, descriptorProto, clientId, clientSecret, options)` - With options
- `ZerobusSdk.createJsonStream(tableName, clientId, clientSecret)` - Create JSON stream
- `ZerobusSdk.createJsonStream(tableName, clientId, clientSecret, options)` - With options
- `ZerobusSdk.recreateStream(ZerobusProtoStream)` - Recreate proto stream with unacked record re-ingestion
- `ZerobusSdk.recreateStream(ZerobusJsonStream)` - Recreate JSON stream with unacked record re-ingestion
- `ZerobusSdk.recreateStream(ZerobusStream)` - Recreate legacy stream (deprecated)

### New Supporting Types

- `BaseZerobusStream` - Abstract base class with native JNI methods
- `JsonSerializer<T>` - Functional interface for object-to-JSON serialization
- `JsonDeserializer<T>` - Functional interface for JSON-to-object deserialization
- `EncodedBatch` - Represents a batch of encoded records for recovery
- `AckCallback` - Callback interface with `onAck(long)` and `onError(long, String)`

### Deprecated (Backward Compatible)

**ZerobusStream<T>** - Use `ZerobusProtoStream` instead:
- `ingestRecord(T record)` - Returns `CompletableFuture<Void>`, use `ingestRecordOffset()` instead
- `getStreamId()` - No longer exposed by native backend, returns empty string
- `getState()` - Returns `OPENED` or `CLOSED` only
- `getUnackedRecords()` - **Breaking:** Returns empty iterator (records stored in native, type erasure prevents deserialization). Use `ZerobusProtoStream.getUnackedRecords(Parser<T>)` for typed access, or use `recreateStream()` which handles re-ingestion automatically using cached raw bytes.

**StreamConfigurationOptions**:
- `setAckCallback(Consumer<IngestRecordResponse>)` - No longer invoked by native backend. Use `setAckCallback(AckCallback)` instead

### Removed

- `TokenFactory` - Token management now handled by native Rust SDK
- `BackgroundTask` - Background processing now handled by native Rust SDK
- `ZerobusSdkStubUtils` - gRPC stub utilities no longer needed with native backend

### Platform Support

- Linux x86_64: Supported
- Windows x86_64: Supported
- macOS x86_64: Supported
- macOS aarch64 (Apple Silicon): Supported

## Release v0.1.0

Initial release of the Databricks Zerobus Ingest SDK for Java.

### API Changes

- Added `ZerobusSdk` class for creating ingestion streams
- Added `ZerobusStream` class for managing stateful gRPC streams
- Added `RecordAcknowledgment` for blocking until record acknowledgment
- Added `TableProperties` for configuring table schema and name
- Added `StreamConfigurationOptions` for stream behavior configuration
- Added `ZerobusException` and `NonRetriableException` for error handling
- Added `StreamState` enum for tracking stream lifecycle
- Added utility methods in `ZerobusSdkStubUtils` for gRPC stub management
- Support for Java 8 and higher
