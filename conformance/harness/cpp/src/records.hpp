// Logical record -> SDK-native record conversion. The only place this harness
// understands record bodies.
//
// Pipeline: `CreateStreamRequest.descriptor_proto_bytes` -> RecordSchema (a
// protobuf DescriptorPool entry); `IngestRecordRequest.record_json` + schema ->
// EncodeJsonRecord -> protobuf wire bytes, which the SDK then ingests.
#ifndef ZB_HARNESS_RECORDS_HPP
#define ZB_HARNESS_RECORDS_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace google::protobuf {
class Descriptor;
class DescriptorPool;
class DynamicMessageFactory;
}  // namespace google::protobuf

namespace zb_harness {

class RecordSchema {
 public:
  // Decodes a serialized `google.protobuf.DescriptorProto` and builds a message
  // type from it. Returns nullptr and sets `*error` on failure.
  static std::unique_ptr<RecordSchema> FromDescriptorProto(
      const std::string& descriptor_proto_bytes, std::string* error);

  ~RecordSchema();
  RecordSchema(const RecordSchema&) = delete;
  RecordSchema& operator=(const RecordSchema&) = delete;

  // Parses `record_json` (a JSON object, canonical protobuf-JSON) into a message
  // of this schema and serializes it. Unknown fields are rejected, so a fixture
  // typo fails loudly. Returns false and sets `*error` on failure.
  bool EncodeJsonRecord(const std::string& record_json,
                        std::vector<std::uint8_t>* proto_bytes,
                        std::string* error) const;

 private:
  RecordSchema();

  // Declaration order matters: the factory references the pool, so it must be
  // destroyed first.
  std::unique_ptr<google::protobuf::DescriptorPool> pool_;
  const google::protobuf::Descriptor* descriptor_ = nullptr;
  std::unique_ptr<google::protobuf::DynamicMessageFactory> factory_;
};

}  // namespace zb_harness

#endif  // ZB_HARNESS_RECORDS_HPP
