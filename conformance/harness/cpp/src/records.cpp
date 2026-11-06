#include "records.hpp"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/message.h>
#include <google/protobuf/util/json_util.h>

namespace zb_harness {

RecordSchema::RecordSchema()
    : pool_(std::make_unique<google::protobuf::DescriptorPool>()) {}

RecordSchema::~RecordSchema() = default;

std::unique_ptr<RecordSchema> RecordSchema::FromDescriptorProto(
    const std::string& descriptor_proto_bytes, std::string* error) {
  google::protobuf::DescriptorProto descriptor_proto;
  if (!descriptor_proto.ParseFromString(descriptor_proto_bytes)) {
    *error = "descriptor_proto_bytes is not a DescriptorProto";
    return nullptr;
  }
  // Same synthetic file the Rust harness's SDK helper builds: no package and
  // default syntax, so the bare message name is the fully-qualified name. An
  // unnamed message gets the same fallback name as the Rust side.
  if (descriptor_proto.name().empty()) {
    descriptor_proto.set_name("ZerobusDynamicMessage");
  }
  google::protobuf::FileDescriptorProto file_proto;
  file_proto.set_name("zerobus_dynamic.proto");
  *file_proto.add_message_type() = descriptor_proto;

  std::unique_ptr<RecordSchema> schema(new RecordSchema());
  const google::protobuf::FileDescriptor* file =
      schema->pool_->BuildFile(file_proto);
  if (file == nullptr || file->message_type_count() == 0) {
    *error = "descriptor_proto_bytes does not describe a valid message";
    return nullptr;
  }
  schema->descriptor_ = file->message_type(0);
  schema->factory_ =
      std::make_unique<google::protobuf::DynamicMessageFactory>(schema->pool_.get());
  return schema;
}

bool RecordSchema::EncodeJsonRecord(const std::string& record_json,
                                    std::vector<std::uint8_t>* proto_bytes,
                                    std::string* error) const {
  std::unique_ptr<google::protobuf::Message> message(
      factory_->GetPrototype(descriptor_)->New());

  // Default options: unknown fields are an error (matches the Rust harness).
  google::protobuf::util::JsonParseOptions options;
  const google::protobuf::util::Status status =
      google::protobuf::util::JsonStringToMessage(record_json, message.get(), options);
  if (!status.ok()) {
    *error = "record_json does not match the schema: " + status.ToString();
    return false;
  }

  std::string bytes;
  if (!message->SerializeToString(&bytes)) {
    *error = "failed to serialize record";
    return false;
  }
  proto_bytes->assign(bytes.begin(), bytes.end());
  return true;
}

}  // namespace zb_harness
