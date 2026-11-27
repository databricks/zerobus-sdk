#include "harness.hpp"

#include <cstdio>
#include <exception>
#include <utility>
#include <vector>

#include "records.hpp"
#include "zerobus/zerobus.hpp"

namespace zb_harness {
namespace {

// Sends no credentials. The mock does not check auth, so this is the C++
// equivalent of the Rust harness's .no_auth().
class NoAuthHeadersProvider final : public zerobus::HeadersProvider {
 public:
  std::map<std::string, std::string> get_headers() override { return {}; }
};

// Reject every protocol/format combination this slice does not implement.
// Each TODO names the work that remains.
grpc::Status CheckSliceSupported(pb::Protocol protocol, pb::RecordFormat format) {
  if (protocol == pb::PROTOCOL_GRPC && format == pb::RECORD_FORMAT_PROTO) {
    return grpc::Status::OK;
  }
  if (protocol == pb::PROTOCOL_GRPC && format == pb::RECORD_FORMAT_JSON) {
    // The C++ SDK has ingest_json_record(); wiring it needs the JSON golden
    // convention settled first (the Rust harness also returns UNIMPLEMENTED).
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "TODO(conformance): GRPC + JSON not implemented in the C++ harness yet");
  }
  if (protocol == pb::PROTOCOL_GRPC && format == pb::RECORD_FORMAT_AVRO) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "TODO(conformance): GRPC + AVRO not implemented in the C++ harness yet (the C FFI has no Avro support)");
  }
  if (protocol == pb::PROTOCOL_GRPC && format == pb::RECORD_FORMAT_ARROW) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "TODO(conformance): GRPC + ARROW is not a valid pairing; ARROW rides ARROW_FLIGHT");
  }
  if (protocol == pb::PROTOCOL_ARROW_FLIGHT) {
    // The C++ SDK exposes create_arrow_stream(); a later slice wires it up.
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "TODO(conformance): ARROW_FLIGHT protocol not implemented in the C++ harness yet");
  }
  return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                      "protocol and format must be specified");
}

// Reports an SDK rejection verbatim. retryable is the SDK's own classification.
void SetSdkError(pb::SdkError* error, const zerobus::ZerobusException& e) {
  error->set_message(e.what());
  error->set_retryable(e.is_retryable());
}

void LogLine(const std::string& line) {
  // stdout is reserved for the endpoint line, so logs go to stderr.
  std::fprintf(stderr, "%s\n", line.c_str());
}

}  // namespace

// One open SDK stream plus the schema its records are built against.
// Member order matters: members are destroyed in reverse order, so stream
// (which the SDK handle must outlive) goes first and sdk goes last.
struct HarnessService::StreamEntry {
  std::unique_ptr<zerobus::Sdk> sdk;
  std::unique_ptr<zerobus::Stream> stream;
  std::unique_ptr<RecordSchema> schema;
  // The SDK's Stream is not thread-safe, so every RPC on this stream holds mu.
  std::mutex mu;
};

HarnessService::HarnessService(std::function<void()> on_shutdown)
    : on_shutdown_(std::move(on_shutdown)) {}

template <typename Response, typename Call>
grpc::Status HarnessService::WithStream(const std::string& stream_id,
                                        Response* response, Call&& call) {
  std::shared_ptr<StreamEntry> entry;
  {
    std::lock_guard<std::mutex> lock(streams_mu_);
    auto it = streams_.find(stream_id);
    if (it == streams_.end()) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND,
                          "unknown stream_id '" + stream_id + "'");
    }
    entry = it->second;
  }
  std::lock_guard<std::mutex> lock(entry->mu);
  try {
    return call(*entry);
  } catch (const zerobus::ZerobusException& e) {
    SetSdkError(response->mutable_error(), e);
    return grpc::Status::OK;
  } catch (const std::exception& e) {
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status HarnessService::CreateStream(grpc::ServerContext*,
                                          const pb::CreateStreamRequest* request,
                                          pb::CreateStreamResponse* response) {
  grpc::Status supported = CheckSliceSupported(request->protocol(), request->format());
  if (!supported.ok()) {
    return supported;
  }

  LogLine("CreateStream endpoint=" + request->server_endpoint() +
          " table=" + request->table_path());

  // The SDK validates the descriptor and gives its own verdict. The harness
  // parses the descriptor only after the SDK accepts it, to build its encoder.
  auto entry = std::make_shared<StreamEntry>();
  try {
    // The mock is a local plaintext server that checks no credentials, so the
    // SDK runs with TLS disabled ("http://") and no-auth headers. disable_tls()
    // is a public, released C++ method, so it is not gated here. (The Rust core
    // gates its equivalent behind the `testing` feature.)
    entry->sdk = std::make_unique<zerobus::Sdk>(
        zerobus::Sdk::builder()
            .endpoint("http://" + request->server_endpoint())
            .disable_tls()
            .build());

    zerobus::TableProperties table;
    table.table_name = request->table_path();
    const std::string& descriptor = request->descriptor_proto_bytes();
    table.descriptor_proto.assign(descriptor.begin(), descriptor.end());
    entry->stream = std::make_unique<zerobus::Stream>(entry->sdk->create_stream(
        table, std::make_shared<NoAuthHeadersProvider>()));
  } catch (const zerobus::ZerobusException& e) {
    // stream_id stays unset when the error is set.
    SetSdkError(response->mutable_error(), e);
    return grpc::Status::OK;
  } catch (const std::exception& e) {
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }

  std::string error;
  entry->schema =
      RecordSchema::FromDescriptorProto(request->descriptor_proto_bytes(), &error);
  if (!entry->schema) {
    // The SDK accepted a descriptor the harness cannot encode against. That is
    // a harness fault. The unregistered entry is freed on return.
    return grpc::Status(grpc::StatusCode::INTERNAL, error);
  }

  std::string stream_id;
  {
    std::lock_guard<std::mutex> lock(streams_mu_);
    stream_id = "stream-" + std::to_string(next_stream_id_++);
    streams_.emplace(stream_id, std::move(entry));
  }
  response->set_stream_id(stream_id);
  return grpc::Status::OK;
}

grpc::Status HarnessService::IngestRecord(grpc::ServerContext*,
                                          const pb::IngestRecordRequest* request,
                                          pb::IngestRecordResponse* response) {
  return WithStream(request->stream_id(), response, [&](StreamEntry& entry) {
    std::string error;
    std::vector<std::uint8_t> proto_bytes;
    if (!entry.schema->EncodeJsonRecord(request->record_json(), &proto_bytes, &error)) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, error);
    }
    // Queue only: returns once the SDK has enqueued the record, not once it is
    // acknowledged.
    response->set_offset(entry.stream->ingest_proto_record(proto_bytes));
    return grpc::Status::OK;
  });
}

grpc::Status HarnessService::IngestRecords(grpc::ServerContext*,
                                           const pb::IngestRecordsRequest* request,
                                           pb::IngestRecordsResponse* response) {
  return WithStream(request->stream_id(), response, [&](StreamEntry& entry) {
    if (request->record_json_size() == 0) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          "record_json must contain at least one record");
    }
    // The SDK's batch ingest takes every record's protobuf bytes in one call, so
    // the fixtures are converted first. A record the harness cannot convert fails
    // the call before the SDK sees any of the batch.
    std::vector<std::vector<std::uint8_t>> encoded;
    encoded.reserve(static_cast<std::size_t>(request->record_json_size()));
    for (const std::string& record_json : request->record_json()) {
      std::string error;
      std::vector<std::uint8_t> proto_bytes;
      if (!entry.schema->EncodeJsonRecord(record_json, &proto_bytes, &error)) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, error);
      }
      encoded.push_back(std::move(proto_bytes));
    }
    // Queue only, as in IngestRecord. The SDK returns one offset for the whole batch.
    response->set_offset(entry.stream->ingest_proto_records(encoded));
    return grpc::Status::OK;
  });
}

grpc::Status HarnessService::AwaitWriteCompleted(grpc::ServerContext*,
                                                 const pb::AwaitRequest* request,
                                                 pb::AwaitResponse* response) {
  // No harness-side timeout: the wait is bounded by the SDK's own ack/flush timeout.
  return WithStream(request->stream_id(), response, [&](StreamEntry& entry) {
    entry.stream->wait_for_offset(request->offset());
    return grpc::Status::OK;
  });
}

grpc::Status HarnessService::Flush(grpc::ServerContext*,
                                   const pb::FlushRequest* request,
                                   pb::ActionResponse* response) {
  return WithStream(request->stream_id(), response, [&](StreamEntry& entry) {
    entry.stream->flush();
    return grpc::Status::OK;
  });
}

grpc::Status HarnessService::Close(grpc::ServerContext*,
                                   const pb::CloseRequest* request,
                                   pb::ActionResponse* response) {
  // The entry stays registered after close, so later calls reach the SDK, which
  // reports its own closed-stream error. A failed close keeps the SDK handle
  // alive for recovery, so close can be retried.
  return WithStream(request->stream_id(), response, [&](StreamEntry& entry) {
    entry.stream->close();
    LogLine("stream closed: " + request->stream_id());
    return grpc::Status::OK;
  });
}

grpc::Status HarnessService::Shutdown(grpc::ServerContext*, const pb::Empty*,
                                      pb::Empty*) {
  LogLine("Shutdown RPC received");
  // Only signal here. main() calls Server::Shutdown(), which waits for this
  // handler to return, so the response is sent before the process exits.
  on_shutdown_();
  return grpc::Status::OK;
}

}  // namespace zb_harness
