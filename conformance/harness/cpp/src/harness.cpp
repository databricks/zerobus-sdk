#include "harness.hpp"

#include <chrono>
#include <cstdio>
#include <exception>
#include <future>
#include <utility>
#include <vector>

#include "records.hpp"
#include "zerobus/zerobus.hpp"

namespace zb_harness {
namespace {

// Sends no credentials. The mock does not check auth, so this is the C++
// equivalent of the Rust harness's `.no_auth()`.
class NoAuthHeadersProvider final : public zerobus::HeadersProvider {
 public:
  std::map<std::string, std::string> get_headers() override { return {}; }
};

// Reject every protocol/format combination this slice does not implement.
// Each TODO names the work that remains.
grpc::Status CheckSliceSupported(pb::Protocol protocol, pb::RecordFormat format) {
  if (protocol == pb::GRPC && format == pb::PROTO) {
    return grpc::Status::OK;
  }
  if (protocol == pb::GRPC && format == pb::JSON) {
    // The C++ SDK has ingest_json_record(); wiring it needs the JSON golden
    // convention settled first (the Rust harness also returns UNIMPLEMENTED).
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "TODO(conformance): GRPC + JSON not implemented in the C++ harness yet");
  }
  if (protocol == pb::GRPC && format == pb::AVRO) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "TODO(conformance): GRPC + AVRO not implemented in the C++ harness yet (the C FFI has no Avro support)");
  }
  if (protocol == pb::GRPC && format == pb::ARROW) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "TODO(conformance): GRPC + ARROW is not a valid pairing; ARROW rides ARROW_FLIGHT");
  }
  if (protocol == pb::ARROW_FLIGHT) {
    // The C++ SDK exposes create_arrow_stream(); a later slice wires it up.
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "TODO(conformance): ARROW_FLIGHT protocol not implemented in the C++ harness yet");
  }
  return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                      "protocol and format must be specified");
}

// Map an SDK error onto a gRPC status. The SDK does not expose a finer error
// category through the exception, so every SDK failure is INTERNAL.
grpc::Status ToStatus(const zerobus::ZerobusException& e) {
  return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
}

void LogLine(const std::string& line) {
  // stdout is reserved for the endpoint line, so logs go to stderr.
  std::fprintf(stderr, "%s\n", line.c_str());
}

}  // namespace

// One open SDK stream plus the schema its records are built against.
// Member order matters: members are destroyed in reverse order, so
// `pending_wait` (which may still use `stream`) goes first and `sdk` (which the
// stream depends on) goes last.
struct HarnessService::StreamEntry {
  std::unique_ptr<zerobus::Sdk> sdk;
  std::unique_ptr<zerobus::Stream> stream;
  std::unique_ptr<RecordSchema> schema;
  // Serializes the per-stream RPCs (ingest / await / flush / close).
  std::mutex mu;
  // A timed-out AwaitWriteCompleted still running on its helper thread.
  std::future<void> pending_wait;
};

HarnessService::HarnessService(std::function<void()> on_shutdown)
    : on_shutdown_(std::move(on_shutdown)) {}

std::shared_ptr<HarnessService::StreamEntry> HarnessService::Find(
    const std::string& stream_id, grpc::Status* status) {
  std::lock_guard<std::mutex> lock(streams_mu_);
  auto it = streams_.find(stream_id);
  if (it == streams_.end()) {
    *status = grpc::Status(grpc::StatusCode::NOT_FOUND,
                           "unknown stream_id '" + stream_id + "'");
    return nullptr;
  }
  return it->second;
}

void HarnessService::DrainPending(StreamEntry& entry) {
  if (!entry.pending_wait.valid()) {
    return;
  }
  entry.pending_wait.wait();
  // The timed-out caller already reported the timeout, so a late error here
  // has no RPC to report to.
  try {
    entry.pending_wait.get();
  } catch (const std::exception&) {
  }
}

grpc::Status HarnessService::CreateStream(grpc::ServerContext*,
                                          const pb::CreateStreamRequest* request,
                                          pb::CreateStreamResponse* response) {
  grpc::Status supported = CheckSliceSupported(request->protocol(), request->format());
  if (!supported.ok()) {
    return supported;
  }

  std::string error;
  std::unique_ptr<RecordSchema> schema =
      RecordSchema::FromDescriptorProto(request->descriptor_proto_bytes(), &error);
  if (!schema) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, error);
  }

  LogLine("CreateStream endpoint=" + request->server_endpoint() +
          " table=" + request->table_path());

  auto entry = std::make_shared<StreamEntry>();
  entry->schema = std::move(schema);
  try {
    // The mock is a local plaintext server that checks no credentials, so the
    // SDK runs with TLS disabled ("http://") and no-auth headers.
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
    return ToStatus(e);
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
  grpc::Status status;
  std::shared_ptr<StreamEntry> entry = Find(request->stream_id(), &status);
  if (!entry) {
    return status;
  }
  std::lock_guard<std::mutex> lock(entry->mu);
  DrainPending(*entry);

  std::string error;
  std::vector<std::uint8_t> proto_bytes;
  if (!entry->schema->EncodeJsonRecord(request->record_json(), &proto_bytes, &error)) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, error);
  }

  try {
    // Queue only: returns once the SDK has enqueued the record, not once it is
    // acknowledged.
    response->set_offset(entry->stream->ingest_proto_record(proto_bytes));
  } catch (const zerobus::ZerobusException& e) {
    return ToStatus(e);
  }
  return grpc::Status::OK;
}

grpc::Status HarnessService::AwaitWriteCompleted(grpc::ServerContext*,
                                                 const pb::AwaitRequest* request,
                                                 pb::AwaitResponse* response) {
  grpc::Status status;
  std::shared_ptr<StreamEntry> entry = Find(request->stream_id(), &status);
  if (!entry) {
    return status;
  }
  std::lock_guard<std::mutex> lock(entry->mu);
  DrainPending(*entry);

  zerobus::Stream* stream = entry->stream.get();
  const std::int64_t offset = request->offset();

  // wait_for_offset() has no timeout of its own, so timeout_ms is enforced by
  // running the wait on a helper thread. A value <= 0 waits with no deadline.
  if (request->timeout_ms() > 0) {
    std::future<void> wait = std::async(std::launch::async, [stream, offset] {
      stream->wait_for_offset(offset);
    });
    if (wait.wait_for(std::chrono::milliseconds(request->timeout_ms())) ==
        std::future_status::timeout) {
      // The helper still uses the stream, so keep it and drain it before the
      // next call on this stream.
      entry->pending_wait = std::move(wait);
      response->set_error_message("timed out after " +
                                  std::to_string(request->timeout_ms()) +
                                  " ms waiting for offset " + std::to_string(offset));
      return grpc::Status::OK;
    }
    try {
      wait.get();
    } catch (const std::exception& e) {
      response->set_error_message(e.what());
    }
    return grpc::Status::OK;
  }

  try {
    stream->wait_for_offset(offset);
  } catch (const std::exception& e) {
    response->set_error_message(e.what());
  }
  // Per the contract, a failed wait is reported in the response body, not as a
  // gRPC error. An empty error_message means success.
  return grpc::Status::OK;
}

grpc::Status HarnessService::Flush(grpc::ServerContext*,
                                   const pb::FlushRequest* request,
                                   pb::Empty*) {
  grpc::Status status;
  std::shared_ptr<StreamEntry> entry = Find(request->stream_id(), &status);
  if (!entry) {
    return status;
  }
  std::lock_guard<std::mutex> lock(entry->mu);
  DrainPending(*entry);
  try {
    entry->stream->flush();
  } catch (const zerobus::ZerobusException& e) {
    return ToStatus(e);
  }
  return grpc::Status::OK;
}

grpc::Status HarnessService::Close(grpc::ServerContext*,
                                   const pb::CloseRequest* request,
                                   pb::Empty*) {
  // Remove first so new RPCs on this id see NOT_FOUND. The entry is dropped
  // even if the SDK close fails.
  std::shared_ptr<StreamEntry> entry;
  {
    std::lock_guard<std::mutex> lock(streams_mu_);
    auto it = streams_.find(request->stream_id());
    if (it == streams_.end()) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND,
                          "unknown stream_id '" + request->stream_id() + "'");
    }
    entry = std::move(it->second);
    streams_.erase(it);
  }

  grpc::Status status;
  {
    std::lock_guard<std::mutex> lock(entry->mu);
    DrainPending(*entry);
    try {
      entry->stream->close();
    } catch (const zerobus::ZerobusException& e) {
      status = ToStatus(e);
    }
  }
  LogLine("stream closed: " + request->stream_id());
  // `entry` is released here. The SDK and stream handles are freed with it,
  // stream first, once no concurrent RPC still holds the entry.
  return status;
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
