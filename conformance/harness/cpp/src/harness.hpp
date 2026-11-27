// TestHarness service: maps each contract RPC onto the C++ Zerobus SDK.
//
// Pure pass-through: each RPC forwards to the SDK and reports what the SDK does.
// An SDK rejection (zerobus::ZerobusException) is returned in the response's
// SdkError; any other failure is a harness fault and returns a non-OK status.
// Only protocol=GRPC, format=PROTO is implemented (see CheckSliceSupported in
// harness.cpp).
#ifndef ZB_HARNESS_HARNESS_HPP
#define ZB_HARNESS_HARNESS_HPP

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include <grpcpp/grpcpp.h>

#include "test_harness.grpc.pb.h"

namespace zb_harness {

namespace pb = ::zerobus::conformance::harness;

class HarnessService final : public pb::TestHarness::Service {
 public:
  // on_shutdown is called by the Shutdown RPC; main() uses it to exit.
  explicit HarnessService(std::function<void()> on_shutdown);

  grpc::Status CreateStream(grpc::ServerContext* context,
                            const pb::CreateStreamRequest* request,
                            pb::CreateStreamResponse* response) override;
  grpc::Status IngestRecord(grpc::ServerContext* context,
                            const pb::IngestRecordRequest* request,
                            pb::IngestRecordResponse* response) override;
  grpc::Status IngestRecords(grpc::ServerContext* context,
                             const pb::IngestRecordsRequest* request,
                             pb::IngestRecordsResponse* response) override;
  grpc::Status AwaitWriteCompleted(grpc::ServerContext* context,
                                   const pb::AwaitRequest* request,
                                   pb::AwaitResponse* response) override;
  grpc::Status Flush(grpc::ServerContext* context,
                     const pb::FlushRequest* request,
                     pb::ActionResponse* response) override;
  grpc::Status Close(grpc::ServerContext* context,
                     const pb::CloseRequest* request,
                     pb::ActionResponse* response) override;
  grpc::Status Shutdown(grpc::ServerContext* context, const pb::Empty* request,
                        pb::Empty* response) override;

 private:
  struct StreamEntry;

  // Shared skeleton of the per-stream RPCs. Looks up stream_id (NOT_FOUND if it
  // was never created), holds that stream's lock, and runs call(entry). A
  // ZerobusException from the SDK is reported in response->error; any other
  // exception is a harness fault and returns INTERNAL.
  template <typename Response, typename Call>
  grpc::Status WithStream(const std::string& stream_id, Response* response,
                          Call&& call);

  std::mutex streams_mu_;  // guards streams_ and next_stream_id_ only
  std::map<std::string, std::shared_ptr<StreamEntry>> streams_;
  std::uint64_t next_stream_id_ = 0;
  std::function<void()> on_shutdown_;
};

}  // namespace zb_harness

#endif  // ZB_HARNESS_HARNESS_HPP
