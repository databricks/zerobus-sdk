// `TestHarness` service: maps each contract RPC onto the C++ Zerobus SDK.
//
// First slice: protocol=GRPC, format=PROTO only. Every other combination returns
// UNIMPLEMENTED with a TODO (see CheckSliceSupported in harness.cpp).
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
  // `on_shutdown` is called by the Shutdown RPC; main() uses it to exit.
  explicit HarnessService(std::function<void()> on_shutdown);

  grpc::Status CreateStream(grpc::ServerContext* context,
                            const pb::CreateStreamRequest* request,
                            pb::CreateStreamResponse* response) override;
  grpc::Status IngestRecord(grpc::ServerContext* context,
                            const pb::IngestRecordRequest* request,
                            pb::IngestRecordResponse* response) override;
  grpc::Status AwaitWriteCompleted(grpc::ServerContext* context,
                                   const pb::AwaitRequest* request,
                                   pb::AwaitResponse* response) override;
  grpc::Status Flush(grpc::ServerContext* context,
                     const pb::FlushRequest* request,
                     pb::Empty* response) override;
  grpc::Status Close(grpc::ServerContext* context,
                     const pb::CloseRequest* request,
                     pb::Empty* response) override;
  grpc::Status Shutdown(grpc::ServerContext* context, const pb::Empty* request,
                        pb::Empty* response) override;

 private:
  struct StreamEntry;

  // Returns the entry for `stream_id`, or null with `*status` set to NOT_FOUND.
  std::shared_ptr<StreamEntry> Find(const std::string& stream_id,
                                    grpc::Status* status);
  // Waits for a timed-out AwaitWriteCompleted that is still running, so no
  // other call touches the SDK stream while it is in use. Caller holds the lock.
  static void DrainPending(StreamEntry& entry);

  std::mutex streams_mu_;  // guards `streams_` and `next_stream_id_` only
  std::map<std::string, std::shared_ptr<StreamEntry>> streams_;
  std::uint64_t next_stream_id_ = 0;
  std::function<void()> on_shutdown_;
};

}  // namespace zb_harness

#endif  // ZB_HARNESS_HARNESS_HPP
