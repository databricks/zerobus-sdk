// Layer-1 conformance harness for the C++ Zerobus SDK.
//
// Handshake: binds 127.0.0.1:0, prints exactly `HARNESS_ENDPOINT=127.0.0.1:<port>`
// to stdout, then serves the `TestHarness` gRPC service. Logs go to stderr only.
// Exits on the `Shutdown` RPC or SIGINT/SIGTERM.

#include <csignal>
#include <condition_variable>
#include <cstdio>
#include <memory>
#include <mutex>
#include <thread>

#include <grpcpp/grpcpp.h>
#include <pthread.h>

#include "harness.hpp"

namespace {

// Set once by the Shutdown RPC or by a termination signal; main() waits on it.
class StopSignal {
 public:
  void Request() {
    std::lock_guard<std::mutex> lock(mu_);
    requested_ = true;
    cv_.notify_all();
  }

  void Wait() {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [this] { return requested_; });
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  bool requested_ = false;
};

}  // namespace

int main() {
  // Block SIGINT/SIGTERM before any thread exists, so only the sigwait() thread
  // below consumes them.
  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  sigaddset(&signals, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &signals, nullptr);

  // Shared with the detached signal thread, so it stays valid if main returns
  // first.
  auto stop = std::make_shared<StopSignal>();
  std::thread([stop, signals] {
    int sig = 0;
    sigwait(&signals, &sig);
    std::fprintf(stderr, "termination signal %d received\n", sig);
    stop->Request();
  }).detach();

  // Declared before `server`, so the server is destroyed first.
  zb_harness::HarnessService service([stop] { stop->Request(); });

  int port = 0;
  grpc::ServerBuilder builder;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  if (!server || port == 0) {
    std::fprintf(stderr, "failed to bind 127.0.0.1:0\n");
    return 1;
  }

  // stdout is reserved for this line. It is flushed before anything else can
  // happen.
  std::printf("HARNESS_ENDPOINT=127.0.0.1:%d\n", port);
  std::fflush(stdout);
  std::fprintf(stderr, "TestHarness listening on 127.0.0.1:%d\n", port);

  stop->Wait();
  // Waits for in-flight RPCs, including the Shutdown RPC's own response.
  server->Shutdown();
  std::fprintf(stderr, "harness exited\n");
  return 0;
}
