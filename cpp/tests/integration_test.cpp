// Live integration test: create-stream -> ingest -> flush -> close against a
// real endpoint, mirroring the Java/TypeScript suites. Gated on env vars;
// SKIPS (exits 0) when unset, so `make test` and CI stay hermetic.
//
// Required: ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL,
// ZEROBUS_TABLE_NAME, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET.
// Optional: ZEROBUS_TEST_RECORD_JSON — a record matching the table schema
// (the default almost certainly won't match; set it for a real run).

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "zerobus/error.hpp"
#include "zerobus/sdk.hpp"

namespace {

int g_failures = 0;

void fail(const char* msg) {
  std::fprintf(stderr, "FAIL: %s\n", msg);
  ++g_failures;
}

// Returns the env var value, or an empty string if unset/empty.
std::string env(const char* name) {
  const char* v = std::getenv(name);
  return (v != nullptr) ? std::string(v) : std::string();
}

void check(const char* msg, bool ok) {
  if (!ok) {
    fail(msg);
  }
}

// Runs @p call; checks it throws a non-retryable ZerobusException whose
// message contains @p message.
template <typename F>
void check_throws(const char* what, F call, const std::string& message) {
  try {
    call();
    fail(what);
  } catch (const zerobus::ZerobusException& e) {
    if (e.is_retryable() ||
        std::string(e.what()).find(message) == std::string::npos) {
      fail(what);
      std::fprintf(stderr, "  what(): %s (retryable=%s)\n", e.what(),
                   e.is_retryable() ? "true" : "false");
    }
  }
}

// Counts every message ID its mux delivers.
class IdRecorder : public zerobus::MultiplexedAckCallback {
 public:
  void on_ack(zerobus::MessageId id) noexcept override {
    std::lock_guard<std::mutex> lock(mu_);
    ++acked_[id];
  }
  void on_error(zerobus::MessageId, const std::string&) noexcept override {
    std::lock_guard<std::mutex> lock(mu_);
    ++errors_;
  }
  // Every ID in @p ids was acked exactly once, nothing else was, and no
  // error was reported.
  bool saw_each_once(const std::vector<zerobus::MessageId>& ids) {
    std::lock_guard<std::mutex> lock(mu_);
    if (errors_ != 0 || acked_.size() != ids.size()) {
      return false;
    }
    for (zerobus::MessageId id : ids) {
      auto it = acked_.find(id);
      if (it == acked_.end() || it->second != 1) {
        return false;
      }
    }
    return true;
  }

 private:
  std::mutex mu_;
  std::unordered_map<zerobus::MessageId, int> acked_;
  int errors_ = 0;
};

// The multiplexed stream through the builder: 33 lanes, so the lane-32 IDs
// are negative. Ingest in a loop, flush once.
void run_multiplexed(zerobus::Sdk& sdk, const std::string& table_name,
                     const std::string& client_id,
                     const std::string& client_secret,
                     const std::string& record_json) {
  constexpr std::size_t kLanes = 33;
  auto recorder = std::make_shared<IdRecorder>();
  std::weak_ptr<IdRecorder> recorder_alive = recorder;
  std::vector<zerobus::MessageId> ids;
  {
    zerobus::MultiplexedStream mux = [&] {
      zerobus::MultiplexedStreamBuilder builder =
          sdk.stream_builder()
              .table(table_name)
              .oauth(client_id, client_secret)
              .json()
              .multiplexed(kLanes)
              .ack_callback(recorder);
      return builder.build();
    }();
    recorder.reset();
    check("the callback outlives the builder and the caller's reference",
          !recorder_alive.expired());

    for (std::size_t i = 0; i < kLanes + 1; ++i) {
      ids.push_back(mux.ingest_json_record(record_json));
    }
    std::optional<zerobus::MessageId> batch =
        mux.ingest_json_records({record_json, record_json});
    check("a JSON batch returns an ID", batch.has_value());
    if (batch) {
      ids.push_back(*batch);
    }
    check("an empty batch returns nullopt",
          !mux.ingest_json_records({}).has_value());
    // Round-robin: the 33rd ingest lands on lane 32.
    const zerobus::MessageId lane_32 = ids[kLanes - 1];
    check("the lane-32 message ID is negative", lane_32.raw() < 0);
    mux.wait_for_message_id(lane_32);
    // A lane this mux doesn't have: the call reaches the core, which rejects
    // it (so wait_for_message_id is not a no-op).
    check_throws(
        "a wait on lane 40 of a 33-lane mux must throw",
        [&] {
          mux.wait_for_message_id(zerobus::MessageId(std::int64_t{40} << 58));
        },
        "Invalid stream index 40");
    mux.flush();
    check("is_closed() is false before close()", !mux.is_closed());
    mux.close();
    check("is_closed() is true after close()", mux.is_closed());
    // close() keeps the handle, so recovery still works on the closed mux.
    check("nothing is unacked after close()",
          mux.get_unacked_records().empty());
    // close() drained the callbacks; the FFI still holds the callback.
    auto alive = recorder_alive.lock();
    check("the callback is alive until the stream is destroyed",
          alive != nullptr);
    check("the callback saw every returned ID once, incl. negative ones",
          alive != nullptr && alive->saw_each_once(ids));
    alive.reset();
    for (const auto& [what, call] :
         std::vector<std::pair<const char*, std::function<void()>>>{
             {"ingest after close() must throw",
              [&] { mux.ingest_json_record(record_json); }},
             {"an empty batch after close() must throw",
              [&] { mux.ingest_json_records({}); }},
             {"flush after close() must throw", [&] { mux.flush(); }},
         }) {
      check_throws(what, call, "MultiplexedStream is closed");
    }

    // Using a moved-from stream is the point of this check.
    zerobus::MultiplexedStream moved = std::move(mux);
    check_throws(
        "a moved-from MultiplexedStream must throw", [&] { mux.flush(); },
        "MultiplexedStream has been moved from");
  }
  // The stream is destroyed; the FFI frees the callback (maybe a bit later).
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!recorder_alive.expired() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  check("the callback is freed after the stream is destroyed",
        recorder_alive.expired());

  auto small_mux = [&] {
    return sdk.stream_builder()
        .table(table_name)
        .oauth(client_id, client_secret)
        .json()
        .multiplexed(2)
        .build();
  };
  // Move-assigning over a live mux releases it and takes the other one.
  zerobus::MultiplexedStream json_mux = small_mux();
  zerobus::MultiplexedStream other = small_mux();
  json_mux = std::move(other);
  check_throws(
      "the moved-from source must throw", [&] { other.flush(); },
      "MultiplexedStream has been moved from");

  // One MultiplexedStream may be used from several threads at once.
  constexpr int kThreads = 4;
  constexpr int kPerThread = 10;
  std::mutex ids_mu;
  std::unordered_set<zerobus::MessageId> concurrent_ids;
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < kPerThread; ++i) {
        zerobus::MessageId id = json_mux.ingest_json_record(record_json);
        std::lock_guard<std::mutex> lock(ids_mu);
        concurrent_ids.insert(id);
      }
    });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  json_mux.flush();
  check("concurrent ingests get distinct IDs",
        concurrent_ids.size() == kThreads * kPerThread);

  // The proto overloads cross the FFI too: on this JSON table the core rejects
  // each one, which proves the call reached it.
  const std::string kMismatch =
      "Record type does not match stream configuration";
  const std::vector<std::uint8_t> proto = {0x08, 0x01};
  check_throws(
      "proto record on a JSON mux must throw",
      [&] { json_mux.ingest_proto_record(proto); }, kMismatch);
  check_throws(
      "proto batch on a JSON mux must throw",
      [&] { json_mux.ingest_proto_records({proto}); }, kMismatch);
  json_mux.close();
}

}  // namespace

int main() {
  const std::string endpoint = env("ZEROBUS_SERVER_ENDPOINT");
  const std::string workspace_url = env("DATABRICKS_WORKSPACE_URL");
  const std::string table_name = env("ZEROBUS_TABLE_NAME");
  const std::string client_id = env("DATABRICKS_CLIENT_ID");
  const std::string client_secret = env("DATABRICKS_CLIENT_SECRET");
  // Required too: without a record matching the table, flush() would fail with
  // a schema mismatch, so a half-configured env should skip rather than go red.
  const std::string record_json = env("ZEROBUS_TEST_RECORD_JSON");

  // Skip (not fail) when any required variable is missing.
  if (endpoint.empty() || workspace_url.empty() || table_name.empty() ||
      client_id.empty() || client_secret.empty() || record_json.empty()) {
    std::printf(
        "SKIP integration_test: set ZEROBUS_SERVER_ENDPOINT, "
        "DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME, DATABRICKS_CLIENT_ID, "
        "DATABRICKS_CLIENT_SECRET, and ZEROBUS_TEST_RECORD_JSON (a record "
        "matching ZEROBUS_TABLE_NAME) to run it.\n");
    return 0;
  }

  try {
    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(endpoint)
                           .unity_catalog_url(workspace_url)
                           .build();

    // A JSON stream: no descriptor, and the record type set to JSON (the
    // StreamOptions default is Proto, which needs a descriptor).
    zerobus::TableProperties table;
    table.table_name = table_name;
    zerobus::StreamOptions options;
    options.record_type = zerobus::RecordType::Json;

    zerobus::Stream stream =
        sdk.create_stream(table, client_id, client_secret, options);

    // Queue in a loop, flush once — never wait per record.
    constexpr int kRecordCount = 10;
    std::int64_t last_offset = -1;
    for (int i = 0; i < kRecordCount; ++i) {
      last_offset = stream.ingest_json_record(record_json);
    }
    if (last_offset < 0) {
      fail("ingest returned a negative offset for a non-empty record");
    }

    // One flush covers every offset (acks are monotonic), so a single flush()
    // confirms all 10 records are durable. close() flushes again internally,
    // but calling flush() explicitly first surfaces an ack failure here rather
    // than folded into close()'s teardown.
    stream.flush();
    stream.close();

    // The same stream through the builder.
    zerobus::Stream built = sdk.stream_builder()
                                .table(table_name)
                                .oauth(client_id, client_secret)
                                .json()
                                .build();
    built.ingest_json_record(record_json);
    built.flush();
    built.close();

    std::printf("integration_test: ingested %d records, flushed, closed\n",
                kRecordCount);

    run_multiplexed(sdk, table_name, client_id, client_secret, record_json);
    std::printf("integration_test: multiplexed stream OK\n");
  } catch (const zerobus::ZerobusException& e) {
    fail("live ingestion path threw ZerobusException");
    std::fprintf(stderr, "  what(): %s (retryable=%s)\n", e.what(),
                 e.is_retryable() ? "true" : "false");
  }

  if (g_failures != 0) {
    std::fprintf(stderr, "%d check(s) failed.\n", g_failures);
    return 1;
  }
  return 0;
}
