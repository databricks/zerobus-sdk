// JSON ingestion over a multiplexed stream (Beta) with the Zerobus C++ SDK.
//
// A multiplexed stream opens several sub-streams to one table behind one
// handle and sends each ingest call to the next one, round-robin, to raise
// throughput when one stream is the bottleneck. Records keep their order within
// a sub-stream, not across them. Ingest in a loop, then flush ONCE: a wait per
// record would collapse throughput. The ack callback counts acknowledgments.
//
// Configuration — every connection setting is read from the environment.
// Export these before running (see ../README.md):
//   ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//   DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
//
//       ./build/examples/json_multiplexed
//
// Target table: the orders table from ../README.md.

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "zerobus/zerobus.hpp"

namespace {

std::string require_env(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    std::cerr << "error: environment variable " << name << " is not set.\n";
    std::exit(2);
  }
  return value;
}

}  // namespace

int main() {
  const std::string server_endpoint = require_env("ZEROBUS_SERVER_ENDPOINT");
  const std::string workspace_url = require_env("DATABRICKS_WORKSPACE_URL");
  const std::string table_name = require_env("ZEROBUS_TABLE_NAME");
  const std::string client_id = require_env("DATABRICKS_CLIENT_ID");
  const std::string client_secret = require_env("DATABRICKS_CLIENT_SECRET");

  try {
    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(server_endpoint)
                           .unity_catalog_url(workspace_url)
                           .application_name("json-multiplexed")
                           .build();

    // Sub-streams call back concurrently, so the counter is atomic. The SDK
    // can call the callback until every sub-stream has released it, so it
    // captures only state that outlives the stream.
    static std::atomic<int> acked{0};
    auto ack_callback = zerobus::MultiplexedAckCallback::from(
        [](zerobus::MessageId) noexcept { acked.fetch_add(1); },
        [](zerobus::MessageId, const std::string& message) noexcept {
          std::cerr << "Ingest failed: " << message << "\n";
        });

    zerobus::MultiplexedStream stream = sdk.stream_builder()
                                            .table(table_name)
                                            .oauth(client_id, client_secret)
                                            .json()
                                            .multiplexed(4)
                                            .ack_callback(ack_callback)
                                            .build();

    try {
      // Send batches, at least one per sub-stream, so all four carry load.
      for (int batch = 0; batch < 8; ++batch) {
        std::vector<std::string> records;
        for (int i = 0; i < 25; ++i) {
          const int id = batch * 25 + i + 1;
          records.push_back(
              "{\"id\": " + std::to_string(id) +
              ", \"customer_name\": \"Alice\", \"product_name\": \"Mouse\", "
              "\"quantity\": 1, \"price\": 25.99, \"status\": \"pending\"}");
        }
        stream.ingest_json_records(records);  // queue only — no per-batch wait
      }
      stream.flush();
      stream.close();
      std::cout << "8 batches acknowledged (" << acked.load()
                << " ack callbacks so far). Stream closed.\n";
    } catch (const zerobus::ZerobusException& e) {
      // A sub-stream failed, during an ingest, the flush or the close: recover
      // what was not acknowledged and send it on a new stream.
      std::cerr << "Stream failed: " << e.what() << "\n";
      std::vector<zerobus::UnackedRecord> unacked =
          stream.get_unacked_records();
      std::cerr << unacked.size() << " records to send again.\n";
      return 1;
    }
  } catch (const zerobus::ZerobusException& e) {
    std::cerr << "Zerobus error: " << e.what()
              << " (retryable=" << (e.is_retryable() ? "true" : "false")
              << ")\n";
    return 1;
  }
  return 0;
}
