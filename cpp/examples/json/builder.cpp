// JSON ingestion through the stream builder (Beta) with the Zerobus C++ SDK.
//
// Sdk::stream_builder() collects the table, authentication, record format and
// any option you want to change, then build() opens the stream. It creates the
// same stream as Sdk::create_stream. This example ingests a batch of records in
// a loop and flushes ONCE at the end: ingest_json_record() returns as soon as
// the record is queued, so a per-record wait would collapse throughput.
//
// Configuration — every connection setting is read from the environment.
// Export these before running (see ../README.md):
//   ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//   DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
//
//       ./build/examples/json_builder
//
// Target table: the orders table from ../README.md.

#include <cstdlib>
#include <iostream>
#include <string>

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
                           .application_name("json-builder")
                           .build();

    // Unset options keep their defaults; set only what you need.
    zerobus::Stream stream = sdk.stream_builder()
                                 .table(table_name)
                                 .oauth(client_id, client_secret)
                                 .json()
                                 .max_inflight_requests(50'000)
                                 .build();

    // Ingest in a loop: each call only queues the record.
    for (int id = 1; id <= 100; ++id) {
      stream.ingest_json_record(
          "{\"id\": " + std::to_string(id) +
          ", \"customer_name\": \"Alice\", \"product_name\": \"Mouse\", "
          "\"quantity\": 1, \"price\": 25.99, \"status\": \"pending\"}");
    }

    // Wait once for every queued record, then close at a controlled point.
    stream.flush();
    stream.close();
    std::cout << "100 records acknowledged. Stream closed.\n";
  } catch (const zerobus::ZerobusException& e) {
    std::cerr << "Zerobus error: " << e.what()
              << " (retryable=" << (e.is_retryable() ? "true" : "false")
              << ")\n";
    return 1;
  }
  return 0;
}
