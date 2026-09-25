// Batch Avro JSON ingestion with the Zerobus C++ SDK.
//
// This example demonstrates high-volume ingestion using the batch API.
// The batch ingestion amortizes the per-call FFI crossing overhead and allows
// a single flush to confirm many records durable. This is the recommended path
// for high throughput.

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

zerobus::Stream open_avro_stream(zerobus::Sdk& sdk,
                                 const std::string& table_name,
                                 const std::string& client_id,
                                 const std::string& client_secret) {
  zerobus::TableProperties props;
  props.table_name = table_name;
  zerobus::StreamOptions options;
  options.record_type = zerobus::RecordType::Avro;
  return sdk.create_stream(props, client_id, client_secret, options);
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
                           .application_name("avro-batch")
                           .build();

    zerobus::Stream stream =
        open_avro_stream(sdk, table_name, client_id, client_secret);

    // Build a batch of JSON records.
    std::vector<std::string> batch = {
        R"({"id": 1, "name": "Alice"})",
        R"({"id": 2, "name": "Bob"})",
        R"({"id": 3, "name": "Carol"})",
    };

    // Ingest the batch at once — one FFI call covers all records.
    // This is faster than per-record calls and returns a single offset
    // representing the batch.
    std::int64_t batch_offset = stream.ingest_avro_records(batch);
    std::cout << "Batch of " << batch.size()
              << " records queued with offset ID: " << batch_offset << "\n";

    // One flush confirms the entire batch durable (acks are monotonic).
    stream.flush();
    stream.close();
    std::cout << "Stream closed successfully.\n";
  } catch (const zerobus::ZerobusException& e) {
    std::cerr << "Zerobus error: " << e.what() << "\n";
    return 1;
  }

  return 0;
}
