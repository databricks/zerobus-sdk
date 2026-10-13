// Single-record Avro JSON ingestion with the Zerobus C++ SDK.
//
// This example opens an Avro stream to a Delta table and ingests records by
// encoding them as JSON objects against the table's writer schema. Like all
// ingestion, prefer queuing records in a loop and flushing once at the end
// rather than waiting per record — this pattern is shown below.
//
// Configuration — every connection setting is read from the environment:
//   ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//   DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET

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
                           .application_name("avro-single")
                           .build();

    zerobus::Stream stream =
        open_avro_stream(sdk, table_name, client_id, client_secret);

    // Ingest records one at a time as JSON, queue-only (no per-record wait).
    // The JSON objects are encoded against the stream's writer schema.
    std::int64_t offset =
        stream.ingest_avro_record(R"({"id": 1, "name": "Alice"})");
    std::cout << "Record 1 queued with offset ID: " << offset << "\n";

    offset = stream.ingest_avro_record(R"({"id": 2, "name": "Bob"})");
    std::cout << "Record 2 queued with offset ID: " << offset << "\n";

    // Flush once at the end, then close.
    stream.flush();
    stream.close();
    std::cout << "Stream closed successfully.\n";
  } catch (const zerobus::ZerobusException& e) {
    std::cerr << "Zerobus error: " << e.what() << "\n";
    return 1;
  }

  return 0;
}
