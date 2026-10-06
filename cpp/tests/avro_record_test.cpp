// Compile+link test for Avro record ingestion API surface.
// This test does not reach the FFI or a live server; it verifies that:
// 1. Pre-encoded bytes and JSON overloads compile and link
// 2. String overloads resolve uniquely

#include <string>
#include <vector>

#include "zerobus/stream.hpp"

namespace {

[[maybe_unused]] void test_avro_overload_signatures() {
#if defined(ZEROBUS_AVRO)
  if (false) {
    zerobus::Stream* stream = nullptr;

    // Pre-encoded bytes (single).
    std::vector<std::uint8_t> bytes_record{1, 2, 3};
    stream->ingest_avro_record(bytes_record);

    // Pre-encoded bytes (batch).
    std::vector<std::vector<std::uint8_t>> bytes_records = {bytes_record};
    stream->ingest_avro_records(bytes_records);

    // JSON (single) — encoded natively via avro-cpp.
    std::string json_record = R"({"id": 1, "name": "test"})";
    stream->ingest_avro_record(json_record);

    // JSON (batch) — each encoded natively via avro-cpp.
    std::vector<std::string> json_records = {json_record};
    stream->ingest_avro_records(json_records);
  }
#endif
}

}  // namespace

int main() {
#if defined(ZEROBUS_AVRO)
  return 0;
#else
  return 0;
#endif
}
