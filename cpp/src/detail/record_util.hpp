#ifndef ZEROBUS_DETAIL_RECORD_UTIL_HPP
#define ZEROBUS_DETAIL_RECORD_UTIL_HPP

// Record marshalling shared by Stream and MultiplexedStream: pointer/length
// views for the batch FFI entry points, and copying an FFI-owned CRecordArray
// into owning UnackedRecords.
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "detail/ffi_util.hpp"
#include "zerobus/record.hpp"

namespace zerobus {
namespace detail {

// An empty payload still needs a valid, non-null pointer to pass across the FFI
// (paired with length 0); hand out the address of a static sentinel byte rather
// than nullptr or a dangling data() result.
inline const std::uint8_t* ptr_or_sentinel(
    const std::vector<std::uint8_t>& bytes) {
  static const std::uint8_t kEmptyPayloadSentinel = 0;
  return bytes.empty() ? &kEmptyPayloadSentinel : bytes.data();
}

// Same sentinel for the raw (pointer, length) form, so {nullptr, 0} is a valid
// empty record instead of a null-pointer error.
inline const std::uint8_t* ptr_or_sentinel(const std::uint8_t* data,
                                           std::size_t len) {
  static const std::uint8_t kEmptyPayloadSentinel = 0;
  return len == 0 ? &kEmptyPayloadSentinel : data;
}

// Build the parallel pointer/length arrays the batch FFI entry points expect.
struct ProtoBatchView {
  std::vector<const std::uint8_t*> ptrs;
  std::vector<std::uintptr_t> lens;
};

inline ProtoBatchView make_proto_batch(
    const std::vector<std::vector<std::uint8_t>>& records) {
  ProtoBatchView v;
  v.ptrs.reserve(records.size());
  v.lens.reserve(records.size());
  for (const auto& r : records) {
    v.ptrs.push_back(ptr_or_sentinel(r));
    v.lens.push_back(r.size());
  }
  return v;
}

// JSON records cross the FFI as an array of NUL-terminated C strings, so unlike
// the proto path there is no parallel length array — only the pointers.
struct JsonBatchView {
  std::vector<const char*> ptrs;
};

inline JsonBatchView make_json_batch(const std::vector<std::string>& records) {
  JsonBatchView v;
  v.ptrs.reserve(records.size());
  for (const auto& r : records) {
    // Whole batch fails fast if any record has an embedded NUL.
    v.ptrs.push_back(checked_c_str(r, "JSON record"));
  }
  return v;
}

// Releases the FFI-owned CRecordArray on scope exit, so the array is freed even
// if copying the records into owning UnackedRecords throws (e.g. std::bad_alloc
// while growing the output vector) — not only on the straight-line return.
struct RecordArrayGuard {
  CRecordArray array;
  ~RecordArrayGuard() { zerobus_free_record_array(array); }
  RecordArrayGuard(const RecordArrayGuard&) = delete;
  RecordArrayGuard& operator=(const RecordArrayGuard&) = delete;
};

// Copy each record out of the FFI-owned array into owning UnackedRecords, then
// free the array. The borrowed buffers are only valid until that free, so the
// copy happens first; the guard frees the array even if a copy throws.
inline std::vector<UnackedRecord> take_unacked_records(CRecordArray array) {
  RecordArrayGuard array_guard{array};
  std::vector<UnackedRecord> out;
  if (array.records != nullptr && array.len > 0) {
    out.reserve(array.len);
    for (std::size_t i = 0; i < array.len; ++i) {
      const CRecord& rec = array.records[i];
      std::vector<std::uint8_t> data;
      if (rec.data != nullptr && rec.data_len > 0) {
        data.assign(rec.data, rec.data + rec.data_len);
      }
      out.emplace_back(rec.is_json, std::move(data));
    }
  }
  return out;
}

}  // namespace detail
}  // namespace zerobus

#endif  // ZEROBUS_DETAIL_RECORD_UTIL_HPP
