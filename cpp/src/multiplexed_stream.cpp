// Implementation of MultiplexedStream (declared in
// zerobus/multiplexed_stream.hpp): a thin forwarding layer over the
// zerobus_multiplexed_stream_* C FFI. Message IDs can be negative, so success
// always comes from the CResult, never from the sign of a return value.
#include "zerobus/multiplexed_stream.hpp"

#include <utility>

#include "detail/ffi_util.hpp"
#include "detail/record_util.hpp"

namespace zerobus {

namespace {

// Only a moved-from stream has a null handle; close() keeps the handle until
// destruction, so the native stream reports its own closed errors.
void ensure_valid(const CZerobusMultiplexedStream* handle) {
  if (handle == nullptr) {
    throw ZerobusException("MultiplexedStream has been moved from", false);
  }
}

// The FFI returns this for an empty batch, with success set.
constexpr std::int64_t kEmptyBatch = -2;

// An empty batch still crosses the FFI, so a closed mux reports its error as
// for any other batch. The FFI rejects null arrays even for zero records, so
// pass these placeholders instead of an empty vector's data().
const std::uint8_t* const kNoProtoRecords[1] = {nullptr};
const std::uintptr_t kNoProtoLens[1] = {0};
const char* const kNoJsonRecords[1] = {nullptr};

std::optional<MessageId> batch_id(std::int64_t id) {
  if (id == kEmptyBatch) {
    return std::nullopt;
  }
  return MessageId(id);
}

// Close (best effort) and free a handle that this object owns.
void release(CZerobusMultiplexedStream* handle) noexcept {
  if (handle != nullptr) {
    detail::ResultGuard guard;
    zerobus_multiplexed_stream_close(handle, guard.ptr());
    zerobus_multiplexed_stream_free(handle);
  }
}

}  // namespace

MultiplexedStream::~MultiplexedStream() { release(handle_); }

MultiplexedStream::MultiplexedStream(MultiplexedStream&& other) noexcept
    : handle_(other.handle_) {
  other.handle_ = nullptr;
}

MultiplexedStream& MultiplexedStream::operator=(
    MultiplexedStream&& other) noexcept {
  if (this != &other) {
    release(handle_);
    handle_ = other.handle_;
    other.handle_ = nullptr;
  }
  return *this;
}

MessageId MultiplexedStream::ingest_proto_record(const std::uint8_t* data,
                                                 std::size_t len) {
  ensure_valid(handle_);
  detail::ResultGuard guard;
  std::int64_t id = zerobus_multiplexed_stream_ingest_proto_record(
      handle_, detail::ptr_or_sentinel(data, len), len, guard.ptr());
  guard.throw_if_error();
  return MessageId(id);
}

MessageId MultiplexedStream::ingest_proto_record(
    const std::vector<std::uint8_t>& data) {
  return ingest_proto_record(detail::ptr_or_sentinel(data), data.size());
}

MessageId MultiplexedStream::ingest_json_record(const std::string& json) {
  ensure_valid(handle_);
  detail::ResultGuard guard;
  std::int64_t id = zerobus_multiplexed_stream_ingest_json_record(
      handle_, detail::checked_c_str(json, "JSON record"), guard.ptr());
  guard.throw_if_error();
  return MessageId(id);
}

std::optional<MessageId> MultiplexedStream::ingest_proto_records(
    const std::vector<std::vector<std::uint8_t>>& records) {
  ensure_valid(handle_);
  detail::ProtoBatchView v = detail::make_proto_batch(records);
  const bool empty = records.empty();
  detail::ResultGuard guard;
  std::int64_t id = zerobus_multiplexed_stream_ingest_proto_records(
      handle_, empty ? kNoProtoRecords : v.ptrs.data(),
      empty ? kNoProtoLens : v.lens.data(), v.ptrs.size(), guard.ptr());
  guard.throw_if_error();
  return batch_id(id);
}

std::optional<MessageId> MultiplexedStream::ingest_json_records(
    const std::vector<std::string>& records) {
  ensure_valid(handle_);
  detail::JsonBatchView v = detail::make_json_batch(records);
  const bool empty = records.empty();
  detail::ResultGuard guard;
  std::int64_t id = zerobus_multiplexed_stream_ingest_json_records(
      handle_, empty ? kNoJsonRecords : v.ptrs.data(), v.ptrs.size(),
      guard.ptr());
  guard.throw_if_error();
  return batch_id(id);
}

void MultiplexedStream::wait_for_message_id(MessageId id) {
  ensure_valid(handle_);
  detail::ResultGuard guard;
  zerobus_multiplexed_stream_wait_for_message_id(handle_, id.raw(),
                                                 guard.ptr());
  guard.throw_if_error();
}

void MultiplexedStream::flush() {
  ensure_valid(handle_);
  detail::ResultGuard guard;
  zerobus_multiplexed_stream_flush(handle_, guard.ptr());
  guard.throw_if_error();
}

// Always calls the native close, even if is_closed() is already true: a mux is
// "closed" as soon as one sub-stream fails, while the healthy ones still hold
// accepted records that close flushes.
void MultiplexedStream::close() {
  ensure_valid(handle_);
  detail::ResultGuard guard;
  zerobus_multiplexed_stream_close(handle_, guard.ptr());
  guard.throw_if_error();
}

bool MultiplexedStream::is_closed() const noexcept {
  return handle_ == nullptr || zerobus_multiplexed_stream_is_closed(handle_);
}

std::vector<UnackedRecord> MultiplexedStream::get_unacked_records() {
  ensure_valid(handle_);
  detail::ResultGuard guard;
  CRecordArray array =
      zerobus_multiplexed_stream_get_unacked_records(handle_, guard.ptr());
  guard.throw_if_error();
  return detail::take_unacked_records(array);
}

}  // namespace zerobus
