// MultiplexedAckCallback FFI wiring: the trampolines pass the raw message ID
// through unchanged (lane >= 32 IDs are negative), copy the error message,
// ignore null user_data, and the free hook releases the heap shared_ptr once.
// Also the MessageId and MultiplexedStream type contracts. Dependency-free,
// like the other tests: returns non-zero on failure.

#include "zerobus/multiplexed_ack_callback.hpp"

#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "detail/multiplexed_ack_callback.hpp"
#include "zerobus/multiplexed_stream.hpp"

static_assert(!std::is_copy_constructible_v<zerobus::MultiplexedStream>,
              "MultiplexedStream must not be copyable");
static_assert(std::is_move_constructible_v<zerobus::MultiplexedStream>,
              "MultiplexedStream must be movable");

namespace {

using zerobus::MessageId;
using zerobus::MultiplexedAckCallback;
namespace detail = zerobus::detail;

int g_failures = 0;

void check(const std::string& what, bool ok) {
  if (!ok) {
    std::fprintf(stderr, "FAIL: %s\n", what.c_str());
    ++g_failures;
  }
}

class Recorder : public MultiplexedAckCallback {
 public:
  void on_ack(MessageId id) noexcept override { acks.push_back(id.raw()); }
  void on_error(MessageId id, const std::string& message) noexcept override {
    errors.emplace_back(id.raw(), message);
  }

  std::vector<std::int64_t> acks;
  std::vector<std::pair<std::int64_t, std::string>> errors;
};

// A lane-63 ID: the sign bit is set, so the raw value is negative.
constexpr std::int64_t kNegativeId = (std::int64_t{63} << 58) | 7;

void trampolines_pass_ids_and_messages() {
  check("the lane-63 test ID is negative", kNegativeId < 0);
  auto recorder = std::make_shared<Recorder>();
  // As Sdk::open_multiplexed_stream does: a heap shared_ptr is the user_data.
  void* user_data = new std::shared_ptr<MultiplexedAckCallback>(recorder);

  detail::zerobus_cpp_mux_ack_on_ack_trampoline(kNegativeId, user_data);
  detail::zerobus_cpp_mux_ack_on_ack_trampoline(0, user_data);
  detail::zerobus_cpp_mux_ack_on_error_trampoline(kNegativeId, "kaboom",
                                                  user_data);
  detail::zerobus_cpp_mux_ack_on_error_trampoline(5, nullptr, user_data);
  check("on_ack IDs pass through unchanged",
        recorder->acks == std::vector<std::int64_t>{kNegativeId, 0});
  check("on_error ID and message pass through",
        recorder->errors.size() == 2 &&
            recorder->errors[0] ==
                std::make_pair(kNegativeId, std::string("kaboom")) &&
            recorder->errors[1] ==
                std::make_pair(std::int64_t{5}, std::string()));

  // Null user_data is ignored, not dereferenced.
  detail::zerobus_cpp_mux_ack_on_ack_trampoline(1, nullptr);
  detail::zerobus_cpp_mux_ack_on_error_trampoline(1, "x", nullptr);

  const long before_free = recorder.use_count();
  detail::zerobus_cpp_mux_ack_free(user_data);
  check("the free hook releases its shared_ptr once",
        recorder.use_count() == before_free - 1);
}

void from_adapter_dispatches() {
  std::vector<std::int64_t> acks;
  std::vector<std::pair<std::int64_t, std::string>> errors;
  auto both = MultiplexedAckCallback::from(
      [&](MessageId id) { acks.push_back(id.raw()); },
      [&](MessageId id, const std::string& message) {
        errors.emplace_back(id.raw(), message);
      });
  detail::zerobus_cpp_mux_ack_on_ack_trampoline(kNegativeId, &both);
  detail::zerobus_cpp_mux_ack_on_error_trampoline(kNegativeId, "bad", &both);
  check("from(): on_ack gets the ID",
        acks == std::vector<std::int64_t>{kNegativeId});
  check("from(): on_error gets the ID and message",
        errors.size() == 1 &&
            errors[0] == std::make_pair(kNegativeId, std::string("bad")));

  // An omitted error handler ignores errors.
  auto ack_only = MultiplexedAckCallback::from([](MessageId) {});
  detail::zerobus_cpp_mux_ack_on_error_trampoline(1, "ignored", &ack_only);
}

void message_id_equality_and_hash() {
  const MessageId a(kNegativeId), b(kNegativeId), c(0);
  check("equal raws are equal", a == b && !(a != b));
  check("different raws differ", a != c);
  check("equal IDs hash the same",
        std::hash<MessageId>{}(a) == std::hash<MessageId>{}(b));
  std::unordered_set<MessageId> ids{a, b, c};
  check("an unordered_set dedups equal IDs", ids.size() == 2);
}

}  // namespace

int main() {
  trampolines_pass_ids_and_messages();
  from_adapter_dispatches();
  message_id_equality_and_hash();

  if (g_failures != 0) {
    std::fprintf(stderr, "%d mux ack callback check(s) failed.\n", g_failures);
    return 1;
  }
  std::printf("multiplexed ack callback wiring OK\n");
  return 0;
}
