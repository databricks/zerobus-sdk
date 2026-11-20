// MultiplexedAckCallback::from and the extern "C" trampolines the core uses to
// deliver mux acks/errors (declared in detail/multiplexed_ack_callback.hpp).
#include "detail/multiplexed_ack_callback.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "zerobus/multiplexed_ack_callback.hpp"

namespace zerobus {

namespace {

// MultiplexedAckCallback backed by std::function handlers. The error handler
// may be empty (errors ignored).
class LambdaMultiplexedAckCallback : public MultiplexedAckCallback {
 public:
  LambdaMultiplexedAckCallback(
      std::function<void(MessageId)> on_ack,
      std::function<void(MessageId, const std::string&)> on_error)
      : on_ack_(std::move(on_ack)), on_error_(std::move(on_error)) {}

  void on_ack(MessageId id) noexcept override {
    if (on_ack_) {
      on_ack_(id);
    }
  }

  void on_error(MessageId id,
                const std::string& error_message) noexcept override {
    if (on_error_) {
      on_error_(id, error_message);
    }
  }

 private:
  std::function<void(MessageId)> on_ack_;
  std::function<void(MessageId, const std::string&)> on_error_;
};

using OwnedCallback = std::shared_ptr<MultiplexedAckCallback>;

}  // namespace

std::shared_ptr<MultiplexedAckCallback> MultiplexedAckCallback::from(
    std::function<void(MessageId)> on_ack,
    std::function<void(MessageId, const std::string&)> on_error) {
  return std::make_shared<LambdaMultiplexedAckCallback>(std::move(on_ack),
                                                        std::move(on_error));
}

namespace detail {

extern "C" void zerobus_cpp_mux_ack_on_ack_trampoline(
    std::int64_t message_id, void* user_data) noexcept {
  if (user_data == nullptr) {
    return;
  }
  (*static_cast<OwnedCallback*>(user_data))->on_ack(MessageId(message_id));
}

extern "C" void zerobus_cpp_mux_ack_on_error_trampoline(
    std::int64_t message_id, const char* error_message,
    void* user_data) noexcept {
  if (user_data == nullptr) {
    return;
  }
  // error_message is borrowed for this call only; copy it (empty on null).
  std::string message = error_message != nullptr ? error_message : "";
  (*static_cast<OwnedCallback*>(user_data))
      ->on_error(MessageId(message_id), message);
}

extern "C" void zerobus_cpp_mux_ack_free(void* user_data) noexcept {
  delete static_cast<OwnedCallback*>(user_data);
}

}  // namespace detail
}  // namespace zerobus
