#ifndef ZEROBUS_DETAIL_MULTIPLEXED_ACK_CALLBACK_HPP
#define ZEROBUS_DETAIL_MULTIPLEXED_ACK_CALLBACK_HPP

#include <cstdint>

namespace zerobus {
namespace detail {

/// C trampolines for the mux create functions' `ack_on_ack` / `ack_on_error`
/// parameters. `user_data` is a heap-allocated
/// `std::shared_ptr<MultiplexedAckCallback>` owned by the FFI, which releases
/// it through `zerobus_cpp_mux_ack_free` exactly once, after the last
/// sub-stream and background task is done with it. `noexcept`: nothing may
/// unwind across the C boundary.
extern "C" void zerobus_cpp_mux_ack_on_ack_trampoline(std::int64_t message_id,
                                                      void* user_data) noexcept;

extern "C" void zerobus_cpp_mux_ack_on_error_trampoline(
    std::int64_t message_id, const char* error_message,
    void* user_data) noexcept;

extern "C" void zerobus_cpp_mux_ack_free(void* user_data) noexcept;

}  // namespace detail
}  // namespace zerobus

#endif  // ZEROBUS_DETAIL_MULTIPLEXED_ACK_CALLBACK_HPP
