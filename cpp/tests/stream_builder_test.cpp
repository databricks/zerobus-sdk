// StreamBuilder and MultiplexedStreamBuilder: the settings-to-C mapping,
// validation errors, reuse, and the native bridges a failed build() must
// release. Nothing here connects: a
// non-empty malformed descriptor makes the FFI create fail before any I/O.
// Dependency-free, like the other tests: returns non-zero on failure.

#include "zerobus/stream_builder.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "detail/stream_builder_access.hpp"
#include "zerobus/error.hpp"
#include "zerobus/sdk.hpp"

namespace {

using zerobus::CallbackWaitPolicy;
using zerobus::CStreamConfigurationOptions;
using zerobus::MultiplexedAckCallback;
using zerobus::MultiplexedStreamBuilder;
using zerobus::Sdk;
using zerobus::StreamBuilder;
using zerobus::ZerobusException;
using zerobus::detail::StreamBuilderAccess;

int g_failures = 0;

void check(const std::string& what, bool ok) {
  if (!ok) {
    std::fprintf(stderr, "FAIL: %s\n", what.c_str());
    ++g_failures;
  }
}

template <typename A, typename B>
void check_eq(const std::string& what, A actual, B expected) {
  if (static_cast<std::uint64_t>(actual) !=
      static_cast<std::uint64_t>(expected)) {
    std::fprintf(stderr, "FAIL: %s: got %llu, want %llu\n", what.c_str(),
                 static_cast<unsigned long long>(actual),
                 static_cast<unsigned long long>(expected));
    ++g_failures;
  }
}

// Field by field: memcmp would also compare padding.
void check_same_c_options(const std::string& what,
                          const CStreamConfigurationOptions& a,
                          const CStreamConfigurationOptions& b) {
  check_eq(what + ": max_inflight_requests", a.max_inflight_requests,
           b.max_inflight_requests);
  check_eq(what + ": recovery", a.recovery, b.recovery);
  check_eq(what + ": recovery_timeout_ms", a.recovery_timeout_ms,
           b.recovery_timeout_ms);
  check_eq(what + ": recovery_backoff_ms", a.recovery_backoff_ms,
           b.recovery_backoff_ms);
  check_eq(what + ": recovery_retries", a.recovery_retries, b.recovery_retries);
  check_eq(what + ": server_lack_of_ack_timeout_ms",
           a.server_lack_of_ack_timeout_ms, b.server_lack_of_ack_timeout_ms);
  check_eq(what + ": flush_timeout_ms", a.flush_timeout_ms, b.flush_timeout_ms);
  check_eq(what + ": record_type", a.record_type, b.record_type);
  check_eq(what + ": has_stream_paused_max_wait_time_ms",
           a.has_stream_paused_max_wait_time_ms,
           b.has_stream_paused_max_wait_time_ms);
  check_eq(what + ": stream_paused_max_wait_time_ms",
           a.stream_paused_max_wait_time_ms, b.stream_paused_max_wait_time_ms);
  check_eq(what + ": has_callback_max_wait_time_ms",
           a.has_callback_max_wait_time_ms, b.has_callback_max_wait_time_ms);
  check_eq(what + ": callback_max_wait_time_ms", a.callback_max_wait_time_ms,
           b.callback_max_wait_time_ms);
  check(what + ": ack fields", a.ack_on_ack == b.ack_on_ack &&
                                   a.ack_on_error == b.ack_on_error &&
                                   a.ack_user_data == b.ack_user_data);
}

Sdk local_sdk() {
  return Sdk::builder()
      .endpoint("http://127.0.0.1:1")
      .unity_catalog_url("http://127.0.0.1:1")
      .disable_tls()
      .build();
}

// Not a valid DescriptorProto, so the FFI create fails before connecting.
std::vector<std::uint8_t> malformed_descriptor() { return {0xff, 0xff, 0xff}; }

class NoopProvider : public zerobus::HeadersProvider {
 public:
  std::map<std::string, std::string> get_headers() override { return {}; }
};

class NoopAckCallback : public zerobus::AckCallback {
 public:
  void on_ack(std::int64_t) noexcept override {}
  void on_error(std::int64_t, const std::string&) noexcept override {}
};

// Runs @p call and returns the ZerobusException it throws; records a failure
// if it throws nothing.
std::optional<ZerobusException> thrown(const std::string& what,
                                       const std::function<void()>& call) {
  try {
    call();
  } catch (const ZerobusException& e) {
    return e;
  }
  check(what + ": must throw ZerobusException", false);
  return std::nullopt;
}

// validate() and build() both throw a non-retryable ZerobusException whose
// message contains @p message.
template <typename Builder>
void check_rejected(const std::string& what, const Builder& builder,
                    const std::string& message) {
  for (bool build : {false, true}) {
    const std::string call = what + (build ? ": build()" : ": validate()");
    auto e = thrown(call, [&] {
      if (build) {
        builder.build();
      } else {
        builder.validate();
      }
    });
    if (!e) {
      continue;
    }
    check(call + ": non-retryable", !e->is_retryable());
    check(call + ": message '" + e->what() + "' contains '" + message + "'",
          std::string(e->what()).find(message) != std::string::npos);
  }
}

// The message of a pre-connect build() failure.
template <typename Builder>
std::string build_failure(const std::string& what, const Builder& builder) {
  auto e = thrown(what, [&] { builder.build(); });
  return e ? e->what() : std::string();
}

// R5-1: an unset builder converts like create_stream with default
// StreamOptions; every setter, with distinct values and with 0, reaches its
// own C field; the callback wait policy maps its states.
void builder_converts_options_like_create_stream(Sdk& sdk) {
  check_same_c_options("unset builder",
                       StreamBuilderAccess::c_options(
                           sdk.stream_builder()
                               .table("main.default.t")
                               .oauth("id", "secret")
                               .compiled_proto(malformed_descriptor())),
                       zerobus::detail::to_c(zerobus::StreamOptions{}));

  const CStreamConfigurationOptions seed =
      zerobus::zerobus_get_default_config();
  struct Case {
    const char* name;
    std::function<void(StreamBuilder&)> set;
    std::function<void(const std::string&, const CStreamConfigurationOptions&)>
        expect;
  };
  const std::vector<Case> cases = {
      {"every scalar set to a distinct value",
       [](auto& b) {
         b.max_inflight_requests(101)
             .recovery(false)
             .recovery_timeout_ms(102)
             .recovery_backoff_ms(103)
             .recovery_retries(104)
             .server_lack_of_ack_timeout_ms(105)
             .flush_timeout_ms(106)
             .stream_paused_max_wait_time_ms(107);
       },
       [](auto& w, auto& c) {
         check_eq(w + ": max_inflight_requests", c.max_inflight_requests, 101);
         check_eq(w + ": recovery", c.recovery, false);
         check_eq(w + ": recovery_timeout_ms", c.recovery_timeout_ms, 102);
         check_eq(w + ": recovery_backoff_ms", c.recovery_backoff_ms, 103);
         check_eq(w + ": recovery_retries", c.recovery_retries, 104);
         check_eq(w + ": server_lack_of_ack_timeout_ms",
                  c.server_lack_of_ack_timeout_ms, 105);
         check_eq(w + ": flush_timeout_ms", c.flush_timeout_ms, 106);
         check_eq(w + ": has_stream_paused_max_wait_time_ms",
                  c.has_stream_paused_max_wait_time_ms, true);
         check_eq(w + ": stream_paused_max_wait_time_ms",
                  c.stream_paused_max_wait_time_ms, 107);
       }},
      {"every scalar set to 0",
       [](auto& b) {
         b.max_inflight_requests(0)
             .recovery_timeout_ms(0)
             .recovery_backoff_ms(0)
             .recovery_retries(0)
             .server_lack_of_ack_timeout_ms(0)
             .flush_timeout_ms(0)
             .stream_paused_max_wait_time_ms(0);
       },
       [](auto& w, auto& c) {
         check_eq(w + ": max_inflight_requests", c.max_inflight_requests, 0);
         check_eq(w + ": recovery_timeout_ms", c.recovery_timeout_ms, 0);
         check_eq(w + ": recovery_backoff_ms", c.recovery_backoff_ms, 0);
         check_eq(w + ": recovery_retries", c.recovery_retries, 0);
         check_eq(w + ": server_lack_of_ack_timeout_ms",
                  c.server_lack_of_ack_timeout_ms, 0);
         check_eq(w + ": flush_timeout_ms", c.flush_timeout_ms, 0);
         check_eq(w + ": has_stream_paused_max_wait_time_ms",
                  c.has_stream_paused_max_wait_time_ms, true);
         check_eq(w + ": stream_paused_max_wait_time_ms",
                  c.stream_paused_max_wait_time_ms, 0);
       }},
      {"stream_paused_max_wait_time_ms(nullopt)",
       [](auto& b) {
         b.stream_paused_max_wait_time_ms(16);
         b.stream_paused_max_wait_time_ms(std::nullopt);
       },
       [](auto& w, auto& c) {
         check_eq(w + " has", c.has_stream_paused_max_wait_time_ms, false);
       }},
      {"callback_wait_policy(forever())",
       [](auto& b) { b.callback_wait_policy(CallbackWaitPolicy::forever()); },
       [](auto& w, auto& c) {
         check_eq(w + " has", c.has_callback_max_wait_time_ms, false);
       }},
      {"callback_wait_policy(duration(1234))",
       [](auto& b) {
         b.callback_wait_policy(CallbackWaitPolicy::duration(1234));
       },
       [](auto& w, auto& c) {
         check_eq(w + " has", c.has_callback_max_wait_time_ms, true);
         check_eq(w, c.callback_max_wait_time_ms, 1234);
       }},
      {"callback_wait_policy(duration(0))",
       [](auto& b) { b.callback_wait_policy(CallbackWaitPolicy::duration(0)); },
       [](auto& w, auto& c) {
         check_eq(w + " has", c.has_callback_max_wait_time_ms, true);
         check_eq(w, c.callback_max_wait_time_ms, 0);
       }},
      {"callback_wait_policy(use_default())",
       [](auto& b) {
         b.callback_wait_policy(CallbackWaitPolicy::forever());
         b.callback_wait_policy(CallbackWaitPolicy::use_default());
       },
       [&seed](auto& w, auto& c) {
         check_eq(w + " has", c.has_callback_max_wait_time_ms,
                  seed.has_callback_max_wait_time_ms);
         check_eq(w, c.callback_max_wait_time_ms,
                  seed.callback_max_wait_time_ms);
       }},
      {"json()", [](auto& b) { b.json(); },
       [](auto& w, auto& c) {
         check_eq(w, c.record_type,
                  static_cast<int>(zerobus::RecordType::Json));
       }},
  };
  for (const Case& c : cases) {
    StreamBuilder builder = sdk.stream_builder();
    c.set(builder);
    c.expect(c.name, StreamBuilderAccess::c_options(builder));
  }
}

// R5-2: each bad state is rejected by validate() and build(), never by a
// setter; the last auth and format setters win.
void builder_validation_errors(Sdk& sdk) {
  const StreamBuilder complete =
      sdk.stream_builder().table("main.default.t").oauth("id", "secret").json();
  struct Case {
    const char* name;
    StreamBuilder builder;
    const char* message;
  };
  const std::vector<Case> cases = {
      {"missing table", sdk.stream_builder().oauth("id", "secret").json(),
       "table name is required"},
      {"missing auth", sdk.stream_builder().table("main.default.t").json(),
       "authentication is required"},
      {"missing format",
       sdk.stream_builder().table("main.default.t").oauth("id", "secret"),
       "record format is required"},
      {"headers_provider(nullptr)",
       StreamBuilder(complete).headers_provider(nullptr),
       "headers_provider must not be null"},
      // The last auth setter wins, so this null provider is checked.
      {"oauth then headers_provider(nullptr)",
       StreamBuilder(complete).oauth("id", "secret").headers_provider(nullptr),
       "headers_provider must not be null"},
      {"max_inflight_requests(0)",
       StreamBuilder(complete).max_inflight_requests(0),
       "max_inflight_requests must be at least 1"},
  };
  for (const Case& c : cases) {
    check_rejected(c.name, c.builder, c.message);
  }
  // The mux builder runs the same checks first.
  check_rejected(
      "multiplexed(2) missing table",
      sdk.stream_builder().oauth("id", "secret").json().multiplexed(2),
      "table name is required");

  // OAuth set after a null provider wins, so validate() passes.
  try {
    StreamBuilder(complete)
        .headers_provider(nullptr)
        .oauth("id", "secret")
        .validate();
  } catch (const ZerobusException& e) {
    check(std::string("headers_provider(nullptr) then oauth: ") + e.what(),
          false);
  }
  // The last format setter wins.
  check("json then compiled_proto",
        StreamBuilderAccess::options(
            StreamBuilder(complete).json().compiled_proto({1}))
                .record_type == zerobus::RecordType::Proto);
  check("compiled_proto then json",
        StreamBuilderAccess::options(
            StreamBuilder(complete).compiled_proto({1}).json())
                .record_type == zerobus::RecordType::Json);
}

// R5-3: the mux checks run before the FFI, with the core's wording; the
// boundaries are accepted.
void mux_builder_validation(Sdk& sdk) {
  const StreamBuilder base =
      sdk.stream_builder().table("main.default.t").oauth("id", "secret").json();
  const auto ordinary = std::make_shared<NoopAckCallback>();
  const auto mux_callback = MultiplexedAckCallback::from([](auto) {});
  struct Case {
    const char* name;
    MultiplexedStreamBuilder builder;
    const char* message;
  };
  const std::vector<Case> cases = {
      {"multiplexed(0)", base.multiplexed(0),
       "multiplexed stream_count must be between 1 and 64"},
      {"multiplexed(65)", base.multiplexed(65),
       "multiplexed stream_count must be between 1 and 64"},
      {"max_inflight_requests(2).multiplexed(3)",
       StreamBuilder(base).max_inflight_requests(2).multiplexed(3),
       "max_inflight_requests (2) must be at least stream_count (3) for "
       "multiplexed streams"},
      {"ack_callback then multiplexed(2)",
       StreamBuilder(base).ack_callback(ordinary).multiplexed(2),
       "ack_callback is only valid for ordinary streams; use "
       "multiplexed(n).ack_callback(...)"},
      {"ack_callback then multiplexed(2).ack_callback",
       StreamBuilder(base).ack_callback(ordinary).multiplexed(2).ack_callback(
           mux_callback),
       "ack_callback is only valid for ordinary streams; use "
       "multiplexed(n).ack_callback(...)"},
  };
  for (const Case& c : cases) {
    check_rejected(c.name, c.builder, c.message);
  }

  const std::vector<std::pair<const char*, MultiplexedStreamBuilder>> accepted =
      {{"multiplexed(64)", base.multiplexed(64)},
       {"max_inflight_requests(2).multiplexed(2)",
        StreamBuilder(base).max_inflight_requests(2).multiplexed(2)}};
  for (const auto& [name, builder] : accepted) {
    try {
      builder.validate();
    } catch (const ZerobusException& e) {
      check(std::string(name) + " must be accepted: " + e.what(), false);
    }
  }
}

// R5-4: copies and multiplexed(n) snapshots are independent, validate()
// repeats, a failed build() leaves the builder usable, the caller's strings
// and vectors are copied, and the builder survives a move of its Sdk.
void builder_snapshot_copies_and_sdk_move() {
  Sdk sdk = local_sdk();
  StreamBuilder original = sdk.stream_builder()
                               .table("main.default.t")
                               .oauth("id", "secret")
                               .compiled_proto(malformed_descriptor())
                               .flush_timeout_ms(21);
  StreamBuilder copy = original;
  copy.flush_timeout_ms(22);
  check_eq("a copy's change leaves the original",
           StreamBuilderAccess::options(original).flush_timeout_ms, 21);

  original.validate();
  original.validate();
  const std::string first = build_failure("first build()", original);
  const std::string second = build_failure("second build()", original);
  check("a failed build() leaves the builder usable: '" + first + "' vs '" +
            second + "'",
        !first.empty() && first == second);

  // The builder copies the table name and the descriptor (ASan would flag a
  // dangling reference here).
  StreamBuilder from_temporaries = sdk.stream_builder().oauth("id", "secret");
  {
    std::string table = "main.default.t";
    std::vector<std::uint8_t> descriptor = malformed_descriptor();
    from_temporaries.table(table).compiled_proto(descriptor);
    table.assign("x");
    descriptor.clear();
  }
  check("the builder copied the table name",
        StreamBuilderAccess::table_name(from_temporaries) ==
            std::optional<std::string>("main.default.t"));
  check("the builder copied the descriptor",
        StreamBuilderAccess::descriptor(from_temporaries) ==
            malformed_descriptor());

  // multiplexed(n) takes a snapshot: later changes to the ordinary builder
  // don't reach it.
  StreamBuilder ordinary = original;
  const MultiplexedStreamBuilder mux = ordinary.multiplexed(2);
  ordinary.flush_timeout_ms(23).table("main.default.other");
  check_eq("the mux builder keeps its snapshot",
           StreamBuilderAccess::options(StreamBuilderAccess::base(mux))
               .flush_timeout_ms,
           21);
  check("the mux builder keeps its table",
        StreamBuilderAccess::table_name(StreamBuilderAccess::base(mux)) ==
            std::optional<std::string>("main.default.t"));

  Sdk moved = std::move(sdk);
  check_eq("build() after the Sdk moved",
           build_failure("build() after the Sdk moved", original) == first,
           true);
}

// R5-5: a failed build() (ordinary and mux) releases the bridges it allocated;
// an abandoned builder releases its copies.
void builder_bridge_ownership() {
  Sdk sdk = local_sdk();
  auto provider = std::make_shared<NoopProvider>();
  auto callback = std::make_shared<NoopAckCallback>();
  const long provider_alone = provider.use_count();
  const long callback_alone = callback.use_count();
  {
    StreamBuilder builder = sdk.stream_builder()
                                .table("main.default.t")
                                .headers_provider(provider)
                                .compiled_proto(malformed_descriptor())
                                .ack_callback(callback);
    const long provider_held = provider.use_count();
    const long callback_held = callback.use_count();
    check("the builder holds the provider", provider_held > provider_alone);
    check("the builder holds the callback", callback_held > callback_alone);
    for (int i = 0; i < 2; ++i) {
      build_failure("headers build() with a malformed descriptor", builder);
      check_eq("provider released after failed build() " + std::to_string(i),
               provider.use_count(), provider_held);
      check_eq("callback released after failed build() " + std::to_string(i),
               callback.use_count(), callback_held);
    }
  }
  check_eq("abandoned builder releases the provider", provider.use_count(),
           provider_alone);
  check_eq("abandoned builder releases the callback", callback.use_count(),
           callback_alone);

  // The mux path: the FFI frees the ack bridge exactly once, possibly after
  // build() returns, so wait (bounded) for the count to come back.
  auto mux_callback = MultiplexedAckCallback::from([](auto) {});
  const long mux_alone = mux_callback.use_count();
  {
    const MultiplexedStreamBuilder builder =
        sdk.stream_builder()
            .table("main.default.t")
            .headers_provider(provider)
            .compiled_proto(malformed_descriptor())
            .multiplexed(2)
            .ack_callback(mux_callback);
    const long provider_held = provider.use_count();
    const long mux_held = mux_callback.use_count();
    check("the mux builder holds the callback", mux_held > mux_alone);
    for (int i = 0; i < 2; ++i) {
      build_failure("mux build() with a malformed descriptor", builder);
      const auto deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(5);
      while ((provider.use_count() != provider_held ||
              mux_callback.use_count() != mux_held) &&
             std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      check_eq(
          "mux: provider released after failed build() " + std::to_string(i),
          provider.use_count(), provider_held);
      check_eq(
          "mux: callback released after failed build() " + std::to_string(i),
          mux_callback.use_count(), mux_held);
    }
  }
  check_eq("abandoned mux builder releases the provider", provider.use_count(),
           provider_alone);
  check_eq("abandoned mux builder releases the callback",
           mux_callback.use_count(), mux_alone);
}

}  // namespace

int main() {
  Sdk sdk = local_sdk();
  builder_converts_options_like_create_stream(sdk);
  builder_validation_errors(sdk);
  mux_builder_validation(sdk);
  builder_snapshot_copies_and_sdk_move();
  builder_bridge_ownership();

  if (g_failures != 0) {
    std::fprintf(stderr, "%d stream builder check(s) failed.\n", g_failures);
    return 1;
  }
  std::printf("stream builder OK\n");
  return 0;
}
