// get_token: the FFI-token conversion, plus the failure paths that need no
// network. The token request itself is covered by the Rust FFI tests.

#include "zerobus/token.hpp"

#include <chrono>
#include <cstdio>
#include <string>

#include "detail/token_convert.hpp"
#include "zerobus/error.hpp"

namespace {

int g_failures = 0;

void fail(const char* msg) {
  std::fprintf(stderr, "FAIL: %s\n", msg);
  ++g_failures;
}

// Expects get_token to throw a non-retryable ZerobusException.
void expect_non_retryable(const std::string& table_name,
                          const std::string& client_secret, const char* what) {
  try {
    zerobus::get_token("https://workspace.example.com", table_name, "client-id",
                       client_secret, "1234567890");
    fail(what);
  } catch (const zerobus::ZerobusException& e) {
    if (e.is_retryable()) fail(what);
  }
}

}  // namespace

int main() {
  using std::chrono::milliseconds;
  using std::chrono::system_clock;

  {
    char access_token[] = "secret-token-value";
    zerobus::CZerobusToken raw{access_token, 1700000000123};
    zerobus::OAuthToken token = zerobus::detail::to_oauth_token(raw);
    if (token.access_token != "secret-token-value") {
      fail("access_token was not copied");
    }
    if (token.expires_at !=
        system_clock::time_point(milliseconds(1700000000123))) {
      fail("expires_at does not match expires_at_unix_ms");
    }
  }

  {
    char access_token[] = "secret-token-value";
    zerobus::CZerobusToken raw{access_token, 0};
    if (zerobus::detail::to_oauth_token(raw).expires_at.has_value()) {
      fail("expires_at_unix_ms == 0 should leave expires_at empty");
    }
  }

  expect_non_retryable("not-a-table", "client-secret",
                       "an invalid table name should be non-retryable");
  expect_non_retryable("main.default.events", std::string("sec\0ret", 7),
                       "an embedded NUL should be non-retryable");

  if (g_failures != 0) {
    std::fprintf(stderr, "%d failure(s)\n", g_failures);
    return 1;
  }
  std::printf("token_test: OK\n");
  return 0;
}
