# All unit tests link the ZB_TESTING build. It is a superset of the shipped
# library (every public symbol, plus ZB_STATIC helpers exposed for testing) and
# is behaviorally identical to it, so a public-only test pays nothing to link
# it. Each tests/src/<name>_test.c tests the functions in src/<name>.

# Code the tests share, from tests/src/_support, which is also on their include
# path. Every test links it.
add_library(zerobus_test_support STATIC src/_support/fake_zerobus.c
                                        src/_support/http_server.c
                                        src/_support/loopback.c)
target_include_directories(zerobus_test_support PUBLIC src/_support)
target_link_libraries(zerobus_test_support PUBLIC zerobus_c_testing)
target_compile_options(zerobus_test_support PRIVATE ${ZEROBUS_WARN_FLAGS})
target_compile_features(zerobus_test_support PRIVATE c_std_11)

function(zerobus_add_test test_path)
  string(REPLACE "/" "_" test "${test_path}")
  add_executable(${test} src/${test_path}.c)
  target_link_libraries(${test} PRIVATE zerobus_c_testing zerobus_test_support)
  target_compile_options(${test} PRIVATE ${ZEROBUS_WARN_FLAGS})
  # Tests may include internal headers, which use C11 like the library. The
  # example, built as C99, checks that the public headers stay C99.
  target_compile_features(${test} PRIVATE c_std_11)
  # A directory of its own where the test can write files, also available
  # below as ${<test>_DIR}.
  set(dir ${CMAKE_CURRENT_BINARY_DIR}/output/${test})
  file(MAKE_DIRECTORY ${dir})
  target_compile_definitions(${test} PRIVATE ZB_TEST_DIR="${dir}")
  set(${test}_DIR ${dir} PARENT_SCOPE)
  add_test(NAME ${test} COMMAND ${test})
endfunction()

zerobus_add_test(error_test)
zerobus_add_test(headers_provider_test)
zerobus_add_test(sdk_test)
zerobus_add_test(stream_test)
zerobus_add_test(internal/landing_zone_test)
zerobus_add_test(internal/common/utils_test)
zerobus_add_test(internal/common/queue_test)
zerobus_add_test(internal/platform/concurrency_test)
zerobus_add_test(internal/platform/log_test)
zerobus_add_test(internal/platform/network_test)
zerobus_add_test(internal/proto/zerobus_service_test)
zerobus_add_test(internal/network/tls_test)
zerobus_add_test(internal/network/grpc_test)
zerobus_add_test(internal/network/grpc_utils_test)
zerobus_add_test(internal/network/channel_test)
zerobus_add_test(internal/network/connection_test)
zerobus_add_test(internal/network/transport_test)
zerobus_add_test(internal/network/http_client_test)
zerobus_add_test(internal/oauth/oauth_test)

target_include_directories(internal_proto_zerobus_service_test PRIVATE
  "${GENERATED_DIR}")

set_tests_properties(internal_platform_log_test PROPERTIES ENVIRONMENT
  "ZEROBUS_LOG_LEVEL=info;ZEROBUS_LOG_FILE=${internal_platform_log_test_DIR}/log_test.log")

# Suppresses TSan's lock-order report from inside libcrypto, for the tests
# that use OpenSSL; see tsan.supp. Replaces CI's TSAN_OPTIONS for them.
set_tests_properties(sdk_test stream_test
                     internal_network_tls_test internal_network_connection_test
                     internal_network_transport_test internal_network_http_client_test
                     PROPERTIES ENVIRONMENT
  "TSAN_OPTIONS=halt_on_error=1 suppressions=${CMAKE_CURRENT_SOURCE_DIR}/tsan.supp")

# A synchronization regression must fail instead of hanging the test suite.
set_tests_properties(sdk_test stream_test internal_platform_concurrency_test
                     internal_common_queue_test internal_landing_zone_test
                     internal_network_grpc_test internal_platform_network_test
                     internal_network_tls_test internal_network_channel_test
                     internal_network_connection_test internal_network_transport_test
                     internal_network_http_client_test internal_oauth_oauth_test
                     PROPERTIES TIMEOUT 30)
