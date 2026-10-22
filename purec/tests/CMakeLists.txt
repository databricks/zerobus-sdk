# All unit tests link the ZB_TESTING build. It is a superset of the shipped
# library (every public symbol, plus ZB_STATIC helpers exposed for testing) and
# is behaviorally identical to it, so a public-only test pays nothing to link
# it. Each tests/src/<name>_test.c tests the functions in src/<name>.

function(zerobus_add_test test_path)
  string(REPLACE "/" "_" test "${test_path}")
  add_executable(${test} src/${test_path}.c)
  target_link_libraries(${test} PRIVATE zerobus_c_testing)
  target_compile_options(${test} PRIVATE ${ZEROBUS_WARN_FLAGS})
  # Internal headers may use C11, like the library they belong to.
  if(test_path MATCHES "^internal/")
    target_compile_features(${test} PRIVATE c_std_11)
  endif()
  # A directory of its own where the test can write files, also available
  # below as ${<test>_DIR}.
  set(dir ${CMAKE_CURRENT_BINARY_DIR}/output/${test})
  file(MAKE_DIRECTORY ${dir})
  target_compile_definitions(${test} PRIVATE ZB_TEST_DIR="${dir}")
  set(${test}_DIR ${dir} PARENT_SCOPE)
  add_test(NAME ${test} COMMAND ${test})
endfunction()

zerobus_add_test(error_test)
zerobus_add_test(sdk_test)
zerobus_add_test(stream_test)
zerobus_add_test(internal/ingest_request_test)
zerobus_add_test(internal/landing_zone_test)
zerobus_add_test(internal/common/utils_test)
zerobus_add_test(internal/common/queue_test)
zerobus_add_test(internal/platform/concurrency_test)
zerobus_add_test(internal/platform/log_test)
zerobus_add_test(internal/proto/zerobus_service_test)
zerobus_add_test(internal/network/grpc_test)
zerobus_add_test(internal/network/grpc_utils_test)
zerobus_add_test(internal/platform/network_test)
zerobus_add_test(internal/network/tls_test)

# The stream tests run against an in-process fake service.
target_sources(stream_test PRIVATE src/fake_zerobus.c)
target_compile_features(stream_test PRIVATE c_std_11)
target_link_libraries(stream_test PRIVATE PkgConfig::NGHTTP2)

target_include_directories(internal_proto_zerobus_service_test PRIVATE
  "${GENERATED_DIR}")

set_tests_properties(internal_platform_log_test PROPERTIES ENVIRONMENT
  "ZEROBUS_LOG_LEVEL=info;ZEROBUS_LOG_FILE=${internal_platform_log_test_DIR}/log_test.log")

# Suppresses TSan's lock-order report from inside libcrypto in the tests that
# create a TLS context; see tsan.supp. Replaces CI's TSAN_OPTIONS for them.
set_tests_properties(sdk_test internal_network_tls_test PROPERTIES ENVIRONMENT
  "TSAN_OPTIONS=halt_on_error=1 suppressions=${CMAKE_CURRENT_SOURCE_DIR}/tsan.supp")

# A synchronization regression must fail instead of hanging the test suite.
set_tests_properties(sdk_test stream_test internal_platform_concurrency_test
                     internal_common_queue_test internal_landing_zone_test
                     internal_network_grpc_test internal_platform_network_test
                     internal_network_tls_test PROPERTIES TIMEOUT 30)
