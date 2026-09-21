# All unit tests link the ZB_TESTING build. It is a superset of the shipped
# library (every public symbol, plus ZB_STATIC helpers exposed for testing) and
# is behaviorally identical to it, so a public-only test pays nothing to link
# it. Each tests/src/<name>_test.c tests the functions in src/<name>.
function(zerobus_add_test test_path)
  string(REPLACE "/" "_" test "${test_path}")
  add_executable(${test} src/${test_path}.c)
  target_link_libraries(${test} PRIVATE zerobus_c_testing)
  target_compile_options(${test} PRIVATE ${ZEROBUS_WARN_FLAGS})
  add_test(NAME ${test} COMMAND ${test})
endfunction()

zerobus_add_test(error_test)
zerobus_add_test(utils_test)
zerobus_add_test(sdk_test)
zerobus_add_test(stream_test)
zerobus_add_test(internal/concurrency_test)
zerobus_add_test(internal/log_test)

set_tests_properties(internal_log_test PROPERTIES ENVIRONMENT
  "ZEROBUS_LOG_LEVEL=info;ZEROBUS_LOG_FILE=${CMAKE_CURRENT_BINARY_DIR}/log_test.log")

# A synchronization regression must fail instead of hanging the test suite.
set_tests_properties(internal_concurrency_test PROPERTIES TIMEOUT 30)
