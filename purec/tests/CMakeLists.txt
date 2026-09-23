# All unit tests link the ZB_TESTING build. It is a superset of the shipped
# library (every public symbol, plus ZB_STATIC helpers exposed for testing) and
# is behaviorally identical to it, so a public-only test pays nothing to link
# it. Each tests/src/<name>_test.c tests the functions in src/<name>.
set(ZEROBUS_TESTS error_test utils_test sdk_test stream_test)

foreach(test ${ZEROBUS_TESTS})
  add_executable(${test} src/${test}.c)
  target_link_libraries(${test} PRIVATE zerobus_c_testing)
  target_compile_options(${test} PRIVATE ${ZEROBUS_WARN_FLAGS})
  add_test(NAME ${test} COMMAND ${test})
endforeach()

add_executable(log_test src/internal/log_test.c)
target_link_libraries(log_test PRIVATE zerobus_c_testing)
target_compile_options(log_test PRIVATE ${ZEROBUS_WARN_FLAGS})
add_test(NAME log_test COMMAND log_test)
set_tests_properties(log_test PROPERTIES ENVIRONMENT
  "ZEROBUS_LOG_LEVEL=info;ZEROBUS_LOG_FILE=${CMAKE_CURRENT_BINARY_DIR}/log_test.log")
