cmake_minimum_required(VERSION 3.16)
project(zbperf_bench VERSION 0.1.0 LANGUAGES C)

find_package(zbperf CONFIG REQUIRED)
find_package(Threads REQUIRED)
if(NOT DEFINED zbperf_NGHTTP2_VERSION)
    set(zbperf_NGHTTP2_VERSION "unknown")
endif()
if(NOT DEFINED zbperf_OPENSSL_VERSION)
    set(zbperf_OPENSSL_VERSION "unknown")
endif()

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
string(TOUPPER "${CMAKE_BUILD_TYPE}" BENCH_BUILD_TYPE_UPPER)
set(BENCH_BUILD_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${BENCH_BUILD_TYPE_UPPER}}")
string(REPLACE "\\" "\\\\" BENCH_BUILD_FLAGS "${BENCH_BUILD_FLAGS}")
string(REPLACE "\"" "\\\"" BENCH_BUILD_FLAGS "${BENCH_BUILD_FLAGS}")
configure_file(build_info.h.in build_info.h @ONLY)

add_executable(zbperf-bench benchmark.c)
target_compile_options(zbperf-bench PRIVATE -Wall -Wextra -Wpedantic -Werror)
target_include_directories(zbperf-bench PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(zbperf-bench PRIVATE zbperf::zbperf Threads::Threads m)
install(TARGETS zbperf-bench RUNTIME DESTINATION bin)

include(CTest)
if(BUILD_TESTING)
    find_package(Python3 COMPONENTS Interpreter REQUIRED)
    add_test(NAME benchmark_cli COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_cli.py" "$<TARGET_FILE:zbperf-bench>")
    add_test(NAME benchmark_matrix COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_matrix.py")
    add_test(NAME benchmark_wrapper COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_run.py")
endif()
