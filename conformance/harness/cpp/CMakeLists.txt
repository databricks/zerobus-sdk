# Layer-1 conformance harness for the C++ Zerobus SDK.
#
# Builds the in-repo C++ SDK (cpp/, which builds the Rust C FFI from rust/ffi)
# from this checkout, then the TestHarness gRPC server on top of it.
#
#   cmake -S conformance/harness/cpp -B conformance/harness/cpp/build
#   cmake --build conformance/harness/cpp/build -j
#
# Requires gRPC C++ (gRPC::grpc++ and gRPC::grpc_cpp_plugin) and Protobuf with
# matching CMake config packages installed. Set -DCMAKE_PREFIX_PATH to their
# install prefix if they are not in a default location.

cmake_minimum_required(VERSION 3.16)

project(zerobus_testing_harness_cpp
    DESCRIPTION "Layer-1 conformance harness for the C++ Zerobus SDK"
    LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# The SDK under test: the repo's C++ wrapper. Added as a subdirectory, so its
# tests and examples stay off (they default on only for a top-level build).
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../../../cpp" zerobus_cpp)

# Protobuf via CMake's FindProtobuf (provides protobuf::libprotobuf and
# protobuf::protoc). protoc must match the libprotobuf version; override with
# -DProtobuf_PROTOC_EXECUTABLE=<path> if PATH finds a different protoc first.
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Threads REQUIRED)

# ---------------------------------------------------------------------------
# TestHarness stubs, generated from the shared contract at build time.
# ---------------------------------------------------------------------------
set(HARNESS_PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../proto")
set(HARNESS_PROTO "${HARNESS_PROTO_DIR}/test_harness.proto")
set(HARNESS_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/gen")
set(HARNESS_GEN_SRCS
    "${HARNESS_GEN_DIR}/test_harness.pb.cc"
    "${HARNESS_GEN_DIR}/test_harness.grpc.pb.cc")
set(HARNESS_GEN_HDRS
    "${HARNESS_GEN_DIR}/test_harness.pb.h"
    "${HARNESS_GEN_DIR}/test_harness.grpc.pb.h")

file(MAKE_DIRECTORY "${HARNESS_GEN_DIR}")
add_custom_command(
    OUTPUT ${HARNESS_GEN_SRCS} ${HARNESS_GEN_HDRS}
    COMMAND $<TARGET_FILE:protobuf::protoc>
            --cpp_out=${HARNESS_GEN_DIR}
            --grpc_out=${HARNESS_GEN_DIR}
            --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
            -I ${HARNESS_PROTO_DIR}
            ${HARNESS_PROTO}
    DEPENDS ${HARNESS_PROTO}
    COMMENT "Generating TestHarness C++ stubs"
    VERBATIM)

# ---------------------------------------------------------------------------
# Harness binary
# ---------------------------------------------------------------------------
add_executable(zerobus-testing-harness-cpp
    src/main.cpp
    src/harness.cpp
    src/records.cpp
    ${HARNESS_GEN_SRCS})

target_include_directories(zerobus-testing-harness-cpp PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/src"
    "${HARNESS_GEN_DIR}")

target_link_libraries(zerobus-testing-harness-cpp PRIVATE
    zerobus_cpp
    gRPC::grpc++
    protobuf::libprotobuf
    Threads::Threads)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(zerobus-testing-harness-cpp PRIVATE -Wall -Wextra)
endif()
