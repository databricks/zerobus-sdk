# Zerobus Pure C SDK build: the library, examples, and tests.

cmake_minimum_required(VERSION 3.13)
project(zerobus_c LANGUAGES C VERSION 0.0.1)

# ---------------------------------------------------------------------------
# Build settings
# ---------------------------------------------------------------------------
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
# Keep consumers on strict C99, library targets opt into C11 privately.
set(CMAKE_C_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Hidden visibility by default: only ZEROBUS_API symbols are exported from a
# shared build. ELF visibility only affects the dynamic symbol table, so the
# static archive still carries its external zb_* helpers as globals.
set(CMAKE_C_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)

# Treat compiler warnings as errors (used by `make lint`; off for normal builds).
option(ZEROBUS_WERROR "Treat compiler warnings as errors" OFF)

# Warning flags shared by the library, tests, and example. -Werror is appended
# only under ZEROBUS_WERROR, so a normal build surfaces warnings without failing.
set(ZEROBUS_WARN_FLAGS -Wall -Wextra)
if(ZEROBUS_WERROR)
  list(APPEND ZEROBUS_WARN_FLAGS -Werror)
endif()

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

find_path(PROTOBUF_C_INCLUDE_DIR NAMES protobuf-c/protobuf-c.h)
find_library(PROTOBUF_C_LIBRARY NAMES protobuf-c libprotobuf-c)
if(NOT PROTOBUF_C_INCLUDE_DIR OR NOT PROTOBUF_C_LIBRARY)
  message(FATAL_ERROR "libprotobuf-c headers and library are required")
endif()
add_library(zerobus_protobuf_c UNKNOWN IMPORTED)
set_target_properties(zerobus_protobuf_c PROPERTIES
  IMPORTED_LOCATION "${PROTOBUF_C_LIBRARY}"
  INTERFACE_INCLUDE_DIRECTORIES "${PROTOBUF_C_INCLUDE_DIR}")

# ---------------------------------------------------------------------------
# Protobuf generation
# ---------------------------------------------------------------------------
# Generate missing or stale protobuf-c bindings during configuration.
include("${CMAKE_CURRENT_SOURCE_DIR}/generate-proto.cmake")

# protoc-gen-c 1.3.3 emits a union initializer that GCC warns about.
# Keep this exception on generated code; handwritten sources retain -Werror.
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
  set_source_files_properties(
    generated/zerobus_service.pb-c.c
    PROPERTIES COMPILE_OPTIONS -Wno-missing-braces)
endif()

# ---------------------------------------------------------------------------
# Library targets
# ---------------------------------------------------------------------------
set(ZEROBUS_SOURCES
  src/internal/common/utils.c
  src/internal/platform/concurrency.c
  src/internal/platform/log.c
  src/internal/proto/zerobus_service.c
  src/error.c
  src/sdk.c
  src/stream.c

  generated/google/protobuf/duration.pb-c.c
  generated/zerobus_service.pb-c.c
)

add_library(zerobus_c STATIC ${ZEROBUS_SOURCES})
add_library(zerobus::c ALIAS zerobus_c)

target_include_directories(zerobus_c
  PUBLIC  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
          ${CMAKE_CURRENT_SOURCE_DIR}/generated)

target_compile_options(zerobus_c PRIVATE ${ZEROBUS_WARN_FLAGS})
target_compile_features(zerobus_c PRIVATE c_std_11)
target_link_libraries(zerobus_c PUBLIC Threads::Threads zerobus_protobuf_c)

# Same sources compiled with ZB_TESTING, which gives ZB_STATIC helpers external
# linkage so unit tests can call them directly. Not shipped.
add_library(zerobus_c_testing STATIC ${ZEROBUS_SOURCES})
target_compile_definitions(zerobus_c_testing PUBLIC ZB_TESTING)
target_include_directories(zerobus_c_testing
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         ${CMAKE_CURRENT_SOURCE_DIR}/src
         ${CMAKE_CURRENT_SOURCE_DIR}/tests/src
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/generated)
target_compile_options(zerobus_c_testing PRIVATE ${ZEROBUS_WARN_FLAGS})
target_compile_features(zerobus_c_testing PRIVATE c_std_11)
target_link_libraries(zerobus_c_testing PUBLIC Threads::Threads zerobus_protobuf_c)

# ---------------------------------------------------------------------------
# Sanitizers
# ---------------------------------------------------------------------------
# Optional sanitizer, e.g. -DZEROBUS_SANITIZE=address (or thread, undefined).
set(ZEROBUS_SANITIZE "" CACHE STRING "Runtime sanitizer: address|thread|undefined")

if(ZEROBUS_SANITIZE)
  foreach(lib zerobus_c zerobus_c_testing)
    target_compile_options(${lib} PUBLIC -fsanitize=${ZEROBUS_SANITIZE} -fno-omit-frame-pointer -g)
    target_link_options(${lib} PUBLIC -fsanitize=${ZEROBUS_SANITIZE})
  endforeach()
endif()

# ---------------------------------------------------------------------------
# Tests and examples
# ---------------------------------------------------------------------------
enable_testing()
add_subdirectory(examples)
add_subdirectory(tests)
