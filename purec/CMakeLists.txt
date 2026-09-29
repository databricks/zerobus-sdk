# Zerobus Pure C SDK build: the library, examples, and tests.
cmake_minimum_required(VERSION 3.13)
project(zerobus_c LANGUAGES C VERSION 0.0.1)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
# Keep consumers on strict C99, library targets opt into C11 privately.
set(CMAKE_C_EXTENSIONS OFF)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Hidden visibility by default: only ZEROBUS_API symbols are exported from a
# shared build. ELF visibility only affects the dynamic symbol table, so the
# static archive still carries its external zb_* helpers as globals.
set(CMAKE_C_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)

# Optional sanitizer, e.g. -DZEROBUS_SANITIZE=address (or thread, undefined).
set(ZEROBUS_SANITIZE "" CACHE STRING "Runtime sanitizer: address|thread|undefined")

# Treat compiler warnings as errors (used by `make lint`; off for normal builds).
option(ZEROBUS_WERROR "Treat compiler warnings as errors" OFF)

# Warning flags shared by the library, tests, and example. -Werror is appended
# only under ZEROBUS_WERROR, so a normal build surfaces warnings without failing.
set(ZEROBUS_WARN_FLAGS -Wall -Wextra)
if(ZEROBUS_WERROR)
  list(APPEND ZEROBUS_WARN_FLAGS -Werror)
endif()

set(ZEROBUS_SOURCES
  src/internal/concurrency.c
  src/error.c
  src/internal/log.c
  src/utils.c
  src/sdk.c
  src/stream.c
)

add_library(zerobus_c STATIC ${ZEROBUS_SOURCES})
add_library(zerobus::c ALIAS zerobus_c)

target_include_directories(zerobus_c
  PUBLIC  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

target_compile_options(zerobus_c PRIVATE ${ZEROBUS_WARN_FLAGS})
target_compile_features(zerobus_c PRIVATE c_std_11)
target_link_libraries(zerobus_c PUBLIC Threads::Threads)

# Same sources compiled with ZB_TESTING, which gives ZB_STATIC helpers external
# linkage so unit tests can call them directly. Not shipped.
add_library(zerobus_c_testing STATIC ${ZEROBUS_SOURCES})
target_compile_definitions(zerobus_c_testing PUBLIC ZB_TESTING)
target_include_directories(zerobus_c_testing
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         ${CMAKE_CURRENT_SOURCE_DIR}/src
         ${CMAKE_CURRENT_SOURCE_DIR}/tests/src)
target_compile_options(zerobus_c_testing PRIVATE ${ZEROBUS_WARN_FLAGS})
target_compile_features(zerobus_c_testing PRIVATE c_std_11)
target_link_libraries(zerobus_c_testing PUBLIC Threads::Threads)

if(ZEROBUS_SANITIZE)
  foreach(lib zerobus_c zerobus_c_testing)
    target_compile_options(${lib} PUBLIC -fsanitize=${ZEROBUS_SANITIZE} -fno-omit-frame-pointer -g)
    target_link_options(${lib} PUBLIC -fsanitize=${ZEROBUS_SANITIZE})
  endforeach()
endif()

enable_testing()
add_subdirectory(examples)
add_subdirectory(tests)
