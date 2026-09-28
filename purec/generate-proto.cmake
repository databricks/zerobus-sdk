# Generate missing or stale C bindings into generated/ during configuration.
# Also supports standalone generation with cmake -P.
# Override PROTOC_EXECUTABLE, PROTOC_GEN_C_EXECUTABLE, or
# PROTOBUF_SCHEMA_INCLUDE_DIR with -D if tools are outside standard paths.

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
set(PUREC_DIR "${CMAKE_CURRENT_LIST_DIR}")
get_filename_component(PROTO_DIR "${PUREC_DIR}/../rust/sdk" ABSOLUTE)
set(GENERATED_DIR "${PUREC_DIR}/generated")

# ---------------------------------------------------------------------------
# Tool and schema discovery
# ---------------------------------------------------------------------------
if(NOT PROTOC_EXECUTABLE)
  find_program(PROTOC_EXECUTABLE NAMES protoc)
endif()
if(NOT PROTOC_GEN_C_EXECUTABLE)
  find_program(PROTOC_GEN_C_EXECUTABLE NAMES protoc-gen-c)
endif()
if(NOT PROTOBUF_SCHEMA_INCLUDE_DIR)
  get_filename_component(PROTOC_BIN_DIR "${PROTOC_EXECUTABLE}" DIRECTORY)
  get_filename_component(PROTOC_PREFIX "${PROTOC_BIN_DIR}/.." ABSOLUTE)
  find_path(PROTOBUF_SCHEMA_INCLUDE_DIR NAMES google/protobuf/duration.proto
    HINTS "${PROTOC_PREFIX}/include"
    PATHS /usr/local/include /usr/include)
endif()
if(NOT PROTOC_EXECUTABLE OR NOT PROTOC_GEN_C_EXECUTABLE OR
   NOT PROTOBUF_SCHEMA_INCLUDE_DIR)
  message(FATAL_ERROR
    "Generation requires protoc, protoc-gen-c, and google/protobuf/duration.proto. "
    "Found protoc=${PROTOC_EXECUTABLE}, protoc-gen-c=${PROTOC_GEN_C_EXECUTABLE}, "
    "schema include=${PROTOBUF_SCHEMA_INCLUDE_DIR}")
endif()

# ---------------------------------------------------------------------------
# Inputs, outputs, and configuration dependencies
# ---------------------------------------------------------------------------
set(PROTO_INPUTS
  "${PROTO_DIR}/zerobus_service.proto"
  "${PROTOBUF_SCHEMA_INCLUDE_DIR}/google/protobuf/duration.proto"
  "${CMAKE_CURRENT_LIST_FILE}"
  "${PROTOC_EXECUTABLE}"
  "${PROTOC_GEN_C_EXECUTABLE}")
set(PROTO_OUTPUTS
  "${GENERATED_DIR}/zerobus_service.pb-c.c"
  "${GENERATED_DIR}/zerobus_service.pb-c.h"
  "${GENERATED_DIR}/google/protobuf/duration.pb-c.c"
  "${GENERATED_DIR}/google/protobuf/duration.pb-c.h")
set(PROTO_STAMP "${GENERATED_DIR}/INPUTS_SHA256")

if(NOT CMAKE_SCRIPT_MODE_FILE)
  # Reconfigure on schema changes or deleted outputs, including direct builds.
  set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
    ${PROTO_INPUTS} ${PROTO_OUTPUTS} "${PROTO_STAMP}")
endif()

# ---------------------------------------------------------------------------
# Reuse checks
# ---------------------------------------------------------------------------
# Build variants share generated/, so only one process may generate at a time.
file(MAKE_DIRECTORY "${GENERATED_DIR}")
file(LOCK "${GENERATED_DIR}/.generation.lock" GUARD PROCESS TIMEOUT 30)

set(INPUT_HASHES "")
foreach(INPUT IN LISTS PROTO_INPUTS)
  file(SHA256 "${INPUT}" INPUT_HASH)
  string(APPEND INPUT_HASHES "${INPUT_HASH}\n")
endforeach()
string(SHA256 INPUTS_SHA256 "${INPUT_HASHES}")

set(NEEDS_REGEN FALSE)
foreach(OUTPUT IN LISTS PROTO_OUTPUTS)
  if(NOT EXISTS "${OUTPUT}")
    set(NEEDS_REGEN TRUE)
  endif()
endforeach()
set(PREVIOUS_SHA256 "")
if(EXISTS "${PROTO_STAMP}")
  file(READ "${PROTO_STAMP}" PREVIOUS_SHA256)
  string(STRIP "${PREVIOUS_SHA256}" PREVIOUS_SHA256)
endif()
if(NOT INPUTS_SHA256 STREQUAL PREVIOUS_SHA256)
  set(NEEDS_REGEN TRUE)
endif()
if(NOT NEEDS_REGEN)
  return()
endif()

# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------
# A failed generation must not leave a stamp marking partial output as current.
file(REMOVE "${PROTO_STAMP}")
execute_process(
  COMMAND "${PROTOC_EXECUTABLE}"
    "--plugin=protoc-gen-c=${PROTOC_GEN_C_EXECUTABLE}"
    "-I${PROTO_DIR}" "-I${PROTOBUF_SCHEMA_INCLUDE_DIR}"
    "--c_out=${GENERATED_DIR}"
    "${PROTO_DIR}/zerobus_service.proto"
    "${PROTOBUF_SCHEMA_INCLUDE_DIR}/google/protobuf/duration.proto"
  RESULT_VARIABLE RESULT
  ERROR_VARIABLE ERROR_OUTPUT)
if(NOT "${RESULT}" STREQUAL "0")
  message(FATAL_ERROR "Failed to generate protobuf-c bindings: ${ERROR_OUTPUT}")
endif()
file(WRITE "${PROTO_STAMP}" "${INPUTS_SHA256}\n")
message(STATUS "Generated protobuf-c sources in ${GENERATED_DIR}")
