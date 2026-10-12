# Regenerate committed C bindings with `make generate-proto` after schema edits.
# Normal builds do not run this script or require either generator.
# For reproducible output, use the versions below from
# https://github.com/protocolbuffers/protobuf/releases and
# https://github.com/protobuf-c/protobuf-c/releases.
# CI reads these pins via PRINT_TOOL_VERSIONS and checks for generated-code drift.
# Override PROTOC_EXECUTABLE, PROTOC_GEN_C_EXECUTABLE, or GENERATED_DIR with -D.

# ---------------------------------------------------------------------------
# Pinned tool versions
# ---------------------------------------------------------------------------
set(EXPECTED_PROTOC_VERSION "33.0")
set(EXPECTED_PROTOBUF_C_VERSION "1.3.3")
if(PRINT_TOOL_VERSIONS)
  execute_process(COMMAND "${CMAKE_COMMAND}" -E echo
    "PROTOC_VERSION=${EXPECTED_PROTOC_VERSION}")
  execute_process(COMMAND "${CMAKE_COMMAND}" -E echo
    "PROTOBUF_C_VERSION=${EXPECTED_PROTOBUF_C_VERSION}")
  return()
endif()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
get_filename_component(PROTO_DIR "${CMAKE_CURRENT_LIST_DIR}/../../rust/sdk" ABSOLUTE)
set(PROTOBUF_SCHEMA_INCLUDE_DIR "${CMAKE_CURRENT_LIST_DIR}/third_party")
if(NOT GENERATED_DIR)
  set(GENERATED_DIR "${CMAKE_CURRENT_LIST_DIR}/generated")
endif()

# ---------------------------------------------------------------------------
# Tool discovery
# ---------------------------------------------------------------------------
if(NOT PROTOC_EXECUTABLE)
  find_program(PROTOC_EXECUTABLE NAMES protoc)
endif()
if(NOT PROTOC_GEN_C_EXECUTABLE)
  find_program(PROTOC_GEN_C_EXECUTABLE NAMES protoc-gen-c)
endif()
if(NOT PROTOC_EXECUTABLE OR NOT PROTOC_GEN_C_EXECUTABLE)
  message(FATAL_ERROR
    "Regeneration requires protoc and protoc-gen-c. Install them or "
    "set PROTOC_EXECUTABLE and PROTOC_GEN_C_EXECUTABLE.")
endif()

# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------
foreach(INPUT "${PROTO_DIR}/zerobus_service.proto"
              "${PROTOBUF_SCHEMA_INCLUDE_DIR}/google/protobuf/duration.proto")
  if(NOT EXISTS "${INPUT}")
    message(FATAL_ERROR "Regeneration requires the source schema: ${INPUT}")
  endif()
endforeach()

# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------
file(MAKE_DIRECTORY "${GENERATED_DIR}")
execute_process(
  COMMAND "${PROTOC_EXECUTABLE}"
    "--plugin=protoc-gen-c=${PROTOC_GEN_C_EXECUTABLE}"
    "-I${PROTO_DIR}" "-I${PROTOBUF_SCHEMA_INCLUDE_DIR}"
    "--c_out=${GENERATED_DIR}"
    "${PROTO_DIR}/zerobus_service.proto"
    "${PROTOBUF_SCHEMA_INCLUDE_DIR}/google/protobuf/duration.proto"
  RESULT_VARIABLE RESULT ERROR_VARIABLE ERROR_OUTPUT)
if(NOT "${RESULT}" STREQUAL "0")
  message(FATAL_ERROR "Failed to generate protobuf-c bindings: ${ERROR_OUTPUT}")
endif()
message(STATUS "Generated protobuf-c sources in ${GENERATED_DIR}")
