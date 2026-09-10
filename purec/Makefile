# Makefile for the Zerobus Pure C SDK: convenience wrappers around CMake/CTest.

BUILD_TYPE ?= Release
JOBS ?= 4

SANITIZE ?=
SANITIZE_FLAG := $(if $(SANITIZE),-DZEROBUS_SANITIZE=$(SANITIZE),)

# Extra -D flags forwarded to cmake configure (lint sets -DZEROBUS_WERROR=ON).
CMAKE_EXTRA ?=

# Sanitizer builds use their own build dir so they don't clash with a normal
# build dir. Override BUILD_DIR to force a specific path.
BUILD_DIR ?= $(if $(SANITIZE),build-$(SANITIZE),build)

# Coverage uses its own build dir (matched by `clean`'s build-* glob).
COV_DIR ?= build-cov
COV_OBJ_DIR := $(COV_DIR)/CMakeFiles/zerobus_c_testing.dir/src

# gcov must match the compiler that built the objects or it misreads the
# coverage data (.gcno/.gcda). CMake builds with cc, so pick the gcov-N whose
# major version matches cc, and fall back to plain gcov. Override with GCOV=...
GCOV ?= $(shell \
	major=$$(cc -dumpversion 2>/dev/null | cut -d. -f1); \
	if command -v gcov-$$major >/dev/null 2>&1; \
		then echo gcov-$$major; \
		else echo gcov; \
	fi)

# All hand-written C sources (excludes the CMake build tree).
SOURCES := $(shell find include src examples tests -name '*.c' -o -name '*.h' 2>/dev/null)

.PHONY: help configure build test examples lint fmt fmt-check clean check verify coverage

help:
	@echo "Available targets:"
	@echo "  make build      - Configure and build the SDK, examples, and tests"
	@echo "  make test       - Build and run the offline test suite (ctest)"
	@echo "  make examples   - Build the example binaries"
	@echo "  make fmt        - Format all C sources with clang-format"
	@echo "  make fmt-check  - Check formatting without modifying files"
	@echo "  make lint       - Warnings-as-errors build (-Werror)"
	@echo "  make check      - fmt-check + lint (static gates, no tests)"
	@echo "  make verify     - Full workspace check: check + tests, plain and sanitized"
	@echo "  make coverage   - Coverage build+test, then gcov: read .gcov files for uncovered lines (#####)"
	@echo "  make clean      - Remove the build dirs (build, build-*, and BUILD_DIR)"
	@echo ""
	@echo "  Add SANITIZE=address (or thread/undefined) to build+test with a"
	@echo "  sanitizer, e.g. 'make test SANITIZE=address'."

configure:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) \
		-DCMAKE_EXPORT_COMPILE_COMMANDS=ON $(SANITIZE_FLAG) $(CMAKE_EXTRA)

build: configure
	cmake --build $(BUILD_DIR) -j $(JOBS)

test: build
	cd $(BUILD_DIR) && ctest --output-on-failure

examples: build
	@echo "Examples built under $(BUILD_DIR)/examples/"

fmt:
	clang-format -i $(SOURCES)

fmt-check:
	clang-format --dry-run --Werror $(SOURCES)

# No clang-tidy here, so -Wall -Wextra is the linter: run the build with
# -Werror. Its own tree keeps the flag out of the normal build's cache.
lint:
	$(MAKE) build BUILD_DIR=build-lint CMAKE_EXTRA=-DZEROBUS_WERROR=ON

check: fmt-check lint

# Full workspace check: static checks, then the suite plain and under all three
# sanitizers. address and thread can't coexist, so undefined rides with each.
verify:
	$(MAKE) clean
	$(MAKE) check
	$(MAKE) test
	$(MAKE) test SANITIZE=address,undefined
	$(MAKE) test SANITIZE=thread,undefined

# Coverage: build and test with gcov instrumentation, then run gcov.
# Read the generated .gcov files to see which lines are not covered.
coverage:
	rm -rf $(COV_DIR)
	cmake -S . -B $(COV_DIR) -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_FLAGS="--coverage -O0 -g" \
		-DCMAKE_EXE_LINKER_FLAGS="--coverage" $(CMAKE_EXTRA)
	cmake --build $(COV_DIR) -j $(JOBS)
	cd $(COV_DIR) && ctest --output-on-failure
	cd $(COV_OBJ_DIR) && $(GCOV) *.gcno

clean:
	rm -rf $(BUILD_DIR) build build-*
