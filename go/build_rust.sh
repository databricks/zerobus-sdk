#!/bin/bash
set -e

# This script is automatically invoked by `go generate` or `go build`
# It builds the Rust FFI library and places it where CGO expects it

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FFI_DIR="$SCRIPT_DIR/zerobus-ffi"
OUTPUT_DIR="$SCRIPT_DIR"

# Detect platform
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

# Normalize OS for directory name
case "$OS" in
    darwin*)  GOOS="darwin" ;;
    linux*)   GOOS="linux" ;;
    msys*|mingw*|cygwin*) GOOS="windows" ;;
    *)        GOOS="$OS" ;;
esac

# Normalize ARCH for directory name
case "$ARCH" in
    x86_64)   GOARCH="amd64" ;;
    aarch64|arm64)  GOARCH="arm64" ;;
    *)        GOARCH="$ARCH" ;;
esac

TARGET_LIB_DIR="$OUTPUT_DIR/lib/${GOOS}_${GOARCH}"
TARGET_LIB_PATH="$TARGET_LIB_DIR/libzerobus_ffi.a"

# Skip rebuild if library already exists and is newer than source
if [ -f "$TARGET_LIB_PATH" ]; then
    # Check if any Rust source file is newer than the library
    NEEDS_REBUILD=0
    while IFS= read -r -d '' file; do
        if [ "$file" -nt "$TARGET_LIB_PATH" ]; then
            NEEDS_REBUILD=1
            break
        fi
    done < <(find "$FFI_DIR/src" -name "*.rs" -print0 2>/dev/null)

    if [ $NEEDS_REBUILD -eq 0 ]; then
        echo "✓ Rust library up to date, skipping rebuild"
        exit 0
    fi
fi

echo "Building Rust FFI library for ${GOOS}_${GOARCH}..."

cd "$FFI_DIR"

# Determine Rust target for Windows MinGW compatibility
if [[ "$GOOS" == "windows" ]]; then
    echo "Detected Windows environment - building for GNU target..."
    TARGET="x86_64-pc-windows-gnu"
    cargo build --release --target "$TARGET"
elif command -v cargo-zigbuild &> /dev/null; then
    echo "Using cargo-zigbuild for optimized build..."
    cargo zigbuild --release
else
    echo "Using cargo (install cargo-zigbuild for better cross-compilation)"
    cargo build --release
fi

mkdir -p "$TARGET_LIB_DIR"

if [ -f "target/release/libzerobus_ffi.a" ]; then
    cp "target/release/libzerobus_ffi.a" "$TARGET_LIB_PATH"
    echo "✓ Rust library built successfully: $TARGET_LIB_PATH"
elif [ -f "target/x86_64-pc-windows-gnu/release/libzerobus_ffi.a" ]; then
    # Windows GNU target
    cp "target/x86_64-pc-windows-gnu/release/libzerobus_ffi.a" "$TARGET_LIB_PATH"
    echo "✓ Rust library built successfully: $TARGET_LIB_PATH (Windows GNU)"
elif [ -f "target/release/zerobus_ffi.lib" ]; then
    # Windows MSVC: copy .lib as .a for CGO compatibility
    cp "target/release/zerobus_ffi.lib" "$TARGET_LIB_PATH"
    echo "✓ Rust library built successfully: $TARGET_LIB_PATH (from zerobus_ffi.lib)"
else
    echo "✗ Error: Could not find Rust library"
    echo "   Tried: target/release/libzerobus_ffi.a"
    echo "          target/x86_64-pc-windows-gnu/release/libzerobus_ffi.a"
    echo "          target/release/zerobus_ffi.lib"
    exit 1
fi
