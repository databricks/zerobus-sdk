# Zerobus C FFI

C Foreign Function Interface bindings for the Zerobus Rust SDK.

## Building

```bash
# Build both static and dynamic libraries
cargo build -p zerobus-ffi --release

# Output:
# target/release/libzerobus_ffi.a    (static library)
# target/release/libzerobus_ffi.so   (Linux dynamic library)
# target/release/libzerobus_ffi.dylib (macOS dynamic library)
# target/release/zerobus_ffi.dll     (Windows dynamic library)
```

## Cross-compilation

```bash
# Linux ARM64
rustup target add aarch64-unknown-linux-gnu
cargo build -p zerobus-ffi --release --target aarch64-unknown-linux-gnu

# macOS ARM64 (Apple Silicon)
rustup target add aarch64-apple-darwin
cargo build -p zerobus-ffi --release --target aarch64-apple-darwin

# Windows
rustup target add x86_64-pc-windows-gnu
cargo build -p zerobus-ffi --release --target x86_64-pc-windows-gnu
```

## Usage

### Go (CGO with static library)

```go
/*
#cgo LDFLAGS: -L${SRCDIR}/lib -lzerobus_ffi -ldl -lpthread -lm
#include "zerobus.h"
*/
import "C"
```

### C# (P/Invoke with dynamic library)

```csharp
[DllImport("zerobus_ffi", CallingConvention = CallingConvention.Cdecl)]
private static extern IntPtr zerobus_sdk_new(string endpoint, string ucUrl, ref CResult result);
```

### C++

```cpp
#include "zerobus.h"

// Link with -lzerobus_ffi
```

## API Reference

See `zerobus.h` for the complete C API documentation.
