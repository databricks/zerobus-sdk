# Contributing to Zerobus SDK for Go

We happily welcome contributions to the Zerobus SDK for Go. We use [GitHub Issues](https://github.com/databricks/zerobus-sdk-go/issues) to track community reported issues and [GitHub Pull Requests](https://github.com/databricks/zerobus-sdk-go/pulls) for accepting changes.

Contributions are licensed on a license-in/license-out basis.

## Communication

Before starting work on a major feature, please open a GitHub issue. We will make sure no one else is already working on it and that it is aligned with the goals of the project.

A "major feature" is defined as any change that is > 100 LOC altered (not including tests), or changes any user-facing behavior.

We will use the GitHub issue to discuss the feature and come to agreement. This is to prevent your time being wasted, as well as ours. The GitHub review process for major features is also important so that organizations with commit access can come to agreement on design.

If it is appropriate to write a design document, the document must be hosted either in the GitHub tracking issue, or linked to from the issue and hosted in a world-readable location.

Small patches and bug fixes don't need prior communication.

## Development Setup

### Prerequisites

- Git
- Go 1.19 or higher
- Rust toolchain (cargo)
- CGO enabled (default on most systems)

### Setting Up Your Development Environment

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/databricks/zerobus-sdk-go.git
    cd zerobus-sdk-go
    ```

2. **Build the project:**
   ```bash
   make build
   ```

   This will:
   - Build the Rust FFI layer
   - Compile the Go SDK
   - Create a self-contained static library

## Coding Style

Code style is enforced by formatters in your pull request. We use `gofmt` and `rustfmt` to format our code.

### Running the Formatter

Format your code before committing:

```bash
make fmt
```

This runs:
- `go fmt ./...` for Go code
- `cargo fmt --all` for Rust FFI code

### Running Linters

Check your code for issues:

```bash
make lint
```

This runs:
- `go vet ./...` to catch common Go mistakes
- `cargo clippy` to catch common Rust issues

### Running Tests

Run the test suite to ensure your changes don't break existing functionality:

```bash
make test
```

This runs:
- `go test` for all Go packages
- `cargo test` for the Rust FFI layer

## Pull Request Process

1. **Create a feature branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes:**
   - Write clear, concise commit messages
   - Follow existing code style
   - Update documentation as needed

3. **Format and test your code:**
   ```bash
   make fmt
   make test
   ```

4. **Commit your changes:**
   ```bash
   git add .
   git commit -s -m "Add feature: description of your changes"
   ```

5. **Push to your fork:**
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Create a Pull Request:**
   - Provide a clear description of changes
   - Reference any related issues
   - Ensure all CI checks pass

## Signed Commits

This repo requires all contributors to sign their commits. To configure this, you can follow [Github's documentation](https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits) to create a GPG key, upload it to your Github account, and configure your git client to sign commits.

## Developer Certificate of Origin

To contribute to this repository, you must sign off your commits to certify that you have the right to contribute the code and that it complies with the open source license. The rules are pretty simple, if you can certify the content of [DCO](./DCO), then simply add a "Signed-off-by" line to your commit message to certify your compliance. Please use your real name as pseudonymous/anonymous contributions are not accepted.

```
Signed-off-by: Joe Smith <joe.smith@email.com>
```

If you set your `user.name` and `user.email` git configs, you can sign your commit automatically with `git commit -s`:

```bash
git commit -s -m "Your commit message"
```

## Code Review Guidelines

When reviewing code:

- Check for adherence to code style
- Look for potential edge cases
- Consider performance implications
- Ensure documentation is updated
- Verify CGO bindings are correct
- Check for proper error handling

## Commit Message Guidelines

Follow these conventions for commit messages:

- Use present tense: "Add feature" not "Added feature"
- Use imperative mood: "Fix bug" not "Fixes bug"
- First line should be 50 characters or less
- Reference issues: "Fix #123: Description of fix"

Example:
```
Add async stream creation example

- Add async example demonstrating concurrent ingestion
- Update README with goroutine usage patterns

Fixes #42

Signed-off-by: Jane Doe <jane.doe@example.com>
```

## Documentation

### Updating Documentation

- Add godoc comments for all exported types and functions
- Use complete sentences in documentation
- Include examples in godoc where helpful
- Update README.md for user-facing changes
- Update examples/ for new features

Example godoc comment:
```go
// IngestRecord submits a single record for ingestion into the stream.
//
// This method may block if the maximum number of in-flight records
// has been reached, based on StreamConfigurationOptions.MaxInflightRequests.
//
// Returns the logical offset ID assigned to the record by the server.
//
// Example:
//
//	offset, err := stream.IngestRecord(`{"id": 1, "value": "hello"}`)
//	if err != nil {
//		log.Fatal(err)
//	}
//	fmt.Printf("Record ingested at offset: %d\n", offset)
func (s *ZerobusStream) IngestRecord(payload interface{}) (int64, error) {
	// ...
}
```

## Continuous Integration

All pull requests must pass CI checks:

- **fmt**: Runs formatting checks (`go fmt`, `cargo fmt`)
- **lint**: Runs linting checks (`go vet`, `cargo clippy`)
- **tests**: Runs unit tests for both Go and Rust components
- **build**: Verifies the SDK builds successfully on Linux and macOS

You can view CI results in the GitHub Actions tab of the pull request.

## Makefile Targets

Available make targets:

- `make build` - Build both Rust FFI and Go SDK
- `make build-rust` - Build only the Rust FFI layer
- `make build-go` - Build only the Go SDK
- `make clean` - Remove build artifacts
- `make fmt` - Format all code (Go and Rust)
- `make lint` - Run linters on all code
- `make check` - Run all checks (fmt and lint)
- `make test` - Run all tests
- `make examples` - Build all examples
- `make help` - Show available targets

## Working with CGO

When making changes to the FFI layer:

1. Update the Rust code in `zerobus-ffi/src/`
2. Update the C header if needed in `zerobus-ffi/zerobus.h`
3. Update the Go bindings in `ffi.go`
4. Rebuild with `make build`
5. Test the changes with `make test`

### CGO Best Practices

- Always use `C.CString()` for string conversion and free with `C.free()`
- Use `defer C.free()` immediately after allocating C memory
- Handle panics gracefully in exported functions
- Document memory ownership clearly
- Test for memory leaks

## Versioning

We follow [Semantic Versioning](https://semver.org/):

- **MAJOR**: Incompatible API changes
- **MINOR**: Backwards-compatible functionality additions
- **PATCH**: Backwards-compatible bug fixes

## Getting Help

- **Issues**: Open an issue on GitHub for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions
- **Documentation**: Check the README and examples/

## Code of Conduct

- Be respectful and inclusive
- Welcome newcomers
- Focus on constructive feedback
- Follow the [Go Community Code of Conduct](https://go.dev/conduct)
