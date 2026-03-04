# Contributing to Zerobus SDK for Python

Please read the [top-level CONTRIBUTING.md](../CONTRIBUTING.md) first for general contribution guidelines, pull request process, and commit requirements.

This document covers Python-specific development setup and workflow.

## Development Setup

### Prerequisites

- **Python 3.9 or higher**
- **Git**
- **Rust toolchain** (required for building the native extension)
  - Install from [rustup.rs](https://rustup.rs/)
  - Verify: `rustc --version`
- **maturin** (for building Rust-Python bindings)
  - Install: `pip install maturin`

### Setting Up Your Development Environment

Since v0.3.0, the SDK uses a Rust core with Python bindings built via [maturin](https://github.com/PyO3/maturin) and [PyO3](https://pyo3.rs/).

1. **Clone the repository:**
   ```bash
   git clone https://github.com/databricks/zerobus-sdk.git
   cd zerobus-sdk/python
   ```

2. **Create and activate a virtual environment:**
   ```bash
   make dev
   ```

   This will:
   - Create a virtual environment in `.venv`
   - Install the package in development mode with all dev dependencies
   - **Note**: This uses `pip install -e .` which builds the Rust extension automatically

3. **Activate the virtual environment:**
   ```bash
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

### Building the Rust Extension

The SDK's core is implemented in Rust. There are two ways to build it:

**Option 1: Development Mode (Recommended for active development)**
```bash
make develop-rust
```
This compiles the Rust code in debug mode and installs it directly into your virtual environment. **After modifying Rust code, re-run this command to rebuild.**

**Option 2: Release Mode (For benchmarking or distribution)**
```bash
make build-rust
```
This creates an optimized wheel in `dist/`. Use this for performance testing or creating release builds.

## Coding Style

Code style is enforced by a formatter check in your pull request. We use [Black](https://github.com/psf/black) to format our code. Run `make fmt` to ensure your code is properly formatted prior to raising a pull request.

### Running the Formatter

Format your code before committing:

```bash
make fmt
```

This runs:
- **Black**: Code formatting
- **autoflake**: Remove unused imports
- **isort**: Sort imports

### Running Linters

Check your code for issues:

```bash
make lint
```

This runs:
- **pycodestyle**: Style guide enforcement
- **autoflake**: Check for unused imports

### Running Tests

Run the test suite to ensure your changes don't break existing functionality:

```bash
make test
```

This runs:
- **pytest**: Unit tests with coverage reports
- Generates HTML and XML coverage reports in `htmlcov/` and `coverage.xml`

Open the coverage report in your browser:
```bash
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

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
   git commit -m "Add feature: description of your changes"
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

All commits must be signed off to certify compliance with the [Developer Certificate of Origin](../DCO). Use `git commit -s` to sign automatically.

## Code Review Guidelines

When reviewing code:

- Check for adherence to code style
- Look for potential edge cases
- Consider performance implications
- Ensure documentation is updated

## Commit Message Guidelines

Follow these conventions for commit messages:

- Use present tense: "Add feature" not "Added feature"
- Use imperative mood: "Fix bug" not "Fixes bug"
- First line should be 50 characters or less
- Reference issues: "Fix #123: Description of fix"

Example:
```
Add async stream creation example

- Add async_example.py demonstrating non-blocking ingestion
- Update README with async API documentation

Fixes #42
```

## Documentation

### Updating Documentation

- Update docstrings for all public APIs
- Use Google-style docstrings
- Include examples in docstrings where helpful
- Update README.md for user-facing changes
- Update examples/ for new features

Example docstring:
```python
def ingest_record(self, record) -> RecordAcknowledgment:
    """
    Submits a single record for ingestion into the stream.

    This method may block if the maximum number of in-flight records
    has been reached.

    Args:
        record: The Protobuf message object to be ingested.

    Returns:
        RecordAcknowledgment: An object to wait on for the server's acknowledgment.

    Raises:
        ZerobusException: If the stream is not in a valid state for ingestion.

    Example:
        >>> record = AirQuality(device_name="sensor-1", temp=25)
        >>> ack = stream.ingest_record(record)
        >>> ack.wait_for_ack()
    """
```

## Continuous Integration

All pull requests must pass CI checks:

- **fmt**: Runs formatting checks (black, autoflake, isort)
- **lint**: Runs linting checks (pycodestyle, autoflake)
- **tests-ubuntu**: Runs tests on Ubuntu with Python 3.9, 3.10, 3.11, 3.12
- **tests-windows**: Runs tests on Windows with Python 3.9, 3.10, 3.11, 3.12

The formatting check runs `make dev fmt` and then checks for any git differences. If there are differences, the check will fail.

You can view CI results in the GitHub Actions tab of the pull request.

## Makefile Targets

### Python Targets

- `make dev` - Set up development environment (creates venv, installs deps, builds Rust extension)
- `make install` - Install package in editable mode
- `make build` - Build wheel package using maturin (use `PYTHON=python3.X` to specify version)
- `make install-wheel` - Install the built wheel
- `make fmt` - Format code with black, autoflake, and isort
- `make lint` - Run linting with pycodestyle and autoflake
- `make test` - Run unit tests with pytest and generate coverage reports
- `make clean` - Remove Python build artifacts (dist, egg-info, pytest cache)
- `make clean-all` - Remove all build artifacts including Rust and `.venv`
- `make help` - Show all available targets

### Rust Targets (v0.3.0+)

- `make build-rust` - Build Rust extension in release mode (optimized, slower compile)
- `make develop-rust` - Build Rust extension in debug mode and install to venv (faster compile)
- `make clean-rust` - Remove Rust build artifacts

### Common Workflows

**Daily development (Python changes only):**
```bash
make fmt        # Format your code
make lint       # Check for issues
make test       # Run tests
```

**After modifying Rust code:**
```bash
make develop-rust   # Rebuild and install Rust extension
make test          # Test Python bindings
```

**Creating a release build:**
```bash
make build    # Build optimized wheel
# Wheel will be in dist/
```

## Versioning

We follow [Semantic Versioning](https://semver.org/):

- **MAJOR**: Incompatible API changes
- **MINOR**: Backwards-compatible functionality additions
- **PATCH**: Backwards-compatible bug fixes

## Getting Help

- **Issues**: Open an issue on GitHub for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions
- **Documentation**: Check the README and examples/

## Package Name

The package is published on PyPI as `databricks-zerobus-ingest-sdk`.

## Code of Conduct

- Be respectful and inclusive
- Welcome newcomers
- Focus on constructive feedback
- Follow the [Python Community Code of Conduct](https://www.python.org/psf/conduct/)
