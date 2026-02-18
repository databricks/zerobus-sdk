# Contributing to Zerobus SDKs

We happily welcome contributions to the Zerobus SDKs. We use [GitHub Issues](https://github.com/databricks/zerobus-sdk/issues) to track community reported issues and [GitHub Pull Requests](https://github.com/databricks/zerobus-sdk/pulls) for accepting changes.

Contributions are licensed on a license-in/license-out basis.

For language-specific development setup, coding style, and CI details, see the contributing guide in each SDK directory (e.g., [`rust/CONTRIBUTING.md`](rust/CONTRIBUTING.md)).

## Communication

Before starting work on a major feature, please open a GitHub issue. We will make sure no one else is already working on it and that it is aligned with the goals of the project.

A "major feature" is defined as any change that is > 100 LOC altered (not including tests), or changes any user-facing behavior.

We will use the GitHub issue to discuss the feature and come to agreement. This is to prevent your time being wasted, as well as ours. The GitHub review process for major features is also important so that organizations with commit access can come to agreement on design.

If it is appropriate to write a design document, the document must be hosted either in the GitHub tracking issue, or linked to from the issue and hosted in a world-readable location.

Small patches and bug fixes don't need prior communication.

## Pull Request Process

1. **Create a feature branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes:**
   - Write clear, concise commit messages
   - Follow existing code style
   - Update documentation as needed

3. **Run formatting and tests** for the SDK you are modifying (see the SDK's own contributing guide for details).

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

## Commit Message Guidelines

Follow these conventions for commit messages:

- Use present tense: "Add feature" not "Added feature"
- Use imperative mood: "Fix bug" not "Fixes bug"
- First line should be 50 characters or less
- Reference issues: "Fix #123: Description of fix"

Example:
```
Add batch ingestion support

- Add batch method with all-or-nothing semantics
- Update README with batch API documentation

Fixes #42
```

## Versioning

We follow [Semantic Versioning](https://semver.org/):

- **MAJOR**: Incompatible API changes
- **MINOR**: Backwards-compatible functionality additions
- **PATCH**: Backwards-compatible bug fixes

## Getting Help

- **Issues**: Open an issue on GitHub for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions

## Code of Conduct

- Be respectful and inclusive
- Welcome newcomers
- Focus on constructive feedback
