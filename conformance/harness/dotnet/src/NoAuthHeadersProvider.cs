using Databricks.Zerobus;

namespace Zerobus.Conformance.Harness;

/// <summary>
/// Sends no authentication headers. Equivalent to the Rust SDK's testing-only
/// <c>NoAuthHeadersProvider</c> (empty header map); used because the mock accepts any caller.
/// </summary>
internal sealed class NoAuthHeadersProvider : IHeadersProvider
{
    public static readonly NoAuthHeadersProvider Instance = new();

    public IDictionary<string, string> GetHeaders() => new Dictionary<string, string>();
}
