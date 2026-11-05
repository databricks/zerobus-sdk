namespace Databricks.Zerobus;

/// <summary>
/// A Zerobus-scoped OAuth 2.0 access token and its expiry, returned by
/// <see cref="ZerobusSdk.GetToken"/>.
/// </summary>
/// <remarks>
/// <see cref="ToString"/> redacts the token so it can't leak into logs.
/// </remarks>
public sealed class OAuthToken
{
    internal OAuthToken(string accessToken, DateTimeOffset? expiresAt)
    {
        AccessToken = accessToken;
        ExpiresAt = expiresAt;
    }

    /// <summary>
    /// The OAuth 2.0 access token, sent as <c>Bearer &lt;AccessToken&gt;</c>.
    /// </summary>
    public string AccessToken { get; }

    /// <summary>
    /// When the token expires, or <c>null</c> if the server reported no lifetime.
    /// Measured from when the request was sent, so it never overstates the token's validity.
    /// </summary>
    public DateTimeOffset? ExpiresAt { get; }

    /// <summary>
    /// Returns a description of the token with <see cref="AccessToken"/> redacted.
    /// </summary>
    public override string ToString() =>
        $"OAuthToken {{ AccessToken = <redacted>, ExpiresAt = {ExpiresAt?.ToString("O")} }}";
}
