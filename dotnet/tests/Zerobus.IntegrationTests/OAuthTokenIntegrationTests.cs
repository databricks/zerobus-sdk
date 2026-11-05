using NUnit.Framework;

namespace Databricks.Zerobus.IntegrationTests;

[TestFixture]
[Parallelizable(ParallelScope.Children)]
public class OAuthTokenIntegrationTests : IntegrationTestBase
{
    private const string AccessToken = "secret-token-value";

    private static OAuthToken GetToken(string workspaceUrl, string tableName = TestTableName) =>
        ZerobusSdk.GetToken(workspaceUrl, tableName, "id", "secret", "ws");

    [Test]
    public async Task GetToken_ReturnsTokenAndExpiry()
    {
        await using var endpoint = await TokenEndpointFixture.StartAsync(
            200,
            $$"""{"access_token":"{{AccessToken}}","token_type":"Bearer","expires_in":3600}""");

        var before = DateTimeOffset.UtcNow;
        var token = GetToken(endpoint.WorkspaceUrl);
        var after = DateTimeOffset.UtcNow;

        Assert.That(token.AccessToken, Is.EqualTo(AccessToken));
        Assert.That(token.ExpiresAt, Is.Not.Null);
        Assert.That(
            token.ExpiresAt!.Value.ToUnixTimeMilliseconds(),
            Is.InRange(
                before.AddHours(1).ToUnixTimeMilliseconds(),
                after.AddHours(1).ToUnixTimeMilliseconds()));
    }

    [Test]
    public async Task GetToken_MissingExpiresIn_ReturnsNullExpiresAt()
    {
        await using var endpoint = await TokenEndpointFixture.StartAsync(
            200,
            $$"""{"access_token":"{{AccessToken}}"}""");

        var token = GetToken(endpoint.WorkspaceUrl);

        Assert.That(token.AccessToken, Is.EqualTo(AccessToken));
        Assert.That(token.ExpiresAt, Is.Null);
    }

    [Test]
    public async Task GetToken_RejectedRequest_ThrowsNonRetryable()
    {
        await using var endpoint = await TokenEndpointFixture.StartAsync(
            401,
            """{"error":"invalid_client"}""");

        var ex = Assert.Throws<ZerobusException>(() => GetToken(endpoint.WorkspaceUrl));

        Assert.That(ex!.IsRetryable, Is.False);
        Assert.That(ex.RawMessage, Does.Contain("401"));
    }

    [Test]
    public async Task GetToken_ServerError_ThrowsRetryable()
    {
        await using var endpoint = await TokenEndpointFixture.StartAsync(503, "unavailable");

        var ex = Assert.Throws<ZerobusException>(() => GetToken(endpoint.WorkspaceUrl));

        Assert.That(ex!.IsRetryable, Is.True);
        Assert.That(ex.RawMessage, Does.Contain("503"));
    }

    [Test]
    public void GetToken_InvalidTableName_ThrowsNonRetryable()
    {
        var ex = Assert.Throws<ZerobusException>(() => GetToken("http://127.0.0.1:1", "not-a-table"));

        Assert.That(ex!.IsRetryable, Is.False);
        Assert.That(ex.RawMessage, Does.Contain("table name"));
    }

    [Test]
    public void GetToken_NullArgument_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
        {
            ZerobusSdk.GetToken("http://127.0.0.1:1", TestTableName, "id", "secret", null!);
        });
    }

    [Test]
    public async Task ToString_RedactsAccessToken()
    {
        await using var endpoint = await TokenEndpointFixture.StartAsync(
            200,
            $$"""{"access_token":"{{AccessToken}}","expires_in":3600}""");

        var description = GetToken(endpoint.WorkspaceUrl).ToString();

        Assert.That(description, Does.Not.Contain(AccessToken));
        Assert.That(description, Does.Contain("<redacted>"));
    }
}
