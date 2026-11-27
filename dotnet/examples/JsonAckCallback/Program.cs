using Databricks.Zerobus;

// Get configuration from environment.
var serverEndpoint = Environment.GetEnvironmentVariable("ZEROBUS_SERVER_ENDPOINT")
    ?? throw new InvalidOperationException("ZEROBUS_SERVER_ENDPOINT not set");
var workspaceUrl = Environment.GetEnvironmentVariable("DATABRICKS_WORKSPACE_URL")
    ?? throw new InvalidOperationException("DATABRICKS_WORKSPACE_URL not set");
var clientId = Environment.GetEnvironmentVariable("DATABRICKS_CLIENT_ID")
    ?? throw new InvalidOperationException("DATABRICKS_CLIENT_ID not set");
var clientSecret = Environment.GetEnvironmentVariable("DATABRICKS_CLIENT_SECRET")
    ?? throw new InvalidOperationException("DATABRICKS_CLIENT_SECRET not set");
var tableName = Environment.GetEnvironmentVariable("ZEROBUS_TABLE_NAME")
    ?? throw new InvalidOperationException("ZEROBUS_TABLE_NAME not set");

using var sdk = ZerobusSdk.CreateBuilder()
    .Endpoint(serverEndpoint)
    .UnityCatalogUrl(workspaceUrl)
    .Build();

// The ack callback (Beta) reports acknowledgments in the background.
var tracker = new AckTracker();
var options = StreamConfigurationOptions.Default with { AckCallback = tracker };

var stream = sdk.CreateJsonStream(tableName, clientId, clientSecret, options);

// Ingest in a loop: each call only queues the record.
Console.WriteLine("Ingesting records...");
int rejected = 0;
for (int i = 0; i < 100; i++)
{
    // Change this JSON to match the schema of your table.
    var jsonRecord = """
        {
            "device_name": "sensor-001",
            "temp": 20,
            "humidity": 60
        }
        """;

    try
    {
        stream.IngestRecord(jsonRecord);
    }
    catch (ZerobusException ex)
    {
        rejected++;
        Console.WriteLine($"Failed to ingest record {i}: {ex.RawMessage}");
    }
}

// Flush once, then dispose: Dispose waits for pending callbacks, so read the
// tracker after it. Flush can return before the last OnAck has run.
try
{
    stream.Flush();
}
finally
{
    stream.Dispose();
}

Console.WriteLine(
    $"Acknowledged {tracker.Acked} records, {tracker.Failed} failed, {rejected} rejected at ingest.");
if (rejected > 0 || tracker.Failed > 0)
{
    throw new InvalidOperationException(
        $"{tracker.Failed} records failed and {rejected} were rejected at ingest.");
}

// Counts acknowledged records. The SDK calls it from a background thread, so it
// only touches Interlocked state and returns quickly.
internal sealed class AckTracker : IAckCallback
{
    private long _acked;
    private long _failed;

    public long Acked => Interlocked.Read(ref _acked);
    public long Failed => Interlocked.Read(ref _failed);

    public void OnAck(long offset) => Interlocked.Exchange(ref _acked, offset + 1);

    public void OnError(long offset, string errorMessage)
    {
        Interlocked.Increment(ref _failed);
        Console.WriteLine($"Record at offset {offset} failed: {errorMessage}");
    }
}
