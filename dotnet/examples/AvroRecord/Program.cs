#if ZEROBUS_AVRO

using Databricks.Zerobus;

// Get configuration from environment.
var zerobusEndpoint = Environment.GetEnvironmentVariable("ZEROBUS_SERVER_ENDPOINT")
    ?? throw new InvalidOperationException("ZEROBUS_SERVER_ENDPOINT not set");
var unityCatalogUrl = Environment.GetEnvironmentVariable("DATABRICKS_WORKSPACE_URL")
    ?? throw new InvalidOperationException("DATABRICKS_WORKSPACE_URL not set");
var clientId = Environment.GetEnvironmentVariable("DATABRICKS_CLIENT_ID")
    ?? throw new InvalidOperationException("DATABRICKS_CLIENT_ID not set");
var clientSecret = Environment.GetEnvironmentVariable("DATABRICKS_CLIENT_SECRET")
    ?? throw new InvalidOperationException("DATABRICKS_CLIENT_SECRET not set");
var tableName = Environment.GetEnvironmentVariable("ZEROBUS_TABLE_NAME")
    ?? throw new InvalidOperationException("ZEROBUS_TABLE_NAME not set");
var avroSchemaJson = Environment.GetEnvironmentVariable("ZEROBUS_AVRO_SCHEMA")
    ?? throw new InvalidOperationException("ZEROBUS_AVRO_SCHEMA not set");

// Create SDK instance.
using var sdk = ZerobusSdk.CreateBuilder()
    .Endpoint(zerobusEndpoint)
    .UnityCatalogUrl(unityCatalogUrl)
    .Build();

// Configure stream options (optional).
var options = StreamConfigurationOptions.Default with
{
    MaxInflightRequests = 50_000,
};

// Create Avro stream.
using var stream = sdk.CreateAvroStream(
    tableName,
    avroSchemaJson,
    clientId,
    clientSecret,
    options);

Console.WriteLine("Ingesting Avro records...");
int failed = 0;

// Define record as a plain object (will be serialized to JSON and Avro-encoded).
var records = new object[]
{
    new { device_name = "sensor-001", temp = 20, humidity = 60 },
    new { device_name = "sensor-002", temp = 25, humidity = 55 },
    new { device_name = "sensor-003", temp = 18, humidity = 65 },
};

for (int i = 0; i < records.Length; i++)
{
    try
    {
        long offset = stream.IngestRecord(records[i]);
        Console.WriteLine($"Queued record {i} at offset {offset}");
    }
    catch (ZerobusException ex) when (ex.IsRetryable)
    {
        failed++;
        Console.WriteLine($"Failed to ingest record {i} (retryable): {ex.RawMessage}");
    }
    catch (ZerobusException ex)
    {
        failed++;
        Console.WriteLine($"Failed to ingest record {i}: {ex.RawMessage}");
    }
}

if (failed == records.Length)
{
    throw new InvalidOperationException("No records were queued");
}

Console.WriteLine("Waiting for acknowledgments...");
stream.Flush();

if (failed > 0)
{
    throw new InvalidOperationException(
        $"{records.Length - failed} records flushed; {failed} ingest calls failed.");
}
else
{
    Console.WriteLine("All records successfully ingested and acknowledged!");
}

#else
Console.WriteLine("This example requires the ZEROBUS_AVRO feature to be enabled.");
#endif
