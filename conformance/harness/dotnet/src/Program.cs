// Layer-1 conformance harness for the .NET Zerobus SDK.
//
// Handshake: binds 127.0.0.1:0, prints exactly `HARNESS_ENDPOINT=127.0.0.1:<port>` to stdout
// and flushes, then serves the `TestHarness` gRPC service over unencrypted HTTP/2 (h2c).
// Logs go to stderr only. Exits on the `Shutdown` RPC or SIGINT/SIGTERM.

using System.Net;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Zerobus.Conformance.Harness;

var builder = WebApplication.CreateBuilder(args);

// stdout is reserved for the endpoint line, so all logging goes to stderr.
builder.Logging.ClearProviders();
builder.Logging.AddConsole(options => options.LogToStandardErrorThreshold = LogLevel.Trace);

builder.WebHost.ConfigureKestrel(options =>
    options.Listen(IPAddress.Loopback, 0, listen => listen.Protocols = HttpProtocols.Http2));

builder.Services.AddGrpc();
builder.Services.AddSingleton<HarnessService>();

var app = builder.Build();
app.MapGrpcService<HarnessService>();

await app.StartAsync();

var bound = app.Services.GetRequiredService<IServer>()
    .Features.Get<IServerAddressesFeature>()!.Addresses.Single();
var port = new Uri(bound).Port;

Console.Out.Write($"HARNESS_ENDPOINT=127.0.0.1:{port}\n");
Console.Out.Flush();
app.Logger.LogInformation("TestHarness listening on {Address}", bound);

// Returns on the Shutdown RPC (StopApplication) or SIGINT/SIGTERM (ConsoleLifetime).
await app.WaitForShutdownAsync();

await app.Services.GetRequiredService<HarnessService>().CloseAllAsync();
app.Logger.LogInformation("harness exited");
