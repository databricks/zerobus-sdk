// Layer-1 conformance harness for the TypeScript Zerobus SDK.
//
// Handshake: binds 127.0.0.1:0, prints exactly `HARNESS_ENDPOINT=127.0.0.1:<port>` to
// stdout, then serves the `TestHarness` gRPC service. Logs go to stderr only.
// Exits on the `Shutdown` RPC or SIGINT/SIGTERM.

import * as fs from "fs";
import * as path from "path";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { buildHarnessImpl } from "./harness";

// dist/main.js -> conformance/harness/proto/test_harness.proto
const PROTO_PATH = path.resolve(__dirname, "../../proto/test_harness.proto");

async function main(): Promise<void> {
  const packageDef = protoLoader.loadSync(PROTO_PATH, {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
    bytes: Buffer,
  });
  const loaded = grpc.loadPackageDefinition(packageDef) as any;
  const service = loaded.zerobus.conformance.harness.TestHarness.service;

  const server = new grpc.Server();
  let requestShutdown: () => void = () => {};
  const shutdownRequested = new Promise<void>((resolve) => {
    requestShutdown = resolve;
  });
  server.addService(service, buildHarnessImpl(() => requestShutdown()));

  const port = await new Promise<number>((resolve, reject) => {
    server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, boundPort) => {
      if (err) return reject(err);
      resolve(boundPort);
    });
  });

  // stdout is reserved for the endpoint line. writeSync flushes before we serve.
  fs.writeSync(1, `HARNESS_ENDPOINT=127.0.0.1:${port}\n`);
  console.error(`TestHarness listening on 127.0.0.1:${port}`);

  const onSignal = (sig: string) => {
    console.error(`received ${sig}, shutting down`);
    requestShutdown();
  };
  process.on("SIGINT", () => onSignal("SIGINT"));
  process.on("SIGTERM", () => onSignal("SIGTERM"));

  await shutdownRequested;
  await new Promise<void>((resolve) => {
    server.tryShutdown((err) => {
      if (err) console.error(`server shutdown error: ${err}`);
      resolve();
    });
  });
  console.error("harness exited");
  // Open SDK streams are not closed by Shutdown (matches the Rust harness), so exit explicitly.
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
