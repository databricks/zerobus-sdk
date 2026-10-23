package com.databricks.zerobus.conformance;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Layer-1 conformance harness for the Java Zerobus SDK.
 *
 * <p>Handshake: binds {@code 127.0.0.1:0}, prints exactly {@code HARNESS_ENDPOINT=127.0.0.1:<port>}
 * to stdout, then serves the {@code TestHarness} gRPC service. Logs go to stderr only. Exits on the
 * {@code Shutdown} RPC or on SIGINT/SIGTERM (via the JVM shutdown hook).
 *
 * <p>Build and launch (see the report for the full sequence): the Java SDK must be installed into
 * ~/.m2 from this checkout with the JNI library built using the {@code testing} feature, then:
 *
 * <pre>
 *   cd conformance/harness/java &amp;&amp; mvn package
 *   java -jar target/zerobus-testing-harness-java.jar
 * </pre>
 */
public final class Main {

  private Main() {}

  public static void main(String[] args) throws Exception {
    CountDownLatch shutdownRequested = new CountDownLatch(1);
    CountDownLatch cleanupDone = new CountDownLatch(1);
    HarnessService service = new HarnessService(shutdownRequested);

    // SIGINT / SIGTERM: request shutdown and wait for the main thread to finish cleanup.
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  shutdownRequested.countDown();
                  try {
                    cleanupDone.await(30, TimeUnit.SECONDS);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                },
                "harness-signal"));

    Server server =
        NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
            .addService(service)
            .build()
            .start();

    // stdout is reserved for the endpoint line. Everything else goes through the logger (stderr).
    PrintStream stdout = System.out;
    stdout.println("HARNESS_ENDPOINT=127.0.0.1:" + server.getPort());
    stdout.flush();
    System.err.println("TestHarness listening on 127.0.0.1:" + server.getPort());

    try {
      shutdownRequested.await();
      server.shutdown();
      if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
        server.shutdownNow();
      }
    } finally {
      service.closeAll();
      System.err.println("harness exited");
      cleanupDone.countDown();
    }
  }
}
