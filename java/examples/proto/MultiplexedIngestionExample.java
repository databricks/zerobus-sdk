package com.databricks.zerobus.examples.proto;

import com.databricks.zerobus.*;
import com.databricks.zerobus.examples.proto.AirQualityProto.AirQuality;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Proto multiplexed ingestion example (Beta).
 *
 * <p>A multiplexed stream spreads records round-robin across several sub-streams. Use it when one
 * stream is the throughput bottleneck and global ordering is not required.
 *
 * <p>Run with: {@code java -cp <classpath> com.databricks.zerobus.examples.proto.MultiplexedIngestionExample}
 */
public class MultiplexedIngestionExample {

    private static final int STREAM_COUNT = 4;
    private static final int RECORD_COUNT = 1000;
    private static final int BATCH_SIZE = 10;

    public static void main(String[] args) throws Exception {
        String serverEndpoint = System.getenv("ZEROBUS_SERVER_ENDPOINT");
        String workspaceUrl = System.getenv("DATABRICKS_WORKSPACE_URL");
        String tableName = System.getenv("ZEROBUS_TABLE_NAME");
        String clientId = System.getenv("DATABRICKS_CLIENT_ID");
        String clientSecret = System.getenv("DATABRICKS_CLIENT_SECRET");

        if (serverEndpoint == null || workspaceUrl == null || tableName == null
                || clientId == null || clientSecret == null) {
            System.err.println("Error: Required environment variables not set.");
            System.err.println("Set: ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,");
            System.err.println("     DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET");
            System.exit(1);
        }

        System.out.println("=== Proto Multiplexed Ingestion Example ===\n");

        // Optional: track acknowledgments without blocking the ingest loop. On a multiplexed
        // stream the callback receives message IDs, once per record or batch, in no order, and
        // sub-streams may call it concurrently, so it must be thread-safe.
        AtomicLong acked = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        AckCallback callback = new AckCallback() {
            @Override
            public void onAck(long messageId) {
                acked.incrementAndGet();
            }

            @Override
            public void onError(long messageId, String errorMessage) {
                failed.incrementAndGet();
                System.err.println("Message " + messageId + " failed: " + errorMessage);
            }
        };

        try (ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl)) {
        ZerobusMultiplexedProtoStream stream = sdk.streamBuilder()
            .table(tableName)
            .oauth(clientId, clientSecret)
            .compiledProto(AirQuality.getDescriptor().toProto())
            .multiplexed(STREAM_COUNT)
            .ackCallback(callback)
            .build()
            .join();

        ZerobusException failure = null;
        try {
            // === Ingest in a loop, then flush once ===
            // Each call only queues the record and returns a message ID; it does not wait.
            for (int i = 0; i < RECORD_COUNT; i++) {
                stream.ingestRecord(AirQuality.newBuilder()
                    .setDeviceName("proto-mux-" + i)
                    .setTemp(20 + i % 10)
                    .setHumidity(50 + i % 10)
                    .build());
            }
            stream.flush();
            System.out.println(RECORD_COUNT + " records ingested across " + STREAM_COUNT
                + " sub-streams and acknowledged");

            // === Batches: each batch lands on one sub-stream ===
            for (int b = 0; b < 10; b++) {
                List<AirQuality> batch = new ArrayList<>();
                for (int i = 0; i < BATCH_SIZE; i++) {
                    batch.add(AirQuality.newBuilder()
                        .setDeviceName("proto-mux-batch-" + b + "-" + i)
                        .setTemp(30 + i)
                        .setHumidity(60 + i)
                        .build());
                }
                stream.ingestRecords(batch);
            }
            stream.flush();
            System.out.println("10 batches (" + (10 * BATCH_SIZE) + " records) ingested and acknowledged");

        } catch (ZerobusException e) {
            failure = e;
            throw e;
        } finally {
            // Always close: it releases the native stream. If ingesting already failed, keep that
            // error and attach the close failure to it instead of replacing it.
            try {
                stream.close();
            } catch (ZerobusException closeFailure) {
                if (failure == null) {
                    throw closeFailure;
                }
                failure.addSuppressed(closeFailure);
            }
        }

        // Records that were never acknowledged stay available after close, so they can be
        // ingested into a new stream. After a successful flush and close, this is empty.
        List<AirQuality> unacked = stream.getUnackedRecords(AirQuality.parser());
        System.out.println("\nUnacked records after close: " + unacked.size());

        // There is one callback per record plus one per batch. When close() returns, all callbacks
        // have run, unless close() waited longer than the callback wait time (up to 5 seconds;
        // sub-streams wait in parallel; not configurable in Java), so the count can be lower;
        // flush() and getUnackedRecords() are what confirm that every record was acknowledged.
        System.out.println("Callback: " + acked.get() + " acknowledged, " + failed.get() + " failed");
        }
    }
}
