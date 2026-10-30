package com.databricks.zerobus.examples.avro;

import com.databricks.zerobus.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Avro batch ingestion example (Beta).
 *
 * <p>Ingests records in batches with {@code ingestRecordsOffset}. Each batch is all-or-nothing and
 * returns a single offset. Requires Java 11+ and the {@code org.apache.avro:avro} dependency.
 *
 * <p>Run with: {@code java -cp <classpath> com.databricks.zerobus.examples.avro.BatchIngestionExample}
 */
public class BatchIngestionExample {

  private static final String SCHEMA_JSON =
      "{\"type\":\"record\",\"name\":\"AirQuality\",\"fields\":["
          + "{\"name\":\"device_name\",\"type\":\"string\"},"
          + "{\"name\":\"temp\",\"type\":\"int\"},"
          + "{\"name\":\"humidity\",\"type\":\"long\"}]}";

  public static void main(String[] args) throws Exception {
    String serverEndpoint = System.getenv("ZEROBUS_SERVER_ENDPOINT");
    String workspaceUrl = System.getenv("DATABRICKS_WORKSPACE_URL");
    String tableName = System.getenv("ZEROBUS_TABLE_NAME");
    String clientId = System.getenv("DATABRICKS_CLIENT_ID");
    String clientSecret = System.getenv("DATABRICKS_CLIENT_SECRET");

    if (serverEndpoint == null
        || workspaceUrl == null
        || tableName == null
        || clientId == null
        || clientSecret == null) {
      System.err.println("Error: Required environment variables not set.");
      System.err.println(
          "Set: ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,");
      System.err.println("     DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET");
      System.exit(1);
    }

    System.out.println("=== Avro Batch Ingestion Example ===\n");

    Schema schema = new Schema.Parser().parse(SCHEMA_JSON);

    try (ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
        ZerobusAvroStream stream =
            sdk.streamBuilder()
                .table(tableName)
                .oauth(clientId, clientSecret)
                .avro(SCHEMA_JSON)
                .build()
                .join()) {

      for (int batch = 0; batch < 5; batch++) {
        List<GenericRecord> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
          GenericRecord record = new GenericData.Record(schema);
          record.put("device_name", "avro-batch-" + batch + "-" + i);
          record.put("temp", 20 + i);
          record.put("humidity", (long) (50 + i));
          records.add(record);
        }
        stream.ingestRecordsOffset(records); // queue the batch — do NOT wait here
      }
      stream.flush(); // wait once for all batches
      System.out.println("5 batches of 100 records queued and flushed");

      System.out.println("\n=== Complete ===");
    }
  }
}
