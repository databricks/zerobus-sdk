package com.databricks.zerobus.examples.avro;

import com.databricks.zerobus.*;
import java.io.ByteArrayOutputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

/**
 * Avro single-record ingestion example (Beta).
 *
 * <p>Demonstrates both ingestion methods:
 *
 * <ul>
 *   <li>Record object: {@code GenericRecord} (SDK encodes against the writer schema)
 *   <li>Pre-encoded: {@code byte[]} (you provide an encoded Avro datum)
 * </ul>
 *
 * <p>Requires Java 11+ and the {@code org.apache.avro:avro} dependency.
 *
 * <p>Run with: {@code java -cp <classpath> com.databricks.zerobus.examples.avro.SingleRecordExample}
 */
public class SingleRecordExample {

  // The Avro writer schema, declared once at stream creation.
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

    System.out.println("=== Avro Single Record Example ===\n");

    Schema schema = new Schema.Parser().parse(SCHEMA_JSON);

    try (ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
        ZerobusAvroStream stream =
            sdk.streamBuilder()
                .table(tableName)
                .oauth(clientId, clientSecret)
                .avro(SCHEMA_JSON)
                .build()
                .join()) {

      // === Record objects: GenericRecord, encoded by the SDK ===
      System.out.println("Record object (GenericRecord):");
      for (int i = 0; i < 10; i++) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("device_name", "avro-main-loop-" + i);
        record.put("temp", 20 + i);
        record.put("humidity", (long) (50 + i));
        stream.ingestRecordOffset(record); // queue only — do NOT wait here
      }
      stream.flush(); // wait once for all pending acknowledgments
      System.out.println("  10 records queued and flushed");

      // === Pre-encoded: byte[] you encoded yourself ===
      System.out.println("\nPre-encoded (byte[]):");
      GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
      for (int i = 0; i < 10; i++) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("device_name", "avro-alt-loop-" + i);
        record.put("temp", 30 + i);
        record.put("humidity", (long) (60 + i));
        stream.ingestRecordOffset(encode(writer, record));
      }
      stream.flush();
      System.out.println("  10 records queued and flushed");

      System.out.println("\n=== Complete ===");
    }
  }

  /** Encodes a record to an Avro binary datum (for the pre-encoded ingestion path). */
  private static byte[] encode(GenericDatumWriter<GenericRecord> writer, GenericRecord record)
      throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(record, encoder);
    encoder.flush();
    return out.toByteArray();
  }
}
