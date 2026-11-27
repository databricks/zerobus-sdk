import com.databricks.zerobus.IngestRecordResponse;

/**
 * Loads a generated message from the regular JAR and round-trips it through the protobuf runtime
 * on the classpath. Run by CI with each supported protobuf-java runtime, outside Maven's test
 * classpath, so a bad class in the published JAR fails here.
 */
public class RegularJarCheck {
  public static void main(String[] args) throws Exception {
    IngestRecordResponse response =
        IngestRecordResponse.newBuilder().setDurabilityAckUpToOffset(7L).build();
    IngestRecordResponse parsed = IngestRecordResponse.parseFrom(response.toByteArray());
    if (parsed.getDurabilityAckUpToOffset() != 7L) {
      throw new AssertionError("round trip returned " + parsed.getDurabilityAckUpToOffset());
    }
    System.out.println(
        "ok: protobuf-java loaded from "
            + com.google.protobuf.Message.class.getProtectionDomain().getCodeSource().getLocation());
  }
}
