package com.databricks.zerobus.conformance;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

/**
 * Logical record -> SDK-native record conversion. The only place this harness understands record
 * bodies (mirrors {@code conformance/harness/rust/src/records.rs}).
 *
 * <p>Pipeline: {@code CreateStreamRequest.descriptor_proto_bytes} -> {@link #descriptorProto};
 * {@link #messageDescriptor}; {@code IngestRecordRequest.record_json} + descriptor -> {@link
 * #toMessage} -> {@link DynamicMessage}. The SDK then encodes the message itself on ingest.
 */
final class Records {
  private static final String DESCRIPTOR_FILE_NAME = "conformance_record.proto";

  private Records() {}

  /** Decodes a serialized {@code google.protobuf.DescriptorProto}. */
  static DescriptorProto descriptorProto(ByteString bytes) throws InvalidProtocolBufferException {
    return DescriptorProto.parseFrom(bytes);
  }

  /**
   * Resolves a standalone {@code DescriptorProto} into a message {@link Descriptor}. The message is
   * wrapped in a synthetic file with no package and no dependencies, which is all the SDK's
   * simple_record fixture needs.
   */
  static Descriptor messageDescriptor(DescriptorProto descriptorProto)
      throws DescriptorValidationException {
    FileDescriptorProto file =
        FileDescriptorProto.newBuilder()
            .setName(DESCRIPTOR_FILE_NAME)
            .addMessageType(descriptorProto)
            .build();
    FileDescriptor fd = FileDescriptor.buildFrom(file, new FileDescriptor[0]);
    Descriptor d = fd.findMessageTypeByName(descriptorProto.getName());
    if (d == null) {
      throw new IllegalArgumentException("message not found: " + descriptorProto.getName());
    }
    return d;
  }

  /**
   * Parses a canonical protobuf-JSON object into a message of {@code descriptor}. Unknown fields
   * are rejected (the parser default), so a fixture typo fails loudly instead of being dropped.
   */
  static Message toMessage(Descriptor descriptor, String recordJson)
      throws InvalidProtocolBufferException {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
    JsonFormat.parser().merge(recordJson, builder);
    return builder.build();
  }
}
