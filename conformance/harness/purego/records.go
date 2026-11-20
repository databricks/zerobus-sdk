// Logical record -> SDK-native record conversion: the only place this harness
// understands record bodies. Proto streams encode record_json to protobuf bytes here
// (purego's proto stream takes raw bytes); JSON streams pass record_json through as-is.
package main

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// descriptorFromBytes decodes a serialized google.protobuf.DescriptorProto (the
// schema artifact) and resolves it into a message descriptor. The file is proto3,
// matching conformance/schemas/*/schema.proto, so fields have implicit presence.
func descriptorFromBytes(raw []byte) (protoreflect.MessageDescriptor, error) {
	var dp descriptorpb.DescriptorProto
	if err := proto.Unmarshal(raw, &dp); err != nil {
		return nil, fmt.Errorf("descriptor_proto_bytes is not a DescriptorProto: %w", err)
	}
	fd := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("conformance_record.proto"),
		Syntax:      proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{&dp},
	}
	file, err := protodesc.NewFile(fd, nil)
	if err != nil {
		return nil, fmt.Errorf("descriptor does not resolve: %w", err)
	}
	return file.Messages().Get(0), nil
}

// encodeProtoRecord parses record_json (a protobuf-JSON object) into a dynamic
// message of desc and returns its serialized protobuf bytes. Unknown fields are
// rejected (protojson default), so a fixture typo fails loudly instead of being
// silently dropped.
func encodeProtoRecord(desc protoreflect.MessageDescriptor, recordJSON string) ([]byte, error) {
	msg := dynamicpb.NewMessage(desc)
	if err := (protojson.UnmarshalOptions{}).Unmarshal([]byte(recordJSON), msg); err != nil {
		return nil, fmt.Errorf("record_json does not match the schema: %w", err)
	}
	out, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("encode record: %w", err)
	}
	return out, nil
}

// jsonRecordPayload returns record_json's bytes unchanged, for a JSON stream. The SDK
// validates the payload itself.
func jsonRecordPayload(recordJSON string) []byte {
	return []byte(recordJSON)
}
