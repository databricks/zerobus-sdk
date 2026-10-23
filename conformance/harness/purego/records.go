// Logical record -> SDK-native record conversion. The only place this harness
// understands record bodies.
//
// Proto streams: CreateStreamRequest.descriptor_proto_bytes -> descriptorFromBytes
// -> protoreflect.MessageDescriptor; IngestRecordRequest.record_json + descriptor ->
// encodeProtoRecord -> serialized protobuf bytes. purego's proto stream takes those
// bytes (IngestRecordOffset), so the encoding happens here with google.golang.org/
// protobuf, not in purego's own encoder.
//
// JSON streams: record_json is validated as a JSON object and passed through as the
// UTF-8 JSON payload (IngestRecordOffset on a WithJSON() stream). No descriptor.
package main

import (
	"encoding/json"
	"errors"
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
	if err := requireJSONObject(recordJSON); err != nil {
		return nil, err
	}
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

// jsonRecordPayload validates record_json as a JSON object and returns its bytes
// unchanged, for a JSON stream (the SDK sends them as the record payload).
func jsonRecordPayload(recordJSON string) ([]byte, error) {
	if err := requireJSONObject(recordJSON); err != nil {
		return nil, err
	}
	return []byte(recordJSON), nil
}

// requireJSONObject rejects anything that is not a single JSON object. json.Unmarshal
// (unlike a Decoder) also rejects trailing data, and a "null" leaves the map nil.
func requireJSONObject(recordJSON string) error {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(recordJSON), &obj); err != nil {
		return fmt.Errorf("record_json must be a JSON object: %w", err)
	}
	if obj == nil {
		return errors.New("record_json must be a JSON object")
	}
	return nil
}
