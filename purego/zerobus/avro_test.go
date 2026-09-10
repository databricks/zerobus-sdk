//go:build avro

package zerobus

import (
	"math/big"
	"testing"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

func TestWithAvroSetsSchemaAndRecordType(t *testing.T) {
	c := defaultStreamConfig()
	WithAvro(`{"type":"record","name":"R","fields":[]}`)(&c)
	if c.recordType != zerobuspb.RecordType_AVRO {
		t.Fatalf("want AVRO record type, got %v", c.recordType)
	}
	if c.avroSchema == "" {
		t.Fatal("want avro schema to be set")
	}
	if c.descriptor != nil {
		t.Fatal("want nil descriptor for an avro stream")
	}
}

func TestValidateStreamArgsRejectsEmptyAvroSchema(t *testing.T) {
	c := defaultStreamConfig()
	WithAvro("")(&c)
	if err := validateStreamArgs("catalog.schema.table", c); err == nil {
		t.Fatal("want error for empty avro schema")
	}
}

func TestNewAvroObjectEncoderRejectsBadSchema(t *testing.T) {
	if newAvroObjectEncoder == nil {
		t.Fatal("avro build must set newAvroObjectEncoder")
	}
	if _, err := newAvroObjectEncoder("{ not avro"); err == nil {
		t.Fatal("want error for malformed schema")
	}
}

// TestAvroRecordEncodesAllTypes proves the object path covers every Avro type
// in the Avro-to-Delta mapping, including unions, fixed, decimal, and logicals.
func TestAvroRecordEncodesAllTypes(t *testing.T) {
	schema := `{"type":"record","name":"AllTypes","fields":[
		{"name":"b","type":"boolean"},
		{"name":"i","type":"int"},
		{"name":"l","type":"long"},
		{"name":"f","type":"float"},
		{"name":"d","type":"double"},
		{"name":"s","type":"string"},
		{"name":"by","type":"bytes"},
		{"name":"en","type":{"type":"enum","name":"Color","symbols":["RED","GREEN"]}},
		{"name":"arr","type":{"type":"array","items":"long"}},
		{"name":"mp","type":{"type":"map","values":"long"}},
		{"name":"rec","type":{"type":"record","name":"Nested","fields":[{"name":"x","type":"long"}]}},
		{"name":"un_null","type":["null","string"]},
		{"name":"un_int_long","type":["int","long"]},
		{"name":"un_multi","type":["string","long",{"type":"enum","name":"C","symbols":["A","B"]}]},
		{"name":"fx","type":{"type":"fixed","name":"F16","size":16}},
		{"name":"uuid_s","type":{"type":"string","logicalType":"uuid"}},
		{"name":"dt","type":{"type":"int","logicalType":"date"}},
		{"name":"ts_ms","type":{"type":"long","logicalType":"timestamp-millis"}},
		{"name":"ts_us","type":{"type":"long","logicalType":"timestamp-micros"}},
		{"name":"dec","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
		{"name":"dec_fx","type":{"type":"fixed","name":"DecFx","size":8,"logicalType":"decimal","precision":10,"scale":2}}
	]}`
	enc, err := newAvroObjectEncoder(schema)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	utc := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	dec := big.NewRat(12345, 100)
	rec := AvroRecord{
		"b": true, "i": int32(7), "l": int64(7), "f": float32(1.5), "d": float64(1.5),
		"s": "hi", "by": []byte{1, 2, 3}, "en": "GREEN",
		"arr": []any{int64(1), int64(2)}, "mp": map[string]any{"k": int64(9)},
		"rec":         map[string]any{"x": int64(4)},
		"un_null":     Union("string", "hi"),
		"un_int_long": Union("long", int64(5)),
		"un_multi":    Union("C", "A"),
		"fx":          [16]byte{1, 2, 3},
		"uuid_s":      "de305d54-75b4-431b-adb2-eb6b9e546013",
		"dt":          time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		"ts_ms":       utc, "ts_us": utc,
		"dec": dec, "dec_fx": dec,
	}
	b, err := enc.encode(map[string]any(rec))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(b) == 0 {
		t.Fatal("want non-empty datum")
	}
}
