//go:build avro

package zerobus

// AvroRecord is a record the stream encodes against the writer schema declared
// via WithAvro. Keys are field names; values follow the Avro-to-Go mapping:
//
//	null/bool/int/long/float/double  nil, bool, int32, int64, float32, float64
//	string, enum, uuid               string
//	bytes                            []byte
//	fixed(N)                         [N]byte
//	array, map, record               []any, map[string]any, map[string]any
//	union                            Union("branch", v) (nullable may use *T)
//	date, timestamp-millis/micros    time.Time
//	decimal                          *big.Rat
//
// Feature in development; requires the avro build tag.
type AvroRecord map[string]any

// Union wraps a value as a single-branch Avro union, for use as an AvroRecord
// field value. branch is the member's Avro type name (or full name for a named
// type).
func Union(branch string, v any) map[string]any {
	return map[string]any{branch: v}
}
