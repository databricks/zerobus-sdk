module github.com/databricks/zerobus-sdk/conformance/harness/purego

go 1.25.0

require (
	github.com/databricks/zerobus-sdk/purego v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.82.0
	google.golang.org/protobuf v1.36.11
)

require (
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
)

// Build the SDK from this checkout, not a released tag.
replace github.com/databricks/zerobus-sdk/purego => ../../../purego
