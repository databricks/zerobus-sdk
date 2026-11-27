module github.com/databricks/zerobus-sdk/purego/examples

go 1.25.0

require (
	github.com/databricks/zerobus-sdk/purego v0.0.0
	google.golang.org/protobuf v1.36.11
)

require (
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/grpc v1.82.0 // indirect
)

// Use the local pure-Go SDK module.
replace github.com/databricks/zerobus-sdk/purego => ..
