// Package pb holds the Go bindings for the TestHarness control contract, generated
// from conformance/harness/proto/test_harness.proto. The contract is frozen; these
// bindings are committed (no runtime codegen), mirroring purego/internal/zerobuspb.
//
// Regenerate from this directory with `go generate ./...`. Requires protoc v33.0 on
// PATH plus protoc-gen-go v1.36.11 and protoc-gen-go-grpc v1.6.2 (the versions
// pinned in purego/internal/zerobuspb/gen.go):
//
//	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
//	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
package pb

//go:generate protoc -I ../../../proto --go_out=. --go_opt=paths=source_relative --go_opt=Mtest_harness.proto=github.com/databricks/zerobus-sdk/conformance/harness/purego/internal/pb --go-grpc_out=. --go-grpc_opt=paths=source_relative --go-grpc_opt=Mtest_harness.proto=github.com/databricks/zerobus-sdk/conformance/harness/purego/internal/pb test_harness.proto
