#!/bin/bash
set -e

echo "Generating Go code from protobuf definitions..."

# Create output directory
mkdir -p pb

# Generate Go code
protoc --go_out=. --go_opt=paths=source_relative \
    air_quality.proto

echo "✓ Generated Go code in pb/ directory"
echo ""
echo "You can now import and use:"
echo "  import pb \"github.com/databricks/zerobus-sdk-go/examples/proto/pb\""
