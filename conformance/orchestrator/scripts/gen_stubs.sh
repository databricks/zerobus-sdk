#!/usr/bin/env bash
# Regenerates the committed contract stubs under src/zerobus_test_runner/_generated/.
# Run by hand when a contract proto changes; the runtime never runs protoc.
set -euo pipefail
ORCH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONF="$(cd "$ORCH/.." && pwd)"
OUT="$ORCH/src/zerobus_test_runner/_generated"
PY="${PYTHON:-python3}"

gen() {  # gen <proto_dir> <proto_file>
  "$PY" -m grpc_tools.protoc -I "$1" --python_out="$OUT" --grpc_python_out="$OUT" "$2"
  local base="${2%.proto}"
  # protoc emits a top-level import of the sibling module; make it package-relative.
  sed -i -E "s/^import ${base}_pb2 as /from . import ${base}_pb2 as /" "$OUT/${base}_pb2_grpc.py"
}

gen "$CONF/harness/proto" test_harness.proto
gen "$CONF/mock_server/proto" mock_admin.proto
: > "$OUT/__init__.py"
echo "generated stubs in $OUT"
