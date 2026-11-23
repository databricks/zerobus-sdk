#!/usr/bin/env bash
# Regenerate the committed contract stubs under src/zerobus_test_runner/_generated/.
# Run by hand after a contract proto changes; the runtime never runs protoc on them.
#
# grpcio-tools is pinned and installed into a throwaway venv, so both stubs are generated
# by the same toolchain and regeneration is reproducible on any machine. CI runs this and
# fails if the committed output differs (see the drift gate in push.yml).
set -euo pipefail
PIN="grpcio-tools==1.85.1"
ORCH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONF="$(cd "$ORCH/.." && pwd)"
OUT="$ORCH/src/zerobus_test_runner/_generated"

# The committed stubs embed the generator's version in their import guards, so the runtime floors
# in pyproject.toml must equal this PIN. Fail loudly if they drift apart.
PIN_VER="${PIN##*==}"
FLOOR_VER="$(grep -oE 'grpcio-tools>=[0-9][0-9.]*' "$ORCH/pyproject.toml" | grep -oE '[0-9][0-9.]*')"
if [ "$PIN_VER" != "$FLOOR_VER" ]; then
  echo "error: gen PIN ($PIN_VER) != grpcio-tools floor in pyproject.toml ($FLOOR_VER); keep them equal" >&2
  exit 1
fi

VENV="$(mktemp -d)"
trap 'rm -rf "$VENV"' EXIT
python3 -m venv "$VENV"
"$VENV/bin/pip" install --quiet --upgrade pip
"$VENV/bin/pip" install --quiet "$PIN"
PY="$VENV/bin/python"

rm -f "$OUT"/*_pb2.py "$OUT"/*_pb2_grpc.py
gen() {  # gen <proto_dir> <proto_file>
  "$PY" -m grpc_tools.protoc -I "$1" --python_out="$OUT" --grpc_python_out="$OUT" "$2"
  local base="${2%.proto}"
  # protoc emits a top-level import of the sibling module; make it package-relative.
  # -i.bak is portable (BSD sed needs the suffix); remove the backup afterwards.
  sed -i.bak -E "s/^import ${base}_pb2 as /from . import ${base}_pb2 as /" "$OUT/${base}_pb2_grpc.py"
  rm -f "$OUT/${base}_pb2_grpc.py.bak"
}
gen "$CONF/harness/proto" test_harness.proto
gen "$CONF/mock_server/proto" mock_admin.proto
: > "$OUT/__init__.py"
echo "generated stubs in $OUT"
