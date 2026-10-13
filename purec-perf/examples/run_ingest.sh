#!/usr/bin/env bash
# Usage: ./run_ingest.sh [COUNT]; SP credentials must already be exported.
set +x
set -euo pipefail
sdk_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
make -C "$sdk_dir" BUILD=build
ZB_WORKSPACE_URL="https://e2-dogfood.staging.cloud.databricks.com"
ZB_WORKSPACE_ID="6051921418418893"
source "$sdk_dir/../purec-perf-bench/mint_token.sh" shinkansen.default.danilo_fluentbit_poc
"$sdk_dir/build/zbp_ingest_example" \
  6051921418418893.zerobus.us-west-2.staging.cloud.databricks.com 443 \
  "$ZB_TABLE" \
  '{"id":1,"name":"purec-poc","level":"INFO","msg":"Hello from the pure C SDK","region":"us-west-2"}' \
  "${1:-1}"
printf 'Successfully ingested and acknowledged %s record(s).\n' "${1:-1}"
