#!/usr/bin/env bash
# Build the sibling SDK and benchmark, mint a scoped token, and run a matrix config.
set +x
set -euo pipefail

usage() {
  printf 'Usage: %s CONFIG [matrix flags...]\n' "$0"
  printf 'Defaults: --stage finalists --producers 1 --repeats 1 --output results/<timestamp>.json\n'
  printf 'Use --dry-run to preview without building, minting a token, or ingesting.\n'
}

if [[ $# == 0 ]]; then usage >&2; exit 2; fi
if [[ $1 == --help || $1 == -h ]]; then usage; exit 0; fi
config=$1
shift
bench_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
sdk_dir="$bench_dir/../purec-perf"
dry_run=false
for arg in "$@"; do
  case "$arg" in
    --help|-h) usage; python3 "$bench_dir/run_matrix.py" --help; exit 0 ;;
    --bench|--bench=*|--config|--config=*)
      printf 'run.sh: --bench and --config are selected by the wrapper; pass CONFIG first.\n' >&2
      exit 2 ;;
    --d|--dr|--dry|--dry-|--dry-r|--dry-ru|--dry-run) dry_run=true ;;
  esac
done

matrix=(python3 "$bench_dir/run_matrix.py"
  --stage finalists --producers 1 --repeats 1
  --output "$bench_dir/results/$(date -u +%Y%m%dT%H%M%S%N)-$$.json" "$@"
  --bench "$bench_dir/build/zbperf-bench" --config "$config")
# Resolve forwarded flags with the runner itself before any build or token request.
plan="$("${matrix[@]}" --dry-run)"
table="$(jq -er '.baseline.table | select(type == "string" and length > 0)' "$config")"
if ! jq -e --slurpfile config "$config" '
  all(.runs[].options;
    .table == $config[0].baseline.table and
    .host == $config[0].baseline.host and
    (.port | tostring) == ($config[0].baseline.port | tostring) and
    (."token-env" // "ZB_TOKEN") == "ZB_TOKEN" and
    (."test-plaintext" // false) == false)
' <<<"$plan" >/dev/null; then
  printf 'run.sh: all runs must use baseline host/port/table, ZB_TOKEN, and TLS.\n' >&2
  exit 2
fi
if "$dry_run"; then printf '%s\n' "$plan"; exit 0; fi

cmake -S "$sdk_dir" -B "$sdk_dir/build" -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DZBPERF_SANITIZE= \
  -DCMAKE_INSTALL_PREFIX="$sdk_dir/install" -DCMAKE_INSTALL_LIBDIR=lib
cmake --build "$sdk_dir/build" --parallel
cmake --install "$sdk_dir/build"
cmake -S "$bench_dir" -B "$bench_dir/build" -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
  -Dzbperf_DIR="$sdk_dir/install/lib/cmake/zbperf"
cmake --build "$bench_dir/build" --parallel
mkdir -p "$bench_dir/results"
source "$bench_dir/mint_token.sh" "$table"
exec "${matrix[@]}"
