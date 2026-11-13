#!/usr/bin/env bash
# Generates synthetic traces, writes them with otlp2parquet in three layouts (JSON strings,
# VARIANT, shredded VARIANT) and runs the DataFusion benchmark over them.
#
#   scripts/run.sh [spans] [runs]
#
# Needs a release otlp2parquet built with the `variant` feature (make build-cli-variant)
# at $BIN (default target/release/otlp2parquet), uv, curl and cargo.
set -euo pipefail

SPANS="${1:-1000000}"
RUNS="${2:-5}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
BIN="${BIN:-$ROOT/target/release/otlp2parquet}"
WORK="${WORK:-$HERE/work}"
PORT="${PORT:-14330}"
[ -x "$BIN" ] || { echo "missing $BIN: run 'make build-cli-variant' first" >&2; exit 1; }

SERVER_PID=""
trap '[ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true' EXIT

mkdir -p "$WORK"
if [ ! -d "$WORK/payload" ] || [ "$(cat "$WORK/payload.spans" 2>/dev/null)" != "$SPANS" ]; then
  rm -rf "$WORK/payload"
  uv run "$HERE/scripts/gen_otlp.py" "$WORK/payload" "$SPANS" 1500
  echo "$SPANS" > "$WORK/payload.spans"
fi

# $1 layout name, $2 extra [parquet.variant] config
write_layout() {
  local name="$1" variant="$2" dir="$WORK/$1"
  rm -rf "$dir"; mkdir -p "$dir"
  cat > "$dir/config.toml" <<TOML
[batch]
max_rows = 250000
max_bytes = 268435456
max_age_secs = 5
enabled = true
[server]
listen_addr = "127.0.0.1:$PORT"
log_level = "warn"
log_format = "text"
[storage]
backend = "fs"
[storage.fs]
path = "$dir/data"
$variant
TOML
  "$BIN" --config "$dir/config.toml" >"$dir/server.log" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 50); do
    curl -fs "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && break
    sleep 0.2
  done
  for f in "$WORK"/payload/*.json; do
    curl -fsS -X POST "http://127.0.0.1:$PORT/v1/traces" \
      -H 'Content-Type: application/json' --data-binary "@$f" >/dev/null
  done
  # Wait for the age-based flush of the last partial batch, then stop the server.
  sleep 8
  kill "$SERVER_PID"; wait "$SERVER_PID" 2>/dev/null || true; SERVER_PID=""
  echo "$name: $(find "$dir/data" -name '*.parquet' | wc -l) files, $(du -sh "$dir/data" | cut -f1)"
}

write_layout plain ""
write_layout variant '[parquet.variant]
enabled = true
columns = ["resource_attributes", "span_attributes"]'
write_layout shredded '[parquet.variant]
enabled = true
columns = ["resource_attributes", "span_attributes"]
[parquet.variant.shred.traces]
span_attributes = { "http.route" = "utf8", "http.request.method" = "utf8", "http.response.status_code" = "int64" }
resource_attributes = { "service.namespace" = "utf8" }'

cd "$HERE"
cargo build --release -q
for l in plain variant shredded; do
  rm -rf "$WORK/$l/compact"
  ./target/release/variant-bench compact "$WORK/$l/data/traces" "$WORK/$l/compact"
done
./target/release/variant-bench "$WORK/plain/compact" "$WORK/variant/compact" \
  "$WORK/shredded/compact" "$RUNS"
