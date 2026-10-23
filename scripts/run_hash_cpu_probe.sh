#!/usr/bin/env bash
set -euo pipefail

# Run hash and control modes in separate processes with matching JVM and CPU settings.
# Example: scripts/run_hash_cpu_probe.sh results/hash-16 16 100000000 hash -Xms2g -Xmx2g
if [ "$#" -lt 4 ]; then
  echo "usage: $0 NEW_OUTPUT_DIR KEY_BYTES OPERATIONS hash|control [JVM_ARGS...]" >&2
  exit 2
fi
[ "$(uname -s)" = Linux ] || { echo "Linux perf is required" >&2; exit 2; }
command -v perf >/dev/null || { echo "perf is required" >&2; exit 2; }
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 2; }

PROBE_ROOT=$(cd "$(dirname "$0")/.." && pwd)
PROBE_OUTPUT=$1
PROBE_BYTES=$2
PROBE_OPERATIONS=$3
PROBE_MODE=$4
shift 4
JAVA_BIN=${JAVA_BIN:-java}
PROBE_JAR=${PROBE_JAR:-"$PROBE_ROOT/red-ohc-jmh/target/red-ohc-jmh-1.0.0-benchmarks.jar"}
PERF_EVENTS=${PERF_EVENTS:-cycles:u,instructions:u,branches:u,branch-misses:u,cache-references:u,cache-misses:u}
python3 - "$PROBE_BYTES" "$PROBE_OPERATIONS" "$PROBE_MODE" <<'PYTHON'
import sys

key_bytes = int(sys.argv[1])
operations = int(sys.argv[2])
mode = sys.argv[3]
if not 0 <= key_bytes <= 1 << 20 or not 0 < operations <= (1 << 63) - 1:
    raise SystemExit("invalid key size or operation count")
if mode not in {"hash", "control"}:
    raise SystemExit("mode must be hash or control")
PYTHON
[ -f "$PROBE_JAR" ] || { echo "build the Maven reactor package first" >&2; exit 2; }
mkdir "$PROBE_OUTPUT"
PROBE_OUTPUT=$(cd "$PROBE_OUTPUT" && pwd)
PROBE_CONTROL="$PROBE_OUTPUT/control.fifo"
PROBE_ACK="$PROBE_OUTPUT/ack.fifo"
mkfifo "$PROBE_CONTROL" "$PROBE_ACK"
trap 'rm -f "$PROBE_CONTROL" "$PROBE_ACK"' EXIT

export LC_ALL=C
uname -a >"$PROBE_OUTPUT/system.txt"
lscpu >>"$PROBE_OUTPUT/system.txt"
"$JAVA_BIN" -version >"$PROBE_OUTPUT/java.txt" 2>&1
perf --version >"$PROBE_OUTPUT/perf-version.txt"
printf '%s\n' "$JAVA_BIN" "$@" >"$PROBE_OUTPUT/jvm-args.txt"

perf stat -D -1 --control="fifo:$PROBE_CONTROL,$PROBE_ACK" \
  -x ';' -o "$PROBE_OUTPUT/perf.csv" -e "$PERF_EVENTS" -- \
  "$JAVA_BIN" "$@" -cp "$PROBE_JAR" com.red.ohc.jmh.HashCpuProbe \
  "$PROBE_BYTES" "$PROBE_OPERATIONS" "$PROBE_MODE" "$PROBE_CONTROL" "$PROBE_ACK" \
  >"$PROBE_OUTPUT/probe.json" 2>"$PROBE_OUTPUT/java-stderr.txt"

python3 - "$PROBE_OUTPUT" <<'PYTHON'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
probe = json.loads((root / "probe.json").read_text())
operations = probe["operations"]
events = {}
invalid = []
for line in (root / "perf.csv").read_text().splitlines():
    if not line.strip() or line.lstrip().startswith("#"):
        continue
    columns = [column.strip() for column in line.split(";")]
    if len(columns) < 5 or not columns[0] or not columns[2]:
        continue
    try:
        count = float(columns[0])
        running = float(columns[4].rstrip("%"))
    except ValueError:
        invalid.append(line)
        continue
    events[columns[2]] = {
        "count": count,
        "unit": columns[1],
        "per_operation": count / operations,
        "runtime": columns[3],
        "running_percent": running,
    }
result = {
    "probe": probe,
    "events": events,
    "unavailable": invalid,
    "wall_ns_per_operation": probe["wallNanos"] / operations,
    "thread_cpu_ns_per_operation": (
        None if probe["threadCpuNanos"] is None
        else probe["threadCpuNanos"] / operations
    ),
    "counter_scope": "process user-mode events, gated after warmup; includes harness overhead",
}
(root / "cpu-ops.json").write_text(json.dumps(result, indent=2) + "\n")
if invalid or not events:
    raise SystemExit("missing/unsupported counters; inspect preserved perf.csv and cpu-ops.json")
print(root / "cpu-ops.json")
PYTHON
