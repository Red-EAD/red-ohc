#!/usr/bin/env bash

set -u
set -o pipefail

if [ "$#" -lt 3 ]; then
  echo "usage: run_with_rss_guard.sh RSS_LIMIT_BYTES TIME_PATH COMMAND..." >&2
  exit 2
fi

RSS_LIMIT_BYTES=$1
TIME_PATH=$2
shift 2

case "$RSS_LIMIT_BYTES" in
  ''|*[!0-9]*)
    echo "RSS_LIMIT_BYTES must be a non-negative integer" >&2
    exit 2
    ;;
esac

run_without_guard() {
  /usr/bin/time -p -o "$TIME_PATH" "$@"
  local status=$?
  printf 'rss_guard=disabled\nrss_limit_bytes=%s\nrss_peak_bytes=\n' \
    "$RSS_LIMIT_BYTES" >> "$TIME_PATH"
  return "$status"
}

if [ "$(uname -s)" != "Linux" ] || [ "$RSS_LIMIT_BYTES" -eq 0 ]; then
  run_without_guard "$@"
  exit $?
fi

if ! command -v ps >/dev/null 2>&1 || ! command -v setsid >/dev/null 2>&1; then
  echo "Linux RSS guard requires ps and setsid" >&2
  printf 'rss_guard=unavailable\nrss_limit_bytes=%s\nrss_peak_bytes=\n' \
    "$RSS_LIMIT_BYTES" >> "$TIME_PATH"
  exit 2
fi

rss_for_process_tree() {
  ps -e -o pid=,ppid=,rss= | awk -v root="$1" '
    {
      parent[$1] = $2;
      resident[$1] = $3;
    }
    END {
      total = 0;
      for (pid in parent) {
        current = pid;
        while (current != root && current != "" && current != 1 && current != 0) {
          current = parent[current];
        }
        if (current == root) {
          total += resident[pid] * 1024;
        }
      }
      printf "%.0f\n", total;
    }'
}

setsid /usr/bin/time -p -o "$TIME_PATH" "$@" &
ROOT_PID=$!
peak_bytes=0
rss_killed=0
rss_parse_failed=0

while kill -0 "$ROOT_PID" 2>/dev/null; do
  current_bytes=$(rss_for_process_tree "$ROOT_PID")
  case "$current_bytes" in
    ''|*[!0-9]*)
      echo "RSS guard failed to parse process RSS: $current_bytes" >&2
      rss_parse_failed=1
      kill -TERM -"$ROOT_PID" 2>/dev/null || kill -TERM "$ROOT_PID" 2>/dev/null || true
      sleep 1
      kill -0 "$ROOT_PID" 2>/dev/null && \
        (kill -KILL -"$ROOT_PID" 2>/dev/null || kill -KILL "$ROOT_PID" 2>/dev/null || true)
      break
      ;;
  esac
  if [ "$current_bytes" -gt "$peak_bytes" ]; then
    peak_bytes=$current_bytes
  fi
  if [ "$current_bytes" -gt "$RSS_LIMIT_BYTES" ]; then
    rss_killed=1
    kill -TERM -"$ROOT_PID" 2>/dev/null || kill -TERM "$ROOT_PID" 2>/dev/null || true
    sleep 1
    kill -0 "$ROOT_PID" 2>/dev/null && \
      (kill -KILL -"$ROOT_PID" 2>/dev/null || kill -KILL "$ROOT_PID" 2>/dev/null || true)
    break
  fi
  sleep "${RED_OHC_RSS_POLL_INTERVAL_SECONDS:-0.2}"
done

wait "$ROOT_PID"
status=$?
if [ "$rss_parse_failed" -ne 0 ]; then
  status=2
elif [ "$rss_killed" -ne 0 ]; then
  status=137
fi
printf 'rss_guard=enabled\nrss_limit_bytes=%s\nrss_peak_bytes=%s\nrss_killed=%s\nrss_parse_failed=%s\n' \
  "$RSS_LIMIT_BYTES" "$peak_bytes" "$rss_killed" "$rss_parse_failed" >> "$TIME_PATH"
exit "$status"
