#!/usr/bin/env bash
#
# Section 15's connection target: 100,000 concurrent connections per node.
#
#     scripts/connection-target.sh            # in the development container
#     CONNECTIONS=20000 HOLD=10 scripts/connection-target.sh
#
# Runs in the development image, on Linux, because that is what a node
# runs on and because every address in 127.0.0.0/8 is loopback there,
# which is how one generator opens more connections than one source
# address has ports. It needs Docker and nothing else on the host.
#
# # Two phases
#
# 1. **The target.** The broker with a descriptor limit far above the
#    target and `max_connections` above it; `sdmqtt-load` opens
#    CONNECTIONS (100,000), holds them HOLD seconds (60) and pings each
#    at the end. Fails unless every one was held. Prints the broker's
#    resident memory idle and at the peak, and per connection.
# 2. **Running out of descriptors.** The same broker under `ulimit -n
#    4096` and 6,000 connections. The broker must stay up and ready, must
#    count what happened (`sdmqtt_accept_pauses_total`), and must not
#    spin: it uses under one core while the excess waits in its backlog.
#
# Exits non-zero naming the phase and the check that failed. The machine
# is printed first, because a connection count means nothing without it.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
image="${IMAGE:-devcontainer-broker:latest}"

if [ "${1:-}" != "--inside" ]; then
    if ! docker image inspect "$image" >/dev/null 2>&1; then
        docker build -f "$here/.devcontainer/base.Dockerfile" -t "$image" "$here"
    fi
    exec docker run --rm \
        --ulimit nofile=1048576:1048576 \
        --sysctl net.ipv4.ip_local_port_range="1024 65000" \
        --sysctl net.core.somaxconn=65535 \
        -v "$here:/workspaces/repo" \
        -v sdmqtt-linux-target:/target \
        -e CARGO_TARGET_DIR=/target \
        -e CONNECTIONS="${CONNECTIONS:-100000}" \
        -e HOLD="${HOLD:-60}" \
        -w /workspaces/repo \
        "$image" bash -lc "scripts/connection-target.sh --inside"
fi

connections="${CONNECTIONS:-100000}"
hold="${HOLD:-60}"
bin=/target/release

echo "machine: $(nproc) cores, $(awk '/MemTotal/ {printf "%.1f GB", $2/1048576}' /proc/meminfo), kernel $(uname -r), $(uname -m)"
echo "limits: $(ulimit -n) descriptors, ports $(cat /proc/sys/net/ipv4/ip_local_port_range | tr '\t' '-')"

(cd services/broker && cargo build --release -q -p sdmqtt -p sdmqtt-load)

rss_kb() { awk '/VmRSS/ {print $2}' "/proc/$1/status"; }
cpu_ticks() { awk '{print $14 + $15}' "/proc/$1/stat"; }
metric() { curl -s http://127.0.0.1:1884/metrics | awk -v name="$1" '$1 == name {print $2}'; }

# Start a broker with `$1` descriptors, and wait until it is ready.
start() {
    local limit="$1"
    rm -rf /tmp/store
    ( ulimit -n "$limit"
      exec env SDMQTT_CONFIG= SDMQTT_MAX_CONNECTIONS=200000 "$bin/sdmqtt" serve \
          --address 127.0.0.1:1883 --admin-address 127.0.0.1:1884 \
          --store-directory /tmp/store ) >/tmp/broker.log 2>&1 &
    broker=$!
    for _ in $(seq 100); do
        curl -s http://127.0.0.1:1884/health/ready | grep -q '"ready"' && return 0
        sleep 0.2
    done
    echo "the broker never became ready:" >&2
    tail -20 /tmp/broker.log >&2
    exit 1
}

stop() {
    kill -TERM "$broker" 2>/dev/null || true
    wait "$broker" 2>/dev/null || true
}

failed=0

echo
echo "phase 1: $connections connections held for ${hold}s"
start 1048576
idle=$(rss_kb "$broker")
peak=$idle
( while kill -0 "$broker" 2>/dev/null; do
      now=$(rss_kb "$broker" 2>/dev/null || echo 0)
      [ "${now:-0}" -gt "$(cat /tmp/peak 2>/dev/null || echo 0)" ] && echo "$now" > /tmp/peak
      sleep 1
  done ) &
sampler=$!
if "$bin/sdmqtt-load" --connections "$connections" --hold "$hold"; then
    echo "  held: all $connections"
else
    echo "  FAILED: the target was not held" >&2
    failed=1
fi
peak=$(cat /tmp/peak 2>/dev/null || echo "$idle")
kill "$sampler" 2>/dev/null || true
echo "  broker resident: idle $((idle / 1024)) MB, peak $((peak / 1024)) MB," \
     "$(( (peak - idle) * 1024 / connections )) bytes a connection"
stop

echo
echo "phase 2: 6000 connections against a broker limited to 4096 descriptors"
start 4096
"$bin/sdmqtt-load" --connections 6000 --hold 10 --concurrency 6000 > /tmp/load.log 2>&1 &
load=$!
sleep 8
before=$(cpu_ticks "$broker")
sleep 5
after=$(cpu_ticks "$broker")
ticks=$(getconf CLK_TCK)
used=$(( (after - before) * 100 / (5 * ticks) ))
wait "$load" || true
sed 's/^/  /' /tmp/load.log
pauses=$(metric sdmqtt_accept_pauses_total)
ready=$(curl -s http://127.0.0.1:1884/health/ready | grep -c '"ready"' || true)
echo "  broker: ${used}% of one core while the excess waited, ${pauses:-0} accept pauses, ready=${ready}"
if [ "$ready" != 1 ]; then echo "  FAILED: the broker is not ready after running out" >&2; failed=1; fi
if [ "${pauses:-0}" = 0 ]; then echo "  FAILED: running out of descriptors was not counted" >&2; failed=1; fi
if [ "$used" -ge 100 ]; then echo "  FAILED: the broker spun at ${used}% while out of descriptors" >&2; failed=1; fi
stop

exit "$failed"
