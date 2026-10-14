#!/usr/bin/env bash
#
# Run an independent MQTT conformance suite against this broker.
#
# The suite is Eclipse Paho's interoperability tests, pinned to a commit
# below. It is independent of this project, covers both protocol
# versions, and is dual licensed EPL-2.0 OR BSD-3-Clause.
#
# On the licence: this is a test tool run in CI. It is never linked into
# the shipped binary and never distributed, so the requirement is that
# its licence permit use. That is weaker than `deny.toml`, which governs
# code that ships. The distinction happens not to matter here, because
# the suite's BSD-3-Clause option is already on the allow list, but the
# rule being applied is the weaker one.
#
# The suite needs nothing but Python 3: no pip install, no broker of its
# own, no network beyond the clone.
#
# # Each test gets a fresh broker
#
# The suite's tests share one broker and one pair of clients, and two of
# them fail only because of what an earlier one left behind: run whole,
# `test_subscribe_identifiers` and `test_will_message` fail; run alone,
# both pass. A baseline taken from a whole-suite run would therefore
# encode the running order as if it were conformance. So every test is
# run on its own against a broker started for it and killed after it.
#
# # Usage
#
#   services/broker/conformance/run.sh [--update]
#
# `--update` rewrites nothing; it prints the lines that would have to
# change in known-failures.txt, so that updating the baseline is a
# deliberate edit rather than a side effect of a run.
set -uo pipefail

SUITE_REPO="https://github.com/eclipse-paho/paho.mqtt.testing.git"
# Pinned. "Add required Eclipse meta docs", 2024-01-18.
SUITE_COMMIT="9d7bb80bb8b9d9cfc0b52f8cb4c1916401281103"

# The suites hard-code localhost:1883: `client_test5.py` mangles its own
# `--port` handling before passing the rest of the command line to
# unittest, so overriding it is not reliable.
PORT=1883

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
broker_dir="$(cd "$here/.." && pwd)"
baseline="$here/known-failures.txt"
work="${CONFORMANCE_WORKDIR:-$here/.work}"
update=0
[ "${1:-}" = "--update" ] && update=1

fail() { echo "conformance: $*" >&2; exit 1; }

command -v python3 >/dev/null || fail "python3 is required"

# What this catches is a broker already listening when the run starts:
# the suites cannot be pointed elsewhere, so that broker would be the
# one under test and the run would pass or fail for the wrong reasons.
#
# It is one look at one moment and not a claim on the port. Every test
# below starts a broker and kills it, so the port is free between
# tests, and a second runner started mid-run passes this check and then
# fights this one for it. Harmless with one runner, which is the only
# way this is run.
#
# Checked where `lsof` exists; where it does not, the first test fails
# loudly anyway.
if command -v lsof >/dev/null && lsof -iTCP:"$PORT" -sTCP:LISTEN -n -P >/dev/null 2>&1; then
  fail "port $PORT is already in use; the suites cannot be pointed elsewhere"
fi

# The binary under test. Built here unless one is handed in, so that CI
# can test exactly the artifact it built.
SDMQTT="${SDMQTT:-}"
if [ -z "$SDMQTT" ]; then
  ( cd "$broker_dir" && cargo build --release -p sdmqtt ) || fail "the broker did not build"
  SDMQTT="$broker_dir/target/release/sdmqtt"
fi
[ -x "$SDMQTT" ] || fail "no broker binary at $SDMQTT"

mkdir -p "$work"
suite="$work/paho.mqtt.testing"
if [ ! -d "$suite/.git" ]; then
  git clone --quiet "$SUITE_REPO" "$suite" || fail "could not clone the suite"
fi
( cd "$suite" && git fetch --quiet origin && git checkout --quiet "$SUITE_COMMIT" ) \
  || fail "could not check out $SUITE_COMMIT"
echo "conformance: suite pinned at $SUITE_COMMIT"
echo "conformance: broker $SDMQTT"

interop="$suite/interoperability"

# ---- TLS and WebSocket -----------------------------------------------------
#
# The suite speaks plain TCP and nothing else, so for the TLS and
# WebSocket passes the broker listens on three ports — plain on
# PORT+1 (unused by those passes), TLS on PORT+2, WebSocket on PORT+3 —
# and `via.py` listens on PORT, carrying the suite's bytes through the
# layer under test. Section 16: "the full conformance suite green
# including TLS and WebSocket".
#
# A certificate is minted for the run with openssl when it is on the
# path. Without it the TLS pass is skipped and the skip is printed,
# never silent: a pass that quietly did not run is a pass that lies.
tls_pass=1
if command -v openssl >/dev/null; then
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -keyout "$work/key.pem" -out "$work/cert.pem" -subj /CN=localhost -days 2 \
    -addext subjectAltName=DNS:localhost >/dev/null 2>&1 \
    || fail "openssl could not mint a certificate for the run"
else
  echo "conformance: openssl is not on the path, so the TLS passes are SKIPPED"
  tls_pass=0
fi
# Section 12.1's access control, for the suite's `test_subscribe_failure`:
# a subscription to `test/nosubscribe` is refused for everyone and
# everything else is allowed. Written as `[[acl]]` tables because the
# suite's broker has no store and so no API-managed rules; the first
# rule flips the default to deny, which is why the second exists.
acl_tables='
[[acl]]
id = "no-subscribe"
order = 10
subject = "anyone"
filter = "test/nosubscribe"
subscribe = false

[[acl]]
id = "everything-else"
order = 100
subject = "anyone"
filter = "#"
publish = true
subscribe = true
'
cat > "$work/plain.toml" <<EOF
[listener]
address = "127.0.0.1:$PORT"

[admin]
enabled = false

[log]
level = "warn"
$acl_tables
EOF
cat > "$work/listeners.toml" <<EOF
[listener]
address = "127.0.0.1:$((PORT + 1))"

[[listeners]]
name = "secure"
kind = "tls"
address = "127.0.0.1:$((PORT + 2))"

[listeners.tls]
certificate = "$work/cert.pem"
key = "$work/key.pem"

[[listeners]]
name = "web"
kind = "ws"
address = "127.0.0.1:$((PORT + 3))"

[admin]
enabled = false

[log]
level = "warn"
$acl_tables
EOF
if [ "$tls_pass" = 0 ]; then
  # Without a certificate the file cannot name a TLS listener.
  sed -i.bak '/^\[\[listeners\]\]$/,/^$/{/secure/,/^$/d;}' "$work/listeners.toml" 2>/dev/null || true
fi

# The tests each file defines, in the order unittest would run them.
#
# The digits matter: `test_flow_control1` and `test_flow_control2` both
# truncate to `test_flow_control` without them, which is a name neither
# suite defines, so the run reports one bogus failure and silently never
# runs either test.
tests_in() {
  grep -oE '^    def (test[A-Za-z0-9_]*)' "$1" | awk '{print $2}' | sort -u
}

failures="$work/failures.txt"
: > "$failures"
passed=0
failed=0

# Wait until a loopback port answers, up to ten seconds.
listening() {
  local port="$1" tries=0
  while [ "$tries" -lt 100 ]; do
    if python3 -c "import socket,sys; s=socket.socket(); s.settimeout(0.2); sys.exit(0 if s.connect_ex(('127.0.0.1', $port)) == 0 else 1)"; then
      return 0
    fi
    tries=$((tries + 1))
    sleep 0.1
  done
  return 1
}

run_one() {
  local label="$1" script="$2" test="$3" mode="$4"
  local via_pid=""
  case "$mode" in
    plain)
      "$SDMQTT" serve --config "$work/plain.toml" > "$work/broker.log" 2>&1 &
      local pid=$!
      listening "$PORT" || fail "the broker did not listen on $PORT: $(cat "$work/broker.log")"
      ;;
    tls|ws)
      # The three-listener file; `via.py` on PORT carries the suite's
      # plain client through the layer under test.
      "$SDMQTT" serve --config "$work/listeners.toml" > "$work/broker.log" 2>&1 &
      local pid=$!
      if [ "$mode" = tls ]; then
        listening $((PORT + 2)) || fail "the TLS listener did not come up: $(cat "$work/broker.log")"
        python3 "$here/via.py" tls "$PORT" "127.0.0.1:$((PORT + 2))" "$work/cert.pem" \
          > "$work/via.log" 2>&1 &
      else
        listening $((PORT + 3)) || fail "the WebSocket listener did not come up: $(cat "$work/broker.log")"
        python3 "$here/via.py" ws "$PORT" "127.0.0.1:$((PORT + 3))" \
          > "$work/via.log" 2>&1 &
      fi
      via_pid=$!
      listening "$PORT" || fail "via.py did not listen on $PORT: $(cat "$work/via.log")"
      ;;
  esac
  local out="$work/$label-$test.out"
  ( cd "$interop" && python3 "$script" "Test.$test" ) > "$out" 2>&1
  [ -n "$via_pid" ] && kill "$via_pid" 2>/dev/null
  kill "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  [ -n "$via_pid" ] && wait "$via_pid" 2>/dev/null
  if grep -qE '^OK' "$out"; then
    passed=$((passed + 1))
    printf '  %-42s pass\n' "$label:$test"
  else
    failed=$((failed + 1))
    echo "$label:$test" >> "$failures"
    printf '  %-42s FAIL\n' "$label:$test"
  fi
}

passes="v5 client_test5.py plain
v311 client_test.py plain
v5-ws client_test5.py ws
v311-ws client_test.py ws"
if [ "$tls_pass" = 1 ]; then
  passes="$passes
v5-tls client_test5.py tls
v311-tls client_test.py tls"
fi
# `CONFORMANCE_MODES="tls ws"` runs a subset while working on one layer;
# the baseline comparison then covers those labels alone. Not for CI,
# which runs everything.
if [ -n "${CONFORMANCE_MODES:-}" ]; then
  kept=""
  while read -r spec; do
    set -- $spec
    case " $CONFORMANCE_MODES " in *" $3 "*) kept="$kept$spec
";; esac
  done <<< "$passes"
  passes="${kept%
}"
  echo "conformance: running only: $CONFORMANCE_MODES"
fi
while read -r spec; do
  set -- $spec
  label="$1"; script="$2"; mode="$3"
  echo
  echo "conformance: $label ($script, $mode)"
  while read -r t; do
    [ -n "$t" ] && run_one "$label" "$script" "$t" "$mode"
  done < <(tests_in "$interop/$script")
done <<< "$passes"

echo
echo "conformance: $passed passed, $failed failed"

# The baseline, minus comments and the classification columns. Anything
# marked `flaky` is dropped from both sides: its result is not evidence
# either way, and leaving it in would fail the build whichever way it
# landed.
expected="$work/expected.txt"
flaky="$work/flaky.txt"
grep -vE '^\s*(#|$)' "$baseline" | awk '$2 == "flaky" {print $1}' | sort -u > "$flaky"
grep -vE '^\s*(#|$)' "$baseline" | awk '$2 != "flaky" {print $1}' | sort -u > "$expected"
if [ "$tls_pass" = 0 ]; then
  # A skipped pass has no failures to compare; its baseline lines are
  # not "now passing", they were never run.
  grep -vE '^v(5|311)-tls:' "$expected" > "$expected.kept" && mv "$expected.kept" "$expected"
fi
if [ -n "${CONFORMANCE_MODES:-}" ]; then
  ran=""
  while read -r spec; do
    set -- $spec
    ran="$ran|$1"
  done <<< "$passes"
  grep -E "^(${ran#|}):" "$expected" > "$expected.kept" && mv "$expected.kept" "$expected" || : > "$expected"
fi
sort -u "$failures" -o "$failures"
comm -23 "$failures" "$flaky" > "$failures.kept" && mv "$failures.kept" "$failures"
if [ -s "$flaky" ]; then
  echo "conformance: ignored as flaky: $(tr '\n' ' ' < "$flaky")"
fi

new_failures="$(comm -23 "$failures" "$expected")"
now_passing="$(comm -13 "$failures" "$expected")"

status=0
if [ -n "$new_failures" ]; then
  echo
  echo "conformance: NEW FAILURES (not in known-failures.txt):"
  echo "$new_failures" | sed 's/^/  /'
  status=1
fi
if [ -n "$now_passing" ]; then
  echo
  echo "conformance: listed as failing but now passing; remove these lines:"
  echo "$now_passing" | sed 's/^/  /'
  status=1
fi

if [ "$update" = 1 ]; then
  echo
  echo "conformance: the current failing set is:"
  sed 's/^/  /' "$failures"
  exit 0
fi

[ "$status" = 0 ] && echo "conformance: the failing set matches the baseline"
exit "$status"
