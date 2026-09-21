#!/usr/bin/env bash
#
# Check the budgets section 15 of the design says are checked.
#
# Section 15 has said "Budgets are checked in CI. Exceeding one fails
# the build" since before milestone 1. It was not true, and two
# milestones shipped with nothing enforced; convention 8's walk found
# it. This is the script that makes it true.
#
# # Three of the five, and why the other two are not here
#
# Idle resident memory needs a process started and left to settle, so
# it is a scheduled job rather than a gate on every push. The
# concurrent-connection target is a load test on target hardware and is
# milestone 7's to prove. Neither belongs on a pull request.
#
# # Every budget prints its measurement, passing or not
#
# A budget at 94% and a budget at 4% are different situations and a
# green tick hides which one you are in. The point of a budget is the
# trend towards it, so the number is printed every run.
#
# # A budget that cannot be measured says so
#
# The interface bundle does not exist yet. A check that quietly passes
# when it cannot measure is worse than no check, because it reports
# three budgets met when it looked at two. That line prints SKIPPED and
# says why, and the summary counts it separately.
#
# # Usage
#
#   scripts/budgets.sh
#
# Exits non-zero when a budget is exceeded, naming which and by how
# much. Needs cargo and python3; python3 reads `cargo metadata`'s JSON,
# which is the only description of the dependency graph that is not a
# manifest this script would have to parse by hand.
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
broker="$here/services/broker"

# ---------------------------------------------------------------------
# The budgets, from section 15.
# ---------------------------------------------------------------------

# "Release binary, all features, stripped, link-time optimized: under
# 15 MB". Decimal megabytes, which is the stricter of the two readings
# and therefore the safe one: 15,000,000 bytes is less than 15 MiB, so
# a binary this accepts is under the budget on either reading.
BUDGET_BINARY_BYTES=15000000

# "Direct dependencies across broker crates: under 35". Counted as
# distinct external crates named in the workspace's `[dependencies]`.
# Distinct, because the budget is about how much of somebody else's
# code ships, and a crate two of ours both use is one thing to audit,
# not two. Normal dependencies only: a dev-dependency is not
# distributed, and this is the same rule `conformance/run.sh` applies
# to the suite it downloads.
BUDGET_DIRECT_DEPS=35

# "UI bundle, gzipped: under 150 KB". Not measurable yet; see below.
BUDGET_BUNDLE_BYTES=150000

checked=0
over=0
skipped=0
failures=()

# Print one budget's line, and remember it if it is over.
#
# $1 name, $2 measured, $3 limit, $4 the function that renders a
# number of these units. "Under" is strict: section 15 says under 15
# MB, so exactly 15 MB is over.
report() {
    local name="$1" measured="$2" limit="$3" render="$4"
    checked=$((checked + 1))
    local shown allowed
    shown="$("$render" "$measured")"
    allowed="$("$render" "$limit")"
    if [ "$measured" -ge "$limit" ]; then
        over=$((over + 1))
        local excess
        excess="$("$render" $((measured - limit)))"
        local percent=$((measured * 100 / limit))
        failures+=("$name: $shown against a budget of $allowed, over by $excess ($percent% of it)")
        printf '  %-32s %13s of %-12s OVER BUDGET by %s\n' \
            "$name" "$shown" "$allowed" "$excess"
    else
        printf '  %-32s %13s of %-12s %3d%% headroom   ok\n' \
            "$name" "$shown" "$allowed" "$(((limit - measured) * 100 / limit))"
    fi
}

# Print one budget that could not be measured, and why.
skip() {
    local name="$1" why="$2"
    skipped=$((skipped + 1))
    printf '  %-32s %13s %-12s   SKIPPED\n' "$name" "not" "measured"
    printf '      %s\n' "$why"
}

megabytes() {
    python3 -c "print(f'{$1 / 1000000:.2f} MB')"
}

kilobytes() {
    python3 -c "print(f'{$1 / 1000:.1f} KB')"
}

plain() {
    printf '%s' "$1"
}

echo "sd-mqtt budgets, from section 15 of the design"
echo

# ---------------------------------------------------------------------
# The release binary.
# ---------------------------------------------------------------------

build_log="$(cd "$broker" && cargo build --release --all-features -p sdmqtt 2>&1)"
if [ $? -ne 0 ]; then
    # Printed, not summarised. A gate that says only "it did not
    # build" makes the next person run the build again to find out
    # why, which is the whole of what this already knows.
    echo "  the release binary did not build; nothing can be measured" >&2
    echo "$build_log" >&2
    exit 2
fi
binary="$broker/target/release/sdmqtt"
if [ ! -f "$binary" ]; then
    echo "  the release binary is not where it was expected: $binary" >&2
    exit 2
fi
# `wc -c` rather than `stat`, whose flags differ between GNU and BSD.
binary_bytes=$(wc -c <"$binary" | tr -d ' ')
report "release binary, stripped" "$binary_bytes" "$BUDGET_BINARY_BYTES" megabytes

# ---------------------------------------------------------------------
# Direct dependencies.
# ---------------------------------------------------------------------

direct=$(cd "$broker" && cargo metadata --format-version 1 --no-deps 2>/dev/null | python3 -c '
import json
import sys

metadata = json.load(sys.stdin)
ours = {package["name"] for package in metadata["packages"]}
external = set()
for package in metadata["packages"]:
    for dependency in package["dependencies"]:
        # `kind` is null for a normal dependency, "dev" or "build"
        # otherwise. Only the normal ones are distributed.
        if dependency["kind"] is not None:
            continue
        # A crate in this workspace is not somebody else’s code.
        if dependency["name"] in ours:
            continue
        external.add(dependency["name"])
print(len(external))
print(" ".join(sorted(external)))
')
direct_count=$(echo "$direct" | head -1)
direct_names=$(echo "$direct" | tail -1)
if [ -z "$direct_count" ]; then
    echo "  the dependency graph could not be read" >&2
    exit 2
fi
report "direct dependencies, distinct" "$direct_count" "$BUDGET_DIRECT_DEPS" plain
printf '      %s\n' "$direct_names"

# ---------------------------------------------------------------------
# The interface bundle.
# ---------------------------------------------------------------------

# Vite's default output directory, in the service the interface is
# scaffolded in. Empty but for a dev container today.
bundle_dir="$here/services/web/dist"
if [ -d "$bundle_dir" ]; then
    bundle_bytes=$(
        find "$bundle_dir" -type f \( -name '*.js' -o -name '*.css' -o -name '*.html' \) \
            -exec cat {} + | gzip -9 -c | wc -c | tr -d ' '
    )
    report "interface bundle, gzipped" "$bundle_bytes" "$BUDGET_BUNDLE_BYTES" kilobytes
else
    skip "interface bundle, gzipped" \
        "there is no interface yet. services/web holds nothing but a dev container, so there is no services/web/dist to weigh; milestone 3 builds it. Printed rather than omitted: a check that quietly passes when it cannot measure reports three budgets met having looked at two."
fi

# ---------------------------------------------------------------------

echo
printf '  %d measured, %d over budget, %d skipped\n' "$checked" "$over" "$skipped"
if [ "$over" -gt 0 ]; then
    echo
    for failure in "${failures[@]}"; do
        echo "  OVER BUDGET  $failure" >&2
    done
    echo >&2
    echo "  Section 15: exceeding a budget fails the build and requires either a" >&2
    echo "  fix or an explicit, recorded decision to move the budget." >&2
    exit 1
fi
exit 0
