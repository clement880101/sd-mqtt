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
# This said the interface bundle did not exist yet and printed SKIPPED
# for it. It exists now and is measured. The rule it was an instance of
# stands: a check that quietly passes when it cannot measure is worse
# than no check, so an unbuilt interface is an error naming what to run
# rather than a budget quietly met.
#
# # The tools it measures with are checked before it measures anything
#
# The rule above, applied to this script itself. A missing `cargo` or
# `python3` is not a budget met; it is a budget nobody weighed, and
# every path to that answer here now ends in an exit code that stops a
# build. `scripts/test_budgets.sh` is what says so, because a check
# whose failure path is never run is a check nobody has seen fail.
#
# Two habits go with it. Every measurement is a number before it is
# compared, so a budget cannot pass because an empty string compared
# oddly; and a command whose failure matters is tested with `if !`
# rather than by reading `$?` afterwards, which is one inserted line
# away from reading somebody else's status.
#
# # Usage
#
#   scripts/budgets.sh
#
# Exits non-zero when a budget is exceeded, naming which and by how
# much; **2** when it could not measure at all, which is the case worth
# telling apart. Needs cargo and python3; python3 reads
# `cargo metadata`'s JSON, which is the only description of the
# dependency graph that is not a manifest this script would have to
# parse by hand.
#
# One warning for whoever runs it: `scripts/budgets.sh | tee log`
# reports `tee`'s exit status and not this one. Run it on its own, or
# set `pipefail` in the shell that pipes it.
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
broker="$here/services/broker"

# Everything this measures with, checked before anything is measured.
#
# Named one at a time rather than as a list, because "python3 is not
# installed" and "cargo is not on this PATH" have different answers and
# the whole point of this section is that the person reading it is
# about to do something about it.
missing=0
for tool in cargo python3 gzip find; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "  $tool is not on the PATH, and the budgets cannot be measured without it" >&2
        missing=$((missing + 1))
    fi
done
if [ "$missing" -gt 0 ]; then
    echo >&2
    echo "  A budget that quietly passes when it cannot measure is worse than no" >&2
    echo "  budget, so this is an error rather than a skip. For cargo:" >&2
    echo "    source \"\$HOME/.cargo/env\"" >&2
    exit 2
fi

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

# "UI bundle, gzipped: under 150 KB".
BUDGET_BUNDLE_BYTES=150000

checked=0
over=0
failures=()

# Print one budget's line, and remember it if it is over.
#
# $1 name, $2 measured, $3 limit, $4 the function that renders a
# number of these units. "Under" is strict: section 15 says under 15
# MB, so exactly 15 MB is over.
report() {
    local name="$1" measured="$2" limit="$3" render="$4"
    # A measurement that is not a number is a measurement that did not
    # happen. `[ "" -ge 15000000 ]` is an error bash reports as false,
    # which would print a budget comfortably met on the strength of
    # nothing at all.
    case "$measured" in
        '' | *[!0-9]*)
            echo "  $name could not be measured: ${measured:-nothing came back}" >&2
            exit 2
            ;;
    esac
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

# `if !` rather than reading `$?` on the next line: the status is
# consumed by the thing that cares about it, and no line inserted
# between the two can quietly change which command is being asked
# about.
if ! build_log="$(cd "$broker" && cargo build --release --all-features -p sdmqtt 2>&1)"; then
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

# Vite's default output directory, named by `services/web/vite.config.ts`.
#
# **Summed per file, not gzipped as one stream.** Every document is
# compressed on its own by `crates/admin/build.rs` and served on its
# own, so the sum of the individual compressed sizes is what a browser
# downloads and what the binary carries. Concatenating first and
# compressing once reads several kilobytes lower, because the
# compressor gets to share a dictionary across files that never travel
# together, and a budget that flatters itself is not a budget.
#
# Everything in the directory counts, not only scripts and styles:
# fonts, icons and images are embedded and served too, and a budget
# that looks at half of what ships is the same mistake in a different
# place.
bundle_dir="$here/services/web/dist"
if [ ! -d "$bundle_dir" ]; then
    # Not skipped. It was skipped while there was no interface to
    # weigh; there is one now, and a check that quietly passes when it
    # cannot measure is worse than no check.
    echo "  the interface bundle is not built, so it cannot be weighed" >&2
    echo "  build it first:  (cd services/web && npm ci && npm run build)" >&2
    exit 2
fi
bundle_bytes=$(
    find "$bundle_dir" -type f -exec gzip -9 -c {} + 2>/dev/null |
        wc -c | tr -d ' '
)
if [ -z "$bundle_bytes" ] || [ "$bundle_bytes" -eq 0 ]; then
    echo "  $bundle_dir holds no files; the interface build produced nothing" >&2
    exit 2
fi
report "interface bundle, gzipped" "$bundle_bytes" "$BUDGET_BUNDLE_BYTES" kilobytes
bundle_files=$(find "$bundle_dir" -type f | wc -l | tr -d ' ')
printf '      %s documents, each compressed and embedded on its own\n' "$bundle_files"

# ---------------------------------------------------------------------

echo
printf '  %d measured, %d over budget\n' "$checked" "$over"
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
