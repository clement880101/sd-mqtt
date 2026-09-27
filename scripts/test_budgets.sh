#!/usr/bin/env bash
#
# Tests for `scripts/budgets.sh`: the paths where it cannot measure.
#
#     scripts/test_budgets.sh
#
# The budget script's own header says a check that quietly passes when
# it cannot measure is worse than no check. Nothing said whether that
# was true of the script itself, and a failure path nobody has watched
# fail is a failure path nobody knows the exit code of. A reviewer
# reported that a missing `cargo` was reported and then exited zero;
# that did not reproduce here, and the honest answer to "I could not
# reproduce it" is not "then there is nothing to do" — it is a test
# that would have caught it either way.
#
# It asserts exit codes rather than output, because the exit code is
# the whole of what a continuous integration job reads.
#
# # Nothing here builds anything
#
# Every case removes something the script needs before it gets as far
# as a build, so this costs a second and can run wherever the budget
# job runs. The budgets themselves are measured by running the script
# for real, which is the step after this one.
#
# One thing this cannot assert: that `budgets.sh | tee log` fails. It
# does not, because a pipeline reports its last command, and that is a
# property of the shell running it rather than of this script. The
# usage note says so.
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
budgets="$here/budgets.sh"

failures=0

# A directory holding a symbolic link to each named tool, and nothing
# else, for use as a whole PATH.
#
# Built this way rather than by pruning directories out of the real
# PATH, because on this platform `python3` and half the shell live in
# one directory: a PATH that keeps `wc` keeps `python3` with it, and a
# case that meant to remove one tool removes none. Written down
# because the first version of this file made exactly that mistake and
# reported a hole in the script that was a hole in the test.
sandbox() {
    local directory
    directory="$(mktemp -d)"
    sandboxes+=("$directory")
    local tool resolved
    for tool in "$@"; do
        resolved="$(command -v "$tool" 2>/dev/null)" || continue
        ln -s "$resolved" "$directory/$tool"
    done
    printf '%s' "$directory"
}

sandboxes=()
# A loop rather than `rm -rf "${sandboxes[@]}"`: under `set -u` an
# empty array is an unbound variable on the bash this platform ships,
# and a cleanup that fails is noise on every run.
tidy() {
    local directory
    for directory in ${sandboxes[@]+"${sandboxes[@]}"} "${elsewhere:-}"; do
        [ -n "$directory" ] && rm -rf "$directory"
    done
}
trap tidy EXIT

# Run the budget script with `$1` as its whole PATH, and require exit
# `$2`. `$3` names the case, so a failure says which one rather than
# only that there was one.
expect() {
    local path="$1" wanted="$2" what="$3"
    local status
    PATH="$path" "$budgets" >/dev/null 2>&1
    status=$?
    if [ "$status" -eq "$wanted" ]; then
        printf '  ok        %s (exit %d)\n' "$what" "$status"
    else
        printf '  NOT OK    %s: wanted exit %d and got %d\n' \
            "$what" "$wanted" "$status" >&2
        failures=$((failures + 1))
    fi
}

# Everything the script reaches for, so that each case below is the
# full set less exactly one thing.
ALL=(bash env dirname cargo python3 gzip find wc tr cut sed)

echo "budgets.sh, where it cannot measure"
echo

# One case per tool it measures with. Each is the whole set less that
# one, so a case that passed because something *else* was missing
# would have to be missing something this script does not use.
for absent in cargo python3 gzip find; do
    kept=()
    for tool in "${ALL[@]}"; do
        [ "$tool" = "$absent" ] || kept+=("$tool")
    done
    expect "$(sandbox "${kept[@]}")" 2 "no $absent on the PATH is an error, not a pass"
done

# There is no case here for "every tool present, budgets measured",
# and that is deliberate rather than missing: it is a release build and
# an interface build, which is what running `scripts/budgets.sh` for
# real is, and it is the step after this one wherever this runs. A
# second copy of it here would double the slowest job in the build to
# assert what the job already asserts.

# Nothing to build: the script is run from a copy with no broker under
# it, so `cargo build` fails for a reason that is not a missing tool.
# This is the path the reviewer's message came from.
elsewhere="$(mktemp -d)"
mkdir -p "$elsewhere/scripts"
cp "$budgets" "$elsewhere/scripts/budgets.sh"
if "$elsewhere/scripts/budgets.sh" >/dev/null 2>&1; then
    echo "  NOT OK    a tree with nothing to build passed its budgets" >&2
    failures=$((failures + 1))
else
    status=$?
    if [ "$status" -eq 2 ]; then
        printf '  ok        a tree with nothing to build is an error (exit %d)\n' "$status"
    else
        printf '  NOT OK    a tree with nothing to build: wanted exit 2 and got %d\n' \
            "$status" >&2
        failures=$((failures + 1))
    fi
fi

echo
if [ "$failures" -gt 0 ]; then
    printf '  %d case(s) did not hold\n' "$failures" >&2
    exit 1
fi
echo "  every way of not measuring stops a build"
exit 0
