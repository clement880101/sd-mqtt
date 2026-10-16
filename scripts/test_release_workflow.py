#!/usr/bin/env python3
"""Tests for the shape of `.github/workflows/release.yml`.

    python3 scripts/test_release_workflow.py

The workflow runs once per release, on GitHub's machines, so a mistake
in its shape is found on the day of the release unless something finds
it first. Three were found by review and are held here:

- **nothing is published before every artifact has been run**: the
  image job needs both archive jobs, and the release job needs all
  three, so a failing archive leaves no image in the registry;
- **no runner label GitHub has retired** (macOS 13 went on 4 December
  2025), since a job with no runner never starts and the release waits
  on it forever;
- every archive job unpacks and runs what it packaged, and the image
  job tests the image before pushing it.
"""
import sys
from pathlib import Path

import yaml

WORKFLOW = Path(__file__).resolve().parent.parent / ".github/workflows/release.yml"
RETIRED = {"macos-12", "macos-13", "ubuntu-20.04"}
failures = 0


def expect(name, ok, detail=""):
    global failures
    print(f"  {'ok  ' if ok else 'FAIL'}  {name}{(': ' + str(detail)) if not ok else ''}")
    failures += 0 if ok else 1


jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]


def needs(job):
    value = jobs[job].get("needs", [])
    return {value} if isinstance(value, str) else set(value)


def runs(job):
    return " ".join(step.get("run", "") for step in jobs[job].get("steps", []))


expect("the image waits for both archive jobs", {"linux", "macos"} <= needs("image"), needs("image"))
expect("the release waits for the archives and the image", {"linux", "macos", "image", "version"} <= needs("release"), needs("release"))
labels = {jobs[job].get("runs-on") for job in jobs} | {
    entry.get("runner") for job in jobs for entry in jobs[job].get("strategy", {}).get("matrix", {}).get("include", [])
}
expect("no runner label GitHub has retired", not (labels & RETIRED), sorted(str(l) for l in labels & RETIRED))
for job in ("linux", "macos"):
    expect(f"the {job} job runs what it packaged", "package.sh" in runs(job) and "test_package.py" in runs(job), runs(job))
expect("the image is tested before it is pushed", "test_image.py" in runs("image"), runs("image"))

# The release job must download what it publishes and nothing else: the
# image action uploads a build record of its own, which cannot be
# extracted, and downloading every artifact failed the first release
# after the image had already been pushed.
downloads = [s for s in jobs["release"]["steps"] if "download-artifact" in s.get("uses", "")]
expect("the release job downloads artifacts", bool(downloads))
unnamed = [s for s in downloads if not ({"name", "pattern"} & set(s.get("with", {})))]
expect("every download names what it takes", not unnamed, unnamed)

# The image is built for two platforms at once from one Dockerfile, and
# both builders run on the build machine: a cache mount they share is
# written by two cargos at once. The first real release failed on
# exactly that ("failed to unpack package ... File exists"), after the
# dry run had happened to win the race.
import re

dockerfile = (WORKFLOW.parent.parent.parent / "services/broker/Dockerfile").read_text()
mounts = re.findall(r"--mount=type=cache,[^\s\\]+", dockerfile)
expect("the Dockerfile has cache mounts to check", bool(mounts), mounts)
unsafe = [m for m in mounts if "sharing=locked" not in m and "TARGETARCH" not in m]
expect("every cache mount is locked or per platform", not unsafe, unsafe)
sys.exit(1 if failures else 0)
