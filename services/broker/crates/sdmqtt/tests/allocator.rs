//! Section 10.4: mimalloc is the global allocator on every non-Windows
//! target, enabled by default and removable with a cargo feature.
//!
//! # Proven by the allocator, not by a label
//!
//! A `#[global_allocator]` line deleted, or a feature that compiles the
//! crate in without installing it, both build and both pass any test
//! that reads a name the binary reports about itself. What cannot be
//! faked is mimalloc's own account of its work: asked through
//! `MIMALLOC_SHOW_STATS`, it prints its statistics as the process exits,
//! and a binary that allocated through it has allocations in them. The
//! workspace forbids `unsafe`, which rules out asking mimalloc about a
//! pointer directly; this asks the real binary instead, which is the
//! stronger test anyway.

use std::process::Command;

/// What mimalloc printed when the binary exited, if anything.
fn statistics() -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_sdmqtt"))
        .arg("--version")
        .env("MIMALLOC_SHOW_STATS", "1")
        .output()
        .expect("the binary runs");
    assert!(out.status.success(), "--version exits cleanly");
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// With the default features, the binary's allocations are mimalloc's.
#[cfg(all(feature = "mimalloc", not(windows)))]
#[test]
fn the_binary_allocates_through_mimalloc() {
    let printed = statistics();
    // `  committed :     7.8 MiB  ...`: the label, a colon, then the
    // peak as a number and a unit.
    let committed = printed
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(label, _)| label.trim() == "committed")
        .map(|(_, rest)| rest.split_whitespace().next().unwrap_or("0").to_owned())
        .unwrap_or_else(|| panic!("mimalloc printed no statistics: {printed:?}"));
    // Zero would be a mimalloc that was linked and initialised but never
    // asked for memory.
    let amount: f64 = committed.parse().unwrap_or(0.0);
    assert!(
        amount > 0.0,
        "mimalloc committed nothing, so nothing was allocated through it: {printed}"
    );
}

/// Without the feature, mimalloc is not in the binary at all.
#[cfg(not(feature = "mimalloc"))]
#[test]
fn without_the_feature_the_system_allocator_is_used() {
    let printed = statistics();
    assert!(
        !printed.contains("committed"),
        "mimalloc statistics from a build without it: {printed:?}"
    );
}
