//! `sdmqtt licenses` prints what is in the binary.
//!
//! The unit tests in `sdmqtt-admin` assert what the embedded document
//! says. This asserts the thing they cannot: that a person who has
//! only the binary can get it out, by running the real executable and
//! reading its standard output.
//!
//! That distinction is the whole finding. The browser notices were
//! generated correctly and reached nobody, and the Rust notices did
//! not exist; in both cases what was missing was the last step, and a
//! test of the document rather than of the command would have been
//! green throughout.
//!
//! # This file is not the drift gate, and reads like it
//!
//! Said out loud here because this is where a reader meets it. What
//! is below compares the command's output against the embedded
//! constant it was compiled from, and then asserts that three
//! strings are in it. Both sides of that equality come from the same
//! bytes, so it cannot notice a stale document: a
//! `THIRD-PARTY-NOTICES.txt` that no longer matches the dependency
//! graph is embedded, printed and compared to itself, and every
//! assertion here passes.
//!
//! **The gate is `scripts/rust-notices.py --check`, and it runs in
//! CI.** It regenerates the file from the graph, fails on any
//! difference, and reads the release binary for crates the file
//! omits. No `cargo test` runs it — it needs `python3` and a release
//! binary, neither of which a test has — so a green suite locally
//! says nothing about whether the notices are current. Run it by
//! hand before changing a dependency:
//!
//! ```text
//! (cd services/broker && cargo build --release --all-features -p sdmqtt)
//! scripts/rust-notices.py --check
//! ```

use std::process::Command;

/// Run the binary and return what it wrote to standard output.
fn licenses() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_sdmqtt"))
        .arg("licenses")
        .output()
        .expect("the binary runs");
    assert!(
        output.status.success(),
        "`sdmqtt licenses` failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "it is a document on standard output and nothing else: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("the notices are text")
}

#[test]
fn the_binary_prints_the_notices_for_the_code_inside_it() {
    let printed = licenses();
    assert_eq!(
        printed,
        sdmqtt_admin::NOTICES,
        "the command prints the embedded document and not a summary of it"
    );
    // The two Apache-2.0 crates this branch links in. Named, because
    // "it printed something" is what an empty generator also does.
    assert!(printed.contains("sync_wrapper 1."), "{printed:.400}");
    assert!(printed.contains("ryu 1."), "{printed:.400}");
    assert!(
        printed.contains("Apache License"),
        "the text has to travel with the work, not only the identifier"
    );
}

/// It answers without starting anything, which is the point of it.
///
/// The environment here is one the broker **refuses**: a non-loopback
/// admin address with no token is rejected before a socket is opened.
/// So a `licenses` that went anywhere near startup would exit
/// non-zero with that refusal, and this would catch it. A user whose
/// deployment is misconfigured, or who turned the admin listener off
/// entirely, still receives the third-party code and must still be
/// able to read what it is.
#[test]
fn it_answers_without_starting_a_broker() {
    let output = Command::new(env!("CARGO_BIN_EXE_sdmqtt"))
        .arg("licenses")
        .env("SDMQTT_ADMIN_ADDRESS", "0.0.0.0:1884")
        .env_remove("SDMQTT_ADMIN_TOKEN")
        .output()
        .expect("the binary runs");
    assert!(
        output.status.success(),
        "a configuration the broker would refuse must not stop it printing \
         the notices: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("Apache License"),
        "and what it printed is the notices"
    );
}
