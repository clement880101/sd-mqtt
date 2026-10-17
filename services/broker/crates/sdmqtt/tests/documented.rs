//! Every setting an operator can make is in the README.
//!
//! The configuration grows a variable or a section most milestones, and
//! the README is where an operator looks for one. A setting the code
//! reads and the README never names is a setting nobody uses, or worse,
//! one somebody finds by reading the source. So the two are compared
//! here, from the source rather than from a list: every `SDMQTT_*` name
//! `Config::apply_env` reads, and every top-level section `Config`
//! deserialises, must appear in the README — a variable by name, a
//! section as `[name]` or `[[name]]`.

use std::fs;
use std::path::PathBuf;

fn read(relative: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
    fs::read_to_string(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

/// Every `"SDMQTT_…"` literal in the configuration's source, outside
/// its tests.
///
/// Every literal and not only the argument of `lookup("…")`: the
/// strict flags are read from a table of names that is handed to
/// `lookup` one entry at a time, and a scan that keyed on the call
/// walked straight past them. The tests are left out because they
/// name variables that do not exist, on purpose, to check they are
/// refused.
fn variables(source: &str) -> Vec<String> {
    let outside_tests = source.split("#[cfg(test)]").next().unwrap_or(source);
    let mut found: Vec<String> = outside_tests
        .split("\"SDMQTT_")
        .skip(1)
        .filter_map(|rest| rest.split('"').next())
        .filter(|name| {
            name.chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        })
        .map(|name| format!("SDMQTT_{name}"))
        .collect();
    found.sort();
    found.dedup();
    found
}

/// The field names of `pub struct Config`, which are its sections.
fn sections(source: &str) -> Vec<String> {
    let body = source
        .split("pub struct Config {")
        .nth(1)
        .and_then(|rest| rest.split("\n}").next())
        .expect("the source declares pub struct Config");
    body.lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("pub "))
        .filter_map(|line| line.split(':').next())
        .map(str::to_owned)
        .collect()
}

#[test]
fn every_variable_and_section_is_in_the_readme() {
    let source = read("src/config.rs");
    let readme = read("../../../../README.md");
    let variables = variables(&source);
    let sections = sections(&source);
    assert!(
        variables.len() > 20,
        "found too few variables to trust: {variables:?}"
    );
    assert!(
        sections.len() >= 12,
        "found too few sections to trust: {sections:?}"
    );
    let mut missing: Vec<String> = variables
        .iter()
        .filter(|name| !readme.contains(name.as_str()))
        .cloned()
        .collect();
    missing.extend(
        sections
            .iter()
            .filter(|name| {
                !readme.contains(&format!("[{name}]")) && !readme.contains(&format!("[[{name}]]"))
            })
            .map(|name| format!("[{name}]")),
    );
    assert!(missing.is_empty(), "the README never names: {missing:?}");
}
