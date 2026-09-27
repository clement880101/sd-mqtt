//! Turning one broker's topic into the other's.
//!
//! Section 9.2: "Rewriting supports a prefix on either side, and for
//! the outbound direction a template that captures wildcard segments
//! from the matched filter and substitutes them by position, so
//! `sensors/+/temp` can be published remotely as `site-a/temp/{1}`.
//! Inbound messages need no such facility, because a bridge publishes
//! them into the local broker where any rule can act on them."
//!
//! # The prefixes
//!
//! An entry's `filter` carries neither. The local side is
//! `local_prefix` glued to the front of it and the remote side is
//! `remote_prefix` glued to the front of the same, so one filter
//! describes both namespaces and a rewrite is a prefix swap. Glued, not
//! joined by a separator: a prefix is a string, so an operator who
//! wants a level boundary writes the slash. [`super::topics::check`]
//! refuses a prefix that leaves something which is not a filter, which
//! is what catches the mistake.
//!
//! # The template, and the two cases the plan asked about
//!
//! **A capture the filter has not got** is refused when the
//! configuration is read, not substituted with nothing when a message
//! arrives. Both halves are known at that point — the filter says how
//! many wildcards it has and the template says which it wants — so an
//! operator who wrote `{2}` against a filter with one wildcard is told
//! immediately rather than left to find a topic with a hole in it.
//! [`super::topics::TopicError::NoSuchCapture`] is that refusal.
//!
//! **A multi-level wildcard captures several segments, not one**, and
//! it captures them with their separators intact: `sensors/#` matching
//! `sensors/kitchen/temp` gives `{1}` the value `kitchen/temp`. It can
//! also capture *nothing*, because `a/#` matches `a`, and then `{1}` is
//! the empty string. That is not an error either: an empty topic level
//! is a legal topic level, and `site/{1}` becomes `site/`, which is a
//! topic a subscriber to `site/+` receives. What is refused is a
//! rewrite that produces something which is not a topic at all — the
//! empty string, or a name the codec would not encode — and that is
//! [`Rewritten::NotATopic`], because convention 1 says the writer
//! refuses what the reader rejects and the reader here is the remote.

use mqtt_codec::{Limits, QoS};

use crate::rule::{BridgeTopic, Direction};

use super::topics;

/// One piece of a compiled template.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Piece {
    /// Text that appears as written.
    Literal(String),
    /// The capture with this one-based number.
    Capture(usize),
}

/// What a rewrite produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rewritten {
    /// The topic to publish to on the other side.
    Topic(String),
    /// This entry does not carry this topic.
    ///
    /// An ordinary answer rather than a failure: a bridge with four
    /// entries asks all four about every message, and three of them
    /// usually say this.
    NoMatch,
    /// The rewrite produced something that is not a topic.
    ///
    /// Reachable only through a template, and only through captures
    /// that came out empty — a multi-level capture that matched no
    /// levels at all, in a template that is nothing else. Reported
    /// rather than sent, because the remote's decoder would refuse it
    /// and convention 1 says this end refuses it first.
    NotATopic {
        /// What the rewrite produced.
        produced: String,
        /// Why it is not a topic.
        why: String,
    },
}

/// One topic entry, compiled.
///
/// Built once when the bridge starts rather than per message: the
/// filters, the template and the level are all fixed by the
/// configuration, and a bridge carrying a thousand messages a second
/// should not be splitting the same strings a thousand times.
#[derive(Debug, Clone)]
pub struct Rewrite {
    /// Which way this entry moves messages.
    direction: Direction,
    /// The level it moves them at.
    qos: QoS,
    /// The local prefix, as written.
    local_prefix: String,
    /// The remote prefix, as written.
    remote_prefix: String,
    /// The bare filter, split into levels once.
    filter: Vec<String>,
    /// What an outbound message is fired by.
    local_filter: String,
    /// What an inbound entry subscribes to.
    remote_filter: String,
    /// The compiled template, for an outbound entry that has one.
    template: Option<Vec<Piece>>,
}

impl Rewrite {
    /// Compile one topic entry.
    ///
    /// Takes an entry [`super::topics::check`] has accepted. Nothing
    /// here refuses anything, because everything refusable was refused
    /// when the configuration was read; a template that got here has
    /// paired braces and no capture out of range.
    pub fn new(topic: &BridgeTopic) -> Self {
        Self {
            direction: topic.direction,
            qos: topic.qos,
            local_prefix: topic.local_prefix.clone(),
            remote_prefix: topic.remote_prefix.clone(),
            filter: topic.filter.split('/').map(str::to_owned).collect(),
            local_filter: topics::local_filter(topic),
            remote_filter: topics::subscribes_to(topic),
            template: topic.template.as_deref().map(compile),
        }
    }

    /// Which way this entry moves messages.
    pub fn direction(&self) -> Direction {
        self.direction
    }

    /// The level it moves them at.
    pub fn qos(&self) -> QoS {
        self.qos
    }

    /// The local filter an outbound entry fires on.
    pub fn local_filter(&self) -> &str {
        &self.local_filter
    }

    /// The remote filter an inbound entry subscribes with.
    pub fn remote_filter(&self) -> &str {
        &self.remote_filter
    }

    /// A local topic, as the remote should see it.
    ///
    /// [`Rewritten::NoMatch`] when this entry does not send out, or
    /// does not carry this topic.
    pub fn outbound(&self, local: &str) -> Rewritten {
        if !self.direction.outbound() {
            return Rewritten::NoMatch;
        }
        let Some(rest) = local.strip_prefix(self.local_prefix.as_str()) else {
            return Rewritten::NoMatch;
        };
        let Some(captured) = self.capture(rest) else {
            return Rewritten::NoMatch;
        };

        let produced = match &self.template {
            Some(template) => expand(template, &captured),
            None => format!("{}{rest}", self.remote_prefix),
        };
        checked(produced)
    }

    /// A remote topic, as this broker should see it.
    ///
    /// The prefix swap and nothing else. Section 9.2: an inbound
    /// message is published into the local broker, where any rule can
    /// act on it, so there is nothing a template here would add that a
    /// rule cannot do better.
    pub fn inbound(&self, remote: &str) -> Rewritten {
        if !self.direction.inbound() {
            return Rewritten::NoMatch;
        }
        let Some(rest) = remote.strip_prefix(self.remote_prefix.as_str()) else {
            return Rewritten::NoMatch;
        };
        if self.capture(rest).is_none() {
            return Rewritten::NoMatch;
        }
        checked(format!("{}{rest}", self.local_prefix))
    }

    /// Match the bare filter against `rest`, collecting what each
    /// wildcard stood for.
    ///
    /// `None` when it does not match. The captures come back in the
    /// order the wildcards appear, so `{n}` is `captured[n - 1]`.
    ///
    /// A multi-level wildcard takes **every level that remains, with
    /// its separators**, and takes them even when there are none:
    /// `a/#` matches `a`, and the capture is then the empty string.
    fn capture(&self, rest: &str) -> Option<Vec<String>> {
        let levels: Vec<&str> = rest.split('/').collect();
        let mut captured = Vec::new();
        let mut at = 0usize;
        for (index, level) in self.filter.iter().enumerate() {
            match level.as_str() {
                "#" => {
                    // Everything that is left, as it was written. An
                    // `a/#` that matched exactly `a` arrives here with
                    // `at` one past the end, which is no levels and so
                    // the empty capture.
                    captured.push(
                        levels
                            .get(at..)
                            .map_or(String::new(), |rest| rest.join("/")),
                    );
                    return Some(captured);
                }
                "+" => {
                    captured.push((*levels.get(at)?).to_owned());
                    at += 1;
                }
                literal => {
                    if levels.get(at)? != &literal {
                        return None;
                    }
                    at += 1;
                }
            }
            // A filter with `#` in the middle is not a filter, and
            // `check` refused it, so the loop only ever ends here.
            let _ = index;
        }
        (at == levels.len()).then_some(captured)
    }
}

/// A produced topic, or why it is not one.
fn checked(produced: String) -> Rewritten {
    match mqtt_codec::topic::validate_name(&produced, &Limits::default()) {
        Ok(()) => Rewritten::Topic(produced),
        Err(error) => Rewritten::NotATopic {
            why: error.to_string(),
            produced,
        },
    }
}

/// Split a template into literals and captures.
///
/// Textual rather than per level, because a capture is allowed inside a
/// level: `site/{1}-temp` is a template and substitutes into the middle
/// of a level. A brace that does not close, or that holds something
/// which is not a capture number, cannot reach here —
/// [`super::topics::check`] refuses it — and is kept as a literal if it
/// ever does, which produces a visibly wrong topic rather than a
/// silently plausible one.
fn compile(template: &str) -> Vec<Piece> {
    let mut pieces = Vec::new();
    let mut literal = String::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else {
            break;
        };
        match after[..close].parse::<usize>().ok().filter(|n| *n > 0) {
            Some(number) => {
                literal.push_str(&rest[..open]);
                if !literal.is_empty() {
                    pieces.push(Piece::Literal(std::mem::take(&mut literal)));
                }
                pieces.push(Piece::Capture(number));
            }
            None => literal.push_str(&rest[..open + 1 + close + 1]),
        }
        rest = &after[close + 1..];
    }
    literal.push_str(rest);
    if !literal.is_empty() {
        pieces.push(Piece::Literal(literal));
    }
    pieces
}

/// Substitute the captures into a compiled template.
fn expand(template: &[Piece], captured: &[String]) -> String {
    let mut out = String::new();
    for piece in template {
        match piece {
            Piece::Literal(text) => out.push_str(text),
            // Out of range cannot happen: `check` compares the
            // template's captures against the filter's wildcards when
            // the configuration is read. The empty string if it ever
            // does, which makes the hole visible in the topic rather
            // than taking the bridge down.
            Piece::Capture(number) => {
                out.push_str(captured.get(number - 1).map_or("", String::as_str));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rule::BridgeTopic;

    fn entry(filter: &str, direction: Direction) -> BridgeTopic {
        BridgeTopic {
            filter: filter.to_owned(),
            direction,
            ..BridgeTopic::default()
        }
    }

    fn topic(rewritten: Rewritten) -> String {
        match rewritten {
            Rewritten::Topic(topic) => topic,
            other => panic!("expected a topic, got {other:?}"),
        }
    }

    #[test]
    fn a_prefix_on_either_side_is_swapped() {
        let mut entry = entry("sensors/#", Direction::Both);
        entry.local_prefix = "site-a/".to_owned();
        entry.remote_prefix = "upstream/a/".to_owned();
        let rewrite = Rewrite::new(&entry);

        assert_eq!(
            topic(rewrite.outbound("site-a/sensors/kitchen/temp")),
            "upstream/a/sensors/kitchen/temp"
        );
        assert_eq!(
            topic(rewrite.inbound("upstream/a/sensors/kitchen/temp")),
            "site-a/sensors/kitchen/temp"
        );
        assert_eq!(
            rewrite.outbound("elsewhere/sensors/kitchen/temp"),
            Rewritten::NoMatch,
            "the local prefix is not there"
        );
        assert_eq!(
            rewrite.outbound("site-a/other/kitchen/temp"),
            Rewritten::NoMatch,
            "the prefix is there and the filter does not match"
        );
    }

    /// Section 9.2's own example.
    #[test]
    fn a_template_substitutes_a_captured_segment_by_position() {
        let mut entry = entry("sensors/+/temp", Direction::Out);
        entry.template = Some("site-a/temp/{1}".to_owned());
        let rewrite = Rewrite::new(&entry);

        assert_eq!(
            topic(rewrite.outbound("sensors/kitchen/temp")),
            "site-a/temp/kitchen"
        );
        assert_eq!(
            rewrite.outbound("sensors/kitchen/humidity"),
            Rewritten::NoMatch
        );
    }

    #[test]
    fn captures_are_numbered_in_the_order_the_wildcards_appear() {
        let mut entry = entry("+/sensors/+/temp", Direction::Out);
        entry.template = Some("{2}/from/{1}".to_owned());
        let rewrite = Rewrite::new(&entry);
        assert_eq!(
            topic(rewrite.outbound("site-a/sensors/kitchen/temp")),
            "kitchen/from/site-a"
        );
    }

    /// The case the plan asked about: a multi-level wildcard captures
    /// several segments rather than one, and it keeps their
    /// separators.
    #[test]
    fn a_multi_level_capture_takes_every_segment_that_remains() {
        let mut entry = entry("sensors/#", Direction::Out);
        entry.template = Some("site-a/{1}".to_owned());
        let rewrite = Rewrite::new(&entry);

        assert_eq!(
            topic(rewrite.outbound("sensors/kitchen/temp")),
            "site-a/kitchen/temp",
            "two segments, with the separator between them"
        );
        assert_eq!(topic(rewrite.outbound("sensors/one")), "site-a/one");
    }

    /// And it can capture nothing at all, because `a/#` matches `a`.
    /// An empty topic level is a legal topic level, so the result is a
    /// topic and is published, rather than being a failure.
    #[test]
    fn a_multi_level_capture_can_come_out_empty() {
        let mut entry = entry("sensors/#", Direction::Out);
        entry.template = Some("site-a/{1}".to_owned());
        let rewrite = Rewrite::new(&entry);
        assert_eq!(
            topic(rewrite.outbound("sensors")),
            "site-a/",
            "an empty last level, which a subscriber to site-a/+ receives"
        );
    }

    /// The one way a rewrite can produce something that is not a
    /// topic: every piece of the template came out empty.
    #[test]
    fn a_rewrite_that_produces_nothing_is_not_a_topic() {
        let mut entry = entry("#", Direction::Out);
        entry.template = Some("{1}".to_owned());
        let rewrite = Rewrite::new(&entry);

        assert_eq!(topic(rewrite.outbound("a/b")), "a/b");
        let produced = rewrite.outbound("");
        assert!(
            matches!(produced, Rewritten::NotATopic { .. }),
            "an empty name is not a topic, and the remote would refuse it; got {produced:?}"
        );
    }

    #[test]
    fn a_capture_can_sit_inside_a_level() {
        let mut entry = entry("sensors/+", Direction::Out);
        entry.template = Some("readings/{1}-latest".to_owned());
        let rewrite = Rewrite::new(&entry);
        assert_eq!(
            topic(rewrite.outbound("sensors/kitchen")),
            "readings/kitchen-latest"
        );
    }

    #[test]
    fn a_template_with_no_capture_is_a_constant_topic() {
        let mut entry = entry("sensors/+/temp", Direction::Out);
        entry.template = Some("aggregate/temp".to_owned());
        let rewrite = Rewrite::new(&entry);
        assert_eq!(
            topic(rewrite.outbound("sensors/kitchen/temp")),
            "aggregate/temp"
        );
        assert_eq!(
            topic(rewrite.outbound("sensors/hall/temp")),
            "aggregate/temp"
        );
    }

    #[test]
    fn an_entry_only_rewrites_in_the_direction_it_was_given() {
        let out = Rewrite::new(&entry("a/#", Direction::Out));
        assert_eq!(out.inbound("a/b"), Rewritten::NoMatch);
        assert_eq!(topic(out.outbound("a/b")), "a/b");

        let inbound = Rewrite::new(&entry("a/#", Direction::In));
        assert_eq!(inbound.outbound("a/b"), Rewritten::NoMatch);
        assert_eq!(topic(inbound.inbound("a/b")), "a/b");
    }

    /// Convention 5: the matching a rewrite does must agree with the
    /// matcher the workspace tests against the specification, or the
    /// bridge carries a set of topics nothing else in the process
    /// agrees is the set.
    #[test]
    fn what_a_rewrite_carries_is_what_the_reference_matcher_says_it_carries() {
        let filters = ["a/b", "a/+", "a/#", "#", "+", "a/+/c", "a//b", "+/b/#"];
        let names = [
            "a", "a/b", "a/c", "a/b/c", "a//b", "x/b", "x/b/c", "a/b/c/d", "",
        ];
        for filter in filters {
            let rewrite = Rewrite::new(&entry(filter, Direction::Out));
            for name in names {
                let carried = !matches!(rewrite.outbound(name), Rewritten::NoMatch);
                assert_eq!(
                    carried,
                    broker_core::topic::reference::matches(filter, name),
                    "{filter} against {name}"
                );
            }
        }
    }

    #[test]
    fn a_template_compiles_to_its_literals_and_captures() {
        assert_eq!(
            compile("site-a/temp/{1}"),
            vec![Piece::Literal("site-a/temp/".to_owned()), Piece::Capture(1)]
        );
        assert_eq!(compile("{1}"), vec![Piece::Capture(1)]);
        assert_eq!(
            compile("{2}/x/{1}"),
            vec![
                Piece::Capture(2),
                Piece::Literal("/x/".to_owned()),
                Piece::Capture(1),
            ]
        );
        assert_eq!(compile("plain"), vec![Piece::Literal("plain".to_owned())]);
    }
}
