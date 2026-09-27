//! What a bridge may carry, and the configuration that is refused
//! before anything connects.
//!
//! Section 9.2 puts loop prevention in two layers and is explicit about
//! which comes first:
//!
//! > **That mechanism does not exist in MQTT 3.1.1**, which has no user
//! > properties, and a bridge to a 3.1.1 endpoint is exactly the bridge
//! > people build. So the marker is the second line of defence, not the
//! > first.
//!
//! The first line is here, and it runs when the configuration is read
//! rather than when a message arrives. That matters twice over: it
//! catches the loop before a message does, and it works on both
//! versions, where the marker works on one.
//!
//! # The two namespaces, and the one they are compared in
//!
//! A topic entry's `filter` carries neither prefix. The local topics an
//! outbound entry fires on are `local_prefix` followed by something
//! matching `filter`; the remote filter an inbound entry subscribes
//! with is `remote_prefix` followed by the same. One filter describes
//! two namespaces, which is the shape every bridge configuration people
//! already know uses.
//!
//! The two directions are only comparable in one namespace, and it is
//! the **remote's**:
//!
//! - What the bridge writes to the remote is [`publishes_to`] — the
//!   outbound entry's filter after its rewrite, which is the whole
//!   point of "intersect *after rewriting*".
//! - What the bridge reads from the remote is [`subscribes_to`].
//!
//! If those two share a single topic, everything this bridge sends it
//! will be given back, and that is the loop. [`check`] refuses it.
//!
//! # A string comparison would not do
//!
//! Two filters that are not equal still both match one topic.
//! `sensors/+/temp` and `sensors/kitchen/#` do not look alike and both
//! match `sensors/kitchen/temp`. So [`intersects`] is an algebra over
//! levels rather than a comparison of strings, and the test that
//! guards it uses exactly that pair.
//!
//! # `both` is refused, and that is the rule working
//!
//! Section 9.2 gives a topic entry a direction "of in, out or both", so
//! [`Direction::Both`] exists. A `both` entry **with no template** is
//! also a loop by construction: it publishes to
//! `remote_prefix + filter` and subscribes to the same set, whatever
//! the prefixes are, because a prefix swap is symmetric.
//!
//! That is not a special case in the code and there is no arm for it.
//! It falls out of the one rule, and it is worth saying out loud
//! because `topic # both` is the configuration people write first and
//! the storm it causes on a 3.1.1 link is the reason this refusal is a
//! refusal rather than a warning. What such an operator wants is two
//! entries whose remote namespaces do not overlap.
//!
//! A `both` entry **with** a template is another matter and is
//! accepted when it separates the two: a template rewrites only the
//! outbound half, so `sensors/+/temp` published as `site-a/temp/{1}`
//! and subscribed as `sensors/+/temp` do not meet. The direction is
//! not dead; it is only dead in the shape people write first.
//!
//! # What is **not** refused here, and where it is handled instead
//!
//! A message arriving from the remote and going straight back out to it
//! on some *other* remote topic is not a loop — nothing brings it round
//! again — but it is a bridge sending back what it received. That one
//! is the marker's, and the hop count's, because refusing it outright
//! would refuse the hub-and-spoke configurations that are the reason a
//! bridge publishes inbound messages into the local broker at all. See
//! [`crate::bridge::marker`] and [`crate::hops`].

use mqtt_codec::Limits;

use crate::rule::{BridgeTopic, MqttBridge};

/// What can be wrong with an `mqtt_bridge` rule's topics.
///
/// Found when the rule is read rather than on the first message: a
/// bridge that loops should stop the broker starting, said once with
/// the entries named, rather than becoming a storm an operator has to
/// diagnose from the far end.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TopicError {
    /// An entry's filter is not a topic filter.
    #[error("mqtt_bridge topic {index}: {filter:?} is not a topic filter: {why}")]
    NotAFilter {
        /// Which entry, counting from zero as the configuration lists
        /// them.
        index: usize,
        /// What was configured.
        filter: String,
        /// Why it is not one.
        why: String,
    },
    /// A prefix and a filter do not make a filter between them.
    ///
    /// A prefix is glued to the front of the filter as written, so a
    /// prefix that does not end on a level boundary can produce
    /// something like `site-+/temp`, where the wildcard no longer
    /// occupies a whole level. That is not a filter, and refusing it is
    /// convention 1: the remote's decoder rejects it, so this end must.
    #[error(
        "mqtt_bridge topic {index}: the {side} prefix {prefix:?} and the filter {filter:?} \
         make {joined:?}, which is not a topic filter: {why}. A prefix is glued to the \
         front of the filter as written, so it usually wants to end with '/'."
    )]
    PrefixedFilter {
        /// Which entry.
        index: usize,
        /// `"local"` or `"remote"`.
        side: &'static str,
        /// The prefix as configured.
        prefix: String,
        /// The filter as configured.
        filter: String,
        /// The two glued together.
        joined: String,
        /// Why that is not a filter.
        why: String,
    },
    /// An entry moves nothing, because its filter is empty.
    #[error(
        "mqtt_bridge topic {index} has no filter. An entry with nothing to match moves \
         nothing; remove it or give it one."
    )]
    NoFilter {
        /// Which entry.
        index: usize,
    },
    /// A template names a capture the filter cannot supply.
    ///
    /// Refused here rather than substituted with nothing at run time,
    /// because both halves are known when the configuration is read:
    /// the filter says how many wildcards it has, and the template says
    /// which it wants. An operator who wrote `{2}` against a filter
    /// with one wildcard has a belief about the rewrite that is wrong,
    /// and the moment to tell them is now.
    #[error(
        "mqtt_bridge topic {index}: the template {template:?} refers to capture {wanted}, \
         and the filter {filter:?} has {available} wildcard(s) to capture. Captures are \
         numbered from 1, in the order the wildcards appear."
    )]
    NoSuchCapture {
        /// Which entry.
        index: usize,
        /// The template as configured.
        template: String,
        /// The capture it asked for.
        wanted: usize,
        /// The filter it was asked of.
        filter: String,
        /// How many captures that filter offers.
        available: usize,
    },
    /// A template's braces do not pair up, or hold something that is
    /// not a capture number.
    #[error(
        "mqtt_bridge topic {index}: the template {template:?} is not a template: {why}. \
         A capture is written {{1}}, numbered from 1; a literal brace has no spelling, \
         so a template cannot contain one."
    )]
    BadTemplate {
        /// Which entry.
        index: usize,
        /// The template as configured.
        template: String,
        /// What is wrong with it.
        why: String,
    },
    /// A template was given to an entry that sends nothing out.
    ///
    /// Refused rather than ignored, on the same reasoning that refuses
    /// a trigger on an ingest rule: a template on an inbound entry is a
    /// rewrite that can never run, and an operator who wrote one
    /// believes something about where their messages land that is not
    /// true. Section 9.2 gives the template to the outbound direction
    /// and says why: an inbound message is published into the local
    /// broker, where any rule can act on it.
    #[error(
        "mqtt_bridge topic {index} has a template but never sends anything out. A \
         template rewrites an outbound topic; an inbound message is published into this \
         broker, where a rule can act on it. Remove the template or set direction = \"out\"."
    )]
    TemplateOnInbound {
        /// Which entry.
        index: usize,
    },
    /// Two entries make a loop through the remote broker.
    ///
    /// The refusal section 9.2 puts first. What the outbound entry
    /// publishes to the remote is what the inbound entry reads back.
    #[error(
        "mqtt_bridge loops: topic {out} publishes to the remote under {publishes:?} and \
         topic {inbound} subscribes to {subscribes:?}, and both match {witness:?}. \
         Everything this bridge sends it would be given straight back. MQTT 3.1.1 has no \
         user property to mark a message with, so this is refused rather than detected; \
         give the two directions remote namespaces that do not overlap."
    )]
    Loops {
        /// The outbound entry's index.
        out: usize,
        /// The inbound entry's index.
        inbound: usize,
        /// The remote filter the outbound entry publishes into.
        publishes: String,
        /// The remote filter the inbound entry subscribes to.
        subscribes: String,
        /// One topic both of them match, so the refusal can be checked
        /// by hand.
        witness: String,
    },
    /// Two bridge **rules** to one remote make a loop between them.
    ///
    /// The refusal above takes one bridge and is called once per rule,
    /// which sees nothing at all here: rule A publishes to the remote,
    /// rule B reads that back and publishes it into this broker, and
    /// what it publishes locally is what rule A carries out. Nothing
    /// in either rule's own entries is wrong.
    ///
    /// The marker closes it one turn later on MQTT 5 — the origin is
    /// this bridge's rather than this broker's, so A's mark is not B's
    /// and the message comes round once before A refuses it. **On
    /// 3.1.1 nothing closes it at all**, and 3.1.1 is the version
    /// people bridge to. So it is refused where the single-rule loop
    /// is refused: when the configuration is read.
    ///
    /// Boxed because it carries twice what any other refusal here
    /// does — two rules, two entries and a witness on each side — and
    /// an enum is as large as its largest variant on every `Result`
    /// this module returns.
    #[error(transparent)]
    LoopsAcrossRules(Box<AcrossLoop>),
    /// The state topic is one this bridge would forward.
    ///
    /// Section 9.2: "A bridge whose own state topic matches its
    /// outbound filter is refused for the same reason, because a bridge
    /// republishing its own liveness is a loop with a heartbeat."
    #[error(
        "mqtt_bridge state_topic {topic:?} matches topic {index}'s outbound filter \
         {filter:?}, so every time this bridge said it was up it would forward the fact \
         to the remote. A bridge republishing its own liveness is a loop with a \
         heartbeat; move the state topic outside what this bridge carries."
    )]
    StateTopicIsCarried {
        /// The state topic as configured.
        topic: String,
        /// Which entry would carry it.
        index: usize,
        /// That entry's local filter.
        filter: String,
    },
    /// The state topic is one this bridge would read back.
    ///
    /// The sibling of the refusal above, on the other side. The state
    /// topic is also the will topic sent to the remote, so an inbound
    /// entry that covers it subscribes this bridge to its own death
    /// notice.
    #[error(
        "mqtt_bridge state_topic {topic:?} is also the will topic on the remote, and it \
         matches topic {index}'s remote filter {filter:?}, so this bridge would subscribe \
         to its own liveness and republish it here. Move the state topic outside what \
         this bridge carries."
    )]
    StateTopicIsSubscribed {
        /// The state topic as configured.
        topic: String,
        /// Which entry would read it back.
        index: usize,
        /// That entry's remote filter.
        filter: String,
    },
    /// The state topic is not a topic a message can be published to.
    #[error(
        "mqtt_bridge.state_topic {topic:?} is not a topic a message can be published to: {why}"
    )]
    StateTopicNotATopic {
        /// What was configured.
        topic: String,
        /// Why it is not one.
        why: String,
    },
    /// The state topic is in the reserved namespace.
    #[error(
        "mqtt_bridge.state_topic {topic:?} is in the dollar-prefixed namespace the broker \
         reserves for itself, which is not publishable"
    )]
    StateTopicReserved {
        /// What was configured.
        topic: String,
    },
    /// A bridge that carries nothing.
    #[error(
        "an mqtt_bridge rule has no topic entries, so it would connect to the remote and \
         move nothing. Add a [[rules.mqtt_bridge.topics]] entry."
    )]
    NoTopics,
}

/// Two rules, one remote, and the cycle they close between them.
///
/// The payload of [`TopicError::LoopsAcrossRules`]; see
/// [`check_across`] for what is being refused and why it is refused
/// rather than detected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "mqtt_bridge rules {out_rule:?} and {in_rule:?} are both connected to {address} and \
     loop through it: {out_rule:?} topic {out} publishes to {publishes:?} there, \
     {in_rule:?} topic {inbound} subscribes to {subscribes:?}, and both match {witness:?}; \
     what {in_rule:?} then publishes here matches {local:?}, which is what {out_rule:?} \
     carries out, and both match {local_witness:?}. MQTT 3.1.1 has no user property to \
     mark a message with, so this is refused rather than detected; give the two rules \
     remote namespaces that do not overlap, or local ones."
)]
pub struct AcrossLoop {
    /// The rule whose outbound entry starts the cycle.
    pub out_rule: String,
    /// Which of its entries.
    pub out: usize,
    /// The rule that reads it back off the remote.
    pub in_rule: String,
    /// Which of its entries.
    pub inbound: usize,
    /// The remote both are connected to.
    pub address: String,
    /// The remote filter the outbound entry publishes into.
    pub publishes: String,
    /// The remote filter the inbound entry subscribes to.
    pub subscribes: String,
    /// One remote topic both of them match.
    pub witness: String,
    /// The local filter the inbound entry publishes into, which is
    /// also the one the outbound entry fires on.
    pub local: String,
    /// One local topic both of those match.
    pub local_witness: String,
}

impl TopicError {
    /// Which field of an `mqtt_bridge` action this refusal is about.
    pub fn field(&self) -> &'static str {
        match self {
            Self::StateTopicIsCarried { .. }
            | Self::StateTopicIsSubscribed { .. }
            | Self::StateTopicNotATopic { .. }
            | Self::StateTopicReserved { .. } => "mqtt_bridge.state_topic",
            // A pair of rules, and the entries are what an operator
            // changes to fix it.
            Self::LoopsAcrossRules(_) => "mqtt_bridge.topics",
            // Everything else is about an entry, including the two
            // that are about a *pair* of entries: a form marks the
            // table, because neither of the two is the wrong one on
            // its own.
            _ => "mqtt_bridge.topics",
        }
    }
}

/// What one wildcard in a filter can capture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capture {
    /// A single-level wildcard, which captures exactly one level.
    One,
    /// A multi-level wildcard, which captures every level that remains
    /// — and that can be none of them, because `a/#` matches `a`.
    Many,
}

/// What each wildcard in `filter` would capture, in the order they
/// appear.
///
/// Captures are numbered from one, so `captures(f)[n - 1]` is what
/// `{n}` refers to.
pub fn captures(filter: &str) -> Vec<Capture> {
    filter
        .split('/')
        .filter_map(|level| match level {
            "+" => Some(Capture::One),
            "#" => Some(Capture::Many),
            _ => None,
        })
        .collect()
}

/// Whether two topic filters share at least one topic, and one if they
/// do.
///
/// An algebra over levels, not a comparison of strings, because two
/// filters that are not equal still both match one topic.
///
/// The witness is returned rather than a bare yes, so that the refusal
/// can name a topic an operator can check by hand: "these two overlap"
/// is an assertion, and "these two both match `sensors/kitchen/temp`"
/// is a fact.
pub fn intersection(a: &str, b: &str) -> Option<String> {
    // [MQTT-4.7.2-1]: a filter beginning with a wildcard does not match
    // a topic whose first level begins with a dollar sign. So two
    // filters that would otherwise meet only inside that namespace do
    // not meet at all. The reference matcher this workspace tests
    // against the specification's examples keeps the same rule.
    let wild_first = |filter: &str| matches!(filter.split('/').next(), Some("+") | Some("#"));
    let dollar_first = |filter: &str| {
        filter
            .split('/')
            .next()
            .is_some_and(|level| level.starts_with('$'))
    };
    if (wild_first(a) && dollar_first(b)) || (wild_first(b) && dollar_first(a)) {
        return None;
    }

    let left: Vec<&str> = a.split('/').collect();
    let right: Vec<&str> = b.split('/').collect();
    let mut witness = Vec::new();
    walk(&left, &right, &mut witness).then(|| witness.join("/"))
}

/// Whether two topic filters share at least one topic.
pub fn intersects(a: &str, b: &str) -> bool {
    intersection(a, b).is_some()
}

/// The level a wildcard stands for in a witness topic.
///
/// Any single level would do. A word rather than a letter so that a
/// witness reads as something somebody chose: `sensors/x/temp` looks
/// like a typo and `sensors/any/temp` looks like an example.
const ANY_LEVEL: &str = "any";

/// Whether the two remaining filters meet, building a topic they both
/// match as it goes.
fn walk(a: &[&str], b: &[&str], witness: &mut Vec<String>) -> bool {
    match (a.first(), b.first()) {
        // A multi-level wildcard absorbs everything that remains,
        // including nothing at all, which is why `a/#` matches `a`. So
        // whatever the other side still wants, it can have — and what
        // it still wants is the witness.
        (Some(&"#"), _) => {
            for level in b {
                witness.push(concrete(level));
            }
            true
        }
        (_, Some(&"#")) => {
            for level in a {
                witness.push(concrete(level));
            }
            true
        }
        (Some(&left), Some(&right)) => {
            let level = match (left, right) {
                ("+", "+") => ANY_LEVEL.to_owned(),
                ("+", other) | (other, "+") => other.to_owned(),
                (one, two) if one == two => one.to_owned(),
                _ => return false,
            };
            witness.push(level);
            walk(&a[1..], &b[1..], witness)
        }
        (None, None) => true,
        // One filter has run out and the other still wants a level.
        // Only `#` could have absorbed that, and it was handled above.
        (Some(_), None) | (None, Some(_)) => false,
    }
}

/// One level standing in for whatever a wildcard would have matched.
fn concrete(level: &str) -> String {
    match level {
        "+" | "#" => ANY_LEVEL.to_owned(),
        other => other.to_owned(),
    }
}

/// The local filter an entry fires on, and publishes into.
///
/// One function for both, because they are one set: an outbound entry
/// fires on `local_prefix + filter`, and an inbound entry publishes
/// locally at `local_prefix` followed by whatever the remote topic had
/// after `remote_prefix`, which is something matching `filter`.
pub fn local_filter(topic: &BridgeTopic) -> String {
    format!("{}{}", topic.local_prefix, topic.filter)
}

/// The remote filter an inbound entry subscribes to.
pub fn subscribes_to(topic: &BridgeTopic) -> String {
    format!("{}{}", topic.remote_prefix, topic.filter)
}

/// The remote filter an outbound entry publishes into, after its
/// rewrite.
///
/// The "after rewriting" half of section 9.2's refusal. Without a
/// template the rewrite is the prefix swap, so the answer is
/// [`subscribes_to`]'s. With one it is the template with each capture
/// replaced by the wildcard that would fill it.
///
/// **It over-approximates, deliberately.** A multi-level capture can
/// span any number of levels, and a filter can only say `#` at the end,
/// so a template that puts such a capture in the middle is answered
/// with everything from that level on. The error of an
/// over-approximation is a configuration refused that would have been
/// safe; the error of the other kind is a loop that starts in
/// production.
pub fn publishes_to(topic: &BridgeTopic) -> String {
    let Some(template) = &topic.template else {
        return subscribes_to(topic);
    };
    let kinds = captures(&topic.filter);
    let mut levels: Vec<String> = Vec::new();
    for level in template.split('/') {
        match level_capture(level, &kinds) {
            // A level that swallowed a multi-level capture, or that
            // follows one: nothing after it can be pinned down.
            Some(Capture::Many) => {
                levels.push("#".to_owned());
                return levels.join("/");
            }
            Some(Capture::One) => levels.push("+".to_owned()),
            None => levels.push(level.to_owned()),
        }
    }
    levels.join("/")
}

/// The widest capture this template level holds, if it holds one.
fn level_capture(level: &str, kinds: &[Capture]) -> Option<Capture> {
    let mut widest = None;
    for wanted in placeholders(level).flatten() {
        match kinds.get(wanted - 1) {
            Some(Capture::Many) => return Some(Capture::Many),
            // A capture the filter cannot supply is refused by
            // [`check`] before anything gets here. Treated as the
            // widest thing it could be if it ever did, so that a gap in
            // the checking becomes an over-approximation rather than a
            // loop.
            None => return Some(Capture::Many),
            Some(Capture::One) => widest = Some(Capture::One),
        }
    }
    widest
}

/// Every `{n}` in one template level, in order.
///
/// A `None` item is a brace that does not open and close around a
/// number; [`check_template`] turns one into a refusal and everything
/// else treats it as the widest capture there could be.
fn placeholders(level: &str) -> impl Iterator<Item = Option<usize>> + '_ {
    let mut rest = level;
    std::iter::from_fn(move || {
        let open = rest.find('{')?;
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else {
            rest = "";
            return Some(None);
        };
        let inside = &after[..close];
        rest = &after[close + 1..];
        Some(inside.parse::<usize>().ok().filter(|n| *n > 0))
    })
}

/// Check every topic entry and the state topic, and refuse a bridge
/// that would loop.
///
/// **Before anything connects**, which is the whole point: a loop
/// caught here costs a line in a log, and one caught in production
/// costs whatever the remote does under a message storm.
///
/// # Errors
///
/// [`TopicError`], naming the entries involved and, for a loop, a topic
/// that both sides match.
pub fn check(action: &MqttBridge) -> Result<(), TopicError> {
    if action.topics.is_empty() {
        return Err(TopicError::NoTopics);
    }

    let limits = Limits::default();
    for (index, topic) in action.topics.iter().enumerate() {
        if topic.filter.is_empty() {
            return Err(TopicError::NoFilter { index });
        }
        mqtt_codec::topic::validate_filter(&topic.filter, &limits).map_err(|error| {
            TopicError::NotAFilter {
                index,
                filter: topic.filter.clone(),
                why: error.to_string(),
            }
        })?;
        for (side, prefix) in [
            ("local", &topic.local_prefix),
            ("remote", &topic.remote_prefix),
        ] {
            let joined = format!("{prefix}{}", topic.filter);
            mqtt_codec::topic::validate_filter(&joined, &limits).map_err(|error| {
                TopicError::PrefixedFilter {
                    index,
                    side,
                    prefix: prefix.clone(),
                    filter: topic.filter.clone(),
                    joined: joined.clone(),
                    why: error.to_string(),
                }
            })?;
        }
        if topic.template.is_some() && !topic.direction.outbound() {
            return Err(TopicError::TemplateOnInbound { index });
        }
        if let Some(template) = &topic.template {
            check_template(index, template, &topic.filter)?;
        }
    }

    check_state_topic(action)?;

    // The refusal section 9.2 puts first, and the only one that has to
    // look at two entries at once. Every outbound entry against every
    // inbound one, including an entry against itself: a `both` entry is
    // an outbound entry and an inbound entry, and it is the pair that
    // loops.
    for (out, sender) in action.topics.iter().enumerate() {
        if !sender.direction.outbound() {
            continue;
        }
        let publishes = publishes_to(sender);
        for (inbound, reader) in action.topics.iter().enumerate() {
            if !reader.direction.inbound() {
                continue;
            }
            let subscribes = subscribes_to(reader);
            if let Some(witness) = intersection(&publishes, &subscribes) {
                return Err(TopicError::Loops {
                    out,
                    inbound,
                    publishes,
                    subscribes,
                    witness,
                });
            }
        }
    }

    Ok(())
}

/// Refuse a set of bridges in which **two rules** loop through one
/// remote.
///
/// [`check`] takes one bridge and is called once per rule, so it can
/// only see a rule feeding itself. The cycle it cannot see is two of
/// them: rule A carries a local topic out to the remote, rule B is
/// subscribed to that topic on the same remote and publishes what it
/// finds back into this broker, and what B publishes here is what A
/// carries out. Neither rule is wrong on its own, and the readme's own
/// worked example is a two-entry bridge — so "two rules to one remote"
/// is the shape an operator generalises to.
///
/// # Why this is refused rather than detected
///
/// The same sentence section 9.2 gives for the single-rule case. The
/// marker is per **bridge** rather than per broker, deliberately, so
/// that two bridges to two remotes do not read each other's marks; the
/// price is that a message going out through A and coming back through
/// B is not marked with B's origin, so it reaches this broker once and
/// is only refused when A sees its own mark on the way out again. On
/// MQTT 5 that is one wasted turn. On **3.1.1 there is no user
/// property at all**, so nothing closes it and the two rules storm the
/// remote between them.
///
/// # Both halves, and why not just the remote one
///
/// A cycle needs the local hop as well. Rule A publishing to the same
/// remote filter rule B subscribes to is a mirror, not a loop, when B
/// delivers it somewhere locally that A does not carry — which is an
/// ordinary hub-and-spoke configuration and one this must not refuse.
/// So both are required: the remote filters must meet, **and** what B
/// publishes locally must be something A fires on. Each is checked
/// with the same algebra [`check`] uses, and each refusal names a
/// witness on its own side so an operator can check it by hand.
///
/// For a single rule the local half is always true — the inbound and
/// outbound rewrites of one entry are inverses — which is why [`check`]
/// does not need it and this does.
///
/// # One remote means one address
///
/// Compared after parsing, so `mqtt://host` and `mqtt://host:1883` are
/// one remote. Two spellings that resolve to one broker — a name and
/// its address, two names for one host — are **not** caught, and
/// cannot be from here: what a name resolves to is not known when a
/// configuration is read and may not be the same thing twice. That is
/// the gap the marker and the hop count are still for.
///
/// # Errors
///
/// [`TopicError::LoopsAcrossRules`], naming both rules, both entries
/// and a witness on each side.
pub fn check_across<'a>(
    bridges: impl IntoIterator<Item = (&'a str, &'a MqttBridge)>,
) -> Result<(), TopicError> {
    let bridges: Vec<(&str, &MqttBridge)> = bridges.into_iter().collect();
    for (first, (sender_id, sender)) in bridges.iter().enumerate() {
        for (second, (reader_id, reader)) in bridges.iter().enumerate() {
            // A rule against itself is [`check`]'s, and it has
            // already run.
            if first == second || !one_remote(sender, reader) {
                continue;
            }
            check_between(sender_id, sender, reader_id, reader)?;
        }
    }
    Ok(())
}

/// Whether two bridges are connected to the same remote broker.
fn one_remote(a: &MqttBridge, b: &MqttBridge) -> bool {
    match (
        super::supervisor::address(&a.address),
        super::supervisor::address(&b.address),
    ) {
        (Ok(a), Ok(b)) => a.port == b.port && a.host.eq_ignore_ascii_case(&b.host),
        // One of them is not an address at all, which
        // `supervisor::address` refuses on its own. Compared as written
        // so that this says nothing new about it either way.
        _ => a.address == b.address,
    }
}

/// Refuse `out`'s outbound entries feeding `inbound`'s inbound ones
/// and coming back round.
fn check_between(
    out_rule: &str,
    out: &MqttBridge,
    in_rule: &str,
    inbound: &MqttBridge,
) -> Result<(), TopicError> {
    for (sending, sender) in out.topics.iter().enumerate() {
        if !sender.direction.outbound() {
            continue;
        }
        let publishes = publishes_to(sender);
        let fires_on = local_filter(sender);
        for (reading, reader) in inbound.topics.iter().enumerate() {
            if !reader.direction.inbound() {
                continue;
            }
            let subscribes = subscribes_to(reader);
            let Some(witness) = intersection(&publishes, &subscribes) else {
                continue;
            };
            // And back: what the inbound entry publishes into this
            // broker against what the outbound entry carries out of it.
            let delivers = local_filter(reader);
            let Some(local_witness) = intersection(&delivers, &fires_on) else {
                continue;
            };
            return Err(TopicError::LoopsAcrossRules(Box::new(AcrossLoop {
                out_rule: out_rule.to_owned(),
                out: sending,
                in_rule: in_rule.to_owned(),
                inbound: reading,
                address: out.address.clone(),
                publishes,
                subscribes,
                witness,
                local: delivers,
                local_witness,
            })));
        }
    }
    Ok(())
}

/// Refuse a state topic this bridge would carry.
fn check_state_topic(action: &MqttBridge) -> Result<(), TopicError> {
    if action.state_topic.is_empty() {
        // No state topic is a bridge that reports nothing, which is a
        // thinner deployment rather than a broken one: section 9.2 asks
        // for the topic, and an operator who did not configure one has
        // said they do not want it. Nothing to refuse.
        return Ok(());
    }

    mqtt_codec::topic::validate_name(&action.state_topic, &Limits::default()).map_err(|error| {
        TopicError::StateTopicNotATopic {
            topic: action.state_topic.clone(),
            why: error.to_string(),
        }
    })?;
    if action.state_topic.starts_with('$') {
        return Err(TopicError::StateTopicReserved {
            topic: action.state_topic.clone(),
        });
    }

    for (index, topic) in action.topics.iter().enumerate() {
        if topic.direction.outbound() {
            let filter = local_filter(topic);
            if broker_core::topic::reference::matches(&filter, &action.state_topic) {
                return Err(TopicError::StateTopicIsCarried {
                    topic: action.state_topic.clone(),
                    index,
                    filter,
                });
            }
        }
        if topic.direction.inbound() {
            let filter = subscribes_to(topic);
            if broker_core::topic::reference::matches(&filter, &action.state_topic) {
                return Err(TopicError::StateTopicIsSubscribed {
                    topic: action.state_topic.clone(),
                    index,
                    filter,
                });
            }
        }
    }
    Ok(())
}

/// Refuse a template that is not one, or that asks for a capture the
/// filter has not got.
fn check_template(index: usize, template: &str, filter: &str) -> Result<(), TopicError> {
    let bad = |why: &str| TopicError::BadTemplate {
        index,
        template: template.to_owned(),
        why: why.to_owned(),
    };
    if template.is_empty() {
        return Err(bad("it is empty, and an empty topic is not a topic"));
    }
    // A template produces a topic name, and a topic name has no
    // wildcards in it. Convention 1: the remote's decoder rejects a
    // publish to a filter, so this end refuses to build one.
    if template.contains(['+', '#']) {
        return Err(bad(
            "it contains '+' or '#'. A template makes a topic name to publish to, and a \
             topic name has no wildcards; a capture is written {1}",
        ));
    }
    if template.contains('\u{0}') {
        return Err(bad("it contains a NUL, which a topic may not"));
    }

    let available = captures(filter).len();
    let mut opens = 0usize;
    for level in template.split('/') {
        opens += level.matches('{').count();
        if level.matches('}').count() != level.matches('{').count() {
            return Err(bad("its braces do not pair up"));
        }
        for wanted in placeholders(level) {
            let Some(wanted) = wanted else {
                return Err(bad(
                    "a brace holds something that is not a capture number. Captures are \
                     numbered from 1",
                ));
            };
            if wanted > available {
                return Err(TopicError::NoSuchCapture {
                    index,
                    template: template.to_owned(),
                    wanted,
                    filter: filter.to_owned(),
                    available,
                });
            }
        }
    }
    if opens == 0 {
        // A template with no capture in it is a constant topic, which
        // is a thing an operator may genuinely want: every message on
        // this filter, published remotely at one name. Not refused,
        // and named here so that a reader does not add the refusal.
        return Ok(());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rule::{Direction, MqttBridge};
    use mqtt_codec::QoS;

    fn topic(filter: &str, direction: Direction) -> BridgeTopic {
        BridgeTopic {
            filter: filter.to_owned(),
            direction,
            qos: QoS::AtLeastOnce,
            ..BridgeTopic::default()
        }
    }

    fn bridge(topics: Vec<BridgeTopic>) -> MqttBridge {
        MqttBridge {
            address: "remote.invalid:1883".to_owned(),
            client_id: "bridge".to_owned(),
            topics,
            ..MqttBridge::default()
        }
    }

    /// The case the whole algebra exists for. These two filters are not
    /// equal, do not look alike, and both match one topic; a string
    /// comparison lets the loop straight through.
    #[test]
    fn two_filters_that_overlap_only_through_a_wildcard_intersect() {
        assert_eq!(
            intersection("sensors/+/temp", "sensors/kitchen/#"),
            Some("sensors/kitchen/temp".to_owned()),
            "the witness is a topic both of them match"
        );
        assert_ne!(
            "sensors/+/temp", "sensors/kitchen/#",
            "and they are not equal, which is what a string comparison would ask"
        );
    }

    #[test]
    fn a_bridge_that_loops_only_through_a_wildcard_is_refused() {
        // The outbound entry publishes everything under
        // `sensors/+/temp` to the remote; the inbound one subscribes to
        // `sensors/kitchen/#`. Neither filter equals the other and the
        // remote would hand `sensors/kitchen/temp` straight back.
        let action = bridge(vec![
            topic("sensors/+/temp", Direction::Out),
            topic("sensors/kitchen/#", Direction::In),
        ]);
        let error = check(&action).expect_err("this loops");
        let TopicError::Loops {
            out,
            inbound,
            witness,
            ..
        } = error
        else {
            panic!("expected a loop, got {error:?}");
        };
        assert_eq!((out, inbound), (0, 1));
        assert_eq!(witness, "sensors/kitchen/temp");
    }

    /// The refusal is about the remote namespace, so a rewrite that
    /// separates the two is accepted even though the filters overlap.
    #[test]
    fn the_same_pair_is_accepted_once_the_rewrite_separates_them() {
        let mut out = topic("sensors/+/temp", Direction::Out);
        out.remote_prefix = "from-here/".to_owned();
        let action = bridge(vec![out, topic("sensors/kitchen/#", Direction::In)]);
        check(&action).expect("the remote namespaces no longer overlap");
    }

    /// The configuration people write first, and the storm that follows
    /// it on a link with no marker.
    #[test]
    fn a_both_entry_is_a_loop_by_construction() {
        let error = check(&bridge(vec![topic("#", Direction::Both)])).expect_err("this loops");
        assert!(
            matches!(
                error,
                TopicError::Loops {
                    out: 0,
                    inbound: 0,
                    ..
                }
            ),
            "a both entry loops against itself, got {error:?}"
        );

        // And no arrangement of prefixes saves it, because both halves
        // are the remote prefix followed by the same filter.
        let mut both = topic("#", Direction::Both);
        both.local_prefix = "a/".to_owned();
        both.remote_prefix = "b/".to_owned();
        assert!(matches!(
            check(&bridge(vec![both])).expect_err("still loops"),
            TopicError::Loops { .. }
        ));
    }

    /// The direction is not dead, only dead in the shape people write
    /// first: a template rewrites the outbound half alone, so a `both`
    /// entry that separates the two namespaces is accepted.
    #[test]
    fn a_both_entry_whose_template_separates_the_two_is_accepted() {
        let mut both = topic("sensors/+/temp", Direction::Both);
        both.template = Some("site-a/temp/{1}".to_owned());
        assert_eq!(publishes_to(&both), "site-a/temp/+");
        assert_eq!(subscribes_to(&both), "sensors/+/temp");
        check(&bridge(vec![both])).expect("these two do not meet");
    }

    #[test]
    fn two_directions_with_disjoint_remote_namespaces_are_accepted() {
        let mut out = topic("#", Direction::Out);
        out.remote_prefix = "to-remote/".to_owned();
        let mut inbound = topic("#", Direction::In);
        inbound.remote_prefix = "from-remote/".to_owned();
        inbound.local_prefix = "remote/".to_owned();
        check(&bridge(vec![out, inbound])).expect("these do not meet on the remote");
    }

    /// The loop is judged after the template has been applied, which is
    /// what "intersect after rewriting" means. Before it, these two do
    /// not overlap at all.
    #[test]
    fn a_template_can_rewrite_a_safe_pair_into_a_loop() {
        let mut out = topic("sensors/+/temp", Direction::Out);
        out.template = Some("site-a/temp/{1}".to_owned());
        let inbound = topic("site-a/#", Direction::In);
        assert!(
            !intersects("sensors/+/temp", "site-a/#"),
            "the filters as written do not overlap"
        );
        let error = check(&bridge(vec![out, inbound])).expect_err("the rewrite makes them overlap");
        assert!(matches!(error, TopicError::Loops { .. }), "got {error:?}");
    }

    #[test]
    fn a_state_topic_the_bridge_would_forward_is_refused() {
        let mut action = bridge(vec![topic("sensors/#", Direction::Out)]);
        action.state_topic = "sensors/bridge/state".to_owned();
        let error = check(&action).expect_err("a loop with a heartbeat");
        assert!(
            matches!(error, TopicError::StateTopicIsCarried { index: 0, .. }),
            "got {error:?}"
        );

        action.state_topic = "$sd/bridges/remote/state".to_owned();
        assert!(
            matches!(
                check(&action).expect_err("reserved"),
                TopicError::StateTopicReserved { .. }
            ),
            "the reserved namespace is refused before the overlap is asked about"
        );

        action.state_topic = "bridges/remote/state".to_owned();
        check(&action).expect("outside what this bridge carries");
    }

    #[test]
    fn a_state_topic_the_bridge_would_read_back_is_refused() {
        // The state topic is the will topic on the remote, so an
        // inbound entry covering it subscribes this bridge to its own
        // death notice.
        let mut action = bridge(vec![topic("status/#", Direction::In)]);
        action.state_topic = "status/bridge".to_owned();
        assert!(
            matches!(
                check(&action).expect_err("its own liveness comes back"),
                TopicError::StateTopicIsSubscribed { index: 0, .. }
            ),
            "the remote side of the same refusal"
        );
    }

    #[test]
    fn a_template_naming_a_capture_the_filter_has_not_got_is_refused() {
        let mut out = topic("sensors/+/temp", Direction::Out);
        out.template = Some("site-a/{2}/temp".to_owned());
        let error = check(&bridge(vec![out])).expect_err("there is one wildcard, not two");
        assert!(
            matches!(
                error,
                TopicError::NoSuchCapture {
                    wanted: 2,
                    available: 1,
                    ..
                }
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn a_template_on_an_inbound_entry_is_refused() {
        let mut inbound = topic("a/#", Direction::In);
        inbound.template = Some("b/{1}".to_owned());
        assert!(matches!(
            check(&bridge(vec![inbound])).expect_err("it could never run"),
            TopicError::TemplateOnInbound { index: 0 }
        ));
    }

    #[test]
    fn a_template_that_is_not_one_is_refused() {
        for (template, filter) in [
            ("a/{1", "x/+"),
            ("a/{one}", "x/+"),
            ("a/{0}", "x/+"),
            ("a/+", "x/+"),
            ("a/#", "x/+"),
            ("", "x/+"),
        ] {
            let mut out = topic(filter, Direction::Out);
            out.template = Some(template.to_owned());
            let error =
                check(&bridge(vec![out])).expect_err(&format!("{template:?} is not a template"));
            assert!(
                matches!(
                    error,
                    TopicError::BadTemplate { .. } | TopicError::NoSuchCapture { .. }
                ),
                "{template:?} gave {error:?}"
            );
        }
    }

    /// A multi-level capture can stand for any number of levels, so the
    /// remote filter it implies is everything from there on. An
    /// over-approximation, and the safe direction to err in.
    #[test]
    fn a_multi_level_capture_widens_the_remote_filter() {
        let mut out = topic("sensors/#", Direction::Out);
        out.template = Some("site-a/{1}".to_owned());
        assert_eq!(publishes_to(&out), "site-a/#");

        // And in the middle of a template, everything after it is
        // unknowable too.
        out.template = Some("site-a/{1}/tail".to_owned());
        assert_eq!(publishes_to(&out), "site-a/#");
    }

    #[test]
    fn a_single_level_capture_narrows_to_one_level() {
        let mut out = topic("sensors/+/temp", Direction::Out);
        out.template = Some("site-a/temp/{1}".to_owned());
        assert_eq!(publishes_to(&out), "site-a/temp/+");
    }

    #[test]
    fn a_prefix_that_does_not_land_on_a_level_boundary_is_refused() {
        let mut out = topic("+/temp", Direction::Out);
        out.remote_prefix = "site-".to_owned();
        let error = check(&bridge(vec![out])).expect_err("site-+/temp is not a filter");
        assert!(
            matches!(error, TopicError::PrefixedFilter { side: "remote", .. }),
            "got {error:?}"
        );
    }

    #[test]
    fn a_bridge_that_carries_nothing_is_refused() {
        assert_eq!(check(&bridge(Vec::new())), Err(TopicError::NoTopics));
    }

    #[test]
    fn the_algebra_agrees_with_the_reference_matcher_on_its_own_witnesses() {
        // Convention 6, from the other side: this is a harness that
        // could happily mirror its own mistake, so every witness is
        // checked against the matcher the workspace tests against the
        // specification's examples.
        let filters = [
            "#",
            "+",
            "a",
            "a/b",
            "a/+",
            "a/#",
            "a/+/c",
            "a/b/#",
            "+/b/+",
            "$SYS/#",
            "$SYS/+/x",
            "a//b",
            "sensors/+/temp",
            "sensors/kitchen/#",
        ];
        for a in filters {
            for b in filters {
                if let Some(witness) = intersection(a, b) {
                    assert!(
                        broker_core::topic::reference::matches(a, &witness),
                        "{a} should match its own witness {witness} (against {b})"
                    );
                    assert!(
                        broker_core::topic::reference::matches(b, &witness),
                        "{b} should match its own witness {witness} (against {a})"
                    );
                }
            }
        }
    }

    /// The other half of the same question: when the algebra says two
    /// filters do not meet, no topic either of them describes is
    /// matched by both.
    #[test]
    fn filters_the_algebra_separates_share_no_topic_it_can_name() {
        let filters = [
            "a/b", "a/c", "a/+", "a/#", "b/#", "+/b", "$SYS/#", "#", "a", "a/b/c",
        ];
        let topics = [
            "a", "a/b", "a/c", "a/b/c", "b/b", "b", "$SYS/x", "$SYS", "a/b/c/d", "x/b",
        ];
        for a in filters {
            for b in filters {
                if intersects(a, b) {
                    continue;
                }
                for name in topics {
                    assert!(
                        !(broker_core::topic::reference::matches(a, name)
                            && broker_core::topic::reference::matches(b, name)),
                        "{a} and {b} were said not to meet, and both match {name}"
                    );
                }
            }
        }
    }

    // ---- two rules, one remote -----------------------------------------

    /// A bridge to `address` with the given entries.
    fn bridge_to(address: &str, topics: Vec<BridgeTopic>) -> MqttBridge {
        MqttBridge {
            address: address.to_owned(),
            client_id: "bridge".to_owned(),
            topics,
            ..MqttBridge::default()
        }
    }

    /// One entry, with prefixes.
    fn entry(
        filter: &str,
        direction: Direction,
        local_prefix: &str,
        remote_prefix: &str,
    ) -> BridgeTopic {
        BridgeTopic {
            filter: filter.to_owned(),
            direction,
            qos: QoS::AtLeastOnce,
            local_prefix: local_prefix.to_owned(),
            remote_prefix: remote_prefix.to_owned(),
            ..BridgeTopic::default()
        }
    }

    /// The cycle [`check`] cannot see: A out, the remote, B in, this
    /// broker, A out again.
    ///
    /// Each rule passes [`check`] on its own — asserted first, because
    /// otherwise this could be passing for the single-rule reason and
    /// nobody would know.
    #[test]
    fn two_bridge_rules_to_one_remote_that_feed_each_other_are_refused() {
        let out = bridge_to(
            "mqtt://127.0.0.1:1883",
            vec![entry("site/#", Direction::Out, "", "up/")],
        );
        let back = bridge_to(
            "mqtt://127.0.0.1:1883",
            vec![entry("site/#", Direction::In, "", "up/")],
        );
        check(&out).expect("the outbound rule loops through nothing of its own");
        check(&back).expect("the inbound rule loops through nothing of its own");

        let refused = check_across([("a", &out), ("b", &back)])
            .expect_err("two rules to one remote that feed each other must be refused");
        match refused {
            TopicError::LoopsAcrossRules(across) => {
                assert_eq!(
                    (across.out_rule.as_str(), across.in_rule.as_str()),
                    ("a", "b")
                );
                assert_eq!(
                    across.witness, "up/site/any",
                    "the remote witness is not a topic"
                );
                assert_eq!(
                    across.local_witness, "site/any",
                    "the local witness is not a topic"
                );
            }
            other => panic!("the wrong refusal: {other}"),
        }
        // And the other order, because the pair is a pair: `check`
        // walks every ordered pair and a walk that only went forwards
        // would let the same two rules through when they were written
        // the other way round.
        assert!(check_across([("b", &back), ("a", &out)]).is_err());
    }

    /// The same two rules pointed at two remotes are not a loop.
    ///
    /// Nothing carries what A published to one broker onto the other,
    /// so there is no cycle to close. The pair is otherwise identical
    /// to the one above, which is what makes this an assertion about
    /// the address rather than about the filters.
    #[test]
    fn two_bridge_rules_to_different_remotes_do_not_loop() {
        let out = bridge_to(
            "mqtt://127.0.0.1:1883",
            vec![entry("site/#", Direction::Out, "", "up/")],
        );
        let back = bridge_to(
            "mqtt://127.0.0.2:1883",
            vec![entry("site/#", Direction::In, "", "up/")],
        );
        check_across([("a", &out), ("b", &back)]).expect("two remotes are not one cycle");
    }

    /// One remote written two ways is one remote.
    ///
    /// The default port is the port, so a pair that a string
    /// comparison would call different brokers is refused.
    #[test]
    fn a_default_port_and_a_written_one_are_one_remote() {
        let out = bridge_to(
            "mqtt://localhost",
            vec![entry("site/#", Direction::Out, "", "up/")],
        );
        let back = bridge_to(
            "mqtt://LOCALHOST:1883",
            vec![entry("site/#", Direction::In, "", "up/")],
        );
        assert!(check_across([("a", &out), ("b", &back)]).is_err());
    }

    /// Hub and spoke, which must not be refused.
    ///
    /// The remote filters meet — A publishes to `up/site/...` and B
    /// subscribes to it — and B delivers it locally under a prefix
    /// that A does not carry. Nothing comes round, and refusing this
    /// would refuse the configuration a bridge publishing inbound
    /// messages into the local broker exists for.
    #[test]
    fn a_rule_that_mirrors_what_another_sends_is_not_a_loop() {
        let out = bridge_to(
            "mqtt://127.0.0.1:1883",
            vec![entry("site/#", Direction::Out, "", "up/")],
        );
        let mirror = bridge_to(
            "mqtt://127.0.0.1:1883",
            vec![entry("site/#", Direction::In, "mirror/", "up/")],
        );
        check_across([("a", &out), ("b", &mirror)])
            .expect("a mirror onto a local namespace nothing carries out is not a cycle");
    }

    /// And the other half of the same point: two rules whose local
    /// sides meet and whose remote sides do not.
    ///
    /// B delivers exactly where A fires, so the local hop closes — and
    /// the remote hop does not, because B is subscribed to something A
    /// never publishes. Both halves are required, and this is the test
    /// that says the remote half is really being checked rather than
    /// assumed.
    #[test]
    fn two_rules_that_meet_only_on_this_broker_do_not_loop() {
        let out = bridge_to(
            "mqtt://127.0.0.1:1883",
            vec![entry("site/#", Direction::Out, "", "up/")],
        );
        let elsewhere = bridge_to(
            "mqtt://127.0.0.1:1883",
            vec![entry("site/#", Direction::In, "", "down/")],
        );
        check_across([("a", &out), ("b", &elsewhere)])
            .expect("filters that do not meet on the remote are not a cycle");
    }

    /// One rule on its own is [`check`]'s and not this one's.
    ///
    /// A `both` entry is a loop and is refused when the rule is read;
    /// walking it against itself here would refuse it a second time
    /// under a message about a pair of rules, which names the wrong
    /// thing to fix.
    #[test]
    fn a_rule_is_not_checked_against_itself() {
        let both = bridge_to(
            "mqtt://127.0.0.1:1883",
            vec![entry("site/#", Direction::Both, "", "")],
        );
        check(&both).expect_err("a both entry with no template is a loop, and `check` says so");
        check_across([("a", &both)]).expect("one rule is not a pair");
    }
}
