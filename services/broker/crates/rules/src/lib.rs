//! Rules: what fires a connector, and what the connector does.
//!
//! Section 9 of the design. A rule is durable node-local metadata in
//! milestone 4 and replicated metadata in milestone 5; what changes then
//! is how many nodes hold one, not its shape.
//!
//! # Where the line is
//!
//! [`rule`], [`outbox`] and [`envelope`] do no I/O at all. They are the
//! vocabulary — a rule, its trigger, its action's configuration, the
//! document that goes on the wire — and the translation between that
//! vocabulary and the bytes the store holds. [`http_out`] is the half
//! that reaches the network, and it is a **separate task** rather than
//! something a hook calls, for the reason [`broker_core::hooks`] gives:
//! a hook runs under the broker's one lock and may not await.
//!
//! So a rule firing is a hook appending to a durable outbox and
//! returning. The posting happens somewhere else, later, and survives
//! the broker being killed in between.
//!
//! # What `max_concurrency` does today, and what it does not
//!
//! Section 9.2 lists maximum concurrency among the things a rule
//! configures. It is parsed, validated — zero is refused, because a rule
//! that may never post is a rule that silently delivers nothing — and
//! stored, and **the pump does not yet use it to go wider**: one
//! envelope is posted at a time, which is within any configured maximum
//! of one or more. That is a deliberate deviation and not an oversight.
//!
//! The reason is the interaction with the backoff. The backoff belongs
//! to the rule rather than to an envelope, precisely so that a failing
//! endpoint sees one attempt per window instead of one per queued
//! message; a concurrent pump has to decide what several in-flight
//! requests do to one rule-wide wait, and getting that wrong
//! reintroduces the thundering herd this design exists to avoid. It is
//! worth doing properly rather than approximately, and the exit
//! criterion this milestone is measured against does not need it.
//!
//! # Delivery is at least once
//!
//! An `http_out` endpoint will eventually receive the same message
//! twice, because a request that timed out may have arrived. Every
//! envelope carries a stable identifier so that a receiver can tell the
//! repeat from a new message; the whole argument is in [`http_out`].

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod bridge;
pub mod envelope;
pub mod hops;
pub mod http_in;
pub mod http_out;
pub mod http_poll;
pub mod outbox;
pub mod rule;
pub mod singleton;

pub use bridge::TopicError;
pub use envelope::Envelope;
pub use http_in::{HttpInError, Ingest};
pub use http_out::{Attempt, DeadLetters, Delivery, HttpOutError, Poster, Turn};
pub use http_poll::{Fetched, Fetcher, HttpPollError, Polled, Poller};
pub use outbox::Outbox;
pub use rule::{
    Action, BridgeTopic, ClientPattern, Direction, HttpIn, HttpOut, HttpPoll, MqttBridge, Rule,
    RuleError, RuleId, TopicFromRequest, TopicSource, Trigger,
};
pub use singleton::{Membership, NodeId, Ownership};
