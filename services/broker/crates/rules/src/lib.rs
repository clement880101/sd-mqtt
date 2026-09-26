//! Rules: what fires a connector, and what the connector does.
//!
//! Section 9 of the design. A rule is durable node-local metadata in
//! milestone 4 and replicated metadata in milestone 5; what changes then
//! is how many nodes hold one, not its shape.
//!
//! **This crate does no I/O and knows nothing about HTTP.** It is the
//! vocabulary — a rule, its trigger, its action's configuration — and the
//! translation between that vocabulary and the bytes the store holds. The
//! hook that writes to an outbox and the task that posts are separate,
//! for the reason [`broker_core::hooks`] gives: a hook runs under the
//! broker's one lock and may not reach the network.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod outbox;
pub mod rule;

pub use outbox::Outbox;
pub use rule::{Action, ClientPattern, HttpOut, Rule, RuleError, RuleId, Trigger};
