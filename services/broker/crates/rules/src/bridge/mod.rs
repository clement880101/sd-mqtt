//! Section 9.2's `mqtt_bridge`: a connection to a remote broker, made
//! as a client.
//!
//! # This is the first client this codebase has ever been
//!
//! The codec encodes and decodes every packet in both directions and
//! the framing reader is generic over any stream, so the wire format
//! was finished before this module existed. What was missing is
//! everything a client does that a server does not, and the research
//! notes list it: composing a CONNECT, **interpreting a CONNACK and
//! honouring the limits the remote declares in it**, a keep-alive with
//! a deadline on the response, an in-flight window against the remote's
//! receive maximum, inbound exactly-once, an inbound alias table, and
//! correlating subscribe acknowledgements.
//!
//! The third of those has no equivalent anywhere else here. This broker
//! only ever decodes a CONNECT and writes a CONNACK; nothing in it has
//! ever had to obey one. Ignoring any of the limits a remote declares
//! is a protocol error it will disconnect for, and the symptom is a
//! bridge that works against a permissive broker and fails against a
//! strict one.
//!
//! # Two of these exist already, typed for the other side
//!
//! `broker_core::session::outbound` is an in-flight window and
//! `broker_core::session::inbound` is exactly-once receipt, and both
//! are written in server vocabulary throughout: a session key, a
//! journal of durable records, a queue with per-session bounds, an
//! `Action` a connection hands back. Read them before reading
//! [`flow`] and [`inbound`], which are the same two rules from the
//! client's side, and do not try to share them.
//!
//! # What an inbound message does here, and it is section 9.2
//!
//! Section 9.2 says a bridge "publishes them into the local broker
//! where any rule can act on them". It did not, and the reason was one
//! layer down: a bridge publishes through `sdmqtt_server::publish`,
//! which went to `Broker::publish` — the internal route that fires no
//! message hook. So an inbound message reached every subscriber and
//! fired no rule, and the same was true of section 9.2's ingest
//! endpoint and of a poller.
//!
//! It now goes through `Broker::originate`, which fires them, and the
//! sentence is true. What that opens is a loop: a message this bridge
//! brings in can now fire a rule, and a rule can be another bridge's
//! outbound half. Three things bound it, and they are different checks
//! rather than three copies of one.
//!
//! - [`topics::check`] refuses a configuration whose inbound and
//!   outbound filters intersect, so the tightest loop — this bridge's
//!   own entries feeding each other — cannot be configured at all.
//! - [`marker`] refuses a message that has already been through **this**
//!   bridge, whichever direction it was going. That closes every cycle
//!   made of bridges, because such a cycle must come back to the bridge
//!   it left by, and by then the mark is on it.
//! - The hop count closes the one the marker cannot see, which has no
//!   bridge in it: an `http_out` rule feeding a system that posts back
//!   through an ingest rule. [`marker::carry`] applies this rule's
//!   `mqtt_bridge.max_hops` before a message costs a round trip, and
//!   `Broker::originate` applies the node's `broker.max_hops` at the
//!   door every publication without a session goes through. See
//!   [`crate::hops`].
//!
//! # No client library, and the reason is not weight
//!
//! Recorded in `docs/components.md`: the one
//! considered fails this project's advisory gate today through a pin
//! only its maintainer can move, would link a second cryptography
//! provider, a second root-trust mechanism, a second logging facade and
//! a second MQTT codec, and keeps its in-flight state in private
//! in-memory fields, so the durable bridge would have had to be written
//! anyway.

pub mod aliases;
pub mod backoff;
pub mod connack;
pub mod flow;
pub mod inbound;
pub mod keepalive;
pub mod marker;
pub mod rewrite;
pub mod session;
pub mod subs;
pub mod supervisor;
pub mod topics;

pub use aliases::Aliases;
pub use backoff::{Backoff, Entropy};
pub use connack::{negotiate, Asked, ConnAckError, Negotiated};
pub use flow::{Flow, FlowError};
pub use inbound::Inbound;
pub use keepalive::KeepAlive;
pub use marker::{carry, Marker, Refused};
pub use rewrite::{Rewrite, Rewritten};
pub use subs::{Granted, SubscribeError, Subscriptions};
pub use supervisor::{address, Address, AddressError, Supervisor};
pub use topics::{intersects, AcrossLoop, TopicError};
