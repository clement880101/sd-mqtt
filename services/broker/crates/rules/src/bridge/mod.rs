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
//! # What an inbound message does **not** do here, and it is section 9.2
//!
//! Section 9.2 says a bridge "publishes them into the local broker
//! where any rule can act on them". Today they cannot, and the reason
//! is one layer down: a bridge publishes through
//! `sdmqtt_server::publish`, which goes to `Broker::publish` — the
//! internal route a will takes — and **that route does not fire the
//! message hooks**. So an inbound message reaches every subscriber and
//! fires no rule, and the same is true of section 9.2's ingest
//! endpoint, which has used that route since it was written.
//!
//! This is recorded rather than fixed because fixing it is a change to
//! the publish path shared by three connectors and the admin API, with
//! its own question about what a hook may then do to a message the
//! broker originated, and it is not what this task is. Convention 8:
//! the deferred and the forgotten look identical in a codebase and
//! opposite in a review, so it is said here.
//!
//! Two consequences worth stating plainly. The loop that cannot happen
//! today is a message arriving through this bridge and being sent
//! straight back out by its own outbound entries, because nothing
//! fires them. The one that can, and which [`marker`] is tested
//! against end to end, is a message this bridge sent that the *remote*
//! republished and handed back.
//!
//! # No client library, and the reason is not weight
//!
//! Recorded in `docs/research/component-selection.md`: the one
//! considered fails this project's advisory gate today through a pin
//! only its maintainer can move, would link a second cryptography
//! provider, a second root-trust mechanism, a second logging facade and
//! a second MQTT codec, and keeps its in-flight state in private
//! in-memory fields, so the durable bridge would have had to be written
//! anyway.

pub mod backoff;
pub mod rewrite;
pub mod topics;

pub use backoff::{Backoff, Entropy};
pub use rewrite::{Rewrite, Rewritten};
pub use topics::{intersects, TopicError};
