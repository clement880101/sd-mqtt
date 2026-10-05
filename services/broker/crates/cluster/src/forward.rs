//! One copy to each node that wants it.
//!
//! Section 8.3: a publish matches locally, enqueues to local sessions,
//! and **forwards one copy to each remote node that has any match**.
//! The receiving node matches against its own local subscribers. Fan-out
//! cost is therefore proportional to the number of interested nodes, not
//! to the number of subscribers — which is the whole economy the design
//! is built on, and the part a test asserting that a message arrived
//! cannot see.
//!
//! # A forwarded message is marked and is never forwarded again
//!
//! The mark is not here. It is `broker_core::session::Message::forwarded`,
//! set by `Broker::accept_forwarded` and tested by `Broker::publish`,
//! which is the only place that could get it right: this type is asked
//! what to do with a publication *after* that test, so an implementation
//! of the rule here would be a second answer and the one that cannot see
//! the message that never reached it.
//!
//! What is here is the other half of the same sentence: nothing in this
//! module ever sends a [`Forwarded`] on. A frame arrives, the shell
//! hands it to `accept_forwarded`, and the mark stops it at the next
//! publish.
//!
//! # The frame carries the record the store already holds
//!
//! [`Forwarded`] is a message identifier and a [`BodyRecord`], which is
//! the record a message body is *stored* as. One format rather than two:
//! `broker_core::journal`'s `body_record` and `live_message` are the
//! conversion pair, they are already exercised by every restart, and a
//! second spelling of "a message outside the broker" would drift from
//! this one the first time either was corrected.
//!
//! The identifier travels with it, so a message keeps the identity it
//! was given on the node where it entered. Minting a new one per hop
//! would make one publication several messages in a log, and section
//! 6's identifiers are UUID v7 precisely so that two nodes need not
//! coordinate about them.
//!
//! # What is deferred, and said here rather than rediscovered
//!
//! Section 6 prefers "a local online member, then the least loaded
//! remote node" for a shared subscription. The local preference is
//! implemented — see [`crate::groups`] — and **the remote ordering waits
//! for milestone 6**, because a queue depth from another node is not a
//! number this cluster carries yet. Queue replication is what makes a
//! depth worth reading; until then the choice among remote member nodes
//! is a rotation, which is the honest answer rather than a load
//! comparison against a figure that does not exist.

use std::sync::Arc;

use broker_core::broker::Forwarder;
use broker_core::session::message::Message;
use serde::{Deserialize, Serialize};
use storage::record::BodyRecord;

use crate::identity::NodeId;
use crate::mesh::{self, Mesh};
use crate::metadata::Replicated;
use crate::routes::Routes;

/// A publication on its way to another node.
///
/// The body is the record the store holds, for the reason the module
/// documentation gives. Nothing here says which node sent it: a link
/// has already proved who is on the other end of it, which is where
/// section 8.2's frames take the same answer from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Forwarded {
    /// The message's identifier, unchanged across the hop.
    pub id: [u8; 16],
    /// Everything else about it.
    pub body: BodyRecord,
}

impl Forwarded {
    /// The frame for `message`.
    #[must_use]
    pub fn of(message: &Message) -> Self {
        Self {
            id: *message.id.as_bytes(),
            body: broker_core::journal::body_record(message),
        }
    }

    /// The message it was made from.
    ///
    /// **Unmarked**, and that is not an oversight: the mark is set by
    /// `Broker::accept_forwarded`, which is the one door a frame goes
    /// through. Setting it here as well would be two places that have to
    /// agree, and the one a test is less likely to walk.
    #[must_use]
    pub fn into_message(self) -> Message {
        broker_core::journal::live_message(storage::record::MessageId(self.id), &self.body)
    }
}

/// Which nodes a publication on `topic` goes to.
///
/// Everything [`Routes`] answers, less this node. The subtraction is
/// here rather than in the map because a map that quietly left one node
/// out would be a map whose answer depended on who was asking — and
/// because this node has already delivered to its own subscribers by
/// the time anything asks: forwarding to itself would be a second copy
/// to every one of them.
///
/// A node this one has judged dead is already absent; [`Routes`] says
/// why that is a withholding rather than a forgetting.
#[must_use]
pub fn targets(me: NodeId, routes: &Routes, topic: &str) -> Vec<NodeId> {
    let mut nodes = routes.nodes_for(topic);
    nodes.retain(|node| *node != me);
    nodes
}

/// What forwards a publication over a mesh.
///
/// The shell installs one of these into the broker on a node that
/// clusters; a node that does not install none, and
/// `Broker::publish` then costs one `Option` test per publication.
///
/// # It sends under the broker's lock, and that is the bargain
///
/// `Broker::publish` calls this with whatever lock the shell holds
/// around the broker still held, as it calls a hook. What that buys is
/// that a publication cannot be forwarded out of order with the local
/// delivery it belongs with, and that the routing map cannot change
/// between the two. What it costs is that everything here must return
/// without waiting: [`Mesh::send`] is a bounded queue per peer and a
/// full one is a **drop, counted** by section 8.1's own numbers rather
/// than a wait, which is exactly the property that makes this safe.
#[derive(Debug)]
pub struct Forwarding {
    /// Who this node is, and so which node not to forward to.
    me: NodeId,
    /// Where the routing map lives, section 8.2's registers indexed.
    replicated: Arc<Replicated>,
    /// What there are links over.
    mesh: Arc<Mesh>,
}

impl Forwarding {
    /// Forward over `mesh`, routing by what `replicated` holds.
    #[must_use]
    pub fn new(replicated: Arc<Replicated>, mesh: Arc<Mesh>) -> Self {
        Self {
            me: replicated.me(),
            replicated,
            mesh,
        }
    }
}

impl Forwarder for Forwarding {
    fn forward(&self, message: &Message) -> usize {
        let targets = targets(self.me, self.replicated.routes(), &message.topic);
        if targets.is_empty() {
            // **The economy, and the thing a delivery test cannot see.**
            // A publication nobody else wants is framed once for nobody
            // and sent to nobody, rather than broadcast and discarded at
            // the far end.
            return 0;
        }
        let frame = mesh::Message::Forward(Forwarded::of(message));
        let mut sent = 0;
        for peer in targets {
            match self.mesh.send(peer, &frame) {
                Some(Ok(crate::link::Sent::Queued)) => sent += 1,
                // Section 8.1 drops and counts rather than letting a
                // wedged peer block whoever had something to say to it.
                // Counted there, in bytes, which is where the bound is.
                Some(Ok(crate::link::Sent::Dropped)) => tracing::warn!(
                    %peer,
                    topic = %message.topic,
                    "a peer's queue was full, so a forwarded publication was dropped"
                ),
                Some(Err(why)) => tracing::error!(
                    %why,
                    "a forwarded publication could not be framed, which is this build \
                     refusing to write what it would not read back"
                ),
                // A node that wants the topic and has no link right now.
                // Not an error and not repaired: section 8.3 forwards
                // live traffic, and a message held for a peer that might
                // come back is section 8.6's queue replication.
                None => {}
            }
        }
        sent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{HybridClock, DEFAULT_MAX_DRIFT_MILLIS};
    use bytes::Bytes;
    use mqtt_codec::QoS;
    use storage::Restored;

    fn node(index: u8) -> NodeId {
        NodeId::from_bytes([index; 16])
    }

    fn replicated(me: NodeId) -> Replicated {
        Replicated::open(
            me,
            HybridClock::resume(None, DEFAULT_MAX_DRIFT_MILLIS),
            &Restored::default(),
        )
        .expect("an empty store holds nothing to refuse")
    }

    #[test]
    fn a_node_is_never_a_target_of_its_own() {
        // It has already delivered to its own subscribers by the time
        // anything asks, so a forward to itself is a second copy to
        // every one of them.
        let me = node(1);
        let held = replicated(me);
        held.subscriber_arrived("a/b")
            .expect("the first subscriber");
        assert_eq!(
            held.nodes_for("a/b"),
            vec![me],
            "the map holds this node like any other"
        );
        assert!(targets(me, held.routes(), "a/b").is_empty());
    }

    #[test]
    fn only_the_nodes_that_want_the_topic_are_targets() {
        let me = node(1);
        let wants = node(2);
        let does_not = node(3);
        let held = replicated(me);
        held.observed(crate::MetadataWrite::new(
            crate::metadata::MetadataKey::NodeSubscription {
                node: wants,
                filter: "a/+".to_owned(),
            },
            crate::Register::written(
                crate::metadata::MetadataValue::NodeSubscription,
                crate::Stamp::new(crate::clock::Reading::new(10, 0), wants),
            ),
        ));
        held.observed(crate::MetadataWrite::new(
            crate::metadata::MetadataKey::NodeSubscription {
                node: does_not,
                filter: "z/#".to_owned(),
            },
            crate::Register::written(
                crate::metadata::MetadataValue::NodeSubscription,
                crate::Stamp::new(crate::clock::Reading::new(11, 0), does_not),
            ),
        ));

        assert_eq!(targets(me, held.routes(), "a/b"), vec![wants]);
        assert_eq!(targets(me, held.routes(), "z/y"), vec![does_not]);
        assert!(
            targets(me, held.routes(), "nobody/wants/this").is_empty(),
            "a topic nobody wants is forwarded nowhere at all"
        );
    }

    #[test]
    fn a_frame_round_trips_through_the_record_the_store_holds() {
        // One format for "a message outside the broker". The identifier
        // survives, because a publication is one message however many
        // hops it makes.
        let mut message = Message::new(
            "site/1/temp",
            Bytes::from_static(b"21.5"),
            QoS::AtLeastOnce,
            true,
        );
        message.expires_at = Some(42);
        let back = Forwarded::of(&message).into_message();

        assert_eq!(back.id, message.id, "the identifier changed across a hop");
        assert_eq!(&*back.topic, "site/1/temp");
        assert_eq!(back.payload, message.payload);
        assert_eq!(back.qos, QoS::AtLeastOnce);
        assert!(back.retain, "the flag travels, the retained store does not");
        assert_eq!(back.expires_at, Some(42));
        assert!(
            !back.forwarded,
            "the mark is set by the one door a frame goes through, not here"
        );
    }
}
