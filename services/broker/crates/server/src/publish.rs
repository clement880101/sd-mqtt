//! Publishing from something that is not a connection.
//!
//! # There is one publish, and this is it
//!
//! Retention, expiry, matching, shared-subscription selection, the
//! per-session queue and its bounds are all rules, and a second set of
//! them anywhere would drift from the first the day either was
//! corrected. So everything that publishes without a session behind it
//! — the admin API's `POST /api/v1/publish`, section 9.2's ingest
//! endpoint, and whatever comes next — builds the same [`Message`] a
//! connection's publish builds and hands it to the same
//! [`Broker::publish`], under the same [`Shared::with_broker_committed`],
//! and delivers what comes back with the same [`Shared::deliver`].
//!
//! This module was extracted from the admin API's handler when the
//! second caller arrived, which is the moment a copy would have been
//! made instead.
//!
//! # The answer is the acknowledgement
//!
//! Section 7.3: an acknowledgement of an at-least-once or exactly-once
//! publication tells a publisher the broker has taken responsibility,
//! so it must not be sent before the message is durable. A caller here
//! has no session and therefore no PUBACK to send; the promise it makes
//! is whatever it answers its own client with, so [`publish`] does not
//! return until the batch is durable when the level asks for it.
//!
//! [`promises_durability`] is not what decides that. That function
//! reads a batch's actions for a PUBACK, PUBREC or PUBCOMP, and there
//! is no such packet here because there is no publishing session to
//! send one to. So the level is asked directly, and at-most-once, which
//! promises nobody anything, waits for nothing.
//!
//! [`Broker::publish`]: broker_core::broker::Broker::publish
//! [`Message`]: broker_core::session::message::Message
//! [`promises_durability`]: crate::shared::promises_durability

use std::sync::Arc;

use broker_core::session::message::Message;
use bytes::Bytes;
use mqtt_codec::{Properties, QoS};

use crate::shared::Shared;

/// One publication, as something that is not a connection asks for it.
#[derive(Debug, Clone)]
pub struct Publication {
    /// Where it goes. A topic name, so no wildcards.
    pub topic: String,
    /// What it carries.
    pub payload: Bytes,
    /// The level to publish at.
    pub qos: QoS,
    /// Whether to retain it.
    pub retain: bool,
    /// How long it stays deliverable, in seconds.
    pub message_expiry_interval: Option<u32>,
}

/// What a publication that was made comes back with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Published {
    /// How many subscribers it reached. Zero is an ordinary answer: a
    /// retained publish to a topic nobody holds reaches nobody and is
    /// still kept.
    pub delivered: usize,
    /// Whether this answer waited for the store.
    ///
    /// Reported rather than implied, because the promise a caller is
    /// entitled to read into an acknowledgement differs between the
    /// levels and an interface should not have to know the rule to say
    /// so.
    pub durable: bool,
}

/// Why a publication was not made.
#[derive(Debug, thiserror::Error)]
pub enum PublishRefused {
    /// The topic is not one a message can be published to.
    #[error("that is not a topic a message can be published to: {0}")]
    NotATopic(String),
    /// The topic is in the namespace the broker reserves for itself.
    ///
    /// Section 12.3: dollar-prefixed topics are not publishable.
    ///
    /// **Here, and not yet on a connection.** The rule is written about
    /// clients and the MQTT publish path does not enforce it; that is
    /// older than this module and is deferred to milestone 7, where the
    /// authorization work it belongs with lives. What is not deferred
    /// is this, because every caller of this module is a new way to
    /// write one — and a retained one, which stays until somebody finds
    /// it. A reserved namespace that can be filled from a form or a
    /// webhook is worse than one a client can fill, because those are
    /// the things that look sanctioned.
    ///
    /// Only the first level. `a/$b` is an ordinary topic and the
    /// matching layer already treats it as one.
    #[error(
        "the dollar-prefixed namespace is reserved for the broker and is not publishable, \
         so this endpoint will not write one. Publish to a topic outside it."
    )]
    Reserved,
    /// The batch could not be made durable, so nothing is acknowledged.
    #[error("the message could not be written to the store, so it is not acknowledged")]
    NotDurable,
}

/// Publish `publication` through the path a connection publishes by.
///
/// # Errors
///
/// [`PublishRefused`], which is the topic being unusable, the topic
/// being reserved, or the store being unable to take the batch. The
/// first two are refusals made before anything is written; the third is
/// a publication that was made and then could not be promised, and
/// [`PublishRefused::NotDurable`] says the caller is told so rather
/// than answered.
pub async fn publish(
    shared: &Shared,
    publication: Publication,
) -> Result<Published, PublishRefused> {
    let Publication {
        topic,
        payload,
        qos,
        retain,
        message_expiry_interval,
    } = publication;
    // Only at-most-once promises nobody anything. Both acknowledged
    // levels are a promise, and both wait.
    let promised = qos != QoS::AtMostOnce;
    let properties = Properties {
        message_expiry_interval,
        ..Properties::default()
    };

    let (outcome, committed) = shared.with_broker_committed(
        |broker| {
            // Validated inside the lock only because the limits are the
            // broker's: one acquisition rather than two, and the work
            // is a scan of a short string.
            mqtt_codec::topic::validate_name(&topic, &broker.config().limits)
                .map_err(|error| PublishRefused::NotATopic(error.to_string()))?;
            if topic.starts_with('$') {
                return Err(PublishRefused::Reserved);
            }

            let now = broker.clock().now();
            let mut message = Message::new(&topic, payload.clone(), qos, retain);
            // The same two lines a connection's publish uses, so the
            // properties a subscriber receives and the deadline the
            // broker enforces come from one rule.
            message.properties = Arc::new(properties.clone().into());
            message.expires_at = properties
                .message_expiry_interval
                .map(|interval| broker.clock().deadline(interval));

            let mut out = Vec::new();
            // `None`: this publication has no publishing session, so
            // no-local cannot apply to it and it is not any
            // subscriber's own message.
            let delivered = broker.publish(message, None, now, &mut out);
            Ok((delivered, out))
        },
        // A batch that failed validation produced nothing and promised
        // nothing.
        |outcome: &Result<(usize, Vec<_>), PublishRefused>| promised && outcome.is_ok(),
    );

    let (delivered, actions) = outcome?;

    if let Err(error) = committed.wait().await {
        // The same partition a connection makes, with the easy half:
        // nothing in this batch is addressed to the caller, because the
        // caller has no session. So every action goes, exactly as it
        // does for a publisher that is being closed — those subscribers
        // are alive, were promised nothing, and have already had the
        // delivery recorded against their windows. Leaving them
        // undelivered would strand a window slot until the peer
        // reconnected.
        tracing::error!(
            %error,
            "a publication made without a session could not be made durable; \
             the caller is told so rather than answered"
        );
        shared.deliver(actions);
        return Err(PublishRefused::NotDurable);
    }
    shared.deliver(actions);

    Ok(Published {
        delivered,
        durable: promised,
    })
}
