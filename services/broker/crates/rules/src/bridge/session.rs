//! One network connection to the remote, from CONNECT to close.
//!
//! Everything the other modules in [`super`] decide is applied here,
//! and nothing is decided here that could have been decided there. The
//! split is deliberate: a socket loop is the hardest thing in a
//! codebase to write a test for, so the rules live in pure functions
//! next door and this is the part that is exercised against a real
//! broker.
//!
//! # The order of the first four packets is the whole protocol
//!
//! CONNECT goes out, the CONNACK comes back, [`super::connack`] turns
//! it into a [`Negotiated`], and **only then** does anything else
//! happen. A bridge that sent a SUBSCRIBE before reading the CONNACK
//! would be sending it before it knew whether wildcards were
//! available, how large a packet may be, or what identifier it is
//! connected under. [MQTT-3.1.4-5] says the server sends the CONNACK
//! first; this end waits for it.
//!
//! # Two directions, one loop
//!
//! Inbound is driven by the socket. Outbound is driven by the durable
//! outbox, which the local broker's hook fills, and is woken by a
//! [`Notify`] rather than polled — a bridge that polled would add its
//! interval to every message's latency for no reason. The poll
//! interval that remains is a safety net against a wake-up lost to a
//! race, not the mechanism.
//!
//! # What is durable and what is not
//!
//! The outbox is. The in-flight window, the alias table and the
//! exactly-once receipts are not, and [`super::flow`] says why that is
//! safe: the supervisor forces a clean start on the first connection
//! of a process, so there is no session holding identifiers this end
//! has forgotten.

use std::sync::Arc;

use bytes::BytesMut;
use mqtt_codec::packet::{Disconnect, Publish};
use mqtt_codec::{Limits, Packet, Properties, QoS, ReasonCode, Version};
use sdmqtt_server::framing::FrameReader;
use sdmqtt_server::publish::{self, Publication, PublishRefused};
use sdmqtt_server::shared::Shared;
use storage::record::{OutboxRecord, Record};
use storage::Storage;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::Notify;
use tokio::time::{Duration, Instant};

use crate::outbox::Outbox;
use crate::rule::{MqttBridge, RuleId};

use super::aliases::Aliases;
use super::connack::{self, Asked, ConnAckError, Negotiated, OFFLINE, ONLINE};
use super::flow::{Finished, Flow};
use super::inbound::Inbound;
use super::keepalive::{Due, KeepAlive};
use super::marker::{self, Marker, Refused};
use super::rewrite::{Rewrite, Rewritten};
use super::subs::Subscriptions;

/// How long to wait for the CONNACK.
///
/// A remote that accepts a socket and never answers is the failure a
/// plain connect timeout does not catch, because the socket is open
/// and healthy. Ten seconds is long enough for a slow far side and
/// short enough that a bridge pointed at a black hole is back in the
/// reconnection schedule rather than sitting there.
const CONNACK_TIMEOUT: Duration = Duration::from_secs(10);

/// The safety net behind the outbox's [`Notify`].
///
/// Not the mechanism. A wake-up can be lost to a race between the
/// hook's notify and the store's write becoming visible, and a bridge
/// that then waited for the next message would hold the one before it
/// for ever.
const OUTBOX_POLL: Duration = Duration::from_secs(1);

/// Why a connection ended.
#[derive(Debug, thiserror::Error)]
pub enum Ended {
    /// The remote closed the connection.
    #[error("the remote closed the connection")]
    Closed,
    /// The remote sent a DISCONNECT.
    #[error("the remote disconnected: {reason:?}")]
    Disconnected {
        /// What it said.
        reason: ReasonCode,
    },
    /// The socket failed.
    #[error("the connection failed: {0}")]
    Io(String),
    /// One of us broke the protocol.
    ///
    /// The reason code is the one this end closed with, so that an
    /// operator reading a log on either side sees the same word.
    #[error("protocol error ({reason:?}): {why}")]
    Protocol {
        /// What this end told the remote.
        reason: ReasonCode,
        /// What went wrong.
        why: String,
    },
    /// The remote stopped answering pings.
    #[error(
        "the remote did not answer a ping inside the keep-alive deadline, so the link is \
         gone whatever the socket believes"
    )]
    KeepAliveLost,
    /// The remote would not accept this connection.
    #[error(transparent)]
    ConnAck(#[from] ConnAckError),
}

impl Ended {
    /// Whether reconnecting could plausibly help.
    ///
    /// A bridge whose remote refuses wildcards will be refused again in
    /// a second and in an hour, so it is worth saying loudly once
    /// rather than quietly for ever. Everything else is worth
    /// retrying, including a refusal: a remote that is not authorising
    /// us today may be reconfigured tomorrow, and the backoff is what
    /// keeps asking cheap.
    pub fn worth_retrying(&self) -> bool {
        !matches!(self, Self::ConnAck(ConnAckError::NoWildcards { .. }))
    }
}

/// Everything one bridge needs that does not change between
/// connections.
pub struct Bridge {
    /// Whose bridge.
    pub rule: RuleId,
    /// What an operator configured.
    pub action: MqttBridge,
    /// The topic entries, compiled.
    pub rewrites: Vec<Rewrite>,
    /// What this bridge marks its traffic with.
    pub marker: Marker,
    /// Where outbound messages are queued durably.
    pub outbox: Arc<Outbox>,
    /// Where they are read from.
    pub store: Arc<dyn Storage>,
    /// Where records are written.
    pub writer: Arc<storage::Writer>,
    /// The local broker, for inbound messages.
    pub shared: Arc<Shared>,
    /// Woken when the hook has put something in the outbox.
    pub wake: Arc<Notify>,
    /// What this rule has been doing. See [`crate::stats`].
    ///
    /// A bridge reports `fired` from the hook that queues a message,
    /// `delivered` when the remote has taken one, and `failed` for
    /// every message it will not hand over: one the remote refused by
    /// reason code, one this bridge would not carry, and one whose
    /// payload has gone from the store. **It never dead letters**, and
    /// that is the connector rather than a counter nobody wired up: a
    /// bridge has no attempt limit to exhaust. It reconnects, and what
    /// it queued meanwhile is still queued.
    pub stats: Arc<crate::stats::RuleStats>,
}

impl Bridge {
    /// Count a message this bridge would not carry, and tell the
    /// hooks.
    ///
    /// Section 11.3 asks for both on every discard path and this one
    /// had neither: a message refused by the loop marker was a debug
    /// line and nothing else, which means section 9.2's second line of
    /// defence was invisible to an operator in exactly the situation
    /// it exists for.
    ///
    /// Delegated to [`publish::refuse`] rather than reaching for the
    /// metric here, so that a refusal a connector makes before the
    /// publish door and one the door itself makes land on the same
    /// series through the same call. The `qos` is the level the
    /// message would have gone at; the topic is the one it would have
    /// gone to.
    fn refused(&self, refusal: &Refused, topic: &str, qos: QoS) {
        publish::refuse(
            &self.shared,
            self.rule.as_str(),
            topic,
            qos,
            refusal.refusal(),
        );
    }

    /// The filters this bridge subscribes to on the remote, with the
    /// level each is wanted at.
    pub fn inbound_filters(&self) -> Vec<(String, QoS)> {
        self.rewrites
            .iter()
            .filter(|rewrite| rewrite.direction().inbound())
            .map(|rewrite| (rewrite.remote_filter().to_owned(), rewrite.qos()))
            .collect()
    }
}

/// Why an envelope is leaving this bridge's outbox.
///
/// **Every way out goes through [`Session::retire`]**, and this is
/// what it is told, because the three are not the same event and the
/// statistics an operator reads are where the difference shows. A
/// `retire` that counted a delivery whatever had happened drew a
/// healthy line for a bridge whose every message was being refused,
/// and that is the shape this enum exists to make impossible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Retired {
    /// The remote took responsibility for it.
    Delivered,
    /// It was refused: by the remote's reason code, or by this bridge
    /// before it ever went out.
    Refused,
    /// Its payload is gone from the store, so there was nothing to
    /// send and there is nothing to release.
    Orphaned,
}

/// Why an outbox entry produced no publish.
///
/// Three outcomes rather than an [`Option`], because what the entry is
/// owed differs: one leaves with its payload released, one leaves
/// without a release because the payload is what has gone, and one
/// decides nothing at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Declined {
    /// This bridge will not carry it. The entry leaves, released.
    Refused,
    /// The store no longer holds its payload.
    Orphaned,
    /// The store could not be read, so nothing is known about this
    /// entry and nothing is decided about it.
    Unreadable,
}

/// What one connection produced, for the supervisor's log line.
///
/// **This connection's, and that is the whole of what it is for.** It
/// resets on every reconnection, so it cannot answer "how much has
/// this bridge brought in" and must not be read as though it could.
/// The lifetime numbers are the rule's counters —
/// `RuleEvent::Received` and the `publications_refused` family — which
/// this struct is now counted beside rather than instead of. It was
/// the only record of either, which is why section 11.3's sentence was
/// false for the whole of this milestone's inbound path.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Carried {
    /// Messages handed to the remote and acknowledged by it.
    pub sent: u64,
    /// Messages taken from the remote and published here.
    pub received: u64,
    /// Messages this bridge refused to carry, in either direction.
    pub refused: u64,
}

/// One connection, from the CONNECT to the close.
pub struct Session<'a> {
    bridge: &'a Bridge,
    flow: &'a mut Flow,
    inbound: Inbound,
    aliases: Aliases,
    subs: Subscriptions,
    negotiated: Negotiated,
    /// The outbox sequence this end has read up to. Entries below it
    /// are either in flight or gone.
    cursor: u64,
    carried: Carried,
}

/// Connect, negotiate, and run the connection until it ends.
///
/// Answers what was negotiated — so the supervisor can reset its
/// backoff and remember the identifier the remote assigned — together
/// with how the connection ended.
///
/// **The state topic is published here rather than by the caller**,
/// because "connected" is only true while this function has not
/// returned. A caller that published it after the await would be
/// announcing a connection that had just ended.
pub async fn run<S>(
    bridge: &Bridge,
    stream: S,
    client_id: &str,
    clean_start: bool,
    flow: &mut Flow,
) -> (Option<Negotiated>, Carried, Ended)
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let version = bridge.action.version;
    let limits = Limits {
        max_packet_size: bridge.action.maximum_packet_size,
        ..Limits::default()
    };
    let (read, mut write) = tokio::io::split(stream);
    let mut reader = FrameReader::unmetered(read, limits);
    let mut out = BytesMut::new();

    let mut connect = connack::connect(&bridge.action, client_id);
    connect.clean_start = clean_start;
    if let Err(error) = send(
        &mut write,
        &mut out,
        &Packet::Connect(Box::new(connect)),
        version,
    )
    .await
    {
        return (None, Carried::default(), error);
    }

    // [MQTT-3.1.4-5]: the CONNACK comes first, and nothing else may be
    // sent until it has been read. A remote that accepts the socket and
    // says nothing is bounded here rather than for ever.
    let connack = match tokio::time::timeout(CONNACK_TIMEOUT, reader.next(version)).await {
        Err(_) => {
            return (
                None,
                Carried::default(),
                Ended::Io(format!(
                    "the remote accepted the connection and sent no connack within {}s",
                    CONNACK_TIMEOUT.as_secs()
                )),
            )
        }
        Ok(Err(error)) => return (None, Carried::default(), Ended::Io(error.to_string())),
        Ok(Ok(None)) => return (None, Carried::default(), Ended::Closed),
        Ok(Ok(Some(Packet::ConnAck(connack)))) => connack,
        Ok(Ok(Some(other))) => {
            return (
                None,
                Carried::default(),
                Ended::Protocol {
                    reason: ReasonCode::ProtocolError,
                    why: format!(
                        "the first packet from the remote was a {:?} and must be a connack",
                        other.kind()
                    ),
                },
            )
        }
    };

    let wanted: Vec<(String, QoS)> = bridge.inbound_filters();
    let asked = Asked {
        client_id: client_id.to_owned(),
        keep_alive_secs: bridge.action.keep_alive_secs,
        receive_maximum: bridge.action.receive_maximum,
        version,
    };
    let filters: Vec<String> = wanted.iter().map(|(filter, _)| filter.clone()).collect();
    let negotiated = match connack::negotiate(&connack, &asked, &filters) {
        Ok(negotiated) => negotiated,
        Err(error) => return (None, Carried::default(), Ended::ConnAck(error)),
    };

    tracing::info!(
        rule = bridge.rule.as_str(),
        client_id = %negotiated.client_id,
        session_present = negotiated.session_present,
        keep_alive_s = negotiated.keep_alive.map(|d| d.as_secs()),
        send_quota = negotiated.send_quota,
        maximum_packet_size = negotiated.maximum_packet_size,
        maximum_qos = ?negotiated.maximum_qos,
        retain_available = negotiated.retain_available,
        wildcard_available = negotiated.wildcard_available,
        outbound_alias_maximum = negotiated.outbound_alias_maximum,
        "the bridge is connected, and these are the limits the remote declared"
    );
    // **Beside the state topic, because they are the same fact said to
    // two audiences.** The topic is for a subscriber watching this
    // bridge; the counter is for an alert. It was a log line and
    // nothing else, which cannot be alerted on: a bridge that
    // reconnects every thirty seconds is working perfectly by every
    // other number this broker publishes.
    //
    // Here rather than in the supervisor, for the reason the state
    // topic is here: "connected" is only true from this point until
    // this function returns, and the supervisor sees only the
    // aftermath. It is also the difference between a fact a test can
    // reach over a pipe and one that needs a listener.
    bridge.stats.connected();
    state(bridge, ONLINE).await;

    // **The window belongs to the supervisor, not to this
    // connection.** It is what a resumed session resends, so a window
    // rebuilt per connection would make `Flow::resume` unreachable and
    // the "keeps its original identifier" rule untestable in anything
    // but a unit test. The limit is re-applied because the remote may
    // declare a different one on a resumed session.
    flow.set_limit(negotiated.send_quota);
    let mut session = Session {
        bridge,
        flow,
        inbound: Inbound::new(bridge.action.receive_maximum),
        aliases: Aliases::new(bridge.action.topic_alias_maximum),
        subs: Subscriptions::new(wanted),
        negotiated: negotiated.clone(),
        cursor: 0,
        carried: Carried::default(),
    };
    let ended = session.pump(&mut reader, &mut write, &mut out).await;
    // [MQTT-3.14.x]: a client that has found a protocol error says so
    // before it goes, so the operator on the far side reads the same
    // word this log does. Best effort — the socket is usually already
    // the reason we are here.
    if let Ended::Protocol { reason, .. } = &ended {
        let _ = send(
            &mut write,
            &mut out,
            &Packet::Disconnect(Disconnect {
                reason: *reason,
                properties: Properties::default(),
            }),
            version,
        )
        .await;
    }
    // Counted only on the path that counted a connect, so the two
    // pair: their difference is whether this bridge is up now. A
    // remote that accepts a socket and then refuses the CONNECT
    // returns above, having done neither.
    bridge.stats.disconnected();
    state(bridge, OFFLINE).await;
    (Some(negotiated), session.carried, ended)
}

/// Publish the bridge's state topic, if it has one.
///
/// Retained and at least-once, for the reason the will is: somebody who
/// subscribes after the fact must still learn what happened, and a
/// liveness message the broker may drop leaves a dashboard wrong for
/// ever.
///
/// The properties are deliberately empty rather than
/// [`crate::hops::originated`]. This is the bridge talking about
/// itself rather than carrying somebody's message, and a hop count on
/// it would count a journey that did not happen;
/// [`super::topics::check`] has already refused a state topic this
/// bridge would carry, so there is no loop for a count to bound.
pub async fn state(bridge: &Bridge, value: &'static [u8]) {
    if bridge.action.state_topic.is_empty() {
        return;
    }
    if let Err(error) = sdmqtt_server::publish::publish(
        &bridge.shared,
        Publication {
            topic: bridge.action.state_topic.clone(),
            payload: bytes::Bytes::from_static(value),
            qos: QoS::AtLeastOnce,
            retain: true,
            message_expiry_interval: None,
            user_properties: Vec::new(),
            origin: bridge.rule.as_str().to_owned(),
        },
    )
    .await
    {
        tracing::warn!(
            rule = bridge.rule.as_str(),
            topic = %bridge.action.state_topic,
            %error,
            "the bridge's state topic could not be published"
        );
    }
}

impl Session<'_> {
    /// Run the connection until something ends it.
    async fn pump<R, W>(
        &mut self,
        reader: &mut FrameReader<R>,
        write: &mut W,
        out: &mut BytesMut,
    ) -> Ended
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        let version = self.bridge.action.version;
        let mut keep = KeepAlive::new(self.negotiated.keep_alive, Instant::now());

        // **Before anything is sent.** A remote with no session for us
        // has none of our subscriptions either, and a bridge that
        // skipped this would sit on a healthy connection receiving
        // nothing for ever.
        if let Some(subscribe) = self.subs.on_connected(self.negotiated.session_present) {
            if let Err(error) = send(write, out, &subscribe, version).await {
                return error;
            }
            keep.sent(Instant::now());
        }
        if self.negotiated.session_present {
            // **The resend, with the original identifiers and the
            // duplicate flag, in the order sent.** [MQTT-4.4.0-1]. A
            // publish the remote has already taken ownership of comes
            // back as its release instead, because it holds the
            // message and is waiting to be told to let it go.
            let resumed = self.flow.resume();
            if !resumed.is_empty() {
                tracing::info!(
                    rule = self.bridge.rule.as_str(),
                    count = resumed.len(),
                    "the remote resumed our session, so what was in flight goes again \
                     under the identifiers it is still waiting on"
                );
            }
            for packet in resumed {
                if let Err(error) = send(write, out, &packet, version).await {
                    return error;
                }
                keep.sent(Instant::now());
            }
            // Everything at or below the highest token is on the wire
            // already. Re-reading the outbox from the start would send
            // it a second time under a second identifier, which is the
            // duplication this window exists to prevent.
            self.cursor = self.flow.highest_token().map_or(0, |token| token + 1);
        } else {
            // The remote discarded whatever it held, so nothing this
            // end thinks is in flight is. The entries go back to the
            // outbox cursor and are sent again with fresh identifiers,
            // which is correct exactly because the remote forgot them.
            self.flow.forget();
            self.cursor = 0;
        }

        loop {
            // Fill the window before waiting, so that a reconnection
            // with a backlog starts moving immediately.
            match self.drain_outbox(write, out, &mut keep).await {
                Ok(()) => {}
                Err(ended) => return ended,
            }

            let now = Instant::now();
            let wake = match keep.due(now) {
                Due::Dead => return Ended::KeepAliveLost,
                Due::Ping => {
                    if let Err(error) = send(write, out, &Packet::PingReq, version).await {
                        return error;
                    }
                    // `due` has already recorded the send; asking it
                    // again is what gives the deadline.
                    match keep.due(Instant::now()) {
                        Due::Idle(at) => at,
                        Due::Dead => return Ended::KeepAliveLost,
                        Due::Ping => now + OUTBOX_POLL,
                    }
                }
                Due::Idle(at) => at,
            }
            .min(now + OUTBOX_POLL);

            tokio::select! {
                packet = reader.next(version) => {
                    match packet {
                        Err(error) => return Ended::Io(error.to_string()),
                        Ok(None) => return Ended::Closed,
                        Ok(Some(packet)) => {
                            match self.received(packet, write, out, &mut keep).await {
                                Ok(()) => {}
                                Err(ended) => return ended,
                            }
                        }
                    }
                }
                () = self.bridge.wake.notified() => {}
                () = tokio::time::sleep_until(wake) => {}
            }
        }
    }

    /// Handle one packet from the remote.
    async fn received<W>(
        &mut self,
        packet: Packet,
        write: &mut W,
        out: &mut BytesMut,
        keep: &mut KeepAlive,
    ) -> Result<(), Ended>
    where
        W: AsyncWrite + Unpin,
    {
        let version = self.bridge.action.version;
        match packet {
            Packet::Publish(publish) => self.take_in(publish, write, out, keep).await,
            Packet::PubAck(ack) => {
                let finished = self.flow.on_puback(&ack).map_err(protocol)?;
                self.answered(finished).await;
                Ok(())
            }
            Packet::PubRec(ack) => {
                let (release, finished) = self.flow.on_pubrec(&ack).map_err(protocol)?;
                if let Some(release) = release {
                    send(write, out, &release, version).await?;
                    keep.sent(Instant::now());
                }
                if let Some(finished) = finished {
                    // The remote refused it, which is the only way an
                    // exchange ends here. The entry still leaves the
                    // outbox: [MQTT-4.3.3-x] says the publish must not
                    // be retransmitted, so retrying would be a protocol
                    // error dressed as persistence.
                    self.answered(finished).await;
                }
                Ok(())
            }
            Packet::PubComp(ack) => {
                let finished = self.flow.on_pubcomp(&ack).map_err(protocol)?;
                self.answered(finished).await;
                Ok(())
            }
            Packet::PubRel(ack) => {
                let answer = self.inbound.on_pubrel(&ack);
                send(write, out, &answer, version).await?;
                keep.sent(Instant::now());
                Ok(())
            }
            Packet::SubAck(suback) => {
                let granted = self.subs.on_suback(&suback, version).map_err(protocol)?;
                for one in granted {
                    if !one.accepted() {
                        // Section 9.2 wants a per-filter failure
                        // surfaced as a rule error rather than dropped.
                        // A whole direction of this bridge does not
                        // work, and nothing else would ever say so.
                        tracing::error!(
                            rule = self.bridge.rule.as_str(),
                            filter = %one.filter,
                            reason = ?one.reason,
                            "the remote refused one of this bridge's subscriptions; \
                             nothing will arrive on that filter"
                        );
                    } else if one.downgraded() {
                        tracing::warn!(
                            rule = self.bridge.rule.as_str(),
                            filter = %one.filter,
                            wanted = ?one.wanted,
                            granted = ?one.granted,
                            "the remote granted a lower level than this bridge asked for"
                        );
                    } else {
                        tracing::info!(
                            rule = self.bridge.rule.as_str(),
                            filter = %one.filter,
                            granted = ?one.granted,
                            "subscribed on the remote"
                        );
                    }
                }
                Ok(())
            }
            Packet::PingResp => {
                keep.pong();
                Ok(())
            }
            Packet::Disconnect(Disconnect { reason, .. }) => Err(Ended::Disconnected { reason }),
            // A server does not send these to a client, and one that
            // does has confused this end for something else.
            other => Err(Ended::Protocol {
                reason: ReasonCode::ProtocolError,
                why: format!("a server does not send a {:?} to a client", other.kind()),
            }),
        }
    }

    /// A publish from the remote, on its way into the local broker.
    async fn take_in<W>(
        &mut self,
        mut publish: Publish,
        write: &mut W,
        out: &mut BytesMut,
        keep: &mut KeepAlive,
    ) -> Result<(), Ended>
    where
        W: AsyncWrite + Unpin,
    {
        let version = self.bridge.action.version;
        // Before anything else, because everything else is about a
        // topic and this is what decides which topic it is.
        let topic = self
            .aliases
            .resolve(&mut publish)
            .map_err(|reason| Ended::Protocol {
                reason,
                why: "the remote used a topic alias this end cannot resolve".to_owned(),
            })?;
        publish.topic = topic.clone();

        let received = self
            .inbound
            .on_publish(&publish)
            .map_err(|reason| Ended::Protocol {
                reason,
                why: "the remote sent more unanswered publishes than this bridge \
                      advertised it would take"
                    .to_owned(),
            })?;

        if received.deliver {
            self.deliver(&publish, &topic).await;
        }
        if let Some(answer) = received.answer {
            send(write, out, &answer, version).await?;
            keep.sent(Instant::now());
        }
        Ok(())
    }

    /// Publish one inbound message into the local broker.
    ///
    /// # Two counters here are not guarded, and that is a deferral
    ///
    /// Convention 8: said out loud rather than left to be found.
    /// Deleting either `self.bridge.stats.received()` or the
    /// `self.bridge.refused(..)` beside the marker check below leaves
    /// the whole suite green — both mutations were run and both
    /// survived.
    ///
    /// The reason is the same for both: the in-process fixture in
    /// `rules/tests/bridge.rs` drives an **outbound-only** bridge over
    /// a pipe, which is where a test can read this rule's metric
    /// handles; and `sdmqtt/tests/bridge.rs`, which does move messages
    /// inbound between two real brokers, reaches neither broker's
    /// `/metrics` — it is given no admin port. The outbound halves of
    /// both counters are guarded, and the *logic* the inbound marker
    /// check performs is guarded by
    /// `a_message_this_bridge_sent_is_not_taken_back`. What is
    /// unguarded is the counting, on the inbound side only.
    ///
    /// Closing it means either an inbound fixture over a pipe or an
    /// admin port in the two-process test, and either is worth doing
    /// before a connector is added to this path.
    async fn deliver(&mut self, publish: &Publish, topic: &str) {
        let Some((rewrite, local)) = self.bridge.rewrites.iter().find_map(|rewrite| match rewrite
            .inbound(topic)
        {
            Rewritten::Topic(local) => Some((rewrite, local)),
            Rewritten::NoMatch => None,
            Rewritten::NotATopic { produced, why } => {
                tracing::warn!(
                    rule = self.bridge.rule.as_str(),
                    remote = topic,
                    produced,
                    why,
                    "an inbound rewrite produced something that is not a topic"
                );
                None
            }
        }) else {
            // The remote sent something no entry carries. Not an
            // error: a wildcard subscription legitimately matches more
            // than the rewrite covers, and a remote is free to publish
            // whatever it likes on a filter we asked for.
            tracing::debug!(
                rule = self.bridge.rule.as_str(),
                topic,
                "the remote sent a message no topic entry carries"
            );
            return;
        };

        let properties: Vec<(String, String)> = publish.properties.user_properties.clone();
        let carried = match marker::carry(
            &self.bridge.marker,
            self.bridge.action.max_hops,
            &properties,
        ) {
            Ok(carried) => carried,
            Err(refused) => {
                // **The marker, read on the way back in.** Without
                // this half the mark is a property nobody looks at.
                self.carried.refused += 1;
                self.bridge
                    .refused(&refused, topic, rewrite.qos().min(publish.qos));
                match refused {
                    Refused::Looped => tracing::debug!(
                        rule = self.bridge.rule.as_str(),
                        topic,
                        "this message has already been through this bridge and is not \
                         taken back"
                    ),
                    Refused::TooManyHops { hops, maximum } => tracing::warn!(
                        rule = self.bridge.rule.as_str(),
                        topic,
                        hops,
                        maximum,
                        "a message arrived having been through more connectors than \
                         this rule allows, and is dropped"
                    ),
                }
                return;
            }
        };

        let qos = rewrite.qos().min(publish.qos);
        match sdmqtt_server::publish::publish(
            &self.bridge.shared,
            Publication {
                topic: local.clone(),
                payload: publish.payload.clone(),
                qos,
                retain: publish.retain,
                message_expiry_interval: publish.properties.message_expiry_interval,
                user_properties: carried,
                origin: self.bridge.rule.as_str().to_owned(),
            },
        )
        .await
        {
            Ok(_) => {
                self.carried.received += 1;
                // The lifetime count, beside the per-connection one.
                // See [`Carried`]: that field resets on every
                // reconnection and answers a log line, and "how much
                // has this bridge brought in" had no answer at all.
                self.bridge.stats.received();
            }
            Err(refused @ (PublishRefused::NotATopic(_) | PublishRefused::Reserved)) => {
                self.carried.refused += 1;
                tracing::warn!(
                    rule = self.bridge.rule.as_str(),
                    topic = %local,
                    why = %refused,
                    "an inbound message could not be published here"
                );
            }
            Err(refused @ PublishRefused::NotDurable) => {
                self.carried.refused += 1;
                tracing::error!(
                    rule = self.bridge.rule.as_str(),
                    topic = %local,
                    why = %refused,
                    "an inbound message could not be made durable"
                );
            }
            // The node's limit, which is not this bridge's: `carry`
            // above has already applied `mqtt_bridge.max_hops` and
            // would have refused anything over it. Reaching this means
            // `broker.max_hops` is the lower of the two, which is a
            // configuration an operator is entitled to write and which
            // this counts as a refusal like any other.
            Err(refused @ PublishRefused::TooManyHops(_)) => {
                self.carried.refused += 1;
                tracing::warn!(
                    rule = self.bridge.rule.as_str(),
                    topic = %local,
                    why = %refused,
                    "an inbound message is over this broker's hop limit and is dropped"
                );
            }
        }
    }

    /// Send as much of the outbox as the remote's window allows.
    async fn drain_outbox<W>(
        &mut self,
        write: &mut W,
        out: &mut BytesMut,
        keep: &mut KeepAlive,
    ) -> Result<(), Ended>
    where
        W: AsyncWrite + Unpin,
    {
        let version = self.bridge.action.version;
        while self.flow.has_room() {
            let entry = match self
                .bridge
                .outbox
                .head(self.bridge.store.as_ref(), self.cursor)
            {
                Ok(Some(entry)) => entry,
                Ok(None) => return Ok(()),
                Err(error) => {
                    tracing::error!(
                        rule = self.bridge.rule.as_str(),
                        %error,
                        "this bridge's outbox could not be read"
                    );
                    return Ok(());
                }
            };
            self.cursor = entry.sequence + 1;

            let publish = match self.compose(&entry) {
                Ok(publish) => publish,
                // Not carried, for a reason already logged. The entry
                // leaves the outbox: keeping it would stall the bridge
                // behind a message it has decided not to send.
                Err(Declined::Refused) => {
                    self.retire(entry.sequence, Retired::Refused).await;
                    continue;
                }
                // The same, minus the release: see [`Retired`].
                Err(Declined::Orphaned) => {
                    self.retire(entry.sequence, Retired::Orphaned).await;
                    continue;
                }
                // **Nothing is known about this entry, so nothing is
                // decided about it.** A store that could not answer is
                // not a message this bridge has refused, and taking
                // the envelope out on the strength of one failed read
                // would throw away something this broker accepted. The
                // cursor has already moved past it, so the bridge is
                // not stalled behind it either: the next pass over the
                // outbox — this connection's or the next one's — reads
                // it again.
                Err(Declined::Unreadable) => continue,
            };

            if publish.qos == QoS::AtMostOnce {
                // Section 4.9 does not count these, and there is
                // nothing to wait for: the entry is done the moment the
                // bytes are written.
                send(write, out, &Packet::Publish(publish), version).await?;
                keep.sent(Instant::now());
                self.retire(entry.sequence, Retired::Delivered).await;
                self.carried.sent += 1;
                continue;
            }

            let Some(packet) = self.flow.send(publish, entry.sequence) else {
                // The window closed between the check and here, or the
                // identifier space is exhausted. Either way the entry
                // is still owed, so the cursor goes back.
                self.cursor = entry.sequence;
                return Ok(());
            };
            send(write, out, &packet, version).await?;
            keep.sent(Instant::now());
        }
        Ok(())
    }

    /// Turn one outbox entry into the publish that goes out, or
    /// decline it.
    fn compose(&mut self, entry: &OutboxRecord) -> Result<Publish, Declined> {
        let body = match self.bridge.store.body(&entry.body) {
            Ok(Some(body)) => body,
            Ok(None) => {
                // The payload is gone, so there is nothing to send and
                // no reconnection that would ever make it come back.
                // **[`Declined::Orphaned`] rather than a refusal**: the
                // store refuses a release of a body it does not hold
                // and a batch is applied in full or not at all, so an
                // entry retired with a completion record would leave
                // neither record behind — the envelope would still be
                // there on the next connection, and on every one after
                // it. `http_out` has the same case and the same
                // answer; see [`crate::Outbox::orphaned`].
                tracing::error!(
                    rule = self.bridge.rule.as_str(),
                    sequence = entry.sequence,
                    "an outbox entry named a payload the store no longer holds; it \
                     cannot be sent"
                );
                return Err(Declined::Orphaned);
            }
            Err(error) => {
                tracing::error!(
                    rule = self.bridge.rule.as_str(),
                    sequence = entry.sequence,
                    %error,
                    "an outbox entry's payload could not be read"
                );
                return Err(Declined::Unreadable);
            }
        };

        let Some(remote) =
            self.bridge
                .rewrites
                .iter()
                .find_map(|rewrite| match rewrite.outbound(&body.topic) {
                    Rewritten::Topic(remote) => Some(remote),
                    Rewritten::NoMatch => None,
                    Rewritten::NotATopic { produced, why } => {
                        tracing::warn!(
                            rule = self.bridge.rule.as_str(),
                            local = %body.topic,
                            produced,
                            why,
                            "an outbound rewrite produced something that is not a topic"
                        );
                        None
                    }
                })
        else {
            tracing::debug!(
                rule = self.bridge.rule.as_str(),
                topic = %body.topic,
                "an outbox entry matches no outbound topic entry"
            );
            self.carried.refused += 1;
            return Err(Declined::Refused);
        };

        let stored = broker_core::journal::live_properties(&body.properties);
        let carried = match marker::carry(
            &self.bridge.marker,
            self.bridge.action.max_hops,
            &stored.user_properties,
        ) {
            Ok(carried) => carried,
            Err(refused) => {
                // **The marker, set on the way out** — and, for a
                // message that came in through this same bridge,
                // refused here. Either half alone leaves the loop open.
                self.carried.refused += 1;
                self.bridge
                    .refused(&refused, &body.topic, live_qos(body.qos));
                match refused {
                    Refused::Looped => tracing::debug!(
                        rule = self.bridge.rule.as_str(),
                        topic = %body.topic,
                        "this message came in through this bridge and is not sent back"
                    ),
                    Refused::TooManyHops { hops, maximum } => tracing::warn!(
                        rule = self.bridge.rule.as_str(),
                        topic = %body.topic,
                        hops,
                        maximum,
                        "a message has been through more connectors than this rule \
                         allows, and is dropped"
                    ),
                }
                return Err(Declined::Refused);
            }
        };

        let qos = self.negotiated.allowed_qos(live_qos(body.qos));
        let publish = Publish {
            dup: false,
            qos,
            retain: self.negotiated.allowed_retain(body.retain),
            topic: remote,
            packet_id: None,
            payload: body.payload.clone(),
            properties: Properties {
                message_expiry_interval: stored.message_expiry_interval,
                content_type: stored.content_type.clone(),
                response_topic: stored.response_topic.clone(),
                correlation_data: stored.correlation_data.clone(),
                payload_format_indicator: stored.payload_format_indicator,
                user_properties: carried,
                ..Properties::default()
            },
        };

        // [MQTT-3.2.2-15]: a packet larger than the remote declared is
        // a protocol error it closes the connection for. There is
        // nothing to downgrade, so it is dropped and said out loud.
        if let Some(maximum) = self.negotiated.maximum_packet_size {
            let mut sized = BytesMut::new();
            // An identifier is added when the window takes it; a
            // placeholder here so the measurement is of the packet that
            // will actually go out rather than two bytes short of it.
            let mut measured = publish.clone();
            if measured.qos != QoS::AtMostOnce {
                measured.packet_id = Some(1);
            }
            if mqtt_codec::encode(
                &Packet::Publish(measured),
                self.bridge.action.version,
                &mut sized,
            )
            .is_err()
                || !self.negotiated.fits(sized.len())
            {
                tracing::warn!(
                    rule = self.bridge.rule.as_str(),
                    topic = %body.topic,
                    bytes = sized.len(),
                    maximum,
                    "a message is larger than the remote said it would accept, and is \
                     dropped; sending it would be a protocol error the remote closes for"
                );
                self.carried.refused += 1;
                return Err(Declined::Refused);
            }
        }

        Ok(publish)
    }

    /// One exchange the remote has answered, counted for what the
    /// answer said.
    ///
    /// **The reason code decides.** [MQTT-3.4.2-1] gives a PUBACK and
    /// a PUBCOMP a reason code, and 0x80 and above is a refusal: "not
    /// authorized" and "quota exceeded" are the ordinary answers from
    /// a broker with access control, and a bridge that counted them as
    /// deliveries would drop every message while its chart drew a flat
    /// healthy line. The entry leaves the outbox either way, because
    /// [MQTT-4.3.3-x] says a refused publish must not be
    /// retransmitted; what changes is which counter moves.
    async fn answered(&mut self, finished: Finished) {
        if finished.accepted {
            self.retire(finished.token, Retired::Delivered).await;
            self.carried.sent += 1;
            return;
        }
        tracing::warn!(
            rule = self.bridge.rule.as_str(),
            reason = ?finished.reason,
            "the remote refused a message; it is not retried"
        );
        self.retire(finished.token, Retired::Refused).await;
        self.carried.refused += 1;
    }

    /// Take one entry out of the outbox, and say what became of it.
    ///
    /// The one door out of this bridge's outbox, so that what an
    /// operator reads cannot disagree with what happened.
    async fn retire(&self, sequence: u64, outcome: Retired) {
        let Ok(Some(entry)) = self
            .bridge
            .outbox
            .head(self.bridge.store.as_ref(), sequence)
        else {
            return;
        };
        if entry.sequence != sequence {
            // Already gone, which a repeat acknowledgement can cause.
            return;
        }
        match outcome {
            Retired::Delivered => self.bridge.stats.delivered(),
            // A message this bridge accepted and will never hand over
            // is a failure, whether the remote refused it or its
            // payload went missing. The bridge has no attempt limit to
            // exhaust, so it never dead letters; see [`Bridge::stats`].
            Retired::Refused | Retired::Orphaned => self.bridge.stats.failed(),
        }
        let records: Vec<Record> = match outcome {
            // **No release.** The store refuses a release of a body it
            // does not hold, and the body is what has gone; a batch is
            // all or nothing, so a completion record here would leave
            // the envelope in the outbox for ever.
            Retired::Orphaned => self.bridge.outbox.orphaned(sequence),
            Retired::Delivered | Retired::Refused => self.bridge.outbox.completed(&entry),
        };
        if let Err(error) = self.bridge.writer.apply(records) {
            tracing::error!(
                rule = self.bridge.rule.as_str(),
                sequence,
                ?outcome,
                %error,
                "a finished message could not be taken out of its outbox"
            );
        }
    }
}

/// A stored level as the broker spells it.
fn live_qos(qos: storage::record::Qos) -> QoS {
    match qos {
        storage::record::Qos::AtMostOnce => QoS::AtMostOnce,
        storage::record::Qos::AtLeastOnce => QoS::AtLeastOnce,
        storage::record::Qos::ExactlyOnce => QoS::ExactlyOnce,
    }
}

/// Whatever went wrong, as a protocol error to close with.
fn protocol(error: impl std::fmt::Display) -> Ended {
    Ended::Protocol {
        reason: ReasonCode::ProtocolError,
        why: error.to_string(),
    }
}

/// Write one packet.
///
/// The buffer is reused across calls, so a bridge carrying a thousand
/// messages a second allocates once.
async fn send<W>(
    write: &mut W,
    out: &mut BytesMut,
    packet: &Packet,
    version: Version,
) -> Result<(), Ended>
where
    W: AsyncWrite + Unpin,
{
    out.clear();
    if let Err(error) = mqtt_codec::encode(packet, version, out) {
        // Convention 1 from the inside: this end will not put on the
        // wire what its own decoder would reject.
        return Err(Ended::Protocol {
            reason: ReasonCode::MalformedPacket,
            why: format!(
                "this bridge composed a {:?} it cannot encode: {error}",
                packet.kind()
            ),
        });
    }
    write
        .write_all(out)
        .await
        .map_err(|error| Ended::Io(error.to_string()))
}
