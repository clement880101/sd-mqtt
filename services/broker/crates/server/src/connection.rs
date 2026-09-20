//! One connection, from its first packet to its last.
//!
//! Three things the broker deliberately leaves undone are done here,
//! because all three belong to a connection rather than to a session:
//! the connect acknowledgement, the send window narrowed to what the
//! client said it can receive, and the receive maximum we impose on the
//! client. Section 4.9 says the last two are "not preserved across
//! Network Connections", which is precisely why they are not the
//! broker's to keep.
//!
//! # The obligation
//!
//! A caller that finishes with a session **must** call the broker's
//! disconnect, for any reason including an error. Closing the socket is
//! not enough: a session that believes it is still attached never
//! expires and never fires its will. [`serve`] discharges it by
//! dropping the value that represents the attachment, so no early
//! return, and no panic, can skip it.
//!
//! # The lock
//!
//! Every use of the broker here is a closure passed to
//! [`Shared::with_broker`] that neither awaits nor writes. The guard is
//! not `Send`, so holding one across an await would not compile, but
//! that is a backstop rather than the rule: the rule is that the span is
//! as short as the work and everything written goes out after it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use broker_core::broker::{Broker, BrokerConfig};
use broker_core::registry::SessionId;
use broker_core::session::Action;
use broker_core::subscriptions::SubscriberId;
use bytes::BytesMut;
use mqtt_codec::packet::{ConnAck, Connect, Disconnect};
use mqtt_codec::{encode, CodecError, Limits, Packet, Properties, ReasonCode, Version};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use crate::framing::{FrameError, FrameReader};
use crate::shared::{promises_durability, Shared};

/// What a client's receive maximum is when it does not say.
///
/// Section 3.1.2.11.3: "If the Receive Maximum value is absent then its
/// value defaults to 65,535." Zero is a protocol error rather than a
/// synonym for unlimited, which matters because the window this feeds
/// reads zero as unbounded.
const ABSENT_RECEIVE_MAXIMUM: u16 = 65_535;

/// Headroom above the largest burst one session can produce, for the
/// acknowledgements, closes and drop reports that share an outbox with
/// a queue drain.
const OUTBOX_SLACK: usize = 64;

/// What a client is told when its publication could not be made
/// durable.
///
/// # Why a close, and why this code
///
/// Section 7.3 leaves two honest answers and rules out the other two.
/// Silence leaves a publisher holding a message it believes is in
/// flight, with nothing to act on until its own timeout. A success is
/// the lie the whole milestone exists to prevent. What is left is a
/// close or a negative acknowledgement, and this is a close.
///
/// A negative acknowledgement is defensible and was not chosen, for
/// three reasons. MQTT 3.1.1 has no reason code on a PUBACK at all, so
/// half this broker's clients would receive a packet indistinguishable
/// from success — the outcome that is not defensible, reached by
/// accident. The failure is not about one message either: a store that
/// would not take this batch has already stopped holding what the
/// broker believes it holds, and every later publication on the same
/// connection would be answered the same way. And a close is what
/// makes the protocol's own recovery run at once, because an
/// unacknowledged publication is resent on the next connection, which
/// is exactly the behaviour wanted.
///
/// 0x83 rather than 0x80: 3.14.2.1 permits both on a DISCONNECT, and
/// "implementation specific error" is what this is. The packet was
/// valid and the protocol was followed; this particular server could
/// not write it down. An unspecified error says less about a failure
/// that is entirely specifiable.
///
/// A 3.1.1 client gets no packet, because 3.1.1 has no server-sent
/// disconnect — but it does get the close, which is still neither
/// silence nor a success.
const NOT_DURABLE: ReasonCode = ReasonCode::ImplementationSpecificError;

/// How deep a session's outbox must be for the queue and window it
/// drains, or `None` when those bounds are too large for one to be
/// sized at all.
///
/// The formula, rather than a constant, because both of its inputs are
/// an operator's to set and a constant beside a configurable value is a
/// broken invariant waiting to be written in a file.
///
/// `Outbound::pump` sends while the window has room and an at-most-once
/// dispatch consumes no slot, so a single acknowledgement can flush an
/// entire queue into one batch. A resume puts the window's contents
/// back at the head of that queue, bypassing its bounds, so the most
/// one batch can carry is the queue bound plus the window, and the
/// slack covers what travels beside it.
///
/// Undersizing it is not a slow reader: `Shared::deliver` discards
/// whatever does not fit against a counter, with no reason and no
/// `Action::Dropped`, which section 6 forbids. Worse, a discarded
/// publish at an acknowledged level has its inflight entry already
/// recorded, so the window slot is held for an acknowledgement that
/// can never arrive.
pub fn outbox_depth_for(max_queue_messages: usize, max_inflight: u16) -> Option<usize> {
    max_queue_messages
        .checked_add(usize::from(max_inflight))?
        .checked_add(OUTBOX_SLACK)
}

/// How a connection is configured.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Bounds handed to the codec.
    pub limits: Limits,
    /// How many actions may wait for the writer before the shell starts
    /// dropping them.
    pub outbox_depth: usize,
    /// How long a client has to send its connect packet.
    pub connect_timeout: Duration,
    /// How many quality-of-service 1 and 2 publications we are willing
    /// to process concurrently from one client.
    pub receive_maximum: u16,
    /// How many topic aliases one client may have in use at once.
    ///
    /// Sixteen is a default and not a discovery: nothing was measured.
    /// It is enough for the handful of topics a device publishes to,
    /// and small enough that the table costs nothing per connection.
    /// Zero would tell a client to send none at all, which 3.3.2.3.4
    /// permits and which is what this broker did before it had a
    /// table.
    pub topic_alias_maximum: u16,
    /// The namespace this listener's clients live in, empty for the
    /// default. Two clients with the same identifier on two mountpoints
    /// are two sessions.
    pub mountpoint: Arc<str>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            // Sized for the queue and window a session has by default,
            // through the same formula `Config::server` uses on the
            // configured ones. It was a flat 1024 written against a
            // thousand-message queue, which stopped being a bound the
            // moment that queue became configurable.
            outbox_depth: outbox_depth_for(
                BrokerConfig::default().max_queue_messages,
                BrokerConfig::default().max_inflight,
            )
            .expect("the default bounds size an outbox"),
            connect_timeout: Duration::from_secs(10),
            // Finite, so a client cannot make us hold unbounded
            // exactly-once state, and far above any sensible client's
            // pipelining depth, so it never throttles one.
            receive_maximum: 1024,
            topic_alias_maximum: 16,
            mountpoint: Arc::from(""),
        }
    }
}

/// Why a connection ended.
///
/// Every variant is an ending rather than a fault to recover from: the
/// caller logs it and lets the task finish.
#[derive(Debug, Error)]
pub enum ConnectionError {
    /// The client connected and then said nothing.
    #[error("no connect packet arrived within {0:?}")]
    ConnectTimeout(Duration),
    /// The first packet was something other than a connect. There is no
    /// session yet, so there is nothing to address a reason to.
    #[error("the first packet was not a connect")]
    NotAConnect,
    /// The client sent something that is not a valid packet, or the
    /// connection failed.
    #[error(transparent)]
    Frame(#[from] FrameError),
    /// Something we produced could not be encoded.
    #[error("codec error: {0}")]
    Codec(#[from] CodecError),
    /// The socket failed while writing.
    #[error("connection error: {0}")]
    Io(#[from] std::io::Error),
    /// The client broke the protocol and was told why.
    #[error("protocol error: {0:?}")]
    Protocol(ReasonCode),
    /// A change this connection was about to be acknowledged for could
    /// not be made durable, so it was closed instead. See
    /// [`NOT_DURABLE`].
    #[error("not made durable: {0}")]
    NotDurable(#[source] storage::NotDurable),
}

/// Serve one connection until it ends, whatever ends it.
///
/// The broker is told about the departure on every path out of here,
/// including every error. See the module documentation for why that is
/// not optional.
pub async fn serve<S>(
    socket: S,
    shared: Arc<Shared>,
    config: ConnectionConfig,
) -> Result<(), ConnectionError>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    serve_with_candidates(socket, shared, config, candidate_identifier).await
}

/// The same, with the source of assigned client identifiers as a
/// parameter.
///
/// A seam, and it exists for one reason: a clash between a nanosecond
/// timestamp and a process-wide counter cannot be waited for, so the
/// refusal that follows every candidate being taken is unreachable
/// from a test that goes through [`serve`]. [`assign_identifier`] takes
/// the same parameter for the same reason and is tested on its own; the
/// refusal is a separate piece of code and this is what lets a test
/// walk it.
async fn serve_with_candidates<S>(
    socket: S,
    shared: Arc<Shared>,
    config: ConnectionConfig,
    mut candidates: impl FnMut() -> String + Send,
) -> Result<(), ConnectionError>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (read_half, write_half) = tokio::io::split(socket);
    let mut reader = FrameReader::new(read_half, config.limits);
    let mut write_half = write_half;

    // Nothing below this point is owed to the broker: it does not know
    // this connection exists yet, so these returns are free.
    let mut connect = match first_packet(&mut reader, config.connect_timeout).await {
        Ok(Some(connect)) => connect,
        Ok(None) => return Ok(()),
        Err(error) => {
            if let ConnectionError::Frame(FrameError::Protocol(CodecError::UnsupportedVersion(_))) =
                &error
            {
                // Section 2, and both standards: a protocol level we do
                // not serve is answered rather than dropped. MQTT 5
                // wants 0x84 and 3.1.1 return code 1, and [MQTT-3.1.4-5]
                // requires the acknowledgement even though the server
                // then closes the connection.
                //
                // Answered in 3.1.1 form because the version is
                // precisely what could not be parsed, so there is none
                // to encode in. A refusing CONNACK is the same four
                // bytes in 3.1, 3.1.1 and 5, and the return code for
                // this refusal is 1 in both of the older versions, so
                // whichever level the client named it can read the
                // answer. A 5 client would rather have 0x84, but a
                // client that reached this path did not claim to be
                // one. Bytes we could not parse at all still get
                // nothing, which is all there is to give them.
                //
                // A failure to write is discarded rather than returned:
                // the socket is ending either way, and the version is
                // the ending worth reporting.
                let _ = refuse(
                    &mut write_half,
                    Version::V311,
                    ReasonCode::UnsupportedProtocolVersion,
                )
                .await;
            }
            return Err(error);
        }
    };
    let version = connect.version;

    if connect.client_id.is_empty() && version == Version::V311 && !connect.clean_start {
        // [MQTT-3.1.3-8]: in 3.1.1 the pair is illegal, because there is
        // no property to tell the client which session it was given, so
        // it could never resume the one it is asking to keep.
        refuse(
            &mut write_half,
            version,
            ReasonCode::ClientIdentifierNotValid,
        )
        .await?;
        return Err(ConnectionError::Protocol(
            ReasonCode::ClientIdentifierNotValid,
        ));
    }

    let window = match connect.properties.receive_maximum {
        // Section 3.1.2.11.3 makes zero a protocol error. It is refused
        // here rather than clamped, because the window it feeds reads
        // zero as "no limit" and a silent inversion of a limit is worse
        // than a refusal.
        Some(0) => {
            refuse(&mut write_half, version, ReasonCode::ProtocolError).await?;
            return Err(ConnectionError::Protocol(ReasonCode::ProtocolError));
        }
        Some(advertised) => usize::from(advertised),
        None => usize::from(ABSENT_RECEIVE_MAXIMUM),
    };

    let maximum_packet_size = match connect.properties.maximum_packet_size {
        // Section 3.1.2.11.4 makes zero a protocol error, exactly as
        // 3.1.2.11.3 does for the receive maximum above, and for the
        // same reason: a limit of zero forbids every packet including
        // the acknowledgement, so honouring it literally and refusing
        // it come to the same thing. Refused, so the client is told.
        Some(0) => {
            refuse(&mut write_half, version, ReasonCode::ProtocolError).await?;
            return Err(ConnectionError::Protocol(ReasonCode::ProtocolError));
        }
        // Absent means unlimited: 3.1.2.11.4 says "If the Maximum
        // Packet Size is not present, no limit on the packet size is
        // imposed beyond the limitations in the protocol".
        other => other,
    };

    // From here the broker knows about this connection and is owed a
    // disconnect.
    //
    // One lock span for everything it must do before the client hears
    // anything. The identifier is checked for a clash and used under the
    // same guard, so nothing can claim it in between; the two flow
    // control limits are set there too, so no publish from another
    // connection can overshoot a window not yet narrowed.
    let proposed_keep_alive = connect.keep_alive;
    let registered = shared.with_broker(|broker| {
        // Section 5.4 lists server keep-alive as in scope. The interval
        // we will hold this client to is the lesser of what it proposed
        // and the configured maximum, and [MQTT-3.2.2-21] makes a
        // Server Keep Alive in the connack the value the client must
        // use instead of its own. Clamped here,
        // before `connect`, so that the ring is armed with the number
        // the acknowledgement is about to advertise: the whole point of
        // sending the property is that the two agree, and a clamp
        // applied anywhere else could drift from it.
        //
        // Two cases are deliberately left alone.
        //
        // A proposal of zero asks for no keep-alive at all rather than
        // for a very long one, so there is no maximum for it to exceed.
        // The standard would permit imposing one, but that is a
        // different decision from bounding an interval a client did
        // choose, and only the second is a maximum.
        //
        // A 3.1.1 client is left alone because 3.1.1 has no property to
        // tell it with. Clamping there would produce exactly the defect
        // this guards against, a client silent for its own interval and
        // disconnected at ours, with no way to say so.
        let maximum = broker.config().max_keep_alive;
        if version == Version::V5 && connect.keep_alive > maximum && maximum > 0 {
            connect.keep_alive = maximum;
        }

        // [MQTT-3.1.3-6]: a zero length identifier is a special case and
        // the server assigns a unique one, then processes the connect as
        // if the client had sent it [MQTT-3.1.3-7].
        let assigned = if connect.client_id.is_empty() {
            let assigned = assign_identifier(broker, &config.mountpoint, &mut candidates)?;
            connect.client_id.clone_from(&assigned);
            Some(assigned)
        } else {
            None
        };

        let mut out = Vec::new();
        let registration = broker.connect(&config.mountpoint, &connect, &mut out);
        let configured = usize::from(broker.config().max_inflight);
        if let Some(session) = broker.session_mut(registration.subscriber) {
            // Section 4.9: what the client can receive bounds what we
            // may have outstanding towards it, and what we advertise
            // bounds what it may have outstanding towards us. Two
            // directions, two settings, neither of them session state.
            //
            // Section 6: the lesser of the two. An absent Receive
            // Maximum means 65535 [3.1.2.11.3], so assigning the peer's
            // value would quietly replace the configured window with
            // the protocol's ceiling. A 3.1.1 client advertises nothing
            // at all, which is the same absence by another road and
            // lands on the same number; that is why this is not guarded
            // on the version.
            //
            // Set on every connect rather than only on a fresh session,
            // because a resumed one carries the window the last
            // connection narrowed it to and this one may advertise
            // something else entirely.
            session.outbound.set_inflight_limit(window.min(configured));
            session
                .inbound
                .set_receive_maximum(usize::from(config.receive_maximum));
        }
        Some((registration, out, assigned))
    });

    let Some((registration, actions, assigned)) = registered else {
        // Every candidate identifier was taken. Nothing was registered,
        // so nothing is owed; the client is told rather than dropped.
        refuse(
            &mut write_half,
            version,
            ReasonCode::ClientIdentifierNotValid,
        )
        .await?;
        return Err(ConnectionError::Protocol(
            ReasonCode::ClientIdentifierNotValid,
        ));
    };

    let attached = Attached {
        shared: Arc::clone(&shared),
        subscriber: registration.subscriber,
        epoch: registration.epoch,
        version,
        maximum_packet_size,
        topic_alias_maximum: config.topic_alias_maximum,
    };

    // A takeover addresses two connections by one handle, and the
    // broker's rule is that everything up to and including the close
    // belongs to the incumbent and everything after it to us. So the
    // incumbent's share goes out while its outbox is still the
    // registered one, and only then do we claim the handle.
    let mine = if registration.took_over {
        let mut actions = actions;
        match actions
            .iter()
            .position(|(_, action)| matches!(action, Action::Close(_)))
        {
            Some(at) => {
                let mine = actions.split_off(at + 1);
                shared.deliver(actions);
                mine
            }
            None => actions,
        }
    } else {
        actions
    };
    let outbox = shared.register_outbox(attached.subscriber, config.outbox_depth);
    shared.deliver(mine);

    // [MQTT-3.2.2-22]: an absent Server Keep Alive means the client's
    // own value stands, so the property is sent when and only when the
    // clamp above changed it. Sending it unchanged is noise the client
    // must still parse and act on.
    let server_keep_alive =
        (connect.keep_alive != proposed_keep_alive).then_some(connect.keep_alive);
    let ack = acknowledgement(&registration, assigned, &config, server_keep_alive);
    // The inherited obligation is discharged by dropping `attached`,
    // which happens on every path out of here including a panic. It is
    // deliberately not a call at the end of this function: the mutation
    // step of this task showed that a call can be skipped by turning one
    // `await` into an `await?`, and that the obligation's own test could
    // not see it happen.
    attached.run(&mut reader, write_half, ack, outbox).await
}

/// Read the connect packet, or decide there will not be one.
///
/// `Ok(None)` means the client closed without saying anything, which is
/// a port scan or a health check rather than a fault.
async fn first_packet<R: AsyncRead + Unpin>(
    reader: &mut FrameReader<R>,
    timeout: Duration,
) -> Result<Option<Connect>, ConnectionError> {
    // The version is not known until the connect has been decoded, and
    // the decoder ignores the one passed to it for that packet.
    let first = tokio::time::timeout(timeout, reader.next(Version::V311))
        .await
        .map_err(|_| ConnectionError::ConnectTimeout(timeout))??;

    match first {
        None => Ok(None),
        Some(Packet::Connect(connect)) => Ok(Some(*connect)),
        // [MQTT-3.2.0-1] lets a server close without a connack, and
        // there is no session to address a reason to in any case.
        Some(_) => Err(ConnectionError::NotAConnect),
    }
}

/// Refuse a connection with a reason, before any session exists.
///
/// Section 4.13: a server that rejects a connect sends a connack
/// carrying the reason and then closes the connection.
async fn refuse<W: AsyncWrite + Unpin>(
    writer: &mut W,
    version: Version,
    reason: ReasonCode,
) -> Result<(), ConnectionError> {
    let ack = Packet::ConnAck(ConnAck {
        // [MQTT-3.2.2-6]: a non-zero reason code carries a zero session
        // present flag.
        session_present: false,
        reason,
        properties: Properties::default(),
    });
    let mut buf = BytesMut::new();
    encode(&ack, version, &mut buf)?;
    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

/// Build the acknowledgement.
///
/// What it carries, and what it deliberately does not:
///
/// - The session present flag reports whether state was resumed rather
///   than created [MQTT-3.2.2-2], [MQTT-3.2.2-3].
/// - The assigned client identifier is mandatory when the client sent a
///   zero length one [MQTT-3.2.2-16], and is how it learns what it was
///   given.
/// - The receive maximum is what we are willing to process concurrently
///   from this client. A client cannot honour a limit it was never told,
///   and the default it would otherwise assume is 65,535.
/// - The maximum packet size is sent because ours is not the protocol's
///   default of unlimited. A client told nothing would learn our limit
///   only by being disconnected for exceeding it.
/// - The topic alias maximum is how many aliases this client may have
///   in use at once. 3.3.2.3.4 says a client that is told nothing must
///   send none at all, which is what this broker used to say and why
///   every alias a client did send was lost.
/// - The server keep-alive is the interval we will hold the client to,
///   and is present only when it is not the one the client proposed.
///   [MQTT-3.2.2-21] makes it the value the client must use instead of
///   its own, and [MQTT-3.2.2-22] makes its absence mean the client's
///   value stands, so a client within the configured maximum is told
///   nothing rather than told its own number back. The caller decides,
///   because only the caller knows what was proposed.
/// - Everything else is omitted because it equals its default: we
///   support retained messages, both higher qualities of service,
///   wildcards, subscription identifiers and shared subscriptions, and
///   we do not shorten the session expiry the client asked for.
fn acknowledgement(
    registration: &broker_core::registry::Registration,
    assigned: Option<String>,
    config: &ConnectionConfig,
    server_keep_alive: Option<u16>,
) -> ConnAck {
    ConnAck {
        session_present: registration.session_present,
        reason: ReasonCode::Success,
        properties: Properties {
            assigned_client_identifier: assigned,
            receive_maximum: Some(config.receive_maximum),
            maximum_packet_size: Some(config.limits.max_packet_size),
            topic_alias_maximum: Some(config.topic_alias_maximum),
            server_keep_alive,
            ..Properties::default()
        },
    }
}

/// A client identifier for a client that sent none, held by nobody.
///
/// [MQTT-3.2.2-16] requires one "not used by any other Session currently
/// in the Server", and the registry is the only thing that knows. The
/// consequence of skipping the question is not a duplicate name:
/// `Broker::connect` would read the newcomer as that client returning
/// and take the session over, so an unrelated client is disconnected and
/// its state handed away. Rare, invisible, and catastrophic for whoever
/// hits it, which is the argument for asking rather than reasoning about
/// the odds.
///
/// `candidates` is a parameter because a clash cannot be waited for: it
/// is how a test seeds one. `None` when every attempt clashed, which
/// means something is wrong with the candidates rather than with the
/// broker, and the connection is refused.
fn assign_identifier(
    broker: &Broker,
    mountpoint: &str,
    mut candidates: impl FnMut() -> String,
) -> Option<String> {
    // Bounded because this runs under the broker's lock, where retrying
    // for as long as it takes would stall every other connection.
    (0..ASSIGNMENT_ATTEMPTS).find_map(|_| {
        let candidate = candidates();
        broker
            .subscriber_of(&SessionId::new(mountpoint, &candidate))
            .is_none()
            .then_some(candidate)
    })
}

/// How many identifiers to try before giving up.
const ASSIGNMENT_ATTEMPTS: usize = 8;

/// One identifier that is probably free.
///
/// A process-wide counter cannot collide with itself, the timestamp
/// separates one run of the process from the next, and the prefix
/// separates the whole space from identifiers a client would plausibly
/// choose. It is a candidate rather than an answer because none of that
/// is a proof, and [`assign_identifier`] asks.
fn candidate_identifier() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let serial = NEXT.fetch_add(1, Ordering::Relaxed);
    let started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos() as u64);
    format!("auto-{started:016x}-{serial:x}")
}

/// The topic aliases one client has set on this connection.
///
/// 3.3.2.3.4: "Topic Alias mappings exist only within a Network
/// Connection and last only for the lifetime of that Network
/// Connection." So this belongs to the connection and not to the
/// session: it is rebuilt on every reconnect without anyone having to
/// remember to clear it, and a resumed session inherits nothing.
///
/// A vector rather than a map, because the maximum is small, known at
/// connect time, and bounded by us rather than by the client. Its
/// length is the whole of the bounds check: an alias is legal exactly
/// when it indexes a slot, so there is no second limit that could
/// disagree with the first. It is allocated on the first alias rather
/// than on every connection, because most clients send none and this
/// broker is sized for a great many connections.
struct TopicAliases {
    /// What the acknowledgement advertised, and therefore how many
    /// slots there are once there are any.
    maximum: u16,
    /// Indexed by alias minus one. Empty until the first alias.
    table: Vec<Option<Arc<str>>>,
}

impl TopicAliases {
    /// An empty table for a client told it may use `maximum` aliases.
    fn new(maximum: u16) -> Self {
        Self {
            maximum,
            table: Vec::new(),
        }
    }

    /// The slot `alias` names, or `None` when it names none.
    fn slot(&mut self, alias: u16) -> Option<&mut Option<Arc<str>>> {
        if self.table.is_empty() {
            self.table = vec![None; usize::from(self.maximum)];
        }
        // Alias one is the first slot. Alias zero names none, which
        // the codec refuses before this is reached; `checked_sub`
        // makes that a refusal here too rather than a wrap.
        self.table.get_mut(usize::from(alias).checked_sub(1)?)
    }

    /// Resolve an inbound publish's alias, and strip it.
    ///
    /// The strip is what makes the retained store and every subscriber
    /// correct without any of them knowing that aliases exist. An
    /// alias means nothing to a connection that did not set it, and
    /// 3.1.2.11.5 forbids sending one to a client that advertised no
    /// maximum of its own; every subscriber is a different connection
    /// from the publisher, so the only safe place to do this is here,
    /// once, on ingress.
    fn apply(&mut self, packet: &mut Packet) -> Result<(), ReasonCode> {
        let Packet::Publish(publish) = packet else {
            return Ok(());
        };
        let Some(alias) = publish.properties.topic_alias.take() else {
            return Ok(());
        };
        let Some(slot) = self.slot(alias) else {
            return Err(ReasonCode::TopicAliasInvalid);
        };
        if publish.topic.is_empty() {
            // An alias standing in for a topic name. One nothing has
            // mapped names no topic at all; reading it as the empty
            // topic would publish to one.
            let Some(topic) = slot.as_deref() else {
                return Err(ReasonCode::TopicAliasInvalid);
            };
            publish.topic = topic.to_owned();
        } else {
            // A topic and an alias: the mapping is set, replacing
            // whatever this alias meant before.
            *slot = Some(Arc::from(publish.topic.as_str()));
        }
        Ok(())
    }
}

/// A connection the broker knows about, and therefore owes a disconnect.
struct Attached {
    shared: Arc<Shared>,
    subscriber: SubscriberId,
    /// Which claim on the handle is ours. A takeover raises it, and a
    /// connection whose claim has been superseded must not tear down the
    /// session it no longer owns.
    epoch: u64,
    version: Version,
    /// The largest packet this client said it will accept, and `None`
    /// when it said nothing. Per connection, not per session: 3.1.2.11.4
    /// is a property of the CONNECT, so a resumed session inherits
    /// nothing from the limit the last connection advertised.
    maximum_packet_size: Option<u32>,
    /// How many topic aliases this client was told it may use. Per
    /// connection for the same reason: 3.3.2.3.4 ends a mapping with
    /// the network connection that made it.
    topic_alias_maximum: u16,
}

impl Attached {
    /// Acknowledge, start writing, and read until the connection ends.
    async fn run<R, W>(
        &self,
        reader: &mut FrameReader<R>,
        mut write_half: W,
        ack: ConnAck,
        outbox: mpsc::Receiver<Action>,
    ) -> Result<(), ConnectionError>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        // [MQTT-3.2.0-1]: the acknowledgement precedes every other
        // packet. It is written here rather than queued because a resumed
        // session's redeliveries are already in the outbox and would
        // otherwise overtake it.
        let mut buf = BytesMut::new();
        encode(&Packet::ConnAck(ack), self.version, &mut buf)?;
        write_half.write_all(&buf).await?;
        write_half.flush().await?;

        let (gone, mut writer_gone) = oneshot::channel();
        let version = self.version;
        let limit = SizeLimit {
            limit: self.maximum_packet_size,
            shared: Arc::clone(&self.shared),
            subscriber: self.subscriber,
            epoch: self.epoch,
        };
        tokio::spawn(async move {
            write_loop(write_half, outbox, version, limit).await;
            drop(gone);
        });

        self.read_loop(reader, &mut writer_gone).await
    }

    /// Hand each arriving packet to the broker and deliver what comes
    /// back.
    async fn read_loop<R: AsyncRead + Unpin>(
        &self,
        reader: &mut FrameReader<R>,
        writer_gone: &mut oneshot::Receiver<()>,
    ) -> Result<(), ConnectionError> {
        let mut aliases = TopicAliases::new(self.topic_alias_maximum);
        loop {
            let mut packet = tokio::select! {
                // The writer stops when its outbox closes, which is how
                // a displaced connection learns it has been replaced, or
                // when it has written a close. Either way there is
                // nothing left to read for.
                _ = &mut *writer_gone => return Ok(()),
                next = reader.next(self.version) => match next {
                    Ok(Some(packet)) => packet,
                    Ok(None) => return Ok(()),
                    // A malformed packet is the client's fault and it
                    // is owed the reason: the decoder found it, so
                    // nothing above has had the chance to report it.
                    // An I/O failure is not reportable, because the
                    // socket is what failed.
                    Err(FrameError::Protocol(error)) => {
                        let reason = reportable(error.reason_code());
                        self.close_with(reason);
                        return Err(ConnectionError::Frame(FrameError::Protocol(error)));
                    }
                    Err(other) => return Err(other.into()),
                },
            };

            if matches!(packet, Packet::Connect(_)) {
                // [MQTT-3.2.0-2] forbids a second acknowledgement, and a
                // second connect is a protocol error. There is a session
                // now, so it is told why before the socket closes.
                self.close_with(ReasonCode::ProtocolError);
                return Err(ConnectionError::Protocol(ReasonCode::ProtocolError));
            }
            // Before the broker sees it, so that nothing below this
            // shell ever meets an alias. Refused by the same
            // `close_with` path as the second connect above, so the
            // client is told rather than dropped.
            if let Err(reason) = aliases.apply(&mut packet) {
                self.close_with(reason);
                return Err(ConnectionError::Protocol(reason));
            }
            let said_goodbye = matches!(packet, Packet::Disconnect(_));

            // Section 7.3, and the ordering this milestone exists
            // for. The journal leaves under the lock that filled it;
            // whether it is waited for is decided from what came out
            // beside it, and the wait happens here, after the lock is
            // released and before a single action is delivered.
            let ((outcome, actions), records) = self.shared.with_broker_journalled(|broker| {
                let mut out = Vec::new();
                let outcome = broker.handle(self.subscriber, &packet, &mut out);
                (outcome, out)
            });

            if promises_durability(&actions) {
                if let Err(error) = self.shared.commit_durable(records).await {
                    // Not acknowledged, and not silently. See
                    // `not_durable` for why the answer is a close.
                    tracing::error!(
                        %error,
                        subscriber = self.subscriber.0,
                        "a publication could not be made durable; the client is closed \
                         rather than acknowledged"
                    );
                    self.close_with(NOT_DURABLE);
                    return Err(ConnectionError::NotDurable(error));
                }
            } else {
                self.shared.commit(records);
            }
            self.shared.deliver(actions);

            if let Err(reason) = outcome {
                self.close_with(reason);
                return Err(ConnectionError::Protocol(reason));
            }
            if said_goodbye {
                return Ok(());
            }
        }
    }

    /// Tell the client why it is being closed, through its own outbox so
    /// that it lands behind whatever is already queued for it.
    fn close_with(&self, reason: ReasonCode) {
        self.shared
            .deliver(vec![(self.subscriber, Action::Close(reason))]);
    }

    /// Tell the broker the connection is gone. The obligation.
    ///
    /// Called from [`Drop`] rather than from a line at the end of
    /// [`serve`], so that no early return, and no panic, can skip it.
    ///
    /// Silent when a newcomer has taken the handle over: the session is
    /// now that connection's, and disconnecting it here would detach a
    /// live client and arm a will nobody is owed. The same for the
    /// outbox, which by then is the newcomer's.
    fn detach(&self) {
        let (actions, ours) = self.shared.with_broker(|broker| {
            let superseded = broker
                .session(self.subscriber)
                .is_some_and(|session| session.epoch() != self.epoch);
            let mut out = Vec::new();
            if !superseded {
                broker.disconnect(self.subscriber, &mut out);
            }
            (out, !superseded)
        });
        self.shared.deliver(actions);
        if ours {
            self.shared.remove_outbox(self.subscriber);
        }
    }
}

impl Drop for Attached {
    fn drop(&mut self) {
        self.detach();
    }
}

/// Write what the broker addresses to this connection, until there is
/// nothing left to write.
///
/// Ends when the outbox closes, which happens when the connection is
/// torn down or displaced, or when an action says to close. The socket's
/// write half is shut down either way, so a client that has been
/// displaced or timed out learns it from the stream rather than only
/// from a packet it might never read.
async fn write_loop<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut outbox: mpsc::Receiver<Action>,
    version: Version,
    limit: SizeLimit,
) {
    let mut buf = BytesMut::new();
    while let Some(action) = outbox.recv().await {
        buf.clear();
        let last = render(&action, version, &mut buf);
        if limit.exceeded(buf.len()) {
            // [MQTT-3.1.2-24]: not one byte of it goes out. The size
            // is checked here rather than in the broker because this
            // is where the packet exists and its length is a fact
            // rather than an estimate.
            buf.clear();
            limit.discard(&action);
        }
        if !buf.is_empty() && writer.write_all(&buf).await.is_err() {
            break;
        }
        if last {
            break;
        }
    }
    let _ = writer.flush().await;
    let _ = writer.shutdown().await;
}

/// The client's Maximum Packet Size, and the way back into the broker
/// for a packet that exceeds it.
///
/// # The lock on the write path
///
/// [`SizeLimit::discard`] takes the broker lock, and the write path
/// otherwise never touches it. That is deliberate, not an oversight:
/// the benchmark in this crate found the single lock to be this
/// broker's throughput ceiling, so the next person reading this path
/// will be counting acquisitions. This one is on the oversize branch
/// only. A connection that advertised no limit never reaches it, a
/// connection that advertised one reaches it only for packets it has
/// refused in advance to read, and in both cases nothing that is
/// actually written costs a lock.
///
/// The alternative was to size the packet in `broker-core` before
/// building it, under a lock that is already held. That needs either a
/// second size calculation that would drift from the encoder or a
/// throwaway encode, and both are paid by every message rather than by
/// the discarded ones.
struct SizeLimit {
    /// What the client advertised, `None` when it advertised nothing.
    limit: Option<u32>,
    shared: Arc<Shared>,
    subscriber: SubscriberId,
    /// Which claim on the session is ours. A writer can still be
    /// draining actions it was handed before a newcomer took the
    /// session over, and settling a delivery that now belongs to the
    /// newcomer would free an identifier the newcomer is using.
    epoch: u64,
}

impl SizeLimit {
    /// Whether an encoded packet is larger than the client allows.
    ///
    /// Strictly larger. 3.1.2.11.4 gives "the Maximum Packet Size the
    /// Client is willing to accept", so a packet of exactly that size
    /// is one the client accepts, and refusing it would discard a
    /// message the protocol says to deliver.
    fn exceeded(&self, len: usize) -> bool {
        self.limit
            .is_some_and(|limit| len as u64 > u64::from(limit))
    }

    /// Complete the delivery of a packet that was discarded for size.
    ///
    /// [MQTT-3.1.2-25]. Only a publish carrying a packet identifier
    /// has anything to complete: quality of service zero owes no
    /// acknowledgement, and no other packet holds an inflight slot.
    fn discard(&self, action: &Action) {
        let Action::Send(Packet::Publish(publish)) = action else {
            return;
        };
        let Some(packet_id) = publish.packet_id else {
            return;
        };
        tracing::debug!(
            packet_id,
            topic = %publish.topic,
            limit = self.limit,
            "a publication was larger than the client allows"
        );
        // See the type's documentation for why a lock acquisition here
        // is deliberate.
        let actions = self.shared.with_broker(|broker| {
            let ours = broker
                .session(self.subscriber)
                .is_some_and(|session| session.epoch() == self.epoch);
            let mut out = Vec::new();
            if ours {
                let now = broker.clock().now();
                broker.discard_oversize(self.subscriber, packet_id, now, &mut out);
            }
            out
        });
        self.shared.deliver(actions);
    }
}

/// Narrow a reason to one a DISCONNECT may carry.
///
/// Every code the codec reports is in Table 3-10 under 3.14.2.1 except
/// unsupported protocol version, which 2.4 lists for CONNACK alone. It
/// is reachable here: a client that has already been acknowledged and
/// then sends a second CONNECT naming a level we do not serve is
/// refused by the decoder with exactly that error. Sent as it stands,
/// the encoder would refuse the disconnect, the writer would log and
/// write nothing, and the client would learn no more than it did
/// before this path existed. It is a protocol error like any other
/// second CONNECT, so that is what it is told.
///
/// Which codes a DISCONNECT may carry is the shell's business rather
/// than the error's, which is why this is here and not in the codec:
/// the same error answers a CONNECT with a CONNACK, where 0x84 is the
/// right and only answer.
fn reportable(reason: ReasonCode) -> ReasonCode {
    match reason {
        ReasonCode::UnsupportedProtocolVersion => ReasonCode::ProtocolError,
        other => other,
    }
}

/// Turn one action into bytes, reporting whether it ends the connection.
fn render(action: &Action, version: Version, buf: &mut BytesMut) -> bool {
    match action {
        Action::Send(packet) => {
            if let Err(error) = encode(packet, version, buf) {
                // The encoder refusing what the broker produced is a
                // fault in this process, not in the peer, and a partly
                // written packet would corrupt the stream.
                tracing::error!(%error, kind = ?packet.kind(), "an outgoing packet was refused");
                buf.clear();
            }
            false
        }
        Action::Dropped {
            id,
            reason,
            qos,
            topic,
        } => {
            tracing::debug!(?id, ?reason, ?qos, %topic, "a message was discarded");
            false
        }
        Action::Close(reason) => {
            // MQTT 3.1.1 has no server-sent disconnect, so there the
            // close is the whole message.
            if version == Version::V5 {
                let packet = Packet::Disconnect(Disconnect {
                    reason: *reason,
                    properties: Properties::default(),
                });
                if let Err(error) = encode(&packet, version, buf) {
                    tracing::error!(%error, ?reason, "a disconnect was refused");
                    buf.clear();
                }
            }
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_core::broker::BrokerConfig;
    use bytes::BytesMut;
    use mqtt_codec::packet::Connect;
    use mqtt_codec::{decode, encode, Limits, Packet, Properties, QoS, ReasonCode, Version};
    use std::sync::Arc;
    use std::time::Duration;
    use storage::Writer;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::shared::gated::GatedStore;

    fn connect(client_id: &str, keep_alive: u16) -> Connect {
        Connect {
            version: Version::V5,
            client_id: client_id.into(),
            clean_start: true,
            keep_alive,
            username: None,
            password: None,
            will: None,
            properties: Properties::default(),
        }
    }

    /// Drive one connection over an in-memory socket, returning the
    /// client's half so a test can write packets and read replies.
    fn serve_one(shared: Arc<Shared>) -> tokio::io::DuplexStream {
        serve_one_with_config(shared, ConnectionConfig::default())
    }

    /// The same, for a test whose subject is a configured limit. The
    /// defaults are far above anything a test can reach in a reasonable
    /// number of packets, so a limit that has to be exercised rather
    /// than only read has to be lowered first.
    fn serve_one_with_config(
        shared: Arc<Shared>,
        config: ConnectionConfig,
    ) -> tokio::io::DuplexStream {
        let (client, server) = tokio::io::duplex(8192);
        tokio::spawn(async move {
            let _ = serve(server, shared, config).await;
        });
        client
    }

    async fn write(client: &mut tokio::io::DuplexStream, packet: &Packet, version: Version) {
        let mut buf = BytesMut::new();
        encode(packet, version, &mut buf).unwrap();
        client.write_all(&buf).await.unwrap();
    }

    async fn read(client: &mut tokio::io::DuplexStream, version: Version) -> Packet {
        let mut buf = BytesMut::new();
        read_buffered(client, &mut buf, version).await
    }

    /// The next reply, decoded from a buffer that outlives the call.
    ///
    /// Two packets can arrive in one read, and [`read`] starts from an
    /// empty buffer every time, so it decodes the first and discards
    /// whatever followed it in the same chunk. That is invisible to a
    /// test that reads one reply per request, and it makes a test that
    /// waits for two consecutive publications hang on the second.
    async fn read_buffered(
        client: &mut tokio::io::DuplexStream,
        buf: &mut BytesMut,
        version: Version,
    ) -> Packet {
        loop {
            if let Some(packet) = decode(buf, version, &Limits::default()).unwrap() {
                return packet;
            }
            let mut chunk = [0u8; 1024];
            let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut chunk))
                .await
                .expect("a reply arrives")
                .unwrap();
            assert_ne!(n, 0, "the connection closed instead of replying");
            buf.extend_from_slice(&chunk[..n]);
        }
    }

    #[tokio::test]
    async fn a_connect_is_acknowledged() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(Arc::clone(&shared));

        write(
            &mut client,
            &Packet::Connect(Box::new(connect("c", 0))),
            Version::V5,
        )
        .await;
        match read(&mut client, Version::V5).await {
            Packet::ConnAck(ack) => {
                assert_eq!(ack.reason, ReasonCode::Success);
                assert!(!ack.session_present, "a clean start resumes nothing");
            }
            other => panic!("expected an acknowledgement, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_client_with_no_identifier_is_told_the_one_it_was_given() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(shared);

        write(
            &mut client,
            &Packet::Connect(Box::new(connect("", 0))),
            Version::V5,
        )
        .await;
        match read(&mut client, Version::V5).await {
            Packet::ConnAck(ack) => {
                let assigned = ack
                    .properties
                    .assigned_client_identifier
                    .expect("the client must learn its identifier");
                assert!(!assigned.is_empty());
            }
            other => panic!("expected an acknowledgement, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn anything_before_a_connect_closes_the_connection() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(shared);

        write(&mut client, &Packet::PingReq, Version::V311).await;
        let mut chunk = [0u8; 16];
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut chunk))
            .await
            .expect("the server reacts")
            .unwrap();
        assert_eq!(n, 0, "the connection is closed, not answered");
    }

    /// A CONNECT naming a protocol level, built by hand.
    ///
    /// By hand because the encoder will not produce one: it writes the
    /// level from a `Version`, and there is no `Version` for a level we
    /// do not serve. That is the same absence the refusal has to work
    /// around, seen from the writing side.
    fn connect_at_level(level: u8) -> Vec<u8> {
        connect_naming("MQTT", level)
    }

    /// The same, naming a protocol of the caller's choosing.
    fn connect_naming(name: &str, level: u8) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(
            &u16::try_from(name.len())
                .expect("a short name")
                .to_be_bytes(),
        );
        body.extend_from_slice(name.as_bytes());
        body.push(level);
        // Clean session, no will, no credentials.
        body.push(0x02);
        // Keep alive.
        body.extend_from_slice(&[0x00, 0x00]);
        // Client identifier "c".
        body.extend_from_slice(&[0x00, 0x01, b'c']);
        let mut out = vec![0x10, u8::try_from(body.len()).expect("a short packet")];
        out.extend_from_slice(&body);
        out
    }

    #[tokio::test]
    async fn an_unserved_protocol_version_is_refused_out_loud() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(shared);

        // Level 3 is MQTT 3.1, which section 2 says is "rejected with an
        // unacceptable protocol version response".
        client.write_all(&connect_at_level(3)).await.unwrap();

        // Asserted as bytes rather than as a decoded reason, because the
        // code is the whole content of the answer. That a packet arrived
        // is the easy half and proves nothing: a CONNACK carrying
        // Success would satisfy it while telling the client it had been
        // accepted. 0x20 0x02 is the header of a two byte CONNACK, 0x00
        // the session present flag that a failure must carry
        // [MQTT-3.2.2-6], and 0x01 the 3.1.1 return code for an
        // unacceptable protocol version.
        let mut reply = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut reply))
            .await
            .expect("the refusal arrives")
            .expect("the socket carries it rather than closing");
        assert_eq!(
            reply,
            [0x20, 0x02, 0x00, 0x01],
            "a connack refusing the version, not silence and not an acceptance"
        );

        // [MQTT-3.1.4-5]: the acknowledgement is required even though
        // the server then closes the connection.
        let mut rest = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut rest))
            .await
            .expect("the socket closes")
            .unwrap();
        assert_eq!(n, 0, "nothing follows the refusal");
    }

    #[tokio::test]
    async fn a_real_mqtt_three_one_client_reaches_the_refusal() {
        // The client section 2's promise was written about. A 3.1
        // client sends the protocol name `MQIsdp`, which the decoder
        // rejected as malformed before it ever read the level, so the
        // refusal above was unreachable by the one client that needed
        // it.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(shared);

        client
            .write_all(&connect_naming("MQIsdp", 3))
            .await
            .unwrap();

        // The same four bytes the refusal above asserts, and asserted
        // as bytes for the same reason: the return code is the whole
        // content of the answer. 3.1 and 3.1.1 spell a CONNACK
        // identically, and return code 1 is "unacceptable protocol
        // version" in both, so a 3.1 client can read this.
        let mut reply = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut reply))
            .await
            .expect("the refusal arrives")
            .expect("the socket carries it rather than closing");
        assert_eq!(
            reply,
            [0x20, 0x02, 0x00, 0x01],
            "a connack refusing the version, not silence and not an acceptance"
        );
    }

    #[tokio::test]
    async fn a_connect_that_is_merely_malformed_is_not_blamed_on_its_version() {
        // The other side of the boundary, and the reason it is here: a
        // mutation widening the refusal to every protocol error the
        // decoder can raise survived the whole suite. A client whose
        // CONNECT is garbled would then be told the one thing that was
        // not wrong with it, and 3.1.4 lets a server close without a
        // CONNACK when it cannot validate the packet at all.
        //
        // [MQTT-3.1.2-1] is why the protocol name is the case chosen:
        // an incorrect one may be disconnected, and it is the nearest
        // neighbour of the refusal above.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(shared);

        let mut bytes = connect_at_level(4);
        // "MQTT" becomes "MQTX", leaving every other field valid. The
        // name is the two length bytes and four characters that follow
        // the two byte fixed header, so its last character is here.
        bytes[7] = b'X';
        client.write_all(&bytes).await.unwrap();

        let mut chunk = [0u8; 16];
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut chunk))
            .await
            .expect("the server reacts")
            .unwrap();
        assert_eq!(
            n,
            0,
            "closed in silence, not answered with a version refusal: got {:?}",
            &chunk[..n]
        );
    }

    #[tokio::test]
    async fn a_second_connect_is_a_protocol_error() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(shared);

        write(
            &mut client,
            &Packet::Connect(Box::new(connect("c", 0))),
            Version::V5,
        )
        .await;
        let _ = read(&mut client, Version::V5).await;

        write(
            &mut client,
            &Packet::Connect(Box::new(connect("c", 0))),
            Version::V5,
        )
        .await;
        let mut chunk = [0u8; 64];
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut chunk))
            .await
            .expect("the server reacts")
            .unwrap();
        assert_ne!(n, 0, "a reason is given before closing");
    }

    /// A publish whose topic alias is zero, written as bytes because
    /// the encoder refuses to produce one (convention 1), so a test
    /// cannot reach the decoder's check through `encode`.
    ///
    /// Section 3.3: 0x30 is a publish at quality of service zero, then
    /// a remaining length of nine, a two byte topic length and "a/b",
    /// a property length of three, and property 0x23 (topic alias)
    /// carrying the two byte value zero. 3.3.2.3.4 makes that a
    /// protocol error.
    const PUBLISH_WITH_A_ZERO_ALIAS: &[u8] = &[
        0x30, 0x09, 0x00, 0x03, b'a', b'/', b'b', 0x03, 0x23, 0x00, 0x00,
    ];

    /// A connect naming protocol level six, which this broker does not
    /// serve. Sent after a connect has already been acknowledged, it is
    /// the one way a decoder's unsupported-version error is reached
    /// from the read loop rather than from the first packet.
    ///
    /// 0x10 and a remaining length of seven, then the protocol name
    /// "MQTT" and the level byte. The decoder rejects the level before
    /// it reads anything further, so nothing follows it.
    const CONNECT_AT_LEVEL_SIX: &[u8] = &[0x10, 0x07, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x06];

    #[tokio::test]
    async fn a_decode_error_is_reported_before_the_socket_closes() {
        // The decoder finds this one, so nothing above it has had the
        // chance to report it, and before this task the socket simply
        // closed: an MQTT 5 client had no way to learn what it sent
        // that was wrong.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(shared);

        write(
            &mut client,
            &Packet::Connect(Box::new(connect("c", 0))),
            Version::V5,
        )
        .await;
        let _ = read(&mut client, Version::V5).await;

        client.write_all(PUBLISH_WITH_A_ZERO_ALIAS).await.unwrap();

        match read(&mut client, Version::V5).await {
            Packet::Disconnect(disconnect) => assert_eq!(
                disconnect.reason,
                ReasonCode::ProtocolError,
                "3.3.2.3.4 is a protocol error, and the code is the point"
            ),
            other => panic!("expected a disconnect carrying the reason, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_reason_no_disconnect_may_carry_is_reported_as_a_protocol_error() {
        // Table 3-10 has no unsupported protocol version: 0x84 is a
        // connack code. Sending it here would be refused by the
        // encoder, the writer would log and write nothing, and the
        // client would be back to learning nothing at all -- the very
        // defect this task fixes, in the one corner that reaches it.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(shared);

        write(
            &mut client,
            &Packet::Connect(Box::new(connect("c", 0))),
            Version::V5,
        )
        .await;
        let _ = read(&mut client, Version::V5).await;

        client.write_all(CONNECT_AT_LEVEL_SIX).await.unwrap();

        match read(&mut client, Version::V5).await {
            Packet::Disconnect(disconnect) => {
                assert_eq!(disconnect.reason, ReasonCode::ProtocolError)
            }
            other => panic!("expected a disconnect carrying the reason, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_ping_is_answered() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(shared);

        write(
            &mut client,
            &Packet::Connect(Box::new(connect("c", 0))),
            Version::V5,
        )
        .await;
        let _ = read(&mut client, Version::V5).await;

        write(&mut client, &Packet::PingReq, Version::V5).await;
        assert_eq!(read(&mut client, Version::V5).await, Packet::PingResp);
    }

    #[tokio::test]
    async fn a_subscriber_receives_what_another_client_publishes() {
        // The end-to-end shape of the whole product, over a socket.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut subscriber = serve_one(Arc::clone(&shared));
        let mut publisher = serve_one(Arc::clone(&shared));

        write(
            &mut subscriber,
            &Packet::Connect(Box::new(connect("sub", 0))),
            Version::V5,
        )
        .await;
        let _ = read(&mut subscriber, Version::V5).await;
        write(
            &mut subscriber,
            &Packet::Subscribe(mqtt_codec::packet::Subscribe {
                packet_id: 1,
                filters: vec![mqtt_codec::packet::SubscriptionOptions {
                    filter: "a/+".into(),
                    qos: QoS::AtMostOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: mqtt_codec::packet::RetainHandling::DoNotSend,
                }],
                properties: Properties::default(),
            }),
            Version::V5,
        )
        .await;
        assert!(matches!(
            read(&mut subscriber, Version::V5).await,
            Packet::SubAck(_)
        ));

        write(
            &mut publisher,
            &Packet::Connect(Box::new(connect("pub", 0))),
            Version::V5,
        )
        .await;
        let _ = read(&mut publisher, Version::V5).await;
        write(
            &mut publisher,
            &Packet::Publish(mqtt_codec::packet::Publish {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: false,
                topic: "a/b".into(),
                packet_id: None,
                payload: bytes::Bytes::from_static(b"hello"),
                properties: Properties::default(),
            }),
            Version::V5,
        )
        .await;

        match read(&mut subscriber, Version::V5).await {
            Packet::Publish(publish) => {
                assert_eq!(publish.topic, "a/b");
                assert_eq!(&publish.payload[..], b"hello");
            }
            other => panic!("expected the publication, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_dropped_connection_detaches_the_session() {
        // The obligation inherited from the broker: closing the socket is
        // not enough. A session left attached never expires and never
        // fires its will.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(Arc::clone(&shared));

        write(
            &mut client,
            &Packet::Connect(Box::new(connect("c", 0))),
            Version::V5,
        )
        .await;
        let _ = read(&mut client, Version::V5).await;
        drop(client);

        // Give the connection task a moment to notice and detach.
        for _ in 0..50 {
            let attached = shared.with_broker(|broker| {
                broker
                    .session(broker_core::subscriptions::SubscriberId(0))
                    .is_some_and(|s| s.is_connected())
            });
            if !attached {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the session was left attached after the socket closed");
    }

    #[tokio::test]
    async fn a_connection_that_ends_in_an_error_detaches_the_session_too() {
        // Convention 5, found by the mutation step of this task: the
        // test above walks only the clean close, where the reader
        // reports the end rather than a fault. Making the error path
        // return without detaching therefore changed nothing it could
        // see, and a truncated stream, a malformed packet and a protocol
        // error are the common ways a real connection ends. This walks
        // that path: half a packet, then silence.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(Arc::clone(&shared));

        write(
            &mut client,
            &Packet::Connect(Box::new(connect("c", 0))),
            Version::V5,
        )
        .await;
        let _ = read(&mut client, Version::V5).await;

        client.write_all(&[0x30, 0x10]).await.unwrap();
        drop(client);

        for _ in 0..50 {
            let attached = shared.with_broker(|broker| {
                broker
                    .session(broker_core::subscriptions::SubscriberId(0))
                    .is_some_and(|s| s.is_connected())
            });
            if !attached {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the session was left attached after the connection failed");
    }

    #[test]
    fn an_identifier_a_live_session_holds_is_not_handed_out_again() {
        // Seeded, because a clash between a nanosecond timestamp and a
        // counter cannot be waited for. What it guards is not a
        // duplicate name: the broker reads a repeated identifier as that
        // client returning, so handing one out would take an unrelated
        // client's session over and disconnect it.
        let shared = Shared::new(BrokerConfig::default());
        shared.with_broker(|broker| {
            let mut out = Vec::new();
            broker.connect("", &connect("taken", 0), &mut out);
        });

        let mut offered = ["taken", "free"].into_iter();
        let assigned = shared
            .with_broker(|broker| assign_identifier(broker, "", || offered.next().unwrap().into()));

        assert_eq!(assigned.as_deref(), Some("free"));
    }

    #[test]
    fn an_assignment_that_never_finds_a_free_identifier_gives_up() {
        // The loop runs under the broker's lock, so it is bounded. This
        // is the path that refuses the connection rather than spinning.
        let shared = Shared::new(BrokerConfig::default());
        shared.with_broker(|broker| {
            let mut out = Vec::new();
            broker.connect("", &connect("taken", 0), &mut out);
        });

        let mut attempts = 0;
        let assigned = shared.with_broker(|broker| {
            assign_identifier(broker, "", || {
                attempts += 1;
                "taken".into()
            })
        });

        assert_eq!(assigned, None);
        assert_eq!(attempts, ASSIGNMENT_ATTEMPTS);
    }

    #[test]
    fn an_identifier_taken_on_another_mountpoint_is_free_here() {
        // A handle is an identity, and an identity is a mountpoint and
        // an identifier. Asking without the mountpoint would refuse
        // names that are not in use on this listener at all.
        let shared = Shared::new(BrokerConfig::default());
        shared.with_broker(|broker| {
            let mut out = Vec::new();
            broker.connect("tenant-a", &connect("taken", 0), &mut out);
        });

        let assigned =
            shared.with_broker(|broker| assign_identifier(broker, "tenant-b", || "taken".into()));

        assert_eq!(assigned.as_deref(), Some("taken"));
    }

    /// The reason a CONNECT was answered with, whatever it was.
    async fn connack_reason(client: &mut tokio::io::DuplexStream, version: Version) -> ReasonCode {
        match read(client, version).await {
            Packet::ConnAck(ack) => ack.reason,
            other => panic!("expected an acknowledgement, got {other:?}"),
        }
    }

    /// Connect over a fresh connection and report how it was answered.
    async fn answered(shared: &Arc<Shared>, packet: Connect) -> ReasonCode {
        let version = packet.version;
        let mut client = serve_one(Arc::clone(shared));
        write(&mut client, &Packet::Connect(Box::new(packet)), version).await;
        connack_reason(&mut client, version).await
    }

    /// A 3.1.1 connect with no client identifier, keeping or
    /// discarding whatever session it names.
    fn anonymous_v311(clean_session: bool) -> Connect {
        let mut packet = connect("", 0);
        packet.version = Version::V311;
        packet.clean_start = clean_session;
        packet
    }

    #[tokio::test]
    async fn a_three_one_one_connect_with_no_identifier_and_clean_session_clear_is_refused() {
        // [MQTT-3.1.3-8]: "If the Client supplies a zero-byte
        // ClientId with CleanSession set to 0, the Server MUST respond
        // to the CONNECT Packet with a CONNACK return code 0x02
        // (Identifier rejected) and then close the Network
        // Connection." 3.1.1 has no Assigned Client Identifier
        // property, so a client given one could never name the
        // session it is asking to keep.
        //
        // Nothing else in this module sends a 3.1.1 CONNECT at all,
        // which is how the whole refusal went unguarded.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        assert_eq!(
            answered(&shared, anonymous_v311(false)).await,
            ReasonCode::ClientIdentifierNotValid,
            "return code 2 over 3.1.1"
        );
    }

    #[tokio::test]
    async fn a_three_one_one_connect_with_no_identifier_and_clean_session_set_is_accepted() {
        // The other side of the same condition, and the half that
        // keeps the refusal from becoming a ban. 3.1.1's
        // [MQTT-3.1.3-7] is the rule the refusal enforces, "If the
        // Client supplies a zero-byte ClientId, the Client MUST also
        // set CleanSession to 1", and a client that obeys it is served
        // with an identifier the server assigns [MQTT-3.1.3-6].
        // Dropping the `!clean_start` term refuses every anonymous
        // 3.1.1 client and passes the test above.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        assert_eq!(
            answered(&shared, anonymous_v311(true)).await,
            ReasonCode::Success,
            "an anonymous 3.1.1 client that keeps nothing is served"
        );
    }

    /// A version 5 connect advertising `receive_maximum`.
    fn advertising(receive_maximum: u16) -> Connect {
        let mut packet = connect("c", 0);
        packet.properties.receive_maximum = Some(receive_maximum);
        packet
    }

    #[tokio::test]
    async fn a_zero_receive_maximum_is_a_protocol_error() {
        // 3.1.2.11.3: "It is a Protocol Error to include the Receive
        // Maximum value more than once or for it to have the value 0."
        //
        // Refused rather than clamped, and that is the whole point:
        // the window it feeds reads zero as no limit at all, so a
        // silent clamp would turn the strictest limit a client can ask
        // for into the loosest. Its sibling, a Maximum Packet Size of
        // zero, has a test; this one had none, and a clamp here passed
        // the suite.
        //
        // On the boundary: one is the smallest legal value and must
        // still be served, so a check widened to `<= 1` fails here.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        assert_eq!(
            answered(&shared, advertising(0)).await,
            ReasonCode::ProtocolError,
            "zero is refused, not read as unbounded"
        );
        assert_eq!(
            answered(&shared, advertising(1)).await,
            ReasonCode::Success,
            "one is the strictest legal window, not an error"
        );
    }

    /// Drive one connection whose assigned identifiers come from
    /// `candidates` rather than from the real generator.
    fn serve_one_with_candidates(
        shared: Arc<Shared>,
        candidates: impl FnMut() -> String + Send + 'static,
    ) -> tokio::io::DuplexStream {
        let (client, server) = tokio::io::duplex(8192);
        tokio::spawn(async move {
            let _ = serve_with_candidates(server, shared, ConnectionConfig::default(), candidates)
                .await;
        });
        client
    }

    #[tokio::test]
    async fn a_connect_is_refused_when_every_candidate_identifier_is_taken() {
        // [MQTT-3.2.2-16] wants an identifier "not used by any other
        // Session currently in the Server", and the search for one is
        // bounded because it runs under the broker's lock. This is
        // what happens when it comes back empty: the client is told,
        // rather than dropped or handed a name that would take an
        // unrelated session over.
        //
        // Seeded through the candidate seam, because a clash between a
        // nanosecond timestamp and a counter cannot be waited for.
        // `assign_identifier` returning `None` is tested directly; the
        // refusal is a different piece of code and this is the only
        // way to reach it.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        shared.with_broker(|broker| {
            let mut out = Vec::new();
            broker.connect("", &connect("taken", 0), &mut out);
        });

        let mut client = serve_one_with_candidates(Arc::clone(&shared), || "taken".into());
        write(
            &mut client,
            &Packet::Connect(Box::new(connect("", 0))),
            Version::V5,
        )
        .await;
        assert_eq!(
            connack_reason(&mut client, Version::V5).await,
            ReasonCode::ClientIdentifierNotValid,
            "the client is told, not dropped"
        );

        // And the session that owned the name is untouched: the
        // refusal must not have taken it over on the way past. This is
        // the consequence the whole search exists to prevent, so
        // asserting only the reason code would leave it unguarded.
        assert_eq!(
            shared.with_broker(|broker| broker.session_count()),
            1,
            "nothing was registered for the refused connection"
        );
        assert!(
            shared
                .with_broker(|broker| broker
                    .subscriber_of(&broker_core::registry::SessionId::new("", "taken")))
                .is_some(),
            "the session holding the name is still there"
        );
    }

    #[tokio::test]
    async fn a_connect_is_served_when_a_candidate_identifier_is_free() {
        // The boundary of the search, over a real connection: the
        // second candidate is free and the client is given it. Without
        // this, refusing every anonymous client passes the test above.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        shared.with_broker(|broker| {
            let mut out = Vec::new();
            broker.connect("", &connect("taken", 0), &mut out);
        });

        let mut offered = ["taken", "free"].into_iter();
        let mut client =
            serve_one_with_candidates(Arc::clone(&shared), move || offered.next().unwrap().into());
        write(
            &mut client,
            &Packet::Connect(Box::new(connect("", 0))),
            Version::V5,
        )
        .await;
        match read(&mut client, Version::V5).await {
            Packet::ConnAck(ack) => {
                assert_eq!(ack.reason, ReasonCode::Success);
                assert_eq!(
                    ack.properties.assigned_client_identifier.as_deref(),
                    Some("free"),
                    "the name that was free, and the client is told which"
                );
            }
            other => panic!("expected an acknowledgement, got {other:?}"),
        }
    }

    /// Connect as `client_id` with the given properties, and subscribe
    /// to `a/+` at `qos`. Leaves the connection ready to receive.
    async fn attach(
        client: &mut tokio::io::DuplexStream,
        client_id: &str,
        properties: Properties,
        qos: QoS,
    ) {
        let mut packet = connect(client_id, 0);
        packet.properties = properties;
        write(client, &Packet::Connect(Box::new(packet)), Version::V5).await;
        assert!(matches!(
            read(client, Version::V5).await,
            Packet::ConnAck(_)
        ));
        write(
            client,
            &Packet::Subscribe(mqtt_codec::packet::Subscribe {
                packet_id: 1,
                filters: vec![mqtt_codec::packet::SubscriptionOptions {
                    filter: "a/+".into(),
                    qos,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: mqtt_codec::packet::RetainHandling::DoNotSend,
                }],
                properties: Properties::default(),
            }),
            Version::V5,
        )
        .await;
        assert!(matches!(read(client, Version::V5).await, Packet::SubAck(_)));
    }

    /// A publish of `payload` to `a/b`, at `qos`, under `packet_id`.
    fn publication(payload: &[u8], qos: QoS, packet_id: Option<u16>) -> Packet {
        Packet::Publish(mqtt_codec::packet::Publish {
            dup: false,
            qos,
            retain: false,
            topic: "a/b".into(),
            packet_id,
            payload: bytes::Bytes::copy_from_slice(payload),
            properties: Properties::default(),
        })
    }

    /// Offer `payload` to a subscriber that advertised `limit`, and
    /// say whether it arrived.
    ///
    /// A four byte publication follows it, so the answer is read off a
    /// message that arrives either way rather than off a timeout. A
    /// test that waits for nothing to happen cannot tell a discarded
    /// packet from a wedged connection, which is convention 5's
    /// "a test asserting an absence".
    async fn arrives_under_limit(payload: &[u8], limit: u32, qos: QoS) -> bool {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut subscriber = serve_one(Arc::clone(&shared));
        let mut publisher = serve_one(Arc::clone(&shared));

        attach(
            &mut subscriber,
            "sub",
            Properties {
                maximum_packet_size: Some(limit),
                ..Properties::default()
            },
            qos,
        )
        .await;
        attach(
            &mut publisher,
            "pub",
            Properties::default(),
            QoS::AtMostOnce,
        )
        .await;

        let packet_id = (qos != QoS::AtMostOnce).then_some(1);
        write(
            &mut publisher,
            &publication(payload, qos, packet_id),
            Version::V5,
        )
        .await;
        write(
            &mut publisher,
            &publication(b"tiny", QoS::AtMostOnce, None),
            Version::V5,
        )
        .await;

        // Whichever arrives first is the answer: the two were published
        // in order, so the large one reaching this client at all means
        // it reaches it before the small one.
        match read(&mut subscriber, Version::V5).await {
            Packet::Publish(publish) if publish.payload == b"tiny"[..] => false,
            Packet::Publish(publish) => {
                assert_eq!(
                    &publish.payload[..],
                    payload,
                    "one of the two, and no other"
                );
                true
            }
            other => panic!("expected a publication, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_packet_larger_than_the_client_allows_is_not_sent() {
        // [MQTT-3.1.2-24]. The publication behind it is what makes this
        // an assertion rather than the absence of one: the small one
        // arriving proves the connection is still delivering.
        assert!(
            !arrives_under_limit(&[b'x'; 200], 64, QoS::AtMostOnce).await,
            "a 200 byte publication must not reach a client that allows 64"
        );
    }

    /// The encoded size of one packet, as the writer will measure it.
    fn encoded_size(packet: &Packet) -> usize {
        let mut buf = BytesMut::new();
        encode(packet, Version::V5, &mut buf).unwrap();
        buf.len()
    }

    #[tokio::test]
    async fn a_packet_of_exactly_the_advertised_size_is_sent() {
        // The boundary. 3.1.2.11.4 gives "the Maximum Packet Size the
        // Client is willing to accept", so a packet of exactly that
        // size is one the client accepts and refusing it loses a
        // message the protocol says to deliver.
        //
        // Nothing else here is within a byte of a limit, so `>`
        // degraded to `>=` passes the whole of the rest of the suite.
        let payload = b"12345678";
        let exact = encoded_size(&publication(payload, QoS::AtMostOnce, None));
        // Derived from section 3.3 rather than from the encoder, so
        // that a drift in either shows up here: one byte of fixed
        // header and one of remaining length, two of topic length and
        // three of topic, one property length of zero, eight of
        // payload.
        assert_eq!(exact, 1 + 1 + 2 + 3 + 1 + 8, "a v5 publish of 'a/b'");

        assert!(
            arrives_under_limit(payload, exact as u32, QoS::AtMostOnce).await,
            "a packet of exactly the advertised size is legal"
        );
        assert!(
            !arrives_under_limit(payload, exact as u32 - 1, QoS::AtMostOnce).await,
            "and one byte more is not"
        );
    }

    #[tokio::test]
    async fn a_zero_maximum_packet_size_is_a_protocol_error() {
        // Section 3.1.2.11.4: "It is a Protocol Error to include the
        // Maximum Packet Size more than once, or for the value to be
        // set to zero." Refused rather than clamped, for the same
        // reason a zero receive maximum is.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(shared);

        let mut packet = connect("c", 0);
        packet.properties.maximum_packet_size = Some(0);
        write(&mut client, &Packet::Connect(Box::new(packet)), Version::V5).await;

        match read(&mut client, Version::V5).await {
            Packet::ConnAck(ack) => assert_eq!(ack.reason, ReasonCode::ProtocolError),
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    /// The other half of [MQTT-3.1.2-25], at each level that has an
    /// acknowledgement to owe: after a discard the session must behave
    /// as though the message had been delivered, which means its
    /// window slot and its packet identifier come back.
    ///
    /// Without that, one oversized publication takes the last slot in
    /// a window of one and holds it for an acknowledgement that cannot
    /// arrive, because the packet was never sent. Delivery to this
    /// client then stops for the life of the session, and a test that
    /// checks only that the large message is absent still passes.
    async fn a_discard_does_not_wedge_the_connection(qos: QoS) {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut subscriber = serve_one(Arc::clone(&shared));
        let mut publisher = serve_one(Arc::clone(&shared));

        attach(
            &mut subscriber,
            "sub",
            Properties {
                maximum_packet_size: Some(64),
                // A window of one, so a slot never given back is a
                // slot never available again.
                receive_maximum: Some(1),
                ..Properties::default()
            },
            qos,
        )
        .await;
        attach(
            &mut publisher,
            "pub",
            Properties::default(),
            QoS::AtMostOnce,
        )
        .await;

        write(
            &mut publisher,
            &publication(&[b'x'; 200], qos, Some(1)),
            Version::V5,
        )
        .await;
        write(
            &mut publisher,
            &publication(b"tiny", qos, Some(2)),
            Version::V5,
        )
        .await;

        match read(&mut subscriber, Version::V5).await {
            Packet::Publish(publish) => {
                assert_eq!(
                    &publish.payload[..],
                    b"tiny",
                    "the window slot the discarded message held must come back"
                );
                assert_eq!(publish.qos, qos);
            }
            other => panic!("expected the publication behind the discard, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_discard_by_a_displaced_writer_leaves_the_newcomer_alone() {
        // A takeover hands the session to a new connection, but the
        // displaced writer keeps draining whatever was already in its
        // outbox, and one of those may be a publish too large to send.
        // Settling it then frees an identifier the newcomer is using,
        // so the newcomer's own delivery is stranded: it waits for an
        // acknowledgement whose entry has gone, and the identifier is
        // handed to an unrelated message.
        //
        // The race cannot be driven through a socket, so the writer's
        // share is built here directly. Without the epoch check, the
        // first discard below settles the entry.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let registration = shared.with_broker(|broker| {
            let mut out = Vec::new();
            let registration = broker.connect("", &connect("c", 0), &mut out);
            let session = broker.session_mut(registration.subscriber).unwrap();
            session.outbound.set_online(true);
            registration
        });

        let action = shared.with_broker(|broker| {
            let mut actions = Vec::new();
            broker
                .session_mut(registration.subscriber)
                .unwrap()
                .outbound
                .offer(
                    broker_core::session::Delivery {
                        message: broker_core::session::Message::new(
                            "a/b",
                            bytes::Bytes::from_static(b"x"),
                            QoS::AtLeastOnce,
                            false,
                        ),
                        qos: QoS::AtLeastOnce,
                        retain: false,
                        dup: false,
                        subscription_ids: Vec::new(),
                        packet_id: None,
                        sequence: 0,
                    },
                    0,
                    &mut actions,
                );
            actions.pop().expect("the publish went out")
        });

        let outstanding = || {
            shared.with_broker(|broker| {
                broker
                    .session(registration.subscriber)
                    .unwrap()
                    .outbound
                    .inflight_len()
            })
        };

        let displaced = SizeLimit {
            limit: Some(1),
            shared: Arc::clone(&shared),
            subscriber: registration.subscriber,
            epoch: registration.epoch + 1,
        };
        displaced.discard(&action);
        assert_eq!(
            outstanding(),
            1,
            "the session is not this writer's to settle"
        );

        let ours = SizeLimit {
            limit: Some(1),
            shared: Arc::clone(&shared),
            subscriber: registration.subscriber,
            epoch: registration.epoch,
        };
        ours.discard(&action);
        assert_eq!(outstanding(), 0, "and the writer that owns it does settle");
    }

    #[tokio::test]
    async fn an_at_least_once_discard_does_not_wedge_the_connection() {
        a_discard_does_not_wedge_the_connection(QoS::AtLeastOnce).await;
    }

    #[tokio::test]
    async fn an_exactly_once_discard_does_not_wedge_the_connection() {
        // The level with no completion path of its own before this
        // task: `on_puback` ends an at-least-once exchange and nothing
        // ended one still awaiting its receipt.
        a_discard_does_not_wedge_the_connection(QoS::ExactlyOnce).await;
    }

    /// A publish to `topic` carrying `alias`, at quality of service
    /// zero. An empty topic means "use the alias".
    fn aliased(topic: &str, alias: u16, payload: &[u8]) -> Packet {
        Packet::Publish(mqtt_codec::packet::Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: topic.into(),
            packet_id: None,
            payload: bytes::Bytes::copy_from_slice(payload),
            properties: Properties {
                topic_alias: Some(alias),
                ..Properties::default()
            },
        })
    }

    /// Connect as version 5 and return the acknowledgement.
    async fn connect_v5(client: &mut tokio::io::DuplexStream, client_id: &str) -> ConnAck {
        write(
            client,
            &Packet::Connect(Box::new(connect(client_id, 0))),
            Version::V5,
        )
        .await;
        match read(client, Version::V5).await {
            Packet::ConnAck(ack) => ack,
            other => panic!("expected an acknowledgement, got {other:?}"),
        }
    }

    /// Read the next publish, failing on anything else.
    ///
    /// Takes the buffer rather than allocating one, because the tests
    /// below wait for two publications in a row; see [`read_buffered`].
    async fn next_publish(
        client: &mut tokio::io::DuplexStream,
        buf: &mut BytesMut,
    ) -> mqtt_codec::packet::Publish {
        match read_buffered(client, buf, Version::V5).await {
            Packet::Publish(publish) => publish,
            other => panic!("expected a publication, got {other:?}"),
        }
    }

    /// Read the next disconnect, failing on anything else.
    async fn next_disconnect(client: &mut tokio::io::DuplexStream) -> Disconnect {
        match read(client, Version::V5).await {
            Packet::Disconnect(disconnect) => disconnect,
            other => panic!("expected a disconnect, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_acknowledgement_advertises_a_topic_alias_maximum() {
        // 3.3.2.3.4: without one, a client must send no aliases at all,
        // which is why nothing crashed before this task and why every
        // alias it did send was lost.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(shared);

        let ack = connect_v5(&mut client, "c").await;
        assert_eq!(
            ack.properties.topic_alias_maximum,
            Some(ConnectionConfig::default().topic_alias_maximum)
        );
    }

    #[tokio::test]
    async fn a_publish_with_only_an_alias_is_routed_to_the_topic_it_named() {
        // The message-loss defect. Both publications are asserted, not
        // just the second: a broker that routed the aliased one and
        // lost the one that named the topic would pass an assertion
        // about the second alone.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut subscriber = serve_one(Arc::clone(&shared));
        let mut publisher = serve_one(Arc::clone(&shared));

        attach(
            &mut subscriber,
            "sub",
            Properties::default(),
            QoS::AtMostOnce,
        )
        .await;
        connect_v5(&mut publisher, "pub").await;

        write(&mut publisher, &aliased("a/b", 1, b"first"), Version::V5).await;
        write(&mut publisher, &aliased("", 1, b"second"), Version::V5).await;

        let mut buf = BytesMut::new();
        let first = next_publish(&mut subscriber, &mut buf).await;
        assert_eq!(first.topic, "a/b");
        assert_eq!(&first.payload[..], b"first");

        let second = next_publish(&mut subscriber, &mut buf).await;
        assert_eq!(
            second.topic, "a/b",
            "the alias stands in for the topic it was mapped to"
        );
        assert_eq!(&second.payload[..], b"second");
    }

    #[tokio::test]
    async fn a_subscriber_never_sees_the_publishers_topic_alias() {
        // 3.1.2.11.5: an alias means nothing to a connection that did
        // not set it, and this subscriber advertised no maximum, so it
        // may be sent none at all.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut subscriber = serve_one(Arc::clone(&shared));
        let mut publisher = serve_one(Arc::clone(&shared));

        attach(
            &mut subscriber,
            "sub",
            Properties::default(),
            QoS::AtMostOnce,
        )
        .await;
        connect_v5(&mut publisher, "pub").await;

        write(&mut publisher, &aliased("a/b", 1, b"first"), Version::V5).await;

        let delivered = next_publish(&mut subscriber, &mut BytesMut::new()).await;
        assert_eq!(delivered.topic, "a/b");
        assert_eq!(
            delivered.properties.topic_alias, None,
            "the alias is stripped on ingress, so nothing below the shell sees one"
        );
    }

    #[tokio::test]
    async fn a_retained_message_is_delivered_without_the_alias_it_was_published_with() {
        // The retained store keeps a message's properties verbatim and
        // was deliberately left alone by the task that fixed it, on the
        // grounds that the strip here is the only one. If it is wrong,
        // the store holds an alias and hands it to every later
        // subscriber, on connections that never set it.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut publisher = serve_one(Arc::clone(&shared));
        connect_v5(&mut publisher, "pub").await;

        let mut retained = aliased("a/b", 1, b"kept");
        let Packet::Publish(publish) = &mut retained else {
            unreachable!()
        };
        publish.retain = true;
        write(&mut publisher, &retained, Version::V5).await;

        let mut subscriber = serve_one(Arc::clone(&shared));
        connect_v5(&mut subscriber, "sub").await;
        write(
            &mut subscriber,
            &Packet::Subscribe(mqtt_codec::packet::Subscribe {
                packet_id: 1,
                filters: vec![mqtt_codec::packet::SubscriptionOptions {
                    filter: "a/+".into(),
                    qos: QoS::AtMostOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: mqtt_codec::packet::RetainHandling::SendAlways,
                }],
                properties: Properties::default(),
            }),
            Version::V5,
        )
        .await;
        // Section 3.8.4 permits the server to start sending matching
        // publications before the acknowledgement, and this one does,
        // so both are waited for rather than one then the other.
        let mut buf = BytesMut::new();
        let mut acknowledged = false;
        let mut delivered = None;
        for _ in 0..2 {
            match read_buffered(&mut subscriber, &mut buf, Version::V5).await {
                Packet::SubAck(_) => acknowledged = true,
                Packet::Publish(publish) => delivered = Some(publish),
                other => panic!("expected the subscription's replies, got {other:?}"),
            }
        }
        assert!(acknowledged, "the subscription is acknowledged too");
        let delivered = delivered.expect("the retained message is sent");
        assert_eq!(&delivered.payload[..], b"kept", "the retained message");
        assert_eq!(delivered.topic, "a/b");
        assert_eq!(
            delivered.properties.topic_alias, None,
            "a retained delivery carries no alias either"
        );
    }

    #[tokio::test]
    async fn an_alias_of_exactly_the_maximum_is_accepted() {
        // The boundary. Nothing else here is within one of it, so a
        // bound one too tight passes the whole of the rest of the
        // suite while silently refusing the last alias a client may
        // legally use.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut subscriber = serve_one(Arc::clone(&shared));
        let mut publisher = serve_one(Arc::clone(&shared));

        attach(
            &mut subscriber,
            "sub",
            Properties::default(),
            QoS::AtMostOnce,
        )
        .await;
        let maximum = connect_v5(&mut publisher, "pub")
            .await
            .properties
            .topic_alias_maximum
            .expect("a maximum is advertised");

        write(
            &mut publisher,
            &aliased("a/b", maximum, b"at the limit"),
            Version::V5,
        )
        .await;

        let delivered = next_publish(&mut subscriber, &mut BytesMut::new()).await;
        assert_eq!(&delivered.payload[..], b"at the limit");
    }

    #[tokio::test]
    async fn an_alias_above_the_maximum_is_a_protocol_error() {
        // 3.3.2.3.4: "It is a Protocol Error if the Topic Alias value
        // is [...] greater than the Topic Alias Maximum".
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(shared);

        let maximum = connect_v5(&mut client, "c")
            .await
            .properties
            .topic_alias_maximum
            .expect("a maximum is advertised");

        write(
            &mut client,
            &aliased("a/b", maximum + 1, b"one too many"),
            Version::V5,
        )
        .await;

        assert_eq!(
            next_disconnect(&mut client).await.reason,
            ReasonCode::TopicAliasInvalid
        );
    }

    #[tokio::test]
    async fn an_unmapped_alias_is_a_protocol_error() {
        // An empty topic and an alias nothing was ever mapped to names
        // no topic at all. Treating it as an empty topic would publish
        // to one, silently.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(shared);
        connect_v5(&mut client, "c").await;

        write(&mut client, &aliased("", 1, b"nowhere"), Version::V5).await;

        assert_eq!(
            next_disconnect(&mut client).await.reason,
            ReasonCode::TopicAliasInvalid
        );
    }

    #[tokio::test]
    async fn an_alias_does_not_survive_the_connection_that_set_it() {
        // 3.3.2.3.4: "Topic Alias mappings exist only within a Network
        // Connection and last only for the lifetime of that Network
        // Connection." The session is resumed here and the mapping is
        // still gone, which is why the table lives in the shell rather
        // than in the session.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut first = serve_one(Arc::clone(&shared));

        let mut durable = connect("durable", 0);
        durable.clean_start = false;
        write(&mut first, &Packet::Connect(Box::new(durable)), Version::V5).await;
        assert!(matches!(
            read(&mut first, Version::V5).await,
            Packet::ConnAck(_)
        ));
        write(&mut first, &aliased("a/b", 1, b"maps it"), Version::V5).await;
        write(&mut first, &Packet::PingReq, Version::V5).await;
        assert_eq!(read(&mut first, Version::V5).await, Packet::PingResp);
        drop(first);

        // The takeover the second connect performs is what ends the
        // first, so no wait for the socket to close is needed.
        let mut second = serve_one(Arc::clone(&shared));
        let mut durable = connect("durable", 0);
        durable.clean_start = false;
        write(
            &mut second,
            &Packet::Connect(Box::new(durable)),
            Version::V5,
        )
        .await;
        assert!(matches!(
            read(&mut second, Version::V5).await,
            Packet::ConnAck(_)
        ));

        write(&mut second, &aliased("", 1, b"stale"), Version::V5).await;

        assert_eq!(
            next_disconnect(&mut second).await.reason,
            ReasonCode::TopicAliasInvalid
        );
    }

    /// Connect over `client` and return the acknowledgement, which must
    /// be a successful one.
    async fn acknowledged(client: &mut tokio::io::DuplexStream) -> ConnAck {
        write(
            client,
            &Packet::Connect(Box::new(connect("c", 0))),
            Version::V5,
        )
        .await;
        match read(client, Version::V5).await {
            Packet::ConnAck(ack) => {
                assert_eq!(ack.reason, ReasonCode::Success);
                ack
            }
            other => panic!("expected an acknowledgement, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_acknowledgement_reports_the_receive_maximum_it_enforces() {
        // The number was asserted to be present and not to be anything
        // in particular, which is convention 2: any constant passed,
        // including one the broker does not enforce. The two can
        // disagree silently, and a client that believes a limit larger
        // than the real one is disconnected for keeping to what it was
        // told.
        //
        // So the advertised number is read off the wire and then spent:
        // exactly-once exchanges hold their slot until their completion
        // [MQTT-3.3.4-9], and none is sent here, so the count that fits
        // is the limit the broker actually imposes.
        const IMPOSED: u16 = 3;
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one_with_config(
            shared,
            ConnectionConfig {
                receive_maximum: IMPOSED,
                ..ConnectionConfig::default()
            },
        );

        let advertised = acknowledged(&mut client)
            .await
            .properties
            .receive_maximum
            .expect("a client cannot honour a limit it was never told");
        assert_eq!(
            advertised, IMPOSED,
            "the client is told the configured limit"
        );

        let mut buf = BytesMut::new();
        for packet_id in 1..=advertised {
            write(
                &mut client,
                &publication(b"x", QoS::ExactlyOnce, Some(packet_id)),
                Version::V5,
            )
            .await;
            match read_buffered(&mut client, &mut buf, Version::V5).await {
                Packet::PubRec(ack) => assert_eq!(ack.packet_id, packet_id),
                other => panic!(
                    "the client was told it could have {advertised} outstanding \
                     and the {packet_id} was answered with {other:?}"
                ),
            }
        }

        write(
            &mut client,
            &publication(b"x", QoS::ExactlyOnce, Some(advertised + 1)),
            Version::V5,
        )
        .await;
        match read_buffered(&mut client, &mut buf, Version::V5).await {
            Packet::Disconnect(disconnect) => assert_eq!(
                disconnect.reason,
                ReasonCode::ReceiveMaximumExceeded,
                "[MQTT-3.3.4-9]: one past the advertised limit is refused"
            ),
            other => panic!("expected the refusal past the limit, got {other:?}"),
        }
    }

    /// A frame header declaring a packet of exactly `total` bytes, whose
    /// body never arrives.
    ///
    /// The decoder refuses an oversized packet from its declared length
    /// alone, before any of the body is read, so this is the whole of
    /// an oversized packet that a test needs to send.
    fn frame_declaring(total: usize) -> Vec<u8> {
        let remaining = total - 2;
        assert!(remaining < 128, "one byte of type and one of length");
        // A publish at quality of service zero, which is legal at any
        // point in a connection and so cannot be refused for any other
        // reason than its size.
        vec![0x30, remaining as u8]
    }

    #[tokio::test]
    async fn the_acknowledgement_advertises_the_maximum_packet_size_it_enforces() {
        // Section 5.4 lists it in scope and 3.1.2.11.4 defaults it to
        // unlimited, so a client told nothing assumes it may send
        // anything and learns our limit by being disconnected for
        // exceeding it. The property had no assertion at all: deleting
        // it from the acknowledgement passed the whole suite.
        //
        // Advertised and enforced are asserted together, because either
        // alone permits the pair to disagree, and a limit a client is
        // told but not held to is as bad as one it is held to but not
        // told.
        const LIMIT: u32 = 64;
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one_with_config(
            shared,
            ConnectionConfig {
                limits: Limits {
                    max_packet_size: LIMIT,
                    ..Limits::default()
                },
                ..ConnectionConfig::default()
            },
        );

        let advertised = acknowledged(&mut client)
            .await
            .properties
            .maximum_packet_size
            .expect("a client that is told nothing assumes it may send anything");
        assert_eq!(advertised, LIMIT, "the client is told the configured limit");

        client
            .write_all(&frame_declaring(advertised as usize + 1))
            .await
            .unwrap();
        match read(&mut client, Version::V5).await {
            Packet::Disconnect(disconnect) => assert_eq!(
                disconnect.reason,
                ReasonCode::PacketTooLarge,
                "3.2.2.3.6: one byte past the advertised size is refused"
            ),
            other => panic!("expected the refusal past the limit, got {other:?}"),
        }
    }

    /// How long to wait before concluding that nothing more is coming.
    ///
    /// Only ever the gap between two messages already in flight on an
    /// in-memory socket, and never the whole of an assertion: what
    /// follows the quiet period is a positive check that the
    /// connection was withholding rather than broken.
    const WINDOW_QUIET: Duration = Duration::from_millis(500);

    /// The next reply, or `None` if none arrives within `within`.
    async fn read_within(
        client: &mut tokio::io::DuplexStream,
        buf: &mut BytesMut,
        version: Version,
        within: Duration,
    ) -> Option<Packet> {
        tokio::time::timeout(within, read_buffered(client, buf, version))
            .await
            .ok()
    }

    /// Connect at `version`, advertising `receive_maximum` if there is
    /// one, and check the acknowledgement arrived.
    async fn connect_advertising(
        client: &mut tokio::io::DuplexStream,
        version: Version,
        client_id: &str,
        receive_maximum: Option<u16>,
    ) {
        let mut packet = connect(client_id, 0);
        packet.version = version;
        packet.properties.receive_maximum = receive_maximum;
        write(client, &Packet::Connect(Box::new(packet)), version).await;
        assert!(matches!(read(client, version).await, Packet::ConnAck(_)));
    }

    /// Subscribe to `a/+` at `qos` over an already connected client.
    async fn subscribe_at(client: &mut tokio::io::DuplexStream, version: Version, qos: QoS) {
        write(
            client,
            &Packet::Subscribe(mqtt_codec::packet::Subscribe {
                packet_id: 1,
                filters: vec![mqtt_codec::packet::SubscriptionOptions {
                    filter: "a/+".into(),
                    qos,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: mqtt_codec::packet::RetainHandling::DoNotSend,
                }],
                properties: Properties::default(),
            }),
            version,
        )
        .await;
        assert!(matches!(read(client, version).await, Packet::SubAck(_)));
    }

    /// The window the broker actually applies to a subscriber, measured
    /// over a real connection.
    ///
    /// Section 6 makes it "the lesser of the configured maximum and the
    /// peer's receive maximum", so what is measured is how many
    /// quality-of-service 1 publications the broker will hold
    /// unacknowledged before it stops. Measured rather than read off an
    /// accessor because the defect is in the wiring: a unit test of
    /// `set_inflight_limit` passed throughout.
    async fn measured_window(
        version: Version,
        receive_maximum: Option<u16>,
        max_inflight: u16,
    ) -> usize {
        // More than any window these tests configure or advertise, so
        // the count below is the window and not the supply.
        const OFFERED: u16 = 8;

        let shared = Arc::new(Shared::new(BrokerConfig {
            max_inflight,
            ..BrokerConfig::default()
        }));
        let mut subscriber = serve_one(Arc::clone(&shared));
        let mut publisher = serve_one(Arc::clone(&shared));

        connect_advertising(&mut subscriber, version, "sub", receive_maximum).await;
        subscribe_at(&mut subscriber, version, QoS::AtLeastOnce).await;
        connect_advertising(&mut publisher, Version::V5, "pub", None).await;

        for packet_id in 1..=OFFERED {
            write(
                &mut publisher,
                &publication(b"x", QoS::AtLeastOnce, Some(packet_id)),
                Version::V5,
            )
            .await;
        }

        // Nothing is acknowledged, so the window is what stops this.
        let mut buf = BytesMut::new();
        let mut held: Vec<u16> = Vec::new();
        while held.len() < usize::from(OFFERED) {
            let Some(packet) = read_within(&mut subscriber, &mut buf, version, WINDOW_QUIET).await
            else {
                break;
            };
            match packet {
                Packet::Publish(publish) => {
                    held.push(publish.packet_id.expect("a qos 1 publish carries one"));
                }
                other => panic!("expected a publication, got {other:?}"),
            }
        }

        assert!(
            held.len() < usize::from(OFFERED),
            "all {OFFERED} offered messages arrived, so this measured the supply \
             rather than a window: the broker held no window at all"
        );

        // Convention 5: this is not an assertion of absence. One
        // acknowledgement frees exactly one slot, and the message that
        // follows proves the connection was withholding rather than
        // wedged, closed or simply out of messages.
        write(
            &mut subscriber,
            &Packet::PubAck(mqtt_codec::packet::Ack {
                packet_id: held[0],
                reason: ReasonCode::Success,
                properties: Properties::default(),
            }),
            version,
        )
        .await;
        match read_buffered(&mut subscriber, &mut buf, version).await {
            Packet::Publish(publish) => assert!(
                !held.contains(&publish.packet_id.expect("a qos 1 publish carries one")),
                "the freed slot must carry the next message, not a redelivery"
            ),
            other => panic!("expected the next publication, got {other:?}"),
        }

        held.len()
    }

    #[tokio::test]
    async fn a_client_that_advertises_no_receive_maximum_gets_the_configured_window() {
        // An absent Receive Maximum means 65535 [3.1.2.11.3], which is
        // not a licence to hold 65535 messages: section 6 takes the
        // lesser of the two, and the configured maximum is the lesser.
        assert_eq!(
            measured_window(Version::V5, None, 4).await,
            4,
            "the configured maximum, not the protocol's ceiling"
        );
    }

    #[tokio::test]
    async fn a_client_that_advertises_more_than_we_allow_gets_our_number() {
        assert_eq!(
            measured_window(Version::V5, Some(10), 4).await,
            4,
            "ours is the lesser"
        );
    }

    #[tokio::test]
    async fn a_client_that_advertises_less_than_we_allow_gets_its_own_number() {
        assert_eq!(
            measured_window(Version::V5, Some(2), 4).await,
            2,
            "theirs is the lesser"
        );
    }

    #[tokio::test]
    async fn a_three_one_one_client_gets_the_configured_window() {
        // 3.1.1 has no Receive Maximum to advertise, so it is the
        // absent case by another road and must land on the same number.
        // It kept the constructor's 20 before this task only because
        // the assignment that replaced it was guarded on version 5.
        assert_eq!(measured_window(Version::V311, None, 4).await, 4);
    }

    /// Long enough for a disconnect already written on an in-memory
    /// socket to arrive, and never the whole of an assertion: each use
    /// is paired with the neighbouring second, where the opposite must
    /// be true.
    const SWEEP_QUIET: Duration = Duration::from_millis(500);

    /// Advance the broker's clock by whole seconds, delivering what the
    /// sweep produces, exactly as the server's own tick loop does.
    fn advance(shared: &Arc<Shared>, seconds: u32) {
        for _ in 0..seconds {
            let actions = shared.with_broker(|broker| {
                let mut out = Vec::new();
                broker.tick(&mut out);
                out
            });
            shared.deliver(actions);
        }
    }

    /// What has become of a connection by the end of the quiet period.
    #[derive(Debug, PartialEq, Eq)]
    enum Ending {
        /// Nothing has happened to it.
        Open,
        /// It has been closed, carrying a reason where the version has
        /// one to carry: 3.1.1 has no server sent disconnect, so there
        /// the close is the whole message.
        Closed(Option<ReasonCode>),
    }

    /// How the broker ended this connection, if it has.
    async fn ending(client: &mut tokio::io::DuplexStream, version: Version) -> Ending {
        let mut chunk = [0u8; 64];
        let Ok(read) = tokio::time::timeout(SWEEP_QUIET, client.read(&mut chunk)).await else {
            return Ending::Open;
        };
        let n = read.expect("the socket reads");
        if n == 0 {
            return Ending::Closed(None);
        }
        let mut buf = BytesMut::from(&chunk[..n]);
        match decode(&mut buf, version, &Limits::default()).unwrap() {
            Some(Packet::Disconnect(disconnect)) => Ending::Closed(Some(disconnect.reason)),
            other => panic!("expected a disconnect, got {other:?}"),
        }
    }

    /// Assert that a silent client survives second `limit` and is cut
    /// off in the one after it.
    ///
    /// On the boundary rather than around it, because either half alone
    /// is satisfied by the wrong interval: the closure alone passes for
    /// any interval already lapsed, including one shorter than was
    /// advertised, and the survival alone passes for a broker that
    /// disconnects nobody. The ring gives up after three halves of the
    /// negotiated interval, so `limit` is that, and the second after it
    /// is the first at which the client has been silent for longer.
    async fn expires_in_the_second_after(
        shared: &Arc<Shared>,
        client: &mut tokio::io::DuplexStream,
        version: Version,
        limit: u32,
    ) {
        advance(shared, limit);
        assert_eq!(
            ending(client, version).await,
            Ending::Open,
            "at {limit} seconds the interval has not lapsed yet"
        );

        advance(shared, 1);
        let expected = match version {
            Version::V5 => Ending::Closed(Some(ReasonCode::KeepAliveTimeout)),
            Version::V311 => Ending::Closed(None),
        };
        assert_eq!(
            ending(client, version).await,
            expected,
            "at {} seconds the client should have been given up on",
            limit + 1
        );
    }

    /// Connect at `version` proposing `proposed` to a broker configured
    /// with `maximum`, returning what the acknowledgement said about the
    /// keep-alive alongside the pieces needed to time the sweep.
    async fn proposing(
        version: Version,
        proposed: u16,
        maximum: u16,
    ) -> (Arc<Shared>, tokio::io::DuplexStream, Option<u16>) {
        let shared = Arc::new(Shared::new(BrokerConfig {
            max_keep_alive: maximum,
            ..BrokerConfig::default()
        }));
        let mut client = serve_one(Arc::clone(&shared));
        let mut packet = connect("c", proposed);
        packet.version = version;
        write(&mut client, &Packet::Connect(Box::new(packet)), version).await;
        let advertised = match read(&mut client, version).await {
            Packet::ConnAck(ack) => ack.properties.server_keep_alive,
            other => panic!("expected an acknowledgement, got {other:?}"),
        };
        (shared, client, advertised)
    }

    #[tokio::test]
    async fn a_keep_alive_above_the_maximum_is_clamped_and_the_client_told() {
        // [MQTT-3.2.2-21]: a Server Keep Alive in the connack is the
        // value the client must use instead of its own. Without it the
        // client believes it may be silent for sixty seconds and is cut
        // off at four.
        let (shared, mut client, advertised) = proposing(Version::V5, 60, 4).await;
        assert_eq!(advertised, Some(4), "the client must learn our maximum");

        // The second half, and the one that makes the property truthful
        // rather than decorative: the interval advertised is the
        // interval enforced. Four gives up in the seventh second, where
        // the sixty the client proposed would not until the ninety
        // first.
        expires_in_the_second_after(&shared, &mut client, Version::V5, 6).await;
    }

    #[tokio::test]
    async fn a_keep_alive_within_the_maximum_is_left_alone_and_unremarked() {
        // [MQTT-3.2.2-22]: an absent Server Keep Alive means the
        // client's own value stands, so sending it back unchanged is
        // noise it must parse and act on for nothing.
        let (shared, mut client, advertised) = proposing(Version::V5, 10, 60).await;
        assert_eq!(
            advertised, None,
            "nothing was imposed, so there is nothing to say"
        );

        // And silence in the acknowledgement must mean the client's own
        // ten, not the configured sixty by another road.
        expires_in_the_second_after(&shared, &mut client, Version::V5, 15).await;
    }

    #[tokio::test]
    async fn a_keep_alive_exactly_at_the_maximum_is_not_remarked_on() {
        // The boundary itself, because the test above sits well inside
        // it and passes against a comparison off by one. A client
        // proposing exactly what we allow has had nothing imposed on
        // it, so [MQTT-3.2.2-22] says we send nothing.
        let (shared, mut client, advertised) = proposing(Version::V5, 30, 30).await;
        assert_eq!(advertised, None, "thirty is not more than thirty");
        expires_in_the_second_after(&shared, &mut client, Version::V5, 45).await;
    }

    #[tokio::test]
    async fn a_maximum_of_zero_is_no_ceiling_rather_than_a_ceiling_of_zero() {
        // Zero is the one value that cannot mean what the others mean:
        // read as a ceiling it would force every client's keep-alive
        // off, which is the opposite of bounding one. Guarded because
        // dropping the guard changes only this configuration, and
        // nothing else in the suite sets it.
        let (shared, mut client, advertised) = proposing(Version::V5, 10, 0).await;
        assert_eq!(advertised, None, "no ceiling, so nothing imposed");
        expires_in_the_second_after(&shared, &mut client, Version::V5, 15).await;
    }

    #[tokio::test]
    async fn a_three_one_one_client_keeps_the_interval_it_asked_for() {
        // 3.1.1 has no property to carry a Server Keep Alive, so a
        // clamp here could not be announced, and an unannounced clamp
        // is the whole defect: the client believes it may be silent for
        // its own interval and is disconnected at ours. The ceiling is
        // therefore a version 5 policy, and this says so in the only
        // way that cannot be satisfied by an absence: the client is
        // held to its own sixty, not to the configured four.
        let (shared, mut client, advertised) = proposing(Version::V311, 60, 4).await;
        assert_eq!(advertised, None, "3.1.1 has no properties at all");
        expires_in_the_second_after(&shared, &mut client, Version::V311, 90).await;
    }

    // ----------------------------------------------------------------
    // Section 7.3: nothing is acknowledged before it is durable.
    // ----------------------------------------------------------------

    /// A shell with a store whose flush the test controls.
    ///
    /// The writer is the real one. Nothing here stubs the ordering
    /// out: what is replaced is the device, so that the window in
    /// which an acknowledgement could overtake a flush can be held
    /// open for as long as an assertion needs.
    fn with_gated_store() -> (Arc<Shared>, Arc<GatedStore>) {
        let store = Arc::new(GatedStore::new());
        let writer = Writer::spawn(
            Arc::clone(&store) as Arc<dyn storage::Storage>,
            storage::WriterConfig::default(),
        )
        .expect("a writer thread");
        let shared = Arc::new(Shared::with_store(
            Broker::new(BrokerConfig::default()),
            Arc::new(writer),
        ));
        (shared, store)
    }

    /// Whether anything at all arrives within `within`.
    async fn quiet_for(client: &mut tokio::io::DuplexStream, within: Duration) -> bool {
        let mut chunk = [0u8; 256];
        tokio::time::timeout(within, client.read(&mut chunk))
            .await
            .is_err()
    }

    #[tokio::test]
    async fn an_acknowledgement_does_not_reach_the_client_until_the_store_has_it() {
        // The test the whole milestone turns on, and the reason it is
        // shaped this way: an assertion that the acknowledgement
        // eventually arrives passes against a broker that sends it
        // first, so the only thing that can tell the two apart is a
        // store that has not answered yet.
        //
        // Convention 5 warns about a test asserting an absence, and
        // the first half of this is one. The second half is what
        // makes it a discrimination rather than an absence: the same
        // connection, the same packet, and the acknowledgement
        // arrives the moment the device does.
        let (shared, store) = with_gated_store();
        store.hold();
        let mut client = serve_one(Arc::clone(&shared));
        connect_v5(&mut client, "publisher").await;

        write(
            &mut client,
            &publication(b"x", QoS::AtLeastOnce, Some(1)),
            Version::V5,
        )
        .await;

        assert!(
            quiet_for(&mut client, Duration::from_millis(400)).await,
            "the acknowledgement overtook the flush it is a promise about"
        );

        store.release();
        match read(&mut client, Version::V5).await {
            Packet::PubAck(ack) => assert_eq!(ack.packet_id, 1),
            other => panic!("expected the acknowledgement, got {other:?}"),
        }
        assert!(store.flushes() > 0, "the store was asked for a flush");
    }

    #[tokio::test]
    async fn an_exactly_once_receipt_does_not_reach_the_client_until_the_store_has_it() {
        // The other half of the promise. A receipt is where an
        // exactly-once publication becomes the broker's
        // responsibility, so it waits for exactly the same reason a
        // PUBACK does, and a shell that matched only on PUBACK would
        // pass every test above this one.
        let (shared, store) = with_gated_store();
        store.hold();
        let mut client = serve_one(Arc::clone(&shared));
        connect_v5(&mut client, "publisher").await;

        write(
            &mut client,
            &publication(b"x", QoS::ExactlyOnce, Some(7)),
            Version::V5,
        )
        .await;
        assert!(
            quiet_for(&mut client, Duration::from_millis(400)).await,
            "the receipt overtook the flush"
        );

        store.release();
        match read(&mut client, Version::V5).await {
            Packet::PubRec(receipt) => assert_eq!(receipt.packet_id, 7),
            other => panic!("expected the receipt, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn what_promises_nothing_is_not_held_up_by_the_store() {
        // The other side of the decision, and the one that fails if
        // every batch is treated as a promise. A subscription is
        // durable state and is journalled, but nobody was told a
        // message was safe, so section 7.3 does not make anyone wait
        // for it. A shell that waited on everything would serialise
        // the entire broker behind the device for traffic that
        // promised nothing.
        let (shared, store) = with_gated_store();
        store.hold();
        let mut client = serve_one(Arc::clone(&shared));
        connect_v5(&mut client, "subscriber").await;

        subscribe_at(&mut client, Version::V5, QoS::AtMostOnce).await;

        // At most once, the level section 7.3 names as never waited
        // on, through the same held store.
        write(
            &mut client,
            &publication(b"x", QoS::AtMostOnce, None),
            Version::V5,
        )
        .await;
        match read(&mut client, Version::V5).await {
            Packet::Publish(publish) => assert_eq!(publish.qos, QoS::AtMostOnce),
            other => panic!("expected the publication back, got {other:?}"),
        }

        store.release();
    }

    #[tokio::test]
    async fn a_flush_that_fails_closes_the_connection_instead_of_acknowledging() {
        // What a client gets when the store will not take the message.
        //
        // Not silence, which leaves a publisher holding an
        // unacknowledged message and no reason to resend it before
        // its own timeout. Not a success, which is the lie this
        // milestone exists to prevent. A close, carrying a reason a
        // version 5 client can read, because MQTT's own recovery for
        // an unacknowledged publication is to send it again on the
        // next connection, and a close is what makes that happen at
        // once.
        let (shared, store) = with_gated_store();
        store.fail();
        let mut client = serve_one(Arc::clone(&shared));
        connect_v5(&mut client, "publisher").await;

        write(
            &mut client,
            &publication(b"x", QoS::AtLeastOnce, Some(1)),
            Version::V5,
        )
        .await;

        let disconnect = next_disconnect(&mut client).await;
        assert_eq!(
            disconnect.reason,
            ReasonCode::ImplementationSpecificError,
            "the client is told why, in the only code that fits a store this server could not write to"
        );

        // And nothing after it: the acknowledgement is not sent late,
        // which is the failure mode a test that only looked for the
        // disconnect would miss.
        let mut chunk = [0u8; 256];
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut chunk))
            .await
            .expect("the connection closes")
            .expect("the read succeeds");
        assert_eq!(n, 0, "the acknowledgement must never follow the refusal");
    }

    #[tokio::test]
    async fn a_client_that_promised_nothing_is_not_closed_by_a_failing_flush() {
        // The failure is reported to the connection that was about to
        // be lied to, and to no other. A subscribe through a store
        // that cannot flush is not a broken promise, because no
        // promise was made, so the connection carries on.
        let (shared, store) = with_gated_store();
        store.fail();
        let mut client = serve_one(Arc::clone(&shared));
        connect_v5(&mut client, "subscriber").await;

        subscribe_at(&mut client, Version::V5, QoS::AtMostOnce).await;
        write(&mut client, &Packet::PingReq, Version::V5).await;
        assert!(
            matches!(read(&mut client, Version::V5).await, Packet::PingResp),
            "the connection is still serving"
        );
    }
}
