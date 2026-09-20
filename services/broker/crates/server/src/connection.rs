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

use broker_core::broker::Broker;
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
use crate::shared::Shared;

/// What a client's receive maximum is when it does not say.
///
/// Section 3.1.2.11.3: "If the Receive Maximum value is absent then its
/// value defaults to 65,535." Zero is a protocol error rather than a
/// synonym for unlimited, which matters because the window this feeds
/// reads zero as unbounded.
const ABSENT_RECEIVE_MAXIMUM: u16 = 65_535;

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
    /// The namespace this listener's clients live in, empty for the
    /// default. Two clients with the same identifier on two mountpoints
    /// are two sessions.
    pub mountpoint: Arc<str>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            // A session's queue holds a thousand messages, so a thousand
            // is what can arrive at a writer at once. Anything smaller
            // would let a legitimate drain count as a slow reader.
            outbox_depth: 1024,
            connect_timeout: Duration::from_secs(10),
            // Finite, so a client cannot make us hold unbounded
            // exactly-once state, and far above any sensible client's
            // pipelining depth, so it never throttles one.
            receive_maximum: 1024,
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
    let (read_half, write_half) = tokio::io::split(socket);
    let mut reader = FrameReader::new(read_half, config.limits);
    let mut write_half = write_half;

    // Nothing below this point is owed to the broker: it does not know
    // this connection exists yet, so these returns are free.
    let mut connect = match first_packet(&mut reader, config.connect_timeout).await? {
        Some(connect) => connect,
        None => return Ok(()),
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
    let registered = shared.with_broker(|broker| {
        // [MQTT-3.1.3-6]: a zero length identifier is a special case and
        // the server assigns a unique one, then processes the connect as
        // if the client had sent it [MQTT-3.1.3-7].
        let assigned = if connect.client_id.is_empty() {
            let assigned = assign_identifier(broker, &config.mountpoint, candidate_identifier)?;
            connect.client_id.clone_from(&assigned);
            Some(assigned)
        } else {
            None
        };

        let mut out = Vec::new();
        let registration = broker.connect(&config.mountpoint, &connect, &mut out);
        if let Some(session) = broker.session_mut(registration.subscriber) {
            // Section 4.9: what the client can receive bounds what we
            // may have outstanding towards it, and what we advertise
            // bounds what it may have outstanding towards us. Two
            // directions, two settings, neither of them session state.
            if version == Version::V5 {
                // A 3.1.1 client advertises nothing, so there is nothing
                // to narrow to and the broker's own window stands.
                session.outbound.set_inflight_limit(window);
            }
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

    let ack = acknowledgement(&registration, assigned, &config);
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
/// - Everything else is omitted because it equals its default: we
///   support retained messages, both higher qualities of service,
///   wildcards, subscription identifiers and shared subscriptions, we
///   accept no topic aliases, and we neither shorten the session expiry
///   the client asked for nor impose a keep-alive of our own. On the
///   last of those, [MQTT-3.2.2-22] says an absent server keep-alive
///   means the client's value stands, which is what we want.
fn acknowledgement(
    registration: &broker_core::registry::Registration,
    assigned: Option<String>,
    config: &ConnectionConfig,
) -> ConnAck {
    ConnAck {
        session_present: registration.session_present,
        reason: ReasonCode::Success,
        properties: Properties {
            assigned_client_identifier: assigned,
            receive_maximum: Some(config.receive_maximum),
            maximum_packet_size: Some(config.limits.max_packet_size),
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
        loop {
            let packet = tokio::select! {
                // The writer stops when its outbox closes, which is how
                // a displaced connection learns it has been replaced, or
                // when it has written a close. Either way there is
                // nothing left to read for.
                _ = &mut *writer_gone => return Ok(()),
                next = reader.next(self.version) => match next? {
                    Some(packet) => packet,
                    None => return Ok(()),
                },
            };

            if matches!(packet, Packet::Connect(_)) {
                // [MQTT-3.2.0-2] forbids a second acknowledgement, and a
                // second connect is a protocol error. There is a session
                // now, so it is told why before the socket closes.
                self.close_with(ReasonCode::ProtocolError);
                return Err(ConnectionError::Protocol(ReasonCode::ProtocolError));
            }
            let said_goodbye = matches!(packet, Packet::Disconnect(_));

            let (outcome, actions) = self.shared.with_broker(|broker| {
                let mut out = Vec::new();
                let outcome = broker.handle(self.subscriber, &packet, &mut out);
                (outcome, out)
            });
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
        let (client, server) = tokio::io::duplex(8192);
        tokio::spawn(async move {
            let _ = serve(server, shared, ConnectionConfig::default()).await;
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
        loop {
            if let Some(packet) = decode(&mut buf, version, &Limits::default()).unwrap() {
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

    #[tokio::test]
    async fn the_acknowledgement_reports_our_own_receive_maximum() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(shared);

        write(
            &mut client,
            &Packet::Connect(Box::new(connect("c", 0))),
            Version::V5,
        )
        .await;
        match read(&mut client, Version::V5).await {
            Packet::ConnAck(ack) => {
                assert!(
                    ack.properties.receive_maximum.is_some(),
                    "a client cannot honour a limit it was never told"
                );
            }
            other => panic!("expected an acknowledgement, got {other:?}"),
        }
    }
}
