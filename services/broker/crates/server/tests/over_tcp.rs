//! The broker over a real socket.
//!
//! This duplicates what the connection tests already cover over an
//! in-memory duplex stream, on purpose. The two differ in exactly the
//! ways that break a shell: a duplex stream hands over whatever was
//! written in one piece, where a socket splits and coalesces as it
//! likes, and a duplex close is immediate where a socket close has to
//! travel. A layer whose whole job is reading bytes off a socket should
//! be tested against one.

use std::sync::Arc;
use std::time::Duration;

use broker_core::broker::Broker;
use bytes::BytesMut;
use mqtt_codec::packet::{Connect, Publish, Subscribe, SubscriptionOptions};
use mqtt_codec::{decode, encode, Limits, Packet, Properties, QoS, ReasonCode, Version};
use sdmqtt_server::listener::{Running, Server, ServerConfig};
use sdmqtt_server::shared::Shared;
use storage::record::{ProtocolVersion, Record, SessionKey, SessionRecord, SubscriptionRecord};
use storage::{MemoryStorage, Storage, Writer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// How long a reply may take before the test calls it a failure.
///
/// Generous, because it bounds a real socket round trip on a loaded
/// machine rather than measuring anything.
const REPLY: Duration = Duration::from_secs(5);

fn connect(client_id: &str, keep_alive: u16) -> Packet {
    Packet::Connect(Box::new(Connect {
        version: Version::V5,
        client_id: client_id.into(),
        clean_start: true,
        keep_alive,
        username: None,
        password: None,
        will: None,
        properties: Properties::default(),
    }))
}

fn bytes_of(packet: &Packet) -> BytesMut {
    let mut buf = BytesMut::new();
    encode(packet, Version::V5, &mut buf).expect("the packet encodes");
    buf
}

async fn send(stream: &mut TcpStream, packet: &Packet) {
    let buf = bytes_of(packet);
    stream.write_all(&buf).await.expect("the write succeeds");
    stream.flush().await.expect("the flush succeeds");
}

/// The next packet, or `None` when the peer closed first.
async fn next(stream: &mut TcpStream, buf: &mut BytesMut) -> Option<Packet> {
    loop {
        if let Some(packet) = decode(buf, Version::V5, &Limits::default()).expect("a valid packet")
        {
            return Some(packet);
        }
        let mut chunk = [0u8; 1024];
        let read = tokio::time::timeout(REPLY, stream.read(&mut chunk))
            .await
            .expect("the server answers or closes")
            .expect("the socket is readable");
        if read == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..read]);
    }
}

/// Connect a client and wait for its acknowledgement.
async fn handshake(running: &Running, client_id: &str, keep_alive: u16) -> (TcpStream, BytesMut) {
    let mut stream = TcpStream::connect(running.address())
        .await
        .expect("the listener accepts");
    let mut buf = BytesMut::new();
    send(&mut stream, &connect(client_id, keep_alive)).await;
    match next(&mut stream, &mut buf).await {
        Some(Packet::ConnAck(ack)) => assert_eq!(ack.reason, ReasonCode::Success),
        other => panic!("expected an acknowledgement, got {other:?}"),
    }
    (stream, buf)
}

#[tokio::test]
async fn a_subscriber_receives_what_another_client_publishes() {
    let running = Server::bind(ServerConfig::ephemeral())
        .await
        .expect("the server binds");

    let (mut subscriber, mut subscriber_buf) = handshake(&running, "subscriber", 0).await;
    send(
        &mut subscriber,
        &Packet::Subscribe(Subscribe {
            packet_id: 1,
            filters: vec![SubscriptionOptions {
                filter: "over/tcp".into(),
                qos: QoS::AtMostOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: mqtt_codec::packet::RetainHandling::SendAlways,
            }],
            properties: Properties::default(),
        }),
    )
    .await;
    match next(&mut subscriber, &mut subscriber_buf).await {
        Some(Packet::SubAck(ack)) => assert_eq!(ack.reasons, vec![ReasonCode::Success]),
        other => panic!("expected a subscribe acknowledgement, got {other:?}"),
    }

    let (mut publisher, _) = handshake(&running, "publisher", 0).await;
    send(
        &mut publisher,
        &Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: "over/tcp".into(),
            packet_id: None,
            payload: bytes::Bytes::from_static(b"over a real socket"),
            properties: Properties::default(),
        }),
    )
    .await;

    match next(&mut subscriber, &mut subscriber_buf).await {
        Some(Packet::Publish(publish)) => {
            assert_eq!(publish.topic, "over/tcp");
            assert_eq!(&publish.payload[..], b"over a real socket");
        }
        other => panic!("expected the publication, got {other:?}"),
    }

    running.shutdown().await;
}

#[tokio::test]
async fn a_packet_split_across_two_writes_is_reassembled() {
    // The difference a real socket makes. A duplex stream delivers a
    // write whole; a socket delivers whatever has arrived, so a shell
    // that assumed one read is one packet works in the unit tests and
    // fails here.
    let running = Server::bind(ServerConfig::ephemeral())
        .await
        .expect("the server binds");

    let mut stream = TcpStream::connect(running.address())
        .await
        .expect("the listener accepts");
    let whole = bytes_of(&connect("split", 0));
    let (head, tail) = whole.split_at(3);
    stream.write_all(head).await.unwrap();
    stream.flush().await.unwrap();
    // Long enough that the two halves arrive in separate reads rather
    // than being coalesced into one, which is the case under test.
    tokio::time::sleep(Duration::from_millis(50)).await;
    stream.write_all(tail).await.unwrap();
    stream.flush().await.unwrap();

    let mut buf = BytesMut::new();
    match next(&mut stream, &mut buf).await {
        Some(Packet::ConnAck(ack)) => assert_eq!(ack.reason, ReasonCode::Success),
        other => panic!("expected an acknowledgement, got {other:?}"),
    }

    running.shutdown().await;
}

/// Wait for a connection that has just been displaced to finish
/// unwinding, and therefore to have run the detach it owes the broker.
///
/// There is nothing the correct path leaves behind to wait for: the
/// guard under test fires by doing nothing at all. So the wait is
/// bounded by the damage instead. It ends the moment the session's
/// outbox disappears, which is what a detach by the displaced
/// connection does and what closes the newcomer's socket, and
/// otherwise spends the whole budget — three orders of magnitude
/// longer than the scheduler hops between the displaced socket closing
/// and the task that owned it dropping its attachment.
async fn displaced_connection_unwound(running: &Running) {
    for _ in 0..50 {
        if running.shared().outbox_count() == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_displaced_connection_does_not_disconnect_the_session_it_lost() {
    // An ordinary takeover, which is the most common event on this
    // path, and one deleted condition away from killing the newcomer:
    // the displaced connection unwinds a moment after the newcomer has
    // claimed the handle, and without the epoch check its detach
    // disconnects a live client and arms a will nobody is owed.
    //
    // It needs two real connections and the race between them, so it
    // cannot be driven from a unit test that builds the pieces by
    // hand. The assertion is on the newcomer's own socket, because
    // what the defect does is close it.
    let running = Server::bind(ServerConfig::ephemeral())
        .await
        .expect("the server binds");

    let (mut displaced, mut displaced_buf) = handshake(&running, "contested", 0).await;
    let (mut newcomer, mut newcomer_buf) = handshake(&running, "contested", 0).await;

    // Both halves of the incumbent's ending are waited for. The
    // disconnect says the takeover happened; the close says its writer
    // has finished, which is the last thing before the connection task
    // unwinds into the detach under test.
    match next(&mut displaced, &mut displaced_buf).await {
        Some(Packet::Disconnect(disconnect)) => {
            assert_eq!(disconnect.reason, ReasonCode::SessionTakenOver);
        }
        other => panic!("expected the takeover disconnect, got {other:?}"),
    }
    assert!(
        next(&mut displaced, &mut displaced_buf).await.is_none(),
        "the displaced connection is closed, not left open"
    );
    drop(displaced);
    displaced_connection_unwound(&running).await;

    // The positive check, so this is not an assertion that nothing
    // happened: the newcomer is still being served by the session it
    // took over.
    let ping = bytes_of(&Packet::PingReq);
    assert!(
        newcomer.write_all(&ping).await.is_ok(),
        "the newcomer's socket was closed by the connection it displaced"
    );
    match next(&mut newcomer, &mut newcomer_buf).await {
        Some(Packet::PingResp) => {}
        None => panic!("the newcomer's socket was closed by the connection it displaced"),
        other => panic!("expected a ping response, got {other:?}"),
    }

    running.shutdown().await;
}

#[tokio::test]
async fn a_disconnect_closes_the_socket_at_the_other_end() {
    // The other difference. A duplex stream's close is visible the
    // instant it happens; a socket's has to be written, shut down and
    // travel, and a shell that drops the write half without shutting it
    // down leaves the client waiting on a connection nobody is serving.
    let running = Server::bind(ServerConfig::ephemeral())
        .await
        .expect("the server binds");

    let (mut stream, mut buf) = handshake(&running, "goodbye", 0).await;
    send(
        &mut stream,
        &Packet::Disconnect(mqtt_codec::packet::Disconnect {
            reason: ReasonCode::Success,
            properties: Properties::default(),
        }),
    )
    .await;

    assert!(
        next(&mut stream, &mut buf).await.is_none(),
        "the server closed the connection after the goodbye"
    );

    running.shutdown().await;
}

#[tokio::test]
async fn a_silent_client_is_closed_once_its_keep_alive_lapses() {
    // This is the only test in the suite that waits on wall time, and it
    // does so because the thing under test is the real ticker: keep-alive
    // exists only because a task advances the clock once a second, and a
    // paused clock would prove the broker's arithmetic rather than that
    // anything drives it. The client's interval is one second, and
    // section 3.1.2.10 expires it at one and a half of those, so this
    // costs about three seconds and no more.
    let running = Server::bind(ServerConfig::ephemeral())
        .await
        .expect("the server binds");

    let (mut stream, mut buf) = handshake(&running, "silent", 1).await;

    let closed = tokio::time::timeout(Duration::from_secs(20), async {
        match next(&mut stream, &mut buf).await {
            Some(Packet::Disconnect(disconnect)) => Some(disconnect.reason),
            Some(other) => panic!("expected a disconnect, got {other:?}"),
            None => None,
        }
    })
    .await
    .expect("the silent client is closed rather than left attached");

    assert_eq!(
        closed,
        Some(ReasonCode::KeepAliveTimeout),
        "the client is told why it was closed"
    );

    running.shutdown().await;
}

#[tokio::test]
async fn a_connection_past_the_limit_is_closed_rather_than_queued() {
    // A queued connection that eventually gets in is worse for the
    // client than a prompt refusal it can retry elsewhere, so the cap
    // closes rather than waits. Without this the semaphore is a path no
    // test walks: every other test here stays far below any limit.
    let config = ServerConfig {
        max_connections: 1,
        ..ServerConfig::ephemeral()
    };
    let running = Server::bind(config).await.expect("the server binds");

    // The handshake is what proves the first connection is being served
    // and is therefore holding the only permit.
    let (_first, _buf) = handshake(&running, "first", 0).await;

    let mut second = TcpStream::connect(running.address())
        .await
        .expect("the kernel completes the handshake from its backlog");
    let mut chunk = [0u8; 16];
    let read = tokio::time::timeout(REPLY, second.read(&mut chunk))
        .await
        .expect("the refusal is prompt rather than a wait in a queue")
        .expect("the socket is readable");
    assert_eq!(read, 0, "the connection past the limit was closed");

    running.shutdown().await;
}

#[tokio::test]
async fn a_peer_that_stops_reading_is_cut_and_its_permit_and_outbox_released() {
    // Keep-alive watches what a client sends, and a client that keeps
    // sending while never reading passes it; the close that would end
    // it then sits behind a write the kernel cannot complete. Without a
    // bound on that write the task, its permit and its outbox entry
    // outlive the session the broker has already released. Two permits
    // here: the stalled subscriber's and the publisher's, so that a
    // third connection is refused until the stalled one is cut.
    let config = ServerConfig {
        max_connections: 2,
        connection: sdmqtt_server::connection::ConnectionConfig {
            write_timeout: Duration::from_millis(500),
            ..Default::default()
        },
        ..ServerConfig::ephemeral()
    };
    let running = Server::bind(config).await.expect("the server binds");

    let (mut stalled, mut stalled_buf) = handshake(&running, "stalled", 0).await;
    send(
        &mut stalled,
        &Packet::Subscribe(Subscribe {
            packet_id: 1,
            filters: vec![SubscriptionOptions {
                filter: "flood".into(),
                qos: QoS::AtMostOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: mqtt_codec::packet::RetainHandling::SendAlways,
            }],
            properties: Properties::default(),
        }),
    )
    .await;
    assert!(matches!(
        next(&mut stalled, &mut stalled_buf).await,
        Some(Packet::SubAck(_))
    ));

    // From here the subscriber reads nothing. The kernel's loopback
    // buffers are a few megabytes at most, and this is far more.
    let (mut publisher, _) = handshake(&running, "publisher", 0).await;
    let payload = bytes::Bytes::from(vec![0_u8; 512 * 1024]);
    for _ in 0..48 {
        send(
            &mut publisher,
            &Packet::Publish(Publish {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: false,
                topic: "flood".into(),
                packet_id: None,
                payload: payload.clone(),
                properties: Properties::default(),
            }),
        )
        .await;
    }
    assert_eq!(running.shared().outbox_count(), 2);

    let mut refused = TcpStream::connect(running.address())
        .await
        .expect("the kernel completes the handshake from its backlog");
    let mut chunk = [0u8; 16];
    let read = tokio::time::timeout(REPLY, refused.read(&mut chunk))
        .await
        .expect("the refusal is prompt")
        .expect("the socket is readable");
    assert_eq!(read, 0, "both permits are held while the writer is stalled");

    // The stalled connection's task ends, and with it its outbox entry.
    let mut released = false;
    for _ in 0..200 {
        if running.shared().outbox_count() == 1 {
            released = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        released,
        "the connection whose peer stopped reading was never cut"
    );

    // And its permit: the task that held it has returned, so a new
    // connection is served rather than refused.
    let (_third, _) = handshake(&running, "third", 0).await;

    drop(stalled);
    running.shutdown().await;
}

/// Connect without a clean start, and hand back what the broker said.
/// Connect without a clean start, and hand back what the broker said.
///
/// The resumption half of the handshake above, which always starts
/// clean: a session can only be observed to have survived by a client
/// that asks for the one it had.
async fn resume(running: &Running, client_id: &str) -> (TcpStream, BytesMut, bool) {
    let mut stream = TcpStream::connect(running.address())
        .await
        .expect("the listener accepts");
    let mut buf = BytesMut::new();
    send(
        &mut stream,
        &Packet::Connect(Box::new(Connect {
            version: Version::V5,
            client_id: client_id.into(),
            clean_start: false,
            keep_alive: 0,
            username: None,
            password: None,
            will: None,
            properties: Properties::default(),
        })),
    )
    .await;
    let present = match next(&mut stream, &mut buf).await {
        Some(Packet::ConnAck(ack)) => {
            assert_eq!(ack.reason, ReasonCode::Success);
            ack.session_present
        }
        other => panic!("expected an acknowledgement, got {other:?}"),
    };
    (stream, buf, present)
}

#[tokio::test]
async fn what_restore_built_is_the_broker_the_listener_serves() {
    // Section 7.5, walked rather than constructed. Restore is proved
    // in `broker-core` and the shell's own tests call it directly, so
    // every assertion about it is made against a `Shared` the test
    // holds. Nothing asked whether the `Shared` a listener is given is
    // the one its clients reach — and a `bind_shared` that quietly
    // built a fresh broker instead passed the entire workspace suite.
    // Convention 5: a test that constructs the type instead of walking
    // the path.
    //
    // So this walks it: a store, a restore, a listener bound against
    // what came back, and a real socket asking for the session.
    let store = Arc::new(MemoryStorage::new());
    let key = SessionKey::new("", "resumed");
    store
        .apply(&[
            Record::SessionPut(SessionRecord {
                key: key.clone(),
                version: ProtocolVersion::V5,
                expiry_interval: 3_600,
                // The broker's clock is restored to `now` below, and
                // both are counted from zero here rather than from the
                // wall clock: what this test is about is which broker
                // the listener serves, not which sessions outlived an
                // outage, and a stored expiry on an invented scale
                // cannot go stale while the test runs.
                expires_at: Some(3_600),
                will: None,
                will_at: None,
            }),
            Record::SubscriptionAdded {
                session: key,
                subscription: SubscriptionRecord {
                    filter: "restored/filter".to_owned(),
                    qos: storage::Qos::AtMostOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: storage::RetainHandling::SendAlways,
                    subscription_id: None,
                },
            },
        ])
        .expect("the store takes them");

    let config = ServerConfig::ephemeral();
    let mut broker = Broker::new(config.broker.clone());
    let report =
        broker_core::restore::restore(&mut broker, &store.restore().expect("the store reads"), 0);
    assert_eq!(report.sessions, 1, "the fixture restored a session");

    let writer = Writer::spawn(
        Arc::clone(&store) as Arc<dyn Storage>,
        storage::WriterConfig::default(),
    )
    .expect("a writer thread");
    let running = Server::bind_shared(
        config,
        Arc::new(Shared::with_store(broker, Arc::new(writer))),
    )
    .await
    .expect("the server binds");

    let (mut resumed, mut resumed_buf, present) = resume(&running, "resumed").await;
    assert!(
        present,
        "the session the store held is the one a client reaches"
    );

    // And the other half, which a session-only rebuild gets wrong: the
    // filter is in the matching trie of the broker actually serving,
    // not merely on a session in it.
    let (mut publisher, _) = handshake(&running, "publisher", 0).await;
    send(
        &mut publisher,
        &Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: "restored/filter".into(),
            packet_id: None,
            payload: bytes::Bytes::from_static(b"to a restored subscription"),
            properties: Properties::default(),
        }),
    )
    .await;
    match next(&mut resumed, &mut resumed_buf).await {
        Some(Packet::Publish(publish)) => {
            assert_eq!(&publish.payload[..], b"to a restored subscription");
        }
        other => panic!("expected the publication, got {other:?}"),
    }

    // The discrimination. A broker that reported every session present
    // would pass everything above.
    let (_stranger, _stranger_buf, stranger_present) = resume(&running, "stranger").await;
    assert!(
        !stranger_present,
        "a session the store never held is not present"
    );

    running.shutdown().await;
}
