//! The directory on a broker, through the connection: who gets in.

use std::sync::Arc;

use broker_core::broker::{Broker, BrokerConfig};
use bytes::BytesMut;
use cluster::{HybridClock, NodeId, Replicated, DEFAULT_MAX_DRIFT_MILLIS};
use directory::{Directory, Settings};
use mqtt_codec::packet::Connect;
use mqtt_codec::{decode, encode, Limits, Packet, Properties, ReasonCode, Version};
use sdmqtt_server::connection::{serve, ConnectionConfig};
use sdmqtt_server::shared::Shared;
use storage::record::Secret;
use storage::{MemoryStorage, Storage, Writer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A broker with a directory installed, on an in-memory store.
fn broker_with(settings: Settings) -> (Arc<Shared>, Arc<Directory>) {
    let store = Arc::new(MemoryStorage::new());
    let writer = Arc::new(
        Writer::spawn(
            Arc::clone(&store) as Arc<dyn Storage>,
            storage::WriterConfig::default(),
        )
        .expect("a writer thread"),
    );
    let replicated = Arc::new(
        Replicated::open(
            NodeId::mint(),
            HybridClock::resume(None, DEFAULT_MAX_DRIFT_MILLIS),
            &storage::Restored::default(),
        )
        .expect("an empty store holds nothing to refuse"),
    );
    let shared = Arc::new(Shared::with_replication(
        Broker::new(BrokerConfig::default()),
        Arc::clone(&writer),
        Arc::clone(&replicated),
    ));
    let directory = Directory::new(
        writer,
        Arc::clone(&shared),
        replicated,
        settings,
        Vec::new(),
        Vec::new(),
    );
    shared.with_broker(|broker| broker.hooks_mut().install(directory.hook()));
    (shared, directory)
}

/// CONNECT with these credentials, and the CONNACK's reason.
async fn connect_as(
    shared: &Arc<Shared>,
    username: Option<&str>,
    password: Option<&[u8]>,
) -> ReasonCode {
    let (mut client, server) = tokio::io::duplex(8192);
    let shared_for_server = Arc::clone(shared);
    tokio::spawn(async move {
        let _ = serve(server, shared_for_server, ConnectionConfig::default()).await;
    });
    let packet = Packet::Connect(Box::new(Connect {
        version: Version::V5,
        client_id: "probe".into(),
        clean_start: true,
        keep_alive: 0,
        username: username.map(str::to_owned),
        password: password.map(bytes::Bytes::copy_from_slice),
        will: None,
        properties: Properties::default(),
    }));
    let mut out = BytesMut::new();
    encode(&packet, Version::V5, &mut out).expect("encodes");
    client.write_all(&out).await.expect("written");
    let mut buf = BytesMut::new();
    loop {
        if let Some(Packet::ConnAck(ack)) =
            decode(&mut buf, Version::V5, &Limits::default()).expect("valid packets")
        {
            return ack.reason;
        }
        let mut chunk = [0u8; 1024];
        let read = client.read(&mut chunk).await.expect("a read");
        assert!(read > 0, "the broker closed without a connack");
        buf.extend_from_slice(&chunk[..read]);
    }
}

#[tokio::test]
async fn a_user_with_the_right_password_connects_and_the_wrong_one_is_refused() {
    let (shared, directory) = broker_with(Settings::default());
    directory
        .put_user("alice", &Secret::new("hunter2"))
        .await
        .expect("created");
    assert_eq!(
        connect_as(&shared, Some("alice"), Some(b"hunter2")).await,
        ReasonCode::Success
    );
    assert_eq!(
        connect_as(&shared, Some("alice"), Some(b"hunter3")).await,
        ReasonCode::NotAuthorized
    );
    assert_eq!(
        connect_as(&shared, Some("nobody"), Some(b"hunter2")).await,
        ReasonCode::NotAuthorized,
        "a name nobody has and a wrong password are one answer"
    );
    assert_eq!(
        connect_as(&shared, Some("alice"), None).await,
        ReasonCode::NotAuthorized,
        "a name with no password is not a password login"
    );
}

/// Anonymous is the switch, and nothing else.
#[tokio::test]
async fn anonymous_is_admitted_by_the_switch_and_refused_without_it() {
    let (open, directory) = broker_with(Settings::default());
    assert_eq!(connect_as(&open, None, None).await, ReasonCode::Success);
    // Creating a user does not flip the switch.
    directory
        .put_user("alice", &Secret::new("hunter2"))
        .await
        .expect("created");
    assert_eq!(connect_as(&open, None, None).await, ReasonCode::Success);

    let (closed, _) = broker_with(Settings {
        anonymous: false,
        ..Settings::default()
    });
    assert_eq!(
        connect_as(&closed, None, None).await,
        ReasonCode::NotAuthorized
    );
}

/// A deleted user is refused at once.
#[tokio::test]
async fn a_deleted_user_is_refused() {
    let (shared, directory) = broker_with(Settings::default());
    directory
        .put_user("alice", &Secret::new("hunter2"))
        .await
        .expect("created");
    directory.delete_user("alice").await.expect("deleted");
    assert_eq!(
        connect_as(&shared, Some("alice"), Some(b"hunter2")).await,
        ReasonCode::NotAuthorized
    );
}
