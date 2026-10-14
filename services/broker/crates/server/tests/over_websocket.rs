//! MQTT over WebSocket through real sockets, plain and over TLS.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use mqtt_codec::packet::{Connect, Publish, Subscribe, SubscriptionOptions};
use mqtt_codec::{decode, encode, Limits, Packet, Properties, QoS, ReasonCode, Version};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, ServerName};
use sdmqtt_server::listener::{Kind, Running, Server, ServerConfig};
use sdmqtt_server::tls;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

fn minted(directory: &Path) -> (PathBuf, PathBuf) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("rcgen mints a certificate");
    let certificate = directory.join("cert.pem");
    let key = directory.join("key.pem");
    std::fs::write(&certificate, cert.cert.pem()).expect("written");
    std::fs::write(&key, cert.signing_key.serialize_pem()).expect("written");
    (certificate, key)
}

fn encoded(packet: &Packet) -> bytes::Bytes {
    let mut out = BytesMut::new();
    encode(packet, Version::V5, &mut out).expect("encodes");
    out.freeze()
}

/// The next MQTT packet from the socket's binary frames.
async fn next_packet<S>(socket: &mut WebSocketStream<S>, buf: &mut BytesMut) -> Packet
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        if let Some(packet) =
            decode(buf, Version::V5, &Limits::default()).expect("the broker sends valid packets")
        {
            return packet;
        }
        match tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("a frame within five seconds")
        {
            Some(Ok(Message::Binary(data))) => buf.extend_from_slice(&data),
            other => panic!("expected a binary frame, got {other:?}"),
        }
    }
}

/// CONNECT, SUBSCRIBE, PUBLISH to oneself, and the delivery, over an
/// upgraded socket of any kind.
async fn speaks_mqtt<S>(mut socket: WebSocketStream<S>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf = BytesMut::new();
    socket
        .send(Message::Binary(encoded(&Packet::Connect(Box::new(
            Connect {
                version: Version::V5,
                client_id: "over-websocket".into(),
                clean_start: true,
                keep_alive: 0,
                username: None,
                password: None,
                will: None,
                properties: Properties::default(),
            },
        )))))
        .await
        .expect("sent");
    match next_packet(&mut socket, &mut buf).await {
        Packet::ConnAck(ack) => assert_eq!(ack.reason, ReasonCode::Success),
        other => panic!("expected a connack, got {other:?}"),
    }
    socket
        .send(Message::Binary(encoded(&Packet::Subscribe(Subscribe {
            packet_id: 1,
            filters: vec![SubscriptionOptions {
                filter: "ws/+".into(),
                qos: QoS::AtMostOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: mqtt_codec::packet::RetainHandling::DoNotSend,
            }],
            properties: Properties::default(),
        }))))
        .await
        .expect("sent");
    match next_packet(&mut socket, &mut buf).await {
        Packet::SubAck(ack) => assert_eq!(ack.reasons, vec![ReasonCode::Success]),
        other => panic!("expected a suback, got {other:?}"),
    }
    socket
        .send(Message::Binary(encoded(&Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: "ws/hello".into(),
            packet_id: None,
            payload: bytes::Bytes::from_static(b"framed"),
            properties: Properties::default(),
        }))))
        .await
        .expect("sent");
    match next_packet(&mut socket, &mut buf).await {
        Packet::Publish(publish) => assert_eq!(&publish.payload[..], b"framed"),
        other => panic!("expected the delivery, got {other:?}"),
    }
}

fn request(
    address: std::net::SocketAddr,
) -> tokio_tungstenite::tungstenite::handshake::client::Request {
    let mut request = format!("ws://{address}/mqtt")
        .into_client_request()
        .expect("a request");
    request
        .headers_mut()
        .insert("sec-websocket-protocol", "mqtt".parse().expect("ascii"));
    request
}

async fn bound(kind: Kind) -> Running {
    Server::bind(ServerConfig {
        kind,
        ..ServerConfig::ephemeral()
    })
    .await
    .expect("binds")
}

#[tokio::test]
async fn a_client_speaks_mqtt_over_a_plain_websocket() {
    let server = bound(Kind::Ws).await;
    let socket = TcpStream::connect(server.address())
        .await
        .expect("connects");
    let (socket, response) = tokio_tungstenite::client_async(request(server.address()), socket)
        .await
        .expect("the upgrade is accepted");
    assert_eq!(
        response
            .headers()
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok()),
        Some("mqtt"),
        "the server confirms the subprotocol"
    );
    speaks_mqtt(socket).await;
    server.shutdown().await;
}

#[tokio::test]
async fn a_client_speaks_mqtt_over_a_websocket_over_tls() {
    let directory = tempfile::tempdir().expect("a directory");
    let (certificate, key) = minted(directory.path());
    let server = bound(Kind::Wss(tls::Settings {
        certificate: certificate.clone(),
        key,
        reload: Duration::from_secs(60),
    }))
    .await;

    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_file(&certificate).expect("a certificate"))
        .expect("its own root");
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("the default versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    let socket = TcpStream::connect(server.address())
        .await
        .expect("connects");
    let socket = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(ServerName::try_from("localhost").expect("a name"), socket)
        .await
        .expect("the handshake completes");
    let (socket, _) = tokio_tungstenite::client_async(request(server.address()), socket)
        .await
        .expect("the upgrade is accepted under TLS");
    speaks_mqtt(socket).await;
    server.shutdown().await;
}

/// An upgrade without the subprotocol is refused on a real socket too,
/// and counted.
#[tokio::test]
async fn an_upgrade_without_the_subprotocol_is_refused_and_counted() {
    let server = bound(Kind::Ws).await;
    let socket = TcpStream::connect(server.address())
        .await
        .expect("connects");
    let bare = format!("ws://{}/", server.address())
        .into_client_request()
        .expect("a request");
    let refused = tokio_tungstenite::client_async(bare, socket)
        .await
        .expect_err("refused");
    assert!(
        matches!(refused, tokio_tungstenite::tungstenite::Error::Http(ref r) if r.status() == 400),
        "{refused:?}"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while server
        .shared()
        .metrics()
        .websocket_refused("no_subprotocol")
        .get()
        == 0
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the refusal was never counted"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    server.shutdown().await;
}
