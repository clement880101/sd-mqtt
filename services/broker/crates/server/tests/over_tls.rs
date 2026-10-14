//! A client speaks MQTT through a TLS handshake, and a renewed
//! certificate is what the next client sees.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use mqtt_codec::packet::Connect;
use mqtt_codec::{decode, encode, Limits, Packet, Properties, ReasonCode, Version};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, ServerName};
use sdmqtt_server::listener::{Kind, Server, ServerConfig};
use sdmqtt_server::tls;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

const REPLY: Duration = Duration::from_secs(5);

/// A certificate and key on disk, minted for the test.
fn minted(directory: &Path, name: &str) -> (PathBuf, PathBuf) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("rcgen mints a certificate");
    let certificate = directory.join(format!("{name}.pem"));
    let key = directory.join(format!("{name}.key"));
    std::fs::write(&certificate, cert.cert.pem()).expect("written");
    std::fs::write(&key, cert.signing_key.serialize_pem()).expect("written");
    (certificate, key)
}

fn der_of(certificate: &Path) -> Vec<u8> {
    CertificateDer::from_pem_file(certificate)
        .expect("a certificate")
        .to_vec()
}

/// A connector trusting exactly one certificate.
fn trusting(certificate: &Path) -> TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_file(certificate).expect("a certificate"))
        .expect("a self-signed certificate is its own root");
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("the default versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

/// Handshake with the listener, trusting `certificate`.
async fn handshake(
    address: std::net::SocketAddr,
    certificate: &Path,
) -> std::io::Result<TlsStream<TcpStream>> {
    let socket = TcpStream::connect(address).await?;
    trusting(certificate)
        .connect(ServerName::try_from("localhost").expect("a name"), socket)
        .await
}

/// CONNECT over the stream, and the CONNACK's reason.
async fn connect_over(stream: &mut TlsStream<TcpStream>) -> ReasonCode {
    let packet = Packet::Connect(Box::new(Connect {
        version: Version::V5,
        client_id: "secure".into(),
        clean_start: true,
        keep_alive: 0,
        username: None,
        password: None,
        will: None,
        properties: Properties::default(),
    }));
    let mut out = BytesMut::new();
    encode(&packet, Version::V5, &mut out).expect("encodes");
    stream.write_all(&out).await.expect("written");
    stream.flush().await.expect("flushed");
    let mut buf = BytesMut::new();
    let deadline = tokio::time::Instant::now() + REPLY;
    loop {
        if let Some(Packet::ConnAck(ack)) =
            decode(&mut buf, Version::V5, &Limits::default()).expect("valid packets")
        {
            return ack.reason;
        }
        let mut chunk = [0u8; 1024];
        let read = tokio::time::timeout_at(deadline, stream.read(&mut chunk))
            .await
            .expect("an answer within the deadline")
            .expect("a read");
        assert!(read > 0, "the listener closed before answering");
        buf.extend_from_slice(&chunk[..read]);
    }
}

#[tokio::test]
async fn a_client_connects_over_tls_and_the_next_one_sees_the_renewed_certificate() {
    let directory = tempfile::tempdir().expect("a directory");
    let (certificate, key) = minted(directory.path(), "first");
    let server = Server::bind(ServerConfig {
        kind: Kind::Tls(tls::Settings {
            certificate: certificate.clone(),
            key: key.clone(),
            reload: Duration::from_millis(30),
        }),
        ..ServerConfig::ephemeral()
    })
    .await
    .expect("a TLS listener binds");
    let acceptor = Arc::clone(server.tls().expect("a TLS listener has an acceptor"));
    let first_der = der_of(&certificate);
    assert_eq!(acceptor.certificate_der(), first_der);

    // The first client: handshake, CONNECT, CONNACK, and the certificate
    // it was shown is the one on disk.
    let mut client = handshake(server.address(), &certificate)
        .await
        .expect("the handshake completes against the minted certificate");
    assert_eq!(connect_over(&mut client).await, ReasonCode::Success);
    let shown = client
        .get_ref()
        .1
        .peer_certificates()
        .expect("a certificate was presented")[0]
        .to_vec();
    assert_eq!(shown, first_der);
    drop(client);

    // A renewal lands: the files are replaced.
    let (renewed_cert, renewed_key) = minted(directory.path(), "second");
    let second_der = der_of(&renewed_cert);
    std::fs::copy(&renewed_key, &key).expect("key replaced");
    std::fs::copy(&renewed_cert, &certificate).expect("certificate replaced");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while acceptor.certificate_der() != second_der {
        assert!(
            std::time::Instant::now() < deadline,
            "the renewal was never picked up"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The next client, trusting the renewed certificate, is shown it;
    // one still trusting the first alone is refused, because the old
    // pair is gone.
    let mut renewed = handshake(server.address(), &renewed_cert)
        .await
        .expect("the handshake completes against the renewed certificate");
    assert_eq!(connect_over(&mut renewed).await, ReasonCode::Success);
    assert_eq!(
        renewed.get_ref().1.peer_certificates().expect("presented")[0].to_vec(),
        second_der
    );
    let stale = directory.path().join("stale.pem");
    std::fs::write(&stale, {
        // The first certificate, kept aside before it was overwritten.
        let mut out = String::new();
        for line in rcgen_pem(&first_der).lines() {
            out.push_str(line);
            out.push('\n');
        }
        out
    })
    .expect("written");
    let refused = handshake(server.address(), &stale).await;
    assert!(
        refused.is_err(),
        "a client trusting only the old certificate must not handshake"
    );
    // Counted on the server side once its half of the handshake fails,
    // which is a moment after the client's half did.
    let counted = std::time::Instant::now() + REPLY;
    while server.shared().metrics().tls().handshakes_failed.get() == 0 {
        assert!(
            std::time::Instant::now() < counted,
            "the failed handshake was never counted"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    server.shutdown().await;
}

/// The first certificate as PEM again, from its DER, so a client can
/// be given it after the file on disk was overwritten.
fn rcgen_pem(der: &[u8]) -> String {
    use base64_lite::encode;
    let body = encode(der);
    let mut pem = String::from("-----BEGIN CERTIFICATE-----\n");
    for chunk in body.as_bytes().chunks(64) {
        pem.push_str(std::str::from_utf8(chunk).expect("ascii"));
        pem.push('\n');
    }
    pem.push_str("-----END CERTIFICATE-----\n");
    pem
}

/// Standard base64, by hand, so the test adds no dependency.
mod base64_lite {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    pub fn encode(bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
        for chunk in bytes.chunks(3) {
            let mut triple = [0u8; 3];
            triple[..chunk.len()].copy_from_slice(chunk);
            let n = u32::from_be_bytes([0, triple[0], triple[1], triple[2]]);
            let quads = [(n >> 18) & 63, (n >> 12) & 63, (n >> 6) & 63, n & 63];
            for (at, quad) in quads.into_iter().enumerate() {
                if at <= chunk.len() {
                    out.push(TABLE[quad as usize] as char);
                } else {
                    out.push('=');
                }
            }
        }
        out
    }
}
