//! MQTT over WebSocket, section 12.1.
//!
//! # A WebSocket is a byte stream with a frame around it
//!
//! The MQTT specification's WebSocket clause (section 6 of both
//! versions) says MQTT control packets are sent in binary frames, a
//! frame may carry part of a packet or several packets, and the
//! subprotocol is `mqtt`. So [`Bytes`] turns a stream of binary
//! messages into `AsyncRead` and each write into one binary message,
//! and [`crate::connection::serve`] never learns there was a frame.
//! Text frames close the connection with 1003, which is RFC 6455's
//! "unsupported data"; pings are answered by tungstenite underneath and
//! reach nothing here.
//!
//! # The path is not checked
//!
//! Every path upgrades. Client libraries default to `/mqtt`, `/ws`, `/`
//! and others, and a broker that refused all but one of them would be
//! the broker that is wrong in every tutorial. The subprotocol is what
//! says this is MQTT, and that *is* required: an upgrade that does not
//! offer `mqtt` is refused with 400 before anything is served.
//!
//! # One frame per write, not per packet
//!
//! The connection's writer batches, so one `poll_write` is often
//! several packets; they go in one binary frame, which the clause
//! allows and which is cheaper than a frame per packet. A reader that
//! wanted one packet per frame would have been promised something the
//! standard does not promise.
//!
//! # A write is not done until the frame has left
//!
//! tungstenite queues a frame on `start_send` and writes it to the
//! socket on flush, while the connection's writer flushes only at the
//! end of a burst, because a TCP socket and a TLS stream both write
//! through on `poll_write`. So this adapter flushes inside
//! `poll_write` and reports the write complete only when the frame is
//! on the wire — holding the length of a frame it has queued but not
//! yet sent, so the caller's retry with the same bytes is answered
//! rather than sent twice. Found by the first real-socket test: the
//! acknowledgement went out on the burst's flush and the next packet
//! sat in the queue until something else was written.

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use futures_util::{Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// The one subprotocol MQTT over WebSocket names.
pub const SUBPROTOCOL: &str = "mqtt";

/// Why an upgrade was not served.
#[derive(Debug, thiserror::Error)]
pub enum Refused {
    /// The request did not name `mqtt` among its subprotocols.
    #[error("the request did not offer the mqtt subprotocol")]
    NoMqttSubprotocol,
    /// The upgrade did not finish in time.
    #[error("the upgrade did not complete within {0:?}")]
    TimedOut(Duration),
    /// The HTTP exchange was not a WebSocket upgrade, or failed.
    #[error("the WebSocket handshake failed: {0}")]
    Handshake(#[from] tokio_tungstenite::tungstenite::Error),
}

impl Refused {
    /// The label this refusal is counted under.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::NoMqttSubprotocol => "no_subprotocol",
            Self::TimedOut(_) => "timed_out",
            Self::Handshake(_) => "handshake",
        }
    }
}

/// Perform the server handshake, insisting on the `mqtt` subprotocol.
// The callback's error type is tungstenite's `ErrorResponse`, an HTTP
// response, and its size is the library's choice rather than ours.
#[allow(clippy::result_large_err)]
pub async fn upgrade<S>(socket: S, timeout: Duration) -> Result<Bytes<S>, Refused>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let offered = Arc::new(AtomicBool::new(false));
    let seen = Arc::clone(&offered);
    let callback = move |request: &Request, mut response: Response| {
        let offers_mqtt = request
            .headers()
            .get_all("sec-websocket-protocol")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .any(|name| name.trim().eq_ignore_ascii_case(SUBPROTOCOL));
        if !offers_mqtt {
            let mut refused =
                ErrorResponse::new(Some("the mqtt subprotocol is required".to_owned()));
            *refused.status_mut() = StatusCode::BAD_REQUEST;
            return Err(refused);
        }
        seen.store(true, Ordering::Relaxed);
        response.headers_mut().insert(
            "sec-websocket-protocol",
            SUBPROTOCOL.parse().expect("ascii"),
        );
        Ok(response)
    };
    let stream = tokio::time::timeout(
        timeout,
        tokio_tungstenite::accept_hdr_async(socket, callback),
    )
    .await
    .map_err(|_| Refused::TimedOut(timeout))?
    .map_err(|error| {
        if offered.load(Ordering::Relaxed) {
            Refused::Handshake(error)
        } else {
            Refused::NoMqttSubprotocol
        }
    })?;
    Ok(Bytes {
        inner: stream,
        unread: bytes::Bytes::new(),
        queued: None,
    })
}

/// The frames as a byte stream. See the module documentation.
pub struct Bytes<S> {
    inner: WebSocketStream<S>,
    /// What the last binary frame held that the reader has not taken.
    unread: bytes::Bytes,
    /// The length of a frame queued on `start_send` and not yet
    /// flushed, whose write has therefore not been reported.
    queued: Option<usize>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for Bytes<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if !self.unread.is_empty() {
                let take = self.unread.len().min(buf.remaining());
                let chunk = self.unread.split_to(take);
                buf.put_slice(&chunk);
                return Poll::Ready(Ok(()));
            }
            match ready!(Pin::new(&mut self.inner).poll_next(cx)) {
                None | Some(Ok(Message::Close(_))) => return Poll::Ready(Ok(())),
                Some(Err(error)) => return Poll::Ready(Err(io::Error::other(error))),
                Some(Ok(Message::Binary(data))) => self.unread = data,
                // Pings are answered by tungstenite; pongs and raw frames
                // are nobody's business here.
                Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {}
                Some(Ok(Message::Text(_))) => {
                    // The clause: binary only. The close goes out on a
                    // best-effort basis — the connection is ending
                    // either way — and the reader is told why.
                    let close = Message::Close(Some(CloseFrame {
                        code: CloseCode::Unsupported,
                        reason: "MQTT over WebSocket is binary".into(),
                    }));
                    if Pin::new(&mut self.inner).poll_ready(cx).is_ready() {
                        let _ = Pin::new(&mut self.inner).start_send(close);
                        let _ = Pin::new(&mut self.inner).poll_flush(cx);
                    }
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "a text frame on an MQTT WebSocket",
                    )));
                }
            }
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for Bytes<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        // A frame queued last time and not yet on the wire: finish
        // that first, and answer the caller's retry with its length
        // rather than queueing the same bytes again.
        if let Some(len) = self.queued {
            ready!(Pin::new(&mut self.inner).poll_flush(cx)).map_err(io::Error::other)?;
            self.queued = None;
            return Poll::Ready(Ok(len));
        }
        ready!(Pin::new(&mut self.inner).poll_ready(cx)).map_err(io::Error::other)?;
        Pin::new(&mut self.inner)
            .start_send(Message::Binary(bytes::Bytes::copy_from_slice(data)))
            .map_err(io::Error::other)?;
        self.queued = Some(data.len());
        ready!(Pin::new(&mut self.inner).poll_flush(cx)).map_err(io::Error::other)?;
        self.queued = None;
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner)
            .poll_flush(cx)
            .map_err(io::Error::other)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner)
            .poll_close(cx)
            .map_err(io::Error::other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc as StdArc;

    use broker_core::broker::BrokerConfig;
    use bytes::BytesMut;
    use futures_util::{SinkExt, StreamExt};
    use mqtt_codec::packet::Connect;
    use mqtt_codec::{decode, encode, Limits, Packet, Properties, ReasonCode, Version};
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    use crate::connection::{serve, ConnectionConfig};
    use crate::shared::Shared;

    fn connect_bytes() -> bytes::Bytes {
        let packet = Packet::Connect(Box::new(Connect {
            version: Version::V5,
            client_id: "over-ws".into(),
            clean_start: true,
            keep_alive: 0,
            username: None,
            password: None,
            will: None,
            properties: Properties::default(),
        }));
        let mut out = BytesMut::new();
        encode(&packet, Version::V5, &mut out).expect("encodes");
        out.freeze()
    }

    fn decode_one(data: &[u8]) -> Packet {
        let mut buf = BytesMut::from(data);
        decode(&mut buf, Version::V5, &Limits::default())
            .expect("a valid packet")
            .expect("a whole packet")
    }

    /// A served WebSocket end and the client's end, over an in-memory
    /// duplex. `protocols` is what the client offers.
    async fn served(
        protocols: &[&str],
    ) -> (
        StdArc<Shared>,
        tokio::task::JoinHandle<Result<(), String>>,
        WebSocketStream<tokio::io::DuplexStream>,
    ) {
        let shared = StdArc::new(Shared::new(BrokerConfig::default()));
        let (client_half, server_half) = tokio::io::duplex(16 * 1024);
        let serving = tokio::spawn({
            let shared = StdArc::clone(&shared);
            async move {
                let upgraded = upgrade(server_half, Duration::from_secs(5))
                    .await
                    .map_err(|why| why.to_string())?;
                serve(upgraded, shared, ConnectionConfig::default())
                    .await
                    .map_err(|why| why.to_string())
            }
        });
        let mut request = "ws://localhost/".into_client_request().expect("a request");
        if !protocols.is_empty() {
            request.headers_mut().insert(
                "sec-websocket-protocol",
                protocols.join(", ").parse().expect("ascii"),
            );
        }
        let (client, _) = tokio_tungstenite::client_async(request, client_half)
            .await
            .expect("the upgrade is accepted");
        (shared, serving, client)
    }

    /// One MQTT packet in one binary frame, and the reply in one frame.
    #[tokio::test]
    async fn a_connect_in_a_binary_frame_is_served_and_answered_in_one() {
        let (_shared, _serving, mut client) = served(&["mqtt"]).await;
        client
            .send(Message::Binary(connect_bytes()))
            .await
            .expect("sent");
        match client.next().await {
            Some(Ok(Message::Binary(data))) => match decode_one(&data) {
                Packet::ConnAck(ack) => assert_eq!(ack.reason, ReasonCode::Success),
                other => panic!("expected a connack, got {other:?}"),
            },
            other => panic!("expected a binary frame, got {other:?}"),
        }
    }

    /// A packet split across two frames is one packet.
    #[tokio::test]
    async fn a_packet_split_across_frames_is_reassembled() {
        let (_shared, _serving, mut client) = served(&["mqtt"]).await;
        let whole = connect_bytes();
        let (head, tail) = whole.split_at(5);
        client
            .send(Message::Binary(bytes::Bytes::copy_from_slice(head)))
            .await
            .expect("sent");
        tokio::time::sleep(Duration::from_millis(50)).await;
        client
            .send(Message::Binary(bytes::Bytes::copy_from_slice(tail)))
            .await
            .expect("sent");
        match client.next().await {
            Some(Ok(Message::Binary(data))) => {
                assert!(matches!(decode_one(&data), Packet::ConnAck(_)));
            }
            other => panic!("expected a connack frame, got {other:?}"),
        }
    }

    /// A ping between frames is answered and does not reach MQTT.
    #[tokio::test]
    async fn a_ping_is_answered_and_not_seen_by_mqtt() {
        let (_shared, _serving, mut client) = served(&["mqtt"]).await;
        client
            .send(Message::Ping(bytes::Bytes::from_static(b"hello")))
            .await
            .expect("sent");
        match client.next().await {
            Some(Ok(Message::Pong(data))) => assert_eq!(&data[..], b"hello"),
            other => panic!("expected a pong, got {other:?}"),
        }
        client
            .send(Message::Binary(connect_bytes()))
            .await
            .expect("sent");
        match client.next().await {
            Some(Ok(Message::Binary(data))) => {
                assert!(matches!(decode_one(&data), Packet::ConnAck(_)));
            }
            other => panic!("the connection is still MQTT after the ping: {other:?}"),
        }
    }

    /// A text frame is not MQTT: closed with 1003, and nothing served.
    #[tokio::test]
    async fn a_text_frame_closes_the_connection_with_unsupported_data() {
        let (_shared, serving, mut client) = served(&["mqtt"]).await;
        client
            .send(Message::Text("not mqtt".into()))
            .await
            .expect("sent");
        match client.next().await {
            Some(Ok(Message::Close(Some(frame)))) => {
                assert_eq!(frame.code, CloseCode::Unsupported);
            }
            other => panic!("expected a close with 1003, got {other:?}"),
        }
        let ended = serving.await.expect("the task finishes");
        assert!(
            ended.is_err(),
            "the connection ended in an error, not a clean close"
        );
    }

    /// Without the subprotocol the upgrade is refused with 400 and
    /// nothing is served.
    #[tokio::test]
    async fn an_upgrade_without_the_mqtt_subprotocol_is_refused() {
        let shared = StdArc::new(Shared::new(BrokerConfig::default()));
        let (client_half, server_half) = tokio::io::duplex(16 * 1024);
        let upgrading =
            tokio::spawn(
                async move { upgrade(server_half, Duration::from_secs(5)).await.map(drop) },
            );
        drop(shared);
        let request = "ws://localhost/".into_client_request().expect("a request");
        let refused = tokio_tungstenite::client_async(request, client_half)
            .await
            .expect_err("the server refuses the upgrade");
        match refused {
            tokio_tungstenite::tungstenite::Error::Http(response) => {
                assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            }
            other => panic!("expected an HTTP refusal, got {other:?}"),
        }
        let outcome = upgrading.await.expect("the task finishes");
        assert!(
            matches!(outcome, Err(Refused::NoMqttSubprotocol)),
            "{outcome:?}"
        );
    }
}
