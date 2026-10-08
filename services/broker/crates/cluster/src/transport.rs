//! What a link is opened over, so that a mesh is not a socket.
//!
//! Section 14 committed to three nodes in one process over an
//! in-memory link, and the plan's harnesses put a gate in front of a
//! real one. Neither is possible if [`crate::mesh`] names
//! [`tokio::net::TcpStream`], so it names this instead: something that
//! can be dialled by address and can be accepted from.
//!
//! # Why an address is a string
//!
//! A [`std::net::SocketAddr`] is what TCP dials and is not what the
//! in-memory switch dials, which addresses a node by name. Making the
//! address a string is what lets one mesh run over both, and it costs
//! nothing at the one place it is parsed: [`TcpTransport::dial`] says
//! which string it could not read.
//!
//! # Why the futures are boxed
//!
//! An `async fn` in a trait is not dyn-compatible, and a mesh whose
//! transport was a type parameter would put that parameter in every
//! type above it — including the handle the binary hands to the admin
//! API and to the metrics. One allocation per dial and per accept is
//! not a cost worth that.

use std::future::Future;
use std::io;
use std::pin::Pin;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};

/// A byte stream a link runs over.
///
/// The blanket implementation is the point: nothing has to opt in, so
/// a duplex pipe, a socket and anything wrapped around either are all
/// already one of these.
pub trait Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static {}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Stream for S {}

/// One of those, owned.
pub type BoxStream = Box<dyn Stream>;

/// What a transport's two operations return.
pub type Pending<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Somewhere links come from.
///
/// Two operations and an address. Deliberately not a listener and a
/// connector as separate types: a node binds once and dials many
/// times, and the pair is what a node has.
pub trait Transport: Send + Sync + 'static {
    /// Open a connection to `address`.
    fn dial<'a>(&'a self, address: &'a str) -> Pending<'a, io::Result<BoxStream>>;

    /// Wait for a connection somebody else opened.
    fn accept(&self) -> Pending<'_, io::Result<BoxStream>>;

    /// The address this transport is reachable at.
    ///
    /// What was actually bound rather than what was asked for, so that
    /// a test binding port zero can say where it ended up.
    fn address(&self) -> String;
}

/// The transport a node in a cluster actually uses.
#[derive(Debug)]
pub struct TcpTransport {
    listener: TcpListener,
    address: String,
}

impl TcpTransport {
    /// Bind `address` and answer on it.
    ///
    /// # Errors
    ///
    /// [`std::io::Error`] if the address cannot be bound, which is a
    /// startup failure rather than something to retry: a node whose
    /// cluster port is taken cannot be reached by its peers.
    pub async fn bind(address: &str) -> io::Result<Self> {
        let listener = TcpListener::bind(address).await?;
        let bound = listener.local_addr()?;
        Ok(Self {
            listener,
            address: bound.to_string(),
        })
    }
}

impl Transport for TcpTransport {
    fn dial<'a>(&'a self, address: &'a str) -> Pending<'a, io::Result<BoxStream>> {
        Box::pin(async move {
            let stream = TcpStream::connect(address).await?;
            // Section 8.1's heartbeat is one small frame a second and
            // the link's own batching has already decided when to
            // write, so waiting for company in the kernel as well
            // would add up to two hundred milliseconds to a beat for
            // nothing.
            stream.set_nodelay(true)?;
            Ok(Box::new(stream) as BoxStream)
        })
    }

    fn accept(&self) -> Pending<'_, io::Result<BoxStream>> {
        Box::pin(async move {
            let (stream, _) = self.listener.accept().await?;
            stream.set_nodelay(true)?;
            Ok(Box::new(stream) as BoxStream)
        })
    }

    fn address(&self) -> String {
        self.address.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A socket dialled by address carries bytes both ways.
    ///
    /// The whole of what the mesh asks of a transport, over the one
    /// implementation that is not a harness.
    #[tokio::test]
    async fn a_tcp_transport_dials_what_it_bound() {
        let listening = TcpTransport::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        let dialling = TcpTransport::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        let address = listening.address();
        assert!(address.starts_with("127.0.0.1:"), "{address}");

        let accepted = tokio::spawn(async move {
            let mut stream = listening.accept().await.expect("a connection arrives");
            let mut said = [0_u8; 5];
            stream.read_exact(&mut said).await.expect("it said hello");
            stream.write_all(b"world").await.expect("and is answered");
            stream.flush().await.expect("flushed");
            said
        });

        let mut stream = dialling.dial(&address).await.expect("the dial succeeds");
        stream.write_all(b"hello").await.expect("written");
        stream.flush().await.expect("flushed");
        let mut heard = [0_u8; 5];
        stream.read_exact(&mut heard).await.expect("answered");

        assert_eq!(&accepted.await.expect("the listener finished"), b"hello");
        assert_eq!(&heard, b"world");
    }

    /// An address nothing is listening on is an error, not a hang.
    #[tokio::test]
    async fn dialling_nothing_fails() {
        // Bound and dropped, so the port is one nothing answers on.
        let address = {
            let transport = TcpTransport::bind("127.0.0.1:0")
                .await
                .expect("a loopback port");
            transport.address()
        };
        let dialling = TcpTransport::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        assert!(dialling.dial(&address).await.is_err());
    }
}
