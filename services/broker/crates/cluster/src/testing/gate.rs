//! A fault-injection gate in front of a real socket.
//!
//! One of these per ordered pair of nodes: it binds a port of its own,
//! and whoever dials that port is connected on to the upstream address
//! the gate was opened towards. Two [`Valve`]s decide, independently,
//! whether bytes move each way.
//!
//! # It never closes either socket
//!
//! This is the property the whole harness exists for, and the one a
//! mutation is most likely to take away. A gate that closed the
//! connection when told to cut would be testing the failure both ends
//! see at once and agree about instantly — which the mesh already
//! detects from the socket alone, and which therefore finds nothing.
//!
//! What this does instead is stop the bytes on an established
//! connection, which is a half-open link, which is the case section
//! 8.1's heartbeat is there for. And because the connection is still
//! there, it can be **healed on the same connection**: see
//! [`tests::a_cut_heals_on_the_same_connection`], which asserts the
//! count of connections the gate ever accepted has not moved.
//!
//! # Why the address is a whole listener rather than a flag
//!
//! A node advertises one address, so two peers dialling one node
//! through one address cannot be severed from it independently. A
//! listener per ordered pair gives each pair an address of its own,
//! which is what makes "A cannot hear B" a statement about A and B
//! rather than about B and everybody.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use super::{relay, Direction, Flow, Valve};

/// A listener in front of one upstream address.
///
/// Dropping it stops accepting and releases the port. Connections
/// already open are dropped with it, which is the only thing here that
/// does close a socket and happens only when the test is over.
#[derive(Debug)]
pub struct Gate {
    address: SocketAddr,
    upstream: SocketAddr,
    forward: Arc<Valve>,
    backward: Arc<Valve>,
    connections: Arc<AtomicU64>,
    accepting: JoinHandle<()>,
}

impl Gate {
    /// Bind a port on the loopback and pass what arrives to `upstream`.
    ///
    /// The upstream need not be listening yet: nothing is dialled
    /// until somebody dials the gate, so a gate can be opened before
    /// the process behind it is started, which is what lets a test
    /// write the gate's address into that process's configuration.
    ///
    /// # Errors
    ///
    /// [`std::io::Error`] if no port can be bound.
    pub async fn open(upstream: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let forward = Arc::new(Valve::new());
        let backward = Arc::new(Valve::new());
        let connections = Arc::new(AtomicU64::new(0));

        let accepting = tokio::spawn(accept_loop(
            listener,
            upstream,
            Arc::clone(&forward),
            Arc::clone(&backward),
            Arc::clone(&connections),
        ));

        Ok(Self {
            address,
            upstream,
            forward,
            backward,
            connections,
            accepting,
        })
    }

    /// Where to dial to reach the upstream through this gate.
    #[must_use]
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// What this gate is in front of.
    #[must_use]
    pub fn upstream(&self) -> SocketAddr {
        self.upstream
    }

    /// The valve for one direction.
    #[must_use]
    pub fn valve(&self, direction: Direction) -> &Valve {
        match direction {
            Direction::Forward => &self.forward,
            Direction::Backward => &self.backward,
        }
    }

    /// Stop the bytes one way, leaving the connection established.
    pub fn cut(&self, direction: Direction) {
        self.valve(direction).cut();
    }

    /// Stop reading them one way, so the sender pushes back and
    /// nothing is lost.
    pub fn stall(&self, direction: Direction) {
        self.valve(direction).set(Flow::Stall);
    }

    /// Let them move again.
    pub fn heal(&self, direction: Direction) {
        self.valve(direction).heal();
    }

    /// How many connections this gate has ever accepted.
    ///
    /// What "the same connection" is asserted with. A heal that had
    /// quietly become a reconnection would move this.
    #[must_use]
    pub fn connections(&self) -> u64 {
        self.connections.load(Ordering::Relaxed)
    }
}

impl Drop for Gate {
    fn drop(&mut self) {
        self.accepting.abort();
    }
}

/// Accept, dial on, and relay each way until the test is over.
async fn accept_loop(
    listener: TcpListener,
    upstream: SocketAddr,
    forward: Arc<Valve>,
    backward: Arc<Valve>,
    connections: Arc<AtomicU64>,
) {
    loop {
        let Ok((near, _)) = listener.accept().await else {
            return;
        };
        let Ok(far) = TcpStream::connect(upstream).await else {
            // The upstream is not there. Dropping the near end is the
            // honest answer: a gate in front of nothing is not a
            // partition, it is an address that refuses.
            continue;
        };
        // Nagle off on both, for the reason `TcpTransport` gives: a
        // harness that batched on its own would decide when a beat
        // arrives.
        let _ = near.set_nodelay(true);
        let _ = far.set_nodelay(true);
        connections.fetch_add(1, Ordering::Relaxed);

        let (near_read, near_write) = tokio::io::split(near);
        let (far_read, far_write) = tokio::io::split(far);
        tokio::spawn(relay(near_read, far_write, Arc::clone(&forward)));
        tokio::spawn(relay(far_read, near_write, Arc::clone(&backward)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    /// How long to wait for something that should happen.
    const PATIENCE: Duration = Duration::from_secs(5);

    /// How long to wait to be sure something has not.
    ///
    /// Short, and never the only evidence: every absence asserted here
    /// is paired with a count from the valve that says the bytes were
    /// seen and thrown away, so "nothing arrived" cannot pass because
    /// nothing was sent.
    const SILENCE: Duration = Duration::from_millis(300);

    /// A node that answers every byte with its own name.
    ///
    /// Byte for byte rather than a protocol, so a test can say "one
    /// request, one answer" without a framing of its own — and so the
    /// count of bytes a valve reports is the count of exchanges.
    struct Node {
        address: SocketAddr,
        heard: Arc<AtomicU64>,
        serving: JoinHandle<()>,
    }

    impl Drop for Node {
        fn drop(&mut self) {
            self.serving.abort();
        }
    }

    impl Node {
        async fn start(name: u8) -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("a loopback port");
            let address = listener.local_addr().expect("it has one");
            let heard = Arc::new(AtomicU64::new(0));
            let counted = Arc::clone(&heard);
            let serving = tokio::spawn(async move {
                while let Ok((mut stream, _)) = listener.accept().await {
                    let counted = Arc::clone(&counted);
                    tokio::spawn(async move {
                        let mut buffer = [0_u8; 64];
                        loop {
                            let Ok(read) = stream.read(&mut buffer).await else {
                                return;
                            };
                            if read == 0 {
                                return;
                            }
                            counted.fetch_add(read as u64, Ordering::Relaxed);
                            let answer = vec![name; read];
                            if stream.write_all(&answer).await.is_err() {
                                return;
                            }
                            let _ = stream.flush().await;
                        }
                    });
                }
            });
            Self {
                address,
                heard,
                serving,
            }
        }

        fn heard(&self) -> u64 {
            self.heard.load(Ordering::Relaxed)
        }
    }

    /// Wait until `condition` holds, or fail saying it never did.
    async fn until(what: &str, mut condition: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + PATIENCE;
        while tokio::time::Instant::now() < deadline {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("{what} never happened within {PATIENCE:?}");
    }

    /// Say one byte and hear the answer.
    async fn exchange(stream: &mut TcpStream) -> u8 {
        stream.write_all(b"?").await.expect("the write succeeds");
        stream.flush().await.expect("the flush succeeds");
        let mut heard = [0_u8; 1];
        tokio::time::timeout(PATIENCE, stream.read_exact(&mut heard))
            .await
            .expect("an answer arrives")
            .expect("and it is not an error");
        heard[0]
    }

    /// Say one byte and expect no answer.
    async fn ask_into_silence(stream: &mut TcpStream) {
        stream.write_all(b"?").await.expect("the write succeeds");
        stream.flush().await.expect("the flush succeeds");
        let mut heard = [0_u8; 1];
        let outcome = tokio::time::timeout(SILENCE, stream.read_exact(&mut heard)).await;
        assert!(
            outcome.is_err(),
            "the cut direction delivered {:?}",
            outcome.map(|r| r.map(|_| heard[0]))
        );
    }

    /// One direction stops, the other keeps flowing.
    ///
    /// The asymmetry is the whole point: the upstream still hears
    /// every request, and the dialler hears none of the answers.
    #[tokio::test]
    async fn a_cut_backward_stops_the_answers_and_not_the_questions() {
        let node = Node::start(b'B').await;
        let gate = Gate::open(node.address).await.expect("a gate opens");
        let mut stream = TcpStream::connect(gate.address())
            .await
            .expect("the gate accepts");

        assert_eq!(exchange(&mut stream).await, b'B');
        let heard_before = node.heard();

        gate.cut(Direction::Backward);
        ask_into_silence(&mut stream).await;

        until("the answer is swallowed", || {
            gate.valve(Direction::Backward).swallowed() > 0
        })
        .await;
        assert!(
            node.heard() > heard_before,
            "the forward direction must still carry: the upstream heard {} \
             and had heard {heard_before}",
            node.heard()
        );
        assert_eq!(
            gate.valve(Direction::Forward).swallowed(),
            0,
            "cutting one direction must not cut the other"
        );
    }

    /// And the same the other way, so that neither flag is secretly
    /// both.
    ///
    /// Without this, a gate whose two valves were one value would pass
    /// the test above: cutting the backward direction would cut the
    /// forward one too, and the only assertion that would notice is
    /// the one that says the upstream still heard the question.
    #[tokio::test]
    async fn a_cut_forward_stops_the_questions_and_not_the_answers() {
        let node = Node::start(b'B').await;
        let gate = Gate::open(node.address).await.expect("a gate opens");
        let mut stream = TcpStream::connect(gate.address())
            .await
            .expect("the gate accepts");

        assert_eq!(exchange(&mut stream).await, b'B');
        let heard_before = node.heard();

        gate.cut(Direction::Forward);
        ask_into_silence(&mut stream).await;

        until("the question is swallowed", || {
            gate.valve(Direction::Forward).swallowed() > 0
        })
        .await;
        assert_eq!(
            node.heard(),
            heard_before,
            "the upstream must not have heard anything after the cut"
        );
        assert_eq!(
            gate.valve(Direction::Backward).swallowed(),
            0,
            "cutting one direction must not cut the other"
        );
    }

    /// Both directions cut is a full partition, and still not a close.
    #[tokio::test]
    async fn both_directions_cut_is_a_full_partition() {
        let node = Node::start(b'B').await;
        let gate = Gate::open(node.address).await.expect("a gate opens");
        let mut stream = TcpStream::connect(gate.address())
            .await
            .expect("the gate accepts");
        assert_eq!(exchange(&mut stream).await, b'B');
        let heard_before = node.heard();

        gate.cut(Direction::Forward);
        gate.cut(Direction::Backward);
        ask_into_silence(&mut stream).await;

        until("the question is swallowed", || {
            gate.valve(Direction::Forward).swallowed() > 0
        })
        .await;
        assert_eq!(node.heard(), heard_before);
        assert_eq!(
            gate.connections(),
            1,
            "a full partition is still not a closed socket"
        );
    }

    /// The property the harness exists for: a cut heals **on the same
    /// connection**.
    ///
    /// The connection count is what says so. A gate that closed the
    /// socket instead of swallowing would fail at the exchange after
    /// the heal — or, if the client reconnected, would leave this at
    /// two.
    #[tokio::test]
    async fn a_cut_heals_on_the_same_connection() {
        let node = Node::start(b'B').await;
        let gate = Gate::open(node.address).await.expect("a gate opens");
        let mut stream = TcpStream::connect(gate.address())
            .await
            .expect("the gate accepts");

        assert_eq!(exchange(&mut stream).await, b'B');
        gate.cut(Direction::Backward);
        ask_into_silence(&mut stream).await;
        until("the answer is swallowed", || {
            gate.valve(Direction::Backward).swallowed() > 0
        })
        .await;

        gate.heal(Direction::Backward);
        assert_eq!(
            exchange(&mut stream).await,
            b'B',
            "the very same connection must carry again once the cut is healed"
        );
        assert_eq!(
            gate.connections(),
            1,
            "nothing reconnected: the heal is on the connection the cut was made on"
        );
    }

    /// A stall holds the bytes rather than discarding them, and
    /// delivers every one of them when it reopens.
    ///
    /// This is the overloaded peer rather than the black hole, and the
    /// difference is observable: nothing is counted as swallowed, and
    /// the answers that were held all arrive.
    #[tokio::test]
    async fn a_stall_delivers_what_it_held() {
        let node = Node::start(b'B').await;
        let gate = Gate::open(node.address).await.expect("a gate opens");
        let mut stream = TcpStream::connect(gate.address())
            .await
            .expect("the gate accepts");
        assert_eq!(exchange(&mut stream).await, b'B');

        gate.stall(Direction::Backward);
        for _ in 0..4 {
            stream.write_all(b"?").await.expect("written");
        }
        stream.flush().await.expect("flushed");
        until("the upstream has answered all four", || node.heard() >= 5).await;

        let mut heard = [0_u8; 4];
        let held = tokio::time::timeout(SILENCE, stream.read_exact(&mut heard)).await;
        assert!(held.is_err(), "a stall must not deliver while it is on");
        assert_eq!(
            gate.valve(Direction::Backward).swallowed(),
            0,
            "a stall discards nothing; that is what tells it from a swallow"
        );

        gate.heal(Direction::Backward);
        tokio::time::timeout(PATIENCE, stream.read_exact(&mut heard))
            .await
            .expect("the held answers arrive")
            .expect("and are not an error");
        assert_eq!(&heard, b"BBBB");
        assert_eq!(gate.connections(), 1);
    }

    /// A partial partition among three: the third reaches both.
    ///
    /// The shape every test after this one depends on. One pair is
    /// severed one way and the other two pairs are untouched, so the
    /// node in the middle is the only one that still sees the whole
    /// cluster.
    #[tokio::test]
    async fn a_partial_partition_among_three_leaves_the_third_reaching_both() {
        let b = Node::start(b'B').await;
        let c = Node::start(b'C').await;

        // A reaches B and C; B reaches C. Three ordered pairs, three
        // gates, because a flag on B's own port would sever B from
        // everybody rather than from A.
        let a_to_b = Gate::open(b.address).await.expect("a gate opens");
        let a_to_c = Gate::open(c.address).await.expect("a gate opens");
        let b_to_c = Gate::open(c.address).await.expect("a gate opens");

        let mut ab = TcpStream::connect(a_to_b.address()).await.expect("dialled");
        let mut ac = TcpStream::connect(a_to_c.address()).await.expect("dialled");
        let mut bc = TcpStream::connect(b_to_c.address()).await.expect("dialled");
        assert_eq!(exchange(&mut ab).await, b'B');
        assert_eq!(exchange(&mut ac).await, b'C');
        assert_eq!(exchange(&mut bc).await, b'C');

        a_to_b.cut(Direction::Backward);
        ask_into_silence(&mut ab).await;
        until("A stops hearing B", || {
            a_to_b.valve(Direction::Backward).swallowed() > 0
        })
        .await;

        assert_eq!(exchange(&mut ac).await, b'C', "A must still reach C");
        assert_eq!(exchange(&mut bc).await, b'C', "B must still reach C");
        assert_eq!(
            a_to_c.valve(Direction::Backward).swallowed()
                + b_to_c.valve(Direction::Backward).swallowed(),
            0,
            "a cut on one pair's gate must not reach another pair's"
        );

        a_to_b.heal(Direction::Backward);
        assert_eq!(exchange(&mut ab).await, b'B');
        assert_eq!(a_to_b.connections(), 1);
    }
}
