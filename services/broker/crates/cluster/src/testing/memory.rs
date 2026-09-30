//! Section 14's in-memory link: a whole cluster in one process.
//!
//! Nodes are named rather than addressed, dials go through a
//! [`Switch`] instead of a kernel, and the pipe between two of them is
//! a pair of [`tokio::io::duplex`] ends with a [`Valve`] on each
//! direction. No port is bound, so the matrix can be run wide and in
//! parallel, and nothing in it depends on the machine it runs on.
//!
//! # The switch is what makes a partition possible
//!
//! Two duplex ends handed straight to two nodes cannot be severed:
//! there is nothing between them to sever at. So every link goes
//! through a relay the switch owns, and the valve on each direction is
//! keyed by the **ordered** pair of names. `cut("a", "b")` stops what
//! `a` says reaching `b` and leaves what `b` says reaching `a` alone,
//! which is the asymmetry the interesting failures live in.
//!
//! # What it cannot do, and what [`super::gate`] is for
//!
//! It is not a socket. It cannot be dialled by another process, it has
//! no kernel buffers, and a broker binary cannot be pointed at it. The
//! cases that need those are the gate's.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use tokio::io::duplex;
use tokio::sync::{mpsc, Mutex};

use super::{relay, Flow, Valve};
use crate::transport::{BoxStream, Pending, Transport};

/// How much a pipe holds before the writer waits.
///
/// Generous, so that a node writing a batch of frames does not block
/// on a peer that has not read yet — which would turn an ordinary
/// exchange into the stall this harness wants to be able to ask for
/// deliberately.
const PIPE_BYTES: usize = 64 * 1024;

/// Where in-process nodes find each other.
///
/// Shared, because a dial has to reach a listener somebody else
/// registered.
#[derive(Debug, Default)]
pub struct Switch {
    listeners: std::sync::Mutex<HashMap<String, mpsc::UnboundedSender<BoxStream>>>,
    valves: std::sync::Mutex<HashMap<(String, String), Arc<Valve>>>,
}

impl Switch {
    /// A switch nobody is on yet.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Answer to `name`, and hand back the transport that does.
    ///
    /// # Panics
    ///
    /// If `name` is already listening. Two nodes on one name is a test
    /// that is not testing what it says it is, and the second one
    /// would silently never be dialled.
    pub fn listen(self: &Arc<Self>, name: &str) -> MemoryTransport {
        let (sender, receiver) = mpsc::unbounded_channel();
        let previous = self
            .listeners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name.to_owned(), sender);
        assert!(previous.is_none(), "two nodes are listening as {name:?}");
        MemoryTransport {
            switch: Arc::clone(self),
            name: name.to_owned(),
            incoming: Mutex::new(receiver),
        }
    }

    /// Stop answering to `name`.
    ///
    /// What lets a test stop a node and start it again under the same
    /// name, which is how a rejoin is written. Anything still holding
    /// the old transport simply stops being dialled.
    pub fn forget(&self, name: &str) {
        self.listeners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(name);
    }

    /// The valve on what `from` says to `to`, made if it is new.
    ///
    /// On demand and by name, so that a test can sever a pair before
    /// either of them has connected — which is what lets a node come
    /// up already unable to hear another.
    #[must_use]
    pub fn valve(&self, from: &str, to: &str) -> Arc<Valve> {
        let key = (from.to_owned(), to.to_owned());
        Arc::clone(
            self.valves
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(key)
                .or_insert_with(|| Arc::new(Valve::new())),
        )
    }

    /// Stop what `from` says reaching `to`, and nothing else.
    pub fn cut(&self, from: &str, to: &str) {
        self.valve(from, to).cut();
    }

    /// Hold it instead, so `from` pushes back and nothing is lost.
    pub fn stall(&self, from: &str, to: &str) {
        self.valve(from, to).set(Flow::Stall);
    }

    /// Let it through again.
    pub fn heal(&self, from: &str, to: &str) {
        self.valve(from, to).heal();
    }

    /// Cut both directions between two nodes.
    ///
    /// Named apart from [`Switch::cut`] because it is a different
    /// experiment: both ends notice at once and agree, which is the
    /// easy half of section 8.5.
    pub fn partition(&self, one: &str, other: &str) {
        self.cut(one, other);
        self.cut(other, one);
    }

    /// Heal both directions between two nodes.
    pub fn rejoin(&self, one: &str, other: &str) {
        self.heal(one, other);
        self.heal(other, one);
    }
}

/// One node's end of the switch.
#[derive(Debug)]
pub struct MemoryTransport {
    switch: Arc<Switch>,
    name: String,
    incoming: Mutex<mpsc::UnboundedReceiver<BoxStream>>,
}

impl Transport for MemoryTransport {
    fn dial<'a>(&'a self, address: &'a str) -> Pending<'a, io::Result<BoxStream>> {
        Box::pin(async move {
            let listener = self
                .switch
                .listeners
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(address)
                .cloned();
            let Some(listener) = listener else {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("no node on this switch is listening as {address:?}"),
                ));
            };

            // Four ends and two relays, rather than one pipe: a pipe
            // handed straight to both nodes has nothing in the middle
            // to sever.
            let (near, near_relay) = duplex(PIPE_BYTES);
            let (far_relay, far) = duplex(PIPE_BYTES);
            let (near_read, near_write) = tokio::io::split(near_relay);
            let (far_read, far_write) = tokio::io::split(far_relay);

            let outward = self.switch.valve(&self.name, address);
            let homeward = self.switch.valve(address, &self.name);
            tokio::spawn(relay(near_read, far_write, outward));
            tokio::spawn(relay(far_read, near_write, homeward));

            listener.send(Box::new(far) as BoxStream).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("the node listening as {address:?} has gone"),
                )
            })?;
            Ok(Box::new(near) as BoxStream)
        })
    }

    fn accept(&self) -> Pending<'_, io::Result<BoxStream>> {
        Box::pin(async move {
            let mut incoming = self.incoming.lock().await;
            incoming.recv().await.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "the switch this node listened on has gone",
                )
            })
        })
    }

    fn address(&self) -> String {
        self.name.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::task::JoinHandle;

    use super::super::Direction;

    /// How long to wait for something that should happen.
    const PATIENCE: Duration = Duration::from_secs(5);

    /// How long to wait to be sure something has not.
    const SILENCE: Duration = Duration::from_millis(300);

    /// A node that answers every byte with its own name.
    ///
    /// The same shape the gate's tests use, so that the two harnesses
    /// are asked the same questions.
    struct Node {
        heard: Arc<AtomicU64>,
        serving: JoinHandle<()>,
    }

    impl Drop for Node {
        fn drop(&mut self) {
            self.serving.abort();
        }
    }

    impl Node {
        fn start(switch: &Arc<Switch>, at: &str, name: u8) -> Self {
            let transport = switch.listen(at);
            let heard = Arc::new(AtomicU64::new(0));
            let counted = Arc::clone(&heard);
            let serving = tokio::spawn(async move {
                while let Ok(mut stream) = transport.accept().await {
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
                            if stream.write_all(&vec![name; read]).await.is_err() {
                                return;
                            }
                            let _ = stream.flush().await;
                        }
                    });
                }
            });
            Self { heard, serving }
        }

        fn heard(&self) -> u64 {
            self.heard.load(Ordering::Relaxed)
        }
    }

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

    async fn exchange(stream: &mut BoxStream) -> u8 {
        stream.write_all(b"?").await.expect("the write succeeds");
        stream.flush().await.expect("the flush succeeds");
        let mut heard = [0_u8; 1];
        tokio::time::timeout(PATIENCE, stream.read_exact(&mut heard))
            .await
            .expect("an answer arrives")
            .expect("and it is not an error");
        heard[0]
    }

    async fn ask_into_silence(stream: &mut BoxStream) {
        stream.write_all(b"?").await.expect("the write succeeds");
        stream.flush().await.expect("the flush succeeds");
        let mut heard = [0_u8; 1];
        let outcome = tokio::time::timeout(SILENCE, stream.read_exact(&mut heard)).await;
        assert!(outcome.is_err(), "the cut direction delivered something");
    }

    /// A dial reaches the node listening under that name, and nothing
    /// else does.
    #[tokio::test]
    async fn a_dial_reaches_the_node_of_that_name() {
        let switch = Switch::new();
        let _b = Node::start(&switch, "b", b'B');
        let a = switch.listen("a");

        let mut link = a.dial("b").await.expect("b is listening");
        assert_eq!(exchange(&mut link).await, b'B');

        let refused = match a.dial("nobody").await {
            Ok(_) => panic!("nobody is listening as \"nobody\""),
            Err(why) => why,
        };
        assert_eq!(refused.kind(), io::ErrorKind::ConnectionRefused);
    }

    /// One direction stops while the other flows.
    #[tokio::test]
    async fn a_cut_one_way_leaves_the_other_way_flowing() {
        let switch = Switch::new();
        let b = Node::start(&switch, "b", b'B');
        let a = switch.listen("a");
        let mut link = a.dial("b").await.expect("b is listening");
        assert_eq!(exchange(&mut link).await, b'B');
        let heard_before = b.heard();

        switch.cut("b", "a");
        ask_into_silence(&mut link).await;
        until("b's answer is swallowed", || {
            switch.valve("b", "a").swallowed() > 0
        })
        .await;

        assert!(
            b.heard() > heard_before,
            "what a says must still reach b: b heard {} and had heard {heard_before}",
            b.heard()
        );
        assert_eq!(
            switch.valve("a", "b").swallowed(),
            0,
            "cutting one direction must not cut the other"
        );

        switch.heal("b", "a");
        assert_eq!(
            exchange(&mut link).await,
            b'B',
            "the very same link must carry again once the cut is healed"
        );
    }

    /// And the other way about, so that neither valve is secretly
    /// both.
    #[tokio::test]
    async fn a_cut_the_other_way_stops_the_questions_instead() {
        let switch = Switch::new();
        let b = Node::start(&switch, "b", b'B');
        let a = switch.listen("a");
        let mut link = a.dial("b").await.expect("b is listening");
        assert_eq!(exchange(&mut link).await, b'B');
        let heard_before = b.heard();

        switch.cut("a", "b");
        ask_into_silence(&mut link).await;
        until("a's question is swallowed", || {
            switch.valve("a", "b").swallowed() > 0
        })
        .await;

        assert_eq!(
            b.heard(),
            heard_before,
            "nothing a said after the cut may reach b"
        );
        assert_eq!(switch.valve("b", "a").swallowed(), 0);
    }

    /// A full partition: neither hears the other, and the link is
    /// still open.
    #[tokio::test]
    async fn a_full_partition_stops_both_directions() {
        let switch = Switch::new();
        let b = Node::start(&switch, "b", b'B');
        let a = switch.listen("a");
        let mut link = a.dial("b").await.expect("b is listening");
        assert_eq!(exchange(&mut link).await, b'B');
        let heard_before = b.heard();

        switch.partition("a", "b");
        ask_into_silence(&mut link).await;
        until("a's question is swallowed", || {
            switch.valve("a", "b").swallowed() > 0
        })
        .await;
        assert_eq!(b.heard(), heard_before);

        switch.rejoin("a", "b");
        assert_eq!(exchange(&mut link).await, b'B');
    }

    /// A stall holds what it is given and delivers all of it on heal.
    #[tokio::test]
    async fn a_stall_delivers_what_it_held() {
        let switch = Switch::new();
        let b = Node::start(&switch, "b", b'B');
        let a = switch.listen("a");
        let mut link = a.dial("b").await.expect("b is listening");
        assert_eq!(exchange(&mut link).await, b'B');

        switch.stall("b", "a");
        link.write_all(b"????").await.expect("written");
        link.flush().await.expect("flushed");
        until("b has answered all four", || b.heard() >= 5).await;

        let mut heard = [0_u8; 4];
        assert!(
            tokio::time::timeout(SILENCE, link.read_exact(&mut heard))
                .await
                .is_err(),
            "a stall must not deliver while it is on"
        );
        assert_eq!(
            switch.valve("b", "a").swallowed(),
            0,
            "a stall discards nothing; that is what tells it from a swallow"
        );

        switch.heal("b", "a");
        tokio::time::timeout(PATIENCE, link.read_exact(&mut heard))
            .await
            .expect("the held answers arrive")
            .expect("and are not an error");
        assert_eq!(&heard, b"BBBB");
    }

    /// A stall pushes back on the sender.
    ///
    /// This is what tells a stall from an unbounded buffer, and it is
    /// the half the plan asks for in as many words: **a mode that
    /// stalls the sender rather than discarding**. A relay that went
    /// on reading while stalled would hold everything in memory, the
    /// writer would never wait, and the "overloaded peer" this mode
    /// exists to be would be a peer with infinite patience.
    ///
    /// The sink deliberately never answers. A node that echoed would
    /// fill the return pipe, stop reading, and deadlock the test
    /// against its own backpressure rather than against the stall's.
    #[tokio::test]
    async fn a_stall_pushes_back_on_the_sender() {
        let switch = Switch::new();
        let sink = switch.listen("sink");
        let swallowed = Arc::new(AtomicU64::new(0));
        let counted = Arc::clone(&swallowed);
        let serving = tokio::spawn(async move {
            while let Ok(mut stream) = sink.accept().await {
                let counted = Arc::clone(&counted);
                tokio::spawn(async move {
                    let mut buffer = vec![0_u8; 8192];
                    while let Ok(read) = stream.read(&mut buffer).await {
                        if read == 0 {
                            return;
                        }
                        counted.fetch_add(read as u64, Ordering::Relaxed);
                    }
                });
            }
        });

        let a = switch.listen("a");
        let mut link = a.dial("sink").await.expect("the sink is listening");
        switch.stall("a", "sink");

        // Four times the pipe, so that no buffer between here and the
        // relay can swallow it.
        let load = vec![7_u8; PIPE_BYTES * 4];
        let wedged = tokio::time::timeout(SILENCE, link.write_all(&load)).await;
        assert!(
            wedged.is_err(),
            "a stalled direction must stop the sender rather than buffer for it"
        );

        switch.heal("a", "sink");
        tokio::time::timeout(PATIENCE, link.write_all(&load))
            .await
            .expect("the write completes once the stall is healed")
            .expect("and is not an error");
        link.flush().await.expect("flushed");
        until("everything written arrives", || {
            swallowed.load(Ordering::Relaxed) >= (PIPE_BYTES * 4) as u64
        })
        .await;
        assert_eq!(
            switch.valve("a", "sink").swallowed(),
            0,
            "a stall discards nothing"
        );
        serving.abort();
    }

    /// A partial partition among three: the third reaches both.
    #[tokio::test]
    async fn a_partial_partition_among_three_leaves_the_third_reaching_both() {
        let switch = Switch::new();
        let _b = Node::start(&switch, "b", b'B');
        let _c = Node::start(&switch, "c", b'C');
        let a = switch.listen("a");
        let b_dialling = switch.listen("b-dialling");

        let mut ab = a.dial("b").await.expect("b is listening");
        let mut ac = a.dial("c").await.expect("c is listening");
        let mut bc = b_dialling.dial("c").await.expect("c is listening");
        assert_eq!(exchange(&mut ab).await, b'B');
        assert_eq!(exchange(&mut ac).await, b'C');
        assert_eq!(exchange(&mut bc).await, b'C');

        switch.cut("b", "a");
        ask_into_silence(&mut ab).await;
        until("a stops hearing b", || {
            switch.valve("b", "a").swallowed() > 0
        })
        .await;

        assert_eq!(exchange(&mut ac).await, b'C', "a must still reach c");
        assert_eq!(exchange(&mut bc).await, b'C', "b must still reach c");
        assert_eq!(
            switch.valve("c", "a").swallowed() + switch.valve("c", "b-dialling").swallowed(),
            0,
            "a cut on one pair must not reach another"
        );

        switch.heal("b", "a");
        assert_eq!(exchange(&mut ab).await, b'B');
    }

    /// The two harnesses use one vocabulary, and this is what says so.
    ///
    /// [`Direction`] is the gate's way of naming the same two valves
    /// this switch names by ordered pair, and a test that never
    /// mentioned it would let the two drift into different meanings.
    #[test]
    fn a_direction_names_the_same_two_ends_the_switch_does() {
        assert_ne!(Direction::Forward, Direction::Backward);
        let valve = Valve::new();
        assert_eq!(valve.flow(), Flow::Open);
        valve.cut();
        assert_eq!(valve.flow(), Flow::Swallow);
        valve.set(Flow::Stall);
        assert_eq!(valve.flow(), Flow::Stall);
        valve.heal();
        assert_eq!(valve.flow(), Flow::Open);
    }
}
