//! The socket section 9.2's ingest routes are served on.
//!
//! # What was wrong with binding it at startup
//!
//! It used to be bound once, during startup, and only if this node
//! held an ingest rule at that moment. A node that started with none
//! got no socket, which was right for that instant and wrong ever
//! after: section 11.1's admin API creates rules, so the first ingest
//! rule written over the API — or made on the Rules page — was stored,
//! installed, counted, listed and answered `200`, and posts to it were
//! refused by a socket nobody was listening on until the broker was
//! restarted.
//!
//! That is worse than refusing to create it. Everything an operator
//! can see says the rule is live, so the only way to find out
//! otherwise is for whoever is posting to it to notice that nothing
//! arrives.
//!
//! # Lazily, and closed again
//!
//! So the socket follows the rules rather than the other way round:
//! it is bound the first time this node has an ingest route to serve
//! and closed when the last one goes away. [`IngestSocket`] is what
//! holds it, and [`rules::live::Registry`] is what asks — before it
//! answers whoever asked for the rule, which is the whole point.
//!
//! The alternative was to bind it unconditionally at startup, which is
//! one deleted condition and no new states at all. What it costs is a
//! port: [`crate::config::DEFAULT_INGEST_PORT`] is a fixed number, so
//! every broker on a host would contend for it whether or not it does
//! any ingest, and the second of two brokers on one machine — which
//! this project runs in its own bridge test — would fail to start
//! against an address it was never going to use. A broker that does no
//! ingest binds nothing here.
//!
//! # What it costs instead, and what is done about each part
//!
//! A listener that comes and goes is a thing to get wrong, and these
//! are the three ways:
//!
//! - **Binding can fail after startup.** So it is asked for *before*
//!   the rule is stored, and a failure is the API refusing to create
//!   the rule rather than creating one that cannot be reached. See
//!   [`rules::live::Registry::install`].
//! - **The inventory can go stale.** `/api/v1/listeners` is built from
//!   a shared list, and this adds its row when it binds and takes it
//!   away when it closes. A list fixed at startup would be a second
//!   thing telling the same lie.
//! - **It has to stop on a signal.** This project has already been
//!   bitten by a listener whose body did not end when it was told to,
//!   which stopped the broker exiting. [`IngestSocket::close`] is
//!   called from the same place in the shutdown sequence the old
//!   listener's was, is bounded by the same grace period, and leaves
//!   this in a state that binds nothing more — so a rule created in
//!   the moment between the signal and the end is refused rather than
//!   answered with a socket nothing will ever close.

use std::net::SocketAddr;
use std::sync::Arc;

use rules::live::IngestListening;
use rules::Ingest;
use sdmqtt_admin::{AdminStartError, Bound, ListenerProtocol, ListenerView, Listeners, Serving};
use sdmqtt_server::shared::Shared;
use tokio::sync::Mutex;

/// What this listener is called, in the log lines and in the row
/// `/api/v1/listeners` reports.
const NAME: &str = "ingest";

/// What the socket is doing.
enum State {
    /// Nothing is bound, because nothing needs it.
    Idle,
    /// Bound, and serving the ingest routes.
    ///
    /// Boxed because [`Serving`] is much the largest of these and a
    /// variant nothing else can be as big as makes every `State` that
    /// size.
    Serving(Box<Serving>),
    /// This node is stopping, and this binds nothing more.
    Stopped,
}

/// What this call has to do to make the world match what was asked
/// for.
///
/// Decided while the state is borrowed and acted on afterwards, so
/// that the decision is a small readable table rather than something
/// spread through the arms of a match that is also doing the work.
enum Step {
    /// It is already that way.
    Nothing,
    /// Bind and serve.
    Bind,
    /// Stop serving and release the port.
    Close,
    /// It is wanted and this node is stopping.
    Refuse,
}

/// The ingest listener, bound while this node has a route for it.
pub struct IngestSocket {
    /// Where it binds. The configured address, whose port may be zero
    /// and be the operating system's to choose — which is why the row
    /// below is written from what was bound rather than from this.
    address: SocketAddr,
    /// The broker a post publishes into.
    shared: Arc<Shared>,
    /// The routes, which are also the answer to whether anything
    /// needs this socket at all.
    ingest: Arc<Ingest>,
    /// What `/api/v1/listeners` reports, which this keeps its own row
    /// in.
    listeners: Listeners,
    /// What it is doing, and the lock that makes one thing happen at
    /// a time.
    ///
    /// Asynchronous, because binding and shutting down both await and
    /// the whole point of the lock is to hold it across them: two
    /// installs racing must not both bind, and a delete must not
    /// close a socket in the middle of an install that needs it.
    state: Mutex<State>,
}

impl IngestSocket {
    /// A socket for `address`, bound to nothing yet.
    pub fn new(
        address: SocketAddr,
        shared: Arc<Shared>,
        ingest: Arc<Ingest>,
        listeners: Listeners,
    ) -> Self {
        Self {
            address,
            shared,
            ingest,
            listeners,
            state: Mutex::new(State::Idle),
        }
    }

    /// Bind it now if this node already holds an ingest route.
    ///
    /// What startup calls. A node whose configuration file names an
    /// ingest rule has one by the time this runs, so the address
    /// being unavailable is a **startup failure** exactly as it was
    /// before — an operator who wrote an ingest rule and a port
    /// something else is on is told at once rather than when the
    /// first post arrives.
    ///
    /// # Errors
    ///
    /// [`AdminStartError::Bind`], naming this listener and its
    /// address.
    pub async fn bind_if_wanted(&self) -> Result<(), AdminStartError> {
        if self.ingest.is_empty() {
            return Ok(());
        }
        let mut state = self.state.lock().await;
        if matches!(*state, State::Idle) {
            *state = State::Serving(Box::new(self.bind().await?));
        }
        Ok(())
    }

    /// Stop serving, for good, and say whether anything was.
    ///
    /// The graceful half of shutdown, called where the old
    /// unconditional listener's `shutdown` was: after the admin
    /// listener, so nothing may create a rule that would want this
    /// back, and before the MQTT listener, so a post already in
    /// flight still reaches the subscribers that are attached.
    ///
    /// Bounded by [`sdmqtt_admin::SHUTDOWN_GRACE`], which is
    /// [`Serving::shutdown`]'s own backstop: nothing served here
    /// holds a connection open past its response, so reaching it
    /// would be a defect and says so.
    pub async fn close(&self) -> bool {
        let mut state = self.state.lock().await;
        // Stopped before the wait rather than after, so that anything
        // asking for it back during the wait is refused rather than
        // given a socket this call has already decided to close.
        match std::mem::replace(&mut *state, State::Stopped) {
            State::Serving(serving) => {
                self.forget_the_row();
                serving.shutdown().await;
                true
            }
            State::Idle | State::Stopped => false,
        }
    }

    /// Bind the address and start serving the ingest routes on it.
    async fn bind(&self) -> Result<Serving, AdminStartError> {
        let bound = Bound::at(NAME, self.address).await?;
        let address = bound.address();
        let routes =
            rules::http_in::routes::<()>(Arc::clone(&self.shared), Arc::clone(&self.ingest))
                // Section 13's one shape, for a path this listener does not
                // serve. Registered rather than left to axum's default, which
                // is an empty body a client cannot tell from a proxy that
                // swallowed the request.
                .fallback(rules::http_in::not_found)
                .method_not_allowed_fallback(rules::http_in::method_not_allowed);
        // Nothing served here holds a connection open past its
        // response, so there is nothing to tell before hyper starts
        // waiting.
        let serving = bound.serve(routes, || {});
        // From what was bound, not from what was configured: a
        // configured port of zero is the operating system's to
        // choose, and the row is what an operator reads to find out
        // which one it chose.
        self.listeners.write().push(ListenerView {
            name: NAME.to_owned(),
            protocol: ListenerProtocol::Ingest,
            address,
            // An ingest listener has no topic namespace and no
            // connection limit of its own.
            mountpoint: String::new(),
            max_connections: None,
            max_qos: None,
            retain_available: None,
            durability: None,
        });
        tracing::info!(
            %address,
            rules = self.ingest.len(),
            "the ingest listener is serving, with every route behind its own rule's token"
        );
        Ok(serving)
    }

    /// Take this listener out of what the API reports.
    ///
    /// By name, which is the key a row is identified by, rather than
    /// by address: a node serves one of these and the name is what
    /// stays the same across a rebind onto a different chosen port.
    fn forget_the_row(&self) {
        self.listeners.write().retain(|view| view.name != NAME);
    }
}

impl IngestListening for IngestSocket {
    fn reconcile(
        &self,
        wanted: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            let step = match (&*state, wanted) {
                (State::Stopped, true) => Step::Refuse,
                (State::Stopped, false) => Step::Nothing,
                (State::Serving(_), true) | (State::Idle, false) => Step::Nothing,
                (State::Idle, true) => Step::Bind,
                (State::Serving(_), false) => Step::Close,
            };
            match step {
                Step::Nothing => Ok(()),
                Step::Refuse => Err("this node is stopping and binds nothing more".to_owned()),
                Step::Bind => {
                    let serving = self.bind().await.map_err(|error| error.to_string())?;
                    *state = State::Serving(Box::new(serving));
                    Ok(())
                }
                Step::Close => {
                    let State::Serving(serving) = std::mem::replace(&mut *state, State::Idle)
                    else {
                        unreachable!("the step was decided from this same state");
                    };
                    self.forget_the_row();
                    serving.shutdown().await;
                    tracing::info!(
                        "the last ingest rule with a route is gone, so the ingest listener \
                         has stopped and its port is free"
                    );
                    Ok(())
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    /// A socket for a loopback address nothing has taken, over a
    /// broker with no store.
    fn a_socket() -> IngestSocket {
        IngestSocket::new(
            "127.0.0.1:0".parse().expect("an address"),
            Arc::new(Shared::new(Config::default().server().broker)),
            Arc::new(Ingest::new()),
            sdmqtt_admin::listener_list(Vec::new()),
        )
    }

    /// Once this node is stopping, nothing gets a listener back.
    ///
    /// **A unit test of a state machine, and it says so.** What
    /// happens on a signal is covered where it happens, against the
    /// real binary, in `tests/rules.rs`; what cannot be reached from
    /// there is the window *after* the signal, because the admin
    /// listener is shut down first and so there is no request left
    /// that could ask for a rule. That ordering is the only reason
    /// this arm is unreachable, and a later change to it would make
    /// the difference between a refusal and a socket nothing will
    /// ever close.
    ///
    /// The answer is a refusal rather than a silent `Ok`, which is
    /// the whole subject of this module: whoever is told a rule can
    /// be reached must be told the truth.
    #[tokio::test]
    async fn a_stopping_node_refuses_to_bind_rather_than_pretending_it_did() {
        let socket = a_socket();
        assert!(
            !socket.close().await,
            "nothing was bound, so nothing was closed"
        );
        let asked = socket.reconcile(true).await;
        assert!(
            asked.is_err(),
            "a stopping node answered as though it had bound a listener"
        );
        assert!(
            socket.reconcile(false).await.is_ok(),
            "and it is not asked to close what it has already closed"
        );
    }

    /// Binding, closing and binding again leave the inventory saying
    /// what is actually there.
    ///
    /// The row is what `/api/v1/listeners` reports, and a listener
    /// that comes and goes is exactly the one an inventory can be
    /// wrong about: a row left behind is a port an operator is told
    /// to expect and will not find.
    #[tokio::test]
    async fn the_reported_row_appears_and_disappears_with_the_socket() {
        let socket = a_socket();
        let listeners = Arc::clone(&socket.listeners);

        socket.reconcile(true).await.expect("a free loopback port");
        assert_eq!(listeners.read().len(), 1, "it bound and said nothing");
        let bound = listeners.read()[0].address;
        assert_ne!(
            bound.port(),
            0,
            "the row must carry the port the operating system chose, not the zero asked for"
        );

        socket.reconcile(false).await.expect("closing cannot fail");
        assert!(
            listeners.read().is_empty(),
            "it closed the socket and still reports it"
        );

        socket.reconcile(true).await.expect("it binds again");
        assert_eq!(listeners.read().len(), 1, "and says so again");

        socket.close().await;
    }
}
