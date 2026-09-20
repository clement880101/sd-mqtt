//! The broker behind its lock, and where to send what it produces.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use broker_core::broker::{Addressed, Broker, BrokerConfig};
use broker_core::session::Action;
use broker_core::subscriptions::SubscriberId;
use foldhash::fast::RandomState;
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc;

/// The broker, and where to send what it produces.
///
/// The broker's methods take an exclusive reference, so every packet in
/// the process passes through one lock. At the connection counts this
/// design targets that is the bottleneck, and it undoes the sharding the
/// subscription trie does internally. It is measured rather than guessed
/// at: see the benchmark in this crate. What matters until then is that
/// the lock is held for the shortest possible span and **never across an
/// await**, so the number measures the design rather than an accident.
pub struct Shared {
    broker: Mutex<Broker>,
    outboxes: RwLock<HashMap<SubscriberId, mpsc::Sender<Action>, RandomState>>,
    dropped_for_slow_reader: AtomicU64,
}

impl Shared {
    /// A server with no connections.
    pub fn new(config: BrokerConfig) -> Self {
        Self {
            broker: Mutex::new(Broker::new(config)),
            outboxes: RwLock::new(HashMap::with_hasher(RandomState::default())),
            dropped_for_slow_reader: AtomicU64::new(0),
        }
    }

    /// Do something with the broker, holding the lock for exactly that.
    ///
    /// The closure must not await and must not block. Anything slow
    /// belongs outside it.
    pub fn with_broker<T>(&self, f: impl FnOnce(&mut Broker) -> T) -> T {
        let mut broker = self.broker.lock();
        f(&mut broker)
    }

    /// Claim the outbox for a session, returning the receiving half.
    ///
    /// A handle names a session rather than a connection, so a takeover
    /// registers a second outbox under the same handle. The newcomer
    /// replaces the incumbent, whose receiver then sees the channel
    /// closed, which is how its task learns it has been displaced.
    pub fn register_outbox(
        &self,
        subscriber: SubscriberId,
        depth: usize,
    ) -> mpsc::Receiver<Action> {
        let (sender, receiver) = mpsc::channel(depth);
        self.outboxes.write().insert(subscriber, sender);
        receiver
    }

    /// Forget a session's outbox.
    pub fn remove_outbox(&self, subscriber: SubscriberId) {
        self.outboxes.write().remove(&subscriber);
    }

    /// Send each action to the session it is addressed to.
    ///
    /// Never blocks. Waiting here would hold the read half of the
    /// outboxes lock, which every connect and every teardown needs the
    /// write half of, and would block the caller: another connection
    /// with its own client to serve, or the one ticker task that drives
    /// every deadline in the process.
    ///
    /// **A full outbox should now be impossible.** Its depth is derived
    /// from the configured queue and window bounds, and a configuration
    /// that breaks the relation is refused at startup, so no drain can
    /// produce more actions at once than the channel holds. The branch
    /// is kept because the alternative to an unreachable branch is an
    /// unchecked assumption, and it shouts rather than counting quietly:
    /// a message discarded here carries no reason to the session that
    /// lost it, and if it was an acknowledged delivery its window slot
    /// is held for a receipt that can never come.
    pub fn deliver(&self, actions: Vec<Addressed>) {
        if actions.is_empty() {
            return;
        }
        let outboxes = self.outboxes.read();
        for (subscriber, action) in actions {
            let Some(sender) = outboxes.get(&subscriber) else {
                // The session went away between the broker producing this
                // and us delivering it. Ordinary, not an error.
                continue;
            };
            match sender.try_send(action) {
                Ok(()) => {}
                // The writer has stopped and its receiver is gone. That
                // is the same ordinary teardown as a missing outbox,
                // reached a moment later, and counting it as a slow
                // reader would make the counter report routine
                // disconnections as a capacity problem.
                Err(mpsc::error::TrySendError::Closed(_)) => {}
                Err(mpsc::error::TrySendError::Full(action)) => {
                    self.dropped_for_slow_reader.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        subscriber = subscriber.0,
                        kind = ?std::mem::discriminant(&action),
                        "an outbox was full, which its sizing should make \
                         impossible; an action was discarded with no reason \
                         delivered to the session"
                    );
                }
            }
        }
    }

    /// How many sessions have an outbox.
    pub fn outbox_count(&self) -> usize {
        self.outboxes.read().len()
    }

    /// How many actions were discarded because a reader could not keep up.
    pub fn dropped_for_slow_reader(&self) -> u64 {
        self.dropped_for_slow_reader.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_core::subscriptions::SubscriberId;
    use mqtt_codec::ReasonCode;

    #[tokio::test]
    async fn an_outbox_receives_what_is_addressed_to_it() {
        let shared = Shared::new(Default::default());
        let mut inbox = shared.register_outbox(SubscriberId(1), 8);

        shared.deliver(vec![(
            SubscriberId(1),
            broker_core::session::Action::Close(ReasonCode::Success),
        )]);

        let action = inbox.recv().await.expect("delivered");
        assert!(matches!(
            action,
            broker_core::session::Action::Close(ReasonCode::Success)
        ));
    }

    #[tokio::test]
    async fn an_action_for_an_unknown_session_is_discarded_not_panicked() {
        // A session can be torn down between the broker producing an
        // action and the shell delivering it. That is ordinary, not an
        // error.
        let shared = Shared::new(Default::default());
        shared.deliver(vec![(
            SubscriberId(99),
            broker_core::session::Action::Close(ReasonCode::Success),
        )]);
        assert_eq!(shared.outbox_count(), 0);
    }

    #[tokio::test]
    async fn removing_an_outbox_stops_delivery() {
        let shared = Shared::new(Default::default());
        let mut inbox = shared.register_outbox(SubscriberId(1), 8);
        shared.remove_outbox(SubscriberId(1));

        shared.deliver(vec![(
            SubscriberId(1),
            broker_core::session::Action::Close(ReasonCode::Success),
        )]);
        assert!(inbox.try_recv().is_err());
        assert_eq!(shared.outbox_count(), 0);
    }

    #[tokio::test]
    async fn a_full_outbox_does_not_block_the_sender() {
        // Not the broker's mutex: it is released before anything is
        // delivered and this path never takes it. What waiting here
        // would hold is the read half of the outboxes lock, which
        // every connect and every teardown needs the write half of,
        // and the caller itself — another connection with its own
        // client to serve, or the one ticker task that drives every
        // deadline in the process.
        let shared = Shared::new(Default::default());
        let _inbox = shared.register_outbox(SubscriberId(1), 1);

        for _ in 0..100 {
            shared.deliver(vec![(
                SubscriberId(1),
                broker_core::session::Action::Close(ReasonCode::Success),
            )]);
        }
        assert_eq!(shared.dropped_for_slow_reader(), 99);
    }

    #[tokio::test]
    async fn a_closed_outbox_is_not_a_slow_reader() {
        // A writer that has stopped leaves its outbox registered for a
        // moment longer than its receiver. That is the ordinary end of
        // a connection, and counting it here would make the slow-reader
        // figure climb with routine disconnections, which is the one
        // thing it must not do if anyone is to act on it.
        let shared = Shared::new(Default::default());
        let inbox = shared.register_outbox(SubscriberId(1), 8);
        drop(inbox);

        shared.deliver(vec![(
            SubscriberId(1),
            broker_core::session::Action::Close(ReasonCode::Success),
        )]);

        assert_eq!(
            shared.dropped_for_slow_reader(),
            0,
            "a closed outbox is a departure, not a capacity problem"
        );
    }

    #[tokio::test]
    async fn the_broker_is_reachable_under_the_lock() {
        let shared = Shared::new(Default::default());
        assert_eq!(shared.with_broker(|broker| broker.session_count()), 0);
    }

    #[tokio::test]
    async fn a_takeover_replaces_the_outbox_for_the_same_handle() {
        // A handle names a session, not a connection. The newcomer's
        // outbox must replace the incumbent's, or the broker's
        // redeliveries go to a socket that is closing.
        let shared = Shared::new(Default::default());
        let mut first = shared.register_outbox(SubscriberId(1), 8);
        let mut second = shared.register_outbox(SubscriberId(1), 8);

        shared.deliver(vec![(
            SubscriberId(1),
            broker_core::session::Action::Close(ReasonCode::Success),
        )]);
        assert!(second.try_recv().is_ok());
        assert!(first.try_recv().is_err(), "the incumbent is disconnected");
        assert_eq!(shared.outbox_count(), 1);
    }
}
