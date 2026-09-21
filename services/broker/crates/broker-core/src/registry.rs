//! Sessions by identity, and what happens when two connections claim one.

use std::collections::HashMap;
use std::sync::Arc;

use foldhash::fast::RandomState;

use crate::session::queue::{QueueLimits, QueueTotal};
use crate::session::state::Session;
use crate::session::state::SessionBounds;
use crate::subscriptions::SubscriberId;

/// What identifies a session.
///
/// The mountpoint namespaces a listener's clients into their own topic
/// tree, so the same client identifier arriving on two listeners is two
/// sessions. That is multi-tenancy for the cost of one string.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId {
    /// The listener's namespace, empty for the default.
    pub mountpoint: Arc<str>,
    /// The identifier the client chose, or one assigned to it.
    pub client_id: Arc<str>,
}

impl SessionId {
    /// Build an identity.
    pub fn new(mountpoint: &str, client_id: &str) -> Self {
        Self {
            mountpoint: Arc::from(mountpoint),
            client_id: Arc::from(client_id),
        }
    }
}

/// The outcome of a connect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Registration {
    /// The session's handle, which keys its subscriptions.
    pub subscriber: SubscriberId,
    /// Whether state was resumed rather than created. This is what the
    /// acknowledgement's session-present flag reports.
    pub session_present: bool,
    /// Whether a live connection was displaced and must be closed with
    /// "session taken over".
    pub took_over: bool,
    /// The claim's generation. Higher wins.
    pub epoch: u64,
}

/// Sessions by identity.
///
/// There is deliberately no `disconnect` here. One existed, marking the
/// departure graceful, and a graceful departure discards the will: a
/// shell reaching for the obvious-looking method on the obvious-looking
/// type would have silently lost every will in the system. Nor could
/// aligning it have saved it, because a departure must also disarm the
/// keep-alive and drop the activity cell, and neither lives here.
/// Ending a connection belongs to the assembly that owns all three:
/// see `Broker::disconnect`.
pub struct Registry {
    by_identity: HashMap<SessionId, SubscriberId, RandomState>,
    sessions: HashMap<SubscriberId, Session, RandomState>,
    next_subscriber: u64,
    next_epoch: u64,
    /// The bounds every session it builds starts with, the
    /// process-wide queue total among them. Held rather than passed
    /// per call because a session is also built by a clean start,
    /// which has nothing to hand it.
    bounds: SessionBounds,
}

impl Registry {
    /// An empty registry whose sessions start with `max_inflight` and
    /// the given queue bounds.
    pub fn new(max_inflight: usize, queue: QueueLimits) -> Self {
        Self {
            by_identity: HashMap::default(),
            sessions: HashMap::default(),
            next_subscriber: 0,
            next_epoch: 0,
            bounds: SessionBounds {
                max_inflight,
                queue,
                total: QueueTotal::default(),
            },
        }
    }

    /// How many messages are waiting, across every session.
    ///
    /// Read rather than computed. [`Registry::queued_by_walking`] is
    /// the same number the slow way, and a test asserts they agree.
    pub fn queued(&self) -> usize {
        self.bounds.total.get()
    }

    /// The same, by asking every session. For the test that keeps the
    /// maintained figure honest, and for nothing else.
    pub fn queued_by_walking(&self) -> usize {
        self.sessions
            .values()
            .map(|session| session.outbound.queued_len())
            .sum()
    }

    /// Claim a session for a connecting client.
    ///
    /// Claiming one that already has a live connection is takeover, not an
    /// error: the protocol says the newcomer wins and the incumbent is
    /// disconnected. The epoch decides who wins if two nodes both accepted
    /// the client during a partition, so it only ever increases.
    pub fn register(&mut self, id: SessionId, clean_start: bool, now: u32) -> Registration {
        self.next_epoch += 1;
        let epoch = self.next_epoch;

        if let Some(subscriber) = self.by_identity.get(&id).copied() {
            let existing = self
                .sessions
                .get_mut(&subscriber)
                .expect("the index and the map agree");
            let took_over = existing.is_connected();

            if clean_start {
                let version = existing.version();
                *existing = Session::new(id, subscriber, version, epoch, now, self.bounds.clone());
                return Registration {
                    subscriber,
                    session_present: false,
                    took_over,
                    epoch,
                };
            }

            existing.claim(epoch, now);
            return Registration {
                subscriber,
                session_present: true,
                took_over,
                epoch,
            };
        }

        // A handle is never reused, because the subscription trie is keyed
        // by it and reuse would hand one session another's subscriptions.
        let subscriber = SubscriberId(self.next_subscriber);
        self.next_subscriber += 1;
        self.by_identity.insert(id.clone(), subscriber);
        self.sessions.insert(
            subscriber,
            Session::new(
                id,
                subscriber,
                crate::session::state::DEFAULT_VERSION,
                epoch,
                now,
                self.bounds.clone(),
            ),
        );

        Registration {
            subscriber,
            session_present: false,
            took_over: false,
            epoch,
        }
    }

    /// A session by handle.
    pub fn get(&self, subscriber: SubscriberId) -> Option<&Session> {
        self.sessions.get(&subscriber)
    }

    /// A session by handle, mutably.
    pub fn get_mut(&mut self, subscriber: SubscriberId) -> Option<&mut Session> {
        self.sessions.get_mut(&subscriber)
    }

    /// The handle for an identity.
    pub fn subscriber_of(&self, id: &SessionId) -> Option<SubscriberId> {
        self.by_identity.get(id).copied()
    }

    /// Destroy a session.
    pub fn remove(&mut self, subscriber: SubscriberId) -> Option<Session> {
        let session = self.sessions.remove(&subscriber)?;
        self.by_identity.remove(session.id());
        Some(session)
    }

    /// Every session, mutably, for a sweep.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&SubscriberId, &mut Session)> {
        self.sessions.iter_mut()
    }

    /// Every session, for a report.
    pub fn iter(&self) -> impl Iterator<Item = (&SubscriberId, &Session)> {
        self.sessions.iter()
    }

    /// How many sessions exist, connected or not.
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    /// Whether none do.
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(client: &str) -> SessionId {
        SessionId::new("", client)
    }

    /// A connection going away, spelled the way the broker spells it.
    ///
    /// The registry used to offer this itself, and offering it was the
    /// trap: it marked every departure graceful, which discards the
    /// will. See the note on [`Registry`].
    fn depart(registry: &mut Registry, subscriber: SubscriberId) {
        registry
            .get_mut(subscriber)
            .expect("a registered session")
            .on_disconnected(false, 0);
    }

    #[test]
    fn a_first_connect_creates_a_session() {
        let mut registry = Registry::new(20, QueueLimits::default());
        let first = registry.register(id("a"), true, 0);
        assert!(!first.session_present);
        assert!(!first.took_over);
        assert_eq!(first.epoch, 1);
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn distinct_clients_get_distinct_subscribers() {
        let mut registry = Registry::new(20, QueueLimits::default());
        let a = registry.register(id("a"), true, 0);
        let b = registry.register(id("b"), true, 0);
        assert_ne!(a.subscriber, b.subscriber);
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn the_same_client_on_a_different_mountpoint_is_a_different_session() {
        // A mountpoint namespaces a listener's clients, so the same
        // identifier on two listeners must not collide.
        let mut registry = Registry::new(20, QueueLimits::default());
        let a = registry.register(SessionId::new("tenant-a", "device"), true, 0);
        let b = registry.register(SessionId::new("tenant-b", "device"), true, 0);
        assert_ne!(a.subscriber, b.subscriber);
    }

    #[test]
    fn reconnecting_without_a_clean_start_resumes_the_session() {
        let mut registry = Registry::new(20, QueueLimits::default());
        let first = registry.register(id("a"), false, 0);
        depart(&mut registry, first.subscriber);

        let second = registry.register(id("a"), false, 10);
        assert!(second.session_present, "the session was still there");
        assert!(!second.took_over, "nothing live was displaced");
        assert_eq!(second.subscriber, first.subscriber, "the same session");
        assert_eq!(second.epoch, 2, "a new epoch each time");
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn a_clean_start_discards_whatever_was_there() {
        let mut registry = Registry::new(20, QueueLimits::default());
        let first = registry.register(id("a"), false, 0);
        depart(&mut registry, first.subscriber);

        let second = registry.register(id("a"), true, 10);
        assert!(!second.session_present, "clean start means a fresh session");
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn connecting_while_already_connected_takes_the_session_over() {
        let mut registry = Registry::new(20, QueueLimits::default());
        let first = registry.register(id("a"), false, 0);

        let second = registry.register(id("a"), false, 5);
        assert!(second.took_over, "the live connection must be displaced");
        assert!(second.session_present);
        assert_eq!(second.subscriber, first.subscriber);
        assert!(second.epoch > first.epoch, "the epoch decides who wins");
        assert_eq!(registry.len(), 1, "one session, not two");
    }

    #[test]
    fn the_epoch_never_goes_backwards() {
        // Two nodes may both accept a client during a partition. The
        // higher epoch wins when they meet, so it must only increase.
        let mut registry = Registry::new(20, QueueLimits::default());
        let mut last = 0;
        for n in 0..10 {
            let reg = registry.register(id("a"), false, n);
            assert!(reg.epoch > last);
            last = reg.epoch;
        }
    }

    #[test]
    fn a_session_can_be_looked_up_and_removed() {
        let mut registry = Registry::new(20, QueueLimits::default());
        let reg = registry.register(id("a"), true, 0);
        assert!(registry.get(reg.subscriber).is_some());
        assert_eq!(registry.subscriber_of(&id("a")), Some(reg.subscriber));

        registry.remove(reg.subscriber);
        assert!(registry.get(reg.subscriber).is_none());
        assert_eq!(registry.subscriber_of(&id("a")), None);
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn a_subscriber_identifier_is_never_reused() {
        // The subscription trie is keyed by it, so reuse would hand one
        // session another's subscriptions.
        let mut registry = Registry::new(20, QueueLimits::default());
        let first = registry.register(id("a"), true, 0);
        registry.remove(first.subscriber);
        let second = registry.register(id("b"), true, 0);
        assert_ne!(second.subscriber, first.subscriber);
    }
}
