//! Sessions by identity, and what happens when two connections claim one.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use foldhash::fast::RandomState;
use parking_lot::Mutex;

use crate::session::queue::{QueueLimits, QueueTotal};
use crate::session::state::AttachedSessions;
use crate::session::state::Session;
use crate::session::state::SessionBounds;
use crate::subscriptions::SubscriberId;

/// What identifies a session.
///
/// The mountpoint namespaces a listener's clients into their own topic
/// tree, so the same client identifier arriving on two listeners is two
/// sessions. That is multi-tenancy for the cost of one string.
///
/// **Ordered, and the order is load bearing.** The identity index is a
/// `BTreeMap` keyed by this, and the admin API pages its sessions,
/// clients and subscriptions by seeking into it, so the order derived
/// here — mountpoint first, then client identifier — is the order those
/// pages come out in. `the_ordering_key_is_the_session_identity` in
/// `sdmqtt-admin` is the other end of that agreement. Swapping the two
/// fields would silently reorder every page.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

/// How many changes [`Presence`] holds before it gives up and tells
/// its reader to look for itself.
///
/// A bound rather than a growing log, because the log is written by
/// every connect and disconnect and drained by one task: a reader
/// that stops draining must not turn every departure into memory
/// nothing frees. Sixty-five thousand is far more than a second of
/// churn on a broker sized for a hundred thousand sessions — the
/// interface samples once a second — and small enough to be a
/// megabyte or so of handles rather than something to think about.
pub const MAX_PENDING_PRESENCE: usize = 65_536;

/// One session's connectedness, as of the last thing that happened
/// to it.
#[derive(Debug, Clone)]
pub struct PresenceChange {
    /// The handle, which is never reused, so a reader may key on it.
    pub subscriber: SubscriberId,
    /// Who it is, for a reader that has to name it.
    pub id: SessionId,
    /// Where it ended up.
    pub connected: bool,
}

/// Which sessions gained or lost a connection since somebody looked.
///
/// # Why this exists
///
/// Nothing in this crate reports a connect to anybody — the broker
/// does no I/O and has no observer to tell — so the admin listener's
/// events stream *derived* connects by walking every session once a
/// second and diffing the result. That walk was measured at 11.9
/// milliseconds with a hundred thousand sessions, under the one lock
/// the whole process shares, and it ran whether or not a browser was
/// watching. It is the same shape as summing every queue to answer
/// one gauge, and it has the same answer: maintain it where it
/// changes. See [`QueueTotal`], which is that answer written down
/// the first time.
///
/// [`QueueTotal`]: crate::session::QueueTotal
///
/// # What it costs when nobody is reading
///
/// One relaxed atomic load per connect and per disconnect. Recording
/// starts when a reader calls [`Presence::watch`] and not before, so
/// a broker with no admin listener — which is every test in this
/// crate and every embedding that does not want one — allocates
/// nothing and holds nothing.
///
/// # What a reader has to accept
///
/// **Only the final state of each session is kept.** A client that
/// connected and left between two reads is one entry saying it is
/// gone, not two saying both. That is what the walk did too: a
/// sample cannot see what happened between samples, and pretending
/// otherwise would be a history this structure does not have.
///
/// **A reader that stops reading is told to start again.** Past
/// [`MAX_PENDING_PRESENCE`] the log is dropped and the next
/// [`Presence::take`] says so, which is the reader's signal to walk
/// the sessions itself rather than to carry on with a set that has
/// quietly lost entries.
///
/// **There is one reader.** A read takes the changes away, so two
/// readers would each see the half the other did not, and both
/// would draw a broker that is not there. One is what exists: the
/// process has one admin listener and it has one sampler. A second
/// observer wants its own log — this type behind another handle —
/// and not a second [`Presence::take`] on this one. Said here
/// because the shape does not refuse it and the failure would be
/// silent.
#[derive(Clone, Debug, Default)]
pub struct Presence(Arc<PresenceLog>);

/// What [`Presence`] holds, behind one lock.
#[derive(Debug, Default)]
struct PresenceLog {
    /// Whether anybody is reading. Nothing is recorded until
    /// somebody is.
    watched: AtomicBool,
    changed: Mutex<Changed>,
}

/// The changes themselves, and whether any were lost.
#[derive(Debug, Default)]
struct Changed {
    /// Keyed by handle, so a session that moves twice between reads
    /// is one entry and the log cannot grow past the population that
    /// moved.
    by_subscriber: HashMap<SubscriberId, (SessionId, bool), RandomState>,
    /// Set when [`MAX_PENDING_PRESENCE`] was reached and the log was
    /// dropped. Cleared by the read that is told about it.
    overflowed: bool,
}

impl Presence {
    /// Start recording. Idempotent, and the only way recording
    /// begins.
    ///
    /// A reader calls this once and then walks the sessions for its
    /// first picture, in that order and under one acquisition of the
    /// broker's lock: the other order loses every change that
    /// happens between the walk and the arming.
    pub fn watch(&self) {
        self.0.watched.store(true, Ordering::Relaxed);
    }

    /// Whether anybody has asked to be told.
    pub fn is_watched(&self) -> bool {
        self.0.watched.load(Ordering::Relaxed)
    }

    /// Note that `id` is now connected, or is not.
    ///
    /// Called from the three places a session's connectedness can
    /// change and from the one place a session is destroyed. It is
    /// deliberately not public: a caller that could write to this
    /// could tell a browser about a connect that never happened.
    pub(crate) fn record(&self, subscriber: SubscriberId, id: &SessionId, connected: bool) {
        if !self.is_watched() {
            return;
        }
        let mut changed = self.0.changed.lock();
        if changed.by_subscriber.len() >= MAX_PENDING_PRESENCE
            && !changed.by_subscriber.contains_key(&subscriber)
        {
            // Nobody has read this for a very long time. Holding a
            // partial log would hand that reader a set with holes in
            // it that looks complete; saying so hands it a walk.
            changed.by_subscriber.clear();
            changed.by_subscriber.shrink_to_fit();
            changed.overflowed = true;
            return;
        }
        changed
            .by_subscriber
            .insert(subscriber, (id.clone(), connected));
    }

    /// Take everything recorded since the last call.
    ///
    /// Returns whether the log overflowed, in which case `out` is
    /// whatever survived and the reader must rebuild its picture by
    /// walking the sessions.
    pub fn take(&self, out: &mut Vec<PresenceChange>) -> bool {
        let mut changed = self.0.changed.lock();
        out.reserve(changed.by_subscriber.len());
        for (subscriber, (id, connected)) in changed.by_subscriber.drain() {
            out.push(PresenceChange {
                subscriber,
                id,
                connected,
            });
        }
        std::mem::replace(&mut changed.overflowed, false)
    }

    /// How many changes are waiting to be read. For a test, and for
    /// nothing else.
    pub fn pending(&self) -> usize {
        self.0.changed.lock().by_subscriber.len()
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
    /// Every session's handle, keyed by its identity, **in identity
    /// order**.
    ///
    /// Ordered rather than hashed, and the order is what it is for as
    /// much as the lookup is. A hash map can answer "which handle is
    /// this identity" and nothing else; the admin API's sessions,
    /// clients and subscriptions pages all ask "the next hundred
    /// identities after this one", and with no order to resume from
    /// the only way to answer was to look at every session and keep
    /// the smallest hundred. That was 13.5, 7.9 and 25.5
    /// milliseconds respectively with a hundred thousand sessions, on
    /// the process's one lock, on endpoints section 11.1 leaves
    /// unauthenticated.
    ///
    /// A `BTreeMap` answers both, so this is one structure rather
    /// than a hash map with an ordered index beside it that could
    /// drift from it. What it costs is the lookup: a handful of short
    /// string comparisons instead of one hash, on connect and on the
    /// one other caller, [`Registry::subscriber_of`]. Neither is a
    /// per-packet path — routing a publish goes through the
    /// subscription trie and then [`Registry::get`], which is still
    /// the hash map below.
    by_identity: BTreeMap<SessionId, SubscriberId>,
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
            by_identity: BTreeMap::new(),
            sessions: HashMap::default(),
            next_subscriber: 0,
            next_epoch: 0,
            bounds: SessionBounds {
                max_inflight,
                queue,
                total: QueueTotal::default(),
                presence: Presence::default(),
                attached: AttachedSessions::default(),
            },
        }
    }

    /// Which sessions have gained or lost a connection.
    ///
    /// Empty and free until somebody calls [`Presence::watch`]. See
    /// [`Presence`] for what it replaced and why.
    pub fn presence(&self) -> &Presence {
        &self.bounds.presence
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

    /// How many sessions have a connection attached.
    ///
    /// Read rather than counted; see [`AttachedSessions`] for what
    /// the walk cost and where this is kept in step.
    pub fn attached(&self) -> usize {
        self.bounds.attached.get()
    }

    /// The same, by asking every session. For the test that keeps the
    /// maintained figure honest, and for nothing else.
    pub fn attached_by_walking(&self) -> usize {
        self.sessions
            .values()
            .filter(|session| session.is_connected())
            .count()
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
                // A clean start over a live connection replaces one
                // connected session with another, and the count of
                // connected sessions must come out where it went in.
                // It does, in either order: an assignment evaluates
                // its right-hand side before it drops what was in the
                // place, so the newcomer's arrival is counted and the
                // old session's departure taken off it, and destroying
                // the old one first would net out the same way. Said
                // out loud because it looks like it depends on the
                // order and does not, and because the mutation that
                // swaps them is one a reviewer would otherwise be
                // asked to check for. See [`AttachedSessions`].
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
        // A session that goes while it is still connected is a
        // departure nothing else records: [`Session::on_disconnected`]
        // is what usually does it, and a destroyed session never
        // reaches it. Today only an expired session is destroyed and
        // a connected session cannot expire, so this is the branch
        // that stops a future caller leaving a reader holding a
        // client that is not there. It is reached in
        // `destroying_a_connected_session_is_recorded_as_a_departure`.
        if session.is_connected() {
            self.bounds.presence.record(subscriber, session.id(), false);
        }
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

    /// Sessions in identity order, starting strictly after `after`.
    ///
    /// The ordered half of what [`Registry::by_identity`] is for.
    /// Strictly after, so the session a cursor names is not returned
    /// twice, and a cursor naming a session that has since gone
    /// resumes at whatever now follows it rather than failing — which
    /// is the whole reason the admin API's cursor is a key and not an
    /// offset.
    ///
    /// The iterator is lazy, so a caller taking a page of a hundred
    /// looks at a hundred sessions and no more. A caller filtering as
    /// it goes — the Clients page, which wants only the connected —
    /// looks at as many as it has to skip, which is a fact about the
    /// population rather than about this.
    ///
    /// [`Registry::by_identity`]: Registry
    pub fn iter_after(
        &self,
        after: Option<&SessionId>,
    ) -> impl Iterator<Item = (SubscriberId, &Session)> {
        self.iter_from(match after {
            Some(after) => Bound::Excluded(after),
            None => Bound::Unbounded,
        })
    }

    /// Sessions in identity order from `lower`.
    ///
    /// The inclusive bound exists for a reader paging something a
    /// session holds several of rather than the sessions themselves:
    /// the admin API's subscriptions page resumes *inside* a session,
    /// at the filter after the one its cursor names, so it has to be
    /// given that session again. Giving it the next one instead would
    /// silently drop every filter of the last session on a page after
    /// the first.
    pub fn iter_from(
        &self,
        lower: Bound<&SessionId>,
    ) -> impl Iterator<Item = (SubscriberId, &Session)> {
        self.by_identity
            .range((lower, Bound::Unbounded))
            .map(|(_, subscriber)| {
                (
                    *subscriber,
                    self.sessions
                        .get(subscriber)
                        .expect("the index and the map agree"),
                )
            })
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

    // -----------------------------------------------------------
    // How many are connected, for a page that would otherwise count
    // -----------------------------------------------------------

    /// The count and the walk, after every path that can move it.
    fn agree(registry: &Registry, after: &str) {
        assert_eq!(
            registry.attached(),
            registry.attached_by_walking(),
            "the maintained count and the sessions disagree after {after}"
        );
    }

    #[test]
    fn the_attached_count_follows_every_arrival_and_departure() {
        let mut registry = Registry::new(20, QueueLimits::default());
        agree(&registry, "nothing at all");
        assert_eq!(registry.attached(), 0);

        let a = registry.register(id("a"), true, 0);
        let b = registry.register(id("b"), true, 0);
        agree(&registry, "two connects");
        assert_eq!(registry.attached(), 2);

        depart(&mut registry, a.subscriber);
        agree(&registry, "a departure");
        assert_eq!(registry.attached(), 1);

        // A second departure for a session already gone is not a
        // second departure.
        depart(&mut registry, a.subscriber);
        agree(&registry, "a departure repeated");
        assert_eq!(registry.attached(), 1);

        registry.register(id("a"), false, 0);
        agree(&registry, "a resume");
        assert_eq!(registry.attached(), 2);

        // Takeover displaces one connection with another. The
        // identity never stopped being connected, so the count does
        // not move.
        registry.register(id("b"), false, 0);
        agree(&registry, "a takeover");
        assert_eq!(registry.attached(), 2);

        registry.remove(b.subscriber);
        agree(&registry, "destroying a connected session");
        assert_eq!(registry.attached(), 1);
    }

    /// A clean start over a live connection leaves one connection,
    /// not none.
    ///
    /// Convention 5's "a path no test walks", and it is a path the
    /// obvious code gets wrong silently. `*slot = Session::new(..)`
    /// builds the replacement and drops what was there *afterwards*,
    /// so the arrival is counted and then the old session's departure
    /// takes it off again: a connected client missing from the
    /// Clients page's total for as long as it stays. Nothing else
    /// here notices, because the session itself is perfectly correct.
    #[test]
    fn a_clean_start_over_a_live_session_is_still_one_connection() {
        let mut registry = Registry::new(20, QueueLimits::default());
        let first = registry.register(id("a"), true, 0);
        assert_eq!(registry.attached(), 1);

        let second = registry.register(id("a"), true, 5);
        assert_eq!(second.subscriber, first.subscriber, "one identity");
        assert!(second.took_over);
        assert!(!second.session_present, "clean start discarded the state");
        assert!(
            registry
                .get(second.subscriber)
                .is_some_and(Session::is_connected),
            "the newcomer is connected"
        );
        agree(&registry, "a clean start over a live connection");
        assert_eq!(registry.attached(), 1);
    }

    // -----------------------------------------------------------
    // Sessions in identity order
    // -----------------------------------------------------------

    /// What `iter_after` gives, as `mountpoint/client`.
    fn ordered(registry: &Registry, after: Option<&SessionId>) -> Vec<String> {
        registry
            .iter_after(after)
            .map(|(_, session)| {
                let id = session.id();
                format!("{}/{}", id.mountpoint, id.client_id)
            })
            .collect()
    }

    /// The order is mountpoint first, then client identifier, and it
    /// is not the order sessions arrived in.
    ///
    /// The admin API pages by seeking into this, so this order is the
    /// order its rows come out in.
    #[test]
    fn sessions_come_out_in_identity_order() {
        let mut registry = Registry::new(20, QueueLimits::default());
        for (mountpoint, client) in [
            ("tenant-b", "z"),
            ("", "m"),
            ("tenant-a", "b"),
            ("", "a"),
            ("tenant-a", "a"),
        ] {
            registry.register(SessionId::new(mountpoint, client), true, 0);
        }
        assert_eq!(
            ordered(&registry, None),
            ["/a", "/m", "tenant-a/a", "tenant-a/b", "tenant-b/z"]
        );
    }

    /// Resuming after a session gives the rest and never it again,
    /// and a cursor naming a session that has gone still works.
    #[test]
    fn resuming_after_a_session_gives_what_follows_it() {
        let mut registry = Registry::new(20, QueueLimits::default());
        for client in ["a", "b", "c", "d"] {
            registry.register(id(client), true, 0);
        }
        assert_eq!(ordered(&registry, Some(&id("b"))), ["/c", "/d"]);

        let gone = registry.subscriber_of(&id("b")).expect("it is there");
        registry.remove(gone);
        assert_eq!(
            ordered(&registry, Some(&id("b"))),
            ["/c", "/d"],
            "a cursor naming a session that has gone resumes after it"
        );
        assert_eq!(ordered(&registry, Some(&id("d"))), Vec::<String>::new());
    }

    // -----------------------------------------------------------
    // What changed, for a reader that would otherwise walk
    // -----------------------------------------------------------

    /// What the presence log holds, as `client:connected`.
    fn recorded(registry: &Registry) -> Vec<String> {
        let mut out = Vec::new();
        registry.presence().take(&mut out);
        let mut lines: Vec<String> = out
            .iter()
            .map(|change| format!("{}:{}", change.id.client_id, change.connected))
            .collect();
        lines.sort();
        lines
    }

    #[test]
    fn nothing_is_recorded_until_somebody_is_reading() {
        // The cost of the log on a broker with no admin listener,
        // which is most of them: one atomic load and no memory.
        let mut registry = Registry::new(20, QueueLimits::default());
        let first = registry.register(id("a"), true, 0);
        depart(&mut registry, first.subscriber);
        assert_eq!(registry.presence().pending(), 0);
        assert!(recorded(&registry).is_empty());

        registry.presence().watch();
        registry.register(id("b"), true, 0);
        assert_eq!(recorded(&registry), vec!["b:true"]);
    }

    #[test]
    fn a_connect_and_a_disconnect_are_recorded() {
        let mut registry = Registry::new(20, QueueLimits::default());
        registry.presence().watch();
        let first = registry.register(id("a"), true, 0);
        assert_eq!(recorded(&registry), vec!["a:true"]);

        depart(&mut registry, first.subscriber);
        assert_eq!(recorded(&registry), vec!["a:false"]);

        // A reconnect onto the session it left behind, which goes
        // through `claim` rather than through a fresh session.
        let again = registry.register(id("a"), false, 0);
        assert_eq!(again.subscriber, first.subscriber);
        assert!(again.session_present);
        assert_eq!(recorded(&registry), vec!["a:true"]);
    }

    #[test]
    fn a_read_takes_each_change_once() {
        let mut registry = Registry::new(20, QueueLimits::default());
        registry.presence().watch();
        registry.register(id("a"), true, 0);
        assert_eq!(recorded(&registry), vec!["a:true"]);
        assert!(
            recorded(&registry).is_empty(),
            "a change already read is not read again"
        );
    }

    #[test]
    fn a_session_that_moves_twice_is_one_entry() {
        // The bound on what the log can hold is that it is keyed by
        // handle: a client flapping a thousand times between two
        // reads is one entry saying where it ended up, not a
        // thousand. A log that grew with the flapping would be the
        // leak this structure exists instead of.
        let mut registry = Registry::new(20, QueueLimits::default());
        registry.presence().watch();
        let a = registry.register(id("a"), true, 0);
        for _ in 0..1_000 {
            depart(&mut registry, a.subscriber);
            registry.register(id("a"), false, 0);
        }
        assert_eq!(registry.presence().pending(), 1);
        assert_eq!(recorded(&registry), vec!["a:true"]);
    }

    #[test]
    fn a_log_nobody_reads_is_bounded_and_says_so() {
        // Convention 5: the branch that gives up is reachable, and
        // what it promises — that the reader is *told*, rather than
        // handed a set with holes in it — is what is asserted.
        let mut registry = Registry::new(20, QueueLimits::default());
        registry.presence().watch();
        for n in 0..MAX_PENDING_PRESENCE + 1 {
            registry.register(id(&format!("client-{n}")), true, 0);
        }
        assert!(
            registry.presence().pending() <= MAX_PENDING_PRESENCE,
            "the log is bounded, whatever nobody reads: {}",
            registry.presence().pending()
        );

        let mut out = Vec::new();
        assert!(
            registry.presence().take(&mut out),
            "and the reader is told that what it is holding is not the whole story"
        );
        let mut after = Vec::new();
        assert!(
            !registry.presence().take(&mut after),
            "the overflow is reported once, to the reader that can act on it"
        );
    }

    #[test]
    fn destroying_a_connected_session_is_recorded_as_a_departure() {
        // No path in the broker destroys a connected session today —
        // only an expired one is removed, and a connected session
        // cannot expire — so this reaches `remove` directly. Without
        // the record, a reader would hold a client that no longer
        // exists and no later event would ever take it away.
        let mut registry = Registry::new(20, QueueLimits::default());
        registry.presence().watch();
        let a = registry.register(id("a"), true, 0);
        assert_eq!(recorded(&registry), vec!["a:true"]);
        registry.remove(a.subscriber);
        assert_eq!(recorded(&registry), vec!["a:false"]);
    }

    #[test]
    fn a_session_taken_over_stays_connected() {
        // Takeover is not a disconnect and then a connect: the
        // identity never stopped being connected, and reporting one
        // would take the client's row off the page for a moment it
        // was never gone for.
        let mut registry = Registry::new(20, QueueLimits::default());
        registry.presence().watch();
        let first = registry.register(id("a"), true, 0);
        assert_eq!(recorded(&registry), vec!["a:true"]);
        let second = registry.register(id("a"), true, 0);
        assert_eq!(
            second.subscriber, first.subscriber,
            "one identity, one handle"
        );
        assert!(second.took_over);
        assert_eq!(
            recorded(&registry),
            vec!["a:true"],
            "still connected, which is what a reader already believes"
        );
    }
}
