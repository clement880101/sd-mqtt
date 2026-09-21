//! The interface connectors sit on.
//!
//! Section 9.3. A connector is not special-cased in the publish path;
//! it is a [`Hook`] the broker calls at nine points, because that is
//! the shape an out-of-process plugin transport would need later and
//! adding it afterwards would mean disturbing the core a second time.
//!
//! # A hook is synchronous, and that is the whole design
//!
//! **The obvious implementation of an HTTP connector is an `async fn`,
//! and this design refuses it.** Every method below returns a value
//! rather than a future, and this crate does no I/O and holds no lock
//! of its own — it is driven by a shell that holds the process's one
//! broker lock across the call. There is nowhere for an `await` to go
//! that would not either block that lock or reorder the packet stream
//! flowing through it.
//!
//! The outbound connector milestone four builds is written the way
//! every connector after it should be: its [`Hook::message_received`]
//! appends an envelope to a durable outbox and returns, and a separate
//! task reads the outbox and does the posting. The hook's job is to
//! decide and to record; the network is somebody else's.
//!
//! # What a hook may not do
//!
//! A hook runs on the thread that is handling a packet, under the lock
//! every packet in the process passes through. So for as long as a hook
//! is running, no other connection is served: not a publish, not a
//! subscribe, not the keep-alive sweep, not the admin API. A hook that
//! takes a millisecond costs the whole broker a millisecond.
//!
//! Therefore a hook must not:
//!
//! - **block**, on a socket, a file, a channel, a condition variable or
//!   a mutex another thread can hold for any length of time;
//!   `std::thread::sleep` included;
//! - **await**, or enter an async runtime — `Handle::block_on` from
//!   inside the broker lock is a deadlock waiting for the runtime it is
//!   blocking to make progress;
//! - **re-enter the broker**. It has no handle to one and must not be
//!   given one: the lock is already held and taking it again is a
//!   deadlock. A hook that wants to publish hands the message to
//!   something outside;
//! - **panic**. A panic here unwinds through the broker's lock, and a
//!   poisoned broker is the whole process;
//! - **allocate without bound**, or do work proportional to anything a
//!   client controls.
//!
//! **Nothing enforces any of this, and nothing can from inside this
//! crate.** It is a documented obligation on the implementor, and the
//! reasons are worth stating so that nobody looks for the guard that
//! is not there:
//!
//! - A type system cannot say "this function does not block". The
//!   signature is `fn(&self, ...) -> T`, and a blocking read has that
//!   signature just as a field access does.
//! - A watchdog could time a hook, but only by reporting afterwards.
//!   By the time it fires the lock has already been held too long, and
//!   the only remedies — killing the thread, or unwinding it — are
//!   worse than the stall.
//! - A panic could be caught with `catch_unwind`, which would turn a
//!   hook's panic into a denied decision. It is not, deliberately: a
//!   hook is in-process code compiled into this binary, `catch_unwind`
//!   would hide a bug that should be loud, and the crate forbids the
//!   `unsafe` that a sound implementation of the surrounding machinery
//!   tends to want. Out-of-process hooks, when they arrive, get this
//!   for free, because a crashing subprocess is not a panicking
//!   thread.
//!
//! What this crate *does* enforce is the part it can: a hook cannot
//! reach the broker, because it is never handed one, and it cannot
//! await, because it is never called from an async context that would
//! let it.
//!
//! # The chain
//!
//! The three authorizing points return a [`Decision`]. The first hook
//! to answer [`Decision::Allow`] or [`Decision::Deny`] ends the chain;
//! [`Decision::Next`] passes the question along. If every hook says
//! next — which includes the case of no hooks at all — the configured
//! [`Defaults`] apply.

use std::fmt;
use std::sync::Arc;

use mqtt_codec::packet::Publish;
use mqtt_codec::{QoS, Version};

use crate::session::message::Message;
use crate::session::queue::DropReason;
use crate::subscriptions::SubscriberId;

/// What an authorizing hook says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Permit it, and ask nobody else.
    Allow,
    /// Refuse it, and ask nobody else.
    Deny,
    /// No opinion; ask the next hook, or the default.
    Next,
}

/// What a message hook says should happen to the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Carry on with it, rewritten or not.
    Deliver,
    /// Do not route it. The publisher is still acknowledged, because a
    /// dropped message is not a protocol error.
    Drop,
}

/// What applies when every hook says [`Decision::Next`].
///
/// Three separate switches rather than one, because "anyone may
/// connect but only some may publish" is the ordinary shape of a
/// broker with a connector on it, and a single default cannot say it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Defaults {
    /// Whether an unclaimed connection is authenticated.
    pub authenticate: bool,
    /// Whether an unclaimed publish is permitted.
    pub publish: bool,
    /// Whether an unclaimed subscription is permitted.
    pub subscribe: bool,
}

impl Default for Defaults {
    /// Everything allowed.
    ///
    /// Which is what this broker did before there were hooks, and
    /// changing that silently would lock out every existing
    /// deployment on an upgrade. Section 12.1's access control, when
    /// it arrives, is a hook with its own configured default; it is
    /// not this one.
    fn default() -> Self {
        Self {
            authenticate: true,
            publish: true,
            subscribe: true,
        }
    }
}

/// Who is connecting, before there is a session to name.
#[derive(Debug, Clone, Copy)]
pub struct Credentials<'a> {
    /// The listener's namespace, empty for the default.
    pub mountpoint: &'a str,
    /// The identifier the client sent, which may be empty.
    pub client_id: &'a str,
    /// The offered user name.
    pub username: Option<&'a str>,
    /// The offered password, which is binary rather than text.
    pub password: Option<&'a [u8]>,
    /// Which protocol version the client speaks.
    pub version: Version,
    /// Whether it asked to discard any existing session.
    pub clean_start: bool,
}

/// Which session a hook is being told about.
#[derive(Debug, Clone, Copy)]
pub struct ClientInfo<'a> {
    /// The session's handle, which is never reused.
    pub subscriber: SubscriberId,
    /// The listener's namespace, empty for the default.
    pub mountpoint: &'a str,
    /// The identifier the session is keyed by.
    pub client_id: &'a str,
}

/// Something the broker tells, or asks, at one of section 9.3's nine
/// points.
///
/// Every method has a default that changes nothing, so a connector
/// implements the two or three it cares about. Read the module
/// documentation first: what a hook may not do is not enforced, and
/// the list is short.
pub trait Hook: Send + Sync {
    /// What to call this hook in a log line.
    fn name(&self) -> &str;

    /// A client is offering credentials.
    fn authenticate(&self, _credentials: &Credentials<'_>) -> Decision {
        Decision::Next
    }

    /// A client wants to publish this message.
    ///
    /// Read-only: rewriting belongs to [`Hook::message_received`], so
    /// that a hook cannot change a message after another hook
    /// authorized the version it saw.
    fn authorize_publish(&self, _client: &ClientInfo<'_>, _message: &Message) -> Decision {
        Decision::Next
    }

    /// A client wants to subscribe to this filter at this level.
    ///
    /// Asked once per filter, because [MQTT-3.8.4-5] answers a
    /// subscribe per filter and refusing the packet would punish the
    /// filters that were fine.
    fn authorize_subscribe(&self, _client: &ClientInfo<'_>, _filter: &str, _qos: QoS) -> Decision {
        Decision::Next
    }

    /// A message has arrived and has not been matched against anything
    /// yet.
    ///
    /// **Before matching, which is the point of it.** A hook may
    /// rewrite the topic, the payload, the level, the retain flag and
    /// the properties, and a rewritten topic has to decide where the
    /// message goes; a rewrite applied after matching would route by
    /// the old topic and deliver the new one.
    ///
    /// Returning [`Disposition::Drop`] means nobody receives it. The
    /// publisher is still acknowledged.
    fn message_received(&self, _client: &ClientInfo<'_>, _message: &mut Message) -> Disposition {
        Disposition::Deliver
    }

    /// A publish is on its way to a subscriber.
    ///
    /// Fired when the broker hands the packet to that session's
    /// connection, which is the only moment this crate can see: a
    /// crate that touches no socket cannot know when bytes left, and
    /// an acknowledgement is a separate event at levels one and two
    /// and does not exist at level zero.
    ///
    /// **Fired once per packet, not once per message.** A
    /// retransmission after a resumed session is another delivery
    /// attempt and says so in its duplicate flag; a hook that must
    /// count messages rather than attempts reads that flag. The
    /// packet is the whole of what went, so a connector sees the
    /// level the subscriber was granted and the subscription
    /// identifiers it will route on, neither of which is the
    /// publisher's.
    fn message_delivered(&self, _to: &ClientInfo<'_>, _publish: &Publish) {}

    /// A message will not be delivered, and why.
    ///
    /// Every discard path reaches this, including the ones a client
    /// never learns about. Section 11.3: every discard path has both a
    /// metric and a hook.
    fn message_dropped(&self, _to: &ClientInfo<'_>, _topic: &str, _qos: QoS, _reason: DropReason) {}

    /// A connection has been accepted and its session registered.
    fn client_connected(&self, _client: &ClientInfo<'_>) {}

    /// A connection has gone, gracefully or not. The session may
    /// outlive it.
    fn client_disconnected(&self, _client: &ClientInfo<'_>) {}

    /// A session has been destroyed, taking its subscriptions and its
    /// queue with it.
    fn session_expired(&self, _client: &ClientInfo<'_>) {}
}

/// The hooks a broker holds, in the order they are asked.
///
/// Cheap to hold and cheap to ask when it is empty, which is every
/// test in this crate and every embedding that wants no connectors:
/// the chain is a slice walk over nothing and the default is returned.
#[derive(Clone, Default)]
pub struct Hooks {
    installed: Vec<Arc<dyn Hook>>,
    defaults: Defaults,
}

impl fmt::Debug for Hooks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hooks")
            .field(
                "installed",
                &self
                    .installed
                    .iter()
                    .map(|hook| hook.name())
                    .collect::<Vec<_>>(),
            )
            .field("defaults", &self.defaults)
            .finish()
    }
}

impl Hooks {
    /// No hooks, everything allowed.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a hook to the end of the chain.
    pub fn install(&mut self, hook: Arc<dyn Hook>) {
        self.installed.push(hook);
    }

    /// What applies when every hook says [`Decision::Next`].
    pub fn set_defaults(&mut self, defaults: Defaults) {
        self.defaults = defaults;
    }

    /// The defaults in force.
    pub fn defaults(&self) -> Defaults {
        self.defaults
    }

    /// How many hooks are installed.
    pub fn len(&self) -> usize {
        self.installed.len()
    }

    /// Whether no hook is installed.
    pub fn is_empty(&self) -> bool {
        self.installed.is_empty()
    }

    /// Walk the chain until somebody answers, then fall back.
    fn chain(&self, default: bool, mut ask: impl FnMut(&dyn Hook) -> Decision) -> bool {
        for hook in &self.installed {
            match ask(hook.as_ref()) {
                Decision::Allow => return true,
                Decision::Deny => return false,
                Decision::Next => {}
            }
        }
        default
    }

    /// Whether this connection is authenticated.
    pub fn authenticate(&self, credentials: &Credentials<'_>) -> bool {
        self.chain(self.defaults.authenticate, |hook| {
            hook.authenticate(credentials)
        })
    }

    /// Whether this client may publish this message.
    pub fn authorize_publish(&self, client: &ClientInfo<'_>, message: &Message) -> bool {
        self.chain(self.defaults.publish, |hook| {
            hook.authorize_publish(client, message)
        })
    }

    /// Whether this client may hold this subscription.
    pub fn authorize_subscribe(&self, client: &ClientInfo<'_>, filter: &str, qos: QoS) -> bool {
        self.chain(self.defaults.subscribe, |hook| {
            hook.authorize_subscribe(client, filter, qos)
        })
    }

    /// Offer the message to every hook in turn, and say whether it
    /// survived.
    ///
    /// Every hook sees the message as the hook before it left it, so a
    /// chain of rewrites composes. A hook that drops it ends the walk:
    /// the hooks after it are not asked about a message that is not
    /// going anywhere.
    pub fn message_received(&self, client: &ClientInfo<'_>, message: &mut Message) -> Disposition {
        for hook in &self.installed {
            if hook.message_received(client, message) == Disposition::Drop {
                return Disposition::Drop;
            }
        }
        Disposition::Deliver
    }

    /// Tell every hook a publish is on its way to a subscriber.
    pub fn message_delivered(&self, to: &ClientInfo<'_>, publish: &Publish) {
        for hook in &self.installed {
            hook.message_delivered(to, publish);
        }
    }

    /// Tell every hook a message was discarded.
    pub fn message_dropped(&self, to: &ClientInfo<'_>, topic: &str, qos: QoS, reason: DropReason) {
        for hook in &self.installed {
            hook.message_dropped(to, topic, qos, reason);
        }
    }

    /// Tell every hook a connection was accepted.
    pub fn client_connected(&self, client: &ClientInfo<'_>) {
        for hook in &self.installed {
            hook.client_connected(client);
        }
    }

    /// Tell every hook a connection went away.
    pub fn client_disconnected(&self, client: &ClientInfo<'_>) {
        for hook in &self.installed {
            hook.client_disconnected(client);
        }
    }

    /// Tell every hook a session was destroyed.
    pub fn session_expired(&self, client: &ClientInfo<'_>) {
        for hook in &self.installed {
            hook.session_expired(client);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;

    use super::*;

    /// A hook that answers the authorizing points with one canned
    /// decision and counts how often it was asked.
    struct Canned {
        decision: Decision,
        asked: AtomicUsize,
    }

    impl Canned {
        fn new(decision: Decision) -> Arc<Self> {
            Arc::new(Self {
                decision,
                asked: AtomicUsize::new(0),
            })
        }

        fn asked(&self) -> usize {
            self.asked.load(Ordering::Relaxed)
        }
    }

    impl Hook for Canned {
        fn name(&self) -> &str {
            "canned"
        }

        fn authenticate(&self, _credentials: &Credentials<'_>) -> Decision {
            self.asked.fetch_add(1, Ordering::Relaxed);
            self.decision
        }
    }

    fn credentials() -> Credentials<'static> {
        Credentials {
            mountpoint: "",
            client_id: "c",
            username: None,
            password: None,
            version: Version::V5,
            clean_start: true,
        }
    }

    fn message() -> Message {
        Message::new("a/b", Bytes::from_static(b"x"), QoS::AtMostOnce, false)
    }

    #[test]
    fn an_empty_chain_is_the_default() {
        let mut hooks = Hooks::new();
        assert!(hooks.authenticate(&credentials()));

        hooks.set_defaults(Defaults {
            authenticate: false,
            ..Defaults::default()
        });
        assert!(!hooks.authenticate(&credentials()));
    }

    #[test]
    fn a_chain_of_nexts_falls_through_to_the_default() {
        let first = Canned::new(Decision::Next);
        let second = Canned::new(Decision::Next);
        let mut hooks = Hooks::new();
        hooks.install(first.clone());
        hooks.install(second.clone());

        assert!(hooks.authenticate(&credentials()));
        hooks.set_defaults(Defaults {
            authenticate: false,
            ..Defaults::default()
        });
        assert!(!hooks.authenticate(&credentials()));

        assert_eq!(first.asked(), 2, "both calls should reach the first hook");
        assert_eq!(second.asked(), 2, "and pass through it to the second");
    }

    #[test]
    fn a_deny_stops_the_chain() {
        let denier = Canned::new(Decision::Deny);
        let after = Canned::new(Decision::Allow);
        let mut hooks = Hooks::new();
        hooks.install(denier.clone());
        hooks.install(after.clone());

        assert!(!hooks.authenticate(&credentials()));
        assert_eq!(after.asked(), 0, "nobody after a decision is asked");
    }

    #[test]
    fn an_allow_stops_the_chain() {
        let allower = Canned::new(Decision::Allow);
        let after = Canned::new(Decision::Deny);
        let mut hooks = Hooks::new();
        hooks.install(allower.clone());
        hooks.install(after.clone());

        // Against a default of deny, so that the allow is what
        // produced the answer rather than the fall-through.
        hooks.set_defaults(Defaults {
            authenticate: false,
            ..Defaults::default()
        });
        assert!(hooks.authenticate(&credentials()));
        assert_eq!(after.asked(), 0, "nobody after a decision is asked");
    }

    /// A hook that appends to the topic, so a chain of them composes
    /// visibly.
    struct Rewriter(&'static str);

    impl Hook for Rewriter {
        fn name(&self) -> &str {
            "rewriter"
        }

        fn message_received(&self, _client: &ClientInfo<'_>, message: &mut Message) -> Disposition {
            message.topic = Arc::from(format!("{}/{}", message.topic, self.0).as_str());
            Disposition::Deliver
        }
    }

    struct Dropper;

    impl Hook for Dropper {
        fn name(&self) -> &str {
            "dropper"
        }

        fn message_received(
            &self,
            _client: &ClientInfo<'_>,
            _message: &mut Message,
        ) -> Disposition {
            Disposition::Drop
        }
    }

    fn client() -> ClientInfo<'static> {
        ClientInfo {
            subscriber: SubscriberId(1),
            mountpoint: "",
            client_id: "c",
        }
    }

    #[test]
    fn rewrites_compose_in_order() {
        let mut hooks = Hooks::new();
        hooks.install(Arc::new(Rewriter("one")));
        hooks.install(Arc::new(Rewriter("two")));

        let mut message = message();
        assert_eq!(
            hooks.message_received(&client(), &mut message),
            Disposition::Deliver
        );
        assert_eq!(&*message.topic, "a/b/one/two");
    }

    #[test]
    fn a_drop_stops_the_walk() {
        let mut hooks = Hooks::new();
        hooks.install(Arc::new(Dropper));
        hooks.install(Arc::new(Rewriter("never")));

        let mut message = message();
        assert_eq!(
            hooks.message_received(&client(), &mut message),
            Disposition::Drop
        );
        assert_eq!(
            &*message.topic, "a/b",
            "a hook after the drop should not have been asked"
        );
    }
}
