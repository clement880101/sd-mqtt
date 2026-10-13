//! Who is in this cluster, and which of them are answering.
//!
//! Section 8.1's failure detection, and nothing else: no sockets, no
//! tasks and no clock of its own. Everything here is a function of
//! what was last heard from each peer and what time it is now, which
//! is what lets the whole of it be tested without a link or a sleep.
//!
//! # Two signals, and they are not the same signal
//!
//! - **A closed socket is immediate.** Something at the other end went
//!   away and said so, or the connection failed; either way this node
//!   knows now rather than in ten seconds. [`Membership::gone`].
//! - **Silence over an open socket is the slow one**, and it is the
//!   one the heartbeat exists for. A peer that is suspect has missed
//!   beats; a peer that is dead has been silent past the window.
//!   [`Membership::age`].
//!
//! A half-open link produces only the second, which is why a test that
//! severs a connection by closing it finds nothing: it reaches the
//! signal that was never in doubt.
//!
//! # A member is not forgotten when it dies
//!
//! A dead peer keeps its row, with the address it advertised. Two
//! reasons, and the second is the one that matters: an operator
//! looking at a node wants to see the peer that has gone rather than a
//! shorter list, and **the advertised address is how this node dials
//! it again**. A peer that was never a seed and is dropped on death is
//! a peer only the other end can restore.
//!
//! What this milestone does not have is a way to say a node has left
//! for good. Section 8.5's departure is a later plan's; until then a
//! member is forgotten when this process restarts and not before, and
//! that is a deferral rather than an omission.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::identity::{NodeId, NodeIdentity};

/// Section 8.1's heartbeat interval when nothing says otherwise.
pub const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(1);

/// How many beats may be missed before a peer is suspect.
pub const DEFAULT_SUSPECT_AFTER: u32 = 3;

/// How long a peer may be silent before it is dead.
pub const DEFAULT_DEAD_AFTER: Duration = Duration::from_secs(10);

/// How a peer looks from here.
///
/// Ordered deliberately: worse is greater, so the worst state in a
/// view is its maximum and nothing has to remember which way round the
/// comparison goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MemberState {
    /// Heard from within the window.
    Alive,
    /// Has missed beats, and is not yet past the window.
    ///
    /// Its own state rather than a shade of alive, because section
    /// 8.5's answer to a partition depends on telling "quiet" from
    /// "gone", and a node that only had the two would have to decide
    /// on the strength of the first missed beat.
    Suspect,
    /// Silent past the window, or its connection has gone.
    Dead,
}

impl MemberState {
    /// The word an operator reads, and the admin API prints.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Alive => "up",
            Self::Suspect => "suspect",
            Self::Dead => "down",
        }
    }
}

/// One peer, as this node sees it.
#[derive(Debug, Clone)]
pub struct Member {
    /// Who it is, and what it is called.
    pub identity: NodeIdentity,
    /// The address it says peers should dial.
    ///
    /// **Not the address this node connected from.** Section 8.1: a
    /// node bound to all interfaces, behind a port mapping, or behind
    /// a test's gate has a bind address no peer can use.
    pub advertised: String,
    /// How it looks from here.
    pub state: MemberState,
    /// When anything was last heard from it.
    pub last_heard: Instant,
}

/// The windows failure detection uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Detection {
    /// How often a beat is sent.
    pub heartbeat: Duration,
    /// How many beats may be missed before a peer is suspect.
    pub suspect_after: u32,
    /// How long a peer may be silent before it is dead.
    pub dead_after: Duration,
}

impl Default for Detection {
    fn default() -> Self {
        Self {
            heartbeat: DEFAULT_HEARTBEAT,
            suspect_after: DEFAULT_SUSPECT_AFTER,
            dead_after: DEFAULT_DEAD_AFTER,
        }
    }
}

impl Detection {
    /// How long a peer may be silent before it is suspect.
    #[must_use]
    pub fn suspect_window(&self) -> Duration {
        self.heartbeat * self.suspect_after
    }

    /// What a peer silent this long is.
    #[must_use]
    pub fn verdict(&self, silence: Duration) -> MemberState {
        if silence >= self.dead_after {
            MemberState::Dead
        } else if silence >= self.suspect_window() {
            MemberState::Suspect
        } else {
            MemberState::Alive
        }
    }
}

/// This node's view of the cluster.
///
/// Keyed by [`NodeId`] and ordered by it, which is the ordering
/// section 8.1 says everything orders by: a listing is stable across
/// nodes and across restarts without anybody sorting it.
#[derive(Debug)]
pub struct Membership {
    detection: Detection,
    members: BTreeMap<NodeId, Member>,
}

impl Membership {
    /// An empty view, detecting failure by these windows.
    #[must_use]
    pub fn new(detection: Detection) -> Self {
        Self {
            detection,
            members: BTreeMap::new(),
        }
    }

    /// The windows this view uses.
    #[must_use]
    pub fn detection(&self) -> Detection {
        self.detection
    }

    /// Something arrived from this peer.
    ///
    /// Any frame, not only a beat: a link carrying traffic is a link
    /// whose peer is answering, and a heartbeat is what covers the
    /// links that are not.
    ///
    /// **Alive again immediately.** A peer that was dead and has
    /// spoken is alive, on the same link, without waiting for
    /// anything: that is what makes a partition that heals a partition
    /// that heals.
    ///
    /// Returns the state when this call **changed** it, and `None` when
    /// a peer that was already alive said something else. The
    /// difference matters to whoever keeps section 8.3's routing map in
    /// step: this is called once per inbound frame, and a map told
    /// every time would take a lock on the publish path for a fact it
    /// already held. [`Membership::age`] reports a change for the same
    /// reason — a line per second per peer is how a log stops being
    /// read — and nothing else here reports one, so a peer that was
    /// dead and spoke again had nobody to tell until this said so.
    pub fn heard(
        &mut self,
        identity: &NodeIdentity,
        advertised: &str,
        now: Instant,
    ) -> Option<MemberState> {
        let was = self.members.get(&identity.id()).map(|member| member.state);
        let member = self.members.entry(identity.id()).or_insert_with(|| Member {
            identity: identity.clone(),
            advertised: advertised.to_owned(),
            state: MemberState::Alive,
            last_heard: now,
        });
        // Taken afresh every time, because an operator may have
        // renamed the peer or moved it behind a different address, and
        // the greeting is where it says so.
        member.identity = identity.clone();
        member.advertised = advertised.to_owned();
        member.state = MemberState::Alive;
        member.last_heard = now;
        (was != Some(MemberState::Alive)).then_some(MemberState::Alive)
    }

    /// This peer's connection has gone.
    ///
    /// Dead at once rather than after the window. The window exists
    /// for silence over a connection that is still there; a connection
    /// that is not is not something to wait about.
    pub fn gone(&mut self, id: NodeId) {
        if let Some(member) = self.members.get_mut(&id) {
            member.state = MemberState::Dead;
        }
    }

    /// Take `id` out of the table altogether.
    ///
    /// **The first thing in this design that makes membership smaller.**
    /// Until it existed a peer once met was remembered as `down` for
    /// ever, which is the blind spot section 8.5's expected size works
    /// around and the hole section 8.6's repair cannot fill: a replica
    /// set is repaired *away* from a node, and a node that is never
    /// forgotten is never repaired away from.
    ///
    /// A forgotten peer that comes back is re-admitted by
    /// [`Membership::heard`], which inserts what it does not hold.
    /// Returns whether there was anything to forget.
    pub fn forget(&mut self, id: NodeId) -> bool {
        self.members.remove(&id).is_some()
    }

    /// How long `id` has been silent, as of `now`.
    ///
    /// `None` for a peer this node has not met. What section 8.6's
    /// grace is measured against: a peer dead for longer than it is
    /// treated as gone.
    #[must_use]
    pub fn silent_for(&self, id: NodeId, now: Instant) -> Option<Duration> {
        self.members
            .get(&id)
            .map(|member| now.saturating_duration_since(member.last_heard))
    }

    /// Recompute every peer's state against the clock.
    ///
    /// Called on every beat. Returns the peers whose state changed, so
    /// that a caller can say so once rather than every tick — a line
    /// per second per peer is how a log stops being read.
    pub fn age(&mut self, now: Instant) -> Vec<(NodeIdentity, MemberState)> {
        let mut changed = Vec::new();
        for member in self.members.values_mut() {
            // **Death is absorbing until something is heard.** A peer
            // whose socket has gone was heard from a moment ago, so
            // the window below says alive; taking that answer would
            // bring a link that failed back to life on the very next
            // beat and leave it there. Only [`Membership::heard`]
            // undoes a death, which is the same rule for both signals.
            let verdict = if member.state == MemberState::Dead {
                MemberState::Dead
            } else {
                self.detection
                    .verdict(now.saturating_duration_since(member.last_heard))
            };
            if verdict != member.state {
                member.state = verdict;
                changed.push((member.identity.clone(), verdict));
            }
        }
        changed
    }

    /// One peer, if this node has met it.
    #[must_use]
    pub fn member(&self, id: NodeId) -> Option<&Member> {
        self.members.get(&id)
    }

    /// Every peer, in identity order.
    pub fn members(&self) -> impl Iterator<Item = &Member> {
        self.members.values()
    }

    /// How many peers this node has met.
    #[must_use]
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Whether it has met any.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// How many of them are answering.
    #[must_use]
    pub fn alive(&self) -> usize {
        self.members
            .values()
            .filter(|member| member.state == MemberState::Alive)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn detection() -> Detection {
        Detection {
            heartbeat: Duration::from_secs(1),
            suspect_after: 3,
            dead_after: Duration::from_secs(10),
        }
    }

    fn peer(name: &str) -> NodeIdentity {
        NodeIdentity::mint(name)
    }

    /// The three states are three windows, at the boundaries section
    /// 8.1 names.
    ///
    /// The boundaries themselves, not a value comfortably inside each:
    /// a comparison written `>` where it should be `>=` moves every
    /// one of them by an instant and nothing in the middle of a window
    /// can see it.
    #[test]
    fn silence_becomes_suspicion_and_then_death_at_the_configured_windows() {
        let detection = detection();
        assert_eq!(
            detection.verdict(Duration::from_millis(2999)),
            MemberState::Alive
        );
        assert_eq!(
            detection.verdict(Duration::from_secs(3)),
            MemberState::Suspect,
            "three missed beats is suspect"
        );
        assert_eq!(
            detection.verdict(Duration::from_millis(9999)),
            MemberState::Suspect
        );
        assert_eq!(
            detection.verdict(Duration::from_secs(10)),
            MemberState::Dead,
            "ten seconds is dead"
        );
    }

    /// A peer heard from is alive, and ages into suspicion and death.
    #[test]
    fn a_peer_ages_from_alive_through_suspect_to_dead() {
        let mut view = Membership::new(detection());
        let start = Instant::now();
        let b = peer("b");
        view.heard(&b, "10.0.0.2:7700", start);
        assert_eq!(view.member(b.id()).expect("met").state, MemberState::Alive);
        assert_eq!(view.alive(), 1);

        assert!(view.age(start + Duration::from_secs(1)).is_empty());
        assert_eq!(view.member(b.id()).expect("met").state, MemberState::Alive);

        let changed = view.age(start + Duration::from_secs(4));
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].1, MemberState::Suspect);
        assert_eq!(view.alive(), 0);

        let changed = view.age(start + Duration::from_secs(11));
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].1, MemberState::Dead);
        assert_eq!(view.member(b.id()).expect("met").state, MemberState::Dead);
    }

    /// A state that has not changed is not reported again.
    ///
    /// The whole point of returning what changed: a beat a second
    /// against a dead peer would otherwise be a log line a second.
    #[test]
    fn an_unchanged_state_is_not_reported_twice() {
        let mut view = Membership::new(detection());
        let start = Instant::now();
        view.heard(&peer("b"), "10.0.0.2:7700", start);
        assert_eq!(view.age(start + Duration::from_secs(11)).len(), 1);
        assert!(
            view.age(start + Duration::from_secs(12)).is_empty(),
            "a peer that was already dead has not changed"
        );
    }

    /// A dead peer that speaks is alive again, without waiting.
    ///
    /// This is healing, and it is the assertion a partition test
    /// leans on.
    #[test]
    fn a_dead_peer_that_speaks_is_alive_again_at_once() {
        let mut view = Membership::new(detection());
        let start = Instant::now();
        let b = peer("b");
        view.heard(&b, "10.0.0.2:7700", start);
        view.age(start + Duration::from_secs(11));
        assert_eq!(view.member(b.id()).expect("met").state, MemberState::Dead);

        view.heard(&b, "10.0.0.2:7700", start + Duration::from_secs(12));
        assert_eq!(view.member(b.id()).expect("met").state, MemberState::Alive);
        assert_eq!(view.alive(), 1);
    }

    /// A closed socket is dead now, not in ten seconds.
    #[test]
    fn a_closed_connection_is_dead_immediately() {
        let mut view = Membership::new(detection());
        let start = Instant::now();
        let b = peer("b");
        view.heard(&b, "10.0.0.2:7700", start);
        view.gone(b.id());
        assert_eq!(
            view.member(b.id()).expect("met").state,
            MemberState::Dead,
            "a closed socket does not wait for the window"
        );
        assert_eq!(view.alive(), 0);
    }

    /// And ageing does not resurrect it.
    ///
    /// A peer whose connection has gone was heard from a moment ago,
    /// so the window says alive. If the recomputation took that
    /// answer, a link that failed would come back to life on the very
    /// next beat and stay there.
    #[test]
    fn ageing_does_not_bring_a_closed_connection_back_to_life() {
        let mut view = Membership::new(detection());
        let start = Instant::now();
        let b = peer("b");
        view.heard(&b, "10.0.0.2:7700", start);
        view.gone(b.id());
        assert!(view.age(start + Duration::from_millis(1)).is_empty());
        assert_eq!(view.member(b.id()).expect("met").state, MemberState::Dead);
    }

    /// A peer keeps its row when it dies, with the address to dial.
    #[test]
    fn a_dead_peer_keeps_the_address_it_advertised() {
        let mut view = Membership::new(detection());
        let start = Instant::now();
        let b = peer("b");
        view.heard(&b, "10.0.0.2:7700", start);
        view.gone(b.id());
        assert_eq!(view.len(), 1);
        assert_eq!(
            view.member(b.id()).expect("met").advertised,
            "10.0.0.2:7700"
        );
    }

    /// A rename and a move are taken from the greeting.
    #[test]
    fn a_greeting_updates_the_name_and_the_advertised_address() {
        let mut view = Membership::new(detection());
        let start = Instant::now();
        let b = peer("edge-1");
        view.heard(&b, "10.0.0.2:7700", start);
        let renamed = NodeIdentity::new(b.id(), "edge-2");
        view.heard(&renamed, "10.0.0.9:7700", start);
        let member = view.member(b.id()).expect("met");
        assert_eq!(member.identity.name(), "edge-2");
        assert_eq!(member.advertised, "10.0.0.9:7700");
        assert_eq!(view.len(), 1, "a rename is not a second node");
    }

    /// Members are listed in identity order, whatever order they
    /// arrived in.
    #[test]
    fn members_are_listed_in_identity_order() {
        let mut view = Membership::new(detection());
        let start = Instant::now();
        let mut minted: Vec<NodeIdentity> = (0..5).map(|at| peer(&format!("n{at}"))).collect();
        for identity in minted.iter().rev() {
            view.heard(identity, "10.0.0.1:7700", start);
        }
        minted.sort_by_key(NodeIdentity::id);
        let listed: Vec<NodeId> = view.members().map(|member| member.identity.id()).collect();
        let expected: Vec<NodeId> = minted.iter().map(NodeIdentity::id).collect();
        assert_eq!(listed, expected);
    }

    /// The words the admin API prints.
    #[test]
    fn a_state_has_a_word_an_operator_reads() {
        assert_eq!(MemberState::Alive.label(), "up");
        assert_eq!(MemberState::Suspect.label(), "suspect");
        assert_eq!(MemberState::Dead.label(), "down");
    }

    /// Forgetting is the first thing that makes the table smaller, and
    /// a forgotten peer that speaks again is a peer met for the first
    /// time.
    #[test]
    fn a_forgotten_peer_is_gone_and_is_re_admitted_when_heard() {
        let mut membership = Membership::new(detection());
        let peer = NodeIdentity::new(NodeId::from_bytes([7; 16]), "beta");
        let now = Instant::now();
        membership.heard(&peer, "127.0.0.1:7701", now);
        assert_eq!(membership.len(), 1);
        assert_eq!(membership.silent_for(peer.id(), now), Some(Duration::ZERO));

        assert!(
            membership.forget(peer.id()),
            "there was something to forget"
        );
        assert_eq!(
            membership.len(),
            0,
            "and it is gone from the table, not marked"
        );
        assert_eq!(
            membership.silent_for(peer.id(), now),
            None,
            "a peer not in the table has no silence to measure"
        );
        assert!(
            !membership.forget(peer.id()),
            "forgetting twice is not an error"
        );

        // It speaks again, and it is back: forgetting is about this
        // node's table, not the other node's right to exist.
        membership.heard(&peer, "127.0.0.1:7701", now);
        assert_eq!(membership.len(), 1);
        assert_eq!(
            membership.member(peer.id()).map(|member| member.state),
            Some(MemberState::Alive)
        );
    }
}
