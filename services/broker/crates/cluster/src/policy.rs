//! What a node does while the cluster is not whole.
//!
//! Section 8.5: the default is availability, so a partitioned node keeps
//! serving its clients. Per-operation flags let an operator refuse
//! connects, publishes or subscribes instead, for deployments that would
//! rather lose availability than accept a write the other side cannot
//! see.
//!
//! # "Whole" is a number an operator writes down
//!
//! It cannot be anything else. Membership is learnt from seeds, so a node
//! that has never heard of a fourth node cannot tell a cluster of three
//! from a cluster of four with one down, and a node decommissioned
//! yesterday would otherwise be a hole for ever. Section 8.5 says this
//! outright and the health endpoint has served an expected count since
//! milestone 3 with nothing behind it; this is what supplies it.
//!
//! # A strict flag with no expected size is refused, by the type
//!
//! Not by a check in the configuration loader. A flag whose condition
//! can never be evaluated is a flag that silently means nothing, which
//! is the reinterpretation of a value that cannot mean what it says that
//! this project already refuses for an inflight window of zero and an
//! empty secret. [`Policy::new`] is the only way to build one and it
//! returns an error, so the case does not exist further in.

use std::sync::Arc;

use serde::Deserialize;
use thiserror::Error;

use crate::mesh::Mesh;

/// Which operations an operator asked to refuse while the cluster is
/// short of its expected size.
///
/// All false is the default, which is section 8.5's availability.
///
/// `Deserialize` here rather than a mirror of three booleans in
/// `sdmqtt::config`, so that the names an operator writes are declared
/// once, in the module that says what they mean.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Strict {
    /// Refuse a client connecting.
    pub connects: bool,
    /// Refuse a publication.
    pub publishes: bool,
    /// Refuse a subscription.
    pub subscribes: bool,
    /// Refuse a durable publish that no follower confirmed. Section
    /// 8.6.
    ///
    /// **Not about the cluster being whole**, which is what the three
    /// above are about, and it is in this struct anyway because it is
    /// the same kind of thing: an operator choosing to lose
    /// availability rather than accept something weaker than they
    /// asked for. Without it a synchronous publish with no reachable
    /// follower is acknowledged and counted, which is section 8.6's
    /// default and section 8.5's trade.
    ///
    /// It needs no expected size, because what it is evaluated against
    /// is whether a follower answered — a fact this node has to hand.
    pub unconfirmed: bool,
}

impl Strict {
    /// The first flag set, named as an operator writes it.
    ///
    /// **One list of the three names**, in the order they are written
    /// so that the first complaint is about the first mistake on the
    /// page. [`Policy::new`]'s refusal and `sdmqtt::config`'s are both
    /// about naming a flag an operator has to go and fix, and two
    /// lists of three string literals is the way one of them ends up
    /// naming a key that no longer exists.
    #[must_use]
    pub fn first_set(self) -> Option<&'static str> {
        [
            (self.connects, "cluster.strict.connects"),
            (self.publishes, "cluster.strict.publishes"),
            (self.subscribes, "cluster.strict.subscribes"),
            // **Not in `Policy::new`'s refusal below**, because this
            // one is evaluable without an expected size: see the field.
        ]
        .into_iter()
        .find_map(|(set, name)| set.then_some(name))
    }

    /// Whether any operation is to be refused.
    #[must_use]
    pub fn any(self) -> bool {
        self.first_set().is_some()
    }
}

/// What a client asked to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// A CONNECT.
    Connect,
    /// A PUBLISH.
    Publish,
    /// A SUBSCRIBE.
    Subscribe,
}

/// Why a policy could not be built.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PolicyError {
    /// An operation was to be refused while the cluster is not whole,
    /// and nothing says how large whole is.
    #[error(
        "refusing to start: {what} is set to refuse while the cluster is not whole, and \
         cluster.expected_size is unset, so nothing can say whether it is. Set \
         cluster.expected_size to the number of nodes this cluster is meant to have, or \
         unset the flag and serve through a partition."
    )]
    NoExpectedSize {
        /// The flag that cannot be evaluated, named as it is written.
        what: &'static str,
    },
    /// An expected size of zero, which no cluster has.
    #[error(
        "refusing to start: cluster.expected_size is 0, and a cluster contains at least \
         the node reading this. Write the number of nodes it is meant to have."
    )]
    NoNodes,
}

/// The rule, and the number it is evaluated against.
#[derive(Debug, Clone, Copy)]
pub struct Policy {
    expected: Option<usize>,
    strict: Strict,
}

impl Default for Policy {
    /// Availability, with no expected size: serve everything.
    fn default() -> Self {
        Self {
            expected: None,
            strict: Strict::default(),
        }
    }
}

impl Policy {
    /// Build a policy, refusing a flag whose condition cannot be read.
    ///
    /// The error names the flag rather than saying one of them is set,
    /// because an operator with three flags and one mistake should not
    /// have to find out which.
    pub fn new(expected: Option<usize>, strict: Strict) -> Result<Self, PolicyError> {
        if expected == Some(0) {
            return Err(PolicyError::NoNodes);
        }
        if expected.is_none() {
            if let Some(what) = strict.first_set() {
                return Err(PolicyError::NoExpectedSize { what });
            }
        }
        Ok(Self { expected, strict })
    }

    /// The size an operator says this cluster has, if they said.
    #[must_use]
    pub fn expected(&self) -> Option<usize> {
        self.expected
    }

    /// Whether `live` nodes is the whole cluster.
    ///
    /// With no expected size nothing is ever short, which is what makes
    /// the default serve: there is no number to fall below.
    #[must_use]
    pub fn whole(&self, live: usize) -> bool {
        self.expected.is_none_or(|expected| live >= expected)
    }

    /// Whether this operation may proceed with `live` nodes reachable.
    ///
    /// A flag refuses **only** the operation it names. One flag refusing
    /// all three would make the three flags one flag, which is the
    /// mutation that spelling exists to fail.
    #[must_use]
    pub fn allows(&self, operation: Operation, live: usize) -> bool {
        if self.whole(live) {
            return true;
        }
        match operation {
            Operation::Connect => !self.strict.connects,
            Operation::Publish => !self.strict.publishes,
            Operation::Subscribe => !self.strict.subscribes,
        }
    }
}

/// How many nodes are answering, to whoever is counting them.
///
/// A trait, for the reason `admin::Cluster` is one: what a policy needs
/// of a mesh is a single number, and a [`Gate`] built on the mesh type
/// itself could not be tested without a transport, a handshake and a
/// failure detector behind it.
pub trait Live: Send + Sync + 'static {
    /// The nodes this one can see, **itself included**.
    ///
    /// Itself included because a node that has lost every peer is a
    /// cluster of one and not of none, and because the expected size an
    /// operator writes down counts every node in the cluster. The two
    /// numbers compared have to be counting the same thing.
    fn live(&self) -> usize;
}

impl Live for Mesh {
    /// The peers answering, and this node.
    ///
    /// `Mesh::alive()` is peers alone — a membership view does not hold
    /// the node holding it — so the one here is this node, which is
    /// always answering to itself.
    fn live(&self) -> usize {
        self.alive() + 1
    }
}

/// A policy and the thing it reads, which is what a caller holds.
///
/// Separate from [`Policy`] so that the rule stays a function of two
/// numbers and can be tested as one. This is the pairing, and all it
/// adds is where the second number comes from.
#[derive(Clone)]
pub struct Gate {
    policy: Policy,
    live: Arc<dyn Live>,
}

impl std::fmt::Debug for Gate {
    /// The policy and the reading, without naming the thing read.
    ///
    /// `dyn Live` is not `Debug` and should not have to be: a mesh
    /// printing itself into a connection log is section 12.3's
    /// problem, not this type's.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gate")
            .field("policy", &self.policy)
            .field("live", &self.live.live())
            .finish()
    }
}

impl Gate {
    /// Pair a policy with the nodes it counts.
    #[must_use]
    pub fn new(policy: Policy, live: Arc<dyn Live>) -> Arc<Self> {
        Arc::new(Self { policy, live })
    }

    /// Whether this operation may proceed right now.
    #[must_use]
    pub fn allows(&self, operation: Operation) -> bool {
        self.policy.allows(operation, self.live.live())
    }

    /// Whether the cluster is at its expected size right now.
    #[must_use]
    pub fn whole(&self) -> bool {
        self.policy.whole(self.live.live())
    }

    /// The size an operator says this cluster has, if they said.
    ///
    /// What the health endpoint serves as `expected`. It is here rather
    /// than read from a configuration a handler happens to hold,
    /// because the number that decides a refusal and the number an
    /// operator reads off a probe must be the same number.
    #[must_use]
    pub fn expected(&self) -> Option<usize> {
        self.policy.expected()
    }

    /// The nodes answering, this one included.
    #[must_use]
    pub fn live(&self) -> usize {
        self.live.live()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strict_everything() -> Strict {
        Strict {
            connects: true,
            publishes: true,
            subscribes: true,
            unconfirmed: true,
        }
    }

    #[test]
    fn the_default_serves_everything_because_nothing_says_how_large_whole_is() {
        let policy = Policy::default();
        assert!(
            policy.whole(1),
            "one node is not short of a number nobody set"
        );
        for operation in [Operation::Connect, Operation::Publish, Operation::Subscribe] {
            assert!(
                policy.allows(operation, 1),
                "section 8.5's default is availability: {operation:?}"
            );
        }
    }

    #[test]
    fn a_cluster_at_its_expected_size_is_whole_and_one_short_is_not() {
        let policy = Policy::new(Some(3), strict_everything()).expect("three nodes and flags");
        assert!(!policy.whole(2), "two of three is short");
        assert!(policy.whole(3), "three of three is whole");
        assert!(
            policy.whole(4),
            "more than expected is not short; a fourth node joining is not an outage"
        );
    }

    #[test]
    fn a_flag_refuses_the_operation_it_names_and_not_the_others() {
        // The mutation this exists for is one flag refusing all three,
        // which would make the three flags one flag and which every
        // test using a single flag would pass.
        let only_publishes = Policy::new(
            Some(3),
            Strict {
                publishes: true,
                ..Strict::default()
            },
        )
        .expect("one flag");
        assert!(
            !only_publishes.allows(Operation::Publish, 2),
            "the flag that was set refuses"
        );
        assert!(
            only_publishes.allows(Operation::Connect, 2),
            "and the two that were not do not"
        );
        assert!(only_publishes.allows(Operation::Subscribe, 2));
    }

    #[test]
    fn every_flag_refuses_its_own_operation() {
        // The mirror of the test above: one flag tested is also passed
        // by a policy that refuses nothing.
        for (strict, refused) in [
            (
                Strict {
                    connects: true,
                    ..Strict::default()
                },
                Operation::Connect,
            ),
            (
                Strict {
                    publishes: true,
                    ..Strict::default()
                },
                Operation::Publish,
            ),
            (
                Strict {
                    subscribes: true,
                    ..Strict::default()
                },
                Operation::Subscribe,
            ),
        ] {
            let policy = Policy::new(Some(2), strict).expect("a flag and a size");
            assert!(
                !policy.allows(refused, 1),
                "{refused:?} is refused while the cluster is short"
            );
        }
    }

    #[test]
    fn a_strict_flag_refuses_nothing_while_the_cluster_is_whole() {
        let policy = Policy::new(Some(3), strict_everything()).expect("three nodes and flags");
        for operation in [Operation::Connect, Operation::Publish, Operation::Subscribe] {
            assert!(
                policy.allows(operation, 3),
                "a whole cluster serves whatever the flags say: {operation:?}"
            );
        }
    }

    #[test]
    fn a_flag_with_no_expected_size_is_refused_and_the_error_names_it() {
        for (strict, named) in [
            (
                Strict {
                    connects: true,
                    ..Strict::default()
                },
                "cluster.strict.connects",
            ),
            (
                Strict {
                    publishes: true,
                    ..Strict::default()
                },
                "cluster.strict.publishes",
            ),
            (
                Strict {
                    subscribes: true,
                    ..Strict::default()
                },
                "cluster.strict.subscribes",
            ),
        ] {
            let error = Policy::new(None, strict).expect_err("a flag nothing can evaluate");
            assert_eq!(
                error,
                PolicyError::NoExpectedSize { what: named },
                "the refusal names the flag an operator has to go and fix"
            );
        }
    }

    /// The one flag that needs no expected size.
    ///
    /// The other three are about whether the cluster is whole, which
    /// nothing but a configured size can say. This one is about
    /// whether a follower answered — a fact this node has to hand —
    /// so refusing it for want of a number it does not use would
    /// refuse a configuration that works.
    #[test]
    fn refusing_an_unconfirmed_publish_needs_no_expected_size() {
        let policy = Policy::new(
            None,
            Strict {
                unconfirmed: true,
                ..Strict::default()
            },
        )
        .expect("this flag is evaluable without a cluster size");
        assert_eq!(policy.expected(), None);
        // And it does not make the other three fire: a node with no
        // expected size is never short, so nothing else is refused.
        for operation in [Operation::Connect, Operation::Publish, Operation::Subscribe] {
            assert!(policy.allows(operation, 1));
        }
    }

    #[test]
    fn no_flags_and_no_expected_size_is_the_ordinary_configuration() {
        // The other side of the boundary: a guard that refused an absent
        // size whatever the flags said would refuse every single-node
        // broker, which is most of them.
        Policy::new(None, Strict::default()).expect("availability needs no number");
    }

    #[test]
    fn an_expected_size_of_zero_is_refused() {
        // A cluster contains at least the node reading the setting, so
        // zero is a number that cannot mean what it says. Refused
        // rather than read as "unset", which is the reinterpretation
        // this project refuses elsewhere.
        assert_eq!(
            Policy::new(Some(0), Strict::default()).expect_err("no cluster has no nodes"),
            PolicyError::NoNodes
        );
    }

    #[test]
    fn an_expected_size_of_one_is_whole_on_its_own() {
        // A single-node cluster that an operator has written down. It
        // must not be permanently short of itself.
        let policy = Policy::new(Some(1), strict_everything()).expect("one node");
        assert!(policy.whole(1));
        assert!(policy.allows(Operation::Publish, 1));
    }

    /// A count a test sets by hand.
    struct Counted(std::sync::atomic::AtomicUsize);

    impl Live for Counted {
        fn live(&self) -> usize {
            self.0.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    #[test]
    fn a_gate_re_reads_the_count_rather_than_remembering_it() {
        // A gate that read the membership once at startup would be a
        // partition policy that never notices a partition.
        let counted = Arc::new(Counted(std::sync::atomic::AtomicUsize::new(3)));
        let gate = Gate::new(
            Policy::new(Some(3), strict_everything()).expect("three nodes and flags"),
            Arc::clone(&counted) as Arc<dyn Live>,
        );
        assert!(gate.whole());
        assert!(gate.allows(Operation::Publish));
        assert_eq!(gate.live(), 3);

        counted.0.store(2, std::sync::atomic::Ordering::Relaxed);
        assert!(!gate.whole(), "a peer went and the gate noticed");
        assert!(!gate.allows(Operation::Publish));
        assert_eq!(gate.live(), 2);
        assert_eq!(gate.expected(), Some(3));
    }

    #[test]
    fn a_gate_does_not_print_what_it_counts() {
        // Section 12.3. `Debug` is hand-written here, and a derive
        // would not compile — but a future one over a `Live` that is
        // `Debug` would, and would print a mesh into a connection log.
        let gate = Gate::new(
            Policy::default(),
            Arc::new(Counted(std::sync::atomic::AtomicUsize::new(1))),
        );
        let shown = format!("{gate:?}");
        assert!(shown.contains("live: 1"), "{shown}");
        assert!(!shown.contains("Counted"), "{shown}");
    }
}
