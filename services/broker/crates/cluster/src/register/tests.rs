//! Tests for the register and the merge.
//!
//! # The writes are issued by real clocks, deliberately
//!
//! [`convergence`] does not construct stamps. Every stamp here comes out
//! of a [`HybridClock`] belonging to a node with its own identity,
//! because **convergence is conditional on no two writes ever sharing a
//! stamp** and a constructed stamp can duplicate. A generator that built
//! stamps by hand would eventually produce two writes carrying one stamp
//! with different values, and the property would then fail for a reason
//! that is not a defect in anything this module does — which is exactly
//! how the condition was found in the first place.
//!
//! What guarantees the condition here is what guarantees it in
//! production, and it is worth naming both halves:
//!
//! - **Within one node**, [`HybridClock`] is strictly increasing, so it
//!   cannot issue a reading twice. Across a restart it resumes from the
//!   highest reading its store holds rather than from zero, which is
//!   `crate::clock`'s own property.
//! - **Across nodes**, the identity differs, and the identity is the
//!   last part of the comparison. Two nodes with one identity would
//!   break this, and the handshake refuses a peer claiming ours.
//!
//! [`convergence::the_generator_produces_the_ties_each_part_of_the_order_breaks`]
//! is the other half of taking the generator seriously: the three parts
//! of the comparison are only load-bearing if the generated writes
//! actually tie on the first two, and a generator that never produced
//! two writes in one millisecond would let a comparison with the counter
//! or the identity deleted pass everything here.

use super::*;

use crate::clock::HybridClock;

/// How far ahead a peer may be, which nothing here reaches.
const BOUND: u64 = 1_000;

/// How many nodes the generated plans write from.
const NODES: usize = 3;

/// An identity that sorts by its index.
///
/// Sixteen identical bytes, so the ordering of two of these is the
/// ordering of the numbers that made them, which is what lets a test say
/// "the higher identity won" and mean something a reader can check.
fn node(index: usize) -> NodeId {
    NodeId::from_bytes([u8::try_from(index).expect("a small index"); 16])
}

/// A stamp built by hand, for the tests that are about the comparison
/// itself rather than about convergence.
///
/// Safe here and not in [`convergence`]: these are two or three stamps
/// written out in full where the duplication is the thing being asserted
/// about, rather than a generated population where a duplicate would be
/// an accident that looked like a defect.
fn stamp(millis: u64, counter: u32, node_index: usize) -> Stamp {
    Stamp::new(Reading::new(millis, counter), node(node_index))
}

// ---- the order -----------------------------------------------------------

/// The order is the milliseconds, then the counter, then the identity.
///
/// Three comparisons, each differing in exactly one of the three parts
/// and arranged so that a comparison which had dropped that part would
/// report equality. Section 8.2 names all three as load-bearing.
#[test]
fn the_order_is_the_milliseconds_then_the_counter_then_the_identity() {
    // The millisecond decides first: the earlier one has the higher
    // counter and the higher identity, so only the millisecond can be
    // what put them this way round.
    assert!(stamp(5, 900, 9) < stamp(6, 0, 1));

    // The counter decides next, with one node and one millisecond. This
    // is the comparison that keeps a node from being ambiguous with
    // itself.
    assert!(stamp(5, 1, 1) < stamp(5, 2, 1));

    // The identity breaks what is left. Without it these two are equal,
    // which is two nodes each keeping their own write for ever.
    assert!(stamp(5, 1, 1) < stamp(5, 1, 2));
}

/// The stamp's order agrees with the reading's, for one node.
///
/// [`Stamp`] spells the comparison out rather than deriving it, which
/// buys legibility and costs a second definition of the same order. This
/// is what stops the two drifting: the readings here differ in the
/// millisecond, in the counter, and in both.
#[test]
fn the_stamp_order_agrees_with_the_reading_order_for_one_node() {
    let readings = [
        Reading::new(4, 7),
        Reading::new(5, 0),
        Reading::new(5, 1),
        Reading::new(6, 0),
    ];
    for pair in readings.windows(2) {
        assert!(pair[0] < pair[1], "the readings themselves are in order");
        assert!(
            Stamp::new(pair[0], node(1)) < Stamp::new(pair[1], node(1)),
            "the stamp disagreed with the reading it holds: {:?} and {:?}",
            pair[0],
            pair[1]
        );
    }
}

// ---- tombstones ----------------------------------------------------------

/// A tombstone loses to a later write.
///
/// Which is the half "a delete always wins" gets wrong, and the half a
/// test is most likely to omit because the other direction is the one
/// everybody thinks about.
#[test]
fn a_tombstone_loses_to_a_later_write() {
    let mut held = Register::<u8>::deleted(stamp(5, 0, 1));
    assert!(
        held.merge(Register::written(7, stamp(6, 0, 1))),
        "a later write must be taken over a tombstone"
    );
    assert_eq!(held.value(), Some(&7));
    assert!(!held.is_tombstone());
}

/// A write loses to a tombstone written after it.
#[test]
fn a_write_loses_to_a_tombstone_written_after_it() {
    let mut held = Register::written(7, stamp(5, 0, 1));
    assert!(held.merge(Register::deleted(stamp(6, 0, 1))));
    assert!(held.is_tombstone());
    assert_eq!(held.value(), None);
}

/// Both orders of arrival agree, in both directions.
///
/// Four merges: a value and a tombstone, each merged into the other, in
/// both of the two possible stamp orders. If a delete won on its own
/// account, the two halves of each pair would disagree.
#[test]
fn both_orders_of_arrival_agree_for_a_value_and_a_tombstone() {
    for (earlier, later) in [
        (
            Register::written(7, stamp(5, 0, 1)),
            Register::deleted(stamp(6, 0, 1)),
        ),
        (
            Register::deleted(stamp(5, 0, 1)),
            Register::written(7, stamp(6, 0, 1)),
        ),
    ] {
        assert_eq!(
            earlier.merged(later),
            later.merged(earlier),
            "the two orders of arrival disagreed, which is divergence \
             between two nodes that saw the same two writes"
        );
        assert_eq!(
            earlier.merged(later).stamp(),
            later.stamp(),
            "the later stamp must be what survived"
        );
    }
}

// ---- what merge reports --------------------------------------------------

/// A merge says whether anything changed.
///
/// What a caller uses to decide whether to store a register and pass it
/// on. A copy that lost told us nothing, and gossiping it onwards would
/// be a message with no information in it.
#[test]
fn a_merge_reports_whether_it_took_what_it_was_given() {
    let later = Register::written(2u8, stamp(6, 0, 1));
    let earlier = Register::written(1u8, stamp(5, 0, 1));

    let mut held = earlier;
    assert!(held.merge(later), "a later copy is taken");
    assert!(!held.merge(earlier), "an earlier copy is not");
    assert!(!held.merge(later), "and neither is one we already hold");
    assert_eq!(held, later);
}

/// Equal stamps keep what is held, which is arbitrary and is the point.
///
/// Section 8.2 makes "no two writes share a stamp" an operational
/// requirement rather than a theoretical one, and this test is the shape
/// of what goes wrong when it is broken: the two nodes below each keep
/// their own value, and re-merging changes nothing, because each
/// believes it already holds the winner. There is no rule that repairs
/// it — whatever one side applies, the other applies to the mirror
/// image.
#[test]
fn two_writes_sharing_a_stamp_cannot_be_resolved_by_any_merge() {
    let shared = stamp(5, 0, 1);
    let mut one = Register::written(1u8, shared);
    let mut two = Register::written(2u8, shared);

    assert!(!one.merge(two), "neither copy beats the other");
    assert!(!two.merge(one));
    assert_eq!(one.value(), Some(&1), "each side kept its own");
    assert_eq!(two.value(), Some(&2));
    assert_ne!(
        one, two,
        "and that is divergence no further exchange repairs, which is why \
         a clock may not restart behind itself and two nodes may not share \
         an identity"
    );
}

// ---- convergence ---------------------------------------------------------

/// Two nodes handed the same writes in any order agree.
///
/// The property the whole module exists for. See this file's own
/// documentation for why the writes come from real clocks.
mod convergence {
    use super::*;

    use proptest::prelude::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;

    /// One write in a generated plan: which node made it, what its wall
    /// clock said, what it wrote, and whether it was a deletion.
    type Planned = (usize, u64, u8, bool);

    /// Writes, each stamped by the clock of the node that made it.
    ///
    /// The wall readings come from a small range on purpose: two writes
    /// in one millisecond is the case the counter and the identity
    /// exist for, and a wide range produces neither.
    fn writes() -> impl Strategy<Value = Vec<Register<u8>>> {
        proptest::collection::vec((0usize..NODES, 0u64..6, any::<u8>(), any::<bool>()), 1..12)
            .prop_map(|plan| issued(&plan))
    }

    /// Stamp a plan with real clocks, one per node.
    ///
    /// Every node resumes from nothing and issues in plan order, so
    /// within a node the readings are strictly increasing because that
    /// is what the clock promises, and across nodes the identities
    /// differ. Those two together are what guarantee no two of these
    /// writes share a stamp.
    fn issued(plan: &[Planned]) -> Vec<Register<u8>> {
        let nodes: Vec<(NodeId, HybridClock)> = (0..NODES)
            .map(|index| (node(index + 1), HybridClock::resume(None, BOUND)))
            .collect();
        plan.iter()
            .map(|(which, wall, value, deleted)| {
                let (id, clock) = &nodes[*which];
                let stamp = Stamp::new(clock.local(*wall), *id);
                if *deleted {
                    Register::deleted(stamp)
                } else {
                    Register::written(*value, stamp)
                }
            })
            .collect()
    }

    /// The same writes, in two independently shuffled orders.
    ///
    /// Shuffled *after* they are stamped. Shuffling the plan instead
    /// would hand the clocks a different order and so produce different
    /// stamps, which is a different set of writes rather than the same
    /// set delivered differently.
    fn two_deliveries() -> impl Strategy<Value = (Vec<Register<u8>>, Vec<Register<u8>>)> {
        writes().prop_flat_map(|writes| {
            (
                Just(writes.clone()).prop_shuffle(),
                Just(writes).prop_shuffle(),
            )
        })
    }

    /// Everything one node ends up holding, having seen these writes in
    /// this order.
    fn held(writes: &[Register<u8>]) -> Register<u8> {
        let mut held = writes[0];
        for write in &writes[1..] {
            held.merge(*write);
        }
        held
    }

    /// The six ordered pairs of three nodes, which is one full round of
    /// gossip.
    const PAIRS: [(usize, usize); 6] = [(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)];

    /// Two full rounds of gossip, each in a random order.
    ///
    /// The *order* is random and the *multiset* is not, which is what
    /// makes the assertion meaningful: a schedule generated freely could
    /// leave a node nothing ever reached, and three nodes failing to
    /// agree because one of them was never told is not a defect in the
    /// merge.
    fn gossip_schedule() -> impl Strategy<Value = Vec<(usize, usize)>> {
        (
            Just(PAIRS.to_vec()).prop_shuffle(),
            Just(PAIRS.to_vec()).prop_shuffle(),
        )
            .prop_map(|(first, second)| first.into_iter().chain(second).collect())
    }

    /// Hand the writes round three nodes, then gossip on this schedule.
    fn gossiped(writes: &[Register<u8>], schedule: &[(usize, usize)]) -> Vec<Option<Register<u8>>> {
        let mut nodes: Vec<Option<Register<u8>>> = vec![None; NODES];
        for (index, write) in writes.iter().enumerate() {
            take(&mut nodes[index % NODES], *write);
        }
        for (from, to) in schedule {
            if let Some(sent) = nodes[*from] {
                take(&mut nodes[*to], sent);
            }
        }
        nodes
    }

    /// Merge into a slot that may hold nothing yet.
    ///
    /// A node that has never seen this record takes whatever arrives,
    /// which is the one case `Register::merge` cannot express: there is
    /// no stamp to lose to.
    fn take(slot: &mut Option<Register<u8>>, arriving: Register<u8>) {
        match slot {
            Some(held) => {
                held.merge(arriving);
            }
            empty => *empty = Some(arriving),
        }
    }

    proptest! {
        /// The property. Two nodes handed the same writes in any order
        /// agree.
        #[test]
        fn two_nodes_handed_the_same_writes_in_any_order_agree(
            (first, second) in two_deliveries(),
        ) {
            // The condition convergence rests on, asserted rather than
            // assumed. If this ever fails it is the generator that is
            // wrong and not the merge, and saying so here is the
            // difference between a defect and an hour.
            let mut stamps: Vec<Stamp> = first.iter().map(Register::stamp).collect();
            stamps.sort_unstable();
            let distinct = stamps.windows(2).all(|pair| pair[0] != pair[1]);
            prop_assert!(
                distinct,
                "two generated writes shared a stamp, which no merge can resolve; \
                 the clocks or the identities are wrong, not the merge"
            );

            prop_assert_eq!(
                held(&first),
                held(&second),
                "two nodes given the same writes in different orders disagreed"
            );
        }

        /// Three nodes gossiping in a random order all end up the same.
        ///
        /// A step past the property above: the writes are not handed to
        /// everybody, they are handed to one node each and spread by
        /// merging, which is what the mesh will actually do.
        #[test]
        fn three_nodes_gossiping_in_a_random_order_all_end_up_the_same(
            writes in writes(),
            schedule in gossip_schedule(),
        ) {
            let nodes = gossiped(&writes, &schedule);
            let first = nodes[0].expect("a full round of gossip reaches every node");
            for (index, node) in nodes.iter().enumerate() {
                prop_assert_eq!(
                    node.expect("a full round of gossip reaches every node"),
                    first,
                    "node {} disagreed with node 0 after gossiping",
                    index
                );
            }
            // And what they agree on is the latest write, not merely
            // something they all share: a merge that discarded
                // everything would also make them agree.
            let latest = writes
                .iter()
                .max_by_key(|write| write.stamp())
                .expect("at least one write");
            prop_assert_eq!(first, *latest);
        }
    }

    /// The generator produces the ties each part of the order breaks.
    ///
    /// Convention 5's "a generator that cannot produce a case", pointed
    /// at the two cases that make two of the three parts of the
    /// comparison load-bearing:
    ///
    /// - Two writes from **one** node in one millisecond, which only the
    ///   counter separates.
    /// - Two writes from **different** nodes with the same reading,
    ///   which only the identity separates.
    ///
    /// Without the first, a comparison with the counter deleted passes
    /// everything in this module. Without the second, so does one with
    /// the identity deleted. Tombstones are counted too, because a
    /// generated population of pure writes would never exercise the
    /// merge's deliberate lack of a tombstone branch.
    #[test]
    fn the_generator_produces_the_ties_each_part_of_the_order_breaks() {
        let mut runner = TestRunner::deterministic();
        let strategy = writes();

        let (mut counter_ties, mut identity_ties, mut tombstones, mut values) = (0, 0, 0, 0);
        for _ in 0..512 {
            let writes = strategy
                .new_tree(&mut runner)
                .expect("the strategy must produce a value")
                .current();
            for (index, one) in writes.iter().enumerate() {
                if one.is_tombstone() {
                    tombstones += 1;
                } else {
                    values += 1;
                }
                for two in &writes[index + 1..] {
                    let (first, second) = (one.stamp(), two.stamp());
                    if first.node() == second.node()
                        && first.reading().millis() == second.reading().millis()
                    {
                        counter_ties += 1;
                    }
                    if first.node() != second.node() && first.reading() == second.reading() {
                        identity_ties += 1;
                    }
                }
            }
        }

        assert!(
            counter_ties > 0,
            "the generator never produced two writes from one node inside one \
             millisecond, so a comparison with the counter deleted would pass"
        );
        assert!(
            identity_ties > 0,
            "the generator never produced two writes from different nodes with \
             the same reading, so a comparison with the identity deleted would pass"
        );
        assert!(tombstones > 0, "the generator never produced a deletion");
        assert!(values > 0, "the generator never produced a write");
    }
}
