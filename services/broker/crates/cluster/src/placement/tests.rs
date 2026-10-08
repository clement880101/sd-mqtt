//! Section 8.6's placement, and the four things it has to be.
//!
//! Deterministic across nodes, stable across a membership change,
//! spread across the cluster, and honest about a cluster smaller than
//! the factor.
//!
//! # The golden vectors here were computed somewhere else
//!
//! Every expected number below came from Python's `hashlib` and, for
//! the node digest, from `shasum -a 256` as well — not from printing
//! what this module returns. That is the difference between a vector
//! that pins the behaviour and one that pins whatever the code does
//! today, and this project has the second kind on record as a defect
//! rather than a test.

use super::*;

use proptest::prelude::*;

/// The node whose identity bytes are `01..10`.
///
/// Chosen so the digest can be checked by hand:
///
/// ```text
/// $ printf '\x01\x02...\x10' | shasum -a 256
/// 5dfbabeedf318bf33c0927c43d7630f51b82f351740301354fa3d7fc51f0132e
/// ```
fn counted_node() -> NodeId {
    let mut bytes = [0u8; 16];
    for (at, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::try_from(at + 1).expect("sixteen fits");
    }
    NodeId::from_bytes(bytes)
}

/// A node whose sixteen bytes are all `nth`.
fn node(nth: u8) -> NodeId {
    NodeId::from_bytes([nth; 16])
}

/// `count` distinct nodes, weighed.
fn members(count: u8) -> Vec<Weighted> {
    (1..=count).map(|nth| Weighted::new(node(nth))).collect()
}

fn key(client: &str) -> SessionKey {
    SessionKey::new("", client)
}

#[test]
fn the_node_digest_is_sha256_of_its_sixteen_identity_bytes() {
    // From `shasum -a 256` over those bytes, first eight, big-endian.
    // Big-endian so this reads off the front of the printed digest:
    // 5dfbabeedf318bf3|3c0927c4...
    assert_eq!(Weighted::new(counted_node()).digest, 0x5dfb_abee_df31_8bf3);
}

#[test]
fn a_session_digest_is_length_prefixed_so_two_keys_cannot_collide() {
    // **Mountpoint `a` with client `bc`, and mountpoint `ab` with
    // client `c`.** Concatenated they are the same four bytes, and a
    // digest over the concatenation would place two different
    // sessions' queues on the same nodes under one name. This broker
    // has shipped that bug once already, in the stored sequence keys.
    let left = session_digest(&SessionKey::new("a", "bc"));
    let right = session_digest(&SessionKey::new("ab", "c"));
    assert_eq!(left, 0x3faf_a1cf_2f19_a7c1);
    assert_eq!(right, 0x601d_5476_e2cc_fe2c);
    assert_ne!(
        left, right,
        "two keys whose concatenations are equal must not hash the same"
    );
    assert_eq!(session_digest(&key("a-client")), 0xb651_6f47_5a08_9fda);
}

#[test]
fn the_mixer_is_the_published_finalizer_over_the_two_digests() {
    // SplitMix64's finalizer, computed in Python over the two vectors
    // above rather than printed from here.
    assert_eq!(
        mix(0xb651_6f47_5a08_9fda, 0x5dfb_abee_df31_8bf3),
        0x361c_8e34_8c3c_965e
    );
}

#[test]
fn a_factor_is_refused_outside_one_to_three() {
    assert_eq!(Factor::new(0), Err(FactorError::None));
    assert_eq!(Factor::new(4), Err(FactorError::TooMany { asked: 4 }));
    assert_eq!(Factor::new(1).expect("one copy").copies(), 1);
    assert_eq!(Factor::new(3).expect("three copies").copies(), 3);
    assert_eq!(Factor::default().copies(), DEFAULT_FACTOR);
    // The messages differ, because the two mistakes have different
    // answers: one is "write a number" and the other is "write a
    // smaller one".
    assert_ne!(
        FactorError::None.to_string(),
        FactorError::TooMany { asked: 4 }.to_string()
    );
}

#[test]
fn a_factor_of_one_is_the_owner_and_nobody_else() {
    // A supported configuration, and the one where a follower loop
    // with an off-by-one streams to the owner itself.
    let placed = place(&key("a-client"), &members(5), Factor::new(1).expect("one"));
    assert_eq!(placed.len(), 1);
}

#[test]
fn fewer_members_than_the_factor_gives_as_many_as_there_are() {
    // Section 8.6: an ordinary state, not an error. A cluster of one
    // with the default factor is a cluster still being built.
    let placed = place(&key("a-client"), &members(1), Factor::default());
    assert_eq!(placed.len(), 1);
    assert_eq!(placed[0], node(1));
    assert!(
        place(&key("a-client"), &[], Factor::default()).is_empty(),
        "no members is no placement, rather than a panic"
    );
}

#[test]
fn a_tie_is_broken_on_identity_rather_than_on_the_order_they_arrived() {
    // Two equal weights cannot be produced from real digests at any
    // rate worth waiting for, so they are forced. Left to the input
    // order the two nodes below would place differently depending on
    // which answered a membership query first, which is two nodes
    // disagreeing about placement while agreeing about everything
    // else.
    let forced = |nth: u8| Weighted {
        node: node(nth),
        digest: 7,
    };
    let one = place(&key("a-client"), &[forced(1), forced(2)], Factor::default());
    let other = place(&key("a-client"), &[forced(2), forced(1)], Factor::default());
    assert_eq!(one, other);
    assert_eq!(one[0], node(1), "the lower identity wins a tie");
}

#[test]
fn every_node_owns_some_sessions_rather_than_one_owning_all() {
    // **The test the mutation that drops the node digest fails.** A
    // weight that ignores which node it is about is the same for
    // every member, so the tie-break decides and one node owns
    // everything — and every determinism test still passes, because
    // it is perfectly deterministic.
    let members = members(5);
    let mut owned = std::collections::BTreeMap::new();
    for nth in 0..1_000 {
        let placed = place(&key(&format!("client-{nth}")), &members, Factor::default());
        *owned.entry(placed[0]).or_insert(0usize) += 1;
    }
    assert_eq!(owned.len(), 5, "every node should own some: {owned:?}");
    // **This is also the test that fails if the ordering is by
    // identity rather than by weight**, which the stability property
    // below does not catch. Said here because the two look like a
    // smoke test and a real test, and it is the other way round.
    //
    // A fifth of a thousand is two hundred. The bound is loose
    // because this asserts that the mixer spreads rather than that it
    // is uniform, and a tight bound here would be a test that fails
    // when somebody adds a node to the fixture.
    for (node, count) in &owned {
        assert!(
            (80..=400).contains(count),
            "{node} owns {count} of 1000, which is not a spread"
        );
    }
}

proptest! {
    /// The same inputs give the same answer, whatever order the
    /// members are in.
    ///
    /// Membership arrives from a hash map and over a network, so the
    /// order is not a fact about the cluster. Two nodes that sorted
    /// their members differently and placed differently would stream
    /// copies to different places while both believing they agreed.
    #[test]
    fn placement_does_not_depend_on_the_order_members_arrive_in(
        client in "[a-z]{1,12}",
        count in 1u8..8,
        seed in any::<u64>(),
    ) {
        let session = key(&client);
        let straight = members(count);
        let mut shuffled = straight.clone();
        // A deterministic shuffle from the seed, so a failure can be
        // reproduced from the case proptest prints.
        let mut state = seed | 1;
        for at in (1..shuffled.len()).rev() {
            state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            shuffled.swap(at, (state >> 33) as usize % (at + 1));
        }
        prop_assert_eq!(
            place(&session, &straight, Factor::default()),
            place(&session, &shuffled, Factor::default())
        );
    }

    /// Removing a member moves only the sessions that were on it.
    ///
    /// **Necessary, and on its own not sufficient**, which is worth
    /// knowing before anybody treats it as the test that pins
    /// rendezvous. "Always the lowest R identities" has this property
    /// too — removing a node outside the set changes nothing, and
    /// removing one inside it lets exactly one more in — and that
    /// placement was tried as a mutation and passed here. What fails
    /// it is the distribution test above. The two together are what
    /// pin the choice: this one says placement is stable, that one
    /// says it is spread, and a scheme needs both.
    #[test]
    fn removing_a_member_moves_only_what_it_held(
        count in 3u8..8,
        removed in 0usize..3,
    ) {
        let all = members(count);
        let gone = all[removed % all.len()].node;
        let rest: Vec<Weighted> = all.iter().copied().filter(|m| m.node != gone).collect();

        for nth in 0..200 {
            let session = key(&format!("client-{nth}"));
            let before = place(&session, &all, Factor::default());
            let after = place(&session, &rest, Factor::default());
            if before.contains(&gone) {
                // It held this one, so the set changes — but only by
                // dropping it: whoever else was in the set stays, and
                // stays in the same order.
                let kept: Vec<NodeId> =
                    before.iter().copied().filter(|node| *node != gone).collect();
                prop_assert_eq!(
                    &after[..kept.len()],
                    &kept[..],
                    "the members that remained should keep their places"
                );
            } else {
                prop_assert_eq!(
                    before,
                    after,
                    "a session that was not on the departed member must not move"
                );
            }
        }
    }
}
