//! What a publish costs, against a trie of a realistic size.

use broker_core::subscriptions::{SubscriberId, Subscription, SubscriptionTrie};
use criterion::{criterion_group, criterion_main, Criterion};
use mqtt_codec::QoS;
use std::hint::black_box;

fn sub(id: u64) -> Subscription {
    Subscription {
        subscriber: SubscriberId(id),
        qos: QoS::AtMostOnce,
        no_local: false,
        retain_as_published: false,
        id: None,
    }
}

/// Ten thousand device-shaped subscriptions, the shape a fleet produces.
fn populated() -> SubscriptionTrie {
    let trie = SubscriptionTrie::new(64);
    let mut id = 0u64;
    for site in 0..100 {
        for device in 0..100 {
            trie.insert(&format!("site/{site}/device/{device}/telemetry"), sub(id));
            id += 1;
        }
        trie.insert(&format!("site/{site}/device/+/telemetry"), sub(id));
        id += 1;
    }
    trie.insert("site/#", sub(id));
    trie
}

fn benches(c: &mut Criterion) {
    let trie = populated();
    let mut out = Vec::with_capacity(16);

    c.bench_function("match one deep topic", |b| {
        b.iter(|| {
            out.clear();
            trie.matching(black_box("site/42/device/17/telemetry"), &mut out);
            black_box(out.len())
        })
    });

    c.bench_function("match a topic nothing wants", |b| {
        b.iter(|| {
            out.clear();
            trie.matching(black_box("other/thing/entirely"), &mut out);
            black_box(out.len())
        })
    });

    c.bench_function("subscribe then unsubscribe", |b| {
        b.iter(|| {
            trie.insert(black_box("bench/scratch/topic"), sub(u64::MAX));
            trie.remove(black_box("bench/scratch/topic"), SubscriberId(u64::MAX));
        })
    });
}

criterion_group!(matching, benches);
criterion_main!(matching);
