//! A user written on one node is a user on another: section 8.2's
//! register carries it, and a delete is a tombstone that carries too.

use std::sync::Arc;

use broker_core::broker::{Broker, BrokerConfig};
use cluster::{HybridClock, NodeId, Replicated, DEFAULT_MAX_DRIFT_MILLIS};
use directory::{Directory, Settings};
use sdmqtt_server::shared::Shared;
use storage::record::Secret;
use storage::{MemoryStorage, Storage, Writer};

/// One node's directory, with its replicated state exposed so a test
/// can hand its writes to another node.
fn node() -> (Arc<Directory>, Arc<Replicated>, Arc<MemoryStorage>) {
    let store = Arc::new(MemoryStorage::new());
    let writer = Arc::new(
        Writer::spawn(
            Arc::clone(&store) as Arc<dyn Storage>,
            storage::WriterConfig::default(),
        )
        .expect("a writer thread"),
    );
    let replicated = Arc::new(
        Replicated::open(
            NodeId::mint(),
            HybridClock::resume(None, DEFAULT_MAX_DRIFT_MILLIS),
            &storage::Restored::default(),
        )
        .expect("an empty store holds nothing to refuse"),
    );
    let shared = Arc::new(Shared::with_replication(
        Broker::new(BrokerConfig::default()),
        Arc::clone(&writer),
        Arc::clone(&replicated),
    ));
    let directory = Directory::new(
        writer,
        shared,
        Arc::clone(&replicated),
        Settings::default(),
        Vec::new(),
    );
    (directory, replicated, store)
}

#[tokio::test]
async fn a_user_put_on_one_node_connects_on_another_and_a_delete_follows() {
    let (alpha, alpha_replicated, _) = node();
    let (beta, _, beta_store) = node();

    alpha
        .put_user("alice", &Secret::new("hunter2"))
        .await
        .expect("created");
    // The register alpha wrote, as a peer would receive it.
    let write = alpha_replicated.user(storage::record::UserRecord {
        name: "alice".to_owned(),
        ..alpha_record(&alpha_replicated, "alice")
    });
    beta.apply_replicated(&write).await.expect("applied");
    assert!(
        beta.check_password("alice", b"hunter2"),
        "the same password works on beta"
    );
    assert_eq!(beta.users().len(), 1);
    // And it is on beta's disk, not only in its memory.
    let restored = beta_store.restore().expect("restores");
    assert_eq!(restored.users.len(), 1);
    assert_eq!(restored.users[0].name, "alice");

    let tombstone = alpha_replicated.user_removed("alice");
    beta.apply_replicated(&tombstone).await.expect("applied");
    assert!(!beta.check_password("alice", b"hunter2"));
    assert!(beta.users().is_empty());
    assert!(beta_store.restore().expect("restores").users.is_empty());
}

/// The record alpha holds for `name`, read back through its register.
fn alpha_record(replicated: &Replicated, name: &str) -> storage::record::UserRecord {
    let key = cluster::MetadataKey::User {
        name: name.to_owned(),
    };
    match replicated
        .register(&key)
        .and_then(|register| register.value().cloned())
    {
        Some(cluster::MetadataValue::User(user)) => *user,
        other => panic!("alpha holds no user register for {name}: {other:?}"),
    }
}
