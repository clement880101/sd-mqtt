//! Subscription storage and matching.
//!
//! # Precondition
//!
//! Every filter and topic handed to this module must already have been
//! validated by [`mqtt_codec::topic`]. Matching recurses once per topic
//! level, so traversal depth is stack depth, and nothing here bounds it:
//! the bound is [`mqtt_codec::topic::MAX_TOPIC_DEPTH`], enforced at
//! validation. An unvalidated topic of sufficient depth aborts the
//! process rather than returning an error.

pub(crate) mod node;
pub mod shared;
pub mod trie;

pub use node::{SubscriberId, Subscription};
pub use shared::SharedSubscriptions;
pub use trie::SubscriptionTrie;
