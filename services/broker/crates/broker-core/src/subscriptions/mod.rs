//! Subscription storage and matching.

pub mod node;
pub mod shared;
pub mod trie;

pub use node::{SubscriberId, Subscription};
pub use shared::SharedSubscriptions;
pub use trie::SubscriptionTrie;
