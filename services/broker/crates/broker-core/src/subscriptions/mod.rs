//! Subscription storage and matching.

pub mod node;
pub mod trie;

pub use node::{SubscriberId, Subscription};
pub use trie::SubscriptionTrie;
