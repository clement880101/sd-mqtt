//! Subscription storage and matching.

pub(crate) mod node;
pub mod shared;
pub mod trie;

pub use node::{SubscriberId, Subscription};
pub use shared::SharedSubscriptions;
pub use trie::SubscriptionTrie;
