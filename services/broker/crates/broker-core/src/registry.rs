//! Sessions by identity, and what happens when two connections claim one.

use std::sync::Arc;

/// What identifies a session.
///
/// The mountpoint namespaces a listener's clients into their own topic
/// tree, so the same client identifier arriving on two listeners is two
/// sessions. That is multi-tenancy for the cost of one string.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId {
    /// The listener's namespace, empty for the default.
    pub mountpoint: Arc<str>,
    /// The identifier the client chose, or one assigned to it.
    pub client_id: Arc<str>,
}

impl SessionId {
    /// Build an identity.
    pub fn new(mountpoint: &str, client_id: &str) -> Self {
        Self {
            mountpoint: Arc::from(mountpoint),
            client_id: Arc::from(client_id),
        }
    }
}
