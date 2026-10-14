//! Who may connect, and what they may do. Sections 12.1, 12.2 and 8.2.
//!
//! # Replicated, like a rule
//!
//! A user is durable metadata: a register per name, stamped with the
//! node's hybrid clock and broadcast once the store has it, exactly as
//! `rules::live::Registry` does for a rule. A peer's install stamps
//! nothing and broadcasts nothing. A delete is a tombstone.
//!
//! # A hook, not a special case
//!
//! The directory installs itself on the broker through section 9.3's
//! interface. `authenticate` is asked on every CONNECT. Nothing in the
//! broker's connect path knows users exist, which is the point of the
//! interface: an external authenticator would sit in the same seat.
//!
//! # Anonymous is a switch
//!
//! [`Settings::anonymous`] decides whether a connect with no username
//! is admitted. It does not flip itself when the first user is created,
//! because a setting that changes under an operator is one they cannot
//! reason about.

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock};

use broker_core::hooks::{Credentials, Decision, Hook};
use cluster::{MetadataKey, MetadataValue, MetadataWrite, Replicated};
use sdmqtt_server::shared::Shared;
use storage::record::{ScramVerifierRecord, Secret, UserRecord};
use storage::{Record, Writer};

pub mod user;

pub use user::UserView;

/// How the directory behaves beyond its tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    /// Whether a connect with no username is admitted. Section 12.1's
    /// "anonymous" is this switch.
    pub anonymous: bool,
    /// PBKDF2 rounds for a new SCRAM verifier. Section 12.2.
    pub scram_iterations: u32,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            anonymous: true,
            scram_iterations: 4096,
        }
    }
}

/// Why a change to the directory was refused.
#[derive(Debug, thiserror::Error)]
pub enum DirectoryError {
    /// The name is empty, or carries a control character.
    #[error("a user name cannot be empty or contain a control character")]
    BadName,
    /// The password is empty.
    #[error("a password cannot be empty")]
    EmptyPassword,
    /// No such user.
    #[error("there is no user named {name}")]
    NoSuchUser {
        /// The name asked for.
        name: String,
    },
    /// The store would not take it.
    #[error("the store refused the change: {why}")]
    Storage {
        /// What the store said.
        why: String,
    },
}

/// The directory: users, with access control rules and API keys to come.
pub struct Directory {
    writer: Arc<Writer>,
    shared: Arc<Shared>,
    replicated: Arc<Replicated>,
    settings: Settings,
    users: RwLock<BTreeMap<String, UserRecord>>,
    /// One change at a time, so two puts of one name cannot interleave
    /// their records and their registers.
    changing: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for Directory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Directory")
            .field("settings", &self.settings)
            .field("users", &self.users.read().map(|u| u.len()).unwrap_or(0))
            .finish_non_exhaustive()
    }
}

impl Directory {
    /// A directory over `writer`, seeded with what the store restored.
    #[must_use]
    pub fn new(
        writer: Arc<Writer>,
        shared: Arc<Shared>,
        replicated: Arc<Replicated>,
        settings: Settings,
        restored: Vec<UserRecord>,
    ) -> Arc<Self> {
        Arc::new(Self {
            writer,
            shared,
            replicated,
            settings,
            users: RwLock::new(
                restored
                    .into_iter()
                    .map(|user| (user.name.clone(), user))
                    .collect(),
            ),
            changing: tokio::sync::Mutex::new(()),
        })
    }

    /// The settings this directory runs with.
    #[must_use]
    pub fn settings(&self) -> Settings {
        self.settings
    }

    /// Every user, by name, without secrets.
    #[must_use]
    pub fn users(&self) -> Vec<UserView> {
        self.users
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(UserView::from)
            .collect()
    }

    /// One user, without secrets.
    #[must_use]
    pub fn user(&self, name: &str) -> Option<UserView> {
        self.users
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .map(UserView::from)
    }

    /// Create a user, or set an existing one's password. Both
    /// credential forms are derived now, because this is the one moment
    /// the password is in hand (section 12.2).
    ///
    /// # Errors
    ///
    /// A bad name or an empty password, or a store that refused.
    pub async fn put_user(
        &self,
        name: &str,
        password: &Secret,
    ) -> Result<UserView, DirectoryError> {
        if name.is_empty() || name.chars().any(char::is_control) {
            return Err(DirectoryError::BadName);
        }
        if password.is_empty() {
            return Err(DirectoryError::EmptyPassword);
        }
        let _changing = self.changing.lock().await;
        let created_at = self
            .users
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .map_or_else(now, |existing| existing.created_at);
        let record =
            user::with_password(name, password, self.settings.scram_iterations, created_at);
        let write = self.replicated.user(record.clone());
        let mut records = vec![Record::UserPut(Box::new(record.clone()))];
        records.extend(self.replicated.records(&write));
        self.durable(records, Some(write)).await?;
        self.users
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name.to_owned(), record.clone());
        Ok(UserView::from(&record))
    }

    /// Remove a user. A tombstone replicates the removal.
    ///
    /// # Errors
    ///
    /// No such user, or a store that refused.
    pub async fn delete_user(&self, name: &str) -> Result<(), DirectoryError> {
        let _changing = self.changing.lock().await;
        if !self
            .users
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(name)
        {
            return Err(DirectoryError::NoSuchUser {
                name: name.to_owned(),
            });
        }
        let write = self.replicated.user_removed(name);
        let mut records = vec![Record::UserDeleted {
            name: name.to_owned(),
        }];
        records.extend(self.replicated.records(&write));
        self.durable(records, Some(write)).await?;
        self.users
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(name);
        Ok(())
    }

    /// Whether `password` is `name`'s. False for a name nobody has: a
    /// missing user and a wrong password are one answer, so a caller
    /// cannot tell which names exist by trying.
    #[must_use]
    pub fn check_password(&self, name: &str, password: &[u8]) -> bool {
        let users = self.users.read().unwrap_or_else(PoisonError::into_inner);
        users
            .get(name)
            .is_some_and(|user| user::check_password(user, password))
    }

    /// `name`'s SCRAM verifier, for the AUTH exchange.
    #[must_use]
    pub fn verifier(&self, name: &str) -> Option<ScramVerifierRecord> {
        self.users
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .and_then(|user| user.scram.clone())
    }

    /// Apply what a peer replicated: the record and the register, in
    /// one batch, stamping nothing and broadcasting nothing.
    ///
    /// # Errors
    ///
    /// A store that refused.
    pub async fn apply_replicated(&self, write: &MetadataWrite) -> Result<(), DirectoryError> {
        let MetadataKey::User { name } = write.key() else {
            tracing::error!(
                kind = write.key().kind(),
                "a register that is not a user reached the directory"
            );
            return Ok(());
        };
        let _changing = self.changing.lock().await;
        // The register itself is stored by whoever hands the write over,
        // in the batch that carried it; this batch is this node's own
        // copy of the user and nothing else.
        let mut records = Vec::new();
        match write.register().value() {
            Some(MetadataValue::User(record)) => {
                records.push(Record::UserPut(record.clone()));
                self.durable(records, None).await?;
                self.users
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(name.clone(), (**record).clone());
            }
            Some(other) => {
                tracing::error!(
                    kind = other.kind(),
                    "a user's register carried something that is not a user"
                );
            }
            None => {
                records.push(Record::UserDeleted { name: name.clone() });
                self.durable(records, None).await?;
                self.users
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(name);
            }
        }
        Ok(())
    }

    /// Write, and when the write is ours, broadcast once it is durable.
    async fn durable(
        &self,
        records: Vec<Record>,
        ours: Option<MetadataWrite>,
    ) -> Result<(), DirectoryError> {
        fn storage(error: impl std::fmt::Display) -> DirectoryError {
            DirectoryError::Storage {
                why: error.to_string(),
            }
        }
        let Some(write) = ours else {
            self.writer
                .apply_durable(records)
                .map_err(storage)?
                .await
                .map_err(storage)?;
            return Ok(());
        };
        let (mine, theirs) = self.writer.apply_durable_twice(records).map_err(storage)?;
        self.shared.broadcast_when_durable(theirs, vec![write]);
        mine.await.map_err(storage)
    }

    /// This directory as a hook on the broker.
    #[must_use]
    pub fn hook(self: &Arc<Self>) -> Arc<dyn Hook> {
        Arc::new(DirectoryHook {
            directory: Arc::clone(self),
        })
    }
}

/// Seconds since the epoch, for `created_at`.
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// The directory in the hook chain.
struct DirectoryHook {
    directory: Arc<Directory>,
}

impl Hook for DirectoryHook {
    fn name(&self) -> &str {
        "directory"
    }

    fn authenticate(&self, credentials: &Credentials<'_>) -> Decision {
        match (credentials.username, credentials.password) {
            // Anonymous: the switch decides. `Next` rather than `Allow`
            // when admitted, so a connector's hook after this one may
            // still refuse and the configured default still applies.
            (None, _) => {
                if self.directory.settings.anonymous {
                    Decision::Next
                } else {
                    Decision::Deny
                }
            }
            (Some(name), Some(password)) => {
                if self.directory.check_password(name, password) {
                    Decision::Allow
                } else {
                    Decision::Deny
                }
            }
            // A name with no password is not a password login. SCRAM
            // arrives through the AUTH exchange, which the connection
            // handles before it asks this chain.
            (Some(_), None) => Decision::Deny,
        }
    }
}
