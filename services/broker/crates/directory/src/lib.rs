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

pub mod acl;
pub mod key;
pub mod user;

pub use key::{ApiKeyView, Minted, Scope};
pub use storage::record::{AclRecord, ApiKeyRecord};
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
    /// An access control rule that is not one.
    #[error("the rule's {field} is not usable: {why}")]
    BadRule {
        /// Which field.
        field: &'static str,
        /// Why.
        why: String,
    },
    /// No such rule.
    #[error("there is no access control rule named {id}")]
    NoSuchRule {
        /// The identifier asked for.
        id: String,
    },
    /// No such API key.
    #[error("there is no API key with the identifier {id}")]
    NoSuchKey {
        /// The identifier asked for.
        id: String,
    },
    /// The store would not take it.
    #[error("the store refused the change: {why}")]
    Storage {
        /// What the store said.
        why: String,
    },
}

/// The directory: users, access control rules and API keys.
pub struct Directory {
    writer: Arc<Writer>,
    shared: Arc<Shared>,
    replicated: Arc<Replicated>,
    settings: Settings,
    users: RwLock<BTreeMap<String, UserRecord>>,
    /// Section 12.1's access control rules, by identifier; consulted in
    /// order through [`Directory::acl`].
    acl: RwLock<BTreeMap<String, AclRecord>>,
    /// Section 11.1's API keys, by identifier, as digests.
    api_keys: RwLock<BTreeMap<String, ApiKeyRecord>>,
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
        restored_acl: Vec<AclRecord>,
        restored_keys: Vec<ApiKeyRecord>,
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
            acl: RwLock::new(
                restored_acl
                    .into_iter()
                    .map(|rule| (rule.id.clone(), rule))
                    .collect(),
            ),
            api_keys: RwLock::new(
                restored_keys
                    .into_iter()
                    .map(|key| (key.id.clone(), key))
                    .collect(),
            ),
            changing: tokio::sync::Mutex::new(()),
        })
    }

    /// Every access control rule, in consultation order.
    #[must_use]
    pub fn acl(&self) -> Vec<AclRecord> {
        let mut rules: Vec<AclRecord> = self
            .acl
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        acl::in_order(&mut rules);
        rules
    }

    /// Create or replace an access control rule, whole.
    ///
    /// # Errors
    ///
    /// A rule that is not one — an empty identifier, a subject that is
    /// none of the three forms, a filter that is not a filter, or no
    /// opinion on either operation — or a store that refused.
    pub async fn put_acl(&self, rule: AclRecord) -> Result<AclRecord, DirectoryError> {
        check_rule(&rule)?;
        let _changing = self.changing.lock().await;
        let write = self.replicated.acl(rule.clone());
        let mut records = vec![Record::AclPut(Box::new(rule.clone()))];
        records.extend(self.replicated.records(&write));
        self.durable(records, Some(write)).await?;
        self.acl
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(rule.id.clone(), rule.clone());
        Ok(rule)
    }

    /// Remove an access control rule. A tombstone replicates the removal.
    ///
    /// # Errors
    ///
    /// No such rule, or a store that refused.
    pub async fn delete_acl(&self, id: &str) -> Result<(), DirectoryError> {
        let _changing = self.changing.lock().await;
        if !self
            .acl
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(id)
        {
            return Err(DirectoryError::NoSuchRule { id: id.to_owned() });
        }
        let write = self.replicated.acl_removed(id);
        let mut records = vec![Record::AclDeleted { id: id.to_owned() }];
        records.extend(self.replicated.records(&write));
        self.durable(records, Some(write)).await?;
        self.acl
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(id);
        Ok(())
    }

    /// Mint an API key. Section 11.1. The text is in the answer and
    /// nowhere else.
    ///
    /// # Errors
    ///
    /// An unusable name, or a store that refused.
    pub async fn create_api_key(&self, name: &str, scope: Scope) -> Result<Minted, DirectoryError> {
        if name.is_empty() || name.chars().any(char::is_control) {
            return Err(DirectoryError::BadName);
        }
        let _changing = self.changing.lock().await;
        let minted = key::mint(name, scope, now());
        let write = self.replicated.api_key(minted.record.clone());
        let mut records = vec![Record::ApiKeyPut(Box::new(minted.record.clone()))];
        records.extend(self.replicated.records(&write));
        self.durable(records, Some(write)).await?;
        self.api_keys
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(minted.record.id.clone(), minted.record.clone());
        Ok(minted)
    }

    /// Revoke an API key. A tombstone replicates the revocation.
    ///
    /// # Errors
    ///
    /// No such key, or a store that refused.
    pub async fn revoke_api_key(&self, id: &str) -> Result<(), DirectoryError> {
        let _changing = self.changing.lock().await;
        if !self
            .api_keys
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(id)
        {
            return Err(DirectoryError::NoSuchKey { id: id.to_owned() });
        }
        let write = self.replicated.api_key_removed(id);
        let mut records = vec![Record::ApiKeyDeleted { id: id.to_owned() }];
        records.extend(self.replicated.records(&write));
        self.durable(records, Some(write)).await?;
        self.api_keys
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(id);
        Ok(())
    }

    /// Every API key, by identifier, without secrets.
    #[must_use]
    pub fn api_keys(&self) -> Vec<ApiKeyView> {
        self.api_keys
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(ApiKeyView::from)
            .collect()
    }

    /// One API key, by identifier, without its secret.
    #[must_use]
    pub fn api_key(&self, id: &str) -> Option<ApiKeyView> {
        self.api_keys
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .map(ApiKeyView::from)
    }

    /// What a presented key may do, when it is one this directory
    /// minted and has not revoked.
    ///
    /// `None` for text that is not a key, a key nobody holds, or one
    /// whose stored scope this build cannot read.
    #[must_use]
    pub fn scope_of_key(&self, presented: &str) -> Option<Scope> {
        let id = key::id_of(presented)?;
        let held = self.api_keys.read().unwrap_or_else(PoisonError::into_inner);
        let record = held.get(id)?;
        key::matches(record, presented).then(|| Scope::parse(&record.scope))?
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
        let name = match write.key() {
            MetadataKey::User { name } => name,
            MetadataKey::Acl { id } => return self.apply_replicated_acl(id, write).await,
            MetadataKey::ApiKey { id } => return self.apply_replicated_key(id, write).await,
            other => {
                tracing::error!(
                    kind = other.kind(),
                    "a register that is not a user, an access control rule or an API key \
                     reached the directory"
                );
                return Ok(());
            }
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

    /// A peer's access control rule, applied the way a peer's user is.
    async fn apply_replicated_acl(
        &self,
        id: &str,
        write: &MetadataWrite,
    ) -> Result<(), DirectoryError> {
        let _changing = self.changing.lock().await;
        match write.register().value() {
            Some(MetadataValue::Acl(rule)) => {
                self.durable(vec![Record::AclPut(rule.clone())], None)
                    .await?;
                self.acl
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(id.to_owned(), (**rule).clone());
            }
            Some(other) => {
                tracing::error!(
                    kind = other.kind(),
                    "an access control rule's register carried something that is not one"
                );
            }
            None => {
                self.durable(vec![Record::AclDeleted { id: id.to_owned() }], None)
                    .await?;
                self.acl
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(id);
            }
        }
        Ok(())
    }

    /// A peer's API key, applied the way a peer's rule is.
    async fn apply_replicated_key(
        &self,
        id: &str,
        write: &MetadataWrite,
    ) -> Result<(), DirectoryError> {
        let _changing = self.changing.lock().await;
        match write.register().value() {
            Some(MetadataValue::ApiKey(key)) => {
                self.durable(vec![Record::ApiKeyPut(key.clone())], None)
                    .await?;
                self.api_keys
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(id.to_owned(), (**key).clone());
            }
            Some(other) => {
                tracing::error!(
                    kind = other.kind(),
                    "an API key's register carried something that is not one"
                );
            }
            None => {
                self.durable(vec![Record::ApiKeyDeleted { id: id.to_owned() }], None)
                    .await?;
                self.api_keys
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(id);
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

/// Whether `rule` is one this broker can consult. Section 12.1.
///
/// # Errors
///
/// Names the field, for the configuration and the API alike.
pub fn check_rule(rule: &AclRecord) -> Result<(), DirectoryError> {
    if rule.id.is_empty() || rule.id.chars().any(char::is_control) {
        return Err(DirectoryError::BadRule {
            field: "id",
            why: "an identifier cannot be empty or contain a control character".to_owned(),
        });
    }
    acl::Subject::parse(&rule.subject).map_err(|why| DirectoryError::BadRule {
        field: "subject",
        why,
    })?;
    mqtt_codec::topic::validate_filter(&rule.filter, &mqtt_codec::Limits::default()).map_err(
        |why| DirectoryError::BadRule {
            field: "filter",
            why: why.to_string(),
        },
    )?;
    if rule.publish.is_none() && rule.subscribe.is_none() {
        return Err(DirectoryError::BadRule {
            field: "publish",
            why: "a rule with no opinion on publishing or subscribing decides nothing; set \
                  at least one"
                .to_owned(),
        });
    }
    Ok(())
}

/// The rules in force on this node: the configured ones and, on a node
/// with a store, the directory's, consulted together in one order.
///
/// One order rather than "the file first", because `order` is the one
/// knob an operator has and it has to mean what it says wherever the
/// rule was written: a narrow deny in the API at order 10 must sit
/// above a broad allow in the file at order 100.
#[derive(Debug)]
pub struct RulesInForce {
    configured: Vec<AclRecord>,
    directory: Option<Arc<Directory>>,
}

impl RulesInForce {
    /// Over `configured` and, when the node has one, `directory`.
    #[must_use]
    pub fn new(mut configured: Vec<AclRecord>, directory: Option<Arc<Directory>>) -> Arc<Self> {
        acl::in_order(&mut configured);
        Arc::new(Self {
            configured,
            directory,
        })
    }

    /// The access control hook over these rules.
    #[must_use]
    pub fn hook(self: &Arc<Self>) -> Arc<dyn Hook> {
        acl::AclHook::over(Arc::clone(self) as Arc<dyn acl::RuleSource>)
    }
}

impl acl::RuleSource for RulesInForce {
    fn rules(&self) -> Vec<AclRecord> {
        let mut rules = self.configured.clone();
        if let Some(directory) = &self.directory {
            rules.extend(directory.acl());
        }
        acl::in_order(&mut rules);
        rules
    }
}

impl sdmqtt_server::scram::Verifiers for Directory {
    fn verifier(&self, name: &str) -> Option<ScramVerifierRecord> {
        Directory::verifier(self, name)
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
