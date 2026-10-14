# Authentication and Access Control Implementation Plan (milestone 7, part 2 of 3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A broker that knows who is connecting and what they may do: users
with argon2 passwords or SCRAM-SHA-256 verifiers, access control rules that
deny by default once one exists, API keys with scopes in place of the one
interim token, the refusal of a client publishing to a dollar-prefixed
topic, the two rate limiters of section 10.3, and the Settings page that
manages all of it.

**Architecture:** A new `directory` crate holds users, access control rules
and API keys as replicated metadata — the same last-writer-wins registers
section 8.2 already carries rules in — and installs itself on the broker as
a section 9.3 hook for `authenticate`, `authorize_publish` and
`authorize_subscribe`. SCRAM runs in the connection shell, where the AUTH
exchange lives, against verifiers the directory computes when a password is
set. The admin API grows three resources and the bearer token becomes one
API key among several. Nothing about authentication is a special case in the
publish path: the hook interface was built so this would not be.

**Tech Stack:** `argon2` (one new direct dependency, PHC strings), SCRAM
primitives from `aws-lc-rs` which the cluster crate already links (PBKDF2,
HMAC-SHA-256, SHA-256), the `storage` crate's records and `Secret`, the
`cluster` crate's `MetadataKey`/`MetadataValue`/`MetadataWrite`, the
`broker-core` hook interface, axum in the admin crate, Preact in the
interface.

**Spec:** `docs/superpowers/specs/2026-09-19-sd-mqtt-design.md`, sections
12.1 (authentication, access control), 12.2 (SCRAM-SHA-256 over AUTH, the
verifier as a secret), 12.3 (the secret wrapper, the per-address limiter, the
dollar-topic refusal and its conformance baseline line), 10.3 (the two
limiters), 11.1 (users, access control rules and API keys as resources;
API keys replacing the interim token), 11.2 (the Settings page), 8.2
(users, access control rules and API keys are replicated metadata), 9.3
(the hook interface), 16 (milestone 7's exit criteria: "a SCRAM client
authenticates and re-authenticates").

**Two decisions taken before this plan**, confirmed with the owner:

- Users, access control rules and API keys **replicate** through section
  8.2's metadata, as rules do, so a user created on one node connects on
  any. Each is a register keyed by its identifier; a delete is a tombstone.
- The configured bearer token **stays as a bootstrap key** with admin scope,
  so an existing configuration keeps working after the upgrade and an
  operator can mint real keys with it. Section 11.1 calls the token an
  interim; it now describes a key with no identifier of its own.

## Global Constraints

- `docs/conventions.md`, all eight plus 7b and 7c. Prove every test can
  fail (4); restore mutations byte-identical with a newer timestamp (7b);
  check free space before calling a test flaky (7c); walk the spec sections
  at the close (8).
- Direct dependencies across broker crates: **under 35** (29 today; this
  plan adds `argon2` and nothing else — SCRAM uses `aws-lc-rs`, already
  direct in the cluster crate).
- Release binary, stripped, link-time optimised: **under 15 MB** (9.2 MB).
- **Secrets never print.** Passwords, keys, tokens and verifiers travel in a
  wrapper whose `Debug` says `<redacted>` (section 12.3); a verifier is
  never returned by the API (12.2); a key's secret is shown once, at
  creation, and only its hash is stored.
- Every discard path has a metric and, where a hook can see it, a hook
  (section 11.3). A refused connect, a refused publish, a refused
  subscribe and a rate-limited publish are discards.
- Version 3.1.1 has no reason codes on most acknowledgements and none on
  DISCONNECT: a refusal that v5 answers with a code, 3.1.1 answers with the
  one code it has (0x80 on SUBACK) or by closing the socket (section 13).
- Postcard variant indices are wire format: **new record and metadata
  variants go at the end of their enums** and the stored format version
  moves from 10 to 11, with the refusal message updated.
- The conformance baseline changes only with a classified line:
  `test_subscribe_failure` leaves it (deferred to here), `test_dollar_topics`
  joins it as `assumption` (section 12.3 says so).
- Reads on the admin API stay open as they are today; mutating requests
  need a key with `write` scope and directory mutations need `admin`.
- Derive SCRAM from RFC 5802 and RFC 7677, not from memory (convention 3):
  the message grammar, the proof, the signature, `SaltedPassword =
  Hi(Normalize(password), salt, i)`, `ClientKey = HMAC(SaltedPassword,
  "Client Key")`, `StoredKey = H(ClientKey)`, `ServerKey = HMAC(SaltedPassword,
  "Server Key")`. RFC 7677's test vector pins the implementation.

## Review Focus

Inputs the spec implies and no task's tests would otherwise exercise, most
likely to bite first. Each has its test added to the owning task.

1. **A password with a comma or an equals sign, or a username with one.**
   SCRAM escapes `,` and `=` in names as `=2C` and `=3D`; a verifier
   computed from an unescaped name, or a name compared without unescaping,
   authenticates nobody. (Task 2.)
2. **An access control rule whose filter has a wildcard, matched against a
   publish topic.** `fleet/+/telemetry` permits publishing to
   `fleet/7/telemetry` and not to `fleet/7/status`; matching is the topic
   crate's, not a string compare. (Task 3.)
3. **A client that re-authenticates with a wrong proof mid-session.** The
   session is disconnected with 0x8C, not left connected as the old user,
   and the attempt counts against the address. (Task 2.)
4. **An API key presented after it was revoked on another node.** The
   revoke is a tombstone that replicates; until it arrives the key works
   on the other node, and the page says keys are eventually consistent.
   The test pins that a revoke on one node refuses on the same node at
   once and on a peer once the register has arrived. (Task 4.)
5. **A session that exceeds the publish rate with QoS 0 messages**, which
   carry no acknowledgement to refuse on. The connection is disconnected
   with 0x96 (v5) or closed (3.1.1), counted, and the session's queue is
   untouched. (Task 5.)

---

## Read first

- `crates/rules/src/live.rs`: `Registry::install`, `durable`,
  `apply_replicated`. The directory follows this shape line for line —
  one lock around a change, records and a `MetadataWrite` written twice so
  the broadcast waits for the store, the peer's install stamping nothing.
- `crates/storage/src/record.rs`: `Record`, `MetadataValueRecord`,
  `FORMAT_VERSION`, `Secret`; `record/tests.rs`'s golden module for how a
  variant index is pinned.
- `crates/cluster/src/metadata.rs`: `MetadataKey`, `MetadataValue`,
  `MetadataWrite`, `kind()`.
- `crates/broker-core/src/hooks.rs`: `Hook`, `Credentials`, `ClientInfo`,
  `Decision`, `Defaults`, `Hooks::authenticate` and the two `authorize_*`.
- `crates/server/src/connection.rs`: `serve` from the CONNECT read to
  `Attached`, `refuse`, `Attached`'s packet loop. SCRAM is a second kind of
  CONNECT and a new packet in the loop.
- `crates/admin/src/resources.rs`: `Authorized`, `secret_eq`, and
  `crates/admin/src/rules.rs` as the template for a CRUD resource.
- `crates/sdmqtt/src/replication.rs` near line 470: where a peer's
  metadata writes are routed by key kind.
- RFC 5802 §3 and §5, RFC 7677 §3 (the test vector), MQTT 5 §4.12 and
  §4.12.1 (enhanced authentication and re-authentication).

## Five things settled before you start

**The directory is a hook, not a special case.** `authenticate` asks the
directory for a password check; `authorize_publish` and
`authorize_subscribe` ask it for a verdict. The broker's publish and
subscribe paths already call the chain and do not change. What changes is
that `ClientInfo` carries the authenticated username, which the session
remembers for its connection and no longer.

**Deny by default once any rule exists, and allow everything before.** A
broker with no access control rules serves every publish and subscribe, as
it did through milestone 6, which is what the conformance suite and every
existing test rely on. The first rule written flips the default to deny:
from then on, a publish or a subscribe is allowed only by a rule that names
it. The page says this in words above the table, because an operator who
writes one rule and locks everyone else out should have been told.

**Anonymous is a switch, not an inference.** `auth.anonymous = true` (the
default) lets a client with no username connect; `false` refuses it with
0x87. It does not flip itself when the first user is created, because a
setting that changes under an operator is a setting they cannot reason
about. The page shows the switch's state beside the user list.

**SCRAM lives in the connection, the verifier in the directory.** The
exchange is MQTT 5's AUTH packets, which only the shell sees; the arithmetic
is pure functions in the directory crate, tested against RFC 7677's vector.
The shell asks the directory for a user's verifier by name and runs the
exchange; the directory never sees a socket.

**The bootstrap token is an API key with no row.** `Authorized` accepts
either a configured token or a key the directory knows, and attaches the
scope to the request. The token's scope is `admin`. The page lists keys the
directory holds; the token is described in a sentence, not a row, because
it has no identifier to key a row on.

---

### Task 1: The directory — users with argon2 passwords, replicated

**Files:**
- Create: `crates/directory/Cargo.toml`, `crates/directory/src/lib.rs`,
  `crates/directory/src/user.rs`, `crates/directory/src/store.rs`
- Modify: `Cargo.toml` (workspace members), `crates/storage/src/record.rs`
  (`Record::UserPut`, `Record::UserDeleted`, `MetadataValueRecord::User`,
  `UserRecord`, `FORMAT_VERSION = 11`), `crates/storage/src/restore.rs`
  (`Restored.users`), `crates/storage/src/{memory,redb}.rs` (apply the two
  records), `crates/cluster/src/metadata.rs` (`MetadataKey::User`,
  `MetadataValue::User`, `kind()`), `crates/broker-core/src/hooks.rs`
  (`ClientInfo.username`), `crates/broker-core/src/session/state.rs`
  (`Session.username`), `crates/broker-core/src/broker/mod.rs` (set and
  carry it), `crates/sdmqtt/src/{main,replication,config}.rs`
- Test: `crates/directory/src/user.rs` tests, `crates/storage/src/record/tests.rs`
  (golden vectors for the two records), `crates/directory/tests/replicated.rs`

**Interfaces:**
- Produces: `storage::record::UserRecord { name: String, password:
  Option<Secret> /* PHC string */, scram: Option<ScramVerifierRecord>,
  created_at: u64 }`; `ScramVerifierRecord { salt: Vec<u8>, iterations:
  u32, stored_key: Secret /* base64 */, server_key: Secret }`;
  `Record::UserPut(Box<UserRecord>)`, `Record::UserDeleted { name: String }`;
  `MetadataKey::User { name: String }`, `MetadataValue::User(Box<UserRecord>)`;
  `directory::Directory::new(writer: Arc<Writer>, shared: Arc<Shared>,
  settings: Settings) -> Self`; `Directory::put_user(&self, name: &str,
  password: &Secret, scram: bool) -> Result<UserView, DirectoryError>`;
  `Directory::delete_user(&self, name: &str) -> Result<(), DirectoryError>`;
  `Directory::users(&self) -> Vec<UserView>` (sorted by name, no secrets);
  `Directory::verifier(&self, name: &str) -> Option<ScramVerifierRecord>`;
  `Directory::check_password(&self, name: &str, password: &[u8]) -> bool`;
  `Directory::apply_replicated(&self, write: &MetadataWrite) -> Result<(),
  DirectoryError>`; `Directory::hook(self: &Arc<Self>) -> Arc<dyn Hook>`;
  `directory::Settings { anonymous: bool, scram_iterations: u32 }` with
  defaults `true` and `4096`; `hooks::ClientInfo { username:
  Option<&'a str>, .. }`; `Credentials` unchanged.
- Consumes: `rules::live::Registry`'s shape; `Shared::broadcast_when_durable`;
  `Writer::apply_durable_twice`; `MetadataWrite::new(key, register)` and
  the node's `stamp()`.

- [ ] **Step 1: The records, with golden vectors**

In `crates/storage/src/record.rs`, at the **end** of `Record`:

```rust
    /// A user exists, or its credentials changed. Section 12.1.
    ///
    /// Carries the argon2 string and the SCRAM verifier, never a
    /// password: both are derived when the password is set, because a
    /// verifier cannot be derived from an argon2 hash (section 12.2).
    UserPut(Box<UserRecord>),
    /// A user is gone.
    UserDeleted {
        /// Its name.
        name: String,
    },
```

and at the end of `MetadataValueRecord`: `User(Box<UserRecord>)`. The
record types:

```rust
/// A user, as stored. Section 12.1 and 12.2.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserRecord {
    /// The name a client presents. Compared exactly.
    pub name: String,
    /// The argon2id hash in PHC string form, when the user has a
    /// password. `None` for a user enrolled in SCRAM alone.
    pub password: Option<Secret>,
    /// The SCRAM-SHA-256 verifier, when the user is enrolled.
    pub scram: Option<ScramVerifierRecord>,
    /// Seconds since the epoch, for the page.
    pub created_at: u64,
}

/// What SCRAM-SHA-256 needs to verify a client without the password.
/// Section 12.2: "a weaker secret at rest than an argon2 hash, because
/// possession of the stored key permits impersonating that client", so
/// both keys are `Secret`s and the API never returns them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScramVerifierRecord {
    /// Random, 16 bytes, per user.
    pub salt: Vec<u8>,
    /// PBKDF2 rounds. Section 12.2: configurable, defaults to a value
    /// constrained clients can afford.
    pub iterations: u32,
    /// `H(ClientKey)`, base64.
    pub stored_key: Secret,
    /// `HMAC(SaltedPassword, "Server Key")`, base64.
    pub server_key: Secret,
}
```

`FORMAT_VERSION` becomes 11; the refusal message's test and the CHANGELOG
"Before you upgrade" paragraph say 11. In `record/tests.rs`'s golden
module add vectors for `UserPut` and `UserDeleted` at their new indices,
derived by hand from postcard's rules (variant index as a varint, then
fields in order; a `String` is a varint length then bytes; `Option` is a
0 or 1 byte then the value), the way the file's existing vectors are. The
memory and redb backends apply the two records to a `users` table keyed
by name; `restore.rs` gains `pub users: Vec<UserRecord>` in name order.
`storage/tests/backends.rs` gets a case: put, restart, the user is there
with its verifier; delete, restart, it is gone.

- [ ] **Step 2: The metadata kinds**

`MetadataKey::User { name: String }` and `MetadataValue::User(Box<UserRecord>)`
at the end of each enum; `kind()` returns `"user"`. The conversions
between `MetadataValue` and `MetadataValueRecord` gain the arm. The
cluster crate's own golden vectors for the register wire format, if any
cover the enum, gain the index.

- [ ] **Step 3: Write the failing directory tests**

`crates/directory/src/user.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// A password set is an argon2 string and a verifier, and checks.
    #[test]
    fn a_password_set_checks_and_the_wrong_one_does_not() {
        let user = UserRecord::with_password("alice", &Secret::new("hunter2"), 4096, now());
        assert!(user.password.as_ref().expect("hashed").expose().starts_with("$argon2id$"));
        assert!(check_password(&user, b"hunter2"));
        assert!(!check_password(&user, b"hunter3"));
        assert!(!check_password(&user, b""));
        let scram = user.scram.as_ref().expect("enrolled");
        assert_eq!(scram.salt.len(), 16);
        assert_eq!(scram.iterations, 4096);
    }

    /// Two users with one password hash differently: the salt is per
    /// user, for both forms.
    #[test]
    fn two_users_with_one_password_share_nothing_stored() {
        let one = UserRecord::with_password("alice", &Secret::new("same"), 4096, now());
        let two = UserRecord::with_password("bob", &Secret::new("same"), 4096, now());
        assert_ne!(one.password, two.password);
        assert_ne!(one.scram.as_ref().unwrap().salt, two.scram.as_ref().unwrap().salt);
        assert_ne!(
            one.scram.as_ref().unwrap().stored_key,
            two.scram.as_ref().unwrap().stored_key
        );
    }

    /// The record's Debug says nothing about either secret.
    #[test]
    fn a_user_record_debugs_without_its_secrets() {
        let user = UserRecord::with_password("alice", &Secret::new("hunter2"), 4096, now());
        let printed = format!("{user:?}");
        assert!(!printed.contains("argon2"), "{printed}");
        assert!(printed.contains("<redacted>"), "{printed}");
    }

    /// A view carries no secret, only whether each form is set.
    #[test]
    fn a_view_says_which_credentials_exist_and_shows_neither() {
        let user = UserRecord::with_password("alice", &Secret::new("hunter2"), 4096, now());
        let view = UserView::from(&user);
        assert_eq!(view.name, "alice");
        assert!(view.has_password);
        assert!(view.scram);
        let json = serde_json::to_string(&view).expect("serialises");
        assert!(!json.contains("argon2") && !json.contains("stored_key"), "{json}");
    }
}
```

`UserRecord::with_password(name, password, iterations, created_at)` hashes
with `argon2::Argon2::default()` (argon2id, the crate's defaults, a random
16-byte salt via `password_hash::SaltString::generate`) into a PHC string,
and computes the verifier with `scram::verifier(password, salt, iterations)`
from task 2's module — for this task, a function in `user.rs` that
computes `SaltedPassword` with `aws_lc_rs::pbkdf2::derive(PBKDF2_HMAC_SHA256,
NonZeroU32::new(iterations), &salt, password, &mut out)`, then
`ClientKey = hmac(SaltedPassword, b"Client Key")`, `StoredKey =
sha256(ClientKey)`, `ServerKey = hmac(SaltedPassword, b"Server Key")`
(`aws_lc_rs::hmac::sign(&Key::new(HMAC_SHA256, &salted), ...)`,
`aws_lc_rs::digest::digest(&SHA256, ...)`), base64 via the `base64` crate
already direct in the rules crate — add it as a dependency of this crate;
it is not a new distinct one. `check_password` parses the PHC string with
`argon2::PasswordHash::new` and verifies with `Argon2::default().verify_password`.

`UserView { name, has_password: bool, scram: bool, created_at: u64 }`,
`Serialize`.

- [ ] **Step 4: The directory itself**

`crates/directory/src/lib.rs`:

```rust
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
//! interface. `authenticate` is asked on every CONNECT; the two
//! `authorize_*` are asked on every publish and subscribe once task 3
//! gives it rules to answer with. Nothing in the broker's publish path
//! knows users exist.

pub struct Directory {
    writer: Arc<Writer>,
    shared: Arc<Shared>,
    settings: Settings,
    users: RwLock<BTreeMap<String, UserRecord>>,
    changing: tokio::sync::Mutex<()>,
    metrics: Arc<Metrics>,
}
```

`put_user`: under `changing`, build the record, write
`Record::UserPut` and the `MetadataWrite` (key `User { name }`, value
`User(record)`, stamp from `shared.metadata().stamp()` — find how the rules
registry gets its stamp and use the same path) with
`writer.apply_durable_twice`, `shared.broadcast_when_durable(theirs,
vec![write])`, await `mine`, then insert into `users`. `delete_user`: the
same with `UserDeleted` and a tombstone register. `apply_replicated`:
match the key, write the record **without** stamping or broadcasting,
insert or remove. `check_password`: read lock, `user.password` present and
`check_password(user, bytes)`. `verifier`: a clone of the record's
`scram`. The hook:

```rust
impl Hook for DirectoryHook {
    fn name(&self) -> &str { "directory" }

    fn authenticate(&self, credentials: &Credentials<'_>) -> Decision {
        match (credentials.username, credentials.password) {
            // Anonymous: the switch decides, and a user that does not
            // exist is indistinguishable from a wrong password.
            (None, _) => if self.directory.settings.anonymous { Decision::Next } else { Decision::Deny },
            (Some(name), Some(password)) => {
                if self.directory.check_password(name, password) { Decision::Allow } else { Decision::Deny }
            }
            // A name and no password is not a password login; SCRAM
            // comes through the AUTH exchange, not here.
            (Some(_), None) => Decision::Deny,
        }
    }
}
```

`Decision::Next` for anonymous when allowed, so a later hook (a
connector's) can still refuse, and the configured `Defaults.authenticate`
applies; `Allow` on a good password ends the chain.

The rest of task 1 and the tasks after it are executed from the spec and
the RFCs directly, each with its tests, its mutation proof and the full
gate, and ticked here as it lands:

- [x] **Task 1** — the directory crate: users with argon2 passwords and
  SCRAM verifiers, replicated as metadata, installed as the authenticate
  hook; `auth.anonymous`; `ClientInfo.username`; format version 11.
- [x] **Task 2** — SCRAM-SHA-256 over MQTT 5's AUTH exchange, including
  re-authentication; pinned by RFC 7677's test vector; a wrong method is
  0x8C; names with `,` or `=` are escaped per RFC 5802.
- [ ] **Task 3** — access control rules, deny by default once one exists,
  through the two authorize hooks with the topic crate's matching; the
  dollar-topic refusal on connections; `test_subscribe_failure` leaves the
  conformance baseline and `test_dollar_topics` joins it as `assumption`.
- [ ] **Task 4** — API keys with `read`, `write` and `admin` scopes,
  hashed at rest and shown once; the configured token stays as a bootstrap
  key with admin scope; revocation replicates as a tombstone.
- [ ] **Task 5** — section 10.3's two limiters: the per-session publish
  rate (a cell-rate field in the session, 0x96 on v5, a close on 3.1.1)
  and the per-address authentication-failure limiter swept by the tick.
- [ ] **Task 6** — the Settings page: users, access control rules, API
  keys and listeners, checked against a real broker.
- [ ] **Task 7** — the closing walk of sections 12, 10.3, 11.1 and 11.2;
  README, CHANGELOG and spec say what shipped.
