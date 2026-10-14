//! API keys: what a request to the admin listener presents. Section 11.1.
//!
//! # Shown once, stored as a digest
//!
//! A key is minted from thirty-two random bytes and handed to the caller
//! in the answer that created it, and nowhere else: the directory keeps
//! the SHA-256 of the text and compares that. Thirty-two random bytes
//! are not guessable, so the digest needs no salt and no work factor —
//! a password hash is slow because a password is weak, and a key is not
//! — and a request is checked in the time one hash takes.
//!
//! # The text
//!
//! `sdmqtt_<id>_<secret>`: a fixed prefix so a leaked one is
//! recognisable by the tools that scan for leaks, the identifier in
//! clear so the digest to compare against is found without trying every
//! key, and the secret, which is the only part that is one. The
//! identifier is what a listing shows and what a revocation names.
//!
//! The identifier is hex, not base64: URL-safe base64 spells with `_`,
//! which is the separator, and an identifier that held one would be
//! read back cut short. The secret may hold one, since everything after
//! the first separator is the secret.

use aws_lc_rs::{digest, rand};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use storage::record::{ApiKeyRecord, Secret};

/// What every key's text begins with.
pub const PREFIX: &str = "sdmqtt_";
/// Bytes of secret.
const SECRET_LEN: usize = 32;
/// Bytes of identifier, before encoding as twelve hex digits.
const ID_LEN: usize = 6;

/// What a key may do. Section 11.1: a mutating request needs a key with
/// an appropriate scope.
///
/// Ordered: `admin` may do what `write` may, and `write` what `read`
/// may. The comparison is the whole of the authorization check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    /// Open taps and read what the read resources answer anyway.
    Read,
    /// Publish, and write rules.
    Write,
    /// Everything, including users, access control rules and keys.
    Admin,
}

impl Scope {
    /// The word the API uses.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Admin => "admin",
        }
    }

    /// The scope a stored record names, if it is one this build knows.
    ///
    /// `None` for a word a newer node wrote: a key whose scope cannot
    /// be read is a key that may do nothing, rather than one that may
    /// do something guessed.
    #[must_use]
    pub fn parse(word: &str) -> Option<Self> {
        match word {
            "read" => Some(Self::Read),
            "write" => Some(Self::Write),
            "admin" => Some(Self::Admin),
            _ => None,
        }
    }
}

impl std::fmt::Display for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A key as the API and the page list one: everything but the secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApiKeyView {
    /// The identifier, as the key's text carries it.
    pub id: String,
    /// What an operator called it.
    pub name: String,
    /// What it may do.
    pub scope: String,
    /// Seconds since the epoch.
    pub created_at: u64,
}

impl From<&ApiKeyRecord> for ApiKeyView {
    fn from(key: &ApiKeyRecord) -> Self {
        Self {
            id: key.id.clone(),
            name: key.name.clone(),
            scope: key.scope.clone(),
            created_at: key.created_at,
        }
    }
}

/// A key just minted: its record, and the one copy of its text.
#[derive(Debug)]
pub struct Minted {
    /// What the directory stores.
    pub record: ApiKeyRecord,
    /// What the caller is shown, once.
    pub text: Secret,
}

/// Mint a key for `name` with `scope`.
///
/// # Panics
///
/// If the platform's random source fails, which is not a condition a
/// broker can do anything with but stop.
#[must_use]
pub fn mint(name: &str, scope: Scope, created_at: u64) -> Minted {
    let mut id_bytes = [0u8; ID_LEN];
    rand::fill(&mut id_bytes).expect("the random source answers");
    let mut secret = [0u8; SECRET_LEN];
    rand::fill(&mut secret).expect("the random source answers");
    let id: String = id_bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    let url = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let text = format!("{PREFIX}{id}_{}", url.encode(secret));
    let record = ApiKeyRecord {
        id,
        name: name.to_owned(),
        scope: scope.as_str().to_owned(),
        digest: digest_of(&text),
        created_at,
    };
    Minted {
        record,
        text: Secret::new(text),
    }
}

/// The identifier a presented key names, when the text has the shape
/// of one. Nothing about whether it is valid: that is the digest's.
#[must_use]
pub fn id_of(presented: &str) -> Option<&str> {
    let rest = presented.strip_prefix(PREFIX)?;
    let (id, secret) = rest.split_once('_')?;
    (!id.is_empty() && !secret.is_empty()).then_some(id)
}

/// What is stored of a key's text: base64 of its SHA-256.
#[must_use]
pub fn digest_of(text: &str) -> Secret {
    let hashed = digest::digest(&digest::SHA256, text.as_bytes());
    Secret::new(base64::engine::general_purpose::STANDARD.encode(hashed.as_ref()))
}

/// Whether `presented` is the key `record` was minted as.
///
/// The digests are compared in full whatever the first differing byte,
/// so the time taken says nothing about how close a guess was. Both
/// are base64 of thirty-two bytes and so the same length; a stored one
/// that is not — a record a newer node wrote differently — is simply
/// never equal.
#[must_use]
pub fn matches(record: &ApiKeyRecord, presented: &str) -> bool {
    let presented = digest_of(presented);
    let (mine, theirs) = (
        record.digest.expose().as_bytes(),
        presented.expose().as_bytes(),
    );
    let mut difference = mine.len() ^ theirs.len();
    for (index, byte) in theirs.iter().enumerate() {
        let against = if mine.is_empty() {
            0
        } else {
            mine[index % mine.len()]
        };
        difference |= usize::from(byte ^ against);
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every minted key names its own identifier, and checks.
    ///
    /// Many of them, because an identifier whose alphabet held the
    /// separator would be cut short about one time in eight, and one
    /// key would pass that by luck.
    #[test]
    fn every_minted_key_carries_its_identifier_and_matches_its_record() {
        for _ in 0..512 {
            let minted = mint("ci", Scope::Write, 1);
            let text = minted.text.expose();
            assert_eq!(id_of(text), Some(minted.record.id.as_str()), "{text}");
            assert!(matches(&minted.record, text));
        }
    }

    /// The record keeps a digest, not the text, and a near miss fails.
    #[test]
    fn the_record_holds_no_secret_and_a_near_miss_does_not_match() {
        let minted = mint("ci", Scope::Read, 1);
        let text = minted.text.expose();
        let secret = text.rsplit_once('_').map(|(_, s)| s).unwrap_or_default();
        assert!(!minted.record.digest.expose().contains(secret));
        assert!(!matches(&minted.record, &text[..text.len() - 1]));
        assert!(!matches(&minted.record, &format!("{text}x")));
        assert_eq!(id_of("sdmqtt_"), None);
        assert_eq!(id_of("sdmqtt_abc_"), None);
        assert_eq!(id_of("bearer_abc_def"), None);
    }

    /// The scopes are ordered, and an unknown word is no scope.
    #[test]
    fn scopes_are_ordered_and_parse_only_their_own_words() {
        assert!(Scope::Read < Scope::Write && Scope::Write < Scope::Admin);
        for scope in [Scope::Read, Scope::Write, Scope::Admin] {
            assert_eq!(Scope::parse(scope.as_str()), Some(scope));
        }
        assert_eq!(Scope::parse("root"), None);
    }
}
