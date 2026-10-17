//! A user's credentials: an argon2id hash for a password login and a
//! SCRAM-SHA-256 verifier for the AUTH exchange. Sections 12.1 and 12.2.
//!
//! Both are derived when the password is set and the password is kept
//! by neither: a verifier cannot be derived from an argon2 hash, so the
//! one moment the password is in hand is the moment both are made.

use argon2::password_hash::{PasswordHasher as _, PasswordVerifier as _};
use aws_lc_rs::rand;
use serde::Serialize;
use storage::record::{ScramVerifierRecord, Secret, UserRecord};

/// Bytes of salt, for both forms.
const SALT_LEN: usize = 16;

/// Build a user with `password`: an argon2 hash always, and a SCRAM
/// verifier when `scram` says the user is enrolled.
///
/// Section 12.2: a verifier is a weaker secret at rest than the hash —
/// whoever holds its stored key can impersonate the user — so it exists
/// only for users explicitly enrolled, and is made here or never, since
/// it cannot be derived later from the hash.
///
/// # Panics
///
/// If the platform's random source fails, which is not a condition a
/// broker can do anything with but stop.
#[must_use]
pub fn with_password(
    name: &str,
    password: &Secret,
    iterations: u32,
    created_at: u64,
    scram: bool,
) -> UserRecord {
    let mut salt = [0u8; SALT_LEN];
    rand::fill(&mut salt).expect("the random source answers");
    let hash = argon2::Argon2::default()
        .hash_password_with_salt(password.expose().as_bytes(), &salt)
        .expect("argon2 hashes any password with a sixteen-byte salt")
        .to_string();
    let scram = scram.then(|| {
        let mut scram_salt = [0u8; SALT_LEN];
        rand::fill(&mut scram_salt).expect("the random source answers");
        verifier(password.expose(), &scram_salt, iterations)
    });
    UserRecord {
        name: name.to_owned(),
        password: Some(Secret::new(hash)),
        scram,
        created_at,
    }
}

/// A user nobody is, with a password nobody knows, for a login naming
/// a user the directory does not hold.
///
/// Verifying against it costs what verifying against a real hash costs,
/// so an unknown name and a wrong password answer in the same time as
/// well as with the same word; returning early for the unknown name
/// would let a caller list the users by timing refusals. The password
/// is random, so the verification is a refusal by construction, the
/// same way the SCRAM exchange's decoy verifier refuses: there is no
/// separate "was this a real user" flag for a mutation to drop.
///
/// # Panics
///
/// If the platform's random source fails, as [`with_password`] does.
#[must_use]
pub fn decoy() -> UserRecord {
    let mut password = [0u8; 32];
    rand::fill(&mut password).expect("the random source answers");
    let password: String = password.iter().map(|byte| format!("{byte:02x}")).collect();
    with_password("", &Secret::new(password), 0, 0, false)
}

/// Whether `password` is the one `user`'s hash was made from.
///
/// False for a user with no password: SCRAM-only users log in through
/// the AUTH exchange, and a password login for one is a wrong password.
/// A caller that must not say so by timing hands such a user to
/// [`decoy`] rather than here; see `Directory::check_password`.
#[must_use]
pub fn check_password(user: &UserRecord, password: &[u8]) -> bool {
    let Some(hash) = &user.password else {
        return false;
    };
    let Ok(parsed) = argon2::password_hash::phc::PasswordHash::new(hash.expose()) else {
        return false;
    };
    argon2::Argon2::default()
        .verify_password(password, &parsed)
        .is_ok()
}

/// RFC 5802 §3's two keys, by the server crate's one implementation,
/// so the derivation that makes a verifier and the one that checks a
/// proof cannot drift apart.
#[must_use]
pub fn verifier(password: &str, salt: &[u8], iterations: u32) -> ScramVerifierRecord {
    sdmqtt_server::scram::verifier(password.as_bytes(), salt, iterations)
}

/// A user as the API and the page see it: which credentials exist, and
/// neither of them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UserView {
    /// The name.
    pub name: String,
    /// Whether a password login is possible.
    pub has_password: bool,
    /// Whether the user is enrolled in SCRAM-SHA-256.
    pub scram: bool,
    /// Seconds since the epoch.
    pub created_at: u64,
}

impl From<&UserRecord> for UserView {
    fn from(user: &UserRecord) -> Self {
        Self {
            name: user.name.clone(),
            has_password: user.password.is_some(),
            scram: user.scram.is_some(),
            created_at: user.created_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A password set is an argon2 string and a verifier, and checks.
    #[test]
    fn a_password_set_checks_and_the_wrong_one_does_not() {
        let user = with_password("alice", &Secret::new("hunter2"), 4096, 7, true);
        assert!(user
            .password
            .as_ref()
            .expect("hashed")
            .expose()
            .starts_with("$argon2id$"));
        assert!(check_password(&user, b"hunter2"));
        assert!(!check_password(&user, b"hunter3"));
        assert!(!check_password(&user, b""));
        let scram = user.scram.as_ref().expect("enrolled");
        assert_eq!(scram.salt.len(), SALT_LEN);
        assert_eq!(scram.iterations, 4096);
    }

    /// Two users with one password hash differently: the salt is per
    /// user, for both forms.
    #[test]
    fn two_users_with_one_password_share_nothing_stored() {
        let one = with_password("alice", &Secret::new("same"), 4096, 1, true);
        let two = with_password("bob", &Secret::new("same"), 4096, 1, true);
        assert_ne!(one.password, two.password);
        let (a, b) = (one.scram.unwrap(), two.scram.unwrap());
        assert_ne!(a.salt, b.salt);
        assert_ne!(a.stored_key, b.stored_key);
        assert_ne!(a.server_key, b.server_key);
    }

    /// A user without a password form is not a password login.
    #[test]
    fn a_user_without_a_password_refuses_every_password() {
        let mut user = with_password("alice", &Secret::new("hunter2"), 4096, 1, true);
        user.password = None;
        assert!(!check_password(&user, b"hunter2"));
    }

    /// The record's Debug says nothing about either secret.
    #[test]
    fn a_user_record_debugs_without_its_secrets() {
        let user = with_password("alice", &Secret::new("hunter2"), 4096, 1, true);
        let printed = format!("{user:?}");
        assert!(!printed.contains("argon2"), "{printed}");
        assert!(printed.contains("<redacted>"), "{printed}");
    }

    /// Section 12.2: a verifier exists only for a user enrolled in
    /// SCRAM. Not enrolled is a password login and nothing else.
    #[test]
    fn a_verifier_is_made_only_for_a_user_enrolled_in_scram() {
        let plain = with_password("alice", &Secret::new("hunter2"), 4096, 1, false);
        assert!(plain.scram.is_none());
        assert!(check_password(&plain, b"hunter2"));
        let enrolled = with_password("alice", &Secret::new("hunter2"), 4096, 1, true);
        assert!(enrolled.scram.is_some());
        assert!(!UserView::from(&plain).scram && UserView::from(&enrolled).scram);
    }

    /// A view carries no secret, only whether each form is set.
    #[test]
    fn a_view_says_which_credentials_exist_and_shows_neither() {
        let user = with_password("alice", &Secret::new("hunter2"), 4096, 1, true);
        let view = UserView::from(&user);
        assert_eq!(view.name, "alice");
        assert!(view.has_password && view.scram);
        let json = serde_json::to_string(&view).expect("serialises");
        assert!(
            !json.contains("argon2") && !json.contains("stored_key") && !json.contains("salt"),
            "{json}"
        );
    }
}
