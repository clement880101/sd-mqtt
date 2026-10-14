//! A user's credentials: an argon2id hash for a password login and a
//! SCRAM-SHA-256 verifier for the AUTH exchange. Sections 12.1 and 12.2.
//!
//! Both are derived when the password is set and the password is kept
//! by neither: a verifier cannot be derived from an argon2 hash, so the
//! one moment the password is in hand is the moment both are made.

use std::num::NonZeroU32;

use argon2::password_hash::{PasswordHasher as _, PasswordVerifier as _};
use aws_lc_rs::{digest, hmac, pbkdf2, rand};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde::Serialize;
use storage::record::{ScramVerifierRecord, Secret, UserRecord};

/// Bytes of salt, for both forms.
const SALT_LEN: usize = 16;

/// Build a user with `password`, hashing it both ways.
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
) -> UserRecord {
    let mut salt = [0u8; SALT_LEN];
    rand::fill(&mut salt).expect("the random source answers");
    let hash = argon2::Argon2::default()
        .hash_password_with_salt(password.expose().as_bytes(), &salt)
        .expect("argon2 hashes any password with a sixteen-byte salt")
        .to_string();
    let mut scram_salt = [0u8; SALT_LEN];
    rand::fill(&mut scram_salt).expect("the random source answers");
    UserRecord {
        name: name.to_owned(),
        password: Some(Secret::new(hash)),
        scram: Some(verifier(password.expose(), &scram_salt, iterations)),
        created_at,
    }
}

/// Whether `password` is the one `user`'s hash was made from.
///
/// False for a user with no password: SCRAM-only users log in through
/// the AUTH exchange, and a password login for one is a wrong password.
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

/// RFC 5802 §3 with RFC 7677's hash: the two keys a server keeps.
///
/// `SaltedPassword := Hi(Normalize(password), salt, i)`, where `Hi` is
/// PBKDF2 with HMAC-SHA-256; `ClientKey := HMAC(SaltedPassword, "Client
/// Key")`; `StoredKey := H(ClientKey)`; `ServerKey :=
/// HMAC(SaltedPassword, "Server Key")`. Normalisation is SASLprep in the
/// RFC and the identity here, which the RFC permits for passwords that
/// are already plain ASCII and which this broker documents.
#[must_use]
pub fn verifier(password: &str, salt: &[u8], iterations: u32) -> ScramVerifierRecord {
    let salted = salted_password(password, salt, iterations);
    let key = hmac::Key::new(hmac::HMAC_SHA256, &salted);
    let client_key = hmac::sign(&key, b"Client Key");
    let stored_key = digest::digest(&digest::SHA256, client_key.as_ref());
    let server_key = hmac::sign(&key, b"Server Key");
    ScramVerifierRecord {
        salt: salt.to_vec(),
        iterations,
        stored_key: Secret::new(BASE64.encode(stored_key.as_ref())),
        server_key: Secret::new(BASE64.encode(server_key.as_ref())),
    }
}

/// `Hi(password, salt, i)`: PBKDF2-HMAC-SHA-256, 32 bytes.
#[must_use]
pub fn salted_password(password: &str, salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut out = [0u8; 32];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        NonZeroU32::new(iterations.max(1)).expect("at least one"),
        salt,
        password.as_bytes(),
        &mut out,
    );
    out
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
        let user = with_password("alice", &Secret::new("hunter2"), 4096, 7);
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
        let one = with_password("alice", &Secret::new("same"), 4096, 1);
        let two = with_password("bob", &Secret::new("same"), 4096, 1);
        assert_ne!(one.password, two.password);
        let (a, b) = (one.scram.unwrap(), two.scram.unwrap());
        assert_ne!(a.salt, b.salt);
        assert_ne!(a.stored_key, b.stored_key);
        assert_ne!(a.server_key, b.server_key);
    }

    /// A user without a password form is not a password login.
    #[test]
    fn a_user_without_a_password_refuses_every_password() {
        let mut user = with_password("alice", &Secret::new("hunter2"), 4096, 1);
        user.password = None;
        assert!(!check_password(&user, b"hunter2"));
    }

    /// The record's Debug says nothing about either secret.
    #[test]
    fn a_user_record_debugs_without_its_secrets() {
        let user = with_password("alice", &Secret::new("hunter2"), 4096, 1);
        let printed = format!("{user:?}");
        assert!(!printed.contains("argon2"), "{printed}");
        assert!(printed.contains("<redacted>"), "{printed}");
    }

    /// A view carries no secret, only whether each form is set.
    #[test]
    fn a_view_says_which_credentials_exist_and_shows_neither() {
        let user = with_password("alice", &Secret::new("hunter2"), 4096, 1);
        let view = UserView::from(&user);
        assert_eq!(view.name, "alice");
        assert!(view.has_password && view.scram);
        let json = serde_json::to_string(&view).expect("serialises");
        assert!(
            !json.contains("argon2") && !json.contains("stored_key") && !json.contains("salt"),
            "{json}"
        );
    }

    /// RFC 7677 §3's example — user "user", password "pencil", salt
    /// `W22ZaJ0SNY7soEsUEjb6gQ==`, 4096 rounds — pins the derivation to
    /// the standard rather than to this implementation. The salted
    /// password is the intermediate every SCRAM implementation derives
    /// for that vector.
    #[test]
    fn the_rfc_7677_vector_derives_the_known_salted_password() {
        let salt = BASE64
            .decode("W22ZaJ0SNY7soEsUEjb6gQ==")
            .expect("the RFC's salt");
        let salted = salted_password("pencil", &salt, 4096);
        assert_eq!(
            BASE64.encode(salted),
            "xKSVEDI6tPlSysH6mUQZOeeOp01r6B3fcJbodRPcYV0=",
        );
    }
}
