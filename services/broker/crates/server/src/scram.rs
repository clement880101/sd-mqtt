//! SCRAM-SHA-256, the server's half, and a client's half for tests.
//! Section 12.2, RFC 5802 with RFC 7677's hash.
//!
//! # Pure arithmetic over messages
//!
//! Nothing here touches a socket or a packet. The connection shell
//! hands [`Server::start`] the bytes a CONNECT's authentication data
//! carried and sends back what it is given; hands [`Server::finish`]
//! the bytes the client's AUTH carried and sends back what it is
//! given. That is what lets RFC 7677's one published exchange pin the
//! whole thing: the nonces are injected, so the bytes can be compared
//! to the RFC's.
//!
//! # An unknown user is answered like a wrong password
//!
//! A server that said "no such user" at the first message would let a
//! caller enumerate names by trying them. So an unknown name gets a
//! server-first message with a salt and count of its own, the exchange
//! runs to the end, and the proof fails there — the same answer, at the
//! same step, as a wrong password for a user that exists.
//!
//! # Channel binding is not offered
//!
//! Section 12.2: the `-PLUS` variant is not implemented in v1. A client
//! whose GS2 header says `p=` is asking for a binding this server did
//! not advertise, and is refused rather than silently downgraded; `n`
//! and `y` are both accepted, since `y` means "I would have bound had
//! you offered".

use std::fmt;
use std::num::NonZeroU32;

use aws_lc_rs::{digest, hmac, pbkdf2, rand};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use storage::record::{ScramVerifierRecord, Secret};

/// The one mechanism this broker names. MQTT 5's Authentication Method
/// property carries exactly this string.
pub const METHOD: &str = "SCRAM-SHA-256";

/// Bytes of server nonce, before base64.
const NONCE_LEN: usize = 18;

/// Where the connection finds a user's verifier. The directory
/// implements it; a test can implement it over a map.
pub trait Verifiers: Send + Sync + fmt::Debug {
    /// The verifier for `name`, if the user exists and is enrolled.
    fn verifier(&self, name: &str) -> Option<ScramVerifierRecord>;
}

/// Why the exchange did not complete, and what to tell the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// A message that is not SCRAM: wrong shape, missing attribute, bad
    /// base64, a nonce that is not ours. MQTT 5 calls this a protocol
    /// error on the exchange, 0x82.
    Malformed,
    /// The client asked for a channel binding this server does not
    /// offer. 0x8C, bad authentication method.
    ChannelBinding,
    /// The proof did not verify: a wrong password, or a name nobody
    /// has. 0x87.
    NotAuthorized,
}

/// `Hi(password, salt, i)`: PBKDF2-HMAC-SHA-256, 32 bytes. RFC 5802 §2.2.
#[must_use]
pub fn salted_password(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut out = [0u8; 32];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        NonZeroU32::new(iterations.max(1)).expect("at least one"),
        salt,
        password,
        &mut out,
    );
    out
}

/// The two keys a server keeps for a password. RFC 5802 §3.
///
/// `ClientKey := HMAC(SaltedPassword, "Client Key")`, `StoredKey :=
/// H(ClientKey)`, `ServerKey := HMAC(SaltedPassword, "Server Key")`.
/// Normalisation is SASLprep in the RFC and the identity here, which the
/// RFC permits for passwords already in plain ASCII and which the README
/// says.
#[must_use]
pub fn verifier(password: &[u8], salt: &[u8], iterations: u32) -> ScramVerifierRecord {
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

/// A fresh nonce, base64 so it is printable and carries no comma.
fn fresh_nonce() -> String {
    let mut bytes = [0u8; NONCE_LEN];
    rand::fill(&mut bytes).expect("the random source answers");
    BASE64.encode(bytes)
}

/// A verifier for a name nobody has, so the exchange runs to the end
/// and fails there. Random keys, so no proof can match it: that
/// randomness is the whole of the refusal, and a mutation test showed a
/// separate "was this a real user" flag guarded nothing a test could
/// reach, so there is none.
fn decoy() -> ScramVerifierRecord {
    let mut salt = [0u8; 16];
    rand::fill(&mut salt).expect("the random source answers");
    let mut stored = [0u8; 32];
    rand::fill(&mut stored).expect("the random source answers");
    let mut server = [0u8; 32];
    rand::fill(&mut server).expect("the random source answers");
    ScramVerifierRecord {
        salt: salt.to_vec(),
        iterations: 4096,
        stored_key: Secret::new(BASE64.encode(stored)),
        server_key: Secret::new(BASE64.encode(server)),
    }
}

/// RFC 5802 §5.1: `=2C` is a comma and `=3D` an equals sign in a name.
fn unescape_name(name: &str) -> Option<String> {
    let mut out = String::with_capacity(name.len());
    let mut rest = name;
    while let Some(at) = rest.find('=') {
        out.push_str(&rest[..at]);
        match rest.get(at + 1..at + 3)? {
            "2C" => out.push(','),
            "3D" => out.push('='),
            _ => return None,
        }
        rest = &rest[at + 3..];
    }
    if rest.contains(',') {
        return None;
    }
    out.push_str(rest);
    Some(out)
}

/// The reverse, for the client half.
fn escape_name(name: &str) -> String {
    name.replace('=', "=3D").replace(',', "=2C")
}

/// The attributes of one SCRAM message, `k=v` separated by commas, in
/// order. A key appears once.
fn attributes(message: &str) -> Option<Vec<(char, &str)>> {
    let mut out = Vec::new();
    for part in message.split(',') {
        let mut chars = part.chars();
        let key = chars.next()?;
        if chars.next()? != '=' {
            return None;
        }
        out.push((key, &part[2..]));
    }
    Some(out)
}

/// The server's side of one exchange: what it must remember between
/// the client's first message and its last.
#[derive(Debug)]
pub struct Server {
    username: String,
    verifier: ScramVerifierRecord,
    nonce: String,
    client_first_bare: String,
    server_first: String,
    gs2_header: String,
}

impl Server {
    /// Begin with the client's first message.
    ///
    /// `verifiers` answers the name; `nonce` is this server's half of
    /// the nonce, random in production and injected by a test. Returns
    /// the server-first message to send.
    ///
    /// # Errors
    ///
    /// A message that is not a SCRAM client-first, or one asking for a
    /// channel binding.
    pub fn start(
        client_first: &[u8],
        verifiers: &dyn Verifiers,
        nonce: Option<String>,
    ) -> Result<(Self, Vec<u8>), Refusal> {
        let text = std::str::from_utf8(client_first).map_err(|_| Refusal::Malformed)?;
        // GS2 header: `n,,` or `y,,` or `p=<binding>,,`; an authzid in
        // the middle field (`a=`) is not accepted, because this broker
        // has no notion of acting as somebody else.
        let (binding, after_binding) = text.split_once(',').ok_or(Refusal::Malformed)?;
        match binding {
            "n" | "y" => {}
            other if other.starts_with("p=") => return Err(Refusal::ChannelBinding),
            _ => return Err(Refusal::Malformed),
        }
        let (authzid, bare) = after_binding.split_once(',').ok_or(Refusal::Malformed)?;
        if !authzid.is_empty() {
            return Err(Refusal::Malformed);
        }
        let gs2_header = format!("{binding},{authzid},");
        let attrs = attributes(bare).ok_or(Refusal::Malformed)?;
        // RFC 5802 §7: an optional `m=` first means an extension this
        // server does not know, and must be refused.
        if attrs.first().is_some_and(|(key, _)| *key == 'm') {
            return Err(Refusal::Malformed);
        }
        let (name, client_nonce) = match attrs.as_slice() {
            [('n', name), ('r', nonce), ..] => (*name, *nonce),
            _ => return Err(Refusal::Malformed),
        };
        if client_nonce.is_empty() || client_nonce.contains(',') {
            return Err(Refusal::Malformed);
        }
        let username = unescape_name(name).ok_or(Refusal::Malformed)?;
        let verifier = verifiers.verifier(&username).unwrap_or_else(decoy);
        let nonce = format!("{client_nonce}{}", nonce.unwrap_or_else(fresh_nonce));
        let server_first = format!(
            "r={nonce},s={},i={}",
            BASE64.encode(&verifier.salt),
            verifier.iterations
        );
        Ok((
            Self {
                username,
                verifier,
                nonce,
                client_first_bare: bare.to_owned(),
                server_first: server_first.clone(),
                gs2_header,
            },
            server_first.into_bytes(),
        ))
    }

    /// The name the client is claiming, before it has proved anything.
    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Finish with the client's final message. Returns the server-final
    /// message to send, and the name the client has now proved.
    ///
    /// # Errors
    ///
    /// A message that is not a SCRAM client-final, a nonce that is not
    /// the one this exchange issued, or a proof that does not verify —
    /// which is also what an unknown name comes to.
    pub fn finish(self, client_final: &[u8]) -> Result<(String, Vec<u8>), Refusal> {
        let text = std::str::from_utf8(client_final).map_err(|_| Refusal::Malformed)?;
        let attrs = attributes(text).ok_or(Refusal::Malformed)?;
        let (channel, nonce, proof) = match attrs.as_slice() {
            [('c', channel), ('r', nonce), .., ('p', proof)] => (*channel, *nonce, *proof),
            _ => return Err(Refusal::Malformed),
        };
        // The channel binding attribute is the GS2 header the client
        // sent first, base64; anything else is a client that changed
        // its mind mid-exchange.
        let expected_channel = BASE64.encode(self.gs2_header.as_bytes());
        if channel != expected_channel || nonce != self.nonce {
            return Err(Refusal::Malformed);
        }
        let proof = BASE64.decode(proof).map_err(|_| Refusal::Malformed)?;
        if proof.len() != 32 {
            return Err(Refusal::Malformed);
        }
        let without_proof = &text[..text.rfind(",p=").ok_or(Refusal::Malformed)?];
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, without_proof
        );
        let stored_key = BASE64
            .decode(self.verifier.stored_key.expose())
            .map_err(|_| Refusal::NotAuthorized)?;
        let server_key = BASE64
            .decode(self.verifier.server_key.expose())
            .map_err(|_| Refusal::NotAuthorized)?;
        // ClientSignature := HMAC(StoredKey, AuthMessage)
        // ClientKey := ClientProof XOR ClientSignature
        // verify: H(ClientKey) == StoredKey
        let signature = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, &stored_key),
            auth_message.as_bytes(),
        );
        let client_key: Vec<u8> = proof
            .iter()
            .zip(signature.as_ref())
            .map(|(p, s)| p ^ s)
            .collect();
        let recovered = digest::digest(&digest::SHA256, &client_key);
        let matches =
            aws_lc_rs::constant_time::verify_slices_are_equal(recovered.as_ref(), &stored_key)
                .is_ok();
        if !matches {
            return Err(Refusal::NotAuthorized);
        }
        // ServerSignature := HMAC(ServerKey, AuthMessage)
        let server_signature = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, &server_key),
            auth_message.as_bytes(),
        );
        let server_final = format!("v={}", BASE64.encode(server_signature.as_ref()));
        Ok((self.username, server_final.into_bytes()))
    }
}

/// A client's half, for tests and for the conformance of our own
/// server against the RFC's vector. Not what a device would use — a
/// device has its own library — but what proves this server speaks to
/// one.
#[derive(Debug)]
pub struct Client {
    gs2_header: String,
    client_first_bare: String,
    nonce: String,
    auth_message: Option<String>,
    salted: Option<[u8; 32]>,
}

impl Client {
    /// The client-first message for `username`, with this nonce.
    #[must_use]
    pub fn first(username: &str, nonce: &str) -> (Self, Vec<u8>) {
        let gs2_header = "n,,".to_owned();
        let bare = format!("n={},r={nonce}", escape_name(username));
        let message = format!("{gs2_header}{bare}");
        (
            Self {
                gs2_header,
                client_first_bare: bare,
                nonce: nonce.to_owned(),
                auth_message: None,
                salted: None,
            },
            message.into_bytes(),
        )
    }

    /// The client-final message, given the server's first and the
    /// password.
    ///
    /// # Errors
    ///
    /// A server-first that is not SCRAM, or a nonce that does not
    /// start with ours.
    pub fn finish(
        mut self,
        server_first: &[u8],
        password: &[u8],
    ) -> Result<(Self, Vec<u8>), Refusal> {
        let text = std::str::from_utf8(server_first).map_err(|_| Refusal::Malformed)?;
        let attrs = attributes(text).ok_or(Refusal::Malformed)?;
        let (nonce, salt, iterations) = match attrs.as_slice() {
            [('r', nonce), ('s', salt), ('i', iterations), ..] => (*nonce, *salt, *iterations),
            _ => return Err(Refusal::Malformed),
        };
        if !nonce.starts_with(self.nonce.as_str()) || nonce.len() == self.nonce.len() {
            return Err(Refusal::Malformed);
        }
        let salt = BASE64.decode(salt).map_err(|_| Refusal::Malformed)?;
        let iterations: u32 = iterations.parse().map_err(|_| Refusal::Malformed)?;
        let salted = salted_password(password, &salt, iterations);
        let without_proof = format!("c={},r={nonce}", BASE64.encode(self.gs2_header.as_bytes()));
        let auth_message = format!("{},{text},{without_proof}", self.client_first_bare);
        let key = hmac::Key::new(hmac::HMAC_SHA256, &salted);
        let client_key = hmac::sign(&key, b"Client Key");
        let stored_key = digest::digest(&digest::SHA256, client_key.as_ref());
        let signature = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, stored_key.as_ref()),
            auth_message.as_bytes(),
        );
        let proof: Vec<u8> = client_key
            .as_ref()
            .iter()
            .zip(signature.as_ref())
            .map(|(k, s)| k ^ s)
            .collect();
        let message = format!("{without_proof},p={}", BASE64.encode(proof));
        self.auth_message = Some(auth_message);
        self.salted = Some(salted);
        Ok((self, message.into_bytes()))
    }

    /// Whether the server's final message proves it knew the password's
    /// server key: the mutual half of the authentication.
    #[must_use]
    pub fn verify(&self, server_final: &[u8]) -> bool {
        let (Some(auth_message), Some(salted)) = (&self.auth_message, &self.salted) else {
            return false;
        };
        let Ok(text) = std::str::from_utf8(server_final) else {
            return false;
        };
        let Some(signature) = text.strip_prefix("v=") else {
            return false;
        };
        let Ok(signature) = BASE64.decode(signature) else {
            return false;
        };
        let key = hmac::Key::new(hmac::HMAC_SHA256, salted);
        let server_key = hmac::sign(&key, b"Server Key");
        let expected = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, server_key.as_ref()),
            auth_message.as_bytes(),
        );
        aws_lc_rs::constant_time::verify_slices_are_equal(expected.as_ref(), &signature).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Section 17's measurement, by hand: how long the derivation a
    /// client must run takes at three iteration counts. Ignored, because
    /// it is a timing and not a check; `docs/decisions/0002` records
    /// what it printed and where.
    ///
    ///     cargo test -p sdmqtt-server --release --lib scram_cost -- --ignored --nocapture
    #[test]
    #[ignore = "a measurement, run by hand"]
    fn scram_cost() {
        for iterations in [4_096_u32, 10_000, 50_000] {
            let rounds = 20;
            let started = std::time::Instant::now();
            for round in 0..rounds {
                std::hint::black_box(salted_password(
                    b"a password of ordinary length",
                    &[round; 16],
                    iterations,
                ));
            }
            let each = started.elapsed() / u32::from(rounds);
            println!("scram_cost iterations={iterations} each={each:?}");
        }
    }

    /// RFC 7677 §3's exchange, byte for byte: the one published vector.
    const CLIENT_NONCE: &str = "rOprNGfwEbeRWgbNEkqO";
    const SERVER_NONCE: &str = "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
    const SALT: &str = "W22ZaJ0SNY7soEsUEjb6gQ==";

    #[derive(Debug)]
    struct One(ScramVerifierRecord);
    impl Verifiers for One {
        fn verifier(&self, name: &str) -> Option<ScramVerifierRecord> {
            (name == "user").then(|| self.0.clone())
        }
    }

    fn rfc_user() -> One {
        One(verifier(
            b"pencil",
            &BASE64.decode(SALT).expect("the RFC's salt"),
            4096,
        ))
    }

    #[test]
    fn the_rfc_7677_exchange_reproduces_every_message() {
        let (client, client_first) = Client::first("user", CLIENT_NONCE);
        assert_eq!(client_first, b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO");
        let (server, server_first) =
            Server::start(&client_first, &rfc_user(), Some(SERVER_NONCE.to_owned()))
                .expect("a well-formed first message");
        assert_eq!(
            server_first,
            b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
        );
        let (client, client_final) = client
            .finish(&server_first, b"pencil")
            .expect("a well-formed server-first");
        assert_eq!(
            client_final,
            b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        let (name, server_final) = server
            .finish(&client_final)
            .expect("the RFC's proof verifies");
        assert_eq!(name, "user");
        assert_eq!(
            server_final,
            b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
        );
        assert!(client.verify(&server_final));
    }

    #[test]
    fn a_wrong_password_and_an_unknown_user_are_one_answer_at_the_same_step() {
        let (client, first) = Client::first("user", CLIENT_NONCE);
        let (server, server_first) = Server::start(&first, &rfc_user(), None).expect("starts");
        let (_, client_final) = client.finish(&server_first, b"pen").expect("finishes");
        assert_eq!(server.finish(&client_final), Err(Refusal::NotAuthorized));

        let (client, first) = Client::first("nobody", CLIENT_NONCE);
        let (server, server_first) =
            Server::start(&first, &rfc_user(), None).expect("an unknown name still gets a first");
        let (_, client_final) = client.finish(&server_first, b"pencil").expect("finishes");
        assert_eq!(server.finish(&client_final), Err(Refusal::NotAuthorized));
    }

    #[test]
    fn a_tampered_server_final_does_not_verify() {
        let (client, first) = Client::first("user", CLIENT_NONCE);
        let (server, server_first) = Server::start(&first, &rfc_user(), None).expect("starts");
        let (client, client_final) = client.finish(&server_first, b"pencil").expect("finishes");
        let (_, server_final) = server.finish(&client_final).expect("verifies");
        assert!(client.verify(&server_final));
        assert!(!client.verify(b"v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="));
    }

    /// RFC 5802 §6: `y` is a client that supports channel binding and
    /// believes this server does not. A server that offers no `-PLUS`
    /// must serve it; refusing would lock out every client that asks
    /// politely. Decision 0003 rests on this.
    #[test]
    fn a_client_that_could_bind_but_does_not_is_served() {
        let started = Server::start(b"y,,n=user,r=abc", &rfc_user(), None);
        assert!(started.is_ok(), "{:?}", started.err());
    }

    #[test]
    fn channel_binding_is_refused_and_junk_is_malformed() {
        assert_eq!(
            Server::start(b"p=tls-unique,,n=user,r=abc", &rfc_user(), None).err(),
            Some(Refusal::ChannelBinding)
        );
        assert_eq!(
            Server::start(b"n,a=admin,n=user,r=abc", &rfc_user(), None).err(),
            Some(Refusal::Malformed),
            "an authorization identity is not accepted"
        );
        assert_eq!(
            Server::start(b"n,,m=ext,n=user,r=abc", &rfc_user(), None).err(),
            Some(Refusal::Malformed),
            "a mandatory extension this server does not know"
        );
        assert_eq!(
            Server::start(b"hello", &rfc_user(), None).err(),
            Some(Refusal::Malformed)
        );
        assert_eq!(
            Server::start(&[0xff, 0xfe], &rfc_user(), None).err(),
            Some(Refusal::Malformed)
        );
        let (server, _) = Server::start(b"n,,n=user,r=abc", &rfc_user(), None).expect("starts");
        assert_eq!(
            server.finish(b"c=biws,r=wrong,p=AAAA").err(),
            Some(Refusal::Malformed)
        );
    }

    /// A name with a comma or an equals sign is escaped on the wire and
    /// is the stored name when it arrives.
    #[test]
    fn names_with_commas_and_equals_signs_round_trip_through_the_escaping() {
        #[derive(Debug)]
        struct Odd(ScramVerifierRecord);
        impl Verifiers for Odd {
            fn verifier(&self, name: &str) -> Option<ScramVerifierRecord> {
                (name == "a,b=c").then(|| self.0.clone())
            }
        }
        let odd = Odd(verifier(b"pw", b"saltsaltsaltsalt", 4096));
        let (client, first) = Client::first("a,b=c", "nonce1");
        assert_eq!(first, b"n,,n=a=2Cb=3Dc,r=nonce1");
        let (server, server_first) = Server::start(&first, &odd, None).expect("starts");
        assert_eq!(server.username(), "a,b=c");
        let (client, client_final) = client.finish(&server_first, b"pw").expect("finishes");
        let (name, server_final) = server.finish(&client_final).expect("verifies");
        assert_eq!(name, "a,b=c");
        assert!(client.verify(&server_final));
        assert_eq!(
            unescape_name("a=2Zb"),
            None,
            "an escape that is not one of the two"
        );
    }
}
