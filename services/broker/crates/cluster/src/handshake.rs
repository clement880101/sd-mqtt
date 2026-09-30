//! Proving that both ends hold the same secret, without sending it.
//!
//! Section 8.1: **the link is authenticated by a mutual challenge, not
//! by sending the secret.** Each side sends a nonce, then a keyed hash
//! over a transcript of a domain string, a role byte, the cluster name,
//! both node identities and both nonces. Each verifies the other's
//! before trusting it, and the listener verifies before answering, so
//! an unauthenticated dialler learns nothing.
//!
//! Every part of that earns its place, and each part is a different
//! attack:
//!
//! - **The secret is never sent.** Wrapping the link in TLS is
//!   optional, so a handshake that sent it would hand it to a passive
//!   observer, once, for ever.
//! - **Both nonces are fresh, every connection.** A transcript captured
//!   off the wire then names nonces no later connection will use, so
//!   the proof over it authenticates nothing later.
//! - **The role byte** is what stops the answer being *reflected*.
//!   Every node holds the same secret, so without it the dialler's
//!   proof and the listener's proof over one connection are the same
//!   thirty-two bytes — and an attacker who knows nothing waits for the
//!   honest dialler to prove first and echoes that proof straight back
//!   as its own. See
//!   [`tests::an_answer_harvested_in_one_role_is_the_wrong_answer_in_the_other`],
//!   which performs exactly that.
//! - **The cluster name** stops a staging node configured with a
//!   production secret from joining production.
//! - **Both identities** are in the transcript, so a proof made for a
//!   handshake with one peer is not a proof for a handshake with
//!   another.
//! - **The listener verifies before it answers**, so a dialler that
//!   cannot prove itself is given nothing it did not already have.
//!
//! # Every length prefix earns its place
//!
//! The transcript is a concatenation, and a concatenation of
//! variable-length fields is ambiguous unless something says where each
//! one ends: `"ab" + "c"` and `"a" + "bc"` are the same bytes. Two
//! different handshakes hashing to one transcript is two different
//! handshakes with one proof. So every variable-length field carries a
//! 32-bit length, and [`tests::sliding_a_field_boundary_changes_the_transcript`]
//! is what says so by moving a boundary.
//!
//! Fixed-width fields — the role byte, the two identities, the two
//! nonces — carry no prefix, because their width is not a thing a peer
//! can move.
//!
//! # What this does not do
//!
//! It does not wrap the link in TLS; section 8.1 makes that optional
//! and it is configured above this. It does not decide who to dial, or
//! what to do with a refusal: a mismatched secret backs off harder than
//! a transport failure and is logged once per peer rather than once per
//! attempt, and that is the reconnection policy's, not this module's.

use aws_lc_rs::hmac;
use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use storage::Secret;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::config::ClusterConfig;
use crate::identity::{NodeId, NodeIdentity};

/// What the keyed hash is over, before anything else.
///
/// A domain string, so that a proof from this protocol cannot be a
/// proof for any other use of the same secret. Versioned, because the
/// day the transcript's layout changes, the two layouts must not be
/// able to produce one proof.
const DOMAIN: &[u8] = b"sd-mqtt cluster handshake v1";

/// What a greeting starts with, so that something which is not one is
/// refused as such rather than as a bad proof.
const GREETING_MAGIC: [u8; 8] = *b"sdmqttcl";

/// The greeting layout this build writes and reads.
///
/// Its own version, separate from the framed link's: a node that
/// cannot even be greeted must say so before either side has spent
/// anything, and the frame version is about messages that only flow
/// once this has succeeded.
const GREETING_VERSION: u8 = 1;

/// How many bytes of nonce each side sends.
///
/// Thirty-two, which is the output width of the hash below. What it
/// has to be is large enough that a nonce is never repeated by
/// accident, and the cost of being generous is thirty-two bytes once
/// per connection.
pub const NONCE_BYTES: usize = 32;

/// How many bytes a proof is: HMAC-SHA256's output.
pub const PROOF_BYTES: usize = 32;

/// The longest name a node may advertise.
///
/// A byte, because the name is for a human to read in a log line and
/// this is a length a peer controls. A greeting is read into a buffer
/// this bounds, before anything is trusted.
const MAX_NAME_BYTES: usize = 255;

/// Magic, version, identity, nonce and the name's length.
const GREETING_HEAD: usize = GREETING_MAGIC.len() + 1 + 16 + NONCE_BYTES + 1;

/// Which end of the link a proof was made by.
///
/// **In the transcript**, which is the whole reason it exists: without
/// it the two proofs over one connection are the same bytes, and the
/// answer one end gives is the answer the other end is waiting for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// The end that opened the connection, and proves first.
    Dialler,
    /// The end that accepted it, and verifies before answering.
    Listener,
}

impl Role {
    /// The byte this role contributes to a transcript.
    ///
    /// Not zero and not one: two values that are neither the default
    /// state of a byte nor each other's complement, so a transcript
    /// built over a zeroed buffer is not accidentally a dialler's.
    const fn byte(self) -> u8 {
        match self {
            Self::Dialler => 0x44,
            Self::Listener => 0x4c,
        }
    }
}

/// What a node says about itself when a connection opens.
///
/// The identity and a fresh nonce. **Not the secret**, and not
/// anything derived from it: everything here is public, which is what
/// makes a passive observer's capture worth nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Greeting {
    /// Who it is.
    pub id: NodeId,
    /// What it is called. In the transcript, so it cannot be altered
    /// in flight, but never compared; see [`crate::identity`].
    pub name: String,
    /// The challenge this end contributes, fresh per connection.
    pub nonce: [u8; NONCE_BYTES],
}

impl Greeting {
    /// This node's greeting on a new connection, with a fresh nonce.
    ///
    /// # Errors
    ///
    /// [`HandshakeError::NoRandomness`] if the system's random source
    /// fails, and [`HandshakeError::NameTooLong`] if this node's own
    /// name will not fit in a greeting. Both are refusals to open a
    /// link rather than a link opened with a nonce that is not one.
    pub fn mint(identity: &NodeIdentity) -> Result<Self, HandshakeError> {
        if identity.name().len() > MAX_NAME_BYTES {
            return Err(HandshakeError::NameTooLong {
                length: identity.name().len(),
            });
        }
        let mut nonce = [0_u8; NONCE_BYTES];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| HandshakeError::NoRandomness)?;
        Ok(Self {
            id: identity.id(),
            name: identity.name().to_owned(),
            nonce,
        })
    }

    /// The greeting, as it goes on the wire.
    fn encode(&self) -> Vec<u8> {
        let name = self.name.as_bytes();
        let mut out = Vec::with_capacity(GREETING_HEAD + name.len());
        out.extend_from_slice(&GREETING_MAGIC);
        out.push(GREETING_VERSION);
        out.extend_from_slice(&self.id.to_bytes());
        out.extend_from_slice(&self.nonce);
        // Fits: `mint` refused a longer one, and this is the only way
        // one is made.
        out.push(u8::try_from(name.len()).unwrap_or(u8::MAX));
        out.extend_from_slice(name);
        out
    }
}

/// Read the peer's greeting off the stream.
///
/// Two reads, because the name's length is in the first: the head is a
/// fixed size and is read whole, and only then is a bounded number of
/// further bytes read. Nothing a peer says decides how much is
/// allocated beyond the 255 bytes a name may be.
async fn read_greeting<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Greeting, HandshakeError> {
    let mut head = [0_u8; GREETING_HEAD];
    stream.read_exact(&mut head).await?;
    if head[..GREETING_MAGIC.len()] != GREETING_MAGIC {
        return Err(HandshakeError::NotAGreeting);
    }
    let version = head[GREETING_MAGIC.len()];
    if version != GREETING_VERSION {
        return Err(HandshakeError::UnknownGreeting {
            found: version,
            expected: GREETING_VERSION,
        });
    }
    let mut id = [0_u8; 16];
    let at = GREETING_MAGIC.len() + 1;
    id.copy_from_slice(&head[at..at + 16]);
    let mut nonce = [0_u8; NONCE_BYTES];
    nonce.copy_from_slice(&head[at + 16..at + 16 + NONCE_BYTES]);
    let length = usize::from(head[GREETING_HEAD - 1]);

    let mut name = vec![0_u8; length];
    stream.read_exact(&mut name).await?;
    let name = String::from_utf8(name).map_err(|_| HandshakeError::NameNotText)?;

    Ok(Greeting {
        id: NodeId::from_bytes(id),
        name,
        nonce,
    })
}

/// Read a proof off the stream.
async fn read_proof<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> Result<[u8; PROOF_BYTES], HandshakeError> {
    let mut proof = [0_u8; PROOF_BYTES];
    stream.read_exact(&mut proof).await?;
    Ok(proof)
}

/// Everything both ends hash, in one place.
///
/// One function, used by both ends and for both roles, because a
/// transcript built in two places is two layouts waiting to disagree —
/// and the disagreement is a link that authenticates nothing, in the
/// direction nobody tested.
///
/// The order is section 8.1's: the domain string, the role byte, the
/// cluster name, both node identities, both nonces. The dialler's half
/// of each pair comes first, whichever end is building it, so that the
/// two ends build the same bytes.
fn transcript(role: Role, cluster: &str, dialler: &Greeting, listener: &Greeting) -> Vec<u8> {
    let mut out = Vec::with_capacity(DOMAIN.len() + cluster.len() + 128);
    prefixed(&mut out, DOMAIN);
    // Fixed width: a peer cannot move the boundary of one byte.
    out.push(role.byte());
    prefixed(&mut out, cluster.as_bytes());
    out.extend_from_slice(&dialler.id.to_bytes());
    out.extend_from_slice(&listener.id.to_bytes());
    prefixed(&mut out, dialler.name.as_bytes());
    prefixed(&mut out, listener.name.as_bytes());
    out.extend_from_slice(&dialler.nonce);
    out.extend_from_slice(&listener.nonce);
    out
}

/// Append a field, and say how long it is first.
///
/// Thirty-two bits, little-endian, matching the framed link's length.
/// See the module documentation for what happens without it.
fn prefixed(out: &mut Vec<u8>, field: &[u8]) {
    // A field longer than four gigabytes is not reachable: the domain
    // is a constant, a cluster name comes from a configuration file
    // and a node name is bounded at 255 bytes by the greeting.
    let length = u32::try_from(field.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(field);
}

/// The proof over a transcript.
///
/// HMAC-SHA256, keyed with the cluster secret. The secret itself never
/// leaves this function's caller, and the output is what goes on the
/// wire.
fn proof(secret: &Secret, transcript: &[u8]) -> [u8; PROOF_BYTES] {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.expose().as_bytes());
    let tag = hmac::sign(&key, transcript);
    let mut out = [0_u8; PROOF_BYTES];
    // HMAC-SHA256 is thirty-two bytes wide, which is what
    // `PROOF_BYTES` is.
    out.copy_from_slice(tag.as_ref());
    out
}

/// Whether a proof is the one this transcript should have produced.
///
/// **`hmac::verify` rather than `==`**, because it takes the same time
/// whatever the first differing byte is. An ordinary comparison returns
/// as soon as two bytes differ, which leaks how much of a guess was
/// right to anyone who can measure it, and a proof can be guessed a
/// byte at a time by anyone who can open connections.
///
/// No test here can see the difference — a timing assertion over
/// thirty-two bytes in a test suite would be measuring the scheduler —
/// so this is a comment rather than a guard, and it is said out loud
/// for that reason.
fn verify(secret: &Secret, transcript: &[u8], offered: &[u8]) -> bool {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.expose().as_bytes());
    hmac::verify(&key, transcript, offered).is_ok()
}

/// Who the other end turned out to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    /// The peer's identity, as it advertised it and as the proof
    /// covers it.
    pub identity: NodeIdentity,
    /// The nonce this end contributed, kept so that a caller can tie a
    /// link to the handshake that opened it.
    pub nonce: [u8; NONCE_BYTES],
}

/// Open a link as the end that dialled.
///
/// Greets, hears the peer out, proves itself, and **verifies the
/// peer's proof before returning**. A dialler that took the link on
/// trust would give a middle that never knew the secret a one-way
/// link: everything this node sends, to whoever answered the address.
///
/// # Errors
///
/// [`HandshakeError`], and every variant of it is a link that must not
/// be used. [`HandshakeError::OurOwnIdentity`] is the loud one: it is
/// a copied data directory or a cloned image, and it breaks the
/// uniqueness conflict resolution depends on.
pub async fn dial<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    config: &ClusterConfig,
    me: &NodeIdentity,
) -> Result<Peer, HandshakeError> {
    let mine = Greeting::mint(me)?;
    stream.write_all(&mine.encode()).await?;
    stream.flush().await?;

    let theirs = read_greeting(stream).await?;
    refuse_ourselves(me, &theirs)?;

    // The dialler proves first. That is what lets the listener verify
    // before it answers, and it is why the role byte matters: this
    // proof is handed to whoever answered the address.
    let ours = transcript(Role::Dialler, config.name(), &mine, &theirs);
    stream.write_all(&proof(config.secret(), &ours)).await?;
    stream.flush().await?;

    let offered = read_proof(stream).await?;
    let theirs_transcript = transcript(Role::Listener, config.name(), &mine, &theirs);
    if !verify(config.secret(), &theirs_transcript, &offered) {
        return Err(HandshakeError::Unauthenticated {
            role: Role::Listener,
        });
    }

    Ok(Peer {
        identity: NodeIdentity::new(theirs.id, theirs.name),
        nonce: mine.nonce,
    })
}

/// Open a link as the end that accepted the connection.
///
/// Hears the peer out, greets, **verifies before answering**, and only
/// then proves itself. The order is the point: a dialler that cannot
/// prove it holds the secret never receives a proof, so it learns
/// nothing it could not have worked out on its own.
///
/// # Errors
///
/// As [`dial`].
pub async fn accept<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    config: &ClusterConfig,
    me: &NodeIdentity,
) -> Result<Peer, HandshakeError> {
    let theirs = read_greeting(stream).await?;
    refuse_ourselves(me, &theirs)?;

    let mine = Greeting::mint(me)?;
    stream.write_all(&mine.encode()).await?;
    stream.flush().await?;

    let offered = read_proof(stream).await?;
    let theirs_transcript = transcript(Role::Dialler, config.name(), &theirs, &mine);
    if !verify(config.secret(), &theirs_transcript, &offered) {
        // **Before answering.** Returning here rather than after the
        // write is the whole of "an unauthenticated dialler learns
        // nothing".
        return Err(HandshakeError::Unauthenticated {
            role: Role::Dialler,
        });
    }

    let ours = transcript(Role::Listener, config.name(), &theirs, &mine);
    stream.write_all(&proof(config.secret(), &ours)).await?;
    stream.flush().await?;

    Ok(Peer {
        identity: NodeIdentity::new(theirs.id, theirs.name),
        nonce: mine.nonce,
    })
}

/// Refuse a greeting carrying this node's own identity.
///
/// Section 8.1, and it says to say so loudly: that is a copied data
/// directory or a cloned image, and it breaks the uniqueness that
/// conflict resolution depends on, quietly and permanently. The
/// handshake detects it for nothing, so it does.
fn refuse_ourselves(me: &NodeIdentity, theirs: &Greeting) -> Result<(), HandshakeError> {
    if theirs.id == me.id() {
        return Err(HandshakeError::OurOwnIdentity { id: theirs.id });
    }
    Ok(())
}

/// Why a link was not opened.
#[derive(Debug, thiserror::Error)]
pub enum HandshakeError {
    /// The connection failed, or ended partway through the handshake.
    #[error("the connection failed during the handshake: {0}")]
    Io(#[from] std::io::Error),
    /// What arrived does not begin like a greeting.
    ///
    /// Told apart from an unknown version, because this is usually
    /// something else entirely on the port rather than a peer from a
    /// different build.
    #[error("the peer did not send a cluster greeting")]
    NotAGreeting,
    /// A greeting from a build whose layout this one does not know.
    #[error("the peer greeted with version {found}, and this build speaks {expected}")]
    UnknownGreeting {
        /// What the peer sent.
        found: u8,
        /// What this build speaks.
        expected: u8,
    },
    /// The peer's advertised name is not text.
    #[error("the peer's name is not valid UTF-8")]
    NameNotText,
    /// This node's own name will not fit in a greeting.
    #[error("this node's name is {length} bytes, and a greeting carries at most 255")]
    NameTooLong {
        /// How long it is.
        length: usize,
    },
    /// The system's random source failed.
    ///
    /// A refusal rather than a weaker nonce. A nonce that is not fresh
    /// is the whole of what stops a captured transcript authenticating
    /// a later connection.
    #[error("no randomness is available, so no nonce can be minted")]
    NoRandomness,
    /// The peer greeted with this node's own identity.
    ///
    /// A copied store directory or a cloned image. Its own variant
    /// because the answer is not "check the secret": it is "one of
    /// these two nodes needs a store of its own".
    #[error(
        "the peer greeted with this node's own identity, {id}. That is a copied \
         store directory or a cloned image, and two nodes with one identity is \
         what conflict resolution cannot survive: give one of them a store \
         directory of its own."
    )]
    OurOwnIdentity {
        /// The identity both ends claim.
        id: NodeId,
    },
    /// The proof did not verify.
    ///
    /// Names which end failed to prove itself, because the two are
    /// different situations: a listener that cannot prove itself is
    /// something answering an address it should not be, and a dialler
    /// that cannot is something trying to join.
    #[error("the {role:?} end did not prove it holds this cluster's secret")]
    Unauthenticated {
        /// Which end failed.
        role: Role,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::config::ClusterSettings;
    use tokio::io::{duplex, DuplexStream};

    fn config(secret: &str) -> ClusterConfig {
        ClusterSettings {
            name: "production".to_owned(),
            secret: Some(Secret::new(secret)),
        }
        .check()
        .expect("a usable configuration")
    }

    fn identity(name: &str) -> NodeIdentity {
        NodeIdentity::mint(name)
    }

    /// A greeting an attacker writes by hand.
    fn greeting(name: &str, nonce: u8) -> Greeting {
        Greeting {
            id: NodeId::mint(),
            name: name.to_owned(),
            nonce: [nonce; NONCE_BYTES],
        }
    }

    async fn write_all(stream: &mut DuplexStream, bytes: &[u8]) {
        stream.write_all(bytes).await.expect("the write succeeds");
        stream.flush().await.expect("the flush succeeds");
    }

    /// A pair of ends, big enough that neither side blocks on a write
    /// the other has not read yet.
    fn pair() -> (DuplexStream, DuplexStream) {
        duplex(4096)
    }

    // -----------------------------------------------------------
    // The happy path, both ways
    // -----------------------------------------------------------

    /// A matching secret authenticates both ways, and each end learns
    /// who the other is.
    #[tokio::test]
    async fn a_matching_secret_authenticates_both_ways() {
        let (mut dialling_side, mut accepting_side) = pair();
        let (dialler, listener) = (identity("edge-1"), identity("edge-2"));
        let config = config("a-shared-secret");

        let dialled = {
            let config = config.clone();
            let dialler = dialler.clone();
            tokio::spawn(async move { dial(&mut dialling_side, &config, &dialler).await })
        };
        let accepted = accept(&mut accepting_side, &config, &listener)
            .await
            .expect("the listener authenticates the dialler");
        let dialled = dialled
            .await
            .expect("the dialler did not panic")
            .expect("the dialler authenticates the listener");

        assert_eq!(accepted.identity, dialler, "the listener knows who called");
        assert_eq!(
            dialled.identity, listener,
            "and the dialler knows who answered"
        );
        assert_ne!(
            accepted.nonce, dialled.nonce,
            "each end contributes its own challenge"
        );
    }

    /// A mismatched secret is refused at **both** ends.
    ///
    /// Both halves are written against a hand-driven opposite end, so
    /// that each end's refusal is its own. Run as one handshake, the
    /// listener refuses first and closes, and the dialler would then
    /// fail on a truncated read — which looks the same in the log and
    /// proves nothing about whether the dialler checks anything at
    /// all. A dialler that did not would give a middle that never knew
    /// the secret a one-way link: everything this node sends, to
    /// whoever answered the address.
    #[tokio::test]
    async fn a_mismatched_secret_is_refused_at_both_ends() {
        // The listener's half: an impostor dials with the wrong
        // secret and speaks the protocol correctly otherwise.
        let (mut impostor, mut honest) = pair();
        let listener = identity("edge-2");
        let accepting = {
            let config = config("the-real-secret");
            let listener = listener.clone();
            tokio::spawn(async move { accept(&mut honest, &config, &listener).await })
        };
        let mine = greeting("impostor", 0x11);
        write_all(&mut impostor, &mine.encode()).await;
        let theirs = read_greeting(&mut impostor).await.expect("a greeting");
        let wrong = config("the-wrong-secret");
        let script = transcript(Role::Dialler, wrong.name(), &mine, &theirs);
        write_all(&mut impostor, &proof(wrong.secret(), &script)).await;

        let refused = accepting
            .await
            .expect("no panic")
            .expect_err("the wrong secret is not this cluster's");
        assert!(
            matches!(
                refused,
                HandshakeError::Unauthenticated {
                    role: Role::Dialler
                }
            ),
            "the listener must name the end that failed: {refused:?}"
        );

        // The dialler's half: something answers the address and cannot
        // prove it holds the secret.
        let (mut impostor, mut honest) = pair();
        let dialler = identity("edge-1");
        let dialling = {
            let config = config("the-real-secret");
            let dialler = dialler.clone();
            tokio::spawn(async move { dial(&mut honest, &config, &dialler).await })
        };
        let theirs = read_greeting(&mut impostor).await.expect("a greeting");
        let mine = greeting("impostor", 0x22);
        write_all(&mut impostor, &mine.encode()).await;
        let _their_proof = read_proof(&mut impostor).await.expect("the dialler proves");
        let script = transcript(Role::Listener, wrong.name(), &theirs, &mine);
        write_all(&mut impostor, &proof(wrong.secret(), &script)).await;

        let refused = dialling
            .await
            .expect("no panic")
            .expect_err("the answerer did not hold the secret");
        assert!(
            matches!(
                refused,
                HandshakeError::Unauthenticated {
                    role: Role::Listener
                }
            ),
            "the dialler must refuse on its own account: {refused:?}"
        );
    }

    /// A node configured for one cluster cannot join another.
    ///
    /// The same secret, a different cluster name: the staging node
    /// with a production secret that binding the name exists to stop.
    #[tokio::test]
    async fn a_different_cluster_name_does_not_authenticate() {
        let (mut theirs, mut ours) = pair();
        let listener = identity("edge-2");
        let accepting = {
            let config = config("a-shared-secret");
            let listener = listener.clone();
            tokio::spawn(async move { accept(&mut ours, &config, &listener).await })
        };

        let staging = ClusterSettings {
            name: "staging".to_owned(),
            secret: Some(Secret::new("a-shared-secret")),
        }
        .check()
        .expect("usable");
        let dialler = identity("edge-1");
        let refused = dial(&mut theirs, &staging, &dialler)
            .await
            .expect_err("a staging node must not join production");
        // The listener refuses and closes, so the dialler sees the
        // close; what matters is that neither end opened a link.
        assert!(
            matches!(
                refused,
                HandshakeError::Io(_) | HandshakeError::Unauthenticated { .. }
            ),
            "{refused:?}"
        );
        assert!(matches!(
            accepting
                .await
                .expect("no panic")
                .expect_err("the names differ"),
            HandshakeError::Unauthenticated {
                role: Role::Dialler
            }
        ));
    }

    // -----------------------------------------------------------
    // The two attacks
    // -----------------------------------------------------------

    /// A captured transcript does not authenticate a later connection.
    ///
    /// Every byte one end sent in a handshake that succeeded is
    /// captured and replayed, exactly, at a fresh listener. It must not
    /// get in — because the listener's nonce is fresh, so the captured
    /// proof is a proof over a transcript naming a challenge nobody
    /// issued this time.
    ///
    /// This is also what catches a nonce that is not fresh: reuse it
    /// across connections and the replay works.
    #[tokio::test]
    async fn a_captured_transcript_does_not_authenticate_a_later_connection() {
        let config = config("a-shared-secret");
        let (dialler, listener) = (identity("edge-1"), identity("edge-2"));

        // A handshake that succeeds, with a tap on the wire: every
        // byte the dialler sent is kept.
        let (mut wire, mut accepting_side) = pair();
        let mut captured = Vec::new();
        let accepting = {
            let config = config.clone();
            let listener = listener.clone();
            tokio::spawn(async move { accept(&mut accepting_side, &config, &listener).await })
        };
        let mine = Greeting::mint(&dialler).expect("a greeting");
        captured.extend_from_slice(&mine.encode());
        write_all(&mut wire, &mine.encode()).await;
        let theirs = read_greeting(&mut wire).await.expect("a greeting");
        let script = transcript(Role::Dialler, config.name(), &mine, &theirs);
        let sent = proof(config.secret(), &script);
        captured.extend_from_slice(&sent);
        write_all(&mut wire, &sent).await;
        accepting
            .await
            .expect("no panic")
            .expect("this handshake is the real one");

        // The same bytes, at a fresh listener.
        let (mut replay, mut accepting_side) = pair();
        let accepting = {
            let config = config.clone();
            let listener = listener.clone();
            tokio::spawn(async move { accept(&mut accepting_side, &config, &listener).await })
        };
        write_all(&mut replay, &captured).await;
        let refused = accepting
            .await
            .expect("no panic")
            .expect_err("a recording is not a proof");
        assert!(
            matches!(
                refused,
                HandshakeError::Unauthenticated {
                    role: Role::Dialler
                }
            ),
            "{refused:?}"
        );
    }

    /// An answer harvested in one role is the wrong answer in the
    /// other.
    ///
    /// **The reflection is performed, not described.** The attacker
    /// here knows no secret at all. It plays the listener to an honest
    /// dialler, lets that dialler prove itself first — which is what
    /// the protocol has the dialler do — and feeds the dialler's own
    /// thirty-two bytes straight back as the listener's answer. Without
    /// the role byte the two proofs over one connection are the same
    /// bytes, so the echo authenticates, and this node opens a link to
    /// something that knows nothing.
    ///
    /// Then it opens the second connection, back to the same honest
    /// node's listening end, replays the challenge it was given, and
    /// feeds the harvested answer in the other role.
    #[tokio::test]
    async fn an_answer_harvested_in_one_role_is_the_wrong_answer_in_the_other() {
        let config = config("a-shared-secret");
        let honest = identity("edge-1");

        // Connection one. The honest node dials; the attacker answers
        // by hand and harvests the dialler's proof.
        let (mut attacker, mut honest_side) = pair();
        let dialling = {
            let config = config.clone();
            let honest = honest.clone();
            tokio::spawn(async move { dial(&mut honest_side, &config, &honest).await })
        };
        let theirs = read_greeting(&mut attacker)
            .await
            .expect("the honest node greets first");
        let mine = greeting("attacker", 0x5a);
        write_all(&mut attacker, &mine.encode()).await;
        let harvested = read_proof(&mut attacker)
            .await
            .expect("the dialler proves first, which is the answer to harvest");

        // The reflection: the answer, straight back, in the other role.
        write_all(&mut attacker, &harvested).await;
        let refused = dialling
            .await
            .expect("no panic")
            .expect_err("an echo of our own proof is not the peer's proof");
        assert!(
            matches!(
                refused,
                HandshakeError::Unauthenticated {
                    role: Role::Listener
                }
            ),
            "the dialler must refuse its own proof handed back: {refused:?}"
        );

        // And it is the role byte that refused it. The harvested bytes
        // are exactly this transcript's proof in the dialler's role;
        // the listener's role over the same transcript is different,
        // and that difference is the only thing between the two.
        let script = transcript(Role::Dialler, config.name(), &theirs, &mine);
        assert_eq!(
            proof(config.secret(), &script),
            harvested,
            "the harvested answer is the dialler's proof over this transcript"
        );
        let reflected = transcript(Role::Listener, config.name(), &theirs, &mine);
        assert_ne!(
            proof(config.secret(), &reflected),
            harvested,
            "and the same transcript in the other role is a different answer, \
             which is the whole of what the role byte buys"
        );

        // Connection two: back to the honest node, this time to the
        // end that accepts, replaying the challenge from connection
        // one and feeding the harvested answer back.
        let (mut attacker, mut honest_side) = pair();
        let accepting = {
            let config = config.clone();
            let honest = honest.clone();
            tokio::spawn(async move { accept(&mut honest_side, &config, &honest).await })
        };
        write_all(&mut attacker, &mine.encode()).await;
        let _fresh = read_greeting(&mut attacker).await.expect("a greeting");
        write_all(&mut attacker, &harvested).await;
        let refused = accepting
            .await
            .expect("no panic")
            .expect_err("a harvested answer is not a proof for this connection");
        assert!(
            matches!(
                refused,
                HandshakeError::Unauthenticated {
                    role: Role::Dialler
                }
            ),
            "{refused:?}"
        );
    }

    /// The listener does not answer a dialler that cannot prove
    /// itself.
    ///
    /// Section 8.1: an unauthenticated dialler learns nothing. What it
    /// must not learn is a proof — thirty-two bytes made with the
    /// secret, over a transcript the dialler chose half of. The
    /// assertion is on what came back on the wire, not on the error,
    /// because the error is the listener's own view and the attacker
    /// only ever sees the bytes.
    #[tokio::test]
    async fn an_unauthenticated_dialler_is_told_nothing() {
        let (mut impostor, mut honest) = pair();
        let listener = identity("edge-2");
        let accepting = {
            let config = config("the-real-secret");
            let listener = listener.clone();
            tokio::spawn(async move { accept(&mut honest, &config, &listener).await })
        };

        let mine = greeting("impostor", 0x33);
        write_all(&mut impostor, &mine.encode()).await;
        let _theirs = read_greeting(&mut impostor).await.expect("a greeting");
        // Thirty-two bytes of nothing in particular.
        write_all(&mut impostor, &[0x00; PROOF_BYTES]).await;

        accepting
            .await
            .expect("no panic")
            .expect_err("a guess is not a proof");

        // Nothing more came back. The greeting was answered, because
        // the transcript needs both nonces before either end can
        // prove anything; the proof was not.
        let mut nothing = [0_u8; PROOF_BYTES];
        let read = impostor
            .read(&mut nothing)
            .await
            .expect("the closed end reads as an end");
        assert_eq!(
            read, 0,
            "the listener answered a dialler that had not proved itself"
        );
    }

    // -----------------------------------------------------------
    // The identity, and the transcript
    // -----------------------------------------------------------

    /// A greeting carrying our own identity is refused, at both ends.
    ///
    /// That is a copied store directory or a cloned image. Asserted at
    /// both ends because either may be the one dialled.
    #[tokio::test]
    async fn a_greeting_carrying_our_own_identity_is_refused() {
        let config = config("a-shared-secret");
        let me = identity("edge-1");
        // The twin: the same identity, which is what copying a store
        // directory produces.
        let twin = me.clone();

        let (mut theirs, mut ours) = pair();
        let accepting = {
            let config = config.clone();
            let me = me.clone();
            tokio::spawn(async move { accept(&mut ours, &config, &me).await })
        };
        let refused_by_dialler = dial(&mut theirs, &config, &twin)
            .await
            .expect_err("refused");
        let refused_by_listener = accepting.await.expect("no panic").expect_err("refused");

        for refusal in [refused_by_dialler, refused_by_listener] {
            match refusal {
                HandshakeError::OurOwnIdentity { id } => assert_eq!(id, me.id()),
                // Whichever end notices first closes the connection,
                // and the other may see the close rather than the
                // greeting. What must not happen is a link.
                HandshakeError::Io(_) => {}
                other => panic!("a copied identity was reported as {other:?}"),
            }
        }
    }

    /// Sliding a field boundary changes the transcript.
    ///
    /// Every length prefix earns its place, and this is the test that
    /// says which ones. `"ab" + "c"` and `"a" + "bc"` are the same
    /// bytes concatenated; two handshakes that hash to one transcript
    /// are two handshakes with one proof. Each pair below is the same
    /// characters split differently, and every transcript must be
    /// distinct.
    #[test]
    fn sliding_a_field_boundary_changes_the_transcript() {
        let id = |byte: u8| NodeId::from_bytes([byte; 16]);
        let make = |cluster: &str, dialler_name: &str, listener_name: &str| {
            transcript(
                Role::Dialler,
                cluster,
                &Greeting {
                    id: id(1),
                    name: dialler_name.to_owned(),
                    nonce: [0x01; NONCE_BYTES],
                },
                &Greeting {
                    id: id(2),
                    name: listener_name.to_owned(),
                    nonce: [0x02; NONCE_BYTES],
                },
            )
        };

        // The two names are adjacent in the transcript, which is where
        // a boundary can actually slide.
        let mut seen = std::collections::BTreeSet::new();
        for (cluster, dialler, listener) in [
            ("prod", "ab", "c"),
            ("prod", "a", "bc"),
            ("prod", "abc", ""),
            ("prod", "", "abc"),
            ("pro", "dab", "c"),
            ("p", "rodab", "c"),
        ] {
            assert!(
                seen.insert(make(cluster, dialler, listener)),
                "two distinct handshakes produced one transcript: \
                 cluster {cluster:?}, names {dialler:?} and {listener:?}"
            );
        }
    }

    /// The role byte is in the transcript and is the only difference.
    ///
    /// Stated on its own as well as through the reflection above,
    /// because this is the property and the reflection is the
    /// consequence.
    #[test]
    fn the_two_roles_hash_the_same_connection_differently() {
        let dialler = greeting("edge-1", 0x01);
        let listener = greeting("edge-2", 0x02);
        let as_dialler = transcript(Role::Dialler, "production", &dialler, &listener);
        let as_listener = transcript(Role::Listener, "production", &dialler, &listener);

        assert_ne!(as_dialler, as_listener);
        assert_eq!(
            as_dialler.len(),
            as_listener.len(),
            "the two differ in one byte, and it is the role"
        );
        // And they differ in exactly that byte, so the role is what
        // made them differ rather than something that moved beside it.
        let differences = as_dialler
            .iter()
            .zip(&as_listener)
            .filter(|(one, other)| one != other)
            .count();
        assert_eq!(differences, 1, "one byte, and it is the role byte");
    }

    /// Both ends build the same transcript from their own point of
    /// view.
    ///
    /// The dialler's greeting goes in the dialler's slot whichever end
    /// is doing the building. A transcript built "mine then theirs"
    /// would give two ends two different byte strings, and the
    /// handshake would never authenticate — or, worse, would
    /// authenticate in one direction only.
    #[test]
    fn both_ends_build_one_transcript() {
        let dialler = greeting("edge-1", 0x01);
        let listener = greeting("edge-2", 0x02);
        assert_eq!(
            transcript(Role::Dialler, "production", &dialler, &listener),
            transcript(Role::Dialler, "production", &dialler, &listener)
        );
        assert_ne!(
            transcript(Role::Dialler, "production", &dialler, &listener),
            transcript(Role::Dialler, "production", &listener, &dialler),
            "which end is which is part of what is proved"
        );
    }

    /// A greeting round trips, and a fresh one is fresh.
    #[tokio::test]
    async fn a_greeting_round_trips_and_its_nonce_is_fresh() {
        let me = identity("edge-1");
        let first = Greeting::mint(&me).expect("a greeting");
        let second = Greeting::mint(&me).expect("another");
        assert_ne!(
            first.nonce, second.nonce,
            "a nonce reused across connections is a captured transcript that works"
        );

        let (mut writing, mut reading) = pair();
        write_all(&mut writing, &first.encode()).await;
        assert_eq!(read_greeting(&mut reading).await.expect("it reads"), first);
    }

    /// Something that is not a greeting is refused as such.
    #[tokio::test]
    async fn bytes_that_are_not_a_greeting_are_refused_by_what_is_wrong() {
        let (mut writing, mut reading) = pair();
        write_all(&mut writing, &[0x00; GREETING_HEAD]).await;
        assert!(matches!(
            read_greeting(&mut reading)
                .await
                .expect_err("not a greeting"),
            HandshakeError::NotAGreeting
        ));

        let mut wrong_version = Greeting::mint(&identity("edge-1"))
            .expect("a greeting")
            .encode();
        wrong_version[GREETING_MAGIC.len()] = GREETING_VERSION + 7;
        let (mut writing, mut reading) = pair();
        write_all(&mut writing, &wrong_version).await;
        match read_greeting(&mut reading)
            .await
            .expect_err("a version this build does not speak")
        {
            HandshakeError::UnknownGreeting { found, expected } => {
                assert_eq!(found, GREETING_VERSION + 7);
                assert_eq!(expected, GREETING_VERSION);
            }
            other => panic!("reported as {other:?}"),
        }
    }

    /// A name longer than a greeting can carry is refused before a
    /// connection is opened.
    #[test]
    fn a_name_too_long_for_a_greeting_is_refused() {
        let long = NodeIdentity::new(NodeId::mint(), "n".repeat(MAX_NAME_BYTES + 1));
        assert!(matches!(
            Greeting::mint(&long).expect_err("it will not fit"),
            HandshakeError::NameTooLong { .. }
        ));
        let longest = NodeIdentity::new(NodeId::mint(), "n".repeat(MAX_NAME_BYTES));
        Greeting::mint(&longest).expect("the longest name that fits, fits");
    }
}
