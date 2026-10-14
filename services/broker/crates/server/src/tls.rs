//! Section 12.1's TLS listener, and the certificate that renews without
//! a restart.
//!
//! # The resolver is the hot reload
//!
//! rustls asks a [`ResolvesServerCert`] for a certificate on every
//! handshake. Ours holds the current pair behind a lock, and a task
//! re-reads the two files every `reload` and swaps the pair in when both
//! parse and match. A handshake in flight keeps the `Arc` it was given;
//! the next one gets the new pair. Nothing is reloaded on a signal,
//! because a renewal tool writes files and does not know what signals
//! this process takes; and nothing watches the files through a
//! platform notification API, because those differ per platform and
//! miss a replace-by-rename on some, while a poll of two files a minute
//! costs nothing measurable.
//!
//! # A bad write keeps the old pair
//!
//! Renewal writes two files, usually by rename and sometimes not, and
//! there is a moment when one is new and the other old, or one is
//! empty. A pair that does not load, or does not match, is logged once
//! per change and the previous pair stays. Counted both ways, on
//! `sdmqtt_tls_certificate_reloads_total{outcome}`.
//!
//! # What is deliberately not here
//!
//! Client certificates are not asked for: section 12.1 does not list
//! them, and a listener that asked would need a trust store and a
//! policy for what a certificate's name means, which is authorization
//! work. An encrypted private key is refused rather than prompted for,
//! because a broker starts under a supervisor with nobody at a keyboard.
//! The certificate's expiry is not a metric, because reading it needs an
//! X.509 parser this build does not otherwise carry.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, SystemTime};

use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;

use crate::metrics::TlsCounters;

/// Where the certificate and key are, and how often to look again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// The certificate chain, PEM, leaf first.
    pub certificate: PathBuf,
    /// The private key, PEM, unencrypted.
    pub key: PathBuf,
    /// How often the two files are re-read.
    pub reload: Duration,
}

/// Why a pair of files is not a certificate and key.
///
/// Each names the file, because the message ends the process at
/// startup and an operator fixing it needs to know which of two paths
/// to look at.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The file could not be read at all.
    #[error("{path} could not be read: {why}")]
    Unreadable {
        /// Which file.
        path: PathBuf,
        /// What the operating system said.
        why: io::Error,
    },
    /// The file is not PEM, or is PEM with a section that does not end.
    #[error("{path} is not PEM: {why}")]
    NotPem {
        /// Which file.
        path: PathBuf,
        /// What the parser said.
        why: String,
    },
    /// The file parsed and held no certificate.
    #[error("{path} holds no certificate")]
    NoCertificate {
        /// Which file.
        path: PathBuf,
    },
    /// The file parsed and held no private key.
    #[error("{path} holds no private key; an encrypted key is not supported, decrypt it first")]
    NoKey {
        /// Which file.
        path: PathBuf,
    },
    /// The key is not the one the certificate was issued for.
    #[error("the private key does not match the certificate's public key")]
    Mismatch,
    /// The key is of a kind this build does not sign with.
    #[error("the key type is not one this build signs with: {why}")]
    Unsupported {
        /// What rustls said.
        why: String,
    },
}

/// Read and check the pair once, without keeping it.
///
/// What a configuration check calls at startup, so that a listener
/// whose files are wrong refuses to start rather than failing every
/// handshake.
pub fn check(settings: &Settings) -> Result<(), Error> {
    read_pair(settings).map(drop)
}

fn read_pair(settings: &Settings) -> Result<CertifiedKey, Error> {
    let certificates: Vec<CertificateDer<'static>> =
        CertificateDer::pem_file_iter(&settings.certificate)
            .map_err(|why| pem_error(&settings.certificate, why))?
            .collect::<Result<_, _>>()
            .map_err(|why| pem_error(&settings.certificate, why))?;
    if certificates.is_empty() {
        return Err(Error::NoCertificate {
            path: settings.certificate.clone(),
        });
    }
    let key = PrivateKeyDer::from_pem_file(&settings.key).map_err(|why| match why {
        rustls_pki_types::pem::Error::NoItemsFound => Error::NoKey {
            path: settings.key.clone(),
        },
        other => pem_error(&settings.key, other),
    })?;
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let signing =
        provider
            .key_provider
            .load_private_key(key)
            .map_err(|why| Error::Unsupported {
                why: why.to_string(),
            })?;
    let pair = CertifiedKey::new(certificates, signing);
    pair.keys_match().map_err(|_| Error::Mismatch)?;
    Ok(pair)
}

fn pem_error(path: &Path, why: rustls_pki_types::pem::Error) -> Error {
    match why {
        rustls_pki_types::pem::Error::Io(why) => Error::Unreadable {
            path: path.to_owned(),
            why,
        },
        rustls_pki_types::pem::Error::NoItemsFound => Error::NoCertificate {
            path: path.to_owned(),
        },
        other => Error::NotPem {
            path: path.to_owned(),
            why: other.to_string(),
        },
    }
}

/// The pair as rustls sees it, swappable.
#[derive(Debug)]
struct Resolver {
    current: RwLock<Arc<CertifiedKey>>,
}

impl ResolvesServerCert for Resolver {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(
            &self.current.read().unwrap_or_else(PoisonError::into_inner),
        ))
    }
}

/// One listener's TLS: accepts handshakes, re-reads its files.
pub struct Acceptor {
    settings: Settings,
    resolver: Arc<Resolver>,
    acceptor: TlsAcceptor,
    counters: Arc<TlsCounters>,
    /// Whether the last reload failed, so a failing file is logged on
    /// the change and not on every poll.
    failing: AtomicBool,
}

impl std::fmt::Debug for Acceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Acceptor")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl Acceptor {
    /// Read the pair and build the acceptor. Refuses a pair that does
    /// not load or does not match, naming the file.
    pub fn load(settings: &Settings, counters: Arc<TlsCounters>) -> Result<Self, Error> {
        let pair = read_pair(settings)?;
        let resolver = Arc::new(Resolver {
            current: RwLock::new(Arc::new(pair)),
        });
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("the provider supports the default versions")
        .with_no_client_auth()
        .with_cert_resolver(Arc::clone(&resolver) as Arc<dyn ResolvesServerCert>);
        Ok(Self {
            settings: settings.clone(),
            resolver,
            acceptor: TlsAcceptor::from(Arc::new(config)),
            counters,
            failing: AtomicBool::new(false),
        })
    }

    /// The handshake.
    pub async fn accept<S>(&self, socket: S) -> io::Result<TlsStream<S>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        self.acceptor.accept(socket).await
    }

    /// Re-read the files every `reload`, for ever.
    ///
    /// Driven by the files' modification times: a poll that finds both
    /// unchanged reads nothing. The first poll compares against nothing,
    /// so a pair replaced between [`Acceptor::load`] and the first poll
    /// is picked up.
    pub async fn reloading(&self) {
        let mut last_seen: Option<(SystemTime, SystemTime)> = None;
        loop {
            tokio::time::sleep(self.settings.reload).await;
            let stamps = (
                modified(&self.settings.certificate),
                modified(&self.settings.key),
            );
            let (Some(certificate), Some(key)) = stamps else {
                self.note_failure("a certificate or key file is missing");
                continue;
            };
            if last_seen == Some((certificate, key)) {
                continue;
            }
            last_seen = Some((certificate, key));
            match read_pair(&self.settings) {
                Ok(pair) => {
                    *self
                        .resolver
                        .current
                        .write()
                        .unwrap_or_else(PoisonError::into_inner) = Arc::new(pair);
                    self.counters.reloaded().inc();
                    self.failing.store(false, Ordering::Relaxed);
                    tracing::info!(
                        certificate = %self.settings.certificate.display(),
                        "the TLS certificate was reloaded"
                    );
                }
                Err(why) => self.note_failure(&why.to_string()),
            }
        }
    }

    fn note_failure(&self, why: &str) {
        self.counters.reload_failed().inc();
        if !self.failing.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                certificate = %self.settings.certificate.display(),
                why,
                "the TLS certificate could not be reloaded; the previous one is still served"
            );
        }
    }

    /// The leaf certificate being served, DER. For a test that wants to
    /// know whether a renewal landed, and for the admin API.
    pub fn certificate_der(&self) -> Vec<u8> {
        self.resolver
            .current
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .cert[0]
            .to_vec()
    }
}

fn modified(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A certificate and key on disk, minted for the test, with
    /// `localhost` as the name.
    pub(crate) fn minted(directory: &Path, name: &str) -> (PathBuf, PathBuf) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("rcgen mints a certificate");
        let certificate = directory.join(format!("{name}.pem"));
        let key = directory.join(format!("{name}.key"));
        std::fs::write(&certificate, cert.cert.pem()).expect("written");
        std::fs::write(&key, cert.signing_key.serialize_pem()).expect("written");
        (certificate, key)
    }

    fn settings(certificate: PathBuf, key: PathBuf, reload: Duration) -> Settings {
        Settings {
            certificate,
            key,
            reload,
        }
    }

    #[test]
    fn a_certificate_and_key_load_and_a_mismatched_pair_is_refused() {
        let directory = tempfile::tempdir().expect("a directory");
        let (certificate, key) = minted(directory.path(), "one");
        let (_, other_key) = minted(directory.path(), "two");
        Acceptor::load(
            &settings(certificate.clone(), key, Duration::from_secs(60)),
            TlsCounters::detached(),
        )
        .expect("a matching pair loads");
        let refused = Acceptor::load(
            &settings(certificate, other_key, Duration::from_secs(60)),
            TlsCounters::detached(),
        )
        .expect_err("the key does not match the certificate");
        assert!(matches!(refused, Error::Mismatch), "{refused:?}");
    }

    #[test]
    fn an_empty_key_file_and_a_missing_certificate_name_themselves() {
        let directory = tempfile::tempdir().expect("a directory");
        let (certificate, key) = minted(directory.path(), "one");
        std::fs::write(&key, b"").expect("emptied");
        let refused = check(&settings(
            certificate.clone(),
            key.clone(),
            Duration::from_secs(60),
        ))
        .expect_err("no key in an empty file");
        assert!(
            matches!(&refused, Error::NoKey { path } if *path == key),
            "{refused:?}"
        );
        let missing = directory.path().join("nowhere.pem");
        let refused = check(&settings(missing.clone(), key, Duration::from_secs(60)))
            .expect_err("no such file");
        assert!(
            matches!(&refused, Error::Unreadable { path, .. } if *path == missing),
            "{refused:?}"
        );
    }

    /// A renewal half-written keeps the old pair; a renewal that lands
    /// is served next.
    #[tokio::test]
    async fn a_reload_swaps_a_good_pair_and_keeps_the_old_one_through_a_bad_write() {
        let directory = tempfile::tempdir().expect("a directory");
        let (certificate, key) = minted(directory.path(), "first");
        let counters = TlsCounters::detached();
        let acceptor = Arc::new(
            Acceptor::load(
                &settings(certificate.clone(), key.clone(), Duration::from_millis(30)),
                Arc::clone(&counters),
            )
            .expect("loads"),
        );
        let before = acceptor.certificate_der();
        let reloader = tokio::spawn({
            let acceptor = Arc::clone(&acceptor);
            async move { acceptor.reloading().await }
        });

        // A half-written renewal: the certificate is empty for a moment.
        std::fs::write(&certificate, b"").expect("emptied");
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(acceptor.certificate_der(), before, "the old pair is kept");
        assert!(counters.reload_failed().get() >= 1);
        assert_eq!(counters.reloaded().get(), 0);

        // Then the renewal lands.
        let (renewed_cert, renewed_key) = minted(directory.path(), "second");
        std::fs::copy(&renewed_key, &key).expect("key replaced");
        std::fs::copy(&renewed_cert, &certificate).expect("certificate replaced");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while acceptor.certificate_der() == before {
            assert!(
                std::time::Instant::now() < deadline,
                "the renewal was never picked up"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(counters.reloaded().get(), 1);
        reloader.abort();
    }
}
