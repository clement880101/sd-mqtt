//! The browser interface: embedded, precompressed, and negotiated.
//!
//! Section 11.2: the production bundle is precompressed at build time
//! and embedded into the binary, "served with content negotiation:
//! brotli, gzip and identity from three embedded blobs, chosen by a
//! parsed `Accept-Encoding` including its quality values, answered
//! with a `Vary` header and a distinct entity tag per encoding".
//!
//! The blobs and their tags come from `build.rs`. What is here is the
//! choosing.
//!
//! # The easy version of this is wrong in three specific ways
//!
//! Each of them looks like a working interface until it does not, and
//! each has a test of its own.
//!
//! - **A quality value of zero means *not acceptable*.** `identity;q=0`
//!   forbids the uncompressed form rather than asking for it, and `*`
//!   is a wildcard standing for every coding not named beside it. A
//!   reader that looks for the substring `gzip` answers a client that
//!   said `gzip;q=0` with gzip.
//! - **Without `Vary`, a shared cache poisons itself.** One client's
//!   brotli bytes are handed to the next client, which asked for gzip
//!   and cannot read them. The response is not a function of the path
//!   alone and has to say so.
//! - **Two encodings are two representations.** One entity tag across
//!   both makes a conditional request answer for the wrong one: a
//!   browser holding the gzip bytes asks with its tag, gets `304` on a
//!   request it made in identity, and renders compressed bytes as
//!   text.
//!
//! # Not built is a supported state
//!
//! Section 11.2 again: a build of the binary must not require Node. In
//! a tree with no `services/web/dist`, [`INTERFACE_BUILT`] is false and
//! the root serves [`PLACEHOLDER`], which says the interface was not
//! built and what to run. That document is embedded either way, so it
//! is a document the tests can always read rather than one that only
//! exists in the builds that do not test it.

use axum::body::Body;
use axum::http::header::{
    ACCEPT, ACCEPT_ENCODING, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_TYPE, ETAG, IF_NONE_MATCH,
    VARY,
};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};

use crate::error::{ApiError, ErrorCode};

/// One document in one encoding, with the tag for exactly these bytes.
pub(crate) struct Encoded {
    /// What is sent.
    pub bytes: &'static [u8],
    /// Its entity tag, quoted and ready for the header.
    pub etag: &'static str,
}

/// One document, in the three encodings it was precompressed into.
pub(crate) struct Asset {
    /// The path it is served at, with a leading slash.
    pub path: &'static str,
    /// What it is.
    pub content_type: &'static str,
    /// How long it may be kept.
    pub cache_control: &'static str,
    /// The uncompressed form.
    pub identity: Encoded,
    /// The gzip form.
    pub gzip: Encoded,
    /// The brotli form.
    pub brotli: Encoded,
}

include!(concat!(env!("OUT_DIR"), "/interface.rs"));

/// The document the interface is entered through.
const INDEX: &str = "/index.html";

/// Where the third-party attribution is served.
///
/// **Not under `/assets/`**, and not a document of the interface. It
/// is served whether or not one was built, because the crates
/// compiled into this binary are attributed either way, and it is
/// revalidated rather than cached forever because its name carries no
/// hash of its contents.
///
/// A plain path with no version in it, so that the sentence in the
/// readme naming it stays true across releases. The same bytes are
/// [`NOTICES`], which `sdmqtt licenses` prints for anyone who has the
/// binary and no listener to point a browser at.
pub const NOTICES_PATH: &str = "/third-party-notices.txt";

/// An encoding this listener can answer with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
    /// `br`.
    Brotli,
    /// `gzip`.
    Gzip,
    /// No content coding at all.
    Identity,
}

impl Encoding {
    /// The token that names this encoding on the wire.
    fn token(self) -> &'static str {
        match self {
            Encoding::Brotli => "br",
            Encoding::Gzip => "gzip",
            Encoding::Identity => "identity",
        }
    }
}

/// What a client said about each coding, in thousandths.
///
/// `None` is "did not mention it", which is not the same as zero and
/// is why these are options rather than defaulted numbers.
#[derive(Debug, Default)]
struct Preferences {
    /// `br`.
    brotli: Option<u16>,
    /// `gzip`, and its `x-gzip` alias.
    gzip: Option<u16>,
    /// `identity`.
    identity: Option<u16>,
    /// `*`, standing for everything not named.
    wildcard: Option<u16>,
}

/// A quality value in thousandths, or `None` if it will not parse.
///
/// An unparseable one is ignored by the caller rather than refused.
/// The cost of ignoring it is a response the client can read; the
/// cost of refusing is an interface that does not load over a header
/// nobody typed by hand.
fn quality(value: &str) -> Option<u16> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    let whole: u16 = whole.parse().ok()?;
    if whole > 1 || fraction.len() > 3 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let mut milli = whole * 1000;
    let mut scale = 100;
    for byte in fraction.bytes() {
        milli += u16::from(byte - b'0') * scale;
        scale /= 10;
    }
    (milli <= 1000).then_some(milli)
}

/// Read one `Accept-Encoding` field into what it says about each
/// coding.
///
/// Codings this listener does not have are dropped: they cannot be
/// chosen, and the wildcard is what speaks for them.
fn parse(header: &str) -> Preferences {
    let mut preferences = Preferences::default();
    for element in header.split(',') {
        let mut parts = element.split(';');
        let coding = parts.next().unwrap_or("").trim().to_ascii_lowercase();
        if coding.is_empty() {
            continue;
        }
        let mut weight = 1000;
        for parameter in parts {
            let parameter = parameter.trim().to_ascii_lowercase();
            if let Some(value) = parameter.strip_prefix("q=") {
                if let Some(parsed) = quality(value) {
                    weight = parsed;
                }
            }
        }
        let slot = match coding.as_str() {
            "br" => &mut preferences.brotli,
            // RFC 9110 keeps `x-gzip` as a deprecated alias.
            "gzip" | "x-gzip" => &mut preferences.gzip,
            "identity" => &mut preferences.identity,
            "*" => &mut preferences.wildcard,
            _ => continue,
        };
        // The first mention wins, so a client that names a coding
        // twice cannot have the second reading change the answer
        // depending on which way this loop happened to run.
        if slot.is_none() {
            *slot = Some(weight);
        }
    }
    preferences
}

/// Which encoding to answer with, or `None` when none is acceptable.
///
/// `None` is a `406`, not a licence to send the uncompressed bytes
/// anyway. A client that wrote `identity;q=0` said the uncompressed
/// form is unacceptable, and answering with it is answering with
/// something the client told us it cannot use.
///
/// The defaults are RFC 9110's. A coding the client did not mention is
/// unacceptable unless a wildcard speaks for it, **except** identity,
/// which is acceptable unless it is excluded — by naming it at zero,
/// or by a wildcard at zero with no entry of its own.
pub fn negotiate(header: Option<&str>) -> Option<Encoding> {
    // No field at all means anything is acceptable, and the honest
    // reading of "the client said nothing" is to send what it asked
    // for without a coding it never mentioned.
    let Some(header) = header else {
        return Some(Encoding::Identity);
    };
    let preferences = parse(header);
    let weights = [
        (
            Encoding::Identity,
            preferences
                .identity
                .or(preferences.wildcard)
                .unwrap_or(1000),
        ),
        (
            Encoding::Gzip,
            preferences.gzip.or(preferences.wildcard).unwrap_or(0),
        ),
        (
            Encoding::Brotli,
            preferences.brotli.or(preferences.wildcard).unwrap_or(0),
        ),
    ];
    // `max_by_key` keeps the last of equal keys, and the list is in
    // increasing order of what this listener would rather send, so a
    // tie goes to brotli. That is the server's choice to make: the
    // client said the two are equally acceptable.
    weights
        .into_iter()
        .filter(|(_, weight)| *weight > 0)
        .max_by_key(|(_, weight)| *weight)
        .map(|(encoding, _)| encoding)
}

/// The document served at `path`, if this binary has one.
fn asset_for(path: &str) -> Option<&'static Asset> {
    let path = if path == "/" { INDEX } else { path };
    // Before the interface's own documents, and outside the `ASSETS`
    // table, because it is answered in a build that has no table at
    // all: the crates are in the binary whether or not the bundle is.
    if path == NOTICES_PATH {
        return Some(&NOTICES_ASSET);
    }
    if ASSETS.is_empty() {
        return (path == INDEX).then_some(&PLACEHOLDER);
    }
    ASSETS
        .binary_search_by(|asset| asset.path.cmp(path))
        .ok()
        .map(|found| &ASSETS[found])
}

/// One document's bytes in one encoding.
fn encoded(asset: &'static Asset, encoding: Encoding) -> &'static Encoded {
    match encoding {
        Encoding::Brotli => &asset.brotli,
        Encoding::Gzip => &asset.gzip,
        Encoding::Identity => &asset.identity,
    }
}

/// Whether `If-None-Match` covers `etag`.
///
/// `*` matches anything that exists. A `W/` prefix is stripped, which
/// is the weak comparison this header is defined to use.
fn matches(header: &str, etag: &str) -> bool {
    header.split(',').any(|candidate| {
        let candidate = candidate.trim();
        candidate == "*" || candidate.trim_start_matches("W/") == etag
    })
}

/// Answer for one document.
fn serve(asset: &'static Asset, headers: &HeaderMap) -> Response {
    let accept = headers
        .get(ACCEPT_ENCODING)
        .and_then(|value| value.to_str().ok());
    let Some(encoding) = negotiate(accept) else {
        let mut response = ApiError::new(
            ErrorCode::NotAcceptable,
            "this resource can be sent as brotli, gzip or uncompressed, and \
             Accept-Encoding excluded all three",
        )
        .into_response();
        vary(&mut response);
        return response;
    };
    let encoded = encoded(asset, encoding);

    let conditional = headers
        .get(IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| matches(value, encoded.etag));
    let mut response = if conditional {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        (StatusCode::OK, Body::from(encoded.bytes)).into_response()
    };

    let out = response.headers_mut();
    out.insert(CONTENT_TYPE, HeaderValue::from_static(asset.content_type));
    out.insert(CACHE_CONTROL, HeaderValue::from_static(asset.cache_control));
    out.insert(ETAG, HeaderValue::from_static(encoded.etag));
    if encoding != Encoding::Identity {
        out.insert(CONTENT_ENCODING, HeaderValue::from_static(encoding.token()));
    }
    vary(&mut response);
    response
}

/// Say that the answer depends on the requested encoding.
///
/// On every response this module produces, including the `304` and
/// the `406`: a cache that keeps either of those against a bare path
/// is as wrong as one that keeps the body.
fn vary(response: &mut Response) {
    response
        .headers_mut()
        .insert(VARY, HeaderValue::from_static("accept-encoding"));
}

/// Whether this request is a browser asking for a page.
fn wants_a_document(headers: &HeaderMap) -> bool {
    headers
        .get(ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("text/html"))
}

/// Whether this path belongs to the API rather than to the interface.
fn is_api(path: &str) -> bool {
    path.starts_with("/api/") || path.starts_with("/health") || path == "/metrics"
}

/// The router's fallback: the interface, or the one JSON error shape.
///
/// Registered as the fallback rather than as routes because the set
/// of documents is not known until the build script has run, and a
/// route table built at startup would be a second place for a path to
/// be spelled.
///
/// A path that is not a document is answered two ways. A browser
/// asking for a page gets the entry document, because the interface
/// routes in the browser and a reload on `/clients` is a request this
/// listener has never heard of. Anything else — a mistyped API path,
/// a fetch, a missing asset — gets the `404` every other failure of
/// this API gets.
pub(crate) async fn fallback(method: Method, uri: Uri, headers: HeaderMap) -> Response {
    if method != Method::GET && method != Method::HEAD {
        return ApiError::not_found("no such resource on this listener").into_response();
    }
    let path = uri.path();
    if let Some(asset) = asset_for(path) {
        return serve(asset, &headers);
    }
    if !is_api(path) && wants_a_document(&headers) {
        if let Some(index) = asset_for(INDEX) {
            return serve(index, &headers);
        }
    }
    ApiError::not_found("no such resource on this listener").into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing;
    use crate::{AdminConfig, AdminRunning, AdminServer, AdminState};
    use broker_core::broker::BrokerConfig;
    use sdmqtt_server::shared::Shared;
    use std::net::SocketAddr;
    use std::sync::Arc;

    /// A listener over a broker with no store, which is all any of
    /// these need.
    async fn serving() -> AdminRunning {
        let config = AdminConfig {
            address: "127.0.0.1:0"
                .parse::<SocketAddr>()
                .expect("a valid address"),
            ..AdminConfig::default()
        };
        let state = AdminState::new(Arc::new(Shared::new(BrokerConfig::default())));
        AdminServer::bind(&config, state)
            .await
            .expect("loopback needs no token")
    }

    /// `GET path` with the headers given, keeping the body as bytes.
    async fn get(address: SocketAddr, path: &str, headers: &[(&str, &str)]) -> testing::Raw {
        testing::get_raw(address, path, headers).await
    }

    // -----------------------------------------------------------
    // The choosing, on its own
    // -----------------------------------------------------------

    #[test]
    fn a_client_accepting_brotli_is_answered_with_brotli() {
        assert_eq!(negotiate(Some("br")), Some(Encoding::Brotli));
        assert_eq!(
            negotiate(Some("gzip, deflate, br, zstd")),
            Some(Encoding::Brotli)
        );
    }

    #[test]
    fn a_client_accepting_only_gzip_is_answered_with_gzip() {
        assert_eq!(negotiate(Some("gzip")), Some(Encoding::Gzip));
        assert_eq!(negotiate(Some("gzip, deflate")), Some(Encoding::Gzip));
    }

    #[test]
    fn a_client_that_said_nothing_is_answered_uncompressed() {
        assert_eq!(negotiate(None), Some(Encoding::Identity));
        // An empty field is not the absent field: it says the client
        // wants no content coding at all.
        assert_eq!(negotiate(Some("")), Some(Encoding::Identity));
    }

    /// **The one most often missed.** A quality of zero is a refusal.
    #[test]
    fn a_quality_of_zero_is_a_refusal_and_not_a_preference() {
        // Named at zero, with nothing else acceptable: nothing can be
        // sent.
        assert_eq!(negotiate(Some("identity;q=0")), None);
        assert_eq!(negotiate(Some("identity;q=0, gzip;q=0, br;q=0")), None);
        // Named at zero with something else: that something else.
        assert_eq!(
            negotiate(Some("identity;q=0, gzip")),
            Some(Encoding::Gzip),
            "the uncompressed form was refused, so gzip is the answer"
        );
        // A coding refused is not a coding requested.
        assert_eq!(
            negotiate(Some("gzip;q=0")),
            Some(Encoding::Identity),
            "gzip at zero forbids gzip; it does not ask for it"
        );
        assert_eq!(
            negotiate(Some("br;q=0, gzip")),
            Some(Encoding::Gzip),
            "brotli was refused even though it is what we would rather send"
        );
    }

    /// The wildcard, which has to be read for the refusals to work.
    #[test]
    fn the_wildcard_speaks_for_everything_not_named() {
        assert_eq!(
            negotiate(Some("*")),
            Some(Encoding::Brotli),
            "everything is acceptable, so the smallest is sent"
        );
        assert_eq!(
            negotiate(Some("*;q=0")),
            None,
            "everything, uncompressed included, was refused"
        );
        assert_eq!(
            negotiate(Some("*;q=0, identity")),
            Some(Encoding::Identity),
            "a name beats the wildcard"
        );
        assert_eq!(
            negotiate(Some("*;q=0, gzip;q=1")),
            Some(Encoding::Gzip),
            "and a name beats it in the other direction too"
        );
    }

    /// Quality values order the answer, rather than only gating it.
    ///
    /// Identity is named in each of these. It has to be: a coding
    /// nobody mentioned is unacceptable, **but identity is
    /// acceptable at full quality unless it is excluded**, so a
    /// field that only grudgingly allows brotli is a field that
    /// prefers the uncompressed form. That is the next assertion,
    /// and it is the reason these three say what they will take
    /// uncompressed.
    #[test]
    fn a_lower_quality_loses_to_a_higher_one() {
        assert_eq!(
            negotiate(Some("br;q=0.1, gzip;q=0.9, identity;q=0.2")),
            Some(Encoding::Gzip)
        );
        assert_eq!(
            negotiate(Some("br;q=0.9, gzip;q=0.1, identity;q=0.2")),
            Some(Encoding::Brotli)
        );
        assert_eq!(
            negotiate(Some("br;q=0.001, identity;q=0")),
            Some(Encoding::Brotli),
            "a tiny quality is still acceptable; only zero is not"
        );
    }

    /// The default nobody writes down, and it decides real requests.
    #[test]
    fn the_uncompressed_form_is_acceptable_unless_it_is_excluded() {
        assert_eq!(
            negotiate(Some("br;q=0.1")),
            Some(Encoding::Identity),
            "a grudging brotli loses to an uncompressed form the client \
             never said anything about"
        );
        assert_eq!(
            negotiate(Some("br;q=0.1, identity;q=0")),
            Some(Encoding::Brotli),
            "and wins the moment the client excludes the alternative"
        );
    }

    /// Whitespace, case and an unreadable quality.
    #[test]
    fn a_field_is_read_the_way_it_arrives() {
        assert_eq!(negotiate(Some("GZIP")), Some(Encoding::Gzip));
        assert_eq!(
            negotiate(Some("  BR ;  Q=0.5 , identity;q=0.1")),
            Some(Encoding::Brotli)
        );
        assert_eq!(
            negotiate(Some("gzip;q=banana")),
            Some(Encoding::Gzip),
            "an unreadable quality is ignored, not refused"
        );
        assert_eq!(negotiate(Some("x-gzip")), Some(Encoding::Gzip));
    }

    // -----------------------------------------------------------
    // The choosing, over a socket
    // -----------------------------------------------------------

    #[tokio::test]
    async fn the_three_encodings_arrive_as_themselves() {
        let admin = serving().await;
        let address = admin.address();

        let brotli = get(address, "/", &[("Accept-Encoding", "br")]).await;
        assert_eq!(brotli.status, 200);
        assert_eq!(brotli.header("content-encoding"), Some("br"));

        let gzip = get(address, "/", &[("Accept-Encoding", "gzip")]).await;
        assert_eq!(gzip.status, 200);
        assert_eq!(gzip.header("content-encoding"), Some("gzip"));
        assert_eq!(
            &gzip.body[..2],
            &[0x1f, 0x8b],
            "gzip's own magic number, not a label on other bytes"
        );

        let identity = get(address, "/", &[]).await;
        assert_eq!(identity.status, 200);
        assert_eq!(
            identity.header("content-encoding"),
            None,
            "the uncompressed form names no coding"
        );
        assert!(
            identity.body.starts_with(b"<!doctype html>"),
            "it is a document: {:?}",
            String::from_utf8_lossy(&identity.body[..identity.body.len().min(64)])
        );

        assert!(
            brotli.body.len() < identity.body.len(),
            "brotli is {} bytes against {} uncompressed, which is not compression",
            brotli.body.len(),
            identity.body.len()
        );

        admin.shutdown().await;
    }

    #[tokio::test]
    async fn a_client_that_refuses_every_encoding_is_refused() {
        let admin = serving().await;
        let refused = get(
            admin.address(),
            "/",
            &[("Accept-Encoding", "identity;q=0, gzip;q=0, br;q=0")],
        )
        .await;
        assert_eq!(
            refused.status,
            406,
            "it is refused, not quietly served uncompressed: {:?}",
            String::from_utf8_lossy(&refused.body)
        );
        assert!(
            String::from_utf8_lossy(&refused.body).contains("not_acceptable"),
            "in the one shape every failure of this API takes: {:?}",
            String::from_utf8_lossy(&refused.body)
        );
        assert_eq!(
            refused.header("vary"),
            Some("accept-encoding"),
            "a cache must not keep this against the bare path either"
        );
        admin.shutdown().await;
    }

    #[tokio::test]
    async fn every_response_says_what_it_varies_on() {
        let admin = serving().await;
        let address = admin.address();
        for accept in [None, Some("gzip"), Some("br"), Some("*")] {
            let headers: Vec<(&str, &str)> = accept
                .map(|value| vec![("Accept-Encoding", value)])
                .unwrap_or_default();
            let response = get(address, "/", &headers).await;
            assert_eq!(
                response.header("vary"),
                Some("accept-encoding"),
                "without it a shared cache hands one client's bytes to another \
                 that cannot read them; Accept-Encoding was {accept:?}"
            );
        }
        admin.shutdown().await;
    }

    #[tokio::test]
    async fn two_encodings_of_one_document_are_two_representations() {
        let admin = serving().await;
        let address = admin.address();
        let identity = get(address, "/", &[]).await;
        let gzip = get(address, "/", &[("Accept-Encoding", "gzip")]).await;
        let brotli = get(address, "/", &[("Accept-Encoding", "br")]).await;

        let tags = [
            identity.header("etag").expect("identity is tagged"),
            gzip.header("etag").expect("gzip is tagged"),
            brotli.header("etag").expect("brotli is tagged"),
        ];
        assert_ne!(tags[0], tags[1], "identity and gzip share a tag");
        assert_ne!(tags[1], tags[2], "gzip and brotli share a tag");
        assert_ne!(tags[0], tags[2], "identity and brotli share a tag");

        // The consequence, rather than the property: a browser
        // holding the gzip bytes asks about them on a request it made
        // uncompressed, and must be sent the document rather than
        // told it already has it.
        let wrong = get(address, "/", &[("If-None-Match", tags[1])]).await;
        assert_eq!(
            wrong.status, 200,
            "a tag for the gzip form does not answer for the uncompressed one"
        );

        // And the right one still works, or the tags would be
        // achieving nothing but cache misses.
        let right = get(address, "/", &[("If-None-Match", tags[0])]).await;
        assert_eq!(right.status, 304, "the tag it was given back is its own");
        assert_eq!(right.header("etag"), Some(tags[0]));
        assert!(right.body.is_empty(), "a 304 carries no body");

        admin.shutdown().await;
    }

    // -----------------------------------------------------------
    // A build with no interface
    // -----------------------------------------------------------

    /// The placeholder says what it is for, whichever build this is.
    ///
    /// Asserted against the embedded document rather than against
    /// what is served, because in a tree where the interface *has*
    /// been built nothing serves it, and a test that only runs in the
    /// other tree is a test nobody runs.
    #[test]
    fn the_placeholder_says_the_interface_was_not_built_and_what_to_run() {
        let document = std::str::from_utf8(PLACEHOLDER.identity.bytes).expect("it is text");
        assert!(
            document.contains("interface was not built"),
            "it says what happened: {document}"
        );
        assert!(
            document.contains("npm run build"),
            "and what to run: {document}"
        );
        assert!(document.contains("services/web"), "and where: {document}");
        assert_eq!(PLACEHOLDER.content_type, "text/html; charset=utf-8");
    }

    /// What the root is, in whichever build this is.
    ///
    /// Both branches assert something: that the placeholder is served
    /// when there is nothing else, and that it is **not** served when
    /// there is. Without the second, a build script that quietly
    /// embedded the placeholder over a perfectly good interface would
    /// pass.
    #[tokio::test]
    async fn the_root_is_the_interface_when_there_is_one_and_says_so_when_there_is_not() {
        let admin = serving().await;
        let root = get(admin.address(), "/", &[]).await;
        assert_eq!(root.status, 200);
        let document = String::from_utf8_lossy(&root.body).into_owned();
        if INTERFACE_BUILT {
            assert_ne!(
                root.body, PLACEHOLDER.identity.bytes,
                "an interface was built, so the placeholder must not be what is served"
            );
            assert!(
                document.contains("<script") && document.contains("id=\"app\""),
                "it is the built application: {document}"
            );
        } else {
            assert_eq!(
                root.body, PLACEHOLDER.identity.bytes,
                "with no interface built, the root is the page that says so"
            );
        }
        admin.shutdown().await;
    }

    // -----------------------------------------------------------
    // The third-party notices
    // -----------------------------------------------------------

    /// The Rust half is in the binary, and names what obliges us.
    ///
    /// Named crates rather than a length or a substring like
    /// "License", because the failure this guards is a generator that
    /// produced *something*: an empty list, a list of the workspace's
    /// own crates, a list of build tooling. `sync_wrapper` and `ryu`
    /// are the two whose only branch on our allow list is Apache-2.0,
    /// which is the licence that asks for its text to travel with the
    /// work, so they are the two whose absence matters most.
    #[test]
    fn the_notices_name_the_apache_licensed_crates_that_ship() {
        assert!(
            NOTICES.contains("sync_wrapper 1."),
            "sync_wrapper is Apache-2.0 with no alternative and is linked in"
        );
        assert!(
            NOTICES.contains("ryu 1."),
            "ryu offers Apache-2.0 OR BSL-1.0, and BSL-1.0 is not on the allow list"
        );
        assert!(
            NOTICES.contains("Apache License"),
            "naming an Apache-2.0 crate without carrying the licence text is \
             the half of the obligation that is not met by a list"
        );
        assert!(
            NOTICES.contains("Version 2.0, January 2004"),
            "and it is the Apache 2.0 text rather than a line mentioning it"
        );
        // The ones a reader would notice missing, and a cross-section
        // of the licences: MIT, Zlib and the standard library.
        for named in ["tokio 1.", "axum 0.", "redb 4.", "foldhash 0.", "hyper 1."] {
            assert!(
                NOTICES.contains(named),
                "{named} is in the binary and unnamed"
            );
        }
        assert!(
            NOTICES.contains("the Rust standard library"),
            "std is linked from the sysroot and is in no manifest, which is \
             exactly why it is the piece a manifest-driven list loses"
        );
    }

    /// The browser half ships exactly when the browser half ships.
    ///
    /// **Both directions.** This is the bug that was found: the
    /// notices were generated correctly, one directory above the
    /// bundle, and the build script embeds only what is inside it, so
    /// the binary carried the libraries and none of their notices. A
    /// test that only checked for their presence would pass in a tree
    /// with no interface, which is every tree the suite normally runs
    /// in — so the other branch asserts they are absent, and says so.
    #[test]
    fn the_bundled_libraries_are_attributed_exactly_when_they_are_embedded() {
        if INTERFACE_BUILT {
            for library in ["preact", "uplot", "open-props", "valibot", "virtual-core"] {
                assert!(
                    NOTICES.contains(library),
                    "{library} is in the bundle this binary embeds and is unattributed"
                );
            }
        } else {
            assert!(
                NOTICES.contains("compiled without the browser interface"),
                "with no bundle embedded the notices say so rather than going quiet"
            );
            assert!(
                !NOTICES.contains("uPlot Copyright"),
                "and do not attribute libraries that are not in this binary"
            );
        }
    }

    /// A user of the binary can read them over the listener.
    ///
    /// Served whether or not an interface was built, because the
    /// crates are compiled in either way, and through the same
    /// negotiation as everything else rather than a second path.
    #[tokio::test]
    async fn the_notices_are_served_and_are_the_bytes_that_were_embedded() {
        let admin = serving().await;
        let address = admin.address();

        let plain = get(address, NOTICES_PATH, &[]).await;
        assert_eq!(plain.status, 200);
        assert_eq!(
            plain.header("content-type"),
            Some("text/plain; charset=utf-8")
        );
        assert_eq!(
            plain.body,
            NOTICES.as_bytes(),
            "what is served is what `sdmqtt licenses` prints, to the byte"
        );

        let brotli = get(address, NOTICES_PATH, &[("Accept-Encoding", "br")]).await;
        assert_eq!(brotli.status, 200);
        assert_eq!(brotli.header("content-encoding"), Some("br"));
        assert!(
            brotli.body.len() < plain.body.len(),
            "seventy-five kilobytes of licence text compresses or something is wrong"
        );
        assert_ne!(
            brotli.header("etag"),
            plain.header("etag"),
            "two encodings are two representations here as everywhere else"
        );

        admin.shutdown().await;
    }

    // -----------------------------------------------------------
    // What the fallback must not swallow
    // -----------------------------------------------------------

    #[tokio::test]
    async fn an_unknown_api_path_is_still_the_one_error_shape() {
        let admin = serving().await;
        let response = get(
            admin.address(),
            "/api/v1/nowhere",
            &[("Accept", "text/html")],
        )
        .await;
        assert_eq!(
            response.status, 404,
            "an API path is not answered with a page, even to a browser"
        );
        assert!(
            String::from_utf8_lossy(&response.body).contains("\"code\":\"not_found\""),
            "{:?}",
            String::from_utf8_lossy(&response.body)
        );
        admin.shutdown().await;
    }

    #[tokio::test]
    async fn a_page_the_interface_routes_is_answered_with_the_interface() {
        let admin = serving().await;
        let address = admin.address();
        let page = get(address, "/clients", &[("Accept", "text/html")]).await;
        assert_eq!(
            page.status, 200,
            "the interface routes in the browser, so a reload on one of its \
             paths must be answered with the entry document"
        );
        let root = get(address, "/", &[]).await;
        assert_eq!(page.body, root.body);

        // But a fetch for something that is not there is not a page.
        let missing = get(address, "/assets/gone.js", &[]).await;
        assert_eq!(
            missing.status, 404,
            "a missing script answered with a document is a syntax error in a \
             console instead of a 404 in a network tab"
        );
        admin.shutdown().await;
    }
}
