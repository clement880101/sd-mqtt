//! Which names this listener answers to.
//!
//! # What this is for, and what it is not for
//!
//! Section 11.1 protects the admin listener by where it listens: it
//! binds loopback by default, and a non-loopback address with no token
//! is refused at startup. That is a good answer to a program on
//! another host. It is not an answer to **a browser on this one**.
//!
//! An ordinary cross-origin request from a web page is already
//! stopped, and not by anything written here: every mutating endpoint
//! takes a JSON body, `Content-Type: application/json` is not a
//! simple request header, so the browser sends a preflight, and
//! nothing on this listener answers a preflight. The page never gets
//! to make the real request.
//!
//! **DNS rebinding goes around that**, and this milestone is the first
//! where it matters, because it is the first that makes the admin
//! listener something a browser is pointed at. The attack: a page is
//! served from `attacker.example`, whose name the attacker's own
//! resolver then re-answers as `127.0.0.1`. The page fetches
//! `http://attacker.example:1884/api/v1/publish`. To the browser that
//! is **same-origin** — same scheme, same name, same port — so there
//! is no preflight and no origin check to fail. The connection lands
//! on this listener. Against the default configuration, which is
//! loopback with no token, the page can then publish and read every
//! retained payload on the broker.
//!
//! The standard mitigation is to check the name the request was made
//! under, because the one thing the attack cannot change is that the
//! browser puts that name in `Host`.
//!
//! # The rule
//!
//! A request is answered when its `Host` is:
//!
//! - **an IP address literal.** For a browser to send one, the page's
//!   own origin has to be that address, which means this listener
//!   served it. Rebinding needs a *name*: a page at
//!   `attacker.example` fetching `http://127.0.0.1:1884/` is
//!   cross-origin again and back behind the preflight. So an IP
//!   literal is not a hole, and refusing it would break every way an
//!   operator, a probe or a scraper actually reaches this listener.
//! - **`localhost`.** RFC 6761 reserves it and browsers resolve it to
//!   loopback and nowhere else, so it cannot be rebound.
//! - **a name in `admin.allowed_hosts`,** for an operator who reaches
//!   the listener through one.
//!
//! The port is not checked. In development the interface is served by
//! Vite, which forwards this listener's requests with the original
//! `Host` and therefore the development server's port; requiring a
//! match would break that while protecting nothing, since the name is
//! the whole of what rebinding controls.
//!
//! An absent `Host` is answered. HTTP/1.1 requires the field and hyper
//! rejects a request without it before this runs; what is left is
//! HTTP/1.0, which no browser speaks. A check aimed at browsers has
//! nothing to say about a request that cannot have come from one, and
//! an attacker who can choose the field can choose an allowed value
//! anyway — this defends the browser, not the socket.
//!
//! # The ingest listener is deliberately not behind this
//!
//! A decision, written here rather than left as an omission somebody
//! later reads as an oversight.
//!
//! Section 9.2's `http_in` puts `POST /ingest/<rule-id>` on a listener
//! of its own by default. That listener is **not** behind this layer,
//! and it is not behind it for a reason rather than because nobody
//! applied it: the callers are webhook senders, each reaching the
//! broker under whatever name its own configuration was written with.
//! Applying this check there would mean an operator either adding every
//! sender's hostname to a list, or — far likelier — seeing every
//! delivery refused with a message about DNS rebinding, which reads as
//! a bug rather than as a policy.
//!
//! What makes that safe is that the reasoning above does not transfer.
//! This check exists because the admin listener's protection is *where
//! it listens*: against the default configuration — loopback, no token
//! — a rebound page can publish and read every retained payload. An
//! ingest listener has no such default. **Every route on it is guarded
//! by its own rule's bearer token, and section 9.2 refuses a rule
//! without one**, so a rebound page reaching it has exactly the access
//! anybody on the network has: none, unless it already holds a token,
//! and a page that holds the token did not need to be rebound.
//!
//! The rest of the attack does not transfer either. An ingest route
//! publishes and reads nothing back, so there is no response for a
//! rebound page to steal; the reply is a status and a sentence. What is
//! left is a page that can make a request it could have made from
//! anywhere, which is what a token is for.
//!
//! Running the ingest routes on the admin listener is permitted and
//! must be chosen explicitly. A route set served there **is** behind
//! this layer, because the layer belongs to that router; an operator
//! who makes that choice takes the admin listener's rules with it, and
//! that is the right way round.
//!
//! The layer is therefore applied by [`crate::router`], which builds
//! the admin route set, and not by [`crate::Bound::serve`], which
//! serves whatever it is given.
//! The test `a_route_set_is_served_where_it_is_asked_for_and_nowhere_else`
//! in that module is this paragraph written as something that fails.

use std::net::IpAddr;

use axum::extract::{Request, State};
use axum::http::header::HOST;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::error::ApiError;
use crate::AdminState;

/// The name this listener always answers to besides an address.
///
/// Reserved by RFC 6761 and resolved to loopback by browsers and
/// resolvers alike, so it is not a name an attacker can point
/// anywhere.
pub const ALWAYS_ALLOWED: &str = "localhost";

/// How much of a refused `Host` is quoted back.
///
/// Enough to recognise a hostname, and bounded because the value is
/// the client's and the sentence goes into a log.
const QUOTED: usize = 80;

/// The name part of a `Host` field, without its port.
///
/// `None` when the field is not a `Host` at all, which is refused
/// rather than guessed at.
fn name_of(host: &str) -> Option<&str> {
    let host = host.trim();
    if host.is_empty() {
        return None;
    }
    // A whole field that is itself an address. An unbracketed IPv6
    // literal is not a legal `Host` and no browser writes one, but
    // `::1` would otherwise be split at its last colon into the
    // nonsense `::` and a port, and the answer to "is this a name
    // somebody could point at me" is no either way.
    if host.parse::<IpAddr>().is_ok() {
        return Some(host);
    }
    // An IPv6 literal is bracketed, and its colons are not port
    // separators.
    let name = if let Some(rest) = host.strip_prefix('[') {
        let (inside, after) = rest.split_once(']')?;
        if !after.is_empty() && !after.starts_with(':') {
            return None;
        }
        inside
    } else {
        match host.rsplit_once(':') {
            Some((name, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => {
                name
            }
            Some(_) => return None,
            None => host,
        }
    };
    if name.is_empty() {
        return None;
    }
    // A fully qualified name may carry the root's trailing dot, and
    // `example.test.` and `example.test` are the same name.
    Some(name.strip_suffix('.').unwrap_or(name))
}

/// Whether this listener answers to the name in `host`.
///
/// See the module documentation for why an address literal is
/// always answered and a name is not.
pub fn is_allowed(host: &str, configured: &[String]) -> bool {
    let Some(name) = name_of(host) else {
        return false;
    };
    if name.parse::<IpAddr>().is_ok() {
        return true;
    }
    if name.eq_ignore_ascii_case(ALWAYS_ALLOWED) {
        return true;
    }
    configured
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(name))
}

/// Refuse a request made under a name this listener does not answer to.
fn refusal(host: &str) -> Response {
    let quoted: String = host.chars().take(QUOTED).collect();
    ApiError::invalid_request(format!(
        "this listener does not answer to the host \"{quoted}\". A request \
         arriving under a name rather than an address is checked, because a \
         name is what DNS rebinding controls and this API publishes. Reach it \
         by address or as localhost, or add the name to admin.allowed_hosts."
    ))
    .into_response()
}

/// The check, as a layer over every route on this listener.
///
/// Over every one of them, including the health endpoints and the
/// interface: a rebound page reading `/health/ready` learns less than
/// one reading the retained store, but the rule is easier to reason
/// about with no exceptions in it than with one that has to be
/// re-justified each time a route is added.
pub(crate) async fn guard(
    State(state): State<AdminState>,
    request: Request,
    next: Next,
) -> Response {
    match request.headers().get(HOST).map(|value| value.to_str()) {
        // See the module documentation: a request with no `Host` did
        // not come from a browser, and this check is about browsers.
        None => next.run(request).await,
        Some(Ok(host)) if is_allowed(host, &state.allowed_hosts) => next.run(request).await,
        Some(Ok(host)) => refusal(host),
        // Not text, so not a host either.
        Some(Err(_)) => refusal(""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing;
    use crate::{AdminConfig, AdminRunning, AdminServer};
    use broker_core::broker::BrokerConfig;
    use sdmqtt_server::shared::Shared;
    use std::net::SocketAddr;
    use std::sync::Arc;

    /// A listener answering to nothing but an address and localhost.
    async fn serving(allowed: &[&str]) -> AdminRunning {
        let config = AdminConfig {
            address: "127.0.0.1:0"
                .parse::<SocketAddr>()
                .expect("a valid address"),
            allowed_hosts: allowed.iter().map(|name| (*name).to_owned()).collect(),
            ..AdminConfig::default()
        };
        let state = AdminState::new(Arc::new(Shared::new(BrokerConfig::default())));
        AdminServer::bind(&config, state)
            .await
            .expect("loopback needs no token")
    }

    // -----------------------------------------------------------
    // The rule, on its own
    // -----------------------------------------------------------

    /// The attack this exists for, in one assertion.
    ///
    /// A name the attacker owns, re-answered as loopback. The browser
    /// calls it same-origin and sends no preflight; `Host` is the one
    /// thing it still carries that the attacker cannot forge away.
    #[test]
    fn a_rebound_name_is_refused() {
        assert!(!is_allowed("attacker.example", &[]));
        assert!(!is_allowed("attacker.example:1884", &[]));
        assert!(
            !is_allowed("localhost.attacker.example", &[]),
            "a name that merely begins with the allowed one is a different name"
        );
        assert!(
            !is_allowed("notlocalhost", &[]),
            "and so is one that merely ends with it; a name is matched whole \
             or the attacker picks the rest of it"
        );
        assert!(!is_allowed("notlocalhost:1884", &[]));
        assert!(
            !is_allowed("attacker.example:1884.", &[]),
            "and a trailing dot does not make it one either"
        );
    }

    /// Every way anyone actually reaches this listener still works.
    #[test]
    fn an_address_or_localhost_is_answered() {
        for host in [
            "127.0.0.1",
            "127.0.0.1:1884",
            "[::1]",
            "[::1]:1884",
            "::1",
            "192.0.2.10:1884",
            "localhost",
            "localhost:1884",
            "LocalHost:1884",
            // A fully qualified `localhost` is the same name.
            "localhost.:1884",
        ] {
            assert!(is_allowed(host, &[]), "{host} is refused and should not be");
        }
    }

    /// Why an address literal is safe, said as an assertion.
    ///
    /// Rebinding needs a name. For a browser to put an address in
    /// `Host`, the page's own origin has to be that address, which
    /// means this listener served the page. So the literal is not the
    /// hole, and this is the case that would break every probe and
    /// scraper if it were treated as one.
    #[test]
    fn an_address_is_not_a_name_and_cannot_be_rebound() {
        assert!(is_allowed("203.0.113.9:1884", &[]));
        assert!(
            !is_allowed("203-0-113-9.attacker.example:1884", &[]),
            "a name that looks like an address is still a name"
        );
    }

    /// An operator's own name, and only theirs.
    #[test]
    fn a_configured_name_is_answered_and_others_are_not() {
        let allowed = ["broker.internal".to_owned()];
        assert!(is_allowed("broker.internal", &allowed));
        assert!(is_allowed("BROKER.INTERNAL:1884", &allowed));
        assert!(is_allowed("broker.internal.", &allowed));
        assert!(!is_allowed("broker.internal.attacker.example", &allowed));
        assert!(
            !is_allowed("notbroker.internal", &allowed),
            "matched whole, not as a suffix"
        );
        assert!(!is_allowed("other.internal", &allowed));
    }

    /// A field that is not a host is refused rather than guessed at.
    #[test]
    fn something_that_is_not_a_host_is_refused() {
        assert!(!is_allowed("", &[]));
        assert!(!is_allowed("   ", &[]));
        assert!(!is_allowed(":1884", &[]));
        assert!(!is_allowed("localhost:notaport", &[]));
        assert!(!is_allowed("[::1", &[]));
        assert!(!is_allowed("[::1]x", &[]));
    }

    // -----------------------------------------------------------
    // Over a socket
    // -----------------------------------------------------------

    /// The publish endpoint, reached under a rebound name.
    ///
    /// **The endpoint, not a health probe.** This is the request the
    /// attack is worth making: against the default configuration —
    /// loopback, no token — it writes a retained message into the
    /// broker. A test that only proved a `GET /` was refused would
    /// pass with the layer applied to the interface and not to the
    /// API.
    #[tokio::test]
    async fn a_page_on_a_rebound_name_cannot_publish() {
        let admin = serving(&[]).await;
        let refused = testing::post_json_with_host(
            admin.address(),
            "/api/v1/publish",
            "attacker.example:1884",
            r#"{"topic":"rebound","payload":"x","retain":true}"#,
        )
        .await;
        assert_eq!(refused.status, 400, "{}", refused.body);
        assert!(
            refused.body.contains("\"code\":\"invalid_request\""),
            "in the one shape every failure of this API takes: {}",
            refused.body
        );
        assert!(
            refused.body.contains("admin.allowed_hosts"),
            "and it says how an operator who meant it makes it work: {}",
            refused.body
        );

        // And nothing was written. The refusal has to be before the
        // handler, not a different answer from it.
        let retained = testing::get(admin.address(), "/api/v1/retained").await;
        assert!(
            !retained.body.contains("rebound"),
            "the publication was refused, not performed and then complained about: {}",
            retained.body
        );

        admin.shutdown().await;
    }

    /// And the same request under a name the operator allowed.
    #[tokio::test]
    async fn a_configured_name_reaches_the_api() {
        let admin = serving(&["broker.internal"]).await;
        let accepted = testing::post_json_with_host(
            admin.address(),
            "/api/v1/publish",
            "broker.internal",
            r#"{"topic":"allowed","payload":"x","retain":true}"#,
        )
        .await;
        assert_eq!(accepted.status, 200, "{}", accepted.body);
        admin.shutdown().await;
    }

    /// The interface and the probes are behind it too.
    #[tokio::test]
    async fn every_route_is_behind_the_check() {
        let admin = serving(&[]).await;
        for path in ["/", "/health/ready", "/metrics", "/api/v1/retained"] {
            let refused =
                testing::get_with_host(admin.address(), path, "attacker.example:1884").await;
            assert_eq!(
                refused.status, 400,
                "{path} answered a rebound name: {}",
                refused.body
            );
        }
        admin.shutdown().await;
    }
}
