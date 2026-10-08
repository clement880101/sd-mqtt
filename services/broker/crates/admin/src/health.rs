//! Liveness, readiness, cluster membership, and the metrics
//! exposition.
//!
//! # Why there are two health endpoints and not one
//!
//! Section 11.1: health is tiered. Liveness answers whether the
//! process is running; readiness answers whether listeners are bound
//! and storage is open; a cluster endpoint answers whether this
//! node's view of membership is complete. An operator chooses which
//! one gates traffic.
//!
//! The difference is the whole point, and it is easy to build two
//! endpoints that are the same handler twice. **Liveness must answer
//! before anything is bound.** A liveness probe that fails while the
//! store is being read is a probe that kills a broker in the middle
//! of a restore — a restore that, on a large store, is the slowest
//! and most important thing the process ever does — and the
//! supervisor then restarts it into the same restore, forever.
//! Readiness is the one that says "not yet"; liveness says only "this
//! process has not wedged".
//!
//! So liveness reads nothing. It does not look at the readiness gates,
//! it does not take the broker lock, and it does not ask the store
//! anything. That it is answering at all is the answer.
//!
//! # The health endpoints are outside the error envelope
//!
//! Every other endpoint answers a failure with the one shape in
//! [`crate::error`]. These do not: an unready readiness probe is the
//! endpoint working correctly, not failing, and a probe reading it
//! wants the same document whichever way it went. The status code
//! carries ready against unready and the body says which gate is
//! open.

use std::sync::atomic::{AtomicBool, Ordering};

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::AdminState;

/// The content type the OpenMetrics exposition format requires.
///
/// [`sdmqtt_server::metrics::Metrics::render`] writes OpenMetrics text
/// — it ends with the `# EOF` that format mandates and the older
/// Prometheus text format forbids — so this is that format's type and
/// not `text/plain`. A scraper told `text/plain` for a body ending in
/// `# EOF` reads a trailing line it does not expect.
pub const METRICS_CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

/// What has to be true before this node is ready to be sent traffic.
///
/// Two gates, each set once by the startup path as it passes, and
/// read by the readiness endpoint. Atomics rather than a lock because
/// a probe must never queue behind anything, least of all behind the
/// process-wide broker lock it is trying to report on.
///
/// # Why this is not derived from the broker
///
/// It would be tempting to answer readiness by asking whether a
/// listener exists and whether the store handle is `Some`. Neither
/// question can be asked from here: `Shared` deliberately does not
/// hand out its writer, and a broker configured with no store at all
/// is a supported mode rather than a permanently unready one. What
/// readiness reports is what startup did, so startup is what says it.
#[derive(Debug, Default)]
pub struct Readiness {
    storage: AtomicBool,
    listeners: AtomicBool,
}

impl Readiness {
    /// Nothing has happened yet: not ready.
    pub fn starting() -> Self {
        Self::default()
    }

    /// The store is open and the broker has been restored from it.
    pub fn storage_is_open(&self) {
        self.storage.store(true, Ordering::Release);
    }

    /// There is no store to open.
    ///
    /// A distinct call from [`Readiness::storage_is_open`] although it
    /// sets the same gate, because the two are different facts and a
    /// reader of the startup path should be able to see which one
    /// happened. Section's worth of reasoning in `sdmqtt`'s
    /// configuration: running with no store is a real mode, so a node
    /// in it is ready rather than forever unready.
    pub fn storage_is_not_configured(&self) {
        self.storage.store(true, Ordering::Release);
    }

    /// The listeners are bound and accepting.
    pub fn listeners_are_bound(&self) {
        self.listeners.store(true, Ordering::Release);
    }

    /// Whether every gate is open.
    pub fn is_ready(&self) -> bool {
        self.storage.load(Ordering::Acquire) && self.listeners.load(Ordering::Acquire)
    }

    /// What the endpoint says.
    fn report(&self) -> ReadinessView {
        let storage = self.storage.load(Ordering::Acquire);
        let listeners = self.listeners.load(Ordering::Acquire);
        ReadinessView {
            status: if storage && listeners {
                "ready"
            } else {
                "unready"
            },
            storage,
            listeners,
        }
    }
}

/// The liveness document.
#[derive(Debug, Serialize)]
struct LivenessView {
    status: &'static str,
}

/// The readiness document, naming each gate so that an operator
/// looking at an unready node learns which one it is waiting on.
#[derive(Debug, Serialize)]
struct ReadinessView {
    status: &'static str,
    storage: bool,
    listeners: bool,
}

/// The cluster document.
#[derive(Debug, Serialize)]
struct ClusterView {
    status: &'static str,
    members: usize,
    expected: usize,
    /// Whether `expected` is a number an operator wrote down.
    ///
    /// **Without this the document is reassuring rather than
    /// informative.** `expected` is `cluster.expected_size` when it is
    /// set and the count of nodes this one has met when it is not, and
    /// those two carry entirely different weight: the first can say a
    /// node is missing, and the second says `complete` on a node that
    /// came up alone and never heard of the others. A reader cannot
    /// tell them apart from the number, because three nodes that met
    /// each other and a configured three look identical.
    expected_configured: bool,
}

/// Whether this process is running.
///
/// Always `200`. It reads no state at all, deliberately: see the
/// module documentation.
async fn live() -> Json<LivenessView> {
    Json(LivenessView { status: "alive" })
}

/// Whether this node is ready to be sent traffic.
async fn ready(State(state): State<AdminState>) -> (StatusCode, Json<ReadinessView>) {
    let view = state.readiness.report();
    // 503 rather than 200-with-a-field. A load balancer reads the
    // status code and nothing else, and a node that answers 200 while
    // its store is still being read is a node traffic is sent to.
    let status = if view.status == "ready" {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(view))
}

/// Whether this node's view of cluster membership is complete.
///
/// `members` counts the peers answering, and this node. `expected` is
/// **what an operator configured as `cluster.expected_size`**, and the
/// status says `complete` while `members` has reached it.
///
/// Configured, because nothing else can say: membership is learnt, so
/// a node that never heard of a fourth node cannot tell a cluster of
/// three from a cluster of four with one down. With the key unset the
/// count of nodes met is served instead, which is what this endpoint
/// has always served and carries exactly that blind spot — it reports
/// `complete` on a node that came up alone. Section 8.5, and
/// `cluster::policy`.
///
/// More nodes answering than expected is `complete` rather than a
/// disagreement: a fourth node joining a cluster of three is not an
/// outage, and an operator who has not updated the key yet should not
/// be paged for it. It is the same comparison `cluster::Policy::whole`
/// makes, which is the point — the same reading decides this answer
/// and whether a client is refused.
///
/// **A node that does not cluster answers a cluster of one**, and that
/// is the truth rather than a placeholder: it is the only node, and
/// its view of that is complete. An operator who pointed a probe at
/// this before clustering existed does not have to change anything
/// now that it does.
///
/// A peer that is suspect counts as missing. That is the point of
/// having the endpoint separate from readiness: an operator chooses
/// which one gates traffic, and the stricter reading is the useful one
/// for the choice that says "do not send me work until I can see
/// everybody".
async fn cluster(State(state): State<AdminState>) -> Json<ClusterView> {
    let Some(cluster) = &state.cluster else {
        return Json(ClusterView {
            status: "complete",
            members: 1,
            expected: 1,
            // A node that does not cluster is a cluster of one, and
            // that is not a number anybody configured.
            expected_configured: false,
        });
    };
    let peers = cluster.peers();
    let members = peers.iter().filter(|peer| peer.state == "up").count() + 1;
    let configured = cluster.expected();
    let expected = configured.unwrap_or(peers.len() + 1);
    Json(ClusterView {
        status: if members >= expected {
            "complete"
        } else {
            "incomplete"
        },
        members,
        expected,
        expected_configured: configured.is_some(),
    })
}

/// The registry, in the exposition format.
async fn metrics(State(state): State<AdminState>) -> Response {
    let rendered = state.shared.metrics().render();
    ([(header::CONTENT_TYPE, METRICS_CONTENT_TYPE)], rendered).into_response()
}

/// The routes this module owns.
pub(crate) fn routes() -> Router<AdminState> {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/health/cluster", get(cluster))
        .route("/metrics", get(metrics))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing;
    use crate::{AdminConfig, AdminServer, AdminState};
    use broker_core::broker::BrokerConfig;
    use sdmqtt_server::shared::Shared;
    use std::net::SocketAddr;
    use std::sync::Arc;

    /// An admin listener on a loopback port the operating system
    /// chooses, over `readiness`.
    async fn serve(readiness: Arc<Readiness>) -> crate::AdminRunning {
        let config = AdminConfig {
            address: "127.0.0.1:0"
                .parse::<SocketAddr>()
                .expect("a valid address"),
            ..AdminConfig::default()
        };
        let state = AdminState::new(Arc::new(Shared::new(BrokerConfig::default())))
            .with_readiness(readiness);
        AdminServer::bind(&config, state)
            .await
            .expect("loopback needs no token")
    }

    /// Liveness answers while nothing is bound and no store is open.
    ///
    /// This is the case that separates the two endpoints. Asserting
    /// that both answer `200` once everything is running would pass
    /// against one handler registered twice.
    #[tokio::test]
    async fn liveness_answers_before_anything_is_ready() {
        let readiness = Arc::new(Readiness::starting());
        let running = serve(Arc::clone(&readiness)).await;

        let response = testing::get(running.address(), "/health/live").await;
        assert_eq!(response.status, 200, "{}", response.body);
        assert!(response.body.contains("alive"), "{}", response.body);

        assert!(
            !readiness.is_ready(),
            "and it answered without anything being ready"
        );
        running.shutdown().await;
    }

    /// Readiness does not.
    #[tokio::test]
    async fn readiness_is_unready_until_storage_is_open() {
        let readiness = Arc::new(Readiness::starting());
        let running = serve(Arc::clone(&readiness)).await;

        // Listeners bound, store not yet open. The one gate that is
        // shut is enough.
        readiness.listeners_are_bound();
        let response = testing::get(running.address(), "/health/ready").await;
        assert_eq!(
            response.status, 503,
            "a node whose store is not open is not ready: {}",
            response.body
        );
        assert!(
            response.body.contains("\"storage\":false"),
            "and it says which gate: {}",
            response.body
        );

        readiness.storage_is_open();
        let response = testing::get(running.address(), "/health/ready").await;
        assert_eq!(response.status, 200, "{}", response.body);
        assert!(
            response.body.contains("\"status\":\"ready\""),
            "{}",
            response.body
        );

        running.shutdown().await;
    }

    /// And the other gate on its own, so that neither can be the only
    /// one being read.
    #[tokio::test]
    async fn readiness_is_unready_until_the_listeners_are_bound() {
        let readiness = Arc::new(Readiness::starting());
        let running = serve(Arc::clone(&readiness)).await;

        readiness.storage_is_open();
        let response = testing::get(running.address(), "/health/ready").await;
        assert_eq!(
            response.status, 503,
            "storage alone is not readiness: {}",
            response.body
        );
        assert!(
            response.body.contains("\"listeners\":false"),
            "{}",
            response.body
        );

        readiness.listeners_are_bound();
        assert_eq!(
            testing::get(running.address(), "/health/ready")
                .await
                .status,
            200
        );
        running.shutdown().await;
    }

    /// A node that clusters reports what it can see, and says
    /// incomplete when it cannot see all of it.
    ///
    /// The endpoint answered a cluster of one whatever was true, which
    /// was correct while membership did not exist and is a placeholder
    /// now that it does.
    #[tokio::test]
    async fn the_cluster_endpoint_reports_what_this_node_can_see() {
        #[derive(Debug)]
        struct Peers(Vec<crate::ClusterPeer>, Option<usize>);
        impl crate::Cluster for Peers {
            fn peers(&self) -> Vec<crate::ClusterPeer> {
                self.0.clone()
            }
            fn expected(&self) -> Option<usize> {
                self.1
            }
        }
        fn peer(state: &'static str) -> crate::ClusterPeer {
            crate::ClusterPeer {
                id: format!("peer-{state}"),
                name: "edge".to_owned(),
                address: "10.0.0.2:7700".to_owned(),
                state,
            }
        }
        async fn serve_with(peers: Vec<crate::ClusterPeer>) -> crate::AdminRunning {
            configured(peers, None).await
        }
        async fn configured(
            peers: Vec<crate::ClusterPeer>,
            expected: Option<usize>,
        ) -> crate::AdminRunning {
            let config = AdminConfig {
                address: "127.0.0.1:0"
                    .parse::<SocketAddr>()
                    .expect("a valid address"),
                ..AdminConfig::default()
            };
            let state = AdminState::new(Arc::new(Shared::new(BrokerConfig::default())))
                .with_cluster(Some(
                    Arc::new(Peers(peers, expected)) as Arc<dyn crate::Cluster>
                ));
            AdminServer::bind(&config, state)
                .await
                .expect("loopback needs no token")
        }

        let whole = serve_with(vec![peer("up"), peer("up")]).await;
        let response = testing::get(whole.address(), "/health/cluster").await;
        assert!(response.body.contains("\"members\":3"), "{}", response.body);
        assert!(
            response.body.contains("\"expected_configured\":false"),
            "nothing was configured here, so the three is nodes met and says so: {}",
            response.body
        );
        assert!(
            response.body.contains("\"expected\":3"),
            "{}",
            response.body
        );
        assert!(
            response.body.contains("\"status\":\"complete\""),
            "{}",
            response.body
        );
        whole.shutdown().await;

        let broken = serve_with(vec![peer("up"), peer("down")]).await;
        let response = testing::get(broken.address(), "/health/cluster").await;
        assert!(response.body.contains("\"members\":2"), "{}", response.body);
        assert!(
            response.body.contains("\"expected\":3"),
            "{}",
            response.body
        );
        assert!(
            response.body.contains("\"status\":\"incomplete\""),
            "a node that cannot see a peer does not have a complete view: {}",
            response.body
        );
        broken.shutdown().await;

        // And a peer that is merely suspect counts as missing, which
        // is the stricter reading this endpoint exists to offer.
        let unsure = serve_with(vec![peer("suspect")]).await;
        let response = testing::get(unsure.address(), "/health/cluster").await;
        assert!(
            response.body.contains("\"status\":\"incomplete\""),
            "{}",
            response.body
        );
        unsure.shutdown().await;

        // **The case the count of nodes met cannot see.** Three nodes
        // answering, all three of them each other's whole world, and a
        // fourth that this one has never heard of because the
        // partition started before it did. `peers.len() + 1` is three
        // and would call that complete; the configured four is what
        // makes it the outage it is. Section 8.5.
        let unaware = configured(vec![peer("up"), peer("up")], Some(4)).await;
        let response = testing::get(unaware.address(), "/health/cluster").await;
        assert!(
            response.body.contains("\"expected_configured\":true"),
            "a reader has to be able to tell a configured four from four nodes met: {}",
            response.body
        );
        assert!(response.body.contains("\"members\":3"), "{}", response.body);
        assert!(
            response.body.contains("\"expected\":4"),
            "the configured size is what is served, not the nodes met: {}",
            response.body
        );
        assert!(
            response.body.contains("\"status\":\"incomplete\""),
            "three of four is incomplete, and nothing this node has met could \
             have told it so: {}",
            response.body
        );
        unaware.shutdown().await;

        // And the other side of the comparison: a node joined that
        // the key has not caught up with yet. Not an outage, and not
        // a disagreement to page anybody about.
        let grown = configured(vec![peer("up"), peer("up")], Some(2)).await;
        let response = testing::get(grown.address(), "/health/cluster").await;
        assert!(response.body.contains("\"members\":3"), "{}", response.body);
        assert!(
            response.body.contains("\"status\":\"complete\""),
            "more nodes than expected is not short of anything: {}",
            response.body
        );
        grown.shutdown().await;
    }

    /// A broker configured with no store is ready, not forever
    /// unready.
    #[tokio::test]
    async fn a_broker_with_no_store_becomes_ready() {
        let readiness = Arc::new(Readiness::starting());
        let running = serve(Arc::clone(&readiness)).await;

        readiness.storage_is_not_configured();
        readiness.listeners_are_bound();
        assert_eq!(
            testing::get(running.address(), "/health/ready")
                .await
                .status,
            200
        );
        running.shutdown().await;
    }

    /// Membership is milestone 5, and the path is served anyway.
    #[tokio::test]
    async fn the_cluster_endpoint_answers_a_cluster_of_one() {
        let running = serve(Arc::new(Readiness::starting())).await;
        let response = testing::get(running.address(), "/health/cluster").await;
        assert_eq!(response.status, 200);
        assert!(response.body.contains("\"members\":1"), "{}", response.body);
        assert!(
            response.body.contains("\"status\":\"complete\""),
            "a node on its own has a complete view of its membership: {}",
            response.body
        );
        running.shutdown().await;
    }

    #[tokio::test]
    async fn metrics_renders_the_registry_in_the_exposition_format() {
        let running = serve(Arc::new(Readiness::starting())).await;
        let response = testing::get(running.address(), "/metrics").await;

        assert_eq!(response.status, 200);
        assert_eq!(
            response.header("content-type"),
            Some(METRICS_CONTENT_TYPE),
            "the format's own type, not text/plain"
        );
        assert!(
            response.body.contains("sdmqtt_packets_received_total"),
            "the registry the previous plan built: {}",
            &response.body[..200.min(response.body.len())]
        );
        assert!(
            response.body.trim_end().ends_with("# EOF"),
            "OpenMetrics ends with its terminator, and the content type promises it"
        );
        running.shutdown().await;
    }
}
