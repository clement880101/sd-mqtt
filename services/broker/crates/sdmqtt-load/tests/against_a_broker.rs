//! The generator against a real broker, in process.
//!
//! What the tool is for is a number nobody can check by eye, so the two
//! things it must get right are checked here at a size a test can
//! afford: every connection it reports as held really is held and
//! answering, and connections the broker turns away are reported as
//! such, by cause, rather than vanishing from the count.

use std::net::IpAddr;
use std::time::Duration;

use sdmqtt_load::{run, Plan};
use sdmqtt_server::listener::{Server, ServerConfig};

fn plan(target: std::net::SocketAddr, connections: usize) -> Plan {
    Plan {
        target,
        connections,
        sources: vec![IpAddr::from([127, 0, 0, 1])],
        hold: Duration::from_millis(500),
        keep_alive: 30,
        concurrency: 64,
        connect_timeout: Duration::from_secs(5),
    }
}

/// Two hundred connect, are held, and answer a ping at the end.
#[tokio::test(flavor = "multi_thread")]
async fn every_connection_reported_held_is_held() {
    let broker = Server::bind(ServerConfig::ephemeral())
        .await
        .expect("a broker binds");
    let report = run(&plan(broker.address(), 200)).await;
    assert_eq!(report.connected, 200, "{report}");
    assert_eq!(report.still_connected, 200, "{report}");
    assert!(report.failed.is_empty(), "{report}");
    broker.shutdown().await;
}

/// A broker that serves fifty at once turns the rest away, and the
/// report says so by cause rather than reporting fifty and nothing else.
#[tokio::test(flavor = "multi_thread")]
async fn connections_turned_away_are_reported_by_cause() {
    let broker = Server::bind(ServerConfig {
        max_connections: 50,
        ..ServerConfig::ephemeral()
    })
    .await
    .expect("a broker binds");
    let report = run(&plan(broker.address(), 80)).await;
    assert_eq!(report.connected, 50, "{report}");
    let turned_away: usize = report.failed.values().sum();
    assert_eq!(turned_away, 30, "{report}");
    assert!(
        report
            .failed
            .keys()
            .all(|cause| cause.contains("closed before CONNACK") || cause.contains("reset")),
        "a refused socket is a close or a reset: {report}"
    );
    assert!(
        report.to_string().contains("closed before CONNACK")
            || report.to_string().contains("reset")
    );
    broker.shutdown().await;
}

/// A connection the broker drops during the hold is not counted as
/// held: the last ping is what says so, not the connect.
#[tokio::test(flavor = "multi_thread")]
async fn a_connection_dropped_during_the_hold_is_not_counted_as_held() {
    let broker = Server::bind(ServerConfig::ephemeral())
        .await
        .expect("a broker binds");
    let mut plan = plan(broker.address(), 20);
    plan.hold = Duration::from_secs(2);
    let stopping = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(700)).await;
        broker.shutdown().await;
    });
    let report = run(&plan).await;
    stopping.await.expect("the broker stops");
    assert_eq!(report.connected, 20, "{report}");
    assert_eq!(report.still_connected, 0, "{report}");
}
