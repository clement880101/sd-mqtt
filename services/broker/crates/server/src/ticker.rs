//! The one-second tick that drives every deadline.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::shared::Shared;

/// Start the tick that drives every deadline in the broker.
///
/// One task for the whole process. Keep-alive, wills, session expiry and
/// retained expiry all happen because of this and nothing else, so if it
/// stops the broker goes quietly deaf rather than failing loudly.
///
/// The interval is missed rather than burst when the process is
/// descheduled: a tick that arrives late should do one second of work,
/// not catch up on several at once, because the work each one does is
/// idempotent in wall time and catching up would only deepen the stall.
pub fn spawn_ticker(shared: Arc<Shared>, mut shutdown: broadcast::Receiver<()>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The first tick completes immediately; skip it so the clock does
        // not jump a second the moment the process starts.
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Expiries, wills and reclaimed retained messages
                    // are durable changes, and `with_broker` commits
                    // them. **Nothing here is waited on.** Section
                    // 7.3: the wait is for an acknowledgement, and a
                    // tick was promised to nobody. Waiting would put
                    // the one task that drives every deadline in the
                    // process behind a disk flush once a second.
                    let actions = shared.with_broker(|broker| {
                        let mut out = Vec::new();
                        broker.tick(&mut out);
                        out
                    });
                    shared.deliver(actions);
                    // Once a second, from the one task that runs
                    // whether or not anybody is connected. See
                    // `Shared::report_rejections` and
                    // `Shared::report_pacing`: one says the store and
                    // the broker disagree, the other says the device
                    // cannot keep up, and an operator needs both.
                    shared.report_rejections();
                    shared.report_pacing();
                }
                _ = shutdown.recv() => return,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_core::broker::BrokerConfig;
    use broker_core::session::Action;
    use mqtt_codec::packet::Connect;
    use mqtt_codec::{Properties, ReasonCode, Version};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn the_clock_advances_once_a_second_and_not_at_the_start() {
        // Paused time, so this asserts the schedule rather than waiting
        // for it.
        //
        // The exact second, not a range. `tokio::time::interval`
        // completes its first tick immediately, and this loop skips
        // that one so the clock does not jump the moment the process
        // starts; a range wide enough to hold both answers cannot see
        // that skip deleted, which is convention 5's test measuring
        // the wrong dimension.
        //
        // Half a second past the fourth, so that nothing here lands on
        // the instant a tick is also due and the answer does not
        // depend on which of the two the runtime polls first.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        let handle = spawn_ticker(Arc::clone(&shared), shutdown.subscribe());

        tokio::time::sleep(Duration::from_millis(4_500)).await;
        let now = shared.with_broker(|broker| broker.clock().now());
        assert_eq!(
            now, 4,
            "four ticks in four and a half seconds: one a second, and none at zero"
        );

        drop(shutdown);
        let _ = handle.await;
    }

    #[tokio::test(start_paused = true)]
    async fn shutting_down_stops_the_ticker() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        let handle = spawn_ticker(Arc::clone(&shared), shutdown.subscribe());

        tokio::time::sleep(Duration::from_secs(2)).await;
        shutdown.send(()).unwrap();

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("the ticker stops promptly")
            .expect("it does not panic");
    }

    /// A client that proposes a one second keep-alive, which section
    /// 3.1.2.10 expires at one and a half of those: the earliest thing
    /// a tick can be made to produce.
    fn impatient() -> Connect {
        Connect {
            version: Version::V5,
            client_id: "silent".into(),
            clean_start: true,
            keep_alive: 1,
            username: None,
            password: None,
            will: None,
            properties: Properties::default(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn what_a_tick_produces_reaches_the_session_it_is_addressed_to() {
        // What this replaces asserted that a tick producing nothing
        // delivered nothing, against a broker with no sessions at all:
        // nothing could have made that count non-zero, so it told a
        // working delivery from a missing one not at all. Convention 5,
        // a test asserting an absence.
        //
        // The delivery is the whole point of the loop. Keep-alive,
        // wills, session expiry and retained expiry produce nothing a
        // client can see unless what the tick returns is handed to the
        // outboxes, and dropping it instead was caught by one test in
        // the tree, the only one in the suite that waits on wall time.
        // This drives the clock directly instead, so it proves the
        // same thing in milliseconds and cannot go flaky on a loaded
        // machine.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let subscriber = shared
            .with_broker(|broker| {
                let mut out = Vec::new();
                broker
                    .connect("", &impatient(), &mut out)
                    .expect("the default decision authenticates")
            })
            .subscriber;
        let mut inbox = shared.register_outbox(subscriber, 8);

        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        let handle = spawn_ticker(Arc::clone(&shared), shutdown.subscribe());

        // Generous, and paid in virtual seconds: the timeout is what
        // fails the test when nothing is delivered, and the clock runs
        // as fast as the runtime can advance it.
        let delivered = tokio::time::timeout(Duration::from_secs(30), inbox.recv())
            .await
            .expect("what the tick produced reaches the outbox it is addressed to")
            .expect("the outbox is still registered");
        assert!(
            matches!(delivered, Action::Close(ReasonCode::KeepAliveTimeout)),
            "expected the keep-alive close, got {delivered:?}"
        );

        drop(shutdown);
        let _ = handle.await;
    }
}
