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
                    let actions = shared.with_broker(|broker| {
                        let mut out = Vec::new();
                        broker.tick(&mut out);
                        out
                    });
                    shared.deliver(actions);
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
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn the_clock_advances_once_a_second() {
        // Paused time, so this asserts the schedule rather than waiting
        // for it.
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        let handle = spawn_ticker(Arc::clone(&shared), shutdown.subscribe());

        tokio::time::sleep(Duration::from_secs(5)).await;
        let now = shared.with_broker(|broker| broker.clock().now());
        assert!(
            (4..=6).contains(&now),
            "advanced to {now} after five seconds"
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

    #[tokio::test(start_paused = true)]
    async fn a_tick_that_produces_nothing_delivers_nothing() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        let handle = spawn_ticker(Arc::clone(&shared), shutdown.subscribe());

        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(shared.dropped_for_slow_reader(), 0);

        drop(shutdown);
        let _ = handle.await;
    }
}
