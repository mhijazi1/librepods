//! Default sink routing on handoff (#40): a take makes the AirPods the
//! default sink, so a stream on another sink (a USB DAC) follows them, and
//! losing the take gives the default back to the sink it replaced.
//!
//! `plan_default_sink` decides; `DefaultSinkRouter` runs the events in order
//! on one worker task, so the handoff shell only enqueues and never waits on
//! the sound server.

use crate::media_controller::MediaController;
use crate::pulse;
use futures::future::BoxFuture;
use log::{info, warn};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, watch};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SinkEvent {
    /// Linux took the AirPods.
    Take,
    /// Linux lost them.
    Yield,
    /// The handoff's A2DP hold on or off (#264); queued with the routes so
    /// `settled()` covers it and a later `Take` finds the AirPods sink.
    Hold(bool),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SinkAction {
    SetDefault(String),
}

/// What `event` does, given the sink `remembered` from an earlier take, the
/// AirPods' sink and the current default; returns the actions and what to
/// remember next. A take remembers the default it replaces; a retake with
/// the AirPods already default keeps the memory. A yield restores the
/// memory only if the AirPods are still default (the user's own change
/// wins), and always forgets it.
pub(crate) fn plan_default_sink(
    remembered: Option<&str>,
    airpods_sink: Option<&str>,
    current_default: Option<&str>,
    event: SinkEvent,
) -> (Vec<SinkAction>, Option<String>) {
    match (event, airpods_sink) {
        (SinkEvent::Take, Some(airpods)) if current_default != Some(airpods) => (
            vec![SinkAction::SetDefault(airpods.to_string())],
            current_default.map(str::to_string),
        ),
        (SinkEvent::Take, _) => (Vec::new(), remembered.map(str::to_string)),
        (SinkEvent::Yield, Some(airpods)) if current_default == Some(airpods) => (
            remembered
                .map(|sink| SinkAction::SetDefault(sink.to_string()))
                .into_iter()
                .collect(),
            None,
        ),
        (SinkEvent::Yield, _) => (Vec::new(), None),
        (SinkEvent::Hold(_), _) => (Vec::new(), remembered.map(str::to_string)),
    }
}

/// The sound server, injected so tests need none.
pub(crate) trait DefaultSinkIo: Send + Sync + 'static {
    fn current_default(&self) -> BoxFuture<'static, Option<String>>;
    fn airpods_sink(&self) -> BoxFuture<'static, Option<String>>;
    /// True when the server accepted `name` as the default sink.
    fn set_default(&self, name: String) -> BoxFuture<'static, bool>;
    /// Apply the A2DP hold (#264); a release returns once the AirPods sink
    /// is back or its wait ran out.
    fn hold_a2dp(&self, hold: bool) -> BoxFuture<'static, ()>;
}

/// Enqueues route events for one worker task, which ends when this drops.
pub(crate) struct DefaultSinkRouter {
    events: mpsc::UnboundedSender<SinkEvent>,
    /// Events enqueued so far.
    sent: AtomicU64,
    /// Events the worker has applied so far.
    applied: watch::Receiver<u64>,
}

impl DefaultSinkRouter {
    pub(crate) fn spawn(io: impl DefaultSinkIo) -> Self {
        let (router, events, applied) = Self::new();
        tokio::spawn(run(io, events, applied));
        router
    }

    /// The router, and the channel ends its worker takes.
    fn new() -> (Self, mpsc::UnboundedReceiver<SinkEvent>, watch::Sender<u64>) {
        let (events, rx) = mpsc::unbounded_channel();
        let (applied_tx, applied) = watch::channel(0);
        let router = DefaultSinkRouter {
            events,
            sent: AtomicU64::new(0),
            applied,
        };
        (router, rx, applied_tx)
    }

    /// Never blocks.
    pub(crate) fn route(&self, event: SinkEvent) {
        self.sent.fetch_add(1, Ordering::SeqCst);
        let _ = self.events.send(event);
    }

    /// Resolves once the worker has applied every event enqueued before this
    /// call, whatever their outcome, or once the worker is gone.
    pub(crate) fn settled(&self) -> impl Future<Output = ()> + Send + 'static {
        let sent = self.sent.load(Ordering::SeqCst);
        let mut applied = self.applied.clone();
        async move {
            // An Err means the worker is gone: nothing more will apply.
            let _ = applied.wait_for(|&n| n >= sent).await;
        }
    }
}

/// Plan and apply each event in order, counting each in `applied`.
async fn run(
    io: impl DefaultSinkIo,
    mut events: mpsc::UnboundedReceiver<SinkEvent>,
    applied: watch::Sender<u64>,
) {
    let mut remembered: Option<String> = None;
    while let Some(event) = events.recv().await {
        remembered = apply(&io, remembered, event).await;
        applied.send_modify(|n| *n += 1);
    }
}

/// One event; returns the new memory, which is empty unless every action
/// succeeded, so a failed take or restore remembers nothing.
async fn apply(
    io: &impl DefaultSinkIo,
    remembered: Option<String>,
    event: SinkEvent,
) -> Option<String> {
    if let SinkEvent::Hold(hold) = event {
        io.hold_a2dp(hold).await;
        return remembered;
    }
    let airpods = io.airpods_sink().await;
    let current = io.current_default().await;
    let (actions, next) = plan_default_sink(
        remembered.as_deref(),
        airpods.as_deref(),
        current.as_deref(),
        event,
    );
    let mut ok = true;
    for SinkAction::SetDefault(name) in actions {
        if io.set_default(name.clone()).await {
            info!("Default sink: {event:?} set {name}");
        } else {
            warn!("Default sink: {event:?} failed to set {name}");
            ok = false;
        }
    }
    if ok { next } else { None }
}

/// Production I/O on the controller clone `ControllerMedia` holds; it never
/// takes the outer MediaController mutex.
impl DefaultSinkIo for MediaController {
    fn current_default(&self) -> BoxFuture<'static, Option<String>> {
        Box::pin(async {
            let name = tokio::task::spawn_blocking(|| pulse::default_sink_name("LibrePods")).await;
            match name {
                Ok(Ok(name)) => name,
                Ok(Err(e)) => {
                    warn!("Default sink: {e}");
                    None
                }
                Err(_) => None,
            }
        })
    }

    fn airpods_sink(&self) -> BoxFuture<'static, Option<String>> {
        let controller = self.clone();
        Box::pin(async move { controller.airpods_sink().await })
    }

    fn set_default(&self, name: String) -> BoxFuture<'static, bool> {
        Box::pin(async move {
            tokio::task::spawn_blocking(move || set_default_sink_sync(&name))
                .await
                .unwrap_or(false)
        })
    }

    fn hold_a2dp(&self, hold: bool) -> BoxFuture<'static, ()> {
        let controller = self.clone();
        Box::pin(async move { controller.apply_a2dp_hold(hold).await })
    }
}

fn set_default_sink_sync(name: &str) -> bool {
    pulse::set_default_sink("LibrePods", name)
}

#[cfg(test)]
mod tests {
    use super::SinkAction::SetDefault;
    use super::SinkEvent::{Take, Yield};
    use super::*;
    use futures::FutureExt;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::Notify;

    const DAC: &str = "alsa_output.dac";
    const AIRPODS: &str = "bluez_output.AA";

    fn set(sink: &str) -> Vec<SinkAction> {
        vec![SetDefault(sink.to_string())]
    }

    #[test]
    fn take_with_other_default_sets_airpods_and_remembers_it() {
        assert_eq!(
            plan_default_sink(None, Some(AIRPODS), Some(DAC), Take),
            (set(AIRPODS), Some(DAC.to_string()))
        );
    }

    #[test]
    fn take_with_airpods_already_default_keeps_memory() {
        assert_eq!(
            plan_default_sink(None, Some(AIRPODS), Some(AIRPODS), Take),
            (vec![], None)
        );
        assert_eq!(
            plan_default_sink(Some(DAC), Some(AIRPODS), Some(AIRPODS), Take),
            (vec![], Some(DAC.to_string()))
        );
    }

    #[test]
    fn take_with_unknown_default_forgets_earlier_memory() {
        // The default couldn't be read, so the old DAC is stale: a later
        // yield must not restore it over the user's choice.
        assert_eq!(
            plan_default_sink(Some(DAC), Some(AIRPODS), None, Take),
            (set(AIRPODS), None)
        );
    }

    #[test]
    fn take_without_airpods_sink_does_nothing() {
        assert_eq!(
            plan_default_sink(None, None, Some(DAC), Take),
            (vec![], None)
        );
    }

    #[test]
    fn yield_with_airpods_default_restores_previous() {
        assert_eq!(
            plan_default_sink(Some(DAC), Some(AIRPODS), Some(AIRPODS), Yield),
            (set(DAC), None)
        );
    }

    #[test]
    fn yield_after_user_changed_default_does_nothing() {
        assert_eq!(
            plan_default_sink(Some(DAC), Some(AIRPODS), Some("speakers"), Yield),
            (vec![], None)
        );
        // The AirPods sink is gone.
        assert_eq!(
            plan_default_sink(Some(DAC), None, Some(DAC), Yield),
            (vec![], None)
        );
    }

    #[test]
    fn yield_with_nothing_remembered_does_nothing() {
        assert_eq!(
            plan_default_sink(None, Some(AIRPODS), Some(AIRPODS), Yield),
            (vec![], None)
        );
    }

    #[test]
    fn retake_then_yield_restores_original_default() {
        let (_, memory) = plan_default_sink(None, Some(AIRPODS), Some(DAC), Take);
        let (actions, memory) =
            plan_default_sink(memory.as_deref(), Some(AIRPODS), Some(AIRPODS), Take);
        assert_eq!(actions, vec![]);
        assert_eq!(
            plan_default_sink(memory.as_deref(), Some(AIRPODS), Some(AIRPODS), Yield),
            (set(DAC), None)
        );
    }

    /// A sound server with the AirPods sink present; records each
    /// `set_default` call.
    #[derive(Clone)]
    struct FakeSinks {
        default: Arc<Mutex<String>>,
        sets: Arc<Mutex<Vec<String>>>,
        fail: Arc<AtomicBool>,
        /// When set, each `set_default` finishes only on one of its permits.
        gate: Option<Arc<Notify>>,
    }

    impl FakeSinks {
        fn new(fail: bool) -> Self {
            FakeSinks {
                default: Arc::new(Mutex::new(DAC.to_string())),
                sets: Arc::default(),
                fail: Arc::new(AtomicBool::new(fail)),
                gate: None,
            }
        }

        fn sets(&self) -> Vec<String> {
            self.sets.lock().unwrap().clone()
        }
    }

    impl DefaultSinkIo for FakeSinks {
        fn current_default(&self) -> BoxFuture<'static, Option<String>> {
            let default = self.default.lock().unwrap().clone();
            Box::pin(async move { Some(default) })
        }

        fn airpods_sink(&self) -> BoxFuture<'static, Option<String>> {
            Box::pin(async { Some(AIRPODS.to_string()) })
        }

        fn set_default(&self, name: String) -> BoxFuture<'static, bool> {
            self.sets.lock().unwrap().push(name.clone());
            let ok = !self.fail.load(Ordering::SeqCst);
            if ok {
                *self.default.lock().unwrap() = name;
            }
            let gate = self.gate.clone();
            Box::pin(async move {
                if let Some(gate) = gate {
                    gate.notified().await;
                }
                ok
            })
        }

        /// Recorded in `sets`, so the order shows; gated like a set.
        fn hold_a2dp(&self, hold: bool) -> BoxFuture<'static, ()> {
            self.sets.lock().unwrap().push(format!("Hold({hold})"));
            let gate = self.gate.clone();
            Box::pin(async move {
                if let Some(gate) = gate {
                    gate.notified().await;
                }
            })
        }
    }

    /// Send `Take`, let the worker finish it, run `between`, then send
    /// `Yield` and wait for the worker to end.
    async fn take_then_yield(sinks: &FakeSinks, between: impl FnOnce()) {
        let (router, rx, applied) = DefaultSinkRouter::new();
        let worker = tokio::spawn(run(sinks.clone(), rx, applied));
        router.route(Take);
        tokio::time::timeout(Duration::from_secs(3), async {
            while sinks.sets().is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the take's set_default");
        between();
        router.route(Yield);
        drop(router);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn router_take_then_yield_sets_then_restores() {
        let sinks = FakeSinks::new(false);
        take_then_yield(&sinks, || {}).await;
        assert_eq!(sinks.sets(), [AIRPODS, DAC]);
    }

    #[tokio::test]
    async fn router_yield_after_user_change_sets_nothing() {
        let sinks = FakeSinks::new(false);
        take_then_yield(&sinks, || {
            *sinks.default.lock().unwrap() = "speakers".to_string();
        })
        .await;
        assert_eq!(sinks.sets(), [AIRPODS]);
    }

    #[tokio::test]
    async fn router_failed_take_restores_nothing() {
        let sinks = FakeSinks::new(true);
        // The AirPods end up default anyway: only the memory stops the
        // yield.
        take_then_yield(&sinks, || {
            *sinks.default.lock().unwrap() = AIRPODS.to_string();
        })
        .await;
        assert_eq!(sinks.sets(), [AIRPODS]);
    }

    #[tokio::test]
    async fn failed_restore_forgets_the_default() {
        let sinks = FakeSinks::new(false);
        let memory = apply(&sinks, None, Take).await;
        sinks.fail.store(true, Ordering::SeqCst);
        let memory = apply(&sinks, memory, Yield).await;
        sinks.fail.store(false, Ordering::SeqCst);
        // The AirPods are still default: the retake keeps the empty memory.
        let memory = apply(&sinks, memory, Take).await;
        assert_eq!(apply(&sinks, memory, Yield).await, None);
        assert_eq!(sinks.sets(), [AIRPODS, DAC]);
    }

    #[tokio::test]
    async fn failed_take_forgets_an_earlier_default() {
        let sinks = FakeSinks::new(false);
        let memory = apply(&sinks, None, Take).await;
        assert_eq!(memory.as_deref(), Some(DAC));
        *sinks.default.lock().unwrap() = "speakers".to_string();
        sinks.fail.store(true, Ordering::SeqCst);
        let memory = apply(&sinks, memory, Take).await;
        assert_eq!(memory, None);
        sinks.fail.store(false, Ordering::SeqCst);
        *sinks.default.lock().unwrap() = AIRPODS.to_string();
        assert_eq!(apply(&sinks, memory, Yield).await, None);
        assert_eq!(sinks.sets(), [AIRPODS, AIRPODS]);
    }

    #[tokio::test]
    async fn router_runs_events_in_send_order() {
        let sinks = FakeSinks::new(false);
        let (router, rx, applied) = DefaultSinkRouter::new();
        for event in [Take, Yield, Take, Yield] {
            router.route(event);
        }
        drop(router);
        run(sinks.clone(), rx, applied).await;
        assert_eq!(sinks.sets(), [AIRPODS, DAC, AIRPODS, DAC]);
    }

    /// Wait up to 3 s for `settled`; panics if it doesn't resolve.
    async fn settles(settled: impl Future<Output = ()>) {
        tokio::time::timeout(Duration::from_secs(3), settled)
            .await
            .expect("settled");
    }

    #[tokio::test]
    async fn settled_is_ready_with_nothing_queued() {
        let router = DefaultSinkRouter::spawn(FakeSinks::new(false));
        assert_eq!(router.settled().now_or_never(), Some(()));
    }

    #[tokio::test]
    async fn settled_waits_for_a_slow_take() {
        let gate = Arc::new(Notify::new());
        let sinks = FakeSinks {
            gate: Some(gate.clone()),
            ..FakeSinks::new(false)
        };
        let router = DefaultSinkRouter::spawn(sinks.clone());
        router.route(Take);
        let take = router.settled();
        router.route(Yield);
        let take_and_yield = router.settled();
        tokio::pin!(take, take_and_yield);
        // Each set_default is in flight once recorded, held by the gate.
        let sinks = &sinks;
        let in_flight = move |n| {
            tokio::time::timeout(Duration::from_secs(3), async move {
                while sinks.sets().len() < n {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
        };
        in_flight(1).await.expect("the take's set_default");
        let held = tokio::time::timeout(Duration::from_millis(100), &mut take).await;
        assert!(held.is_err(), "settled before the take finished");
        gate.notify_one();
        settles(&mut take).await;
        in_flight(2).await.expect("the yield's set_default");
        let held = tokio::time::timeout(Duration::from_millis(100), &mut take_and_yield).await;
        assert!(held.is_err(), "settled before the yield finished");
        gate.notify_one();
        settles(take_and_yield).await;
        assert_eq!(sinks.sets(), [AIRPODS, DAC]);
    }

    #[tokio::test]
    async fn settled_after_failed_take_and_yield() {
        let sinks = FakeSinks::new(true);
        let router = DefaultSinkRouter::spawn(sinks.clone());
        router.route(Take);
        settles(router.settled()).await;
        router.route(Yield);
        settles(router.settled()).await;
        // The take failed, so the yield had nothing to restore.
        assert_eq!(sinks.sets(), [AIRPODS]);
    }

    #[tokio::test]
    async fn settled_resolves_when_worker_is_gone() {
        let (router, rx, applied) = DefaultSinkRouter::new();
        router.route(Take);
        let settled = router.settled();
        drop((rx, applied));
        settles(settled).await;
    }

    #[test]
    fn hold_plan_sets_nothing_and_keeps_memory() {
        for hold in [true, false] {
            assert_eq!(
                plan_default_sink(
                    Some(DAC),
                    Some(AIRPODS),
                    Some(AIRPODS),
                    SinkEvent::Hold(hold)
                ),
                (vec![], Some(DAC.to_string()))
            );
        }
    }

    #[tokio::test]
    async fn hold_release_then_take_settle_in_send_order() {
        let gate = Arc::new(Notify::new());
        let sinks = FakeSinks {
            gate: Some(gate.clone()),
            ..FakeSinks::new(false)
        };
        let router = DefaultSinkRouter::spawn(sinks.clone());
        router.route(SinkEvent::Hold(false));
        router.route(Take);
        let both = router.settled();
        tokio::pin!(both);
        let sinks = &sinks;
        let in_flight = move |n| {
            tokio::time::timeout(Duration::from_secs(3), async move {
                while sinks.sets().len() < n {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
        };
        in_flight(1).await.expect("the release");
        let held = tokio::time::timeout(Duration::from_millis(100), &mut both).await;
        assert!(held.is_err(), "settled before the release finished");
        // The take waits for the slow release.
        assert_eq!(sinks.sets(), ["Hold(false)"]);
        gate.notify_one();
        in_flight(2).await.expect("the take's set_default");
        let held = tokio::time::timeout(Duration::from_millis(100), &mut both).await;
        assert!(held.is_err(), "settled before the take finished");
        gate.notify_one();
        settles(both).await;
        assert_eq!(sinks.sets(), ["Hold(false)", AIRPODS]);
    }
}
