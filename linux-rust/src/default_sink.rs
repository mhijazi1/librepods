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
use std::cell::RefCell;
use std::rc::Rc;
use tokio::sync::mpsc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SinkEvent {
    /// Linux took the AirPods.
    Take,
    /// Linux lost them.
    Yield,
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
            current_default.or(remembered).map(str::to_string),
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
    }
}

/// The sound server, injected so tests need none.
pub(crate) trait DefaultSinkIo: Send + Sync + 'static {
    fn current_default(&self) -> BoxFuture<'static, Option<String>>;
    fn airpods_sink(&self) -> BoxFuture<'static, Option<String>>;
    /// True when the server accepted `name` as the default sink.
    fn set_default(&self, name: String) -> BoxFuture<'static, bool>;
}

/// Enqueues route events for one worker task, which ends when this drops.
pub(crate) struct DefaultSinkRouter(mpsc::UnboundedSender<SinkEvent>);

impl DefaultSinkRouter {
    pub(crate) fn spawn(io: impl DefaultSinkIo) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run(io, rx));
        DefaultSinkRouter(tx)
    }

    /// Never blocks.
    pub(crate) fn route(&self, event: SinkEvent) {
        let _ = self.0.send(event);
    }
}

/// Plan and apply each event in order. The memory changes only when every
/// action succeeded, so a failed take remembers nothing new.
async fn run(io: impl DefaultSinkIo, mut events: mpsc::UnboundedReceiver<SinkEvent>) {
    let mut remembered: Option<String> = None;
    while let Some(event) = events.recv().await {
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
        if ok {
            remembered = next;
        }
    }
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
}

fn set_default_sink_sync(name: &str) -> bool {
    let Ok((mut mainloop, mut context)) = pulse::connect("LibrePods") else {
        return false;
    };
    let accepted = Rc::new(RefCell::new(false));
    let mut op = context.set_default_sink(name, {
        let accepted = Rc::downgrade(&accepted);
        move |success| pulse::update(&accepted, |accepted| *accepted = success)
    });
    pulse::wait(&mut mainloop, &mut op, "default sink change").is_ok() && *accepted.borrow()
}

#[cfg(test)]
mod tests {
    use super::SinkAction::SetDefault;
    use super::SinkEvent::{Take, Yield};
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

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
        fail: bool,
    }

    impl FakeSinks {
        fn new(fail: bool) -> Self {
            FakeSinks {
                default: Arc::new(Mutex::new(DAC.to_string())),
                sets: Arc::default(),
                fail,
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
            if !self.fail {
                *self.default.lock().unwrap() = name;
            }
            let ok = !self.fail;
            Box::pin(async move { ok })
        }
    }

    /// Send `Take`, let the worker finish it, run `between`, then send
    /// `Yield` and wait for the worker to end.
    async fn take_then_yield(sinks: &FakeSinks, between: impl FnOnce()) {
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = tokio::spawn(run(sinks.clone(), rx));
        let router = DefaultSinkRouter(tx);
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
    async fn router_runs_events_in_send_order() {
        let sinks = FakeSinks::new(false);
        let (tx, rx) = mpsc::unbounded_channel();
        let router = DefaultSinkRouter(tx);
        for event in [Take, Yield, Take, Yield] {
            router.route(event);
        }
        drop(router);
        run(sinks.clone(), rx).await;
        assert_eq!(sinks.sets(), [AIRPODS, DAC, AIRPODS, DAC]);
    }
}
