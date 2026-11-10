//! Default sink routing on handoff (#40): a take makes the AirPods the
//! default sink, so a stream on another sink (a USB DAC) follows them, and
//! losing the take gives the default back to the sink it replaced.
//!
//! `plan_default_sink` decides; `DefaultSinkRouter` runs the events in order
//! on one worker task, so the handoff shell only enqueues and never waits on
//! the sound server.
//!
//! While another host owns, the router can park new playback on a silent
//! null sink (#311): `ParkWatch` then moves the streams that are not worth a
//! take to the local sink as they appear.

use crate::call_detect::{
    StreamDirection, StreamProps, TARGET_PROPS, is_own_or_plumbing, is_takeover_worthy,
    prop_is_true,
};
use crate::media_controller::{MediaController, blocking};
use crate::pulse;
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use libpulse_sys as sys;
use log::{info, warn};
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

/// The silent null sink new playback waits on while parked (#311).
pub(crate) const PARK_SINK: &str = "librepods_park";
/// Its module-null-sink arguments: a low `priority.session`, so the session
/// manager never picks it as a fallback default (spike #314).
const PARK_SINK_ARGS: &str = "sink_name=librepods_park sink_properties=\"priority.session=1 node.description=librepods_park\"";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SinkEvent {
    /// Linux took the AirPods.
    Take,
    /// Linux lost them.
    Yield,
    /// The handoff's A2DP hold on or off (#264); queued with the routes so
    /// `settled()` covers it and a later `Take` finds the AirPods sink.
    Hold(bool),
    /// Make the park sink the default (#311).
    Park,
    /// End the park: an explicit default, then the park sink's unload.
    Unpark(UnparkTo),
}

/// Where an unpark sends the parked playback (#311).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UnparkTo {
    /// The take's ACK: the AirPods' sink, or the local one if it is missing.
    AirPods,
    /// The local sink the park replaced.
    Local,
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
        // `run` applies these without planning.
        (SinkEvent::Hold(_) | SinkEvent::Park | SinkEvent::Unpark(_), _) => {
            (Vec::new(), remembered.map(str::to_string))
        }
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
    /// Load the park sink (#311); its module.
    fn load_park_sink(&self) -> BoxFuture<'static, Option<u32>>;
    /// True when the server unloaded `module`.
    fn unload_module(&self, module: u32) -> BoxFuture<'static, bool>;
    /// The module of a park sink an earlier run left loaded; None when the
    /// module listing failed.
    fn park_module(&self) -> BoxFuture<'static, Option<Option<u32>>>;
    /// Start the park event path, which moves the non-worthy streams on the
    /// park sink to `local` (#311).
    fn watch_park(&self, local: String) -> ParkWatch;
    /// The one park every router on this server shares.
    fn shared_park(&self) -> SharedPark;
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

/// Plan and apply each event in order, counting each in `applied`, after
/// unloading a park sink an earlier run left behind.
async fn run(
    io: impl DefaultSinkIo,
    mut events: mpsc::UnboundedReceiver<SinkEvent>,
    applied: watch::Sender<u64>,
) {
    let shared = io.shared_park();
    unload_leftover_park(&io, &shared).await;
    let mut remembered: Option<String> = None;
    let mut park: Option<Held> = None;
    // The local sink of the park a take with no hold replaced (#488).
    let mut park_local: Option<String> = None;
    loop {
        // Only the park this router holds can end it: an earlier one's
        // watch is dropped.
        let watch_ended = async {
            match park.as_ref() {
                Some(held) => drop(held.ended.clone().await),
                None => std::future::pending().await,
            }
        };
        let event = tokio::select! {
            biased;
            event = events.recv() => event,
            () = watch_ended => {
                // Not a routed event: `applied` doesn't count it. The first
                // holder to see it unparks, for every holder.
                let mut state = shared.lock().await;
                if let Some(held) = park.take()
                    && state.holds(&held)
                    && let Some(parked) = state.park.take()
                {
                    warn!("Default sink: the park's event path failed; unparking to the local sink");
                    unpark(&io, parked, UnparkTo::Local).await;
                }
                continue;
            }
        };
        let Some(event) = event else { break };
        if matches!(event, SinkEvent::Park | SinkEvent::Unpark(_)) {
            park = apply_park(&io, &shared, park, event, &mut remembered).await;
        } else if event == SinkEvent::Take && park.is_some() {
            // A take while parked that replaced the park sink remembers the
            // local sink of the park in place (this router's hold may be on
            // an ended one), which no holder can end while the lock is held.
            let state = shared.lock().await;
            remembered = match apply(&io, remembered, event).await {
                Some(sink) if sink == PARK_SINK => {
                    state.park.as_ref().map(|parked| parked.local.clone())
                }
                next => next,
            };
        } else if event == SinkEvent::Take {
            // A take that replaced another device's park remembers the park,
            // and its local sink for a yield after the park ended (#488); a
            // retake that replaced nothing keeps the first one's.
            let state = shared.lock().await;
            let (next, replaced) = apply_replacing(&io, remembered, event).await;
            remembered = next;
            if replaced.as_deref() == Some(PARK_SINK) {
                park_local = state.park.as_ref().map(|parked| parked.local.clone());
            }
        } else if event == SinkEvent::Yield && remembered.as_deref() == Some(PARK_SINK) {
            // The park only while one is in place, else that local sink; no
            // unpark can end the park between this check and the set.
            let state = shared.lock().await;
            let local = park_local.take();
            if state.park.is_none() {
                remembered = local;
            }
            remembered = apply(&io, remembered, event).await;
        } else {
            remembered = apply(&io, remembered, event).await;
        }
        applied.send_modify(|n| *n += 1);
    }
    // Only this router's hold ends; the last one stops the event path and
    // leaves the park sink loaded, as before.
    let mut state = shared.lock().await;
    if let Some(held) = park
        && state.holds(&held)
    {
        state.holders -= 1;
        if state.holders == 0 {
            state.park = None;
        }
    }
}

/// A park in place (#311).
struct Parked {
    module: u32,
    /// The default it replaced.
    local: String,
    watch: ParkWatch,
}

/// The process's one park, which every router (one per device) shares.
pub(crate) type SharedPark = Arc<tokio::sync::Mutex<ParkState>>;

#[derive(Default)]
pub(crate) struct ParkState {
    park: Option<Parked>,
    /// The routers holding `park`.
    holders: usize,
    /// Bumped by each new park, so a hold on an ended one shows.
    generation: u64,
    /// A module listing has looked for an earlier run's park sink.
    checked: bool,
}

/// One router's hold on the shared park.
struct Held {
    generation: u64,
    /// The park's watch's `ended`.
    ended: Shared<oneshot::Receiver<()>>,
}

impl ParkState {
    /// A new hold on the park in place.
    fn hold(&mut self) -> Option<Held> {
        let parked = self.park.as_ref()?;
        self.holders += 1;
        Some(Held {
            generation: self.generation,
            ended: parked.watch.ended.clone(),
        })
    }

    /// Whether `held` is on the park in place.
    fn holds(&self, held: &Held) -> bool {
        self.park.is_some() && self.generation == held.generation
    }
}

/// Park or unpark; returns this router's hold after `event`.
async fn apply_park(
    io: &impl DefaultSinkIo,
    shared: &SharedPark,
    held: Option<Held>,
    event: SinkEvent,
    remembered: &mut Option<String>,
) -> Option<Held> {
    let mut state = shared.lock().await;
    // A hold on a park whose event path failed holds nothing.
    let held = held.filter(|held| state.holds(held));
    match (event, held) {
        (SinkEvent::Park, None) => {
            // Another device's park: share it, and make it the default
            // again if a take and yield since replaced it.
            if let Some(held) = state.hold() {
                if io.current_default().await.as_deref() != Some(PARK_SINK) {
                    if io.set_default(PARK_SINK.to_string()).await {
                        info!("Default sink: Park set the shared {PARK_SINK} again");
                    } else {
                        warn!("Default sink: Park failed to set the shared {PARK_SINK}");
                    }
                }
                return Some(held);
            }
            // With no local sink, no event path could move notifications
            // and calls off the park sink.
            let Some(local) = io.current_default().await.filter(|sink| sink != PARK_SINK) else {
                warn!("Default sink: no local default sink to park from; not parking");
                return None;
            };
            let Some(module) = io.load_park_sink().await else {
                warn!("Default sink: failed to load {PARK_SINK}");
                return None;
            };
            if !io.set_default(PARK_SINK.to_string()).await {
                warn!("Default sink: Park failed to set {PARK_SINK}");
                // A set that timed out may still have been applied.
                unload_park(io, module, true).await;
                return None;
            }
            info!("Default sink: Park set {PARK_SINK}, replacing {local}");
            let watch = io.watch_park(local.clone());
            state.generation += 1;
            state.holders = 0;
            state.park = Some(Parked {
                module,
                local,
                watch,
            });
            state.hold()
        }
        (SinkEvent::Unpark(to), Some(_)) => {
            state.holders -= 1;
            let local = if state.holders == 0
                && let Some(parked) = state.park.take()
            {
                unpark(io, parked, to).await
            } else if to == UnparkTo::AirPods
                && io.current_default().await.as_deref() == Some(PARK_SINK)
                && let Some(airpods) = io.airpods_sink().await
                && io.set_default(airpods.clone()).await
            {
                // Another device is still parked: the park sink stays.
                info!("Default sink: Unpark(AirPods) set {airpods}; {PARK_SINK} stays");
                state.park.as_ref().map(|parked| parked.local.clone())
            } else {
                None
            };
            if let Some(local) = local {
                // The take before it failed to set the AirPods, so
                // remembered nothing; a later Yield restores the local sink.
                remembered.get_or_insert(local);
            }
            None
        }
        (_, held) => held,
    }
}

/// Stop the event path, set an explicit default while the park sink is
/// still the default (the user's own change wins), then unload it:
/// WirePlumber would keep naming the gone sink as the configured default,
/// and a re-loaded park sink would win it back (spike #314). Never waits
/// for the event path, so a slow one never holds the queue (an exit's hold
/// release) past `SHUTDOWN_RELEASE_WAIT` (#470). Returns the local sink
/// when it set the AirPods sink.
async fn unpark(io: &impl DefaultSinkIo, parked: Parked, to: UnparkTo) -> Option<String> {
    drop(parked.watch);
    let park_default = io.current_default().await.as_deref() == Some(PARK_SINK);
    let airpods = match to {
        UnparkTo::AirPods => io.airpods_sink().await,
        UnparkTo::Local => None,
    };
    let sink = airpods.clone().unwrap_or_else(|| parked.local.clone());
    let set = park_default && io.set_default(sink.clone()).await;
    if set {
        info!("Default sink: Unpark({to:?}) set {sink}");
    } else if park_default {
        warn!("Default sink: Unpark({to:?}) failed to set {sink}");
    }
    unload_park(io, parked.module, park_default && !set).await;
    (set && airpods.is_some()).then_some(parked.local)
}

/// Unload the park sink's `module`; with `pin`, make the default the server
/// picks after it explicit.
async fn unload_park(io: &impl DefaultSinkIo, module: u32, pin: bool) {
    if !io.unload_module(module).await {
        warn!("Default sink: failed to unload {PARK_SINK} (module {module})");
    }
    if pin
        && let Some(sink) = io.current_default().await.filter(|sink| sink != PARK_SINK)
        && io.set_default(sink.clone()).await
    {
        info!("Default sink: set {sink} after unloading {PARK_SINK}");
    }
}

/// An earlier run's park sink (a crash, or an exit before its unpark):
/// looked for until a listing succeeds, and never while this process's
/// own park is in place.
async fn unload_leftover_park(io: &impl DefaultSinkIo, shared: &SharedPark) {
    let mut state = shared.lock().await;
    if state.checked || state.park.is_some() {
        return;
    }
    let Some(leftover) = io.park_module().await else {
        return;
    };
    state.checked = true;
    if let Some(module) = leftover {
        let park_default = io.current_default().await.as_deref() == Some(PARK_SINK);
        info!("Default sink: unloading a leftover {PARK_SINK} (module {module})");
        unload_park(io, module, park_default).await;
    }
}

/// One event; returns the new memory, which is empty unless every action
/// was applied, so a failed take or restore remembers nothing.
async fn apply(
    io: &impl DefaultSinkIo,
    remembered: Option<String>,
    event: SinkEvent,
) -> Option<String> {
    apply_replacing(io, remembered, event).await.0
}

/// `apply`, and the default read that planned its sets once they all
/// applied (#488).
async fn apply_replacing(
    io: &impl DefaultSinkIo,
    remembered: Option<String>,
    event: SinkEvent,
) -> (Option<String>, Option<String>) {
    if let SinkEvent::Hold(hold) = event {
        io.hold_a2dp(hold).await;
        return (remembered, None);
    }
    let airpods = io.airpods_sink().await;
    let current = io.current_default().await;
    let (actions, next) = plan_default_sink(
        remembered.as_deref(),
        airpods.as_deref(),
        current.as_deref(),
        event,
    );
    let replaced = current.filter(|_| !actions.is_empty());
    let mut ok = true;
    for SinkAction::SetDefault(name) in actions {
        if io.set_default(name.clone()).await {
            info!("Default sink: {event:?} set {name}");
        } else if io.current_default().await.as_deref() == Some(name.as_str()) {
            // The reply was lost, but the server applied it.
            info!("Default sink: {event:?} set {name} (its reply was lost)");
        } else {
            warn!("Default sink: {event:?} failed to set {name}");
            ok = false;
        }
    }
    if ok { (next, replaced) } else { (None, None) }
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
        Box::pin(async move { blocking(move || set_default_sink_sync(&name)).await })
    }

    fn hold_a2dp(&self, hold: bool) -> BoxFuture<'static, ()> {
        let controller = self.clone();
        Box::pin(async move { controller.apply_a2dp_hold(hold).await })
    }

    fn load_park_sink(&self) -> BoxFuture<'static, Option<u32>> {
        Box::pin(blocking(|| {
            pulse::load_module("LibrePods", "module-null-sink", PARK_SINK_ARGS)
                .map_err(|e| warn!("Default sink: {e}"))
                .ok()
        }))
    }

    fn unload_module(&self, module: u32) -> BoxFuture<'static, bool> {
        Box::pin(blocking(move || pulse::unload_module("LibrePods", module)))
    }

    fn park_module(&self) -> BoxFuture<'static, Option<Option<u32>>> {
        Box::pin(blocking(|| {
            let modules = pulse::modules("LibrePods").map_err(|e| warn!("Default sink: {e}"));
            let found = modules.ok()?.into_iter().find_map(|m| {
                let ours = m.name.as_deref() == Some("module-null-sink")
                    && m.argument.as_deref().is_some_and(|a| {
                        a.split_whitespace()
                            .any(|arg| arg == "sink_name=librepods_park")
                    });
                ours.then_some(m.index)
            });
            Some(found)
        }))
    }

    fn watch_park(&self, local: String) -> ParkWatch {
        ParkWatch::spawn(pulse::ParkConnection::connect, local)
    }

    fn shared_park(&self) -> SharedPark {
        // One per process: every device's router parks on it.
        static PARK: LazyLock<SharedPark> = LazyLock::new(SharedPark::default);
        PARK.clone()
    }
}

fn set_default_sink_sync(name: &str) -> bool {
    pulse::set_default_sink("LibrePods", name)
}

/// How long the event path waits for sink input events before it checks
/// for a stop.
const PARK_POLL: Duration = Duration::from_millis(100);

/// The park event path (#311), on its own thread while parked, which stops
/// once this drops. It moves the non-worthy streams on the park sink to the
/// local sink: those already there when it starts, then each one a sink
/// input event shows there.
pub(crate) struct ParkWatch {
    /// Dropped to stop the thread.
    _stop: Sender<()>,
    /// Closed once the thread ends: before a stop, only on a failure.
    ended: Shared<oneshot::Receiver<()>>,
}

impl ParkWatch {
    /// Run the event path on the sound server `connect` gives, moving to
    /// the sink named `local`.
    pub(crate) fn spawn<I: ParkIo>(
        connect: impl FnOnce() -> io::Result<I> + Send + 'static,
        local: String,
    ) -> Self {
        let (stop, stopped) = std::sync::mpsc::channel();
        let (end, ended) = oneshot::channel();
        let spawned = std::thread::Builder::new()
            .name("librepods-park".to_string())
            .spawn(move || {
                match connect() {
                    Ok(mut io) => watch_events(&mut io, &local, stopped),
                    Err(e) => warn!("Park: no event path: {e}"),
                }
                drop(end);
            });
        if let Err(e) = spawned {
            warn!("Park: no event path thread: {e}");
        }
        let ended = ended.shared();
        ParkWatch { _stop: stop, ended }
    }
}

/// The park event path's sound server, injected so tests need none.
pub(crate) trait ParkIo {
    /// The sink input events since the last call, after waiting at most
    /// `wait` for one; an error once the connection is gone.
    fn next(&mut self, wait: Duration) -> io::Result<Vec<pulse::SinkInputEvent>>;
    /// The index of the sink named `name`.
    fn sink_index(&mut self, name: &str) -> Option<u32>;
    /// Every sink input, or only the one at `index`; none on an error.
    fn sink_inputs(&mut self, index: Option<u32>) -> Vec<pulse::SinkInput>;
    /// Move sink input `input` to sink `sink`.
    fn move_input(&mut self, input: u32, sink: u32) -> bool;
    /// Move sink input `input` to the default sink.
    fn move_to_default(&mut self, input: u32) -> bool;
    /// Open a corked playback stream that carries `key` = `value`; its sink
    /// input index.
    fn open_corked(&mut self, key: &str, value: &str) -> Option<u32>;
    /// Close the corked stream `input`. Production closes it 500 ms later,
    /// so WirePlumber stores its target first, without waiting.
    fn close(&mut self, input: u32);
}

impl ParkIo for pulse::ParkConnection {
    fn next(&mut self, wait: Duration) -> io::Result<Vec<pulse::SinkInputEvent>> {
        pulse::ParkConnection::next(self, wait)
    }

    fn sink_index(&mut self, name: &str) -> Option<u32> {
        let sinks = self.connection().sinks(pulse::Sinks::Name(name));
        Some(sinks.ok()?.pop()?.index)
    }

    fn sink_inputs(&mut self, index: Option<u32>) -> Vec<pulse::SinkInput> {
        self.connection().sink_inputs(index).unwrap_or_default()
    }

    fn move_input(&mut self, input: u32, sink: u32) -> bool {
        self.connection().move_sink_input(input, Some(sink))
    }

    fn move_to_default(&mut self, input: u32) -> bool {
        self.connection().move_sink_input(input, None)
    }

    fn open_corked(&mut self, key: &str, value: &str) -> Option<u32> {
        let opened = pulse::ParkConnection::open_corked(self, key, value);
        opened.map_err(|e| warn!("Park: {e}")).ok()
    }

    fn close(&mut self, input: u32) {
        pulse::ParkConnection::close(self, input)
    }
}

/// The event path's loop: the park sink's non-worthy streams already there,
/// then each sink input event, until `stopped` is disconnected.
fn watch_events(io: &mut impl ParkIo, local: &str, stopped: Receiver<()>) {
    let (Some(park), Some(local)) = (io.sink_index(PARK_SINK), io.sink_index(local)) else {
        return warn!("Park: no {PARK_SINK} or {local} sink; no event path");
    };
    let mut mover = ParkMover {
        park,
        local,
        moves: HashMap::new(),
        stopped,
    };
    // A watch stopped while it connected lists nothing.
    if !mover.running() {
        return;
    }
    for input in io.sink_inputs(None) {
        mover.input(io, input);
    }
    while mover.running() {
        let events = match io.next(PARK_POLL) {
            Ok(events) => events,
            Err(e) => return warn!("Park: event path ended: {e}"),
        };
        for event in events {
            if !mover.running() {
                return;
            }
            mover.event(io, event);
        }
    }
}

/// How often a stream is moved off the park sink: once, then one retry.
const MAX_MOVES: u8 = 2;

/// What the event path does with each sink input.
struct ParkMover {
    park: u32,
    local: u32,
    /// The moves made so far, by sink input.
    moves: HashMap<u32, u8>,
    /// Disconnected once the park ends: no new input or move follows.
    stopped: Receiver<()>,
}

impl ParkMover {
    /// False once the watch is stopped.
    fn running(&self) -> bool {
        matches!(self.stopped.try_recv(), Err(TryRecvError::Empty))
    }

    fn event(&mut self, io: &mut impl ParkIo, (event, index): pulse::SinkInputEvent) {
        if event & sys::PA_SUBSCRIPTION_EVENT_TYPE_MASK == sys::PA_SUBSCRIPTION_EVENT_REMOVE {
            self.moves.remove(&index);
            return;
        }
        // A NEW, or a CHANGE that may show a moved stream back on the park
        // sink.
        for input in io.sink_inputs(Some(index)) {
            self.input(io, input);
        }
    }

    /// Move a non-worthy stream on the park sink to the local sink, then
    /// reset the target that move stored for its key. A stop ends new
    /// inputs and moves only: a move made always gets its reset.
    fn input(&mut self, io: &mut impl ParkIo, input: pulse::SinkInput) {
        // A stream not linked yet has no sink: moved now, none of it plays
        // on the park sink (spike #314).
        let unlinked = input.sink == sys::PA_INVALID_INDEX;
        if !self.running() || (input.sink != self.park && !unlinked) {
            return;
        }
        let props = &input.props;
        // Unless it targets another sink of its own.
        let park = self.park.to_string();
        if unlinked
            && TARGET_PROPS
                .into_iter()
                .filter_map(|key| props.get(key).map(|v| v.trim()))
                .any(|target| target != PARK_SINK && target != park)
        {
            return;
        }
        let stream = StreamProps {
            direction: StreamDirection::Playback,
            props: props.clone(),
            monitor: false,
            index: input.index,
            corked: input.corked,
            muted: input.mute,
        };
        // LibrePods' own streams (its clearers included), plumbing and
        // monitors are never moved.
        if is_own_or_plumbing(props, std::process::id()) || is_takeover_worthy(&stream) {
            return;
        }
        let index = input.index;
        let moves = self.moves.entry(index).or_default();
        if *moves >= MAX_MOVES {
            return;
        }
        if prop_is_true(props, "node.dont-reconnect") {
            *moves = MAX_MOVES;
            return info!("Park: sink input {index} has node.dont-reconnect; not moving it");
        }
        *moves += 1;
        if !io.move_input(index, self.local) {
            return warn!("Park: failed to move sink input {index} to the local sink");
        }
        info!("Park: moved sink input {index} to the local sink");
        let Some((key, value)) = reset_key(props) else {
            if props.contains_key("media.name") {
                info!(
                    "Park: sink input {index} is keyed by media.name, which no reset can carry; its target stays (known limit)"
                );
            }
            return;
        };
        let Some(own) = io.open_corked(key, value) else {
            return;
        };
        if !io.move_to_default(own) {
            warn!("Park: failed to reset the target of {key}={value}");
        }
        io.close(own);
    }
}

/// The property WirePlumber keys a stream's stored target by (`formKey`,
/// state-stream.lua), when a LibrePods stream can carry it: `media.role`,
/// else `application.id`, else `application.name`. None for a stream keyed
/// by `media.name`: libpulse names every client, so a clearer's own
/// `application.name` would key it instead.
fn reset_key(props: &BTreeMap<String, String>) -> Option<(&'static str, &str)> {
    ["media.role", "application.id", "application.name"]
        .into_iter()
        .find_map(|key| Some((key, props.get(key)?.as_str())))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::SinkAction::SetDefault;
    use super::SinkEvent::{Take, Yield};
    use super::*;
    use futures::FutureExt;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::Notify;

    /// The local default sink `FakeSinks` starts with.
    pub(crate) const DAC: &str = "alsa_output.dac";
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
    /// `set_default` call, each hold, and each park sink load and unload.
    /// The switching tests share it (#311).
    #[derive(Clone)]
    pub(crate) struct FakeSinks {
        pub(crate) default: Arc<Mutex<String>>,
        sets: Arc<Mutex<Vec<String>>>,
        fail: Arc<AtomicBool>,
        /// Each `set_default` applies but reports a failure, as one that
        /// timed out (#311).
        lost_reply: Arc<AtomicBool>,
        /// When set, each `set_default` finishes only on one of its permits.
        gate: Option<Arc<Notify>>,
        /// The AirPods sink is gone (#311).
        no_airpods: Arc<AtomicBool>,
        /// `current_default` reads none, as on a query error, until the
        /// next `airpods_sink` (#488).
        read_fails: Arc<AtomicBool>,
        /// A park sink module an earlier run left (#311).
        leftover: Option<u32>,
        /// The module listing that looks for it fails.
        listing_fails: Arc<AtomicBool>,
        /// The park every router on this server shares.
        park: SharedPark,
        /// The park event path's sound server (#311).
        pulse: Arc<Mutex<FakePulse>>,
        /// Once set, a hold release waits its full `HOLD_RELEASE_WAIT` for
        /// an AirPods sink that never shows up, or until `found` (#311).
        pub(crate) stuck: Arc<AtomicBool>,
        pub(crate) found: Arc<Notify>,
        /// When set, the park's watch connects only once the gate's sender
        /// drops (#311).
        pub(crate) connect: Arc<Mutex<Option<Receiver<()>>>>,
    }

    impl FakeSinks {
        pub(crate) fn new(fail: bool) -> Self {
            FakeSinks {
                default: Arc::new(Mutex::new(DAC.to_string())),
                sets: Arc::default(),
                fail: Arc::new(AtomicBool::new(fail)),
                lost_reply: Arc::default(),
                gate: None,
                no_airpods: Arc::default(),
                read_fails: Arc::default(),
                leftover: None,
                listing_fails: Arc::default(),
                park: Arc::default(),
                pulse: Arc::default(),
                stuck: Arc::default(),
                found: Arc::default(),
                connect: Arc::default(),
            }
        }

        pub(crate) fn sets(&self) -> Vec<String> {
            self.sets.lock().unwrap().clone()
        }
    }

    impl DefaultSinkIo for FakeSinks {
        fn current_default(&self) -> BoxFuture<'static, Option<String>> {
            // An empty default reads as none.
            let default = Some(self.default.lock().unwrap().clone())
                .filter(|d| !d.is_empty() && !self.read_fails.load(Ordering::SeqCst));
            Box::pin(async move { default })
        }

        fn airpods_sink(&self) -> BoxFuture<'static, Option<String>> {
            self.read_fails.store(false, Ordering::SeqCst);
            let gone = self.no_airpods.load(Ordering::SeqCst);
            Box::pin(async move { (!gone).then(|| AIRPODS.to_string()) })
        }

        fn set_default(&self, name: String) -> BoxFuture<'static, bool> {
            self.sets.lock().unwrap().push(name.clone());
            let ok = !self.fail.load(Ordering::SeqCst) && name != GONE;
            if ok {
                *self.default.lock().unwrap() = name;
            }
            let ok = ok && !self.lost_reply.load(Ordering::SeqCst);
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
            let stuck = !hold && self.stuck.load(Ordering::SeqCst);
            let found = self.found.clone();
            Box::pin(async move {
                if let Some(gate) = gate {
                    gate.notified().await;
                }
                if stuck {
                    let wait = crate::media_controller::HOLD_RELEASE_WAIT;
                    let _ = tokio::time::timeout(wait, found.notified()).await;
                }
            })
        }

        fn load_park_sink(&self) -> BoxFuture<'static, Option<u32>> {
            self.sets.lock().unwrap().push("load".to_string());
            Box::pin(async { Some(PARK_MODULE) })
        }

        /// The server picks `PICK` once the park sink it had as default is
        /// gone.
        fn unload_module(&self, module: u32) -> BoxFuture<'static, bool> {
            self.sets.lock().unwrap().push(format!("unload {module}"));
            let mut default = self.default.lock().unwrap();
            if *default == PARK_SINK {
                *default = PICK.to_string();
            }
            Box::pin(async { true })
        }

        fn park_module(&self) -> BoxFuture<'static, Option<Option<u32>>> {
            let listed = (!self.listing_fails.load(Ordering::SeqCst)).then_some(self.leftover);
            Box::pin(async move { listed })
        }

        fn watch_park(&self, local: String) -> ParkWatch {
            let pulse = self.pulse.clone();
            pulse.lock().unwrap().watches += 1;
            let gate = self.connect.lock().unwrap().take();
            ParkWatch::spawn(
                move || {
                    if let Some(gate) = gate {
                        let _ = gate.recv();
                    }
                    let mut fake = pulse.lock().unwrap();
                    if fake.fail == Some("connect") {
                        fake.calls.push("connect failed".to_string());
                        return Err(io::Error::other("connect failed"));
                    }
                    fake.watching = true;
                    drop(fake);
                    Ok(FakePulseIo(pulse))
                },
                local,
            )
        }

        fn shared_park(&self) -> SharedPark {
            self.park.clone()
        }
    }

    const PARK_MODULE: u32 = 7;
    /// The default the server picks once the park sink is gone.
    const PICK: &str = "alsa_output.pick";
    /// A sink that vanished after it was read as the default: `set_default`
    /// and the event path's lookup fail for it.
    const GONE: &str = "alsa_output.gone";
    /// Sink indices in `FakePulse`.
    const PARK_INDEX: u32 = 9;
    const DAC_INDEX: u32 = 1;

    /// The park event path's sound server: sink inputs, the events it
    /// delivers while a watch runs (as a subscription), and the calls made.
    #[derive(Default)]
    struct FakePulse {
        inputs: BTreeMap<u32, pulse::SinkInput>,
        queue: Vec<pulse::SinkInputEvent>,
        watching: bool,
        calls: Vec<String>,
        /// Watches started.
        watches: u32,
        clearers: u32,
        /// When set, the next call named so (`move` or `open`) waits for
        /// the receiver's sender to drop.
        gate: Option<(&'static str, Receiver<()>)>,
        /// The event path's step that fails: `connect`, `park` (its
        /// sink lookup) or `next`.
        fail: Option<&'static str>,
    }

    impl FakePulse {
        /// Add or update a sink input, with its event if a watch runs.
        fn put(&mut self, input: pulse::SinkInput, event: u32) {
            if self.watching {
                self.queue.push((event, input.index));
            }
            self.inputs.insert(input.index, input);
        }
    }

    /// The `ParkIo` a fake watch runs on; ends the watching when dropped.
    struct FakePulseIo(Arc<Mutex<FakePulse>>);

    impl Drop for FakePulseIo {
        fn drop(&mut self) {
            self.0.lock().unwrap().watching = false;
        }
    }

    impl FakePulseIo {
        /// Wait at `call`'s gate, if it has one.
        fn gate(&self, call: &str) {
            let gate = self.0.lock().unwrap().gate.take_if(|(at, _)| *at == call);
            if let Some((_, gate)) = gate {
                let _ = gate.recv();
            }
        }
    }

    impl ParkIo for FakePulseIo {
        fn next(&mut self, wait: Duration) -> io::Result<Vec<pulse::SinkInputEvent>> {
            let mut pulse = self.0.lock().unwrap();
            if pulse.fail == Some("next") {
                return Err(io::Error::other("connection lost"));
            }
            let events = std::mem::take(&mut pulse.queue);
            drop(pulse);
            if events.is_empty() {
                std::thread::sleep(wait.min(Duration::from_millis(5)));
            }
            Ok(events)
        }

        fn sink_index(&mut self, name: &str) -> Option<u32> {
            match name {
                PARK_SINK if self.0.lock().unwrap().fail == Some("park") => None,
                PARK_SINK => Some(PARK_INDEX),
                DAC => Some(DAC_INDEX),
                _ => None,
            }
        }

        fn sink_inputs(&mut self, index: Option<u32>) -> Vec<pulse::SinkInput> {
            let pulse = self.0.lock().unwrap();
            let inputs = pulse
                .inputs
                .values()
                .filter(|i| index.is_none_or(|n| n == i.index));
            inputs.map(clone_input).collect()
        }

        fn move_input(&mut self, input: u32, sink: u32) -> bool {
            self.gate("move");
            let mut pulse = self.0.lock().unwrap();
            pulse.calls.push(format!("move {input} -> {sink}"));
            if let Some(i) = pulse.inputs.get_mut(&input) {
                i.sink = sink;
            }
            true
        }

        fn move_to_default(&mut self, input: u32) -> bool {
            let mut pulse = self.0.lock().unwrap();
            pulse.calls.push(format!("move {input} -> default"));
            true
        }

        /// A LibrePods stream on the park sink, announced like any other.
        fn open_corked(&mut self, key: &str, value: &str) -> Option<u32> {
            self.gate("open");
            let mut pulse = self.0.lock().unwrap();
            pulse.clearers += 1;
            let index = 100 + pulse.clearers;
            pulse.calls.push(format!("open {key}={value} -> {index}"));
            let pid = std::process::id().to_string();
            let own = input(
                index,
                PARK_INDEX,
                &[(key, value), ("application.process.id", &pid)],
            );
            pulse.put(own, sys::PA_SUBSCRIPTION_EVENT_NEW);
            Some(index)
        }

        fn close(&mut self, input: u32) {
            let mut pulse = self.0.lock().unwrap();
            pulse.calls.push(format!("close {input}"));
            pulse.inputs.remove(&input);
        }
    }

    fn clone_input(i: &pulse::SinkInput) -> pulse::SinkInput {
        pulse::SinkInput {
            props: i.props.clone(),
            volume: i.volume,
            ..*i
        }
    }

    /// A sink input on `sink` with `props`.
    fn input(index: u32, sink: u32, props: &[(&str, &str)]) -> pulse::SinkInput {
        pulse::SinkInput {
            index,
            sink,
            corked: false,
            mute: false,
            volume: Default::default(),
            props: props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            client: None,
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

    // #311: the park sink.

    use super::SinkEvent::{Hold, Park, Unpark};
    use super::UnparkTo::{AirPods as ToAirPods, Local as ToLocal};

    /// Route `events` on a worker over `sinks`, then let it end.
    async fn route_all(sinks: &FakeSinks, events: &[SinkEvent]) {
        let (router, rx, applied) = DefaultSinkRouter::new();
        for &event in events {
            router.route(event);
        }
        drop(router);
        run(sinks.clone(), rx, applied).await;
    }

    /// No unload follows a `set_default` of the park sink: an explicit
    /// non-park default comes first.
    fn assert_explicit_default_before_unload(sets: &[String]) {
        let mut last_set = None;
        for call in sets {
            if call.starts_with("unload") {
                assert_ne!(last_set, Some(PARK_SINK), "{sets:?}");
            } else if call != "load" && !call.starts_with("Hold(") {
                last_set = Some(call.as_str());
            }
        }
    }

    /// Wait up to 3 s for `condition`.
    async fn eventually(what: &str, condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
    }

    impl FakeSinks {
        fn calls(&self) -> Vec<String> {
            self.pulse.lock().unwrap().calls.clone()
        }

        fn watching(&self) -> bool {
            self.pulse.lock().unwrap().watching
        }
    }

    /// A router over `sinks` that has parked and whose event path runs.
    async fn parked_router(sinks: &FakeSinks) -> DefaultSinkRouter {
        let router = DefaultSinkRouter::spawn(sinks.clone());
        router.route(Park);
        settles(router.settled()).await;
        eventually("the event path", || sinks.watching()).await;
        router
    }

    fn music_on_park(index: u32) -> pulse::SinkInput {
        input(
            index,
            PARK_INDEX,
            &[("media.role", "Music"), ("application.name", "mpv")],
        )
    }

    fn event_on(index: u32, sink: u32) -> pulse::SinkInput {
        input(
            index,
            sink,
            &[("media.role", "event"), ("application.name", "Firefox")],
        )
    }

    /// The I/O of one move of sink input 5 and its reset by clearer 101.
    fn moved_and_reset(key: &str) -> Vec<String> {
        vec![
            format!("move 5 -> {DAC_INDEX}"),
            format!("open {key} -> 101"),
            "move 101 -> default".to_string(),
            "close 101".to_string(),
        ]
    }

    #[tokio::test]
    async fn park_take_remembers_the_local_sink_and_moves_no_worthy_stream() {
        let sinks = FakeSinks::new(false);
        sinks.pulse.lock().unwrap().put(music_on_park(5), 0);
        let router = parked_router(&sinks).await;
        // A new worthy stream waits too.
        let event = sys::PA_SUBSCRIPTION_EVENT_NEW;
        sinks.pulse.lock().unwrap().put(music_on_park(6), event);
        eventually("the event's handling", || {
            sinks.pulse.lock().unwrap().queue.is_empty()
        })
        .await;
        for event in [Take, Unpark(ToAirPods), Yield] {
            router.route(event);
        }
        settles(router.settled()).await;
        let sets = sinks.sets();
        assert_eq!(sets, ["load", PARK_SINK, AIRPODS, "unload 7", DAC]);
        assert_explicit_default_before_unload(&sets);
        assert_eq!(sinks.calls(), Vec::<String>::new());
    }

    #[tokio::test]
    async fn park_fallback_sets_the_local_sink_and_moves_no_worthy_stream() {
        let sinks = FakeSinks::new(false);
        sinks.pulse.lock().unwrap().put(music_on_park(5), 0);
        let router = parked_router(&sinks).await;
        router.route(Unpark(ToLocal));
        settles(router.settled()).await;
        let sets = sinks.sets();
        assert_eq!(sets, ["load", PARK_SINK, DAC, "unload 7"]);
        assert_explicit_default_before_unload(&sets);
        assert_eq!(sinks.calls(), Vec::<String>::new());
    }

    #[tokio::test]
    async fn park_take_whose_set_failed_then_unpark_remembers_the_local_sink() {
        let sinks = FakeSinks::new(false);
        let router = parked_router(&sinks).await;
        sinks.fail.store(true, Ordering::SeqCst);
        router.route(Take);
        settles(router.settled()).await;
        sinks.fail.store(false, Ordering::SeqCst);
        router.route(Unpark(ToAirPods));
        router.route(Yield);
        settles(router.settled()).await;
        let sets = sinks.sets();
        assert_eq!(sets, ["load", PARK_SINK, AIRPODS, AIRPODS, "unload 7", DAC]);
    }

    /// Park from `local` on a router over `sinks`, then wait for the park
    /// to end itself (its unload) and return the router.
    async fn park_until_it_ends(sinks: &FakeSinks, local: &str) -> DefaultSinkRouter {
        *sinks.default.lock().unwrap() = local.to_string();
        let router = DefaultSinkRouter::spawn(sinks.clone());
        router.route(Park);
        settles(router.settled()).await;
        let unloaded = || sinks.sets().contains(&"unload 7".to_string());
        eventually("the park's own unpark", unloaded).await;
        // The server's pick, if any, is set right after the unload.
        tokio::time::sleep(Duration::from_millis(100)).await;
        router
    }

    #[tokio::test]
    async fn park_event_path_failing_to_connect_unparks_to_local() {
        let sinks = FakeSinks::new(false);
        sinks.pulse.lock().unwrap().fail = Some("connect");
        let _router = park_until_it_ends(&sinks, DAC).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK, DAC, "unload 7"]);
        assert_eq!(*sinks.default.lock().unwrap(), DAC);
    }

    #[tokio::test]
    async fn park_event_path_without_a_sink_unparks() {
        // No park sink to watch.
        let sinks = FakeSinks::new(false);
        sinks.pulse.lock().unwrap().fail = Some("park");
        let _router = park_until_it_ends(&sinks, DAC).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK, DAC, "unload 7"]);
        assert_eq!(*sinks.default.lock().unwrap(), DAC);
        // The local sink vanished after the park read it: the server's
        // pick is set.
        let sinks = FakeSinks::new(false);
        let _router = park_until_it_ends(&sinks, GONE).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK, GONE, "unload 7", PICK]);
        assert_eq!(*sinks.default.lock().unwrap(), PICK);
    }

    #[tokio::test]
    async fn park_event_path_lost_after_start_unparks_to_local() {
        let sinks = FakeSinks::new(false);
        let router = parked_router(&sinks).await;
        sinks.pulse.lock().unwrap().fail = Some("next");
        let unloaded = || sinks.sets().contains(&"unload 7".to_string());
        eventually("the park's own unpark", unloaded).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK, DAC, "unload 7"]);
        assert_eq!(*sinks.default.lock().unwrap(), DAC);
        // The router still routes, and counts only routed events.
        router.route(Take);
        settles(router.settled()).await;
        assert_eq!(sinks.sets().last().map(String::as_str), Some(AIRPODS));
    }

    #[tokio::test]
    async fn park_event_path_failure_of_an_earlier_park_leaves_a_new_one() {
        let sinks = FakeSinks::new(false);
        let (connect, gate) = std::sync::mpsc::channel::<()>();
        *sinks.connect.lock().unwrap() = Some(gate);
        let router = DefaultSinkRouter::spawn(sinks.clone());
        // The first park's watch still connects when it ends.
        for event in [Park, Unpark(ToLocal), Park] {
            router.route(event);
        }
        settles(router.settled()).await;
        eventually("the new park's event path", || sinks.watching()).await;
        sinks.pulse.lock().unwrap().fail = Some("connect");
        drop(connect);
        let failed = || sinks.calls().contains(&"connect failed".to_string());
        eventually("the old watch's failure", failed).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let sets = sinks.sets();
        assert_eq!(
            sets,
            ["load", PARK_SINK, DAC, "unload 7", "load", PARK_SINK]
        );
        assert_eq!(*sinks.default.lock().unwrap(), PARK_SINK);
        assert!(sinks.watching());
        drop(router);
    }

    #[tokio::test]
    async fn park_lifecycle_park_loads_then_sets() {
        let sinks = FakeSinks::new(false);
        route_all(&sinks, &[Park]).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK]);
        // A second park while parked does nothing.
        let sinks = FakeSinks::new(false);
        route_all(&sinks, &[Park, Park]).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK]);
    }

    #[tokio::test]
    async fn park_lifecycle_unpark_on_ack() {
        let sinks = FakeSinks::new(false);
        route_all(&sinks, &[Park, Hold(false), Take, Unpark(ToAirPods)]).await;
        let sets = sinks.sets();
        assert_eq!(
            sets,
            ["load", PARK_SINK, "Hold(false)", AIRPODS, "unload 7"]
        );
        assert_explicit_default_before_unload(&sets);
    }

    #[tokio::test]
    async fn park_lifecycle_unpark_on_ack_without_airpods_sink() {
        let sinks = FakeSinks::new(false);
        sinks.no_airpods.store(true, Ordering::SeqCst);
        route_all(&sinks, &[Park, Hold(false), Take, Unpark(ToAirPods)]).await;
        let sets = sinks.sets();
        assert_eq!(sets, ["load", PARK_SINK, "Hold(false)", DAC, "unload 7"]);
        assert_explicit_default_before_unload(&sets);
    }

    #[tokio::test]
    async fn park_lifecycle_fallback_then_take() {
        let sinks = FakeSinks::new(false);
        route_all(&sinks, &[Park, Unpark(ToLocal), Take, Yield]).await;
        let sets = sinks.sets();
        assert_eq!(sets, ["load", PARK_SINK, DAC, "unload 7", AIRPODS, DAC]);
        assert_explicit_default_before_unload(&sets);
    }

    #[tokio::test]
    async fn park_lifecycle_quit() {
        let sinks = FakeSinks::new(false);
        route_all(&sinks, &[Park, Hold(false), Unpark(ToLocal)]).await;
        let sets = sinks.sets();
        assert_eq!(sets, ["load", PARK_SINK, "Hold(false)", DAC, "unload 7"]);
        assert_explicit_default_before_unload(&sets);
    }

    #[tokio::test]
    async fn park_lifecycle_user_changed_default_wins() {
        let sinks = FakeSinks::new(false);
        let router = DefaultSinkRouter::spawn(sinks.clone());
        router.route(Park);
        settles(router.settled()).await;
        *sinks.default.lock().unwrap() = "speakers".to_string();
        router.route(Unpark(ToLocal));
        settles(router.settled()).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK, "unload 7"]);
        assert_eq!(*sinks.default.lock().unwrap(), "speakers");
    }

    #[tokio::test]
    async fn park_lifecycle_failed_park_set_pins_the_servers_pick() {
        // The set timed out after the server applied it.
        let sinks = FakeSinks::new(false);
        sinks.lost_reply.store(true, Ordering::SeqCst);
        route_all(&sinks, &[Park]).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK, "unload 7", PICK]);
        assert_eq!(*sinks.default.lock().unwrap(), PICK);
    }

    #[tokio::test]
    async fn park_lifecycle_no_local_sink_parks_nothing() {
        // Unreadable, or the park sink itself: no event path could run.
        for default in ["", PARK_SINK] {
            let sinks = FakeSinks::new(false);
            *sinks.default.lock().unwrap() = default.to_string();
            route_all(&sinks, &[Park, Unpark(ToLocal)]).await;
            assert_eq!(sinks.sets(), Vec::<String>::new(), "{default:?}");
        }
    }

    #[tokio::test]
    async fn park_lifecycle_startup_unloads_a_leftover() {
        // The leftover was the default: the server's pick is made explicit.
        let sinks = FakeSinks {
            leftover: Some(3),
            ..FakeSinks::new(false)
        };
        *sinks.default.lock().unwrap() = PARK_SINK.to_string();
        route_all(&sinks, &[]).await;
        assert_eq!(sinks.sets(), ["unload 3", PICK]);
        // It wasn't.
        let sinks = FakeSinks {
            leftover: Some(3),
            ..FakeSinks::new(false)
        };
        route_all(&sinks, &[]).await;
        assert_eq!(sinks.sets(), ["unload 3"]);
        // None.
        let sinks = FakeSinks::new(false);
        route_all(&sinks, &[]).await;
        assert_eq!(sinks.sets(), Vec::<String>::new());
    }

    // #448: one park shared by every router; the leftover check retries; a
    // lost reply keeps the memory.

    #[tokio::test]
    async fn park_shared_two_routers_unload_after_last() {
        let gate = Arc::new(Notify::new());
        let sinks = FakeSinks {
            gate: Some(gate.clone()),
            ..FakeSinks::new(false)
        };
        let a = DefaultSinkRouter::spawn(sinks.clone());
        let b = DefaultSinkRouter::spawn(sinks.clone());
        a.route(Park);
        eventually("A's set of the park sink", || sinks.sets().len() == 2).await;
        // B's Park overlaps A's set, held by the gate.
        b.route(Park);
        tokio::time::sleep(Duration::from_millis(50)).await;
        gate.notify_one();
        settles(a.settled()).await;
        settles(b.settled()).await;
        eventually("the event path", || sinks.watching()).await;
        a.route(Unpark(ToLocal));
        settles(a.settled()).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK]);
        b.route(Unpark(ToLocal));
        eventually("B's set of the local sink", || sinks.sets().len() == 3).await;
        gate.notify_one();
        settles(b.settled()).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK, DAC, "unload 7"]);
        assert_eq!(sinks.pulse.lock().unwrap().watches, 1);
        assert_eq!(*sinks.default.lock().unwrap(), DAC);
    }

    #[tokio::test]
    async fn park_shared_airpods_unpark_keeps_park_for_other() {
        let sinks = FakeSinks::new(false);
        let a = parked_router(&sinks).await;
        let b = parked_router(&sinks).await;
        a.route(Unpark(ToAirPods));
        settles(a.settled()).await;
        assert_eq!(*sinks.default.lock().unwrap(), AIRPODS);
        assert!(sinks.watching());
        b.route(Unpark(ToLocal));
        settles(b.settled()).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK, AIRPODS, "unload 7"]);
        assert_eq!(*sinks.default.lock().unwrap(), AIRPODS);
    }

    #[tokio::test]
    async fn park_shared_joined_take_remembers_the_local_sink() {
        let sinks = FakeSinks::new(false);
        let _a = parked_router(&sinks).await;
        let b = parked_router(&sinks).await;
        for event in [Take, Unpark(ToAirPods), Yield] {
            b.route(event);
        }
        settles(b.settled()).await;
        // No PARK_SINK set follows the take, and the yield restores the DAC.
        assert_eq!(sinks.sets(), ["load", PARK_SINK, AIRPODS, DAC]);
    }

    #[tokio::test]
    async fn park_shared_event_path_failure_ends_it_for_all() {
        let sinks = FakeSinks::new(false);
        let a = parked_router(&sinks).await;
        let b = parked_router(&sinks).await;
        sinks.pulse.lock().unwrap().fail = Some("next");
        let unloaded = || sinks.sets().contains(&"unload 7".to_string());
        eventually("the park's own unpark", unloaded).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let ended = ["load", PARK_SINK, DAC, "unload 7"];
        assert_eq!(sinks.sets(), ended);
        for router in [&a, &b] {
            router.route(Unpark(ToLocal));
            settles(router.settled()).await;
        }
        assert_eq!(sinks.sets(), ended);
    }

    #[tokio::test]
    async fn park_shared_router_dropped_keeps_park_for_other() {
        let sinks = FakeSinks::new(false);
        let a = parked_router(&sinks).await;
        let b = parked_router(&sinks).await;
        drop(a);
        let one_hold = || sinks.park.try_lock().is_ok_and(|park| park.holders == 1);
        eventually("A's hold to end", one_hold).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK]);
        assert!(sinks.watching());
        b.route(Unpark(ToLocal));
        settles(b.settled()).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK, DAC, "unload 7"]);
    }

    #[tokio::test]
    async fn park_shared_rejoin_after_yield_sets_the_park() {
        let sinks = FakeSinks::new(false);
        let a = parked_router(&sinks).await;
        let _b = parked_router(&sinks).await;
        // A takes and yields while B stays parked, then parks again.
        for event in [Take, Unpark(ToAirPods), Yield, Park] {
            a.route(event);
        }
        settles(a.settled()).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK, AIRPODS, DAC, PARK_SINK]);
        assert_eq!(sinks.pulse.lock().unwrap().watches, 1);
        assert_eq!(*sinks.default.lock().unwrap(), PARK_SINK);
    }

    #[tokio::test]
    async fn park_shared_stale_hold_take_remembers_the_live_parks_local() {
        const HDMI: &str = "alsa_output.hdmi";
        let sinks = FakeSinks::new(false);
        let a = parked_router(&sinks).await;
        let b = parked_router(&sinks).await;
        // B's events wait behind a hold release that waits for `found`.
        sinks.stuck.store(true, Ordering::SeqCst);
        for event in [Hold(false), Take, Unpark(ToAirPods), Yield] {
            b.route(event);
        }
        let held = || sinks.sets().contains(&"Hold(false)".to_string());
        eventually("B's hold release", held).await;
        // The event path fails; A unparks to the DAC.
        sinks.pulse.lock().unwrap().fail = Some("next");
        let unloaded = || sinks.sets().contains(&"unload 7".to_string());
        eventually("the park's own unpark", unloaded).await;
        // A parks again from HDMI; its watch waits to connect, so it runs.
        *sinks.default.lock().unwrap() = HDMI.to_string();
        let (_connect, gate) = std::sync::mpsc::channel::<()>();
        *sinks.connect.lock().unwrap() = Some(gate);
        a.route(Park);
        settles(a.settled()).await;
        sinks.found.notify_one();
        settles(b.settled()).await;
        let sets = sinks.sets();
        assert_eq!(
            sets,
            [
                "load",
                PARK_SINK,
                "Hold(false)",
                DAC,
                "unload 7",
                "load",
                PARK_SINK,
                AIRPODS,
                HDMI
            ]
        );
    }

    #[tokio::test]
    async fn park_shared_take_reply_pending_keeps_the_parks_local() {
        let sinks = FakeSinks::new(false);
        let _a = parked_router(&sinks).await;
        // Only B's sets wait on the gate; its join sets nothing.
        let gate = Arc::new(Notify::new());
        let gated = FakeSinks {
            gate: Some(gate.clone()),
            ..sinks.clone()
        };
        let b = parked_router(&gated).await;
        b.route(Take);
        eventually("B's take to apply", || sinks.sets().len() == 3).await;
        // The event path fails while the take's reply is pending.
        sinks.pulse.lock().unwrap().fail = Some("next");
        tokio::time::sleep(Duration::from_millis(100)).await;
        gate.notify_one();
        settles(b.settled()).await;
        let unloaded = || sinks.sets().contains(&"unload 7".to_string());
        eventually("the park's own unpark", unloaded).await;
        b.route(Unpark(ToAirPods));
        b.route(Yield);
        // A permit for the yield's set.
        gate.notify_one();
        settles(b.settled()).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK, AIRPODS, "unload 7", DAC]);
        assert_eq!(*sinks.default.lock().unwrap(), DAC);
    }

    // #488: a take on a router that holds no park replaced another device's.

    #[tokio::test]
    async fn park_shared_unheld_take_yield_restores_the_park_while_parked() {
        let sinks = FakeSinks::new(false);
        let _a = parked_router(&sinks).await;
        let b = DefaultSinkRouter::spawn(sinks.clone());
        b.route(Take);
        b.route(Yield);
        settles(b.settled()).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK, AIRPODS, PARK_SINK]);
        assert_eq!(*sinks.default.lock().unwrap(), PARK_SINK);
    }

    #[tokio::test]
    async fn park_shared_unheld_take_yield_after_unpark_restores_the_parks_local() {
        let sinks = FakeSinks::new(false);
        let a = parked_router(&sinks).await;
        let b = DefaultSinkRouter::spawn(sinks.clone());
        b.route(Take);
        settles(b.settled()).await;
        a.route(Unpark(ToLocal));
        settles(a.settled()).await;
        b.route(Yield);
        settles(b.settled()).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK, AIRPODS, "unload 7", DAC]);
        assert_eq!(*sinks.default.lock().unwrap(), DAC);
    }

    #[tokio::test]
    async fn park_shared_unheld_take_yield_restores_a_new_park() {
        const HDMI: &str = "alsa_output.hdmi";
        let sinks = FakeSinks::new(false);
        let a = parked_router(&sinks).await;
        let b = DefaultSinkRouter::spawn(sinks.clone());
        b.route(Take);
        settles(b.settled()).await;
        a.route(Unpark(ToLocal));
        settles(a.settled()).await;
        // A parks again from HDMI, then another device's Unpark(AirPods)
        // leaves the park in place. Its watch waits to connect, so it runs.
        *sinks.default.lock().unwrap() = HDMI.to_string();
        let (_connect, gate) = std::sync::mpsc::channel::<()>();
        *sinks.connect.lock().unwrap() = Some(gate);
        a.route(Park);
        settles(a.settled()).await;
        *sinks.default.lock().unwrap() = AIRPODS.to_string();
        b.route(Yield);
        settles(b.settled()).await;
        assert_eq!(sinks.sets().last().map(String::as_str), Some(PARK_SINK));
        assert_eq!(*sinks.default.lock().unwrap(), PARK_SINK);
    }

    #[tokio::test]
    async fn park_shared_unheld_retake_keeps_the_replaced_parks_local() {
        const HDMI: &str = "alsa_output.hdmi";
        let sinks = FakeSinks::new(false);
        let a = parked_router(&sinks).await;
        let b = DefaultSinkRouter::spawn(sinks.clone());
        b.route(Take);
        settles(b.settled()).await;
        a.route(Unpark(ToLocal));
        settles(a.settled()).await;
        // A parks again from HDMI; its watch waits to connect, so it runs.
        *sinks.default.lock().unwrap() = HDMI.to_string();
        let (_connect, gate) = std::sync::mpsc::channel::<()>();
        *sinks.connect.lock().unwrap() = Some(gate);
        a.route(Park);
        settles(a.settled()).await;
        // B's retake, with the AirPods already default, sets nothing.
        *sinks.default.lock().unwrap() = AIRPODS.to_string();
        b.route(Take);
        settles(b.settled()).await;
        a.route(Unpark(ToLocal));
        settles(a.settled()).await;
        b.route(Yield);
        settles(b.settled()).await;
        assert_eq!(sinks.sets().last().map(String::as_str), Some(DAC));
        assert_eq!(*sinks.default.lock().unwrap(), DAC);
    }

    #[tokio::test]
    async fn park_shared_unheld_take_records_from_the_read_that_planned_it() {
        let sinks = FakeSinks::new(false);
        let a = parked_router(&sinks).await;
        let b = DefaultSinkRouter::spawn(sinks.clone());
        // A query error that clears before the take lists the AirPods sink.
        sinks.read_fails.store(true, Ordering::SeqCst);
        b.route(Take);
        settles(b.settled()).await;
        a.route(Unpark(ToLocal));
        settles(a.settled()).await;
        b.route(Yield);
        settles(b.settled()).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK, AIRPODS, "unload 7", DAC]);
        assert_eq!(*sinks.default.lock().unwrap(), DAC);
    }

    #[tokio::test]
    async fn park_shared_unheld_retake_without_airpods_keeps_the_replaced_parks_local() {
        const HDMI: &str = "alsa_output.hdmi";
        let sinks = FakeSinks::new(false);
        let a = parked_router(&sinks).await;
        let b = DefaultSinkRouter::spawn(sinks.clone());
        b.route(Take);
        settles(b.settled()).await;
        a.route(Unpark(ToLocal));
        settles(a.settled()).await;
        // A parks again from HDMI; its watch waits to connect, so it runs.
        *sinks.default.lock().unwrap() = HDMI.to_string();
        let (_connect, gate) = std::sync::mpsc::channel::<()>();
        *sinks.connect.lock().unwrap() = Some(gate);
        a.route(Park);
        settles(a.settled()).await;
        // B's retake finds no AirPods sink, so sets nothing.
        sinks.no_airpods.store(true, Ordering::SeqCst);
        b.route(Take);
        settles(b.settled()).await;
        sinks.no_airpods.store(false, Ordering::SeqCst);
        *sinks.default.lock().unwrap() = AIRPODS.to_string();
        a.route(Unpark(ToLocal));
        settles(a.settled()).await;
        b.route(Yield);
        settles(b.settled()).await;
        assert_eq!(sinks.sets().last().map(String::as_str), Some(DAC));
        assert_eq!(*sinks.default.lock().unwrap(), DAC);
    }

    #[tokio::test]
    async fn park_leftover_check_retries_after_failed_listing() {
        let sinks = FakeSinks {
            leftover: Some(3),
            ..FakeSinks::new(false)
        };
        sinks.listing_fails.store(true, Ordering::SeqCst);
        route_all(&sinks, &[]).await;
        assert_eq!(sinks.sets(), Vec::<String>::new());
        sinks.listing_fails.store(false, Ordering::SeqCst);
        route_all(&sinks, &[]).await;
        assert_eq!(sinks.sets(), ["unload 3"]);
        // Done once a listing succeeded.
        route_all(&sinks, &[]).await;
        assert_eq!(sinks.sets(), ["unload 3"]);
    }

    #[tokio::test]
    async fn park_leftover_check_spares_the_shared_park() {
        let sinks = FakeSinks::new(false);
        sinks.listing_fails.store(true, Ordering::SeqCst);
        let a = parked_router(&sinks).await;
        sinks.listing_fails.store(false, Ordering::SeqCst);
        // The listing would show the park itself.
        let listed = FakeSinks {
            leftover: Some(PARK_MODULE),
            ..sinks.clone()
        };
        route_all(&listed, &[]).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK]);
        assert!(sinks.watching());
        a.route(Unpark(ToLocal));
        settles(a.settled()).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK, DAC, "unload 7"]);
    }

    #[tokio::test]
    async fn take_with_lost_reply_still_restores_on_yield() {
        let sinks = FakeSinks::new(false);
        sinks.lost_reply.store(true, Ordering::SeqCst);
        route_all(&sinks, &[Take, Yield]).await;
        assert_eq!(sinks.sets(), [AIRPODS, DAC]);
    }

    #[tokio::test]
    async fn park_take_with_lost_reply_then_unpark_remembers_the_local_sink() {
        let sinks = FakeSinks::new(false);
        let router = parked_router(&sinks).await;
        sinks.lost_reply.store(true, Ordering::SeqCst);
        for event in [Take, Unpark(ToAirPods), Yield] {
            router.route(event);
        }
        settles(router.settled()).await;
        assert_eq!(sinks.sets(), ["load", PARK_SINK, AIRPODS, "unload 7", DAC]);
    }

    #[test]
    fn park_event_reset_key_selection() {
        let key = |props: &[(&str, &str)]| {
            let props: BTreeMap<String, String> = props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            reset_key(&props).map(|(k, v)| (k, v.to_string()))
        };
        let pair = |k, v: &str| Some((k, v.to_string()));
        assert_eq!(
            key(&[("media.role", "event"), ("application.name", "Firefox")]),
            pair("media.role", "event")
        );
        assert_eq!(
            key(&[("application.name", "Firefox"), ("media.name", "beep")]),
            pair("application.name", "Firefox")
        );
        assert_eq!(
            key(&[
                ("application.name", "Firefox"),
                ("application.id", "org.mozilla.firefox")
            ]),
            pair("application.id", "org.mozilla.firefox")
        );
        // A clearer's own application.name would key it: no reset.
        assert_eq!(key(&[("media.name", "beep")]), None);
        assert_eq!(key(&[]), None);
    }

    /// A `ParkMover` between the park sink and the DAC on `pulse`; it runs
    /// while the returned sender lives.
    fn mover(pulse: &Arc<Mutex<FakePulse>>) -> (ParkMover, FakePulseIo, Sender<()>) {
        let (running, stopped) = std::sync::mpsc::channel();
        let mover = ParkMover {
            park: PARK_INDEX,
            local: DAC_INDEX,
            moves: HashMap::new(),
            stopped,
        };
        (mover, FakePulseIo(pulse.clone()), running)
    }

    /// The I/O one NEW for a sink input 5 with `props` on the park sink
    /// gives.
    fn new_on_park(props: &[(&str, &str)]) -> Vec<String> {
        new_on(PARK_INDEX, props)
    }

    /// The I/O one NEW for a sink input 5 with `props` on `sink` gives.
    fn new_on(sink: u32, props: &[(&str, &str)]) -> Vec<String> {
        let pulse = Arc::<Mutex<FakePulse>>::default();
        pulse.lock().unwrap().put(input(5, sink, props), 0);
        let (mut mover, mut io, _running) = mover(&pulse);
        mover.event(&mut io, (sys::PA_SUBSCRIPTION_EVENT_NEW, 5));
        pulse.lock().unwrap().calls.clone()
    }

    #[test]
    fn park_event_moves_then_resets() {
        let role = [("media.role", "event"), ("application.name", "Firefox")];
        assert_eq!(new_on_park(&role), moved_and_reset("media.role=event"));
        // libcanberra: no role, an event.id.
        let roleless = [("application.name", "canberra"), ("event.id", "bell")];
        assert_eq!(
            new_on_park(&roleless),
            moved_and_reset("application.name=canberra")
        );
        let call = [("media.role", "phone"), ("application.name", "Firefox")];
        assert_eq!(new_on_park(&call), moved_and_reset("media.role=phone"));
        let call = [("media.role", "Communication")];
        assert_eq!(
            new_on_park(&call),
            moved_and_reset("media.role=Communication")
        );
        // Not linked yet: moved before it plays on the park sink.
        let pulse = Arc::<Mutex<FakePulse>>::default();
        let unlinked = event_on(5, sys::PA_INVALID_INDEX);
        pulse.lock().unwrap().put(unlinked, 0);
        let (mut mover, mut io, _running) = mover(&pulse);
        mover.event(&mut io, (sys::PA_SUBSCRIPTION_EVENT_NEW, 5));
        let calls = pulse.lock().unwrap().calls.clone();
        assert_eq!(calls, moved_and_reset("media.role=event"));
        // Or when it targets the park sink, by name or index.
        let park = PARK_INDEX.to_string();
        for target in [("target.object", PARK_SINK), ("node.target", park.as_str())] {
            let props = [("media.role", "event"), target];
            let calls = new_on(sys::PA_INVALID_INDEX, &props);
            assert_eq!(calls, moved_and_reset("media.role=event"), "{target:?}");
        }
    }

    #[test]
    fn park_event_media_name_only_moves_without_a_reset() {
        let anonymous = [("media.name", "beep")];
        assert_eq!(new_on_park(&anonymous), [format!("move 5 -> {DAC_INDEX}")]);
    }

    #[test]
    fn park_event_leaves_own_virtual_and_dont_reconnect_streams() {
        let pid = std::process::id().to_string();
        let event = [("media.role", "event")];
        for extra in [
            ("application.process.id", pid.as_str()),
            ("application.name", "LibrePods"),
            ("node.virtual", "true"),
            ("stream.monitor", "true"),
            ("node.dont-reconnect", "true"),
        ] {
            let props = [event[0], extra];
            assert_eq!(new_on_park(&props), Vec::<String>::new(), "{extra:?}");
        }
        // Nor a worthy stream, or one on another sink.
        let music = [("media.role", "Music"), ("application.name", "mpv")];
        assert_eq!(new_on_park(&music), Vec::<String>::new());
        let pulse = Arc::<Mutex<FakePulse>>::default();
        pulse.lock().unwrap().put(event_on(5, DAC_INDEX), 0);
        let (mut mover, mut io, _running) = mover(&pulse);
        mover.event(&mut io, (sys::PA_SUBSCRIPTION_EVENT_NEW, 5));
        assert_eq!(pulse.lock().unwrap().calls, Vec::<String>::new());
        // Nor an unlinked stream that targets a sink of its own: its stored
        // target would be reset too.
        for target in [("target.object", "alsa_output.usb"), ("node.target", "42")] {
            let props = [event[0], target];
            let calls = new_on(sys::PA_INVALID_INDEX, &props);
            assert_eq!(calls, Vec::<String>::new(), "{target:?}");
        }
    }

    #[test]
    fn park_event_change_still_on_park_retries_once() {
        let pulse = Arc::<Mutex<FakePulse>>::default();
        pulse.lock().unwrap().put(event_on(5, PARK_INDEX), 0);
        let (mut mover, mut io, _running) = mover(&pulse);
        mover.event(&mut io, (sys::PA_SUBSCRIPTION_EVENT_NEW, 5));
        // A CHANGE once it is on the local sink: nothing.
        let change = sys::PA_SUBSCRIPTION_EVENT_SINK_INPUT | sys::PA_SUBSCRIPTION_EVENT_CHANGE;
        mover.event(&mut io, (change, 5));
        assert_eq!(
            pulse.lock().unwrap().calls,
            moved_and_reset("media.role=event")
        );
        // Back on the park sink: one retry, then never again.
        for _ in 0..3 {
            pulse.lock().unwrap().inputs.get_mut(&5).unwrap().sink = PARK_INDEX;
            mover.event(&mut io, (change, 5));
        }
        let calls = pulse.lock().unwrap().calls.clone();
        let mut expected = moved_and_reset("media.role=event");
        expected.extend([
            format!("move 5 -> {DAC_INDEX}"),
            "open media.role=event -> 102".to_string(),
            "move 102 -> default".to_string(),
            "close 102".to_string(),
        ]);
        assert_eq!(calls, expected);
        // A removal forgets it: its index may be reused.
        let remove = sys::PA_SUBSCRIPTION_EVENT_SINK_INPUT | sys::PA_SUBSCRIPTION_EVENT_REMOVE;
        mover.event(&mut io, (remove, 5));
        assert!(mover.moves.is_empty());
    }

    #[tokio::test]
    async fn park_event_new_while_parked_moves_and_resets_once() {
        let sinks = FakeSinks::new(false);
        let router = parked_router(&sinks).await;
        let new = sys::PA_SUBSCRIPTION_EVENT_NEW;
        sinks
            .pulse
            .lock()
            .unwrap()
            .put(event_on(5, PARK_INDEX), new);
        eventually("the move and reset", || sinks.calls().len() >= 4).await;
        // The clearer's own NEW moves nothing.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(sinks.calls(), moved_and_reset("media.role=event"));
        drop(router);
    }

    #[tokio::test]
    async fn park_event_nothing_unparked_worthy_or_own() {
        let sinks = FakeSinks::new(false);
        let new = sys::PA_SUBSCRIPTION_EVENT_NEW;
        // Before the park, an event sound plays on the local sink.
        sinks.pulse.lock().unwrap().put(event_on(4, DAC_INDEX), new);
        let router = parked_router(&sinks).await;
        let pid = std::process::id().to_string();
        let own = input(
            6,
            PARK_INDEX,
            &[("media.role", "event"), ("application.process.id", &pid)],
        );
        sinks.pulse.lock().unwrap().put(music_on_park(5), new);
        sinks.pulse.lock().unwrap().put(own, new);
        eventually("the events' handling", || {
            sinks.pulse.lock().unwrap().queue.is_empty()
        })
        .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(sinks.calls(), Vec::<String>::new());
        drop(router);
    }

    #[tokio::test]
    async fn park_event_listing_at_park_start_moves_a_running_sound() {
        let sinks = FakeSinks::new(false);
        // A call and a song already follow the default to the park sink.
        let call = input(5, PARK_INDEX, &[("media.role", "phone")]);
        sinks.pulse.lock().unwrap().put(call, 0);
        sinks.pulse.lock().unwrap().put(music_on_park(6), 0);
        let router = parked_router(&sinks).await;
        eventually("the move and reset", || sinks.calls().len() >= 4).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(sinks.calls(), moved_and_reset("media.role=phone"));
        drop(router);
    }

    /// Wait up to 3 s for the watch on `pulse` to end: its `FakePulseIo`,
    /// made while `watching`, dropped.
    fn wait_for_end(pulse: &Arc<Mutex<FakePulse>>) {
        let waiting = std::time::Instant::now();
        while pulse.lock().unwrap().watching {
            assert!(waiting.elapsed() < Duration::from_secs(3), "still watching");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn park_event_watch_stopped_before_its_listing_moves_nothing() {
        let pulse = Arc::<Mutex<FakePulse>>::default();
        pulse.lock().unwrap().put(event_on(5, PARK_INDEX), 0);
        pulse.lock().unwrap().watching = true;
        let (connect, connected) = std::sync::mpsc::channel::<()>();
        let io = FakePulseIo(pulse.clone());
        let watch = ParkWatch::spawn(
            move || {
                let _ = connected.recv();
                Ok(io)
            },
            DAC.to_string(),
        );
        // Unparked while it connected.
        drop(watch);
        connect.send(()).unwrap();
        wait_for_end(&pulse);
        assert_eq!(pulse.lock().unwrap().calls, Vec::<String>::new());
    }

    /// A watch stopped during a move or a reset still resets the stream it
    /// moved (#311 r3), but moves nothing more: the listing's next stream
    /// stays.
    #[test]
    fn park_event_watch_stopped_during_a_reset_moves_nothing_more() {
        for at in ["move", "open"] {
            let pulse = Arc::<Mutex<FakePulse>>::default();
            let (release, gate) = std::sync::mpsc::channel();
            {
                let mut pulse = pulse.lock().unwrap();
                pulse.put(event_on(5, PARK_INDEX), 0);
                pulse.put(event_on(6, PARK_INDEX), 0);
                pulse.gate = Some((at, gate));
                pulse.watching = true;
            }
            let io = FakePulseIo(pulse.clone());
            let watch = ParkWatch::spawn(move || Ok(io), DAC.to_string());
            let waiting = std::time::Instant::now();
            while pulse.lock().unwrap().gate.is_some() {
                assert!(waiting.elapsed() < Duration::from_secs(3), "no {at}");
                std::thread::sleep(Duration::from_millis(5));
            }
            drop(watch);
            drop(release);
            wait_for_end(&pulse);
            let calls = pulse.lock().unwrap().calls.clone();
            assert_eq!(calls, moved_and_reset("media.role=event"), "{at}");
        }
    }

    #[tokio::test]
    async fn park_event_after_unpark_moves_nothing() {
        let sinks = FakeSinks::new(false);
        let router = parked_router(&sinks).await;
        router.route(Unpark(ToLocal));
        settles(router.settled()).await;
        eventually("the event path's end", || !sinks.watching()).await;
        {
            // Delivered as if still subscribed.
            let mut pulse = sinks.pulse.lock().unwrap();
            pulse.inputs.insert(5, event_on(5, PARK_INDEX));
            pulse.queue.push((sys::PA_SUBSCRIPTION_EVENT_NEW, 5));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(sinks.calls(), Vec::<String>::new());
        assert_eq!(sinks.sets(), ["load", PARK_SINK, DAC, "unload 7"]);
    }
}
