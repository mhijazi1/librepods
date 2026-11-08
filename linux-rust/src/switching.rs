//! The app's switching policy (issue #9): who answers AirPods ownership for
//! a connected `AirPodsDevice`, selected by the `switching` preference.
//!
//! - `Handoff` (default) runs the pure #841 core (`handoff.rs`) in one task
//!   per AirPods MAC. The task only feeds the core (the session snapshot, the
//!   AACP events, playback samples, local call changes and ticks) and
//!   executes the commands it returns; every decision is the core's
//!   (DECISIONS #8).
//! - `Legacy` is the eager playback takeover the app used before: the
//!   playback listener and the three ownership reactions, moved here from
//!   devices/airpods.rs without changes.
//! - `Off` sends no ownership, hijack or announcement packet and never pauses
//!   or drops audio on ownership events.
//!
//! The app lifecycle (main.rs, BlueZ, le.rs auto-connect) owns reconnection:
//! when the control session dies the handoff task ends, and the replacement
//! `AirPodsDevice` starts a new one.
//!
//! `Switching::dispatch_events` is the device's AACP event dispatch: a
//! router that never awaits anything but the next event hands each event to
//! the switching mode first, then to the device's handlers (ear detection,
//! conversational awareness, stem press, tray, UI) in their own task. A slow
//! handler therefore never holds up ownership answers, and on session loss
//! the handoff task can still drain every event the manager emitted.

use crate::a2dp_resume;
use crate::bluetooth::aacp::{
    AACPEvent, AACPManager, AACPManagerState, ControlCommandIdentifiers,
    ControlCommandIdentifiers as Control,
};
use crate::default_sink::{DefaultSinkRouter, SinkEvent};
use crate::handoff::{Command, Handoff, Input, SessionFacts};
use crate::handoff_log;
use crate::media_controller::{self, MediaController};
use crate::stream_monitor::{
    self, CallSamples, MIN_STREAM_PLAYBACK, PeakSource, PlaybackSource, PulsePeaks,
    PulsePlaybackStreams,
};
use crate::utils::{self, ConnectMode, SwitchingMode};
use futures::future::BoxFuture;
use log::{error, info};
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::AbortHandle;

/// How often the handoff task ticks the core (ACK timeouts) and checks that
/// the control session is still alive.
pub(crate) const TICK: Duration = Duration::from_millis(200);
/// Interval between playback polls in production.
pub(crate) const PLAYBACK_POLL: Duration = Duration::from_millis(500);

type ControlTx = mpsc::UnboundedSender<(ControlCommandIdentifiers, Vec<u8>)>;
type Events = mpsc::UnboundedReceiver<AACPEvent>;
type EventTx = mpsc::UnboundedSender<AACPEvent>;
/// A request to the event router to forward everything still in the primary
/// queue; answered once it has.
type FlushRequest = oneshot::Sender<()>;
/// Playback samples: whether any local MPRIS player was playing, or a
/// takeover-worthy audio stream had been playing for the minimum duration,
/// at the instant.
pub(crate) struct PlaybackSamples {
    rx: mpsc::Receiver<(Instant, bool)>,
    /// Asks the stream monitor to re-baseline at the core's reset (#90).
    rebaseline: Arc<StdMutex<Option<Instant>>>,
}

impl PlaybackSamples {
    pub(crate) async fn recv(&mut self) -> Option<(Instant, bool)> {
        self.rx.recv().await
    }

    /// The core's step at `reset` asked for `Command::DrainPlayback`:
    /// re-baseline the stream monitor at `reset` before it observes its next
    /// listing. Anchored to `reset`, not to now, so a stream first audible
    /// after the reset still counts however late this is applied.
    #[cfg(test)]
    pub(crate) fn rebaseline(&self, reset: Instant) {
        *self.rebaseline.lock().unwrap_or_else(|e| e.into_inner()) = Some(reset);
    }

    /// Run the core's step at now, `core_step(now)`, under the re-baseline
    /// lock, and when it asks for `DrainPlayback` request the stream
    /// monitor's re-baseline at that now before the lock is released, so
    /// before any command is awaited (#90). The stream worker holds
    /// the same lock from its clock read to its publish, so each of its
    /// observes falls wholly before now, published before it (a poller
    /// sample stamped at or after now reads it), or wholly after the
    /// request.
    pub(crate) fn handle(
        &self,
        core_step: impl FnOnce(Instant) -> Vec<Command>,
    ) -> (Instant, Vec<Command>) {
        let mut request = self.rebaseline.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let commands = core_step(now);
        if commands.contains(&Command::DrainPlayback) {
            *request = Some(now);
        }
        (now, commands)
    }

    /// `Command::DrainPlayback`: drop the samples already taken.
    pub(crate) fn drain(&mut self) {
        while self.rx.try_recv().is_ok() {}
    }
}

/// The media side effects the handoff core can ask for, injected so tests
/// need no D-Bus or PipeWire.
pub(crate) trait MediaActions: Send + Sync {
    /// Pause local players.
    fn pause(&self) -> BoxFuture<'static, ()>;
    /// Activate the AirPods' A2DP profile.
    fn activate_a2dp(&self) -> BoxFuture<'static, ()>;
    /// Run the A2DP resume chain (#200) until it ends, or until `cancel`
    /// fires or its sender is dropped (checked between steps). It starts no
    /// kick or profile cycle whose Play can't be due before `until`.
    fn resume_a2dp(&self, until: Instant, cancel: oneshot::Receiver<()>) -> BoxFuture<'static, ()>;
    /// Enqueue a default sink route (#40); never waits for the sound server.
    fn route_default_sink(&self, event: SinkEvent);
}

/// Blocking: pauses the local players and reports what it did.
type PausePlayers = Arc<dyn Fn() -> media_controller::PauseOutcome + Send + Sync>;

/// Production media actions on a clone of the device's `MediaController`
/// (its state is shared, DECISIONS #5). The device handlers lock the outer
/// `Arc<Mutex<MediaController>>` and ear detection holds it across an A2DP
/// activation (up to ~43 s, #32), so this adapter never takes that mutex: it
/// is built from the controller before the controller is put behind it
/// (`device_media`), and has no field that could reach it.
pub(crate) struct ControllerMedia {
    controller: MediaController,
    pause_players: PausePlayers,
    /// Its worker holds another clone of `controller`.
    sinks: DefaultSinkRouter,
}

impl MediaActions for ControllerMedia {
    fn pause(&self) -> BoxFuture<'static, ()> {
        let controller = self.controller.clone();
        let pause_players = self.pause_players.clone();
        Box::pin(async move {
            controller
                .pause_all_media_with(move || pause_players())
                .await;
        })
    }

    fn activate_a2dp(&self) -> BoxFuture<'static, ()> {
        let controller = self.controller.clone();
        Box::pin(async move {
            controller.activate_a2dp_profile().await;
        })
    }

    fn resume_a2dp(&self, until: Instant, cancel: oneshot::Receiver<()>) -> BoxFuture<'static, ()> {
        Box::pin(a2dp_resume::run(self.controller.clone(), until, cancel))
    }

    fn route_default_sink(&self, event: SinkEvent) {
        self.sinks.route(event);
    }
}

/// A device's `MediaController`, behind the outer mutex its handlers lock,
/// and the handoff media adapter, used by `AirPodsDevice::new`.
pub(crate) fn device_media(
    airpods_mac: String,
    local_mac: String,
) -> (Arc<Mutex<MediaController>>, Arc<dyn MediaActions>) {
    share_media(
        MediaController::new(airpods_mac, local_mac),
        Arc::new(MediaController::pause_playing_mpris_players),
    )
}

/// The adapter takes its clone before the controller goes behind the outer
/// mutex, so no handler can hold that mutex at that point.
fn share_media(
    controller: MediaController,
    pause_players: PausePlayers,
) -> (Arc<Mutex<MediaController>>, Arc<dyn MediaActions>) {
    let media = ControllerMedia {
        controller: controller.clone(),
        pause_players,
        sinks: DefaultSinkRouter::spawn(controller.clone()),
    };
    (Arc::new(Mutex::new(controller)), Arc::new(media))
}

/// Poll local playback every `PLAYBACK_POLL`: playing when an MPRIS player
/// plays or a takeover-worthy audio stream has carried sound for
/// `MIN_STREAM_PLAYBACK` (#10, #88). The poller stops once the receiver is
/// dropped.
pub(crate) fn spawn_playback_poller() -> PlaybackSamples {
    spawn_playback_poller_with(
        MediaController::check_if_playing,
        PulsePlaybackStreams::new(),
        PulsePeaks::new(),
        MIN_STREAM_PLAYBACK,
        PLAYBACK_POLL,
        |_| (),
    )
}

/// `spawn_playback_poller` with its sources, minimum stream duration and
/// interval injected, and the stream monitor's `observed` hook. Every
/// `interval` it samples `mpris` on the blocking pool and sends
/// `(sampled_at, mpris || streams)`, where `streams` is the latest state of
/// the stream monitor over `streams` and `peaks`. The stream monitor
/// samples in its own task (`stream_monitor::spawn_stream_playback`), so a
/// sound server that never answers leaves the stream state as it was but
/// never holds up the MPRIS samples. Only edges reach the handoff core
/// (`Input::Playback`), which keeps every takeover decision;
/// `PlaybackSamples::handle` re-baselines the stream monitor when the core
/// resets its baseline (#90).
fn spawn_playback_poller_with<M, S, P>(
    mpris: M,
    streams: S,
    peaks: P,
    min_stream: Duration,
    interval: Duration,
    observed: impl Fn(Instant) + Send + Sync + 'static,
) -> PlaybackSamples
where
    M: Fn() -> bool + Send + Sync + 'static,
    S: PlaybackSource + 'static,
    P: PeakSource + 'static,
{
    let (play_tx, rx) = mpsc::channel(1);
    let rebaseline = Arc::new(StdMutex::new(None));
    let streams = stream_monitor::spawn_stream_playback(
        streams,
        peaks,
        min_stream,
        interval,
        rebaseline.clone(),
        observed,
    );
    let mpris = Arc::new(mpris);
    tokio::spawn(async move {
        loop {
            // Stamped before the stream state is read: a sample
            // stamped at or after a step's reset reads every state the
            // stream monitor published before that reset.
            let sampled_at = Instant::now();
            let mpris = mpris.clone();
            let mpris_playing = tokio::task::spawn_blocking(move || mpris())
                .await
                .unwrap_or(false);
            let playing = mpris_playing || *streams.borrow();
            if play_tx.send((sampled_at, playing)).await.is_err() {
                break;
            }
            tokio::time::sleep(interval).await;
        }
    });
    PlaybackSamples { rx, rebaseline }
}

/// Copy a session's routing, wear and ownership state for the core. The
/// caller holds the guard only for the copy.
pub(crate) fn session_facts(state: &AACPManagerState) -> SessionFacts {
    SessionFacts {
        hosts: state.connected_devices.clone(),
        ears: state.ear_detection_status.clone(),
        audio: state.audio_source.clone(),
        owner: state
            .control_command_status_list
            .iter()
            .find(|s| s.identifier == Control::OwnsConnection)
            .map(|s| s.value.clone()),
    }
}

/// The core inputs that replay a session snapshot, for reports the AirPods
/// sent before the shell saw the event stream. Decision-free: the hosts are
/// reported as all new, and the core announces nothing while not automatic.
pub(crate) fn seed_inputs(facts: SessionFacts) -> Vec<Input> {
    let mut inputs = vec![
        Input::ConnectedDevices {
            old: Vec::new(),
            new: facts.hosts,
        },
        Input::EarState(facts.ears),
    ];
    if let Some(audio) = facts.audio {
        inputs.push(Input::AudioSource(audio));
    }
    if let Some(owner) = facts.owner {
        inputs.push(Input::Ownership(owner));
    }
    inputs
}

/// The core input for an AACP event, if the core takes it.
pub(crate) fn event_input(event: AACPEvent) -> Option<Input> {
    match event {
        AACPEvent::ControlCommand(s) if s.identifier == Control::OwnsConnection => {
            Some(Input::Ownership(s.value))
        }
        AACPEvent::ConnectedDevices(old, new) => Some(Input::ConnectedDevices { old, new }),
        AACPEvent::EarDetection(_, new) => Some(Input::EarState(new)),
        AACPEvent::AudioSource(source) => Some(Input::AudioSource(source)),
        AACPEvent::OwnershipToFalseRequest => Some(Input::RemoteOwnershipRequest),
        _ => None,
    }
}

/// Announce Linux to every connected remote host (the core's `Announce`).
pub(crate) async fn announce(
    manager: &AACPManager,
    local: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let peers = manager.state.lock().await.connected_devices.clone();
    for peer in peers.iter().filter(|d| d.mac != local) {
        manager
            .send_media_information_new_device(local, &peer.mac)
            .await?;
        manager.send_add_tipi_device(local, &peer.mac).await?;
    }
    Ok(())
}

/// Tell every connected remote host whether Linux is in a call (the core's
/// `AdvertiseCall`): the in-call media information, or the idle one the
/// announcement sends once the call ends.
pub(crate) async fn advertise_call(
    manager: &AACPManager,
    local: &str,
    in_call: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let peers = manager.state.lock().await.connected_devices.clone();
    for peer in peers.iter().filter(|d| d.mac != local) {
        if in_call {
            manager
                .send_media_information_call(local, &peer.mac)
                .await?;
        } else {
            manager
                .send_media_information_new_device(local, &peer.mac)
                .await?;
        }
    }
    Ok(())
}

/// Whether the AACP receive loop still has the control session. The guard is
/// dropped before this returns.
async fn session_alive(manager: &AACPManager) -> bool {
    manager.state.lock().await.sender.is_some()
}

struct HandoffTask {
    id: u64,
    handle: AbortHandle,
}

// One handoff task per AirPods MAC. A replacement AirPodsDevice for the same
// MAC aborts its predecessor, like the legacy PLAYBACK_LISTENERS.
static HANDOFF_TASKS: LazyLock<StdMutex<HashMap<String, HandoffTask>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));
static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(0);

// Whether Linux was the last owner of each AirPods MAC (#16). Outlives the
// handoff task, which a reconnect replaces; forgotten when the app quits.
static LAST_OWNER_LINUX: LazyLock<StdMutex<HashMap<String, bool>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

fn last_owner_linux(mac: &str) -> bool {
    LAST_OWNER_LINUX.lock().unwrap().get(mac) == Some(&true)
}

fn remember_last_owner_linux(mac: &str, value: bool) {
    LAST_OWNER_LINUX
        .lock()
        .unwrap()
        .insert(mac.to_string(), value);
}

/// Stop the handoff task of this AirPods MAC, if one runs.
pub(crate) fn stop_handoff_task(mac: &str) {
    if let Some(task) = HANDOFF_TASKS.lock().unwrap().remove(mac) {
        task.handle.abort();
    }
}

/// The running handoff task of this AirPods MAC.
#[cfg(test)]
pub(crate) fn handoff_task(mac: &str) -> Option<AbortHandle> {
    HANDOFF_TASKS
        .lock()
        .unwrap()
        .get(mac)
        .map(|task| task.handle.clone())
}

/// Removes a finished task's registry entry, unless a replacement already
/// took the MAC. Runs on return, abort and panic alike.
struct Unregister {
    mac: String,
    id: u64,
}

impl Drop for Unregister {
    fn drop(&mut self) {
        let mut tasks = HANDOFF_TASKS.lock().unwrap_or_else(|e| e.into_inner());
        if tasks.get(&self.mac).is_some_and(|task| task.id == self.id) {
            tasks.remove(&self.mac);
        }
    }
}

fn spawn_handoff_task(mac: String, shell: Shell) {
    let id = NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed);
    // Held across the spawn, so the new task cannot unregister before it is
    // registered.
    let mut tasks = HANDOFF_TASKS.lock().unwrap();
    if let Some(previous) = tasks.remove(&mac) {
        info!("Stopping the previous handoff task for {mac}");
        previous.handle.abort();
    }
    let unregister = Unregister {
        mac: mac.clone(),
        id,
    };
    let task = tokio::spawn(async move {
        let _unregister = unregister;
        run_handoff(shell).await;
    });
    tasks.insert(
        mac,
        HandoffTask {
            id,
            handle: task.abort_handle(),
        },
    );
}

/// The handoff task's I/O: executes the core's commands.
struct Shell {
    mac: String,
    local: String,
    manager: AACPManager,
    media: Arc<dyn MediaActions>,
    /// The switching side of the event router's fan-out.
    events: Events,
    /// Asks the router to forward what is left in the primary queue.
    flush: mpsc::UnboundedSender<FlushRequest>,
    play_rx: PlaybackSamples,
    /// Local call state changes (#11).
    call_rx: CallSamples,
    /// A2DP transport state changes (#200).
    a2dp_rx: mpsc::Receiver<bool>,
    /// Stops the running A2DP resume chain when dropped.
    resume: Option<oneshot::Sender<()>>,
    /// Where the core's lines go after they are logged, if anywhere.
    lines: Option<LineSink>,
    /// The device's "Connect to this computer" (#16).
    connect_mode: ConnectMode,
}

/// Receives each line the handoff core emits, in order, right after it is
/// logged and before the step's commands run.
pub(crate) type LineSink = Arc<dyn Fn(&str) + Send + Sync>;

impl Shell {
    /// Wait until the router has forwarded every event still in the primary
    /// AACP queue to `events`. Returns at once if the router has ended (the
    /// primary queue closed after it forwarded everything).
    async fn flush(&self) {
        let (done, flushed) = oneshot::channel();
        if self.flush.send(done).is_ok() {
            let _ = flushed.await;
        }
    }

    /// Execute the core's commands in order. True when the task should end.
    /// No AACP or MediaController guard is held here; the sends take their
    /// own, and the media actions never take the outer MediaController mutex
    /// the device handlers hold (`ControllerMedia`).
    async fn execute(&mut self, now: Instant, commands: Vec<Command>) -> bool {
        let mut end = false;
        for command in commands {
            match command {
                Command::SendOwnership(owns) => {
                    if !owns {
                        self.resume = None;
                    }
                    if let Err(e) = self
                        .manager
                        .send_control_command(Control::OwnsConnection, &[u8::from(owns)])
                        .await
                    {
                        error!("Handoff: failed to send ownership {owns}: {e}");
                    }
                }
                Command::Announce => {
                    if let Err(e) = announce(&self.manager, &self.local).await {
                        error!("Handoff: failed to announce Linux to remote hosts: {e}");
                    }
                }
                Command::AdvertiseCall(in_call) => {
                    if let Err(e) = advertise_call(&self.manager, &self.local, in_call).await {
                        error!("Handoff: failed to advertise call state {in_call}: {e}");
                    }
                }
                Command::PauseMedia => {
                    // Before the pause, so the chain plays nothing after it.
                    self.resume = None;
                    self.media.pause().await
                }
                // Activation can take tens of seconds; it must not stall
                // ownership answers and events behind it.
                Command::ConnectA2dp => {
                    tokio::spawn(self.media.activate_a2dp());
                }
                // Seconds of sleeps; replacing the sender stops a running one.
                Command::ResumeA2dp => {
                    let (cancel, cancelled) = oneshot::channel();
                    self.resume = Some(cancel);
                    // The core opened its hold window at `now`.
                    let until = now + a2dp_resume::A2DP_RESUME_WINDOW;
                    tokio::spawn(self.media.resume_a2dp(until, cancelled));
                }
                Command::RouteToAirPods => self.media.route_default_sink(SinkEvent::Take),
                Command::RouteAway => self.media.route_default_sink(SinkEvent::Yield),
                Command::DropSession => {
                    self.resume = None;
                    info!(
                        "Handoff: control session of {} ended; the app reconnects it",
                        self.mac
                    );
                    end = true;
                }
                // Reconnection belongs to the app lifecycle (main.rs, BlueZ,
                // le.rs auto-connect), which starts a new device and task.
                Command::Reconnect { audio } => {
                    info!("Handoff: reconnect (audio={audio}) left to the app lifecycle")
                }
                Command::AdoptSession => info!("Handoff: no reconnect session to adopt"),
                Command::CancelReconnect => info!("Handoff: no reconnect to cancel"),
                Command::DrainPlayback => self.play_rx.drain(),
                Command::Exit => return true,
                Command::Fail(reason) => {
                    error!("Handoff: {reason}");
                    return true;
                }
            }
        }
        end
    }
}

/// Feed one input to the core, log its lines and execute its commands. True
/// when the task should end.
async fn step(core: &mut Handoff, shell: &mut Shell, input: Input) -> bool {
    let (now, commands) = shell.play_rx.handle(|now| core.handle(now, input));
    remember_last_owner_linux(&shell.mac, core.last_owner_linux());
    for line in core.take_lines() {
        info!("Handoff {}: {line}", shell.mac);
        if let Some(sink) = &shell.lines {
            sink(&line);
        }
    }
    shell.execute(now, commands).await
}

async fn on_event(core: &mut Handoff, shell: &mut Shell, event: AACPEvent) -> bool {
    match event_input(event) {
        Some(input) => step(core, shell, input).await,
        None => false,
    }
}

/// The AACP receive loop clears a dead session without an event. Mark it
/// lost before stepping a dequeued event (DECISIONS #8).
async fn mark_session_lost(core: &mut Handoff, shell: &mut Shell) -> bool {
    if core.live() && !session_alive(&shell.manager).await {
        return step(core, shell, Input::SessionLost).await;
    }
    false
}

/// Once the session is gone: SessionLost, every event the manager emitted
/// before it cleared the session, then Disconnected (which ends the task).
async fn sync_session(core: &mut Handoff, shell: &mut Shell) -> bool {
    if !core.connected() || session_alive(&shell.manager).await {
        return false;
    }
    if step(core, shell, Input::SessionLost).await {
        return true;
    }
    // The manager emits under the state lock that cleared the sender, so
    // those events are all in the primary queue or already forwarded here.
    shell.flush().await;
    while let Ok(event) = shell.events.try_recv() {
        if on_event(core, shell, event).await {
            return true;
        }
    }
    step(core, shell, Input::Disconnected).await
}

/// The event stream closed: no event can reach the core any more, so it
/// stops through SessionLost and Disconnected whether or not the session is
/// still alive.
async fn end_stream(core: &mut Handoff, shell: &mut Shell) {
    if core.live() && step(core, shell, Input::SessionLost).await {
        return;
    }
    if core.connected() {
        step(core, shell, Input::Disconnected).await;
    }
}

async fn run_handoff(mut shell: Shell) {
    // The event channel is set after connect and handshake, so the earlier
    // reports exist only in the state. The manager emits under the state
    // lock it updates, and the router never takes that lock: flushing the
    // router under the snapshot's guard splits the queue exactly into the
    // events already in the snapshot and the later ones (#49).
    let (facts, backlog) = {
        let state = shell.manager.state.lock().await;
        if state.sender.is_none() {
            info!(
                "Handoff: no control session for {}; not starting",
                shell.mac
            );
            return;
        }
        shell.flush().await;
        let mut backlog = Vec::new();
        while let Ok(event) = shell.events.try_recv() {
            backlog.push(event);
        }
        (session_facts(&state), backlog)
    };
    let mut core = Handoff::new(shell.local.clone(), Instant::now(), false, true)
        .with_connect_mode(shell.connect_mode)
        .with_last_owner_linux(last_owner_linux(&shell.mac))
        .with_default_sink_routing(true);
    for input in seed_inputs(facts) {
        if step(&mut core, &mut shell, input).await {
            return;
        }
    }
    // Replayed before Auto, so a stale [0] in it cannot pass for the reply
    // to Auto's "not owning" (#54).
    for event in backlog {
        if mark_session_lost(&mut core, &mut shell).await
            || on_event(&mut core, &mut shell, event).await
        {
            return;
        }
    }
    // The core's own automatic startup: A2DP, "not owning", announce.
    if step(&mut core, &mut shell, Input::Auto).await {
        return;
    }
    if step(&mut core, &mut shell, Input::InitBacklogDrained).await {
        return;
    }
    let mut tick = tokio::time::interval(TICK);
    loop {
        let end = tokio::select! {
            biased;
            event = shell.events.recv() => match event {
                // Check the session before stepping the event, but keep the
                // rest of the queue for the next sync.
                Some(event) => {
                    mark_session_lost(&mut core, &mut shell).await
                        || on_event(&mut core, &mut shell, event).await
                }
                None => {
                    info!("Handoff: event stream of {} closed", shell.mac);
                    end_stream(&mut core, &mut shell).await;
                    true
                }
            },
            Some((sampled_at, playing)) = shell.play_rx.recv() => {
                sync_session(&mut core, &mut shell).await
                    || step(&mut core, &mut shell, Input::Playback { sampled_at, playing }).await
            }
            Some((_, in_call)) = shell.call_rx.recv() => {
                sync_session(&mut core, &mut shell).await
                    || step(&mut core, &mut shell, Input::LocalCall(in_call)).await
            }
            Some(active) = shell.a2dp_rx.recv() => {
                sync_session(&mut core, &mut shell).await
                    || step(&mut core, &mut shell, Input::A2dpTransport { active }).await
            }
            _ = tick.tick() => {
                sync_session(&mut core, &mut shell).await
                    || step(&mut core, &mut shell, Input::Tick).await
            }
        };
        if end {
            break;
        }
    }
    info!("Handoff task for {} ended", shell.mac);
}

/// What a device's switching needs from it.
pub(crate) struct SwitchingSetup {
    pub(crate) mode: SwitchingMode,
    pub(crate) airpods_mac: String,
    pub(crate) local_mac: String,
    pub(crate) manager: AACPManager,
    pub(crate) media_controller: Arc<Mutex<MediaController>>,
    pub(crate) command_tx: ControlTx,
}

/// One device's switching policy. `AirPodsDevice` starts it once, then hands
/// it the device's AACP event stream with `dispatch_events`.
pub(crate) struct Switching {
    airpods_mac: String,
    policy: Policy,
    /// Given to the handoff task's `Shell`; `start_with_calls` sets none.
    lines: Option<LineSink>,
    /// Given to the handoff core; `start_with_calls` keeps the default.
    connect_mode: ConnectMode,
    /// The A2DP transport poller (#200) for the handoff task; `start` sets
    /// it in the handoff mode, `start_with_calls` sets none.
    a2dp: Option<mpsc::Receiver<bool>>,
}

enum Policy {
    /// The handoff task's inputs; it starts with the event stream.
    Handoff {
        local: String,
        manager: AACPManager,
        media: Arc<dyn MediaActions>,
        play_rx: PlaybackSamples,
        call_rx: CallSamples,
    },
    Legacy(Legacy),
    Off,
}

/// What the legacy ConnectedDevices / OwnershipToFalseRequest reactions use.
struct Legacy {
    local_mac: String,
    manager: AACPManager,
    media_controller: Arc<Mutex<MediaController>>,
    command_tx: ControlTx,
}

impl Switching {
    /// Start the selected mode. `media` and `playback` are used only by the
    /// handoff mode; `playback` is called only in that mode. The handoff
    /// mode monitors local calls with `stream_monitor::spawn_call_poller`
    /// and appends the core's lines to the handoff log (#21), which is
    /// present unless no state directory is known (neither XDG_STATE_HOME
    /// nor HOME set) or its writer thread failed to start. A failed open or
    /// write warns once, then stops logging.
    pub(crate) async fn start(
        setup: SwitchingSetup,
        media: Arc<dyn MediaActions>,
        playback: impl FnOnce() -> PlaybackSamples,
    ) -> Self {
        let handoff = setup.mode == SwitchingMode::Handoff;
        let log = handoff.then(handoff_log::app_log).flatten();
        let mac = setup.airpods_mac.clone();
        // shortcut: connect mode is read once at start; a live update is tracked in (#153)
        let mut switching =
            Self::start_with_calls(setup, media, playback, stream_monitor::spawn_call_poller)
                .await
                .with_connect_mode(utils::load_connect_mode(&mac));
        switching.a2dp = handoff.then(|| a2dp_resume::spawn_transport_poller(&mac));
        match log {
            Some(log) => switching.with_line_sink(log.sink(mac)),
            None => switching,
        }
    }

    /// `start` with the local call monitor injected: `calls` gets the AirPods
    /// MAC and is called only in the handoff mode.
    pub(crate) async fn start_with_calls(
        setup: SwitchingSetup,
        media: Arc<dyn MediaActions>,
        playback: impl FnOnce() -> PlaybackSamples,
        calls: impl FnOnce(&str) -> CallSamples,
    ) -> Self {
        let SwitchingSetup {
            mode,
            airpods_mac,
            local_mac,
            manager,
            media_controller,
            command_tx,
        } = setup;
        info!("Switching mode for {airpods_mac}: {mode:?}");
        let policy = match mode {
            SwitchingMode::Handoff => {
                media_controller::stop_playback_listener(&airpods_mac);
                Policy::Handoff {
                    local: local_mac,
                    manager,
                    media,
                    play_rx: playback(),
                    call_rx: calls(&airpods_mac),
                }
            }
            SwitchingMode::Legacy => {
                stop_handoff_task(&airpods_mac);
                Self::start_legacy(&manager, &media_controller, &command_tx).await;
                Policy::Legacy(Legacy {
                    local_mac,
                    manager,
                    media_controller,
                    command_tx,
                })
            }
            SwitchingMode::Off => {
                stop_handoff_task(&airpods_mac);
                media_controller::stop_playback_listener(&airpods_mac);
                Policy::Off
            }
        };
        Switching {
            airpods_mac,
            policy,
            lines: None,
            connect_mode: ConnectMode::default(),
            a2dp: None,
        }
    }

    /// Run the handoff core in `mode` ("Connect to this computer", #16).
    pub(crate) fn with_connect_mode(mut self, mode: ConnectMode) -> Self {
        self.connect_mode = mode;
        self
    }

    /// Also hand every line of the handoff core to `sink`, after it is
    /// logged. Only the handoff mode emits lines.
    pub(crate) fn with_line_sink(mut self, sink: LineSink) -> Self {
        self.lines = Some(sink);
        self
    }

    /// The device's AACP event dispatch, used by `AirPodsDevice::new`.
    /// `events` is the receiver of the channel given to
    /// `AACPManager::set_event_channel`. A router task hands every event to
    /// the switching mode first (the handoff task's queue, or the legacy
    /// reactions' task), then to `handle`: the device's ear detection,
    /// conversational awareness, stem press, tray and UI handling, which runs
    /// in its own task and sees every event once, in order. The router only
    /// sends on unbounded channels, so a slow `handle` (an A2DP activation
    /// behind the MediaController lock) never holds up the switching mode,
    /// and when the handoff task finds the session dead it can have the
    /// router forward whatever the manager emitted before (DECISIONS #8).
    pub(crate) fn dispatch_events<H, F>(self, events: Events, mut handle: H)
    where
        H: FnMut(AACPEvent) -> F + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        let (handlers, mut handler_events) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(event) = handler_events.recv().await {
                handle(event).await;
            }
        });
        let (switching, flush) = match self.policy {
            Policy::Handoff {
                local,
                manager,
                media,
                play_rx,
                call_rx,
            } => {
                let (events_tx, handoff_events) = mpsc::unbounded_channel();
                let (flush_tx, flush_rx) = mpsc::unbounded_channel();
                let shell = Shell {
                    mac: self.airpods_mac.clone(),
                    local,
                    manager,
                    media,
                    events: handoff_events,
                    flush: flush_tx,
                    play_rx,
                    call_rx,
                    // No poller: a closed channel, so no take resumes A2DP.
                    a2dp_rx: self.a2dp.unwrap_or_else(|| mpsc::channel(1).1),
                    resume: None,
                    lines: self.lines,
                    connect_mode: self.connect_mode,
                };
                spawn_handoff_task(self.airpods_mac, shell);
                (Some(events_tx), Some(flush_rx))
            }
            Policy::Legacy(legacy) => {
                let (events_tx, mut legacy_events) = mpsc::unbounded_channel();
                tokio::spawn(async move {
                    while let Some(event) = legacy_events.recv().await {
                        legacy.legacy_event(&event).await;
                    }
                });
                (Some(events_tx), None)
            }
            Policy::Off => (None, None),
        };
        tokio::spawn(route_events(events, switching, handlers, flush));
    }

    /// The legacy playback listener and OwnsConnection reaction, moved from
    /// AirPodsDevice::new unchanged.
    async fn start_legacy(
        aacp_manager: &AACPManager,
        media_controller: &Arc<Mutex<MediaController>>,
        command_tx: &ControlTx,
    ) {
        let mc_listener = media_controller.lock().await;
        let aacp_manager_clone_listener = aacp_manager.clone();
        mc_listener
            .start_playback_listener(aacp_manager_clone_listener, command_tx.clone())
            .await;
        drop(mc_listener);

        let (owns_connection_tx, mut owns_connection_rx) = tokio::sync::mpsc::unbounded_channel();
        aacp_manager
            .subscribe_to_control_command(
                ControlCommandIdentifiers::OwnsConnection,
                owns_connection_tx,
            )
            .await;
        let mc_clone_owns = media_controller.clone();
        tokio::spawn(async move {
            while let Some(value) = owns_connection_rx.recv().await {
                let owns = value.first().copied().unwrap_or(0) != 0;
                if !owns {
                    info!("Lost ownership, pausing media and disconnecting audio");
                    let controller = mc_clone_owns.lock().await.clone();
                    controller.pause_all_media().await;
                    controller.deactivate_a2dp_profile().await;
                }
            }
        });
    }
}

/// Forward each AACP event to the switching mode, if it takes events, then
/// to the device handlers. The router only sends on unbounded channels, so it
/// is always ready for the next event and for a flush request, which the
/// handoff task makes once it finds the session dead.
async fn route_events(
    mut events: Events,
    switching: Option<EventTx>,
    handlers: EventTx,
    mut flush: Option<mpsc::UnboundedReceiver<FlushRequest>>,
) {
    let forward = |event: AACPEvent| {
        if let Some(switching) = &switching {
            let _ = switching.send(event.clone());
        }
        let _ = handlers.send(event);
    };
    loop {
        tokio::select! {
            biased;
            request = flush_request(&mut flush) => match request {
                Some(done) => {
                    while let Ok(event) = events.try_recv() {
                        forward(event);
                    }
                    let _ = done.send(());
                }
                // The handoff task ended.
                None => flush = None,
            },
            event = events.recv() => match event {
                Some(event) => forward(event),
                None => break,
            },
        }
    }
}

/// The next flush request; never resolves once there can be none.
async fn flush_request(
    flush: &mut Option<mpsc::UnboundedReceiver<FlushRequest>>,
) -> Option<FlushRequest> {
    match flush {
        Some(requests) => requests.recv().await,
        None => std::future::pending().await,
    }
}

impl Legacy {
    /// The legacy reactions, moved from AirPodsDevice::new's event loop
    /// unchanged.
    async fn legacy_event(&self, event: &AACPEvent) {
        let aacp_manager_clone_events = &self.manager;
        let local_mac_events = &self.local_mac;
        let command_tx_clone = &self.command_tx;
        let mc_clone = &self.media_controller;
        match event {
            AACPEvent::ConnectedDevices(old_devices, new_devices) => {
                let local_mac = local_mac_events.clone();
                let new_devices_filtered = new_devices.iter().filter(|new_device| {
                    let not_in_old = old_devices
                        .iter()
                        .all(|old_device| old_device.mac != new_device.mac);
                    let not_local = new_device.mac != local_mac;
                    not_in_old && not_local
                });

                for device in new_devices_filtered {
                    info!(
                        "New connected device: {}, info1: {}, info2: {}",
                        device.mac, device.info1, device.info2
                    );
                    info!(
                        "Sending new Tipi packet for device {}, and sending media info to the device",
                        device.mac
                    );
                    let aacp_manager_clone = aacp_manager_clone_events.clone();
                    let local_mac_clone = local_mac.clone();
                    let device_mac_clone = device.mac.clone();
                    tokio::spawn(async move {
                        if let Err(e) = aacp_manager_clone
                            .send_media_information_new_device(&local_mac_clone, &device_mac_clone)
                            .await
                        {
                            error!("Failed to send media info new device: {}", e);
                        }
                        if let Err(e) = aacp_manager_clone
                            .send_add_tipi_device(&local_mac_clone, &device_mac_clone)
                            .await
                        {
                            error!("Failed to send add tipi device: {}", e);
                        }
                    });
                }
            }
            AACPEvent::OwnershipToFalseRequest => {
                info!(
                    "Received ownership to false request. Setting ownership to false and pausing media."
                );
                let _ =
                    command_tx_clone.send((ControlCommandIdentifiers::OwnsConnection, vec![0x00]));
                let controller = mc_clone.lock().await.clone();
                controller.pause_all_media().await;
                controller.deactivate_a2dp_profile().await;
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bluetooth::aacp::{
        AudioSource, AudioSourceType, ConnectedDevice, ControlCommandStatus, EarDetectionStatus,
        encode_media_information_call,
    };
    use crate::call_detect::{StreamDirection, StreamProps};
    use crate::stream_monitor::AllAudible;
    use std::io;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use tokio::sync::Notify;

    const LOCAL: &str = "01:02:03:04:05:06";
    const REMOTE: &str = "07:08:09:0A:0B:0C";
    const SECOND_REMOTE: &str = "0D:0E:0F:10:11:12";
    /// Long enough for several ticks: anything the task would still send has
    /// been sent by then.
    const SETTLE: Duration = Duration::from_millis(600);

    /// Per AirPods MAC: the core's lines and the media actions the shell
    /// executed, in order.
    static TRACES: LazyLock<StdMutex<HashMap<String, Vec<String>>>> =
        LazyLock::new(|| StdMutex::new(HashMap::new()));

    fn trace(mac: &str, entry: String) {
        TRACES
            .lock()
            .unwrap()
            .entry(mac.to_string())
            .or_default()
            .push(entry);
    }

    fn traced(mac: &str) -> Vec<String> {
        TRACES.lock().unwrap().get(mac).cloned().unwrap_or_default()
    }

    fn position(trace: &[String], prefix: &str) -> usize {
        trace
            .iter()
            .position(|entry| entry.starts_with(prefix))
            .unwrap_or_else(|| panic!("no {prefix:?} in {trace:#?}"))
    }

    #[derive(Default)]
    struct Records {
        pauses: AtomicUsize,
        activations: AtomicUsize,
        /// Not traced, so the trace assertions don't see them.
        routes: StdMutex<Vec<SinkEvent>>,
    }

    /// Records the media actions instead of touching D-Bus or PipeWire. The
    /// record is made when the shell asks, before the future runs.
    struct FakeMedia {
        records: Arc<Records>,
        mac: String,
    }

    impl MediaActions for FakeMedia {
        fn pause(&self) -> BoxFuture<'static, ()> {
            self.records.pauses.fetch_add(1, Ordering::SeqCst);
            trace(&self.mac, "PauseMedia".to_string());
            Box::pin(async {})
        }

        fn activate_a2dp(&self) -> BoxFuture<'static, ()> {
            self.records.activations.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {})
        }

        fn resume_a2dp(
            &self,
            until: Instant,
            cancel: oneshot::Receiver<()>,
        ) -> BoxFuture<'static, ()> {
            let io = FakeResume {
                mac: self.mac.clone(),
            };
            Box::pin(a2dp_resume::run(io, until, cancel))
        }

        fn route_default_sink(&self, event: SinkEvent) {
            self.records.routes.lock().unwrap().push(event);
        }
    }

    /// The one MPRIS player `FakeResume` reports Playing.
    const PLAYER: &str = "org.mpris.MediaPlayer2.fake";

    /// Resume chain I/O for tests: silent throughout, `PLAYER` always
    /// Playing; each action is traced as `Resume <action>`.
    struct FakeResume {
        mac: String,
    }

    /// AirPods MACs whose `FakeResume` audio check takes `HELD_CHECK`, like
    /// the production peak sample, traced as `Audible check` then `Audible
    /// checked`.
    static HELD_CHECKS: LazyLock<StdMutex<Vec<String>>> = LazyLock::new(StdMutex::default);
    const HELD_CHECK: Duration = Duration::from_secs(1);

    impl a2dp_resume::A2dpResumeIo for FakeResume {
        fn act(&self, action: a2dp_resume::Action) -> BoxFuture<'static, ()> {
            trace(&self.mac, format!("Resume {action:?}"));
            Box::pin(async {})
        }

        fn audible(&self) -> BoxFuture<'static, bool> {
            let mac = self.mac.clone();
            Box::pin(async move {
                if HELD_CHECKS.lock().unwrap().contains(&mac) {
                    trace(&mac, "Audible check".to_string());
                    tokio::time::sleep(HELD_CHECK).await;
                    trace(&mac, "Audible checked".to_string());
                }
                false
            })
        }

        fn playing(&self) -> BoxFuture<'static, Vec<String>> {
            Box::pin(async { vec![PLAYER.to_string()] })
        }
    }

    /// Stands in for the device's ear/CA/stem/tray/UI handler. Records each
    /// event it starts and finishes (in Debug form); with `block_ear` it
    /// waits on the first EarDetection until `release`, like ear detection
    /// stuck behind an A2DP activation.
    #[derive(Default)]
    struct DeviceHandler {
        started: StdMutex<Vec<String>>,
        finished: StdMutex<Vec<String>>,
        block_ear: AtomicBool,
        release: Notify,
        /// While blocked, hold this outer MediaController mutex, as the
        /// device's ear detection holds it across `handle_ear_detection`.
        hold: Option<Arc<Mutex<MediaController>>>,
    }

    impl DeviceHandler {
        fn started(&self) -> Vec<String> {
            self.started.lock().unwrap().clone()
        }

        fn finished(&self) -> Vec<String> {
            self.finished.lock().unwrap().clone()
        }

        fn handle(self: Arc<Self>) -> impl FnMut(AACPEvent) -> BoxFuture<'static, ()> + Send {
            move |event| {
                let handler = self.clone();
                Box::pin(async move {
                    let name = format!("{event:?}");
                    handler.started.lock().unwrap().push(name.clone());
                    if matches!(event, AACPEvent::EarDetection(..))
                        && handler.block_ear.swap(false, Ordering::SeqCst)
                    {
                        let _guard = match &handler.hold {
                            Some(outer) => Some(outer.lock().await),
                            None => None,
                        };
                        handler.release.notified().await;
                    }
                    handler.finished.lock().unwrap().push(name);
                })
            }
        }
    }

    fn named(events: &[AACPEvent]) -> Vec<String> {
        events.iter().map(|e| format!("{e:?}")).collect()
    }

    fn host(mac: &str) -> ConnectedDevice {
        ConnectedDevice {
            mac: mac.to_string(),
            info1: 0,
            info2: 0,
            r#type: None,
        }
    }

    fn owns(value: u8) -> AACPEvent {
        AACPEvent::ControlCommand(ControlCommandStatus {
            identifier: Control::OwnsConnection,
            value: vec![value],
        })
    }

    /// Both buds go from in ear to `status`.
    fn wear(status: EarDetectionStatus) -> AACPEvent {
        AACPEvent::EarDetection(vec![EarDetectionStatus::InEar; 2], vec![status; 2])
    }

    /// A manager with an injected send channel (DECISIONS #2).
    async fn injected_manager() -> (AACPManager, mpsc::Receiver<Vec<u8>>, mpsc::Sender<Vec<u8>>) {
        let manager = AACPManager::new();
        let (tx, rx) = mpsc::channel(64);
        manager.state.lock().await.sender = Some(tx.clone());
        (manager, rx, tx)
    }

    /// Hosts [LOCAL, REMOTE], both buds in ear, idle audio.
    async fn ready_session() -> (AACPManager, mpsc::Receiver<Vec<u8>>, mpsc::Sender<Vec<u8>>) {
        let (manager, rx, tx) = injected_manager().await;
        {
            let mut state = manager.state.lock().await;
            state.connected_devices = vec![host(LOCAL), host(REMOTE)];
            state.ear_detection_status = vec![EarDetectionStatus::InEar; 2];
            state.audio_source = Some(AudioSource {
                mac: "00:00:00:00:00:00".to_string(),
                r#type: AudioSourceType::None,
            });
        }
        (manager, rx, tx)
    }

    /// The bytes one `send_*` call puts on the wire, from a reference
    /// injected manager.
    async fn reference<F, Fut>(send: F) -> Vec<u8>
    where
        F: FnOnce(AACPManager) -> Fut,
        Fut: std::future::Future<Output = bluer::Result<()>>,
    {
        let (manager, mut rx, _tx) = injected_manager().await;
        send(manager).await.expect("reference send");
        let packet = rx.try_recv().expect("reference packet");
        assert!(rx.try_recv().is_err());
        packet
    }

    async fn expected_owns(value: u8) -> Vec<u8> {
        reference(|m| async move {
            m.send_control_command(Control::OwnsConnection, &[value])
                .await
        })
        .await
    }

    async fn expected_new_device(remote: &'static str) -> Vec<u8> {
        reference(|m| async move { m.send_media_information_new_device(LOCAL, remote).await }).await
    }

    async fn expected_tipi(remote: &'static str) -> Vec<u8> {
        reference(|m| async move { m.send_add_tipi_device(LOCAL, remote).await }).await
    }

    async fn expected_call(remote: &'static str) -> Vec<u8> {
        reference(|m| async move { m.send_media_information_call(LOCAL, remote).await }).await
    }

    /// Packets the legacy takeover sends and the handoff path must not.
    async fn hijack_packets() -> Vec<Vec<u8>> {
        vec![
            reference(|m| async move { m.send_hijack_request(REMOTE).await }).await,
            reference(|m| async move { m.send_smart_routing_show_ui(REMOTE).await }).await,
            reference(|m| async move { m.send_media_information(LOCAL, REMOTE, true).await }).await,
            reference(|m| async move { m.send_media_information(LOCAL, REMOTE, false).await })
                .await,
        ]
    }

    async fn next_packet(rx: &mut mpsc::Receiver<Vec<u8>>) -> Vec<u8> {
        tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("expected a packet")
            .expect("send channel open")
    }

    /// Wait for the task to settle and return everything else it sent.
    async fn rest(rx: &mut mpsc::Receiver<Vec<u8>>) -> Vec<Vec<u8>> {
        tokio::time::sleep(SETTLE).await;
        let mut packets = Vec::new();
        while let Ok(packet) = rx.try_recv() {
            packets.push(packet);
        }
        packets
    }

    async fn wait_until(what: &str, condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
    }

    /// A device's switching wired as AirPodsDevice::new wires it: started,
    /// then fed through `dispatch_events` from the manager's event channel.
    struct Harness {
        records: Arc<Records>,
        play_tx: mpsc::Sender<(Instant, bool)>,
        playback_started: Arc<AtomicBool>,
        /// The injected call monitor's sender (no sound server in tests).
        call_tx: mpsc::Sender<(Instant, bool)>,
        /// The injected A2DP transport poller's sender (no BlueZ in tests).
        a2dp_tx: mpsc::Sender<bool>,
        /// The sender given to `set_event_channel`.
        events: EventTx,
        handler: Arc<DeviceHandler>,
    }

    impl Harness {
        async fn start(mode: SwitchingMode, mac: &str, manager: &AACPManager) -> Self {
            Self::start_with(mode, mac, manager, false).await
        }

        /// `block_ear`: the device handler blocks on the first EarDetection.
        async fn start_with(
            mode: SwitchingMode,
            mac: &str,
            manager: &AACPManager,
            block_ear: bool,
        ) -> Self {
            let records = Arc::new(Records::default());
            let media = Arc::new(FakeMedia {
                records: records.clone(),
                mac: mac.to_string(),
            });
            let media_controller = Arc::new(Mutex::new(MediaController::new(
                mac.to_string(),
                LOCAL.to_string(),
            )));
            let handler = DeviceHandler::default();
            handler.block_ear.store(block_ear, Ordering::SeqCst);
            Self::start_wired(
                mode,
                mac,
                manager,
                (media_controller, media),
                records,
                Arc::new(handler),
            )
            .await
        }

        /// Start with the given controller and media adapter, and device
        /// handler.
        async fn start_wired(
            mode: SwitchingMode,
            mac: &str,
            manager: &AACPManager,
            media: (Arc<Mutex<MediaController>>, Arc<dyn MediaActions>),
            records: Arc<Records>,
            handler: Arc<DeviceHandler>,
        ) -> Self {
            let (play_tx, play_rx) = mpsc::channel(8);
            Self::start_playback(
                mode,
                mac,
                manager,
                media,
                records,
                handler,
                (play_tx, move || PlaybackSamples {
                    rx: play_rx,
                    rebaseline: Default::default(),
                }),
            )
            .await
        }

        /// Start in handoff mode with the playback samples of the real
        /// playback poller over `mpris` and `streams` (#10). `play` then
        /// reaches no task.
        async fn start_streams<M, S>(
            mac: &str,
            manager: &AACPManager,
            mpris: M,
            streams: S,
            min_stream: Duration,
        ) -> Self
        where
            M: Fn() -> bool + Send + Sync + 'static,
            S: PlaybackSource + 'static,
        {
            let records = Arc::new(Records::default());
            let media = Arc::new(FakeMedia {
                records: records.clone(),
                mac: mac.to_string(),
            });
            let media_controller = Arc::new(Mutex::new(MediaController::new(
                mac.to_string(),
                LOCAL.to_string(),
            )));
            let (play_tx, _) = mpsc::channel(1);
            Self::start_playback(
                SwitchingMode::Handoff,
                mac,
                manager,
                (media_controller, media),
                records,
                Arc::new(DeviceHandler::default()),
                (play_tx, move || {
                    spawn_playback_poller_with(
                        mpris,
                        streams,
                        AllAudible,
                        min_stream,
                        STREAM_TEST_POLL,
                        |_| (),
                    )
                }),
            )
            .await
        }

        /// Start with the given playback samples and the sender `play` uses.
        async fn start_playback(
            mode: SwitchingMode,
            mac: &str,
            manager: &AACPManager,
            (media_controller, media): (Arc<Mutex<MediaController>>, Arc<dyn MediaActions>),
            records: Arc<Records>,
            handler: Arc<DeviceHandler>,
            (play_tx, playback): (
                mpsc::Sender<(Instant, bool)>,
                impl FnOnce() -> PlaybackSamples,
            ),
        ) -> Self {
            let playback_started = Arc::new(AtomicBool::new(false));
            let started = playback_started.clone();
            let (command_tx, mut command_rx) =
                mpsc::unbounded_channel::<(ControlCommandIdentifiers, Vec<u8>)>();
            // Forward control commands to the manager as AirPodsDevice does,
            // so a legacy reaction's OwnsConnection would reach the wire.
            let forward = manager.clone();
            tokio::spawn(async move {
                while let Some((id, value)) = command_rx.recv().await {
                    let _ = forward.send_control_command(id, &value).await;
                }
            });
            let (events, events_rx) = mpsc::unbounded_channel();
            manager.set_event_channel(events.clone()).await;
            let (call_tx, call_rx) = mpsc::channel(8);
            let calls_for = mac.to_string();
            let traced_mac = mac.to_string();
            let sink: LineSink = Arc::new(move |line: &str| trace(&traced_mac, line.to_string()));
            let (a2dp_tx, a2dp_rx) = mpsc::channel(8);
            let mut switching = Switching::start_with_calls(
                SwitchingSetup {
                    mode,
                    airpods_mac: mac.to_string(),
                    local_mac: LOCAL.to_string(),
                    manager: manager.clone(),
                    media_controller,
                    command_tx,
                },
                media,
                move || {
                    started.store(true, Ordering::SeqCst);
                    playback()
                },
                move |airpods_mac: &str| {
                    assert_eq!(airpods_mac, calls_for);
                    call_rx
                },
            )
            .await
            .with_line_sink(sink);
            switching.a2dp = Some(a2dp_rx);
            switching.dispatch_events(events_rx, handler.clone().handle());
            Harness {
                records,
                play_tx,
                playback_started,
                call_tx,
                a2dp_tx,
                events,
                handler,
            }
        }

        /// Emit an event the way the manager does.
        fn emit(&self, event: AACPEvent) {
            self.events.send(event).expect("event channel open");
        }

        /// Drop the harness's sender of the primary event channel.
        fn close_events(&mut self) {
            self.events = mpsc::unbounded_channel().0;
        }

        /// A call state change from the call monitor.
        async fn call(&self, in_call: bool) {
            let _ = self.call_tx.send((Instant::now(), in_call)).await;
        }

        /// An A2DP transport state from the transport poller.
        async fn a2dp(&self, active: bool) {
            let _ = self.a2dp_tx.send(active).await;
        }

        async fn play(&self, playing: bool) {
            // A closed channel (no handoff task) drops the sample.
            let _ = self.play_tx.send((Instant::now(), playing)).await;
        }

        fn pauses(&self) -> usize {
            self.records.pauses.load(Ordering::SeqCst)
        }

        fn activations(&self) -> usize {
            self.records.activations.load(Ordering::SeqCst)
        }

        fn routes(&self) -> Vec<SinkEvent> {
            self.records.routes.lock().unwrap().clone()
        }
    }

    /// Startup: the core's automatic start, [ConnectA2dp, SendOwnership(false),
    /// Announce], and nothing else.
    async fn expect_startup(h: &Harness, rx: &mut mpsc::Receiver<Vec<u8>>) {
        assert_eq!(next_packet(rx).await, expected_owns(0).await);
        assert_eq!(next_packet(rx).await, expected_new_device(REMOTE).await);
        assert_eq!(next_packet(rx).await, expected_tipi(REMOTE).await);
        assert_eq!(rest(rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.activations(), 1);
        assert_eq!(h.pauses(), 0);
    }

    /// The reply to Auto's "not owning" (#54), then playback stopped then
    /// started: one take, OwnsConnection [1] only.
    async fn expect_take(h: &Harness, rx: &mut mpsc::Receiver<Vec<u8>>) {
        h.emit(owns(0));
        h.play(false).await;
        h.play(true).await;
        assert_eq!(next_packet(rx).await, expected_owns(1).await);
        let rest = rest(rx).await;
        assert_eq!(rest, Vec::<Vec<u8>>::new());
    }

    #[tokio::test]
    async fn handoff_default_path_takes_and_releases() {
        let mac = "5A:00:00:00:00:01";
        let (manager, mut rx, _tx) = ready_session().await;
        let h = Harness::start(SwitchingMode::default(), mac, &manager).await;
        assert!(h.playback_started.load(Ordering::SeqCst));
        assert!(handoff_task(mac).is_some());
        assert!(!media_controller::playback_listener_registered(mac));

        expect_startup(&h, &mut rx).await;

        // Take on new playback: OwnsConnection [1], no hijack, show-UI or
        // media information packet. The reply to Auto's "not owning" comes
        // first (#54).
        h.emit(owns(0));
        h.play(false).await;
        h.play(true).await;
        let take = next_packet(&mut rx).await;
        assert_eq!(take, expected_owns(1).await);
        assert!(!hijack_packets().await.contains(&take));
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());

        // ACK, then playback stops: exactly the release.
        h.emit(owns(1));
        let stopped = Instant::now();
        h.play(false).await;
        // shortcut: waits the real RELEASE_HOLD; std::time::Instant ignores a paused clock (#249)
        let hold = crate::handoff::RELEASE_HOLD;
        let release = tokio::time::timeout(hold + Duration::from_secs(3), rx.recv())
            .await
            .expect("expected the release")
            .expect("send channel open");
        assert!(stopped.elapsed() >= hold);
        assert_eq!(release, expected_owns(0).await);
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 0);

        // The release's ACK pauses nothing: playback already stopped
        // (DECISIONS #39; handoff::tests::owns_connection_zero_after_own_release_is_noop).
        h.emit(owns(0));
        wait_until("the release ACK", || {
            traced(mac)
                .iter()
                .any(|l| l.starts_with("ACK owner=not-linux"))
        })
        .await;
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 0);
        assert_eq!(h.activations(), 1);
        // The device handler saw both reports too.
        assert_eq!(h.handler.finished(), named(&[owns(0), owns(1), owns(0)]));

        // New playback takes again; another host then takes the AirPods
        // (a [0] that acknowledges nothing): the core yields once, with
        // exactly one pause and no packet (#39's `yield_take`).
        h.play(true).await;
        assert_eq!(next_packet(&mut rx).await, expected_owns(1).await);
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        h.emit(owns(1));
        h.emit(owns(0));
        wait_until("the yield's pause", || h.pauses() >= 1).await;
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 1);
        let trace = traced(mac);
        assert_eq!(
            trace.iter().filter(|l| l.starts_with("YIELD")).count(),
            1,
            "{trace:#?}"
        );
        assert_eq!(h.activations(), 1);

        stop_handoff_task(mac);
    }

    /// The resume chain's actions traced so far, in order.
    fn resume_actions(mac: &str) -> Vec<String> {
        traced(mac)
            .into_iter()
            .filter(|l| l.starts_with("Resume "))
            .collect()
    }

    /// An automatic take with the transport reported idle, acknowledged:
    /// the core resumes A2DP.
    async fn resumed_take(h: &Harness, rx: &mut mpsc::Receiver<Vec<u8>>, mac: &str) {
        expect_startup(h, rx).await;
        h.a2dp(false).await;
        expect_take(h, rx).await;
        h.emit(owns(1));
        wait_until("the resume chain's start", || {
            !resume_actions(mac).is_empty()
        })
        .await;
    }

    #[tokio::test]
    async fn a2dp_resume_chain_runs_after_take_ack_without_release() {
        let mac = "5A:00:00:00:00:C8";
        let (manager, mut rx, _tx) = ready_session().await;
        let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
        resumed_take(&h, &mut rx, mac).await;
        let trace = traced(mac);
        assert!(
            position(&trace, "A2DP transport idle") < position(&trace, "Resume "),
            "{trace:#?}"
        );
        // Playback stops inside the window: no release.
        h.play(false).await;
        let player = format!("{PLAYER:?}");
        let expected = [
            "SuspendSink(true)".to_string(),
            "SuspendSink(false)".to_string(),
            format!("Pause({player})"),
            format!("Play({player})"),
            "ProfileCycle".to_string(),
            format!("Play({player})"),
        ]
        .map(|action| format!("Resume {action}"));
        tokio::time::timeout(Duration::from_secs(8), async {
            while resume_actions(mac).len() < expected.len() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the whole resume chain");
        assert_eq!(resume_actions(mac), expected);
        // Still inside the window: no OwnsConnection at all.
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 0);

        stop_handoff_task(mac);
    }

    #[tokio::test]
    async fn a2dp_resume_chain_stops_after_yield() {
        let mac = "5A:00:00:00:00:C9";
        let (manager, mut rx, _tx) = ready_session().await;
        let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
        resumed_take(&h, &mut rx, mac).await;
        // Another host takes the AirPods while the kick has the player
        // paused: the yield pauses, and the chain plays nothing after it.
        tokio::time::timeout(Duration::from_secs(3), async {
            while !resume_actions(mac)
                .iter()
                .any(|a| a.starts_with("Resume Pause"))
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the kick's Pause");
        h.emit(owns(0));
        wait_until("the yield's pause", || h.pauses() == 1).await;
        // Longer than the rest of the chain would take.
        tokio::time::sleep(Duration::from_secs(4)).await;
        let trace = traced(mac);
        let paused = position(&trace, "PauseMedia");
        let after: Vec<_> = trace[paused..]
            .iter()
            .filter(|l| l.starts_with("Resume "))
            .collect();
        assert_eq!(after, Vec::<&String>::new(), "{trace:#?}");
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 1);

        stop_handoff_task(mac);
    }

    /// Another host takes the AirPods while the chain's `check`-th audio
    /// check runs (2: after the kick, 3: after the profile cycle): the chain
    /// acts no more after the yield's pause.
    async fn yield_during_audio_check(mac: &str, check: usize) {
        HELD_CHECKS.lock().unwrap().push(mac.to_string());
        let (manager, mut rx, _tx) = ready_session().await;
        let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
        resumed_take(&h, &mut rx, mac).await;
        let count = |trace: &[String], entry: &str| trace.iter().filter(|l| *l == entry).count();
        tokio::time::timeout(Duration::from_secs(10), async {
            while count(&traced(mac), "Audible check") < check {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the audio check");
        h.emit(owns(0));
        wait_until("the yield's pause", || h.pauses() == 1).await;
        // Past the held check and what the chain would do after it.
        tokio::time::sleep(HELD_CHECK * 2).await;
        let trace = traced(mac);
        let paused = position(&trace, "PauseMedia");
        // The yield came during that check.
        assert_eq!(
            count(&trace[..paused], "Audible checked"),
            check - 1,
            "{trace:#?}"
        );
        let after: Vec<_> = trace[paused..]
            .iter()
            .filter(|l| l.starts_with("Resume "))
            .collect();
        assert_eq!(after, Vec::<&String>::new(), "{trace:#?}");
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 1);

        stop_handoff_task(mac);
    }

    #[tokio::test]
    async fn a2dp_resume_yield_during_check_before_cycle_stops_chain() {
        yield_during_audio_check("5A:00:00:00:00:CA", 2).await;
    }

    #[tokio::test]
    async fn a2dp_resume_yield_during_check_after_cycle_stops_chain() {
        yield_during_audio_check("5A:00:00:00:00:CB", 3).await;
    }

    #[tokio::test]
    async fn handoff_default_path_boundary_follows_pre_snapshot_backlog() {
        let mac = "5A:00:00:00:00:31";
        let (manager, _rx, _tx) = ready_session().await;
        let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
        // Nothing awaited since the task was spawned on this current-thread
        // runtime: it has not taken its snapshot yet.
        h.emit(AACPEvent::OwnershipToFalseRequest);
        wait_until("the boundary", || {
            traced(mac).iter().any(|l| l.starts_with("BACKLOG drained"))
        })
        .await;
        h.emit(AACPEvent::OwnershipToFalseRequest);
        let remotes = || -> Vec<usize> {
            let trace = traced(mac);
            (0..trace.len())
                .filter(|&i| trace[i].starts_with("REMOTE requested ownership"))
                .collect()
        };
        wait_until("the post-snapshot request", || remotes().len() == 2).await;
        let trace = traced(mac);
        let boundary = position(&trace, "BACKLOG drained");
        let remote = remotes();
        assert!(remote[0] < boundary && boundary < remote[1], "{trace:#?}");
        assert_eq!(
            trace
                .iter()
                .filter(|l| l.starts_with("BACKLOG drained"))
                .count(),
            1,
            "{trace:#?}"
        );

        stop_handoff_task(mac);
    }

    /// #21: a handoff session through the default path, its lines appended
    /// by the handoff log's LineSink, never puts the proximity keys the
    /// session carried in the log.
    #[tokio::test]
    async fn handoff_log_never_contains_proximity_keys() {
        const IRK: [u8; 16] = [
            0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0x07, 0x18, 0x29, 0x3a, 0x4b, 0x5c, 0x6d, 0x7e,
            0x8f, 0x90,
        ];
        const ENC: [u8; 16] = [
            0x0f, 0x1e, 0x2d, 0x3c, 0x4b, 0x5a, 0x69, 0x78, 0x87, 0x96, 0xa5, 0xb4, 0xc3, 0xd2,
            0xe1, 0xf0,
        ];
        // Standard base64 of IRK and ENC, without the padding.
        const BASE64: [&str; 2] = ["obLD1OX2BxgpOktcbX6PkA", "Dx4tPEtaaXiHlqW0w9Lh8A"];
        let mac = "5A:00:00:00:00:21";
        let dir =
            std::env::temp_dir().join(format!("librepods-handoff-log-keys-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("librepods/handoff.jsonl");
        let log = Arc::new(handoff_log::Log::open(&path, handoff_log::Source::App).unwrap());
        let logged = || std::fs::read_to_string(&path).unwrap();

        let (manager, _rx, _tx) = ready_session().await;
        let media = Arc::new(FakeMedia {
            records: Arc::new(Records::default()),
            mac: mac.to_string(),
        });
        let media_controller = Arc::new(Mutex::new(MediaController::new(
            mac.to_string(),
            LOCAL.to_string(),
        )));
        let (events, events_rx) = mpsc::unbounded_channel();
        manager.set_event_channel(events.clone()).await;
        let (play_tx, play_rx) = mpsc::channel(8);
        let (_call_tx, call_rx) = mpsc::channel(8);
        let (command_tx, _command_rx) = mpsc::unbounded_channel();
        Switching::start_with_calls(
            SwitchingSetup {
                mode: SwitchingMode::Handoff,
                airpods_mac: mac.to_string(),
                local_mac: LOCAL.to_string(),
                manager: manager.clone(),
                media_controller,
                command_tx,
            },
            media,
            move || PlaybackSamples {
                rx: play_rx,
                rebaseline: Default::default(),
            },
            move |_: &str| call_rx,
        )
        .await
        .with_line_sink(log.sink(mac.to_string()))
        .dispatch_events(events_rx, Arc::new(DeviceHandler::default()).handle());
        wait_until("the automatic start", || logged().contains("AUTO enabled")).await;

        events
            .send(AACPEvent::ProximityKeys(vec![
                (0x01, IRK.to_vec()),
                (0x04, ENC.to_vec()),
            ]))
            .unwrap();
        events.send(owns(0)).unwrap();
        play_tx.send((Instant::now(), false)).await.unwrap();
        play_tx.send((Instant::now(), true)).await.unwrap();
        wait_until("the take", || {
            logged().contains("requested Linux ownership")
        })
        .await;
        events.send(owns(1)).unwrap();
        events
            .send(AACPEvent::ConnectedDevices(
                vec![host(LOCAL), host(REMOTE)],
                vec![host(LOCAL), host(REMOTE), host(SECOND_REMOTE)],
            ))
            .unwrap();
        events.send(AACPEvent::OwnershipToFalseRequest).unwrap();
        wait_until("the yield", || {
            logged().contains("REMOTE requested ownership; yielding")
        })
        .await;
        stop_handoff_task(mac);

        let text = logged();
        assert!(text.contains("ACK owner=linux"), "{text}");
        for key in [IRK, ENC] {
            for encoded in [hex::encode(key), hex::encode_upper(key)] {
                assert!(!text.contains(&encoded), "{encoded} in {text}");
            }
        }
        for encoded in BASE64 {
            assert!(!text.contains(encoded), "{encoded} in {text}");
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn handoff_remote_request_yields_once() {
        let mac = "5A:00:00:00:00:02";
        let (manager, mut rx, _tx) = ready_session().await;
        let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
        expect_startup(&h, &mut rx).await;
        expect_take(&h, &mut rx).await;
        h.emit(owns(1));

        // Another host asks for the AirPods: the core pauses and yields once;
        // no legacy reaction adds a second [0] or a deactivate.
        h.emit(AACPEvent::OwnershipToFalseRequest);
        assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 1);
        // The yield's [0] ACK is not a second yield (#39).
        h.emit(owns(0));
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 1);
        let trace = traced(mac);
        assert!(!trace.iter().any(|l| l.starts_with("YIELD")), "{trace:#?}");

        // A new remote host is announced once (Announce covers every
        // connected remote host, as the probe's `announce` does).
        let hosts = vec![host(LOCAL), host(REMOTE), host(SECOND_REMOTE)];
        manager.state.lock().await.connected_devices = hosts.clone();
        h.emit(AACPEvent::ConnectedDevices(
            vec![host(LOCAL), host(REMOTE)],
            hosts,
        ));
        let mut announced = Vec::new();
        for _ in 0..4 {
            announced.push(next_packet(&mut rx).await);
        }
        assert_eq!(
            announced,
            [
                expected_new_device(REMOTE).await,
                expected_tipi(REMOTE).await,
                expected_new_device(SECOND_REMOTE).await,
                expected_tipi(SECOND_REMOTE).await,
            ]
        );
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 1);

        stop_handoff_task(mac);
    }

    #[tokio::test]
    async fn handoff_local_call_refuses_remote_and_advertises() {
        let mac = "5A:00:00:00:00:0C";
        let (manager, mut rx, _tx) = ready_session().await;
        let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
        expect_startup(&h, &mut rx).await;
        expect_take(&h, &mut rx).await;
        h.emit(owns(1));
        wait_until("the take's ACK", || {
            traced(mac).iter().any(|l| l.starts_with("ACK owner=linux"))
        })
        .await;

        // A call starts on Linux: one in-call media information packet per
        // remote host, and nothing else.
        h.call(true).await;
        let advertised = next_packet(&mut rx).await;
        assert_eq!(advertised, expected_call(REMOTE).await);
        assert_eq!(
            advertised[4..],
            encode_media_information_call(LOCAL, REMOTE).expect("reference")[..]
        );
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        // The same state again changes nothing.
        h.call(true).await;
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());

        // An iPhone starts media and asks for the AirPods: refused, with no
        // OwnsConnection [0] and no pause.
        h.emit(AACPEvent::AudioSource(AudioSource {
            mac: REMOTE.to_string(),
            r#type: AudioSourceType::Media,
        }));
        h.emit(AACPEvent::OwnershipToFalseRequest);
        wait_until("the refusal", || {
            traced(mac)
                .iter()
                .any(|l| l == "REMOTE requested ownership; refused during a local call")
        })
        .await;
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 0);

        // A host that joins during the call is announced and told of it.
        let hosts = vec![host(LOCAL), host(REMOTE), host(SECOND_REMOTE)];
        manager.state.lock().await.connected_devices = hosts.clone();
        h.emit(AACPEvent::ConnectedDevices(
            vec![host(LOCAL), host(REMOTE)],
            hosts,
        ));
        let mut announced = Vec::new();
        for _ in 0..6 {
            announced.push(next_packet(&mut rx).await);
        }
        assert_eq!(
            announced,
            [
                expected_new_device(REMOTE).await,
                expected_tipi(REMOTE).await,
                expected_new_device(SECOND_REMOTE).await,
                expected_tipi(SECOND_REMOTE).await,
                expected_call(REMOTE).await,
                expected_call(SECOND_REMOTE).await,
            ]
        );
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());

        // The call ends with playback still running: the idle media
        // information per remote host, no release.
        h.call(false).await;
        assert_eq!(
            next_packet(&mut rx).await,
            expected_new_device(REMOTE).await
        );
        assert_eq!(
            next_packet(&mut rx).await,
            expected_new_device(SECOND_REMOTE).await
        );
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 0);

        // Normal policy again: the next request yields once.
        h.emit(AACPEvent::OwnershipToFalseRequest);
        assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 1);
        let trace = traced(mac);
        assert!(
            position(&trace, "CALL ended on Linux")
                < position(&trace, "REMOTE requested ownership; yielding")
        );

        stop_handoff_task(mac);
    }

    #[tokio::test]
    async fn handoff_task_ends_on_session_loss_without_reconnect() {
        let mac = "5A:00:00:00:00:03";
        let (manager, mut rx, tx) = ready_session().await;
        let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
        let task = handoff_task(mac).expect("handoff task");
        expect_startup(&h, &mut rx).await;
        expect_take(&h, &mut rx).await;
        h.emit(owns(1));

        // The receive loop clears the dead session without an event.
        manager.disconnect().await;
        wait_until("the task to end", || task.is_finished()).await;
        // Disconnected was fed: the core paused the media Linux took for.
        assert_eq!(h.pauses(), 1);
        assert!(handoff_task(mac).is_none());

        // Nothing is sent afterwards, even on a live sender: no reconnect,
        // no take.
        manager.state.lock().await.sender = Some(tx);
        h.play(false).await;
        h.play(true).await;
        h.emit(owns(0));
        h.emit(AACPEvent::OwnershipToFalseRequest);
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert!(rx.try_recv().is_err());
        assert_eq!(h.pauses(), 1);
        assert_eq!(h.activations(), 1);
    }

    #[tokio::test]
    async fn handoff_closed_event_stream_feeds_session_lost_then_disconnected() {
        let mac = "5A:00:00:00:00:12";
        let (manager, mut rx, _tx) = ready_session().await;
        let mut h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
        let task = handoff_task(mac).expect("handoff task");
        expect_startup(&h, &mut rx).await;
        expect_take(&h, &mut rx).await;
        h.emit(owns(1));
        wait_until("the take's ACK", || {
            traced(mac)
                .iter()
                .any(|line| line.starts_with("ACK owner=linux"))
        })
        .await;
        let before = traced(mac).len();

        // Every sender of the primary channel goes; the session stays alive.
        h.close_events();
        manager.set_event_channel(mpsc::unbounded_channel().0).await;
        wait_until("the task to end", || task.is_finished()).await;
        assert!(handoff_task(mac).is_none());

        let trace = traced(mac)[before..].to_vec();
        assert!(
            trace.len() >= 2
                && trace[0].starts_with("SESSION lost")
                && trace[1].starts_with("DISCONNECTED"),
            "{trace:#?}"
        );
        // Disconnected owes the pause for the automatic take.
        assert_eq!(h.pauses(), 1);

        // Nothing is sent afterwards, even though the session is alive.
        h.play(false).await;
        h.play(true).await;
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert!(rx.try_recv().is_err());
        assert_eq!(h.pauses(), 1);
    }

    #[tokio::test]
    async fn handoff_not_stalled_by_slow_device_handler() {
        let mac = "5A:00:00:00:00:08";
        let (manager, mut rx, _tx) = ready_session().await;
        let h = Harness::start_with(SwitchingMode::Handoff, mac, &manager, true).await;
        expect_startup(&h, &mut rx).await;
        expect_take(&h, &mut rx).await;

        // The device handler blocks on a wear report, as ear detection does
        // behind an A2DP activation (up to ~43 s).
        let worn = wear(EarDetectionStatus::InEar);
        h.emit(worn.clone());
        wait_until("the handler to block", || {
            h.handler.started() == named(&[owns(0), worn.clone()])
        })
        .await;
        // Queued behind it: the take's ACK, then another host's request.
        h.emit(owns(1));
        h.emit(AACPEvent::OwnershipToFalseRequest);

        // The core still yields once and pauses once.
        assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 1);
        // And it saw the ownership report queued behind the blocked event
        // before the request.
        let trace = traced(mac);
        assert!(
            position(&trace, "ACK owner=linux")
                < position(&trace, "REMOTE requested ownership; yielding")
        );
        // The handler is still blocked on the wear report.
        assert_eq!(h.handler.started(), named(&[owns(0), worn.clone()]));
        assert_eq!(h.handler.finished(), named(&[owns(0)]));

        // Released, it handles every event once, in order.
        h.handler.release.notify_one();
        let all = named(&[owns(0), worn, owns(1), AACPEvent::OwnershipToFalseRequest]);
        wait_until("the handler to catch up", || h.handler.finished() == all).await;
        assert_eq!(h.handler.started(), all);
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 1);

        stop_handoff_task(mac);
    }

    /// The production media adapter (`device_media`) never waits for the
    /// outer MediaController mutex that ear detection holds across an A2DP
    /// activation: a remote request is still answered at once, and the task
    /// keeps taking events and ticks (session loss) while it is held.
    #[tokio::test]
    async fn handoff_production_media_ignores_held_controller_lock() {
        let mac = "5A:00:00:00:00:0B";
        let (manager, mut rx, _tx) = ready_session().await;
        let records = Arc::new(Records::default());
        let counted = records.clone();
        let paused_for = mac.to_string();
        // Built as `device_media` builds it. Only the blocking D-Bus pause is
        // replaced (no session bus here, and a test must not pause the
        // developer's players); an empty AirPods MAC makes the real A2DP
        // activation return before touching PipeWire.
        let (outer, media) = share_media(
            MediaController::new(String::new(), LOCAL.to_string()),
            Arc::new(move || {
                counted.pauses.fetch_add(1, Ordering::SeqCst);
                trace(&paused_for, "PauseMedia".to_string());
                media_controller::PauseOutcome::default()
            }),
        );
        let handler = DeviceHandler {
            hold: Some(outer.clone()),
            ..Default::default()
        };
        handler.block_ear.store(true, Ordering::SeqCst);
        let h = Harness::start_wired(
            SwitchingMode::Handoff,
            mac,
            &manager,
            (outer.clone(), media),
            records,
            Arc::new(handler),
        )
        .await;
        let task = handoff_task(mac).expect("handoff task");
        assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
        assert_eq!(
            next_packet(&mut rx).await,
            expected_new_device(REMOTE).await
        );
        assert_eq!(next_packet(&mut rx).await, expected_tipi(REMOTE).await);
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        expect_take(&h, &mut rx).await;
        h.emit(owns(1));
        wait_until("the take's ACK", || {
            traced(mac).iter().any(|l| l.starts_with("ACK owner=linux"))
        })
        .await;

        // Ear detection takes the outer mutex and holds it, as across a slow
        // A2DP activation.
        let worn = wear(EarDetectionStatus::InEar);
        h.emit(worn.clone());
        wait_until("ear detection to hold the MediaController", || {
            outer.try_lock().is_err()
        })
        .await;

        // Another host asks: [PauseMedia, SendOwnership(false)] runs through
        // the real adapter without waiting for the mutex.
        h.emit(AACPEvent::OwnershipToFalseRequest);
        assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 1);
        let trace = traced(mac);
        assert!(
            position(&trace, "REMOTE requested ownership; yielding")
                < position(&trace, "PauseMedia")
        );

        // Later events still reach the core.
        h.emit(owns(0));
        wait_until("the yield's ACK", || {
            let trace = traced(mac);
            trace
                .iter()
                .skip(position(&trace, "REMOTE requested ownership; yielding"))
                .any(|l| l.starts_with("ACK owner=not-linux"))
        })
        .await;

        // And ticks: the task notices the lost session and ends.
        manager.disconnect().await;
        wait_until("the task to end", || task.is_finished()).await;
        assert!(outer.try_lock().is_err(), "the mutex was held throughout");
        assert_eq!(h.handler.finished(), named(&[owns(0), owns(1)]));

        // Released, ear detection and the rest run once, in order.
        h.handler.release.notify_one();
        let all = named(&[
            owns(0),
            owns(1),
            worn,
            AACPEvent::OwnershipToFalseRequest,
            owns(0),
        ]);
        wait_until("the handler to catch up", || h.handler.finished() == all).await;
        assert_eq!(h.handler.started(), all);
    }

    #[tokio::test]
    async fn handoff_session_loss_drains_primary_queue() {
        let mac = "5A:00:00:00:00:09";
        let (manager, mut rx, tx) = ready_session().await;
        let h = Harness::start_with(SwitchingMode::Handoff, mac, &manager, true).await;
        let task = handoff_task(mac).expect("handoff task");
        expect_startup(&h, &mut rx).await;
        expect_take(&h, &mut rx).await;
        h.emit(owns(1));
        // Playback stops: the release is pending. Playback restarts during
        // it, so its ACK carries the deferred pause (#39 keeps that one).
        let stopped = Instant::now();
        h.play(false).await;
        // shortcut: waits the real RELEASE_HOLD; std::time::Instant ignores a paused clock (#249)
        let hold = crate::handoff::RELEASE_HOLD;
        let release = tokio::time::timeout(hold + Duration::from_secs(3), rx.recv())
            .await
            .expect("expected the release")
            .expect("send channel open");
        assert!(stopped.elapsed() >= hold);
        assert_eq!(release, expected_owns(0).await);
        h.play(true).await;
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 0);

        // The buds go in the case; the device handler blocks on that report.
        let in_case = wear(EarDetectionStatus::InCase);
        h.emit(in_case.clone());
        wait_until("the handler to block", || {
            h.handler.started() == named(&[owns(0), owns(1), in_case.clone()])
        })
        .await;
        let before = traced(mac).len();

        // The release ACK arrives, then the receive loop clears the session.
        // The test runtime is single-threaded and neither call yields, so no
        // task sees the ACK before the session is gone.
        let release_ack = expected_owns(0).await;
        manager.receive_packet(&release_ack).await;
        manager.disconnect().await;
        wait_until("the task to end", || task.is_finished()).await;

        // As in handoff::tests::session_loss_before_release_ack_does_not_retake:
        // SessionLost → [], Ownership([0]) → [PauseMedia], Disconnected →
        // [DropSession], with the case closure from the queued wear report.
        let trace = traced(mac)[before..].to_vec();
        let expected = [
            "SESSION lost",
            "ACK owner=not-linux",
            "PauseMedia",
            "DISCONNECTED",
            "CASE waiting for new Linux playback",
        ];
        assert_eq!(trace.len(), expected.len(), "{trace:#?}");
        for (entry, prefix) in trace.iter().zip(expected) {
            assert!(entry.starts_with(prefix), "{trace:#?}");
        }
        assert_eq!(h.pauses(), 1);
        assert!(handoff_task(mac).is_none());

        // Nothing is sent afterwards, even on a live sender.
        manager.state.lock().await.sender = Some(tx);
        h.play(true).await;
        h.emit(AACPEvent::OwnershipToFalseRequest);
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert!(rx.try_recv().is_err());
        assert_eq!(h.pauses(), 1);

        // Released, the handler gets the wear report once, then the rest in
        // order.
        assert_eq!(h.handler.finished(), named(&[owns(0), owns(1)]));
        h.handler.release.notify_one();
        let all = named(&[
            owns(0),
            owns(1),
            in_case,
            owns(0),
            AACPEvent::OwnershipToFalseRequest,
        ]);
        wait_until("the handler to catch up", || h.handler.finished() == all).await;
        assert_eq!(h.handler.started(), all);
    }

    #[tokio::test]
    async fn router_flush_forwards_every_queued_event() {
        // The handoff task's flush, made once it finds the session dead,
        // returns only after everything already in the primary queue has
        // been forwarded to it.
        let (primary_tx, primary) = mpsc::unbounded_channel();
        let (switching_tx, mut switching) = mpsc::unbounded_channel();
        let (handlers_tx, mut handlers) = mpsc::unbounded_channel();
        let (flush_tx, flush) = mpsc::unbounded_channel();
        let queued = [owns(1), wear(EarDetectionStatus::InCase), owns(0)];
        for event in &queued {
            primary_tx.send(event.clone()).unwrap();
        }
        let (done, flushed) = oneshot::channel();
        flush_tx.send(done).unwrap();
        let router = tokio::spawn(route_events(
            primary,
            Some(switching_tx),
            handlers_tx,
            Some(flush),
        ));
        flushed.await.expect("flush answered");
        let mut forwarded = Vec::new();
        while let Ok(event) = switching.try_recv() {
            forwarded.push(event);
        }
        assert_eq!(named(&forwarded), named(&queued));

        // The handlers get the same events, in order.
        let mut handled = Vec::new();
        while let Ok(event) = handlers.try_recv() {
            handled.push(event);
        }
        assert_eq!(named(&handled), named(&queued));

        // Once the primary queue closes the router ends, and a flush (the
        // shell's `Shell::flush`) is not left waiting.
        drop(primary_tx);
        tokio::time::timeout(Duration::from_secs(3), router)
            .await
            .expect("router ended")
            .unwrap();
        let (done, flushed) = oneshot::channel();
        assert!(flush_tx.send(done).is_err());
        assert!(flushed.await.is_err());
    }

    #[tokio::test]
    async fn handoff_task_replacement_is_scoped_to_device() {
        let first_mac = "5A:00:00:00:00:04";
        let other_mac = "5A:00:00:00:00:05";
        let (first_manager, _first_rx, _first_tx) = ready_session().await;
        let (other_manager, _other_rx, _other_tx) = ready_session().await;
        let (replacement_manager, _replacement_rx, _replacement_tx) = ready_session().await;

        let _first = Harness::start(SwitchingMode::Handoff, first_mac, &first_manager).await;
        let first_task = handoff_task(first_mac).expect("first task");
        let _other = Harness::start(SwitchingMode::Handoff, other_mac, &other_manager).await;
        let other_task = handoff_task(other_mac).expect("other task");
        assert!(!first_task.is_finished());

        let _replacement =
            Harness::start(SwitchingMode::Handoff, first_mac, &replacement_manager).await;
        wait_until("the superseded task to stop", || first_task.is_finished()).await;
        let replacement_task = handoff_task(first_mac).expect("replacement task");
        tokio::time::sleep(SETTLE).await;
        assert!(!replacement_task.is_finished());
        assert!(!other_task.is_finished());
        assert!(handoff_task(first_mac).is_some());

        stop_handoff_task(first_mac);
        stop_handoff_task(other_mac);
    }

    #[tokio::test]
    async fn off_never_sends_ownership_or_hijack() {
        let mac = "5A:00:00:00:00:06";
        let (manager, mut rx, _tx) = ready_session().await;
        let h = Harness::start(SwitchingMode::Off, mac, &manager).await;

        h.play(false).await;
        h.play(true).await;
        h.emit(owns(1));
        h.play(false).await;
        h.emit(owns(0));
        h.emit(AACPEvent::OwnershipToFalseRequest);
        // An OwnsConnection [0] report also reaches control command
        // subscribers (the legacy pause/deactivate reaction).
        manager
            .receive_packet(&[4, 0, 4, 0, 9, 0, 6, 0, 0, 0, 0])
            .await;
        let hosts = vec![host(LOCAL), host(REMOTE), host(SECOND_REMOTE)];
        manager.state.lock().await.connected_devices = hosts.clone();
        h.emit(AACPEvent::ConnectedDevices(
            vec![host(LOCAL), host(REMOTE)],
            hosts,
        ));

        tokio::time::sleep(SETTLE).await;
        assert!(rx.try_recv().is_err());
        assert_eq!(h.pauses(), 0);
        assert_eq!(h.activations(), 0);
        assert!(!h.playback_started.load(Ordering::SeqCst));
        assert!(handoff_task(mac).is_none());
        assert!(!media_controller::playback_listener_registered(mac));
        // The device handlers still get every event.
        assert_eq!(h.handler.finished().len(), 5);
    }

    #[tokio::test]
    async fn switching_legacy_starts_playback_listener() {
        let mac = "5A:00:00:00:00:07";
        let (manager, _rx, _tx) = ready_session().await;

        // A handoff task left by a previous device for this MAC stops.
        let _previous = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
        let previous_task = handoff_task(mac).expect("handoff task");

        let h = Harness::start(SwitchingMode::Legacy, mac, &manager).await;
        assert!(media_controller::playback_listener_registered(mac));
        assert!(handoff_task(mac).is_none());
        assert!(!h.playback_started.load(Ordering::SeqCst));
        wait_until("the handoff task to stop", || previous_task.is_finished()).await;

        // And a handoff device replacing a legacy one stops its listener.
        let _next = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
        assert!(!media_controller::playback_listener_registered(mac));
        assert!(handoff_task(mac).is_some());
        stop_handoff_task(mac);
    }

    #[tokio::test]
    async fn switching_legacy_reactions_get_dispatched_events() {
        // The legacy ConnectedDevices reaction runs on events from the
        // dispatch: it announces the new remote host.
        let mac = "5A:00:00:00:00:0A";
        let (manager, mut rx, _tx) = ready_session().await;
        let h = Harness::start(SwitchingMode::Legacy, mac, &manager).await;
        h.emit(AACPEvent::ConnectedDevices(
            vec![host(LOCAL), host(REMOTE)],
            vec![host(LOCAL), host(REMOTE), host(SECOND_REMOTE)],
        ));
        let announce = [
            expected_new_device(SECOND_REMOTE).await,
            expected_tipi(SECOND_REMOTE).await,
        ];
        let mut sent = Vec::new();
        while sent.len() < 2 || !sent.ends_with(&announce) {
            sent.push(next_packet(&mut rx).await);
        }
        wait_until("the handler", || h.handler.finished().len() == 1).await;
        media_controller::stop_playback_listener(mac);
    }

    #[tokio::test]
    async fn handoff_take_and_yield_route_default_sink() {
        let mac = "5A:00:00:00:00:28";
        let (manager, mut rx, _tx) = ready_session().await;
        let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
        expect_startup(&h, &mut rx).await;
        expect_take(&h, &mut rx).await;
        assert_eq!(h.routes(), []);
        h.emit(owns(1));
        wait_until("the take's route", || !h.routes().is_empty()).await;
        // Another host takes the AirPods.
        h.emit(owns(0));
        wait_until("the yield's route", || h.routes().len() > 1).await;
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.routes(), [SinkEvent::Take, SinkEvent::Yield]);
        stop_handoff_task(mac);
    }

    /// Playback, a take's ACK and a loss, in a mode with no handoff core.
    async fn take_and_loss_without_core(h: &Harness) {
        h.play(false).await;
        h.play(true).await;
        h.emit(owns(1));
        h.emit(owns(0));
        wait_until("the handler", || h.handler.finished().len() == 2).await;
        tokio::time::sleep(SETTLE).await;
    }

    #[tokio::test]
    async fn off_never_routes_default_sink() {
        let mac = "5A:00:00:00:00:29";
        let (manager, _rx, _tx) = ready_session().await;
        let h = Harness::start(SwitchingMode::Off, mac, &manager).await;
        take_and_loss_without_core(&h).await;
        assert_eq!(h.routes(), []);
    }

    #[tokio::test]
    async fn legacy_never_routes_default_sink() {
        let mac = "5A:00:00:00:00:2A";
        let (manager, _rx, _tx) = ready_session().await;
        let h = Harness::start(SwitchingMode::Legacy, mac, &manager).await;
        take_and_loss_without_core(&h).await;
        assert_eq!(h.routes(), []);
        media_controller::stop_playback_listener(mac);
    }

    #[test]
    fn seed_inputs_copy_the_snapshot_without_decisions() {
        let facts = SessionFacts {
            hosts: vec![host(LOCAL), host(REMOTE)],
            ears: vec![EarDetectionStatus::InEar; 2],
            audio: None,
            owner: Some(vec![0]),
        };
        assert_eq!(
            seed_inputs(facts),
            [
                Input::ConnectedDevices {
                    old: Vec::new(),
                    new: vec![host(LOCAL), host(REMOTE)],
                },
                Input::EarState(vec![EarDetectionStatus::InEar; 2]),
                Input::Ownership(vec![0]),
            ]
        );
    }

    /// The playback poll interval of the stream tests.
    const STREAM_TEST_POLL: Duration = Duration::from_millis(20);
    /// The minimum stream duration of the stream tests: long enough that a
    /// stream set for a fifth of it can't count however the test is
    /// scheduled, short enough for `next_packet`'s timeout.
    const STREAM_TEST_MIN: Duration = Duration::from_millis(1000);

    /// The sink inputs the test sets, listed as they are at each call.
    #[derive(Clone, Default)]
    struct LiveStreams {
        streams: Arc<StdMutex<Vec<StreamProps>>>,
        /// Listings returned so far.
        listings: Arc<AtomicUsize>,
    }

    impl LiveStreams {
        fn set(&self, streams: Vec<StreamProps>) {
            *self.streams.lock().unwrap() = streams;
        }

        /// Wait until a listing that started after this call has returned,
        /// so the stream monitor has seen what was set before it.
        async fn settle(&self) {
            let after = self.listings.load(Ordering::SeqCst) + 2;
            wait_until("a stream listing", || {
                self.listings.load(Ordering::SeqCst) >= after
            })
            .await;
        }
    }

    impl PlaybackSource for LiveStreams {
        fn playback_streams(&mut self) -> io::Result<Vec<StreamProps>> {
            let streams = self.streams.lock().unwrap().clone();
            self.listings.fetch_add(1, Ordering::SeqCst);
            Ok(streams)
        }
    }

    /// `LiveStreams` whose first listing waits until `gate` is released.
    struct GatedFirst {
        streams: LiveStreams,
        gate: Option<std::sync::mpsc::Receiver<()>>,
    }

    impl PlaybackSource for GatedFirst {
        fn playback_streams(&mut self) -> io::Result<Vec<StreamProps>> {
            if let Some(gate) = self.gate.take() {
                let _ = gate.recv();
            }
            self.streams.playback_streams()
        }
    }

    /// A sound server that never answers: every listing blocks until the
    /// sender is dropped.
    struct StalledStreams(std::sync::mpsc::Receiver<()>);

    impl PlaybackSource for StalledStreams {
        fn playback_streams(&mut self) -> io::Result<Vec<StreamProps>> {
            let _ = self.0.recv();
            Err(io::Error::other("stalled source released"))
        }
    }

    fn sink_input(index: u32, role: &str) -> StreamProps {
        StreamProps {
            direction: StreamDirection::Playback,
            props: [
                ("media.role".to_string(), role.to_string()),
                ("application.name".to_string(), "test player".to_string()),
            ]
            .into(),
            monitor: false,
            index,
            corked: false,
            muted: false,
        }
    }

    fn music(index: u32) -> StreamProps {
        sink_input(index, "Music")
    }

    fn event_sound(index: u32) -> StreamProps {
        sink_input(index, "event")
    }

    /// The next playback sample.
    async fn next_sample(rx: &mut PlaybackSamples) -> (Instant, bool) {
        tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("expected a playback sample")
            .expect("poller running")
    }

    /// Read samples until one says `playing`, for at most 3 s; returns its
    /// sample time.
    async fn sample_until(rx: &mut PlaybackSamples, playing: bool) -> Instant {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let (sampled_at, sample) = next_sample(rx).await;
                if sample == playing {
                    return sampled_at;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("no playing={playing} sample"))
    }

    #[tokio::test]
    async fn playback_poller_mpris_only_still_plays() {
        let mpris = Arc::new(AtomicBool::new(false));
        let reads = mpris.clone();
        let mut rx = spawn_playback_poller_with(
            move || reads.load(Ordering::SeqCst),
            LiveStreams::default(),
            AllAudible,
            STREAM_TEST_MIN,
            STREAM_TEST_POLL,
            |_| (),
        );
        assert!(!next_sample(&mut rx).await.1);
        mpris.store(true, Ordering::SeqCst);
        sample_until(&mut rx, true).await;
        // Every sample says so while MPRIS plays, with no stream at all.
        for _ in 0..5 {
            assert!(next_sample(&mut rx).await.1);
        }
        mpris.store(false, Ordering::SeqCst);
        sample_until(&mut rx, false).await;
    }

    #[tokio::test]
    async fn playback_poller_stream_plays_without_mpris() {
        let streams = LiveStreams::default();
        streams.set(vec![event_sound(1)]);
        let mut rx = spawn_playback_poller_with(
            || false,
            streams.clone(),
            AllAudible,
            STREAM_TEST_MIN,
            STREAM_TEST_POLL,
            |_| (),
        );
        assert!(!next_sample(&mut rx).await.1);
        // The stream monitor's baseline has the event sound only.
        streams.settle().await;
        let started = Instant::now();
        streams.set(vec![event_sound(1), music(2)]);
        let playing_at = sample_until(&mut rx, true).await;
        // Only once the stream lasted the minimum.
        assert!(playing_at.duration_since(started) >= STREAM_TEST_MIN);
        for _ in 0..5 {
            assert!(next_sample(&mut rx).await.1);
        }
        // The stream ends: not playing, with only the event sound left.
        streams.set(vec![event_sound(1)]);
        sample_until(&mut rx, false).await;
    }

    #[tokio::test]
    async fn playback_poller_stalled_stream_source_keeps_mpris() {
        let (release, stalled) = std::sync::mpsc::channel::<()>();
        let mpris = Arc::new(AtomicBool::new(false));
        let reads = mpris.clone();
        let mut rx = spawn_playback_poller_with(
            move || reads.load(Ordering::SeqCst),
            StalledStreams(stalled),
            AllAudible,
            STREAM_TEST_MIN,
            STREAM_TEST_POLL,
            |_| (),
        );
        assert!(!next_sample(&mut rx).await.1);
        mpris.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(1), sample_until(&mut rx, true))
            .await
            .expect("an MPRIS sample despite the stalled stream source");
        for _ in 0..5 {
            assert!(next_sample(&mut rx).await.1);
        }
        drop(rx);
        // Let the blocked listing return so the runtime can shut down.
        drop(release);
    }

    /// Every playback sample's value for `period`.
    async fn samples_for(rx: &mut PlaybackSamples, period: Duration) -> Vec<bool> {
        let end = tokio::time::Instant::now() + period;
        let mut seen = Vec::new();
        while let Ok((_, playing)) = tokio::time::timeout_at(end, next_sample(rx)).await {
            seen.push(playing);
        }
        seen
    }

    /// A stream-only poller over `streams` (no MPRIS) past its empty
    /// baseline listing.
    async fn stream_poller<S: PlaybackSource + 'static>(
        streams: &LiveStreams,
        source: S,
    ) -> PlaybackSamples {
        let rx = spawn_playback_poller_with(
            || false,
            source,
            AllAudible,
            STREAM_TEST_MIN,
            STREAM_TEST_POLL,
            |_| (),
        );
        streams.settle().await;
        rx
    }

    #[tokio::test]
    async fn playback_poller_rebaseline_debouncing_stream_never_plays() {
        let streams = LiveStreams::default();
        let mut rx = stream_poller(&streams, streams.clone()).await;
        streams.set(vec![music(2)]);
        streams.settle().await;
        tokio::time::sleep(STREAM_TEST_MIN / 5).await;
        // The core resets its baseline while the stream debounces.
        rx.rebaseline(Instant::now());
        rx.drain();
        let seen = samples_for(&mut rx, STREAM_TEST_MIN * 2 + STREAM_TEST_MIN / 2).await;
        assert!(!seen.is_empty());
        assert!(seen.iter().all(|playing| !playing), "{seen:?}");
    }

    #[tokio::test]
    async fn playback_poller_rebaseline_later_stream_plays_after_min() {
        let streams = LiveStreams::default();
        let mut rx = stream_poller(&streams, streams.clone()).await;
        rx.rebaseline(Instant::now());
        rx.drain();
        streams.settle().await;
        let started = Instant::now();
        streams.set(vec![music(3)]);
        let playing_at = sample_until(&mut rx, true).await;
        assert!(playing_at.duration_since(started) >= STREAM_TEST_MIN);
    }

    /// A stream-only poller whose core resets while `music(6)` debounces,
    /// that stream having started before the reset (`before_reset`) or just
    /// after it. The re-baseline, stamped with the reset, is requested only a
    /// fifth of the minimum after the stream was seen, so it applies late.
    /// Returns the samples and when the stream started. It calls
    /// `rebaseline` directly; the `step_rebaseline` tests go through `step`.
    async fn late_rebaseline(before_reset: bool) -> (PlaybackSamples, Instant) {
        let streams = LiveStreams::default();
        let mut rx = stream_poller(&streams, streams.clone()).await;
        let start = || {
            let started = Instant::now();
            streams.set(vec![music(6)]);
            started
        };
        let started = before_reset.then(start);
        streams.settle().await;
        let reset = Instant::now();
        let started = started.unwrap_or_else(start);
        streams.settle().await;
        tokio::time::sleep(STREAM_TEST_MIN / 5).await;
        rx.rebaseline(reset);
        rx.drain();
        (rx, started)
    }

    #[tokio::test]
    async fn playback_poller_rebaseline_late_request_keeps_start_after_reset() {
        let (mut rx, started) = late_rebaseline(false).await;
        let playing_at = sample_until(&mut rx, true).await;
        assert!(playing_at.duration_since(started) >= STREAM_TEST_MIN);
    }

    #[tokio::test]
    async fn playback_poller_rebaseline_late_request_drops_start_before_reset() {
        let (mut rx, _) = late_rebaseline(true).await;
        let seen = samples_for(&mut rx, STREAM_TEST_MIN * 2 + STREAM_TEST_MIN / 2).await;
        assert!(!seen.is_empty());
        assert!(seen.iter().all(|playing| !playing), "{seen:?}");
    }

    /// The shell requests the re-baseline at the reset but reaches
    /// DrainPlayback only after its awaits, past the stream's remaining
    /// debounce, with a sample sender blocked meanwhile: still no true.
    #[tokio::test]
    async fn playback_poller_rebaseline_before_slow_drain_never_plays() {
        let streams = LiveStreams::default();
        let mut rx = stream_poller(&streams, streams.clone()).await;
        streams.set(vec![music(7)]);
        streams.settle().await;
        let before = samples_for(&mut rx, STREAM_TEST_MIN * 2 / 5).await;
        assert!(before.iter().all(|playing| !playing), "{before:?}");
        rx.rebaseline(Instant::now());
        // SendOwnership and Announce await; play_rx is not read.
        tokio::time::sleep(STREAM_TEST_MIN).await;
        rx.drain();
        let seen = samples_for(&mut rx, STREAM_TEST_MIN * 2 + STREAM_TEST_MIN / 2).await;
        assert!(!seen.is_empty());
        assert!(seen.iter().all(|playing| !playing), "{seen:?}");
    }

    /// `LiveStreams` whose listings wait while the test holds `hold`;
    /// `started` counts the listings begun.
    struct HeldStreams {
        streams: LiveStreams,
        hold: Arc<StdMutex<()>>,
        started: Arc<AtomicUsize>,
    }

    impl PlaybackSource for HeldStreams {
        fn playback_streams(&mut self) -> io::Result<Vec<StreamProps>> {
            self.started.fetch_add(1, Ordering::SeqCst);
            drop(self.hold.lock());
            self.streams.playback_streams()
        }
    }

    #[tokio::test]
    async fn playback_poller_rebaseline_during_listing_never_plays() {
        let streams = LiveStreams::default();
        let hold = Arc::new(StdMutex::new(()));
        let started = Arc::new(AtomicUsize::new(0));
        let source = HeldStreams {
            streams: streams.clone(),
            hold: hold.clone(),
            started: started.clone(),
        };
        let mut rx = stream_poller(&streams, source).await;
        streams.set(vec![music(4)]);
        streams.settle().await;
        // Hold the next listing; once it returns the stream has lasted the
        // minimum, so it would count. A listing that begins before the
        // hold may still pass; the one after it waits.
        let begun = started.load(Ordering::SeqCst) + 1;
        let held = hold.lock().unwrap();
        wait_until("a held listing", || started.load(Ordering::SeqCst) >= begun).await;
        tokio::time::sleep(STREAM_TEST_MIN + STREAM_TEST_MIN / 5).await;
        rx.rebaseline(Instant::now());
        rx.drain();
        drop(held);
        let seen = samples_for(&mut rx, STREAM_TEST_MIN * 2 + STREAM_TEST_MIN / 2).await;
        assert!(!seen.is_empty());
        assert!(seen.iter().all(|playing| !playing), "{seen:?}");
    }

    #[tokio::test]
    async fn playback_poller_rebaseline_keeps_counted_stream() {
        let streams = LiveStreams::default();
        let mut rx = stream_poller(&streams, streams.clone()).await;
        streams.set(vec![music(5)]);
        sample_until(&mut rx, true).await;
        // The ReconnectedForPlayback case: the take is for this stream.
        rx.rebaseline(Instant::now());
        rx.drain();
        let seen = samples_for(&mut rx, STREAM_TEST_MIN * 2 + STREAM_TEST_MIN / 2).await;
        assert!(!seen.is_empty());
        assert!(seen.iter().all(|playing| *playing), "{seen:?}");
    }

    /// What a held stream worker reports: the instant it was held at, and
    /// the hold, released when sent to or dropped.
    type Held = (Instant, std::sync::mpsc::Sender<()>);

    /// Holds the stream worker once armed: at its first `reached(at)` with
    /// `at` at or after the armed instant, it reports `at` and waits there.
    #[derive(Clone, Default)]
    struct Hold(Arc<StdMutex<Option<(Instant, oneshot::Sender<Held>)>>>);

    impl Hold {
        fn arm(&self, from: Instant) -> oneshot::Receiver<Held> {
            let (report, held) = oneshot::channel();
            *self.0.lock().unwrap() = Some((from, report));
            held
        }

        fn reached(&self, at: Instant) {
            let armed = self.0.lock().unwrap().take_if(|(from, _)| at >= *from);
            if let Some((_, report)) = armed {
                let (release, released) = std::sync::mpsc::channel();
                if report.send((at, release)).is_ok() {
                    let _ = released.recv();
                }
            }
        }
    }

    async fn held(held: oneshot::Receiver<Held>) -> Held {
        tokio::time::timeout(Duration::from_secs(3), held)
            .await
            .expect("the stream worker held")
            .expect("hold armed")
    }

    /// Every candidate audible; asked about candidates, it first waits at
    /// its `Hold`, before the worker's clock read.
    struct HeldPeaks(Hold);

    impl PeakSource for HeldPeaks {
        fn levels(&mut self, candidates: &std::collections::HashSet<u32>) -> HashMap<u32, f32> {
            if !candidates.is_empty() {
                self.0.reached(Instant::now());
            }
            AllAudible.levels(candidates)
        }
    }

    /// A stream-only poller over `streams` with `peaks` and the stream
    /// worker's `observed` hook, past its empty baseline listing.
    async fn held_poller<P: PeakSource + 'static>(
        streams: &LiveStreams,
        peaks: P,
        observed: impl Fn(Instant) + Send + Sync + 'static,
    ) -> PlaybackSamples {
        let rx = spawn_playback_poller_with(
            || false,
            streams.clone(),
            peaks,
            STREAM_TEST_MIN,
            STREAM_TEST_POLL,
            observed,
        );
        streams.settle().await;
        rx
    }

    /// A core step that resets the baseline, run through
    /// `PlaybackSamples::handle` on its own thread over `rx`'s re-baseline
    /// slot. The core's now goes to the returned receiver; the core returns
    /// [DrainPlayback] once `finish` is sent to or dropped.
    fn reset_step(
        rx: &PlaybackSamples,
    ) -> (oneshot::Receiver<Instant>, std::sync::mpsc::Sender<()>) {
        let samples = PlaybackSamples {
            rx: mpsc::channel(1).1,
            rebaseline: rx.rebaseline.clone(),
        };
        let (reset_tx, reset) = oneshot::channel();
        let (finish, finished) = std::sync::mpsc::channel::<()>();
        std::thread::spawn(move || {
            samples.handle(|now| {
                let _ = reset_tx.send(now);
                let _ = finished.recv();
                vec![Command::DrainPlayback]
            })
        });
        (reset, finish)
    }

    /// The samples stamped at or after `reset` for `period`, up to and
    /// including the first that `last` accepts.
    async fn samples_since(
        rx: &mut PlaybackSamples,
        reset: Instant,
        period: Duration,
        last: impl Fn(bool) -> bool,
    ) -> Vec<(Instant, bool)> {
        let end = tokio::time::Instant::now() + period;
        let mut seen = Vec::new();
        while let Ok((sampled_at, playing)) = tokio::time::timeout_at(end, next_sample(rx)).await {
            if sampled_at >= reset {
                seen.push((sampled_at, playing));
                if last(playing) {
                    break;
                }
            }
        }
        seen
    }

    fn values(seen: &[(Instant, bool)]) -> Vec<bool> {
        seen.iter().map(|&(_, playing)| playing).collect()
    }

    /// The synchronous gap: the core resets while the worker samples a
    /// stream debouncing since before the reset, and the worker's clock,
    /// read after the reset, completes the minimum before the core's step
    /// has returned. The stream still never plays.
    #[tokio::test]
    async fn playback_poller_rebaseline_sync_gap_never_plays() {
        let streams = LiveStreams::default();
        let hold = Hold::default();
        let mut rx = held_poller(&streams, HeldPeaks(hold.clone()), |_| ()).await;
        // Held at the levels of the stream's first listing; its clock, read
        // after this, starts the debounce.
        let first = hold.arm(Instant::now());
        streams.set(vec![music(12)]);
        let (seen, release) = held(first).await;
        // Half the minimum later, so no observe before it completes it.
        let racing = hold.arm(seen + STREAM_TEST_MIN / 2);
        drop(release);
        let (listed, release) = held(racing).await;
        // The step runs while that sample is held, its core step held too.
        let (reset, finish) = reset_step(&rx);
        let reset = reset.await.expect("the core stepped");
        let mut seen = samples_since(&mut rx, reset, Duration::from_secs(3), |_| true).await;
        // The worker's clock completes the minimum.
        tokio::time::sleep_until((listed + STREAM_TEST_MIN).into()).await;
        drop(release);
        // Without the shared lock, the observe counts the stream here,
        // before the step's request.
        seen.extend(samples_since(&mut rx, reset, STREAM_TEST_MIN / 2, |playing| playing).await);
        drop(finish);
        seen.extend(samples_since(&mut rx, reset, STREAM_TEST_MIN * 2, |_| false).await);
        let seen = values(&seen);
        assert!(seen.iter().all(|playing| !playing), "{seen:?}");
    }

    /// The publish latency: the worker completes a stream's minimum
    /// before the core's reset and publishes only after it. The core sees
    /// no new start: every sample from the reset on plays.
    #[tokio::test]
    async fn playback_poller_rebaseline_publish_latency_keeps_counted_stream() {
        let streams = LiveStreams::default();
        let hold = Hold::default();
        let observed = hold.clone();
        let mut rx = held_poller(&streams, AllAudible, move |at| observed.reached(at)).await;
        // Held after an observe: the next listing has the stream, so the
        // next observe starts its debounce.
        let (_, release) = held(hold.arm(Instant::now())).await;
        streams.set(vec![music(13)]);
        let first = hold.arm(Instant::now());
        drop(release);
        let (started, release) = held(first).await;
        // Held after the observe that completes the minimum, before its
        // publish.
        let completing = hold.arm(started + STREAM_TEST_MIN);
        drop(release);
        let (_, release) = held(completing).await;
        let (mut reset, finish) = reset_step(&rx);
        drop(finish);
        // Without the shared lock the step resets now, and a sample after
        // it reads the unpublished state; with it, the step waits.
        let mut seen = Vec::new();
        let early = tokio::time::timeout(STREAM_TEST_MIN / 2, &mut reset)
            .await
            .ok()
            .map(|reset| reset.expect("the core stepped"));
        if let Some(reset) = early {
            seen = samples_since(&mut rx, reset, Duration::from_secs(3), |_| true).await;
        }
        drop(release);
        let reset = match early {
            Some(reset) => reset,
            None => reset.await.expect("the core stepped"),
        };
        seen.extend(samples_since(&mut rx, reset, STREAM_TEST_MIN * 2, |_| false).await);
        let seen = values(&seen);
        assert!(!seen.is_empty());
        assert!(seen.iter().all(|playing| *playing), "{seen:?}");
    }

    /// Its twin: a stream first audible after the reset plays once, after
    /// the minimum.
    #[tokio::test]
    async fn playback_poller_rebaseline_publish_latency_later_stream_plays_once() {
        let streams = LiveStreams::default();
        let mut rx = held_poller(&streams, AllAudible, |_| ()).await;
        let (reset, finish) = reset_step(&rx);
        drop(finish);
        let reset = reset.await.expect("the core stepped");
        let started = Instant::now();
        streams.set(vec![music(14)]);
        let seen = samples_since(
            &mut rx,
            reset,
            STREAM_TEST_MIN * 2 + STREAM_TEST_MIN / 2,
            |_| false,
        )
        .await;
        let starts: Vec<Instant> = seen
            .windows(2)
            .filter(|pair| !pair[0].1 && pair[1].1)
            .map(|pair| pair[1].0)
            .collect();
        assert_eq!(starts.len(), 1, "{:?}", values(&seen));
        assert!(!seen[0].1);
        assert!(starts[0].duration_since(started) >= STREAM_TEST_MIN);
    }

    /// The daemon's startup `Input::Auto` resets the core's baseline with
    /// the real playback poller running, and `music(21)` starts before the
    /// reset (`before_reset`) or during Auto's SendOwnership(false), which
    /// the shell awaits before DrainPlayback and the test holds with a full
    /// send channel: past the stream's remaining debounce (before the reset)
    /// or briefly. The first MPRIS poll waits for the reset, so the
    /// poller's pending sample is stamped after it. Returns the harness, the
    /// packets after Auto's and the reply to its "not owning", and when the
    /// stream started.
    async fn step_rebaseline(
        mac: &str,
        before_reset: bool,
    ) -> (Harness, mpsc::Receiver<Vec<u8>>, Instant) {
        let (manager, mut rx, tx) = ready_session().await;
        let mut queued = 0;
        while tx.try_send(Vec::new()).is_ok() {
            queued += 1;
        }
        let (open_mpris, mpris_gate) = std::sync::mpsc::channel::<()>();
        let mpris_gate = StdMutex::new(Some(mpris_gate));
        let mpris = move || {
            if let Some(gate) = mpris_gate.lock().unwrap().take() {
                let _ = gate.recv();
            }
            false
        };
        let streams = LiveStreams::default();
        let h =
            Harness::start_streams(mac, &manager, mpris, streams.clone(), STREAM_TEST_MIN).await;
        // The handoff task has not run yet (a current-thread runtime): it
        // reads the session first, so Auto waits for this guard.
        let session = manager.state.try_lock().expect("session state free");
        streams.settle().await;
        let start = || {
            let started = Instant::now();
            streams.set(vec![music(21)]);
            started
        };
        let started = before_reset.then(start);
        streams.settle().await;
        drop(session);
        // Auto's ConnectA2dp: the core has reset, and SendOwnership(false)
        // is held.
        wait_until("Auto", || h.activations() == 1).await;
        drop(open_mpris);
        let started = started.unwrap_or_else(start);
        streams.settle().await;
        let held = if before_reset {
            STREAM_TEST_MIN + STREAM_TEST_MIN / 5
        } else {
            STREAM_TEST_MIN / 5
        };
        tokio::time::sleep(held).await;
        for _ in 0..queued {
            assert_eq!(next_packet(&mut rx).await, Vec::<u8>::new());
        }
        assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
        assert_eq!(
            next_packet(&mut rx).await,
            expected_new_device(REMOTE).await
        );
        assert_eq!(next_packet(&mut rx).await, expected_tipi(REMOTE).await);
        h.emit(owns(0));
        (h, rx, started)
    }

    #[tokio::test]
    async fn handoff_step_rebaseline_stream_before_reset_never_takes() {
        let mac = "5A:00:00:00:00:14";
        let (h, mut rx, _) = step_rebaseline(mac, true).await;
        tokio::time::sleep(STREAM_TEST_MIN * 2).await;
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 0);
        stop_handoff_task(mac);
    }

    #[tokio::test]
    async fn handoff_step_rebaseline_stream_after_reset_takes_once() {
        let mac = "5A:00:00:00:00:15";
        let (h, mut rx, started) = step_rebaseline(mac, false).await;
        assert_eq!(next_packet(&mut rx).await, expected_owns(1).await);
        assert!(started.elapsed() >= STREAM_TEST_MIN);
        h.emit(owns(1));
        tokio::time::sleep(STREAM_TEST_MIN).await;
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 0);
        stop_handoff_task(mac);
    }

    #[tokio::test]
    async fn handoff_stream_playback_takes() {
        let mac = "5A:00:00:00:00:0D";
        let (manager, mut rx, _tx) = ready_session().await;
        let streams = LiveStreams::default();
        let h =
            Harness::start_streams(mac, &manager, || false, streams.clone(), STREAM_TEST_MIN).await;
        assert!(h.playback_started.load(Ordering::SeqCst));
        expect_startup(&h, &mut rx).await;
        // The reply to Auto's "not owning" (#54).
        h.emit(owns(0));
        // The stream monitor's baseline has no stream.
        streams.settle().await;

        // A music stream with no MPRIS player: one take once it lasted the
        // minimum, OwnsConnection [1] only.
        let started = Instant::now();
        streams.set(vec![music(11)]);
        assert_eq!(next_packet(&mut rx).await, expected_owns(1).await);
        assert!(started.elapsed() >= STREAM_TEST_MIN);
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 0);

        // ACK, then the stream ends: the core's usual release.
        h.emit(owns(1));
        let stopped = Instant::now();
        streams.set(Vec::new());
        // shortcut: waits the real RELEASE_HOLD; std::time::Instant ignores a paused clock (#249)
        let hold = crate::handoff::RELEASE_HOLD;
        let release = tokio::time::timeout(hold + Duration::from_secs(3), rx.recv())
            .await
            .expect("expected the release")
            .expect("send channel open");
        assert!(stopped.elapsed() >= hold);
        assert_eq!(release, expected_owns(0).await);
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 0);

        stop_handoff_task(mac);
    }

    #[tokio::test]
    async fn handoff_event_sound_stream_never_takes() {
        let mac = "5A:00:00:00:00:0E";
        let (manager, mut rx, _tx) = ready_session().await;
        // An event sound open from the start, for longer than the minimum.
        let streams = LiveStreams::default();
        streams.set(vec![event_sound(21)]);
        let h =
            Harness::start_streams(mac, &manager, || false, streams.clone(), STREAM_TEST_MIN).await;
        expect_startup(&h, &mut rx).await;
        streams.settle().await;
        tokio::time::sleep(STREAM_TEST_MIN).await;
        // A new music stream that ends well before the minimum.
        streams.set(vec![event_sound(21), music(22)]);
        tokio::time::sleep(STREAM_TEST_MIN / 5).await;
        streams.set(vec![event_sound(21)]);
        tokio::time::sleep(STREAM_TEST_MIN).await;

        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 0);
        assert_eq!(h.activations(), 1);
        stop_handoff_task(mac);
    }

    #[tokio::test]
    async fn handoff_muted_stream_never_takes() {
        let mac = "5A:00:00:00:00:11";
        let (manager, mut rx, _tx) = ready_session().await;
        let streams = LiveStreams::default();
        let h =
            Harness::start_streams(mac, &manager, || false, streams.clone(), STREAM_TEST_MIN).await;
        expect_startup(&h, &mut rx).await;
        // The baseline has no stream.
        streams.settle().await;
        // A new, uncorked but muted music stream (a muted autoplaying tab),
        // for well over the minimum.
        streams.set(vec![StreamProps {
            muted: true,
            ..music(41)
        }]);
        tokio::time::sleep(STREAM_TEST_MIN * 2 + STREAM_TEST_MIN / 2).await;

        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 0);
        assert_eq!(h.activations(), 1);
        stop_handoff_task(mac);
    }

    /// A music stream already playing in the stream monitor's first listing,
    /// optionally held until playback samples have reached the core: no
    /// take while it keeps playing, one take once it is paused and resumed.
    async fn preexisting_stream_case(mac: &str, delay_first_listing: bool) {
        let (manager, mut rx, _tx) = ready_session().await;
        let streams = LiveStreams::default();
        streams.set(vec![music(31)]);
        let (release, gate) = std::sync::mpsc::channel::<()>();
        let source = GatedFirst {
            streams: streams.clone(),
            gate: delay_first_listing.then_some(gate),
        };
        let mpris_calls = Arc::new(AtomicUsize::new(0));
        let calls = mpris_calls.clone();
        let mpris = move || {
            calls.fetch_add(1, Ordering::SeqCst);
            false
        };
        let h = Harness::start_streams(mac, &manager, mpris, source, STREAM_TEST_MIN).await;
        expect_startup(&h, &mut rx).await;
        if delay_first_listing {
            // The third MPRIS sample is taken only once the core received
            // the first (the channel holds one): the baseline sample came
            // before any stream listing.
            wait_until("MPRIS samples", || mpris_calls.load(Ordering::SeqCst) >= 3).await;
            assert_eq!(streams.listings.load(Ordering::SeqCst), 0);
            release.send(()).expect("first listing waiting");
        }
        streams.settle().await;

        // Playing for more than twice the minimum: no new start, no take.
        tokio::time::sleep(STREAM_TEST_MIN * 2 + STREAM_TEST_MIN / 2).await;
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 0);

        // Paused, then resumed: a new start, one take once it lasted the
        // minimum from the resume, OwnsConnection [1] only.
        streams.set(vec![StreamProps {
            corked: true,
            ..music(31)
        }]);
        streams.settle().await;
        let resumed = Instant::now();
        streams.set(vec![music(31)]);
        assert_eq!(next_packet(&mut rx).await, expected_owns(1).await);
        assert!(resumed.elapsed() >= STREAM_TEST_MIN);
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(h.pauses(), 0);
        stop_handoff_task(mac);
    }

    #[tokio::test]
    async fn handoff_preexisting_stream_playback_never_takes() {
        preexisting_stream_case("5A:00:00:00:00:0F", false).await;
    }

    #[tokio::test]
    async fn handoff_preexisting_stream_late_first_listing_never_takes() {
        preexisting_stream_case("5A:00:00:00:00:10", true).await;
    }

    /// #20: the non-events of upstream #724's reports never take the AirPods
    /// or pause anything. Each runs through the real stream tracker and core
    /// at injected times, with a counterfactual twin that does take.
    mod non_event {
        use super::*;
        use crate::stream_monitor::PlaybackTracker;
        use Command::{PauseMedia, SendOwnership};
        use EarDetectionStatus::{InCase, InEar, OutOfEar};
        use libpulse_binding::proplist::Proplist;
        use libpulse_binding::volume::{ChannelVolumes, Volume};

        const POLL_MS: u64 = PLAYBACK_POLL.as_millis() as u64;
        const MIN_MS: u64 = MIN_STREAM_PLAYBACK.as_millis() as u64;
        /// Peak levels of a stream carrying sound and of a silent one (#88).
        const LOUD: f32 = 0.5;
        const SILENT: f32 = 0.0;
        /// When each scenario's event starts, after the baselines at 10 ms.
        const EVENT_MS: u64 = 500;
        /// Neither takes nor pauses.
        const NO_TAKE: Vec<Vec<Command>> = Vec::new();

        fn facts() -> SessionFacts {
            SessionFacts {
                hosts: vec![host(LOCAL), host(REMOTE)],
                ears: vec![InEar, InEar],
                audio: Some(AudioSource {
                    mac: "00:00:00:00:00:00".to_string(),
                    r#type: AudioSourceType::None,
                }),
                owner: Some(vec![0]),
            }
        }

        /// The core fed as the default path feeds it: each playback sample
        /// is MPRIS `||` the real `PlaybackTracker`'s state, as
        /// `spawn_playback_poller_with` combines them.
        struct Pipeline {
            core: Handoff,
            tracker: PlaybackTracker,
            streams_playing: bool,
            t0: Instant,
            /// Every command list the core returned, in order.
            log: Vec<Vec<Command>>,
        }

        impl Pipeline {
            /// `handoff::tests::Probe::ready`: automatic, connected, Linux
            /// plus a remote host, worn, idle audio, owner [0], then a
            /// stopped sample and an empty stream listing as the baselines.
            fn ready() -> Self {
                let t0 = Instant::now();
                let mut p = Pipeline {
                    core: Handoff::new(LOCAL.to_string(), t0, true, true),
                    tracker: PlaybackTracker::new(MIN_STREAM_PLAYBACK),
                    streams_playing: false,
                    t0,
                    log: Vec::new(),
                };
                for input in seed_inputs(facts()) {
                    p.feed(0, input);
                }
                assert_eq!(p.step(10, false, &[]), []);
                p
            }

            fn feed(&mut self, ms: u64, input: Input) -> Vec<Command> {
                let now = self.t0 + Duration::from_millis(ms);
                let commands = self.core.handle(now, input);
                // As the shells' `step` (#90).
                if commands.contains(&Command::DrainPlayback) {
                    self.tracker.rebaseline(now);
                }
                self.log.push(commands.clone());
                commands
            }

            /// One playback sample at `ms` with every candidate audible.
            fn step(&mut self, ms: u64, mpris: bool, streams: &[StreamProps]) -> Vec<Command> {
                self.step_levels(ms, mpris, streams, LOUD)
            }

            /// One playback sample at `ms`: the stream listing with every
            /// candidate's peak `level`, then the core.
            fn step_levels(
                &mut self,
                ms: u64,
                mpris: bool,
                streams: &[StreamProps],
                level: f32,
            ) -> Vec<Command> {
                let now = self.t0 + Duration::from_millis(ms);
                if let Some(playing) = self.tracker.observe_levels(now, streams, |_| level) {
                    self.streams_playing = playing;
                }
                let playing = mpris || self.streams_playing;
                self.feed(
                    ms,
                    Input::Playback {
                        sampled_at: now,
                        playing,
                    },
                )
            }

            /// A sample every `PLAYBACK_POLL` from `EVENT_MS` until over
            /// twice the minimum stream playback later.
            fn event(&mut self, mpris: bool, streams: &[StreamProps]) {
                self.event_levels(mpris, streams, |_| LOUD);
            }

            /// `event`, with the candidates' peak level at each sample's ms.
            fn event_levels(
                &mut self,
                mpris: bool,
                streams: &[StreamProps],
                level: impl Fn(u64) -> f32,
            ) {
                for ms in (EVENT_MS..=EVENT_MS + 2 * MIN_MS + POLL_MS).step_by(POLL_MS as usize) {
                    self.step_levels(ms, mpris, streams, level(ms));
                }
            }

            /// The command lists that take the AirPods or pause anything.
            fn takes(&self) -> Vec<Vec<Command>> {
                self.log
                    .iter()
                    .filter(|c| c.contains(&SendOwnership(true)) || c.contains(&PauseMedia))
                    .cloned()
                    .collect()
            }
        }

        /// `ready`, then `mpris` and `streams` from the event on.
        fn run(mpris: bool, streams: &[StreamProps]) -> Pipeline {
            let mut p = Pipeline::ready();
            p.event(mpris, streams);
            p
        }

        /// `ready`, then `streams` with no MPRIS player from the event on,
        /// at the peak level `level` gives for each sample's ms.
        fn run_levels(streams: &[StreamProps], level: impl Fn(u64) -> f32) -> Pipeline {
            let mut p = Pipeline::ready();
            p.event_levels(false, streams, level);
            p
        }

        /// Sink input 1, uncorked and unmuted, with these properties only.
        fn sink(props: &[(&str, &str)]) -> StreamProps {
            StreamProps {
                props: props
                    .iter()
                    .map(|&(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                ..music(1)
            }
        }

        #[test]
        fn non_event_system_sound() {
            assert_eq!(run(false, &[event_sound(1)]).takes(), NO_TAKE);
            assert_eq!(run(false, &[music(1)]).takes(), [vec![SendOwnership(true)]]);
        }

        #[test]
        fn non_event_notification_sound() {
            let canberra = sink(&[
                ("application.name", "test player"),
                ("event.id", "message-new-instant"),
            ]);
            for stream in [sink_input(1, "notification"), canberra] {
                assert_eq!(run(false, &[stream.clone()]).takes(), NO_TAKE, "{stream:?}");
            }
            let app_only = sink(&[("application.name", "test player")]);
            assert_eq!(run(false, &[app_only]).takes(), [vec![SendOwnership(true)]]);
        }

        const DISCONNECT_MS: u64 = EVENT_MS;
        /// The music stream of the reconnect case has played over the
        /// minimum by now, and started over the minimum earlier (#90).
        const RECONNECT_MS: u64 = DISCONNECT_MS + POLL_MS + MIN_MS + POLL_MS;

        /// The session drops while worn (recovery Immediate) and a new one
        /// comes at `RECONNECT_MS`; the player (a music stream, with MPRIS
        /// when `mpris`) plays from `start` ms on.
        fn reconnect_case(start: u64, mpris: bool) -> Pipeline {
            let mut p = Pipeline::ready();
            assert_eq!(
                p.feed(DISCONNECT_MS, Input::Disconnected),
                [Command::DropSession]
            );
            let player = [music(1)];
            let end = RECONNECT_MS + 2 * MIN_MS + POLL_MS;
            for ms in (DISCONNECT_MS + POLL_MS..=end).step_by(POLL_MS as usize) {
                if ms == DISCONNECT_MS + 2 * POLL_MS {
                    assert_eq!(
                        p.feed(ms, Input::Tick),
                        [Command::Reconnect { audio: true }]
                    );
                }
                if ms == RECONNECT_MS {
                    // The tracker counts a stream that lasted the minimum.
                    assert_eq!(p.streams_playing, start + MIN_MS < ms);
                    p.feed(ms, Input::Reconnected(facts()));
                }
                let playing = ms >= start;
                let streams: &[StreamProps] = if playing { &player } else { &[] };
                p.step(ms, mpris && playing, streams);
            }
            p
        }

        #[test]
        fn non_event_playback_already_running_on_reconnect() {
            assert_eq!(
                reconnect_case(DISCONNECT_MS + POLL_MS, true).takes(),
                NO_TAKE
            );
            assert_eq!(
                reconnect_case(RECONNECT_MS + POLL_MS, true).takes(),
                [vec![SendOwnership(true)]]
            );
        }

        /// #90: a stream (no MPRIS) still in its debounce at the reconnect's
        /// baseline reset is no new start.
        #[test]
        fn non_event_stream_started_before_reconnect_reset() {
            assert_eq!(
                reconnect_case(RECONNECT_MS - POLL_MS, false).takes(),
                NO_TAKE
            );
            assert_eq!(
                reconnect_case(RECONNECT_MS + POLL_MS, false).takes(),
                [vec![SendOwnership(true)]]
            );
        }

        /// Under the minimum after `EVENT_MS`.
        const RESET_MS: u64 = EVENT_MS + 3 * POLL_MS;

        /// `ready`, then a live `Input::Auto` (answered) at `RESET_MS`; a
        /// music stream with no MPRIS player plays from `start` ms on.
        fn auto_reset_case(start: u64) -> Pipeline {
            let mut p = Pipeline::ready();
            let stream = [music(1)];
            for ms in (EVENT_MS..=RESET_MS + 2 * MIN_MS + POLL_MS).step_by(POLL_MS as usize) {
                if ms == RESET_MS {
                    assert!(!p.streams_playing);
                    assert_eq!(
                        p.feed(ms, Input::Auto),
                        [
                            Command::ConnectA2dp,
                            SendOwnership(false),
                            Command::Announce,
                            Command::DrainPlayback
                        ]
                    );
                    // The reply to Auto's "not owning" (#54).
                    assert_eq!(p.feed(ms, Input::Ownership(vec![0])), []);
                }
                let streams: &[StreamProps] = if ms >= start { &stream } else { &[] };
                p.step(ms, false, streams);
            }
            p
        }

        /// #90: a stream (no MPRIS) still in its debounce when Auto resets
        /// the baseline is no new start.
        #[test]
        fn non_event_stream_started_before_auto_reset() {
            assert_eq!(auto_reset_case(EVENT_MS).takes(), NO_TAKE);
            assert_eq!(
                auto_reset_case(RESET_MS + POLL_MS).takes(),
                [vec![SendOwnership(true)]]
            );
        }

        #[test]
        fn non_event_paused_player() {
            let corked = StreamProps {
                corked: true,
                ..music(1)
            };
            assert_eq!(run(false, &[corked]).takes(), NO_TAKE);
            assert_eq!(run(false, &[music(1)]).takes(), [vec![SendOwnership(true)]]);
        }

        /// A browser tab's sink input (no role) as `PulseStreams` lists it.
        fn tab(mute: bool, level: Volume) -> StreamProps {
            let mut proplist = Proplist::new().expect("proplist");
            proplist.set_str("application.name", "Firefox").unwrap();
            let mut volume = ChannelVolumes::default();
            volume.set(2, level);
            stream_monitor::playback_stream(1, &proplist, false, mute, &volume)
        }

        #[test]
        fn non_event_muted_autoplay_tab() {
            for stream in [tab(true, Volume::NORMAL), tab(false, Volume::MUTED)] {
                assert_eq!(run(false, &[stream.clone()]).takes(), NO_TAKE, "{stream:?}");
            }
            assert_eq!(
                run(false, &[tab(false, Volume::NORMAL)]).takes(),
                [vec![SendOwnership(true)]]
            );
        }

        /// #88: a paused SDL game keeps its Game stream uncorked and writes
        /// silence into it.
        #[test]
        fn non_event_silent_game_stream() {
            let game = [sink(&[
                ("media.role", "Game"),
                ("application.name", "SDL Application"),
            ])];
            assert_eq!(run_levels(&game, |_| SILENT).takes(), NO_TAKE);
            assert_eq!(
                run_levels(&game, |_| LOUD).takes(),
                [vec![SendOwnership(true)]]
            );
        }

        /// #88: an idle Chromium/Electron WebAudio stream (no role) after
        /// one short notification sound.
        #[test]
        fn non_event_webaudio_blip() {
            let webaudio = [sink(&[("application.name", "Chromium")])];
            let blip = |ms| if ms == EVENT_MS { LOUD } else { SILENT };
            assert_eq!(run_levels(&webaudio, blip).takes(), NO_TAKE);
            assert_eq!(
                run_levels(&webaudio, |_| LOUD).takes(),
                [vec![SendOwnership(true)]]
            );
        }

        /// #97/#101: the live pw-loopback playback stream, named and given
        /// a role by user config, stays uncorked and loud.
        #[test]
        fn non_event_virtual_loopback() {
            let twin = [
                ("node.name", "output.pw-loopback-3412"),
                ("node.group", "pw-loopback-3412"),
                ("node.link-group", "pw-loopback-3412"),
                ("stream.is-live", "true"),
                (
                    "target.object",
                    "alsa_output.pci-0000_00_1f.3.analog-stereo",
                ),
                ("application.name", "pw-loopback"),
                ("media.role", "Music"),
            ];
            let loopback = [&twin[..], &[("node.virtual", "true")]].concat();
            assert_eq!(run(false, &[sink(&loopback)]).takes(), NO_TAKE);
            assert_eq!(
                run(false, &[sink(&twin)]).takes(),
                [vec![SendOwnership(true)]]
            );
        }

        /// `ready`, then a wear report, then a fresh music stream with MPRIS.
        fn wearing(ears: [EarDetectionStatus; 2]) -> Pipeline {
            let mut p = Pipeline::ready();
            p.feed(20, Input::EarState(ears.to_vec()));
            p.event(true, &[music(1)]);
            p
        }

        #[test]
        fn non_event_playback_not_worn() {
            for (off, one_in) in [
                ([OutOfEar, OutOfEar], [InEar, OutOfEar]),
                ([InCase, InCase], [InEar, InCase]),
            ] {
                assert_eq!(wearing(off).takes(), NO_TAKE, "{off:?}");
                assert_eq!(
                    wearing(one_in).takes(),
                    [vec![SendOwnership(true)]],
                    "{one_in:?}"
                );
            }
        }

        #[test]
        fn non_event_control_genuine_play_takes() {
            let mut p = Pipeline::ready();
            let stream = [music(1)];
            let mut ms = EVENT_MS;
            while ms < EVENT_MS + MIN_MS {
                assert_eq!(p.step(ms, false, &stream), [], "{ms} ms");
                ms += POLL_MS;
            }
            assert_eq!(p.step(ms, false, &stream), [SendOwnership(true)]);
            for later in (ms + POLL_MS..=ms + MIN_MS).step_by(POLL_MS as usize) {
                assert_eq!(p.step(later, false, &stream), [], "{later} ms");
            }
            assert_eq!(p.takes(), [vec![SendOwnership(true)]]);
        }

        /// #205: a track change's gap (MPRIS stopped, the old stream gone)
        /// keeps the AirPods; a real stop releases after `RELEASE_HOLD`.
        #[test]
        fn non_event_track_change_keeps_ownership() {
            let hold_ms = crate::handoff::RELEASE_HOLD.as_millis() as u64;
            let sent = |p: &Pipeline, owner| {
                let c = SendOwnership(owner);
                p.log.iter().flatten().filter(|&x| *x == c).count()
            };
            let mut p = Pipeline::ready();
            assert_eq!(p.step(EVENT_MS, true, &[music(1)]), [SendOwnership(true)]);
            p.feed(EVENT_MS, Input::Ownership(vec![1]));
            let gap = EVENT_MS + POLL_MS;
            p.step(gap, false, &[]);
            let next = gap + POLL_MS;
            for ms in (next..=next + 2 * hold_ms).step_by(POLL_MS as usize) {
                p.step(ms, true, &[music(2)]);
                p.feed(ms, Input::Tick);
            }
            assert_eq!(sent(&p, true), 1);
            assert_eq!(sent(&p, false), 0);

            // The player stops.
            let stop = next + 2 * hold_ms + POLL_MS;
            for ms in (stop..=stop + hold_ms + POLL_MS).step_by(POLL_MS as usize) {
                p.step(ms, false, &[]);
                p.feed(ms, Input::Tick);
            }
            assert_eq!(sent(&p, true), 1);
            assert_eq!(sent(&p, false), 1);
        }
    }

    #[test]
    fn last_owner_store_is_per_mac() {
        // MACs no other test uses: the store is process-wide.
        const A: &str = "16:16:16:16:16:0A";
        const B: &str = "16:16:16:16:16:0B";
        assert!(!last_owner_linux(A));
        remember_last_owner_linux(A, true);
        assert!(last_owner_linux(A));
        assert!(!last_owner_linux(B));
        remember_last_owner_linux(A, false);
        assert!(!last_owner_linux(A));
    }
}
