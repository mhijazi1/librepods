//! The handoff task: feeds the core and executes its commands.

use super::inputs::{
    advertise_call, announce, event_input, relay_category, seed_inputs, session_alive,
    session_facts,
};
use super::poller::{MediaActions, NOT_PAUSING, PlaybackSamples};
use super::registry::{Pending, last_owner_linux, remember_last_owner_linux};
use super::{Events, FlushRequest, TICK};
use crate::a2dp_resume;
use crate::bluetooth::aacp::{AACPEvent, AACPManager, ControlCommandIdentifiers as Control};
use crate::default_sink::{SinkEvent, UnparkTo};
use crate::handoff::{Command, Handoff, Input, Park};
use crate::reconnect;
use crate::stream_monitor::CallSamples;
use crate::utils::ConnectMode;
use futures::future::BoxFuture;
use log::{error, info};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, watch};

/// The handoff task's I/O: executes the core's commands.
pub(super) struct Shell {
    pub(super) mac: String,
    pub(super) local: String,
    pub(super) manager: AACPManager,
    pub(super) media: Arc<dyn MediaActions>,
    /// The switching side of the event router's fan-out.
    pub(super) events: Events,
    /// Asks the router to forward what is left in the primary queue.
    pub(super) flush: mpsc::UnboundedSender<FlushRequest>,
    pub(super) play_rx: PlaybackSamples,
    /// Local call state changes (#11).
    pub(super) call_rx: CallSamples,
    /// A2DP transport state changes (#200).
    pub(super) a2dp_rx: mpsc::Receiver<bool>,
    /// Ear detection's resume reports (#313).
    pub(super) ear_rx: mpsc::UnboundedReceiver<bool>,
    /// Stops the running A2DP resume chain when dropped.
    pub(super) resume: Option<oneshot::Sender<()>>,
    /// Where the core's lines go after they are logged, if anywhere.
    pub(super) lines: Option<LineSink>,
    /// The device's "Connect to this computer" (#16); the registry keeps a
    /// clone to change it live (#153).
    pub(super) connect_mode: watch::Sender<ConnectMode>,
    /// `connect_mode`'s changes not yet applied to the core.
    pub(super) connect_mode_rx: watch::Receiver<ConnectMode>,
    /// Relay Linux's audio category to the other hosts (#272).
    pub(super) relay_category: bool,
    /// Whether the last `HoldA2dp` held (#264); released on drop (#294).
    pub(super) held: bool,
    /// Whether the last park route parked (#311); unparked on drop.
    pub(super) parked: bool,
    /// Where the drop sends its release's settle.
    pub(super) released: Option<oneshot::Sender<BoxFuture<'static, ()>>>,
}

/// The task's future drops its `Shell` on return, abort and panic alike,
/// after its last poll, so no hold of its own can follow this release.
impl Drop for Shell {
    fn drop(&mut self) {
        // Ahead of the release, which may wait seconds for the AirPods sink,
        // so the default never stays the park sink (#311).
        if self.parked {
            self.media
                .route_default_sink(SinkEvent::Unpark(UnparkTo::Local));
        }
        if self.held {
            self.media.hold_a2dp(false);
        }
        // Whatever this task queued, a natural end's release and unpark
        // included: the registry waits for it.
        if let Some(released) = self.released.take() {
            let _ = released.send(self.media.hold_settled());
        }
    }
}

/// Receives each line the handoff core emits, in order, right after it is
/// logged and before the step's commands run.
pub(crate) type LineSink = Arc<dyn Fn(&str) + Send + Sync>;

impl Shell {
    /// Log a line of the handoff task and hand it to the line sink.
    fn log(&self, line: &str) {
        info!("Handoff {}: {line}", self.mac);
        if let Some(sink) = &self.lines {
            sink(line);
        }
    }

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
                Command::RelayCategory(category) => {
                    if let Err(e) = relay_category(&self.manager, &self.local, category).await {
                        error!("Handoff: failed to relay audio category {category}: {e}");
                    }
                }
                Command::PauseMedia => {
                    // Before the pause, so the chain plays nothing after it.
                    self.resume = None;
                    if !self.media.pause(self.play_rx.streams()).await {
                        // Remote controls only, if anything (#302, #329).
                        self.log(NOT_PAUSING);
                    }
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
                    // Now: a player's stream may cork at the route (#330),
                    // and a player may pause itself after it (#351).
                    let streams = self.play_rx.streams();
                    let players = self.play_rx.players();
                    tokio::spawn(self.media.resume_a2dp(streams, players, until, cancelled));
                }
                Command::RouteToAirPods => self.media.route_default_sink(SinkEvent::Take),
                Command::RouteAway => self.media.route_default_sink(SinkEvent::Yield),
                Command::HoldA2dp(hold) => {
                    self.held = hold;
                    self.media.hold_a2dp(hold);
                }
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
                // Only a reconnect-for-playback start feeds Reconnected (#307).
                Command::AdoptSession => info!("Handoff: adopted the reconnected session"),
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

/// Hand the latest "Connect to this computer" to the core (#153). It gates
/// the next automatic take only: no step, nothing sent.
fn apply_connect_mode(core: &mut Handoff, shell: &mut Shell) {
    let mode = *shell.connect_mode_rx.borrow_and_update();
    core.set_connect_mode(mode);
    shell.log(&format!("CONNECT MODE {mode:?} (live)"));
}

/// Feed one input to the core, log its lines and execute its commands. True
/// when the task should end.
pub(super) async fn step(core: &mut Handoff, shell: &mut Shell, input: Input) -> bool {
    let case_closed = core.case_closed();
    {
        let streams = &shell.play_rx.streams.borrow().0;
        core.set_streams(streams.live.is_some(), streams.baseline);
    }
    // A change saved while this input waited still gates its decision.
    if shell.connect_mode_rx.has_changed().unwrap_or(false) {
        apply_connect_mode(core, shell);
    }
    let (now, commands) = shell.play_rx.handle(|now| core.handle(now, input));
    if commands.contains(&Command::DropSession) {
        // A case close also drops as Remote: no reconnect for it (#307).
        reconnect::case_closed_at_drop(&shell.mac, case_closed);
    }
    remember_last_owner_linux(&shell.mac, core.last_owner_linux());
    for line in core.take_lines() {
        shell.log(&line);
    }
    // On the commands' queue (#311): an unpark to the local sink ahead of
    // the step's hold release, which may wait seconds for the AirPods sink;
    // a park after its hold, an unpark to the AirPods after the ACK's route.
    // `parked` changes only once its route is queued, so a drop in between
    // still unparks.
    let park = core.park();
    let parked = park == Park::Parked;
    let route = (parked != shell.parked).then(|| match park {
        Park::Parked => SinkEvent::Park,
        Park::AirPods => SinkEvent::Unpark(UnparkTo::AirPods),
        Park::Local | Park::FallenBack => SinkEvent::Unpark(UnparkTo::Local),
    });
    let local = SinkEvent::Unpark(UnparkTo::Local);
    if route == Some(local) {
        shell.media.route_default_sink(local);
        shell.parked = parked;
    }
    let end = shell.execute(now, commands).await;
    if let Some(route) = route.filter(|&route| route != local) {
        shell.media.route_default_sink(route);
        shell.parked = parked;
    }
    end
}

pub(super) async fn on_event(core: &mut Handoff, shell: &mut Shell, event: AACPEvent) -> bool {
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

/// Ear detection's resume report (#313). The router queues its reinsertion's
/// EarState here before the handlers see it, but the select can still poll
/// the report first: step the queued events so it never counts early (#358).
pub(super) async fn on_ear_resume(core: &mut Handoff, shell: &mut Shell, resumed: bool) -> bool {
    info!(
        "Handoff {}: ear detection resumed a player: {resumed}",
        shell.mac
    );
    while let Ok(event) = shell.events.try_recv() {
        if mark_session_lost(core, shell).await || on_event(core, shell, event).await {
            return true;
        }
    }
    sync_session(core, shell).await || step(core, shell, Input::EarResume { resumed }).await
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

pub(super) async fn run_handoff(mut shell: Shell, previous: Option<Pending>) {
    // A reconnect the watcher made for new playback takes for it once the
    // session reports its routing (#307); otherwise the normal start. Claimed
    // first, so the watcher still clears it on a stop during the wait; its
    // age is judged there only, so the wait can't expire it.
    let marked = reconnect::claim_marker(&shell.mac);
    // The stopped tasks' card writes must not land after this session's
    // first hold (#294).
    if let Some(previous) = previous {
        previous.await;
    }
    let ready = marked && reconnect::routing_ready(&shell.manager).await;
    if marked && !ready {
        shell.log("RECONNECT host state not ready in 5 s; starting without a take");
    }
    let take_category = reconnect::take_marker(&shell.mac).filter(|_| ready);
    let for_playback = take_category.is_some();
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
    // The latest mode, so a change during the waits above is not lost.
    let mut core = Handoff::new(
        shell.local.clone(),
        Instant::now(),
        for_playback,
        !for_playback,
    )
    .with_connect_mode(*shell.connect_mode_rx.borrow_and_update())
    .with_last_owner_linux(last_owner_linux(&shell.mac))
    .with_default_sink_routing(true)
    .with_a2dp_hold(true)
    .with_park_sink(true)
    .with_category_relay(shell.relay_category);
    let start = if let Some((category, requested)) = take_category {
        core = core
            .with_playback_requested()
            .with_category(category)
            .with_requested_category(requested);
        vec![Input::Reconnected(facts)]
    } else {
        seed_inputs(facts)
    };
    for input in start {
        if step(&mut core, &mut shell, input).await {
            return;
        }
    }
    // Replayed before Auto, so a stale [0] in it cannot pass for the reply
    // to Auto's "not owning" (#54).
    for event in backlog {
        // The facts already hold the last OwnsConnection, and a stale [1]
        // would ack the reconnect take early (#360).
        if core.reconnect_take_pending()
            && matches!(&event, AACPEvent::ControlCommand(s) if s.identifier == Control::OwnsConnection)
        {
            continue;
        }
        if mark_session_lost(&mut core, &mut shell).await
            || on_event(&mut core, &mut shell, event).await
        {
            return;
        }
    }
    // The core's own automatic startup: A2DP, "not owning" (or the connect
    // take from an idle other host, #324), announce. The ownership decision
    // may wait for the routing reports queued after it (#452).
    if !for_playback && step(&mut core, &mut shell, Input::Auto).await {
        return;
    }
    if step(&mut core, &mut shell, Input::InitBacklogDrained).await {
        return;
    }
    let mut tick = tokio::time::interval(TICK);
    loop {
        let end = tokio::select! {
            biased;
            // An idle change logs at once; `step` applies one it beats.
            Ok(()) = shell.connect_mode_rx.changed() => {
                apply_connect_mode(&mut core, &mut shell);
                false
            }
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
            // After events, so a reinsertion's EarState comes first, and
            // before samples, so a queued `false` beats a ready play (#313).
            Some(resumed) = shell.ear_rx.recv() => {
                on_ear_resume(&mut core, &mut shell, resumed).await
            }
            Some((sampled_at, category, new_source)) = shell.play_rx.sample() => {
                sync_session(&mut core, &mut shell).await
                    || step(&mut core, &mut shell, Input::Playback { sampled_at, category, new_source }).await
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
