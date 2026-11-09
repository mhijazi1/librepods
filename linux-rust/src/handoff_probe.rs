use crate::bluetooth::aacp::{
    AACPEvent, AACPManager, AACPManagerState, ControlCommandIdentifiers as Control,
};
use crate::handoff::{Command, Handoff, Input, SessionFacts};
use crate::handoff_log;
use crate::media_controller::{
    A2DP_SINK, CaseRecovery, MediaController, TakeoverAction, TakeoverInputs, TakeoverTrigger,
    decide_takeover, routing_snapshot_of,
};
use crate::switching::{
    NOT_PAUSING, PlaybackSamples, advertise_call, announce, event_input, pause_own_players,
    session_facts, spawn_playback_poller,
};
use bluer::Address;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type ControlSession = (AACPManager, tokio::sync::mpsc::UnboundedReceiver<AACPEvent>);
/// A reconnect attempt's session and its init backlog.
type Reconnection = (ControlSession, InitBacklog);

/// How long the reconnect task waits for the echo of its last "not owning",
/// sent while the cached owner is [1] (a real release).
const INIT_ECHO_WAIT: Duration = Duration::from_secs(1);

/// The events a reconnected session queued while it initialized (#49).
#[derive(Default)]
struct InitBacklog {
    events: Vec<AACPEvent>,
    /// No echo of the last "not owning" was due, or it was seen, so the
    /// boundary may follow `events`, which the facts cut ends (#143): what
    /// follows is fresh.
    drained: bool,
}

/// Take exactly the first `n` queued events.
fn take_prefix<T>(rx: &mut tokio::sync::mpsc::UnboundedReceiver<T>, n: usize) -> Vec<T> {
    (0..n).map_while(|_| rx.try_recv().ok()).collect()
}

/// Run `send` (the last "not owning") and take the events queued when it
/// returns. The AirPods report OwnsConnection only on a real change (#159),
/// so only a cached owner of [1] makes that send a release with an echo:
/// then take events up to and including the first OwnsConnection report,
/// and without one in time the backlog is not drained. Otherwise it is
/// drained at once.
async fn collect_init_backlog<E>(
    manager: &AACPManager,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<AACPEvent>,
    send: impl Future<Output = std::result::Result<(), E>>,
) -> Result<InitBacklog>
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    // Cut after `send` returns: it can wait on the state lock or the writer
    // queue, and a report arriving meanwhile is not its echo. The cut is the
    // queue length then, so an echo racing the prefix take (multi-thread
    // runtime) stays eligible. The cached owner is read under the same state
    // guard, under which the CONTROL_COMMAND arm stores and queues, so an
    // echo already queued has set it to [0] and nothing waits for another.
    send.await.map_err(Into::into)?;
    let (pre_send, owner) = {
        let state = manager.state.lock().await;
        (events.len(), session_facts(&state).owner)
    };
    let mut backlog = InitBacklog {
        events: take_prefix(events, pre_send),
        drained: owner.as_deref() != Some(&[1]),
    };
    if backlog.drained {
        return Ok(backlog);
    }
    backlog.drained = tokio::time::timeout(INIT_ECHO_WAIT, async {
        while let Some(event) = events.recv().await {
            let echo = matches!(
                &event,
                AACPEvent::ControlCommand(s) if s.identifier == Control::OwnsConnection
            );
            backlog.events.push(event);
            if echo {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false);
    Ok(backlog)
}

/// Read the session facts and move every event queued at that moment to the
/// end of the backlog, under one state guard (#143). The CONNECTED_DEVICES
/// arm stores and sends under the same lock, so each report is either in the
/// facts and the backlog, or applied after the facts and still queued.
async fn cut_facts(
    (manager, mut events): ControlSession,
    mut backlog: InitBacklog,
) -> (Reconnection, SessionFacts) {
    let facts = {
        let state = manager.state.lock().await;
        let queued = events.len();
        backlog.events.extend(take_prefix(&mut events, queued));
        session_facts(&state)
    };
    (((manager, events), backlog), facts)
}

/// The core inputs an adopted session's backlog feeds before any later
/// event: its events, then the queue boundary if it was drained.
fn adoption_inputs(backlog: InitBacklog) -> Vec<Input> {
    let mut inputs: Vec<Input> = backlog.events.into_iter().filter_map(event_input).collect();
    if backlog.drained {
        inputs.push(Input::InitBacklogDrained);
    }
    inputs
}

/// During a reconnect take, drop the backlog's OwnsConnection reports: the
/// facts cut, under the state guard that stores them, already holds their
/// last value, and a stale [1] would ack the take early (#360).
fn skip_backlog_ownership(mut inputs: Vec<Input>, take_pending: bool) -> Vec<Input> {
    if take_pending {
        inputs.retain(|input| !matches!(input, Input::Ownership(_)));
    }
    inputs
}

async fn connect_control(address: Address) -> Result<ControlSession> {
    let mut manager = AACPManager::new();
    let (tx, events) = tokio::sync::mpsc::unbounded_channel();
    manager.set_event_channel(tx).await;
    manager.connect(address).await;
    let result = async {
        if manager.state.lock().await.sender.is_none() {
            return Err("AirPods control connection failed".into());
        }
        manager.initialize().await?;
        // A missing ownership report does not prove that Linux is unowned.
        manager
            .send_control_command(Control::OwnsConnection, &[0])
            .await?;
        wait_routing_ready(&manager).await?;
        Ok(())
    }
    .await;
    if let Err(error) = result {
        manager.disconnect().await;
        return Err(error);
    }
    Ok((manager, events))
}

async fn wait_routing_ready(manager: &AACPManager) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = manager.state.lock().await;
            if state.sender.is_none() {
                return Err("AirPods disconnected during initialization".into());
            }
            let ready = !state.connected_devices.is_empty()
                && !state.ear_detection_status.is_empty()
                && state.audio_source.is_some();
            drop(state);
            if ready {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| "AirPods did not provide fresh routing and wear state")?
}

/// Whether a manual take would go ahead now, by the single takeover policy.
fn take_allowed(state: &AACPManagerState, local: &str) -> bool {
    let routing = routing_snapshot_of(state, local);
    let inputs = TakeoverInputs::handoff(TakeoverTrigger::Manual, routing, CaseRecovery::Immediate);
    decide_takeover(inputs).action == TakeoverAction::TakeOver
}

async fn status(manager: &AACPManager, local: &str) {
    let state = manager.state.lock().await;
    let owner = state
        .control_command_status_list
        .iter()
        .find(|s| s.identifier == Control::OwnsConnection)
        .map(|s| match s.value.as_slice() {
            [0] => "not-linux",
            [1] => "linux",
            _ => "unknown",
        })
        .unwrap_or("unknown");
    println!(
        "STATUS connected={} owner={} hosts={} wear={:?} take-ready={}",
        state.sender.is_some(),
        owner,
        state.connected_devices.len(),
        state.ear_detection_status,
        take_allowed(&state, local)
    );
    for host in &state.connected_devices {
        println!(
            "HOST {} flags={:02x}/{:02x}",
            if host.mac == local { "linux" } else { "remote" },
            host.info1,
            host.info2
        );
    }
    if let Some(source) = &state.audio_source {
        println!(
            "AUDIO source={} activity={:?}",
            if source.mac == local {
                "linux"
            } else if source.mac == "00:00:00:00:00:00" {
                "none"
            } else {
                "remote"
            },
            source.r#type
        );
    } else {
        println!("AUDIO unknown");
    }
}

/// Whether the AACP receive loop still has the control session. The guard is
/// dropped before this returns.
async fn session_alive(manager: &AACPManager) -> bool {
    manager.state.lock().await.sender.is_some()
}

/// The probe's I/O: executes the commands of the handoff core.
struct Io {
    adapter: bluer::Adapter,
    address: Address,
    local: String,
    media: MediaController,
    manager: AACPManager,
    events: tokio::sync::mpsc::UnboundedReceiver<AACPEvent>,
    play_rx: PlaybackSamples,
    retry: Option<tokio::task::JoinHandle<Result<(Reconnection, SessionFacts)>>>,
    /// Session made by the last reconnect, until the core adopts or cancels it.
    candidate: Option<Reconnection>,
    /// The adopted session's backlog inputs, fed right after the step that
    /// adopted it.
    adopted: Vec<Input>,
    /// The handoff log (#21); None only if no state directory is known
    /// (neither XDG_STATE_HOME nor HOME set) or its writer thread failed to
    /// start. A failed open or write warns once, then stops logging.
    log: Option<handoff_log::Log>,
}

impl Io {
    /// A send that failed because the session died after the core's last
    /// liveness check is not fatal: the next `sync_session` tears the session
    /// down. Any other send error still stops the probe.
    async fn tolerate_lost_session(&self, sent: Result<()>) -> Result<()> {
        match sent {
            Err(error) if !session_alive(&self.manager).await => {
                println!("SEND failed: {error}; control session lost");
                Ok(())
            }
            other => other,
        }
    }

    /// Execute the core's commands in order. Ok(true) when it asked to exit.
    /// No AACP state guard is held here; the sends take their own.
    async fn execute(&mut self, commands: Vec<Command>) -> Result<bool> {
        for command in commands {
            match command {
                Command::SendOwnership(owns) => {
                    let sent = self
                        .manager
                        .send_control_command(Control::OwnsConnection, &[u8::from(owns)])
                        .await
                        .map_err(Into::into);
                    self.tolerate_lost_session(sent).await?
                }
                Command::Announce => {
                    let sent = announce(&self.manager, &self.local).await;
                    self.tolerate_lost_session(sent).await?
                }
                // The probe feeds no LocalCall, so the core never asks.
                Command::AdvertiseCall(in_call) => {
                    let sent = advertise_call(&self.manager, &self.local, in_call).await;
                    self.tolerate_lost_session(sent).await?
                }
                Command::PauseMedia => {
                    let pause = Arc::new(MediaController::pause_playing_mpris_players);
                    if !pause_own_players(&self.media, pause, self.play_rx.streams()).await {
                        println!("{NOT_PAUSING}");
                        if let Some(log) = &self.log {
                            log.line(&self.address.to_string(), NOT_PAUSING);
                        }
                    }
                }
                Command::ConnectA2dp => {
                    let device = self.adapter.device(self.address)?;
                    device.connect_profile(&A2DP_SINK).await?;
                }
                Command::DropSession => {
                    self.manager.disconnect().await;
                    self.events.close();
                    while self.events.try_recv().is_ok() {}
                }
                Command::Reconnect { audio } => {
                    let adapter = self.adapter.clone();
                    let address = self.address;
                    self.retry = Some(tokio::spawn(async move {
                        let (manager, mut events) = connect_control(address).await?;
                        if !audio {
                            return Ok(cut_facts((manager, events), InitBacklog::default()).await);
                        }
                        let device = adapter.device(address)?;
                        let connected = tokio::time::timeout(
                            Duration::from_secs(5),
                            device.connect_profile(&A2DP_SINK),
                        )
                        .await;
                        match connected {
                            Ok(Ok(())) => {}
                            _ => {
                                manager.disconnect().await;
                                return Err("A2DP reconnection failed".into());
                            }
                        }
                        let backlog = collect_init_backlog(
                            &manager,
                            &mut events,
                            manager.send_control_command(Control::OwnsConnection, &[0]),
                        )
                        .await?;
                        Ok(cut_facts((manager, events), backlog).await)
                    }));
                }
                Command::AdoptSession => {
                    if let Some(((manager, events), backlog)) = self.candidate.take() {
                        self.manager = manager;
                        self.events = events;
                        self.adopted = adoption_inputs(backlog);
                    }
                }
                Command::CancelReconnect => {
                    if let Some(((manager, _), _)) = self.candidate.take() {
                        manager.disconnect().await;
                    }
                    self.adapter.device(self.address)?.disconnect().await?;
                }
                Command::DrainPlayback => self.play_rx.drain(),
                // The probe feeds no A2dpTransport, so the core never asks.
                Command::ResumeA2dp => println!("A2DP resume is not run by the probe"),
                // The probe never enables default sink routing.
                Command::RouteToAirPods | Command::RouteAway => {
                    println!("Default sink routing is not run by the probe")
                }
                // The probe never enables the A2DP hold.
                Command::HoldA2dp(_) => println!("A2DP hold is not run by the probe"),
                Command::RelayCategory(_) => println!("category relay is not run by the probe"),
                Command::Exit => return Ok(true),
                Command::Fail(reason) => return Err(reason.into()),
            }
        }
        Ok(false)
    }
}

/// Feed one input to the core, print its protocol lines and execute its
/// commands. Ok(true) when the probe should stop.
async fn step(core: &mut Handoff, io: &mut Io, input: Input) -> Result<bool> {
    // The daemon's order (#90): the re-baseline is requested at the
    // core's now, under the stream worker's lock.
    let (_, commands) = io.play_rx.handle(|now| core.handle(now, input));
    for line in core.take_lines() {
        println!("{line}");
        if let Some(log) = &io.log {
            log.line(&io.address.to_string(), &line);
        }
    }
    io.execute(commands).await
}

/// Feed one AACP event to the core and print STATUS after routing reports.
/// Ok(true) when the probe should stop.
async fn on_event(core: &mut Handoff, io: &mut Io, event: AACPEvent) -> Result<bool> {
    let input = event_input(event);
    // STATUS follows routing and ownership reports, not a remote request.
    let show_status = input
        .as_ref()
        .is_some_and(|input| *input != Input::RemoteOwnershipRequest);
    if let Some(input) = input
        && step(core, io, input).await?
    {
        return Ok(true);
    }
    if show_status {
        status(&io.manager, &io.local).await;
    }
    Ok(false)
}

/// The AACP receive loop clears a dead session (aacp.rs `clear_connection`)
/// without an event. Mark it lost in the core so nothing is sent or taken on
/// it, without dropping events that are still queued.
async fn mark_session_lost(core: &mut Handoff, io: &mut Io) -> Result<bool> {
    if core.live() && !session_alive(&io.manager).await {
        return step(core, io, Input::SessionLost).await;
    }
    Ok(false)
}

/// Tell the core when the control session died under it, before it acts on
/// routing state that the disconnect already cleared. The receive loop sends
/// its events before it clears the session, so once the sender is gone the
/// queue holds all of them: feed them first (a case-closed wear report must
/// still set case recovery), then the disconnect, which drops the session.
async fn sync_session(core: &mut Handoff, io: &mut Io) -> Result<bool> {
    if !core.connected() || session_alive(&io.manager).await {
        return Ok(false);
    }
    if step(core, io, Input::SessionLost).await? {
        return Ok(true);
    }
    while let Ok(event) = io.events.try_recv() {
        if on_event(core, io, event).await? {
            return Ok(true);
        }
    }
    step(core, io, Input::Disconnected).await
}

/// Feed the init backlog of the session the last step adopted, then its
/// boundary, before any later event of that session (#49). Ok(true) when the
/// probe should stop.
async fn feed_adopted(core: &mut Handoff, io: &mut Io) -> Result<bool> {
    let adopted = std::mem::take(&mut io.adopted);
    for input in skip_backlog_ownership(adopted, core.reconnect_take_pending()) {
        if mark_session_lost(core, io).await? || step(core, io, input).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Wait for the running reconnect attempt; never resolves without one.
async fn reconnect_result(
    retry: &mut Option<tokio::task::JoinHandle<Result<(Reconnection, SessionFacts)>>>,
) -> std::result::Result<Result<(Reconnection, SessionFacts)>, tokio::task::JoinError> {
    match retry {
        Some(task) => task.await,
        None => std::future::pending().await,
    }
}

pub async fn run(address: Address, daemon: bool) -> Result<()> {
    let session = bluer::Session::new().await?;
    let adapter = session.default_adapter().await?;
    let local = adapter.address().await?.to_string();
    let (manager, events) = if daemon && !adapter.device(address)?.is_connected().await? {
        // Do not race the iPhone to reconnect when starting with the buds in their case.
        let manager = AACPManager::new();
        let (tx, events) = tokio::sync::mpsc::unbounded_channel();
        manager.set_event_channel(tx).await;
        (manager, events)
    } else {
        connect_control(address).await?
    };
    let connected = manager.state.lock().await.sender.is_some();
    if daemon {
        if connected {
            let device = adapter.device(address)?;
            tokio::time::timeout(Duration::from_secs(5), device.connect_profile(&A2DP_SINK))
                .await??;
            manager
                .send_control_command(Control::OwnsConnection, &[0])
                .await?;
            announce(&manager, &local).await?;
        }
        println!("AUTO service enabled; waiting for a new playback start");
    } else {
        println!("READY commands: status, refresh, announce, take, release, auto, manual, quit");
        println!("Automatic switching is disabled until the auto command.");
    }
    let media = MediaController::new(address.to_string(), local.clone());
    let play_rx = spawn_playback_poller();
    // Every handoff decision lives in the core; this loop only feeds it.
    let mut core = Handoff::new(local.clone(), Instant::now(), daemon, connected);
    let mut io = Io {
        adapter,
        address,
        local,
        media,
        manager,
        events,
        play_rx,
        retry: None,
        candidate: None,
        adopted: Vec::new(),
        log: handoff_log::open_default(handoff_log::Source::Probe),
    };
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    loop {
        tokio::select! {
            biased;
            Some(event) = io.events.recv() => {
                // Check the session before stepping the event (an ownership
                // ACK must not retake on a dead session), but keep the rest
                // of the queue: the disconnect comes at the next sync.
                if mark_session_lost(&mut core, &mut io).await?
                    || on_event(&mut core, &mut io, event).await?
                {
                    break;
                }
            }
            Some((sampled_at, category, new_source)) = io.play_rx.sample() => {
                if sync_session(&mut core, &mut io).await?
                    || step(&mut core, &mut io, Input::Playback { sampled_at, category, new_source }).await?
                {
                    break;
                }
            }
            result = reconnect_result(&mut io.retry) => {
                io.retry = None;
                let input = match result? {
                    Ok((reconnection, facts)) => {
                        io.candidate = Some(reconnection);
                        Input::Reconnected(facts)
                    }
                    Err(error) => Input::ReconnectFailed(error.to_string()),
                };
                if step(&mut core, &mut io, input).await?
                    || feed_adopted(&mut core, &mut io).await?
                {
                    break;
                }
            }
            _ = tick.tick() => {
                if sync_session(&mut core, &mut io).await?
                    || step(&mut core, &mut io, Input::Tick).await?
                {
                    break;
                }
            }
            _ = tokio::signal::ctrl_c(), if !core.quitting() => {
                if step(&mut core, &mut io, Input::Quit).await? {
                    break;
                }
            }
            _ = terminate.recv(), if !core.quitting() => {
                if step(&mut core, &mut io, Input::Quit).await? {
                    break;
                }
            }
            line = lines.next_line(), if !core.quitting() && !daemon => {
                let line = line?;
                let command = line.as_deref().map(str::trim).unwrap_or("quit");
                // The live-sender check comes first, so take, release and
                // auto never act on a session the receive loop has closed.
                if sync_session(&mut core, &mut io).await? {
                    break;
                }
                if !core.connected() && matches!(command, "refresh" | "announce") {
                    println!("WAIT reconnecting; status, manual, and quit remain available");
                    continue;
                }
                let input = match command {
                    "auto" => Input::Auto,
                    "manual" => Input::Manual,
                    "take" => Input::Take,
                    "release" => Input::Release,
                    "quit" => Input::Quit,
                    "status" => {
                        status(&io.manager, &io.local).await;
                        continue;
                    }
                    "refresh" => {
                        io.manager.send_notification_request().await?;
                        continue;
                    }
                    "announce" => {
                        announce(&io.manager, &io.local).await?;
                        println!("Announced Linux to connected remote hosts");
                        continue;
                    }
                    "" => continue,
                    _ => {
                        println!("Commands: status, refresh, announce, take, release, auto, manual, quit");
                        continue;
                    }
                };
                if step(&mut core, &mut io, input).await? {
                    break;
                }
            }
        }
    }
    if let Some(task) = io.retry {
        task.abort();
    }
    io.manager.disconnect().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bluetooth::aacp::{AudioSourceType, EarDetectionStatus};
    // Case recovery is part of the single takeover policy now.
    use crate::media_controller::CaseRecovery as Recovery;

    /// Whether the policy lets a reconnect take for playback in `recovery`,
    /// with routing that is otherwise ready.
    fn reconnect_take_allowed(recovery: Recovery) -> bool {
        let routing = crate::media_controller::RoutingSnapshot {
            has_session: true,
            host_count: 2,
            local_host: true,
            remote_host: true,
            bud_in_ear: true,
            audio: Some(AudioSourceType::None),
            audio_from_local: false,
            local_category: crate::media_controller::AUDIO_CATEGORY_MEDIA,
            remote_category: None,
        };
        let inputs =
            TakeoverInputs::handoff(TakeoverTrigger::ReconnectedForPlayback, routing, recovery);
        decide_takeover(inputs).action == TakeoverAction::TakeOver
    }

    #[test]
    fn case_recovery_requires_new_playback_and_cancels_when_stopped() {
        let mut recovery = Recovery::WaitingForPlayback;
        recovery.playback(false, false);
        assert!(recovery == Recovery::WaitingForPlayback);
        assert!(!reconnect_take_allowed(recovery));
        recovery.playback(true, false);
        assert!(recovery == Recovery::PlaybackRequested);
        assert!(reconnect_take_allowed(recovery));
        recovery.playback(false, true);
        assert!(recovery == Recovery::WaitingForPlayback);
        assert!(!reconnect_take_allowed(recovery));
        recovery.playback(true, false);
        assert!(recovery == Recovery::PlaybackRequested);
        recovery = Recovery::Immediate;
        recovery.playback(false, true);
        assert!(recovery == Recovery::Immediate);
        assert!(!reconnect_take_allowed(recovery));
    }

    #[tokio::test]
    async fn open_transport_is_not_a_ready_control_session() {
        let manager = AACPManager::new();
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        manager.state.lock().await.sender = Some(sender);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), wait_routing_ready(&manager))
                .await
                .is_err()
        );
        manager
            .receive_packet(&[4, 0, 4, 0, 0x2e, 0, 0, 0, 1, 1, 2, 3, 4, 5, 6, 0, 0])
            .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), wait_routing_ready(&manager))
                .await
                .is_err()
        );
        manager.receive_packet(&[4, 0, 4, 0, 6, 0, 0, 0]).await;
        manager
            .receive_packet(&[4, 0, 4, 0, 0x0e, 0, 0, 0, 0, 0, 0, 0, 0])
            .await;
        wait_routing_ready(&manager).await.unwrap();
        manager.disconnect().await;
        assert!(wait_routing_ready(&manager).await.is_err());
    }

    #[tokio::test]
    async fn connected_hosts_count_and_truncation() {
        let manager = AACPManager::new();
        let packet = [
            4, 0, 4, 0, 0x2e, 0, 0, 0, 2, 1, 2, 3, 4, 5, 6, 0, 0, 7, 8, 9, 10, 11, 12, 1, 0,
        ];
        for end in 0..packet.len() {
            manager.receive_packet(&packet[..end]).await;
        }
        assert!(manager.state.lock().await.connected_devices.is_empty());
        manager.receive_packet(&packet).await;
        let state = manager.state.lock().await;
        assert_eq!(state.connected_devices.len(), 2);
        assert_eq!(state.connected_devices[1].mac, "07:08:09:0A:0B:0C");
    }

    #[tokio::test]
    async fn unknown_activity_must_not_become_idle() {
        let manager = AACPManager::new();
        manager
            .receive_packet(&[4, 0, 4, 0, 0x0e, 0, 0, 0, 0, 0, 0, 0, 0x42])
            .await;
        assert_eq!(
            manager
                .state
                .lock()
                .await
                .audio_source
                .as_ref()
                .unwrap()
                .r#type,
            AudioSourceType::Unknown
        );
    }

    #[tokio::test]
    async fn short_notifications_do_not_panic_or_claim_ownership() {
        let manager = AACPManager::new();
        for opcode in [0x06, 0x09, 0x0e, 0x11, 0x1d, 0x2e] {
            let packet = [4, 0, 4, 0, opcode, 0, 0];
            for end in 0..=packet.len() {
                manager.receive_packet(&packet[..end]).await;
            }
        }
        assert!(!take_allowed(
            &*manager.state.lock().await,
            "01:02:03:04:05:06"
        ));
    }

    #[tokio::test]
    async fn take_requires_idle_worn_and_both_hosts() {
        let manager = AACPManager::new();
        manager
            .receive_packet(&[
                4, 0, 4, 0, 0x2e, 0, 0, 0, 2, 1, 2, 3, 4, 5, 6, 0, 0, 7, 8, 9, 10, 11, 12, 1, 0,
            ])
            .await;
        manager.receive_packet(&[4, 0, 4, 0, 6, 0, 0, 0]).await;
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        manager.state.lock().await.sender = Some(sender);
        let local = "01:02:03:04:05:06";
        assert!(!take_allowed(&*manager.state.lock().await, local));
        for activity in [0, 1, 2, 0xff] {
            manager
                .receive_packet(&[4, 0, 4, 0, 0x0e, 0, 0, 0, 0, 0, 0, 0, activity])
                .await;
            assert_eq!(
                take_allowed(&*manager.state.lock().await, local),
                activity == 0
            );
        }
        manager
            .receive_packet(&[4, 0, 4, 0, 0x0e, 0, 0, 0, 0, 0, 0, 0, 0])
            .await;
        manager.state.lock().await.ear_detection_status.clear();
        assert!(!take_allowed(&*manager.state.lock().await, local));
        manager
            .state
            .lock()
            .await
            .ear_detection_status
            .push(EarDetectionStatus::InEar);
        manager.state.lock().await.connected_devices.pop();
        assert!(!take_allowed(&*manager.state.lock().await, local));
    }

    #[tokio::test]
    async fn take_allowed_with_three_hosts() {
        // Linux plus two remotes (hardware #22: iPhone + Mac + Linux).
        let manager = AACPManager::new();
        manager
            .receive_packet(&[
                4, 0, 4, 0, 0x2e, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 0, 0, 7, 8, 9, 10, 11, 12, 1, 0,
                13, 14, 15, 16, 17, 18, 1, 0,
            ])
            .await;
        manager.receive_packet(&[4, 0, 4, 0, 6, 0, 0, 0]).await;
        manager
            .receive_packet(&[4, 0, 4, 0, 0x0e, 0, 0, 0, 0, 0, 0, 0, 0])
            .await;
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        manager.state.lock().await.sender = Some(sender);
        let local = "01:02:03:04:05:06";
        let state = manager.state.lock().await;
        assert_eq!(state.connected_devices.len(), 3);
        assert_eq!(state.connected_devices[2].mac, "0D:0E:0F:10:11:12");
        assert!(
            state
                .ear_detection_status
                .contains(&EarDetectionStatus::InEar)
        );
        assert!(take_allowed(&state, local));
    }

    #[tokio::test]
    async fn disconnect_invalidates_routing_and_wear_before_reconnect() {
        let manager = AACPManager::new();
        manager
            .receive_packet(&[
                4, 0, 4, 0, 0x2e, 0, 0, 0, 2, 1, 2, 3, 4, 5, 6, 0, 0, 7, 8, 9, 10, 11, 12, 1, 0,
            ])
            .await;
        manager.receive_packet(&[4, 0, 4, 0, 6, 0, 0, 0]).await;
        manager
            .receive_packet(&[4, 0, 4, 0, 9, 0, 6, 1, 0, 0, 0])
            .await;
        manager
            .receive_packet(&[4, 0, 4, 0, 0x0e, 0, 0, 0, 0, 0, 0, 0, 0])
            .await;
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        manager.state.lock().await.sender = Some(sender.clone());
        assert!(take_allowed(
            &*manager.state.lock().await,
            "01:02:03:04:05:06"
        ));
        manager.disconnect().await;
        let mut state = manager.state.lock().await;
        assert!(state.sender.is_none());
        assert!(!state.owns);
        assert!(state.control_command_status_list.is_empty());
        assert!(state.audio_source.is_none());
        assert!(state.ear_detection_status.is_empty());
        assert!(state.connected_devices.is_empty());
        state.sender = Some(sender);
        assert!(!take_allowed(&state, "01:02:03:04:05:06"));
    }

    fn cli_parse(args: &[&str]) -> std::result::Result<crate::Args, clap::Error> {
        <crate::Args as clap::Parser>::try_parse_from(args)
    }

    #[test]
    fn cli_default_startup_has_no_handoff_mode() {
        let args = cli_parse(&["librepods"]).unwrap();
        assert!(args.handoff_probe.is_none());
        assert!(!args.handoff_auto);

        let args = cli_parse(&["librepods", "--no-tray", "--start-minimized", "-d"]).unwrap();
        assert!(args.handoff_probe.is_none());
        assert!(!args.handoff_auto);
        assert!(args.no_tray);
        assert!(args.start_minimized);
        assert!(args.debug);
    }

    #[test]
    fn cli_handoff_auto_requires_handoff_probe() {
        assert!(cli_parse(&["librepods", "--handoff-auto"]).is_err());
    }

    #[test]
    fn cli_handoff_report_parses() {
        assert!(
            cli_parse(&["librepods", "--handoff-report"])
                .unwrap()
                .handoff_report
        );
        assert!(!cli_parse(&["librepods"]).unwrap().handoff_report);
    }

    #[test]
    fn cli_handoff_report_conflicts_with_probe() {
        let error = cli_parse(&[
            "librepods",
            "--handoff-report",
            "--handoff-probe",
            "AA:BB:CC:DD:EE:FF",
        ])
        .err()
        .expect("--handoff-report with --handoff-probe must not parse");
        assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn cli_handoff_probe_with_auto_parses_address() {
        let args = cli_parse(&[
            "librepods",
            "--handoff-probe",
            "AA:BB:CC:DD:EE:FF",
            "--handoff-auto",
        ])
        .unwrap();
        assert_eq!(
            args.handoff_probe,
            Some(bluer::Address::new([0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]))
        );
        assert!(args.handoff_auto);
    }

    // What the REPL `take` prints, straight from the core (#42): with no live
    // control session it is the NO_SESSION WAIT line, whatever the policy
    // skip reason (case recovery is checked first).
    use crate::bluetooth::aacp::{AudioSource, ConnectedDevice};

    const CORE_LOCAL: &str = "01:02:03:04:05:06";
    const CORE_REMOTE: &str = "07:08:09:0A:0B:0C";
    const NO_SESSION_WAIT: &str = "WAIT no live control session";

    fn core_device(mac: &str) -> ConnectedDevice {
        ConnectedDevice {
            mac: mac.to_string(),
            info1: 0,
            info2: 0,
            r#type: None,
        }
    }

    /// Linux plus a remote host, both buds in ear, idle audio and Linux not
    /// owning: a take on this session would be allowed.
    fn feed_worn_idle_session(core: &mut Handoff, now: Instant) {
        core.handle(
            now,
            Input::ConnectedDevices {
                old: Vec::new(),
                new: vec![core_device(CORE_LOCAL), core_device(CORE_REMOTE)],
            },
        );
        core.handle(
            now,
            Input::EarState(vec![EarDetectionStatus::InEar, EarDetectionStatus::InEar]),
        );
        core.handle(
            now,
            Input::AudioSource(AudioSource {
                mac: "00:00:00:00:00:00".to_string(),
                r#type: AudioSourceType::None,
            }),
        );
        core.handle(now, Input::Ownership(vec![0]));
    }

    #[test]
    fn core_take_during_case_recovery_waits() {
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let mut core = Handoff::new(CORE_LOCAL.to_string(), t0, true, true);
        feed_worn_idle_session(&mut core, t0);
        assert_eq!(
            core.handle(
                at(20),
                Input::EarState(vec![EarDetectionStatus::InCase, EarDetectionStatus::InCase]),
            ),
            []
        );
        assert_eq!(
            core.handle(at(100), Input::Disconnected),
            [Command::DropSession]
        );
        core.take_lines();

        assert_eq!(core.handle(at(200), Input::Take), []);
        let lines = core.take_lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].starts_with(NO_SESSION_WAIT), "{lines:?}");
        assert!(!lines.iter().any(|l| l.starts_with("BLOCKED")), "{lines:?}");
    }

    #[test]
    fn core_take_without_session_at_startup_waits() {
        let t0 = Instant::now();
        let mut core = Handoff::new(CORE_LOCAL.to_string(), t0, true, false);
        feed_worn_idle_session(&mut core, t0);
        core.take_lines();

        assert_eq!(core.handle(t0, Input::Take), []);
        let lines = core.take_lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].starts_with(NO_SESSION_WAIT), "{lines:?}");
        assert!(!lines.iter().any(|l| l.starts_with("BLOCKED")), "{lines:?}");
    }

    #[test]
    fn core_take_on_lost_session_waits() {
        let t0 = Instant::now();
        let mut core = Handoff::new(CORE_LOCAL.to_string(), t0, true, true);
        feed_worn_idle_session(&mut core, t0);
        assert_eq!(core.handle(t0, Input::SessionLost), []);
        core.take_lines();

        assert_eq!(core.handle(t0, Input::Take), []);
        let lines = core.take_lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].starts_with(NO_SESSION_WAIT), "{lines:?}");
    }

    #[test]
    fn core_take_live_blocked_by_wear_still_blocked() {
        let t0 = Instant::now();
        let mut core = Handoff::new(CORE_LOCAL.to_string(), t0, true, true);
        feed_worn_idle_session(&mut core, t0);
        assert_eq!(
            core.handle(
                t0,
                Input::EarState(vec![
                    EarDetectionStatus::OutOfEar,
                    EarDetectionStatus::InCase,
                ]),
            ),
            []
        );
        core.take_lines();

        assert_eq!(core.handle(t0, Input::Take), []);
        let lines = core.take_lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].starts_with("BLOCKED take needs Linux plus a remote host"),
            "{lines:?}"
        );
    }

    fn owns_event(value: u8) -> AACPEvent {
        AACPEvent::ControlCommand(crate::bluetooth::aacp::ControlCommandStatus {
            identifier: Control::OwnsConnection,
            value: vec![value],
        })
    }

    fn hosts_event() -> AACPEvent {
        AACPEvent::ConnectedDevices(
            Vec::new(),
            vec![core_device(CORE_LOCAL), core_device(CORE_REMOTE)],
        )
    }

    fn named(events: &[AACPEvent]) -> Vec<String> {
        events.iter().map(|e| format!("{e:?}")).collect()
    }

    /// CONTROL_COMMAND packets: OwnsConnection = 0 and = 1.
    const OWNS_0_PACKET: [u8; 11] = [4, 0, 4, 0, 9, 0, 6, 0, 0, 0, 0];
    const OWNS_1_PACKET: [u8; 11] = [4, 0, 4, 0, 9, 0, 6, 1, 0, 0, 0];

    /// A manager whose cached owner is [1] (Linux), set with no event
    /// channel so nothing is queued: the final "not owning" is a release.
    async fn linux_owned_manager() -> AACPManager {
        let manager = AACPManager::new();
        manager.receive_packet(&OWNS_1_PACKET).await;
        manager
    }

    /// Collect a backlog, with the cached owner [1], after hosts and
    /// connect_control's [0] answer. The send queues `during` while
    /// suspended, and a spawned task queues the echo [0] and a later [0]
    /// only once the send has returned.
    async fn backlog_around_send(during: Option<AACPEvent>) -> InitBacklog {
        let manager = linux_owned_manager().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(hosts_event()).unwrap();
        tx.send(owns_event(0)).unwrap();
        let (returned, after) = tokio::sync::oneshot::channel::<()>();
        let echo_tx = tx.clone();
        tokio::spawn(async move {
            after.await.unwrap();
            echo_tx.send(owns_event(0)).unwrap();
            echo_tx.send(owns_event(0)).unwrap();
        });
        let send = async {
            if let Some(event) = during {
                tx.send(event).unwrap();
                tokio::task::yield_now().await;
            }
            returned.send(()).unwrap();
            Ok::<(), std::io::Error>(())
        };
        let backlog = collect_init_backlog(&manager, &mut rx, send).await.unwrap();
        // The later [0] is fresh: it stays queued for the core.
        assert_eq!(named(&[rx.try_recv().unwrap()]), named(&[owns_event(0)]));
        assert!(rx.try_recv().is_err());
        backlog
    }

    #[tokio::test]
    async fn init_backlog_ends_at_echo_of_final_not_owning() {
        let backlog = backlog_around_send(None).await;
        assert!(backlog.drained);
        assert_eq!(
            named(&backlog.events),
            named(&[hosts_event(), owns_event(0), owns_event(0)])
        );
    }

    #[tokio::test]
    async fn init_backlog_report_during_suspended_send_is_pre_send() {
        let backlog = backlog_around_send(Some(owns_event(0))).await;
        assert!(backlog.drained);
        assert_eq!(
            named(&backlog.events),
            named(&[hosts_event(), owns_event(0), owns_event(0), owns_event(0)])
        );
    }

    /// #49 on a release: after the boundary, a genuine [0] during the
    /// reconnect take is a loss and yields once, not a stale report.
    #[tokio::test]
    async fn release_echo_boundary_ends_reconnect_take_window() {
        let inputs = adoption_inputs(backlog_around_send(None).await);
        let (mut core, at) = reconnect_take_core(ready_facts(vec![0]), inputs);
        assert_eq!(
            core.handle(at, Input::Ownership(vec![0])),
            [Command::PauseMedia]
        );
        let lines = core.take_lines();
        assert!(!lines.iter().any(|l| l.contains("stale")), "{lines:?}");
        assert_eq!(
            lines.iter().filter(|l| l.starts_with("YIELD")).count(),
            1,
            "{lines:?}"
        );
    }

    #[test]
    fn init_backlog_takes_only_the_queue_prefix() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(hosts_event()).unwrap();
        tx.send(owns_event(0)).unwrap();
        tx.send(owns_event(1)).unwrap();
        assert_eq!(
            named(&take_prefix(&mut rx, 2)),
            named(&[hosts_event(), owns_event(0)])
        );
        assert_eq!(named(&[rx.try_recv().unwrap()]), named(&[owns_event(1)]));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn init_backlog_without_echo_has_no_boundary() {
        // Owner [1]: the send is a release and waits for its echo. `tx`
        // stays open, so only the timeout ends the wait (paused time).
        let manager = linux_owned_manager().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(hosts_event()).unwrap();
        tx.send(owns_event(0)).unwrap();
        let backlog =
            collect_init_backlog(&manager, &mut rx, async { Ok::<(), std::io::Error>(()) })
                .await
                .unwrap();
        assert!(!backlog.drained);
        assert_eq!(
            named(&backlog.events),
            named(&[hosts_event(), owns_event(0)])
        );
    }

    /// Linux plus a remote host, both buds in ear, idle audio, and `owner`.
    fn ready_facts(owner: Vec<u8>) -> SessionFacts {
        SessionFacts {
            hosts: vec![core_device(CORE_LOCAL), core_device(CORE_REMOTE)],
            ears: vec![EarDetectionStatus::InEar, EarDetectionStatus::InEar],
            audio: Some(AudioSource {
                mac: "00:00:00:00:00:00".to_string(),
                r#type: AudioSourceType::None,
            }),
            owner: Some(owner),
        }
    }

    /// An automatic core whose session dropped with the case closed, then
    /// reconnected for new playback with `facts` and took ownership. Returns
    /// the core and the time after the reconnect.
    fn reconnect_take_started(facts: SessionFacts) -> (Handoff, Instant) {
        use crate::media_controller::{AUDIO_CATEGORY_IDLE, AUDIO_CATEGORY_STREAM};
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let play = |ms: u64, category: u32| Input::Playback {
            sampled_at: at(ms),
            category,
            new_source: None,
        };
        let mut core = Handoff::new(CORE_LOCAL.to_string(), t0, true, true);
        feed_worn_idle_session(&mut core, t0);
        assert_eq!(core.handle(at(10), play(10, AUDIO_CATEGORY_IDLE)), []);
        let in_case = vec![EarDetectionStatus::InCase, EarDetectionStatus::InCase];
        core.handle(at(20), Input::EarState(in_case));
        assert_eq!(
            core.handle(at(100), Input::Disconnected),
            [Command::DropSession]
        );
        assert_eq!(core.handle(at(500), play(500, AUDIO_CATEGORY_IDLE)), []);
        assert_eq!(core.handle(at(1000), play(1000, AUDIO_CATEGORY_STREAM)), []);
        assert_eq!(
            core.handle(at(1100), Input::Tick),
            [Command::Reconnect { audio: true }]
        );
        let commands = core.handle(at(1200), Input::Reconnected(facts));
        assert!(
            commands.contains(&Command::SendOwnership(true)),
            "{commands:?}"
        );
        core.take_lines();
        (core, at(1300))
    }

    /// `reconnect_take_started`, then adopted `inputs`, which end the
    /// backlog without acking or yielding the take.
    fn reconnect_take_core(facts: SessionFacts, inputs: Vec<Input>) -> (Handoff, Instant) {
        let (mut core, at) = reconnect_take_started(facts);
        for input in inputs {
            assert_eq!(core.handle(at, input), []);
        }
        let lines = core.take_lines();
        assert!(
            !lines
                .iter()
                .any(|l| l.starts_with("ACK owner=") || l.starts_with("YIELD")),
            "{lines:?}"
        );
        assert!(
            lines
                .last()
                .unwrap()
                .starts_with("BACKLOG drained; the reconnect take"),
            "{lines:?}"
        );
        (core, at)
    }

    /// A live [1] after the backlog acks the still pending take once.
    fn live_owner_acks_take(core: &mut Handoff, at: Instant) {
        let commands = core.handle(at, Input::Ownership(vec![1]));
        assert!(!commands.contains(&Command::PauseMedia), "{commands:?}");
        let lines = core.take_lines();
        assert_eq!(
            lines
                .iter()
                .filter(|l| l.starts_with("ACK owner="))
                .collect::<Vec<_>>(),
            ["ACK owner=linux elapsed_ms=100"],
            "{lines:?}"
        );
        assert!(!lines.iter().any(|l| l.starts_with("YIELD")), "{lines:?}");
    }

    /// #360: a stale [1] then the release echo [0] in a drained backlog
    /// neither ack nor yield the reconnect take.
    #[test]
    fn reconnect_take_ignores_backlog_owner_1_then_echo() {
        let backlog = InitBacklog {
            events: vec![hosts_event(), owns_event(1), owns_event(0)],
            drained: true,
        };
        let inputs = skip_backlog_ownership(adoption_inputs(backlog), true);
        let (mut core, at) = reconnect_take_core(ready_facts(vec![0]), inputs);
        live_owner_acks_take(&mut core, at);
    }

    /// #360 end to end: the cached and queued [1], then the release echo [0];
    /// the facts cut already holds [0], so the dropped reports change nothing.
    #[tokio::test]
    async fn reconnect_take_drops_backlog_owner_reports_held_by_facts() {
        let manager = AACPManager::new();
        let (tx, events) = tokio::sync::mpsc::unbounded_channel();
        manager.set_event_channel(tx).await;
        manager.receive_packet(&hosts_packet(&[L_MAC, R_MAC])).await;
        manager.receive_packet(&[4, 0, 4, 0, 6, 0, 0, 0]).await;
        manager
            .receive_packet(&[4, 0, 4, 0, 0x0e, 0, 0, 0, 0, 0, 0, 0, 0])
            .await;
        manager.receive_packet(&OWNS_1_PACKET).await;
        let mut session = (manager, events);
        let (returned, after) = tokio::sync::oneshot::channel::<()>();
        let send = async {
            returned.send(()).unwrap();
            Ok::<(), std::io::Error>(())
        };
        let echo = async {
            after.await.unwrap();
            session.0.receive_packet(&OWNS_0_PACKET).await;
        };
        let (backlog, ()) =
            tokio::join!(collect_init_backlog(&session.0, &mut session.1, send), echo);
        let backlog = backlog.unwrap();
        assert!(backlog.drained);
        let ((_session, backlog), facts) = cut_facts(session, backlog).await;
        assert_eq!(facts.owner, Some(vec![0]));
        let names = named(&backlog.events);
        let owns = |value| {
            names
                .iter()
                .position(|n| *n == named(&[owns_event(value)])[0])
        };
        assert!(owns(1).is_some() && owns(1) < owns(0), "{names:?}");
        let inputs = skip_backlog_ownership(adoption_inputs(backlog), true);
        let (mut core, at) = reconnect_take_core(facts, inputs);
        live_owner_acks_take(&mut core, at);
    }

    /// #360 without a boundary (echo timeout): the queued [1] is dropped, the
    /// late live echo is stale (#43), and the live [1] acks the take once.
    #[test]
    fn reconnect_take_echo_timeout_acks_on_live_owner_1() {
        let backlog = InitBacklog {
            events: vec![hosts_event(), owns_event(1)],
            drained: false,
        };
        let inputs = skip_backlog_ownership(adoption_inputs(backlog), true);
        let (mut core, at) = reconnect_take_started(ready_facts(vec![1]));
        for input in inputs {
            assert_eq!(core.handle(at, input), []);
        }
        assert_eq!(core.handle(at, Input::Ownership(vec![0])), []);
        let lines = core.take_lines();
        assert!(
            lines.iter().any(|l| l.starts_with("OWNER stale")),
            "{lines:?}"
        );
        assert!(
            !lines
                .iter()
                .any(|l| l.starts_with("ACK owner=") || l.starts_with("YIELD")),
            "{lines:?}"
        );
        live_owner_acks_take(&mut core, at);
    }

    /// #360 Q5: only a pending reconnect take drops the backlog's Ownership
    /// inputs; everything else, and the boundary last, is kept in order.
    #[test]
    fn skip_backlog_ownership_only_during_reconnect_take() {
        let inputs = adoption_inputs(InitBacklog {
            events: vec![
                hosts_event(),
                owns_event(1),
                AACPEvent::OwnershipToFalseRequest,
                owns_event(0),
            ],
            drained: true,
        });
        assert!(inputs.contains(&Input::Ownership(vec![1])));
        assert_eq!(skip_backlog_ownership(inputs.clone(), false), inputs);
        let kept = skip_backlog_ownership(inputs, true);
        assert_eq!(
            kept,
            [
                Input::ConnectedDevices {
                    old: Vec::new(),
                    new: vec![core_device(CORE_LOCAL), core_device(CORE_REMOTE)],
                },
                Input::RemoteOwnershipRequest,
                Input::InitBacklogDrained,
            ]
        );
    }

    /// #290: the AirPods report OwnsConnection only on a real change (#159),
    /// so with the cached owner [0] nothing waits for an echo, and the
    /// boundary follows the facts cut.
    #[tokio::test(start_paused = true)]
    async fn reconnect_without_owns_report_drains_without_wait() {
        let manager = AACPManager::new();
        let (tx, events) = tokio::sync::mpsc::unbounded_channel();
        manager.set_event_channel(tx).await;
        manager.receive_packet(&hosts_packet(&[L_MAC, R_MAC])).await;
        manager.receive_packet(&[4, 0, 4, 0, 6, 0, 0, 0]).await;
        manager
            .receive_packet(&[4, 0, 4, 0, 0x0e, 0, 0, 0, 0, 0, 0, 0, 0])
            .await;
        // connect_control's [0] answer.
        manager.receive_packet(&OWNS_0_PACKET).await;
        let mut session = (manager, events);
        let start = tokio::time::Instant::now();
        let backlog = collect_init_backlog(&session.0, &mut session.1, async {
            Ok::<(), std::io::Error>(())
        })
        .await
        .unwrap();
        assert_eq!(tokio::time::Instant::now() - start, Duration::ZERO);
        assert!(backlog.drained);
        let ((_session, backlog), facts) = cut_facts(session, backlog).await;
        assert_eq!(facts, ready_facts(vec![0]));
        let inputs = adoption_inputs(backlog);
        let hosts = inputs
            .iter()
            .position(|i| *i == hosts_input(&[], &[CORE_LOCAL, CORE_REMOTE]));
        let owner = inputs.iter().position(|i| *i == Input::Ownership(vec![0]));
        assert!(hosts.is_some() && hosts < owner, "{inputs:?}");
        assert_eq!(inputs.last(), Some(&Input::InitBacklogDrained));
        reconnect_take_core(facts, inputs);
    }

    #[test]
    fn adoption_inputs_end_with_boundary() {
        let backlog = |drained| InitBacklog {
            events: vec![
                hosts_event(),
                owns_event(0),
                AACPEvent::OwnershipToFalseRequest,
            ],
            drained,
        };
        let events = vec![
            Input::ConnectedDevices {
                old: Vec::new(),
                new: vec![core_device(CORE_LOCAL), core_device(CORE_REMOTE)],
            },
            Input::Ownership(vec![0]),
            Input::RemoteOwnershipRequest,
        ];
        let mut drained = events.clone();
        drained.push(Input::InitBacklogDrained);
        assert_eq!(adoption_inputs(backlog(true)), drained);
        assert_eq!(adoption_inputs(backlog(false)), events);
        assert_eq!(adoption_inputs(InitBacklog::default()), []);
    }

    /// A CONNECTED_DEVICES packet listing `macs`, with zero flags.
    fn hosts_packet(macs: &[[u8; 6]]) -> Vec<u8> {
        let mut packet = vec![4, 0, 4, 0, 0x2e, 0, 0, 0, macs.len() as u8];
        for mac in macs {
            packet.extend(mac);
            packet.extend([0, 0]);
        }
        packet
    }

    /// The (old, new) host lists of the ConnectedDevices events, in order.
    fn host_reports(events: &[AACPEvent]) -> Vec<(&[ConnectedDevice], &[ConnectedDevice])> {
        events
            .iter()
            .filter_map(|event| match event {
                AACPEvent::ConnectedDevices(old, new) => Some((&old[..], &new[..])),
                _ => None,
            })
            .collect()
    }

    /// Report A=[L] is queued, then the cut and report B=[L]->[L,R] queue on
    /// the held state lock in the given order (tokio's Mutex is FIFO).
    /// Returns the facts, the backlog and the events still queued.
    async fn facts_cut_around_report(
        cut_first: bool,
    ) -> (SessionFacts, Vec<AACPEvent>, Vec<AACPEvent>) {
        let manager = AACPManager::new();
        let (tx, events) = tokio::sync::mpsc::unbounded_channel();
        manager.set_event_channel(tx).await;
        manager.receive_packet(&hosts_packet(&[L_MAC])).await;
        let held = manager.state.lock().await;
        let cut = cut_facts((manager.clone(), events), InitBacklog::default());
        let reporter = manager.clone();
        let report = async move {
            reporter
                .receive_packet(&hosts_packet(&[L_MAC, R_MAC]))
                .await
        };
        // current_thread: each yield runs the spawned task up to its lock wait.
        let (cut, report) = if cut_first {
            let cut = tokio::spawn(cut);
            tokio::task::yield_now().await;
            (cut, tokio::spawn(report))
        } else {
            let report = tokio::spawn(report);
            tokio::task::yield_now().await;
            (tokio::spawn(cut), report)
        };
        tokio::task::yield_now().await;
        drop(held);
        let (((_, mut events), backlog), facts) = cut.await.unwrap();
        report.await.unwrap();
        (facts, backlog.events, take_prefix(&mut events, usize::MAX))
    }

    /// (a) The cut queues before B: if it released the lock between the
    /// backlog take and the facts read, B would land in between.
    /// (b) B queues before the cut: if the cut read `events.len()` before
    /// taking the lock, B would be in the facts but left in the queue.
    #[tokio::test]
    async fn reconnect_facts_cut_splits_reports_exactly() {
        let (facts, backlog, rest) = facts_cut_around_report(true).await;
        assert_eq!(facts.hosts, [core_device(CORE_LOCAL)]);
        assert_eq!(host_reports(&backlog).last().unwrap().1, facts.hosts);
        let rest = host_reports(&rest);
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].0, facts.hosts);

        let (facts, backlog, rest) = facts_cut_around_report(false).await;
        assert_eq!(facts.hosts, [CORE_LOCAL, CORE_REMOTE].map(core_device));
        assert_eq!(host_reports(&backlog).last().unwrap().1, facts.hosts);
        assert_eq!(host_reports(&rest), []);
    }

    const L_MAC: [u8; 6] = [1, 2, 3, 4, 5, 6];
    const R_MAC: [u8; 6] = [7, 8, 9, 10, 11, 12];
    const R2_MAC: [u8; 6] = [13, 14, 15, 16, 17, 18];
    /// R2_MAC as a host string, as CORE_LOCAL/CORE_REMOTE are for L_MAC/R_MAC.
    const CORE_R2: &str = "0D:0E:0F:10:11:12";
    const M_MAC: [u8; 6] = [19, 20, 21, 22, 23, 24];

    fn hosts_input(old: &[&str], new: &[&str]) -> Input {
        let hosts = |macs: &[&str]| macs.iter().map(|m| core_device(m)).collect();
        Input::ConnectedDevices {
            old: hosts(old),
            new: hosts(new),
        }
    }

    /// #143/#290: a session at [L], cached owner unknown, sends no echo of
    /// the last "not owning"; it reports A=[L]->[L,R] and
    /// B=[L,R]->[L,R,R2] before the facts are read, and each of `late`
    /// lands after the cut. An automatic core reconnects and adopts it, and
    /// the Announce counts before and after the boundary are returned.
    async fn post_send_reports_announces(late: &[&[[u8; 6]]]) -> (usize, usize) {
        let (l, r, r2) = (CORE_LOCAL, CORE_REMOTE, CORE_R2);
        let manager = AACPManager::new();
        let (tx, mut events) = tokio::sync::mpsc::unbounded_channel();
        manager.set_event_channel(tx).await;
        manager.receive_packet(&hosts_packet(&[L_MAC])).await;
        let (returned, after) = tokio::sync::oneshot::channel::<()>();
        let reporter = manager.clone();
        let reports = tokio::spawn(async move {
            after.await.unwrap();
            reporter
                .receive_packet(&hosts_packet(&[L_MAC, R_MAC]))
                .await;
            reporter
                .receive_packet(&hosts_packet(&[L_MAC, R_MAC, R2_MAC]))
                .await;
        });
        let send = async {
            returned.send(()).unwrap();
            Ok::<(), std::io::Error>(())
        };
        let backlog = collect_init_backlog(&manager, &mut events, send)
            .await
            .unwrap();
        assert!(backlog.drained);
        reports.await.unwrap();
        let (((manager, mut events), backlog), facts) = cut_facts((manager, events), backlog).await;
        assert_eq!(facts.hosts, [l, r, r2].map(core_device));
        for late in late {
            manager.receive_packet(&hosts_packet(late)).await;
        }

        let mut inputs = vec![Input::Reconnected(facts)];
        inputs.extend(adoption_inputs(backlog));
        let boundary = inputs
            .iter()
            .position(|i| *i == Input::InitBacklogDrained)
            .unwrap();
        let a = inputs.iter().position(|i| *i == hosts_input(&[l], &[l, r]));
        let b = inputs
            .iter()
            .position(|i| *i == hosts_input(&[l, r], &[l, r, r2]));
        assert!(a.is_some() && a < b && b < Some(boundary), "{inputs:?}");
        inputs.extend(
            take_prefix(&mut events, usize::MAX)
                .into_iter()
                .filter_map(event_input),
        );

        let t0 = Instant::now();
        let mut core = Handoff::new(CORE_LOCAL.to_string(), t0, true, true);
        assert_eq!(core.handle(t0, Input::Disconnected), [Command::DropSession]);
        let at = t0 + Duration::from_secs(1);
        assert_eq!(
            core.handle(at, Input::Tick),
            [Command::Reconnect { audio: true }]
        );
        let mut announces = (0, 0);
        for (i, input) in inputs.into_iter().enumerate() {
            let n = core
                .handle(at, input)
                .iter()
                .filter(|c| **c == Command::Announce)
                .count();
            if i < boundary {
                announces.0 += n;
            } else {
                announces.1 += n;
            }
        }
        announces
    }

    #[tokio::test]
    async fn reconnect_post_send_reports_announce_once() {
        assert_eq!(post_send_reports_announces(&[]).await, (1, 0));
    }

    #[tokio::test]
    async fn reconnect_post_send_reports_then_new_host_announce_twice() {
        let late = [L_MAC, R_MAC, R2_MAC, M_MAC];
        assert_eq!(post_send_reports_announces(&[&late]).await, (1, 1));
    }

    /// #290: with no echo the boundary still clears the snapshot hosts, so
    /// R leaving and rejoining after it is announced again.
    #[tokio::test]
    async fn reconnect_snapshot_host_rejoining_after_boundary_announces() {
        let left = [L_MAC, R2_MAC];
        let rejoined = [L_MAC, R_MAC, R2_MAC];
        assert_eq!(
            post_send_reports_announces(&[&left, &rejoined]).await,
            (1, 1)
        );
    }
}
