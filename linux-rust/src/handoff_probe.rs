use crate::bluetooth::aacp::{
    AACPEvent, AACPManager, AACPManagerState, ControlCommandIdentifiers as Control,
};
use crate::handoff::{Command, Handoff, Input};
use crate::switching::{
    advertise_call, announce, event_input, session_facts, spawn_playback_poller,
};
use crate::media_controller::{
    CaseRecovery, MediaController, TakeoverAction, TakeoverInputs, TakeoverTrigger,
    decide_takeover, routing_snapshot_of,
};
use bluer::Address;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type ControlSession = (AACPManager, tokio::sync::mpsc::UnboundedReceiver<AACPEvent>);

const A2DP_SINK: uuid::Uuid = uuid::Uuid::from_u128(0x0000110b00001000800000805f9b34fb);

async fn connect_control(address: Address) -> Result<ControlSession> {
    let mut manager = AACPManager::new();
    let (tx, events) = tokio::sync::mpsc::unbounded_channel();
    manager.set_event_channel(tx).await;
    manager.connect(address).await;
    let result = async {
        if manager.state.lock().await.sender.is_none() {
            return Err("AirPods control connection failed".into());
        }
        manager.send_handshake().await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        manager.send_set_feature_flags_packet().await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        manager.send_notification_request().await?;
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
    play_rx: tokio::sync::mpsc::Receiver<(Instant, bool)>,
    retry: Option<tokio::task::JoinHandle<Result<ControlSession>>>,
    /// Session made by the last reconnect, until the core adopts or cancels it.
    candidate: Option<ControlSession>,
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
                Command::PauseMedia => self.media.pause_all_media().await,
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
                        let result = connect_control(address).await?;
                        if audio {
                            let device = adapter.device(address)?;
                            let connected = tokio::time::timeout(
                                Duration::from_secs(5),
                                device.connect_profile(&A2DP_SINK),
                            )
                            .await;
                            match connected {
                                Ok(Ok(())) => {}
                                _ => {
                                    result.0.disconnect().await;
                                    return Err("A2DP reconnection failed".into());
                                }
                            }
                            result
                                .0
                                .send_control_command(Control::OwnsConnection, &[0])
                                .await?;
                        }
                        Ok(result)
                    }));
                }
                Command::AdoptSession => {
                    if let Some((manager, events)) = self.candidate.take() {
                        self.manager = manager;
                        self.events = events;
                    }
                }
                Command::CancelReconnect => {
                    if let Some((manager, _)) = self.candidate.take() {
                        manager.disconnect().await;
                    }
                    self.adapter.device(self.address)?.disconnect().await?;
                }
                Command::DrainPlayback => while self.play_rx.try_recv().is_ok() {},
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
    let commands = core.handle(Instant::now(), input);
    for line in core.take_lines() {
        println!("{line}");
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

/// Wait for the running reconnect attempt; never resolves without one.
async fn reconnect_result(
    retry: &mut Option<tokio::task::JoinHandle<Result<ControlSession>>>,
) -> std::result::Result<Result<ControlSession>, tokio::task::JoinError> {
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
            Some((sampled_at, playing)) = io.play_rx.recv() => {
                if sync_session(&mut core, &mut io).await?
                    || step(&mut core, &mut io, Input::Playback { sampled_at, playing }).await?
                {
                    break;
                }
            }
            result = reconnect_result(&mut io.retry) => {
                io.retry = None;
                let input = match result? {
                    Ok((manager, events)) => {
                        let facts = session_facts(&*manager.state.lock().await);
                        io.candidate = Some((manager, events));
                        Input::Reconnected(facts)
                    }
                    Err(error) => Input::ReconnectFailed(error.to_string()),
                };
                if step(&mut core, &mut io, input).await? {
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
}
