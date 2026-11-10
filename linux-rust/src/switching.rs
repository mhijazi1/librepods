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
use crate::bluetooth::aacp::{AACPEvent, AACPManager, ControlCommandIdentifiers};
use crate::handoff_log;
use crate::media_controller::{self, MediaController};
use crate::stream_monitor::{self, CallSamples};
use crate::utils::{self, ConnectMode, SwitchingMode};
use log::{error, info};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc, oneshot, watch};

mod inputs;
mod poller;
mod registry;
mod shell;

pub(crate) use inputs::{advertise_call, announce, event_input, session_facts};
use poller::MediaActions;
pub(crate) use poller::{
    NOT_PAUSING, PlaybackSamples, device_media, pause_own_players, spawn_playback_poller,
};
use registry::{HANDOFF_TASKS, spawn_handoff_task, stop_handoff_task};
pub(crate) use registry::{last_owner_linux, set_connect_mode, stop_all_handoff_tasks};
pub(crate) use shell::LineSink;
use shell::Shell;

/// How often the handoff task ticks the core (ACK timeouts) and checks that
/// the control session is still alive.
pub(crate) const TICK: Duration = Duration::from_millis(200);
/// Interval between playback polls in production.
pub(crate) const PLAYBACK_POLL: Duration = Duration::from_millis(500);
/// How long a replacement task or an exit waits for a stopped task's A2DP
/// hold release to settle (#294).
pub(crate) const SHUTDOWN_RELEASE_WAIT: Duration = Duration::from_secs(2);
const _: () = assert!(SHUTDOWN_RELEASE_WAIT.as_millis() <= 2000);

type ControlTx = mpsc::UnboundedSender<(ControlCommandIdentifiers, Vec<u8>)>;
type Events = mpsc::UnboundedReceiver<AACPEvent>;
type EventTx = mpsc::UnboundedSender<AACPEvent>;
/// A request to the event router to forward everything still in the primary
/// queue; answered once it has.
type FlushRequest = oneshot::Sender<()>;

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
    /// Relay Linux's audio category (#272); `start` sets it,
    /// `start_with_calls` leaves it off.
    relay_category: bool,
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
        let mut switching =
            Self::start_with_calls(setup, media, playback, stream_monitor::spawn_call_poller)
                .await
                .with_connect_mode(utils::load_connect_mode(&mac));
        switching.a2dp = handoff.then(|| a2dp_resume::spawn_transport_poller(&mac));
        switching.relay_category = true;
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
            relay_category: false,
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
                // Before the router starts, so no reinsertion's report is lost.
                let ear_rx = media.ear_resumes();
                let connect_mode = watch::Sender::new(self.connect_mode);
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
                    ear_rx,
                    resume: None,
                    lines: self.lines,
                    connect_mode_rx: connect_mode.subscribe(),
                    connect_mode,
                    relay_category: self.relay_category,
                    held: false,
                    parked: false,
                    released: None,
                };
                spawn_handoff_task(&HANDOFF_TASKS, self.airpods_mac, shell);
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
mod tests;
