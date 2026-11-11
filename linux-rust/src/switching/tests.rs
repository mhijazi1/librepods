//! Shared fixtures; each area's tests live in `tests/<area>.rs`.

use super::inputs::*;
use super::poller::*;
use super::registry::*;
use super::shell::*;
use super::*;
use crate::activity::{ActivityInputs, ActivitySource};
use crate::bluetooth::aacp::ControlCommandIdentifiers as Control;
use crate::bluetooth::aacp::{
    AudioSource, AudioSourceType, ConnectedDevice, ControlCommandStatus, EarDetectionStatus,
    TipiFields, encode_media_information_call,
};
use crate::call_detect::{StreamDirection, StreamProps};
use crate::default_sink::PARK_SINK;
use crate::default_sink::tests::{DAC, FakeSinks};
use crate::default_sink::{DefaultSinkRouter, SinkEvent, UnparkTo};
use crate::handoff::{Command, Handoff, Input, SessionFacts};
use crate::media_controller::{AUDIO_CATEGORY_IDLE, AUDIO_CATEGORY_MEDIA, AUDIO_CATEGORY_STREAM};
use crate::reconnect;
use crate::stream_monitor::{
    AllAudible, MIN_STREAM_PLAYBACK, PeakSource, PlaybackSource, PlaybackState,
};
use futures::future::BoxFuture;
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex as StdMutex};
use std::time::Instant;
use tokio::sync::{Notify, watch};
use tokio::task::AbortHandle;

mod inputs;
/// #20: the non-events of upstream #724's reports never take the AirPods
/// or pause anything. Each runs through the real stream tracker and core
/// at injected times, with a counterfactual twin that does take.
mod non_event;
mod policy;
mod poller;
mod registry;
mod shell;

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
    /// `routes.len()` at each resume chain spawn (#264).
    resume_spawns: StdMutex<Vec<usize>>,
    /// The players given to each resume chain spawn (#351).
    resume_players: StdMutex<Vec<Vec<(String, Option<u32>)>>>,
    /// While set, `hold_settled` waits for one `gate` permit (#294).
    gated: AtomicBool,
    gate: Notify,
    /// Record the park routes (#311) too.
    parks: AtomicBool,
    /// Panic right after recording a `Take` route (#311).
    panic_at_take: AtomicBool,
    /// `pause` never completes (#534).
    hung_pause: AtomicBool,
    /// `routes.len()` at each `RouterMedia::hold_settled` (#544).
    settles: StdMutex<Vec<usize>>,
}

/// Records the media actions instead of touching D-Bus or PipeWire. The
/// record is made when the shell asks, before the future runs.
struct FakeMedia {
    records: Arc<Records>,
    mac: String,
}

impl MediaActions for FakeMedia {
    fn pause(&self, _: PlaybackState) -> BoxFuture<'static, bool> {
        self.records.pauses.fetch_add(1, Ordering::SeqCst);
        trace(&self.mac, "PauseMedia".to_string());
        if self.records.hung_pause.load(Ordering::SeqCst) {
            return Box::pin(std::future::pending());
        }
        Box::pin(async { true })
    }

    fn activate_a2dp(&self) -> BoxFuture<'static, ()> {
        self.records.activations.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {})
    }

    fn resume_a2dp(
        &self,
        streams: PlaybackState,
        players: Vec<(String, Option<u32>)>,
        until: Instant,
        cancel: oneshot::Receiver<()>,
    ) -> BoxFuture<'static, ()> {
        let routed = self.records.routes.lock().unwrap().len();
        self.records.resume_spawns.lock().unwrap().push(routed);
        self.records
            .resume_players
            .lock()
            .unwrap()
            .push(players.clone());
        let io = FakeResume {
            mac: self.mac.clone(),
        };
        Box::pin(a2dp_resume::run(
            io,
            streams,
            players,
            until,
            cancel,
            std::future::ready(()),
        ))
    }

    fn route_default_sink(&self, event: SinkEvent) {
        // Recorded only for the tests that opt in, so the others'
        // sequences stay as they were before the park (#311).
        if matches!(event, SinkEvent::Park | SinkEvent::Unpark(_))
            && !self.records.parks.load(Ordering::SeqCst)
        {
            return;
        }
        self.records.routes.lock().unwrap().push(event);
        if event == SinkEvent::Take && self.records.panic_at_take.load(Ordering::SeqCst) {
            panic!("test: a failure right after the take's route");
        }
    }

    fn hold_a2dp(&self, hold: bool) {
        self.records
            .routes
            .lock()
            .unwrap()
            .push(SinkEvent::Hold(hold));
    }

    fn hold_settled(&self) -> BoxFuture<'static, ()> {
        let records = self.records.clone();
        Box::pin(async move {
            if records.gated.load(Ordering::SeqCst) {
                records.gate.notified().await;
            }
        })
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
    /// Harness setup, not handler behavior: the core relays categories (#505).
    relay_category: bool,
    /// Harness setup: the announced tipi's activity source (#13).
    activity: Option<Arc<dyn ActivitySource>>,
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

/// `ready_session` whose audio source report is `Unknown` (no usable
/// source): the startup's connect take (#324) needs a confirmed idle
/// source and the state is complete, so the startup sends "not owning"
/// at once (#452) and the playback take stays under test.
/// `expect_startup` then reports the idle source.
async fn no_source_session() -> (AACPManager, mpsc::Receiver<Vec<u8>>, mpsc::Sender<Vec<u8>>) {
    let session = ready_session().await;
    session.0.state.lock().await.audio_source = Some(AudioSource {
        mac: "00:00:00:00:00:00".to_string(),
        r#type: AudioSourceType::Unknown,
    });
    session
}

/// The idle audio source report that replaces `no_source_session`'s
/// `Unknown` one.
fn idle_source() -> AACPEvent {
    AACPEvent::AudioSource(AudioSource {
        mac: "00:00:00:00:00:00".to_string(),
        r#type: AudioSourceType::None,
    })
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
    reference(|m| async move {
        m.send_add_tipi_device(LOCAL, remote, TipiFields::LEGACY)
            .await
    })
    .await
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
    play_tx: mpsc::Sender<(Instant, u32, Option<u32>)>,
    playback_started: Arc<AtomicBool>,
    /// The injected call monitor's sender (no sound server in tests).
    call_tx: mpsc::Sender<(Instant, crate::call_detect::CallState)>,
    /// The injected A2DP transport poller's sender (no BlueZ in tests).
    a2dp_tx: mpsc::Sender<bool>,
    /// Undo clicks by yield id, as the desktop notifier sends them (#17).
    undo_tx: mpsc::UnboundedSender<u64>,
    /// What the task posted, in order (#17).
    notices: Arc<StdMutex<Vec<crate::notifications::Notice>>>,
    /// The sender given to `set_event_channel`.
    events: EventTx,
    handler: Arc<DeviceHandler>,
}

impl Harness {
    /// Fresh records and a fake media adapter and controller for `mac`.
    fn fake_media(
        mac: &str,
    ) -> (
        Arc<Records>,
        (Arc<Mutex<MediaController>>, Arc<dyn MediaActions>),
    ) {
        let records = Arc::new(Records::default());
        let media = Arc::new(FakeMedia {
            records: records.clone(),
            mac: mac.to_string(),
        });
        let media_controller = Arc::new(Mutex::new(MediaController::new(
            mac.to_string(),
            LOCAL.to_string(),
        )));
        (records, (media_controller, media))
    }

    /// The sender `play` uses and the playback samples it feeds.
    fn play_channel() -> (
        mpsc::Sender<(Instant, u32, Option<u32>)>,
        impl FnOnce() -> PlaybackSamples,
    ) {
        let (play_tx, play_rx) = mpsc::channel(8);
        (play_tx, move || PlaybackSamples {
            rx: play_rx,
            rebaseline: Default::default(),
            players: Default::default(),
            streams: watch::channel(Default::default()).1,
        })
    }

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
        let (records, media) = Self::fake_media(mac);
        let handler = DeviceHandler::default();
        handler.block_ear.store(block_ear, Ordering::SeqCst);
        Self::start_wired(mode, mac, manager, media, records, Arc::new(handler)).await
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
        Self::start_playback(
            mode,
            mac,
            manager,
            media,
            records,
            handler,
            Self::play_channel(),
            ConnectMode::default(),
        )
        .await
    }

    /// `start` in handoff mode with "Connect to this computer" set to
    /// `connect_mode` (#16).
    async fn start_connect(connect_mode: ConnectMode, mac: &str, manager: &AACPManager) -> Self {
        let (records, media) = Self::fake_media(mac);
        Self::start_playback(
            SwitchingMode::Handoff,
            mac,
            manager,
            media,
            records,
            Arc::new(DeviceHandler::default()),
            Self::play_channel(),
            connect_mode,
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
        M: Fn() -> Vec<(String, Option<u32>)> + Send + Sync + 'static,
        S: PlaybackSource + 'static,
    {
        let (records, media) = Self::fake_media(mac);
        let (play_tx, _) = mpsc::channel(1);
        Self::start_playback(
            SwitchingMode::Handoff,
            mac,
            manager,
            media,
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
            ConnectMode::default(),
        )
        .await
    }

    /// Start with the given playback samples and the sender `play` uses,
    /// in `connect_mode`.
    async fn start_playback(
        mode: SwitchingMode,
        mac: &str,
        manager: &AACPManager,
        (media_controller, media): (Arc<Mutex<MediaController>>, Arc<dyn MediaActions>),
        records: Arc<Records>,
        handler: Arc<DeviceHandler>,
        (play_tx, playback): (
            mpsc::Sender<(Instant, u32, Option<u32>)>,
            impl FnOnce() -> PlaybackSamples,
        ),
        connect_mode: ConnectMode,
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
        .with_line_sink(sink)
        .with_connect_mode(connect_mode);
        switching.a2dp = Some(a2dp_rx);
        let (undo_tx, undo_rx) = mpsc::unbounded_channel();
        switching.undo = Some(undo_rx);
        let notices = Arc::new(StdMutex::new(Vec::new()));
        let posted = notices.clone();
        switching.notifier = Arc::new(move |notice: crate::notifications::Notice| {
            posted.lock().unwrap().push(notice)
        });
        switching.relay_category = handler.relay_category;
        if let Some(source) = handler.activity.clone() {
            switching = switching.with_activity(source);
        }
        switching.dispatch_events(events_rx, handler.clone().handle());
        Harness {
            records,
            play_tx,
            playback_started,
            call_tx,
            a2dp_tx,
            undo_tx,
            notices,
            events,
            handler,
        }
    }

    /// Emit an event the way the manager does.
    fn emit(&self, event: AACPEvent) {
        self.events.send(event).expect("event channel open");
    }

    /// `emit`, then wait until the device handler has started the event:
    /// the router queued it for the core first, and the core steps queued
    /// events before later playback samples.
    async fn emit_queued(&self, event: AACPEvent) {
        let name = format!("{event:?}");
        let seen = || {
            self.handler
                .started()
                .iter()
                .filter(|e| **e == name)
                .count()
        };
        let before = seen();
        self.emit(event);
        wait_until("the device handler to start the event", || seen() > before).await;
    }

    /// Drop the harness's sender of the primary event channel.
    fn close_events(&mut self) {
        self.events = mpsc::unbounded_channel().0;
    }

    /// A call state change from the call monitor.
    async fn call(&self, in_call: bool) {
        self.call_state(in_call.into()).await;
    }

    /// A call monitor state, mic use included (#505).
    async fn call_state(&self, state: crate::call_detect::CallState) {
        let _ = self.call_tx.send((Instant::now(), state)).await;
    }

    /// An A2DP transport state from the transport poller.
    async fn a2dp(&self, active: bool) {
        let _ = self.a2dp_tx.send(active).await;
    }

    /// A sample from a local MPRIS player (301, with its live stream)
    /// playing, or idle (100).
    async fn play(&self, playing: bool) {
        self.play_category(local_category(playing, playing, false))
            .await;
    }

    async fn play_category(&self, category: u32) {
        // A closed channel (no handoff task) drops the sample.
        let _ = self.play_tx.send((Instant::now(), category, None)).await;
    }

    fn pauses(&self) -> usize {
        self.records.pauses.load(Ordering::SeqCst)
    }

    fn activations(&self) -> usize {
        self.records.activations.load(Ordering::SeqCst)
    }

    fn routes(&self) -> Vec<SinkEvent> {
        self.sink_events()
            .into_iter()
            .filter(|e| !matches!(e, SinkEvent::Hold(_)))
            .collect()
    }

    /// The routes and A2DP holds (#264), in the order the shell asked.
    fn sink_events(&self) -> Vec<SinkEvent> {
        self.records.routes.lock().unwrap().clone()
    }
}

/// Startup on a `no_source_session` (or one whose connect mode blocks
/// the connect take): the core's automatic start, [ConnectA2dp,
/// SendOwnership(false), Announce], and nothing else; then the idle
/// source report that replaces `no_source_session`'s `Unknown` one.
async fn expect_startup(h: &Harness, rx: &mut mpsc::Receiver<Vec<u8>>) {
    assert_eq!(next_packet(rx).await, expected_owns(0).await);
    assert_eq!(next_packet(rx).await, expected_new_device(REMOTE).await);
    assert_eq!(next_packet(rx).await, expected_tipi(REMOTE).await);
    assert_eq!(rest(rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.activations(), 1);
    assert_eq!(h.pauses(), 0);
    h.emit_queued(idle_source()).await;
}

/// The AirPods' own owner report, [0], stepped before playback stopped
/// then started: one take, OwnsConnection [1] only.
async fn expect_take(h: &Harness, rx: &mut mpsc::Receiver<Vec<u8>>) {
    h.emit_queued(owns(0)).await;
    h.play(false).await;
    h.play(true).await;
    assert_eq!(next_packet(rx).await, expected_owns(1).await);
    let rest = rest(rx).await;
    assert_eq!(rest, Vec::<Vec<u8>>::new());
}

/// A task's `Shell` on fake media, with no samples.
fn bare_shell(mac: &str, manager: AACPManager, events: Events) -> Shell {
    let (_records, (_, media)) = Harness::fake_media(mac);
    let connect_mode = watch::Sender::new(ConnectMode::default());
    Shell {
        mac: mac.to_string(),
        local: LOCAL.to_string(),
        manager,
        media,
        events,
        flush: mpsc::unbounded_channel().0,
        play_rx: Harness::play_channel().1(),
        call_rx: mpsc::channel(1).1,
        a2dp_rx: mpsc::channel(1).1,
        ear_rx: mpsc::unbounded_channel().1,
        resume: None,
        lines: None,
        connect_mode_rx: connect_mode.subscribe(),
        connect_mode,
        relay_category: false,
        held: false,
        parked: false,
        released: None,
        notifier: Arc::new(|_: crate::notifications::Notice| {}),
        notifications: Arc::new(true.into()),
        undo_rx: mpsc::unbounded_channel().1,
        activity: Arc::new(|| None),
        sleep_disconnected: None,
        // No BlueZ in tests.
        sleep_disconnect: Arc::new(|_| Box::pin(async { false })),
        sleepers: Box::leak(Box::default()),
    }
}

/// The bus owner PIDs of a Spotify Connect remote and of Firefox in the
/// yield pause tests.
const CONNECT_PID: u32 = 99_999_002;
const FIREFOX_PID: u32 = 99_999_003;

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

/// The process of the test player: its MPRIS bus owner and the owner of
/// the test streams (#329). No such process, so /proc names no parent.
const PLAYER_PID: u32 = 99_999_001;

/// The MPRIS sample: the test player while `playing`, else nothing.
fn player(playing: bool) -> Vec<Option<u32>> {
    playing.then_some(Some(PLAYER_PID)).into_iter().collect()
}

/// `player` with the player's name, as the poller's MPRIS source gives it.
fn named_player(playing: bool) -> Vec<(String, Option<u32>)> {
    let owners = player(playing).into_iter();
    owners.map(|owner| (PLAYER.to_string(), owner)).collect()
}

fn sink_input(index: u32, role: &str) -> StreamProps {
    StreamProps {
        direction: StreamDirection::Playback,
        props: [
            ("media.role".to_string(), role.to_string()),
            ("application.name".to_string(), "test player".to_string()),
            ("application.process.id".to_string(), PLAYER_PID.to_string()),
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

/// The sink events once `mac`'s task has stopped and settled.
async fn after_stop(h: &Harness, task: AbortHandle) -> Vec<SinkEvent> {
    wait_until("the task to stop", || task.is_finished()).await;
    tokio::time::sleep(SETTLE).await;
    h.sink_events()
}

/// Moves these MACs' tasks and pending releases out of the process-wide
/// registry, so a test's shutdown leaves the other tests alone.
fn private_registry(macs: &[&str]) -> &'static StdMutex<Registry> {
    let mut global = HANDOFF_TASKS.lock().unwrap();
    let mut private = Registry::default();
    for &mac in macs {
        if let Some(task) = global.tasks.remove(mac) {
            private.tasks.insert(mac.to_string(), task);
        }
        if let Some(pending) = global.releases.remove(mac) {
            private.releases.insert(mac.to_string(), pending);
        }
    }
    Box::leak(Box::new(StdMutex::new(private)))
}

// #311: the park sink.

/// `Harness::play_channel` with the streams known: a listing showed
/// none.
fn known_play_channel() -> (
    mpsc::Sender<(Instant, u32, Option<u32>)>,
    impl FnOnce() -> PlaybackSamples,
) {
    let (play_tx, play_rx) = mpsc::channel(8);
    let state = PlaybackState {
        live: Some(HashSet::new()),
        ..Default::default()
    };
    let streams = watch::channel((state, Vec::new())).1;
    (play_tx, move || PlaybackSamples {
        rx: play_rx,
        rebaseline: Default::default(),
        players: Default::default(),
        streams,
    })
}

/// Another host's request answered while Linux is idle: held, then
/// parked once a sample shows the idle (a harness with no playback
/// task drops it).
async fn park_after_remote_request(h: &Harness, rx: &mut mpsc::Receiver<Vec<u8>>) {
    h.play(false).await;
    h.emit(AACPEvent::OwnershipToFalseRequest);
    assert_eq!(next_packet(rx).await, expected_owns(0).await);
    // Its ACK, so a later play edge can take.
    h.emit_queued(owns(0)).await;
    wait_until("the park", || h.sink_events().ends_with(&[SinkEvent::Park])).await;
    use SinkEvent::{Hold, Park};
    assert_eq!(h.sink_events(), [Hold(false), Hold(true), Park]);
}

/// `FakeMedia` whose routes and holds also go to a real router.
struct RouterMedia {
    fake: FakeMedia,
    sinks: DefaultSinkRouter,
}

impl MediaActions for RouterMedia {
    fn pause(&self, streams: PlaybackState) -> BoxFuture<'static, bool> {
        self.fake.pause(streams)
    }

    fn activate_a2dp(&self) -> BoxFuture<'static, ()> {
        self.fake.activate_a2dp()
    }

    fn resume_a2dp(
        &self,
        streams: PlaybackState,
        players: Vec<(String, Option<u32>)>,
        until: Instant,
        cancel: oneshot::Receiver<()>,
    ) -> BoxFuture<'static, ()> {
        self.fake.resume_a2dp(streams, players, until, cancel)
    }

    fn route_default_sink(&self, event: SinkEvent) {
        self.fake.route_default_sink(event);
        self.sinks.route(event);
    }

    fn hold_a2dp(&self, hold: bool) {
        self.fake.hold_a2dp(hold);
        self.sinks.route(SinkEvent::Hold(hold));
    }

    fn hold_settled(&self) -> BoxFuture<'static, ()> {
        let records = &self.fake.records;
        let routed = records.routes.lock().unwrap().len();
        records.settles.lock().unwrap().push(routed);
        Box::pin(self.sinks.settled())
    }
}

/// `park_task` on a real router over `FakeSinks` with the `connect`
/// gate, parked by another host's request; then each hold release
/// waits its full `HOLD_RELEASE_WAIT` for the AirPods sink.
async fn router_parked_task(
    mac: &str,
    rx: &mut mpsc::Receiver<Vec<u8>>,
    manager: &AACPManager,
    connect: Option<std::sync::mpsc::Receiver<()>>,
) -> (Harness, FakeSinks) {
    let sinks = FakeSinks::new(false);
    *sinks.connect.lock().unwrap() = connect;
    let records = Arc::new(Records::default());
    records.parks.store(true, Ordering::SeqCst);
    let fake = FakeMedia {
        records: records.clone(),
        mac: mac.to_string(),
    };
    let media: Arc<dyn MediaActions> = Arc::new(RouterMedia {
        fake,
        sinks: DefaultSinkRouter::spawn(sinks.clone()),
    });
    let controller = MediaController::new(mac.to_string(), LOCAL.to_string());
    let media = (Arc::new(Mutex::new(controller)), media);
    let h = Harness::start_playback(
        SwitchingMode::Handoff,
        mac,
        manager,
        media,
        records,
        Arc::new(DeviceHandler::default()),
        known_play_channel(),
        ConnectMode::default(),
    )
    .await;
    expect_startup(&h, rx).await;
    park_after_remote_request(&h, rx).await;
    wait_until("the park sink", || default_sink(&sinks) == PARK_SINK).await;
    sinks.stuck.store(true, Ordering::SeqCst);
    (h, sinks)
}

fn default_sink(sinks: &FakeSinks) -> String {
    sinks.default.lock().unwrap().clone()
}

/// #505: the shell's call branch feeds mic use to the core.
mod t505 {
    use super::*;
    use crate::call_detect::CallState;
    use crate::media_controller::AUDIO_CATEGORY_MIC;

    async fn expected_category(category: u32) -> Vec<u8> {
        reference(|m| async move {
            m.send_media_information_category(LOCAL, REMOTE, category)
                .await
        })
        .await
    }

    #[tokio::test]
    async fn mic_use_relays_200_on_the_wire() {
        let mac = "5A:00:00:00:05:05";
        let (manager, mut rx, _tx) = no_source_session().await;
        let (records, media) = Harness::fake_media(mac);
        let handler = DeviceHandler {
            relay_category: true,
            ..Default::default()
        };
        let h = Harness::start_wired(
            SwitchingMode::Handoff,
            mac,
            &manager,
            media,
            records,
            Arc::new(handler),
        )
        .await;
        expect_startup(&h, &mut rx).await;

        h.call_state(CallState::Mic).await;
        assert_eq!(
            next_packet(&mut rx).await,
            expected_category(AUDIO_CATEGORY_MIC).await
        );
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        h.call_state(CallState::Idle).await;
        assert_eq!(
            next_packet(&mut rx).await,
            expected_category(AUDIO_CATEGORY_IDLE).await
        );
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());

        stop_handoff_task(mac);
    }
}

// #13: the startup's announcement carries the activity source's level.

/// The tipi `mac`'s handoff startup announces to REMOTE when `source`
/// gives `snapshot`.
async fn announced_tipi(mac: &str, snapshot: Option<ActivityInputs>) -> Vec<u8> {
    let (manager, mut rx, _tx) = no_source_session().await;
    let (records, media) = Harness::fake_media(mac);
    let handler = DeviceHandler {
        activity: Some(Arc::new(move || snapshot)),
        ..Default::default()
    };
    let _h = Harness::start_wired(
        SwitchingMode::Handoff,
        mac,
        &manager,
        media,
        records,
        Arc::new(handler),
    )
    .await;
    assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
    assert_eq!(
        next_packet(&mut rx).await,
        expected_new_device(REMOTE).await
    );
    let tipi = next_packet(&mut rx).await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    stop_handoff_task(mac);
    tipi
}

async fn expected_tipi_fields(idle_time: u32, nearby_audio_score: u32) -> Vec<u8> {
    let fields = TipiFields {
        idle_time,
        nearby_audio_score,
    };
    reference(|m| async move { m.send_add_tipi_device(LOCAL, REMOTE, fields).await }).await
}

fn unlocked_since(secs: u64) -> Option<ActivityInputs> {
    Some(ActivityInputs {
        locked: false,
        idle: false,
        since_input: Some(Duration::from_secs(secs)),
    })
}

#[tokio::test]
async fn announce_tipi_locked_sends_idle_score() {
    let locked = Some(ActivityInputs {
        locked: true,
        idle: false,
        since_input: Some(Duration::from_secs(46)),
    });
    let tipi = announced_tipi("5A:00:00:00:13:01", locked).await;
    assert_eq!(tipi, expected_tipi_fields(0, 1).await);
    assert_ne!(tipi, expected_tipi(REMOTE).await);
}

#[tokio::test]
async fn announce_tipi_recent_input_sends_max() {
    let tipi = announced_tipi("5A:00:00:00:13:02", unlocked_since(2)).await;
    assert_eq!(tipi, expected_tipi_fields(0, 7).await);
}

#[tokio::test]
async fn announce_tipi_long_idle_sends_low() {
    let tipi = announced_tipi("5A:00:00:00:13:03", unlocked_since(40)).await;
    assert_eq!(tipi, expected_tipi_fields(0, 2).await);
}

#[tokio::test]
async fn announce_tipi_failed_snapshot_sends_legacy() {
    let tipi = announced_tipi("5A:00:00:00:13:04", None).await;
    assert_eq!(tipi, expected_tipi(REMOTE).await);
}

/// #534: `on_sleep`'s glue on fake media, a fake disconnect and the
/// shell's own `Sleepers`, never BlueZ or the app's subscriptions.
mod sleep {
    use super::*;
    use crate::suspend::{SLEEP_DISCONNECT_RESERVE, SLEEP_PREPARE_TIMEOUT, SleepNotice};

    /// Each sleep disconnect's MAC and (paused) time.
    type Disconnects = Arc<StdMutex<Vec<(String, tokio::time::Instant)>>>;

    /// `bare_shell` on a ready session, its pauses and sleep disconnects
    /// recorded; each disconnect returns `disconnects`.
    async fn sleep_shell(mac: &str, disconnects: bool) -> (Shell, Arc<Records>, Disconnects) {
        let (manager, _rx, _tx) = ready_session().await;
        let mut shell = bare_shell(mac, manager, mpsc::unbounded_channel().1);
        let (records, (_, media)) = Harness::fake_media(mac);
        shell.media = media;
        let calls = Disconnects::default();
        let record = calls.clone();
        shell.sleep_disconnect = Arc::new(move |mac| {
            let now = tokio::time::Instant::now();
            record.lock().unwrap().push((mac, now));
            Box::pin(async move { disconnects })
        });
        (shell, records, calls)
    }

    fn macs(calls: &Disconnects) -> Vec<String> {
        let calls = calls.lock().unwrap();
        calls.iter().map(|(mac, _)| mac.clone()).collect()
    }

    fn pauses(records: &Records) -> usize {
        records.pauses.load(Ordering::SeqCst)
    }

    /// A live core with Linux playing media: `Sleep` pauses, then
    /// disconnects.
    fn playing_core() -> Handoff {
        let mut core = Handoff::new(LOCAL.to_string(), Instant::now(), false, true);
        let play = Input::Playback {
            sampled_at: Instant::now(),
            category: AUDIO_CATEGORY_MEDIA,
            new_source: None,
        };
        assert_eq!(core.handle(Instant::now(), play), []);
        core
    }

    #[tokio::test]
    async fn stale_notice_does_nothing() {
        let (mut shell, records, calls) = sleep_shell("5A:00:00:05:34:01", true).await;
        let mut core = playing_core();
        let (notice, acked, slept) = SleepNotice::detached();
        drop(acked);
        assert!(!on_sleep(&mut core, &mut shell, notice).await);
        assert_eq!(macs(&calls), Vec::<String>::new());
        assert_eq!(*slept.lock().unwrap(), Vec::<String>::new());
        assert_eq!(pauses(&records), 0);
        assert!(core.live());
    }

    #[tokio::test]
    async fn live_session_sleeps_disconnects_and_acks() {
        let mac = "5A:00:00:05:34:02";
        let (mut shell, records, calls) = sleep_shell(mac, true).await;
        let mut core = playing_core();
        let (notice, acked, slept) = SleepNotice::detached();
        assert!(!on_sleep(&mut core, &mut shell, notice).await);
        assert_eq!(*slept.lock().unwrap(), [mac]);
        assert_eq!(macs(&calls), [mac]);
        assert_eq!(acked.await, Ok(()));
        assert_eq!(pauses(&records), 1);
        // Sleeping: no SleepDisconnectFailed made it live again.
        assert!(!core.live());
    }

    #[tokio::test]
    async fn session_not_live_records_nothing_and_acks() {
        let (mut shell, records, calls) = sleep_shell("5A:00:00:05:34:03", true).await;
        let mut core = playing_core();
        // Found dead, though the manager still has the session.
        core.handle(Instant::now(), Input::SessionLost);
        assert!(!core.live());
        let (notice, acked, slept) = SleepNotice::detached();
        assert!(!on_sleep(&mut core, &mut shell, notice).await);
        assert_eq!(*slept.lock().unwrap(), Vec::<String>::new());
        assert_eq!(macs(&calls), Vec::<String>::new());
        assert_eq!(pauses(&records), 0);
        assert_eq!(acked.await, Ok(()));
    }

    /// #535's case.
    #[tokio::test]
    async fn failed_disconnect_makes_the_session_live_again() {
        let mac = "5A:00:00:05:34:04";
        let (mut shell, _records, calls) = sleep_shell(mac, false).await;
        let mut core = playing_core();
        let (notice, acked, slept) = SleepNotice::detached();
        assert!(!on_sleep(&mut core, &mut shell, notice).await);
        assert_eq!(macs(&calls), [mac]);
        // Still registered: the wake reconnects it if it did drop.
        assert_eq!(*slept.lock().unwrap(), [mac]);
        assert!(core.live());
        assert_eq!(acked.await, Ok(()));
    }

    /// The sleep is released while `sync_session` waits on the session's
    /// lock (#532).
    #[tokio::test]
    async fn notice_stale_after_the_session_lock_does_nothing() {
        let (mut shell, records, calls) = sleep_shell("5A:00:00:05:34:05", true).await;
        let mut core = playing_core();
        let manager = shell.manager.clone();
        let (notice, acked, slept) = SleepNotice::detached();
        let state = manager.state.lock().await;
        let ended = {
            let mut sleep = std::pin::pin!(on_sleep(&mut core, &mut shell, notice));
            // Past the first stale check, waiting on the lock.
            assert!(futures::poll!(sleep.as_mut()).is_pending());
            drop(acked);
            drop(state);
            sleep.await
        };
        assert!(!ended);
        assert_eq!(*slept.lock().unwrap(), Vec::<String>::new());
        assert_eq!(pauses(&records), 0);
        assert_eq!(macs(&calls), Vec::<String>::new());
        assert!(core.live());
    }

    /// #532: a hung pause is cut the reserve before the sleep's deadline,
    /// so the disconnect still runs before the release.
    #[tokio::test(start_paused = true)]
    async fn hung_pause_leaves_time_for_the_disconnect() {
        let mac = "5A:00:00:05:34:06";
        let (mut shell, records, calls) = sleep_shell(mac, true).await;
        records.hung_pause.store(true, Ordering::SeqCst);
        let mut core = playing_core();
        let mut sleep_rx = shell.sleepers.subscribe();
        let t0 = tokio::time::Instant::now();
        let (slept, sleep) = shell.sleepers.start_sleep();
        let notice = sleep_rx.recv().await.unwrap();
        // Bounded, so an unbounded hung pause fails instead of hanging.
        let sleep_step = on_sleep(&mut core, &mut shell, notice);
        let ended = tokio::time::timeout(SLEEP_PREPARE_TIMEOUT * 2, sleep_step).await;
        assert_eq!(ended, Ok(false));
        let cut = t0 + SLEEP_PREPARE_TIMEOUT - SLEEP_DISCONNECT_RESERVE;
        assert_eq!(*calls.lock().unwrap(), [(mac.to_string(), cut)]);
        assert_eq!(pauses(&records), 1);
        assert_eq!(*slept.lock().unwrap(), [mac]);
        // Acked: the sleep ends now, not at its deadline.
        sleep.await.unwrap();
        assert_eq!(tokio::time::Instant::now(), cut);
    }
}

/// #544: `run_handoff` claims a wake park (#529): its own Park joins the
/// park before suspend.rs lets go, and the core starts parked.
mod wake_park {
    use super::*;
    use crate::suspend::{claim_wake_park, register_parked_wake_park};

    /// A wake park parked for `mac`, its deadline far past the test.
    async fn parked(mac: &str) -> FakeSinks {
        let sinks = FakeSinks::new(false);
        let until = tokio::time::Instant::now() + Duration::from_secs(60);
        register_parked_wake_park(mac, sinks.clone(), Some(until));
        wait_until("the wake park", || default_sink(&sinks) == PARK_SINK).await;
        sinks
    }

    fn loads(sinks: &FakeSinks) -> usize {
        sinks.sets().iter().filter(|s| *s == "load").count()
    }

    fn unloaded(sinks: &FakeSinks) -> bool {
        sinks.sets().iter().any(|s| s.starts_with("unload"))
    }

    /// `mac`'s task on a ready session, routing to a real router over its
    /// wake park's sinks, past its startup's connect take; the park kept.
    async fn claimed(mac: &str) -> (Harness, FakeSinks) {
        let sinks = parked(mac).await;
        let (manager, mut rx, _tx) = ready_session().await;
        let records = Arc::new(Records::default());
        records.parks.store(true, Ordering::SeqCst);
        let fake = FakeMedia {
            records: records.clone(),
            mac: mac.to_string(),
        };
        let media: Arc<dyn MediaActions> = Arc::new(RouterMedia {
            fake,
            sinks: DefaultSinkRouter::spawn(sinks.clone()),
        });
        let controller = MediaController::new(mac.to_string(), LOCAL.to_string());
        let h = Harness::start_playback(
            SwitchingMode::Handoff,
            mac,
            &manager,
            (Arc::new(Mutex::new(controller)), media),
            records,
            Arc::new(DeviceHandler::default()),
            known_play_channel(),
            ConnectMode::default(),
        )
        .await;
        assert_eq!(next_packet(&mut rx).await, expected_owns(1).await);
        assert_eq!(
            next_packet(&mut rx).await,
            expected_new_device(REMOTE).await
        );
        assert_eq!(next_packet(&mut rx).await, expected_tipi(REMOTE).await);
        assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
        assert_eq!(default_sink(&sinks), PARK_SINK);
        assert_eq!(loads(&sinks), 1, "{:?}", sinks.sets());
        assert!(!unloaded(&sinks), "{:?}", sinks.sets());
        assert_eq!(h.routes(), [SinkEvent::Park]);
        (h, sinks)
    }

    #[tokio::test]
    async fn claim_keeps_the_park_through_startup() {
        let mac = "5A:00:00:05:44:01";
        let (h, _sinks) = claimed(mac).await;
        // The join was routed before the settle suspend.rs's release awaits.
        let first_settle = h.records.settles.lock().unwrap().first().copied();
        assert_eq!(first_settle, Some(1));
        assert!(claim_wake_park(mac).is_none());
        stop_handoff_task(mac);
    }

    #[tokio::test]
    async fn stopped_task_unparks_the_claimed_park() {
        let mac = "5A:00:00:05:44:02";
        let (h, sinks) = claimed(mac).await;
        stop_handoff_task(mac);
        let local = SinkEvent::Unpark(UnparkTo::Local);
        wait_until("the drop's unpark", || h.routes().last() == Some(&local)).await;
        // The unload comes last, after the local sink is the default.
        wait_until("the park sink's unload", || unloaded(&sinks)).await;
        assert_eq!(default_sink(&sinks), DAC);
        assert_eq!(h.routes(), [SinkEvent::Park, local]);
    }

    #[tokio::test]
    async fn no_session_leaves_the_wake_park_unclaimed() {
        let mac = "5A:00:00:05:44:03";
        let sinks = parked(mac).await;
        let mut shell = bare_shell(mac, AACPManager::new(), mpsc::unbounded_channel().1);
        let (records, (_, media)) = Harness::fake_media(mac);
        records.parks.store(true, Ordering::SeqCst);
        shell.media = media;
        run_handoff(shell, None).await;
        assert_eq!(*records.routes.lock().unwrap(), []);
        assert_eq!(default_sink(&sinks), PARK_SINK);
        assert!(!unloaded(&sinks), "{:?}", sinks.sets());
        assert!(claim_wake_park(mac).is_some());
    }
}
