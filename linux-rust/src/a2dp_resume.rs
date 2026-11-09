//! Restores A2DP streaming after a take that found the AirPods' transport
//! idle (#200, DECISIONS 2026-11-08 option (b)).
//!
//! `Chain` is the pure step machine: after a settle, read the transport
//! fresh, again while it stays idle, and, only if every read was idle up to
//! one issued `TRANSPORT_START_WAIT` after the start, nudge the AirPods sink
//! (suspend, then resume), then kick the players that were
//! playing at the take (MPRIS Pause, then Play), then, as a last resort,
//! cycle the card profile once and play them again. Any audible check stops
//! it. `run` drives it on injected I/O (`A2dpResumeIo`); the production I/O
//! is `MediaController`'s. The transport state the core needs comes from
//! `spawn_transport_poller`, which only reads BlueZ's `MediaTransport1.State`
//! (never `Acquire`: the fd would go to LibrePods instead of PipeWire).

use crate::stream_monitor::PlaybackState;
use dbus::blocking::Connection;
use dbus::blocking::stdintf::org_freedesktop_dbus::{ObjectManager, Properties};
use futures::FutureExt;
use futures::future::BoxFuture;
use log::{debug, info, warn};
use std::sync::{Mutex, TryLockError};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

/// From the chain's start to its first transport read: PipeWire starts the
/// transport by itself about 0.3 s after a take (#267).
const RESUME_SETTLE: Duration = Duration::from_millis(500);
/// From the chain's start to the first transport read whose idle may nudge:
/// PipeWire's own start can still be in flight well after the settle, and a
/// nudge then restarts the transport under Linux audio (#331, #267).
const TRANSPORT_START_WAIT: Duration = Duration::from_secs(2);
/// The most a fresh transport read takes, and how long after
/// `TRANSPORT_START_WAIT` the chain waits for its reads: one still running
/// then is unknown.
const TRANSPORT_READ_TIMEOUT: Duration = Duration::from_millis(500);
/// Between the chain's fresh transport reads while they read idle: after a
/// take from a streaming iPhone, PipeWire starts the transport about 0.8 s
/// after the ACK (#305). All reads end by `TRANSPORT_START_WAIT` +
/// `TRANSPORT_READ_TIMEOUT` from the chain's start.
const TRANSPORT_REREAD: Duration = Duration::from_millis(100);
/// From the sink's suspend to its resume.
const NUDGE_GAP: Duration = Duration::from_millis(500);
/// From the nudge to the check that decides on the kick.
const AUDIBLE_WAIT: Duration = Duration::from_secs(1);
/// From the kick's Pause to its Play.
const KICK_GAP: Duration = Duration::from_millis(500);
/// From the kick's Play to the check that decides on the profile cycle.
const KICK_WAIT: Duration = Duration::from_secs(1);
/// From the profile cycle to the Play of the players it paused (the sink
/// came back, and Firefox paused itself when it went away).
const CYCLE_SETTLE: Duration = Duration::from_millis(1500);
/// How long the production audibility read samples the peaks of the
/// AirPods sink's streams.
pub(crate) const AUDIBLE_SAMPLE: Duration = Duration::from_millis(500);
/// How long before the window's end a chain Play must be due: the playback
/// poll needs up to this long to see it, and a play edge seen after the
/// window-end Tick comes after its release.
const PLAY_EDGE_MARGIN: Duration = crate::switching::PLAYBACK_POLL;
/// The chain's sleeps when nothing ever plays: every step runs, with its
/// transport reads timing out and its three audibility reads.
const CHAIN_WORST_CASE_SLEEP: Duration = TRANSPORT_START_WAIT
    .saturating_add(TRANSPORT_READ_TIMEOUT)
    .saturating_add(NUDGE_GAP)
    .saturating_add(AUDIBLE_WAIT)
    .saturating_add(KICK_GAP)
    .saturating_add(KICK_WAIT)
    .saturating_add(CYCLE_SETTLE)
    .saturating_add(AUDIBLE_SAMPLE.saturating_mul(3));
/// Run time of the chain's D-Bus and sound-server calls on top of its
/// sleeps and the last Play's `PLAY_EDGE_MARGIN`. Like
/// `A2DP_RECOVERY_BOUND`, the window counts sleeps; this margin is an
/// engineering default, not a measured bound. The chain also starts no kick
/// or cycle that can't finish inside the window.
const RESUME_MARGIN: Duration = Duration::from_millis(1500);
/// How long after the take's acknowledgement the core holds playback-stop
/// releases while the chain may pause and play the players.
pub(crate) const A2DP_RESUME_WINDOW: Duration = Duration::from_millis(10_500);
const _: () = assert!(
    CHAIN_WORST_CASE_SLEEP
        .saturating_add(PLAY_EDGE_MARGIN)
        .saturating_add(RESUME_MARGIN)
        .as_millis()
        <= A2DP_RESUME_WINDOW.as_millis()
);
/// How long after the route a player that was Playing at the take and
/// paused itself gets one Play (#327: Spotify pauses itself ~50 ms after it).
const SELF_PAUSE_GUARD: Duration = Duration::from_secs(3);
/// The most between the starts of the self-pause watch's Paused reads.
const SELF_PAUSE_READ: Duration = Duration::from_millis(250);
const _: () = assert!(
    SELF_PAUSE_GUARD
        .saturating_add(SELF_PAUSE_READ)
        .saturating_add(PLAY_EDGE_MARGIN)
        .as_millis()
        <= A2DP_RESUME_WINDOW.as_millis()
);

/// How often the transport poller reads BlueZ.
const TRANSPORT_POLL: Duration = Duration::from_millis(500);
/// The proxy timeout of each poller bus call, and the most a fresh read's
/// call gets.
const TRANSPORT_CALL_TIMEOUT: Duration = Duration::from_secs(2);

/// One side effect of the chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// Suspend (true) or resume (false) the AirPods sink.
    SuspendSink(bool),
    /// MPRIS Pause to this player service.
    Pause(String),
    /// MPRIS Play to this player service.
    Play(String),
    /// Set the AirPods card profile to off, then back to its A2DP profile,
    /// in one I/O call so nothing can stop it with the card off.
    ProfileCycle,
}

enum Stage {
    /// Read the transport fresh, again every `TRANSPORT_REREAD` while it is
    /// idle; nudge only on an idle read issued from `nudge_at` on, and only
    /// if every read before it was idle.
    Start,
    Suspended,
    Nudged,
    /// These players were paused; Play them next.
    Kicking(Vec<String>),
    Kicked,
    /// The profile was cycled; Play these players next.
    Cycled(Vec<String>),
    Done,
}

/// The resume chain's pure step machine.
struct Chain {
    stage: Stage,
    /// The players playing at the chain's start (the take's ACK).
    start: Vec<String>,
    due: Instant,
    /// The end of the core's hold window: a stop after it releases.
    until: Instant,
    /// The earliest issue time of a transport read whose idle may nudge.
    nudge_at: Instant,
    /// The end of the fresh transport reads: one still running then is
    /// unknown.
    read_end: Instant,
}

impl Chain {
    fn new(now: Instant, until: Instant, playing: Vec<String>) -> Self {
        Chain {
            stage: Stage::Start,
            start: playing,
            due: now + RESUME_SETTLE,
            until,
            nudge_at: now + TRANSPORT_START_WAIT,
            read_end: now + TRANSPORT_START_WAIT + TRANSPORT_READ_TIMEOUT,
        }
    }

    /// When the next step is due.
    fn due(&self) -> Instant {
        self.due
    }

    /// Whether the next step reads the transport.
    fn reads_transport(&self) -> bool {
        matches!(self.stage, Stage::Start)
    }

    /// Whether the next step reads audibility.
    fn senses(&self) -> bool {
        matches!(self.stage, Stage::Nudged | Stage::Kicked | Stage::Cycled(_))
    }

    /// Whether the next step reads the playing players. The replay after
    /// the cycle doesn't: a slow read there would delay its Play.
    fn reads_players(&self) -> bool {
        matches!(self.stage, Stage::Nudged | Stage::Kicked)
    }

    /// Run the step due at `now`: its actions, or None once the chain is
    /// over. `transport` (the fresh read issued at `issued`: active, idle or
    /// unknown) matters only when `reads_transport()`: active ends the chain;
    /// idle issued from `nudge_at` on suspends the sink (`run` bounds each
    /// read by `read_end`); idle issued earlier and back before `read_end`
    /// reads again `TRANSPORT_REREAD` later, or at `nudge_at` if sooner;
    /// anything else goes straight to the audible check, never stopping a
    /// transport that may carry audio. `audible` matters only when
    /// `senses()`, and ends the chain there; `playing` only when
    /// `reads_players()`. Once the sink is suspended or the players paused,
    /// the next step always undoes it. The kick and the cycle are skipped,
    /// ending the chain, when the Play after them can't be due
    /// `PLAY_EDGE_MARGIN` before `until`: the core would release on the stop
    /// they cause.
    fn step(
        &mut self,
        issued: Instant,
        now: Instant,
        transport: Option<bool>,
        audible: bool,
        playing: &[String],
    ) -> Option<Vec<Action>> {
        let play_at = match self.stage {
            Stage::Nudged => Some(now + KICK_GAP),
            Stage::Kicked => Some(now + CYCLE_SETTLE + AUDIBLE_SAMPLE),
            _ => None,
        };
        let too_late = play_at.is_some_and(|play| play + PLAY_EDGE_MARGIN >= self.until);
        if too_late || (audible && self.senses()) {
            self.stage = Stage::Done;
            return None;
        }
        let still_playing = || -> Vec<String> {
            self.start
                .iter()
                .filter(|p| playing.contains(p))
                .cloned()
                .collect()
        };
        let (actions, next, wait) = match std::mem::replace(&mut self.stage, Stage::Done) {
            Stage::Start => match transport {
                Some(true) => return None,
                // Judged by its issue time: a read issued before `nudge_at`
                // may have raced PipeWire's own start, however late it is back.
                Some(false) if issued >= self.nudge_at => {
                    (vec![Action::SuspendSink(true)], Stage::Suspended, NUDGE_GAP)
                }
                Some(false) if now < self.read_end => {
                    let wait = TRANSPORT_REREAD.min(self.nudge_at.saturating_duration_since(now));
                    (Vec::new(), Stage::Start, wait)
                }
                _ => (Vec::new(), Stage::Nudged, NUDGE_GAP + AUDIBLE_WAIT),
            },
            Stage::Suspended => (
                vec![Action::SuspendSink(false)],
                Stage::Nudged,
                AUDIBLE_WAIT,
            ),
            Stage::Nudged => {
                let kick = still_playing();
                let pauses = kick.iter().cloned().map(Action::Pause).collect();
                (pauses, Stage::Kicking(kick), KICK_GAP)
            }
            Stage::Kicking(kick) => (
                kick.into_iter().map(Action::Play).collect(),
                Stage::Kicked,
                KICK_WAIT,
            ),
            Stage::Kicked => (
                vec![Action::ProfileCycle],
                Stage::Cycled(still_playing()),
                CYCLE_SETTLE,
            ),
            Stage::Cycled(replay) => return Some(replay.into_iter().map(Action::Play).collect()),
            Stage::Done => return None,
        };
        self.stage = next;
        self.due = now + wait;
        Some(actions)
    }

    /// Stop the chain between steps: only a suspended sink is resumed.
    fn cancel(&mut self) -> Vec<Action> {
        match std::mem::replace(&mut self.stage, Stage::Done) {
            Stage::Suspended => vec![Action::SuspendSink(false)],
            _ => Vec::new(),
        }
    }
}

/// The chain's I/O, injected so tests need no D-Bus or sound server.
pub(crate) trait A2dpResumeIo: Send + Sync {
    fn act(&self, action: Action) -> BoxFuture<'static, ()>;
    /// Some uncorked stream on the AirPods sink is above `AUDIBLE_PEAK`.
    fn audible(&self) -> BoxFuture<'static, bool>;
    /// The MPRIS players that are Playing.
    fn playing(&self) -> BoxFuture<'static, Vec<String>>;
    /// The MPRIS players that are Playing and whose bus owner PID (None
    /// when the bus can't tell) `own` accepts. The default (`playing`,
    /// unfiltered) is a test seam for fakes; the production I/O must
    /// override it.
    fn playing_owned(
        &self,
        _own: Box<dyn Fn(Option<u32>) -> bool + Send>,
    ) -> BoxFuture<'static, Vec<String>> {
        self.playing()
    }
    /// A fresh read of whether the AirPods' A2DP transport is active; None
    /// when unknown. The default (idle) is a test seam for fakes; the
    /// production I/O must override it.
    fn transport_active(&self) -> BoxFuture<'static, Option<bool>> {
        Box::pin(async { Some(false) })
    }
    /// The MPRIS players that are Paused. The default (none) is a test seam
    /// for fakes; the production I/O must override it.
    fn paused(&self) -> BoxFuture<'static, Vec<String>> {
        Box::pin(async { Vec::new() })
    }
    /// How many ear-removal or ownership-loss pauses LibrePods has started.
    /// The default never changes.
    fn self_pauses(&self) -> usize {
        0
    }
}

/// Run the chain on `io` once `routed` resolves (the default sink route
/// queued before it, #40), starting no kick or cycle that can't end before
/// `until`. `cancel` firing (or its sender dropping) stops it between steps,
/// including during a step's audio check; during the wait for `routed`, it
/// or `until` ends the run with no action. The take's players
/// (`take_candidates` of `streams`, the stream monitor's state at the take)
/// are read before the route; beside the chain, `replay_self_pauses` plays
/// those that pause themselves after it once (#327).
pub(crate) async fn run(
    io: impl A2dpResumeIo,
    streams: PlaybackState,
    until: Instant,
    cancel: oneshot::Receiver<()>,
    routed: impl Future<Output = ()>,
) {
    let mut cancel = cancel.shared();
    let pauses = io.self_pauses();
    let routed = async {
        routed.await;
        clock()
    };
    let (start, routed_at) = tokio::select! {
        biased;
        _ = &mut cancel => {
            info!("A2DP resume: cancelled before the default sink route ended");
            return;
        }
        read = futures::future::join(take_candidates(&io, streams), routed) => read,
        _ = tokio::time::sleep_until(until.into()) => (Vec::new(), until),
    };
    // The route and the deadline can both be ready at one poll.
    if clock() >= until {
        info!("A2DP resume: the default sink route outlasted the window");
        return;
    }
    let ms = |at: Instant| at.saturating_duration_since(routed_at).as_millis();
    // routed_at - (until - A2DP_RESUME_WINDOW), the ACK, with no Instant underflow.
    let after_ack = A2DP_RESUME_WINDOW.saturating_sub(until.saturating_duration_since(routed_at));
    debug!(
        "A2DP resume: chain start {} ms after the ACK",
        after_ack.as_millis()
    );
    let replayable = Mutex::new(start.clone());
    let watch = replay_self_pauses(&io, &replayable, routed_at, until, pauses, cancel.clone());
    let mut chain = Chain::new(routed_at, until, start);
    let mut reads = 0;
    let chained = async {
        loop {
            let (due, senses, reads_players) = (chain.due(), chain.senses(), chain.reads_players());
            let (reads_transport, read_end) = (chain.reads_transport(), chain.read_end);
            reads += usize::from(reads_transport);
            // The transport read and the audio check give way to a cancel too,
            // so a yield during them stops the chain before the step they decide.
            let sensed = async {
                tokio::time::sleep_until(due.into()).await;
                let issued = clock();
                let transport = if reads_transport {
                    let read = tokio::time::timeout_at(read_end.into(), io.transport_active())
                        .await
                        .ok()
                        .flatten();
                    let back = ms(clock());
                    debug!(
                        "A2DP resume: transport read issued {} ms, back {back} ms after the chain start: {read:?}",
                        ms(issued)
                    );
                    read
                } else {
                    None
                };
                let audible = senses && io.audible().await;
                let playing = if reads_players {
                    io.playing().await
                } else {
                    Vec::new()
                };
                (issued, transport, audible, playing)
            };
            let (issued, transport, audible, playing) = tokio::select! {
                biased;
                _ = &mut cancel => {
                    info!("A2DP resume: cancelled");
                    act(&io, chain.cancel()).await;
                    return;
                }
                sensed = sensed => sensed,
            };
            let now = clock();
            let Some(actions) = chain.step(issued, now, transport, audible, &playing) else {
                let left = until.saturating_duration_since(now);
                info!(
                    "A2DP resume: done (transport: {transport:?}, transport reads: {reads}, audible: {audible}, window left: {left:?})"
                );
                return;
            };
            // Before they act: the kick's or the cycle's own Play is the only
            // Play for the players they pause, those in `playing`.
            if actions
                .iter()
                .any(|a| matches!(a, Action::Pause(_) | Action::ProfileCycle))
            {
                replayable.lock().unwrap().retain(|p| !playing.contains(p));
            }
            act(&io, actions).await;
        }
    };
    futures::join!(chained, watch);
}

/// The take's players (#361): the Playing ones whose bus owner PID owns a
/// live stream in `streams` (`PlaybackState::live_or_unknown`), so a remote
/// such as Spotify Connect is never paused or played; every Playing one,
/// with no owner lookup, while live presence is unknown.
fn take_candidates(
    io: &impl A2dpResumeIo,
    streams: PlaybackState,
) -> BoxFuture<'static, Vec<String>> {
    if streams.live.is_none() {
        return io.playing();
    }
    io.playing_owned(Box::new(move |owner| streams.live_or_unknown(owner)))
}

/// Tokio's clock, so tests can pause it; the same as std's otherwise.
fn clock() -> Instant {
    tokio::time::Instant::now().into_std()
}

/// Play once each of `replayable` that a Paused read due within
/// `SELF_PAUSE_GUARD` of `routed_at` finds, while that Play can be due
/// `PLAY_EDGE_MARGIN` before `until`: reads are due every `SELF_PAUSE_READ`
/// from `routed_at` (one due during a slower read starts right after it),
/// and each is cut off `SELF_PAUSE_READ` after the guard's end. The Plays
/// go out in turn beside the reads, so a slow one holds up no read.
/// `cancel`, or a LibrePods pause since `pauses` was counted (at the take),
/// ends it with no further Play; both are checked before each Play.
async fn replay_self_pauses(
    io: &impl A2dpResumeIo,
    replayable: &Mutex<Vec<String>>,
    routed_at: Instant,
    until: Instant,
    pauses: usize,
    cancel: impl Future + Clone + Unpin,
) {
    let guard_end = routed_at + SELF_PAUSE_GUARD;
    // A read due by the guard's end may run into the next interval, and see
    // a pause made after the guard.
    let read_end = guard_end + SELF_PAUSE_READ;
    let (queue, mut queued) = mpsc::unbounded_channel();
    let mut read_cancel = cancel.clone();
    let reads = async move {
        // Fixed due times, so late wake-ups don't push the last read out.
        let mut due = tokio::time::interval_at(routed_at.into(), SELF_PAUSE_READ);
        due.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let read_at = tokio::select! {
                biased;
                _ = &mut read_cancel => return,
                read_at = due.tick() => read_at.into_std(),
            };
            if read_at > guard_end || replayable.lock().unwrap().is_empty() {
                return;
            }
            let paused = tokio::select! {
                biased;
                _ = &mut read_cancel => return,
                paused = tokio::time::timeout_at(read_end.into(), io.paused()) => paused,
            };
            let Ok(paused) = paused else { return };
            if clock() > read_end {
                return;
            }
            let replay: Vec<String> = {
                let mut replayable = replayable.lock().unwrap();
                let (replay, keep) = replayable.drain(..).partition(|p| paused.contains(p));
                *replayable = keep;
                replay
            };
            for player in replay {
                if queue.send(player).is_err() {
                    return;
                }
            }
        }
    };
    let plays = async move {
        let mut cancel = cancel;
        while let Some(player) = queued.recv().await {
            // A Play can be slow: re-check before each. LibrePods counts a
            // pause before making it, so the count is read after the Paused read.
            let now = clock();
            if (&mut cancel).now_or_never().is_some()
                || io.self_pauses() != pauses
                || now + PLAY_EDGE_MARGIN >= until
            {
                return;
            }
            let ms = (now - routed_at).as_millis();
            info!("A2DP resume: {player} paused itself {ms} ms after the route; Play");
            io.act(Action::Play(player)).await;
        }
    };
    futures::join!(reads, plays);
}

async fn act(io: &impl A2dpResumeIo, actions: Vec<Action>) {
    for action in actions {
        info!("A2DP resume: {action:?}");
        io.act(action).await;
    }
}

/// Poll BlueZ every `TRANSPORT_POLL` for whether the AirPods' A2DP
/// transport is streaming: the first state, then each change. A device with
/// no transport reads idle; a failed read keeps the last state. Stops once
/// the receiver is dropped.
pub(crate) fn spawn_transport_poller(airpods_mac: &str) -> mpsc::Receiver<bool> {
    let device = device_fragment(airpods_mac);
    let (tx, rx) = mpsc::channel(4);
    let spawned = std::thread::Builder::new()
        .name("librepods-a2dp".to_string())
        .spawn(move || {
            let mut bus = None;
            let mut reader = TransportReader::default();
            poll_transport(&tx, TRANSPORT_POLL, || {
                if bus.is_none() {
                    bus = Connection::new_system().ok();
                }
                let active = bus.as_ref().and_then(|bus| {
                    reader.read(
                        || transport_paths(bus, &device, TRANSPORT_CALL_TIMEOUT),
                        |path| transport_state(bus, path, TRANSPORT_CALL_TIMEOUT),
                    )
                });
                // The reader cleared its paths on that None too.
                if active.is_none() {
                    bus = None;
                }
                active
            })
        });
    if let Err(e) = spawned {
        warn!("A2DP resume: no transport poller, so takes never resume A2DP: {e}");
    }
    rx
}

/// The AirPods' BlueZ object path fragment, `/dev_XX_..._XX/`.
fn device_fragment(airpods_mac: &str) -> String {
    format!("/dev_{}/", airpods_mac.to_uppercase().replace(':', "_"))
}

/// The fresh reads' one system bus connection, shared by `read_shared`.
static FRESH_BUS: Mutex<Option<Connection>> = Mutex::new(None);

/// One blocking read of whether the AirPods' A2DP transport is active, on
/// the shared `FRESH_BUS` connection. None for no MAC (never idle: "/dev_/"
/// matches no transport), when another read holds the connection (a stuck
/// open or call), or when BlueZ can't be read in `TRANSPORT_READ_TIMEOUT`:
/// the chain stops waiting by its read window's end, and this blocking read
/// can't be aborted.
pub(crate) fn read_transport_once(airpods_mac: &str) -> Option<bool> {
    // The budget counts the connection's opening too.
    let deadline = Instant::now() + TRANSPORT_READ_TIMEOUT;
    if airpods_mac.is_empty() {
        return None;
    }
    let device = device_fragment(airpods_mac);
    read_shared(
        &FRESH_BUS,
        || Connection::new_system().ok(),
        |bus| {
            read_within(
                deadline,
                Instant::now,
                |timeout| transport_paths(bus, &device, timeout),
                |path, timeout| transport_state(bus, path, timeout),
            )
        },
    )
}

/// `read` on the bus in `slot`, opened by `open` only when the slot is
/// empty. None at once when another read holds the slot, so a stalled bus
/// pins one thread, not one per read. A None read drops the bus, so the
/// next read reopens; a poisoned slot is cleared and reopened.
fn read_shared<B>(
    slot: &Mutex<Option<B>>,
    open: impl FnOnce() -> Option<B>,
    read: impl FnOnce(&B) -> Option<bool>,
) -> Option<bool> {
    let mut bus = match slot.try_lock() {
        Ok(bus) => bus,
        Err(TryLockError::WouldBlock) => return None,
        Err(TryLockError::Poisoned(poisoned)) => {
            slot.clear_poison();
            let mut bus = poisoned.into_inner();
            *bus = None;
            bus
        }
    };
    if bus.is_none() {
        *bus = open();
    }
    let active = bus.as_ref().and_then(read);
    if active.is_none() {
        *bus = None;
    }
    active
}

/// A fresh `TransportReader` read that makes no bus call after `deadline`:
/// each call's proxy timeout is the time left, at most
/// `TRANSPORT_CALL_TIMEOUT`. Past it, a lookup fails the bus and a state
/// is unknown, so the read is None.
fn read_within(
    deadline: Instant,
    now: impl Fn() -> Instant,
    mut lookup: impl FnMut(Duration) -> Option<Vec<String>>,
    mut state: impl FnMut(&str, Duration) -> Result<Option<bool>, Gone>,
) -> Option<bool> {
    // Under 1 ms is spent: dbus rounds down to whole ms, and 0 is special.
    let timeout = || {
        Some(deadline.saturating_duration_since(now()))
            .filter(|left| *left >= Duration::from_millis(1))
            .map(|left| left.min(TRANSPORT_CALL_TIMEOUT))
    };
    TransportReader::default().read(
        || lookup(timeout()?),
        |path| match timeout() {
            Some(timeout) => state(path, timeout),
            None => Ok(None),
        },
    )
}

/// Send `read`'s first state, then each change, every `interval`, until the
/// receiver is gone. None keeps the last state.
fn poll_transport(
    tx: &mpsc::Sender<bool>,
    interval: Duration,
    mut read: impl FnMut() -> Option<bool>,
) {
    let mut last = None;
    while !tx.is_closed() {
        if let Some(active) = read()
            && last != Some(active)
        {
            if tx.blocking_send(active).is_err() {
                return;
            }
            last = Some(active);
        }
        std::thread::sleep(interval);
    }
}

/// A cached transport path no longer exists (BlueZ removed or re-created it).
struct Gone;

/// The AirPods' transport paths, looked up once and then only read.
#[derive(Default)]
struct TransportReader {
    paths: Vec<String>,
}

impl TransportReader {
    /// Whether any transport is not idle; no transport reads idle. Looks the
    /// paths up when none are cached or one is gone, in the same read. None
    /// when `lookup` or `state` fails on the bus, or paths keep going; that
    /// clears the cache.
    fn read(
        &mut self,
        mut lookup: impl FnMut() -> Option<Vec<String>>,
        mut state: impl FnMut(&str) -> Result<Option<bool>, Gone>,
    ) -> Option<bool> {
        if !self.paths.is_empty()
            && let Ok(active) = self.states(&mut state)
        {
            return active;
        }
        // A path can go between its lookup and its read: look up once more,
        // then give up on this read rather than guess idle.
        for _ in 0..2 {
            self.paths = lookup()?;
            if let Ok(active) = self.states(&mut state) {
                return active;
            }
        }
        None
    }

    /// Any cached path's State not idle. Gone when any path is gone. On a
    /// failure or a gone path, the cache is cleared.
    fn states(
        &mut self,
        state: &mut impl FnMut(&str) -> Result<Option<bool>, Gone>,
    ) -> Result<Option<bool>, Gone> {
        let mut active = false;
        for path in &self.paths {
            match state(path) {
                Ok(Some(path_active)) => active |= path_active,
                Ok(None) => {
                    self.paths.clear();
                    return Ok(None);
                }
                Err(Gone) => {
                    self.paths.clear();
                    return Err(Gone);
                }
            }
        }
        Ok(Some(active))
    }
}

/// The `MediaTransport1` paths under `device` (`/dev_XX_..._XX/`), from
/// BlueZ's object tree. None when BlueZ can't be read in `timeout`.
fn transport_paths(bus: &Connection, device: &str, timeout: Duration) -> Option<Vec<String>> {
    let objects = bus
        .with_proxy("org.bluez", "/", timeout)
        .get_managed_objects()
        .ok()?;
    Some(
        objects
            .into_iter()
            .filter(|(path, interfaces)| {
                path.contains(device) && interfaces.contains_key("org.bluez.MediaTransport1")
            })
            .map(|(path, _)| path.to_string())
            .collect(),
    )
}

/// Whether the transport at `path` is not idle. Gone when BlueZ no longer
/// has it; Ok(None) when BlueZ can't be read in `timeout`.
fn transport_state(bus: &Connection, path: &str, timeout: Duration) -> Result<Option<bool>, Gone> {
    let state: Result<String, _> = bus
        .with_proxy("org.bluez", path, timeout)
        .get("org.bluez.MediaTransport1", "State");
    match state {
        Ok(state) => Ok(Some(state != "idle")),
        Err(e) => match e.name().unwrap_or("") {
            "org.freedesktop.DBus.Error.UnknownObject"
            | "org.freedesktop.DBus.Error.UnknownInterface"
            | "org.freedesktop.DBus.Error.UnknownMethod"
            | "org.freedesktop.DBus.Error.InvalidArgs" => Err(Gone),
            _ => Ok(None),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Action::*;
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};

    const P: &str = "org.mpris.MediaPlayer2.firefox";
    const Q: &str = "org.mpris.MediaPlayer2.spotify";

    fn players(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    /// Run a chain started with `start` playing, its transport reads idle, to
    /// its end: `sense(ms since the nudge)` gives audibility and the playing
    /// players. Returns each step's (ms since the nudge, actions), from the
    /// nudge on.
    fn drive(
        start: &[&str],
        sense: impl Fn(u128) -> (bool, Vec<String>),
    ) -> Vec<(u128, Vec<Action>)> {
        let t0 = Instant::now();
        let mut chain = at_last_read(t0, t0 + A2DP_RESUME_WINDOW, players(start));
        let first = chain.due();
        let mut steps = Vec::new();
        loop {
            let now = chain.due();
            let ms = (now - first).as_millis();
            let (audible, playing) = sense(ms);
            match chain.step(now, now, Some(false), audible, &playing) {
                Some(actions) => steps.push((ms, actions)),
                None => return steps,
            }
        }
    }

    fn actions(steps: &[(u128, Vec<Action>)]) -> Vec<Action> {
        steps.iter().flat_map(|(_, a)| a.clone()).collect()
    }

    /// A chain started at `t0`, stepped through its instant idle re-reads:
    /// its next step is its last transport read.
    fn at_last_read(t0: Instant, until: Instant, playing: Vec<String>) -> Chain {
        let mut chain = Chain::new(t0, until, playing);
        let last = t0 + TRANSPORT_START_WAIT;
        while chain.due() < last {
            let due = chain.due();
            assert_eq!(chain.step(due, due, Some(false), false, &[]), Some(vec![]));
            assert_eq!(chain.due(), (due + TRANSPORT_REREAD).min(last));
        }
        assert!(chain.reads_transport());
        chain
    }

    #[test]
    fn idle_read_nudges_only_from_the_last_read_time() {
        // The last read is issued at TRANSPORT_START_WAIT, however long the
        // reads before it took.
        let t0 = Instant::now();
        let last = t0 + TRANSPORT_START_WAIT;
        let idle_at = |now| {
            let mut chain = Chain::new(t0, t0 + A2DP_RESUME_WINDOW, players(&[P]));
            (chain.step(now, now, Some(false), false, &[]), chain.due())
        };
        // A read issued and back 99 ms before `last`: the re-read at `last`.
        let early = last - TRANSPORT_REREAD + Duration::from_millis(1);
        assert_eq!(idle_at(early), (Some(vec![]), last));
        // 1 ms before `last`: still a re-read, at `last`.
        let just_before = last - Duration::from_millis(1);
        assert_eq!(idle_at(just_before), (Some(vec![]), last));
        // From `last` on: the nudge.
        assert_eq!(idle_at(last).0, Some(vec![SuspendSink(true)]));
    }

    /// Run a chain started at `t0` to its end, silent with `P` playing: each
    /// transport read is issued when due and back `read` later (unknown when
    /// back at the read window's end, as `run` times it out), and active if
    /// issued more than `idle_for` after `t0`. Returns each action with its
    /// step time since `t0`.
    fn run_reads(read: Duration, idle_for: Duration) -> Vec<(Duration, Action)> {
        let t0 = Instant::now();
        let mut chain = Chain::new(t0, t0 + A2DP_RESUME_WINDOW, players(&[P]));
        let mut acted = Vec::new();
        loop {
            let issued = chain.due();
            let (now, transport) = if chain.reads_transport() {
                let back = issued + read;
                let value = (back < chain.read_end).then_some(issued - t0 > idle_for);
                (back.min(chain.read_end), value)
            } else {
                (issued, None)
            };
            let Some(actions) = chain.step(issued, now, transport, false, &players(&[P])) else {
                return acted;
            };
            acted.extend(actions.into_iter().map(|a| (now - t0, a)));
        }
    }

    #[test]
    fn transport_start_wait_outlasts_a_slow_natural_start() {
        assert!(TRANSPORT_START_WAIT >= Duration::from_secs(2));
        // Idle until 1.9 s, then active: no action at all.
        for read in [Duration::ZERO, ms(50)] {
            assert_eq!(run_reads(read, ms(1900)), [], "{read:?}");
        }
    }

    #[test]
    fn never_starting_transport_nudges_within_the_read_window() {
        for read in [Duration::ZERO, ms(50), ms(150), ms(400)] {
            let acted = run_reads(read, Duration::MAX);
            let nudge = acted.iter().find(|(_, a)| *a == SuspendSink(true));
            let at = nudge.expect("a nudge").0;
            assert!(at >= TRANSPORT_START_WAIT, "{read:?}: {at:?}");
            assert!(
                at <= TRANSPORT_START_WAIT + TRANSPORT_READ_TIMEOUT,
                "{read:?}: {at:?}"
            );
        }
    }

    #[test]
    fn idle_read_issued_before_nudge_time_never_nudges() {
        let t0 = Instant::now();
        let new = || Chain::new(t0, t0 + A2DP_RESUME_WINDOW, players(&[P]));
        let nudge_at = t0 + TRANSPORT_START_WAIT;
        let issued = nudge_at - ms(100);
        // Back idle 50 ms after the nudge time: read again at once.
        let mut chain = new();
        let back = nudge_at + ms(50);
        assert_eq!(
            chain.step(issued, back, Some(false), false, &[]),
            Some(vec![])
        );
        assert!(chain.reads_transport());
        assert_eq!(chain.due(), back);
        // That read, issued after the nudge time, may nudge.
        assert_eq!(
            chain.step(back, back, Some(false), false, &[]),
            Some(vec![SuspendSink(true)])
        );
        // Back at the read window's end: no time for another read, unknown.
        let mut chain = new();
        let read_end = chain.read_end;
        assert_eq!(
            chain.step(issued, read_end, Some(false), false, &[]),
            Some(vec![])
        );
        assert!(!chain.reads_transport());
        assert_eq!(chain.due(), read_end + NUDGE_GAP + AUDIBLE_WAIT);
    }

    #[test]
    fn audible_after_nudge_stops_there() {
        let steps = drive(&[P], |_| (true, players(&[P])));
        assert_eq!(
            steps,
            [
                (0, vec![SuspendSink(true)]),
                (NUDGE_GAP.as_millis(), vec![SuspendSink(false)]),
            ]
        );
    }

    #[test]
    fn silent_after_nudge_kicks_players_playing_at_take() {
        // Audible once the kick has played P again.
        let kicked = (NUDGE_GAP + AUDIBLE_WAIT + KICK_GAP).as_millis();
        let steps = drive(&[P, Q], |ms| (ms > kicked, players(&[P, Q])));
        assert_eq!(
            actions(&steps),
            [
                SuspendSink(true),
                SuspendSink(false),
                Pause(P.into()),
                Pause(Q.into()),
                Play(P.into()),
                Play(Q.into()),
            ]
        );
        assert_eq!(steps[2].0, (NUDGE_GAP + AUDIBLE_WAIT).as_millis());
        assert_eq!(steps[3].0, kicked);
    }

    #[test]
    fn kick_skips_players_paused_since_take() {
        // Q stopped since the take; R started after it.
        let kicked = (NUDGE_GAP + AUDIBLE_WAIT + KICK_GAP).as_millis();
        let steps = drive(&[P, Q], |ms| {
            (ms > kicked, players(&[P, "org.mpris.MediaPlayer2.r"]))
        });
        assert_eq!(actions(&steps)[2..], [Pause(P.into()), Play(P.into())]);
    }

    #[test]
    fn silent_after_kick_falls_back_to_one_profile_cycle_then_play() {
        // Q is still playing before the cycle, P paused itself after the kick.
        let kicked = (NUDGE_GAP + AUDIBLE_WAIT + KICK_GAP).as_millis();
        let steps = drive(&[P, Q], |ms| {
            let playing = if ms > kicked {
                players(&[Q])
            } else {
                players(&[P, Q])
            };
            (false, playing)
        });
        assert_eq!(
            actions(&steps),
            [
                SuspendSink(true),
                SuspendSink(false),
                Pause(P.into()),
                Pause(Q.into()),
                Play(P.into()),
                Play(Q.into()),
                ProfileCycle,
                Play(Q.into()),
            ]
        );
        let cycle = (NUDGE_GAP + AUDIBLE_WAIT + KICK_GAP + KICK_WAIT).as_millis();
        assert_eq!(steps[4], (cycle, vec![ProfileCycle]));
        assert_eq!(steps[5].0, cycle + CYCLE_SETTLE.as_millis());

        // Audible once the profile is back: no Play after the cycle.
        let steps = drive(&[P], |ms| (ms > cycle, players(&[P])));
        assert_eq!(actions(&steps).last(), Some(&ProfileCycle));
    }

    #[test]
    fn never_cycles_profile_twice() {
        let t0 = Instant::now();
        let mut chain = Chain::new(t0, t0 + A2DP_RESUME_WINDOW, players(&[P]));
        let mut all = Vec::new();
        // Far more steps than the chain has, all silent.
        for _ in 0..40 {
            if let Some(actions) =
                chain.step(chain.due(), chain.due(), Some(false), false, &players(&[P]))
            {
                all.extend(actions);
            }
        }
        assert_eq!(all.iter().filter(|a| **a == ProfileCycle).count(), 1);
        assert_eq!(
            chain.step(chain.due(), chain.due(), Some(false), false, &[]),
            None
        );
    }

    #[test]
    fn cancel_after_suspend_still_resumes_sink() {
        let t0 = Instant::now();
        let mut chain = at_last_read(t0, t0 + A2DP_RESUME_WINDOW, players(&[P]));
        assert_eq!(
            chain.step(chain.due(), chain.due(), Some(false), false, &[]),
            Some(vec![SuspendSink(true)])
        );
        assert_eq!(chain.cancel(), [SuspendSink(false)]);
        assert_eq!(
            chain.step(chain.due(), chain.due(), Some(false), false, &players(&[P])),
            None
        );
        assert_eq!(chain.cancel(), []);

        // Cancelled after the kick's Pause: nothing more, no Play.
        let mut chain = at_last_read(t0, t0 + A2DP_RESUME_WINDOW, players(&[P]));
        for _ in 0..3 {
            chain.step(chain.due(), chain.due(), Some(false), false, &players(&[P]));
        }
        assert_eq!(chain.cancel(), []);
        assert_eq!(
            chain.step(chain.due(), chain.due(), Some(false), false, &players(&[P])),
            None
        );
    }

    #[test]
    fn resume_window_covers_chain() {
        let steps = drive(&[P], |_| (false, players(&[P])));
        let last = Duration::from_millis(steps.last().unwrap().0 as u64);
        // `drive`'s transport and audibility reads take no time; the
        // production ones do. A read still running at the read window's end
        // is unknown, so the nudge (or the unknown read) is by then.
        assert_eq!(
            TRANSPORT_START_WAIT + TRANSPORT_READ_TIMEOUT + last + AUDIBLE_SAMPLE * 3,
            CHAIN_WORST_CASE_SLEEP
        );
        // Tight: the window grows only with the chain (#331).
        assert_eq!(
            CHAIN_WORST_CASE_SLEEP + PLAY_EDGE_MARGIN + RESUME_MARGIN,
            A2DP_RESUME_WINDOW
        );
        // An unknown transport read leaves the later steps on the same times.
        let t0 = Instant::now();
        let until = t0 + A2DP_RESUME_WINDOW;
        let mut idle = at_last_read(t0, until, players(&[P]));
        idle.step(idle.due(), idle.due(), Some(false), false, &[]);
        idle.step(idle.due(), idle.due(), Some(false), false, &[]);
        let mut unknown = at_last_read(t0, until, players(&[P]));
        assert_eq!(
            unknown.step(unknown.due(), unknown.due(), None, false, &[]),
            Some(vec![])
        );
        assert_eq!(unknown.due(), idle.due());
        // The same for an unknown first read.
        let mut unknown = Chain::new(t0, until, players(&[P]));
        let first = unknown.due();
        assert_eq!(unknown.step(first, first, None, false, &[]), Some(vec![]));
        assert_eq!(unknown.due(), first + NUDGE_GAP + AUDIBLE_WAIT);
    }

    #[test]
    fn kick_or_cycle_due_within_play_edge_margin_of_window_end_is_skipped() {
        let t0 = Instant::now();
        let until = t0 + A2DP_RESUME_WINDOW;
        let nudged = || {
            let mut chain = at_last_read(t0, until, players(&[P]));
            chain.step(chain.due(), chain.due(), Some(false), false, &[]);
            chain.step(chain.due(), chain.due(), Some(false), false, &[]);
            chain
        };
        // The kick's Play would be due just PLAY_EDGE_MARGIN before the
        // window's end: no Pause. 1 ms earlier, the kick runs.
        let edge = until - KICK_GAP - PLAY_EDGE_MARGIN;
        let mut chain = nudged();
        assert_eq!(
            chain.step(edge, edge, Some(false), false, &players(&[P])),
            None
        );
        assert_eq!(chain.cancel(), []);
        let before = edge - Duration::from_millis(1);
        assert_eq!(
            nudged().step(before, before, Some(false), false, &players(&[P])),
            Some(vec![Pause(P.into())])
        );

        // The same for the cycle's replay Play.
        let kicked = || {
            let mut chain = nudged();
            chain.step(chain.due(), chain.due(), Some(false), false, &players(&[P]));
            chain.step(chain.due(), chain.due(), Some(false), false, &[]);
            chain
        };
        let edge = until - CYCLE_SETTLE - AUDIBLE_SAMPLE - PLAY_EDGE_MARGIN;
        assert_eq!(
            kicked().step(edge, edge, Some(false), false, &players(&[P])),
            None
        );
        let before = edge - Duration::from_millis(1);
        assert_eq!(
            kicked().step(before, before, Some(false), false, &players(&[P])),
            Some(vec![ProfileCycle])
        );
    }

    /// Resume I/O on tokio's clock: silent for `AUDIBLE_SAMPLE`, `P` always
    /// Playing after `read(n)` for the n-th read; records the actions.
    struct SlowIo {
        read: fn(usize) -> Duration,
        reads: AtomicUsize,
        acted: Arc<StdMutex<Vec<Action>>>,
    }

    impl A2dpResumeIo for SlowIo {
        fn act(&self, action: Action) -> BoxFuture<'static, ()> {
            self.acted.lock().unwrap().push(action);
            Box::pin(async {})
        }

        fn audible(&self) -> BoxFuture<'static, bool> {
            Box::pin(async {
                tokio::time::sleep(AUDIBLE_SAMPLE).await;
                false
            })
        }

        fn playing(&self) -> BoxFuture<'static, Vec<String>> {
            let read = (self.read)(self.reads.fetch_add(1, Ordering::SeqCst));
            Box::pin(async move {
                tokio::time::sleep(read).await;
                players(&[P])
            })
        }
    }

    /// Run the chain to its end for a take acknowledged now, the n-th
    /// `playing()` read taking `read(n)`; returns its actions and how long
    /// it ran.
    async fn run_with_reads_of(read: fn(usize) -> Duration) -> (Vec<Action>, Duration) {
        let acted = Arc::new(StdMutex::new(Vec::new()));
        let io = SlowIo {
            read,
            reads: AtomicUsize::new(0),
            acted: acted.clone(),
        };
        let (_cancel, cancelled) = oneshot::channel();
        let start = tokio::time::Instant::now();
        run(
            io,
            PlaybackState::default(),
            start.into_std() + A2DP_RESUME_WINDOW,
            cancelled,
            std::future::ready(()),
        )
        .await;
        let actions = acted.lock().unwrap().clone();
        (actions, start.elapsed())
    }

    #[tokio::test(start_paused = true)]
    async fn slow_player_reads_skip_a_kick_past_the_window() {
        // A hung MPRIS app: the reads end at 5 s, then at 12 s, past the
        // window, where the kick's stop would be released at once.
        let (acted, _) = run_with_reads_of(|_| Duration::from_secs(5)).await;
        assert_eq!(acted, [SuspendSink(true), SuspendSink(false)]);
    }

    #[tokio::test(start_paused = true)]
    async fn profile_cycle_that_would_end_past_the_window_is_skipped() {
        // 1 s reads: the kick is at 5 s, the cycle would be at 8 s and its
        // Play at 10 s, PLAY_EDGE_MARGIN before the window's end.
        let (acted, _) = run_with_reads_of(|_| Duration::from_secs(1)).await;
        assert_eq!(
            acted,
            [
                SuspendSink(true),
                SuspendSink(false),
                Pause(P.into()),
                Play(P.into())
            ]
        );
        // Instant reads: the nudge at TRANSPORT_START_WAIT, 2 s; the cycle at
        // 6 s, its Play at 7.5 s.
        let (acted, _) = run_with_reads_of(|_| Duration::ZERO).await;
        assert_eq!(acted[4..], [ProfileCycle, Play(P.into())]);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_player_read_never_delays_the_replay_after_the_cycle() {
        // Only a 4th read would be slow, longer than the whole window.
        let (acted, ran) = run_with_reads_of(|n| match n {
            3 => A2DP_RESUME_WINDOW,
            _ => Duration::ZERO,
        })
        .await;
        assert_eq!(acted[4..], [ProfileCycle, Play(P.into())]);
        // The replay Play ends the run, at 7.5 s, in time for the poll.
        assert!(ran + PLAY_EDGE_MARGIN < A2DP_RESUME_WINDOW, "{ran:?}");
    }

    /// Spawn the chain, with instant reads, for a take acknowledged now and
    /// a route that ends with `routed`; returns its recorded actions.
    fn spawn_after_route(
        routed: impl Future<Output = ()> + Send + 'static,
        cancel: oneshot::Receiver<()>,
    ) -> (Arc<StdMutex<Vec<Action>>>, tokio::task::JoinHandle<()>) {
        let acted = Arc::new(StdMutex::new(Vec::new()));
        let io = SlowIo {
            read: |_| Duration::ZERO,
            reads: AtomicUsize::new(0),
            acted: acted.clone(),
        };
        let until = tokio::time::Instant::now().into_std() + A2DP_RESUME_WINDOW;
        (
            acted,
            tokio::spawn(run(io, PlaybackState::default(), until, cancel, routed)),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn chain_waits_for_route_before_any_step() {
        let (route_done, route) = oneshot::channel::<()>();
        let (_cancel, cancelled) = oneshot::channel();
        let (acted, chain) = spawn_after_route(
            async {
                let _ = route.await;
            },
            cancelled,
        );
        // Well past the first audibility check (NUDGE_GAP + AUDIBLE_WAIT).
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(*acted.lock().unwrap(), []);
        route_done.send(()).unwrap();
        chain.await.unwrap();
        assert_eq!(
            acted.lock().unwrap()[..2],
            [SuspendSink(true), SuspendSink(false)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_while_waiting_for_route_acts_nothing() {
        for drop_sender in [false, true] {
            let (cancel, cancelled) = oneshot::channel();
            let (acted, chain) = spawn_after_route(std::future::pending(), cancelled);
            tokio::time::sleep(Duration::from_secs(1)).await;
            if drop_sender {
                drop(cancel);
            } else {
                cancel.send(()).unwrap();
            }
            let cancelled_at = tokio::time::Instant::now();
            chain.await.unwrap();
            assert_eq!(cancelled_at.elapsed(), Duration::ZERO);
            assert_eq!(*acted.lock().unwrap(), []);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn route_never_settling_ends_at_window_without_acting() {
        let (_cancel, cancelled) = oneshot::channel();
        let start = tokio::time::Instant::now();
        let (acted, chain) = spawn_after_route(std::future::pending(), cancelled);
        chain.await.unwrap();
        assert_eq!(start.elapsed(), A2DP_RESUME_WINDOW);
        assert_eq!(*acted.lock().unwrap(), []);
    }

    #[tokio::test(start_paused = true)]
    async fn route_settling_after_window_unpolled_acts_nothing() {
        let (route_done, route) = oneshot::channel::<()>();
        let (_cancel, cancelled) = oneshot::channel();
        let acted = Arc::new(StdMutex::new(Vec::new()));
        let io = SlowIo {
            read: |_| Duration::ZERO,
            reads: AtomicUsize::new(0),
            acted: acted.clone(),
        };
        let until = tokio::time::Instant::now().into_std() + A2DP_RESUME_WINDOW;
        let mut chain =
            std::pin::pin!(run(io, PlaybackState::default(), until, cancelled, async {
                let _ = route.await;
            }));
        assert!(futures::poll!(&mut chain).is_pending());
        // The window passes and the route ends before the run is polled
        // again, so both are ready at once.
        tokio::time::advance(A2DP_RESUME_WINDOW + Duration::from_secs(1)).await;
        route_done.send(()).unwrap();
        chain.await;
        assert_eq!(*acted.lock().unwrap(), []);
    }

    /// `SlowIo` with instant player reads whose n-th transport read is
    /// `read(n)`, counted in `reads`.
    struct TransportIo {
        io: SlowIo,
        read: fn(usize) -> BoxFuture<'static, Option<bool>>,
        reads: Arc<AtomicUsize>,
    }

    impl A2dpResumeIo for TransportIo {
        fn act(&self, action: Action) -> BoxFuture<'static, ()> {
            self.io.act(action)
        }

        fn audible(&self) -> BoxFuture<'static, bool> {
            self.io.audible()
        }

        fn playing(&self) -> BoxFuture<'static, Vec<String>> {
            self.io.playing()
        }

        fn transport_active(&self) -> BoxFuture<'static, Option<bool>> {
            (self.read)(self.reads.fetch_add(1, Ordering::SeqCst))
        }
    }

    type Spawned = (
        Arc<StdMutex<Vec<Action>>>,
        Arc<AtomicUsize>,
        tokio::task::JoinHandle<()>,
    );

    /// Spawn the chain on `TransportIo` with `read`, for a take acknowledged
    /// now; returns its recorded actions and transport read count.
    fn spawn_with_transport(
        read: fn(usize) -> BoxFuture<'static, Option<bool>>,
        cancel: oneshot::Receiver<()>,
    ) -> Spawned {
        let (acted, reads) = (Arc::default(), Arc::default());
        let io = TransportIo {
            io: SlowIo {
                read: |_| Duration::ZERO,
                reads: AtomicUsize::new(0),
                acted: Arc::clone(&acted),
            },
            read,
            reads: Arc::clone(&reads),
        };
        let until = tokio::time::Instant::now().into_std() + A2DP_RESUME_WINDOW;
        let ready = std::future::ready(());
        (
            acted,
            reads,
            tokio::spawn(run(io, PlaybackState::default(), until, cancel, ready)),
        )
    }

    fn reads(n: &AtomicUsize) -> usize {
        n.load(Ordering::SeqCst)
    }

    #[tokio::test(start_paused = true)]
    async fn settle_active_read_acts_nothing() {
        let (_cancel, cancelled) = oneshot::channel();
        let (acted, n, chain) = spawn_with_transport(|_| Box::pin(async { Some(true) }), cancelled);
        chain.await.unwrap();
        assert_eq!(*acted.lock().unwrap(), []);
        assert_eq!(reads(&n), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn settle_idle_then_active_within_read_window_acts_nothing() {
        // PipeWire starts the transport after the settle (#305): idle, idle,
        // then active at the third read.
        let (_cancel, cancelled) = oneshot::channel();
        let (acted, n, chain) =
            spawn_with_transport(|n| Box::pin(async move { Some(n >= 2) }), cancelled);
        chain.await.unwrap();
        assert_eq!(*acted.lock().unwrap(), []);
        assert_eq!(reads(&n), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn settle_idle_through_read_window_nudges() {
        assert_nudges_in_read_window(|_| Box::pin(async { Some(false) })).await;
    }

    /// A transport read that takes `ms`, then reads `active`.
    fn read_after(ms: u64, active: bool) -> BoxFuture<'static, Option<bool>> {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            Some(active)
        })
    }

    #[tokio::test(start_paused = true)]
    async fn settle_slow_idle_reads_never_nudge_early() {
        // Read latency must not pull the nudge before the last read's time.
        assert_nudges_in_read_window(|_| read_after(50, false)).await;
        assert_nudges_in_read_window(|n| read_after(if n == 0 { 301 } else { 0 }, false)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn settle_slow_reads_then_active_late_in_read_window_acts_nothing() {
        // 50 ms reads issued every 150 ms from 500 ms to 1850 ms, then at
        // 2000 ms; the transport is active by the last.
        let (_cancel, cancelled) = oneshot::channel();
        let (acted, n, chain) = spawn_with_transport(|n| read_after(50, n >= 10), cancelled);
        chain.await.unwrap();
        assert_eq!(*acted.lock().unwrap(), []);
        assert_eq!(reads(&n), 11);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_idle_read_back_after_nudge_time_never_nudges() {
        // 170 ms idle reads issued at 500, 770, 1040, 1310, 1580, 1850 and
        // 2020 ms: the 1850 ms read is back at 2020 ms, after the nudge time,
        // so it reads again at once, and only that read nudges.
        let (_cancel, cancelled) = oneshot::channel();
        let (acted, n, _chain) = spawn_with_transport(|_| read_after(170, false), cancelled);
        assert_eq!(TRANSPORT_START_WAIT, ms(2000));
        tokio::time::sleep(ms(2189)).await;
        assert_eq!(*acted.lock().unwrap(), []);
        assert_eq!(reads(&n), 7);
        tokio::time::sleep(ms(1)).await;
        // The 2020 ms read's timer fires with ours; let the chain act on it.
        tokio::task::yield_now().await;
        assert_eq!(*acted.lock().unwrap(), [SuspendSink(true)]);
    }

    /// Run the chain on transport reads `read` that stay idle: nothing acts
    /// before a read issued at `TRANSPORT_START_WAIT`, the nudge comes by the
    /// read window's end, after more than one read.
    async fn assert_nudges_in_read_window(read: fn(usize) -> BoxFuture<'static, Option<bool>>) {
        let (_cancel, cancelled) = oneshot::channel();
        let (acted, n, chain) = spawn_with_transport(read, cancelled);
        let lower = TRANSPORT_START_WAIT;
        tokio::time::sleep(lower - Duration::from_millis(1)).await;
        assert_eq!(*acted.lock().unwrap(), []);
        tokio::time::sleep(TRANSPORT_START_WAIT + TRANSPORT_READ_TIMEOUT - lower).await;
        assert_eq!(*acted.lock().unwrap(), [SuspendSink(true)]);
        chain.await.unwrap();
        assert_eq!(
            acted.lock().unwrap()[..2],
            [SuspendSink(true), SuspendSink(false)]
        );
        assert!(reads(&n) >= 2, "{}", reads(&n));
    }

    /// The chain kicked (Pause, then Play) without any `SuspendSink`.
    fn assert_kicked_without_nudge(acted: &[Action]) {
        assert!(
            !acted.iter().any(|a| matches!(a, SuspendSink(_))),
            "{acted:?}"
        );
        let at = |action: Action| acted.iter().position(|a| *a == action);
        assert!(at(Pause(P.into())).is_some(), "{acted:?}");
        assert!(at(Pause(P.into())) < at(Play(P.into())), "{acted:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn settle_unknown_read_skips_nudge_then_kicks() {
        let (_cancel, cancelled) = oneshot::channel();
        let (acted, n, chain) = spawn_with_transport(|_| Box::pin(async { None }), cancelled);
        chain.await.unwrap();
        assert_kicked_without_nudge(&acted.lock().unwrap());
        assert_eq!(reads(&n), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn settle_slow_read_counts_as_unknown() {
        let (_cancel, cancelled) = oneshot::channel();
        let (acted, n, chain) =
            spawn_with_transport(|_| Box::pin(std::future::pending()), cancelled);
        chain.await.unwrap();
        assert_kicked_without_nudge(&acted.lock().unwrap());
        assert_eq!(reads(&n), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn settle_idle_then_unknown_read_skips_nudge() {
        let (_cancel, cancelled) = oneshot::channel();
        let (acted, n, chain) = spawn_with_transport(
            |n| Box::pin(async move { (n == 0).then_some(false) }),
            cancelled,
        );
        chain.await.unwrap();
        assert_kicked_without_nudge(&acted.lock().unwrap());
        assert_eq!(reads(&n), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn settle_idle_then_slow_read_skips_nudge() {
        // The re-read is still pending at the read window's end.
        let (_cancel, cancelled) = oneshot::channel();
        let (acted, n, chain) = spawn_with_transport(
            |n| match n {
                0 => Box::pin(async { Some(false) }),
                _ => Box::pin(std::future::pending()),
            },
            cancelled,
        );
        chain.await.unwrap();
        assert_kicked_without_nudge(&acted.lock().unwrap());
        assert_eq!(reads(&n), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn settle_cancel_during_settle_acts_nothing() {
        let (cancel, cancelled) = oneshot::channel();
        let (acted, n, chain) =
            spawn_with_transport(|_| Box::pin(async { Some(false) }), cancelled);
        tokio::time::sleep(RESUME_SETTLE / 2).await;
        cancel.send(()).unwrap();
        chain.await.unwrap();
        assert_eq!(*acted.lock().unwrap(), []);
        assert_eq!(reads(&n), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn settle_cancel_during_reread_acts_nothing() {
        let (cancel, cancelled) = oneshot::channel();
        let (acted, n, chain) =
            spawn_with_transport(|_| Box::pin(async { Some(false) }), cancelled);
        tokio::time::sleep(RESUME_SETTLE + TRANSPORT_REREAD + TRANSPORT_REREAD / 2).await;
        assert_eq!(reads(&n), 2);
        cancel.send(()).unwrap();
        chain.await.unwrap();
        assert_eq!(*acted.lock().unwrap(), []);
    }

    #[tokio::test(start_paused = true)]
    async fn settle_cancel_during_read_acts_nothing() {
        let (cancel, cancelled) = oneshot::channel();
        let (acted, n, chain) =
            spawn_with_transport(|_| Box::pin(std::future::pending()), cancelled);
        tokio::time::sleep(RESUME_SETTLE + TRANSPORT_READ_TIMEOUT / 2).await;
        assert_eq!(reads(&n), 1);
        cancel.send(()).unwrap();
        chain.await.unwrap();
        assert_eq!(*acted.lock().unwrap(), []);
    }

    /// A take for the self-pause watch (#327), on tokio's clock. `Q` is
    /// Playing at the take (Paused with `paused_at_take`) until it pauses
    /// itself at each of `pauses` after the route, or the chain's Pause or
    /// ProfileCycle pauses it; a Play plays it again. It leaves MPRIS `gone`
    /// after the route. The route ends `route` after the take; the transport
    /// reads `transport`; audio is silent for `AUDIBLE_SAMPLE`; a Paused read
    /// takes `read` and reads `Q` at its end. LibrePods' pause count is
    /// `count`, plus one from `bump` after the route.
    #[derive(Default)]
    struct Take {
        route: Duration,
        pauses: Vec<Duration>,
        paused_at_take: bool,
        gone: Option<Duration>,
        transport: Option<bool>,
        read: Duration,
        count: usize,
        bump: Option<Duration>,
    }

    /// `Take`'s I/O; records each action with its time since the route.
    #[derive(Clone)]
    struct TakeIo {
        take: Arc<Take>,
        routed_at: tokio::time::Instant,
        acted: Arc<StdMutex<Vec<(Duration, Action)>>>,
    }

    impl TakeIo {
        /// Whether `at` after the route has passed; never before the route.
        fn past(&self, at: Duration) -> bool {
            Some(at) <= tokio::time::Instant::now().checked_duration_since(self.routed_at)
        }

        /// `Q` now: Some(playing), or None once it left MPRIS.
        fn q(&self) -> Option<bool> {
            if self.take.gone.is_some_and(|at| self.past(at)) {
                return None;
            }
            let mut events: Vec<_> = self.take.pauses.iter().map(|at| (*at, false)).collect();
            events.extend(
                self.acted
                    .lock()
                    .unwrap()
                    .iter()
                    .filter_map(|(at, a)| match a {
                        Pause(_) | ProfileCycle => Some((*at, false)),
                        Play(_) => Some((*at, true)),
                        SuspendSink(_) => None,
                    }),
            );
            events.sort_by_key(|(at, _)| *at);
            let last = events.into_iter().filter(|(at, _)| self.past(*at)).last();
            Some(last.map_or(!self.take.paused_at_take, |(_, playing)| playing))
        }
    }

    impl A2dpResumeIo for TakeIo {
        fn act(&self, action: Action) -> BoxFuture<'static, ()> {
            let at = tokio::time::Instant::now().duration_since(self.routed_at);
            self.acted.lock().unwrap().push((at, action));
            Box::pin(async {})
        }

        fn audible(&self) -> BoxFuture<'static, bool> {
            Box::pin(async {
                tokio::time::sleep(AUDIBLE_SAMPLE).await;
                false
            })
        }

        fn playing(&self) -> BoxFuture<'static, Vec<String>> {
            let playing = self.q() == Some(true);
            Box::pin(async move { if playing { players(&[Q]) } else { vec![] } })
        }

        fn transport_active(&self) -> BoxFuture<'static, Option<bool>> {
            let transport = self.take.transport;
            Box::pin(async move { transport })
        }

        fn paused(&self) -> BoxFuture<'static, Vec<String>> {
            let io = self.clone();
            Box::pin(async move {
                tokio::time::sleep(io.take.read).await;
                if io.q() == Some(false) {
                    players(&[Q])
                } else {
                    vec![]
                }
            })
        }

        fn self_pauses(&self) -> usize {
            self.take.count + usize::from(self.take.bump.is_some_and(|at| self.past(at)))
        }
    }

    /// Run `take` from now until it ends or `cancel`; returns its actions
    /// with their ms since the route.
    async fn run_take(take: Take, cancel: oneshot::Receiver<()>) -> Vec<(u128, Action)> {
        let now = tokio::time::Instant::now();
        let routed_at = now + take.route;
        let io = TakeIo {
            take: Arc::new(take),
            routed_at,
            acted: Arc::default(),
        };
        let acted = Arc::clone(&io.acted);
        let until = now.into_std() + A2DP_RESUME_WINDOW;
        run(
            io,
            PlaybackState::default(),
            until,
            cancel,
            tokio::time::sleep_until(routed_at),
        )
        .await;
        let acted = acted.lock().unwrap();
        acted
            .iter()
            .map(|(at, a)| (at.as_millis(), a.clone()))
            .collect()
    }

    async fn take(take: Take) -> Vec<(u128, Action)> {
        let (_cancel, cancelled) = oneshot::channel();
        run_take(take, cancelled).await
    }

    /// `Q` pauses itself at each of `pauses` ms after the route; the first
    /// transport read is active, so the chain ends there.
    fn self_paused(pauses: &[u64]) -> Take {
        Take {
            pauses: pauses.iter().map(|p| ms(*p)).collect(),
            transport: Some(true),
            ..Take::default()
        }
    }

    #[test]
    fn self_pause_guard_and_read_interval() {
        assert_eq!(SELF_PAUSE_GUARD, Duration::from_secs(3));
        assert!(SELF_PAUSE_READ <= ms(250));
    }

    #[tokio::test(start_paused = true)]
    async fn self_pause_candidates_are_read_before_the_route() {
        // Playing until the route ends, Paused from then on.
        let acted = take(Take {
            route: ms(300),
            ..self_paused(&[0])
        })
        .await;
        assert_eq!(acted, [(0, Play(Q.into()))]);
    }

    #[tokio::test(start_paused = true)]
    async fn self_pause_within_guard_gets_one_play() {
        // The first transport read, active, ends the chain at 500 ms.
        assert_eq!(take(self_paused(&[50])).await, [(250, Play(Q.into()))]);
        // The read due at the guard's end gets time to finish.
        let late = Take {
            read: ms(20),
            ..self_paused(&[2900])
        };
        assert_eq!(take(late).await, [(3020, Play(Q.into()))]);
    }

    #[tokio::test(start_paused = true)]
    async fn self_pause_after_guard_gets_no_play() {
        assert_eq!(take(self_paused(&[3100])).await, []);
        // A slow read running past the guard doesn't see that pause either.
        let slow = Take {
            read: ms(2000),
            ..self_paused(&[3100])
        };
        assert_eq!(take(slow).await, []);
    }

    /// P and Q pause themselves at `paused_at` (ms after the route, the
    /// test's start), and each Play takes 1 s.
    #[derive(Clone)]
    struct TwoIo {
        paused_at: [u64; 2],
        start: tokio::time::Instant,
        acted: Arc<StdMutex<Vec<(u128, Action)>>>,
        count: Arc<AtomicUsize>,
    }

    impl TwoIo {
        fn new(paused_at: [u64; 2]) -> Self {
            TwoIo {
                paused_at,
                start: tokio::time::Instant::now(),
                acted: Arc::default(),
                count: Arc::default(),
            }
        }
    }

    impl A2dpResumeIo for TwoIo {
        fn act(&self, action: Action) -> BoxFuture<'static, ()> {
            let at = self.start.elapsed().as_millis();
            self.acted.lock().unwrap().push((at, action));
            Box::pin(tokio::time::sleep(Duration::from_secs(1)))
        }

        fn audible(&self) -> BoxFuture<'static, bool> {
            Box::pin(async { false })
        }

        fn playing(&self) -> BoxFuture<'static, Vec<String>> {
            Box::pin(async { players(&[P, Q]) })
        }

        fn transport_active(&self) -> BoxFuture<'static, Option<bool>> {
            Box::pin(async { Some(true) })
        }

        fn paused(&self) -> BoxFuture<'static, Vec<String>> {
            let elapsed = self.start.elapsed();
            let paused = [P, Q]
                .into_iter()
                .zip(self.paused_at)
                .filter(|(_, at)| ms(*at) <= elapsed)
                .map(|(p, _)| p.to_string())
                .collect();
            Box::pin(async { paused })
        }

        fn self_pauses(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_or_librepods_pause_during_a_slow_play_stops_the_next() {
        for cancel_it in [true, false] {
            let io = TwoIo::new([0, 0]);
            let (cancel, cancelled) = oneshot::channel();
            let until = tokio::time::Instant::now().into_std() + A2DP_RESUME_WINDOW;
            let ready = std::future::ready(());
            let run = tokio::spawn(run(
                io.clone(),
                PlaybackState::default(),
                until,
                cancelled,
                ready,
            ));
            // During P's Play.
            tokio::time::sleep(ms(100)).await;
            if cancel_it {
                cancel.send(()).unwrap();
            } else {
                io.count.fetch_add(1, Ordering::SeqCst);
            }
            run.await.unwrap();
            assert_eq!(
                *io.acted.lock().unwrap(),
                [(0, Play(P.into()))],
                "{cancel_it}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn slow_play_holds_up_no_paused_read() {
        // P's Play runs from 2750 to 3750 ms; the 3000 ms read still sees Q.
        let io = TwoIo::new([2750, 2900]);
        let (_cancel, cancelled) = oneshot::channel();
        let until = tokio::time::Instant::now().into_std() + A2DP_RESUME_WINDOW;
        run(
            io.clone(),
            PlaybackState::default(),
            until,
            cancelled,
            std::future::ready(()),
        )
        .await;
        assert_eq!(
            *io.acted.lock().unwrap(),
            [(2750, Play(P.into())), (3750, Play(Q.into()))]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn second_self_pause_gets_no_play() {
        assert_eq!(
            take(self_paused(&[50, 1000])).await,
            [(250, Play(Q.into()))]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn player_not_playing_at_take_gets_no_play() {
        let acted = take(Take {
            paused_at_take: true,
            ..self_paused(&[])
        })
        .await;
        assert_eq!(acted, []);
    }

    #[tokio::test(start_paused = true)]
    async fn player_gone_from_mpris_gets_no_play() {
        let acted = take(Take {
            gone: Some(ms(100)),
            ..self_paused(&[50])
        })
        .await;
        assert_eq!(acted, []);
    }

    #[tokio::test(start_paused = true)]
    async fn self_pause_play_within_play_edge_margin_of_window_end_is_skipped() {
        let edge = A2DP_RESUME_WINDOW - PLAY_EDGE_MARGIN;
        let late = |route| {
            take(Take {
                route,
                ..self_paused(&[0])
            })
        };
        assert_eq!(late(edge).await, []);
        assert_eq!(late(edge - ms(1)).await, [(0, Play(Q.into()))]);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_paused_reads_never_delay_a_chain_step() {
        let idle = |read| Take {
            transport: Some(false),
            read,
            ..Take::default()
        };
        let fast = take(idle(Duration::ZERO)).await;
        assert_eq!(fast[0], (2000, SuspendSink(true)));
        assert_eq!(take(idle(Duration::from_secs(1))).await, fast);
    }

    #[tokio::test(start_paused = true)]
    async fn kick_and_cycle_plays_are_the_only_plays_for_players_they_pause() {
        // An unknown transport read (an idle one nudges first and kicks
        // after the watch, #331): the kick pauses Q at 2.5 s; a Paused read
        // sees it at 2.75 s, or, slow, is back from 0 s at 2.6 s.
        for read in [Duration::ZERO, ms(2600)] {
            let acted = take(Take {
                read,
                ..Take::default()
            })
            .await;
            let acted: Vec<_> = acted.into_iter().map(|(_, a)| a).collect();
            assert_eq!(
                acted,
                [
                    Pause(Q.into()),
                    Play(Q.into()),
                    ProfileCycle,
                    Play(Q.into()),
                ],
                "{read:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn self_pause_before_the_kick_is_played_once_then_cycled() {
        // After an unknown transport read (#331), paused at the kick's check,
        // so not kicked; played again at 2.52 s, so the cycle's Play is the
        // only one after it.
        let acted = take(Take {
            pauses: vec![ms(2400)],
            read: ms(20),
            ..Take::default()
        })
        .await;
        assert_eq!(
            acted,
            [
                (2520, Play(Q.into())),
                (4500, ProfileCycle),
                (6500, Play(Q.into())),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_after_self_pause_plays_nothing() {
        for drop_sender in [false, true] {
            let (cancel, cancelled) = oneshot::channel();
            let run = tokio::spawn(run_take(self_paused(&[100]), cancelled));
            // Before the 250 ms read.
            tokio::time::sleep(ms(200)).await;
            if drop_sender {
                drop(cancel);
            } else {
                cancel.send(()).unwrap();
            }
            let cancelled_at = tokio::time::Instant::now();
            assert_eq!(run.await.unwrap(), []);
            assert_eq!(cancelled_at.elapsed(), Duration::ZERO);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn librepods_pause_since_take_suppresses_the_play() {
        // An ear-removal pause at 100 ms.
        let acted = take(Take {
            bump: Some(ms(100)),
            ..self_paused(&[100])
        })
        .await;
        assert_eq!(acted, []);
        // Pauses before the take don't count.
        let acted = take(Take {
            count: 5,
            ..self_paused(&[50])
        })
        .await;
        assert_eq!(acted, [(250, Play(Q.into()))]);
    }

    /// R's bus owner lookup fails.
    const R: &str = "org.mpris.MediaPlayer2.vlc";
    /// P owns PID 1's live stream; Q, a remote (Spotify Connect), is owned
    /// by PID 2, which has none.
    const OWNERS: [(&str, Option<u32>); 3] = [(P, Some(1)), (Q, Some(2)), (R, None)];

    /// The stream monitor's state with `pids` live; None is unknown.
    fn live(pids: Option<&[u32]>) -> PlaybackState {
        PlaybackState {
            live: pids.map(|pids| pids.iter().copied().collect()),
            ..PlaybackState::default()
        }
    }

    /// `owners` Playing at the take, which the route ends at once. Q pauses
    /// itself at 50 ms, before any action; the chain's Pause or ProfileCycle
    /// pauses a player, a Play plays it. The transport reads unknown, audio
    /// is silent.
    #[derive(Clone)]
    struct OwnIo {
        owners: Vec<(&'static str, Option<u32>)>,
        start: tokio::time::Instant,
        acted: Arc<StdMutex<Vec<Action>>>,
    }

    impl OwnIo {
        fn new(owners: &[(&'static str, Option<u32>)]) -> Self {
            OwnIo {
                owners: owners.to_vec(),
                start: tokio::time::Instant::now(),
                acted: Arc::default(),
            }
        }

        fn plays(&self, player: &str) -> bool {
            let self_paused = player == Q && self.start.elapsed() >= ms(50);
            self.acted
                .lock()
                .unwrap()
                .iter()
                .fold(!self_paused, |plays, action| match action {
                    Pause(p) if p == player => false,
                    Play(p) if p == player => true,
                    ProfileCycle => false,
                    _ => plays,
                })
        }

        /// The players whose playing is `plays` and whose owner `own` accepts.
        fn listing(&self, plays: bool, own: impl Fn(Option<u32>) -> bool) -> Vec<String> {
            self.owners
                .iter()
                .filter(|(p, owner)| self.plays(p) == plays && own(*owner))
                .map(|(p, _)| p.to_string())
                .collect()
        }
    }

    impl A2dpResumeIo for OwnIo {
        fn act(&self, action: Action) -> BoxFuture<'static, ()> {
            self.acted.lock().unwrap().push(action);
            Box::pin(async {})
        }

        fn audible(&self) -> BoxFuture<'static, bool> {
            Box::pin(async { false })
        }

        fn playing(&self) -> BoxFuture<'static, Vec<String>> {
            let playing = self.listing(true, |_| true);
            Box::pin(async { playing })
        }

        fn playing_owned(
            &self,
            own: Box<dyn Fn(Option<u32>) -> bool + Send>,
        ) -> BoxFuture<'static, Vec<String>> {
            let playing = self.listing(true, own);
            Box::pin(async { playing })
        }

        fn transport_active(&self) -> BoxFuture<'static, Option<bool>> {
            Box::pin(async { None })
        }

        fn paused(&self) -> BoxFuture<'static, Vec<String>> {
            let paused = self.listing(false, |_| true);
            Box::pin(async { paused })
        }
    }

    async fn take_candidates_of(pids: Option<&[u32]>) -> Vec<String> {
        take_candidates(&OwnIo::new(&OWNERS), live(pids)).await
    }

    #[tokio::test(start_paused = true)]
    async fn take_candidates_keep_owners_of_a_live_stream() {
        // Q owns no live stream; R's failed owner lookup owns nothing.
        assert_eq!(take_candidates_of(Some(&[1])).await, [P]);
        assert_eq!(take_candidates_of(Some(&[1, 2])).await, [P, Q]);
    }

    #[tokio::test(start_paused = true)]
    async fn take_candidates_keep_every_player_while_live_is_unknown() {
        assert_eq!(take_candidates_of(None).await, [P, Q, R]);
    }

    #[tokio::test(start_paused = true)]
    async fn take_candidates_none_without_a_live_stream() {
        assert_eq!(take_candidates_of(Some(&[])).await, [] as [&str; 0]);
    }

    /// Run the chain on P and Q with `streams` taken at the take; its actions.
    async fn own_take(streams: PlaybackState) -> Vec<Action> {
        let io = OwnIo::new(&OWNERS[..2]);
        let (_cancel, cancelled) = oneshot::channel();
        let until = tokio::time::Instant::now().into_std() + A2DP_RESUME_WINDOW;
        run(
            io.clone(),
            streams,
            until,
            cancelled,
            std::future::ready(()),
        )
        .await;
        io.acted.lock().unwrap().clone()
    }

    #[tokio::test(start_paused = true)]
    async fn take_with_a_live_stream_acts_only_on_its_owner() {
        // Q, a remote, is never paused or played, not even after pausing
        // itself within the guard. A snapshot taken before Spotify corks at
        // the route (#330) still kicks P.
        let (monitor, streams) = tokio::sync::watch::channel(live(Some(&[1])));
        let snapshot = streams.borrow().clone();
        monitor.send(live(Some(&[]))).unwrap();
        assert_eq!(
            own_take(snapshot).await,
            [
                Pause(P.into()),
                Play(P.into()),
                ProfileCycle,
                Play(P.into())
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn take_with_unknown_live_presence_acts_on_every_player() {
        assert_eq!(
            own_take(PlaybackState::default()).await,
            [
                // The watch's Play for Q's self-pause.
                Play(Q.into()),
                Pause(P.into()),
                Pause(Q.into()),
                Play(P.into()),
                Play(Q.into()),
                ProfileCycle,
                Play(P.into()),
                Play(Q.into()),
            ]
        );
    }

    #[test]
    fn read_transport_once_empty_mac_is_unknown() {
        assert_eq!(read_transport_once(""), None);
    }

    /// A `read_shared` opener that counts its calls and opens bus `n`.
    fn opener(opens: &Cell<usize>, n: u32) -> impl FnOnce() -> Option<u32> {
        move || {
            opens.set(opens.get() + 1);
            Some(n)
        }
    }

    #[test]
    fn shared_bus_reads_reuse_one_open() {
        let (slot, opens) = (Mutex::new(None), Cell::new(0));
        assert_eq!(
            read_shared(&slot, opener(&opens, 1), |_| Some(true)),
            Some(true)
        );
        assert_eq!(
            read_shared(&slot, opener(&opens, 2), |bus| Some(*bus != 1)),
            Some(false)
        );
        assert_eq!(opens.get(), 1);
    }

    #[test]
    fn shared_bus_unknown_read_reopens() {
        let (slot, opens) = (Mutex::new(None), Cell::new(0));
        assert_eq!(read_shared(&slot, opener(&opens, 1), |_| None), None);
        assert_eq!(
            read_shared(&slot, opener(&opens, 2), |bus| Some(*bus == 2)),
            Some(true)
        );
        assert_eq!(opens.get(), 2);
        // A failed open reads unknown without reading.
        let failed = read_shared(&Mutex::new(None::<u32>), || None, |_| panic!("read"));
        assert_eq!(failed, None);
    }

    #[test]
    fn shared_bus_busy_is_unknown() {
        let slot = Mutex::new(None);
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let busy_opens = AtomicUsize::new(0);
        std::thread::scope(|s| {
            let first = s.spawn(|| {
                let open = move || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Some(1)
                };
                read_shared(&slot, open, |_| Some(true))
            });
            entered_rx.recv().unwrap();
            let start = Instant::now();
            let open = || {
                busy_opens.fetch_add(1, Ordering::SeqCst);
                Some(2)
            };
            let busy = read_shared(&slot, open, |_| Some(false));
            let took = start.elapsed();
            // Release before asserting, so a failure can't hang the scope.
            release_tx.send(()).unwrap();
            assert_eq!(first.join().unwrap(), Some(true));
            assert_eq!(busy, None);
            assert!(took < ms(50), "busy read took {took:?}");
            assert_eq!(busy_opens.load(Ordering::SeqCst), 0);
        });
    }

    #[test]
    fn shared_bus_poisoned_slot_reopens() {
        let (slot, opens) = (Mutex::new(None), Cell::new(0));
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            read_shared(&slot, opener(&opens, 1), |_| panic!("read"))
        }));
        assert!(panicked.is_err() && slot.is_poisoned());
        assert_eq!(
            read_shared(&slot, opener(&opens, 2), |bus| Some(*bus == 2)),
            Some(true)
        );
        assert_eq!(
            read_shared(&slot, opener(&opens, 3), |bus| Some(*bus == 2)),
            Some(true)
        );
        assert_eq!(opens.get(), 2);
    }

    #[test]
    fn device_fragment_matches_bluez_path() {
        let device = device_fragment("aa:bb:cc:dd:ee:ff");
        assert_eq!(device, "/dev_AA_BB_CC_DD_EE_FF/");
        assert!(FD0.contains(&device));
    }

    #[test]
    fn transport_poller_sends_first_state_then_changes() {
        let (tx, mut rx) = mpsc::channel(8);
        let mut script = vec![
            Some(false),
            Some(false),
            None,
            Some(true),
            Some(true),
            Some(false),
        ]
        .into_iter();
        poll_transport(&tx, Duration::ZERO, || {
            script.next().unwrap_or_else(|| {
                // Script done: the receiver goes away, so the poller stops.
                rx.close();
                None
            })
        });
        let mut sent = Vec::new();
        while let Ok(active) = rx.try_recv() {
            sent.push(active);
        }
        assert_eq!(sent, [false, true, false]);
    }

    /// Counting fake BlueZ for `TransportReader`: `paths` is what a lookup
    /// finds, `states` each path's State (absent: gone); None fails the bus.
    /// `lookup_seq` overrides `paths` for the next lookups, one per lookup.
    #[derive(Default)]
    struct FakeBlueZ {
        paths: Option<Vec<&'static str>>,
        lookup_seq: VecDeque<Option<Vec<&'static str>>>,
        states: Vec<(&'static str, Option<&'static str>)>,
        lookups: usize,
        reads: usize,
    }

    impl FakeBlueZ {
        fn read(&mut self, reader: &mut TransportReader) -> Option<bool> {
            let (lookup, state) = self.calls();
            reader.read(lookup, state)
        }

        /// The bus calls: (lookup, state).
        fn calls(
            &mut self,
        ) -> (
            impl FnMut() -> Option<Vec<String>> + '_,
            impl FnMut(&str) -> Result<Option<bool>, Gone> + '_,
        ) {
            let (paths, states) = (&self.paths, &self.states);
            let lookup_seq = &mut self.lookup_seq;
            let (lookups, reads) = (&mut self.lookups, &mut self.reads);
            (
                move || {
                    *lookups += 1;
                    lookup_seq
                        .pop_front()
                        .unwrap_or_else(|| paths.clone())
                        .map(|p| p.iter().map(|p| p.to_string()).collect())
                },
                move |path: &str| {
                    *reads += 1;
                    match states.iter().find(|(p, _)| *p == path) {
                        Some((_, Some(state))) => Ok(Some(*state != "idle")),
                        Some((_, None)) => Ok(None),
                        None => Err(Gone),
                    }
                },
            )
        }

        /// `read_transport_once`'s budgeted read, each call taking `cost` on
        /// an injected clock. Also returns each call's (time into the read,
        /// proxy timeout).
        fn read_fresh(&mut self, cost: Duration) -> (Option<bool>, Vec<(Duration, Duration)>) {
            let t0 = Instant::now();
            let clock = Cell::new(t0);
            let calls = RefCell::new(Vec::new());
            let call = |timeout| {
                calls.borrow_mut().push((clock.get() - t0, timeout));
                clock.set(clock.get() + cost);
            };
            let (mut lookup, mut state) = self.calls();
            let active = read_within(
                t0 + TRANSPORT_READ_TIMEOUT,
                || clock.get(),
                |timeout| {
                    call(timeout);
                    lookup()
                },
                |path, timeout| {
                    call(timeout);
                    state(path)
                },
            );
            (active, calls.into_inner())
        }
    }

    const FD0: &str = "/org/bluez/hci0/dev_AA_BB_CC_DD_EE_FF/fd0";
    const FD1: &str = "/org/bluez/hci0/dev_AA_BB_CC_DD_EE_FF/fd1";
    const FD2: &str = "/org/bluez/hci0/dev_AA_BB_CC_DD_EE_FF/fd2";

    #[test]
    fn transport_reader_looks_up_path_once_then_reads_state() {
        let mut bluez = FakeBlueZ {
            paths: Some(vec![FD0]),
            ..Default::default()
        };
        let mut reader = TransportReader::default();
        for (tick, state) in ["idle", "pending", "active", "idle"]
            .iter()
            .cycle()
            .take(12)
            .enumerate()
        {
            bluez.states = vec![(FD0, Some(*state))];
            assert_eq!(
                bluez.read(&mut reader),
                Some(*state != "idle"),
                "tick {tick}"
            );
        }
        assert_eq!((bluez.lookups, bluez.reads), (1, 12));
    }

    #[test]
    fn transport_reader_relooks_up_when_path_gone() {
        let mut bluez = FakeBlueZ {
            paths: Some(vec![FD0]),
            states: vec![(FD0, Some("idle"))],
            ..Default::default()
        };
        let mut reader = TransportReader::default();
        assert_eq!(bluez.read(&mut reader), Some(false));
        // BlueZ re-created the transport as fd1, now active: seen this tick.
        bluez.paths = Some(vec![FD1]);
        bluez.states = vec![(FD1, Some("active"))];
        assert_eq!(bluez.read(&mut reader), Some(true));
        assert_eq!((bluez.lookups, bluez.reads), (2, 3));
        assert_eq!(bluez.read(&mut reader), Some(true));
        assert_eq!((bluez.lookups, bluez.reads), (2, 4));
    }

    #[test]
    fn transport_reader_without_transport_reads_idle_and_retries() {
        let mut bluez = FakeBlueZ {
            paths: Some(vec![]),
            ..Default::default()
        };
        let mut reader = TransportReader::default();
        assert_eq!(bluez.read(&mut reader), Some(false));
        assert_eq!(bluez.read(&mut reader), Some(false));
        assert_eq!((bluez.lookups, bluez.reads), (2, 0));
        bluez.paths = Some(vec![FD0]);
        bluez.states = vec![(FD0, Some("pending"))];
        assert_eq!(bluez.read(&mut reader), Some(true));
        assert_eq!((bluez.lookups, bluez.reads), (3, 1));
    }

    #[test]
    fn transport_reader_gone_path_never_reads_idle() {
        // Gone right after both lookups of a read: inconclusive, not idle.
        let mut bluez = FakeBlueZ {
            paths: Some(vec![FD0]),
            ..Default::default()
        };
        let mut reader = TransportReader::default();
        assert_eq!(bluez.read(&mut reader), None);
        assert!(reader.paths.is_empty());
        assert_eq!((bluez.lookups, bluez.reads), (2, 2));
        bluez.states = vec![(FD0, Some("idle"))];
        assert_eq!(bluez.read(&mut reader), Some(false));
        assert_eq!((bluez.lookups, bluez.reads), (3, 3));

        // Replaced (fd0 -> fd1) between the lookup and its read: seen this read.
        let mut bluez = FakeBlueZ {
            lookup_seq: [Some(vec![FD0]), Some(vec![FD1])].into(),
            states: vec![(FD1, Some("active"))],
            ..Default::default()
        };
        let mut reader = TransportReader::default();
        assert_eq!(bluez.read(&mut reader), Some(true));
        assert_eq!(bluez.lookups, 2);
        assert_eq!(reader.paths, [FD1]);
    }

    #[test]
    fn transport_reader_gone_beside_live_path_relooks_up_same_read() {
        // Cached fd1 is re-created as fd2 beside a live fd0: the read that
        // finds fd1 gone looks up again and reads fd2.
        let mut bluez = FakeBlueZ {
            paths: Some(vec![FD0, FD1]),
            states: vec![(FD0, Some("idle")), (FD1, Some("idle"))],
            ..Default::default()
        };
        let mut reader = TransportReader::default();
        assert_eq!(bluez.read(&mut reader), Some(false));
        bluez.paths = Some(vec![FD0, FD2]);
        bluez.states = vec![(FD0, Some("idle")), (FD2, Some("active"))];
        assert_eq!(bluez.read(&mut reader), Some(true));
        assert_eq!((bluez.lookups, bluez.reads), (2, 6));
        assert_eq!(reader.paths, [FD0, FD2]);

        // Fresh lookup lists an active fd0 beside a gone fd1; the next
        // lookup drops fd1: active in the same read.
        let mut bluez = FakeBlueZ {
            lookup_seq: [Some(vec![FD0, FD1]), Some(vec![FD0])].into(),
            states: vec![(FD0, Some("active"))],
            ..Default::default()
        };
        let mut reader = TransportReader::default();
        assert_eq!(bluez.read(&mut reader), Some(true));
        assert_eq!(bluez.lookups, 2);
        assert_eq!(reader.paths, [FD0]);

        // Every lookup still lists the gone fd1: inconclusive, never idle.
        let mut bluez = FakeBlueZ {
            paths: Some(vec![FD0, FD1]),
            states: vec![(FD0, Some("active"))],
            ..Default::default()
        };
        assert_eq!(bluez.read(&mut TransportReader::default()), None);
        assert_eq!((bluez.lookups, bluez.reads), (2, 4));
    }

    #[test]
    fn transport_reader_any_non_idle_is_active() {
        let mut bluez = FakeBlueZ {
            paths: Some(vec![FD0, FD1]),
            states: vec![(FD0, Some("idle")), (FD1, Some("active"))],
            ..Default::default()
        };
        let mut reader = TransportReader::default();
        assert_eq!(bluez.read(&mut reader), Some(true));
        bluez.states = vec![(FD0, Some("idle")), (FD1, Some("idle"))];
        assert_eq!(bluez.read(&mut reader), Some(false));
        assert_eq!((bluez.lookups, bluez.reads), (1, 4));
    }

    #[test]
    fn transport_reader_bus_failure_returns_none_and_clears_cache() {
        let mut bluez = FakeBlueZ {
            paths: Some(vec![FD0]),
            states: vec![(FD0, Some("active"))],
            ..Default::default()
        };
        let mut reader = TransportReader::default();
        assert_eq!(bluez.read(&mut reader), Some(true));
        bluez.states = vec![(FD0, None)];
        assert_eq!(bluez.read(&mut reader), None);
        assert!(reader.paths.is_empty());
        // The failed lookup too.
        bluez.paths = None;
        assert_eq!(bluez.read(&mut reader), None);
        bluez.paths = Some(vec![FD0]);
        bluez.states = vec![(FD0, Some("active"))];
        assert_eq!(bluez.read(&mut reader), Some(true));
        assert_eq!((bluez.lookups, bluez.reads), (3, 3));
    }

    fn ms(ms: u64) -> Duration {
        Duration::from_millis(ms)
    }

    #[test]
    fn fresh_read_budget_fast_calls_read_like_unbudgeted() {
        let fakes = || {
            [
                FakeBlueZ {
                    paths: Some(vec![FD0]),
                    states: vec![(FD0, Some("active"))],
                    ..Default::default()
                },
                FakeBlueZ {
                    paths: Some(vec![]),
                    ..Default::default()
                },
                FakeBlueZ {
                    paths: Some(vec![FD0, FD1]),
                    states: vec![(FD0, Some("idle")), (FD1, Some("idle"))],
                    ..Default::default()
                },
                // Gone after both lookups.
                FakeBlueZ {
                    paths: Some(vec![FD0]),
                    ..Default::default()
                },
            ]
        };
        let expected = [Some(true), Some(false), Some(false), None];
        for ((mut fresh, mut plain), want) in fakes().into_iter().zip(fakes()).zip(expected) {
            let (active, calls) = fresh.read_fresh(Duration::ZERO);
            assert_eq!(active, want);
            assert_eq!(plain.read(&mut TransportReader::default()), want);
            assert_eq!((fresh.lookups, fresh.reads), (plain.lookups, plain.reads));
            assert_eq!(calls.len(), fresh.lookups + fresh.reads);
            assert!(calls.iter().all(|&(_, t)| t == TRANSPORT_READ_TIMEOUT));
        }
    }

    #[test]
    fn fresh_read_budget_shrinks_then_stops_calling() {
        // lookup -> gone state -> lookup, then spent before the second state.
        let mut bluez = FakeBlueZ {
            paths: Some(vec![FD0]),
            ..Default::default()
        };
        let (active, calls) = bluez.read_fresh(ms(200));
        assert_eq!(active, None);
        // Each timeout is the budget left at its call: 500 ms less its time.
        assert_eq!(
            calls,
            [(ms(0), ms(500)), (ms(200), ms(300)), (ms(400), ms(100))]
        );
        assert_eq!((bluez.lookups, bluez.reads), (2, 1));

        // Spent before the second lookup: it fails like the bus.
        let mut bluez = FakeBlueZ {
            paths: Some(vec![FD0]),
            ..Default::default()
        };
        let (active, calls) = bluez.read_fresh(ms(300));
        assert_eq!(active, None);
        assert_eq!(calls, [(ms(0), ms(500)), (ms(300), ms(200))]);
        assert_eq!((bluez.lookups, bluez.reads), (1, 1));
    }

    #[test]
    fn fresh_read_budget_spent_never_reads_idle() {
        // Spent after the first of two idle paths: unknown, not idle.
        let mut bluez = FakeBlueZ {
            paths: Some(vec![FD0, FD1]),
            states: vec![(FD0, Some("idle")), (FD1, Some("idle"))],
            ..Default::default()
        };
        let (active, calls) = bluez.read_fresh(ms(300));
        assert_eq!(active, None);
        assert_eq!(calls, [(ms(0), ms(500)), (ms(300), ms(200))]);
        assert_eq!((bluez.lookups, bluez.reads), (1, 1));

        // Under 1 ms left counts as spent: no zero timeout reaches dbus.
        let mut bluez = FakeBlueZ {
            paths: Some(vec![FD0]),
            states: vec![(FD0, Some("idle"))],
            ..Default::default()
        };
        let (active, calls) = bluez.read_fresh(Duration::from_micros(499_500));
        assert_eq!(active, None);
        assert_eq!(calls, [(ms(0), ms(500))]);
        assert_eq!((bluez.lookups, bluez.reads), (1, 0));
    }
}
