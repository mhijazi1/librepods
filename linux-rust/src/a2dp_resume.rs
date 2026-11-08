//! Restores A2DP streaming after a take that found the AirPods' transport
//! idle (#200, DECISIONS 2026-11-08 option (b)).
//!
//! `Chain` is the pure step machine: after a settle, read the transport
//! fresh and, only if it is idle, nudge the AirPods sink (suspend, then
//! resume), then kick the players that were playing at the take (MPRIS Pause,
//! then Play), then, as a last resort, cycle the card profile once and play
//! them again. Any audible check stops it. `run` drives it on injected I/O
//! (`A2dpResumeIo`); the production I/O is `MediaController`'s. The transport
//! state the core needs comes from `spawn_transport_poller`, which only reads
//! BlueZ's `MediaTransport1.State` (never `Acquire`: the fd would go to
//! LibrePods instead of PipeWire).

use dbus::blocking::Connection;
use dbus::blocking::stdintf::org_freedesktop_dbus::{ObjectManager, Properties};
use futures::future::BoxFuture;
use log::{info, warn};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

/// From the chain's start to its transport read: PipeWire starts the
/// transport by itself about 0.3 s after a take (#267).
const RESUME_SETTLE: Duration = Duration::from_millis(500);
/// The longest the chain waits for its transport read; a slower read is
/// unknown.
const TRANSPORT_READ_TIMEOUT: Duration = Duration::from_millis(500);
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
/// transport read timing out and its three audibility reads.
const CHAIN_WORST_CASE_SLEEP: Duration = RESUME_SETTLE
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
pub(crate) const A2DP_RESUME_WINDOW: Duration = Duration::from_secs(9);
const _: () = assert!(
    CHAIN_WORST_CASE_SLEEP
        .saturating_add(PLAY_EDGE_MARGIN)
        .saturating_add(RESUME_MARGIN)
        .as_millis()
        <= A2DP_RESUME_WINDOW.as_millis()
);

/// How often the transport poller reads BlueZ.
const TRANSPORT_POLL: Duration = Duration::from_millis(500);

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
    /// Read the transport fresh; nudge only if it is idle.
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
}

impl Chain {
    fn new(now: Instant, until: Instant, playing: Vec<String>) -> Self {
        Chain {
            stage: Stage::Start,
            start: playing,
            due: now + RESUME_SETTLE,
            until,
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
    /// over. `transport` (the fresh read: active, idle or unknown) matters
    /// only when `reads_transport()`: active ends the chain, and only idle
    /// suspends the sink; unknown goes straight to the audible check, never
    /// stopping a transport that may carry audio. `audible` matters only
    /// when `senses()`, and ends the chain there; `playing` only when
    /// `reads_players()`. Once the sink is suspended or the players paused,
    /// the next step always undoes it. The kick and the cycle are skipped,
    /// ending the chain, when the Play after them can't be due
    /// `PLAY_EDGE_MARGIN` before `until`: the core would release on the stop
    /// they cause.
    fn step(
        &mut self,
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
                Some(false) => (vec![Action::SuspendSink(true)], Stage::Suspended, NUDGE_GAP),
                None => (Vec::new(), Stage::Nudged, NUDGE_GAP + AUDIBLE_WAIT),
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
    /// A fresh read of whether the AirPods' A2DP transport is active; None
    /// when unknown. The default (idle) is a test seam for fakes; the
    /// production I/O must override it.
    fn transport_active(&self) -> BoxFuture<'static, Option<bool>> {
        Box::pin(async { Some(false) })
    }
}

/// Run the chain on `io` once `routed` resolves (the default sink route
/// queued before it, #40), starting no kick or cycle that can't end before
/// `until`. `cancel` firing (or its sender dropping) stops it between steps,
/// including during a step's audio check; during the wait for `routed`, it
/// or `until` ends the run with no action.
pub(crate) async fn run(
    io: impl A2dpResumeIo,
    until: Instant,
    mut cancel: oneshot::Receiver<()>,
    routed: impl Future<Output = ()>,
) {
    // Tokio's clock, so tests can pause it; the same as std's otherwise.
    let clock = || tokio::time::Instant::now().into_std();
    tokio::select! {
        biased;
        _ = &mut cancel => {
            info!("A2DP resume: cancelled before the default sink route ended");
            return;
        }
        _ = routed => {}
        _ = tokio::time::sleep_until(until.into()) => {}
    }
    // The route and the deadline can both be ready at one poll.
    if clock() >= until {
        info!("A2DP resume: the default sink route outlasted the window");
        return;
    }
    let mut chain = Chain::new(clock(), until, io.playing().await);
    loop {
        let (due, senses, reads_players) = (chain.due(), chain.senses(), chain.reads_players());
        let reads_transport = chain.reads_transport();
        // The transport read and the audio check give way to a cancel too,
        // so a yield during them stops the chain before the step they decide.
        let sensed = async {
            tokio::time::sleep_until(due.into()).await;
            let transport = if reads_transport {
                tokio::time::timeout(TRANSPORT_READ_TIMEOUT, io.transport_active())
                    .await
                    .ok()
                    .flatten()
            } else {
                None
            };
            let audible = senses && io.audible().await;
            let playing = if reads_players {
                io.playing().await
            } else {
                Vec::new()
            };
            (transport, audible, playing)
        };
        let (transport, audible, playing) = tokio::select! {
            biased;
            _ = &mut cancel => {
                info!("A2DP resume: cancelled");
                act(&io, chain.cancel()).await;
                return;
            }
            sensed = sensed => sensed,
        };
        let now = clock();
        let Some(actions) = chain.step(now, transport, audible, &playing) else {
            let left = until.saturating_duration_since(now);
            info!(
                "A2DP resume: done (transport: {transport:?}, audible: {audible}, window left: {left:?})"
            );
            return;
        };
        act(&io, actions).await;
    }
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
                        || transport_paths(bus, &device),
                        |path| transport_state(bus, path),
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

/// One blocking read of whether the AirPods' A2DP transport is active. None
/// for no MAC (never idle: "/dev_/" matches no transport) or when BlueZ
/// can't be read.
pub(crate) fn read_transport_once(airpods_mac: &str) -> Option<bool> {
    if airpods_mac.is_empty() {
        return None;
    }
    let bus = Connection::new_system().ok()?;
    let device = device_fragment(airpods_mac);
    TransportReader::default().read(
        || transport_paths(&bus, &device),
        |path| transport_state(&bus, path),
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
/// BlueZ's object tree. None when BlueZ can't be read.
fn transport_paths(bus: &Connection, device: &str) -> Option<Vec<String>> {
    let objects = bus
        .with_proxy("org.bluez", "/", Duration::from_secs(2))
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
/// has it; Ok(None) when BlueZ can't be read.
fn transport_state(bus: &Connection, path: &str) -> Result<Option<bool>, Gone> {
    let state: Result<String, _> = bus
        .with_proxy("org.bluez", path, Duration::from_secs(2))
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
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};

    const P: &str = "org.mpris.MediaPlayer2.firefox";
    const Q: &str = "org.mpris.MediaPlayer2.spotify";

    fn players(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    /// Run a chain started with `start` playing, its transport read idle, to
    /// its end: `sense(ms since the first step)` gives audibility and the
    /// playing players. Returns each step's (ms, actions).
    fn drive(
        start: &[&str],
        sense: impl Fn(u128) -> (bool, Vec<String>),
    ) -> Vec<(u128, Vec<Action>)> {
        let t0 = Instant::now();
        let mut chain = Chain::new(t0, t0 + A2DP_RESUME_WINDOW, players(start));
        let first = chain.due();
        let mut steps = Vec::new();
        loop {
            let now = chain.due();
            let ms = (now - first).as_millis();
            let (audible, playing) = sense(ms);
            match chain.step(now, Some(false), audible, &playing) {
                Some(actions) => steps.push((ms, actions)),
                None => return steps,
            }
        }
    }

    fn actions(steps: &[(u128, Vec<Action>)]) -> Vec<Action> {
        steps.iter().flat_map(|(_, a)| a.clone()).collect()
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
        for _ in 0..20 {
            if let Some(actions) = chain.step(chain.due(), Some(false), false, &players(&[P])) {
                all.extend(actions);
            }
        }
        assert_eq!(all.iter().filter(|a| **a == ProfileCycle).count(), 1);
        assert_eq!(chain.step(chain.due(), Some(false), false, &[]), None);
    }

    #[test]
    fn cancel_after_suspend_still_resumes_sink() {
        let t0 = Instant::now();
        let mut chain = Chain::new(t0, t0 + A2DP_RESUME_WINDOW, players(&[P]));
        assert_eq!(
            chain.step(t0, Some(false), false, &[]),
            Some(vec![SuspendSink(true)])
        );
        assert_eq!(chain.cancel(), [SuspendSink(false)]);
        assert_eq!(
            chain.step(chain.due(), Some(false), false, &players(&[P])),
            None
        );
        assert_eq!(chain.cancel(), []);

        // Cancelled after the kick's Pause: nothing more, no Play.
        let mut chain = Chain::new(t0, t0 + A2DP_RESUME_WINDOW, players(&[P]));
        for _ in 0..3 {
            chain.step(chain.due(), Some(false), false, &players(&[P]));
        }
        assert_eq!(chain.cancel(), []);
        assert_eq!(
            chain.step(chain.due(), Some(false), false, &players(&[P])),
            None
        );
    }

    #[test]
    fn resume_window_covers_chain() {
        let steps = drive(&[P], |_| (false, players(&[P])));
        let last = Duration::from_millis(steps.last().unwrap().0 as u64);
        // `drive`'s transport and audibility reads take no time; the
        // production ones do.
        assert_eq!(
            RESUME_SETTLE + TRANSPORT_READ_TIMEOUT + last + AUDIBLE_SAMPLE * 3,
            CHAIN_WORST_CASE_SLEEP
        );
        assert!(A2DP_RESUME_WINDOW >= CHAIN_WORST_CASE_SLEEP + PLAY_EDGE_MARGIN + RESUME_MARGIN);
        // An unknown transport read leaves the later steps on the same times.
        let t0 = Instant::now();
        let mut idle = Chain::new(t0, t0 + A2DP_RESUME_WINDOW, players(&[P]));
        idle.step(idle.due(), Some(false), false, &[]);
        idle.step(idle.due(), Some(false), false, &[]);
        let mut unknown = Chain::new(t0, t0 + A2DP_RESUME_WINDOW, players(&[P]));
        assert_eq!(unknown.step(unknown.due(), None, false, &[]), Some(vec![]));
        assert_eq!(unknown.due(), idle.due());
    }

    #[test]
    fn kick_or_cycle_due_within_play_edge_margin_of_window_end_is_skipped() {
        let t0 = Instant::now();
        let until = t0 + A2DP_RESUME_WINDOW;
        let nudged = || {
            let mut chain = Chain::new(t0, until, players(&[P]));
            chain.step(t0, Some(false), false, &[]);
            chain.step(chain.due(), Some(false), false, &[]);
            chain
        };
        // The kick's Play would be due just PLAY_EDGE_MARGIN before the
        // window's end: no Pause. 1 ms earlier, the kick runs.
        let edge = until - KICK_GAP - PLAY_EDGE_MARGIN;
        let mut chain = nudged();
        assert_eq!(chain.step(edge, Some(false), false, &players(&[P])), None);
        assert_eq!(chain.cancel(), []);
        let before = edge - Duration::from_millis(1);
        assert_eq!(
            nudged().step(before, Some(false), false, &players(&[P])),
            Some(vec![Pause(P.into())])
        );

        // The same for the cycle's replay Play.
        let kicked = || {
            let mut chain = nudged();
            chain.step(chain.due(), Some(false), false, &players(&[P]));
            chain.step(chain.due(), Some(false), false, &[]);
            chain
        };
        let edge = until - CYCLE_SETTLE - AUDIBLE_SAMPLE - PLAY_EDGE_MARGIN;
        assert_eq!(
            kicked().step(edge, Some(false), false, &players(&[P])),
            None
        );
        let before = edge - Duration::from_millis(1);
        assert_eq!(
            kicked().step(before, Some(false), false, &players(&[P])),
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
        // 1 s reads: the kick is at 4 s, the cycle would be at 7 s and its
        // Play at 9 s.
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
        // Instant reads: the cycle at 4.5 s, its Play at 6.5 s.
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
        // The replay Play ends the run, at 6.5 s, in time for the poll.
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
        (acted, tokio::spawn(run(io, until, cancel, routed)))
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
        let mut chain = std::pin::pin!(run(io, until, cancelled, async {
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

    /// `SlowIo` with instant player reads whose transport read is `read`,
    /// counted in `reads`.
    struct TransportIo {
        io: SlowIo,
        read: fn() -> BoxFuture<'static, Option<bool>>,
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
            self.reads.fetch_add(1, Ordering::SeqCst);
            (self.read)()
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
        read: fn() -> BoxFuture<'static, Option<bool>>,
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
        (acted, reads, tokio::spawn(run(io, until, cancel, ready)))
    }

    fn reads(n: &AtomicUsize) -> usize {
        n.load(Ordering::SeqCst)
    }

    #[tokio::test(start_paused = true)]
    async fn settle_active_read_acts_nothing() {
        let (_cancel, cancelled) = oneshot::channel();
        let (acted, n, chain) = spawn_with_transport(|| Box::pin(async { Some(true) }), cancelled);
        chain.await.unwrap();
        assert_eq!(*acted.lock().unwrap(), []);
        assert_eq!(reads(&n), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn settle_idle_read_nudges_after_settle() {
        let (_cancel, cancelled) = oneshot::channel();
        let (acted, n, chain) = spawn_with_transport(|| Box::pin(async { Some(false) }), cancelled);
        tokio::time::sleep(RESUME_SETTLE - Duration::from_millis(1)).await;
        assert_eq!((acted.lock().unwrap().len(), reads(&n)), (0, 0));
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(*acted.lock().unwrap(), [SuspendSink(true)]);
        chain.await.unwrap();
        assert_eq!(
            acted.lock().unwrap()[..2],
            [SuspendSink(true), SuspendSink(false)]
        );
        assert_eq!(reads(&n), 1);
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
        let (acted, n, chain) = spawn_with_transport(|| Box::pin(async { None }), cancelled);
        chain.await.unwrap();
        assert_kicked_without_nudge(&acted.lock().unwrap());
        assert_eq!(reads(&n), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn settle_slow_read_counts_as_unknown() {
        let (_cancel, cancelled) = oneshot::channel();
        let (acted, n, chain) =
            spawn_with_transport(|| Box::pin(std::future::pending()), cancelled);
        chain.await.unwrap();
        assert_kicked_without_nudge(&acted.lock().unwrap());
        assert_eq!(reads(&n), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn settle_cancel_during_settle_acts_nothing() {
        let (cancel, cancelled) = oneshot::channel();
        let (acted, n, chain) = spawn_with_transport(|| Box::pin(async { Some(false) }), cancelled);
        tokio::time::sleep(RESUME_SETTLE / 2).await;
        cancel.send(()).unwrap();
        chain.await.unwrap();
        assert_eq!(*acted.lock().unwrap(), []);
        assert_eq!(reads(&n), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn settle_cancel_during_read_acts_nothing() {
        let (cancel, cancelled) = oneshot::channel();
        let (acted, n, chain) =
            spawn_with_transport(|| Box::pin(std::future::pending()), cancelled);
        tokio::time::sleep(RESUME_SETTLE + TRANSPORT_READ_TIMEOUT / 2).await;
        assert_eq!(reads(&n), 1);
        cancel.send(()).unwrap();
        chain.await.unwrap();
        assert_eq!(*acted.lock().unwrap(), []);
    }

    #[test]
    fn read_transport_once_empty_mac_is_unknown() {
        assert_eq!(read_transport_once(""), None);
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
            let (paths, states) = (&self.paths, &self.states);
            let lookup_seq = &mut self.lookup_seq;
            let (lookups, reads) = (&mut self.lookups, &mut self.reads);
            reader.read(
                || {
                    *lookups += 1;
                    lookup_seq
                        .pop_front()
                        .unwrap_or_else(|| paths.clone())
                        .map(|p| p.iter().map(|p| p.to_string()).collect())
                },
                |path| {
                    *reads += 1;
                    match states.iter().find(|(p, _)| *p == path) {
                        Some((_, Some(state))) => Ok(Some(*state != "idle")),
                        Some((_, None)) => Ok(None),
                        None => Err(Gone),
                    }
                },
            )
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
}
