//! Restores A2DP streaming after a take that found the AirPods' transport
//! idle (#200, DECISIONS 2026-11-08 option (b)).
//!
//! `Chain` is the pure step machine: nudge the AirPods sink (suspend, then
//! resume), then kick the players that were playing at the take (MPRIS Pause,
//! then Play), then, as a last resort, cycle the card profile once and play
//! them again. Any audible check stops it. `run` drives it on injected I/O
//! (`A2dpResumeIo`); the production I/O is `MediaController`'s. The transport
//! state the core needs comes from `spawn_transport_poller`, which only reads
//! BlueZ's `MediaTransport1.State` (never `Acquire`: the fd would go to
//! LibrePods instead of PipeWire).

use dbus::blocking::Connection;
use dbus::blocking::stdintf::org_freedesktop_dbus::ObjectManager;
use futures::future::BoxFuture;
use log::{info, warn};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

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
/// The chain's sleeps when nothing ever plays: every step runs.
const CHAIN_WORST_CASE_SLEEP: Duration = NUDGE_GAP
    .saturating_add(AUDIBLE_WAIT)
    .saturating_add(KICK_GAP)
    .saturating_add(KICK_WAIT)
    .saturating_add(CYCLE_SETTLE);
/// Run time of the chain's I/O on top of its sleeps: three audibility reads
/// (`MediaController`'s 500 ms peak sample) and the D-Bus and sound-server
/// calls. Like `A2DP_RECOVERY_BOUND`, the window counts sleeps; this margin
/// is an engineering default, not a measured bound.
const RESUME_MARGIN: Duration = Duration::from_secs(3);
/// How long after the take's acknowledgement the core holds playback-stop
/// releases while the chain may pause and play the players.
pub(crate) const A2DP_RESUME_WINDOW: Duration = Duration::from_secs(8);
const _: () = assert!(
    CHAIN_WORST_CASE_SLEEP
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
    /// Set the AirPods card profile to off.
    ProfileOff,
    /// Set the AirPods card's preferred A2DP profile.
    ProfileA2dp,
}

enum Stage {
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
pub(crate) struct Chain {
    stage: Stage,
    /// The players playing at the chain's start (the take's ACK).
    start: Vec<String>,
    due: Instant,
}

impl Chain {
    pub(crate) fn new(now: Instant, playing: Vec<String>) -> Self {
        Chain {
            stage: Stage::Start,
            start: playing,
            due: now,
        }
    }

    /// When the next step is due.
    pub(crate) fn due(&self) -> Instant {
        self.due
    }

    /// Whether the next step reads audibility and the playing players.
    pub(crate) fn senses(&self) -> bool {
        matches!(self.stage, Stage::Nudged | Stage::Kicked | Stage::Cycled(_))
    }

    /// Run the step due at `now`: its actions, or None once the chain is
    /// over. `audible` and `playing` matter only when `senses()`; audible
    /// ends the chain there. Once the sink is suspended or the players
    /// paused, the next step always undoes it.
    pub(crate) fn step(
        &mut self,
        now: Instant,
        audible: bool,
        playing: &[String],
    ) -> Option<Vec<Action>> {
        if audible && self.senses() {
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
            Stage::Start => (vec![Action::SuspendSink(true)], Stage::Suspended, NUDGE_GAP),
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
                vec![Action::ProfileOff, Action::ProfileA2dp],
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
    pub(crate) fn cancel(&mut self) -> Vec<Action> {
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
}

/// Run the chain from now on `io`. `cancel` firing (or its sender dropping)
/// stops it between steps.
pub(crate) async fn run(io: impl A2dpResumeIo, mut cancel: oneshot::Receiver<()>) {
    let mut chain = Chain::new(Instant::now(), io.playing().await);
    loop {
        tokio::select! {
            biased;
            _ = &mut cancel => {
                info!("A2DP resume: cancelled");
                act(&io, chain.cancel()).await;
                return;
            }
            _ = tokio::time::sleep_until(chain.due().into()) => {}
        }
        let (audible, playing) = if chain.senses() {
            (io.audible().await, io.playing().await)
        } else {
            (false, Vec::new())
        };
        let Some(actions) = chain.step(Instant::now(), audible, &playing) else {
            info!("A2DP resume: done (audible: {audible})");
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
    let device = format!("/dev_{}/", airpods_mac.to_uppercase().replace(':', "_"));
    let (tx, rx) = mpsc::channel(4);
    let spawned = std::thread::Builder::new()
        .name("librepods-a2dp".to_string())
        .spawn(move || {
            let mut bus = None;
            poll_transport(&tx, TRANSPORT_POLL, || {
                if bus.is_none() {
                    bus = Connection::new_system().ok();
                }
                let active = bus.as_ref().and_then(|bus| transport_active(bus, &device));
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

/// Whether a `MediaTransport1` under `device` (`/dev_XX_..._XX/`) is not
/// idle, read from BlueZ's object tree. None when BlueZ can't be read.
fn transport_active(bus: &Connection, device: &str) -> Option<bool> {
    let objects = bus
        .with_proxy("org.bluez", "/", Duration::from_secs(2))
        .get_managed_objects()
        .ok()?;
    Some(
        objects
            .iter()
            .filter(|(path, _)| path.contains(device))
            .filter_map(|(_, interfaces)| interfaces.get("org.bluez.MediaTransport1"))
            .any(|props| {
                props
                    .get("State")
                    .and_then(|state| state.0.as_str())
                    .is_some_and(|state| state != "idle")
            }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use Action::*;

    const P: &str = "org.mpris.MediaPlayer2.firefox";
    const Q: &str = "org.mpris.MediaPlayer2.spotify";

    fn players(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    /// Run a chain started with `start` playing, from t0, to its end:
    /// `sense(ms since t0)` gives audibility and the playing players. Returns
    /// each step's (ms, actions).
    fn drive(
        start: &[&str],
        sense: impl Fn(u128) -> (bool, Vec<String>),
    ) -> Vec<(u128, Vec<Action>)> {
        let t0 = Instant::now();
        let mut chain = Chain::new(t0, players(start));
        let mut steps = Vec::new();
        loop {
            let now = chain.due();
            let ms = (now - t0).as_millis();
            let (audible, playing) = sense(ms);
            match chain.step(now, audible, &playing) {
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
                ProfileOff,
                ProfileA2dp,
                Play(Q.into()),
            ]
        );
        let cycle = (NUDGE_GAP + AUDIBLE_WAIT + KICK_GAP + KICK_WAIT).as_millis();
        assert_eq!(steps[4], (cycle, vec![ProfileOff, ProfileA2dp]));
        assert_eq!(steps[5].0, cycle + CYCLE_SETTLE.as_millis());

        // Audible once the profile is back: no Play after the cycle.
        let steps = drive(&[P], |ms| (ms > cycle, players(&[P])));
        assert_eq!(actions(&steps).last(), Some(&ProfileA2dp));
    }

    #[test]
    fn never_cycles_profile_twice() {
        let t0 = Instant::now();
        let mut chain = Chain::new(t0, players(&[P]));
        let mut all = Vec::new();
        // Far more steps than the chain has, all silent.
        for _ in 0..20 {
            if let Some(actions) = chain.step(chain.due(), false, &players(&[P])) {
                all.extend(actions);
            }
        }
        assert_eq!(all.iter().filter(|a| **a == ProfileOff).count(), 1);
        assert_eq!(all.iter().filter(|a| **a == ProfileA2dp).count(), 1);
        assert_eq!(chain.step(chain.due(), false, &[]), None);
    }

    #[test]
    fn cancel_after_suspend_still_resumes_sink() {
        let t0 = Instant::now();
        let mut chain = Chain::new(t0, players(&[P]));
        assert_eq!(chain.step(t0, false, &[]), Some(vec![SuspendSink(true)]));
        assert_eq!(chain.cancel(), [SuspendSink(false)]);
        assert_eq!(chain.step(chain.due(), false, &players(&[P])), None);
        assert_eq!(chain.cancel(), []);

        // Cancelled after the kick's Pause: nothing more, no Play.
        let mut chain = Chain::new(t0, players(&[P]));
        for _ in 0..3 {
            chain.step(chain.due(), false, &players(&[P]));
        }
        assert_eq!(chain.cancel(), []);
        assert_eq!(chain.step(chain.due(), false, &players(&[P])), None);
    }

    #[test]
    fn resume_window_covers_chain() {
        let steps = drive(&[P], |_| (false, players(&[P])));
        let last = Duration::from_millis(steps.last().unwrap().0 as u64);
        assert_eq!(last, CHAIN_WORST_CASE_SLEEP);
        assert!(A2DP_RESUME_WINDOW >= last + RESUME_MARGIN);
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
}
