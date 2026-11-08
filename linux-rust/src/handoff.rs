//! Pure core of the #841 handoff controller.
//!
//! `Handoff` is a synchronous state machine: it takes input events (playback
//! samples, AACP routing and ownership reports, connection changes, user
//! commands, ticks) with an injected `now` and returns the commands the I/O
//! shell in `handoff_probe::run` must execute. It owns every decision the
//! probe makes, so the sequences are tested without Bluetooth, D-Bus or a
//! runtime. Every take goes through `decide_takeover`, the single takeover
//! policy in media_controller.rs.

use crate::bluetooth::aacp::{AudioSource, ConnectedDevice, EarDetectionStatus};
use crate::media_controller::{
    CaseRecovery, HandoffGuards, RoutingSnapshot, TakeoverAction, TakeoverDecision, TakeoverInputs,
    TakeoverSkip, TakeoverTrigger, case_closed_after, decide_takeover, routing_snapshot,
};
use std::time::{Duration, Instant};

/// How long an ownership request may wait for its acknowledgement.
pub(crate) const ACK_TIMEOUT: Duration = Duration::from_secs(5);
/// First reconnect delay after a disconnect or a successful reconnect.
const FIRST_RETRY: Duration = Duration::from_secs(1);
/// Reconnect backoff cap.
const MAX_RETRY: Duration = Duration::from_secs(8);

/// Routing, wear and ownership facts of a freshly made control session,
/// copied by the shell so a take right after a reconnect sees them before the
/// session's queued events are processed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SessionFacts {
    pub(crate) hosts: Vec<ConnectedDevice>,
    pub(crate) ears: Vec<EarDetectionStatus>,
    pub(crate) audio: Option<AudioSource>,
    /// Last reported OwnsConnection value.
    pub(crate) owner: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Input {
    /// A playback poll: whether any local player was playing at `sampled_at`.
    Playback { sampled_at: Instant, playing: bool },
    /// Wear report (AACP ear detection).
    EarState(Vec<EarDetectionStatus>),
    /// Audio source report.
    AudioSource(AudioSource),
    /// Connected hosts report: previous and new list.
    ConnectedDevices {
        old: Vec<ConnectedDevice>,
        new: Vec<ConnectedDevice>,
    },
    /// OwnsConnection report; an acknowledgement when it matches the pending
    /// request.
    Ownership(Vec<u8>),
    /// Another host asked Linux to give up ownership.
    RemoteOwnershipRequest,
    /// The control session dropped.
    Disconnected,
    /// A reconnect attempt made a new control session (not yet adopted).
    Reconnected(SessionFacts),
    /// A reconnect attempt failed.
    ReconnectFailed(String),
    /// Switch to automatic handoff (REPL `auto`).
    Auto,
    /// Switch to manual mode (REPL `manual`).
    Manual,
    /// The user asked for the AirPods (REPL `take`).
    Take,
    /// The user gave them up (REPL `release`).
    Release,
    /// REPL `quit`, end of input, SIGINT or SIGTERM.
    Quit,
    /// Periodic tick: reconnect backoff and acknowledgement timeouts.
    Tick,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Command {
    /// Send OwnsConnection with this value.
    SendOwnership(bool),
    /// Announce Linux to the connected remote hosts.
    Announce,
    /// Pause local players.
    PauseMedia,
    /// Connect the A2DP sink profile.
    ConnectA2dp,
    /// Tear down the dead control session and drop its queued events.
    DropSession,
    /// Start a reconnect attempt; `audio` also connects A2DP and reports
    /// Linux as not owning.
    Reconnect { audio: bool },
    /// Use the session the last reconnect made.
    AdoptSession,
    /// Close the session the last reconnect made and disconnect the device.
    CancelReconnect,
    /// Drop playback samples taken before the new baseline.
    DrainPlayback,
    /// Stop the probe.
    Exit,
    /// Stop the probe with this error.
    Fail(&'static str),
}

/// Why an ownership request was sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cause {
    /// Automatic: playback started or stopped.
    Playback,
    /// A remote host asked for ownership.
    Remote,
    /// REPL `take` or `release`.
    User,
    /// Releasing on exit.
    Quit,
}

#[derive(Clone, Copy, Debug)]
struct Pending {
    desired: bool,
    since: Instant,
    cause: Cause,
}

pub(crate) struct Handoff {
    local: String,
    automatic: bool,
    connected: bool,
    quitting: bool,
    /// Ownership request awaiting its acknowledgement.
    pending: Option<Pending>,
    /// Linux asked for ownership and has not given it back.
    requested_take: bool,
    /// Last playback sample since the baseline; None until one arrives.
    was_playing: Option<bool>,
    /// Samples taken before this instant are ignored.
    playback_baseline_at: Instant,
    recovery: CaseRecovery,
    case_closed: bool,
    /// Playback stopped while a take was pending: release once it settles.
    stop_during_take: bool,
    /// Playback restarted while an automatic release was pending: take once
    /// it settles.
    play_during_release: bool,
    reconnecting: bool,
    retry_at: Instant,
    retry_delay: Duration,
    hosts: Vec<ConnectedDevice>,
    ears: Vec<EarDetectionStatus>,
    audio: Option<AudioSource>,
    owner: Option<Vec<u8>>,
    lines: Vec<String>,
}

impl Handoff {
    /// `connected`: the control session is already up. Starting without one
    /// waits for new playback before reconnecting (buds in the case).
    pub(crate) fn new(local: String, now: Instant, automatic: bool, connected: bool) -> Self {
        Handoff {
            local,
            automatic,
            connected,
            quitting: false,
            pending: None,
            requested_take: false,
            was_playing: None,
            playback_baseline_at: now,
            recovery: if connected {
                CaseRecovery::Immediate
            } else {
                CaseRecovery::WaitingForPlayback
            },
            case_closed: false,
            stop_during_take: false,
            play_during_release: false,
            reconnecting: false,
            retry_at: now,
            retry_delay: FIRST_RETRY,
            hosts: Vec::new(),
            ears: Vec::new(),
            audio: None,
            owner: None,
            lines: Vec::new(),
        }
    }

    pub(crate) fn connected(&self) -> bool {
        self.connected
    }

    pub(crate) fn quitting(&self) -> bool {
        self.quitting
    }

    /// Protocol lines (READY/AUTO/ACK/TIMEOUT/RECONNECT...) produced since
    /// the last call, for the shell to print.
    pub(crate) fn take_lines(&mut self) -> Vec<String> {
        std::mem::take(&mut self.lines)
    }

    /// Feed one input at `now`; returns the commands to execute, in order.
    pub(crate) fn handle(&mut self, now: Instant, input: Input) -> Vec<Command> {
        let mut out = Vec::new();
        self.apply(now, input, &mut out);
        if !out
            .iter()
            .any(|c| matches!(c, Command::Exit | Command::Fail(_)))
        {
            self.finish_quit(now, &mut out);
        }
        out
    }

    fn say(&mut self, line: impl Into<String>) {
        self.lines.push(line.into());
    }

    fn apply(&mut self, now: Instant, input: Input, out: &mut Vec<Command>) {
        match input {
            Input::Playback {
                sampled_at,
                playing,
            } => self.playback(now, sampled_at, playing, out),
            Input::EarState(ears) => {
                self.case_closed = case_closed_after(&ears, self.case_closed);
                self.ears = ears;
            }
            Input::AudioSource(source) => self.audio = Some(source),
            Input::ConnectedDevices { old, new } => {
                if self.automatic
                    && new
                        .iter()
                        .any(|d| d.mac != self.local && !old.iter().any(|o| o.mac == d.mac))
                {
                    out.push(Command::Announce);
                }
                self.hosts = new;
            }
            Input::Ownership(value) => self.ownership(now, value, out),
            Input::RemoteOwnershipRequest => {
                if self.automatic && self.requested_take {
                    out.push(Command::PauseMedia);
                }
                if self.owner.as_deref() == Some(&[0]) && !self.requested_take {
                    self.say("REMOTE requested ownership; Linux already yielded");
                    return;
                }
                self.say("REMOTE requested ownership; yielding");
                self.request(now, false, Cause::Remote, out);
            }
            Input::Disconnected => self.disconnected(now, out),
            Input::Reconnected(facts) => self.reconnected(now, facts, out),
            Input::ReconnectFailed(error) => {
                self.reconnecting = false;
                self.say(format!(
                    "RECONNECT failed: {error}; retrying in {}s",
                    self.retry_delay.as_secs()
                ));
                self.retry_at = now + self.retry_delay;
                self.retry_delay = (self.retry_delay * 2).min(MAX_RETRY);
            }
            Input::Auto => {
                out.push(Command::ConnectA2dp);
                if !self.requested_take {
                    out.push(Command::SendOwnership(false));
                }
                self.automatic = true;
                out.push(Command::Announce);
                self.reset_baseline(now);
                self.say("AUTO enabled; waiting for a new playback start after the baseline");
            }
            Input::Manual => {
                self.automatic = false;
                self.say("AUTO disabled");
            }
            Input::Take => {
                if self.pending.is_some() {
                    self.say("WAIT ownership request pending");
                } else if self.decide_take(TakeoverTrigger::Manual).action
                    == TakeoverAction::TakeOver
                {
                    self.request(now, true, Cause::User, out);
                    self.say("REQUEST owner=linux");
                } else {
                    self.say(
                        "BLOCKED take needs two hosts, an in-ear bud, and confirmed idle audio",
                    );
                }
            }
            Input::Release => {
                if self.pending.is_some() {
                    self.say("WAIT ownership request pending");
                } else if self.owner.as_deref() == Some(&[0]) && !self.requested_take {
                    self.say("Ownership already released");
                } else {
                    self.request(now, false, Cause::User, out);
                    self.say("REQUEST owner=not-linux");
                }
            }
            Input::Quit => self.quitting = true,
            Input::Tick => self.tick(now, out),
        }
    }

    fn playback(
        &mut self,
        now: Instant,
        sampled_at: Instant,
        playing: bool,
        out: &mut Vec<Command>,
    ) {
        if sampled_at < self.playback_baseline_at {
            return;
        }
        let previous = self.was_playing.replace(playing);
        let fresh_play = previous == Some(false) && playing;
        let stopped = previous == Some(true) && !playing;
        if !self.automatic {
            return;
        }
        if !self.connected {
            self.recovery.playback(fresh_play, stopped);
            if fresh_play && self.recovery == CaseRecovery::PlaybackRequested {
                self.retry_at = now;
                self.say("AUTO new playback requested reconnection after case closure");
            }
            return;
        }
        if self.quitting {
            return;
        }
        match self.pending {
            None => match self.decide_poll(previous, playing).action {
                TakeoverAction::TakeOver => {
                    self.request(now, true, Cause::Playback, out);
                    self.say("AUTO playback started; requested Linux ownership");
                }
                TakeoverAction::Skip(TakeoverSkip::InitialState | TakeoverSkip::NoTransition) => {
                    if stopped && self.requested_take {
                        self.request(now, false, Cause::Playback, out);
                        self.say("AUTO playback stopped; releasing ownership");
                    }
                }
                TakeoverAction::Skip(_) => self
                    .say("AUTO takeover blocked by remote activity, wear, or missing host state"),
            },
            // Remember edges that arrive while a request is in flight, so the
            // acknowledgement (or its timeout) cannot strand Linux with the
            // wrong ownership.
            Some(p) if p.desired => {
                if stopped {
                    self.stop_during_take = true;
                } else if fresh_play {
                    self.stop_during_take = false;
                }
            }
            Some(p) if p.cause == Cause::Playback => {
                if fresh_play {
                    self.play_during_release = true;
                } else if stopped {
                    self.play_during_release = false;
                }
            }
            Some(_) => {}
        }
    }

    fn ownership(&mut self, now: Instant, value: Vec<u8>, out: &mut Vec<Command>) {
        let acked = self
            .pending
            .filter(|p| value.as_slice() == [u8::from(p.desired)]);
        // Our own release was acknowledged while the user started playing
        // again: the reconcile below takes the AirPods back, so pausing the
        // players here would undo the play it is acting on.
        let replay_play = acked.is_some_and(|p| !p.desired)
            && self.play_during_release
            && self.was_playing == Some(true);
        self.owner = Some(value.clone());
        if value == [0] {
            if self.requested_take && self.automatic && !replay_play {
                out.push(Command::PauseMedia);
            }
            self.requested_take = false;
        }
        let Some(pending) = acked else {
            return;
        };
        self.say(format!(
            "ACK owner={} elapsed_ms={}",
            if pending.desired {
                "linux"
            } else {
                "not-linux"
            },
            now.saturating_duration_since(pending.since).as_millis()
        ));
        self.pending = None;
        if !pending.desired {
            self.requested_take = false;
        }
        if self.quitting {
            out.push(Command::Exit);
            return;
        }
        self.reconcile(now, out);
    }

    /// The pending request settled (acknowledged or timed out): act on a
    /// playback edge that arrived while it was in flight.
    fn reconcile(&mut self, now: Instant, out: &mut Vec<Command>) {
        let stop = std::mem::take(&mut self.stop_during_take);
        let play = std::mem::take(&mut self.play_during_release);
        if !self.automatic || !self.connected || self.quitting || self.pending.is_some() {
            return;
        }
        if stop && self.requested_take && self.was_playing == Some(false) {
            self.request(now, false, Cause::Playback, out);
            self.say("AUTO playback stopped during the take; releasing ownership");
        } else if play && self.was_playing == Some(true) {
            // The stopped-to-playing edge was seen while the release was in
            // flight; decide on it now.
            if self.decide_poll(Some(false), true).action == TakeoverAction::TakeOver {
                self.request(now, true, Cause::Playback, out);
                self.say("AUTO playback restarted during the release; requested Linux ownership");
            } else {
                self.say("AUTO takeover blocked by remote activity, wear, or missing host state");
            }
        }
    }

    fn disconnected(&mut self, now: Instant, out: &mut Vec<Command>) {
        if !self.connected {
            return;
        }
        self.say("DISCONNECTED clearing ownership and playback baseline");
        if self.requested_take && self.automatic {
            out.push(Command::PauseMedia);
        }
        out.push(Command::DropSession);
        self.connected = false;
        self.pending = None;
        self.requested_take = false;
        self.was_playing = None;
        self.stop_during_take = false;
        self.play_during_release = false;
        self.hosts.clear();
        self.ears.clear();
        self.audio = None;
        self.owner = None;
        self.recovery = if self.automatic && self.case_closed {
            self.say("CASE waiting for new Linux playback before reconnecting");
            CaseRecovery::WaitingForPlayback
        } else {
            CaseRecovery::Immediate
        };
        self.case_closed = false;
        self.retry_at = now + FIRST_RETRY;
    }

    fn reconnected(&mut self, now: Instant, facts: SessionFacts, out: &mut Vec<Command>) {
        self.reconnecting = false;
        if self.recovery == CaseRecovery::WaitingForPlayback {
            out.push(Command::CancelReconnect);
            self.say("RECONNECT cancelled because playback stopped");
            return;
        }
        out.push(Command::AdoptSession);
        self.connected = true;
        self.hosts = facts.hosts;
        self.ears = facts.ears;
        self.audio = facts.audio;
        self.owner = facts.owner;
        if self.automatic {
            out.push(Command::Announce);
        }
        match self
            .decide_take(TakeoverTrigger::ReconnectedForPlayback)
            .action
        {
            TakeoverAction::TakeOver => {
                self.request(now, true, Cause::Playback, out);
                self.say("AUTO reconnected for new playback; requested Linux ownership");
            }
            // An ordinary reconnect: no playback asked for it.
            TakeoverAction::Skip(TakeoverSkip::AwaitingNewPlayback) => {}
            TakeoverAction::Skip(_) => self.say(
                "AUTO reconnected; takeover blocked by remote activity, wear, or missing host state",
            ),
        }
        self.recovery = CaseRecovery::Immediate;
        self.reset_baseline(now);
        out.push(Command::DrainPlayback);
        self.retry_delay = FIRST_RETRY;
        self.say("RECONNECTED with fresh routing and wear state");
    }

    fn tick(&mut self, now: Instant, out: &mut Vec<Command>) {
        if !self.connected
            && !self.reconnecting
            && self.recovery != CaseRecovery::WaitingForPlayback
            && now >= self.retry_at
            && !self.quitting
        {
            self.reconnecting = true;
            out.push(Command::Reconnect {
                audio: self.automatic,
            });
        }
        if let Some(pending) = self.pending
            && now.saturating_duration_since(pending.since) > ACK_TIMEOUT
        {
            self.say("TIMEOUT ownership acknowledgement not received; audible handoff unconfirmed");
            self.pending = None;
            if self.quitting {
                out.push(Command::Fail("release was not acknowledged"));
                return;
            }
            self.reconcile(now, out);
        }
    }

    /// While quitting, release ownership Linux asked for, then exit once
    /// nothing is left to release.
    fn finish_quit(&mut self, now: Instant, out: &mut Vec<Command>) {
        if !self.quitting || self.pending.is_some_and(|p| !p.desired) {
            return;
        }
        if !self.requested_take {
            out.push(Command::Exit);
            return;
        }
        self.request(now, false, Cause::Quit, out);
        self.say("EXIT releasing ownership");
    }

    fn request(&mut self, now: Instant, desired: bool, cause: Cause, out: &mut Vec<Command>) {
        if desired {
            self.requested_take = true;
        }
        self.pending = Some(Pending {
            desired,
            since: now,
            cause,
        });
        self.stop_during_take = false;
        self.play_during_release = false;
        out.push(Command::SendOwnership(desired));
    }

    fn reset_baseline(&mut self, now: Instant) {
        self.was_playing = None;
        self.playback_baseline_at = now;
    }

    fn routing(&self) -> RoutingSnapshot {
        routing_snapshot(
            self.connected,
            &self.hosts,
            &self.ears,
            self.audio.as_ref(),
            &self.local,
        )
    }

    fn decide_poll(&self, previous: Option<bool>, playing: bool) -> TakeoverDecision {
        let routing = self.routing();
        decide_takeover(TakeoverInputs {
            baseline_taken: previous.is_some(),
            was_playing: previous == Some(true),
            is_playing: playing,
            // The probe keeps no self-pause flag (#841 semantics).
            i_paused_the_media: false,
            bud_in_ear: routing.bud_in_ear,
            trigger: TakeoverTrigger::PlaybackPoll,
            handoff: Some(HandoffGuards {
                routing,
                recovery: self.recovery,
            }),
        })
    }

    fn decide_take(&self, trigger: TakeoverTrigger) -> TakeoverDecision {
        decide_takeover(TakeoverInputs::handoff(
            trigger,
            self.routing(),
            self.recovery,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::Command::*;
    use super::*;
    use crate::bluetooth::aacp::AudioSourceType;
    use EarDetectionStatus::{InCase, InEar, OutOfEar};

    const LOCAL: &str = "01:02:03:04:05:06";
    const REMOTE: &str = "07:08:09:0A:0B:0C";

    fn host(mac: &str) -> ConnectedDevice {
        ConnectedDevice {
            mac: mac.to_string(),
            info1: 0,
            info2: 0,
            r#type: None,
        }
    }

    fn two_hosts() -> Vec<ConnectedDevice> {
        vec![host(LOCAL), host(REMOTE)]
    }

    fn source(mac: &str, r#type: AudioSourceType) -> AudioSource {
        AudioSource {
            mac: mac.to_string(),
            r#type,
        }
    }

    fn idle() -> AudioSource {
        source("00:00:00:00:00:00", AudioSourceType::None)
    }

    fn ready_facts() -> SessionFacts {
        SessionFacts {
            hosts: two_hosts(),
            ears: vec![InEar, InEar],
            audio: Some(idle()),
            owner: Some(vec![0]),
        }
    }

    /// A core driven with millisecond offsets from a fixed start.
    struct Probe {
        core: Handoff,
        t0: Instant,
    }

    impl Probe {
        fn new(automatic: bool, connected: bool) -> Self {
            let t0 = Instant::now();
            Probe {
                core: Handoff::new(LOCAL.to_string(), t0, automatic, connected),
                t0,
            }
        }

        /// Automatic, connected, both hosts, worn, idle audio, and a first
        /// "stopped" sample as the baseline.
        fn ready() -> Self {
            let mut probe = Self::new(true, true);
            probe.routing(ready_facts());
            assert_eq!(probe.play(10, false), []);
            probe
        }

        fn at(&self, ms: u64) -> Instant {
            self.t0 + Duration::from_millis(ms)
        }

        fn feed(&mut self, ms: u64, input: Input) -> Vec<Command> {
            self.core.handle(self.at(ms), input)
        }

        fn play(&mut self, ms: u64, playing: bool) -> Vec<Command> {
            let sampled_at = self.at(ms);
            self.feed(
                ms,
                Input::Playback {
                    sampled_at,
                    playing,
                },
            )
        }

        /// Feed routing reports as AACP events; the commands they cause
        /// (announcing a new host) are not under test here.
        fn routing(&mut self, facts: SessionFacts) {
            self.feed(
                0,
                Input::ConnectedDevices {
                    old: Vec::new(),
                    new: facts.hosts,
                },
            );
            self.feed(0, Input::EarState(facts.ears));
            if let Some(audio) = facts.audio {
                self.feed(0, Input::AudioSource(audio));
            }
            if let Some(owner) = facts.owner {
                self.feed(0, Input::Ownership(owner));
            }
        }
    }

    #[test]
    fn takes_on_new_playback_when_worn() {
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(1000, true), []);
        // Stopping gives the AirPods back.
        assert_eq!(p.play(1500, false), [SendOwnership(false)]);
        assert_eq!(p.feed(1600, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(p.play(2000, false), []);
    }

    #[test]
    fn no_take_when_not_worn() {
        let mut p = Probe::ready();
        p.feed(20, Input::EarState(vec![OutOfEar, InCase]));
        assert_eq!(p.play(500, true), []);
        assert_eq!(p.play(1000, false), []);
        assert_eq!(p.feed(1100, Input::Take), []);
        assert!(p.core.take_lines().iter().any(|l| l.starts_with("BLOCKED")));
        // Putting a bud in makes the next new playback take.
        p.feed(1200, Input::EarState(vec![OutOfEar, InEar]));
        assert_eq!(p.play(1500, true), [SendOwnership(true)]);
    }

    #[test]
    fn no_take_when_remote_in_call() {
        let mut p = Probe::ready();
        p.feed(
            20,
            Input::AudioSource(source(REMOTE, AudioSourceType::Call)),
        );
        assert_eq!(p.play(500, true), []);
        assert_eq!(p.play(1000, false), []);
        // Remote media blocks too; local media does not.
        p.feed(
            1100,
            Input::AudioSource(source(REMOTE, AudioSourceType::Media)),
        );
        assert_eq!(p.play(1500, true), []);
        assert_eq!(p.play(2000, false), []);
        assert_eq!(p.feed(2100, Input::Take), []);
        p.feed(
            2200,
            Input::AudioSource(source(LOCAL, AudioSourceType::Media)),
        );
        assert_eq!(p.play(2500, true), [SendOwnership(true)]);
    }

    #[test]
    fn no_take_on_stale_state() {
        // No session at all.
        let mut p = Probe::new(true, false);
        p.routing(ready_facts());
        assert_eq!(p.play(10, false), []);
        assert_eq!(p.play(500, true), []);

        // Hosts other than this machine plus one remote.
        let mut p = Probe::ready();
        p.feed(
            20,
            Input::ConnectedDevices {
                old: two_hosts(),
                new: vec![host(LOCAL)],
            },
        );
        assert_eq!(p.play(500, true), []);
        assert_eq!(p.play(1000, false), []);
        let three = vec![host(LOCAL), host(REMOTE), host("0D:0E:0F:10:11:12")];
        assert_eq!(
            p.feed(
                1100,
                Input::ConnectedDevices {
                    old: vec![host(LOCAL)],
                    new: three.clone(),
                },
            ),
            [Announce]
        );
        assert_eq!(p.play(1500, true), []);
        assert_eq!(p.play(2000, false), []);
        assert_eq!(
            p.feed(
                2100,
                Input::ConnectedDevices {
                    old: three,
                    new: two_hosts(),
                },
            ),
            []
        );

        // A disconnect clears wear and audio; a reconnect that has not
        // reported them yet must not take.
        assert_eq!(p.feed(2200, Input::Disconnected), [DropSession]);
        assert_eq!(p.feed(2300, Input::Take), []);
        assert_eq!(p.play(2500, true), []);
        assert_eq!(p.feed(3000, Input::Tick), []);
        assert_eq!(p.feed(3200, Input::Tick), [Reconnect { audio: true }]);
        let stale = SessionFacts {
            hosts: two_hosts(),
            ..SessionFacts::default()
        };
        assert_eq!(
            p.feed(3300, Input::Reconnected(stale)),
            [AdoptSession, Announce, DrainPlayback]
        );
        assert_eq!(p.play(3500, false), []);
        assert_eq!(p.play(4000, true), []);
        assert_eq!(p.feed(4100, Input::Take), []);
    }

    #[test]
    fn yields_on_remote_request() {
        // Linux did not ask for the AirPods: yield without pausing.
        let mut p = Probe::ready();
        p.feed(20, Input::Ownership(vec![1]));
        assert_eq!(
            p.feed(100, Input::RemoteOwnershipRequest),
            [SendOwnership(false)]
        );
        assert_eq!(p.feed(200, Input::Ownership(vec![0])), []);
        // Already yielded: nothing to send.
        assert_eq!(p.feed(300, Input::RemoteOwnershipRequest), []);

        // Linux took them for playback: pause the players and yield.
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(
            p.feed(700, Input::RemoteOwnershipRequest),
            [PauseMedia, SendOwnership(false)]
        );
        // Upstream #841 also pauses when the AirPods report the loss.
        assert_eq!(p.feed(800, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(p.play(1000, false), []);
        assert_eq!(p.feed(9000, Input::Tick), []);
    }

    #[test]
    fn case_disconnect_waits_for_new_playback() {
        let mut p = Probe::ready();
        p.feed(20, Input::EarState(vec![InCase, InCase]));
        assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
        // No reconnect while waiting, however long.
        assert_eq!(p.feed(5000, Input::Tick), []);
        assert_eq!(p.play(5100, true), []);
        assert_eq!(p.feed(5200, Input::Tick), []);
        // A new stopped-to-playing playback asks for the reconnection now.
        assert_eq!(p.play(5500, false), []);
        assert_eq!(p.play(6000, true), []);
        assert_eq!(p.feed(6100, Input::Tick), [Reconnect { audio: true }]);
        assert_eq!(p.feed(6200, Input::Tick), []);
        // Stopping before it completes cancels it.
        assert_eq!(p.play(6500, false), []);
        assert_eq!(
            p.feed(6600, Input::Reconnected(ready_facts())),
            [CancelReconnect]
        );
        assert_eq!(p.feed(9000, Input::Tick), []);
        // Playing again reconnects and takes for that playback.
        assert_eq!(p.play(9500, true), []);
        assert_eq!(p.feed(9600, Input::Tick), [Reconnect { audio: true }]);
        assert_eq!(
            p.feed(9700, Input::Reconnected(ready_facts())),
            [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
        );
        assert_eq!(p.feed(9800, Input::Ownership(vec![1])), []);

        // Without the case closed a disconnect reconnects after the backoff.
        let mut p = Probe::ready();
        assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
        assert_eq!(p.feed(1000, Input::Tick), []);
        assert_eq!(p.feed(1100, Input::Tick), [Reconnect { audio: true }]);
        assert_eq!(p.feed(1200, Input::ReconnectFailed("x".into())), []);
        assert_eq!(p.feed(2100, Input::Tick), []);
        assert_eq!(p.feed(2200, Input::Tick), [Reconnect { audio: true }]);
        assert_eq!(
            p.feed(2300, Input::Reconnected(ready_facts())),
            [AdoptSession, Announce, DrainPlayback]
        );
    }

    #[test]
    fn baseline_ignores_preexisting_playback() {
        // Auto mode starts while something is already playing.
        let mut p = Probe::new(false, true);
        p.routing(ready_facts());
        assert_eq!(
            p.feed(1000, Input::Auto),
            [ConnectA2dp, SendOwnership(false), Announce]
        );
        // A stopped sample from before the baseline is not an observation.
        assert_eq!(p.play(900, false), []);
        assert_eq!(p.play(1100, true), []);
        assert_eq!(p.play(1600, true), []);
        assert_eq!(p.play(2100, false), []);
        assert_eq!(p.play(2600, true), [SendOwnership(true)]);
        assert_eq!(p.feed(2700, Input::Ownership(vec![1])), []);

        // The same after a reconnect.
        assert_eq!(p.feed(3000, Input::Disconnected), [PauseMedia, DropSession]);
        assert_eq!(p.feed(4000, Input::Tick), [Reconnect { audio: true }]);
        assert_eq!(
            p.feed(5000, Input::Reconnected(ready_facts())),
            [AdoptSession, Announce, DrainPlayback]
        );
        assert_eq!(p.play(4900, false), []);
        assert_eq!(p.play(5100, true), []);
        assert_eq!(p.play(5600, true), []);
        assert_eq!(p.play(6100, false), []);
        assert_eq!(p.play(6600, true), [SendOwnership(true)]);
    }

    #[test]
    fn stop_during_pending_take_releases_after_ack() {
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        // Playback stops while the take waits for its acknowledgement.
        assert_eq!(p.play(1000, false), []);
        assert_eq!(
            p.feed(1100, Input::Ownership(vec![1])),
            [SendOwnership(false)]
        );
        for ms in [1500, 2000, 2500] {
            assert_eq!(p.play(ms, false), []);
        }
        assert_eq!(p.feed(2600, Input::Ownership(vec![0])), [PauseMedia]);
        for ms in [3000, 3500, 4000] {
            assert_eq!(p.play(ms, false), []);
        }
        assert_eq!(p.feed(9000, Input::Tick), []);
    }

    #[test]
    fn stop_during_pending_take_releases_after_timeout() {
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.play(1000, false), []);
        assert_eq!(p.feed(5500, Input::Tick), []);
        assert_eq!(p.feed(5600, Input::Tick), [SendOwnership(false)]);
        assert_eq!(p.play(6000, false), []);
    }

    #[test]
    fn restart_during_pending_take_keeps_ownership() {
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.play(1000, false), []);
        assert_eq!(p.play(1500, true), []);
        assert_eq!(p.feed(1600, Input::Ownership(vec![1])), []);
        for ms in [2000, 2500, 3000] {
            assert_eq!(p.play(ms, true), []);
        }
        assert_eq!(p.feed(9000, Input::Tick), []);
    }

    #[test]
    fn restart_during_pending_release_takes_after_ack() {
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(1000, false), [SendOwnership(false)]);
        // Playback restarts before the release is acknowledged: no pause,
        // and the AirPods are taken back.
        assert_eq!(p.play(1500, true), []);
        assert_eq!(
            p.feed(1600, Input::Ownership(vec![0])),
            [SendOwnership(true)]
        );
        assert_eq!(p.feed(1700, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(2000, true), []);

        // Stopped again before the acknowledgement: plain release.
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(1000, false), [SendOwnership(false)]);
        assert_eq!(p.play(1500, true), []);
        assert_eq!(p.play(2000, false), []);
        assert_eq!(p.feed(2100, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(p.play(2500, false), []);
    }

    #[test]
    fn manual_take_and_release() {
        let mut p = Probe::new(false, true);
        p.routing(ready_facts());
        assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
        assert_eq!(p.feed(200, Input::Release), []);
        assert_eq!(p.feed(300, Input::Ownership(vec![1])), []);
        // Manual mode never releases on its own.
        assert_eq!(p.play(400, true), []);
        assert_eq!(p.play(900, false), []);
        assert_eq!(p.feed(1000, Input::Release), [SendOwnership(false)]);
        assert_eq!(p.feed(1100, Input::Ownership(vec![0])), []);
        assert_eq!(p.feed(1200, Input::Release), []);
    }

    #[test]
    fn quit_releases_requested_ownership() {
        let mut p = Probe::ready();
        assert_eq!(p.feed(100, Input::Quit), [Exit]);

        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.feed(700, Input::Quit), [SendOwnership(false)]);
        assert_eq!(p.feed(800, Input::Tick), []);
        assert_eq!(p.feed(900, Input::Ownership(vec![0])), [PauseMedia, Exit]);

        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Quit), [SendOwnership(false)]);
        assert_eq!(
            p.feed(5700, Input::Tick),
            [Fail("release was not acknowledged")]
        );
    }
}
