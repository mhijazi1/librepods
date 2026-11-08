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
/// How long after a release was sent an OwnsConnection=0 may still be taken
/// for its late acknowledgement once its ACK timed out (#62), inclusive.
/// Measured from the send, so the tick cadence does not move it. Twice
/// `ACK_TIMEOUT`, an engineering default: a capture of real late-ACK delays
/// may retune it. A later 0 is a genuine loss and yields.
pub(crate) const LATE_ACK_WINDOW: Duration = Duration::from_secs(10);
/// First reconnect delay after a disconnect or a successful reconnect.
const FIRST_RETRY: Duration = Duration::from_secs(1);
/// Reconnect backoff cap.
const MAX_RETRY: Duration = Duration::from_secs(8);
/// Reply to a REPL command that needs a live control session.
const NO_SESSION: &str =
    "WAIT no live control session; reconnecting (status, manual and quit remain available)";

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
    /// request. A 0 that acknowledges no release of Linux's, while Linux
    /// holds or is taking the AirPods, is a loss: Linux yields once (pauses
    /// in automatic mode, forgets the take and its pending request). The 0
    /// that acknowledges Linux's own playback-stop or remote-request release
    /// pauses nothing (#39). During the reconnect take a 0 only updates the
    /// cached owner (#43). A 0 within `LATE_ACK_WINDOW` of sending a release
    /// whose acknowledgement timed out is that release's late
    /// acknowledgement, not a loss (#55); a later 0 is a loss (#62).
    Ownership(Vec<u8>),
    /// Another host asked Linux to give up ownership. A repeat while Linux's
    /// release for an earlier request is pending sends nothing; a repeat
    /// after that release timed out re-sends it without pausing again (#61).
    RemoteOwnershipRequest,
    /// The shell found the control session dead (the AACP receive loop
    /// cleared it without an event) while some of its events may still be
    /// queued. From here on nothing is sent on it and no take is allowed,
    /// but the session is not dropped: the queued events (a case-closed wear
    /// report, an ownership ACK) are still fed, then `Disconnected`.
    SessionLost,
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
    /// The connected session died; it is torn down at the next
    /// `Disconnected`.
    session_lost: bool,
    quitting: bool,
    /// Ownership request awaiting its acknowledgement.
    pending: Option<Pending>,
    /// Linux asked for ownership and has not given it back.
    requested_take: bool,
    /// Last playback sample since the baseline; None until one arrives.
    was_playing: Option<bool>,
    /// Samples taken before this instant are ignored.
    playback_baseline_at: Instant,
    /// A take was sent for playback seen before the last baseline reset (a
    /// reconnect for requested playback). Until the first post-baseline
    /// sample arrives, release decisions treat playback as running, so a
    /// "stopped" first sample is a stop edge. Takeover decisions still see
    /// no baseline.
    take_assumes_playing: bool,
    /// The take sent on reconnect is awaiting its acknowledgement. The
    /// session's queued OwnsConnection=0 reports from this window answer the
    /// "not owning" that connecting the session (and its audio) sent, which
    /// `SessionFacts::owner` already reflected; they are not a loss of the
    /// take. Cleared at the take's ACK, its timeout, a disconnect and any new
    /// request.
    reconnect_take_pending: bool,
    /// Cause and send time of Linux's last release whose acknowledgement
    /// timed out. An OwnsConnection=0 within `LATE_ACK_WINDOW` of that send
    /// is the release's late acknowledgement, not a loss (#55); a later one
    /// is a genuine loss (#62). Expiry is checked only when a report
    /// arrives, never by `tick`. Cleared when any 0 consumes it (late or
    /// not), by any report that Linux still owns, a disconnect and any new
    /// request.
    timed_out_release: Option<(Cause, Instant)>,
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
            session_lost: false,
            quitting: false,
            pending: None,
            requested_take: false,
            was_playing: None,
            playback_baseline_at: now,
            take_assumes_playing: false,
            reconnect_take_pending: false,
            timed_out_release: None,
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

    /// A control session the core may send on: connected and not found
    /// dead.
    pub(crate) fn live(&self) -> bool {
        self.connected && !self.session_lost
    }

    pub(crate) fn quitting(&self) -> bool {
        self.quitting
    }

    /// Protocol lines (READY/AUTO/ACK/TIMEOUT/YIELD/RECONNECT...) produced
    /// since the last call, for the shell to print.
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
                    && self.live()
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
                if !self.live() {
                    // Nothing can be sent; the disconnect that follows
                    // pauses and clears ownership.
                    self.say("REMOTE requested ownership; control session lost");
                    return;
                }
                // A repeat of a request Linux already yielded to: that yield
                // paused once, so this one must not pause again (#39, #61).
                if self
                    .pending
                    .is_some_and(|p| !p.desired && p.cause == Cause::Remote)
                {
                    // Keep the pending release and its `since`, so its ACK
                    // timeout is not pushed back.
                    self.say("REMOTE requested ownership; release already pending");
                    return;
                }
                if self.pending.is_none()
                    && matches!(self.timed_out_release, Some((Cause::Remote, _)))
                {
                    // The release was never acknowledged: send it again. Its
                    // [0] is acknowledged like the first one, without a pause.
                    self.say("REMOTE requested ownership; re-sending the timed-out release");
                    self.request(now, false, Cause::Remote, out);
                    return;
                }
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
            Input::SessionLost => {
                if self.live() {
                    self.session_lost = true;
                    self.say("SESSION lost; finishing its queued events before disconnecting");
                }
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
                if !self.live() {
                    self.say(NO_SESSION);
                    return;
                }
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
                } else {
                    match self.decide_take(TakeoverTrigger::Manual).action {
                        TakeoverAction::TakeOver => {
                            self.request(now, true, Cause::User, out);
                            self.say("REQUEST owner=linux");
                        }
                        TakeoverAction::Skip(_) if !self.live() => self.say(NO_SESSION),
                        TakeoverAction::Skip(_) => self.say(
                            "BLOCKED take needs two hosts, an in-ear bud, and confirmed idle audio",
                        ),
                    }
                }
            }
            Input::Release => {
                if !self.live() {
                    self.say(NO_SESSION);
                } else if self.pending.is_some() {
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
        let assumed_playing = std::mem::take(&mut self.take_assumes_playing);
        let fresh_play = previous == Some(false) && playing;
        // Release edges also count a take sent for playback before the
        // baseline: the stop after a reconnect take must not be lost.
        let stopped =
            (previous == Some(true) || (previous.is_none() && assumed_playing)) && !playing;
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
        if self.quitting || self.session_lost {
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

    /// An OwnsConnection report. Rules for a 0 (#39), in order:
    /// - during the reconnect take it is the session's stale init report and
    ///   only updates `owner` (#43);
    /// - the acknowledgement of Linux's own release pauses only after a user
    ///   or quit release (upstream #841), or as the deferred pause of a
    ///   blocked retake; a playback-stop or remote-request release left
    ///   nothing playing to pause. A 0 within `LATE_ACK_WINDOW` of sending
    ///   a release whose acknowledgement timed out is that release's late
    ///   acknowledgement and follows the same rule, without a second
    ///   reconcile (#55); a later 0 falls through to the rule below (#62);
    /// - otherwise, while Linux holds or is taking the AirPods, another host
    ///   took them: `yield_take` yields once.
    fn ownership(&mut self, now: Instant, value: Vec<u8>, out: &mut Vec<Command>) {
        let acked = self
            .pending
            .filter(|p| value.as_slice() == [u8::from(p.desired)]);
        // Our own release was acknowledged while the user started playing
        // again: the reconcile below may take the AirPods back, and pausing
        // the players first would undo the play it is acting on. The pause
        // upstream #841 makes here is deferred until the reconcile has
        // decided, and made unless it actually re-took.
        let replay_play = acked.is_some_and(|p| !p.desired)
            && self.play_during_release
            && self.was_playing == Some(true);
        self.owner = Some(value.clone());
        if value == [0] && self.reconnect_take_pending {
            // A stale report queued while the reconnected session
            // initialized, not a loss of the pending take (which wants 1, so
            // this is not its acknowledgement): neither pause the playback
            // the take is for nor forget the take, or a stop during it could
            // not be released.
            self.say("OWNER stale not-linux report during the reconnect take; ignored");
            return;
        }
        // A release of Linux's timed out with nothing pending since: a 0
        // within `LATE_ACK_WINDOW` of its send is its late acknowledgement
        // (#55); a later 0 is a genuine loss as if there were no marker
        // (#62); any other value says Linux still owns, so that release
        // failed and is forgotten.
        let late_release = self
            .timed_out_release
            .take()
            .filter(|&(_, sent)| {
                value == [0]
                    && acked.is_none()
                    && now.saturating_duration_since(sent) <= LATE_ACK_WINDOW
            })
            .map(|(cause, _)| cause);
        let mut deferred_pause = false;
        if value == [0] {
            match acked.map(|p| p.cause).or(late_release) {
                // The acknowledgement of Linux's own release (#39). A release
                // for stopped playback or for a remote request already left
                // nothing to pause: the players were stopped, or
                // RemoteOwnershipRequest paused them when it yielded. Only
                // the deferred pause of a blocked retake remains. A user or
                // quit release may leave Linux playing, so upstream #841's
                // pause stays. A late acknowledgement (#55) is handled the
                // same way; it cannot be a replay, as nothing was pending.
                Some(cause) => {
                    if self.requested_take && self.automatic {
                        match cause {
                            Cause::User | Cause::Quit => out.push(Command::PauseMedia),
                            Cause::Playback | Cause::Remote => deferred_pause = replay_play,
                        }
                    }
                }
                // Another host took the AirPods from a take Linux made or
                // was making: yield once and forget the take.
                None if self.requested_take => {
                    self.yield_take(out);
                    return;
                }
                None => {}
            }
            self.requested_take = false;
        }
        let Some(pending) = acked else {
            if late_release.is_some() {
                // The timeout already reconciled; nothing more to decide.
                self.say("ACK owner=not-linux late (after TIMEOUT); release confirmed");
            }
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
        self.reconnect_take_pending = false;
        if !pending.desired {
            self.requested_take = false;
        }
        if self.quitting {
            if deferred_pause {
                out.push(Command::PauseMedia);
            }
            out.push(Command::Exit);
            return;
        }
        let first = out.len();
        self.reconcile(now, out);
        if deferred_pause && !out[first..].contains(&Command::SendOwnership(true)) {
            // The retake was blocked: the AirPods went to the other host, so
            // stop playing here as upstream does.
            out.push(Command::PauseMedia);
        }
    }

    /// The AirPods reported OwnsConnection=0 while Linux held, or was
    /// taking, them and the report acknowledges no release of Linux's: the
    /// take is lost. Give it up once: pause in automatic mode, forget the
    /// take and its pending request, so neither a stop nor the request's
    /// timeout acts on it later.
    fn yield_take(&mut self, out: &mut Vec<Command>) {
        // `ownership` only yields on a report that is not a release's
        // acknowledgement, so a pending request here is the take.
        let dropped = self.pending.take_if(|p| p.desired).is_some();
        self.requested_take = false;
        self.stop_during_take = false;
        self.play_during_release = false;
        if self.automatic {
            out.push(Command::PauseMedia);
        }
        self.say(format!(
            "YIELD owner=not-linux; another host has the AirPods; Linux gave up its {}take{}",
            if dropped { "pending " } else { "" },
            if self.automatic {
                " and paused local playback"
            } else {
                ""
            },
        ));
    }

    /// The pending request settled (acknowledged or timed out): act on a
    /// playback edge that arrived while it was in flight.
    fn reconcile(&mut self, now: Instant, out: &mut Vec<Command>) {
        let stop = std::mem::take(&mut self.stop_during_take);
        let play = std::mem::take(&mut self.play_during_release);
        // Nothing is sent on a session found dead, even if the routing
        // cached before it died would allow a take.
        if !self.automatic || !self.live() || self.quitting || self.pending.is_some() {
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
        self.session_lost = false;
        self.pending = None;
        self.reconnect_take_pending = false;
        self.timed_out_release = None;
        self.requested_take = false;
        self.was_playing = None;
        self.take_assumes_playing = false;
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
        self.session_lost = false;
        self.hosts = facts.hosts;
        self.ears = facts.ears;
        self.audio = facts.audio;
        self.owner = facts.owner;
        if self.automatic {
            out.push(Command::Announce);
        }
        let took = match self
            .decide_take(TakeoverTrigger::ReconnectedForPlayback)
            .action
        {
            TakeoverAction::TakeOver => {
                self.request(now, true, Cause::Playback, out);
                self.say("AUTO reconnected for new playback; requested Linux ownership");
                true
            }
            // An ordinary reconnect: no playback asked for it.
            TakeoverAction::Skip(TakeoverSkip::AwaitingNewPlayback) => false,
            TakeoverAction::Skip(_) => {
                self.say(
                    "AUTO reconnected; takeover blocked by remote activity, wear, or missing host state",
                );
                false
            }
        };
        self.recovery = CaseRecovery::Immediate;
        self.reset_baseline(now);
        // The take was for playback seen before this baseline: a first
        // sample that finds it stopped must release.
        self.take_assumes_playing = took;
        // The session's queued OwnsConnection=0 reports predate this take.
        self.reconnect_take_pending = took;
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
            self.reconnect_take_pending = false;
            // A release may still be acknowledged late: that 0 is not a
            // loss of the take it gave back (#55).
            self.timed_out_release = (!pending.desired).then_some((pending.cause, pending.since));
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
        if !self.requested_take || self.session_lost {
            // Nothing to release, or no session left to release it on.
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
        self.reconnect_take_pending = false;
        self.timed_out_release = None;
        out.push(Command::SendOwnership(desired));
    }

    fn reset_baseline(&mut self, now: Instant) {
        self.was_playing = None;
        self.take_assumes_playing = false;
        self.playback_baseline_at = now;
    }

    fn routing(&self) -> RoutingSnapshot {
        routing_snapshot(
            self.live(),
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
        // #39: the release's ACK pauses nothing; playback already stopped.
        assert_eq!(p.feed(1600, Input::Ownership(vec![0])), []);
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
        // #39: this [0] acknowledges the remote yield, which already paused,
        // so it pauses nothing.
        assert_eq!(p.feed(800, Input::Ownership(vec![0])), []);
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
        // #39: the release's ACK pauses nothing; playback already stopped.
        assert_eq!(p.feed(2600, Input::Ownership(vec![0])), []);
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
        // #39: the release's ACK pauses nothing; playback already stopped.
        assert_eq!(p.feed(2100, Input::Ownership(vec![0])), []);
        assert_eq!(p.play(2500, false), []);
    }

    #[test]
    fn stop_after_reconnect_take_releases_after_ack() {
        // Automatic mode, case closed, then a disconnect.
        let mut p = Probe::ready();
        p.feed(20, Input::EarState(vec![InCase, InCase]));
        assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
        // New playback asks for the reconnection.
        assert_eq!(p.play(500, false), []);
        assert_eq!(p.play(1000, true), []);
        assert_eq!(p.feed(1100, Input::Tick), [Reconnect { audio: true }]);
        assert_eq!(
            p.feed(1200, Input::Reconnected(ready_facts())),
            [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
        );
        // Playback is no longer running by the first post-baseline sample.
        assert_eq!(p.play(1500, false), []);
        let mut settled = p.feed(1600, Input::Ownership(vec![1]));
        settled.extend(p.play(2000, false));
        assert_eq!(settled, [SendOwnership(false)]);
        for ms in [2500, 3000, 3500] {
            assert_eq!(p.play(ms, false), []);
        }
        assert_eq!(p.feed(3600, Input::Tick), []);
        // #39: the release's ACK pauses nothing; playback already stopped.
        assert_eq!(p.feed(3700, Input::Ownership(vec![0])), []);
        for ms in [4000, 4500, 5000] {
            assert_eq!(p.play(ms, false), []);
        }
        assert_eq!(p.feed(9000, Input::Tick), []);

        // The first post-baseline sample arriving only after the ACK
        // releases too.
        let mut p = Probe::ready();
        p.feed(20, Input::EarState(vec![InCase, InCase]));
        assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
        assert_eq!(p.play(500, false), []);
        assert_eq!(p.play(1000, true), []);
        assert_eq!(p.feed(1100, Input::Tick), [Reconnect { audio: true }]);
        assert_eq!(
            p.feed(1200, Input::Reconnected(ready_facts())),
            [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
        );
        assert_eq!(p.feed(1300, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(1500, false), [SendOwnership(false)]);
        for ms in [2000, 2500, 3000] {
            assert_eq!(p.play(ms, false), []);
        }
        assert_eq!(p.feed(3100, Input::Tick), []);

        // Control: playback still running after the reconnect keeps the
        // AirPods.
        let mut p = Probe::ready();
        p.feed(20, Input::EarState(vec![InCase, InCase]));
        assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
        assert_eq!(p.play(500, false), []);
        assert_eq!(p.play(1000, true), []);
        assert_eq!(p.feed(1100, Input::Tick), [Reconnect { audio: true }]);
        assert_eq!(
            p.feed(1200, Input::Reconnected(ready_facts())),
            [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
        );
        assert_eq!(p.play(1500, true), []);
        assert_eq!(p.feed(1600, Input::Ownership(vec![1])), []);
        for ms in [2000, 2500, 3000] {
            assert_eq!(p.play(ms, true), []);
        }
        assert_eq!(p.feed(9000, Input::Tick), []);
        // A later stop releases as usual.
        assert_eq!(p.play(9500, false), [SendOwnership(false)]);
    }

    #[test]
    fn restart_during_pending_release_blocked_pauses() {
        // A remote call starts while the release is in flight.
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(1000, false), [SendOwnership(false)]);
        assert_eq!(p.play(1500, true), []);
        assert_eq!(
            p.feed(
                1550,
                Input::AudioSource(source(REMOTE, AudioSourceType::Call))
            ),
            []
        );
        assert_eq!(p.feed(1600, Input::Ownership(vec![0])), [PauseMedia]);
        for ms in [2000, 2500, 3000] {
            assert_eq!(p.play(ms, true), []);
        }
        assert_eq!(p.feed(9000, Input::Tick), []);

        // The buds come out while the release is in flight.
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(1000, false), [SendOwnership(false)]);
        assert_eq!(p.play(1500, true), []);
        assert_eq!(p.feed(1550, Input::EarState(vec![OutOfEar, InCase])), []);
        assert_eq!(p.feed(1600, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(p.play(2000, true), []);
    }

    #[test]
    fn session_loss_before_release_ack_does_not_retake() {
        // As in restart_during_pending_release_takes_after_ack: an automatic
        // release is pending and playback restarted during it.
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(1000, false), [SendOwnership(false)]);
        assert_eq!(p.play(1500, true), []);
        p.core.take_lines();
        // The buds go in the case and the link drops. run() finds the sender
        // gone with the wear report and the release ACK still queued: it
        // marks the session lost, feeds the queued events, then disconnects.
        assert_eq!(p.feed(1600, Input::SessionLost), []);
        assert_eq!(p.feed(1610, Input::EarState(vec![InCase, InCase])), []);
        // No retake on the dead session; c8's deferred pause applies.
        assert_eq!(p.feed(1620, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(p.feed(1630, Input::Disconnected), [DropSession]);
        assert!(
            p.core
                .take_lines()
                .iter()
                .any(|l| l.starts_with("CASE waiting for new Linux playback"))
        );
        // Case recovery: no reconnect, however long, until a new
        // stopped-to-playing playback.
        for ms in (2000..=5600).step_by(200) {
            assert_eq!(p.feed(ms, Input::Tick), []);
        }
        assert_eq!(p.play(5700, true), []);
        assert_eq!(p.feed(5800, Input::Tick), []);
        assert_eq!(p.play(6000, false), []);
        assert_eq!(p.feed(6100, Input::Tick), []);
        assert_eq!(p.play(6500, true), []);
        assert_eq!(p.feed(6600, Input::Tick), [Reconnect { audio: true }]);
    }

    #[test]
    fn session_loss_blocks_manual_take() {
        // run() checks the live sender before a REPL command: on a dead
        // sender it feeds SessionLost, the queued events (none: the select
        // is biased), then Disconnected, and only then the command.
        let mut p = Probe::new(false, true);
        p.routing(ready_facts());
        p.core.take_lines();
        assert_eq!(p.feed(100, Input::SessionLost), []);
        // Even before the disconnect is fed, the cached routing does not
        // allow a take, a release or auto mode on the dead session.
        assert_eq!(p.feed(110, Input::Take), []);
        assert_eq!(p.feed(120, Input::Release), []);
        assert_eq!(p.feed(130, Input::Auto), []);
        let lines = p.core.take_lines();
        assert_eq!(lines.iter().filter(|l| l.starts_with("WAIT")).count(), 3);
        assert_eq!(p.feed(200, Input::Disconnected), [DropSession]);
        assert_eq!(p.feed(210, Input::Take), []);
        assert_eq!(p.feed(220, Input::Release), []);
        assert_eq!(p.feed(230, Input::Auto), []);
        let lines = p.core.take_lines();
        assert_eq!(lines.iter().filter(|l| l.starts_with("WAIT")).count(), 3);

        // A manual take Linux holds: losing the session sends no release.
        let mut p = Probe::new(false, true);
        p.routing(ready_facts());
        assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
        assert_eq!(p.feed(200, Input::Ownership(vec![1])), []);
        assert_eq!(p.feed(300, Input::SessionLost), []);
        assert_eq!(p.feed(310, Input::Release), []);
        assert_eq!(p.feed(320, Input::RemoteOwnershipRequest), []);
        assert_eq!(p.feed(330, Input::Disconnected), [DropSession]);
        assert_eq!(p.feed(340, Input::Take), []);
    }

    #[test]
    fn session_loss_sends_nothing() {
        // A stop during a pending take whose ACK is dequeued after the
        // session died: no release on it.
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.play(1000, false), []);
        assert_eq!(p.feed(1100, Input::SessionLost), []);
        assert_eq!(p.feed(1110, Input::Ownership(vec![1])), []);
        assert_eq!(
            p.feed(
                1120,
                Input::ConnectedDevices {
                    old: vec![host(LOCAL)],
                    new: two_hosts(),
                },
            ),
            []
        );
        assert_eq!(p.feed(1130, Input::RemoteOwnershipRequest), []);
        assert_eq!(p.play(1140, true), []);
        assert_eq!(p.feed(1150, Input::Disconnected), [PauseMedia, DropSession]);
        assert_eq!(p.feed(2200, Input::Tick), [Reconnect { audio: true }]);

        // Quitting with the AirPods taken: nothing to release them on.
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.feed(700, Input::SessionLost), []);
        assert_eq!(p.feed(710, Input::Quit), [Exit]);
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

    /// Automatic, worn, case closed, disconnected, then new playback that
    /// reconnects and takes at 1200 ms: the take's [1] is pending.
    fn reconnect_take() -> Probe {
        let mut p = Probe::ready();
        p.feed(20, Input::EarState(vec![InCase, InCase]));
        assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
        assert_eq!(p.play(500, false), []);
        assert_eq!(p.play(1000, true), []);
        assert_eq!(p.feed(1100, Input::Tick), [Reconnect { audio: true }]);
        assert_eq!(
            p.feed(1200, Input::Reconnected(ready_facts())),
            [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
        );
        p
    }

    #[test]
    fn reconnect_take_ignores_stale_init_ownership() {
        // The adopted session's queue still holds the [0] answering the
        // "not owning" sent by connect_control and the audio reconnect.
        let mut p = reconnect_take();
        assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
        // The stop during the take is not lost: it releases once the take
        // settles.
        assert_eq!(p.play(1500, false), []);
        let mut settled = p.feed(1600, Input::Ownership(vec![1]));
        settled.extend(p.play(2000, false));
        assert_eq!(settled, [SendOwnership(false)]);
        // The release's acknowledgement never takes back.
        assert!(
            !p.feed(2100, Input::Ownership(vec![0]))
                .contains(&SendOwnership(true))
        );
        for ms in [2500, 3000, 3500] {
            assert_eq!(p.play(ms, false), []);
        }
        assert_eq!(p.feed(9000, Input::Tick), []);

        // Playback keeps running: Linux keeps the AirPods, nothing pauses,
        // and a later stop releases.
        let mut p = reconnect_take();
        assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
        assert_eq!(p.play(1500, true), []);
        assert_eq!(p.feed(1600, Input::Ownership(vec![1])), []);
        for ms in [2000, 2500, 3000] {
            assert_eq!(p.play(ms, true), []);
        }
        assert_eq!(p.feed(9000, Input::Tick), []);
        assert_eq!(p.play(9500, false), [SendOwnership(false)]);

        // Queued wear, host and audio reports in the same window still
        // update the core: the buds went back in the case, so the next
        // disconnect waits for new playback instead of reconnecting. The
        // pause on that disconnect shows the take was not forgotten.
        let mut p = reconnect_take();
        assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
        assert_eq!(
            p.feed(
                1310,
                Input::ConnectedDevices {
                    old: two_hosts(),
                    new: two_hosts(),
                },
            ),
            []
        );
        assert_eq!(p.feed(1320, Input::AudioSource(idle())), []);
        assert_eq!(p.feed(1330, Input::EarState(vec![InCase, InCase])), []);
        assert_eq!(p.feed(1600, Input::Ownership(vec![1])), []);
        p.core.take_lines();
        assert_eq!(p.feed(1700, Input::Disconnected), [PauseMedia, DropSession]);
        assert!(
            p.core
                .take_lines()
                .iter()
                .any(|l| l.starts_with("CASE waiting for new Linux playback"))
        );
        for ms in (2000..=9000).step_by(500) {
            assert_eq!(p.feed(ms, Input::Tick), []);
        }
    }

    #[test]
    fn genuine_loss_after_reconnect_take_pauses() {
        // The take settled with its ACK: a later [0] is a real loss and
        // pauses as upstream #841 does.
        let mut p = reconnect_take();
        assert_eq!(p.feed(1300, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(1500, true), []);
        assert_eq!(p.feed(2000, Input::Ownership(vec![0])), [PauseMedia]);
        // The take was forgotten: the stop releases nothing.
        assert_eq!(p.play(2500, false), []);

        // The take is never acknowledged: its timeout ends the window too.
        let mut p = reconnect_take();
        assert_eq!(p.feed(6000, Input::Tick), []);
        assert_eq!(p.feed(6300, Input::Tick), []);
        assert_eq!(p.feed(6400, Input::Ownership(vec![0])), [PauseMedia]);
    }

    fn count_lines(lines: &[String], prefix: &str) -> usize {
        lines.iter().filter(|l| l.starts_with(prefix)).count()
    }

    /// Automatic take for playback, acknowledged, playback still running at
    /// 1000 ms.
    fn acked_auto_take() -> Probe {
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(1000, true), []);
        p
    }

    #[test]
    fn yield_on_owns_connection_zero() {
        // (a) Linux owns the AirPods and another host takes them: yield once.
        let mut p = acked_auto_take();
        p.core.take_lines();
        assert_eq!(p.feed(1100, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
        // The take is forgotten: the stop the pause causes releases nothing,
        // and no timeout fires for it.
        assert_eq!(p.play(1500, false), []);
        assert_eq!(p.feed(7000, Input::Tick), []);
        assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 0);
        // Not stuck: a new stopped-to-playing edge takes again.
        assert_eq!(p.play(7500, true), [SendOwnership(true)]);

        // (b) The pending take resolves to not-linux before any [1].
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        p.core.take_lines();
        assert_eq!(p.feed(600, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
        assert_eq!(p.play(1000, false), []);
        // Past the take's ACK_TIMEOUT: no TIMEOUT, no reconcile.
        assert_eq!(p.feed(6000, Input::Tick), []);
        assert_eq!(p.feed(9000, Input::Tick), []);
        assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 0);
        // A late [1] is no acknowledgement of a request.
        assert_eq!(p.feed(9100, Input::Ownership(vec![1])), []);
        assert_eq!(count_lines(&p.core.take_lines(), "ACK"), 0);
        // Nothing is left pending: a take is not answered with WAIT.
        assert_eq!(p.feed(9200, Input::Take), [SendOwnership(true)]);
        assert_eq!(count_lines(&p.core.take_lines(), "WAIT"), 0);

        // (c) Manual mode: the take is given up without pausing.
        let mut p = Probe::new(false, true);
        p.routing(ready_facts());
        assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
        assert_eq!(p.feed(200, Input::Ownership(vec![1])), []);
        p.core.take_lines();
        assert_eq!(p.feed(300, Input::Ownership(vec![0])), []);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
        assert_eq!(p.feed(400, Input::Release), []);
        assert!(
            p.core
                .take_lines()
                .iter()
                .any(|l| l == "Ownership already released")
        );
    }

    #[test]
    fn yield_on_set_ownership_false() {
        // Linux took the AirPods for playback: pause and hand them over.
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(
            p.feed(700, Input::RemoteOwnershipRequest),
            [PauseMedia, SendOwnership(false)]
        );

        // Linux owns them without having taken them: hand over, no pause.
        let mut p = Probe::ready();
        assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
        assert_eq!(
            p.feed(100, Input::RemoteOwnershipRequest),
            [SendOwnership(false)]
        );
    }

    #[test]
    fn no_double_yield_remote_then_ack() {
        let mut p = acked_auto_take();
        p.core.take_lines();
        let mut all = p.feed(1100, Input::RemoteOwnershipRequest);
        all.extend(p.feed(1200, Input::Ownership(vec![0])));
        all.extend(p.play(1500, false));
        all.extend(p.feed(9000, Input::Tick));
        assert_eq!(all.iter().filter(|c| **c == PauseMedia).count(), 1);
        assert_eq!(
            all.iter().filter(|c| **c == SendOwnership(false)).count(),
            1
        );
        assert!(!all.contains(&SendOwnership(true)));
        assert_eq!(all, [PauseMedia, SendOwnership(false)]);
        // The [0] acknowledged the yield; it is not a second one.
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
    }

    #[test]
    fn no_double_yield_loss_then_remote() {
        let mut p = acked_auto_take();
        p.core.take_lines();
        let mut all = p.feed(1100, Input::Ownership(vec![0]));
        all.extend(p.feed(1200, Input::RemoteOwnershipRequest));
        all.extend(p.play(1500, false));
        all.extend(p.feed(9000, Input::Tick));
        assert_eq!(all.iter().filter(|c| **c == PauseMedia).count(), 1);
        assert!(!all.iter().any(|c| matches!(c, SendOwnership(_))));
        assert_eq!(all, [PauseMedia]);
        let lines = p.core.take_lines();
        assert_eq!(count_lines(&lines, "YIELD"), 1);
        assert!(
            lines
                .iter()
                .any(|l| l == "REMOTE requested ownership; Linux already yielded")
        );
    }

    #[test]
    fn owns_connection_zero_after_own_release_is_noop() {
        // (a) The ACK of an automatic playback-stop release.
        let mut p = acked_auto_take();
        assert_eq!(p.play(1500, false), [SendOwnership(false)]);
        p.core.take_lines();
        assert_eq!(p.feed(1600, Input::Ownership(vec![0])), []);
        let lines = p.core.take_lines();
        assert_eq!(count_lines(&lines, "YIELD"), 0);
        assert_eq!(count_lines(&lines, "ACK owner=not-linux"), 1);

        // (b) The ACK of the yield to a remote request.
        let mut p = acked_auto_take();
        assert_eq!(
            p.feed(1100, Input::RemoteOwnershipRequest),
            [PauseMedia, SendOwnership(false)]
        );
        p.core.take_lines();
        assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);
        let lines = p.core.take_lines();
        assert_eq!(count_lines(&lines, "YIELD"), 0);
        assert_eq!(count_lines(&lines, "ACK owner=not-linux"), 1);

        // (c) #43's stale init report during the reconnect take: the take is
        // not forgotten, so a stop after its ACK still releases.
        let mut p = reconnect_take();
        p.core.take_lines();
        assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
        assert_eq!(p.feed(1600, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(2000, false), [SendOwnership(false)]);
    }

    /// The c1 sequence of #55 up to the release's timeout: a remote request
    /// makes Linux yield, playback stops, and the release's ACK times out.
    /// Returns the commands from the remote request on.
    fn remote_release_timed_out() -> (Probe, Vec<Command>) {
        let mut p = acked_auto_take();
        p.core.take_lines();
        let mut all = p.feed(1100, Input::RemoteOwnershipRequest);
        assert_eq!(all, [PauseMedia, SendOwnership(false)]);
        let stop = p.play(1500, false);
        assert_eq!(stop, []);
        all.extend(stop);
        let tick = p.feed(6200, Input::Tick);
        assert_eq!(tick, []);
        all.extend(tick);
        assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);
        (p, all)
    }

    #[test]
    fn late_release_ack_after_timeout_remote() {
        let (mut p, mut all) = remote_release_timed_out();
        let late = p.feed(6300, Input::Ownership(vec![0]));
        assert_eq!(late, []);
        all.extend(late);
        let lines = p.core.take_lines();
        assert_eq!(count_lines(&lines, "YIELD"), 0);
        assert_eq!(count_lines(&lines, "ACK owner=not-linux late"), 1);
        assert_eq!(all.iter().filter(|c| **c == PauseMedia).count(), 1);
        // The release is settled: nothing is left to release or time out.
        assert_eq!(p.play(6500, false), []);
        assert_eq!(p.feed(12000, Input::Tick), []);
        assert_eq!(p.feed(12100, Input::Ownership(vec![0])), []);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
    }

    #[test]
    fn late_release_ack_after_timeout_playback() {
        let mut p = acked_auto_take();
        p.core.take_lines();
        let mut all = p.play(1500, false);
        assert_eq!(all, [SendOwnership(false)]);
        let tick = p.feed(6600, Input::Tick);
        assert_eq!(tick, []);
        all.extend(tick);
        assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);
        let late = p.feed(6700, Input::Ownership(vec![0]));
        assert_eq!(late, []);
        all.extend(late);
        assert!(!all.contains(&PauseMedia));
        let lines = p.core.take_lines();
        assert_eq!(count_lines(&lines, "YIELD"), 0);
        assert_eq!(count_lines(&lines, "ACK owner=not-linux late"), 1);
    }

    #[test]
    fn late_release_ack_after_timeout_user() {
        let mut p = acked_auto_take();
        p.core.take_lines();
        assert_eq!(p.feed(1100, Input::Release), [SendOwnership(false)]);
        assert_eq!(p.feed(6200, Input::Tick), []);
        assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);
        // Upstream #841's pause of a user release, as for an on-time ACK.
        assert_eq!(p.feed(6300, Input::Ownership(vec![0])), [PauseMedia]);
        let lines = p.core.take_lines();
        assert_eq!(count_lines(&lines, "YIELD"), 0);
        assert_eq!(count_lines(&lines, "ACK owner=not-linux late"), 1);
        // Consumed once: the release is not paused for again.
        assert_eq!(p.feed(6400, Input::Ownership(vec![0])), []);
    }

    /// Automatic playback release at 1500 ms whose ACK timed out at 6600 ms.
    fn playback_release_timed_out() -> Probe {
        let mut p = acked_auto_take();
        p.core.take_lines();
        assert_eq!(p.play(1500, false), [SendOwnership(false)]);
        assert_eq!(p.feed(6600, Input::Tick), []);
        assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);
        p
    }

    #[test]
    fn release_timeout_then_loss_after_window_yields() {
        // Long after the release's late-ACK window, a 0 is another host
        // taking the AirPods (#62): one YIELD and one pause.
        let mut p = playback_release_timed_out();
        assert_eq!(p.feed(60000, Input::Ownership(vec![0])), [PauseMedia]);
        let lines = p.core.take_lines();
        assert_eq!(count_lines(&lines, "YIELD"), 1);
        assert_eq!(count_lines(&lines, "ACK owner=not-linux late"), 0);
        assert_eq!(p.core.timed_out_release, None);
    }

    #[test]
    fn late_release_ack_window_boundary() {
        assert_eq!(LATE_ACK_WINDOW, Duration::from_secs(10));
        // The release was sent at 1500 ms; the window is inclusive.
        let edge = 1500 + LATE_ACK_WINDOW.as_millis() as u64;

        // (a) Exactly at the window's end: the late acknowledgement.
        let mut p = playback_release_timed_out();
        assert_eq!(p.feed(edge, Input::Ownership(vec![0])), []);
        let lines = p.core.take_lines();
        assert_eq!(count_lines(&lines, "ACK owner=not-linux late"), 1);
        assert_eq!(count_lines(&lines, "YIELD"), 0);
        assert_eq!(p.core.timed_out_release, None);

        // (b) One millisecond later: a genuine loss.
        let mut p = playback_release_timed_out();
        assert_eq!(p.feed(edge + 1, Input::Ownership(vec![0])), [PauseMedia]);
        let lines = p.core.take_lines();
        assert_eq!(count_lines(&lines, "YIELD"), 1);
        assert_eq!(count_lines(&lines, "ACK owner=not-linux late"), 0);
        assert_eq!(p.core.timed_out_release, None);
    }

    #[test]
    fn take_after_release_timeout_still_yields() {
        // A new take forgets the timed-out release.
        let (mut p, _) = remote_release_timed_out();
        assert_eq!(p.play(6500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(6600, Input::Ownership(vec![1])), []);
        p.core.take_lines();
        assert_eq!(p.feed(6700, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);

        // A report that Linux still owns says the release failed: a later 0
        // is a genuine loss.
        let (mut p, _) = remote_release_timed_out();
        assert_eq!(p.feed(6300, Input::Ownership(vec![1])), []);
        p.core.take_lines();
        assert_eq!(p.feed(6400, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);

        // A disconnect forgets it too.
        let (mut p, _) = remote_release_timed_out();
        assert_eq!(p.feed(6300, Input::Disconnected), [PauseMedia, DropSession]);
        assert_eq!(p.core.timed_out_release, None);
    }

    #[test]
    fn repeated_remote_request_while_release_pending() {
        let mut p = acked_auto_take();
        p.core.take_lines();
        let mut all = p.feed(1100, Input::RemoteOwnershipRequest);
        assert_eq!(all, [PauseMedia, SendOwnership(false)]);
        p.core.take_lines();
        // The same yield is still in flight: no second pause, no resend.
        let again = p.feed(1300, Input::RemoteOwnershipRequest);
        assert_eq!(again, []);
        all.extend(again);
        assert_eq!(
            p.core.take_lines(),
            ["REMOTE requested ownership; release already pending"]
        );
        let stop = p.play(1500, false);
        assert_eq!(stop, []);
        all.extend(stop);
        // 5.1 s after the first request, 4.9 s after the second: the
        // release's timeout still counts from the first.
        let tick = p.feed(6200, Input::Tick);
        assert_eq!(tick, []);
        all.extend(tick);
        assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);
        assert_eq!(all.iter().filter(|c| **c == PauseMedia).count(), 1);
    }

    #[test]
    fn repeated_remote_request_after_release_timeout() {
        let (mut p, mut all) = remote_release_timed_out();
        // The release is re-sent; the first request already paused.
        let again = p.feed(6300, Input::RemoteOwnershipRequest);
        assert_eq!(again, [SendOwnership(false)]);
        all.extend(again);
        // Its [0] is the remote release's ACK (#39): no pause, no YIELD.
        let ack = p.feed(6400, Input::Ownership(vec![0]));
        assert_eq!(ack, []);
        all.extend(ack);
        let lines = p.core.take_lines();
        assert_eq!(count_lines(&lines, "YIELD"), 0);
        assert_eq!(count_lines(&lines, "ACK owner=not-linux"), 1);
        assert_eq!(all.iter().filter(|c| **c == PauseMedia).count(), 1);
        assert_eq!(
            all.iter().filter(|c| **c == SendOwnership(false)).count(),
            2
        );
    }
}
