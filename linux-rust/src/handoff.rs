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
    /// Connected hosts report: previous and new list. Linux announces itself
    /// for a remote host in `new` that the core does not already know; the
    /// report's `old` may be stale (a replay of a report already in the
    /// session snapshot) and is not used for that decision (#58). Until
    /// `InitBacklogDrained`, `Auto` or a disconnect, a host in the
    /// `Reconnected` snapshot is known too, so stale replays never
    /// re-announce (#79); only the probe's echo timeout keeps it longer.
    ConnectedDevices {
        old: Vec<ConnectedDevice>,
        new: Vec<ConnectedDevice>,
    },
    /// OwnsConnection report; an acknowledgement when it matches the pending
    /// request. A 0 that acknowledges no release of Linux's, while Linux
    /// holds or is taking the AirPods, is a loss: Linux yields once (pauses
    /// in automatic mode, forgets the take and its pending request). The 0
    /// that acknowledges Linux's own playback-stop or remote-request release
    /// pauses nothing (#39). During the reconnect take, until
    /// `InitBacklogDrained`, a 0 only updates the cached owner (#43, #49).
    /// A 0 within `LATE_ACK_WINDOW` of sending a release
    /// whose acknowledgement timed out is that release's late
    /// acknowledgement, not a loss (#55); a later 0 is a loss (#62).
    Ownership(Vec<u8>),
    /// Another host asked Linux to give up ownership. While any Linux release
    /// is pending (playback/auto, user, quit or remote) the request joins it
    /// and sends no second "not owning" (#61, #137). The join pauses at most
    /// once, in automatic mode when Linux had taken: for a user/quit release
    /// or a playback restart during the release. A repeat after Linux's
    /// remote release timed out re-sends it without pausing again (#61).
    /// While a local call holds or is taking the AirPods, a pending or
    /// timed-out remote release still goes ahead (joined, or re-sent); any
    /// other request is refused (#11), and a release Linux already sent is
    /// not taken back.
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
    /// Queue boundary from the shell (#49): every event queued before the
    /// session's snapshot has been fed (the reconnect's init backlog up to
    /// the echo of its last "not owning" in the probe, the pre-snapshot
    /// queue on the default path); later events are fresh. It ends the
    /// reconnect take's stale-report window, so a later 0 that acknowledges
    /// nothing is a loss, and the snapshot hosts' announce exemption (#79),
    /// which `Auto` or a disconnect also ends; only the probe's echo timeout
    /// leaves it in place. Otherwise it changes nothing. Fed in queue order,
    /// never on a timer.
    InitBacklogDrained,
    /// Switch to automatic handoff (REPL `auto`). Its "not owning" is a
    /// pending release, so the reply is that release's acknowledgement and
    /// never a loss; a play edge before the reply is retaken once it
    /// settles (#54). While any request is pending (a repeated Auto, a
    /// release or a take) it sends no "not owning", so every one in flight
    /// has a tracked acknowledgement.
    Auto,
    /// Switch to manual mode (REPL `manual`).
    Manual,
    /// The user asked for the AirPods (REPL `take`).
    Take,
    /// The user gave them up (REPL `release`).
    Release,
    /// Whether Linux is in a call (#11), from the shell's call monitor.
    /// While it is, Linux keeps the AirPods it holds or is taking: a remote
    /// ownership request is refused and a playback stop releases nothing;
    /// takes are unchanged (a remote call still blocks them). A change is
    /// advertised to the remote hosts while automatic with a live session;
    /// otherwise it is only recorded. A repeat is a no-op. The state
    /// survives `Disconnected`: it is a local fact, not a session one.
    LocalCall(bool),
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
    /// Tell the connected remote hosts whether Linux is in a call (#11).
    AdvertiseCall(bool),
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
    /// take. Cleared at the shell's queue boundary (#49), the take's ACK,
    /// its timeout, a disconnect and any new request.
    reconnect_take_pending: bool,
    /// Cause and send time of Linux's last release whose acknowledgement
    /// timed out. An OwnsConnection=0 within `LATE_ACK_WINDOW` of that send
    /// is the release's late acknowledgement, not a loss (#55); a later one
    /// is a genuine loss (#62). Expiry is checked only when a report
    /// arrives, never by `tick`. Cleared when any 0 consumes it (late or
    /// not), by any report that Linux still owns, a disconnect and any new
    /// request.
    timed_out_release: Option<(Cause, Instant)>,
    /// Linux is in a call (#11): keep the AirPods Linux holds or is taking
    /// until it ends.
    local_call: bool,
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
    /// Hosts of the adopted `Reconnected` snapshot: replayed reports queued
    /// before it never announce them (#79). Cleared at `InitBacklogDrained`,
    /// `Auto` and on disconnect. Accepted limit: if the probe's echo timed
    /// out (automatic already on, so no boundary and no `Auto`) they stay
    /// exempt until the next disconnect, so a snapshot host that leaves and
    /// rejoins in that session is not re-announced (#150).
    /// A host outside the snapshot is always announced.
    snapshot_hosts: Vec<ConnectedDevice>,
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
            local_call: false,
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
            snapshot_hosts: Vec::new(),
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
            Input::ConnectedDevices { old: _, new } => {
                // Compare against the hosts the core already knows, not the
                // event's `old`: a report queued before the session snapshot
                // (seed, Reconnected facts) is replayed with a stale `old`
                // and must not announce a second time (#58).
                // Reconnected snapshot hosts count as known until the boundary,
                // `Auto` or a disconnect (#79); only the probe's echo timeout
                // leaves them known for the session.
                if self.automatic
                    && self.live()
                    && new.iter().any(|d| {
                        d.mac != self.local
                            && !self
                                .hosts
                                .iter()
                                .chain(&self.snapshot_hosts)
                                .any(|h| h.mac == d.mac)
                    })
                {
                    self.announce(out);
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
                // A release Linux already sent answers this request too: a
                // second "not owning" would get a reply that acknowledges
                // nothing and yields a later take (#61, #137). Join it,
                // keeping its `since` so the ACK timeout is not pushed back,
                // and make now the one pause its ACK would have made: a
                // user/quit release, or a playback restart it would retake
                // for. A remote release already paused when it was sent. A
                // local call refuses (below) any release but a remote one, so
                // the release still settles for the call (#11).
                let call_holds = self.local_call && self.requested_take;
                if let Some(p) = self
                    .pending
                    .as_mut()
                    .filter(|p| !p.desired && (p.cause == Cause::Remote || !call_holds))
                {
                    if self.automatic
                        && self.requested_take
                        && (matches!(p.cause, Cause::User | Cause::Quit)
                            || self.play_during_release)
                    {
                        out.push(Command::PauseMedia);
                    }
                    p.cause = Cause::Remote;
                    self.play_during_release = false;
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
                // Linux holds or is taking the AirPods for a call: keep them
                // whatever the other host plays (#11). A release Linux
                // already sent (above) is not taken back.
                if self.local_call && self.requested_take {
                    self.say("REMOTE requested ownership; refused during a local call");
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
            Input::InitBacklogDrained => {
                self.snapshot_hosts.clear();
                if std::mem::take(&mut self.reconnect_take_pending) {
                    self.say("BACKLOG drained; the reconnect take's stale-report window ended");
                } else {
                    self.say("BACKLOG drained");
                }
            }
            Input::Auto => {
                if !self.live() {
                    self.say(NO_SESSION);
                    return;
                }
                // Queued replays are fed before REPL input, so the #79 exemption can end here.
                self.snapshot_hosts.clear();
                out.push(Command::ConnectA2dp);
                if !self.requested_take && self.pending.is_none() {
                    // Tracked like a playback-stop release (#54). A release
                    // already pending covers it: a second "not owning" would
                    // get a reply that acknowledges nothing.
                    self.request(now, false, Cause::Playback, out);
                }
                self.automatic = true;
                self.announce(out);
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
                            "BLOCKED take needs Linux plus a remote host, an in-ear bud, and confirmed idle audio",
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
            Input::LocalCall(in_call) => self.local_call(now, in_call, out),
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
                        if self.local_call {
                            // The call keeps the AirPods; its end releases
                            // them if playback is still stopped.
                            self.say(
                                "AUTO playback stopped during a local call; keeping ownership",
                            );
                        } else {
                            self.request(now, false, Cause::Playback, out);
                            self.say("AUTO playback stopped; releasing ownership");
                        }
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
    /// - during the reconnect take, until the shell's queue boundary, it is
    ///   the session's stale init report and only updates `owner` (#43,
    ///   #49);
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
            if self.local_call {
                // As for a stop edge during the call: its end releases.
                self.say(
                    "AUTO playback stopped during the take; keeping ownership for the local call",
                );
            } else {
                self.request(now, false, Cause::Playback, out);
                self.say("AUTO playback stopped during the take; releasing ownership");
            }
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
        self.snapshot_hosts.clear();
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
        self.snapshot_hosts = facts.hosts.clone();
        self.hosts = facts.hosts;
        self.ears = facts.ears;
        self.audio = facts.audio;
        self.owner = facts.owner;
        if self.automatic {
            self.announce(out);
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

    /// Announce Linux to the remote hosts. The announcement carries the idle
    /// media information, so during a call the call is advertised again
    /// after it (#11).
    fn announce(&self, out: &mut Vec<Command>) {
        out.push(Command::Announce);
        if self.local_call {
            out.push(Command::AdvertiseCall(true));
        }
    }

    /// The call monitor reported Linux's call state (#11).
    fn local_call(&mut self, now: Instant, in_call: bool, out: &mut Vec<Command>) {
        if self.local_call == in_call {
            return;
        }
        self.local_call = in_call;
        // Without a live automatic session only the state is kept: nothing
        // can be sent, and in manual mode Linux announces nothing.
        if !self.automatic || !self.live() {
            return;
        }
        out.push(Command::AdvertiseCall(in_call));
        if in_call {
            self.say("CALL started on Linux; keeping the AirPods Linux holds until it ends");
            return;
        }
        self.say("CALL ended on Linux");
        // A stop seen during the call (or while its take was settling) was
        // held for it; release now if playback is still stopped. A pending
        // request settles through `reconcile` as usual.
        if self.requested_take
            && self.pending.is_none()
            && self.was_playing != Some(true)
            && !self.quitting
        {
            self.request(now, false, Cause::Playback, out);
            self.say("AUTO call ended with playback stopped; releasing ownership");
        }
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
    const REMOTE_2: &str = "0D:0E:0F:10:11:12";
    const REMOTE_3: &str = "13:14:15:16:17:18";

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

        /// Automatic, connected, Linux plus a remote host, worn, idle audio,
        /// and a first "stopped" sample as the baseline.
        fn ready() -> Self {
            Self::ready_with_hosts(two_hosts())
        }

        /// `ready`, with the AirPods reporting `hosts` instead.
        fn ready_with_hosts(hosts: Vec<ConnectedDevice>) -> Self {
            let mut probe = Self::new(true, true);
            probe.routing(SessionFacts {
                hosts,
                ..ready_facts()
            });
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
    fn takes_with_three_or_four_hosts() {
        // Linux plus two or three remotes (hardware #22: iPhone + Mac +
        // Linux) takes like Linux plus one.
        let hosts_lists = [
            vec![host(LOCAL), host(REMOTE), host(REMOTE_2)],
            vec![host(REMOTE), host(LOCAL), host(REMOTE_2), host(REMOTE_3)],
        ];
        for hosts in hosts_lists {
            let n = hosts.len();
            let mut p = Probe::ready_with_hosts(hosts.clone());
            assert_eq!(p.play(500, true), [SendOwnership(true)], "hosts={n}");

            let mut p = Probe::new(false, true);
            p.routing(SessionFacts {
                hosts,
                ..ready_facts()
            });
            p.core.take_lines();
            assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)], "hosts={n}");
            let lines = p.core.take_lines();
            assert!(
                lines.iter().any(|l| l.starts_with("REQUEST owner=linux")),
                "hosts={n}: {lines:?}"
            );
        }
    }

    #[test]
    fn no_take_when_only_linux_hosts() {
        let mut p = Probe::ready_with_hosts(vec![host(LOCAL)]);
        assert_eq!(p.play(500, true), []);
        p.core.take_lines();
        assert_eq!(p.feed(600, Input::Take), []);
        let lines = p.core.take_lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].starts_with("BLOCKED take needs Linux plus a remote host"),
            "{lines:?}"
        );
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

        // Hosts that are not this machine plus a remote one: Linux alone,
        // then remotes without Linux.
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
        let remotes_only = vec![host(REMOTE), host(REMOTE_2)];
        assert_eq!(
            p.feed(
                1100,
                Input::ConnectedDevices {
                    old: vec![host(LOCAL)],
                    new: remotes_only.clone(),
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
                    old: remotes_only,
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

    fn announces(out: &[Command]) -> usize {
        out.iter().filter(|c| matches!(c, Announce)).count()
    }

    fn stale_replay() -> Input {
        Input::ConnectedDevices {
            old: vec![host(LOCAL)],
            new: two_hosts(),
        }
    }

    #[test]
    fn connected_devices_replay_after_auto_no_second_announce() {
        // #58: the startup seed, then Auto, then the queued report that the
        // seed already contained, replayed with its pre-update `old`.
        for automatic in [false, true] {
            let mut p = Probe::new(automatic, true);
            p.feed(
                0,
                Input::ConnectedDevices {
                    old: Vec::new(),
                    new: two_hosts(),
                },
            );
            let auto = p.feed(10, Input::Auto);
            assert_eq!(announces(&auto), 1, "automatic={automatic}: {auto:?}");
            assert_eq!(p.feed(20, stale_replay()), [], "automatic={automatic}");
        }
    }

    #[test]
    fn connected_devices_replay_after_reconnect_no_second_announce() {
        // #58: the reconnect's facts already hold the remote host; the
        // report queued before them is replayed without a second announce.
        let mut p = Probe::new(true, true);
        assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
        assert_eq!(p.feed(5000, Input::Tick), [Reconnect { audio: true }]);
        let facts = SessionFacts {
            hosts: two_hosts(),
            ..SessionFacts::default()
        };
        let out = p.feed(5100, Input::Reconnected(facts));
        assert_eq!(announces(&out), 1, "{out:?}");
        assert_eq!(p.feed(5200, stale_replay()), []);
    }

    #[test]
    fn connected_devices_new_host_still_announced() {
        let second = "0D:0E:0F:10:11:12";
        let three = || vec![host(LOCAL), host(REMOTE), host(second)];

        // A fresh `old`.
        let mut p = Probe::ready();
        assert_eq!(
            p.feed(
                100,
                Input::ConnectedDevices {
                    old: two_hosts(),
                    new: three(),
                },
            ),
            [Announce]
        );
        // The same report again announces nothing.
        assert_eq!(
            p.feed(
                200,
                Input::ConnectedDevices {
                    old: two_hosts(),
                    new: three(),
                },
            ),
            []
        );

        // A stale `old` still announces the new host exactly once.
        let mut p = Probe::ready();
        assert_eq!(
            p.feed(
                100,
                Input::ConnectedDevices {
                    old: vec![host(LOCAL)],
                    new: three(),
                },
            ),
            [Announce]
        );
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
        // The reply to Auto's "not owning" (#54).
        assert_eq!(p.feed(1050, Input::Ownership(vec![0])), []);
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

    #[test]
    fn reconnect_take_window_ends_at_backlog_boundary() {
        let mut p = reconnect_take();
        // Queued before the boundary: the stale init report (#43).
        assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
        p.core.take_lines();
        assert_eq!(p.feed(1350, Input::InitBacklogDrained), []);
        assert_eq!(count_lines(&p.core.take_lines(), "BACKLOG drained"), 1);
        // After it a [0] acknowledges nothing: a loss, yielded once (#39).
        assert_eq!(p.feed(1400, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
        // `requested_take` is cleared: the stop releases nothing.
        assert_eq!(p.play(1500, false), []);
        // The pending take was dropped: no TIMEOUT for it.
        assert_eq!(p.feed(7000, Input::Tick), []);
        assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 0);
    }

    #[test]
    fn backlog_boundary_outside_reconnect_window_is_noop() {
        // Take, ACK, stop, release ACK, take again, then a loss.
        let script = |p: &mut Probe| {
            vec![
                p.play(1100, true),
                p.feed(1200, Input::Ownership(vec![1])),
                p.play(1300, false),
                p.feed(1400, Input::Ownership(vec![0])),
                p.play(1500, true),
                p.feed(1600, Input::Ownership(vec![0])),
            ]
        };
        for setup in [Probe::ready as fn() -> Probe, acked_auto_take] {
            let mut with = setup();
            assert_eq!(with.feed(1050, Input::InitBacklogDrained), []);
            let commands = script(&mut with);
            assert_eq!(commands, script(&mut setup()));
            assert_eq!(commands.last().unwrap(), &[PauseMedia]);
        }
        // An acked take still yields on [0] after the boundary.
        let mut p = acked_auto_take();
        assert_eq!(p.feed(1050, Input::InitBacklogDrained), []);
        assert_eq!(p.feed(1100, Input::Ownership(vec![0])), [PauseMedia]);
    }

    #[test]
    fn auto_release_reply_never_yields() {
        // #54: a stop->play edge lands before the reply to Auto's "not
        // owning"; the reply acknowledges that release and the edge is
        // retaken, with no yield or pause.
        let mut p = Probe::new(false, true);
        p.routing(ready_facts());
        assert_eq!(
            p.feed(100, Input::Auto),
            [ConnectA2dp, SendOwnership(false), Announce]
        );
        p.core.take_lines();
        let mut all = p.play(200, false);
        all.extend(p.play(300, true));
        all.extend(p.feed(400, Input::Ownership(vec![0])));
        assert_eq!(all, [SendOwnership(true)]);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
        assert_eq!(p.feed(500, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(600, false), [SendOwnership(false)]);
    }

    #[test]
    fn repeated_auto_reply_never_yields() {
        // A second Auto while the first one's release is pending sends no
        // second "not owning", so no reply is left acknowledging nothing.
        let mut p = Probe::new(false, true);
        p.routing(ready_facts());
        let mut all = p.feed(100, Input::Auto);
        all.extend(p.feed(150, Input::Auto));
        all.extend(p.play(200, false));
        all.extend(p.play(300, true));
        let releases = all.iter().filter(|c| **c == SendOwnership(false)).count();
        for i in 0..releases {
            all.extend(p.feed(400 + i as u64, Input::Ownership(vec![0])));
        }
        all.extend(p.feed(500, Input::Ownership(vec![1])));
        assert!(!all.contains(&PauseMedia));
        assert_eq!(all.iter().filter(|c| **c == SendOwnership(true)).count(), 1);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
        assert_eq!(p.play(600, false), [SendOwnership(false)]);
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

    fn count(all: &[Vec<Command>], command: Command) -> usize {
        all.iter().flatten().filter(|c| **c == command).count()
    }

    #[test]
    fn remote_request_joins_pending_playback_release() {
        // #137: a remote request during a playback-stop release joins it. No
        // second "not owning" is sent, so no stray [0] yields the retake.
        let mut p = acked_auto_take();
        p.core.take_lines();
        let all = [
            p.play(1500, false),
            p.feed(1800, Input::RemoteOwnershipRequest),
            p.feed(1900, Input::Ownership(vec![0])),
            p.play(2000, true),
            p.feed(2100, Input::Ownership(vec![1])),
        ];
        assert_eq!(
            all,
            [
                vec![SendOwnership(false)],
                vec![],
                vec![],
                vec![SendOwnership(true)],
                vec![],
            ]
        );
        assert_eq!(count(&all, SendOwnership(false)), 1);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);

        // The joined release's timeout still counts from its send at 1500.
        let mut p = acked_auto_take();
        assert_eq!(p.play(1500, false), [SendOwnership(false)]);
        assert_eq!(p.feed(1800, Input::RemoteOwnershipRequest), []);
        p.core.take_lines();
        let tick = 1500 + ACK_TIMEOUT.as_millis() as u64 + 1;
        assert_eq!(p.feed(tick, Input::Tick), []);
        assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);

        // Playback restarted during the release: the request pauses it once
        // and the ACK neither retakes nor pauses again.
        let mut p = acked_auto_take();
        p.core.take_lines();
        assert_eq!(p.play(1500, false), [SendOwnership(false)]);
        assert_eq!(p.play(1600, true), []);
        assert_eq!(p.feed(1700, Input::RemoteOwnershipRequest), [PauseMedia]);
        assert_eq!(p.feed(1800, Input::Ownership(vec![0])), []);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);

        // A call started during the release: refused as before (#11), so
        // the ACK still retakes for the restarted playback.
        let mut p = acked_auto_take();
        assert_eq!(p.play(1500, false), [SendOwnership(false)]);
        assert_eq!(p.feed(1550, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.play(1600, true), []);
        p.core.take_lines();
        assert_eq!(p.feed(1700, Input::RemoteOwnershipRequest), []);
        assert_eq!(
            p.core.take_lines(),
            ["REMOTE requested ownership; refused during a local call"]
        );
        assert_eq!(
            p.feed(1800, Input::Ownership(vec![0])),
            [SendOwnership(true)]
        );
    }

    #[test]
    fn remote_request_joins_pending_auto_release() {
        // #49's Auto release joined by a remote request (#137).
        let mut p = Probe::new(false, true);
        p.routing(SessionFacts {
            owner: Some(vec![1]),
            ..ready_facts()
        });
        let all = [
            p.feed(100, Input::Auto),
            p.feed(200, Input::RemoteOwnershipRequest),
            p.feed(300, Input::Ownership(vec![0])),
            p.play(400, false),
            p.play(500, true),
            p.feed(600, Input::Ownership(vec![1])),
        ];
        assert_eq!(
            all,
            [
                vec![ConnectA2dp, SendOwnership(false), Announce],
                vec![],
                vec![],
                vec![],
                vec![SendOwnership(true)],
                vec![],
            ]
        );
        assert_eq!(count(&all, SendOwnership(false)), 1);
        assert_eq!(count(&all, PauseMedia), 0);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
    }

    #[test]
    fn remote_request_joins_pending_user_or_quit_release() {
        // A user or quit release of a live take: the request makes the one
        // pause the release's ACK would have made (#137).
        for (release, ack) in [(Input::Release, vec![]), (Input::Quit, vec![Exit])] {
            let mut p = Probe::ready();
            p.core.take_lines();
            let all = [
                p.feed(100, Input::Take),
                p.feed(200, Input::Ownership(vec![1])),
                p.feed(300, release),
                p.feed(400, Input::RemoteOwnershipRequest),
                p.feed(500, Input::Ownership(vec![0])),
            ];
            assert_eq!(
                all,
                [
                    vec![SendOwnership(true)],
                    vec![],
                    vec![SendOwnership(false)],
                    vec![PauseMedia],
                    ack,
                ]
            );
            assert_eq!(count(&all, PauseMedia), 1);
            assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
        }
    }

    /// `acked_auto_take` (playing at 1000 ms), then a local call starts.
    fn acked_take_in_call() -> Probe {
        let mut p = acked_auto_take();
        p.core.take_lines();
        assert_eq!(p.feed(1050, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(
            p.core.take_lines(),
            ["CALL started on Linux; keeping the AirPods Linux holds until it ends"]
        );
        p
    }

    #[test]
    fn local_call_refuses_remote_request() {
        // An iPhone starts media and asks for the AirPods mid-call.
        let mut p = acked_take_in_call();
        assert_eq!(
            p.feed(
                1100,
                Input::AudioSource(source(REMOTE, AudioSourceType::Media))
            ),
            []
        );
        assert_eq!(p.feed(1200, Input::RemoteOwnershipRequest), []);
        assert_eq!(
            p.core.take_lines(),
            ["REMOTE requested ownership; refused during a local call"]
        );
        // Whatever the remote plays, and however often it asks.
        assert_eq!(
            p.feed(
                1300,
                Input::AudioSource(source(REMOTE, AudioSourceType::Call))
            ),
            []
        );
        assert_eq!(p.feed(1400, Input::RemoteOwnershipRequest), []);
        assert_eq!(p.feed(9000, Input::Tick), []);
        let lines = p.core.take_lines();
        assert_eq!(
            count_lines(&lines, "REMOTE requested ownership; refused"),
            1
        );
        assert_eq!(count_lines(&lines, "TIMEOUT"), 0);

        // A take still pending is kept too.
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(550, Input::LocalCall(true)), [AdvertiseCall(true)]);
        p.core.take_lines();
        assert_eq!(p.feed(600, Input::RemoteOwnershipRequest), []);
        assert_eq!(
            p.core.take_lines(),
            ["REMOTE requested ownership; refused during a local call"]
        );
        assert_eq!(p.feed(700, Input::Ownership(vec![1])), []);

        // A dead session is reported first, as without a call.
        let mut p = acked_take_in_call();
        assert_eq!(p.feed(1100, Input::SessionLost), []);
        p.core.take_lines();
        assert_eq!(p.feed(1200, Input::RemoteOwnershipRequest), []);
        assert_eq!(
            p.core.take_lines(),
            ["REMOTE requested ownership; control session lost"]
        );

        // Linux owns them without having taken them: the existing yield.
        let mut p = Probe::ready();
        p.feed(20, Input::Ownership(vec![1]));
        assert_eq!(p.feed(30, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(
            p.feed(100, Input::RemoteOwnershipRequest),
            [SendOwnership(false)]
        );
    }

    #[test]
    fn local_call_holds_on_playback_stop() {
        let mut p = acked_take_in_call();
        assert_eq!(p.play(1500, false), []);
        assert_eq!(
            p.core.take_lines(),
            ["AUTO playback stopped during a local call; keeping ownership"]
        );
        assert_eq!(p.play(2000, false), []);
        assert_eq!(p.feed(9000, Input::Tick), []);
        // A genuine loss still yields once (#39).
        p.core.take_lines();
        assert_eq!(p.feed(9100, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
        // Nothing is left to release when the call ends.
        assert_eq!(
            p.feed(9200, Input::LocalCall(false)),
            [AdvertiseCall(false)]
        );
    }

    #[test]
    fn local_call_holds_stop_during_pending_take() {
        // Stopped while the take was pending, ACKed during the call.
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(550, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.play(1000, false), []);
        p.core.take_lines();
        assert_eq!(p.feed(1100, Input::Ownership(vec![1])), []);
        let lines = p.core.take_lines();
        assert_eq!(
            count_lines(
                &lines,
                "AUTO playback stopped during the take; keeping ownership for the local call"
            ),
            1
        );
        assert_eq!(p.play(1500, false), []);
        assert_eq!(p.feed(9000, Input::Tick), []);
        // The held stop is released when the call ends.
        assert_eq!(
            p.feed(9100, Input::LocalCall(false)),
            [AdvertiseCall(false), SendOwnership(false)]
        );
        assert_eq!(p.feed(9200, Input::Ownership(vec![0])), []);

        // The same when the take's ACK times out during the call.
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(550, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.play(1000, false), []);
        assert_eq!(p.feed(5600, Input::Tick), []);
        assert_eq!(
            p.feed(6000, Input::LocalCall(false)),
            [AdvertiseCall(false), SendOwnership(false)]
        );

        // The call ends while the take is still pending: its ACK releases.
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(550, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.play(1000, false), []);
        assert_eq!(
            p.feed(1100, Input::LocalCall(false)),
            [AdvertiseCall(false)]
        );
        assert_eq!(
            p.feed(1200, Input::Ownership(vec![1])),
            [SendOwnership(false)]
        );
    }

    #[test]
    fn local_call_repeat_is_noop() {
        let mut p = acked_take_in_call();
        assert_eq!(p.feed(1100, Input::LocalCall(true)), []);
        assert!(p.core.take_lines().is_empty());
        assert_eq!(
            p.feed(1200, Input::LocalCall(false)),
            [AdvertiseCall(false)]
        );
        p.core.take_lines();
        assert_eq!(p.feed(1300, Input::LocalCall(false)), []);
        assert!(p.core.take_lines().is_empty());

        // A first report of no call is no change either.
        let mut p = Probe::ready();
        assert_eq!(p.feed(20, Input::LocalCall(false)), []);
        assert!(p.core.take_lines().is_empty());
    }

    #[test]
    fn local_call_recorded_without_live_session() {
        // Manual mode records the call; going automatic advertises it.
        let mut p = Probe::new(false, true);
        p.routing(ready_facts());
        assert_eq!(p.feed(100, Input::LocalCall(true)), []);
        assert_eq!(
            p.feed(200, Input::Auto),
            [
                ConnectA2dp,
                SendOwnership(false),
                Announce,
                AdvertiseCall(true)
            ]
        );

        // The call survives a disconnect and is advertised after the
        // reconnect's announcement; changes while disconnected only record.
        let mut p = acked_take_in_call();
        assert_eq!(p.feed(1100, Input::Disconnected), [PauseMedia, DropSession]);
        assert_eq!(p.feed(1150, Input::LocalCall(false)), []);
        assert_eq!(p.feed(1160, Input::LocalCall(true)), []);
        assert_eq!(p.feed(2200, Input::Tick), [Reconnect { audio: true }]);
        assert_eq!(
            p.feed(2300, Input::Reconnected(ready_facts())),
            [AdoptSession, Announce, AdvertiseCall(true), DrainPlayback]
        );

        // A new host announced during the call learns of it too.
        let three = vec![host(LOCAL), host(REMOTE), host("0D:0E:0F:10:11:12")];
        assert_eq!(
            p.feed(
                2400,
                Input::ConnectedDevices {
                    old: two_hosts(),
                    new: three,
                },
            ),
            [Announce, AdvertiseCall(true)]
        );
    }

    #[test]
    fn remote_call_blocks_takeover_during_local_call() {
        let mut p = Probe::ready();
        p.feed(
            20,
            Input::AudioSource(source(REMOTE, AudioSourceType::Call)),
        );
        assert_eq!(p.feed(30, Input::LocalCall(true)), [AdvertiseCall(true)]);
        // Fresh playback and a manual take are blocked.
        assert_eq!(p.play(500, true), []);
        assert_eq!(p.play(1000, false), []);
        assert_eq!(p.play(1500, true), []);
        p.core.take_lines();
        assert_eq!(p.feed(1600, Input::Take), []);
        assert!(p.core.take_lines().iter().any(|l| l.starts_with("BLOCKED")));

        // A reconnect for new playback after case closure, into a session
        // whose facts still show the remote call: no take.
        assert_eq!(p.play(2000, false), []);
        p.feed(2100, Input::EarState(vec![InCase, InCase]));
        assert_eq!(p.feed(2200, Input::Disconnected), [DropSession]);
        assert_eq!(p.play(2500, false), []);
        assert_eq!(p.play(3000, true), []);
        assert_eq!(p.feed(3100, Input::Tick), [Reconnect { audio: true }]);
        let facts = SessionFacts {
            audio: Some(source(REMOTE, AudioSourceType::Call)),
            ..ready_facts()
        };
        p.core.take_lines();
        assert_eq!(
            p.feed(3200, Input::Reconnected(facts)),
            [AdoptSession, Announce, AdvertiseCall(true), DrainPlayback]
        );
        assert!(
            p.core
                .take_lines()
                .iter()
                .any(|l| l.starts_with("AUTO reconnected; takeover blocked"))
        );
        assert_eq!(p.feed(3300, Input::Take), []);

        // The remote call ends: the next fresh playback takes.
        p.feed(3400, Input::AudioSource(idle()));
        assert_eq!(p.play(3500, false), []);
        assert_eq!(p.play(4000, true), [SendOwnership(true)]);
    }

    #[test]
    fn call_end_restores_policy() {
        // Still playing when the call ends: nothing is released.
        let mut p = acked_take_in_call();
        assert_eq!(p.play(1500, true), []);
        assert_eq!(
            p.feed(1600, Input::LocalCall(false)),
            [AdvertiseCall(false)]
        );
        assert_eq!(p.core.take_lines(), ["CALL ended on Linux"]);
        // A remote request yields as in yields_on_remote_request.
        assert_eq!(
            p.feed(1700, Input::RemoteOwnershipRequest),
            [PauseMedia, SendOwnership(false)]
        );
        assert_eq!(p.feed(1800, Input::Ownership(vec![0])), []);

        // A stop edge releases as in takes_on_new_playback_when_worn.
        let mut p = acked_take_in_call();
        assert_eq!(
            p.feed(1100, Input::LocalCall(false)),
            [AdvertiseCall(false)]
        );
        assert_eq!(p.play(1500, false), [SendOwnership(false)]);
        assert_eq!(p.feed(1600, Input::Ownership(vec![0])), []);
        assert_eq!(p.play(2000, false), []);
    }

    #[test]
    fn call_end_releases_when_stopped() {
        let mut p = acked_take_in_call();
        assert_eq!(p.play(1500, false), []);
        p.core.take_lines();
        assert_eq!(
            p.feed(2000, Input::LocalCall(false)),
            [AdvertiseCall(false), SendOwnership(false)]
        );
        assert_eq!(
            p.core.take_lines(),
            [
                "CALL ended on Linux",
                "AUTO call ended with playback stopped; releasing ownership"
            ]
        );
        // A playback release: its ACK pauses nothing (#39).
        assert_eq!(p.feed(2100, Input::Ownership(vec![0])), []);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
        assert_eq!(p.play(2500, false), []);
        assert_eq!(p.feed(9000, Input::Tick), []);

        // Without a take a call end releases nothing.
        let mut p = Probe::ready();
        assert_eq!(p.feed(20, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.feed(30, Input::LocalCall(false)), [AdvertiseCall(false)]);
    }

    fn report(old: &[&str], new: &[&str]) -> Input {
        let hosts = |macs: &[&str]| macs.iter().map(|m| host(m)).collect();
        Input::ConnectedDevices {
            old: hosts(old),
            new: hosts(new),
        }
    }

    /// Drop the session at `ms` and adopt a reconnect reporting `hosts`,
    /// which announces once.
    fn reconnect_with(p: &mut Probe, ms: u64, hosts: &[&str]) {
        assert_eq!(p.feed(ms, Input::Disconnected), [DropSession]);
        assert_eq!(p.feed(ms + 1000, Input::Tick), [Reconnect { audio: true }]);
        let facts = SessionFacts {
            hosts: hosts.iter().map(|m| host(m)).collect(),
            ..SessionFacts::default()
        };
        let out = p.feed(ms + 1100, Input::Reconnected(facts));
        assert_eq!(announces(&out), 1, "{out:?}");
    }

    const L: &str = LOCAL;
    const R: &str = REMOTE;
    const R2: &str = REMOTE_2;
    const R3: &str = REMOTE_3;

    #[test]
    fn connected_devices_two_stale_replays_after_reconnect_no_reannounce() {
        // #79: the probe adopts the reconnect, then replays its init backlog.
        let mut p = Probe::new(true, true);
        reconnect_with(&mut p, 100, &[L, R, R2]);
        assert_eq!(p.feed(1300, report(&[L], &[L, R])), []);
        // Without the snapshot exemption this one announced R2 again.
        assert_eq!(p.feed(1400, report(&[L, R], &[L, R, R2])), []);
        assert_eq!(p.feed(1500, Input::InitBacklogDrained), []);
        assert_eq!(
            p.feed(1600, report(&[L, R, R2], &[L, R, R2, R3])),
            [Announce]
        );
    }

    #[test]
    fn connected_devices_two_stale_replays_default_order_one_announce() {
        // run_handoff: seed, pre-snapshot backlog, Auto, boundary.
        let mut p = Probe::new(false, true);
        assert_eq!(p.feed(0, report(&[], &[L, R, R2])), []);
        assert_eq!(p.feed(10, report(&[L], &[L, R])), []);
        assert_eq!(p.feed(20, report(&[L, R], &[L, R, R2])), []);
        let auto = p.feed(30, Input::Auto);
        assert_eq!(announces(&auto), 1, "{auto:?}");
        assert_eq!(p.feed(40, Input::InitBacklogDrained), []);
        assert_eq!(p.feed(50, report(&[L, R, R2], &[L, R, R2, R3])), [Announce]);
    }

    #[test]
    fn connected_devices_snapshot_exemption_ends_at_boundary_or_disconnect() {
        // (a) After the boundary a snapshot host that leaves and rejoins is
        // announced again.
        let mut p = Probe::new(true, true);
        reconnect_with(&mut p, 100, &[L, R, R2]);
        assert_eq!(p.feed(1300, Input::InitBacklogDrained), []);
        assert_eq!(p.feed(1400, report(&[L, R, R2], &[L, R])), []);
        assert_eq!(p.feed(1500, report(&[L, R], &[L, R, R2])), [Announce]);

        // (b) A disconnect ends it without a boundary.
        let mut p = Probe::new(true, true);
        reconnect_with(&mut p, 100, &[L, R, R2]);
        reconnect_with(&mut p, 2000, &[L, R]);
        assert_eq!(p.feed(3200, Input::InitBacklogDrained), []);
        assert_eq!(p.feed(3300, report(&[L, R], &[L, R, R2])), [Announce]);

        // (c) Before any boundary a host outside the snapshot is announced.
        let mut p = Probe::new(true, true);
        reconnect_with(&mut p, 100, &[L, R, R2]);
        assert_eq!(p.feed(1300, report(&[L], &[L, R])), []);
        assert_eq!(p.feed(1400, report(&[L, R], &[L, R, R3])), [Announce]);
    }

    #[test]
    fn connected_devices_snapshot_exemption_ends_at_auto() {
        // #150: a manual/no-audio reconnect gets no boundary; Auto ends it.
        let mut p = Probe::new(false, true);
        assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
        assert_eq!(p.feed(1100, Input::Tick), [Reconnect { audio: false }]);
        let facts = SessionFacts {
            hosts: vec![host(L), host(R)],
            ..SessionFacts::default()
        };
        let out = p.feed(1200, Input::Reconnected(facts));
        assert_eq!(announces(&out), 0, "{out:?}");
        let auto = p.feed(1300, Input::Auto);
        assert_eq!(announces(&auto), 1, "{auto:?}");
        assert_eq!(p.feed(1400, report(&[L, R], &[L])), []);
        assert_eq!(p.feed(1500, report(&[L], &[L, R])), [Announce]);
    }
}
