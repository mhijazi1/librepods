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
    AUDIO_CATEGORY_IDLE, CaseRecovery, HandoffGuards, RoutingSnapshot, TakeoverAction,
    TakeoverDecision, TakeoverInputs, TakeoverSkip, TakeoverTrigger, case_closed_after,
    decide_takeover, routing_snapshot,
};
use crate::utils::ConnectMode;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// How long an ownership request may wait for its acknowledgement.
pub(crate) const ACK_TIMEOUT: Duration = Duration::from_secs(5);
/// How long after a release was sent an OwnsConnection=0 may still be taken
/// for its late acknowledgement once its ACK timed out (#62), inclusive.
/// Measured from the send, so the tick cadence does not move it. Twice
/// `ACK_TIMEOUT`, an engineering default: a capture of real late-ACK delays
/// may retune it. A later 0 is a genuine loss and yields.
pub(crate) const LATE_ACK_WINDOW: Duration = Duration::from_secs(10);
/// After a bud goes back in while another host owns the AirPods, a play edge
/// sampled within this long is ear detection's resume and takes nothing, as
/// on macOS (#301, #304). Covers one playback poll plus a stream's debounce.
pub(crate) const EAR_RESUME_GRACE: Duration = Duration::from_secs(3);
const EAR_RESUMED: &str =
    "AUTO playback resumed by ear detection while another host owns; not taking";
/// How long parked playback waits for its take's ACK after a play edge
/// before it falls back to the local output (#311), as the Mac's
/// `RouteToSpeaker … in 2.00s`.
pub(crate) const PARK_TIMEOUT: Duration = Duration::from_secs(2);
/// First reconnect delay after a disconnect or a successful reconnect.
const FIRST_RETRY: Duration = Duration::from_secs(1);
/// Reconnect backoff cap.
const MAX_RETRY: Duration = Duration::from_secs(8);
/// Reply to a REPL command that needs a live control session.
const NO_SESSION: &str =
    "WAIT no live control session; reconnecting (status, manual and quit remain available)";
/// An automatic take the connect mode blocked (#16). `handoff_log::classify`
/// maps it to `Kind::Other`: it is no take request.
const CONNECT_MODE_SKIP: &str =
    "AUTO takeover skipped: connect mode is when-last-connected and Linux was not the last owner";
/// The connect take (#324).
const CONNECT_TAKE: &str = "AUTO connected while the other host is idle; requested Linux ownership";
/// How long after `Auto` its ownership decision waits for the routing
/// reports the connect take needs, which land just after it on hardware
/// (#452, spike #157).
const CONNECT_TAKE_WINDOW: Duration = Duration::from_secs(2);

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
    /// A playback poll: Linux's audio category at `sampled_at` (#272): 301
    /// while an MPRIS player plays, else 201 for a counted stream, else 100.
    /// Above 100 is playing. A rise (100 to 201 or 301, 201 to 301) asks for
    /// a take; only a fall to 100 is a stop. `new_source` is the category of
    /// a local source that started since the poller's previous sample (#96):
    /// behind playback already above 100 it asks for a take at that category.
    Playback {
        sampled_at: Instant,
        category: u32,
        new_source: Option<u32>,
    },
    /// Wear report (AACP ear detection).
    EarState(Vec<EarDetectionStatus>),
    /// The shell's ear-detection resume for one reinsertion: whether it
    /// played any player (#313). A `false` answering the last reinsertion
    /// still due a report ends the ear-resume grace, unless a `true` came in
    /// for that grace.
    EarResume { resumed: bool },
    /// Audio source report.
    AudioSource(AudioSource),
    /// Connected hosts report: previous and new list. Linux announces itself
    /// for a remote host in `new` that the core does not already know; the
    /// report's `old` may be stale (a replay of a report already in the
    /// session snapshot) and is not used for that decision (#58). Until
    /// `InitBacklogDrained`, `Auto` or a disconnect, a host in the
    /// `Reconnected` snapshot is known too, so stale replays never
    /// re-announce (#79); only the probe's echo timeout (cached owner [1], a
    /// real release, #290) keeps it longer.
    ConnectedDevices {
        old: Vec<ConnectedDevice>,
        new: Vec<ConnectedDevice>,
    },
    /// OwnsConnection report; an acknowledgement when it matches the pending
    /// request. A 0 that acknowledges no release of Linux's, while Linux
    /// holds or is taking the AirPods, is a loss: Linux yields once (pauses
    /// in automatic mode, forgets the take and its pending request). The 0
    /// that acknowledges Linux's own `Auto` or remote-request release
    /// pauses nothing (#39). During the reconnect take, until
    /// `InitBacklogDrained`, a 0 only updates the cached owner (#43, #49).
    /// A 0 within `LATE_ACK_WINDOW` of sending a release
    /// whose acknowledgement timed out is that release's late
    /// acknowledgement, not a loss (#55); a later 0 is a loss (#62).
    Ownership(Vec<u8>),
    /// Another host asked Linux to give up ownership. While any Linux release
    /// is pending (playback/auto, user, quit or remote) the request joins it
    /// and sends no second "not owning" (#61, #137). The join pauses at most
    /// once, in automatic mode when Linux had taken, for a user/quit release.
    /// A repeat after Linux's remote release timed out re-sends it without
    /// pausing again (#61).
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
    /// the probe's facts cut (#143), which waits for the echo of its last
    /// "not owning" only when the cached owner was [1], a real release
    /// (#290); the pre-snapshot queue on the default path); later events are
    /// fresh. It ends the reconnect take's stale-report window, so a later 0
    /// that acknowledges nothing is a loss, and the snapshot hosts' announce
    /// exemption (#79), which `Auto` or a disconnect also ends; only that
    /// owner-[1] echo timing out leaves it in place. Otherwise it changes
    /// nothing. Fed in queue order, never on a timer.
    InitBacklogDrained,
    /// Switch to automatic handoff (REPL `auto`). When the cached owner is
    /// Linux ([1]) its "not owning" is a pending release, so the reply is
    /// that release's acknowledgement and never a loss; a play edge before
    /// the reply is retaken once it settles (#54). With owner [0] or unknown
    /// it is sent untracked: the AirPods reply only to a real change (#159),
    /// so nothing waits for a reply (#289). Accepted limit: with an unknown
    /// owner while Linux did own, a take sent before that [0] arrives
    /// yields to it. While any request is pending (a repeated Auto, a
    /// release or a take) it sends no "not owning", so no reply is left
    /// acknowledging nothing. Nor while Linux holds a take: Linux stays the
    /// idle owner (#262). When the other host is idle (the connect take's
    /// guards, #324), it takes with only "owning" instead, or keeps an owner
    /// already reported as Linux ([1]) without sending anything. Unless no
    /// report can allow the take, that one decision waits up to
    /// `CONNECT_TAKE_WINDOW` for hosts, ears, an audio source and a remote
    /// media source's relay (#452); an OwnsConnection [1] in the wait keeps
    /// at once, the deadline `Tick` releases. A play edge's take, a remote
    /// request, a lost session or quit end the wait with no decision of its
    /// own.
    Auto,
    /// Switch to manual mode (REPL `manual`). Linux keeps the AirPods it
    /// holds (#253).
    Manual,
    /// The user asked for the AirPods (REPL `take`).
    Take,
    /// The user gave them up (REPL `release`).
    Release,
    /// Whether Linux is in a call (#11), from the shell's call monitor.
    /// While it is, Linux keeps the AirPods it holds or is taking: a remote
    /// ownership request is refused; takes are unchanged (a remote call still
    /// blocks them). Its end releases nothing (#262). A change is
    /// advertised to the remote hosts while automatic with a live session;
    /// otherwise it is only recorded. A repeat is a no-op. The state
    /// survives `Disconnected`: it is a local fact, not a session one.
    LocalCall(bool),
    /// REPL `quit`, end of input, SIGINT or SIGTERM.
    Quit,
    /// Periodic tick: reconnect backoff, acknowledgement timeouts and the
    /// end of `Auto`'s wait (#452).
    Tick,
    /// Whether the AirPods' A2DP transport streams (BlueZ
    /// `MediaTransport1.State` not idle), from the shell's poller (#200). A
    /// local fact: the last report is kept. With no report yet, a take never
    /// resumes A2DP.
    A2dpTransport { active: bool },
    /// `host` relayed its audio category (#272). The latest per host counts
    /// for automatic takes until the session ends. Its own variant: no other
    /// input carries a per-host fact.
    MediaInformation { host: String, category: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Command {
    /// Send OwnsConnection with this value.
    SendOwnership(bool),
    /// Announce Linux to the connected remote hosts.
    Announce,
    /// Tell the connected remote hosts whether Linux is in a call (#11).
    AdvertiseCall(bool),
    /// Relay Linux's audio category to the connected remote hosts (#272).
    /// Only with `with_category_relay(true)`.
    RelayCategory(u32),
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
    /// Drop playback samples taken before the new baseline, and re-baseline
    /// stream playback: a stream still in its debounce counts only after it
    /// stops and plays again (#90).
    DrainPlayback,
    /// Restore A2DP streaming after a take found the transport idle (#200):
    /// nudge the sink, kick the players, cycle the profile as a last resort.
    /// Runs alongside the core; a pause, a release or a dropped session
    /// stops it.
    ResumeA2dp,
    /// Make the AirPods the default sink, remembering the previous one
    /// (#40). Only with `with_default_sink_routing(true)`.
    RouteToAirPods,
    /// Give the default sink back to the one `RouteToAirPods` replaced, if
    /// the AirPods are still default (#40).
    RouteAway,
    /// Turn the AirPods card `off` while another host owns them and Linux
    /// holds no take, so PipeWire can't re-acquire A2DP; false turns it back
    /// to A2DP from `off` (#264). Only when `with_a2dp_hold` enables it.
    HoldA2dp(bool),
    /// Stop the probe.
    Exit,
    /// Stop the probe with this error.
    Fail(&'static str),
}

/// Where new playback goes while another host owns (#311); the shell routes
/// the changes to the default sink.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Park {
    /// Not parked: the default sink is the local one (or the AirPods').
    Local,
    /// Not parked since a take's ACK sent the parked playback to the
    /// AirPods.
    AirPods,
    /// The silent park sink is the default.
    Parked,
    /// No ACK within `PARK_TIMEOUT`: the local sink is the default until
    /// the next take's ACK or the hold's end.
    FallenBack,
}

/// Why an ownership request was sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cause {
    /// Automatic: playback started, or `Auto`'s "not owning".
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
    /// "Connect to this computer" (#16); see `auto_take_allowed`.
    connect_mode: ConnectMode,
    /// Linux was the last host to own the AirPods: set by an
    /// OwnsConnection=1 (report or snapshot), cleared when another host gets
    /// them (a yield, a remote request, remote media or call audio). Starts
    /// false and survives a disconnect, so it means "last connected".
    last_owner_linux: bool,
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
    /// sample arrives, stop edges treat playback as running, so a "stopped"
    /// first sample during the take defers its A2DP resume (#205). Takeover
    /// decisions still see no baseline.
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
    /// Last `Input::A2dpTransport` report; None until one arrives.
    a2dp_active: Option<bool>,
    /// The A2DP resume Linux's take owes, put off until Linux plays. Set by
    /// a take's ACK for a stop during the take (#205), and by #324's idle
    /// connect take (its ACK then skips the resume) or keep at owner [1].
    /// The next play edge, or the session's first playing sample, uses it:
    /// while Linux owns it resumes once (`resume_if_idle`); while the take
    /// is pending its ACK resumes instead. Any request, yield or disconnect
    /// clears it.
    resume_deferred: bool,
    recovery: CaseRecovery,
    /// The highest category of the plays that asked for the case reconnect
    /// (#421): a new source's own, else the aggregate. A withdrawal and the
    /// reconnect clear it.
    requested_category: Option<u32>,
    case_closed: bool,
    /// Playback stopped while a take was pending: its ACK defers the A2DP
    /// resume.
    stop_during_take: bool,
    /// Playback restarted while an automatic release was pending: take once
    /// it settles, deciding at the highest category requested (#96).
    play_during_release: Option<u32>,
    reconnecting: bool,
    retry_at: Instant,
    retry_delay: Duration,
    hosts: Vec<ConnectedDevice>,
    /// Hosts of the adopted `Reconnected` snapshot: replayed reports queued
    /// before it never announce them (#79). Cleared at `InitBacklogDrained`,
    /// `Auto` and on disconnect. Accepted limit: if the probe's echo timed
    /// out (cached owner [1], a real release, #290; automatic already on, so
    /// no boundary and no `Auto`) they stay exempt until the next
    /// disconnect, so a snapshot host that leaves and rejoins in that
    /// session is not re-announced (#150).
    /// A host outside the snapshot is always announced.
    snapshot_hosts: Vec<ConnectedDevice>,
    ears: Vec<EarDetectionStatus>,
    audio: Option<AudioSource>,
    owner: Option<Vec<u8>>,
    lines: Vec<String>,
    /// Emit `RouteToAirPods`/`RouteAway` (#40).
    default_sink_routing: bool,
    /// This take already sent `RouteAway`; the next take clears it, so a
    /// timed-out remote release's later loss sends no second one.
    routed_away: bool,
    /// Emit `HoldA2dp` (#264).
    a2dp_hold: bool,
    /// Another host got the AirPods while Linux held no take (#264). Set by
    /// the hold edges, cleared by an OwnsConnection=1 and a disconnect; a
    /// pending, refused or lost take keeps it.
    remote_owns: bool,
    /// The last `HoldA2dp` emitted; None until a session start emits one.
    held: Option<bool>,
    /// Report the park state (#311).
    park_sink: bool,
    park: Park,
    /// While parked: `PARK_TIMEOUT` after the last play edge.
    park_until: Option<Instant>,
    /// A worthy stream of the stream monitor's baseline is live (#311):
    /// playing for the park, never for a take.
    baseline_playing: bool,
    /// The stream monitor has a successful listing and runs (#311).
    streams_known: bool,
    /// The last playback sample was handled while `streams_known`: until
    /// then an idle `category` is a default, not a fact (#311).
    sampled_known: bool,
    /// Linux's audio category in the last playback sample since the
    /// baseline (#272). A local fact: a reset keeps it.
    category: u32,
    /// The category each host last relayed this session (#272).
    relays: HashMap<String, u32>,
    /// Emit `RelayCategory` (#272).
    category_relay: bool,
    /// End of `EAR_RESUME_GRACE` after a bud went back in while another host
    /// owned (#301). Only its deadline, a request, an OwnsConnection=1, a
    /// lost session or a disconnect end it; a stop sample doesn't, as the
    /// removal's stop may be polled after the reinsertion.
    ear_resume_until: Option<Instant>,
    /// Reinsertions not yet answered by an `EarResume` report (#313).
    ear_reports_due: u32,
    /// A `true` report came in for the live grace, so no `false` ends it
    /// (#313); a reinsertion with no live grace resets it.
    ear_resume_confirmed: bool,
    /// `Auto`'s ownership decision waits for the routing reports until
    /// then (#452); None once decided or ended.
    connect_until: Option<Instant>,
}

impl Handoff {
    /// `connected`: the control session is already up. Starting without one
    /// waits for new playback before reconnecting (buds in the case).
    pub(crate) fn new(local: String, now: Instant, automatic: bool, connected: bool) -> Self {
        Handoff {
            local,
            automatic,
            connect_mode: ConnectMode::default(),
            last_owner_linux: false,
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
            a2dp_active: None,
            resume_deferred: false,
            recovery: if connected {
                CaseRecovery::Immediate
            } else {
                CaseRecovery::WaitingForPlayback
            },
            requested_category: None,
            case_closed: false,
            stop_during_take: false,
            play_during_release: None,
            reconnecting: false,
            retry_at: now,
            retry_delay: FIRST_RETRY,
            hosts: Vec::new(),
            snapshot_hosts: Vec::new(),
            ears: Vec::new(),
            audio: None,
            owner: None,
            lines: Vec::new(),
            default_sink_routing: false,
            routed_away: false,
            a2dp_hold: false,
            remote_owns: false,
            held: None,
            park_sink: false,
            park: Park::Local,
            park_until: None,
            baseline_playing: false,
            streams_known: false,
            sampled_known: false,
            category: AUDIO_CATEGORY_IDLE,
            relays: HashMap::new(),
            category_relay: false,
            ear_resume_until: None,
            ear_reports_due: 0,
            ear_resume_confirmed: false,
            connect_until: None,
        }
    }

    /// Relay Linux's audio category to the other hosts in automatic mode
    /// (#272); off by default.
    pub(crate) fn with_category_relay(mut self, enabled: bool) -> Self {
        self.category_relay = enabled;
        self
    }

    /// Hold the AirPods card `off` while another host owns them (#264); off
    /// by default.
    pub(crate) fn with_a2dp_hold(mut self, enabled: bool) -> Self {
        self.a2dp_hold = enabled;
        self
    }

    /// Park new playback while the A2DP hold is on (#311), reported by
    /// `park`; off by default.
    pub(crate) fn with_park_sink(mut self, enabled: bool) -> Self {
        self.park_sink = enabled;
        self
    }

    /// Where new playback goes (#311); always `Local` without
    /// `with_park_sink`.
    pub(crate) fn park(&self) -> Park {
        self.park
    }

    /// The stream monitor's state, which the shell sets before each step
    /// (#311): whether it has a successful listing and runs (`known`), and
    /// whether a baseline stream is live (`baseline`), which the takeover
    /// signal leaves out (#90).
    pub(crate) fn set_streams(&mut self, known: bool, baseline: bool) {
        self.streams_known = known;
        self.baseline_playing = baseline;
    }

    /// Route the default sink to the AirPods on a take's ACK and away on
    /// losing the take (#40); off by default.
    pub(crate) fn with_default_sink_routing(mut self, enabled: bool) -> Self {
        self.default_sink_routing = enabled;
        self
    }

    /// Use `mode` for "Connect to this computer" (#16); the default is
    /// automatically.
    pub(crate) fn with_connect_mode(mut self, mode: ConnectMode) -> Self {
        self.connect_mode = mode;
        self
    }

    /// Start with Linux as the last owner or not: the shell keeps the value
    /// across a session's handoff tasks (#16).
    pub(crate) fn with_last_owner_linux(mut self, last_owner_linux: bool) -> Self {
        self.last_owner_linux = last_owner_linux;
        self
    }

    pub(crate) fn last_owner_linux(&self) -> bool {
        self.last_owner_linux
    }

    /// Start as a reconnect made for new playback (#307): the next
    /// `Input::Reconnected` takes through
    /// `TakeoverTrigger::ReconnectedForPlayback`.
    pub(crate) fn with_playback_requested(mut self) -> Self {
        self.recovery = CaseRecovery::PlaybackRequested;
        self
    }

    /// Start with Linux's audio category sampled before this core, for the
    /// reconnect take's category check (#307).
    pub(crate) fn with_category(mut self, category: u32) -> Self {
        self.category = category;
        self
    }

    /// Start with the category the reconnect's play asked at (#422), for
    /// `Input::Reconnected`'s take.
    pub(crate) fn with_requested_category(mut self, category: u32) -> Self {
        self.requested_category = Some(category);
        self
    }

    /// Both buds were last reported in the case (#307).
    pub(crate) fn case_closed(&self) -> bool {
        self.case_closed
    }

    #[cfg(test)]
    pub(crate) fn ear_reports_due(&self) -> u32 {
        self.ear_reports_due
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

    /// Reconnected took ownership, and no boundary, ACK or timeout has ended
    /// its stale-report window yet.
    pub(crate) fn reconnect_take_pending(&self) -> bool {
        self.reconnect_take_pending
    }

    /// Protocol lines (READY/AUTO/ACK/TIMEOUT/YIELD/RECONNECT...) produced
    /// since the last call, for the shell to print.
    pub(crate) fn take_lines(&mut self) -> Vec<String> {
        std::mem::take(&mut self.lines)
    }

    /// Feed one input at `now`; returns the commands to execute, in order.
    pub(crate) fn handle(&mut self, now: Instant, input: Input) -> Vec<Command> {
        let mut out = Vec::new();
        // An [1] in `Auto`'s wait keeps (#452).
        let owns = input == Input::Ownership(vec![1]);
        self.apply(now, input, &mut out);
        self.settle_connect(now, owns, &mut out);
        if !out
            .iter()
            .any(|c| matches!(c, Command::Exit | Command::Fail(_)))
        {
            self.finish_quit(now, &mut out);
        }
        self.update_park();
        out
    }

    /// Park once the hold is on and a sample with the streams known shows
    /// Linux plays nothing and has no call; end a park or a fallback when
    /// the hold ends (#311). A take's ACK has already unparked to the
    /// AirPods. A baseline stream, which can't take, or a dead stream
    /// monitor, which can't see one, falls a park back to the local output.
    fn update_park(&mut self) {
        let held = self.held == Some(true);
        let known = self.streams_known && self.sampled_known;
        let idle = known && self.category <= AUDIO_CATEGORY_IDLE && !self.local_call;
        self.park = match self.park {
            Park::Parked | Park::FallenBack if !held => Park::Local,
            Park::Parked if self.baseline_playing || !known => {
                self.say("PARK a stream playing since the baseline, or no stream monitor; new playback goes to the local output");
                Park::FallenBack
            }
            Park::Local | Park::AirPods
                if self.park_sink && held && idle && !self.baseline_playing =>
            {
                self.say("PARK new playback waits on the silent park sink while another host owns");
                Park::Parked
            }
            park => park,
        };
        if self.park != Park::Parked {
            self.park_until = None;
        }
    }

    fn say(&mut self, line: impl Into<String>) {
        self.lines.push(line.into());
    }

    fn apply(&mut self, now: Instant, input: Input, out: &mut Vec<Command>) {
        match input {
            Input::Playback {
                sampled_at,
                category,
                new_source,
            } => self.playback(now, sampled_at, category, new_source, out),
            Input::EarState(ears) => {
                let in_ear = |e: &[EarDetectionStatus]| {
                    e.iter()
                        .filter(|s| **s == EarDetectionStatus::InEar)
                        .count()
                };
                // A bud put back; an empty list is a first reading, not one.
                let reinserted = !self.ears.is_empty() && in_ear(&ears) > in_ear(&self.ears);
                // A fresh grace has no `true` report yet (#313).
                if reinserted && !self.ear_resume_until.is_some_and(|end| now < end) {
                    self.ear_resume_confirmed = false;
                }
                if reinserted && self.remote_owns && !self.requested_take {
                    self.ear_resume_until = Some(now + EAR_RESUME_GRACE);
                }
                // The shell reports on every reinsertion, whoever owns.
                self.ear_reports_due += u32::from(reinserted);
                self.case_closed = case_closed_after(&ears, self.case_closed);
                self.ears = ears;
            }
            Input::EarResume { resumed } => {
                // A report with no reinsertion due is stray: it changes nothing.
                if self.ear_reports_due > 0 {
                    self.ear_reports_due -= 1;
                    self.ear_resume_confirmed |= resumed;
                    if self.ear_reports_due == 0 && !self.ear_resume_confirmed {
                        self.ear_resume_until = None;
                    }
                }
            }
            Input::AudioSource(source) => {
                if self.remote_took(&source) {
                    self.last_owner_linux = false;
                    // While Linux holds a take, the [0] that follows yields.
                    if !self.requested_take {
                        self.hold_for_remote(out);
                    }
                }
                self.audio = Some(source);
            }
            Input::ConnectedDevices { old: _, new } => {
                // Compare against the hosts the core already knows, not the
                // event's `old`: a report queued before the session snapshot
                // (seed, Reconnected facts) is replayed with a stale `old`
                // and must not announce a second time (#58).
                // Reconnected snapshot hosts count as known until the boundary,
                // `Auto` or a disconnect (#79); only the probe's echo timeout
                // (cached owner [1], #290) leaves them known for the session.
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
                // The other host left: nothing to hold A2DP for (#264).
                if self.only_local_listed() {
                    self.remote_owns = false;
                }
                // A remote edge seen before its host was listed holds now (#296).
                self.sync_hold(false, out);
            }
            Input::Ownership(value) => self.ownership(now, value, out),
            Input::RemoteOwnershipRequest => {
                // Its own yield answers the session start (#452).
                self.connect_until = None;
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
                // user/quit release's. Linux's only playback release (Auto's)
                // is sent holding no take, so it has nothing to pause. A
                // remote release already paused when it was sent. A
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
                        && matches!(p.cause, Cause::User | Cause::Quit)
                    {
                        out.push(Command::PauseMedia);
                    }
                    // A remote request joining a release Linux sent while
                    // holding a take loses it (#40); `route_away` runs once
                    // per take.
                    p.cause = Cause::Remote;
                    if self.requested_take {
                        self.route_away(out);
                    }
                    self.play_during_release = None;
                    self.last_owner_linux = false;
                    self.say("REMOTE requested ownership; release already pending");
                    self.hold_for_remote(out);
                    return;
                }
                if self.pending.is_none()
                    && matches!(self.timed_out_release, Some((Cause::Remote, _)))
                {
                    // The release was never acknowledged: send it again. Its
                    // [0] is acknowledged like the first one, without a pause.
                    self.say("REMOTE requested ownership; re-sending the timed-out release");
                    self.request(now, false, Cause::Remote, out);
                    self.hold_for_remote(out);
                    return;
                }
                // Linux holds or is taking the AirPods for a call: keep them
                // whatever the other host plays (#11). A release Linux
                // already sent (above) is not taken back.
                if self.local_call && self.requested_take {
                    self.say("REMOTE requested ownership; refused during a local call");
                    return;
                }
                self.last_owner_linux = false;
                if self.automatic && self.requested_take {
                    out.push(Command::PauseMedia);
                }
                if self.requested_take {
                    self.route_away(out);
                }
                if self.owner.as_deref() == Some(&[0]) && !self.requested_take {
                    self.say("REMOTE requested ownership; Linux already yielded");
                    self.hold_for_remote(out);
                    return;
                }
                self.say("REMOTE requested ownership; yielding");
                self.request(now, false, Cause::Remote, out);
                self.hold_for_remote(out);
            }
            Input::SessionLost => {
                self.ear_resume_until = None;
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
                self.automatic = true;
                // A take Linux holds stays held: Linux is the idle owner (#262).
                self.reset_baseline(now);
                self.connect_until = Some(now + CONNECT_TAKE_WINDOW);
                let decision = self.connect_decision(now, false);
                // An idle other host (#324): Linux takes, so hold nothing.
                if decision == Some(true) {
                    self.remote_owns = false;
                }
                // A session start: restores a card an earlier run left `off`
                // (#264); before ConnectA2dp, which the hold blocks.
                self.sync_hold(true, out);
                out.push(Command::ConnectA2dp);
                if let Some(take) = decision {
                    self.decide_connect(now, take, out);
                }
                self.announce(out);
                out.push(Command::DrainPlayback);
                self.say("AUTO enabled; waiting for a new playback start after the baseline");
            }
            Input::Manual => {
                // Linux keeps the AirPods it holds, as the Mac keeps them
                // when its setting changes after a stop (#253, #255, #262).
                self.automatic = false;
                self.say("AUTO disabled");
            }
            Input::Take => {
                if self.pending.is_some() {
                    self.say("WAIT ownership request pending");
                } else {
                    match self.decide_take(TakeoverTrigger::Manual, self.category).action {
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
            Input::Quit => {
                self.quitting = true;
                self.sync_hold(false, out);
            }
            Input::LocalCall(in_call) => self.local_call(in_call, out),
            Input::Tick => self.tick(now, out),
            Input::A2dpTransport { active } => self.a2dp_active = Some(active),
            Input::MediaInformation { host, category } => {
                self.relays.insert(host, category);
            }
        }
    }

    fn playback(
        &mut self,
        now: Instant,
        sampled_at: Instant,
        category: u32,
        new_source: Option<u32>,
        out: &mut Vec<Command>,
    ) {
        if sampled_at < self.playback_baseline_at {
            return;
        }
        let playing = category > AUDIO_CATEGORY_IDLE;
        let previous = self.was_playing.replace(playing);
        let last_category = std::mem::replace(&mut self.category, category);
        self.sampled_known = self.streams_known;
        let assumed_playing = std::mem::take(&mut self.take_assumes_playing);
        // A rise while already playing (201 to 301) asks for a take like a
        // start (#272), unless Linux already holds one.
        let rose = previous == Some(true) && category > last_category && !self.requested_take;
        // A new source behind playback is a fresh request at its own
        // category, not the aggregate's (#96), also for case recovery (#421).
        let behind = new_source
            .filter(|_| previous == Some(true) && playing && !rose && !self.requested_take);
        let fresh_play = (previous == Some(false) && playing) || rose || behind.is_some();
        // Stop edges also count a take sent for playback before the
        // baseline: a stop during a reconnect take defers its resume (#205).
        let stopped =
            (previous == Some(true) || (previous.is_none() && assumed_playing)) && !playing;
        // A session's first playing sample also uses the deferral: Linux
        // already played when #324's take connected.
        let resume_deferred = (fresh_play || (playing && previous.is_none()))
            && std::mem::take(&mut self.resume_deferred);
        // Every play edge while parked, taken or skipped, gets its ACK or
        // falls back within `PARK_TIMEOUT` (#311).
        if self.park == Park::Parked && (fresh_play || (playing && previous.is_none())) {
            self.park_until = Some(now + PARK_TIMEOUT);
        }
        if !self.automatic {
            return;
        }
        if !self.connected {
            self.recovery.playback(fresh_play, stopped);
            if self.recovery == CaseRecovery::WaitingForPlayback {
                self.requested_category = None;
            }
            if fresh_play && self.recovery == CaseRecovery::PlaybackRequested {
                let requested = Some(behind.unwrap_or(category));
                self.requested_category = self.requested_category.max(requested);
                self.retry_at = now;
                self.say("AUTO new playback requested reconnection after case closure");
            }
            return;
        }
        if self.quitting || self.session_lost {
            return;
        }
        // A local call's 501 wins; its end relays the category (#272).
        if self.category_relay && category != last_category && !self.local_call {
            out.push(Command::RelayCategory(category));
        }
        // A release whose ACK timed out left the ownership unknown: retake.
        let owning = self.requested_take
            && self.owner.as_deref() == Some(&[1])
            && self.timed_out_release.is_none();
        match self.pending {
            // Linux stayed the idle owner (#262), as the Mac does: a fresh
            // take's host and wear policy is moot.
            None if owning && (fresh_play || resume_deferred) => {
                self.say("AUTO playback started; Linux already owns");
                if resume_deferred {
                    self.resume_if_idle(out);
                }
            }
            None => match self
                .decide_poll(
                    previous.map(|p| p && !fresh_play),
                    playing,
                    behind.unwrap_or(category),
                )
                .action
            {
                TakeoverAction::TakeOver if !self.auto_take_allowed() => {
                    self.say(CONNECT_MODE_SKIP)
                }
                TakeoverAction::TakeOver if self.ear_resumed(sampled_at) => self.say(EAR_RESUMED),
                TakeoverAction::TakeOver => {
                    self.request(now, true, Cause::Playback, out);
                    self.say("AUTO playback started; requested Linux ownership");
                }
                TakeoverAction::Skip(TakeoverSkip::InitialState | TakeoverSkip::NoTransition) => {
                    // Linux keeps the AirPods idle, relaying 100 like the
                    // Mac (#262); only a yield path gives them up.
                    if stopped && self.requested_take {
                        self.say("AUTO playback stopped; keeping ownership");
                    }
                }
                TakeoverAction::Skip(TakeoverSkip::RemoteCategoryHigher { local, remote }) => self
                    .say(format!(
                        "AUTO takeover skipped: the audio source's category {remote} is above Linux's {local}"
                    )),
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
                // The settled release must not retake for it either.
                if fresh_play && self.ear_resumed(sampled_at) {
                    self.say(EAR_RESUMED);
                } else if fresh_play {
                    let requested = Some(behind.unwrap_or(category));
                    self.play_during_release = self.play_during_release.max(requested);
                } else if stopped {
                    self.play_during_release = None;
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
    ///   or quit release (upstream #841); Auto's playback release is sent
    ///   holding no take, and a remote-request release already paused. A 0
    ///   within `LATE_ACK_WINDOW` of sending a release whose acknowledgement
    ///   timed out is that release's late acknowledgement and follows the
    ///   same rule, without a second reconcile (#55); a later 0 falls through
    ///   to the rule below (#62);
    /// - otherwise, while Linux holds or is taking the AirPods, another host
    ///   took them: `yield_take` yields once.
    fn ownership(&mut self, now: Instant, value: Vec<u8>, out: &mut Vec<Command>) {
        let acked = self
            .pending
            .filter(|p| value.as_slice() == [u8::from(p.desired)]);
        let was_linux = self.owner.as_deref() == Some(&[1]);
        self.owner = Some(value.clone());
        if value == [1] {
            self.last_owner_linux = true;
            // Before a take's RouteToAirPods and ResumeA2dp (#264).
            self.remote_owns = false;
            self.ear_resume_until = None;
            self.sync_hold(false, out);
        }
        if value == [0] && self.reconnect_take_pending {
            // A stale report queued while the reconnected session
            // initialized, not a loss of the pending take (which wants 1, so
            // this is not its acknowledgement): neither pause the playback
            // the take is for nor forget the take, which a stop during it
            // keeps (#262).
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
        if value == [0] {
            match acked.map(|p| p.cause).or(late_release) {
                // The acknowledgement of Linux's own release (#39). Auto's
                // playback release is sent holding no take, and
                // RemoteOwnershipRequest paused when it yielded, so neither
                // has anything to pause. A user or quit release may leave
                // Linux playing, so upstream #841's pause stays. A late
                // acknowledgement (#55) is handled the same way.
                Some(cause) => {
                    // A remote release handed the AirPods to the other host.
                    if cause == Cause::Remote {
                        self.last_owner_linux = false;
                    }
                    if self.requested_take
                        && self.automatic
                        && matches!(cause, Cause::User | Cause::Quit)
                    {
                        out.push(Command::PauseMedia);
                    }
                }
                // Another host took the AirPods from a take Linux made or
                // was making: yield once and forget the take.
                None if self.requested_take => {
                    self.yield_take(out);
                    return;
                }
                // Another host took the AirPods without asking.
                None if was_linux => {
                    self.last_owner_linux = false;
                    self.hold_for_remote(out);
                }
                None => {}
            }
            self.requested_take = false;
        }
        let Some(pending) = acked else {
            if late_release.is_some() {
                // The timeout already reconciled; nothing more to decide.
                self.say("ACK owner=not-linux late (after TIMEOUT); release confirmed");
                self.hold_if_remote_source(out);
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
            out.push(Command::Exit);
            return;
        }
        self.reconcile(now, out);
        self.hold_if_remote_source(out);
        // The take holds unless the reconcile just released it.
        let held = pending.desired && self.live() && self.requested_take && self.pending.is_none();
        // Parked playback follows the default to the AirPods (#311).
        if held && matches!(self.park, Park::Parked | Park::FallenBack) {
            self.park = Park::AirPods;
        }
        // Before the resume, so its audible check finds the stream on the
        // AirPods sink (#40).
        if held && self.default_sink_routing {
            out.push(Command::RouteToAirPods);
        }
        // A stop during the take (#205), or an idle connect take (#324),
        // defers the resume until Linux plays.
        if held && self.automatic && !self.resume_deferred {
            self.resume_if_idle(out);
        }
    }

    /// A take that found the transport idle gets its audio back (#200).
    /// Not during a call: the card is on HFP, so the idle A2DP transport
    /// says nothing.
    fn resume_if_idle(&mut self, out: &mut Vec<Command>) {
        if !self.local_call && self.a2dp_active == Some(false) {
            out.push(Command::ResumeA2dp);
            self.say("A2DP transport idle after the take; resuming it");
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
        self.last_owner_linux = false;
        self.stop_during_take = false;
        self.play_during_release = None;
        self.resume_deferred = false;
        if self.automatic {
            out.push(Command::PauseMedia);
        }
        self.route_away(out);
        self.hold_for_remote(out);
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

    /// `RouteAway`, once per take (#40).
    fn route_away(&mut self, out: &mut Vec<Command>) {
        if self.default_sink_routing && !std::mem::replace(&mut self.routed_away, true) {
            out.push(Command::RouteAway);
        }
    }

    /// Another host got the AirPods and Linux holds no take: hold A2DP
    /// (#264).
    fn hold_for_remote(&mut self, out: &mut Vec<Command>) {
        self.remote_owns = true;
        self.sync_hold(false, out);
    }

    /// A remote source seen while Linux's release was pending (the
    /// `AudioSource` arm skips it then) holds once the release settled and
    /// Linux has no take (#264).
    fn hold_if_remote_source(&mut self, out: &mut Vec<Command>) {
        if !self.requested_take && self.audio.as_ref().is_some_and(|a| self.remote_took(a)) {
            self.hold_for_remote(out);
        }
    }

    /// Emit the hold the facts ask for when it differs from the last one
    /// emitted. Before a session start has emitted one, only `start` does,
    /// so the start also restores a card an earlier run left `off`.
    fn sync_hold(&mut self, start: bool, out: &mut Vec<Command>) {
        let want = self.remote_owns && !self.quitting && !self.only_local_listed();
        // No new hold after SessionLost (#287); Disconnected releases an earlier one.
        if want && !self.live() {
            return;
        }
        if self.a2dp_hold && (start || self.held.is_some()) && self.held != Some(want) {
            self.held = Some(want);
            out.push(Command::HoldA2dp(want));
        }
    }

    /// The AirPods list only Linux: no other host to hold A2DP for. An empty
    /// list is no evidence of that.
    fn only_local_listed(&self) -> bool {
        !self.hosts.is_empty() && self.hosts.iter().all(|d| d.mac == self.local)
    }

    /// A play sampled at `sampled_at` is ear detection's resume while another
    /// host still owns (#301).
    fn ear_resumed(&self, sampled_at: Instant) -> bool {
        self.remote_owns && self.ear_resume_until.is_some_and(|end| sampled_at < end)
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
            // Linux keeps the AirPods idle (#262); a held take's ACK defers
            // its A2DP resume to the next play edge (#205).
            self.resume_deferred = true;
            self.say("AUTO playback stopped during the take; keeping ownership");
        } else if let Some(category) = play.filter(|_| self.was_playing == Some(true)) {
            // The stopped-to-playing edge was seen while the release was in
            // flight; decide on it now.
            if self.decide_poll(Some(false), true, category).action == TakeoverAction::TakeOver {
                if !self.auto_take_allowed() {
                    self.say(CONNECT_MODE_SKIP);
                    return;
                }
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
        // The next session start emits the hold again.
        self.remote_owns = false;
        self.ear_resume_until = None;
        self.sync_hold(false, out);
        self.held = None;
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
        self.play_during_release = None;
        self.resume_deferred = false;
        self.hosts.clear();
        self.snapshot_hosts.clear();
        self.relays.clear();
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
        // Relays belong to the session that carried them.
        self.relays.clear();
        self.ears = facts.ears;
        self.audio = facts.audio;
        self.owner = facts.owner;
        if self.audio.as_ref().is_some_and(|a| self.remote_took(a)) {
            self.last_owner_linux = false;
            self.remote_owns = true;
        }
        if self.owner.as_deref() == Some(&[1]) {
            self.last_owner_linux = true;
            self.remote_owns = false;
        }
        self.sync_hold(true, out);
        if self.automatic {
            self.announce(out);
        }
        // The take asks at the play that requested it, capped at the
        // aggregate (#421); a #307 core carries none.
        let category = self
            .requested_category
            .take()
            .map_or(self.category, |c| c.min(self.category));
        let took = match self
            .decide_take(TakeoverTrigger::ReconnectedForPlayback, category)
            .action
        {
            // Like an ordinary reconnect: no take, nothing pending.
            TakeoverAction::TakeOver if !self.auto_take_allowed() => {
                self.say(CONNECT_MODE_SKIP);
                false
            }
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
        // An ordinary reconnect takes from an idle other host (#324).
        let connect_take = !took && self.automatic && self.connect_take_allowed();
        if connect_take {
            self.connect_take(now, out);
        }
        self.recovery = CaseRecovery::Immediate;
        self.reset_baseline(now);
        // The take was for playback seen before this baseline: a first
        // sample that finds it stopped is a stop edge: the take is kept
        // (#262) and, while still pending, defers its resume (#205).
        self.take_assumes_playing = took;
        // The session's queued OwnsConnection=0 reports predate this take.
        self.reconnect_take_pending = took || connect_take;
        out.push(Command::DrainPlayback);
        self.retry_delay = FIRST_RETRY;
        self.say("RECONNECTED with fresh routing and wear state");
    }

    fn tick(&mut self, now: Instant, out: &mut Vec<Command>) {
        if self.park == Park::Parked && self.park_until.is_some_and(|end| now >= end) {
            self.park = Park::FallenBack;
            self.say("PARK no ACK within the park timeout; new playback goes to the local output");
        }
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
            self.routed_away = false;
        }
        self.pending = Some(Pending {
            desired,
            since: now,
            cause,
        });
        self.stop_during_take = false;
        self.play_during_release = None;
        self.reconnect_take_pending = false;
        self.timed_out_release = None;
        self.resume_deferred = false;
        self.ear_resume_until = None;
        out.push(Command::SendOwnership(desired));
    }

    fn reset_baseline(&mut self, now: Instant) {
        self.was_playing = None;
        self.take_assumes_playing = false;
        self.playback_baseline_at = now;
    }

    /// Announce Linux to the remote hosts. The announcement carries the idle
    /// media information, so during a call the call is advertised again
    /// after it (#11), and otherwise a relayed category is re-sent (#299).
    fn announce(&self, out: &mut Vec<Command>) {
        out.push(Command::Announce);
        if self.local_call {
            out.push(Command::AdvertiseCall(true));
        } else if self.category_relay && self.category != AUDIO_CATEGORY_IDLE {
            out.push(Command::RelayCategory(self.category));
        }
    }

    /// The call monitor reported Linux's call state (#11).
    fn local_call(&mut self, in_call: bool, out: &mut Vec<Command>) {
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
        // The call's 501 replaced the category the other hosts had (#272).
        // Linux keeps the AirPods it holds, as after any stop (#262).
        if self.category_relay && self.category != AUDIO_CATEGORY_IDLE {
            out.push(Command::RelayCategory(self.category));
        }
    }

    fn routing(&self) -> RoutingSnapshot {
        routing_snapshot(
            self.live(),
            &self.hosts,
            &self.ears,
            self.audio.as_ref(),
            &self.relays,
            self.category,
            &self.local,
        )
    }

    /// A playback poll's decision, Linux asking at `category` (#96).
    fn decide_poll(
        &self,
        previous: Option<bool>,
        playing: bool,
        category: u32,
    ) -> TakeoverDecision {
        let routing = RoutingSnapshot {
            local_category: category,
            ..self.routing()
        };
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

    /// "Connect to this computer" (#16): when last connected, an automatic
    /// take (`Cause::Playback`) needs Linux to be the last owner. A mode gate
    /// like `automatic`, checked once `decide_takeover` allowed the take; it
    /// is not a routing guard, so the takeover policy stays unchanged. A
    /// manual take is never gated.
    fn auto_take_allowed(&self) -> bool {
        self.connect_mode == ConnectMode::Automatically || self.last_owner_linux
    }

    /// Another host plays media or is in a call: it has the AirPods, unless a
    /// local call holds them (it then refuses the request that follows).
    fn remote_took(&self, source: &AudioSource) -> bool {
        use crate::bluetooth::aacp::AudioSourceType::{Call, Media};
        source.mac != self.local
            && matches!(source.r#type, Media | Call)
            && !(self.local_call && self.requested_take)
    }

    /// A take's decision, Linux asking at `local_category`.
    fn decide_take(&self, trigger: TakeoverTrigger, local_category: u32) -> TakeoverDecision {
        let routing = RoutingSnapshot {
            local_category,
            ..self.routing()
        };
        decide_takeover(TakeoverInputs::handoff(trigger, routing, self.recovery))
    }

    /// A session start or reconnect may take from an idle other host, as the
    /// Mac does (#324, spike #325): no take held or pending, the connect
    /// mode allows it, and the policy allows it with Linux asking at 100.
    fn connect_take_allowed(&self) -> bool {
        !self.requested_take
            && self.pending.is_none()
            && self.auto_take_allowed()
            && self
                .decide_take(TakeoverTrigger::Connected, AUDIO_CATEGORY_IDLE)
                .action
                == TakeoverAction::TakeOver
    }

    /// The connect take: only "owning" is sent. It is idle, so it stays
    /// (#262) and its A2DP resume waits until Linux plays.
    fn connect_take(&mut self, now: Instant, out: &mut Vec<Command>) {
        self.request(now, true, Cause::Playback, out);
        self.resume_deferred = true;
        self.say(CONNECT_TAKE);
    }

    /// `Auto`'s one ownership decision (#324, #452): Some(true) takes, or
    /// keeps an owner [1]; Some(false) releases; None waits for the routing
    /// reports. An [1] reported in the wait (`owns`) keeps; past the
    /// deadline, or when no report can allow the take, it releases.
    fn connect_decision(&self, now: Instant, owns: bool) -> Option<bool> {
        use crate::bluetooth::aacp::AudioSourceType::Media;
        let expired = now >= self.connect_until?;
        let gated = !self.automatic
            || self.requested_take
            || self.pending.is_some()
            || !self.auto_take_allowed();
        // A remote media source needs its host's relay (#272).
        let complete = !self.hosts.is_empty()
            && !self.ears.is_empty()
            && self.audio.as_ref().is_some_and(|a| {
                a.mac == self.local || a.r#type != Media || self.relays.contains_key(&a.mac)
            });
        if !expired && !gated && self.connect_take_allowed() {
            Some(true)
        } else if expired || gated || complete {
            Some(false)
        } else if owns {
            Some(true)
        } else {
            None
        }
    }

    /// End `Auto`'s wait: a lost session or quit sends nothing; otherwise
    /// decide once the routing reports allow it (#452).
    fn settle_connect(&mut self, now: Instant, owns: bool, out: &mut Vec<Command>) {
        if !self.live() || self.quitting {
            self.connect_until = None;
        }
        if let Some(take) = self.connect_decision(now, owns) {
            self.decide_connect(now, take, out);
        }
    }

    fn decide_connect(&mut self, now: Instant, take: bool, out: &mut Vec<Command>) {
        self.connect_until = None;
        // Linux played in the wait (#452): no later first sample resumes.
        let played = self.was_playing == Some(true);
        if take && self.owner.as_deref() == Some(&[1]) {
            // The AirPods already report Linux (an app restart while it
            // owned): keep them as an idle take (#324).
            self.requested_take = true;
            self.last_owner_linux = true;
            self.routed_away = false;
            self.resume_deferred = !played;
            if self.default_sink_routing {
                out.push(Command::RouteToAirPods);
            }
            if played {
                self.resume_if_idle(out);
            }
            self.say("AUTO connected; Linux already owns the AirPods, keeping them");
        } else if take {
            self.connect_take(now, out);
            // Its ACK resumes.
            self.resume_deferred = !played;
        } else if !self.requested_take && self.pending.is_none() {
            // Tracked like an automatic release (#54) only when Linux
            // owns: the AirPods reply only to a real change (#159, #289). A
            // release already pending covers it: a second "not owning"
            // would get a reply that acknowledges nothing.
            self.request(now, false, Cause::Playback, out);
            if self.owner.as_deref() != Some(&[1]) {
                self.pending = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Command::*;
    use super::*;
    use crate::a2dp_resume::A2DP_RESUME_WINDOW;
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

    /// `ready_facts` with both buds out of ear: the connect take (#324)
    /// needs a bud in, and the state is complete, so `Auto` sends its "not
    /// owning" at once (#452) and a reconnect takes nothing.
    /// `Probe::wear_in` then puts them in.
    fn unworn_facts() -> SessionFacts {
        SessionFacts {
            ears: vec![OutOfEar, OutOfEar],
            ..ready_facts()
        }
    }

    /// A core driven with millisecond offsets from a fixed start, never
    /// rewound (#383).
    struct Probe {
        core: Handoff,
        t0: Instant,
        last_ms: u64,
    }

    impl Probe {
        fn new(automatic: bool, connected: bool) -> Self {
            let t0 = Instant::now();
            Probe {
                core: Handoff::new(LOCAL.to_string(), t0, automatic, connected),
                t0,
                last_ms: 0,
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

        /// Both buds go in at `ms` after a session start made with
        /// `unworn_facts`, and the shell's ear resume played nothing (#313):
        /// no grace is left, and nothing is sent.
        fn wear_in(&mut self, ms: u64) {
            assert_eq!(self.feed(ms, Input::EarState(vec![InEar, InEar])), []);
            assert_eq!(self.feed(ms, Input::EarResume { resumed: false }), []);
        }

        fn feed(&mut self, ms: u64, input: Input) -> Vec<Command> {
            assert!(ms >= self.last_ms, "clock rewound: {ms} < {}", self.last_ms);
            self.last_ms = ms;
            self.core.handle(self.at(ms), input)
        }

        /// Ticks every 100 ms from `ms` through `ms` + 60 s send nothing:
        /// Linux stays the idle owner (#262).
        fn idle_ticks(&mut self, ms: u64) {
            for ms in (ms..=ms + 60_000).step_by(100) {
                assert_eq!(self.feed(ms, Input::Tick), [], "{ms}");
            }
        }

        /// A sample at `ms`; `playing` is a counted stream (201).
        fn play(&mut self, ms: u64, playing: bool) -> Vec<Command> {
            self.play_category(ms, if playing { STREAM } else { IDLE })
        }

        /// A stopped sample taken at `sampled_ms` but fed at `ms`.
        fn stale_stop(&mut self, ms: u64, sampled_ms: u64) -> Vec<Command> {
            let sampled_at = self.at(sampled_ms);
            self.feed(
                ms,
                Input::Playback {
                    sampled_at,
                    category: IDLE,
                    new_source: None,
                },
            )
        }

        fn play_category(&mut self, ms: u64, category: u32) -> Vec<Command> {
            self.play_source(ms, category, None)
        }

        /// A sample at `ms` that also reports a new local source (#96).
        fn play_source(&mut self, ms: u64, category: u32, new_source: Option<u32>) -> Vec<Command> {
            let sampled_at = self.at(ms);
            self.feed(
                ms,
                Input::Playback {
                    sampled_at,
                    category,
                    new_source,
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
    #[should_panic(expected = "clock rewound")]
    fn probe_clock_never_rewinds() {
        let mut p = Probe::ready();
        p.feed(500, Input::Tick);
        p.feed(400, Input::Tick);
    }

    #[test]
    fn takes_on_new_playback_when_worn() {
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(1000, true), []);
        // Stopping keeps the AirPods: Linux is the idle owner (#262).
        assert_eq!(p.play(1500, false), []);
        p.idle_ticks(1500);
        assert_eq!(p.play(62_000, false), []);
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
        // The other host is idle and a bud is in: the connect take (#324).
        assert_eq!(
            p.feed(2300, Input::Reconnected(ready_facts())),
            [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
        );
    }

    #[test]
    fn with_playback_requested_takes_on_reconnected() {
        let mut p = Probe::new(true, false);
        p.core = p.core.with_playback_requested();
        assert_eq!(
            p.feed(100, Input::Reconnected(ready_facts())),
            [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
        );
        assert!(
            p.core
                .take_lines()
                .contains(&"AUTO reconnected for new playback; requested Linux ownership".into())
        );
        // Without it, the same reconnect takes nothing.
        let mut p = Probe::new(true, false);
        assert_eq!(
            p.feed(100, Input::Reconnected(ready_facts())),
            [CancelReconnect]
        );
    }

    #[test]
    fn with_category_takes_over_remote_media_on_reconnected() {
        let remote_media = || SessionFacts {
            audio: Some(source(REMOTE, AudioSourceType::Media)),
            ..ready_facts()
        };
        let mut p = Probe::new(true, false);
        p.core = p
            .core
            .with_playback_requested()
            .with_category(crate::media_controller::AUDIO_CATEGORY_MEDIA);
        assert_eq!(
            p.feed(100, Input::Reconnected(remote_media())),
            [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
        );
        // Without it, Linux's idle category is below the remote media's.
        let mut p = Probe::new(true, false);
        p.core = p.core.with_playback_requested();
        assert!(
            !p.feed(100, Input::Reconnected(remote_media()))
                .contains(&SendOwnership(true))
        );
    }

    #[test]
    fn case_closed_follows_wear() {
        let mut p = Probe::ready();
        assert!(!p.core.case_closed());
        p.feed(20, Input::EarState(vec![InCase, InCase]));
        assert!(p.core.case_closed());
        p.feed(30, Input::EarState(vec![InEar, InCase]));
        assert!(!p.core.case_closed());
    }

    #[test]
    fn baseline_ignores_preexisting_playback() {
        // Auto mode starts while something is already playing. Buds out at
        // the start, so no connect take (#324) hides the playback take.
        let mut p = Probe::new(false, true);
        p.routing(unworn_facts());
        assert_eq!(
            p.feed(1000, Input::Auto),
            [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
        );
        p.wear_in(1000);
        // The reply to Auto's "not owning" (#54).
        assert_eq!(p.feed(1050, Input::Ownership(vec![0])), []);
        // A stopped sample from before the baseline is not an observation.
        assert_eq!(p.stale_stop(1050, 900), []);
        assert_eq!(p.play(1100, true), []);
        assert_eq!(p.play(1600, true), []);
        assert_eq!(p.play(2100, false), []);
        assert_eq!(p.play(2600, true), [SendOwnership(true)]);
        assert_eq!(p.feed(2700, Input::Ownership(vec![1])), []);

        // The same after a reconnect.
        assert_eq!(p.feed(3000, Input::Disconnected), [PauseMedia, DropSession]);
        assert_eq!(p.feed(4000, Input::Tick), [Reconnect { audio: true }]);
        assert_eq!(
            p.feed(5000, Input::Reconnected(unworn_facts())),
            [AdoptSession, Announce, DrainPlayback]
        );
        p.wear_in(5000);
        assert_eq!(p.stale_stop(5000, 4900), []);
        assert_eq!(p.play(5100, true), []);
        assert_eq!(p.play(5600, true), []);
        assert_eq!(p.play(6100, false), []);
        assert_eq!(p.play(6600, true), [SendOwnership(true)]);
    }

    #[test]
    fn stop_during_pending_take_keeps_ownership_after_ack() {
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        // Playback stops while the take waits for its acknowledgement.
        assert_eq!(p.play(1000, false), []);
        // The take holds (#262).
        assert_eq!(p.feed(1100, Input::Ownership(vec![1])), []);
        for ms in [1500, 2000, 2500] {
            assert_eq!(p.play(ms, false), []);
        }
        p.idle_ticks(2500);
    }

    #[test]
    fn stop_during_pending_take_keeps_ownership_after_timeout() {
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.play(1000, false), []);
        assert_eq!(p.feed(5500, Input::Tick), []);
        // The timeout releases nothing either (#262).
        assert_eq!(p.feed(5600, Input::Tick), []);
        p.idle_ticks(5600);
        assert_eq!(p.play(66_000, false), []);
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

    /// Linux owns the AirPods without a take of its own, so `Auto` at 100 ms
    /// sends a tracked "not owning" (#54); a stopped baseline sample at 200.
    /// The buds go in only after `Auto`, so the connect take (#324) doesn't
    /// keep the ownership instead.
    fn auto_release() -> Probe {
        let mut p = Probe::new(false, true);
        p.routing(SessionFacts {
            owner: Some(vec![1]),
            ..unworn_facts()
        });
        assert_eq!(
            p.feed(100, Input::Auto),
            [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
        );
        p.wear_in(150);
        assert_eq!(p.play(200, false), []);
        p
    }

    #[test]
    fn restart_during_pending_release_takes_after_ack() {
        // Playback restarts before Auto's release is acknowledged: no pause,
        // and the AirPods are taken back.
        let mut p = auto_release();
        assert_eq!(p.play(1500, true), []);
        assert_eq!(
            p.feed(1600, Input::Ownership(vec![0])),
            [SendOwnership(true)]
        );
        assert_eq!(p.feed(1700, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(2000, true), []);

        // Stopped again before the acknowledgement: plain release.
        let mut p = auto_release();
        assert_eq!(p.play(1500, true), []);
        assert_eq!(p.play(2000, false), []);
        // #39: the release's ACK pauses nothing.
        assert_eq!(p.feed(2100, Input::Ownership(vec![0])), []);
        assert_eq!(p.play(2500, false), []);
    }

    #[test]
    fn stop_after_reconnect_take_keeps_ownership() {
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
        // The take holds (#262).
        assert_eq!(settled, []);
        for ms in [2500, 3000, 3500] {
            assert_eq!(p.play(ms, false), []);
        }
        p.idle_ticks(3500);

        // The first post-baseline sample arriving only after the ACK keeps
        // them too.
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
        assert_eq!(p.play(1500, false), []);
        for ms in [2000, 2500, 3000] {
            assert_eq!(p.play(ms, false), []);
        }
        p.idle_ticks(3000);

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
        // A later stop keeps them too (#262).
        assert_eq!(p.play(9500, false), []);
        p.idle_ticks(9500);
    }

    #[test]
    fn restart_during_pending_release_blocked_does_not_retake() {
        // A remote call starts while Auto's release is in flight. Linux held
        // no take, so its ACK pauses nothing either.
        let mut p = auto_release();
        assert_eq!(p.play(1500, true), []);
        assert_eq!(
            p.feed(
                1550,
                Input::AudioSource(source(REMOTE, AudioSourceType::Call))
            ),
            []
        );
        assert_eq!(p.feed(1600, Input::Ownership(vec![0])), []);
        for ms in [2000, 2500, 3000] {
            assert_eq!(p.play(ms, true), []);
        }
        assert_eq!(p.feed(9000, Input::Tick), []);

        // The buds come out while the release is in flight.
        let mut p = auto_release();
        assert_eq!(p.play(1500, true), []);
        assert_eq!(p.feed(1550, Input::EarState(vec![OutOfEar, InCase])), []);
        assert_eq!(p.feed(1600, Input::Ownership(vec![0])), []);
        assert_eq!(p.play(2000, true), []);
    }

    #[test]
    fn session_loss_before_release_ack_does_not_retake() {
        // As in restart_during_pending_release_takes_after_ack: Auto's
        // release is pending and playback restarted during it.
        let mut p = auto_release();
        assert_eq!(p.play(1500, true), []);
        p.core.take_lines();
        // The buds go in the case and the link drops. run() finds the sender
        // gone with the wear report and the release ACK still queued: it
        // marks the session lost, feeds the queued events, then disconnects.
        assert_eq!(p.feed(1600, Input::SessionLost), []);
        assert_eq!(p.feed(1610, Input::EarState(vec![InCase, InCase])), []);
        // No retake on the dead session, and no take to pause for.
        assert_eq!(p.feed(1620, Input::Ownership(vec![0])), []);
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
        // The stop during the take is not lost: the take settles and Linux
        // keeps it (#262).
        assert_eq!(p.play(1500, false), []);
        let mut settled = p.feed(1600, Input::Ownership(vec![1]));
        settled.extend(p.play(2000, false));
        assert_eq!(settled, []);
        for ms in [2500, 3000, 3500] {
            assert_eq!(p.play(ms, false), []);
        }
        p.idle_ticks(3500);

        // Playback keeps running: Linux keeps the AirPods, nothing pauses,
        // and a later stop keeps them too.
        let mut p = reconnect_take();
        assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
        assert_eq!(p.play(1500, true), []);
        assert_eq!(p.feed(1600, Input::Ownership(vec![1])), []);
        for ms in [2000, 2500, 3000] {
            assert_eq!(p.play(ms, true), []);
        }
        assert_eq!(p.feed(9000, Input::Tick), []);
        assert_eq!(p.play(9500, false), []);
        p.idle_ticks(9500);

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
        // retaken, with no yield or pause. Owner [1]: the only case where
        // the AirPods reply (#289). Buds in only after Auto: no connect take.
        let mut p = Probe::new(false, true);
        p.routing(SessionFacts {
            owner: Some(vec![1]),
            ..unworn_facts()
        });
        assert_eq!(
            p.feed(100, Input::Auto),
            [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
        );
        p.wear_in(150);
        p.core.take_lines();
        let mut all = p.play(200, false);
        all.extend(p.play(300, true));
        all.extend(p.feed(400, Input::Ownership(vec![0])));
        assert_eq!(all, [SendOwnership(true)]);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
        assert_eq!(p.feed(500, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(600, false), []);
        p.idle_ticks(600);
    }

    #[test]
    fn repeated_auto_reply_never_yields() {
        // A second Auto while the first one's release is pending sends no
        // second "not owning", so no reply is left acknowledging nothing.
        // Owner [1]: the only case where the AirPods reply (#289). Buds in
        // only after the Autos: no connect take.
        let mut p = Probe::new(false, true);
        p.routing(SessionFacts {
            owner: Some(vec![1]),
            ..unworn_facts()
        });
        let mut all = p.feed(100, Input::Auto);
        all.extend(p.feed(150, Input::Auto));
        p.wear_in(160);
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
        assert_eq!(p.play(600, false), []);
        p.idle_ticks(600);
    }

    #[test]
    fn auto_tracked_when_linux_owner_acks() {
        // Owner [1]: the AirPods reply to Auto's "not owning", and that [0]
        // acknowledges it rather than being a loss (#54). Buds out: no
        // connect take.
        let mut p = Probe::new(false, true);
        p.routing(SessionFacts {
            owner: Some(vec![1]),
            ..unworn_facts()
        });
        assert_eq!(
            p.feed(100, Input::Auto),
            [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
        );
        assert!(p.core.pending.is_some());
        p.core.take_lines();
        assert_eq!(p.feed(200, Input::Ownership(vec![0])), []);
        let lines = p.core.take_lines();
        assert_eq!(count_lines(&lines, "ACK owner=not-linux"), 1);
        assert_eq!(count_lines(&lines, "YIELD"), 0);
        assert!(p.core.pending.is_none());
    }

    /// A manual, live session with `facts` gets Auto at 100 ms: owner [0] or
    /// unknown, so its "not owning" is untracked (#289). The buds go in only
    /// after it, so the connect take (#324) doesn't replace that release.
    fn untracked_auto(facts: SessionFacts) -> Probe {
        let mut p = Probe::new(false, true);
        p.routing(SessionFacts {
            ears: unworn_facts().ears,
            ..facts
        });
        assert_eq!(
            p.feed(100, Input::Auto),
            [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
        );
        p.wear_in(100);
        assert!(p.core.pending.is_none());
        p.core.take_lines();
        p
    }

    #[test]
    fn auto_untracked_when_not_owner_waits_for_nothing() {
        // Owner [0]: the AirPods never reply to this "not owning" (#159).
        let tick = 100 + ACK_TIMEOUT.as_millis() as u64 + 1;
        let mut p = untracked_auto(ready_facts());
        assert_eq!(p.feed(tick, Input::Tick), []);
        assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 0);
        assert_eq!(p.core.timed_out_release, None);
        // From the same point, 1 s after Auto: (a) a play edge takes in
        // that step;
        let mut p = untracked_auto(ready_facts());
        assert_eq!(p.play(200, false), []);
        assert_eq!(p.play(1100, true), [SendOwnership(true)]);
        // (b) a take is not answered with WAIT;
        let mut p = untracked_auto(ready_facts());
        assert_eq!(p.feed(1100, Input::Take), [SendOwnership(true)]);
        assert_eq!(count_lines(&p.core.take_lines(), "WAIT"), 0);
        // (c) a quit exits without a Fail;
        let mut p = untracked_auto(ready_facts());
        assert_eq!(p.feed(1100, Input::Quit), [Exit]);
        // (d) a remote request has no release to join.
        let mut p = untracked_auto(ready_facts());
        assert_eq!(p.feed(1100, Input::RemoteOwnershipRequest), []);
        let lines = p.core.take_lines();
        assert_eq!(
            count_lines(
                &lines,
                "REMOTE requested ownership; release already pending"
            ),
            0
        );
    }

    #[test]
    fn auto_untracked_with_unknown_owner() {
        // The usual app case: the AirPods' owner report arrives after Auto.
        let unknown = || SessionFacts {
            owner: None,
            ..ready_facts()
        };
        let tick = 100 + ACK_TIMEOUT.as_millis() as u64 + 1;
        let mut p = untracked_auto(unknown());
        assert_eq!(p.feed(tick, Input::Tick), []);
        assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 0);
        let mut p = untracked_auto(unknown());
        assert_eq!(p.play(200, false), []);
        assert_eq!(p.play(1100, true), [SendOwnership(true)]);
        // That report, a [0] with nothing pending, is no loss and leaves the
        // last owner alone: only an unsolicited [0] after [1] clears it (#16).
        let mut p = untracked_auto(unknown());
        p.core.last_owner_linux = true;
        assert_eq!(p.feed(200, Input::Ownership(vec![0])), []);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
        assert!(p.core.pending.is_none());
        assert!(p.core.last_owner_linux);
        // Accepted residual: if Linux did own, a take sent before that [0]
        // arrives yields to it.
        let mut p = untracked_auto(unknown());
        assert_eq!(p.play(200, false), []);
        assert_eq!(p.play(300, true), [SendOwnership(true)]);
        assert_eq!(p.feed(400, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
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
        // (a) The ACK of Auto's release.
        let mut p = auto_release();
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
        // not forgotten, so Linux keeps it after a stop and a later [0] is
        // its loss.
        let mut p = reconnect_take();
        p.core.take_lines();
        assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
        assert_eq!(p.feed(1600, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(2000, false), []);
        p.idle_ticks(2000);
        assert_eq!(p.feed(62_100, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
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
        // Auto's release, sent at 100 ms.
        let mut p = auto_release();
        p.core.take_lines();
        let tick = p.feed(5200, Input::Tick);
        assert_eq!(tick, []);
        assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);
        assert_eq!(p.feed(5300, Input::Ownership(vec![0])), []);
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

    /// A user release at 1500 ms of an automatic take, whose ACK timed out
    /// at 6600 ms.
    fn user_release_timed_out() -> Probe {
        let mut p = acked_auto_take();
        p.core.take_lines();
        assert_eq!(p.feed(1500, Input::Release), [SendOwnership(false)]);
        assert_eq!(p.feed(6600, Input::Tick), []);
        assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);
        p
    }

    #[test]
    fn release_timeout_then_loss_after_window_yields() {
        // Long after the release's late-ACK window, a 0 is another host
        // taking the AirPods (#62): one YIELD and one pause.
        let mut p = user_release_timed_out();
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

        // (a) Exactly at the window's end: the late acknowledgement, with
        // the user release's pause.
        let mut p = user_release_timed_out();
        assert_eq!(p.feed(edge, Input::Ownership(vec![0])), [PauseMedia]);
        let lines = p.core.take_lines();
        assert_eq!(count_lines(&lines, "ACK owner=not-linux late"), 1);
        assert_eq!(count_lines(&lines, "YIELD"), 0);
        assert_eq!(p.core.timed_out_release, None);

        // (b) One millisecond later: a genuine loss.
        let mut p = user_release_timed_out();
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
        // #137: a remote request during Auto's release (a Cause::Playback
        // one) joins it; the join itself is pinned in
        // remote_request_joins_pending_auto_release. The joined release's
        // timeout still counts from its send at 100.
        let mut p = auto_release();
        assert_eq!(p.feed(1800, Input::RemoteOwnershipRequest), []);
        p.core.take_lines();
        let tick = 100 + ACK_TIMEOUT.as_millis() as u64 + 1;
        assert_eq!(p.feed(tick, Input::Tick), []);
        assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);

        // Playback restarted during the release: Linux held no take, so the
        // request pauses nothing and the ACK neither retakes nor pauses.
        let mut p = auto_release();
        p.core.take_lines();
        assert_eq!(p.play(1600, true), []);
        assert_eq!(p.feed(1700, Input::RemoteOwnershipRequest), []);
        assert_eq!(p.feed(1800, Input::Ownership(vec![0])), []);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);

        // A call started during the release: with no take to keep, the
        // request joins it too, so the ACK retakes nothing.
        let mut p = auto_release();
        assert_eq!(p.feed(1550, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.play(1600, true), []);
        p.core.take_lines();
        assert_eq!(p.feed(1700, Input::RemoteOwnershipRequest), []);
        assert_eq!(
            p.core.take_lines(),
            ["REMOTE requested ownership; release already pending"]
        );
        assert_eq!(p.feed(1800, Input::Ownership(vec![0])), []);
    }

    #[test]
    fn remote_request_joins_pending_auto_release() {
        // #49's Auto release joined by a remote request (#137). The buds go
        // in only after Auto, so the connect take (#324) doesn't keep the
        // ownership instead.
        let mut p = Probe::new(false, true);
        p.routing(SessionFacts {
            owner: Some(vec![1]),
            ..unworn_facts()
        });
        let all = [
            p.feed(100, Input::Auto),
            p.feed(150, Input::EarState(vec![InEar, InEar])),
            p.feed(200, Input::RemoteOwnershipRequest),
            p.feed(300, Input::Ownership(vec![0])),
            p.play(400, false),
            p.play(500, true),
            p.feed(600, Input::Ownership(vec![1])),
        ];
        assert_eq!(
            all,
            [
                vec![ConnectA2dp, SendOwnership(false), Announce, DrainPlayback],
                vec![],
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
            ["AUTO playback stopped; keeping ownership"]
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
                "AUTO playback stopped during the take; keeping ownership"
            ),
            1
        );
        assert_eq!(p.play(1500, false), []);
        assert_eq!(p.feed(9000, Input::Tick), []);
        // The call's end releases nothing (#262).
        assert_eq!(
            p.feed(9100, Input::LocalCall(false)),
            [AdvertiseCall(false)]
        );
        p.idle_ticks(9100);

        // The same when the take's ACK times out during the call.
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(550, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.play(1000, false), []);
        assert_eq!(p.feed(5600, Input::Tick), []);
        assert_eq!(
            p.feed(6000, Input::LocalCall(false)),
            [AdvertiseCall(false)]
        );
        p.idle_ticks(6000);

        // The call ends while the take is still pending: its ACK keeps it.
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(550, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.play(1000, false), []);
        assert_eq!(
            p.feed(1100, Input::LocalCall(false)),
            [AdvertiseCall(false)]
        );
        assert_eq!(p.feed(1200, Input::Ownership(vec![1])), []);
        p.idle_ticks(1200);
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
        // Manual mode records the call; going automatic advertises it. Buds
        // out: the connect take (#324) is not under test here.
        let mut p = Probe::new(false, true);
        p.routing(unworn_facts());
        assert_eq!(p.feed(100, Input::LocalCall(true)), []);
        assert_eq!(
            p.feed(200, Input::Auto),
            [
                ConnectA2dp,
                SendOwnership(false),
                Announce,
                AdvertiseCall(true),
                DrainPlayback
            ]
        );

        // The call survives a disconnect and is advertised after the
        // reconnect's announcement; changes while disconnected only record.
        let mut p = acked_take_in_call();
        assert_eq!(p.feed(1100, Input::Disconnected), [PauseMedia, DropSession]);
        assert_eq!(p.feed(1150, Input::LocalCall(false)), []);
        assert_eq!(p.feed(1160, Input::LocalCall(true)), []);
        assert_eq!(p.feed(2200, Input::Tick), [Reconnect { audio: true }]);
        // The idle other host: the connect take (#324), after the call's advert.
        assert_eq!(
            p.feed(2300, Input::Reconnected(ready_facts())),
            [
                AdoptSession,
                Announce,
                AdvertiseCall(true),
                SendOwnership(true),
                DrainPlayback
            ]
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

        // A stop edge keeps them as in takes_on_new_playback_when_worn.
        let mut p = acked_take_in_call();
        assert_eq!(
            p.feed(1100, Input::LocalCall(false)),
            [AdvertiseCall(false)]
        );
        assert_eq!(p.play(1500, false), []);
        p.idle_ticks(1500);
        assert_eq!(p.play(62_000, false), []);
    }

    #[test]
    fn call_end_at_idle_keeps_ownership() {
        // Stopped during the call while Linux owns: the end only advertises
        // it (#262).
        let mut p = acked_take_in_call();
        assert_eq!(p.play(1500, false), []);
        p.core.take_lines();
        assert_eq!(
            p.feed(2000, Input::LocalCall(false)),
            [AdvertiseCall(false)]
        );
        assert_eq!(p.core.take_lines(), ["CALL ended on Linux"]);
        p.idle_ticks(2000);
        assert_eq!(p.play(62_500, false), []);

        // During the call, a new host still hears of it after its Announce,
        // and a remote request is still refused.
        let mut p = acked_take_in_call();
        assert_eq!(p.play(1500, false), []);
        assert_eq!(
            p.feed(1600, report(&[L, R], &[L, R, R2])),
            [Announce, AdvertiseCall(true)]
        );
        assert_eq!(p.feed(1700, Input::RemoteOwnershipRequest), []);
        p.idle_ticks(1700);

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

    // "Connect to this computer" (#16).

    use crate::handoff_log::{Kind, classify};
    use ConnectMode::{Automatically, WhenLastConnected};

    /// `Probe::ready`, with its core built in `mode`.
    fn ready_in(mode: ConnectMode) -> Probe {
        let t0 = Instant::now();
        let mut p = Probe {
            core: Handoff::new(LOCAL.to_string(), t0, true, true).with_connect_mode(mode),
            t0,
            last_ms: 0,
        };
        p.routing(ready_facts());
        assert_eq!(p.play(10, false), []);
        p
    }

    /// Connect-mode skip lines since the last call; each classifies as Other.
    fn skips(p: &mut Probe) -> usize {
        let lines = p.core.take_lines();
        let skipped: Vec<_> = lines.iter().filter(|l| *l == CONNECT_MODE_SKIP).collect();
        for line in &skipped {
            assert!(line.starts_with("AUTO "), "{line}");
            assert_eq!(classify(line), Kind::Other, "{line}");
        }
        skipped.len()
    }

    #[test]
    fn connect_mode_automatically_takes_on_new_playback() {
        for mut p in [Probe::ready(), ready_in(Automatically)] {
            assert_eq!(p.play(500, true), [SendOwnership(true)]);
            assert_eq!(skips(&mut p), 0);
        }
        // Another host taking first changes nothing in this mode.
        let mut p = ready_in(Automatically);
        assert_eq!(
            p.feed(
                100,
                Input::AudioSource(source(REMOTE, AudioSourceType::Media))
            ),
            []
        );
        assert_eq!(p.feed(200, Input::AudioSource(idle())), []);
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
    }

    #[test]
    fn when_last_connected_skips_when_linux_was_not_last_owner() {
        let mut p = ready_in(WhenLastConnected);
        p.core.take_lines();
        assert_eq!(p.play(500, true), []);
        assert_eq!(skips(&mut p), 1);
        // Nothing is pending or held: no timeout, no release on stop.
        assert_eq!(p.feed(6000, Input::Tick), []);
        assert_eq!(p.play(6500, false), []);
        assert_eq!(p.play(7000, true), []);
        assert_eq!(skips(&mut p), 1);
    }

    #[test]
    fn when_last_connected_takes_when_linux_was_last_owner() {
        let mut p = ready_in(WhenLastConnected);
        // A manual take's ACK makes Linux the last owner.
        assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
        assert_eq!(p.feed(200, Input::Ownership(vec![1])), []);
        assert_eq!(p.feed(300, Input::Release), [SendOwnership(false)]);
        assert_eq!(p.feed(400, Input::Ownership(vec![0])), [PauseMedia]);
        // Linux's own releases keep it the last owner, and a stop keeps the
        // take Linux holds (#262).
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(1000, false), []);
        p.idle_ticks(1000);
        assert_eq!(p.play(61_500, true), []);
        assert_eq!(skips(&mut p), 0);

        // An OwnsConnection=1 report alone does too.
        let mut p = ready_in(WhenLastConnected);
        assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(skips(&mut p), 0);
    }

    /// WhenLastConnected, Linux the last owner after an acknowledged take
    /// for playback still running at 1000 ms.
    fn last_owner_take() -> Probe {
        let mut p = ready_in(WhenLastConnected);
        assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(1000, true), []);
        p.core.take_lines();
        p
    }

    #[test]
    fn when_last_connected_skips_after_remote_host_took() {
        // A genuine loss: another host took the AirPods.
        let mut p = last_owner_take();
        assert_eq!(p.feed(1100, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(p.play(1500, false), []);
        assert_eq!(p.play(2000, true), []);
        assert_eq!(skips(&mut p), 1);

        // A remote request Linux yields to.
        let mut p = last_owner_take();
        assert_eq!(
            p.feed(1100, Input::RemoteOwnershipRequest),
            [PauseMedia, SendOwnership(false)]
        );
        assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);
        assert_eq!(p.play(1500, false), []);
        assert_eq!(p.play(2000, true), []);
        assert_eq!(skips(&mut p), 1);

        // A remote request after a stop: Linux, the idle owner, yields.
        let mut p = last_owner_take();
        assert_eq!(p.play(1100, false), []);
        assert_eq!(
            p.feed(1200, Input::RemoteOwnershipRequest),
            [PauseMedia, SendOwnership(false)]
        );
        assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
        assert_eq!(p.play(2000, true), []);
        assert_eq!(skips(&mut p), 1);

        // A remote request joining Linux's pending user release.
        let mut p = last_owner_take();
        assert_eq!(p.feed(1100, Input::Release), [SendOwnership(false)]);
        assert_eq!(p.feed(1200, Input::RemoteOwnershipRequest), [PauseMedia]);
        assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
        assert_eq!(p.play(1400, false), []);
        assert_eq!(p.play(2000, true), []);
        assert_eq!(skips(&mut p), 1);

        // A remote request after Linux's user release was acknowledged.
        let mut p = ready_in(WhenLastConnected);
        assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
        assert_eq!(p.feed(200, Input::Ownership(vec![1])), []);
        assert_eq!(p.feed(300, Input::Release), [SendOwnership(false)]);
        assert_eq!(p.feed(400, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(p.feed(500, Input::RemoteOwnershipRequest), []);
        assert_eq!(p.play(1000, true), []);
        assert_eq!(skips(&mut p), 1);

        // A remote host already given the AirPods asks again.
        let mut p = ready_in(WhenLastConnected);
        assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
        assert_eq!(p.feed(30, Input::Ownership(vec![0])), []);
        assert_eq!(p.feed(100, Input::RemoteOwnershipRequest), []);
        assert_eq!(p.play(500, true), []);
        assert_eq!(skips(&mut p), 1);

        // Remote media or a remote call.
        for r#type in [AudioSourceType::Media, AudioSourceType::Call] {
            let mut p = ready_in(WhenLastConnected);
            assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
            assert_eq!(p.feed(100, Input::AudioSource(source(REMOTE, r#type))), []);
            assert_eq!(p.feed(200, Input::AudioSource(idle())), []);
            p.core.take_lines();
            assert_eq!(p.play(500, true), [], "{type:?}");
            assert_eq!(skips(&mut p), 1, "{type:?}");
        }

        // Linux's own media and remote idle audio leave it the last owner.
        let mut p = ready_in(WhenLastConnected);
        assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
        assert_eq!(
            p.feed(
                100,
                Input::AudioSource(source(LOCAL, AudioSourceType::Media))
            ),
            []
        );
        assert_eq!(
            p.feed(
                150,
                Input::AudioSource(source(REMOTE, AudioSourceType::None))
            ),
            []
        );
        assert_eq!(p.feed(200, Input::AudioSource(idle())), []);
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
    }

    #[test]
    fn when_last_connected_refused_remote_request_keeps_last_owner() {
        // A local call refuses the request: Linux keeps the AirPods. As in
        // `local_call_refuses_remote_request`, the iPhone's media comes
        // first.
        let mut p = last_owner_take();
        assert_eq!(p.feed(1100, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(
            p.feed(
                1150,
                Input::AudioSource(source(REMOTE, AudioSourceType::Media))
            ),
            []
        );
        assert_eq!(p.feed(1200, Input::RemoteOwnershipRequest), []);
        assert_eq!(
            p.feed(1300, Input::LocalCall(false)),
            [AdvertiseCall(false)]
        );
        assert_eq!(p.feed(1350, Input::AudioSource(idle())), []);
        assert_eq!(p.play(1500, false), []);
        // Still the last owner: once Linux gives the AirPods up, a new play
        // edge takes them back.
        assert_eq!(p.feed(1550, Input::Release), [SendOwnership(false)]);
        assert_eq!(p.feed(1600, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(p.play(2000, true), [SendOwnership(true)]);
    }

    /// Disconnect with the case closed, then new playback that reconnects
    /// with `facts` at 1200 ms; returns the reconnect's commands.
    fn case_reconnect(p: &mut Probe, facts: SessionFacts) -> Vec<Command> {
        p.feed(30, Input::EarState(vec![InCase, InCase]));
        assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
        assert_eq!(p.play(500, false), []);
        assert_eq!(p.play(1000, true), []);
        assert_eq!(p.feed(1100, Input::Tick), [Reconnect { audio: true }]);
        p.core.take_lines();
        p.feed(1200, Input::Reconnected(facts))
    }

    #[test]
    fn when_last_connected_last_owner_survives_reconnect() {
        // Linux was the last owner before the disconnect: the reconnect for
        // playback takes.
        let mut p = ready_in(WhenLastConnected);
        assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
        assert_eq!(
            case_reconnect(&mut p, ready_facts()),
            [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
        );
        assert_eq!(skips(&mut p), 0);
        // The stale init [0] stays ignored, and the take settles.
        assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
        assert_eq!(p.feed(1400, Input::Ownership(vec![1])), []);

        // It was not: the reconnect adopts the session but takes nothing,
        // and leaves no take to release or time out.
        let mut p = ready_in(WhenLastConnected);
        assert_eq!(
            case_reconnect(&mut p, ready_facts()),
            [AdoptSession, Announce, DrainPlayback]
        );
        assert_eq!(skips(&mut p), 1);
        assert_eq!(p.play(1500, false), []);
        assert_eq!(p.feed(9000, Input::Tick), []);

        // A session snapshot where Linux owns makes it the last owner.
        let mut p = ready_in(WhenLastConnected);
        let facts = SessionFacts {
            owner: Some(vec![1]),
            ..ready_facts()
        };
        assert_eq!(
            case_reconnect(&mut p, facts),
            [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
        );
        assert_eq!(skips(&mut p), 0);
    }

    #[test]
    fn when_last_connected_reconcile_retake_gated() {
        // A play edge during Linux's release retakes once it settles only
        // while Linux is the last owner: here a remote host's media cleared
        // it. Auto's release: Linux owns ([1]) without a take, and the buds
        // are out at Auto, so the connect take (#324) doesn't keep it.
        let released = || {
            let mut p = ready_in(WhenLastConnected);
            assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
            assert_eq!(p.feed(30, Input::EarState(vec![OutOfEar, OutOfEar])), []);
            assert_eq!(
                p.feed(100, Input::Auto),
                [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
            );
            p.wear_in(150);
            assert_eq!(p.play(1100, false), []);
            p.core.take_lines();
            p
        };
        let mut p = released();
        assert_eq!(
            p.feed(
                1150,
                Input::AudioSource(source(REMOTE, AudioSourceType::Media))
            ),
            []
        );
        assert_eq!(p.feed(1160, Input::AudioSource(idle())), []);
        assert_eq!(p.play(1200, true), []);
        assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
        assert_eq!(skips(&mut p), 1);

        // Still the last owner: the same edge retakes.
        let mut p = released();
        assert_eq!(p.play(1200, true), []);
        assert_eq!(
            p.feed(1300, Input::Ownership(vec![0])),
            [SendOwnership(true)]
        );
        assert_eq!(skips(&mut p), 0);
    }

    #[test]
    fn when_last_connected_manual_take_not_gated() {
        let mut p = ready_in(WhenLastConnected);
        p.core.take_lines();
        assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
        assert_eq!(skips(&mut p), 0);
        assert_eq!(p.feed(200, Input::Ownership(vec![1])), []);
    }

    #[test]
    fn when_last_connected_late_take_ack_after_remote_request_skips() {
        // A take's ACK [1] arrives after Linux yielded to a remote request;
        // the [0] that acknowledges the remote release still clears it.
        let mut p = ready_in(WhenLastConnected);
        assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
        assert_eq!(
            p.feed(200, Input::RemoteOwnershipRequest),
            [PauseMedia, SendOwnership(false)]
        );
        assert_eq!(p.feed(300, Input::Ownership(vec![1])), []);
        assert_eq!(p.feed(400, Input::Ownership(vec![0])), []);
        p.core.take_lines();
        assert_eq!(p.play(1000, true), []);
        assert_eq!(skips(&mut p), 1);
    }

    #[test]
    fn when_last_connected_late_remote_release_ack_skips() {
        // The same, with the remote release's [0] after its timeout (#55).
        let mut p = last_owner_take();
        assert_eq!(
            p.feed(1100, Input::RemoteOwnershipRequest),
            [PauseMedia, SendOwnership(false)]
        );
        assert_eq!(p.feed(1150, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(1500, false), []);
        assert_eq!(p.feed(6200, Input::Tick), []);
        assert_eq!(p.feed(6300, Input::Ownership(vec![0])), []);
        assert_eq!(
            count_lines(&p.core.take_lines(), "ACK owner=not-linux late"),
            1
        );
        assert_eq!(p.play(7000, true), []);
        assert_eq!(skips(&mut p), 1);
    }

    #[test]
    fn when_last_connected_unsolicited_loss_skips() {
        // Owner [1] without a Linux take, then [0]: another host took them.
        let mut p = ready_in(WhenLastConnected);
        assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
        assert_eq!(p.feed(30, Input::Ownership(vec![0])), []);
        p.core.take_lines();
        assert_eq!(p.play(500, true), []);
        assert_eq!(skips(&mut p), 1);

        // A repeated [0] after Linux's own release leaves it the last owner.
        let mut p = last_owner_take();
        assert_eq!(p.feed(1100, Input::Release), [SendOwnership(false)]);
        assert_eq!(p.feed(1200, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
        assert_eq!(p.play(1400, false), []);
        assert_eq!(p.play(1500, true), [SendOwnership(true)]);
        assert_eq!(skips(&mut p), 0);
    }

    #[test]
    fn when_last_connected_seeded_last_owner_takes() {
        // A new core for the same AirPods (the shell's per-MAC store, #16)
        // with seed facts saying owner [0] and no remote audio.
        for (seeded, playback) in [(true, vec![SendOwnership(true)]), (false, vec![])] {
            let t0 = Instant::now();
            let mut p = Probe {
                core: Handoff::new(LOCAL.to_string(), t0, true, true)
                    .with_connect_mode(WhenLastConnected)
                    .with_last_owner_linux(seeded),
                t0,
                last_ms: 0,
            };
            p.routing(ready_facts());
            assert_eq!(p.play(10, false), []);
            p.core.take_lines();
            assert_eq!(p.play(500, true), playback, "seeded={seeded}");
            assert_eq!(skips(&mut p), usize::from(!seeded), "seeded={seeded}");
            assert_eq!(p.core.last_owner_linux(), seeded);
        }
    }

    // #200: A2DP resume after a take that found the transport idle.

    fn idle_transport() -> Input {
        Input::A2dpTransport { active: false }
    }

    /// `Probe::ready`, an idle transport reported, then a take for playback
    /// at 500 ms.
    fn take_with_idle_transport() -> Probe {
        let mut p = Probe::ready();
        assert_eq!(p.feed(20, idle_transport()), []);
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        p
    }

    /// `take_with_idle_transport`, acknowledged at 600 ms: the shell's
    /// resume chain may run until `window_end()`.
    fn resumed_take() -> Probe {
        let mut p = take_with_idle_transport();
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), [ResumeA2dp]);
        p
    }

    fn window_end() -> u64 {
        600 + A2DP_RESUME_WINDOW.as_millis() as u64
    }

    #[test]
    fn take_ack_with_idle_transport_resumes_a2dp_once() {
        let mut p = take_with_idle_transport();
        p.core.take_lines();
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), [ResumeA2dp]);
        let lines = p.core.take_lines();
        assert_eq!(lines.len(), 2, "{lines:?}");
        assert!(lines[0].starts_with("ACK owner=linux"));
        assert_eq!(lines[1], "A2DP transport idle after the take; resuming it");
        // A duplicate [1] acknowledges nothing.
        assert_eq!(p.feed(700, Input::Ownership(vec![1])), []);
        assert_eq!(p.feed(800, idle_transport()), []);
        assert_eq!(p.feed(900, Input::Ownership(vec![1])), []);
    }

    #[test]
    fn take_ack_with_active_transport_does_not_resume() {
        let mut p = Probe::ready();
        // The last report counts.
        assert_eq!(p.feed(20, idle_transport()), []);
        assert_eq!(p.feed(30, Input::A2dpTransport { active: true }), []);
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        // A stop keeps the take (#262); a play edge while owning resumes
        // nothing.
        assert_eq!(p.play(1000, false), []);
        p.idle_ticks(1000);
        assert_eq!(p.play(62_000, true), []);
    }

    #[test]
    fn take_ack_without_transport_report_does_not_resume() {
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(1000, false), []);
        p.idle_ticks(1000);
        assert_eq!(p.play(62_000, true), []);
    }

    #[test]
    fn release_ack_never_resumes_a2dp() {
        // Auto's release.
        let mut p = auto_release();
        assert_eq!(p.feed(300, idle_transport()), []);
        assert_eq!(p.feed(400, Input::Ownership(vec![0])), []);

        // A user release of AirPods Linux owns.
        let mut p = Probe::ready();
        assert_eq!(p.feed(20, idle_transport()), []);
        assert_eq!(p.feed(30, Input::Ownership(vec![1])), []);
        assert_eq!(p.feed(100, Input::Release), [SendOwnership(false)]);
        assert_eq!(p.feed(200, Input::Ownership(vec![0])), []);
    }

    #[test]
    fn manual_take_ack_never_resumes_a2dp() {
        let mut p = Probe::new(false, true);
        p.routing(ready_facts());
        assert_eq!(p.feed(50, idle_transport()), []);
        assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
        assert_eq!(p.feed(200, Input::Ownership(vec![1])), []);
    }

    #[test]
    fn take_ack_during_local_call_does_not_resume() {
        // The call starts while the take is pending.
        let mut p = take_with_idle_transport();
        assert_eq!(p.feed(550, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);

        // The call started before the take.
        let mut p = Probe::ready();
        assert_eq!(p.feed(20, idle_transport()), []);
        assert_eq!(p.feed(30, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    }

    #[test]
    fn stop_during_take_defers_a2dp_resume_to_next_play() {
        // Plain, then with default sink routing and the category relay on.
        for (routing, relay) in [(false, false), (true, true)] {
            let relayed = |c| {
                if relay {
                    vec![RelayCategory(c)]
                } else {
                    vec![]
                }
            };
            let mut p = Probe::ready();
            p.core.default_sink_routing = routing;
            p.core.category_relay = relay;
            assert_eq!(p.feed(20, idle_transport()), []);
            assert_eq!(
                p.play_category(500, MEDIA),
                [relayed(MEDIA), vec![SendOwnership(true)]].concat()
            );
            assert_eq!(p.play_category(550, IDLE), relayed(IDLE));
            // The ACK holds the take (#262) and skips the resume.
            let route = if routing {
                vec![RouteToAirPods]
            } else {
                vec![]
            };
            assert_eq!(p.feed(600, Input::Ownership(vec![1])), route);
            p.idle_ticks(600);
            // The next play edge while Linux owns resumes, once.
            p.core.take_lines();
            assert_eq!(
                p.play_category(62_000, MEDIA),
                [relayed(MEDIA), vec![ResumeA2dp]].concat()
            );
            assert_eq!(
                p.core.take_lines(),
                [
                    "AUTO playback started; Linux already owns",
                    "A2DP transport idle after the take; resuming it"
                ]
            );
            assert_eq!(p.play_category(62_500, IDLE), relayed(IDLE));
            assert_eq!(p.play_category(63_000, MEDIA), relayed(MEDIA));
            p.idle_ticks(63_000);
        }

        // An active transport, or a local call, needs no resume.
        let mut p = routed();
        assert_eq!(p.feed(20, Input::A2dpTransport { active: true }), []);
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.play(550, false), []);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), [RouteToAirPods]);
        assert_eq!(p.play(2000, true), []);

        let mut p = routed();
        assert_eq!(p.feed(20, idle_transport()), []);
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.play(550, false), []);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), [RouteToAirPods]);
        assert_eq!(p.feed(700, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.play(2000, true), []);

        // The phone disconnecting first blocks a fresh take, not the resume
        // of the take Linux already holds.
        let mut p = routed();
        assert_eq!(p.feed(20, idle_transport()), []);
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.play(550, false), []);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), [RouteToAirPods]);
        assert_eq!(p.feed(700, report(&[LOCAL, REMOTE], &[LOCAL])), []);
        assert_eq!(p.play(2000, true), [ResumeA2dp]);
        assert_eq!(p.play(2500, false), []);
        assert_eq!(p.play(3000, true), []);
        p.idle_ticks(3000);

        // After a yield, a new take resumes at its own ACK.
        let mut p = take_with_idle_transport();
        assert_eq!(p.play(550, false), []);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.feed(700, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(p.play(1000, true), [SendOwnership(true)]);
        assert_eq!(p.feed(1100, Input::Ownership(vec![1])), [ResumeA2dp]);
    }

    #[test]
    fn stop_during_a2dp_resume_window_keeps_ownership() {
        let mut p = resumed_take();
        p.core.take_lines();
        assert_eq!(p.play(1000, false), []);
        assert_eq!(
            p.core.take_lines(),
            ["AUTO playback stopped; keeping ownership"]
        );
        for ms in [1500, 3000, window_end() - 1] {
            assert_eq!(p.feed(ms, Input::Tick), []);
            assert_eq!(p.play(ms, false), []);
        }
        p.idle_ticks(window_end());
    }

    #[test]
    fn kick_pause_play_during_resume_window_sends_nothing() {
        let mut p = resumed_take();
        assert_eq!(p.play(1000, true), []);
        // The kick's Pause and Play.
        assert_eq!(p.play(2000, false), []);
        assert_eq!(p.play(2500, true), []);
        // The profile cycle pauses the player; the chain plays it again.
        assert_eq!(p.play(4000, false), []);
        assert_eq!(p.play(5500, true), []);
        assert_eq!(p.feed(window_end(), Input::Tick), []);
        assert_eq!(p.feed(window_end() + 200, Input::Tick), []);
        // Linux still owns the AirPods and keeps them after the next stop.
        assert_eq!(p.play(window_end() + 500, false), []);
        p.idle_ticks(window_end() + 500);
    }

    #[test]
    fn stop_persisting_past_resume_window_keeps_ownership() {
        let mut p = resumed_take();
        assert_eq!(p.play(1000, false), []);
        p.idle_ticks(1000);
        assert_eq!(p.play(window_end() + 60_300, false), []);
    }

    #[test]
    fn play_resumed_before_window_end_keeps_ownership() {
        let mut p = resumed_take();
        assert_eq!(p.play(1000, false), []);
        assert_eq!(p.play(3000, true), []);
        assert_eq!(p.feed(window_end(), Input::Tick), []);
        assert_eq!(p.feed(window_end() + 200, Input::Tick), []);
        assert_eq!(p.play(window_end() + 300, true), []);
    }

    #[test]
    fn remote_take_during_resume_window_still_yields() {
        let mut p = resumed_take();
        assert_eq!(p.play(1000, false), []);
        p.core.take_lines();
        assert_eq!(p.feed(1500, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
        assert_eq!(p.feed(window_end(), Input::Tick), []);
        assert_eq!(p.feed(window_end() + 200, Input::Tick), []);
    }

    #[test]
    fn call_ending_during_resume_window_keeps_ownership() {
        let mut p = resumed_take();
        assert_eq!(p.feed(700, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.play(1000, false), []);
        assert_eq!(
            p.feed(2000, Input::LocalCall(false)),
            [AdvertiseCall(false)]
        );
        // Through the window's end and a minute past it (#262).
        p.idle_ticks(2000);
    }

    // #40: default sink routing.

    /// `Probe::ready` with default sink routing on.
    fn routed() -> Probe {
        let mut p = Probe::ready();
        p.core.default_sink_routing = true;
        p
    }

    /// `routed`, then a take for playback acknowledged at 600 ms.
    fn routed_take() -> Probe {
        let mut p = routed();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), [RouteToAirPods]);
        p
    }

    #[test]
    fn take_ack_routes_to_airpods_once() {
        let mut p = routed_take();
        // A duplicate [1] acknowledges nothing.
        assert_eq!(p.feed(700, Input::Ownership(vec![1])), []);
        assert_eq!(p.play(1000, true), []);

        // A manual take routes too.
        let mut p = Probe::new(false, true);
        p.core.default_sink_routing = true;
        p.routing(ready_facts());
        assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
        assert_eq!(p.feed(200, Input::Ownership(vec![1])), [RouteToAirPods]);
        assert_eq!(p.feed(300, Input::Ownership(vec![1])), []);
        // A release ACK routes nothing.
        assert_eq!(p.feed(400, Input::Release), [SendOwnership(false)]);
        assert_eq!(p.feed(500, Input::Ownership(vec![0])), []);
    }

    #[test]
    fn take_ack_routes_before_a2dp_resume() {
        let mut p = routed();
        assert_eq!(p.feed(20, idle_transport()), []);
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        p.core.take_lines();
        assert_eq!(
            p.feed(600, Input::Ownership(vec![1])),
            [RouteToAirPods, ResumeA2dp]
        );
        // No new line for the route.
        let lines = p.core.take_lines();
        assert_eq!(lines.len(), 2, "{lines:?}");
    }

    #[test]
    fn genuine_loss_routes_away_once() {
        let mut p = routed_take();
        p.core.take_lines();
        assert_eq!(
            p.feed(1100, Input::Ownership(vec![0])),
            [PauseMedia, RouteAway]
        );
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
        assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);
        assert_eq!(p.feed(1300, Input::RemoteOwnershipRequest), []);
    }

    #[test]
    fn remote_request_routes_away_once() {
        // A fresh yield: pause, route away, then "not owning".
        let mut p = routed_take();
        assert_eq!(
            p.feed(1100, Input::RemoteOwnershipRequest),
            [PauseMedia, RouteAway, SendOwnership(false)]
        );
        // A repeat joins the remote release; its ACK routes nothing more.
        assert_eq!(p.feed(1150, Input::RemoteOwnershipRequest), []);
        assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);
        assert_eq!(p.feed(1300, Input::RemoteOwnershipRequest), []);

        // Joining a pending user release loses the take too.
        let mut p = routed_take();
        assert_eq!(p.feed(1000, Input::Release), [SendOwnership(false)]);
        assert_eq!(
            p.feed(1100, Input::RemoteOwnershipRequest),
            [PauseMedia, RouteAway]
        );
        assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);

        // Refused during a local call: Linux keeps the AirPods.
        let mut p = routed_take();
        assert_eq!(p.feed(700, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.feed(1100, Input::RemoteOwnershipRequest), []);
    }

    #[test]
    fn timed_out_remote_release_then_loss_routes_away_once() {
        let mut p = routed_take();
        assert_eq!(
            p.feed(1100, Input::RemoteOwnershipRequest),
            [PauseMedia, RouteAway, SendOwnership(false)]
        );
        assert_eq!(p.play(1500, false), []);
        assert_eq!(p.feed(6200, Input::Tick), []);
        // Past LATE_ACK_WINDOW: a genuine loss of the same take.
        assert_eq!(p.feed(11101, Input::Ownership(vec![0])), [PauseMedia]);
        // The next take routes and can route away again.
        assert_eq!(p.play(12000, true), [SendOwnership(true)]);
        assert_eq!(p.feed(12100, Input::Ownership(vec![1])), [RouteToAirPods]);
        assert_eq!(
            p.feed(12500, Input::Ownership(vec![0])),
            [PauseMedia, RouteAway]
        );
    }

    #[test]
    fn retake_lost_before_ack_routes_away() {
        let mut p = routed_take();
        assert_eq!(
            p.feed(1100, Input::Ownership(vec![0])),
            [PauseMedia, RouteAway]
        );
        assert_eq!(p.play(1500, false), []);
        assert_eq!(p.play(2000, true), [SendOwnership(true)]);
        // Lost before its ACK: this take routes away too.
        assert_eq!(
            p.feed(2100, Input::Ownership(vec![0])),
            [PauseMedia, RouteAway]
        );
    }

    #[test]
    fn stop_while_owning_keeps_the_route() {
        let mut p = routed_take();
        assert_eq!(p.play(1000, false), []);
        // Linux keeps the AirPods idle (#262): no RouteAway, and the next
        // play edge routes nothing again.
        p.idle_ticks(1000);
        assert_eq!(p.play(61_500, true), []);
        assert_eq!(p.feed(61_600, Input::Ownership(vec![1])), []);
    }

    #[test]
    fn routing_disabled_emits_no_route_commands() {
        // Take with an idle transport, then a genuine loss.
        let mut p = Probe::ready();
        let mut all = p.feed(20, idle_transport());
        all.extend(p.play(500, true));
        all.extend(p.feed(600, Input::Ownership(vec![1])));
        all.extend(p.feed(1100, Input::Ownership(vec![0])));
        assert_eq!(all, [SendOwnership(true), ResumeA2dp, PauseMedia]);
        // Take, remote request, its ACK.
        let mut p = Probe::ready();
        let mut all = p.play(500, true);
        all.extend(p.feed(600, Input::Ownership(vec![1])));
        all.extend(p.feed(1100, Input::RemoteOwnershipRequest));
        all.extend(p.feed(1200, Input::Ownership(vec![0])));
        assert_eq!(all, [SendOwnership(true), PauseMedia, SendOwnership(false)]);
        // Take, a user release joined by a remote request, its ACK.
        let mut p = Probe::ready();
        let mut all = p.play(500, true);
        all.extend(p.feed(600, Input::Ownership(vec![1])));
        all.extend(p.feed(1000, Input::Release));
        all.extend(p.feed(1100, Input::RemoteOwnershipRequest));
        all.extend(p.feed(1200, Input::Ownership(vec![0])));
        assert_eq!(all, [SendOwnership(true), SendOwnership(false), PauseMedia]);
    }

    // #262: Linux stays the idle owner, as the Mac does.

    /// `relaying` (and routing if asked), a take for 301 acknowledged at
    /// 600 ms with the transport active, then a stop at 1000 ms.
    fn idle_owner(routing: bool) -> Probe {
        let mut p = relaying();
        p.core.default_sink_routing = routing;
        assert_eq!(p.feed(20, Input::A2dpTransport { active: true }), []);
        assert_eq!(
            p.play_category(500, MEDIA),
            [RelayCategory(MEDIA), SendOwnership(true)]
        );
        let route = if routing {
            vec![RouteToAirPods]
        } else {
            vec![]
        };
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), route);
        // The stop only relays 100.
        assert_eq!(p.play_category(1000, IDLE), [RelayCategory(IDLE)]);
        p
    }

    #[test]
    fn stop_keeps_ownership_as_idle_owner() {
        let mut p = idle_owner(false);
        p.core.take_lines();
        // No release and no second relay of 100, however long it stays
        // stopped.
        for ms in (1000..=61_000).step_by(100) {
            assert_eq!(p.feed(ms, Input::Tick), [], "{ms}");
            assert_eq!(p.play_category(ms, IDLE), [], "{ms}");
        }
        assert_eq!(count_lines(&p.core.take_lines(), "AUTO"), 0);
    }

    #[test]
    fn play_while_idle_owner_sends_no_take() {
        // A play edge soon after the stop, and minutes later.
        for play in [2000, 1000 + 120_000] {
            let mut p = idle_owner(false);
            // The idle minute of ticks fits only before the later edge.
            if play > 61_000 {
                p.idle_ticks(1000);
            }
            p.core.take_lines();
            assert_eq!(p.play_category(play, MEDIA), [RelayCategory(MEDIA)]);
            assert_eq!(
                p.core.take_lines(),
                ["AUTO playback started; Linux already owns"]
            );
            assert_eq!(p.play_category(play + 500, IDLE), [RelayCategory(IDLE)]);
            assert_eq!(p.play_category(play + 1000, MEDIA), [RelayCategory(MEDIA)]);
            p.idle_ticks(play + 1000);
        }
    }

    #[test]
    fn play_after_timed_out_take_retakes() {
        let mut p = Probe::ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(5600, Input::Tick), []);
        assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);
        // The owner is still [0]: the take is unconfirmed, so a play edge
        // takes afresh.
        assert_eq!(p.play(6000, false), []);
        assert_eq!(p.play(6500, true), [SendOwnership(true)]);
        assert_eq!(
            count_lines(&p.core.take_lines(), "AUTO playback started; requested"),
            1
        );
    }

    #[test]
    fn idle_owner_loss_yields_once() {
        for (routing, yielded) in [
            (false, vec![PauseMedia]),
            (true, vec![PauseMedia, RouteAway]),
        ] {
            let mut p = idle_owner(routing);
            p.core.take_lines();
            assert_eq!(p.feed(2000, Input::Ownership(vec![0])), yielded);
            assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
            p.idle_ticks(2000);
            assert_eq!(p.feed(62_100, Input::Ownership(vec![0])), []);
            // An idle remote: the next play edge takes afresh.
            assert_eq!(
                p.play_category(63_000, MEDIA),
                [RelayCategory(MEDIA), SendOwnership(true)]
            );
        }
    }

    #[test]
    fn idle_owner_remote_request_yields_once() {
        for (routing, yielded) in [
            (false, vec![PauseMedia, SendOwnership(false)]),
            (true, vec![PauseMedia, RouteAway, SendOwnership(false)]),
        ] {
            let mut p = idle_owner(routing);
            p.core.take_lines();
            assert_eq!(p.feed(2000, Input::RemoteOwnershipRequest), yielded);
            assert_eq!(p.feed(2100, Input::Ownership(vec![0])), []);
            p.idle_ticks(2100);
            let lines = p.core.take_lines();
            assert_eq!(
                count_lines(&lines, "REMOTE requested ownership; yielding"),
                1
            );
            assert_eq!(count_lines(&lines, "YIELD"), 0);
            assert_eq!(
                p.play_category(63_000, MEDIA),
                [RelayCategory(MEDIA), SendOwnership(true)]
            );
        }
    }

    // #253: mode toggles after a stop keep Linux the idle owner (#262).

    #[test]
    fn manual_after_stop_keeps_ownership() {
        let mut p = acked_auto_take();
        assert_eq!(p.play(2000, false), []);
        assert_eq!(p.feed(3000, Input::Manual), []);
        for ms in (3000..=63_000).step_by(100) {
            assert_eq!(p.feed(ms, Input::Tick), [], "{ms}");
            assert_eq!(p.play(ms, false), [], "{ms}");
        }
        // Back to automatic: no "not owning" for the take Linux holds.
        assert_eq!(
            p.feed(64_000, Input::Auto),
            [ConnectA2dp, Announce, DrainPlayback]
        );
        p.idle_ticks(64_000);
    }

    #[test]
    fn auto_after_stop_keeps_ownership() {
        let mut p = acked_auto_take();
        assert_eq!(p.play(2000, false), []);
        assert_eq!(
            p.feed(3000, Input::Auto),
            [ConnectA2dp, Announce, DrainPlayback]
        );
        // Stopped samples after the new baseline release nothing either.
        for ms in (3050..=63_050).step_by(100) {
            assert_eq!(p.feed(ms, Input::Tick), [], "{ms}");
            assert_eq!(p.play(ms, false), [], "{ms}");
        }
    }

    // #264: the A2DP hold.

    fn remote_media() -> Input {
        Input::AudioSource(source(REMOTE, AudioSourceType::Media))
    }

    fn holds(out: &[Command]) -> Vec<Command> {
        out.iter()
            .filter(|c| matches!(c, HoldA2dp(_)))
            .cloned()
            .collect()
    }

    /// The daemon's start with `hold` and routing on: seed facts, then the
    /// session start's `Auto`, its release acknowledged, then a "stopped"
    /// baseline sample. The buds go in only after `Auto`, so the connect
    /// take (#324) doesn't replace that release.
    fn hold_ready(hold: bool) -> Probe {
        let mut p = Probe::new(false, true);
        p.core.a2dp_hold = hold;
        p.core.default_sink_routing = true;
        p.routing(unworn_facts());
        let start = p.feed(5, Input::Auto);
        assert_eq!(
            holds(&start),
            if hold { vec![HoldA2dp(false)] } else { vec![] }
        );
        assert_eq!(p.feed(8, Input::Ownership(vec![0])), []);
        p.wear_in(9);
        assert_eq!(p.play(10, false), []);
        p
    }

    /// `hold_ready(true)`, then a take for playback acknowledged at 600 ms.
    fn hold_take() -> Probe {
        let mut p = hold_ready(true);
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), [RouteToAirPods]);
        p
    }

    /// `hold_ready(true)` with another host's request answered: held.
    fn held() -> Probe {
        let mut p = hold_ready(true);
        assert_eq!(p.feed(100, Input::RemoteOwnershipRequest), [HoldA2dp(true)]);
        p
    }

    #[test]
    fn hold_session_start_idle_releases_before_connect_a2dp() {
        // Buds out: Auto keeps its release, no connect take (#324).
        let mut p = Probe::new(false, true);
        p.core.a2dp_hold = true;
        p.routing(unworn_facts());
        assert_eq!(
            p.feed(5, Input::Auto),
            [
                HoldA2dp(false),
                ConnectA2dp,
                SendOwnership(false),
                Announce,
                DrainPlayback
            ]
        );
        // A later Auto emits only a change.
        assert_eq!(p.feed(8, Input::Ownership(vec![0])), []);
        assert_eq!(
            p.feed(10, Input::Auto),
            [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
        );
    }

    #[test]
    fn hold_session_start_with_remote_media_holds() {
        let mut p = Probe::new(false, true);
        p.core.a2dp_hold = true;
        // The seed's edge only records the fact: no session start yet.
        p.routing(ready_facts());
        assert_eq!(p.feed(1, remote_media()), []);
        // Relayed, so the state is complete and Auto decides at once (#452).
        assert_eq!(p.feed(2, relay(REMOTE, MEDIA)), []);
        assert_eq!(
            p.feed(5, Input::Auto),
            [
                HoldA2dp(true),
                ConnectA2dp,
                SendOwnership(false),
                Announce,
                DrainPlayback
            ]
        );
    }

    #[test]
    fn hold_reconnected_facts_start_a_session() {
        let mut p = held();
        assert_eq!(
            p.feed(200, Input::Disconnected),
            [HoldA2dp(false), DropSession]
        );
        assert_eq!(p.feed(1200, Input::Tick), [Reconnect { audio: true }]);
        let remote = SessionFacts {
            audio: Some(source(REMOTE, AudioSourceType::Media)),
            ..ready_facts()
        };
        assert_eq!(
            p.feed(1300, Input::Reconnected(remote)),
            [AdoptSession, HoldA2dp(true), Announce, DrainPlayback]
        );
        // Idle facts restore the card, and the idle other host gets the
        // connect take (#324).
        let mut p = hold_ready(true);
        assert_eq!(p.feed(200, Input::Disconnected), [DropSession]);
        assert_eq!(p.feed(1200, Input::Tick), [Reconnect { audio: true }]);
        assert_eq!(
            p.feed(1300, Input::Reconnected(ready_facts())),
            [
                AdoptSession,
                HoldA2dp(false),
                Announce,
                SendOwnership(true),
                DrainPlayback
            ]
        );
    }

    #[test]
    fn hold_issue_sequence_remote_request_after_stop() {
        // Linux stays the idle owner after the stop (#262), so the remote
        // request yields the take and holds.
        let mut p = hold_take();
        assert_eq!(p.play(1000, false), []);
        assert_eq!(
            p.feed(1200, Input::RemoteOwnershipRequest),
            [PauseMedia, RouteAway, SendOwnership(false), HoldA2dp(true)]
        );
        assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
    }

    #[test]
    fn hold_not_on_own_release_ack_alone() {
        let mut p = hold_take();
        let mut all = p.play(1000, false);
        all.extend(p.feed(1050, Input::Release));
        all.extend(p.feed(1100, Input::Ownership(vec![0])));
        for ms in (1200..20_000).step_by(1000) {
            all.extend(p.feed(ms, Input::Tick));
        }
        assert_eq!(all, [SendOwnership(false), PauseMedia]);
    }

    #[test]
    fn hold_remote_request_on_acked_take() {
        let mut p = hold_take();
        assert_eq!(
            p.feed(1100, Input::RemoteOwnershipRequest),
            [PauseMedia, RouteAway, SendOwnership(false), HoldA2dp(true)]
        );
        // Later edges while held emit nothing more.
        assert_eq!(p.feed(1150, Input::RemoteOwnershipRequest), []);
        assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);
        assert_eq!(p.feed(1300, Input::RemoteOwnershipRequest), []);
        assert_eq!(p.feed(1400, remote_media()), []);
    }

    #[test]
    fn hold_remote_request_joins_pending_release() {
        let mut p = hold_take();
        assert_eq!(p.feed(1000, Input::Release), [SendOwnership(false)]);
        assert_eq!(
            p.feed(1100, Input::RemoteOwnershipRequest),
            [PauseMedia, RouteAway, HoldA2dp(true)]
        );
        assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);
    }

    #[test]
    fn hold_remote_request_resends_timed_out_release() {
        let mut p = hold_take();
        assert_eq!(p.feed(700, Input::Release), [SendOwnership(false)]);
        assert_eq!(p.feed(700 + 5001, Input::Tick), []);
        // Not held yet: the timed-out release was the user's.
        assert_eq!(
            p.feed(5800, Input::RemoteOwnershipRequest),
            [PauseMedia, RouteAway, SendOwnership(false), HoldA2dp(true)]
        );
        assert_eq!(p.feed(5900, Input::Tick), []);
        assert_eq!(p.feed(10_900, Input::Tick), []);
        // The remote release timed out too: a repeat re-sends it.
        assert_eq!(
            p.feed(11_000, Input::RemoteOwnershipRequest),
            [SendOwnership(false)]
        );
    }

    #[test]
    fn hold_yield_take() {
        let mut p = hold_take();
        assert_eq!(
            p.feed(1100, Input::Ownership(vec![0])),
            [PauseMedia, RouteAway, HoldA2dp(true)]
        );
        assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);
    }

    #[test]
    fn hold_unsolicited_loss_after_linux_owned() {
        let mut p = hold_ready(true);
        // Linux owns without a take of its own.
        assert_eq!(p.feed(100, Input::Ownership(vec![1])), []);
        assert_eq!(p.feed(200, Input::Ownership(vec![0])), [HoldA2dp(true)]);
        assert_eq!(p.feed(300, Input::Ownership(vec![0])), []);
    }

    #[test]
    fn hold_remote_audio_source() {
        let mut p = hold_ready(true);
        assert_eq!(p.feed(100, remote_media()), [HoldA2dp(true)]);
        assert_eq!(p.feed(200, remote_media()), []);
        // While Linux holds a take, the loss that follows holds instead.
        let mut p = hold_take();
        assert_eq!(p.feed(1100, remote_media()), []);
        assert_eq!(
            p.feed(1200, Input::Ownership(vec![0])),
            [PauseMedia, RouteAway, HoldA2dp(true)]
        );
    }

    #[test]
    fn hold_not_on_refusal_or_session_lost() {
        let mut p = hold_take();
        assert_eq!(p.feed(700, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.feed(1100, Input::RemoteOwnershipRequest), []);

        let mut p = hold_ready(true);
        assert_eq!(p.feed(100, Input::SessionLost), []);
        assert_eq!(p.feed(200, Input::RemoteOwnershipRequest), []);
    }

    #[test]
    fn hold_not_after_session_lost_audio_source() {
        let mut p = hold_ready(true);
        assert_eq!(p.feed(100, Input::SessionLost), []);
        assert_eq!(holds(&p.feed(200, remote_media())), []);
        assert_eq!(p.feed(300, Input::Disconnected), [DropSession]);
    }

    #[test]
    fn hold_not_after_session_lost_unsolicited_zero() {
        let mut p = hold_ready(true);
        assert_eq!(p.feed(100, Input::Ownership(vec![1])), []);
        assert_eq!(p.feed(150, Input::SessionLost), []);
        assert_eq!(holds(&p.feed(200, Input::Ownership(vec![0]))), []);
        assert_eq!(p.feed(300, Input::Disconnected), [DropSession]);
    }

    #[test]
    fn hold_session_lost_keeps_emitted_hold_until_disconnect() {
        // A yield after SessionLost emits no hold.
        let mut p = hold_take();
        assert_eq!(p.feed(700, Input::SessionLost), []);
        assert_eq!(holds(&p.feed(1100, Input::Ownership(vec![0]))), []);

        // A hold emitted before SessionLost stays until Disconnected.
        let mut p = held();
        assert_eq!(p.feed(150, Input::SessionLost), []);
        assert_eq!(holds(&p.feed(200, remote_media())), []);
        assert_eq!(holds(&p.feed(300, Input::RemoteOwnershipRequest)), []);
        assert_eq!(
            p.feed(400, Input::Disconnected),
            [HoldA2dp(false), DropSession]
        );
    }

    #[test]
    fn hold_take_ack_releases_before_route_and_resume() {
        let mut p = held();
        assert_eq!(p.feed(200, idle_transport()), []);
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(
            p.feed(600, Input::Ownership(vec![1])),
            [HoldA2dp(false), RouteToAirPods, ResumeA2dp]
        );
        assert_eq!(p.feed(700, Input::Ownership(vec![1])), []);
    }

    #[test]
    fn hold_kept_by_timed_out_or_lost_take() {
        let mut p = held();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        let mut all = Vec::new();
        for ms in (600..12_000).step_by(500) {
            all.extend(p.feed(ms, Input::Tick));
        }
        assert_eq!(holds(&all), []);

        let mut p = held();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(
            p.feed(600, Input::Ownership(vec![0])),
            [PauseMedia, RouteAway]
        );
    }

    #[test]
    fn hold_disconnected_releases_before_drop_session() {
        let mut p = held();
        assert_eq!(
            p.feed(200, Input::Disconnected),
            [HoldA2dp(false), DropSession]
        );
    }

    #[test]
    fn hold_quit_releases_before_exit() {
        let mut p = held();
        assert_eq!(p.feed(200, Input::Quit), [HoldA2dp(false), Exit]);
    }

    #[test]
    fn hold_remote_source_during_own_release() {
        // A user release, a remote source while it is pending, then its
        // ACK: the ACK holds once Linux has no take.
        let mut p = hold_take();
        assert_eq!(p.feed(1000, Input::Release), [SendOwnership(false)]);
        assert_eq!(p.feed(1050, remote_media()), []);
        assert_eq!(
            p.feed(1100, Input::Ownership(vec![0])),
            [PauseMedia, HoldA2dp(true)]
        );
        assert_eq!(p.feed(1200, Input::Tick), []);
        assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);

        // A user release acknowledged late (#55), after its timeout.
        let mut p = hold_take();
        assert_eq!(p.feed(700, Input::Release), [SendOwnership(false)]);
        assert_eq!(p.feed(800, remote_media()), []);
        assert_eq!(p.feed(700 + 5001, Input::Tick), []);
        assert_eq!(
            p.feed(5800, Input::Ownership(vec![0])),
            [PauseMedia, HoldA2dp(true)]
        );
    }

    #[test]
    fn hold_remote_source_after_own_release_ack() {
        let mut p = hold_take();
        assert_eq!(p.feed(1000, Input::Release), [SendOwnership(false)]);
        assert_eq!(p.feed(1100, Input::Ownership(vec![0])), [PauseMedia]);
        assert_eq!(p.feed(1200, remote_media()), [HoldA2dp(true)]);
    }

    #[test]
    fn hold_ends_when_the_other_host_leaves() {
        let mut p = held();
        let only = |hosts| Input::ConnectedDevices {
            old: two_hosts(),
            new: hosts,
        };
        // The other host is still there, or the list is empty: still held.
        assert_eq!(p.feed(200, only(two_hosts())), []);
        assert_eq!(p.feed(250, only(vec![])), []);
        assert_eq!(p.feed(300, only(vec![host(LOCAL)])), [HoldA2dp(false)]);
        assert_eq!(p.feed(400, only(vec![host(LOCAL)])), []);
    }

    #[test]
    fn hold_kept_by_host_list_during_pending_take() {
        let mut p = held();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        let hosts = |new| Input::ConnectedDevices {
            old: two_hosts(),
            new,
        };
        let mut all = p.feed(550, hosts(two_hosts()));
        all.extend(p.feed(560, hosts(vec![])));
        all.extend(p.feed(570, hosts(two_hosts())));
        assert_eq!(holds(&all), []);
        // Its ACK still ends the hold.
        assert_eq!(
            holds(&p.feed(600, Input::Ownership(vec![1]))),
            [HoldA2dp(false)]
        );
    }

    /// Held, a remote source seen, a take acknowledged, then the AirPods
    /// list only Linux.
    fn retaken_alone() -> Probe {
        let mut p = held();
        let mut all = p.feed(150, remote_media());
        all.extend(p.play(500, true));
        all.extend(p.feed(600, Input::Ownership(vec![1])));
        assert_eq!(holds(&all), [HoldA2dp(false)]);
        let only_linux = Input::ConnectedDevices {
            old: two_hosts(),
            new: vec![host(LOCAL)],
        };
        assert_eq!(p.feed(700, only_linux), []);
        p
    }

    #[test]
    fn hold_not_resumed_after_the_other_host_left() {
        // A stop Linux keeps the take through (#262), then its loss [0].
        let mut p = retaken_alone();
        let mut all = p.play(1000, false);
        all.extend(p.feed(1100, Input::Ownership(vec![0])));
        // An unsolicited [0] (the `None if was_linux` branch).
        let mut q = retaken_alone();
        all.extend(q.feed(1000, Input::Ownership(vec![0])));
        all.extend(q.play(1100, true));
        for ms in (1200..20_000).step_by(1000) {
            all.extend(q.feed(ms, Input::Tick));
        }
        assert_eq!(holds(&all), []);
    }

    /// `hold_ready(true)` with the AirPods listing only Linux: released.
    fn listed_alone() -> Probe {
        let mut p = hold_ready(true);
        assert_eq!(p.feed(50, report(&[LOCAL, REMOTE], &[LOCAL])), []);
        p
    }

    /// A remote edge from 200 ms, while only Linux is listed, holds once
    /// the other host is listed, and only once (#296).
    fn assert_holds_once_listed(mut p: Probe, edge: &[Input]) {
        for (i, input) in edge.iter().enumerate() {
            assert_eq!(holds(&p.feed(200 + 100 * i as u64, input.clone())), []);
        }
        assert_eq!(
            holds(&p.feed(1000, report(&[LOCAL, REMOTE], &[LOCAL, REMOTE]))),
            [HoldA2dp(true)]
        );
        assert_eq!(
            holds(&p.feed(1100, report(&[LOCAL, REMOTE], &[LOCAL, REMOTE]))),
            []
        );
    }

    #[test]
    fn hold_remote_edge_before_host_listed_audio_source() {
        assert_holds_once_listed(listed_alone(), &[remote_media()]);
        // The same from a hold the host list released.
        let mut p = held();
        assert_eq!(
            p.feed(150, report(&[LOCAL, REMOTE], &[LOCAL])),
            [HoldA2dp(false)]
        );
        assert_holds_once_listed(p, &[remote_media()]);
    }

    #[test]
    fn hold_remote_edge_before_host_listed_unsolicited_loss() {
        let edge = [Input::Ownership(vec![1]), Input::Ownership(vec![0])];
        assert_holds_once_listed(listed_alone(), &edge);
    }

    #[test]
    fn hold_remote_edge_before_host_listed_remote_request() {
        assert_holds_once_listed(listed_alone(), &[Input::RemoteOwnershipRequest]);
    }

    #[test]
    fn hold_remote_request_retry_after_host_left_and_returned() {
        let mut p = hold_take();
        assert_eq!(
            holds(&p.feed(1100, Input::RemoteOwnershipRequest)),
            [HoldA2dp(true)]
        );
        assert_eq!(
            p.feed(1200, report(&[LOCAL, REMOTE], &[LOCAL])),
            [HoldA2dp(false)]
        );
        assert_eq!(holds(&p.feed(1100 + 5001, Input::Tick)), []);
        assert!(matches!(p.core.timed_out_release, Some((Cause::Remote, _))));
        assert_eq!(
            holds(&p.feed(6200, report(&[LOCAL, REMOTE], &[LOCAL, REMOTE]))),
            []
        );
        assert_eq!(
            p.feed(6300, Input::RemoteOwnershipRequest),
            [SendOwnership(false), HoldA2dp(true)]
        );
    }

    #[test]
    fn hold_host_list_alone_adds_no_hold() {
        let mut p = held();
        assert_eq!(
            p.feed(150, report(&[LOCAL, REMOTE], &[LOCAL])),
            [HoldA2dp(false)]
        );
        let mut all = p.feed(200, report(&[LOCAL, REMOTE], &[LOCAL, REMOTE]));
        for ms in (300..10_000).step_by(500) {
            all.extend(p.feed(ms, Input::Tick));
        }
        assert_eq!(holds(&all), []);
        // An empty list after only Linux was listed holds nothing either.
        let mut p = held();
        assert_eq!(
            p.feed(150, report(&[LOCAL, REMOTE], &[LOCAL])),
            [HoldA2dp(false)]
        );
        assert_eq!(holds(&p.feed(200, report(&[LOCAL, REMOTE], &[]))), []);
    }

    /// The c1/c2 sequences of the tests above; every command, in order.
    fn hold_sequences(hold: bool) -> Vec<Command> {
        let mut all = Vec::new();
        let mut start = Probe::new(false, true);
        start.core.a2dp_hold = hold;
        start.routing(ready_facts());
        all.extend(start.feed(1, remote_media()));
        all.extend(start.feed(5, Input::Auto));

        let mut p = hold_ready(hold);
        all.extend(p.play(500, true));
        all.extend(p.feed(600, Input::Ownership(vec![1])));
        all.extend(p.play(1000, false));
        all.extend(p.feed(1200, Input::RemoteOwnershipRequest));
        all.extend(p.feed(1250, Input::Ownership(vec![0])));
        all.extend(p.feed(1300, idle_transport()));
        all.extend(p.play(1500, true));
        all.extend(p.feed(1600, Input::Ownership(vec![1])));
        all.extend(p.feed(1700, Input::Ownership(vec![0])));
        all.extend(p.feed(1800, Input::Disconnected));

        let mut p = hold_ready(hold);
        all.extend(p.feed(100, Input::Ownership(vec![1])));
        all.extend(p.feed(200, Input::Ownership(vec![0])));
        all.extend(p.feed(300, Input::Quit));

        let mut p = hold_ready(hold);
        all.extend(p.feed(100, remote_media()));
        all.extend(p.feed(200, Input::Disconnected));
        all.extend(p.feed(1200, Input::Tick));
        all.extend(p.feed(1300, Input::Reconnected(ready_facts())));

        let mut p = hold_ready(hold);
        all.extend(p.play(500, true));
        all.extend(p.feed(600, Input::Ownership(vec![1])));
        all.extend(p.play(1000, false));
        all.extend(p.feed(1050, remote_media()));
        all.extend(p.feed(1100, Input::Ownership(vec![0])));
        all.extend(p.feed(
            1200,
            Input::ConnectedDevices {
                old: two_hosts(),
                new: vec![host(LOCAL)],
            },
        ));
        all
    }

    #[test]
    fn hold_off_never_emits_hold() {
        assert_eq!(holds(&hold_sequences(false)), []);
        // The twin: the same sequences hold and release with the flag on.
        let on = holds(&hold_sequences(true));
        assert!(on.contains(&HoldA2dp(true)) && on.contains(&HoldA2dp(false)));
    }

    // #272: audio categories.
    const IDLE: u32 = AUDIO_CATEGORY_IDLE;
    const STREAM: u32 = crate::media_controller::AUDIO_CATEGORY_STREAM;
    const MEDIA: u32 = crate::media_controller::AUDIO_CATEGORY_MEDIA;

    fn relay(host: &str, category: u32) -> Input {
        Input::MediaInformation {
            host: host.to_string(),
            category,
        }
    }

    /// `Probe::ready` with REMOTE reported playing media, and its relay of
    /// `relayed` if any.
    fn remote_playing(relayed: Option<u32>) -> Probe {
        let mut p = Probe::ready();
        assert_eq!(p.feed(20, remote_media()), []);
        if let Some(category) = relayed {
            assert_eq!(p.feed(30, relay(REMOTE, category)), []);
        }
        p.core.take_lines();
        p
    }

    #[test]
    fn category_rises_are_fresh_requests() {
        // 100 -> 201 and 100 -> 301 take from an idle owner.
        for category in [STREAM, MEDIA] {
            let mut p = Probe::ready();
            assert_eq!(
                p.play_category(500, category),
                [SendOwnership(true)],
                "{category}"
            );
        }
        // Against a media owner a stream is skipped; MPRIS (201 -> 301) takes.
        let mut p = remote_playing(None);
        assert_eq!(p.play_category(500, STREAM), []);
        assert_eq!(
            p.core.take_lines(),
            ["AUTO takeover skipped: the audio source's category 301 is above Linux's 201"]
        );
        assert_eq!(p.play_category(1000, STREAM), []);
        assert_eq!(p.play_category(1500, MEDIA), [SendOwnership(true)]);
        assert_eq!(p.play_category(2000, MEDIA), []);
    }

    /// #96 with #262: Linux took for MPRIS while a stream counted; neither
    /// stop sends ownership, and only the fall to 100 relays 100.
    #[test]
    fn fresh_source_stop_behind_stream_keeps_ownership() {
        let mut p = remote_playing(Some(MEDIA));
        p.core.category_relay = true;
        assert_eq!(
            p.play_source(500, STREAM, Some(STREAM)),
            [RelayCategory(STREAM)]
        );
        assert_eq!(
            p.play_source(1000, MEDIA, Some(MEDIA)),
            [RelayCategory(MEDIA), SendOwnership(true)]
        );
        assert_eq!(p.feed(1100, Input::Ownership(vec![1])), []);
        // MPRIS stops; the stream still counts.
        assert_eq!(p.play_category(1500, STREAM), [RelayCategory(STREAM)]);
        // The stream stops counting.
        assert_eq!(p.play_category(2000, IDLE), [RelayCategory(IDLE)]);
        p.idle_ticks(2000);
    }

    /// #421: while disconnected with the case closed, a new source behind
    /// playback is new playback and requests the reconnection.
    #[test]
    fn fresh_source_behind_playback_requests_case_reconnect() {
        let mut p = Probe::new(true, false);
        assert_eq!(p.play_category(10, STREAM), []);
        assert_eq!(p.core.take_lines(), Vec::<String>::new());
        assert_eq!(p.play_source(500, STREAM, Some(STREAM)), []);
        assert_eq!(
            p.core.take_lines(),
            ["AUTO new playback requested reconnection after case closure"]
        );
        assert_eq!(p.feed(600, Input::Tick), [Reconnect { audio: true }]);
    }

    /// `Probe::ready` with the case closed, then disconnected at 100.
    fn case_closed_disconnected() -> Probe {
        let mut p = Probe::ready();
        p.feed(20, Input::EarState(vec![InCase, InCase]));
        assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
        p.core.take_lines();
        p
    }

    /// `ready_facts` with the other host playing media (301).
    fn remote_media_facts() -> SessionFacts {
        SessionFacts {
            audio: Some(source(REMOTE, AudioSourceType::Media)),
            ..ready_facts()
        }
    }

    /// From `case_closed_disconnected`, the playback samples
    /// `(category, new_source)` from 200 ms on, then the reconnect they
    /// requested with `facts`: its commands and lines.
    fn requested_reconnect(
        samples: &[(u32, Option<u32>)],
        facts: SessionFacts,
    ) -> (Vec<Command>, Vec<String>) {
        let mut p = case_closed_disconnected();
        let mut ms = 200;
        for &(category, new_source) in samples {
            assert_eq!(p.play_source(ms, category, new_source), []);
            ms += 500;
        }
        assert_eq!(p.feed(ms, Input::Tick), [Reconnect { audio: true }]);
        p.core.take_lines();
        let out = p.feed(ms + 100, Input::Reconnected(facts));
        (out, p.core.take_lines())
    }

    /// #421 with #96 c4: the reconnect take compares at the behind source's
    /// category, not the aggregate's.
    #[test]
    fn case_reconnect_takes_at_behind_source_category() {
        let blocked =
            "AUTO reconnected; takeover blocked by remote activity, wear, or missing host state";
        // The aggregate falls to the stream source's 201, or stays at 301
        // with the 201 source behind it.
        for samples in [
            [(MEDIA, None), (STREAM, Some(STREAM))],
            [(MEDIA, None), (MEDIA, Some(STREAM))],
        ] {
            let (out, lines) = requested_reconnect(&samples, remote_media_facts());
            assert_eq!(out, [AdoptSession, Announce, DrainPlayback], "{samples:?}");
            assert!(lines.contains(&blocked.into()), "{lines:?}");
            assert_eq!(
                requested_reconnect(&samples, ready_facts()).0,
                [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
            );
        }
    }

    /// #421: a start or rise counts as the aggregate, the highest request
    /// wins, the aggregate caps it, and a withdrawal forgets it.
    #[test]
    fn case_reconnect_category_follows_requests() {
        let takes = |samples: &[(u32, Option<u32>)]| {
            requested_reconnect(samples, remote_media_facts())
                .0
                .contains(&SendOwnership(true))
        };
        // (a) A rise to 301 after a 201 source asked.
        assert!(takes(&[
            (STREAM, None),
            (STREAM, Some(STREAM)),
            (MEDIA, None)
        ]));
        // (b) A 301 source asked, then the aggregate fell to 201.
        assert!(!takes(&[
            (MEDIA, None),
            (MEDIA, Some(MEDIA)),
            (STREAM, None)
        ]));
        // (c) A 201 source asked, the stop withdrew it, a 301 start asked.
        assert!(takes(&[
            (STREAM, None),
            (STREAM, Some(STREAM)),
            (IDLE, None),
            (MEDIA, None)
        ]));
        // (d) A 301 start asked, then a 201 source: the higher request wins.
        assert!(takes(&[(IDLE, None), (MEDIA, None), (MEDIA, Some(STREAM))]));
    }

    /// #421: steady playback while disconnected requests nothing.
    #[test]
    fn steady_playback_while_case_closed_requests_nothing() {
        let mut p = case_closed_disconnected();
        for ms in (200..5000).step_by(500) {
            assert_eq!(p.play_category(ms, STREAM), []);
            assert_eq!(p.play_source(ms + 250, STREAM, None), []);
        }
        assert_eq!(p.core.take_lines(), Vec::<String>::new());
        for ms in (5000..=70_000).step_by(100) {
            assert_eq!(p.feed(ms, Input::Tick), [], "{ms}");
        }
    }

    /// #96: a new stream behind MPRIS during Auto's pending release is
    /// decided at 201 once the release settles, not at Linux's 301.
    #[test]
    fn fresh_source_during_release() {
        let run = |owner| {
            // No wear report at Auto, so no connect take (#324).
            let mut p = Probe::new(false, true);
            p.routing(SessionFacts {
                owner: Some(vec![1]),
                ..unworn_facts()
            });
            assert_eq!(
                p.feed(100, Input::Auto),
                [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
            );
            p.wear_in(140);
            assert_eq!(p.feed(150, remote_media()), []);
            assert_eq!(p.feed(160, relay(REMOTE, owner)), []);
            assert_eq!(p.play_category(200, MEDIA), []);
            assert_eq!(p.play_source(500, MEDIA, Some(STREAM)), []);
            p.core.take_lines();
            let ack = p.feed(600, Input::Ownership(vec![0]));
            (ack, p.core.take_lines())
        };
        let (ack, lines) = run(MEDIA);
        assert_eq!(ack, []);
        assert_eq!(count_lines(&lines, "AUTO takeover blocked"), 1);
        let (ack, lines) = run(IDLE);
        assert_eq!(ack, [SendOwnership(true)]);
        assert_eq!(
            count_lines(
                &lines,
                "AUTO playback restarted during the release; requested Linux ownership"
            ),
            1
        );
    }

    #[test]
    fn category_rise_while_holding_the_take_sends_nothing() {
        let mut p = Probe::ready();
        assert_eq!(p.play_category(500, STREAM), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.play_category(1000, MEDIA), []);
    }

    #[test]
    fn category_fall_to_stream_is_no_stop() {
        let mut p = Probe::ready();
        assert_eq!(p.play_category(500, MEDIA), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        // 301 -> 201: no release, however long it lasts.
        assert_eq!(p.play_category(1000, STREAM), []);
        p.idle_ticks(1000);
        // 201 -> 100 is a stop: Linux keeps the AirPods idle (#262).
        assert_eq!(p.play_category(62_000, IDLE), []);
        p.idle_ticks(62_000);
    }

    #[test]
    fn category_relay_beats_stale_report() {
        // REMOTE's relay of 100 beats its media report: a stream takes.
        let mut p = remote_playing(Some(IDLE));
        assert_eq!(p.play_category(500, STREAM), [SendOwnership(true)]);
        // Another host's relay does not stand for REMOTE's.
        let mut p = remote_playing(None);
        assert_eq!(p.feed(40, relay(REMOTE_2, IDLE)), []);
        assert_eq!(p.play_category(500, STREAM), []);
    }

    #[test]
    fn category_relays_cleared_on_disconnect_and_session_restart() {
        let mut p = remote_playing(Some(IDLE));
        p.feed(40, Input::Disconnected);
        assert!(p.core.relays.is_empty());
        // A relay fed after the drop is gone once the next session starts.
        p.feed(50, relay(REMOTE, IDLE));
        assert_eq!(p.feed(1100, Input::Tick), [Reconnect { audio: true }]);
        p.feed(
            1200,
            Input::Reconnected(SessionFacts {
                audio: Some(source(REMOTE, AudioSourceType::Media)),
                ..ready_facts()
            }),
        );
        assert!(p.core.relays.is_empty());
        // So the media report blocks a stream again.
        assert_eq!(p.play_category(1300, IDLE), []);
        assert_eq!(p.play_category(1500, STREAM), []);
    }

    /// `Probe::ready`, relaying its category.
    fn relaying() -> Probe {
        let mut p = Probe::ready();
        p.core.category_relay = true;
        p
    }

    #[test]
    fn category_relay_each_change_in_automatic_mode() {
        let mut p = relaying();
        assert_eq!(
            p.play_category(500, STREAM),
            [RelayCategory(STREAM), SendOwnership(true)]
        );
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.play_category(1000, STREAM), []);
        assert_eq!(p.play_category(1500, MEDIA), [RelayCategory(MEDIA)]);
        assert_eq!(p.play_category(2000, IDLE), [RelayCategory(IDLE)]);
        // Relayed whether or not Linux wins.
        let mut p = remote_playing(None);
        p.core.category_relay = true;
        assert_eq!(p.play_category(500, STREAM), [RelayCategory(STREAM)]);
    }

    #[test]
    fn category_relay_none_in_manual_mode() {
        let mut p = relaying();
        p.feed(100, Input::Manual);
        for (ms, category) in [(500, STREAM), (1000, MEDIA), (1500, IDLE)] {
            assert_eq!(p.play_category(ms, category), [], "{category}");
        }
    }

    #[test]
    fn category_relay_none_during_a_call() {
        let mut p = relaying();
        assert_eq!(p.feed(100, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.play_category(500, STREAM), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.play_category(1000, MEDIA), []);
        assert_eq!(p.play_category(1500, IDLE), []);
    }

    #[test]
    fn category_relay_once_at_call_end_unless_idle() {
        let mut p = relaying();
        assert_eq!(p.feed(100, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.play_category(500, MEDIA), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(
            p.feed(1000, Input::LocalCall(false)),
            [AdvertiseCall(false), RelayCategory(MEDIA)]
        );
        assert_eq!(p.play_category(1500, MEDIA), []);
        // Idle when the call ends: only the call's own advert.
        let mut p = relaying();
        assert_eq!(p.feed(100, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(
            p.feed(1000, Input::LocalCall(false)),
            [AdvertiseCall(false)]
        );
    }

    #[test]
    fn category_held_takeover_command_lists() {
        // The production setup: A2DP hold and category relay on.
        let mut p = hold_ready(true);
        p.core.category_relay = true;
        p.core.take_lines();
        assert_eq!(p.feed(100, remote_media()), [HoldA2dp(true)]);
        assert_eq!(p.play_category(500, STREAM), [RelayCategory(STREAM)]);
        assert_eq!(
            p.core.take_lines(),
            ["AUTO takeover skipped: the audio source's category 301 is above Linux's 201"]
        );
        assert_eq!(
            p.play_category(1000, MEDIA),
            [RelayCategory(MEDIA), SendOwnership(true)]
        );
        // The ack releases the hold before it routes.
        assert_eq!(
            p.feed(1100, Input::Ownership(vec![1])),
            [HoldA2dp(false), RouteToAirPods]
        );
    }

    // #299: an announcement carries category 100; Linux's own follows it.
    #[test]
    fn category_relay_after_auto_toggle_at_301() {
        let mut p = relaying();
        p.feed(100, Input::Manual);
        assert_eq!(p.play_category(500, MEDIA), []);
        assert_eq!(
            p.feed(1000, Input::Auto),
            [
                ConnectA2dp,
                // The idle other host: the connect take (#324).
                SendOwnership(true),
                Announce,
                RelayCategory(MEDIA),
                DrainPlayback
            ]
        );
        assert_eq!(p.play_category(1500, MEDIA), []);
    }

    #[test]
    fn category_relay_after_new_host_at_201() {
        let mut p = relaying();
        assert_eq!(
            p.play_category(500, STREAM),
            [RelayCategory(STREAM), SendOwnership(true)]
        );
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        let three = vec![host(LOCAL), host(REMOTE), host(REMOTE_2)];
        assert_eq!(
            p.feed(
                1000,
                Input::ConnectedDevices {
                    old: two_hosts(),
                    new: three,
                },
            ),
            [Announce, RelayCategory(STREAM)]
        );
    }

    #[test]
    fn category_relay_after_reconnect_adoption() {
        // 301 set before the disconnect.
        let mut p = relaying();
        assert_eq!(
            p.play_category(500, MEDIA),
            [RelayCategory(MEDIA), SendOwnership(true)]
        );
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert_eq!(p.feed(1100, Input::Disconnected), [PauseMedia, DropSession]);
        assert_eq!(p.feed(2200, Input::Tick), [Reconnect { audio: true }]);
        // The idle other host: the connect take (#324) after the relay.
        assert_eq!(
            p.feed(2300, Input::Reconnected(ready_facts())),
            [
                AdoptSession,
                Announce,
                RelayCategory(MEDIA),
                SendOwnership(true),
                DrainPlayback
            ]
        );
        assert_eq!(p.play_category(2500, MEDIA), []);
        // 301 set while disconnected.
        let mut p = relaying();
        assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
        assert_eq!(p.play_category(500, MEDIA), []);
        assert_eq!(p.feed(1200, Input::Tick), [Reconnect { audio: true }]);
        assert_eq!(
            p.feed(1300, Input::Reconnected(ready_facts())),
            [
                AdoptSession,
                Announce,
                RelayCategory(MEDIA),
                SendOwnership(true),
                DrainPlayback
            ]
        );
        assert_eq!(p.play_category(1500, MEDIA), []);
    }

    // The idle other host also gets the connect take (#324).
    #[test]
    fn category_relay_after_announce_none_when_idle() {
        let mut p = relaying();
        p.feed(100, Input::Manual);
        assert_eq!(
            p.feed(1000, Input::Auto),
            [ConnectA2dp, SendOwnership(true), Announce, DrainPlayback]
        );
    }

    #[test]
    fn category_relay_after_announce_none_when_disabled() {
        let mut p = Probe::ready();
        p.feed(100, Input::Manual);
        assert_eq!(p.play_category(500, MEDIA), []);
        assert_eq!(
            p.feed(1000, Input::Auto),
            [ConnectA2dp, SendOwnership(true), Announce, DrainPlayback]
        );
    }

    #[test]
    fn category_relay_after_announce_none_during_call() {
        let mut p = relaying();
        assert_eq!(p.feed(100, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.play_category(500, MEDIA), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        let three = vec![host(LOCAL), host(REMOTE), host(REMOTE_2)];
        assert_eq!(
            p.feed(
                1000,
                Input::ConnectedDevices {
                    old: two_hosts(),
                    new: three,
                },
            ),
            [Announce, AdvertiseCall(true)]
        );
    }

    // #301: ear detection's resume while another host owns.
    const GRACE_MS: u64 = EAR_RESUME_GRACE.as_millis() as u64;

    /// `remote_playing(Some(relayed))`: another host owns, Linux holds no
    /// take; a bud out at 100 ms, and back in at 200 ms if `reinsert`.
    fn bud_out(relayed: u32, reinsert: bool) -> Probe {
        let mut p = remote_playing(Some(relayed));
        assert!(p.core.remote_owns && !p.core.requested_take);
        assert_eq!(p.feed(100, Input::EarState(vec![InEar, OutOfEar])), []);
        if reinsert {
            assert_eq!(p.feed(200, Input::EarState(vec![InEar, InEar])), []);
        }
        p
    }

    fn takes(all: &[Command]) -> usize {
        all.iter().filter(|c| **c == SendOwnership(true)).count()
    }

    /// The resumed play, after the removal's late stop sample, then playing
    /// samples and Ticks past the grace: no take.
    fn ear_resume_does_not_take(relayed: u32) {
        let mut p = bud_out(relayed, true);
        let mut all = p.play_category(300, IDLE);
        all.extend(p.play_category(700, MEDIA));
        assert_eq!(p.core.take_lines(), [EAR_RESUMED]);
        for ms in (1200..=200 + 2 * GRACE_MS).step_by(500) {
            all.extend(p.play_category(ms, MEDIA));
            all.extend(p.feed(ms, Input::Tick));
        }
        assert_eq!(takes(&all), 0, "{relayed}: {all:?}");
    }

    #[test]
    fn ear_resume_while_remote_owns_does_not_take() {
        ear_resume_does_not_take(MEDIA);
    }

    #[test]
    fn ear_resume_while_remote_idle_does_not_take() {
        ear_resume_does_not_take(IDLE);
    }

    #[test]
    fn play_without_ear_resume_takes() {
        for relayed in [MEDIA, IDLE] {
            let mut p = bud_out(relayed, false);
            let mut all = p.play_category(700, MEDIA);
            all.extend(p.play_category(1200, MEDIA));
            assert_eq!(takes(&all), 1, "{relayed}: {all:?}");
        }
    }

    #[test]
    fn ear_resume_grace_expires() {
        let mut p = bud_out(MEDIA, true);
        let mut all = p.play_category(700, MEDIA);
        all.extend(p.play_category(1200, IDLE));
        all.extend(p.feed(1200, Input::Tick));
        assert_eq!(takes(&all), 0, "{all:?}");
        // A fresh play sampled after the grace takes.
        let after = 200 + GRACE_MS + 1;
        assert_eq!(p.play_category(after, MEDIA), [SendOwnership(true)]);
        assert_eq!(p.play_category(after + 500, MEDIA), []);
    }

    #[test]
    fn ear_resume_while_linux_owns_unchanged() {
        let mut p = remote_playing(Some(IDLE));
        assert_eq!(p.play_category(500, MEDIA), [SendOwnership(true)]);
        // Out and in while the take is pending, then while Linux owns.
        p.feed(520, Input::EarState(vec![InEar, OutOfEar]));
        p.feed(540, Input::EarState(vec![InEar, InEar]));
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
        assert!(!p.core.remote_owns);
        assert_eq!(p.feed(700, Input::EarState(vec![InEar, OutOfEar])), []);
        assert_eq!(p.play_category(1000, IDLE), []);
        assert_eq!(p.feed(1100, Input::EarState(vec![InEar, InEar])), []);
        assert_eq!(p.core.ear_resume_until, None);
        p.core.take_lines();
        assert_eq!(p.play_category(1200, MEDIA), []);
        assert_eq!(
            p.core.take_lines(),
            ["AUTO playback started; Linux already owns"]
        );
        assert_eq!(p.play_category(1500, IDLE), []);
        p.idle_ticks(1500);
    }

    #[test]
    fn ear_resume_after_late_stop_does_not_take() {
        // Linux's player already plays at the baseline while the remote owns.
        let mut p = remote_playing(Some(MEDIA));
        p.feed(30, Input::Auto);
        assert_eq!(p.play_category(40, MEDIA), []);
        p.feed(100, Input::EarState(vec![InEar, OutOfEar]));
        p.feed(200, Input::EarState(vec![InEar, InEar]));
        // The removal's pause is polled after the reinsertion.
        assert_eq!(p.play_category(300, IDLE), []);
        assert_eq!(p.play_category(800, MEDIA), []);
        assert!(p.core.take_lines().contains(&EAR_RESUMED.to_string()));
    }

    #[test]
    fn ear_resume_grace_covers_poll() {
        assert!(
            EAR_RESUME_GRACE
                >= crate::switching::PLAYBACK_POLL + crate::stream_monitor::MIN_STREAM_PLAYBACK
        );
    }

    #[test]
    fn ear_resume_grace_cleared() {
        let ended: [(&str, fn(&mut Probe)); 4] = [
            ("take", |p| {
                // The iPhone stopped, so a manual take is allowed.
                p.feed(250, Input::AudioSource(idle()));
                assert_eq!(p.feed(300, Input::Take), [SendOwnership(true)])
            }),
            ("owner linux", |p| {
                p.feed(300, Input::Ownership(vec![1]));
            }),
            ("session lost", |p| {
                p.feed(300, Input::SessionLost);
            }),
            ("disconnected", |p| {
                p.feed(300, Input::Disconnected);
            }),
        ];
        for (name, end) in ended {
            let mut p = bud_out(IDLE, true);
            assert!(p.core.ear_resume_until.is_some(), "{name}");
            end(&mut p);
            assert_eq!(p.core.ear_resume_until, None, "{name}");
        }
    }

    #[test]
    fn ear_resume_during_auto_release_does_not_take() {
        let timed_out = 100 + ACK_TIMEOUT.as_millis() as u64 + 100;
        for (name, ms, settle) in [
            ("ack", 1100, Input::Ownership(vec![0])),
            ("timeout", timed_out, Input::Tick),
        ] {
            // Auto releases the AirPods Linux owned while the iPhone plays;
            // no wear report yet, so no connect take (#324).
            let mut p = Probe::new(false, true);
            p.routing(SessionFacts {
                owner: Some(vec![1]),
                ..unworn_facts()
            });
            let mut all = p.feed(100, Input::Auto);
            assert_eq!(takes(&all), 0, "{name}: {all:?}");
            p.wear_in(140);
            all.extend(p.feed(150, remote_media()));
            all.extend(p.feed(160, relay(REMOTE, MEDIA)));
            all.extend(p.play_category(200, MEDIA));
            all.extend(p.feed(300, Input::EarState(vec![InEar, OutOfEar])));
            all.extend(p.feed(400, Input::EarState(vec![InEar, InEar])));
            all.extend(p.play_category(500, IDLE));
            all.extend(p.play_category(1000, MEDIA));
            all.extend(p.feed(ms, settle));
            assert!(p.core.pending.is_none(), "{name}");
            assert_eq!(takes(&all), 0, "{name}: {all:?}");
            assert!(
                p.core.take_lines().contains(&EAR_RESUMED.to_string()),
                "{name}"
            );
        }
    }

    #[test]
    fn ear_resume_grace_needs_remote_owner() {
        let mut p = bud_out(IDLE, true);
        // The iPhone stopped, left (only Linux listed) and came back idle:
        // nobody else owns the AirPods.
        p.feed(240, Input::AudioSource(idle()));
        for (ms, new) in [(250, vec![host(LOCAL)]), (260, two_hosts())] {
            let old = p.core.hosts.clone();
            p.feed(ms, Input::ConnectedDevices { old, new });
        }
        assert!(!p.core.remote_owns && p.core.ear_resume_until.is_some());
        p.core.take_lines();
        // A play within the grace is not ear detection's resume.
        assert_eq!(p.play_category(700, MEDIA), [SendOwnership(true)]);
        assert!(!p.core.take_lines().contains(&EAR_RESUMED.to_string()));
    }

    // #313: the shell's report on its ear-detection resume.
    fn resumed(p: &mut Probe, ms: u64, resumed: bool) {
        assert_eq!(p.feed(ms, Input::EarResume { resumed }), []);
    }

    #[test]
    fn ear_reinsert_without_resume_play_takes() {
        for relayed in [MEDIA, IDLE] {
            let mut p = bud_out(relayed, true);
            resumed(&mut p, 250, false);
            let mut all = p.play_category(700, MEDIA);
            all.extend(p.play_category(1200, MEDIA));
            assert_eq!(takes(&all), 1, "{relayed}: {all:?}");
            assert!(!p.core.take_lines().contains(&EAR_RESUMED.to_string()));
        }
    }

    #[test]
    fn ear_reinsert_without_resume_during_auto_release() {
        let timed_out = 100 + ACK_TIMEOUT.as_millis() as u64 + 100;
        for (name, ms, settle) in [
            ("ack", 1100, Input::Ownership(vec![0])),
            ("timeout", timed_out, Input::Tick),
        ] {
            // ear_resume_during_auto_release_does_not_take's sequence, with
            // the report after the reinsertion, or with no reinsertion.
            let run = |reinsert: bool| {
                let mut p = Probe::new(false, true);
                p.routing(SessionFacts {
                    owner: Some(vec![1]),
                    ..unworn_facts()
                });
                let mut all = p.feed(100, Input::Auto);
                p.wear_in(140);
                all.extend(p.feed(150, remote_media()));
                all.extend(p.feed(160, relay(REMOTE, MEDIA)));
                all.extend(p.play_category(200, MEDIA));
                all.extend(p.feed(300, Input::EarState(vec![InEar, OutOfEar])));
                if reinsert {
                    all.extend(p.feed(400, Input::EarState(vec![InEar, InEar])));
                    all.extend(p.feed(450, Input::EarResume { resumed: false }));
                }
                all.extend(p.play_category(500, IDLE));
                all.extend(p.play_category(1000, MEDIA));
                assert!(p.core.play_during_release.is_some(), "{name}");
                all.extend(p.feed(ms, settle.clone()));
                let lines = p.core.take_lines();
                assert!(!lines.contains(&EAR_RESUMED.to_string()), "{name}");
                all
            };
            assert_eq!(run(true), run(false), "{name}");
        }
    }

    #[test]
    fn ear_resume_reported_still_does_not_take() {
        for relayed in [MEDIA, IDLE] {
            let mut p = bud_out(relayed, true);
            resumed(&mut p, 250, true);
            let mut all = p.play_category(700, MEDIA);
            all.extend(p.play_category(1200, MEDIA));
            assert_eq!(takes(&all), 0, "{relayed}: {all:?}");
            assert_eq!(p.core.take_lines(), [EAR_RESUMED]);
        }
    }

    #[test]
    fn ear_resume_play_before_report_does_not_take() {
        let mut p = bud_out(MEDIA, true);
        assert_eq!(p.play_category(700, MEDIA), []);
        assert_eq!(p.core.take_lines(), [EAR_RESUMED]);
        resumed(&mut p, 800, false);
        assert_eq!(p.play_category(1200, MEDIA), []);
    }

    #[test]
    fn ear_resume_stale_report_keeps_later_grace() {
        let mut p = bud_out(MEDIA, true);
        p.feed(300, Input::EarState(vec![InEar, OutOfEar]));
        p.feed(400, Input::EarState(vec![InEar, InEar]));
        // The first reinsertion's report: the second is still due one.
        resumed(&mut p, 450, false);
        assert_eq!(p.play_category(700, MEDIA), []);
        assert_eq!(p.core.take_lines(), [EAR_RESUMED]);
        resumed(&mut p, 800, false);
        assert_eq!(p.play_category(900, IDLE), []);
        assert_eq!(p.play_category(1000, MEDIA), [SendOwnership(true)]);

        // A stray report changes nothing.
        let mut p = remote_playing(Some(MEDIA));
        resumed(&mut p, 100, false);
        assert_eq!(p.core.ear_resume_until, None);
        assert_eq!(p.core.ear_reports_due, 0);
    }

    #[test]
    fn ear_resume_stray_report_after_true_keeps_grace() {
        let mut p = bud_out(MEDIA, true);
        resumed(&mut p, 250, true);
        resumed(&mut p, 300, false);
        assert!(p.core.ear_resume_until.is_some());
        assert_eq!(p.play_category(700, MEDIA), []);
        assert_eq!(p.core.take_lines(), [EAR_RESUMED]);
    }

    #[test]
    fn ear_resume_two_step_reinsertion_does_not_take() {
        let mut p = remote_playing(Some(MEDIA));
        p.feed(100, Input::EarState(vec![OutOfEar, OutOfEar]));
        // The first bud resumes the paused player; the second finds none.
        p.feed(200, Input::EarState(vec![InEar, OutOfEar]));
        resumed(&mut p, 250, true);
        p.feed(300, Input::EarState(vec![InEar, InEar]));
        resumed(&mut p, 350, false);
        assert_eq!(p.play_category(700, MEDIA), []);
        assert_eq!(p.core.take_lines(), [EAR_RESUMED]);

        // Past that grace, a fresh one ends on its own `false` report.
        let after = 300 + GRACE_MS + 100;
        assert_eq!(p.play_category(after, IDLE), []);
        p.feed(after + 100, Input::EarState(vec![InEar, OutOfEar]));
        p.feed(after + 200, Input::EarState(vec![InEar, InEar]));
        resumed(&mut p, after + 250, false);
        assert_eq!(p.play_category(after + 300, MEDIA), [SendOwnership(true)]);
    }

    // #324: the connect take from an idle other host.

    const WINDOW_MS: u64 = CONNECT_TAKE_WINDOW.as_millis() as u64;

    /// A manual live session seeded with `facts` (default sink routing on,
    /// an idle A2DP transport, `hold` for #264), then the session start's
    /// `Auto` at 10 ms; returns its commands.
    fn session_start(facts: SessionFacts, hold: bool) -> (Probe, Vec<Command>) {
        let mut p = Probe::new(false, true);
        p.core.default_sink_routing = true;
        p.core.a2dp_hold = hold;
        p.routing(facts);
        assert_eq!(p.feed(5, idle_transport()), []);
        p.core.take_lines();
        let out = p.feed(10, Input::Auto);
        (p, out)
    }

    /// `session_start` with idle facts and owner [0], its take acked at
    /// 100 ms.
    fn acked_connect_take(hold: bool) -> Probe {
        let (mut p, _) = session_start(ready_facts(), hold);
        assert_eq!(p.feed(100, Input::Ownership(vec![1])), [RouteToAirPods]);
        p
    }

    /// An automatic live session that drops at 0 ms and reconnects at
    /// 1100 ms with `facts`; returns the reconnect's commands.
    fn ordinary_reconnect(p: &mut Probe, facts: SessionFacts) -> Vec<Command> {
        assert_eq!(p.feed(0, Input::Disconnected), [DropSession]);
        assert_eq!(p.feed(1000, Input::Tick), [Reconnect { audio: true }]);
        p.feed(1100, Input::Reconnected(facts))
    }

    #[test]
    fn session_start_take_with_idle_remote_owner() {
        for (owner, hold) in [(None, false), (Some(vec![0]), false), (Some(vec![0]), true)] {
            let name = format!("owner={owner:?} hold={hold}");
            let (mut p, auto) = session_start(
                SessionFacts {
                    owner,
                    ..ready_facts()
                },
                hold,
            );
            let mut expected = if hold { vec![HoldA2dp(false)] } else { vec![] };
            expected.extend([ConnectA2dp, SendOwnership(true), Announce, DrainPlayback]);
            assert_eq!(auto, expected, "{name}");
            assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 1, "{name}");
            // The idle take routes but leaves the A2DP resume to a play edge.
            assert_eq!(
                p.feed(100, Input::Ownership(vec![1])),
                [RouteToAirPods],
                "{name}"
            );
        }
    }

    #[test]
    fn connect_take_with_idle_remote_owner() {
        for (owner, hold) in [
            (None, false),
            (Some(vec![0]), false),
            (Some(vec![1]), false),
            (Some(vec![0]), true),
        ] {
            let name = format!("owner={owner:?} hold={hold}");
            let mut p = Probe::new(true, true);
            p.core.a2dp_hold = hold;
            let out = ordinary_reconnect(
                &mut p,
                SessionFacts {
                    owner,
                    ..ready_facts()
                },
            );
            let mut expected = vec![AdoptSession];
            if hold {
                expected.push(HoldA2dp(false));
            }
            expected.extend([Announce, SendOwnership(true), DrainPlayback]);
            assert_eq!(out, expected, "{name}");
            assert!(p.core.reconnect_take_pending(), "{name}");
            assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 1, "{name}");
        }
        // A playback reconnect sends its one take, not a second one.
        let mut p = Probe::new(true, false);
        p.core = p.core.with_playback_requested();
        let out = p.feed(100, Input::Reconnected(ready_facts()));
        assert_eq!(takes(&out), 1, "{out:?}");
        assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 0);
    }

    #[test]
    fn connect_take_ignores_stale_init_ownership() {
        let mut p = Probe::new(true, true);
        ordinary_reconnect(&mut p, ready_facts());
        p.core.take_lines();
        let mut all = p.feed(1200, Input::Ownership(vec![0]));
        all.extend(p.feed(1300, Input::InitBacklogDrained));
        all.extend(p.feed(1400, Input::Ownership(vec![1])));
        assert_eq!(all, []);
        let lines = p.core.take_lines();
        assert_eq!(count_lines(&lines, "ACK owner=linux"), 1);
        assert_eq!(count_lines(&lines, "YIELD"), 0);
    }

    #[test]
    fn session_start_keeps_reported_linux_ownership() {
        let linux = || SessionFacts {
            owner: Some(vec![1]),
            ..ready_facts()
        };
        let (mut p, auto) = session_start(linux(), false);
        assert_eq!(auto, [ConnectA2dp, RouteToAirPods, Announce, DrainPlayback]);
        assert!(p.core.pending.is_none());
        assert!(p.core.requested_take && p.core.last_owner_linux);
        assert_eq!(p.play(100, false), []);
        p.idle_ticks(100);
        p.core.take_lines();
        // The deferred resume runs on the first play edge, once.
        assert_eq!(p.play(61_000, true), [ResumeA2dp]);
        assert_eq!(
            count_lines(
                &p.core.take_lines(),
                "AUTO playback started; Linux already owns"
            ),
            1
        );
        assert_eq!(p.play(61_500, false), []);
        assert_eq!(p.play(62_000, true), []);
        // A remote media report after the [1] cleared the last owner and
        // asks for the hold; its relay of 100 still lets Linux keep the
        // AirPods, so it is the last owner again and holds nothing.
        let mut p = Probe::new(false, true);
        p.core.a2dp_hold = true;
        p.routing(linux());
        p.feed(
            1,
            Input::AudioSource(source(REMOTE, AudioSourceType::Media)),
        );
        p.feed(2, relay(REMOTE, IDLE));
        assert!(!p.core.last_owner_linux);
        assert_eq!(
            p.feed(10, Input::Auto),
            [HoldA2dp(false), ConnectA2dp, Announce, DrainPlayback]
        );
        assert!(p.core.requested_take && p.core.last_owner_linux);
        // c1's conditions fail (no bud in): today's tracked release (#54).
        let (p, auto) = session_start(
            SessionFacts {
                ears: vec![OutOfEar, OutOfEar],
                ..linux()
            },
            false,
        );
        assert_eq!(
            auto,
            [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
        );
        assert!(p.core.pending.is_some());
    }

    #[test]
    fn connect_take_ignores_relay_age() {
        // #325 q1 was not run: every connect kind takes alike, however long
        // ago the other host relayed 100.
        const MINUTES: u64 = 10 * 60_000;
        let mut p = Probe::new(false, true);
        p.routing(ready_facts());
        p.feed(0, relay(REMOTE, IDLE));
        assert_eq!(takes(&p.feed(MINUTES, Input::Auto)), 1);
        let mut p = Probe::new(true, true);
        p.routing(ready_facts());
        p.feed(0, relay(REMOTE, IDLE));
        assert_eq!(p.feed(MINUTES, Input::Disconnected), [DropSession]);
        assert_eq!(
            p.feed(MINUTES + 1000, Input::Tick),
            [Reconnect { audio: true }]
        );
        assert_eq!(
            takes(&p.feed(MINUTES + 1100, Input::Reconnected(ready_facts()))),
            1
        );
        // An old relay of 100 beats the stale media report at Auto.
        let mut p = Probe::new(false, true);
        p.routing(SessionFacts {
            audio: Some(source(REMOTE, AudioSourceType::Media)),
            ..ready_facts()
        });
        p.feed(0, relay(REMOTE, IDLE));
        assert_eq!(takes(&p.feed(MINUTES, Input::Auto)), 1);
    }

    #[test]
    fn connect_take_from_relayed_idle_media_holds_nothing() {
        // The media report asked for the hold, but its relay of 100 lets
        // Linux take: the card is not held off and the ACK only routes.
        let mut p = Probe::new(false, true);
        p.core.default_sink_routing = true;
        p.core.a2dp_hold = true;
        p.routing(SessionFacts {
            audio: Some(source(REMOTE, AudioSourceType::Media)),
            ..ready_facts()
        });
        p.feed(5, relay(REMOTE, IDLE));
        assert_eq!(p.feed(6, idle_transport()), []);
        assert_eq!(
            p.feed(10, Input::Auto),
            [
                HoldA2dp(false),
                ConnectA2dp,
                SendOwnership(true),
                Announce,
                DrainPlayback
            ]
        );
        assert_eq!(p.feed(100, Input::Ownership(vec![1])), [RouteToAirPods]);
    }

    #[test]
    fn connect_take_first_sample_before_ack_resumes() {
        // Linux already plays at connect (c10 a): the ACK resumes once.
        for owner in [None, Some(vec![0])] {
            let facts = SessionFacts {
                owner: owner.clone(),
                ..ready_facts()
            };
            let (mut p, _) = session_start(facts, false);
            assert_eq!(p.play(50, true), [], "{owner:?}");
            assert_eq!(
                p.feed(100, Input::Ownership(vec![1])),
                [RouteToAirPods, ResumeA2dp],
                "{owner:?}"
            );
            assert_eq!(p.play(200, false), []);
            assert_eq!(p.play(700, true), [], "{owner:?}");
        }
        // A streaming transport has nothing to resume.
        let (mut p, _) = session_start(ready_facts(), false);
        assert_eq!(p.feed(20, Input::A2dpTransport { active: true }), []);
        assert_eq!(p.play(50, true), []);
        assert_eq!(p.feed(100, Input::Ownership(vec![1])), [RouteToAirPods]);
        assert_eq!(p.play(200, false), []);
        assert_eq!(p.play(700, true), []);
        // A stop before the ACK still defers to the next play edge (#205).
        let (mut p, _) = session_start(ready_facts(), false);
        assert_eq!(p.play(50, true), []);
        assert_eq!(p.play(70, false), []);
        assert_eq!(p.feed(100, Input::Ownership(vec![1])), [RouteToAirPods]);
        assert_eq!(p.play(700, true), [ResumeA2dp]);
        assert_eq!(p.play(1200, false), []);
        assert_eq!(p.play(1700, true), []);
    }

    #[test]
    fn connect_take_first_sample_after_ack_resumes() {
        let linux = || SessionFacts {
            owner: Some(vec![1]),
            ..ready_facts()
        };
        // The acked take (c10 b) and the keep path at owner [1] (c10 c).
        for mut p in [acked_connect_take(false), session_start(linux(), false).0] {
            p.core.take_lines();
            assert_eq!(p.play(200, true), [ResumeA2dp]);
            assert_eq!(
                count_lines(
                    &p.core.take_lines(),
                    "AUTO playback started; Linux already owns"
                ),
                1
            );
            assert_eq!(p.play(700, false), []);
            assert_eq!(p.play(1200, true), []);
        }
        // A streaming transport has nothing to resume.
        for mut p in [acked_connect_take(false), session_start(linux(), false).0] {
            assert_eq!(p.feed(150, Input::A2dpTransport { active: true }), []);
            assert_eq!(p.play(200, true), []);
            assert_eq!(p.play(700, false), []);
            assert_eq!(p.play(1200, true), []);
        }
    }

    /// No connect take for `facts` in `mode` (with `relayed` from REMOTE
    /// before the start): Auto and an ordinary reconnect keep today's
    /// commands.
    /// `waits`: the facts lack a report the take needs, so Auto's release
    /// comes at the deadline (#452).
    fn assert_no_connect_take(
        facts: SessionFacts,
        mode: ConnectMode,
        relayed: Option<u32>,
        waits: bool,
    ) {
        let name = format!("{facts:?} {mode:?} {relayed:?}");
        let mut p = Probe::new(false, true);
        p.core.connect_mode = mode;
        p.routing(facts.clone());
        if let Some(category) = relayed {
            p.feed(5, relay(REMOTE, category));
        }
        if waits {
            assert_eq!(
                p.feed(10, Input::Auto),
                [ConnectA2dp, Announce, DrainPlayback],
                "auto {name}"
            );
            assert_eq!(p.feed(10 + WINDOW_MS - 1, Input::Tick), [], "{name}");
            assert_eq!(
                p.feed(10 + WINDOW_MS, Input::Tick),
                [SendOwnership(false)],
                "deadline {name}"
            );
        } else {
            assert_eq!(
                p.feed(10, Input::Auto),
                [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback],
                "auto {name}"
            );
        }
        // A reconnect forgets the relays: only facts without one apply.
        if relayed.is_none() {
            let mut p = Probe::new(true, true);
            p.core.connect_mode = mode;
            assert_eq!(
                ordinary_reconnect(&mut p, facts),
                [AdoptSession, Announce, DrainPlayback],
                "reconnect {name}"
            );
        }
    }

    #[test]
    fn no_connect_take_remote_media_or_call() {
        let auto = ConnectMode::Automatically;
        for r#type in [AudioSourceType::Media, AudioSourceType::Call] {
            let remote = || SessionFacts {
                audio: Some(source(REMOTE, r#type)),
                ..ready_facts()
            };
            // Remote media waits for its host's relay.
            let media = r#type == AudioSourceType::Media;
            assert_no_connect_take(remote(), auto, None, media);
            assert_no_connect_take(remote(), auto, Some(STREAM), false);
        }
        // A call's relay of 100 doesn't make it idle.
        let call = SessionFacts {
            audio: Some(source(REMOTE, AudioSourceType::Call)),
            ..ready_facts()
        };
        assert_no_connect_take(call, auto, Some(IDLE), false);
        // An idle report whose host relays above 100.
        let relayed_busy = SessionFacts {
            audio: Some(source(REMOTE, AudioSourceType::None)),
            ..ready_facts()
        };
        assert_no_connect_take(relayed_busy, auto, Some(STREAM), false);
    }

    #[test]
    fn no_connect_take_no_bud_in_ear() {
        for ears in [vec![OutOfEar, OutOfEar], vec![InCase, OutOfEar], vec![]] {
            let waits = ears.is_empty();
            let facts = SessionFacts {
                ears,
                ..ready_facts()
            };
            assert_no_connect_take(facts, ConnectMode::Automatically, None, waits);
        }
    }

    #[test]
    fn no_connect_take_when_last_connected_and_not_last_owner() {
        assert_no_connect_take(ready_facts(), ConnectMode::WhenLastConnected, None, false);
    }

    #[test]
    fn no_connect_take_without_a_remote_host() {
        for hosts in [vec![host(LOCAL)], vec![]] {
            let waits = hosts.is_empty();
            let facts = SessionFacts {
                hosts,
                ..ready_facts()
            };
            assert_no_connect_take(facts, ConnectMode::Automatically, None, waits);
        }
    }

    #[test]
    fn no_connect_take_on_manual_reconnect() {
        let mut p = Probe::new(false, true);
        assert_eq!(p.feed(0, Input::Disconnected), [DropSession]);
        assert_eq!(p.feed(1000, Input::Tick), [Reconnect { audio: false }]);
        assert_eq!(
            p.feed(1100, Input::Reconnected(ready_facts())),
            [AdoptSession, DrainPlayback]
        );
    }

    #[test]
    fn no_connect_take_while_case_recovery_waits() {
        let mut p = Probe::new(true, false);
        assert_eq!(
            p.feed(100, Input::Reconnected(ready_facts())),
            [CancelReconnect]
        );
    }

    #[test]
    fn connect_take_keeps_ownership_while_idle() {
        let mut p = acked_connect_take(false);
        for ms in [200, 700, 1200] {
            assert_eq!(p.play(ms, false), []);
        }
        p.idle_ticks(1200);
        assert!(p.core.requested_take && p.core.pending.is_none());
    }

    #[test]
    fn connect_take_then_play_sends_no_second_take() {
        let mut p = acked_connect_take(false);
        assert_eq!(p.play(200, false), []);
        p.core.take_lines();
        assert_eq!(p.play(700, true), [ResumeA2dp]);
        assert_eq!(
            count_lines(
                &p.core.take_lines(),
                "AUTO playback started; Linux already owns"
            ),
            1
        );
        assert_eq!(p.play(1200, false), []);
        assert_eq!(p.play(1700, true), []);
        // With the transport streaming there is nothing to resume.
        let mut p = acked_connect_take(false);
        assert_eq!(p.feed(150, Input::A2dpTransport { active: true }), []);
        assert_eq!(p.play(200, false), []);
        assert_eq!(p.play(700, true), []);
    }

    #[test]
    fn connect_take_yields_on_live_zero() {
        let mut p = acked_connect_take(true);
        assert_eq!(
            p.feed(200, Input::Ownership(vec![0])),
            [PauseMedia, RouteAway, HoldA2dp(true)]
        );
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
    }

    // #452: the session start's decision waits for the routing reports.

    /// A manual live session with nothing reported yet (default sink
    /// routing on, an idle A2DP transport, `hold` for #264) gets `Auto` at
    /// 10 ms and the backlog boundary at 20 ms: no ownership packet yet.
    fn waiting_start(hold: bool) -> Probe {
        let mut p = Probe::new(false, true);
        p.core.default_sink_routing = true;
        p.core.a2dp_hold = hold;
        assert_eq!(p.feed(5, idle_transport()), []);
        let mut start = if hold { vec![HoldA2dp(false)] } else { vec![] };
        start.extend([ConnectA2dp, Announce, DrainPlayback]);
        assert_eq!(p.feed(10, Input::Auto), start);
        assert_eq!(p.feed(20, Input::InitBacklogDrained), []);
        p.core.take_lines();
        p
    }

    /// The hosts [LOCAL, REMOTE] at 100 ms (a new remote host: announced)
    /// and both buds in at 110 ms.
    fn hosts_and_ears(p: &mut Probe) {
        let hosts = Input::ConnectedDevices {
            old: Vec::new(),
            new: two_hosts(),
        };
        assert_eq!(p.feed(100, hosts), [Announce]);
        assert_eq!(p.feed(110, Input::EarState(vec![InEar, InEar])), []);
    }

    fn remote_source(r#type: AudioSourceType) -> Input {
        Input::AudioSource(source(REMOTE, r#type))
    }

    #[test]
    fn connect_take_waits_for_the_routing_reports() {
        // Spike #157's order: an idle (or Linux's own) source completes it.
        for audio in [idle(), source(LOCAL, AudioSourceType::Media)] {
            let mut p = waiting_start(false);
            hosts_and_ears(&mut p);
            assert_eq!(
                p.feed(120, Input::AudioSource(audio.clone())),
                [SendOwnership(true)],
                "{audio:?}"
            );
            assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 1);
            assert!(p.core.resume_deferred);
            // One take per session start: a later report sends nothing.
            assert_eq!(p.feed(130, relay(REMOTE, IDLE)), []);
            assert_eq!(p.feed(200, Input::Ownership(vec![1])), [RouteToAirPods]);
            assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
            assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 0);
        }
    }

    #[test]
    fn connect_take_waits_for_the_remote_media_relay() {
        use AudioSourceType::{Call, Media};
        // Relayed idle in the window: the take.
        let mut p = waiting_start(false);
        hosts_and_ears(&mut p);
        assert_eq!(p.feed(120, remote_source(Media)), []);
        assert_eq!(p.feed(130, relay(REMOTE, IDLE)), [SendOwnership(true)]);
        assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 1);
        // Relayed playing: Auto's release at that report, untracked with
        // the owner unknown (#289).
        let mut p = waiting_start(false);
        hosts_and_ears(&mut p);
        assert_eq!(p.feed(120, remote_source(Media)), []);
        assert_eq!(p.feed(130, relay(REMOTE, MEDIA)), [SendOwnership(false)]);
        assert!(p.core.pending.is_none());
        assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
        // A remote call: the state is complete, the release at once.
        let mut p = waiting_start(false);
        hosts_and_ears(&mut p);
        assert_eq!(p.feed(120, remote_source(Call)), [SendOwnership(false)]);
        // No relay: the release at the deadline, and a relay of 100 1 ms
        // after it makes no take.
        let mut p = waiting_start(false);
        hosts_and_ears(&mut p);
        assert_eq!(p.feed(120, remote_source(Media)), []);
        assert_eq!(p.feed(10 + WINDOW_MS - 1, Input::Tick), []);
        assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), [SendOwnership(false)]);
        assert_eq!(p.feed(10 + WINDOW_MS + 1, relay(REMOTE, IDLE)), []);
        assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 0);
    }

    #[test]
    fn connect_window_owner_report_keeps() {
        let mut p = waiting_start(false);
        assert_eq!(p.feed(50, Input::Ownership(vec![1])), [RouteToAirPods]);
        assert!(p.core.requested_take && p.core.pending.is_none());
        assert!(p.core.resume_deferred);
        let keep = "AUTO connected; Linux already owns the AirPods, keeping them";
        assert_eq!(count_lines(&p.core.take_lines(), keep), 1);
        hosts_and_ears(&mut p);
        assert_eq!(p.feed(120, Input::AudioSource(idle())), []);
        assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
    }

    #[test]
    fn connect_window_release_tracked_only_for_a_cached_linux_owner() {
        // [1] cached before Auto, then a complete state with no take: the
        // release is tracked, and its [0] is its ACK, not a loss (#289).
        let mut p = Probe::new(false, true);
        assert_eq!(p.feed(0, Input::Ownership(vec![1])), []);
        assert_eq!(
            p.feed(10, Input::Auto),
            [ConnectA2dp, Announce, DrainPlayback]
        );
        hosts_and_ears(&mut p);
        let call = remote_source(AudioSourceType::Call);
        assert_eq!(p.feed(120, call), [SendOwnership(false)]);
        assert!(p.core.pending.is_some());
        p.core.take_lines();
        assert_eq!(p.feed(200, Input::Ownership(vec![0])), []);
        let lines = p.core.take_lines();
        assert_eq!(count_lines(&lines, "ACK owner=not-linux"), 1);
        assert_eq!(count_lines(&lines, "YIELD"), 0);
    }

    #[test]
    fn connect_window_not_opened_when_no_report_can_allow_the_take() {
        // The connect mode blocks the take: Auto's release, as on 84ac699.
        let mut p = Probe::new(false, true);
        p.core.connect_mode = ConnectMode::WhenLastConnected;
        assert_eq!(
            p.feed(10, Input::Auto),
            [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
        );
        hosts_and_ears(&mut p);
        assert_eq!(p.feed(120, Input::AudioSource(idle())), []);
        // A take held: kept, nothing sent (#262).
        let mut p = acked_auto_take();
        assert_eq!(
            p.feed(1100, Input::Auto),
            [ConnectA2dp, Announce, DrainPlayback]
        );
        assert_eq!(p.feed(1100 + WINDOW_MS, Input::Tick), []);
        // Manual mode in the window: no take can follow, so the release.
        let mut p = waiting_start(false);
        assert_eq!(p.feed(50, Input::Manual), [SendOwnership(false)]);
        reports_send_nothing(&mut p);
    }

    /// The routing reports and the deadline in manual mode or on a lost
    /// session: nothing is announced or sent.
    fn reports_send_nothing(p: &mut Probe) {
        let hosts = Input::ConnectedDevices {
            old: Vec::new(),
            new: two_hosts(),
        };
        assert_eq!(p.feed(100, hosts), []);
        assert_eq!(p.feed(110, Input::EarState(vec![InEar, InEar])), []);
        assert_eq!(p.feed(120, Input::AudioSource(idle())), []);
        assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
    }

    #[test]
    fn connect_window_play_edge_takes_for_playback() {
        let mut p = waiting_start(false);
        hosts_and_ears(&mut p);
        // Remote media with no relay yet: the connect take waits.
        assert_eq!(p.feed(120, remote_source(AudioSourceType::Media)), []);
        assert_eq!(p.play(200, false), []);
        assert_eq!(p.play_category(300, MEDIA), [SendOwnership(true)]);
        let lines = p.core.take_lines();
        assert_eq!(count_lines(&lines, CONNECT_TAKE), 0);
        assert_eq!(
            count_lines(&lines, "AUTO playback started; requested Linux ownership"),
            1
        );
        assert_eq!(p.feed(400, relay(REMOTE, IDLE)), []);
        assert_eq!(
            p.feed(500, Input::Ownership(vec![1])),
            [RouteToAirPods, ResumeA2dp]
        );
        assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
    }

    #[test]
    fn connect_window_ends_without_a_packet() {
        // Disconnected: no ownership packet, only the reconnect.
        let mut p = waiting_start(false);
        assert_eq!(p.feed(50, Input::Disconnected), [DropSession]);
        assert_eq!(
            p.feed(10 + WINDOW_MS, Input::Tick),
            [Reconnect { audio: true }]
        );
        // Quit exits at once.
        let mut p = waiting_start(false);
        assert_eq!(p.feed(50, Input::Quit), [Exit]);
        // A lost session: its queued reports and the deadline send nothing.
        let mut p = waiting_start(false);
        assert_eq!(p.feed(50, Input::SessionLost), []);
        reports_send_nothing(&mut p);
    }

    #[test]
    fn connect_window_remote_request_only_yields() {
        // Owner unknown: the request's own "not owning", then no take.
        let mut p = waiting_start(false);
        assert_eq!(
            p.feed(50, Input::RemoteOwnershipRequest),
            [SendOwnership(false)]
        );
        hosts_and_ears(&mut p);
        assert_eq!(p.feed(120, Input::AudioSource(idle())), []);
        assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
        // Owner [0]: Linux already yielded, nothing at all.
        let mut p = waiting_start(false);
        assert_eq!(p.feed(30, Input::Ownership(vec![0])), []);
        assert_eq!(p.feed(50, Input::RemoteOwnershipRequest), []);
        hosts_and_ears(&mut p);
        assert_eq!(p.feed(120, Input::AudioSource(idle())), []);
        assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
        assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 0);
    }

    #[test]
    fn connect_window_take_undoes_the_remote_media_hold_and_park() {
        let mut p = waiting_start(true);
        p.core = p.core.with_park_sink(true);
        p.core.set_streams(true, false);
        assert_eq!(p.play(30, false), []);
        hosts_and_ears(&mut p);
        let media = remote_source(AudioSourceType::Media);
        assert_eq!(p.feed(120, media), [HoldA2dp(true)]);
        assert_eq!(p.core.park(), Park::Parked);
        assert_eq!(p.feed(130, relay(REMOTE, IDLE)), [SendOwnership(true)]);
        assert_eq!(
            p.feed(200, Input::Ownership(vec![1])),
            [HoldA2dp(false), RouteToAirPods]
        );
        assert_ne!(p.core.park(), Park::Parked);
    }

    #[test]
    fn connect_window_play_before_the_decision_resumes() {
        // Linux plays before the reports: the take's ACK resumes.
        let mut p = waiting_start(false);
        assert_eq!(p.play(30, true), []);
        hosts_and_ears(&mut p);
        assert_eq!(
            p.feed(120, Input::AudioSource(idle())),
            [SendOwnership(true)]
        );
        assert_eq!(
            p.feed(200, Input::Ownership(vec![1])),
            [RouteToAirPods, ResumeA2dp]
        );
        // The keep has no ACK: it resumes at once.
        let mut p = waiting_start(false);
        assert_eq!(p.play(30, true), []);
        assert_eq!(
            p.feed(50, Input::Ownership(vec![1])),
            [RouteToAirPods, ResumeA2dp]
        );
    }

    /// A #307 reconnect at the aggregate 301 for a play that asked at
    /// `requested`, the remote playing media (301).
    fn reconnect_requested(requested: u32) -> (Probe, Vec<Command>) {
        use crate::media_controller::AUDIO_CATEGORY_MEDIA;
        let remote_media_facts = SessionFacts {
            audio: Some(source(REMOTE, AudioSourceType::Media)),
            ..ready_facts()
        };
        let mut p = Probe::new(true, false);
        p.core = p
            .core
            .with_playback_requested()
            .with_category(AUDIO_CATEGORY_MEDIA)
            .with_requested_category(requested);
        let out = p.feed(100, Input::Reconnected(remote_media_facts));
        (p, out)
    }

    #[test]
    fn with_requested_category_takes_at_the_request() {
        use crate::media_controller::{AUDIO_CATEGORY_MEDIA, AUDIO_CATEGORY_STREAM};
        let (_, out) = reconnect_requested(AUDIO_CATEGORY_MEDIA);
        assert!(out.contains(&SendOwnership(true)), "{out:?}");
        // A source behind playback asked at 201, below the remote's media.
        let (mut p, out) = reconnect_requested(AUDIO_CATEGORY_STREAM);
        assert!(!out.contains(&SendOwnership(true)), "{out:?}");
        // The core's own category is the aggregate: 301 is no rise.
        let out = p.play_source(200, AUDIO_CATEGORY_MEDIA, None);
        assert!(!out.contains(&SendOwnership(true)), "{out:?}");
    }

    // #311: the park sink.

    /// `hold_ready(true)` with the park on, and an idle sample with the
    /// streams known.
    fn park_ready() -> Probe {
        let mut p = hold_ready(true);
        p.core = p.core.with_park_sink(true);
        p.core.set_streams(true, false);
        assert_eq!(p.play(20, false), []);
        assert_eq!(p.core.park(), Park::Local);
        p
    }

    /// `park_ready`, held by another host's request while idle: parked.
    fn parked() -> Probe {
        let mut p = park_ready();
        assert_eq!(p.feed(100, Input::RemoteOwnershipRequest), [HoldA2dp(true)]);
        assert_eq!(p.core.park(), Park::Parked);
        p
    }

    /// Ticks up to `ms` - 1 keep the park; the one at `ms` falls back.
    fn park_falls_back_at(p: &mut Probe, ms: u64) {
        for at in (ms - 200..ms).step_by(100) {
            assert_eq!(p.feed(at, Input::Tick), []);
            assert_eq!(p.core.park(), Park::Parked, "{at}");
        }
        assert_eq!(p.feed(ms - 1, Input::Tick), []);
        assert_eq!(p.core.park(), Park::Parked);
        assert_eq!(p.feed(ms, Input::Tick), []);
        assert_eq!(p.core.park(), Park::FallenBack);
    }

    #[test]
    fn park_yield_while_playing_waits_for_an_idle_sample() {
        let mut p = park_ready();
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), [RouteToAirPods]);
        assert_eq!(
            p.feed(1100, Input::Ownership(vec![0])),
            [PauseMedia, RouteAway, HoldA2dp(true)]
        );
        // A game that can't pause keeps playing on the local output.
        assert_eq!(p.core.park(), Park::Local);
        assert_eq!(p.play(1500, true), []);
        assert_eq!(p.core.park(), Park::Local);
        assert_eq!(p.play(2000, false), []);
        assert_eq!(p.core.park(), Park::Parked);
    }

    #[test]
    fn park_waits_for_the_local_call_to_end() {
        let mut p = park_ready();
        assert_eq!(p.feed(50, Input::LocalCall(true)), [AdvertiseCall(true)]);
        assert_eq!(p.feed(100, Input::RemoteOwnershipRequest), [HoldA2dp(true)]);
        assert_eq!(p.core.park(), Park::Local);
        assert_eq!(p.feed(200, Input::LocalCall(false)), [AdvertiseCall(false)]);
        assert_eq!(p.core.park(), Park::Parked);
    }

    #[test]
    fn park_never_without_park_sink() {
        let mut p = hold_ready(true);
        assert_eq!(p.feed(100, Input::RemoteOwnershipRequest), [HoldA2dp(true)]);
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        for ms in (600..10_000).step_by(100) {
            p.feed(ms, Input::Tick);
            assert_eq!(p.core.park(), Park::Local, "{ms}");
        }
    }

    #[test]
    fn park_take_request_falls_back_at_the_timeout() {
        let mut p = parked();
        assert_eq!(p.play(1000, true), [SendOwnership(true)]);
        assert_eq!(p.feed(2800, Input::Tick), []);
        assert_eq!(p.core.park(), Park::Parked, "1.8 s");
        park_falls_back_at(&mut p, 3000);
        // The fallback stays through a stop: no new park until the hold ends.
        assert_eq!(p.play(3500, false), []);
        assert_eq!(p.core.park(), Park::FallenBack);
    }

    #[test]
    fn park_category_skip_falls_back_at_the_timeout() {
        let mut p = park_ready();
        assert_eq!(p.feed(100, remote_media()), [HoldA2dp(true)]);
        assert_eq!(p.core.park(), Park::Parked);
        assert_eq!(p.play(1000, true), []);
        assert!(
            p.core
                .take_lines()
                .iter()
                .any(|l| l.starts_with("AUTO takeover skipped: the audio source's category"))
        );
        park_falls_back_at(&mut p, 3000);
    }

    #[test]
    fn park_connect_mode_skip_falls_back_at_the_timeout() {
        let mut p = parked();
        p.core.connect_mode = ConnectMode::WhenLastConnected;
        assert_eq!(p.play(1000, true), []);
        assert!(p.core.take_lines().iter().any(|l| l == CONNECT_MODE_SKIP));
        park_falls_back_at(&mut p, 3000);
    }

    #[test]
    fn park_every_play_edge_restarts_the_timeout() {
        let mut p = parked();
        p.core.connect_mode = ConnectMode::WhenLastConnected;
        assert_eq!(p.play(1000, true), []);
        assert_eq!(p.play(1500, false), []);
        assert_eq!(p.play(2000, true), []);
        assert_eq!(p.feed(3000, Input::Tick), []);
        assert_eq!(
            p.core.park(),
            Park::Parked,
            "the second edge restarted the deadline to 4000"
        );
        park_falls_back_at(&mut p, 4000);
    }

    #[test]
    fn park_ack_unparks_to_the_airpods_with_the_route() {
        let mut p = parked();
        assert_eq!(p.feed(200, idle_transport()), []);
        assert_eq!(p.play(1000, true), [SendOwnership(true)]);
        assert_eq!(
            p.feed(1500, Input::Ownership(vec![1])),
            [HoldA2dp(false), RouteToAirPods, ResumeA2dp]
        );
        assert_eq!(p.core.park(), Park::AirPods);
        // A later yield parks again once Linux is idle.
        assert_eq!(p.play(2000, false), []);
        assert_eq!(
            p.feed(2500, Input::Ownership(vec![0])),
            [PauseMedia, RouteAway, HoldA2dp(true)]
        );
        assert_eq!(p.core.park(), Park::Parked);
    }

    #[test]
    fn park_ack_after_the_fallback_unparks_to_the_airpods() {
        let mut p = parked();
        assert_eq!(p.play(1000, true), [SendOwnership(true)]);
        park_falls_back_at(&mut p, 3000);
        assert_eq!(
            p.feed(3500, Input::Ownership(vec![1])),
            [HoldA2dp(false), RouteToAirPods]
        );
        assert_eq!(p.core.park(), Park::AirPods);
    }

    #[test]
    fn park_refusal_falls_back_at_the_timeout() {
        let mut p = parked();
        assert_eq!(p.play(1000, true), [SendOwnership(true)]);
        assert_eq!(
            p.feed(1200, Input::Ownership(vec![0])),
            [PauseMedia, RouteAway]
        );
        park_falls_back_at(&mut p, 3000);
    }

    #[test]
    fn park_disconnected_unparks_to_local() {
        let mut p = parked();
        assert_eq!(
            p.feed(200, Input::Disconnected),
            [HoldA2dp(false), DropSession]
        );
        assert_eq!(p.core.park(), Park::Local);
        // So does a fallback.
        let mut p = parked();
        assert_eq!(p.play(1000, true), [SendOwnership(true)]);
        park_falls_back_at(&mut p, 3000);
        p.feed(3100, Input::Disconnected);
        assert_eq!(p.core.park(), Park::Local);
    }

    #[test]
    fn park_quit_unparks_to_local() {
        let mut p = parked();
        assert_eq!(p.feed(200, Input::Quit), [HoldA2dp(false), Exit]);
        assert_eq!(p.core.park(), Park::Local);
    }

    #[test]
    fn park_only_linux_listed_unparks_to_local() {
        let mut p = parked();
        let only_linux = Input::ConnectedDevices {
            old: two_hosts(),
            new: vec![host(LOCAL)],
        };
        assert_eq!(p.feed(200, only_linux), [HoldA2dp(false)]);
        assert_eq!(p.core.park(), Park::Local);
    }

    #[test]
    fn park_baseline_stream_falls_back_without_a_take() {
        let mut p = parked();
        p.core.set_streams(true, true);
        assert_eq!(p.play(1000, false), []);
        assert_eq!(p.core.park(), Park::FallenBack);
    }

    #[test]
    fn park_baseline_stream_keeps_the_hold_from_parking() {
        let mut p = park_ready();
        p.core.set_streams(true, true);
        assert_eq!(p.feed(100, Input::RemoteOwnershipRequest), [HoldA2dp(true)]);
        assert_eq!(p.core.park(), Park::Local);
        p.core.set_streams(true, false);
        assert_eq!(p.play(500, false), []);
        assert_eq!(p.core.park(), Park::Parked);
    }

    /// The seed's hold (#311 r3): stream presence unknown isn't idle. No
    /// park before a sample taken with the streams known; never with a dead
    /// stream monitor.
    #[test]
    fn park_waits_for_a_sample_with_the_streams_known() {
        // `hold_ready`'s idle sample came before any listing.
        let mut p = hold_ready(true);
        p.core = p.core.with_park_sink(true);
        assert_eq!(p.feed(100, Input::RemoteOwnershipRequest), [HoldA2dp(true)]);
        assert_eq!(p.core.park(), Park::Local);
        // Listed, but no sample since.
        p.core.set_streams(true, false);
        assert_eq!(p.feed(200, Input::Tick), []);
        assert_eq!(p.core.park(), Park::Local);
        assert_eq!(p.play(300, false), []);
        assert_eq!(p.core.park(), Park::Parked);
        // The stream monitor dies: back to the local output.
        p.core.set_streams(false, false);
        assert_eq!(p.play(400, false), []);
        assert_eq!(p.core.park(), Park::FallenBack);

        // A dead monitor from the start never parks.
        let mut p = hold_ready(true);
        p.core = p.core.with_park_sink(true);
        assert_eq!(p.feed(100, Input::RemoteOwnershipRequest), [HoldA2dp(true)]);
        for ms in (200..3000).step_by(100) {
            assert_eq!(p.play(ms, false), []);
            assert_eq!(p.core.park(), Park::Local, "{ms}");
        }
    }
}
