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

mod calls;
mod ownership;
mod park;
mod playback;
mod session;
mod take_back;
mod timers;

pub(crate) use calls::local_inputs;

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
/// A take for a Linux play edge.
const PLAYBACK_TAKE: &str = "AUTO playback started; requested Linux ownership";
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
    /// The system is about to sleep (logind `PrepareForSleep(true)`). As
    /// the Mac does (spike #261): pause if Linux plays, relay idle, then
    /// disconnect without releasing. Until the drop the session is lost,
    /// and the drop pauses nothing more.
    Sleep,
    /// `Sleep`'s disconnect didn't happen (refused, timed out, or the sleep
    /// no longer waited): the session is live again, ownership kept.
    SleepDisconnectFailed,
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
    /// Whether Linux uses the AirPods' mic without a call (#505): the card
    /// on a head-unit profile. Holds the AirPods as `LocalCall` does, but
    /// relays audio category 200 instead of advertising a call; during a
    /// call it is only recorded.
    LocalMic(bool),
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
    /// The user clicked Undo on the "moved to …" of yield `id` (#17). While
    /// that yield stands (no newer yield, take, OwnsConnection [1] or
    /// disconnect since), Linux takes back without resuming and relays the
    /// reverse request to the host that took, if known. A click during the
    /// yield's pending release runs once the release settles.
    Undo(u64),
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
    /// Disconnect the AirPods through BlueZ (`Input::Sleep`). If it doesn't
    /// happen, the shell feeds `Input::SleepDisconnectFailed`.
    Disconnect,
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
    /// Post "moved to <taker>" with Undo (#17): another host took the
    /// AirPods while Linux played. `id` names this yield for its Undo;
    /// `taker` is the taker's address, if known. Only with
    /// `with_notifications(true)`.
    NotifyMovedTo { id: u64, taker: Option<String> },
    /// Post "Connected": a take of Linux's was acknowledged (#17). Only
    /// with `with_notifications(true)`.
    NotifyConnected,
    /// Relay the reverse request (`AACPManager::send_hijack_reversed`) to
    /// this host after an Undo's take (#17).
    SendHijackReversed(String),
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
    /// `Input::Sleep` disconnected the session: its drop doesn't pause.
    sleeping: bool,
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
    /// Linux uses the mic without a call (#505): held like a call.
    local_mic: bool,
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
    /// Parked on a wake park this task claimed (#529): exempt from the #311
    /// rules until the connect decision, then while a take is pending, and
    /// until the take's ACK or `wake_until`.
    wake_park: bool,
    /// The wake park's deadline: the first play edge's, which later ones
    /// don't push out.
    wake_until: Option<Instant>,
    /// `Auto`'s or `Reconnected`'s ownership decision was made: a claimed
    /// wake park then waits only for a pending take (#529).
    wake_decided: bool,
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
    /// The category of a play edge in that wait skipped only for a report
    /// not yet in (#490); read only while the wait is open.
    deferred_play: Option<u32>,
    /// Emit the notification commands (#17).
    notifications: bool,
    /// The id and taker (None: unknown) of the playing yield an Undo would
    /// answer (#17); None once a take, an OwnsConnection [1] or a disconnect
    /// made it stale.
    undo_taker: Option<(u64, Option<String>)>,
    /// The last playing yield's id.
    last_yield: u64,
    /// An Undo clicked while a release was pending: it runs once nothing
    /// is (#17).
    undo_waiting: Option<u64>,
    /// Taking the AirPods back after a remote call (#14).
    take_back: take_back::TakeBack,
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
            sleeping: false,
            quitting: false,
            pending: None,
            requested_take: false,
            was_playing: None,
            playback_baseline_at: now,
            take_assumes_playing: false,
            reconnect_take_pending: false,
            timed_out_release: None,
            local_call: false,
            local_mic: false,
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
            wake_park: false,
            wake_until: None,
            wake_decided: false,
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
            deferred_play: None,
            notifications: false,
            undo_taker: None,
            last_yield: 0,
            undo_waiting: None,
            take_back: take_back::TakeBack::default(),
        }
    }

    /// Relay Linux's audio category to the other hosts in automatic mode
    /// (#272); off by default.
    pub(crate) fn with_category_relay(mut self, enabled: bool) -> Self {
        self.category_relay = enabled;
        self
    }

    /// Emit the "moved to" and "Connected" notification commands (#17);
    /// off by default.
    pub(crate) fn with_notifications(mut self, enabled: bool) -> Self {
        self.notifications = enabled;
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

    /// Start parked on the wake park this task claimed (#529), with the
    /// deadline its first play edge started, if any.
    pub(crate) fn with_wake_park(mut self, until: Option<Instant>) -> Self {
        self.park = Park::Parked;
        self.wake_park = true;
        self.wake_until = until;
        self.park_until = until;
        self
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

    /// Change the connect mode of a running core (#153): no command.
    pub(crate) fn set_connect_mode(&mut self, mode: ConnectMode) {
        self.connect_mode = mode;
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
        let disconnected = input == Input::Disconnected;
        // `Auto`'s or `Reconnected`'s ownership decision (#529).
        let deciding =
            self.connect_until.is_some() || matches!(input, Input::Auto | Input::Reconnected(_));
        self.apply(now, input, &mut out);
        self.settle_connect(now, owns, &mut out);
        self.take_back(now, owns, &mut out);
        // Linux has the AirPods again (a take of this step's, the connect
        // wait's and the take-back's included), or the session is gone: a
        // later Undo answers nothing (#17).
        if owns || disconnected || out.contains(&Command::SendOwnership(true)) {
            self.undo_taker = None;
        }
        // A click that waited for a release runs once it settled (#17).
        if self.pending.is_none()
            && let Some(id) = self.undo_waiting.take()
        {
            self.undo(now, id, &mut out);
        }
        if !out
            .iter()
            .any(|c| matches!(c, Command::Exit | Command::Fail(_)))
        {
            self.finish_quit(now, &mut out);
        }
        self.update_park(now, deciding && self.connect_until.is_none());
        out
    }

    /// Park once the hold is on and a sample with the streams known shows
    /// Linux plays nothing and has no call; end a park or a fallback when
    /// the hold ends (#311). A take's ACK has already unparked to the
    /// AirPods. A baseline stream, which can't take, or a dead stream
    /// monitor, which can't see one, falls a park back to the local output.
    /// A claimed wake park (#529) is exempt until the connect decision, and
    /// after it only while a take is pending; with no sample yet, only a
    /// dead monitor falls it back.
    fn update_park(&mut self, now: Instant, decided: bool) {
        self.wake_decided |= decided;
        if self.wake_decided && !self.pending.is_some_and(|p| p.desired) {
            self.wake_park = false;
            self.wake_until = None;
        }
        let held = self.held == Some(true);
        let known = self.streams_known && self.sampled_known;
        let idle = known && self.category <= AUDIO_CATEGORY_IDLE && !self.call_or_mic();
        self.park = match self.park {
            Park::Parked if self.wake_park => Park::Parked,
            Park::Parked | Park::FallenBack if !held => Park::Local,
            // Only a park made on a known sample has one: no `sampled_known`.
            Park::Parked if self.baseline_playing || !self.streams_known => {
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
            self.wake_park = false;
            self.wake_until = None;
        } else if self.wake_park {
            // A baseline stream, which this task's poller never counts, is
            // the parked play: its deadline runs from now.
            if self.baseline_playing && self.park_until.is_none() {
                self.park_until = Some(now + PARK_TIMEOUT);
            }
            self.wake_until = self.wake_until.or(self.park_until);
            self.park_until = self.wake_until;
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
                let call_holds = self.call_or_mic() && self.requested_take;
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
                if self.call_or_mic() && self.requested_take {
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
                if self.requested_take || self.owner.as_deref() == Some(&[1]) {
                    self.take_back.yielded(now);
                }
                self.say("REMOTE requested ownership; yielding");
                self.request(now, false, Cause::Remote, out);
                self.hold_for_remote(out);
                self.notify_moved_to(out);
            }
            Input::SessionLost => {
                self.ear_resume_until = None;
                if self.live() {
                    self.session_lost = true;
                    self.say("SESSION lost; finishing its queued events before disconnecting");
                }
            }
            Input::Sleep => self.sleep(out),
            Input::SleepDisconnectFailed => self.sleep_disconnect_failed(),
            Input::Disconnected => {
                self.take_back.clear();
                self.disconnected(now, out);
            }
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
                self.deferred_play = None;
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
                    // The user released: Auto's wait takes nothing back (#452).
                    self.connect_until = None;
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
            Input::LocalMic(on) => self.local_mic(on, out),
            Input::Tick => self.tick(now, out),
            Input::A2dpTransport { active } => self.a2dp_active = Some(active),
            Input::MediaInformation { host, category } => {
                self.take_back.relay(now, &host, category);
                self.relays.insert(host, category);
            }
            Input::Undo(id) => self.undo(now, id, out),
        }
    }

    /// Undo of yield `id` (#17): take back without resuming, then relay
    /// the reverse request to the taker, if known.
    fn undo(&mut self, now: Instant, id: u64, out: &mut Vec<Command>) {
        if !self
            .undo_taker
            .as_ref()
            .is_some_and(|(yielded, _)| *yielded == id)
        {
            self.say("UNDO ignored: its yield no longer stands");
            return;
        }
        if self.pending.is_some() {
            // A take now would leave the pending release's [0] acking
            // nothing, which yields that take: run once it settles.
            self.undo_waiting = Some(id);
            self.say("WAIT ownership request pending; Undo runs once it settles");
            return;
        }
        let Some((_, taker)) = self.undo_taker.take() else {
            return;
        };
        if self
            .decide_take(TakeoverTrigger::Undo, self.category)
            .action
            != TakeoverAction::TakeOver
        {
            self.say("BLOCKED undo needs a live session, a remote host, an in-ear bud, and no remote call");
            return;
        }
        // A timed-out release (the yield's) may still be acknowledged late:
        // that [0] is not a loss of this take (#55).
        let late = self.timed_out_release;
        self.request(now, true, Cause::User, out);
        self.timed_out_release = late;
        // Undo never resumes (spike #259): the ACK skips the resume.
        self.resume_deferred = true;
        self.say("REQUEST owner=linux for Undo");
        match taker {
            Some(host) => out.push(Command::SendHijackReversed(host)),
            None => {
                self.say("UNDO the host that took is unknown; not relaying the reverse request")
            }
        }
    }
}

#[cfg(test)]
mod tests;
