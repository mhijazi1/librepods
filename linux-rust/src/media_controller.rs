use crate::a2dp_resume::{A2dpResumeIo, AUDIBLE_SAMPLE, Action};
use crate::bluetooth::aacp::AACPManager;
use crate::bluetooth::aacp::EarDetectionStatus;
use crate::bluetooth::aacp::{AACPManagerState, AudioSource, AudioSourceType, ConnectedDevice};
use crate::pulse;
use crate::stream_monitor::{AUDIBLE_PEAK, PeakSource, PulsePeaks};
use dbus::arg::RefArg;
use dbus::blocking::Connection;
use dbus::blocking::stdintf::org_freedesktop_dbus::Properties;
use futures::future::BoxFuture;
use libpulse_binding::volume::{ChannelVolumes, Volume};
use log::{debug, error, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::AbortHandle;
use tokio::time::Instant;

// Reconnecting AirPods creates a fresh MediaController. Keep the listener task
// outside the controller so the replacement can stop its predecessor by MAC.
static PLAYBACK_LISTENERS: LazyLock<StdMutex<HashMap<String, AbortHandle>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

/// Stop the legacy playback listener of this AirPods MAC, if one runs. Used
/// when a replacement device runs another switching mode (switching.rs).
pub(crate) fn stop_playback_listener(mac: &str) {
    if let Some(listener) = PLAYBACK_LISTENERS.lock().unwrap().remove(mac) {
        listener.abort();
    }
}

/// Whether a legacy playback listener is registered for this AirPods MAC.
#[cfg(test)]
pub(crate) fn playback_listener_registered(mac: &str) -> bool {
    PLAYBACK_LISTENERS.lock().unwrap().contains_key(mac)
}

/// What asked for a takeover decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TakeoverTrigger {
    /// A playback poll. Needs the baseline, a stopped-to-playing transition
    /// and no pending self-pause.
    PlaybackPoll,
    /// The control session came back for playback that started while it was
    /// down after a case-closed disconnect (#841 case recovery).
    ReconnectedForPlayback,
    /// The user asked for the AirPods (handoff probe `take`).
    Manual,
}

/// #841 case recovery: after the buds were put in the case, Linux does not
/// race the other host to reconnect until new local playback asks for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CaseRecovery {
    /// Reconnect as usual.
    Immediate,
    /// Disconnected with the case closed; waiting for new playback.
    WaitingForPlayback,
    /// New playback started while waiting; reconnect and take for it.
    PlaybackRequested,
}

impl CaseRecovery {
    /// Advance on a playback poll taken while disconnected: a new
    /// stopped-to-playing playback requests the reconnection, and a stop
    /// before it completes withdraws the request.
    pub(crate) fn playback(&mut self, fresh_play: bool, stopped: bool) {
        match (*self, fresh_play, stopped) {
            (Self::WaitingForPlayback, true, _) => *self = Self::PlaybackRequested,
            (Self::PlaybackRequested, _, true) => *self = Self::WaitingForPlayback,
            _ => {}
        }
    }
}

/// Whether both buds are in the case after a wear report. A bud in or out of
/// an ear opens it again; any other report keeps `was_closed`.
pub(crate) fn case_closed_after(ears: &[EarDetectionStatus], was_closed: bool) -> bool {
    if ears.len() == 2 && ears.iter().all(|s| *s == EarDetectionStatus::InCase) {
        true
    } else if ears
        .iter()
        .any(|s| matches!(s, EarDetectionStatus::InEar | EarDetectionStatus::OutOfEar))
    {
        false
    } else {
        was_closed
    }
}

/// The AACP routing facts the #841 handoff guards look at. Only copied from
/// the AACP state (see `routing_snapshot`); `decide_takeover` judges them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RoutingSnapshot {
    /// The AACP control session is up.
    pub(crate) has_session: bool,
    /// Number of hosts the AirPods report as connected.
    pub(crate) host_count: usize,
    /// One of those hosts is this machine.
    pub(crate) local_host: bool,
    /// One of those hosts is another device.
    pub(crate) remote_host: bool,
    /// At least one bud reports `InEar`.
    pub(crate) bud_in_ear: bool,
    /// Activity of the reported audio source; None when nothing is reported.
    pub(crate) audio: Option<AudioSourceType>,
    /// The reported audio source is this machine.
    pub(crate) audio_from_local: bool,
    /// Linux's audio category (#272).
    pub(crate) local_category: u32,
    /// The latest category the reported audio source's host relayed this
    /// session (#272); None without a relay from it.
    pub(crate) remote_category: Option<u32>,
}

/// Audio categories of the media-information relays (#272, captures from
/// #74 and #257): idle, a non-MPRIS stream, MPRIS media. A call is
/// `AUDIO_CATEGORY_CALL` (501).
pub(crate) const AUDIO_CATEGORY_IDLE: u32 = 100;
pub(crate) const AUDIO_CATEGORY_STREAM: u32 = 201;
pub(crate) const AUDIO_CATEGORY_MEDIA: u32 = 301;

/// Copy the routing facts out of the AACP fields. Pure and decision-free so
/// the handoff core can feed it from its own event mirror. `relays` maps a
/// host's address to the category it last relayed.
pub(crate) fn routing_snapshot(
    has_session: bool,
    hosts: &[ConnectedDevice],
    ears: &[EarDetectionStatus],
    audio: Option<&AudioSource>,
    relays: &HashMap<String, u32>,
    local_category: u32,
    local: &str,
) -> RoutingSnapshot {
    RoutingSnapshot {
        has_session,
        host_count: hosts.len(),
        local_host: hosts.iter().any(|d| d.mac == local),
        remote_host: hosts.iter().any(|d| d.mac != local),
        bud_in_ear: ears.contains(&EarDetectionStatus::InEar),
        audio: audio.map(|a| a.r#type),
        audio_from_local: audio.is_some_and(|a| a.mac == local),
        local_category,
        remote_category: audio
            .filter(|a| a.mac != local)
            .and_then(|a| relays.get(&a.mac))
            .copied(),
    }
}

/// `routing_snapshot` of a locked AACP state, with no relays and an idle
/// local category (the probe's manual take ignores both). Callers drop the
/// guard before any await.
pub(crate) fn routing_snapshot_of(state: &AACPManagerState, local: &str) -> RoutingSnapshot {
    routing_snapshot(
        state.sender.is_some(),
        &state.connected_devices,
        &state.ear_detection_status,
        state.audio_source.as_ref(),
        &HashMap::new(),
        AUDIO_CATEGORY_IDLE,
        local,
    )
}

/// The #841 handoff guards. Absent on the legacy playback listener path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct HandoffGuards {
    pub(crate) routing: RoutingSnapshot,
    pub(crate) recovery: CaseRecovery,
}

/// What one playback poll observed, as far as the takeover decision cares.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TakeoverInputs {
    /// Whether an earlier poll already recorded the initial playback state.
    pub(crate) baseline_taken: bool,
    pub(crate) was_playing: bool,
    pub(crate) is_playing: bool,
    /// Set when LibrePods paused the players itself (ear removal, lost ownership).
    pub(crate) i_paused_the_media: bool,
    /// At least one bud reports `InEar`.
    pub(crate) bud_in_ear: bool,
    /// The playback fields above only count for `PlaybackPoll`.
    pub(crate) trigger: TakeoverTrigger,
    /// #841 routing and case-recovery guards; None skips them (legacy path).
    pub(crate) handoff: Option<HandoffGuards>,
}

impl TakeoverInputs {
    /// Inputs for a take that is not a playback poll (manual take, reconnect
    /// for requested playback), with the #841 guards.
    pub(crate) fn handoff(
        trigger: TakeoverTrigger,
        routing: RoutingSnapshot,
        recovery: CaseRecovery,
    ) -> Self {
        TakeoverInputs {
            baseline_taken: true,
            was_playing: false,
            is_playing: true,
            i_paused_the_media: false,
            bud_in_ear: routing.bud_in_ear,
            trigger,
            handoff: Some(HandoffGuards { routing, recovery }),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TakeoverSkip {
    /// First poll: `was_playing` is a placeholder, not an observation.
    InitialState,
    /// Playback did not go from stopped to playing.
    NoTransition,
    /// Players came back after a pause LibrePods made itself.
    ResumeAfterOwnPause,
    /// Playback started but nobody is wearing the buds.
    NoBudInEar,
    /// #841: case recovery has not seen new playback asking for this take.
    AwaitingNewPlayback,
    /// #841: no live AACP control session, so the routing state is stale.
    NoSession,
    /// #841: the AirPods do not report this machine plus at least one remote
    /// host (Linux alone, Linux absent, or no host list).
    NoRemoteHost,
    /// #841: the audio source is not confirmed idle or local media (a remote
    /// call, unknown activity, or no report; remote media for a manual take).
    AudioNotIdle,
    /// #272: an automatic take found the audio-source host's category (its
    /// relay, else derived from the report) above Linux's, as macOS skips.
    RemoteCategoryHigher { local: u32, remote: u32 },
    /// Legacy listener: an ownership-loss pause is still running, so it has
    /// not yet decided whether the players coming back are its own doing.
    OwnPausePending,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TakeoverAction {
    TakeOver,
    Skip(TakeoverSkip),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TakeoverDecision {
    pub(crate) action: TakeoverAction,
    /// Consume `i_paused_the_media`: the resume it was waiting for has happened.
    pub(crate) clear_self_pause: bool,
}

/// Decide whether to take the AirPods away from another device. Pure, so the
/// order of the guards is tested without D-Bus or AACP. For a playback poll:
/// baseline, transition, self-pause; then the #841 guards when supplied (case
/// recovery, session, hosts, audio source); then ear detection. Without
/// `handoff` this is exactly the legacy listener's policy.
pub(crate) fn decide_takeover(inputs: TakeoverInputs) -> TakeoverDecision {
    let skip = |reason| TakeoverDecision {
        action: TakeoverAction::Skip(reason),
        clear_self_pause: false,
    };
    if inputs.trigger == TakeoverTrigger::PlaybackPoll {
        if !inputs.baseline_taken {
            return skip(TakeoverSkip::InitialState);
        }
        if inputs.was_playing || !inputs.is_playing {
            return skip(TakeoverSkip::NoTransition);
        }
        if inputs.i_paused_the_media {
            return TakeoverDecision {
                action: TakeoverAction::Skip(TakeoverSkip::ResumeAfterOwnPause),
                clear_self_pause: true,
            };
        }
    }
    if let Some(guards) = inputs.handoff
        && let Some(reason) = handoff_block(inputs.trigger, guards)
    {
        return skip(reason);
    }
    if !inputs.bud_in_ear {
        return skip(TakeoverSkip::NoBudInEar);
    }
    TakeoverDecision {
        action: TakeoverAction::TakeOver,
        clear_self_pause: false,
    }
}

/// The legacy listener's policy: `decide_takeover`, except that a take is
/// suppressed (not postponed) while an ownership-loss pause is in flight.
pub(crate) fn decide_legacy_poll(
    inputs: TakeoverInputs,
    own_pause_pending: bool,
) -> TakeoverDecision {
    let decision = decide_takeover(inputs);
    if own_pause_pending && decision.action == TakeoverAction::TakeOver {
        return TakeoverDecision {
            action: TakeoverAction::Skip(TakeoverSkip::OwnPausePending),
            clear_self_pause: false,
        };
    }
    decision
}

/// One `pause_all_media` in flight. Dropping it releases the count, so a
/// panicking pauser or a dropped pause future cannot leave it set.
struct OwnershipPause(Arc<AtomicUsize>);

impl OwnershipPause {
    fn start(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        OwnershipPause(count.clone())
    }
}

impl Drop for OwnershipPause {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// What a blocking MPRIS pause did.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PauseOutcome {
    /// Players that reported `Playing` and paused.
    pub(crate) paused: usize,
    /// Players that reported `Playing` but whose `Pause` call failed.
    pub(crate) failed: usize,
    /// No session bus, so nothing could be checked.
    pub(crate) bus_unavailable: bool,
}

/// Whether a pause counts as ours for `i_paused_the_media`: it paused or
/// tried to pause a player, or could not look while we last saw playback.
/// `was_playing` is `state.is_playing` from before the pause. A pause with
/// nothing playing must not mark it, or the next genuine play is skipped.
pub(crate) fn marks_self_pause(outcome: PauseOutcome, was_playing: bool) -> bool {
    outcome.paused > 0 || outcome.failed > 0 || (outcome.bus_unavailable && was_playing)
}

/// The #841 guards, in order: case recovery, live session, Linux plus a
/// remote host (this machine and at least one other device, so 2, 3, 4...
/// hosts), audio source idle or local media. Automatic takes compare audio
/// categories instead of that last guard (#272): they take when Linux's is
/// at least the source host's, its relay if it sent one this session, else
/// 100 for an idle report and 301 for media. A call or unknown source, or no
/// report, still blocks every take.
fn handoff_block(trigger: TakeoverTrigger, guards: HandoffGuards) -> Option<TakeoverSkip> {
    let recovery_allows = match trigger {
        TakeoverTrigger::ReconnectedForPlayback => {
            guards.recovery == CaseRecovery::PlaybackRequested
        }
        _ => guards.recovery != CaseRecovery::WaitingForPlayback,
    };
    let routing = guards.routing;
    if !recovery_allows {
        Some(TakeoverSkip::AwaitingNewPlayback)
    } else if !routing.has_session {
        Some(TakeoverSkip::NoSession)
    } else if routing.host_count < 2 || !routing.local_host || !routing.remote_host {
        Some(TakeoverSkip::NoRemoteHost)
    } else {
        use AudioSourceType::{Media, None as Idle};
        match routing.audio {
            Some(Media) if routing.audio_from_local => None,
            Some(Idle) if trigger == TakeoverTrigger::Manual => None,
            Some(source @ (Idle | Media)) if trigger != TakeoverTrigger::Manual => {
                let reported = if source == Media {
                    AUDIO_CATEGORY_MEDIA
                } else {
                    AUDIO_CATEGORY_IDLE
                };
                let (local, remote) = (
                    routing.local_category,
                    routing.remote_category.unwrap_or(reported),
                );
                (local < remote).then_some(TakeoverSkip::RemoteCategoryHigher { local, remote })
            }
            _ => Some(TakeoverSkip::AudioNotIdle),
        }
    }
}

/// How many times `get_audio_device_index` looks for the card (~3 s in total).
const CARD_LOOKUP_ATTEMPTS: u32 = 12;
const CARD_LOOKUP_INTERVAL: Duration = Duration::from_millis(250);

/// Card lookups per availability check inside the enumeration waits of
/// `activate_a2dp_profile`. One attempt means no lookup-internal sleep: the
/// wait's own `A2DP_ENUMERATION_INTERVAL` already spaces the checks, and the
/// full ~3 s poll (#1/#724 renegotiation window) is spent once, on the
/// initial lookup.
const CARD_LOOKUP_ATTEMPTS_PER_CHECK: u32 = 1;

/// Time WirePlumber gets to settle after a successful restart.
const WIREPLUMBER_SETTLE: Duration = Duration::from_secs(2);

/// Upper bound on the time `activate_a2dp_profile` may spend waiting for the
/// card and its A2DP profile (see `a2dp_activation_worst_case_wait`).
const A2DP_RECOVERY_BOUND: Duration = Duration::from_secs(15);

/// Longest a `retry_card_lookup` with `attempts` tries sleeps when the card
/// never shows up: `attempts - 1` intervals, as no sleep follows the last try.
const fn card_lookup_max_wait(attempts: u32) -> Duration {
    CARD_LOOKUP_INTERVAL.saturating_mul(attempts.saturating_sub(1))
}

/// BlueZ's A2DP Sink profile, asked for when BlueZ never connected it (#310).
pub(crate) const A2DP_SINK: uuid::Uuid =
    uuid::Uuid::from_u128(0x0000110b_0000_1000_8000_00805f9b34fb);

/// `connect_if_connected`'s refusal: no ConnectProfile was sent (#320).
const NOT_CONNECTED: &str = "AirPods not connected";

/// Longest the A2DP profile connect may take (the handoff probe's value).
const A2DP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Card lookups after the A2DP profile connect: the card can trail BlueZ's
/// reply. 1.75 s of sleeps keeps `a2dp_activation_worst_case_wait` <= 15 s.
const A2DP_CONNECT_LOOKUP_ATTEMPTS: u32 = 8;

/// Worst-case waiting in `activate_a2dp_profile`, from the production
/// constants: the initial full card lookup misses, the A2DP profile connect's
/// re-lookup finds the card on its last try, then the card is missing at
/// every enumeration check, so both waits run all their checks (each a
/// `CARD_LOOKUP_ATTEMPTS_PER_CHECK` lookup) and WirePlumber is restarted once
/// in between. The run time of the lookups themselves, of the connect and of
/// `systemctl` is not counted.
const fn a2dp_activation_worst_case_wait() -> Duration {
    let wait = MediaController::A2DP_ENUMERATION_INTERVAL
        .saturating_mul(MediaController::A2DP_ENUMERATION_ATTEMPTS.saturating_sub(1))
        .saturating_add(
            card_lookup_max_wait(CARD_LOOKUP_ATTEMPTS_PER_CHECK)
                .saturating_mul(MediaController::A2DP_ENUMERATION_ATTEMPTS),
        );
    card_lookup_max_wait(CARD_LOOKUP_ATTEMPTS)
        .saturating_add(card_lookup_max_wait(A2DP_CONNECT_LOOKUP_ATTEMPTS))
        .saturating_add(wait)
        .saturating_add(WIREPLUMBER_SETTLE)
        .saturating_add(wait)
}

const _: () = assert!(
    a2dp_activation_worst_case_wait().as_millis() <= A2DP_RECOVERY_BOUND.as_millis(),
    "activate_a2dp_profile worst case exceeds A2DP_RECOVERY_BOUND"
);

/// Wall-clock limit on `activate_a2dp_profile`, run time of every sound-server
/// query (up to `pulse::CONNECT_TIMEOUT + OPERATION_TIMEOUT` each) and of the
/// WirePlumber restart included (#123). A profile write started before it
/// runs to its end (#264).
const A2DP_ACTIVATION_DEADLINE: Duration = Duration::from_secs(20);

// The sleeps alone never trip the deadline.
const _: () = assert!(
    A2DP_ACTIVATION_DEADLINE.as_millis() >= a2dp_activation_worst_case_wait().as_millis()
        && A2DP_ACTIVATION_DEADLINE.as_millis() >= A2DP_RECOVERY_BOUND.as_millis(),
    "A2DP_ACTIVATION_DEADLINE is shorter than the activation's sleeps"
);

// Nor do the sleeps plus a connect that runs to its timeout.
const _: () = assert!(
    A2DP_ACTIVATION_DEADLINE.as_millis()
        >= a2dp_activation_worst_case_wait()
            .saturating_add(A2DP_CONNECT_TIMEOUT)
            .as_millis(),
    "A2DP_ACTIVATION_DEADLINE is shorter than the sleeps and the A2DP connect"
);

/// A2DP activation ran out of `A2DP_ACTIVATION_DEADLINE`.
#[derive(Debug, PartialEq, Eq)]
struct DeadlineExceeded;

/// Run `step` with only the time left before `deadline`; never start it once
/// the deadline has passed.
async fn before_deadline<Fut: Future>(
    deadline: Instant,
    step: impl FnOnce() -> Fut,
) -> Result<Fut::Output, DeadlineExceeded> {
    if Instant::now() >= deadline {
        return Err(DeadlineExceeded);
    }
    // Deadline first: `timeout_at` polls the step before its timer, so a step
    // woken exactly at the deadline could still start another query.
    tokio::select! {
        biased;
        _ = tokio::time::sleep_until(deadline) => Err(DeadlineExceeded),
        out = step() => Ok(out),
    }
}

/// Start `step` only before `deadline`. Past it, never finish, so the
/// enclosing `before_deadline` ends there and nothing after `step` runs.
async fn started_before<Fut: Future>(deadline: Instant, step: impl FnOnce() -> Fut) -> Fut::Output {
    if Instant::now() >= deadline {
        std::future::pending::<()>().await;
    }
    step().await
}

/// Call `lookup` until it finds a card index, at most `attempts` times,
/// sleeping `interval` between tries (not after the last one). The lookup is
/// injected so the retry budget is testable without a sound server.
async fn retry_card_lookup<F, Fut>(attempts: u32, interval: Duration, lookup: F) -> Option<u32>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<u32>>,
{
    retry_card_lookup_with(attempts, interval, lookup, tokio::time::sleep).await
}

/// `retry_card_lookup` with the sleep injected, so tests can count sleeps.
/// Generic so the hold's connect can re-read the whole card.
async fn retry_card_lookup_with<T, F, Fut, S, SFut>(
    attempts: u32,
    interval: Duration,
    mut lookup: F,
    mut sleep: S,
) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
    S: FnMut(Duration) -> SFut,
    SFut: std::future::Future<Output = ()>,
{
    for attempt in 1..=attempts {
        if let Some(index) = lookup().await {
            if attempt > 1 {
                debug!("Found audio card after {attempt} attempts");
            }
            return Some(index);
        }
        if attempt < attempts {
            sleep(interval).await;
        }
    }
    None
}

/// Ask BlueZ for the A2DP profile once, cut at `A2DP_CONNECT_TIMEOUT`, then
/// look for the card it brings with `A2DP_CONNECT_LOOKUP_ATTEMPTS` tries of
/// `find`. `None` after one `warn!` naming the cause (#310), or one `debug!`
/// when the AirPods were not connected, so nothing was asked (#320).
async fn connect_a2dp_then_find<T, C, CFut, F, FFut, S, SFut>(
    connect: C,
    find: F,
    sleep: S,
) -> Option<T>
where
    C: FnOnce() -> CFut,
    CFut: Future<Output = Result<(), String>>,
    F: FnMut() -> FFut,
    FFut: Future<Output = Option<T>>,
    S: FnMut(Duration) -> SFut,
    SFut: Future<Output = ()>,
{
    // shortcut: a ConnectProfile cut here can outlive `profile_write` (#316):
    // it can still finish in BlueZ after the lock is released, so a later
    // hold may miss its card.
    let cause = match tokio::time::timeout(A2DP_CONNECT_TIMEOUT, connect()).await {
        Ok(Ok(())) => {
            let attempts = A2DP_CONNECT_LOOKUP_ATTEMPTS;
            let found = retry_card_lookup_with(attempts, CARD_LOOKUP_INTERVAL, find, sleep).await;
            if found.is_some() {
                return found;
            }
            "still no AirPods card".to_string()
        }
        Ok(Err(e)) if e == NOT_CONNECTED => {
            debug!("A2DP profile connect skipped: {e}");
            return None;
        }
        Ok(Err(e)) => e,
        Err(_) => format!("no reply within {A2DP_CONNECT_TIMEOUT:?}"),
    };
    warn!("A2DP profile connect gave up: {cause}");
    None
}

/// BlueZ `Device1.ConnectProfile(A2DP_SINK)` on the AirPods `mac`, only while
/// BlueZ reports them connected. Unbounded: callers cut it.
async fn connect_a2dp_profile(mac: &str) -> Result<(), String> {
    let address = mac.parse::<bluer::Address>().map_err(|e| e.to_string())?;
    let session = bluer::Session::new().await.map_err(|e| e.to_string())?;
    let device = session
        .default_adapter()
        .await
        .and_then(|adapter| adapter.device(address))
        .map_err(|e| e.to_string())?;
    connect_if_connected(
        || async { device.is_connected().await.unwrap_or(false) },
        || async { connect_profile_reply(device.connect_profile(&A2DP_SINK).await) },
    )
    .await
}

/// A connect already under way (`InProgress`) or done (`AlreadyConnected`)
/// still brings the card, so it goes on to the re-lookup like success (#321).
fn connect_profile_reply(reply: bluer::Result<()>) -> Result<(), String> {
    use bluer::ErrorKind::{AlreadyConnected, InProgress};
    match reply {
        Err(e) if !matches!(e.kind, InProgress | AlreadyConnected) => Err(e.to_string()),
        _ => Ok(()),
    }
}

/// Connect only while connected, so a hold released on a disconnect never
/// pages AirPods that left (#310 Q5).
async fn connect_if_connected<I, IFut, C, CFut>(is_connected: I, connect: C) -> Result<(), String>
where
    I: FnOnce() -> IFut,
    IFut: Future<Output = bool>,
    C: FnOnce() -> CFut,
    CFut: Future<Output = Result<(), String>>,
{
    if !is_connected().await {
        return Err(NOT_CONNECTED.to_string());
    }
    info!("No AirPods card: asking BlueZ to ConnectProfile A2DP Sink");
    connect().await
}

/// What `activate_a2dp_profile` should do with the card's profile.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ProfileChoice {
    /// An A2DP variant is already active. Switching would recreate the sink
    /// and stop the stream that triggered the activation.
    KeepActive(String),
    /// Switch the card to this profile.
    Activate(String),
    /// The card offers none of the A2DP profiles we know.
    Unavailable,
}

/// Pick the card profile to use. Pure, so the order is tested without a sound
/// server: an active `a2dp*` profile is kept whatever its codec, then an
/// available cached choice wins, then the first available entry of the
/// preferred codec's order.
fn select_a2dp_profile(
    available: &[String],
    active: Option<&str>,
    cached: Option<&str>,
    preferred: crate::utils::PreferredCodec,
) -> ProfileChoice {
    if let Some(active) = active
        && active.starts_with("a2dp")
    {
        return ProfileChoice::KeepActive(active.to_string());
    }
    let is_available = |profile: &str| available.iter().any(|a| a == profile);
    if let Some(cached) = cached.filter(|c| !c.is_empty() && is_available(c)) {
        return ProfileChoice::Activate(cached.to_string());
    }
    preferred
        .profile_order()
        .into_iter()
        .find(|profile| is_available(profile))
        .map_or(ProfileChoice::Unavailable, |profile| {
            ProfileChoice::Activate(profile.to_string())
        })
}

/// Longest the A2DP hold's release waits for the AirPods sink (#264).
const HOLD_RELEASE_WAIT: Duration = Duration::from_secs(3);

/// The A2DP hold's write (#264): `off`, only from an active A2DP profile, so
/// a call, a card already off or an unknown profile is left alone.
fn hold_profile(active: Option<&str>) -> Option<&'static str> {
    active.filter(|p| p.starts_with("a2dp")).map(|_| "off")
}

/// The hold's release (#264): the A2DP choice, only from `off`, so it never
/// breaks a call or undoes a profile the user chose.
fn release_profile(
    available: &[String],
    active: Option<&str>,
    cached: Option<&str>,
    preferred: crate::utils::PreferredCodec,
) -> Option<String> {
    match select_a2dp_profile(available, active, cached, preferred) {
        ProfileChoice::Activate(profile) if active == Some("off") => Some(profile),
        _ => None,
    }
}

/// Poll `sink` until it finds the AirPods sink, at most `HOLD_RELEASE_WAIT`.
async fn wait_for_sink<F, Fut>(mut sink: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<String>>,
{
    let found = tokio::time::timeout(HOLD_RELEASE_WAIT, async {
        while sink().await.is_none() {
            tokio::time::sleep(CARD_LOOKUP_INTERVAL).await;
        }
    })
    .await
    .is_ok();
    if !found {
        info!("A2DP hold released; no AirPods sink within {HOLD_RELEASE_WAIT:?}");
    }
    found
}

/// Make sure the card offers an A2DP profile: check once, then wait up to
/// `attempts` checks `interval` apart, then `restart` WirePlumber exactly once
/// and wait the same way again. No sleep follows the final check of a wait.
/// The check, the restart and the sleep are injected so the sequence is
/// testable without a sound server.
///
/// Worst case in production: every check after the first re-resolves the card
/// by MAC with a single lookup (`CARD_LOOKUP_ATTEMPTS_PER_CHECK`, no sleep),
/// so each wait is at most 4 × 1 s sleeps ≈ 4 s whether or not the card is
/// there. With the initial full lookup in `activate_a2dp_profile` (11 × 250 ms
/// ≈ 2.75 s) and the restart (systemctl + `WIREPLUMBER_SETTLE` 2 s) the whole
/// activation waits ≈ 12.75 s plus lookup and systemctl run time (≈ 14.5 s
/// when the A2DP profile connect's re-lookup runs too), within
/// `A2DP_RECOVERY_BOUND` (15 s); see `a2dp_activation_worst_case_wait`.
async fn recover_a2dp_availability<C, CFut, R, RFut, S, SFut>(
    attempts: u32,
    interval: Duration,
    mut available: C,
    mut restart: R,
    mut sleep: S,
) -> bool
where
    C: FnMut() -> CFut,
    CFut: std::future::Future<Output = bool>,
    R: FnMut() -> RFut,
    RFut: std::future::Future<Output = bool>,
    S: FnMut(Duration) -> SFut,
    SFut: std::future::Future<Output = ()>,
{
    if available().await {
        return true;
    }
    // A freshly connected card can show up before its profiles are
    // enumerated. Give that a grace period before restarting WirePlumber,
    // which interrupts playback for several seconds.
    warn!("A2DP profile not available yet, waiting for enumeration");
    if wait_for_a2dp_profile(attempts, interval, &mut available, &mut sleep).await {
        return true;
    }
    warn!("A2DP profile still missing, restarting WirePlumber as a last resort");
    restart().await && wait_for_a2dp_profile(attempts, interval, &mut available, &mut sleep).await
}

/// Up to `attempts` checks, `interval` apart, never sleeping after the last one.
async fn wait_for_a2dp_profile<C, CFut, S, SFut>(
    attempts: u32,
    interval: Duration,
    available: &mut C,
    sleep: &mut S,
) -> bool
where
    C: FnMut() -> CFut,
    CFut: std::future::Future<Output = bool>,
    S: FnMut(Duration) -> SFut,
    SFut: std::future::Future<Output = ()>,
{
    for attempt in 1..=attempts {
        if available().await {
            return true;
        }
        if attempt < attempts {
            sleep(interval).await;
        }
    }
    false
}

struct MediaControllerState {
    connected_device_mac: String,
    local_mac: String,
    is_playing: bool,
    paused_by_app_services: Vec<String>,
    device_index: Option<u32>,
    cached_a2dp_profile: String,
    old_in_ear_data: Vec<bool>,
    user_played_the_media: bool,
    i_paused_the_media: bool,
    ear_detection_enabled: bool,
    disconnect_when_not_wearing: bool,
    conv_original_volume: Option<u32>,
    conv_conversation_started: bool,
    playback_listener_running: bool,
}

impl MediaControllerState {
    fn new() -> Self {
        MediaControllerState {
            connected_device_mac: String::new(),
            local_mac: String::new(),
            is_playing: false,
            paused_by_app_services: Vec::new(),
            device_index: None,
            cached_a2dp_profile: String::new(),
            old_in_ear_data: vec![false, false],
            user_played_the_media: false,
            i_paused_the_media: false,
            ear_detection_enabled: true,
            disconnect_when_not_wearing: true,
            conv_original_volume: None,
            conv_conversation_started: false,
            playback_listener_running: false,
        }
    }
}

#[derive(Clone)]
pub struct MediaController {
    state: Arc<Mutex<MediaControllerState>>,
    /// `pause_all_media` calls in flight. Released under `state` together
    /// with the self-pause flag they set; read under `state` by the listener.
    ownership_pauses: Arc<AtomicUsize>,
    /// Ear-removal and ownership-loss pauses started, so the A2DP resume
    /// never undoes one (#327).
    self_pauses: Arc<AtomicUsize>,
    /// The handoff's A2DP hold is on (#264): `activate_a2dp_profile` writes
    /// nothing. Set synchronously by the shell, in emit order.
    a2dp_held: Arc<AtomicBool>,
    /// Serializes activation's last hold check + write with the hold's
    /// read + write, so a write in flight can't undo a hold (#264).
    profile_write: Arc<Mutex<()>>,
}

impl MediaController {
    pub fn new(connected_mac: String, local_mac: String) -> Self {
        let mut state = MediaControllerState::new();
        state.connected_device_mac = connected_mac;
        state.local_mac = local_mac;
        MediaController {
            state: Arc::new(Mutex::new(state)),
            ownership_pauses: Arc::new(AtomicUsize::new(0)),
            self_pauses: Arc::default(),
            a2dp_held: Arc::default(),
            profile_write: Arc::default(),
        }
    }

    /// The AirPods' sink name, if the sound server has one (#40).
    pub(crate) async fn airpods_sink(&self) -> Option<String> {
        let mac = self.state.lock().await.connected_device_mac.clone();
        get_sink_name_by_mac(&mac).await
    }

    pub async fn start_playback_listener(
        &self,
        aacp_manager: AACPManager,
        control_tx: tokio::sync::mpsc::UnboundedSender<(
            crate::bluetooth::aacp::ControlCommandIdentifiers,
            Vec<u8>,
        )>,
    ) {
        let mut state = self.state.lock().await;
        if state.playback_listener_running {
            debug!("Playback listener already running");
            return;
        }
        state.playback_listener_running = true;
        let connected_device_mac = state.connected_device_mac.clone();
        drop(state);

        let controller_clone = self.clone();
        let mut listeners = PLAYBACK_LISTENERS.lock().unwrap();
        if let Some(previous_listener) = listeners.remove(&connected_device_mac) {
            previous_listener.abort();
        }
        let listener = tokio::spawn(async move {
            controller_clone
                .playback_listener_loop(aacp_manager, control_tx)
                .await;
        });
        listeners.insert(connected_device_mac, listener.abort_handle());
    }

    async fn playback_listener_loop(
        &self,
        aacp_manager: AACPManager,
        control_tx: tokio::sync::mpsc::UnboundedSender<(
            crate::bluetooth::aacp::ControlCommandIdentifiers,
            Vec<u8>,
        )>,
    ) {
        info!("Starting playback listener loop");
        // The first poll only records the playback state; see
        // TakeoverSkip::InitialState.
        let mut baseline_taken = false;
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;

            let is_playing = tokio::task::spawn_blocking(|| Self::check_if_playing())
                .await
                .unwrap_or(false);

            // Snapshot the AACP state and release the guard straight away: the
            // takeover below awaits profile activation and packet sends, and
            // must not block incoming events or queued control commands. Taken
            // before (not inside) the playback-state lock so the two are never
            // nested. The guards still run in #724's order: decide_takeover
            // checks baseline and self-pause before looking at the ears.
            let (bud_in_ear, connected_devices) = {
                let aacp_state = aacp_manager.state.lock().await;
                (
                    aacp_state
                        .ear_detection_status
                        .contains(&EarDetectionStatus::InEar),
                    aacp_state.connected_devices.clone(),
                )
            };

            let (decision, local_mac) = self
                .record_playback_poll(baseline_taken, is_playing, bud_in_ear)
                .await;
            baseline_taken = true;

            if let TakeoverAction::Skip(reason) = decision.action {
                match reason {
                    // `is_playing` starts false, which is a placeholder rather
                    // than an observation. Whatever is already playing when this
                    // loop starts would otherwise read as playback that just
                    // began, and on a reconnect take the audio from a device the
                    // user was listening on.
                    TakeoverSkip::InitialState => {
                        debug!("Recorded initial playback state ({is_playing}); not a transition")
                    }
                    TakeoverSkip::NoTransition => {}
                    // Losing ownership pauses local players and drops the audio
                    // profile. Players tend to resume on their own once the
                    // profile comes back, and that resume is not the user asking
                    // for the audio; treating it as one starts a tug of war with
                    // the device that just took over.
                    TakeoverSkip::ResumeAfterOwnPause => {
                        debug!("Playback resumed after our own pause; not taking ownership")
                    }
                    TakeoverSkip::NoBudInEar => {
                        info!("Media playback started but buds not in ear, skipping takeover")
                    }
                    TakeoverSkip::OwnPausePending => {
                        debug!("Playback started during our own pause; not taking ownership")
                    }
                    // Only produced by the #841 guards, which this loop does
                    // not supply (`handoff: None`).
                    TakeoverSkip::AwaitingNewPlayback
                    | TakeoverSkip::NoSession
                    | TakeoverSkip::NoRemoteHost
                    | TakeoverSkip::AudioNotIdle
                    | TakeoverSkip::RemoteCategoryHigher { .. } => {}
                }
                continue;
            }

            info!("Media playback started, taking ownership and activating a2dp");
            let _ = control_tx.send((
                crate::bluetooth::aacp::ControlCommandIdentifiers::OwnsConnection,
                vec![0x01],
            ));
            self.activate_a2dp_profile().await;

            info!("already connected locally, hijacking connection by asking AirPods");

            for device in connected_devices {
                if device.mac != local_mac {
                    if let Err(e) = aacp_manager
                        .send_media_information(&local_mac, &device.mac, true)
                        .await
                    {
                        error!("Failed to send media information to {}: {}", device.mac, e);
                    }
                    if let Err(e) = aacp_manager.send_smart_routing_show_ui(&device.mac).await {
                        error!(
                            "Failed to send smart routing show ui to {}: {}",
                            device.mac, e
                        );
                    }
                    if let Err(e) = aacp_manager.send_hijack_request(&device.mac).await {
                        error!("Failed to send hijack request to {}: {}", device.mac, e);
                    }
                }
            }

            debug!("completed playback takeover process");
        }
    }

    /// The listener's per-poll state step: record `is_playing`, decide, and
    /// consume the self-pause flag. The flag and the ownership-pause count
    /// are read under one `state` lock, and `pause_all_media` releases its
    /// count under the same lock as it sets the flag, so a poll never sees
    /// the pause finished without its flag.
    async fn record_playback_poll(
        &self,
        baseline_taken: bool,
        is_playing: bool,
        bud_in_ear: bool,
    ) -> (TakeoverDecision, String) {
        let mut state = self.state.lock().await;
        let was_playing = state.is_playing;
        state.is_playing = is_playing;
        let decision = decide_legacy_poll(
            TakeoverInputs {
                baseline_taken,
                was_playing,
                is_playing,
                i_paused_the_media: state.i_paused_the_media,
                bud_in_ear,
                trigger: TakeoverTrigger::PlaybackPoll,
                // The #841 routing guards are for the handoff core (#8/#9).
                handoff: None,
            },
            self.ownership_pauses.load(Ordering::SeqCst) > 0,
        );
        if decision.clear_self_pause {
            state.i_paused_the_media = false;
        }
        (decision, state.local_mac.clone())
    }

    pub(crate) fn check_if_playing() -> bool {
        let conn = match Connection::new_session() {
            Ok(c) => c,
            Err(_) => return false,
        };
        Self::list_mpris_services(&conn)
            .iter()
            .any(|service| Self::mpris_playing(&conn, service))
    }

    /// The MPRIS players (KDE Connect's excluded) that report Playing.
    /// Blocking.
    fn playing_mpris_players() -> Vec<String> {
        let Ok(conn) = Connection::new_session() else {
            return Vec::new();
        };
        Self::list_mpris_services(&conn)
            .into_iter()
            .filter(|service| Self::mpris_playing(&conn, service))
            .collect()
    }

    /// The bus owner PID of each MPRIS player (KDE Connect's excluded) that
    /// reports Playing; None where the bus can't tell. Blocking.
    pub(crate) fn playing_mpris_owners() -> Vec<Option<u32>> {
        let Ok(conn) = Connection::new_session() else {
            return Vec::new();
        };
        Self::list_mpris_services(&conn)
            .iter()
            .filter(|service| Self::mpris_playing(&conn, service))
            .map(|service| Self::bus_owner_pid(&conn, service))
            .collect()
    }

    /// The PID of the process owning bus name `service`; None when the bus
    /// can't tell.
    fn bus_owner_pid(conn: &Connection, service: &str) -> Option<u32> {
        conn.with_proxy(
            "org.freedesktop.DBus",
            "/org/freedesktop/DBus",
            Duration::from_secs(5),
        )
        .method_call(
            "org.freedesktop.DBus",
            "GetConnectionUnixProcessID",
            (service,),
        )
        .map(|(pid,): (u32,)| pid)
        .ok()
    }

    /// The MPRIS players (KDE Connect's excluded) that report Paused.
    /// Blocking.
    fn paused_mpris_players() -> Vec<String> {
        let Ok(conn) = Connection::new_session() else {
            return Vec::new();
        };
        Self::list_mpris_services(&conn)
            .into_iter()
            .filter(|service| {
                conn.with_proxy(service, "/org/mpris/MediaPlayer2", Duration::from_secs(5))
                    .get::<String>("org.mpris.MediaPlayer2.Player", "PlaybackStatus")
                    .is_ok_and(|s| s == "Paused")
            })
            .collect()
    }

    fn mpris_playing(conn: &Connection, service: &str) -> bool {
        let proxy = conn.with_proxy(service, "/org/mpris/MediaPlayer2", Duration::from_secs(5));
        proxy
            .get::<String>("org.mpris.MediaPlayer2.Player", "PlaybackStatus")
            .map(|s| s == "Playing")
            .unwrap_or(false)
    }

    fn is_kdeconnect_service(service: &str) -> bool {
        service.starts_with("org.mpris.MediaPlayer2.kdeconnect.mpris_")
    }

    pub async fn handle_ear_detection(
        &self,
        old_statuses: Vec<EarDetectionStatus>,
        new_statuses: Vec<EarDetectionStatus>,
    ) {
        self.handle_ear_detection_with(old_statuses, new_statuses, Self::pause_mpris_players)
            .await;
    }

    /// `handle_ear_detection` with the blocking MPRIS pause injected, so the
    /// self-pause marking can be tested without a session bus.
    async fn handle_ear_detection_with<F>(
        &self,
        old_statuses: Vec<EarDetectionStatus>,
        new_statuses: Vec<EarDetectionStatus>,
        pause_players: F,
    ) where
        F: FnOnce() -> (Vec<String>, PauseOutcome) + Clone + Send + 'static,
    {
        debug!(
            "Entering handle_ear_detection with old_statuses: {:?}, new_statuses: {:?}",
            old_statuses, new_statuses
        );

        let old_in_ear_data: Vec<bool> = old_statuses
            .iter()
            .map(|s| *s == EarDetectionStatus::InEar)
            .collect();
        let new_in_ear_data: Vec<bool> = new_statuses
            .iter()
            .map(|s| *s == EarDetectionStatus::InEar)
            .collect();

        let in_ear = new_in_ear_data.iter().all(|&b| b);
        let old_all_out = old_in_ear_data.iter().all(|&b| !b);
        let new_has_at_least_one_in = new_in_ear_data.iter().any(|&b| b);
        let new_all_out = new_in_ear_data.iter().all(|&b| !b);

        debug!(
            "Computed states: in_ear={}, old_all_out={}, new_has_at_least_one_in={}, new_all_out={}",
            in_ear, old_all_out, new_has_at_least_one_in, new_all_out
        );

        {
            let state = self.state.lock().await;
            if !state.ear_detection_enabled {
                debug!("Ear detection disabled, skipping");
                return;
            }
        }

        // No previous reading means this is the first report after connecting,
        // not a change the wearer made. It matters because `old_all_out` is
        // vacuously true for an empty list, so an already-worn pair looks like
        // it was just put in: playback resumes, and with a second device
        // connected the resume escalates into taking the audio away from it.
        // Record the baseline and wait for a real transition instead.
        if old_statuses.is_empty() {
            debug!("First ear reading after connecting: recording baseline, not acting");
            let mut state = self.state.lock().await;
            state.old_in_ear_data = new_in_ear_data;
            return;
        }

        // Both buds out pauses twice (below and in the not-fully-in branch);
        // the first pause is the one that finds the players playing.
        let mut removal_self_paused = false;
        if new_has_at_least_one_in && old_all_out {
            debug!("Condition met: buds inserted, activating A2DP and checking play state");
            self.activate_a2dp_profile().await;
            {
                let mut state = self.state.lock().await;
                if state.is_playing {
                    state.user_played_the_media = true;
                    debug!("Set user_played_the_media to true as media was playing");
                }
            }
        } else if new_all_out {
            debug!("Condition met: buds removed, pausing media");
            let was_playing = self.state.lock().await.is_playing;
            removal_self_paused =
                marks_self_pause(self.pause_with(pause_players.clone()).await, was_playing);
            {
                let state = self.state.lock().await;
                if state.disconnect_when_not_wearing {
                    debug!("Disconnect when not wearing enabled, deactivating A2DP");
                    drop(state);
                    self.deactivate_a2dp_profile().await;
                }
            }
        }

        let reset_user_played = (old_in_ear_data.iter().any(|&b| !b)
            && new_in_ear_data.iter().all(|&b| b))
            || (new_in_ear_data.iter().any(|&b| !b) && old_in_ear_data.iter().all(|&b| b));
        if reset_user_played {
            debug!("Transition detected, resetting user_played_the_media");
            let mut state = self.state.lock().await;
            state.user_played_the_media = false;
        }

        info!(
            "Ear Detection - old_in_ear_data: {:?}, new_in_ear_data: {:?}",
            old_in_ear_data, new_in_ear_data
        );

        let mut old_sorted = old_in_ear_data.clone();
        old_sorted.sort();
        let mut new_sorted = new_in_ear_data.clone();
        new_sorted.sort();
        if new_sorted != old_sorted {
            debug!("Ear data changed, checking resume/pause logic");
            if in_ear {
                debug!("Resuming media as buds are in ear");
                self.resume().await;
                self.state.lock().await.i_paused_the_media = false;
            } else if !old_all_out {
                debug!("Pausing media as buds are not fully in ear");
                let was_playing = self.state.lock().await.is_playing;
                let outcome = self.pause_with(pause_players).await;
                self.state.lock().await.i_paused_the_media |=
                    removal_self_paused || marks_self_pause(outcome, was_playing);
            } else {
                debug!("Playing media");
                self.resume().await;
                self.state.lock().await.i_paused_the_media = false;
            }
        }

        {
            let mut state = self.state.lock().await;
            state.old_in_ear_data = new_in_ear_data;
            debug!("Updated old_in_ear_data to {:?}", state.old_in_ear_data);
        }
    }

    /// Number of one-second attempts to wait for A2DP profiles to be enumerated.
    const A2DP_ENUMERATION_ATTEMPTS: u32 = 5;
    const A2DP_ENUMERATION_INTERVAL: Duration = Duration::from_secs(1);

    /// Profiles the card offers and its active profile, from one snapshot.
    /// The active profile is used to avoid pointless switches.
    async fn get_card_profiles(&self, card_index: u32) -> (Vec<String>, Option<String>) {
        tokio::task::spawn_blocking(move || {
            get_card_info_list_sync()
                .into_iter()
                .find(|c| c.index == card_index)
                .map(|c| (c.profiles, c.active_profile))
                .unwrap_or_default()
        })
        .await
        .unwrap_or_default()
    }

    /// One availability check inside the A2DP enumeration waits. The first
    /// check reuses the index resolved before the wait. Every later check
    /// re-resolves the card (a reconnect can change its index) with a
    /// `CARD_LOOKUP_ATTEMPTS_PER_CHECK` lookup, stores it, and asks for A2DP
    /// only if the card is there. Nesting the full ~3 s poll in each check
    /// made the worst case ~43 s (#32).
    async fn a2dp_enumeration_check<L, LFut, S, SFut, A, AFut>(
        &self,
        first: bool,
        lookup: L,
        sleep: S,
        available: A,
    ) -> bool
    where
        L: FnMut() -> LFut,
        LFut: std::future::Future<Output = Option<u32>>,
        S: FnMut(Duration) -> SFut,
        SFut: std::future::Future<Output = ()>,
        A: FnOnce() -> AFut,
        AFut: std::future::Future<Output = bool>,
    {
        if !first {
            let index = retry_card_lookup_with(
                CARD_LOOKUP_ATTEMPTS_PER_CHECK,
                CARD_LOOKUP_INTERVAL,
                lookup,
                sleep,
            )
            .await;
            self.state.lock().await.device_index = index;
            if index.is_none() {
                return false;
            }
        }
        available().await
    }

    async fn restart_wire_plumber(&self) -> bool {
        info!("Restarting WirePlumber to rediscover A2DP profiles");
        let result = tokio::process::Command::new("systemctl")
            .args(["--user", "restart", "wireplumber"])
            // The activation deadline drops this future; don't leave systemctl behind.
            .kill_on_drop(true)
            .output()
            .await;

        match result {
            Ok(output) if output.status.success() => {
                info!("WirePlumber restarted successfully");
                tokio::time::sleep(WIREPLUMBER_SETTLE).await;
                true
            }
            _ => {
                error!("Failed to restart WirePlumber. Do you use wireplumber?");
                false
            }
        }
    }

    pub async fn activate_a2dp_profile(&self) {
        debug!("Entering activate_a2dp_profile");
        let deadline = Instant::now() + A2DP_ACTIVATION_DEADLINE;

        let mac = self.state.lock().await.connected_device_mac.clone();
        if mac.is_empty() {
            warn!("Connected device MAC is empty, cannot activate A2DP profile");
            return;
        }

        let outcome = self
            .activate_a2dp_profile_with(
                deadline,
                || self.get_audio_device_index(&mac),
                || {
                    connect_a2dp_then_find(
                        || connect_a2dp_profile(&mac),
                        || find_card_index(mac.clone()),
                        tokio::time::sleep,
                    )
                },
                || {
                    self.recover_a2dp_availability_by(
                        deadline,
                        || find_card_index(mac.clone()),
                        || self.is_a2dp_profile_available(),
                        || self.restart_wire_plumber(),
                    )
                },
                |idx| self.get_preferred_a2dp_profile(idx),
                |idx, profile| async move {
                    tokio::task::spawn_blocking(move || set_card_profile_sync(idx, &profile))
                        .await
                        .unwrap_or(false)
                },
            )
            .await;
        if outcome == Err(DeadlineExceeded) {
            warn!("A2DP activation gave up: not done within {A2DP_ACTIVATION_DEADLINE:?}");
        }
    }

    /// `recover_a2dp_availability` with the production checks, bounded by
    /// `deadline`: no step starts once it has passed, and recovery returns
    /// false by then. A restart cut off by the deadline is a failed restart.
    /// `find` is the one-shot card scan of each re-check (#68), `available`
    /// the A2DP query.
    async fn recover_a2dp_availability_by<F, FFut, A, AFut, R, RFut>(
        &self,
        deadline: Instant,
        find: F,
        available: A,
        restart: R,
    ) -> bool
    where
        F: Fn() -> FFut,
        FFut: Future<Output = Option<u32>>,
        A: Fn() -> AFut,
        AFut: Future<Output = bool>,
        R: Fn() -> RFut,
        RFut: Future<Output = bool>,
    {
        let (find, available, restart) = (&find, &available, &restart);
        let mut first_check = true;
        before_deadline(deadline, || {
            recover_a2dp_availability(
                Self::A2DP_ENUMERATION_ATTEMPTS,
                Self::A2DP_ENUMERATION_INTERVAL,
                move || {
                    self.a2dp_enumeration_check(
                        std::mem::replace(&mut first_check, false),
                        move || started_before(deadline, find),
                        tokio::time::sleep,
                        move || started_before(deadline, available),
                    )
                },
                move || async move { before_deadline(deadline, restart).await.unwrap_or(false) },
                tokio::time::sleep,
            )
        })
        .await
        .unwrap_or(false)
    }

    /// The activation after the MAC check, every step bounded by `deadline`:
    /// `lookup` is the initial full card lookup, `connect` asks BlueZ for the
    /// A2DP profile and finds the card again when the lookup found none,
    /// `recover` makes sure A2DP is offered, `choose` picks the profile and
    /// `set` switches to it. State is written only after a step returns, so a
    /// step cut off writes nothing.
    async fn activate_a2dp_profile_with<L, LFut, K, KFut, V, VFut, C, CFut, P, PFut>(
        &self,
        deadline: Instant,
        lookup: L,
        connect: K,
        recover: V,
        choose: C,
        set: P,
    ) -> Result<(), DeadlineExceeded>
    where
        L: FnOnce() -> LFut,
        LFut: Future<Output = Option<u32>>,
        K: FnOnce() -> KFut,
        KFut: Future<Output = Option<u32>>,
        V: FnOnce() -> VFut,
        VFut: Future<Output = bool>,
        C: FnOnce(u32) -> CFut,
        CFut: Future<Output = ProfileChoice>,
        P: FnOnce(u32, String) -> PFut,
        PFut: Future<Output = bool>,
    {
        if self.a2dp_hold_skips() {
            return Ok(());
        }
        // Always resolve the card by MAC first: a reconnect can change its index.
        let index = before_deadline(deadline, lookup).await?;
        self.state.lock().await.device_index = index;
        if index.is_none() {
            // BlueZ never connected A2DP (inbound connect). Under the write
            // lock, so a hold that starts now wins (#264).
            let _write = before_deadline(deadline, || self.profile_write.lock()).await?;
            if self.a2dp_hold_skips() {
                return Ok(());
            }
            let index = before_deadline(deadline, connect).await?;
            self.state.lock().await.device_index = index;
            if index.is_none() {
                return Ok(());
            }
        }

        // shortcut: no hold check before `recover` (WirePlumber restart) (#288).
        if !before_deadline(deadline, recover).await? {
            if Instant::now() >= deadline {
                return Err(DeadlineExceeded);
            }
            error!("A2DP profile unavailable, skipping profile activation");
            return Ok(());
        }

        let Some(idx) = self.state.lock().await.device_index else {
            error!("Device index not available for activating profile.");
            return Ok(());
        };

        let preferred_profile = match before_deadline(deadline, || choose(idx)).await? {
            // Leave an already-active A2DP variant alone. Switching profiles
            // recreates the PipeWire sink and stops the stream that triggered
            // this activation, so players pause again right after the user
            // pressed play.
            ProfileChoice::KeepActive(active) => {
                debug!(
                    "A2DP profile {} already active, leaving it unchanged",
                    active
                );
                return Ok(());
            }
            ProfileChoice::Unavailable => {
                error!("No suitable A2DP profile found");
                return Ok(());
            }
            ProfileChoice::Activate(profile) => profile,
        };

        // A hold that started during the activation wins (#264); the lock
        // keeps the hold's card read behind this write.
        let _write = before_deadline(deadline, || self.profile_write.lock()).await?;
        if self.a2dp_hold_skips() {
            return Ok(());
        }
        info!("Activating A2DP profile for AirPods: {}", preferred_profile);
        // Awaited to its end, past the deadline too: a dropped wait would let
        // the blocking write land after the lock is gone, under a hold (#264).
        // Its own sound-server timeouts bound it.
        if set(idx, preferred_profile.clone()).await {
            info!("Successfully activated A2DP profile: {}", preferred_profile);
        } else {
            warn!("Failed to activate A2DP profile: {}", preferred_profile);
        }
        Ok(())
    }

    async fn pause_with<F>(&self, pause_players: F) -> PauseOutcome
    where
        F: FnOnce() -> (Vec<String>, PauseOutcome) + Send + 'static,
    {
        debug!("Pausing playback");
        // Counted before the pause, so the A2DP resume never sees it uncounted.
        self.self_pauses.fetch_add(1, Ordering::SeqCst);

        let (paused_services, outcome) = tokio::task::spawn_blocking(pause_players).await.unwrap();

        if !paused_services.is_empty() {
            info!("Paused {} media player(s) via DBus", paused_services.len());
            let mut state = self.state.lock().await;
            state.paused_by_app_services = paused_services;
            state.is_playing = false;
        } else {
            info!("No playing media players found to pause");
        }
        outcome
    }

    pub async fn pause_all_media(&self) {
        self.pause_all_media_with(|| Self::pause_playing_mpris_players(&|_| true))
            .await;
    }

    /// `pause_all_media` with the D-Bus side injected, so the self-pause
    /// marking can be tested without a session bus.
    pub(crate) async fn pause_all_media_with<F>(&self, pause_players: F) -> PauseOutcome
    where
        F: FnOnce() -> PauseOutcome + Send + 'static,
    {
        debug!("Pausing all media (without tracking for resume)");

        let pending = OwnershipPause::start(&self.ownership_pauses);
        let was_playing = self.state.lock().await.is_playing;
        // Counted before the pause, so the A2DP resume never sees it uncounted.
        self.self_pauses.fetch_add(1, Ordering::SeqCst);
        let outcome = tokio::task::spawn_blocking(pause_players).await.unwrap();

        let mut state = self.state.lock().await;
        // Remember that the pause came from us, so the playback listener does
        // not mistake the players coming back for the user starting something.
        state.i_paused_the_media |= marks_self_pause(outcome, was_playing);
        // Under the same lock as the flag; see record_playback_poll.
        drop(pending);
        if outcome.paused > 0 {
            info!(
                "Paused {} media player(s) due to ownership loss",
                outcome.paused
            );
            state.is_playing = false;
        } else {
            debug!("No playing media players found to pause");
        }
        outcome
    }

    /// Pause every MPRIS player that reports `Playing` and whose bus owner
    /// PID (None when the bus can't tell) `own` accepts. Blocking: run it on
    /// a blocking thread.
    pub(crate) fn pause_playing_mpris_players(own: &dyn Fn(Option<u32>) -> bool) -> PauseOutcome {
        Self::pause_mpris_players_owned(own).1
    }

    /// Pause every MPRIS player that reports `Playing`, also returning the
    /// paused services.
    fn pause_mpris_players() -> (Vec<String>, PauseOutcome) {
        Self::pause_mpris_players_owned(&|_| true)
    }

    /// `pause_playing_mpris_players`, also returning the paused services.
    fn pause_mpris_players_owned(own: &dyn Fn(Option<u32>) -> bool) -> (Vec<String>, PauseOutcome) {
        let mut paused_services = Vec::new();
        let mut outcome = PauseOutcome::default();
        let Ok(conn) = Connection::new_session() else {
            outcome.bus_unavailable = true;
            return (paused_services, outcome);
        };

        for service in Self::list_mpris_services(&conn) {
            let proxy =
                conn.with_proxy(&service, "/org/mpris/MediaPlayer2", Duration::from_secs(5));
            if let Ok(playback_status) =
                proxy.get::<String>("org.mpris.MediaPlayer2.Player", "PlaybackStatus")
                && playback_status == "Playing"
                && own(Self::bus_owner_pid(&conn, &service))
            {
                if proxy
                    .method_call::<(), _, &str, &str>("org.mpris.MediaPlayer2.Player", "Pause", ())
                    .is_ok()
                {
                    info!("Paused playback for: {}", service);
                    paused_services.push(service);
                } else {
                    error!("Failed to pause {}", service);
                    outcome.failed += 1;
                }
            }
        }
        outcome.paused = paused_services.len();
        (paused_services, outcome)
    }

    async fn resume(&self) {
        debug!("Resuming playback");
        let services = self.state.lock().await.paused_by_app_services.clone();

        if services.is_empty() {
            debug!("No services to resume");
            return;
        }

        let resumed_count = tokio::task::spawn_blocking(move || {
            let conn = match Connection::new_session() {
                Ok(c) => c,
                Err(_) => return 0,
            };
            let mut resumed_count = 0;
            for service in services {
                let proxy =
                    conn.with_proxy(&service, "/org/mpris/MediaPlayer2", Duration::from_secs(5));
                if proxy
                    .method_call::<(), _, &str, &str>("org.mpris.MediaPlayer2.Player", "Play", ())
                    .is_ok()
                {
                    info!("Resumed playback for: {}", service);
                    resumed_count += 1;
                } else {
                    warn!("Failed to resume {}", service);
                }
            }
            resumed_count
        })
        .await
        .unwrap();

        if resumed_count > 0 {
            info!("Resumed {} media player(s) via DBus", resumed_count);
            self.state.lock().await.paused_by_app_services.clear();
        } else {
            error!("Failed to resume any media players via DBus");
        }
    }

    pub async fn next_track(&self) {
        info!("Skipping to next track");
        self.mpris_command("Next").await;
    }

    pub async fn previous_track(&self) {
        info!("Going to previous track");
        self.mpris_command("Previous").await;
    }

    async fn is_a2dp_profile_available(&self) -> bool {
        let index = match self.state.lock().await.device_index {
            Some(i) => i,
            None => {
                debug!("Device index is None, returning false");
                return false;
            }
        };

        tokio::task::spawn_blocking(move || {
            let available = get_card_info_list_sync()
                .iter()
                .find(|c| c.index == index)
                .map(|card| card.profiles.iter().any(|p| p.starts_with("a2dp-sink")))
                .unwrap_or(false);
            debug!("A2DP profile available: {}", available);
            available
        })
        .await
        .unwrap_or(false)
    }

    /// Gathers the inputs of `select_a2dp_profile` (one card snapshot, the
    /// cached choice and the codec setting) and caches a new choice.
    async fn get_preferred_a2dp_profile(&self, card_index: u32) -> ProfileChoice {
        let (available, active) = self.get_card_profiles(card_index).await;
        let cached_profile = self.state.lock().await.cached_a2dp_profile.clone();

        let choice = select_a2dp_profile(
            &available,
            active.as_deref(),
            Some(&cached_profile),
            crate::utils::load_preferred_codec(),
        );
        match &choice {
            ProfileChoice::Activate(profile) if *profile == cached_profile => {
                debug!("Using cached A2DP profile: {}", profile);
            }
            ProfileChoice::Activate(profile) => {
                info!("Selected best available A2DP profile: {}", profile);
                self.state.lock().await.cached_a2dp_profile = profile.clone();
            }
            ProfileChoice::KeepActive(_) => {}
            ProfileChoice::Unavailable => debug!("No suitable profile found"),
        }
        choice
    }

    async fn get_audio_device_index(&self, mac: &str) -> Option<u32> {
        // Taking the connection back from another device makes the earbuds
        // renegotiate, and the card briefly disappears from the sound server
        // while that happens. A single lookup can land in that window and give
        // up, leaving the audio nowhere: ownership already taken from the other
        // device, but no local profile to play through. So poll for a moment.
        if mac.is_empty() {
            return None;
        }

        let found = retry_card_lookup(CARD_LOOKUP_ATTEMPTS, CARD_LOOKUP_INTERVAL, || {
            find_card_index(mac.to_string())
        })
        .await;

        match found {
            Some(index) => {
                info!("Found audio device index for MAC {}: {}", mac, index);
                Some(index)
            }
            None => {
                error!(
                    "No matching Bluetooth card found for MAC address: {} after {} attempts ({:?})",
                    mac,
                    CARD_LOOKUP_ATTEMPTS,
                    card_lookup_max_wait(CARD_LOOKUP_ATTEMPTS)
                );
                None
            }
        }
    }

    pub async fn deactivate_a2dp_profile(&self) {
        self.deactivate_a2dp_profile_with(
            |mac| async move { self.get_audio_device_index(&mac).await },
            |index| set_card_profile_sync(index, "off"),
        )
        .await
    }

    /// `lookup` resolves the card by MAC; `set_off` turns the profile off.
    async fn deactivate_a2dp_profile_with<L, LFut, S>(&self, lookup: L, set_off: S)
    where
        L: FnOnce(String) -> LFut,
        LFut: std::future::Future<Output = Option<u32>>,
        S: FnOnce(u32) -> bool + Send + 'static,
    {
        debug!("Entering deactivate_a2dp_profile");
        // Snapshot and drop the guard: the lookup can poll for ~3 s (#33).
        let (mac, cached) = {
            let state = self.state.lock().await;
            (state.connected_device_mac.clone(), state.device_index)
        };
        let index = match cached {
            Some(index) => Some(index),
            None => {
                let index = lookup(mac.clone()).await;
                self.state.lock().await.device_index = index;
                index
            }
        };

        let device_index = match index {
            Some(index) if !mac.is_empty() => index,
            _ => {
                warn!("Connected device MAC or index is empty, cannot deactivate A2DP profile");
                return;
            }
        };

        info!("Deactivating A2DP profile for AirPods by setting to off");

        let success = tokio::task::spawn_blocking(move || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| set_off(device_index)))
                .unwrap_or_else(|e| {
                    warn!("Panic in set_card_profile_sync: {:?}", e);
                    false
                })
        })
        .await
        .unwrap_or(false);

        if success {
            info!("Successfully deactivated A2DP profile");
        } else {
            warn!("Failed to deactivate A2DP profile");
        }
    }

    /// Mark the A2DP hold on or off (#264). The handoff shell calls it in
    /// emit order, before `apply_a2dp_hold` runs on the router's worker.
    pub(crate) fn set_a2dp_held(&self, held: bool) {
        self.a2dp_held.store(held, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) async fn i_paused_the_media(&self) -> bool {
        self.state.lock().await.i_paused_the_media
    }

    /// The A2DP hold is on, so an activation must not write (#264).
    fn a2dp_hold_skips(&self) -> bool {
        let held = self.a2dp_held.load(Ordering::SeqCst);
        if held {
            info!("A2DP activation skipped: another host owns the AirPods (A2DP hold)");
        }
        held
    }

    /// Turn the card `off` (`hold`), or back to the A2DP choice from `off`,
    /// from one card read (#264). A release then waits for the AirPods sink,
    /// at most `HOLD_RELEASE_WAIT`. Never suspends a sink or moves streams.
    pub(crate) async fn apply_a2dp_hold(&self, hold: bool) {
        let (mac, cached) = {
            let state = self.state.lock().await;
            (
                state.connected_device_mac.clone(),
                state.cached_a2dp_profile.clone(),
            )
        };
        // An empty MAC would match the first card with a device string.
        if mac.is_empty() {
            debug!("A2DP hold({hold}): no connected device MAC");
            return;
        }
        let read = || {
            let card_mac = mac.clone();
            async move {
                tokio::task::spawn_blocking(move || {
                    get_card_info_list_sync().into_iter().find(|card| {
                        card.device_string
                            .as_deref()
                            .is_some_and(|d| d.contains(&card_mac))
                    })
                })
                .await
                .ok()
                .flatten()
            }
        };
        let connect =
            || connect_a2dp_then_find(|| connect_a2dp_profile(&mac), read, tokio::time::sleep);
        let write =
            |index, profile: String| blocking(move || set_card_profile_sync(index, &profile));
        let preferred = crate::utils::load_preferred_codec();
        self.apply_a2dp_hold_with(hold, &cached, preferred, read, connect, write, || {
            get_sink_name_by_mac(&mac)
        })
        .await;
    }

    /// `apply_a2dp_hold` with the card read, the A2DP profile connect (with
    /// its card re-read), the profile write and the sink lookup injected.
    #[allow(clippy::too_many_arguments)]
    async fn apply_a2dp_hold_with<R, RFut, K, KFut, W, WFut, S, SFut>(
        &self,
        hold: bool,
        cached: &str,
        preferred: crate::utils::PreferredCodec,
        read: R,
        connect: K,
        write: W,
        sink: S,
    ) where
        R: FnOnce() -> RFut,
        RFut: Future<Output = Option<pulse::Card>>,
        K: FnOnce() -> KFut,
        KFut: Future<Output = Option<pulse::Card>>,
        W: FnOnce(u32, String) -> WFut,
        WFut: Future<Output = bool>,
        S: FnMut() -> SFut,
        SFut: Future<Output = Option<String>>,
    {
        // Held from the read through the write, not during the sink wait.
        let write_lock = self.profile_write.lock().await;
        let card = match read().await {
            Some(card) => card,
            // A release with no card: BlueZ never connected A2DP (#310).
            None if !hold && !self.a2dp_held.load(Ordering::SeqCst) => {
                let Some(card) = connect().await else { return };
                card
            }
            None => {
                debug!("A2DP hold({hold}): no AirPods card");
                return;
            }
        };
        let active = card.active_profile.as_deref();
        let profile = if hold {
            hold_profile(active).map(str::to_string)
        } else {
            release_profile(&card.profiles, active, Some(cached), preferred)
        };
        let Some(profile) = profile else {
            debug!("A2DP hold({hold}): card profile {active:?} left as is");
            // An activation in the gap after the flag cleared already turned
            // A2DP on: a current release still waits for the sink, so a
            // queued Take finds it.
            let a2dp_on = active.is_some_and(|p| p.starts_with("a2dp"));
            if !hold && a2dp_on && !self.a2dp_held.load(Ordering::SeqCst) {
                drop(write_lock);
                wait_for_sink(sink).await;
            }
            return;
        };
        // Checked right before the write: the opposite hold may have been
        // emitted during the awaits above and is queued behind this one, so
        // writing now would only flap the transport (#267).
        if self.a2dp_held.load(Ordering::SeqCst) != hold {
            debug!("A2DP hold({hold}): superseded, card left as is");
            return;
        }
        let written = write(card.index, profile.clone()).await;
        drop(write_lock);
        if !written {
            warn!("A2DP hold({hold}): failed to set the card to {profile}");
            return;
        }
        info!("A2DP hold({hold}): card set to {profile}");
        if !hold {
            wait_for_sink(sink).await;
        }
    }

    pub async fn handle_conversational_awareness(&self, status: u8) {
        debug!(
            "Entering handle_conversational_awareness with status: {}",
            status
        );

        let mac = self.state.lock().await.connected_device_mac.clone();
        if mac.is_empty() {
            debug!("No connected device MAC, skipping conversational awareness");
            return;
        }

        let sink = match get_sink_name_by_mac(&mac).await {
            Some(s) => s,
            None => {
                warn!(
                    "Could not find sink for MAC {}, skipping conversational awareness",
                    mac
                );
                return;
            }
        };

        let current_volume_opt = tokio::task::spawn_blocking({
            let sink = sink.clone();
            move || get_sink_volume_percent_by_name_sync(&sink)
        })
        .await
        .unwrap_or(None);

        match status {
            1 => {
                let original = current_volume_opt.unwrap_or(0);
                debug!("Conversation start (1). Current volume: {}", original);
                {
                    let mut state = self.state.lock().await;
                    if !state.conv_conversation_started {
                        state.conv_original_volume = Some(original);
                        state.conv_conversation_started = true;
                    } else {
                        debug!(
                            "Conversation already started; not overwriting conv_original_volume"
                        );
                    }
                }
                if original > 25 {
                    let sink_clone = sink.clone();
                    tokio::task::spawn_blocking(move || transition_sink_volume(&sink_clone, 25))
                        .await
                        .unwrap_or(false);
                    info!(
                        "Conversation start: lowered volume to 25% (original {})",
                        original
                    );
                } else {
                    debug!("Original volume {} <= 25, not reducing to 25", original);
                }
            }
            2 => {
                let original = self.state.lock().await.conv_original_volume;
                if let Some(orig) = original {
                    debug!("Conversation reduce (2). Original: {}", orig);
                    if orig > 15 {
                        let sink_clone = sink.clone();
                        tokio::task::spawn_blocking(move || {
                            transition_sink_volume(&sink_clone, 15)
                        })
                        .await
                        .unwrap_or(false);
                        info!(
                            "Conversation reduce: lowered volume to 15% (original {})",
                            orig
                        );
                    } else {
                        debug!("Original {} <= 15, not reducing to 15", orig);
                    }
                } else {
                    debug!("No original volume known for status 2, skipping");
                }
            }
            3 => {
                let (conv_started, conv_original) = {
                    let state = self.state.lock().await;
                    (state.conv_conversation_started, state.conv_original_volume)
                };
                if !conv_started {
                    debug!("Received status 3 but conversation was not started; ignoring increase");
                    return;
                }
                if let Some(orig) = conv_original {
                    let target = orig.min(25);
                    let sink_clone = sink.clone();
                    tokio::task::spawn_blocking(move || {
                        transition_sink_volume(&sink_clone, target)
                    })
                    .await
                    .unwrap_or(false);
                    info!(
                        "Conversation partial increase (3): set volume to {} (original {})",
                        target, orig
                    );
                } else if let Some(orig_from_current) = current_volume_opt {
                    let target = orig_from_current.min(25);
                    let sink_clone = sink.clone();
                    tokio::task::spawn_blocking(move || {
                        transition_sink_volume(&sink_clone, target)
                    })
                    .await
                    .unwrap_or(false);
                    info!(
                        "Conversation partial increase (3) with fallback current: set volume to {} (measured {})",
                        target, orig_from_current
                    );
                } else {
                    debug!("No original volume known for status 3, skipping");
                }
            }
            4 | 6 | 7 => {
                debug!("Conversation end ({}), restoring volume if needed", status);
                self.restore_volume_if_needed(&sink).await;
            }
            _ => {
                debug!("Conversation status ({}), ignoring", status);
            }
        }
    }

    async fn mpris_command(&self, command: &'static str) {
        tokio::task::spawn_blocking(move || {
            let conn = match Connection::new_session() {
                Ok(c) => c,
                Err(_) => return,
            };

            let services = Self::list_mpris_services(&conn);
            let mut playing = None;
            let mut fallback = None;

            for service in &services {
                let proxy =
                    conn.with_proxy(service, "/org/mpris/MediaPlayer2", Duration::from_secs(5));
                if let Ok(status) =
                    proxy.get::<String>("org.mpris.MediaPlayer2.Player", "PlaybackStatus")
                {
                    if status == "Playing" && playing.is_none() {
                        playing = Some(service);
                    }
                }
                if fallback.is_none() {
                    fallback = Some(service);
                }
            }

            if let Some(service) = playing.or(fallback) {
                let proxy =
                    conn.with_proxy(service, "/org/mpris/MediaPlayer2", Duration::from_secs(5));
                if proxy
                    .method_call::<(), _, &str, &str>("org.mpris.MediaPlayer2.Player", command, ())
                    .is_ok()
                {
                    info!("Sent {} to: {}", command, service);
                } else {
                    debug!("Failed to send {} to: {}", command, service);
                }
            }
        })
        .await
        .unwrap();
    }

    async fn restore_volume_if_needed(&self, sink: &str) {
        let maybe_original = {
            let mut state = self.state.lock().await;
            if state.conv_conversation_started {
                let orig = state.conv_original_volume;
                state.conv_original_volume = None;
                state.conv_conversation_started = false;
                orig
            } else {
                return;
            }
        };

        if let Some(orig) = maybe_original {
            let sink = sink.to_string();
            tokio::task::spawn_blocking(move || transition_sink_volume(&sink, orig))
                .await
                .unwrap_or(false);
        }
    }

    fn list_mpris_services(conn: &Connection) -> Vec<String> {
        let proxy = conn.with_proxy(
            "org.freedesktop.DBus",
            "/org/freedesktop/DBus",
            Duration::from_secs(5),
        );

        let (names,): (Vec<String>,) =
            match proxy.method_call("org.freedesktop.DBus", "ListNames", ()) {
                Ok(n) => n,
                Err(_) => return vec![],
            };

        names
            .into_iter()
            .filter(|s| s.starts_with("org.mpris.MediaPlayer2.") && !Self::is_kdeconnect_service(s))
            .collect()
    }

    /// Call MPRIS `method` (Pause, Play) on one player service. Blocking.
    fn mpris_call(service: &str, method: &'static str) -> bool {
        let Ok(conn) = Connection::new_session() else {
            return false;
        };
        conn.with_proxy(service, "/org/mpris/MediaPlayer2", Duration::from_secs(5))
            .method_call::<(), _, &str, &str>("org.mpris.MediaPlayer2.Player", method, ())
            .is_ok()
    }

    /// One step of the A2DP resume chain (#200); false when it failed.
    async fn resume_action(&self, action: Action) -> bool {
        let mac = self.state.lock().await.connected_device_mac.clone();
        match action {
            Action::SuspendSink(suspend) => match get_sink_name_by_mac(&mac).await {
                Some(sink) => blocking(move || suspend_sink_sync(&sink, suspend)).await,
                None => false,
            },
            Action::Pause(service) => blocking(move || Self::mpris_call(&service, "Pause")).await,
            Action::Play(service) => blocking(move || Self::mpris_call(&service, "Play")).await,
            // One card lookup and one choice for both sets, made before the
            // card goes off. Set directly: no WirePlumber restart or wait.
            Action::ProfileCycle => {
                let Some(index) = find_card_index(mac).await else {
                    return false;
                };
                let (available, active) = self.get_card_profiles(index).await;
                let cached = self.state.lock().await.cached_a2dp_profile.clone();
                let profiles = resume_profiles(
                    &available,
                    active.as_deref(),
                    &cached,
                    crate::utils::load_preferred_codec(),
                );
                if profiles.is_empty() {
                    info!("A2DP resume: no profile cycle from {active:?}");
                }
                blocking(move || run_profile_cycle(&profiles, |p| set_card_profile_sync(index, p)))
                    .await
            }
        }
    }
}

/// The profiles the resume chain's profile cycle (#200) sets, in order:
/// off, then the A2DP profile to return to (the active one if it is A2DP).
/// Nothing without an A2DP profile to return to, and nothing on a call
/// profile, so a call that started meanwhile keeps its audio. The card
/// stays off only if setting the A2DP profile fails (logged as failed).
fn resume_profiles(
    available: &[String],
    active: Option<&str>,
    cached: &str,
    preferred: crate::utils::PreferredCodec,
) -> Vec<String> {
    if crate::call_detect::is_call_profile(active) {
        return Vec::new();
    }
    match select_a2dp_profile(available, active, Some(cached), preferred) {
        ProfileChoice::KeepActive(profile) | ProfileChoice::Activate(profile) => {
            vec!["off".to_string(), profile]
        }
        ProfileChoice::Unavailable => Vec::new(),
    }
}

/// Sets every profile in order, even after a failed write, so a lost reply
/// to the "off" write never leaves the card off. True only if all succeeded.
fn run_profile_cycle(profiles: &[String], mut set: impl FnMut(&str) -> bool) -> bool {
    profiles.iter().fold(true, |ok, p| set(p) && ok)
}

/// The A2DP resume chain's production I/O (#200), on the controller clone
/// `ControllerMedia` holds, so it never takes the outer MediaController
/// mutex. The kick's Pause goes straight to MPRIS: it is no self-pause.
impl A2dpResumeIo for MediaController {
    fn act(&self, action: Action) -> BoxFuture<'static, ()> {
        let controller = self.clone();
        Box::pin(async move {
            if !controller.resume_action(action.clone()).await {
                warn!("A2DP resume: {action:?} failed");
            }
        })
    }

    /// Some uncorked stream on the AirPods sink peaks above `AUDIBLE_PEAK`
    /// within `AUDIBLE_SAMPLE`.
    fn audible(&self) -> BoxFuture<'static, bool> {
        let state = self.state.clone();
        Box::pin(async move {
            let mac = state.lock().await.connected_device_mac.clone();
            let inputs = tokio::task::spawn_blocking(move || uncorked_inputs_on_sink_of(&mac))
                .await
                .unwrap_or_default();
            if inputs.is_empty() {
                return false;
            }
            let mut peaks = PulsePeaks::new();
            peaks.levels(&inputs);
            tokio::time::sleep(AUDIBLE_SAMPLE).await;
            peaks
                .levels(&inputs)
                .values()
                .any(|&peak| peak > AUDIBLE_PEAK)
        })
    }

    fn playing(&self) -> BoxFuture<'static, Vec<String>> {
        Box::pin(async {
            tokio::task::spawn_blocking(Self::playing_mpris_players)
                .await
                .unwrap_or_default()
        })
    }

    fn paused(&self) -> BoxFuture<'static, Vec<String>> {
        Box::pin(async {
            tokio::task::spawn_blocking(Self::paused_mpris_players)
                .await
                .unwrap_or_default()
        })
    }

    fn self_pauses(&self) -> usize {
        self.self_pauses.load(Ordering::SeqCst)
    }

    fn transport_active(&self) -> BoxFuture<'static, Option<bool>> {
        let state = self.state.clone();
        Box::pin(async move {
            let mac = state.lock().await.connected_device_mac.clone();
            tokio::task::spawn_blocking(move || crate::a2dp_resume::read_transport_once(&mac))
                .await
                .ok()
                .flatten()
        })
    }
}

async fn blocking(f: impl FnOnce() -> bool + Send + 'static) -> bool {
    tokio::task::spawn_blocking(f).await.unwrap_or(false)
}

// --- PulseAudio helpers ---
// Each query connects through `pulse`, so a stalled sound server fails it
// after the pulse timeouts with the same result as no sound server.

/// One scan of the card list for the card whose `device.string` contains
/// `mac`. Never retries; callers pick the retry budget.
async fn find_card_index(mac: String) -> Option<u32> {
    tokio::task::spawn_blocking(move || {
        get_card_info_list_sync().into_iter().find_map(|card| {
            let device_string = card.device_string?;
            device_string.contains(&mac).then_some(card.index)
        })
    })
    .await
    .unwrap_or(None)
}

fn get_card_info_list_sync() -> Vec<pulse::Card> {
    pulse::RawConnection::connect("LibrePods")
        .and_then(|mut connection| connection.cards())
        .unwrap_or_default()
}

fn get_sink_volume_percent_by_name_sync(sink_name: &str) -> Option<u32> {
    let sinks = pulse::sinks("LibrePods", pulse::Sinks::Name(sink_name)).ok()?;
    let info = sinks.first()?;
    let channels = info.volume.len();
    if channels == 0 {
        return None;
    }
    let total: f64 = info.volume.get().iter().map(|v| v.0 as f64).sum();
    let percent = ((total / channels as f64 / Volume::NORMAL.0 as f64) * 100.0).round() as u32;
    Some(percent)
}

fn set_card_profile_sync(card_index: u32, profile_name: &str) -> bool {
    let Ok((mut mainloop, context)) = pulse::connect("LibrePods") else {
        return false;
    };
    let mut introspector = context.introspect();
    let mut op = introspector.set_card_profile_by_index(card_index, profile_name, None);
    pulse::wait(&mut mainloop, &mut op, "card profile change").is_ok()
}

fn suspend_sink_sync(sink_name: &str, suspend: bool) -> bool {
    let Ok((mut mainloop, context)) = pulse::connect("LibrePods") else {
        return false;
    };
    let mut introspector = context.introspect();
    let mut op = introspector.suspend_sink_by_name(sink_name, suspend, None);
    pulse::wait(&mut mainloop, &mut op, "sink suspend").is_ok()
}

/// The uncorked sink inputs that play to the sink of the device `mac`.
fn uncorked_inputs_on_sink_of(mac: &str) -> HashSet<u32> {
    let inputs = pulse::RawConnection::connect("LibrePods")
        .and_then(|mut connection| connection.sink_inputs(None));
    let Ok(inputs) = inputs else {
        return HashSet::new();
    };
    let mut device_sink: HashMap<u32, bool> = HashMap::new();
    inputs
        .into_iter()
        .filter(|input| !input.corked)
        .map(|input| (input.index, input.sink))
        .filter(|&(_, sink)| {
            *device_sink.entry(sink).or_insert_with(|| {
                pulse::sinks("LibrePods", pulse::Sinks::Index(sink))
                    .ok()
                    .and_then(|sinks| sinks.into_iter().next())
                    .is_some_and(|sink| sink_of_mac(&sink, mac))
            })
        })
        .map(|(index, _)| index)
        .collect()
}

pub fn transition_sink_volume(sink_name: &str, target_volume: u32) -> bool {
    // The lookup has its own connection (#193) and fails on a missing sink.
    let sinks = pulse::sinks("LibrePods", pulse::Sinks::Name(sink_name));
    let Some(info) = sinks.ok().and_then(|sinks| sinks.into_iter().next()) else {
        error!("Sink not found: {}", sink_name);
        return false;
    };
    let Ok((mut mainloop, context)) = pulse::connect("LibrePods") else {
        return false;
    };
    let mut introspector = context.introspect();
    let channels = info.volume.len();
    let mut new_volumes = ChannelVolumes::default();
    let raw =
        (((target_volume as f64) / 100.0) * Volume::NORMAL.0.as_f64().unwrap()).round() as u32;
    new_volumes.set(channels, Volume(raw));

    let mut op = introspector.set_sink_volume_by_name(sink_name, &new_volumes, None);
    pulse::wait(&mut mainloop, &mut op, "sink volume change").is_ok()
}

async fn get_sink_name_by_mac(mac: &str) -> Option<String> {
    if mac.is_empty() {
        return None;
    }
    let mac_clone = mac.to_string();

    tokio::task::spawn_blocking(move || {
        let sink_list = pulse::sinks("LibrePods", pulse::Sinks::All).ok()?;
        let name = sink_list
            .into_iter()
            .find_map(|sink| sink.name.clone().filter(|_| sink_of_mac(&sink, &mac_clone)));
        match &name {
            Some(name) => debug!("Found sink name for MAC {}: {}", mac_clone, name),
            None => debug!("No matching sink found for MAC address: {}", mac_clone),
        }
        name
    })
    .await
    .unwrap_or(None)
}

/// Whether `sink` belongs to the Bluetooth device `mac`.
fn sink_of_mac(sink: &pulse::Sink, mac: &str) -> bool {
    sink.device_string
        .as_ref()
        .is_some_and(|d| d.to_uppercase().contains(&mac.to_uppercase()))
        || sink.bluez_path.as_ref().is_some_and(|path| {
            path.split('/')
                .next_back()
                .unwrap_or("")
                .replace("dev_", "")
                .replace('_', ":")
                .eq_ignore_ascii_case(mac)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn playback_listener_replacement_is_scoped_to_device() {
        let first_mac = "00:00:00:00:00:01";
        let other_mac = "00:00:00:00:00:02";

        let first = MediaController::new(first_mac.to_string(), String::new());
        let (first_tx, _first_rx) = tokio::sync::mpsc::unbounded_channel();
        first
            .start_playback_listener(AACPManager::new(), first_tx)
            .await;
        let first_handle = PLAYBACK_LISTENERS
            .lock()
            .unwrap()
            .get(first_mac)
            .unwrap()
            .clone();

        let other = MediaController::new(other_mac.to_string(), String::new());
        let (other_tx, _other_rx) = tokio::sync::mpsc::unbounded_channel();
        other
            .start_playback_listener(AACPManager::new(), other_tx)
            .await;
        assert!(!first_handle.is_finished());

        let replacement = MediaController::new(first_mac.to_string(), String::new());
        let (replacement_tx, _replacement_rx) = tokio::sync::mpsc::unbounded_channel();
        replacement
            .start_playback_listener(AACPManager::new(), replacement_tx)
            .await;

        tokio::time::timeout(Duration::from_secs(1), async {
            while !first_handle.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("superseded playback listener should stop");

        let listeners = PLAYBACK_LISTENERS.lock().unwrap();
        listeners.get(first_mac).unwrap().abort();
        listeners.get(other_mac).unwrap().abort();
    }

    fn takeover_inputs() -> TakeoverInputs {
        // A genuine play: baseline recorded, stopped -> playing, no pause of
        // our own pending, a bud in an ear.
        TakeoverInputs {
            baseline_taken: true,
            was_playing: false,
            is_playing: true,
            i_paused_the_media: false,
            bud_in_ear: true,
            trigger: TakeoverTrigger::PlaybackPoll,
            handoff: None,
        }
    }

    #[test]
    fn takeover_skips_initial_playback_state() {
        let decision = decide_takeover(TakeoverInputs {
            baseline_taken: false,
            ..takeover_inputs()
        });
        assert_eq!(
            decision,
            TakeoverDecision {
                action: TakeoverAction::Skip(TakeoverSkip::InitialState),
                clear_self_pause: false,
            }
        );

        // The first poll is not a resume either, so a pending self-pause
        // mark is left for the real one.
        let decision = decide_takeover(TakeoverInputs {
            baseline_taken: false,
            i_paused_the_media: true,
            ..takeover_inputs()
        });
        assert_eq!(
            decision.action,
            TakeoverAction::Skip(TakeoverSkip::InitialState)
        );
        assert!(!decision.clear_self_pause);
    }

    #[test]
    fn takeover_skips_resume_after_own_pause() {
        let decision = decide_takeover(TakeoverInputs {
            i_paused_the_media: true,
            ..takeover_inputs()
        });
        assert_eq!(
            decision,
            TakeoverDecision {
                action: TakeoverAction::Skip(TakeoverSkip::ResumeAfterOwnPause),
                clear_self_pause: true,
            }
        );
    }

    #[test]
    fn takeover_on_genuine_play_with_bud_in_ear() {
        assert_eq!(
            decide_takeover(takeover_inputs()),
            TakeoverDecision {
                action: TakeoverAction::TakeOver,
                clear_self_pause: false,
            }
        );
    }

    #[test]
    fn takeover_skipped_when_no_bud_in_ear() {
        assert_eq!(
            decide_takeover(TakeoverInputs {
                bud_in_ear: false,
                ..takeover_inputs()
            }),
            TakeoverDecision {
                action: TakeoverAction::Skip(TakeoverSkip::NoBudInEar),
                clear_self_pause: false,
            }
        );
    }

    #[test]
    fn takeover_skipped_without_stopped_to_playing_transition() {
        for (was_playing, is_playing) in [(true, true), (true, false), (false, false)] {
            let decision = decide_takeover(TakeoverInputs {
                was_playing,
                is_playing,
                i_paused_the_media: true,
                ..takeover_inputs()
            });
            assert_eq!(
                decision,
                TakeoverDecision {
                    action: TakeoverAction::Skip(TakeoverSkip::NoTransition),
                    clear_self_pause: false,
                },
                "was_playing={was_playing} is_playing={is_playing}"
            );
        }
    }

    const LOCAL: &str = "01:02:03:04:05:06";
    const REMOTE: &str = "07:08:09:0A:0B:0C";

    const REMOTE_2: &str = "0D:0E:0F:10:11:12";
    const REMOTE_3: &str = "13:14:15:16:17:18";

    /// Routing copied from a real `ConnectedDevice` list `macs`, with a live
    /// session, a bud in an ear, idle audio and a local stream (201).
    fn routing_with_hosts(macs: &[&str]) -> RoutingSnapshot {
        let hosts: Vec<ConnectedDevice> = macs
            .iter()
            .map(|mac| ConnectedDevice {
                mac: mac.to_string(),
                info1: 0,
                info2: 0,
                r#type: None,
            })
            .collect();
        let idle = AudioSource {
            mac: "00:00:00:00:00:00".to_string(),
            r#type: AudioSourceType::None,
        };
        routing_snapshot(
            true,
            &hosts,
            &[EarDetectionStatus::InEar, EarDetectionStatus::OutOfEar],
            Some(&idle),
            &HashMap::new(),
            AUDIO_CATEGORY_STREAM,
            LOCAL,
        )
    }

    /// Routing a #841 take accepts: live session, this machine plus one
    /// remote host, a bud in an ear, idle audio.
    fn ready_routing() -> RoutingSnapshot {
        routing_with_hosts(&[LOCAL, REMOTE])
    }

    fn guards(routing: RoutingSnapshot) -> Option<HandoffGuards> {
        Some(HandoffGuards {
            routing,
            recovery: CaseRecovery::Immediate,
        })
    }

    #[test]
    fn takeover_legacy_ignores_routing_guards() {
        // Only Linux connected and a remote call: the #841 guards block this
        // take on the hosts, and with a remote host back on the call.
        let blocking = RoutingSnapshot {
            audio: Some(AudioSourceType::Call),
            audio_from_local: false,
            ..routing_with_hosts(&[LOCAL])
        };
        assert_eq!(
            decide_takeover(TakeoverInputs {
                handoff: guards(blocking),
                ..takeover_inputs()
            })
            .action,
            TakeoverAction::Skip(TakeoverSkip::NoRemoteHost)
        );
        let with_remote = ready_routing();
        assert_eq!(
            decide_takeover(TakeoverInputs {
                handoff: guards(RoutingSnapshot {
                    host_count: with_remote.host_count,
                    remote_host: with_remote.remote_host,
                    ..blocking
                }),
                ..takeover_inputs()
            })
            .action,
            TakeoverAction::Skip(TakeoverSkip::AudioNotIdle)
        );

        // The legacy listener does not supply them: a genuine play takes over
        // and the baseline -> transition -> self-pause -> ear table holds.
        let legacy = TakeoverInputs {
            handoff: None,
            ..takeover_inputs()
        };
        assert_eq!(
            decide_takeover(legacy),
            TakeoverDecision {
                action: TakeoverAction::TakeOver,
                clear_self_pause: false,
            }
        );
        let table = [
            (
                TakeoverInputs {
                    baseline_taken: false,
                    ..legacy
                },
                TakeoverSkip::InitialState,
                false,
            ),
            (
                TakeoverInputs {
                    was_playing: true,
                    ..legacy
                },
                TakeoverSkip::NoTransition,
                false,
            ),
            (
                TakeoverInputs {
                    is_playing: false,
                    ..legacy
                },
                TakeoverSkip::NoTransition,
                false,
            ),
            (
                TakeoverInputs {
                    i_paused_the_media: true,
                    ..legacy
                },
                TakeoverSkip::ResumeAfterOwnPause,
                true,
            ),
            (
                TakeoverInputs {
                    bud_in_ear: false,
                    ..legacy
                },
                TakeoverSkip::NoBudInEar,
                false,
            ),
        ];
        for (inputs, reason, clear_self_pause) in table {
            assert_eq!(
                decide_takeover(inputs),
                TakeoverDecision {
                    action: TakeoverAction::Skip(reason),
                    clear_self_pause,
                },
                "{inputs:?}"
            );
        }
    }

    #[test]
    fn takeover_handoff_guards_follow_841() {
        let poll = |routing: RoutingSnapshot| TakeoverInputs {
            handoff: guards(routing),
            bud_in_ear: routing.bud_in_ear,
            ..takeover_inputs()
        };
        assert_eq!(
            decide_takeover(poll(ready_routing())).action,
            TakeoverAction::TakeOver
        );
        let blocked = [
            (
                RoutingSnapshot {
                    has_session: false,
                    ..ready_routing()
                },
                TakeoverSkip::NoSession,
            ),
            (
                RoutingSnapshot {
                    host_count: 1,
                    remote_host: false,
                    ..ready_routing()
                },
                TakeoverSkip::NoRemoteHost,
            ),
            (
                RoutingSnapshot {
                    local_host: false,
                    ..ready_routing()
                },
                TakeoverSkip::NoRemoteHost,
            ),
            (
                RoutingSnapshot {
                    audio: Some(AudioSourceType::Call),
                    ..ready_routing()
                },
                TakeoverSkip::AudioNotIdle,
            ),
            (
                RoutingSnapshot {
                    audio: Some(AudioSourceType::Media),
                    audio_from_local: false,
                    ..ready_routing()
                },
                TakeoverSkip::RemoteCategoryHigher {
                    local: AUDIO_CATEGORY_STREAM,
                    remote: AUDIO_CATEGORY_MEDIA,
                },
            ),
            (
                RoutingSnapshot {
                    audio: Some(AudioSourceType::Unknown),
                    ..ready_routing()
                },
                TakeoverSkip::AudioNotIdle,
            ),
            (
                RoutingSnapshot {
                    audio: None,
                    ..ready_routing()
                },
                TakeoverSkip::AudioNotIdle,
            ),
            (
                RoutingSnapshot {
                    bud_in_ear: false,
                    ..ready_routing()
                },
                TakeoverSkip::NoBudInEar,
            ),
        ];
        for (routing, reason) in blocked {
            assert_eq!(
                decide_takeover(poll(routing)).action,
                TakeoverAction::Skip(reason),
                "{routing:?}"
            );
        }
        // Local media is not remote activity.
        let local_media = RoutingSnapshot {
            audio: Some(AudioSourceType::Media),
            audio_from_local: true,
            ..ready_routing()
        };
        assert_eq!(
            decide_takeover(poll(local_media)).action,
            TakeoverAction::TakeOver
        );
        // The playback guards still come first for a poll.
        assert_eq!(
            decide_takeover(TakeoverInputs {
                was_playing: true,
                ..poll(ready_routing())
            })
            .action,
            TakeoverAction::Skip(TakeoverSkip::NoTransition)
        );
    }

    #[test]
    fn takeover_host_count_table() {
        // Linux plus any number of remote hosts takes; Linux alone, Linux
        // absent or no host list does not. Hardware #22 took with three
        // hosts (iPhone + Mac + Linux).
        let take = TakeoverAction::TakeOver;
        let skip = TakeoverAction::Skip(TakeoverSkip::NoRemoteHost);
        let table: [(&[&str], TakeoverAction); 7] = [
            (&[], skip),
            (&[LOCAL], skip),
            (&[LOCAL, REMOTE], take),
            (&[LOCAL, REMOTE, REMOTE_2], take),
            (&[REMOTE, LOCAL, REMOTE_2, REMOTE_3], take),
            (&[REMOTE, REMOTE_2], skip),
            (&[REMOTE, REMOTE_2, REMOTE_3], skip),
        ];
        for (hosts, expected) in table {
            let routing = routing_with_hosts(hosts);
            assert_eq!(routing.host_count, hosts.len(), "{hosts:?}");
            let poll = TakeoverInputs {
                handoff: guards(routing),
                bud_in_ear: routing.bud_in_ear,
                ..takeover_inputs()
            };
            assert_eq!(decide_takeover(poll).action, expected, "poll {hosts:?}");
            for trigger in [
                TakeoverTrigger::Manual,
                TakeoverTrigger::ReconnectedForPlayback,
            ] {
                let recovery = match trigger {
                    TakeoverTrigger::ReconnectedForPlayback => CaseRecovery::PlaybackRequested,
                    _ => CaseRecovery::Immediate,
                };
                assert_eq!(
                    decide_takeover(TakeoverInputs::handoff(trigger, routing, recovery)).action,
                    expected,
                    "{trigger:?} {hosts:?}"
                );
            }
            // The legacy path has no host guard.
            assert_eq!(
                decide_takeover(TakeoverInputs {
                    handoff: None,
                    ..poll
                })
                .action,
                take,
                "legacy {hosts:?}"
            );
        }
    }

    #[test]
    fn takeover_case_recovery_gates_takes() {
        let take = |trigger, recovery| {
            decide_takeover(TakeoverInputs::handoff(trigger, ready_routing(), recovery)).action
        };
        use CaseRecovery::*;
        use TakeoverTrigger::*;
        assert_eq!(take(Manual, Immediate), TakeoverAction::TakeOver);
        assert_eq!(
            take(Manual, WaitingForPlayback),
            TakeoverAction::Skip(TakeoverSkip::AwaitingNewPlayback)
        );
        assert_eq!(
            take(ReconnectedForPlayback, PlaybackRequested),
            TakeoverAction::TakeOver
        );
        for recovery in [Immediate, WaitingForPlayback] {
            assert_eq!(
                take(ReconnectedForPlayback, recovery),
                TakeoverAction::Skip(TakeoverSkip::AwaitingNewPlayback)
            );
        }
        // Manual and reconnect takes need no playback transition.
        let manual = TakeoverInputs {
            was_playing: true,
            is_playing: false,
            baseline_taken: false,
            ..TakeoverInputs::handoff(Manual, ready_routing(), Immediate)
        };
        assert_eq!(decide_takeover(manual).action, TakeoverAction::TakeOver);
    }

    /// Routing with Linux plus REMOTE and REMOTE_2, worn, the audio source
    /// `audio` reported by `source` (None: no report), these relays, and
    /// Linux at `local`.
    fn category_routing(
        local: u32,
        audio: Option<(&str, AudioSourceType)>,
        relays: &[(&str, u32)],
    ) -> RoutingSnapshot {
        let hosts = [LOCAL, REMOTE, REMOTE_2].map(|mac| ConnectedDevice {
            mac: mac.to_string(),
            info1: 0,
            info2: 0,
            r#type: None,
        });
        let audio = audio.map(|(mac, r#type)| AudioSource {
            mac: mac.to_string(),
            r#type,
        });
        let relays = relays.iter().map(|&(h, c)| (h.to_string(), c)).collect();
        routing_snapshot(
            true,
            &hosts,
            &[EarDetectionStatus::InEar; 2],
            audio.as_ref(),
            &relays,
            local,
            LOCAL,
        )
    }

    #[test]
    fn category_takeover_table() {
        use AudioSourceType::{Call, Media, Unknown};
        const IDLE: u32 = AUDIO_CATEGORY_IDLE;
        const STREAM: u32 = AUDIO_CATEGORY_STREAM;
        const MEDIA: u32 = AUDIO_CATEGORY_MEDIA;
        const CALL: u32 = crate::bluetooth::aacp::AUDIO_CATEGORY_CALL;
        let take = TakeoverAction::TakeOver;
        let higher = |local, remote| {
            TakeoverAction::Skip(TakeoverSkip::RemoteCategoryHigher { local, remote })
        };
        let not_idle = TakeoverAction::Skip(TakeoverSkip::AudioNotIdle);
        let none = AudioSourceType::None;
        // H = REMOTE is the audio source; G = REMOTE_2 is another host.
        let h = REMOTE;
        let g = REMOTE_2;
        let table: [(u32, Option<(&str, AudioSourceType)>, &[(&str, u32)], _); 16] = [
            (MEDIA, Some((h, Media)), &[], take),
            (STREAM, Some((h, Media)), &[], higher(STREAM, MEDIA)),
            (STREAM, Some((h, none)), &[], take),
            (MEDIA, Some((h, none)), &[], take),
            (STREAM, Some(("00:00:00:00:00:00", none)), &[], take),
            (STREAM, Some((h, Media)), &[(h, STREAM)], take),
            // #280 addendum: the relay beats a stale media report.
            (STREAM, Some((h, Media)), &[(h, IDLE)], take),
            (MEDIA, Some((h, Media)), &[(h, IDLE)], take),
            (MEDIA, Some((h, Media)), &[(h, CALL)], higher(MEDIA, CALL)),
            (MEDIA, Some((h, none)), &[(h, CALL)], higher(MEDIA, CALL)),
            (MEDIA, Some((h, Call)), &[], not_idle),
            (MEDIA, Some((h, Call)), &[(h, IDLE)], not_idle),
            (MEDIA, Some((h, Unknown)), &[], not_idle),
            (MEDIA, None, &[], not_idle),
            // A relay from a host that is not the audio source changes nothing.
            (
                STREAM,
                Some((h, Media)),
                &[(g, IDLE)],
                higher(STREAM, MEDIA),
            ),
            // A relay stored under Linux's own address is not remote.
            (STREAM, Some((LOCAL, none)), &[(LOCAL, MEDIA)], take),
        ];
        for (local, audio, relays, expected) in table {
            let routing = category_routing(local, audio, relays);
            let poll = TakeoverInputs {
                handoff: guards(routing),
                ..takeover_inputs()
            };
            assert_eq!(
                decide_takeover(poll).action,
                expected,
                "poll {local} {audio:?} {relays:?}"
            );
            let reconnect = TakeoverInputs::handoff(
                TakeoverTrigger::ReconnectedForPlayback,
                routing,
                CaseRecovery::PlaybackRequested,
            );
            assert_eq!(
                decide_takeover(reconnect).action,
                expected,
                "reconnect {local} {audio:?} {relays:?}"
            );
        }
        // Local media is never remote activity, whatever the categories.
        let local_media = category_routing(IDLE, Some((LOCAL, Media)), &[(LOCAL, MEDIA)]);
        assert_eq!(
            decide_takeover(TakeoverInputs {
                handoff: guards(local_media),
                ..takeover_inputs()
            })
            .action,
            take
        );
        // A manual take keeps the #841 guard: idle report or local media.
        let manual = |routing| {
            decide_takeover(TakeoverInputs::handoff(
                TakeoverTrigger::Manual,
                routing,
                CaseRecovery::Immediate,
            ))
            .action
        };
        assert_eq!(
            manual(category_routing(IDLE, Some((h, none)), &[(h, CALL)])),
            take
        );
        assert_eq!(
            manual(category_routing(MEDIA, Some((h, Media)), &[(h, IDLE)])),
            not_idle
        );
        assert_eq!(
            manual(category_routing(IDLE, Some((LOCAL, Media)), &[])),
            take
        );
    }

    #[test]
    fn case_closed_tracks_both_buds_in_case() {
        use EarDetectionStatus::*;
        assert!(case_closed_after(&[InCase, InCase], false));
        assert!(!case_closed_after(&[InCase, OutOfEar], true));
        assert!(!case_closed_after(&[InEar, InCase], true));
        assert!(case_closed_after(&[InCase, Disconnected], true));
        assert!(!case_closed_after(&[InCase, Disconnected], false));
        assert!(case_closed_after(&[], true));
    }

    #[tokio::test]
    async fn ear_detection_first_reading_is_baseline() {
        let controller = MediaController::new(String::new(), String::new());
        {
            // Markers the insertion path would change: it sets
            // user_played_the_media when playing, and a resume clears
            // i_paused_the_media.
            let mut state = controller.state.lock().await;
            state.is_playing = true;
            state.i_paused_the_media = true;
        }

        controller
            .handle_ear_detection(
                Vec::new(),
                vec![EarDetectionStatus::InEar, EarDetectionStatus::InEar],
            )
            .await;

        let state = controller.state.lock().await;
        assert_eq!(state.old_in_ear_data, vec![true, true]);
        assert!(!state.user_played_the_media);
        assert!(state.i_paused_the_media);
        assert!(state.is_playing);
        assert_eq!(state.device_index, None);
    }

    /// Both buds in ear, then `new`, with the blocking pause injected: its
    /// n-th call returns `outcomes[n]` (nothing paused past the end). A2DP
    /// deactivation is off, so no test reaches PulseAudio. Returns the
    /// controller and the pauser's call count.
    async fn ear_removal(
        new: [EarDetectionStatus; 2],
        is_playing: bool,
        outcomes: Vec<(Vec<String>, PauseOutcome)>,
    ) -> (MediaController, usize) {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let controller = MediaController::new(String::new(), String::new());
        {
            let mut state = controller.state.lock().await;
            state.is_playing = is_playing;
            state.disconnect_when_not_wearing = false;
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let pauser = move || {
            let n = counter.fetch_add(1, SeqCst);
            outcomes.get(n).cloned().unwrap_or_default()
        };
        controller
            .handle_ear_detection_with(
                vec![EarDetectionStatus::InEar, EarDetectionStatus::InEar],
                new.to_vec(),
                pauser,
            )
            .await;
        (controller, calls.load(SeqCst))
    }

    fn paused_one(service: &str) -> (Vec<String>, PauseOutcome) {
        let outcome = PauseOutcome {
            paused: 1,
            ..Default::default()
        };
        (vec![service.to_string()], outcome)
    }

    #[tokio::test]
    async fn ear_removal_one_bud_idle_leaves_self_pause_unset() {
        use EarDetectionStatus::*;
        for removed in [OutOfEar, InCase] {
            let (controller, calls) = ear_removal([removed, InEar], false, vec![]).await;
            assert_eq!(calls, 1, "{removed:?}");
            assert!(
                !controller.state.lock().await.i_paused_the_media,
                "{removed:?}"
            );
        }
    }

    #[tokio::test]
    async fn ear_removal_one_bud_playing_marks_self_pause() {
        use EarDetectionStatus::*;
        let player = "org.mpris.MediaPlayer2.test";
        let (controller, calls) =
            ear_removal([OutOfEar, InEar], true, vec![paused_one(player)]).await;
        assert_eq!(calls, 1);
        let state = controller.state.lock().await;
        assert!(state.i_paused_the_media);
        assert!(!state.is_playing);
        assert_eq!(state.paused_by_app_services, vec![player.to_string()]);
    }

    #[tokio::test]
    async fn ear_removal_both_buds_keeps_first_pause_verdict() {
        use EarDetectionStatus::*;
        let player = "org.mpris.MediaPlayer2.test";
        let outcomes = vec![paused_one(player), (vec![], PauseOutcome::default())];
        let (controller, calls) = ear_removal([OutOfEar, OutOfEar], true, outcomes).await;
        assert_eq!(calls, 2);
        let state = controller.state.lock().await;
        assert!(state.i_paused_the_media);
        assert_eq!(state.paused_by_app_services, vec![player.to_string()]);
    }

    #[tokio::test]
    async fn ear_removal_counts_each_pause_before_it_runs() {
        use EarDetectionStatus::*;
        let controller = MediaController::new(String::new(), String::new());
        controller.state.lock().await.disconnect_when_not_wearing = false;
        let counts = Arc::new(StdMutex::new(Vec::new()));
        let (seen, pauses) = (counts.clone(), controller.self_pauses.clone());
        let pauser = move || {
            seen.lock().unwrap().push(pauses.load(Ordering::SeqCst));
            (vec![], PauseOutcome::default())
        };
        controller
            .handle_ear_detection_with(vec![InEar, InEar], vec![OutOfEar, OutOfEar], pauser)
            .await;
        // Both buds out pauses twice; each pause sees itself counted.
        assert_eq!(*counts.lock().unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn ear_removal_both_buds_idle_leaves_self_pause_unset() {
        use EarDetectionStatus::*;
        let (controller, calls) = ear_removal([OutOfEar, OutOfEar], false, vec![]).await;
        assert_eq!(calls, 2);
        assert!(!controller.state.lock().await.i_paused_the_media);
    }

    #[tokio::test]
    async fn pause_all_media_marks_self_pause() {
        let controller = MediaController::new(String::new(), String::new());
        controller.state.lock().await.is_playing = true;

        // D-Bus unavailable while we last saw playback: still our pause.
        controller
            .pause_all_media_with(|| PauseOutcome {
                bus_unavailable: true,
                ..Default::default()
            })
            .await;
        {
            let state = controller.state.lock().await;
            assert!(state.i_paused_the_media);
            assert!(state.is_playing);
        }

        controller.state.lock().await.i_paused_the_media = false;
        controller
            .pause_all_media_with(|| PauseOutcome {
                paused: 2,
                ..Default::default()
            })
            .await;
        let state = controller.state.lock().await;
        assert!(state.i_paused_the_media);
        assert!(!state.is_playing);
    }

    #[test]
    fn marks_self_pause_rule() {
        let outcome = |paused, failed, bus_unavailable| PauseOutcome {
            paused,
            failed,
            bus_unavailable,
        };
        for (pause, was_playing, marks) in [
            (outcome(0, 0, false), false, false),
            (outcome(1, 0, false), false, true),
            (outcome(0, 1, false), false, true),
            (outcome(0, 0, true), true, true),
            (outcome(0, 0, true), false, false),
        ] {
            assert_eq!(
                marks_self_pause(pause, was_playing),
                marks,
                "{pause:?} {was_playing}"
            );
        }
    }

    #[tokio::test]
    async fn pause_all_media_idle_leaves_self_pause_unset() {
        let controller = MediaController::new(String::new(), String::new());
        controller.pause_all_media_with(PauseOutcome::default).await;
        assert!(!controller.state.lock().await.i_paused_the_media);
    }

    #[tokio::test]
    async fn pause_all_media_failed_pause_marks_self_pause() {
        let controller = MediaController::new(String::new(), String::new());
        controller
            .pause_all_media_with(|| PauseOutcome {
                failed: 1,
                ..Default::default()
            })
            .await;
        assert!(controller.state.lock().await.i_paused_the_media);
    }

    #[tokio::test]
    async fn pause_all_media_never_clears_self_pause() {
        let controller = MediaController::new(String::new(), String::new());
        controller.state.lock().await.i_paused_the_media = true;
        controller.pause_all_media_with(PauseOutcome::default).await;
        assert!(controller.state.lock().await.i_paused_the_media);
    }

    #[tokio::test]
    async fn takeover_after_idle_ownership_loss() {
        // Ownership lost with `paused` players, then a genuine stopped->playing
        // poll with a bud in ear.
        async fn play_after_loss(paused: usize) -> TakeoverDecision {
            let controller = MediaController::new(String::new(), String::new());
            controller
                .pause_all_media_with(move || PauseOutcome {
                    paused,
                    ..Default::default()
                })
                .await;
            decide_takeover(TakeoverInputs {
                baseline_taken: true,
                was_playing: false,
                is_playing: true,
                i_paused_the_media: controller.state.lock().await.i_paused_the_media,
                bud_in_ear: true,
                trigger: TakeoverTrigger::PlaybackPoll,
                handoff: None,
            })
        }

        assert_eq!(play_after_loss(0).await.action, TakeoverAction::TakeOver);
        let twin = play_after_loss(1).await;
        assert_eq!(
            twin.action,
            TakeoverAction::Skip(TakeoverSkip::ResumeAfterOwnPause)
        );
        assert!(twin.clear_self_pause);
    }

    #[test]
    fn takeover_skips_while_ownership_pause_pending() {
        let play = takeover_inputs();
        assert_eq!(
            decide_legacy_poll(play, true),
            TakeoverDecision {
                action: TakeoverAction::Skip(TakeoverSkip::OwnPausePending),
                clear_self_pause: false,
            }
        );
        // Only a take is suppressed: everything else, and every decision
        // without a pause in flight, is decide_takeover's.
        for inputs in [
            play,
            TakeoverInputs {
                baseline_taken: false,
                ..play
            },
            TakeoverInputs {
                was_playing: true,
                ..play
            },
            TakeoverInputs {
                is_playing: false,
                ..play
            },
            TakeoverInputs {
                i_paused_the_media: true,
                ..play
            },
            TakeoverInputs {
                bud_in_ear: false,
                ..play
            },
        ] {
            let decision = decide_takeover(inputs);
            assert_eq!(decide_legacy_poll(inputs, false), decision, "{inputs:?}");
            if decision.action != TakeoverAction::TakeOver {
                assert_eq!(decide_legacy_poll(inputs, true), decision, "{inputs:?}");
            }
        }
        let resume = TakeoverInputs {
            i_paused_the_media: true,
            ..play
        };
        assert_eq!(
            decide_legacy_poll(resume, true),
            TakeoverDecision {
                action: TakeoverAction::Skip(TakeoverSkip::ResumeAfterOwnPause),
                clear_self_pause: true,
            }
        );
        let unworn = TakeoverInputs {
            bud_in_ear: false,
            ..play
        };
        assert_eq!(
            decide_legacy_poll(unworn, true).action,
            TakeoverAction::Skip(TakeoverSkip::NoBudInEar)
        );
    }

    /// Runs `pause_all_media_with` on a task whose pauser blocks until the
    /// returned sender sends or is dropped, then calls `pauser`. Returns once
    /// the pauser is running.
    async fn blocked_pause(
        controller: &MediaController,
        pauser: impl FnOnce() -> PauseOutcome + Send + 'static,
    ) -> (tokio::task::JoinHandle<()>, std::sync::mpsc::Sender<()>) {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let controller = controller.clone();
        let task = tokio::spawn(async move {
            controller
                .pause_all_media_with(move || {
                    started_tx.send(()).unwrap();
                    let _ = release_rx.recv();
                    pauser()
                })
                .await;
        });
        started_rx.await.unwrap();
        (task, release_tx)
    }

    /// A listener poll that sees stopped -> playing with a bud in ear.
    async fn stopped_to_playing(controller: &MediaController) -> TakeoverDecision {
        controller.state.lock().await.is_playing = false;
        controller.record_playback_poll(true, true, true).await.0
    }

    #[tokio::test]
    async fn ownership_pause_in_flight_defers_takeover() {
        use std::sync::atomic::Ordering::SeqCst;
        // A pause that paused a player, and an idle one. After either, the
        // player that started during the pause is not taken: a paused player
        // coming back is our resume, and a player that kept playing needs a
        // fresh stopped -> playing.
        for (outcome, next) in [
            (
                PauseOutcome {
                    paused: 1,
                    ..Default::default()
                },
                TakeoverSkip::ResumeAfterOwnPause,
            ),
            (PauseOutcome::default(), TakeoverSkip::NoTransition),
        ] {
            let controller = MediaController::new(String::new(), String::new());
            let (task, release) = blocked_pause(&controller, move || outcome).await;
            assert_eq!(controller.ownership_pauses.load(SeqCst), 1);
            drop(
                tokio::time::timeout(Duration::from_secs(1), controller.state.lock())
                    .await
                    .expect("state is not held across the pauser"),
            );
            assert_eq!(
                stopped_to_playing(&controller).await,
                TakeoverDecision {
                    action: TakeoverAction::Skip(TakeoverSkip::OwnPausePending),
                    clear_self_pause: false,
                }
            );

            release.send(()).unwrap();
            task.await.unwrap();
            assert_eq!(controller.ownership_pauses.load(SeqCst), 0);
            assert_eq!(
                controller.state.lock().await.i_paused_the_media,
                marks_self_pause(outcome, false),
                "{outcome:?}"
            );
            assert_eq!(
                controller
                    .record_playback_poll(true, true, true)
                    .await
                    .0
                    .action,
                TakeoverAction::Skip(next)
            );
        }
    }

    #[tokio::test]
    async fn ownership_pause_marker_lifecycle() {
        use std::sync::atomic::Ordering::SeqCst;
        let controller = MediaController::new(String::new(), String::new());
        let pending = || controller.ownership_pauses.load(SeqCst);
        let take = TakeoverAction::TakeOver;
        let skip = TakeoverAction::Skip(TakeoverSkip::OwnPausePending);

        // Overlapping pauses (OwnershipToFalseRequest and OwnsConnection=0):
        // the marker holds until the last one finishes.
        let (first, release_first) = blocked_pause(&controller, PauseOutcome::default).await;
        let (second, release_second) = blocked_pause(&controller, PauseOutcome::default).await;
        assert_eq!(pending(), 2);
        release_first.send(()).unwrap();
        first.await.unwrap();
        assert_eq!(pending(), 1);
        assert_eq!(stopped_to_playing(&controller).await.action, skip);
        release_second.send(()).unwrap();
        second.await.unwrap();
        assert_eq!(pending(), 0);
        assert_eq!(stopped_to_playing(&controller).await.action, take);

        // A pause future dropped mid-pause.
        let (task, release) = blocked_pause(&controller, PauseOutcome::default).await;
        assert_eq!(pending(), 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(pending(), 0);
        drop(release);
        assert_eq!(stopped_to_playing(&controller).await.action, take);

        // A panicking pauser.
        let (task, release) = blocked_pause(&controller, || -> PauseOutcome {
            panic!("pauser panicked")
        })
        .await;
        assert_eq!(pending(), 1);
        release.send(()).unwrap();
        assert!(task.await.unwrap_err().is_panic());
        assert_eq!(pending(), 0);
        assert_eq!(stopped_to_playing(&controller).await.action, take);
    }

    #[test]
    fn card_lookup_budget_is_about_three_seconds() {
        assert_eq!(CARD_LOOKUP_ATTEMPTS, 12);
        assert_eq!(CARD_LOOKUP_INTERVAL, Duration::from_millis(250));
    }

    #[tokio::test]
    async fn card_lookup_retries_until_found() {
        let mut calls = 0u32;
        let found = retry_card_lookup(CARD_LOOKUP_ATTEMPTS, Duration::ZERO, || {
            calls += 1;
            let attempt = calls;
            async move { (attempt == 4).then_some(7) }
        })
        .await;
        assert_eq!(found, Some(7));
        assert_eq!(calls, 4);
    }

    #[tokio::test]
    async fn card_lookup_gives_up_after_budget() {
        let mut calls = 0u32;
        let found = retry_card_lookup(CARD_LOOKUP_ATTEMPTS, Duration::ZERO, || {
            calls += 1;
            async { None }
        })
        .await;
        assert_eq!(found, None);
        assert_eq!(calls, 12);
    }

    /// Runs `retry_card_lookup_with` against a card that never shows up and
    /// returns (lookup calls, sleeps, total slept).
    async fn run_missing_card_lookup(attempts: u32) -> (u32, u32, Duration) {
        let calls = std::cell::Cell::new(0u32);
        let sleeps = std::cell::Cell::new(0u32);
        let slept = std::cell::Cell::new(Duration::ZERO);
        let found = retry_card_lookup_with(
            attempts,
            CARD_LOOKUP_INTERVAL,
            || {
                calls.set(calls.get() + 1);
                async { None::<u32> }
            },
            |interval| {
                sleeps.set(sleeps.get() + 1);
                slept.set(slept.get() + interval);
                std::future::ready(())
            },
        )
        .await;
        assert_eq!(found, None);
        (calls.get(), sleeps.get(), slept.get())
    }

    #[tokio::test]
    async fn card_lookup_single_shot_never_sleeps() {
        assert_eq!(CARD_LOOKUP_ATTEMPTS_PER_CHECK, 1);
        assert_eq!(
            run_missing_card_lookup(CARD_LOOKUP_ATTEMPTS_PER_CHECK).await,
            (1, 0, Duration::ZERO)
        );
        assert_eq!(
            card_lookup_max_wait(CARD_LOOKUP_ATTEMPTS_PER_CHECK),
            Duration::ZERO
        );
        // The full poll used by the initial lookup and by deactivation keeps
        // its budget: 12 lookups, 11 sleeps of 250 ms.
        assert_eq!(
            run_missing_card_lookup(CARD_LOOKUP_ATTEMPTS).await,
            (
                CARD_LOOKUP_ATTEMPTS,
                CARD_LOOKUP_ATTEMPTS - 1,
                card_lookup_max_wait(CARD_LOOKUP_ATTEMPTS)
            )
        );
        assert_eq!(
            card_lookup_max_wait(CARD_LOOKUP_ATTEMPTS),
            Duration::from_millis(2750)
        );
    }

    #[tokio::test]
    async fn a2dp_recovery_worst_case_bound() {
        assert_eq!(A2DP_RECOVERY_BOUND, Duration::from_secs(15));
        assert_eq!(WIREPLUMBER_SETTLE, Duration::from_secs(2));

        let attempts = MediaController::A2DP_ENUMERATION_ATTEMPTS;
        let interval = MediaController::A2DP_ENUMERATION_INTERVAL;
        let initial_lookup = CARD_LOOKUP_INTERVAL * (CARD_LOOKUP_ATTEMPTS - 1);
        let connect_lookup = CARD_LOOKUP_INTERVAL * (A2DP_CONNECT_LOOKUP_ATTEMPTS - 1);
        let per_check_lookup = CARD_LOOKUP_INTERVAL * (CARD_LOOKUP_ATTEMPTS_PER_CHECK - 1);
        let wait = interval * (attempts - 1) + per_check_lookup * attempts;
        let expected = initial_lookup + connect_lookup + wait + WIREPLUMBER_SETTLE + wait;
        assert_eq!(a2dp_activation_worst_case_wait(), expected);
        assert!(expected <= A2DP_RECOVERY_BOUND, "{expected:?} > 15 s");
        assert!(expected + A2DP_CONNECT_TIMEOUT <= A2DP_ACTIVATION_DEADLINE);
        // Still within the bound when both lookups are charged their nominal
        // attempts × CARD_LOOKUP_INTERVAL (~3 s and 2 s).
        let nominal = CARD_LOOKUP_INTERVAL * (CARD_LOOKUP_ATTEMPTS + A2DP_CONNECT_LOOKUP_ATTEMPTS)
            + wait
            + WIREPLUMBER_SETTLE
            + wait;
        assert!(nominal <= A2DP_RECOVERY_BOUND, "{nominal:?} > 15 s");

        // Replay the production sequence on a virtual clock: the initial full
        // lookup misses, the A2DP profile connect succeeds and its re-lookup
        // finds the card on its last try, then the card is missing at every
        // enumeration check (single-shot re-lookups) and the restart succeeds.
        let clock = std::cell::Cell::new(Duration::ZERO);
        let lookups = std::cell::Cell::new(0u32);
        let tick = |d: Duration| {
            clock.set(clock.get() + d);
            std::future::ready(())
        };
        let initial = retry_card_lookup_with(
            CARD_LOOKUP_ATTEMPTS,
            CARD_LOOKUP_INTERVAL,
            || {
                lookups.set(lookups.get() + 1);
                async { None::<u32> }
            },
            tick,
        )
        .await;
        assert_eq!((initial, lookups.get()), (None, CARD_LOOKUP_ATTEMPTS));
        lookups.set(0);
        let found = connect_a2dp_then_find(
            || async { Ok(()) },
            || {
                lookups.set(lookups.get() + 1);
                let n = lookups.get();
                async move { (n == A2DP_CONNECT_LOOKUP_ATTEMPTS).then_some(3) }
            },
            tick,
        )
        .await;
        assert_eq!(found, Some(3));

        let controller = MediaController::new(String::new(), String::new());
        let checks = std::cell::Cell::new(0u32);
        let restarts = std::cell::Cell::new(0u32);
        let recheck_lookups = std::cell::Cell::new(0u32);
        let ok = recover_a2dp_availability(
            attempts,
            interval,
            || {
                checks.set(checks.get() + 1);
                let recheck_lookups = &recheck_lookups;
                // The card is missing at every re-check; at the first check
                // (resolved index) it has no A2DP profile yet.
                controller.a2dp_enumeration_check(
                    checks.get() == 1,
                    move || {
                        recheck_lookups.set(recheck_lookups.get() + 1);
                        async { None }
                    },
                    tick,
                    || std::future::ready(false),
                )
            },
            || {
                restarts.set(restarts.get() + 1);
                clock.set(clock.get() + WIREPLUMBER_SETTLE);
                std::future::ready(true)
            },
            tick,
        )
        .await;
        assert!(!ok);
        assert_eq!(checks.get(), 1 + 2 * attempts);
        assert_eq!(restarts.get(), 1);
        assert_eq!(
            recheck_lookups.get(),
            2 * attempts * CARD_LOOKUP_ATTEMPTS_PER_CHECK
        );
        assert_eq!(clock.get(), expected);
        assert!(clock.get() <= A2DP_RECOVERY_BOUND);
    }

    /// Runs one `a2dp_enumeration_check` whose lookup finds `found` and whose
    /// A2DP query answers `a2dp`; returns (result, lookups, sleeps, asked).
    async fn run_a2dp_check(
        controller: &MediaController,
        first: bool,
        found: Option<u32>,
        a2dp: bool,
    ) -> (bool, u32, u32, bool) {
        let lookups = std::cell::Cell::new(0u32);
        let sleeps = std::cell::Cell::new(0u32);
        let asked = std::cell::Cell::new(false);
        let ok = controller
            .a2dp_enumeration_check(
                first,
                || {
                    lookups.set(lookups.get() + 1);
                    std::future::ready(found)
                },
                |_| {
                    sleeps.set(sleeps.get() + 1);
                    std::future::ready(())
                },
                || {
                    asked.set(true);
                    std::future::ready(a2dp)
                },
            )
            .await;
        (ok, lookups.get(), sleeps.get(), asked.get())
    }

    #[tokio::test]
    async fn a2dp_enumeration_check_relooks_up_once_after_first() {
        let controller = MediaController::new(String::new(), String::new());
        controller.state.lock().await.device_index = Some(3);

        // First check: reuses the resolved index, no lookup, asks for A2DP.
        assert_eq!(
            run_a2dp_check(&controller, true, None, true).await,
            (true, 0, 0, true)
        );
        assert_eq!(controller.state.lock().await.device_index, Some(3));

        // Later check, card found under a new index: stored, A2DP answered.
        for a2dp in [true, false] {
            assert_eq!(
                run_a2dp_check(&controller, false, Some(9), a2dp).await,
                (a2dp, CARD_LOOKUP_ATTEMPTS_PER_CHECK, 0, true)
            );
            assert_eq!(controller.state.lock().await.device_index, Some(9));
        }

        // Later check, card missing: stored as gone, A2DP not asked.
        assert_eq!(
            run_a2dp_check(&controller, false, None, true).await,
            (false, CARD_LOOKUP_ATTEMPTS_PER_CHECK, 0, false)
        );
        assert_eq!(controller.state.lock().await.device_index, None);
    }

    /// Just under one media_controller query's bound (`pulse` connect + operation).
    fn slow_query() -> Duration {
        pulse::CONNECT_TIMEOUT + pulse::OPERATION_TIMEOUT - Duration::from_millis(1)
    }

    /// Injected activation steps for the deadline tests: each records when it
    /// started, then answers `out` after `took` on the (paused) tokio clock.
    #[derive(Default)]
    struct Steps(std::cell::RefCell<Vec<(&'static str, Instant)>>);

    impl Steps {
        fn step<T>(&self, name: &'static str, took: Duration, out: T) -> impl Future<Output = T> {
            self.0.borrow_mut().push((name, Instant::now()));
            async move {
                tokio::time::sleep(took).await;
                out
            }
        }

        fn started(&self, name: &str) -> usize {
            self.0.borrow().iter().filter(|(n, _)| *n == name).count()
        }

        fn started_from(&self, deadline: Instant) -> usize {
            self.0
                .borrow()
                .iter()
                .filter(|(_, t)| *t >= deadline)
                .count()
        }
    }

    #[test]
    fn a2dp_activation_deadline_value() {
        assert_eq!(A2DP_ACTIVATION_DEADLINE, Duration::from_secs(20));
        assert!(A2DP_ACTIVATION_DEADLINE <= Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn a2dp_activation_deadline_cuts_slow_initial_lookup() {
        let controller = MediaController::new(String::new(), String::new());
        controller.state.lock().await.device_index = Some(99);
        let steps = Steps::default();
        let start = Instant::now();
        let deadline = start + A2DP_ACTIVATION_DEADLINE;

        // The card never shows up: the full poll alone would take ~50 s.
        let outcome = controller
            .activate_a2dp_profile_with(
                deadline,
                || {
                    retry_card_lookup(CARD_LOOKUP_ATTEMPTS, CARD_LOOKUP_INTERVAL, || {
                        steps.step("lookup", slow_query(), None)
                    })
                },
                || steps.step("connect", Duration::ZERO, None),
                || steps.step("recover", Duration::ZERO, true),
                |_| steps.step("choose", Duration::ZERO, ProfileChoice::Unavailable),
                |_, _| steps.step("set", Duration::ZERO, true),
            )
            .await;

        assert_eq!(outcome, Err(DeadlineExceeded));
        assert!(start.elapsed() <= A2DP_ACTIVATION_DEADLINE);
        assert_eq!(steps.started_from(deadline), 0);
        assert!(steps.started("lookup") > 1);
        assert_eq!(
            steps.started("recover") + steps.started("choose") + steps.started("set"),
            0
        );
        // The cut lookup wrote nothing.
        assert_eq!(controller.state.try_lock().unwrap().device_index, Some(99));
    }

    #[tokio::test(start_paused = true)]
    async fn a2dp_activation_deadline_initial_lookup_retry_at_deadline() {
        let controller = MediaController::new(String::new(), String::new());
        let steps = Steps::default();
        let start = Instant::now();
        let deadline = start + A2DP_ACTIVATION_DEADLINE;

        // Failing lookups of 3.75 s plus the 250 ms retry gap start at
        // 0, 4, 8, 12 and 16 s; the gap after the fifth ends exactly at the
        // deadline, and the sixth lookup must not start.
        let outcome = controller
            .activate_a2dp_profile_with(
                deadline,
                || {
                    retry_card_lookup(CARD_LOOKUP_ATTEMPTS, CARD_LOOKUP_INTERVAL, || {
                        steps.step("lookup", Duration::from_millis(3750), None)
                    })
                },
                || steps.step("connect", Duration::ZERO, None),
                || steps.step("recover", Duration::ZERO, true),
                |_| steps.step("choose", Duration::ZERO, ProfileChoice::Unavailable),
                |_, _| steps.step("set", Duration::ZERO, true),
            )
            .await;

        assert_eq!(outcome, Err(DeadlineExceeded));
        assert_eq!(Instant::now(), deadline);
        assert_eq!(steps.started_from(deadline), 0);
        assert_eq!(steps.started("lookup"), 5);
        assert!(controller.state.try_lock().is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn a2dp_activation_deadline_cuts_slow_recovery() {
        let controller = MediaController::new(String::new(), String::new());
        let steps = Steps::default();
        let finds = std::cell::Cell::new(0u32);
        let start = Instant::now();
        let deadline = start + A2DP_ACTIVATION_DEADLINE;

        // Every query takes just under its bound and the card never offers
        // A2DP. Each re-check finds the card under a new index.
        let outcome = controller
            .activate_a2dp_profile_with(
                deadline,
                || steps.step("lookup", slow_query(), Some(10)),
                || steps.step("connect", Duration::ZERO, None),
                || {
                    controller.recover_a2dp_availability_by(
                        deadline,
                        || {
                            finds.set(finds.get() + 1);
                            steps.step("find", slow_query(), Some(10 + finds.get()))
                        },
                        || steps.step("available", slow_query(), false),
                        || steps.step("restart", Duration::ZERO, true),
                    )
                },
                |_| steps.step("choose", slow_query(), ProfileChoice::Unavailable),
                |_, _| steps.step("set", slow_query(), true),
            )
            .await;

        assert_eq!(outcome, Err(DeadlineExceeded));
        assert!(start.elapsed() <= A2DP_ACTIVATION_DEADLINE);
        assert_eq!(steps.started_from(deadline), 0);
        assert_eq!(steps.started("choose") + steps.started("set"), 0);
        // The last find was cut off: the index of the one before it stays.
        let finds = finds.get();
        assert!(finds >= 2);
        assert_eq!(
            controller.state.try_lock().unwrap().device_index,
            Some(10 + finds - 1)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a2dp_activation_deadline_restart_hangs() {
        let controller = MediaController::new(String::new(), String::new());
        controller.state.lock().await.device_index = Some(3);
        let steps = Steps::default();
        let start = Instant::now();
        let deadline = start + A2DP_ACTIVATION_DEADLINE;

        let recovered = controller
            .recover_a2dp_availability_by(
                deadline,
                || steps.step("find", Duration::ZERO, Some(3)),
                || steps.step("available", Duration::ZERO, false),
                || async {
                    steps.step("restart", Duration::ZERO, ()).await;
                    std::future::pending::<bool>().await
                },
            )
            .await;

        assert!(!recovered);
        assert!(start.elapsed() <= A2DP_ACTIVATION_DEADLINE);
        assert_eq!(steps.started("restart"), 1);
        assert_eq!(steps.started_from(deadline), 0);
        assert!(controller.state.try_lock().is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn a2dp_activation_deadline_no_step_starts_at_deadline() {
        let controller = MediaController::new(String::new(), String::new());
        controller.state.lock().await.device_index = Some(3);
        let steps = Steps::default();
        let start = Instant::now();
        // The enumeration wait's first re-check (find 2 s, query instant)
        // starts at once; the next one is due one interval later, exactly at
        // the deadline, and must not start its find.
        let find = Duration::from_secs(2);
        let deadline = start + find + MediaController::A2DP_ENUMERATION_INTERVAL;

        let recovered = controller
            .recover_a2dp_availability_by(
                deadline,
                || steps.step("find", find, Some(3)),
                || steps.step("available", Duration::ZERO, false),
                || steps.step("restart", Duration::ZERO, true),
            )
            .await;

        assert!(!recovered);
        assert_eq!(Instant::now(), deadline);
        assert_eq!((steps.started("find"), steps.started("available")), (1, 2));
        assert_eq!(steps.started_from(deadline), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a2dp_activation_deadline_fast_path_unchanged() {
        for (active, expected_set) in [
            (None, Some((3, "a2dp-sink".to_string()))),
            (Some("a2dp-sink-sbc"), None),
        ] {
            let controller = MediaController::new(String::new(), String::new());
            let steps = Steps::default();
            let set_to = std::cell::RefCell::new(None);
            let start = Instant::now();
            let deadline = start + A2DP_ACTIVATION_DEADLINE;

            let outcome = controller
                .activate_a2dp_profile_with(
                    deadline,
                    || steps.step("lookup", Duration::ZERO, Some(3)),
                    || steps.step("connect", Duration::ZERO, None),
                    || {
                        controller.recover_a2dp_availability_by(
                            deadline,
                            || steps.step("find", Duration::ZERO, Some(3)),
                            || steps.step("available", Duration::ZERO, true),
                            || steps.step("restart", Duration::ZERO, true),
                        )
                    },
                    |_| {
                        let choice = select_a2dp_profile(
                            &profiles(&ALL_A2DP),
                            active,
                            None,
                            PreferredCodec::Aac,
                        );
                        steps.step("choose", Duration::ZERO, choice)
                    },
                    |idx, profile| {
                        *set_to.borrow_mut() = Some((idx, profile));
                        steps.step("set", Duration::ZERO, true)
                    },
                )
                .await;

            assert_eq!(outcome, Ok(()));
            assert_eq!(start.elapsed(), Duration::ZERO);
            assert_eq!(set_to.into_inner(), expected_set);
            assert_eq!(steps.started("find") + steps.started("restart"), 0);
            assert_eq!(controller.state.try_lock().unwrap().device_index, Some(3));
        }
    }

    #[tokio::test]
    async fn deactivate_releases_state_during_card_lookup() {
        let controller = MediaController::new("00:00:00:00:00:01".to_string(), String::new());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let set_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task = tokio::spawn({
            let controller = controller.clone();
            let set_called = set_called.clone();
            async move {
                controller
                    .deactivate_a2dp_profile_with(
                        |_| async move {
                            let _ = started_tx.send(());
                            let _ = release_rx.await;
                            None
                        },
                        move |_| {
                            set_called.store(true, std::sync::atomic::Ordering::SeqCst);
                            true
                        },
                    )
                    .await
            }
        });

        started_rx.await.unwrap();
        assert!(controller.state.try_lock().is_ok());
        release_tx.send(()).unwrap();
        task.await.unwrap();
        assert_eq!(controller.state.lock().await.device_index, None);
        assert!(!set_called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn deactivate_cached_index_skips_lookup() {
        let controller = MediaController::new("00:00:00:00:00:01".to_string(), String::new());
        controller.state.lock().await.device_index = Some(7);
        let (set_tx, set_rx) = std::sync::mpsc::channel();
        controller
            .deactivate_a2dp_profile_with(
                |_| async { panic!("cached index must skip the lookup") },
                move |index| set_tx.send(index).is_ok(),
            )
            .await;
        assert_eq!(set_rx.try_recv(), Ok(7));
    }

    use crate::utils::PreferredCodec;

    fn profiles(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    const ALL_A2DP: [&str; 3] = ["a2dp-sink-sbc", "a2dp-sink-sbc_xq", "a2dp-sink"];

    #[test]
    fn a2dp_profile_keeps_any_active_a2dp_variant() {
        let available = profiles(&ALL_A2DP);
        for active in ["a2dp-sink-sbc", "a2dp-sink-sbc_xq", "a2dp-sink"] {
            for preferred in PreferredCodec::ALL {
                assert_eq!(
                    select_a2dp_profile(&available, Some(active), None, preferred),
                    ProfileChoice::KeepActive(active.to_string())
                );
                // The cache does not override an active A2DP profile either.
                assert_eq!(
                    select_a2dp_profile(&available, Some(active), Some("a2dp-sink"), preferred),
                    ProfileChoice::KeepActive(active.to_string())
                );
            }
        }
        assert_eq!(
            select_a2dp_profile(&available, Some("a2dp-sink-sbc"), None, PreferredCodec::Aac),
            ProfileChoice::KeepActive("a2dp-sink-sbc".to_string())
        );
    }

    #[test]
    fn a2dp_profile_activates_preferred_when_not_a2dp() {
        let available = profiles(&["off", "headset-head-unit", "a2dp-sink-sbc", "a2dp-sink"]);
        for active in [Some("off"), Some("headset-head-unit"), None] {
            assert_eq!(
                select_a2dp_profile(&available, active, None, PreferredCodec::Aac),
                ProfileChoice::Activate("a2dp-sink".to_string())
            );
            assert_eq!(
                select_a2dp_profile(&available, active, Some(""), PreferredCodec::Sbc),
                ProfileChoice::Activate("a2dp-sink-sbc".to_string())
            );
        }
    }

    #[test]
    fn a2dp_profile_falls_back_when_preferred_missing() {
        // AAC missing: AAC order continues with SBC-XQ, then SBC.
        let available = profiles(&["off", "a2dp-sink-sbc", "a2dp-sink-sbc_xq"]);
        assert_eq!(
            select_a2dp_profile(&available, Some("off"), None, PreferredCodec::Aac),
            ProfileChoice::Activate("a2dp-sink-sbc_xq".to_string())
        );
        let available = profiles(&["off", "a2dp-sink-sbc"]);
        assert_eq!(
            select_a2dp_profile(&available, Some("off"), None, PreferredCodec::Aac),
            ProfileChoice::Activate("a2dp-sink-sbc".to_string())
        );
        // SBC-XQ missing: its order falls back to AAC before SBC.
        let available = profiles(&["off", "a2dp-sink-sbc", "a2dp-sink"]);
        assert_eq!(
            select_a2dp_profile(&available, Some("off"), None, PreferredCodec::SbcXq),
            ProfileChoice::Activate("a2dp-sink".to_string())
        );
        // A cached profile that disappeared falls back to the codec order.
        assert_eq!(
            select_a2dp_profile(
                &available,
                Some("off"),
                Some("a2dp-sink-sbc_xq"),
                PreferredCodec::Sbc
            ),
            ProfileChoice::Activate("a2dp-sink-sbc".to_string())
        );
    }

    #[test]
    fn a2dp_profile_prefers_cached_when_available() {
        // Upstream semantics: the cached choice wins over a changed codec
        // preference until the controller is recreated.
        let available = profiles(&ALL_A2DP);
        assert_eq!(
            select_a2dp_profile(
                &available,
                Some("off"),
                Some("a2dp-sink-sbc"),
                PreferredCodec::Aac
            ),
            ProfileChoice::Activate("a2dp-sink-sbc".to_string())
        );
    }

    #[test]
    fn a2dp_profile_unavailable_when_no_a2dp_profiles() {
        let available = profiles(&["off", "headset-head-unit"]);
        for preferred in PreferredCodec::ALL {
            assert_eq!(
                select_a2dp_profile(&available, Some("off"), Some("a2dp-sink"), preferred),
                ProfileChoice::Unavailable
            );
        }
        assert_eq!(
            select_a2dp_profile(&[], None, None, PreferredCodec::Aac),
            ProfileChoice::Unavailable
        );
    }

    #[test]
    fn resume_profile_cycle_returns_to_the_profile_it_left() {
        let cycle = |available: &[&str], active, cached| {
            resume_profiles(&profiles(available), active, cached, PreferredCodec::Aac)
        };
        // An active A2DP profile outside the codec order, empty cache.
        assert_eq!(
            cycle(&["off", "a2dp_sink"], Some("a2dp_sink"), ""),
            ["off", "a2dp_sink"]
        );
        // Kept active with AAC preferred: back to the same codec.
        let available = ["off", "a2dp-sink", "a2dp-sink-sbc"];
        assert_eq!(
            cycle(&available, Some("a2dp-sink-sbc"), ""),
            ["off", "a2dp-sink-sbc"]
        );
        // Off already: to the cached choice.
        assert_eq!(
            cycle(&available, Some("off"), "a2dp-sink-sbc"),
            ["off", "a2dp-sink-sbc"]
        );
        // No A2DP profile to return to: no cycle.
        assert!(cycle(&["off", "headset-head-unit"], Some("off"), "").is_empty());
        // A call on HFP: no cycle, even with A2DP available.
        let hfp = Some("headset-head-unit");
        assert!(cycle(&["off", "headset-head-unit", "a2dp-sink"], hfp, "").is_empty());
    }

    /// Runs the profile cycle over `list` with a setter failing on `fails`.
    /// Returns (result, profiles attempted in order).
    fn run_cycle(list: &[&str], fails: &[&str]) -> (bool, Vec<String>) {
        let mut attempted = Vec::new();
        let ok = run_profile_cycle(&profiles(list), |p| {
            attempted.push(p.to_string());
            !fails.contains(&p)
        });
        (ok, attempted)
    }

    #[test]
    fn profile_cycle_restores_a2dp_after_off_write_fails() {
        let both = ["off", "a2dp-sink"];
        assert_eq!(run_cycle(&both, &["off"]), (false, profiles(&both)));
    }

    #[test]
    fn profile_cycle_reports_each_outcome_after_trying_every_write() {
        let both = ["off", "a2dp-sink"];
        assert_eq!(run_cycle(&both, &[]), (true, profiles(&both)));
        assert_eq!(run_cycle(&both, &["a2dp-sink"]), (false, profiles(&both)));
        assert_eq!(run_cycle(&[], &[]), (true, Vec::new()));
    }

    const TEST_INTERVAL: Duration = Duration::from_millis(1);

    /// Runs the recovery sequence with an availability check that turns true
    /// on call `available_on` (never if `None`), counting checks, restarts and
    /// sleeps. Returns (result, checks, restarts, sleeps).
    async fn run_recovery(available_on: Option<u32>, restart_ok: bool) -> (bool, u32, u32, u32) {
        let checks = std::cell::Cell::new(0u32);
        let restarts = std::cell::Cell::new(0u32);
        let sleeps = std::cell::Cell::new(0u32);
        let result = recover_a2dp_availability(
            MediaController::A2DP_ENUMERATION_ATTEMPTS,
            TEST_INTERVAL,
            || {
                checks.set(checks.get() + 1);
                let now = checks.get();
                async move { available_on.is_some_and(|k| now >= k) }
            },
            || {
                restarts.set(restarts.get() + 1);
                async move { restart_ok }
            },
            |interval| {
                assert_eq!(interval, TEST_INTERVAL);
                sleeps.set(sleeps.get() + 1);
                tokio::time::sleep(interval)
            },
        )
        .await;
        (result, checks.get(), restarts.get(), sleeps.get())
    }

    #[test]
    fn a2dp_recovery_production_budget() {
        assert_eq!(MediaController::A2DP_ENUMERATION_ATTEMPTS, 5);
        assert_eq!(
            MediaController::A2DP_ENUMERATION_INTERVAL,
            Duration::from_secs(1)
        );
    }

    #[tokio::test]
    async fn a2dp_recovery_no_restart_when_available() {
        assert_eq!(run_recovery(Some(1), true).await, (true, 1, 0, 0));
    }

    #[tokio::test]
    async fn a2dp_recovery_no_restart_when_profile_appears_in_grace() {
        let attempts = MediaController::A2DP_ENUMERATION_ATTEMPTS;
        // Check 1 is the initial one; checks 2..=1+attempts form the grace wait.
        for k in 1..=attempts {
            let (ok, checks, restarts, sleeps) = run_recovery(Some(1 + k), true).await;
            assert!(ok);
            assert_eq!(checks, 1 + k);
            assert_eq!(restarts, 0);
            assert_eq!(sleeps, k - 1);
        }
    }

    #[tokio::test]
    async fn a2dp_recovery_restarts_once_then_gives_up() {
        let attempts = MediaController::A2DP_ENUMERATION_ATTEMPTS;
        let (ok, checks, restarts, sleeps) = run_recovery(None, true).await;
        assert!(!ok);
        assert_eq!(restarts, 1);
        assert_eq!(checks, 1 + 2 * attempts);
        // No sleep after the final attempt of either wait.
        assert_eq!(sleeps, 2 * (attempts - 1));
    }

    #[tokio::test]
    async fn a2dp_recovery_succeeds_after_restart() {
        let attempts = MediaController::A2DP_ENUMERATION_ATTEMPTS;
        let (ok, checks, restarts, sleeps) = run_recovery(Some(2 + attempts), true).await;
        assert!(ok);
        assert_eq!((checks, restarts, sleeps), (2 + attempts, 1, attempts - 1));
    }

    #[tokio::test]
    async fn a2dp_recovery_gives_up_when_restart_fails() {
        let attempts = MediaController::A2DP_ENUMERATION_ATTEMPTS;
        let (ok, checks, restarts, sleeps) = run_recovery(None, false).await;
        assert!(!ok);
        assert_eq!((checks, restarts, sleeps), (1 + attempts, 1, attempts - 1));
    }

    // #264: the A2DP hold.

    #[test]
    fn hold_profile_turns_a2dp_off() {
        for active in ALL_A2DP {
            assert_eq!(hold_profile(Some(active)), Some("off"));
        }
    }

    #[test]
    fn hold_profile_leaves_off_call_and_unknown() {
        for active in [Some("off"), Some("headset-head-unit"), None] {
            assert_eq!(hold_profile(active), None, "{active:?}");
        }
    }

    #[test]
    fn release_profile_from_off_takes_the_a2dp_choice() {
        let available = profiles(&["off", "a2dp-sink-sbc", "a2dp-sink"]);
        let release =
            |cached| release_profile(&available, Some("off"), cached, PreferredCodec::Aac);
        // The cached choice if available, else the preferred order.
        assert_eq!(release(Some("a2dp-sink-sbc")), Some("a2dp-sink-sbc".into()));
        assert_eq!(release(Some("a2dp-sink-sbc_xq")), Some("a2dp-sink".into()));
        assert_eq!(release(None), Some("a2dp-sink".into()));
    }

    #[test]
    fn release_profile_leaves_any_other_profile() {
        let available = profiles(&["off", "a2dp-sink", "headset-head-unit"]);
        for active in [Some("a2dp-sink"), Some("headset-head-unit"), None] {
            assert_eq!(
                release_profile(&available, active, Some("a2dp-sink"), PreferredCodec::Aac),
                None,
                "{active:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn hold_release_wait_ends_after_the_bound_without_a_sink() {
        let start = Instant::now();
        assert!(!wait_for_sink(|| async { None }).await);
        assert_eq!(start.elapsed(), HOLD_RELEASE_WAIT);
        assert!(HOLD_RELEASE_WAIT <= Duration::from_secs(3));
    }

    #[tokio::test(start_paused = true)]
    async fn hold_release_wait_ends_once_the_sink_appears() {
        let start = Instant::now();
        assert!(wait_for_sink(|| async { Some("bluez_output.AA".to_string()) }).await);
        assert_eq!(start.elapsed(), Duration::ZERO);
        // Found on the third look.
        let looks = std::cell::Cell::new(0);
        let found = wait_for_sink(|| {
            looks.set(looks.get() + 1);
            let found = looks.get() == 3;
            async move { found.then(|| "bluez_output.AA".to_string()) }
        })
        .await;
        assert!(found);
        assert_eq!(start.elapsed(), 2 * CARD_LOOKUP_INTERVAL);
    }

    /// An activation with every step immediate; `choose` runs `during` and
    /// asks for `a2dp-sink`. Returns the profiles `set` was called with.
    async fn activate_held(held: bool, during: impl FnOnce(&MediaController)) -> Vec<String> {
        let controller = MediaController::new(String::new(), String::new());
        controller.set_a2dp_held(held);
        let sets = std::cell::RefCell::new(Vec::new());
        let deadline = Instant::now() + A2DP_ACTIVATION_DEADLINE;
        let outcome = controller
            .activate_a2dp_profile_with(
                deadline,
                || async { Some(3) },
                || async { None },
                || async { true },
                |_| {
                    during(&controller);
                    async { ProfileChoice::Activate("a2dp-sink".to_string()) }
                },
                |_, profile| {
                    sets.borrow_mut().push(profile);
                    async { true }
                },
            )
            .await;
        assert_eq!(outcome, Ok(()));
        sets.into_inner()
    }

    #[tokio::test]
    async fn activate_skips_while_held() {
        assert_eq!(activate_held(true, |_| {}).await, Vec::<String>::new());
    }

    #[tokio::test]
    async fn activate_runs_when_not_held() {
        assert_eq!(activate_held(false, |_| {}).await, ["a2dp-sink"]);
    }

    #[tokio::test]
    async fn activate_skips_hold_set_mid_activation() {
        let sets = activate_held(false, |c| c.set_a2dp_held(true)).await;
        assert_eq!(sets, Vec::<String>::new());
    }

    /// A `hold` whose card read (A2DP for a hold, `off` for a release) runs
    /// `during`. Returns the profiles written.
    async fn apply_hold(hold: bool, during: impl FnOnce(&MediaController)) -> Vec<String> {
        let controller = MediaController::new(String::new(), String::new());
        controller.set_a2dp_held(hold);
        let sets = std::cell::RefCell::new(Vec::new());
        let card = pulse::Card {
            index: 3,
            name: None,
            device_string: None,
            profiles: profiles(&["off", "a2dp-sink"]),
            active_profile: Some(if hold { "a2dp-sink" } else { "off" }.into()),
        };
        let read = || {
            during(&controller);
            async { Some(card) }
        };
        let write = |_, profile| {
            sets.borrow_mut().push(profile);
            async { true }
        };
        let sink = || async { Some("bluez_output.AA".to_string()) };
        controller
            .apply_a2dp_hold_with(
                hold,
                "",
                PreferredCodec::Aac,
                read,
                || async { None },
                write,
                sink,
            )
            .await;
        sets.into_inner()
    }

    #[tokio::test]
    async fn hold_write_runs_when_current() {
        assert_eq!(apply_hold(true, |_| {}).await, ["off"]);
        assert_eq!(apply_hold(false, |_| {}).await, ["a2dp-sink"]);
    }

    #[tokio::test]
    async fn hold_write_skipped_when_superseded_during_the_read() {
        let none = Vec::<String>::new();
        assert_eq!(apply_hold(true, |c| c.set_a2dp_held(false)).await, none);
        assert_eq!(apply_hold(false, |c| c.set_a2dp_held(true)).await, none);
    }

    /// A release whose card read finds `active` (an activation won the gap
    /// since the flag cleared) and whose sink appears on the third look,
    /// with the flag `held` at the read. Returns the writes and the time.
    async fn release_on(active: &str, held: bool) -> (Vec<String>, Duration) {
        let controller = MediaController::new(String::new(), String::new());
        controller.set_a2dp_held(held);
        let sets = std::cell::RefCell::new(Vec::new());
        let card = pulse::Card {
            index: 3,
            name: None,
            device_string: None,
            profiles: profiles(&["off", "a2dp-sink"]),
            active_profile: Some(active.into()),
        };
        let looks = std::cell::Cell::new(0);
        let sink = || {
            looks.set(looks.get() + 1);
            let found = looks.get() == 3;
            async move { found.then(|| "bluez_output.AA".to_string()) }
        };
        let write = |_, profile| {
            sets.borrow_mut().push(profile);
            async { true }
        };
        let start = Instant::now();
        controller
            .apply_a2dp_hold_with(
                false,
                "",
                PreferredCodec::Aac,
                || async { Some(card) },
                || async { None },
                write,
                sink,
            )
            .await;
        (sets.into_inner(), start.elapsed())
    }

    #[tokio::test(start_paused = true)]
    async fn hold_release_wait_on_a_card_already_on_a2dp() {
        let none = Vec::<String>::new();
        let wait = 2 * CARD_LOOKUP_INTERVAL;
        assert_eq!(release_on("a2dp-sink", false).await, (none.clone(), wait));
        // Twins: superseded by a new hold, or a call profile: no wait.
        assert_eq!(
            release_on("a2dp-sink", true).await,
            (none.clone(), Duration::ZERO)
        );
        assert_eq!(
            release_on("headset-head-unit", false).await,
            (none, Duration::ZERO)
        );
    }

    /// An activation's write is in flight (stalled on a gate, like a pulse
    /// connect) when, `hold_after` later, a hold starts on the same fake card.
    /// Returns the activation's outcome, the card's last profile and the
    /// write log.
    async fn hold_during_activation_write(
        hold_after: Duration,
    ) -> (Result<(), DeadlineExceeded>, String, Vec<String>) {
        let controller = MediaController::new(String::new(), String::new());
        let card = Arc::new(std::sync::Mutex::new("off".to_string()));
        let log = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let set_card = {
            let (card, log) = (card.clone(), log.clone());
            move |profile: String| {
                *card.lock().unwrap() = profile.clone();
                log.lock().unwrap().push(profile);
                true
            }
        };
        let entered = Arc::new(tokio::sync::Notify::new());
        let gate = Arc::new(tokio::sync::Notify::new());
        let activation = controller.activate_a2dp_profile_with(
            Instant::now() + A2DP_ACTIVATION_DEADLINE,
            || async { Some(3) },
            || async { None },
            || async { true },
            |_| async { ProfileChoice::Activate("a2dp-sink".to_string()) },
            // Like `spawn_blocking`: dropping the wait doesn't stop the write.
            |_, profile| {
                let (entered, gate, set_card) = (entered.clone(), gate.clone(), set_card.clone());
                let write = tokio::spawn(async move {
                    entered.notify_one();
                    gate.notified().await;
                    set_card(profile)
                });
                async { write.await.unwrap() }
            },
        );
        let hold = async {
            entered.notified().await;
            tokio::time::sleep(hold_after).await;
            controller.set_a2dp_held(true);
            let read = || async {
                Some(pulse::Card {
                    index: 3,
                    name: None,
                    device_string: None,
                    profiles: profiles(&["off", "a2dp-sink"]),
                    active_profile: Some(card.lock().unwrap().clone()),
                })
            };
            let apply = controller.apply_a2dp_hold_with(
                true,
                "",
                PreferredCodec::Aac,
                read,
                || async { None },
                |_, profile| async { set_card(profile) },
                || async { None },
            );
            let open_gate = async {
                // A hold not kept behind the write reads `off` here.
                for _ in 0..10 {
                    tokio::task::yield_now().await;
                }
                gate.notify_one();
            };
            tokio::join!(apply, open_gate);
        };
        let (outcome, ()) = tokio::join!(activation, hold);
        // Let an orphaned write land.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        let last = card.lock().unwrap().clone();
        (outcome, last, log.lock().unwrap().clone())
    }

    /// The hold reads after the activation's write and turns the card `off`.
    fn hold_won() -> (Result<(), DeadlineExceeded>, String, Vec<String>) {
        let log = vec!["a2dp-sink".to_string(), "off".to_string()];
        (Ok(()), "off".to_string(), log)
    }

    #[tokio::test(start_paused = true)]
    async fn hold_wins_over_activation_write_in_flight() {
        let outcome = hold_during_activation_write(Duration::ZERO).await;
        assert_eq!(outcome, hold_won());
    }

    #[tokio::test(start_paused = true)]
    async fn hold_wins_over_activation_write_past_the_deadline() {
        let past = A2DP_ACTIVATION_DEADLINE + Duration::from_secs(1);
        assert_eq!(hold_during_activation_write(past).await, hold_won());
    }

    /// An activation whose initial lookup runs `during` and finds `first`,
    /// whose BlueZ connect answers `connected` after `took`, whose re-lookups
    /// find `again` and whose `choose` asks for `a2dp-sink`. Returns the
    /// outcome, the steps and the writes.
    async fn activate_connecting(
        controller: &MediaController,
        first: Option<u32>,
        (took, connected): (Duration, Result<(), String>),
        again: Option<u32>,
        during: impl FnOnce(&MediaController),
    ) -> (Result<(), DeadlineExceeded>, Steps, Vec<(u32, String)>) {
        let steps = Steps::default();
        let sets = std::cell::RefCell::new(Vec::new());
        let outcome = controller
            .activate_a2dp_profile_with(
                Instant::now() + A2DP_ACTIVATION_DEADLINE,
                || {
                    during(controller);
                    steps.step("lookup", Duration::ZERO, first)
                },
                || {
                    connect_a2dp_then_find(
                        || steps.step("connect", took, connected),
                        || steps.step("find", Duration::ZERO, again),
                        tokio::time::sleep,
                    )
                },
                || steps.step("recover", Duration::ZERO, true),
                |_| {
                    let choice = ProfileChoice::Activate("a2dp-sink".to_string());
                    steps.step("choose", Duration::ZERO, choice)
                },
                |idx, profile| {
                    sets.borrow_mut().push((idx, profile));
                    steps.step("set", Duration::ZERO, true)
                },
            )
            .await;
        (outcome, steps, sets.into_inner())
    }

    /// No recover, choose or set step started.
    fn activation_stopped(steps: &Steps) -> bool {
        ["recover", "choose", "set"]
            .iter()
            .all(|step| steps.started(step) == 0)
    }

    #[tokio::test(start_paused = true)]
    async fn activation_without_card_connects_a2dp_profile_then_activates() {
        let controller = MediaController::new(String::new(), String::new());
        let (outcome, steps, sets) =
            activate_connecting(&controller, None, (Duration::ZERO, Ok(())), Some(7), |_| {}).await;
        assert_eq!(outcome, Ok(()));
        assert_eq!((steps.started("connect"), steps.started("find")), (1, 1));
        for step in ["recover", "choose", "set"] {
            assert_eq!(steps.started(step), 1, "{step}");
        }
        assert_eq!(sets, [(7, "a2dp-sink".to_string())]);
        assert_eq!(controller.state.lock().await.device_index, Some(7));
    }

    #[tokio::test(start_paused = true)]
    async fn activation_with_card_never_connects() {
        let controller = MediaController::new(String::new(), String::new());
        let (outcome, steps, sets) = activate_connecting(
            &controller,
            Some(3),
            (Duration::ZERO, Ok(())),
            Some(7),
            |_| {},
        )
        .await;
        assert_eq!(outcome, Ok(()));
        assert_eq!((steps.started("connect"), steps.started("find")), (0, 0));
        assert_eq!(sets, [(3, "a2dp-sink".to_string())]);
    }

    #[tokio::test(start_paused = true)]
    async fn activation_connect_profile_failure_gives_up_once() {
        let controller = MediaController::new(String::new(), String::new());
        let failed = Err("org.bluez.Error.Failed".to_string());
        let start = Instant::now();
        let (outcome, steps, sets) =
            activate_connecting(&controller, None, (Duration::ZERO, failed), Some(7), |_| {}).await;
        assert_eq!(outcome, Ok(()));
        assert_eq!(start.elapsed(), Duration::ZERO);
        assert_eq!((steps.started("connect"), steps.started("find")), (1, 0));
        assert!(activation_stopped(&steps));
        assert!(sets.is_empty());
        assert_eq!(controller.state.lock().await.device_index, None);
    }

    #[tokio::test(start_paused = true)]
    async fn activation_connect_profile_timeout_is_bounded() {
        let controller = MediaController::new(String::new(), String::new());
        let never = (Duration::from_secs(3600), Ok(()));
        let start = Instant::now();
        let (outcome, steps, sets) =
            activate_connecting(&controller, None, never, Some(7), |_| {}).await;
        assert_eq!(outcome, Ok(()));
        assert_eq!(start.elapsed(), A2DP_CONNECT_TIMEOUT);
        assert!(start.elapsed() <= A2DP_ACTIVATION_DEADLINE);
        assert_eq!((steps.started("connect"), steps.started("find")), (1, 0));
        assert!(activation_stopped(&steps));
        assert!(sets.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn activation_connect_then_card_still_missing_gives_up() {
        let controller = MediaController::new(String::new(), String::new());
        let start = Instant::now();
        let (outcome, steps, sets) =
            activate_connecting(&controller, None, (Duration::ZERO, Ok(())), None, |_| {}).await;
        assert_eq!(outcome, Ok(()));
        assert_eq!(
            start.elapsed(),
            card_lookup_max_wait(A2DP_CONNECT_LOOKUP_ATTEMPTS)
        );
        assert_eq!(
            (steps.started("connect"), steps.started("find")),
            (1, A2DP_CONNECT_LOOKUP_ATTEMPTS as usize)
        );
        assert!(activation_stopped(&steps));
        assert!(sets.is_empty());
        assert_eq!(controller.state.lock().await.device_index, None);
    }

    #[tokio::test(start_paused = true)]
    async fn activation_while_held_never_connects_a2dp_profile() {
        let controller = MediaController::new(String::new(), String::new());
        controller.set_a2dp_held(true);
        let (outcome, steps, sets) =
            activate_connecting(&controller, None, (Duration::ZERO, Ok(())), Some(7), |_| {}).await;
        assert_eq!(outcome, Ok(()));
        assert_eq!((steps.started("lookup"), steps.started("connect")), (0, 0));
        assert!(sets.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn activation_hold_set_during_lookup_skips_connect() {
        let controller = MediaController::new(String::new(), String::new());
        let (outcome, steps, sets) =
            activate_connecting(&controller, None, (Duration::ZERO, Ok(())), Some(7), |c| {
                c.set_a2dp_held(true)
            })
            .await;
        assert_eq!(outcome, Ok(()));
        assert_eq!((steps.started("lookup"), steps.started("connect")), (1, 0));
        assert!(activation_stopped(&steps));
        assert!(sets.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn activation_connect_waits_for_profile_write() {
        let controller = MediaController::new(String::new(), String::new());
        let held_for = Duration::from_secs(1);
        let start = Instant::now();
        let write = controller.profile_write.lock().await;
        let release = async move {
            tokio::time::sleep(held_for).await;
            drop(write);
        };
        let activation =
            activate_connecting(&controller, None, (Duration::ZERO, Ok(())), Some(7), |_| {});
        let ((outcome, steps, sets), ()) = tokio::join!(activation, release);
        assert_eq!(outcome, Ok(()));
        let connects: Vec<Instant> = steps
            .0
            .borrow()
            .iter()
            .filter(|(n, _)| *n == "connect")
            .map(|(_, t)| *t)
            .collect();
        assert_eq!(connects, [start + held_for]);
        assert_eq!(sets, [(7, "a2dp-sink".to_string())]);
    }

    fn card_on(active: &str) -> pulse::Card {
        pulse::Card {
            index: 3,
            name: None,
            device_string: None,
            profiles: profiles(&["off", "a2dp-sink"]),
            active_profile: Some(active.into()),
        }
    }

    /// A `hold` with the flag at `held`, whose card read finds a card on
    /// `found` and whose A2DP profile connect re-reads one on `again`; the
    /// sink is there. Returns the connects and the writes.
    async fn hold_connecting(
        hold: bool,
        held: bool,
        found: Option<&str>,
        again: Option<&str>,
    ) -> (usize, Vec<String>) {
        let controller = MediaController::new(String::new(), String::new());
        controller.set_a2dp_held(held);
        let steps = Steps::default();
        let sets = std::cell::RefCell::new(Vec::new());
        controller
            .apply_a2dp_hold_with(
                hold,
                "",
                PreferredCodec::Aac,
                || async { found.map(card_on) },
                || {
                    connect_a2dp_then_find(
                        || steps.step("connect", Duration::ZERO, Ok(())),
                        || async { again.map(card_on) },
                        tokio::time::sleep,
                    )
                },
                |_, profile| {
                    sets.borrow_mut().push(profile);
                    async { true }
                },
                || async { Some("bluez_output.AA".to_string()) },
            )
            .await;
        (steps.started("connect"), sets.into_inner())
    }

    #[tokio::test(start_paused = true)]
    async fn hold_release_without_card_connects_a2dp_profile() {
        let none = Vec::<String>::new();
        // The new card is off: the A2DP choice is written.
        assert_eq!(
            hold_connecting(false, false, None, Some("off")).await,
            (1, vec!["a2dp-sink".to_string()])
        );
        // Already on A2DP: no write, the sink wait as today.
        assert_eq!(
            hold_connecting(false, false, None, Some("a2dp-sink")).await,
            (1, none.clone())
        );
        // Still no card after the connect.
        assert_eq!(hold_connecting(false, false, None, None).await, (1, none));
    }

    #[tokio::test(start_paused = true)]
    async fn hold_release_with_card_unchanged() {
        assert_eq!(
            hold_connecting(false, false, Some("off"), None).await,
            (0, vec!["a2dp-sink".to_string()])
        );
        assert_eq!(
            hold_connecting(false, false, Some("a2dp-sink"), None).await,
            (0, vec![])
        );
    }

    #[tokio::test(start_paused = true)]
    async fn hold_on_without_card_never_connects() {
        let off = Some("off");
        assert_eq!(hold_connecting(true, true, None, off).await, (0, vec![]));
    }

    #[tokio::test(start_paused = true)]
    async fn hold_release_superseded_never_connects() {
        let off = Some("off");
        assert_eq!(hold_connecting(false, true, None, off).await, (0, vec![]));
    }

    #[tokio::test]
    async fn connect_a2dp_profile_skips_disconnected_device() {
        for connected in [false, true] {
            let calls = std::cell::Cell::new(0);
            let result = connect_if_connected(
                || async move { connected },
                || {
                    calls.set(calls.get() + 1);
                    async { Ok(()) }
                },
            )
            .await;
            assert_eq!(calls.get(), usize::from(connected));
            let refused = Err(NOT_CONNECTED.to_string());
            assert_eq!(result, if connected { Ok(()) } else { refused });
        }
    }

    fn bluez_error(kind: bluer::ErrorKind) -> bluer::Result<()> {
        let message = format!("{kind:?}");
        Err(bluer::Error { kind, message })
    }

    #[test]
    fn connect_profile_reply_maps_bluez_errors() {
        use bluer::ErrorKind::*;
        assert_eq!(connect_profile_reply(Ok(())), Ok(()));
        for kind in [InProgress, AlreadyConnected] {
            assert_eq!(
                connect_profile_reply(bluez_error(kind.clone())),
                Ok(()),
                "{kind:?}"
            );
        }
        for kind in [Failed, NotAvailable, NotReady] {
            assert!(
                connect_profile_reply(bluez_error(kind.clone())).is_err(),
                "{kind:?}"
            );
        }
    }

    #[tokio::test]
    async fn connect_a2dp_in_progress_still_relooks() {
        let finds = std::cell::Cell::new(0u32);
        let found = connect_a2dp_then_find(
            || async { connect_profile_reply(bluez_error(bluer::ErrorKind::InProgress)) },
            || {
                finds.set(finds.get() + 1);
                let n = finds.get();
                async move { (n == 3).then_some(7) }
            },
            |_| std::future::ready(()),
        )
        .await;
        assert_eq!((found, finds.get()), (Some(7), 3));
    }
}
