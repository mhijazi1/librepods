use crate::bluetooth::aacp::AACPManager;
use crate::bluetooth::aacp::EarDetectionStatus;
use crate::bluetooth::aacp::{AACPManagerState, AudioSource, AudioSourceType, ConnectedDevice};
use dbus::arg::RefArg;
use dbus::blocking::Connection;
use dbus::blocking::stdintf::org_freedesktop_dbus::Properties;
use libpulse_binding::callbacks::ListResult;
use libpulse_binding::context::introspect::SinkInfo;
use libpulse_binding::context::{Context, FlagSet as ContextFlagSet};
use libpulse_binding::def::Retval;
use libpulse_binding::mainloop::standard::Mainloop;
use libpulse_binding::operation::State as OperationState;
use libpulse_binding::proplist::Proplist;
use libpulse_binding::volume::{ChannelVolumes, Volume};
use log::{debug, error, info, warn};
use std::cell::RefCell;
use std::collections::HashMap;
use std::process::Command;
use std::rc::Rc;
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::AbortHandle;

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
}

/// Copy the routing facts out of the AACP fields. Pure and decision-free so
/// the handoff core can feed it from its own event mirror.
pub(crate) fn routing_snapshot(
    has_session: bool,
    hosts: &[ConnectedDevice],
    ears: &[EarDetectionStatus],
    audio: Option<&AudioSource>,
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
    }
}

/// `routing_snapshot` of a locked AACP state. Callers drop the guard before
/// any await.
pub(crate) fn routing_snapshot_of(state: &AACPManagerState, local: &str) -> RoutingSnapshot {
    routing_snapshot(
        state.sender.is_some(),
        &state.connected_devices,
        &state.ear_detection_status,
        state.audio_source.as_ref(),
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
    /// call or remote media, unknown activity, or no report).
    AudioNotIdle,
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

/// The #841 guards, in order: case recovery, live session, Linux plus a
/// remote host (this machine and at least one other device, so 2, 3, 4...
/// hosts), audio source idle or local media.
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
        match routing.audio {
            Some(AudioSourceType::None) => None,
            Some(AudioSourceType::Media) if routing.audio_from_local => None,
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

/// Worst-case waiting in `activate_a2dp_profile`, from the production
/// constants: the initial full card lookup finds the card on its last try,
/// then the card is missing at every enumeration check, so both waits run all
/// their checks (each a `CARD_LOOKUP_ATTEMPTS_PER_CHECK` lookup) and
/// WirePlumber is restarted once in between. The run time of the lookups
/// themselves and of `systemctl` is not counted.
const fn a2dp_activation_worst_case_wait() -> Duration {
    let wait = MediaController::A2DP_ENUMERATION_INTERVAL
        .saturating_mul(MediaController::A2DP_ENUMERATION_ATTEMPTS.saturating_sub(1))
        .saturating_add(
            card_lookup_max_wait(CARD_LOOKUP_ATTEMPTS_PER_CHECK)
                .saturating_mul(MediaController::A2DP_ENUMERATION_ATTEMPTS),
        );
    card_lookup_max_wait(CARD_LOOKUP_ATTEMPTS)
        .saturating_add(wait)
        .saturating_add(WIREPLUMBER_SETTLE)
        .saturating_add(wait)
}

const _: () = assert!(
    a2dp_activation_worst_case_wait().as_millis() <= A2DP_RECOVERY_BOUND.as_millis(),
    "activate_a2dp_profile worst case exceeds A2DP_RECOVERY_BOUND"
);

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
async fn retry_card_lookup_with<F, Fut, S, SFut>(
    attempts: u32,
    interval: Duration,
    mut lookup: F,
    mut sleep: S,
) -> Option<u32>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<u32>>,
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
/// activation waits ≈ 12.75 s plus lookup and systemctl run time, within
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

#[derive(Clone, Debug)]
struct OwnedCardProfileInfo {
    name: Option<String>,
}

#[derive(Clone, Debug)]
struct OwnedCardInfo {
    index: u32,
    proplist: Proplist,
    profiles: Vec<OwnedCardProfileInfo>,
    active_profile: Option<String>,
}

#[derive(Clone, Debug)]
struct OwnedSinkInfo {
    name: Option<String>,
    proplist: Proplist,
    volume: ChannelVolumes,
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
}

impl MediaController {
    pub fn new(connected_mac: String, local_mac: String) -> Self {
        let mut state = MediaControllerState::new();
        state.connected_device_mac = connected_mac;
        state.local_mac = local_mac;
        MediaController {
            state: Arc::new(Mutex::new(state)),
        }
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

            // Read and consume the self-pause flag under the same lock as the
            // playback state, so a pause_all_media racing with this poll is not
            // lost.
            let mut state = self.state.lock().await;
            let was_playing = state.is_playing;
            state.is_playing = is_playing;
            let local_mac = state.local_mac.clone();
            let decision = decide_takeover(TakeoverInputs {
                baseline_taken,
                was_playing,
                is_playing,
                i_paused_the_media: state.i_paused_the_media,
                bud_in_ear,
                trigger: TakeoverTrigger::PlaybackPoll,
                // The #841 routing guards are for the handoff core (#8/#9).
                handoff: None,
            });
            if decision.clear_self_pause {
                state.i_paused_the_media = false;
            }
            drop(state);
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
                    // Only produced by the #841 guards, which this loop does
                    // not supply (`handoff: None`).
                    TakeoverSkip::AwaitingNewPlayback
                    | TakeoverSkip::NoSession
                    | TakeoverSkip::NoRemoteHost
                    | TakeoverSkip::AudioNotIdle => {}
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

    pub(crate) fn check_if_playing() -> bool {
        let conn = match Connection::new_session() {
            Ok(c) => c,
            Err(_) => return false,
        };
        Self::list_mpris_services(&conn).iter().any(|service| {
            let proxy = conn.with_proxy(service, "/org/mpris/MediaPlayer2", Duration::from_secs(5));
            proxy
                .get::<String>("org.mpris.MediaPlayer2.Player", "PlaybackStatus")
                .map(|s| s == "Playing")
                .unwrap_or(false)
        })
    }

    fn is_kdeconnect_service(service: &str) -> bool {
        service.starts_with("org.mpris.MediaPlayer2.kdeconnect.mpris_")
    }

    pub async fn handle_ear_detection(
        &self,
        old_statuses: Vec<EarDetectionStatus>,
        new_statuses: Vec<EarDetectionStatus>,
    ) {
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
            self.pause().await;
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
                self.pause().await;
                self.state.lock().await.i_paused_the_media = true;
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
                .map(|c| {
                    let names = c.profiles.into_iter().filter_map(|p| p.name).collect();
                    (names, c.active_profile)
                })
                .unwrap_or_default()
        })
        .await
        .unwrap_or_default()
    }

    /// Resolves the card by Bluetooth MAC and refreshes the cached index.
    /// PipeWire can assign a different index after a disconnect/reconnect.
    /// Polls the full `CARD_LOOKUP_ATTEMPTS` budget.
    async fn refresh_device_index(&self) -> Option<u32> {
        self.refresh_device_index_within(CARD_LOOKUP_ATTEMPTS).await
    }

    /// `refresh_device_index` with a single lookup and no sleep, for the
    /// checks inside the A2DP enumeration waits (#32).
    async fn recheck_device_index(&self) -> Option<u32> {
        self.refresh_device_index_within(CARD_LOOKUP_ATTEMPTS_PER_CHECK)
            .await
    }

    async fn refresh_device_index_within(&self, attempts: u32) -> Option<u32> {
        let mac = self.state.lock().await.connected_device_mac.clone();
        if mac.is_empty() {
            return None;
        }
        let index = self.find_audio_device_index(&mac, attempts).await;
        self.state.lock().await.device_index = index;
        index
    }

    async fn restart_wire_plumber(&self) -> bool {
        info!("Restarting WirePlumber to rediscover A2DP profiles");
        let result = Command::new("systemctl")
            .args(["--user", "restart", "wireplumber"])
            .output();

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

        if self.state.lock().await.connected_device_mac.is_empty() {
            warn!("Connected device MAC is empty, cannot activate A2DP profile");
            return;
        }

        // Always resolve the card by MAC first: a reconnect can change its index.
        if self.refresh_device_index().await.is_none() {
            warn!("Could not get device index. Cannot activate A2DP profile.");
            return;
        }

        // The first check reuses the index resolved just above; every later
        // check re-resolves the card by MAC, as a reconnect can change it.
        // Those re-checks are single-shot: the wait already spaces them, and
        // nesting the ~3 s poll in each one made the worst case ~43 s (#32).
        let mut first_check = true;
        let available = recover_a2dp_availability(
            Self::A2DP_ENUMERATION_ATTEMPTS,
            Self::A2DP_ENUMERATION_INTERVAL,
            || {
                let refresh = !std::mem::replace(&mut first_check, false);
                async move {
                    (!refresh || self.recheck_device_index().await.is_some())
                        && self.is_a2dp_profile_available().await
                }
            },
            || self.restart_wire_plumber(),
            tokio::time::sleep,
        )
        .await;
        if !available {
            error!("A2DP profile unavailable, skipping profile activation");
            return;
        }

        let Some(idx) = self.state.lock().await.device_index else {
            error!("Device index not available for activating profile.");
            return;
        };

        let preferred_profile = match self.get_preferred_a2dp_profile(idx).await {
            // Leave an already-active A2DP variant alone. Switching profiles
            // recreates the PipeWire sink and stops the stream that triggered
            // this activation, so players pause again right after the user
            // pressed play.
            ProfileChoice::KeepActive(active) => {
                debug!(
                    "A2DP profile {} already active, leaving it unchanged",
                    active
                );
                return;
            }
            ProfileChoice::Unavailable => {
                error!("No suitable A2DP profile found");
                return;
            }
            ProfileChoice::Activate(profile) => profile,
        };

        info!("Activating A2DP profile for AirPods: {}", preferred_profile);
        let profile_name = preferred_profile.clone();
        let success = tokio::task::spawn_blocking(move || set_card_profile_sync(idx, &profile_name))
            .await
            .unwrap_or(false);

        if success {
            info!("Successfully activated A2DP profile: {}", preferred_profile);
        } else {
            warn!("Failed to activate A2DP profile: {}", preferred_profile);
        }
    }

    async fn pause(&self) {
        debug!("Pausing playback");

        let paused_services = tokio::task::spawn_blocking(|| {
            let conn = match Connection::new_session() {
                Ok(c) => c,
                Err(_) => return vec![],
            };
            let mut paused_services = Vec::new();

            for service in Self::list_mpris_services(&conn) {
                debug!("Checking playback status for service: {}", service);
                let proxy = conn.with_proxy(
                    &service,
                    "/org/mpris/MediaPlayer2",
                    Duration::from_secs(5),
                );

                if let Ok(playback_status) =
                    proxy.get::<String>("org.mpris.MediaPlayer2.Player", "PlaybackStatus")
                    && playback_status == "Playing"
                {
                    if proxy
                        .method_call::<(), _, &str, &str>(
                            "org.mpris.MediaPlayer2.Player",
                            "Pause",
                            (),
                        )
                        .is_ok()
                    {
                        info!("Paused playback for: {}", service);
                        paused_services.push(service);
                    } else {
                        error!("Failed to pause {}", service);
                    }
                }
            }

            paused_services
        })
            .await
            .unwrap();

        if !paused_services.is_empty() {
            info!("Paused {} media player(s) via DBus", paused_services.len());
            let mut state = self.state.lock().await;
            state.paused_by_app_services = paused_services;
            state.is_playing = false;
        } else {
            info!("No playing media players found to pause");
        }
    }

    pub async fn pause_all_media(&self) {
        self.pause_all_media_with(Self::pause_playing_mpris_players)
            .await;
    }

    /// `pause_all_media` with the D-Bus side injected, so the self-pause
    /// marking can be tested without a session bus.
    pub(crate) async fn pause_all_media_with<F>(&self, pause_players: F)
    where
        F: FnOnce() -> usize + Send + 'static,
    {
        debug!("Pausing all media (without tracking for resume)");

        // Remember that the pause came from us, so the playback listener does
        // not mistake the players coming back for the user starting something.
        // Set before talking to D-Bus, so a failed pause still marks it.
        self.state.lock().await.i_paused_the_media = true;

        let paused_count = tokio::task::spawn_blocking(pause_players).await.unwrap();

        if paused_count > 0 {
            info!("Paused {} media player(s) due to ownership loss", paused_count);
            self.state.lock().await.is_playing = false;
        } else {
            debug!("No playing media players found to pause");
        }
    }

    /// Pause every MPRIS player that reports `Playing`; returns how many
    /// paused. Blocking: run it on a blocking thread.
    pub(crate) fn pause_playing_mpris_players() -> usize {
        let conn = match Connection::new_session() {
            Ok(c) => c,
            Err(_) => return 0,
        };
        let mut paused_count = 0;

        for service in Self::list_mpris_services(&conn) {
            let proxy =
                conn.with_proxy(&service, "/org/mpris/MediaPlayer2", Duration::from_secs(5));
            if let Ok(playback_status) =
                proxy.get::<String>("org.mpris.MediaPlayer2.Player", "PlaybackStatus")
                && playback_status == "Playing"
            {
                if proxy
                    .method_call::<(), _, &str, &str>("org.mpris.MediaPlayer2.Player", "Pause", ())
                    .is_ok()
                {
                    info!("Paused playback for: {}", service);
                    paused_count += 1;
                } else {
                    error!("Failed to pause {}", service);
                }
            }
        }
        paused_count
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
                .map(|card| {
                    card.profiles
                        .iter()
                        .any(|p| p.name.as_ref().is_some_and(|n| n.starts_with("a2dp-sink")))
                })
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
        self.find_audio_device_index(mac, CARD_LOOKUP_ATTEMPTS)
            .await
    }

    /// Looks the card up by MAC at most `attempts` times, `CARD_LOOKUP_INTERVAL`
    /// apart.
    async fn find_audio_device_index(&self, mac: &str, attempts: u32) -> Option<u32> {
        if mac.is_empty() {
            return None;
        }

        let found = retry_card_lookup(attempts, CARD_LOOKUP_INTERVAL, || {
            let mac_clone = mac.to_string();
            async move {
                tokio::task::spawn_blocking(move || {
                    get_card_info_list_sync().into_iter().find_map(|card| {
                        let device_string = card.proplist.get_str("device.string")?;
                        device_string.contains(&mac_clone).then_some(card.index)
                    })
                })
                .await
                .unwrap_or(None)
            }
        })
        .await;

        match found {
            Some(index) => {
                info!("Found audio device index for MAC {}: {}", mac, index);
                Some(index)
            }
            None if attempts >= CARD_LOOKUP_ATTEMPTS => {
                error!(
                    "No matching Bluetooth card found for MAC address: {} after {} attempts ({:?})",
                    mac,
                    attempts,
                    card_lookup_max_wait(attempts)
                );
                None
            }
            None => {
                // A short re-check inside a wait; the wait logs the outcome.
                debug!(
                    "No Bluetooth card for MAC address {} after {} attempt(s)",
                    mac, attempts
                );
                None
            }
        }
    }

    pub async fn deactivate_a2dp_profile(&self) {
        debug!("Entering deactivate_a2dp_profile");
        let mut state = self.state.lock().await;

        if state.device_index.is_none() {
            state.device_index = self
                .get_audio_device_index(&state.connected_device_mac)
                .await;
        }

        if state.connected_device_mac.is_empty() || state.device_index.is_none() {
            warn!("Connected device MAC or index is empty, cannot deactivate A2DP profile");
            return;
        }
        let device_index = state.device_index.unwrap();
        drop(state);

        info!("Deactivating A2DP profile for AirPods by setting to off");

        let success = tokio::task::spawn_blocking(move || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                set_card_profile_sync(device_index, "off")
            }))
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

    pub async fn handle_conversational_awareness(&self, status: u8) {
        debug!("Entering handle_conversational_awareness with status: {}", status);

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
                    tokio::task::spawn_blocking(move || transition_sink_volume(&sink_clone, target))
                        .await
                        .unwrap_or(false);
                    info!(
                        "Conversation partial increase (3): set volume to {} (original {})",
                        target, orig
                    );
                } else if let Some(orig_from_current) = current_volume_opt {
                    let target = orig_from_current.min(25);
                    let sink_clone = sink.clone();
                    tokio::task::spawn_blocking(move || transition_sink_volume(&sink_clone, target))
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
                    .method_call::<(), _, &str, &str>(
                        "org.mpris.MediaPlayer2.Player",
                        command,
                        (),
                    )
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
            .filter(|s| {
                s.starts_with("org.mpris.MediaPlayer2.") && !Self::is_kdeconnect_service(s)
            })
            .collect()
    }
}

// --- PulseAudio helpers ---

fn pulse_connect() -> Option<(Mainloop, Context)> {
    let mut mainloop = Mainloop::new()?;
    let mut context = Context::new(&mainloop, "LibrePods")?;
    context.connect(None, ContextFlagSet::NOAUTOSPAWN, None).ok()?;
    loop {
        mainloop.iterate(false);
        match context.get_state() {
            libpulse_binding::context::State::Ready => break,
            libpulse_binding::context::State::Failed
            | libpulse_binding::context::State::Terminated => return None,
            _ => {}
        }
    }
    Some((mainloop, context))
}

fn get_card_info_list_sync() -> Vec<OwnedCardInfo> {
    let (mut mainloop, context) = match pulse_connect() {
        Some(c) => c,
        None => return vec![],
    };

    let introspector = context.introspect();
    let cards: Rc<RefCell<Vec<OwnedCardInfo>>> = Rc::new(RefCell::new(Vec::new()));
    let op = introspector.get_card_info_list({
        let cards = cards.clone();
        move |result| {
            if let ListResult::Item(item) = result {
                let profiles = item
                    .profiles
                    .iter()
                    .map(|p| OwnedCardProfileInfo {
                        name: p.name.as_ref().map(|n| n.to_string()),
                    })
                    .collect();
                let active_profile = item
                    .active_profile
                    .as_ref()
                    .and_then(|p| p.name.as_ref().map(|n| n.to_string()));
                cards.borrow_mut().push(OwnedCardInfo {
                    index: item.index,
                    proplist: item.proplist.clone(),
                    profiles,
                    active_profile,
                });
            }
        }
    });

    while op.get_state() == OperationState::Running {
        mainloop.iterate(false);
    }
    mainloop.quit(Retval(0));
    Rc::try_unwrap(cards).unwrap().into_inner()
}

fn get_sink_volume_percent_by_name_sync(sink_name: &str) -> Option<u32> {
    let (mut mainloop, context) = pulse_connect()?;
    let introspector = context.introspect();
    let sink_info: Rc<RefCell<Option<OwnedSinkInfo>>> = Rc::new(RefCell::new(None));
    let op = introspector.get_sink_info_by_name(sink_name, {
        let sink_info = sink_info.clone();
        move |result: ListResult<&SinkInfo>| {
            if let ListResult::Item(item) = result {
                *sink_info.borrow_mut() = Some(OwnedSinkInfo {
                    name: item.name.as_ref().map(|s| s.to_string()),
                    proplist: item.proplist.clone(),
                    volume: item.volume,
                });
            }
        }
    });
    while op.get_state() == OperationState::Running {
        mainloop.iterate(false);
    }
    mainloop.quit(Retval(0));

    let borrowed = sink_info.borrow();
    let info = borrowed.as_ref()?;
    let channels = info.volume.len();
    if channels == 0 {
        return None;
    }
    let total: f64 = info.volume.get().iter().map(|v| v.0 as f64).sum();
    let percent = ((total / channels as f64 / Volume::NORMAL.0 as f64) * 100.0).round() as u32;
    Some(percent)
}

fn set_card_profile_sync(card_index: u32, profile_name: &str) -> bool {
    let (mut mainloop, context) = match pulse_connect() {
        Some(c) => c,
        None => return false,
    };
    let mut introspector = context.introspect();
    let op = introspector.set_card_profile_by_index(card_index, profile_name, None);
    while op.get_state() == OperationState::Running {
        mainloop.iterate(false);
    }
    mainloop.quit(Retval(0));
    true
}

pub fn transition_sink_volume(sink_name: &str, target_volume: u32) -> bool {
    let (mut mainloop, context) = match pulse_connect() {
        Some(c) => c,
        None => return false,
    };
    let mut introspector = context.introspect();
    let sink_info: Rc<RefCell<Option<OwnedSinkInfo>>> = Rc::new(RefCell::new(None));
    let op = introspector.get_sink_info_by_name(sink_name, {
        let sink_info = sink_info.clone();
        move |result: ListResult<&SinkInfo>| {
            if let ListResult::Item(item) = result {
                *sink_info.borrow_mut() = Some(OwnedSinkInfo {
                    name: item.name.as_ref().map(|s| s.to_string()),
                    proplist: item.proplist.clone(),
                    volume: item.volume,
                });
            }
        }
    });
    while op.get_state() == OperationState::Running {
        mainloop.iterate(false);
    }

    if let Some(info) = sink_info.borrow().as_ref() {
        let channels = info.volume.len();
        let mut new_volumes = ChannelVolumes::default();
        let raw =
            (((target_volume as f64) / 100.0) * Volume::NORMAL.0.as_f64().unwrap()).round() as u32;
        new_volumes.set(channels, Volume(raw));

        let op = introspector.set_sink_volume_by_name(sink_name, &new_volumes, None);
        while op.get_state() == OperationState::Running {
            mainloop.iterate(false);
        }
        mainloop.quit(Retval(0));
        true
    } else {
        error!("Sink not found: {}", sink_name);
        false
    }
}

async fn get_sink_name_by_mac(mac: &str) -> Option<String> {
    if mac.is_empty() {
        return None;
    }
    let mac_clone = mac.to_string();

    tokio::task::spawn_blocking(move || {
        let (mut mainloop, context) = pulse_connect()?;
        let introspector = context.introspect();
        let sink_list: Rc<RefCell<Vec<OwnedSinkInfo>>> = Rc::new(RefCell::new(Vec::new()));
        let op = introspector.get_sink_info_list({
            let sink_list = sink_list.clone();
            move |result: ListResult<&SinkInfo>| {
                if let ListResult::Item(item) = result {
                    sink_list.borrow_mut().push(OwnedSinkInfo {
                        name: item.name.as_ref().map(|s| s.to_string()),
                        proplist: item.proplist.clone(),
                        volume: item.volume,
                    });
                }
            }
        });
        while op.get_state() == OperationState::Running {
            mainloop.iterate(false);
        }
        mainloop.quit(Retval(0));

        for sink in sink_list.borrow().iter() {
            if let Some(device_string) = sink.proplist.get_str("device.string")
                && device_string.to_uppercase().contains(&mac_clone.to_uppercase())
                && let Some(name) = &sink.name
            {
                info!("Found sink name for MAC {}: {}", mac_clone, name);
                return Some(name.to_string());
            }
            if let Some(bluez_path) = sink.proplist.get_str("bluez.path") {
                let mac_from_path = bluez_path
                    .split('/')
                    .next_back()
                    .unwrap_or("")
                    .replace("dev_", "")
                    .replace('_', ":");
                if mac_from_path.eq_ignore_ascii_case(&mac_clone)
                    && let Some(name) = &sink.name
                {
                    info!("Found sink name for MAC {}: {}", mac_clone, name);
                    return Some(name.to_string());
                }
            }
        }
        error!("No matching sink found for MAC address: {}", mac_clone);
        None
    })
    .await
    .unwrap_or(None)
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
    /// session, a bud in an ear and idle audio.
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
                TakeoverSkip::AudioNotIdle,
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

    #[tokio::test]
    async fn pause_all_media_marks_self_pause() {
        let controller = MediaController::new(String::new(), String::new());
        controller.state.lock().await.is_playing = true;

        // D-Bus unavailable or nothing paused: still our pause.
        controller.pause_all_media_with(|| 0).await;
        {
            let state = controller.state.lock().await;
            assert!(state.i_paused_the_media);
            assert!(state.is_playing);
        }

        controller.state.lock().await.i_paused_the_media = false;
        controller.pause_all_media_with(|| 2).await;
        let state = controller.state.lock().await;
        assert!(state.i_paused_the_media);
        assert!(!state.is_playing);
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
                async { None }
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
        assert_eq!(card_lookup_max_wait(CARD_LOOKUP_ATTEMPTS_PER_CHECK), Duration::ZERO);
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
        let per_check_lookup = CARD_LOOKUP_INTERVAL * (CARD_LOOKUP_ATTEMPTS_PER_CHECK - 1);
        let wait = interval * (attempts - 1) + per_check_lookup * attempts;
        let expected = initial_lookup + wait + WIREPLUMBER_SETTLE + wait;
        assert_eq!(a2dp_activation_worst_case_wait(), expected);
        assert!(expected <= A2DP_RECOVERY_BOUND, "{expected:?} > 15 s");
        // Still within the bound when the initial lookup is charged its
        // nominal CARD_LOOKUP_ATTEMPTS × CARD_LOOKUP_INTERVAL (~3 s).
        let nominal = CARD_LOOKUP_INTERVAL * CARD_LOOKUP_ATTEMPTS + wait + WIREPLUMBER_SETTLE + wait;
        assert!(nominal <= A2DP_RECOVERY_BOUND, "{nominal:?} > 15 s");

        // Replay the production sequence on a virtual clock: the initial full
        // lookup finds the card on its last try, then the card is missing at
        // every enumeration check (single-shot re-lookups) and the restart
        // succeeds.
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
                let n = lookups.get();
                async move { (n == CARD_LOOKUP_ATTEMPTS).then_some(3) }
            },
            tick,
        )
        .await;
        assert_eq!(initial, Some(3));

        let checks = std::cell::Cell::new(0u32);
        let restarts = std::cell::Cell::new(0u32);
        let recheck_lookups = std::cell::Cell::new(0u32);
        let ok = recover_a2dp_availability(
            attempts,
            interval,
            || {
                checks.set(checks.get() + 1);
                let refresh = checks.get() > 1;
                let (clock, recheck_lookups) = (&clock, &recheck_lookups);
                async move {
                    // The first check reuses the resolved index; the card
                    // has no A2DP profile yet.
                    if !refresh {
                        return false;
                    }
                    retry_card_lookup_with(
                        CARD_LOOKUP_ATTEMPTS_PER_CHECK,
                        CARD_LOOKUP_INTERVAL,
                        || {
                            recheck_lookups.set(recheck_lookups.get() + 1);
                            async { None }
                        },
                        |d| {
                            clock.set(clock.get() + d);
                            std::future::ready(())
                        },
                    )
                    .await
                    .is_some()
                }
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
        assert_eq!(recheck_lookups.get(), 2 * attempts * CARD_LOOKUP_ATTEMPTS_PER_CHECK);
        assert_eq!(clock.get(), expected);
        assert!(clock.get() <= A2DP_RECOVERY_BOUND);
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
}
