//! Reconnect after a host-driven drop (#307, DECISIONS #308).
//!
//! When another Apple host pushes Linux off the AirPods (BlueZ reason
//! `Remote`, case not closed), LibrePods reconnects only on a NEW Linux
//! playback start, never on a timer: a timer would ping-pong with the Mac's
//! ~45 s heal. A per-MAC watcher runs while the AirPods are disconnected in
//! the handoff mode. It owns its own playback poller (first sample =
//! baseline), decides at each play edge with the pure `decide`, spaces its
//! attempts with the pure `Backoff`, and shares a per-MAC in-flight guard
//! with le.rs. Each attempt leaves a marker (its play edge's category) the
//! next handoff task consumes to take for that playback (switching.rs); a
//! failure or a playback stop clears it.

use crate::bluetooth::aacp::AACPManager;
use crate::media_controller::AUDIO_CATEGORY_IDLE;
use crate::switching::{self, PlaybackSamples};
use crate::utils::{self, ConnectMode, SwitchingMode};
use log::info;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::task::AbortHandle;

/// The least spacing between two reconnect attempts.
/// Engineering defaults; the #291 q4 hardware run may retune them.
pub(crate) const RECONNECT_BACKOFF_MIN: Duration = Duration::from_secs(15);
/// The spacing never doubles past this.
pub(crate) const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(300);
/// A session up this long was stable (resets the backoff); a host-driven drop
/// sooner after an attempt doubles it.
pub(crate) const RECONNECT_STABLE: Duration = Duration::from_secs(60);
// Longer than the Mac's ~45 s heal (#308), so a heal-driven drop doubles.
const _: () = assert!(RECONNECT_STABLE.as_secs() > 45);
/// How long after the watcher's connect the next handoff task still takes
/// for the playback that caused it.
pub(crate) const RECONNECT_TAKE_WINDOW: Duration = Duration::from_secs(15);
/// How long that task waits for hosts, ears and an audio source.
pub(crate) const ROUTING_READY_WAIT: Duration = Duration::from_secs(5);
/// Bounds one `bluetoothctl connect`, so the guard le.rs shares is released.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// BlueZ's `Device1.Disconnected` reason: only `Remote` is host-driven.
pub(crate) fn drop_cause(reason: Option<&str>) -> bool {
    reason == Some("org.bluez.Reason.Remote")
}

/// The preferences read at a play edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Settings {
    pub(crate) mode: SwitchingMode,
    pub(crate) auto_connect: bool,
    pub(crate) connect_mode: ConnectMode,
    pub(crate) last_owner_linux: bool,
}

impl Settings {
    fn load(mac: &str) -> Self {
        Settings {
            mode: utils::load_switching_mode(),
            auto_connect: utils::load_auto_connect(mac),
            connect_mode: utils::load_connect_mode(mac),
            last_owner_linux: switching::last_owner_linux(mac),
        }
    }
}

/// What `decide` looks at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Facts {
    pub(crate) host_driven: bool,
    pub(crate) case_closed: bool,
    /// The previous playback sample since the drop; None at the baseline.
    pub(crate) previous: Option<bool>,
    pub(crate) playing: bool,
    pub(crate) settings: Settings,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Decision {
    Connect,
    Wait(&'static str),
}

/// Reconnect only for a host-driven drop with the case open, on a new Linux
/// playback start, in the handoff mode with autoConnect on and a connect
/// mode that allows the take. Pushing an Apple host off is no reason to wait
/// (the AirPods keep 2 hosts, #308).
pub(crate) fn decide(f: &Facts) -> Decision {
    let s = f.settings;
    if s.mode != SwitchingMode::Handoff {
        Decision::Wait("switching mode is not handoff")
    } else if !f.host_driven {
        Decision::Wait("the drop was not host-driven")
    } else if f.case_closed {
        Decision::Wait("the case was closed at the drop")
    } else if !(f.previous == Some(false) && f.playing) {
        Decision::Wait("no new Linux playback start")
    } else if !s.auto_connect {
        Decision::Wait("autoConnect is off")
    } else if s.connect_mode == ConnectMode::WhenLastConnected && !s.last_owner_linux {
        Decision::Wait("connect mode is when-last-connected and Linux was not the last owner")
    } else {
        Decision::Connect
    }
}

/// Attempt spacing: `delay` after the last attempt, doubled (up to the max)
/// by a failure or a host-driven drop within `RECONNECT_STABLE` of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Backoff {
    delay: Duration,
    last_attempt: Option<Instant>,
}

impl Default for Backoff {
    fn default() -> Self {
        Backoff {
            delay: RECONNECT_BACKOFF_MIN,
            last_attempt: None,
        }
    }
}

impl Backoff {
    pub(crate) fn ready(&self, now: Instant) -> bool {
        self.last_attempt
            .is_none_or(|at| now.saturating_duration_since(at) >= self.delay)
    }

    pub(crate) fn attempted(&mut self, now: Instant) {
        self.last_attempt = Some(now);
    }

    pub(crate) fn failed(&mut self) {
        self.delay = (self.delay * 2).min(RECONNECT_BACKOFF_MAX);
    }

    /// A host-driven drop at `at`.
    pub(crate) fn dropped(&mut self, at: Instant) {
        if self
            .last_attempt
            .is_some_and(|t| at.saturating_duration_since(t) < RECONNECT_STABLE)
        {
            self.failed();
        }
    }

    /// A session ended after being up for `up`.
    pub(crate) fn session_ended(&mut self, up: Duration) {
        if up >= RECONNECT_STABLE {
            *self = Backoff::default();
        }
    }
}

/// What BlueZ and the handoff task told us about one AirPods MAC.
#[derive(Default)]
struct Entry {
    /// Since the last Connected=true: the drop's cause and case state.
    host_driven: Option<bool>,
    case_closed: bool,
    dropped_at: Option<Instant>,
    up_since: Option<Instant>,
    backoff: Backoff,
    marker: Option<Marker>,
    watcher: Option<AbortHandle>,
}

/// The watcher's connect for playback, for the next handoff task to take.
#[derive(Clone, Copy)]
struct Marker {
    /// Linux's audio category at the play edge, for the take's category
    /// check.
    category: u32,
    /// None while the connect is in flight: BlueZ's Connected=true starts
    /// the task before `bluetoothctl connect` returns.
    connected_at: Option<Instant>,
    /// A handoff task started for it and is waiting for its routing: the
    /// watcher keeps watching for a stop until the task takes it.
    claimed: bool,
}

impl Marker {
    /// In flight, or within `RECONNECT_TAKE_WINDOW` of its connect.
    fn fresh(&self, now: Instant) -> bool {
        self.connected_at
            .is_none_or(|at| now.saturating_duration_since(at) <= RECONNECT_TAKE_WINDOW)
    }
}

static ENTRIES: LazyLock<Mutex<HashMap<String, Entry>>> = LazyLock::new(Default::default);

fn with_entry<T>(mac: &str, f: impl FnOnce(&mut Entry) -> T) -> T {
    let mut entries = ENTRIES.lock().unwrap_or_else(|e| e.into_inner());
    f(entries.entry(mac.to_ascii_uppercase()).or_default())
}

/// The tokio clock, so paused-clock tests move it.
fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}

/// BlueZ's `Device1.Disconnected(reason, message)`, before or after the
/// Connected=false it explains.
/// BlueZ may send one per bearer: a drop counts once.
pub(crate) fn disconnect_reason(mac: &str, reason: Option<&str>) {
    info!("Reconnect {mac}: disconnect reason {reason:?}");
    let host_driven = drop_cause(reason);
    with_entry(mac, |e| {
        let counted = e.host_driven == Some(true);
        e.host_driven = Some(host_driven);
        if host_driven
            && !counted
            && let Some(at) = e.dropped_at
        {
            e.backoff.dropped(at);
        }
    });
}

/// The handoff task's DropSession: whether the case was closed at the drop.
pub(crate) fn case_closed_at_drop(mac: &str, closed: bool) {
    with_entry(mac, |e| e.case_closed = closed);
}

#[cfg(test)]
pub(crate) fn closed_at_drop(mac: &str) -> bool {
    with_entry(mac, |e| e.case_closed)
}

/// BlueZ Connected=true for an AirPods MAC: a running watcher stops at its
/// next sample, unless its own connect is waiting for the handoff task.
pub(crate) fn device_connected(mac: &str) {
    let now = now();
    with_entry(mac, |e| {
        e.up_since = Some(now);
        e.host_driven = None;
        e.case_closed = false;
        e.dropped_at = None;
    });
}

/// BlueZ Connected=false for an AirPods MAC: record the drop, and in the
/// handoff mode watch for a new playback start. A repeat leaves the running
/// watcher (and its connect) alone.
pub(crate) fn device_disconnected(mac: &str) {
    if !record_drop(mac, now()) || utils::load_switching_mode() != SwitchingMode::Handoff {
        return;
    }
    let watched = mac.to_string();
    let task = tokio::spawn(async move {
        watch(watched, switching::spawn_playback_poller(), Bluez).await;
    });
    if let Some(old) = with_entry(mac, |e| e.watcher.replace(task.abort_handle())) {
        old.abort();
    }
}

/// Once per Connected cycle: a repeated Connected=false neither counts again
/// nor clears a marker whose connect is still running. Whether it was new.
fn record_drop(mac: &str, now: Instant) -> bool {
    with_entry(mac, |e| {
        if e.dropped_at.is_some() {
            return false;
        }
        if let Some(up) = e.up_since.take() {
            e.backoff.session_ended(now.saturating_duration_since(up));
        }
        e.marker = None;
        e.dropped_at = Some(now);
        if e.host_driven == Some(true) {
            e.backoff.dropped(now);
        }
        true
    })
}

/// The MAC in a BlueZ Device1 object path (`/org/bluez/hci0/dev_AA_BB_...`).
pub(crate) fn mac_from_path(path: &str) -> Option<String> {
    path.rsplit_once("/dev_")
        .map(|(_, dev)| dev.replace('_', ":"))
}

/// Connected=true whose UUIDs or Address read failed: `device_connected`,
/// but only for a recorded drop, which only an AirPods MAC has.
pub(crate) fn device_connected_if_dropped(mac: &str) {
    // Only `device_connected` clears `dropped_at`, so the check stays true.
    let dropped = ENTRIES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&mac.to_ascii_uppercase())
        .is_some_and(|e| e.dropped_at.is_some());
    if dropped {
        device_connected(mac);
    }
}

/// A connect for playback at `category` succeeded now.
#[cfg(test)]
pub(crate) fn set_marker(mac: &str, category: u32) {
    let connected_at = Some(now());
    with_entry(mac, |e| {
        e.marker = Some(Marker {
            category,
            connected_at,
            claimed: false,
        })
    });
}

/// A starting handoff task claims a fresh marker, left in place: the
/// watcher keeps watching for a playback stop, which clears it, until
/// `take_marker`. Whether there was one to claim.
pub(crate) fn claim_marker(mac: &str) -> bool {
    let now = now();
    with_entry(mac, |e| match e.marker.as_mut() {
        Some(m) if m.fresh(now) => {
            m.claimed = true;
            true
        }
        _ => false,
    })
}

/// The watcher is done with its marker: taken, cleared, or expired with no
/// task claiming it (removed here, as no one watches for its stop after).
fn marker_done(mac: &str) -> bool {
    let now = now();
    with_entry(mac, |e| {
        if e.marker.is_some_and(|m| m.claimed || m.fresh(now)) {
            return false;
        }
        e.marker = None;
        true
    })
}

/// Consume the marker: Linux's audio category at the play edge. Its age is
/// judged at `claim_marker`, before the routing wait; only a stop or a
/// failure removes a claimed one after that.
pub(crate) fn take_marker(mac: &str) -> Option<u32> {
    with_entry(mac, |e| e.marker.take()).map(|m| m.category)
}

/// Linux's playback stopped: no task takes for it. Whether a marker was
/// cleared.
pub(crate) fn playback_stopped(mac: &str) -> bool {
    with_entry(mac, |e| e.marker.take()).is_some()
}

/// Whether the session reports hosts, ears and an audio source within
/// `ROUTING_READY_WAIT`, as the probe's `wait_routing_ready`.
pub(crate) async fn routing_ready(manager: &AACPManager) -> bool {
    tokio::time::timeout(ROUTING_READY_WAIT, async {
        loop {
            let state = manager.state.lock().await;
            if state.sender.is_none() {
                return false;
            }
            if !state.connected_devices.is_empty()
                && !state.ear_detection_status.is_empty()
                && state.audio_source.is_some()
            {
                return true;
            }
            drop(state);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(false)
}

static IN_FLIGHT: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(Default::default);

/// One connect at a time per AirPods public MAC, shared by the watcher and
/// le.rs's auto-connect; released on drop.
pub(crate) struct ConnectGuard(String);

impl ConnectGuard {
    /// None while another connect to `mac` is in flight.
    pub(crate) fn try_new(mac: &str) -> Option<Self> {
        let key = mac.to_ascii_uppercase();
        let mut in_flight = IN_FLIGHT.lock().unwrap_or_else(|e| e.into_inner());
        in_flight.insert(key.clone()).then(|| ConnectGuard(key))
    }
}

impl Drop for ConnectGuard {
    fn drop(&mut self) {
        IN_FLIGHT
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.0);
    }
}

/// The watcher's playback samples: Linux's audio category.
pub(crate) trait Samples: Send + 'static {
    fn next(&mut self) -> impl Future<Output = Option<u32>> + Send;
}

impl Samples for PlaybackSamples {
    async fn next(&mut self) -> Option<u32> {
        self.recv().await.map(|(_, category)| category)
    }
}

/// The watcher's I/O, faked in tests.
pub(crate) trait Connector: Send + Sync + 'static {
    /// BlueZ already reports the device connected.
    fn connected(&self, mac: &str) -> impl Future<Output = bool> + Send;
    /// Connect the device; the error to log on failure.
    fn connect(&self, mac: &str) -> impl Future<Output = Result<(), String>> + Send;
    fn settings(&self, mac: &str) -> Settings;
    fn log(&self, mac: &str, line: &str);
}

struct Bluez;

impl Connector for Bluez {
    async fn connected(&self, mac: &str) -> bool {
        let Ok(address) = mac.parse::<bluer::Address>() else {
            return false;
        };
        async {
            let session = bluer::Session::new().await?;
            session
                .default_adapter()
                .await?
                .device(address)?
                .is_connected()
                .await
        }
        .await
        .unwrap_or(false)
    }

    // bluetoothctl, like le.rs: bluer's connect doesn't work there.
    async fn connect(&self, mac: &str) -> Result<(), String> {
        let command = tokio::process::Command::new("bluetoothctl")
            .arg("connect")
            .arg(mac)
            .kill_on_drop(true)
            .output();
        let output = tokio::time::timeout(CONNECT_TIMEOUT, command)
            .await
            .map_err(|_| "bluetoothctl connect timed out".to_string())?
            .map_err(|e| format!("failed to run bluetoothctl: {e}"))?;
        if output.status.success() {
            return Ok(());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!("{} {}", stdout.trim(), stderr.trim())
            .trim()
            .to_string())
    }

    fn settings(&self, mac: &str) -> Settings {
        Settings::load(mac)
    }

    fn log(&self, mac: &str, line: &str) {
        info!("Reconnect {mac}: {line}");
    }
}

/// Watch one disconnected AirPods MAC until it connects, the drop rules a
/// reconnect out, or the mode isn't handoff. Only a play edge (stopped then
/// playing, after the first sample) can start an attempt; a play the backoff
/// or le.rs's connect blocks isn't queued.
pub(crate) async fn watch(mac: String, mut samples: impl Samples, io: impl Connector) {
    let mut previous = None;
    let mut connected_for_playback = false;
    // The marker is for playback that still plays.
    let stopped = |playing: bool| {
        if !playing && playback_stopped(&mac) {
            io.log(&mac, "playback stopped before the handoff task took");
        }
    };
    while let Some(category) = samples.next().await {
        let playing = category > AUDIO_CATEGORY_IDLE;
        let edge = previous == Some(false) && playing;
        let last = previous.replace(playing);
        if connected_for_playback {
            stopped(playing);
            if !playing || marker_done(&mac) {
                return;
            }
            continue;
        }
        let (host_driven, case_closed, up) = with_entry(&mac, |e| {
            (e.host_driven, e.case_closed, e.up_since.is_some())
        });
        // These hold until the next Connected=true.
        if up || host_driven == Some(false) || case_closed {
            if host_driven == Some(false) {
                io.log(&mac, "not reconnecting: the drop was not host-driven");
            } else if case_closed {
                io.log(&mac, "not reconnecting: the case was closed at the drop");
            }
            return;
        }
        let settings = io.settings(&mac);
        if settings.mode != SwitchingMode::Handoff {
            io.log(&mac, "not reconnecting: switching mode is not handoff");
            return;
        }
        if !edge {
            continue;
        }
        let facts = Facts {
            host_driven: host_driven == Some(true),
            case_closed,
            previous: last,
            playing,
            settings,
        };
        if let Decision::Wait(reason) = decide(&facts) {
            io.log(&mac, &format!("not reconnecting: {reason}"));
            continue;
        }
        let now = now();
        if !with_entry(&mac, |e| e.backoff.ready(now)) {
            io.log(
                &mac,
                "new playback inside the reconnect backoff; not queued",
            );
            continue;
        }
        let Some(_guard) = ConnectGuard::try_new(&mac) else {
            io.log(&mac, "le.rs is already connecting; skipping");
            continue;
        };
        with_entry(&mac, |e| {
            e.backoff.attempted(now);
            // Before the connect: its Connected=true starts the task.
            e.marker = Some(Marker {
                category,
                connected_at: None,
                claimed: false,
            });
        });
        io.log(
            &mac,
            "new Linux playback after a host-driven drop; reconnecting",
        );
        let attempt = async {
            // Already connected: no Connected=true starts a task to take.
            if io.connected(&mac).await {
                Ok(true)
            } else {
                io.connect(&mac).await.map(|()| false)
            }
        };
        tokio::pin!(attempt);
        // Sample while connecting: a stop clears the marker, and no edge
        // waits in the poller's channel past the attempt.
        let result = loop {
            tokio::select! {
                result = &mut attempt => break result,
                Some(category) = samples.next() => {
                    let playing = category > AUDIO_CATEGORY_IDLE;
                    previous = Some(playing);
                    stopped(playing);
                }
            }
        };
        match result {
            Ok(already) => {
                // A task that already took it, or a stop, leaves it cleared.
                let now = self::now();
                with_entry(&mac, |e| {
                    if already {
                        e.marker = None;
                    } else if let Some(m) = e.marker.as_mut() {
                        m.connected_at = Some(now);
                    }
                });
                connected_for_playback = true;
                io.log(&mac, "reconnected for new playback");
            }
            Err(e) => {
                with_entry(&mac, |entry| {
                    entry.backoff.failed();
                    entry.marker = None;
                });
                io.log(&mac, &format!("reconnect failed: {e}"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media_controller::AUDIO_CATEGORY_MEDIA;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::mpsc;

    const REMOTE: &str = "org.bluez.Reason.Remote";

    fn settings() -> Settings {
        Settings {
            mode: SwitchingMode::Handoff,
            auto_connect: true,
            connect_mode: ConnectMode::Automatically,
            last_owner_linux: false,
        }
    }

    /// Host-driven, case open, a new play edge, every setting allowing it.
    fn connect_facts() -> Facts {
        Facts {
            host_driven: true,
            case_closed: false,
            previous: Some(false),
            playing: true,
            settings: settings(),
        }
    }

    fn waits(facts: Facts) -> bool {
        matches!(decide(&facts), Decision::Wait(_))
    }

    #[test]
    fn reconnect_drop_cause_remote_is_host_driven() {
        assert!(drop_cause(Some("org.bluez.Reason.Remote")));
    }

    #[test]
    fn reconnect_drop_cause_others_are_not_host_driven() {
        for reason in [
            "org.bluez.Reason.Local",
            "org.bluez.Reason.Timeout",
            "org.bluez.Reason.Suspend",
            "org.bluez.Reason.Authentication",
            "org.bluez.Reason.Unknown",
            "Remote",
            "",
        ] {
            assert!(!drop_cause(Some(reason)), "{reason}");
        }
    }

    #[test]
    fn reconnect_drop_cause_missing_is_not_host_driven() {
        assert!(!drop_cause(None));
    }

    #[test]
    fn reconnect_policy_connects_for_host_driven_drop_and_new_play() {
        assert_eq!(decide(&connect_facts()), Decision::Connect);
        // Linux the last owner also allows it in when-last-connected.
        let mut facts = connect_facts();
        facts.settings.connect_mode = ConnectMode::WhenLastConnected;
        facts.settings.last_owner_linux = true;
        assert_eq!(decide(&facts), Decision::Connect);
    }

    fn cause(reason: Option<&str>) -> Facts {
        Facts {
            host_driven: drop_cause(reason),
            ..connect_facts()
        }
    }

    #[test]
    fn reconnect_policy_waits_on_local_disconnect() {
        assert!(waits(cause(Some("org.bluez.Reason.Local"))));
    }

    #[test]
    fn reconnect_policy_waits_on_timeout() {
        assert!(waits(cause(Some("org.bluez.Reason.Timeout"))));
    }

    #[test]
    fn reconnect_policy_waits_on_suspend() {
        assert!(waits(cause(Some("org.bluez.Reason.Suspend"))));
    }

    #[test]
    fn reconnect_policy_waits_on_authentication() {
        assert!(waits(cause(Some("org.bluez.Reason.Authentication"))));
    }

    #[test]
    fn reconnect_policy_waits_on_unknown_or_missing_reason() {
        assert!(waits(cause(Some("org.bluez.Reason.Unknown"))));
        assert!(waits(cause(Some("something else"))));
        assert!(waits(cause(None)));
    }

    #[test]
    fn reconnect_policy_waits_when_case_closed_at_drop() {
        assert!(waits(Facts {
            case_closed: true,
            ..connect_facts()
        }));
    }

    #[test]
    fn reconnect_policy_waits_for_playback_running_at_drop() {
        // The first sample after the drop is the baseline.
        assert!(waits(Facts {
            previous: None,
            ..connect_facts()
        }));
    }

    #[test]
    fn reconnect_policy_waits_without_new_play_edge() {
        for (previous, playing) in [
            (Some(true), true),
            (Some(false), false),
            (Some(true), false),
        ] {
            assert!(waits(Facts {
                previous,
                playing,
                ..connect_facts()
            }));
        }
    }

    fn with(settings: Settings) -> Facts {
        Facts {
            settings,
            ..connect_facts()
        }
    }

    #[test]
    fn reconnect_policy_waits_in_legacy_mode() {
        assert!(waits(with(Settings {
            mode: SwitchingMode::Legacy,
            ..settings()
        })));
    }

    #[test]
    fn reconnect_policy_waits_in_off_mode() {
        assert!(waits(with(Settings {
            mode: SwitchingMode::Off,
            ..settings()
        })));
    }

    #[test]
    fn reconnect_policy_waits_with_auto_connect_off() {
        assert!(waits(with(Settings {
            auto_connect: false,
            ..settings()
        })));
    }

    #[test]
    fn reconnect_policy_waits_when_last_connected_and_not_last_owner() {
        assert!(waits(with(Settings {
            connect_mode: ConnectMode::WhenLastConnected,
            last_owner_linux: false,
            ..settings()
        })));
    }

    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    #[test]
    fn reconnect_backoff_spaces_attempts_by_min() {
        let t0 = Instant::now();
        let mut b = Backoff::default();
        assert!(b.ready(t0));
        b.attempted(t0);
        assert!(!b.ready(t0 + RECONNECT_BACKOFF_MIN - secs(1)));
        assert!(b.ready(t0 + RECONNECT_BACKOFF_MIN));
    }

    #[test]
    fn reconnect_backoff_doubles_on_failure_up_to_max() {
        let t0 = Instant::now();
        let mut b = Backoff::default();
        let mut at = t0;
        let mut expected = RECONNECT_BACKOFF_MIN;
        for _ in 0..10 {
            b.attempted(at);
            b.failed();
            expected = (expected * 2).min(RECONNECT_BACKOFF_MAX);
            assert!(!b.ready(at + expected - secs(1)));
            assert!(b.ready(at + expected));
            at += expected;
        }
        assert_eq!(expected, RECONNECT_BACKOFF_MAX);
    }

    #[test]
    fn reconnect_backoff_doubles_on_host_driven_drop_within_stable() {
        let t0 = Instant::now();
        let mut b = Backoff::default();
        b.attempted(t0);
        b.dropped(t0 + RECONNECT_STABLE - secs(1));
        assert!(!b.ready(t0 + RECONNECT_BACKOFF_MIN * 2 - secs(1)));
        assert!(b.ready(t0 + RECONNECT_BACKOFF_MIN * 2));
        // A drop later than that leaves the spacing as it is.
        let mut b = Backoff::default();
        b.attempted(t0);
        b.dropped(t0 + RECONNECT_STABLE);
        assert!(b.ready(t0 + RECONNECT_BACKOFF_MIN));
    }

    #[test]
    fn reconnect_backoff_resets_after_stable_session() {
        let t0 = Instant::now();
        let mut b = Backoff::default();
        b.attempted(t0);
        b.failed();
        b.failed();
        b.session_ended(RECONNECT_STABLE - secs(1));
        assert!(!b.ready(t0 + RECONNECT_BACKOFF_MIN * 4 - secs(1)));
        b.session_ended(RECONNECT_STABLE);
        assert_eq!(b, Backoff::default());
    }

    /// Records calls and log lines; `results` scripts `connect`.
    #[derive(Default)]
    struct Fake {
        already_connected: AtomicBool,
        results: Mutex<VecDeque<Result<(), String>>>,
        connects: Mutex<Vec<Instant>>,
        /// Per connect: the watcher held the shared guard.
        guarded: Mutex<Vec<bool>>,
        lines: Mutex<Vec<String>>,
        settings: Mutex<Option<Settings>>,
        /// How long each `connect` takes.
        delay: Mutex<Duration>,
    }

    impl Connector for Arc<Fake> {
        async fn connected(&self, _: &str) -> bool {
            self.already_connected.load(Ordering::SeqCst)
        }

        async fn connect(&self, mac: &str) -> Result<(), String> {
            self.connects.lock().unwrap().push(now());
            let guarded = ConnectGuard::try_new(mac).is_none();
            self.guarded.lock().unwrap().push(guarded);
            let delay = *self.delay.lock().unwrap();
            tokio::time::sleep(delay).await;
            self.results.lock().unwrap().pop_front().unwrap_or(Ok(()))
        }

        fn settings(&self, _: &str) -> Settings {
            self.settings.lock().unwrap().unwrap_or_else(settings)
        }

        fn log(&self, _: &str, line: &str) {
            self.lines.lock().unwrap().push(line.to_string());
        }
    }

    impl Fake {
        fn connects(&self) -> usize {
            self.connects.lock().unwrap().len()
        }

        fn lines(&self, prefix: &str) -> usize {
            let lines = self.lines.lock().unwrap();
            lines.iter().filter(|l| l.starts_with(prefix)).count()
        }
    }

    impl Samples for mpsc::Receiver<u32> {
        async fn next(&mut self) -> Option<u32> {
            self.recv().await
        }
    }

    /// A watcher for `mac` after a drop with `reason`, fed by `sample`.
    struct Rig {
        tx: mpsc::Sender<u32>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Rig {
        fn start(mac: &str, fake: Arc<Fake>) -> Self {
            let (tx, rx) = mpsc::channel(1);
            let task = tokio::spawn(watch(mac.to_string(), rx, fake));
            Rig { tx, task }
        }

        /// One sample, processed before this returns.
        async fn sample(&self, playing: bool) {
            let category = if playing {
                AUDIO_CATEGORY_MEDIA
            } else {
                AUDIO_CATEGORY_IDLE
            };
            let _ = self.tx.send(category).await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        /// A stopped-then-playing edge.
        async fn play_edge(&self) {
            self.sample(false).await;
            self.sample(true).await;
        }
    }

    fn host_driven_drop(mac: &str) {
        record_drop(mac, now());
        disconnect_reason(mac, Some(REMOTE));
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_watcher_play_inside_window_not_queued() {
        let mac = "7E:00:00:00:00:01";
        host_driven_drop(mac);
        let fake = Arc::new(Fake::default());
        fake.results
            .lock()
            .unwrap()
            .push_back(Err("busy".to_string()));
        let rig = Rig::start(mac, fake.clone());
        rig.sample(false).await;
        rig.sample(true).await;
        assert_eq!(fake.connects(), 1);
        assert_eq!(fake.lines("reconnect failed"), 1);
        // Plays inside the doubled window start nothing, and none is queued.
        let t0 = now();
        rig.play_edge().await;
        tokio::time::sleep(RECONNECT_BACKOFF_MIN).await;
        rig.play_edge().await;
        assert_eq!(fake.connects(), 1);
        assert_eq!(fake.lines("new playback inside the reconnect backoff"), 2);
        tokio::time::sleep(RECONNECT_BACKOFF_MIN * 2).await;
        assert_eq!(fake.connects(), 1);
        // The next play edge after the window does.
        rig.play_edge().await;
        assert_eq!(fake.connects(), 2);
        assert!(fake.connects.lock().unwrap()[1] >= t0 + RECONNECT_BACKOFF_MIN * 2);
        rig.sample(false).await;
        assert!(rig.task.is_finished());
        assert_eq!(take_marker(mac), None);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_watcher_consecutive_failures_double_then_cap() {
        let mac = "7E:00:00:00:00:02";
        host_driven_drop(mac);
        let fake = Arc::new(Fake::default());
        for _ in 0..8 {
            fake.results
                .lock()
                .unwrap()
                .push_back(Err("failed".to_string()));
        }
        let rig = Rig::start(mac, fake.clone());
        rig.sample(true).await;
        // Play edges every second for 20 minutes.
        for _ in 0..1200 {
            rig.play_edge().await;
            tokio::time::sleep(Duration::from_millis(998)).await;
        }
        let times = fake.connects.lock().unwrap().clone();
        let gaps: Vec<u64> = times.windows(2).map(|w| (w[1] - w[0]).as_secs()).collect();
        // Doubling from the min, then held at the max: each attempt is the
        // first play edge once the window ends.
        let mut delay = RECONNECT_BACKOFF_MIN;
        let expected: Vec<u64> = gaps
            .iter()
            .map(|_| {
                delay = (delay * 2).min(RECONNECT_BACKOFF_MAX);
                delay.as_secs()
            })
            .collect();
        assert_eq!(gaps, expected);
        assert!(gaps.len() >= 6, "{gaps:?}");
        assert_eq!(gaps.last(), Some(&RECONNECT_BACKOFF_MAX.as_secs()));
        assert_eq!(fake.lines("reconnect failed"), times.len());
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_watcher_drop_after_reconnect_within_stable_doubles() {
        let mac = "7E:00:00:00:00:03";
        host_driven_drop(mac);
        let fake = Arc::new(Fake::default());
        let rig = Rig::start(mac, fake.clone());
        rig.sample(true).await;
        rig.play_edge().await;
        assert_eq!(fake.connects(), 1);
        let t0 = fake.connects.lock().unwrap()[0];
        device_connected(mac);
        // Its playback stops; the watcher ends.
        rig.sample(false).await;
        assert!(rig.task.is_finished());

        // Another host-driven drop 5 s later.
        tokio::time::sleep(Duration::from_secs(5)).await;
        host_driven_drop(mac);
        let rig = Rig::start(mac, fake.clone());
        rig.sample(false).await;
        tokio::time::sleep_until((t0 + RECONNECT_BACKOFF_MIN * 2 - secs(2)).into()).await;
        rig.sample(true).await;
        assert_eq!(fake.connects(), 1);
        tokio::time::sleep(secs(2)).await;
        rig.play_edge().await;
        assert_eq!(fake.connects(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_watcher_stable_session_resets() {
        let mac = "7E:00:00:00:00:04";
        host_driven_drop(mac);
        let fake = Arc::new(Fake::default());
        fake.results
            .lock()
            .unwrap()
            .push_back(Err("failed".to_string()));
        let rig = Rig::start(mac, fake.clone());
        rig.sample(false).await;
        rig.sample(true).await;
        tokio::time::sleep(RECONNECT_BACKOFF_MIN * 2).await;
        rig.play_edge().await;
        assert_eq!(fake.connects(), 2);
        device_connected(mac);
        rig.sample(false).await;
        assert!(rig.task.is_finished());

        // Up for RECONNECT_STABLE, then dropped by a host: back to no wait.
        tokio::time::sleep(RECONNECT_STABLE).await;
        host_driven_drop(mac);
        let rig = Rig::start(mac, fake.clone());
        rig.play_edge().await;
        assert_eq!(fake.connects(), 3);
        assert_eq!(with_entry(mac, |e| e.backoff.delay), RECONNECT_BACKOFF_MIN);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_watcher_already_connected_makes_no_call() {
        let mac = "7E:00:00:00:00:05";
        host_driven_drop(mac);
        let fake = Arc::new(Fake::default());
        fake.already_connected.store(true, Ordering::SeqCst);
        let rig = Rig::start(mac, fake.clone());
        rig.play_edge().await;
        assert_eq!(fake.connects(), 0);
        assert_eq!(fake.lines("reconnected for new playback"), 1);
        // No Connected=true follows to start a task: no marker is left.
        assert_eq!(take_marker(mac), None);
        // No further attempt, however many plays follow.
        tokio::time::sleep(RECONNECT_BACKOFF_MAX).await;
        rig.sample(true).await;
        assert!(rig.task.is_finished());
        rig.play_edge().await;
        assert_eq!(fake.connects(), 0);
        assert_eq!(take_marker(mac), None);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_watcher_skips_while_le_connects() {
        let mac = "7E:00:00:00:00:06";
        host_driven_drop(mac);
        let fake = Arc::new(Fake::default());
        let rig = Rig::start(mac, fake.clone());
        // le.rs holds the guard (it normalizes the case too).
        let le = ConnectGuard::try_new(&mac.to_lowercase()).expect("free");
        rig.play_edge().await;
        assert_eq!(fake.connects(), 0);
        assert_eq!(fake.lines("le.rs is already connecting"), 1);
        drop(le);
        // A skip spends no backoff: the next edge connects at once, holding
        // the guard le.rs skips on, and releases it after.
        rig.play_edge().await;
        assert_eq!(fake.connects(), 1);
        assert_eq!(*fake.guarded.lock().unwrap(), [true]);
        assert!(ConnectGuard::try_new(mac).is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_watcher_marker_for_playing_reconnect() {
        let mac = "7E:00:00:00:00:07";
        host_driven_drop(mac);
        let fake = Arc::new(Fake::default());
        let rig = Rig::start(mac, fake.clone());
        rig.play_edge().await;
        device_connected(mac);
        rig.sample(true).await;
        assert!(!rig.task.is_finished());
        assert_eq!(take_marker(mac), Some(AUDIO_CATEGORY_MEDIA));
        // Consumed once.
        assert_eq!(take_marker(mac), None);
        rig.sample(true).await;
        assert!(rig.task.is_finished());

        // A stop before the task starts clears it.
        let mac = "7E:00:00:00:00:08";
        host_driven_drop(mac);
        let rig = Rig::start(mac, fake.clone());
        rig.play_edge().await;
        rig.sample(false).await;
        assert!(rig.task.is_finished());
        assert_eq!(take_marker(mac), None);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_watcher_stops_on_connected_or_ruled_out_drop() {
        let fake = Arc::new(Fake::default());
        let mac = "7E:00:00:00:00:09";
        host_driven_drop(mac);
        let rig = Rig::start(mac, fake.clone());
        rig.sample(false).await;
        device_connected(mac);
        rig.sample(true).await;
        assert!(rig.task.is_finished());

        for (mac, reason, closed) in [
            ("7E:00:00:00:00:0A", Some("org.bluez.Reason.Local"), false),
            ("7E:00:00:00:00:0B", Some(REMOTE), true),
        ] {
            record_drop(mac, now());
            case_closed_at_drop(mac, closed);
            let rig = Rig::start(mac, fake.clone());
            rig.sample(false).await;
            // Either signal order.
            disconnect_reason(mac, reason);
            rig.sample(true).await;
            assert!(rig.task.is_finished(), "{mac}");
        }
        assert_eq!(fake.connects(), 0);
        // One line per ruled-out drop; none for Connected=true.
        assert_eq!(fake.lines("not reconnecting"), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_watcher_stops_when_mode_leaves_handoff() {
        for (mac, mode) in [
            ("7E:00:00:00:00:11", SwitchingMode::Off),
            ("7E:00:00:00:00:12", SwitchingMode::Legacy),
        ] {
            host_driven_drop(mac);
            let fake = Arc::new(Fake::default());
            let rig = Rig::start(mac, fake.clone());
            rig.sample(true).await;
            assert!(!rig.task.is_finished(), "{mac}");
            *fake.settings.lock().unwrap() = Some(Settings { mode, ..settings() });
            // The same playback goes on: no edge, yet the watcher ends.
            rig.sample(true).await;
            assert!(rig.task.is_finished(), "{mac}");
            assert_eq!(
                fake.lines("not reconnecting: switching mode is not handoff"),
                1
            );
            assert_eq!(fake.connects(), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_backoff_duplicate_drop_signals_count_once() {
        let mac = "7E:00:00:00:00:13";
        let delay = || with_entry(mac, |e| e.backoff.delay);
        // Reason first, then the drop, each signalled twice.
        with_entry(mac, |e| e.backoff.attempted(now()));
        device_connected(mac);
        disconnect_reason(mac, Some(REMOTE));
        disconnect_reason(mac, Some(REMOTE));
        assert!(record_drop(mac, now()));
        assert!(!record_drop(mac, now()));
        assert_eq!(delay(), RECONNECT_BACKOFF_MIN * 2);
        // The drop first: a repeat neither counts nor clears the marker of a
        // connect still running (nor restarts its watcher).
        with_entry(mac, |e| e.backoff.attempted(now()));
        device_connected(mac);
        record_drop(mac, now());
        set_marker(mac, AUDIO_CATEGORY_MEDIA);
        disconnect_reason(mac, Some(REMOTE));
        assert!(!record_drop(mac, now()));
        disconnect_reason(mac, Some(REMOTE));
        assert_eq!(delay(), RECONNECT_BACKOFF_MIN * 4);
        assert_eq!(take_marker(mac), Some(AUDIO_CATEGORY_MEDIA));
    }

    #[test]
    fn reconnect_mac_from_path_device_and_adapter() {
        assert_eq!(
            mac_from_path("/org/bluez/hci0/dev_AA_BB_CC_DD_EE_FF").as_deref(),
            Some("AA:BB:CC:DD:EE:FF")
        );
        assert_eq!(mac_from_path("/org/bluez/hci0"), None);
    }

    fn has_entry(mac: &str) -> bool {
        ENTRIES.lock().unwrap().contains_key(mac)
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_connected_if_dropped_resets_a_drop() {
        let mac = "7E:00:00:00:04:08";
        assert!(record_drop(mac, now()));
        assert!(!record_drop(mac, now()));
        device_connected_if_dropped(mac);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(record_drop(mac, now()));
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_connected_if_dropped_leaves_others_alone() {
        let mac = "7E:00:00:00:04:09";
        device_connected_if_dropped(mac);
        assert!(!has_entry(mac));
        // A reason alone (any device gets one) has no drop recorded.
        let mac = "7E:00:00:00:04:0A";
        device_connected(mac);
        disconnect_reason(mac, Some(REMOTE));
        let state = || with_entry(mac, |e| (e.up_since, e.dropped_at, e.host_driven));
        let before = state();
        tokio::time::advance(Duration::from_secs(1)).await;
        device_connected_if_dropped(mac);
        assert_eq!(state(), before);
        assert_eq!(before.1, None);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_watcher_waits_and_logs_without_handoff() {
        let mac = "7E:00:00:00:00:0C";
        host_driven_drop(mac);
        let fake = Arc::new(Fake::default());
        *fake.settings.lock().unwrap() = Some(Settings {
            auto_connect: false,
            ..settings()
        });
        let rig = Rig::start(mac, fake.clone());
        rig.play_edge().await;
        assert_eq!(fake.lines("not reconnecting: autoConnect is off"), 1);
        *fake.settings.lock().unwrap() = Some(Settings {
            mode: SwitchingMode::Legacy,
            ..settings()
        });
        rig.play_edge().await;
        assert!(rig.task.is_finished());
        assert_eq!(fake.connects(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_watcher_slow_failure_drops_plays_while_connecting() {
        let mac = "7E:00:00:00:00:0F";
        host_driven_drop(mac);
        let fake = Arc::new(Fake::default());
        *fake.delay.lock().unwrap() = CONNECT_TIMEOUT;
        fake.results
            .lock()
            .unwrap()
            .push_back(Err("timed out".to_string()));
        let rig = Rig::start(mac, fake.clone());
        rig.play_edge().await;
        assert_eq!(fake.connects(), 1);
        // A stop and a play while the connect hangs, then it fails.
        tokio::time::sleep(Duration::from_millis(500)).await;
        rig.play_edge().await;
        tokio::time::sleep(CONNECT_TIMEOUT).await;
        assert_eq!(fake.lines("reconnect failed"), 1);
        assert_eq!(take_marker(mac), None);
        // That play is no edge after the attempt: still playing starts none.
        rig.sample(true).await;
        tokio::time::sleep(RECONNECT_BACKOFF_MAX).await;
        rig.sample(true).await;
        assert_eq!(fake.connects(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_watcher_stop_while_connecting_clears_marker() {
        let mac = "7E:00:00:00:00:10";
        host_driven_drop(mac);
        let fake = Arc::new(Fake::default());
        *fake.delay.lock().unwrap() = Duration::from_secs(5);
        let rig = Rig::start(mac, fake.clone());
        rig.play_edge().await;
        // The handoff task may start before the connect returns.
        assert!(claim_marker(mac));
        rig.sample(false).await;
        assert_eq!(
            fake.lines("playback stopped before the handoff task took"),
            1
        );
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(fake.lines("reconnected for new playback"), 1);
        assert_eq!(take_marker(mac), None);
        rig.sample(true).await;
        assert!(rig.task.is_finished());
    }

    /// Ten minutes after a host-driven drop with no new play edge.
    async fn no_timer(mac: &'static str, playing: bool) {
        host_driven_drop(mac);
        let fake = Arc::new(Fake::default());
        let rig = Rig::start(mac, fake.clone());
        for _ in 0..1200 {
            rig.sample(playing).await;
            tokio::time::sleep(switching::PLAYBACK_POLL).await;
        }
        assert_eq!(fake.connects(), 0);
        assert!(!rig.task.is_finished());
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_watcher_no_timer_idle() {
        no_timer("7E:00:00:00:00:0D", false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_watcher_no_timer_playing_from_drop() {
        no_timer("7E:00:00:00:00:0E", true).await;
    }
}
