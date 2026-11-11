//! Linux's activity level, sent as the tipi's nearbyAudioScore (#13).
//!
//! The rules and scores copy the Mac's own tipi (spike #258 q1): input
//! within 5 s is Max, within 25 s High, longer while unlocked Low, and
//! locked or idle Idle. Playback never raises the level.

use crate::bluetooth::aacp::TipiFields;
use dbus::blocking::Connection;
use dbus::blocking::stdintf::org_freedesktop_dbus::Properties;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Input at most this long ago is Max.
pub(crate) const ACTIVITY_MAX: Duration = Duration::from_secs(5);
/// Input at most this long ago (and more than `ACTIVITY_MAX`) is High.
pub(crate) const ACTIVITY_HIGH: Duration = Duration::from_secs(25);
/// Each logind property read's D-Bus timeout.
const LOGIND_CALL_TIMEOUT: Duration = Duration::from_millis(200);
/// How long an announcement waits for a snapshot before it sends LEGACY:
/// opening the system bus has no timeout of its own.
const SNAPSHOT_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ActivityLevel {
    Idle,
    Low,
    High,
    Max,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActivityInputs {
    pub(crate) locked: bool,
    pub(crate) idle: bool,
    /// Time since the last user input; None when the source can't tell.
    pub(crate) since_input: Option<Duration>,
}

/// The Mac's Activity State. ASSUMPTION (until #508/#510): locked is Idle
/// even while playing, whatever `since_input` is.
pub(crate) fn activity_level(inputs: ActivityInputs) -> ActivityLevel {
    if inputs.locked || inputs.idle {
        return ActivityLevel::Idle;
    }
    match inputs.since_input {
        Some(since) if since <= ACTIVITY_MAX => ActivityLevel::Max,
        Some(since) if since > ACTIVITY_HIGH => ActivityLevel::Low,
        _ => ActivityLevel::High,
    }
}

const SCORE_IDLE: u32 = 1;
const SCORE_LOW: u32 = 2;
const SCORE_HIGH: u32 = 6;
const SCORE_MAX: u32 = 7;

/// The Mac's nearbyAudioScore per level (spike #258 q1, Mac sent-side log
/// and btmon). ASSUMPTION (until #508/#510): idleTime is 0 at every level.
pub(crate) fn tipi_fields(level: ActivityLevel) -> TipiFields {
    let nearby_audio_score = match level {
        ActivityLevel::Idle => SCORE_IDLE,
        ActivityLevel::Low => SCORE_LOW,
        ActivityLevel::High => SCORE_HIGH,
        ActivityLevel::Max => SCORE_MAX,
    };
    TipiFields {
        idle_time: 0,
        nearby_audio_score,
    }
}

/// Whether the Mac re-sends its tipi on this change (spike #258): its
/// Activity State group, Idle, Low or {High, Max}, changed.
pub(crate) fn resend_tipi(prev: ActivityLevel, next: ActivityLevel) -> bool {
    let group = |level| match level {
        ActivityLevel::Max => ActivityLevel::High,
        level => level,
    };
    group(prev) != group(next)
}

/// Where the activity level's inputs come from. None when they can't be
/// read; the announcement then sends `TipiFields::LEGACY`.
pub(crate) trait ActivitySource: Send + Sync {
    /// Blocking; `current_tipi` runs it off the async runtime.
    fn snapshot(&self) -> Option<ActivityInputs>;
}

impl<F: Fn() -> Option<ActivityInputs> + Send + Sync> ActivitySource for F {
    fn snapshot(&self) -> Option<ActivityInputs> {
        self()
    }
}

/// The tipi values for `source`'s snapshot, or LEGACY when it fails or
/// takes longer than `SNAPSHOT_TIMEOUT`. The caller holds no lock.
pub(crate) async fn current_tipi(source: &Arc<dyn ActivitySource>) -> TipiFields {
    tokio::time::timeout(SNAPSHOT_TIMEOUT, current_level(source))
        .await
        .ok()
        .flatten()
        .map_or(TipiFields::LEGACY, tipi_fields)
}

/// `source`'s level, or None when its snapshot fails. Waits as long as the
/// snapshot blocks, so a caller that awaits it never overlaps two.
pub(crate) async fn current_level(source: &Arc<dyn ActivitySource>) -> Option<ActivityLevel> {
    let source = source.clone();
    let snapshot = tokio::task::spawn_blocking(move || source.snapshot()).await;
    snapshot.ok().flatten().map(activity_level)
}

/// logind's session hints. A user service has no session of its own
/// (`session/self` fails), so this reads `session/auto`, the user's
/// display session.
pub(crate) struct LogindActivity;

/// The logind reads' one system bus connection, so the 1 s poll doesn't
/// open a bus per snapshot (#525).
static LOGIND_BUS: std::sync::Mutex<Option<Connection>> = std::sync::Mutex::new(None);

impl ActivitySource for LogindActivity {
    fn snapshot(&self) -> Option<ActivityInputs> {
        logind_snapshot(&LOGIND_BUS, || Connection::new_system().ok(), read_hints)
    }
}

/// `session/auto`'s (LockedHint, IdleHint) on `bus`.
fn read_hints(bus: &Connection) -> Option<(bool, bool)> {
    let session = bus.with_proxy(
        "org.freedesktop.login1",
        "/org/freedesktop/login1/session/auto",
        LOGIND_CALL_TIMEOUT,
    );
    let hint = |name| session.get::<bool>("org.freedesktop.login1.Session", name);
    Some((hint("LockedHint").ok()?, hint("IdleHint").ok()?))
}

/// logind's inputs from `hints` on the bus in `slot`, shared as
/// `read_shared_any` does: opened once, reopened after a failed read, and
/// None at once while another snapshot holds the bus (LEGACY is sent).
fn logind_snapshot<B>(
    slot: &std::sync::Mutex<Option<B>>,
    open: impl FnOnce() -> Option<B>,
    hints: impl FnOnce(&B) -> Option<(bool, bool)>,
) -> Option<ActivityInputs> {
    let (locked, idle) = crate::a2dp_resume::read_shared_any(slot, open, hints)?;
    Some(logind_inputs(locked, idle))
}

/// logind gives no input time, so an unlocked, non-idle session is High.
/// hypridle/hyprlock/Hyprland never set these hints (spike #160), so on
/// Hyprland this is always High until #511.
pub(crate) fn logind_inputs(locked: bool, idle: bool) -> ActivityInputs {
    ActivityInputs {
        locked,
        idle,
        since_input: None,
    }
}

/// The process's activity source, chosen once at startup (#511).
static SESSION_SOURCE: OnceLock<Arc<dyn ActivitySource>> = OnceLock::new();

/// Sets the session source; only the first call counts.
pub(crate) fn set_session_source(source: Arc<dyn ActivitySource>) {
    let _ = SESSION_SOURCE.set(source);
}

/// The source `main` set, or logind when none was set.
pub(crate) fn session_source() -> Arc<dyn ActivitySource> {
    SESSION_SOURCE
        .get()
        .cloned()
        .unwrap_or_else(|| Arc::new(LogindActivity))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC: Duration = Duration::from_secs(1);

    fn unlocked(since_input: Option<Duration>) -> ActivityLevel {
        activity_level(ActivityInputs {
            locked: false,
            idle: false,
            since_input,
        })
    }

    #[test]
    fn level_max_within_5s() {
        for since in [Duration::ZERO, 2 * SEC, ACTIVITY_MAX] {
            assert_eq!(unlocked(Some(since)), ActivityLevel::Max, "{since:?}");
        }
    }

    #[test]
    fn level_high_just_past_5s() {
        let since = ACTIVITY_MAX + Duration::from_millis(1);
        assert_eq!(unlocked(Some(since)), ActivityLevel::High);
    }

    #[test]
    fn level_high_at_25s() {
        assert_eq!(unlocked(Some(ACTIVITY_HIGH)), ActivityLevel::High);
    }

    #[test]
    fn level_low_just_past_25s() {
        let since = ACTIVITY_HIGH + Duration::from_millis(1);
        assert_eq!(unlocked(Some(since)), ActivityLevel::Low);
        assert_eq!(unlocked(Some(100 * SEC)), ActivityLevel::Low);
    }

    #[test]
    fn level_high_when_since_input_unknown() {
        assert_eq!(unlocked(None), ActivityLevel::High);
    }

    #[test]
    fn level_idle_when_idle_hint() {
        for since in [None, Some(Duration::ZERO), Some(100 * SEC)] {
            let inputs = ActivityInputs {
                locked: false,
                idle: true,
                since_input: since,
            };
            assert_eq!(activity_level(inputs), ActivityLevel::Idle, "{since:?}");
        }
    }

    // ASSUMPTION (until #508/#510): there is no playing input, so a locked
    // session that plays is Idle too.
    #[test]
    fn locked_playing_is_idle() {
        for since in [None, Some(Duration::ZERO), Some(2 * SEC), Some(100 * SEC)] {
            let inputs = ActivityInputs {
                locked: true,
                idle: false,
                since_input: since,
            };
            assert_eq!(activity_level(inputs), ActivityLevel::Idle, "{since:?}");
        }
    }

    #[test]
    fn level_monotonic() {
        let millis = [0, 1, 4_999, 5_000, 5_001, 24_999, 25_000, 25_001, 100_000];
        let mut since: Vec<_> = millis
            .iter()
            .map(|&ms| Some(Duration::from_millis(ms)))
            .collect();
        since.push(None);
        for &a in &since {
            for &b in &since {
                for (locked, idle) in [(false, false), (false, true), (true, false), (true, true)] {
                    let level = |since_input, locked, idle| {
                        activity_level(ActivityInputs {
                            locked,
                            idle,
                            since_input,
                        })
                    };
                    let before = level(a, locked, idle);
                    // Clearing locked or idle never lowers the level.
                    assert!(level(a, false, idle) >= before);
                    assert!(level(a, locked, false) >= before);
                    // Less time since input never lowers it.
                    if let (Some(a), Some(b)) = (a, b)
                        && b < a
                    {
                        assert!(level(Some(b), locked, idle) >= before, "{b:?} < {a:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn tipi_fields_scores() {
        for (level, score) in [
            (ActivityLevel::Idle, 1),
            (ActivityLevel::Low, 2),
            (ActivityLevel::High, 6),
            (ActivityLevel::Max, 7),
        ] {
            let fields = tipi_fields(level);
            assert_eq!(fields.nearby_audio_score, score, "{level:?}");
            assert_eq!(fields.idle_time, 0, "{level:?}");
        }
    }

    #[test]
    fn tipi_fields_scores_strictly_increase() {
        use ActivityLevel::*;
        let scores: Vec<_> = [Idle, Low, High, Max]
            .into_iter()
            .map(|level| tipi_fields(level).nearby_audio_score)
            .collect();
        assert!(scores.windows(2).all(|w| w[0] < w[1]), "{scores:?}");
    }

    #[test]
    fn resend_tipi_all_pairs() {
        use ActivityLevel::*;
        let levels = [Idle, Low, High, Max];
        // Rows: prev; columns: next, in `levels` order.
        let table = [
            [false, true, true, true],
            [true, false, true, true],
            [true, true, false, false],
            [true, true, false, false],
        ];
        for (prev, row) in levels.into_iter().zip(table) {
            for (next, resend) in levels.into_iter().zip(row) {
                assert_eq!(resend_tipi(prev, next), resend, "{prev:?} -> {next:?}");
            }
        }
    }

    #[test]
    fn logind_hints_unlocked_is_high() {
        let fields = tipi_fields(activity_level(logind_inputs(false, false)));
        assert_eq!(fields.nearby_audio_score, 6);
    }

    #[test]
    fn logind_hints_locked_is_idle() {
        let fields = tipi_fields(activity_level(logind_inputs(true, false)));
        assert_eq!(fields.nearby_audio_score, 1);
    }

    /// A `logind_snapshot` opener that counts its calls and opens bus `n`.
    fn logind_opener(opens: &std::cell::Cell<usize>, n: u32) -> impl FnOnce() -> Option<u32> {
        move || {
            opens.set(opens.get() + 1);
            Some(n)
        }
    }

    /// Fake hints: locked only on bus `n`, so a result shows which bus read.
    fn locked_on(n: u32) -> impl Fn(&u32) -> Option<(bool, bool)> {
        move |bus| Some((*bus == n, true))
    }

    #[test]
    fn logind_bus_opens_once() {
        let (slot, opens) = (std::sync::Mutex::new(None), std::cell::Cell::new(0));
        for n in 1..=3 {
            assert_eq!(
                logind_snapshot(&slot, logind_opener(&opens, n), locked_on(1)),
                Some(logind_inputs(true, true)),
                "snapshot {n}"
            );
        }
        assert_eq!(opens.get(), 1);
    }

    #[test]
    fn logind_bus_reopens_after_failed_read() {
        let (slot, opens) = (std::sync::Mutex::new(None), std::cell::Cell::new(0));
        assert_eq!(
            logind_snapshot(&slot, logind_opener(&opens, 1), |_| None),
            None
        );
        assert_eq!(opens.get(), 1);
        for n in [2, 3] {
            assert_eq!(
                logind_snapshot(&slot, logind_opener(&opens, n), locked_on(2)),
                Some(logind_inputs(true, true)),
                "snapshot after the failed read on bus {n}"
            );
        }
        assert_eq!(opens.get(), 2);
    }

    /// Another snapshot holding the bus (a stalled read) gives None at
    /// once, without opening a second bus.
    #[test]
    fn logind_busy_bus_is_none() {
        let (slot, opens) = (std::sync::Mutex::new(None), std::cell::Cell::new(0));
        let _held = slot.lock().unwrap();
        assert_eq!(
            logind_snapshot(&slot, logind_opener(&opens, 1), locked_on(1)),
            None
        );
        assert_eq!(opens.get(), 0);
    }

    /// A source stuck in its blocking read (a stalled system bus) gives
    /// LEGACY once `SNAPSHOT_TIMEOUT` passes, not when the read ends.
    #[tokio::test]
    async fn current_tipi_stuck_source_sends_legacy() {
        let (gate, stuck) = std::sync::mpsc::channel::<()>();
        let stuck = std::sync::Mutex::new(stuck);
        let source: Arc<dyn ActivitySource> = Arc::new(move || {
            let _ = stuck.lock().unwrap().recv();
            Some(logind_inputs(true, false))
        });
        let fields = tokio::time::timeout(10 * SNAPSHOT_TIMEOUT, current_tipi(&source))
            .await
            .expect("current_tipi must not wait for a stuck source");
        assert_eq!(fields, TipiFields::LEGACY);
        drop(gate);
    }
}
