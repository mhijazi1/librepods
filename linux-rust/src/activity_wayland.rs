//! The activity level's inputs from the Wayland compositor (#511): on
//! Hyprland, logind's IdleHint/LockedHint never change (spike #160).
//!
//! ext-idle-notify-v1 at `ACTIVITY_MAX` and `ACTIVITY_HIGH` gives input
//! recency and hyprland-lock-notify-v1 the lock state (logind's LockedHint
//! where the compositor lacks it, #526). One named thread does all Wayland
//! I/O; `snapshot()` only reads the state it keeps.

use crate::activity::{
    ACTIVITY_HIGH, ACTIVITY_MAX, ActivityInputs, ActivitySource, LogindActivity,
};
use log::{info, warn};
use std::env;
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use wayland_client::globals::{GlobalListContents, registry_queue_init};
use wayland_client::protocol::{wl_registry::WlRegistry, wl_seat::WlSeat};
use wayland_client::{Connection, Dispatch, QueueHandle, delegate_noop};
use wayland_protocols::ext::idle_notify::v1::client::{
    ext_idle_notification_v1::{self, ExtIdleNotificationV1},
    ext_idle_notifier_v1::ExtIdleNotifierV1,
};

#[allow(
    dead_code,
    non_camel_case_types,
    non_upper_case_globals,
    unused_imports
)]
mod lock_notify {
    use wayland_client;
    pub mod __interfaces {
        use wayland_client::backend as wayland_backend;
        wayland_scanner::generate_interfaces!("protocols/hyprland-lock-notify-v1.xml");
    }
    use self::__interfaces::*;
    wayland_scanner::generate_client_code!("protocols/hyprland-lock-notify-v1.xml");
}
use lock_notify::{
    hyprland_lock_notification_v1::{self, HyprlandLockNotificationV1},
    hyprland_lock_notifier_v1::HyprlandLockNotifierV1,
};

/// The idle notifications' timeouts; each one's index is its user data.
const IDLE_TIMEOUTS: [Duration; 2] = [ACTIVITY_MAX, ACTIVITY_HIGH];
/// How long `start()` waits for the Wayland thread's selection.
const SELECT_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, Default)]
struct WaylandState {
    /// When each `IDLE_TIMEOUTS` notification idled; None until it does and
    /// after a resume.
    idled: [Option<Instant>; 2],
    locked: bool,
    /// The connection dropped: `snapshot()` gives None from then on.
    lost: bool,
}

impl WaylandState {
    fn idled(&mut self, notification: usize, at: Instant) {
        self.idled[notification] = Some(at);
    }

    /// Either notification resuming means input, so both clear.
    fn resumed(&mut self) {
        self.idled = [None; 2];
    }

    fn inputs(&self, now: Instant) -> ActivityInputs {
        let since_input = self
            .idled
            .iter()
            .zip(IDLE_TIMEOUTS)
            .filter_map(|(at, timeout)| Some(now.saturating_duration_since((*at)?) + timeout))
            .max()
            .unwrap_or(Duration::ZERO);
        ActivityInputs {
            locked: self.locked,
            idle: false,
            since_input: Some(since_input),
        }
    }
}

struct WaylandActivity(Arc<Mutex<WaylandState>>);

impl ActivitySource for WaylandActivity {
    fn snapshot(&self) -> Option<ActivityInputs> {
        let state = self.0.lock().unwrap();
        (!state.lost).then(|| state.inputs(Instant::now()))
    }
}

/// `state`'s source. Without hyprland-lock-notify-v1, `locked` comes from
/// `logind_locked` (`read_lock_hint`'s result: busy is no snapshot, a failed
/// read is unlocked), read after the state mutex is released so a slow bus
/// never stalls the Wayland thread (#526).
fn wayland_source(
    state: Arc<Mutex<WaylandState>>,
    lock_notify: bool,
    logind_locked: impl Fn() -> Option<Option<bool>> + Send + Sync + 'static,
) -> Arc<dyn ActivitySource> {
    let wayland = WaylandActivity(state);
    if lock_notify {
        return Arc::new(wayland);
    }
    Arc::new(move || {
        let inputs = wayland.snapshot()?;
        Some(ActivityInputs {
            locked: logind_locked()?.unwrap_or(false),
            ..inputs
        })
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Selection {
    /// Use logind; the reason goes in the log.
    Logind(&'static str),
    Wayland {
        /// ext_idle_notifier_v1 v2: input notifications, which ignore idle
        /// inhibitors.
        input_idle: bool,
        lock_notify: bool,
    },
}

fn select_source(display: bool, connected: bool, globals: &[(&str, u32)]) -> Selection {
    let version = |name| globals.iter().find(|g| g.0 == name).map(|g| g.1);
    if !display {
        return Selection::Logind("no WAYLAND_DISPLAY, or WAYLAND_SOCKET is set");
    }
    if !connected {
        return Selection::Logind("the Wayland connection failed");
    }
    let (Some(idle), Some(_)) = (version("ext_idle_notifier_v1"), version("wl_seat")) else {
        return Selection::Logind("the compositor has no ext_idle_notifier_v1 or wl_seat");
    };
    Selection::Wayland {
        input_idle: idle >= 2,
        lock_notify: version("hyprland_lock_notifier_v1").is_some(),
    }
}

/// Starts the Wayland thread and returns its source, or logind (with one
/// info line) when the compositor can't give the inputs within
/// `SELECT_TIMEOUT`.
pub(crate) fn start() -> Arc<dyn ActivitySource> {
    let state = Arc::new(Mutex::new(WaylandState::default()));
    // Rendezvous: the thread's send succeeds only if this side took it.
    let (tx, rx) = mpsc::sync_channel(0);
    let thread_state = state.clone();
    let spawned = thread::Builder::new()
        .name("wayland-activity".into())
        .spawn(move || run(tx, thread_state));
    let selection = match spawned {
        Ok(_) => rx
            .recv_timeout(SELECT_TIMEOUT)
            .unwrap_or(Selection::Logind("the Wayland thread gave no answer")),
        Err(_) => Selection::Logind("the Wayland thread failed to start"),
    };
    match selection {
        Selection::Logind(reason) => {
            info!("Activity: {reason}; using logind's session hints");
            Arc::new(LogindActivity)
        }
        Selection::Wayland {
            input_idle,
            lock_notify,
        } => {
            if input_idle {
                info!("Activity: Wayland source, ext-idle-notify v2 input notifications");
            } else {
                info!(
                    "Activity: Wayland source, ext-idle-notify v1 idle notifications; idle inhibitors (e.g. video playback) now keep the level at Max"
                );
            }
            if !lock_notify {
                warn!(
                    "Activity: the compositor has no hyprland_lock_notifier_v1; the lock state comes from logind's LockedHint"
                );
            }
            let logind = LogindActivity;
            wayland_source(state, lock_notify, move || logind.locked())
        }
    }
}

/// The Wayland thread: connects, selects, binds, then dispatches events into
/// `state` until the connection drops.
fn run(tx: SyncSender<Selection>, state: Arc<Mutex<WaylandState>>) {
    // connect_to_env would take a WAYLAND_SOCKET fd meant for the UI and
    // remove_var it while other threads run.
    let display =
        env::var_os("WAYLAND_DISPLAY").is_some() && env::var_os("WAYLAND_SOCKET").is_none();
    let conn = display
        .then(Connection::connect_to_env)
        .and_then(Result::ok);
    let init = conn
        .as_ref()
        .and_then(|conn| registry_queue_init::<Handler>(conn).ok());
    let advertised = init
        .as_ref()
        .map_or_else(Vec::new, |(globals, _)| globals.contents().clone_list());
    let names: Vec<_> = advertised
        .iter()
        .map(|g| (g.interface.as_str(), g.version))
        .collect();
    let selection = select_source(display, init.is_some(), &names);
    let (
        Selection::Wayland {
            input_idle,
            lock_notify,
        },
        Some((globals, mut queue)),
    ) = (selection, init)
    else {
        let _ = tx.send(selection);
        return;
    };
    let qh = queue.handle();
    let bind = || -> Result<(), wayland_client::globals::BindError> {
        let seat: WlSeat = globals.bind(&qh, 1..=1, ())?;
        let notifier: ExtIdleNotifierV1 = globals.bind(&qh, 1..=2, ())?;
        for (i, timeout) in IDLE_TIMEOUTS.into_iter().enumerate() {
            let ms = timeout.as_millis() as u32;
            if input_idle {
                notifier.get_input_idle_notification(ms, &seat, &qh, i);
            } else {
                notifier.get_idle_notification(ms, &seat, &qh, i);
            }
        }
        if lock_notify {
            let lock: HyprlandLockNotifierV1 = globals.bind(&qh, 1..=1, ())?;
            lock.get_lock_notification(&qh, ());
        }
        Ok(())
    };
    if bind().is_err() {
        let _ = tx.send(Selection::Logind("binding a Wayland global failed"));
        return;
    }
    if tx.send(selection).is_err() {
        return; // start() stopped waiting and uses logind.
    }
    let mut handler = Handler(state);
    loop {
        if let Err(e) = queue.blocking_dispatch(&mut handler) {
            warn!("Activity: lost the Wayland connection ({e}); the tipi is LEGACY until restart");
            handler.0.lock().unwrap().lost = true;
            return;
        }
    }
}

struct Handler(Arc<Mutex<WaylandState>>);

impl Dispatch<WlRegistry, GlobalListContents> for Handler {
    fn event(
        _: &mut Self,
        _: &WlRegistry,
        _: <WlRegistry as wayland_client::Proxy>::Event,
        _: &GlobalListContents,
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
    }
}

delegate_noop!(Handler: ignore WlSeat);
delegate_noop!(Handler: ExtIdleNotifierV1);
delegate_noop!(Handler: HyprlandLockNotifierV1);

impl Dispatch<ExtIdleNotificationV1, usize> for Handler {
    fn event(
        handler: &mut Self,
        _: &ExtIdleNotificationV1,
        event: ext_idle_notification_v1::Event,
        &notification: &usize,
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
        let mut state = handler.0.lock().unwrap();
        match event {
            ext_idle_notification_v1::Event::Idled => state.idled(notification, Instant::now()),
            ext_idle_notification_v1::Event::Resumed => state.resumed(),
            _ => {}
        }
    }
}

impl Dispatch<HyprlandLockNotificationV1, ()> for Handler {
    fn event(
        handler: &mut Self,
        _: &HyprlandLockNotificationV1,
        event: hyprland_lock_notification_v1::Event,
        _: &(),
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
        handler.0.lock().unwrap().locked = match event {
            hyprland_lock_notification_v1::Event::Locked => true,
            hyprland_lock_notification_v1::Event::Unlocked => false,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activity::{ActivityLevel, activity_level, current_tipi};
    use crate::bluetooth::aacp::TipiFields;

    const SEC: Duration = Duration::from_secs(1);
    const FULL: [(&str, u32); 3] = [
        ("wl_seat", 9),
        ("ext_idle_notifier_v1", 2),
        ("hyprland_lock_notifier_v1", 1),
    ];

    fn level(state: &WaylandState, now: Instant) -> ActivityLevel {
        activity_level(state.inputs(now))
    }

    #[test]
    fn no_events_is_max() {
        let state = WaylandState::default();
        let now = Instant::now();
        assert_eq!(state.inputs(now).since_input, Some(Duration::ZERO));
        assert!(!state.inputs(now).locked);
        assert!(!state.inputs(now).idle);
        assert_eq!(level(&state, now), ActivityLevel::Max);
    }

    #[test]
    fn idled_5s_is_high() {
        let mut state = WaylandState::default();
        let t5 = Instant::now();
        state.idled(0, t5);
        assert_eq!(state.inputs(t5 + 3 * SEC).since_input, Some(8 * SEC));
        assert_eq!(level(&state, t5 + 3 * SEC), ActivityLevel::High);
    }

    #[test]
    fn idled_25s_is_low() {
        let mut state = WaylandState::default();
        let t5 = Instant::now();
        state.idled(0, t5);
        assert_eq!(level(&state, t5 + 20 * SEC), ActivityLevel::High);
        state.idled(1, t5 + 20 * SEC);
        assert_eq!(state.inputs(t5 + 21 * SEC).since_input, Some(26 * SEC));
        assert_eq!(level(&state, t5 + 21 * SEC), ActivityLevel::Low);
        assert_eq!(level(&state, t5 + 100 * SEC), ActivityLevel::Low);
    }

    #[test]
    fn resumed_after_idle_is_max() {
        let mut state = WaylandState::default();
        let t5 = Instant::now();
        state.idled(0, t5);
        state.idled(1, t5 + 20 * SEC);
        state.resumed();
        let now = t5 + 30 * SEC;
        assert_eq!(state.inputs(now).since_input, Some(Duration::ZERO));
        assert_eq!(level(&state, now), ActivityLevel::Max);
    }

    #[test]
    fn locked_is_idle_at_any_input() {
        let t5 = Instant::now();
        let mut state = WaylandState {
            locked: true,
            ..Default::default()
        };
        assert_eq!(level(&state, t5), ActivityLevel::Idle);
        state.idled(0, t5);
        assert_eq!(level(&state, t5 + 3 * SEC), ActivityLevel::Idle);
        state.idled(1, t5 + 20 * SEC);
        assert_eq!(level(&state, t5 + 30 * SEC), ActivityLevel::Idle);
    }

    #[test]
    fn unlocked_returns_to_input_level() {
        let mut state = WaylandState::default();
        let t5 = Instant::now();
        state.idled(0, t5);
        state.locked = true;
        assert_eq!(level(&state, t5 + 3 * SEC), ActivityLevel::Idle);
        state.locked = false;
        assert_eq!(level(&state, t5 + 3 * SEC), ActivityLevel::High);
        state.resumed();
        assert_eq!(level(&state, t5 + 3 * SEC), ActivityLevel::Max);
    }

    #[test]
    fn select_idle_v2_uses_input_notifications() {
        let selection = select_source(true, true, &FULL);
        let want = Selection::Wayland {
            input_idle: true,
            lock_notify: true,
        };
        assert_eq!(selection, want);
    }

    #[test]
    fn select_idle_v1_uses_idle_notifications() {
        let mut globals = FULL;
        globals[1].1 = 1;
        let want = Selection::Wayland {
            input_idle: false,
            lock_notify: true,
        };
        assert_eq!(select_source(true, true, &globals), want);
    }

    #[test]
    fn select_no_lock_notifier_stays_unlocked() {
        let want = Selection::Wayland {
            input_idle: true,
            lock_notify: false,
        };
        assert_eq!(select_source(true, true, &FULL[..2]), want);
        let source = WaylandActivity(Default::default());
        assert!(!source.snapshot().unwrap().locked);
    }

    fn is_logind(selection: Selection) -> bool {
        matches!(selection, Selection::Logind(_))
    }

    #[test]
    fn select_no_wayland_display_is_logind() {
        assert!(is_logind(select_source(false, false, &[])));
    }

    #[test]
    fn select_connect_failed_is_logind() {
        assert!(is_logind(select_source(true, false, &[])));
    }

    #[test]
    fn select_no_idle_notifier_is_logind() {
        assert!(is_logind(select_source(true, true, &[FULL[0], FULL[2]])));
        assert!(is_logind(select_source(true, true, &FULL[1..])));
    }

    #[tokio::test]
    async fn lost_connection_snapshot_is_none() {
        let state = Arc::new(Mutex::new(WaylandState::default()));
        let source: Arc<dyn ActivitySource> = Arc::new(WaylandActivity(state.clone()));
        assert!(source.snapshot().is_some());
        state.lock().unwrap().lost = true;
        assert_eq!(source.snapshot(), None);
        assert_eq!(current_tipi(&source).await, TipiFields::LEGACY);
    }

    #[test]
    fn snapshot_reads_shared_state() {
        let state = Arc::new(Mutex::new(WaylandState::default()));
        let source = WaylandActivity(state.clone());
        let before = source.snapshot().unwrap();
        assert_eq!(activity_level(before), ActivityLevel::Max);
        state.lock().unwrap().idled(1, Instant::now() - 10 * SEC);
        let idled = source.snapshot().unwrap();
        assert!(idled.since_input.unwrap() >= 35 * SEC, "{idled:?}");
        state.lock().unwrap().locked = true;
        assert_eq!(
            activity_level(source.snapshot().unwrap()),
            ActivityLevel::Idle
        );
    }

    use crate::activity::read_lock_hint;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    /// A logind lock reader giving `locked` that counts its calls.
    fn counting_reader(
        locked: Option<Option<bool>>,
    ) -> (
        Arc<AtomicUsize>,
        impl Fn() -> Option<Option<bool>> + Send + Sync + 'static,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let reader = move || {
            counter.fetch_add(1, SeqCst);
            locked
        };
        (calls, reader)
    }

    fn calls(calls: &AtomicUsize) -> usize {
        calls.load(SeqCst)
    }

    /// A state whose 5 s notification idled 3 s ago: High while unlocked.
    fn idled_3s_ago() -> Arc<Mutex<WaylandState>> {
        let mut state = WaylandState::default();
        state.idled(0, Instant::now() - 3 * SEC);
        Arc::new(Mutex::new(state))
    }

    /// The source's snapshot, checking its `since_input` is the state's.
    fn mixed_snapshot(
        state: &Arc<Mutex<WaylandState>>,
        source: &Arc<dyn ActivitySource>,
    ) -> ActivityInputs {
        let before = state.lock().unwrap().inputs(Instant::now()).since_input;
        let inputs = source.snapshot().unwrap();
        let after = state.lock().unwrap().inputs(Instant::now()).since_input;
        assert!(before <= inputs.since_input && inputs.since_input <= after);
        inputs
    }

    #[test]
    fn logind_lock_locked_is_idle() {
        let state = idled_3s_ago();
        let (calls_made, reader) = counting_reader(Some(Some(true)));
        let source = wayland_source(state.clone(), false, reader);
        let inputs = mixed_snapshot(&state, &source);
        assert!(inputs.locked);
        assert!(inputs.since_input.unwrap() >= 8 * SEC, "{inputs:?}");
        assert_eq!(activity_level(inputs), ActivityLevel::Idle);
        assert_eq!(calls(&calls_made), 1);
    }

    #[test]
    fn logind_lock_unlocked_keeps_input_level() {
        let state = idled_3s_ago();
        let (_, reader) = counting_reader(Some(Some(false)));
        let source = wayland_source(state.clone(), false, reader);
        let inputs = mixed_snapshot(&state, &source);
        assert!(!inputs.locked);
        assert_eq!(activity_level(inputs), ActivityLevel::High);
    }

    #[test]
    fn lock_notify_never_reads_logind() {
        let state = idled_3s_ago();
        let (calls_made, reader) = counting_reader(Some(Some(true)));
        let source = wayland_source(state.clone(), true, reader);
        for locked in [false, true, false, true] {
            state.lock().unwrap().locked = locked;
            assert_eq!(source.snapshot().unwrap().locked, locked);
        }
        assert_eq!(calls(&calls_made), 0);
    }

    #[tokio::test]
    async fn logind_lock_failed_read_keeps_input() {
        // No events yet is Max (7), so a LEGACY (6) tipi would show.
        let state = Arc::new(Mutex::new(WaylandState::default()));
        let (calls_made, reader) = counting_reader(Some(None));
        let source = wayland_source(state.clone(), false, reader);
        let inputs = mixed_snapshot(&state, &source);
        assert!(!inputs.locked);
        assert_eq!(inputs.since_input, Some(Duration::ZERO));
        assert_eq!(activity_level(inputs), ActivityLevel::Max);
        assert_eq!(calls(&calls_made), 1);
        assert_ne!(current_tipi(&source).await, TipiFields::LEGACY);
    }

    #[test]
    fn logind_lock_read_outside_state_lock() {
        let state = idled_3s_ago();
        let shared = state.clone();
        let source = wayland_source(state, false, move || {
            assert!(
                shared.try_lock().is_ok(),
                "logind read under the state lock"
            );
            Some(Some(true))
        });
        assert!(source.snapshot().unwrap().locked);
    }

    #[test]
    fn logind_lock_lost_connection_is_none() {
        let state = idled_3s_ago();
        let (calls_made, reader) = counting_reader(Some(Some(true)));
        let source = wayland_source(state.clone(), false, reader);
        state.lock().unwrap().lost = true;
        assert_eq!(source.snapshot(), None);
        assert_eq!(calls(&calls_made), 0);
    }

    #[test]
    fn logind_lock_busy_slot_is_none() {
        let slot = Mutex::new(None::<u32>);
        let held = slot.lock().unwrap();
        let busy = read_lock_hint(&slot, || panic!("opened"), |_| panic!("read"));
        assert_eq!(busy, None);
        drop(held);
        let state = idled_3s_ago();
        let (calls_made, reader) = counting_reader(None);
        let source = wayland_source(state, false, reader);
        assert_eq!(source.snapshot(), None);
        assert_eq!(calls(&calls_made), 1);
    }

    #[test]
    fn logind_lock_failed_read_is_unlocked() {
        let slot = Mutex::new(None::<u32>);
        assert_eq!(read_lock_hint(&slot, || None, |_| Some(true)), Some(None));
        assert_eq!(read_lock_hint(&slot, || Some(1), |_| None), Some(None));
        assert_eq!(
            read_lock_hint(&slot, || Some(1), |_| Some(true)),
            Some(Some(true))
        );
        let state = idled_3s_ago();
        let (_, reader) = counting_reader(Some(None));
        let source = wayland_source(state.clone(), false, reader);
        let inputs = mixed_snapshot(&state, &source);
        assert!(!inputs.locked);
        assert_eq!(activity_level(inputs), ActivityLevel::High);
    }
}
