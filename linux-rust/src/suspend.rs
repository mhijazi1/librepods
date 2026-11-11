//! Sleep and wake in the handoff mode (#15, spike #261, DECISIONS
//! 2026-11-10), as the Mac does. A logind `delay` inhibitor holds the sleep
//! until every running handoff task has stepped `Input::Sleep` (pause, relay
//! idle, disconnect, no release) and acked, or `SLEEP_PREPARE_TIMEOUT`
//! passed. On wake the inhibitor is taken again and each AirPods a task
//! disconnected is reconnected, if its autoConnect is on, on a short capped
//! schedule. No #307 marker is set: the new task takes through the #324
//! connect take, which resumes nothing. Linux dropped the link itself, so
//! #308's rule against timer reconnects doesn't apply.

use crate::reconnect::{Bluez, ConnectGuard, Connector, bluez_device};
use crate::utils;
use dbus::Message;
use dbus::arg::OwnedFd;
use dbus::blocking::Connection;
use dbus::message::MatchRule;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use log::{info, warn};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::Instant;

/// How long the sleep waits for the handoff tasks' acks.
pub(crate) const SLEEP_PREPARE_TIMEOUT: Duration = Duration::from_secs(4);
// Below logind's default InhibitDelayMaxSec (5 s), past which it sleeps anyway.
const _: () = assert!(SLEEP_PREPARE_TIMEOUT.as_millis() < 5000);
/// While a sleep is pending, the pause and the relay before the sleep
/// disconnect stop waiting this long before its deadline, leaving the
/// disconnect this long before the release.
pub(crate) const SLEEP_DISCONNECT_RESERVE: Duration = Duration::from_millis(1500);
const _: () = assert!(SLEEP_DISCONNECT_RESERVE.as_millis() < SLEEP_PREPARE_TIMEOUT.as_millis());
/// The wait before the first wake reconnect attempt; each later one waits
/// twice the one before, up to `WAKE_RECONNECT_MAX`.
/// Engineering defaults; #177's hardware run may retune them.
pub(crate) const WAKE_RECONNECT_FIRST: Duration = Duration::from_secs(2);
pub(crate) const WAKE_RECONNECT_MAX: Duration = Duration::from_secs(30);
pub(crate) const WAKE_RECONNECT_ATTEMPTS: u32 = 6;

/// The wait before wake reconnect attempt `attempt` (from 0); None once
/// `WAKE_RECONNECT_ATTEMPTS` were made.
fn wake_delay(attempt: u32) -> Option<Duration> {
    (attempt < WAKE_RECONNECT_ATTEMPTS).then(|| {
        WAKE_RECONNECT_FIRST
            .saturating_mul(2u32.saturating_pow(attempt))
            .min(WAKE_RECONNECT_MAX)
    })
}

/// logind's sleep inhibitor, faked in tests.
pub(crate) trait Logind: Send + 'static {
    /// Released when dropped.
    type Inhibitor: Send;
    /// Take a `delay` sleep inhibitor; None (logged) if logind refused.
    fn inhibit(&self) -> impl Future<Output = Option<Self::Inhibitor>> + Send;
}

/// One AirPods' BlueZ link, faked in tests.
pub(crate) trait BluezLink: Send + Sync + 'static {
    /// BlueZ already reports the device connected.
    fn connected(&self, mac: &str) -> impl Future<Output = bool> + Send;
    /// Connect the device; the error to log on failure.
    fn connect(&self, mac: &str) -> impl Future<Output = Result<(), String>> + Send;
    /// Disconnect the device; the error to log on failure.
    fn disconnect(&self, mac: &str) -> impl Future<Output = Result<(), String>> + Send;
    /// The device's autoConnect preference.
    fn auto_connect(&self, mac: &str) -> bool;
}

struct SystemLogind;

impl Logind for SystemLogind {
    type Inhibitor = OwnedFd;

    async fn inhibit(&self) -> Option<OwnedFd> {
        let taken = tokio::task::spawn_blocking(|| {
            let conn = Connection::new_system()?;
            let proxy = conn.with_proxy(
                "org.freedesktop.login1",
                "/org/freedesktop/login1",
                Duration::from_secs(2),
            );
            let (fd,): (OwnedFd,) = proxy.method_call(
                "org.freedesktop.login1.Manager",
                "Inhibit",
                (
                    "sleep",
                    "LibrePods",
                    "Disconnect the AirPods before sleep",
                    "delay",
                ),
            )?;
            Ok::<_, dbus::Error>(fd)
        })
        .await
        .map_err(|e| e.to_string())
        .and_then(|taken| taken.map_err(|e| e.to_string()));
        taken
            .inspect_err(|e| warn!("Sleep: no logind sleep inhibitor: {e}"))
            .ok()
    }
}

impl BluezLink for Bluez {
    async fn connected(&self, mac: &str) -> bool {
        Connector::connected(self, mac).await
    }

    async fn connect(&self, mac: &str) -> Result<(), String> {
        Connector::connect(self, mac).await
    }

    async fn disconnect(&self, mac: &str) -> Result<(), String> {
        let address = mac.parse::<bluer::Address>().map_err(|e| e.to_string())?;
        async { bluez_device(address).await?.disconnect().await }
            .await
            .map_err(|e| e.to_string())
    }

    fn auto_connect(&self, mac: &str) -> bool {
        utils::load_auto_connect(mac)
    }
}

/// The handoff task's `Command::Disconnect`, bounded by the pending
/// sleep's deadline.
pub(crate) async fn disconnect(mac: &str) {
    SLEEPERS.disconnect(&Bluez, mac).await
}

/// Await `work`, but while a sleep is pending stop at its deadline minus
/// `SLEEP_DISCONNECT_RESERVE`, so the disconnect after it still starts
/// before the release. None if cut short.
pub(crate) async fn sleep_bounded<T>(work: impl Future<Output = T>) -> Option<T> {
    SLEEPERS.bounded(work).await
}

/// The MACs the handoff tasks disconnected for sleep, for the wake.
type Slept = Arc<Mutex<Vec<String>>>;

/// A sleep notice to one handoff task.
pub(crate) struct SleepNotice {
    ack: oneshot::Sender<()>,
    slept: Slept,
}

impl SleepNotice {
    /// The sleep stopped waiting for it: stepped after the wake, it would
    /// disconnect the AirPods again.
    pub(crate) fn stale(&self) -> bool {
        self.ack.is_closed()
    }

    /// The task disconnects `mac`: the wake reconnects it, even if the ack
    /// comes after `SLEEP_PREPARE_TIMEOUT`.
    pub(crate) fn slept(&self, mac: &str) {
        let mut slept = lock(&self.slept);
        if !slept.iter().any(|m| m == mac) {
            slept.push(mac.to_string());
        }
    }

    /// Done with the sleep.
    pub(crate) fn ack(self) {
        let _ = self.ack.send(());
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// The running handoff tasks' sleep subscriptions, and the pending sleep's
/// deadline.
#[derive(Default)]
pub(crate) struct Sleepers {
    senders: Mutex<Vec<mpsc::UnboundedSender<SleepNotice>>>,
    /// Now + `SLEEP_PREPARE_TIMEOUT` while `sleep` waits; None otherwise.
    deadline: Mutex<Option<Instant>>,
}

/// The app's subscriptions; tests use their own.
pub(crate) static SLEEPERS: Sleepers = Sleepers {
    senders: Mutex::new(Vec::new()),
    deadline: Mutex::new(None),
};

impl Sleepers {
    /// A handoff task's subscription; dropping it unsubscribes.
    pub(crate) fn subscribe(&self) -> mpsc::UnboundedReceiver<SleepNotice> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut senders = lock(&self.senders);
        senders.retain(|s| !s.is_closed());
        senders.push(tx);
        rx
    }

    /// Notify every subscribed task and wait for its ack, at most until
    /// `SLEEP_PREPARE_TIMEOUT` after the signal was `received`: logind's
    /// delay runs from then, however long the signal waited in the queue.
    /// A task that ends without acking drops its notice, which counts as
    /// its ack.
    async fn sleep(&self, slept: &Slept, received: Instant) {
        let deadline = received + SLEEP_PREPARE_TIMEOUT;
        *lock(&self.deadline) = Some(deadline);
        // Cleared on return, and if the sleep is dropped: a stale deadline
        // would cut every later pause short.
        struct Pending<'a>(&'a Mutex<Option<Instant>>);
        impl Drop for Pending<'_> {
            fn drop(&mut self) {
                *lock(self.0) = None;
            }
        }
        let _pending = Pending(&self.deadline);
        let mut acks: FuturesUnordered<_> = {
            let mut senders = lock(&self.senders);
            senders.retain(|s| !s.is_closed());
            senders
                .iter()
                .filter_map(|s| {
                    let (ack, acked) = oneshot::channel();
                    let slept = slept.clone();
                    s.send(SleepNotice { ack, slept }).ok().map(|()| acked)
                })
                .collect()
        };
        let all = async { while acks.next().await.is_some() {} };
        if tokio::time::timeout_at(deadline, all).await.is_err() {
            warn!("Sleep: a handoff task didn't ack within {SLEEP_PREPARE_TIMEOUT:?}");
        }
    }

    fn deadline(&self) -> Option<Instant> {
        *lock(&self.deadline)
    }

    /// `sleep_bounded` on these subscriptions.
    async fn bounded<T>(&self, work: impl Future<Output = T>) -> Option<T> {
        let Some(deadline) = self.deadline() else {
            return Some(work.await);
        };
        let done = tokio::time::timeout_at(deadline - SLEEP_DISCONNECT_RESERVE, work).await;
        if done.is_err() {
            warn!("Sleep: stopped waiting to leave time for the disconnect");
        }
        done.ok()
    }

    /// The sleep disconnect, bounded by the pending sleep's deadline; with
    /// none pending (released, or woken) BlueZ isn't called.
    async fn disconnect(&self, link: &impl BluezLink, mac: &str) {
        let Some(deadline) = self.deadline() else {
            // shortcut: a task that steps Sleep after the release leaves the core lost without a disconnect (#535)
            warn!("Sleep: not disconnecting {mac}: the sleep no longer waits");
            return;
        };
        match tokio::time::timeout_at(deadline, link.disconnect(mac)).await {
            Ok(Ok(())) => info!("Sleep: disconnected {mac}"),
            // shortcut: on a failed or timed-out disconnect the core stays lost (no take) until the session drops (#535)
            Ok(Err(e)) => warn!("Sleep: failed to disconnect {mac}: {e}"),
            Err(_) => warn!("Sleep: disconnecting {mac} timed out"),
        }
    }
}

/// Take the sleep inhibitor and follow logind's `PrepareForSleep` on a
/// system-bus connection of its own, on its own thread: the app's shared one
/// blocks seconds in BlueZ reads, which would eat logind's delay.
pub(crate) fn start() {
    let (prepare, prepared) = mpsc::unbounded_channel();
    tokio::spawn(run(SystemLogind, Arc::new(Bluez), &SLEEPERS, prepared));
    let watch = move || -> Result<(), dbus::Error> {
        let conn = Connection::new_system()?;
        // The bus routes only logind's broadcasts to this rule; the shared
        // system-service check `is_broadcast` drops a unicast one, which any
        // sender can address to us.
        let rule = MatchRule::new_signal("org.freedesktop.login1.Manager", "PrepareForSleep")
            .with_sender("org.freedesktop.login1");
        conn.add_match(rule, move |(start,): (bool,), _, msg| {
            if is_broadcast(msg) {
                // Stamped here: `run` may get to it only after an inhibit.
                let _ = prepare.send((start, Instant::now()));
            }
            true
        })?;
        loop {
            conn.process(Duration::from_secs(60))?;
        }
    };
    let spawned = std::thread::Builder::new()
        .name("prepare-for-sleep".into())
        .spawn(move || {
            if let Err(e) = watch() {
                warn!("Sleep: not following logind's PrepareForSleep: {e}");
            }
        });
    if let Err(e) = spawned {
        warn!("Sleep: not following logind's PrepareForSleep: {e}");
    }
}

/// System services (logind, BlueZ) broadcast the signals we follow: one
/// addressed to us is not theirs, as dbus-rs's non-strict sender match lets
/// any unique name through.
pub(crate) fn is_broadcast(msg: &Message) -> bool {
    msg.destination().is_none()
}

/// Follow `PrepareForSleep` (true: about to sleep, false: woke up), each
/// with the time the bus thread received it.
async fn run<L: Logind, B: BluezLink>(
    logind: L,
    bluez: Arc<B>,
    sleepers: &'static Sleepers,
    mut prepare: mpsc::UnboundedReceiver<(bool, Instant)>,
) {
    let mut inhibitor = logind.inhibit().await;
    // Until its wake reconnect ends, so a sleep that aborts it keeps it.
    let slept = Slept::default();
    let mut reconnects = JoinSet::new();
    while let Some((start, received)) = prepare.recv().await {
        // Aborts the running reconnects (their MACs stay in `slept`) and
        // forgets the finished ones.
        reconnects.shutdown().await;
        if start {
            sleepers.sleep(&slept, received).await;
            drop(inhibitor.take());
            info!("Sleep: ready; disconnected {:?}", lock(&slept));
            continue;
        }
        if inhibitor.is_none() {
            inhibitor = logind.inhibit().await;
        }
        let macs = lock(&slept).clone();
        for mac in macs {
            if !bluez.auto_connect(&mac) {
                info!("Wake: autoConnect is off for {mac}; not reconnecting");
                lock(&slept).retain(|m| *m != mac);
                continue;
            }
            let (bluez, slept) = (bluez.clone(), slept.clone());
            reconnects.spawn(async move {
                wake_reconnect(bluez, mac.clone()).await;
                lock(&slept).retain(|m| *m != mac);
            });
        }
    }
}

/// Reconnect `mac` on the wake schedule until it connects, BlueZ reports
/// it connected (le.rs got there first) or the attempts run out. An
/// attempt whose connect guard is busy is skipped.
async fn wake_reconnect(bluez: Arc<impl BluezLink>, mac: String) {
    let mut attempt = 0;
    while let Some(delay) = wake_delay(attempt) {
        attempt += 1;
        tokio::time::sleep(delay).await;
        if bluez.connected(&mac).await {
            info!("Wake: {mac} is connected");
            return;
        }
        let Some(_guard) = ConnectGuard::try_new(&mac) else {
            info!("Wake: a connect to {mac} is in flight; skipping attempt {attempt}");
            continue;
        };
        match bluez.connect(&mac).await {
            Ok(()) => {
                info!("Wake: reconnected {mac}");
                return;
            }
            Err(e) => info!("Wake: reconnect {attempt} to {mac} failed: {e}"),
        }
    }
    info!("Wake: gave up reconnecting {mac}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::Instant;

    // From the issue number, so other tests' connect guards never collide;
    // one MAC per test, as the guard is process-wide.
    const A: &str = "15:00:00:00:00:0A";
    const C: &str = "15:00:00:00:00:0C";
    const D: &str = "15:00:00:00:00:0D";
    const E: &str = "15:00:00:00:00:0E";
    const F: &str = "15:00:00:00:00:0F";
    const G: &str = "15:00:00:00:00:10";
    const H: &str = "15:00:00:00:00:11";
    const I: &str = "15:00:00:00:00:12";
    const J: &str = "15:00:00:00:00:13";
    const K: &str = "15:00:00:00:00:14";
    const M: &str = "15:00:00:00:00:15";

    // Fails the MAC parse before any D-Bus call.
    #[tokio::test]
    async fn bluez_disconnect_err_on_bad_mac() {
        assert_eq!(
            BluezLink::disconnect(&Bluez, "not-a-mac").await,
            Err("invalid Bluetooth address: not-a-mac".to_string())
        );
    }

    /// Counts inhibitors taken and still held.
    #[derive(Clone, Default)]
    struct FakeLogind {
        taken: Arc<AtomicUsize>,
        held: Arc<AtomicUsize>,
        /// How long each next inhibit takes; none: at once.
        delays: Arc<Mutex<VecDeque<Duration>>>,
    }

    struct Held(Arc<AtomicUsize>);

    impl Drop for Held {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl Logind for FakeLogind {
        type Inhibitor = Held;

        async fn inhibit(&self) -> Option<Held> {
            let delay = self.delays.lock().unwrap().pop_front();
            tokio::time::sleep(delay.unwrap_or_default()).await;
            self.taken.fetch_add(1, Ordering::SeqCst);
            self.held.fetch_add(1, Ordering::SeqCst);
            Some(Held(self.held.clone()))
        }
    }

    impl FakeLogind {
        fn held(&self) -> usize {
            self.held.load(Ordering::SeqCst)
        }
    }

    #[derive(Default)]
    struct FakeLink {
        /// Successive connects' results; Err once used up.
        results: Mutex<VecDeque<Result<(), String>>>,
        /// `connected` answers true from this call (0-based) on.
        connected_from: Option<usize>,
        connected_calls: AtomicUsize,
        /// Each connect's MAC and time.
        connects: Mutex<Vec<(String, Instant)>>,
        auto_off: Vec<&'static str>,
        disconnects: AtomicUsize,
        /// `disconnect` never returns.
        hung_disconnect: bool,
    }

    impl BluezLink for FakeLink {
        async fn connected(&self, _: &str) -> bool {
            let call = self.connected_calls.fetch_add(1, Ordering::SeqCst);
            self.connected_from.is_some_and(|from| call >= from)
        }

        async fn connect(&self, mac: &str) -> Result<(), String> {
            // The attempt holds the guard: no second connect alongside.
            assert!(
                ConnectGuard::try_new(mac).is_none(),
                "connect without the guard"
            );
            self.connects
                .lock()
                .unwrap()
                .push((mac.to_string(), Instant::now()));
            let next = self.results.lock().unwrap().pop_front();
            next.unwrap_or_else(|| Err("refused".into()))
        }

        async fn disconnect(&self, _: &str) -> Result<(), String> {
            self.disconnects.fetch_add(1, Ordering::SeqCst);
            if self.hung_disconnect {
                std::future::pending::<()>().await;
            }
            Ok(())
        }

        fn auto_connect(&self, mac: &str) -> bool {
            !self.auto_off.contains(&mac)
        }
    }

    impl FakeLink {
        fn connects(&self) -> Vec<(String, Instant)> {
            self.connects.lock().unwrap().clone()
        }
    }

    /// Let every ready task run; the paused clock doesn't move meanwhile.
    async fn settle() {
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
    }

    fn sleepers() -> &'static Sleepers {
        Box::leak(Box::default())
    }

    /// `run` with a fresh `Sleepers`, its inhibitor taken.
    async fn start_run(
        link: Arc<FakeLink>,
    ) -> (
        FakeLogind,
        &'static Sleepers,
        mpsc::UnboundedSender<(bool, Instant)>,
    ) {
        let logind = FakeLogind::default();
        let sleepers = sleepers();
        let (prepare, prepared) = mpsc::unbounded_channel();
        tokio::spawn(run(logind.clone(), link, sleepers, prepared));
        settle().await;
        assert_eq!(logind.held(), 1);
        (logind, sleepers, prepare)
    }

    #[test]
    fn prepare_for_sleep_only_from_a_broadcast() {
        let signal = || {
            Message::new_signal(
                "/org/freedesktop/login1",
                "org.freedesktop.login1.Manager",
                "PrepareForSleep",
            )
            .unwrap()
            .append1(true)
        };
        assert!(is_broadcast(&signal()));
        // A unicast one, which any sender can address to us.
        let mut unicast = signal();
        unicast.set_destination(Some(":1.15".into()));
        assert!(!is_broadcast(&unicast));
    }

    #[test]
    fn bluez_signals_only_from_a_broadcast() {
        let path = "/org/bluez/hci0/dev_AA_BB_CC_DD_EE_FF";
        let changed =
            Message::new_signal(path, "org.freedesktop.DBus.Properties", "PropertiesChanged")
                .unwrap()
                .append3(
                    "org.bluez.Device1",
                    std::collections::HashMap::from([("Connected", dbus::arg::Variant(false))]),
                    Vec::<String>::new(),
                );
        let disconnected = Message::new_signal(path, "org.bluez.Device1", "Disconnected")
            .unwrap()
            .append1("org.bluez.Reason.Remote");
        for signal in [changed, disconnected] {
            assert!(is_broadcast(&signal));
            // A unicast one, which any sender can address to us.
            let mut unicast = signal;
            unicast.set_destination(Some(":1.15".into()));
            assert!(!is_broadcast(&unicast));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_releases_the_inhibitor_after_every_task_is_ready() {
        let start = Instant::now();
        // No task: released at once.
        let (logind, _, prepare) = start_run(Arc::default()).await;
        prepare.send((true, Instant::now())).unwrap();
        settle().await;
        assert_eq!(logind.held(), 0);

        // Two tasks: held until both ack.
        let (logind, sleepers, prepare) = start_run(Arc::default()).await;
        let mut a = sleepers.subscribe();
        let mut b = sleepers.subscribe();
        // An ended task's subscription counts for nothing.
        drop(sleepers.subscribe());
        prepare.send((true, Instant::now())).unwrap();
        let notice_a = a.recv().await.unwrap();
        let notice_b = b.recv().await.unwrap();
        settle().await;
        assert_eq!(logind.held(), 1);
        notice_a.slept(A);
        notice_a.ack();
        settle().await;
        assert_eq!(logind.held(), 1);
        notice_b.ack();
        settle().await;
        assert_eq!(logind.held(), 0);
        // Released by the acks, not the timeout.
        assert_eq!(Instant::now(), start);
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_releases_the_inhibitor_at_the_prepare_timeout() {
        let link = Arc::new(FakeLink {
            results: Mutex::new([Ok(())].into()),
            ..FakeLink::default()
        });
        let (logind, sleepers, prepare) = start_run(link.clone()).await;
        let mut a = sleepers.subscribe();
        let mut b = sleepers.subscribe();
        prepare.send((true, Instant::now())).unwrap();
        a.recv().await.unwrap().ack();
        // B disconnects, but never acks.
        let notice_b = b.recv().await.unwrap();
        notice_b.slept(I);
        settle().await;
        assert!(!notice_b.stale());
        tokio::time::advance(SLEEP_PREPARE_TIMEOUT - Duration::from_millis(1)).await;
        settle().await;
        assert_eq!(logind.held(), 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        assert_eq!(logind.held(), 0);
        // Stepped after the wake, it would disconnect again.
        assert!(notice_b.stale());
        // Its AirPods are still reconnected on wake.
        prepare.send((false, Instant::now())).unwrap();
        settle().await;
        tokio::time::advance(WAKE_RECONNECT_FIRST).await;
        settle().await;
        assert_eq!(link.connects().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn queued_sleep_keeps_the_signal_deadline() {
        let (logind, sleepers, prepare) = start_run(Arc::default()).await;
        // A first sleep releases the startup inhibitor.
        prepare.send((true, Instant::now())).unwrap();
        settle().await;
        assert_eq!(logind.held(), 0);
        // One task that never acks.
        let _task = sleepers.subscribe();
        // The wake at t0: its inhibit takes 2 s.
        logind
            .delays
            .lock()
            .unwrap()
            .push_back(Duration::from_secs(2));
        let t0 = Instant::now();
        prepare.send((false, t0)).unwrap();
        settle().await;
        // A sleep received at t0 + 0.5 s waits behind it.
        tokio::time::advance(Duration::from_millis(500)).await;
        let received = Instant::now();
        prepare.send((true, received)).unwrap();
        settle().await;
        let release = received + SLEEP_PREPARE_TIMEOUT;
        tokio::time::advance(release - Duration::from_millis(1) - Instant::now()).await;
        settle().await;
        assert_eq!(logind.taken.load(Ordering::SeqCst), 2);
        assert_eq!(logind.held(), 1);
        // The c7 bounds use the same deadline.
        assert_eq!(sleepers.deadline(), Some(release));
        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        assert_eq!(logind.held(), 0);
        assert_eq!(Instant::now(), t0 + Duration::from_millis(4500));
    }

    /// Run `wake_reconnect` for `mac` to its end; the connect times, from
    /// its start.
    async fn reconnect_times(link: FakeLink, mac: &str) -> Vec<Duration> {
        let start = Instant::now();
        let link = Arc::new(link);
        wake_reconnect(link.clone(), mac.to_string()).await;
        link.connects().iter().map(|(_, at)| *at - start).collect()
    }

    /// The schedule's attempt times from the wake, as a running sum.
    fn schedule(attempts: u32) -> Vec<Duration> {
        (0..attempts)
            .scan(Duration::ZERO, |at, n| {
                *at += wake_delay(n).unwrap();
                Some(*at)
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn wake_reconnect_schedule_is_capped() {
        assert_eq!(wake_delay(0), Some(WAKE_RECONNECT_FIRST));
        assert_eq!(wake_delay(1), Some(WAKE_RECONNECT_FIRST * 2));
        for n in 0..WAKE_RECONNECT_ATTEMPTS {
            assert!(wake_delay(n).unwrap() <= WAKE_RECONNECT_MAX, "{n}");
        }
        assert_eq!(
            wake_delay(WAKE_RECONNECT_ATTEMPTS - 1),
            Some(WAKE_RECONNECT_MAX)
        );
        assert_eq!(wake_delay(WAKE_RECONNECT_ATTEMPTS), None);
        assert_eq!(wake_delay(u32::MAX), None);
        // Every connect fails: one per scheduled time, then none.
        let times = reconnect_times(FakeLink::default(), E).await;
        assert_eq!(times, schedule(WAKE_RECONNECT_ATTEMPTS));
    }

    #[tokio::test(start_paused = true)]
    async fn wake_reconnect_stops_on_first_success() {
        let link = FakeLink {
            results: Mutex::new([Err("busy".into()), Ok(())].into()),
            ..FakeLink::default()
        };
        assert_eq!(reconnect_times(link, F).await, schedule(2));
        // BlueZ reports it connected at the third attempt: no connect then.
        let link = FakeLink {
            connected_from: Some(2),
            ..FakeLink::default()
        };
        assert_eq!(reconnect_times(link, G).await, schedule(2));
        // Already connected at the first.
        let link = FakeLink {
            connected_from: Some(0),
            ..FakeLink::default()
        };
        assert_eq!(reconnect_times(link, H).await, []);
    }

    #[tokio::test(start_paused = true)]
    async fn wake_reconnect_waits_for_the_connect_guard() {
        let link = Arc::new(FakeLink {
            results: Mutex::new([Ok(())].into()),
            ..FakeLink::default()
        });
        // le.rs's connect is in flight.
        let guard = ConnectGuard::try_new(D).unwrap();
        let task = tokio::spawn(wake_reconnect(link.clone(), D.to_string()));
        let at = schedule(2);
        tokio::time::advance(at[0]).await;
        settle().await;
        assert_eq!(link.connects(), []);
        drop(guard);
        tokio::time::advance(at[1] - at[0]).await;
        settle().await;
        assert_eq!(link.connects().len(), 1);
        assert!(task.is_finished());
    }

    #[tokio::test(start_paused = true)]
    async fn wake_reconnects_only_devices_put_to_sleep() {
        let link = Arc::new(FakeLink {
            results: Mutex::new([Ok(())].into()),
            auto_off: vec![C],
            ..FakeLink::default()
        });
        let (logind, sleepers, prepare) = start_run(link.clone()).await;
        // A and C disconnected, B's session was already gone, D has no task.
        let tasks = [Some(A), None, Some(C)].map(|slept| (sleepers.subscribe(), slept));
        prepare.send((true, Instant::now())).unwrap();
        for (mut rx, slept) in tasks {
            let notice = rx.recv().await.unwrap();
            if let Some(mac) = slept {
                notice.slept(mac);
            }
            notice.ack();
        }
        settle().await;
        assert_eq!(logind.held(), 0);
        prepare.send((false, Instant::now())).unwrap();
        settle().await;
        assert_eq!(logind.taken.load(Ordering::SeqCst), 2);
        assert_eq!(logind.held(), 1);
        tokio::time::advance(WAKE_RECONNECT_MAX * WAKE_RECONNECT_ATTEMPTS).await;
        settle().await;
        let macs: Vec<_> = link.connects().into_iter().map(|(mac, _)| mac).collect();
        assert_eq!(macs, [A]);
    }

    /// A sleep pending on fresh `Sleepers`, its one task's notice held.
    async fn pending_sleep() -> (&'static Sleepers, SleepNotice, tokio::task::JoinHandle<()>) {
        let sleepers = sleepers();
        let mut task = sleepers.subscribe();
        let sleep =
            tokio::spawn(async move { sleepers.sleep(&Slept::default(), Instant::now()).await });
        let notice = task.recv().await.unwrap();
        (sleepers, notice, sleep)
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_disconnect_after_the_release_does_nothing() {
        let link = FakeLink::default();
        let disconnects = || link.disconnects.load(Ordering::SeqCst);
        // No sleep pending: BlueZ isn't called.
        sleepers().disconnect(&link, M).await;
        assert_eq!(disconnects(), 0);
        let (sleepers, notice, sleep) = pending_sleep().await;
        sleepers.disconnect(&link, M).await;
        assert_eq!(disconnects(), 1);
        notice.ack();
        sleep.await.unwrap();
        // Released (or woken): a late step disconnects nothing.
        sleepers.disconnect(&link, M).await;
        assert_eq!(disconnects(), 1);

        // A hung disconnect ends at the deadline, and the release with it.
        let hung = FakeLink {
            hung_disconnect: true,
            ..FakeLink::default()
        };
        let start = Instant::now();
        let (sleepers, _notice, sleep) = pending_sleep().await;
        sleepers.disconnect(&hung, M).await;
        assert_eq!(Instant::now(), start + SLEEP_PREPARE_TIMEOUT);
        sleep.await.unwrap();
        sleepers.disconnect(&hung, M).await;
        assert_eq!(hung.disconnects.load(Ordering::SeqCst), 1);

        // A sleep dropped while pending leaves no deadline behind.
        let (sleepers, _notice, sleep) = pending_sleep().await;
        sleep.abort();
        assert!(sleep.await.unwrap_err().is_cancelled());
        sleepers.disconnect(&link, M).await;
        assert_eq!(disconnects(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_bounded_cuts_hung_work_before_the_deadline() {
        // No sleep pending: awaited unchanged, however long.
        let start = Instant::now();
        let long = SLEEP_PREPARE_TIMEOUT * 3;
        let slow = async {
            tokio::time::sleep(long).await;
            7
        };
        assert_eq!(sleepers().bounded(slow).await, Some(7));
        assert_eq!(Instant::now(), start + long);

        let start = Instant::now();
        let (sleepers, _notice, _sleep) = pending_sleep().await;
        assert_eq!(sleepers.bounded(async { 7 }).await, Some(7));
        // A hung pause stops waiting the reserve before the deadline...
        let hung = std::future::pending::<()>();
        assert_eq!(sleepers.bounded(hung).await, None);
        assert_eq!(
            Instant::now(),
            start + SLEEP_PREPARE_TIMEOUT - SLEEP_DISCONNECT_RESERVE
        );
        // ...so the disconnect still runs before the release.
        let link = FakeLink::default();
        sleepers.disconnect(&link, M).await;
        assert_eq!(link.disconnects.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn resleep_keeps_unfinished_wake_reconnects() {
        let link = Arc::new(FakeLink {
            results: Mutex::new([Ok(()), Ok(())].into()),
            ..FakeLink::default()
        });
        let (_, sleepers, prepare) = start_run(link.clone()).await;
        let mut task = sleepers.subscribe();
        let sleep = async |task: Option<(&mut mpsc::UnboundedReceiver<SleepNotice>, &str)>| {
            prepare.send((true, Instant::now())).unwrap();
            if let Some((task, mac)) = task {
                let notice = task.recv().await.unwrap();
                notice.slept(mac);
                notice.ack();
            }
            settle().await;
            prepare.send((false, Instant::now())).unwrap();
            settle().await;
        };
        // J sleeps and the wake reconnects it.
        sleep(Some((&mut task, J))).await;
        tokio::time::advance(WAKE_RECONNECT_FIRST).await;
        settle().await;
        // K sleeps, and its task ends.
        sleep(Some((&mut task, K))).await;
        drop(task);
        // A second sleep before K's first attempt; nothing records it again.
        sleep(None).await;
        // K once, on the schedule from the last wake; J not again.
        let macs = || -> Vec<String> { link.connects().into_iter().map(|(m, _)| m).collect() };
        tokio::time::advance(WAKE_RECONNECT_FIRST - Duration::from_millis(1)).await;
        settle().await;
        assert_eq!(macs(), [J]);
        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        assert_eq!(macs(), [J, K]);
        tokio::time::advance(WAKE_RECONNECT_MAX * WAKE_RECONNECT_ATTEMPTS).await;
        settle().await;
        assert_eq!(macs(), [J, K]);
    }
}
