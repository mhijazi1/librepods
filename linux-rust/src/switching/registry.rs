//! One handoff task per AirPods MAC, its stop and the exit's wait for A2DP hold releases.

use super::SHUTDOWN_RELEASE_WAIT;
use super::shell::{Shell, run_handoff};
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use log::info;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex as StdMutex};
use tokio::sync::oneshot;
use tokio::task::AbortHandle;

pub(super) struct HandoffTask {
    id: u64,
    handle: AbortHandle,
    released: Released,
}

/// A stopped task's queued sink work (#294, #311): its settle, sent by the
/// task's `Shell` as it drops.
type Released = oneshot::Receiver<BoxFuture<'static, ()>>;

/// Stopped tasks' releases, each bounded by `SHUTDOWN_RELEASE_WAIT` from its
/// stop.
pub(super) type Pending = Shared<BoxFuture<'static, ()>>;

#[derive(Default)]
pub(super) struct Registry {
    pub(super) tasks: HashMap<String, HandoffTask>,
    /// Each MAC's pending releases, kept outside the abortable tasks so a
    /// replacement replaced in turn still hands them on (#294).
    pub(super) releases: HashMap<String, Pending>,
    /// Set by the exit's shutdown, which every exit request awaits; no task
    /// starts after it.
    shutdown: Option<Pending>,
}

impl Registry {
    /// Abort this MAC's task, if one runs, and add its release to the
    /// pending ones. Its drop releases its A2DP hold.
    fn stop(&mut self, mac: &str) {
        let Some(task) = self.tasks.remove(mac) else {
            return;
        };
        task.handle.abort();
        let release = tokio::time::timeout(SHUTDOWN_RELEASE_WAIT, async {
            if let Ok(settled) = task.released.await {
                settled.await;
            }
        });
        let earlier = self.releases.remove(mac);
        let pending = async move {
            let _ = release.await;
            if let Some(earlier) = earlier {
                earlier.await;
            }
        };
        self.releases
            .insert(mac.to_string(), pending.boxed().shared());
    }

    /// Stop every task once; the result settles with all pending releases.
    fn shutdown(&mut self) -> Pending {
        if let Some(shutdown) = &self.shutdown {
            return shutdown.clone();
        }
        let macs: Vec<_> = self.tasks.keys().cloned().collect();
        for mac in macs {
            self.stop(&mac);
        }
        let all = futures::future::join_all(self.releases.drain().map(|(_, pending)| pending));
        let shutdown = all.map(drop).boxed().shared();
        self.shutdown = Some(shutdown.clone());
        shutdown
    }
}

// One handoff task per AirPods MAC. A replacement AirPodsDevice for the same
// MAC aborts its predecessor, like the legacy PLAYBACK_LISTENERS.
pub(super) static HANDOFF_TASKS: LazyLock<StdMutex<Registry>> = LazyLock::new(StdMutex::default);
static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(0);

// Whether Linux was the last owner of each AirPods MAC (#16). Outlives the
// handoff task, which a reconnect replaces; forgotten when the app quits.
static LAST_OWNER_LINUX: LazyLock<StdMutex<HashMap<String, bool>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

pub(crate) fn last_owner_linux(mac: &str) -> bool {
    LAST_OWNER_LINUX.lock().unwrap().get(mac) == Some(&true)
}

pub(super) fn remember_last_owner_linux(mac: &str, value: bool) {
    LAST_OWNER_LINUX
        .lock()
        .unwrap()
        .insert(mac.to_string(), value);
}

/// Stop the handoff task of this AirPods MAC, if one runs. Its drop releases
/// its A2DP hold (#294).
pub(crate) fn stop_handoff_task(mac: &str) {
    HANDOFF_TASKS.lock().unwrap().stop(mac);
}

/// At exit: stop every handoff task, then wait, at most
/// `SHUTDOWN_RELEASE_WAIT`, for the held ones' releases to settle (#294).
/// Every call waits for the same shutdown.
pub(crate) async fn stop_all_handoff_tasks() {
    shutdown(&HANDOFF_TASKS).await;
}

pub(super) fn shutdown(registry: &StdMutex<Registry>) -> Pending {
    registry
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .shutdown()
}

/// The running handoff task of this AirPods MAC.
#[cfg(test)]
pub(crate) fn handoff_task(mac: &str) -> Option<AbortHandle> {
    HANDOFF_TASKS
        .lock()
        .unwrap()
        .tasks
        .get(mac)
        .map(|task| task.handle.clone())
}

/// Ends a finished task's registry entry, unless a replacement already
/// took the MAC, as `Registry::stop` does, so a task that ended by itself
/// (a Disconnected or Quit step) still hands its release's settle on. Runs
/// on return, abort and panic alike.
struct Unregister {
    registry: &'static StdMutex<Registry>,
    mac: String,
    id: u64,
}

impl Drop for Unregister {
    fn drop(&mut self) {
        let mut registry = self.registry.lock().unwrap_or_else(|e| e.into_inner());
        if registry
            .tasks
            .get(&self.mac)
            .is_some_and(|task| task.id == self.id)
        {
            registry.stop(&self.mac);
        }
    }
}

pub(super) fn spawn_handoff_task(
    registry: &'static StdMutex<Registry>,
    mac: String,
    mut shell: Shell,
) {
    // Held across the spawn, so the new task cannot unregister before it is
    // registered.
    let mut tasks = registry.lock().unwrap();
    if tasks.shutdown.is_some() {
        info!("Exiting: no handoff task for {mac}");
        return;
    }
    if tasks.tasks.contains_key(&mac) {
        info!("Stopping the previous handoff task for {mac}");
    }
    tasks.stop(&mac);
    let previous = tasks.releases.get(&mac).cloned();
    let id = NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed);
    let (release_tx, released) = oneshot::channel();
    shell.released = Some(release_tx);
    let unregister = Unregister {
        registry,
        mac: mac.clone(),
        id,
    };
    let task = tokio::spawn(async move {
        let _unregister = unregister;
        run_handoff(shell, previous).await;
    });
    tasks.tasks.insert(
        mac,
        HandoffTask {
            id,
            handle: task.abort_handle(),
            released,
        },
    );
}
