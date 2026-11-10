//! Switching notifications, as the Mac posts them (#17, spike #259): "<AirPods
//! name> / moved to <taker>" with Undo when another host takes the AirPods
//! while Linux plays, and "Connected" when a take of Linux's is
//! acknowledged. Posted over org.freedesktop.Notifications.

use dbus::arg::PropMap;
use dbus::blocking::Connection;
use dbus::message::MatchRule;
use log::warn;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// What to post. `MovedTo` carries the taker's name, if known.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Notice {
    MovedTo(Option<String>),
    Connected,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Content {
    pub(crate) title: String,
    pub(crate) body: String,
    /// The one action's label, if any.
    pub(crate) action: Option<&'static str>,
    pub(crate) expire_ms: i32,
}

/// The notification for `notice` about the AirPods named `airpods`. The
/// Mac's timeouts: 20 s for its ReverseRoute banner, 4 s for Connected.
pub(crate) fn content(notice: &Notice, airpods: &str) -> Content {
    let title = if airpods.is_empty() {
        "AirPods"
    } else {
        airpods
    }
    .to_string();
    match notice {
        Notice::MovedTo(taker) => Content {
            title,
            body: format!(
                "moved to {}",
                taker
                    .as_deref()
                    .filter(|n| !n.is_empty())
                    .unwrap_or("another device")
            ),
            action: Some("Undo"),
            expire_ms: 20_000,
        },
        Notice::Connected => Content {
            title,
            body: "Connected".to_string(),
            action: None,
            expire_ms: 4_000,
        },
    }
}

/// Posts notices; Undo clicks go wherever the notifier was built to send
/// them.
pub(crate) trait Notifier: Send + Sync {
    fn post(&self, notice: Notice);
}

impl<F: Fn(Notice) + Send + Sync> Notifier for F {
    fn post(&self, notice: Notice) {
        self(notice)
    }
}

/// Post `notice` unless the `switchingNotifications` preference is off.
pub(crate) fn post(notifier: &dyn Notifier, enabled: bool, notice: Notice) {
    if enabled {
        notifier.post(notice);
    }
}

const IFACE: &str = "org.freedesktop.Notifications";
const UNDO_KEY: &str = "undo";
const CALL_TIMEOUT: Duration = Duration::from_secs(5);

/// The desktop's notification server. The title is the AirPods' BlueZ
/// alias, read when posting (#17 Q13); an Undo click sends on `undo`.
pub(crate) struct Desktop {
    pub(crate) airpods_mac: String,
    pub(crate) undo: mpsc::UnboundedSender<()>,
}

impl Notifier for Desktop {
    fn post(&self, notice: Notice) {
        let (mac, undo) = (self.airpods_mac.clone(), self.undo.clone());
        tokio::spawn(async move {
            let airpods = alias(&mac).await.unwrap_or_default();
            let content = content(&notice, &airpods);
            match tokio::task::spawn_blocking(move || show(&content, &undo)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!("Notification not shown: {e}"),
                Err(e) => warn!("Notification task failed: {e}"),
            }
        });
    }
}

async fn alias(mac: &str) -> Option<String> {
    let session = bluer::Session::new().await.ok()?;
    let adapter = session.default_adapter().await.ok()?;
    adapter.device(mac.parse().ok()?).ok()?.alias().await.ok()
}

/// Show `content`; with an action, wait until it is clicked (sending on
/// `undo`), closed or expired, and close it on expiry so no dead button
/// stays.
fn show(content: &Content, undo: &mpsc::UnboundedSender<()>) -> Result<(), dbus::Error> {
    let bus = Connection::new_session()?;
    let proxy = bus.with_proxy(IFACE, "/org/freedesktop/Notifications", CALL_TIMEOUT);
    let id = Arc::new(AtomicU32::new(0));
    let done = Arc::new(AtomicBool::new(false));
    if content.action.is_some() {
        // Matched before Notify, so an instant click is not missed; the
        // signals queue until `process` runs, after the id is known.
        let (clicked, finished, undo) = (id.clone(), done.clone(), undo.clone());
        bus.add_match(
            MatchRule::new_signal(IFACE, "ActionInvoked"),
            move |(n, key): (u32, String), _, _| {
                if n == clicked.load(Ordering::SeqCst) && key == UNDO_KEY {
                    let _ = undo.send(());
                    finished.store(true, Ordering::SeqCst);
                }
                true
            },
        )?;
        let (closed, finished) = (id.clone(), done.clone());
        bus.add_match(
            MatchRule::new_signal(IFACE, "NotificationClosed"),
            move |(n, _): (u32, u32), _, _| {
                if n == closed.load(Ordering::SeqCst) {
                    finished.store(true, Ordering::SeqCst);
                }
                true
            },
        )?;
    }
    let actions: Vec<&str> = content
        .action
        .iter()
        .flat_map(|label| [UNDO_KEY, *label])
        .collect();
    let (posted,): (u32,) = proxy.method_call(
        IFACE,
        "Notify",
        (
            "LibrePods",
            0u32,
            "",
            content.title.as_str(),
            content.body.as_str(),
            actions,
            PropMap::new(),
            content.expire_ms,
        ),
    )?;
    if content.action.is_none() {
        return Ok(());
    }
    id.store(posted, Ordering::SeqCst);
    let expiry = Instant::now() + Duration::from_millis(content.expire_ms as u64);
    while !done.load(Ordering::SeqCst) {
        let left = expiry.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return proxy.method_call(IFACE, "CloseNotification", (posted,));
        }
        bus.process(left)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn moved_to_names_the_taker_with_undo() {
        let notice = Notice::MovedTo(Some("Mo's iPhone".to_string()));
        assert_eq!(
            content(&notice, "Mo's AirPods Pro"),
            Content {
                title: "Mo's AirPods Pro".to_string(),
                body: "moved to Mo's iPhone".to_string(),
                action: Some("Undo"),
                expire_ms: 20_000,
            }
        );
    }

    #[test]
    fn moved_to_falls_back_to_another_device() {
        for taker in [None, Some(String::new())] {
            let content = content(&Notice::MovedTo(taker), "Mo's AirPods Pro");
            assert_eq!(content.body, "moved to another device");
            assert_eq!(content.action, Some("Undo"));
        }
        let content = content(&Notice::MovedTo(Some("iPhone".to_string())), "");
        assert_eq!(content.title, "AirPods");
    }

    #[test]
    fn linux_take_is_a_connected_notice() {
        assert_eq!(
            content(&Notice::Connected, "Mo's AirPods Pro"),
            Content {
                title: "Mo's AirPods Pro".to_string(),
                body: "Connected".to_string(),
                action: None,
                expire_ms: 4_000,
            }
        );
        assert_eq!(content(&Notice::Connected, "").title, "AirPods");
    }

    #[test]
    fn preference_off_posts_nothing() {
        let posted = Mutex::new(Vec::new());
        let fake = |notice| posted.lock().unwrap().push(notice);
        let moved = || Notice::MovedTo(Some("iPhone".to_string()));
        post(&fake, false, moved());
        post(&fake, false, Notice::Connected);
        assert_eq!(*posted.lock().unwrap(), []);
        post(&fake, true, moved());
        post(&fake, true, Notice::Connected);
        assert_eq!(*posted.lock().unwrap(), [moved(), Notice::Connected]);
    }
}
