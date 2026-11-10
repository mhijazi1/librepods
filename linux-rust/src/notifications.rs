//! Switching notifications, as the Mac posts them (#17, spike #259): "<AirPods
//! name> / moved to <taker>" with Undo when another host takes the AirPods
//! while Linux plays, and "Connected" when a take of Linux's is
//! acknowledged. Posted over org.freedesktop.Notifications.

use dbus::Message;
use dbus::arg::PropMap;
use dbus::blocking::{BlockingSender, Connection};
use dbus::message::MatchRule;
use log::warn;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// What to post. `MovedTo` carries its yield's id, which its Undo sends
/// back, and the taker's name, if known.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Notice {
    MovedTo { id: u64, taker: Option<String> },
    Connected,
}

#[derive(Debug, PartialEq, Eq)]
struct Content {
    title: String,
    body: String,
    /// The one action's label, if any.
    action: Option<&'static str>,
    expire_ms: i32,
}

/// The notification for `notice` about the AirPods named `airpods`. The
/// Mac's timeouts: 20 s for its ReverseRoute banner, 4 s for Connected.
fn content(notice: &Notice, airpods: &str) -> Content {
    let title = if airpods.is_empty() {
        "AirPods"
    } else {
        airpods
    }
    .to_string();
    match notice {
        Notice::MovedTo { taker, .. } => Content {
            title,
            // The body may be markup, and the name is the peer's.
            body: escape(&format!(
                "moved to {}",
                taker
                    .as_deref()
                    .filter(|n| !n.is_empty())
                    .unwrap_or("another device")
            )),
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

/// `text` safe in a body that the server may read as markup.
fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
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
const PATH: &str = "/org/freedesktop/Notifications";
const UNDO_KEY: &str = "undo";
const CALL_TIMEOUT: Duration = Duration::from_secs(5);

/// The desktop's notification server. The title is the AirPods' BlueZ
/// alias, read when posting (#17 Q13); an Undo click sends its notice's
/// yield id on `undo`, and the core ignores an id no longer current.
pub(crate) struct Desktop {
    airpods_mac: String,
    undo: mpsc::UnboundedSender<u64>,
}

impl Desktop {
    pub(crate) fn new(airpods_mac: String, undo: mpsc::UnboundedSender<u64>) -> Self {
        Desktop { airpods_mac, undo }
    }
}

impl Notifier for Desktop {
    fn post(&self, notice: Notice) {
        let (mac, undo) = (self.airpods_mac.clone(), self.undo.clone());
        // Only "moved to" has a button.
        let id = match &notice {
            Notice::MovedTo { id, .. } => *id,
            Notice::Connected => 0,
        };
        let on_undo = move || {
            let _ = undo.send(id);
        };
        tokio::spawn(async move {
            let airpods = alias(&mac).await.unwrap_or_default();
            let content = content(&notice, &airpods);
            match tokio::task::spawn_blocking(move || show(&content, on_undo)).await {
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

/// Show `content`; with an action, wait until it is clicked (calling
/// `on_undo`), closed or expired, and close it on expiry so no dead button
/// stays.
fn show(content: &Content, on_undo: impl Fn() + Send + 'static) -> Result<(), dbus::Error> {
    let bus = Connection::new_session()?;
    let actions: Vec<&str> = content
        .action
        .iter()
        .flat_map(|label| [UNDO_KEY, *label])
        .collect();
    let mut notify = Message::new_method_call(IFACE, PATH, IFACE, "Notify")
        .map_err(|e| dbus::Error::new_failed(&e))?;
    notify.append_all((
        "LibrePods",
        0u32,
        "",
        content.title.as_str(),
        content.body.as_str(),
        actions,
        PropMap::new(),
        content.expire_ms,
    ));
    let reply = bus.send_with_reply_and_block(notify, CALL_TIMEOUT)?;
    let id: u32 = reply.read1()?;
    // The unique name that posted it: only its signals count, so another
    // client can't fake a click. Matched after the reply, before the
    // banner can have been clicked.
    let Some(server) = reply.sender().map(|s| s.to_string()) else {
        return Ok(());
    };
    if content.action.is_none() {
        return Ok(());
    }
    let done = Arc::new(AtomicBool::new(false));
    let rule = |member: &'static str| {
        MatchRule::new_signal(IFACE, member)
            .with_sender(server.clone())
            .with_path(PATH)
    };
    let (owner, finished) = (server.clone(), done.clone());
    bus.add_match(rule("ActionInvoked"), move |_: (), _, msg: &Message| {
        if clicked_undo(msg, &owner, id) {
            on_undo();
            finished.store(true, Ordering::SeqCst);
        }
        true
    })?;
    let (owner, finished) = (server.clone(), done.clone());
    bus.add_match(
        rule("NotificationClosed"),
        move |_: (), _, msg: &Message| {
            if from_server(msg, &owner, id) {
                finished.store(true, Ordering::SeqCst);
            }
            true
        },
    )?;
    let expiry = Instant::now() + Duration::from_millis(content.expire_ms as u64);
    while !done.load(Ordering::SeqCst) {
        let left = expiry.saturating_duration_since(Instant::now());
        if left.is_zero() {
            let server = bus.with_proxy(server.as_str(), PATH, CALL_TIMEOUT);
            return server.method_call(IFACE, "CloseNotification", (id,));
        }
        bus.process(left)?;
    }
    Ok(())
}

/// A signal about notice `id` (its first argument) from `server`, the
/// unique name that posted it, at the spec's path. Anyone on the session
/// bus can send the same signal, and a later owner of the well-known name
/// is not `server`.
fn from_server(signal: &Message, server: &str, id: u32) -> bool {
    signal.sender().as_deref() == Some(server)
        && signal.path().as_deref() == Some(PATH)
        && signal.read1::<u32>().is_ok_and(|n| n == id)
}

/// An `ActionInvoked` that is the Undo click on notice `id` from `server`.
fn clicked_undo(signal: &Message, server: &str, id: u32) -> bool {
    from_server(signal, server, id)
        && signal
            .read2::<u32, &str>()
            .is_ok_and(|(_, key)| key == UNDO_KEY)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn moved_to(taker: Option<&str>) -> Notice {
        Notice::MovedTo {
            id: 1,
            taker: taker.map(str::to_string),
        }
    }

    #[test]
    fn moved_to_names_the_taker_with_undo() {
        assert_eq!(
            content(&moved_to(Some("Mo's iPhone")), "Mo's AirPods Pro"),
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
        for taker in [None, Some("")] {
            let content = content(&moved_to(taker), "Mo's AirPods Pro");
            assert_eq!(content.body, "moved to another device");
            assert_eq!(content.action, Some("Undo"));
        }
        assert_eq!(content(&moved_to(Some("iPhone")), "").title, "AirPods");
    }

    #[test]
    fn moved_to_escapes_the_takers_markup() {
        let content = content(&moved_to(Some(r#"<a href="x">&"#)), "<b>AirPods</b>");
        assert_eq!(content.body, r#"moved to &lt;a href="x"&gt;&amp;"#);
        // The summary is plain text.
        assert_eq!(content.title, "<b>AirPods</b>");
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
        let moved = || moved_to(Some("iPhone"));
        post(&fake, false, moved());
        post(&fake, false, Notice::Connected);
        assert_eq!(*posted.lock().unwrap(), []);
        post(&fake, true, moved());
        post(&fake, true, Notice::Connected);
        assert_eq!(*posted.lock().unwrap(), [moved(), Notice::Connected]);
    }

    const SERVER: &str = ":1.42";

    /// `member` about notice 7 from `sender` at `path`, as the bus
    /// delivers it.
    fn signal(sender: &str, path: &str, member: &str, key: Option<&str>) -> Message {
        let signal = Message::new_signal(path, IFACE, member).expect("signal");
        let mut signal = match key {
            Some(key) => signal.append2(7u32, key),
            None => signal.append2(7u32, 2u32),
        };
        signal.set_sender(Some(sender.into()));
        signal
    }

    #[test]
    fn only_the_posting_server_can_click_or_close() {
        let click = |sender, path, key| {
            clicked_undo(&signal(sender, path, "ActionInvoked", Some(key)), SERVER, 7)
        };
        assert!(click(SERVER, PATH, UNDO_KEY));
        // Another client sending the same signal.
        assert!(!click(":1.99", PATH, UNDO_KEY));
        assert!(!click(SERVER, "/elsewhere", UNDO_KEY));
        assert!(!click(SERVER, PATH, "default"));
        // Another notice.
        let undo = signal(SERVER, PATH, "ActionInvoked", Some(UNDO_KEY));
        assert!(!clicked_undo(&undo, SERVER, 8));
        let closed =
            |sender| from_server(&signal(sender, PATH, "NotificationClosed", None), SERVER, 7);
        assert!(closed(SERVER));
        assert!(!closed(":1.99"));
    }
}
