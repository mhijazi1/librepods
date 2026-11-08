//! Exports one AirPods battery percentage to the desktop through BlueZ's
//! `org.bluez.BatteryProviderManager1` / `org.bluez.BatteryProvider1` API, so
//! that BlueZ's `org.bluez.Battery1` (and therefore UPower and desktop bars)
//! show it.
//!
//! The module has three layers:
//! - the pure mapping: `BudLevels` (per-component state) and
//!   `export_percentage`, which picks the single percentage Battery1 carries;
//! - the exporter state machine `BatteryExporter`, which drives any
//!   `BatterySink` (register once, set on change, remove when nothing is
//!   known) and never panics on sink errors;
//! - the real sink `BluezBatteryProvider`, which serves the provider objects
//!   on the system bus from a dedicated thread, so the tokio runtime never
//!   blocks on D-Bus.

use crate::bluetooth::aacp::{BatteryComponent, BatteryInfo, BatteryStatus};
use dbus::arg::{PropMap, RefArg, Variant};
use dbus::blocking::Connection;
use dbus::message::MessageType;
use dbus::strings::ErrorName;
use dbus::{Message, Path};
use log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::mpsc;
use std::time::Duration;

/// Object path under which the provider's batteries live. BlueZ reads them
/// through the `org.freedesktop.DBus.ObjectManager` served at this path.
pub const PROVIDER_ROOT: &str = "/org/librepods/battery";
/// `Source` property of the exported battery.
pub const PROVIDER_SOURCE: &str = "LibrePods";

const BLUEZ_SERVICE: &str = "org.bluez";
const PROVIDER_MANAGER_IFACE: &str = "org.bluez.BatteryProviderManager1";
const PROVIDER_IFACE: &str = "org.bluez.BatteryProvider1";
const OBJECT_MANAGER_IFACE: &str = "org.freedesktop.DBus.ObjectManager";
const PROPERTIES_IFACE: &str = "org.freedesktop.DBus.Properties";
const INTROSPECTABLE_IFACE: &str = "org.freedesktop.DBus.Introspectable";
const PEER_IFACE: &str = "org.freedesktop.DBus.Peer";

/// Timeout of the (Un)RegisterBatteryProvider calls to bluetoothd.
const BLUEZ_CALL_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the provider thread waits for bus traffic before it looks at its
/// command queue again.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The latest known battery entry per component. The case is not tracked,
/// because it never contributes to the exported percentage.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BudLevels {
    pub left: Option<BatteryInfo>,
    pub right: Option<BatteryInfo>,
    pub headphone: Option<BatteryInfo>,
}

impl BudLevels {
    /// Merges one AACP battery report: an entry present in the update replaces
    /// that component, an absent component keeps its previous value, and the
    /// Case entry is ignored.
    pub fn apply(&mut self, infos: &[BatteryInfo]) {
        for info in infos {
            match info.component {
                BatteryComponent::Left => self.left = Some(info.clone()),
                BatteryComponent::Right => self.right = Some(info.clone()),
                BatteryComponent::Headphone => self.headphone = Some(info.clone()),
                BatteryComponent::Case => {}
            }
        }
    }
}

/// A component is known when it is present and not `Disconnected`.
fn known(info: &Option<BatteryInfo>) -> Option<&BatteryInfo> {
    info.as_ref()
        .filter(|b| b.status != BatteryStatus::Disconnected)
}

/// The single percentage exported to BlueZ (Battery1 carries one value).
///
/// With both buds known: if exactly one is charging (in the case) and the other
/// is not, the worn (not charging) bud; otherwise the lower bud. With one bud
/// known, that bud. With no bud known, the headphone level (single-battery
/// devices). Otherwise nothing. The case never contributes. Clamped to 100.
pub fn export_percentage(levels: &BudLevels) -> Option<u8> {
    use BatteryStatus::{Charging, NotCharging};
    let level = match (known(&levels.left), known(&levels.right)) {
        (Some(l), Some(r)) => match (l.status, r.status) {
            (Charging, NotCharging) => r.level,
            (NotCharging, Charging) => l.level,
            _ => l.level.min(r.level),
        },
        (Some(bud), None) | (None, Some(bud)) => bud.level,
        (None, None) => known(&levels.headphone)?.level,
    };
    Some(level.min(100))
}

/// Where an exported percentage goes. Implemented by the BlueZ provider and by
/// the recording fake in the tests. Calls must not block for long: the real
/// sink only blocks in `register` (one local D-Bus round trip).
pub trait BatterySink {
    /// Registers the provider; called at most once per exporter.
    fn register(&mut self) -> Result<(), String>;
    /// Exports (or updates) the percentage.
    fn set(&mut self, pct: u8) -> Result<(), String>;
    /// Withdraws the exported battery.
    fn remove(&mut self) -> Result<(), String>;
}

/// Lifecycle of a `BatteryExporter`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExporterState {
    /// Nothing has been exported yet, the sink is not registered.
    Unregistered,
    /// The sink is registered; a percentage may or may not be exported.
    Registered,
    /// Registration failed; the exporter makes no further sink calls.
    Disabled,
}

/// Feeds AACP battery reports through `export_percentage` into a sink.
pub struct BatteryExporter<S: BatterySink> {
    sink: S,
    levels: BudLevels,
    state: ExporterState,
    exported: Option<u8>,
}

impl<S: BatterySink> BatteryExporter<S> {
    pub fn new(sink: S) -> Self {
        Self {
            sink,
            levels: BudLevels::default(),
            state: ExporterState::Unregistered,
            exported: None,
        }
    }

    #[cfg(test)]
    pub fn state(&self) -> ExporterState {
        self.state
    }

    /// Applies one battery report and exports the resulting percentage. The
    /// first percentage registers the sink once; an unchanged percentage
    /// makes no call; losing every known level removes the battery. A failed
    /// registration logs one warning and disables the exporter. Sink errors
    /// never propagate.
    pub fn update(&mut self, infos: &[BatteryInfo]) {
        self.levels.apply(infos);
        if self.state == ExporterState::Disabled {
            return;
        }
        let pct = export_percentage(&self.levels);
        if pct == self.exported {
            return;
        }
        match pct {
            Some(p) => {
                if self.state == ExporterState::Unregistered {
                    if let Err(e) = self.sink.register() {
                        warn!("Desktop battery export disabled: {e}");
                        self.state = ExporterState::Disabled;
                        return;
                    }
                    info!("Exporting AirPods battery to BlueZ");
                    self.state = ExporterState::Registered;
                }
                match self.sink.set(p) {
                    Ok(()) => self.exported = Some(p),
                    Err(e) => warn!("Failed to export battery level {p}%: {e}"),
                }
            }
            None => match self.sink.remove() {
                Ok(()) => self.exported = None,
                Err(e) => warn!("Failed to withdraw the exported battery: {e}"),
            },
        }
    }
}

impl<S: BatterySink> Drop for BatteryExporter<S> {
    /// Withdraws the battery so the desktop does not keep a stale value after
    /// the device handler ends.
    fn drop(&mut self) {
        if self.exported.take().is_some() {
            if let Err(e) = self.sink.remove() {
                warn!("Failed to withdraw the exported battery: {e}");
            }
        }
    }
}

/// `dev_AA_BB_CC_DD_EE_FF`, BlueZ's object name for a device address.
fn device_node(mac: &str) -> String {
    format!("dev_{}", mac.to_uppercase().replace(':', "_"))
}

/// BlueZ's object path of a device: `/org/bluez/<adapter>/dev_AA_BB_..`.
pub fn device_object_path(adapter: &str, mac: &str) -> String {
    format!("/org/bluez/{adapter}/{}", device_node(mac))
}

/// The provider's battery object for a device, a child of `PROVIDER_ROOT`.
pub fn battery_object_path(mac: &str) -> String {
    format!("{PROVIDER_ROOT}/{}", device_node(mac))
}

/// Commands from the sink to its D-Bus thread.
enum Command {
    Set(u8),
    Remove,
}

/// The real sink: serves one `org.bluez.BatteryProvider1` object under
/// `PROVIDER_ROOT` on the system bus and registers it with the adapter's
/// `BatteryProviderManager1`. All D-Bus traffic runs on a dedicated thread;
/// `set`/`remove` only queue a command. Dropping the sink stops the thread,
/// which unregisters the provider (BlueZ also drops a provider's batteries
/// when its connection goes away).
pub struct BluezBatteryProvider {
    adapter: String,
    mac: String,
    commands: Option<mpsc::Sender<Command>>,
}

impl BluezBatteryProvider {
    pub fn new(adapter: &str, mac: &str) -> Self {
        Self {
            adapter: adapter.to_string(),
            mac: mac.to_string(),
            commands: None,
        }
    }

    fn send(&self, command: Command) -> Result<(), String> {
        let commands = self
            .commands
            .as_ref()
            .ok_or_else(|| "battery provider is not registered".to_string())?;
        commands
            .send(command)
            .map_err(|_| "battery provider thread has stopped".to_string())
    }
}

impl BatterySink for BluezBatteryProvider {
    /// Starts the provider thread and waits for its `RegisterBatteryProvider`
    /// answer. Blocks for one local D-Bus round trip; callers on the tokio
    /// runtime run it through `spawn_blocking`.
    fn register(&mut self) -> Result<(), String> {
        if self.commands.is_some() {
            return Ok(());
        }
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (commands, command_rx) = mpsc::channel();
        let (adapter, mac) = (self.adapter.clone(), self.mac.clone());
        std::thread::Builder::new()
            .name("battery-provider".into())
            .spawn(move || match ProviderThread::start(&adapter, &mac) {
                Ok(thread) => {
                    let _ = ready_tx.send(Ok(()));
                    thread.run(command_rx);
                }
                Err(e) => {
                    let _ = ready_tx.send(Err(e));
                }
            })
            .map_err(|e| format!("cannot start the battery provider thread: {e}"))?;
        match ready_rx.recv() {
            Ok(Ok(())) => {
                self.commands = Some(commands);
                Ok(())
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err("battery provider thread exited during setup".to_string()),
        }
    }

    fn set(&mut self, pct: u8) -> Result<(), String> {
        self.send(Command::Set(pct))
    }

    fn remove(&mut self) -> Result<(), String> {
        self.send(Command::Remove)
    }
}

/// The provider's objects and the exported value, with the D-Bus messages
/// they produce. Holds no connection, so the replies and signals are tested
/// offline.
struct ProviderObjects {
    root: Path<'static>,
    battery: Path<'static>,
    device: Path<'static>,
    percentage: Option<u8>,
}

impl ProviderObjects {
    fn new(adapter: &str, mac: &str) -> Result<Self, String> {
        let path = |p: String| Path::new(p.clone()).map_err(|e| format!("invalid object path {p}: {e}"));
        Ok(Self {
            root: path(PROVIDER_ROOT.to_string())?,
            battery: path(battery_object_path(mac))?,
            device: path(device_object_path(adapter, mac))?,
            percentage: None,
        })
    }

    fn properties(&self, pct: u8) -> PropMap {
        let mut props = PropMap::new();
        props.insert(
            "Device".into(),
            Variant(Box::new(self.device.clone()) as Box<dyn RefArg>),
        );
        props.insert("Percentage".into(), Variant(Box::new(pct) as Box<dyn RefArg>));
        props.insert(
            "Source".into(),
            Variant(Box::new(PROVIDER_SOURCE.to_string()) as Box<dyn RefArg>),
        );
        props
    }

    fn interfaces(&self, pct: u8) -> HashMap<String, PropMap> {
        HashMap::from([(PROVIDER_IFACE.to_string(), self.properties(pct))])
    }

    /// Applies a command and returns the signal announcing the change, if any.
    fn apply(&mut self, command: Command) -> Result<Option<Message>, String> {
        match command {
            Command::Set(pct) => match self.percentage.replace(pct) {
                None => Message::new_signal(&*self.root, OBJECT_MANAGER_IFACE, "InterfacesAdded")
                    .map(|m| Some(m.append2(self.battery.clone(), self.interfaces(pct)))),
                Some(old) if old == pct => Ok(None),
                Some(_) => {
                    let mut changed = PropMap::new();
                    changed.insert("Percentage".into(), Variant(Box::new(pct) as Box<dyn RefArg>));
                    Message::new_signal(&*self.battery, PROPERTIES_IFACE, "PropertiesChanged")
                        .map(|m| Some(m.append3(PROVIDER_IFACE, changed, Vec::<String>::new())))
                }
            },
            Command::Remove => match self.percentage.take() {
                None => Ok(None),
                Some(_) => Message::new_signal(&*self.root, OBJECT_MANAGER_IFACE, "InterfacesRemoved")
                    .map(|m| Some(m.append2(self.battery.clone(), vec![PROVIDER_IFACE.to_string()]))),
            },
        }
    }

    /// The reply to a method call on our objects, or `None` for anything that
    /// is not a method call (or a call without a serial, which libdbus cannot
    /// answer).
    fn reply(&self, call: &Message) -> Option<Message> {
        if call.msg_type() != MessageType::MethodCall || call.get_serial().is_none() {
            return None;
        }
        let iface = call.interface();
        let iface = iface.as_deref().unwrap_or("");
        if iface == PEER_IFACE {
            // Ping / GetMachineId.
            return dbus::channel::default_reply(call);
        }
        let path = call.path();
        let path = path.as_deref().unwrap_or("");
        let member = call.member();
        let member = member.as_deref().unwrap_or("");
        let on_root = path == &*self.root;
        let on_battery = path == &*self.battery && self.percentage.is_some();

        match (iface, member) {
            (OBJECT_MANAGER_IFACE, "GetManagedObjects") if on_root => {
                let mut objects: HashMap<Path<'static>, HashMap<String, PropMap>> = HashMap::new();
                if let Some(pct) = self.percentage {
                    objects.insert(self.battery.clone(), self.interfaces(pct));
                }
                Some(call.method_return().append1(objects))
            }
            (PROPERTIES_IFACE, "GetAll") if on_battery => {
                let props = match (call.read1::<&str>(), self.percentage) {
                    (Ok(PROVIDER_IFACE), Some(pct)) => self.properties(pct),
                    _ => PropMap::new(),
                };
                Some(call.method_return().append1(props))
            }
            (PROPERTIES_IFACE, "Get") if on_battery => {
                let value = match (call.read2::<&str, &str>(), self.percentage) {
                    (Ok((PROVIDER_IFACE, name)), Some(pct)) => {
                        self.properties(pct).remove(name)
                    }
                    _ => None,
                };
                Some(match value {
                    Some(v) => call.method_return().append1(v),
                    None => call.error(
                        &ErrorName::from("org.freedesktop.DBus.Error.InvalidArgs"),
                        c"No such property",
                    ),
                })
            }
            (INTROSPECTABLE_IFACE, "Introspect") if on_root || on_battery => {
                Some(call.method_return().append1(self.introspection(on_root)))
            }
            _ => Some(call.error(
                &ErrorName::from("org.freedesktop.DBus.Error.UnknownMethod"),
                c"Path, Interface, or Method does not exist",
            )),
        }
    }

    fn introspection(&self, root: bool) -> String {
        let body = if root {
            let child = self.battery.rsplit('/').next().unwrap_or_default();
            let node = if self.percentage.is_some() {
                format!("<node name=\"{child}\"/>")
            } else {
                String::new()
            };
            format!(
                "<interface name=\"{OBJECT_MANAGER_IFACE}\"><method name=\"GetManagedObjects\">\
                 <arg type=\"a{{oa{{sa{{sv}}}}}}\" direction=\"out\"/></method>\
                 <signal name=\"InterfacesAdded\"><arg type=\"o\"/><arg type=\"a{{sa{{sv}}}}\"/></signal>\
                 <signal name=\"InterfacesRemoved\"><arg type=\"o\"/><arg type=\"as\"/></signal>\
                 </interface>{node}"
            )
        } else {
            format!(
                "<interface name=\"{PROVIDER_IFACE}\">\
                 <property name=\"Device\" type=\"o\" access=\"read\"/>\
                 <property name=\"Percentage\" type=\"y\" access=\"read\"/>\
                 <property name=\"Source\" type=\"s\" access=\"read\"/>\
                 </interface>"
            )
        };
        format!(
            "<!DOCTYPE node PUBLIC \"-//freedesktop//DTD D-BUS Object Introspection 1.0//EN\" \
             \"http://www.freedesktop.org/standards/dbus/1.0/introspect.dtd\">\
             <node><interface name=\"{INTROSPECTABLE_IFACE}\"><method name=\"Introspect\">\
             <arg type=\"s\" direction=\"out\"/></method></interface>{body}</node>"
        )
    }
}

/// The provider's own system-bus connection, driven by one thread.
struct ProviderThread {
    conn: Connection,
    adapter_path: String,
    objects: ProviderObjects,
}

impl ProviderThread {
    /// Connects to the system bus and registers the provider root with the
    /// adapter's BatteryProviderManager1.
    fn start(adapter: &str, mac: &str) -> Result<Self, String> {
        let objects = ProviderObjects::new(adapter, mac)?;
        let adapter_path = format!("/org/bluez/{adapter}");
        let conn = Connection::new_system()
            .map_err(|e| format!("cannot connect to the system bus: {e}"))?;
        conn.with_proxy(BLUEZ_SERVICE, adapter_path.clone(), BLUEZ_CALL_TIMEOUT)
            .method_call::<(), _, _, _>(
                PROVIDER_MANAGER_IFACE,
                "RegisterBatteryProvider",
                (objects.root.clone(),),
            )
            .map_err(|e| {
                format!(
                    "RegisterBatteryProvider on {adapter_path} failed: {e} \
                     (BlueZ may need `Experimental = true` in the [General] section of main.conf)"
                )
            })?;
        debug!("Registered battery provider {} on {adapter_path}", PROVIDER_ROOT);
        Ok(Self {
            conn,
            adapter_path,
            objects,
        })
    }

    /// Answers BlueZ's calls and applies queued commands until the sink is
    /// dropped or the bus connection is lost.
    fn run(mut self, commands: mpsc::Receiver<Command>) {
        loop {
            loop {
                match commands.try_recv() {
                    Ok(command) => self.apply(command),
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        self.unregister();
                        return;
                    }
                }
            }
            match self.conn.channel().blocking_pop_message(POLL_INTERVAL) {
                Ok(Some(message)) => {
                    if let Some(reply) = self.objects.reply(&message) {
                        self.send(reply);
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    warn!("Battery provider lost the system bus: {e}");
                    return;
                }
            }
        }
    }

    fn apply(&mut self, command: Command) {
        match self.objects.apply(command) {
            Ok(Some(signal)) => self.send(signal),
            Ok(None) => {}
            Err(e) => warn!("Cannot build the battery provider signal: {e}"),
        }
    }

    fn send(&self, message: Message) {
        if self.conn.channel().send(message).is_err() {
            warn!("Battery provider could not send a D-Bus message");
        }
        self.conn.channel().flush();
    }

    fn unregister(&mut self) {
        if let Ok(Some(signal)) = self.objects.apply(Command::Remove) {
            self.send(signal);
        }
        let result = self
            .conn
            .with_proxy(BLUEZ_SERVICE, self.adapter_path.clone(), BLUEZ_CALL_TIMEOUT)
            .method_call::<(), _, _, _>(
                PROVIDER_MANAGER_IFACE,
                "UnregisterBatteryProvider",
                (self.objects.root.clone(),),
            );
        match result {
            Ok(()) => debug!("Unregistered battery provider on {}", self.adapter_path),
            Err(e) => debug!("UnregisterBatteryProvider failed: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use BatteryComponent::{Case, Headphone, Left, Right};
    use BatteryStatus::{Charging, Disconnected, NotCharging};

    fn b(component: BatteryComponent, level: u8, status: BatteryStatus) -> BatteryInfo {
        BatteryInfo {
            component,
            level,
            status,
        }
    }

    fn levels(infos: &[BatteryInfo]) -> BudLevels {
        let mut l = BudLevels::default();
        l.apply(infos);
        l
    }

    fn pct(infos: &[BatteryInfo]) -> Option<u8> {
        export_percentage(&levels(infos))
    }

    #[test]
    fn percentage_both_buds_worn_gives_minimum() {
        assert_eq!(pct(&[b(Left, 80, NotCharging), b(Right, 60, NotCharging)]), Some(60));
        assert_eq!(pct(&[b(Left, 30, NotCharging), b(Right, 90, NotCharging)]), Some(30));
        // Both charging (both in the case): still the lower one.
        assert_eq!(pct(&[b(Left, 40, Charging), b(Right, 70, Charging)]), Some(40));
    }

    #[test]
    fn percentage_bud_in_case_gives_worn_bud() {
        // The worn bud wins even when it is the higher one.
        assert_eq!(pct(&[b(Left, 20, Charging), b(Right, 70, NotCharging)]), Some(70));
        assert_eq!(pct(&[b(Left, 85, NotCharging), b(Right, 10, Charging)]), Some(85));
        // And when it is the lower one.
        assert_eq!(pct(&[b(Left, 90, Charging), b(Right, 50, NotCharging)]), Some(50));
    }

    #[test]
    fn percentage_one_bud_unknown_gives_other() {
        assert_eq!(pct(&[b(Left, 10, Disconnected), b(Right, 55, NotCharging)]), Some(55));
        assert_eq!(pct(&[b(Left, 45, Charging), b(Right, 5, Disconnected)]), Some(45));
        assert_eq!(pct(&[b(Right, 66, NotCharging)]), Some(66));
        assert_eq!(pct(&[b(Left, 77, Charging)]), Some(77));
    }

    #[test]
    fn percentage_nothing_known_gives_none() {
        assert_eq!(pct(&[]), None);
        assert_eq!(pct(&[b(Left, 50, Disconnected), b(Right, 50, Disconnected)]), None);
        assert_eq!(pct(&[b(Case, 90, Charging)]), None);
        assert_eq!(pct(&[b(Headphone, 70, Disconnected)]), None);
    }

    #[test]
    fn percentage_headphone_only_gives_its_level() {
        assert_eq!(pct(&[b(Headphone, 42, NotCharging)]), Some(42));
        assert_eq!(pct(&[b(Headphone, 88, Charging)]), Some(88));
        // A known bud takes precedence over the headphone entry.
        assert_eq!(pct(&[b(Headphone, 88, NotCharging), b(Left, 12, NotCharging)]), Some(12));
    }

    #[test]
    fn percentage_case_never_contributes() {
        let buds = [b(Left, 80, NotCharging), b(Right, 60, NotCharging)];
        for case in [b(Case, 5, NotCharging), b(Case, 100, Charging), b(Case, 0, Charging)] {
            let mut with_case = buds.to_vec();
            with_case.push(case.clone());
            assert_eq!(pct(&with_case), Some(60), "{case:?}");
        }
        let l = levels(&[b(Case, 30, NotCharging)]);
        assert_eq!(l, BudLevels::default());
    }

    #[test]
    fn percentage_apply_keeps_absent_components() {
        let mut l = levels(&[b(Left, 80, NotCharging), b(Right, 60, NotCharging)]);
        l.apply(&[b(Left, 90, NotCharging)]);
        assert_eq!(l.right, Some(b(Right, 60, NotCharging)));
        assert_eq!(l.left, Some(b(Left, 90, NotCharging)));
        assert_eq!(export_percentage(&l), Some(60));
        // A present Disconnected entry replaces the old value.
        l.apply(&[b(Right, 60, Disconnected)]);
        assert_eq!(export_percentage(&l), Some(90));
    }

    #[test]
    fn percentage_clamped_to_100() {
        assert_eq!(pct(&[b(Left, 150, NotCharging), b(Right, 120, NotCharging)]), Some(100));
        assert_eq!(pct(&[b(Headphone, 255, NotCharging)]), Some(100));
        assert_eq!(pct(&[b(Right, 100, NotCharging)]), Some(100));
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Call {
        Register,
        Set(u8),
        Remove,
    }

    /// Records every sink call into a log shared with the test, so calls made
    /// from `Drop` are visible too.
    struct FakeSink {
        calls: std::rc::Rc<std::cell::RefCell<Vec<Call>>>,
        fail_register: bool,
        fail_set: bool,
    }

    impl BatterySink for FakeSink {
        fn register(&mut self) -> Result<(), String> {
            self.calls.borrow_mut().push(Call::Register);
            if self.fail_register {
                Err("org.freedesktop.DBus.Error.UnknownMethod".into())
            } else {
                Ok(())
            }
        }
        fn set(&mut self, pct: u8) -> Result<(), String> {
            self.calls.borrow_mut().push(Call::Set(pct));
            if self.fail_set { Err("bus gone".into()) } else { Ok(()) }
        }
        fn remove(&mut self) -> Result<(), String> {
            self.calls.borrow_mut().push(Call::Remove);
            Ok(())
        }
    }

    fn exporter(
        fail_register: bool,
        fail_set: bool,
    ) -> (BatteryExporter<FakeSink>, std::rc::Rc<std::cell::RefCell<Vec<Call>>>) {
        let calls = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let sink = FakeSink {
            calls: calls.clone(),
            fail_register,
            fail_set,
        };
        (BatteryExporter::new(sink), calls)
    }

    #[test]
    fn exporter_sequence_registers_once_and_follows_levels() {
        let (mut ex, calls) = exporter(false, false);
        assert_eq!(ex.state(), ExporterState::Unregistered);
        ex.update(&[b(Left, 80, NotCharging), b(Right, 60, NotCharging)]);
        ex.update(&[b(Left, 80, NotCharging), b(Right, 60, NotCharging)]);
        ex.update(&[b(Left, 50, NotCharging)]);
        ex.update(&[b(Left, 50, Disconnected), b(Right, 60, Disconnected)]);
        ex.update(&[b(Right, 40, NotCharging)]);
        assert_eq!(ex.state(), ExporterState::Registered);
        assert_eq!(
            *calls.borrow(),
            [Call::Register, Call::Set(60), Call::Set(50), Call::Remove, Call::Set(40)]
        );
    }

    #[test]
    fn exporter_no_call_before_anything_is_known() {
        let (mut ex, calls) = exporter(false, false);
        ex.update(&[b(Case, 90, Charging)]);
        ex.update(&[b(Left, 50, Disconnected)]);
        drop(ex);
        assert!(calls.borrow().is_empty());
    }

    #[test]
    fn exporter_drop_removes_exported_battery() {
        let (mut ex, calls) = exporter(false, false);
        ex.update(&[b(Headphone, 70, NotCharging)]);
        drop(ex);
        assert_eq!(*calls.borrow(), [Call::Register, Call::Set(70), Call::Remove]);

        // Nothing exported at drop time: no trailing remove.
        let (mut ex, calls) = exporter(false, false);
        ex.update(&[b(Headphone, 70, NotCharging)]);
        ex.update(&[b(Headphone, 70, Disconnected)]);
        drop(ex);
        assert_eq!(*calls.borrow(), [Call::Register, Call::Set(70), Call::Remove]);
    }

    #[test]
    fn exporter_register_failure_disables() {
        let (mut ex, calls) = exporter(true, false);
        ex.update(&[b(Left, 80, NotCharging), b(Right, 60, NotCharging)]);
        ex.update(&[b(Left, 50, NotCharging)]);
        ex.update(&[b(Right, 20, Charging)]);
        assert_eq!(ex.state(), ExporterState::Disabled);
        drop(ex);
        assert_eq!(*calls.borrow(), [Call::Register]);
    }

    #[test]
    fn exporter_set_failure_keeps_running() {
        let (mut ex, calls) = exporter(false, true);
        ex.update(&[b(Left, 80, NotCharging)]);
        // The failed value was not exported, so the same value is retried.
        ex.update(&[b(Left, 80, NotCharging)]);
        assert_eq!(ex.state(), ExporterState::Registered);
        drop(ex);
        assert_eq!(*calls.borrow(), [Call::Register, Call::Set(80), Call::Set(80)]);
    }

    #[test]
    fn path_device_and_battery_objects() {
        assert_eq!(
            device_object_path("hci0", "AA:BB:CC:DD:EE:FF"),
            "/org/bluez/hci0/dev_AA_BB_CC_DD_EE_FF"
        );
        assert_eq!(
            device_object_path("hci1", "aa:bb:cc:dd:ee:0f"),
            "/org/bluez/hci1/dev_AA_BB_CC_DD_EE_0F"
        );
        let battery = battery_object_path("AA:BB:CC:DD:EE:FF");
        assert!(battery.starts_with(&format!("{PROVIDER_ROOT}/")), "{battery}");
        assert_eq!(battery, "/org/librepods/battery/dev_AA_BB_CC_DD_EE_FF");
        assert!(Path::new(battery).is_ok());
        assert!(ProviderObjects::new("hci0", "AA:BB:CC:DD:EE:FF").is_ok());
    }

    type ManagedObjects = HashMap<Path<'static>, HashMap<String, PropMap>>;

    fn call(path: &str, iface: &str, member: &str) -> Result<Message, String> {
        let mut msg = Message::new_method_call(":1.1", path.to_string(), iface, member)?;
        // As if received from the bus: libdbus can only answer a sent call.
        msg.set_serial(7);
        Ok(msg)
    }

    fn managed(objects: &ProviderObjects) -> Result<ManagedObjects, String> {
        let get = call(PROVIDER_ROOT, OBJECT_MANAGER_IFACE, "GetManagedObjects")?;
        let reply = objects.reply(&get).ok_or("no reply")?;
        reply.read1().map_err(|e| e.to_string())
    }

    fn signal(objects: &mut ProviderObjects, command: Command) -> Result<Message, String> {
        objects.apply(command)?.ok_or_else(|| "no signal".to_string())
    }

    #[test]
    fn provider_objects_serve_the_exported_battery() -> Result<(), String> {
        let mut objects = ProviderObjects::new("hci0", "AA:BB:CC:DD:EE:FF")?;
        assert!(managed(&objects)?.is_empty());

        let added = signal(&mut objects, Command::Set(55))?;
        assert_eq!(added.member().as_deref(), Some("InterfacesAdded"));
        assert_eq!(added.path().as_deref(), Some(PROVIDER_ROOT));

        let objs = managed(&objects)?;
        let battery = Path::new(battery_object_path("AA:BB:CC:DD:EE:FF"))?;
        let props = &objs[&battery][PROVIDER_IFACE];
        assert_eq!(props["Percentage"].0.as_u64(), Some(55));
        assert_eq!(props["Source"].0.as_str(), Some(PROVIDER_SOURCE));
        assert_eq!(
            props["Device"].0.as_str(),
            Some("/org/bluez/hci0/dev_AA_BB_CC_DD_EE_FF")
        );

        let changed = signal(&mut objects, Command::Set(40))?;
        assert_eq!(changed.member().as_deref(), Some("PropertiesChanged"));
        assert_eq!(changed.path().as_deref(), Some(&*battery));
        assert!(objects.apply(Command::Set(40))?.is_none());

        let removed = signal(&mut objects, Command::Remove)?;
        assert_eq!(removed.member().as_deref(), Some("InterfacesRemoved"));
        assert!(objects.apply(Command::Remove)?.is_none());
        assert!(managed(&objects)?.is_empty());
        Ok(())
    }

    #[test]
    fn provider_objects_reject_unknown_calls() -> Result<(), String> {
        let objects = ProviderObjects::new("hci0", "AA:BB:CC:DD:EE:FF")?;
        let elsewhere = call("/elsewhere", OBJECT_MANAGER_IFACE, "GetManagedObjects")?;
        let reply = objects.reply(&elsewhere);
        assert_eq!(reply.map(|r| r.msg_type()), Some(MessageType::Error));
        // The battery object is only served while a percentage is exported.
        let get_all = call(&battery_object_path("AA:BB:CC:DD:EE:FF"), PROPERTIES_IFACE, "GetAll")?
            .append1(PROVIDER_IFACE);
        let reply = objects.reply(&get_all);
        assert_eq!(reply.map(|r| r.msg_type()), Some(MessageType::Error));
        Ok(())
    }
}
