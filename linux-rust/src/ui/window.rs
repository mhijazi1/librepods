use crate::bluetooth::aacp::{
    AACPEvent, BatteryComponent, BatteryInfo, BatteryStatus, ControlCommandIdentifiers,
};
use crate::bluetooth::managers::DeviceManagers;
use crate::devices::enums::{
    AirPodsNoiseControlMode, AirPodsState, DeviceData, DeviceState, DeviceType, NothingAncMode,
    NothingState,
};
use crate::settings_file::{parse_devices, write_settings_file};
use crate::ui::airpods::airpods_view;
use crate::ui::messages::BluetoothUIMessage;
use crate::ui::nothing::nothing_view;
use crate::utils::{
    ConnectMode, MyTheme, PreferredCodec, get_app_settings_path, get_devices_path,
    get_preferences_path,
};
use bluer::{Address};
use iced::border::Radius;
use iced::overlay::menu;
use iced::widget::button::Style;
use iced::widget::rule::FillMode;
use iced::widget::{
    Space, button, column, combo_box, container, pane_grid, pick_list, row, rule, scrollable, text,
    text_input, toggler
};
use iced::{Background, Border, Center, Element, Font, Length, Padding, Size, Subscription, Task, Theme, daemon, window, Settings, Program};
use log::{debug, error};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{Mutex, RwLock};

/// The single per-component battery rule, shared by the window (`merge_battery`) and
/// the tray (devices/airpods.rs). A Disconnected update (AACP status 0x04, e.g. the
/// case while the buds are out of it) never replaces a known non-Disconnected
/// level/status: the previous entry is kept. Otherwise the update wins: a Disconnected
/// update is stored when nothing (or only Disconnected) is known, and a Charging or
/// NotCharging update always replaces the previous entry.
pub(crate) fn merge_battery_entry(
    prev: Option<(u8, BatteryStatus)>,
    update: (u8, BatteryStatus),
) -> (u8, BatteryStatus) {
    match prev {
        Some(known)
            if update.1 == BatteryStatus::Disconnected
                && known.1 != BatteryStatus::Disconnected =>
        {
            known
        }
        _ => update,
    }
}

/// Merges a partial battery update into the previous levels, one component at a time.
/// A component absent from `update` keeps its previous entry. A component present in
/// both goes through `merge_battery_entry`. A component with no previous entry is
/// appended.
fn merge_battery(prev: &[BatteryInfo], update: &[BatteryInfo]) -> Vec<BatteryInfo> {
    let mut merged = prev.to_vec();
    for entry in update {
        match merged.iter_mut().find(|m| m.component == entry.component) {
            Some(existing) => {
                let (level, status) = merge_battery_entry(
                    Some((existing.level, existing.status)),
                    (entry.level, entry.status),
                );
                existing.level = level;
                existing.status = status;
            }
            None => merged.push(entry.clone()),
        }
    }
    merged
}

/// Text for one battery level in the window. `None` means the level is not known.
fn battery_level_label(level: Option<u8>) -> String {
    match level {
        Some(n) => format!("{n}%"),
        None => "--".to_string(),
    }
}

/// Text for one component's battery entry in the window. A missing entry or a
/// Disconnected one (whose level byte is not a real level) is shown as `--`.
fn battery_label(entry: Option<&BatteryInfo>) -> String {
    battery_level_label(
        entry
            .filter(|x| x.status != BatteryStatus::Disconnected)
            .map(|x| x.level),
    )
}

/// The charging marker appended after a component's level, only for status Charging.
fn charging_marker(entry: Option<&BatteryInfo>) -> &'static str {
    match entry {
        Some(x) if x.status == BatteryStatus::Charging => "\u{1002E6}",
        _ => "",
    }
}

/// Note under the "Connect to this computer" picker: connectMode is only
/// acted on in handoff switching mode, so other modes say so.
fn connect_mode_hint(mode: crate::utils::SwitchingMode) -> Option<&'static str> {
    match mode {
        crate::utils::SwitchingMode::Handoff => None,
        crate::utils::SwitchingMode::Legacy | crate::utils::SwitchingMode::Off => {
            Some("Applies only when switching mode is handoff")
        }
    }
}

/// Text under a connected AirPods tab. If a Headphone entry exists (any status) only
/// that is shown; otherwise Left, Right and Case. Every level goes through
/// `battery_label`, so a missing or Disconnected entry is `--`, never its raw byte.
fn battery_tab_label(battery: &[BatteryInfo]) -> String {
    let find = |component: BatteryComponent| battery.iter().find(|x| x.component == component);
    if let Some(headphone) = find(BatteryComponent::Headphone) {
        return format!(
            "\u{100EB9} {}{}",
            battery_label(Some(headphone)),
            charging_marker(Some(headphone))
        );
    }
    let left = find(BatteryComponent::Left);
    let right = find(BatteryComponent::Right);
    let case = find(BatteryComponent::Case);
    format!(
        "\u{1018E5} {}{} \u{1018E8} {}{} \u{100E6C} {}{}",
        battery_label(left),
        charging_marker(left),
        battery_label(right),
        charging_marker(right),
        battery_label(case),
        charging_marker(case)
    )
}

/// The preferences JSON `prefs` (None: no file yet) with `[mac].connectMode`
/// set to `mode` and every other key kept. None when `prefs` is not a JSON
/// object or the device's entry is not one: the caller then writes nothing,
/// so the user's file is never replaced.
fn with_connect_mode(
    prefs: Option<&str>,
    mac: &str,
    mode: ConnectMode,
) -> Option<serde_json::Value> {
    let mut root = match prefs {
        Some(content) => serde_json::from_str(content).ok()?,
        None => serde_json::json!({}),
    };
    root.as_object_mut()?
        .entry(mac)
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()?
        .insert("connectMode".to_string(), serde_json::json!(mode));
    Some(root)
}

/// Why a settings or preferences write didn't happen.
enum SaveError {
    /// Reading preferences.json or `write_settings_file` failed.
    Io(std::io::Error),
    /// `with_connect_mode` refused: preferences.json isn't a JSON object with an object per device.
    NotSettingsJson,
}

/// The status shown on the tab whose write failed. Never includes file contents.
fn save_status_text(err: &SaveError) -> String {
    match err {
        SaveError::Io(e) => format!("Couldn't save: {e}"),
        SaveError::NotSettingsJson => {
            "Couldn't save: preferences.json isn't valid settings JSON".to_string()
        }
    }
}

/// Records a write's outcome for `scope` (the tab that triggered it): an error sets
/// that tab's status, a success clears only that tab's status.
fn apply_save_result(
    statuses: &mut HashMap<Tab, String>,
    scope: Tab,
    result: Result<(), SaveError>,
) {
    match result {
        Ok(()) => {
            statuses.remove(&scope);
        }
        Err(e) => {
            statuses.insert(scope, save_status_text(&e));
        }
    }
}

/// The app settings file's JSON: all four app-wide settings.
fn app_settings_json(
    theme: MyTheme,
    tray_text_mode: bool,
    stem_control: bool,
    preferred_codec: PreferredCodec,
) -> serde_json::Value {
    serde_json::json!({
        "theme": theme,
        "tray_text_mode": tray_text_mode,
        "stem_control": stem_control,
        "preferred_codec": preferred_codec,
    })
}

/// Returns the devices JSON with `data` stored under `addr`, given the result of
/// reading the current file. Only a missing file counts as empty; any other read,
/// parse or serialize error is returned so the caller never drops existing devices.
fn devices_with_added(
    existing: std::io::Result<String>,
    addr: &str,
    data: DeviceData,
) -> std::io::Result<String> {
    let mut devices = parse_devices(existing)?;
    devices.insert(addr.to_string(), data);
    Ok(serde_json::to_string(&devices)?)
}

pub fn start_ui(
    ui_rx: UnboundedReceiver<BluetoothUIMessage>,
    start_minimized: bool,
    device_managers: Arc<RwLock<HashMap<String, DeviceManagers>>>,
    // stem_control: Arc<AtomicBool>,
) -> iced::Result {
    let ui_rx = Arc::new(Mutex::new(ui_rx));

    // not sure if this is a good idea
    daemon(
        move || {
            App::new(
                Arc::clone(&ui_rx),
                start_minimized,
                Arc::clone(&device_managers),
                // Arc::clone(&stem_control),
            )
        },
        App::update,
        App::view,
    )
    .subscription(App::subscription)
    .theme(App::theme)
    .title(App::title)
    .settings(Settings {
        id: Some("librepods".to_string()),
        fonts: vec![include_bytes!("../../assets/font/sf_pro.otf").as_slice().into()],
        default_font: Font::with_name("SF Pro Text"),
        ..Settings::default()
    })
    .run()
}

pub struct App {
    window: Option<window::Id>,
    panes: pane_grid::State<Pane>,
    selected_tab: Tab,
    theme_state: combo_box::State<MyTheme>,
    selected_theme: MyTheme,
    ui_rx: Arc<Mutex<UnboundedReceiver<BluetoothUIMessage>>>,
    bluetooth_state: BluetoothState,
    paired_devices: HashMap<String, Address>,
    device_states: HashMap<String, DeviceState>,
    device_managers: Arc<RwLock<HashMap<String, DeviceManagers>>>,
    pending_add_device: Option<(String, Address)>,
    device_type_state: combo_box::State<DeviceType>,
    selected_device_type: Option<DeviceType>,
    tray_text_mode: bool,
    stem_control: bool,
    preferred_codec: PreferredCodec,
    /// Each AirPods' "Connect to this computer" (#16), read from
    /// preferences.json when the device connects.
    connect_modes: HashMap<String, ConnectMode>,
    /// Switching mode as of the last AirPods connection, for the picker's hint.
    switching_mode: crate::utils::SwitchingMode,
    /// "Couldn't save" status per tab (Settings, or Device(mac) for connectMode),
    /// set and cleared only through `apply_save_result`.
    save_statuses: HashMap<Tab, String>,
}

pub struct BluetoothState {
    connected_devices: Vec<String>,
}

impl BluetoothState {
    pub fn new() -> Self {
        Self {
            connected_devices: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Message {
    WindowOpened(window::Id),
    WindowClosed(window::Id),
    Resized(pane_grid::ResizeEvent),
    SelectTab(Tab),
    ThemeSelected(MyTheme),
    CopyToClipboard(String),
    BluetoothMessage(BluetoothUIMessage),
    // ShowNewDialogTab,
    GotPairedDevices(HashMap<String, Address>),
    StartAddDevice(String, Address),
    SelectDeviceType(DeviceType),
    ConfirmAddDevice,
    CancelAddDevice,
    StateChanged(String, DeviceState),
    TrayTextModeChanged(bool), // yes, I know I should add all settings to a struct, but I'm lazy
    StemControlChanged(bool),
    PreferredCodecChanged(PreferredCodec),
    ConnectModeChanged(String, ConnectMode), // AirPods MAC
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Tab {
    Device(String),
    Settings,
    AddDevice,
}

#[derive(Clone, Copy)]
pub enum Pane {
    Sidebar,
    Content,
}

impl App {
    pub fn new(
        ui_rx: Arc<Mutex<UnboundedReceiver<BluetoothUIMessage>>>,
        start_minimized: bool,
        device_managers: Arc<RwLock<HashMap<String, DeviceManagers>>>,
        // stem_control: Arc<AtomicBool>,
    ) -> (Self, Task<Message>) {
        let (mut panes, first_pane) = pane_grid::State::new(Pane::Sidebar);
        let split = panes.split(pane_grid::Axis::Vertical, first_pane, Pane::Content);
        panes.resize(split.unwrap().1, 0.2);


        let wait_task = Task::perform(wait_for_message(Arc::clone(&ui_rx)), |msg| msg);

        let (window, open_task) = if start_minimized {
            (None, Task::none())
        } else {
            let mut settings = window::Settings::default();
            settings.min_size = Some(Size::new(400.0, 300.0));
            settings.icon = window::icon::from_file("../../assets/icon.png").ok();
            let (id, open) = window::open(settings);
            (Some(id), open.map(Message::WindowOpened))
        };

        let app_settings_path = get_app_settings_path();
        let settings = std::fs::read_to_string(&app_settings_path)
            .ok()
            .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok());
        let selected_theme = settings
            .clone()
            .and_then(|v| v.get("theme").cloned())
            .and_then(|t| serde_json::from_value(t).ok())
            .unwrap_or(MyTheme::Dark);
        let tray_text_mode = settings
            .clone()
            .and_then(|v| v.get("tray_text_mode").cloned())
            .and_then(|ttm| serde_json::from_value(ttm).ok())
            .unwrap_or(false);
        let stem_control = settings
            .clone()
            .and_then(|v| v.get("stem_control").cloned())
            .and_then(|s| serde_json::from_value(s).ok())
            .unwrap_or(false);
        let preferred_codec = crate::utils::preferred_codec_from_value(settings.as_ref());

        let bluetooth_state = BluetoothState::new();

        // let dummy_device_state = DeviceState::AirPods(AirPodsState {
        //     conversation_awareness_enabled: false,
        // });
        // let device_states = HashMap::from([
        //     ("28:2D:7F:C2:05:5B".to_string(), dummy_device_state),
        // ]);

        let device_states = HashMap::new();
        (
            Self {
                window,
                panes,
                selected_tab: Tab::Device("none".to_string()),
                theme_state: combo_box::State::new(vec![
                    MyTheme::Light,
                    MyTheme::Dark,
                    MyTheme::Dracula,
                    MyTheme::Nord,
                    MyTheme::SolarizedLight,
                    MyTheme::SolarizedDark,
                    MyTheme::GruvboxLight,
                    MyTheme::GruvboxDark,
                    MyTheme::CatppuccinLatte,
                    MyTheme::CatppuccinFrappe,
                    MyTheme::CatppuccinMacchiato,
                    MyTheme::CatppuccinMocha,
                    MyTheme::TokyoNight,
                    MyTheme::TokyoNightStorm,
                    MyTheme::TokyoNightLight,
                    MyTheme::KanagawaWave,
                    MyTheme::KanagawaDragon,
                    MyTheme::KanagawaLotus,
                    MyTheme::Moonfly,
                    MyTheme::Nightfly,
                    MyTheme::Oxocarbon,
                    MyTheme::Ferra,
                ]),
                selected_theme,
                ui_rx,
                bluetooth_state,
                paired_devices: HashMap::new(),
                device_states,
                pending_add_device: None,
                device_type_state: combo_box::State::new(vec![DeviceType::Nothing]),
                selected_device_type: None,
                device_managers,
                tray_text_mode,
                stem_control,
                preferred_codec,
                connect_modes: HashMap::new(),
                switching_mode: crate::utils::SwitchingMode::default(),
                save_statuses: HashMap::new(),
            },
            Task::batch(vec![open_task, wait_task]),
        )
    }

    fn title(&self, _id: window::Id) -> String {
        "LibrePods".to_string()
    }

    /// Writes the app settings to app_settings.json and records the outcome on the Settings tab.
    fn save_app_settings(&mut self) {
        let app_settings_path = get_app_settings_path();
        let settings = app_settings_json(
            self.selected_theme,
            self.tray_text_mode,
            self.stem_control,
            self.preferred_codec,
        );
        debug!(
            "Writing settings to {}: {}",
            app_settings_path.to_str().unwrap(),
            settings
        );
        let result = write_settings_file(&app_settings_path, &settings.to_string());
        if let Err(e) = &result {
            error!("Failed to write {}: {}", app_settings_path.display(), e);
        }
        apply_save_result(
            &mut self.save_statuses,
            Tab::Settings,
            result.map_err(SaveError::Io),
        );
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::WindowOpened(id) => {
                self.window = Some(id);
                Task::none()
            }
            Message::WindowClosed(id) => {
                if self.window == Some(id) {
                    self.window = None;
                }
                Task::none()
            }
            Message::Resized(event) => {
                self.panes.resize(event.split, event.ratio);
                Task::none()
            }
            Message::SelectTab(tab) => {
                self.selected_tab = tab;
                Task::none()
            }
            Message::ThemeSelected(theme) => {
                self.selected_theme = theme;
                self.save_app_settings();
                Task::none()
            }
            Message::CopyToClipboard(data) => iced::clipboard::write(data),
            Message::BluetoothMessage(ui_message) => {
                match ui_message {
                    BluetoothUIMessage::NoOp => {
                        let ui_rx = Arc::clone(&self.ui_rx);

                        Task::perform(wait_for_message(ui_rx), |msg| msg)
                    }
                    BluetoothUIMessage::OpenWindow => {
                        let ui_rx = Arc::clone(&self.ui_rx);
                        let wait_task = Task::perform(wait_for_message(ui_rx), |msg| msg);
                        debug!("Opening main window...");
                        if let Some(window_id) = self.window {
                            Task::batch(vec![window::gain_focus(window_id), wait_task])
                        } else {
                            let mut settings = window::Settings::default();
                            settings.min_size = Some(Size::new(400.0, 300.0));
                            settings.icon = window::icon::from_file("../../assets/icon.png").ok();
                            let (new_window_task, open_task) = window::open(settings);
                            self.window = Some(new_window_task);
                            Task::batch(vec![open_task.map(Message::WindowOpened), wait_task])
                        }
                    }
                    BluetoothUIMessage::DeviceConnected(mac) => {
                        let ui_rx = Arc::clone(&self.ui_rx);
                        let wait_task = Task::perform(wait_for_message(ui_rx), |msg| msg);
                        debug!(
                            "Device connected: {}. Adding to connected devices list",
                            mac
                        );
                        let mut already_connected = false;
                        for device in &self.bluetooth_state.connected_devices {
                            if device == &mac {
                                already_connected = true;
                                break;
                            }
                        }
                        if !already_connected {
                            self.bluetooth_state.connected_devices.push(mac.clone());
                        }

                        // self.device_states.insert(mac.clone(), DeviceState::AirPods(AirPodsState {
                        //     conversation_awareness_enabled: false,
                        // }));

                        let type_ = {
                            let devices_json = std::fs::read_to_string(get_devices_path())
                                .unwrap_or_else(|e| {
                                    error!("Failed to read devices file: {}", e);
                                    "{}".to_string()
                                });
                            let devices_list: HashMap<String, DeviceData> =
                                serde_json::from_str(&devices_json).unwrap_or_else(|e| {
                                    error!("Deserialization failed: {}", e);
                                    HashMap::new()
                                });
                            devices_list.get(&mac).map(|d| d.type_.clone())
                        };
                        match type_ {
                            Some(DeviceType::AirPods) => {
                                let device_managers = self.device_managers.blocking_read();
                                let device_manager = device_managers.get(&mac).unwrap();
                                let aacp_manager = device_manager.get_aacp().unwrap();
                                let aacp_manager_state = aacp_manager.state.clone();
                                let state = aacp_manager_state.blocking_lock();
                                debug!("AACP manager found for AirPods device {}", mac);
                                let device_name = {
                                    let devices_json = std::fs::read_to_string(get_devices_path())
                                        .unwrap_or_else(|e| {
                                            error!("Failed to read devices file: {}", e);
                                            "{}".to_string()
                                        });
                                    let devices_list: HashMap<String, DeviceData> =
                                        serde_json::from_str(&devices_json).unwrap_or_else(|e| {
                                            error!("Deserialization failed: {}", e);
                                            HashMap::new()
                                        });
                                    devices_list
                                        .get(&mac)
                                        .map(|d| d.name.clone())
                                        .unwrap_or_else(|| "Unknown Device".to_string())
                                };
                                self.connect_modes
                                    .insert(mac.clone(), crate::utils::load_connect_mode(&mac));
                                self.switching_mode = crate::utils::load_switching_mode();
                                self.device_states.insert(mac.clone(), DeviceState::AirPods(AirPodsState {
                                    device_name,
                                    battery: state.battery_info.clone(),
                                    noise_control_mode: state.control_command_status_list.iter().find_map(|status| {
                                        if status.identifier == ControlCommandIdentifiers::ListeningMode {
                                            status.value.first().map(AirPodsNoiseControlMode::from_byte)
                                        } else {
                                            None
                                        }
                                    }).unwrap_or(AirPodsNoiseControlMode::Transparency),
                                    noise_control_state: combo_box::State::new(
                                        {
                                            let mut modes = vec![
                                                AirPodsNoiseControlMode::Transparency,
                                                AirPodsNoiseControlMode::NoiseCancellation,
                                                AirPodsNoiseControlMode::Adaptive
                                            ];
                                            if state.control_command_status_list.iter().any(|status| {
                                                status.identifier == ControlCommandIdentifiers::AllowOffOption &&
                                                matches!(status.value.as_slice(), [0x01])
                                            }) {
                                                modes.insert(0, AirPodsNoiseControlMode::Off);
                                            }
                                            modes
                                        }
                                    ),
                                    conversation_awareness_enabled: state.control_command_status_list.iter().any(|status| {
                                        status.identifier == ControlCommandIdentifiers::ConversationDetectConfig &&
                                        matches!(status.value.as_slice(), [0x01])
                                    }),
                                    personalized_volume_enabled: state.control_command_status_list.iter().any(|status| {
                                        status.identifier == ControlCommandIdentifiers::AdaptiveVolumeConfig &&
                                        matches!(status.value.as_slice(), [0x01])
                                    }),
                                    allow_off_mode: state.control_command_status_list.iter().any(|status| {
                                        status.identifier == ControlCommandIdentifiers::AllowOffOption &&
                                        matches!(status.value.as_slice(), [0x01])
                                    }),
                                }));
                            }
                            Some(DeviceType::Nothing) => {
                                self.device_states.insert(
                                    mac.clone(),
                                    DeviceState::Nothing(NothingState {
                                        anc_mode: NothingAncMode::Off,
                                        anc_mode_state: combo_box::State::new(vec![
                                            NothingAncMode::Off,
                                            NothingAncMode::Transparency,
                                            NothingAncMode::AdaptiveNoiseCancellation,
                                            NothingAncMode::LowNoiseCancellation,
                                            NothingAncMode::MidNoiseCancellation,
                                            NothingAncMode::HighNoiseCancellation,
                                        ]),
                                    }),
                                );
                            }
                            _ => {}
                        }

                        Task::batch(vec![wait_task])
                    }
                    BluetoothUIMessage::DeviceDisconnected(mac) => {
                        let ui_rx = Arc::clone(&self.ui_rx);
                        let wait_task = Task::perform(wait_for_message(ui_rx), |msg| msg);
                        debug!("Device disconnected: {}", mac);

                        self.bluetooth_state
                            .connected_devices
                            .retain(|device| device != &mac);

                        self.device_states.remove(&mac);

                        if matches!(&self.selected_tab, Tab::Device(selected_mac) if selected_mac == &mac) {
                            self.selected_tab = Tab::Device("none".to_string());
                        }

                        Task::batch(vec![wait_task])
                    }
                    BluetoothUIMessage::AACPUIEvent(mac, event) => {
                        let ui_rx = Arc::clone(&self.ui_rx);
                        let wait_task = Task::perform(wait_for_message(ui_rx), |msg| msg);
                        debug!("AACP UI Event for {}: {:?}", mac, event);
                        match event {
                            AACPEvent::ControlCommand(status) => match status.identifier {
                                ControlCommandIdentifiers::ListeningMode => {
                                    let mode = status
                                        .value
                                        .first()
                                        .map(AirPodsNoiseControlMode::from_byte)
                                        .unwrap_or(AirPodsNoiseControlMode::Transparency);
                                    if let Some(DeviceState::AirPods(state)) =
                                        self.device_states.get_mut(&mac)
                                    {
                                        state.noise_control_mode = mode;
                                    }
                                }
                                ControlCommandIdentifiers::ConversationDetectConfig => {
                                    let is_enabled = match status.value.as_slice() {
                                        [0x01] => true,
                                        [0x02] => false,
                                        _ => {
                                            error!(
                                                "Unknown Conversation Detect Config value: {:?}",
                                                status.value
                                            );
                                            false
                                        }
                                    };
                                    if let Some(DeviceState::AirPods(state)) =
                                        self.device_states.get_mut(&mac)
                                    {
                                        state.conversation_awareness_enabled = is_enabled;
                                    }
                                }
                                ControlCommandIdentifiers::AdaptiveVolumeConfig => {
                                    let is_enabled = match status.value.as_slice() {
                                        [0x01] => true,
                                        [0x02] => false,
                                        _ => {
                                            error!(
                                                "Unknown Adaptive Volume Config value: {:?}",
                                                status.value
                                            );
                                            false
                                        }
                                    };
                                    if let Some(DeviceState::AirPods(state)) =
                                        self.device_states.get_mut(&mac)
                                    {
                                        state.personalized_volume_enabled = is_enabled;
                                    }
                                }
                                ControlCommandIdentifiers::AllowOffOption => {
                                    let is_enabled = match status.value.as_slice() {
                                        [0x01] => true,
                                        [0x02] => false,
                                        _ => {
                                            error!(
                                                "Unknown Allow Off Option value: {:?}",
                                                status.value
                                            );
                                            false
                                        }
                                    };
                                    if let Some(DeviceState::AirPods(state)) =
                                        self.device_states.get_mut(&mac)
                                    {
                                        state.allow_off_mode = is_enabled;
                                        state.noise_control_state = combo_box::State::new({
                                            let mut modes = vec![
                                                AirPodsNoiseControlMode::Transparency,
                                                AirPodsNoiseControlMode::NoiseCancellation,
                                                AirPodsNoiseControlMode::Adaptive,
                                            ];
                                            if is_enabled {
                                                modes.insert(0, AirPodsNoiseControlMode::Off);
                                            }
                                            modes
                                        });
                                    }
                                }
                                // The handoff path (switching.rs) owns connection ownership.
                                ControlCommandIdentifiers::OwnsConnection => {}
                                _ => {
                                    debug!("Unhandled Control Command Status: {:?}", status);
                                }
                            },
                            AACPEvent::BatteryInfo(battery_info) => {
                                if let Some(DeviceState::AirPods(state)) =
                                    self.device_states.get_mut(&mac)
                                {
                                    state.battery = merge_battery(&state.battery, &battery_info);
                                    debug!("Updated battery info for {}: {:?}", mac, state.battery);
                                }
                            }
                            _ => {}
                        }
                        Task::batch(vec![wait_task])
                    }
                    BluetoothUIMessage::ATTNotification(mac, handle, value) => {
                        debug!(
                            "ATT Notification for {}: handle=0x{:04X}, value={:?}",
                            mac, handle, value
                        );

                        // TODO: Handle Nothing's ANC Mode changes here

                        let ui_rx = Arc::clone(&self.ui_rx);
                        let wait_task = Task::perform(wait_for_message(ui_rx), |msg| msg);
                        Task::batch(vec![wait_task])
                    }
                }
            }
            // Message::ShowNewDialogTab => {
            //     debug!("switching to Add Device tab");
            //     self.selected_tab = Tab::AddDevice;
            //     Task::perform(load_paired_devices(), Message::GotPairedDevices)
            // }
            Message::GotPairedDevices(map) => {
                self.paired_devices = map;
                Task::none()
            }
            Message::StartAddDevice(name, addr) => {
                self.pending_add_device = Some((name, addr));
                self.selected_device_type = None;
                Task::none()
            }
            Message::SelectDeviceType(device_type) => {
                self.selected_device_type = Some(device_type);
                Task::none()
            }
            Message::ConfirmAddDevice => {
                if let Some((name, addr)) = self.pending_add_device.take()
                    && let Some(type_) = self.selected_device_type.take()
                {
                    let devices_path = get_devices_path();
                    let data = DeviceData {
                        name,
                        type_,
                        information: None,
                    };
                    match devices_with_added(
                        std::fs::read_to_string(&devices_path),
                        &addr.to_string(),
                        data,
                    ) {
                        Ok(updated_json) => {
                            if let Err(e) = write_settings_file(&devices_path, &updated_json) {
                                error!("Failed to write devices file: {}", e);
                            }
                        }
                        Err(e) => error!("Not adding device, devices file unusable: {}", e),
                    }
                    self.selected_tab = Tab::Device(addr.to_string());
                }
                Task::none()
            }
            Message::CancelAddDevice => {
                self.pending_add_device = None;
                self.selected_device_type = None;
                Task::none()
            }
            Message::StateChanged(mac, state) => {
                self.device_states.insert(mac.clone(), state);
                // if airpods, update the noise control state combo box based on allow off mode
                let type_ = {
                    let devices_json =
                        std::fs::read_to_string(get_devices_path()).unwrap_or_else(|e| {
                            error!("Failed to read devices file: {}", e);
                            "{}".to_string()
                        });
                    let devices_list: HashMap<String, DeviceData> =
                        serde_json::from_str(&devices_json).unwrap_or_else(|e| {
                            error!("Deserialization failed: {}", e);
                            HashMap::new()
                        });
                    devices_list.get(&mac).map(|d| d.type_.clone())
                };
                if let Some(DeviceType::AirPods) = type_
                    && let Some(DeviceState::AirPods(state)) = self.device_states.get_mut(&mac)
                {
                    state.noise_control_state = combo_box::State::new({
                        let mut modes = vec![
                            AirPodsNoiseControlMode::Transparency,
                            AirPodsNoiseControlMode::NoiseCancellation,
                            AirPodsNoiseControlMode::Adaptive,
                        ];
                        if state.allow_off_mode {
                            modes.insert(0, AirPodsNoiseControlMode::Off);
                        }
                        modes
                    });
                }
                Task::none()
            }
            Message::TrayTextModeChanged(is_enabled) => {
                self.tray_text_mode = is_enabled;
                self.save_app_settings();
                Task::none()
            }
            Message::PreferredCodecChanged(codec) => {
                self.preferred_codec = codec;
                self.save_app_settings();
                Task::none()
            }
            Message::ConnectModeChanged(mac, mode) => {
                let path = get_preferences_path();
                let scope = Tab::Device(mac.clone());
                // Only a missing file counts as empty: any other read error
                // must not replace the user's preferences.
                let current = match std::fs::read_to_string(&path) {
                    Ok(content) => Some(content),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                    Err(e) => {
                        error!("Not saving connectMode: failed to read {}: {}", path.display(), e);
                        apply_save_result(&mut self.save_statuses, scope, Err(SaveError::Io(e)));
                        return Task::none();
                    }
                };
                let Some(prefs) = with_connect_mode(current.as_deref(), &mac, mode) else {
                    error!(
                        "Not saving connectMode: {} is not a JSON object with an object per device",
                        path.display()
                    );
                    apply_save_result(&mut self.save_statuses, scope, Err(SaveError::NotSettingsJson));
                    return Task::none();
                };
                let result = write_settings_file(&path, &format!("{prefs:#}"));
                match &result {
                    Ok(()) => {
                        self.connect_modes.insert(mac, mode);
                    }
                    Err(e) => error!("Failed to write {}: {}", path.display(), e),
                }
                apply_save_result(&mut self.save_statuses, scope, result.map_err(SaveError::Io));
                Task::none()
            }
            Message::StemControlChanged(is_enabled) => {
                self.stem_control = is_enabled;
                self.save_app_settings();
                Task::none()
            }
        }
    }

    fn view(&self, _id: window::Id) -> Element<'_, Message> {
        let devices_json = std::fs::read_to_string(get_devices_path()).unwrap_or_else(|e| {
            error!("Failed to read devices file: {}", e);
            "{}".to_string()
        });
        let devices_list: HashMap<String, DeviceData> = serde_json::from_str(&devices_json)
            .unwrap_or_else(|e| {
                error!("Deserialization failed: {}", e);
                HashMap::new()
            });
        let pane_grid = pane_grid::PaneGrid::new(&self.panes, |_pane_id, pane, _is_maximized| {
            match pane {
                Pane::Sidebar => {
                    let create_tab_button = |tab: Tab, label: &str, mac_addr: &str, connected: bool| -> Element<'_, Message> {
                        let label = label.to_string() + if connected { " 􀉣" } else { "" };
                        let is_selected = self.selected_tab == tab;
                        let col = column![
                            text(label).size(16),
                            text({
                                if connected {
                                    let mac = match tab {
                                        Tab::Device(ref mac) => mac.as_str(),
                                        _ => "",
                                    };

                                    match self.device_states.get(mac) {
                                        Some(DeviceState::AirPods(state)) => {
                                            battery_tab_label(&state.battery)
                                        }
                                        _ => "Connected".to_string(),
                                    }
                                } else {
                                    mac_addr.to_string()
                                }
                            }).size(12)
                        ];
                        let content = container(col)
                            .padding(8);
                        let style = move |theme: &Theme, _status| {
                            if is_selected {
                                let mut style = Style::default()
                                    .with_background(theme.palette().primary);
                                let mut border = Border::default();
                                border.color = theme.palette().text;
                                style.border = border.rounded(12);
                                style
                            } else {
                                let mut style = Style::default()
                                    .with_background(theme.palette().primary.scale_alpha(0.1));
                                let mut border = Border::default();
                                border.color = theme.palette().primary.scale_alpha(0.1);
                                style.border = border.rounded(8);
                                style.text_color = theme.palette().text;
                                style
                            }
                        };
                        button(content)
                            .style(style)
                            .padding(5)
                            .on_press(Message::SelectTab(tab))
                            .width(Length::Fill)
                            .into()
                    };

                    let create_settings_button = || -> Element<'_, Message> {
                        let label = "Settings".to_string();
                        let is_selected = self.selected_tab == Tab::Settings;
                        let col = column![text(label).size(16)];
                        let content = container(col)
                            .padding(8);
                        let style = move |theme: &Theme, _status| {
                            if is_selected {
                                let mut style = Style::default()
                                    .with_background(theme.palette().primary);
                                let mut border = Border::default();
                                border.color = theme.palette().text;
                                style.border = border.rounded(12);
                                style
                            } else {
                                let mut style = Style::default()
                                    .with_background(theme.palette().primary.scale_alpha(0.1));
                                let mut border = Border::default();
                                border.color = theme.palette().primary.scale_alpha(0.1);
                                style.border = border.rounded(8);
                                style.text_color = theme.palette().text;
                                style
                            }
                        };
                        button(content)
                            .style(style)
                            .padding(5)
                            .on_press(Message::SelectTab(Tab::Settings))
                            .width(Length::Fill)
                            .into()
                    };

                    let mut devices = column!().spacing(4);
                    let mut devices_vec: Vec<(String, DeviceData)> = devices_list.clone().into_iter().collect();
                    devices_vec.sort_by(|a, b| a.1.name.cmp(&b.1.name));
                    for (mac, device) in devices_vec {
                        let name = device.name.clone();
                        let tab_button = create_tab_button(
                            Tab::Device(mac.clone()),
                            &name,
                            &mac,
                            self.bluetooth_state.connected_devices.contains(&mac)
                        );
                        devices = devices.push(tab_button);
                    }

                    let settings = create_settings_button();

                    let content = column![
                        row![
                            text("Devices").size(18),
                            // Removing until I actually add support for devices other than AirPods
                            // Space::new().width(Length::Fill),
                            // button(
                            //     container(text("+").size(18)).center_x(Length::Fill).center_y(Length::Fill)
                            // )
                            //     .style(
                            //         |theme: &Theme, _status| {
                            //             let mut style = Style::default();
                            //             style.text_color = theme.palette().text;
                            //             style.background = Some(Background::Color(theme.palette().primary.scale_alpha(0.1)));
                            //             style.border = Border {
                            //                 width: 1.0,
                            //                 color: theme.palette().primary.scale_alpha(0.1),
                            //                 radius: Radius::from(8.0),
                            //             };
                            //             style
                            //         }
                            //     )
                            //     .padding(0)
                            //     .width(Length::from(28))
                            //     .height(Length::from(28))
                            //     .on_press(Message::ShowNewDialogTab)
                        ]
                        .align_y(Center)
                        .padding(4),
                        Space::new().height(Length::from(8)),
                        devices,
                        Space::new().height(Length::Fill),
                        settings
                    ]
                        .padding(12);
                    pane_grid::Content::new(
                        row![
                            content,
                            rule::vertical(1).style(
                                |theme: &Theme| {
                                    rule::Style{
                                        color: theme.palette().primary.scale_alpha(0.2),
                                        radius: Radius::from(8.0),
                                        fill_mode: FillMode::Full,
                                        snap: false
                                    }
                                }
                            )
                        ]
                    )
                }

                Pane::Content => {
                    let device_managers = self.device_managers.blocking_read();
                    let content = match &self.selected_tab {
                        Tab::Device(id) => {
                            if id == "none" {
                                container(
                                    text("Select a device".to_string()).size(16)
                                )
                                    .center_x(Length::Fill)
                                    .center_y(Length::Fill)
                            } else {
                                let device_type = devices_list.get(id).map(|d| d.type_.clone());
                                let device_state = self.device_states.get(id);
                                debug!("Rendering device view for {}: type={:?}, state={:?}", id, device_type, device_state);
                                match device_type {
                                    Some(DeviceType::AirPods) => {

                                        device_state.as_ref().and_then(|state| {
                                            match state {
                                                DeviceState::AirPods(state) => {
                                                    device_managers.get(id).and_then(|managers| {
                                                        managers.get_aacp().map(|aacp_manager| container(column![
                                                            container(column![
                                                                row![
                                                                    text("Connect to this computer").size(16).width(Length::Fill),
                                                                    pick_list(
                                                                        [ConnectMode::Automatically, ConnectMode::WhenLastConnected],
                                                                        Some(self.connect_modes.get(id).copied().unwrap_or_default()),
                                                                        move |mode| Message::ConnectModeChanged(id.clone(), mode),
                                                                    )
                                                                ]
                                                                    .align_y(Center)
                                                                    .spacing(12),
                                                                // Void (skipped) when None.
                                                                connect_mode_hint(self.switching_mode).map(|hint| text(hint).size(12).style(
                                                                    |theme: &Theme| {
                                                                        let mut style = text::Style::default();
                                                                        style.color = Some(theme.palette().text.scale_alpha(0.7));
                                                                        style
                                                                    }
                                                                )),
                                                                self.save_statuses.get(&Tab::Device(id.clone()))
                                                                    .map(|status| text(status).size(12).style(text::danger)),
                                                            ])
                                                                .padding(Padding {
                                                                    top: 20.0,
                                                                    bottom: 0.0,
                                                                    left: 38.0,
                                                                    right: 38.0,
                                                                }),
                                                            airpods_view(
                                                                    id,
                                                                    &devices_list,
                                                                    state,
                                                                    aacp_manager.clone()
                                                                )
                                                        ]))
                                                    })
                                                }
                                                _ => None,
                                            }
                                        }).unwrap_or_else(|| {
                                            container(
                                                text("Required managers or state not available for this AirPods device").size(16)
                                            )
                                                .center_x(Length::Fill)
                                                .center_y(Length::Fill)
                                        })
                                    }
                                    Some(DeviceType::Nothing) => {
                                        if let Some(DeviceState::Nothing(state)) = device_state {
                                            if let Some(device_managers) = device_managers.get(id) {
                                                if let Some(att_manager) = device_managers.get_att() {
                                                    nothing_view(id, &devices_list, state, att_manager.clone())
                                                } else {
                                                    error!("No ATT manager found for Nothing device {}", id);
                                                    container(
                                                        text("No valid ATT manager found for this Nothing device").size(16)
                                                    )
                                                        .center_x(Length::Fill)
                                                        .center_y(Length::Fill)
                                                }
                                            } else {
                                                error!("No manager found for Nothing device {}", id);
                                                container(
                                                    text("No manager found for this Nothing device").size(16)
                                                )
                                                    .center_x(Length::Fill)
                                                    .center_y(Length::Fill)
                                            }
                                        } else {
                                            container(
                                                text("No state available for this Nothing device").size(16)
                                            )
                                                .center_x(Length::Fill)
                                                .center_y(Length::Fill)
                                        }
                                    }
                                    _ => {
                                        container(text("Unsupported device").size(16))
                                            .center_x(Length::Fill)
                                            .center_y(Length::Fill)
                                    }
                                }
                            }
                        }
                        Tab::Settings => {
                            let preferred_codec_picker = container(
                                row![
                                    column![
                                        text("Preferred audio codec").size(16),
                                        text("Codec to activate for playback. The others are used as fallbacks if the chosen one is unavailable.").size(12).style(
                                            |theme: &Theme| {
                                                let mut style = text::Style::default();
                                                style.color = Some(theme.palette().text.scale_alpha(0.7));
                                                style
                                            }
                                        ).width(Length::Fill)
                                    ].width(Length::Fill),
                                    pick_list(
                                        PreferredCodec::ALL,
                                        Some(self.preferred_codec),
                                        Message::PreferredCodecChanged,
                                    )
                                    ]
                                        .align_y(Center)
                                        .spacing(12)
                                    )
                                        .padding(Padding{
                                            top: 5.0,
                                            bottom: 5.0,
                                            left: 18.0,
                                            right: 18.0,
                                        })
                                        .style(
                                            |theme: &Theme| {
                                                let mut style = container::Style::default();
                                                style.background = Some(Background::Color(theme.palette().primary.scale_alpha(0.1)));
                                                let mut border = Border::default();
                                                border.color = theme.palette().primary.scale_alpha(0.5);
                                                style.border = border.rounded(16);
                                                style
                                            }
                                        )
                                    .align_y(Center);

                            let tray_text_mode_toggle = container(
                                row![
                                    column![
                                        text("Use text in tray").size(16),
                                        text("Use text for battery status in tray instead of a progress bar.").size(12).style(
                                            |theme: &Theme| {
                                                let mut style = text::Style::default();
                                                style.color = Some(theme.palette().text.scale_alpha(0.7));
                                                style
                                            }
                                        ).width(Length::Fill)
                                    ].width(Length::Fill),
                                    toggler(self.tray_text_mode)
                                        .on_toggle(move |is_enabled| {
                                            Message::TrayTextModeChanged(is_enabled)
                                        })
                                    .spacing(0)
                                    .size(20)
                                    ]
                                        .align_y(Center)
                                        .spacing(12)
                                    )
                                        .padding(Padding{
                                            top: 5.0,
                                            bottom: 5.0,
                                            left: 18.0,
                                            right: 18.0,
                                        })
                                        .style(
                                            |theme: &Theme| {
                                                let mut style = container::Style::default();
                                                style.background = Some(Background::Color(theme.palette().primary.scale_alpha(0.1)));
                                                let mut border = Border::default();
                                                border.color = theme.palette().primary.scale_alpha(0.5);
                                                style.border = border.rounded(16);
                                                style
                                            }
                                        )
                                    .align_y(Center);

                            let appearance_settings_col = column![
                                container(
                                    text("Appearance").size(20).style(
                                        |theme: &Theme| {
                                            let mut style = text::Style::default();
                                            style.color = Some(theme.palette().primary);
                                            style
                                        }
                                    )
                                )
                                .padding(Padding{
                                    top: 0.0,
                                    bottom: 0.0,
                                    left: 18.0,
                                    right: 18.0,
                                }),
                                container(
                                    row![
                                        text("Theme")
                                            .size(16),
                                        Space::new().width(Length::Fill),
                                        combo_box(
                                            &self.theme_state,
                                            "Select theme",
                                            Some(&self.selected_theme),
                                            Message::ThemeSelected
                                        )
                                        .input_style(
                                            |theme: &Theme, _status| {
                                                text_input::Style {
                                                    background: Background::Color(theme.palette().primary.scale_alpha(0.2)),
                                                    border: Border {
                                                        width: 1.0,
                                                        color: theme.palette().text.scale_alpha(0.3),
                                                        radius: Radius::from(4.0)
                                                    },
                                                    icon: Default::default(),
                                                    placeholder: theme.palette().text,
                                                    value: theme.palette().text,
                                                    selection: Default::default(),
                                                }
                                            }
                                        )
                                        .menu_style(
                                            |theme: &Theme| {
                                                menu::Style {
                                                    background: Background::Color(theme.palette().background),
                                                    border: Border {
                                                        width: 1.0,
                                                        color: theme.palette().text,
                                                        radius: Radius::from(4.0)
                                                    },
                                                    text_color: theme.palette().text,
                                                    selected_text_color: theme.palette().text,
                                                    selected_background: Background::Color(theme.palette().primary.scale_alpha(0.3)),
                                                    shadow: Default::default()
                                                }
                                            }
                                        )
                                        .padding(Padding{
                                            top: 5.0,
                                            bottom: 5.0,
                                            left: 10.0,
                                            right: 10.0,
                                        })
                                        .width(Length::from(200))
                                    ]
                                    .align_y(Center)
                                )
                                    .padding(Padding{
                                        top: 5.0,
                                        bottom: 5.0,
                                        left: 18.0,
                                        right: 18.0,
                                    })
                                    .style(
                                        |theme: &Theme| {
                                            let mut style = container::Style::default();
                                            style.background = Some(Background::Color(theme.palette().primary.scale_alpha(0.1)));
                                            let mut border = Border::default();
                                            border.color = theme.palette().primary.scale_alpha(0.5);
                                            style.border = border.rounded(16);
                                            style
                                        }
                                    )
                                ]
                                .spacing(12);

                            let stem_control_value = self.stem_control;
                            let stem_control_toggle = container(
                                row![
                                    column![
                                        text("Stem press track control").size(16),
                                        text("Double press = next track, triple press = previous track. Disable if your environment handles AirPods AVRCP commands natively.").size(12).style(
                                            |theme: &Theme| {
                                                let mut style = text::Style::default();
                                                style.color = Some(theme.palette().text.scale_alpha(0.7));
                                                style
                                            }
                                        ).width(Length::Fill)
                                    ].width(Length::Fill),
                                    toggler(stem_control_value)
                                        .on_toggle(move |is_enabled| {
                                            Message::StemControlChanged(is_enabled)
                                        })
                                    .spacing(0)
                                    .size(20)
                                    ]
                                        .align_y(Center)
                                        .spacing(12)
                                    )
                                        .padding(Padding{
                                            top: 5.0,
                                            bottom: 5.0,
                                            left: 18.0,
                                            right: 18.0,
                                        })
                                        .style(
                                            |theme: &Theme| {
                                                let mut style = container::Style::default();
                                                style.background = Some(Background::Color(theme.palette().primary.scale_alpha(0.1)));
                                                let mut border = Border::default();
                                                border.color = theme.palette().primary.scale_alpha(0.5);
                                                style.border = border.rounded(16);
                                                style
                                            }
                                        )
                                    .align_y(Center);

                            let audio_settings_col = column![
                                container(
                                    text("Audio").size(20).style(
                                        |theme: &Theme| {
                                            let mut style = text::Style::default();
                                            style.color = Some(theme.palette().primary);
                                            style
                                        }
                                    )
                                )
                                .padding(Padding{
                                    top: 0.0,
                                    bottom: 0.0,
                                    left: 18.0,
                                    right: 18.0,
                                }),
                                preferred_codec_picker
                            ]
                            .spacing(12);

                            let controls_settings_col = column![
                                container(
                                    text("Controls").size(20).style(
                                        |theme: &Theme| {
                                            let mut style = text::Style::default();
                                            style.color = Some(theme.palette().primary);
                                            style
                                        }
                                    )
                                )
                                .padding(Padding{
                                    top: 0.0,
                                    bottom: 0.0,
                                    left: 18.0,
                                    right: 18.0,
                                }),
                                stem_control_toggle
                            ]
                            .spacing(12);

                            container(
                                column![
                                    appearance_settings_col,
                                    Space::new().height(Length::from(20)),
                                    tray_text_mode_toggle,
                                    Space::new().height(Length::from(20)),
                                    controls_settings_col,
                                    Space::new().height(Length::from(20)),
                                    audio_settings_col,
                                    self.save_statuses.get(&Tab::Settings)
                                        .map(|status| text(status).size(12).style(text::danger)),
                                ]
                            )
                                .padding(20)
                                .width(Length::Fill)
                                .height(Length::Fill)
                        },
                        Tab::AddDevice => {
                            container(
                                column![
                                    text("Pick a paired device to add:").size(18),
                                    Space::new().height(Length::from(10)),
                                    {
                                        let mut list_col = column![].spacing(12);
                                        for device in self.paired_devices.clone() {
                                            if !devices_list.contains_key(&device.1.to_string()) {
                                                let mut item_col = column![].spacing(8);
                                                let mut row_elements = vec![
                                                    column![
                                                        text(device.0.to_string()).size(16),
                                                        text(device.1.to_string()).size(12)
                                                    ].into(),
                                                    Space::new().height(Length::Fill).into(),
                                                ];
                                                if !matches!(&self.pending_add_device, Some((_, addr)) if addr == &device.1) {
                                                    row_elements.push(
                                                        button(
                                                            text("Add").size(14).width(120).align_y(Center).align_x(Center)
                                                        )
                                                            .style(
                                                                |theme: &Theme, _status| {
                                                                    let mut style = Style::default();
                                                                    style.text_color = theme.palette().text;
                                                                    style.background = Some(Background::Color(theme.palette().primary.scale_alpha(0.5)));
                                                                    style.border = Border {
                                                                        width: 1.0,
                                                                        color: theme.palette().primary,
                                                                        radius: Radius::from(8.0),
                                                                    };
                                                                    style
                                                                }
                                                            )
                                                            .padding(8)
                                                            .on_press(Message::StartAddDevice(device.0.clone(), device.1))
                                                            .into()
                                                    );
                                                }
                                                item_col = item_col.push(row(row_elements).align_y(Center));

                                                if let Some((_, pending_addr)) = &self.pending_add_device
                                                    && pending_addr == &device.1 {
                                                        item_col = item_col.push(
                                                            row![
                                                                text("Device Type:").size(16),
                                                                Space::new().width(Length::Fill),
                                                                combo_box(
                                                                    &self.device_type_state,
                                                                    "Select device type",
                                                                    self.selected_device_type.as_ref(),
                                                                    Message::SelectDeviceType
                                                                )
                                                                    .input_style(
                                                                        |theme: &Theme, _status| {
                                                                            text_input::Style {
                                                                                background: Background::Color(theme.palette().background),
                                                                                border: Border {
                                                                                    width: 1.0,
                                                                                    color: theme.palette().text,
                                                                                    radius: Radius::from(8.0),
                                                                                },
                                                                                icon: Default::default(),
                                                                                placeholder: theme.palette().text.scale_alpha(0.5),
                                                                                value: theme.palette().text,
                                                                                selection: theme.palette().primary
                                                                            }
                                                                        }
                                                                    )
                                                                    .menu_style(
                                                                        |theme: &Theme| {
                                                                            menu::Style {
                                                                                background: Background::Color(theme.palette().background),
                                                                                border: Border {
                                                                                    width: 1.0,
                                                                                    color: theme.palette().text,
                                                                                    radius: Radius::from(8.0)
                                                                                },
                                                                                text_color: theme.palette().text,
                                                                                selected_text_color: theme.palette().text,
                                                                                selected_background: Background::Color(theme.palette().primary.scale_alpha(0.3)),
                                                                                shadow: Default::default()
                                                                            }
                                                                        }
                                                                    )
                                                                    .width(Length::from(200))
                                                            ]
                                                        );
                                                        item_col = item_col.push(
                                                            row![
                                                                Space::new().width(Length::Fill),
                                                                button(text("Cancel").size(16).width(Length::Fill).center())
                                                                    .on_press(Message::CancelAddDevice)
                                                                    .style(|theme: &Theme, _status| {
                                                                        let mut style = Style::default();
                                                                        style.background = Some(Background::Color(theme.palette().primary.scale_alpha(0.1)));
                                                                        style.text_color = theme.palette().text;
                                                                        style.border = Border::default().rounded(8.0);
                                                                        style
                                                                    })
                                                                    .width(Length::from(120))
                                                                    .padding(4),
                                                                Space::new().width(Length::from(20)),
                                                                button(text("Add Device").size(16).width(Length::Fill).center())
                                                                    .on_press(Message::ConfirmAddDevice)
                                                                    .style(|theme: &Theme, _status| {
                                                                        let mut style = Style::default();
                                                                        style.background = Some(Background::Color(theme.palette().primary.scale_alpha(0.3)));
                                                                        style.text_color = theme.palette().text;
                                                                        style.border = Border::default().rounded(8.0);
                                                                        style
                                                                    })
                                                                    .width(Length::from(120))
                                                                    .padding(4),
                                                            ]
                                                            .align_y(Center)
                                                            .width(Length::Fill)
                                                        );
                                                    }
                                                list_col = list_col.push(
                                                    container(item_col)
                                                        .padding(8)
                                                        .style(
                                                            |theme: &Theme| {
                                                                let mut style = container::Style::default();
                                                                style.background = Some(Background::Color(theme.palette().primary.scale_alpha(0.1)));
                                                                let mut border = Border::default();
                                                                border.color = theme.palette().text;
                                                                style.border = border.rounded(8);
                                                                style
                                                            }
                                                        )
                                                );
                                            }
                                        }
                                        if self.paired_devices.iter().all(|device| devices_list.contains_key(&device.1.to_string())) && self.pending_add_device.is_none() {
                                            list_col = list_col.push(
                                                container(
                                                    text("No new paired devices found. All paired devices are already added.").size(16)
                                                )
                                                .width(Length::Fill)
                                            );
                                        }
                                        scrollable(list_col)
                                            .height(Length::Fill)
                                            .width(Length::Fill)
                                    }
                                ]
                            )
                            .padding(20)
                            .height(Length::Fill)
                            .width(Length::Fill)
                        }
                    };

                    pane_grid::Content::new(content)
                }
            }
        })
            .width(Length::Fill)
            .height(Length::Fill)
            .on_resize(20, Message::Resized);

        container(pane_grid).into()
    }

    fn theme(&self, _id: window::Id) -> Theme {
        self.selected_theme.into()
    }

    fn subscription(&self) -> Subscription<Message> {
        window::close_events().map(Message::WindowClosed)
    }
}

async fn wait_for_message(ui_rx: Arc<Mutex<UnboundedReceiver<BluetoothUIMessage>>>) -> Message {
    let mut rx = ui_rx.lock().await;
    match rx.recv().await {
        Some(msg) => Message::BluetoothMessage(msg),
        None => {
            error!("UI message channel closed");
            Message::BluetoothMessage(BluetoothUIMessage::NoOp)
        }
    }
}

// async fn load_paired_devices() -> HashMap<String, Address> {
//     let mut devices = HashMap::new();
//
//     let session = Session::new().await.ok().unwrap();
//     let adapter = session.default_adapter().await.ok().unwrap();
//     let addresses = adapter.device_addresses().await.ok().unwrap();
//     for addr in addresses {
//         let device = adapter.device(addr).ok().unwrap();
//         let paired = device.is_paired().await.ok().unwrap();
//         if paired {
//             let name = device
//                 .name()
//                 .await
//                 .ok()
//                 .flatten()
//                 .unwrap_or_else(|| "Unknown".to_string());
//             devices.insert(name, addr);
//         }
//     }
//
//     devices
// }

#[cfg(test)]
mod tests {
    use super::*;

    fn device(name: &str, type_: DeviceType) -> DeviceData {
        DeviceData {
            name: name.to_string(),
            type_,
            information: None,
        }
    }

    /// Two devices, one with `information`, as a devices.json map.
    fn two_devices() -> serde_json::Map<String, serde_json::Value> {
        let mut nothing = device("Ear (2)", DeviceType::Nothing);
        nothing.information = Some(crate::devices::enums::DeviceInformation::Nothing(
            crate::devices::nothing::NothingInformation {
                serial_number: "SN1".to_string(),
                firmware_version: "1.0.0".to_string(),
            },
        ));
        let mut map = serde_json::Map::new();
        map.insert(
            "AA:AA:AA:AA:AA:AA".into(),
            serde_json::to_value(nothing).unwrap(),
        );
        map.insert(
            "BB:BB:BB:BB:BB:BB".into(),
            serde_json::to_value(device("AirPods Pro", DeviceType::AirPods)).unwrap(),
        );
        map
    }

    fn parse(json: &str) -> serde_json::Map<String, serde_json::Value> {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn devices_with_added_missing_file_starts_empty() {
        let new = device("New", DeviceType::AirPods);
        let json = devices_with_added(
            Err(std::io::ErrorKind::NotFound.into()),
            "CC:CC:CC:CC:CC:CC",
            new.clone(),
        )
        .unwrap();
        let map = parse(&json);
        assert_eq!(map.len(), 1);
        assert_eq!(map["CC:CC:CC:CC:CC:CC"], serde_json::to_value(new).unwrap());
    }

    #[test]
    fn devices_with_added_keeps_existing() {
        let existing = two_devices();
        let new = device("New", DeviceType::AirPods);
        let json = devices_with_added(
            Ok(serde_json::to_string(&existing).unwrap()),
            "CC:CC:CC:CC:CC:CC",
            new.clone(),
        )
        .unwrap();
        let map = parse(&json);
        assert_eq!(map.len(), 3);
        for (addr, value) in &existing {
            assert_eq!(&map[addr], value, "{addr} changed");
        }
        assert_eq!(map["CC:CC:CC:CC:CC:CC"], serde_json::to_value(new).unwrap());
    }

    #[test]
    fn devices_with_added_replaces_same_address() {
        let existing = two_devices();
        let new = device("Renamed", DeviceType::AirPods);
        let json = devices_with_added(
            Ok(serde_json::to_string(&existing).unwrap()),
            "AA:AA:AA:AA:AA:AA",
            new.clone(),
        )
        .unwrap();
        let map = parse(&json);
        assert_eq!(map.len(), 2);
        assert_eq!(map["AA:AA:AA:AA:AA:AA"], serde_json::to_value(new).unwrap());
        assert_eq!(map["BB:BB:BB:BB:BB:BB"], existing["BB:BB:BB:BB:BB:BB"]);
    }

    #[test]
    fn devices_with_added_rejects_unparsable_file() {
        let json =
            r#"{"AA:AA:AA:AA:AA:AA": {"name": "x", "type_": "AirPods", "information": null},}"#;
        let err = devices_with_added(
            Ok(json.to_string()),
            "CC:CC:CC:CC:CC:CC",
            device("New", DeviceType::AirPods),
        )
        .unwrap_err();
        assert!(err.to_string().contains("trailing comma"), "{err}");
    }

    #[test]
    fn devices_with_added_rejects_read_errors() {
        for kind in [
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::InvalidData,
        ] {
            let err = devices_with_added(
                Err(kind.into()),
                "CC:CC:CC:CC:CC:CC",
                device("New", DeviceType::AirPods),
            )
            .unwrap_err();
            assert_eq!(err.kind(), kind);
        }
    }

    fn info(component: BatteryComponent, level: u8, status: BatteryStatus) -> BatteryInfo {
        BatteryInfo {
            component,
            level,
            status,
        }
    }

    /// The AACP case entry sent while the buds are out of the case.
    fn case_disconnected(level: u8) -> BatteryInfo {
        info(BatteryComponent::Case, level, BatteryStatus::Disconnected)
    }

    fn prev_all() -> Vec<BatteryInfo> {
        vec![
            info(BatteryComponent::Left, 80, BatteryStatus::NotCharging),
            info(BatteryComponent::Right, 75, BatteryStatus::NotCharging),
            info(BatteryComponent::Case, 60, BatteryStatus::NotCharging),
        ]
    }

    #[test]
    fn merge_battery_case_only_keeps_buds() {
        let update = vec![info(BatteryComponent::Case, 55, BatteryStatus::Charging)];
        let merged = merge_battery(&prev_all(), &update);
        assert_eq!(
            merged,
            vec![
                info(BatteryComponent::Left, 80, BatteryStatus::NotCharging),
                info(BatteryComponent::Right, 75, BatteryStatus::NotCharging),
                info(BatteryComponent::Case, 55, BatteryStatus::Charging),
            ]
        );
    }

    #[test]
    fn merge_battery_buds_only_keeps_case() {
        let update = vec![
            info(BatteryComponent::Left, 70, BatteryStatus::NotCharging),
            info(BatteryComponent::Right, 65, BatteryStatus::Charging),
        ];
        let merged = merge_battery(&prev_all(), &update);
        assert_eq!(
            merged,
            vec![
                info(BatteryComponent::Left, 70, BatteryStatus::NotCharging),
                info(BatteryComponent::Right, 65, BatteryStatus::Charging),
                info(BatteryComponent::Case, 60, BatteryStatus::NotCharging),
            ]
        );
    }

    #[test]
    fn merge_battery_full_update_replaces_all() {
        let update = vec![
            info(BatteryComponent::Left, 10, BatteryStatus::Charging),
            info(BatteryComponent::Right, 20, BatteryStatus::Charging),
            info(BatteryComponent::Case, 30, BatteryStatus::Disconnected),
        ];
        let merged = merge_battery(&prev_all(), &update);
        assert_eq!(
            merged,
            vec![
                info(BatteryComponent::Left, 10, BatteryStatus::Charging),
                info(BatteryComponent::Right, 20, BatteryStatus::Charging),
                info(BatteryComponent::Case, 60, BatteryStatus::NotCharging),
            ]
        );
    }

    #[test]
    fn merge_battery_empty_prev_takes_update() {
        let update = vec![
            info(BatteryComponent::Left, 40, BatteryStatus::NotCharging),
            info(BatteryComponent::Case, 50, BatteryStatus::NotCharging),
        ];
        let merged = merge_battery(&[], &update);
        assert_eq!(merged, update);
    }

    #[test]
    fn merge_battery_appends_new_component() {
        let prev = vec![info(BatteryComponent::Left, 80, BatteryStatus::NotCharging)];
        let update = vec![info(BatteryComponent::Headphone, 90, BatteryStatus::NotCharging)];
        let merged = merge_battery(&prev, &update);
        assert_eq!(
            merged,
            vec![
                info(BatteryComponent::Left, 80, BatteryStatus::NotCharging),
                info(BatteryComponent::Headphone, 90, BatteryStatus::NotCharging),
            ]
        );
    }

    #[test]
    fn merge_battery_disconnected_case_keeps_ble_level() {
        let prev = vec![
            info(BatteryComponent::Left, 70, BatteryStatus::NotCharging),
            info(BatteryComponent::Right, 72, BatteryStatus::NotCharging),
            info(BatteryComponent::Case, 80, BatteryStatus::NotCharging),
        ];
        let update = vec![case_disconnected(30)];
        let merged = merge_battery(&prev, &update);
        assert_eq!(merged, prev);
    }

    #[test]
    fn merge_battery_disconnected_keeps_charging_entry() {
        let prev = vec![info(BatteryComponent::Case, 45, BatteryStatus::Charging)];
        let update = vec![case_disconnected(30)];
        let merged = merge_battery(&prev, &update);
        assert_eq!(merged, prev);
    }

    #[test]
    fn merge_battery_disconnected_without_prev_is_appended() {
        let prev = vec![
            info(BatteryComponent::Left, 80, BatteryStatus::NotCharging),
            info(BatteryComponent::Right, 75, BatteryStatus::NotCharging),
        ];
        let update = vec![case_disconnected(30)];
        let merged = merge_battery(&prev, &update);
        assert_eq!(
            merged,
            vec![
                info(BatteryComponent::Left, 80, BatteryStatus::NotCharging),
                info(BatteryComponent::Right, 75, BatteryStatus::NotCharging),
                info(BatteryComponent::Case, 30, BatteryStatus::Disconnected),
            ]
        );
    }

    #[test]
    fn merge_battery_disconnected_over_disconnected_replaces() {
        let prev = vec![case_disconnected(30)];
        let update = vec![case_disconnected(25)];
        let merged = merge_battery(&prev, &update);
        assert_eq!(merged, update);
    }

    #[test]
    fn merge_battery_known_status_over_disconnected_replaces() {
        let prev = vec![case_disconnected(30)];

        let update = vec![info(BatteryComponent::Case, 55, BatteryStatus::NotCharging)];
        assert_eq!(merge_battery(&prev, &update), update);

        let update = vec![info(BatteryComponent::Case, 56, BatteryStatus::Charging)];
        assert_eq!(merge_battery(&prev, &update), update);
    }

    #[test]
    fn merge_battery_entry_tray_shaped() {
        assert_eq!(
            merge_battery_entry(
                Some((80, BatteryStatus::NotCharging)),
                (30, BatteryStatus::Disconnected)
            ),
            (80, BatteryStatus::NotCharging)
        );
        assert_eq!(
            merge_battery_entry(None, (30, BatteryStatus::Disconnected)),
            (30, BatteryStatus::Disconnected)
        );
        assert_eq!(
            merge_battery_entry(
                Some((30, BatteryStatus::Disconnected)),
                (55, BatteryStatus::Charging)
            ),
            (55, BatteryStatus::Charging)
        );
    }

    #[test]
    fn merge_battery_entry_known_update_always_wins() {
        assert_eq!(
            merge_battery_entry(
                Some((80, BatteryStatus::Charging)),
                (79, BatteryStatus::NotCharging)
            ),
            (79, BatteryStatus::NotCharging)
        );
        assert_eq!(
            merge_battery_entry(
                Some((80, BatteryStatus::NotCharging)),
                (81, BatteryStatus::Charging)
            ),
            (81, BatteryStatus::Charging)
        );
        assert_eq!(
            merge_battery_entry(None, (50, BatteryStatus::NotCharging)),
            (50, BatteryStatus::NotCharging)
        );
        assert_eq!(
            merge_battery_entry(
                Some((30, BatteryStatus::Disconnected)),
                (25, BatteryStatus::Disconnected)
            ),
            (25, BatteryStatus::Disconnected)
        );
    }

    #[test]
    fn battery_level_label_shows_dash_for_unknown_level() {
        assert_eq!(battery_level_label(None), "--");
        assert_eq!(battery_level_label(Some(85)), "85%");
    }

    #[test]
    fn battery_label_dash_for_missing_or_disconnected() {
        assert_eq!(battery_label(None), "--");
        let disconnected = info(BatteryComponent::Case, 30, BatteryStatus::Disconnected);
        assert_eq!(battery_label(Some(&disconnected)), "--");
    }

    #[test]
    fn battery_label_percent_for_known_status() {
        let not_charging = info(BatteryComponent::Left, 85, BatteryStatus::NotCharging);
        assert_eq!(battery_label(Some(&not_charging)), "85%");
        let charging = info(BatteryComponent::Case, 85, BatteryStatus::Charging);
        assert_eq!(battery_label(Some(&charging)), "85%");
    }

    #[test]
    fn battery_tab_label_headphone_disconnected_shows_dashes() {
        let battery = vec![info(BatteryComponent::Headphone, 85, BatteryStatus::Disconnected)];
        assert_eq!(battery_tab_label(&battery), "\u{100EB9} --");
    }

    #[test]
    fn battery_tab_label_headphone_known_level() {
        let battery = vec![info(BatteryComponent::Headphone, 85, BatteryStatus::NotCharging)];
        assert_eq!(battery_tab_label(&battery), "\u{100EB9} 85%");
    }

    #[test]
    fn battery_tab_label_headphone_charging() {
        // Headphone wins over any Left/Right/Case entries present.
        let battery = vec![
            info(BatteryComponent::Left, 80, BatteryStatus::NotCharging),
            info(BatteryComponent::Headphone, 85, BatteryStatus::Charging),
            info(BatteryComponent::Right, 75, BatteryStatus::Charging),
            info(BatteryComponent::Case, 60, BatteryStatus::NotCharging),
        ];
        assert_eq!(battery_tab_label(&battery), "\u{100EB9} 85%\u{1002E6}");
    }

    #[test]
    fn battery_tab_label_buds_form() {
        let battery = vec![
            info(BatteryComponent::Left, 80, BatteryStatus::Charging),
            case_disconnected(30),
        ];
        assert_eq!(
            battery_tab_label(&battery),
            "\u{1018E5} 80%\u{1002E6} \u{1018E8} -- \u{100E6C} --"
        );

        let battery = vec![
            info(BatteryComponent::Left, 80, BatteryStatus::NotCharging),
            info(BatteryComponent::Right, 75, BatteryStatus::Charging),
            info(BatteryComponent::Case, 60, BatteryStatus::Charging),
        ];
        assert_eq!(
            battery_tab_label(&battery),
            "\u{1018E5} 80% \u{1018E8} 75%\u{1002E6} \u{100E6C} 60%\u{1002E6}"
        );

        assert_eq!(
            battery_tab_label(&[]),
            "\u{1018E5} -- \u{1018E8} -- \u{100E6C} --"
        );
    }

    const MAC: &str = "AA:BB:CC:DD:EE:FF";

    #[test]
    fn connect_mode_write_preserves_other_keys() {
        let prefs = r#"{"switching":"legacy","exportBattery":false,
            "AA:BB:CC:DD:EE:FF":{"autoConnect":false},
            "11:22:33:44:55:66":{"connectMode":"when_last_connected"}}"#;
        assert_eq!(
            with_connect_mode(Some(prefs), MAC, ConnectMode::WhenLastConnected),
            Some(serde_json::json!({
                "switching": "legacy",
                "exportBattery": false,
                MAC: {"autoConnect": false, "connectMode": "when_last_connected"},
                "11:22:33:44:55:66": {"connectMode": "when_last_connected"},
            }))
        );
        // A device without an entry gets one; an existing value is replaced.
        let prefs = r#"{"switching":"off","AA:BB:CC:DD:EE:FF":{"connectMode":"when_last_connected"}}"#;
        assert_eq!(
            with_connect_mode(Some(prefs), MAC, ConnectMode::Automatically),
            Some(serde_json::json!({"switching": "off", MAC: {"connectMode": "automatically"}}))
        );
        assert_eq!(
            with_connect_mode(Some(r#"{"switching":"off"}"#), MAC, ConnectMode::Automatically),
            Some(serde_json::json!({"switching": "off", MAC: {"connectMode": "automatically"}}))
        );
    }

    #[test]
    fn connect_mode_write_creates_missing_file() {
        assert_eq!(
            with_connect_mode(None, MAC, ConnectMode::WhenLastConnected),
            Some(serde_json::json!({MAC: {"connectMode": "when_last_connected"}}))
        );
    }

    #[test]
    fn connect_mode_write_refuses_invalid_file() {
        for prefs in [
            "",
            "not json",
            "[]",
            "3",
            "null",
            r#"{"AA:BB:CC:DD:EE:FF":3}"#,
            r#"{"AA:BB:CC:DD:EE:FF":null}"#,
            r#"{"AA:BB:CC:DD:EE:FF":"off"}"#,
        ] {
            assert_eq!(
                with_connect_mode(Some(prefs), MAC, ConnectMode::WhenLastConnected),
                None,
                "{prefs}"
            );
        }
    }

    #[test]
    fn connect_mode_round_trips() {
        for mode in [ConnectMode::Automatically, ConnectMode::WhenLastConnected] {
            for prefs in [None, Some(r#"{"AA:BB:CC:DD:EE:FF":{"autoConnect":true}}"#)] {
                let written = with_connect_mode(prefs, MAC, mode).unwrap();
                assert_eq!(
                    crate::utils::connect_mode_from_value(Some(&written), MAC),
                    mode
                );
                // Through the text the arm writes, too.
                assert_eq!(
                    crate::utils::parse_connect_mode(&format!("{written:#}"), MAC),
                    mode
                );
            }
        }
    }

    #[test]
    fn connect_mode_hint_only_outside_handoff() {
        use crate::utils::SwitchingMode;
        assert_eq!(connect_mode_hint(SwitchingMode::Handoff), None);
        for mode in [SwitchingMode::Legacy, SwitchingMode::Off] {
            let hint = connect_mode_hint(mode).unwrap();
            assert!(hint.to_lowercase().contains("handoff"), "{mode:?}: {hint}");
        }
    }

    #[test]
    fn save_status_text_names_reason() {
        let err = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        let text = save_status_text(&SaveError::Io(err));
        assert_eq!(text, "Couldn't save: permission denied");
        assert_eq!(
            save_status_text(&SaveError::NotSettingsJson),
            "Couldn't save: preferences.json isn't valid settings JSON"
        );
    }

    #[test]
    fn app_settings_json_has_all_four_keys() {
        for (theme, flag, codec) in [
            (MyTheme::Nord, true, PreferredCodec::SbcXq),
            (MyTheme::Light, false, PreferredCodec::Aac),
        ] {
            let json = app_settings_json(theme, flag, !flag, codec);
            let obj = json.as_object().unwrap();
            let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
            keys.sort_unstable();
            assert_eq!(
                keys,
                ["preferred_codec", "stem_control", "theme", "tray_text_mode"]
            );
            assert_eq!(obj["theme"], serde_json::to_value(theme).unwrap());
            assert_eq!(obj["tray_text_mode"], serde_json::to_value(flag).unwrap());
            assert_eq!(obj["stem_control"], serde_json::to_value(!flag).unwrap());
            assert_eq!(obj["preferred_codec"], serde_json::to_value(codec).unwrap());
        }
    }

    fn denied() -> Result<(), SaveError> {
        Err(SaveError::Io(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied,
        )))
    }

    #[test]
    fn save_failure_sets_status() {
        let mut statuses = HashMap::new();
        apply_save_result(&mut statuses, Tab::Settings, denied());
        assert!(statuses[&Tab::Settings].starts_with("Couldn't save: "));
        let device = Tab::Device(MAC.to_string());
        apply_save_result(&mut statuses, device.clone(), Err(SaveError::NotSettingsJson));
        assert_eq!(
            statuses[&device],
            save_status_text(&SaveError::NotSettingsJson)
        );
    }

    #[test]
    fn save_success_clears_status() {
        let mut statuses = HashMap::new();
        apply_save_result(&mut statuses, Tab::Settings, denied());
        apply_save_result(&mut statuses, Tab::Settings, Ok(()));
        assert!(statuses.is_empty());
    }

    #[test]
    fn save_success_keeps_other_scope_status() {
        let mut statuses = HashMap::new();
        let device = Tab::Device(MAC.to_string());
        let other = Tab::Device("11:22:33:44:55:66".to_string());
        apply_save_result(&mut statuses, Tab::Settings, denied());
        apply_save_result(&mut statuses, device.clone(), denied());
        apply_save_result(&mut statuses, other.clone(), denied());
        apply_save_result(&mut statuses, device.clone(), Ok(()));
        assert!(!statuses.contains_key(&device));
        assert!(statuses.contains_key(&Tab::Settings));
        assert!(statuses.contains_key(&other));
        apply_save_result(&mut statuses, Tab::Settings, Ok(()));
        assert_eq!(statuses.keys().collect::<Vec<_>>(), vec![&other]);
    }
}

