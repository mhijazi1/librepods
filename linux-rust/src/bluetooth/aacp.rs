use crate::devices::airpods::AirPodsInformation;
use crate::devices::enums::{DeviceData, DeviceInformation, DeviceType};
use crate::utils::get_devices_path;
use bluer::{
    Address, AddressType, Error, Result,
    l2cap::{SeqPacket, Socket, SocketAddr},
};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep};

const PSM: u16 = 0x1001;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const HEADER_BYTES: [u8; 4] = [0x04, 0x00, 0x04, 0x00];

pub mod opcodes {
    pub const SET_FEATURE_FLAGS: u8 = 0x4D;
    pub const REQUEST_NOTIFICATIONS: u8 = 0x0F;
    pub const BATTERY_INFO: u8 = 0x04;
    pub const CONTROL_COMMAND: u8 = 0x09;
    pub const EAR_DETECTION: u8 = 0x06;
    pub const CONVERSATION_AWARENESS: u8 = 0x4B;
    pub const INFORMATION: u8 = 0x1D;
    pub const RENAME: u8 = 0x1A;
    pub const PROXIMITY_KEYS_REQ: u8 = 0x30;
    pub const PROXIMITY_KEYS_RSP: u8 = 0x31;
    pub const STEM_PRESS: u8 = 0x19;
    pub const EQ_DATA: u8 = 0x53;
    pub const CONNECTED_DEVICES: u8 = 0x2E;
    pub const AUDIO_SOURCE: u8 = 0x0E;
    pub const SMART_ROUTING: u8 = 0x10;
    pub const SMART_ROUTING_RESP: u8 = 0x11;
    pub const SEND_CONNECTED_MAC: u8 = 0x14;
    pub const HEADTRACKING: u8 = 0x17;
    pub const TIPI_3: u8 = 0x0C;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ControlCommandStatus {
    pub identifier: ControlCommandIdentifiers,
    pub value: Vec<u8>,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ControlCommandIdentifiers {
    MicMode = 0x01,
    ButtonSendMode = 0x05,
    VoiceTrigger = 0x12,
    SingleClickMode = 0x14,
    DoubleClickMode = 0x15,
    ClickHoldMode = 0x16,
    DoubleClickInterval = 0x17,
    ClickHoldInterval = 0x18,
    ListeningModeConfigs = 0x1A,
    OneBudAncMode = 0x1B,
    CrownRotationDirection = 0x1C,
    ListeningMode = 0x0D,
    AutoAnswerMode = 0x1E,
    ChimeVolume = 0x1F,
    VolumeSwipeInterval = 0x23,
    CallManagementConfig = 0x24,
    VolumeSwipeMode = 0x25,
    AdaptiveVolumeConfig = 0x26,
    SoftwareMuteConfig = 0x27,
    ConversationDetectConfig = 0x28,
    Ssl = 0x29,
    HearingAid = 0x2C,
    AutoAncStrength = 0x2E,
    HpsGainSwipe = 0x2F,
    HrmState = 0x30,
    InCaseToneConfig = 0x31,
    SiriMultitoneConfig = 0x32,
    HearingAssistConfig = 0x33,
    AllowOffOption = 0x34,
    StemConfig = 0x39,
    SleepDetectionConfig = 0x35,
    AllowAutoConnect = 0x36,
    EarDetectionConfig = 0x0A,
    AutomaticConnectionConfig = 0x20,
    OwnsConnection = 0x06,
}

impl ControlCommandIdentifiers {
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x01 => Some(Self::MicMode),
            0x05 => Some(Self::ButtonSendMode),
            0x12 => Some(Self::VoiceTrigger),
            0x14 => Some(Self::SingleClickMode),
            0x15 => Some(Self::DoubleClickMode),
            0x16 => Some(Self::ClickHoldMode),
            0x17 => Some(Self::DoubleClickInterval),
            0x18 => Some(Self::ClickHoldInterval),
            0x1A => Some(Self::ListeningModeConfigs),
            0x1B => Some(Self::OneBudAncMode),
            0x1C => Some(Self::CrownRotationDirection),
            0x0D => Some(Self::ListeningMode),
            0x1E => Some(Self::AutoAnswerMode),
            0x1F => Some(Self::ChimeVolume),
            0x23 => Some(Self::VolumeSwipeInterval),
            0x24 => Some(Self::CallManagementConfig),
            0x25 => Some(Self::VolumeSwipeMode),
            0x26 => Some(Self::AdaptiveVolumeConfig),
            0x27 => Some(Self::SoftwareMuteConfig),
            0x28 => Some(Self::ConversationDetectConfig),
            0x29 => Some(Self::Ssl),
            0x2C => Some(Self::HearingAid),
            0x2E => Some(Self::AutoAncStrength),
            0x2F => Some(Self::HpsGainSwipe),
            0x30 => Some(Self::HrmState),
            0x31 => Some(Self::InCaseToneConfig),
            0x32 => Some(Self::SiriMultitoneConfig),
            0x33 => Some(Self::HearingAssistConfig),
            0x34 => Some(Self::AllowOffOption),
            0x39 => Some(Self::StemConfig),
            0x35 => Some(Self::SleepDetectionConfig),
            0x36 => Some(Self::AllowAutoConnect),
            0x0A => Some(Self::EarDetectionConfig),
            0x20 => Some(Self::AutomaticConnectionConfig),
            0x06 => Some(Self::OwnsConnection),
            _ => None,
        }
    }
}

impl std::fmt::Display for ControlCommandIdentifiers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            ControlCommandIdentifiers::MicMode => "Mic Mode",
            ControlCommandIdentifiers::ButtonSendMode => "Button Send Mode",
            ControlCommandIdentifiers::VoiceTrigger => "Voice Trigger",
            ControlCommandIdentifiers::SingleClickMode => "Single Click Mode",
            ControlCommandIdentifiers::DoubleClickMode => "Double Click Mode",
            ControlCommandIdentifiers::ClickHoldMode => "Click Hold Mode",
            ControlCommandIdentifiers::DoubleClickInterval => "Double Click Interval",
            ControlCommandIdentifiers::ClickHoldInterval => "Click Hold Interval",
            ControlCommandIdentifiers::ListeningModeConfigs => "Listening Mode Configs",
            ControlCommandIdentifiers::OneBudAncMode => "One Bud ANC Mode",
            ControlCommandIdentifiers::CrownRotationDirection => "Crown Rotation Direction",
            ControlCommandIdentifiers::ListeningMode => "Listening Mode",
            ControlCommandIdentifiers::AutoAnswerMode => "Auto Answer Mode",
            ControlCommandIdentifiers::ChimeVolume => "Chime Volume",
            ControlCommandIdentifiers::VolumeSwipeInterval => "Volume Swipe Interval",
            ControlCommandIdentifiers::CallManagementConfig => "Call Management Config",
            ControlCommandIdentifiers::VolumeSwipeMode => "Volume Swipe Mode",
            ControlCommandIdentifiers::AdaptiveVolumeConfig => "Adaptive Volume Config",
            ControlCommandIdentifiers::SoftwareMuteConfig => "Software Mute Config",
            ControlCommandIdentifiers::ConversationDetectConfig => "Conversation Detect Config",
            ControlCommandIdentifiers::Ssl => "SSL",
            ControlCommandIdentifiers::HearingAid => "Hearing Aid",
            ControlCommandIdentifiers::AutoAncStrength => "Auto ANC Strength",
            ControlCommandIdentifiers::HpsGainSwipe => "HPS Gain Swipe",
            ControlCommandIdentifiers::HrmState => "HRM State",
            ControlCommandIdentifiers::InCaseToneConfig => "In Case Tone Config",
            ControlCommandIdentifiers::SiriMultitoneConfig => "Siri Multitone Config",
            ControlCommandIdentifiers::HearingAssistConfig => "Hearing Assist Config",
            ControlCommandIdentifiers::AllowOffOption => "Allow Off Option",
            ControlCommandIdentifiers::StemConfig => "Stem Config",
            ControlCommandIdentifiers::SleepDetectionConfig => "Sleep Detection Config",
            ControlCommandIdentifiers::AllowAutoConnect => "Allow Auto Connect",
            ControlCommandIdentifiers::EarDetectionConfig => "Ear Detection Config",
            ControlCommandIdentifiers::AutomaticConnectionConfig => "Automatic Connection Config",
            ControlCommandIdentifiers::OwnsConnection => "Owns Connection",
        };
        write!(f, "{}", name)
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum ProximityKeyType {
    Irk = 0x01,
    EncKey = 0x04,
}

impl ProximityKeyType {
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x01 => Some(Self::Irk),
            0x04 => Some(Self::EncKey),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StemPressType {
    SinglePress = 0x05,
    DoublePress = 0x06,
    TriplePress = 0x07,
    LongPress = 0x08,
}

impl StemPressType {
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x05 => Some(Self::SinglePress),
            0x06 => Some(Self::DoublePress),
            0x07 => Some(Self::TriplePress),
            0x08 => Some(Self::LongPress),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StemPressBudType {
    Left = 0x01,
    Right = 0x02,
}

impl StemPressBudType {
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x01 => Some(Self::Left),
            0x02 => Some(Self::Right),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioSourceType {
    None = 0x00,
    Call = 0x01,
    Media = 0x02,
    Unknown = 0xFF,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatteryComponent {
    Headphone = 1,
    Left = 4,
    Right = 2,
    Case = 8,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatteryStatus {
    Charging = 1,
    NotCharging = 2,
    Disconnected = 4,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EarDetectionStatus {
    InEar = 0x00,
    OutOfEar = 0x01,
    InCase = 0x02,
    Disconnected = 0x03,
}

impl AudioSourceType {
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(Self::None),
            0x01 => Some(Self::Call),
            0x02 => Some(Self::Media),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioSource {
    pub mac: String,
    pub r#type: AudioSourceType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatteryInfo {
    pub component: BatteryComponent,
    pub level: u8,
    pub status: BatteryStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectedDevice {
    pub mac: String,
    pub info1: u8,
    pub info2: u8,
    pub r#type: Option<String>,
}

#[derive(Debug, Clone)]
pub enum AACPEvent {
    BatteryInfo(Vec<BatteryInfo>),
    ControlCommand(ControlCommandStatus),
    EarDetection(Vec<EarDetectionStatus>, Vec<EarDetectionStatus>),
    ConversationalAwareness(u8),
    ProximityKeys(Vec<(u8, Vec<u8>)>),
    AudioSource(AudioSource),
    ConnectedDevices(Vec<ConnectedDevice>, Vec<ConnectedDevice>),
    OwnershipToFalseRequest,
    StemPress(StemPressType, StemPressBudType),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AirPodsLEKeys {
    pub irk: String,
    pub enc_key: String,
}

pub struct AACPManagerState {
    pub sender: Option<mpsc::Sender<Vec<u8>>>,
    pub control_command_status_list: Vec<ControlCommandStatus>,
    pub control_command_subscribers:
        HashMap<ControlCommandIdentifiers, Vec<mpsc::UnboundedSender<Vec<u8>>>>,
    pub owns: bool,
    pub old_connected_devices: Vec<ConnectedDevice>,
    pub connected_devices: Vec<ConnectedDevice>,
    pub audio_source: Option<AudioSource>,
    pub battery_info: Vec<BatteryInfo>,
    pub conversational_awareness_status: u8,
    pub old_ear_detection_status: Vec<EarDetectionStatus>,
    pub ear_detection_status: Vec<EarDetectionStatus>,
    event_tx: Option<mpsc::UnboundedSender<AACPEvent>>,
    pub devices: HashMap<String, DeviceData>,
    pub airpods_mac: Option<Address>,
}

impl AACPManagerState {
    fn clear_connection(&mut self) {
        self.sender = None;
        self.owns = false;
        self.control_command_status_list.clear();
        self.connected_devices.clear();
        self.old_connected_devices.clear();
        self.audio_source = None;
        self.ear_detection_status.clear();
        self.old_ear_detection_status.clear();
    }

    fn new() -> Self {
        let devices: HashMap<String, DeviceData> = std::fs::read_to_string(get_devices_path())
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        AACPManagerState {
            sender: None,
            control_command_status_list: Vec::new(),
            control_command_subscribers: HashMap::new(),
            owns: false,
            old_connected_devices: Vec::new(),
            connected_devices: Vec::new(),
            audio_source: None,
            battery_info: Vec::new(),
            conversational_awareness_status: 0,
            old_ear_detection_status: Vec::new(),
            ear_detection_status: Vec::new(),
            event_tx: None,
            devices,
            airpods_mac: None,
        }
    }
}

#[derive(Clone)]
pub struct AACPManager {
    pub state: Arc<Mutex<AACPManagerState>>,
    tasks: Arc<Mutex<JoinSet<()>>>,
}

impl AACPManager {
    pub async fn disconnect(&self) {
        self.tasks.lock().await.shutdown().await;
        self.state.lock().await.clear_connection();
    }

    pub fn new() -> Self {
        AACPManager {
            state: Arc::new(Mutex::new(AACPManagerState::new())),
            tasks: Arc::new(Mutex::new(JoinSet::new())),
        }
    }

    pub async fn connect(&mut self, addr: Address) {
        info!("AACPManager connecting to {} on PSM {:#06X}...", addr, PSM);
        let target_sa = SocketAddr::new(addr, AddressType::BrEdr, PSM);

        {
            let mut state = self.state.lock().await;
            state.airpods_mac = Some(addr);
        }

        let socket = match Socket::new_seq_packet() {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to create L2CAP socket: {}", e);
                return;
            }
        };

        let seq_packet =
            match tokio::time::timeout(CONNECT_TIMEOUT, socket.connect(target_sa)).await {
                Ok(Ok(s)) => Arc::new(s),
                Ok(Err(e)) => {
                    error!("L2CAP connect failed: {}", e);
                    return;
                }
                Err(_) => {
                    error!("L2CAP connect timed out");
                    return;
                }
            };

        // Wait for connection to be fully established
        let start = Instant::now();
        loop {
            match seq_packet.peer_addr() {
                Ok(peer) if peer.cid != 0 => break,
                Ok(_) => { /* still waiting */ }
                Err(e) => {
                    if e.raw_os_error() == Some(107) {
                        // ENOTCONN
                        error!("Peer has disconnected during connection setup.");
                        return;
                    }
                    error!("Error getting peer address: {}", e);
                }
            }
            if start.elapsed() >= CONNECT_TIMEOUT {
                error!("Timed out waiting for L2CAP connection to be fully established.");
                return;
            }
            sleep(POLL_INTERVAL).await;
        }

        info!("L2CAP connection established with {}", addr);

        let (tx, rx) = mpsc::channel(128);

        let manager_clone = self.clone();
        {
            let mut state = self.state.lock().await;
            state.sender = Some(tx);
        }

        let mut tasks = self.tasks.lock().await;
        tasks.spawn(recv_thread(manager_clone, seq_packet.clone()));
        tasks.spawn(send_thread(rx, seq_packet));
    }

    async fn send_packet(&self, data: &[u8]) -> Result<()> {
        // The sender is cloned and the lock released before awaiting. Holding
        // the state mutex across a full channel blocks every other task,
        // including the receive loop, and the manager stops responding
        // entirely. The timeout turns a stuck mutex into a logged error
        // instead of a hang with no diagnosis.
        let sender = match tokio::time::timeout(Duration::from_secs(2), self.state.lock()).await {
            Ok(state) => state.sender.clone(),
            Err(_) => {
                error!("send_packet: state mutex held for over 2s, giving up");
                return Err(Error::from(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "state mutex busy",
                )));
            }
        };
        if let Some(sender) = sender {
            sender.send(data.to_vec()).await.map_err(|e| {
                error!("Failed to send packet to channel: {}", e);
                Error::from(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "L2CAP send channel closed",
                ))
            })
        } else {
            error!("Cannot send packet, sender is not available.");
            Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "L2CAP stream not connected",
            )))
        }
    }

    async fn send_data_packet(&self, data: &[u8]) -> Result<()> {
        let packet = [HEADER_BYTES.as_slice(), data].concat();
        self.send_packet(&packet).await
    }

    pub async fn set_event_channel(&self, tx: mpsc::UnboundedSender<AACPEvent>) {
        let mut state = self.state.lock().await;
        state.event_tx = Some(tx);
    }

    pub async fn subscribe_to_control_command(
        &self,
        identifier: ControlCommandIdentifiers,
        tx: mpsc::UnboundedSender<Vec<u8>>,
    ) {
        let mut state = self.state.lock().await;
        state
            .control_command_subscribers
            .entry(identifier)
            .or_default()
            .push(tx);
        // send initial value if available
        if let Some(status) = state
            .control_command_status_list
            .iter()
            .find(|s| s.identifier == identifier)
        {
            let _ = state
                .control_command_subscribers
                .get(&identifier)
                .unwrap()
                .last()
                .unwrap()
                .send(status.value.clone());
        }
    }

    pub async fn receive_packet(&self, packet: &[u8]) {
        if !packet.starts_with(&HEADER_BYTES) {
            debug!(
                "Received packet does not start with expected header: {}",
                hex::encode(packet)
            );
            return;
        }
        if packet.len() < 5 {
            debug!("Received packet too short: {}", hex::encode(packet));
            return;
        }

        let opcode = packet[4];
        let payload = &packet[4..];

        match opcode {
            opcodes::BATTERY_INFO => {
                if payload.len() < 3 {
                    error!("Battery Info packet too short: {}", hex::encode(payload));
                    return;
                }
                let count = payload[2] as usize;
                if payload.len() < 3 + count * 5 {
                    error!(
                        "Battery Info packet length mismatch: {}",
                        hex::encode(payload)
                    );
                    return;
                }
                let mut batteries = Vec::with_capacity(count);
                for i in 0..count {
                    let base_index = 3 + i * 5;
                    batteries.push(BatteryInfo {
                        component: match payload[base_index] {
                            0x01 => BatteryComponent::Headphone,
                            0x02 => BatteryComponent::Right,
                            0x04 => BatteryComponent::Left,
                            0x08 => BatteryComponent::Case,
                            _ => {
                                error!("Unknown battery component: {:#04x}", payload[base_index]);
                                continue;
                            }
                        },
                        level: payload[base_index + 2],
                        status: match payload[base_index + 3] {
                            0x01 => BatteryStatus::Charging,
                            0x02 => BatteryStatus::NotCharging,
                            0x04 => BatteryStatus::Disconnected,
                            _ => {
                                error!("Unknown battery status: {:#04x}", payload[base_index + 3]);
                                continue;
                            }
                        },
                    });
                }
                let mut state = self.state.lock().await;
                state.battery_info = batteries.clone();
                if let Some(ref tx) = state.event_tx {
                    let _ = tx.send(AACPEvent::BatteryInfo(batteries));
                }
                info!("Received Battery Info: {:?}", state.battery_info);
            }
            opcodes::CONTROL_COMMAND => {
                let Some(parsed) = parse_control_command(payload) else {
                    error!("Control Command packet too short: {}", hex::encode(payload));
                    return;
                };

                match parsed {
                    ParsedControlCommand::Known {
                        status,
                        owns_connection,
                    } => {
                        let identifier = status.identifier;
                        let value = status.value.clone();
                        let mut state = self.state.lock().await;
                        if let Some(existing) = state
                            .control_command_status_list
                            .iter_mut()
                            .find(|s| s.identifier == identifier)
                        {
                            existing.value = value.clone();
                        } else {
                            state.control_command_status_list.push(status.clone());
                        }
                        if let Some(owns) = owns_connection {
                            state.owns = owns;
                        }
                        if let Some(subscribers) =
                            state.control_command_subscribers.get(&identifier)
                        {
                            for sub in subscribers {
                                let _ = sub.send(value.clone());
                            }
                        }
                        if let Some(ref tx) = state.event_tx {
                            let _ = tx.send(AACPEvent::ControlCommand(status));
                        }
                        info!(
                            "Received Control Command: {:?}, value: {}",
                            identifier,
                            hex::encode(&value)
                        );
                    }
                    ParsedControlCommand::Unknown {
                        identifier: identifier_byte,
                    } => {
                        error!(
                            "Unknown Control Command identifier: {:#04x}",
                            identifier_byte
                        );
                    }
                }
            }
            opcodes::EAR_DETECTION => {
                if packet.len() < 8 {
                    return;
                }
                let primary_status = packet[6];
                let secondary_status = packet[7];
                let mut statuses = Vec::new();
                statuses.push(match primary_status {
                    0x00 => EarDetectionStatus::InEar,
                    0x01 => EarDetectionStatus::OutOfEar,
                    0x02 => EarDetectionStatus::InCase,
                    0x03 => EarDetectionStatus::Disconnected,
                    _ => {
                        error!("Unknown ear detection status: {:#04x}", primary_status);
                        EarDetectionStatus::OutOfEar
                    }
                });
                statuses.push(match secondary_status {
                    0x00 => EarDetectionStatus::InEar,
                    0x01 => EarDetectionStatus::OutOfEar,
                    0x02 => EarDetectionStatus::InCase,
                    0x03 => EarDetectionStatus::Disconnected,
                    _ => {
                        error!("Unknown ear detection status: {:#04x}", secondary_status);
                        EarDetectionStatus::OutOfEar
                    }
                });
                let mut state = self.state.lock().await;
                state.old_ear_detection_status = state.ear_detection_status.clone();
                state.ear_detection_status = statuses.clone();

                if let Some(ref tx) = state.event_tx {
                    debug!(
                        "Sending Ear Detection event: old: {:?}, new: {:?}",
                        state.old_ear_detection_status, statuses
                    );
                    let _ = tx.send(AACPEvent::EarDetection(
                        state.old_ear_detection_status.clone(),
                        statuses,
                    ));
                }
                info!(
                    "Received Ear Detection Status: {:?}",
                    state.ear_detection_status
                );
            }
            opcodes::CONVERSATION_AWARENESS => {
                if packet.len() == 10 {
                    let status = packet[9];
                    let mut state = self.state.lock().await;
                    state.conversational_awareness_status = status;
                    if let Some(ref tx) = state.event_tx {
                        let _ = tx.send(AACPEvent::ConversationalAwareness(status));
                    }
                    info!("Received Conversation Awareness: {}", status);
                } else {
                    info!(
                        "Received Conversation Awareness packet with unexpected length: {}",
                        packet.len()
                    );
                }
            }
            opcodes::INFORMATION => {
                if payload.len() < 6 {
                    error!("Information packet too short: {}", hex::encode(payload));
                    return;
                }
                let data = &payload[4..];
                let mut index = 0;
                while index < data.len() && data[index] != 0x00 {
                    index += 1;
                }
                let mut strings = Vec::new();
                while index < data.len() {
                    while index < data.len() && data[index] == 0x00 {
                        index += 1;
                    }
                    if index >= data.len() {
                        break;
                    }
                    let start = index;
                    while index < data.len() && data[index] != 0x00 {
                        index += 1;
                    }
                    let str_bytes = &data[start..index];
                    if let Ok(s) = std::str::from_utf8(str_bytes) {
                        strings.push(s.to_string());
                    }
                }
                if strings.is_empty() {
                    return;
                }
                strings.remove(0);
                let info = AirPodsInformation {
                    name: strings.first().cloned().unwrap_or_default(),
                    model_number: strings.get(1).cloned().unwrap_or_default(),
                    manufacturer: strings.get(2).cloned().unwrap_or_default(),
                    serial_number: strings.get(3).cloned().unwrap_or_default(),
                    version1: strings.get(4).cloned().unwrap_or_default(),
                    version2: strings.get(5).cloned().unwrap_or_default(),
                    hardware_revision: strings.get(6).cloned().unwrap_or_default(),
                    updater_identifier: strings.get(7).cloned().unwrap_or_default(),
                    left_serial_number: strings.get(8).cloned().unwrap_or_default(),
                    right_serial_number: strings.get(9).cloned().unwrap_or_default(),
                    version3: strings.get(10).cloned().unwrap_or_default(),
                    le_keys: AirPodsLEKeys {
                        irk: "".to_string(),
                        enc_key: "".to_string(),
                    },
                };
                let mut state = self.state.lock().await;
                if let Some(mac) = state.airpods_mac
                    && let Some(device_data) = state.devices.get_mut(&mac.to_string())
                {
                    device_data.name = info.name.clone();
                    device_data.information = Some(DeviceInformation::AirPods(info.clone()));
                }
                let json = serde_json::to_string(&state.devices).unwrap();
                if let Some(parent) = get_devices_path().parent()
                    && let Err(e) = tokio::fs::create_dir_all(&parent).await
                {
                    error!("Failed to create directory for devices: {}", e);
                    return;
                }
                if let Err(e) = tokio::fs::write(&get_devices_path(), json).await {
                    error!("Failed to save devices: {}", e);
                }
                info!("Received Information: {:?}", info);
            }

            opcodes::PROXIMITY_KEYS_RSP => {
                if payload.len() < 4 {
                    error!(
                        "Proximity Keys Response packet too short: {}",
                        hex::encode(payload)
                    );
                    return;
                }
                let key_count = payload[2] as usize;
                debug!("Proximity Keys Response contains {} keys.", key_count);
                let mut offset = 3;
                let mut keys = Vec::new();
                for _ in 0..key_count {
                    if offset + 3 >= payload.len() {
                        error!(
                            "Proximity Keys Response packet too short while parsing keys: {}",
                            hex::encode(payload)
                        );
                        return;
                    }
                    let key_type = payload[offset];
                    let key_length = payload[offset + 2] as usize;
                    offset += 4;
                    if offset + key_length > payload.len() {
                        error!(
                            "Proximity Keys Response packet too short for key data: {}",
                            hex::encode(payload)
                        );
                        return;
                    }
                    let key_data = payload[offset..offset + key_length].to_vec();
                    keys.push((key_type, key_data));
                    offset += key_length;
                }
                info!(
                    "Received Proximity Keys Response: {:?}",
                    keys.iter()
                        .map(|(kt, kd)| (kt, hex::encode(kd)))
                        .collect::<Vec<_>>()
                );
                let mut state = self.state.lock().await;
                for (key_type, key_data) in &keys {
                    if let Some(kt) = ProximityKeyType::from_u8(*key_type)
                        && let Some(mac) = state.airpods_mac
                    {
                        let mac_str = mac.to_string();
                        let device_data =
                            state.devices.entry(mac_str.clone()).or_insert(DeviceData {
                                name: mac_str.clone(),
                                type_: DeviceType::AirPods,
                                information: None,
                            });
                        match kt {
                            ProximityKeyType::Irk => match device_data.information.as_mut() {
                                Some(DeviceInformation::AirPods(info)) => {
                                    info.le_keys.irk = hex::encode(key_data);
                                }
                                _ => {
                                    error!("Device information is not AirPods for adding LE IRK.");
                                }
                            },
                            ProximityKeyType::EncKey => match device_data.information.as_mut() {
                                Some(DeviceInformation::AirPods(info)) => {
                                    info.le_keys.enc_key = hex::encode(key_data);
                                }
                                _ => {
                                    error!(
                                        "Device information is not AirPods for adding LE encryption key."
                                    );
                                }
                            },
                        }
                    }
                }
                let json = serde_json::to_string(&state.devices).unwrap();
                if let Some(parent) = get_devices_path().parent()
                    && let Err(e) = tokio::fs::create_dir_all(&parent).await
                {
                    error!("Failed to create directory for devices: {}", e);
                    return;
                }
                if let Err(e) = tokio::fs::write(&get_devices_path(), json).await {
                    error!("Failed to save devices: {}", e);
                }
            }
            opcodes::STEM_PRESS => {
                if payload.len() < 4 {
                    error!("Stem Press packet too short: {}", hex::encode(payload));
                    return;
                }
                let press_type = StemPressType::from_u8(payload[2]);
                let bud_type = StemPressBudType::from_u8(payload[3]);
                if let (Some(press), Some(bud)) = (press_type, bud_type) {
                    info!("Received Stem Press: {:?} on {:?}", press, bud);
                    let state = self.state.lock().await;
                    if let Some(ref tx) = state.event_tx {
                        let _ = tx.send(AACPEvent::StemPress(press, bud));
                    }
                } else {
                    error!(
                        "Invalid Stem Press packet - type: {:?}, bud: {:?}",
                        press_type, bud_type
                    );
                }
            }
            opcodes::AUDIO_SOURCE => {
                let Some(audio_source) = parse_audio_source(payload) else {
                    error!("Audio Source packet too short: {}", hex::encode(payload));
                    return;
                };
                let mut state = self.state.lock().await;
                state.audio_source = Some(audio_source.clone());
                if let Some(ref tx) = state.event_tx {
                    let _ = tx.send(AACPEvent::AudioSource(audio_source));
                }
                info!("Received Audio Source: {:?}", state.audio_source);
            }
            opcodes::CONNECTED_DEVICES => {
                let Some(devices) = parse_connected_devices(payload) else {
                    if payload.len() < 5 {
                        error!(
                            "Connected Devices packet too short: {}",
                            hex::encode(payload)
                        );
                    } else {
                        error!(
                            "Connected Devices packet length mismatch: {}",
                            hex::encode(payload)
                        );
                    }
                    return;
                };
                let mut state = self.state.lock().await;
                state.old_connected_devices = state.connected_devices.clone();
                state.connected_devices = devices.clone();
                if let Some(ref tx) = state.event_tx {
                    let _ = tx.send(AACPEvent::ConnectedDevices(
                        state.old_connected_devices.clone(),
                        devices,
                    ));
                }
                info!("Received Connected Devices: {:?}", state.connected_devices);
            }
            opcodes::SMART_ROUTING_RESP => {
                let Some(response) = parse_smart_routing_response(payload) else {
                    return;
                };
                info!("Received Smart Routing Response: {}", response.text);
                if response.set_ownership_to_false {
                    info!("Received OwnershipToFalse request");
                    if let Some(ref tx) = self.state.lock().await.event_tx {
                        let _ = tx.send(AACPEvent::OwnershipToFalseRequest);
                    }
                }
            }
            opcodes::EQ_DATA => {
                debug!("Received EQ Data");
            }
            _ => debug!("Received unknown packet with opcode {:#04x}", opcode),
        }
    }

    pub async fn send_notification_request(&self) -> Result<()> {
        let opcode = [opcodes::REQUEST_NOTIFICATIONS, 0x00];
        let data = [0xFF, 0xFF, 0xFF, 0xFF];
        let packet = [opcode.as_slice(), data.as_slice()].concat();
        self.send_data_packet(&packet).await
    }

    pub async fn send_set_feature_flags_packet(&self) -> Result<()> {
        let opcode = [opcodes::SET_FEATURE_FLAGS, 0x00];
        // let data = [0xD7, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let data = [0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]; // adaptive volume is actually useful, seeing if it works
        let packet = [opcode.as_slice(), data.as_slice()].concat();
        self.send_data_packet(&packet).await
    }

    pub async fn send_handshake(&self) -> Result<()> {
        let packet = [
            0x00, 0x00, 0x04, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        ];
        self.send_packet(&packet).await
    }

    pub async fn send_proximity_keys_request(
        &self,
        key_types: Vec<ProximityKeyType>,
    ) -> Result<()> {
        let opcode = [opcodes::PROXIMITY_KEYS_REQ, 0x00];
        let mut data = Vec::with_capacity(2);
        data.push(key_types.iter().fold(0u8, |acc, kt| acc | (*kt as u8)));
        data.push(0x00);
        let packet = [opcode.as_slice(), data.as_slice()].concat();
        self.send_data_packet(&packet).await
    }

    pub async fn send_rename_packet(&self, name: &str) -> Result<()> {
        let name_bytes = name.as_bytes();
        let size = name_bytes.len();
        let mut packet = Vec::with_capacity(6 + size);
        packet.push(opcodes::RENAME);
        packet.push(0x00);
        packet.push(0x01);
        packet.push(size as u8);
        packet.push(0x00);
        packet.extend_from_slice(name_bytes);
        self.send_data_packet(&packet).await
    }

    pub async fn send_control_command(
        &self,
        identifier: ControlCommandIdentifiers,
        value: &[u8],
    ) -> Result<()> {
        self.send_data_packet(&encode_control_command(identifier, value))
            .await
    }

    pub async fn send_media_information_new_device(
        &self,
        self_mac_address: &str,
        target_mac_address: &str,
    ) -> Result<()> {
        self.send_data_packet(&encode_media_information_new_device(
            self_mac_address,
            target_mac_address,
        ))
        .await
    }

    pub async fn send_hijack_request(&self, target_mac_address: &str) -> Result<()> {
        self.send_data_packet(&encode_hijack_request(target_mac_address))
            .await
    }

    pub async fn send_media_information(
        &self,
        self_mac_address: &str,
        target_mac_address: &str,
        streaming_state: bool,
    ) -> Result<()> {
        self.send_data_packet(&encode_media_information(
            self_mac_address,
            target_mac_address,
            streaming_state,
        ))
        .await
    }

    pub async fn send_smart_routing_show_ui(&self, target_mac_address: &str) -> Result<()> {
        self.send_data_packet(&encode_smart_routing_show_ui(target_mac_address))
            .await
    }

    pub async fn send_hijack_reversed(&self, target_mac_address: &str) -> Result<()> {
        self.send_data_packet(&encode_hijack_reversed(target_mac_address))
            .await
    }

    pub async fn send_add_tipi_device(
        &self,
        self_mac_address: &str,
        target_mac_address: &str,
    ) -> Result<()> {
        self.send_data_packet(&encode_add_tipi_device(
            self_mac_address,
            target_mac_address,
        ))
        .await
    }

    pub async fn send_some_packet(&self) -> Result<()> {
        self.send_data_packet(&[0x29, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF])
            .await
    }
}

// Pure AACP packet encoders and parsers. They take no locks and do no I/O so
// the protocol can be tested offline. Encoders return the data packet without
// the 4-byte AACP header (send_data_packet adds it). Parsers take the payload
// the way receive_packet slices it: `&packet[4..]`, opcode at payload[0].

// Target MAC components in reverse order, as every smart-routing packet sends
// them. A component that is not valid hex panics, as it did before these
// encoders were extracted; changing that is out of scope for this refactor.
fn reversed_mac_bytes(mac_address: &str) -> Vec<u8> {
    let mac_bytes: Vec<u8> = mac_address
        .split(':')
        .map(|s| u8::from_str_radix(s, 16).unwrap())
        .collect();
    mac_bytes.iter().rev().cloned().collect()
}

pub(crate) fn encode_control_command(
    identifier: ControlCommandIdentifiers,
    value: &[u8],
) -> Vec<u8> {
    let opcode = [opcodes::CONTROL_COMMAND, 0x00];
    let mut data = vec![identifier as u8];
    for i in 0..4 {
        data.push(value.get(i).copied().unwrap_or(0));
    }
    [opcode.as_slice(), data.as_slice()].concat()
}

pub(crate) fn encode_media_information_new_device(
    self_mac_address: &str,
    target_mac_address: &str,
) -> Vec<u8> {
    let opcode = [opcodes::SMART_ROUTING, 0x00];
    let mut buffer = Vec::with_capacity(112);
    buffer.extend_from_slice(&reversed_mac_bytes(target_mac_address));

    buffer.extend_from_slice(&[0x68, 0x00]);
    buffer.extend_from_slice(&[0x01, 0xE5, 0x4A]);
    buffer.extend_from_slice(b"playingApp");
    buffer.push(0x42);
    buffer.extend_from_slice(b"NA");
    buffer.push(0x52);
    buffer.extend_from_slice(b"hostStreamingState");
    buffer.push(0x42);
    buffer.extend_from_slice(b"NO");
    buffer.push(0x49);
    buffer.extend_from_slice(b"btAddress");
    buffer.push(0x51);
    buffer.extend_from_slice(self_mac_address.as_bytes());
    buffer.push(0x46);
    buffer.extend_from_slice(b"btName");
    buffer.push(0x43);
    buffer.extend_from_slice(b"Mac");
    buffer.push(0x58);
    buffer.extend_from_slice(b"otherDevice");
    buffer.extend_from_slice(b"AudioCategory");
    buffer.extend_from_slice(&[0x30, 0x64]);

    [opcode.as_slice(), buffer.as_slice()].concat()
}

pub(crate) fn encode_hijack_request(target_mac_address: &str) -> Vec<u8> {
    let opcode = [opcodes::SMART_ROUTING, 0x00];
    let mut buffer = Vec::with_capacity(106);
    buffer.extend_from_slice(&reversed_mac_bytes(target_mac_address));
    buffer.extend_from_slice(&[0x62, 0x00]);
    buffer.extend_from_slice(&[0x01, 0xE5]);
    buffer.push(0x4A);
    buffer.extend_from_slice(b"localscore");
    buffer.extend_from_slice(&[0x30, 0x64]);
    buffer.push(0x46);
    buffer.extend_from_slice(b"reason");
    buffer.push(0x48);
    buffer.extend_from_slice(b"Hijackv2");
    buffer.push(0x51);
    buffer.extend_from_slice(b"audioRoutingScore");
    buffer.extend_from_slice(&[0x31, 0x2D, 0x01, 0x5F]);
    buffer.extend_from_slice(b"audioRoutingSetOwnershipToFalse");
    buffer.push(0x01);
    buffer.push(0x4B);
    buffer.extend_from_slice(b"remotescore");
    buffer.push(0xA5);

    while buffer.len() < 106 {
        buffer.push(0x00);
    }

    [opcode.as_slice(), buffer.as_slice()].concat()
}

pub(crate) fn encode_media_information(
    self_mac_address: &str,
    target_mac_address: &str,
    streaming_state: bool,
) -> Vec<u8> {
    let opcode = [opcodes::SMART_ROUTING, 0x00];
    let mut buffer = Vec::with_capacity(138);
    buffer.extend_from_slice(&reversed_mac_bytes(target_mac_address));
    buffer.extend_from_slice(&[0x82, 0x00]);
    buffer.extend_from_slice(&[0x01, 0xE5, 0x4A]);
    buffer.extend_from_slice(b"PlayingApp");
    buffer.push(0x56);
    buffer.extend_from_slice(b"com.google.ios.youtube");
    buffer.push(0x52);
    buffer.extend_from_slice(b"HostStreamingState");
    buffer.push(0x42);
    buffer.extend_from_slice(if streaming_state { b"YES" } else { b"NO" });
    buffer.push(0x49);
    buffer.extend_from_slice(b"btAddress");
    buffer.push(0x51);
    buffer.extend_from_slice(self_mac_address.as_bytes());
    buffer.extend_from_slice(b"btName");
    buffer.push(0x43);
    buffer.extend_from_slice(b"Mac");
    buffer.push(0x58);
    buffer.extend_from_slice(b"otherDevice");
    buffer.extend_from_slice(b"AudioCategory");
    buffer.extend_from_slice(&[0x31, 0x2D, 0x01]);

    while buffer.len() < 138 {
        buffer.push(0x00);
    }
    [opcode.as_slice(), buffer.as_slice()].concat()
}

pub(crate) fn encode_smart_routing_show_ui(target_mac_address: &str) -> Vec<u8> {
    let opcode = [opcodes::SMART_ROUTING, 0x00];
    let mut buffer = Vec::with_capacity(134);
    buffer.extend_from_slice(&reversed_mac_bytes(target_mac_address));
    buffer.extend_from_slice(&[0x7E, 0x00]);
    buffer.extend_from_slice(&[0x01, 0xE6, 0x5B]);
    buffer.extend_from_slice(b"SmartRoutingKeyShowNearbyUI");
    buffer.push(0x01);
    buffer.push(0x4A);
    buffer.extend_from_slice(b"localscore");
    buffer.extend_from_slice(&[0x31, 0x2D]);
    buffer.push(0x01);
    buffer.push(0x46);
    buffer.extend_from_slice(b"reasonHhijackv2");
    buffer.push(0x51);
    buffer.extend_from_slice(b"audioRoutingScore");
    buffer.push(0xA2);
    buffer.push(0x5F);
    buffer.extend_from_slice(b"audioRoutingSetOwnershipToFalse");
    buffer.push(0x01);
    buffer.push(0x4B);
    buffer.extend_from_slice(b"remotescore");
    buffer.push(0xA2);

    while buffer.len() < 134 {
        buffer.push(0x00);
    }

    [opcode.as_slice(), buffer.as_slice()].concat()
}

pub(crate) fn encode_hijack_reversed(target_mac_address: &str) -> Vec<u8> {
    let opcode = [opcodes::SMART_ROUTING, 0x00];
    let mut buffer = Vec::with_capacity(97);
    buffer.extend_from_slice(&reversed_mac_bytes(target_mac_address));
    buffer.extend_from_slice(&[0x59, 0x00]);
    buffer.extend_from_slice(&[0x01, 0xE3]);
    buffer.push(0x5F);
    buffer.extend_from_slice(b"audioRoutingSetOwnershipToFalse");
    buffer.push(0x01);
    buffer.push(0x59);
    buffer.extend_from_slice(b"audioRoutingShowReverseUI");
    buffer.push(0x01);
    buffer.push(0x46);
    buffer.extend_from_slice(b"reason");
    buffer.push(0x53);
    buffer.extend_from_slice(b"ReverseBannerTapped");

    while buffer.len() < 97 {
        buffer.push(0x00);
    }

    [opcode.as_slice(), buffer.as_slice()].concat()
}

pub(crate) fn encode_add_tipi_device(self_mac_address: &str, target_mac_address: &str) -> Vec<u8> {
    let opcode = [opcodes::SMART_ROUTING, 0x00];
    let mut buffer = Vec::with_capacity(86);
    buffer.extend_from_slice(&reversed_mac_bytes(target_mac_address));
    buffer.extend_from_slice(&[0x4E, 0x00]);
    buffer.extend_from_slice(&[0x01, 0xE5]);
    buffer.extend_from_slice(&[0x48, 0x69]);
    buffer.extend_from_slice(b"idleTime");
    buffer.extend_from_slice(&[0x08, 0x47]);
    buffer.extend_from_slice(b"newTipi");
    buffer.extend_from_slice(&[0x01, 0x49]);
    buffer.extend_from_slice(b"btAddress");
    buffer.push(0x51);
    buffer.extend_from_slice(self_mac_address.as_bytes());
    buffer.push(0x46);
    buffer.extend_from_slice(b"btName");
    buffer.push(0x43);
    buffer.extend_from_slice(b"Mac");
    buffer.push(0x50);
    buffer.extend_from_slice(b"nearbyAudioScore");
    buffer.push(0x0E);

    [opcode.as_slice(), buffer.as_slice()].concat()
}

// AUDIO_SOURCE (0x0E): source MAC in payload[2..8], least significant byte
// first; source type in payload[8]. None if payload.len() < 9.
pub(crate) fn parse_audio_source(payload: &[u8]) -> Option<AudioSource> {
    if payload.len() < 9 {
        return None;
    }
    let mac = format!(
        "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
        payload[7], payload[6], payload[5], payload[4], payload[3], payload[2]
    );
    let typ = AudioSourceType::from_u8(payload[8]).unwrap_or(AudioSourceType::Unknown);
    Some(AudioSource { mac, r#type: typ })
}

// CONNECTED_DEVICES (0x2E): device count in payload[4], then 8 bytes per
// device from payload[5] (6-byte MAC in order, info1, info2). None if the
// payload is shorter than the count requires.
pub(crate) fn parse_connected_devices(payload: &[u8]) -> Option<Vec<ConnectedDevice>> {
    if payload.len() < 5 {
        return None;
    }
    let count = payload[4] as usize;
    if payload.len() < 5 + count * 8 {
        return None;
    }
    let mut devices = Vec::with_capacity(count);
    for i in 0..count {
        let base = 5 + i * 8;
        let mac = format!(
            "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
            payload[base],
            payload[base + 1],
            payload[base + 2],
            payload[base + 3],
            payload[base + 4],
            payload[base + 5]
        );
        let info1 = payload[base + 6];
        let info2 = payload[base + 7];
        devices.push(ConnectedDevice {
            mac,
            info1,
            info2,
            r#type: None,
        });
    }
    Some(devices)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedControlCommand {
    Known {
        status: ControlCommandStatus,
        // Some(payload[3] != 0) for OwnsConnection, None for other identifiers.
        owns_connection: Option<bool>,
    },
    Unknown {
        identifier: u8,
    },
}

// CONTROL_COMMAND (0x09): identifier in payload[2], value in payload[3..7]
// with trailing zero bytes trimmed (all zero becomes [0]). None if
// payload.len() < 7.
pub(crate) fn parse_control_command(payload: &[u8]) -> Option<ParsedControlCommand> {
    if payload.len() < 7 {
        return None;
    }
    let identifier_byte = payload[2];
    let value_bytes = &payload[3..7];

    let last_non_zero = value_bytes.iter().rposition(|&b| b != 0);
    let value = match last_non_zero {
        Some(i) => value_bytes[..=i].to_vec(),
        None => vec![0],
    };

    match ControlCommandIdentifiers::from_u8(identifier_byte) {
        Some(identifier) => Some(ParsedControlCommand::Known {
            status: ControlCommandStatus { identifier, value },
            owns_connection: (identifier == ControlCommandIdentifiers::OwnsConnection)
                .then(|| value_bytes[0] != 0),
        }),
        None => Some(ParsedControlCommand::Unknown {
            identifier: identifier_byte,
        }),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SmartRoutingResponse {
    // Lossy UTF-8 of payload[2..].
    pub text: String,
    pub set_ownership_to_false: bool,
}

// SMART_ROUTING_RESP (0x11): the text is the lossy UTF-8 of payload[2..].
// None if payload.len() < 2.
pub(crate) fn parse_smart_routing_response(payload: &[u8]) -> Option<SmartRoutingResponse> {
    if payload.len() < 2 {
        return None;
    }
    let text = String::from_utf8_lossy(&payload[2..]).into_owned();
    let set_ownership_to_false = text.contains("SetOwnershipToFalse");
    Some(SmartRoutingResponse {
        text,
        set_ownership_to_false,
    })
}

async fn recv_thread(manager: AACPManager, sp: Arc<SeqPacket>) {
    let mut buf = vec![0u8; 1024];
    loop {
        match sp.recv(&mut buf).await {
            Ok(0) => {
                info!("Remote closed the connection.");
                break;
            }
            Ok(n) => {
                let data = &buf[..n];
                debug!("Received {} bytes: {}", n, hex::encode(data));
                manager.receive_packet(data).await;
            }
            Err(e) => {
                debug!("Read error: {}", e);
                info!(
                    "We have probably disconnected, clearing state variables (owns=false, connected_devices=empty, control_command_status_list=empty)."
                );
                break;
            }
        }
    }
    manager.state.lock().await.clear_connection();
}

async fn send_thread(mut rx: mpsc::Receiver<Vec<u8>>, sp: Arc<SeqPacket>) {
    while let Some(data) = rx.recv().await {
        let mut attempts = 0;
        loop {
          match sp.send(&data).await {
            Ok(_) => {
              debug!("Sent {} bytes: {}", data.len(), hex::encode(&data));
              break;
            }
            Err(e) if e.raw_os_error() == Some(107) && attempts < 10 => {
              attempts += 1;
              sleep(Duration::from_millis(100)).await;
            }
            Err(e) => {
              error!("Failed to send data: {}", e);
              return;
            }
          }
        }
    }
    info!("Send thread finished.");
}

#[cfg(test)]
mod tests {
    use super::*;

    // A full send channel must not keep the state mutex locked: other tasks
    // (receive loop, tray, control commands) need the state while the send
    // waits for capacity.
    #[tokio::test]
    async fn send_packet_full_channel_does_not_hold_state_lock() {
        let manager = AACPManager::new();
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1);
        tx.try_send(vec![0xAA])
            .expect("filling the single slot should succeed");
        manager.state.lock().await.sender = Some(tx);

        let sender_manager = manager.clone();
        let handle =
            tokio::spawn(async move { sender_manager.send_packet(&[0x01, 0x02, 0x03]).await });

        sleep(Duration::from_millis(100)).await;
        assert!(
            !handle.is_finished(),
            "send_packet should still be waiting for channel capacity"
        );

        {
            let guard = tokio::time::timeout(Duration::from_millis(500), manager.state.lock())
                .await
                .expect("state lock must be free while send_packet waits on a full channel");
            assert!(guard.sender.is_some());
        }

        assert_eq!(rx.recv().await, Some(vec![0xAA]));
        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("send_packet should complete once the channel has capacity")
            .expect("send_packet task should not panic");
        assert!(result.is_ok(), "send_packet failed: {:?}", result.err());
        assert_eq!(rx.recv().await, Some(vec![0x01, 0x02, 0x03]));
    }

    // A state mutex held elsewhere must make send_packet fail with TimedOut
    // after ~2s instead of hanging forever.
    #[tokio::test]
    async fn send_packet_lock_timeout_returns_err() {
        let manager = AACPManager::new();
        let (tx, _rx) = mpsc::channel::<Vec<u8>>(1);
        let guard = {
            let mut state = manager.state.lock().await;
            state.sender = Some(tx);
            state
        };

        let result = tokio::time::timeout(Duration::from_secs(5), manager.send_packet(&[0x01]))
            .await
            .expect("send_packet must not hang while the state mutex is held");
        drop(guard);

        let err = result.expect_err("send_packet should fail when the state lock is busy");
        assert!(
            matches!(
                err.kind,
                bluer::ErrorKind::Internal(bluer::InternalErrorKind::Io(
                    std::io::ErrorKind::TimedOut
                ))
            ),
            "unexpected error kind: {:?}",
            err.kind
        );
    }

    // Golden packets: the exact bytes the send_* methods put on the send
    // channel (header included), captured from the implementation before the
    // encoders were extracted. They pin current behaviour, quirks included
    // (no 0x46 before "btName" in media information, "reasonHhijackv2" in the
    // show-UI packet, the 0x4E length in add-tipi, the padding lengths). Do not
    // "fix" these bytes in a refactor; protocol changes need their own ticket.
    const GOLDEN_TARGET_MAC: &str = "AA:BB:CC:DD:EE:FF";
    const GOLDEN_SELF_MAC: &str = "11:22:33:44:55:66";

    const GOLDEN_OWNS_CONNECTION_0: &[u8] = &[
        0x04, 0x00, 0x04, 0x00, 0x09, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00,
    ];
    const GOLDEN_OWNS_CONNECTION_1: &[u8] = &[
        0x04, 0x00, 0x04, 0x00, 0x09, 0x00, 0x06, 0x01, 0x00, 0x00, 0x00,
    ];
    const GOLDEN_HIJACK_REQUEST: &[u8] = &[
        0x04, 0x00, 0x04, 0x00, 0x10, 0x00, 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x62, 0x00, 0x01,
        0xE5, 0x4A, 0x6C, 0x6F, 0x63, 0x61, 0x6C, 0x73, 0x63, 0x6F, 0x72, 0x65, 0x30, 0x64, 0x46,
        0x72, 0x65, 0x61, 0x73, 0x6F, 0x6E, 0x48, 0x48, 0x69, 0x6A, 0x61, 0x63, 0x6B, 0x76, 0x32,
        0x51, 0x61, 0x75, 0x64, 0x69, 0x6F, 0x52, 0x6F, 0x75, 0x74, 0x69, 0x6E, 0x67, 0x53, 0x63,
        0x6F, 0x72, 0x65, 0x31, 0x2D, 0x01, 0x5F, 0x61, 0x75, 0x64, 0x69, 0x6F, 0x52, 0x6F, 0x75,
        0x74, 0x69, 0x6E, 0x67, 0x53, 0x65, 0x74, 0x4F, 0x77, 0x6E, 0x65, 0x72, 0x73, 0x68, 0x69,
        0x70, 0x54, 0x6F, 0x46, 0x61, 0x6C, 0x73, 0x65, 0x01, 0x4B, 0x72, 0x65, 0x6D, 0x6F, 0x74,
        0x65, 0x73, 0x63, 0x6F, 0x72, 0x65, 0xA5,
    ];
    const GOLDEN_SMART_ROUTING_SHOW_UI: &[u8] = &[
        0x04, 0x00, 0x04, 0x00, 0x10, 0x00, 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x7E, 0x00, 0x01,
        0xE6, 0x5B, 0x53, 0x6D, 0x61, 0x72, 0x74, 0x52, 0x6F, 0x75, 0x74, 0x69, 0x6E, 0x67, 0x4B,
        0x65, 0x79, 0x53, 0x68, 0x6F, 0x77, 0x4E, 0x65, 0x61, 0x72, 0x62, 0x79, 0x55, 0x49, 0x01,
        0x4A, 0x6C, 0x6F, 0x63, 0x61, 0x6C, 0x73, 0x63, 0x6F, 0x72, 0x65, 0x31, 0x2D, 0x01, 0x46,
        0x72, 0x65, 0x61, 0x73, 0x6F, 0x6E, 0x48, 0x68, 0x69, 0x6A, 0x61, 0x63, 0x6B, 0x76, 0x32,
        0x51, 0x61, 0x75, 0x64, 0x69, 0x6F, 0x52, 0x6F, 0x75, 0x74, 0x69, 0x6E, 0x67, 0x53, 0x63,
        0x6F, 0x72, 0x65, 0xA2, 0x5F, 0x61, 0x75, 0x64, 0x69, 0x6F, 0x52, 0x6F, 0x75, 0x74, 0x69,
        0x6E, 0x67, 0x53, 0x65, 0x74, 0x4F, 0x77, 0x6E, 0x65, 0x72, 0x73, 0x68, 0x69, 0x70, 0x54,
        0x6F, 0x46, 0x61, 0x6C, 0x73, 0x65, 0x01, 0x4B, 0x72, 0x65, 0x6D, 0x6F, 0x74, 0x65, 0x73,
        0x63, 0x6F, 0x72, 0x65, 0xA2,
    ];
    const GOLDEN_MEDIA_INFORMATION_STREAMING: &[u8] = &[
        0x04, 0x00, 0x04, 0x00, 0x10, 0x00, 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x82, 0x00, 0x01,
        0xE5, 0x4A, 0x50, 0x6C, 0x61, 0x79, 0x69, 0x6E, 0x67, 0x41, 0x70, 0x70, 0x56, 0x63, 0x6F,
        0x6D, 0x2E, 0x67, 0x6F, 0x6F, 0x67, 0x6C, 0x65, 0x2E, 0x69, 0x6F, 0x73, 0x2E, 0x79, 0x6F,
        0x75, 0x74, 0x75, 0x62, 0x65, 0x52, 0x48, 0x6F, 0x73, 0x74, 0x53, 0x74, 0x72, 0x65, 0x61,
        0x6D, 0x69, 0x6E, 0x67, 0x53, 0x74, 0x61, 0x74, 0x65, 0x42, 0x59, 0x45, 0x53, 0x49, 0x62,
        0x74, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x51, 0x31, 0x31, 0x3A, 0x32, 0x32, 0x3A,
        0x33, 0x33, 0x3A, 0x34, 0x34, 0x3A, 0x35, 0x35, 0x3A, 0x36, 0x36, 0x62, 0x74, 0x4E, 0x61,
        0x6D, 0x65, 0x43, 0x4D, 0x61, 0x63, 0x58, 0x6F, 0x74, 0x68, 0x65, 0x72, 0x44, 0x65, 0x76,
        0x69, 0x63, 0x65, 0x41, 0x75, 0x64, 0x69, 0x6F, 0x43, 0x61, 0x74, 0x65, 0x67, 0x6F, 0x72,
        0x79, 0x31, 0x2D, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    const GOLDEN_MEDIA_INFORMATION_NOT_STREAMING: &[u8] = &[
        0x04, 0x00, 0x04, 0x00, 0x10, 0x00, 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x82, 0x00, 0x01,
        0xE5, 0x4A, 0x50, 0x6C, 0x61, 0x79, 0x69, 0x6E, 0x67, 0x41, 0x70, 0x70, 0x56, 0x63, 0x6F,
        0x6D, 0x2E, 0x67, 0x6F, 0x6F, 0x67, 0x6C, 0x65, 0x2E, 0x69, 0x6F, 0x73, 0x2E, 0x79, 0x6F,
        0x75, 0x74, 0x75, 0x62, 0x65, 0x52, 0x48, 0x6F, 0x73, 0x74, 0x53, 0x74, 0x72, 0x65, 0x61,
        0x6D, 0x69, 0x6E, 0x67, 0x53, 0x74, 0x61, 0x74, 0x65, 0x42, 0x4E, 0x4F, 0x49, 0x62, 0x74,
        0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x51, 0x31, 0x31, 0x3A, 0x32, 0x32, 0x3A, 0x33,
        0x33, 0x3A, 0x34, 0x34, 0x3A, 0x35, 0x35, 0x3A, 0x36, 0x36, 0x62, 0x74, 0x4E, 0x61, 0x6D,
        0x65, 0x43, 0x4D, 0x61, 0x63, 0x58, 0x6F, 0x74, 0x68, 0x65, 0x72, 0x44, 0x65, 0x76, 0x69,
        0x63, 0x65, 0x41, 0x75, 0x64, 0x69, 0x6F, 0x43, 0x61, 0x74, 0x65, 0x67, 0x6F, 0x72, 0x79,
        0x31, 0x2D, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    const GOLDEN_MEDIA_INFORMATION_NEW_DEVICE: &[u8] = &[
        0x04, 0x00, 0x04, 0x00, 0x10, 0x00, 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x68, 0x00, 0x01,
        0xE5, 0x4A, 0x70, 0x6C, 0x61, 0x79, 0x69, 0x6E, 0x67, 0x41, 0x70, 0x70, 0x42, 0x4E, 0x41,
        0x52, 0x68, 0x6F, 0x73, 0x74, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x69, 0x6E, 0x67, 0x53,
        0x74, 0x61, 0x74, 0x65, 0x42, 0x4E, 0x4F, 0x49, 0x62, 0x74, 0x41, 0x64, 0x64, 0x72, 0x65,
        0x73, 0x73, 0x51, 0x31, 0x31, 0x3A, 0x32, 0x32, 0x3A, 0x33, 0x33, 0x3A, 0x34, 0x34, 0x3A,
        0x35, 0x35, 0x3A, 0x36, 0x36, 0x46, 0x62, 0x74, 0x4E, 0x61, 0x6D, 0x65, 0x43, 0x4D, 0x61,
        0x63, 0x58, 0x6F, 0x74, 0x68, 0x65, 0x72, 0x44, 0x65, 0x76, 0x69, 0x63, 0x65, 0x41, 0x75,
        0x64, 0x69, 0x6F, 0x43, 0x61, 0x74, 0x65, 0x67, 0x6F, 0x72, 0x79, 0x30, 0x64,
    ];
    const GOLDEN_ADD_TIPI_DEVICE: &[u8] = &[
        0x04, 0x00, 0x04, 0x00, 0x10, 0x00, 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x4E, 0x00, 0x01,
        0xE5, 0x48, 0x69, 0x69, 0x64, 0x6C, 0x65, 0x54, 0x69, 0x6D, 0x65, 0x08, 0x47, 0x6E, 0x65,
        0x77, 0x54, 0x69, 0x70, 0x69, 0x01, 0x49, 0x62, 0x74, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73,
        0x73, 0x51, 0x31, 0x31, 0x3A, 0x32, 0x32, 0x3A, 0x33, 0x33, 0x3A, 0x34, 0x34, 0x3A, 0x35,
        0x35, 0x3A, 0x36, 0x36, 0x46, 0x62, 0x74, 0x4E, 0x61, 0x6D, 0x65, 0x43, 0x4D, 0x61, 0x63,
        0x50, 0x6E, 0x65, 0x61, 0x72, 0x62, 0x79, 0x41, 0x75, 0x64, 0x69, 0x6F, 0x53, 0x63, 0x6F,
        0x72, 0x65, 0x0E,
    ];
    const GOLDEN_HIJACK_REVERSED: &[u8] = &[
        0x04, 0x00, 0x04, 0x00, 0x10, 0x00, 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x59, 0x00, 0x01,
        0xE3, 0x5F, 0x61, 0x75, 0x64, 0x69, 0x6F, 0x52, 0x6F, 0x75, 0x74, 0x69, 0x6E, 0x67, 0x53,
        0x65, 0x74, 0x4F, 0x77, 0x6E, 0x65, 0x72, 0x73, 0x68, 0x69, 0x70, 0x54, 0x6F, 0x46, 0x61,
        0x6C, 0x73, 0x65, 0x01, 0x59, 0x61, 0x75, 0x64, 0x69, 0x6F, 0x52, 0x6F, 0x75, 0x74, 0x69,
        0x6E, 0x67, 0x53, 0x68, 0x6F, 0x77, 0x52, 0x65, 0x76, 0x65, 0x72, 0x73, 0x65, 0x55, 0x49,
        0x01, 0x46, 0x72, 0x65, 0x61, 0x73, 0x6F, 0x6E, 0x53, 0x52, 0x65, 0x76, 0x65, 0x72, 0x73,
        0x65, 0x42, 0x61, 0x6E, 0x6E, 0x65, 0x72, 0x54, 0x61, 0x70, 0x70, 0x65, 0x64,
    ];

    // Offline manager with an injected bounded send channel (#2/#24 pattern).
    async fn manager_with_send_channel() -> (AACPManager, mpsc::Receiver<Vec<u8>>) {
        let manager = AACPManager::new();
        let (tx, rx) = mpsc::channel::<Vec<u8>>(8);
        manager.state.lock().await.sender = Some(tx);
        (manager, rx)
    }

    // Takes the single packet a send_* call put on the channel.
    fn sent_packet(rx: &mut mpsc::Receiver<Vec<u8>>) -> Vec<u8> {
        let packet = rx
            .try_recv()
            .expect("send_* should queue exactly one packet");
        assert!(rx.try_recv().is_err(), "send_* queued more than one packet");
        packet
    }

    #[tokio::test]
    async fn golden_send_control_command_owns_connection_0() {
        let (manager, mut rx) = manager_with_send_channel().await;
        manager
            .send_control_command(ControlCommandIdentifiers::OwnsConnection, &[0])
            .await
            .expect("send_control_command");
        assert_eq!(sent_packet(&mut rx), GOLDEN_OWNS_CONNECTION_0);
    }

    #[tokio::test]
    async fn golden_send_control_command_owns_connection_1() {
        let (manager, mut rx) = manager_with_send_channel().await;
        manager
            .send_control_command(ControlCommandIdentifiers::OwnsConnection, &[1])
            .await
            .expect("send_control_command");
        assert_eq!(sent_packet(&mut rx), GOLDEN_OWNS_CONNECTION_1);
    }

    #[tokio::test]
    async fn golden_send_hijack_request() {
        let (manager, mut rx) = manager_with_send_channel().await;
        manager
            .send_hijack_request(GOLDEN_TARGET_MAC)
            .await
            .expect("send_hijack_request");
        assert_eq!(sent_packet(&mut rx), GOLDEN_HIJACK_REQUEST);
    }

    #[tokio::test]
    async fn golden_send_smart_routing_show_ui() {
        let (manager, mut rx) = manager_with_send_channel().await;
        manager
            .send_smart_routing_show_ui(GOLDEN_TARGET_MAC)
            .await
            .expect("send_smart_routing_show_ui");
        assert_eq!(sent_packet(&mut rx), GOLDEN_SMART_ROUTING_SHOW_UI);
    }

    #[tokio::test]
    async fn golden_send_media_information_streaming() {
        let (manager, mut rx) = manager_with_send_channel().await;
        manager
            .send_media_information(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC, true)
            .await
            .expect("send_media_information");
        assert_eq!(sent_packet(&mut rx), GOLDEN_MEDIA_INFORMATION_STREAMING);
    }

    #[tokio::test]
    async fn golden_send_media_information_not_streaming() {
        let (manager, mut rx) = manager_with_send_channel().await;
        manager
            .send_media_information(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC, false)
            .await
            .expect("send_media_information");
        assert_eq!(sent_packet(&mut rx), GOLDEN_MEDIA_INFORMATION_NOT_STREAMING);
    }

    #[tokio::test]
    async fn golden_send_media_information_new_device() {
        let (manager, mut rx) = manager_with_send_channel().await;
        manager
            .send_media_information_new_device(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC)
            .await
            .expect("send_media_information_new_device");
        assert_eq!(sent_packet(&mut rx), GOLDEN_MEDIA_INFORMATION_NEW_DEVICE);
    }

    #[tokio::test]
    async fn golden_send_add_tipi_device() {
        let (manager, mut rx) = manager_with_send_channel().await;
        manager
            .send_add_tipi_device(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC)
            .await
            .expect("send_add_tipi_device");
        assert_eq!(sent_packet(&mut rx), GOLDEN_ADD_TIPI_DEVICE);
    }

    #[tokio::test]
    async fn golden_send_hijack_reversed() {
        let (manager, mut rx) = manager_with_send_channel().await;
        manager
            .send_hijack_reversed(GOLDEN_TARGET_MAC)
            .await
            .expect("send_hijack_reversed");
        assert_eq!(sent_packet(&mut rx), GOLDEN_HIJACK_REVERSED);
    }

    // The data packet the encoders return, without the 4-byte AACP header that
    // send_data_packet prepends.
    fn without_header(packet: &[u8]) -> &[u8] {
        assert!(packet.starts_with(&HEADER_BYTES));
        &packet[HEADER_BYTES.len()..]
    }

    #[test]
    fn golden_encode_control_command_owns_connection() {
        assert_eq!(
            encode_control_command(ControlCommandIdentifiers::OwnsConnection, &[0]),
            without_header(GOLDEN_OWNS_CONNECTION_0)
        );
        assert_eq!(
            encode_control_command(ControlCommandIdentifiers::OwnsConnection, &[1]),
            without_header(GOLDEN_OWNS_CONNECTION_1)
        );
    }

    #[test]
    fn encode_control_command_pads_and_truncates_value_to_four_bytes() {
        assert_eq!(
            encode_control_command(ControlCommandIdentifiers::ListeningMode, &[]),
            [0x09, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x00]
        );
        assert_eq!(
            encode_control_command(
                ControlCommandIdentifiers::ListeningMode,
                &[1, 2, 3, 4, 5, 6]
            ),
            [0x09, 0x00, 0x0D, 0x01, 0x02, 0x03, 0x04]
        );
    }

    #[test]
    fn golden_encode_hijack_request() {
        assert_eq!(
            encode_hijack_request(GOLDEN_TARGET_MAC),
            without_header(GOLDEN_HIJACK_REQUEST)
        );
    }

    #[test]
    fn golden_encode_smart_routing_show_ui() {
        assert_eq!(
            encode_smart_routing_show_ui(GOLDEN_TARGET_MAC),
            without_header(GOLDEN_SMART_ROUTING_SHOW_UI)
        );
    }

    #[test]
    fn golden_encode_media_information_streaming() {
        assert_eq!(
            encode_media_information(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC, true),
            without_header(GOLDEN_MEDIA_INFORMATION_STREAMING)
        );
    }

    #[test]
    fn golden_encode_media_information_not_streaming() {
        assert_eq!(
            encode_media_information(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC, false),
            without_header(GOLDEN_MEDIA_INFORMATION_NOT_STREAMING)
        );
    }

    #[test]
    fn golden_encode_media_information_new_device() {
        assert_eq!(
            encode_media_information_new_device(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC),
            without_header(GOLDEN_MEDIA_INFORMATION_NEW_DEVICE)
        );
    }

    #[test]
    fn golden_encode_add_tipi_device() {
        assert_eq!(
            encode_add_tipi_device(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC),
            without_header(GOLDEN_ADD_TIPI_DEVICE)
        );
    }

    #[test]
    fn golden_encode_hijack_reversed() {
        assert_eq!(
            encode_hijack_reversed(GOLDEN_TARGET_MAC),
            without_header(GOLDEN_HIJACK_REVERSED)
        );
    }

    // Current behaviour, kept on purpose by #7: a MAC component that is not
    // valid hex panics instead of returning an error.
    #[test]
    #[should_panic]
    fn encode_hijack_request_invalid_mac_panics() {
        let _ = encode_hijack_request("ZZ:BB:CC:DD:EE:FF");
    }

    // Parser vectors are payloads as receive_packet slices them (`&packet[4..]`,
    // opcode at payload[0]).

    // AUDIO_SOURCE: MAC 11:22:33:44:55:66 stored LSB first, type Media.
    const AUDIO_SOURCE_PAYLOAD: &[u8] = &[0x0E, 0x00, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x02];

    #[test]
    fn golden_parse_audio_source() {
        assert_eq!(
            parse_audio_source(AUDIO_SOURCE_PAYLOAD),
            Some(AudioSource {
                mac: "11:22:33:44:55:66".to_string(),
                r#type: AudioSourceType::Media,
            })
        );
    }

    #[test]
    fn parse_audio_source_types() {
        let mut payload = AUDIO_SOURCE_PAYLOAD.to_vec();
        for (byte, typ) in [
            (0x00, AudioSourceType::None),
            (0x01, AudioSourceType::Call),
            (0x02, AudioSourceType::Media),
            (0x03, AudioSourceType::Unknown),
            (0xFF, AudioSourceType::Unknown),
        ] {
            payload[8] = byte;
            assert_eq!(parse_audio_source(&payload).map(|s| s.r#type), Some(typ));
        }
    }

    #[test]
    fn parse_audio_source_truncated_returns_none() {
        for n in 0..AUDIO_SOURCE_PAYLOAD.len() {
            assert_eq!(
                parse_audio_source(&AUDIO_SOURCE_PAYLOAD[..n]),
                None,
                "n = {n}"
            );
        }
    }

    #[test]
    fn parse_audio_source_oversized_ignores_trailing_bytes() {
        let mut payload = AUDIO_SOURCE_PAYLOAD.to_vec();
        payload.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(
            parse_audio_source(&payload),
            parse_audio_source(AUDIO_SOURCE_PAYLOAD)
        );
    }

    // CONNECTED_DEVICES: count 2 at payload[4], then 8 bytes per device.
    const CONNECTED_DEVICES_PAYLOAD: &[u8] = &[
        0x2E, 0x00, 0x00, 0x00, 0x02, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x01, 0x02, 0x11, 0x22,
        0x33, 0x44, 0x55, 0x66, 0x03, 0x04,
    ];

    fn connected_devices_vector() -> Vec<ConnectedDevice> {
        vec![
            ConnectedDevice {
                mac: "AA:BB:CC:DD:EE:FF".to_string(),
                info1: 0x01,
                info2: 0x02,
                r#type: None,
            },
            ConnectedDevice {
                mac: "11:22:33:44:55:66".to_string(),
                info1: 0x03,
                info2: 0x04,
                r#type: None,
            },
        ]
    }

    #[test]
    fn golden_parse_connected_devices() {
        assert_eq!(
            parse_connected_devices(CONNECTED_DEVICES_PAYLOAD),
            Some(connected_devices_vector())
        );
    }

    #[test]
    fn parse_connected_devices_zero_count_is_empty() {
        assert_eq!(
            parse_connected_devices(&[0x2E, 0x00, 0x00, 0x00, 0x00]),
            Some(Vec::new())
        );
    }

    #[test]
    fn parse_connected_devices_truncated_returns_none() {
        for n in 0..CONNECTED_DEVICES_PAYLOAD.len() {
            assert_eq!(
                parse_connected_devices(&CONNECTED_DEVICES_PAYLOAD[..n]),
                None,
                "n = {n}"
            );
        }
    }

    #[test]
    fn parse_connected_devices_count_255_short_payload_returns_none() {
        let mut payload = CONNECTED_DEVICES_PAYLOAD.to_vec();
        payload[4] = 0xFF;
        assert_eq!(parse_connected_devices(&payload), None);
        assert_eq!(parse_connected_devices(&payload[..5]), None);
    }

    #[test]
    fn parse_connected_devices_oversized_ignores_trailing_bytes() {
        let mut payload = CONNECTED_DEVICES_PAYLOAD.to_vec();
        payload.extend_from_slice(&[0x99; 7]);
        assert_eq!(
            parse_connected_devices(&payload),
            Some(connected_devices_vector())
        );
    }

    // CONTROL_COMMAND: OwnsConnection (0x06) = 1.
    const CONTROL_COMMAND_PAYLOAD: &[u8] = &[0x09, 0x00, 0x06, 0x01, 0x00, 0x00, 0x00];

    fn owns_connection_command(value: Vec<u8>, owns: bool) -> ParsedControlCommand {
        ParsedControlCommand::Known {
            status: ControlCommandStatus {
                identifier: ControlCommandIdentifiers::OwnsConnection,
                value,
            },
            owns_connection: Some(owns),
        }
    }

    #[test]
    fn golden_parse_control_command_owns_connection() {
        assert_eq!(
            parse_control_command(CONTROL_COMMAND_PAYLOAD),
            Some(owns_connection_command(vec![0x01], true))
        );
        assert_eq!(
            parse_control_command(&[0x09, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00]),
            Some(owns_connection_command(vec![0x00], false))
        );
        // The owns flag is payload[3] alone, even when later bytes are set.
        assert_eq!(
            parse_control_command(&[0x09, 0x00, 0x06, 0x00, 0x01, 0x00, 0x00]),
            Some(owns_connection_command(vec![0x00, 0x01], false))
        );
    }

    #[test]
    fn parse_control_command_trims_trailing_zero_bytes() {
        assert_eq!(
            parse_control_command(&[0x09, 0x00, 0x0D, 0x02, 0x00, 0x00, 0x00]),
            Some(ParsedControlCommand::Known {
                status: ControlCommandStatus {
                    identifier: ControlCommandIdentifiers::ListeningMode,
                    value: vec![0x02],
                },
                owns_connection: None,
            })
        );
        assert_eq!(
            parse_control_command(&[0x09, 0x00, 0x1A, 0x00, 0x03, 0x00, 0x04]),
            Some(ParsedControlCommand::Known {
                status: ControlCommandStatus {
                    identifier: ControlCommandIdentifiers::ListeningModeConfigs,
                    value: vec![0x00, 0x03, 0x00, 0x04],
                },
                owns_connection: None,
            })
        );
    }

    #[test]
    fn parse_control_command_unknown_identifier() {
        assert_eq!(
            parse_control_command(&[0x09, 0x00, 0x99, 0x01, 0x00, 0x00, 0x00]),
            Some(ParsedControlCommand::Unknown { identifier: 0x99 })
        );
    }

    #[test]
    fn parse_control_command_truncated_returns_none() {
        for n in 0..CONTROL_COMMAND_PAYLOAD.len() {
            assert_eq!(
                parse_control_command(&CONTROL_COMMAND_PAYLOAD[..n]),
                None,
                "n = {n}"
            );
        }
    }

    #[test]
    fn parse_control_command_oversized_ignores_trailing_bytes() {
        let mut payload = CONTROL_COMMAND_PAYLOAD.to_vec();
        payload.extend_from_slice(&[0x05, 0x06, 0x07]);
        assert_eq!(
            parse_control_command(&payload),
            Some(owns_connection_command(vec![0x01], true))
        );
    }

    // SMART_ROUTING_RESP: opcode, one byte, then text.
    const SMART_ROUTING_PAYLOAD: &[u8] = b"\x11\x00reason:SetOwnershipToFalse";

    #[test]
    fn golden_parse_smart_routing_set_ownership_to_false() {
        assert_eq!(
            parse_smart_routing_response(SMART_ROUTING_PAYLOAD),
            Some(SmartRoutingResponse {
                text: "reason:SetOwnershipToFalse".to_string(),
                set_ownership_to_false: true,
            })
        );
    }

    #[test]
    fn parse_smart_routing_without_ownership_request() {
        assert_eq!(
            parse_smart_routing_response(b"\x11\x00SetOwnership\xFFToFalse"),
            Some(SmartRoutingResponse {
                text: "SetOwnership\u{FFFD}ToFalse".to_string(),
                set_ownership_to_false: false,
            })
        );
        assert_eq!(
            parse_smart_routing_response(&[0x11, 0x00]),
            Some(SmartRoutingResponse {
                text: String::new(),
                set_ownership_to_false: false,
            })
        );
    }

    #[test]
    fn parse_smart_routing_truncated() {
        for n in 0..SMART_ROUTING_PAYLOAD.len() {
            let parsed = parse_smart_routing_response(&SMART_ROUTING_PAYLOAD[..n]);
            if n < 2 {
                assert_eq!(parsed, None, "n = {n}");
            } else {
                let text = String::from_utf8_lossy(&SMART_ROUTING_PAYLOAD[2..n]).into_owned();
                let set_ownership_to_false = text.contains("SetOwnershipToFalse");
                assert_eq!(
                    parsed,
                    Some(SmartRoutingResponse {
                        text,
                        set_ownership_to_false
                    }),
                    "n = {n}"
                );
            }
        }
    }

    #[test]
    fn parse_smart_routing_oversized_keeps_trailing_bytes_in_text() {
        let mut payload = SMART_ROUTING_PAYLOAD.to_vec();
        payload.extend_from_slice(b"\x00tail");
        assert_eq!(
            parse_smart_routing_response(&payload),
            Some(SmartRoutingResponse {
                text: "reason:SetOwnershipToFalse\u{0}tail".to_string(),
                set_ownership_to_false: true,
            })
        );
    }

    fn with_header(payload: &[u8]) -> Vec<u8> {
        [HEADER_BYTES.as_slice(), payload].concat()
    }

    #[tokio::test]
    async fn receive_control_command_owns_connection_sets_owns_and_emits_event() {
        let manager = AACPManager::new();
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        manager.set_event_channel(event_tx).await;
        let (sub_tx, mut sub_rx) = mpsc::unbounded_channel();
        manager
            .subscribe_to_control_command(ControlCommandIdentifiers::OwnsConnection, sub_tx)
            .await;

        manager
            .receive_packet(&with_header(CONTROL_COMMAND_PAYLOAD))
            .await;

        {
            let state = manager.state.lock().await;
            assert!(state.owns);
            assert_eq!(
                state.control_command_status_list,
                vec![ControlCommandStatus {
                    identifier: ControlCommandIdentifiers::OwnsConnection,
                    value: vec![0x01],
                }]
            );
        }
        match event_rx.try_recv() {
            Ok(AACPEvent::ControlCommand(status)) => {
                assert_eq!(status.identifier, ControlCommandIdentifiers::OwnsConnection);
                assert_eq!(status.value, vec![0x01]);
            }
            other => panic!("expected ControlCommand event, got {other:?}"),
        }
        assert_eq!(sub_rx.try_recv().ok(), Some(vec![0x01]));

        manager
            .receive_packet(&with_header(&[0x09, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00]))
            .await;
        {
            let state = manager.state.lock().await;
            assert!(!state.owns);
            assert_eq!(state.control_command_status_list.len(), 1);
            assert_eq!(state.control_command_status_list[0].value, vec![0x00]);
        }
        assert!(matches!(
            event_rx.try_recv(),
            Ok(AACPEvent::ControlCommand(_))
        ));
        assert_eq!(sub_rx.try_recv().ok(), Some(vec![0x00]));
    }

    #[tokio::test]
    async fn receive_smart_routing_set_ownership_to_false_emits_event() {
        let manager = AACPManager::new();
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        manager.set_event_channel(event_tx).await;

        manager
            .receive_packet(&with_header(b"\x11\x00SomethingElse"))
            .await;
        assert!(event_rx.try_recv().is_err());

        manager
            .receive_packet(&with_header(SMART_ROUTING_PAYLOAD))
            .await;
        assert!(matches!(
            event_rx.try_recv(),
            Ok(AACPEvent::OwnershipToFalseRequest)
        ));
        assert!(event_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn receive_short_parsed_packets_change_nothing() {
        let manager = AACPManager::new();
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        manager.set_event_channel(event_tx).await;

        let mut connected_devices_255 = CONNECTED_DEVICES_PAYLOAD.to_vec();
        connected_devices_255[4] = 0xFF;
        let packets = [
            with_header(&AUDIO_SOURCE_PAYLOAD[..8]),
            with_header(&CONNECTED_DEVICES_PAYLOAD[..4]),
            with_header(&connected_devices_255),
            with_header(&CONTROL_COMMAND_PAYLOAD[..6]),
            with_header(&[0x09, 0x00, 0x99, 0x01, 0x00, 0x00, 0x00]),
            with_header(&SMART_ROUTING_PAYLOAD[..1]),
        ];
        for packet in &packets {
            manager.receive_packet(packet).await;
        }

        let state = manager.state.lock().await;
        assert!(state.audio_source.is_none());
        assert!(state.connected_devices.is_empty());
        assert!(state.control_command_status_list.is_empty());
        assert!(!state.owns);
        assert!(event_rx.try_recv().is_err());
    }
}
