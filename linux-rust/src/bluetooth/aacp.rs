use crate::devices::airpods::AirPodsInformation;
use crate::devices::enums::{DeviceData, DeviceInformation, DeviceType};
use crate::settings_file::{parse_devices, rewrite_devices_file};
use crate::utils::get_devices_path;
use bluer::{
    Address, AddressType, Error, Result,
    l2cap::{SeqPacket, Socket, SocketAddr},
};
use log::{debug, error, info, trace, warn};
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
// The AirPods answer the handshake with a non-AACP packet starting with these
// bytes (all captures: 010004000000010003000000000000000000).
const HANDSHAKE_REPLY_PREFIX: [u8; 4] = [0x01, 0x00, 0x04, 0x00];
// The reply can come 1-2 s late after a Mac eviction; requests sent before it are dropped.
const HANDSHAKE_REPLY_TIMEOUT: Duration = Duration::from_secs(3);
const INIT_REQUEST_GAP: Duration = Duration::from_millis(300);
const INIT_REPORT_WAIT: Duration = Duration::from_secs(5);

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
    // Seen from AirPods Pro in practice; no known name or meaning, so unhandled.
    pub const UNKNOWN_0X02: u8 = 0x02;
    pub const UNKNOWN_0X2B: u8 = 0x2B;
    pub const UNKNOWN_0X4C: u8 = 0x4C;
}

/// Opcodes we receive routinely but deliberately ignore (logged at trace, not debug).
pub(crate) fn is_known_ignored_opcode(opcode: u8) -> bool {
    matches!(
        opcode,
        opcodes::UNKNOWN_0X02 | opcodes::UNKNOWN_0X2B | opcodes::UNKNOWN_0X4C
    )
}

pub(crate) fn is_handshake_reply(packet: &[u8]) -> bool {
    packet.starts_with(&HANDSHAKE_REPLY_PREFIX)
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
    /// A host relayed its audio category in a media-information message
    /// (#272).
    MediaInformation {
        host: String,
        category: u32,
    },
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
    pub airpods_mac: Option<Address>,
    handshake_replied: bool,
}

impl AACPManagerState {
    fn clear_connection(&mut self) {
        self.sender = None;
        self.handshake_replied = false;
        self.owns = false;
        self.control_command_status_list.clear();
        self.connected_devices.clear();
        self.old_connected_devices.clear();
        self.audio_source = None;
        self.ear_detection_status.clear();
        self.old_ear_detection_status.clear();
    }

    fn new() -> Self {
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
            airpods_mac: None,
            handshake_replied: false,
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

    /// Attaches the event channel and returns the stored battery report
    /// (empty when none has arrived) under one state-lock guard (#92). The
    /// BATTERY_INFO arm stores and sends under the same lock, so each report
    /// is either in the returned snapshot (it arrived before the attach) or
    /// queued as `AACPEvent::BatteryInfo` (it arrived after), never both.
    /// Nothing is awaited while the guard is held.
    pub async fn attach_event_channel(
        &self,
        tx: mpsc::UnboundedSender<AACPEvent>,
    ) -> Vec<BatteryInfo> {
        let mut state = self.state.lock().await;
        state.event_tx = Some(tx);
        state.battery_info.clone()
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
            if is_handshake_reply(packet)
                && !std::mem::replace(&mut self.state.lock().await.handshake_replied, true)
            {
                info!("Received AACP handshake reply: {}", hex::encode(packet));
            } else {
                debug!(
                    "Received packet does not start with expected header: {}",
                    hex::encode(packet)
                );
            }
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
                let state = self.state.lock().await;
                if let Some(mac) = state.airpods_mac {
                    let mac = mac.to_string();
                    update_devices_file(&get_devices_path(), |d| {
                        devices_with_information(d, &mac, &info)
                    });
                }
                info!("Received Information: {:?}", info);
            }

            opcodes::PROXIMITY_KEYS_RSP => {
                if payload.len() < 4 {
                    error!(
                        "Proximity Keys Response packet too short: {} bytes",
                        payload.len()
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
                            "Proximity Keys Response packet too short while parsing keys: {} bytes",
                            payload.len()
                        );
                        return;
                    }
                    let key_type = payload[offset];
                    let key_length = payload[offset + 2] as usize;
                    offset += 4;
                    if offset + key_length > payload.len() {
                        error!(
                            "Proximity Keys Response packet too short for key data: {} bytes",
                            payload.len()
                        );
                        return;
                    }
                    let key_data = payload[offset..offset + key_length].to_vec();
                    keys.push((key_type, key_data));
                    offset += key_length;
                }
                info!(
                    "Received Proximity Keys Response: {}",
                    proximity_keys_summary(&keys)
                );
                let state = self.state.lock().await;
                if let Some(mac) = state.airpods_mac {
                    let mac = mac.to_string();
                    update_devices_file(&get_devices_path(), |d| {
                        devices_with_proximity_keys(d, &mac, &keys)
                    });
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
                if let Some(info) = parse_media_information(payload)
                    && let Some(category) = info.category
                {
                    let host = info.host().to_string();
                    info!("Received media information: {host} audio category {category}");
                    if let Some(ref tx) = self.state.lock().await.event_tx {
                        let _ = tx.send(AACPEvent::MediaInformation { host, category });
                    }
                }
            }
            opcodes::EQ_DATA => {
                debug!("Received EQ Data");
            }
            op if is_known_ignored_opcode(op) => {
                trace!(
                    "Ignoring packet with opcode {op:#04x}, len {}",
                    payload.len()
                );
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

    /// Session init: handshake, wait (bounded) for its reply, then feature
    /// flags and the notification request. If no host-state report follows
    /// within INIT_REPORT_WAIT, the three are re-sent once (as Android does).
    pub async fn initialize(&self) -> Result<()> {
        self.send_handshake().await?;
        if !self
            .wait_for_state(HANDSHAKE_REPLY_TIMEOUT, |s| s.handshake_replied)
            .await?
        {
            warn!(
                "No AACP handshake reply within {HANDSHAKE_REPLY_TIMEOUT:?}, sending requests anyway"
            );
        }
        self.send_init_requests().await?;
        let reported = |s: &AACPManagerState| {
            !s.connected_devices.is_empty() || !s.ear_detection_status.is_empty()
        };
        if self.wait_for_state(INIT_REPORT_WAIT, reported).await? {
            return Ok(());
        }
        warn!("No host-state report within {INIT_REPORT_WAIT:?}, re-sending init requests once");
        self.send_handshake().await?;
        sleep(INIT_REQUEST_GAP).await;
        self.send_init_requests().await
    }

    async fn send_init_requests(&self) -> Result<()> {
        self.send_set_feature_flags_packet().await?;
        sleep(INIT_REQUEST_GAP).await;
        self.send_notification_request().await
    }

    /// Polls the state until `done` holds (true) or `timeout` passes (false).
    /// NotConnected once the session drops. The lock is never held across a sleep.
    async fn wait_for_state(
        &self,
        timeout: Duration,
        done: impl Fn(&AACPManagerState) -> bool,
    ) -> Result<bool> {
        let deadline = Instant::now() + timeout;
        loop {
            {
                let state = self.state.lock().await;
                if done(&state) {
                    return Ok(true);
                }
                if state.sender.is_none() {
                    return Err(Error::from(std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        "AACP session dropped during init",
                    )));
                }
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            sleep(POLL_INTERVAL).await;
        }
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
        )?)
        .await
    }

    pub async fn send_media_information_call(
        &self,
        self_mac_address: &str,
        target_mac_address: &str,
    ) -> Result<()> {
        self.send_data_packet(&encode_media_information_call(
            self_mac_address,
            target_mac_address,
        )?)
        .await
    }

    pub async fn send_media_information_category(
        &self,
        self_mac_address: &str,
        target_mac_address: &str,
        category: u32,
    ) -> Result<()> {
        self.send_data_packet(&encode_media_information_category(
            self_mac_address,
            target_mac_address,
            category,
        )?)
        .await
    }

    pub async fn send_hijack_request(&self, target_mac_address: &str) -> Result<()> {
        self.send_data_packet(&encode_hijack_request(target_mac_address)?)
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
        )?)
        .await
    }

    pub async fn send_smart_routing_show_ui(&self, target_mac_address: &str) -> Result<()> {
        self.send_data_packet(&encode_smart_routing_show_ui(target_mac_address)?)
            .await
    }

    // Caller arrives with #17 (Undo notification).
    pub async fn send_hijack_reversed(&self, target_mac_address: &str) -> Result<()> {
        self.send_data_packet(&encode_hijack_reversed(target_mac_address)?)
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
        )?)
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

// Parses "AA:BB:CC:DD:EE:FF" into bytes in address order. Exactly six
// components, each exactly two hex digits in either case. Anything else,
// including a leading '+' (which from_str_radix would accept), gives None.
pub(crate) fn parse_mac(s: &str) -> Option<[u8; 6]> {
    let mut out = [0u8; 6];
    let mut parts = s.split(':');
    for slot in out.iter_mut() {
        let part = parts.next()?;
        if part.len() != 2 || !part.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        *slot = u8::from_str_radix(part, 16).ok()?;
    }
    if parts.next().is_some() {
        return None;
    }
    Some(out)
}

fn invalid_mac_error(mac_address: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "invalid target MAC address {mac_address:?}: expected six colon-separated hex octets"
        ),
    )
}

// Target MAC components in reverse order, as every smart-routing packet sends
// them. An invalid address is an InvalidInput error, not a panic.
fn reversed_mac_bytes(mac_address: &str) -> std::io::Result<[u8; 6]> {
    let mut mac = parse_mac(mac_address).ok_or_else(|| invalid_mac_error(mac_address))?;
    mac.reverse();
    Ok(mac)
}

// OPACK value writers: the subset of Apple's OPACK encoding that the
// smart-routing dictionaries use, matching the audioaccessoryd relay payloads
// captured in #35.

// String: 0x40+n for n <= 32, then 0x61/0x62/0x63/0x64 with a 1/2/3/4-byte
// little-endian length.
fn opack_write_str(out: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let n = bytes.len();
    if n <= 0x20 {
        out.push(0x40 + n as u8);
    } else if n <= 0xFF {
        out.push(0x61);
        out.push(n as u8);
    } else if n <= 0xFFFF {
        out.push(0x62);
        out.extend_from_slice(&(n as u16).to_le_bytes());
    } else if n <= 0xFF_FFFF {
        out.push(0x63);
        out.extend_from_slice(&(n as u32).to_le_bytes()[..3]);
    } else {
        out.push(0x64);
        out.extend_from_slice(&(n as u32).to_le_bytes());
    }
    out.extend_from_slice(bytes);
}

// Non-negative integer. OPACK ints are signed (Apple reads `30 c9` as -55):
// 0x08+n for 0..=39, then the narrowest of 0x30 <i8>, 0x31 <i16 LE>,
// 0x32 <i32 LE> and 0x33 <i64 LE>.
fn opack_write_uint(out: &mut Vec<u8>, n: u32) {
    if n <= 39 {
        out.push(0x08 + n as u8);
    } else if let Ok(n) = i8::try_from(n) {
        out.push(0x30);
        out.extend_from_slice(&n.to_le_bytes());
    } else if let Ok(n) = i16::try_from(n) {
        out.push(0x31);
        out.extend_from_slice(&n.to_le_bytes());
    } else if let Ok(n) = i32::try_from(n) {
        out.push(0x32);
        out.extend_from_slice(&n.to_le_bytes());
    } else {
        out.push(0x33);
        out.extend_from_slice(&i64::from(n).to_le_bytes());
    }
}

fn opack_write_true(out: &mut Vec<u8>) {
    out.push(0x01);
}

// Back-reference to an earlier object in the same dict (0xA0 + index, index
// < 32). Apple's captured Hijackv2 payload uses 0xA5 for remotescore (301).
fn opack_write_ref(out: &mut Vec<u8>, index: u8) {
    debug_assert!(index < 0x20);
    out.push(0xA0 + index);
}

// Dictionary header for `count` key/value pairs (count < 15).
fn opack_write_dict_header(out: &mut Vec<u8>, count: u8) {
    debug_assert!(count < 0x0F);
    out.push(0xE0 + count);
}

// OPACK dict of a media-information relay message, in Apple's key order:
// playingApp, hostStreamingState ("YES"/"NO"), btAddress, btName,
// otherDeviceAudioCategory.
pub(crate) fn media_information_body(
    playing_app: &str,
    host_streaming: bool,
    bt_address: &str,
    bt_name: &str,
    audio_category: u32,
) -> Vec<u8> {
    let mut dict = Vec::with_capacity(128);
    opack_write_dict_header(&mut dict, 5);
    opack_write_str(&mut dict, "playingApp");
    opack_write_str(&mut dict, playing_app);
    opack_write_str(&mut dict, "hostStreamingState");
    opack_write_str(&mut dict, if host_streaming { "YES" } else { "NO" });
    opack_write_str(&mut dict, "btAddress");
    opack_write_str(&mut dict, bt_address);
    opack_write_str(&mut dict, "btName");
    opack_write_str(&mut dict, bt_name);
    opack_write_str(&mut dict, "otherDeviceAudioCategory");
    opack_write_uint(&mut dict, audio_category);
    dict
}

// OPACK dict of a tipi relay message, in Apple's key order: idleTime,
// newTipi (only present when true, as Apple omits it otherwise), btAddress,
// btName, nearbyAudioScore.
pub(crate) fn tipi_body(
    idle_time: u32,
    new_tipi: bool,
    bt_address: &str,
    bt_name: &str,
    nearby_audio_score: u32,
) -> Vec<u8> {
    let mut dict = Vec::with_capacity(80);
    opack_write_dict_header(&mut dict, if new_tipi { 5 } else { 4 });
    opack_write_str(&mut dict, "idleTime");
    opack_write_uint(&mut dict, idle_time);
    if new_tipi {
        opack_write_str(&mut dict, "newTipi");
        opack_write_true(&mut dict);
    }
    opack_write_str(&mut dict, "btAddress");
    opack_write_str(&mut dict, bt_address);
    opack_write_str(&mut dict, "btName");
    opack_write_str(&mut dict, bt_name);
    opack_write_str(&mut dict, "nearbyAudioScore");
    opack_write_uint(&mut dict, nearby_audio_score);
    dict
}

// Smart-routing data packet: opcode, reversed target MAC, u16 LE length of
// what follows (the 0x01 byte plus the dict), 0x01, then the dict with no
// padding. A dict too long for the u16 length field is an InvalidInput error.
fn smart_routing_packet(target_mac_address: &str, dict: &[u8]) -> std::io::Result<Vec<u8>> {
    let mac = reversed_mac_bytes(target_mac_address)?;
    let length = u16::try_from(1 + dict.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "smart-routing dict of {} bytes does not fit the u16 length field",
                dict.len()
            ),
        )
    })?;
    let mut packet = Vec::with_capacity(11 + dict.len());
    packet.extend_from_slice(&[opcodes::SMART_ROUTING, 0x00]);
    packet.extend_from_slice(&mac);
    packet.extend_from_slice(&length.to_le_bytes());
    packet.push(0x01);
    packet.extend_from_slice(dict);
    Ok(packet)
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
) -> std::io::Result<Vec<u8>> {
    let dict = media_information_body("NA", false, self_mac_address, "Mac", 100);
    smart_routing_packet(target_mac_address, &dict)
}

// otherDeviceAudioCategory for a host in a call (#11).
//
// From mo's iPhone phone-call capture on #74 (2026-11-07): during a cellular
// call the iPhone relays otherDeviceAudioCategory 501, OPACK `31 f5 01`
// (pinned as APPLE_MEDIA_INFORMATION_CALL_YES_501). The same capture shows 100
// for idle and 301 for media. A VoIP/FaceTime call has not been captured yet
// and may use another value.
pub(crate) const AUDIO_CATEGORY_CALL: u32 = 501;

// Media information that tells the other hosts Linux is in a call: the same
// dict as `encode_media_information_new_device` (playingApp "NA"), with
// hostStreamingState YES and otherDeviceAudioCategory AUDIO_CATEGORY_CALL.
pub(crate) fn encode_media_information_call(
    self_mac_address: &str,
    target_mac_address: &str,
) -> std::io::Result<Vec<u8>> {
    let dict = media_information_body("NA", true, self_mac_address, "Mac", AUDIO_CATEGORY_CALL);
    smart_routing_packet(target_mac_address, &dict)
}

// Media information that relays Linux's audio category to another host, as
// the Mac relays its own (#272).
pub(crate) fn encode_media_information_category(
    self_mac_address: &str,
    target_mac_address: &str,
    category: u32,
) -> std::io::Result<Vec<u8>> {
    let dict = media_information_body("Unknown", false, self_mac_address, "Mac", category);
    smart_routing_packet(target_mac_address, &dict)
}

pub(crate) fn encode_hijack_request(target_mac_address: &str) -> std::io::Result<Vec<u8>> {
    let mut dict = Vec::with_capacity(97);
    opack_write_dict_header(&mut dict, 5);
    opack_write_str(&mut dict, "localscore");
    opack_write_uint(&mut dict, 100);
    opack_write_str(&mut dict, "reason");
    opack_write_str(&mut dict, "Hijackv2");
    opack_write_str(&mut dict, "audioRoutingScore");
    opack_write_uint(&mut dict, 301);
    opack_write_str(&mut dict, "audioRoutingSetOwnershipToFalse");
    opack_write_true(&mut dict);
    opack_write_str(&mut dict, "remotescore");
    opack_write_ref(&mut dict, 5);
    smart_routing_packet(target_mac_address, &dict)
}

pub(crate) fn encode_media_information(
    self_mac_address: &str,
    target_mac_address: &str,
    streaming_state: bool,
) -> std::io::Result<Vec<u8>> {
    let dict = media_information_body(
        "com.google.ios.youtube",
        streaming_state,
        self_mac_address,
        "Mac",
        301,
    );
    smart_routing_packet(target_mac_address, &dict)
}

pub(crate) fn encode_smart_routing_show_ui(target_mac_address: &str) -> std::io::Result<Vec<u8>> {
    let mut dict = Vec::with_capacity(125);
    opack_write_dict_header(&mut dict, 6);
    opack_write_str(&mut dict, "SmartRoutingKeyShowNearbyUI");
    opack_write_true(&mut dict);
    opack_write_str(&mut dict, "localscore");
    opack_write_uint(&mut dict, 301);
    opack_write_str(&mut dict, "reason");
    // Lower-case "hijackv2" is deliberate. The old literal "reasonHhijackv2"
    // was the key "reason" followed by the string-length tag for eight bytes
    // and "hijackv2", so this writes the same bytes. Don't capitalise it.
    opack_write_str(&mut dict, "hijackv2");
    opack_write_str(&mut dict, "audioRoutingScore");
    opack_write_ref(&mut dict, 2);
    opack_write_str(&mut dict, "audioRoutingSetOwnershipToFalse");
    opack_write_true(&mut dict);
    opack_write_str(&mut dict, "remotescore");
    opack_write_ref(&mut dict, 2);
    smart_routing_packet(target_mac_address, &dict)
}

// Caller arrives with #17 (Undo notification).
pub(crate) fn encode_hijack_reversed(target_mac_address: &str) -> std::io::Result<Vec<u8>> {
    let mut dict = Vec::with_capacity(88);
    opack_write_dict_header(&mut dict, 3);
    opack_write_str(&mut dict, "audioRoutingSetOwnershipToFalse");
    opack_write_true(&mut dict);
    opack_write_str(&mut dict, "audioRoutingShowReverseUI");
    opack_write_true(&mut dict);
    opack_write_str(&mut dict, "reason");
    opack_write_str(&mut dict, "ReverseBannerTapped");
    smart_routing_packet(target_mac_address, &dict)
}

pub(crate) fn encode_add_tipi_device(
    self_mac_address: &str,
    target_mac_address: &str,
) -> std::io::Result<Vec<u8>> {
    let dict = tipi_body(0, true, self_mac_address, "Mac", 6);
    smart_routing_packet(target_mac_address, &dict)
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

/// `devices` with `info` set on `mac`'s entry, keeping each stored LE key that
/// `info` leaves empty; a missing entry is not created.
fn devices_with_information(
    mut devices: HashMap<String, DeviceData>,
    mac: &str,
    info: &AirPodsInformation,
) -> HashMap<String, DeviceData> {
    if let Some(device_data) = devices.get_mut(mac) {
        let mut info = info.clone();
        if let Some(DeviceInformation::AirPods(stored)) = &device_data.information {
            if info.le_keys.irk.is_empty() {
                info.le_keys.irk = stored.le_keys.irk.clone();
            }
            if info.le_keys.enc_key.is_empty() {
                info.le_keys.enc_key = stored.le_keys.enc_key.clone();
            }
        }
        device_data.name = info.name.clone();
        device_data.information = Some(DeviceInformation::AirPods(info));
    }
    devices
}

/// Key types and lengths of `keys`, never the key bytes.
fn proximity_keys_summary(keys: &[(u8, Vec<u8>)]) -> String {
    keys.iter()
        .map(|(kt, kd)| match ProximityKeyType::from_u8(*kt) {
            Some(t) => format!("{t:?} ({} bytes)", kd.len()),
            None => format!("type {kt:#04x} ({} bytes)", kd.len()),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// `devices` with each known key in `keys` stored on `mac`'s entry, which is
/// created (named after the MAC) if missing. An entry without information gets
/// AirPods information with empty strings, so the keys survive until the
/// Information packet fills the rest in.
fn devices_with_proximity_keys(
    mut devices: HashMap<String, DeviceData>,
    mac: &str,
    keys: &[(u8, Vec<u8>)],
) -> HashMap<String, DeviceData> {
    for (key_type, key_data) in keys {
        let Some(kt) = ProximityKeyType::from_u8(*key_type) else {
            continue;
        };
        let device_data = devices.entry(mac.to_string()).or_insert(DeviceData {
            name: mac.to_string(),
            type_: DeviceType::AirPods,
            information: None,
        });
        let information = device_data.information.get_or_insert_with(|| {
            DeviceInformation::AirPods(AirPodsInformation {
                name: String::new(),
                model_number: String::new(),
                manufacturer: String::new(),
                serial_number: String::new(),
                version1: String::new(),
                version2: String::new(),
                hardware_revision: String::new(),
                updater_identifier: String::new(),
                left_serial_number: String::new(),
                right_serial_number: String::new(),
                version3: String::new(),
                le_keys: AirPodsLEKeys {
                    irk: String::new(),
                    enc_key: String::new(),
                },
            })
        });
        match (kt, information) {
            (ProximityKeyType::Irk, DeviceInformation::AirPods(info)) => {
                info.le_keys.irk = hex::encode(key_data);
            }
            (ProximityKeyType::EncKey, DeviceInformation::AirPods(info)) => {
                info.le_keys.enc_key = hex::encode(key_data);
            }
            (ProximityKeyType::Irk, _) => {
                error!("Device information is not AirPods for adding LE IRK.");
            }
            (ProximityKeyType::EncKey, _) => {
                error!("Device information is not AirPods for adding LE encryption key.");
            }
        }
    }
    devices
}

/// Applies `update` to a fresh read of the devices file at `path` and writes
/// the result back atomically, so devices saved since connect are kept.
/// Returns whether it was written; on any error logs it and writes nothing.
fn update_devices_file(
    path: &std::path::Path,
    update: impl FnOnce(HashMap<String, DeviceData>) -> HashMap<String, DeviceData>,
) -> bool {
    let result = rewrite_devices_file(path, |existing| {
        let devices = update(parse_devices(existing)?);
        Ok(serde_json::to_string(&devices)?)
    });
    if let Err(e) = result {
        error!("Not saving devices to {}: {}", path.display(), e);
        return false;
    }
    true
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

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MediaInformation {
    /// The header's sender address.
    pub sender: String,
    /// The dict's `btAddress`, upper case; None if missing or not an address.
    pub bt_address: Option<String>,
    /// The dict's `otherDeviceAudioCategory`.
    pub category: Option<u32>,
}

impl MediaInformation {
    /// The relaying host: `btAddress`, unless missing or all-zero (Apple
    /// zeroes it in some relays), else the header's sender.
    pub fn host(&self) -> &str {
        self.bt_address
            .as_deref()
            .filter(|a| *a != "00:00:00:00:00:00")
            .unwrap_or(&self.sender)
    }
}

// SMART_ROUTING_RESP (0x11) carrying a media-information dict (#272). Header
// as inferred from logs, mirroring `smart_routing_packet`: opcode echo (2),
// sender address reversed (6), u16 LE length, 0x01, then the OPACK dict at
// payload[11]. None without a dict there. A field whose value is missing,
// truncated, a back-reference or behind an unknown tag is None.
pub(crate) fn parse_media_information(payload: &[u8]) -> Option<MediaInformation> {
    let tag = *payload.get(11)?;
    if !(0xE0..=0xEF).contains(&tag) {
        return None;
    }
    let sender = &payload[2..8];
    let mut info = MediaInformation {
        sender: format!(
            "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
            sender[5], sender[4], sender[3], sender[2], sender[1], sender[0]
        ),
        bt_address: None,
        category: None,
    };
    // 0xEF (until 0x03) stops at the terminator, which reads as no value.
    let count = if tag == 0xEF {
        usize::MAX
    } else {
        usize::from(tag - 0xE0)
    };
    let mut pos = 12;
    for _ in 0..count {
        let Some((Opack::Str(key), next)) = opack_read(payload, pos) else {
            break;
        };
        let Some((value, next)) = opack_read(payload, next) else {
            break;
        };
        pos = next;
        match (key, value) {
            ("btAddress", Opack::Str(address)) => {
                info.bt_address = parse_mac(address).map(|_| address.to_ascii_uppercase())
            }
            ("otherDeviceAudioCategory", Opack::Uint(n)) => info.category = Some(n),
            _ => {}
        }
    }
    Some(info)
}

// One OPACK value as `parse_media_information` reads it.
enum Opack<'a> {
    Str(&'a str),
    Uint(u32),
    /// A bool, a back-reference, a non-UTF-8 string, or an int that is negative or above u32::MAX.
    Other,
}

// The OPACK value at `pos` and the offset past it; None for a truncated
// value or a tag whose length is unknown (the writers' subset above, plus
// bools and back-references).
fn opack_read(buf: &[u8], pos: usize) -> Option<(Opack<'_>, usize)> {
    let tag = *buf.get(pos)?;
    let le = |start: usize, width: usize| {
        let bytes = buf.get(start..start.checked_add(width)?)?;
        Some(
            bytes
                .iter()
                .rev()
                .fold(0u32, |n, &b| (n << 8) | u32::from(b)),
        )
    };
    let string = |start: usize, len: u32| {
        let end = start.checked_add(usize::try_from(len).ok()?)?;
        let value = std::str::from_utf8(buf.get(start..end)?).map_or(Opack::Other, Opack::Str);
        Some((value, end))
    };
    match tag {
        0x01 | 0x02 | 0xA0..=0xBF => Some((Opack::Other, pos + 1)),
        0x08..=0x2F => Some((Opack::Uint(u32::from(tag - 0x08)), pos + 1)),
        0x30..=0x33 => {
            // Signed LE int: start from the sign fill, then shift in each byte.
            let width = 1 << (tag - 0x30);
            let bytes = buf.get(pos + 1..pos + 1 + width)?;
            let fill = i64::from(bytes[width - 1] as i8 >> 7);
            let n = bytes
                .iter()
                .rev()
                .fold(fill, |n, &b| (n << 8) | i64::from(b));
            // A negative or over-u32 category/score is never valid.
            let value = u32::try_from(n).map_or(Opack::Other, Opack::Uint);
            Some((value, pos + 1 + width))
        }
        0x40..=0x60 => string(pos + 1, u32::from(tag - 0x40)),
        0x61..=0x64 => {
            let width = usize::from(tag - 0x60);
            string(pos + 1 + width, le(pos + 1, width)?)
        }
        _ => None,
    }
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
    // channel (header included). They were captured from the implementation
    // before the encoders were extracted (#7) and pin current behaviour.
    // Protocol changes need their own ticket and must justify each changed
    // byte here.
    //
    // #35 aligned three of them with the audioaccessoryd (BTSmartRoutingDaemon)
    // relay payloads that mo captured from an iPhone in #35 (the
    // golden_apple_* tests below hold that hex verbatim). The values Linux
    // sends did not change, only the layout:
    // - GOLDEN_MEDIA_INFORMATION_STREAMING / _NOT_STREAMING:
    //   - length 0x82 -> 0x7E (streaming) / 0x7D (not streaming): the length
    //     is 1 (the 0x01 byte) + the dict, 125 / 124 bytes. Every captured
    //     payload is exactly one dict with nothing after it, so the trailing
    //     0x00 padding the old length counted is gone (140 / 139 bytes total).
    //   - "PlayingApp" 0x50 -> 0x70 and "HostStreamingState" 0x48 -> 0x68:
    //     Apple's keys are lowercase, `4a 70 6c 61 79 69 6e 67 41 70 70` and
    //     `52 68 6f 73 74 53 74 72 65 61 6d ...`.
    //   - "YES" tag 0x42 -> 0x43: a 3-byte string, as in the capture's
    //     `43 59 45 53` ("NO" stays `42 4e 4f`).
    //   - 0x46 inserted before "btName": the key was a bare string with no
    //     tag; the capture has `46 62 74 4e 61 6d 65`.
    // - GOLDEN_ADD_TIPI_DEVICE: the stray 0x69 after the key tag 0x48 is
    //   removed, giving `48 69 64 6c 65 54 69 6d 65` ("idleTime") as in the
    //   capture. The length 0x4E (1 + the 77-byte dict) was already right; the
    //   packet is now 92 bytes.
    // The other constants are unchanged, quirks included ("reasonHhijackv2"
    // in show-UI). GOLDEN_HIJACK_REQUEST already equals Apple's own Hijackv2
    // payload (golden_apple_hijack_request).
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
        0x04, 0x00, 0x04, 0x00, 0x10, 0x00, 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x7E, 0x00, 0x01,
        0xE5, 0x4A, 0x70, 0x6C, 0x61, 0x79, 0x69, 0x6E, 0x67, 0x41, 0x70, 0x70, 0x56, 0x63, 0x6F,
        0x6D, 0x2E, 0x67, 0x6F, 0x6F, 0x67, 0x6C, 0x65, 0x2E, 0x69, 0x6F, 0x73, 0x2E, 0x79, 0x6F,
        0x75, 0x74, 0x75, 0x62, 0x65, 0x52, 0x68, 0x6F, 0x73, 0x74, 0x53, 0x74, 0x72, 0x65, 0x61,
        0x6D, 0x69, 0x6E, 0x67, 0x53, 0x74, 0x61, 0x74, 0x65, 0x43, 0x59, 0x45, 0x53, 0x49, 0x62,
        0x74, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x51, 0x31, 0x31, 0x3A, 0x32, 0x32, 0x3A,
        0x33, 0x33, 0x3A, 0x34, 0x34, 0x3A, 0x35, 0x35, 0x3A, 0x36, 0x36, 0x46, 0x62, 0x74, 0x4E,
        0x61, 0x6D, 0x65, 0x43, 0x4D, 0x61, 0x63, 0x58, 0x6F, 0x74, 0x68, 0x65, 0x72, 0x44, 0x65,
        0x76, 0x69, 0x63, 0x65, 0x41, 0x75, 0x64, 0x69, 0x6F, 0x43, 0x61, 0x74, 0x65, 0x67, 0x6F,
        0x72, 0x79, 0x31, 0x2D, 0x01,
    ];
    const GOLDEN_MEDIA_INFORMATION_NOT_STREAMING: &[u8] = &[
        0x04, 0x00, 0x04, 0x00, 0x10, 0x00, 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x7D, 0x00, 0x01,
        0xE5, 0x4A, 0x70, 0x6C, 0x61, 0x79, 0x69, 0x6E, 0x67, 0x41, 0x70, 0x70, 0x56, 0x63, 0x6F,
        0x6D, 0x2E, 0x67, 0x6F, 0x6F, 0x67, 0x6C, 0x65, 0x2E, 0x69, 0x6F, 0x73, 0x2E, 0x79, 0x6F,
        0x75, 0x74, 0x75, 0x62, 0x65, 0x52, 0x68, 0x6F, 0x73, 0x74, 0x53, 0x74, 0x72, 0x65, 0x61,
        0x6D, 0x69, 0x6E, 0x67, 0x53, 0x74, 0x61, 0x74, 0x65, 0x42, 0x4E, 0x4F, 0x49, 0x62, 0x74,
        0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x51, 0x31, 0x31, 0x3A, 0x32, 0x32, 0x3A, 0x33,
        0x33, 0x3A, 0x34, 0x34, 0x3A, 0x35, 0x35, 0x3A, 0x36, 0x36, 0x46, 0x62, 0x74, 0x4E, 0x61,
        0x6D, 0x65, 0x43, 0x4D, 0x61, 0x63, 0x58, 0x6F, 0x74, 0x68, 0x65, 0x72, 0x44, 0x65, 0x76,
        0x69, 0x63, 0x65, 0x41, 0x75, 0x64, 0x69, 0x6F, 0x43, 0x61, 0x74, 0x65, 0x67, 0x6F, 0x72,
        0x79, 0x31, 0x2D, 0x01,
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
    // #11's in-call media information (encode_media_information_call). It is
    // GOLDEN_MEDIA_INFORMATION_NEW_DEVICE with these bytes changed:
    // - length 0x68 -> 0x6A: the dict grows by two bytes (one each below).
    // - "NO" `42 4E 4F` -> "YES" `43 59 45 53`: hostStreamingState YES, a
    //   3-byte string as in Apple's streaming captures (#35).
    // - otherDeviceAudioCategory `30 64` (100) -> `31 F5 01`: the u16 LE form
    //   of AUDIO_CATEGORY_CALL (501 = 0x01F5), the value the iPhone relays
    //   during a phone call (#74 capture, APPLE_MEDIA_INFORMATION_CALL_YES_501).
    const GOLDEN_MEDIA_INFORMATION_CALL: &[u8] = &[
        0x04, 0x00, 0x04, 0x00, 0x10, 0x00, 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x6A, 0x00, 0x01,
        0xE5, 0x4A, 0x70, 0x6C, 0x61, 0x79, 0x69, 0x6E, 0x67, 0x41, 0x70, 0x70, 0x42, 0x4E, 0x41,
        0x52, 0x68, 0x6F, 0x73, 0x74, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x69, 0x6E, 0x67, 0x53,
        0x74, 0x61, 0x74, 0x65, 0x43, 0x59, 0x45, 0x53, 0x49, 0x62, 0x74, 0x41, 0x64, 0x64, 0x72,
        0x65, 0x73, 0x73, 0x51, 0x31, 0x31, 0x3A, 0x32, 0x32, 0x3A, 0x33, 0x33, 0x3A, 0x34, 0x34,
        0x3A, 0x35, 0x35, 0x3A, 0x36, 0x36, 0x46, 0x62, 0x74, 0x4E, 0x61, 0x6D, 0x65, 0x43, 0x4D,
        0x61, 0x63, 0x58, 0x6F, 0x74, 0x68, 0x65, 0x72, 0x44, 0x65, 0x76, 0x69, 0x63, 0x65, 0x41,
        0x75, 0x64, 0x69, 0x6F, 0x43, 0x61, 0x74, 0x65, 0x67, 0x6F, 0x72, 0x79, 0x31, 0xF5, 0x01,
    ];
    const GOLDEN_ADD_TIPI_DEVICE: &[u8] = &[
        0x04, 0x00, 0x04, 0x00, 0x10, 0x00, 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x4E, 0x00, 0x01,
        0xE5, 0x48, 0x69, 0x64, 0x6C, 0x65, 0x54, 0x69, 0x6D, 0x65, 0x08, 0x47, 0x6E, 0x65, 0x77,
        0x54, 0x69, 0x70, 0x69, 0x01, 0x49, 0x62, 0x74, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73,
        0x51, 0x31, 0x31, 0x3A, 0x32, 0x32, 0x3A, 0x33, 0x33, 0x3A, 0x34, 0x34, 0x3A, 0x35, 0x35,
        0x3A, 0x36, 0x36, 0x46, 0x62, 0x74, 0x4E, 0x61, 0x6D, 0x65, 0x43, 0x4D, 0x61, 0x63, 0x50,
        0x6E, 0x65, 0x61, 0x72, 0x62, 0x79, 0x41, 0x75, 0x64, 0x69, 0x6F, 0x53, 0x63, 0x6F, 0x72,
        0x65, 0x0E,
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
    async fn golden_send_media_information_call() {
        let (manager, mut rx) = manager_with_send_channel().await;
        manager
            .send_media_information_call(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC)
            .await
            .expect("send_media_information_call");
        assert_eq!(sent_packet(&mut rx), GOLDEN_MEDIA_INFORMATION_CALL);
    }

    #[test]
    fn golden_encode_media_information_call() {
        assert_eq!(AUDIO_CATEGORY_CALL, 501);
        let packet = encode_media_information_call(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC)
            .expect("encode_media_information_call");
        assert_eq!(packet, without_header(GOLDEN_MEDIA_INFORMATION_CALL));
        // Built by the shared builders: the new-device dict with only the
        // streaming state and the category changed.
        assert_eq!(
            packet[11..],
            media_information_body("NA", true, GOLDEN_SELF_MAC, "Mac", AUDIO_CATEGORY_CALL)
        );
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
            encode_hijack_request(GOLDEN_TARGET_MAC).expect("encode_hijack_request"),
            without_header(GOLDEN_HIJACK_REQUEST)
        );
    }

    #[test]
    fn golden_encode_smart_routing_show_ui() {
        assert_eq!(
            encode_smart_routing_show_ui(GOLDEN_TARGET_MAC).expect("encode_smart_routing_show_ui"),
            without_header(GOLDEN_SMART_ROUTING_SHOW_UI)
        );
    }

    #[test]
    fn golden_encode_media_information_streaming() {
        assert_eq!(
            encode_media_information(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC, true)
                .expect("encode_media_information"),
            without_header(GOLDEN_MEDIA_INFORMATION_STREAMING)
        );
    }

    #[test]
    fn golden_encode_media_information_not_streaming() {
        assert_eq!(
            encode_media_information(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC, false)
                .expect("encode_media_information"),
            without_header(GOLDEN_MEDIA_INFORMATION_NOT_STREAMING)
        );
    }

    #[test]
    fn golden_encode_media_information_new_device() {
        assert_eq!(
            encode_media_information_new_device(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC)
                .expect("encode_media_information_new_device"),
            without_header(GOLDEN_MEDIA_INFORMATION_NEW_DEVICE)
        );
    }

    #[test]
    fn golden_encode_media_information_category_201() {
        let packet = encode_media_information_category(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC, 201)
            .expect("encode_media_information_category");
        // "...Category" then OPACK i16 201, as Apple's own relay sends it.
        assert!(
            packet.ends_with(&[0x6f, 0x72, 0x79, 0x31, 0xc9, 0x00]),
            "{packet:02x?}"
        );
        assert_eq!(
            packet[11..],
            media_information_body("Unknown", false, GOLDEN_SELF_MAC, "Mac", 201)[..]
        );
    }

    #[test]
    fn golden_encode_add_tipi_device() {
        assert_eq!(
            encode_add_tipi_device(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC)
                .expect("encode_add_tipi_device"),
            without_header(GOLDEN_ADD_TIPI_DEVICE)
        );
    }

    #[test]
    fn golden_encode_hijack_reversed() {
        assert_eq!(
            encode_hijack_reversed(GOLDEN_TARGET_MAC).expect("encode_hijack_reversed"),
            without_header(GOLDEN_HIJACK_REVERSED)
        );
    }

    #[test]
    fn parse_mac_accepts_six_hex_octets_in_either_case() {
        let want = [0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF];
        assert_eq!(parse_mac("AA:BB:CC:DD:EE:FF"), Some(want));
        assert_eq!(parse_mac("aa:bb:cc:dd:ee:ff"), Some(want));
        assert_eq!(
            parse_mac("00:01:0a:1F:a0:9e"),
            Some([0, 1, 0x0A, 0x1F, 0xA0, 0x9E])
        );
    }

    #[test]
    fn parse_mac_rejects_malformed_addresses() {
        for bad in [
            "",
            "AA:BB",
            "ZZ:BB:CC:DD:EE:FF",
            "AA:BB:CC:DD:EE:FF:00",
            "AAA:BB:CC:DD:EE:FF",
            "AA::CC:DD:EE:FF",
            "+A:BB:CC:DD:EE:FF",
            "A:BB:CC:DD:EE:FF",
            "AA:BB:CC:DD:EE:",
            "AA-BB-CC-DD-EE-FF",
            " AA:BB:CC:DD:EE:FF",
            "0x:BB:CC:DD:EE:FF",
        ] {
            assert_eq!(parse_mac(bad), None, "{bad:?} must be rejected");
        }
    }

    // Every target-MAC encoder returns InvalidInput with the address in the
    // message, never a panic.
    fn assert_invalid_mac_error(result: std::io::Result<Vec<u8>>, bad: &str) {
        let err = result.expect_err("an invalid target MAC must be an error");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(
            err.to_string().contains(bad),
            "error should name the bad address: {err}"
        );
    }

    #[test]
    fn invalid_mac_encoders_return_invalid_input() {
        for bad in ["ZZ:BB:CC:DD:EE:FF", "AA:BB"] {
            assert_invalid_mac_error(encode_hijack_request(bad), bad);
            assert_invalid_mac_error(encode_smart_routing_show_ui(bad), bad);
            assert_invalid_mac_error(encode_hijack_reversed(bad), bad);
            assert_invalid_mac_error(encode_media_information(GOLDEN_SELF_MAC, bad, true), bad);
            assert_invalid_mac_error(
                encode_media_information_new_device(GOLDEN_SELF_MAC, bad),
                bad,
            );
            assert_invalid_mac_error(encode_media_information_call(GOLDEN_SELF_MAC, bad), bad);
            assert_invalid_mac_error(encode_add_tipi_device(GOLDEN_SELF_MAC, bad), bad);
        }
    }

    // The send_* methods fail before they take the state lock or touch the
    // channel, so nothing is queued.
    #[tokio::test]
    async fn invalid_mac_send_methods_return_invalid_input_and_send_nothing() {
        let (manager, mut rx) = manager_with_send_channel().await;
        let bad = "ZZ:BB:CC:DD:EE:FF";
        let errors = vec![
            manager.send_hijack_request(bad).await,
            manager.send_smart_routing_show_ui(bad).await,
            manager.send_hijack_reversed(bad).await,
            manager
                .send_media_information(GOLDEN_SELF_MAC, bad, true)
                .await,
            manager
                .send_media_information_new_device(GOLDEN_SELF_MAC, bad)
                .await,
            manager
                .send_media_information_call(GOLDEN_SELF_MAC, bad)
                .await,
            manager.send_add_tipi_device(GOLDEN_SELF_MAC, bad).await,
        ];
        for result in errors {
            let err = result.expect_err("an invalid target MAC must be an error");
            assert!(
                matches!(
                    err.kind,
                    bluer::ErrorKind::Internal(bluer::InternalErrorKind::Io(
                        std::io::ErrorKind::InvalidInput
                    ))
                ),
                "unexpected error kind: {:?}",
                err.kind
            );
            assert!(err.message.contains(bad), "message: {}", err.message);
        }
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    fn opack_uint(n: u32) -> Vec<u8> {
        let mut out = Vec::new();
        opack_write_uint(&mut out, n);
        out
    }

    fn opack_str(s: &str) -> Vec<u8> {
        let mut out = Vec::new();
        opack_write_str(&mut out, s);
        out
    }

    #[test]
    fn opack_value_writer_boundaries() {
        // Ints: 0x08+n up to 39, then signed 0x30 <i8>, 0x31 <i16 LE>,
        // 0x32 <i32 LE>, 0x33 <i64 LE>.
        for (n, bytes) in OPACK_INT_CASES {
            assert_eq!(opack_uint(*n), *bytes, "{n}");
        }
        assert_eq!(opack_uint(46), [0x30, 0x2E]);
        assert_eq!(opack_uint(256), [0x31, 0x00, 0x01]);
        assert_eq!(opack_uint(65536), [0x32, 0x00, 0x00, 0x01, 0x00]);
        assert_eq!(opack_uint(952696833), [0x32, 0x01, 0x00, 0xC9, 0x38]);

        // Strings: 0x40+n up to 32 bytes, then 0x61 <u8 n> up to 255.
        assert_eq!(opack_str(""), [0x40]);
        let s32 = "a".repeat(32);
        let enc32 = opack_str(&s32);
        assert_eq!(enc32[0], 0x60);
        assert_eq!(&enc32[1..], s32.as_bytes());
        let s33 = "b".repeat(33);
        let enc33 = opack_str(&s33);
        assert_eq!(enc33[..2], [0x61, 0x21]);
        assert_eq!(&enc33[2..], s33.as_bytes());
        let s255 = "c".repeat(255);
        assert_eq!(opack_str(&s255)[..2], [0x61, 0xFF]);

        // true
        let mut t = Vec::new();
        opack_write_true(&mut t);
        assert_eq!(t, [0x01]);
    }

    // Each signed-width boundary and Apple's categories, with their bytes.
    const OPACK_INT_CASES: &[(u32, &[u8])] = &[
        (0, &[0x08]),
        (39, &[0x2F]),
        (40, &[0x30, 0x28]),
        (127, &[0x30, 0x7F]),
        (128, &[0x31, 0x80, 0x00]),
        (201, &[0x31, 0xC9, 0x00]),
        (255, &[0x31, 0xFF, 0x00]),
        (301, &[0x31, 0x2D, 0x01]),
        (501, &[0x31, 0xF5, 0x01]),
        (32767, &[0x31, 0xFF, 0x7F]),
        (32768, &[0x32, 0x00, 0x80, 0x00, 0x00]),
        (65535, &[0x32, 0xFF, 0xFF, 0x00, 0x00]),
        (2147483647, &[0x32, 0xFF, 0xFF, 0xFF, 0x7F]),
        (
            2147483648,
            &[0x33, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00],
        ),
        (
            4294967295,
            &[0x33, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00],
        ),
    ];

    #[test]
    fn opack_read_int_round_trip() {
        for (n, bytes) in OPACK_INT_CASES {
            let enc = opack_uint(*n);
            assert!(
                matches!(opack_read(&enc, 0), Some((Opack::Uint(m), end)) if m == *n && end == bytes.len()),
                "{n}"
            );
            assert_eq!(opack_walk(&enc, 0), Ok(enc.len()), "{n}");
        }
    }

    #[test]
    fn opack_read_signed_ints() {
        let uint = |bytes: &[u8]| match opack_read(bytes, 0) {
            Some((Opack::Uint(n), end)) => Some((Some(n), end)),
            Some((_, end)) => Some((None, end)),
            None => None,
        };
        assert_eq!(uint(&[0x31, 0xC9, 0x00]), Some((Some(201), 3)));
        assert_eq!(uint(&[0x30, 0x7F]), Some((Some(127), 2)));
        // Negative values are not categories: -55, -1 (i32) and -1 (i64).
        assert_eq!(uint(&[0x30, 0xC9]), Some((None, 2)));
        assert_eq!(uint(&[0x32, 0xFF, 0xFF, 0xFF, 0xFF]), Some((None, 5)));
        assert_eq!(
            uint(&[0x33, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),
            Some((None, 9))
        );
        // Above u32::MAX: valid OPACK, not a category.
        assert_eq!(uint(&[0x33, 0, 0, 0, 0, 1, 0, 0, 0]), Some((None, 9)));
        // Truncated ints.
        assert_eq!(uint(&[0x31, 0xC9]), None);
        assert_eq!(uint(&[0x33, 0, 0, 0, 0, 0, 0, 0]), None);
    }

    #[test]
    fn opack_value_writer_long_strings() {
        // 0x62 <u16 LE len> from 256 bytes, 0x63 <u24 LE len> from 65536.
        let s256 = "a".repeat(256);
        let enc256 = opack_str(&s256);
        assert_eq!(enc256[..3], [0x62, 0x00, 0x01]);
        assert_eq!(&enc256[3..], s256.as_bytes());

        let s65535 = "b".repeat(65535);
        let enc65535 = opack_str(&s65535);
        assert_eq!(enc65535[..3], [0x62, 0xFF, 0xFF]);
        assert_eq!(enc65535.len(), 3 + 65535);

        let s65536 = "c".repeat(65536);
        let enc65536 = opack_str(&s65536);
        assert_eq!(enc65536[..4], [0x63, 0x00, 0x00, 0x01]);
        assert_eq!(&enc65536[4..], s65536.as_bytes());

        // The walker reads each long form back to its exact end.
        assert_eq!(opack_walk(&enc256, 0), Ok(enc256.len()));
        assert_eq!(opack_walk(&enc65535, 0), Ok(enc65535.len()));
        assert_eq!(opack_walk(&enc65536, 0), Ok(enc65536.len()));
    }

    #[test]
    fn opack_write_ref_boundaries() {
        let mut out = Vec::new();
        opack_write_ref(&mut out, 0);
        assert_eq!(out, [0xA0]);

        let mut out = Vec::new();
        opack_write_ref(&mut out, 5);
        assert_eq!(out, [0xA5]);

        let mut out = Vec::new();
        opack_write_ref(&mut out, 31);
        assert_eq!(out, [0xBF]);
    }

    // Apple's relay payloads from #35 (mo's "Decision (mo, 2026-11-07):
    // option (b)" comment): audioaccessoryd / BTSmartRoutingDaemon hex dumps
    // of the OPACK dicts the iPhone sent through the AirPods, copied verbatim.
    // The iPhone's address is replaced by 00:00:00:00:00:00 (same length).
    const APPLE_BT_ADDRESS: &str = "00:00:00:00:00:00";
    const APPLE_BT_NAME: &str = "iPhone";

    const APPLE_MEDIA_INFORMATION_UNKNOWN_NO_100: &str = "e5 4a 70 6c 61 79 69 6e 67 41 70 70 47 55 6e 6b 6e 6f 77 6e 52 68 6f 73 74 53 74 72 65 61 6d 69 6e 67 53 74 61 74 65 42 4e 4f 49 62 74 41 64 64 72 65 73 73 51 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 46 62 74 4e 61 6d 65 46 69 50 68 6f 6e 65 58 6f 74 68 65 72 44 65 76 69 63 65 41 75 64 69 6f 43 61 74 65 67 6f 72 79 30 64";
    const APPLE_MEDIA_INFORMATION_UNKNOWN_YES_100: &str = "e5 4a 70 6c 61 79 69 6e 67 41 70 70 47 55 6e 6b 6e 6f 77 6e 52 68 6f 73 74 53 74 72 65 61 6d 69 6e 67 53 74 61 74 65 43 59 45 53 49 62 74 41 64 64 72 65 73 73 51 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 46 62 74 4e 61 6d 65 46 69 50 68 6f 6e 65 58 6f 74 68 65 72 44 65 76 69 63 65 41 75 64 69 6f 43 61 74 65 67 6f 72 79 30 64";
    const APPLE_MEDIA_INFORMATION_INSTAGRAM_NO_301: &str = "e5 4a 70 6c 61 79 69 6e 67 41 70 70 53 63 6f 6d 2e 62 75 72 62 6e 2e 69 6e 73 74 61 67 72 61 6d 52 68 6f 73 74 53 74 72 65 61 6d 69 6e 67 53 74 61 74 65 42 4e 4f 49 62 74 41 64 64 72 65 73 73 51 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 46 62 74 4e 61 6d 65 46 69 50 68 6f 6e 65 58 6f 74 68 65 72 44 65 76 69 63 65 41 75 64 69 6f 43 61 74 65 67 6f 72 79 31 2d 01";
    const APPLE_MEDIA_INFORMATION_INSTAGRAM_YES_301: &str = "e5 4a 70 6c 61 79 69 6e 67 41 70 70 53 63 6f 6d 2e 62 75 72 62 6e 2e 69 6e 73 74 61 67 72 61 6d 52 68 6f 73 74 53 74 72 65 61 6d 69 6e 67 53 74 61 74 65 43 59 45 53 49 62 74 41 64 64 72 65 73 73 51 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 46 62 74 4e 61 6d 65 46 69 50 68 6f 6e 65 58 6f 74 68 65 72 44 65 76 69 63 65 41 75 64 69 6f 43 61 74 65 67 6f 72 79 31 2d 01";
    const APPLE_MEDIA_INFORMATION_SPOTIFY_YES_301: &str = "e5 4a 70 6c 61 79 69 6e 67 41 70 70 52 63 6f 6d 2e 73 70 6f 74 69 66 79 2e 63 6c 69 65 6e 74 52 68 6f 73 74 53 74 72 65 61 6d 69 6e 67 53 74 61 74 65 43 59 45 53 49 62 74 41 64 64 72 65 73 73 51 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 46 62 74 4e 61 6d 65 46 69 50 68 6f 6e 65 58 6f 74 68 65 72 44 65 76 69 63 65 41 75 64 69 6f 43 61 74 65 67 6f 72 79 31 2d 01";
    // mo's iPhone phone-call capture from #74 (2026-11-07 23:44:57.833): the
    // media information the iPhone relayed during a cellular call, with the
    // iPhone's address zeroed. 145 bytes; playingApp is 38 bytes (`61 26`, the
    // OPACK long-string tag) and the category is 501 (`31 f5 01`).
    const APPLE_MEDIA_INFORMATION_CALL_YES_501: &str = "e5 4a 70 6c 61 79 69 6e 67 41 70 70 61 26 63 6f 6d 2e 61 70 70 6c 65 2e 69 70 54 65 6c 65 70 68 6f 6e 79 2e 43 61 6c 6c 41 75 64 69 6f 53 65 72 76 69 63 65 52 68 6f 73 74 53 74 72 65 61 6d 69 6e 67 53 74 61 74 65 43 59 45 53 49 62 74 41 64 64 72 65 73 73 51 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 46 62 74 4e 61 6d 65 46 69 50 68 6f 6e 65 58 6f 74 68 65 72 44 65 76 69 63 65 41 75 64 69 6f 43 61 74 65 67 6f 72 79 31 f5 01";

    const APPLE_TIPI_IDLE0_NEW_SCORE2: &str = "e5 48 69 64 6c 65 54 69 6d 65 08 47 6e 65 77 54 69 70 69 01 49 62 74 41 64 64 72 65 73 73 51 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 46 62 74 4e 61 6d 65 46 69 50 68 6f 6e 65 50 6e 65 61 72 62 79 41 75 64 69 6f 53 63 6f 72 65 0a";
    const APPLE_TIPI_IDLE0_NEW_SCORE6: &str = "e5 48 69 64 6c 65 54 69 6d 65 08 47 6e 65 77 54 69 70 69 01 49 62 74 41 64 64 72 65 73 73 51 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 46 62 74 4e 61 6d 65 46 69 50 68 6f 6e 65 50 6e 65 61 72 62 79 41 75 64 69 6f 53 63 6f 72 65 0e";
    const APPLE_TIPI_IDLE13_NEW_SCORE1: &str = "e5 48 69 64 6c 65 54 69 6d 65 15 47 6e 65 77 54 69 70 69 01 49 62 74 41 64 64 72 65 73 73 51 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 46 62 74 4e 61 6d 65 46 69 50 68 6f 6e 65 50 6e 65 61 72 62 79 41 75 64 69 6f 53 63 6f 72 65 09";
    const APPLE_TIPI_IDLE46_NEW_SCORE1: &str = "e5 48 69 64 6c 65 54 69 6d 65 30 2e 47 6e 65 77 54 69 70 69 01 49 62 74 41 64 64 72 65 73 73 51 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 46 62 74 4e 61 6d 65 46 69 50 68 6f 6e 65 50 6e 65 61 72 62 79 41 75 64 69 6f 53 63 6f 72 65 09";
    const APPLE_TIPI_IDLE0_SCORE1: &str = "e4 48 69 64 6c 65 54 69 6d 65 08 49 62 74 41 64 64 72 65 73 73 51 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 46 62 74 4e 61 6d 65 46 69 50 68 6f 6e 65 50 6e 65 61 72 62 79 41 75 64 69 6f 53 63 6f 72 65 09";
    const APPLE_TIPI_IDLE0_SCORE2: &str = "e4 48 69 64 6c 65 54 69 6d 65 08 49 62 74 41 64 64 72 65 73 73 51 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 46 62 74 4e 61 6d 65 46 69 50 68 6f 6e 65 50 6e 65 61 72 62 79 41 75 64 69 6f 53 63 6f 72 65 0a";
    const APPLE_TIPI_IDLE3_SCORE7: &str = "e4 48 69 64 6c 65 54 69 6d 65 0b 49 62 74 41 64 64 72 65 73 73 51 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 3a 30 30 46 62 74 4e 61 6d 65 46 69 50 68 6f 6e 65 50 6e 65 61 72 62 79 41 75 64 69 6f 53 63 6f 72 65 0f";

    // Apple payload 5: the iPhone's Hijackv2 request. 0xa5 is an OPACK
    // back-reference to object #5 (301), so remotescore = 301.
    const APPLE_HIJACK_REQUEST: &str = "e5 4a 6c 6f 63 61 6c 73 63 6f 72 65 30 64 46 72 65 61 73 6f 6e 48 48 69 6a 61 63 6b 76 32 51 61 75 64 69 6f 52 6f 75 74 69 6e 67 53 63 6f 72 65 31 2d 01 5f 61 75 64 69 6f 52 6f 75 74 69 6e 67 53 65 74 4f 77 6e 65 72 73 68 69 70 54 6f 46 61 6c 73 65 01 4b 72 65 6d 6f 74 65 73 63 6f 72 65 a5";
    // Captured messages Linux does not send (#44); kept to exercise the walker.
    const APPLE_ROUTED_TO_BLUETOOTH: &str = "e2 5f 61 75 64 69 6f 52 6f 75 74 69 6e 67 53 65 74 4f 77 6e 65 72 73 68 69 70 54 6f 46 61 6c 73 65 01 46 72 65 61 73 6f 6e 51 52 6f 75 74 65 64 54 6f 42 6c 75 65 74 6f 6f 74 68";
    const APPLE_AUDIO_ROUTING_HIJACK_REQUEST: &str = "e3 59 61 75 64 69 6f 52 6f 75 74 69 6e 67 48 69 6a 61 63 6b 52 65 71 75 65 73 74 01 51 61 75 64 69 6f 52 6f 75 74 69 6e 67 53 63 6f 72 65 31 2d 01 55 61 75 64 69 6f 52 6f 75 74 69 6e 67 52 65 71 75 65 73 74 49 44 32 01 00 c9 38";

    // Parses a space-separated hex dump as printed in #35.
    fn capture_bytes(dump: &str) -> Vec<u8> {
        dump.split_whitespace()
            .map(|b| u8::from_str_radix(b, 16))
            .collect::<std::result::Result<Vec<u8>, _>>()
            .expect("hex dump")
    }

    fn assert_media_information_matches_apple(
        playing_app: &str,
        host_streaming: bool,
        audio_category: u32,
        capture: &str,
    ) {
        assert_eq!(
            media_information_body(
                playing_app,
                host_streaming,
                APPLE_BT_ADDRESS,
                APPLE_BT_NAME,
                audio_category,
            ),
            capture_bytes(capture)
        );
    }

    #[test]
    fn golden_apple_media_information_unknown_not_streaming_100() {
        assert_media_information_matches_apple(
            "Unknown",
            false,
            100,
            APPLE_MEDIA_INFORMATION_UNKNOWN_NO_100,
        );
    }

    #[test]
    fn golden_apple_media_information_unknown_streaming_100() {
        assert_media_information_matches_apple(
            "Unknown",
            true,
            100,
            APPLE_MEDIA_INFORMATION_UNKNOWN_YES_100,
        );
    }

    #[test]
    fn golden_apple_media_information_instagram_not_streaming_301() {
        assert_media_information_matches_apple(
            "com.burbn.instagram",
            false,
            301,
            APPLE_MEDIA_INFORMATION_INSTAGRAM_NO_301,
        );
    }

    #[test]
    fn golden_apple_media_information_instagram_streaming_301() {
        assert_media_information_matches_apple(
            "com.burbn.instagram",
            true,
            301,
            APPLE_MEDIA_INFORMATION_INSTAGRAM_YES_301,
        );
    }

    #[test]
    fn golden_apple_media_information_spotify_streaming_301() {
        assert_media_information_matches_apple(
            "com.spotify.client",
            true,
            301,
            APPLE_MEDIA_INFORMATION_SPOTIFY_YES_301,
        );
    }

    #[test]
    fn golden_apple_media_information_call_streaming_501() {
        assert_media_information_matches_apple(
            "com.apple.ipTelephony.CallAudioService",
            true,
            AUDIO_CATEGORY_CALL,
            APPLE_MEDIA_INFORMATION_CALL_YES_501,
        );
        assert_eq!(
            capture_bytes(APPLE_MEDIA_INFORMATION_CALL_YES_501).len(),
            145
        );
    }

    fn assert_tipi_matches_apple(idle_time: u32, new_tipi: bool, score: u32, capture: &str) {
        assert_eq!(
            tipi_body(idle_time, new_tipi, APPLE_BT_ADDRESS, APPLE_BT_NAME, score),
            capture_bytes(capture)
        );
    }

    #[test]
    fn golden_apple_tipi_idle0_new_tipi_score2() {
        assert_tipi_matches_apple(0, true, 2, APPLE_TIPI_IDLE0_NEW_SCORE2);
    }

    #[test]
    fn golden_apple_tipi_idle0_new_tipi_score6() {
        assert_tipi_matches_apple(0, true, 6, APPLE_TIPI_IDLE0_NEW_SCORE6);
    }

    #[test]
    fn golden_apple_tipi_idle13_new_tipi_score1() {
        assert_tipi_matches_apple(13, true, 1, APPLE_TIPI_IDLE13_NEW_SCORE1);
    }

    #[test]
    fn golden_apple_tipi_idle46_new_tipi_score1() {
        assert_tipi_matches_apple(46, true, 1, APPLE_TIPI_IDLE46_NEW_SCORE1);
    }

    #[test]
    fn golden_apple_tipi_idle0_score1() {
        assert_tipi_matches_apple(0, false, 1, APPLE_TIPI_IDLE0_SCORE1);
    }

    #[test]
    fn golden_apple_tipi_idle0_score2() {
        assert_tipi_matches_apple(0, false, 2, APPLE_TIPI_IDLE0_SCORE2);
    }

    #[test]
    fn golden_apple_tipi_idle3_score7() {
        assert_tipi_matches_apple(3, false, 7, APPLE_TIPI_IDLE3_SCORE7);
    }

    #[test]
    fn golden_apple_hijack_request() {
        // Skip opcode (2), reversed MAC (6), length (2) and the 0x01 byte.
        let packet = encode_hijack_request(GOLDEN_TARGET_MAC).expect("encode_hijack_request");
        let apple = capture_bytes(APPLE_HIJACK_REQUEST);
        assert_eq!(apple.len(), 97);
        assert_eq!(packet[11..], apple[..]);
    }

    // `Result` in this module is bluer's; the walker reports plain strings.
    type WalkResult<T> = std::result::Result<T, String>;

    // Test-only OPACK walker. Returns the offset just past the object that
    // starts at `pos`, or an error for an unknown tag or a truncated value.
    // Handles bools 0x01/0x02, ints 0x08-0x2F and 0x30-0x33, strings
    // 0x40-0x60, 0x61 <u8 len>, 0x62 <u16 len>, 0x63 <u24 len> and 0x64 <u32
    // len>, back-references 0xA0-0xBF and dicts
    // 0xE0-0xEE (0xEF: until 0x03). Dict keys must be strings.
    fn opack_walk(buf: &[u8], pos: usize) -> WalkResult<usize> {
        let tag = *buf
            .get(pos)
            .ok_or_else(|| format!("truncated: no tag at {pos}"))?;
        let take = |start: usize, n: usize| -> WalkResult<usize> {
            if start + n > buf.len() {
                Err(format!(
                    "tag {tag:#04x} at {pos} needs {n} bytes at {start}, {} left",
                    buf.len().saturating_sub(start)
                ))
            } else {
                Ok(start + n)
            }
        };
        let string = |start: usize, n: usize| -> WalkResult<usize> {
            let end = take(start, n)?;
            std::str::from_utf8(&buf[start..end])
                .map_err(|e| format!("string at {pos} is not UTF-8: {e}"))?;
            Ok(end)
        };
        let dict_key = |p: usize| -> WalkResult<usize> {
            match buf.get(p) {
                Some(0x40..=0x64) => opack_walk(buf, p),
                Some(t) => Err(format!("dict key at {p} has non-string tag {t:#04x}")),
                None => Err(format!("truncated: no dict key at {p}")),
            }
        };
        match tag {
            0x01 | 0x02 => Ok(pos + 1),
            0x08..=0x2F => Ok(pos + 1),
            0x30 => take(pos + 1, 1),
            0x31 => take(pos + 1, 2),
            0x32 => take(pos + 1, 4),
            0x33 => take(pos + 1, 8),
            0x40..=0x60 => string(pos + 1, (tag - 0x40) as usize),
            0x61 => {
                let n = *buf
                    .get(pos + 1)
                    .ok_or_else(|| format!("truncated: no string length at {}", pos + 1))?;
                string(pos + 2, n as usize)
            }
            0x62..=0x64 => {
                // Little-endian length of 2, 3 or 4 bytes.
                let width = (tag - 0x60) as usize;
                let end = take(pos + 1, width)?;
                let n = buf[pos + 1..end]
                    .iter()
                    .enumerate()
                    .fold(0usize, |acc, (i, b)| acc | ((*b as usize) << (8 * i)));
                string(end, n)
            }
            0xA0..=0xBF => Ok(pos + 1),
            0xE0..=0xEE => {
                let mut p = pos + 1;
                for _ in 0..(tag - 0xE0) {
                    p = dict_key(p)?;
                    p = opack_walk(buf, p)?;
                }
                Ok(p)
            }
            0xEF => {
                let mut p = pos + 1;
                while buf.get(p) != Some(&0x03) {
                    p = dict_key(p)?;
                    p = opack_walk(buf, p)?;
                }
                Ok(p + 1)
            }
            _ => Err(format!("unsupported OPACK tag {tag:#04x} at {pos}")),
        }
    }

    // A smart-routing data packet (as the encoders return it) is framed as
    // opcode (2), reversed MAC (6), u16 LE length of the rest, 0x01, then
    // exactly one OPACK dict with nothing after it.
    fn check_smart_routing_frame(packet: &[u8]) -> WalkResult<()> {
        if packet.len() < 12 {
            return Err(format!("packet too short: {} bytes", packet.len()));
        }
        if packet[..2] != [opcodes::SMART_ROUTING, 0x00] {
            return Err(format!("not a smart-routing opcode: {:02x?}", &packet[..2]));
        }
        let length = u16::from_le_bytes([packet[8], packet[9]]) as usize;
        if length != packet.len() - 10 {
            return Err(format!(
                "length field {length} != {} bytes after it",
                packet.len() - 10
            ));
        }
        if packet[10] != 0x01 {
            return Err(format!("byte 10 is {:#04x}, expected 0x01", packet[10]));
        }
        if !(0xE0..=0xEF).contains(&packet[11]) {
            return Err(format!("byte 11 is {:#04x}, not a dict", packet[11]));
        }
        let end = opack_walk(packet, 11)?;
        if end != packet.len() {
            return Err(format!(
                "{} trailing bytes after the dict",
                packet.len() - end
            ));
        }
        Ok(())
    }

    #[test]
    fn opack_framing_all_smart_routing_encoders() {
        let packets = [
            (
                "hijack_request",
                encode_hijack_request(GOLDEN_TARGET_MAC).expect("hijack_request"),
            ),
            (
                "smart_routing_show_ui",
                encode_smart_routing_show_ui(GOLDEN_TARGET_MAC).expect("smart_routing_show_ui"),
            ),
            (
                "media_information streaming",
                encode_media_information(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC, true)
                    .expect("media_information streaming"),
            ),
            (
                "media_information not streaming",
                encode_media_information(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC, false)
                    .expect("media_information not streaming"),
            ),
            (
                "media_information_new_device",
                encode_media_information_new_device(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC)
                    .expect("media_information_new_device"),
            ),
            (
                "media_information_call",
                encode_media_information_call(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC)
                    .expect("media_information_call"),
            ),
            (
                "media_information_category",
                encode_media_information_category(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC, 201)
                    .expect("media_information_category"),
            ),
            (
                "add_tipi_device",
                encode_add_tipi_device(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC)
                    .expect("add_tipi_device"),
            ),
            (
                "hijack_reversed",
                encode_hijack_reversed(GOLDEN_TARGET_MAC).expect("hijack_reversed"),
            ),
        ];
        for (name, packet) in &packets {
            if let Err(e) = check_smart_routing_frame(packet) {
                panic!("{name}: {e}");
            }
        }
    }

    #[test]
    fn smart_routing_packet_rejects_oversized_dict_without_panicking() {
        // 1 + 65535 does not fit the u16 length field.
        let err = smart_routing_packet(GOLDEN_TARGET_MAC, &vec![0u8; 65535])
            .expect_err("an oversized dict must be an error");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn smart_routing_packet_largest_dict_frames_correctly() {
        // 1 + 65534 == 0xFFFF is the largest length the field can carry.
        let packet = smart_routing_packet(GOLDEN_TARGET_MAC, &vec![0u8; 65534])
            .expect("the largest dict that fits must frame");
        assert_eq!(packet.len(), 11 + 65534);
        assert_eq!(packet[..2], [opcodes::SMART_ROUTING, 0x00]);
        assert_eq!(packet[8..10], [0xFF, 0xFF]);
        assert_eq!(packet[10], 0x01);
    }

    #[test]
    fn opack_walker_accepts_every_apple_capture() {
        for capture in [
            APPLE_MEDIA_INFORMATION_UNKNOWN_NO_100,
            APPLE_MEDIA_INFORMATION_UNKNOWN_YES_100,
            APPLE_MEDIA_INFORMATION_INSTAGRAM_NO_301,
            APPLE_MEDIA_INFORMATION_INSTAGRAM_YES_301,
            APPLE_MEDIA_INFORMATION_SPOTIFY_YES_301,
            APPLE_MEDIA_INFORMATION_CALL_YES_501,
            APPLE_TIPI_IDLE0_NEW_SCORE2,
            APPLE_TIPI_IDLE0_NEW_SCORE6,
            APPLE_TIPI_IDLE13_NEW_SCORE1,
            APPLE_TIPI_IDLE46_NEW_SCORE1,
            APPLE_TIPI_IDLE0_SCORE1,
            APPLE_TIPI_IDLE0_SCORE2,
            APPLE_TIPI_IDLE3_SCORE7,
            APPLE_HIJACK_REQUEST,
            APPLE_ROUTED_TO_BLUETOOTH,
            APPLE_AUDIO_ROUTING_HIJACK_REQUEST,
        ] {
            let bytes = capture_bytes(capture);
            assert_eq!(opack_walk(&bytes, 0), Ok(bytes.len()), "{capture}");
        }
    }

    // What linux-parity 345d400 sent before #35 (encoder output, no AACP
    // header), for the walker's negative cases.
    const LINUX_PARITY_MEDIA_INFORMATION_STREAMING: &str = "10 00 ff ee dd cc bb aa 82 00 01 e5 4a 50 6c 61 79 69 6e 67 41 70 70 56 63 6f 6d 2e 67 6f 6f 67 6c 65 2e 69 6f 73 2e 79 6f 75 74 75 62 65 52 48 6f 73 74 53 74 72 65 61 6d 69 6e 67 53 74 61 74 65 42 59 45 53 49 62 74 41 64 64 72 65 73 73 51 31 31 3a 32 32 3a 33 33 3a 34 34 3a 35 35 3a 36 36 62 74 4e 61 6d 65 43 4d 61 63 58 6f 74 68 65 72 44 65 76 69 63 65 41 75 64 69 6f 43 61 74 65 67 6f 72 79 31 2d 01 00 00 00 00 00";
    const LINUX_PARITY_ADD_TIPI_DEVICE: &str = "10 00 ff ee dd cc bb aa 4e 00 01 e5 48 69 69 64 6c 65 54 69 6d 65 08 47 6e 65 77 54 69 70 69 01 49 62 74 41 64 64 72 65 73 73 51 31 31 3a 32 32 3a 33 33 3a 34 34 3a 35 35 3a 36 36 46 62 74 4e 61 6d 65 43 4d 61 63 50 6e 65 61 72 62 79 41 75 64 69 6f 53 63 6f 72 65 0e";

    #[test]
    fn opack_framing_rejects_linux_parity_media_information() {
        // The full old packet: its length field matched its size, but the
        // dict does not walk (0x42 "YE", then a bare "btName").
        let old = capture_bytes(LINUX_PARITY_MEDIA_INFORMATION_STREAMING);
        assert_eq!(old.len(), 140);
        assert!(opack_walk(&old, 11).is_err());
        assert!(check_smart_routing_frame(&old).is_err());

        let good = encode_media_information(GOLDEN_SELF_MAC, GOLDEN_TARGET_MAC, true)
            .expect("encode_media_information");
        assert_eq!(check_smart_routing_frame(&good), Ok(()));

        // Only the bare btName: drop the 0x46 tag before "btName" (and fix the
        // length). The walker reads 0x62 as a key tag it does not accept.
        let tag = good
            .windows(7)
            .position(|w| w == b"\x46btName")
            .expect("btName key");
        let mut bare = good.clone();
        bare.remove(tag);
        let bare_length = (bare.len() - 10) as u16;
        bare[8..10].copy_from_slice(&bare_length.to_le_bytes());
        assert!(opack_walk(&bare, 11).is_err());
        assert!(check_smart_routing_frame(&bare).is_err());

        // Only the padding: the dict walks, but the zero bytes the length
        // counts trail it.
        let mut padded = good.clone();
        padded.extend_from_slice(&[0x00; 5]);
        let padded_length = (padded.len() - 10) as u16;
        padded[8..10].copy_from_slice(&padded_length.to_le_bytes());
        assert_eq!(opack_walk(&padded, 11), Ok(good.len()));
        assert!(check_smart_routing_frame(&padded).is_err());
    }

    #[test]
    fn opack_framing_rejects_linux_parity_add_tipi_device() {
        // The stray 0x69 made the key "iidleTim" and left a bare 0x65 ('e').
        let old = capture_bytes(LINUX_PARITY_ADD_TIPI_DEVICE);
        assert!(opack_walk(&old, 11).is_err());
        assert!(check_smart_routing_frame(&old).is_err());
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
    fn seen_unhandled_opcodes_are_known_ignored() {
        for op in [0x02, 0x2B, 0x4C] {
            assert!(is_known_ignored_opcode(op), "{op:#04x}");
        }
        for op in [0x00, 0x7F, 0xFF, opcodes::BATTERY_INFO, opcodes::EQ_DATA] {
            assert!(!is_known_ignored_opcode(op), "{op:#04x}");
        }
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

    const RELAY_SENDER: &str = "A1:B2:C3:D4:E5:F6";

    /// `dict` as a 0x11 payload from `RELAY_SENDER`: the smart-routing
    /// header with the response opcode.
    fn media_information_payload(dict: &[u8]) -> Vec<u8> {
        let mut payload = smart_routing_packet(RELAY_SENDER, dict).expect("frame");
        payload[0] = opcodes::SMART_ROUTING_RESP;
        payload
    }

    #[test]
    fn parse_media_information_apple_captures() {
        for (capture, category) in [
            (APPLE_MEDIA_INFORMATION_UNKNOWN_NO_100, 100),
            (APPLE_MEDIA_INFORMATION_UNKNOWN_YES_100, 100),
            (APPLE_MEDIA_INFORMATION_INSTAGRAM_NO_301, 301),
            (APPLE_MEDIA_INFORMATION_SPOTIFY_YES_301, 301),
            (APPLE_MEDIA_INFORMATION_CALL_YES_501, 501),
        ] {
            let info = parse_media_information(&media_information_payload(&capture_bytes(capture)))
                .expect("a dict");
            assert_eq!(
                info,
                MediaInformation {
                    sender: RELAY_SENDER.to_string(),
                    bt_address: Some(APPLE_BT_ADDRESS.to_string()),
                    category: Some(category),
                },
                "{capture}"
            );
            // The captures' btAddress is all-zero: the host is the sender.
            assert_eq!(info.host(), RELAY_SENDER);
        }
    }

    #[test]
    fn parse_media_information_linux_201_names_its_bt_address() {
        let dict = media_information_body("Unknown", false, "aa:bb:cc:dd:ee:ff", "Mac", 201);
        let info = parse_media_information(&media_information_payload(&dict)).expect("a dict");
        assert_eq!(info.bt_address.as_deref(), Some("AA:BB:CC:DD:EE:FF"));
        assert_eq!(info.category, Some(201));
        assert_eq!(info.host(), "AA:BB:CC:DD:EE:FF");
        // An all-zero btAddress falls back to the header's sender.
        let dict = media_information_body("Unknown", false, APPLE_BT_ADDRESS, "Mac", 201);
        let info = parse_media_information(&media_information_payload(&dict)).expect("a dict");
        assert_eq!(info.host(), RELAY_SENDER);
    }

    #[test]
    fn parse_media_information_truncation_sweep() {
        let payload =
            media_information_payload(&capture_bytes(APPLE_MEDIA_INFORMATION_INSTAGRAM_YES_301));
        for n in 0..payload.len() {
            let info = parse_media_information(&payload[..n]);
            // The category is the dict's last value: any cut loses it.
            assert_eq!(info.and_then(|i| i.category), None, "n = {n}");
        }
        assert_eq!(
            parse_media_information(&payload).and_then(|i| i.category),
            Some(301)
        );
    }

    #[test]
    fn parse_media_information_unknown_tags_and_references() {
        // An unknown tag (0x70) before the category ends the walk.
        let mut dict = vec![0xE2];
        opack_write_str(&mut dict, "mystery");
        dict.push(0x70);
        opack_write_str(&mut dict, "otherDeviceAudioCategory");
        opack_write_uint(&mut dict, 301);
        let info = parse_media_information(&media_information_payload(&dict)).expect("a dict");
        assert_eq!(info.category, None);
        // A back-reference is skipped, and gives no category itself.
        let mut dict = vec![0xE2];
        opack_write_str(&mut dict, "remotescore");
        opack_write_ref(&mut dict, 5);
        opack_write_str(&mut dict, "otherDeviceAudioCategory");
        opack_write_ref(&mut dict, 5);
        let info = parse_media_information(&media_information_payload(&dict)).expect("a dict");
        assert_eq!(info.category, None);
        // A terminated dict (0xEF ... 0x03) reads like a counted one.
        let mut dict = capture_bytes(APPLE_MEDIA_INFORMATION_UNKNOWN_NO_100);
        dict[0] = 0xEF;
        dict.push(0x03);
        let info = parse_media_information(&media_information_payload(&dict)).expect("a dict");
        assert_eq!(info.category, Some(100));
        // No dict: the SetOwnershipToFalse text payload.
        assert_eq!(parse_media_information(SMART_ROUTING_PAYLOAD), None);
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

    const AIRPODS_MAC: &str = "AA:AA:AA:AA:AA:AA";
    const OTHER_MAC: &str = "BB:BB:BB:BB:BB:BB";

    fn airpods_info(name: &str) -> AirPodsInformation {
        let s = |v: &str| v.to_string();
        AirPodsInformation {
            name: s(name),
            model_number: s("A2084"),
            manufacturer: s("Apple Inc."),
            serial_number: s("SN"),
            version1: s("1"),
            version2: s("2"),
            hardware_revision: s("1.0.0"),
            updater_identifier: s("com.apple.accessory.updater"),
            left_serial_number: s("L"),
            right_serial_number: s("R"),
            version3: s("3"),
            le_keys: AirPodsLEKeys {
                irk: String::new(),
                enc_key: String::new(),
            },
        }
    }

    /// A device saved after connect (absent from the snapshot), with information.
    fn other_device() -> DeviceData {
        DeviceData {
            name: "Ear (2)".to_string(),
            type_: DeviceType::Nothing,
            information: Some(DeviceInformation::Nothing(
                crate::devices::nothing::NothingInformation {
                    serial_number: "SN1".to_string(),
                    firmware_version: "1.0.0".to_string(),
                },
            )),
        }
    }

    fn devices(entries: Vec<(&str, DeviceData)>) -> HashMap<String, DeviceData> {
        entries
            .into_iter()
            .map(|(mac, data)| (mac.to_string(), data))
            .collect()
    }

    fn assert_other_unchanged(devices: &HashMap<String, DeviceData>) {
        assert_eq!(
            serde_json::to_value(&devices[OTHER_MAC]).unwrap(),
            serde_json::to_value(other_device()).unwrap()
        );
    }

    #[test]
    fn information_keeps_other_devices() {
        let airpods = DeviceData {
            name: AIRPODS_MAC.to_string(),
            type_: DeviceType::AirPods,
            information: None,
        };
        let info = airpods_info("My AirPods");
        let updated = devices_with_information(
            devices(vec![(AIRPODS_MAC, airpods), (OTHER_MAC, other_device())]),
            AIRPODS_MAC,
            &info,
        );

        assert_eq!(updated.len(), 2);
        assert_other_unchanged(&updated);
        let airpods = &updated[AIRPODS_MAC];
        assert_eq!(airpods.name, "My AirPods");
        assert_eq!(
            serde_json::to_value(&airpods.information).unwrap(),
            serde_json::to_value(Some(DeviceInformation::AirPods(info))).unwrap()
        );
    }

    #[test]
    fn information_missing_entry_unchanged() {
        let updated = devices_with_information(
            devices(vec![(OTHER_MAC, other_device())]),
            AIRPODS_MAC,
            &airpods_info("My AirPods"),
        );

        assert_eq!(updated.len(), 1);
        assert_other_unchanged(&updated);
    }

    #[test]
    fn proximity_keys_keeps_other_devices() {
        let airpods = DeviceData {
            name: "My AirPods".to_string(),
            type_: DeviceType::AirPods,
            information: Some(DeviceInformation::AirPods(airpods_info("My AirPods"))),
        };
        let keys = [
            (0x01, vec![0x11, 0x22]),
            (0x04, vec![0x33]),
            (0x02, vec![0x44]),
        ];
        let updated = devices_with_proximity_keys(
            devices(vec![(AIRPODS_MAC, airpods), (OTHER_MAC, other_device())]),
            AIRPODS_MAC,
            &keys,
        );

        assert_eq!(updated.len(), 2);
        assert_other_unchanged(&updated);
        let Some(DeviceInformation::AirPods(info)) = &updated[AIRPODS_MAC].information else {
            panic!("AirPods information lost");
        };
        assert_eq!(info.le_keys.irk, "1122");
        assert_eq!(info.le_keys.enc_key, "33");
        assert_eq!(updated[AIRPODS_MAC].name, "My AirPods");
    }

    #[test]
    fn proximity_keys_inserts_entry() {
        // Only unknown key types: no entry is created.
        let updated = devices_with_proximity_keys(
            devices(vec![(OTHER_MAC, other_device())]),
            AIRPODS_MAC,
            &[(0x02, vec![0x44])],
        );
        assert_eq!(updated.len(), 1);

        let updated = devices_with_proximity_keys(updated, AIRPODS_MAC, &[(0x01, vec![0x11])]);

        assert_eq!(updated.len(), 2);
        assert_other_unchanged(&updated);
        let airpods = &updated[AIRPODS_MAC];
        assert_eq!(airpods.name, AIRPODS_MAC);
        assert!(matches!(airpods.type_, DeviceType::AirPods));
        let Some(DeviceInformation::AirPods(info)) = &airpods.information else {
            panic!("keys not stored");
        };
        assert_eq!(info.le_keys.irk, "11");
    }

    const IRK: [u8; 16] = [0xA1; 16];
    const ENC_KEY: [u8; 16] = [0xB2; 16];

    fn stored_keys(devices: &HashMap<String, DeviceData>) -> (String, String) {
        let Some(DeviceInformation::AirPods(info)) = &devices[AIRPODS_MAC].information else {
            panic!("AirPods information lost");
        };
        (info.le_keys.irk.clone(), info.le_keys.enc_key.clone())
    }

    #[test]
    fn information_keeps_stored_le_keys() {
        let mut stored = airpods_info("Old Name");
        stored.model_number = "A0000".to_string();
        stored.serial_number = "OLDSN".to_string();
        stored.version1 = "0".to_string();
        stored.le_keys.irk = hex::encode(IRK);
        stored.le_keys.enc_key = hex::encode(ENC_KEY);
        let airpods = DeviceData {
            name: "Old Name".to_string(),
            type_: DeviceType::AirPods,
            information: Some(DeviceInformation::AirPods(stored)),
        };
        let packet = airpods_info("My AirPods");

        let updated =
            devices_with_information(devices(vec![(AIRPODS_MAC, airpods)]), AIRPODS_MAC, &packet);

        assert_eq!(
            stored_keys(&updated),
            (hex::encode(IRK), hex::encode(ENC_KEY))
        );
        let Some(DeviceInformation::AirPods(info)) = &updated[AIRPODS_MAC].information else {
            panic!("AirPods information lost");
        };
        assert_eq!(updated[AIRPODS_MAC].name, packet.name);
        assert_eq!(info.name, packet.name);
        assert_eq!(info.model_number, packet.model_number);
        assert_eq!(info.serial_number, packet.serial_number);
        assert_eq!(info.version1, packet.version1);
    }

    #[test]
    fn proximity_keys_before_information_kept() {
        let keys = [(0x01, IRK.to_vec()), (0x04, ENC_KEY.to_vec())];
        let no_info = DeviceData {
            name: AIRPODS_MAC.to_string(),
            type_: DeviceType::AirPods,
            information: None,
        };
        let packet = airpods_info("My AirPods");
        // An entry without information, and a missing entry.
        for start in [devices(vec![(AIRPODS_MAC, no_info)]), devices(vec![])] {
            let updated = devices_with_proximity_keys(start, AIRPODS_MAC, &keys);
            assert_eq!(
                stored_keys(&updated),
                (hex::encode(IRK), hex::encode(ENC_KEY))
            );

            let updated = devices_with_information(updated, AIRPODS_MAC, &packet);

            assert_eq!(
                stored_keys(&updated),
                (hex::encode(IRK), hex::encode(ENC_KEY))
            );
            let mut expected = packet.clone();
            expected.le_keys.irk = hex::encode(IRK);
            expected.le_keys.enc_key = hex::encode(ENC_KEY);
            assert_eq!(
                serde_json::to_value(&updated[AIRPODS_MAC].information).unwrap(),
                serde_json::to_value(Some(DeviceInformation::AirPods(expected))).unwrap()
            );
            assert_eq!(updated[AIRPODS_MAC].name, packet.name);
        }

        // An entry with non-AirPods information is left unchanged.
        let updated = devices_with_proximity_keys(
            devices(vec![(OTHER_MAC, other_device())]),
            OTHER_MAC,
            &keys,
        );
        assert_other_unchanged(&updated);
    }

    #[test]
    fn proximity_keys_summary_has_no_key_bytes() {
        let summary = proximity_keys_summary(&[(0x01, IRK.to_vec()), (0x04, ENC_KEY.to_vec())]);

        assert!(summary.contains("Irk"), "{summary}");
        assert!(summary.contains("EncKey"), "{summary}");
        assert!(summary.contains("16"), "{summary}");
        for key in [IRK, ENC_KEY] {
            assert!(!summary.contains(&hex::encode(key)), "{summary}");
            assert!(!summary.contains(&hex::encode_upper(key)), "{summary}");
        }
    }

    /// A fresh directory under the system temp dir.
    fn devices_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "librepods-aacp-devices-{}-{name}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn update_devices_file_leaves_unparseable_file_unchanged() {
        let path = devices_dir("unparseable").join("devices.json");
        std::fs::write(&path, "{").unwrap();

        let written = update_devices_file(&path, |_| panic!("update must not run"));

        assert!(!written);
        assert_eq!(std::fs::read(&path).unwrap(), b"{");
    }

    #[test]
    fn update_devices_file_creates_missing_file_with_only_update() {
        let path = devices_dir("missing").join("devices.json");

        assert!(update_devices_file(&path, |d| {
            devices_with_proximity_keys(d, AIRPODS_MAC, &[(0x01, vec![0x11])])
        }));
        let written: HashMap<String, DeviceData> =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(written.keys().collect::<Vec<_>>(), [AIRPODS_MAC]);
    }

    // The handshake reply as captured in all 15 hw sessions (#286).
    fn handshake_reply() -> Vec<u8> {
        hex::decode("010004000000010003000000000000000000").unwrap()
    }

    async fn handshake_replied(manager: &AACPManager) -> bool {
        manager.state.lock().await.handshake_replied
    }

    #[test]
    fn handshake_reply_matcher() {
        assert!(is_handshake_reply(&handshake_reply()));
        assert!(is_handshake_reply(&[0x01, 0x00, 0x04, 0x00]));
        assert!(!is_handshake_reply(&[0x01, 0x00, 0x04]));
        assert!(!is_handshake_reply(&[0x02, 0x00, 0x04, 0x00, 0x00]));
        assert!(!is_handshake_reply(&HEADER_BYTES));
    }

    #[tokio::test]
    async fn handshake_reply_is_recorded() {
        let manager = AACPManager::new();
        manager.receive_packet(&handshake_reply()).await;
        assert!(handshake_replied(&manager).await);
    }

    #[tokio::test]
    async fn handshake_reply_other_non_header_packet_records_nothing() {
        let manager = AACPManager::new();
        manager
            .receive_packet(&[0x02, 0x00, 0x04, 0x00, 0x00])
            .await;
        assert!(!handshake_replied(&manager).await);
    }

    #[tokio::test]
    async fn handshake_reply_reset_by_clear_connection_and_disconnect() {
        let manager = AACPManager::new();
        manager.receive_packet(&handshake_reply()).await;
        manager.disconnect().await;
        assert!(!handshake_replied(&manager).await);

        manager.receive_packet(&handshake_reply()).await;
        manager.state.lock().await.clear_connection();
        assert!(!handshake_replied(&manager).await);
    }

    const HANDSHAKE: u8 = 0x00;
    const FLAGS: u8 = opcodes::SET_FEATURE_FLAGS;
    const NOTIFY: u8 = opcodes::REQUEST_NOTIFICATIONS;

    // Drains the send channel into data-packet opcodes, the handshake as HANDSHAKE.
    fn drain_kinds(rx: &mut mpsc::Receiver<Vec<u8>>) -> Vec<u8> {
        std::iter::from_fn(|| rx.try_recv().ok())
            .map(|p| {
                if p.starts_with(&HEADER_BYTES) {
                    p[4]
                } else {
                    assert_eq!(p[..6], [0x00, 0x00, 0x04, 0x00, 0x01, 0x00], "{p:02x?}");
                    HANDSHAKE
                }
            })
            .collect()
    }

    fn spawn_initialize(manager: &AACPManager) -> tokio::task::JoinHandle<Result<()>> {
        let manager = manager.clone();
        tokio::spawn(async move { manager.initialize().await })
    }

    #[tokio::test(start_paused = true)]
    async fn initialize_waits_for_handshake_reply() {
        let (manager, mut rx) = manager_with_send_channel().await;
        let _task = spawn_initialize(&manager);

        sleep(Duration::from_secs(1)).await;
        assert_eq!(drain_kinds(&mut rx), [HANDSHAKE]);

        manager.receive_packet(&handshake_reply()).await;
        sleep(Duration::from_secs(1)).await;
        assert!(Duration::from_secs(2) < HANDSHAKE_REPLY_TIMEOUT);
        assert_eq!(drain_kinds(&mut rx), [FLAGS, NOTIFY]);
    }

    #[tokio::test(start_paused = true)]
    async fn initialize_reply_before_wait_is_not_missed() {
        let (manager, mut rx) = manager_with_send_channel().await;
        manager.receive_packet(&handshake_reply()).await;
        let _task = spawn_initialize(&manager);

        sleep(Duration::from_secs(1)).await;
        assert_eq!(drain_kinds(&mut rx), [HANDSHAKE, FLAGS, NOTIFY]);
    }

    #[tokio::test(start_paused = true)]
    async fn initialize_without_reply_sends_requests_after_timeout() {
        let (manager, mut rx) = manager_with_send_channel().await;
        let _task = spawn_initialize(&manager);

        sleep(HANDSHAKE_REPLY_TIMEOUT - Duration::from_millis(100)).await;
        assert_eq!(drain_kinds(&mut rx), [HANDSHAKE]);

        sleep(Duration::from_secs(1)).await;
        assert_eq!(drain_kinds(&mut rx), [FLAGS, NOTIFY]);
    }

    #[tokio::test(start_paused = true)]
    async fn initialize_resends_once_without_report() {
        let (manager, mut rx) = manager_with_send_channel().await;
        manager.receive_packet(&handshake_reply()).await;
        let task = spawn_initialize(&manager);

        sleep(Duration::from_secs(1)).await;
        assert_eq!(drain_kinds(&mut rx), [HANDSHAKE, FLAGS, NOTIFY]);

        sleep(INIT_REPORT_WAIT + Duration::from_secs(1)).await;
        assert_eq!(drain_kinds(&mut rx), [HANDSHAKE, FLAGS, NOTIFY]);
        assert!(task.is_finished());
        assert!(task.await.unwrap().is_ok());

        sleep(INIT_REPORT_WAIT * 2).await;
        assert!(drain_kinds(&mut rx).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn initialize_report_in_time_sends_nothing_more() {
        let ear_detection = with_header(&[opcodes::EAR_DETECTION, 0x00, 0x00, 0x00]);
        for report in [with_header(CONNECTED_DEVICES_PAYLOAD), ear_detection] {
            let (manager, mut rx) = manager_with_send_channel().await;
            manager.receive_packet(&handshake_reply()).await;
            let task = spawn_initialize(&manager);

            sleep(Duration::from_secs(1)).await;
            manager.receive_packet(&report).await;
            sleep(Duration::from_secs(1)).await;
            assert!(task.is_finished(), "report {report:02x?}");
            assert!(task.await.unwrap().is_ok());

            sleep(INIT_REPORT_WAIT * 2).await;
            assert_eq!(drain_kinds(&mut rx), [HANDSHAKE, FLAGS, NOTIFY]);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn initialize_session_drop_ends_report_wait() {
        let (manager, mut rx) = manager_with_send_channel().await;
        manager.receive_packet(&handshake_reply()).await;
        let task = spawn_initialize(&manager);

        sleep(Duration::from_secs(1)).await;
        manager.state.lock().await.clear_connection();
        sleep(Duration::from_secs(1)).await;
        assert!(task.is_finished());
        assert!(task.await.unwrap().is_err());
        assert_eq!(drain_kinds(&mut rx), [HANDSHAKE, FLAGS, NOTIFY]);
    }
}
