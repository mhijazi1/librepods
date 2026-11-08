use crate::battery_provider::{BatteryExporter, BluezBatteryProvider};
use crate::bluetooth::aacp::ControlCommandIdentifiers;
use crate::bluetooth::aacp::{AACPEvent, AACPManager, AirPodsLEKeys, ProximityKeyType};
use crate::bluetooth::aacp::{BatteryInfo, BatteryStatus};
use crate::media_controller::MediaController;
use crate::switching::{Switching, SwitchingSetup, device_media, spawn_playback_poller};
use crate::ui::messages::BluetoothUIMessage;
use crate::ui::tray::MyTray;
use crate::ui::window::merge_battery_entry;
use bluer::Address;
use ksni::Handle;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use crate::utils::{get_app_settings_path, load_export_battery, load_switching_mode};

pub struct AirPodsDevice {
    pub mac_address: Address,
    pub aacp_manager: AACPManager,
    // pub att_manager: ATTManager,
    pub media_controller: Arc<Mutex<MediaController>>,
    // pub command_tx: Option<tokio::sync::mpsc::UnboundedSender<(ControlCommandIdentifiers, Vec<u8>)>>,
}

impl AirPodsDevice {
    pub async fn new(
        mac_address: Address,
        tray_handle: Option<Handle<MyTray>>,
        ui_tx: tokio::sync::mpsc::UnboundedSender<BluetoothUIMessage>,
    ) -> Result<Self, bluer::Error> {
        info!("Creating new AirPodsDevice for {}", mac_address);

        // Resolve the local adapter before any side effect (AACP connect, tray
        // update, spawned task), so a failure leaves nothing half set up.
        let session = bluer::Session::new().await.inspect_err(|e| {
            error!(
                "Cannot set up AirPods {mac_address}: cannot talk to BlueZ over D-Bus: {e}. \
                 Is the bluetooth service running? Check with `systemctl status bluetooth`."
            )
        })?;
        let adapter = session.default_adapter().await.inspect_err(|e| {
            error!(
                "Cannot set up AirPods {mac_address}: no Bluetooth adapter available: {e}. \
                 Make sure an adapter is present and not blocked - see `rfkill list bluetooth`."
            )
        })?;
        let local_mac = adapter
            .address()
            .await
            .inspect_err(|e| {
                error!(
                    "Cannot set up AirPods {mac_address}: cannot read the Bluetooth adapter \
                     address: {e}. Is the bluetooth service running (`systemctl status \
                     bluetooth`) and the adapter not blocked (`rfkill list bluetooth`)?"
                )
            })?
            .to_string();

        let mut aacp_manager = AACPManager::new();
        aacp_manager.connect(mac_address).await;

        // let mut att_manager = ATTManager::new();
        // att_manager.connect(mac_address).await.expect("Failed to connect ATT");

        if let Some(handle) = &tray_handle {
            handle
                .update(|tray: &mut MyTray| {
                    tray.connected = true;
                    tray.battery_headphone = None;
                    tray.battery_headphone_status = None;
                    tray.battery_l = None;
                    tray.battery_l_status = None;
                    tray.battery_r = None;
                    tray.battery_r_status = None;
                    tray.battery_c = None;
                    tray.battery_c_status = None;
                })
                .await;
        }

        info!("Sending handshake");
        if let Err(e) = aacp_manager.send_handshake().await {
            error!("Failed to send handshake to AirPods device: {}", e);
        }

        sleep(Duration::from_millis(300)).await;

        info!("Setting feature flags");
        if let Err(e) = aacp_manager.send_set_feature_flags_packet().await {
            error!("Failed to set feature flags: {}", e);
        }

        sleep(Duration::from_millis(300)).await;

        info!("Requesting notifications");
        if let Err(e) = aacp_manager.send_notification_request().await {
            error!("Failed to request notifications: {}", e);
        }

        info!("sending some packet");
        if let Err(e) = aacp_manager.send_some_packet().await {
            error!("Failed to send some packet: {}", e);
        }

        info!("Requesting Proximity Keys: IRK and ENC_KEY");
        if let Err(e) = aacp_manager
            .send_proximity_keys_request(vec![ProximityKeyType::Irk, ProximityKeyType::EncKey])
            .await
        {
            error!("Failed to request proximity keys: {}", e);
        }

        let app_settings_path = get_app_settings_path();
        let settings = std::fs::read_to_string(&app_settings_path)
            .ok()
            .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok());
        let stem_control = settings
            .clone()
            .and_then(|v| v.get("stem_control").cloned())
            .and_then(|s| serde_json::from_value(s).ok())
            .unwrap_or(false);

        if stem_control {
            // Enable stem press detection (double and triple tap)
            // StemConfig bitmask for the control command: single=0x01, double=0x02, triple=0x04, long=0x08
            // We want double and triple: 0x02 | 0x04 = 0x06
            // Note: these bitmask values differ from the StemPressType event enum values (0x05–0x08)
            info!("Enabling stem press detection for double and triple tap");
            if let Err(e) = aacp_manager
                .send_control_command(ControlCommandIdentifiers::StemConfig, &[0x06])
                .await
            {
                error!("Failed to enable stem press detection: {}", e);
            }
        }

        // Desktop battery (BlueZ BatteryProvider1, battery_provider.rs). The
        // exporter registers lazily on the first known level; a registration
        // failure logs one warning and leaves the export off.
        let battery_exporter = if load_export_battery() {
            let provider = BluezBatteryProvider::new(adapter.name(), &mac_address.to_string());
            Some(Arc::new(std::sync::Mutex::new(BatteryExporter::new(provider))))
        } else {
            info!("Desktop battery export disabled by preferences (exportBattery)");
            None
        };

        // The handoff media adapter shares the controller's state without
        // taking the outer mutex the handlers below lock (switching.rs).
        let (media_controller, handoff_media) =
            device_media(mac_address.to_string(), local_mac.clone());
        let mc_clone = media_controller.clone();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel();

        aacp_manager.set_event_channel(tx).await;
        if let Some(handle) = &tray_handle {
            handle
                .update(|tray: &mut MyTray| tray.command_tx = Some(command_tx.clone()))
                .await;
        }

        let aacp_manager_clone = aacp_manager.clone();
        tokio::spawn(async move {
            while let Some((id, value)) = command_rx.recv().await {
                if let Err(e) = aacp_manager_clone.send_control_command(id, &value).await {
                    log::error!("Failed to send control command: {}", e);
                }
            }
        });

        // PipeWire can leave the card on the "off" profile when it appears
        // before its A2DP transport is ready, and nothing revisits that choice
        // until playback starts - so freshly connected buds stay silent, and
        // the microphone has no transport either. Claim a profile right away.
        let mc_profile = media_controller.clone();
        tokio::spawn(async move {
            // Activation can take tens of seconds on a slow card (enumeration
            // waits, card lookups, a WirePlumber restart). Clone the controller
            // (its state is shared) and release the outer lock before awaiting,
            // so the playback listener, ear detection and ownership handling
            // are not stalled behind it.
            let mc = mc_profile.lock().await.clone();
            mc.activate_a2dp_profile().await;
        });

        // Who answers AirPods ownership: the handoff core (default), the
        // legacy eager takeover, or nobody (switching.rs).
        let switching = Switching::start(
            SwitchingSetup {
                mode: load_switching_mode(),
                airpods_mac: mac_address.to_string(),
                local_mac: local_mac.clone(),
                manager: aacp_manager.clone(),
                media_controller: media_controller.clone(),
                command_tx: command_tx.clone(),
            },
            handoff_media,
            spawn_playback_poller,
        )
        .await;

        let (listening_mode_tx, mut listening_mode_rx) = tokio::sync::mpsc::unbounded_channel();
        aacp_manager
            .subscribe_to_control_command(
                ControlCommandIdentifiers::ListeningMode,
                listening_mode_tx,
            )
            .await;
        let tray_handle_clone = tray_handle.clone();
        tokio::spawn(async move {
            while let Some(value) = listening_mode_rx.recv().await {
                if let Some(handle) = &tray_handle_clone {
                    handle
                        .update(|tray: &mut MyTray| {
                            tray.listening_mode = Some(value[0]);
                        })
                        .await;
                }
            }
        });

        let (allow_off_tx, mut allow_off_rx) = tokio::sync::mpsc::unbounded_channel();
        aacp_manager
            .subscribe_to_control_command(ControlCommandIdentifiers::AllowOffOption, allow_off_tx)
            .await;
        let tray_handle_clone = tray_handle.clone();
        tokio::spawn(async move {
            while let Some(value) = allow_off_rx.recv().await {
                if let Some(handle) = &tray_handle_clone {
                    handle
                        .update(|tray: &mut MyTray| {
                            tray.allow_off_option = Some(value[0]);
                        })
                        .await;
                }
            }
        });

        let (conversation_detect_tx, mut conversation_detect_rx) =
            tokio::sync::mpsc::unbounded_channel();
        aacp_manager
            .subscribe_to_control_command(
                ControlCommandIdentifiers::ConversationDetectConfig,
                conversation_detect_tx,
            )
            .await;
        let tray_handle_clone = tray_handle.clone();
        tokio::spawn(async move {
            while let Some(value) = conversation_detect_rx.recv().await {
                if let Some(handle) = &tray_handle_clone {
                    handle
                        .update(|tray: &mut MyTray| {
                            tray.conversation_detect_enabled = Some(value[0] == 0x01);
                        })
                        .await;
                }
            }
        });

        let ui_tx_clone = ui_tx.clone();
        // The switching mode gets every event first, independently of these
        // handlers (switching.rs).
        switching.dispatch_events(rx, move |event: AACPEvent| {
            let mc_clone = mc_clone.clone();
            let tray_handle = tray_handle.clone();
            let ui_tx_clone = ui_tx_clone.clone();
            let battery_exporter = battery_exporter.clone();
            async move {
                let event_clone = event.clone();
                match event {
                    AACPEvent::EarDetection(old_status, new_status) => {
                        debug!(
                            "Received EarDetection event: old_status={:?}, new_status={:?}",
                            old_status, new_status
                        );
                        let controller = mc_clone.lock().await;
                        debug!(
                            "Calling handle_ear_detection with old_status: {:?}, new_status: {:?}",
                            old_status, new_status
                        );
                        controller
                            .handle_ear_detection(old_status, new_status)
                            .await;
                    }
                    AACPEvent::BatteryInfo(battery_info) => {
                        debug!("Received BatteryInfo event: {:?}", battery_info);
                        if let Some(handle) = &tray_handle {
                            handle
                                .update(|tray: &mut MyTray| {
                                    // Same per-component rule as the window: a
                                    // Disconnected entry keeps a known level/status.
                                    fn apply(
                                        level: &mut Option<u8>,
                                        status: &mut Option<BatteryStatus>,
                                        b: &BatteryInfo,
                                    ) {
                                        let prev = (*level).zip(*status);
                                        let (l, s) = merge_battery_entry(prev, (b.level, b.status));
                                        *level = Some(l);
                                        *status = Some(s);
                                    }
                                    for b in &battery_info {
                                        match b.component as u8 {
                                            0x01 => apply(
                                                &mut tray.battery_headphone,
                                                &mut tray.battery_headphone_status,
                                                b,
                                            ),
                                            0x02 => apply(
                                                &mut tray.battery_r,
                                                &mut tray.battery_r_status,
                                                b,
                                            ),
                                            0x04 => apply(
                                                &mut tray.battery_l,
                                                &mut tray.battery_l_status,
                                                b,
                                            ),
                                            0x08 => apply(
                                                &mut tray.battery_c,
                                                &mut tray.battery_c_status,
                                                b,
                                            ),
                                            _ => {}
                                        }
                                    }
                                })
                                .await;
                        }
                        debug!("Updated tray with new battery info");

                        let _ = ui_tx_clone.send(BluetoothUIMessage::AACPUIEvent(
                            mac_address.to_string(),
                            event_clone,
                        ));
                        debug!("Sent BatteryInfo event to UI");

                        if let Some(exporter) = battery_exporter {
                            // Registration is a blocking D-Bus round trip:
                            // keep it off the runtime. The lock lives only
                            // inside the blocking closure.
                            let exported = tokio::task::spawn_blocking(move || {
                                exporter
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                                    .update(&battery_info);
                            })
                            .await;
                            if let Err(e) = exported {
                                warn!("Desktop battery export task failed: {}", e);
                            }
                        }
                    }
                    AACPEvent::ControlCommand(status) => {
                        debug!("Received ControlCommand event: {:?}", status);
                        let _ = ui_tx_clone.send(BluetoothUIMessage::AACPUIEvent(
                            mac_address.to_string(),
                            event_clone,
                        ));
                        debug!("Sent ControlCommand event to UI");
                    }
                    AACPEvent::ConversationalAwareness(status) => {
                        debug!("Received ConversationalAwareness event: {}", status);
                        let controller = mc_clone.lock().await;
                        controller.handle_conversational_awareness(status).await;
                    }
                    // Ownership and announcements: the switching mode.
                    AACPEvent::ConnectedDevices(..) | AACPEvent::OwnershipToFalseRequest => {}
                    AACPEvent::StemPress(press_type, bud_type) => {
                        use crate::bluetooth::aacp::StemPressType;
                        info!(
                            "Received Stem Press: {:?} on {:?}",
                            press_type, bud_type
                        );
                        if stem_control {
                            let controller = mc_clone.lock().await;
                            match press_type {
                                StemPressType::DoublePress => {
                                    info!("Double press detected, skipping to next track");
                                    controller.next_track().await;
                                }
                                StemPressType::TriplePress => {
                                    info!("Triple press detected, going to previous track");
                                    controller.previous_track().await;
                                }
                                _ => {
                                    debug!("Unhandled stem press type: {:?}", press_type);
                                }
                            }
                        } else {
                            debug!("Stem control disabled, ignoring stem press event");
                        }
                    }
                    _ => {
                        debug!("Received unhandled AACP event: {:?}", event);
                        let _ = ui_tx_clone.send(BluetoothUIMessage::AACPUIEvent(
                            mac_address.to_string(),
                            event_clone,
                        ));
                        debug!("Sent unhandled AACP event to UI");
                    }
                }
            }
        });

        Ok(AirPodsDevice {
            mac_address,
            aacp_manager,
            // att_manager,
            media_controller,
            // command_tx: Some(command_tx.clone()),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AirPodsInformation {
    pub name: String,
    pub model_number: String,
    pub manufacturer: String,
    pub serial_number: String,
    pub version1: String,
    pub version2: String,
    pub hardware_revision: String,
    pub updater_identifier: String,
    pub left_serial_number: String,
    pub right_serial_number: String,
    pub version3: String,
    pub le_keys: AirPodsLEKeys,
}
