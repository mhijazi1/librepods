use crate::bluetooth::aacp::{AACPEvent, BatteryComponent, BatteryInfo, BatteryStatus};
use crate::devices::enums::{DeviceData, DeviceInformation, DeviceType};
use crate::ui::messages::BluetoothUIMessage;
use crate::ui::tray::MyTray;
use crate::ui::window::merge_battery_entry;
use crate::utils::{ah, get_devices_path, load_auto_connect};
use aes::Aes128;
use aes::cipher::Array;
use aes::cipher::{BlockCipherDecrypt, KeyInit};
use bluer::Session;
use bluer::monitor::{Monitor, MonitorEvent, Pattern};
use futures::StreamExt;
use hex;
use log::{debug, info, warn};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;

/// The least spacing before a failed auto-connect to the same AirPods is retried.
const LE_CONNECT_RETRY: Duration = crate::reconnect::RECONNECT_BACKOFF_MIN;

/// How one `bluetoothctl connect` ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectOutcome {
    Connected,
    Failed,
    SpawnError,
    TimedOut,
}

/// When each AirPods public MAC's last auto-connect failed.
#[derive(Default)]
struct ConnectRetry(HashMap<String, Instant>);

impl ConnectRetry {
    fn ready(&self, mac: &str, now: Instant) -> bool {
        self.0
            .get(mac)
            .is_none_or(|&failed| now.saturating_duration_since(failed) >= LE_CONNECT_RETRY)
    }

    fn finished(&mut self, mac: &str, outcome: ConnectOutcome, now: Instant) {
        if outcome == ConnectOutcome::Connected {
            self.0.remove(mac);
        } else {
            self.0.insert(mac.to_string(), now);
        }
    }
}

// A sync lock: the per-device task must never hold it across an await.
fn lock(retry: &Mutex<ConnectRetry>) -> MutexGuard<'_, ConnectRetry> {
    retry.lock().unwrap_or_else(|e| e.into_inner())
}

/// The connect slot for `mac`, if none is in flight and its retry spacing is over.
/// The guard is taken first, so a failure recorded before the last holder dropped it is seen.
fn try_start(
    retry: &Mutex<ConnectRetry>,
    mac: &str,
    now: Instant,
) -> Option<crate::reconnect::ConnectGuard> {
    let guard = crate::reconnect::ConnectGuard::try_new(mac)?;
    lock(retry).ready(mac, now).then_some(guard)
}

async fn le_connect(mac: &str) -> ConnectOutcome {
    use crate::reconnect::Bluetoothctl;
    match crate::reconnect::bluetoothctl_connect(mac).await {
        Bluetoothctl::Connected => {
            info!("Successfully connected to AirPods {}", mac);
            ConnectOutcome::Connected
        }
        Bluetoothctl::Failed { stderr, .. } => {
            info!("Failed to connect to AirPods {}: {}", mac, stderr);
            ConnectOutcome::Failed
        }
        Bluetoothctl::SpawnError(e) => {
            info!(
                "Failed to execute bluetoothctl to connect to AirPods {}: {}",
                mac, e
            );
            ConnectOutcome::SpawnError
        }
        Bluetoothctl::TimedOut => {
            info!(
                "bluetoothctl connect to AirPods {} timed out after {:?}",
                mac,
                crate::reconnect::CONNECT_TIMEOUT
            );
            ConnectOutcome::TimedOut
        }
    }
}

fn decrypt(key: &[u8; 16], data: &[u8; 16]) -> [u8; 16] {
    let cipher = Aes128::new(&Array::from(*key));
    let mut block = Array::from(*data);
    cipher.decrypt_block(&mut block);
    block.into()
}

fn verify_rpa(addr: &str, irk: &[u8; 16]) -> bool {
    let Some(mut rpa) = crate::bluetooth::aacp::parse_mac(addr) else {
        return false;
    };
    rpa.reverse();
    let prand = [rpa[3], rpa[4], rpa[5]];
    let hash = [rpa[0], rpa[1], rpa[2]];
    let computed_hash = ah(irk, &prand);
    debug!(
        "Verifying RPA: addr={}, hash={:?}, computed_hash={:?}",
        addr, hash, computed_hash
    );
    hash == computed_hash
}

/// 16 bytes from a stored hex key; None if it is empty, not hex or another length.
fn key16(hex_key: &str) -> Option<[u8; 16]> {
    hex::decode(hex_key).ok()?.try_into().ok()
}

/// The AirPods in `devices` whose IRK resolves `addr`: its MAC and its enc key, if valid.
fn rpa_lookup(
    addr: &str,
    devices: &HashMap<String, DeviceData>,
) -> Option<(String, Option<[u8; 16]>)> {
    devices.iter().find_map(|(mac, data)| {
        let (DeviceType::AirPods, Some(DeviceInformation::AirPods(info))) =
            (&data.type_, &data.information)
        else {
            return None;
        };
        let irk = key16(&info.le_keys.irk)?;
        debug!("Verifying RPA {} for AirPods MAC {}", addr, mac);
        verify_rpa(addr, &irk).then(|| (mac.clone(), key16(&info.le_keys.enc_key)))
    })
}

/// Addresses no stored IRK resolved, kept only while the stored IRKs stay the same.
#[derive(Default)]
struct UnmatchedRpas {
    irks: BTreeSet<String>,
    addrs: HashSet<String>,
}

impl UnmatchedRpas {
    /// `rpa_lookup` against `devices_json`, devices.json as read just now. A read or
    /// parse error matches nothing and caches nothing, so the next advert re-checks.
    fn lookup(
        &mut self,
        addr: &str,
        devices_json: std::io::Result<String>,
    ) -> Option<(String, Option<[u8; 16]>)> {
        let devices = crate::settings_file::parse_devices(devices_json)
            .inspect_err(|e| warn!("Could not read devices.json to check {}: {}", addr, e))
            .ok()?;
        let irks = devices
            .values()
            .filter_map(|d| match &d.information {
                Some(DeviceInformation::AirPods(info)) => Some(info.le_keys.irk.clone()),
                _ => None,
            })
            .collect();
        if irks != self.irks {
            self.irks = irks;
            self.addrs.clear();
        }
        if self.addrs.contains(addr) {
            return None;
        }
        let found = rpa_lookup(addr, &devices);
        if found.is_none() {
            debug!("Device {} did not match any of our irks", addr);
            self.addrs.insert(addr.to_string());
        }
        found
    }
}

/// The tray's new (level, status) for one battery component (L, R or Case) after a
/// BLE proximity advertisement whose raw byte for it is `raw`. Bit 7 is the charging
/// flag and bits 0-6 are the level; `0xff` means no reading (e.g. the case after its
/// lid closes). The byte is mapped to an update and the keep/replace decision is left
/// to `merge_battery_entry`, so a `0xff` never clears a known non-Disconnected level.
/// When the result is Disconnected the level is `None`, as the tray had before.
fn ble_tray_entry(
    prev_level: Option<u8>,
    prev_status: Option<BatteryStatus>,
    raw: u8,
) -> (Option<u8>, Option<BatteryStatus>) {
    let update = if raw == 0xff {
        (0, BatteryStatus::Disconnected)
    } else if raw & 0x80 != 0 {
        (raw & 0x7f, BatteryStatus::Charging)
    } else {
        (raw & 0x7f, BatteryStatus::NotCharging)
    };
    match merge_battery_entry(prev_level.zip(prev_status), update) {
        (_, BatteryStatus::Disconnected) => (None, Some(BatteryStatus::Disconnected)),
        (level, status) => (Some(level), Some(status)),
    }
}

/// The warning to log instead of an error when the LE monitor failed because BlueZ
/// has no AdvertisementMonitorManager1 (it is experimental); None for any other error.
pub fn missing_monitor_manager_hint(err: &str) -> Option<&'static str> {
    let unknown = err.contains("org.freedesktop.DBus.Error.UnknownMethod")
        || err.contains("org.freedesktop.DBus.Error.UnknownInterface");
    let monitor = err.contains("AdvertisementMonitorManager1") || err.contains("RegisterMonitor");
    (unknown && monitor).then_some(
        "BlueZ has no org.bluez.AdvertisementMonitorManager1, so the LE features are off \
         (auto-connect when the AirPods leave the case, the case-open popup, advert battery). \
         To turn them on, set `Experimental = true` in the [General] section of \
         /etc/bluetooth/main.conf (NixOS: `hardware.bluetooth.settings.General.Experimental = true;`) \
         and restart Bluetooth.",
    )
}

pub async fn start_le_monitor(
    tray_handle: Option<ksni::Handle<MyTray>>,
    ui_tx: UnboundedSender<BluetoothUIMessage>,
) -> bluer::Result<()> {
    let session = Session::new().await?;
    let adapter = session.default_adapter().await?;
    adapter.set_powered(true).await?;

    let mut unmatched = UnmatchedRpas::default();
    let retry = Arc::new(Mutex::new(ConnectRetry::default()));

    let pattern = Pattern {
        data_type: 0xFF, // Manufacturer specific data
        start_position: 0,
        content: vec![0x4C, 0x00], // Apple manufacturer ID (76) in LE
    };

    let mm = adapter.monitor().await?;
    let mut monitor_handle = mm
        .register(Monitor {
            monitor_type: bluer::monitor::Type::OrPatterns,
            rssi_low_threshold: None,
            rssi_high_threshold: None,
            rssi_low_timeout: None,
            rssi_high_timeout: None,
            rssi_sampling_period: None,
            patterns: Some(vec![pattern]),
            ..Default::default()
        })
        .await?;

    debug!("Started LE monitor");

    while let Some(mevt) = monitor_handle.next().await {
        if let MonitorEvent::DeviceFound(devid) = mevt {
            let adapter_monitor_clone = adapter.clone();
            let dev = adapter_monitor_clone.device(devid.device)?;
            let addr = dev.address();
            let addr_str = addr.to_string();

            // Read now, not at startup: keys stored by a connect in this run must match.
            let Some((airpods_mac, matched_enc_key)) =
                unmatched.lookup(&addr_str, std::fs::read_to_string(get_devices_path()))
            else {
                continue;
            };
            info!(
                "Matched our device ({}) with the irk for {}",
                addr, airpods_mac
            );
            let matched_airpods_mac = Some(airpods_mac);

            if matched_airpods_mac.is_some() {
                let mut events = dev.events().await?;
                let tray_handle_clone = tray_handle.clone();
                let ui_tx_clone = ui_tx.clone();
                let retry_clone = Arc::clone(&retry);
                tokio::spawn(async move {
                    while let Some(ev) = events.next().await {
                        match ev {
                            bluer::DeviceEvent::PropertyChanged(prop) => {
                                if let bluer::DeviceProperty::ManufacturerData(data) = prop {
                                    if let Some(enc_key) = &matched_enc_key
                                        && let Some(apple_data) = data.get(&76)
                                        && apple_data.len() > 20
                                    {
                                        let last_16: [u8; 16] =
                                            apple_data[apple_data.len() - 16..].try_into().unwrap();
                                        let decrypted = decrypt(enc_key, &last_16);
                                        debug!(
                                            "Decrypted data from airpods_mac {}: {}",
                                            matched_airpods_mac
                                                .as_ref()
                                                .unwrap_or(&"unknown".to_string()),
                                            hex::encode(decrypted)
                                        );

                                        let connection_state = apple_data[10] as usize;
                                        debug!("Connection state: {}", connection_state);
                                        if connection_state == 0x00 {
                                            // Tolerates top-level keys such as `switching`.
                                            let mac = matched_airpods_mac.as_ref().unwrap();
                                            let auto_connect = load_auto_connect(mac);
                                            debug!(
                                                "Auto-connect preference for {}: {}",
                                                mac, auto_connect
                                            );
                                            // Every skip only skips the connect; the battery block below still runs.
                                            if !auto_connect {
                                                debug!(
                                                    "Auto-connect is disabled for {}, not attempting to connect.",
                                                    mac
                                                );
                                            } else if let Some(_connect_guard) =
                                                try_start(&retry_clone, mac, Instant::now())
                                            {
                                                // One connect at a time with the playback reconnect (#307).
                                                info!(
                                                    "AirPods are disconnected, attempting to connect to {}",
                                                    mac
                                                );
                                                let outcome = le_connect(mac).await;
                                                lock(&retry_clone).finished(
                                                    mac,
                                                    outcome,
                                                    Instant::now(),
                                                );
                                            } else {
                                                debug!(
                                                    "A connect to {} is in flight or failed recently, skipping.",
                                                    mac
                                                );
                                            }
                                        }

                                        let status = apple_data[5] as usize;
                                        let primary_left = (status >> 5) & 0x01 == 1;
                                        let this_in_case = (status >> 6) & 0x01 == 1;
                                        let xor_factor = primary_left ^ this_in_case;
                                        let is_left_in_ear = if xor_factor {
                                            (status & 0x02) != 0
                                        } else {
                                            (status & 0x08) != 0
                                        };
                                        let is_right_in_ear = if xor_factor {
                                            (status & 0x08) != 0
                                        } else {
                                            (status & 0x02) != 0
                                        };
                                        let is_flipped = !primary_left;

                                        let left_byte_index = if is_flipped { 2 } else { 1 };
                                        let right_byte_index = if is_flipped { 1 } else { 2 };

                                        let left_byte = decrypted[left_byte_index] as i32;
                                        let right_byte = decrypted[right_byte_index] as i32;
                                        let case_byte = decrypted[3] as i32;

                                        let (left_battery, left_charging) = if left_byte == 0xff {
                                            (0, false)
                                        } else {
                                            (left_byte & 0x7F, (left_byte & 0x80) != 0)
                                        };
                                        let (right_battery, right_charging) = if right_byte == 0xff
                                        {
                                            (0, false)
                                        } else {
                                            (right_byte & 0x7F, (right_byte & 0x80) != 0)
                                        };
                                        let (case_battery, case_charging) = if case_byte == 0xff {
                                            (0, false)
                                        } else {
                                            (case_byte & 0x7F, (case_byte & 0x80) != 0)
                                        };

                                        if let Some(handle) = &tray_handle_clone {
                                            handle
                                                .update(|tray: &mut MyTray| {
                                                    (tray.battery_l, tray.battery_l_status) =
                                                        ble_tray_entry(
                                                            tray.battery_l,
                                                            tray.battery_l_status,
                                                            left_byte as u8,
                                                        );
                                                    (tray.battery_r, tray.battery_r_status) =
                                                        ble_tray_entry(
                                                            tray.battery_r,
                                                            tray.battery_r_status,
                                                            right_byte as u8,
                                                        );
                                                    (tray.battery_c, tray.battery_c_status) =
                                                        ble_tray_entry(
                                                            tray.battery_c,
                                                            tray.battery_c_status,
                                                            case_byte as u8,
                                                        );
                                                })
                                                .await;
                                        }

                                        // The tray is not the only consumer: the window shows the
                                        // case level too, and over AACP the case reports itself as
                                        // disconnected whenever the buds are outside it.
                                        if let Some(mac) = matched_airpods_mac.as_ref() {
                                            let battery_info = [
                                                (
                                                    BatteryComponent::Left,
                                                    left_byte,
                                                    left_battery,
                                                    left_charging,
                                                ),
                                                (
                                                    BatteryComponent::Right,
                                                    right_byte,
                                                    right_battery,
                                                    right_charging,
                                                ),
                                                (
                                                    BatteryComponent::Case,
                                                    case_byte,
                                                    case_battery,
                                                    case_charging,
                                                ),
                                            ]
                                            .into_iter()
                                            .filter(|(_, raw, _, _)| *raw != 0xff)
                                            .map(|(component, _, level, charging)| BatteryInfo {
                                                component,
                                                level: level as u8,
                                                status: if charging {
                                                    BatteryStatus::Charging
                                                } else {
                                                    BatteryStatus::NotCharging
                                                },
                                            })
                                            .collect::<Vec<_>>();

                                            if !battery_info.is_empty() {
                                                let _ = ui_tx_clone.send(
                                                    BluetoothUIMessage::AACPUIEvent(
                                                        mac.clone(),
                                                        AACPEvent::BatteryInfo(battery_info),
                                                    ),
                                                );
                                            }
                                        }

                                        debug!(
                                            "Battery status: Left: {}, Right: {}, Case: {}, InEar: L:{} R:{}",
                                            if left_byte == 0xff {
                                                "disconnected".to_string()
                                            } else {
                                                format!(
                                                    "{}% (charging: {})",
                                                    left_battery, left_charging
                                                )
                                            },
                                            if right_byte == 0xff {
                                                "disconnected".to_string()
                                            } else {
                                                format!(
                                                    "{}% (charging: {})",
                                                    right_battery, right_charging
                                                )
                                            },
                                            if case_byte == 0xff {
                                                "disconnected".to_string()
                                            } else {
                                                format!(
                                                    "{}% (charging: {})",
                                                    case_battery, case_charging
                                                )
                                            },
                                            is_left_in_ear,
                                            is_right_in_ear
                                        );
                                    }
                                }
                            }
                        }
                    }
                });
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const IRK: [u8; 16] = [
        0x0F, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78, 0x87, 0x96, 0xA5, 0xB4, 0xC3, 0xD2, 0xE1,
        0xF0,
    ];

    const PEACHES_ERR: &str = "internal error: D-Bus error org.freedesktop.DBus.Error.UnknownMethod: Method \"RegisterMonitor\" with signature \"o\" on interface \"org.bluez.AdvertisementMonitorManager1\" doesn't exist";

    #[test]
    fn missing_monitor_manager_gives_hint() {
        assert!(missing_monitor_manager_hint(PEACHES_ERR).is_some());
        assert!(
            missing_monitor_manager_hint(
                "D-Bus error org.freedesktop.DBus.Error.UnknownInterface: Unknown interface \
                 org.bluez.AdvertisementMonitorManager1"
            )
            .is_some()
        );
    }

    #[test]
    fn other_le_monitor_errors_give_no_hint() {
        assert_eq!(
            missing_monitor_manager_hint(
                "D-Bus error org.bluez.Error.Failed: RegisterMonitor failed"
            ),
            None
        );
        assert_eq!(
            missing_monitor_manager_hint("org.bluez.Error.NotReady: Resource Not Ready"),
            None
        );
    }

    #[test]
    fn missing_monitor_manager_hint_names_the_fix() {
        let hint = missing_monitor_manager_hint(PEACHES_ERR).unwrap();
        for s in [
            "Experimental = true",
            "/etc/bluetooth/main.conf",
            "hardware.bluetooth.settings.General.Experimental",
            "AdvertisementMonitorManager1",
            "LE features are off",
            "auto-connect when the AirPods leave the case",
            "case-open popup",
            "advert battery",
        ] {
            assert!(hint.contains(s), "hint lacks {s:?}");
        }
    }

    // The address string verify_rpa reads for a given hash and prand: the
    // 6-byte little-endian array is hash then prand, and the string is that
    // array reversed.
    fn rpa_string(hash: [u8; 3], prand: [u8; 3]) -> String {
        let le = [hash[0], hash[1], hash[2], prand[0], prand[1], prand[2]];
        le.iter()
            .rev()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(":")
    }

    #[test]
    fn verify_rpa_rejects_malformed_addresses() {
        for bad in [
            "ZZ:BB:CC:DD:EE:FF",
            "",
            "AA:BB",
            "AA:BB:CC:DD:EE:FF:00",
            "+A:BB:CC:DD:EE:FF",
        ] {
            assert!(!verify_rpa(bad, &IRK), "{bad:?} must not verify");
        }
    }

    #[test]
    fn verify_rpa_accepts_valid_rpa_and_rejects_bad_hash() {
        // prand's top two bits are 01, as an RPA's are.
        let prand = [0x01, 0x02, 0x5A];
        let hash = ah(&IRK, &prand);
        let addr = rpa_string(hash, prand);
        assert!(verify_rpa(&addr, &IRK), "valid RPA {addr} must verify");

        let mut bad_hash = hash;
        bad_hash[0] ^= 0x01;
        let forged = rpa_string(bad_hash, prand);
        assert!(
            !verify_rpa(&forged, &IRK),
            "flipped hash {forged} must not verify"
        );
    }

    use BatteryStatus::{Charging, Disconnected, NotCharging};

    const MAC: &str = "AA:BB:CC:DD:EE:FF";

    /// devices.json holding one AirPods (MAC) with these hex keys.
    fn devices_json(irk: &str, enc_key: &str) -> String {
        let s = String::new;
        let info = crate::devices::airpods::AirPodsInformation {
            name: s(),
            model_number: s(),
            manufacturer: s(),
            serial_number: s(),
            version1: s(),
            version2: s(),
            hardware_revision: s(),
            updater_identifier: s(),
            left_serial_number: s(),
            right_serial_number: s(),
            version3: s(),
            le_keys: crate::bluetooth::aacp::AirPodsLEKeys {
                irk: irk.to_string(),
                enc_key: enc_key.to_string(),
            },
        };
        let device = DeviceData {
            name: "AirPods".to_string(),
            type_: DeviceType::AirPods,
            information: Some(DeviceInformation::AirPods(info)),
        };
        serde_json::to_string(&HashMap::from([(MAC.to_string(), device)])).unwrap()
    }

    /// An RPA that IRK resolves.
    fn our_rpa() -> String {
        let prand = [0x01, 0x02, 0x5A];
        rpa_string(ah(&IRK, &prand), prand)
    }

    #[test]
    fn rpa_matches_after_keys_stored_later() {
        let addr = our_rpa();
        let mut unmatched = UnmatchedRpas::default();
        assert_eq!(unmatched.lookup(&addr, Ok(devices_json("", ""))), None);
        // Cached as failed while the keys stay the same.
        assert!(unmatched.addrs.contains(&addr));
        assert_eq!(unmatched.lookup(&addr, Ok(devices_json("", ""))), None);

        let found = unmatched.lookup(&addr, Ok(devices_json(&hex::encode(IRK), "")));
        assert_eq!(found, Some((MAC.to_string(), None)));
    }

    #[test]
    fn unreadable_devices_file_does_not_cache_failure() {
        let addr = our_rpa();
        let stored = devices_json(&hex::encode(IRK), "");
        let mut unmatched = UnmatchedRpas::default();
        // The IRK is already seen, so only a cached failure could block the match below.
        assert_eq!(
            unmatched.lookup("11:22:33:44:55:66", Ok(stored.clone())),
            None
        );

        let errors: [std::io::Result<String>; 3] = [
            Err(std::io::ErrorKind::PermissionDenied.into()),
            Err(std::io::ErrorKind::InvalidData.into()),
            Ok("{".to_string()),
        ];
        for err in errors {
            assert_eq!(unmatched.lookup(&addr, err), None);
            assert!(!unmatched.addrs.contains(&addr));
        }
        assert_eq!(
            unmatched.lookup(&addr, Ok(stored)),
            Some((MAC.to_string(), None))
        );
    }

    #[test]
    fn lookup_returns_current_enc_key() {
        let addr = our_rpa();
        let irk = hex::encode(IRK);
        let enc = [0x5Au8; 16];
        let lookup = |enc_key: &str| {
            let devices = crate::settings_file::parse_devices(Ok(devices_json(&irk, enc_key)));
            rpa_lookup(&addr, &devices.unwrap())
        };
        assert_eq!(
            lookup(&hex::encode(enc)),
            Some((MAC.to_string(), Some(enc)))
        );
        for bad in [
            "",
            "zz",
            &hex::encode([0x5Au8; 15]),
            &hex::encode([0x5Au8; 17]),
        ] {
            assert_eq!(lookup(bad), Some((MAC.to_string(), None)), "{bad:?}");
        }
        // Another IRK doesn't resolve it.
        let devices = crate::settings_file::parse_devices(Ok(devices_json(&"11".repeat(16), "")));
        assert_eq!(rpa_lookup(&addr, &devices.unwrap()), None);
    }

    #[test]
    fn le_connect_slot_freed_on_every_outcome() {
        use ConnectOutcome::*;
        let t0 = Instant::now();
        for outcome in [Connected, Failed, SpawnError, TimedOut] {
            let mut retry = ConnectRetry::default();
            assert!(retry.ready(MAC, t0));
            retry.finished(MAC, outcome, t0);
            // Failures wait out the spacing; a success doesn't.
            assert_eq!(retry.ready(MAC, t0), outcome == Connected, "{outcome:?}");
            assert!(retry.ready(MAC, t0 + LE_CONNECT_RETRY), "{outcome:?}");
            // Only that MAC waits.
            assert!(retry.ready("11:22:33:44:55:66", t0), "{outcome:?}");
        }
    }

    #[test]
    fn le_connect_retry_spaced_after_failure() {
        assert!(LE_CONNECT_RETRY >= Duration::from_secs(10));
        let t0 = Instant::now();
        let ms = Duration::from_millis(1);

        let mut retry = ConnectRetry::default();
        retry.finished(MAC, ConnectOutcome::Failed, t0);
        assert!(!retry.ready(MAC, t0 + LE_CONNECT_RETRY - ms));
        assert!(retry.ready(MAC, t0 + LE_CONNECT_RETRY));

        // A success clears the spacing an earlier failure left.
        retry.finished(MAC, ConnectOutcome::Connected, t0);
        assert!(retry.ready(MAC, t0 + ms));
    }

    #[test]
    fn le_connect_slot_failure_seen_by_next_guard_holder() {
        // Unique: ConnectGuard's in-flight set is process-wide.
        const RACE_MAC: &str = "AA:BB:CC:DD:EE:01";
        let retry = Mutex::new(ConnectRetry::default());
        let t0 = Instant::now();

        // A connect fails while holding the guard, recording before the guard drops.
        let guard = try_start(&retry, RACE_MAC, t0).unwrap();
        assert!(try_start(&retry, RACE_MAC, t0).is_none());
        lock(&retry).finished(RACE_MAC, ConnectOutcome::Failed, t0);
        drop(guard);

        // A racing advert that takes the free guard still sees the spacing, and frees the guard.
        assert!(try_start(&retry, RACE_MAC, t0).is_none());
        drop(crate::reconnect::ConnectGuard::try_new(RACE_MAC).unwrap());
        assert!(try_start(&retry, RACE_MAC, t0 + LE_CONNECT_RETRY).is_some());
    }

    #[test]
    fn ble_tray_entry_0xff_keeps_known_level() {
        assert_eq!(
            ble_tray_entry(Some(40), Some(NotCharging), 0xff),
            (Some(40), Some(NotCharging))
        );
        assert_eq!(
            ble_tray_entry(Some(55), Some(Charging), 0xff),
            (Some(55), Some(Charging))
        );
    }

    #[test]
    fn ble_tray_entry_0xff_without_known_level_is_disconnected() {
        for (level, status) in [
            (None, None),
            (None, Some(Disconnected)),
            (Some(30), Some(Disconnected)),
            (Some(30), None),
            (None, Some(NotCharging)),
        ] {
            assert_eq!(
                ble_tray_entry(level, status, 0xff),
                (None, Some(Disconnected)),
                "prev {level:?}/{status:?}"
            );
        }
    }

    #[test]
    fn ble_tray_entry_reading_always_wins() {
        let prevs = [
            (None, None),
            (None, Some(Disconnected)),
            (Some(40), Some(NotCharging)),
            (Some(55), Some(Charging)),
            (Some(30), Some(Disconnected)),
        ];
        for (level, status) in prevs {
            assert_eq!(
                ble_tray_entry(level, status, 0x85),
                (Some(5), Some(Charging)),
                "prev {level:?}/{status:?}"
            );
            assert_eq!(
                ble_tray_entry(level, status, 0x64),
                (Some(100), Some(NotCharging)),
                "prev {level:?}/{status:?}"
            );
            assert_eq!(
                ble_tray_entry(level, status, 0x00),
                (Some(0), Some(NotCharging)),
                "prev {level:?}/{status:?}"
            );
            assert_eq!(
                ble_tray_entry(level, status, 0xfe),
                (Some(0x7e), Some(Charging)),
                "prev {level:?}/{status:?}"
            );
        }
    }

    #[test]
    fn ble_tray_entry_case_lid_close_sequence() {
        // Case reads 62% while open, then the lid closes (0xff): the tray keeps 62%.
        let (l, s) = ble_tray_entry(None, None, 62);
        assert_eq!((l, s), (Some(62), Some(NotCharging)));
        let (l, s) = ble_tray_entry(l, s, 0xff);
        assert_eq!((l, s), (Some(62), Some(NotCharging)));
        let (l, s) = ble_tray_entry(l, s, 0xff);
        assert_eq!((l, s), (Some(62), Some(NotCharging)));
        // A new reading replaces it.
        assert_eq!(ble_tray_entry(l, s, 0x80 | 63), (Some(63), Some(Charging)));
    }
}
