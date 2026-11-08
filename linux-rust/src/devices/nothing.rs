use crate::bluetooth::att::{ATTHandles, ATTManager};
use crate::devices::enums::{DeviceData, DeviceInformation, DeviceType};
use crate::settings_file::{parse_devices, rewrite_devices_file};
use crate::ui::messages::BluetoothUIMessage;
use crate::utils::get_devices_path;
use bluer::Address;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NothingInformation {
    pub serial_number: String,
    pub firmware_version: String,
}

pub struct NothingDevice {
    pub att_manager: ATTManager,
    pub information: NothingInformation,
}

impl NothingDevice {
    pub async fn new(
        mac_address: Address,
        ui_tx: mpsc::UnboundedSender<BluetoothUIMessage>,
    ) -> Self {
        let mut att_manager = ATTManager::new();
        att_manager
            .connect(mac_address)
            .await
            .expect("Failed to connect");

        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

        att_manager
            .register_listener(ATTHandles::NothingEverythingRead, tx)
            .await;

        let devices: HashMap<String, DeviceData> = std::fs::read_to_string(get_devices_path())
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        let device_key = mac_address.to_string();
        let information = match devices.get(&device_key).map(|d| &d.information) {
            Some(Some(DeviceInformation::Nothing(info))) => info.clone(),
            _ => NothingInformation::default(),
        };

        // Request version information
        att_manager
            .write(
                ATTHandles::NothingEverything,
                &[
                    0x55, 0x20, 0x01, 0x42, 0xC0, 0x00, 0x00, 0x00, 0x00,
                    0x00, // something, idk
                ],
            )
            .await
            .expect("Failed to write");

        sleep(Duration::from_millis(100)).await;

        // Request serial number
        att_manager
            .write(
                ATTHandles::NothingEverything,
                &[0x55, 0x20, 0x01, 0x06, 0xC0, 0x00, 0x00, 0x13, 0x00, 0x00],
            )
            .await
            .expect("Failed to write");

        // let ui_tx_clone = ui_tx.clone();
        tokio::spawn(async move {
            while let Some(data) = rx.recv().await {
                match parse_nothing_reply(&data) {
                    Some(NothingReply::Firmware(firmware_version)) => {
                        info!(
                            "Received firmware version from Nothing device {}: {}",
                            mac_address, firmware_version
                        );
                        save_nothing_info(&mac_address.to_string(), |info| {
                            info.firmware_version = firmware_version
                        });
                    }
                    Some(NothingReply::Serial(serial_number)) => {
                        info!(
                            "Received serial number from Nothing device {}: {}",
                            mac_address, serial_number
                        );
                        save_nothing_info(&mac_address.to_string(), |info| {
                            info.serial_number = serial_number
                        });
                    }
                    None => debug!(
                        "No firmware version or serial number in reply from Nothing device {}",
                        mac_address
                    ),
                }

                debug!(
                    "Received data from (Nothing) device {}, data: {:?}",
                    mac_address, data
                );
            }
        });

        NothingDevice {
            att_manager,
            information,
        }
    }
}

#[derive(Debug, PartialEq)]
enum NothingReply {
    Firmware(String),
    Serial(String),
}

/// The firmware version or serial number in a reply from the device; None for
/// any other or malformed reply.
fn parse_nothing_reply(data: &[u8]) -> Option<NothingReply> {
    if data.starts_with(&[0x55, 0x20, 0x01, 0x42, 0x40]) {
        let version = data.get(8..)?;
        Some(NothingReply::Firmware(
            String::from_utf8_lossy(version).to_string(),
        ))
    } else if data.starts_with(&[0x55, 0x20, 0x01, 0x06, 0x40]) {
        let start = data.iter().position(|&b| b == b'S').unwrap_or(8);
        if data.get(start + 1) != Some(&b'H') {
            return None;
        }
        let end = data[start..]
            .iter()
            .position(|&b| b == 0x0A)
            .map_or(data.len(), |pos| pos + start);
        Some(NothingReply::Serial(
            String::from_utf8_lossy(&data[start..end]).to_string(),
        ))
    } else {
        None
    }
}

/// The devices.json contents after `update` is applied to `mac`'s Nothing
/// information, given the result of reading the file. Every other entry and
/// field is kept; a missing entry is created as "Nothing Device".
fn devices_with_nothing_info(
    existing: std::io::Result<String>,
    mac: &str,
    update: impl FnOnce(&mut NothingInformation),
) -> std::io::Result<String> {
    let mut devices = parse_devices(existing)?;
    let device = devices.entry(mac.to_string()).or_insert(DeviceData {
        name: "Nothing Device".to_string(),
        type_: DeviceType::Nothing,
        information: None,
    });
    let mut info = match device.information.take() {
        Some(DeviceInformation::Nothing(info)) => info,
        _ => NothingInformation::default(),
    };
    update(&mut info);
    device.information = Some(DeviceInformation::Nothing(info));
    Ok(serde_json::to_string(&devices)?)
}

/// Re-reads devices.json and saves `update` to `mac`'s entry; on any error
/// logs it and leaves the file untouched.
fn save_nothing_info(mac: &str, update: impl FnOnce(&mut NothingInformation)) {
    let path = get_devices_path();
    let result = rewrite_devices_file(&path, |existing| {
        devices_with_nothing_info(existing, mac, update)
    });
    if let Err(e) = result {
        error!(
            "Not saving Nothing device {} info to {}: {}",
            mac,
            path.display(),
            e
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAC: &str = "AA:AA:AA:AA:AA:AA";
    const OTHER: &str = "BB:BB:BB:BB:BB:BB";

    fn parse(json: &str) -> HashMap<String, DeviceData> {
        serde_json::from_str(json).unwrap()
    }

    fn nothing_info(device: &DeviceData) -> &NothingInformation {
        match &device.information {
            Some(DeviceInformation::Nothing(info)) => info,
            other => panic!("not Nothing information: {other:?}"),
        }
    }

    #[test]
    fn nothing_info_keeps_other_devices() {
        let other = serde_json::json!({
            OTHER: {"name": "My AirPods", "type_": "AirPods", "information": null}
        });
        let json = devices_with_nothing_info(Ok(other.to_string()), MAC, |i| {
            i.firmware_version = "1.0".to_string()
        })
        .unwrap();
        let written: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(written[OTHER], other[OTHER]);
        assert_eq!(nothing_info(&parse(&json)[MAC]).firmware_version, "1.0");
    }

    #[test]
    fn nothing_info_missing_file_creates_entry() {
        let json = devices_with_nothing_info(Err(std::io::ErrorKind::NotFound.into()), MAC, |i| {
            i.serial_number = "SH123".to_string()
        })
        .unwrap();
        let devices = parse(&json);
        assert_eq!(devices.len(), 1);
        let device = &devices[MAC];
        assert_eq!(device.name, "Nothing Device");
        assert_eq!(device.type_, DeviceType::Nothing);
        let info = nothing_info(device);
        assert_eq!(info.serial_number, "SH123");
        assert_eq!(info.firmware_version, "");
    }

    #[test]
    fn nothing_info_unreadable_file_is_error() {
        let err = devices_with_nothing_info(
            Err(std::io::ErrorKind::PermissionDenied.into()),
            MAC,
            |_| panic!("updated after a read error"),
        )
        .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn nothing_info_bad_json_is_error() {
        for json in ["", "{"] {
            let result = devices_with_nothing_info(Ok(json.to_string()), MAC, |_| {
                panic!("updated after a parse error")
            });
            assert!(result.is_err(), "{json:?}");
        }
    }

    #[test]
    fn nothing_info_updates_only_received_field() {
        let stored = serde_json::json!({
            MAC: {
                "name": "My Ear (2)",
                "type_": "Nothing",
                "information": {
                    "kind": "Nothing",
                    "data": {"serial_number": "SH-OLD", "firmware_version": "0.9"}
                }
            }
        })
        .to_string();

        let json = devices_with_nothing_info(Ok(stored.clone()), MAC, |i| {
            i.firmware_version = "1.0".to_string()
        })
        .unwrap();
        let device = &parse(&json)[MAC];
        assert_eq!(device.name, "My Ear (2)");
        assert_eq!(device.type_, DeviceType::Nothing);
        let info = nothing_info(device);
        assert_eq!(info.firmware_version, "1.0");
        assert_eq!(info.serial_number, "SH-OLD");

        let json =
            devices_with_nothing_info(Ok(stored), MAC, |i| i.serial_number = "SH-NEW".to_string())
                .unwrap();
        let device = &parse(&json)[MAC];
        assert_eq!(device.name, "My Ear (2)");
        assert_eq!(device.type_, DeviceType::Nothing);
        let info = nothing_info(device);
        assert_eq!(info.serial_number, "SH-NEW");
        assert_eq!(info.firmware_version, "0.9");
    }

    const FIRMWARE: [u8; 5] = [0x55, 0x20, 0x01, 0x42, 0x40];
    const SERIAL: [u8; 5] = [0x55, 0x20, 0x01, 0x06, 0x40];

    #[test]
    fn nothing_reply_short_firmware_is_ignored() {
        for len in 5..8 {
            let mut data = FIRMWARE.to_vec();
            data.resize(len, 0);
            assert_eq!(parse_nothing_reply(&data), None, "length {len}");
        }
    }

    #[test]
    fn nothing_reply_firmware_parses() {
        let mut data = [&FIRMWARE[..], &[0, 0, 0]].concat();
        assert_eq!(
            parse_nothing_reply(&data),
            Some(NothingReply::Firmware(String::new()))
        );
        data.extend_from_slice(b"1.0.2");
        assert_eq!(
            parse_nothing_reply(&data),
            Some(NothingReply::Firmware("1.0.2".to_string()))
        );
    }

    #[test]
    fn nothing_reply_serial_parses() {
        let terminated = [&SERIAL[..], &[0, 0x13, 0], b"SH123\nrest"].concat();
        assert_eq!(
            parse_nothing_reply(&terminated),
            Some(NothingReply::Serial("SH123".to_string()))
        );
        let unterminated = [&SERIAL[..], &[0, 0x13, 0], b"SH456"].concat();
        assert_eq!(
            parse_nothing_reply(&unterminated),
            Some(NothingReply::Serial("SH456".to_string()))
        );
        for not_sh in [
            [&SERIAL[..], &[0, 0x13, 0], b"SX123"].concat(),
            [&SERIAL[..], &[0, 0x13, 0], b"12345"].concat(),
            SERIAL.to_vec(),
        ] {
            assert_eq!(parse_nothing_reply(&not_sh), None, "{not_sh:?}");
        }
    }

    #[test]
    fn nothing_reply_other_prefix_is_none() {
        assert_eq!(
            parse_nothing_reply(&[0x55, 0x20, 0x01, 0x99, 0x40, 0, 0, 0, b'S', b'H']),
            None
        );
        assert_eq!(parse_nothing_reply(&[]), None);
    }
}
