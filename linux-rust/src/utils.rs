use aes::Aes128;
use aes::cipher::Array;
use aes::cipher::{BlockCipherEncrypt, KeyInit};
use iced::Theme;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub fn get_devices_path() -> PathBuf {
    let data_dir = std::env::var("XDG_DATA_HOME")
        .unwrap_or_else(|_| format!("{}/.local/share", std::env::var("HOME").unwrap_or_default()));
    PathBuf::from(data_dir)
        .join("librepods")
        .join("devices.json")
}

pub fn get_preferences_path() -> PathBuf {
    let config_dir = std::env::var("XDG_CONFIG_HOME")
        .unwrap_or_else(|_| format!("{}/.config", std::env::var("HOME").unwrap_or_default()));
    PathBuf::from(config_dir)
        .join("librepods")
        .join("preferences.json")
}

pub fn get_app_settings_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_default();

    let config_dir =
        std::env::var("XDG_CONFIG_HOME").unwrap_or_else(|_| format!("{}/.config", home));

    let data_dir =
        std::env::var("XDG_DATA_HOME").unwrap_or_else(|_| format!("{}/.local/share", home));

    let new_path = PathBuf::from(&config_dir)
        .join("librepods")
        .join("app_settings.json");

    let old_path = PathBuf::from(&data_dir).join("app_settings.json");

    // migrate if needed
    if old_path.exists() && !new_path.exists() {
        if let Some(parent) = new_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        if std::fs::copy(&old_path, &new_path).is_ok() {
            let _ = std::fs::remove_file(&old_path);
        }
    }

    new_path
}

/// Preferred A2DP codec. The chosen one is tried first, the rest act as fallbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum PreferredCodec {
    #[default]
    Aac,
    SbcXq,
    Sbc,
}

impl PreferredCodec {
    pub const ALL: [PreferredCodec; 3] = [Self::Aac, Self::SbcXq, Self::Sbc];

    /// PulseAudio/PipeWire card profile name for this codec.
    pub fn profile_name(self) -> &'static str {
        match self {
            Self::Aac => "a2dp-sink",
            Self::SbcXq => "a2dp-sink-sbc_xq",
            Self::Sbc => "a2dp-sink-sbc",
        }
    }

    /// Profiles to try, most preferred first.
    pub fn profile_order(self) -> Vec<&'static str> {
        std::iter::once(self.profile_name())
            .chain(
                Self::ALL
                    .iter()
                    .filter(|c| **c != self)
                    .map(|c| c.profile_name()),
            )
            .collect()
    }
}

impl std::fmt::Display for PreferredCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Aac => "AAC",
            Self::SbcXq => "SBC-XQ",
            Self::Sbc => "SBC",
        })
    }
}

/// Reads the preferred codec from the app settings file, falling back to AAC.
pub fn load_preferred_codec() -> PreferredCodec {
    std::fs::read_to_string(get_app_settings_path())
        .map(|s| parse_preferred_codec(&s))
        .unwrap_or_default()
}

/// Reads `preferred_codec` from already-parsed app settings. Missing settings,
/// a missing key, null or an unknown codec name all give the default (AAC).
pub fn preferred_codec_from_value(settings: Option<&serde_json::Value>) -> PreferredCodec {
    settings
        .and_then(|v| v.get("preferred_codec").cloned())
        .and_then(|c| serde_json::from_value(c).ok())
        .unwrap_or_default()
}

/// Extracts `preferred_codec` from the app settings JSON. Anything missing or
/// unreadable gives the default (AAC). Split from the file read for testing.
pub fn parse_preferred_codec(settings: &str) -> PreferredCodec {
    let value = serde_json::from_str::<serde_json::Value>(settings).ok();
    preferred_codec_from_value(value.as_ref())
}

/// How the app answers AirPods ownership (automatic device switching), from
/// the top-level `switching` key of preferences.json.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SwitchingMode {
    /// The #841 handoff core (handoff.rs) decides every take and yield.
    #[default]
    Handoff,
    /// The eager playback takeover (hijack) the app used before.
    Legacy,
    /// Linux never claims or yields ownership.
    Off,
}

/// Reads `switching` from already-parsed preferences. Missing preferences, a
/// missing key or null give the default (handoff); a value of the wrong type
/// or an unknown name logs a warning and gives the default too.
pub fn switching_mode_from_value(preferences: Option<&serde_json::Value>) -> SwitchingMode {
    let Some(value) = preferences.and_then(|p| p.get("switching")) else {
        return SwitchingMode::default();
    };
    if value.is_null() {
        return SwitchingMode::default();
    }
    serde_json::from_value(value.clone()).unwrap_or_else(|_| {
        log::warn!(
            "Unknown switching preference {value}; expected \"handoff\", \"legacy\" or \"off\". Using handoff."
        );
        SwitchingMode::default()
    })
}

/// Extracts `switching` from the preferences JSON; anything missing or
/// unreadable gives the default (handoff). Split from the file read for tests.
pub fn parse_switching_mode(preferences: &str) -> SwitchingMode {
    let value = serde_json::from_str::<serde_json::Value>(preferences).ok();
    switching_mode_from_value(value.as_ref())
}

/// Reads the switching mode from preferences.json, falling back to handoff.
pub fn load_switching_mode() -> SwitchingMode {
    std::fs::read_to_string(get_preferences_path())
        .map(|s| parse_switching_mode(&s))
        .unwrap_or_default()
}

/// Reads a device's `autoConnect` from already-parsed preferences. The file
/// holds one object per AirPods MAC next to top-level settings such as
/// `switching`, so other keys and value types are ignored. Anything missing
/// or unreadable gives the default (true).
pub fn auto_connect_from_value(preferences: Option<&serde_json::Value>, mac: &str) -> bool {
    preferences
        .and_then(|p| p.get(mac))
        .and_then(|device| device.get("autoConnect"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true)
}

/// Extracts a device's `autoConnect` from the preferences JSON (see
/// `auto_connect_from_value`). Split from the file read for tests.
pub fn parse_auto_connect(preferences: &str, mac: &str) -> bool {
    let value = serde_json::from_str::<serde_json::Value>(preferences).ok();
    auto_connect_from_value(value.as_ref(), mac)
}

/// Reads a device's `autoConnect` from preferences.json, defaulting to true.
pub fn load_auto_connect(mac: &str) -> bool {
    std::fs::read_to_string(get_preferences_path())
        .map(|s| parse_auto_connect(&s, mac))
        .unwrap_or(true)
}

/// Apple's per-device "Connect to this computer", from the device's
/// `connectMode` in preferences.json (written by the UI; read by matching
/// the strings in `connect_mode_from_value`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ConnectMode {
    /// The handoff core takes the AirPods for new playback as before.
    #[default]
    Automatically,
    /// Automatic takes only while Linux was the last owner.
    WhenLastConnected,
}

impl std::fmt::Display for ConnectMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Automatically => "Automatically",
            Self::WhenLastConnected => "When last connected to this computer",
        })
    }
}

/// Reads a device's `connectMode` from already-parsed preferences. Missing
/// preferences, device, key or null give the default (automatically); an
/// unknown name or a value of the wrong type logs a warning and gives the
/// default too.
pub fn connect_mode_from_value(preferences: Option<&serde_json::Value>, mac: &str) -> ConnectMode {
    let Some(value) = preferences
        .and_then(|p| p.get(mac))
        .and_then(|device| device.get("connectMode"))
    else {
        return ConnectMode::default();
    };
    // Match the strings: serde would also take {"when_last_connected": null}.
    match value.as_str() {
        Some("automatically") => ConnectMode::Automatically,
        Some("when_last_connected") => ConnectMode::WhenLastConnected,
        _ if value.is_null() => ConnectMode::default(),
        _ => {
            log::warn!(
                "Unknown connectMode preference {value} for {mac}; expected \"automatically\" or \"when_last_connected\". Using automatically."
            );
            ConnectMode::default()
        }
    }
}

/// Extracts a device's `connectMode` from the preferences JSON (see
/// `connect_mode_from_value`). Split from the file read for tests.
pub fn parse_connect_mode(preferences: &str, mac: &str) -> ConnectMode {
    let value = serde_json::from_str::<serde_json::Value>(preferences).ok();
    connect_mode_from_value(value.as_ref(), mac)
}

/// Reads a device's `connectMode` from preferences.json, defaulting to
/// automatically.
pub fn load_connect_mode(mac: &str) -> ConnectMode {
    std::fs::read_to_string(get_preferences_path())
        .map(|s| parse_connect_mode(&s, mac))
        .unwrap_or_default()
}

/// Reads a device's `takeBackAfterCall` (#14) from already-parsed
/// preferences: only a bool counts, anything else gives the default (off).
pub fn take_back_after_call_from_value(preferences: Option<&serde_json::Value>, mac: &str) -> bool {
    preferences
        .and_then(|p| p.get(mac))
        .and_then(|device| device.get("takeBackAfterCall"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

/// Reads a device's `takeBackAfterCall` from preferences.json, defaulting to
/// off.
pub fn load_take_back_after_call(mac: &str) -> bool {
    let prefs = std::fs::read_to_string(get_preferences_path()).unwrap_or_default();
    take_back_after_call_from_value(serde_json::from_str(&prefs).ok().as_ref(), mac)
}

/// Reads the top-level `exportBattery` (export the AirPods battery to BlueZ,
/// see battery_provider.rs) from already-parsed preferences. Missing
/// preferences, a missing key or null give the default (true); a bool is used
/// as-is; any other type logs a warning and gives the default too.
pub fn export_battery_from_value(preferences: Option<&serde_json::Value>) -> bool {
    match preferences.and_then(|p| p.get("exportBattery")) {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::Bool(enabled)) => *enabled,
        Some(other) => {
            log::warn!(
                "Invalid exportBattery preference {other}; expected true or false. Exporting the battery."
            );
            true
        }
    }
}

/// Extracts `exportBattery` from the preferences JSON; anything missing or
/// unreadable gives the default (true). Split from the file read for tests.
pub fn parse_export_battery(preferences: &str) -> bool {
    let value = serde_json::from_str::<serde_json::Value>(preferences).ok();
    export_battery_from_value(value.as_ref())
}

/// Reads `exportBattery` from preferences.json, defaulting to true.
pub fn load_export_battery() -> bool {
    std::fs::read_to_string(get_preferences_path())
        .map(|s| parse_export_battery(&s))
        .unwrap_or(true)
}

/// Extracts the top-level `switchingNotifications` (post "moved to" and
/// "Connected", #17) from the preferences JSON; only a bool false turns
/// them off.
pub fn parse_switching_notifications(preferences: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(preferences)
        .ok()
        .and_then(|p| p.get("switchingNotifications")?.as_bool())
        .unwrap_or(true)
}

/// Reads `switchingNotifications` from preferences.json, defaulting to true.
pub fn load_switching_notifications() -> bool {
    std::fs::read_to_string(get_preferences_path())
        .map(|s| parse_switching_notifications(&s))
        .unwrap_or(true)
}

fn e(key: &[u8; 16], data: &[u8; 16]) -> [u8; 16] {
    let mut swapped_key = *key;
    swapped_key.reverse();
    let mut swapped_data = *data;
    swapped_data.reverse();
    let cipher = Aes128::new(&Array::from(swapped_key));
    let mut block = Array::from(swapped_data);
    cipher.encrypt_block(&mut block);
    let mut result: [u8; 16] = block.into();
    result.reverse();
    result
}

pub fn ah(k: &[u8; 16], r: &[u8; 3]) -> [u8; 3] {
    let mut r_padded = [0u8; 16];
    r_padded[..3].copy_from_slice(r);
    let encrypted = e(k, &r_padded);
    let mut hash = [0u8; 3];
    hash.copy_from_slice(&encrypted[..3]);
    hash
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum MyTheme {
    Light,
    Dark,
    Dracula,
    Nord,
    SolarizedLight,
    SolarizedDark,
    GruvboxLight,
    GruvboxDark,
    CatppuccinLatte,
    CatppuccinFrappe,
    CatppuccinMacchiato,
    CatppuccinMocha,
    TokyoNight,
    TokyoNightStorm,
    TokyoNightLight,
    KanagawaWave,
    KanagawaDragon,
    KanagawaLotus,
    Moonfly,
    Nightfly,
    Oxocarbon,
    Ferra,
}

impl std::fmt::Display for MyTheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Light => "Light",
            Self::Dark => "Dark",
            Self::Dracula => "Dracula",
            Self::Nord => "Nord",
            Self::SolarizedLight => "Solarized Light",
            Self::SolarizedDark => "Solarized Dark",
            Self::GruvboxLight => "Gruvbox Light",
            Self::GruvboxDark => "Gruvbox Dark",
            Self::CatppuccinLatte => "Catppuccin Latte",
            Self::CatppuccinFrappe => "Catppuccin Frappé",
            Self::CatppuccinMacchiato => "Catppuccin Macchiato",
            Self::CatppuccinMocha => "Catppuccin Mocha",
            Self::TokyoNight => "Tokyo Night",
            Self::TokyoNightStorm => "Tokyo Night Storm",
            Self::TokyoNightLight => "Tokyo Night Light",
            Self::KanagawaWave => "Kanagawa Wave",
            Self::KanagawaDragon => "Kanagawa Dragon",
            Self::KanagawaLotus => "Kanagawa Lotus",
            Self::Moonfly => "Moonfly",
            Self::Nightfly => "Nightfly",
            Self::Oxocarbon => "Oxocarbon",
            Self::Ferra => "Ferra",
        })
    }
}

impl From<MyTheme> for Theme {
    fn from(my_theme: MyTheme) -> Self {
        match my_theme {
            MyTheme::Light => Theme::Light,
            MyTheme::Dark => Theme::Dark,
            MyTheme::Dracula => Theme::Dracula,
            MyTheme::Nord => Theme::Nord,
            MyTheme::SolarizedLight => Theme::SolarizedLight,
            MyTheme::SolarizedDark => Theme::SolarizedDark,
            MyTheme::GruvboxLight => Theme::GruvboxLight,
            MyTheme::GruvboxDark => Theme::GruvboxDark,
            MyTheme::CatppuccinLatte => Theme::CatppuccinLatte,
            MyTheme::CatppuccinFrappe => Theme::CatppuccinFrappe,
            MyTheme::CatppuccinMacchiato => Theme::CatppuccinMacchiato,
            MyTheme::CatppuccinMocha => Theme::CatppuccinMocha,
            MyTheme::TokyoNight => Theme::TokyoNight,
            MyTheme::TokyoNightStorm => Theme::TokyoNightStorm,
            MyTheme::TokyoNightLight => Theme::TokyoNightLight,
            MyTheme::KanagawaWave => Theme::KanagawaWave,
            MyTheme::KanagawaDragon => Theme::KanagawaDragon,
            MyTheme::KanagawaLotus => Theme::KanagawaLotus,
            MyTheme::Moonfly => Theme::Moonfly,
            MyTheme::Nightfly => Theme::Nightfly,
            MyTheme::Oxocarbon => Theme::Oxocarbon,
            MyTheme::Ferra => Theme::Ferra,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preferred_codec_profile_order() {
        assert_eq!(PreferredCodec::default(), PreferredCodec::Aac);
        assert_eq!(
            PreferredCodec::Aac.profile_order(),
            ["a2dp-sink", "a2dp-sink-sbc_xq", "a2dp-sink-sbc"]
        );
        assert_eq!(
            PreferredCodec::SbcXq.profile_order(),
            ["a2dp-sink-sbc_xq", "a2dp-sink", "a2dp-sink-sbc"]
        );
        assert_eq!(
            PreferredCodec::Sbc.profile_order(),
            ["a2dp-sink-sbc", "a2dp-sink", "a2dp-sink-sbc_xq"]
        );
    }

    #[test]
    fn preferred_codec_parse_defaults_to_aac() {
        assert_eq!(parse_preferred_codec("{}"), PreferredCodec::Aac);
        assert_eq!(
            parse_preferred_codec(r#"{"preferred_codec":"bogus"}"#),
            PreferredCodec::Aac
        );
        assert_eq!(parse_preferred_codec("not json"), PreferredCodec::Aac);
        assert_eq!(parse_preferred_codec(""), PreferredCodec::Aac);
        assert_eq!(
            parse_preferred_codec(r#"{"theme":"Dark","preferred_codec":null}"#),
            PreferredCodec::Aac
        );
        assert_eq!(
            parse_preferred_codec(r#"{"preferred_codec":"SbcXq"}"#),
            PreferredCodec::SbcXq
        );
        assert_eq!(
            parse_preferred_codec(r#"{"preferred_codec":"Sbc"}"#),
            PreferredCodec::Sbc
        );
    }

    #[test]
    fn switching_pref_missing_gives_handoff() {
        assert_eq!(SwitchingMode::default(), SwitchingMode::Handoff);
        assert_eq!(switching_mode_from_value(None), SwitchingMode::Handoff);
        assert_eq!(parse_switching_mode("{}"), SwitchingMode::Handoff);
        assert_eq!(parse_switching_mode(""), SwitchingMode::Handoff);
        assert_eq!(
            parse_switching_mode(r#"{"switching":null}"#),
            SwitchingMode::Handoff
        );
    }

    #[test]
    fn switching_pref_valid_values() {
        assert_eq!(
            parse_switching_mode(r#"{"switching":"handoff"}"#),
            SwitchingMode::Handoff
        );
        assert_eq!(
            parse_switching_mode(r#"{"switching":"legacy"}"#),
            SwitchingMode::Legacy
        );
        assert_eq!(
            parse_switching_mode(r#"{"switching":"off"}"#),
            SwitchingMode::Off
        );
        assert_eq!(
            switching_mode_from_value(Some(&serde_json::json!({"switching": "legacy"}))),
            SwitchingMode::Legacy
        );
    }

    #[test]
    fn switching_pref_invalid_string_gives_handoff() {
        for value in [r#""Off""#, r#""bogus""#, r#""""#, r#"" off""#] {
            assert_eq!(
                parse_switching_mode(&format!(r#"{{"switching":{value}}}"#)),
                SwitchingMode::Handoff,
                "{value}"
            );
        }
    }

    #[test]
    fn switching_pref_wrong_type_gives_handoff() {
        for value in ["true", "0", "[\"off\"]", r#"{"mode":"off"}"#] {
            assert_eq!(
                parse_switching_mode(&format!(r#"{{"switching":{value}}}"#)),
                SwitchingMode::Handoff,
                "{value}"
            );
        }
    }

    #[test]
    fn switching_pref_non_json_gives_handoff() {
        assert_eq!(parse_switching_mode("not json"), SwitchingMode::Handoff);
        assert_eq!(
            parse_switching_mode("switching=off"),
            SwitchingMode::Handoff
        );
        assert_eq!(parse_switching_mode(r#""off""#), SwitchingMode::Handoff);
        assert_eq!(parse_switching_mode("[]"), SwitchingMode::Handoff);
    }

    #[test]
    fn switching_pref_next_to_auto_connect() {
        assert_eq!(
            parse_switching_mode(
                r#"{"switching":"off","AA:BB:CC:DD:EE:FF":{"autoConnect":false}}"#
            ),
            SwitchingMode::Off
        );
        assert_eq!(
            parse_switching_mode(r#"{"AA:BB:CC:DD:EE:FF":{"autoConnect":false}}"#),
            SwitchingMode::Handoff
        );
    }

    #[test]
    fn auto_connect_pref_next_to_switching() {
        let prefs = r#"{"switching":"legacy","AA:BB:CC:DD:EE:FF":{"autoConnect":false}}"#;
        assert!(!parse_auto_connect(prefs, "AA:BB:CC:DD:EE:FF"));
        assert!(parse_auto_connect(prefs, "11:22:33:44:55:66"));
    }

    #[test]
    fn auto_connect_pref_defaults_to_true() {
        let mac = "AA:BB:CC:DD:EE:FF";
        assert!(parse_auto_connect("{}", mac));
        assert!(parse_auto_connect("not json", mac));
        assert!(parse_auto_connect("", mac));
        assert!(auto_connect_from_value(None, mac));
        assert!(parse_auto_connect(r#"{"AA:BB:CC:DD:EE:FF":{}}"#, mac));
        assert!(parse_auto_connect(
            r#"{"AA:BB:CC:DD:EE:FF":{"autoConnect":null}}"#,
            mac
        ));
        assert!(parse_auto_connect(r#"{"AA:BB:CC:DD:EE:FF":"off"}"#, mac));
        assert!(parse_auto_connect(
            r#"{"AA:BB:CC:DD:EE:FF":{"autoConnect":true}}"#,
            mac
        ));
    }

    #[test]
    fn auto_connect_pref_reads_each_device() {
        let prefs = r#"{"AA:BB:CC:DD:EE:FF":{"autoConnect":false},"11:22:33:44:55:66":{"autoConnect":true}}"#;
        assert!(!parse_auto_connect(prefs, "AA:BB:CC:DD:EE:FF"));
        assert!(parse_auto_connect(prefs, "11:22:33:44:55:66"));
    }

    #[test]
    fn take_back_after_call_defaults_off() {
        let mac = "AA:BB:CC:DD:EE:FF";
        assert!(!take_back_after_call_from_value(None, mac));
        for prefs in [
            serde_json::json!({}),
            serde_json::json!([]),
            serde_json::json!("garbage"),
            serde_json::json!({mac: "on"}),
            serde_json::json!({mac: {}}),
            serde_json::json!({mac: {"takeBackAfterCall": null}}),
            serde_json::json!({mac: {"takeBackAfterCall": "true"}}),
            serde_json::json!({mac: {"takeBackAfterCall": 1}}),
            serde_json::json!({mac: {"takeBackAfterCall": [true]}}),
            serde_json::json!({"takeBackAfterCall": true}),
        ] {
            assert!(
                !take_back_after_call_from_value(Some(&prefs), mac),
                "{prefs}"
            );
        }
    }

    #[test]
    fn take_back_after_call_reads_each_device() {
        let prefs = serde_json::json!({
            "switching": "handoff",
            "AA:BB:CC:DD:EE:FF": {"takeBackAfterCall": true, "connectMode": "when_last_connected"},
            "11:22:33:44:55:66": {"takeBackAfterCall": false},
        });
        assert!(take_back_after_call_from_value(
            Some(&prefs),
            "AA:BB:CC:DD:EE:FF"
        ));
        assert!(!take_back_after_call_from_value(
            Some(&prefs),
            "11:22:33:44:55:66"
        ));
        assert!(!take_back_after_call_from_value(
            Some(&prefs),
            "77:88:99:AA:BB:CC"
        ));
    }

    #[test]
    fn export_battery_missing_gives_true() {
        assert!(export_battery_from_value(None));
        assert!(export_battery_from_value(Some(&serde_json::json!({}))));
        assert!(export_battery_from_value(Some(
            &serde_json::json!({"exportBattery": null})
        )));
        assert!(parse_export_battery("{}"));
        assert!(parse_export_battery(""));
        assert!(parse_export_battery("not json"));
        assert!(parse_export_battery(r#"{"exportBattery":null}"#));
    }

    #[test]
    fn export_battery_bool_used_as_is() {
        assert!(!export_battery_from_value(Some(
            &serde_json::json!({"exportBattery": false})
        )));
        assert!(export_battery_from_value(Some(
            &serde_json::json!({"exportBattery": true})
        )));
        assert!(!parse_export_battery(r#"{"exportBattery":false}"#));
        assert!(parse_export_battery(r#"{"exportBattery":true}"#));
    }

    #[test]
    fn export_battery_wrong_type_gives_true() {
        for value in [
            r#""no""#,
            r#""false""#,
            "0",
            "1",
            "[]",
            r#"{"enabled":false}"#,
        ] {
            assert!(
                parse_export_battery(&format!(r#"{{"exportBattery":{value}}}"#)),
                "{value}"
            );
        }
    }

    #[test]
    fn export_battery_next_to_switching_and_auto_connect() {
        let prefs = r#"{"switching":"off","exportBattery":false,"AA:BB:CC:DD:EE:FF":{"autoConnect":false}}"#;
        assert!(!parse_export_battery(prefs));
        assert_eq!(parse_switching_mode(prefs), SwitchingMode::Off);
        assert!(!parse_auto_connect(prefs, "AA:BB:CC:DD:EE:FF"));
        assert!(parse_auto_connect(prefs, "11:22:33:44:55:66"));

        let prefs = r#"{"switching":"legacy","AA:BB:CC:DD:EE:FF":{"autoConnect":true}}"#;
        assert!(parse_export_battery(prefs));
        assert_eq!(parse_switching_mode(prefs), SwitchingMode::Legacy);
        assert!(parse_auto_connect(prefs, "AA:BB:CC:DD:EE:FF"));
    }

    #[test]
    fn switching_notifications_preference_defaults_on() {
        for prefs in [
            "{}",
            "",
            "not json",
            r#"{"switchingNotifications":null}"#,
            r#"{"switchingNotifications":"false"}"#,
            r#"{"switchingNotifications":0}"#,
            r#"{"switchingNotifications":true}"#,
            r#"{"AA:BB:CC:DD:EE:FF":{"switchingNotifications":false}}"#,
        ] {
            assert!(parse_switching_notifications(prefs), "{prefs}");
        }
        assert!(!parse_switching_notifications(
            r#"{"switching":"handoff","switchingNotifications":false}"#
        ));
    }

    #[test]
    fn preferred_codec_from_value_defaults_to_aac() {
        assert_eq!(preferred_codec_from_value(None), PreferredCodec::Aac);
        assert_eq!(
            preferred_codec_from_value(Some(&serde_json::json!({}))),
            PreferredCodec::Aac
        );
        assert_eq!(
            preferred_codec_from_value(Some(&serde_json::json!({"preferred_codec": null}))),
            PreferredCodec::Aac
        );
        assert_eq!(
            preferred_codec_from_value(Some(&serde_json::json!({"preferred_codec": "bogus"}))),
            PreferredCodec::Aac
        );
        assert_eq!(
            preferred_codec_from_value(Some(&serde_json::json!({"preferred_codec": "Sbc"}))),
            PreferredCodec::Sbc
        );
        assert_eq!(
            preferred_codec_from_value(Some(&serde_json::json!({"preferred_codec": "SbcXq"}))),
            PreferredCodec::SbcXq
        );
    }

    const MAC: &str = "AA:BB:CC:DD:EE:FF";

    #[test]
    fn connect_mode_defaults_to_automatically() {
        assert_eq!(ConnectMode::default(), ConnectMode::Automatically);
        assert_eq!(
            connect_mode_from_value(None, MAC),
            ConnectMode::Automatically
        );
        for prefs in [
            "",
            "not json",
            "[]",
            "{}",
            r#"{"AA:BB:CC:DD:EE:FF":{}}"#,
            r#"{"AA:BB:CC:DD:EE:FF":3}"#,
            r#"{"AA:BB:CC:DD:EE:FF":{"connectMode":null}}"#,
            r#"{"AA:BB:CC:DD:EE:FF":{"connectMode":"bogus"}}"#,
            r#"{"AA:BB:CC:DD:EE:FF":{"connectMode":"WhenLastConnected"}}"#,
            r#"{"AA:BB:CC:DD:EE:FF":{"connectMode":3}}"#,
            r#"{"AA:BB:CC:DD:EE:FF":{"connectMode":true}}"#,
            r#"{"AA:BB:CC:DD:EE:FF":{"connectMode":["when_last_connected"]}}"#,
            r#"{"AA:BB:CC:DD:EE:FF":{"connectMode":{"when_last_connected":null}}}"#,
            r#"{"AA:BB:CC:DD:EE:FF":{"connectMode":{"automatically":null}}}"#,
        ] {
            assert_eq!(
                parse_connect_mode(prefs, MAC),
                ConnectMode::Automatically,
                "{prefs}"
            );
        }
    }

    #[test]
    fn connect_mode_reads_each_value() {
        assert_eq!(
            parse_connect_mode(
                r#"{"AA:BB:CC:DD:EE:FF":{"connectMode":"automatically"}}"#,
                MAC
            ),
            ConnectMode::Automatically
        );
        assert_eq!(
            parse_connect_mode(
                r#"{"AA:BB:CC:DD:EE:FF":{"autoConnect":false,"connectMode":"when_last_connected"}}"#,
                MAC
            ),
            ConnectMode::WhenLastConnected
        );
        assert_eq!(
            connect_mode_from_value(
                Some(&serde_json::json!({MAC: {"connectMode": "when_last_connected"}})),
                MAC
            ),
            ConnectMode::WhenLastConnected
        );
    }

    #[test]
    fn connect_mode_is_per_device() {
        let prefs =
            r#"{"switching":"handoff","11:22:33:44:55:66":{"connectMode":"when_last_connected"}}"#;
        assert_eq!(parse_connect_mode(prefs, MAC), ConnectMode::Automatically);
        assert_eq!(
            parse_connect_mode(prefs, "11:22:33:44:55:66"),
            ConnectMode::WhenLastConnected
        );
        assert_eq!(
            parse_connect_mode(r#"{"connectMode":"when_last_connected"}"#, MAC),
            ConnectMode::Automatically
        );
        assert_eq!(
            parse_connect_mode(
                r#"{"connectMode":"when_last_connected","AA:BB:CC:DD:EE:FF":{"autoConnect":false}}"#,
                MAC
            ),
            ConnectMode::Automatically
        );
    }
}
