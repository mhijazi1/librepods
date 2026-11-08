//! Pure local-call detection (#11).
//!
//! Decides from audio stream property sets and the AirPods card's active
//! profile whether Linux is in a call. It does no I/O: `stream_monitor.rs`
//! copies the stream and card state out of the sound server and feeds it
//! here, so every rule is tested offline.
//!
//! A call is either
//! - a capture stream (a microphone recording) that is not a monitor of a
//!   sink and whose `media.role` is `Communication` or `phone` (any case),
//!   as WebRTC browsers, Discord, Zoom and the like tag their call input; or
//! - the AirPods card on a head-unit (HFP/HSP) profile: PipeWire's
//!   `headset-head-unit*` or PulseAudio's `headset_head_unit` /
//!   `handsfree_head_unit`.

use std::collections::BTreeMap;

/// Which way a stream carries audio.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StreamDirection {
    /// A sink input: an application playing audio. Not constructed by the
    /// call monitor, which lists capture streams only; #10 classifies
    /// playback streams with the same type.
    #[allow(dead_code)]
    Playback,
    /// A source output: an application recording audio.
    Capture,
}

/// One stream's properties as the sound server reports them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StreamProps {
    pub(crate) direction: StreamDirection,
    /// The stream's property list (`media.role`, `application.name`, ...).
    pub(crate) props: BTreeMap<String, String>,
    /// The stream records a sink's monitor source, not a microphone.
    pub(crate) monitor: bool,
}

/// Property values that mark a capture stream as a sink monitor.
const MONITOR_PROPS: [&str; 2] = ["stream.monitor", "stream.capture.sink"];

/// `media.role` values of call audio.
const CALL_ROLES: [&str; 2] = ["communication", "phone"];

/// Head-unit (HFP/HSP) profile name prefixes: PipeWire's
/// `headset-head-unit`, `-msbc` and `-cvsd`, and PulseAudio's names.
const CALL_PROFILE_PREFIXES: [&str; 3] = [
    "headset-head-unit",
    "headset_head_unit",
    "handsfree_head_unit",
];

fn prop_is_true(props: &BTreeMap<String, String>, key: &str) -> bool {
    props
        .get(key)
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("true"))
}

/// Whether the stream is call input: a capture stream that is not a monitor
/// and whose `media.role` is Communication or phone.
pub(crate) fn is_call_stream(stream: &StreamProps) -> bool {
    if stream.direction != StreamDirection::Capture {
        return false;
    }
    if stream.monitor || MONITOR_PROPS.iter().any(|k| prop_is_true(&stream.props, k)) {
        return false;
    }
    stream.props.get("media.role").is_some_and(|role| {
        let role = role.trim();
        CALL_ROLES.iter().any(|c| role.eq_ignore_ascii_case(c))
    })
}

/// Whether a card profile is a head-unit (call) profile.
pub(crate) fn is_call_profile(profile: Option<&str>) -> bool {
    profile.is_some_and(|p| CALL_PROFILE_PREFIXES.iter().any(|pre| p.starts_with(pre)))
}

/// The active profile of the AirPods card, found by its
/// `bluez_card.<MAC with _>` name (case-insensitive). None when there is no
/// such card or it has no active profile.
pub(crate) fn airpods_card_profile<'a>(
    cards: &'a [(String, Option<String>)],
    airpods_mac: &str,
) -> Option<&'a str> {
    let name = format!("bluez_card.{}", airpods_mac.replace(':', "_"));
    cards
        .iter()
        .find(|(card, _)| card.eq_ignore_ascii_case(&name))
        .and_then(|(_, profile)| profile.as_deref())
}

/// Whether Linux is in a call: any call stream, or the AirPods card on a
/// head-unit profile.
pub(crate) fn call_active(streams: &[StreamProps], airpods_profile: Option<&str>) -> bool {
    is_call_profile(airpods_profile) || streams.iter().any(is_call_stream)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAC: &str = "AA:BB:CC:DD:EE:FF";

    fn stream(direction: StreamDirection, props: &[(&str, &str)], monitor: bool) -> StreamProps {
        StreamProps {
            direction,
            props: props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            monitor,
        }
    }

    fn capture(props: &[(&str, &str)]) -> StreamProps {
        stream(StreamDirection::Capture, props, false)
    }

    fn cards(entries: &[(&str, Option<&str>)]) -> Vec<(String, Option<String>)> {
        entries
            .iter()
            .map(|(name, profile)| (name.to_string(), profile.map(str::to_string)))
            .collect()
    }

    #[test]
    fn capture_with_call_role_is_a_call() {
        for role in [
            "Communication",
            "communication",
            "COMMUNICATION",
            "phone",
            "Phone",
            "PHONE",
        ] {
            let s = capture(&[("media.role", role), ("application.name", "Firefox")]);
            assert!(is_call_stream(&s), "{role}");
            assert!(call_active(&[s], None), "{role}");
        }
    }

    #[test]
    fn recorder_without_role_is_not_a_call() {
        let s = capture(&[("application.name", "OBS")]);
        assert!(!is_call_stream(&s));
        assert!(!call_active(&[s], None));
    }

    #[test]
    fn capture_with_music_role_is_not_a_call() {
        for role in ["Music", "video", "game", ""] {
            let s = capture(&[("media.role", role)]);
            assert!(!is_call_stream(&s), "{role}");
        }
    }

    #[test]
    fn monitor_capture_is_not_a_call() {
        // The monitor flag (source is a sink's monitor).
        let s = stream(
            StreamDirection::Capture,
            &[("media.role", "Communication")],
            true,
        );
        assert!(!is_call_stream(&s));
        // PipeWire's stream properties.
        for key in ["stream.monitor", "stream.capture.sink"] {
            for value in ["true", "TRUE", "True"] {
                let s = capture(&[("media.role", "Communication"), (key, value)]);
                assert!(!is_call_stream(&s), "{key}={value}");
            }
            // An explicit false is not a monitor.
            let s = capture(&[("media.role", "Communication"), (key, "false")]);
            assert!(is_call_stream(&s), "{key}=false");
        }
        assert!(!call_active(
            &[stream(
                StreamDirection::Capture,
                &[("media.role", "phone")],
                true
            )],
            None
        ));
    }

    #[test]
    fn playback_with_phone_role_is_not_a_call() {
        let s = stream(StreamDirection::Playback, &[("media.role", "phone")], false);
        assert!(!is_call_stream(&s));
        let s = stream(
            StreamDirection::Playback,
            &[("media.role", "Communication")],
            false,
        );
        assert!(!is_call_stream(&s));
        assert!(!call_active(&[s], Some("a2dp-sink")));
    }

    #[test]
    fn head_unit_profiles_are_calls() {
        for profile in [
            "headset-head-unit",
            "headset-head-unit-msbc",
            "headset-head-unit-cvsd",
            "headset_head_unit",
            "handsfree_head_unit",
        ] {
            assert!(is_call_profile(Some(profile)), "{profile}");
            assert!(call_active(&[], Some(profile)), "{profile}");
        }
    }

    #[test]
    fn other_profiles_are_not_calls() {
        for profile in [
            Some("a2dp-sink"),
            Some("a2dp-sink-aac"),
            Some("a2dp_sink"),
            Some("off"),
            Some(""),
            None,
        ] {
            assert!(!is_call_profile(profile), "{profile:?}");
            assert!(!call_active(&[], profile), "{profile:?}");
        }
    }

    #[test]
    fn airpods_card_profile_matches_mac_case_insensitively() {
        let list = cards(&[
            ("alsa_card.pci-0000_00_1f.3", Some("output:analog-stereo")),
            ("bluez_card.aa_bb_cc_dd_ee_ff", Some("headset-head-unit")),
        ]);
        assert_eq!(airpods_card_profile(&list, MAC), Some("headset-head-unit"));
        assert_eq!(
            airpods_card_profile(&list, "aa:bb:cc:dd:ee:ff"),
            Some("headset-head-unit")
        );
        let upper = cards(&[("BLUEZ_CARD.AA_BB_CC_DD_EE_FF", Some("a2dp-sink"))]);
        assert_eq!(airpods_card_profile(&upper, MAC), Some("a2dp-sink"));
        // A card with no active profile, and no card at all.
        let none = cards(&[("bluez_card.AA_BB_CC_DD_EE_FF", None)]);
        assert_eq!(airpods_card_profile(&none, MAC), None);
        assert_eq!(airpods_card_profile(&[], MAC), None);
    }

    #[test]
    fn head_unit_on_another_card_is_not_a_call() {
        let list = cards(&[
            ("bluez_card.11_22_33_44_55_66", Some("headset-head-unit")),
            ("bluez_card.AA_BB_CC_DD_EE_FF", Some("a2dp-sink")),
        ]);
        let profile = airpods_card_profile(&list, MAC);
        assert_eq!(profile, Some("a2dp-sink"));
        assert!(!call_active(&[], profile));
        // Only the other device's card is present.
        let other = cards(&[("bluez_card.11_22_33_44_55_66", Some("headset-head-unit"))]);
        assert_eq!(airpods_card_profile(&other, MAC), None);
        assert!(!call_active(&[], airpods_card_profile(&other, MAC)));
    }

    #[test]
    fn any_call_stream_among_others_is_a_call() {
        let streams = [
            capture(&[("application.name", "recorder")]),
            stream(StreamDirection::Playback, &[("media.role", "Music")], false),
            capture(&[("media.role", "Communication")]),
        ];
        assert!(call_active(&streams, Some("a2dp-sink")));
        assert!(!call_active(&streams[..2], Some("a2dp-sink")));
    }
}
