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
//!
//! It also decides which playback streams (sink inputs) are worth taking the
//! AirPods for (#10): `is_takeover_worthy`. `stream_monitor::PlaybackTracker`
//! debounces those streams before the handoff core sees them as playback.

use std::collections::BTreeMap;

/// Which way a stream carries audio.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StreamDirection {
    /// A sink input: an application playing audio.
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
    /// The sound server's index of the stream (sink input or source
    /// output); it identifies the stream across snapshots.
    pub(crate) index: u32,
    /// The application has corked (paused) the stream.
    pub(crate) corked: bool,
    /// Muted, or every channel at zero volume (playback only; call
    /// detection ignores it).
    pub(crate) muted: bool,
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

/// `media.role` values of media worth taking the AirPods for.
const MEDIA_ROLES: [&str; 4] = ["music", "movie", "video", "game"];

/// Properties naming the node a stream is routed to.
const TARGET_PROPS: [&str; 2] = ["target.object", "node.target"];

/// LibrePods' own `application.name`.
const OWN_APPLICATION: &str = "LibrePods";

/// A property's trimmed value, None when it is absent or blank.
fn prop<'a>(props: &'a BTreeMap<String, String>, key: &str) -> Option<&'a str> {
    props.get(key).map(|v| v.trim()).filter(|v| !v.is_empty())
}

/// Whether a playback stream (a sink input) is media worth taking the
/// AirPods for, judged from its properties alone; `stream_monitor` adds the
/// corked state and the minimum duration. Yes: `media.role` Music, Movie,
/// Video or Game (any case), or no `media.role` but an `application.name`
/// (games and browser tabs often set no role). No: capture streams, event
/// and notification sounds (`media.role` event/notification, or an
/// `event.id`), monitor streams (`stream.monitor`, or a target ending in
/// `.monitor`), virtual streams (`node.virtual` true: PipeWire plumbing such
/// as loopback, filter-chain and combine, whatever their role or application
/// name), LibrePods' own streams, call audio (`media.role` phone or
/// Communication; taking the AirPods for calls is #12), any other role, and
/// anonymous streams with neither a role nor an application name.
pub(crate) fn is_takeover_worthy(stream: &StreamProps) -> bool {
    is_takeover_worthy_for(stream, std::process::id())
}

/// `is_takeover_worthy`, with LibrePods' process id given.
fn is_takeover_worthy_for(stream: &StreamProps, own_pid: u32) -> bool {
    if stream.direction != StreamDirection::Playback {
        return false;
    }
    let props = &stream.props;
    if stream.monitor || prop_is_true(props, "stream.monitor") {
        return false;
    }
    // Plumbing (loopback, filter-chain, combine); real clients never set it (#97).
    if prop_is_true(props, "node.virtual") {
        return false;
    }
    if TARGET_PROPS
        .iter()
        .filter_map(|key| prop(props, key))
        .any(|target| target.ends_with(".monitor"))
    {
        return false;
    }
    if prop(props, "event.id").is_some() {
        return false;
    }
    let application = prop(props, "application.name");
    let own_pid = own_pid.to_string();
    if prop(props, "application.process.id") == Some(own_pid.as_str())
        || application.is_some_and(|name| name.eq_ignore_ascii_case(OWN_APPLICATION))
    {
        return false;
    }
    match prop(props, "media.role") {
        Some(role) => MEDIA_ROLES.iter().any(|r| role.eq_ignore_ascii_case(r)),
        None => application.is_some(),
    }
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
            index: 0,
            corked: false,
            muted: false,
        }
    }

    fn capture(props: &[(&str, &str)]) -> StreamProps {
        stream(StreamDirection::Capture, props, false)
    }

    fn playback(props: &[(&str, &str)]) -> StreamProps {
        stream(StreamDirection::Playback, props, false)
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
    fn peak_stream_is_not_call_input() {
        // stream_monitor's PulsePeaks record stream (#88): a monitor
        // capture with no role. PipeWire lists it on no monitor source (it
        // links to the sink input directly), so neither flag may matter.
        for monitor in [true, false] {
            let s = stream(
                StreamDirection::Capture,
                &[
                    ("application.name", "LibrePods"),
                    ("media.name", "LibrePods"),
                ],
                monitor,
            );
            assert!(!is_call_stream(&s), "monitor={monitor}");
            assert!(!call_active(&[s], None), "monitor={monitor}");
        }
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

    #[test]
    fn takeover_worthy_media_roles() {
        for role in [
            "Music", "music", "MUSIC", "Movie", "movie", "Video", "VIDEO", "Game", "game",
        ] {
            assert!(
                is_takeover_worthy(&playback(&[("media.role", role)])),
                "{role}"
            );
            assert!(
                is_takeover_worthy(&playback(&[
                    ("media.role", role),
                    ("application.name", "Firefox"),
                ])),
                "{role} with an application"
            );
        }
        // No role, but an application name: a game or a browser tab.
        assert!(is_takeover_worthy(&playback(&[(
            "application.name",
            "Steam"
        )])));
        assert!(is_takeover_worthy(&playback(&[
            ("media.role", ""),
            ("application.name", "Chromium"),
        ])));
        // A corked stream is still media; stream_monitor handles corking.
        let mut corked = playback(&[("media.role", "Music")]);
        corked.corked = true;
        assert!(is_takeover_worthy(&corked));
    }

    #[test]
    fn takeover_worthy_rejects_event_sounds() {
        for role in ["event", "Event", "EVENT", "notification", "Notification"] {
            assert!(
                !is_takeover_worthy(&playback(&[
                    ("media.role", role),
                    ("application.name", "GNOME Shell"),
                ])),
                "{role}"
            );
        }
        // An event id marks an event sound whatever its role.
        assert!(!is_takeover_worthy(&playback(&[
            ("event.id", "message-new-instant"),
            ("application.name", "Discord"),
        ])));
        assert!(!is_takeover_worthy(&playback(&[
            ("event.id", "bell"),
            ("media.role", "Music"),
        ])));
        // Other roles are not media either.
        for role in ["a11y", "test", "production", "animation"] {
            assert!(
                !is_takeover_worthy(&playback(&[
                    ("media.role", role),
                    ("application.name", "app"),
                ])),
                "{role}"
            );
        }
        // Neither a role nor an application name.
        assert!(!is_takeover_worthy(&playback(&[])));
        assert!(!is_takeover_worthy(&playback(&[("application.name", " ")])));
        assert!(!is_takeover_worthy(&playback(&[(
            "media.name",
            "Playback"
        )])));
    }

    #[test]
    fn takeover_worthy_rejects_monitors() {
        // The monitor flag and PipeWire's stream.monitor property.
        assert!(!is_takeover_worthy(&stream(
            StreamDirection::Playback,
            &[("media.role", "Music")],
            true
        )));
        for value in ["true", "TRUE", "True"] {
            assert!(
                !is_takeover_worthy(&playback(&[
                    ("media.role", "Music"),
                    ("stream.monitor", value),
                ])),
                "stream.monitor={value}"
            );
        }
        assert!(is_takeover_worthy(&playback(&[
            ("media.role", "Music"),
            ("stream.monitor", "false"),
        ])));
        // A stream routed to a monitor.
        for key in ["target.object", "node.target"] {
            assert!(
                !is_takeover_worthy(&playback(&[
                    ("application.name", "pw-loopback"),
                    (key, "alsa_output.pci-0000_00_1f.3.analog-stereo.monitor"),
                ])),
                "{key}"
            );
            // Any other target is fine.
            assert!(
                is_takeover_worthy(&playback(&[
                    ("application.name", "mpv"),
                    (key, "alsa_output.pci-0000_00_1f.3.analog-stereo"),
                ])),
                "{key} to a sink"
            );
        }
    }

    /// The live pw-loopback playback stream from #97, minus node.virtual.
    const LOOPBACK: [(&str, &str); 5] = [
        ("node.name", "output.pw-loopback-3412"),
        ("node.group", "pw-loopback-3412"),
        ("node.link-group", "pw-loopback-3412"),
        ("stream.is-live", "true"),
        (
            "target.object",
            "alsa_output.pci-0000_00_1f.3.analog-stereo",
        ),
    ];

    /// `LOOPBACK` plus `extra`.
    fn loopback(extra: &[(&str, &str)]) -> StreamProps {
        playback(&[&LOOPBACK[..], extra].concat())
    }

    #[test]
    fn takeover_worthy_rejects_virtual_with_application() {
        let app = ("application.name", "X");
        // Without node.virtual the same stream is worthy: node.virtual alone rejects it.
        assert!(is_takeover_worthy(&loopback(&[app])));
        for value in ["true", "TRUE", " true "] {
            assert!(
                !is_takeover_worthy(&loopback(&[app, ("node.virtual", value)])),
                "node.virtual={value:?}"
            );
        }
    }

    #[test]
    fn takeover_worthy_rejects_virtual_with_media_role() {
        for role in ["Music", "music", "MUSIC"] {
            let role = ("media.role", role);
            let app = ("application.name", "X");
            assert!(is_takeover_worthy(&loopback(&[role])), "{role:?}");
            assert!(is_takeover_worthy(&loopback(&[role, app])), "{role:?}");
            let virt = ("node.virtual", "true");
            assert!(!is_takeover_worthy(&loopback(&[role, virt])), "{role:?}");
            assert!(
                !is_takeover_worthy(&loopback(&[role, app, virt])),
                "{role:?} with an application"
            );
        }
    }

    #[test]
    fn takeover_worthy_keeps_real_clients() {
        let firefox = [
            ("application.name", "Firefox"),
            ("application.process.binary", "firefox"),
            ("media.name", "AudioStream"),
        ];
        let sdl = [("media.role", "Game"), ("application.name", "SuperTuxKart")];
        let mpv_movie = [("application.name", "mpv"), ("media.role", "Movie")];
        let mpv_music = [("application.name", "mpv"), ("media.role", "Music")];
        for props in [&firefox[..], &sdl, &mpv_movie, &mpv_music] {
            assert!(is_takeover_worthy(&playback(props)), "{props:?}");
            // A false or blank node.virtual is not virtual.
            for value in ["false", "", "  "] {
                let props = [props, &[("node.virtual", value)]].concat();
                assert!(is_takeover_worthy(&playback(&props)), "{props:?}");
            }
        }
    }

    #[test]
    fn takeover_worthy_rejects_own_and_call_streams() {
        const OWN_PID: u32 = 4242;
        // LibrePods' own process, and its application name in any case.
        let own = playback(&[
            ("media.role", "Music"),
            ("application.name", "mpv"),
            ("application.process.id", "4242"),
        ]);
        assert!(!is_takeover_worthy_for(&own, OWN_PID));
        assert!(is_takeover_worthy_for(&own, OWN_PID + 1));
        let me = std::process::id().to_string();
        assert!(!is_takeover_worthy(&playback(&[
            ("application.name", "mpv"),
            ("application.process.id", me.as_str()),
        ])));
        for name in ["LibrePods", "librepods", "LIBREPODS"] {
            assert!(
                !is_takeover_worthy_for(
                    &playback(&[("media.role", "Music"), ("application.name", name)]),
                    OWN_PID
                ),
                "{name}"
            );
        }
        // Call audio is #12's.
        for role in ["phone", "Phone", "Communication", "communication"] {
            assert!(
                !is_takeover_worthy(&playback(&[
                    ("media.role", role),
                    ("application.name", "Firefox"),
                ])),
                "{role}"
            );
        }
        // Capture streams never are, whatever their role.
        for role in ["Music", "Game", "Communication"] {
            assert!(
                !is_takeover_worthy(&capture(&[
                    ("media.role", role),
                    ("application.name", "OBS"),
                ])),
                "{role}"
            );
        }
        assert!(!is_takeover_worthy(&capture(&[(
            "application.name",
            "OBS"
        )])));
    }
}
