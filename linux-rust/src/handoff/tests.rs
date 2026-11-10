use super::Command::*;
use super::*;
use crate::a2dp_resume::A2DP_RESUME_WINDOW;
use crate::bluetooth::aacp::AudioSourceType;
use ConnectMode::{Automatically, WhenLastConnected};
use EarDetectionStatus::{InCase, InEar, OutOfEar};

mod calls;
mod ownership;
mod park;
mod playback;
mod session;
mod timers;

const LOCAL: &str = "01:02:03:04:05:06";
const REMOTE: &str = "07:08:09:0A:0B:0C";
const REMOTE_2: &str = "0D:0E:0F:10:11:12";
const REMOTE_3: &str = "13:14:15:16:17:18";

fn host(mac: &str) -> ConnectedDevice {
    ConnectedDevice {
        mac: mac.to_string(),
        info1: 0,
        info2: 0,
        r#type: None,
    }
}

fn two_hosts() -> Vec<ConnectedDevice> {
    vec![host(LOCAL), host(REMOTE)]
}

fn source(mac: &str, r#type: AudioSourceType) -> AudioSource {
    AudioSource {
        mac: mac.to_string(),
        r#type,
    }
}

fn idle() -> AudioSource {
    source("00:00:00:00:00:00", AudioSourceType::None)
}

fn ready_facts() -> SessionFacts {
    SessionFacts {
        hosts: two_hosts(),
        ears: vec![InEar, InEar],
        audio: Some(idle()),
        owner: Some(vec![0]),
    }
}

/// `ready_facts` with both buds out of ear: the connect take (#324)
/// needs a bud in, and the state is complete, so `Auto` sends its "not
/// owning" at once (#452) and a reconnect takes nothing.
/// `Probe::wear_in` then puts them in.
fn unworn_facts() -> SessionFacts {
    SessionFacts {
        ears: vec![OutOfEar, OutOfEar],
        ..ready_facts()
    }
}

/// A core driven with millisecond offsets from a fixed start, never
/// rewound (#383).
struct Probe {
    core: Handoff,
    t0: Instant,
    last_ms: u64,
}

impl Probe {
    fn new(automatic: bool, connected: bool) -> Self {
        let t0 = Instant::now();
        Probe {
            core: Handoff::new(LOCAL.to_string(), t0, automatic, connected),
            t0,
            last_ms: 0,
        }
    }

    /// Automatic, connected, Linux plus a remote host, worn, idle audio,
    /// and a first "stopped" sample as the baseline.
    fn ready() -> Self {
        Self::ready_with_hosts(two_hosts())
    }

    /// `ready`, with the AirPods reporting `hosts` instead.
    fn ready_with_hosts(hosts: Vec<ConnectedDevice>) -> Self {
        let mut probe = Self::new(true, true);
        probe.routing(SessionFacts {
            hosts,
            ..ready_facts()
        });
        assert_eq!(probe.play(10, false), []);
        probe
    }

    fn at(&self, ms: u64) -> Instant {
        self.t0 + Duration::from_millis(ms)
    }

    /// Both buds go in at `ms` after a session start made with
    /// `unworn_facts`, and the shell's ear resume played nothing (#313):
    /// no grace is left, and nothing is sent.
    fn wear_in(&mut self, ms: u64) {
        assert_eq!(self.feed(ms, Input::EarState(vec![InEar, InEar])), []);
        assert_eq!(self.feed(ms, Input::EarResume { resumed: false }), []);
    }

    fn feed(&mut self, ms: u64, input: Input) -> Vec<Command> {
        assert!(ms >= self.last_ms, "clock rewound: {ms} < {}", self.last_ms);
        self.last_ms = ms;
        self.core.handle(self.at(ms), input)
    }

    /// Ticks every 100 ms from `ms` through `ms` + 60 s send nothing:
    /// Linux stays the idle owner (#262).
    fn idle_ticks(&mut self, ms: u64) {
        for ms in (ms..=ms + 60_000).step_by(100) {
            assert_eq!(self.feed(ms, Input::Tick), [], "{ms}");
        }
    }

    /// A sample at `ms`; `playing` is a counted stream (201).
    fn play(&mut self, ms: u64, playing: bool) -> Vec<Command> {
        self.play_category(ms, if playing { STREAM } else { IDLE })
    }

    /// A stopped sample taken at `sampled_ms` but fed at `ms`.
    fn stale_stop(&mut self, ms: u64, sampled_ms: u64) -> Vec<Command> {
        let sampled_at = self.at(sampled_ms);
        self.feed(
            ms,
            Input::Playback {
                sampled_at,
                category: IDLE,
                new_source: None,
            },
        )
    }

    fn play_category(&mut self, ms: u64, category: u32) -> Vec<Command> {
        self.play_source(ms, category, None)
    }

    /// A sample at `ms` that also reports a new local source (#96).
    fn play_source(&mut self, ms: u64, category: u32, new_source: Option<u32>) -> Vec<Command> {
        let sampled_at = self.at(ms);
        self.feed(
            ms,
            Input::Playback {
                sampled_at,
                category,
                new_source,
            },
        )
    }

    /// Feed routing reports as AACP events; the commands they cause
    /// (announcing a new host) are not under test here.
    fn routing(&mut self, facts: SessionFacts) {
        self.feed(
            0,
            Input::ConnectedDevices {
                old: Vec::new(),
                new: facts.hosts,
            },
        );
        self.feed(0, Input::EarState(facts.ears));
        if let Some(audio) = facts.audio {
            self.feed(0, Input::AudioSource(audio));
        }
        if let Some(owner) = facts.owner {
            self.feed(0, Input::Ownership(owner));
        }
    }
}

/// Linux owns the AirPods without a take of its own, so `Auto` at 100 ms
/// sends a tracked "not owning" (#54); a stopped baseline sample at 200.
/// The buds go in only after `Auto`, so the connect take (#324) doesn't
/// keep the ownership instead.
fn auto_release() -> Probe {
    let mut p = Probe::new(false, true);
    p.routing(SessionFacts {
        owner: Some(vec![1]),
        ..unworn_facts()
    });
    assert_eq!(
        p.feed(100, Input::Auto),
        [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
    );
    p.wear_in(150);
    assert_eq!(p.play(200, false), []);
    p
}

/// Automatic, worn, case closed, disconnected, then new playback that
/// reconnects and takes at 1200 ms: the take's [1] is pending.
fn reconnect_take() -> Probe {
    let mut p = Probe::ready();
    p.feed(20, Input::EarState(vec![InCase, InCase]));
    assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
    assert_eq!(p.play(500, false), []);
    assert_eq!(p.play(1000, true), []);
    assert_eq!(p.feed(1100, Input::Tick), [Reconnect { audio: true }]);
    assert_eq!(
        p.feed(1200, Input::Reconnected(ready_facts())),
        [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
    );
    p
}

fn count_lines(lines: &[String], prefix: &str) -> usize {
    lines.iter().filter(|l| l.starts_with(prefix)).count()
}

/// Automatic take for playback, acknowledged, playback still running at
/// 1000 ms.
fn acked_auto_take() -> Probe {
    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    assert_eq!(p.play(1000, true), []);
    p
}

fn report(old: &[&str], new: &[&str]) -> Input {
    let hosts = |macs: &[&str]| macs.iter().map(|m| host(m)).collect();
    Input::ConnectedDevices {
        old: hosts(old),
        new: hosts(new),
    }
}

const L: &str = LOCAL;
const R: &str = REMOTE;
const R2: &str = REMOTE_2;

fn idle_transport() -> Input {
    Input::A2dpTransport { active: false }
}

/// `Probe::ready` with default sink routing on.
fn routed() -> Probe {
    let mut p = Probe::ready();
    p.core.default_sink_routing = true;
    p
}

fn remote_media() -> Input {
    Input::AudioSource(source(REMOTE, AudioSourceType::Media))
}

fn holds(out: &[Command]) -> Vec<Command> {
    out.iter()
        .filter(|c| matches!(c, HoldA2dp(_)))
        .cloned()
        .collect()
}

/// The daemon's start with `hold` and routing on: seed facts, then the
/// session start's `Auto`, its release acknowledged, then a "stopped"
/// baseline sample. The buds go in only after `Auto`, so the connect
/// take (#324) doesn't replace that release.
fn hold_ready(hold: bool) -> Probe {
    let mut p = Probe::new(false, true);
    p.core.a2dp_hold = hold;
    p.core.default_sink_routing = true;
    p.routing(unworn_facts());
    let start = p.feed(5, Input::Auto);
    assert_eq!(
        holds(&start),
        if hold { vec![HoldA2dp(false)] } else { vec![] }
    );
    assert_eq!(p.feed(8, Input::Ownership(vec![0])), []);
    p.wear_in(9);
    assert_eq!(p.play(10, false), []);
    p
}

// #272: audio categories.
const IDLE: u32 = AUDIO_CATEGORY_IDLE;
const STREAM: u32 = crate::media_controller::AUDIO_CATEGORY_STREAM;
const MEDIA: u32 = crate::media_controller::AUDIO_CATEGORY_MEDIA;

fn relay(host: &str, category: u32) -> Input {
    Input::MediaInformation {
        host: host.to_string(),
        category,
    }
}

/// `Probe::ready` with REMOTE reported playing media, and its relay of
/// `relayed` if any.
fn remote_playing(relayed: Option<u32>) -> Probe {
    let mut p = Probe::ready();
    assert_eq!(p.feed(20, remote_media()), []);
    if let Some(category) = relayed {
        assert_eq!(p.feed(30, relay(REMOTE, category)), []);
    }
    p.core.take_lines();
    p
}

/// `Probe::ready`, relaying its category.
fn relaying() -> Probe {
    let mut p = Probe::ready();
    p.core.category_relay = true;
    p
}

fn takes(all: &[Command]) -> usize {
    all.iter().filter(|c| **c == SendOwnership(true)).count()
}
