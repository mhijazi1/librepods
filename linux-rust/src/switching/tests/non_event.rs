use super::*;
use crate::stream_monitor::PlaybackTracker;
use Command::{PauseMedia, SendOwnership};
use EarDetectionStatus::{InCase, InEar, OutOfEar};
use libpulse_binding::proplist::Proplist;
use libpulse_binding::volume::{ChannelVolumes, Volume};

const POLL_MS: u64 = PLAYBACK_POLL.as_millis() as u64;
const MIN_MS: u64 = MIN_STREAM_PLAYBACK.as_millis() as u64;
/// Peak levels of a stream carrying sound and of a silent one (#88).
const LOUD: f32 = 0.5;
const SILENT: f32 = 0.0;
/// When each scenario's event starts, after the baselines at 10 ms.
const EVENT_MS: u64 = 500;
/// Neither takes nor pauses.
const NO_TAKE: Vec<Vec<Command>> = Vec::new();

fn facts() -> SessionFacts {
    SessionFacts {
        hosts: vec![host(LOCAL), host(REMOTE)],
        ears: vec![InEar, InEar],
        audio: Some(AudioSource {
            mac: "00:00:00:00:00:00".to_string(),
            r#type: AudioSourceType::None,
        }),
        owner: Some(vec![0]),
    }
}

/// `facts` with an `Unknown` audio source report: no connect take
/// (#324) and a complete state (#452), so Linux does not own when
/// the session starts.
fn no_source() -> SessionFacts {
    SessionFacts {
        audio: Some(AudioSource {
            mac: "00:00:00:00:00:00".to_string(),
            r#type: AudioSourceType::Unknown,
        }),
        ..facts()
    }
}

/// The idle audio source report `no_source` holds back.
fn idle_source() -> Input {
    Input::AudioSource(facts().audio.unwrap())
}

/// The core fed as the default path feeds it: each playback sample
/// combines the MPRIS owners, the listing's live streams and the
/// real `PlaybackTracker`'s state with `sample_sources`, as
/// `spawn_playback_poller_with` does.
struct Pipeline {
    core: Handoff,
    tracker: PlaybackTracker,
    streams_playing: bool,
    /// The poller's sources of the previous sample (#96).
    last: Option<Sources>,
    /// The stream monitor is dead: samples' live set is unknown (#318).
    monitor_dead: bool,
    t0: Instant,
    /// Every command list the core returned, in order.
    log: Vec<Vec<Command>>,
}

impl Pipeline {
    /// `handoff::tests::Probe::ready`: automatic, connected, Linux
    /// plus a remote host, worn, idle audio, owner [0], then a
    /// stopped sample and an empty stream listing as the baselines.
    fn ready() -> Self {
        Self::seeded(facts())
    }

    /// `ready` with the session's `facts` instead.
    fn seeded(facts: SessionFacts) -> Self {
        let t0 = Instant::now();
        let mut p = Pipeline {
            core: Handoff::new(LOCAL.to_string(), t0, true, true),
            tracker: PlaybackTracker::new(MIN_STREAM_PLAYBACK),
            streams_playing: false,
            last: None,
            monitor_dead: false,
            t0,
            log: Vec::new(),
        };
        for input in seed_inputs(facts) {
            p.feed(0, input);
        }
        assert_eq!(p.step(10, false, &[]), []);
        p
    }

    fn feed(&mut self, ms: u64, input: Input) -> Vec<Command> {
        let now = self.t0 + Duration::from_millis(ms);
        let commands = self.core.handle(now, input);
        // As the shells' `step` (#90).
        if commands.contains(&Command::DrainPlayback) {
            self.tracker.rebaseline(now);
        }
        self.log.push(commands.clone());
        commands
    }

    /// One playback sample at `ms` with every candidate audible.
    fn step(&mut self, ms: u64, mpris: bool, streams: &[StreamProps]) -> Vec<Command> {
        self.step_levels(ms, mpris, streams, LOUD)
    }

    /// One playback sample at `ms`: the stream listing with every
    /// candidate's peak `level`, then the core.
    fn step_levels(
        &mut self,
        ms: u64,
        mpris: bool,
        streams: &[StreamProps],
        level: f32,
    ) -> Vec<Command> {
        self.sample(ms, &player(mpris), streams, |_| level)
    }

    /// One playback sample at `ms`: the stream listing with each
    /// candidate's peak `level`, the Playing players' bus `owners`,
    /// combined by `sample_sources`, then the core.
    fn sample(
        &mut self,
        ms: u64,
        owners: &[Option<u32>],
        streams: &[StreamProps],
        level: impl Fn(u32) -> f32,
    ) -> Vec<Command> {
        let now = self.t0 + Duration::from_millis(ms);
        if let Some(playing) = self.tracker.observe_levels(now, streams, level) {
            self.streams_playing = playing;
        }
        let owned = stream_monitor::stream_owners(streams, |_| None);
        let state = PlaybackState {
            counted: self.streams_playing,
            baseline: false,
            live: (!self.monitor_dead).then(|| owned.values().flatten().copied().collect()),
        };
        let started = self.tracker.started(&owned);
        let (category, new_source) = sample_sources(owners, &(state, started), &mut self.last);
        self.feed(
            ms,
            Input::Playback {
                sampled_at: now,
                category,
                new_source,
            },
        )
    }

    /// A sample every `PLAYBACK_POLL` from `EVENT_MS` until over
    /// twice the minimum stream playback later.
    fn event(&mut self, mpris: bool, streams: &[StreamProps]) {
        self.event_levels(mpris, streams, |_| LOUD);
    }

    /// `event`, with the candidates' peak level at each sample's ms.
    fn event_levels(&mut self, mpris: bool, streams: &[StreamProps], level: impl Fn(u64) -> f32) {
        for ms in (EVENT_MS..=EVENT_MS + 2 * MIN_MS + POLL_MS).step_by(POLL_MS as usize) {
            self.step_levels(ms, mpris, streams, level(ms));
        }
    }

    /// The command lists that take the AirPods or pause anything.
    fn takes(&self) -> Vec<Vec<Command>> {
        self.log
            .iter()
            .filter(|c| c.contains(&SendOwnership(true)) || c.contains(&PauseMedia))
            .cloned()
            .collect()
    }
}

/// `ready`, then `mpris` and `streams` from the event on.
fn run(mpris: bool, streams: &[StreamProps]) -> Pipeline {
    let mut p = Pipeline::ready();
    p.event(mpris, streams);
    p
}

/// `ready`, then `streams` with no MPRIS player from the event on,
/// at the peak level `level` gives for each sample's ms.
fn run_levels(streams: &[StreamProps], level: impl Fn(u64) -> f32) -> Pipeline {
    let mut p = Pipeline::ready();
    p.event_levels(false, streams, level);
    p
}

/// Sink input 1, uncorked and unmuted, with these properties only.
fn sink(props: &[(&str, &str)]) -> StreamProps {
    StreamProps {
        props: props
            .iter()
            .map(|&(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ..music(1)
    }
}

#[test]
fn non_event_system_sound() {
    assert_eq!(run(false, &[event_sound(1)]).takes(), NO_TAKE);
    assert_eq!(run(false, &[music(1)]).takes(), [vec![SendOwnership(true)]]);
}

#[test]
fn non_event_notification_sound() {
    let canberra = sink(&[
        ("application.name", "test player"),
        ("event.id", "message-new-instant"),
    ]);
    for stream in [sink_input(1, "notification"), canberra] {
        assert_eq!(run(false, &[stream.clone()]).takes(), NO_TAKE, "{stream:?}");
    }
    let app_only = sink(&[("application.name", "test player")]);
    assert_eq!(run(false, &[app_only]).takes(), [vec![SendOwnership(true)]]);
}

const DISCONNECT_MS: u64 = EVENT_MS;
/// The music stream of the reconnect case has played over the
/// minimum by now, and started over the minimum earlier (#90).
const RECONNECT_MS: u64 = DISCONNECT_MS + POLL_MS + MIN_MS + POLL_MS;

/// The session drops while worn (recovery Immediate) and a new one
/// comes at `RECONNECT_MS`, before its audio source report, so with
/// no connect take (#324); the player (a music stream, with MPRIS
/// when `mpris`) plays from `start` ms on.
fn reconnect_case(start: u64, mpris: bool) -> Pipeline {
    let mut p = Pipeline::ready();
    assert_eq!(
        p.feed(DISCONNECT_MS, Input::Disconnected),
        [Command::DropSession]
    );
    let player = [music(1)];
    let end = RECONNECT_MS + 2 * MIN_MS + POLL_MS;
    for ms in (DISCONNECT_MS + POLL_MS..=end).step_by(POLL_MS as usize) {
        if ms == DISCONNECT_MS + 2 * POLL_MS {
            assert_eq!(
                p.feed(ms, Input::Tick),
                [Command::Reconnect { audio: true }]
            );
        }
        if ms == RECONNECT_MS {
            // The tracker counts a stream that lasted the minimum.
            assert_eq!(p.streams_playing, start + MIN_MS < ms);
            p.feed(ms, Input::Reconnected(no_source()));
            assert_eq!(p.feed(ms, idle_source()), []);
        }
        let playing = ms >= start;
        let streams: &[StreamProps] = if playing { &player } else { &[] };
        p.step(ms, mpris && playing, streams);
    }
    p
}

#[test]
fn non_event_playback_already_running_on_reconnect() {
    assert_eq!(
        reconnect_case(DISCONNECT_MS + POLL_MS, true).takes(),
        NO_TAKE
    );
    assert_eq!(
        reconnect_case(RECONNECT_MS + POLL_MS, true).takes(),
        [vec![SendOwnership(true)]]
    );
}

/// #90: a stream (no MPRIS) still in its debounce at the reconnect's
/// baseline reset is no new start.
#[test]
fn non_event_stream_started_before_reconnect_reset() {
    assert_eq!(
        reconnect_case(RECONNECT_MS - POLL_MS, false).takes(),
        NO_TAKE
    );
    assert_eq!(
        reconnect_case(RECONNECT_MS + POLL_MS, false).takes(),
        [vec![SendOwnership(true)]]
    );
}

/// Under the minimum after `EVENT_MS`.
const RESET_MS: u64 = EVENT_MS + 3 * POLL_MS;

/// `ready` before the audio source report, then a live `Input::Auto`
/// (no connect take, #324), the owner report [0] and the idle source
/// at `RESET_MS`; a music stream with no MPRIS player plays from
/// `start` ms on.
fn auto_reset_case(start: u64) -> Pipeline {
    let mut p = Pipeline::seeded(no_source());
    let stream = [music(1)];
    for ms in (EVENT_MS..=RESET_MS + 2 * MIN_MS + POLL_MS).step_by(POLL_MS as usize) {
        if ms == RESET_MS {
            assert!(!p.streams_playing);
            assert_eq!(
                p.feed(ms, Input::Auto),
                [
                    Command::ConnectA2dp,
                    SendOwnership(false),
                    Command::Announce,
                    Command::DrainPlayback
                ]
            );
            // The AirPods' own owner report.
            assert_eq!(p.feed(ms, Input::Ownership(vec![0])), []);
            assert_eq!(p.feed(ms, idle_source()), []);
        }
        let streams: &[StreamProps] = if ms >= start { &stream } else { &[] };
        p.step(ms, false, streams);
    }
    p
}

/// #90: a stream (no MPRIS) still in its debounce when Auto resets
/// the baseline is no new start.
#[test]
fn non_event_stream_started_before_auto_reset() {
    assert_eq!(auto_reset_case(EVENT_MS).takes(), NO_TAKE);
    assert_eq!(
        auto_reset_case(RESET_MS + POLL_MS).takes(),
        [vec![SendOwnership(true)]]
    );
}

#[test]
fn non_event_paused_player() {
    let corked = StreamProps {
        corked: true,
        ..music(1)
    };
    assert_eq!(run(false, &[corked]).takes(), NO_TAKE);
    assert_eq!(run(false, &[music(1)]).takes(), [vec![SendOwnership(true)]]);
}

/// A browser tab's sink input (no role) as `PulseStreams` lists it.
fn tab(mute: bool, level: Volume) -> StreamProps {
    let mut proplist = Proplist::new().expect("proplist");
    proplist.set_str("application.name", "Firefox").unwrap();
    let mut volume = ChannelVolumes::default();
    volume.set(2, level);
    stream_monitor::playback_stream(1, &proplist, false, mute, &volume)
}

#[test]
fn non_event_muted_autoplay_tab() {
    for stream in [tab(true, Volume::NORMAL), tab(false, Volume::MUTED)] {
        assert_eq!(run(false, &[stream.clone()]).takes(), NO_TAKE, "{stream:?}");
    }
    assert_eq!(
        run(false, &[tab(false, Volume::NORMAL)]).takes(),
        [vec![SendOwnership(true)]]
    );
}

/// #88: a paused SDL game keeps its Game stream uncorked and writes
/// silence into it.
#[test]
fn non_event_silent_game_stream() {
    let game = [sink(&[
        ("media.role", "Game"),
        ("application.name", "SDL Application"),
    ])];
    assert_eq!(run_levels(&game, |_| SILENT).takes(), NO_TAKE);
    assert_eq!(
        run_levels(&game, |_| LOUD).takes(),
        [vec![SendOwnership(true)]]
    );
}

/// #88: an idle Chromium/Electron WebAudio stream (no role) after
/// one short notification sound.
#[test]
fn non_event_webaudio_blip() {
    let webaudio = [sink(&[("application.name", "Chromium")])];
    let blip = |ms| if ms == EVENT_MS { LOUD } else { SILENT };
    assert_eq!(run_levels(&webaudio, blip).takes(), NO_TAKE);
    assert_eq!(
        run_levels(&webaudio, |_| LOUD).takes(),
        [vec![SendOwnership(true)]]
    );
}

/// #97/#101: the live pw-loopback playback stream, named and given
/// a role by user config, stays uncorked and loud.
#[test]
fn non_event_virtual_loopback() {
    let twin = [
        ("node.name", "output.pw-loopback-3412"),
        ("node.group", "pw-loopback-3412"),
        ("node.link-group", "pw-loopback-3412"),
        ("stream.is-live", "true"),
        (
            "target.object",
            "alsa_output.pci-0000_00_1f.3.analog-stereo",
        ),
        ("application.name", "pw-loopback"),
        ("media.role", "Music"),
    ];
    let loopback = [&twin[..], &[("node.virtual", "true")]].concat();
    assert_eq!(run(false, &[sink(&loopback)]).takes(), NO_TAKE);
    assert_eq!(
        run(false, &[sink(&twin)]).takes(),
        [vec![SendOwnership(true)]]
    );
}

/// `ready`, then a wear report, then a fresh music stream with MPRIS.
fn wearing(ears: [EarDetectionStatus; 2]) -> Pipeline {
    let mut p = Pipeline::ready();
    p.feed(20, Input::EarState(ears.to_vec()));
    p.event(true, &[music(1)]);
    p
}

#[test]
fn non_event_playback_not_worn() {
    for (off, one_in) in [
        ([OutOfEar, OutOfEar], [InEar, OutOfEar]),
        ([InCase, InCase], [InEar, InCase]),
    ] {
        assert_eq!(wearing(off).takes(), NO_TAKE, "{off:?}");
        assert_eq!(
            wearing(one_in).takes(),
            [vec![SendOwnership(true)]],
            "{one_in:?}"
        );
    }
}

#[test]
fn non_event_control_genuine_play_takes() {
    let mut p = Pipeline::ready();
    let stream = [music(1)];
    let mut ms = EVENT_MS;
    while ms < EVENT_MS + MIN_MS {
        assert_eq!(p.step(ms, false, &stream), [], "{ms} ms");
        ms += POLL_MS;
    }
    assert_eq!(p.step(ms, false, &stream), [SendOwnership(true)]);
    for later in (ms + POLL_MS..=ms + MIN_MS).step_by(POLL_MS as usize) {
        assert_eq!(p.step(later, false, &stream), [], "{later} ms");
    }
    assert_eq!(p.takes(), [vec![SendOwnership(true)]]);
}

/// #205: a track change's gap (MPRIS stopped, the old stream gone)
/// keeps the AirPods; so does a real stop: Linux stays the idle
/// owner (#262).
#[test]
fn non_event_track_change_keeps_ownership() {
    let minute_ms = 60_000;
    let sent = |p: &Pipeline, owner| {
        let c = SendOwnership(owner);
        p.log.iter().flatten().filter(|&x| *x == c).count()
    };
    let mut p = Pipeline::ready();
    assert_eq!(p.step(EVENT_MS, true, &[music(1)]), [SendOwnership(true)]);
    p.feed(EVENT_MS, Input::Ownership(vec![1]));
    let gap = EVENT_MS + POLL_MS;
    p.step(gap, false, &[]);
    let next = gap + POLL_MS;
    for ms in (next..=next + minute_ms).step_by(POLL_MS as usize) {
        p.step(ms, true, &[music(2)]);
        p.feed(ms, Input::Tick);
    }
    assert_eq!(sent(&p, true), 1);
    assert_eq!(sent(&p, false), 0);

    // The player stops.
    let stop = next + minute_ms + POLL_MS;
    for ms in (stop..=stop + minute_ms).step_by(POLL_MS as usize) {
        p.step(ms, false, &[]);
        p.feed(ms, Input::Tick);
    }
    assert_eq!(sent(&p, true), 1);
    assert_eq!(sent(&p, false), 0);
}

/// #301/#304: a local MPRIS player with a live music stream plays
/// while the iPhone owns; ear detection pauses it on a bud out and
/// resumes it on the bud in. The twin with no reinsertion takes.
/// (#302: an MPRIS-only player would no longer take at all.)
#[test]
fn non_event_ear_resume_while_remote_owns() {
    let run = |reinsert: bool| {
        let mut p = remote_owns();
        // Already playing at the session start's baseline: no edge.
        p.feed(30, Input::Auto);
        p.step(40, true, &[music(1)]);
        p.feed(100, Input::EarState(vec![InEar, OutOfEar]));
        p.step(150, false, &[]);
        p.step(200, false, &[]);
        if reinsert {
            p.feed(300, Input::EarState(vec![InEar, InEar]));
        }
        p.event(true, &[music(1)]);
        p.takes()
    };
    assert_eq!(run(true), NO_TAKE);
    assert_eq!(run(false), [vec![SendOwnership(true)]]);
}

/// `ready`, then the iPhone owns (AudioSource REMOTE Media) and
/// relays 301.
fn remote_owns() -> Pipeline {
    let mut p = Pipeline::ready();
    p.feed(
        20,
        Input::AudioSource(AudioSource {
            mac: REMOTE.to_string(),
            r#type: AudioSourceType::Media,
        }),
    );
    p.feed(
        20,
        Input::MediaInformation {
            host: REMOTE.to_string(),
            category: crate::media_controller::AUDIO_CATEGORY_MEDIA,
        },
    );
    p
}

/// #302: a remote-control MPRIS player (Spotify Connect: Playing,
/// no local stream) while the iPhone owns takes nothing. The twin
/// with a live local music stream takes.
#[test]
fn non_event_remote_control_player() {
    // An MPRIS player plays over `s` from the event on.
    let case = |s: &[StreamProps]| {
        let mut p = remote_owns();
        p.event(true, s);
        p.takes()
    };
    assert_eq!(case(&[]), NO_TAKE);
    assert_eq!(case(&[music(1)]), [vec![SendOwnership(true)]]);
}

/// #328: a Playing player whose only stream is its own muted one
/// (by flag or at every channel's `Volume::MUTED`) takes from the
/// iPhone at 301, as the Mac does with Music at volume 0. The same
/// stream of another process takes nothing.
#[test]
fn mpris_with_muted_own_stream_takes() {
    let case = |stream: &StreamProps, pid: u32| {
        let mut stream = stream.clone();
        stream
            .props
            .insert("application.process.id".to_string(), pid.to_string());
        let mut p = remote_owns();
        p.event(true, &[stream]);
        p.takes()
    };
    let muted = StreamProps {
        muted: true,
        ..music(1)
    };
    for stream in [muted, tab(false, Volume::MUTED)] {
        assert!(stream.muted, "{stream:?}");
        let takes = case(&stream, PLAYER_PID);
        assert_eq!(takes, [vec![SendOwnership(true)]], "{stream:?}");
        assert_eq!(case(&stream, PLAYER_PID + 1), NO_TAKE, "{stream:?}");
    }
}

/// #302: MPRIS with a live stream needs no peak debounce: the take
/// is in the event's first sample.
#[test]
fn mpris_with_live_stream_takes() {
    let mut p = remote_owns();
    assert_eq!(p.step(EVENT_MS, true, &[music(1)]), [SendOwnership(true)]);
}

/// #329: a remote-control MPRIS player (Playing, no stream of its
/// own) beside a 1 s terminal bell chime of role music while the
/// iPhone owns at 301 takes nothing. The twin, where the chime is
/// the player's own stream, takes.
#[test]
fn non_event_chime_beside_remote_player() {
    let run = |pid: u32| {
        let mut p = remote_owns();
        let chime = [sink(&[
            ("application.name", "kitty"),
            ("media.role", "music"),
            ("application.process.id", &pid.to_string()),
        ])];
        let end = EVENT_MS + 2 * MIN_MS + POLL_MS;
        for ms in (EVENT_MS..=end).step_by(POLL_MS as usize) {
            let streams: &[StreamProps] = if ms < EVENT_MS + 1000 { &chime } else { &[] };
            p.step(ms, true, streams);
        }
        p.takes()
    };
    assert_eq!(run(PLAYER_PID + 1), NO_TAKE);
    assert_eq!(run(PLAYER_PID), [vec![SendOwnership(true)]]);
}

// #96: a new local source behind playback already above idle.
const SECOND_PID: u32 = PLAYER_PID + 1;
const SKIP_201: &str =
    "AUTO takeover skipped: the audio source's category 301 is above Linux's 201";
const IDLE: u32 = AUDIO_CATEGORY_IDLE;
const STREAM: u32 = AUDIO_CATEGORY_STREAM;
const MEDIA: u32 = AUDIO_CATEGORY_MEDIA;

/// `stream` played by process `pid`.
fn owned_by(pid: u32, mut stream: StreamProps) -> StreamProps {
    stream
        .props
        .insert("application.process.id".to_string(), pid.to_string());
    stream
}

impl Pipeline {
    /// Every command of the samples every `PLAYBACK_POLL` from
    /// `from` through `to` ms.
    fn samples(
        &mut self,
        from: u64,
        to: u64,
        owners: &[Option<u32>],
        streams: &[StreamProps],
        level: impl Fn(u32) -> f32,
    ) -> Vec<Command> {
        (from..=to)
            .step_by(POLL_MS as usize)
            .flat_map(|ms| self.sample(ms, owners, streams, &level))
            .collect()
    }
}

/// Linux plays `owners` over `streams` (all audible) from
/// `EVENT_MS`, takes for it and holds the AirPods once every stream
/// counts; then the iPhone takes (a genuine [0]: Linux yields) and
/// relays `owner`. Returns the next sample's ms.
fn yielded(owners: &[Option<u32>], streams: &[StreamProps], owner: u32) -> (Pipeline, u64) {
    let mut p = Pipeline::ready();
    let end = EVENT_MS + MIN_MS + POLL_MS;
    let all = p.samples(EVENT_MS, end, owners, streams, |_| LOUD);
    assert_eq!(all, [SendOwnership(true)]);
    assert_eq!(p.feed(end, Input::Ownership(vec![1])), []);
    let source = AudioSource {
        mac: REMOTE.to_string(),
        r#type: AudioSourceType::Media,
    };
    assert_eq!(p.feed(end, Input::AudioSource(source)), []);
    let relay = Input::MediaInformation {
        host: REMOTE.to_string(),
        category: owner,
    };
    assert_eq!(p.feed(end, relay), []);
    assert_eq!(p.feed(end, Input::Ownership(vec![0])), [PauseMedia]);
    p.core.take_lines();
    (p, end + POLL_MS)
}

/// The samples from `from` until a stream started then has counted
/// for a poll.
fn past_min(from: u64) -> u64 {
    from + MIN_MS + POLL_MS
}

/// Stream B passes the minimum behind stream A, which still counts
/// (Linux stays at 201), after a yield to an owner relaying `owner`.
fn stream_behind_counted(owner: u32) -> (Vec<Command>, Vec<String>) {
    let (mut p, ms) = yielded(&[], &[music(1)], owner);
    let both = [music(1), music(2)];
    let all = p.samples(ms, past_min(ms), &[], &both, |_| LOUD);
    (all, p.core.take_lines())
}

#[test]
fn fresh_source_stream_behind_counted_takes_from_idle_owner() {
    assert_eq!(stream_behind_counted(IDLE).0, [SendOwnership(true)]);
}

#[test]
fn fresh_source_stream_behind_counted_skips_playing_owner() {
    let (all, lines) = stream_behind_counted(MEDIA);
    assert_eq!(all, []);
    assert_eq!(lines, [SKIP_201]);
}

/// #96 Q5: stream B counted beside A, falls silent past
/// `SILENCE_HOLD` (A keeps Linux at 201) and counts again: a new
/// source.
#[test]
fn fresh_source_stream_recount_after_silence_hold_is_new() {
    let both = [music(1), music(2)];
    let (mut p, ms) = yielded(&[], &both, IDLE);
    let hold_ms = stream_monitor::SILENCE_HOLD.as_millis() as u64;
    let silent_b = |index| if index == 2 { SILENT } else { LOUD };
    let loud_again = ms + hold_ms + POLL_MS;
    assert_eq!(
        p.samples(ms, loud_again - POLL_MS, &[], &both, silent_b),
        []
    );
    assert!(p.streams_playing);
    let all = p.samples(loud_again, past_min(loud_again), &[], &both, |_| LOUD);
    assert_eq!(all, [SendOwnership(true)]);
}

/// The player of `SECOND_PID` starts behind the test player, which
/// keeps Linux at 301, after a yield to a 301 owner; `pid` is the
/// second player's bus owner.
fn second_player(pid: u32) -> Vec<Command> {
    let first = [Some(PLAYER_PID)];
    let (mut p, ms) = yielded(&first, &[music(1)], MEDIA);
    let both = [Some(PLAYER_PID), Some(pid)];
    let streams = [music(1), owned_by(pid, music(2))];
    p.samples(ms, past_min(ms), &both, &streams, |_| LOUD)
}

#[test]
fn fresh_source_mpris_new_owner_pid_takes() {
    assert_eq!(second_player(SECOND_PID), [SendOwnership(true)]);
}

/// #96 Q4: a second player of the same process is no new source; its
/// stream counts at 201, below the owner's 301.
#[test]
fn fresh_source_mpris_same_pid_no_take() {
    assert_eq!(second_player(PLAYER_PID), []);
}

/// #96 Q3: MPRIS keeps Linux at 301 and a new stream counts behind
/// it: a request at 201, not 301.
#[test]
fn fresh_source_request_uses_source_category() {
    let run = |owner| {
        let player = [Some(PLAYER_PID)];
        let (mut p, ms) = yielded(&player, &[music(1)], owner);
        let streams = [music(1), owned_by(SECOND_PID, music(2))];
        let all = p.samples(ms, past_min(ms), &player, &streams, |_| LOUD);
        (all, p.core.take_lines())
    };
    assert_eq!(run(MEDIA), (vec![], vec![SKIP_201.to_string()]));
    assert_eq!(run(IDLE).0, [SendOwnership(true)]);
}

/// The second player (its own new stream) starts at `ms` beside a
/// stream of a third process: a new source at 301, then one at 201
/// (the player's own stream is none, #329).
fn new_player(p: &mut Pipeline, ms: u64) -> Vec<Command> {
    let both = [Some(PLAYER_PID), Some(SECOND_PID)];
    let streams = [
        music(1),
        owned_by(SECOND_PID, music(2)),
        owned_by(SECOND_PID + 1, music(3)),
    ];
    p.samples(ms, past_min(ms), &both, &streams, |_| LOUD)
}

#[test]
fn fresh_source_non_event_steady_sources() {
    let player = [Some(PLAYER_PID)];
    let (mut p, ms) = yielded(&player, &[music(1)], IDLE);
    let end = ms + 2 * MIN_MS + POLL_MS;
    assert_eq!(p.samples(ms, end, &player, &[music(1)], |_| LOUD), []);
}

#[test]
fn fresh_source_non_event_while_owning() {
    let mut p = Pipeline::ready();
    let player = [Some(PLAYER_PID)];
    let first = p.samples(EVENT_MS, EVENT_MS, &player, &[music(1)], |_| LOUD);
    assert_eq!(first, [SendOwnership(true)]);
    assert_eq!(p.feed(EVENT_MS, Input::Ownership(vec![1])), []);
    assert_eq!(new_player(&mut p, EVENT_MS + POLL_MS), []);
}

#[test]
fn fresh_source_non_event_while_pending() {
    let mut p = Pipeline::ready();
    let player = [Some(PLAYER_PID)];
    let first = p.samples(EVENT_MS, EVENT_MS, &player, &[music(1)], |_| LOUD);
    assert_eq!(first, [SendOwnership(true)]);
    assert_eq!(new_player(&mut p, EVENT_MS + POLL_MS), []);
}

#[test]
fn fresh_source_non_event_remote_call() {
    let (mut p, ms) = yielded(&[Some(PLAYER_PID)], &[music(1)], IDLE);
    let call = AudioSource {
        mac: REMOTE.to_string(),
        r#type: AudioSourceType::Call,
    };
    assert_eq!(p.feed(ms, Input::AudioSource(call)), []);
    assert_eq!(new_player(&mut p, ms), []);
}

#[test]
fn fresh_source_non_event_bud_out() {
    let (mut p, ms) = yielded(&[Some(PLAYER_PID)], &[music(1)], IDLE);
    assert_eq!(p.feed(ms, Input::EarState(vec![OutOfEar, OutOfEar])), []);
    assert_eq!(new_player(&mut p, ms), []);
}

/// The core fed by the real poller through `PlaybackSamples::handle`,
/// until a sample has `category`, then `more` samples; every command.
async fn feed_until(
    rx: &mut PlaybackSamples,
    core: &mut Handoff,
    category: u32,
    more: usize,
) -> Vec<Command> {
    let mut all = Vec::new();
    let mut left = None;
    while left != Some(0) {
        let (sampled_at, sampled, new_source) =
            tokio::time::timeout(Duration::from_secs(3), rx.sample())
                .await
                .expect("a playback sample")
                .expect("poller running");
        let input = Input::Playback {
            sampled_at,
            category: sampled,
            new_source,
        };
        all.extend(rx.handle(|now| core.handle(now, input)).1);
        left = match left {
            Some(n) => Some(n - 1),
            None => (sampled == category).then_some(more),
        };
    }
    all
}

/// #96 c1 (#272's rise) through the poller: a counted audible
/// stream, Linux owns and yields, then a live MPRIS player starts:
/// one take while the owner's category is at most 301.
#[tokio::test]
async fn fresh_source_mpris_behind_counted_stream_takes() {
    for owner in [IDLE, MEDIA] {
        let mut core = Handoff::new(LOCAL.to_string(), Instant::now(), true, true);
        for input in seed_inputs(facts()) {
            core.handle(Instant::now(), input);
        }
        let mpris = Arc::new(AtomicBool::new(false));
        let reads = mpris.clone();
        let streams = LiveStreams::default();
        let mut rx = spawn_playback_poller_with(
            move || named_player(reads.load(Ordering::SeqCst)),
            streams.clone(),
            AllAudible,
            STREAM_TEST_MIN,
            STREAM_TEST_POLL,
            |_| (),
        );
        streams.settle().await;
        assert_eq!(feed_until(&mut rx, &mut core, IDLE, 0).await, []);
        streams.set(vec![music(1)]);
        let take = feed_until(&mut rx, &mut core, STREAM, 0).await;
        assert_eq!(take, [SendOwnership(true)]);
        assert_eq!(core.handle(Instant::now(), Input::Ownership(vec![1])), []);
        let source = AudioSource {
            mac: REMOTE.to_string(),
            r#type: AudioSourceType::Media,
        };
        assert_eq!(core.handle(Instant::now(), Input::AudioSource(source)), []);
        let relay = Input::MediaInformation {
            host: REMOTE.to_string(),
            category: owner,
        };
        assert_eq!(core.handle(Instant::now(), relay), []);
        let lost = core.handle(Instant::now(), Input::Ownership(vec![0]));
        assert_eq!(lost, [PauseMedia]);
        core.take_lines();
        assert_eq!(feed_until(&mut rx, &mut core, STREAM, 5).await, []);
        mpris.store(true, Ordering::SeqCst);
        let all = feed_until(&mut rx, &mut core, MEDIA, 5).await;
        assert_eq!(all, [SendOwnership(true)], "{owner}");
        assert_eq!(
            core.take_lines(),
            ["AUTO playback started; requested Linux ownership"]
        );
    }
}

/// #301: a bud back in while the iPhone owns; the second player's
/// 301 and the third process's 201 both come within the grace.
#[test]
fn fresh_source_non_event_ear_resume_grace() {
    let (mut p, ms) = yielded(&[Some(PLAYER_PID)], &[music(1)], IDLE);
    assert_eq!(p.feed(ms, Input::EarState(vec![InEar, OutOfEar])), []);
    assert_eq!(p.feed(ms, Input::EarState(vec![InEar, InEar])), []);
    assert_eq!(new_player(&mut p, ms), []);
    let resumed = "AUTO playback resumed by ear detection while another host owns; not taking";
    assert_eq!(p.core.take_lines(), [resumed, resumed]);
}

/// The second player (on stream A, which counts already) starts at
/// the reset's first sample, or its second when `later`. The iPhone
/// relays 201, so Auto makes no connect take (#324).
fn after_reset(later: bool) -> Vec<Command> {
    let a = owned_by(SECOND_PID, music(1));
    let (mut p, ms) = yielded(&[], &[a.clone()], STREAM);
    assert_eq!(
        p.feed(ms, Input::Auto),
        [
            Command::ConnectA2dp,
            SendOwnership(false),
            Command::Announce,
            Command::DrainPlayback
        ]
    );
    assert_eq!(p.feed(ms, Input::Ownership(vec![0])), []);
    let start = if later {
        ms + 2 * POLL_MS
    } else {
        ms + POLL_MS
    };
    let mut all = p.samples(ms + POLL_MS, start - POLL_MS, &[], &[a.clone()], |_| LOUD);
    let second = [Some(SECOND_PID)];
    all.extend(p.samples(start, past_min(start), &second, &[a], |_| LOUD));
    all
}

/// The core's first sample after a reset and the poller's first
/// sample are baselines, whatever source they report.
#[test]
fn fresh_source_non_event_after_baseline_reset() {
    assert_eq!(after_reset(false), []);
    // The twin: a second sample takes at 301.
    assert_eq!(after_reset(true), [SendOwnership(true)]);
    let state = (PlaybackState::default(), vec![(1, HashSet::new())]);
    assert_eq!(
        sample_sources(&player(true), &state, &mut None),
        (MEDIA, None)
    );
}

/// #426: player A plays its live stream (301) beside player B,
/// Playing with no stream. The monitor dies: the sample with the
/// live set gone unknown re-baselines, no new source; a player C
/// Playing in a later unknown sample is new.
#[test]
fn sample_sources_live_unknown_rebaselines_once() {
    let (a, b, c) = (Some(PLAYER_PID), Some(SECOND_PID), Some(SECOND_PID + 1));
    let sample = |live| {
        let state = PlaybackState {
            counted: false,
            baseline: false,
            live,
        };
        (state, Vec::new())
    };
    let mut last = None;
    let known = sample(Some(HashSet::from([PLAYER_PID])));
    assert_eq!(sample_sources(&[a, b], &known, &mut last), (MEDIA, None));
    let unknown = sample(None);
    assert_eq!(sample_sources(&[a, b], &unknown, &mut last), (MEDIA, None));
    assert_eq!(
        sample_sources(&[a, b, c], &unknown, &mut last),
        (MEDIA, Some(MEDIA))
    );
}

/// #426 through the core: Linux plays at 301 with A's stream beside
/// B (Playing, no stream) while the iPhone owns relaying 301; the
/// monitor dies. That sample commands nothing.
#[test]
fn fresh_source_non_event_monitor_dead() {
    let player = [Some(PLAYER_PID)];
    let (mut p, ms) = yielded(&player, &[music(1)], MEDIA);
    let both = [Some(PLAYER_PID), Some(SECOND_PID)];
    assert_eq!(p.sample(ms, &both, &[music(1)], |_| LOUD), []);
    p.monitor_dead = true;
    assert_eq!(p.sample(ms + POLL_MS, &both, &[music(1)], |_| LOUD), []);
}

/// #429: player C starts Playing in the sample the monitor dies.
/// C is new since the previous sample: one new source, once.
#[test]
fn sample_sources_live_unknown_new_player_reported_once() {
    let (a, b, c) = (Some(PLAYER_PID), Some(SECOND_PID), Some(SECOND_PID + 1));
    let sample = |live| {
        let state = PlaybackState {
            counted: false,
            baseline: false,
            live,
        };
        (state, Vec::new())
    };
    let mut last = None;
    let known = sample(Some(HashSet::from([PLAYER_PID])));
    assert_eq!(sample_sources(&[a, b], &known, &mut last), (MEDIA, None));
    let unknown = sample(None);
    assert_eq!(
        sample_sources(&[a, b, c], &unknown, &mut last),
        (MEDIA, Some(MEDIA))
    );
    assert_eq!(
        sample_sources(&[a, b, c], &unknown, &mut last),
        (MEDIA, None)
    );
}

/// #429 through the core: as `fresh_source_non_event_monitor_dead`,
/// but a new player starts in the sample the monitor dies: one take.
#[test]
fn fresh_source_monitor_dead_new_player_takes() {
    let player = [Some(PLAYER_PID)];
    let (mut p, ms) = yielded(&player, &[music(1)], MEDIA);
    let both = [Some(PLAYER_PID), Some(SECOND_PID)];
    assert_eq!(p.sample(ms, &both, &[music(1)], |_| LOUD), []);
    p.monitor_dead = true;
    let three = [Some(PLAYER_PID), Some(SECOND_PID), Some(SECOND_PID + 1)];
    let from = ms + POLL_MS;
    let all = p.samples(from, past_min(from), &three, &[music(1)], |_| LOUD);
    assert_eq!(all, [SendOwnership(true)]);
}

/// #96 with #301: a bud back in while the iPhone owns, relaying 100;
/// the player resumes with its own stream silent for 1.5 s, so the
/// stream counts after the grace. That stream is the player's (#329):
/// no new source. With `other`, a stream of another process counts
/// then too: one take.
fn own_stream_after_ear_resume(other: bool) -> (Vec<Command>, Vec<String>) {
    let mut p = Pipeline::ready();
    let remote = AudioSource {
        mac: REMOTE.to_string(),
        r#type: AudioSourceType::Media,
    };
    p.feed(20, Input::AudioSource(remote));
    // Relayed playing at the session start, then idle: an idle
    // iPhone at the start would get the connect take (#324, #452).
    let relay = |category| Input::MediaInformation {
        host: REMOTE.to_string(),
        category,
    };
    p.feed(25, relay(MEDIA));
    p.feed(30, Input::Auto);
    p.feed(35, relay(IDLE));
    p.step(40, true, &[music(1)]);
    p.feed(100, Input::EarState(vec![InEar, OutOfEar]));
    p.step(150, false, &[]);
    p.step(200, false, &[]);
    p.feed(300, Input::EarState(vec![InEar, InEar]));
    p.core.take_lines();
    let mut streams = vec![music(1)];
    if other {
        streams.push(owned_by(SECOND_PID, music(2)));
    }
    let audible = EVENT_MS + 1500;
    let grace_end = 300 + crate::handoff::EAR_RESUME_GRACE.as_millis() as u64;
    assert!(audible + MIN_MS > grace_end);
    let mut all = Vec::new();
    for ms in (EVENT_MS..=past_min(audible)).step_by(POLL_MS as usize) {
        let level = if ms < audible { SILENT } else { LOUD };
        all.extend(p.sample(ms, &player(true), &streams, |_| level));
    }
    assert!(p.streams_playing);
    (all, p.core.take_lines())
}

#[test]
fn fresh_source_own_stream_after_ear_resume_no_take() {
    let resumed = "AUTO playback resumed by ear detection while another host owns; not taking";
    assert_eq!(
        own_stream_after_ear_resume(false),
        (vec![], vec![resumed.to_string()])
    );
}

#[test]
fn fresh_source_own_stream_twin_other_pid_takes() {
    let (all, lines) = own_stream_after_ear_resume(true);
    assert_eq!(all, [SendOwnership(true)]);
    assert_eq!(
        lines.last().map(String::as_str),
        Some("AUTO playback started; requested Linux ownership")
    );
}

// #349: MPRIS bridges, through `media_controller::player_owner`.
const BRIDGE_PID: u32 = 99_999_021;
const MPD_PID: u32 = 99_999_022;
const CHIME_PID: u32 = 99_999_023;

/// The Playing players resolved to `owners` beside `streams` (all
/// audible) while the iPhone owns at 301: Linux's takes over the
/// event, and whether the yield's filter (`pause_own_players`)
/// pauses one of them.
async fn bridged(owners: &[Option<u32>], streams: &[StreamProps]) -> (Vec<Vec<Command>>, bool) {
    let mut p = remote_owns();
    p.samples(
        EVENT_MS,
        EVENT_MS + 2 * MIN_MS + POLL_MS,
        owners,
        streams,
        |_| LOUD,
    );
    let live = stream_monitor::live_stream_pids(streams, |_| None);
    let owners = owners.to_vec();
    let paused = pause_own_players(
        &MediaController::new(String::new(), LOCAL.to_string()),
        Arc::new(
            move |own: &dyn Fn(Option<u32>) -> bool| media_controller::PauseOutcome {
                paused: owners.iter().filter(|&&owner| own(owner)).count(),
                ..Default::default()
            },
        ),
        PlaybackState {
            counted: false,
            baseline: false,
            live: Some(live),
        },
    )
    .await;
    (p.takes(), paused)
}

/// The mpDris2 player (bus owner `BRIDGE_PID`) with `procs` as /proc.
fn mpd_player(procs: &[(u32, &str)]) -> Option<u32> {
    media_controller::player_owner(
        "org.mpris.MediaPlayer2.mpd",
        Some(BRIDGE_PID),
        |_| None,
        || media_controller::mpd_pid(procs.iter().copied()),
    )
}

/// mpDris2 beside MPD's stream. The twin: no local MPD, only a
/// pw-play chime.
#[tokio::test]
async fn bridge_mpd_player_takes_and_pauses_with_mpd_stream() {
    let mpd = mpd_player(&[(CHIME_PID, "pw-play"), (MPD_PID, "mpd")]);
    let mpd_stream = [owned_by(MPD_PID, music(1))];
    assert_eq!(
        bridged(&[mpd], &mpd_stream).await,
        (vec![vec![SendOwnership(true)]], true)
    );
    let remote = mpd_player(&[(CHIME_PID, "pw-play")]);
    let chime = [owned_by(
        CHIME_PID,
        sink(&[("application.name", "pw-play"), ("media.role", "music")]),
    )];
    assert_eq!(bridged(&[remote], &chime).await, (NO_TAKE, false));
}

/// The PBI host (`BRIDGE_PID`) is Firefox's child. The twin: only
/// an unrelated stream plays.
#[tokio::test]
async fn bridge_pbi_player_takes_and_pauses_with_browser_stream() {
    let browser = media_controller::player_owner(
        "org.mpris.MediaPlayer2.plasma-browser-integration",
        Some(BRIDGE_PID),
        |pid| (pid == BRIDGE_PID).then_some(FIREFOX_PID),
        || unreachable!(),
    );
    let firefox = [owned_by(FIREFOX_PID, music(1))];
    assert_eq!(
        bridged(&[browser], &firefox).await,
        (vec![vec![SendOwnership(true)]], true)
    );
    let other = [owned_by(CHIME_PID, music(1))];
    assert_eq!(bridged(&[browser], &other).await, (NO_TAKE, false));
}

/// A Spotify Connect remote (no stream of its own) beside a live MPD
/// stream: the mpd rule doesn't credit it.
#[tokio::test]
async fn bridge_rules_spare_connect_remote_beside_mpd_stream() {
    let connect = media_controller::player_owner(
        "org.mpris.MediaPlayer2.spotify",
        Some(CONNECT_PID),
        |_| unreachable!(),
        || unreachable!(),
    );
    assert_eq!(connect, Some(CONNECT_PID));
    let mpd_stream = [owned_by(MPD_PID, music(1))];
    assert_eq!(bridged(&[connect], &mpd_stream).await, (NO_TAKE, false));
}
