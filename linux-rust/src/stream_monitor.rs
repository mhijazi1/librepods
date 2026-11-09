//! Local-call and stream-playback monitoring (#11, #10): the I/O behind
//! `call_detect`.
//!
//! `StreamSource` copies the sound server's capture streams and cards into an
//! `AudioSnapshot`; `CallPoller` turns snapshots into call state changes with
//! the pure `call_detect` rules; `spawn_call_poller` samples once per
//! `CALL_POLL` and sends each change to the handoff task.
//!
//! `PlaybackSource` lists the playback streams (sink inputs) and
//! `PeakSource` measures how loud the candidates among them are (#88);
//! `PlaybackTracker` debounces the takeover-worthy, audible ones with an
//! injected clock, and `spawn_stream_playback` publishes its state for
//! `switching::spawn_playback_poller`, which combines it with MPRIS. Tests
//! use fake sources, so no sound server is needed.
//!
//! Takeover (#272): as on macOS, a counted stream (audio category 201) takes
//! the AirPods from an idle owner but not from one playing media (301); it
//! then plays on the next sink, where #264's A2DP hold sends it.

use crate::call_detect::{
    StreamDirection, StreamProps, airpods_card_profile, call_active, is_takeover_worthy,
};
use crate::pulse::{self, pulse_error};
use libpulse_binding::context::{Context, State as ContextState};
use libpulse_binding::def::BufferAttr;
use libpulse_binding::mainloop::standard::{IterateResult, Mainloop};
#[cfg(test)]
use libpulse_binding::proplist::Proplist;
use libpulse_binding::sample::{Format, Spec};
use libpulse_binding::stream::{
    FlagSet as StreamFlagSet, PeekResult, State as StreamState, Stream,
};
use libpulse_binding::volume::{ChannelVolumes, Volume};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};

/// Interval between call-state samples in production.
pub(crate) const CALL_POLL: Duration = Duration::from_secs(1);

/// How long a takeover-worthy playback stream must have been present and
/// uncorked, continuously, before it counts as local playback. Event and
/// notification sounds that slip past the classifier (no `event.id`, no
/// role) last well under a second or two, while media someone starts keeps
/// playing; 2 s filters the former and still takes the AirPods about as
/// fast as a person notices which device plays. Samples are taken every
/// poll interval, so the effective delay is 2 s up to 2 s plus one interval.
pub(crate) const MIN_STREAM_PLAYBACK: Duration = Duration::from_secs(2);

/// The peak level (0.0 to 1.0) a playback stream must exceed in a sample to
/// carry sound (#88): -60 dBFS. A paused SDL game or idle WebAudio context
/// writes digital silence, while quiet music stays well above it.
pub(crate) const AUDIBLE_PEAK: f32 = 0.001;

/// How long a counted stream may stay silent and still count (#88): long
/// enough for gaps between tracks and quiet passages, short enough that a
/// paused game or an idle tab hands the AirPods back soon.
pub(crate) const SILENCE_HOLD: Duration = Duration::from_secs(5);

/// Call-state changes: whether Linux was in a call at the instant.
pub(crate) type CallSamples = mpsc::Receiver<(Instant, bool)>;

/// The sound server state call detection needs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AudioSnapshot {
    /// Streams with their property lists.
    pub(crate) streams: Vec<StreamProps>,
    /// Cards: name and active profile.
    pub(crate) cards: Vec<(String, Option<String>)>,
}

/// Where snapshots come from: the sound server in production, a fake in
/// tests.
pub(crate) trait StreamSource: Send {
    fn snapshot(&mut self) -> io::Result<AudioSnapshot>;
}

/// Turns snapshots into call state changes.
#[derive(Debug, Default)]
pub(crate) struct CallPoller {
    last: Option<bool>,
}

impl CallPoller {
    /// Sample `source` once. Some(in_call) only when the call state changed,
    /// including the first sample; None when it did not, or when the source
    /// failed (the last state is kept).
    pub(crate) fn poll(
        &mut self,
        source: &mut dyn StreamSource,
        airpods_mac: &str,
    ) -> Option<bool> {
        let snapshot = match source.snapshot() {
            Ok(snapshot) => snapshot,
            Err(e) => {
                log::debug!("Call monitor: audio snapshot failed: {e}");
                return None;
            }
        };
        let in_call = call_active(
            &snapshot.streams,
            airpods_card_profile(&snapshot.cards, airpods_mac),
        );
        if self.last == Some(in_call) {
            return None;
        }
        self.last = Some(in_call);
        Some(in_call)
    }
}

/// Sample the call state every `CALL_POLL` and send each change. The poller
/// stops once the receiver is dropped.
pub(crate) fn spawn_call_poller(airpods_mac: &str) -> CallSamples {
    spawn_call_poller_with(airpods_mac.to_string(), PulseStreams, CALL_POLL)
}

fn spawn_call_poller_with<S: StreamSource + 'static>(
    airpods_mac: String,
    source: S,
    interval: Duration,
) -> CallSamples {
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let mut state = Some((CallPoller::default(), source));
        while !tx.is_closed() {
            let Some((mut poller, mut source)) = state.take() else {
                break;
            };
            let mac = airpods_mac.clone();
            let sampled_at = Instant::now();
            let sampled = tokio::task::spawn_blocking(move || {
                let change = poller.poll(&mut source, &mac);
                (poller, source, change)
            })
            .await;
            let Ok((poller, source, change)) = sampled else {
                log::error!("Call monitor for {airpods_mac} stopped: sampling panicked");
                break;
            };
            state = Some((poller, source));
            if let Some(in_call) = change
                && tx.send((sampled_at, in_call)).await.is_err()
            {
                break;
            }
            tokio::time::sleep(interval).await;
        }
    });
    rx
}

/// Where playback streams (sink inputs) come from: the sound server in
/// production, a fake in tests.
pub(crate) trait PlaybackSource: Send {
    /// Every playback stream with its index, properties and corked state.
    /// A failed or truncated listing is an error, never a short list.
    fn playback_streams(&mut self) -> io::Result<Vec<StreamProps>>;

    /// The source can no longer list (a dead thread), so an Ok listing says
    /// nothing about live presence: `live` is unknown (None).
    fn dead(&self) -> bool {
        false
    }
}

/// How loud playback streams are (#88): the sinks' monitors in production
/// (`PulsePeaks`), a fake in tests.
pub(crate) trait PeakSource: Send {
    /// Each candidate's highest peak level (0.0 to 1.0) since the previous
    /// call. A candidate missing from the result is silent. Never waits on
    /// the sound server.
    fn levels(&mut self, candidates: &HashSet<u32>) -> HashMap<u32, f32>;
}

/// Where a candidate stream is in the peak gate (#88).
#[derive(Clone, Copy, Debug)]
enum Gate {
    /// Audible in every sample since this one, its first audible sample.
    Debounce(Instant),
    /// Counts; this was its last audible sample, and this its start number
    /// (`PlaybackTracker::started`).
    Counting(Instant, u64),
}

/// Debounces takeover-worthy playback streams that carry sound into a
/// stream-playing state.
///
/// A stream, keyed by its sound server index, is a candidate while it is
/// worthy (`call_detect::is_takeover_worthy`), uncorked and not part of the
/// baseline. A muted or zero-volume stream is treated as corked (#84).
/// Corking, vanishing or reclassification forgets it, so it starts over if
/// it comes back. A candidate counts only while it carries sound (#88): the
/// `min` debounce starts at its first sample whose peak level is above
/// `AUDIBLE_PEAK`, any silent sample before `min` restarts it (so one
/// notification ping on an idle WebAudio stream never counts), and it
/// counts once it has been audible in every sample for `min`. A counted
/// stream keeps counting through silence shorter than `SILENCE_HOLD` and
/// stops once it has been silent that long; then it needs a new debounce.
/// A stream that stays silent (a paused SDL game, an idle Electron app) or
/// has no level at all never counts. Stream playback is on while any
/// stream counts.
///
/// The first successful listing is the baseline: streams already playing in
/// it (worthy, uncorked and unmuted, whatever their level) never count, as
/// MPRIS playback running at the handoff core's baseline is no new start,
/// and they are not measured. Such a stream becomes a candidate only once it
/// has been corked (or reclassified) and plays again, and then counts only
/// after an audible `min`; a new stream is a candidate as usual.
/// `rebaseline` moves the baseline when the handoff core resets its own
/// (#90).
#[derive(Debug)]
pub(crate) struct PlaybackTracker {
    min: Duration,
    /// Candidates that are debouncing or counting, by index.
    gates: HashMap<u32, Gate>,
    /// Worthy, uncorked streams of the first successful listing (or
    /// debouncing since before a `rebaseline`'s reset) that have stayed so
    /// ever since; None until that listing.
    preexisting: Option<HashSet<u32>>,
    /// Whether a stream counts as playing (false before any change).
    playing: bool,
    /// How many times a stream started counting (#96): a stream that stops
    /// counting and counts again is a new start.
    starts: u64,
}

/// The worthy, uncorked, unmuted streams of a listing.
pub(crate) fn live_streams(streams: &[StreamProps]) -> HashSet<u32> {
    streams
        .iter()
        .filter(|s| !s.corked && !s.muted && is_takeover_worthy(s))
        .map(|s| s.index)
        .collect()
}

impl PlaybackTracker {
    pub(crate) fn new(min: Duration) -> Self {
        PlaybackTracker {
            min,
            gates: HashMap::new(),
            preexisting: None,
            playing: false,
            starts: 0,
        }
    }

    /// The streams of a listing whose levels count: worthy, uncorked,
    /// unmuted and not part of the baseline (so none before the baseline).
    pub(crate) fn candidates(&self, streams: &[StreamProps]) -> HashSet<u32> {
        let Some(preexisting) = &self.preexisting else {
            return HashSet::new();
        };
        live_streams(streams)
            .into_iter()
            .filter(|index| !preexisting.contains(index))
            .collect()
    }

    /// Take the playback streams present at `now`, with each candidate's
    /// peak `level` since the previous sample. Some(playing) only when the
    /// aggregate stream-playing state changed.
    pub(crate) fn observe_levels(
        &mut self,
        now: Instant,
        streams: &[StreamProps],
        level: impl Fn(u32) -> f32,
    ) -> Option<bool> {
        let live = live_streams(streams);
        let preexisting = self.preexisting.get_or_insert_with(|| live.clone());
        preexisting.retain(|index| live.contains(index));
        let candidates = self.candidates(streams);
        self.gates.retain(|index, _| candidates.contains(index));
        for index in candidates {
            let audible = level(index) > AUDIBLE_PEAK;
            let since = |at: Instant| now.saturating_duration_since(at);
            let gate = match (self.gates.get(&index).copied(), audible) {
                (None, true) => Some(Gate::Debounce(now)),
                (Some(Gate::Debounce(start)), true) if since(start) >= self.min => {
                    self.starts += 1;
                    Some(Gate::Counting(now, self.starts))
                }
                (debounce @ Some(Gate::Debounce(_)), true) => debounce,
                (Some(Gate::Counting(_, start)), true) => Some(Gate::Counting(now, start)),
                (Some(Gate::Counting(last, start)), false) if since(last) < SILENCE_HOLD => {
                    Some(Gate::Counting(last, start))
                }
                (_, false) => None,
            };
            match gate {
                Some(gate) => self.gates.insert(index, gate),
                None => self.gates.remove(&index),
            };
        }
        let playing = self
            .gates
            .values()
            .any(|gate| matches!(gate, Gate::Counting(..)));
        if playing == self.playing {
            return None;
        }
        self.playing = playing;
        Some(playing)
    }

    /// Each counting stream's start number (`starts` when it started), in
    /// order, with its owner PIDs in `owners` (`stream_owners`; none when
    /// unresolved) (#96).
    pub(crate) fn started(&self, owners: &HashMap<u32, HashSet<u32>>) -> Starts {
        let mut started: Starts = self
            .gates
            .iter()
            .filter_map(|(index, gate)| match gate {
                Gate::Counting(_, start) => {
                    Some((*start, owners.get(index).cloned().unwrap_or_default()))
                }
                Gate::Debounce(_) => None,
            })
            .collect();
        started.sort_unstable_by_key(|&(start, _)| start);
        started
    }

    /// The handoff core reset its playback baseline at `at` (#90): streams
    /// whose audible debounce began before `at` join the baseline, as MPRIS
    /// playback running at the core's baseline is no new start, so they
    /// count only after they stop and play again. Streams first audible at
    /// or after `at` keep debouncing, however late the request arrives.
    /// Counted streams keep counting (no false edge), and silent candidates
    /// stay candidates. Before the first successful listing there is
    /// nothing to move: that listing is still the baseline.
    pub(crate) fn rebaseline(&mut self, at: Instant) {
        let Some(preexisting) = &mut self.preexisting else {
            return;
        };
        self.gates.retain(|&index, gate| match gate {
            Gate::Debounce(start) if *start < at => {
                preexisting.insert(index);
                false
            }
            Gate::Debounce(_) | Gate::Counting(..) => true,
        });
    }

    /// A sample's I/O: list `source`, then ask `peaks` for the levels of
    /// that listing's candidates only. A source error gives None and asks
    /// for no levels: the caller then changes nothing, so the last state and
    /// the streams' timers are kept, and the baseline is still the first
    /// successful listing.
    pub(crate) fn sample(
        &self,
        source: &mut dyn PlaybackSource,
        peaks: &mut dyn PeakSource,
    ) -> Option<Sample> {
        match source.playback_streams() {
            Ok(streams) => {
                let levels = peaks.levels(&self.candidates(&streams));
                Some((streams, levels))
            }
            Err(e) => {
                log::debug!("Stream monitor: playback stream listing failed: {e}");
                None
            }
        }
    }

    /// Re-baseline at `request`'s reset time, if any, then `observe_levels`
    /// a `sample` at `now`. `now` is read once the sample has returned, so a
    /// slow listing can't date a stream's start back to before the stream
    /// was seen, and a request made while the listing was in flight applies
    /// to it.
    pub(crate) fn observe_sample(
        &mut self,
        now: Instant,
        request: Option<Instant>,
        (streams, levels): &Sample,
    ) -> Option<bool> {
        if let Some(at) = request {
            self.rebaseline(at);
        }
        self.observe_levels(now, streams, |index| {
            levels.get(&index).copied().unwrap_or(0.0)
        })
    }
}

/// A playback listing with its candidates' peak levels.
pub(crate) type Sample = (Vec<StreamProps>, HashMap<u32, f32>);

/// The counting streams' starts the monitor publishes beside
/// `PlaybackState` (`PlaybackTracker::started`, #96).
pub(crate) type Starts = Vec<(u64, HashSet<u32>)>;

/// What the stream monitor publishes.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct PlaybackState {
    /// A stream counts as playing (`PlaybackTracker`).
    pub(crate) counted: bool,
    /// The PIDs owning a live stream, muted or not, in the last successful
    /// listing (`stream_owners`); None before the first one (#302) and
    /// once the source is dead (`PlaybackSource::dead`, #318).
    pub(crate) live: Option<HashSet<u32>>,
}

impl PlaybackState {
    /// Whether the MPRIS player whose bus owner is `owner` plays locally:
    /// it owns a live stream (#329), unknown counting as live: before the
    /// first successful listing (a stalled sound server, #83) or with a
    /// dead stream thread (#318), every player plays as before #302. A
    /// failed owner lookup (None) owns nothing.
    pub(crate) fn live_or_unknown(&self, owner: Option<u32>) -> bool {
        self.live
            .as_ref()
            .is_none_or(|pids| owner.is_some_and(|pid| pids.contains(&pid)))
    }
}

/// The key of a stream's or a client's process id.
const PROCESS_ID: &str = "application.process.id";

/// How many ancestors of a live stream's process own it too (#329).
const OWNER_DEPTH: usize = 16;

/// The PIDs that own a live stream: a worthy, uncorked one, muted or not
/// (unlike `live_streams`), as a player muted at its own slider still plays
/// on the Mac (#328). Each one's process id
/// (`PROCESS_ID`; a stream without one owns nothing) and its ancestors by
/// `ppid`, at most `OWNER_DEPTH` of them, stopping at PID 1, an unknown
/// parent or a repeat within that stream's walk. An MPRIS player owns a
/// live stream iff its bus owner is in the set: the stream's process or a
/// bounded ancestor of it.
#[cfg(test)]
pub(crate) fn live_stream_pids(
    streams: &[StreamProps],
    ppid: impl Fn(u32) -> Option<u32>,
) -> HashSet<u32> {
    stream_owners(streams, ppid)
        .into_values()
        .flatten()
        .collect()
}

/// `live_stream_pids` by stream index (#96).
pub(crate) fn stream_owners(
    streams: &[StreamProps],
    ppid: impl Fn(u32) -> Option<u32>,
) -> HashMap<u32, HashSet<u32>> {
    let mut owners = HashMap::new();
    for stream in streams
        .iter()
        .filter(|s| !s.corked && is_takeover_worthy(s))
    {
        let mut walked = HashSet::new();
        let mut next = stream_pid(&stream.props, None);
        for _ in 0..=OWNER_DEPTH {
            match next {
                Some(pid) if pid > 1 && walked.insert(pid) => next = ppid(pid),
                _ => break,
            }
        }
        owners.insert(stream.index, walked);
    }
    owners
}

/// The process a sink input plays for: its own `PROCESS_ID`, else its
/// client's, else the client's `pipewire.sec.pid`. Native PipeWire streams
/// (mpv, pw-cat) set it on their client only; a pulse-protocol client's
/// `pipewire.sec.pid` is pipewire-pulse's own, hence last.
fn stream_pid(
    stream: &BTreeMap<String, String>,
    client: Option<&BTreeMap<String, String>>,
) -> Option<u32> {
    let pid = |props: &BTreeMap<String, String>, key: &str| props.get(key)?.trim().parse().ok();
    pid(stream, PROCESS_ID).or_else(|| {
        let client = client?;
        pid(client, PROCESS_ID).or_else(|| pid(client, "pipewire.sec.pid"))
    })
}

/// A sink input's properties with `PROCESS_ID` set to its `stream_pid`,
/// so the stream carries the process id it inherits from its client.
fn with_client_pid(
    mut props: BTreeMap<String, String>,
    client: Option<&BTreeMap<String, String>>,
) -> BTreeMap<String, String> {
    if let Some(pid) = stream_pid(&props, client) {
        props.insert(PROCESS_ID.to_string(), pid.to_string());
    }
    props
}

/// `pid`'s parent from `/proc/<pid>/stat`; None if it is gone or unreadable.
fn proc_ppid(pid: u32) -> Option<u32> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The command name may hold spaces and parentheses: skip past its last ')'.
    let (_, fields) = stat.rsplit_once(')')?;
    fields.split_whitespace().nth(1)?.parse().ok()
}

/// Sample `source` and `peaks` every `interval` with a `PlaybackTracker`
/// and publish the stream-playing state (initially not counted; streams
/// already playing in the first listing never count, see
/// `PlaybackTracker`) with the live presence of each successful listing,
/// beside the tracker's `started` (#96) in the same send.
/// Each sample is timed when the listing and levels return. Sampling runs on
/// the blocking pool in its own task, so a sound server that never answers
/// stalls only this state, never the caller, and only until its connect or
/// listing times out (`pulse::CONNECT_TIMEOUT`, then
/// `pulse::OPERATION_TIMEOUT` for the sink inputs and again for their
/// clients; the last state is kept). Putting the core's reset time in
/// `rebaseline` asks the tracker to re-baseline at it
/// (`PlaybackTracker::rebaseline`) before it observes the next listing to
/// return, even one already in flight (#90).
///
/// `rebaseline`'s lock is the serialization point with the core's step
/// (`switching::PlaybackSamples::handle`): once the listing and the
/// levels have returned (never across that I/O, so a stalled sound server
/// never blocks a step), the worker holds it from its clock read through
/// the request, the observe and the publish. Each observe so falls wholly
/// before a step's reset (published before it) or wholly after its
/// request. `observed` is called with each observe's time before the
/// publish, under the lock (a test hook). The task stops once every
/// receiver is dropped, dropping `peaks`; if sampling panics the state
/// drops to not counted, live unknown.
pub(crate) fn spawn_stream_playback<S, P>(
    source: S,
    peaks: P,
    min: Duration,
    interval: Duration,
    rebaseline: Arc<Mutex<Option<Instant>>>,
    observed: impl Fn(Instant) + Send + Sync + 'static,
) -> watch::Receiver<(PlaybackState, Starts)>
where
    S: PlaybackSource + 'static,
    P: PeakSource + 'static,
{
    let (tx, rx) = watch::channel(Default::default());
    let observed = Arc::new(observed);
    tokio::spawn(async move {
        let mut state = Some((PlaybackTracker::new(min), source, peaks));
        while !tx.is_closed() {
            let Some((mut tracker, mut source, mut peaks)) = state.take() else {
                break;
            };
            let (rebaseline, observed, publish) =
                (rebaseline.clone(), observed.clone(), tx.clone());
            let sampled = tokio::task::spawn_blocking(move || {
                if let Some(sample) = tracker.sample(&mut source, &mut peaks) {
                    // Read from /proc before the lock, which a step may wait on.
                    let owners = stream_owners(&sample.0, proc_ppid);
                    let live = owners.values().flatten().copied().collect();
                    let mut request = rebaseline.lock().unwrap_or_else(PoisonError::into_inner);
                    let now = Instant::now();
                    tracker.observe_sample(now, request.take(), &sample);
                    observed(now);
                    let state = (
                        PlaybackState {
                            counted: tracker.playing,
                            live: (!source.dead()).then_some(live),
                        },
                        tracker.started(&owners),
                    );
                    if *publish.borrow() != state {
                        publish.send_replace(state);
                    }
                }
                (tracker, source, peaks)
            })
            .await;
            let Ok(sampled) = sampled else {
                log::error!("Stream playback monitor stopped: sampling panicked");
                tx.send_replace(Default::default());
                break;
            };
            state = Some(sampled);
            tokio::time::sleep(interval).await;
        }
    });
    rx
}

/// Whether a sink input is silenced: muted, or every channel (at least one)
/// at `Volume::MUTED`. Reads the channels directly, as `is_muted()` is
/// undefined for a 0-channel volume.
fn sink_input_muted(mute: bool, volume: &ChannelVolumes) -> bool {
    let channels = volume.get();
    mute || (!channels.is_empty() && channels.iter().all(|&v| v == Volume::MUTED))
}

/// `playback_stream_props` on a binding proplist, for tests.
#[cfg(test)]
pub(crate) fn playback_stream(
    index: u32,
    proplist: &Proplist,
    corked: bool,
    mute: bool,
    volume: &ChannelVolumes,
) -> StreamProps {
    playback_stream_props(index, props(proplist), corked, mute, volume)
}

/// A sink input as `call_detect` sees it.
fn playback_stream_props(
    index: u32,
    props: BTreeMap<String, String>,
    corked: bool,
    mute: bool,
    volume: &ChannelVolumes,
) -> StreamProps {
    StreamProps {
        direction: StreamDirection::Playback,
        props,
        monitor: false,
        index,
        corked,
        muted: sink_input_muted(mute, volume),
    }
}

/// The production call source: a short-lived libpulse connection per
/// snapshot (PipeWire's pulse server included), as media_controller.rs makes
/// one per query. It lists the source outputs (capture streams) with their
/// property lists, marking those recording a monitor source, and the cards
/// with their active profile.
pub(crate) struct PulseStreams;

/// How long `PulsePlaybackStreams` waits for one listing: a reconnect, the
/// sink-input and client listings, plus a margin.
const PLAYBACK_LIST_BOUND: Duration = pulse::CONNECT_TIMEOUT
    .saturating_add(pulse::OPERATION_TIMEOUT)
    .saturating_add(pulse::OPERATION_TIMEOUT)
    .saturating_add(Duration::from_millis(500));

/// What a listing request carries: where its own reply goes, so a late
/// reply to a request that gave up is dropped, never read by the next one.
type ListingReply = std::sync::mpsc::SyncSender<io::Result<Vec<StreamProps>>>;

/// The production `PlaybackSource`: lists the sink inputs on one libpulse
/// connection that it keeps across listings (#124). libpulse's mainloop and
/// context can't move between threads, so they live on one dedicated
/// thread, which exits once this is dropped. `playback_streams` waits for
/// its own request at most `bound`; at most one request waits for the
/// thread, and a stalled thread is an error. A dead thread (never spawned,
/// or panicked) lists no streams at once, so stream playback fails safe to
/// off instead of keeping its last state (#134), and reports itself `dead`,
/// so live presence is unknown (None) and MPRIS counts as before #302.
pub(crate) struct PulsePlaybackStreams {
    requests: std::sync::mpsc::SyncSender<ListingReply>,
    bound: Duration,
    /// The thread is gone; its error is logged once.
    dead: bool,
}

impl PulsePlaybackStreams {
    pub(crate) fn new() -> Self {
        Self::spawn(PLAYBACK_LIST_BOUND, || {
            let mut connection = None;
            move || {
                list_reusing(
                    &mut connection,
                    || pulse::RawConnection::connect("LibrePods playback monitor"),
                    pulse::RawConnection::ready,
                    sink_inputs,
                )
            }
        })
    }

    /// Run the listings `make` builds, on the thread, for at most `bound`
    /// each.
    fn spawn<L>(bound: Duration, make: impl FnOnce() -> L + Send + 'static) -> Self
    where
        L: FnMut() -> io::Result<Vec<StreamProps>>,
    {
        let (requests, rx) = std::sync::mpsc::sync_channel::<ListingReply>(1);
        if let Err(e) = std::thread::Builder::new()
            .name("librepods-streams".to_string())
            .spawn(move || {
                let mut list = make();
                for reply in rx {
                    // The asker may have given up; its reply goes nowhere.
                    let _ = reply.send(list());
                }
            })
        {
            log::warn!("Stream monitor: no playback stream thread: {e}");
        }
        PulsePlaybackStreams {
            requests,
            bound,
            dead: false,
        }
    }
}

impl PlaybackSource for PulsePlaybackStreams {
    fn playback_streams(&mut self) -> io::Result<Vec<StreamProps>> {
        use std::sync::mpsc::TrySendError;
        let thread_error =
            |e: &dyn std::fmt::Display| io::Error::other(format!("playback stream thread: {e}"));
        let (reply, listing) = std::sync::mpsc::sync_channel(1);
        match self.requests.try_send(reply) {
            Ok(()) => match listing.recv_timeout(self.bound) {
                Err(RecvTimeoutError::Disconnected) => {}
                result => return result.map_err(|e| thread_error(&e))?,
            },
            Err(TrySendError::Disconnected(_)) => {}
            Err(e) => return Err(thread_error(&e)),
        }
        if !self.dead {
            self.dead = true;
            log::error!("Stream monitor: playback stream thread died; stream playback stays off");
        }
        Ok(Vec::new())
    }

    fn dead(&self) -> bool {
        self.dead
    }
}

/// List on the connection in `slot`: reuse it while `ready`, else replace
/// it with one `connect`. Any error drops it, so the next listing
/// reconnects and a cancelled operation's late reply is never read.
fn list_reusing<C, T>(
    slot: &mut Option<C>,
    connect: impl FnOnce() -> io::Result<C>,
    ready: impl FnOnce(&C) -> bool,
    list: impl FnOnce(&mut C) -> io::Result<T>,
) -> io::Result<T> {
    let connection = match slot.take().filter(ready) {
        Some(connection) => slot.insert(connection),
        None => slot.insert(connect()?),
    };
    let result = list(connection);
    if result.is_err() {
        *slot = None;
    }
    result
}

/// Whether some sink input could inherit a process id from its client: it
/// has a client (module streams have none) and no process id of its own.
fn needs_clients(inputs: &[pulse::SinkInput]) -> bool {
    inputs
        .iter()
        .any(|i| i.client.is_some() && stream_pid(&i.props, None).is_none())
}

/// Every sink input on `connection`, each with the process id it inherits
/// from its client (`with_client_pid`). The clients are listed only when
/// `needs_clients`: native PipeWire streams (mpv, pw-cat) need them.
fn sink_inputs(connection: &mut pulse::RawConnection) -> io::Result<Vec<StreamProps>> {
    let inputs = connection.sink_inputs(None)?;
    let clients = if needs_clients(&inputs) {
        connection.clients()?
    } else {
        Vec::new()
    };
    Ok(inputs
        .into_iter()
        .map(|i| {
            let client = clients.iter().find(|c| Some(c.index) == i.client);
            let props = with_client_pid(i.props, client.map(|c| &c.props));
            playback_stream_props(i.index, props, i.corked, i.mute, &i.volume)
        })
        .collect())
}

impl StreamSource for PulseStreams {
    fn snapshot(&mut self) -> io::Result<AudioSnapshot> {
        let mut raw = pulse::RawConnection::connect("LibrePods call monitor")?;
        let outputs: Vec<(u32, StreamProps)> = raw
            .source_outputs()?
            .into_iter()
            .map(|output| {
                let stream = StreamProps {
                    direction: StreamDirection::Capture,
                    props: output.props,
                    monitor: false,
                    index: output.index,
                    corked: output.corked,
                    muted: false,
                };
                (output.source, stream)
            })
            .collect();
        let cards = raw
            .cards()?
            .into_iter()
            .filter_map(|card| Some((card.name?, card.active_profile)))
            .collect();
        let monitors = if outputs.is_empty() {
            Vec::new()
        } else {
            raw.monitor_sources()?
        };
        let streams = mark_monitors(outputs, &monitors);
        Ok(AudioSnapshot { streams, cards })
    }
}

/// Each source output's stream, a `monitor` iff its source is one of
/// `monitors`, the monitor sources' indices.
fn mark_monitors(outputs: Vec<(u32, StreamProps)>, monitors: &[u32]) -> Vec<StreamProps> {
    outputs
        .into_iter()
        .map(|(source, stream)| StreamProps {
            monitor: monitors.contains(&source),
            ..stream
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn props(list: &Proplist) -> BTreeMap<String, String> {
    list.iter()
        .filter_map(|key| list.get_str(&key).map(|value| (key, value)))
        .collect()
}

/// Peak record streams' sample rate: one peak per 40 ms, as pavucontrol's
/// meters use.
const PEAK_RATE: u32 = 25;
/// How often the peak thread reads its streams, while it has any.
const PEAK_TICK: Duration = Duration::from_millis(40);
/// How long the peak thread waits before retrying a stream it could not
/// open: well under `SILENCE_HOLD`, so a counted stream whose record stream
/// is reopened (its sink input moved) keeps counting.
const PEAK_RETRY: Duration = Duration::from_secs(2);

/// The production `PeakSource`: per-stream peak detection on the sinks'
/// monitors, as pavucontrol meters a stream. libpulse's standard mainloop
/// can't move between threads, so it, its connections and the record
/// streams live on one dedicated thread, which exits once this is dropped.
/// `levels` only hands the candidates to that thread and takes the peaks it
/// has collected; it never waits on the sound server, so a stalled or dead
/// peak thread leaves every stream silent (stream playback never counts;
/// MPRIS still does).
pub(crate) struct PulsePeaks {
    candidates: CandidateTx,
    peaks: Arc<Mutex<HashMap<u32, f32>>>,
}

/// The candidate hand-over to the peak thread: at most one pending set, the
/// newest, so a stalled peak thread neither grows memory nor replays stale
/// sets. `closed` is set once the `CandidateTx` is dropped.
#[derive(Default)]
struct Slot {
    set: Option<HashSet<u32>>,
    closed: bool,
}

impl Slot {
    fn empty(&mut self) -> bool {
        self.set.is_none() && !self.closed
    }
}

type SharedSlot = Arc<(Mutex<Slot>, Condvar)>;

/// The producer side of the hand-over; dropping it closes the slot.
struct CandidateTx(SharedSlot);

/// The consumer side of the hand-over. A set offered before the producer
/// was dropped is still taken before `Disconnected`.
struct CandidateRx(SharedSlot);

fn candidate_slot() -> (CandidateTx, CandidateRx) {
    let shared = SharedSlot::default();
    (CandidateTx(shared.clone()), CandidateRx(shared))
}

impl CandidateTx {
    /// Replace the pending set with `set`; never waits on the consumer.
    fn offer(&self, set: HashSet<u32>) {
        let (slot, ready) = &*self.0;
        slot.lock().unwrap_or_else(PoisonError::into_inner).set = Some(set);
        ready.notify_one();
    }
}

impl Drop for CandidateTx {
    fn drop(&mut self) {
        let (slot, ready) = &*self.0;
        slot.lock().unwrap_or_else(PoisonError::into_inner).closed = true;
        ready.notify_one();
    }
}

impl CandidateRx {
    /// The pending set, waiting for one; `Disconnected` once the slot is
    /// closed and empty.
    fn take(&self) -> Result<HashSet<u32>, RecvTimeoutError> {
        let (slot, ready) = &*self.0;
        let slot = slot.lock().unwrap_or_else(PoisonError::into_inner);
        let mut slot = ready
            .wait_while(slot, Slot::empty)
            .unwrap_or_else(PoisonError::into_inner);
        slot.set.take().ok_or(RecvTimeoutError::Disconnected)
    }

    /// As `take`, but `Timeout` once `timeout` passes with nothing offered.
    fn take_timeout(&self, timeout: Duration) -> Result<HashSet<u32>, RecvTimeoutError> {
        let (slot, ready) = &*self.0;
        let slot = slot.lock().unwrap_or_else(PoisonError::into_inner);
        let (mut slot, _) = ready
            .wait_timeout_while(slot, timeout, Slot::empty)
            .unwrap_or_else(PoisonError::into_inner);
        match slot.set.take() {
            Some(set) => Ok(set),
            None if slot.closed => Err(RecvTimeoutError::Disconnected),
            None => Err(RecvTimeoutError::Timeout),
        }
    }

    #[cfg(test)]
    fn try_take(&self) -> Option<HashSet<u32>> {
        self.take_timeout(Duration::ZERO).ok()
    }
}

impl PulsePeaks {
    pub(crate) fn new() -> Self {
        let (candidates, rx) = candidate_slot();
        let peaks: Arc<Mutex<HashMap<u32, f32>>> = Arc::default();
        let thread_peaks = peaks.clone();
        if let Err(e) = std::thread::Builder::new()
            .name("librepods-peaks".to_string())
            .spawn(move || peak_thread(&rx, &thread_peaks))
        {
            log::warn!("Stream monitor: no peak thread, so streams read silent: {e}");
        }
        PulsePeaks { candidates, peaks }
    }
}

impl PeakSource for PulsePeaks {
    fn levels(&mut self, candidates: &HashSet<u32>) -> HashMap<u32, f32> {
        // A dead peak thread only leaves the streams silent.
        self.candidates.offer(candidates.clone());
        let mut peaks =
            std::mem::take(&mut *self.peaks.lock().unwrap_or_else(PoisonError::into_inner));
        peaks.retain(|index, _| candidates.contains(index));
        peaks
    }
}

/// A sink input's peak record stream on its own connection: PipeWire's pulse
/// server (1.6.6) delivers data for only one of several such streams on one
/// connection. `stream` is declared first, so it is dropped (disconnected)
/// before its context.
struct PeakStream {
    stream: Stream,
    context: Context,
}

/// The peak thread: keeps a `PeakStream` for each index of the latest
/// candidate set, drops it once the index leaves the set, and adds each
/// stream's highest peak to `peaks`, until `PulsePeaks` is dropped. A
/// stream that failed (its connection dropped, or its sink input moved or
/// ended) is reopened with the next candidate set; one that could not be
/// opened is retried after `PEAK_RETRY`.
fn peak_thread(candidates: &CandidateRx, peaks: &Mutex<HashMap<u32, f32>>) {
    let Some(mut mainloop) = Mainloop::new() else {
        log::warn!("Stream monitor: no peak mainloop, so streams read silent");
        return;
    };
    // Declared after the mainloop, so dropped before it.
    let mut streams: HashMap<u32, PeakStream> = HashMap::new();
    let mut retry: HashMap<u32, Instant> = HashMap::new();
    loop {
        // Idle (no stream to read) until the next candidate set.
        let next = if streams.is_empty() {
            candidates.take()
        } else {
            candidates.take_timeout(PEAK_TICK)
        };
        match next {
            Ok(wanted) => {
                streams.retain(|index, _| wanted.contains(index));
                retry.retain(|index, _| wanted.contains(index));
                let now = Instant::now();
                for index in wanted {
                    if streams.contains_key(&index) || retry.get(&index).is_some_and(|&at| now < at)
                    {
                        continue;
                    }
                    match peak_stream(&mut mainloop, index) {
                        Ok(stream) => {
                            streams.insert(index, stream);
                        }
                        Err(e) => {
                            log::debug!("Stream monitor: no peaks for sink input {index}: {e}");
                            retry.insert(index, now + PEAK_RETRY);
                        }
                    }
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
        // Dispatch everything that arrived since the last tick.
        loop {
            match mainloop.iterate(false) {
                IterateResult::Success(0) => break,
                IterateResult::Success(_) => {}
                _ => {
                    log::debug!("Stream monitor: peak mainloop failed");
                    streams.clear();
                    break;
                }
            }
        }
        let mut peaks = peaks.lock().unwrap_or_else(PoisonError::into_inner);
        streams.retain(|&index, peak| {
            if peak.context.get_state() != ContextState::Ready
                || matches!(
                    peak.stream.get_state(),
                    StreamState::Failed | StreamState::Terminated
                )
            {
                return false;
            }
            if let Some(level) = drain_peak(&mut peak.stream) {
                let max = peaks.entry(index).or_insert(0.0);
                *max = max.max(level);
            }
            true
        });
    }
}

/// A record stream of sink input `index`'s peaks, on a new connection:
/// mono FLOAT32 at `PEAK_RATE` on the monitor of the sink it plays to. It
/// sets no `media.role`, so it is no call input (#11) and gives the session
/// manager no reason to switch to a headset profile.
fn peak_stream(mainloop: &mut Mainloop, index: u32) -> io::Result<PeakStream> {
    let mut context = pulse::connect_on(mainloop, "LibrePods")?;
    // One short raw connection for both lookups.
    let mut raw = pulse::RawConnection::connect("LibrePods")?;
    let sink = raw.sink_inputs(Some(index))?.pop();
    let sink = sink.ok_or_else(|| pulse_error("sink input info"))?.sink;

    let monitor = raw
        .sinks(pulse::Sinks::Index(sink))?
        .pop()
        .ok_or_else(|| pulse_error("sink info"))?
        .monitor_source;

    let spec = Spec {
        format: Format::FLOAT32NE,
        rate: PEAK_RATE,
        channels: 1,
    };
    let mut stream =
        Stream::new(&mut context, "LibrePods", &spec, None).ok_or_else(|| pulse_error("stream"))?;
    stream
        .set_monitor_stream(index)
        .map_err(|_| pulse_error("set_monitor_stream"))?;
    let attr = BufferAttr {
        maxlength: u32::MAX,
        tlength: u32::MAX,
        prebuf: u32::MAX,
        minreq: u32::MAX,
        fragsize: size_of::<f32>() as u32,
    };
    stream
        .connect_record(
            Some(&monitor.to_string()),
            Some(&attr),
            StreamFlagSet::PEAK_DETECT
                | StreamFlagSet::ADJUST_LATENCY
                | StreamFlagSet::DONT_MOVE
                | StreamFlagSet::DONT_INHIBIT_AUTO_SUSPEND,
        )
        .map_err(|_| pulse_error("record stream"))?;
    Ok(PeakStream { stream, context })
}
/// The highest peak a stream has recorded since the last call, None when it
/// has nothing new (or isn't ready yet).
fn drain_peak(stream: &mut Stream) -> Option<f32> {
    let mut peak: Option<f32> = None;
    loop {
        match stream.peek() {
            Ok(PeekResult::Data(data)) => {
                let level = data
                    .chunks_exact(size_of::<f32>())
                    .map(|b| f32::from_ne_bytes([b[0], b[1], b[2], b[3]]).abs())
                    .fold(peak.unwrap_or(0.0), f32::max);
                peak = Some(level);
            }
            Ok(PeekResult::Hole(_)) => {}
            Ok(PeekResult::Empty) | Err(_) => return peak,
        }
        if stream.discard().is_err() {
            return peak;
        }
    }
}

/// A `PeakSource` for tests that makes every candidate audible.
#[cfg(test)]
pub(crate) struct AllAudible;

#[cfg(test)]
impl PeakSource for AllAudible {
    fn levels(&mut self, candidates: &HashSet<u32>) -> HashMap<u32, f32> {
        candidates.iter().map(|&index| (index, 1.0)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    const MAC: &str = "AA:BB:CC:DD:EE:FF";

    /// Plays back a script of snapshots and errors, then repeats the last
    /// snapshot.
    struct FakeSource {
        script: Arc<Mutex<VecDeque<io::Result<AudioSnapshot>>>>,
        last: AudioSnapshot,
    }

    impl FakeSource {
        fn new(script: Vec<io::Result<AudioSnapshot>>) -> Self {
            FakeSource {
                script: Arc::new(Mutex::new(script.into())),
                last: AudioSnapshot::default(),
            }
        }
    }

    impl StreamSource for FakeSource {
        fn snapshot(&mut self) -> io::Result<AudioSnapshot> {
            let next = self.script.lock().unwrap().pop_front();
            match next {
                Some(Ok(snapshot)) => {
                    self.last = snapshot.clone();
                    Ok(snapshot)
                }
                Some(Err(e)) => Err(stall(e)),
                None => Ok(self.last.clone()),
            }
        }
    }

    /// A pulse timeout, as `pulse::wait` returns it from a stalled server.
    fn timed_out<T>() -> io::Result<T> {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "PulseAudio timed out",
        ))
    }

    /// Fakes return a timeout only after a short stall, as pulse does at its
    /// deadline; other errors at once.
    fn stall(e: io::Error) -> io::Error {
        if e.kind() == io::ErrorKind::TimedOut {
            std::thread::sleep(Duration::from_millis(30));
        }
        e
    }

    fn call_stream() -> StreamProps {
        StreamProps {
            direction: StreamDirection::Capture,
            props: [("media.role".to_string(), "Communication".to_string())].into(),
            monitor: false,
            index: 0,
            corked: false,
            muted: false,
        }
    }

    fn idle() -> io::Result<AudioSnapshot> {
        Ok(AudioSnapshot::default())
    }

    fn in_call() -> io::Result<AudioSnapshot> {
        Ok(AudioSnapshot {
            streams: vec![call_stream()],
            cards: Vec::new(),
        })
    }

    fn hfp() -> io::Result<AudioSnapshot> {
        Ok(AudioSnapshot {
            streams: Vec::new(),
            cards: vec![(
                "bluez_card.AA_BB_CC_DD_EE_FF".to_string(),
                Some("headset-head-unit-msbc".to_string()),
            )],
        })
    }

    fn failure() -> io::Result<AudioSnapshot> {
        Err(io::Error::other("no sound server"))
    }

    #[test]
    fn poller_reports_first_sample_and_changes_only() {
        let mut source = FakeSource::new(vec![idle(), idle(), in_call(), in_call(), idle()]);
        let mut poller = CallPoller::default();
        assert_eq!(poller.poll(&mut source, MAC), Some(false));
        assert_eq!(poller.poll(&mut source, MAC), None);
        assert_eq!(poller.poll(&mut source, MAC), Some(true));
        assert_eq!(poller.poll(&mut source, MAC), None);
        assert_eq!(poller.poll(&mut source, MAC), Some(false));
        assert_eq!(poller.poll(&mut source, MAC), None);
    }

    #[test]
    fn poller_first_sample_in_call() {
        let mut source = FakeSource::new(vec![hfp(), in_call()]);
        let mut poller = CallPoller::default();
        assert_eq!(poller.poll(&mut source, MAC), Some(true));
        // A different reason for the same state is no change.
        assert_eq!(poller.poll(&mut source, MAC), None);
        // The head-unit profile of another device's card is not a call.
        assert_eq!(poller.poll(&mut source, "11:22:33:44:55:66"), None);
    }

    #[test]
    fn poller_keeps_last_state_on_source_error() {
        let mut source = FakeSource::new(vec![
            failure(),
            in_call(),
            failure(),
            failure(),
            in_call(),
            failure(),
            idle(),
        ]);
        let mut poller = CallPoller::default();
        // No state yet: an error reports nothing.
        assert_eq!(poller.poll(&mut source, MAC), None);
        assert_eq!(poller.poll(&mut source, MAC), Some(true));
        // Errors neither end nor repeat the call.
        assert_eq!(poller.poll(&mut source, MAC), None);
        assert_eq!(poller.poll(&mut source, MAC), None);
        assert_eq!(poller.poll(&mut source, MAC), None);
        assert_eq!(poller.poll(&mut source, MAC), None);
        assert_eq!(poller.poll(&mut source, MAC), Some(false));
    }

    #[test]
    fn poller_keeps_state_through_timed_out_snapshot() {
        let mut source =
            FakeSource::new(vec![in_call(), timed_out(), timed_out(), in_call(), idle()]);
        let mut poller = CallPoller::default();
        assert_eq!(poller.poll(&mut source, MAC), Some(true));
        // Timeouts neither end nor repeat the call...
        assert_eq!(poller.poll(&mut source, MAC), None);
        assert_eq!(poller.poll(&mut source, MAC), None);
        assert_eq!(poller.poll(&mut source, MAC), None);
        // ...and the next good sample still reports a change.
        assert_eq!(poller.poll(&mut source, MAC), Some(false));
    }

    #[test]
    fn poller_holds_call_through_failed_snapshot() {
        // A card listing that never reached End, as `pulse` reports it.
        let failed = Err(io::Error::other("PulseAudio card list ended early"));
        let mut source = FakeSource::new(vec![in_call(), failed, in_call()]);
        let mut poller = CallPoller::default();
        assert_eq!(poller.poll(&mut source, MAC), Some(true));
        // The failed snapshot does not end the call...
        assert_eq!(poller.poll(&mut source, MAC), None);
        // ...and the next good snapshot, still in the call, is no change.
        assert_eq!(poller.poll(&mut source, MAC), None);
    }

    #[tokio::test]
    async fn spawned_poller_sends_changes_and_stops_when_dropped() {
        let source = FakeSource::new(vec![
            idle(),
            idle(),
            in_call(),
            failure(),
            in_call(),
            idle(),
        ]);
        let script = source.script.clone();
        let mut rx = spawn_call_poller_with(MAC.to_string(), source, Duration::from_millis(5));
        let mut changes = Vec::new();
        for _ in 0..3 {
            let (_, in_call) = tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("a change")
                .expect("poller running");
            changes.push(in_call);
        }
        assert_eq!(changes, [false, true, false]);
        // Steady state afterwards: nothing more is sent.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), rx.recv())
                .await
                .is_err()
        );
        assert!(script.lock().unwrap().is_empty());
        // Dropping the receiver stops the task: the script is no longer
        // consumed.
        drop(rx);
        tokio::time::sleep(Duration::from_millis(50)).await;
        script.lock().unwrap().push_back(in_call());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(script.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn spawned_poller_survives_timeout_and_stops_when_dropped() {
        let source = FakeSource::new(vec![idle(), timed_out(), timed_out(), in_call()]);
        let script = source.script.clone();
        let mut rx = spawn_call_poller_with(MAC.to_string(), source, Duration::from_millis(5));
        let mut changes = Vec::new();
        for _ in 0..2 {
            let (_, in_call) = tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("a change")
                .expect("poller running");
            changes.push(in_call);
        }
        // The timeouts reported nothing; the snapshot after them did.
        assert_eq!(changes, [false, true]);
        assert!(script.lock().unwrap().is_empty());
        drop(rx);
        tokio::time::sleep(Duration::from_millis(50)).await;
        script.lock().unwrap().push_back(idle());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(script.lock().unwrap().len(), 1);
    }

    /// Plays back a script of sink-input listings and errors, then repeats
    /// the last listing.
    struct FakePlayback {
        script: Arc<Mutex<VecDeque<io::Result<Vec<StreamProps>>>>>,
        last: Vec<StreamProps>,
    }

    impl FakePlayback {
        fn new(script: Vec<io::Result<Vec<StreamProps>>>) -> Self {
            FakePlayback {
                script: Arc::new(Mutex::new(script.into())),
                last: Vec::new(),
            }
        }
    }

    impl PlaybackSource for FakePlayback {
        fn playback_streams(&mut self) -> io::Result<Vec<StreamProps>> {
            let next = self.script.lock().unwrap().pop_front();
            match next {
                Some(Ok(streams)) => {
                    self.last = streams.clone();
                    Ok(streams)
                }
                Some(Err(e)) => Err(stall(e)),
                None => Ok(self.last.clone()),
            }
        }
    }

    fn sink_input(index: u32, props: &[(&str, &str)], corked: bool) -> StreamProps {
        StreamProps {
            direction: StreamDirection::Playback,
            props: props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            monitor: false,
            index,
            corked,
            muted: false,
        }
    }

    fn music(index: u32) -> StreamProps {
        sink_input(
            index,
            &[("media.role", "Music"), ("application.name", "mpv")],
            false,
        )
    }

    fn corked_music(index: u32) -> StreamProps {
        StreamProps {
            corked: true,
            ..music(index)
        }
    }

    fn muted_music(index: u32) -> StreamProps {
        StreamProps {
            muted: true,
            ..music(index)
        }
    }

    fn event_sound(index: u32) -> StreamProps {
        sink_input(
            index,
            &[("media.role", "event"), ("application.name", "GNOME Shell")],
            false,
        )
    }

    const MIN: Duration = MIN_STREAM_PLAYBACK;
    const STEP: Duration = Duration::from_millis(500);

    impl PlaybackTracker {
        /// `observe_levels` with every candidate audible.
        fn observe(&mut self, now: Instant, streams: &[StreamProps]) -> Option<bool> {
            self.observe_levels(now, streams, |_| 1.0)
        }

        /// `poll_levels` with every candidate audible.
        fn poll(
            &mut self,
            source: &mut dyn PlaybackSource,
            clock: impl FnOnce() -> Instant,
        ) -> Option<bool> {
            self.poll_levels(source, &mut AllAudible, clock)
        }

        /// `poll_levels_rebaselining` with no re-baseline requested.
        fn poll_levels(
            &mut self,
            source: &mut dyn PlaybackSource,
            peaks: &mut dyn PeakSource,
            clock: impl FnOnce() -> Instant,
        ) -> Option<bool> {
            self.poll_levels_rebaselining(source, peaks, clock, || None)
        }

        /// One sample in the stream worker's order, without its lock:
        /// `sample`, then `clock` and `rebaseline_requested` (asked only
        /// over a successful listing), then `observe_sample`.
        fn poll_levels_rebaselining(
            &mut self,
            source: &mut dyn PlaybackSource,
            peaks: &mut dyn PeakSource,
            clock: impl FnOnce() -> Instant,
            rebaseline_requested: impl FnOnce() -> Option<Instant>,
        ) -> Option<bool> {
            let sample = self.sample(source, peaks)?;
            let now = clock();
            self.observe_sample(now, rebaseline_requested(), &sample)
        }
    }

    /// Feed `listings` to a fresh tracker, one per `STEP` from `start`, and
    /// return each sample's report.
    fn track(listings: &[Vec<StreamProps>]) -> Vec<Option<bool>> {
        let start = Instant::now();
        let mut tracker = started(start);
        listings
            .iter()
            .enumerate()
            .map(|(i, streams)| tracker.observe(start + STEP * i as u32, streams))
            .collect()
    }

    /// A tracker past its baseline: its first listing, at `start`, had no
    /// stream, so every stream it sees later is a new one.
    fn started(start: Instant) -> PlaybackTracker {
        let mut tracker = PlaybackTracker::new(MIN);
        assert_eq!(tracker.observe(start, &[]), None);
        tracker
    }

    #[test]
    fn preexisting_stream_counts_only_after_resuming() {
        let start = Instant::now();
        let mut tracker = PlaybackTracker::new(MIN);
        // Already playing in the first listing: the baseline, never a start,
        // however long it plays.
        for i in 0..20 {
            assert_eq!(tracker.observe(start + STEP * i, &[music(1)]), None, "{i}");
        }
        assert!(!tracker.playing);
        // Paused, then playing again: a new start, counted after the
        // minimum from the resume.
        let paused = start + STEP * 20;
        assert_eq!(tracker.observe(paused, &[corked_music(1)]), None);
        let resumed = paused + STEP;
        assert_eq!(tracker.observe(resumed, &[music(1)]), None);
        assert_eq!(
            tracker.observe(resumed + MIN - Duration::from_millis(1), &[music(1)]),
            None
        );
        assert_eq!(tracker.observe(resumed + MIN, &[music(1)]), Some(true));
        assert_eq!(tracker.observe(resumed + MIN * 2, &[]), Some(false));

        // Reclassified and back: a new start too.
        let mut tracker = PlaybackTracker::new(MIN);
        assert_eq!(tracker.observe(start, &[music(1)]), None);
        let event = sink_input(1, &[("media.role", "event")], false);
        assert_eq!(tracker.observe(start + STEP, &[event]), None);
        let back = start + STEP * 2;
        assert_eq!(tracker.observe(back, &[music(1)]), None);
        assert_eq!(
            tracker.observe(back + MIN - Duration::from_millis(1), &[music(1)]),
            None
        );
        assert_eq!(tracker.observe(back + MIN, &[music(1)]), Some(true));
    }

    #[test]
    fn new_stream_counts_beside_preexisting() {
        let start = Instant::now();
        // A new stream next to one playing at the baseline.
        let mut tracker = PlaybackTracker::new(MIN);
        assert_eq!(tracker.observe(start, &[music(1)]), None);
        let added = start + STEP;
        assert_eq!(tracker.observe(added, &[music(1), music(2)]), None);
        assert_eq!(tracker.observe(start + MIN, &[music(1), music(2)]), None);
        assert_eq!(
            tracker.observe(added + MIN, &[music(1), music(2)]),
            Some(true)
        );
        // The new stream ends; the preexisting one alone does not count.
        assert_eq!(tracker.observe(added + MIN * 2, &[music(1)]), Some(false));
        assert_eq!(tracker.observe(added + MIN * 4, &[music(1)]), None);

        // A stream corked in the first listing was not playing: once it
        // plays, it is a new start.
        let mut tracker = PlaybackTracker::new(MIN);
        assert_eq!(tracker.observe(start, &[corked_music(3)]), None);
        let played = start + STEP;
        assert_eq!(tracker.observe(played, &[music(3)]), None);
        assert_eq!(tracker.observe(played + MIN, &[music(3)]), Some(true));
    }

    #[test]
    fn baseline_is_the_first_successful_listing() {
        let start = Instant::now();
        let mut source = FakePlayback::new(vec![
            failure_streams(),
            Ok(vec![music(1)]),
            Ok(vec![music(1), music(2)]),
        ]);
        let mut tracker = PlaybackTracker::new(MIN);
        // A failed listing is no baseline...
        assert_eq!(tracker.poll(&mut source, || start), None);
        assert!(tracker.preexisting.is_none());
        // ...the first successful one is: music(1) never counts.
        assert_eq!(tracker.poll(&mut source, || start + STEP), None);
        assert_eq!(tracker.observe(start + MIN * 3, &[music(1)]), None);
        // A stream new since then counts after the minimum.
        let added = start + MIN * 4;
        assert_eq!(tracker.poll(&mut source, || added), None);
        assert_eq!(tracker.poll(&mut source, || added + MIN), Some(true));
    }

    /// A tracker past its empty baseline whose new stream `music(1)` has
    /// been audible since `start + STEP`, less than the minimum, when the
    /// core resets at `start + STEP * 2`, its next sample.
    fn rebaselined_while_debouncing(start: Instant) -> PlaybackTracker {
        let mut tracker = started(start);
        assert_eq!(tracker.observe(start + STEP, &[music(1)]), None);
        tracker.rebaseline(start + STEP * 2);
        tracker
    }

    #[test]
    fn tracker_rebaseline_debouncing_stream_never_counts_while_live() {
        let start = Instant::now();
        let mut tracker = rebaselined_while_debouncing(start);
        let rebaselined = start + STEP * 2;
        let mut now = rebaselined;
        while now <= rebaselined + MIN * 2 {
            assert_eq!(tracker.observe(now, &[music(1)]), None);
            now += STEP;
        }
        assert!(!tracker.playing);
        assert!(tracker.candidates(&[music(1)]).is_empty());
    }

    #[test]
    fn tracker_rebaseline_stream_counts_after_cork_and_replay() {
        let start = Instant::now();
        for stopped in [corked_music(1), muted_music(1)] {
            let mut tracker = rebaselined_while_debouncing(start);
            let rebaselined = start + STEP * 2;
            assert_eq!(tracker.observe(rebaselined + MIN, &[music(1)]), None);
            let paused = rebaselined + MIN * 2;
            assert_eq!(tracker.observe(paused, &[stopped.clone()]), None);
            let resumed = paused + STEP;
            assert_eq!(tracker.observe(resumed, &[music(1)]), None, "{stopped:?}");
            assert_eq!(
                tracker.observe(resumed + MIN - Duration::from_millis(1), &[music(1)]),
                None
            );
            assert_eq!(
                tracker.observe(resumed + MIN, &[music(1)]),
                Some(true),
                "{stopped:?}"
            );
        }
    }

    #[test]
    fn tracker_rebaseline_keeps_counted_stream() {
        let start = Instant::now();
        let mut tracker = counting(start);
        tracker.rebaseline(start + MIN);
        assert!(tracker.playing);
        let mut now = start + MIN + STEP;
        while now <= start + MIN * 4 {
            assert_eq!(tracker.observe_levels(now, &[music(1)], |_| LOUD), None);
            now += STEP;
        }
        assert!(tracker.playing);
        // It still stops as a counted stream does.
        assert_eq!(tracker.observe(now, &[]), Some(false));
    }

    #[test]
    fn tracker_rebaseline_later_starts_count_after_min() {
        let start = Instant::now();
        // A live but silent candidate at the re-baseline, audible after it.
        let mut silent = started(start);
        assert_eq!(
            silent.observe_levels(start + STEP, &[music(1)], |_| SILENT),
            None
        );
        silent.rebaseline(start + STEP);
        // A stream first seen after the re-baseline.
        let mut fresh = started(start);
        fresh.rebaseline(start + STEP);
        for (case, mut tracker) in [("silent", silent), ("fresh", fresh)] {
            let audible = start + STEP * 2;
            assert_eq!(tracker.observe(audible, &[music(1)]), None, "{case}");
            assert_eq!(
                tracker.observe(audible + MIN - Duration::from_millis(1), &[music(1)]),
                None,
                "{case}"
            );
            assert_eq!(
                tracker.observe(audible + MIN, &[music(1)]),
                Some(true),
                "{case}"
            );
        }
    }

    #[test]
    fn tracker_rebaseline_late_request_keeps_start_after_reset() {
        let reset = Instant::now();
        let mut tracker = started(reset);
        // First audible after the core's reset, before its request arrives.
        let audible = reset + Duration::from_millis(500);
        assert_eq!(tracker.observe(audible, &[music(1)]), None);
        let mut source = FakePlayback::new(vec![Ok(vec![music(1)])]);
        let applied = tracker.poll_levels_rebaselining(
            &mut source,
            &mut AllAudible,
            || reset + Duration::from_millis(1000),
            || Some(reset),
        );
        assert_eq!(applied, None);
        assert_eq!(
            tracker.observe(audible + MIN - Duration::from_millis(1), &[music(1)]),
            None
        );
        assert_eq!(tracker.observe(audible + MIN, &[music(1)]), Some(true));
    }

    #[test]
    fn tracker_rebaseline_before_first_listing_changes_nothing() {
        let start = Instant::now();
        let mut source = FakePlayback::new(vec![
            failure_streams(),
            Ok(vec![music(1)]),
            Ok(vec![music(1), music(2)]),
        ]);
        let mut tracker = PlaybackTracker::new(MIN);
        tracker.rebaseline(start);
        assert!(tracker.preexisting.is_none());
        // A request over a failed listing waits; over the first successful
        // one it moves nothing: that listing is the baseline.
        let failed = tracker.poll_levels_rebaselining(
            &mut source,
            &mut AllAudible,
            || start,
            || panic!("asked over a failed listing"),
        );
        assert_eq!(failed, None);
        let first = tracker.poll_levels_rebaselining(
            &mut source,
            &mut AllAudible,
            || start,
            || Some(start),
        );
        assert_eq!(first, None);
        assert_eq!(tracker.preexisting, Some(HashSet::from([1])));
        assert_eq!(tracker.observe(start + MIN * 3, &[music(1)]), None);
        let added = start + MIN * 4;
        assert_eq!(tracker.poll(&mut source, || added), None);
        assert_eq!(tracker.poll(&mut source, || added + MIN), Some(true));
    }
    #[test]
    fn min_stream_playback_is_a_short_debounce() {
        assert!(MIN_STREAM_PLAYBACK >= Duration::from_secs(1));
        assert!(MIN_STREAM_PLAYBACK <= Duration::from_secs(3));
    }

    #[test]
    fn short_stream_never_counts() {
        let start = Instant::now();
        let mut tracker = started(start);
        assert_eq!(tracker.observe(start, &[music(7)]), None);
        // Just short of the minimum, then gone.
        let almost = start + MIN - Duration::from_millis(1);
        assert_eq!(tracker.observe(almost, &[music(7)]), None);
        assert_eq!(tracker.observe(start + MIN, &[]), None);
        assert_eq!(tracker.observe(start + MIN * 3, &[]), None);
        assert!(!tracker.playing);
        // Corked before the minimum: the same.
        let reports = track(&[
            vec![music(8)],
            vec![music(8)],
            vec![corked_music(8)],
            vec![corked_music(8)],
            vec![corked_music(8)],
            vec![corked_music(8)],
            vec![corked_music(8)],
        ]);
        assert!(reports.iter().all(Option::is_none), "{reports:?}");
    }

    #[test]
    fn long_stream_reports_one_start_and_stop() {
        let start = Instant::now();
        let mut tracker = started(start);
        assert_eq!(tracker.observe(start, &[music(3)]), None);
        assert_eq!(tracker.observe(start + MIN / 2, &[music(3)]), None);
        assert_eq!(tracker.observe(start + MIN, &[music(3)]), Some(true));
        assert!(tracker.playing);
        // Still playing, with other streams coming and going: no change.
        assert_eq!(
            tracker.observe(start + MIN * 2, &[music(3), event_sound(9)]),
            None
        );
        assert_eq!(
            tracker.observe(start + MIN * 3, &[music(3), music(4)]),
            None
        );
        assert_eq!(tracker.observe(start + MIN * 4, &[music(3)]), None);
        // It ends: exactly one stop.
        assert_eq!(tracker.observe(start + MIN * 5, &[]), Some(false));
        assert_eq!(tracker.observe(start + MIN * 6, &[]), None);
        assert!(!tracker.playing);
        // Corking ends it too.
        let reports = track(&[
            vec![music(5)],
            vec![music(5)],
            vec![music(5)],
            vec![music(5)],
            vec![music(5)],
            vec![corked_music(5)],
            vec![corked_music(5)],
        ]);
        assert_eq!(
            reports,
            [None, None, None, None, Some(true), Some(false), None]
        );
    }

    #[test]
    fn corked_stream_restarts_debounce() {
        let start = Instant::now();
        let mut tracker = started(start);
        assert_eq!(tracker.observe(start, &[music(1)]), None);
        assert_eq!(tracker.observe(start + STEP * 3, &[corked_music(1)]), None);
        // Uncorked again: the timer starts over from here.
        let resumed = start + STEP * 4;
        assert_eq!(tracker.observe(resumed, &[music(1)]), None);
        assert_eq!(tracker.observe(start + MIN + STEP, &[music(1)]), None);
        assert_eq!(
            tracker.observe(resumed + MIN - Duration::from_millis(1), &[music(1)]),
            None
        );
        assert_eq!(tracker.observe(resumed + MIN, &[music(1)]), Some(true));
        // A stream that vanishes and comes back under a new index starts
        // over as well.
        let mut tracker = started(start);
        assert_eq!(tracker.observe(start, &[music(1)]), None);
        assert_eq!(tracker.observe(start + STEP * 3, &[music(2)]), None);
        assert_eq!(tracker.observe(start + MIN, &[music(2)]), None);
        assert_eq!(
            tracker.observe(start + STEP * 3 + MIN, &[music(2)]),
            Some(true)
        );
    }

    #[test]
    fn reclassified_stream_restarts_debounce() {
        let start = Instant::now();
        let mut tracker = started(start);
        assert_eq!(tracker.observe(start, &[music(1)]), None);
        // The same index now reports an event role: forgotten.
        let event = sink_input(1, &[("media.role", "event")], false);
        assert_eq!(tracker.observe(start + STEP, &[event]), None);
        let back = start + STEP * 2;
        assert_eq!(tracker.observe(back, &[music(1)]), None);
        assert_eq!(tracker.observe(start + MIN, &[music(1)]), None);
        assert_eq!(tracker.observe(back + MIN, &[music(1)]), Some(true));
    }

    #[test]
    fn muted_stream_never_counts() {
        let start = Instant::now();
        let mut tracker = started(start);
        for i in 0..20 {
            assert_eq!(
                tracker.observe(start + STEP * i, &[muted_music(1)]),
                None,
                "{i}"
            );
        }
        assert!(!tracker.playing);
    }

    #[test]
    fn muting_stops_playback() {
        let start = Instant::now();
        let mut tracker = started(start);
        assert_eq!(tracker.observe(start, &[music(1)]), None);
        assert_eq!(tracker.observe(start + MIN, &[music(1)]), Some(true));
        assert_eq!(
            tracker.observe(start + MIN + STEP, &[muted_music(1)]),
            Some(false)
        );
        assert_eq!(tracker.observe(start + MIN * 3, &[muted_music(1)]), None);
    }

    #[test]
    fn unmute_restarts_debounce() {
        let start = Instant::now();
        let mut tracker = started(start);
        assert_eq!(tracker.observe(start, &[music(1)]), None);
        assert_eq!(tracker.observe(start + STEP * 3, &[muted_music(1)]), None);
        // Unmuted again: the timer starts over from here.
        let unmuted = start + STEP * 4;
        assert_eq!(tracker.observe(unmuted, &[music(1)]), None);
        assert_eq!(tracker.observe(start + MIN + STEP, &[music(1)]), None);
        assert_eq!(
            tracker.observe(unmuted + MIN - Duration::from_millis(1), &[music(1)]),
            None
        );
        assert_eq!(tracker.observe(unmuted + MIN, &[music(1)]), Some(true));
    }

    fn volume(channels: &[Volume]) -> ChannelVolumes {
        let mut volume = ChannelVolumes::default();
        volume.set_len(channels.len() as u8);
        volume.get_mut().copy_from_slice(channels);
        volume
    }

    #[test]
    fn sink_input_muted_flag() {
        assert!(sink_input_muted(true, &volume(&[Volume::NORMAL; 2])));
    }

    #[test]
    fn sink_input_muted_zero_volume() {
        assert!(sink_input_muted(false, &volume(&[Volume::MUTED; 2])));
        assert!(!sink_input_muted(
            false,
            &volume(&[Volume::MUTED, Volume(1)])
        ));
    }

    #[test]
    fn sink_input_muted_normal() {
        assert!(!sink_input_muted(false, &volume(&[Volume::NORMAL; 2])));
    }

    #[test]
    fn sink_input_muted_empty_volume() {
        assert!(!sink_input_muted(false, &ChannelVolumes::default()));
    }

    #[test]
    fn event_stream_never_counts() {
        let start = Instant::now();
        let mut tracker = started(start);
        let streams = [
            event_sound(1),
            sink_input(
                2,
                &[
                    ("event.id", "message-new-instant"),
                    ("application.name", "x"),
                ],
                false,
            ),
            sink_input(3, &[("media.role", "phone")], false),
            sink_input(4, &[], false),
            sink_input(
                5,
                &[("media.role", "Music"), ("stream.monitor", "true")],
                false,
            ),
        ];
        for i in 0..60 {
            assert_eq!(tracker.observe(start + STEP * i, &streams), None, "{i}");
        }
        assert!(!tracker.playing);
    }

    #[test]
    fn playback_tracker_keeps_state_on_source_error() {
        let start = Instant::now();
        let mut source = FakePlayback::new(vec![
            failure_streams(),
            Ok(vec![music(1)]),
            Ok(vec![music(1)]),
            failure_streams(),
            Ok(vec![music(1)]),
            failure_streams(),
            failure_streams(),
            Ok(vec![]),
        ]);
        let mut tracker = started(start);
        // No state yet: an error reports nothing.
        assert_eq!(tracker.poll(&mut source, || start), None);
        assert_eq!(tracker.poll(&mut source, || start), None);
        assert_eq!(tracker.poll(&mut source, || start + STEP), None);
        // An error neither restarts the stream's timer nor reports.
        assert_eq!(tracker.poll(&mut source, || start + MIN), None);
        assert_eq!(tracker.poll(&mut source, || start + MIN), Some(true));
        // Errors neither end nor repeat the playback.
        assert_eq!(tracker.poll(&mut source, || start + MIN * 2), None);
        assert_eq!(tracker.poll(&mut source, || start + MIN * 3), None);
        assert!(tracker.playing);
        assert_eq!(tracker.poll(&mut source, || start + MIN * 4), Some(false));
    }

    fn failure_streams() -> io::Result<Vec<StreamProps>> {
        Err(io::Error::other("no sound server"))
    }

    /// Sink input 4, uncorked and unmuted, as PulseStreams maps it.
    fn unmuted_input(proplist: &Proplist) -> StreamProps {
        playback_stream(4, proplist, false, false, &ChannelVolumes::default())
    }

    #[test]
    fn truncated_sink_input_list_keeps_tracker_state() {
        // Sink inputs built as PulseStreams builds them, from a proplist.
        let mut proplist = Proplist::new().expect("proplist");
        proplist.set_str("media.role", "Music").unwrap();
        proplist.set_str("application.name", "mpv").unwrap();
        let complete = Ok(vec![unmuted_input(&proplist)]);
        let streams = complete.as_ref().expect("a complete list").clone();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].direction, StreamDirection::Playback);
        assert_eq!(streams[0].index, 4);
        assert!(!streams[0].corked);
        assert_eq!(
            streams[0].props.get("media.role").map(String::as_str),
            Some("Music")
        );
        // A listing with no End or with an Error, as `pulse` reports it: an
        // Err snapshot, not one stream (or none).
        let truncated = Err(io::Error::other("PulseAudio sink input info ended early"));
        let empty_cut = Err(io::Error::other(
            "PulseAudio sink input info reported an error",
        ));

        let start = Instant::now();
        let mut source = FakePlayback::new(vec![
            complete,
            Ok(streams.clone()),
            truncated,
            empty_cut,
            Ok(streams.clone()),
        ]);
        let mut tracker = started(start);
        assert_eq!(tracker.poll(&mut source, || start), None);
        assert_eq!(tracker.poll(&mut source, || start + MIN), Some(true));
        // The failed listings do not end the playback...
        assert_eq!(tracker.poll(&mut source, || start + MIN * 2), None);
        assert_eq!(tracker.poll(&mut source, || start + MIN * 3), None);
        assert!(tracker.playing);
        // ...and the next good listing, still playing, is no change.
        assert_eq!(tracker.poll(&mut source, || start + MIN * 4), None);
        assert!(tracker.playing);
    }

    #[tokio::test]
    async fn spawned_stream_playback_publishes_and_stops_when_dropped() {
        let source = FakePlayback::new(vec![Ok(vec![]), Ok(vec![music(1)])]);
        let script = source.script.clone();
        let mut rx = spawn_stream_playback(
            source,
            AllAudible,
            Duration::from_millis(50),
            Duration::from_millis(5),
            Default::default(),
            |_| (),
        );
        assert!(!rx.borrow().0.counted);
        tokio::time::timeout(Duration::from_secs(3), rx.wait_for(|(s, _)| s.counted))
            .await
            .expect("stream playback")
            .expect("monitor running");
        script.lock().unwrap().push_back(Ok(vec![]));
        tokio::time::timeout(Duration::from_secs(3), rx.wait_for(|(s, _)| !s.counted))
            .await
            .expect("stream playback ended")
            .expect("monitor running");
        // Dropping the receiver stops the task.
        drop(rx);
        tokio::time::sleep(Duration::from_millis(50)).await;
        script.lock().unwrap().push_back(Ok(vec![music(2)]));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(script.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn spawned_stream_playback_survives_timeout_and_stops_when_dropped() {
        let source = FakePlayback::new(vec![Ok(vec![]), timed_out(), Ok(vec![music(1)])]);
        let script = source.script.clone();
        let mut rx = spawn_stream_playback(
            source,
            AllAudible,
            Duration::from_millis(50),
            Duration::from_millis(5),
            Default::default(),
            |_| (),
        );
        tokio::time::timeout(Duration::from_secs(3), rx.wait_for(|(s, _)| s.counted))
            .await
            .expect("stream playback after a timeout")
            .expect("monitor running");
        // A timeout while playing keeps the state; the next listing ends it.
        script
            .lock()
            .unwrap()
            .extend([timed_out(), timed_out(), Ok(vec![])]);
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(rx.borrow().0.counted);
        tokio::time::timeout(Duration::from_secs(3), rx.wait_for(|(s, _)| !s.counted))
            .await
            .expect("stream playback ended")
            .expect("monitor running");
        assert!(script.lock().unwrap().is_empty());
        drop(rx);
        tokio::time::sleep(Duration::from_millis(50)).await;
        script.lock().unwrap().push_back(Ok(vec![music(2)]));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(script.lock().unwrap().len(), 1);
    }

    /// #302: a stream in the first listing is live but never counted; live
    /// is unknown before that listing, kept through failed listings, and
    /// off once the stream corks.
    #[tokio::test]
    async fn spawned_stream_playback_publishes_live_baseline_stream() {
        // No such process, so /proc names no parent.
        const PID: u32 = 99_999_999;
        let player = with_pid(music(1), PID);
        let source = FakePlayback::new(vec![Ok(vec![player.clone()])]);
        let script = source.script.clone();
        let mut rx = spawn_stream_playback(
            source,
            AllAudible,
            Duration::from_millis(50),
            Duration::from_millis(5),
            Default::default(),
            |_| (),
        );
        assert_eq!(rx.borrow().0.live, None);
        let owned = Some(HashSet::from([PID]));
        tokio::time::timeout(
            Duration::from_secs(3),
            rx.wait_for(|(s, _)| s.live == owned),
        )
        .await
        .expect("live baseline stream")
        .expect("monitor running");
        script
            .lock()
            .unwrap()
            .extend([timed_out(), timed_out(), Ok(vec![player.clone()])]);
        // Past the minimum and the failed listings: no change at all.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(script.lock().unwrap().is_empty());
        assert!(!rx.has_changed().unwrap());
        assert_eq!(
            rx.borrow().0,
            PlaybackState {
                counted: false,
                live: owned
            }
        );
        let corked = StreamProps {
            corked: true,
            ..player
        };
        script.lock().unwrap().push_back(Ok(vec![corked]));
        tokio::time::timeout(
            Duration::from_secs(3),
            rx.wait_for(|(s, _)| s.live == Some(HashSet::new())),
        )
        .await
        .expect("corked stream not live")
        .expect("monitor running");
        assert!(!rx.borrow().0.counted);
    }

    /// `stream` with `PROCESS_ID` set to `pid`.
    fn with_pid(mut stream: StreamProps, pid: u32) -> StreamProps {
        stream.props.insert(PROCESS_ID.to_string(), pid.to_string());
        stream
    }

    /// A pulse client's properties.
    fn client(props: &[(&str, &str)]) -> BTreeMap<String, String> {
        props
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// The MPRIS player's bus owner in the own-stream tests.
    const OWNER: u32 = 1000;

    /// Whether the player owned by `owner` owns a live stream of the
    /// listing `streams`, each with its client's properties, with the
    /// parents `ppids` (child, parent) as `/proc` would give them.
    fn owns(
        owner: Option<u32>,
        streams: &[(StreamProps, Option<BTreeMap<String, String>>)],
        ppids: &[(u32, u32)],
    ) -> bool {
        let streams: Vec<StreamProps> = streams
            .iter()
            .map(|(stream, client)| StreamProps {
                props: with_client_pid(stream.props.clone(), client.as_ref()),
                ..stream.clone()
            })
            .collect();
        let ppid = |pid| ppids.iter().find(|&&(c, _)| c == pid).map(|&(_, p)| p);
        let state = PlaybackState {
            counted: false,
            live: Some(live_stream_pids(&streams, ppid)),
        };
        state.live_or_unknown(owner)
    }

    /// (a) A remote-control player beside an unrelated `pw-play` stream.
    #[test]
    fn own_stream_unrelated_pw_play_not_matched() {
        let pw_play = sink_input(
            1,
            &[
                ("application.name", "pw-play"),
                ("media.role", "music"),
                (PROCESS_ID, "2000"),
            ],
            false,
        );
        assert!(!owns(Some(OWNER), &[(pw_play, None)], &[(2000, 1)]));
    }

    /// (b) The stream's own process id is the owner.
    #[test]
    fn own_stream_same_pid_matched() {
        assert!(owns(Some(OWNER), &[(with_pid(music(1), OWNER), None)], &[]));
    }

    /// (c) mpv or pw-cat: no process id on the stream, the owner's on its
    /// client; the client's process id wins over its `pipewire.sec.pid`
    /// (pipewire-pulse's own).
    #[test]
    fn own_stream_client_pid_matched() {
        let native = client(&[(PROCESS_ID, "1000")]);
        assert!(owns(Some(OWNER), &[(music(1), Some(native))], &[]));
        let pulse = client(&[(PROCESS_ID, "1000"), ("pipewire.sec.pid", "3000")]);
        assert!(owns(Some(OWNER), &[(music(1), Some(pulse))], &[]));
        // With no process id anywhere else, `pipewire.sec.pid` is it.
        let sec = client(&[("pipewire.sec.pid", "1000")]);
        assert!(owns(Some(OWNER), &[(music(1), Some(sec))], &[]));
        let pipewire_pulse = client(&[("pipewire.sec.pid", "3000")]);
        assert!(!owns(Some(OWNER), &[(music(1), Some(pipewire_pulse))], &[]));
    }

    /// (d) A stream of the owner's grandchild (a browser's audio process).
    #[test]
    fn own_stream_grandchild_matched() {
        let stream = with_pid(music(1), 1002);
        let ppids = [(1002, 1001), (1001, OWNER), (OWNER, 1)];
        assert!(owns(Some(OWNER), &[(stream, None)], &ppids));
    }

    /// (e) A sibling's stream is not the owner's, and a cycle in the
    /// parents ends without matching.
    #[test]
    fn own_stream_sibling_and_cycle_not_matched() {
        let sibling = with_pid(music(1), 1003);
        let ppids = [(1003, 999), (OWNER, 999), (999, 1)];
        assert!(!owns(Some(OWNER), &[(sibling, None)], &ppids));
        let looped = with_pid(music(1), 1004);
        let cycle = [(1004, 1005), (1005, 1004)];
        assert!(!owns(Some(OWNER), &[(looped, None)], &cycle));
    }

    /// The ancestors walked stop at `OWNER_DEPTH`.
    #[test]
    fn own_stream_ancestry_is_bounded() {
        let chain: Vec<(u32, u32)> = (0..20).map(|i| (2000 + i, 2001 + i)).collect();
        let stream = [(with_pid(music(1), 2000), None)];
        let depth = OWNER_DEPTH as u32;
        assert!(owns(Some(2000 + depth), &stream, &chain));
        assert!(!owns(Some(2001 + depth), &stream, &chain));
    }

    /// One stream's walk cut at `OWNER_DEPTH` doesn't cut another's short:
    /// the owner, 16 ancestors above 1016 but 17 above 1017, matches in
    /// either listing order.
    #[test]
    fn own_stream_walks_are_independent_of_order() {
        let chain: Vec<(u32, u32)> = (1001..=1017).map(|pid| (pid, pid - 1)).collect();
        let (deep, shallow) = (with_pid(music(1), 1017), with_pid(music(2), 1016));
        let orders = [
            [(deep.clone(), None), (shallow.clone(), None)],
            [(shallow, None), (deep, None)],
        ];
        for streams in &orders {
            assert!(owns(Some(OWNER), streams, &chain));
        }
    }

    /// (f) The owner's corked or event stream is not live.
    #[test]
    fn own_stream_not_live_not_matched() {
        for stream in [corked_music(1), event_sound(1)] {
            let stream = with_pid(stream, OWNER);
            assert!(
                !owns(Some(OWNER), &[(stream.clone(), None)], &[]),
                "{stream:?}"
            );
        }
    }

    /// #328: the owner's uncorked stream muted by flag or at every channel's
    /// `Volume::MUTED` (a player's slider at 0) is still its own live one.
    #[test]
    fn own_stream_muted_matched() {
        let mut proplist = Proplist::new().expect("proplist");
        proplist.set_str("media.role", "Music").unwrap();
        proplist.set_str(PROCESS_ID, &OWNER.to_string()).unwrap();
        let silenced = [
            with_pid(muted_music(1), OWNER),
            playback_stream(1, &proplist, false, false, &volume(&[Volume::MUTED; 2])),
            playback_stream(1, &proplist, false, true, &volume(&[Volume::NORMAL; 2])),
        ];
        for stream in silenced {
            assert!(stream.muted, "{stream:?}");
            assert!(
                owns(Some(OWNER), &[(stream.clone(), None)], &[]),
                "{stream:?}"
            );
        }
    }

    /// #328: a muted own stream is live for MPRIS only; it never counts.
    #[test]
    fn own_stream_muted_live_but_not_counted() {
        let listing = [with_pid(muted_music(1), OWNER)];
        assert!(live_streams(&listing).is_empty());
        assert!(started(Instant::now()).candidates(&listing).is_empty());
        assert!(live_stream_pids(&listing, |_| None).contains(&OWNER));
    }

    /// (c4) A failed owner lookup owns nothing once a listing is known, and
    /// plays while it is not; a live stream with no process id by any key
    /// is nobody's.
    #[test]
    fn own_stream_live_or_unknown_failed_lookup_and_no_pid() {
        let stream = with_pid(music(1), OWNER);
        assert!(!owns(None, &[(stream, None)], &[]));
        assert!(PlaybackState::default().live_or_unknown(None));
        assert!(PlaybackState::default().live_or_unknown(Some(OWNER)));
        let anonymous = client(&[("application.name", "mpv")]);
        assert!(!owns(Some(OWNER), &[(music(1), Some(anonymous))], &[]));
        assert!(!owns(Some(OWNER), &[(music(1), None)], &[]));
    }

    #[test]
    fn own_stream_proc_ppid_reads_this_process() {
        let parent = proc_ppid(std::process::id()).expect("this process's parent");
        assert_eq!(parent, std::os::unix::process::parent_id());
        assert_eq!(proc_ppid(99_999_999), None);
    }

    /// A sound server slow to answer once: the first listing (the baseline)
    /// has no stream, the second blocks for `delay` and returns a music
    /// stream that only appeared as it returned (noting when), and every
    /// later one returns that stream at once.
    struct SlowListing {
        calls: usize,
        delay: Duration,
        returned: Arc<Mutex<Option<Instant>>>,
    }

    impl PlaybackSource for SlowListing {
        fn playback_streams(&mut self) -> io::Result<Vec<StreamProps>> {
            self.calls += 1;
            match self.calls {
                1 => Ok(Vec::new()),
                2 => {
                    std::thread::sleep(self.delay);
                    *self.returned.lock().unwrap() = Some(Instant::now());
                    Ok(vec![music(1)])
                }
                _ => Ok(vec![music(1)]),
            }
        }
    }

    #[tokio::test]
    async fn delayed_listing_does_not_shorten_debounce() {
        let min = Duration::from_millis(300);
        let returned = Arc::new(Mutex::new(None));
        let source = SlowListing {
            calls: 0,
            // Longer than the minimum: timed from before the listing, the
            // stream would count at once.
            delay: min + Duration::from_millis(200),
            returned: returned.clone(),
        };
        let mut rx = spawn_stream_playback(
            source,
            AllAudible,
            min,
            Duration::from_millis(5),
            Default::default(),
            |_| (),
        );
        tokio::time::timeout(Duration::from_secs(5), rx.wait_for(|(s, _)| s.counted))
            .await
            .expect("stream playback")
            .expect("monitor running");
        let playing_seen = Instant::now();
        let returned = returned.lock().unwrap().expect("the slow listing returned");
        let counted_after = playing_seen.duration_since(returned);
        assert!(
            counted_after >= min,
            "counted {counted_after:?} after the stream was first seen; the minimum is {min:?}"
        );
    }

    const LOUD: f32 = 0.5;
    const SILENT: f32 = 0.0;

    /// Records each candidate set it is asked for (sorted) and gives every
    /// candidate `level`, or no level at all when None.
    struct RecordingPeaks {
        asked: Vec<Vec<u32>>,
        level: Option<f32>,
    }

    impl RecordingPeaks {
        fn new(level: Option<f32>) -> Self {
            RecordingPeaks {
                asked: Vec::new(),
                level,
            }
        }
    }

    impl PeakSource for RecordingPeaks {
        fn levels(&mut self, candidates: &HashSet<u32>) -> HashMap<u32, f32> {
            let mut asked: Vec<u32> = candidates.iter().copied().collect();
            asked.sort();
            self.asked.push(asked);
            let Some(level) = self.level else {
                return HashMap::new();
            };
            candidates.iter().map(|&index| (index, level)).collect()
        }
    }

    /// A paused SDL game's stream: role Game with an application name.
    fn game(index: u32) -> StreamProps {
        sink_input(
            index,
            &[
                ("media.role", "Game"),
                ("application.name", "SDL Application"),
            ],
            false,
        )
    }

    /// A tracker whose new stream `music(1)`, audible from `start`, counts
    /// from `start + MIN`.
    fn counting(start: Instant) -> PlaybackTracker {
        let mut tracker = started(start);
        assert_eq!(tracker.observe_levels(start, &[music(1)], |_| LOUD), None);
        assert_eq!(
            tracker.observe_levels(start + MIN, &[music(1)], |_| LOUD),
            Some(true)
        );
        tracker
    }

    #[test]
    fn silent_stream_never_counts() {
        let start = Instant::now();
        let samples = (4 * MIN.as_millis() / STEP.as_millis()) as u32 + 1;
        // Digital silence, and peaks at (not above) the threshold.
        for level in [SILENT, AUDIBLE_PEAK] {
            let mut tracker = started(start);
            for i in 1..=samples {
                let now = start + STEP * i;
                assert_eq!(
                    tracker.observe_levels(now, &[game(1)], |_| level),
                    None,
                    "{level} {i}"
                );
            }
            assert!(!tracker.playing);
        }
        // No level at all, through `poll_levels`: a candidate every sample
        // after the empty baseline, never counted.
        let mut source = FakePlayback::new(vec![Ok(vec![]), Ok(vec![game(1)])]);
        let mut peaks = RecordingPeaks::new(None);
        let mut tracker = PlaybackTracker::new(MIN);
        for i in 0..=samples {
            let now = start + STEP * i;
            assert_eq!(tracker.poll_levels(&mut source, &mut peaks, || now), None);
        }
        assert_eq!(peaks.asked[0], Vec::<u32>::new());
        assert!(peaks.asked[1..].iter().all(|asked| asked == &[1]));
        assert!(!tracker.playing);
    }

    #[test]
    fn audible_blip_never_counts() {
        let start = Instant::now();
        let tab = sink_input(1, &[("application.name", "Chromium")], false);
        let mut tracker = started(start);
        let samples = (2 * MIN.as_millis() / STEP.as_millis()) as u32 + 2;
        for i in 1..=samples {
            let level = if i == 1 { LOUD } else { SILENT };
            let now = start + STEP * i;
            assert_eq!(
                tracker.observe_levels(now, std::slice::from_ref(&tab), |_| level),
                None,
                "{i}"
            );
        }
        assert!(!tracker.playing);
    }

    #[test]
    fn audible_stream_starts_debounce() {
        let start = Instant::now();
        let mut tracker = started(start);
        for i in 1..=4 {
            let now = start + STEP * i;
            assert_eq!(tracker.observe_levels(now, &[music(1)], |_| SILENT), None);
        }
        let audible = start + STEP * 5;
        let mut now = audible;
        while now < audible + MIN {
            assert_eq!(tracker.observe_levels(now, &[music(1)], |_| LOUD), None);
            now += STEP;
        }
        let almost = audible + MIN - Duration::from_millis(1);
        assert_eq!(tracker.observe_levels(almost, &[music(1)], |_| LOUD), None);
        assert_eq!(
            tracker.observe_levels(audible + MIN, &[music(1)], |_| LOUD),
            Some(true)
        );
    }

    #[test]
    fn silent_sample_restarts_debounce() {
        let start = Instant::now();
        let mut tracker = started(start);
        let first = start + STEP;
        assert_eq!(tracker.observe_levels(first, &[music(1)], |_| LOUD), None);
        let next = first + STEP;
        assert_eq!(tracker.observe_levels(next, &[music(1)], |_| LOUD), None);
        let dip = next + STEP;
        assert_eq!(tracker.observe_levels(dip, &[music(1)], |_| SILENT), None);
        // Audible again: the debounce starts over from here, so the first
        // run's MIN passes without a count.
        let second = dip + STEP;
        let mut now = second;
        while now < second + MIN {
            assert_eq!(tracker.observe_levels(now, &[music(1)], |_| LOUD), None);
            now += STEP;
        }
        assert!(first + MIN < second + MIN);
        assert_eq!(
            tracker.observe_levels(second + MIN, &[music(1)], |_| LOUD),
            Some(true)
        );
    }

    #[test]
    fn silence_stops_counting_after_hold() {
        let start = Instant::now();
        let mut tracker = counting(start);
        let last = start + MIN;
        let mut now = last + STEP;
        while now < last + SILENCE_HOLD {
            assert_eq!(tracker.observe_levels(now, &[music(1)], |_| SILENT), None);
            now += STEP;
        }
        let almost = last + SILENCE_HOLD - Duration::from_millis(1);
        assert_eq!(
            tracker.observe_levels(almost, &[music(1)], |_| SILENT),
            None
        );
        assert_eq!(
            tracker.observe_levels(last + SILENCE_HOLD, &[music(1)], |_| SILENT),
            Some(false)
        );
        assert!(!tracker.playing);
    }

    #[test]
    fn short_dip_keeps_counting() {
        let start = Instant::now();
        let mut tracker = counting(start);
        let last = start + MIN;
        let mut now = last + STEP;
        while now < last + SILENCE_HOLD {
            assert_eq!(tracker.observe_levels(now, &[music(1)], |_| SILENT), None);
            now += STEP;
        }
        // Sound resumes just short of the hold: no new debounce, no stop.
        let resumed = last + SILENCE_HOLD - Duration::from_millis(1);
        let mut now = resumed;
        while now < resumed + MIN * 2 {
            assert_eq!(tracker.observe_levels(now, &[music(1)], |_| LOUD), None);
            now += STEP;
        }
        assert!(tracker.playing);
    }

    #[test]
    fn resumed_after_hold_needs_new_debounce() {
        let start = Instant::now();
        let mut tracker = counting(start);
        let stopped = start + MIN + SILENCE_HOLD;
        assert_eq!(
            tracker.observe_levels(stopped, &[music(1)], |_| SILENT),
            Some(false)
        );
        let resumed = stopped + STEP;
        let mut now = resumed;
        while now < resumed + MIN {
            assert_eq!(tracker.observe_levels(now, &[music(1)], |_| LOUD), None);
            now += STEP;
        }
        assert_eq!(
            tracker.observe_levels(resumed + MIN, &[music(1)], |_| LOUD),
            Some(true)
        );
    }

    #[test]
    fn peak_gate_constants_are_bounded() {
        assert!(AUDIBLE_PEAK > 0.0);
        assert!(AUDIBLE_PEAK <= 0.01);
        assert!(SILENCE_HOLD >= Duration::from_secs(3));
        assert!(SILENCE_HOLD <= Duration::from_secs(10));
    }

    #[test]
    fn baseline_audible_stream_never_counts() {
        let start = Instant::now();
        let mut source = FakePlayback::new(vec![Ok(vec![music(1)])]);
        let script = source.script.clone();
        let mut peaks = RecordingPeaks::new(Some(LOUD));
        let mut tracker = PlaybackTracker::new(MIN);
        for i in 0..20 {
            let now = start + STEP * i;
            assert_eq!(tracker.poll_levels(&mut source, &mut peaks, || now), None);
        }
        assert!(peaks.asked.iter().all(Vec::is_empty), "{:?}", peaks.asked);
        script.lock().unwrap().push_back(Ok(vec![corked_music(1)]));
        let corked = start + STEP * 20;
        assert_eq!(
            tracker.poll_levels(&mut source, &mut peaks, || corked),
            None
        );
        assert_eq!(peaks.asked.last(), Some(&vec![]));
        // Uncorked: a candidate now, counted after an audible MIN.
        script.lock().unwrap().push_back(Ok(vec![music(1)]));
        let resumed = corked + STEP;
        assert_eq!(
            tracker.poll_levels(&mut source, &mut peaks, || resumed),
            None
        );
        assert_eq!(peaks.asked.last(), Some(&vec![1]));
        let counted = resumed + MIN;
        assert_eq!(
            tracker.poll_levels(&mut source, &mut peaks, || counted),
            Some(true)
        );
    }

    #[test]
    fn peak_candidates_are_only_live_streams() {
        let others = vec![
            corked_music(1),
            muted_music(2),
            event_sound(3),
            sink_input(4, &[], false),
        ];
        let listing = |live: &[StreamProps]| Ok([others.clone(), live.to_vec()].concat());
        let mut source = FakePlayback::new(vec![
            Ok(vec![]),
            listing(&[music(5), music(6)]),
            listing(&[corked_music(5), music(6)]),
            listing(&[corked_music(5)]),
        ]);
        let mut peaks = RecordingPeaks::new(Some(LOUD));
        let mut tracker = PlaybackTracker::new(MIN);
        let start = Instant::now();
        for i in 0..4 {
            let now = start + STEP * i;
            tracker.poll_levels(&mut source, &mut peaks, || now);
        }
        assert_eq!(peaks.asked, [vec![], vec![5, 6], vec![6], vec![]]);
    }

    /// Fails if the clock was read before the levels were asked for.
    struct ClockAfterLevels(Arc<Mutex<bool>>);

    impl PeakSource for ClockAfterLevels {
        fn levels(&mut self, _: &HashSet<u32>) -> HashMap<u32, f32> {
            assert!(!*self.0.lock().unwrap(), "clock read before the levels");
            HashMap::new()
        }
    }

    #[test]
    fn poll_reads_clock_after_levels() {
        let read = Arc::new(Mutex::new(false));
        let mut peaks = ClockAfterLevels(read.clone());
        let mut source = FakePlayback::new(vec![Ok(vec![music(1)])]);
        let mut tracker = PlaybackTracker::new(MIN);
        let clock = || {
            *read.lock().unwrap() = true;
            Instant::now()
        };
        tracker.poll_levels(&mut source, &mut peaks, clock);
        assert!(*read.lock().unwrap());
    }

    #[test]
    fn pulse_peaks_thread_exits_when_dropped() {
        let mut peaks = PulsePeaks::new();
        let shared = peaks.peaks.clone();
        // No candidates: no record stream, and nothing to wait for.
        assert!(peaks.levels(&HashSet::new()).is_empty());
        drop(peaks);
        // The thread holds the other reference until it exits.
        let deadline = Instant::now() + Duration::from_secs(5);
        while Arc::strong_count(&shared) > 1 {
            assert!(Instant::now() < deadline, "peak thread still running");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn candidate_slot_keeps_only_newest() {
        let (tx, rx) = candidate_slot();
        for i in 0..1000 {
            tx.offer(HashSet::from([i]));
        }
        assert_eq!(rx.take(), Ok(HashSet::from([999])));
        assert_eq!(rx.try_take(), None);
    }

    #[test]
    fn candidate_offer_never_waits_on_a_stuck_consumer() {
        let (tx, _rx) = candidate_slot();
        let start = Instant::now();
        for i in 0..1000 {
            tx.offer(HashSet::from([i]));
        }
        assert!(start.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn candidate_take_timeout_times_out_with_nothing_offered() {
        let (_tx, rx) = candidate_slot();
        let start = Instant::now();
        assert_eq!(
            rx.take_timeout(Duration::from_millis(50)),
            Err(RecvTimeoutError::Timeout)
        );
        let waited = start.elapsed();
        assert!(waited >= Duration::from_millis(50), "{waited:?}");
        assert!(waited < Duration::from_secs(1), "{waited:?}");
    }

    #[test]
    fn candidate_slot_closes_when_producer_dropped() {
        let (tx, rx) = candidate_slot();
        let (done, result) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            done.send(rx.take()).unwrap();
            rx
        });
        std::thread::sleep(Duration::from_millis(50));
        drop(tx);
        assert_eq!(
            result.recv_timeout(Duration::from_secs(1)),
            Ok(Err(RecvTimeoutError::Disconnected))
        );
        let rx = waiter.join().unwrap();
        assert_eq!(rx.take(), Err(RecvTimeoutError::Disconnected));
        let start = Instant::now();
        assert_eq!(
            rx.take_timeout(Duration::from_secs(5)),
            Err(RecvTimeoutError::Disconnected)
        );
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn candidate_set_offered_before_drop_is_delivered_first() {
        let (tx, rx) = candidate_slot();
        tx.offer(HashSet::from([7]));
        drop(tx);
        assert_eq!(rx.take_timeout(PEAK_TICK), Ok(HashSet::from([7])));
        assert_eq!(rx.take(), Err(RecvTimeoutError::Disconnected));
    }

    /// Fake connections for `list_reusing`, numbered from 1 by each connect;
    /// a listing returns the connection it ran on.
    #[derive(Default)]
    struct FakeConnections {
        slot: Option<u32>,
        connects: u32,
        connect_fails: bool,
        not_ready: HashSet<u32>,
    }

    impl FakeConnections {
        /// One listing that ends with `outcome`.
        fn list(&mut self, outcome: io::Result<()>) -> io::Result<u32> {
            let FakeConnections {
                slot,
                connects,
                connect_fails,
                not_ready,
            } = self;
            list_reusing(
                slot,
                || {
                    *connects += 1;
                    match connect_fails {
                        true => Err(io::Error::other("no sound server")),
                        false => Ok(*connects),
                    }
                },
                |c| !not_ready.contains(c),
                |c| outcome.map(|()| *c),
            )
        }
    }

    #[test]
    fn playback_listing_reuses_connection() {
        let mut fake = FakeConnections::default();
        for _ in 0..5 {
            assert_eq!(fake.list(Ok(())).unwrap(), 1);
        }
        assert_eq!(fake.connects, 1);
    }

    #[test]
    fn playback_listing_reconnects_after_error() {
        let mut fake = FakeConnections::default();
        assert_eq!(fake.list(Ok(())).unwrap(), 1);
        assert!(fake.list(failure_streams().map(drop)).is_err());
        assert_eq!(fake.list(Ok(())).unwrap(), 2);
        assert_eq!(fake.list(Ok(())).unwrap(), 2);
        assert_eq!(
            fake.list(timed_out()).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(fake.list(Ok(())).unwrap(), 3);
        assert_eq!(fake.list(Ok(())).unwrap(), 3);
        assert_eq!(fake.connects, 3);
    }

    #[test]
    fn playback_listing_reconnects_when_not_ready() {
        let mut fake = FakeConnections::default();
        assert_eq!(fake.list(Ok(())).unwrap(), 1);
        fake.not_ready.insert(1);
        // Never listed on 1 again: replaced by one connect.
        assert_eq!(fake.list(Ok(())).unwrap(), 2);
        assert_eq!(fake.list(Ok(())).unwrap(), 2);
        assert_eq!(fake.connects, 2);
    }

    #[test]
    fn playback_listing_reconnects_after_failed_connect() {
        let mut fake = FakeConnections {
            connect_fails: true,
            ..Default::default()
        };
        assert!(fake.list(Ok(())).is_err());
        assert_eq!(fake.connects, 1);
        fake.connect_fails = false;
        assert_eq!(fake.list(Ok(())).unwrap(), 2);
        assert_eq!(fake.list(Ok(())).unwrap(), 2);
        assert_eq!(fake.connects, 2);
    }

    fn raw_sink_input(pid: Option<&str>, client: Option<u32>) -> pulse::SinkInput {
        pulse::SinkInput {
            index: 1,
            sink: 0,
            corked: false,
            mute: false,
            volume: ChannelVolumes::default(),
            props: pid
                .map(|p| (PROCESS_ID.to_string(), p.to_string()))
                .into_iter()
                .collect(),
            client,
        }
    }

    #[test]
    fn needs_clients_skips_own_pid_and_clientless_streams() {
        let inputs = [
            raw_sink_input(Some("1000"), Some(1)),
            raw_sink_input(Some("1001"), None),
            raw_sink_input(None, None),
        ];
        assert!(!needs_clients(&inputs));
    }

    #[test]
    fn needs_clients_for_pidless_stream_with_client() {
        let inputs = [
            raw_sink_input(Some("1000"), None),
            raw_sink_input(None, Some(1)),
        ];
        assert!(needs_clients(&inputs));
    }

    #[test]
    fn needs_clients_not_for_no_streams() {
        assert!(!needs_clients(&[]));
    }

    #[test]
    fn playback_thread_stall_is_bounded_and_late_reply_dropped() {
        let bound = Duration::from_millis(300);
        let (release, stalled) = std::sync::mpsc::channel::<()>();
        // Listing n has the one stream n; the first stalls until released.
        let mut source = PulsePlaybackStreams::spawn(bound, move || {
            let mut n = 0;
            move || {
                n += 1;
                if n == 1 {
                    let _ = stalled.recv();
                }
                Ok(vec![music(n)])
            }
        });
        let start = Instant::now();
        assert!(source.playback_streams().is_err());
        let waited = start.elapsed();
        assert!(waited >= bound, "{waited:?}");
        assert!(waited < bound + Duration::from_millis(500), "{waited:?}");
        release.send(()).unwrap();
        // The stalled listing answers nobody; each request gets its own.
        assert_eq!(source.playback_streams().unwrap(), vec![music(2)]);
        assert_eq!(source.playback_streams().unwrap(), vec![music(3)]);
    }

    #[test]
    fn playback_thread_requests_never_pile_up() {
        let bound = Duration::from_millis(100);
        let (_release, stalled) = std::sync::mpsc::channel::<()>();
        let mut source = PulsePlaybackStreams::spawn(bound, move || {
            move || {
                let _ = stalled.recv();
                Ok(Vec::new())
            }
        });
        // The first request stalls the thread, the second waits in the queue.
        assert!(source.playback_streams().is_err());
        assert!(source.playback_streams().is_err());
        // A full queue is an error at once.
        let start = Instant::now();
        assert!(source.playback_streams().is_err());
        assert!(start.elapsed() < bound, "{:?}", start.elapsed());
    }

    #[test]
    fn playback_thread_dead_lists_nothing_at_once_and_logs_once() {
        let mut source = PulsePlaybackStreams::spawn(Duration::from_secs(5), || {
            || -> io::Result<Vec<StreamProps>> { panic!("listing thread dies") }
        });
        assert!(!source.dead);
        for _ in 0..3 {
            let start = Instant::now();
            assert_eq!(source.playback_streams().unwrap(), Vec::new());
            assert!(start.elapsed() < Duration::from_secs(1));
            // Set by the first poll, so the error is logged only then.
            assert!(source.dead);
        }
    }

    #[tokio::test]
    async fn playback_thread_dead_drops_stream_playback() {
        let die = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let dying = die.clone();
        // An empty baseline, then music until `die`, then the thread panics.
        let source = PulsePlaybackStreams::spawn(Duration::from_secs(5), move || {
            let mut n = 0;
            move || {
                n += 1;
                if dying.load(std::sync::atomic::Ordering::SeqCst) {
                    panic!("listing thread dies");
                }
                Ok(if n == 1 { Vec::new() } else { vec![music(1)] })
            }
        });
        let mut rx = spawn_stream_playback(
            source,
            AllAudible,
            Duration::from_millis(50),
            Duration::from_millis(5),
            Default::default(),
            |_| (),
        );
        tokio::time::timeout(Duration::from_secs(3), rx.wait_for(|(s, _)| s.counted))
            .await
            .expect("stream playback")
            .expect("monitor running");
        die.store(true, std::sync::atomic::Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(3), rx.wait_for(|(s, _)| !s.counted))
            .await
            .expect("stream playback off once the thread died")
            .expect("monitor running");
    }

    #[tokio::test]
    async fn playback_thread_dead_makes_live_unknown() {
        let die = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let dying = die.clone();
        // No such process, so /proc names no parent.
        const PID: u32 = 99_999_999;
        // An empty baseline, then music until `die`, then the thread panics.
        let source = PulsePlaybackStreams::spawn(Duration::from_secs(5), move || {
            let mut n = 0;
            move || {
                n += 1;
                if dying.load(std::sync::atomic::Ordering::SeqCst) {
                    panic!("listing thread dies");
                }
                Ok(if n == 1 {
                    Vec::new()
                } else {
                    vec![with_pid(music(1), PID)]
                })
            }
        });
        let mut rx = spawn_stream_playback(
            source,
            AllAudible,
            Duration::from_millis(50),
            Duration::from_millis(5),
            Default::default(),
            |_| (),
        );
        tokio::time::timeout(
            Duration::from_secs(3),
            rx.wait_for(|(s, _)| s.counted && s.live == Some(HashSet::from([PID]))),
        )
        .await
        .expect("stream playback")
        .expect("monitor running");
        die.store(true, std::sync::atomic::Ordering::SeqCst);
        tokio::time::timeout(
            Duration::from_secs(3),
            rx.wait_for(|(s, _)| s.live.is_none() && !s.counted),
        )
        .await
        .expect("live unknown once the thread died")
        .expect("monitor running");
        // It stays so over later polls.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            rx.borrow().0,
            PlaybackState {
                counted: false,
                live: None
            }
        );
        // Unknown again: every Playing player counts, even one whose owner
        // lookup failed (#83, #329).
        assert!(rx.borrow().0.live_or_unknown(None));
    }

    #[test]
    fn playback_thread_exits_when_dropped() {
        let alive = Arc::new(());
        let held = alive.clone();
        let mut source = PulsePlaybackStreams::spawn(Duration::from_secs(5), move || {
            move || {
                let _ = &held;
                Ok(Vec::new())
            }
        });
        assert_eq!(source.playback_streams().unwrap(), Vec::new());
        drop(source);
        let deadline = Instant::now() + Duration::from_secs(5);
        while Arc::strong_count(&alive) > 1 {
            assert!(
                Instant::now() < deadline,
                "playback stream thread still running"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn capture_is_monitor_iff_its_source_is_a_monitor_source() {
        let capture = |index| StreamProps {
            direction: StreamDirection::Capture,
            props: BTreeMap::from([("media.role".to_string(), "phone".to_string())]),
            monitor: false,
            index,
            corked: false,
            muted: false,
        };
        // Output 1 records monitor source 5, output 2 microphone source 6.
        let streams = mark_monitors(vec![(5, capture(1)), (6, capture(2))], &[5, 9]);
        assert_eq!(
            streams.iter().map(|s| s.monitor).collect::<Vec<_>>(),
            [true, false]
        );
        assert!(!crate::call_detect::is_call_stream(&streams[0]));
        assert!(crate::call_detect::is_call_stream(&streams[1]));
    }
}
