//! Local-call and stream-playback monitoring (#11, #10): the I/O behind
//! `call_detect`.
//!
//! `StreamSource` copies the sound server's capture streams and cards into an
//! `AudioSnapshot`; `CallPoller` turns snapshots into call state changes with
//! the pure `call_detect` rules; `spawn_call_poller` samples once per
//! `CALL_POLL` and sends each change to the handoff task.
//!
//! `PlaybackSource` lists the playback streams (sink inputs);
//! `PlaybackTracker` debounces the takeover-worthy ones with an injected
//! clock, and `spawn_stream_playback` publishes its state for
//! `switching::spawn_playback_poller`, which combines it with MPRIS. Tests
//! use fake sources, so no sound server is needed.

use crate::call_detect::{
    StreamDirection, StreamProps, airpods_card_profile, call_active, is_takeover_worthy,
};
use libpulse_binding::callbacks::ListResult;
use libpulse_binding::context::{Context, FlagSet as ContextFlagSet, State as ContextState};
use libpulse_binding::mainloop::standard::{IterateResult, Mainloop};
use libpulse_binding::operation::{Operation, State as OperationState};
use libpulse_binding::proplist::Proplist;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::rc::Rc;
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
}

/// Debounces takeover-worthy playback streams into a stream-playing state.
///
/// A stream, keyed by its sound server index, counts once it has been seen
/// worthy (`call_detect::is_takeover_worthy`) and uncorked in every sample
/// for at least `min`. Corking, vanishing or reclassification forgets it, so
/// it starts over if it comes back. Stream playback is on while any stream
/// counts. An app that keeps a silent stream open uncorked still counts;
/// libpulse reports no level for sink inputs (#84).
///
/// The first successful listing is the baseline: streams already playing in
/// it never count, as MPRIS playback running at the handoff core's baseline
/// is no new start. Such a stream counts only once it has been corked (or
/// reclassified) and plays again, and then only after `min`; a new stream
/// counts as usual.
#[derive(Debug)]
pub(crate) struct PlaybackTracker {
    min: Duration,
    /// Worthy, uncorked streams by index: when each was first seen so.
    since: HashMap<u32, Instant>,
    /// Worthy, uncorked streams of the first successful listing that have
    /// stayed so ever since; None until that listing.
    preexisting: Option<HashSet<u32>>,
    /// Whether a stream counts as playing (false before any change).
    playing: bool,
}

impl PlaybackTracker {
    pub(crate) fn new(min: Duration) -> Self {
        PlaybackTracker {
            min,
            since: HashMap::new(),
            preexisting: None,
            playing: false,
        }
    }

    /// Take the playback streams present at `now`. Some(playing) only when
    /// the aggregate stream-playing state changed.
    pub(crate) fn observe(&mut self, now: Instant, streams: &[StreamProps]) -> Option<bool> {
        let live: HashSet<u32> = streams
            .iter()
            .filter(|s| !s.corked && is_takeover_worthy(s))
            .map(|s| s.index)
            .collect();
        let preexisting = self.preexisting.get_or_insert_with(|| live.clone());
        preexisting.retain(|index| live.contains(index));
        self.since.retain(|index, _| live.contains(index));
        for index in live {
            if !preexisting.contains(&index) {
                self.since.entry(index).or_insert(now);
            }
        }
        let playing = self
            .since
            .values()
            .any(|&since| now.saturating_duration_since(since) >= self.min);
        if playing == self.playing {
            return None;
        }
        self.playing = playing;
        Some(playing)
    }

    /// Sample `source` once, as `observe` at the time `clock` gives. `clock`
    /// is read only once the listing has returned, so a slow listing can't
    /// date a stream's start back to before the stream was seen. A source
    /// error changes nothing: the last state and the streams' timers are
    /// kept, and the baseline is still the first successful listing.
    pub(crate) fn poll(
        &mut self,
        source: &mut dyn PlaybackSource,
        clock: impl FnOnce() -> Instant,
    ) -> Option<bool> {
        match source.playback_streams() {
            Ok(streams) => self.observe(clock(), &streams),
            Err(e) => {
                log::debug!("Stream monitor: playback stream listing failed: {e}");
                None
            }
        }
    }
}

/// Sample `source` every `interval` with a `PlaybackTracker` and publish the
/// stream-playing state (initially false; streams already playing in the
/// first listing never make it true, see `PlaybackTracker`). Each listing is
/// timed when it returns. Sampling runs on the blocking pool in its own
/// task, so a sound server that never answers stalls only this state, never
/// the caller. The task stops once every receiver is dropped; if sampling
/// panics the state drops to false.
pub(crate) fn spawn_stream_playback<S: PlaybackSource + 'static>(
    source: S,
    min: Duration,
    interval: Duration,
) -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        let mut state = Some((PlaybackTracker::new(min), source));
        while !tx.is_closed() {
            let Some((mut tracker, mut source)) = state.take() else {
                break;
            };
            let sampled = tokio::task::spawn_blocking(move || {
                let change = tracker.poll(&mut source, Instant::now);
                (tracker, source, change)
            })
            .await;
            let Ok((tracker, source, change)) = sampled else {
                log::error!("Stream playback monitor stopped: sampling panicked");
                tx.send_replace(false);
                break;
            };
            state = Some((tracker, source));
            if let Some(playing) = change {
                tx.send_replace(playing);
            }
            tokio::time::sleep(interval).await;
        }
    });
    rx
}

/// A sink input as `call_detect` sees it.
fn playback_stream(index: u32, proplist: &Proplist, corked: bool) -> StreamProps {
    StreamProps {
        direction: StreamDirection::Playback,
        props: props(proplist),
        monitor: false,
        index,
        corked,
    }
}

/// The production source: a short-lived libpulse connection per snapshot
/// (PipeWire's pulse server included), as media_controller.rs makes one per
/// query. For calls it lists the source outputs (capture streams) with their
/// property lists, marking those recording a monitor source, and the cards
/// with their active profile; for playback it lists the sink inputs.
pub(crate) struct PulseStreams;

impl PlaybackSource for PulseStreams {
    fn playback_streams(&mut self) -> io::Result<Vec<StreamProps>> {
        let (mut mainloop, context) = connect()?;
        let introspector = context.introspect();
        let inputs: Rc<RefCell<ListAccumulator<StreamProps>>> = Rc::default();
        let op = introspector.get_sink_input_info_list({
            let inputs = inputs.clone();
            move |result| {
                inputs.borrow_mut().record(result, |input| {
                    Some(playback_stream(input.index, &input.proplist, input.corked))
                })
            }
        });
        wait(&mut mainloop, &op, "sink input list")?;
        inputs.take().finish("sink input list")
    }
}

impl StreamSource for PulseStreams {
    fn snapshot(&mut self) -> io::Result<AudioSnapshot> {
        let (mut mainloop, context) = connect()?;
        let introspector = context.introspect();

        let monitors: Rc<RefCell<ListAccumulator<u32>>> = Rc::default();
        let op = introspector.get_source_info_list({
            let monitors = monitors.clone();
            move |result| {
                monitors.borrow_mut().record(result, |source| {
                    source.monitor_of_sink.is_some().then_some(source.index)
                })
            }
        });
        wait(&mut mainloop, &op, "source list")?;
        let monitors: HashSet<u32> = monitors.take().finish("source list")?.into_iter().collect();

        let outputs: Rc<RefCell<ListAccumulator<(u32, StreamProps)>>> = Rc::default();
        let op = introspector.get_source_output_info_list({
            let outputs = outputs.clone();
            move |result| {
                outputs.borrow_mut().record(result, |output| {
                    let stream = StreamProps {
                        direction: StreamDirection::Capture,
                        props: props(&output.proplist),
                        monitor: false,
                        index: output.index,
                        corked: output.corked,
                    };
                    Some((output.source, stream))
                })
            }
        });
        wait(&mut mainloop, &op, "source output list")?;
        let outputs = outputs.take().finish("source output list")?;

        let cards: Rc<RefCell<ListAccumulator<(String, Option<String>)>>> = Rc::default();
        let op = introspector.get_card_info_list({
            let cards = cards.clone();
            move |result| {
                cards.borrow_mut().record(result, |card| {
                    let name = card.name.as_ref()?.to_string();
                    let profile = card
                        .active_profile
                        .as_ref()
                        .and_then(|p| p.name.as_ref().map(|n| n.to_string()));
                    Some((name, profile))
                })
            }
        });
        wait(&mut mainloop, &op, "card list")?;
        let cards = cards.take().finish("card list")?;

        let streams = outputs
            .into_iter()
            .map(|(source, stream)| StreamProps {
                monitor: monitors.contains(&source),
                ..stream
            })
            .collect();
        Ok(AudioSnapshot { streams, cards })
    }
}

/// The results of one libpulse list query, shared by every list callback so
/// that a failed or truncated listing is an error rather than a short list.
/// libpulse delivers each entry as `Item`, then exactly one `End` on success
/// or one `Error` on failure; an operation can also finish (or be cancelled)
/// without either.
#[derive(Debug)]
struct ListAccumulator<T> {
    items: Vec<T>,
    failed: bool,
    ended: bool,
}

impl<T> Default for ListAccumulator<T> {
    fn default() -> Self {
        ListAccumulator {
            items: Vec::new(),
            failed: false,
            ended: false,
        }
    }
}

impl<T> ListAccumulator<T> {
    /// Record one callback result. `keep` maps an item to the value to keep,
    /// or None to skip that item (it is not an error).
    fn record<I>(&mut self, result: ListResult<I>, keep: impl FnOnce(I) -> Option<T>) {
        match result {
            ListResult::Item(item) => {
                if let Some(value) = keep(item) {
                    self.items.push(value);
                }
            }
            ListResult::End => self.ended = true,
            ListResult::Error => self.failed = true,
        }
    }

    /// The kept items once the operation is over: Err if the server reported
    /// an error or the list never reached its end.
    fn finish(self, what: &str) -> io::Result<Vec<T>> {
        if self.failed {
            return Err(io::Error::other(format!(
                "PulseAudio {what} reported an error"
            )));
        }
        if !self.ended {
            return Err(io::Error::other(format!("PulseAudio {what} ended early")));
        }
        Ok(self.items)
    }
}

fn props(list: &Proplist) -> BTreeMap<String, String> {
    list.iter()
        .filter_map(|key| list.get_str(&key).map(|value| (key, value)))
        .collect()
}

fn pulse_error(what: &str) -> io::Error {
    io::Error::other(format!("PulseAudio {what} failed"))
}

/// Connect to the sound server, blocking until the context is ready.
// shortcut: duplicates media_controller.rs's pulse_connect; fold the two together (#73)
fn connect() -> io::Result<(Mainloop, Context)> {
    let mut mainloop = Mainloop::new().ok_or_else(|| pulse_error("mainloop"))?;
    let mut context =
        Context::new(&mainloop, "LibrePods call monitor").ok_or_else(|| pulse_error("context"))?;
    context
        .connect(None, ContextFlagSet::NOAUTOSPAWN, None)
        .map_err(|_| pulse_error("connection"))?;
    loop {
        if !matches!(mainloop.iterate(true), IterateResult::Success(_)) {
            return Err(pulse_error("mainloop iteration"));
        }
        match context.get_state() {
            ContextState::Ready => return Ok((mainloop, context)),
            ContextState::Failed | ContextState::Terminated => {
                return Err(pulse_error("connection"));
            }
            _ => {}
        }
    }
}

/// Run the mainloop until the operation is done.
// shortcut: no timeout; a sound server that never answers stalls the call and stream-playback monitors (not the handoff task or MPRIS samples) until restart (#73)
fn wait<F: ?Sized>(mainloop: &mut Mainloop, op: &Operation<F>, what: &str) -> io::Result<()> {
    loop {
        match op.get_state() {
            OperationState::Running => {}
            OperationState::Done => return Ok(()),
            OperationState::Cancelled => return Err(pulse_error(what)),
        }
        if !matches!(mainloop.iterate(true), IterateResult::Success(_)) {
            return Err(pulse_error(what));
        }
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
            match self.script.lock().unwrap().pop_front() {
                Some(Ok(snapshot)) => {
                    self.last = snapshot.clone();
                    Ok(snapshot)
                }
                Some(Err(e)) => Err(e),
                None => Ok(self.last.clone()),
            }
        }
    }

    fn call_stream() -> StreamProps {
        StreamProps {
            direction: StreamDirection::Capture,
            props: [("media.role".to_string(), "Communication".to_string())].into(),
            monitor: false,
            index: 0,
            corked: false,
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
    fn list_accumulator_items_then_end_is_ok() {
        let mut list = ListAccumulator::default();
        list.record(ListResult::Item(1), Some);
        list.record(ListResult::Item(2), Some);
        list.record(ListResult::End, Some);
        assert_eq!(list.finish("test list").unwrap(), vec![1, 2]);
    }

    #[test]
    fn list_accumulator_skips_unkept_items() {
        let mut list = ListAccumulator::default();
        list.record(ListResult::Item(1), |n: i32| (n > 1).then_some(n));
        list.record(ListResult::Item(2), |n: i32| (n > 1).then_some(n));
        list.record(ListResult::End, |n: i32| Some(n));
        assert_eq!(list.finish("test list").unwrap(), vec![2]);
    }

    #[test]
    fn list_accumulator_error_is_err() {
        let mut list = ListAccumulator::default();
        list.record(ListResult::Item(1), Some);
        list.record(ListResult::Error, Some);
        list.record(ListResult::End, Some);
        assert!(list.finish("test list").is_err());
    }

    #[test]
    fn list_accumulator_without_end_is_err() {
        let mut list = ListAccumulator::default();
        list.record(ListResult::Item(1), Some);
        assert!(list.finish("test list").is_err());
        assert!(
            ListAccumulator::<i32>::default()
                .finish("test list")
                .is_err()
        );
    }

    #[test]
    fn poller_holds_call_through_failed_snapshot() {
        // A card listing that never reached End, as PulseStreams sees it.
        let mut truncated = ListAccumulator::default();
        truncated.record(ListResult::Item(()), Some);
        let failed = truncated
            .finish("card list")
            .map(|_| AudioSnapshot::default());
        assert!(failed.is_err());
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
            match self.script.lock().unwrap().pop_front() {
                Some(Ok(streams)) => {
                    self.last = streams.clone();
                    Ok(streams)
                }
                Some(Err(e)) => Err(e),
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

    fn event_sound(index: u32) -> StreamProps {
        sink_input(
            index,
            &[("media.role", "event"), ("application.name", "GNOME Shell")],
            false,
        )
    }

    const MIN: Duration = MIN_STREAM_PLAYBACK;
    const STEP: Duration = Duration::from_millis(500);

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

    #[test]
    fn truncated_sink_input_list_keeps_tracker_state() {
        // Sink inputs built as PulseStreams builds them, from a proplist.
        let mut proplist = Proplist::new().expect("proplist");
        proplist.set_str("media.role", "Music").unwrap();
        proplist.set_str("application.name", "mpv").unwrap();
        let mut complete = ListAccumulator::default();
        complete.record(ListResult::Item(&proplist), |p: &Proplist| {
            Some(playback_stream(4, p, false))
        });
        complete.record(ListResult::End, |p: &Proplist| {
            Some(playback_stream(4, p, false))
        });
        let complete = complete.finish("sink input list");
        let streams = complete.as_ref().expect("a complete list").clone();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].direction, StreamDirection::Playback);
        assert_eq!(streams[0].index, 4);
        assert!(!streams[0].corked);
        assert_eq!(
            streams[0].props.get("media.role").map(String::as_str),
            Some("Music")
        );
        // A listing with an item and no End: an Err snapshot, not one
        // stream (or none).
        let mut truncated = ListAccumulator::default();
        truncated.record(ListResult::Item(&proplist), |p: &Proplist| {
            Some(playback_stream(4, p, false))
        });
        let truncated = truncated.finish("sink input list");
        assert!(truncated.is_err());
        // An empty listing cut short is an error too.
        let mut empty_cut = ListAccumulator::<StreamProps>::default();
        empty_cut.record(ListResult::Error, |p: &Proplist| {
            Some(playback_stream(4, p, false))
        });
        let empty_cut = empty_cut.finish("sink input list");
        assert!(empty_cut.is_err());

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
        let mut rx =
            spawn_stream_playback(source, Duration::from_millis(50), Duration::from_millis(5));
        assert!(!*rx.borrow());
        tokio::time::timeout(Duration::from_secs(3), rx.wait_for(|playing| *playing))
            .await
            .expect("stream playback")
            .expect("monitor running");
        script.lock().unwrap().push_back(Ok(vec![]));
        tokio::time::timeout(Duration::from_secs(3), rx.wait_for(|playing| !*playing))
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
        let mut rx = spawn_stream_playback(source, min, Duration::from_millis(5));
        tokio::time::timeout(Duration::from_secs(5), rx.wait_for(|playing| *playing))
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
}
