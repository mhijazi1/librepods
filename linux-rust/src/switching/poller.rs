//! Playback samples for the handoff core and the media actions it drives.

use super::PLAYBACK_POLL;
use crate::a2dp_resume;
use crate::default_sink::{DefaultSinkRouter, SinkEvent};
use crate::handoff::Command;
use crate::media_controller::{
    self, AUDIO_CATEGORY_IDLE, AUDIO_CATEGORY_MEDIA, AUDIO_CATEGORY_STREAM, MediaController,
    blocking,
};
use crate::stream_monitor::{
    self, MIN_STREAM_PLAYBACK, PeakSource, PlaybackSource, PlaybackState, PulsePeaks,
    PulsePlaybackStreams, Starts,
};
use futures::future::BoxFuture;
use log::error;
use std::collections::HashSet;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc, oneshot, watch};

/// Playback samples: Linux's audio category at the instant (#272), from
/// `local_category`, and a new source's category (`Input::Playback`, #96).
pub(crate) struct PlaybackSamples {
    pub(super) rx: mpsc::Receiver<(Instant, u32, Option<u32>)>,
    /// Asks the stream monitor to re-baseline at the core's reset (#90).
    pub(super) rebaseline: Arc<StdMutex<Option<Instant>>>,
    /// The stream monitor's state, read at `Command::PauseMedia` (#302) and
    /// `Command::ResumeA2dp` (#330), with its counting streams' starts (#96).
    pub(super) streams: watch::Receiver<(PlaybackState, Starts)>,
    /// The latest completed MPRIS sample's Playing players with their
    /// resolved owners, read at `Command::ResumeA2dp` (#351).
    pub(super) players: Arc<StdMutex<Vec<(String, Option<u32>)>>>,
}

impl PlaybackSamples {
    pub(crate) async fn recv(&mut self) -> Option<(Instant, u32)> {
        let (sampled_at, category, _) = self.sample().await?;
        Some((sampled_at, category))
    }

    /// `recv` with the category of a source new since the previous sample.
    pub(crate) async fn sample(&mut self) -> Option<(Instant, u32, Option<u32>)> {
        self.rx.recv().await
    }

    /// The core's step at `reset` asked for `Command::DrainPlayback`:
    /// re-baseline the stream monitor at `reset` before it observes its next
    /// listing. Anchored to `reset`, not to now, so a stream first audible
    /// after the reset still counts however late this is applied.
    #[cfg(test)]
    pub(crate) fn rebaseline(&self, reset: Instant) {
        *self.rebaseline.lock().unwrap_or_else(|e| e.into_inner()) = Some(reset);
    }

    /// Run the core's step at now, `core_step(now)`, under the re-baseline
    /// lock, and when it asks for `DrainPlayback` request the stream
    /// monitor's re-baseline at that now before the lock is released, so
    /// before any command is awaited (#90). The stream worker holds
    /// the same lock from its clock read to its publish, so each of its
    /// observes falls wholly before now, published before it (a poller
    /// sample stamped at or after now reads it), or wholly after the
    /// request.
    pub(crate) fn handle(
        &self,
        core_step: impl FnOnce(Instant) -> Vec<Command>,
    ) -> (Instant, Vec<Command>) {
        let mut request = self.rebaseline.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let commands = core_step(now);
        if commands.contains(&Command::DrainPlayback) {
            *request = Some(now);
        }
        (now, commands)
    }

    /// `Command::DrainPlayback`: drop the samples already taken.
    pub(crate) fn drain(&mut self) {
        while self.rx.try_recv().is_ok() {}
    }

    /// The stream monitor's state now, for `Command::PauseMedia` (#302) and
    /// `Command::ResumeA2dp` (#330).
    pub(crate) fn streams(&self) -> PlaybackState {
        self.streams.borrow().0.clone()
    }

    /// The poller's latest MPRIS sample, for `Command::ResumeA2dp` (#351).
    pub(super) fn players(&self) -> Vec<(String, Option<u32>)> {
        self.players
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

/// The media side effects the handoff core can ask for, injected so tests
/// need no D-Bus or PipeWire.
pub(crate) trait MediaActions: Send + Sync {
    /// Pause the local players `streams` says play locally
    /// (`PlaybackState::live_or_unknown`); whether one matched or the
    /// session bus was unavailable.
    fn pause(&self, streams: PlaybackState) -> BoxFuture<'static, bool>;
    /// Activate the AirPods' A2DP profile.
    fn activate_a2dp(&self) -> BoxFuture<'static, ()>;
    /// Run the A2DP resume chain (#200) until it ends, or until `cancel`
    /// fires or its sender is dropped (checked between steps). It starts no
    /// kick or profile cycle whose Play can't be due before `until`. It
    /// first waits for the default sink routes enqueued before it; a cancel
    /// or `until` during that wait ends it with no action. Its players are
    /// those Playing at the take, read then or in `players` (the poller's
    /// latest MPRIS sample, #351), that `streams`, the stream monitor's state
    /// then, says play locally (#361).
    fn resume_a2dp(
        &self,
        streams: PlaybackState,
        players: Vec<(String, Option<u32>)>,
        until: Instant,
        cancel: oneshot::Receiver<()>,
    ) -> BoxFuture<'static, ()>;
    /// Enqueue a default sink route (#40); never waits for the sound server.
    fn route_default_sink(&self, event: SinkEvent);
    /// Mark the A2DP hold (#264) at once, then enqueue its card write with
    /// the routes; never waits for the sound server.
    fn hold_a2dp(&self, hold: bool);
    /// Resolves once the hold writes enqueued before this call are applied
    /// (#294); ready by default.
    fn hold_settled(&self) -> BoxFuture<'static, ()> {
        Box::pin(std::future::ready(()))
    }
    /// Ear detection's resume reports (#313); none by default.
    fn ear_resumes(&self) -> mpsc::UnboundedReceiver<bool> {
        mpsc::unbounded_channel().1
    }
}

/// Blocking: pauses the local players whose resolved owner
/// (`media_controller::player_owner`) the filter accepts and reports what
/// it did.
pub(crate) type PausePlayers =
    Arc<dyn Fn(&dyn Fn(Option<u32>) -> bool) -> media_controller::PauseOutcome + Send + Sync>;

/// Logged when `pause_own_players` matched nothing.
pub(crate) const NOT_PAUSING: &str = "no MPRIS player with a live local stream; not pausing";

/// Pause the Playing players whose resolved owner `streams` says plays
/// locally (`PlaybackState::live_or_unknown`, #302, #329); whether one
/// matched or the session bus was unavailable. The app's yield and the
/// probe both pause through this.
pub(crate) async fn pause_own_players(
    controller: &MediaController,
    pause_players: PausePlayers,
    streams: PlaybackState,
) -> bool {
    // No live local stream at all: nothing here to pause (#355).
    if streams.live.as_ref().is_some_and(|pids| pids.is_empty()) {
        return false;
    }
    let outcome = controller
        .pause_all_media_with(move || pause_players(&|owner| streams.live_or_unknown(owner)))
        .await;
    if outcome.bus_unavailable {
        error!("Handoff: session bus unavailable; could not pause local players");
        return true;
    }
    outcome.paused + outcome.failed > 0
}

/// Production media actions on a clone of the device's `MediaController`
/// (its state is shared, DECISIONS #5). The device handlers lock the outer
/// `Arc<Mutex<MediaController>>` and ear detection holds it across an A2DP
/// activation (up to ~43 s, #32), so this adapter never takes that mutex: it
/// is built from the controller before the controller is put behind it
/// (`device_media`), and has no field that could reach it.
pub(crate) struct ControllerMedia {
    controller: MediaController,
    pause_players: PausePlayers,
    /// Its worker holds another clone of `controller`.
    sinks: DefaultSinkRouter,
}

impl MediaActions for ControllerMedia {
    fn pause(&self, streams: PlaybackState) -> BoxFuture<'static, bool> {
        let controller = self.controller.clone();
        let pause_players = self.pause_players.clone();
        Box::pin(async move { pause_own_players(&controller, pause_players, streams).await })
    }

    fn activate_a2dp(&self) -> BoxFuture<'static, ()> {
        let controller = self.controller.clone();
        Box::pin(async move {
            controller.activate_a2dp_profile().await;
        })
    }

    fn resume_a2dp(
        &self,
        streams: PlaybackState,
        players: Vec<(String, Option<u32>)>,
        until: Instant,
        cancel: oneshot::Receiver<()>,
    ) -> BoxFuture<'static, ()> {
        // Taken now, so it covers the RouteToAirPods enqueued just before.
        let routed = self.sinks.settled();
        Box::pin(a2dp_resume::run(
            self.controller.clone(),
            streams,
            players,
            until,
            cancel,
            routed,
        ))
    }

    fn route_default_sink(&self, event: SinkEvent) {
        self.sinks.route(event);
    }

    fn hold_a2dp(&self, hold: bool) {
        self.controller.set_a2dp_held(hold);
        self.sinks.route(SinkEvent::Hold(hold));
    }

    fn hold_settled(&self) -> BoxFuture<'static, ()> {
        Box::pin(self.sinks.settled())
    }

    fn ear_resumes(&self) -> mpsc::UnboundedReceiver<bool> {
        self.controller.subscribe_ear_resumes()
    }
}

/// A device's `MediaController`, behind the outer mutex its handlers lock,
/// and the handoff media adapter, used by `AirPodsDevice::new`.
pub(crate) fn device_media(
    airpods_mac: String,
    local_mac: String,
) -> (Arc<Mutex<MediaController>>, Arc<dyn MediaActions>) {
    share_media(
        MediaController::new(airpods_mac, local_mac),
        Arc::new(MediaController::pause_playing_mpris_players),
    )
}

/// The adapter takes its clone before the controller goes behind the outer
/// mutex, so no handler can hold that mutex at that point.
pub(super) fn share_media(
    controller: MediaController,
    pause_players: PausePlayers,
) -> (Arc<Mutex<MediaController>>, Arc<dyn MediaActions>) {
    let media = ControllerMedia {
        controller: controller.clone(),
        pause_players,
        sinks: DefaultSinkRouter::spawn(controller.clone()),
    };
    (Arc::new(Mutex::new(controller)), Arc::new(media))
}

/// Poll local playback every `PLAYBACK_POLL`: playing when an MPRIS player
/// plays with a live local stream of its own (#302, #329) or a
/// takeover-worthy audio stream has carried sound for `MIN_STREAM_PLAYBACK`
/// (#10, #88). The poller stops once the receiver is dropped.
pub(crate) fn spawn_playback_poller() -> PlaybackSamples {
    spawn_playback_poller_with(
        MediaController::playing_mpris_owners,
        PulsePlaybackStreams::new(),
        PulsePeaks::new(),
        MIN_STREAM_PLAYBACK,
        PLAYBACK_POLL,
        |_| (),
    )
}

/// `spawn_playback_poller` with its sources, minimum stream duration and
/// interval injected, and the stream monitor's `observed` hook. Every
/// `interval` it samples `mpris` on the blocking pool and sends
/// `(sampled_at, local_category(mpris, live, counted))` over the latest
/// state of the stream monitor over `streams` and `peaks`. The stream monitor
/// samples in its own task (`stream_monitor::spawn_stream_playback`), so a
/// sound server that never answers leaves the stream state as it was but
/// never holds up the MPRIS samples. Only edges reach the handoff core
/// (`Input::Playback`), which keeps every takeover decision;
/// `PlaybackSamples::handle` re-baselines the stream monitor when the core
/// resets its baseline (#90). `mpris` gives the Playing players with their
/// resolved owners (`media_controller::player_owner`), kept as the latest
/// sample for `PlaybackSamples::players` (#351), and `live` is whether one
/// of them owns a live stream (#329).
pub(super) fn spawn_playback_poller_with<M, S, P>(
    mpris: M,
    streams: S,
    peaks: P,
    min_stream: Duration,
    interval: Duration,
    observed: impl Fn(Instant) + Send + Sync + 'static,
) -> PlaybackSamples
where
    M: Fn() -> Vec<(String, Option<u32>)> + Send + Sync + 'static,
    S: PlaybackSource + 'static,
    P: PeakSource + 'static,
{
    let (play_tx, rx) = mpsc::channel(1);
    let rebaseline = Arc::new(StdMutex::new(None));
    let streams = stream_monitor::spawn_stream_playback(
        streams,
        peaks,
        min_stream,
        interval,
        rebaseline.clone(),
        observed,
    );
    let mpris = Arc::new(mpris);
    let samples = streams.clone();
    let players = Arc::new(StdMutex::new(Vec::new()));
    let latest = players.clone();
    tokio::spawn(async move {
        let mut last = None;
        loop {
            // Stamped before the stream state is read: a sample
            // stamped at or after a step's reset reads every state the
            // stream monitor published before that reset.
            let sampled_at = Instant::now();
            let mpris = mpris.clone();
            let playing = blocking(move || mpris()).await;
            let owners: Vec<_> = playing.iter().map(|&(_, owner)| owner).collect();
            *latest.lock().unwrap_or_else(|e| e.into_inner()) = playing;
            let (category, new_source) = sample_sources(&owners, &samples.borrow(), &mut last);
            if play_tx
                .send((sampled_at, category, new_source))
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(interval).await;
        }
    });
    PlaybackSamples {
        rx,
        rebaseline,
        streams,
        players,
    }
}

/// The players with a live local stream, the newest stream start, whether
/// the live set was known (`PlaybackState::live`) and the Playing players
/// one poller sample saw (#96).
pub(super) type Sources = (HashSet<Option<u32>>, u64, bool, HashSet<Option<u32>>);

/// One poller sample over the Playing players' resolved `owners` and the stream
/// monitor's state and stream starts: Linux's `local_category` and, against
/// the previous sample's sources in `last` (none before the first sample),
/// the category of a source new since then (#96): a player with a live local
/// stream whose owner had none (301), else a stream that started counting
/// (201) and is no such player's own stream (#329: its process or a recorded
/// ancestor is the player's owner; a stream with no resolved PID is new).
/// A live set gone unknown since then (a dead monitor, #318) makes every
/// player live: that sample reports only players not Playing in the previous
/// sample (#426, #429).
pub(super) fn sample_sources(
    owners: &[Option<u32>],
    (state, started): &(PlaybackState, Starts),
    last: &mut Option<Sources>,
) -> (u32, Option<u32>) {
    let live: HashSet<_> = owners
        .iter()
        .copied()
        .filter(|&owner| state.live_or_unknown(owner))
        .collect();
    let category = local_category(!owners.is_empty(), !live.is_empty(), state.counted);
    let known = state.live.is_some();
    let seen = last.as_ref().map_or(0, |&(_, seen, _, _)| seen);
    let own = |pids: &HashSet<u32>| live.iter().flatten().any(|pid| pids.contains(pid));
    let new_source = last.as_ref().and_then(|(was_live, _, was_known, were)| {
        // Known -> unknown: `live` is every owner now, so compare owners.
        let was = if *was_known && !known { were } else { was_live };
        if !live.is_subset(was) {
            Some(AUDIO_CATEGORY_MEDIA)
        } else {
            started
                .iter()
                .any(|(start, pids)| *start > seen && !own(pids))
                .then_some(AUDIO_CATEGORY_STREAM)
        }
    });
    let newest = started.last().map_or(seen, |&(start, _)| start.max(seen));
    *last = Some((live, newest, known, owners.iter().copied().collect()));
    (category, new_source)
}

/// Linux's audio category (#272): an MPRIS player playing with a live local
/// stream of its own is media (301), else a counted stream is 201, else idle
/// (100). An MPRIS player with no live stream of its own (a remote control
/// such as Spotify Connect) plays elsewhere (#302, #329).
pub(super) fn local_category(mpris_playing: bool, live: bool, stream_playing: bool) -> u32 {
    if mpris_playing && live {
        AUDIO_CATEGORY_MEDIA
    } else if stream_playing {
        AUDIO_CATEGORY_STREAM
    } else {
        AUDIO_CATEGORY_IDLE
    }
}
