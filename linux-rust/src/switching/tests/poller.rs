use super::*;

/// #313: the production adapter delivers the controller's ear resume
/// reports.
#[tokio::test]
async fn production_media_delivers_ear_resumes() {
    let (outer, media) = share_media(
        MediaController::new(String::new(), LOCAL.to_string()),
        Arc::new(|_: &dyn Fn(Option<u32>) -> bool| media_controller::PauseOutcome::default()),
    );
    let mut ear_rx = media.ear_resumes();
    use EarDetectionStatus::{InEar, OutOfEar};
    outer
        .lock()
        .await
        .handle_ear_detection(vec![InEar, OutOfEar], vec![InEar, InEar])
        .await;
    assert_eq!(ear_rx.try_recv(), Ok(false));
}

/// The production media adapter (`device_media`) never waits for the
/// outer MediaController mutex that ear detection holds across an A2DP
/// activation: a remote request is still answered at once, and the task
/// keeps taking events and ticks (session loss) while it is held.
#[tokio::test]
async fn handoff_production_media_ignores_held_controller_lock() {
    let mac = "5A:00:00:00:00:0B";
    let (manager, mut rx, _tx) = no_source_session().await;
    let records = Arc::new(Records::default());
    let counted = records.clone();
    let paused_for = mac.to_string();
    // Built as `device_media` builds it. Only the blocking D-Bus pause is
    // replaced (no session bus here, and a test must not pause the
    // developer's players); an empty AirPods MAC makes the real A2DP
    // activation return before touching PipeWire.
    let (outer, media) = share_media(
        MediaController::new(String::new(), LOCAL.to_string()),
        Arc::new(move |_: &dyn Fn(Option<u32>) -> bool| {
            counted.pauses.fetch_add(1, Ordering::SeqCst);
            trace(&paused_for, "PauseMedia".to_string());
            media_controller::PauseOutcome::default()
        }),
    );
    let handler = DeviceHandler {
        hold: Some(outer.clone()),
        ..Default::default()
    };
    handler.block_ear.store(true, Ordering::SeqCst);
    let h = Harness::start_wired(
        SwitchingMode::Handoff,
        mac,
        &manager,
        (outer.clone(), media),
        records,
        Arc::new(handler),
    )
    .await;
    let task = handoff_task(mac).expect("handoff task");
    assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
    assert_eq!(
        next_packet(&mut rx).await,
        expected_new_device(REMOTE).await
    );
    assert_eq!(next_packet(&mut rx).await, expected_tipi(REMOTE).await);
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    h.emit_queued(idle_source()).await;
    expect_take(&h, &mut rx).await;
    h.emit(owns(1));
    wait_until("the take's ACK", || {
        traced(mac).iter().any(|l| l.starts_with("ACK owner=linux"))
    })
    .await;

    // Ear detection takes the outer mutex and holds it, as across a slow
    // A2DP activation.
    let worn = wear(EarDetectionStatus::InEar);
    h.emit(worn.clone());
    wait_until("ear detection to hold the MediaController", || {
        outer.try_lock().is_err()
    })
    .await;

    // Another host asks: [PauseMedia, SendOwnership(false)] runs through
    // the real adapter without waiting for the mutex.
    h.emit(AACPEvent::OwnershipToFalseRequest);
    assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 1);
    let trace = traced(mac);
    assert!(
        position(&trace, "REMOTE requested ownership; yielding") < position(&trace, "PauseMedia")
    );

    // Later events still reach the core.
    h.emit(owns(0));
    wait_until("the yield's ACK", || {
        let trace = traced(mac);
        trace
            .iter()
            .skip(position(&trace, "REMOTE requested ownership; yielding"))
            .any(|l| l.starts_with("ACK owner=not-linux"))
    })
    .await;

    // And ticks: the task notices the lost session and ends.
    manager.disconnect().await;
    wait_until("the task to end", || task.is_finished()).await;
    assert!(outer.try_lock().is_err(), "the mutex was held throughout");
    assert_eq!(
        h.handler.finished(),
        named(&[idle_source(), owns(0), owns(1)])
    );

    // Released, ear detection and the rest run once, in order.
    h.handler.release.notify_one();
    let all = named(&[
        idle_source(),
        owns(0),
        owns(1),
        worn,
        AACPEvent::OwnershipToFalseRequest,
        owns(0),
    ]);
    wait_until("the handler to catch up", || h.handler.finished() == all).await;
    assert_eq!(h.handler.started(), all);
}

/// #302, #329: a take, then another host asks, through the production
/// media adapter with a pauser whose Playing players have the bus owners
/// `players`, while the stream monitor reports the PIDs owning a live
/// local stream `live` (None: no listing yet). Returns the paused
/// players' owners and `i_paused_the_media`.
async fn yield_pause(
    mac: &str,
    players: &[Option<u32>],
    live: Option<&[u32]>,
) -> (Vec<Option<u32>>, bool) {
    let paused_players = Arc::new(StdMutex::new(Vec::new()));
    let paused_for = paused_players.clone();
    let players = players.to_vec();
    let controller = yield_pause_with(
        mac,
        false,
        live,
        Arc::new(move |own: &dyn Fn(Option<u32>) -> bool| {
            let mut paused = paused_for.lock().unwrap();
            paused.extend(players.iter().copied().filter(|&owner| own(owner)));
            media_controller::PauseOutcome {
                paused: paused.len(),
                ..Default::default()
            }
        }),
    )
    .await;
    let paused = controller.i_paused_the_media().await;
    let players = paused_players.lock().unwrap().clone();
    (players, paused)
}

/// `yield_pause` with the pauser injected and `is_playing` as last seen
/// before it; returns the controller.
async fn yield_pause_with(
    mac: &str,
    is_playing: bool,
    live: Option<&[u32]>,
    pause_players: PausePlayers,
) -> MediaController {
    let (manager, mut rx, _tx) = no_source_session().await;
    let records = Arc::new(Records::default());
    let controller = MediaController::new(String::new(), LOCAL.to_string());
    controller.set_playing(is_playing).await;
    let (outer, media) = share_media(controller.clone(), pause_players);
    let (play_tx, play_rx) = mpsc::channel(8);
    let streams = watch::channel((
        PlaybackState {
            counted: false,
            baseline: false,
            live: live.map(|pids| pids.iter().copied().collect()),
        },
        Vec::new(),
    ))
    .1;
    let h = Harness::start_playback(
        SwitchingMode::Handoff,
        mac,
        &manager,
        (outer.clone(), media),
        records,
        Arc::new(DeviceHandler::default()),
        (play_tx, move || PlaybackSamples {
            rx: play_rx,
            rebaseline: Default::default(),
            players: Default::default(),
            streams,
        }),
        ConnectMode::default(),
    )
    .await;
    assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
    assert_eq!(
        next_packet(&mut rx).await,
        expected_new_device(REMOTE).await
    );
    assert_eq!(next_packet(&mut rx).await, expected_tipi(REMOTE).await);
    h.emit_queued(idle_source()).await;
    expect_take(&h, &mut rx).await;
    h.emit(owns(1));
    h.emit(AACPEvent::OwnershipToFalseRequest);
    assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    stop_handoff_task(mac);
    controller
}

fn not_pausing(mac: &str) -> bool {
    traced(mac).iter().any(|l| l.contains("not pausing"))
}

/// A pauser that counts its calls and finds no session bus.
fn bus_unavailable(calls: &Arc<AtomicUsize>) -> PausePlayers {
    let calls = calls.clone();
    Arc::new(move |_: &dyn Fn(Option<u32>) -> bool| {
        calls.fetch_add(1, Ordering::SeqCst);
        media_controller::PauseOutcome {
            bus_unavailable: true,
            ..Default::default()
        }
    })
}

/// #355: a known listing with no live local stream skips the pause
/// altogether, even while playback was last seen with no session bus.
#[tokio::test]
async fn yield_pause_known_empty_listing_skips_pause() {
    let mac = "5A:00:00:00:03:07";
    let calls = Arc::new(AtomicUsize::new(0));
    let controller = yield_pause_with(mac, true, Some(&[]), bus_unavailable(&calls)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!controller.i_paused_the_media().await);
    assert_eq!(a2dp_resume::A2dpResumeIo::self_pauses(&controller), 0);
    assert!(not_pausing(mac), "{:#?}", traced(mac));
}

/// #355: with no session bus the pause is not reported as "not pausing";
/// the self-pause mark follows #27 (`marks_self_pause`).
#[tokio::test]
async fn yield_pause_bus_unavailable_is_not_not_pausing() {
    for (mac, live, is_playing) in [
        ("5A:00:00:00:03:08", Some(&[FIREFOX_PID][..]), true),
        ("5A:00:00:00:03:09", Some(&[FIREFOX_PID][..]), false),
        ("5A:00:00:00:03:0A", None, true),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let controller = yield_pause_with(mac, is_playing, live, bus_unavailable(&calls)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1, "{mac}");
        assert_eq!(controller.i_paused_the_media().await, is_playing, "{mac}");
        assert!(!not_pausing(mac), "{:#?}", traced(mac));
    }
}

#[tokio::test]
async fn yield_pause_skips_remote_control_players() {
    let mac = "5A:00:00:00:03:02";
    let connect = [Some(CONNECT_PID)];
    assert_eq!(yield_pause(mac, &connect, Some(&[])).await, (vec![], false));
    assert!(not_pausing(mac), "{:#?}", traced(mac));
}

#[tokio::test]
async fn yield_pause_pauses_local_players() {
    let mac = "5A:00:00:00:03:03";
    let firefox = [Some(FIREFOX_PID)];
    assert_eq!(
        yield_pause(mac, &firefox, Some(&[FIREFOX_PID])).await,
        (firefox.to_vec(), true)
    );
    assert!(!not_pausing(mac), "{:#?}", traced(mac));
}

/// #328: a Playing player whose only own stream is muted pauses on the
/// yield, its PID from the stream monitor's own filter.
#[tokio::test]
async fn yield_pause_pauses_muted_local_player() {
    let mac = "5A:00:00:00:03:0B";
    let mut muted = StreamProps {
        muted: true,
        ..music(1)
    };
    muted.props.insert(
        "application.process.id".to_string(),
        FIREFOX_PID.to_string(),
    );
    let pids: Vec<u32> = stream_monitor::live_stream_pids(&[muted], |_| None)
        .into_iter()
        .collect();
    let firefox = [Some(FIREFOX_PID)];
    assert_eq!(
        yield_pause(mac, &firefox, Some(&pids)).await,
        (firefox.to_vec(), true)
    );
    assert!(!not_pausing(mac), "{:#?}", traced(mac));
}

/// #329: a Connect remote and a player whose owner lookup failed beside
/// Firefox's live stream: only Firefox pauses. Another app's live
/// stream pauses nobody.
#[tokio::test]
async fn yield_pause_spares_remote_beside_live_stream() {
    let mac = "5A:00:00:00:03:04";
    let players = [Some(CONNECT_PID), None, Some(FIREFOX_PID)];
    assert_eq!(
        yield_pause(mac, &players, Some(&[FIREFOX_PID])).await,
        (vec![Some(FIREFOX_PID)], true)
    );
    assert!(!not_pausing(mac), "{:#?}", traced(mac));
    let mac = "5A:00:00:00:03:05";
    assert_eq!(
        yield_pause(mac, &players[..2], Some(&[FIREFOX_PID])).await,
        (vec![], false)
    );
    assert!(not_pausing(mac), "{:#?}", traced(mac));
}

/// #83: before the first stream listing every Playing player pauses.
#[tokio::test]
async fn yield_pause_unknown_listing_pauses_every_player() {
    let mac = "5A:00:00:00:03:06";
    let players = [Some(CONNECT_PID), None, Some(FIREFOX_PID)];
    assert_eq!(
        yield_pause(mac, &players, None).await,
        (players.to_vec(), true)
    );
    assert!(!not_pausing(mac), "{:#?}", traced(mac));
}

/// The probe's PauseMedia path (#319): the same gate, no D-Bus.
#[tokio::test]
async fn pause_own_players_gates_on_live_streams() {
    let players = [Some(CONNECT_PID), Some(FIREFOX_PID), None];
    let pause = async |live: Option<&[u32]>| {
        let accepted = Arc::new(StdMutex::new(Vec::new()));
        let seen = accepted.clone();
        let paused = pause_own_players(
            &MediaController::new(String::new(), LOCAL.to_string()),
            Arc::new(move |own: &dyn Fn(Option<u32>) -> bool| {
                let mut seen = seen.lock().unwrap();
                seen.extend(players.iter().copied().filter(|&owner| own(owner)));
                media_controller::PauseOutcome {
                    paused: seen.len(),
                    ..Default::default()
                }
            }),
            PlaybackState {
                counted: false,
                baseline: false,
                live: live.map(|pids| pids.iter().copied().collect()),
            },
        )
        .await;
        (accepted.lock().unwrap().clone(), paused)
    };
    assert_eq!(
        pause(Some(&[FIREFOX_PID])).await,
        (vec![Some(FIREFOX_PID)], true)
    );
    assert_eq!(pause(Some(&[])).await, (vec![], false));
    assert_eq!(pause(None).await, (players.to_vec(), true));
}

/// A sound server that never answers: every listing blocks until the
/// sender is dropped.
struct StalledStreams(std::sync::mpsc::Receiver<()>);

impl PlaybackSource for StalledStreams {
    fn playback_streams(&mut self) -> io::Result<Vec<StreamProps>> {
        let _ = self.0.recv();
        Err(io::Error::other("stalled source released"))
    }
}

/// The next playback sample, as playing (above idle) or not.
async fn next_sample(rx: &mut PlaybackSamples) -> (Instant, bool) {
    let (sampled_at, category) = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .expect("expected a playback sample")
        .expect("poller running");
    (sampled_at, category > AUDIO_CATEGORY_IDLE)
}

/// Read samples until one says `playing`, for at most 3 s; returns its
/// sample time.
async fn sample_until(rx: &mut PlaybackSamples, playing: bool) -> Instant {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (sampled_at, sample) = next_sample(rx).await;
            if sample == playing {
                return sampled_at;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no playing={playing} sample"))
}

#[tokio::test]
async fn playback_poller_mpris_only_still_plays() {
    let mpris = Arc::new(AtomicBool::new(false));
    let reads = mpris.clone();
    // The player's stream, live but in the baseline, so never counted.
    let streams = LiveStreams::default();
    streams.set(vec![music(1)]);
    let mut rx = spawn_playback_poller_with(
        move || named_player(reads.load(Ordering::SeqCst)),
        streams,
        AllAudible,
        STREAM_TEST_MIN,
        STREAM_TEST_POLL,
        |_| (),
    );
    assert!(!next_sample(&mut rx).await.1);
    mpris.store(true, Ordering::SeqCst);
    sample_until(&mut rx, true).await;
    // Every sample says so while MPRIS plays, with no counted stream.
    for _ in 0..5 {
        assert!(next_sample(&mut rx).await.1);
    }
    mpris.store(false, Ordering::SeqCst);
    sample_until(&mut rx, false).await;
}

#[tokio::test]
async fn playback_poller_stream_plays_without_mpris() {
    let streams = LiveStreams::default();
    streams.set(vec![event_sound(1)]);
    let mut rx = spawn_playback_poller_with(
        Vec::new,
        streams.clone(),
        AllAudible,
        STREAM_TEST_MIN,
        STREAM_TEST_POLL,
        |_| (),
    );
    assert!(!next_sample(&mut rx).await.1);
    // The stream monitor's baseline has the event sound only.
    streams.settle().await;
    let started = Instant::now();
    streams.set(vec![event_sound(1), music(2)]);
    let playing_at = sample_until(&mut rx, true).await;
    // Only once the stream lasted the minimum.
    assert!(playing_at.duration_since(started) >= STREAM_TEST_MIN);
    for _ in 0..5 {
        assert!(next_sample(&mut rx).await.1);
    }
    // The stream ends: not playing, with only the event sound left.
    streams.set(vec![event_sound(1)]);
    sample_until(&mut rx, false).await;
}

#[tokio::test]
async fn playback_poller_stalled_stream_source_keeps_mpris() {
    let (release, stalled) = std::sync::mpsc::channel::<()>();
    let mpris = Arc::new(AtomicBool::new(false));
    let reads = mpris.clone();
    let mut rx = spawn_playback_poller_with(
        move || named_player(reads.load(Ordering::SeqCst)),
        StalledStreams(stalled),
        AllAudible,
        STREAM_TEST_MIN,
        STREAM_TEST_POLL,
        |_| (),
    );
    assert!(!next_sample(&mut rx).await.1);
    mpris.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(1), sample_until(&mut rx, true))
        .await
        .expect("an MPRIS sample despite the stalled stream source");
    for _ in 0..5 {
        assert!(next_sample(&mut rx).await.1);
    }
    drop(rx);
    // Let the blocked listing return so the runtime can shut down.
    drop(release);
}

/// Every playback sample's value for `period`.
async fn samples_for(rx: &mut PlaybackSamples, period: Duration) -> Vec<bool> {
    let end = tokio::time::Instant::now() + period;
    let mut seen = Vec::new();
    while let Ok((_, playing)) = tokio::time::timeout_at(end, next_sample(rx)).await {
        seen.push(playing);
    }
    seen
}

/// A stream-only poller (no MPRIS) listing `source`, with `peaks` and
/// the stream worker's `observed` hook, past the empty baseline listing
/// of `streams`.
async fn stream_poller<S: PlaybackSource + 'static, P: PeakSource + 'static>(
    streams: &LiveStreams,
    source: S,
    peaks: P,
    observed: impl Fn(Instant) + Send + Sync + 'static,
) -> PlaybackSamples {
    let rx = spawn_playback_poller_with(
        Vec::new,
        source,
        peaks,
        STREAM_TEST_MIN,
        STREAM_TEST_POLL,
        observed,
    );
    streams.settle().await;
    rx
}

#[tokio::test]
async fn playback_poller_rebaseline_debouncing_stream_never_plays() {
    let streams = LiveStreams::default();
    let mut rx = stream_poller(&streams, streams.clone(), AllAudible, |_| ()).await;
    streams.set(vec![music(2)]);
    streams.settle().await;
    tokio::time::sleep(STREAM_TEST_MIN / 5).await;
    // The core resets its baseline while the stream debounces.
    rx.rebaseline(Instant::now());
    rx.drain();
    let seen = samples_for(&mut rx, STREAM_TEST_MIN * 2 + STREAM_TEST_MIN / 2).await;
    assert!(!seen.is_empty());
    assert!(seen.iter().all(|playing| !playing), "{seen:?}");
}

#[tokio::test]
async fn playback_poller_rebaseline_later_stream_plays_after_min() {
    let streams = LiveStreams::default();
    let mut rx = stream_poller(&streams, streams.clone(), AllAudible, |_| ()).await;
    rx.rebaseline(Instant::now());
    rx.drain();
    streams.settle().await;
    let started = Instant::now();
    streams.set(vec![music(3)]);
    let playing_at = sample_until(&mut rx, true).await;
    assert!(playing_at.duration_since(started) >= STREAM_TEST_MIN);
}

/// A stream-only poller whose core resets while `music(6)` debounces,
/// that stream having started before the reset (`before_reset`) or just
/// after it. The re-baseline, stamped with the reset, is requested only a
/// fifth of the minimum after the stream was seen, so it applies late.
/// Returns the samples and when the stream started. It calls
/// `rebaseline` directly; the `step_rebaseline` tests go through `step`.
async fn late_rebaseline(before_reset: bool) -> (PlaybackSamples, Instant) {
    let streams = LiveStreams::default();
    let mut rx = stream_poller(&streams, streams.clone(), AllAudible, |_| ()).await;
    let start = || {
        let started = Instant::now();
        streams.set(vec![music(6)]);
        started
    };
    let started = before_reset.then(start);
    streams.settle().await;
    let reset = Instant::now();
    let started = started.unwrap_or_else(start);
    streams.settle().await;
    tokio::time::sleep(STREAM_TEST_MIN / 5).await;
    rx.rebaseline(reset);
    rx.drain();
    (rx, started)
}

#[tokio::test]
async fn playback_poller_rebaseline_late_request_keeps_start_after_reset() {
    let (mut rx, started) = late_rebaseline(false).await;
    let playing_at = sample_until(&mut rx, true).await;
    assert!(playing_at.duration_since(started) >= STREAM_TEST_MIN);
}

#[tokio::test]
async fn playback_poller_rebaseline_late_request_drops_start_before_reset() {
    let (mut rx, _) = late_rebaseline(true).await;
    let seen = samples_for(&mut rx, STREAM_TEST_MIN * 2 + STREAM_TEST_MIN / 2).await;
    assert!(!seen.is_empty());
    assert!(seen.iter().all(|playing| !playing), "{seen:?}");
}

/// The shell requests the re-baseline at the reset but reaches
/// DrainPlayback only after its awaits, past the stream's remaining
/// debounce, with a sample sender blocked meanwhile: still no true.
#[tokio::test]
async fn playback_poller_rebaseline_before_slow_drain_never_plays() {
    let streams = LiveStreams::default();
    let mut rx = stream_poller(&streams, streams.clone(), AllAudible, |_| ()).await;
    streams.set(vec![music(7)]);
    streams.settle().await;
    let before = samples_for(&mut rx, STREAM_TEST_MIN * 2 / 5).await;
    assert!(before.iter().all(|playing| !playing), "{before:?}");
    rx.rebaseline(Instant::now());
    // SendOwnership and Announce await; play_rx is not read.
    tokio::time::sleep(STREAM_TEST_MIN).await;
    rx.drain();
    let seen = samples_for(&mut rx, STREAM_TEST_MIN * 2 + STREAM_TEST_MIN / 2).await;
    assert!(!seen.is_empty());
    assert!(seen.iter().all(|playing| !playing), "{seen:?}");
}

/// `LiveStreams` whose listings wait while the test holds `hold`;
/// `started` counts the listings begun.
struct HeldStreams {
    streams: LiveStreams,
    hold: Arc<StdMutex<()>>,
    started: Arc<AtomicUsize>,
}

impl PlaybackSource for HeldStreams {
    fn playback_streams(&mut self) -> io::Result<Vec<StreamProps>> {
        self.started.fetch_add(1, Ordering::SeqCst);
        drop(self.hold.lock());
        self.streams.playback_streams()
    }
}

#[tokio::test]
async fn playback_poller_rebaseline_during_listing_never_plays() {
    let streams = LiveStreams::default();
    let hold = Arc::new(StdMutex::new(()));
    let started = Arc::new(AtomicUsize::new(0));
    let source = HeldStreams {
        streams: streams.clone(),
        hold: hold.clone(),
        started: started.clone(),
    };
    let mut rx = stream_poller(&streams, source, AllAudible, |_| ()).await;
    streams.set(vec![music(4)]);
    streams.settle().await;
    // Hold the next listing; once it returns the stream has lasted the
    // minimum, so it would count. A listing that begins before the
    // hold may still pass; the one after it waits.
    let begun = started.load(Ordering::SeqCst) + 1;
    let held = hold.lock().unwrap();
    wait_until("a held listing", || started.load(Ordering::SeqCst) >= begun).await;
    tokio::time::sleep(STREAM_TEST_MIN + STREAM_TEST_MIN / 5).await;
    rx.rebaseline(Instant::now());
    rx.drain();
    drop(held);
    let seen = samples_for(&mut rx, STREAM_TEST_MIN * 2 + STREAM_TEST_MIN / 2).await;
    assert!(!seen.is_empty());
    assert!(seen.iter().all(|playing| !playing), "{seen:?}");
}

#[tokio::test]
async fn playback_poller_rebaseline_keeps_counted_stream() {
    let streams = LiveStreams::default();
    let mut rx = stream_poller(&streams, streams.clone(), AllAudible, |_| ()).await;
    streams.set(vec![music(5)]);
    sample_until(&mut rx, true).await;
    // The ReconnectedForPlayback case: the take is for this stream.
    rx.rebaseline(Instant::now());
    rx.drain();
    let seen = samples_for(&mut rx, STREAM_TEST_MIN * 2 + STREAM_TEST_MIN / 2).await;
    assert!(!seen.is_empty());
    assert!(seen.iter().all(|playing| *playing), "{seen:?}");
}

/// What a held stream worker reports: the instant it was held at, and
/// the hold, released when sent to or dropped.
type Held = (Instant, std::sync::mpsc::Sender<()>);

/// Holds the stream worker once armed: at its first `reached(at)` with
/// `at` at or after the armed instant, it reports `at` and waits there.
#[derive(Clone, Default)]
struct Hold(Arc<StdMutex<Option<(Instant, oneshot::Sender<Held>)>>>);

impl Hold {
    fn arm(&self, from: Instant) -> oneshot::Receiver<Held> {
        let (report, held) = oneshot::channel();
        *self.0.lock().unwrap() = Some((from, report));
        held
    }

    fn reached(&self, at: Instant) {
        let armed = self.0.lock().unwrap().take_if(|(from, _)| at >= *from);
        if let Some((_, report)) = armed {
            let (release, released) = std::sync::mpsc::channel();
            if report.send((at, release)).is_ok() {
                let _ = released.recv();
            }
        }
    }
}

async fn held(held: oneshot::Receiver<Held>) -> Held {
    tokio::time::timeout(Duration::from_secs(3), held)
        .await
        .expect("the stream worker held")
        .expect("hold armed")
}

/// Every candidate audible; asked about candidates, it first waits at
/// its `Hold`, before the worker's clock read.
struct HeldPeaks(Hold);

impl PeakSource for HeldPeaks {
    fn levels(&mut self, candidates: &std::collections::HashSet<u32>) -> HashMap<u32, f32> {
        if !candidates.is_empty() {
            self.0.reached(Instant::now());
        }
        AllAudible.levels(candidates)
    }
}

/// A core step that resets the baseline, run through
/// `PlaybackSamples::handle` on its own thread over `rx`'s re-baseline
/// slot. The core's now goes to the returned receiver; the core returns
/// [DrainPlayback] once `finish` is sent to or dropped.
fn reset_step(rx: &PlaybackSamples) -> (oneshot::Receiver<Instant>, std::sync::mpsc::Sender<()>) {
    let samples = PlaybackSamples {
        rx: mpsc::channel(1).1,
        rebaseline: rx.rebaseline.clone(),
        players: Default::default(),
        streams: rx.streams.clone(),
    };
    let (reset_tx, reset) = oneshot::channel();
    let (finish, finished) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        samples.handle(|now| {
            let _ = reset_tx.send(now);
            let _ = finished.recv();
            vec![Command::DrainPlayback]
        })
    });
    (reset, finish)
}

/// The samples stamped at or after `reset` for `period`, up to and
/// including the first that `last` accepts.
async fn samples_since(
    rx: &mut PlaybackSamples,
    reset: Instant,
    period: Duration,
    last: impl Fn(bool) -> bool,
) -> Vec<(Instant, bool)> {
    let end = tokio::time::Instant::now() + period;
    let mut seen = Vec::new();
    while let Ok((sampled_at, playing)) = tokio::time::timeout_at(end, next_sample(rx)).await {
        if sampled_at >= reset {
            seen.push((sampled_at, playing));
            if last(playing) {
                break;
            }
        }
    }
    seen
}

fn values(seen: &[(Instant, bool)]) -> Vec<bool> {
    seen.iter().map(|&(_, playing)| playing).collect()
}

/// The synchronous gap: the core resets while the worker samples a
/// stream debouncing since before the reset, and the worker's clock,
/// read after the reset, completes the minimum before the core's step
/// has returned. The stream still never plays.
#[tokio::test]
async fn playback_poller_rebaseline_sync_gap_never_plays() {
    let streams = LiveStreams::default();
    let hold = Hold::default();
    let mut rx = stream_poller(&streams, streams.clone(), HeldPeaks(hold.clone()), |_| ()).await;
    // Held at the levels of the stream's first listing; its clock, read
    // after this, starts the debounce.
    let first = hold.arm(Instant::now());
    streams.set(vec![music(12)]);
    let (seen, release) = held(first).await;
    // Half the minimum later, so no observe before it completes it.
    let racing = hold.arm(seen + STREAM_TEST_MIN / 2);
    drop(release);
    let (listed, release) = held(racing).await;
    // The step runs while that sample is held, its core step held too.
    let (reset, finish) = reset_step(&rx);
    let reset = reset.await.expect("the core stepped");
    let mut seen = samples_since(&mut rx, reset, Duration::from_secs(3), |_| true).await;
    // The worker's clock completes the minimum.
    tokio::time::sleep_until((listed + STREAM_TEST_MIN).into()).await;
    drop(release);
    // Without the shared lock, the observe counts the stream here,
    // before the step's request.
    seen.extend(samples_since(&mut rx, reset, STREAM_TEST_MIN / 2, |playing| playing).await);
    drop(finish);
    seen.extend(samples_since(&mut rx, reset, STREAM_TEST_MIN * 2, |_| false).await);
    let seen = values(&seen);
    assert!(seen.iter().all(|playing| !playing), "{seen:?}");
}

/// The publish latency: the worker completes a stream's minimum
/// before the core's reset and publishes only after it. The core sees
/// no new start: every sample from the reset on plays.
#[tokio::test]
async fn playback_poller_rebaseline_publish_latency_keeps_counted_stream() {
    let streams = LiveStreams::default();
    let hold = Hold::default();
    let observed = hold.clone();
    let mut rx = stream_poller(&streams, streams.clone(), AllAudible, move |at| {
        observed.reached(at)
    })
    .await;
    // Held after an observe: the next listing has the stream, so the
    // next observe starts its debounce.
    let (_, release) = held(hold.arm(Instant::now())).await;
    streams.set(vec![music(13)]);
    let first = hold.arm(Instant::now());
    drop(release);
    let (started, release) = held(first).await;
    // Held after the observe that completes the minimum, before its
    // publish.
    let completing = hold.arm(started + STREAM_TEST_MIN);
    drop(release);
    let (_, release) = held(completing).await;
    let (mut reset, finish) = reset_step(&rx);
    drop(finish);
    // Without the shared lock the step resets now, and a sample after
    // it reads the unpublished state; with it, the step waits.
    let mut seen = Vec::new();
    let early = tokio::time::timeout(STREAM_TEST_MIN / 2, &mut reset)
        .await
        .ok()
        .map(|reset| reset.expect("the core stepped"));
    if let Some(reset) = early {
        seen = samples_since(&mut rx, reset, Duration::from_secs(3), |_| true).await;
    }
    drop(release);
    let reset = match early {
        Some(reset) => reset,
        None => reset.await.expect("the core stepped"),
    };
    seen.extend(samples_since(&mut rx, reset, STREAM_TEST_MIN * 2, |_| false).await);
    let seen = values(&seen);
    assert!(!seen.is_empty());
    assert!(seen.iter().all(|playing| *playing), "{seen:?}");
}

/// Its twin: a stream first audible after the reset plays once, after
/// the minimum.
#[tokio::test]
async fn playback_poller_rebaseline_publish_latency_later_stream_plays_once() {
    let streams = LiveStreams::default();
    let mut rx = stream_poller(&streams, streams.clone(), AllAudible, |_| ()).await;
    let (reset, finish) = reset_step(&rx);
    drop(finish);
    let reset = reset.await.expect("the core stepped");
    let started = Instant::now();
    streams.set(vec![music(14)]);
    let seen = samples_since(
        &mut rx,
        reset,
        STREAM_TEST_MIN * 2 + STREAM_TEST_MIN / 2,
        |_| false,
    )
    .await;
    let starts: Vec<Instant> = seen
        .windows(2)
        .filter(|pair| !pair[0].1 && pair[1].1)
        .map(|pair| pair[1].0)
        .collect();
    assert_eq!(starts.len(), 1, "{:?}", values(&seen));
    assert!(!seen[0].1);
    assert!(starts[0].duration_since(started) >= STREAM_TEST_MIN);
}

/// Read samples until one has `category`, for at most 3 s.
async fn category_until(rx: &mut PlaybackSamples, category: u32) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while rx.recv().await.expect("poller running").1 != category {}
    })
    .await
    .unwrap_or_else(|_| panic!("no category {category} sample"));
}

#[tokio::test]
async fn category_poller_reports_media_stream_and_idle() {
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
    assert_eq!(
        rx.recv().await.expect("poller running").1,
        AUDIO_CATEGORY_IDLE
    );
    streams.settle().await;
    streams.set(vec![music(61)]);
    category_until(&mut rx, AUDIO_CATEGORY_STREAM).await;
    mpris.store(true, Ordering::SeqCst);
    category_until(&mut rx, AUDIO_CATEGORY_MEDIA).await;
    mpris.store(false, Ordering::SeqCst);
    category_until(&mut rx, AUDIO_CATEGORY_STREAM).await;
    // An event sound never counts.
    streams.set(vec![event_sound(62)]);
    category_until(&mut rx, AUDIO_CATEGORY_IDLE).await;
}

/// #302: a playing MPRIS player with no live local stream (a remote
/// control such as Spotify Connect) is idle once the stream monitor has
/// listed.
#[tokio::test]
async fn playback_poller_mpris_without_live_stream_is_idle() {
    let streams = LiveStreams::default();
    let mut rx = spawn_playback_poller_with(
        || named_player(true),
        streams.clone(),
        AllAudible,
        STREAM_TEST_MIN,
        STREAM_TEST_POLL,
        |_| (),
    );
    streams.settle().await;
    // Samples taken before the first listing may still be queued.
    sample_until(&mut rx, false).await;
    for _ in 0..5 {
        assert!(!next_sample(&mut rx).await.1);
    }
}

/// #329: a playing MPRIS player with a live stream that another process
/// owns counts the stream only, never as the player's media.
#[tokio::test]
async fn playback_poller_mpris_with_other_process_stream_is_not_media() {
    let streams = LiveStreams::default();
    let mut rx = spawn_playback_poller_with(
        || named_player(true),
        streams.clone(),
        AllAudible,
        STREAM_TEST_MIN,
        STREAM_TEST_POLL,
        |_| (),
    );
    streams.settle().await;
    let mut other = music(71);
    other.props.insert(
        "application.process.id".to_string(),
        (PLAYER_PID + 1).to_string(),
    );
    streams.set(vec![other]);
    category_until(&mut rx, AUDIO_CATEGORY_STREAM).await;
    for _ in 0..5 {
        let category = rx.recv().await.expect("poller running").1;
        assert_ne!(category, AUDIO_CATEGORY_MEDIA);
    }
}

/// #302: Linux's category over every (mpris, live, counted), the live
/// input from the stream monitor's MPRIS filter (`live_stream_pids`)
/// over the player's music stream, muted or not (live, #328), or over a
/// corked and an event stream (not live).
#[test]
fn local_category_needs_live_stream_for_mpris() {
    let live_listing = [
        music(1),
        StreamProps {
            muted: true,
            ..music(1)
        },
    ];
    let not_live = [
        StreamProps {
            corked: true,
            ..music(1)
        },
        event_sound(1),
    ];
    let (media, stream, idle) = (
        AUDIO_CATEGORY_MEDIA,
        AUDIO_CATEGORY_STREAM,
        AUDIO_CATEGORY_IDLE,
    );
    for (mpris, live, counted, category) in [
        (true, true, true, media),
        (true, true, false, media),
        (true, false, true, stream),
        (true, false, false, idle),
        (false, true, true, stream),
        (false, true, false, idle),
        (false, false, true, stream),
        (false, false, false, idle),
    ] {
        let listings = if live { &live_listing } else { &not_live };
        for s in listings {
            let filtered =
                stream_monitor::live_stream_pids(&[s.clone()], |_| None).contains(&PLAYER_PID);
            assert_eq!(
                local_category(mpris, filtered, counted),
                category,
                "mpris={mpris} counted={counted} {s:?}"
            );
        }
    }
}
