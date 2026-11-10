use super::*;

/// The resume chain's actions traced so far, in order.
fn resume_actions(mac: &str) -> Vec<String> {
    traced(mac)
        .into_iter()
        .filter(|l| l.starts_with("Resume "))
        .collect()
}

/// An automatic take with the transport reported idle, acknowledged:
/// the core resumes A2DP.
async fn resumed_take(h: &Harness, rx: &mut mpsc::Receiver<Vec<u8>>, mac: &str) {
    expect_startup(h, rx).await;
    h.a2dp(false).await;
    expect_take(h, rx).await;
    h.emit(owns(1));
    wait_until("the resume chain's start", || {
        !resume_actions(mac).is_empty()
    })
    .await;
}

#[tokio::test]
async fn a2dp_resume_chain_runs_after_take_ack_without_release() {
    let mac = "5A:00:00:00:00:C8";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    resumed_take(&h, &mut rx, mac).await;
    let trace = traced(mac);
    assert!(
        position(&trace, "A2DP transport idle") < position(&trace, "Resume "),
        "{trace:#?}"
    );
    // Playback stops inside the window: no release.
    h.play(false).await;
    let player = format!("{PLAYER:?}");
    let expected = [
        "SuspendSink(true)".to_string(),
        "SuspendSink(false)".to_string(),
        format!("Pause({player})"),
        format!("Play({player})"),
        "ProfileCycle".to_string(),
        format!("Play({player})"),
    ]
    .map(|action| format!("Resume {action}"));
    tokio::time::timeout(Duration::from_secs(8), async {
        while resume_actions(mac).len() < expected.len() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the whole resume chain");
    assert_eq!(resume_actions(mac), expected);
    // Still inside the window: no OwnsConnection at all.
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 0);

    stop_handoff_task(mac);
}

#[tokio::test]
async fn a2dp_resume_chain_stops_after_yield() {
    let mac = "5A:00:00:00:00:C9";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    resumed_take(&h, &mut rx, mac).await;
    // Another host takes the AirPods while the kick has the player
    // paused: the yield pauses, and the chain plays nothing after it.
    tokio::time::timeout(Duration::from_secs(3), async {
        while !resume_actions(mac)
            .iter()
            .any(|a| a.starts_with("Resume Pause"))
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the kick's Pause");
    h.emit(owns(0));
    wait_until("the yield's pause", || h.pauses() == 1).await;
    // Longer than the rest of the chain would take.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let trace = traced(mac);
    let paused = position(&trace, "PauseMedia");
    let after: Vec<_> = trace[paused..]
        .iter()
        .filter(|l| l.starts_with("Resume "))
        .collect();
    assert_eq!(after, Vec::<&String>::new(), "{trace:#?}");
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 1);

    stop_handoff_task(mac);
}

/// Another host takes the AirPods while the chain's `check`-th audio
/// check runs (2: after the kick, 3: after the profile cycle): the chain
/// acts no more after the yield's pause.
async fn yield_during_audio_check(mac: &str, check: usize) {
    HELD_CHECKS.lock().unwrap().push(mac.to_string());
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    resumed_take(&h, &mut rx, mac).await;
    let count = |trace: &[String], entry: &str| trace.iter().filter(|l| *l == entry).count();
    tokio::time::timeout(Duration::from_secs(10), async {
        while count(&traced(mac), "Audible check") < check {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the audio check");
    h.emit(owns(0));
    wait_until("the yield's pause", || h.pauses() == 1).await;
    // Past the held check and what the chain would do after it.
    tokio::time::sleep(HELD_CHECK * 2).await;
    let trace = traced(mac);
    let paused = position(&trace, "PauseMedia");
    // The yield came during that check.
    assert_eq!(
        count(&trace[..paused], "Audible checked"),
        check - 1,
        "{trace:#?}"
    );
    let after: Vec<_> = trace[paused..]
        .iter()
        .filter(|l| l.starts_with("Resume "))
        .collect();
    assert_eq!(after, Vec::<&String>::new(), "{trace:#?}");
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 1);

    stop_handoff_task(mac);
}

#[tokio::test]
async fn a2dp_resume_yield_during_check_before_cycle_stops_chain() {
    yield_during_audio_check("5A:00:00:00:00:CA", 2).await;
}

#[tokio::test]
async fn a2dp_resume_yield_during_check_after_cycle_stops_chain() {
    yield_during_audio_check("5A:00:00:00:00:CB", 3).await;
}

#[tokio::test]
async fn handoff_task_ends_on_session_loss_without_reconnect() {
    let mac = "5A:00:00:00:00:03";
    let (manager, mut rx, tx) = no_source_session().await;
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    let task = handoff_task(mac).expect("handoff task");
    expect_startup(&h, &mut rx).await;
    expect_take(&h, &mut rx).await;
    h.emit(owns(1));

    // The receive loop clears the dead session without an event.
    manager.disconnect().await;
    wait_until("the task to end", || task.is_finished()).await;
    // Disconnected was fed: the core paused the media Linux took for.
    assert_eq!(h.pauses(), 1);
    assert!(handoff_task(mac).is_none());

    // Nothing is sent afterwards, even on a live sender: no reconnect,
    // no take.
    manager.state.lock().await.sender = Some(tx);
    h.play(false).await;
    h.play(true).await;
    h.emit(owns(0));
    h.emit(AACPEvent::OwnershipToFalseRequest);
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert!(rx.try_recv().is_err());
    assert_eq!(h.pauses(), 1);
    assert_eq!(h.activations(), 1);
}

#[tokio::test]
async fn handoff_closed_event_stream_feeds_session_lost_then_disconnected() {
    let mac = "5A:00:00:00:00:12";
    let (manager, mut rx, _tx) = no_source_session().await;
    let mut h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    let task = handoff_task(mac).expect("handoff task");
    expect_startup(&h, &mut rx).await;
    expect_take(&h, &mut rx).await;
    h.emit(owns(1));
    wait_until("the take's ACK", || {
        traced(mac)
            .iter()
            .any(|line| line.starts_with("ACK owner=linux"))
    })
    .await;
    let before = traced(mac).len();

    // Every sender of the primary channel goes; the session stays alive.
    h.close_events();
    manager.set_event_channel(mpsc::unbounded_channel().0).await;
    wait_until("the task to end", || task.is_finished()).await;
    assert!(handoff_task(mac).is_none());

    let trace = traced(mac)[before..].to_vec();
    assert!(
        trace.len() >= 2
            && trace[0].starts_with("SESSION lost")
            && trace[1].starts_with("DISCONNECTED"),
        "{trace:#?}"
    );
    // Disconnected owes the pause for the automatic take.
    assert_eq!(h.pauses(), 1);

    // Nothing is sent afterwards, even though the session is alive.
    h.play(false).await;
    h.play(true).await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert!(rx.try_recv().is_err());
    assert_eq!(h.pauses(), 1);
}

/// The task's DropSession reports whether the case was closed (#307).
#[tokio::test]
async fn drop_session_reports_case_closed() {
    for (mac, ears, closed) in [
        ("5A:00:00:00:07:0C", EarDetectionStatus::InCase, true),
        ("5A:00:00:00:07:0D", EarDetectionStatus::OutOfEar, false),
    ] {
        reconnect::case_closed_at_drop(mac, !closed);
        let (manager, mut rx, _tx) = no_source_session().await;
        let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
        let task = handoff_task(mac).expect("handoff task");
        expect_startup(&h, &mut rx).await;
        h.emit_queued(wear(ears)).await;
        manager.disconnect().await;
        wait_until("the task to end", || task.is_finished()).await;
        assert_eq!(reconnect::closed_at_drop(mac), closed, "{mac}");
    }
}

/// #358: a reinsertion's `false` report handled while its EarState is
/// still queued (the report won the select) still ends the grace. Calls
/// the ear_rx arm directly: a current-thread runtime can't interleave
/// the select's polls with the router.
#[tokio::test]
async fn ear_report_drains_its_queued_ear_state() {
    use EarDetectionStatus::{InEar, OutOfEar};
    let mac = "5A:00:00:00:00:1E";
    let (manager, mut rx, _tx) = ready_session().await;
    let (events_tx, events) = mpsc::unbounded_channel();
    let mut shell = bare_shell(mac, manager, events);
    // Remote owns and plays media; one bud out.
    let mut core = Handoff::new(LOCAL.to_string(), Instant::now(), true, true);
    for input in [
        Input::ConnectedDevices {
            old: Vec::new(),
            new: vec![host(LOCAL), host(REMOTE)],
        },
        Input::EarState(vec![InEar, InEar]),
        Input::Ownership(vec![0]),
        Input::Playback {
            sampled_at: Instant::now(),
            category: AUDIO_CATEGORY_IDLE,
            new_source: None,
        },
        Input::AudioSource(AudioSource {
            mac: REMOTE.to_string(),
            r#type: AudioSourceType::Media,
        }),
        Input::MediaInformation {
            host: REMOTE.to_string(),
            category: AUDIO_CATEGORY_MEDIA,
        },
        Input::EarState(vec![InEar, OutOfEar]),
    ] {
        core.handle(Instant::now(), input);
    }
    events_tx
        .send(AACPEvent::EarDetection(
            vec![InEar, OutOfEar],
            vec![InEar, InEar],
        ))
        .unwrap();
    assert!(!on_ear_resume(&mut core, &mut shell, false).await);
    // Whatever is still queued is stepped next, as the select would.
    while let Ok(event) = shell.events.try_recv() {
        assert!(!on_event(&mut core, &mut shell, event).await);
    }
    let play = Input::Playback {
        sampled_at: Instant::now(),
        category: AUDIO_CATEGORY_MEDIA,
        new_source: None,
    };
    assert!(!step(&mut core, &mut shell, play).await);
    let mut packets = Vec::new();
    while let Ok(packet) = rx.try_recv() {
        packets.push(packet);
    }
    assert_eq!(packets, [expected_owns(1).await]);
    assert_eq!(core.ear_reports_due(), 0);
}

#[tokio::test]
async fn handoff_session_loss_drains_primary_queue() {
    let mac = "5A:00:00:00:00:09";
    let (manager, mut rx, tx) = no_source_session().await;
    let h = Harness::start_with(SwitchingMode::Handoff, mac, &manager, true).await;
    let task = handoff_task(mac).expect("handoff task");
    expect_startup(&h, &mut rx).await;
    expect_take(&h, &mut rx).await;
    h.emit_queued(owns(1)).await;
    // Playback stops: Linux stays the idle owner (#262), so no release.
    h.play(false).await;
    wait_until("the stop", || {
        traced(mac)
            .iter()
            .any(|l| l == "AUTO playback stopped; keeping ownership")
    })
    .await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 0);

    // The buds go in the case; the device handler blocks on that report.
    let in_case = wear(EarDetectionStatus::InCase);
    h.emit(in_case.clone());
    wait_until("the handler to block", || {
        h.handler.started() == named(&[idle_source(), owns(0), owns(1), in_case.clone()])
    })
    .await;
    let before = traced(mac).len();

    // Another host's [0] arrives, then the receive loop clears the
    // session. The test runtime is single-threaded and neither call
    // yields, so no task sees the [0] before the session is gone.
    let loss = expected_owns(0).await;
    manager.receive_packet(&loss).await;
    manager.disconnect().await;
    wait_until("the task to end", || task.is_finished()).await;

    // SessionLost → [], Ownership([0]) → the yield's [PauseMedia],
    // Disconnected → [DropSession], with the case closure from the
    // queued wear report.
    let trace = traced(mac)[before..].to_vec();
    let expected = [
        "SESSION lost",
        "YIELD",
        "PauseMedia",
        "DISCONNECTED",
        "CASE waiting for new Linux playback",
    ];
    assert_eq!(trace.len(), expected.len(), "{trace:#?}");
    for (entry, prefix) in trace.iter().zip(expected) {
        assert!(entry.starts_with(prefix), "{trace:#?}");
    }
    assert_eq!(h.pauses(), 1);
    assert!(handoff_task(mac).is_none());

    // Nothing is sent afterwards, even on a live sender.
    manager.state.lock().await.sender = Some(tx);
    h.play(true).await;
    h.emit(AACPEvent::OwnershipToFalseRequest);
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert!(rx.try_recv().is_err());
    assert_eq!(h.pauses(), 1);

    // Released, the handler gets the wear report once, then the rest in
    // order.
    assert_eq!(
        h.handler.finished(),
        named(&[idle_source(), owns(0), owns(1)])
    );
    h.handler.release.notify_one();
    let all = named(&[
        idle_source(),
        owns(0),
        owns(1),
        in_case,
        owns(0),
        AACPEvent::OwnershipToFalseRequest,
    ]);
    wait_until("the handler to catch up", || h.handler.finished() == all).await;
    assert_eq!(h.handler.started(), all);
}

#[tokio::test]
async fn handoff_take_and_yield_route_default_sink() {
    let mac = "5A:00:00:00:00:28";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    expect_startup(&h, &mut rx).await;
    expect_take(&h, &mut rx).await;
    assert_eq!(h.routes(), []);
    h.emit(owns(1));
    wait_until("the take's route", || !h.routes().is_empty()).await;
    // Another host takes the AirPods.
    h.emit(owns(0));
    wait_until("the yield's route", || h.routes().len() > 1).await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.routes(), [SinkEvent::Take, SinkEvent::Yield]);
    stop_handoff_task(mac);
}

/// #264: a take, a remote yield, then a retake: the hold, then its
/// release before the retake's route and resume chain.
#[tokio::test]
async fn hold_on_remote_yield_released_before_retake_route() {
    let mac = "5A:00:00:00:01:08";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    expect_startup(&h, &mut rx).await;
    expect_take(&h, &mut rx).await;
    h.emit(owns(1));
    wait_until("the take's route", || !h.routes().is_empty()).await;

    h.emit(AACPEvent::OwnershipToFalseRequest);
    assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
    h.emit(owns(0));
    wait_until("the hold", || {
        h.sink_events().contains(&SinkEvent::Hold(true))
    })
    .await;

    h.a2dp(false).await;
    h.play(false).await;
    h.play(true).await;
    assert_eq!(next_packet(&mut rx).await, expected_owns(1).await);
    h.emit(owns(1));
    wait_until("the resume chain's start", || {
        !resume_actions(mac).is_empty()
    })
    .await;
    use SinkEvent::{Hold, Take, Yield};
    let events = [Hold(false), Take, Yield, Hold(true), Hold(false), Take];
    assert_eq!(h.sink_events(), events);
    // The chain was spawned after the release and the retake's route.
    assert_eq!(*h.records.resume_spawns.lock().unwrap(), [events.len()]);
    stop_handoff_task(mac);
}

/// A handoff task on a `no_source_session` with the streams known that
/// records its park routes, past its startup.
async fn park_task(mac: &str, rx: &mut mpsc::Receiver<Vec<u8>>, manager: &AACPManager) -> Harness {
    let (records, media) = Harness::fake_media(mac);
    records.parks.store(true, Ordering::SeqCst);
    let h = Harness::start_playback(
        SwitchingMode::Handoff,
        mac,
        manager,
        media,
        records,
        Arc::new(DeviceHandler::default()),
        known_play_channel(),
        ConnectMode::default(),
    )
    .await;
    expect_startup(&h, rx).await;
    h
}

#[tokio::test]
async fn park_take_while_parked_unparks_to_airpods_after_the_route() {
    let mac = "5A:00:00:00:11:01";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = park_task(mac, &mut rx, &manager).await;
    park_after_remote_request(&h, &mut rx).await;
    h.play(false).await;
    h.play(true).await;
    assert_eq!(next_packet(&mut rx).await, expected_owns(1).await);
    h.emit(owns(1));
    wait_until("the unpark", || h.sink_events().len() == 6).await;
    use SinkEvent::{Hold, Park, Take, Unpark};
    assert_eq!(
        h.sink_events(),
        [
            Hold(false),
            Hold(true),
            Park,
            Hold(false),
            Take,
            Unpark(UnparkTo::AirPods)
        ]
    );
    stop_handoff_task(mac);
}

/// The task dropped inside the ACK step's commands, after the take's
/// route, still unparks. No await follows that route, so a panic stands
/// in for an abort: both drop the `Shell` there.
#[tokio::test]
async fn park_drop_after_the_acks_route_still_unparks() {
    let mac = "5A:00:00:00:11:08";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = park_task(mac, &mut rx, &manager).await;
    park_after_remote_request(&h, &mut rx).await;
    h.play(false).await;
    h.play(true).await;
    assert_eq!(next_packet(&mut rx).await, expected_owns(1).await);
    h.records.panic_at_take.store(true, Ordering::SeqCst);
    let task = handoff_task(mac).expect("task");
    h.emit(owns(1));
    let events = after_stop(&h, task).await;
    use SinkEvent::{Hold, Take, Unpark};
    assert!(
        events.ends_with(&[Hold(false), Take, Unpark(UnparkTo::Local)]),
        "{events:?}"
    );
    stop_handoff_task(mac);
}

#[tokio::test]
async fn park_yield_while_playing_parks_on_the_idle_sample() {
    let mac = "5A:00:00:00:11:02";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = park_task(mac, &mut rx, &manager).await;
    expect_take(&h, &mut rx).await;
    h.emit(owns(1));
    wait_until("the take's route", || !h.routes().is_empty()).await;
    h.emit(AACPEvent::OwnershipToFalseRequest);
    assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
    wait_until("the hold", || {
        h.sink_events().contains(&SinkEvent::Hold(true))
    })
    .await;
    tokio::time::sleep(SETTLE).await;
    use SinkEvent::{Hold, Park, Take, Yield};
    // Still playing: no park yet.
    assert_eq!(h.sink_events(), [Hold(false), Take, Yield, Hold(true)]);
    h.play(false).await;
    wait_until("the park", || h.sink_events().len() == 5).await;
    assert_eq!(
        h.sink_events(),
        [Hold(false), Take, Yield, Hold(true), Park]
    );
    stop_handoff_task(mac);
}

#[tokio::test]
async fn park_task_stop_while_parked_unparks_before_the_release() {
    let mac = "5A:00:00:00:11:03";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = park_task(mac, &mut rx, &manager).await;
    park_after_remote_request(&h, &mut rx).await;
    let task = handoff_task(mac).expect("task");
    stop_handoff_task(mac);
    let events = after_stop(&h, task).await;
    assert!(
        events.ends_with(&[SinkEvent::Unpark(UnparkTo::Local), SinkEvent::Hold(false)]),
        "{events:?}"
    );
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
}

/// The exit path while parked: the unpark runs ahead of the release,
/// which outlasts `SHUTDOWN_RELEASE_WAIT`.
#[tokio::test(start_paused = true)]
async fn park_shutdown_unparks_before_a_slow_release() {
    let mac = "5A:00:00:00:11:05";
    let (manager, mut rx, _tx) = no_source_session().await;
    let (_h, sinks) = router_parked_task(mac, &mut rx, &manager, None).await;
    let registry = private_registry(&[mac]);
    let stopped = tokio::time::Instant::now();
    shutdown(registry).await;
    assert!(stopped.elapsed() <= SHUTDOWN_RELEASE_WAIT);
    let sets = sinks.sets();
    assert!(
        sets.ends_with(&[DAC, "unload 7", "Hold(false)"].map(String::from)),
        "{sets:?}"
    );
    assert_eq!(default_sink(&sinks), DAC);
}

/// The exit path while parked, with the park's watch still connecting:
/// the unpark sets the local sink and unloads the park sink, and the
/// hold release runs, all before `SHUTDOWN_RELEASE_WAIT` runs out: no
/// unpark waits for the watch (#470). Real time: the watch runs on its
/// own thread.
#[tokio::test]
async fn park_shutdown_unparks_before_a_slow_watch_stops() {
    let mac = "5A:00:00:00:11:09";
    let (manager, mut rx, _tx) = no_source_session().await;
    let (connected, gate) = std::sync::mpsc::channel();
    let (_h, sinks) = router_parked_task(mac, &mut rx, &manager, Some(gate)).await;
    shutdown(private_registry(&[mac])).await;
    // The production exit follows the shutdown wait at once.
    let sets = sinks.sets();
    assert!(
        sets.ends_with(&[DAC, "unload 7", "Hold(false)"].map(String::from)),
        "{sets:?}"
    );
    assert_eq!(default_sink(&sinks), DAC);
    drop(connected);
}
