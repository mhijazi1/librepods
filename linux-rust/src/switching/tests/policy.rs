use super::*;

async fn expected_call(remote: &'static str) -> Vec<u8> {
    reference(|m| async move { m.send_media_information_call(LOCAL, remote).await }).await
}

/// Packets the legacy takeover sends and the handoff path must not.
async fn hijack_packets() -> Vec<Vec<u8>> {
    vec![
        reference(|m| async move { m.send_hijack_request(REMOTE).await }).await,
        reference(|m| async move { m.send_smart_routing_show_ui(REMOTE).await }).await,
        reference(|m| async move { m.send_media_information(LOCAL, REMOTE, true).await }).await,
        reference(|m| async move { m.send_media_information(LOCAL, REMOTE, false).await }).await,
    ]
}

/// Startup on a `ready_session`: the connect take from the idle other
/// host (#324), [ConnectA2dp, SendOwnership(true), Announce], and
/// nothing else.
async fn expect_connect_take(h: &Harness, rx: &mut mpsc::Receiver<Vec<u8>>) {
    assert_eq!(next_packet(rx).await, expected_owns(1).await);
    assert_eq!(next_packet(rx).await, expected_new_device(REMOTE).await);
    assert_eq!(next_packet(rx).await, expected_tipi(REMOTE).await);
    assert_eq!(rest(rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.activations(), 1);
    assert_eq!(h.pauses(), 0);
}

/// The core's connect-mode skip lines traced for `mac` (#16).
fn connect_mode_skips(mac: &str) -> usize {
    traced(mac)
        .iter()
        .filter(|line| {
            line.starts_with("AUTO takeover skipped: connect mode is when-last-connected")
        })
        .count()
}

/// "When last connected" on a fresh session (Linux not the last owner):
/// new local playback is skipped, not taken.
#[tokio::test]
async fn handoff_when_last_connected_skips_new_playback() {
    let mac = "5A:00:00:00:01:52";
    let (manager, mut rx, _tx) = ready_session().await;
    let h = Harness::start_connect(ConnectMode::WhenLastConnected, mac, &manager).await;
    expect_startup(&h, &mut rx).await;

    h.emit(owns(0));
    h.play(false).await;
    h.play(true).await;
    wait_until("the connect-mode skip", || connect_mode_skips(mac) > 0).await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(connect_mode_skips(mac), 1);
    stop_handoff_task(mac);
}

/// The twin in "Automatically": the same scenario takes.
#[tokio::test]
async fn handoff_automatically_takes_new_playback() {
    let mac = "5A:00:00:00:01:53";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = Harness::start_connect(ConnectMode::Automatically, mac, &manager).await;
    expect_startup(&h, &mut rx).await;

    expect_take(&h, &mut rx).await;
    assert_eq!(connect_mode_skips(mac), 0);
    stop_handoff_task(mac);
}

/// The running task's `CONNECT MODE ` lines (#153).
fn connect_mode_changes(mac: &str) -> usize {
    traced(mac)
        .iter()
        .filter(|line| line.starts_with("CONNECT MODE "))
        .count()
}

/// #153: switching to "Automatically" while the task runs, as the window
/// does, sends nothing by itself; the next new playback takes.
#[tokio::test]
async fn live_connect_mode_automatically_ungates_new_playback() {
    let mac = "5A:01:53:00:00:01";
    let (manager, mut rx, _tx) = ready_session().await;
    let h = Harness::start_connect(ConnectMode::WhenLastConnected, mac, &manager).await;
    expect_startup(&h, &mut rx).await;

    set_connect_mode(mac, ConnectMode::Automatically);
    wait_until("the live connect mode", || connect_mode_changes(mac) == 1).await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    expect_take(&h, &mut rx).await;
    assert_eq!(connect_mode_skips(mac), 0);
    stop_handoff_task(mac);
}

/// #153: switching to "When last connected" while the task runs gates the
/// next new playback, and sends nothing.
#[tokio::test]
async fn live_connect_mode_when_last_connected_gates_new_playback() {
    let mac = "5A:01:53:00:00:02";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = Harness::start_connect(ConnectMode::Automatically, mac, &manager).await;
    expect_startup(&h, &mut rx).await;

    set_connect_mode(mac, ConnectMode::WhenLastConnected);
    wait_until("the live connect mode", || connect_mode_changes(mac) == 1).await;
    h.emit(owns(0));
    h.play(false).await;
    h.play(true).await;
    wait_until("the connect-mode skip", || connect_mode_skips(mac) > 0).await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(connect_mode_skips(mac), 1);
    stop_handoff_task(mac);
}

/// #153: with Linux owning, switching to "When last connected" releases
/// nothing (spike #255) and keeps Linux the last owner.
#[tokio::test]
async fn live_connect_mode_change_keeps_linux_ownership() {
    let mac = "5A:01:53:00:00:03";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = Harness::start_connect(ConnectMode::Automatically, mac, &manager).await;
    expect_startup(&h, &mut rx).await;
    expect_take(&h, &mut rx).await;
    h.emit_queued(owns(1)).await;
    wait_until("the take's ACK", || {
        traced(mac).iter().any(|l| l.starts_with("ACK owner=linux"))
    })
    .await;

    set_connect_mode(mac, ConnectMode::WhenLastConnected);
    wait_until("the live connect mode", || connect_mode_changes(mac) == 1).await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    h.play(false).await;
    h.play(true).await;
    wait_until("the new play edge", || {
        traced(mac)
            .iter()
            .any(|l| l == "AUTO playback started; Linux already owns")
    })
    .await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(connect_mode_skips(mac), 0);
    assert!(last_owner_linux(mac));
    stop_handoff_task(mac);
}

/// #153: a change with no running task, before one starts or after it
/// stopped, reaches nothing and leaves nothing for the next task.
#[tokio::test]
async fn live_connect_mode_without_task_is_ignored() {
    let mac = "5A:01:53:00:00:04";
    set_connect_mode(mac, ConnectMode::Automatically);
    let (manager, mut rx, _tx) = ready_session().await;
    let h = Harness::start_connect(ConnectMode::WhenLastConnected, mac, &manager).await;
    expect_startup(&h, &mut rx).await;
    stop_handoff_task(mac);
    set_connect_mode(mac, ConnectMode::Automatically);
    assert_eq!(connect_mode_changes(mac), 0);

    let (manager, mut rx, _tx) = ready_session().await;
    let h = Harness::start_connect(ConnectMode::WhenLastConnected, mac, &manager).await;
    expect_startup(&h, &mut rx).await;
    h.emit(owns(0));
    h.play(false).await;
    h.play(true).await;
    wait_until("the connect-mode skip", || connect_mode_skips(mac) > 0).await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(connect_mode_skips(mac), 1);
    assert_eq!(connect_mode_changes(mac), 0);
    stop_handoff_task(mac);
}

/// #153: a change while the task waits for its predecessor's release
/// (#294) reaches the core it then builds: the startup's connect take
/// (#324) follows the new mode.
#[tokio::test]
async fn live_connect_mode_during_the_start_wait_is_applied() {
    let mac = "5A:01:53:00:00:05";
    let (first_manager, mut first_rx, _first_tx) = no_source_session().await;
    let first = Harness::start_connect(ConnectMode::Automatically, mac, &first_manager).await;
    expect_startup(&first, &mut first_rx).await;
    first.records.gated.store(true, Ordering::SeqCst);

    let (manager, mut rx, _tx) = ready_session().await;
    let h = Harness::start_connect(ConnectMode::WhenLastConnected, mac, &manager).await;
    tokio::time::sleep(SETTLE).await;
    assert!(rx.try_recv().is_err());
    set_connect_mode(mac, ConnectMode::Automatically);
    first.records.gate.notify_one();
    expect_connect_take(&h, &mut rx).await;
    stop_handoff_task(mac);
}

/// #324: the daemon's session start finds the other host idle, so its
/// first ownership packet is OwnsConnection [1], as the Mac takes when it
/// connects (spike #325), with no pause; the acknowledged idle take
/// sends nothing more.
#[tokio::test]
async fn startup_takes_from_idle_remote_owner() {
    let mac = "5A:00:00:00:03:24";
    let (manager, mut rx, _tx) = ready_session().await;
    let h = Harness::start_connect(ConnectMode::Automatically, mac, &manager).await;
    expect_connect_take(&h, &mut rx).await;
    h.emit_queued(owns(1)).await;
    h.play(false).await;
    wait_until("the take's ACK", || {
        traced(mac).iter().any(|l| l.starts_with("ACK owner=linux"))
    })
    .await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 0);
    stop_handoff_task(mac);
}

/// #452: on hardware the routing reports land after the session start
/// (spike #157). The startup sends nothing; the reports then make the
/// connect take, its only ownership packet, with no pause.
#[tokio::test]
async fn startup_connect_take_waits_for_the_routing_reports() {
    let mac = "5A:00:00:00:04:52";
    let (manager, mut rx, _tx) = injected_manager().await;
    let h = Harness::start_connect(ConnectMode::Automatically, mac, &manager).await;
    wait_until("the startup", || {
        traced(mac).iter().any(|l| l.starts_with("AUTO enabled"))
    })
    .await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    // As the manager does: the state first, then the event.
    let hosts = vec![host(LOCAL), host(REMOTE)];
    manager.state.lock().await.connected_devices = hosts.clone();
    h.emit_queued(AACPEvent::ConnectedDevices(Vec::new(), hosts))
        .await;
    h.emit_queued(wear(EarDetectionStatus::InEar)).await;
    h.emit_queued(idle_source()).await;
    h.emit_queued(AACPEvent::MediaInformation {
        host: REMOTE.to_string(),
        category: AUDIO_CATEGORY_IDLE,
    })
    .await;
    assert_eq!(
        next_packet(&mut rx).await,
        expected_new_device(REMOTE).await
    );
    assert_eq!(next_packet(&mut rx).await, expected_tipi(REMOTE).await);
    assert_eq!(next_packet(&mut rx).await, expected_owns(1).await);
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 0);
    let takes = traced(mac)
        .iter()
        .filter(|l| l.starts_with("AUTO connected while the other host is idle"))
        .count();
    assert_eq!(takes, 1);
    stop_handoff_task(mac);
}

#[tokio::test]
async fn handoff_default_path_takes_and_keeps_ownership() {
    let mac = "5A:00:00:00:00:01";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = Harness::start(SwitchingMode::default(), mac, &manager).await;
    assert!(h.playback_started.load(Ordering::SeqCst));
    assert!(handoff_task(mac).is_some());
    assert!(!media_controller::playback_listener_registered(mac));

    expect_startup(&h, &mut rx).await;

    // Take on new playback: OwnsConnection [1], no hijack, show-UI or
    // media information packet. The AirPods' own owner report, [0],
    // is stepped first, as on hardware.
    h.emit_queued(owns(0)).await;
    h.play(false).await;
    h.play(true).await;
    let take = next_packet(&mut rx).await;
    assert_eq!(take, expected_owns(1).await);
    assert!(!hijack_packets().await.contains(&take));
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());

    // ACK, then playback stops: Linux stays the idle owner (#262), so
    // no release packet.
    h.emit_queued(owns(1)).await;
    h.play(false).await;
    wait_until("the stop", || {
        traced(mac)
            .iter()
            .any(|l| l == "AUTO playback stopped; keeping ownership")
    })
    .await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 0);
    assert_eq!(h.activations(), 1);
    // The device handler saw every report too.
    assert_eq!(
        h.handler.finished(),
        named(&[idle_source(), owns(0), owns(1)])
    );

    // New playback while Linux owns sends nothing; another host then
    // takes the AirPods (a [0] that acknowledges nothing): the core
    // yields once, with exactly one pause and no packet (#39's
    // `yield_take`).
    h.play(true).await;
    wait_until("the play edge", || {
        traced(mac)
            .iter()
            .any(|l| l == "AUTO playback started; Linux already owns")
    })
    .await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    h.emit(owns(0));
    wait_until("the yield's pause", || h.pauses() >= 1).await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 1);
    let trace = traced(mac);
    assert_eq!(
        trace.iter().filter(|l| l.starts_with("YIELD")).count(),
        1,
        "{trace:#?}"
    );
    assert_eq!(h.activations(), 1);

    stop_handoff_task(mac);
}

#[tokio::test]
async fn handoff_default_path_boundary_follows_pre_snapshot_backlog() {
    let mac = "5A:00:00:00:00:31";
    let (manager, _rx, _tx) = ready_session().await;
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    // Nothing awaited since the task was spawned on this current-thread
    // runtime: it has not taken its snapshot yet.
    h.emit(AACPEvent::OwnershipToFalseRequest);
    wait_until("the boundary", || {
        traced(mac).iter().any(|l| l.starts_with("BACKLOG drained"))
    })
    .await;
    h.emit(AACPEvent::OwnershipToFalseRequest);
    let remotes = || -> Vec<usize> {
        let trace = traced(mac);
        (0..trace.len())
            .filter(|&i| trace[i].starts_with("REMOTE requested ownership"))
            .collect()
    };
    wait_until("the post-snapshot request", || remotes().len() == 2).await;
    let trace = traced(mac);
    let boundary = position(&trace, "BACKLOG drained");
    let remote = remotes();
    assert!(remote[0] < boundary && boundary < remote[1], "{trace:#?}");
    assert_eq!(
        trace
            .iter()
            .filter(|l| l.starts_with("BACKLOG drained"))
            .count(),
        1,
        "{trace:#?}"
    );

    stop_handoff_task(mac);
}

/// #21: a handoff session through the default path, its lines appended
/// by the handoff log's LineSink, never puts the proximity keys the
/// session carried in the log.
#[tokio::test]
async fn handoff_log_never_contains_proximity_keys() {
    const IRK: [u8; 16] = [
        0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0x07, 0x18, 0x29, 0x3a, 0x4b, 0x5c, 0x6d, 0x7e, 0x8f,
        0x90,
    ];
    const ENC: [u8; 16] = [
        0x0f, 0x1e, 0x2d, 0x3c, 0x4b, 0x5a, 0x69, 0x78, 0x87, 0x96, 0xa5, 0xb4, 0xc3, 0xd2, 0xe1,
        0xf0,
    ];
    // Standard base64 of IRK and ENC, without the padding.
    const BASE64: [&str; 2] = ["obLD1OX2BxgpOktcbX6PkA", "Dx4tPEtaaXiHlqW0w9Lh8A"];
    let mac = "5A:00:00:00:00:21";
    let dir =
        std::env::temp_dir().join(format!("librepods-handoff-log-keys-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.join("librepods/handoff.jsonl");
    let log = Arc::new(handoff_log::Log::open(&path, handoff_log::Source::App).unwrap());
    let logged = || std::fs::read_to_string(&path).unwrap();

    let (manager, _rx, _tx) = ready_session().await;
    let media = Arc::new(FakeMedia {
        records: Arc::new(Records::default()),
        mac: mac.to_string(),
    });
    let media_controller = Arc::new(Mutex::new(MediaController::new(
        mac.to_string(),
        LOCAL.to_string(),
    )));
    let (events, events_rx) = mpsc::unbounded_channel();
    manager.set_event_channel(events.clone()).await;
    let (play_tx, play_rx) = mpsc::channel(8);
    let (_call_tx, call_rx) = mpsc::channel(8);
    let (command_tx, _command_rx) = mpsc::unbounded_channel();
    Switching::start_with_calls(
        SwitchingSetup {
            mode: SwitchingMode::Handoff,
            airpods_mac: mac.to_string(),
            local_mac: LOCAL.to_string(),
            manager: manager.clone(),
            media_controller,
            command_tx,
        },
        media,
        move || PlaybackSamples {
            rx: play_rx,
            rebaseline: Default::default(),
            players: Default::default(),
            streams: watch::channel(Default::default()).1,
        },
        move |_: &str| call_rx,
    )
    .await
    .with_line_sink(log.sink(mac.to_string()))
    .dispatch_events(events_rx, Arc::new(DeviceHandler::default()).handle());
    // Before the first await, so both are in the init backlog and the
    // core steps the [0] before any playback sample (#381).
    events
        .send(AACPEvent::ProximityKeys(vec![
            (0x01, IRK.to_vec()),
            (0x04, ENC.to_vec()),
        ]))
        .unwrap();
    events.send(owns(0)).unwrap();
    wait_until("the automatic start", || logged().contains("AUTO enabled")).await;

    play_tx
        .send((Instant::now(), AUDIO_CATEGORY_IDLE, None))
        .await
        .unwrap();
    play_tx
        .send((Instant::now(), AUDIO_CATEGORY_MEDIA, None))
        .await
        .unwrap();
    wait_until("the take", || {
        logged().contains("requested Linux ownership")
    })
    .await;
    events.send(owns(1)).unwrap();
    events
        .send(AACPEvent::ConnectedDevices(
            vec![host(LOCAL), host(REMOTE)],
            vec![host(LOCAL), host(REMOTE), host(SECOND_REMOTE)],
        ))
        .unwrap();
    events.send(AACPEvent::OwnershipToFalseRequest).unwrap();
    wait_until("the yield", || {
        logged().contains("REMOTE requested ownership; yielding")
    })
    .await;
    stop_handoff_task(mac);

    let text = logged();
    assert!(text.contains("ACK owner=linux"), "{text}");
    for key in [IRK, ENC] {
        for encoded in [hex::encode(key), hex::encode_upper(key)] {
            assert!(!text.contains(&encoded), "{encoded} in {text}");
        }
    }
    for encoded in BASE64 {
        assert!(!text.contains(encoded), "{encoded} in {text}");
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn handoff_remote_request_yields_once() {
    let mac = "5A:00:00:00:00:02";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    expect_startup(&h, &mut rx).await;
    expect_take(&h, &mut rx).await;
    h.emit(owns(1));

    // Another host asks for the AirPods: the core pauses and yields once;
    // no legacy reaction adds a second [0] or a deactivate.
    h.emit(AACPEvent::OwnershipToFalseRequest);
    assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 1);
    // The yield's [0] ACK is not a second yield (#39).
    h.emit(owns(0));
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 1);
    let trace = traced(mac);
    assert!(!trace.iter().any(|l| l.starts_with("YIELD")), "{trace:#?}");

    // A new remote host is announced once (Announce covers every
    // connected remote host, as the probe's `announce` does).
    let hosts = vec![host(LOCAL), host(REMOTE), host(SECOND_REMOTE)];
    manager.state.lock().await.connected_devices = hosts.clone();
    h.emit(AACPEvent::ConnectedDevices(
        vec![host(LOCAL), host(REMOTE)],
        hosts,
    ));
    let mut announced = Vec::new();
    for _ in 0..4 {
        announced.push(next_packet(&mut rx).await);
    }
    assert_eq!(
        announced,
        [
            expected_new_device(REMOTE).await,
            expected_tipi(REMOTE).await,
            expected_new_device(SECOND_REMOTE).await,
            expected_tipi(SECOND_REMOTE).await,
        ]
    );
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 1);

    stop_handoff_task(mac);
}

#[tokio::test]
async fn handoff_local_call_refuses_remote_and_advertises() {
    let mac = "5A:00:00:00:00:0C";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    expect_startup(&h, &mut rx).await;
    expect_take(&h, &mut rx).await;
    h.emit(owns(1));
    wait_until("the take's ACK", || {
        traced(mac).iter().any(|l| l.starts_with("ACK owner=linux"))
    })
    .await;

    // A call starts on Linux: one in-call media information packet per
    // remote host, and nothing else.
    h.call(true).await;
    let advertised = next_packet(&mut rx).await;
    assert_eq!(advertised, expected_call(REMOTE).await);
    assert_eq!(
        advertised[4..],
        encode_media_information_call(LOCAL, REMOTE).expect("reference")[..]
    );
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    // The same state again changes nothing.
    h.call(true).await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());

    // An iPhone starts media and asks for the AirPods: refused, with no
    // OwnsConnection [0] and no pause.
    h.emit(AACPEvent::AudioSource(AudioSource {
        mac: REMOTE.to_string(),
        r#type: AudioSourceType::Media,
    }));
    h.emit(AACPEvent::OwnershipToFalseRequest);
    wait_until("the refusal", || {
        traced(mac)
            .iter()
            .any(|l| l == "REMOTE requested ownership; refused during a local call")
    })
    .await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 0);

    // A host that joins during the call is announced and told of it.
    let hosts = vec![host(LOCAL), host(REMOTE), host(SECOND_REMOTE)];
    manager.state.lock().await.connected_devices = hosts.clone();
    h.emit(AACPEvent::ConnectedDevices(
        vec![host(LOCAL), host(REMOTE)],
        hosts,
    ));
    let mut announced = Vec::new();
    for _ in 0..6 {
        announced.push(next_packet(&mut rx).await);
    }
    assert_eq!(
        announced,
        [
            expected_new_device(REMOTE).await,
            expected_tipi(REMOTE).await,
            expected_new_device(SECOND_REMOTE).await,
            expected_tipi(SECOND_REMOTE).await,
            expected_call(REMOTE).await,
            expected_call(SECOND_REMOTE).await,
        ]
    );
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());

    // The call ends with playback still running: the idle media
    // information per remote host, no release.
    h.call(false).await;
    assert_eq!(
        next_packet(&mut rx).await,
        expected_new_device(REMOTE).await
    );
    assert_eq!(
        next_packet(&mut rx).await,
        expected_new_device(SECOND_REMOTE).await
    );
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 0);

    // Normal policy again: the next request yields once.
    h.emit(AACPEvent::OwnershipToFalseRequest);
    assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 1);
    let trace = traced(mac);
    assert!(
        position(&trace, "CALL ended on Linux")
            < position(&trace, "REMOTE requested ownership; yielding")
    );

    stop_handoff_task(mac);
}

/// The reconnect-for-playback take's packets: the announcement, then
/// OwnsConnection [1], with no "not owning" first.
async fn expect_reconnect_take(mac: &str, rx: &mut mpsc::Receiver<Vec<u8>>) {
    assert_eq!(next_packet(rx).await, expected_new_device(REMOTE).await);
    assert_eq!(next_packet(rx).await, expected_tipi(REMOTE).await);
    assert_eq!(next_packet(rx).await, expected_owns(1).await);
    wait_until("the reconnect take", || {
        traced(mac)
            .iter()
            .any(|l| l == "AUTO reconnected for new playback; requested Linux ownership")
    })
    .await;
}

#[tokio::test]
async fn reconnect_for_playback_start_takes() {
    let mac = "5A:00:00:00:07:07";
    let (manager, mut rx, _tx) = ready_session().await;
    reconnect::set_marker(mac, AUDIO_CATEGORY_MEDIA);
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    h.play(true).await;
    expect_reconnect_take(mac, &mut rx).await;
    // Acknowledged; the playback goes on: nothing more is sent.
    h.emit_queued(owns(1)).await;
    h.play(true).await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 0);
    // The marker was used once.
    assert_eq!(reconnect::take_marker(mac), None);
    stop_handoff_task(mac);
}

#[tokio::test]
async fn normal_start_keeps_running_playback_as_baseline() {
    let mac = "5A:00:00:00:07:08";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    h.play(true).await;
    expect_startup(&h, &mut rx).await;
    h.emit_queued(owns(0)).await;
    h.play(true).await;
    h.play(true).await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert!(
        !traced(mac)
            .iter()
            .any(|l| l.starts_with("AUTO reconnected"))
    );
    stop_handoff_task(mac);
}

#[tokio::test]
async fn reconnect_marker_expires() {
    let mac = "5A:00:00:00:07:09";
    tokio::time::pause();
    reconnect::set_marker(mac, AUDIO_CATEGORY_MEDIA);
    tokio::time::advance(reconnect::RECONNECT_TAKE_WINDOW + Duration::from_secs(1)).await;
    tokio::time::resume();
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    h.play(true).await;
    expect_startup(&h, &mut rx).await;
    assert!(
        !traced(mac)
            .iter()
            .any(|l| l.starts_with("AUTO reconnected"))
    );
    stop_handoff_task(mac);
}

#[tokio::test(start_paused = true)]
async fn reconnect_start_waits_for_routing_ready() {
    const NOT_READY: &str = "RECONNECT host state not ready in 5 s; starting without a take";
    // Reports arriving after the task starts, within 5 s, still take.
    let mac = "5A:00:00:00:07:0A";
    let (manager, mut rx, _tx) = injected_manager().await;
    reconnect::set_marker(mac, AUDIO_CATEGORY_MEDIA);
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(rx.try_recv().is_err());
    {
        let mut state = manager.state.lock().await;
        state.connected_devices = vec![host(LOCAL), host(REMOTE)];
        state.ear_detection_status = vec![EarDetectionStatus::InEar; 2];
        state.audio_source = Some(AudioSource {
            mac: "00:00:00:00:00:00".to_string(),
            r#type: AudioSourceType::None,
        });
    }
    h.play(true).await;
    expect_reconnect_take(mac, &mut rx).await;
    assert!(!traced(mac).iter().any(|l| l == NOT_READY));
    stop_handoff_task(mac);

    // None within 5 s: one line, then the normal start.
    let mac = "5A:00:00:00:07:0B";
    let (manager, mut rx, _tx) = injected_manager().await;
    reconnect::set_marker(mac, AUDIO_CATEGORY_MEDIA);
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    tokio::time::sleep(reconnect::ROUTING_READY_WAIT).await;
    wait_until("the normal start", || {
        traced(mac).iter().any(|l| l.starts_with("AUTO enabled"))
    })
    .await;
    // With no routing reports its decision waits (#452): nothing is sent.
    // Auto's release at the deadline isn't asserted here: the core runs
    // on std `Instant` and this test on paused tokio time. The core's
    // `assert_no_connect_take` with `waits` (handoff.rs) covers it.
    h.play(true).await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    let trace = traced(mac);
    assert_eq!(trace.iter().filter(|l| *l == NOT_READY).count(), 1);
    assert!(!trace.iter().any(|l| l.starts_with("AUTO reconnected")));
    stop_handoff_task(mac);
}

/// Connects as BlueZ does: its Connected=true starts the handoff task
/// before `bluetoothctl connect` returns (#307).
struct StartsTask {
    manager: AACPManager,
    harness: Arc<StdMutex<Option<Harness>>>,
}

impl reconnect::Connector for StartsTask {
    async fn connected(&self, _: &str) -> bool {
        false
    }

    async fn connect(&self, mac: &str) -> Result<(), String> {
        let h = Harness::start(SwitchingMode::Handoff, mac, &self.manager).await;
        wait_until("the take", || {
            traced(mac)
                .iter()
                .any(|l| l.starts_with("AUTO reconnected for new playback"))
        })
        .await;
        *self.harness.lock().unwrap() = Some(h);
        Ok(())
    }

    fn settings(&self, _: &str) -> reconnect::Settings {
        handoff_settings()
    }

    fn log(&self, _: &str, _: &str) {}
}

fn handoff_settings() -> reconnect::Settings {
    reconnect::Settings {
        mode: SwitchingMode::Handoff,
        auto_connect: true,
        connect_mode: ConnectMode::Automatically,
        last_owner_linux: false,
    }
}

/// Connects at once; the test starts the handoff task itself.
struct Connects;

impl reconnect::Connector for Connects {
    async fn connected(&self, _: &str) -> bool {
        false
    }

    async fn connect(&self, _: &str) -> Result<(), String> {
        Ok(())
    }

    fn settings(&self, _: &str) -> reconnect::Settings {
        handoff_settings()
    }

    fn log(&self, _: &str, _: &str) {}
}

#[tokio::test]
async fn reconnect_take_before_connect_returns() {
    let mac = "5A:00:00:00:07:0E";
    let (manager, mut rx, _tx) = ready_session().await;
    reconnect::disconnect_reason(mac, Some("org.bluez.Reason.Remote"));
    let harness = Arc::new(StdMutex::new(None));
    let (play_tx, samples) = Harness::play_channel();
    let watcher = tokio::spawn(reconnect::watch(
        mac.to_string(),
        samples(),
        StartsTask {
            manager: manager.clone(),
            harness: harness.clone(),
        },
    ));
    for category in [AUDIO_CATEGORY_IDLE, AUDIO_CATEGORY_MEDIA] {
        play_tx
            .send((Instant::now(), category, None))
            .await
            .unwrap();
    }
    expect_reconnect_take(mac, &mut rx).await;
    wait_until("the connect to return", || {
        harness.lock().unwrap().is_some()
    })
    .await;
    // The task used it; the connect's return doesn't set it again.
    assert_eq!(reconnect::take_marker(mac), None);
    watcher.abort();
    stop_handoff_task(mac);
}

#[tokio::test]
async fn reconnect_for_playback_takes_over_remote_media() {
    let mac = "5A:00:00:00:07:0F";
    let (manager, mut rx, _tx) = ready_session().await;
    manager.state.lock().await.audio_source = Some(AudioSource {
        mac: REMOTE.to_string(),
        r#type: AudioSourceType::Media,
    });
    // Linux's media at 301 is not below the remote media's 301.
    reconnect::set_marker(mac, AUDIO_CATEGORY_MEDIA);
    let _h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    expect_reconnect_take(mac, &mut rx).await;
    stop_handoff_task(mac);
}

#[tokio::test]
async fn reconnect_for_new_source_takes_at_its_request() {
    let mac = "5A:00:00:00:04:22";
    let (manager, mut rx, _tx) = ready_session().await;
    manager.state.lock().await.audio_source = Some(AudioSource {
        mac: REMOTE.to_string(),
        r#type: AudioSourceType::Media,
    });
    // Linux plays media, but the reconnect's new source asked at 201.
    reconnect::set_requested_marker(mac, AUDIO_CATEGORY_MEDIA, AUDIO_CATEGORY_STREAM);
    let _h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    wait_until("the blocked take", || {
        traced(mac)
            .iter()
            .any(|l| l.starts_with("AUTO reconnected; takeover blocked"))
    })
    .await;
    let owns_one = expected_owns(1).await;
    assert!(!rest(&mut rx).await.contains(&owns_one));
    assert!(
        !traced(mac)
            .iter()
            .any(|l| l == "AUTO reconnected for new playback; requested Linux ownership")
    );
    assert_eq!(reconnect::take_marker(mac), None);
    stop_handoff_task(mac);
}

#[tokio::test(start_paused = true)]
async fn reconnect_stop_during_routing_wait_starts_normally() {
    let mac = "5A:00:00:00:07:10";
    let (manager, mut rx, _tx) = injected_manager().await;
    reconnect::set_marker(mac, AUDIO_CATEGORY_MEDIA);
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    // The watcher saw the playback stop.
    assert!(reconnect::playback_stopped(mac));
    tokio::time::sleep(Duration::from_secs(2)).await;
    {
        let mut state = manager.state.lock().await;
        state.connected_devices = vec![host(LOCAL), host(REMOTE)];
        state.ear_detection_status = vec![EarDetectionStatus::InEar; 2];
        state.audio_source = Some(AudioSource {
            mac: "00:00:00:00:00:00".to_string(),
            r#type: AudioSourceType::None,
        });
    }
    h.play(true).await;
    // The normal start: the idle other host gets the connect take
    // (#324), not the reconnect's playback take.
    expect_connect_take(&h, &mut rx).await;
    assert!(
        !traced(mac)
            .iter()
            .any(|l| l.starts_with("AUTO reconnected"))
    );
    stop_handoff_task(mac);
}

/// A claimed marker outlives RECONNECT_TAKE_WINDOW only while the
/// watcher still watches for its stop (#307 round 3).
#[tokio::test(start_paused = true)]
async fn reconnect_stop_during_routing_wait_after_expiry_does_not_take() {
    let mac = "5A:00:00:00:07:13";
    let (manager, mut rx, _tx) = injected_manager().await;
    reconnect::disconnect_reason(mac, Some("org.bluez.Reason.Remote"));
    let (play_tx, samples) = Harness::play_channel();
    let watcher = tokio::spawn(reconnect::watch(mac.to_string(), samples(), Connects));
    let sample = |category| play_tx.send((Instant::now(), category, None));
    // Connected for new playback at t=0.
    sample(AUDIO_CATEGORY_IDLE).await.unwrap();
    sample(AUDIO_CATEGORY_MEDIA).await.unwrap();
    tokio::time::sleep(Duration::from_secs(12)).await;
    // The task starts at t=12 and waits for its routing.
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    // Still playing at t=15.5, past the window; stopped at t=16.
    tokio::time::sleep(Duration::from_millis(3500)).await;
    sample(AUDIO_CATEGORY_MEDIA).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    sample(AUDIO_CATEGORY_IDLE).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(watcher.is_finished());
    // Routing at t=16.5, inside the task's 5 s wait.
    {
        let mut state = manager.state.lock().await;
        state.connected_devices = vec![host(LOCAL), host(REMOTE)];
        state.ear_detection_status = vec![EarDetectionStatus::InEar; 2];
        state.audio_source = Some(AudioSource {
            mac: "00:00:00:00:00:00".to_string(),
            r#type: AudioSourceType::None,
        });
    }
    wait_until("the normal start", || {
        traced(mac).iter().any(|l| l.starts_with("AUTO enabled"))
    })
    .await;
    // The idle other host gets the connect take (#324), not the
    // reconnect's playback take.
    expect_connect_take(&h, &mut rx).await;
    assert!(
        !traced(mac)
            .iter()
            .any(|l| l.starts_with("AUTO reconnected"))
    );
    stop_handoff_task(mac);
}

#[tokio::test]
async fn reconnect_take_ignores_stale_backlog_ownership() {
    let mac = "5A:00:00:00:07:11";
    let (manager, mut rx, _tx) = ready_session().await;
    reconnect::set_marker(mac, AUDIO_CATEGORY_MEDIA);
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    // Before the first await: in the init backlog.
    h.emit(owns(1));
    expect_reconnect_take(mac, &mut rx).await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    let acked = || traced(mac).iter().any(|l| l.starts_with("ACK owner=linux"));
    assert!(!acked());
    // A fresh [1] acknowledges it.
    h.emit_queued(owns(1)).await;
    wait_until("the ACK", acked).await;
    stop_handoff_task(mac);
}

/// The marker's age is judged once, at task start (#307 round 2).
#[tokio::test(start_paused = true)]
async fn reconnect_marker_fresh_at_start_survives_routing_wait() {
    let mac = "5A:00:00:00:07:12";
    let (manager, mut rx, _tx) = injected_manager().await;
    reconnect::set_marker(mac, AUDIO_CATEGORY_MEDIA);
    tokio::time::advance(Duration::from_secs(12)).await;
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    // Routing at +4 s: past RECONNECT_TAKE_WINDOW since the connect.
    tokio::time::sleep(Duration::from_secs(4)).await;
    {
        let mut state = manager.state.lock().await;
        state.connected_devices = vec![host(LOCAL), host(REMOTE)];
        state.ear_detection_status = vec![EarDetectionStatus::InEar; 2];
        state.audio_source = Some(AudioSource {
            mac: "00:00:00:00:00:00".to_string(),
            r#type: AudioSourceType::None,
        });
    }
    h.play(true).await;
    expect_reconnect_take(mac, &mut rx).await;
    stop_handoff_task(mac);
}

#[tokio::test]
async fn handoff_not_stalled_by_slow_device_handler() {
    let mac = "5A:00:00:00:00:08";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = Harness::start_with(SwitchingMode::Handoff, mac, &manager, true).await;
    expect_startup(&h, &mut rx).await;
    expect_take(&h, &mut rx).await;

    // The device handler blocks on a wear report, as ear detection does
    // behind an A2DP activation (up to ~43 s).
    let worn = wear(EarDetectionStatus::InEar);
    h.emit(worn.clone());
    wait_until("the handler to block", || {
        h.handler.started() == named(&[idle_source(), owns(0), worn.clone()])
    })
    .await;
    // Queued behind it: the take's ACK, then another host's request.
    h.emit(owns(1));
    h.emit(AACPEvent::OwnershipToFalseRequest);

    // The core still yields once and pauses once.
    assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 1);
    // And it saw the ownership report queued behind the blocked event
    // before the request.
    let trace = traced(mac);
    assert!(
        position(&trace, "ACK owner=linux")
            < position(&trace, "REMOTE requested ownership; yielding")
    );
    // The handler is still blocked on the wear report.
    assert_eq!(
        h.handler.started(),
        named(&[idle_source(), owns(0), worn.clone()])
    );
    assert_eq!(h.handler.finished(), named(&[idle_source(), owns(0)]));

    // Released, it handles every event once, in order.
    h.handler.release.notify_one();
    let all = named(&[
        idle_source(),
        owns(0),
        worn,
        owns(1),
        AACPEvent::OwnershipToFalseRequest,
    ]);
    wait_until("the handler to catch up", || h.handler.finished() == all).await;
    assert_eq!(h.handler.started(), all);
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 1);

    stop_handoff_task(mac);
}

#[tokio::test]
async fn router_flush_forwards_every_queued_event() {
    // The handoff task's flush, made once it finds the session dead,
    // returns only after everything already in the primary queue has
    // been forwarded to it.
    let (primary_tx, primary) = mpsc::unbounded_channel();
    let (switching_tx, mut switching) = mpsc::unbounded_channel();
    let (handlers_tx, mut handlers) = mpsc::unbounded_channel();
    let (flush_tx, flush) = mpsc::unbounded_channel();
    let queued = [owns(1), wear(EarDetectionStatus::InCase), owns(0)];
    for event in &queued {
        primary_tx.send(event.clone()).unwrap();
    }
    let (done, flushed) = oneshot::channel();
    flush_tx.send(done).unwrap();
    let router = tokio::spawn(route_events(
        primary,
        Some(switching_tx),
        handlers_tx,
        Some(flush),
    ));
    flushed.await.expect("flush answered");
    let mut forwarded = Vec::new();
    while let Ok(event) = switching.try_recv() {
        forwarded.push(event);
    }
    assert_eq!(named(&forwarded), named(&queued));

    // The handlers get the same events, in order.
    let mut handled = Vec::new();
    while let Ok(event) = handlers.try_recv() {
        handled.push(event);
    }
    assert_eq!(named(&handled), named(&queued));

    // Once the primary queue closes the router ends, and a flush (the
    // shell's `Shell::flush`) is not left waiting.
    drop(primary_tx);
    tokio::time::timeout(Duration::from_secs(3), router)
        .await
        .expect("router ended")
        .unwrap();
    let (done, flushed) = oneshot::channel();
    assert!(flush_tx.send(done).is_err());
    assert!(flushed.await.is_err());
}

#[tokio::test]
async fn off_never_sends_ownership_or_hijack() {
    let mac = "5A:00:00:00:00:06";
    let (manager, mut rx, _tx) = ready_session().await;
    let h = Harness::start(SwitchingMode::Off, mac, &manager).await;

    h.play(false).await;
    h.play(true).await;
    h.emit(owns(1));
    h.play(false).await;
    h.emit(owns(0));
    h.emit(AACPEvent::OwnershipToFalseRequest);
    // An OwnsConnection [0] report also reaches control command
    // subscribers (the legacy pause/deactivate reaction).
    manager
        .receive_packet(&[4, 0, 4, 0, 9, 0, 6, 0, 0, 0, 0])
        .await;
    let hosts = vec![host(LOCAL), host(REMOTE), host(SECOND_REMOTE)];
    manager.state.lock().await.connected_devices = hosts.clone();
    h.emit(AACPEvent::ConnectedDevices(
        vec![host(LOCAL), host(REMOTE)],
        hosts,
    ));

    tokio::time::sleep(SETTLE).await;
    assert!(rx.try_recv().is_err());
    assert_eq!(h.pauses(), 0);
    assert_eq!(h.activations(), 0);
    assert!(!h.playback_started.load(Ordering::SeqCst));
    assert!(handoff_task(mac).is_none());
    assert!(!media_controller::playback_listener_registered(mac));
    // The device handlers still get every event.
    assert_eq!(h.handler.finished().len(), 5);
}

#[tokio::test]
async fn switching_legacy_starts_playback_listener() {
    let mac = "5A:00:00:00:00:07";
    let (manager, _rx, _tx) = ready_session().await;

    // A handoff task left by a previous device for this MAC stops.
    let _previous = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    let previous_task = handoff_task(mac).expect("handoff task");

    let h = Harness::start(SwitchingMode::Legacy, mac, &manager).await;
    assert!(media_controller::playback_listener_registered(mac));
    assert!(handoff_task(mac).is_none());
    assert!(!h.playback_started.load(Ordering::SeqCst));
    wait_until("the handoff task to stop", || previous_task.is_finished()).await;

    // And a handoff device replacing a legacy one stops its listener.
    let _next = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    assert!(!media_controller::playback_listener_registered(mac));
    assert!(handoff_task(mac).is_some());
    stop_handoff_task(mac);
}

#[tokio::test]
async fn switching_legacy_reactions_get_dispatched_events() {
    // The legacy ConnectedDevices reaction runs on events from the
    // dispatch: it announces the new remote host.
    let mac = "5A:00:00:00:00:0A";
    let (manager, mut rx, _tx) = ready_session().await;
    let h = Harness::start(SwitchingMode::Legacy, mac, &manager).await;
    h.emit(AACPEvent::ConnectedDevices(
        vec![host(LOCAL), host(REMOTE)],
        vec![host(LOCAL), host(REMOTE), host(SECOND_REMOTE)],
    ));
    let announce = [
        expected_new_device(SECOND_REMOTE).await,
        expected_tipi(SECOND_REMOTE).await,
    ];
    let mut sent = Vec::new();
    while sent.len() < 2 || !sent.ends_with(&announce) {
        sent.push(next_packet(&mut rx).await);
    }
    wait_until("the handler", || h.handler.finished().len() == 1).await;
    media_controller::stop_playback_listener(mac);
}

/// Playback, a take's ACK and a loss, in a mode with no handoff core.
async fn take_and_loss_without_core(h: &Harness) {
    h.play(false).await;
    h.play(true).await;
    h.emit(owns(1));
    h.emit(owns(0));
    wait_until("the handler", || h.handler.finished().len() == 2).await;
    tokio::time::sleep(SETTLE).await;
}

#[tokio::test]
async fn off_never_routes_default_sink() {
    let mac = "5A:00:00:00:00:29";
    let (manager, _rx, _tx) = ready_session().await;
    let h = Harness::start(SwitchingMode::Off, mac, &manager).await;
    take_and_loss_without_core(&h).await;
    assert_eq!(h.routes(), []);
}

#[tokio::test]
async fn legacy_never_routes_default_sink() {
    let mac = "5A:00:00:00:00:2A";
    let (manager, _rx, _tx) = ready_session().await;
    let h = Harness::start(SwitchingMode::Legacy, mac, &manager).await;
    take_and_loss_without_core(&h).await;
    assert_eq!(h.routes(), []);
    media_controller::stop_playback_listener(mac);
}

/// `LiveStreams` whose first listing waits until `gate` is released.
struct GatedFirst {
    streams: LiveStreams,
    gate: Option<std::sync::mpsc::Receiver<()>>,
}

impl PlaybackSource for GatedFirst {
    fn playback_streams(&mut self) -> io::Result<Vec<StreamProps>> {
        if let Some(gate) = self.gate.take() {
            let _ = gate.recv();
        }
        self.streams.playback_streams()
    }
}

/// The daemon's startup `Input::Auto` resets the core's baseline with
/// the real playback poller running, and `music(21)` starts before the
/// reset (`before_reset`) or during Auto's SendOwnership(false), which
/// the shell awaits before DrainPlayback and the test holds with a full
/// send channel: past the stream's remaining debounce (before the reset)
/// or briefly. The first MPRIS poll waits for the reset, so the
/// poller's pending sample is stamped after it. Returns the harness, the
/// packets after Auto's and the reply to its "not owning", and when the
/// stream started. No audio source at the start (`no_source_session`),
/// so no connect take (#324); its idle report follows Auto's packets.
async fn step_rebaseline(
    mac: &str,
    before_reset: bool,
) -> (Harness, mpsc::Receiver<Vec<u8>>, Instant) {
    let (manager, mut rx, tx) = no_source_session().await;
    let mut queued = 0;
    while tx.try_send(Vec::new()).is_ok() {
        queued += 1;
    }
    let (open_mpris, mpris_gate) = std::sync::mpsc::channel::<()>();
    let mpris_gate = StdMutex::new(Some(mpris_gate));
    let mpris = move || {
        if let Some(gate) = mpris_gate.lock().unwrap().take() {
            let _ = gate.recv();
        }
        Vec::new()
    };
    let streams = LiveStreams::default();
    let h = Harness::start_streams(mac, &manager, mpris, streams.clone(), STREAM_TEST_MIN).await;
    // The handoff task has not run yet (a current-thread runtime): it
    // reads the session first, so Auto waits for this guard.
    let session = manager.state.try_lock().expect("session state free");
    streams.settle().await;
    let start = || {
        let started = Instant::now();
        streams.set(vec![music(21)]);
        started
    };
    let started = before_reset.then(start);
    streams.settle().await;
    drop(session);
    // Auto's ConnectA2dp: the core has reset, and SendOwnership(false)
    // is held.
    wait_until("Auto", || h.activations() == 1).await;
    drop(open_mpris);
    let started = started.unwrap_or_else(start);
    streams.settle().await;
    let held = if before_reset {
        STREAM_TEST_MIN + STREAM_TEST_MIN / 5
    } else {
        STREAM_TEST_MIN / 5
    };
    tokio::time::sleep(held).await;
    for _ in 0..queued {
        assert_eq!(next_packet(&mut rx).await, Vec::<u8>::new());
    }
    assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
    assert_eq!(
        next_packet(&mut rx).await,
        expected_new_device(REMOTE).await
    );
    assert_eq!(next_packet(&mut rx).await, expected_tipi(REMOTE).await);
    h.emit_queued(idle_source()).await;
    h.emit(owns(0));
    (h, rx, started)
}

#[tokio::test]
async fn handoff_step_rebaseline_stream_before_reset_never_takes() {
    let mac = "5A:00:00:00:00:14";
    let (h, mut rx, _) = step_rebaseline(mac, true).await;
    tokio::time::sleep(STREAM_TEST_MIN * 2).await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 0);
    stop_handoff_task(mac);
}

#[tokio::test]
async fn handoff_step_rebaseline_stream_after_reset_takes_once() {
    let mac = "5A:00:00:00:00:15";
    let (h, mut rx, started) = step_rebaseline(mac, false).await;
    assert_eq!(next_packet(&mut rx).await, expected_owns(1).await);
    assert!(started.elapsed() >= STREAM_TEST_MIN);
    h.emit(owns(1));
    tokio::time::sleep(STREAM_TEST_MIN).await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 0);
    stop_handoff_task(mac);
}

#[tokio::test]
async fn handoff_stream_playback_takes() {
    let mac = "5A:00:00:00:00:0D";
    let (manager, mut rx, _tx) = no_source_session().await;
    let streams = LiveStreams::default();
    let h = Harness::start_streams(mac, &manager, Vec::new, streams.clone(), STREAM_TEST_MIN).await;
    assert!(h.playback_started.load(Ordering::SeqCst));
    expect_startup(&h, &mut rx).await;
    // The AirPods' own owner report, stepped before the stream starts.
    h.emit_queued(owns(0)).await;
    // The stream monitor's baseline has no stream.
    streams.settle().await;

    // A music stream with no MPRIS player: one take once it lasted the
    // minimum, OwnsConnection [1] only.
    let started = Instant::now();
    streams.set(vec![music(11)]);
    assert_eq!(next_packet(&mut rx).await, expected_owns(1).await);
    assert!(started.elapsed() >= STREAM_TEST_MIN);
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 0);

    // ACK, then the stream ends: Linux stays the idle owner (#262), so
    // no release packet.
    h.emit_queued(owns(1)).await;
    streams.set(Vec::new());
    wait_until("the stop", || {
        traced(mac)
            .iter()
            .any(|l| l == "AUTO playback stopped; keeping ownership")
    })
    .await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 0);

    stop_handoff_task(mac);
}

/// #351: the take's resume chain gets the players of the poller's MPRIS
/// sample before the take, without waiting for the read still running
/// at the ACK: every read from the take on takes 5 s.
#[tokio::test]
async fn take_resume_gets_the_sampled_players_during_a_slow_mpris_read() {
    let mac = "5A:00:00:00:03:51";
    let (manager, mut rx, _tx) = no_source_session().await;
    let (slow, running) = (Arc::new(AtomicBool::new(false)), Arc::default());
    // Dropped at the test's end, which ends a slow read at once.
    let (_release, gate) = std::sync::mpsc::channel::<()>();
    let gate = StdMutex::new(gate);
    let mpris = {
        let (slow, running): (_, Arc<AtomicBool>) = (slow.clone(), Arc::clone(&running));
        move || {
            if slow.load(Ordering::SeqCst) {
                running.store(true, Ordering::SeqCst);
                let _ = gate.lock().unwrap().recv_timeout(Duration::from_secs(5));
                running.store(false, Ordering::SeqCst);
            }
            named_player(true)
        }
    };
    let streams = LiveStreams::default();
    let h = Harness::start_streams(mac, &manager, mpris, streams.clone(), STREAM_TEST_MIN).await;
    expect_startup(&h, &mut rx).await;
    h.emit_queued(owns(0)).await;
    h.a2dp(false).await;
    streams.settle().await;
    // The player's own stream goes live: a take.
    streams.set(vec![music(51)]);
    assert_eq!(next_packet(&mut rx).await, expected_owns(1).await);
    slow.store(true, Ordering::SeqCst);
    wait_until("a slow MPRIS read", || running.load(Ordering::SeqCst)).await;

    h.emit(owns(1));
    let spawns = || h.records.resume_spawns.lock().unwrap().clone();
    wait_until("the resume chain's spawn", || !spawns().is_empty()).await;
    assert!(running.load(Ordering::SeqCst), "the slow read ended first");
    let resumed = h.records.resume_players.lock().unwrap().clone();
    assert_eq!(resumed, [named_player(true)]);
    // The take's route was enqueued before the spawn.
    let routes = h.sink_events();
    assert_eq!(routes.last(), Some(&SinkEvent::Take));
    assert_eq!(spawns(), [routes.len()]);
    stop_handoff_task(mac);
}

#[tokio::test]
async fn handoff_event_sound_stream_never_takes() {
    let mac = "5A:00:00:00:00:0E";
    let (manager, mut rx, _tx) = no_source_session().await;
    // An event sound open from the start, for longer than the minimum.
    let streams = LiveStreams::default();
    streams.set(vec![event_sound(21)]);
    let h = Harness::start_streams(mac, &manager, Vec::new, streams.clone(), STREAM_TEST_MIN).await;
    expect_startup(&h, &mut rx).await;
    streams.settle().await;
    tokio::time::sleep(STREAM_TEST_MIN).await;
    // A new music stream that ends well before the minimum.
    streams.set(vec![event_sound(21), music(22)]);
    tokio::time::sleep(STREAM_TEST_MIN / 5).await;
    streams.set(vec![event_sound(21)]);
    tokio::time::sleep(STREAM_TEST_MIN).await;

    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 0);
    assert_eq!(h.activations(), 1);
    stop_handoff_task(mac);
}

#[tokio::test]
async fn handoff_muted_stream_never_takes() {
    let mac = "5A:00:00:00:00:11";
    let (manager, mut rx, _tx) = no_source_session().await;
    let streams = LiveStreams::default();
    let h = Harness::start_streams(mac, &manager, Vec::new, streams.clone(), STREAM_TEST_MIN).await;
    expect_startup(&h, &mut rx).await;
    // The baseline has no stream.
    streams.settle().await;
    // A new, uncorked but muted music stream (a muted autoplaying tab),
    // for well over the minimum.
    streams.set(vec![StreamProps {
        muted: true,
        ..music(41)
    }]);
    tokio::time::sleep(STREAM_TEST_MIN * 2 + STREAM_TEST_MIN / 2).await;

    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 0);
    assert_eq!(h.activations(), 1);
    stop_handoff_task(mac);
}

/// A music stream already playing in the stream monitor's first listing,
/// optionally held until playback samples have reached the core: no
/// take while it keeps playing, one take once it is paused and resumed.
async fn preexisting_stream_case(mac: &str, delay_first_listing: bool) {
    let (manager, mut rx, _tx) = no_source_session().await;
    let streams = LiveStreams::default();
    streams.set(vec![music(31)]);
    let (release, gate) = std::sync::mpsc::channel::<()>();
    let source = GatedFirst {
        streams: streams.clone(),
        gate: delay_first_listing.then_some(gate),
    };
    let mpris_calls = Arc::new(AtomicUsize::new(0));
    let calls = mpris_calls.clone();
    let mpris = move || {
        calls.fetch_add(1, Ordering::SeqCst);
        Vec::new()
    };
    let h = Harness::start_streams(mac, &manager, mpris, source, STREAM_TEST_MIN).await;
    expect_startup(&h, &mut rx).await;
    if delay_first_listing {
        // The third MPRIS sample is taken only once the core received
        // the first (the channel holds one): the baseline sample came
        // before any stream listing.
        wait_until("MPRIS samples", || mpris_calls.load(Ordering::SeqCst) >= 3).await;
        assert_eq!(streams.listings.load(Ordering::SeqCst), 0);
        release.send(()).expect("first listing waiting");
    }
    streams.settle().await;

    // Playing for more than twice the minimum: no new start, no take.
    tokio::time::sleep(STREAM_TEST_MIN * 2 + STREAM_TEST_MIN / 2).await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 0);

    // Paused, then resumed: a new start, one take once it lasted the
    // minimum from the resume, OwnsConnection [1] only.
    streams.set(vec![StreamProps {
        corked: true,
        ..music(31)
    }]);
    streams.settle().await;
    let resumed = Instant::now();
    streams.set(vec![music(31)]);
    assert_eq!(next_packet(&mut rx).await, expected_owns(1).await);
    assert!(resumed.elapsed() >= STREAM_TEST_MIN);
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    assert_eq!(h.pauses(), 0);
    stop_handoff_task(mac);
}

#[tokio::test]
async fn handoff_preexisting_stream_playback_never_takes() {
    preexisting_stream_case("5A:00:00:00:00:0F", false).await;
}

#[tokio::test]
async fn handoff_preexisting_stream_late_first_listing_never_takes() {
    preexisting_stream_case("5A:00:00:00:00:10", true).await;
}

/// The owner plays media and relays 301: a counted stream (201) takes
/// nothing, an MPRIS player (301) takes once.
#[tokio::test]
async fn category_stream_against_relayed_media_waits_for_mpris() {
    let mac = "5A:00:00:00:02:72";
    let (manager, mut rx, _tx) = no_source_session().await;
    let mpris = Arc::new(AtomicBool::new(false));
    let reads = mpris.clone();
    let streams = LiveStreams::default();
    let h = Harness::start_streams(
        mac,
        &manager,
        move || named_player(reads.load(Ordering::SeqCst)),
        streams.clone(),
        STREAM_TEST_MIN,
    )
    .await;
    expect_startup(&h, &mut rx).await;
    h.emit_queued(owns(0)).await;
    h.emit_queued(AACPEvent::AudioSource(AudioSource {
        mac: REMOTE.to_string(),
        r#type: AudioSourceType::Media,
    }))
    .await;
    h.emit_queued(AACPEvent::MediaInformation {
        host: REMOTE.to_string(),
        category: AUDIO_CATEGORY_MEDIA,
    })
    .await;
    streams.settle().await;

    streams.set(vec![music(71)]);
    wait_until("the category skip", || {
        traced(mac)
            .iter()
            .any(|l| l.starts_with("AUTO takeover skipped: the audio source's category 301"))
    })
    .await;
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());

    mpris.store(true, Ordering::SeqCst);
    assert_eq!(next_packet(&mut rx).await, expected_owns(1).await);
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    stop_handoff_task(mac);
}

/// The owner's media report is stale: its inbound relay of 100 reaches
/// the core, so a counted stream (201) takes once.
#[tokio::test]
async fn category_stream_against_relayed_idle_takes() {
    let mac = "5A:00:00:00:02:73";
    let (manager, mut rx, _tx) = no_source_session().await;
    let streams = LiveStreams::default();
    let h = Harness::start_streams(mac, &manager, Vec::new, streams.clone(), STREAM_TEST_MIN).await;
    expect_startup(&h, &mut rx).await;
    h.emit_queued(owns(0)).await;
    h.emit_queued(AACPEvent::AudioSource(AudioSource {
        mac: REMOTE.to_string(),
        r#type: AudioSourceType::Media,
    }))
    .await;
    h.emit_queued(AACPEvent::MediaInformation {
        host: REMOTE.to_string(),
        category: AUDIO_CATEGORY_IDLE,
    })
    .await;
    streams.settle().await;

    streams.set(vec![music(73)]);
    assert_eq!(next_packet(&mut rx).await, expected_owns(1).await);
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    stop_handoff_task(mac);
}

/// #88 on the park sink: a worthy stream with no MPRIS player, which
/// follows the default to the park sink, passes the peak gate and gets
/// its 201 take request after the minimum. The fake `StreamProps` carry
/// no sink: production `peak_stream` measures the monitor of whatever
/// sink the stream plays to, and mo's Wine game check covers the real
/// null sink.
#[tokio::test]
async fn park_peak_stream_on_the_park_sink_takes() {
    let mac = "5A:00:00:00:11:04";
    let (manager, mut rx, _tx) = no_source_session().await;
    let streams = LiveStreams::default();
    let h = Harness::start_streams(mac, &manager, Vec::new, streams.clone(), STREAM_TEST_MIN).await;
    h.records.parks.store(true, Ordering::SeqCst);
    expect_startup(&h, &mut rx).await;
    streams.settle().await;
    park_after_remote_request(&h, &mut rx).await;

    let started = Instant::now();
    streams.set(vec![music(51)]);
    assert_eq!(next_packet(&mut rx).await, expected_owns(1).await);
    assert!(started.elapsed() >= STREAM_TEST_MIN);
    // Still parked while the take waits for its ACK.
    assert_eq!(
        h.sink_events().last(),
        Some(&SinkEvent::Park),
        "{:?}",
        h.sink_events()
    );
    stop_handoff_task(mac);
}

/// A non-MPRIS stream playing since the baseline never takes (#90), so
/// it keeps the hold from parking; once it corks, the park follows.
#[tokio::test]
async fn park_baseline_stream_holds_off_the_park_until_it_corks() {
    let mac = "5A:00:00:00:11:07";
    let (manager, mut rx, _tx) = no_source_session().await;
    let streams = LiveStreams::default();
    streams.set(vec![music(52)]);
    let h = Harness::start_streams(mac, &manager, Vec::new, streams.clone(), STREAM_TEST_MIN).await;
    h.records.parks.store(true, Ordering::SeqCst);
    expect_startup(&h, &mut rx).await;
    streams.settle().await;
    h.emit(AACPEvent::OwnershipToFalseRequest);
    assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
    h.emit_queued(owns(0)).await;
    use SinkEvent::{Hold, Park};
    wait_until("the hold", || h.sink_events().contains(&Hold(true))).await;

    // Playing past twice the minimum: no park, no take.
    tokio::time::sleep(STREAM_TEST_MIN * 2).await;
    assert_eq!(h.sink_events(), [Hold(false), Hold(true)]);
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());

    streams.set(vec![StreamProps {
        corked: true,
        ..music(52)
    }]);
    wait_until("the park", || h.sink_events().ends_with(&[Park])).await;
    assert_eq!(h.sink_events(), [Hold(false), Hold(true), Park]);
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    stop_handoff_task(mac);
}

/// The iPhone owns at startup (#311 r3): the seed facts (it plays, owner
/// [0]) hold while no stream listing has shown what plays, which is no
/// idle. Its first listing, held back, shows a non-MPRIS song playing
/// since the baseline: no park before the listing nor after.
#[tokio::test]
async fn park_baseline_seed_hold_waits_for_the_first_listing() {
    let mac = "5A:00:00:00:11:0A";
    let (manager, mut rx, _tx) = ready_session().await;
    {
        let mut state = manager.state.lock().await;
        state.audio_source = Some(AudioSource {
            mac: REMOTE.to_string(),
            r#type: AudioSourceType::Media,
        });
        state
            .control_command_status_list
            .push(ControlCommandStatus {
                identifier: Control::OwnsConnection,
                value: vec![0],
            });
    }
    let streams = LiveStreams::default();
    streams.set(vec![music(53)]);
    let (release, gate) = std::sync::mpsc::channel::<()>();
    let source = GatedFirst {
        streams: streams.clone(),
        gate: Some(gate),
    };
    let (records, media) = Harness::fake_media(mac);
    records.parks.store(true, Ordering::SeqCst);
    let poller = move || {
        spawn_playback_poller_with(
            Vec::new,
            source,
            AllAudible,
            STREAM_TEST_MIN,
            STREAM_TEST_POLL,
            |_| (),
        )
    };
    let h = Harness::start_playback(
        SwitchingMode::Handoff,
        mac,
        &manager,
        media,
        records,
        Arc::new(DeviceHandler::default()),
        (mpsc::channel(1).0, poller),
        // Blocks the connect take, so the seed's unrelayed media gets
        // Auto's release at once rather than at the deadline (#452).
        ConnectMode::WhenLastConnected,
    )
    .await;
    // The startup: "not owning", announce.
    assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
    assert_eq!(
        next_packet(&mut rx).await,
        expected_new_device(REMOTE).await
    );
    assert_eq!(next_packet(&mut rx).await, expected_tipi(REMOTE).await);
    use SinkEvent::Hold;
    wait_until("the seed's hold", || h.sink_events().contains(&Hold(true))).await;
    // Idle samples, but no listing yet.
    tokio::time::sleep(STREAM_TEST_POLL * 10).await;
    assert_eq!(streams.listings.load(Ordering::SeqCst), 0);
    let events = h.sink_events();
    assert!(!events.contains(&SinkEvent::Park), "{events:?}");

    release.send(()).expect("first listing waiting");
    streams.settle().await;
    tokio::time::sleep(STREAM_TEST_MIN).await;
    let events = h.sink_events();
    assert!(!events.contains(&SinkEvent::Park), "{events:?}");
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
    stop_handoff_task(mac);
}
