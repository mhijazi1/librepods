use super::*;

#[tokio::test]
async fn handoff_task_replacement_is_scoped_to_device() {
    let first_mac = "5A:00:00:00:00:04";
    let other_mac = "5A:00:00:00:00:05";
    let (first_manager, _first_rx, _first_tx) = ready_session().await;
    let (other_manager, _other_rx, _other_tx) = ready_session().await;
    let (replacement_manager, _replacement_rx, _replacement_tx) = ready_session().await;

    let _first = Harness::start(SwitchingMode::Handoff, first_mac, &first_manager).await;
    let first_task = handoff_task(first_mac).expect("first task");
    let _other = Harness::start(SwitchingMode::Handoff, other_mac, &other_manager).await;
    let other_task = handoff_task(other_mac).expect("other task");
    assert!(!first_task.is_finished());

    let _replacement =
        Harness::start(SwitchingMode::Handoff, first_mac, &replacement_manager).await;
    wait_until("the superseded task to stop", || first_task.is_finished()).await;
    let replacement_task = handoff_task(first_mac).expect("replacement task");
    tokio::time::sleep(SETTLE).await;
    assert!(!replacement_task.is_finished());
    assert!(!other_task.is_finished());
    assert!(handoff_task(first_mac).is_some());

    stop_handoff_task(first_mac);
    stop_handoff_task(other_mac);
}

#[test]
fn last_owner_store_is_per_mac() {
    // MACs no other test uses: the store is process-wide.
    const A: &str = "16:16:16:16:16:0A";
    const B: &str = "16:16:16:16:16:0B";
    assert!(!last_owner_linux(A));
    remember_last_owner_linux(A, true);
    assert!(last_owner_linux(A));
    assert!(!last_owner_linux(B));
    remember_last_owner_linux(A, false);
    assert!(!last_owner_linux(A));
}

/// #294: a started task on a `no_source_session`, holding A2DP after
/// another host's request.
async fn held_task(mac: &str, manager: &AACPManager, rx: &mut mpsc::Receiver<Vec<u8>>) -> Harness {
    let h = Harness::start(SwitchingMode::Handoff, mac, manager).await;
    expect_startup(&h, rx).await;
    h.emit(AACPEvent::OwnershipToFalseRequest);
    wait_until("the hold", || {
        h.sink_events().ends_with(&[SinkEvent::Hold(true)])
    })
    .await;
    // The request's answer.
    assert_eq!(next_packet(rx).await, expected_owns(0).await);
    h
}

#[tokio::test]
async fn stop_releases_a2dp_hold_when_held() {
    let mac = "5A:00:00:00:08:01";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = held_task(mac, &manager, &mut rx).await;
    let mut expected = h.sink_events();
    let task = handoff_task(mac).expect("task");
    stop_handoff_task(mac);
    expected.push(SinkEvent::Hold(false));
    assert_eq!(after_stop(&h, task).await, expected);
    // No ownership packet at the stop (map Q1).
    assert_eq!(rest(&mut rx).await, Vec::<Vec<u8>>::new());
}

#[tokio::test]
async fn stop_releases_a2dp_hold_only_when_held() {
    let mac = "5A:00:00:00:08:02";
    let (manager, mut rx, _tx) = no_source_session().await;
    let h = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    expect_startup(&h, &mut rx).await;
    // The session start's release only: the last hold command is false.
    assert_eq!(h.sink_events(), [SinkEvent::Hold(false)]);
    let task = handoff_task(mac).expect("task");
    stop_handoff_task(mac);
    assert_eq!(after_stop(&h, task).await, [SinkEvent::Hold(false)]);
}

#[tokio::test]
async fn replacement_releases_a2dp_hold_before_its_start() {
    let mac = "5A:00:00:00:08:03";
    let (first_manager, mut first_rx, _first_tx) = no_source_session().await;
    let first = held_task(mac, &first_manager, &mut first_rx).await;
    let mut expected = first.sink_events();
    first.records.gated.store(true, Ordering::SeqCst);

    let (manager, mut rx, _tx) = no_source_session().await;
    let replacement = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    wait_until("the release", || first.sink_events().len() > expected.len()).await;
    tokio::time::sleep(SETTLE).await;
    assert!(rx.try_recv().is_err());
    assert_eq!(replacement.sink_events(), []);

    first.records.gate.notify_one();
    expect_startup(&replacement, &mut rx).await;
    assert_eq!(replacement.sink_events(), [SinkEvent::Hold(false)]);
    expected.push(SinkEvent::Hold(false));
    assert_eq!(first.sink_events(), expected);
    stop_handoff_task(mac);
}

#[tokio::test(start_paused = true)]
async fn replacement_releases_a2dp_hold_starts_by_wait_unsettled() {
    let mac = "5A:00:00:00:08:04";
    let (first_manager, mut first_rx, _first_tx) = no_source_session().await;
    let first = held_task(mac, &first_manager, &mut first_rx).await;
    first.records.gated.store(true, Ordering::SeqCst);

    let (manager, mut rx, _tx) = no_source_session().await;
    let replaced = tokio::time::Instant::now();
    let _replacement = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    assert_eq!(next_packet(&mut rx).await, expected_owns(0).await);
    let waited = replaced.elapsed();
    assert!(
        waited >= SHUTDOWN_RELEASE_WAIT && waited < SHUTDOWN_RELEASE_WAIT + TICK,
        "{waited:?}"
    );
    stop_handoff_task(mac);
}

/// The exit path (`stop_all_handoff_tasks`) on this test's tasks only.
#[tokio::test(start_paused = true)]
async fn shutdown_releases_a2dp_holds() {
    let held_mac = "5A:00:00:00:08:05";
    let idle_mac = "5A:00:00:00:08:06";
    let (held_manager, mut held_rx, _held_tx) = no_source_session().await;
    let held = held_task(held_mac, &held_manager, &mut held_rx).await;
    // Its release never settles.
    held.records.gated.store(true, Ordering::SeqCst);
    let (idle_manager, mut idle_rx, _idle_tx) = no_source_session().await;
    let idle = Harness::start(SwitchingMode::Handoff, idle_mac, &idle_manager).await;
    expect_startup(&idle, &mut idle_rx).await;
    let mut expected_held = held.sink_events();
    let expected_idle = idle.sink_events();

    let registry = private_registry(&[held_mac, idle_mac]);
    let stopped = tokio::time::Instant::now();
    shutdown(registry).await;
    assert!(stopped.elapsed() <= SHUTDOWN_RELEASE_WAIT);

    expected_held.push(SinkEvent::Hold(false));
    assert_eq!(held.sink_events(), expected_held);
    tokio::time::sleep(SETTLE).await;
    assert_eq!(held.sink_events(), expected_held);
    assert_eq!(idle.sink_events(), expected_idle);
}

/// A→B→C: B, still waiting for A's release, holds nothing; C must wait
/// for A's release all the same.
#[tokio::test]
async fn second_replacement_releases_a2dp_hold_before_its_start() {
    let mac = "5A:00:00:00:08:07";
    let (first_manager, mut first_rx, _first_tx) = no_source_session().await;
    let first = held_task(mac, &first_manager, &mut first_rx).await;
    let mut expected = first.sink_events();
    first.records.gated.store(true, Ordering::SeqCst);
    let (second_manager, mut second_rx, _second_tx) = no_source_session().await;
    let second = Harness::start(SwitchingMode::Handoff, mac, &second_manager).await;
    wait_until("the release", || first.sink_events().len() > expected.len()).await;

    let (manager, mut rx, _tx) = no_source_session().await;
    let third = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    tokio::time::sleep(SETTLE).await;
    assert!(rx.try_recv().is_err());
    assert_eq!(third.sink_events(), []);

    first.records.gate.notify_one();
    expect_startup(&third, &mut rx).await;
    assert_eq!(third.sink_events(), [SinkEvent::Hold(false)]);
    assert!(second_rx.try_recv().is_err());
    assert_eq!(second.sink_events(), []);
    expected.push(SinkEvent::Hold(false));
    assert_eq!(first.sink_events(), expected);
    stop_handoff_task(mac);
}

/// A held task whose release is gated, replaced by a task that waits for
/// that release; both moved to a private registry.
async fn held_then_replaced(mac: &str) -> (Harness, Harness, &'static StdMutex<Registry>) {
    let (first_manager, mut first_rx, _first_tx) = no_source_session().await;
    let first = held_task(mac, &first_manager, &mut first_rx).await;
    let held = first.sink_events().len();
    first.records.gated.store(true, Ordering::SeqCst);
    let (manager, _rx, _tx) = no_source_session().await;
    let second = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    wait_until("the release", || first.sink_events().len() > held).await;
    (first, second, private_registry(&[mac]))
}

/// The exit while a replacement waits for its predecessor's release
/// waits for that release too.
#[tokio::test]
async fn shutdown_releases_a2dp_holds_of_replaced_tasks() {
    let (first, _second, registry) = held_then_replaced("5A:00:00:00:08:08").await;
    let exit = tokio::spawn(shutdown(registry));
    tokio::time::sleep(SETTLE).await;
    assert!(!exit.is_finished());
    first.records.gate.notify_one();
    tokio::time::timeout(SETTLE, exit)
        .await
        .expect("the exit")
        .unwrap();
}

/// A second exit request (tray Exit, then SIGTERM) waits for the first
/// one's releases.
#[tokio::test]
async fn shutdown_releases_a2dp_holds_for_every_exit_request() {
    let (first, _second, registry) = held_then_replaced("5A:00:00:00:08:09").await;
    let released = first.sink_events();
    let exits = [shutdown(registry), shutdown(registry)].map(tokio::spawn);
    tokio::time::sleep(SETTLE).await;
    assert!(exits.iter().all(|exit| !exit.is_finished()));
    first.records.gate.notify_one();
    for exit in exits {
        tokio::time::timeout(SETTLE, exit)
            .await
            .expect("the exit")
            .unwrap();
    }
    assert_eq!(first.sink_events(), released);
}

/// No task starts once the exit has begun: nothing would release it.
#[tokio::test]
async fn shutdown_releases_a2dp_holds_before_any_new_task() {
    let mac = "5A:00:00:00:08:0A";
    let (first, _second, registry) = held_then_replaced(mac).await;
    let exit = shutdown(registry);
    let (manager, _rx, _tx) = no_source_session().await;
    let shell = bare_shell(mac, manager, mpsc::unbounded_channel().1);
    spawn_handoff_task(registry, mac.to_string(), shell);
    assert!(registry.lock().unwrap().tasks.is_empty());
    first.records.gate.notify_one();
    exit.await;
}

/// A task that ends by itself while parked hands its unpark and release
/// on: a replacement for the MAC waits for them.
#[tokio::test]
async fn park_disconnected_while_parked_replacement_waits_for_the_unpark() {
    let mac = "5A:00:00:00:11:06";
    let (manager, mut rx, _tx) = no_source_session().await;
    let (h, sinks) = router_parked_task(mac, &mut rx, &manager, None).await;
    let task = handoff_task(mac).expect("task");
    manager.disconnect().await;
    wait_until("the task to end", || task.is_finished()).await;
    let events = h.sink_events();
    assert!(
        events.ends_with(&[SinkEvent::Unpark(UnparkTo::Local), SinkEvent::Hold(false)]),
        "{events:?}"
    );

    let (manager, mut rx, _tx) = no_source_session().await;
    let replacement = Harness::start(SwitchingMode::Handoff, mac, &manager).await;
    wait_until("the unload", || sinks.sets().contains(&"unload 7".into())).await;
    tokio::time::sleep(SETTLE).await;
    assert_eq!(replacement.sink_events(), []);
    assert!(rx.try_recv().is_err());
    assert_eq!(default_sink(&sinks), DAC);

    sinks.found.notify_one();
    expect_startup(&replacement, &mut rx).await;
    assert_eq!(replacement.sink_events(), [SinkEvent::Hold(false)]);
    stop_handoff_task(mac);
}
