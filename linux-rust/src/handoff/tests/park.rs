use super::*;

// #200: A2DP resume after a take that found the transport idle.

/// `Probe::ready`, an idle transport reported, then a take for playback
/// at 500 ms.
fn take_with_idle_transport() -> Probe {
    let mut p = Probe::ready();
    assert_eq!(p.feed(20, idle_transport()), []);
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    p
}

/// `take_with_idle_transport`, acknowledged at 600 ms: the shell's
/// resume chain may run until `window_end()`.
fn resumed_take() -> Probe {
    let mut p = take_with_idle_transport();
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), [ResumeA2dp]);
    p
}

fn window_end() -> u64 {
    600 + A2DP_RESUME_WINDOW.as_millis() as u64
}

#[test]
fn take_ack_with_idle_transport_resumes_a2dp_once() {
    let mut p = take_with_idle_transport();
    p.core.take_lines();
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), [ResumeA2dp]);
    let lines = p.core.take_lines();
    assert_eq!(lines.len(), 2, "{lines:?}");
    assert!(lines[0].starts_with("ACK owner=linux"));
    assert_eq!(lines[1], "A2DP transport idle after the take; resuming it");
    // A duplicate [1] acknowledges nothing.
    assert_eq!(p.feed(700, Input::Ownership(vec![1])), []);
    assert_eq!(p.feed(800, idle_transport()), []);
    assert_eq!(p.feed(900, Input::Ownership(vec![1])), []);
}

#[test]
fn take_ack_with_active_transport_does_not_resume() {
    let mut p = Probe::ready();
    // The last report counts.
    assert_eq!(p.feed(20, idle_transport()), []);
    assert_eq!(p.feed(30, Input::A2dpTransport { active: true }), []);
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    // A stop keeps the take (#262); a play edge while owning resumes
    // nothing.
    assert_eq!(p.play(1000, false), []);
    p.idle_ticks(1000);
    assert_eq!(p.play(62_000, true), []);
}

#[test]
fn take_ack_without_transport_report_does_not_resume() {
    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    assert_eq!(p.play(1000, false), []);
    p.idle_ticks(1000);
    assert_eq!(p.play(62_000, true), []);
}

#[test]
fn release_ack_never_resumes_a2dp() {
    // Auto's release.
    let mut p = auto_release();
    assert_eq!(p.feed(300, idle_transport()), []);
    assert_eq!(p.feed(400, Input::Ownership(vec![0])), []);

    // A user release of AirPods Linux owns.
    let mut p = Probe::ready();
    assert_eq!(p.feed(20, idle_transport()), []);
    assert_eq!(p.feed(30, Input::Ownership(vec![1])), []);
    assert_eq!(p.feed(100, Input::Release), [SendOwnership(false)]);
    assert_eq!(p.feed(200, Input::Ownership(vec![0])), []);
}

#[test]
fn manual_take_ack_never_resumes_a2dp() {
    let mut p = Probe::new(false, true);
    p.routing(ready_facts());
    assert_eq!(p.feed(50, idle_transport()), []);
    assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
    assert_eq!(p.feed(200, Input::Ownership(vec![1])), []);
}

#[test]
fn take_ack_during_local_call_does_not_resume() {
    // The call starts while the take is pending.
    let mut p = take_with_idle_transport();
    assert_eq!(p.feed(550, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);

    // The call started before the take.
    let mut p = Probe::ready();
    assert_eq!(p.feed(20, idle_transport()), []);
    assert_eq!(p.feed(30, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
}

#[test]
fn stop_during_take_defers_a2dp_resume_to_next_play() {
    // Plain, then with default sink routing and the category relay on.
    for (routing, relay) in [(false, false), (true, true)] {
        let relayed = |c| {
            if relay {
                vec![RelayCategory(c)]
            } else {
                vec![]
            }
        };
        let mut p = Probe::ready();
        p.core.default_sink_routing = routing;
        p.core.category_relay = relay;
        assert_eq!(p.feed(20, idle_transport()), []);
        assert_eq!(
            p.play_category(500, MEDIA),
            [relayed(MEDIA), vec![SendOwnership(true)]].concat()
        );
        assert_eq!(p.play_category(550, IDLE), relayed(IDLE));
        // The ACK holds the take (#262) and skips the resume.
        let route = if routing {
            vec![RouteToAirPods]
        } else {
            vec![]
        };
        assert_eq!(p.feed(600, Input::Ownership(vec![1])), route);
        p.idle_ticks(600);
        // The next play edge while Linux owns resumes, once.
        p.core.take_lines();
        assert_eq!(
            p.play_category(62_000, MEDIA),
            [relayed(MEDIA), vec![ResumeA2dp]].concat()
        );
        assert_eq!(
            p.core.take_lines(),
            [
                "AUTO playback started; Linux already owns",
                "A2DP transport idle after the take; resuming it"
            ]
        );
        assert_eq!(p.play_category(62_500, IDLE), relayed(IDLE));
        assert_eq!(p.play_category(63_000, MEDIA), relayed(MEDIA));
        p.idle_ticks(63_000);
    }

    // An active transport, or a local call, needs no resume.
    let mut p = routed();
    assert_eq!(p.feed(20, Input::A2dpTransport { active: true }), []);
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.play(550, false), []);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), [RouteToAirPods]);
    assert_eq!(p.play(2000, true), []);

    let mut p = routed();
    assert_eq!(p.feed(20, idle_transport()), []);
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.play(550, false), []);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), [RouteToAirPods]);
    assert_eq!(p.feed(700, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(p.play(2000, true), []);

    // The phone disconnecting first blocks a fresh take, not the resume
    // of the take Linux already holds.
    let mut p = routed();
    assert_eq!(p.feed(20, idle_transport()), []);
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.play(550, false), []);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), [RouteToAirPods]);
    assert_eq!(p.feed(700, report(&[LOCAL, REMOTE], &[LOCAL])), []);
    assert_eq!(p.play(2000, true), [ResumeA2dp]);
    assert_eq!(p.play(2500, false), []);
    assert_eq!(p.play(3000, true), []);
    p.idle_ticks(3000);

    // After a yield, a new take resumes at its own ACK.
    let mut p = take_with_idle_transport();
    assert_eq!(p.play(550, false), []);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    assert_eq!(p.feed(700, Input::Ownership(vec![0])), [PauseMedia]);
    assert_eq!(p.play(1000, true), [SendOwnership(true)]);
    assert_eq!(p.feed(1100, Input::Ownership(vec![1])), [ResumeA2dp]);
}

#[test]
fn stop_during_a2dp_resume_window_keeps_ownership() {
    let mut p = resumed_take();
    p.core.take_lines();
    assert_eq!(p.play(1000, false), []);
    assert_eq!(
        p.core.take_lines(),
        ["AUTO playback stopped; keeping ownership"]
    );
    for ms in [1500, 3000, window_end() - 1] {
        assert_eq!(p.feed(ms, Input::Tick), []);
        assert_eq!(p.play(ms, false), []);
    }
    p.idle_ticks(window_end());
}

#[test]
fn kick_pause_play_during_resume_window_sends_nothing() {
    let mut p = resumed_take();
    assert_eq!(p.play(1000, true), []);
    // The kick's Pause and Play.
    assert_eq!(p.play(2000, false), []);
    assert_eq!(p.play(2500, true), []);
    // The profile cycle pauses the player; the chain plays it again.
    assert_eq!(p.play(4000, false), []);
    assert_eq!(p.play(5500, true), []);
    assert_eq!(p.feed(window_end(), Input::Tick), []);
    assert_eq!(p.feed(window_end() + 200, Input::Tick), []);
    // Linux still owns the AirPods and keeps them after the next stop.
    assert_eq!(p.play(window_end() + 500, false), []);
    p.idle_ticks(window_end() + 500);
}

#[test]
fn stop_persisting_past_resume_window_keeps_ownership() {
    let mut p = resumed_take();
    assert_eq!(p.play(1000, false), []);
    p.idle_ticks(1000);
    assert_eq!(p.play(window_end() + 60_300, false), []);
}

#[test]
fn play_resumed_before_window_end_keeps_ownership() {
    let mut p = resumed_take();
    assert_eq!(p.play(1000, false), []);
    assert_eq!(p.play(3000, true), []);
    assert_eq!(p.feed(window_end(), Input::Tick), []);
    assert_eq!(p.feed(window_end() + 200, Input::Tick), []);
    assert_eq!(p.play(window_end() + 300, true), []);
}

#[test]
fn remote_take_during_resume_window_still_yields() {
    let mut p = resumed_take();
    assert_eq!(p.play(1000, false), []);
    p.core.take_lines();
    assert_eq!(p.feed(1500, Input::Ownership(vec![0])), [PauseMedia]);
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
    assert_eq!(p.feed(window_end(), Input::Tick), []);
    assert_eq!(p.feed(window_end() + 200, Input::Tick), []);
}

#[test]
fn call_ending_during_resume_window_keeps_ownership() {
    let mut p = resumed_take();
    assert_eq!(p.feed(700, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(p.play(1000, false), []);
    assert_eq!(
        p.feed(2000, Input::LocalCall(false)),
        [AdvertiseCall(false)]
    );
    // Through the window's end and a minute past it (#262).
    p.idle_ticks(2000);
}

// #264: the A2DP hold.

/// `hold_ready(true)`, then a take for playback acknowledged at 600 ms.
fn hold_take() -> Probe {
    let mut p = hold_ready(true);
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), [RouteToAirPods]);
    p
}

/// `hold_ready(true)` with another host's request answered: held.
fn held() -> Probe {
    let mut p = hold_ready(true);
    assert_eq!(p.feed(100, Input::RemoteOwnershipRequest), [HoldA2dp(true)]);
    p
}

#[test]
fn hold_session_start_idle_releases_before_connect_a2dp() {
    // Buds out: Auto keeps its release, no connect take (#324).
    let mut p = Probe::new(false, true);
    p.core.a2dp_hold = true;
    p.routing(unworn_facts());
    assert_eq!(
        p.feed(5, Input::Auto),
        [
            HoldA2dp(false),
            ConnectA2dp,
            SendOwnership(false),
            Announce,
            DrainPlayback
        ]
    );
    // A later Auto emits only a change.
    assert_eq!(p.feed(8, Input::Ownership(vec![0])), []);
    assert_eq!(
        p.feed(10, Input::Auto),
        [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
    );
}

#[test]
fn hold_session_start_with_remote_media_holds() {
    let mut p = Probe::new(false, true);
    p.core.a2dp_hold = true;
    // The seed's edge only records the fact: no session start yet.
    p.routing(ready_facts());
    assert_eq!(p.feed(1, remote_media()), []);
    // Relayed, so the state is complete and Auto decides at once (#452).
    assert_eq!(p.feed(2, relay(REMOTE, MEDIA)), []);
    assert_eq!(
        p.feed(5, Input::Auto),
        [
            HoldA2dp(true),
            ConnectA2dp,
            SendOwnership(false),
            Announce,
            DrainPlayback
        ]
    );
}

#[test]
fn hold_reconnected_facts_start_a_session() {
    let mut p = held();
    assert_eq!(
        p.feed(200, Input::Disconnected),
        [HoldA2dp(false), DropSession]
    );
    assert_eq!(p.feed(1200, Input::Tick), [Reconnect { audio: true }]);
    let remote = SessionFacts {
        audio: Some(source(REMOTE, AudioSourceType::Media)),
        ..ready_facts()
    };
    assert_eq!(
        p.feed(1300, Input::Reconnected(remote)),
        [AdoptSession, HoldA2dp(true), Announce, DrainPlayback]
    );
    // Idle facts restore the card, and the idle other host gets the
    // connect take (#324).
    let mut p = hold_ready(true);
    assert_eq!(p.feed(200, Input::Disconnected), [DropSession]);
    assert_eq!(p.feed(1200, Input::Tick), [Reconnect { audio: true }]);
    assert_eq!(
        p.feed(1300, Input::Reconnected(ready_facts())),
        [
            AdoptSession,
            HoldA2dp(false),
            Announce,
            SendOwnership(true),
            DrainPlayback
        ]
    );
}

#[test]
fn hold_issue_sequence_remote_request_after_stop() {
    // Linux stays the idle owner after the stop (#262), so the remote
    // request yields the take and holds.
    let mut p = hold_take();
    assert_eq!(p.play(1000, false), []);
    assert_eq!(
        p.feed(1200, Input::RemoteOwnershipRequest),
        [PauseMedia, RouteAway, SendOwnership(false), HoldA2dp(true)]
    );
    assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
}

#[test]
fn hold_not_on_own_release_ack_alone() {
    let mut p = hold_take();
    let mut all = p.play(1000, false);
    all.extend(p.feed(1050, Input::Release));
    all.extend(p.feed(1100, Input::Ownership(vec![0])));
    for ms in (1200..20_000).step_by(1000) {
        all.extend(p.feed(ms, Input::Tick));
    }
    assert_eq!(all, [SendOwnership(false), PauseMedia]);
}

#[test]
fn hold_remote_request_on_acked_take() {
    let mut p = hold_take();
    assert_eq!(
        p.feed(1100, Input::RemoteOwnershipRequest),
        [PauseMedia, RouteAway, SendOwnership(false), HoldA2dp(true)]
    );
    // Later edges while held emit nothing more.
    assert_eq!(p.feed(1150, Input::RemoteOwnershipRequest), []);
    assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);
    assert_eq!(p.feed(1300, Input::RemoteOwnershipRequest), []);
    assert_eq!(p.feed(1400, remote_media()), []);
}

#[test]
fn hold_remote_request_joins_pending_release() {
    let mut p = hold_take();
    assert_eq!(p.feed(1000, Input::Release), [SendOwnership(false)]);
    assert_eq!(
        p.feed(1100, Input::RemoteOwnershipRequest),
        [PauseMedia, RouteAway, HoldA2dp(true)]
    );
    assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);
}

#[test]
fn hold_remote_request_resends_timed_out_release() {
    let mut p = hold_take();
    assert_eq!(p.feed(700, Input::Release), [SendOwnership(false)]);
    assert_eq!(p.feed(700 + 5001, Input::Tick), []);
    // Not held yet: the timed-out release was the user's.
    assert_eq!(
        p.feed(5800, Input::RemoteOwnershipRequest),
        [PauseMedia, RouteAway, SendOwnership(false), HoldA2dp(true)]
    );
    assert_eq!(p.feed(5900, Input::Tick), []);
    assert_eq!(p.feed(10_900, Input::Tick), []);
    // The remote release timed out too: a repeat re-sends it.
    assert_eq!(
        p.feed(11_000, Input::RemoteOwnershipRequest),
        [SendOwnership(false)]
    );
}

#[test]
fn hold_yield_take() {
    let mut p = hold_take();
    assert_eq!(
        p.feed(1100, Input::Ownership(vec![0])),
        [PauseMedia, RouteAway, HoldA2dp(true)]
    );
    assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);
}

#[test]
fn hold_unsolicited_loss_after_linux_owned() {
    let mut p = hold_ready(true);
    // Linux owns without a take of its own.
    assert_eq!(p.feed(100, Input::Ownership(vec![1])), []);
    assert_eq!(p.feed(200, Input::Ownership(vec![0])), [HoldA2dp(true)]);
    assert_eq!(p.feed(300, Input::Ownership(vec![0])), []);
}

#[test]
fn hold_remote_audio_source() {
    let mut p = hold_ready(true);
    assert_eq!(p.feed(100, remote_media()), [HoldA2dp(true)]);
    assert_eq!(p.feed(200, remote_media()), []);
    // While Linux holds a take, the loss that follows holds instead.
    let mut p = hold_take();
    assert_eq!(p.feed(1100, remote_media()), []);
    assert_eq!(
        p.feed(1200, Input::Ownership(vec![0])),
        [PauseMedia, RouteAway, HoldA2dp(true)]
    );
}

#[test]
fn hold_not_on_refusal_or_session_lost() {
    let mut p = hold_take();
    assert_eq!(p.feed(700, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(p.feed(1100, Input::RemoteOwnershipRequest), []);

    let mut p = hold_ready(true);
    assert_eq!(p.feed(100, Input::SessionLost), []);
    assert_eq!(p.feed(200, Input::RemoteOwnershipRequest), []);
}

#[test]
fn hold_not_after_session_lost_audio_source() {
    let mut p = hold_ready(true);
    assert_eq!(p.feed(100, Input::SessionLost), []);
    assert_eq!(holds(&p.feed(200, remote_media())), []);
    assert_eq!(p.feed(300, Input::Disconnected), [DropSession]);
}

#[test]
fn hold_not_after_session_lost_unsolicited_zero() {
    let mut p = hold_ready(true);
    assert_eq!(p.feed(100, Input::Ownership(vec![1])), []);
    assert_eq!(p.feed(150, Input::SessionLost), []);
    assert_eq!(holds(&p.feed(200, Input::Ownership(vec![0]))), []);
    assert_eq!(p.feed(300, Input::Disconnected), [DropSession]);
}

#[test]
fn hold_session_lost_keeps_emitted_hold_until_disconnect() {
    // A yield after SessionLost emits no hold.
    let mut p = hold_take();
    assert_eq!(p.feed(700, Input::SessionLost), []);
    assert_eq!(holds(&p.feed(1100, Input::Ownership(vec![0]))), []);

    // A hold emitted before SessionLost stays until Disconnected.
    let mut p = held();
    assert_eq!(p.feed(150, Input::SessionLost), []);
    assert_eq!(holds(&p.feed(200, remote_media())), []);
    assert_eq!(holds(&p.feed(300, Input::RemoteOwnershipRequest)), []);
    assert_eq!(
        p.feed(400, Input::Disconnected),
        [HoldA2dp(false), DropSession]
    );
}

#[test]
fn hold_take_ack_releases_before_route_and_resume() {
    let mut p = held();
    assert_eq!(p.feed(200, idle_transport()), []);
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(
        p.feed(600, Input::Ownership(vec![1])),
        [HoldA2dp(false), RouteToAirPods, ResumeA2dp]
    );
    assert_eq!(p.feed(700, Input::Ownership(vec![1])), []);
}

#[test]
fn hold_kept_by_timed_out_or_lost_take() {
    let mut p = held();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    let mut all = Vec::new();
    for ms in (600..12_000).step_by(500) {
        all.extend(p.feed(ms, Input::Tick));
    }
    assert_eq!(holds(&all), []);

    let mut p = held();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(
        p.feed(600, Input::Ownership(vec![0])),
        [PauseMedia, RouteAway]
    );
}

#[test]
fn hold_disconnected_releases_before_drop_session() {
    let mut p = held();
    assert_eq!(
        p.feed(200, Input::Disconnected),
        [HoldA2dp(false), DropSession]
    );
}

#[test]
fn hold_quit_releases_before_exit() {
    let mut p = held();
    assert_eq!(p.feed(200, Input::Quit), [HoldA2dp(false), Exit]);
}

#[test]
fn hold_remote_source_during_own_release() {
    // A user release, a remote source while it is pending, then its
    // ACK: the ACK holds once Linux has no take.
    let mut p = hold_take();
    assert_eq!(p.feed(1000, Input::Release), [SendOwnership(false)]);
    assert_eq!(p.feed(1050, remote_media()), []);
    assert_eq!(
        p.feed(1100, Input::Ownership(vec![0])),
        [PauseMedia, HoldA2dp(true)]
    );
    assert_eq!(p.feed(1200, Input::Tick), []);
    assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);

    // A user release acknowledged late (#55), after its timeout.
    let mut p = hold_take();
    assert_eq!(p.feed(700, Input::Release), [SendOwnership(false)]);
    assert_eq!(p.feed(800, remote_media()), []);
    assert_eq!(p.feed(700 + 5001, Input::Tick), []);
    assert_eq!(
        p.feed(5800, Input::Ownership(vec![0])),
        [PauseMedia, HoldA2dp(true)]
    );
}

#[test]
fn hold_remote_source_after_own_release_ack() {
    let mut p = hold_take();
    assert_eq!(p.feed(1000, Input::Release), [SendOwnership(false)]);
    assert_eq!(p.feed(1100, Input::Ownership(vec![0])), [PauseMedia]);
    assert_eq!(p.feed(1200, remote_media()), [HoldA2dp(true)]);
}

#[test]
fn hold_ends_when_the_other_host_leaves() {
    let mut p = held();
    let only = |hosts| Input::ConnectedDevices {
        old: two_hosts(),
        new: hosts,
    };
    // The other host is still there, or the list is empty: still held.
    assert_eq!(p.feed(200, only(two_hosts())), []);
    assert_eq!(p.feed(250, only(vec![])), []);
    assert_eq!(p.feed(300, only(vec![host(LOCAL)])), [HoldA2dp(false)]);
    assert_eq!(p.feed(400, only(vec![host(LOCAL)])), []);
}

#[test]
fn hold_kept_by_host_list_during_pending_take() {
    let mut p = held();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    let hosts = |new| Input::ConnectedDevices {
        old: two_hosts(),
        new,
    };
    let mut all = p.feed(550, hosts(two_hosts()));
    all.extend(p.feed(560, hosts(vec![])));
    all.extend(p.feed(570, hosts(two_hosts())));
    assert_eq!(holds(&all), []);
    // Its ACK still ends the hold.
    assert_eq!(
        holds(&p.feed(600, Input::Ownership(vec![1]))),
        [HoldA2dp(false)]
    );
}

/// Held, a remote source seen, a take acknowledged, then the AirPods
/// list only Linux.
fn retaken_alone() -> Probe {
    let mut p = held();
    let mut all = p.feed(150, remote_media());
    all.extend(p.play(500, true));
    all.extend(p.feed(600, Input::Ownership(vec![1])));
    assert_eq!(holds(&all), [HoldA2dp(false)]);
    let only_linux = Input::ConnectedDevices {
        old: two_hosts(),
        new: vec![host(LOCAL)],
    };
    assert_eq!(p.feed(700, only_linux), []);
    p
}

#[test]
fn hold_not_resumed_after_the_other_host_left() {
    // A stop Linux keeps the take through (#262), then its loss [0].
    let mut p = retaken_alone();
    let mut all = p.play(1000, false);
    all.extend(p.feed(1100, Input::Ownership(vec![0])));
    // An unsolicited [0] (the `None if was_linux` branch).
    let mut q = retaken_alone();
    all.extend(q.feed(1000, Input::Ownership(vec![0])));
    all.extend(q.play(1100, true));
    for ms in (1200..20_000).step_by(1000) {
        all.extend(q.feed(ms, Input::Tick));
    }
    assert_eq!(holds(&all), []);
}

/// `hold_ready(true)` with the AirPods listing only Linux: released.
fn listed_alone() -> Probe {
    let mut p = hold_ready(true);
    assert_eq!(p.feed(50, report(&[LOCAL, REMOTE], &[LOCAL])), []);
    p
}

/// A remote edge from 200 ms, while only Linux is listed, holds once
/// the other host is listed, and only once (#296).
fn assert_holds_once_listed(mut p: Probe, edge: &[Input]) {
    for (i, input) in edge.iter().enumerate() {
        assert_eq!(holds(&p.feed(200 + 100 * i as u64, input.clone())), []);
    }
    assert_eq!(
        holds(&p.feed(1000, report(&[LOCAL, REMOTE], &[LOCAL, REMOTE]))),
        [HoldA2dp(true)]
    );
    assert_eq!(
        holds(&p.feed(1100, report(&[LOCAL, REMOTE], &[LOCAL, REMOTE]))),
        []
    );
}

#[test]
fn hold_remote_edge_before_host_listed_audio_source() {
    assert_holds_once_listed(listed_alone(), &[remote_media()]);
    // The same from a hold the host list released.
    let mut p = held();
    assert_eq!(
        p.feed(150, report(&[LOCAL, REMOTE], &[LOCAL])),
        [HoldA2dp(false)]
    );
    assert_holds_once_listed(p, &[remote_media()]);
}

#[test]
fn hold_remote_edge_before_host_listed_unsolicited_loss() {
    let edge = [Input::Ownership(vec![1]), Input::Ownership(vec![0])];
    assert_holds_once_listed(listed_alone(), &edge);
}

#[test]
fn hold_remote_edge_before_host_listed_remote_request() {
    assert_holds_once_listed(listed_alone(), &[Input::RemoteOwnershipRequest]);
}

#[test]
fn hold_remote_request_retry_after_host_left_and_returned() {
    let mut p = hold_take();
    assert_eq!(
        holds(&p.feed(1100, Input::RemoteOwnershipRequest)),
        [HoldA2dp(true)]
    );
    assert_eq!(
        p.feed(1200, report(&[LOCAL, REMOTE], &[LOCAL])),
        [HoldA2dp(false)]
    );
    assert_eq!(holds(&p.feed(1100 + 5001, Input::Tick)), []);
    assert!(matches!(p.core.timed_out_release, Some((Cause::Remote, _))));
    assert_eq!(
        holds(&p.feed(6200, report(&[LOCAL, REMOTE], &[LOCAL, REMOTE]))),
        []
    );
    assert_eq!(
        p.feed(6300, Input::RemoteOwnershipRequest),
        [SendOwnership(false), HoldA2dp(true)]
    );
}

#[test]
fn hold_host_list_alone_adds_no_hold() {
    let mut p = held();
    assert_eq!(
        p.feed(150, report(&[LOCAL, REMOTE], &[LOCAL])),
        [HoldA2dp(false)]
    );
    let mut all = p.feed(200, report(&[LOCAL, REMOTE], &[LOCAL, REMOTE]));
    for ms in (300..10_000).step_by(500) {
        all.extend(p.feed(ms, Input::Tick));
    }
    assert_eq!(holds(&all), []);
    // An empty list after only Linux was listed holds nothing either.
    let mut p = held();
    assert_eq!(
        p.feed(150, report(&[LOCAL, REMOTE], &[LOCAL])),
        [HoldA2dp(false)]
    );
    assert_eq!(holds(&p.feed(200, report(&[LOCAL, REMOTE], &[]))), []);
}

/// The c1/c2 sequences of the tests above; every command, in order.
fn hold_sequences(hold: bool) -> Vec<Command> {
    let mut all = Vec::new();
    let mut start = Probe::new(false, true);
    start.core.a2dp_hold = hold;
    start.routing(ready_facts());
    all.extend(start.feed(1, remote_media()));
    all.extend(start.feed(5, Input::Auto));

    let mut p = hold_ready(hold);
    all.extend(p.play(500, true));
    all.extend(p.feed(600, Input::Ownership(vec![1])));
    all.extend(p.play(1000, false));
    all.extend(p.feed(1200, Input::RemoteOwnershipRequest));
    all.extend(p.feed(1250, Input::Ownership(vec![0])));
    all.extend(p.feed(1300, idle_transport()));
    all.extend(p.play(1500, true));
    all.extend(p.feed(1600, Input::Ownership(vec![1])));
    all.extend(p.feed(1700, Input::Ownership(vec![0])));
    all.extend(p.feed(1800, Input::Disconnected));

    let mut p = hold_ready(hold);
    all.extend(p.feed(100, Input::Ownership(vec![1])));
    all.extend(p.feed(200, Input::Ownership(vec![0])));
    all.extend(p.feed(300, Input::Quit));

    let mut p = hold_ready(hold);
    all.extend(p.feed(100, remote_media()));
    all.extend(p.feed(200, Input::Disconnected));
    all.extend(p.feed(1200, Input::Tick));
    all.extend(p.feed(1300, Input::Reconnected(ready_facts())));

    let mut p = hold_ready(hold);
    all.extend(p.play(500, true));
    all.extend(p.feed(600, Input::Ownership(vec![1])));
    all.extend(p.play(1000, false));
    all.extend(p.feed(1050, remote_media()));
    all.extend(p.feed(1100, Input::Ownership(vec![0])));
    all.extend(p.feed(
        1200,
        Input::ConnectedDevices {
            old: two_hosts(),
            new: vec![host(LOCAL)],
        },
    ));
    all
}

#[test]
fn hold_off_never_emits_hold() {
    assert_eq!(holds(&hold_sequences(false)), []);
    // The twin: the same sequences hold and release with the flag on.
    let on = holds(&hold_sequences(true));
    assert!(on.contains(&HoldA2dp(true)) && on.contains(&HoldA2dp(false)));
}

// #301: ear detection's resume while another host owns.
const GRACE_MS: u64 = EAR_RESUME_GRACE.as_millis() as u64;

/// `remote_playing(Some(relayed))`: another host owns, Linux holds no
/// take; a bud out at 100 ms, and back in at 200 ms if `reinsert`.
fn bud_out(relayed: u32, reinsert: bool) -> Probe {
    let mut p = remote_playing(Some(relayed));
    assert!(p.core.remote_owns && !p.core.requested_take);
    assert_eq!(p.feed(100, Input::EarState(vec![InEar, OutOfEar])), []);
    if reinsert {
        assert_eq!(p.feed(200, Input::EarState(vec![InEar, InEar])), []);
    }
    p
}

/// The resumed play, after the removal's late stop sample, then playing
/// samples and Ticks past the grace: no take.
fn ear_resume_does_not_take(relayed: u32) {
    let mut p = bud_out(relayed, true);
    let mut all = p.play_category(300, IDLE);
    all.extend(p.play_category(700, MEDIA));
    assert_eq!(p.core.take_lines(), [EAR_RESUMED]);
    for ms in (1200..=200 + 2 * GRACE_MS).step_by(500) {
        all.extend(p.play_category(ms, MEDIA));
        all.extend(p.feed(ms, Input::Tick));
    }
    assert_eq!(takes(&all), 0, "{relayed}: {all:?}");
}

#[test]
fn ear_resume_while_remote_owns_does_not_take() {
    ear_resume_does_not_take(MEDIA);
}

#[test]
fn ear_resume_while_remote_idle_does_not_take() {
    ear_resume_does_not_take(IDLE);
}

#[test]
fn play_without_ear_resume_takes() {
    for relayed in [MEDIA, IDLE] {
        let mut p = bud_out(relayed, false);
        let mut all = p.play_category(700, MEDIA);
        all.extend(p.play_category(1200, MEDIA));
        assert_eq!(takes(&all), 1, "{relayed}: {all:?}");
    }
}

#[test]
fn ear_resume_grace_expires() {
    let mut p = bud_out(MEDIA, true);
    let mut all = p.play_category(700, MEDIA);
    all.extend(p.play_category(1200, IDLE));
    all.extend(p.feed(1200, Input::Tick));
    assert_eq!(takes(&all), 0, "{all:?}");
    // A fresh play sampled after the grace takes.
    let after = 200 + GRACE_MS + 1;
    assert_eq!(p.play_category(after, MEDIA), [SendOwnership(true)]);
    assert_eq!(p.play_category(after + 500, MEDIA), []);
}

#[test]
fn ear_resume_while_linux_owns_unchanged() {
    let mut p = remote_playing(Some(IDLE));
    assert_eq!(p.play_category(500, MEDIA), [SendOwnership(true)]);
    // Out and in while the take is pending, then while Linux owns.
    p.feed(520, Input::EarState(vec![InEar, OutOfEar]));
    p.feed(540, Input::EarState(vec![InEar, InEar]));
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    assert!(!p.core.remote_owns);
    assert_eq!(p.feed(700, Input::EarState(vec![InEar, OutOfEar])), []);
    assert_eq!(p.play_category(1000, IDLE), []);
    assert_eq!(p.feed(1100, Input::EarState(vec![InEar, InEar])), []);
    assert_eq!(p.core.ear_resume_until, None);
    p.core.take_lines();
    assert_eq!(p.play_category(1200, MEDIA), []);
    assert_eq!(
        p.core.take_lines(),
        ["AUTO playback started; Linux already owns"]
    );
    assert_eq!(p.play_category(1500, IDLE), []);
    p.idle_ticks(1500);
}

#[test]
fn ear_resume_after_late_stop_does_not_take() {
    // Linux's player already plays at the baseline while the remote owns.
    let mut p = remote_playing(Some(MEDIA));
    p.feed(30, Input::Auto);
    assert_eq!(p.play_category(40, MEDIA), []);
    p.feed(100, Input::EarState(vec![InEar, OutOfEar]));
    p.feed(200, Input::EarState(vec![InEar, InEar]));
    // The removal's pause is polled after the reinsertion.
    assert_eq!(p.play_category(300, IDLE), []);
    assert_eq!(p.play_category(800, MEDIA), []);
    assert!(p.core.take_lines().contains(&EAR_RESUMED.to_string()));
}

#[test]
fn ear_resume_grace_covers_poll() {
    assert!(
        EAR_RESUME_GRACE
            >= crate::switching::PLAYBACK_POLL + crate::stream_monitor::MIN_STREAM_PLAYBACK
    );
}

#[test]
fn ear_resume_grace_cleared() {
    let ended: [(&str, fn(&mut Probe)); 4] = [
        ("take", |p| {
            // The iPhone stopped, so a manual take is allowed.
            p.feed(250, Input::AudioSource(idle()));
            assert_eq!(p.feed(300, Input::Take), [SendOwnership(true)])
        }),
        ("owner linux", |p| {
            p.feed(300, Input::Ownership(vec![1]));
        }),
        ("session lost", |p| {
            p.feed(300, Input::SessionLost);
        }),
        ("disconnected", |p| {
            p.feed(300, Input::Disconnected);
        }),
    ];
    for (name, end) in ended {
        let mut p = bud_out(IDLE, true);
        assert!(p.core.ear_resume_until.is_some(), "{name}");
        end(&mut p);
        assert_eq!(p.core.ear_resume_until, None, "{name}");
    }
}

#[test]
fn ear_resume_during_auto_release_does_not_take() {
    let timed_out = 100 + ACK_TIMEOUT.as_millis() as u64 + 100;
    for (name, ms, settle) in [
        ("ack", 1100, Input::Ownership(vec![0])),
        ("timeout", timed_out, Input::Tick),
    ] {
        // Auto releases the AirPods Linux owned while the iPhone plays;
        // no wear report yet, so no connect take (#324).
        let mut p = Probe::new(false, true);
        p.routing(SessionFacts {
            owner: Some(vec![1]),
            ..unworn_facts()
        });
        let mut all = p.feed(100, Input::Auto);
        assert_eq!(takes(&all), 0, "{name}: {all:?}");
        p.wear_in(140);
        all.extend(p.feed(150, remote_media()));
        all.extend(p.feed(160, relay(REMOTE, MEDIA)));
        all.extend(p.play_category(200, MEDIA));
        all.extend(p.feed(300, Input::EarState(vec![InEar, OutOfEar])));
        all.extend(p.feed(400, Input::EarState(vec![InEar, InEar])));
        all.extend(p.play_category(500, IDLE));
        all.extend(p.play_category(1000, MEDIA));
        all.extend(p.feed(ms, settle));
        assert!(p.core.pending.is_none(), "{name}");
        assert_eq!(takes(&all), 0, "{name}: {all:?}");
        assert!(
            p.core.take_lines().contains(&EAR_RESUMED.to_string()),
            "{name}"
        );
    }
}

#[test]
fn ear_resume_grace_needs_remote_owner() {
    let mut p = bud_out(IDLE, true);
    // The iPhone stopped, left (only Linux listed) and came back idle:
    // nobody else owns the AirPods.
    p.feed(240, Input::AudioSource(idle()));
    for (ms, new) in [(250, vec![host(LOCAL)]), (260, two_hosts())] {
        let old = p.core.hosts.clone();
        p.feed(ms, Input::ConnectedDevices { old, new });
    }
    assert!(!p.core.remote_owns && p.core.ear_resume_until.is_some());
    p.core.take_lines();
    // A play within the grace is not ear detection's resume.
    assert_eq!(p.play_category(700, MEDIA), [SendOwnership(true)]);
    assert!(!p.core.take_lines().contains(&EAR_RESUMED.to_string()));
}

// #313: the shell's report on its ear-detection resume.
fn resumed(p: &mut Probe, ms: u64, resumed: bool) {
    assert_eq!(p.feed(ms, Input::EarResume { resumed }), []);
}

#[test]
fn ear_reinsert_without_resume_play_takes() {
    for relayed in [MEDIA, IDLE] {
        let mut p = bud_out(relayed, true);
        resumed(&mut p, 250, false);
        let mut all = p.play_category(700, MEDIA);
        all.extend(p.play_category(1200, MEDIA));
        assert_eq!(takes(&all), 1, "{relayed}: {all:?}");
        assert!(!p.core.take_lines().contains(&EAR_RESUMED.to_string()));
    }
}

#[test]
fn ear_reinsert_without_resume_during_auto_release() {
    let timed_out = 100 + ACK_TIMEOUT.as_millis() as u64 + 100;
    for (name, ms, settle) in [
        ("ack", 1100, Input::Ownership(vec![0])),
        ("timeout", timed_out, Input::Tick),
    ] {
        // ear_resume_during_auto_release_does_not_take's sequence, with
        // the report after the reinsertion, or with no reinsertion.
        let run = |reinsert: bool| {
            let mut p = Probe::new(false, true);
            p.routing(SessionFacts {
                owner: Some(vec![1]),
                ..unworn_facts()
            });
            let mut all = p.feed(100, Input::Auto);
            p.wear_in(140);
            all.extend(p.feed(150, remote_media()));
            all.extend(p.feed(160, relay(REMOTE, MEDIA)));
            all.extend(p.play_category(200, MEDIA));
            all.extend(p.feed(300, Input::EarState(vec![InEar, OutOfEar])));
            if reinsert {
                all.extend(p.feed(400, Input::EarState(vec![InEar, InEar])));
                all.extend(p.feed(450, Input::EarResume { resumed: false }));
            }
            all.extend(p.play_category(500, IDLE));
            all.extend(p.play_category(1000, MEDIA));
            assert!(p.core.play_during_release.is_some(), "{name}");
            all.extend(p.feed(ms, settle.clone()));
            let lines = p.core.take_lines();
            assert!(!lines.contains(&EAR_RESUMED.to_string()), "{name}");
            all
        };
        assert_eq!(run(true), run(false), "{name}");
    }
}

#[test]
fn ear_resume_reported_still_does_not_take() {
    for relayed in [MEDIA, IDLE] {
        let mut p = bud_out(relayed, true);
        resumed(&mut p, 250, true);
        let mut all = p.play_category(700, MEDIA);
        all.extend(p.play_category(1200, MEDIA));
        assert_eq!(takes(&all), 0, "{relayed}: {all:?}");
        assert_eq!(p.core.take_lines(), [EAR_RESUMED]);
    }
}

#[test]
fn ear_resume_play_before_report_does_not_take() {
    let mut p = bud_out(MEDIA, true);
    assert_eq!(p.play_category(700, MEDIA), []);
    assert_eq!(p.core.take_lines(), [EAR_RESUMED]);
    resumed(&mut p, 800, false);
    assert_eq!(p.play_category(1200, MEDIA), []);
}

#[test]
fn ear_resume_stale_report_keeps_later_grace() {
    let mut p = bud_out(MEDIA, true);
    p.feed(300, Input::EarState(vec![InEar, OutOfEar]));
    p.feed(400, Input::EarState(vec![InEar, InEar]));
    // The first reinsertion's report: the second is still due one.
    resumed(&mut p, 450, false);
    assert_eq!(p.play_category(700, MEDIA), []);
    assert_eq!(p.core.take_lines(), [EAR_RESUMED]);
    resumed(&mut p, 800, false);
    assert_eq!(p.play_category(900, IDLE), []);
    assert_eq!(p.play_category(1000, MEDIA), [SendOwnership(true)]);

    // A stray report changes nothing.
    let mut p = remote_playing(Some(MEDIA));
    resumed(&mut p, 100, false);
    assert_eq!(p.core.ear_resume_until, None);
    assert_eq!(p.core.ear_reports_due, 0);
}

#[test]
fn ear_resume_stray_report_after_true_keeps_grace() {
    let mut p = bud_out(MEDIA, true);
    resumed(&mut p, 250, true);
    resumed(&mut p, 300, false);
    assert!(p.core.ear_resume_until.is_some());
    assert_eq!(p.play_category(700, MEDIA), []);
    assert_eq!(p.core.take_lines(), [EAR_RESUMED]);
}

#[test]
fn ear_resume_two_step_reinsertion_does_not_take() {
    let mut p = remote_playing(Some(MEDIA));
    p.feed(100, Input::EarState(vec![OutOfEar, OutOfEar]));
    // The first bud resumes the paused player; the second finds none.
    p.feed(200, Input::EarState(vec![InEar, OutOfEar]));
    resumed(&mut p, 250, true);
    p.feed(300, Input::EarState(vec![InEar, InEar]));
    resumed(&mut p, 350, false);
    assert_eq!(p.play_category(700, MEDIA), []);
    assert_eq!(p.core.take_lines(), [EAR_RESUMED]);

    // Past that grace, a fresh one ends on its own `false` report.
    let after = 300 + GRACE_MS + 100;
    assert_eq!(p.play_category(after, IDLE), []);
    p.feed(after + 100, Input::EarState(vec![InEar, OutOfEar]));
    p.feed(after + 200, Input::EarState(vec![InEar, InEar]));
    resumed(&mut p, after + 250, false);
    assert_eq!(p.play_category(after + 300, MEDIA), [SendOwnership(true)]);
}

// #311: the park sink.

/// `hold_ready(true)` with the park on, and an idle sample with the
/// streams known.
fn park_ready() -> Probe {
    let mut p = hold_ready(true);
    p.core = p.core.with_park_sink(true);
    p.core.set_streams(true, false);
    assert_eq!(p.play(20, false), []);
    assert_eq!(p.core.park(), Park::Local);
    p
}

/// `park_ready`, held by another host's request while idle: parked.
fn parked() -> Probe {
    let mut p = park_ready();
    assert_eq!(p.feed(100, Input::RemoteOwnershipRequest), [HoldA2dp(true)]);
    assert_eq!(p.core.park(), Park::Parked);
    p
}

/// Ticks up to `ms` - 1 keep the park; the one at `ms` falls back.
fn park_falls_back_at(p: &mut Probe, ms: u64) {
    for at in (ms - 200..ms).step_by(100) {
        assert_eq!(p.feed(at, Input::Tick), []);
        assert_eq!(p.core.park(), Park::Parked, "{at}");
    }
    assert_eq!(p.feed(ms - 1, Input::Tick), []);
    assert_eq!(p.core.park(), Park::Parked);
    assert_eq!(p.feed(ms, Input::Tick), []);
    assert_eq!(p.core.park(), Park::FallenBack);
}

#[test]
fn park_yield_while_playing_waits_for_an_idle_sample() {
    let mut p = park_ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), [RouteToAirPods]);
    assert_eq!(
        p.feed(1100, Input::Ownership(vec![0])),
        [PauseMedia, RouteAway, HoldA2dp(true)]
    );
    // A game that can't pause keeps playing on the local output.
    assert_eq!(p.core.park(), Park::Local);
    assert_eq!(p.play(1500, true), []);
    assert_eq!(p.core.park(), Park::Local);
    assert_eq!(p.play(2000, false), []);
    assert_eq!(p.core.park(), Park::Parked);
}

#[test]
fn park_waits_for_the_local_call_to_end() {
    let mut p = park_ready();
    assert_eq!(p.feed(50, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(p.feed(100, Input::RemoteOwnershipRequest), [HoldA2dp(true)]);
    assert_eq!(p.core.park(), Park::Local);
    assert_eq!(p.feed(200, Input::LocalCall(false)), [AdvertiseCall(false)]);
    assert_eq!(p.core.park(), Park::Parked);
}

#[test]
fn park_never_without_park_sink() {
    let mut p = hold_ready(true);
    assert_eq!(p.feed(100, Input::RemoteOwnershipRequest), [HoldA2dp(true)]);
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    for ms in (600..10_000).step_by(100) {
        p.feed(ms, Input::Tick);
        assert_eq!(p.core.park(), Park::Local, "{ms}");
    }
}

#[test]
fn park_take_request_falls_back_at_the_timeout() {
    let mut p = parked();
    assert_eq!(p.play(1000, true), [SendOwnership(true)]);
    assert_eq!(p.feed(2800, Input::Tick), []);
    assert_eq!(p.core.park(), Park::Parked, "1.8 s");
    park_falls_back_at(&mut p, 3000);
    // The fallback stays through a stop: no new park until the hold ends.
    assert_eq!(p.play(3500, false), []);
    assert_eq!(p.core.park(), Park::FallenBack);
}

#[test]
fn park_category_skip_falls_back_at_the_timeout() {
    let mut p = park_ready();
    assert_eq!(p.feed(100, remote_media()), [HoldA2dp(true)]);
    assert_eq!(p.core.park(), Park::Parked);
    assert_eq!(p.play(1000, true), []);
    assert!(
        p.core
            .take_lines()
            .iter()
            .any(|l| l.starts_with("AUTO takeover skipped: the audio source's category"))
    );
    park_falls_back_at(&mut p, 3000);
}

#[test]
fn park_connect_mode_skip_falls_back_at_the_timeout() {
    let mut p = parked();
    p.core.connect_mode = ConnectMode::WhenLastConnected;
    assert_eq!(p.play(1000, true), []);
    assert!(p.core.take_lines().iter().any(|l| l == CONNECT_MODE_SKIP));
    park_falls_back_at(&mut p, 3000);
}

#[test]
fn park_every_play_edge_restarts_the_timeout() {
    let mut p = parked();
    p.core.connect_mode = ConnectMode::WhenLastConnected;
    assert_eq!(p.play(1000, true), []);
    assert_eq!(p.play(1500, false), []);
    assert_eq!(p.play(2000, true), []);
    assert_eq!(p.feed(3000, Input::Tick), []);
    assert_eq!(
        p.core.park(),
        Park::Parked,
        "the second edge restarted the deadline to 4000"
    );
    park_falls_back_at(&mut p, 4000);
}

#[test]
fn park_ack_unparks_to_the_airpods_with_the_route() {
    let mut p = parked();
    assert_eq!(p.feed(200, idle_transport()), []);
    assert_eq!(p.play(1000, true), [SendOwnership(true)]);
    assert_eq!(
        p.feed(1500, Input::Ownership(vec![1])),
        [HoldA2dp(false), RouteToAirPods, ResumeA2dp]
    );
    assert_eq!(p.core.park(), Park::AirPods);
    // A later yield parks again once Linux is idle.
    assert_eq!(p.play(2000, false), []);
    assert_eq!(
        p.feed(2500, Input::Ownership(vec![0])),
        [PauseMedia, RouteAway, HoldA2dp(true)]
    );
    assert_eq!(p.core.park(), Park::Parked);
}

#[test]
fn park_ack_after_the_fallback_unparks_to_the_airpods() {
    let mut p = parked();
    assert_eq!(p.play(1000, true), [SendOwnership(true)]);
    park_falls_back_at(&mut p, 3000);
    assert_eq!(
        p.feed(3500, Input::Ownership(vec![1])),
        [HoldA2dp(false), RouteToAirPods]
    );
    assert_eq!(p.core.park(), Park::AirPods);
}

#[test]
fn park_refusal_falls_back_at_the_timeout() {
    let mut p = parked();
    assert_eq!(p.play(1000, true), [SendOwnership(true)]);
    assert_eq!(
        p.feed(1200, Input::Ownership(vec![0])),
        [PauseMedia, RouteAway]
    );
    park_falls_back_at(&mut p, 3000);
}

#[test]
fn park_disconnected_unparks_to_local() {
    let mut p = parked();
    assert_eq!(
        p.feed(200, Input::Disconnected),
        [HoldA2dp(false), DropSession]
    );
    assert_eq!(p.core.park(), Park::Local);
    // So does a fallback.
    let mut p = parked();
    assert_eq!(p.play(1000, true), [SendOwnership(true)]);
    park_falls_back_at(&mut p, 3000);
    p.feed(3100, Input::Disconnected);
    assert_eq!(p.core.park(), Park::Local);
}

#[test]
fn park_quit_unparks_to_local() {
    let mut p = parked();
    assert_eq!(p.feed(200, Input::Quit), [HoldA2dp(false), Exit]);
    assert_eq!(p.core.park(), Park::Local);
}

#[test]
fn park_only_linux_listed_unparks_to_local() {
    let mut p = parked();
    let only_linux = Input::ConnectedDevices {
        old: two_hosts(),
        new: vec![host(LOCAL)],
    };
    assert_eq!(p.feed(200, only_linux), [HoldA2dp(false)]);
    assert_eq!(p.core.park(), Park::Local);
}

#[test]
fn park_baseline_stream_falls_back_without_a_take() {
    let mut p = parked();
    p.core.set_streams(true, true);
    assert_eq!(p.play(1000, false), []);
    assert_eq!(p.core.park(), Park::FallenBack);
}

#[test]
fn park_baseline_stream_keeps_the_hold_from_parking() {
    let mut p = park_ready();
    p.core.set_streams(true, true);
    assert_eq!(p.feed(100, Input::RemoteOwnershipRequest), [HoldA2dp(true)]);
    assert_eq!(p.core.park(), Park::Local);
    p.core.set_streams(true, false);
    assert_eq!(p.play(500, false), []);
    assert_eq!(p.core.park(), Park::Parked);
}

/// The seed's hold (#311 r3): stream presence unknown isn't idle. No
/// park before a sample taken with the streams known; never with a dead
/// stream monitor.
#[test]
fn park_waits_for_a_sample_with_the_streams_known() {
    // `hold_ready`'s idle sample came before any listing.
    let mut p = hold_ready(true);
    p.core = p.core.with_park_sink(true);
    assert_eq!(p.feed(100, Input::RemoteOwnershipRequest), [HoldA2dp(true)]);
    assert_eq!(p.core.park(), Park::Local);
    // Listed, but no sample since.
    p.core.set_streams(true, false);
    assert_eq!(p.feed(200, Input::Tick), []);
    assert_eq!(p.core.park(), Park::Local);
    assert_eq!(p.play(300, false), []);
    assert_eq!(p.core.park(), Park::Parked);
    // The stream monitor dies: back to the local output.
    p.core.set_streams(false, false);
    assert_eq!(p.play(400, false), []);
    assert_eq!(p.core.park(), Park::FallenBack);

    // A dead monitor from the start never parks.
    let mut p = hold_ready(true);
    p.core = p.core.with_park_sink(true);
    assert_eq!(p.feed(100, Input::RemoteOwnershipRequest), [HoldA2dp(true)]);
    for ms in (200..3000).step_by(100) {
        assert_eq!(p.play(ms, false), []);
        assert_eq!(p.core.park(), Park::Local, "{ms}");
    }
}
