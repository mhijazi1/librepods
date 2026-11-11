use super::*;

#[test]
fn with_playback_requested_takes_on_reconnected() {
    let mut p = Probe::new(true, false);
    p.core = p.core.with_playback_requested();
    assert_eq!(
        p.feed(100, Input::Reconnected(ready_facts())),
        [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
    );
    assert!(
        p.core
            .take_lines()
            .contains(&"AUTO reconnected for new playback; requested Linux ownership".into())
    );
    // Without it, the same reconnect takes nothing.
    let mut p = Probe::new(true, false);
    assert_eq!(
        p.feed(100, Input::Reconnected(ready_facts())),
        [CancelReconnect]
    );
}

#[test]
fn with_category_takes_over_remote_media_on_reconnected() {
    let remote_media = || SessionFacts {
        audio: Some(source(REMOTE, AudioSourceType::Media)),
        ..ready_facts()
    };
    let mut p = Probe::new(true, false);
    p.core = p
        .core
        .with_playback_requested()
        .with_category(crate::media_controller::AUDIO_CATEGORY_MEDIA);
    assert_eq!(
        p.feed(100, Input::Reconnected(remote_media())),
        [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
    );
    // Without it, Linux's idle category is below the remote media's.
    let mut p = Probe::new(true, false);
    p.core = p.core.with_playback_requested();
    assert!(
        !p.feed(100, Input::Reconnected(remote_media()))
            .contains(&SendOwnership(true))
    );
}

#[test]
fn case_closed_follows_wear() {
    let mut p = Probe::ready();
    assert!(!p.core.case_closed());
    p.feed(20, Input::EarState(vec![InCase, InCase]));
    assert!(p.core.case_closed());
    p.feed(30, Input::EarState(vec![InEar, InCase]));
    assert!(!p.core.case_closed());
}

#[test]
fn stop_after_reconnect_take_keeps_ownership() {
    // Automatic mode, case closed, then a disconnect.
    let mut p = Probe::ready();
    p.feed(20, Input::EarState(vec![InCase, InCase]));
    assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
    // New playback asks for the reconnection.
    assert_eq!(p.play(500, false), []);
    assert_eq!(p.play(1000, true), []);
    assert_eq!(p.feed(1100, Input::Tick), [Reconnect { audio: true }]);
    assert_eq!(
        p.feed(1200, Input::Reconnected(ready_facts())),
        [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
    );
    // Playback is no longer running by the first post-baseline sample.
    assert_eq!(p.play(1500, false), []);
    let mut settled = p.feed(1600, Input::Ownership(vec![1]));
    settled.extend(p.play(2000, false));
    // The take holds (#262).
    assert_eq!(settled, []);
    for ms in [2500, 3000, 3500] {
        assert_eq!(p.play(ms, false), []);
    }
    p.idle_ticks(3500);

    // The first post-baseline sample arriving only after the ACK keeps
    // them too.
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
    assert_eq!(p.feed(1300, Input::Ownership(vec![1])), []);
    assert_eq!(p.play(1500, false), []);
    for ms in [2000, 2500, 3000] {
        assert_eq!(p.play(ms, false), []);
    }
    p.idle_ticks(3000);

    // Control: playback still running after the reconnect keeps the
    // AirPods.
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
    assert_eq!(p.play(1500, true), []);
    assert_eq!(p.feed(1600, Input::Ownership(vec![1])), []);
    for ms in [2000, 2500, 3000] {
        assert_eq!(p.play(ms, true), []);
    }
    assert_eq!(p.feed(9000, Input::Tick), []);
    // A later stop keeps them too (#262).
    assert_eq!(p.play(9500, false), []);
    p.idle_ticks(9500);
}

#[test]
fn session_loss_before_release_ack_does_not_retake() {
    // As in restart_during_pending_release_takes_after_ack: Auto's
    // release is pending and playback restarted during it.
    let mut p = auto_release();
    assert_eq!(p.play(1500, true), []);
    p.core.take_lines();
    // The buds go in the case and the link drops. run() finds the sender
    // gone with the wear report and the release ACK still queued: it
    // marks the session lost, feeds the queued events, then disconnects.
    assert_eq!(p.feed(1600, Input::SessionLost), []);
    assert_eq!(p.feed(1610, Input::EarState(vec![InCase, InCase])), []);
    // No retake on the dead session, and no take to pause for.
    assert_eq!(p.feed(1620, Input::Ownership(vec![0])), []);
    assert_eq!(p.feed(1630, Input::Disconnected), [DropSession]);
    assert!(
        p.core
            .take_lines()
            .iter()
            .any(|l| l.starts_with("CASE waiting for new Linux playback"))
    );
    // Case recovery: no reconnect, however long, until a new
    // stopped-to-playing playback.
    for ms in (2000..=5600).step_by(200) {
        assert_eq!(p.feed(ms, Input::Tick), []);
    }
    assert_eq!(p.play(5700, true), []);
    assert_eq!(p.feed(5800, Input::Tick), []);
    assert_eq!(p.play(6000, false), []);
    assert_eq!(p.feed(6100, Input::Tick), []);
    assert_eq!(p.play(6500, true), []);
    assert_eq!(p.feed(6600, Input::Tick), [Reconnect { audio: true }]);
}

#[test]
fn session_loss_blocks_manual_take() {
    // run() checks the live sender before a REPL command: on a dead
    // sender it feeds SessionLost, the queued events (none: the select
    // is biased), then Disconnected, and only then the command.
    let mut p = Probe::new(false, true);
    p.routing(ready_facts());
    p.core.take_lines();
    assert_eq!(p.feed(100, Input::SessionLost), []);
    // Even before the disconnect is fed, the cached routing does not
    // allow a take, a release or auto mode on the dead session.
    assert_eq!(p.feed(110, Input::Take), []);
    assert_eq!(p.feed(120, Input::Release), []);
    assert_eq!(p.feed(130, Input::Auto), []);
    let lines = p.core.take_lines();
    assert_eq!(lines.iter().filter(|l| l.starts_with("WAIT")).count(), 3);
    assert_eq!(p.feed(200, Input::Disconnected), [DropSession]);
    assert_eq!(p.feed(210, Input::Take), []);
    assert_eq!(p.feed(220, Input::Release), []);
    assert_eq!(p.feed(230, Input::Auto), []);
    let lines = p.core.take_lines();
    assert_eq!(lines.iter().filter(|l| l.starts_with("WAIT")).count(), 3);

    // A manual take Linux holds: losing the session sends no release.
    let mut p = Probe::new(false, true);
    p.routing(ready_facts());
    assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
    assert_eq!(p.feed(200, Input::Ownership(vec![1])), []);
    assert_eq!(p.feed(300, Input::SessionLost), []);
    assert_eq!(p.feed(310, Input::Release), []);
    assert_eq!(p.feed(320, Input::RemoteOwnershipRequest), []);
    assert_eq!(p.feed(330, Input::Disconnected), [DropSession]);
    assert_eq!(p.feed(340, Input::Take), []);
}

#[test]
fn session_loss_sends_nothing() {
    // A stop during a pending take whose ACK is dequeued after the
    // session died: no release on it.
    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.play(1000, false), []);
    assert_eq!(p.feed(1100, Input::SessionLost), []);
    assert_eq!(p.feed(1110, Input::Ownership(vec![1])), []);
    assert_eq!(
        p.feed(
            1120,
            Input::ConnectedDevices {
                old: vec![host(LOCAL)],
                new: two_hosts(),
            },
        ),
        []
    );
    assert_eq!(p.feed(1130, Input::RemoteOwnershipRequest), []);
    assert_eq!(p.play(1140, true), []);
    assert_eq!(p.feed(1150, Input::Disconnected), [PauseMedia, DropSession]);
    assert_eq!(p.feed(2200, Input::Tick), [Reconnect { audio: true }]);

    // Quitting with the AirPods taken: nothing to release them on.
    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    assert_eq!(p.feed(700, Input::SessionLost), []);
    assert_eq!(p.feed(710, Input::Quit), [Exit]);
}

#[test]
fn reconnect_take_ignores_stale_init_ownership() {
    // The adopted session's queue still holds the [0] answering the
    // "not owning" sent by connect_control and the audio reconnect.
    let mut p = reconnect_take();
    assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
    // The stop during the take is not lost: the take settles and Linux
    // keeps it (#262).
    assert_eq!(p.play(1500, false), []);
    let mut settled = p.feed(1600, Input::Ownership(vec![1]));
    settled.extend(p.play(2000, false));
    assert_eq!(settled, []);
    for ms in [2500, 3000, 3500] {
        assert_eq!(p.play(ms, false), []);
    }
    p.idle_ticks(3500);

    // Playback keeps running: Linux keeps the AirPods, nothing pauses,
    // and a later stop keeps them too.
    let mut p = reconnect_take();
    assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
    assert_eq!(p.play(1500, true), []);
    assert_eq!(p.feed(1600, Input::Ownership(vec![1])), []);
    for ms in [2000, 2500, 3000] {
        assert_eq!(p.play(ms, true), []);
    }
    assert_eq!(p.feed(9000, Input::Tick), []);
    assert_eq!(p.play(9500, false), []);
    p.idle_ticks(9500);

    // Queued wear, host and audio reports in the same window still
    // update the core: the buds went back in the case, so the next
    // disconnect waits for new playback instead of reconnecting. The
    // pause on that disconnect shows the take was not forgotten.
    let mut p = reconnect_take();
    assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
    assert_eq!(
        p.feed(
            1310,
            Input::ConnectedDevices {
                old: two_hosts(),
                new: two_hosts(),
            },
        ),
        []
    );
    assert_eq!(p.feed(1320, Input::AudioSource(idle())), []);
    assert_eq!(p.feed(1330, Input::EarState(vec![InCase, InCase])), []);
    assert_eq!(p.feed(1600, Input::Ownership(vec![1])), []);
    p.core.take_lines();
    assert_eq!(p.feed(1700, Input::Disconnected), [PauseMedia, DropSession]);
    assert!(
        p.core
            .take_lines()
            .iter()
            .any(|l| l.starts_with("CASE waiting for new Linux playback"))
    );
    for ms in (2000..=9000).step_by(500) {
        assert_eq!(p.feed(ms, Input::Tick), []);
    }
}

#[test]
fn genuine_loss_after_reconnect_take_pauses() {
    // The take settled with its ACK: a later [0] is a real loss and
    // pauses as upstream #841 does.
    let mut p = reconnect_take();
    assert_eq!(p.feed(1300, Input::Ownership(vec![1])), []);
    assert_eq!(p.play(1500, true), []);
    assert_eq!(p.feed(2000, Input::Ownership(vec![0])), [PauseMedia]);
    // The take was forgotten: the stop releases nothing.
    assert_eq!(p.play(2500, false), []);

    // The take is never acknowledged: its timeout ends the window too.
    let mut p = reconnect_take();
    assert_eq!(p.feed(6000, Input::Tick), []);
    assert_eq!(p.feed(6300, Input::Tick), []);
    assert_eq!(p.feed(6400, Input::Ownership(vec![0])), [PauseMedia]);
}

#[test]
fn reconnect_take_window_ends_at_backlog_boundary() {
    let mut p = reconnect_take();
    // Queued before the boundary: the stale init report (#43).
    assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
    p.core.take_lines();
    assert_eq!(p.feed(1350, Input::InitBacklogDrained), []);
    assert_eq!(count_lines(&p.core.take_lines(), "BACKLOG drained"), 1);
    // After it a [0] acknowledges nothing: a loss, yielded once (#39).
    assert_eq!(p.feed(1400, Input::Ownership(vec![0])), [PauseMedia]);
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
    // `requested_take` is cleared: the stop releases nothing.
    assert_eq!(p.play(1500, false), []);
    // The pending take was dropped: no TIMEOUT for it.
    assert_eq!(p.feed(7000, Input::Tick), []);
    assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 0);
}

#[test]
fn backlog_boundary_outside_reconnect_window_is_noop() {
    // Take, ACK, stop, release ACK, take again, then a loss.
    let script = |p: &mut Probe| {
        vec![
            p.play(1100, true),
            p.feed(1200, Input::Ownership(vec![1])),
            p.play(1300, false),
            p.feed(1400, Input::Ownership(vec![0])),
            p.play(1500, true),
            p.feed(1600, Input::Ownership(vec![0])),
        ]
    };
    for setup in [Probe::ready as fn() -> Probe, acked_auto_take] {
        let mut with = setup();
        assert_eq!(with.feed(1050, Input::InitBacklogDrained), []);
        let commands = script(&mut with);
        assert_eq!(commands, script(&mut setup()));
        assert_eq!(commands.last().unwrap(), &[PauseMedia]);
    }
    // An acked take still yields on [0] after the boundary.
    let mut p = acked_auto_take();
    assert_eq!(p.feed(1050, Input::InitBacklogDrained), []);
    assert_eq!(p.feed(1100, Input::Ownership(vec![0])), [PauseMedia]);
}

// #324: the connect take from an idle other host.

const WINDOW_MS: u64 = CONNECT_TAKE_WINDOW.as_millis() as u64;

/// A manual live session seeded with `facts` (default sink routing on,
/// an idle A2DP transport, `hold` for #264), then the session start's
/// `Auto` at 10 ms; returns its commands.
fn session_start(facts: SessionFacts, hold: bool) -> (Probe, Vec<Command>) {
    let mut p = Probe::new(false, true);
    p.core.default_sink_routing = true;
    p.core.a2dp_hold = hold;
    p.routing(facts);
    assert_eq!(p.feed(5, idle_transport()), []);
    p.core.take_lines();
    let out = p.feed(10, Input::Auto);
    (p, out)
}

/// `session_start` with idle facts and owner [0], its take acked at
/// 100 ms.
fn acked_connect_take(hold: bool) -> Probe {
    let (mut p, _) = session_start(ready_facts(), hold);
    assert_eq!(p.feed(100, Input::Ownership(vec![1])), [RouteToAirPods]);
    p
}

/// An automatic live session that drops at 0 ms and reconnects at
/// 1100 ms with `facts`; returns the reconnect's commands.
fn ordinary_reconnect(p: &mut Probe, facts: SessionFacts) -> Vec<Command> {
    assert_eq!(p.feed(0, Input::Disconnected), [DropSession]);
    assert_eq!(p.feed(1000, Input::Tick), [Reconnect { audio: true }]);
    p.feed(1100, Input::Reconnected(facts))
}

#[test]
fn session_start_take_with_idle_remote_owner() {
    for (owner, hold) in [(None, false), (Some(vec![0]), false), (Some(vec![0]), true)] {
        let name = format!("owner={owner:?} hold={hold}");
        let (mut p, auto) = session_start(
            SessionFacts {
                owner,
                ..ready_facts()
            },
            hold,
        );
        let mut expected = if hold { vec![HoldA2dp(false)] } else { vec![] };
        expected.extend([ConnectA2dp, SendOwnership(true), Announce, DrainPlayback]);
        assert_eq!(auto, expected, "{name}");
        assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 1, "{name}");
        // The idle take routes but leaves the A2DP resume to a play edge.
        assert_eq!(
            p.feed(100, Input::Ownership(vec![1])),
            [RouteToAirPods],
            "{name}"
        );
    }
}

#[test]
fn connect_take_with_idle_remote_owner() {
    for (owner, hold) in [
        (None, false),
        (Some(vec![0]), false),
        (Some(vec![1]), false),
        (Some(vec![0]), true),
    ] {
        let name = format!("owner={owner:?} hold={hold}");
        let mut p = Probe::new(true, true);
        p.core.a2dp_hold = hold;
        let out = ordinary_reconnect(
            &mut p,
            SessionFacts {
                owner,
                ..ready_facts()
            },
        );
        let mut expected = vec![AdoptSession];
        if hold {
            expected.push(HoldA2dp(false));
        }
        expected.extend([Announce, SendOwnership(true), DrainPlayback]);
        assert_eq!(out, expected, "{name}");
        assert!(p.core.reconnect_take_pending(), "{name}");
        assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 1, "{name}");
    }
    // A playback reconnect sends its one take, not a second one.
    let mut p = Probe::new(true, false);
    p.core = p.core.with_playback_requested();
    let out = p.feed(100, Input::Reconnected(ready_facts()));
    assert_eq!(takes(&out), 1, "{out:?}");
    assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 0);
}

#[test]
fn connect_take_ignores_stale_init_ownership() {
    let mut p = Probe::new(true, true);
    ordinary_reconnect(&mut p, ready_facts());
    p.core.take_lines();
    let mut all = p.feed(1200, Input::Ownership(vec![0]));
    all.extend(p.feed(1300, Input::InitBacklogDrained));
    all.extend(p.feed(1400, Input::Ownership(vec![1])));
    assert_eq!(all, []);
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, "ACK owner=linux"), 1);
    assert_eq!(count_lines(&lines, "YIELD"), 0);
}

#[test]
fn session_start_keeps_reported_linux_ownership() {
    let linux = || SessionFacts {
        owner: Some(vec![1]),
        ..ready_facts()
    };
    let (mut p, auto) = session_start(linux(), false);
    assert_eq!(auto, [ConnectA2dp, RouteToAirPods, Announce, DrainPlayback]);
    assert!(p.core.pending.is_none());
    assert!(p.core.requested_take && p.core.last_owner_linux);
    assert_eq!(p.play(100, false), []);
    p.idle_ticks(100);
    p.core.take_lines();
    // The deferred resume runs on the first play edge, once.
    assert_eq!(p.play(61_000, true), [ResumeA2dp]);
    assert_eq!(
        count_lines(
            &p.core.take_lines(),
            "AUTO playback started; Linux already owns"
        ),
        1
    );
    assert_eq!(p.play(61_500, false), []);
    assert_eq!(p.play(62_000, true), []);
    // A remote media report after the [1] cleared the last owner and
    // asks for the hold; its relay of 100 still lets Linux keep the
    // AirPods, so it is the last owner again and holds nothing.
    let mut p = Probe::new(false, true);
    p.core.a2dp_hold = true;
    p.routing(linux());
    p.feed(
        1,
        Input::AudioSource(source(REMOTE, AudioSourceType::Media)),
    );
    p.feed(2, relay(REMOTE, IDLE));
    assert!(!p.core.last_owner_linux);
    assert_eq!(
        p.feed(10, Input::Auto),
        [HoldA2dp(false), ConnectA2dp, Announce, DrainPlayback]
    );
    assert!(p.core.requested_take && p.core.last_owner_linux);
    // c1's conditions fail (no bud in): today's tracked release (#54).
    let (p, auto) = session_start(
        SessionFacts {
            ears: vec![OutOfEar, OutOfEar],
            ..linux()
        },
        false,
    );
    assert_eq!(
        auto,
        [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
    );
    assert!(p.core.pending.is_some());
}

#[test]
fn connect_take_ignores_relay_age() {
    // #325 q1 was not run: every connect kind takes alike, however long
    // ago the other host relayed 100.
    const MINUTES: u64 = 10 * 60_000;
    let mut p = Probe::new(false, true);
    p.routing(ready_facts());
    p.feed(0, relay(REMOTE, IDLE));
    assert_eq!(takes(&p.feed(MINUTES, Input::Auto)), 1);
    let mut p = Probe::new(true, true);
    p.routing(ready_facts());
    p.feed(0, relay(REMOTE, IDLE));
    assert_eq!(p.feed(MINUTES, Input::Disconnected), [DropSession]);
    assert_eq!(
        p.feed(MINUTES + 1000, Input::Tick),
        [Reconnect { audio: true }]
    );
    assert_eq!(
        takes(&p.feed(MINUTES + 1100, Input::Reconnected(ready_facts()))),
        1
    );
    // An old relay of 100 beats the stale media report at Auto.
    let mut p = Probe::new(false, true);
    p.routing(SessionFacts {
        audio: Some(source(REMOTE, AudioSourceType::Media)),
        ..ready_facts()
    });
    p.feed(0, relay(REMOTE, IDLE));
    assert_eq!(takes(&p.feed(MINUTES, Input::Auto)), 1);
}

#[test]
fn connect_take_from_relayed_idle_media_holds_nothing() {
    // The media report asked for the hold, but its relay of 100 lets
    // Linux take: the card is not held off and the ACK only routes.
    let mut p = Probe::new(false, true);
    p.core.default_sink_routing = true;
    p.core.a2dp_hold = true;
    p.routing(SessionFacts {
        audio: Some(source(REMOTE, AudioSourceType::Media)),
        ..ready_facts()
    });
    p.feed(5, relay(REMOTE, IDLE));
    assert_eq!(p.feed(6, idle_transport()), []);
    assert_eq!(
        p.feed(10, Input::Auto),
        [
            HoldA2dp(false),
            ConnectA2dp,
            SendOwnership(true),
            Announce,
            DrainPlayback
        ]
    );
    assert_eq!(p.feed(100, Input::Ownership(vec![1])), [RouteToAirPods]);
}

#[test]
fn connect_take_first_sample_before_ack_resumes() {
    // Linux already plays at connect (c10 a): the ACK resumes once.
    for owner in [None, Some(vec![0])] {
        let facts = SessionFacts {
            owner: owner.clone(),
            ..ready_facts()
        };
        let (mut p, _) = session_start(facts, false);
        assert_eq!(p.play(50, true), [], "{owner:?}");
        assert_eq!(
            p.feed(100, Input::Ownership(vec![1])),
            [RouteToAirPods, ResumeA2dp],
            "{owner:?}"
        );
        assert_eq!(p.play(200, false), []);
        assert_eq!(p.play(700, true), [], "{owner:?}");
    }
    // A streaming transport has nothing to resume.
    let (mut p, _) = session_start(ready_facts(), false);
    assert_eq!(p.feed(20, Input::A2dpTransport { active: true }), []);
    assert_eq!(p.play(50, true), []);
    assert_eq!(p.feed(100, Input::Ownership(vec![1])), [RouteToAirPods]);
    assert_eq!(p.play(200, false), []);
    assert_eq!(p.play(700, true), []);
    // A stop before the ACK still defers to the next play edge (#205).
    let (mut p, _) = session_start(ready_facts(), false);
    assert_eq!(p.play(50, true), []);
    assert_eq!(p.play(70, false), []);
    assert_eq!(p.feed(100, Input::Ownership(vec![1])), [RouteToAirPods]);
    assert_eq!(p.play(700, true), [ResumeA2dp]);
    assert_eq!(p.play(1200, false), []);
    assert_eq!(p.play(1700, true), []);
}

#[test]
fn connect_take_first_sample_after_ack_resumes() {
    let linux = || SessionFacts {
        owner: Some(vec![1]),
        ..ready_facts()
    };
    // The acked take (c10 b) and the keep path at owner [1] (c10 c).
    for mut p in [acked_connect_take(false), session_start(linux(), false).0] {
        p.core.take_lines();
        assert_eq!(p.play(200, true), [ResumeA2dp]);
        assert_eq!(
            count_lines(
                &p.core.take_lines(),
                "AUTO playback started; Linux already owns"
            ),
            1
        );
        assert_eq!(p.play(700, false), []);
        assert_eq!(p.play(1200, true), []);
    }
    // A streaming transport has nothing to resume.
    for mut p in [acked_connect_take(false), session_start(linux(), false).0] {
        assert_eq!(p.feed(150, Input::A2dpTransport { active: true }), []);
        assert_eq!(p.play(200, true), []);
        assert_eq!(p.play(700, false), []);
        assert_eq!(p.play(1200, true), []);
    }
}

/// No connect take for `facts` in `mode` (with `relayed` from REMOTE
/// before the start): Auto and an ordinary reconnect keep today's
/// commands.
/// `waits`: the facts lack a report the take needs, so Auto's release
/// comes at the deadline (#452).
fn assert_no_connect_take(
    facts: SessionFacts,
    mode: ConnectMode,
    relayed: Option<u32>,
    waits: bool,
) {
    let name = format!("{facts:?} {mode:?} {relayed:?}");
    let mut p = Probe::new(false, true);
    p.core.connect_mode = mode;
    p.routing(facts.clone());
    if let Some(category) = relayed {
        p.feed(5, relay(REMOTE, category));
    }
    if waits {
        assert_eq!(
            p.feed(10, Input::Auto),
            [ConnectA2dp, Announce, DrainPlayback],
            "auto {name}"
        );
        assert_eq!(p.feed(10 + WINDOW_MS - 1, Input::Tick), [], "{name}");
        assert_eq!(
            p.feed(10 + WINDOW_MS, Input::Tick),
            [SendOwnership(false)],
            "deadline {name}"
        );
    } else {
        assert_eq!(
            p.feed(10, Input::Auto),
            [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback],
            "auto {name}"
        );
    }
    // A reconnect forgets the relays: only facts without one apply.
    if relayed.is_none() {
        let mut p = Probe::new(true, true);
        p.core.connect_mode = mode;
        assert_eq!(
            ordinary_reconnect(&mut p, facts),
            [AdoptSession, Announce, DrainPlayback],
            "reconnect {name}"
        );
    }
}

#[test]
fn no_connect_take_remote_media_or_call() {
    let auto = ConnectMode::Automatically;
    for r#type in [AudioSourceType::Media, AudioSourceType::Call] {
        let remote = || SessionFacts {
            audio: Some(source(REMOTE, r#type)),
            ..ready_facts()
        };
        // Remote media waits for its host's relay.
        let media = r#type == AudioSourceType::Media;
        assert_no_connect_take(remote(), auto, None, media);
        assert_no_connect_take(remote(), auto, Some(STREAM), false);
    }
    // A call's relay of 100 doesn't make it idle.
    let call = SessionFacts {
        audio: Some(source(REMOTE, AudioSourceType::Call)),
        ..ready_facts()
    };
    assert_no_connect_take(call, auto, Some(IDLE), false);
    // An idle report whose host relays above 100.
    let relayed_busy = SessionFacts {
        audio: Some(source(REMOTE, AudioSourceType::None)),
        ..ready_facts()
    };
    assert_no_connect_take(relayed_busy, auto, Some(STREAM), false);
}

#[test]
fn no_connect_take_no_bud_in_ear() {
    for ears in [vec![OutOfEar, OutOfEar], vec![InCase, OutOfEar], vec![]] {
        let waits = ears.is_empty();
        let facts = SessionFacts {
            ears,
            ..ready_facts()
        };
        assert_no_connect_take(facts, ConnectMode::Automatically, None, waits);
    }
}

#[test]
fn no_connect_take_when_last_connected_and_not_last_owner() {
    assert_no_connect_take(ready_facts(), ConnectMode::WhenLastConnected, None, false);
}

#[test]
fn no_connect_take_without_a_remote_host() {
    for hosts in [vec![host(LOCAL)], vec![]] {
        let waits = hosts.is_empty();
        let facts = SessionFacts {
            hosts,
            ..ready_facts()
        };
        assert_no_connect_take(facts, ConnectMode::Automatically, None, waits);
    }
}

#[test]
fn no_connect_take_on_manual_reconnect() {
    let mut p = Probe::new(false, true);
    assert_eq!(p.feed(0, Input::Disconnected), [DropSession]);
    assert_eq!(p.feed(1000, Input::Tick), [Reconnect { audio: false }]);
    assert_eq!(
        p.feed(1100, Input::Reconnected(ready_facts())),
        [AdoptSession, DrainPlayback]
    );
}

#[test]
fn no_connect_take_while_case_recovery_waits() {
    let mut p = Probe::new(true, false);
    assert_eq!(
        p.feed(100, Input::Reconnected(ready_facts())),
        [CancelReconnect]
    );
}

#[test]
fn connect_take_keeps_ownership_while_idle() {
    let mut p = acked_connect_take(false);
    for ms in [200, 700, 1200] {
        assert_eq!(p.play(ms, false), []);
    }
    p.idle_ticks(1200);
    assert!(p.core.requested_take && p.core.pending.is_none());
}

#[test]
fn connect_take_then_play_sends_no_second_take() {
    let mut p = acked_connect_take(false);
    assert_eq!(p.play(200, false), []);
    p.core.take_lines();
    assert_eq!(p.play(700, true), [ResumeA2dp]);
    assert_eq!(
        count_lines(
            &p.core.take_lines(),
            "AUTO playback started; Linux already owns"
        ),
        1
    );
    assert_eq!(p.play(1200, false), []);
    assert_eq!(p.play(1700, true), []);
    // With the transport streaming there is nothing to resume.
    let mut p = acked_connect_take(false);
    assert_eq!(p.feed(150, Input::A2dpTransport { active: true }), []);
    assert_eq!(p.play(200, false), []);
    assert_eq!(p.play(700, true), []);
}

#[test]
fn connect_take_yields_on_live_zero() {
    let mut p = acked_connect_take(true);
    assert_eq!(
        p.feed(200, Input::Ownership(vec![0])),
        [PauseMedia, RouteAway, HoldA2dp(true)]
    );
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
}

// #452: the session start's decision waits for the routing reports.

/// A manual live session with nothing reported yet (default sink
/// routing on, an idle A2DP transport, `hold` for #264) gets `Auto` at
/// 10 ms and the backlog boundary at 20 ms: no ownership packet yet.
fn waiting_start(hold: bool) -> Probe {
    let mut p = Probe::new(false, true);
    p.core.default_sink_routing = true;
    p.core.a2dp_hold = hold;
    assert_eq!(p.feed(5, idle_transport()), []);
    let mut start = if hold { vec![HoldA2dp(false)] } else { vec![] };
    start.extend([ConnectA2dp, Announce, DrainPlayback]);
    assert_eq!(p.feed(10, Input::Auto), start);
    assert_eq!(p.feed(20, Input::InitBacklogDrained), []);
    p.core.take_lines();
    p
}

/// The hosts [LOCAL, REMOTE] at 100 ms (a new remote host: announced)
/// and both buds in at 110 ms.
fn hosts_and_ears(p: &mut Probe) {
    let hosts = Input::ConnectedDevices {
        old: Vec::new(),
        new: two_hosts(),
    };
    assert_eq!(p.feed(100, hosts), [Announce]);
    assert_eq!(p.feed(110, Input::EarState(vec![InEar, InEar])), []);
}

fn remote_source(r#type: AudioSourceType) -> Input {
    Input::AudioSource(source(REMOTE, r#type))
}

#[test]
fn connect_take_waits_for_the_routing_reports() {
    // Spike #157's order: an idle (or Linux's own) source completes it.
    for audio in [idle(), source(LOCAL, AudioSourceType::Media)] {
        let mut p = waiting_start(false);
        hosts_and_ears(&mut p);
        assert_eq!(
            p.feed(120, Input::AudioSource(audio.clone())),
            [SendOwnership(true)],
            "{audio:?}"
        );
        assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 1);
        assert!(p.core.resume_deferred);
        // One take per session start: a later report sends nothing.
        assert_eq!(p.feed(130, relay(REMOTE, IDLE)), []);
        assert_eq!(p.feed(200, Input::Ownership(vec![1])), [RouteToAirPods]);
        assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
        assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 0);
    }
}

#[test]
fn connect_take_waits_for_the_remote_media_relay() {
    use AudioSourceType::{Call, Media};
    // Relayed idle in the window: the take.
    let mut p = waiting_start(false);
    hosts_and_ears(&mut p);
    assert_eq!(p.feed(120, remote_source(Media)), []);
    assert_eq!(p.feed(130, relay(REMOTE, IDLE)), [SendOwnership(true)]);
    assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 1);
    // Relayed playing: Auto's release at that report, untracked with
    // the owner unknown (#289).
    let mut p = waiting_start(false);
    hosts_and_ears(&mut p);
    assert_eq!(p.feed(120, remote_source(Media)), []);
    assert_eq!(p.feed(130, relay(REMOTE, MEDIA)), [SendOwnership(false)]);
    assert!(p.core.pending.is_none());
    assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
    // A remote call: the state is complete, the release at once.
    let mut p = waiting_start(false);
    hosts_and_ears(&mut p);
    assert_eq!(p.feed(120, remote_source(Call)), [SendOwnership(false)]);
    // No relay: the release at the deadline, and a relay of 100 1 ms
    // after it makes no take.
    let mut p = waiting_start(false);
    hosts_and_ears(&mut p);
    assert_eq!(p.feed(120, remote_source(Media)), []);
    assert_eq!(p.feed(10 + WINDOW_MS - 1, Input::Tick), []);
    assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), [SendOwnership(false)]);
    assert_eq!(p.feed(10 + WINDOW_MS + 1, relay(REMOTE, IDLE)), []);
    assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 0);
}

#[test]
fn connect_window_owner_report_keeps() {
    let mut p = waiting_start(false);
    assert_eq!(p.feed(50, Input::Ownership(vec![1])), [RouteToAirPods]);
    assert!(p.core.requested_take && p.core.pending.is_none());
    assert!(p.core.resume_deferred);
    let keep = "AUTO connected; Linux already owns the AirPods, keeping them";
    assert_eq!(count_lines(&p.core.take_lines(), keep), 1);
    hosts_and_ears(&mut p);
    assert_eq!(p.feed(120, Input::AudioSource(idle())), []);
    assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
}

#[test]
fn connect_window_release_tracked_only_for_a_cached_linux_owner() {
    // [1] cached before Auto, then a complete state with no take: the
    // release is tracked, and its [0] is its ACK, not a loss (#289).
    let mut p = Probe::new(false, true);
    assert_eq!(p.feed(0, Input::Ownership(vec![1])), []);
    assert_eq!(
        p.feed(10, Input::Auto),
        [ConnectA2dp, Announce, DrainPlayback]
    );
    hosts_and_ears(&mut p);
    let call = remote_source(AudioSourceType::Call);
    assert_eq!(p.feed(120, call), [SendOwnership(false)]);
    assert!(p.core.pending.is_some());
    p.core.take_lines();
    assert_eq!(p.feed(200, Input::Ownership(vec![0])), []);
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, "ACK owner=not-linux"), 1);
    assert_eq!(count_lines(&lines, "YIELD"), 0);
}

#[test]
fn connect_window_not_opened_when_no_report_can_allow_the_take() {
    // The connect mode blocks the take: Auto's release, as on 84ac699.
    let mut p = Probe::new(false, true);
    p.core.connect_mode = ConnectMode::WhenLastConnected;
    assert_eq!(
        p.feed(10, Input::Auto),
        [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
    );
    hosts_and_ears(&mut p);
    assert_eq!(p.feed(120, Input::AudioSource(idle())), []);
    // A take held: kept, nothing sent (#262).
    let mut p = acked_auto_take();
    assert_eq!(
        p.feed(1100, Input::Auto),
        [ConnectA2dp, Announce, DrainPlayback]
    );
    assert_eq!(p.feed(1100 + WINDOW_MS, Input::Tick), []);
    // Manual mode in the window: no take can follow, so the release.
    let mut p = waiting_start(false);
    assert_eq!(p.feed(50, Input::Manual), [SendOwnership(false)]);
    reports_send_nothing(&mut p);
}

/// The routing reports and the deadline in manual mode or on a lost
/// session: nothing is announced or sent.
fn reports_send_nothing(p: &mut Probe) {
    let hosts = Input::ConnectedDevices {
        old: Vec::new(),
        new: two_hosts(),
    };
    assert_eq!(p.feed(100, hosts), []);
    assert_eq!(p.feed(110, Input::EarState(vec![InEar, InEar])), []);
    assert_eq!(p.feed(120, Input::AudioSource(idle())), []);
    assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
}

#[test]
fn connect_window_play_edge_takes_for_playback() {
    let mut p = waiting_start(false);
    hosts_and_ears(&mut p);
    // Remote media with no relay yet: the connect take waits.
    assert_eq!(p.feed(120, remote_source(AudioSourceType::Media)), []);
    assert_eq!(p.play(200, false), []);
    assert_eq!(p.play_category(300, MEDIA), [SendOwnership(true)]);
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, CONNECT_TAKE), 0);
    assert_eq!(
        count_lines(&lines, "AUTO playback started; requested Linux ownership"),
        1
    );
    assert_eq!(p.feed(400, relay(REMOTE, IDLE)), []);
    assert_eq!(
        p.feed(500, Input::Ownership(vec![1])),
        [RouteToAirPods, ResumeA2dp]
    );
    assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
}

#[test]
fn connect_window_ends_without_a_packet() {
    // Disconnected: no ownership packet, only the reconnect.
    let mut p = waiting_start(false);
    assert_eq!(p.feed(50, Input::Disconnected), [DropSession]);
    assert_eq!(
        p.feed(10 + WINDOW_MS, Input::Tick),
        [Reconnect { audio: true }]
    );
    // Quit exits at once.
    let mut p = waiting_start(false);
    assert_eq!(p.feed(50, Input::Quit), [Exit]);
    // A lost session: its queued reports and the deadline send nothing.
    let mut p = waiting_start(false);
    assert_eq!(p.feed(50, Input::SessionLost), []);
    reports_send_nothing(&mut p);
}

#[test]
fn connect_window_remote_request_only_yields() {
    // Owner unknown: the request's own "not owning", then no take.
    let mut p = waiting_start(false);
    assert_eq!(
        p.feed(50, Input::RemoteOwnershipRequest),
        [SendOwnership(false)]
    );
    hosts_and_ears(&mut p);
    assert_eq!(p.feed(120, Input::AudioSource(idle())), []);
    assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
    // Owner [0]: Linux already yielded, nothing at all.
    let mut p = waiting_start(false);
    assert_eq!(p.feed(30, Input::Ownership(vec![0])), []);
    assert_eq!(p.feed(50, Input::RemoteOwnershipRequest), []);
    hosts_and_ears(&mut p);
    assert_eq!(p.feed(120, Input::AudioSource(idle())), []);
    assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
    assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 0);
}

/// The hosts, both buds out, a remote Media source with no relay (held
/// for it) and an idle sample: the wait goes on.
fn unworn_remote_media(p: &mut Probe) {
    let hosts = Input::ConnectedDevices {
        old: Vec::new(),
        new: two_hosts(),
    };
    assert_eq!(p.feed(100, hosts), [Announce]);
    let out = Input::EarState(vec![OutOfEar, OutOfEar]);
    assert_eq!(p.feed(110, out), []);
    assert_eq!(p.feed(120, remote_source(AudioSourceType::Media)), []);
    assert_eq!(p.play(130, false), []);
}

#[test]
fn connect_window_skipped_play_edge_ends_the_wait() {
    let mut p = waiting_start(false);
    unworn_remote_media(&mut p);
    assert_eq!(p.feed(140, Input::EarState(vec![InEar, InEar])), []);
    assert_eq!(p.feed(145, Input::EarResume { resumed: true }), []);
    assert!(p.core.ear_resume_until.is_some());
    // The skipped edge ends the wait with Auto's release, untracked
    // with the owner unknown (#289).
    assert_eq!(p.play(150, true), [SendOwnership(false)]);
    assert!(p.core.pending.is_none());
    assert_eq!(p.feed(160, relay(REMOTE, IDLE)), []);
    // The release keeps the #313 grace: the next edge is skipped too.
    assert_eq!(p.play(170, false), []);
    assert_eq!(p.play_category(180, MEDIA), []);
    assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
    assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 0);
}

#[test]
fn connect_window_deadline_keeps_the_ear_resume_grace() {
    let mut p = waiting_start(false);
    unworn_remote_media(&mut p);
    assert_eq!(p.feed(1950, Input::EarState(vec![InEar, InEar])), []);
    assert_eq!(p.feed(1955, Input::EarResume { resumed: true }), []);
    // Auto's release at the deadline, as on 84ac699 at Auto, and the
    // #313 grace stays: the edge inside it takes nothing.
    assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), [SendOwnership(false)]);
    assert!(p.core.ear_resume_until.is_some());
    assert_eq!(p.play_category(2100, MEDIA), []);
    assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 0);
}

#[test]
fn connect_window_ends_on_a_release_already_given() {
    let mut p = waiting_start(false);
    assert_eq!(p.feed(30, Input::Ownership(vec![0])), []);
    assert_eq!(p.feed(40, Input::Release), []);
    hosts_and_ears(&mut p);
    assert_eq!(p.feed(120, Input::AudioSource(idle())), []);
    assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
    assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 0);
}

/// `p` idle with the streams known and the park sink on gets the hosts,
/// ears and a remote Media source with no relay: held and parked.
fn park_in_the_wait(p: &mut Probe) {
    p.core.park_sink = true;
    p.core.set_streams(true, false);
    assert_eq!(p.play(30, false), []);
    hosts_and_ears(p);
    let media = remote_source(AudioSourceType::Media);
    assert_eq!(p.feed(120, media), [HoldA2dp(true)]);
    assert_eq!(p.core.park(), Park::Parked);
}

#[test]
fn connect_window_take_undoes_the_remote_media_hold_and_park() {
    let mut p = waiting_start(true);
    park_in_the_wait(&mut p);
    assert_eq!(p.feed(130, relay(REMOTE, IDLE)), [SendOwnership(true)]);
    assert_eq!(
        p.feed(200, Input::Ownership(vec![1])),
        [HoldA2dp(false), RouteToAirPods]
    );
    assert_eq!(p.core.park(), Park::AirPods);
    // An [1] in the wait keeps, ending the hold and park as an ACK does.
    let mut p = waiting_start(true);
    park_in_the_wait(&mut p);
    assert_eq!(
        p.feed(130, Input::Ownership(vec![1])),
        [HoldA2dp(false), RouteToAirPods]
    );
    assert_eq!(p.core.park(), Park::AirPods);
    // A cached [1] keeps at the relay, with no ACK to undo the hold.
    let mut p = Probe::new(false, true);
    p.core.default_sink_routing = true;
    p.core.a2dp_hold = true;
    p.feed(0, Input::Ownership(vec![1]));
    p.feed(5, idle_transport());
    p.feed(10, Input::Auto);
    p.feed(20, Input::InitBacklogDrained);
    park_in_the_wait(&mut p);
    assert_eq!(
        p.feed(130, relay(REMOTE, IDLE)),
        [HoldA2dp(false), RouteToAirPods]
    );
    assert_eq!(p.core.park(), Park::AirPods);
}

#[test]
fn connect_window_play_before_the_decision_resumes() {
    // Linux plays before the reports: the take's ACK resumes.
    let mut p = waiting_start(false);
    assert_eq!(p.play(30, true), []);
    hosts_and_ears(&mut p);
    assert_eq!(
        p.feed(120, Input::AudioSource(idle())),
        [SendOwnership(true)]
    );
    assert_eq!(
        p.feed(200, Input::Ownership(vec![1])),
        [RouteToAirPods, ResumeA2dp]
    );
    // The keep has no ACK: it resumes at once.
    let mut p = waiting_start(false);
    assert_eq!(p.play(30, true), []);
    assert_eq!(
        p.feed(50, Input::Ownership(vec![1])),
        [RouteToAirPods, ResumeA2dp]
    );
}

// #490: a play edge skipped only for a report not yet in waits for it.

/// `waiting_start` with an idle first sample at 30 ms and a play edge
/// at `category` at 60 ms, before any report: nothing is sent and the
/// wait stays open.
fn deferred_edge(category: u32) -> Probe {
    let mut p = waiting_start(false);
    assert_eq!(p.play(30, false), []);
    assert_eq!(p.play_category(60, category), []);
    assert!(p.core.connect_until.is_some());
    p
}

#[test]
fn connect_window_edge_before_the_reports_takes_once_they_land() {
    let mut p = deferred_edge(STREAM);
    hosts_and_ears(&mut p);
    assert_eq!(
        p.feed(120, Input::AudioSource(idle())),
        [SendOwnership(true)]
    );
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, CONNECT_TAKE), 0);
    assert_eq!(count_lines(&lines, PLAYBACK_TAKE), 1);
    // A playback take: its ACK routes and resumes.
    assert_eq!(
        p.feed(200, Input::Ownership(vec![1])),
        [RouteToAirPods, ResumeA2dp]
    );
    assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
    assert_eq!(count_lines(&p.core.take_lines(), PLAYBACK_TAKE), 0);
}

#[test]
fn connect_window_deferred_edge_skips_a_higher_remote_relay() {
    let mut p = deferred_edge(STREAM);
    hosts_and_ears(&mut p);
    assert_eq!(p.feed(120, remote_source(AudioSourceType::Media)), []);
    // Auto's release, untracked with the owner unknown (#289).
    assert_eq!(p.feed(130, relay(REMOTE, MEDIA)), [SendOwnership(false)]);
    assert!(p.core.pending.is_none());
    assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), []);
    assert_eq!(count_lines(&p.core.take_lines(), PLAYBACK_TAKE), 0);
    // A cached [1]: the release is tracked.
    let mut p = Probe::new(false, true);
    assert_eq!(p.feed(0, Input::Ownership(vec![1])), []);
    p.feed(10, Input::Auto);
    assert_eq!(p.play(30, false), []);
    assert_eq!(p.play(60, true), []);
    hosts_and_ears(&mut p);
    assert_eq!(p.feed(120, remote_source(AudioSourceType::Media)), []);
    assert_eq!(p.feed(130, relay(REMOTE, MEDIA)), [SendOwnership(false)]);
    assert!(p.core.pending.is_some());
}

#[test]
fn connect_window_deferred_edge_decides_at_its_own_category() {
    // Deferred for the hosts; a connect take at 100 would skip the
    // relayed 201.
    let mut p = deferred_edge(MEDIA);
    assert_eq!(p.feed(100, Input::EarState(vec![InEar, InEar])), []);
    assert_eq!(p.feed(110, remote_source(AudioSourceType::Media)), []);
    assert_eq!(p.feed(120, relay(REMOTE, STREAM)), []);
    let hosts = Input::ConnectedDevices {
        old: Vec::new(),
        new: two_hosts(),
    };
    assert_eq!(p.feed(130, hosts), [Announce, SendOwnership(true)]);
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, CONNECT_TAKE), 0);
    assert_eq!(count_lines(&lines, PLAYBACK_TAKE), 1);
}

#[test]
fn connect_window_deferred_edge_releases_at_the_deadline() {
    let mut p = deferred_edge(STREAM);
    hosts_and_ears(&mut p);
    assert_eq!(p.feed(10 + WINDOW_MS - 1, Input::Tick), []);
    assert_eq!(p.feed(10 + WINDOW_MS, Input::Tick), [SendOwnership(false)]);
    assert_eq!(p.feed(10 + WINDOW_MS + 1, Input::AudioSource(idle())), []);
    assert_eq!(count_lines(&p.core.take_lines(), PLAYBACK_TAKE), 0);
}

#[test]
fn connect_window_deferred_edge_dropped_when_linux_stops() {
    // As with no edge: the idle connect take, its resume deferred.
    let mut p = deferred_edge(STREAM);
    assert_eq!(p.play(90, false), []);
    hosts_and_ears(&mut p);
    assert_eq!(
        p.feed(120, Input::AudioSource(idle())),
        [SendOwnership(true)]
    );
    assert!(p.core.resume_deferred);
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, CONNECT_TAKE), 1);
    assert_eq!(count_lines(&lines, PLAYBACK_TAKE), 0);
}

#[test]
fn connect_window_deferred_edge_skips_in_the_ear_resume_grace() {
    let mut p = waiting_start(false);
    assert_eq!(p.play(30, false), []);
    assert_eq!(p.feed(40, Input::EarState(vec![OutOfEar, OutOfEar])), []);
    assert_eq!(p.feed(45, remote_source(AudioSourceType::Media)), []);
    assert_eq!(p.play(60, true), []);
    // The buds go back in and the other host resumes (#313).
    assert_eq!(p.feed(70, Input::EarState(vec![InEar, InEar])), []);
    assert_eq!(p.feed(75, Input::EarResume { resumed: true }), []);
    let hosts = Input::ConnectedDevices {
        old: Vec::new(),
        new: two_hosts(),
    };
    assert_eq!(p.feed(100, hosts), [Announce]);
    assert_eq!(p.feed(110, relay(REMOTE, IDLE)), [SendOwnership(false)]);
    assert_eq!(count_lines(&p.core.take_lines(), PLAYBACK_TAKE), 0);
}

#[test]
fn connect_window_deferred_edge_ends_with_the_wait() {
    // The user's release ends the wait; the next Auto has no edge.
    let mut p = waiting_start(false);
    assert_eq!(p.feed(25, Input::Ownership(vec![0])), []);
    assert_eq!(p.play(30, false), []);
    assert_eq!(p.play(60, true), []);
    assert_eq!(p.feed(70, Input::Release), []);
    hosts_and_ears(&mut p);
    assert_eq!(p.feed(120, Input::AudioSource(idle())), []);
    p.core.take_lines();
    let auto = p.feed(3000, Input::Auto);
    assert_eq!(takes(&auto), 1, "{auto:?}");
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, CONNECT_TAKE), 1);
    assert_eq!(count_lines(&lines, PLAYBACK_TAKE), 0);
}

#[test]
fn connect_window_edge_skipped_on_reported_state_ends_the_wait() {
    // Both buds reported out.
    let mut p = waiting_start(false);
    unworn_remote_media(&mut p);
    assert_eq!(p.play_category(150, MEDIA), [SendOwnership(false)]);
    // Only Linux listed.
    let mut p = waiting_start(false);
    let local = Input::ConnectedDevices {
        old: Vec::new(),
        new: vec![host(LOCAL)],
    };
    assert_eq!(p.feed(100, local), []);
    assert_eq!(p.play(130, false), []);
    assert_eq!(p.play(150, true), [SendOwnership(false)]);
    // A remote Media source with no relay, its derived 301 above 201.
    let mut p = waiting_start(false);
    hosts_and_ears(&mut p);
    assert_eq!(p.feed(120, remote_source(AudioSourceType::Media)), []);
    assert_eq!(p.play(130, false), []);
    assert_eq!(p.play(150, true), [SendOwnership(false)]);
    // A remote call reported, the ears not yet: AudioNotIdle on a
    // reported source.
    let mut p = waiting_start(false);
    let hosts = Input::ConnectedDevices {
        old: Vec::new(),
        new: two_hosts(),
    };
    assert_eq!(p.feed(100, hosts), [Announce]);
    assert_eq!(p.feed(120, remote_source(AudioSourceType::Call)), []);
    assert_eq!(p.play(130, false), []);
    assert_eq!(p.play(150, true), [SendOwnership(false)]);
}

/// A #307 reconnect at the aggregate 301 for a play that asked at
/// `requested`, the remote playing media (301).
fn reconnect_requested(requested: u32) -> (Probe, Vec<Command>) {
    use crate::media_controller::AUDIO_CATEGORY_MEDIA;
    let remote_media_facts = SessionFacts {
        audio: Some(source(REMOTE, AudioSourceType::Media)),
        ..ready_facts()
    };
    let mut p = Probe::new(true, false);
    p.core = p
        .core
        .with_playback_requested()
        .with_category(AUDIO_CATEGORY_MEDIA)
        .with_requested_category(requested);
    let out = p.feed(100, Input::Reconnected(remote_media_facts));
    (p, out)
}

#[test]
fn with_requested_category_takes_at_the_request() {
    use crate::media_controller::{AUDIO_CATEGORY_MEDIA, AUDIO_CATEGORY_STREAM};
    let (_, out) = reconnect_requested(AUDIO_CATEGORY_MEDIA);
    assert!(out.contains(&SendOwnership(true)), "{out:?}");
    // A source behind playback asked at 201, below the remote's media.
    let (mut p, out) = reconnect_requested(AUDIO_CATEGORY_STREAM);
    assert!(!out.contains(&SendOwnership(true)), "{out:?}");
    // The core's own category is the aggregate: 301 is no rise.
    let out = p.play_source(200, AUDIO_CATEGORY_MEDIA, None);
    assert!(!out.contains(&SendOwnership(true)), "{out:?}");
}

// #15: sleep, and the link-loss pause.

/// `relaying`, its take for a 201 play acked at 600 ms; Linux owns and
/// plays.
fn relaying_owner() -> Probe {
    let mut p = relaying();
    assert_eq!(
        p.play(500, true),
        [RelayCategory(STREAM), SendOwnership(true)]
    );
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    p
}

/// From the sleep on: a play edge sends nothing and the drop pauses
/// nothing more.
fn sleep_drop(p: &mut Probe, sleep: Vec<Command>) {
    let mut all = sleep;
    all.extend(p.play(1100, false));
    all.extend(p.play(1200, true));
    let drop = p.feed(1300, Input::Disconnected);
    assert!(!all.contains(&SendOwnership(false)), "{all:?}");
    assert_eq!(drop, [DropSession]);
    assert_eq!(&all[all.len() - 1..], [Disconnect]);
}

#[test]
fn sleep_while_playing_pauses_relays_idle_then_disconnects() {
    let mut p = relaying_owner();
    let sleep = p.feed(1000, Input::Sleep);
    assert_eq!(sleep, [PauseMedia, RelayCategory(IDLE), Disconnect]);
    sleep_drop(&mut p, sleep);
}

#[test]
fn sleep_while_idle_only_disconnects() {
    let mut p = relaying_owner();
    assert_eq!(p.play(800, false), [RelayCategory(IDLE)]);
    let sleep = p.feed(1000, Input::Sleep);
    assert_eq!(sleep, [Disconnect]);
    sleep_drop(&mut p, sleep);
    // No session: nothing to put to sleep.
    assert_eq!(p.feed(1400, Input::Sleep), []);
}

#[test]
fn eviction_while_playing_pauses_before_the_drop() {
    // A 0x14 eviction: the session dies with no wear or ownership report.
    let mut p = acked_auto_take();
    assert_eq!(p.feed(1100, Input::SessionLost), []);
    assert_eq!(p.feed(1200, Input::Disconnected), [PauseMedia, DropSession]);
}

#[test]
fn case_close_drop_while_playing_pauses() {
    let mut p = acked_auto_take();
    let mut all = p.feed(1100, Input::EarState(vec![InCase, InCase]));
    all.extend(p.feed(1200, Input::Disconnected));
    assert_eq!(all, [PauseMedia, DropSession]);
}

#[test]
fn reconnect_after_a_loss_pause_takes_without_resume() {
    let mut p = acked_auto_take();
    assert_eq!(p.feed(1050, idle_transport()), []);
    assert_eq!(p.feed(1100, Input::SessionLost), []);
    assert_eq!(p.feed(1200, Input::Disconnected), [PauseMedia, DropSession]);
    assert_eq!(p.feed(2200, Input::Tick), [Reconnect { audio: true }]);
    assert_eq!(
        p.feed(2300, Input::Reconnected(ready_facts())),
        [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
    );
    assert_eq!(count_lines(&p.core.take_lines(), CONNECT_TAKE), 1);
    // The idle take's ACK resumes nothing.
    assert_eq!(p.feed(2400, Input::Ownership(vec![1])), []);
}
