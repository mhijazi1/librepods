use super::*;

fn announces(out: &[Command]) -> usize {
    out.iter().filter(|c| matches!(c, Announce)).count()
}

fn stale_replay() -> Input {
    Input::ConnectedDevices {
        old: vec![host(LOCAL)],
        new: two_hosts(),
    }
}

#[test]
fn connected_devices_replay_after_auto_no_second_announce() {
    // #58: the startup seed, then Auto, then the queued report that the
    // seed already contained, replayed with its pre-update `old`.
    for automatic in [false, true] {
        let mut p = Probe::new(automatic, true);
        p.feed(
            0,
            Input::ConnectedDevices {
                old: Vec::new(),
                new: two_hosts(),
            },
        );
        let auto = p.feed(10, Input::Auto);
        assert_eq!(announces(&auto), 1, "automatic={automatic}: {auto:?}");
        assert_eq!(p.feed(20, stale_replay()), [], "automatic={automatic}");
    }
}

#[test]
fn connected_devices_replay_after_reconnect_no_second_announce() {
    // #58: the reconnect's facts already hold the remote host; the
    // report queued before them is replayed without a second announce.
    let mut p = Probe::new(true, true);
    assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
    assert_eq!(p.feed(5000, Input::Tick), [Reconnect { audio: true }]);
    let facts = SessionFacts {
        hosts: two_hosts(),
        ..SessionFacts::default()
    };
    let out = p.feed(5100, Input::Reconnected(facts));
    assert_eq!(announces(&out), 1, "{out:?}");
    assert_eq!(p.feed(5200, stale_replay()), []);
}

#[test]
fn connected_devices_new_host_still_announced() {
    let second = "0D:0E:0F:10:11:12";
    let three = || vec![host(LOCAL), host(REMOTE), host(second)];

    // A fresh `old`.
    let mut p = Probe::ready();
    assert_eq!(
        p.feed(
            100,
            Input::ConnectedDevices {
                old: two_hosts(),
                new: three(),
            },
        ),
        [Announce]
    );
    // The same report again announces nothing.
    assert_eq!(
        p.feed(
            200,
            Input::ConnectedDevices {
                old: two_hosts(),
                new: three(),
            },
        ),
        []
    );

    // A stale `old` still announces the new host exactly once.
    let mut p = Probe::ready();
    assert_eq!(
        p.feed(
            100,
            Input::ConnectedDevices {
                old: vec![host(LOCAL)],
                new: three(),
            },
        ),
        [Announce]
    );
}

#[test]
fn yields_on_remote_request() {
    // Linux did not ask for the AirPods: yield without pausing.
    let mut p = Probe::ready();
    p.feed(20, Input::Ownership(vec![1]));
    assert_eq!(
        p.feed(100, Input::RemoteOwnershipRequest),
        [SendOwnership(false)]
    );
    assert_eq!(p.feed(200, Input::Ownership(vec![0])), []);
    // Already yielded: nothing to send.
    assert_eq!(p.feed(300, Input::RemoteOwnershipRequest), []);

    // Linux took them for playback: pause the players and yield.
    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    assert_eq!(
        p.feed(700, Input::RemoteOwnershipRequest),
        [PauseMedia, SendOwnership(false)]
    );
    // Upstream #841 also pauses when the AirPods report the loss.
    // #39: this [0] acknowledges the remote yield, which already paused,
    // so it pauses nothing.
    assert_eq!(p.feed(800, Input::Ownership(vec![0])), []);
    assert_eq!(p.play(1000, false), []);
    assert_eq!(p.feed(9000, Input::Tick), []);
}

#[test]
fn stop_during_pending_take_keeps_ownership_after_ack() {
    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    // Playback stops while the take waits for its acknowledgement.
    assert_eq!(p.play(1000, false), []);
    // The take holds (#262).
    assert_eq!(p.feed(1100, Input::Ownership(vec![1])), []);
    for ms in [1500, 2000, 2500] {
        assert_eq!(p.play(ms, false), []);
    }
    p.idle_ticks(2500);
}

#[test]
fn restart_during_pending_take_keeps_ownership() {
    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.play(1000, false), []);
    assert_eq!(p.play(1500, true), []);
    assert_eq!(p.feed(1600, Input::Ownership(vec![1])), []);
    for ms in [2000, 2500, 3000] {
        assert_eq!(p.play(ms, true), []);
    }
    assert_eq!(p.feed(9000, Input::Tick), []);
}

#[test]
fn restart_during_pending_release_takes_after_ack() {
    // Playback restarts before Auto's release is acknowledged: no pause,
    // and the AirPods are taken back.
    let mut p = auto_release();
    assert_eq!(p.play(1500, true), []);
    assert_eq!(
        p.feed(1600, Input::Ownership(vec![0])),
        [SendOwnership(true)]
    );
    assert_eq!(p.feed(1700, Input::Ownership(vec![1])), []);
    assert_eq!(p.play(2000, true), []);

    // Stopped again before the acknowledgement: plain release.
    let mut p = auto_release();
    assert_eq!(p.play(1500, true), []);
    assert_eq!(p.play(2000, false), []);
    // #39: the release's ACK pauses nothing.
    assert_eq!(p.feed(2100, Input::Ownership(vec![0])), []);
    assert_eq!(p.play(2500, false), []);
}

#[test]
fn restart_during_pending_release_blocked_does_not_retake() {
    // A remote call starts while Auto's release is in flight. Linux held
    // no take, so its ACK pauses nothing either.
    let mut p = auto_release();
    assert_eq!(p.play(1500, true), []);
    assert_eq!(
        p.feed(
            1550,
            Input::AudioSource(source(REMOTE, AudioSourceType::Call))
        ),
        []
    );
    assert_eq!(p.feed(1600, Input::Ownership(vec![0])), []);
    for ms in [2000, 2500, 3000] {
        assert_eq!(p.play(ms, true), []);
    }
    assert_eq!(p.feed(9000, Input::Tick), []);

    // The buds come out while the release is in flight.
    let mut p = auto_release();
    assert_eq!(p.play(1500, true), []);
    assert_eq!(p.feed(1550, Input::EarState(vec![OutOfEar, InCase])), []);
    assert_eq!(p.feed(1600, Input::Ownership(vec![0])), []);
    assert_eq!(p.play(2000, true), []);
}

#[test]
fn manual_take_and_release() {
    let mut p = Probe::new(false, true);
    p.routing(ready_facts());
    assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
    assert_eq!(p.feed(200, Input::Release), []);
    assert_eq!(p.feed(300, Input::Ownership(vec![1])), []);
    // Manual mode never releases on its own.
    assert_eq!(p.play(400, true), []);
    assert_eq!(p.play(900, false), []);
    assert_eq!(p.feed(1000, Input::Release), [SendOwnership(false)]);
    assert_eq!(p.feed(1100, Input::Ownership(vec![0])), []);
    assert_eq!(p.feed(1200, Input::Release), []);
}

#[test]
fn auto_release_reply_never_yields() {
    // #54: a stop->play edge lands before the reply to Auto's "not
    // owning"; the reply acknowledges that release and the edge is
    // retaken, with no yield or pause. Owner [1]: the only case where
    // the AirPods reply (#289). Buds in only after Auto: no connect take.
    let mut p = Probe::new(false, true);
    p.routing(SessionFacts {
        owner: Some(vec![1]),
        ..unworn_facts()
    });
    assert_eq!(
        p.feed(100, Input::Auto),
        [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
    );
    p.wear_in(150);
    p.core.take_lines();
    let mut all = p.play(200, false);
    all.extend(p.play(300, true));
    all.extend(p.feed(400, Input::Ownership(vec![0])));
    assert_eq!(all, [SendOwnership(true)]);
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
    assert_eq!(p.feed(500, Input::Ownership(vec![1])), []);
    assert_eq!(p.play(600, false), []);
    p.idle_ticks(600);
}

#[test]
fn repeated_auto_reply_never_yields() {
    // A second Auto while the first one's release is pending sends no
    // second "not owning", so no reply is left acknowledging nothing.
    // Owner [1]: the only case where the AirPods reply (#289). Buds in
    // only after the Autos: no connect take.
    let mut p = Probe::new(false, true);
    p.routing(SessionFacts {
        owner: Some(vec![1]),
        ..unworn_facts()
    });
    let mut all = p.feed(100, Input::Auto);
    all.extend(p.feed(150, Input::Auto));
    p.wear_in(160);
    all.extend(p.play(200, false));
    all.extend(p.play(300, true));
    let releases = all.iter().filter(|c| **c == SendOwnership(false)).count();
    for i in 0..releases {
        all.extend(p.feed(400 + i as u64, Input::Ownership(vec![0])));
    }
    all.extend(p.feed(500, Input::Ownership(vec![1])));
    assert!(!all.contains(&PauseMedia));
    assert_eq!(all.iter().filter(|c| **c == SendOwnership(true)).count(), 1);
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
    assert_eq!(p.play(600, false), []);
    p.idle_ticks(600);
}

#[test]
fn auto_tracked_when_linux_owner_acks() {
    // Owner [1]: the AirPods reply to Auto's "not owning", and that [0]
    // acknowledges it rather than being a loss (#54). Buds out: no
    // connect take.
    let mut p = Probe::new(false, true);
    p.routing(SessionFacts {
        owner: Some(vec![1]),
        ..unworn_facts()
    });
    assert_eq!(
        p.feed(100, Input::Auto),
        [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
    );
    assert!(p.core.pending.is_some());
    p.core.take_lines();
    assert_eq!(p.feed(200, Input::Ownership(vec![0])), []);
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, "ACK owner=not-linux"), 1);
    assert_eq!(count_lines(&lines, "YIELD"), 0);
    assert!(p.core.pending.is_none());
}

/// A manual, live session with `facts` gets Auto at 100 ms: owner [0] or
/// unknown, so its "not owning" is untracked (#289). The buds go in only
/// after it, so the connect take (#324) doesn't replace that release.
fn untracked_auto(facts: SessionFacts) -> Probe {
    let mut p = Probe::new(false, true);
    p.routing(SessionFacts {
        ears: unworn_facts().ears,
        ..facts
    });
    assert_eq!(
        p.feed(100, Input::Auto),
        [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
    );
    p.wear_in(100);
    assert!(p.core.pending.is_none());
    p.core.take_lines();
    p
}

#[test]
fn auto_untracked_when_not_owner_waits_for_nothing() {
    // Owner [0]: the AirPods never reply to this "not owning" (#159).
    let tick = 100 + ACK_TIMEOUT.as_millis() as u64 + 1;
    let mut p = untracked_auto(ready_facts());
    assert_eq!(p.feed(tick, Input::Tick), []);
    assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 0);
    assert_eq!(p.core.timed_out_release, None);
    // From the same point, 1 s after Auto: (a) a play edge takes in
    // that step;
    let mut p = untracked_auto(ready_facts());
    assert_eq!(p.play(200, false), []);
    assert_eq!(p.play(1100, true), [SendOwnership(true)]);
    // (b) a take is not answered with WAIT;
    let mut p = untracked_auto(ready_facts());
    assert_eq!(p.feed(1100, Input::Take), [SendOwnership(true)]);
    assert_eq!(count_lines(&p.core.take_lines(), "WAIT"), 0);
    // (c) a quit exits without a Fail;
    let mut p = untracked_auto(ready_facts());
    assert_eq!(p.feed(1100, Input::Quit), [Exit]);
    // (d) a remote request has no release to join.
    let mut p = untracked_auto(ready_facts());
    assert_eq!(p.feed(1100, Input::RemoteOwnershipRequest), []);
    let lines = p.core.take_lines();
    assert_eq!(
        count_lines(
            &lines,
            "REMOTE requested ownership; release already pending"
        ),
        0
    );
}

#[test]
fn auto_untracked_with_unknown_owner() {
    // The usual app case: the AirPods' owner report arrives after Auto.
    let unknown = || SessionFacts {
        owner: None,
        ..ready_facts()
    };
    let tick = 100 + ACK_TIMEOUT.as_millis() as u64 + 1;
    let mut p = untracked_auto(unknown());
    assert_eq!(p.feed(tick, Input::Tick), []);
    assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 0);
    let mut p = untracked_auto(unknown());
    assert_eq!(p.play(200, false), []);
    assert_eq!(p.play(1100, true), [SendOwnership(true)]);
    // That report, a [0] with nothing pending, is no loss and leaves the
    // last owner alone: only an unsolicited [0] after [1] clears it (#16).
    let mut p = untracked_auto(unknown());
    p.core.last_owner_linux = true;
    assert_eq!(p.feed(200, Input::Ownership(vec![0])), []);
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
    assert!(p.core.pending.is_none());
    assert!(p.core.last_owner_linux);
    // Accepted residual: if Linux did own, a take sent before that [0]
    // arrives yields to it.
    let mut p = untracked_auto(unknown());
    assert_eq!(p.play(200, false), []);
    assert_eq!(p.play(300, true), [SendOwnership(true)]);
    assert_eq!(p.feed(400, Input::Ownership(vec![0])), [PauseMedia]);
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
}

#[test]
fn yield_on_owns_connection_zero() {
    // (a) Linux owns the AirPods and another host takes them: yield once.
    let mut p = acked_auto_take();
    p.core.take_lines();
    assert_eq!(p.feed(1100, Input::Ownership(vec![0])), [PauseMedia]);
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
    // The take is forgotten: the stop the pause causes releases nothing,
    // and no timeout fires for it.
    assert_eq!(p.play(1500, false), []);
    assert_eq!(p.feed(7000, Input::Tick), []);
    assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 0);
    // Not stuck: a new stopped-to-playing edge takes again.
    assert_eq!(p.play(7500, true), [SendOwnership(true)]);

    // (b) The pending take resolves to not-linux before any [1].
    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    p.core.take_lines();
    assert_eq!(p.feed(600, Input::Ownership(vec![0])), [PauseMedia]);
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
    assert_eq!(p.play(1000, false), []);
    // Past the take's ACK_TIMEOUT: no TIMEOUT, no reconcile.
    assert_eq!(p.feed(6000, Input::Tick), []);
    assert_eq!(p.feed(9000, Input::Tick), []);
    assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 0);
    // A late [1] is no acknowledgement of a request.
    assert_eq!(p.feed(9100, Input::Ownership(vec![1])), []);
    assert_eq!(count_lines(&p.core.take_lines(), "ACK"), 0);
    // Nothing is left pending: a take is not answered with WAIT.
    assert_eq!(p.feed(9200, Input::Take), [SendOwnership(true)]);
    assert_eq!(count_lines(&p.core.take_lines(), "WAIT"), 0);

    // (c) Manual mode: the take is given up without pausing.
    let mut p = Probe::new(false, true);
    p.routing(ready_facts());
    assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
    assert_eq!(p.feed(200, Input::Ownership(vec![1])), []);
    p.core.take_lines();
    assert_eq!(p.feed(300, Input::Ownership(vec![0])), []);
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
    assert_eq!(p.feed(400, Input::Release), []);
    assert!(
        p.core
            .take_lines()
            .iter()
            .any(|l| l == "Ownership already released")
    );
}

#[test]
fn yield_on_set_ownership_false() {
    // Linux took the AirPods for playback: pause and hand them over.
    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    assert_eq!(
        p.feed(700, Input::RemoteOwnershipRequest),
        [PauseMedia, SendOwnership(false)]
    );

    // Linux owns them without having taken them: hand over, no pause.
    let mut p = Probe::ready();
    assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
    assert_eq!(
        p.feed(100, Input::RemoteOwnershipRequest),
        [SendOwnership(false)]
    );
}

#[test]
fn no_double_yield_remote_then_ack() {
    let mut p = acked_auto_take();
    p.core.take_lines();
    let mut all = p.feed(1100, Input::RemoteOwnershipRequest);
    all.extend(p.feed(1200, Input::Ownership(vec![0])));
    all.extend(p.play(1500, false));
    all.extend(p.feed(9000, Input::Tick));
    assert_eq!(all.iter().filter(|c| **c == PauseMedia).count(), 1);
    assert_eq!(
        all.iter().filter(|c| **c == SendOwnership(false)).count(),
        1
    );
    assert!(!all.contains(&SendOwnership(true)));
    assert_eq!(all, [PauseMedia, SendOwnership(false)]);
    // The [0] acknowledged the yield; it is not a second one.
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
}

#[test]
fn no_double_yield_loss_then_remote() {
    let mut p = acked_auto_take();
    p.core.take_lines();
    let mut all = p.feed(1100, Input::Ownership(vec![0]));
    all.extend(p.feed(1200, Input::RemoteOwnershipRequest));
    all.extend(p.play(1500, false));
    all.extend(p.feed(9000, Input::Tick));
    assert_eq!(all.iter().filter(|c| **c == PauseMedia).count(), 1);
    assert!(!all.iter().any(|c| matches!(c, SendOwnership(_))));
    assert_eq!(all, [PauseMedia]);
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, "YIELD"), 1);
    assert!(
        lines
            .iter()
            .any(|l| l == "REMOTE requested ownership; Linux already yielded")
    );
}

// #17: "moved to" with Undo, and "Connected" on Linux's take.

/// `routed` with notifications on and `hosts` listed, the transport
/// reported idle, and Linux's playback take at `category` acknowledged:
/// one "Connected".
fn notifying_take(hosts: &[&str], category: u32) -> Probe {
    let mut p = Probe::ready_with_hosts(hosts.iter().map(|m| host(m)).collect());
    p.core.default_sink_routing = true;
    p.core.notifications = true;
    assert_eq!(p.feed(20, idle_transport()), []);
    assert_eq!(p.play_category(500, category), [SendOwnership(true)]);
    assert_eq!(
        p.feed(600, Input::Ownership(vec![1])),
        [RouteToAirPods, ResumeA2dp, NotifyConnected]
    );
    p
}

fn moved_to(out: &[Command]) -> Vec<Command> {
    out.iter()
        .filter(|c| matches!(c, NotifyMovedTo(_)))
        .cloned()
        .collect()
}

/// `notifying_take` on [L, R] while Linux plays 301, REMOTE's media
/// takes and the AirPods report the loss: one "moved to REMOTE".
fn yielded_to_remote() -> Probe {
    let mut p = notifying_take(&[L, R], MEDIA);
    assert_eq!(p.feed(700, remote_media()), []);
    assert_eq!(
        p.feed(800, Input::Ownership(vec![0])),
        [
            PauseMedia,
            RouteAway,
            NotifyMovedTo(Some(REMOTE.to_string()))
        ]
    );
    p
}

#[test]
fn lost_take_while_playing_notifies_moved_to() {
    yielded_to_remote();
    // A pending take lost before its ACK is a yield too.
    let mut p = Probe::ready();
    p.core.notifications = true;
    assert_eq!(p.play_category(500, MEDIA), [SendOwnership(true)]);
    assert_eq!(
        p.feed(600, Input::Ownership(vec![0])),
        [PauseMedia, NotifyMovedTo(Some(REMOTE.to_string()))]
    );
}

#[test]
fn lost_take_while_idle_notifies_nothing() {
    let mut p = notifying_take(&[L, R], MEDIA);
    // Linux keeps the take idle (#262), then loses it.
    assert_eq!(p.play(700, false), []);
    assert_eq!(p.feed(800, remote_media()), []);
    assert_eq!(
        p.feed(900, Input::Ownership(vec![0])),
        [PauseMedia, RouteAway]
    );
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
}

#[test]
fn remote_request_while_playing_notifies_moved_to() {
    let mut p = notifying_take(&[L, R], MEDIA);
    assert_eq!(
        p.feed(700, Input::RemoteOwnershipRequest),
        [
            PauseMedia,
            RouteAway,
            SendOwnership(false),
            NotifyMovedTo(Some(REMOTE.to_string()))
        ]
    );
    // Its ACK yields nothing more.
    assert_eq!(p.feed(800, Input::Ownership(vec![0])), []);
}

#[test]
fn remote_request_while_idle_notifies_nothing() {
    let mut p = notifying_take(&[L, R], MEDIA);
    assert_eq!(p.play(700, false), []);
    assert_eq!(
        p.feed(800, Input::RemoteOwnershipRequest),
        [PauseMedia, RouteAway, SendOwnership(false)]
    );
    // Linux owning without a take of its own, idle: the same.
    let mut p = Probe::ready();
    p.core.notifications = true;
    assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
    assert_eq!(
        p.feed(100, Input::RemoteOwnershipRequest),
        [SendOwnership(false)]
    );
}

/// The taker `notifying_take(hosts, STREAM)` names when `audio` is
/// reported and the AirPods report the loss.
fn taker_with(hosts: &[&str], audio: Option<AudioSource>) -> Vec<Command> {
    let mut p = notifying_take(hosts, STREAM);
    if let Some(audio) = audio {
        p.feed(700, Input::AudioSource(audio));
    }
    moved_to(&p.feed(800, Input::Ownership(vec![0])))
}

#[test]
fn yield_taker_is_the_remote_source() {
    for r#type in [AudioSourceType::Media, AudioSourceType::Call] {
        assert_eq!(
            taker_with(&[L, R, R2], Some(source(R2, r#type))),
            [NotifyMovedTo(Some(R2.to_string()))],
            "{type:?}"
        );
        // The source beats the only remote host.
        assert_eq!(
            taker_with(&[L, R], Some(source(R2, r#type))),
            [NotifyMovedTo(Some(R2.to_string()))],
            "{type:?}"
        );
    }
}

#[test]
fn yield_taker_is_the_sole_remote_host() {
    for audio in [
        None,
        Some(idle()),
        Some(source(LOCAL, AudioSourceType::Media)),
        Some(source(R2, AudioSourceType::None)),
    ] {
        assert_eq!(
            taker_with(&[L, R], audio.clone()),
            [NotifyMovedTo(Some(R.to_string()))],
            "{audio:?}"
        );
    }
}

#[test]
fn yield_taker_unknown_with_two_remote_hosts() {
    for audio in [
        None,
        Some(idle()),
        Some(source(LOCAL, AudioSourceType::Media)),
    ] {
        assert_eq!(
            taker_with(&[L, R, R2], audio.clone()),
            [NotifyMovedTo(None)],
            "{audio:?}"
        );
    }
}

#[test]
fn undo_takes_back_and_relays_without_resume() {
    let mut p = yielded_to_remote();
    p.core.take_lines();
    assert_eq!(
        p.feed(900, Input::Undo),
        [SendOwnership(true), SendHijackReversed(REMOTE.to_string())]
    );
    assert_eq!(count_lines(&p.core.take_lines(), "REQUEST owner=linux"), 1);
    // Q12: the transport is still idle, but Undo never resumes or pauses.
    assert_eq!(
        p.feed(1000, Input::Ownership(vec![1])),
        [RouteToAirPods, NotifyConnected]
    );
    // One Undo per yield.
    assert_eq!(p.feed(1100, Input::Undo), []);
}

#[test]
fn undo_without_a_taker_takes_without_relay() {
    let mut p = notifying_take(&[L, R, R2], MEDIA);
    assert_eq!(
        moved_to(&p.feed(700, Input::Ownership(vec![0]))),
        [NotifyMovedTo(None)]
    );
    p.core.take_lines();
    assert_eq!(p.feed(800, Input::Undo), [SendOwnership(true)]);
    assert_eq!(
        count_lines(&p.core.take_lines(), "UNDO the host that took is unknown"),
        1
    );
}

#[test]
fn undo_after_a_later_take_or_disconnect_does_nothing() {
    // A later take.
    let mut p = yielded_to_remote();
    assert_eq!(p.play(900, false), []);
    assert_eq!(p.play_category(1000, MEDIA), [SendOwnership(true)]);
    assert_eq!(p.feed(1100, Input::Undo), []);
    // An OwnsConnection [1].
    let mut p = yielded_to_remote();
    assert_eq!(p.feed(900, Input::Ownership(vec![1])), []);
    assert_eq!(p.feed(1000, Input::Undo), []);
    // A disconnect.
    let mut p = yielded_to_remote();
    assert_eq!(p.feed(900, Input::Disconnected), [DropSession]);
    assert_eq!(p.feed(1000, Input::Undo), []);
    // A release still pending waits; the Undo stays for after its ACK.
    let mut p = notifying_take(&[L, R], MEDIA);
    p.feed(700, Input::RemoteOwnershipRequest);
    assert_eq!(p.feed(800, Input::Undo), []);
    assert_eq!(p.feed(900, Input::Ownership(vec![0])), []);
    assert_eq!(
        p.feed(1000, Input::Undo),
        [SendOwnership(true), SendHijackReversed(REMOTE.to_string())]
    );
}

#[test]
fn take_ack_notifies_connected() {
    // Once per held take's ACK (in `notifying_take`), not for a repeat.
    let mut p = notifying_take(&[L, R], MEDIA);
    assert_eq!(p.feed(700, Input::Ownership(vec![1])), []);
    // Not on a release's ACK.
    assert_eq!(p.feed(800, Input::Release), [SendOwnership(false)]);
    assert_eq!(p.feed(900, Input::Ownership(vec![0])), [PauseMedia]);
    // Not on a lost take.
    assert_eq!(p.play(1000, false), []);
    assert_eq!(p.play_category(1100, MEDIA), [SendOwnership(true)]);
    assert_eq!(
        p.feed(1200, Input::Ownership(vec![0])),
        [
            PauseMedia,
            RouteAway,
            NotifyMovedTo(Some(REMOTE.to_string()))
        ]
    );
    // Not on #324's keep, which takes nothing.
    let mut p = Probe::new(false, true);
    p.core.notifications = true;
    p.routing(SessionFacts {
        owner: Some(vec![1]),
        ..ready_facts()
    });
    p.core.take_lines();
    assert_eq!(
        p.feed(100, Input::Auto),
        [ConnectA2dp, Announce, DrainPlayback]
    );
    assert_eq!(
        count_lines(&p.core.take_lines(), "AUTO connected; Linux already owns"),
        1
    );
}

#[test]
fn owns_connection_zero_after_own_release_is_noop() {
    // (a) The ACK of Auto's release.
    let mut p = auto_release();
    p.core.take_lines();
    assert_eq!(p.feed(1600, Input::Ownership(vec![0])), []);
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, "YIELD"), 0);
    assert_eq!(count_lines(&lines, "ACK owner=not-linux"), 1);

    // (b) The ACK of the yield to a remote request.
    let mut p = acked_auto_take();
    assert_eq!(
        p.feed(1100, Input::RemoteOwnershipRequest),
        [PauseMedia, SendOwnership(false)]
    );
    p.core.take_lines();
    assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, "YIELD"), 0);
    assert_eq!(count_lines(&lines, "ACK owner=not-linux"), 1);

    // (c) #43's stale init report during the reconnect take: the take is
    // not forgotten, so Linux keeps it after a stop and a later [0] is
    // its loss.
    let mut p = reconnect_take();
    p.core.take_lines();
    assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
    assert_eq!(p.feed(1600, Input::Ownership(vec![1])), []);
    assert_eq!(p.play(2000, false), []);
    p.idle_ticks(2000);
    assert_eq!(p.feed(62_100, Input::Ownership(vec![0])), [PauseMedia]);
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
}

/// The c1 sequence of #55 up to the release's timeout: a remote request
/// makes Linux yield, playback stops, and the release's ACK times out.
/// Returns the commands from the remote request on.
fn remote_release_timed_out() -> (Probe, Vec<Command>) {
    let mut p = acked_auto_take();
    p.core.take_lines();
    let mut all = p.feed(1100, Input::RemoteOwnershipRequest);
    assert_eq!(all, [PauseMedia, SendOwnership(false)]);
    let stop = p.play(1500, false);
    assert_eq!(stop, []);
    all.extend(stop);
    let tick = p.feed(6200, Input::Tick);
    assert_eq!(tick, []);
    all.extend(tick);
    assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);
    (p, all)
}

#[test]
fn late_release_ack_after_timeout_remote() {
    let (mut p, mut all) = remote_release_timed_out();
    let late = p.feed(6300, Input::Ownership(vec![0]));
    assert_eq!(late, []);
    all.extend(late);
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, "YIELD"), 0);
    assert_eq!(count_lines(&lines, "ACK owner=not-linux late"), 1);
    assert_eq!(all.iter().filter(|c| **c == PauseMedia).count(), 1);
    // The release is settled: nothing is left to release or time out.
    assert_eq!(p.play(6500, false), []);
    assert_eq!(p.feed(12000, Input::Tick), []);
    assert_eq!(p.feed(12100, Input::Ownership(vec![0])), []);
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
}

#[test]
fn late_release_ack_after_timeout_playback() {
    // Auto's release, sent at 100 ms.
    let mut p = auto_release();
    p.core.take_lines();
    let tick = p.feed(5200, Input::Tick);
    assert_eq!(tick, []);
    assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);
    assert_eq!(p.feed(5300, Input::Ownership(vec![0])), []);
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, "YIELD"), 0);
    assert_eq!(count_lines(&lines, "ACK owner=not-linux late"), 1);
}

#[test]
fn late_release_ack_after_timeout_user() {
    let mut p = acked_auto_take();
    p.core.take_lines();
    assert_eq!(p.feed(1100, Input::Release), [SendOwnership(false)]);
    assert_eq!(p.feed(6200, Input::Tick), []);
    assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);
    // Upstream #841's pause of a user release, as for an on-time ACK.
    assert_eq!(p.feed(6300, Input::Ownership(vec![0])), [PauseMedia]);
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, "YIELD"), 0);
    assert_eq!(count_lines(&lines, "ACK owner=not-linux late"), 1);
    // Consumed once: the release is not paused for again.
    assert_eq!(p.feed(6400, Input::Ownership(vec![0])), []);
}

/// A user release at 1500 ms of an automatic take, whose ACK timed out
/// at 6600 ms.
fn user_release_timed_out() -> Probe {
    let mut p = acked_auto_take();
    p.core.take_lines();
    assert_eq!(p.feed(1500, Input::Release), [SendOwnership(false)]);
    assert_eq!(p.feed(6600, Input::Tick), []);
    assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);
    p
}

#[test]
fn release_timeout_then_loss_after_window_yields() {
    // Long after the release's late-ACK window, a 0 is another host
    // taking the AirPods (#62): one YIELD and one pause.
    let mut p = user_release_timed_out();
    assert_eq!(p.feed(60000, Input::Ownership(vec![0])), [PauseMedia]);
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, "YIELD"), 1);
    assert_eq!(count_lines(&lines, "ACK owner=not-linux late"), 0);
    assert_eq!(p.core.timed_out_release, None);
}

#[test]
fn late_release_ack_window_boundary() {
    assert_eq!(LATE_ACK_WINDOW, Duration::from_secs(10));
    // The release was sent at 1500 ms; the window is inclusive.
    let edge = 1500 + LATE_ACK_WINDOW.as_millis() as u64;

    // (a) Exactly at the window's end: the late acknowledgement, with
    // the user release's pause.
    let mut p = user_release_timed_out();
    assert_eq!(p.feed(edge, Input::Ownership(vec![0])), [PauseMedia]);
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, "ACK owner=not-linux late"), 1);
    assert_eq!(count_lines(&lines, "YIELD"), 0);
    assert_eq!(p.core.timed_out_release, None);

    // (b) One millisecond later: a genuine loss.
    let mut p = user_release_timed_out();
    assert_eq!(p.feed(edge + 1, Input::Ownership(vec![0])), [PauseMedia]);
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, "YIELD"), 1);
    assert_eq!(count_lines(&lines, "ACK owner=not-linux late"), 0);
    assert_eq!(p.core.timed_out_release, None);
}

#[test]
fn take_after_release_timeout_still_yields() {
    // A new take forgets the timed-out release.
    let (mut p, _) = remote_release_timed_out();
    assert_eq!(p.play(6500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(6600, Input::Ownership(vec![1])), []);
    p.core.take_lines();
    assert_eq!(p.feed(6700, Input::Ownership(vec![0])), [PauseMedia]);
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);

    // A report that Linux still owns says the release failed: a later 0
    // is a genuine loss.
    let (mut p, _) = remote_release_timed_out();
    assert_eq!(p.feed(6300, Input::Ownership(vec![1])), []);
    p.core.take_lines();
    assert_eq!(p.feed(6400, Input::Ownership(vec![0])), [PauseMedia]);
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);

    // A disconnect forgets it too.
    let (mut p, _) = remote_release_timed_out();
    assert_eq!(p.feed(6300, Input::Disconnected), [PauseMedia, DropSession]);
    assert_eq!(p.core.timed_out_release, None);
}

#[test]
fn repeated_remote_request_while_release_pending() {
    let mut p = acked_auto_take();
    p.core.take_lines();
    let mut all = p.feed(1100, Input::RemoteOwnershipRequest);
    assert_eq!(all, [PauseMedia, SendOwnership(false)]);
    p.core.take_lines();
    // The same yield is still in flight: no second pause, no resend.
    let again = p.feed(1300, Input::RemoteOwnershipRequest);
    assert_eq!(again, []);
    all.extend(again);
    assert_eq!(
        p.core.take_lines(),
        ["REMOTE requested ownership; release already pending"]
    );
    let stop = p.play(1500, false);
    assert_eq!(stop, []);
    all.extend(stop);
    // 5.1 s after the first request, 4.9 s after the second: the
    // release's timeout still counts from the first.
    let tick = p.feed(6200, Input::Tick);
    assert_eq!(tick, []);
    all.extend(tick);
    assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);
    assert_eq!(all.iter().filter(|c| **c == PauseMedia).count(), 1);
}

#[test]
fn repeated_remote_request_after_release_timeout() {
    let (mut p, mut all) = remote_release_timed_out();
    // The release is re-sent; the first request already paused.
    let again = p.feed(6300, Input::RemoteOwnershipRequest);
    assert_eq!(again, [SendOwnership(false)]);
    all.extend(again);
    // Its [0] is the remote release's ACK (#39): no pause, no YIELD.
    let ack = p.feed(6400, Input::Ownership(vec![0]));
    assert_eq!(ack, []);
    all.extend(ack);
    let lines = p.core.take_lines();
    assert_eq!(count_lines(&lines, "YIELD"), 0);
    assert_eq!(count_lines(&lines, "ACK owner=not-linux"), 1);
    assert_eq!(all.iter().filter(|c| **c == PauseMedia).count(), 1);
    assert_eq!(
        all.iter().filter(|c| **c == SendOwnership(false)).count(),
        2
    );
}

fn count(all: &[Vec<Command>], command: Command) -> usize {
    all.iter().flatten().filter(|c| **c == command).count()
}

#[test]
fn remote_request_joins_pending_playback_release() {
    // #137: a remote request during Auto's release (a Cause::Playback
    // one) joins it; the join itself is pinned in
    // remote_request_joins_pending_auto_release. The joined release's
    // timeout still counts from its send at 100.
    let mut p = auto_release();
    assert_eq!(p.feed(1800, Input::RemoteOwnershipRequest), []);
    p.core.take_lines();
    let tick = 100 + ACK_TIMEOUT.as_millis() as u64 + 1;
    assert_eq!(p.feed(tick, Input::Tick), []);
    assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);

    // Playback restarted during the release: Linux held no take, so the
    // request pauses nothing and the ACK neither retakes nor pauses.
    let mut p = auto_release();
    p.core.take_lines();
    assert_eq!(p.play(1600, true), []);
    assert_eq!(p.feed(1700, Input::RemoteOwnershipRequest), []);
    assert_eq!(p.feed(1800, Input::Ownership(vec![0])), []);
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);

    // A call started during the release: with no take to keep, the
    // request joins it too, so the ACK retakes nothing.
    let mut p = auto_release();
    assert_eq!(p.feed(1550, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(p.play(1600, true), []);
    p.core.take_lines();
    assert_eq!(p.feed(1700, Input::RemoteOwnershipRequest), []);
    assert_eq!(
        p.core.take_lines(),
        ["REMOTE requested ownership; release already pending"]
    );
    assert_eq!(p.feed(1800, Input::Ownership(vec![0])), []);
}

#[test]
fn remote_request_joins_pending_auto_release() {
    // #49's Auto release joined by a remote request (#137). The buds go
    // in only after Auto, so the connect take (#324) doesn't keep the
    // ownership instead.
    let mut p = Probe::new(false, true);
    p.routing(SessionFacts {
        owner: Some(vec![1]),
        ..unworn_facts()
    });
    let all = [
        p.feed(100, Input::Auto),
        p.feed(150, Input::EarState(vec![InEar, InEar])),
        p.feed(200, Input::RemoteOwnershipRequest),
        p.feed(300, Input::Ownership(vec![0])),
        p.play(400, false),
        p.play(500, true),
        p.feed(600, Input::Ownership(vec![1])),
    ];
    assert_eq!(
        all,
        [
            vec![ConnectA2dp, SendOwnership(false), Announce, DrainPlayback],
            vec![],
            vec![],
            vec![],
            vec![],
            vec![SendOwnership(true)],
            vec![],
        ]
    );
    assert_eq!(count(&all, SendOwnership(false)), 1);
    assert_eq!(count(&all, PauseMedia), 0);
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
}

#[test]
fn remote_request_joins_pending_user_or_quit_release() {
    // A user or quit release of a live take: the request makes the one
    // pause the release's ACK would have made (#137).
    for (release, ack) in [(Input::Release, vec![]), (Input::Quit, vec![Exit])] {
        let mut p = Probe::ready();
        p.core.take_lines();
        let all = [
            p.feed(100, Input::Take),
            p.feed(200, Input::Ownership(vec![1])),
            p.feed(300, release),
            p.feed(400, Input::RemoteOwnershipRequest),
            p.feed(500, Input::Ownership(vec![0])),
        ];
        assert_eq!(
            all,
            [
                vec![SendOwnership(true)],
                vec![],
                vec![SendOwnership(false)],
                vec![PauseMedia],
                ack,
            ]
        );
        assert_eq!(count(&all, PauseMedia), 1);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 0);
    }
}

/// Drop the session at `ms` and adopt a reconnect reporting `hosts`,
/// which announces once.
fn reconnect_with(p: &mut Probe, ms: u64, hosts: &[&str]) {
    assert_eq!(p.feed(ms, Input::Disconnected), [DropSession]);
    assert_eq!(p.feed(ms + 1000, Input::Tick), [Reconnect { audio: true }]);
    let facts = SessionFacts {
        hosts: hosts.iter().map(|m| host(m)).collect(),
        ..SessionFacts::default()
    };
    let out = p.feed(ms + 1100, Input::Reconnected(facts));
    assert_eq!(announces(&out), 1, "{out:?}");
}

const R3: &str = REMOTE_3;

#[test]
fn connected_devices_two_stale_replays_after_reconnect_no_reannounce() {
    // #79: the probe adopts the reconnect, then replays its init backlog.
    let mut p = Probe::new(true, true);
    reconnect_with(&mut p, 100, &[L, R, R2]);
    assert_eq!(p.feed(1300, report(&[L], &[L, R])), []);
    // Without the snapshot exemption this one announced R2 again.
    assert_eq!(p.feed(1400, report(&[L, R], &[L, R, R2])), []);
    assert_eq!(p.feed(1500, Input::InitBacklogDrained), []);
    assert_eq!(
        p.feed(1600, report(&[L, R, R2], &[L, R, R2, R3])),
        [Announce]
    );
}

#[test]
fn connected_devices_two_stale_replays_default_order_one_announce() {
    // run_handoff: seed, pre-snapshot backlog, Auto, boundary.
    let mut p = Probe::new(false, true);
    assert_eq!(p.feed(0, report(&[], &[L, R, R2])), []);
    assert_eq!(p.feed(10, report(&[L], &[L, R])), []);
    assert_eq!(p.feed(20, report(&[L, R], &[L, R, R2])), []);
    let auto = p.feed(30, Input::Auto);
    assert_eq!(announces(&auto), 1, "{auto:?}");
    assert_eq!(p.feed(40, Input::InitBacklogDrained), []);
    assert_eq!(p.feed(50, report(&[L, R, R2], &[L, R, R2, R3])), [Announce]);
}

#[test]
fn connected_devices_snapshot_exemption_ends_at_boundary_or_disconnect() {
    // (a) After the boundary a snapshot host that leaves and rejoins is
    // announced again.
    let mut p = Probe::new(true, true);
    reconnect_with(&mut p, 100, &[L, R, R2]);
    assert_eq!(p.feed(1300, Input::InitBacklogDrained), []);
    assert_eq!(p.feed(1400, report(&[L, R, R2], &[L, R])), []);
    assert_eq!(p.feed(1500, report(&[L, R], &[L, R, R2])), [Announce]);

    // (b) A disconnect ends it without a boundary.
    let mut p = Probe::new(true, true);
    reconnect_with(&mut p, 100, &[L, R, R2]);
    reconnect_with(&mut p, 2000, &[L, R]);
    assert_eq!(p.feed(3200, Input::InitBacklogDrained), []);
    assert_eq!(p.feed(3300, report(&[L, R], &[L, R, R2])), [Announce]);

    // (c) Before any boundary a host outside the snapshot is announced.
    let mut p = Probe::new(true, true);
    reconnect_with(&mut p, 100, &[L, R, R2]);
    assert_eq!(p.feed(1300, report(&[L], &[L, R])), []);
    assert_eq!(p.feed(1400, report(&[L, R], &[L, R, R3])), [Announce]);
}

#[test]
fn connected_devices_snapshot_exemption_ends_at_auto() {
    // #150: a manual/no-audio reconnect gets no boundary; Auto ends it.
    let mut p = Probe::new(false, true);
    assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
    assert_eq!(p.feed(1100, Input::Tick), [Reconnect { audio: false }]);
    let facts = SessionFacts {
        hosts: vec![host(L), host(R)],
        ..SessionFacts::default()
    };
    let out = p.feed(1200, Input::Reconnected(facts));
    assert_eq!(announces(&out), 0, "{out:?}");
    let auto = p.feed(1300, Input::Auto);
    assert_eq!(announces(&auto), 1, "{auto:?}");
    assert_eq!(p.feed(1400, report(&[L, R], &[L])), []);
    assert_eq!(p.feed(1500, report(&[L], &[L, R])), [Announce]);
}

// #40: default sink routing.

/// `routed`, then a take for playback acknowledged at 600 ms.
fn routed_take() -> Probe {
    let mut p = routed();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), [RouteToAirPods]);
    p
}

#[test]
fn take_ack_routes_to_airpods_once() {
    let mut p = routed_take();
    // A duplicate [1] acknowledges nothing.
    assert_eq!(p.feed(700, Input::Ownership(vec![1])), []);
    assert_eq!(p.play(1000, true), []);

    // A manual take routes too.
    let mut p = Probe::new(false, true);
    p.core.default_sink_routing = true;
    p.routing(ready_facts());
    assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
    assert_eq!(p.feed(200, Input::Ownership(vec![1])), [RouteToAirPods]);
    assert_eq!(p.feed(300, Input::Ownership(vec![1])), []);
    // A release ACK routes nothing.
    assert_eq!(p.feed(400, Input::Release), [SendOwnership(false)]);
    assert_eq!(p.feed(500, Input::Ownership(vec![0])), []);
}

#[test]
fn take_ack_routes_before_a2dp_resume() {
    let mut p = routed();
    assert_eq!(p.feed(20, idle_transport()), []);
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    p.core.take_lines();
    assert_eq!(
        p.feed(600, Input::Ownership(vec![1])),
        [RouteToAirPods, ResumeA2dp]
    );
    // No new line for the route.
    let lines = p.core.take_lines();
    assert_eq!(lines.len(), 2, "{lines:?}");
}

#[test]
fn genuine_loss_routes_away_once() {
    let mut p = routed_take();
    p.core.take_lines();
    assert_eq!(
        p.feed(1100, Input::Ownership(vec![0])),
        [PauseMedia, RouteAway]
    );
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
    assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);
    assert_eq!(p.feed(1300, Input::RemoteOwnershipRequest), []);
}

#[test]
fn remote_request_routes_away_once() {
    // A fresh yield: pause, route away, then "not owning".
    let mut p = routed_take();
    assert_eq!(
        p.feed(1100, Input::RemoteOwnershipRequest),
        [PauseMedia, RouteAway, SendOwnership(false)]
    );
    // A repeat joins the remote release; its ACK routes nothing more.
    assert_eq!(p.feed(1150, Input::RemoteOwnershipRequest), []);
    assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);
    assert_eq!(p.feed(1300, Input::RemoteOwnershipRequest), []);

    // Joining a pending user release loses the take too.
    let mut p = routed_take();
    assert_eq!(p.feed(1000, Input::Release), [SendOwnership(false)]);
    assert_eq!(
        p.feed(1100, Input::RemoteOwnershipRequest),
        [PauseMedia, RouteAway]
    );
    assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);

    // Refused during a local call: Linux keeps the AirPods.
    let mut p = routed_take();
    assert_eq!(p.feed(700, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(p.feed(1100, Input::RemoteOwnershipRequest), []);
}

#[test]
fn timed_out_remote_release_then_loss_routes_away_once() {
    let mut p = routed_take();
    assert_eq!(
        p.feed(1100, Input::RemoteOwnershipRequest),
        [PauseMedia, RouteAway, SendOwnership(false)]
    );
    assert_eq!(p.play(1500, false), []);
    assert_eq!(p.feed(6200, Input::Tick), []);
    // Past LATE_ACK_WINDOW: a genuine loss of the same take.
    assert_eq!(p.feed(11101, Input::Ownership(vec![0])), [PauseMedia]);
    // The next take routes and can route away again.
    assert_eq!(p.play(12000, true), [SendOwnership(true)]);
    assert_eq!(p.feed(12100, Input::Ownership(vec![1])), [RouteToAirPods]);
    assert_eq!(
        p.feed(12500, Input::Ownership(vec![0])),
        [PauseMedia, RouteAway]
    );
}

#[test]
fn retake_lost_before_ack_routes_away() {
    let mut p = routed_take();
    assert_eq!(
        p.feed(1100, Input::Ownership(vec![0])),
        [PauseMedia, RouteAway]
    );
    assert_eq!(p.play(1500, false), []);
    assert_eq!(p.play(2000, true), [SendOwnership(true)]);
    // Lost before its ACK: this take routes away too.
    assert_eq!(
        p.feed(2100, Input::Ownership(vec![0])),
        [PauseMedia, RouteAway]
    );
}

#[test]
fn stop_while_owning_keeps_the_route() {
    let mut p = routed_take();
    assert_eq!(p.play(1000, false), []);
    // Linux keeps the AirPods idle (#262): no RouteAway, and the next
    // play edge routes nothing again.
    p.idle_ticks(1000);
    assert_eq!(p.play(61_500, true), []);
    assert_eq!(p.feed(61_600, Input::Ownership(vec![1])), []);
}

#[test]
fn routing_disabled_emits_no_route_commands() {
    // Take with an idle transport, then a genuine loss.
    let mut p = Probe::ready();
    let mut all = p.feed(20, idle_transport());
    all.extend(p.play(500, true));
    all.extend(p.feed(600, Input::Ownership(vec![1])));
    all.extend(p.feed(1100, Input::Ownership(vec![0])));
    assert_eq!(all, [SendOwnership(true), ResumeA2dp, PauseMedia]);
    // Take, remote request, its ACK.
    let mut p = Probe::ready();
    let mut all = p.play(500, true);
    all.extend(p.feed(600, Input::Ownership(vec![1])));
    all.extend(p.feed(1100, Input::RemoteOwnershipRequest));
    all.extend(p.feed(1200, Input::Ownership(vec![0])));
    assert_eq!(all, [SendOwnership(true), PauseMedia, SendOwnership(false)]);
    // Take, a user release joined by a remote request, its ACK.
    let mut p = Probe::ready();
    let mut all = p.play(500, true);
    all.extend(p.feed(600, Input::Ownership(vec![1])));
    all.extend(p.feed(1000, Input::Release));
    all.extend(p.feed(1100, Input::RemoteOwnershipRequest));
    all.extend(p.feed(1200, Input::Ownership(vec![0])));
    assert_eq!(all, [SendOwnership(true), SendOwnership(false), PauseMedia]);
}

// #262: Linux stays the idle owner, as the Mac does.

/// `relaying` (and routing if asked), a take for 301 acknowledged at
/// 600 ms with the transport active, then a stop at 1000 ms.
fn idle_owner(routing: bool) -> Probe {
    let mut p = relaying();
    p.core.default_sink_routing = routing;
    assert_eq!(p.feed(20, Input::A2dpTransport { active: true }), []);
    assert_eq!(
        p.play_category(500, MEDIA),
        [RelayCategory(MEDIA), SendOwnership(true)]
    );
    let route = if routing {
        vec![RouteToAirPods]
    } else {
        vec![]
    };
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), route);
    // The stop only relays 100.
    assert_eq!(p.play_category(1000, IDLE), [RelayCategory(IDLE)]);
    p
}

#[test]
fn stop_keeps_ownership_as_idle_owner() {
    let mut p = idle_owner(false);
    p.core.take_lines();
    // No release and no second relay of 100, however long it stays
    // stopped.
    for ms in (1000..=61_000).step_by(100) {
        assert_eq!(p.feed(ms, Input::Tick), [], "{ms}");
        assert_eq!(p.play_category(ms, IDLE), [], "{ms}");
    }
    assert_eq!(count_lines(&p.core.take_lines(), "AUTO"), 0);
}

#[test]
fn play_while_idle_owner_sends_no_take() {
    // A play edge soon after the stop, and minutes later.
    for play in [2000, 1000 + 120_000] {
        let mut p = idle_owner(false);
        // The idle minute of ticks fits only before the later edge.
        if play > 61_000 {
            p.idle_ticks(1000);
        }
        p.core.take_lines();
        assert_eq!(p.play_category(play, MEDIA), [RelayCategory(MEDIA)]);
        assert_eq!(
            p.core.take_lines(),
            ["AUTO playback started; Linux already owns"]
        );
        assert_eq!(p.play_category(play + 500, IDLE), [RelayCategory(IDLE)]);
        assert_eq!(p.play_category(play + 1000, MEDIA), [RelayCategory(MEDIA)]);
        p.idle_ticks(play + 1000);
    }
}

#[test]
fn play_after_timed_out_take_retakes() {
    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(5600, Input::Tick), []);
    assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);
    // The owner is still [0]: the take is unconfirmed, so a play edge
    // takes afresh.
    assert_eq!(p.play(6000, false), []);
    assert_eq!(p.play(6500, true), [SendOwnership(true)]);
    assert_eq!(
        count_lines(&p.core.take_lines(), "AUTO playback started; requested"),
        1
    );
}

#[test]
fn idle_owner_loss_yields_once() {
    for (routing, yielded) in [
        (false, vec![PauseMedia]),
        (true, vec![PauseMedia, RouteAway]),
    ] {
        let mut p = idle_owner(routing);
        p.core.take_lines();
        assert_eq!(p.feed(2000, Input::Ownership(vec![0])), yielded);
        assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
        p.idle_ticks(2000);
        assert_eq!(p.feed(62_100, Input::Ownership(vec![0])), []);
        // An idle remote: the next play edge takes afresh.
        assert_eq!(
            p.play_category(63_000, MEDIA),
            [RelayCategory(MEDIA), SendOwnership(true)]
        );
    }
}

#[test]
fn idle_owner_remote_request_yields_once() {
    for (routing, yielded) in [
        (false, vec![PauseMedia, SendOwnership(false)]),
        (true, vec![PauseMedia, RouteAway, SendOwnership(false)]),
    ] {
        let mut p = idle_owner(routing);
        p.core.take_lines();
        assert_eq!(p.feed(2000, Input::RemoteOwnershipRequest), yielded);
        assert_eq!(p.feed(2100, Input::Ownership(vec![0])), []);
        p.idle_ticks(2100);
        let lines = p.core.take_lines();
        assert_eq!(
            count_lines(&lines, "REMOTE requested ownership; yielding"),
            1
        );
        assert_eq!(count_lines(&lines, "YIELD"), 0);
        assert_eq!(
            p.play_category(63_000, MEDIA),
            [RelayCategory(MEDIA), SendOwnership(true)]
        );
    }
}

// #253: mode toggles after a stop keep Linux the idle owner (#262).

#[test]
fn manual_after_stop_keeps_ownership() {
    let mut p = acked_auto_take();
    assert_eq!(p.play(2000, false), []);
    assert_eq!(p.feed(3000, Input::Manual), []);
    for ms in (3000..=63_000).step_by(100) {
        assert_eq!(p.feed(ms, Input::Tick), [], "{ms}");
        assert_eq!(p.play(ms, false), [], "{ms}");
    }
    // Back to automatic: no "not owning" for the take Linux holds.
    assert_eq!(
        p.feed(64_000, Input::Auto),
        [ConnectA2dp, Announce, DrainPlayback]
    );
    p.idle_ticks(64_000);
}

#[test]
fn auto_after_stop_keeps_ownership() {
    let mut p = acked_auto_take();
    assert_eq!(p.play(2000, false), []);
    assert_eq!(
        p.feed(3000, Input::Auto),
        [ConnectA2dp, Announce, DrainPlayback]
    );
    // Stopped samples after the new baseline release nothing either.
    for ms in (3050..=63_050).step_by(100) {
        assert_eq!(p.feed(ms, Input::Tick), [], "{ms}");
        assert_eq!(p.play(ms, false), [], "{ms}");
    }
}
