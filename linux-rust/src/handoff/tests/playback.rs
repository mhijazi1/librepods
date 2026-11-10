use super::*;
use crate::handoff_log::{Kind, classify};

#[test]
fn takes_on_new_playback_when_worn() {
    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    assert_eq!(p.play(1000, true), []);
    // Stopping keeps the AirPods: Linux is the idle owner (#262).
    assert_eq!(p.play(1500, false), []);
    p.idle_ticks(1500);
    assert_eq!(p.play(62_000, false), []);
}

#[test]
fn takes_with_three_or_four_hosts() {
    // Linux plus two or three remotes (hardware #22: iPhone + Mac +
    // Linux) takes like Linux plus one.
    let hosts_lists = [
        vec![host(LOCAL), host(REMOTE), host(REMOTE_2)],
        vec![host(REMOTE), host(LOCAL), host(REMOTE_2), host(REMOTE_3)],
    ];
    for hosts in hosts_lists {
        let n = hosts.len();
        let mut p = Probe::ready_with_hosts(hosts.clone());
        assert_eq!(p.play(500, true), [SendOwnership(true)], "hosts={n}");

        let mut p = Probe::new(false, true);
        p.routing(SessionFacts {
            hosts,
            ..ready_facts()
        });
        p.core.take_lines();
        assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)], "hosts={n}");
        let lines = p.core.take_lines();
        assert!(
            lines.iter().any(|l| l.starts_with("REQUEST owner=linux")),
            "hosts={n}: {lines:?}"
        );
    }
}

#[test]
fn no_take_when_only_linux_hosts() {
    let mut p = Probe::ready_with_hosts(vec![host(LOCAL)]);
    assert_eq!(p.play(500, true), []);
    p.core.take_lines();
    assert_eq!(p.feed(600, Input::Take), []);
    let lines = p.core.take_lines();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        lines[0].starts_with("BLOCKED take needs Linux plus a remote host"),
        "{lines:?}"
    );
}

#[test]
fn no_take_when_not_worn() {
    let mut p = Probe::ready();
    p.feed(20, Input::EarState(vec![OutOfEar, InCase]));
    assert_eq!(p.play(500, true), []);
    assert_eq!(p.play(1000, false), []);
    assert_eq!(p.feed(1100, Input::Take), []);
    assert!(p.core.take_lines().iter().any(|l| l.starts_with("BLOCKED")));
    // Putting a bud in makes the next new playback take.
    p.feed(1200, Input::EarState(vec![OutOfEar, InEar]));
    assert_eq!(p.play(1500, true), [SendOwnership(true)]);
}

#[test]
fn no_take_when_remote_in_call() {
    let mut p = Probe::ready();
    p.feed(
        20,
        Input::AudioSource(source(REMOTE, AudioSourceType::Call)),
    );
    assert_eq!(p.play(500, true), []);
    assert_eq!(p.play(1000, false), []);
    // Remote media blocks too; local media does not.
    p.feed(
        1100,
        Input::AudioSource(source(REMOTE, AudioSourceType::Media)),
    );
    assert_eq!(p.play(1500, true), []);
    assert_eq!(p.play(2000, false), []);
    assert_eq!(p.feed(2100, Input::Take), []);
    p.feed(
        2200,
        Input::AudioSource(source(LOCAL, AudioSourceType::Media)),
    );
    assert_eq!(p.play(2500, true), [SendOwnership(true)]);
}

#[test]
fn no_take_on_stale_state() {
    // No session at all.
    let mut p = Probe::new(true, false);
    p.routing(ready_facts());
    assert_eq!(p.play(10, false), []);
    assert_eq!(p.play(500, true), []);

    // Hosts that are not this machine plus a remote one: Linux alone,
    // then remotes without Linux.
    let mut p = Probe::ready();
    p.feed(
        20,
        Input::ConnectedDevices {
            old: two_hosts(),
            new: vec![host(LOCAL)],
        },
    );
    assert_eq!(p.play(500, true), []);
    assert_eq!(p.play(1000, false), []);
    let remotes_only = vec![host(REMOTE), host(REMOTE_2)];
    assert_eq!(
        p.feed(
            1100,
            Input::ConnectedDevices {
                old: vec![host(LOCAL)],
                new: remotes_only.clone(),
            },
        ),
        [Announce]
    );
    assert_eq!(p.play(1500, true), []);
    assert_eq!(p.play(2000, false), []);
    assert_eq!(
        p.feed(
            2100,
            Input::ConnectedDevices {
                old: remotes_only,
                new: two_hosts(),
            },
        ),
        []
    );

    // A disconnect clears wear and audio; a reconnect that has not
    // reported them yet must not take.
    assert_eq!(p.feed(2200, Input::Disconnected), [DropSession]);
    assert_eq!(p.feed(2300, Input::Take), []);
    assert_eq!(p.play(2500, true), []);
    assert_eq!(p.feed(3000, Input::Tick), []);
    assert_eq!(p.feed(3200, Input::Tick), [Reconnect { audio: true }]);
    let stale = SessionFacts {
        hosts: two_hosts(),
        ..SessionFacts::default()
    };
    assert_eq!(
        p.feed(3300, Input::Reconnected(stale)),
        [AdoptSession, Announce, DrainPlayback]
    );
    assert_eq!(p.play(3500, false), []);
    assert_eq!(p.play(4000, true), []);
    assert_eq!(p.feed(4100, Input::Take), []);
}

#[test]
fn baseline_ignores_preexisting_playback() {
    // Auto mode starts while something is already playing. Buds out at
    // the start, so no connect take (#324) hides the playback take.
    let mut p = Probe::new(false, true);
    p.routing(unworn_facts());
    assert_eq!(
        p.feed(1000, Input::Auto),
        [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
    );
    p.wear_in(1000);
    // The reply to Auto's "not owning" (#54).
    assert_eq!(p.feed(1050, Input::Ownership(vec![0])), []);
    // A stopped sample from before the baseline is not an observation.
    assert_eq!(p.stale_stop(1050, 900), []);
    assert_eq!(p.play(1100, true), []);
    assert_eq!(p.play(1600, true), []);
    assert_eq!(p.play(2100, false), []);
    assert_eq!(p.play(2600, true), [SendOwnership(true)]);
    assert_eq!(p.feed(2700, Input::Ownership(vec![1])), []);

    // The same after a reconnect.
    assert_eq!(p.feed(3000, Input::Disconnected), [PauseMedia, DropSession]);
    assert_eq!(p.feed(4000, Input::Tick), [Reconnect { audio: true }]);
    assert_eq!(
        p.feed(5000, Input::Reconnected(unworn_facts())),
        [AdoptSession, Announce, DrainPlayback]
    );
    p.wear_in(5000);
    assert_eq!(p.stale_stop(5000, 4900), []);
    assert_eq!(p.play(5100, true), []);
    assert_eq!(p.play(5600, true), []);
    assert_eq!(p.play(6100, false), []);
    assert_eq!(p.play(6600, true), [SendOwnership(true)]);
}

// "Connect to this computer" (#16).

/// `Probe::ready`, with its core built in `mode`.
fn ready_in(mode: ConnectMode) -> Probe {
    let t0 = Instant::now();
    let mut p = Probe {
        core: Handoff::new(LOCAL.to_string(), t0, true, true).with_connect_mode(mode),
        t0,
        last_ms: 0,
    };
    p.routing(ready_facts());
    assert_eq!(p.play(10, false), []);
    p
}

/// Connect-mode skip lines since the last call; each classifies as Other.
fn skips(p: &mut Probe) -> usize {
    let lines = p.core.take_lines();
    let skipped: Vec<_> = lines.iter().filter(|l| *l == CONNECT_MODE_SKIP).collect();
    for line in &skipped {
        assert!(line.starts_with("AUTO "), "{line}");
        assert_eq!(classify(line), Kind::Other, "{line}");
    }
    skipped.len()
}

#[test]
fn connect_mode_automatically_takes_on_new_playback() {
    for mut p in [Probe::ready(), ready_in(Automatically)] {
        assert_eq!(p.play(500, true), [SendOwnership(true)]);
        assert_eq!(skips(&mut p), 0);
    }
    // Another host taking first changes nothing in this mode.
    let mut p = ready_in(Automatically);
    assert_eq!(
        p.feed(
            100,
            Input::AudioSource(source(REMOTE, AudioSourceType::Media))
        ),
        []
    );
    assert_eq!(p.feed(200, Input::AudioSource(idle())), []);
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
}

#[test]
fn when_last_connected_skips_when_linux_was_not_last_owner() {
    let mut p = ready_in(WhenLastConnected);
    p.core.take_lines();
    assert_eq!(p.play(500, true), []);
    assert_eq!(skips(&mut p), 1);
    // Nothing is pending or held: no timeout, no release on stop.
    assert_eq!(p.feed(6000, Input::Tick), []);
    assert_eq!(p.play(6500, false), []);
    assert_eq!(p.play(7000, true), []);
    assert_eq!(skips(&mut p), 1);
}

#[test]
fn when_last_connected_takes_when_linux_was_last_owner() {
    let mut p = ready_in(WhenLastConnected);
    // A manual take's ACK makes Linux the last owner.
    assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
    assert_eq!(p.feed(200, Input::Ownership(vec![1])), []);
    assert_eq!(p.feed(300, Input::Release), [SendOwnership(false)]);
    assert_eq!(p.feed(400, Input::Ownership(vec![0])), [PauseMedia]);
    // Linux's own releases keep it the last owner, and a stop keeps the
    // take Linux holds (#262).
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    assert_eq!(p.play(1000, false), []);
    p.idle_ticks(1000);
    assert_eq!(p.play(61_500, true), []);
    assert_eq!(skips(&mut p), 0);

    // An OwnsConnection=1 report alone does too.
    let mut p = ready_in(WhenLastConnected);
    assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(skips(&mut p), 0);
}

/// WhenLastConnected, Linux the last owner after an acknowledged take
/// for playback still running at 1000 ms.
fn last_owner_take() -> Probe {
    let mut p = ready_in(WhenLastConnected);
    assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    assert_eq!(p.play(1000, true), []);
    p.core.take_lines();
    p
}

#[test]
fn when_last_connected_skips_after_remote_host_took() {
    // A genuine loss: another host took the AirPods.
    let mut p = last_owner_take();
    assert_eq!(p.feed(1100, Input::Ownership(vec![0])), [PauseMedia]);
    assert_eq!(p.play(1500, false), []);
    assert_eq!(p.play(2000, true), []);
    assert_eq!(skips(&mut p), 1);

    // A remote request Linux yields to.
    let mut p = last_owner_take();
    assert_eq!(
        p.feed(1100, Input::RemoteOwnershipRequest),
        [PauseMedia, SendOwnership(false)]
    );
    assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);
    assert_eq!(p.play(1500, false), []);
    assert_eq!(p.play(2000, true), []);
    assert_eq!(skips(&mut p), 1);

    // A remote request after a stop: Linux, the idle owner, yields.
    let mut p = last_owner_take();
    assert_eq!(p.play(1100, false), []);
    assert_eq!(
        p.feed(1200, Input::RemoteOwnershipRequest),
        [PauseMedia, SendOwnership(false)]
    );
    assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
    assert_eq!(p.play(2000, true), []);
    assert_eq!(skips(&mut p), 1);

    // A remote request joining Linux's pending user release.
    let mut p = last_owner_take();
    assert_eq!(p.feed(1100, Input::Release), [SendOwnership(false)]);
    assert_eq!(p.feed(1200, Input::RemoteOwnershipRequest), [PauseMedia]);
    assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
    assert_eq!(p.play(1400, false), []);
    assert_eq!(p.play(2000, true), []);
    assert_eq!(skips(&mut p), 1);

    // A remote request after Linux's user release was acknowledged.
    let mut p = ready_in(WhenLastConnected);
    assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
    assert_eq!(p.feed(200, Input::Ownership(vec![1])), []);
    assert_eq!(p.feed(300, Input::Release), [SendOwnership(false)]);
    assert_eq!(p.feed(400, Input::Ownership(vec![0])), [PauseMedia]);
    assert_eq!(p.feed(500, Input::RemoteOwnershipRequest), []);
    assert_eq!(p.play(1000, true), []);
    assert_eq!(skips(&mut p), 1);

    // A remote host already given the AirPods asks again.
    let mut p = ready_in(WhenLastConnected);
    assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
    assert_eq!(p.feed(30, Input::Ownership(vec![0])), []);
    assert_eq!(p.feed(100, Input::RemoteOwnershipRequest), []);
    assert_eq!(p.play(500, true), []);
    assert_eq!(skips(&mut p), 1);

    // Remote media or a remote call.
    for r#type in [AudioSourceType::Media, AudioSourceType::Call] {
        let mut p = ready_in(WhenLastConnected);
        assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
        assert_eq!(p.feed(100, Input::AudioSource(source(REMOTE, r#type))), []);
        assert_eq!(p.feed(200, Input::AudioSource(idle())), []);
        p.core.take_lines();
        assert_eq!(p.play(500, true), [], "{type:?}");
        assert_eq!(skips(&mut p), 1, "{type:?}");
    }

    // Linux's own media and remote idle audio leave it the last owner.
    let mut p = ready_in(WhenLastConnected);
    assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
    assert_eq!(
        p.feed(
            100,
            Input::AudioSource(source(LOCAL, AudioSourceType::Media))
        ),
        []
    );
    assert_eq!(
        p.feed(
            150,
            Input::AudioSource(source(REMOTE, AudioSourceType::None))
        ),
        []
    );
    assert_eq!(p.feed(200, Input::AudioSource(idle())), []);
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
}

#[test]
fn when_last_connected_refused_remote_request_keeps_last_owner() {
    // A local call refuses the request: Linux keeps the AirPods. As in
    // `local_call_refuses_remote_request`, the iPhone's media comes
    // first.
    let mut p = last_owner_take();
    assert_eq!(p.feed(1100, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(
        p.feed(
            1150,
            Input::AudioSource(source(REMOTE, AudioSourceType::Media))
        ),
        []
    );
    assert_eq!(p.feed(1200, Input::RemoteOwnershipRequest), []);
    assert_eq!(
        p.feed(1300, Input::LocalCall(false)),
        [AdvertiseCall(false)]
    );
    assert_eq!(p.feed(1350, Input::AudioSource(idle())), []);
    assert_eq!(p.play(1500, false), []);
    // Still the last owner: once Linux gives the AirPods up, a new play
    // edge takes them back.
    assert_eq!(p.feed(1550, Input::Release), [SendOwnership(false)]);
    assert_eq!(p.feed(1600, Input::Ownership(vec![0])), [PauseMedia]);
    assert_eq!(p.play(2000, true), [SendOwnership(true)]);
}

/// Disconnect with the case closed, then new playback that reconnects
/// with `facts` at 1200 ms; returns the reconnect's commands.
fn case_reconnect(p: &mut Probe, facts: SessionFacts) -> Vec<Command> {
    p.feed(30, Input::EarState(vec![InCase, InCase]));
    assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
    assert_eq!(p.play(500, false), []);
    assert_eq!(p.play(1000, true), []);
    assert_eq!(p.feed(1100, Input::Tick), [Reconnect { audio: true }]);
    p.core.take_lines();
    p.feed(1200, Input::Reconnected(facts))
}

#[test]
fn when_last_connected_last_owner_survives_reconnect() {
    // Linux was the last owner before the disconnect: the reconnect for
    // playback takes.
    let mut p = ready_in(WhenLastConnected);
    assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
    assert_eq!(
        case_reconnect(&mut p, ready_facts()),
        [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
    );
    assert_eq!(skips(&mut p), 0);
    // The stale init [0] stays ignored, and the take settles.
    assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
    assert_eq!(p.feed(1400, Input::Ownership(vec![1])), []);

    // It was not: the reconnect adopts the session but takes nothing,
    // and leaves no take to release or time out.
    let mut p = ready_in(WhenLastConnected);
    assert_eq!(
        case_reconnect(&mut p, ready_facts()),
        [AdoptSession, Announce, DrainPlayback]
    );
    assert_eq!(skips(&mut p), 1);
    assert_eq!(p.play(1500, false), []);
    assert_eq!(p.feed(9000, Input::Tick), []);

    // A session snapshot where Linux owns makes it the last owner.
    let mut p = ready_in(WhenLastConnected);
    let facts = SessionFacts {
        owner: Some(vec![1]),
        ..ready_facts()
    };
    assert_eq!(
        case_reconnect(&mut p, facts),
        [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
    );
    assert_eq!(skips(&mut p), 0);
}

#[test]
fn when_last_connected_reconcile_retake_gated() {
    // A play edge during Linux's release retakes once it settles only
    // while Linux is the last owner: here a remote host's media cleared
    // it. Auto's release: Linux owns ([1]) without a take, and the buds
    // are out at Auto, so the connect take (#324) doesn't keep it.
    let released = || {
        let mut p = ready_in(WhenLastConnected);
        assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
        assert_eq!(p.feed(30, Input::EarState(vec![OutOfEar, OutOfEar])), []);
        assert_eq!(
            p.feed(100, Input::Auto),
            [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
        );
        p.wear_in(150);
        assert_eq!(p.play(1100, false), []);
        p.core.take_lines();
        p
    };
    let mut p = released();
    assert_eq!(
        p.feed(
            1150,
            Input::AudioSource(source(REMOTE, AudioSourceType::Media))
        ),
        []
    );
    assert_eq!(p.feed(1160, Input::AudioSource(idle())), []);
    assert_eq!(p.play(1200, true), []);
    assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
    assert_eq!(skips(&mut p), 1);

    // Still the last owner: the same edge retakes.
    let mut p = released();
    assert_eq!(p.play(1200, true), []);
    assert_eq!(
        p.feed(1300, Input::Ownership(vec![0])),
        [SendOwnership(true)]
    );
    assert_eq!(skips(&mut p), 0);
}

#[test]
fn when_last_connected_manual_take_not_gated() {
    let mut p = ready_in(WhenLastConnected);
    p.core.take_lines();
    assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
    assert_eq!(skips(&mut p), 0);
    assert_eq!(p.feed(200, Input::Ownership(vec![1])), []);
}

#[test]
fn when_last_connected_late_take_ack_after_remote_request_skips() {
    // A take's ACK [1] arrives after Linux yielded to a remote request;
    // the [0] that acknowledges the remote release still clears it.
    let mut p = ready_in(WhenLastConnected);
    assert_eq!(p.feed(100, Input::Take), [SendOwnership(true)]);
    assert_eq!(
        p.feed(200, Input::RemoteOwnershipRequest),
        [PauseMedia, SendOwnership(false)]
    );
    assert_eq!(p.feed(300, Input::Ownership(vec![1])), []);
    assert_eq!(p.feed(400, Input::Ownership(vec![0])), []);
    p.core.take_lines();
    assert_eq!(p.play(1000, true), []);
    assert_eq!(skips(&mut p), 1);
}

#[test]
fn when_last_connected_late_remote_release_ack_skips() {
    // The same, with the remote release's [0] after its timeout (#55).
    let mut p = last_owner_take();
    assert_eq!(
        p.feed(1100, Input::RemoteOwnershipRequest),
        [PauseMedia, SendOwnership(false)]
    );
    assert_eq!(p.feed(1150, Input::Ownership(vec![1])), []);
    assert_eq!(p.play(1500, false), []);
    assert_eq!(p.feed(6200, Input::Tick), []);
    assert_eq!(p.feed(6300, Input::Ownership(vec![0])), []);
    assert_eq!(
        count_lines(&p.core.take_lines(), "ACK owner=not-linux late"),
        1
    );
    assert_eq!(p.play(7000, true), []);
    assert_eq!(skips(&mut p), 1);
}

#[test]
fn when_last_connected_unsolicited_loss_skips() {
    // Owner [1] without a Linux take, then [0]: another host took them.
    let mut p = ready_in(WhenLastConnected);
    assert_eq!(p.feed(20, Input::Ownership(vec![1])), []);
    assert_eq!(p.feed(30, Input::Ownership(vec![0])), []);
    p.core.take_lines();
    assert_eq!(p.play(500, true), []);
    assert_eq!(skips(&mut p), 1);

    // A repeated [0] after Linux's own release leaves it the last owner.
    let mut p = last_owner_take();
    assert_eq!(p.feed(1100, Input::Release), [SendOwnership(false)]);
    assert_eq!(p.feed(1200, Input::Ownership(vec![0])), [PauseMedia]);
    assert_eq!(p.feed(1300, Input::Ownership(vec![0])), []);
    assert_eq!(p.play(1400, false), []);
    assert_eq!(p.play(1500, true), [SendOwnership(true)]);
    assert_eq!(skips(&mut p), 0);
}

#[test]
fn when_last_connected_seeded_last_owner_takes() {
    // A new core for the same AirPods (the shell's per-MAC store, #16)
    // with seed facts saying owner [0] and no remote audio.
    for (seeded, playback) in [(true, vec![SendOwnership(true)]), (false, vec![])] {
        let t0 = Instant::now();
        let mut p = Probe {
            core: Handoff::new(LOCAL.to_string(), t0, true, true)
                .with_connect_mode(WhenLastConnected)
                .with_last_owner_linux(seeded),
            t0,
            last_ms: 0,
        };
        p.routing(ready_facts());
        assert_eq!(p.play(10, false), []);
        p.core.take_lines();
        assert_eq!(p.play(500, true), playback, "seeded={seeded}");
        assert_eq!(skips(&mut p), usize::from(!seeded), "seeded={seeded}");
        assert_eq!(p.core.last_owner_linux(), seeded);
    }
}

#[test]
fn category_rises_are_fresh_requests() {
    // 100 -> 201 and 100 -> 301 take from an idle owner.
    for category in [STREAM, MEDIA] {
        let mut p = Probe::ready();
        assert_eq!(
            p.play_category(500, category),
            [SendOwnership(true)],
            "{category}"
        );
    }
    // Against a media owner a stream is skipped; MPRIS (201 -> 301) takes.
    let mut p = remote_playing(None);
    assert_eq!(p.play_category(500, STREAM), []);
    assert_eq!(
        p.core.take_lines(),
        ["AUTO takeover skipped: the audio source's category 301 is above Linux's 201"]
    );
    assert_eq!(p.play_category(1000, STREAM), []);
    assert_eq!(p.play_category(1500, MEDIA), [SendOwnership(true)]);
    assert_eq!(p.play_category(2000, MEDIA), []);
}

/// #96 with #262: Linux took for MPRIS while a stream counted; neither
/// stop sends ownership, and only the fall to 100 relays 100.
#[test]
fn fresh_source_stop_behind_stream_keeps_ownership() {
    let mut p = remote_playing(Some(MEDIA));
    p.core.category_relay = true;
    assert_eq!(
        p.play_source(500, STREAM, Some(STREAM)),
        [RelayCategory(STREAM)]
    );
    assert_eq!(
        p.play_source(1000, MEDIA, Some(MEDIA)),
        [RelayCategory(MEDIA), SendOwnership(true)]
    );
    assert_eq!(p.feed(1100, Input::Ownership(vec![1])), []);
    // MPRIS stops; the stream still counts.
    assert_eq!(p.play_category(1500, STREAM), [RelayCategory(STREAM)]);
    // The stream stops counting.
    assert_eq!(p.play_category(2000, IDLE), [RelayCategory(IDLE)]);
    p.idle_ticks(2000);
}

/// #421: while disconnected with the case closed, a new source behind
/// playback is new playback and requests the reconnection.
#[test]
fn fresh_source_behind_playback_requests_case_reconnect() {
    let mut p = Probe::new(true, false);
    assert_eq!(p.play_category(10, STREAM), []);
    assert_eq!(p.core.take_lines(), Vec::<String>::new());
    assert_eq!(p.play_source(500, STREAM, Some(STREAM)), []);
    assert_eq!(
        p.core.take_lines(),
        ["AUTO new playback requested reconnection after case closure"]
    );
    assert_eq!(p.feed(600, Input::Tick), [Reconnect { audio: true }]);
}

/// `Probe::ready` with the case closed, then disconnected at 100.
fn case_closed_disconnected() -> Probe {
    let mut p = Probe::ready();
    p.feed(20, Input::EarState(vec![InCase, InCase]));
    assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
    p.core.take_lines();
    p
}

/// `ready_facts` with the other host playing media (301).
fn remote_media_facts() -> SessionFacts {
    SessionFacts {
        audio: Some(source(REMOTE, AudioSourceType::Media)),
        ..ready_facts()
    }
}

/// From `case_closed_disconnected`, the playback samples
/// `(category, new_source)` from 200 ms on, then the reconnect they
/// requested with `facts`: its commands and lines.
fn requested_reconnect(
    samples: &[(u32, Option<u32>)],
    facts: SessionFacts,
) -> (Vec<Command>, Vec<String>) {
    let mut p = case_closed_disconnected();
    let mut ms = 200;
    for &(category, new_source) in samples {
        assert_eq!(p.play_source(ms, category, new_source), []);
        ms += 500;
    }
    assert_eq!(p.feed(ms, Input::Tick), [Reconnect { audio: true }]);
    p.core.take_lines();
    let out = p.feed(ms + 100, Input::Reconnected(facts));
    (out, p.core.take_lines())
}

/// #421 with #96 c4: the reconnect take compares at the behind source's
/// category, not the aggregate's.
#[test]
fn case_reconnect_takes_at_behind_source_category() {
    let blocked =
        "AUTO reconnected; takeover blocked by remote activity, wear, or missing host state";
    // The aggregate falls to the stream source's 201, or stays at 301
    // with the 201 source behind it.
    for samples in [
        [(MEDIA, None), (STREAM, Some(STREAM))],
        [(MEDIA, None), (MEDIA, Some(STREAM))],
    ] {
        let (out, lines) = requested_reconnect(&samples, remote_media_facts());
        assert_eq!(out, [AdoptSession, Announce, DrainPlayback], "{samples:?}");
        assert!(lines.contains(&blocked.into()), "{lines:?}");
        assert_eq!(
            requested_reconnect(&samples, ready_facts()).0,
            [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
        );
    }
}

/// #421: a start or rise counts as the aggregate, the highest request
/// wins, the aggregate caps it, and a withdrawal forgets it.
#[test]
fn case_reconnect_category_follows_requests() {
    let takes = |samples: &[(u32, Option<u32>)]| {
        requested_reconnect(samples, remote_media_facts())
            .0
            .contains(&SendOwnership(true))
    };
    // (a) A rise to 301 after a 201 source asked.
    assert!(takes(&[
        (STREAM, None),
        (STREAM, Some(STREAM)),
        (MEDIA, None)
    ]));
    // (b) A 301 source asked, then the aggregate fell to 201.
    assert!(!takes(&[
        (MEDIA, None),
        (MEDIA, Some(MEDIA)),
        (STREAM, None)
    ]));
    // (c) A 201 source asked, the stop withdrew it, a 301 start asked.
    assert!(takes(&[
        (STREAM, None),
        (STREAM, Some(STREAM)),
        (IDLE, None),
        (MEDIA, None)
    ]));
    // (d) A 301 start asked, then a 201 source: the higher request wins.
    assert!(takes(&[(IDLE, None), (MEDIA, None), (MEDIA, Some(STREAM))]));
}

/// #421: steady playback while disconnected requests nothing.
#[test]
fn steady_playback_while_case_closed_requests_nothing() {
    let mut p = case_closed_disconnected();
    for ms in (200..5000).step_by(500) {
        assert_eq!(p.play_category(ms, STREAM), []);
        assert_eq!(p.play_source(ms + 250, STREAM, None), []);
    }
    assert_eq!(p.core.take_lines(), Vec::<String>::new());
    for ms in (5000..=70_000).step_by(100) {
        assert_eq!(p.feed(ms, Input::Tick), [], "{ms}");
    }
}

/// #96: a new stream behind MPRIS during Auto's pending release is
/// decided at 201 once the release settles, not at Linux's 301.
#[test]
fn fresh_source_during_release() {
    let run = |owner| {
        // No wear report at Auto, so no connect take (#324).
        let mut p = Probe::new(false, true);
        p.routing(SessionFacts {
            owner: Some(vec![1]),
            ..unworn_facts()
        });
        assert_eq!(
            p.feed(100, Input::Auto),
            [ConnectA2dp, SendOwnership(false), Announce, DrainPlayback]
        );
        p.wear_in(140);
        assert_eq!(p.feed(150, remote_media()), []);
        assert_eq!(p.feed(160, relay(REMOTE, owner)), []);
        assert_eq!(p.play_category(200, MEDIA), []);
        assert_eq!(p.play_source(500, MEDIA, Some(STREAM)), []);
        p.core.take_lines();
        let ack = p.feed(600, Input::Ownership(vec![0]));
        (ack, p.core.take_lines())
    };
    let (ack, lines) = run(MEDIA);
    assert_eq!(ack, []);
    assert_eq!(count_lines(&lines, "AUTO takeover blocked"), 1);
    let (ack, lines) = run(IDLE);
    assert_eq!(ack, [SendOwnership(true)]);
    assert_eq!(
        count_lines(
            &lines,
            "AUTO playback restarted during the release; requested Linux ownership"
        ),
        1
    );
}

#[test]
fn category_rise_while_holding_the_take_sends_nothing() {
    let mut p = Probe::ready();
    assert_eq!(p.play_category(500, STREAM), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    assert_eq!(p.play_category(1000, MEDIA), []);
}

#[test]
fn category_fall_to_stream_is_no_stop() {
    let mut p = Probe::ready();
    assert_eq!(p.play_category(500, MEDIA), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    // 301 -> 201: no release, however long it lasts.
    assert_eq!(p.play_category(1000, STREAM), []);
    p.idle_ticks(1000);
    // 201 -> 100 is a stop: Linux keeps the AirPods idle (#262).
    assert_eq!(p.play_category(62_000, IDLE), []);
    p.idle_ticks(62_000);
}

#[test]
fn category_relay_beats_stale_report() {
    // REMOTE's relay of 100 beats its media report: a stream takes.
    let mut p = remote_playing(Some(IDLE));
    assert_eq!(p.play_category(500, STREAM), [SendOwnership(true)]);
    // Another host's relay does not stand for REMOTE's.
    let mut p = remote_playing(None);
    assert_eq!(p.feed(40, relay(REMOTE_2, IDLE)), []);
    assert_eq!(p.play_category(500, STREAM), []);
}

#[test]
fn category_relays_cleared_on_disconnect_and_session_restart() {
    let mut p = remote_playing(Some(IDLE));
    p.feed(40, Input::Disconnected);
    assert!(p.core.relays.is_empty());
    // A relay fed after the drop is gone once the next session starts.
    p.feed(50, relay(REMOTE, IDLE));
    assert_eq!(p.feed(1100, Input::Tick), [Reconnect { audio: true }]);
    p.feed(
        1200,
        Input::Reconnected(SessionFacts {
            audio: Some(source(REMOTE, AudioSourceType::Media)),
            ..ready_facts()
        }),
    );
    assert!(p.core.relays.is_empty());
    // So the media report blocks a stream again.
    assert_eq!(p.play_category(1300, IDLE), []);
    assert_eq!(p.play_category(1500, STREAM), []);
}

#[test]
fn category_relay_each_change_in_automatic_mode() {
    let mut p = relaying();
    assert_eq!(
        p.play_category(500, STREAM),
        [RelayCategory(STREAM), SendOwnership(true)]
    );
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    assert_eq!(p.play_category(1000, STREAM), []);
    assert_eq!(p.play_category(1500, MEDIA), [RelayCategory(MEDIA)]);
    assert_eq!(p.play_category(2000, IDLE), [RelayCategory(IDLE)]);
    // Relayed whether or not Linux wins.
    let mut p = remote_playing(None);
    p.core.category_relay = true;
    assert_eq!(p.play_category(500, STREAM), [RelayCategory(STREAM)]);
}

#[test]
fn category_relay_none_in_manual_mode() {
    let mut p = relaying();
    p.feed(100, Input::Manual);
    for (ms, category) in [(500, STREAM), (1000, MEDIA), (1500, IDLE)] {
        assert_eq!(p.play_category(ms, category), [], "{category}");
    }
}

#[test]
fn category_relay_none_during_a_call() {
    let mut p = relaying();
    assert_eq!(p.feed(100, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(p.play_category(500, STREAM), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    assert_eq!(p.play_category(1000, MEDIA), []);
    assert_eq!(p.play_category(1500, IDLE), []);
}

#[test]
fn category_relay_once_at_call_end_unless_idle() {
    let mut p = relaying();
    assert_eq!(p.feed(100, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(p.play_category(500, MEDIA), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    assert_eq!(
        p.feed(1000, Input::LocalCall(false)),
        [AdvertiseCall(false), RelayCategory(MEDIA)]
    );
    assert_eq!(p.play_category(1500, MEDIA), []);
    // Idle when the call ends: only the call's own advert.
    let mut p = relaying();
    assert_eq!(p.feed(100, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(
        p.feed(1000, Input::LocalCall(false)),
        [AdvertiseCall(false)]
    );
}

#[test]
fn category_held_takeover_command_lists() {
    // The production setup: A2DP hold and category relay on.
    let mut p = hold_ready(true);
    p.core.category_relay = true;
    p.core.take_lines();
    assert_eq!(p.feed(100, remote_media()), [HoldA2dp(true)]);
    assert_eq!(p.play_category(500, STREAM), [RelayCategory(STREAM)]);
    assert_eq!(
        p.core.take_lines(),
        ["AUTO takeover skipped: the audio source's category 301 is above Linux's 201"]
    );
    assert_eq!(
        p.play_category(1000, MEDIA),
        [RelayCategory(MEDIA), SendOwnership(true)]
    );
    // The ack releases the hold before it routes.
    assert_eq!(
        p.feed(1100, Input::Ownership(vec![1])),
        [HoldA2dp(false), RouteToAirPods]
    );
}

// #299: an announcement carries category 100; Linux's own follows it.
#[test]
fn category_relay_after_auto_toggle_at_301() {
    let mut p = relaying();
    p.feed(100, Input::Manual);
    assert_eq!(p.play_category(500, MEDIA), []);
    assert_eq!(
        p.feed(1000, Input::Auto),
        [
            ConnectA2dp,
            // The idle other host: the connect take (#324).
            SendOwnership(true),
            Announce,
            RelayCategory(MEDIA),
            DrainPlayback
        ]
    );
    assert_eq!(p.play_category(1500, MEDIA), []);
}

#[test]
fn category_relay_after_new_host_at_201() {
    let mut p = relaying();
    assert_eq!(
        p.play_category(500, STREAM),
        [RelayCategory(STREAM), SendOwnership(true)]
    );
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    let three = vec![host(LOCAL), host(REMOTE), host(REMOTE_2)];
    assert_eq!(
        p.feed(
            1000,
            Input::ConnectedDevices {
                old: two_hosts(),
                new: three,
            },
        ),
        [Announce, RelayCategory(STREAM)]
    );
}

#[test]
fn category_relay_after_reconnect_adoption() {
    // 301 set before the disconnect.
    let mut p = relaying();
    assert_eq!(
        p.play_category(500, MEDIA),
        [RelayCategory(MEDIA), SendOwnership(true)]
    );
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    assert_eq!(p.feed(1100, Input::Disconnected), [PauseMedia, DropSession]);
    assert_eq!(p.feed(2200, Input::Tick), [Reconnect { audio: true }]);
    // The idle other host: the connect take (#324) after the relay.
    assert_eq!(
        p.feed(2300, Input::Reconnected(ready_facts())),
        [
            AdoptSession,
            Announce,
            RelayCategory(MEDIA),
            SendOwnership(true),
            DrainPlayback
        ]
    );
    assert_eq!(p.play_category(2500, MEDIA), []);
    // 301 set while disconnected.
    let mut p = relaying();
    assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
    assert_eq!(p.play_category(500, MEDIA), []);
    assert_eq!(p.feed(1200, Input::Tick), [Reconnect { audio: true }]);
    assert_eq!(
        p.feed(1300, Input::Reconnected(ready_facts())),
        [
            AdoptSession,
            Announce,
            RelayCategory(MEDIA),
            SendOwnership(true),
            DrainPlayback
        ]
    );
    assert_eq!(p.play_category(1500, MEDIA), []);
}

// The idle other host also gets the connect take (#324).
#[test]
fn category_relay_after_announce_none_when_idle() {
    let mut p = relaying();
    p.feed(100, Input::Manual);
    assert_eq!(
        p.feed(1000, Input::Auto),
        [ConnectA2dp, SendOwnership(true), Announce, DrainPlayback]
    );
}

#[test]
fn category_relay_after_announce_none_when_disabled() {
    let mut p = Probe::ready();
    p.feed(100, Input::Manual);
    assert_eq!(p.play_category(500, MEDIA), []);
    assert_eq!(
        p.feed(1000, Input::Auto),
        [ConnectA2dp, SendOwnership(true), Announce, DrainPlayback]
    );
}

#[test]
fn category_relay_after_announce_none_during_call() {
    let mut p = relaying();
    assert_eq!(p.feed(100, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(p.play_category(500, MEDIA), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    let three = vec![host(LOCAL), host(REMOTE), host(REMOTE_2)];
    assert_eq!(
        p.feed(
            1000,
            Input::ConnectedDevices {
                old: two_hosts(),
                new: three,
            },
        ),
        [Announce, AdvertiseCall(true)]
    );
}
