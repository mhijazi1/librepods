use super::*;

/// `acked_auto_take` (playing at 1000 ms), then a local call starts.
fn acked_take_in_call() -> Probe {
    let mut p = acked_auto_take();
    p.core.take_lines();
    assert_eq!(p.feed(1050, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(
        p.core.take_lines(),
        ["CALL started on Linux; keeping the AirPods Linux holds until it ends"]
    );
    p
}

#[test]
fn local_call_refuses_remote_request() {
    // An iPhone starts media and asks for the AirPods mid-call.
    let mut p = acked_take_in_call();
    assert_eq!(
        p.feed(
            1100,
            Input::AudioSource(source(REMOTE, AudioSourceType::Media))
        ),
        []
    );
    assert_eq!(p.feed(1200, Input::RemoteOwnershipRequest), []);
    assert_eq!(
        p.core.take_lines(),
        ["REMOTE requested ownership; refused during a local call"]
    );
    // Whatever the remote plays, and however often it asks.
    assert_eq!(
        p.feed(
            1300,
            Input::AudioSource(source(REMOTE, AudioSourceType::Call))
        ),
        []
    );
    assert_eq!(p.feed(1400, Input::RemoteOwnershipRequest), []);
    assert_eq!(p.feed(9000, Input::Tick), []);
    let lines = p.core.take_lines();
    assert_eq!(
        count_lines(&lines, "REMOTE requested ownership; refused"),
        1
    );
    assert_eq!(count_lines(&lines, "TIMEOUT"), 0);

    // A take still pending is kept too.
    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(550, Input::LocalCall(true)), [AdvertiseCall(true)]);
    p.core.take_lines();
    assert_eq!(p.feed(600, Input::RemoteOwnershipRequest), []);
    assert_eq!(
        p.core.take_lines(),
        ["REMOTE requested ownership; refused during a local call"]
    );
    assert_eq!(p.feed(700, Input::Ownership(vec![1])), []);

    // A dead session is reported first, as without a call.
    let mut p = acked_take_in_call();
    assert_eq!(p.feed(1100, Input::SessionLost), []);
    p.core.take_lines();
    assert_eq!(p.feed(1200, Input::RemoteOwnershipRequest), []);
    assert_eq!(
        p.core.take_lines(),
        ["REMOTE requested ownership; control session lost"]
    );

    // Linux owns them without having taken them: the existing yield.
    let mut p = Probe::ready();
    p.feed(20, Input::Ownership(vec![1]));
    assert_eq!(p.feed(30, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(
        p.feed(100, Input::RemoteOwnershipRequest),
        [SendOwnership(false)]
    );
}

#[test]
fn local_call_holds_on_playback_stop() {
    let mut p = acked_take_in_call();
    assert_eq!(p.play(1500, false), []);
    assert_eq!(
        p.core.take_lines(),
        ["AUTO playback stopped; keeping ownership"]
    );
    assert_eq!(p.play(2000, false), []);
    assert_eq!(p.feed(9000, Input::Tick), []);
    // A genuine loss still yields once (#39).
    p.core.take_lines();
    assert_eq!(p.feed(9100, Input::Ownership(vec![0])), [PauseMedia]);
    assert_eq!(count_lines(&p.core.take_lines(), "YIELD"), 1);
    // Nothing is left to release when the call ends.
    assert_eq!(
        p.feed(9200, Input::LocalCall(false)),
        [AdvertiseCall(false)]
    );
}

#[test]
fn local_call_holds_stop_during_pending_take() {
    // Stopped while the take was pending, ACKed during the call.
    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(550, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(p.play(1000, false), []);
    p.core.take_lines();
    assert_eq!(p.feed(1100, Input::Ownership(vec![1])), []);
    let lines = p.core.take_lines();
    assert_eq!(
        count_lines(
            &lines,
            "AUTO playback stopped during the take; keeping ownership"
        ),
        1
    );
    assert_eq!(p.play(1500, false), []);
    assert_eq!(p.feed(9000, Input::Tick), []);
    // The call's end releases nothing (#262).
    assert_eq!(
        p.feed(9100, Input::LocalCall(false)),
        [AdvertiseCall(false)]
    );
    p.idle_ticks(9100);

    // The same when the take's ACK times out during the call.
    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(550, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(p.play(1000, false), []);
    assert_eq!(p.feed(5600, Input::Tick), []);
    assert_eq!(
        p.feed(6000, Input::LocalCall(false)),
        [AdvertiseCall(false)]
    );
    p.idle_ticks(6000);

    // The call ends while the take is still pending: its ACK keeps it.
    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(550, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(p.play(1000, false), []);
    assert_eq!(
        p.feed(1100, Input::LocalCall(false)),
        [AdvertiseCall(false)]
    );
    assert_eq!(p.feed(1200, Input::Ownership(vec![1])), []);
    p.idle_ticks(1200);
}

#[test]
fn local_call_repeat_is_noop() {
    let mut p = acked_take_in_call();
    assert_eq!(p.feed(1100, Input::LocalCall(true)), []);
    assert!(p.core.take_lines().is_empty());
    assert_eq!(
        p.feed(1200, Input::LocalCall(false)),
        [AdvertiseCall(false)]
    );
    p.core.take_lines();
    assert_eq!(p.feed(1300, Input::LocalCall(false)), []);
    assert!(p.core.take_lines().is_empty());

    // A first report of no call is no change either.
    let mut p = Probe::ready();
    assert_eq!(p.feed(20, Input::LocalCall(false)), []);
    assert!(p.core.take_lines().is_empty());
}

#[test]
fn local_call_recorded_without_live_session() {
    // Manual mode records the call; going automatic advertises it. Buds
    // out: the connect take (#324) is not under test here.
    let mut p = Probe::new(false, true);
    p.routing(unworn_facts());
    assert_eq!(p.feed(100, Input::LocalCall(true)), []);
    assert_eq!(
        p.feed(200, Input::Auto),
        [
            ConnectA2dp,
            SendOwnership(false),
            Announce,
            AdvertiseCall(true),
            DrainPlayback
        ]
    );

    // The call survives a disconnect and is advertised after the
    // reconnect's announcement; changes while disconnected only record.
    let mut p = acked_take_in_call();
    assert_eq!(p.feed(1100, Input::Disconnected), [PauseMedia, DropSession]);
    assert_eq!(p.feed(1150, Input::LocalCall(false)), []);
    assert_eq!(p.feed(1160, Input::LocalCall(true)), []);
    assert_eq!(p.feed(2200, Input::Tick), [Reconnect { audio: true }]);
    // The idle other host: the connect take (#324), after the call's advert.
    assert_eq!(
        p.feed(2300, Input::Reconnected(ready_facts())),
        [
            AdoptSession,
            Announce,
            AdvertiseCall(true),
            SendOwnership(true),
            DrainPlayback
        ]
    );

    // A new host announced during the call learns of it too.
    let three = vec![host(LOCAL), host(REMOTE), host("0D:0E:0F:10:11:12")];
    assert_eq!(
        p.feed(
            2400,
            Input::ConnectedDevices {
                old: two_hosts(),
                new: three,
            },
        ),
        [Announce, AdvertiseCall(true)]
    );
}

#[test]
fn remote_call_blocks_takeover_during_local_call() {
    let mut p = Probe::ready();
    p.feed(
        20,
        Input::AudioSource(source(REMOTE, AudioSourceType::Call)),
    );
    assert_eq!(p.feed(30, Input::LocalCall(true)), [AdvertiseCall(true)]);
    // Fresh playback and a manual take are blocked.
    assert_eq!(p.play(500, true), []);
    assert_eq!(p.play(1000, false), []);
    assert_eq!(p.play(1500, true), []);
    p.core.take_lines();
    assert_eq!(p.feed(1600, Input::Take), []);
    assert!(p.core.take_lines().iter().any(|l| l.starts_with("BLOCKED")));

    // A reconnect for new playback after case closure, into a session
    // whose facts still show the remote call: no take.
    assert_eq!(p.play(2000, false), []);
    p.feed(2100, Input::EarState(vec![InCase, InCase]));
    assert_eq!(p.feed(2200, Input::Disconnected), [DropSession]);
    assert_eq!(p.play(2500, false), []);
    assert_eq!(p.play(3000, true), []);
    assert_eq!(p.feed(3100, Input::Tick), [Reconnect { audio: true }]);
    let facts = SessionFacts {
        audio: Some(source(REMOTE, AudioSourceType::Call)),
        ..ready_facts()
    };
    p.core.take_lines();
    assert_eq!(
        p.feed(3200, Input::Reconnected(facts)),
        [AdoptSession, Announce, AdvertiseCall(true), DrainPlayback]
    );
    assert!(
        p.core
            .take_lines()
            .iter()
            .any(|l| l.starts_with("AUTO reconnected; takeover blocked"))
    );
    assert_eq!(p.feed(3300, Input::Take), []);

    // The remote call ends: the next fresh playback takes.
    p.feed(3400, Input::AudioSource(idle()));
    assert_eq!(p.play(3500, false), []);
    assert_eq!(p.play(4000, true), [SendOwnership(true)]);
}

#[test]
fn call_end_restores_policy() {
    // Still playing when the call ends: nothing is released.
    let mut p = acked_take_in_call();
    assert_eq!(p.play(1500, true), []);
    assert_eq!(
        p.feed(1600, Input::LocalCall(false)),
        [AdvertiseCall(false)]
    );
    assert_eq!(p.core.take_lines(), ["CALL ended on Linux"]);
    // A remote request yields as in yields_on_remote_request.
    assert_eq!(
        p.feed(1700, Input::RemoteOwnershipRequest),
        [PauseMedia, SendOwnership(false)]
    );
    assert_eq!(p.feed(1800, Input::Ownership(vec![0])), []);

    // A stop edge keeps them as in takes_on_new_playback_when_worn.
    let mut p = acked_take_in_call();
    assert_eq!(
        p.feed(1100, Input::LocalCall(false)),
        [AdvertiseCall(false)]
    );
    assert_eq!(p.play(1500, false), []);
    p.idle_ticks(1500);
    assert_eq!(p.play(62_000, false), []);
}

#[test]
fn call_end_at_idle_keeps_ownership() {
    // Stopped during the call while Linux owns: the end only advertises
    // it (#262).
    let mut p = acked_take_in_call();
    assert_eq!(p.play(1500, false), []);
    p.core.take_lines();
    assert_eq!(
        p.feed(2000, Input::LocalCall(false)),
        [AdvertiseCall(false)]
    );
    assert_eq!(p.core.take_lines(), ["CALL ended on Linux"]);
    p.idle_ticks(2000);
    assert_eq!(p.play(62_500, false), []);

    // During the call, a new host still hears of it after its Announce,
    // and a remote request is still refused.
    let mut p = acked_take_in_call();
    assert_eq!(p.play(1500, false), []);
    assert_eq!(
        p.feed(1600, report(&[L, R], &[L, R, R2])),
        [Announce, AdvertiseCall(true)]
    );
    assert_eq!(p.feed(1700, Input::RemoteOwnershipRequest), []);
    p.idle_ticks(1700);

    // Without a take a call end releases nothing.
    let mut p = Probe::ready();
    assert_eq!(p.feed(20, Input::LocalCall(true)), [AdvertiseCall(true)]);
    assert_eq!(p.feed(30, Input::LocalCall(false)), [AdvertiseCall(false)]);
}
