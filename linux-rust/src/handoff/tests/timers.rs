use super::*;

#[test]
#[should_panic(expected = "clock rewound")]
fn probe_clock_never_rewinds() {
    let mut p = Probe::ready();
    p.feed(500, Input::Tick);
    p.feed(400, Input::Tick);
}

#[test]
fn case_disconnect_waits_for_new_playback() {
    let mut p = Probe::ready();
    p.feed(20, Input::EarState(vec![InCase, InCase]));
    assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
    // No reconnect while waiting, however long.
    assert_eq!(p.feed(5000, Input::Tick), []);
    assert_eq!(p.play(5100, true), []);
    assert_eq!(p.feed(5200, Input::Tick), []);
    // A new stopped-to-playing playback asks for the reconnection now.
    assert_eq!(p.play(5500, false), []);
    assert_eq!(p.play(6000, true), []);
    assert_eq!(p.feed(6100, Input::Tick), [Reconnect { audio: true }]);
    assert_eq!(p.feed(6200, Input::Tick), []);
    // Stopping before it completes cancels it.
    assert_eq!(p.play(6500, false), []);
    assert_eq!(
        p.feed(6600, Input::Reconnected(ready_facts())),
        [CancelReconnect]
    );
    assert_eq!(p.feed(9000, Input::Tick), []);
    // Playing again reconnects and takes for that playback.
    assert_eq!(p.play(9500, true), []);
    assert_eq!(p.feed(9600, Input::Tick), [Reconnect { audio: true }]);
    assert_eq!(
        p.feed(9700, Input::Reconnected(ready_facts())),
        [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
    );
    assert_eq!(p.feed(9800, Input::Ownership(vec![1])), []);

    // Without the case closed a disconnect reconnects after the backoff.
    let mut p = Probe::ready();
    assert_eq!(p.feed(100, Input::Disconnected), [DropSession]);
    assert_eq!(p.feed(1000, Input::Tick), []);
    assert_eq!(p.feed(1100, Input::Tick), [Reconnect { audio: true }]);
    assert_eq!(p.feed(1200, Input::ReconnectFailed("x".into())), []);
    assert_eq!(p.feed(2100, Input::Tick), []);
    assert_eq!(p.feed(2200, Input::Tick), [Reconnect { audio: true }]);
    // The other host is idle and a bud is in: the connect take (#324).
    assert_eq!(
        p.feed(2300, Input::Reconnected(ready_facts())),
        [AdoptSession, Announce, SendOwnership(true), DrainPlayback]
    );
}

#[test]
fn stop_during_pending_take_keeps_ownership_after_timeout() {
    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.play(1000, false), []);
    assert_eq!(p.feed(5500, Input::Tick), []);
    // The timeout releases nothing either (#262).
    assert_eq!(p.feed(5600, Input::Tick), []);
    p.idle_ticks(5600);
    assert_eq!(p.play(66_000, false), []);
}

#[test]
fn quit_releases_requested_ownership() {
    let mut p = Probe::ready();
    assert_eq!(p.feed(100, Input::Quit), [Exit]);

    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    assert_eq!(p.feed(700, Input::Quit), [SendOwnership(false)]);
    assert_eq!(p.feed(800, Input::Tick), []);
    assert_eq!(p.feed(900, Input::Ownership(vec![0])), [PauseMedia, Exit]);

    let mut p = Probe::ready();
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Quit), [SendOwnership(false)]);
    assert_eq!(
        p.feed(5700, Input::Tick),
        [Fail("release was not acknowledged")]
    );
}
