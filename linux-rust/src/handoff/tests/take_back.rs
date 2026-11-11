//! #14: taking the AirPods back after a remote call that took them from
//! Linux (`takeBackAfterCall`).

use super::*;
use crate::bluetooth::aacp::AUDIO_CATEGORY_CALL as CALL;
use crate::handoff::take_back::CALL_TAKE_WINDOW;
use crate::handoff_log::{Kind, classify};

/// `CALL_TAKE_WINDOW` in ms.
fn window() -> u64 {
    CALL_TAKE_WINDOW.as_millis() as u64
}

/// When REMOTE relays the call's end in most tests: the call's length
/// doesn't matter.
const END: u64 = 30_000;

/// Turn the setting on through the builder `run_handoff` uses.
fn enable(p: &mut Probe) {
    let core = std::mem::replace(
        &mut p.core,
        Handoff::new(LOCAL.to_string(), p.t0, false, false),
    );
    p.core = core.with_take_back_after_call(true);
}

/// `acked_auto_take` (Linux owns, playing at 1000 ms), with the setting on
/// if `on` (else left at its default); then #179: REMOTE asks at 1100 and
/// Linux yields, the release's ACK [0] at 1200, the pause's stop sample at
/// 1300, and REMOTE relays the call (501) at 2100.
fn remote_call(on: bool) -> Probe {
    let mut p = acked_auto_take();
    if on {
        enable(&mut p);
    }
    assert_eq!(
        p.feed(1100, Input::RemoteOwnershipRequest),
        [PauseMedia, SendOwnership(false)]
    );
    assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);
    assert_eq!(p.play(1300, false), []);
    assert_eq!(p.feed(2100, relay(REMOTE, CALL)), []);
    p.core.take_lines();
    p
}

fn take_requests(lines: &[String]) -> usize {
    lines
        .iter()
        .filter(|l| classify(l) == Kind::TakeRequest)
        .count()
}

/// Ticks every 100 ms from `from` through `to` send no take.
fn no_take_until(p: &mut Probe, from: u64, to: u64) {
    for ms in (from..=to).step_by(100) {
        assert_eq!(takes(&p.feed(ms, Input::Tick)), 0, "{ms}");
    }
}

#[test]
fn off_by_default_never_takes_back() {
    let mut p = remote_call(false);
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), []);
    no_take_until(&mut p, END, END + 2 * window());
    assert_eq!(take_requests(&p.core.take_lines()), 0);
}

#[test]
fn takes_back_once_when_the_call_ends() {
    let mut p = remote_call(true);
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), [SendOwnership(true)]);
    let lines = p.core.take_lines();
    assert_eq!(take_requests(&lines), 1, "{lines:?}");
    // An idle take: its ACK resumes nothing and pauses nothing.
    assert_eq!(p.feed(END + 50, idle_transport()), []);
    let ack = p.feed(END + 100, Input::Ownership(vec![1]));
    assert!(
        !ack.contains(&PauseMedia) && !ack.contains(&ResumeA2dp),
        "{ack:?}"
    );
}

#[test]
fn takes_back_after_a_lost_take() {
    let mut p = acked_auto_take();
    enable(&mut p);
    // The call takes the AirPods without asking: `yield_take`.
    assert_eq!(p.feed(1100, Input::Ownership(vec![0])), [PauseMedia]);
    assert_eq!(p.play(1300, false), []);
    assert_eq!(p.feed(2100, relay(REMOTE, CALL)), []);
    p.core.take_lines();
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), [SendOwnership(true)]);
    assert_eq!(take_requests(&p.core.take_lines()), 1);
}

#[test]
fn takes_back_after_yielding_a_pending_take() {
    // Owner [0]: only the pending take makes the yield Linux's.
    let mut p = Probe::ready();
    enable(&mut p);
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(
        p.feed(600, Input::RemoteOwnershipRequest),
        [PauseMedia, SendOwnership(false)]
    );
    assert_eq!(p.feed(700, Input::Ownership(vec![0])), []);
    assert_eq!(p.feed(1500, relay(REMOTE, CALL)), []);
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), [SendOwnership(true)]);
}

#[test]
fn takes_back_when_the_call_relay_precedes_the_yield() {
    let mut p = acked_auto_take();
    enable(&mut p);
    assert_eq!(p.feed(1100, relay(REMOTE, CALL)), []);
    // The window is inclusive.
    let ask = 1100 + window();
    assert_eq!(
        p.feed(ask, Input::RemoteOwnershipRequest),
        [PauseMedia, SendOwnership(false)]
    );
    assert_eq!(p.feed(ask + 100, Input::Ownership(vec![0])), []);
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), [SendOwnership(true)]);
}

#[test]
fn no_take_back_for_a_call_after_the_window() {
    // A call relayed just after the window, in either order.
    let mut p = acked_auto_take();
    enable(&mut p);
    assert_eq!(
        p.feed(1100, Input::RemoteOwnershipRequest),
        [PauseMedia, SendOwnership(false)]
    );
    assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);
    assert_eq!(p.feed(1100 + window() + 1, relay(REMOTE, CALL)), []);
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), []);
    no_take_until(&mut p, END, END + 2 * window());

    let mut p = acked_auto_take();
    enable(&mut p);
    assert_eq!(p.feed(1100, relay(REMOTE, CALL)), []);
    let ask = 1100 + window() + 1;
    assert_eq!(
        p.feed(ask, Input::RemoteOwnershipRequest),
        [PauseMedia, SendOwnership(false)]
    );
    assert_eq!(p.feed(ask + 100, Input::Ownership(vec![0])), []);
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), []);
    no_take_until(&mut p, END, END + 2 * window());

    // A call that ended before the yield took nothing from it.
    let mut p = acked_auto_take();
    enable(&mut p);
    assert_eq!(p.feed(1100, relay(REMOTE, CALL)), []);
    assert_eq!(p.feed(1500, relay(REMOTE, IDLE)), []);
    assert_eq!(
        p.feed(2000, Input::RemoteOwnershipRequest),
        [PauseMedia, SendOwnership(false)]
    );
    assert_eq!(p.feed(2100, Input::Ownership(vec![0])), []);
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), []);
    no_take_until(&mut p, END, END + 2 * window());
}

#[test]
fn no_take_back_when_linux_did_not_own() {
    // Linux idle, REMOTE already owns ([0]), no take: nothing to yield.
    let mut p = Probe::ready();
    enable(&mut p);
    assert_eq!(p.feed(100, relay(REMOTE, CALL)), []);
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), []);
    no_take_until(&mut p, END, END + 2 * window());

    // REMOTE asks anyway: "Linux already yielded" records nothing.
    let mut p = Probe::ready();
    enable(&mut p);
    assert_eq!(p.feed(100, Input::RemoteOwnershipRequest), []);
    assert_eq!(
        p.core.take_lines(),
        ["REMOTE requested ownership; Linux already yielded"]
    );
    assert_eq!(p.feed(200, relay(REMOTE, CALL)), []);
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), []);
    no_take_until(&mut p, END, END + 2 * window());
}

#[test]
fn no_second_take_back() {
    let mut p = remote_call(true);
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), [SendOwnership(true)]);
    assert_eq!(p.feed(END + 100, relay(REMOTE, IDLE)), []);
    assert_eq!(p.feed(END + 200, Input::Ownership(vec![1])), []);
    // Another call with no new Linux yield.
    assert_eq!(p.feed(END + 1000, relay(REMOTE, CALL)), []);
    assert_eq!(p.feed(END + 2000, relay(REMOTE, IDLE)), []);
    no_take_until(&mut p, END + 2000, END + 2000 + 2 * window());

    // The take-back's ACK times out, then REMOTE relays another call.
    let mut p = remote_call(true);
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), [SendOwnership(true)]);
    no_take_until(&mut p, END + 100, END + 6000);
    assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);
    assert_eq!(p.feed(END + 7000, relay(REMOTE, CALL)), []);
    assert_eq!(p.feed(END + 8000, relay(REMOTE, IDLE)), []);
    no_take_until(&mut p, END + 8000, END + 8000 + 2 * window());
}

#[test]
fn a_linux_take_or_a_disconnect_clears_the_record() {
    // An [1] report: Linux got the AirPods back on its own.
    let mut p = remote_call(true);
    assert_eq!(p.feed(3000, Input::Ownership(vec![1])), []);
    assert_eq!(p.feed(4000, Input::Ownership(vec![0])), []);
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), []);
    no_take_until(&mut p, END, END + 2 * window());

    // A disconnect, then a reconnect (buds out, so no connect take).
    let mut p = remote_call(true);
    assert_eq!(p.feed(3000, Input::Disconnected), [DropSession]);
    assert_eq!(p.feed(4100, Input::Tick), [Reconnect { audio: true }]);
    assert_eq!(takes(&p.feed(4200, Input::Reconnected(unworn_facts()))), 0);
    p.wear_in(4300);
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), []);
    no_take_until(&mut p, END, END + 2 * window());

    // A Linux take request that settles without an [1]: its ACK times
    // out, then a release is ACKed [0].
    let mut p = remote_call(true);
    assert_eq!(p.feed(3000, Input::Take), [SendOwnership(true)]);
    no_take_until(&mut p, 3100, 8100);
    assert_eq!(count_lines(&p.core.take_lines(), "TIMEOUT"), 1);
    assert_eq!(p.feed(9000, Input::Release), [SendOwnership(false)]);
    assert_eq!(p.feed(9100, Input::Ownership(vec![0])), [PauseMedia]);
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), []);
    no_take_until(&mut p, END, END + 2 * window());
}

#[test]
fn a_new_call_after_the_end_drops_the_record() {
    // The end relay comes while the policy blocks the take (buds out).
    let mut p = remote_call(true);
    assert_eq!(
        p.feed(29_000, Input::EarState(vec![OutOfEar, OutOfEar])),
        []
    );
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), []);
    // A new call, outside the window of Linux's yield, took nothing from it.
    assert_eq!(p.feed(END + 100, relay(REMOTE, CALL)), []);
    let worn = p.feed(END + 200, Input::EarState(vec![InEar, InEar]));
    assert_eq!(takes(&worn), 0, "{worn:?}");
    assert_eq!(p.feed(END + 1000, relay(REMOTE, IDLE)), []);
    no_take_until(&mut p, END + 1000, END + 1000 + 2 * window());
}

#[test]
fn no_take_back_in_manual_mode_or_with_a_request_pending() {
    let mut p = remote_call(true);
    assert_eq!(p.feed(3000, Input::Manual), []);
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), []);
    no_take_until(&mut p, END, END + 2 * window());

    // The yield's release is still pending when the call ends.
    let mut p = acked_auto_take();
    enable(&mut p);
    assert_eq!(
        p.feed(1100, Input::RemoteOwnershipRequest),
        [PauseMedia, SendOwnership(false)]
    );
    assert_eq!(p.feed(1500, relay(REMOTE, CALL)), []);
    assert_eq!(p.feed(2000, relay(REMOTE, IDLE)), []);
    no_take_until(&mut p, 2000, 2500);
    // Its ACK within the window lets the take-back decide again.
    assert_eq!(
        p.feed(2600, Input::Ownership(vec![0])),
        [SendOwnership(true)]
    );
}

#[test]
fn takes_back_in_when_last_connected_mode() {
    // `acked_auto_take`, with the mode set when the core is built; Linux
    // was the last owner, so the playback take goes ahead.
    let t0 = Instant::now();
    let mut p = Probe {
        core: Handoff::new(LOCAL.to_string(), t0, true, true)
            .with_connect_mode(WhenLastConnected)
            .with_last_owner_linux(true)
            .with_take_back_after_call(true),
        t0,
        last_ms: 0,
    };
    p.routing(ready_facts());
    assert_eq!(p.play(10, false), []);
    assert_eq!(p.play(500, true), [SendOwnership(true)]);
    assert_eq!(p.feed(600, Input::Ownership(vec![1])), []);
    assert_eq!(
        p.feed(1100, Input::RemoteOwnershipRequest),
        [PauseMedia, SendOwnership(false)]
    );
    assert_eq!(p.feed(1200, Input::Ownership(vec![0])), []);
    assert!(!p.core.last_owner_linux());
    assert_eq!(p.feed(2100, relay(REMOTE, CALL)), []);
    p.core.take_lines();
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), [SendOwnership(true)]);
    assert_eq!(take_requests(&p.core.take_lines()), 1);
}

#[test]
fn waits_for_the_call_source_to_clear() {
    let mut p = remote_call(true);
    let call = Input::AudioSource(source(REMOTE, AudioSourceType::Call));
    assert_eq!(p.feed(2200, call), []);
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), []);
    no_take_until(&mut p, END, END + 1000);
    // The window is inclusive.
    let cleared = Input::AudioSource(source(REMOTE, AudioSourceType::None));
    assert_eq!(p.feed(END + window(), cleared), [SendOwnership(true)]);
    assert_eq!(take_requests(&p.core.take_lines()), 1);
}

#[test]
fn no_take_back_when_the_call_source_outlasts_the_window() {
    let mut p = remote_call(true);
    let call = Input::AudioSource(source(REMOTE, AudioSourceType::Call));
    assert_eq!(p.feed(2200, call), []);
    assert_eq!(p.feed(END, relay(REMOTE, IDLE)), []);
    let cleared = Input::AudioSource(source(REMOTE, AudioSourceType::None));
    assert_eq!(p.feed(END + window() + 1, cleared), []);
    no_take_until(&mut p, END + window() + 100, END + 3 * window());
}
