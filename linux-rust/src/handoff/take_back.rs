//! Taking the AirPods back after a remote call that took them from Linux
//! (#14, `takeBackAfterCall`). It never resumes playback.

use super::*;
use crate::bluetooth::aacp::AUDIO_CATEGORY_CALL;

/// How far apart Linux's yield and a host's call relay (501) may be, in
/// either order, for that call to have taken the AirPods from Linux; also
/// how long after the call's end relay the take-back may wait for the policy
/// to allow it. Inclusive. #179 saw the 501 1 s after the yield.
pub(crate) const CALL_TAKE_WINDOW: Duration = Duration::from_secs(3);
const TAKE_BACK: &str =
    "AUTO the call that took the AirPods from Linux ended; requested Linux ownership";

#[derive(Default)]
pub(super) struct TakeBack {
    pub(super) enabled: bool,
    /// Linux's last yield.
    yielded_at: Option<Instant>,
    /// The last host to relay a call it hasn't ended yet, and when.
    call: Option<(String, Instant)>,
    /// The host whose call took the AirPods from Linux.
    host: Option<String>,
    /// When that host relayed the call's end.
    ended: Option<Instant>,
}

impl TakeBack {
    /// Forget everything but the setting.
    pub(super) fn clear(&mut self) {
        *self = TakeBack {
            enabled: self.enabled,
            ..TakeBack::default()
        };
    }

    /// Linux yielded the AirPods it owned or was taking.
    pub(super) fn yielded(&mut self, now: Instant) {
        if !self.enabled {
            return;
        }
        self.yielded_at = Some(now);
        if let Some((host, at)) = &self.call
            && within(*at, now)
        {
            self.host = Some(host.clone());
            self.ended = None;
        }
    }

    /// `host` relayed `category`.
    pub(super) fn relay(&mut self, now: Instant, host: &str, category: u32) {
        if !self.enabled {
            return;
        }
        if category == AUDIO_CATEGORY_CALL {
            self.call = Some((host.to_string(), now));
            if self.yielded_at.is_some_and(|at| within(at, now)) {
                self.host = Some(host.to_string());
                self.ended = None;
            }
        } else if category < AUDIO_CATEGORY_CALL {
            // A call that already ended took nothing from a later yield.
            self.call.take_if(|(h, _)| h == host);
            if self.host.as_deref() == Some(host) {
                self.ended = self.ended.or(Some(now));
            }
        }
    }
}

/// `a` and `b` are at most `CALL_TAKE_WINDOW` apart.
fn within(a: Instant, b: Instant) -> bool {
    a.max(b).duration_since(a.min(b)) <= CALL_TAKE_WINDOW
}

impl Handoff {
    /// Enable the take-back after a remote call (#14); off by default.
    pub(crate) fn with_take_back_after_call(mut self, enabled: bool) -> Self {
        self.take_back.enabled = enabled;
        self
    }

    /// After each input: a Linux take (`owns`: an OwnsConnection=1 report)
    /// clears the record; once the recorded call ended, take back when
    /// `decide_takeover` allows it, until `CALL_TAKE_WINDOW` after the end.
    /// Not gated by the connect mode: Linux owned until the call took them.
    pub(super) fn take_back(&mut self, now: Instant, owns: bool, out: &mut Vec<Command>) {
        if owns || self.pending.is_some_and(|p| p.desired) {
            self.take_back.clear();
            return;
        }
        let Some(ended) = self.take_back.ended else {
            return;
        };
        if now.saturating_duration_since(ended) > CALL_TAKE_WINDOW {
            self.take_back.clear();
            return;
        }
        if !self.automatic
            || !self.live()
            || self.quitting
            || self.pending.is_some()
            || self.requested_take
            || self.owner.as_deref() == Some(&[1])
            || self
                .decide_take(TakeoverTrigger::Connected, AUDIO_CATEGORY_IDLE)
                .action
                != TakeoverAction::TakeOver
        {
            return;
        }
        self.take_back.clear();
        // An idle take, as `connect_take`: its ACK resumes nothing.
        self.request(now, true, Cause::Playback, out);
        self.resume_deferred = true;
        self.say(TAKE_BACK);
    }
}
