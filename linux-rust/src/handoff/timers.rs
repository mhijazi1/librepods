//! Ticks: timeouts, reconnect retries and quitting.

use super::*;

impl Handoff {
    pub(super) fn tick(&mut self, now: Instant, out: &mut Vec<Command>) {
        if self.park == Park::Parked && self.park_until.is_some_and(|end| now >= end) {
            self.park = Park::FallenBack;
            self.say("PARK no ACK within the park timeout; new playback goes to the local output");
        }
        if !self.connected
            && !self.reconnecting
            && self.recovery != CaseRecovery::WaitingForPlayback
            && now >= self.retry_at
            && !self.quitting
        {
            self.reconnecting = true;
            out.push(Command::Reconnect {
                audio: self.automatic,
            });
        }
        if let Some(pending) = self.pending
            && now.saturating_duration_since(pending.since) > ACK_TIMEOUT
        {
            self.say("TIMEOUT ownership acknowledgement not received; audible handoff unconfirmed");
            self.pending = None;
            self.reconnect_take_pending = false;
            // A release may still be acknowledged late: that 0 is not a
            // loss of the take it gave back (#55).
            self.timed_out_release = (!pending.desired).then_some((pending.cause, pending.since));
            if self.quitting {
                out.push(Command::Fail("release was not acknowledged"));
                return;
            }
            self.reconcile(now, out);
        }
    }

    /// While quitting, release ownership Linux asked for, then exit once
    /// nothing is left to release.
    pub(super) fn finish_quit(&mut self, now: Instant, out: &mut Vec<Command>) {
        if !self.quitting || self.pending.is_some_and(|p| !p.desired) {
            return;
        }
        if !self.requested_take || self.session_lost {
            // Nothing to release, or no session left to release it on.
            out.push(Command::Exit);
            return;
        }
        self.request(now, false, Cause::Quit, out);
        self.say("EXIT releasing ownership");
    }
}
