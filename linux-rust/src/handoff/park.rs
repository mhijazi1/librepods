//! A held take's routing and resume, the A2DP hold and ear detection's resume.

use super::*;

impl Handoff {
    /// A held take's audio follows the AirPods: on the take's ACK, and when
    /// `Auto` keeps an owner [1] (#324).
    pub(super) fn route_held_take(&mut self, out: &mut Vec<Command>) {
        // Parked playback follows the default to the AirPods (#311).
        if matches!(self.park, Park::Parked | Park::FallenBack) {
            self.park = Park::AirPods;
        }
        // Before the resume, so its audible check finds the stream on the
        // AirPods sink (#40).
        if self.default_sink_routing {
            out.push(Command::RouteToAirPods);
        }
        // A stop during the take (#205), or an idle connect take (#324),
        // defers the resume until Linux plays.
        if self.automatic && !self.resume_deferred {
            self.resume_if_idle(out);
        }
    }

    /// A take that found the transport idle gets its audio back (#200).
    /// Not during a call or mic use: the card is on HFP, so the idle A2DP
    /// transport says nothing.
    pub(super) fn resume_if_idle(&mut self, out: &mut Vec<Command>) {
        if !self.call_or_mic() && self.a2dp_active == Some(false) {
            out.push(Command::ResumeA2dp);
            self.say("A2DP transport idle after the take; resuming it");
        }
    }

    /// Another host got the AirPods and Linux holds no take: hold A2DP
    /// (#264).
    pub(super) fn hold_for_remote(&mut self, out: &mut Vec<Command>) {
        self.remote_owns = true;
        self.sync_hold(false, out);
    }

    /// A remote source seen while Linux's release was pending (the
    /// `AudioSource` arm skips it then) holds once the release settled and
    /// Linux has no take (#264).
    pub(super) fn hold_if_remote_source(&mut self, out: &mut Vec<Command>) {
        if !self.requested_take && self.audio.as_ref().is_some_and(|a| self.remote_took(a)) {
            self.hold_for_remote(out);
        }
    }

    /// Emit the hold the facts ask for when it differs from the last one
    /// emitted. Before a session start has emitted one, only `start` does,
    /// so the start also restores a card an earlier run left `off`.
    pub(super) fn sync_hold(&mut self, start: bool, out: &mut Vec<Command>) {
        let want = self.remote_owns && !self.quitting && !self.only_local_listed();
        // No new hold after SessionLost (#287); Disconnected releases an earlier one.
        if want && !self.live() {
            return;
        }
        if self.a2dp_hold && (start || self.held.is_some()) && self.held != Some(want) {
            self.held = Some(want);
            out.push(Command::HoldA2dp(want));
        }
    }

    /// The AirPods list only Linux: no other host to hold A2DP for. An empty
    /// list is no evidence of that.
    pub(super) fn only_local_listed(&self) -> bool {
        !self.hosts.is_empty() && self.hosts.iter().all(|d| d.mac == self.local)
    }

    /// A play sampled at `sampled_at` is ear detection's resume while another
    /// host still owns (#301).
    pub(super) fn ear_resumed(&self, sampled_at: Instant) -> bool {
        self.remote_owns && self.ear_resume_until.is_some_and(|end| sampled_at < end)
    }
}
