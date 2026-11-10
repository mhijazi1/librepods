//! Ownership reports, yielding a lost take, reconciling and announcing.

use super::*;

impl Handoff {
    /// An OwnsConnection report. Rules for a 0 (#39), in order:
    /// - during the reconnect take, until the shell's queue boundary, it is
    ///   the session's stale init report and only updates `owner` (#43,
    ///   #49);
    /// - the acknowledgement of Linux's own release pauses only after a user
    ///   or quit release (upstream #841); Auto's playback release is sent
    ///   holding no take, and a remote-request release already paused. A 0
    ///   within `LATE_ACK_WINDOW` of sending a release whose acknowledgement
    ///   timed out is that release's late acknowledgement and follows the
    ///   same rule, without a second reconcile (#55); a later 0 falls through
    ///   to the rule below (#62);
    /// - otherwise, while Linux holds or is taking the AirPods, another host
    ///   took them: `yield_take` yields once.
    pub(super) fn ownership(&mut self, now: Instant, value: Vec<u8>, out: &mut Vec<Command>) {
        let acked = self
            .pending
            .filter(|p| value.as_slice() == [u8::from(p.desired)]);
        let was_linux = self.owner.as_deref() == Some(&[1]);
        self.owner = Some(value.clone());
        if value == [1] {
            self.last_owner_linux = true;
            // Before a take's RouteToAirPods and ResumeA2dp (#264).
            self.remote_owns = false;
            self.ear_resume_until = None;
            self.sync_hold(false, out);
        }
        if value == [0] && self.reconnect_take_pending {
            // A stale report queued while the reconnected session
            // initialized, not a loss of the pending take (which wants 1, so
            // this is not its acknowledgement): neither pause the playback
            // the take is for nor forget the take, which a stop during it
            // keeps (#262).
            self.say("OWNER stale not-linux report during the reconnect take; ignored");
            return;
        }
        // A release of Linux's timed out with nothing pending since: a 0
        // within `LATE_ACK_WINDOW` of its send is its late acknowledgement
        // (#55); a later 0 is a genuine loss as if there were no marker
        // (#62); any other value says Linux still owns, so that release
        // failed and is forgotten.
        let late_release = self
            .timed_out_release
            .take()
            .filter(|&(_, sent)| {
                value == [0]
                    && acked.is_none()
                    && now.saturating_duration_since(sent) <= LATE_ACK_WINDOW
            })
            .map(|(cause, _)| cause);
        if value == [0] {
            match acked.map(|p| p.cause).or(late_release) {
                // The acknowledgement of Linux's own release (#39). Auto's
                // playback release is sent holding no take, and
                // RemoteOwnershipRequest paused when it yielded, so neither
                // has anything to pause. A user or quit release may leave
                // Linux playing, so upstream #841's pause stays. A late
                // acknowledgement (#55) is handled the same way.
                Some(cause) => {
                    // A remote release handed the AirPods to the other host.
                    if cause == Cause::Remote {
                        self.last_owner_linux = false;
                    }
                    if self.requested_take
                        && self.automatic
                        && matches!(cause, Cause::User | Cause::Quit)
                    {
                        out.push(Command::PauseMedia);
                    }
                }
                // Another host took the AirPods from a take Linux made or
                // was making: yield once and forget the take.
                None if self.requested_take => {
                    self.take_back.yielded(now);
                    self.yield_take(out);
                    return;
                }
                // Another host took the AirPods without asking.
                None if was_linux => {
                    self.last_owner_linux = false;
                    self.hold_for_remote(out);
                }
                None => {}
            }
            self.requested_take = false;
        }
        let Some(pending) = acked else {
            if late_release.is_some() {
                // The timeout already reconciled; nothing more to decide.
                self.say("ACK owner=not-linux late (after TIMEOUT); release confirmed");
                self.hold_if_remote_source(out);
            }
            return;
        };
        self.say(format!(
            "ACK owner={} elapsed_ms={}",
            if pending.desired {
                "linux"
            } else {
                "not-linux"
            },
            now.saturating_duration_since(pending.since).as_millis()
        ));
        self.pending = None;
        self.reconnect_take_pending = false;
        if !pending.desired {
            self.requested_take = false;
        }
        if self.quitting {
            out.push(Command::Exit);
            return;
        }
        self.reconcile(now, out);
        self.hold_if_remote_source(out);
        // The take holds unless the reconcile just released it.
        let held = pending.desired && self.live() && self.requested_take && self.pending.is_none();
        if held {
            self.route_held_take(out);
        }
    }

    /// The AirPods reported OwnsConnection=0 while Linux held, or was
    /// taking, them and the report acknowledges no release of Linux's: the
    /// take is lost. Give it up once: pause in automatic mode, forget the
    /// take and its pending request, so neither a stop nor the request's
    /// timeout acts on it later.
    fn yield_take(&mut self, out: &mut Vec<Command>) {
        // `ownership` only yields on a report that is not a release's
        // acknowledgement, so a pending request here is the take.
        let dropped = self.pending.take_if(|p| p.desired).is_some();
        self.requested_take = false;
        self.last_owner_linux = false;
        self.stop_during_take = false;
        self.play_during_release = None;
        self.resume_deferred = false;
        if self.automatic {
            out.push(Command::PauseMedia);
        }
        self.route_away(out);
        self.hold_for_remote(out);
        self.say(format!(
            "YIELD owner=not-linux; another host has the AirPods; Linux gave up its {}take{}",
            if dropped { "pending " } else { "" },
            if self.automatic {
                " and paused local playback"
            } else {
                ""
            },
        ));
    }

    /// `RouteAway`, once per take (#40).
    pub(super) fn route_away(&mut self, out: &mut Vec<Command>) {
        if self.default_sink_routing && !std::mem::replace(&mut self.routed_away, true) {
            out.push(Command::RouteAway);
        }
    }

    /// The pending request settled (acknowledged or timed out): act on a
    /// playback edge that arrived while it was in flight.
    pub(super) fn reconcile(&mut self, now: Instant, out: &mut Vec<Command>) {
        let stop = std::mem::take(&mut self.stop_during_take);
        let play = std::mem::take(&mut self.play_during_release);
        // Nothing is sent on a session found dead, even if the routing
        // cached before it died would allow a take.
        if !self.automatic || !self.live() || self.quitting || self.pending.is_some() {
            return;
        }
        if stop && self.requested_take && self.was_playing == Some(false) {
            // Linux keeps the AirPods idle (#262); a held take's ACK defers
            // its A2DP resume to the next play edge (#205).
            self.resume_deferred = true;
            self.say("AUTO playback stopped during the take; keeping ownership");
        } else if let Some(category) = play.filter(|_| self.was_playing == Some(true)) {
            // The stopped-to-playing edge was seen while the release was in
            // flight; decide on it now.
            if self.decide_poll(Some(false), true, category).action == TakeoverAction::TakeOver {
                if !self.auto_take_allowed() {
                    self.say(CONNECT_MODE_SKIP);
                    return;
                }
                self.request(now, true, Cause::Playback, out);
                self.say("AUTO playback restarted during the release; requested Linux ownership");
            } else {
                self.say("AUTO takeover blocked by remote activity, wear, or missing host state");
            }
        }
    }

    /// Announce Linux to the remote hosts. The announcement carries the idle
    /// media information, so during a call the call is advertised again
    /// after it (#11), and otherwise a relayed category is re-sent (#299),
    /// at least mic use's 200 (#505).
    pub(super) fn announce(&self, out: &mut Vec<Command>) {
        out.push(Command::Announce);
        let category = self.relayed(self.category);
        if self.local_call {
            out.push(Command::AdvertiseCall(true));
        } else if self.category_relay && category != AUDIO_CATEGORY_IDLE {
            out.push(Command::RelayCategory(category));
        }
    }

    pub(super) fn routing(&self) -> RoutingSnapshot {
        routing_snapshot(
            self.live(),
            &self.hosts,
            &self.ears,
            self.audio.as_ref(),
            &self.relays,
            self.category,
            &self.local,
        )
    }
}
