//! Session start, the connect take, disconnects and reconnects.

use super::*;

impl Handoff {
    pub(super) fn disconnected(&mut self, now: Instant, out: &mut Vec<Command>) {
        if !self.connected {
            return;
        }
        self.say("DISCONNECTED clearing ownership and playback baseline");
        if self.requested_take && self.automatic {
            out.push(Command::PauseMedia);
        }
        // The next session start emits the hold again.
        self.remote_owns = false;
        self.ear_resume_until = None;
        self.sync_hold(false, out);
        self.held = None;
        out.push(Command::DropSession);
        self.connected = false;
        self.session_lost = false;
        self.pending = None;
        self.reconnect_take_pending = false;
        self.timed_out_release = None;
        self.requested_take = false;
        self.was_playing = None;
        self.take_assumes_playing = false;
        self.stop_during_take = false;
        self.play_during_release = None;
        self.resume_deferred = false;
        self.hosts.clear();
        self.snapshot_hosts.clear();
        self.relays.clear();
        self.ears.clear();
        self.audio = None;
        self.owner = None;
        self.recovery = if self.automatic && self.case_closed {
            self.say("CASE waiting for new Linux playback before reconnecting");
            CaseRecovery::WaitingForPlayback
        } else {
            CaseRecovery::Immediate
        };
        self.case_closed = false;
        self.retry_at = now + FIRST_RETRY;
    }

    pub(super) fn reconnected(
        &mut self,
        now: Instant,
        facts: SessionFacts,
        out: &mut Vec<Command>,
    ) {
        self.reconnecting = false;
        if self.recovery == CaseRecovery::WaitingForPlayback {
            out.push(Command::CancelReconnect);
            self.say("RECONNECT cancelled because playback stopped");
            return;
        }
        out.push(Command::AdoptSession);
        self.connected = true;
        self.session_lost = false;
        self.snapshot_hosts = facts.hosts.clone();
        self.hosts = facts.hosts;
        // Relays belong to the session that carried them.
        self.relays.clear();
        self.ears = facts.ears;
        self.audio = facts.audio;
        self.owner = facts.owner;
        if self.audio.as_ref().is_some_and(|a| self.remote_took(a)) {
            self.last_owner_linux = false;
            self.remote_owns = true;
        }
        if self.owner.as_deref() == Some(&[1]) {
            self.last_owner_linux = true;
            self.remote_owns = false;
        }
        self.sync_hold(true, out);
        if self.automatic {
            self.announce(out);
        }
        // The take asks at the play that requested it, capped at the
        // aggregate (#421); a #307 core carries none.
        let category = self
            .requested_category
            .take()
            .map_or(self.category, |c| c.min(self.category));
        let took = match self
            .decide_take(TakeoverTrigger::ReconnectedForPlayback, category)
            .action
        {
            // Like an ordinary reconnect: no take, nothing pending.
            TakeoverAction::TakeOver if !self.auto_take_allowed() => {
                self.say(CONNECT_MODE_SKIP);
                false
            }
            TakeoverAction::TakeOver => {
                self.request(now, true, Cause::Playback, out);
                self.say("AUTO reconnected for new playback; requested Linux ownership");
                true
            }
            // An ordinary reconnect: no playback asked for it.
            TakeoverAction::Skip(TakeoverSkip::AwaitingNewPlayback) => false,
            TakeoverAction::Skip(_) => {
                self.say(
                    "AUTO reconnected; takeover blocked by remote activity, wear, or missing host state",
                );
                false
            }
        };
        // An ordinary reconnect takes from an idle other host (#324).
        let connect_take = !took && self.automatic && self.connect_take_allowed();
        if connect_take {
            self.connect_take(now, out);
        }
        self.recovery = CaseRecovery::Immediate;
        self.reset_baseline(now);
        // The take was for playback seen before this baseline: a first
        // sample that finds it stopped is a stop edge: the take is kept
        // (#262) and, while still pending, defers its resume (#205).
        self.take_assumes_playing = took;
        // The session's queued OwnsConnection=0 reports predate this take.
        self.reconnect_take_pending = took || connect_take;
        out.push(Command::DrainPlayback);
        self.retry_delay = FIRST_RETRY;
        self.say("RECONNECTED with fresh routing and wear state");
    }

    /// A session start or reconnect may take from an idle other host, as the
    /// Mac does (#324, spike #325): no take held or pending, the connect
    /// mode allows it, and the policy allows it with Linux asking at 100.
    fn connect_take_allowed(&self) -> bool {
        !self.requested_take
            && self.pending.is_none()
            && self.auto_take_allowed()
            && self
                .decide_take(TakeoverTrigger::Connected, AUDIO_CATEGORY_IDLE)
                .action
                == TakeoverAction::TakeOver
    }

    /// The connect take: only "owning" is sent. It is idle, so it stays
    /// (#262) and its A2DP resume waits until Linux plays.
    fn connect_take(&mut self, now: Instant, out: &mut Vec<Command>) {
        self.request(now, true, Cause::Playback, out);
        self.resume_deferred = true;
        self.say(CONNECT_TAKE);
    }

    /// `Auto`'s one ownership decision (#324, #452): Some(true) takes (for
    /// playback if a play edge was deferred, #490), or keeps an owner [1];
    /// Some(false) releases; None waits for the routing reports. An [1]
    /// reported in the wait (`owns`) keeps; past the deadline, or when no
    /// report can allow the take, it releases.
    pub(super) fn connect_decision(&self, now: Instant, owns: bool) -> Option<bool> {
        use crate::bluetooth::aacp::AudioSourceType::Media;
        let expired = now >= self.connect_until?;
        let gated = !self.automatic
            || self.requested_take
            || self.pending.is_some()
            || !self.auto_take_allowed();
        // A remote media source needs its host's relay (#272).
        let complete = !self.hosts.is_empty()
            && !self.ears.is_empty()
            && self.audio.as_ref().is_some_and(|a| {
                a.mac == self.local || a.r#type != Media || self.relays.contains_key(&a.mac)
            });
        if !expired
            && !gated
            && complete
            && let Some(category) = self.deferred_play
        {
            // A play edge deferred for these reports (#490): a playback take
            // at its own category, if the policy allows it now.
            Some(
                self.decide_poll(Some(false), true, category).action == TakeoverAction::TakeOver
                    && !self.ear_resumed(now),
            )
        } else if !expired && !gated && self.connect_take_allowed() {
            Some(true)
        } else if expired || gated || complete {
            Some(false)
        } else if owns {
            Some(true)
        } else {
            None
        }
    }

    /// End `Auto`'s wait: a lost session or quit sends nothing; otherwise
    /// decide once the routing reports allow it (#452).
    pub(super) fn settle_connect(&mut self, now: Instant, owns: bool, out: &mut Vec<Command>) {
        if !self.live() || self.quitting {
            self.connect_until = None;
        }
        if let Some(take) = self.connect_decision(now, owns) {
            self.decide_connect(now, take, out);
        }
    }

    pub(super) fn decide_connect(&mut self, now: Instant, take: bool, out: &mut Vec<Command>) {
        self.connect_until = None;
        let deferred = self.deferred_play.take().is_some();
        // Linux played in the wait (#452): no later first sample resumes.
        let played = self.was_playing == Some(true);
        if take && self.owner.as_deref() == Some(&[1]) {
            // The AirPods already report Linux (an app restart while it
            // owned): keep them as an idle take (#324). A remote's hold from
            // the wait ends as an [1] ACK would end it (#264).
            self.remote_owns = false;
            self.ear_resume_until = None;
            self.sync_hold(false, out);
            self.requested_take = true;
            self.last_owner_linux = true;
            self.routed_away = false;
            self.resume_deferred = !played;
            // As the ACK does; only a non-gated (automatic) wait gets here,
            // so the helper resumes exactly when Linux `played`.
            self.route_held_take(out);
            self.say("AUTO connected; Linux already owns the AirPods, keeping them");
        } else if take && deferred {
            self.request(now, true, Cause::Playback, out);
            self.say(PLAYBACK_TAKE);
        } else if take {
            self.connect_take(now, out);
            // Its ACK resumes.
            self.resume_deferred = !played;
        } else if !self.requested_take && self.pending.is_none() {
            // Tracked like an automatic release (#54) only when Linux
            // owns: the AirPods reply only to a real change (#159, #289). A
            // release already pending covers it: a second "not owning"
            // would get a reply that acknowledges nothing.
            let grace = self.ear_resume_until;
            self.request(now, false, Cause::Playback, out);
            // On 84ac699 this went out at Auto, before any reinsertion: a
            // #313 grace from the wait stays.
            self.ear_resume_until = grace;
            if self.owner.as_deref() != Some(&[1]) {
                self.pending = None;
            }
        }
    }
}
