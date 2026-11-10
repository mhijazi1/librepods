//! Playback samples and the takeover decisions they lead to.

use super::*;

impl Handoff {
    pub(super) fn playback(
        &mut self,
        now: Instant,
        sampled_at: Instant,
        category: u32,
        new_source: Option<u32>,
        out: &mut Vec<Command>,
    ) {
        if sampled_at < self.playback_baseline_at {
            return;
        }
        let playing = category > AUDIO_CATEGORY_IDLE;
        let previous = self.was_playing.replace(playing);
        let last_category = std::mem::replace(&mut self.category, category);
        self.sampled_known = self.streams_known;
        let assumed_playing = std::mem::take(&mut self.take_assumes_playing);
        // A rise while already playing (201 to 301) asks for a take like a
        // start (#272), unless Linux already holds one.
        let rose = previous == Some(true) && category > last_category && !self.requested_take;
        // A new source behind playback is a fresh request at its own
        // category, not the aggregate's (#96), also for case recovery (#421).
        let behind = new_source
            .filter(|_| previous == Some(true) && playing && !rose && !self.requested_take);
        let fresh_play = (previous == Some(false) && playing) || rose || behind.is_some();
        // Stop edges also count a take sent for playback before the
        // baseline: a stop during a reconnect take defers its resume (#205).
        let stopped =
            (previous == Some(true) || (previous.is_none() && assumed_playing)) && !playing;
        // A session's first playing sample also uses the deferral: Linux
        // already played when #324's take connected.
        let resume_deferred = (fresh_play || (playing && previous.is_none()))
            && std::mem::take(&mut self.resume_deferred);
        // Every play edge while parked, taken or skipped, gets its ACK or
        // falls back within `PARK_TIMEOUT` (#311).
        if self.park == Park::Parked && (fresh_play || (playing && previous.is_none())) {
            self.park_until = Some(now + PARK_TIMEOUT);
        }
        if !self.automatic {
            return;
        }
        if !self.connected {
            self.recovery.playback(fresh_play, stopped);
            if self.recovery == CaseRecovery::WaitingForPlayback {
                self.requested_category = None;
            }
            if fresh_play && self.recovery == CaseRecovery::PlaybackRequested {
                let requested = Some(behind.unwrap_or(category));
                self.requested_category = self.requested_category.max(requested);
                self.retry_at = now;
                self.say("AUTO new playback requested reconnection after case closure");
            }
            return;
        }
        if self.quitting || self.session_lost {
            return;
        }
        // Linux stopped: the deferred edge is gone (#490).
        if stopped {
            self.deferred_play = None;
        }
        let mut deferred = false;
        // A local call's 501 wins; its end relays the category (#272).
        // During mic use only a change of the relayed value is sent (#505).
        if self.category_relay
            && self.relayed(category) != self.relayed(last_category)
            && !self.local_call
        {
            out.push(Command::RelayCategory(self.relayed(category)));
        }
        // A release whose ACK timed out left the ownership unknown: retake.
        let owning = self.requested_take
            && self.owner.as_deref() == Some(&[1])
            && self.timed_out_release.is_none();
        match self.pending {
            // Linux stayed the idle owner (#262), as the Mac does: a fresh
            // take's host and wear policy is moot.
            None if owning && (fresh_play || resume_deferred) => {
                self.say("AUTO playback started; Linux already owns");
                if resume_deferred {
                    self.resume_if_idle(out);
                }
            }
            None => match self
                .decide_poll(
                    previous.map(|p| p && !fresh_play),
                    playing,
                    behind.unwrap_or(category),
                )
                .action
            {
                TakeoverAction::TakeOver if !self.auto_take_allowed() => {
                    self.say(CONNECT_MODE_SKIP)
                }
                TakeoverAction::TakeOver if self.ear_resumed(sampled_at) => self.say(EAR_RESUMED),
                TakeoverAction::TakeOver => {
                    self.request(now, true, Cause::Playback, out);
                    self.say(PLAYBACK_TAKE);
                }
                TakeoverAction::Skip(TakeoverSkip::InitialState | TakeoverSkip::NoTransition) => {
                    // Linux keeps the AirPods idle, relaying 100 like the
                    // Mac (#262); only a yield path gives them up.
                    if stopped && self.requested_take {
                        self.say("AUTO playback stopped; keeping ownership");
                    }
                }
                TakeoverAction::Skip(TakeoverSkip::RemoteCategoryHigher { local, remote }) => self
                    .say(format!(
                        "AUTO takeover skipped: the audio source's category {remote} is above Linux's {local}"
                    )),
                // In `Auto`'s wait, a skip only for a report not yet in
                // waits for it (#490), as a Mac never decides without it.
                TakeoverAction::Skip(skip)
                    if self.connect_until.is_some()
                        && match skip {
                            TakeoverSkip::NoRemoteHost => self.hosts.is_empty(),
                            TakeoverSkip::NoBudInEar => self.ears.is_empty(),
                            TakeoverSkip::AudioNotIdle => self.audio.is_none(),
                            _ => false,
                        } =>
                {
                    deferred = true;
                    self.deferred_play = self.deferred_play.max(Some(behind.unwrap_or(category)));
                    self.say("AUTO playback started; deciding once the routing reports are in");
                }
                TakeoverAction::Skip(_) => self
                    .say("AUTO takeover blocked by remote activity, wear, or missing host state"),
            },
            // Remember edges that arrive while a request is in flight, so the
            // acknowledgement (or its timeout) cannot strand Linux with the
            // wrong ownership.
            Some(p) if p.desired => {
                if stopped {
                    self.stop_during_take = true;
                } else if fresh_play {
                    self.stop_during_take = false;
                }
            }
            Some(p) if p.cause == Cause::Playback => {
                // The settled release must not retake for it either.
                if fresh_play && self.ear_resumed(sampled_at) {
                    self.say(EAR_RESUMED);
                } else if fresh_play {
                    let requested = Some(behind.unwrap_or(category));
                    self.play_during_release = self.play_during_release.max(requested);
                } else if stopped {
                    self.play_during_release = None;
                }
            }
            Some(_) => {}
        }
        // Any other play edge, taken or skipped, ends `Auto`'s wait (#452).
        if fresh_play && !deferred {
            self.connect_until = self.connect_until.map(|_| now);
        }
    }

    pub(super) fn request(
        &mut self,
        now: Instant,
        desired: bool,
        cause: Cause,
        out: &mut Vec<Command>,
    ) {
        if desired {
            self.requested_take = true;
            self.routed_away = false;
        }
        self.pending = Some(Pending {
            desired,
            since: now,
            cause,
        });
        self.stop_during_take = false;
        self.play_during_release = None;
        self.reconnect_take_pending = false;
        self.timed_out_release = None;
        self.resume_deferred = false;
        self.ear_resume_until = None;
        out.push(Command::SendOwnership(desired));
    }

    pub(super) fn reset_baseline(&mut self, now: Instant) {
        self.was_playing = None;
        self.take_assumes_playing = false;
        self.playback_baseline_at = now;
    }

    /// A playback poll's decision, Linux asking at `category` (#96).
    pub(super) fn decide_poll(
        &self,
        previous: Option<bool>,
        playing: bool,
        category: u32,
    ) -> TakeoverDecision {
        let routing = RoutingSnapshot {
            local_category: category,
            ..self.routing()
        };
        decide_takeover(TakeoverInputs {
            baseline_taken: previous.is_some(),
            was_playing: previous == Some(true),
            is_playing: playing,
            // The probe keeps no self-pause flag (#841 semantics).
            i_paused_the_media: false,
            bud_in_ear: routing.bud_in_ear,
            trigger: TakeoverTrigger::PlaybackPoll,
            handoff: Some(HandoffGuards {
                routing,
                recovery: self.recovery,
            }),
        })
    }

    /// "Connect to this computer" (#16): when last connected, an automatic
    /// take (`Cause::Playback`) needs Linux to be the last owner. A mode gate
    /// like `automatic`, checked once `decide_takeover` allowed the take; it
    /// is not a routing guard, so the takeover policy stays unchanged. A
    /// manual take is never gated.
    pub(super) fn auto_take_allowed(&self) -> bool {
        self.connect_mode == ConnectMode::Automatically || self.last_owner_linux
    }

    /// Another host plays media or is in a call: it has the AirPods, unless a
    /// local call holds them (it then refuses the request that follows).
    pub(super) fn remote_took(&self, source: &AudioSource) -> bool {
        use crate::bluetooth::aacp::AudioSourceType::{Call, Media};
        source.mac != self.local
            && matches!(source.r#type, Media | Call)
            && !(self.call_or_mic() && self.requested_take)
    }

    /// A take's decision, Linux asking at `local_category`.
    pub(super) fn decide_take(
        &self,
        trigger: TakeoverTrigger,
        local_category: u32,
    ) -> TakeoverDecision {
        let routing = RoutingSnapshot {
            local_category,
            ..self.routing()
        };
        decide_takeover(TakeoverInputs::handoff(trigger, routing, self.recovery))
    }
}
