//! Linux's call and mic-use state (#11, #505).

use super::*;
use crate::call_detect::CallState;
use crate::media_controller::AUDIO_CATEGORY_MIC;

/// The core's inputs for one call monitor state, the call's first: a call
/// starts before mic use ends and ends before mic use starts, so its 501 is
/// never followed by mic use's relay.
pub(crate) fn local_inputs(state: CallState) -> [Input; 2] {
    [
        Input::LocalCall(state == CallState::Call),
        Input::LocalMic(state == CallState::Mic),
    ]
}

impl Handoff {
    /// A local call or mic use (#505) keeps the AirPods Linux holds or is
    /// taking.
    pub(super) fn call_or_mic(&self) -> bool {
        self.local_call || self.local_mic
    }

    /// The category relayed for Linux's `category`: during mic use the
    /// higher of it and the mic's 200 (#505).
    pub(super) fn relayed(&self, category: u32) -> u32 {
        if self.local_mic {
            category.max(AUDIO_CATEGORY_MIC)
        } else {
            category
        }
    }

    /// The call monitor reported mic use without a call (#505). The Mac
    /// relays 200 for it, so Linux does too, not a call's 501; a call's 501
    /// wins while it lasts.
    pub(super) fn local_mic(&mut self, on: bool, out: &mut Vec<Command>) {
        if self.local_mic == on {
            return;
        }
        self.local_mic = on;
        if self.local_call || !self.automatic || !self.live() {
            return;
        }
        if on {
            self.say("MIC in use on Linux; keeping the AirPods Linux holds until it ends");
        } else {
            self.say("MIC use ended on Linux");
        }
        // At its end, the category it replaced, idle included.
        if self.category_relay {
            out.push(Command::RelayCategory(self.relayed(self.category)));
        }
    }

    /// The call monitor reported Linux's call state (#11).
    pub(super) fn local_call(&mut self, in_call: bool, out: &mut Vec<Command>) {
        if self.local_call == in_call {
            return;
        }
        self.local_call = in_call;
        // Without a live automatic session only the state is kept: nothing
        // can be sent, and in manual mode Linux announces nothing.
        if !self.automatic || !self.live() {
            return;
        }
        out.push(Command::AdvertiseCall(in_call));
        if in_call {
            self.say("CALL started on Linux; keeping the AirPods Linux holds until it ends");
            return;
        }
        self.say("CALL ended on Linux");
        // The call's 501 replaced the category the other hosts had (#272).
        // Linux keeps the AirPods it holds, as after any stop (#262).
        let category = self.relayed(self.category);
        if self.category_relay && category != AUDIO_CATEGORY_IDLE {
            out.push(Command::RelayCategory(category));
        }
    }
}
