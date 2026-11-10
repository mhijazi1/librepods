//! Linux's call state (#11).

use super::*;

impl Handoff {
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
        if self.category_relay && self.category != AUDIO_CATEGORY_IDLE {
            out.push(Command::RelayCategory(self.category));
        }
    }
}
