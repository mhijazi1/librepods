//! Local-call monitoring (#11): the I/O behind `call_detect`.
//!
//! `StreamSource` copies the sound server's capture streams and cards into an
//! `AudioSnapshot`; `CallPoller` turns snapshots into call state changes with
//! the pure `call_detect` rules; `spawn_call_poller` samples once per
//! `CALL_POLL` and sends each change to the handoff task. Tests use a fake
//! `StreamSource`, so no sound server is needed.

use crate::call_detect::{StreamDirection, StreamProps, airpods_card_profile, call_active};
use libpulse_binding::callbacks::ListResult;
use libpulse_binding::context::{Context, FlagSet as ContextFlagSet, State as ContextState};
use libpulse_binding::mainloop::standard::{IterateResult, Mainloop};
use libpulse_binding::operation::{Operation, State as OperationState};
use libpulse_binding::proplist::Proplist;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};
use std::io;
use std::rc::Rc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Interval between call-state samples in production.
pub(crate) const CALL_POLL: Duration = Duration::from_secs(1);

/// Call-state changes: whether Linux was in a call at the instant.
pub(crate) type CallSamples = mpsc::Receiver<(Instant, bool)>;

/// The sound server state call detection needs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AudioSnapshot {
    /// Streams with their property lists.
    pub(crate) streams: Vec<StreamProps>,
    /// Cards: name and active profile.
    pub(crate) cards: Vec<(String, Option<String>)>,
}

/// Where snapshots come from: the sound server in production, a fake in
/// tests.
pub(crate) trait StreamSource: Send {
    fn snapshot(&mut self) -> io::Result<AudioSnapshot>;
}

/// Turns snapshots into call state changes.
#[derive(Debug, Default)]
pub(crate) struct CallPoller {
    last: Option<bool>,
}

impl CallPoller {
    /// Sample `source` once. Some(in_call) only when the call state changed,
    /// including the first sample; None when it did not, or when the source
    /// failed (the last state is kept).
    pub(crate) fn poll(
        &mut self,
        source: &mut dyn StreamSource,
        airpods_mac: &str,
    ) -> Option<bool> {
        let snapshot = match source.snapshot() {
            Ok(snapshot) => snapshot,
            Err(e) => {
                log::debug!("Call monitor: audio snapshot failed: {e}");
                return None;
            }
        };
        let in_call = call_active(
            &snapshot.streams,
            airpods_card_profile(&snapshot.cards, airpods_mac),
        );
        if self.last == Some(in_call) {
            return None;
        }
        self.last = Some(in_call);
        Some(in_call)
    }
}

/// Sample the call state every `CALL_POLL` and send each change. The poller
/// stops once the receiver is dropped.
pub(crate) fn spawn_call_poller(airpods_mac: &str) -> CallSamples {
    spawn_call_poller_with(airpods_mac.to_string(), PulseStreams, CALL_POLL)
}

fn spawn_call_poller_with<S: StreamSource + 'static>(
    airpods_mac: String,
    source: S,
    interval: Duration,
) -> CallSamples {
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let mut state = Some((CallPoller::default(), source));
        while !tx.is_closed() {
            let Some((mut poller, mut source)) = state.take() else {
                break;
            };
            let mac = airpods_mac.clone();
            let sampled_at = Instant::now();
            let sampled = tokio::task::spawn_blocking(move || {
                let change = poller.poll(&mut source, &mac);
                (poller, source, change)
            })
            .await;
            let Ok((poller, source, change)) = sampled else {
                log::error!("Call monitor for {airpods_mac} stopped: sampling panicked");
                break;
            };
            state = Some((poller, source));
            if let Some(in_call) = change
                && tx.send((sampled_at, in_call)).await.is_err()
            {
                break;
            }
            tokio::time::sleep(interval).await;
        }
    });
    rx
}

/// The production source: a short-lived libpulse connection per snapshot
/// (PipeWire's pulse server included), as media_controller.rs makes one per
/// query. It lists the source outputs (capture streams) with their property
/// lists, marking those recording a monitor source, and the cards with their
/// active profile.
pub(crate) struct PulseStreams;

impl StreamSource for PulseStreams {
    fn snapshot(&mut self) -> io::Result<AudioSnapshot> {
        let (mut mainloop, context) = connect()?;
        let introspector = context.introspect();

        let monitors: Rc<RefCell<HashSet<u32>>> = Rc::default();
        let op = introspector.get_source_info_list({
            let monitors = monitors.clone();
            move |result| {
                if let ListResult::Item(source) = result
                    && source.monitor_of_sink.is_some()
                {
                    monitors.borrow_mut().insert(source.index);
                }
            }
        });
        wait(&mut mainloop, &op, "source list")?;

        let outputs: Rc<RefCell<Vec<(u32, BTreeMap<String, String>)>>> = Rc::default();
        let op = introspector.get_source_output_info_list({
            let outputs = outputs.clone();
            move |result| {
                if let ListResult::Item(output) = result {
                    outputs
                        .borrow_mut()
                        .push((output.source, props(&output.proplist)));
                }
            }
        });
        wait(&mut mainloop, &op, "source output list")?;

        let cards: Rc<RefCell<Vec<(String, Option<String>)>>> = Rc::default();
        let op = introspector.get_card_info_list({
            let cards = cards.clone();
            move |result| {
                if let ListResult::Item(card) = result
                    && let Some(name) = &card.name
                {
                    let profile = card
                        .active_profile
                        .as_ref()
                        .and_then(|p| p.name.as_ref().map(|n| n.to_string()));
                    cards.borrow_mut().push((name.to_string(), profile));
                }
            }
        });
        wait(&mut mainloop, &op, "card list")?;

        let monitors = monitors.take();
        let streams = outputs
            .take()
            .into_iter()
            .map(|(source, props)| StreamProps {
                direction: StreamDirection::Capture,
                props,
                monitor: monitors.contains(&source),
            })
            .collect();
        Ok(AudioSnapshot {
            streams,
            cards: cards.take(),
        })
    }
}

fn props(list: &Proplist) -> BTreeMap<String, String> {
    list.iter()
        .filter_map(|key| list.get_str(&key).map(|value| (key, value)))
        .collect()
}

fn pulse_error(what: &str) -> io::Error {
    io::Error::other(format!("PulseAudio {what} failed"))
}

/// Connect to the sound server, blocking until the context is ready.
// shortcut: duplicates media_controller.rs's pulse_connect, which #32 is changing in parallel; fold the two together after it lands (#TBD)
fn connect() -> io::Result<(Mainloop, Context)> {
    let mut mainloop = Mainloop::new().ok_or_else(|| pulse_error("mainloop"))?;
    let mut context =
        Context::new(&mainloop, "LibrePods call monitor").ok_or_else(|| pulse_error("context"))?;
    context
        .connect(None, ContextFlagSet::NOAUTOSPAWN, None)
        .map_err(|_| pulse_error("connection"))?;
    loop {
        if !matches!(mainloop.iterate(true), IterateResult::Success(_)) {
            return Err(pulse_error("mainloop iteration"));
        }
        match context.get_state() {
            ContextState::Ready => return Ok((mainloop, context)),
            ContextState::Failed | ContextState::Terminated => {
                return Err(pulse_error("connection"));
            }
            _ => {}
        }
    }
}

/// Run the mainloop until the operation is done.
// shortcut: no timeout; a sound server that never answers stalls the call monitor (not the handoff task) until restart (#TBD)
fn wait<F: ?Sized>(mainloop: &mut Mainloop, op: &Operation<F>, what: &str) -> io::Result<()> {
    loop {
        match op.get_state() {
            OperationState::Running => {}
            OperationState::Done => return Ok(()),
            OperationState::Cancelled => return Err(pulse_error(what)),
        }
        if !matches!(mainloop.iterate(true), IterateResult::Success(_)) {
            return Err(pulse_error(what));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    const MAC: &str = "AA:BB:CC:DD:EE:FF";

    /// Plays back a script of snapshots and errors, then repeats the last
    /// snapshot.
    struct FakeSource {
        script: Arc<Mutex<VecDeque<io::Result<AudioSnapshot>>>>,
        last: AudioSnapshot,
    }

    impl FakeSource {
        fn new(script: Vec<io::Result<AudioSnapshot>>) -> Self {
            FakeSource {
                script: Arc::new(Mutex::new(script.into())),
                last: AudioSnapshot::default(),
            }
        }
    }

    impl StreamSource for FakeSource {
        fn snapshot(&mut self) -> io::Result<AudioSnapshot> {
            match self.script.lock().unwrap().pop_front() {
                Some(Ok(snapshot)) => {
                    self.last = snapshot.clone();
                    Ok(snapshot)
                }
                Some(Err(e)) => Err(e),
                None => Ok(self.last.clone()),
            }
        }
    }

    fn call_stream() -> StreamProps {
        StreamProps {
            direction: StreamDirection::Capture,
            props: [("media.role".to_string(), "Communication".to_string())].into(),
            monitor: false,
        }
    }

    fn idle() -> io::Result<AudioSnapshot> {
        Ok(AudioSnapshot::default())
    }

    fn in_call() -> io::Result<AudioSnapshot> {
        Ok(AudioSnapshot {
            streams: vec![call_stream()],
            cards: Vec::new(),
        })
    }

    fn hfp() -> io::Result<AudioSnapshot> {
        Ok(AudioSnapshot {
            streams: Vec::new(),
            cards: vec![(
                "bluez_card.AA_BB_CC_DD_EE_FF".to_string(),
                Some("headset-head-unit-msbc".to_string()),
            )],
        })
    }

    fn failure() -> io::Result<AudioSnapshot> {
        Err(io::Error::other("no sound server"))
    }

    #[test]
    fn poller_reports_first_sample_and_changes_only() {
        let mut source = FakeSource::new(vec![idle(), idle(), in_call(), in_call(), idle()]);
        let mut poller = CallPoller::default();
        assert_eq!(poller.poll(&mut source, MAC), Some(false));
        assert_eq!(poller.poll(&mut source, MAC), None);
        assert_eq!(poller.poll(&mut source, MAC), Some(true));
        assert_eq!(poller.poll(&mut source, MAC), None);
        assert_eq!(poller.poll(&mut source, MAC), Some(false));
        assert_eq!(poller.poll(&mut source, MAC), None);
    }

    #[test]
    fn poller_first_sample_in_call() {
        let mut source = FakeSource::new(vec![hfp(), in_call()]);
        let mut poller = CallPoller::default();
        assert_eq!(poller.poll(&mut source, MAC), Some(true));
        // A different reason for the same state is no change.
        assert_eq!(poller.poll(&mut source, MAC), None);
        // The head-unit profile of another device's card is not a call.
        assert_eq!(poller.poll(&mut source, "11:22:33:44:55:66"), None);
    }

    #[test]
    fn poller_keeps_last_state_on_source_error() {
        let mut source = FakeSource::new(vec![
            failure(),
            in_call(),
            failure(),
            failure(),
            in_call(),
            failure(),
            idle(),
        ]);
        let mut poller = CallPoller::default();
        // No state yet: an error reports nothing.
        assert_eq!(poller.poll(&mut source, MAC), None);
        assert_eq!(poller.poll(&mut source, MAC), Some(true));
        // Errors neither end nor repeat the call.
        assert_eq!(poller.poll(&mut source, MAC), None);
        assert_eq!(poller.poll(&mut source, MAC), None);
        assert_eq!(poller.poll(&mut source, MAC), None);
        assert_eq!(poller.poll(&mut source, MAC), None);
        assert_eq!(poller.poll(&mut source, MAC), Some(false));
    }

    #[tokio::test]
    async fn spawned_poller_sends_changes_and_stops_when_dropped() {
        let source = FakeSource::new(vec![
            idle(),
            idle(),
            in_call(),
            failure(),
            in_call(),
            idle(),
        ]);
        let script = source.script.clone();
        let mut rx = spawn_call_poller_with(MAC.to_string(), source, Duration::from_millis(5));
        let mut changes = Vec::new();
        for _ in 0..3 {
            let (_, in_call) = tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("a change")
                .expect("poller running");
            changes.push(in_call);
        }
        assert_eq!(changes, [false, true, false]);
        // Steady state afterwards: nothing more is sent.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), rx.recv())
                .await
                .is_err()
        );
        assert!(script.lock().unwrap().is_empty());
        // Dropping the receiver stops the task: the script is no longer
        // consumed.
        drop(rx);
        tokio::time::sleep(Duration::from_millis(50)).await;
        script.lock().unwrap().push_back(in_call());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(script.lock().unwrap().len(), 1);
    }
}
