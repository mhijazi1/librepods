//! The one libpulse connect and operation wait. Both are bounded, so a
//! sound server that accepts the socket but never answers fails a query with
//! `io::ErrorKind::TimedOut` instead of wedging the thread that asked.

use libpulse_binding::context::{Context, FlagSet as ContextFlagSet, State as ContextState};
use libpulse_binding::mainloop::standard::Mainloop;
use libpulse_binding::operation::{Operation, State as OperationState};
use libpulse_binding::time::MicroSeconds;
use std::cell::RefCell;
use std::io;
use std::rc::Weak;
use std::time::{Duration, Instant};

/// Longest a connection may take to become ready: a local sound server
/// answers in milliseconds, and 2 s is two `CALL_POLL`s.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Longest one operation (a list query, a profile or volume change) may run.
pub(crate) const OPERATION_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) fn pulse_error(what: &str) -> io::Error {
    io::Error::other(format!("PulseAudio {what} failed"))
}

/// Connect to the sound server as `name` on a new mainloop.
pub(crate) fn connect(name: &str) -> io::Result<(Mainloop, Context)> {
    let mut mainloop = Mainloop::new().ok_or_else(|| pulse_error("mainloop"))?;
    let context = connect_on(&mut mainloop, name)?;
    Ok((mainloop, context))
}

/// Connect to the sound server as `name` on `mainloop`, within
/// `CONNECT_TIMEOUT`.
pub(crate) fn connect_on(mainloop: &mut Mainloop, name: &str) -> io::Result<Context> {
    let mut context = Context::new(mainloop, name).ok_or_else(|| pulse_error("context"))?;
    context
        .connect(None, ContextFlagSet::NOAUTOSPAWN, None)
        .map_err(|_| pulse_error("connection"))?;
    let result = run_until(
        CONNECT_TIMEOUT,
        "connection",
        Instant::now,
        || context_ready(context.get_state()),
        |left| iterate(mainloop, left),
    );
    match result {
        Ok(()) => Ok(context),
        Err(e) => {
            context.disconnect();
            Err(e)
        }
    }
}

/// Run `mainloop` until `op` is done, within `OPERATION_TIMEOUT`. An
/// operation that fails or times out is cancelled, so its callback never
/// runs again, but that does not free the callback: see `update`.
pub(crate) fn wait<F: ?Sized>(
    mainloop: &mut Mainloop,
    op: &mut Operation<F>,
    what: &str,
) -> io::Result<()> {
    let result = run_until(
        OPERATION_TIMEOUT,
        what,
        Instant::now,
        || operation_done(op.get_state(), what),
        |left| iterate(mainloop, left),
    );
    if result.is_err() && op.get_state() == OperationState::Running {
        op.cancel();
    }
    result
}

/// Run `f` on what a query callback collects, if the caller still holds it.
/// Query callbacks hold `state` only as a `Weak`, because libpulse-binding
/// 2.30.1 frees a callback only when it gets End or Error, and a cancelled
/// or failed operation gets neither: `Operation::cancel` frees nothing,
/// since `Operation::from_raw` keeps no pointer for a non-null callback.
/// So the collected items go when the caller drops `state`.
// shortcut: each operation `wait` gives up on leaks its boxed callback and
// the emptied Rc allocation of `state`, a few dozen bytes (#125).
pub(crate) fn update<T>(state: &Weak<RefCell<T>>, f: impl FnOnce(&mut T)) {
    if let Some(state) = state.upgrade() {
        f(&mut state.borrow_mut());
    }
}

fn context_ready(state: ContextState) -> io::Result<bool> {
    match state {
        ContextState::Ready => Ok(true),
        ContextState::Failed | ContextState::Terminated => Err(pulse_error("connection")),
        _ => Ok(false),
    }
}

fn operation_done(state: OperationState, what: &str) -> io::Result<bool> {
    match state {
        OperationState::Running => Ok(false),
        OperationState::Done => Ok(true),
        OperationState::Cancelled => Err(pulse_error(what)),
    }
}

/// One mainloop iteration that blocks for at most `left`.
fn iterate(mainloop: &mut Mainloop, left: Duration) -> io::Result<()> {
    let timeout = MicroSeconds(left.as_micros().min(i32::MAX as u128) as u64);
    mainloop
        .prepare(Some(timeout))
        .and_then(|()| mainloop.poll())
        .and_then(|_| mainloop.dispatch())
        .map(drop)
        .map_err(|e| io::Error::other(format!("PulseAudio mainloop iteration failed: {e}")))
}

/// Check `done`, then `step` with the time left, until `done` is true.
/// TimedOut once `timeout` has passed on `now`; an error from `done` or
/// `step` ends the loop at once.
fn run_until(
    timeout: Duration,
    what: &str,
    mut now: impl FnMut() -> Instant,
    mut done: impl FnMut() -> io::Result<bool>,
    mut step: impl FnMut(Duration) -> io::Result<()>,
) -> io::Result<()> {
    let deadline = now() + timeout;
    while !done()? {
        let left = deadline.saturating_duration_since(now());
        if left.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("PulseAudio {what} timed out after {timeout:?}"),
            ));
        }
        step(left)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;

    /// Runs `run_until` on a virtual clock that each step advances by the
    /// time it may block (or by `cap`, if less); returns the result and the
    /// time each step was given.
    fn virtual_run(
        timeout: Duration,
        cap: Duration,
        done: impl FnMut() -> io::Result<bool>,
    ) -> (io::Result<()>, Vec<Duration>) {
        let start = Instant::now();
        let clock = Cell::new(start);
        let steps = RefCell::new(Vec::new());
        let result = run_until(
            timeout,
            "test",
            || clock.get(),
            done,
            |left| {
                steps.borrow_mut().push(left);
                clock.set(clock.get() + left.min(cap));
                Ok(())
            },
        );
        (result, steps.into_inner())
    }

    #[test]
    fn connect_times_out_when_never_ready() {
        let (result, steps) = virtual_run(CONNECT_TIMEOUT, Duration::from_millis(300), || {
            context_ready(ContextState::Connecting)
        });
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        // Each step may block only for what is left, never past the deadline.
        let ms = [2000, 1700, 1400, 1100, 800, 500, 200].map(Duration::from_millis);
        assert_eq!(steps, ms);
    }

    #[test]
    fn wait_times_out_while_running() {
        let (result, steps) = virtual_run(OPERATION_TIMEOUT, Duration::MAX, || {
            operation_done(OperationState::Running, "test")
        });
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        // A step that blocks for all it is given ends the wait after one.
        assert_eq!(steps, [OPERATION_TIMEOUT]);
    }

    #[test]
    fn failed_step_is_error_not_loop() {
        let steps = Cell::new(0);
        let result = run_until(
            OPERATION_TIMEOUT,
            "test",
            Instant::now,
            || Ok(false),
            |_| {
                steps.set(steps.get() + 1);
                Err(io::Error::other("mainloop iteration failed"))
            },
        );
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Other);
        assert_eq!(steps.get(), 1);
        // A failed connection or cancelled operation ends it too.
        for done in [
            context_ready(ContextState::Failed),
            context_ready(ContextState::Terminated),
            operation_done(OperationState::Cancelled, "test"),
        ] {
            let mut done = Some(done);
            let (result, steps) =
                virtual_run(OPERATION_TIMEOUT, Duration::MAX, || done.take().unwrap());
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Other);
            assert!(steps.is_empty());
        }
    }

    #[test]
    fn ready_before_deadline_is_ok() {
        let states = RefCell::new(vec![
            ContextState::Unconnected,
            ContextState::Connecting,
            ContextState::Authorizing,
            ContextState::SettingName,
            ContextState::Ready,
        ]);
        let (result, steps) = virtual_run(CONNECT_TIMEOUT, Duration::from_millis(100), || {
            context_ready(states.borrow_mut().remove(0))
        });
        result.unwrap();
        assert_eq!(steps.len(), 4);
        let mut done = [OperationState::Running, OperationState::Done].into_iter();
        let (result, steps) = virtual_run(OPERATION_TIMEOUT, Duration::from_millis(100), || {
            operation_done(done.next().unwrap(), "test")
        });
        result.unwrap();
        assert_eq!(steps.len(), 1);
    }

    #[test]
    fn abandoned_callback_frees_collected_items() {
        // What libpulse-binding does with the callback of a cancelled
        // operation: it never frees it.
        let item = Rc::new(());
        let state = Rc::new(RefCell::new(Vec::new()));
        let callback: &mut dyn FnMut(Rc<()>) = Box::leak(Box::new({
            let state = Rc::downgrade(&state);
            move |item| update(&state, |items| items.push(item))
        }));
        callback(item.clone());
        assert_eq!(Rc::strong_count(&item), 2);
        drop(state);
        assert_eq!(Rc::strong_count(&item), 1);
        // A late call after the caller gave up is a no-op.
        callback(item.clone());
        assert_eq!(Rc::strong_count(&item), 1);
    }
}
