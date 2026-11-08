//! The one libpulse connect and operation wait. Both are bounded, so a
//! sound server that accepts the socket but never answers fails a query with
//! `io::ErrorKind::TimedOut` instead of wedging the thread that asked.
//!
//! Sink and source info is read here with raw libpulse-sys calls (#193):
//! libpulse-binding 2.30.1 transmutes their `state` into an enum that lacks
//! INIT (-2) and UNLINKED (-3), which aborts (debug) or is UB (release) when
//! an entry is torn down while listed.

use libpulse_binding::context::{Context, FlagSet as ContextFlagSet, State as ContextState};
use libpulse_binding::mainloop::standard::Mainloop;
use libpulse_binding::operation::{Operation, State as OperationState};
use libpulse_binding::time::MicroSeconds;
use libpulse_binding::volume::ChannelVolumes;
use libpulse_sys as sys;
use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char, c_void};
use std::io;
use std::ptr;
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
// shortcut: (#125) each operation `wait` gives up on leaks its boxed callback
// and the emptied Rc allocation of `state`, a few dozen bytes.
pub(crate) fn update<T>(state: &Weak<RefCell<T>>, f: impl FnOnce(&mut T)) {
    if let Some(state) = state.upgrade() {
        f(&mut state.borrow_mut());
    }
}

/// What LibrePods reads of a sink.
pub(crate) struct Sink {
    pub(crate) name: Option<String>,
    pub(crate) device_string: Option<String>,
    pub(crate) bluez_path: Option<String>,
    pub(crate) volume: ChannelVolumes,
    pub(crate) monitor_source: u32,
}

/// Which sinks `sinks` lists.
pub(crate) enum Sinks<'a> {
    All,
    Name(&'a str),
    Index(u32),
}

/// The sinks `which` picks, on a new connection named `name`.
pub(crate) fn sinks(name: &str, which: Sinks) -> io::Result<Vec<Sink>> {
    let sink_name = match which {
        Sinks::Name(n) => CString::new(n).map_err(|_| pulse_error("sink info"))?,
        _ => CString::default(),
    };
    let cb = Some(sink_cb as extern "C" fn(_, _, _, _));
    query(name, "sink info", |context, list| match which {
        // SAFETY: `context` is Ready, `list` is what `sink_cb` expects, and
        // libpulse copies `sink_name` before this returns.
        Sinks::All => unsafe { sys::pa_context_get_sink_info_list(context, cb, list) },
        // SAFETY: as above.
        Sinks::Name(_) => unsafe {
            sys::pa_context_get_sink_info_by_name(context, sink_name.as_ptr(), cb, list)
        },
        // SAFETY: as above.
        Sinks::Index(i) => unsafe { sys::pa_context_get_sink_info_by_index(context, i, cb, list) },
    })
}

/// The indices of every monitor source (one with `monitor_of_sink` set), on
/// a new connection named `name`.
pub(crate) fn monitor_sources(name: &str) -> io::Result<Vec<u32>> {
    let cb = Some(source_cb as extern "C" fn(_, _, _, _));
    let sources: Vec<Option<u32>> = query(name, "source list", |context, list| {
        // SAFETY: `context` is Ready and `list` is what `source_cb` expects.
        unsafe { sys::pa_context_get_source_info_list(context, cb, list) }
    })?;
    Ok(sources.into_iter().flatten().collect())
}

/// The default sink's name, on a new connection named `name`. Read raw like
/// `sinks`: the binding's server info converts `sample_spec` into an enum
/// the same way (#40).
pub(crate) fn default_sink_name(name: &str) -> io::Result<Option<String>> {
    let cb = Some(server_cb as extern "C" fn(_, _, _));
    let names: Vec<Option<String>> = query(name, "server info", |context, list| {
        // SAFETY: `context` is Ready and `list` is what `server_cb` expects.
        unsafe { sys::pa_context_get_server_info(context, cb, list) }
    })?;
    Ok(names.into_iter().next().flatten())
}

/// A libpulse-sys context on a binding `Mainloop`; libpulse-binding keeps
/// its own context pointer private.
struct RawContext(*mut sys::pa_context);

impl RawContext {
    /// Connect as `name` on `mainloop`, within `CONNECT_TIMEOUT`. The
    /// context must be dropped before `mainloop`.
    fn connect(mainloop: &mut Mainloop, name: &str) -> io::Result<Self> {
        let name = CString::new(name).map_err(|_| pulse_error("context"))?;
        let api: &sys::pa_mainloop_api = mainloop.get_api().as_ref();
        // SAFETY: `api` is `mainloop`'s own vtable (same libpulse-sys), live
        // while the mainloop is; libpulse copies `name`.
        let context = unsafe { sys::pa_context_new(api, name.as_ptr()) };
        if context.is_null() {
            return Err(pulse_error("context"));
        }
        let context = RawContext(context);
        // SAFETY: a live, unconnected context; null server and spawn api
        // mean the default server, and NOAUTOSPAWN as `connect_on` does.
        let connected = unsafe {
            sys::pa_context_connect(
                context.0,
                ptr::null(),
                sys::PA_CONTEXT_NOAUTOSPAWN,
                ptr::null(),
            )
        };
        if connected < 0 {
            return Err(pulse_error("connection"));
        }
        run_until(
            CONNECT_TIMEOUT,
            "connection",
            Instant::now,
            // SAFETY: `context` is live until it drops.
            || context_ready(unsafe { sys::pa_context_get_state(context.0) }.into()),
            |left| iterate(mainloop, left),
        )?;
        Ok(context)
    }
}

impl Drop for RawContext {
    fn drop(&mut self) {
        // SAFETY: `self.0` is the non-null context `connect` created, and
        // this is its only owner.
        unsafe {
            sys::pa_context_disconnect(self.0);
            sys::pa_context_unref(self.0);
        }
    }
}

/// One info query's entries and its end-of-list mark, as libpulse passes it
/// to the callback: 0 while entries come, > 0 at the end, < 0 on error.
struct List<T> {
    items: Vec<T>,
    eol: i32,
}

/// Run the info query `start` begins on a new connection named `name`, and
/// return its entries. `start` gets the Ready context and the callback's
/// userdata. The wait is bounded like `wait`; a query still running when it
/// gives up is cancelled before its userdata is freed.
fn query<T>(
    name: &str,
    what: &str,
    start: impl FnOnce(*mut sys::pa_context, *mut c_void) -> *mut sys::pa_operation,
) -> io::Result<Vec<T>> {
    let mut mainloop = Mainloop::new().ok_or_else(|| pulse_error("mainloop"))?;
    let context = RawContext::connect(&mut mainloop, name)?;
    let list = Box::into_raw(Box::new(List::<T> {
        items: Vec::new(),
        eol: 0,
    }));
    let op = start(context.0, list.cast());
    // SAFETY: `list` came from `Box::into_raw` above; each path below calls
    // this once, when no callback can touch `list` any more.
    let free = || unsafe { Box::from_raw(list) };
    let (result, list) = if op.is_null() {
        (Err(pulse_error(what)), free())
    } else {
        let result = run_until(
            OPERATION_TIMEOUT,
            what,
            Instant::now,
            // SAFETY: we hold `op`'s reference until the unref below.
            || operation_done(unsafe { sys::pa_operation_get_state(op) }, what),
            |left| iterate(&mut mainloop, left),
        );
        // SAFETY (all three): `op` is live until the unref. A Done or
        // Cancelled operation never calls back again, and cancelling a
        // Running one unlinks its callback, so nothing reaches `list` after.
        let list = finish_op(
            || unsafe { sys::pa_operation_get_state(op) },
            || unsafe { sys::pa_operation_cancel(op) },
            || unsafe { sys::pa_operation_unref(op) },
            free,
        );
        (result, list)
    };
    result?;
    match list.eol {
        0 => Err(io::Error::other(format!("PulseAudio {what} ended early"))),
        eol if eol < 0 => Err(io::Error::other(format!(
            "PulseAudio {what} reported an error"
        ))),
        _ => Ok(list.items),
    }
}

/// End an operation `query` waited on: cancel it if still Running, so its
/// callback is unlinked, unref it, and only then `free` the callback's
/// userdata.
fn finish_op<R>(
    state: impl FnOnce() -> OperationState,
    cancel: impl FnOnce(),
    unref: impl FnOnce(),
    free: impl FnOnce() -> R,
) -> R {
    if state() == OperationState::Running {
        cancel();
    }
    unref();
    free()
}

/// Add one callback's entry, or its end mark if it has none, to `list`.
// SAFETY: callers pass the `List<T>` `query` gave libpulse as `list`.
unsafe fn collect<T>(list: *mut c_void, eol: i32, item: Option<T>) {
    // SAFETY: per the contract, `list` is a live `List<T>` that only this
    // callback touches while it runs.
    let list = unsafe { &mut *list.cast::<List<T>>() };
    match item {
        Some(item) => list.items.push(item),
        None => list.eol = eol,
    }
}

extern "C" fn sink_cb(
    _: *mut sys::pa_context,
    info: *const sys::pa_sink_info,
    eol: i32,
    list: *mut c_void,
) {
    // SAFETY: `sinks` passes a `List<Sink>`; a non-null `info` is a
    // filled-in entry.
    unsafe { collect(list, eol, (!info.is_null()).then(|| sink_fields(info))) }
}

extern "C" fn source_cb(
    _: *mut sys::pa_context,
    info: *const sys::pa_source_info,
    eol: i32,
    list: *mut c_void,
) {
    // SAFETY: `monitor_sources` passes a `List<Option<u32>>`; a non-null
    // `info` is a filled-in entry.
    unsafe { collect(list, eol, (!info.is_null()).then(|| monitor_source(info))) }
}

/// Server info has one callback and no end mark: a non-null `info` is the
/// answer and ends the list; a null one is an error.
extern "C" fn server_cb(
    _: *mut sys::pa_context,
    info: *const sys::pa_server_info,
    list: *mut c_void,
) {
    // SAFETY: `default_sink_name` passes a `List<Option<String>>`; a
    // non-null `info` is filled in, and only its `default_sink_name`, null
    // or a C string, is read.
    unsafe {
        if info.is_null() {
            collect::<Option<String>>(list, -1, None);
        } else {
            collect(list, 0, Some(string((*info).default_sink_name)));
            collect::<Option<String>>(list, 1, None);
        }
    }
}

/// A sink's fields. Each is read alone through the raw pointer, so its
/// `state`, which may be a value the sys enum lacks, is never read.
// SAFETY: callers pass an entry libpulse filled in (or a test built).
unsafe fn sink_fields(info: *const sys::pa_sink_info) -> Sink {
    // SAFETY: per the contract; the pointers are null or C strings and a
    // proplist that live for this call.
    unsafe {
        let props = (*info).proplist;
        Sink {
            name: string((*info).name),
            device_string: prop(props, c"device.string"),
            bluez_path: prop(props, c"bluez.path"),
            volume: (*info).volume.into(),
            monitor_source: (*info).monitor_source,
        }
    }
}

/// A source's index if it is a monitor source, read like `sink_fields`.
// SAFETY: callers pass an entry libpulse filled in (or a test built).
unsafe fn monitor_source(info: *const sys::pa_source_info) -> Option<u32> {
    // SAFETY: per the contract.
    unsafe { ((*info).monitor_of_sink != sys::PA_INVALID_INDEX).then_some((*info).index) }
}

/// Property `key` of `props`, which may be null.
// SAFETY: callers pass a null or live proplist.
unsafe fn prop(props: *const sys::pa_proplist, key: &CStr) -> Option<String> {
    if props.is_null() {
        return None;
    }
    // SAFETY: per the contract; the value lives as long as `props`.
    unsafe { string(sys::pa_proplist_gets(props, key.as_ptr())) }
}

// SAFETY: callers pass null or a live C string.
unsafe fn string(s: *const c_char) -> Option<String> {
    // SAFETY: per the contract.
    (!s.is_null()).then(|| unsafe { CStr::from_ptr(s) }.to_string_lossy().into_owned())
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
    use libpulse_binding::volume::Volume;
    use std::cell::Cell;
    use std::mem::MaybeUninit;
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

    /// Runs `finish_op` on an operation in `state` and returns the calls it
    /// made, in order.
    fn finish_calls(state: OperationState) -> Vec<&'static str> {
        let calls = RefCell::new(Vec::new());
        let call = |name| calls.borrow_mut().push(name);
        finish_op(
            || state,
            || call("cancel"),
            || call("unref"),
            || call("free"),
        );
        calls.into_inner()
    }

    #[test]
    fn query_cancels_timed_out_op_before_free() {
        let (result, _) = virtual_run(OPERATION_TIMEOUT, Duration::MAX, || {
            operation_done(OperationState::Running, "test")
        });
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_eq!(
            finish_calls(OperationState::Running),
            ["cancel", "unref", "free"]
        );
    }

    #[test]
    fn query_frees_ended_op_after_unref_without_cancel() {
        for state in [OperationState::Done, OperationState::Cancelled] {
            assert_eq!(finish_calls(state), ["unref", "free"], "{state:?}");
        }
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

    // Each entry below is zeroed C memory with `state` written as the raw C
    // int libpulse sends while a sink or source is set up (INIT, -2) or torn
    // down (UNLINKED, -3), values the sys enums lack.

    #[test]
    fn sink_fields_ignore_init_and_unlinked_state() {
        // SAFETY: a new proplist, freed below; keys and values are C strings.
        let props = unsafe {
            let props = sys::pa_proplist_new();
            sys::pa_proplist_sets(props, c"device.string".as_ptr(), c"AA:BB".as_ptr());
            sys::pa_proplist_sets(props, c"bluez.path".as_ptr(), c"/dev_AA".as_ptr());
            props
        };
        let mut volume = ChannelVolumes::default();
        volume.set(2, Volume(0x8000));
        for state in [-2, -3] {
            let mut info = MaybeUninit::<sys::pa_sink_info>::zeroed();
            let p = info.as_mut_ptr();
            // SAFETY: field writes into `info`, which is never read whole,
            // and reads of it as libpulse would pass it; `props` and the
            // name outlive the reads.
            let (empty, sink) = unsafe {
                (&raw mut (*p).state).cast::<i32>().write(state);
                let empty = sink_fields(p);
                (*p).name = c"bluez_output.AA".as_ptr();
                (*p).proplist = props;
                (*p).volume = *volume.as_ref();
                (*p).monitor_source = 7;
                (empty, sink_fields(p))
            };
            let none = (None, None, None);
            assert_eq!((empty.name, empty.device_string, empty.bluez_path), none);
            assert_eq!(sink.name.as_deref(), Some("bluez_output.AA"), "{state}");
            assert_eq!(sink.device_string.as_deref(), Some("AA:BB"));
            assert_eq!(sink.bluez_path.as_deref(), Some("/dev_AA"));
            assert_eq!(sink.volume, volume);
            assert_eq!(sink.monitor_source, 7);
        }
        // SAFETY: `props` is no longer used.
        unsafe { sys::pa_proplist_free(props) };
    }

    #[test]
    fn source_fields_ignore_init_and_unlinked_state() {
        for state in [-2, -3] {
            for (monitor_of_sink, expected) in [(3, Some(9)), (sys::PA_INVALID_INDEX, None)] {
                let mut info = MaybeUninit::<sys::pa_source_info>::zeroed();
                let p = info.as_mut_ptr();
                // SAFETY: field writes into `info`, which is never read
                // whole, then a read of it as libpulse would pass it.
                let monitor = unsafe {
                    (&raw mut (*p).state).cast::<i32>().write(state);
                    (*p).index = 9;
                    (*p).monitor_of_sink = monitor_of_sink;
                    monitor_source(p)
                };
                assert_eq!(monitor, expected, "state {state}");
            }
        }
    }

    #[test]
    fn server_cb_copies_only_default_sink_name() {
        let mut info = MaybeUninit::<sys::pa_server_info>::zeroed();
        let p = info.as_mut_ptr();
        let run = |info: *const sys::pa_server_info| {
            let mut list = List::<Option<String>> {
                items: Vec::new(),
                eol: 0,
            };
            server_cb(ptr::null_mut(), info, (&raw mut list).cast());
            (list.items, list.eol)
        };
        // SAFETY: field writes into `info`, which is never read whole; the
        // sample format is a raw C int the sys enum lacks.
        unsafe {
            (&raw mut (*p).sample_spec.format).cast::<i32>().write(-7);
            (*p).default_sink_name = c"alsa_output.dac".as_ptr();
        }
        assert_eq!(run(p), (vec![Some("alsa_output.dac".to_string())], 1));
        // SAFETY: as above.
        unsafe { (*p).default_sink_name = ptr::null() };
        assert_eq!(run(p), (vec![None], 1));
        assert_eq!(run(ptr::null()), (vec![], -1));
    }
}
