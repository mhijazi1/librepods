//! The one libpulse connect and operation wait. Both are bounded, so a
//! sound server that accepts the socket but never answers fails a query with
//! `io::ErrorKind::TimedOut` instead of wedging the thread that asked.
//!
//! Sink and source info is read here with raw libpulse-sys calls (#193):
//! libpulse-binding 2.30.1 transmutes their `state` into an enum that lacks
//! INIT (-2) and UNLINKED (-3), which aborts (debug) or is UB (release) when
//! an entry is torn down while listed. Every other operation with a
//! callback is raw too (#125): the binding never frees the callback of an
//! operation that is cancelled or never ends.

use libpulse_binding::context::{Context, FlagSet as ContextFlagSet, State as ContextState};
use libpulse_binding::mainloop::standard::Mainloop;
use libpulse_binding::operation::{Operation, State as OperationState};
use libpulse_binding::time::MicroSeconds;
use libpulse_binding::volume::ChannelVolumes;
use libpulse_sys as sys;
use std::collections::BTreeMap;
use std::ffi::{CStr, CString, c_char, c_void};
use std::io;
use std::ptr;
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
/// operation that fails or times out is cancelled. Only for operations
/// made with a `None` callback: the binding leaks any other (#125), so
/// those are raw `RawConnection` queries.
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

/// What LibrePods reads of a sink input (a playback stream).
pub(crate) struct SinkInput {
    pub(crate) index: u32,
    pub(crate) sink: u32,
    pub(crate) corked: bool,
    pub(crate) mute: bool,
    pub(crate) volume: ChannelVolumes,
    pub(crate) props: BTreeMap<String, String>,
}

/// What LibrePods reads of a source output (a capture stream).
pub(crate) struct SourceOutput {
    pub(crate) index: u32,
    pub(crate) source: u32,
    pub(crate) corked: bool,
    pub(crate) props: BTreeMap<String, String>,
}

/// What LibrePods reads of a card.
pub(crate) struct Card {
    pub(crate) index: u32,
    pub(crate) name: Option<String>,
    pub(crate) device_string: Option<String>,
    /// The names of its profiles.
    pub(crate) profiles: Vec<String>,
    pub(crate) active_profile: Option<String>,
}

/// The sinks `which` picks, on a new connection named `name`.
pub(crate) fn sinks(name: &str, which: Sinks) -> io::Result<Vec<Sink>> {
    RawConnection::connect(name)?.sinks(which)
}

/// The default sink's name, on a new connection named `name`. Read raw like
/// `sinks`: the binding's server info converts `sample_spec` into an enum
/// the same way (#40).
pub(crate) fn default_sink_name(name: &str) -> io::Result<Option<String>> {
    let cb = Some(server_cb as extern "C" fn(_, _, _));
    let names: Vec<Option<String>> =
        RawConnection::connect(name)?.query("server info", |context, list| {
            // SAFETY: `context` is Ready and `list` is what `server_cb` expects.
            unsafe { sys::pa_context_get_server_info(context, cb, list) }
        })?;
    Ok(names.into_iter().next().flatten())
}

/// Make `sink` the default sink, on a new connection named `name`. False
/// on any error or if the server refused.
pub(crate) fn set_default_sink(name: &str, sink: &str) -> bool {
    let Ok(sink) = CString::new(sink) else {
        return false;
    };
    let cb = Some(success_cb as extern "C" fn(_, _, _));
    let accepted = RawConnection::connect(name).and_then(|mut connection| {
        connection.query("default sink change", |context, list| {
            // SAFETY: `context` is Ready, `list` is what `success_cb`
            // expects, and libpulse copies `sink` before this returns.
            unsafe { sys::pa_context_set_default_sink(context, sink.as_ptr(), cb, list) }
        })
    });
    matches!(accepted.as_deref(), Ok([true]))
}

/// A raw libpulse-sys connection on its own binding `Mainloop`. `context`
/// is declared first, so it is dropped before its mainloop.
pub(crate) struct RawConnection {
    context: RawContext,
    mainloop: Mainloop,
}

impl RawConnection {
    /// Connect as `name` on a new mainloop, within `CONNECT_TIMEOUT`.
    pub(crate) fn connect(name: &str) -> io::Result<Self> {
        let mut mainloop = Mainloop::new().ok_or_else(|| pulse_error("mainloop"))?;
        let context = RawContext::connect(&mut mainloop, name)?;
        Ok(RawConnection { context, mainloop })
    }

    /// Whether the connection is still Ready, so it can be queried again.
    pub(crate) fn ready(&self) -> bool {
        // SAFETY: `context` is live until it drops.
        let state = unsafe { sys::pa_context_get_state(self.context.0) };
        ContextState::from(state) == ContextState::Ready
    }

    /// The sinks `which` picks.
    pub(crate) fn sinks(&mut self, which: Sinks) -> io::Result<Vec<Sink>> {
        let sink_name = match which {
            Sinks::Name(n) => CString::new(n).map_err(|_| pulse_error("sink info"))?,
            _ => CString::default(),
        };
        let cb = Some(sink_cb as extern "C" fn(_, _, _, _));
        self.query("sink info", |context, list| match which {
            // SAFETY: `context` is Ready, `list` is what `sink_cb` expects,
            // and libpulse copies `sink_name` before this returns.
            Sinks::All => unsafe { sys::pa_context_get_sink_info_list(context, cb, list) },
            // SAFETY: as above.
            Sinks::Name(_) => unsafe {
                sys::pa_context_get_sink_info_by_name(context, sink_name.as_ptr(), cb, list)
            },
            // SAFETY: as above.
            Sinks::Index(i) => unsafe {
                sys::pa_context_get_sink_info_by_index(context, i, cb, list)
            },
        })
    }

    /// The indices of every monitor source (one with `monitor_of_sink` set).
    pub(crate) fn monitor_sources(&mut self) -> io::Result<Vec<u32>> {
        let cb = Some(source_cb as extern "C" fn(_, _, _, _));
        let sources: Vec<Option<u32>> = self.query("source list", |context, list| {
            // SAFETY: `context` is Ready and `list` is what `source_cb` expects.
            unsafe { sys::pa_context_get_source_info_list(context, cb, list) }
        })?;
        Ok(sources.into_iter().flatten().collect())
    }

    /// Every sink input, or only the one at `index`.
    pub(crate) fn sink_inputs(&mut self, index: Option<u32>) -> io::Result<Vec<SinkInput>> {
        let cb = Some(sink_input_cb as extern "C" fn(_, _, _, _));
        self.query("sink input info", |context, list| match index {
            // SAFETY: `context` is Ready and `list` is what `sink_input_cb`
            // expects.
            None => unsafe { sys::pa_context_get_sink_input_info_list(context, cb, list) },
            // SAFETY: as above.
            Some(i) => unsafe { sys::pa_context_get_sink_input_info(context, i, cb, list) },
        })
    }

    /// Every source output.
    pub(crate) fn source_outputs(&mut self) -> io::Result<Vec<SourceOutput>> {
        let cb = Some(source_output_cb as extern "C" fn(_, _, _, _));
        self.query("source output list", |context, list| {
            // SAFETY: `context` is Ready and `list` is what
            // `source_output_cb` expects.
            unsafe { sys::pa_context_get_source_output_info_list(context, cb, list) }
        })
    }

    /// Every card.
    pub(crate) fn cards(&mut self) -> io::Result<Vec<Card>> {
        let cb = Some(card_cb as extern "C" fn(_, _, _, _));
        self.query("card list", |context, list| {
            // SAFETY: `context` is Ready and `list` is what `card_cb` expects.
            unsafe { sys::pa_context_get_card_info_list(context, cb, list) }
        })
    }

    /// Run the query `start` begins and return its entries. `start` gets
    /// the context and the callback's userdata. The wait is bounded like
    /// `wait`; a query still running when it gives up is cancelled before
    /// its userdata is freed.
    fn query<T>(
        &mut self,
        what: &str,
        start: impl FnOnce(*mut sys::pa_context, *mut c_void) -> *mut sys::pa_operation,
    ) -> io::Result<Vec<T>> {
        let list = Box::into_raw(Box::new(List::<T> {
            items: Vec::new(),
            eol: 0,
        }));
        let op = start(self.context.0, list.cast());
        // SAFETY: `list` came from `Box::into_raw` above; each path below
        // calls this once, when no callback can touch `list` any more.
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
                |left| iterate(&mut self.mainloop, left),
            );
            // SAFETY (all three): `op` is live until the unref. A Done or
            // Cancelled operation never calls back again, and cancelling a
            // Running one unlinks its callback, so nothing reaches `list`
            // after.
            let list = finish_op(
                || unsafe { sys::pa_operation_get_state(op) },
                || unsafe { sys::pa_operation_cancel(op) },
                || unsafe { sys::pa_operation_unref(op) },
                free,
            );
            (result, list)
        };
        result?;
        list.finish(what)
    }
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

/// One query's entries and its end-of-list mark, as libpulse passes it to
/// the callback: 0 while entries come, > 0 at the end, < 0 on error.
struct List<T> {
    items: Vec<T>,
    eol: i32,
}

impl<T> List<T> {
    /// The entries once the operation is over: Err if the server reported
    /// an error or the list never reached its end (#11).
    fn finish(self, what: &str) -> io::Result<Vec<T>> {
        match self.eol {
            0 => Err(io::Error::other(format!("PulseAudio {what} ended early"))),
            eol if eol < 0 => Err(io::Error::other(format!(
                "PulseAudio {what} reported an error"
            ))),
            _ => Ok(self.items),
        }
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

/// Add one callback's entry, or its end mark if it has none, to `list`. An
/// error mark stays, even if an end mark follows.
// SAFETY: callers pass the `List<T>` `query` gave libpulse as `list`.
unsafe fn collect<T>(list: *mut c_void, eol: i32, item: Option<T>) {
    // SAFETY: per the contract, `list` is a live `List<T>` that only this
    // callback touches while it runs.
    let list = unsafe { &mut *list.cast::<List<T>>() };
    match item {
        Some(item) => list.items.push(item),
        None if list.eol >= 0 => list.eol = eol,
        None => {}
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

/// A success callback is called once: `success` is the answer and ends the
/// list.
extern "C" fn success_cb(_: *mut sys::pa_context, success: i32, list: *mut c_void) {
    // SAFETY: `set_default_sink` passes a `List<bool>`.
    unsafe {
        collect(list, 0, Some(success != 0));
        collect::<bool>(list, 1, None);
    }
}

extern "C" fn sink_input_cb(
    _: *mut sys::pa_context,
    info: *const sys::pa_sink_input_info,
    eol: i32,
    list: *mut c_void,
) {
    // SAFETY: `sink_inputs` passes a `List<SinkInput>`; a non-null `info`
    // is a filled-in entry.
    unsafe {
        collect(
            list,
            eol,
            (!info.is_null()).then(|| sink_input_fields(info)),
        )
    }
}

extern "C" fn source_output_cb(
    _: *mut sys::pa_context,
    info: *const sys::pa_source_output_info,
    eol: i32,
    list: *mut c_void,
) {
    // SAFETY: `source_outputs` passes a `List<SourceOutput>`; a non-null
    // `info` is a filled-in entry.
    unsafe {
        collect(
            list,
            eol,
            (!info.is_null()).then(|| source_output_fields(info)),
        )
    }
}

extern "C" fn card_cb(
    _: *mut sys::pa_context,
    info: *const sys::pa_card_info,
    eol: i32,
    list: *mut c_void,
) {
    // SAFETY: `cards` passes a `List<Card>`; a non-null `info` is a
    // filled-in entry.
    unsafe { collect(list, eol, (!info.is_null()).then(|| card_fields(info))) }
}

/// A sink input's fields, each read alone through the raw pointer like
/// `sink_fields`.
// SAFETY: callers pass an entry libpulse filled in (or a test built).
unsafe fn sink_input_fields(info: *const sys::pa_sink_input_info) -> SinkInput {
    // SAFETY: per the contract; the proplist is null or live for this call.
    unsafe {
        SinkInput {
            index: (*info).index,
            sink: (*info).sink,
            corked: (*info).corked != 0,
            mute: (*info).mute != 0,
            volume: volume((*info).volume),
            props: props((*info).proplist),
        }
    }
}

/// A source output's fields, read like `sink_input_fields`.
// SAFETY: callers pass an entry libpulse filled in (or a test built).
unsafe fn source_output_fields(info: *const sys::pa_source_output_info) -> SourceOutput {
    // SAFETY: per the contract; the proplist is null or live for this call.
    unsafe {
        SourceOutput {
            index: (*info).index,
            source: (*info).source,
            corked: (*info).corked != 0,
            props: props((*info).proplist),
        }
    }
}

/// A card's fields, read like `sink_input_fields`.
// SAFETY: callers pass an entry libpulse filled in (or a test built).
unsafe fn card_fields(info: *const sys::pa_card_info) -> Card {
    // SAFETY: per the contract; `profiles2` is null or holds `n_profiles`
    // profile pointers, and every pointer and string lives for this call.
    unsafe {
        let profiles = (*info).profiles2;
        let n_profiles = if profiles.is_null() {
            0
        } else {
            (*info).n_profiles as usize
        };
        Card {
            index: (*info).index,
            name: string((*info).name),
            device_string: prop((*info).proplist, c"device.string"),
            profiles: (0..n_profiles)
                .filter_map(|i| profile_name(*profiles.add(i)))
                .collect(),
            active_profile: profile_name((*info).active_profile2),
        }
    }
}

// SAFETY: callers pass a null or live profile.
unsafe fn profile_name(profile: *const sys::pa_card_profile_info2) -> Option<String> {
    // SAFETY: per the contract.
    unsafe { (!profile.is_null()).then(|| string((*profile).name))? }
}

/// `volume` as the binding holds it. A channel count above
/// `PA_CHANNELS_MAX` would make `ChannelVolumes::get` panic, so it reads as
/// no channels.
fn volume(mut volume: sys::pa_cvolume) -> ChannelVolumes {
    if volume.channels > sys::PA_CHANNELS_MAX {
        volume.channels = 0;
    }
    volume.into()
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
            volume: volume((*info).volume),
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

/// The string properties of `props`, which may be null: the map the
/// binding's `iter` and `get_str` give, so binary values are skipped.
// SAFETY: callers pass a null or live proplist.
unsafe fn props(props: *const sys::pa_proplist) -> BTreeMap<String, String> {
    if props.is_null() {
        return BTreeMap::new();
    }
    let mut state = ptr::null_mut();
    // SAFETY: per the contract; `state` is the iteration cursor libpulse
    // keeps, and keys and values live as long as `props`.
    let keys = std::iter::from_fn(|| {
        let key = unsafe { sys::pa_proplist_iterate(props, &mut state) };
        (!key.is_null()).then_some(key)
    });
    keys.filter_map(|key| unsafe {
        Some((string(key)?, string(sys::pa_proplist_gets(props, key))?))
    })
    .collect()
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
    use libpulse_binding::proplist::Proplist;
    use libpulse_binding::volume::Volume;
    use std::cell::{Cell, RefCell};
    use std::mem::MaybeUninit;

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

    /// Counts its drops in a shared counter.
    struct Counted<'a>(&'a Cell<u32>);

    impl Drop for Counted<'_> {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    #[test]
    fn abandoned_query_frees_userdata() {
        // `None` is the null-op path, which only frees.
        for state in [
            Some(OperationState::Running),
            Some(OperationState::Done),
            Some(OperationState::Cancelled),
            None,
        ] {
            let drops = Cell::new(0);
            let calls = RefCell::new(Vec::new());
            let call = |name| {
                assert_eq!(drops.get(), 0, "{name} after a drop");
                calls.borrow_mut().push(name);
            };
            // The userdata `query` hands libpulse, filled as a callback does.
            let list = Box::into_raw(Box::new(List::<Counted> {
                items: Vec::new(),
                eol: 0,
            }));
            // SAFETY: `list` is a live `List<Counted>`.
            unsafe {
                collect(list.cast(), 0, Some(Counted(&drops)));
                collect(list.cast(), 0, Some(Counted(&drops)));
            }
            // SAFETY: called once, as `query` does.
            let free = || {
                call("free");
                unsafe { Box::from_raw(list) }
            };
            let list = match state {
                Some(state) => finish_op(|| state, || call("cancel"), || call("unref"), free),
                None => free(),
            };
            assert_eq!(drops.get(), 0);
            drop(list);
            assert_eq!(drops.get(), 2, "{state:?}");
            let expected: &[&str] = match state {
                Some(OperationState::Running) => &["cancel", "unref", "free"],
                Some(_) => &["unref", "free"],
                None => &["free"],
            };
            assert_eq!(*calls.borrow(), expected, "{state:?}");
        }
    }

    /// What one query gives after its callback got `marks` (entry, eol).
    fn listed(marks: &[(Option<i32>, i32)]) -> io::Result<Vec<i32>> {
        let mut list = List {
            items: Vec::new(),
            eol: 0,
        };
        for &(item, eol) in marks {
            // SAFETY: `list` is a live `List<i32>`.
            unsafe { collect((&raw mut list).cast(), eol, item) };
        }
        list.finish("test list")
    }

    #[test]
    fn list_items_then_end_is_ok() {
        let items = listed(&[(Some(1), 0), (Some(2), 0), (None, 1)]);
        assert_eq!(items.unwrap(), [1, 2]);
        assert_eq!(listed(&[(None, 1)]).unwrap(), Vec::<i32>::new());
    }

    #[test]
    fn list_error_is_err_even_if_end_follows() {
        for marks in [
            &[(Some(1), 0), (None, -1)][..],
            &[(Some(1), 0), (None, -1), (None, 1)],
            &[(None, 1), (None, -1)],
        ] {
            let e = listed(marks).unwrap_err();
            assert!(e.to_string().ends_with("reported an error"), "{marks:?}");
        }
    }

    #[test]
    fn list_without_end_is_err() {
        for marks in [&[(Some(1), 0)][..], &[]] {
            let e = listed(marks).unwrap_err();
            assert!(e.to_string().ends_with("ended early"), "{marks:?}");
        }
    }

    /// A raw proplist and the binding one with the same entries: two
    /// strings and a binary value. Free the raw one with `pa_proplist_free`.
    fn proplists() -> (*mut sys::pa_proplist, Proplist) {
        let binary = [0xff_u8, 0, 1];
        // SAFETY: a new proplist; keys and values are C strings or `binary`.
        let raw = unsafe {
            let raw = sys::pa_proplist_new();
            sys::pa_proplist_sets(raw, c"media.role".as_ptr(), c"Music".as_ptr());
            sys::pa_proplist_sets(raw, c"application.name".as_ptr(), c"mpv".as_ptr());
            let data = binary.as_ptr().cast();
            sys::pa_proplist_set(raw, c"binary".as_ptr(), data, binary.len());
            raw
        };
        let mut binding = Proplist::new().unwrap();
        binding.set_str("media.role", "Music").unwrap();
        binding.set_str("application.name", "mpv").unwrap();
        binding.set("binary", &binary).unwrap();
        (raw, binding)
    }

    #[test]
    fn sink_input_fields_copy_what_the_binding_gave() {
        let (props, binding) = proplists();
        let mut volume = ChannelVolumes::default();
        volume.set(2, Volume(0x8000));
        let mut info = MaybeUninit::<sys::pa_sink_input_info>::zeroed();
        let p = info.as_mut_ptr();
        // SAFETY: field writes into `info`, which is never read whole, and
        // reads of it as libpulse would pass it; `props` outlives them.
        let (empty, input) = unsafe {
            let empty = sink_input_fields(p);
            (*p).index = 4;
            (*p).sink = 2;
            (*p).corked = 1;
            (*p).mute = 1;
            (*p).volume = *volume.as_ref();
            (*p).proplist = props;
            (empty, sink_input_fields(p))
        };
        assert!(empty.props.is_empty() && !empty.corked && !empty.mute);
        assert_eq!((input.index, input.sink), (4, 2));
        assert!(input.corked && input.mute);
        assert_eq!(input.volume, volume);
        assert_eq!(input.props, crate::stream_monitor::props(&binding));
        assert_eq!(input.props.len(), 2, "the binary value is skipped");
        // A channel count past the maximum reads as no channels.
        // SAFETY: as above.
        let input = unsafe {
            (*p).volume.channels = sys::PA_CHANNELS_MAX + 1;
            sink_input_fields(p)
        };
        assert!(input.volume.get().is_empty());
        // SAFETY: `props` is no longer used.
        unsafe { sys::pa_proplist_free(props) };
    }

    #[test]
    fn source_output_fields_copy_what_the_binding_gave() {
        let (props, binding) = proplists();
        let mut info = MaybeUninit::<sys::pa_source_output_info>::zeroed();
        let p = info.as_mut_ptr();
        // SAFETY: as in `sink_input_fields_copy_what_the_binding_gave`.
        let output = unsafe {
            (*p).index = 5;
            (*p).source = 3;
            (*p).corked = 1;
            (*p).proplist = props;
            source_output_fields(p)
        };
        assert_eq!((output.index, output.source, output.corked), (5, 3, true));
        assert_eq!(output.props, crate::stream_monitor::props(&binding));
        // SAFETY: `props` is no longer used.
        unsafe { sys::pa_proplist_free(props) };
    }

    #[test]
    fn card_fields_copy_names_and_profiles() {
        let profile = |name: &'static CStr| {
            let mut profile = MaybeUninit::<sys::pa_card_profile_info2>::zeroed();
            // SAFETY: a field write into `profile`.
            unsafe { (*profile.as_mut_ptr()).name = name.as_ptr() };
            profile
        };
        let mut a2dp = profile(c"a2dp-sink");
        let mut off = profile(c"off");
        let mut profiles = [a2dp.as_mut_ptr(), off.as_mut_ptr()];
        // SAFETY: a new proplist, freed below.
        let props = unsafe {
            let props = sys::pa_proplist_new();
            sys::pa_proplist_sets(props, c"device.string".as_ptr(), c"AA:BB".as_ptr());
            props
        };
        let mut info = MaybeUninit::<sys::pa_card_info>::zeroed();
        let p = info.as_mut_ptr();
        // SAFETY: field writes into `info`, which is never read whole, and
        // reads of it as libpulse would pass it; the profiles, names and
        // `props` outlive them.
        let (empty, card) = unsafe {
            let empty = card_fields(p);
            (*p).index = 6;
            (*p).name = c"bluez_card.AA".as_ptr();
            (*p).proplist = props;
            (*p).n_profiles = 2;
            (*p).profiles2 = profiles.as_mut_ptr();
            (*p).active_profile2 = a2dp.as_mut_ptr();
            (empty, card_fields(p))
        };
        assert_eq!((empty.name, empty.device_string), (None, None));
        assert!(empty.profiles.is_empty() && empty.active_profile.is_none());
        assert_eq!(card.index, 6);
        assert_eq!(card.name.as_deref(), Some("bluez_card.AA"));
        assert_eq!(card.device_string.as_deref(), Some("AA:BB"));
        assert_eq!(card.profiles, ["a2dp-sink", "off"]);
        assert_eq!(card.active_profile.as_deref(), Some("a2dp-sink"));
        // SAFETY: as above.
        let card = unsafe {
            (*p).active_profile2 = ptr::null_mut();
            card_fields(p)
        };
        assert_eq!(card.active_profile, None);
        // SAFETY: `props` is no longer used.
        unsafe { sys::pa_proplist_free(props) };
    }

    #[test]
    fn success_cb_records_success() {
        for (success, accepted) in [(1, true), (0, false)] {
            // The userdata `set_default_sink` hands libpulse.
            let list = Box::into_raw(Box::new(List::<bool> {
                items: Vec::new(),
                eol: 0,
            }));
            success_cb(ptr::null_mut(), success, list.cast());
            // SAFETY: `list` came from `Box::into_raw` and is freed once.
            let list = finish_op(
                || OperationState::Done,
                || {},
                || {},
                || unsafe { Box::from_raw(list) },
            );
            assert_eq!(list.finish("test").unwrap(), [accepted]);
        }
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
