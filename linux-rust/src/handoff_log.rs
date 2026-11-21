//! Handoff diagnostics (#21): a JSONL log of the handoff core's lines for
//! hardware test sessions, and the `--handoff-report` summary of its last
//! session. The records hold only timestamps, the AirPods MAC and the core's
//! lines, never key material. Logging never fails, stops or stalls
//! switching: the file I/O runs on a writer thread, and a failed open or
//! write stops the log with one warning.

use crate::switching::LineSink;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::fmt;
use std::fs::{DirBuilder, File, OpenOptions, Permissions};
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, FileExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// One JSON object per line of the log.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case")]
pub(crate) enum Record {
    /// Written once when a log is opened; a report covers the records after
    /// the last one.
    SessionStart {
        ts_ms: u64,
        version: String,
        source: Source,
    },
    /// One line of the handoff core for the AirPods `mac`.
    Line {
        ts_ms: u64,
        mac: String,
        kind: Kind,
        line: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Source {
    App,
    Probe,
}

/// What a core line reports, as `classify` reads it.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Kind {
    /// `ACK owner=linux`: Linux took the AirPods.
    AckLinux,
    /// `ACK owner=not-linux`, also the late one after a TIMEOUT.
    AckNotLinux,
    /// `REQUEST owner=linux`, or an AUTO line that requested Linux ownership.
    TakeRequest,
    /// `YIELD` (a lost take) or `REMOTE requested ownership; yielding`.
    Yield,
    Timeout,
    ReconnectFailed,
    Blocked,
    Other,
}

/// The kind of a handoff core line. The core's line texts (handoff.rs) are
/// the contract; `report_counts_lines_of_the_real_core` pins them.
pub(crate) fn classify(line: &str) -> Kind {
    if line.starts_with("ACK owner=linux") {
        Kind::AckLinux
    } else if line.starts_with("ACK owner=not-linux") {
        Kind::AckNotLinux
    } else if line.starts_with("REQUEST owner=linux")
        || (line.starts_with("AUTO ") && line.ends_with("requested Linux ownership"))
    {
        Kind::TakeRequest
    } else if line.starts_with("YIELD ") || line == "REMOTE requested ownership; yielding" {
        Kind::Yield
    } else if line.starts_with("TIMEOUT ") {
        Kind::Timeout
    } else if line.starts_with("RECONNECT failed") {
        Kind::ReconnectFailed
    } else if line.starts_with("BLOCKED ") {
        Kind::Blocked
    } else {
        Kind::Other
    }
}

/// `$XDG_STATE_HOME/librepods/handoff.jsonl`, or under
/// `$HOME/.local/state` when XDG_STATE_HOME is unset, empty or relative
/// (the XDG spec ignores relative paths). None without either.
pub(crate) fn log_path(state_home: Option<OsString>, home: Option<OsString>) -> Option<PathBuf> {
    let absolute = |p: OsString| Some(PathBuf::from(p)).filter(|p| p.is_absolute());
    let state = state_home
        .and_then(absolute)
        .or_else(|| home.and_then(absolute).map(|h| h.join(".local/state")))?;
    Some(state.join("librepods/handoff.jsonl"))
}

fn default_path() -> Option<PathBuf> {
    log_path(std::env::var_os("XDG_STATE_HOME"), std::env::var_os("HOME"))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Past this size (about 5000 lines) a log is rotated to `handoff.jsonl.1`,
/// replacing an older `.1`, when it is next opened.
const MAX_LOG_BYTES: u64 = 1 << 20;

/// Set `mode` on an open file or directory that has group/other bits, through
/// its handle (fchmod), so a swapped path is never chmodded. Warn-only.
fn make_private(handle: &File, mode: u32, path: &Path) {
    let tightened = handle.metadata().and_then(|m| {
        if m.permissions().mode() & 0o077 == 0 {
            return Ok(());
        }
        handle.set_permissions(Permissions::from_mode(mode))
    });
    if let Err(e) = tightened {
        warn!("Handoff log: cannot make {} private: {e}", path.display());
    }
}

fn write_record(file: &mut impl Write, record: &Record) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(record)?;
    bytes.push(b'\n');
    // One write per record, so an O_APPEND writer never interleaves lines.
    file.write_all(&bytes)
}

/// The warning for a rotation that did not leave a fresh log: when the
/// rename happened (`renamed`), the session keeps appending to `rotated`.
fn rotation_warning(path: &Path, rotated: &Path, renamed: bool, e: &io::Error) -> String {
    let (path, rotated) = (path.display(), rotated.display());
    if renamed {
        format!(
            "Handoff log: rotated {path} to {rotated} but cannot open a fresh {path}: {e}; appending to {rotated}"
        )
    } else {
        format!("Handoff log: cannot rotate {path} to {rotated}: {e}; appending to it")
    }
}

/// Records waiting for the writer thread; past this, `Log::line` drops them.
const QUEUE_BOUND: usize = 256;
/// How long dropping a `Log` waits for its writer to drain the queue.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

/// Open `path` for appending (made 0600, its directory 0700) and write the
/// session_start record stamped `started_ms`. A log past `MAX_LOG_BYTES` is
/// first renamed to `<path>.1` (`handoff.jsonl.1`), replacing an older one,
/// and a fresh log started: the report only reads the last session, and a
/// writer still running keeps appending to the renamed file.
fn open_file(path: &Path, source: Source, started_ms: u64) -> io::Result<File> {
    if let Some(dir) = path.parent() {
        DirBuilder::new().recursive(true).mode(0o700).create(dir)?;
        match File::open(dir) {
            Ok(handle) => make_private(&handle, 0o700, dir),
            Err(e) => warn!("Handoff log: cannot make {} private: {e}", dir.display()),
        }
    }
    let open = || {
        OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .mode(0o600)
            .open(path)
    };
    let mut file = open()?;
    make_private(&file, 0o600, path);
    let measured = file.metadata()?;
    if measured.len() > MAX_LOG_BYTES {
        let suffixed = |suffix: &str| {
            let mut name = path.as_os_str().to_owned();
            name.push(suffix);
            PathBuf::from(name)
        };
        let (rotated, lock_path) = (suffixed(".1"), suffixed(".lock"));
        // The lock on `<path>.lock` (dropped, so released, after the reopen)
        // makes check, rename and reopen one step for other openers. A
        // writable file, not the directory: NFS emulates flock with fcntl.
        // Ok holds whether the rename happened and the reopen's result.
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .open(&lock_path);
        let fresh = lock_file.and_then(|lock| {
            make_private(&lock, 0o600, &lock_path);
            #[cfg(test)]
            if let Some(hook) = tests::BEFORE_LOCK.take() {
                hook();
            }
            lock.lock()?;
            // Move only the file measured above: a process that lost a race
            // with another rotator must not move that one's fresh log over .1.
            let moved = std::fs::metadata(path).and_then(|now| {
                if (now.dev(), now.ino()) != (measured.dev(), measured.ino()) {
                    return Ok(false);
                }
                #[cfg(test)]
                if let Some(hook) = tests::BEFORE_RENAME.take() {
                    hook();
                }
                std::fs::rename(path, &rotated).map(|()| true)
            });
            match moved {
                Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
                moved => {
                    let renamed = matches!(moved, Ok(true));
                    if renamed {
                        info!(
                            "Handoff log: rotated {} to {}",
                            path.display(),
                            rotated.display()
                        );
                        #[cfg(test)]
                        if let Some(hook) = tests::AFTER_RENAME.take() {
                            hook();
                        }
                    }
                    Ok((renamed, open()))
                }
            }
        });
        // An error before the rename (lock file, lock, metadata) moved nothing.
        match fresh.unwrap_or_else(|e| (false, Err(e))) {
            (_, Ok(fresh)) => file = fresh,
            (renamed, Err(e)) => warn!("{}", rotation_warning(path, &rotated, renamed, &e)),
        }
    }
    let len = file.metadata()?.len();
    let mut last = [b'\n'];
    if len > 0 {
        file.read_exact_at(&mut last, len - 1)?;
    }
    // End a torn last record, so the session_start is a line of its own.
    if last != [b'\n'] {
        file.write_all(b"\n")?;
    }
    write_record(
        &mut file,
        &Record::SessionStart {
            ts_ms: started_ms,
            version: env!("CARGO_PKG_VERSION").to_string(),
            source,
        },
    )?;
    Ok(file)
}

/// A handoff log. Its file I/O runs on a `handoff-log` writer thread, so a
/// hung state filesystem never stalls the caller. A failed open or write
/// stops the writer after one warning; later lines are dropped silently.
pub(crate) struct Log {
    /// None only while dropping.
    tx: Option<SyncSender<Record>>,
    /// Disconnected when the writer thread ends; a Mutex only so Log is Sync.
    done: Mutex<Receiver<()>>,
    /// The queue is full and its warning was given.
    overflowing: AtomicBool,
    dropped: AtomicUsize,
    overflow_warnings: AtomicUsize,
}

impl Log {
    /// Open the log on the caller's thread (see `open_file`): an Err means
    /// nothing is logged. The appends then run on the writer thread. Only
    /// tests need the synchronous open.
    #[cfg(test)]
    pub(crate) fn open(path: &Path, source: Source) -> io::Result<Log> {
        let file = open_file(path, source, now_ms())?;
        Self::spawn(path, move || Ok(file))
    }

    /// Like `open`, but returns at once: the open runs on the writer thread,
    /// which warns if it fails. Lines sent meanwhile are queued, and the
    /// session_start is stamped now so that no line predates it.
    fn start(path: &Path, source: Source) -> io::Result<Log> {
        let (owned, started_ms) = (path.to_owned(), now_ms());
        Self::spawn(path, move || open_file(&owned, source, started_ms))
    }

    /// Start the writer thread: it runs `opener`, then appends each queued
    /// record to the writer that returned. Err only when the spawn fails.
    fn spawn<W: Write + Send + 'static>(
        path: &Path,
        opener: impl FnOnce() -> io::Result<W> + Send + 'static,
    ) -> io::Result<Log> {
        let (tx, rx) = sync_channel::<Record>(QUEUE_BOUND);
        let (done_tx, done) = mpsc::channel::<()>();
        let path = path.to_owned();
        thread::Builder::new()
            .name("handoff-log".to_string())
            .spawn(move || {
                let _done = done_tx;
                // Drops before _done, so a disconnected done means try_send sees Disconnected.
                let rx = rx;
                let path = path.display();
                let mut writer = match opener() {
                    Ok(writer) => writer,
                    Err(e) => {
                        warn!("Handoff log: cannot open {path}: {e}; not logging");
                        return;
                    }
                };
                for record in rx {
                    if let Err(e) = write_record(&mut writer, &record) {
                        warn!("Handoff log: cannot write {path}: {e}; handoff logging stopped");
                        return;
                    }
                }
            })?;
        Ok(Log {
            tx: Some(tx),
            done: Mutex::new(done),
            overflowing: AtomicBool::new(false),
            dropped: AtomicUsize::new(0),
            overflow_warnings: AtomicUsize::new(0),
        })
    }

    /// Queue a `line` record for the AirPods `mac`, or drop it when the
    /// writer is `QUEUE_BOUND` records behind or has stopped.
    pub(crate) fn line(&self, mac: &str, line: &str) {
        let record = Record::Line {
            ts_ms: now_ms(),
            mac: mac.to_string(),
            kind: classify(line),
            line: line.to_string(),
        };
        let Some(tx) = &self.tx else { return };
        match tx.try_send(record) {
            Ok(()) => self.overflowing.store(false, Ordering::Relaxed),
            Err(TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                if !self.overflowing.swap(true, Ordering::Relaxed) {
                    self.overflow_warnings.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        "Handoff log: writer is not keeping up (a hung state filesystem?); dropping records"
                    );
                }
            }
            // The writer stopped after its own warning.
            Err(TrySendError::Disconnected(_)) => {}
        }
    }

    /// The switching `LineSink` that appends the core's lines for `mac`.
    pub(crate) fn sink(self: Arc<Self>, mac: String) -> LineSink {
        Arc::new(move |line: &str| self.line(&mac, line))
    }
}

impl Drop for Log {
    /// Let the writer drain the queue, but leave a hung one detached after
    /// `DRAIN_TIMEOUT`, so exit never waits on the filesystem.
    fn drop(&mut self) {
        self.tx = None;
        let _ = self
            .done
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .recv_timeout(DRAIN_TIMEOUT);
    }
}

/// Start the log at the default path without waiting on the filesystem, or
/// warn once and return None.
pub(crate) fn open_default(source: Source) -> Option<Log> {
    let Some(path) = default_path() else {
        warn!("Handoff log: neither XDG_STATE_HOME nor HOME is set; not logging");
        return None;
    };
    Log::start(&path, source)
        .inspect_err(|e| warn!("Handoff log: cannot start its writer thread: {e}; not logging"))
        .ok()
}

/// The app's log, opened once per process, so one app run is one session.
/// It is never dropped, so the last queued records may be lost at app exit.
pub(crate) fn app_log() -> Option<Arc<Log>> {
    static LOG: OnceLock<Option<Arc<Log>>> = OnceLock::new();
    LOG.get_or_init(|| open_default(Source::App).map(Arc::new))
        .clone()
}

/// Counts of the last session of a log.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Report {
    started_ms: u64,
    /// From the session_start to its last line record.
    duration_ms: u64,
    takeovers: usize,
    take_requests: usize,
    yields: usize,
    failures: usize,
    retries: usize,
    blocked: usize,
}

/// The report of the records after the last session_start, or None when
/// the log has none. Lines that are not records (a torn last write, even
/// one cut inside a UTF-8 character) are skipped.
pub(crate) fn report(log: &[u8]) -> Option<Report> {
    let mut report = None;
    for record in log
        .split(|&b| b == b'\n')
        .filter_map(|l| serde_json::from_slice(l).ok())
    {
        match record {
            Record::SessionStart { ts_ms, .. } => {
                report = Some(Report {
                    started_ms: ts_ms,
                    ..Report::default()
                })
            }
            Record::Line { ts_ms, kind, .. } => {
                let Some(r) = report.as_mut() else { continue };
                r.duration_ms = ts_ms.saturating_sub(r.started_ms);
                match kind {
                    Kind::AckLinux => r.takeovers += 1,
                    Kind::TakeRequest => r.take_requests += 1,
                    Kind::Yield => r.yields += 1,
                    Kind::Timeout => r.failures += 1,
                    Kind::ReconnectFailed => {
                        // Every RECONNECT failed line schedules a retry.
                        r.failures += 1;
                        r.retries += 1;
                    }
                    Kind::Blocked => r.blocked += 1,
                    Kind::AckNotLinux | Kind::Other => {}
                }
            }
        }
    }
    report
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let secs = self.started_ms / 1000;
        writeln!(
            f,
            "Last handoff session: started @{secs} (unix time, `date -d @{secs}`), lasted {:.1} s",
            self.duration_ms as f64 / 1000.0
        )?;
        writeln!(f, "  takeovers:     {}", self.takeovers)?;
        writeln!(f, "  take requests: {}", self.take_requests)?;
        writeln!(f, "  yields:        {}", self.yields)?;
        writeln!(f, "  failures:      {}", self.failures)?;
        writeln!(f, "  retries:       {}", self.retries)?;
        write!(f, "  blocked takes: {}", self.blocked)
    }
}

/// What `--handoff-report` prints for the log at `path`.
pub(crate) fn report_message(path: Option<&Path>) -> String {
    let Some(path) = path else {
        return "No handoff log: neither XDG_STATE_HOME nor HOME is set.".to_string();
    };
    match std::fs::read(path) {
        Ok(log) => match report(&log) {
            Some(report) => report.to_string(),
            None => format!("The handoff log {} has no session yet.", path.display()),
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => format!(
            "No handoff log at {} yet: run LibrePods in handoff mode (or --handoff-probe) first.",
            path.display()
        ),
        Err(e) => format!("Cannot read the handoff log {}: {e}", path.display()),
    }
}

/// `--handoff-report` for the default log path.
pub(crate) fn default_report() -> String {
    report_message(default_path().as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bluetooth::aacp::{
        AudioSource, AudioSourceType, ConnectedDevice, EarDetectionStatus,
    };
    use crate::handoff::{ACK_TIMEOUT, Handoff, Input};
    use std::cell::Cell;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    /// A fresh directory under the system temp dir.
    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "librepods-handoff-log-{}-{name}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn line_record(line: &str) -> Record {
        Record::Line {
            ts_ms: 1_762_585_200_123,
            mac: "AA:BB:CC:DD:EE:FF".to_string(),
            kind: classify(line),
            line: line.to_string(),
        }
    }

    #[test]
    fn records_serialise_to_one_line_and_round_trip() {
        let records = [
            Record::SessionStart {
                ts_ms: 1_762_585_200_000,
                version: "0.1.0".to_string(),
                source: Source::App,
            },
            Record::SessionStart {
                ts_ms: 1,
                version: "0.1.0".to_string(),
                source: Source::Probe,
            },
            // A line with a newline in it still stays one JSON line.
            line_record("ACK owner=linux elapsed_ms=12\nsecond"),
        ];
        for record in records {
            let json = serde_json::to_string(&record).unwrap();
            assert!(!json.contains('\n'), "{json}");
            assert_eq!(serde_json::from_str::<Record>(&json).unwrap(), record);
        }
        assert_eq!(
            serde_json::to_string(&line_record("BLOCKED take")).unwrap(),
            r#"{"record":"line","ts_ms":1762585200123,"mac":"AA:BB:CC:DD:EE:FF","kind":"blocked","line":"BLOCKED take"}"#
        );
    }

    #[test]
    fn classify_maps_core_lines() {
        let cases = [
            ("ACK owner=linux elapsed_ms=120", Kind::AckLinux),
            ("ACK owner=not-linux elapsed_ms=80", Kind::AckNotLinux),
            (
                "ACK owner=not-linux late (after TIMEOUT); release confirmed",
                Kind::AckNotLinux,
            ),
            ("REQUEST owner=linux", Kind::TakeRequest),
            ("REQUEST owner=not-linux", Kind::Other),
            (
                "AUTO playback started; requested Linux ownership",
                Kind::TakeRequest,
            ),
            (
                "AUTO reconnected for new playback; requested Linux ownership",
                Kind::TakeRequest,
            ),
            (
                "AUTO new playback requested reconnection after case closure",
                Kind::Other,
            ),
            (
                "YIELD owner=not-linux; another host has the AirPods; Linux gave up its take",
                Kind::Yield,
            ),
            ("REMOTE requested ownership; yielding", Kind::Yield),
            (
                "REMOTE requested ownership; release already pending",
                Kind::Other,
            ),
            (
                "TIMEOUT ownership acknowledgement not received; audible handoff unconfirmed",
                Kind::Timeout,
            ),
            (
                "RECONNECT failed: no route; retrying in 2s",
                Kind::ReconnectFailed,
            ),
            ("RECONNECT cancelled because playback stopped", Kind::Other),
            ("BLOCKED take needs Linux plus a remote host", Kind::Blocked),
            ("AUTO disabled", Kind::Other),
        ];
        for (line, kind) in cases {
            assert_eq!(classify(line), kind, "{line}");
        }
    }

    #[test]
    fn log_path_prefers_absolute_xdg_state_home() {
        let path = |state: Option<&str>, home: Option<&str>| {
            log_path(state.map(OsString::from), home.map(OsString::from))
        };
        assert_eq!(
            path(Some("/state"), Some("/home/mo")),
            Some(PathBuf::from("/state/librepods/handoff.jsonl"))
        );
        let fallback = Some(PathBuf::from(
            "/home/mo/.local/state/librepods/handoff.jsonl",
        ));
        assert_eq!(path(None, Some("/home/mo")), fallback);
        assert_eq!(path(Some(""), Some("/home/mo")), fallback);
        assert_eq!(path(Some("state"), Some("/home/mo")), fallback);
        assert_eq!(path(None, None), None);
        assert_eq!(path(Some(""), Some("")), None);
    }

    #[test]
    fn open_creates_private_file_and_appends_sessions() {
        let dir = temp_dir("open");
        let path = dir.join("librepods/handoff.jsonl");
        let log = Log::open(&path, Source::App).unwrap();
        log.line("AA:BB:CC:DD:EE:FF", "REQUEST owner=linux");
        drop(log);
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&path), 0o600);
        assert_eq!(mode(path.parent().unwrap()), 0o700);
        Log::open(&path, Source::Probe).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        let records: Vec<Record> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(records.len(), 3, "{text}");
        assert!(matches!(
            records[0],
            Record::SessionStart {
                source: Source::App,
                ..
            }
        ));
        assert!(matches!(
            &records[1],
            Record::Line { kind: Kind::TakeRequest, line, .. } if line == "REQUEST owner=linux"
        ));
        assert!(matches!(
            records[2],
            Record::SessionStart {
                source: Source::Probe,
                ..
            }
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }

    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    fn records(path: &Path) -> Vec<Record> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect()
    }

    #[test]
    fn handoff_log_open_tightens_loose_file() {
        let dir = temp_dir("loose-file");
        let path = dir.join("librepods/handoff.jsonl");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let old = r#"{"record":"session_start","ts_ms":1000,"version":"0.1.0","source":"app"}"#;
        std::fs::write(&path, format!("{old}\n")).unwrap();
        std::fs::set_permissions(&path, Permissions::from_mode(0o644)).unwrap();
        Log::open(&path, Source::App).unwrap();
        assert_eq!(mode(&path), 0o600);
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().next(), Some(old), "{text}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn handoff_log_open_tightens_loose_dir() {
        let dir = temp_dir("loose-dir");
        let leaf = dir.join("librepods");
        std::fs::create_dir_all(&leaf).unwrap();
        std::fs::set_permissions(&leaf, Permissions::from_mode(0o755)).unwrap();
        std::fs::set_permissions(&dir, Permissions::from_mode(0o755)).unwrap();
        Log::open(&leaf.join("handoff.jsonl"), Source::App).unwrap();
        assert_eq!(mode(&leaf), 0o700);
        assert_eq!(mode(&dir), 0o755);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn handoff_log_open_rotates_oversized() {
        let dir = temp_dir("oversized");
        let path = dir.join("handoff.jsonl");
        let rotated = dir.join("handoff.jsonl.1");
        let old = vec![b'x'; MAX_LOG_BYTES as usize + 1];
        std::fs::write(&path, &old).unwrap();
        std::fs::write(&rotated, b"older marker\n").unwrap();
        Log::open(&path, Source::App).unwrap();
        assert!(std::fs::read(&rotated).unwrap() == old);
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().count(), 1, "{text}");
        assert!(matches!(
            serde_json::from_str(&text),
            Ok(Record::SessionStart { .. })
        ));
        assert_eq!(mode(&path), 0o600);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn handoff_log_open_keeps_oversized_log_when_rotation_fails() {
        let dir = temp_dir("rotate-fails");
        let path = dir.join("handoff.jsonl");
        let mut old = vec![b'x'; MAX_LOG_BYTES as usize + 1];
        *old.last_mut().unwrap() = b'\n';
        std::fs::write(&path, &old).unwrap();
        // rename() cannot replace a non-empty directory.
        std::fs::create_dir_all(dir.join("handoff.jsonl.1/full")).unwrap();
        Log::open(&path, Source::App).unwrap();
        let text = std::fs::read(&path).unwrap();
        assert!(text.starts_with(&old), "the oversized log was truncated");
        assert!(matches!(
            records(&path).as_slice(),
            [Record::SessionStart { .. }]
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn handoff_log_rotation_lock_file_is_private() {
        let dir = temp_dir("lock-file");
        let path = dir.join("handoff.jsonl");
        let lock = dir.join("handoff.jsonl.lock");
        Log::open(&path, Source::App).unwrap();
        assert!(!lock.exists(), "a normal open created the lock file");
        for preexisting in [false, true] {
            if preexisting {
                std::fs::remove_file(&lock).unwrap();
                std::fs::write(&lock, b"").unwrap();
                std::fs::set_permissions(&lock, Permissions::from_mode(0o644)).unwrap();
            }
            std::fs::write(&path, vec![b'x'; MAX_LOG_BYTES as usize + 1]).unwrap();
            Log::open(&path, Source::App).unwrap();
            assert!(std::fs::metadata(&lock).unwrap().is_file());
            assert_eq!(mode(&lock), 0o600, "preexisting={preexisting}");
            let message = report_message(Some(&path));
            assert!(message.starts_with("Last handoff session:"), "{message}");
            let fresh = report(&std::fs::read(&path).unwrap()).unwrap();
            assert_eq!(message, fresh.to_string());
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn handoff_log_open_keeps_oversized_log_when_lock_fails() {
        let dir = temp_dir("lock-fails");
        let path = dir.join("handoff.jsonl");
        let mut old = vec![b'x'; MAX_LOG_BYTES as usize + 1];
        *old.last_mut().unwrap() = b'\n';
        std::fs::write(&path, &old).unwrap();
        // A directory cannot be opened read+write (EISDIR).
        std::fs::create_dir_all(dir.join("handoff.jsonl.lock/full")).unwrap();
        Log::open(&path, Source::App).unwrap();
        let text = std::fs::read(&path).unwrap();
        let tail = text
            .strip_prefix(old.as_slice())
            .expect("the oversized log was truncated");
        assert!(matches!(
            serde_json::from_slice(tail),
            Ok(Record::SessionStart { .. })
        ));
        assert_eq!(tail.iter().filter(|&&b| b == b'\n').count(), 1);
        assert!(!dir.join("handoff.jsonl.1").exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn handoff_log_rotation_keeps_running_writer() {
        let dir = temp_dir("rotate-running");
        let path = dir.join("handoff.jsonl");
        let mut filler = vec![b'x'; MAX_LOG_BYTES as usize - 1];
        *filler.last_mut().unwrap() = b'\n';
        std::fs::write(&path, filler).unwrap();
        let app = Log::open(&path, Source::App).unwrap();
        let _probe = Log::open(&path, Source::Probe).unwrap();
        app.line("AA:BB:CC:DD:EE:FF", "REQUEST owner=linux");
        drop(app);
        let old = records(&dir.join("handoff.jsonl.1"));
        assert!(
            matches!(
                old.as_slice(),
                [
                    Record::SessionStart {
                        source: Source::App,
                        ..
                    },
                    Record::Line {
                        kind: Kind::TakeRequest,
                        ..
                    },
                ]
            ),
            "{old:?}"
        );
        let new = records(&path);
        assert!(
            matches!(
                new.as_slice(),
                [Record::SessionStart {
                    source: Source::Probe,
                    ..
                }]
            ),
            "{new:?}"
        );
        let report = report(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(report.take_requests, 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    thread_local! {
        /// Runs in Log::open's rotation branch after the size measurement,
        /// just before the `<path>.lock` lock is taken.
        pub(super) static BEFORE_LOCK: Cell<Option<Box<dyn FnOnce()>>> = const { Cell::new(None) };
        /// Runs in Log::open between the inode check and the rename.
        pub(super) static BEFORE_RENAME: Cell<Option<Box<dyn FnOnce()>>> = const { Cell::new(None) };
        /// Runs in Log::open after a successful rename, before the reopen.
        pub(super) static AFTER_RENAME: Cell<Option<Box<dyn FnOnce()>>> = const { Cell::new(None) };
    }

    #[test]
    fn handoff_log_reopen_fails_after_rotation() {
        let dir = temp_dir("reopen-fails");
        let path = dir.join("handoff.jsonl");
        let rotated = dir.join("handoff.jsonl.1");
        let mut old = vec![b'x'; MAX_LOG_BYTES as usize + 1];
        *old.last_mut().unwrap() = b'\n';
        std::fs::write(&path, &old).unwrap();
        // A directory at the path makes the create/append reopen fail (EISDIR).
        AFTER_RENAME.set(Some(Box::new({
            let path = path.clone();
            move || std::fs::create_dir(&path).unwrap()
        })));
        let log = Log::open(&path, Source::App);
        assert!(AFTER_RENAME.take().is_none(), "the hook did not run");
        assert!(log.is_ok());
        let text = std::fs::read(&rotated).unwrap();
        let tail = text
            .strip_prefix(old.as_slice())
            .expect("the old bytes are not the prefix");
        assert_eq!(tail.iter().filter(|&&b| b == b'\n').count(), 1);
        assert!(tail.ends_with(b"\n"));
        assert!(matches!(
            serde_json::from_slice(tail),
            Ok(Record::SessionStart {
                source: Source::App,
                ..
            })
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn handoff_log_rotation_warning_texts() {
        let e = io::Error::from(io::ErrorKind::IsADirectory);
        let (path, rotated) = (
            Path::new("/s/handoff.jsonl"),
            Path::new("/s/handoff.jsonl.1"),
        );
        let reopen = rotation_warning(path, rotated, true, &e);
        assert_eq!(
            reopen,
            format!(
                "Handoff log: rotated /s/handoff.jsonl to /s/handoff.jsonl.1 but cannot open a fresh /s/handoff.jsonl: {e}; appending to /s/handoff.jsonl.1"
            )
        );
        assert!(reopen.contains("appending to /s/handoff.jsonl.1"));
        assert!(!reopen.contains("cannot rotate"));
        assert_eq!(
            rotation_warning(path, rotated, false, &e),
            format!(
                "Handoff log: cannot rotate /s/handoff.jsonl to /s/handoff.jsonl.1: {e}; appending to it"
            )
        );
    }

    #[test]
    fn handoff_log_concurrent_rotation_keeps_rotated_log() {
        let dir = temp_dir("rotate-race");
        let path = dir.join("handoff.jsonl");
        let rotated = dir.join("handoff.jsonl.1");
        let mut filler = vec![b'x'; MAX_LOG_BYTES as usize - 1];
        *filler.last_mut().unwrap() = b'\n';
        std::fs::write(&path, filler).unwrap();
        let app = Log::open(&path, Source::App).unwrap();
        // The review's GDB interleaving: a rival opener measures the same
        // oversized log while the probe sits between its check and rename.
        let (go, rival_go) = mpsc::channel();
        let (measured, rival_measured) = mpsc::channel();
        let (done, rival_done) = mpsc::channel();
        let rival = std::thread::spawn({
            let path = path.clone();
            move || {
                rival_go.recv().unwrap();
                BEFORE_LOCK.set(Some(Box::new(move || measured.send(()).unwrap())));
                let log = Log::open(&path, Source::Probe).unwrap();
                let _ = done.send(());
                // Taken by Log::open only in the rotation branch.
                (log, BEFORE_LOCK.take().is_none())
            }
        });
        BEFORE_RENAME.set(Some(Box::new(move || {
            go.send(()).unwrap();
            // The proof of ordering: the rival measured the old oversized
            // inode and is about to lock. A timeout fails (a regression
            // would otherwise deadlock on the held lock), never passes.
            rival_measured
                .recv_timeout(Duration::from_secs(10))
                .expect("the rival never reached the lock in the rotation branch");
            // Sanity only: with the lock held, the rival cannot finish.
            assert!(
                rival_done.recv_timeout(Duration::from_millis(50)).is_err(),
                "the rival rotated while the probe held the rotation lock"
            );
        })));
        let _probe = Log::open(&path, Source::Probe).unwrap();
        // Drop the hook if unused, so a waiting rival errors instead of hanging.
        assert!(BEFORE_RENAME.take().is_none(), "the probe did not rotate");
        let (_rival, rival_measured_old) = rival.join().unwrap();
        assert!(rival_measured_old, "the rival did not reach the lock");
        app.line("AA:BB:CC:DD:EE:FF", "REQUEST owner=linux");
        drop(app);
        let old = records(&rotated);
        assert!(
            matches!(
                old.as_slice(),
                [
                    Record::SessionStart {
                        source: Source::App,
                        ..
                    },
                    Record::Line {
                        kind: Kind::TakeRequest,
                        ..
                    },
                ]
            ),
            "{old:?}"
        );
        let new = records(&path);
        assert!(
            matches!(
                new.as_slice(),
                [
                    Record::SessionStart {
                        source: Source::Probe,
                        ..
                    },
                    Record::SessionStart {
                        source: Source::Probe,
                        ..
                    },
                ]
            ),
            "{new:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn handoff_log_reopen_after_torn_tail() {
        let dir = temp_dir("torn");
        let path = dir.join("handoff.jsonl");
        let old = concat!(
            r#"{"record":"session_start","ts_ms":1000,"version":"0.1.0","source":"app"}"#,
            "\n",
            r#"{"record":"line","ts_ms":2000,"mac":"AA:BB:CC:DD:EE:FF","kind":"ack_linux","line":"ACK owner=linux elapsed_ms=1"}"#,
            "\n",
            r#"{"record":"line","ts_ms":2500,"mac":"AA:BB:CC:DD:EE:FF","kind":"ack_linux","line":"ACK owner=linux elapsed_ms=2"}"#,
            "\n",
            r#"{"record":"line","ts_ms":3000,"mac":"AA:BB:CC:DD:EE:FF","kind":"ack_li"#,
        );
        std::fs::write(&path, old).unwrap();
        let log = Log::open(&path, Source::App).unwrap();
        log.line("AA:BB:CC:DD:EE:FF", "ACK owner=linux elapsed_ms=5");
        drop(log);
        let text = std::fs::read_to_string(&path).unwrap();
        let new_start = text.lines().rev().nth(1).unwrap();
        let Ok(Record::SessionStart { ts_ms, .. }) = serde_json::from_str(new_start) else {
            panic!("the new session_start is not a line of its own:\n{text}");
        };
        let report = report(text.as_bytes()).unwrap();
        assert_eq!(report.started_ms, ts_ms, "{text}");
        assert_eq!(report.takeovers, 1, "{text}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn handoff_log_reopen_after_torn_utf8_tail() {
        let dir = temp_dir("torn-utf8");
        let path = dir.join("handoff.jsonl");
        let mut old = concat!(
            r#"{"record":"session_start","ts_ms":1000,"version":"0.1.0","source":"app"}"#,
            "\n",
            r#"{"record":"line","ts_ms":2000,"mac":"AA:BB:CC:DD:EE:FF","kind":"ack_linux","line":"ACK owner=linux elapsed_ms=1"}"#,
            "\n",
            r#"{"record":"line","ts_ms":2500,"mac":"AA:BB:CC:DD:EE:FF","kind":"ack_linux","line":"ACK owner=linux elapsed_ms=2"}"#,
            "\n",
            r#"{"record":"line","ts_ms":3000,"mac":"AA:BB:CC:DD:EE:FF","kind":"reconnect_failed","line":"RECONNECT failed: caf"#,
        )
        .as_bytes()
        .to_vec();
        // The first byte of "é": the write was cut inside the character.
        old.push(0xC3);
        std::fs::write(&path, old).unwrap();
        let log = Log::open(&path, Source::App).unwrap();
        log.line("AA:BB:CC:DD:EE:FF", "ACK owner=linux elapsed_ms=5");
        drop(log);
        let message = report_message(Some(&path));
        assert!(!message.starts_with("Cannot read"), "{message}");
        assert!(message.starts_with("Last handoff session"), "{message}");
        assert!(message.contains("takeovers:     1\n"), "{message}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn open_under_a_regular_file_fails_without_panic() {
        let dir = temp_dir("unwritable");
        let file = dir.join("not-a-dir");
        std::fs::write(&file, b"").unwrap();
        assert!(Log::open(&file.join("librepods/handoff.jsonl"), Source::App).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    const MAC: &str = "AA:BB:CC:DD:EE:FF";

    /// An opener that blocks until the sender is used or dropped.
    fn hung_opener() -> (
        mpsc::Sender<()>,
        impl FnOnce() -> io::Result<io::Sink> + Send,
    ) {
        let (release, released) = mpsc::channel();
        (release, move || {
            let _ = released.recv();
            Ok(io::sink())
        })
    }

    #[test]
    fn handoff_log_line_never_blocks_on_a_hung_writer() {
        let (release, opener) = hung_opener();
        let log = Arc::new(Log::spawn(Path::new("/hung/handoff.jsonl"), opener).unwrap());
        let sink = log.clone().sink(MAC.to_string());
        let t0 = Instant::now();
        for _ in 0..QUEUE_BOUND + 50 {
            sink("REQUEST owner=linux");
        }
        assert!(t0.elapsed() < Duration::from_secs(1), "{:?}", t0.elapsed());
        assert_eq!(log.dropped.load(Ordering::Relaxed), 50);
        assert_eq!(log.overflow_warnings.load(Ordering::Relaxed), 1);
        release.send(()).unwrap();
        drop((sink, log));
    }

    #[test]
    fn handoff_log_start_does_not_wait_for_open() {
        let dir = temp_dir("slow-open");
        let path = dir.join("librepods/handoff.jsonl");
        let (release, released) = mpsc::channel::<()>();
        // What Log::start does, with an open that waits for the release.
        let started_ms = now_ms();
        let t0 = Instant::now();
        let log = Log::spawn(&path, {
            let path = path.clone();
            move || {
                let _ = released.recv();
                open_file(&path, Source::App, started_ms)
            }
        })
        .unwrap();
        assert!(t0.elapsed() < Duration::from_secs(1), "{:?}", t0.elapsed());
        let lines = [
            "REQUEST owner=linux",
            "ACK owner=linux elapsed_ms=1",
            "REMOTE requested ownership; yielding",
        ];
        for line in lines {
            log.line(MAC, line);
        }
        assert!(!path.exists(), "the open did not wait for the release");
        release.send(()).unwrap();
        drop(log);
        let written = records(&path);
        let [Record::SessionStart { ts_ms: start, .. }, logged @ ..] = written.as_slice() else {
            panic!("{written:?}");
        };
        let mut last = *start;
        for (record, expected) in logged.iter().zip(lines) {
            let Record::Line { ts_ms, line, .. } = record else {
                panic!("{written:?}");
            };
            assert_eq!(line, expected);
            assert!(*ts_ms >= last, "{written:?}");
            last = *ts_ms;
        }
        assert_eq!(logged.len(), lines.len(), "{written:?}");
        // The real Log::start appends after the session_start it stamps.
        let probe = Log::start(&path, Source::Probe).unwrap();
        probe.line(MAC, "BLOCKED take");
        drop(probe);
        assert!(matches!(
            &records(&path)[4..],
            [
                Record::SessionStart {
                    source: Source::Probe,
                    ..
                },
                Record::Line {
                    kind: Kind::Blocked,
                    ..
                },
            ]
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn handoff_log_drop_is_bounded_on_a_hung_writer() {
        let (release, opener) = hung_opener();
        let log = Log::spawn(Path::new("/hung/handoff.jsonl"), opener).unwrap();
        log.line(MAC, "REQUEST owner=linux");
        let t0 = Instant::now();
        drop(log);
        let waited = t0.elapsed();
        assert!(
            waited < DRAIN_TIMEOUT + Duration::from_secs(1),
            "{waited:?}"
        );
        drop(release);
    }

    #[test]
    fn handoff_log_stops_silently_after_writer_failure() {
        // An open failure, and a write failure (/dev/null opened read-only).
        let failed_open = Log::spawn(Path::new("/x"), || {
            Err::<File, _>(io::Error::other("no state dir"))
        });
        let failed_write = Log::spawn(Path::new("/x"), || File::open("/dev/null"));
        for log in [failed_open.unwrap(), failed_write.unwrap()] {
            log.line(MAC, "REQUEST owner=linux");
            // Disconnected once the writer thread has ended.
            assert_eq!(
                log.done
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5)),
                Err(mpsc::RecvTimeoutError::Disconnected)
            );
            for _ in 0..QUEUE_BOUND + 1 {
                log.line(MAC, "REQUEST owner=linux");
            }
            assert_eq!(log.dropped.load(Ordering::Relaxed), 0);
            assert_eq!(log.overflow_warnings.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn report_counts_only_the_last_session_of_the_fixture() {
        let fixture = include_str!("../tests/fixtures/handoff_session.jsonl");
        assert_eq!(
            report(fixture.as_bytes()),
            Some(Report {
                started_ms: 1_762_590_000_000,
                duration_ms: 95_500,
                takeovers: 2,
                take_requests: 4,
                yields: 2,
                failures: 2,
                retries: 1,
                blocked: 1,
            })
        );
    }

    fn device(mac: &str) -> ConnectedDevice {
        ConnectedDevice {
            mac: mac.to_string(),
            info1: 0,
            info2: 0,
            r#type: None,
        }
    }

    #[test]
    fn report_counts_lines_of_the_real_core() {
        let local = "01:02:03:04:05:06";
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let mut core = Handoff::new(local.to_string(), t0, true, true);
        let mut log = serde_json::to_string(&Record::SessionStart {
            ts_ms: 0,
            version: "0.1.0".to_string(),
            source: Source::Probe,
        })
        .unwrap();
        let mut feed = |core: &mut Handoff, ms: u64, input: Input| {
            core.handle(at(ms), input);
            for line in core.take_lines() {
                let record = Record::Line {
                    ts_ms: ms,
                    mac: "AA:BB:CC:DD:EE:FF".to_string(),
                    kind: classify(&line),
                    line,
                };
                log.push('\n');
                log.push_str(&serde_json::to_string(&record).unwrap());
            }
        };
        // Linux plus a remote host, worn, idle, not owning.
        let hosts = vec![device(local), device("07:08:09:0A:0B:0C")];
        feed(
            &mut core,
            0,
            Input::ConnectedDevices {
                old: Vec::new(),
                new: hosts,
            },
        );
        feed(
            &mut core,
            0,
            Input::EarState(vec![EarDetectionStatus::InEar; 2]),
        );
        feed(
            &mut core,
            0,
            Input::AudioSource(AudioSource {
                mac: "00:00:00:00:00:00".to_string(),
                r#type: AudioSourceType::None,
            }),
        );
        feed(&mut core, 0, Input::Ownership(vec![0]));
        // Take and ACK.
        feed(&mut core, 100, Input::Take);
        feed(&mut core, 200, Input::Ownership(vec![1]));
        // A remote host asks: Linux yields, and the release is acknowledged.
        feed(&mut core, 300, Input::RemoteOwnershipRequest);
        feed(&mut core, 400, Input::Ownership(vec![0]));
        // A take whose ACK never comes.
        feed(&mut core, 500, Input::Take);
        let timeout = 500 + ACK_TIMEOUT.as_millis() as u64 + 1;
        feed(&mut core, timeout, Input::Tick);
        // The session drops and the reconnect fails.
        feed(&mut core, timeout + 100, Input::Disconnected);
        feed(
            &mut core,
            timeout + 200,
            Input::ReconnectFailed("no route".into()),
        );

        assert_eq!(
            report(log.as_bytes()),
            Some(Report {
                started_ms: 0,
                duration_ms: timeout + 200,
                takeovers: 1,
                take_requests: 2,
                yields: 1,
                failures: 2,
                retries: 1,
                blocked: 0,
            }),
            "{log}"
        );
    }

    #[test]
    fn report_message_without_a_session() {
        let dir = temp_dir("report");
        let path = dir.join("handoff.jsonl");
        assert!(report_message(Some(&path)).starts_with("No handoff log at "));
        std::fs::write(&path, b"").unwrap();
        assert!(report_message(Some(&path)).contains("has no session yet"));
        assert!(report_message(None).starts_with("No handoff log"));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
