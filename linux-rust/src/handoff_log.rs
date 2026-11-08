//! Handoff diagnostics (#21): a JSONL log of the handoff core's lines for
//! hardware test sessions, and the `--handoff-report` summary of its last
//! session. The records hold only timestamps, the AirPods MAC and the core's
//! lines, never key material. Logging never fails or stops switching: a
//! failed open installs no sink, a failed write stops the log, each with one
//! warning.

use crate::switching::LineSink;
use log::warn;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::fmt;
use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

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

/// Past this size a log is emptied when it is next opened (about 5000 lines).
const MAX_LOG_BYTES: u64 = 1 << 20;

fn write_record(file: &mut File, record: &Record) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(record)?;
    bytes.push(b'\n');
    // One write per record, so an O_APPEND writer never interleaves lines.
    file.write_all(&bytes)
}

/// An open handoff log. A write error stops it after one warning.
pub(crate) struct Log {
    path: PathBuf,
    file: Mutex<Option<File>>,
}

impl Log {
    /// Open `path` for appending (created 0600, its directory 0700) and
    /// write the session_start record. A log past `MAX_LOG_BYTES` is emptied
    /// first: the report only reads the last session.
    pub(crate) fn open(path: &Path, source: Source) -> io::Result<Log> {
        if let Some(dir) = path.parent() {
            DirBuilder::new().recursive(true).mode(0o700).create(dir)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(path)?;
        if file.metadata()?.len() > MAX_LOG_BYTES {
            file.set_len(0)?;
        }
        write_record(
            &mut file,
            &Record::SessionStart {
                ts_ms: now_ms(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                source,
            },
        )?;
        Ok(Log {
            path: path.to_owned(),
            file: Mutex::new(Some(file)),
        })
    }

    /// Append a `line` record for the AirPods `mac`.
    pub(crate) fn line(&self, mac: &str, line: &str) {
        let record = Record::Line {
            ts_ms: now_ms(),
            mac: mac.to_string(),
            kind: classify(line),
            line: line.to_string(),
        };
        // shortcut: the append runs synchronously on the handoff task, so a hung state filesystem (e.g. a stale NFS home) would stall switching; move it to a writer thread if that is ever seen (#105)
        let mut file = self.file.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(open) = file.as_mut()
            && let Err(e) = write_record(open, &record)
        {
            warn!(
                "Handoff log: cannot write {}: {e}; handoff logging stopped",
                self.path.display()
            );
            *file = None;
        }
    }

    /// The switching `LineSink` that appends the core's lines for `mac`.
    pub(crate) fn sink(self: Arc<Self>, mac: String) -> LineSink {
        Arc::new(move |line: &str| self.line(&mac, line))
    }
}

/// Open the log at the default path, or warn once and return None.
pub(crate) fn open_default(source: Source) -> Option<Log> {
    let Some(path) = default_path() else {
        warn!("Handoff log: neither XDG_STATE_HOME nor HOME is set; not logging");
        return None;
    };
    Log::open(&path, source)
        .inspect_err(|e| {
            warn!(
                "Handoff log: cannot open {}: {e}; not logging",
                path.display()
            )
        })
        .ok()
}

/// The app's log, opened once per process, so one app run is one session.
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
/// the log has none. Lines that are not records (a torn last write) are
/// skipped.
pub(crate) fn report(log: &str) -> Option<Report> {
    let mut report = None;
    for record in log.lines().filter_map(|l| serde_json::from_str(l).ok()) {
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
    match std::fs::read_to_string(path) {
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
    use std::os::unix::fs::PermissionsExt;
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

    #[test]
    fn open_empties_an_oversized_log() {
        let dir = temp_dir("oversized");
        let path = dir.join("handoff.jsonl");
        std::fs::write(&path, vec![b'x'; MAX_LOG_BYTES as usize + 1]).unwrap();
        Log::open(&path, Source::App).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().count(), 1, "{text}");
        assert!(report(&text).is_some());
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

    #[test]
    fn report_counts_only_the_last_session_of_the_fixture() {
        let fixture = include_str!("../tests/fixtures/handoff_session.jsonl");
        assert_eq!(
            report(fixture),
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
            report(&log),
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
