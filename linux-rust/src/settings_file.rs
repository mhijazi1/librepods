use crate::devices::enums::DeviceData;
use std::collections::HashMap;
use std::sync::atomic::Ordering;

/// Parses devices.json given the result of reading it. Only a missing file
/// counts as empty; any other read or parse error is returned, so a caller
/// never replaces a file it could not read.
pub(crate) fn parse_devices(
    existing: std::io::Result<String>,
) -> std::io::Result<HashMap<String, DeviceData>> {
    match existing {
        Ok(json) => Ok(serde_json::from_str(&json)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(e) => Err(e),
    }
}

/// The only read-modify-write of devices.json: reads `path`, hands the read
/// result unchanged to `rewrite` (so a missing file arrives as NotFound for
/// `parse_devices`) and writes what it returns. If `rewrite` fails, nothing is
/// written and its error is returned. A process-wide lock is held throughout,
/// so concurrent callers can't drop each other's changes. Synchronous on
/// purpose: the guard can never be held across an `.await`. `rewrite` must not
/// call this again (the std Mutex would deadlock). In-process only: one
/// librepods user service writes this file.
pub(crate) fn rewrite_devices_file(
    path: &std::path::Path,
    rewrite: impl FnOnce(std::io::Result<String>) -> std::io::Result<String>,
) -> std::io::Result<()> {
    static DEVICES_FILE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = DEVICES_FILE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let json = rewrite(std::fs::read_to_string(path))?;
    write_settings_file(path, &json)
}

/// Replaces the settings file at `path` with `contents` without ever leaving it
/// half-written: a temp file next to the target is written, fsynced and renamed
/// over it. An existing target is resolved first, so a symlink stays a symlink,
/// and its permission bits are kept; a new file is created 0600. A missing
/// parent directory is created. On error the temp file is removed and the
/// target is untouched.
pub(crate) fn write_settings_file(path: &std::path::Path, contents: &str) -> std::io::Result<()> {
    write_settings_file_with(path, contents, |p| std::fs::metadata(p), |_| Ok(()))
}

/// `write_settings_file` with the target's `stat` and a `before_rename` step
/// run on the fsynced temp file injectable, so tests can fail the write there.
fn write_settings_file_with(
    path: &std::path::Path,
    contents: &str,
    stat: impl FnOnce(&std::path::Path) -> std::io::Result<std::fs::Metadata>,
    before_rename: impl FnOnce(&std::path::Path) -> std::io::Result<()>,
) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    static NEXT_TMP: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    let target = match std::fs::canonicalize(path) {
        Ok(target) => target,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Follow a dangling symlink (chain) to the file it names, so the
            // link is kept and its referent is created, as fs::write would.
            let mut target = path.to_path_buf();
            let mut hops = 0;
            while std::fs::symlink_metadata(&target).is_ok_and(|m| m.file_type().is_symlink()) {
                hops += 1;
                if hops > 40 {
                    return Err(std::io::Error::other("too many levels of symbolic links"));
                }
                // A relative link resolves against the link's dir; an absolute one replaces it.
                target = target.with_file_name(std::fs::read_link(&target)?);
            }
            if let Some(dir) = target.parent() {
                std::fs::create_dir_all(dir)?;
            }
            target
        }
        Err(e) => return Err(e),
    };
    let tmp = target.with_file_name(format!(
        ".{}.{}.{}.tmp",
        target.file_name().unwrap_or_default().to_string_lossy(),
        std::process::id(),
        NEXT_TMP.fetch_add(1, Ordering::Relaxed)
    ));
    // Private until the target's mode is copied: devices.json holds keys.
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)?;
    let result = (|| {
        file.write_all(contents.as_bytes())?;
        // After the write: a write by a non-root user clears setuid/setgid.
        // Only a missing target (a new file) keeps 0600; any other stat error
        // aborts, or the rename would silently drop the target's mode.
        match stat(&target) {
            Ok(meta) => file.set_permissions(meta.permissions())?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        file.sync_all()?;
        before_rename(&tmp)?;
        std::fs::rename(&tmp, &target)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::devices::enums::DeviceType;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    /// A fresh directory under the system temp dir.
    fn settings_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "librepods-settings-file-{}-{name}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn dir_names(dir: &std::path::Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn parsing_devices_missing_file_is_empty() {
        let devices = parse_devices(Err(std::io::ErrorKind::NotFound.into())).unwrap();
        assert!(devices.is_empty());
    }

    #[test]
    fn parsing_devices_round_trips() {
        let mut devices = HashMap::new();
        devices.insert(
            "AA:AA:AA:AA:AA:AA".to_string(),
            DeviceData {
                name: "AirPods Pro".to_string(),
                type_: DeviceType::AirPods,
                information: None,
            },
        );
        let json = serde_json::to_string(&devices).unwrap();
        let parsed = parse_devices(Ok(json.clone())).unwrap();
        assert_eq!(serde_json::to_string(&parsed).unwrap(), json);
    }

    #[test]
    fn parsing_devices_rejects_read_errors() {
        for kind in [
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::InvalidData,
        ] {
            let err = parse_devices(Err(kind.into())).unwrap_err();
            assert_eq!(err.kind(), kind);
        }
    }

    #[test]
    fn parsing_devices_rejects_bad_json() {
        for json in ["", "{", r#"{"AA:AA:AA:AA:AA:AA": 1}"#] {
            assert!(parse_devices(Ok(json.to_string())).is_err(), "{json:?}");
        }
    }

    #[test]
    fn rewrite_devices_file_creates_missing_file() {
        let dir = settings_dir("rewrite-missing");
        let target = dir.join("devices.json");

        rewrite_devices_file(&target, |existing| {
            assert_eq!(existing.unwrap_err().kind(), std::io::ErrorKind::NotFound);
            Ok("{}".to_string())
        })
        .unwrap();

        assert_eq!(std::fs::read_to_string(&target).unwrap(), "{}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rewrite_devices_file_error_leaves_file_unchanged() {
        let dir = settings_dir("rewrite-error");
        let target = dir.join("devices.json");
        std::fs::write(&target, "{").unwrap();

        rewrite_devices_file(&target, |existing| {
            parse_devices(existing).map(|_| panic!("parsed a truncated file"))
        })
        .unwrap_err();

        assert_eq!(std::fs::read(&target).unwrap(), b"{");
        assert_eq!(dir_names(&dir), ["devices.json"]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn concurrent_rewrites_keep_every_entry() {
        const N: usize = 8;
        let dir = settings_dir("rewrite-concurrent");
        let target = dir.join("devices.json");
        let barrier = std::sync::Barrier::new(N);

        std::thread::scope(|s| {
            for i in 0..N {
                let (target, barrier) = (&target, &barrier);
                s.spawn(move || {
                    barrier.wait();
                    rewrite_devices_file(target, |existing| {
                        let mut devices = parse_devices(existing)?;
                        devices.insert(
                            format!("AA:AA:AA:AA:AA:{i:02X}"),
                            DeviceData {
                                name: format!("Device {i}"),
                                type_: DeviceType::AirPods,
                                information: None,
                            },
                        );
                        // Widen the read-to-write window so a missing lock loses entries.
                        std::thread::sleep(std::time::Duration::from_millis(5));
                        Ok(serde_json::to_string(&devices)?)
                    })
                    .unwrap();
                });
            }
        });

        let devices = parse_devices(std::fs::read_to_string(&target)).unwrap();
        assert_eq!(devices.len(), N);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn settings_write_failure_keeps_original() {
        let dir = settings_dir("failure");
        let target = dir.join("settings.json");
        std::fs::write(&target, "original").unwrap();

        let err = write_settings_file_with(
            &target,
            "new",
            |p| std::fs::metadata(p),
            |tmp| {
                // The hook runs after the temp file is fully written.
                assert_eq!(std::fs::read_to_string(tmp).unwrap(), "new");
                Err(std::io::Error::other("injected"))
            },
        )
        .unwrap_err();

        assert_eq!(err.to_string(), "injected");
        assert_eq!(std::fs::read(&target).unwrap(), b"original");
        assert_eq!(dir_names(&dir), ["settings.json"]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn settings_write_stat_failure_keeps_original() {
        let dir = settings_dir("stat");
        let target = dir.join("settings.json");
        std::fs::write(&target, "original").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o640)).unwrap();

        let err = write_settings_file_with(
            &target,
            "new",
            |_| Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
            |_| panic!("renamed after a failed stat"),
        )
        .unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(std::fs::read(&target).unwrap(), b"original");
        let mode = std::fs::metadata(&target).unwrap().permissions().mode();
        assert_eq!(mode & 0o7777, 0o640);
        assert_eq!(dir_names(&dir), ["settings.json"]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn settings_write_replaces_content_keeps_mode() {
        let dir = settings_dir("mode");
        let target = dir.join("settings.json");
        std::fs::write(&target, "original, longer than the new content").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o4700)).unwrap();

        write_settings_file(&target, "new").unwrap();

        assert_eq!(std::fs::read_to_string(&target).unwrap(), "new");
        let mode = std::fs::metadata(&target).unwrap().permissions().mode();
        assert_eq!(mode & 0o7777, 0o4700);
        assert_eq!(dir_names(&dir), ["settings.json"]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn settings_write_keeps_symlink() {
        let dir = settings_dir("symlink");
        let real = dir.join("real.json");
        let link = dir.join("settings.json");
        std::fs::write(&real, "original").unwrap();
        std::os::unix::fs::symlink(&real, &link).unwrap();

        write_settings_file(&link, "new").unwrap();

        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(std::fs::read_to_string(&real).unwrap(), "new");
        assert_eq!(dir_names(&dir), ["real.json", "settings.json"]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn settings_write_keeps_dangling_symlink_chain() {
        let dir = settings_dir("dangling");
        let link = dir.join("settings.json");
        // settings.json -> mid.json (relative) -> <dir>/sub/real.json (absolute), nothing exists.
        std::os::unix::fs::symlink("mid.json", &link).unwrap();
        std::os::unix::fs::symlink(dir.join("sub/real.json"), dir.join("mid.json")).unwrap();

        write_settings_file(&link, "new").unwrap();

        for name in ["settings.json", "mid.json"] {
            let meta = std::fs::symlink_metadata(dir.join(name)).unwrap();
            assert!(
                meta.file_type().is_symlink(),
                "{name} is no longer a symlink"
            );
        }
        assert_eq!(
            std::fs::read_to_string(dir.join("sub/real.json")).unwrap(),
            "new"
        );
        assert_eq!(dir_names(&dir.join("sub")), ["real.json"]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn settings_write_creates_missing_parent() {
        let dir = settings_dir("parent");
        let target = dir.join("librepods").join("preferences.json");

        write_settings_file(&target, "{}").unwrap();

        assert_eq!(std::fs::read_to_string(&target).unwrap(), "{}");
        assert_eq!(dir_names(target.parent().unwrap()), ["preferences.json"]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn settings_write_new_file_is_private() {
        let dir = settings_dir("private");
        let target = dir.join("devices.json");

        // The temp file is already private while it holds the contents.
        write_settings_file_with(
            &target,
            "{}",
            |p| std::fs::metadata(p),
            |tmp| {
                let mode = std::fs::metadata(tmp)?.permissions().mode();
                assert_eq!(mode & 0o777, 0o600);
                Ok(())
            },
        )
        .unwrap();

        let mode = std::fs::metadata(&target).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
