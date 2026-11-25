//! What `daemon.status` says about the running build, so a client can tell a
//! daemon it may restart (autostarted by the CLI, now running old code, nobody
//! else connected) from one it must leave alone (an app's in-process daemon,
//! a script's daemon, a busy one).

use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        OnceLock,
    },
    time::SystemTime,
};

/// Clients connected to this process's daemon socket(s).
static CLIENTS: AtomicUsize = AtomicUsize::new(0);

/// The file holding the daemon code and its mtime when the daemon started.
static IMAGE: OnceLock<Option<(PathBuf, Option<SystemTime>)>> = OnceLock::new();

/// Set by the `idfon` CLI when it spawns idfond (`IDFON_AUTOSTARTED=1`).
static AUTOSTARTED: OnceLock<bool> = OnceLock::new();

/// Records the image and how the daemon was started; first call wins.
pub fn record_start() {
    IMAGE.get_or_init(|| {
        let path = image_path()?;
        let modified = modified(&path);
        Some((path, modified))
    });
    AUTOSTARTED.get_or_init(|| std::env::var("IDFON_AUTOSTARTED").is_ok_and(|value| value == "1"));
}

/// Counts a connected client until the guard drops.
pub struct ClientGuard(());

impl ClientGuard {
    pub fn new() -> Self {
        CLIENTS.fetch_add(1, Ordering::Relaxed);
        Self(())
    }
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        CLIENTS.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The `daemon.status` fields: `host` (the executable's file name: `idfond`,
/// or the app hosting the daemon in-process), `autostarted`, `clients`, and
/// `build.image` / `build.stale` (the image changed on disk since start; a
/// missing or unreadable image is not called stale).
pub fn status_fields() -> serde_json::Value {
    let host = std::env::current_exe()
        .ok()
        .and_then(|path| path.file_name().map(|name| name.to_string_lossy().into_owned()))
        .unwrap_or_default();
    let (image, stale) = match IMAGE.get().cloned().flatten() {
        Some((path, started)) => {
            let now = modified(&path);
            let stale = matches!((started, now), (Some(started), Some(now)) if started != now);
            (path.display().to_string(), stale)
        }
        None => (String::new(), false),
    };
    serde_json::json!({
        "host": host,
        "autostarted": AUTOSTARTED.get().copied().unwrap_or(false),
        "clients": CLIENTS.load(Ordering::Relaxed),
        "build": {"image": image, "stale": stale},
    })
}

fn modified(path: &PathBuf) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|meta| meta.modified()).ok()
}

/// The shared object (or executable) this code was loaded from: the dylib
/// for idfond, which is only a launcher.
#[cfg(unix)]
fn image_path() -> Option<PathBuf> {
    use std::ffi::CStr;
    let mut info: libc::Dl_info = unsafe { std::mem::zeroed() };
    let address = record_start as *const libc::c_void;
    // SAFETY: dladdr only reads the address and fills `info`; dli_fname, when
    // set, points at a NUL-terminated path owned by the dynamic loader.
    let found = unsafe { libc::dladdr(address, &mut info) };
    if found == 0 || info.dli_fname.is_null() {
        return std::env::current_exe().ok();
    }
    let path = unsafe { CStr::from_ptr(info.dli_fname) };
    Some(PathBuf::from(path.to_string_lossy().into_owned()))
}

#[cfg(not(unix))]
fn image_path() -> Option<PathBuf> {
    std::env::current_exe().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_reports_an_unchanged_image_as_current() {
        record_start();
        let fields = status_fields();
        assert!(!fields["build"]["image"].as_str().unwrap().is_empty(), "{fields}");
        assert_eq!(fields["build"]["stale"], false);
        assert!(fields["clients"].is_u64());
    }

    #[test]
    fn client_guard_counts_connections() {
        let before = CLIENTS.load(Ordering::Relaxed);
        let guard = ClientGuard::new();
        assert!(CLIENTS.load(Ordering::Relaxed) > before);
        drop(guard);
    }
}
