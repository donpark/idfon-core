use std::path::PathBuf;

use once_cell::sync::Lazy;
use safer_ffi::{prelude::*, vec};

pub(crate) static TOKIO_EXECUTOR: Lazy<tokio::runtime::Runtime> =
    Lazy::new(|| tokio::runtime::Runtime::new().unwrap());

pub fn tokio_executor<F: std::future::Future>(future: F) -> F::Output {
    TOKIO_EXECUTOR.block_on(future)
}

/// Serializes tests that drive the process-global media FFI state (the audio
/// queue, the live session, the shared executor). A real-time pipeline there
/// starves under a concurrent heavy test and reads as a pipeline bug.
#[cfg(test)]
pub(crate) static MEDIA_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Frees a Rust-allocated string.
#[ffi_export]
pub fn rust_free_string(string: char_p::Box) {
    drop(string)
}

/// Allocates a buffer managed by rust, given the initial size.
#[ffi_export]
pub fn rust_buffer_alloc(size: usize) -> vec::Vec<u8> {
    vec![0u8; size].into()
}

/// Returns the length of the buffer.
#[ffi_export]
pub fn rust_buffer_len(buf: &vec::Vec<u8>) -> usize {
    buf.len()
}

/// Frees the rust buffer.
#[ffi_export]
pub fn rust_buffer_free(buf: vec::Vec<u8>) {
    drop(buf);
}

/// Installs the tracing subscriber used for iroh/moq diagnostics.
///
/// Filter: `IDFON_LOG` / `RUST_LOG` / `IROH_C_LOG` (default info). Delegates
/// to the shared `idfon-telemetry` subscriber; a no-op when one is already set.
pub fn init_tracing(path: PathBuf) {
    eprintln!("[idfond] tracing init -> {:?}", path);
    idfon_telemetry::init_file("iroh-c-ffi", &path, "info");
}

/// Enables tracing for iroh (FFI entry, used by the apps).
#[ffi_export]
pub fn iroh_enable_tracing() {
    let dir = tracing_dir();
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
        prune_runs(&dir, std::process::id(), KEPT_RUNS);
    }
    init_tracing(dir.join(format!("idfon-{}.log", std::process::id())));
}

/// Earlier runs whose logs survive a launch (the iOS app keeps the same count
/// in `IdfonLogFile`).
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
const KEPT_RUNS: usize = 5;

/// Where the apps' tracing log goes. iOS: the sandbox tmp (there is no literal
/// /tmp there, and the app prunes it with its own log). macOS: the user's
/// `~/Library/Logs/Idfon`, which the mac app's audio log shares.
fn tracing_dir() -> PathBuf {
    #[cfg(target_os = "macos")]
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home).join("Library/Logs/Idfon");
    }
    std::env::temp_dir()
}

/// Keep the newest `keep` earlier runs in `dir`; `current` is this process.
/// A run is one pid's `idfon-<pid>.log` and `idfon-<kind>-<pid>.log` files,
/// aged by the newest of them.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn prune_runs(dir: &std::path::Path, current: u32, keep: usize) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    let mut runs: std::collections::HashMap<u32, (Vec<PathBuf>, std::time::SystemTime)> =
        Default::default();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(stem) = name.strip_prefix("idfon-").and_then(|s| s.strip_suffix(".log")) else {
            continue;
        };
        let Ok(pid) = stem.rsplit('-').next().unwrap_or(stem).parse::<u32>() else { continue };
        if pid == current {
            continue;
        }
        let modified = entry
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let run = runs.entry(pid).or_insert((Vec::new(), std::time::SystemTime::UNIX_EPOCH));
        run.0.push(entry.path());
        run.1 = run.1.max(modified);
    }
    let mut runs: Vec<_> = runs.into_values().collect();
    runs.sort_by(|a, b| b.1.cmp(&a.1));
    for (files, _) in runs.into_iter().skip(keep) {
        for file in files {
            let _ = std::fs::remove_file(file);
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn prune_keeps_the_newest_runs_and_this_one() {
        let dir = std::env::temp_dir().join(format!("idfon-prune-runs-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let base = std::time::SystemTime::now() - std::time::Duration::from_secs(1000);
        for pid in 1..=8u32 {
            for name in [format!("idfon-{pid}.log"), format!("idfon-audio-{pid}.log")] {
                let path = dir.join(name);
                let file = std::fs::File::create(&path).unwrap();
                file.set_modified(base + std::time::Duration::from_secs(pid as u64)).unwrap();
            }
        }
        std::fs::write(dir.join("notes.log"), b"x").unwrap();
        // pid 1 is the oldest, but it is the running process.
        super::prune_runs(&dir, 1, 5);
        let mut left: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        assert_eq!(
            left,
            [
                "idfon-1.log", "idfon-4.log", "idfon-5.log", "idfon-6.log", "idfon-7.log",
                "idfon-8.log", "idfon-audio-1.log", "idfon-audio-4.log", "idfon-audio-5.log",
                "idfon-audio-6.log", "idfon-audio-7.log", "idfon-audio-8.log", "notes.log",
            ]
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
