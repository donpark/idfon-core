//! Where idfon keeps things.
//!
//! | what | default | with profile `p` |
//! | --- | --- | --- |
//! | home (config, models, records) | `$IDFON_HOME`, else `~/.idfon` | same |
//! | daemon data (identities, peers) | macOS: `~/Library/Application Support/Idfon/daemon`; else `<home>/daemon` | `…/Idfon/profiles/p`; `<home>/profiles/p` |
//! | daemon socket | `/tmp/idfon/idfond.sock` | `/tmp/idfon-p/idfond.sock` |
//! | config file | `<home>/config.toml` | same |
//!
//! With `IDFON_HOME` set, daemon data lives under it on every platform. The
//! macOS data dir is the one the Mac app has always used, so a daemon the
//! CLI starts and one the app starts open the same identity. The socket
//! stays short (unix socket paths are limited to ~104 bytes) and in `/tmp`,
//! where it is runtime state, never data.

use std::io;
use std::path::{Path, PathBuf};

/// Longest accepted profile name (longer names are truncated, as before).
pub const PROFILE_MAX_CHARS: usize = 64;

/// The idfon home: `IDFON_HOME`, else `~/.idfon`.
pub fn home() -> PathBuf {
    home_from(std::env::var_os("IDFON_HOME"), std::env::var_os("HOME"))
}

fn home_from(idfon_home: Option<std::ffi::OsString>, home: Option<std::ffi::OsString>) -> PathBuf {
    match idfon_home.filter(|value| !value.is_empty()) {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from(home.unwrap_or_default()).join(".idfon"),
    }
}

/// The active profile: `explicit`, else `IDFON_PROFILE`. `None` is the
/// default profile (also for `default`, empty, or invalid names).
pub fn profile(explicit: Option<&str>) -> Option<String> {
    let raw = match explicit.filter(|value| !value.is_empty()) {
        Some(value) => value.to_owned(),
        None => std::env::var("IDFON_PROFILE").ok()?,
    };
    valid_profile(&raw)
}

/// A usable profile name: ASCII letters, digits, `-`, `_`; at most
/// [`PROFILE_MAX_CHARS`] (longer input is truncated first); not `default`.
pub fn valid_profile(raw: &str) -> Option<String> {
    let name: String = raw.chars().take(PROFILE_MAX_CHARS).collect();
    let ok = !name.is_empty()
        && name != "default"
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_');
    ok.then_some(name)
}

/// The daemon socket for a profile.
pub fn socket(profile: Option<&str>) -> PathBuf {
    legacy_data_dir(profile).join("idfond.sock")
}

/// The daemon's durable data dir for a profile.
pub fn data_dir(profile: Option<&str>) -> PathBuf {
    if std::env::var_os("IDFON_HOME").is_some_and(|value| !value.is_empty()) {
        return under(home(), "daemon", profile);
    }
    if cfg!(target_os = "macos") {
        let support = PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
            .join("Library/Application Support/Idfon");
        return under(support, "daemon", profile);
    }
    under(home(), "daemon", profile)
}

fn under(base: PathBuf, default: &str, profile: Option<&str>) -> PathBuf {
    match profile {
        Some(name) => base.join("profiles").join(name),
        None => base.join(default),
    }
}

/// Where daemon data lived before it moved out of `/tmp` (next to the socket).
pub fn legacy_data_dir(profile: Option<&str>) -> PathBuf {
    match profile {
        Some(name) => PathBuf::from(format!("/tmp/idfon-{name}")),
        None => PathBuf::from("/tmp/idfon"),
    }
}

/// `<home>/config.toml`.
pub fn config_file() -> PathBuf {
    home().join("config.toml")
}

/// `<home>/models`.
pub fn models_dir() -> PathBuf {
    home().join("models")
}

/// Files that are runtime state, never copied by a migration.
const RUNTIME_FILES: &[&str] = &["idfond.sock", "state.lock", "idfond.log"];

/// Copies a pre-move daemon profile from `legacy` into `data` once: only
/// when `data` holds no state yet and `legacy` does. Returns how many
/// entries were copied (0 when there was nothing to do).
pub fn migrate_legacy_data(legacy: &Path, data: &Path) -> io::Result<usize> {
    let has_state = |dir: &Path| dir.join("state.json").exists() || dir.join("state.db").exists();
    if legacy == data || has_state(data) || !has_state(legacy) {
        return Ok(0);
    }
    std::fs::create_dir_all(data)?;
    let mut copied = 0;
    for entry in std::fs::read_dir(legacy)? {
        let entry = entry?;
        let name = entry.file_name();
        if RUNTIME_FILES.iter().any(|runtime| name == *runtime) {
            continue;
        }
        copy_recursive(&entry.path(), &data.join(&name))?;
        copied += 1;
    }
    Ok(copied)
}

fn copy_recursive(from: &Path, to: &Path) -> io::Result<()> {
    if from.is_dir() {
        std::fs::create_dir_all(to)?;
        for entry in std::fs::read_dir(from)? {
            let entry = entry?;
            copy_recursive(&entry.path(), &to.join(entry.file_name()))?;
        }
        Ok(())
    } else {
        std::fs::copy(from, to).map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn home_prefers_idfon_home() {
        assert_eq!(home_from(Some("/x".into()), Some("/h".into())), PathBuf::from("/x"));
        assert_eq!(home_from(Some("".into()), Some("/h".into())), PathBuf::from("/h/.idfon"));
        assert_eq!(home_from(None, Some("/h".into())), PathBuf::from("/h/.idfon"));
    }

    #[test]
    fn profiles_validate_like_before() {
        assert_eq!(valid_profile("alice").as_deref(), Some("alice"));
        assert_eq!(valid_profile("a-b_9").as_deref(), Some("a-b_9"));
        assert_eq!(valid_profile("default"), None);
        assert_eq!(valid_profile(""), None);
        assert_eq!(valid_profile("../evil"), None);
        assert_eq!(valid_profile("sp ace"), None);
        assert_eq!(valid_profile(&"a".repeat(80)).map(|p| p.len()), Some(64));
    }

    #[test]
    fn socket_paths_are_unchanged() {
        assert_eq!(socket(None), PathBuf::from("/tmp/idfon/idfond.sock"));
        assert_eq!(socket(Some("bob")), PathBuf::from("/tmp/idfon-bob/idfond.sock"));
    }

    #[test]
    fn migration_copies_state_once_and_skips_runtime_files() {
        let root = std::env::temp_dir().join(format!("idfon-config-mig-{}", std::process::id()));
        let (legacy, data) = (root.join("legacy"), root.join("data"));
        std::fs::create_dir_all(legacy.join("blobs")).unwrap();
        std::fs::write(legacy.join("state.json"), "{}").unwrap();
        std::fs::write(legacy.join("identity-default.key"), "k").unwrap();
        std::fs::write(legacy.join("blobs/a"), "b").unwrap();
        std::fs::write(legacy.join("state.lock"), "").unwrap();
        assert_eq!(migrate_legacy_data(&legacy, &data).unwrap(), 3);
        assert!(data.join("identity-default.key").exists());
        assert!(data.join("blobs/a").exists());
        assert!(!data.join("state.lock").exists());
        // Second run: data has state now, nothing to do.
        assert_eq!(migrate_legacy_data(&legacy, &data).unwrap(), 0);
        std::fs::remove_dir_all(&root).unwrap();
    }
}
