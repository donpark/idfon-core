//! Where idfon keeps things, and how its settings resolve.
//!
//! **Paths** ([`paths`]): one home (`IDFON_HOME`, else `~/.idfon`) for config,
//! models and records; a durable daemon data dir; a short runtime socket path.
//! A profile (`--profile` / `IDFON_PROFILE`) selects both socket and data dir.
//!
//! **Settings**: each setting has one name, its env var (`IDFON_EDGE_URL`),
//! and one config-file key (`edge.url` in `~/.idfon/config.toml`). The env
//! var wins; the file is the persisted value; callers supply the default.
//! Per-run flags (e.g. `--relay-urls`) sit above both, in the caller.
//! [`SETTINGS`] lists every setting, so `idfon config show` and the docs can
//! never drift from the code.

pub mod paths;

use std::sync::OnceLock;

/// One setting: its env var, its `config.toml` key, and a one-line meaning.
#[derive(Debug, Clone, Copy)]
pub struct Setting {
    pub env: &'static str,
    pub key: &'static str,
    pub help: &'static str,
    /// Values are credentials: shown masked.
    pub secret: bool,
}

const fn setting(env: &'static str, key: &'static str, help: &'static str) -> Setting {
    Setting { env, key, help, secret: false }
}

const fn secret(env: &'static str, key: &'static str, help: &'static str) -> Setting {
    Setting { env, key, help, secret: true }
}

/// Every setting read through this crate.
pub const SETTINGS: &[Setting] = &[
    setting("IDFON_RELAY_URLS", "relay.urls", "relay URLs, comma-separated (default: n0 relays)"),
    secret("IDFON_RELAY_TOKEN", "relay.token", "relay bearer token"),
    setting("IDFON_RELAY_ONLY", "relay.only", "drop direct IP paths, relay only (true/false)"),
    setting("IDFON_RELAY_CA_FILE", "relay.ca_file", "PEM bundle for a relay with a private CA"),
    setting("IDFON_EDGE_URL", "edge.url", "public edge base URL (gateway fallback, `publish`, `web`)"),
    secret("IDFON_EDGE_TOKEN", "edge.token", "edge bearer token"),
    secret("IDFON_EDGE_TICKET", "edge.ticket", "edge capability ticket"),
    secret("IDFON_FILES_TOKEN", "files.token", "upload token for `idfon publish` (default: edge.token)"),
    secret("IDFON_FILES_OPS_TOKEN", "files.ops_token", "operations token for `publish --permanent`"),
    setting("IDFON_MCP_COMMAND", "mcp.command", "local MCP server inbound idfon/mcp/1 streams splice to"),
    setting("IDFON_IDLE_EXIT_SECS", "daemon.idle_exit_secs", "auto-started daemon idle shutdown, seconds (0 = never)"),
];

/// Where a setting's value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Env,
    File,
    Unset,
}

/// A setting's effective value: the env var when set and non-empty, else
/// the config file's value. `name` is the env var name.
pub fn get(name: &str) -> Option<String> {
    lookup(name).0
}

/// [`get`] plus where the value came from.
pub fn lookup(name: &str) -> (Option<String>, Source) {
    resolve(name, |var| std::env::var(var).ok(), file())
}

/// A boolean setting: `1|true|yes|on` → true, `0|false|no|off` → false.
/// Unset or unparsable → `None`.
pub fn flag(name: &str) -> Option<bool> {
    get(name).and_then(|value| parse_flag(&value))
}

pub fn parse_flag(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" | "" => Some(false),
        _ => None,
    }
}

/// The config file's own value for a setting (no env), for callers that
/// rank env and file at different tiers.
pub fn file_value(name: &str) -> Option<String> {
    let key = SETTINGS.iter().find(|setting| setting.env == name)?.key;
    table_value(file(), key)
}

fn resolve(
    name: &str,
    env: impl Fn(&str) -> Option<String>,
    file: &toml::Table,
) -> (Option<String>, Source) {
    if let Some(value) = env(name).filter(|value| !value.trim().is_empty()) {
        return (Some(value), Source::Env);
    }
    let value = SETTINGS
        .iter()
        .find(|setting| setting.env == name)
        .and_then(|setting| table_value(file, setting.key));
    match value {
        Some(value) => (Some(value), Source::File),
        None => (None, Source::Unset),
    }
}

/// `section.key` in a table, as a string (numbers and booleans stringified).
fn table_value(table: &toml::Table, key: &str) -> Option<String> {
    let (section, field) = key.split_once('.')?;
    let value = table.get(section)?.as_table()?.get(field)?;
    let text = match value {
        toml::Value::String(text) => text.clone(),
        toml::Value::Integer(number) => number.to_string(),
        toml::Value::Boolean(flag) => flag.to_string(),
        toml::Value::Float(number) => number.to_string(),
        _ => return None,
    };
    (!text.trim().is_empty()).then_some(text)
}

/// The parsed config file, read once per process. A missing file is empty;
/// an unreadable or invalid one is reported once on stderr and ignored. The
/// legacy `~/.idfon/relay.conf` (`key=value`) fills `[relay]` keys the file
/// does not set.
fn file() -> &'static toml::Table {
    static FILE: OnceLock<toml::Table> = OnceLock::new();
    FILE.get_or_init(|| {
        let path = paths::config_file();
        let mut table = match std::fs::read_to_string(&path) {
            Ok(text) => text.parse::<toml::Table>().unwrap_or_else(|error| {
                eprintln!("idfon: ignoring {}: {error}", path.display());
                toml::Table::new()
            }),
            Err(_) => toml::Table::new(),
        };
        if let Ok(text) = std::fs::read_to_string(paths::home().join("relay.conf")) {
            merge_relay_conf(&mut table, &text);
        }
        table
    })
}

fn merge_relay_conf(table: &mut toml::Table, text: &str) {
    let relay = table
        .entry("relay")
        .or_insert_with(|| toml::Value::Table(toml::Table::new()));
    let Some(relay) = relay.as_table_mut() else {
        return;
    };
    for line in text.lines().map(str::trim) {
        if line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let (key, value) = (key.trim(), value.trim());
        if matches!(key, "urls" | "token" | "only") && !value.is_empty() {
            relay
                .entry(key)
                .or_insert_with(|| toml::Value::String(value.to_owned()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(text: &str) -> toml::Table {
        text.parse().unwrap()
    }

    #[test]
    fn env_beats_file_and_empty_env_falls_through() {
        let file = table("[edge]\nurl = \"https://file\"\n");
        let env = |value: &'static str| move |_: &str| Some(value.to_owned());
        assert_eq!(
            resolve("IDFON_EDGE_URL", env("https://env"), &file),
            (Some("https://env".into()), Source::Env)
        );
        assert_eq!(
            resolve("IDFON_EDGE_URL", env("  "), &file),
            (Some("https://file".into()), Source::File)
        );
        assert_eq!(
            resolve("IDFON_EDGE_URL", |_| None, &toml::Table::new()),
            (None, Source::Unset)
        );
    }

    #[test]
    fn numbers_and_booleans_read_as_strings() {
        let file = table("[daemon]\nidle_exit_secs = 0\n[relay]\nonly = true\n");
        assert_eq!(resolve("IDFON_IDLE_EXIT_SECS", |_| None, &file).0.as_deref(), Some("0"));
        assert_eq!(resolve("IDFON_RELAY_ONLY", |_| None, &file).0.as_deref(), Some("true"));
    }

    #[test]
    fn flags_parse_one_way() {
        for yes in ["1", "true", "YES", "on"] {
            assert_eq!(parse_flag(yes), Some(true));
        }
        for no in ["0", "false", "No", "off", ""] {
            assert_eq!(parse_flag(no), Some(false));
        }
        assert_eq!(parse_flag("maybe"), None);
    }

    #[test]
    fn relay_conf_fills_only_unset_keys() {
        let mut file = table("[relay]\nurls = \"https://toml\"\n");
        merge_relay_conf(&mut file, "# legacy\nurls=https://conf\ntoken = t1\n");
        assert_eq!(table_value(&file, "relay.urls").as_deref(), Some("https://toml"));
        assert_eq!(table_value(&file, "relay.token").as_deref(), Some("t1"));
    }

    #[test]
    fn every_setting_has_a_unique_name_and_key() {
        for (i, a) in SETTINGS.iter().enumerate() {
            assert!(a.env.starts_with("IDFON_"), "{}", a.env);
            assert!(a.key.contains('.'), "{}", a.key);
            for b in &SETTINGS[i + 1..] {
                assert_ne!(a.env, b.env);
                assert_ne!(a.key, b.key);
            }
        }
    }
}
