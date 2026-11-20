# Paths and settings

Status: as-built (2026-11-19). Code: `crates/idfon-config`.

## Where things live

| what | default | with `--profile p` / `IDFON_PROFILE=p` |
| --- | --- | --- |
| home (config, models, records) | `$IDFON_HOME`, else `~/.idfon` | same |
| daemon data (identity keys, peers, state) | macOS: `~/Library/Application Support/Idfon/daemon`; Linux: `<home>/daemon` | `…/Idfon/profiles/p`; `<home>/profiles/p` |
| socket + daemon log | `/tmp/idfon/` | `/tmp/idfon-p/` |
| config file | `<home>/config.toml` | same |

- With `IDFON_HOME` set, daemon data lives under it on every platform.
- The macOS data dir is the one the Mac app has always used, so the CLI and
  the app now open the same identity whichever starts the daemon first.
  Before this, a CLI-started daemon kept its identity in `/tmp/idfon`, which
  macOS clears.
- `idfond` migrates a pre-move `/tmp/idfon[-p]` profile once, when the new
  data dir has no state yet (identity keys, state, blobs; not the socket,
  lock or log).
- A custom `--socket` keeps its data next to it, as before (tests and
  fixtures rely on this).
- `idfon config path` prints all four for the active profile.

## Settings

Each setting has one name: an env var and a `config.toml` key. The env var
wins; the file is the persisted value; per-run flags (`--relay-urls`, …)
beat both. `idfon config show` prints every setting with its value (secrets
masked) and source.

```toml
# ~/.idfon/config.toml
[relay]
urls = "https://relay.example:8443"
only = false

[edge]
url = "https://idfon.net"

[daemon]
idle_exit_secs = 300
```

The full list is `idfon_config::SETTINGS` (relay, edge, files, mcp, daemon).
To add a setting: add a row there, then read it with `idfon_config::get("IDFON_…")`
(or `flag` for booleans: `1|true|yes|on` / `0|false|no|off`). It appears in
`config show` automatically.

`~/.idfon/relay.conf` is still read for `[relay]` keys `config.toml` leaves
unset.

## Not covered yet

- Env vars outside `SETTINGS` (voice, telemetry, test knobs, deploy inputs) still
  read the environment directly.
- No `idfon config set`; edit the file.
- The apps keep their own UserDefaults settings (`docs/relay-config.md`).
