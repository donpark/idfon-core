# Agent notes: idfon-core

## Documentation

Start at **`docs/README.md`** (the index) and read only the entries relevant to
the task. Do not bulk-read `docs/`. If you add or materially change a doc,
update the index.

## Layout

- `crates/`: the Rust workspace (protocol, config, core, daemon, idfond, client,
  media, voice, h1, h3, channel, gateway, mcp, edge, director, iroh-c-ffi).
- `crates/iroh-c-ffi`: the C ABI behind `libiroh_c_ffi.dylib` / `.a`, which
  both apps link. Its artifact must be built before `idfond`.
- `swift/`: Swift packages the apps reference from `../idfon-core/swift/`.

## Other repos

This repo is one of several cloned side by side (see `README.md`). Changes
here can break idfon-sdk, idfon-ios and idfon-mac, which build this checkout:

- After a change to a crate the SDK uses, run the SDK's tests from
  `../idfon-sdk` (its `cargo check` and `scripts/test-cli.sh` build this repo).
- After a change to `iroh-c-ffi`, `idfon-director` or `swift/`, rebuild the
  apps (`../idfon-mac/build.sh`, `../idfon-ios/build.sh`).
- `docs/build.md` maps a change to what must be rebuilt.

## Checks

- **Verify before retrying.** For network, device, or model checks, keep the returned operation ID and inspect that operation and its events before sending again. Reusing an idempotency key returns the existing operation; it does not re-run it. A new key creates a new request and may create another model turn.
- **MAST repeat/contradiction warning.** Stop and reconcile the latest tool result with the current process/config state before taking another action. Do not repeat the last command; state what evidence changed or switch to a read-only check.

- Rebuild scope: `docs/build.md` maps a change to the subprojects that must be
  rebuilt. When you change something shared, say which subprojects to rebuild.
- **Debugging calls/wakes/push: ask, don't grep.** Before any device-log
  archaeology (`devicectl` pulls, sysdiagnose, timestamp reconstruction), query
  the diagnostics the apps already report: `wake.trace` (one call's timeline:
  registration → ring → push → report → answer → media) and `diag.tail` via
  `idfon-mcp`; raw views at `<data-dir>/diag/`; edge health at `/_push/health`.
  See `docs/mcp-control.md` + `docs/diag.md`. (2026-11-10: hours were lost
  reconstructing from USB logs what the app was already reporting.)
- **`rung=0` is an edge problem until proven otherwise** — for a call whose
  ring score should ring or notify (person calls: 80; see `docs/protocol.md`
  "Ring score"). Below the callee's notify threshold, `rung=0` is correct
  (silent). When the caller's app logs `register → 200` and
  `ring → 200 rung=0` for a call that should have pushed, the edge found no
  token or **every APNs send failed** — the apps cannot see which. Read the droplet's
  edge log (`docker compose logs idfon-edge | grep -E 'idfon.edge.push|apns'`
  in `/srv/idfon-edge`; find the droplet with `doctl compute droplet list`)
  before touching app code. `403 InvalidProviderToken` = wrong
  `--push-apns-key-id` for `edge.apns.p8` (C26, regressed as C28). Compose
  refuses to start without the id; never reintroduce a default, and run
  `idfon-edge/docs/edge-deploy-checklist.md` §C step 4 after any edge change.
- **No identifiers in public artifacts.** These repos are public. Keep APNs key
  ids, push tokens, peer/endpoint ids, call UUIDs, droplet IPs and SSH key
  names out of commits, PR titles/bodies, issues and docs. Use placeholders
  (`<key-id>`, `<peer>`) and local files (`~/.idfon/edge.apns.keyid`).
- **Privacy posture lives in `README.md` ("Privacy").** Keep any new surface
  consistent with it: content end-to-end between the parties, the edge holds
  wake tokens + brief invites only, APNs payloads are identifiers-only,
  diagnostics are markers to a user-configured monitor, and control surfaces
  are tier-approved over iroh. If a change would add a new party to the table,
  update that table first.
- Rust: `cargo test -p <crate>` and `cargo check --workspace --all-targets`.