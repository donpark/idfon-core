# idfon-core

The core of Idfon, a peer-to-peer
communication and data-transfer system built on [iroh](https://www.iroh.computer):
the wire protocol, identity and capability tickets, transport, media, the
daemon (`idfond`), the C ABI the iOS and macOS apps link (`iroh-c-ffi`), and the Swift packages both apps use (`swift/`).

> **Status:** experimental and pre-release.

## The idfon repositories

Clone them side by side (for example in `~/dev`):

| Repo | What it holds |
| --- | --- |
| [idfon-core](https://github.com/donpark/idfon-core) | protocol, identity, transport, media, the daemon (`idfond`), the C ABI the apps link, and the Swift packages both apps use |
| [idfon-sdk](https://github.com/donpark/idfon-sdk) | the `idfon` CLI, the MCP server, the Eve integration and agents, the voice-agent runner, and the end-to-end tests |
| idfon-ios, idfon-mac (private) | the iPhone and macOS apps |
| idfon-edge (private) | the `idfon.net` edge server, its deployments, and the relay auth service |
| idfon-home, idfon (private) | product docs and planning; the pre-split archive and model R&D |

idfon-sdk and both apps build idfon-core from `../idfon-core`; set
`IDFON_CORE` to use another checkout.

## Privacy

Idfon is peer-to-peer: content moves directly between the parties' devices,
and keys live on the devices. The always-on edge is a **wake channel, not a
content host**. The precise picture, because vague privacy claims are worse
than none:

| Who | What they see |
|---|---|
| The other party(ies) | Messages and media, end-to-end over iroh (QUIC). A relay (N0's or `relay.idfon.net`) may forward **ciphertext** when NAT demands it. |
| The edge (`idfon.net` or self-hosted) | Push device tokens + topics per identity (needed to wake a phone), each device's ring thresholds, each ring's score (in memory, ~5 min), and — briefly — the **call invite** so a push-woken phone can pull it (in memory, ~5 min, dropped on cancel). The invite carries the call's media ticket; anyone who could read the edge store in those minutes could join that call's media. Self-host the edge if that is a concern for you. |
| Apple (APNs) | **Identifiers only** — `call_uuid`, `caller_id`, `has_video`, plus fixed generic text ("Incoming call" / "Missed call") on notification pushes. No names, no content; display names resolve on-device from the user's own contacts. |
| The iOS system call log | Real peer display names, by deliberate decision (donpark/idfon#48): the call log is the user's own device log. A “hide from call log” preference is the switch if that ever changes. |
| A diagnostics monitor (`IDFON-DIAG/1`) | Event markers only — wake outcomes, push registration results, timings, build stamps. Sent **only** to a monitor peer the user configures; no audio, no message content, no token values. |
| An MCP agent (see `docs/mcp-control.md`) | Reachable only over iroh, to parties the user approves: own devices (standing grants), approved parties (request-time approval block, short-lived scoped tickets), outsiders (one-shot tickets). Diagnostics are read-only first; every request and approval is audited; there is no public HTTP control endpoint. |

Two honest notes: capability **tickets are bearer credentials** — whoever
holds one can exercise its scoped rights until it expires, so treat them
like passwords. And while the iPhone app is suspended and the screen is
locked, nothing runs to be observed — the frozen window is visible only in
what the app reported before and after it.

## Layout

| Path | What |
| --- | --- |
| `crates/` | the Rust workspace; `crates/README.md` says what each crate is for |
| `swift/` | `tyger-swift` (on-device turn-taking) and `liquid-swift`, used by both apps |
| `scripts/` | daemon, relay-lab and telemetry helpers |
| `docs/` | design and as-built docs; start at [docs/README.md](docs/README.md) |

## Development

```sh
cargo check --workspace --all-targets
cargo test -p idfon-protocol -p idfon-core -p idfon-daemon
```

The end-to-end tests drive the daemon through the `idfon` CLI, so they live in
idfon-sdk (`scripts/test-cli.sh`, `scripts/test-e2e.sh`, …) and build this
repo from `../idfon-core`.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).
