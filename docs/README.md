# Idfon docs index

**Read this first, then open only the files you need.** Do not bulk-read `docs/`
— most questions touch one or two entries below.

Status legend: **current** = describes the code as it is; **as-built** = a plan
kept because it records decisions; **design** = not all implemented.

## Orientation

| Doc | Covers | Read when |
| --- | --- | --- |
| `../AGENTS.md` | repository working rules and the iOS shipping checklist (gated IPA, credentials, build numbers, upload verification, TestFlight push smoke test) | shipping an iOS update or checking agent workflow rules |
| `communication-model.md` | identities, peers, messages, grants | anything about the core model |
| `daemon.md` | why there is a daemon at all; the daemon as packaging of the feature functions | questioning the process model |
| `protocol.md` | daemon JSON IPC methods/params | adding/changing an RPC or a client |
| `build.md` | which subprojects to rebuild after a change; the Rust/FFI target dirs; `pnpm build`/`pnpm voice build` orchestration; build-time options (`IDFON_TTS_PHONON`) | touching a shared crate or the c-ffi crate, unsure what needs rebuilding, building/cleaning everything, or toggling a build-time option |

## Platforms and apps

| Doc | Covers |
| --- | --- |
| `durable-files.md` | Durable file publication independent of sessions/agent lifecycle: the edge's content-addressed store (`/files`, `/models`), `idfon publish`, 24 h user pins / permanent operational pins, the agent tool, BunnyCDN, ops-only takedown with a sticky blocklist and write rate limits; as-built (no paid tier) |
| `mcp-control.md` | MCP control-surface access model: three approval tiers over iroh/capability tickets, the request-time approval block (lock-screen actionable variant), verb-class scoping, audit-over-Diag; initial read-only tool slice |
| `contact-exchange.md` | vCard contact cards: the NameDrop substitute, `idfon://add/` deep links, QR, import/export surfaces, the approval modal (rename/deny, admission-code auth), the test-contacts page (`scripts/test-contacts-page.mjs`) |

## Networking, addressing, transport

| Doc | Covers |
| --- | --- |
| `idfon-gateway.md` | `idfon://` addressing, loopback gateway, HTTP/3 provider |
| `multi-device.md` | one account across devices (design) |
| `relay-config.md` | relay selection at runtime — parameter (flags / app launch args / FFI / `IDFON_RELAY_*`) > configuration (app Relay settings, `~/.idfon/relay.conf`) > default (N0); who honors it and why the vendored libs must be re-staged (as-built) | configuring a relay, changing relay without a binary rebuild, precedence questions |
| `config.md` | where idfon keeps things (home, daemon data, socket, `config.toml`), profiles, every setting and its env var, the `/tmp` data migration, `idfon config path`/`show` (as-built) | finding a file, adding a setting, running a second daemon, or debugging "wrong identity" |
| `chatrooms.md` | rooms, direct fan-out, gossip |
| `mcp-transport.md` | idfon as an MCP transport binding (implemented) |
| `idfon-h1.md` | HTTP/1.1 over iroh: serve an existing `axum::Router` with WebSockets, SSE and chunked bodies, `RemoteId` peer identity, client + upgrade handle (slices 1–2 of #142 tested; voxl demo pending) |
| `idfon-channel.md` | message channels over iroh with no HTTP: WebSocket-shaped send/recv on one ordered bi-stream per channel, channel-name sharding and its isolation test, the measured WS-vs-channel overhead A/B (library; no consumer wired yet) |

## Media

| Doc | Covers |
| --- | --- |
| `media-adapter-seam.md` | `video-frame.jpg` polling, seams, bundled playback / shell playback callback |
| `media-adaptation.md` | live-call robustness: VoIP field knowledge (jitter buffers, Opus FEC/DTX/ABR, GCC/SCREAM, Wi-Fi↔cellular handover, burst/gap loss metrics), our MoQ/iroh stack reviewed against it (what `adaptive.rs`/`NetworkSignals`/moq knobs cover vs gaps), dynamic demand-based media configuration, current 60 ms initial / 60–200 ms adaptive playout target, the iOS rendition ladder and why the photo-first gate was removed (#208), prioritized test/change list |
| `moq-fork.md` | the `donpark/moq` fork: the three patch features (loss recovery, working FEC knob, BitrateControl), upstream's two living lines (`release` vs `main`) and our branch map, the update procedure and the git-source unification rules | when touching the moq/media transport stack, updating the fork or the iroh-live pin, or debugging split `moq_net` types |

## Artifacts and agents

| Doc | Covers |
| --- | --- |
| `webmcp-bridge.md` | WebMCP bridgehead + MCP as the app↔agent surface: pages over the gateway for bytes, WebMCP as the in-page declaration shim, MCP over `idfon/mcp/1` for tool exposure, `ui.serve`/`ui.tools` grants for authority, trust tiers and IPAE gating, P0–P4 (design) |
| `reply-blocks.md` | what a reply is: the named block set (`Text`/`Speak`/`Artifact`/`View`/`JsonRender`/`Reference`/`Unparsed`) over the `IDFON-*/1` envelopes, ids and replacement, fail-open parsing, render-spec limits, where each piece lives (as-built) |


## Operations

| Doc | Covers |
| --- | --- |
| `troubleshooting.md` | known issues, diagnoses, fixes; agent profile identity vs display name, active speech-route introspection, and idempotent send/retry workflow |
| `observability.md` | unified telemetry: shared subscriber, structured fields, `trace` correlation, opt-in OTLP, the external-agent seam (as-built; open items in §10) |
| `diag.md` | `IDFON-DIAG/1` app diagnostics over iroh: protocol, app reporter, the `monitor` filing agent, query examples (issue #63) |
| `disk-hygiene.md` | the ANE/CoreAI compile leftovers (scratch workdirs, coreai-cache registrations, daemon caches): the mechanism, `pnpm ane list`/`pnpm ane clean` usage, the output contract for scripts and agents, the flush experiment |
| `dylib-refactoring-plan.md` | shared-dylib build as-built + follow-up log |
The edge and relay-auth docs (`idfon-edge.md`, `edge-deploy-checklist.md`,
`relay-auth.md`, `relay-test-plan.md`) moved to the idfon-edge repo.
