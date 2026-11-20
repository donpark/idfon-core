# Crates

What each crate is for, one line each. Every crate has a `README.md` with the
detail; `docs/README.md` indexes the design docs.

Read the list **bottom-up for the layers**: `idfon-protocol` and `idfon-core` are
the shared floor, the transports and services sit on them, and the apps reach all
of it through `iroh-c-ffi`. The CLI, the MCP server, the Eve integration and
the voice-agent runner live in idfon-sdk.

## Foundation

| Crate | What it is |
| --- | --- |
| `idfon-protocol` | Versioned wire types shared by the daemon and clients (messages, peers, capabilities, calls, blocks, vCards). No transport, no policy. |
| `idfon-core` | Identity, signing, capability tickets, signed paths, relay selection, transport seams. Native + wasm (the Worker uses the gated subset). |
| `idfon-config` | Where idfon keeps things (home, daemon data, socket, `config.toml`, profiles) and its settings: env var over `~/.idfon/config.toml`. No networking deps. |
| `idfon-telemetry` | One process-wide tracing subscriber, installed once; `IDFON_LOG` > `RUST_LOG` > `IROH_C_LOG`. |

## Daemon and its clients

| Crate | What it is |
| --- | --- |
| `idfon-daemon` | The feature functions and request dispatch (blobs, live sessions, MCP bridge, relay auth) — the real daemon. |
| `idfond` | Launcher binary only: parse args, call `idfon_daemon_run` in the c-FFI dylib. |
| `idfon-client` | Daemon IPC client: framing, socket path, retry, response validation. The single source of truth for all of it. |

## Transports and HTTP seams

| Crate | What it is |
| --- | --- |
| `idfon-channel` | WebSocket-shaped message channels over one ordered iroh bi-stream, no HTTP. Library only, no consumer yet. |
| `idfon-h1` | HTTP/1.1 over iroh: serve an existing `axum::Router` (WebSockets, SSE, chunked bodies work unmodified) + client. |
| `idfon-h3` | HTTP/3 over iroh on a side-channel ALPN beside the message ALPN + client. |
| `idfon-gateway` | Reusable loopback HTTP gateway for `idfon://` resources; policy and providers arrive as a `Backend` + `Authorizer`. |
| `idfon-edge` | Public-ingress peer: terminates HTTP(S) for a browser/WebView and bridges to the resource owner over HTTP/3-over-iroh. |

## MCP

| Crate | What it is |
| --- | --- |
| `idfon-mcp` | `idfon-mcp` bridge binary over `idfon/mcp/1`, plus the read-only folder provider for stdio or H3. |

## Media

| Crate | What it is |
| --- | --- |
| `idfon-media` | The media plane: live-call sessions, MoQ video, the capture/render adapter seam, headless pub/sub, connection health. |

## Voice

| Crate | What it is |
| --- | --- |
| `idfon-voice` | The STT/TTS/endpointing provider seam plus the voice-service pieces around it (barge-in, echo, arbitration, turns). Boundary is text. |
| `idfon-director` | The loop boundary — turns, tools, trace — over `yoagent`; on-device tier is Apple Foundation Models. |

## App binding

| Crate | What it is |
| --- | --- |
| `iroh-c-ffi` | The C ABI the iOS/mac/native apps link: daemon, client, media, and voice surfaces in one dylib/staticlib. |

## Building one

```sh
cargo test -p <crate>            # the crate
cargo check --workspace --all-targets
```

`docs/build.md` maps a change to the subprojects that must be rebuilt — the Apple
apps and the c-FFI artifact have an ordering that a bare `cargo build` does not
express.
