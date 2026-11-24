# Idfon Daemon JSON IPC Protocol

The contract between the idfond daemon and every frontend (CLI, iOS, Android,
desktop). Reference implementation: `crates/idfon-protocol` (types) and
`crates/idfon-daemon/src/lib.rs` (dispatch). Regression coverage:
`scripts/test-cli.sh`.

## Transport

Unix domain socket (path ≤ ~104 bytes: device sandbox paths fit without the
longer `/tmp` fallback). Framing: 4-byte little-endian length prefix +
JSON body; one request frame expects one response frame (the daemon may send
multiple response frames for `event.list` with `follow: true` and for `event.wait`
while it holds the request until a match or timeout).

The frames are the contract, not the socket: an embedding target (the iOS
app) calls the same dispatch in-process through `idfon_request`
(`idfon_daemon::request_json`), one request frame in, one response frame out.
`events --follow`'s multi-frame stream and `daemon.shutdown` exist only on
the socket server.

## Envelope

Request:

```json
{ "version": 2, "id": "cli-1", "method": "peer.list", "params": {} }
```

- `version`: `PROTOCOL_VERSION` (currently **2**; bump on any breaking shape
  change; daemons reject mismatched versions). Version 2 replaced the closed
  capability enum with open namespaced strings.
- `id`: client-chosen, echoed in the response.
- `params`: per-method (below). The daemon fills the `identity` param with
  the session identity when it is absent or `null`.

Response:

```json
{ "version": 2, "id": "cli-1", "ok": true, "operation": "daemon.status", "result": { } }
```

Failure: `ok: false` with `error: { code, message, retryable }`. Error codes
include `invalid_request`, `capability_denied`, `cursor_too_old`,
`daemon_unavailable`.

## Method names

Every method is `noun.verb` with a singular noun (`peer.list`, `event.wait`,
`blob.put`). The canonical names and their typed params live in
`crates/idfon-protocol/src/rpc.rs`; clients build requests with
`Request::call(id, &rpc::PeerAdd { .. })` instead of hand-written JSON.
Absent optional params are omitted, never sent as `null`.

Older names are accepted as aliases on any version (`rpc::METHOD_ALIASES`).
The daemon rewrites them before dispatch, and the response's `operation`
field carries the canonical name:

| legacy | canonical |
|---|---|
| `status`, `context` | `daemon.status` |
| `identities`, `identities.compact` | `identity.list`, `identity.list.compact` |
| `peers`, `peers.compact` | `peer.list`, `peer.list.compact` |
| `events`, `events.compact`, `events.export`, `events.sync`, `events.merge` | `event.list`, `event.list.compact`, `event.export`, `event.sync`, `event.merge` |
| `wait` | `event.wait` |
| `capability.ticket`, `capability.ticket.revoke` | `access.ticket`, `access.ticket.revoke` |
| `media.resource.put`, `media.resource.fetch` | `blob.put`, `blob.get` |
| `media.live.{publish,dial,answer,stop,subscribe}`, `media.live.publishers` | `live.{publish,dial,answer,stop,subscribe}`, `live.list` |
| `media.session.{start,stop}`, `media.sessions` | `session.{start,stop}`, `session.list` |
| `files.publish` | `file.publish` |

**Peer argument.** Methods that take one peer accept it as `peer`. The
daemon copies `peer` into the key the method has always read (`ref` for
`peer.*` and `web.url`; `to` for `message.send`, `mcp.listen`, `live.dial`;
`subject` for `access.*`; `from` for `live.answer`) when that key is absent.
The typed structs still send the legacy key, so a new client works with an
older daemon. `event.list`/`event.wait` keep `peer` as their own filter.

**Identity.** The daemon fills an absent or `null` `identity` param with the
session identity.

## Methods

### Lifecycle

| method | params | result |
|---|---|---|
| `daemon.status` | — | `{ ready, daemon, protocol_version, identity: {id, name, active, public_key, endpoint_id}, ticket: [bytes] }` (ticket = endpoint addr ticket, JSON byte array) |
| `daemon.shutdown` | — | `{ shutdown: true }` |

### Identity

| method | params | result |
|---|---|---|
One you: identities are tags on contacts now (2026-11-24, `docs/identity.md`).

| method | params | result |
|---|---|---|
| `identity.list` | — | `{ identities: [{id, name, active, public_key, endpoint_id}] }` — one entry, `default` |
| `identity.rename` | `{ name }` | your name (what your card shows) |
| `identity.create`, `identity.delete` | — | refused |

### Tags and visibility

See `docs/identity.md`.

| method | params | result |
|---|---|---|
| `tag.list` | — | `{ tags: [{name, hide_presence, hide_photo, count}] }` |
| `tag.set` | `{ name, hide_presence?, hide_photo? }` | `{ tag }` |
| `tag.rename` | `{ name, to }` | `{ renamed: true }` |
| `tag.delete` | `{ name }` | `{ deleted: true }` |
| `visibility.get` | — | `{ visibility: {presence, photo} }` (your defaults) |
| `visibility.set` | `{ presence?, photo? }` | `{ visibility }` |
| `visibility.audience` | `{ aspect: "presence" \| "photo" }` | `{ aspect, peers: [id] }` — who may see it |

`peer.add` takes `tags`; `peer.update` takes `tags` (replace), `add_tag`,
`remove_tag`, and `visibility: {presence?, photo?}` (this contact's overrides).

### Peers

`ref` in every method matches peer id, name, alias, or endpoint id.

`ring_score` (integer `0..=100`) is how urgent/important **this device's**
calls to that peer are: the sender's default, sent with every call (invite
`score=` line, edge ring `score`). It is **optional**: absent means unset, and
the shell applies its default for the contact's kind (people 80, agents and
stations 50). `peer.add` without it leaves it unset; `peer.update` only changes
it when the param is present, and `"auto"` clears it. Anything else is rejected
with `invalid_request`. The legacy `call_mode` param is still accepted
(`call_kit` → 80, `bar` → 50, `auto` → unset); `call_mode` is never written.

The **receiver** decides what a score does, with its own thresholds (default
`call` 70, `notify` 30; `idfon_protocol::ring::Thresholds`):

| score | delivery |
|---|---|
| `>= call` | VoIP push → the system call UI (CallKit owns the call) |
| `>= notify` | alert push → a notification ("Incoming call") that opens the caller's chat |
| below | no push; the call shows in the chat (and in-app if open) |

A device registers its thresholds with the edge beside its push tokens and
applies the same ones to invites that arrive over iroh. Filtering abusive
contacts (e.g. a per-contact cap on the score a contact can reach you with)
belongs on this receiver side and is future work.

Migrations on load: stores from before 2026-11-17 kept an implicit `"bar"` for
every peer, cleared once (`call_mode_auto_v1`); remaining legacy `call_mode`
values fold into `ring_score` once (`ring_score_v1`).

| method | params | result |
|---|---|---|
| `peer.list` | — | `{ peers: [Peer] }`; Peer = `{id, identity, name, aliases, endpoint_id, endpoint_addr, ring_score?, ...}` |
| `peer.add` | `{ ref, id, name, endpoint_id, endpoint_addr, aliases, ring_score?, mcp_ticket? }` | `{ peer }` |

`mcp_ticket` (M3) is an MCP contact ticket; when present it fills any omitted
`id`/`endpoint_id`/`endpoint_addr`/`name` and caches the ticket's
`server/discover` result. See the MCP transport section.
| `peer.update` | same as add | `{ peer }` |
| `peer.remove` | `{ ref }` | `{}` |
| `peer.resolve` | `{ ref }` | live channel info |
| `peer.show` / `peer.status` | `{ ref }` | `{ peer }` / connectivity |

#### Contact exchange (vCards)

`contact.export` wraps a contact ticket in a vCard 3.0 card and
`contact.import` unwraps one (codec: `crates/idfon-protocol/src/vcf.rs`). The
card carries the ticket twice: `X-IDFON-TICKET` (ticket token) and
`URL:idfon://add/<base64url>` — a deep link that survives an AirDrop
round-trip through the Contacts app, which strips unknown X- properties but
keeps standard `URL` fields. `peer.add` also accepts a `contact_ticket`
(ContactTicket JSON or JSON string) and names the peer from its `label` when
`name` is omitted. See [contact-exchange.md](contact-exchange.md).

| method | params | result |
|---|---|---|
| `contact.export` | `{ ref?, identity? }` | `{ vcard, link }`; `ref` exports that saved peer, omitted exports an identity's own card (`identity` names the line by id or name, default `default`); `link` is the `idfon://add/<base64url>` deep link |
| `contact.import` | `{ text, identity? }` | `{ peers: [Peer] }`; upserts each idfon card (aborts on a card that fails validation; idempotent) |

### Messaging

| method | params | result |
|---|---|---|
| `message.send` | `{ to, text, idempotency_key, capability_ticket?, retries?, durable? }` (`capability_ticket` is the holder's rcan token string) | `{ authenticated, message_id, operation_id, status: "queued" }` — returns immediately; delivery proceeds in the daemon (retries per `retries`), confirm via `operation.wait`. Sender needs a `message.send` grant for the peer (own daemon); the receiver needs `message.receive` for the sender. A successful send auto-establishes the local receive side of the reply path. `durable: true` makes it an outbox item: if every attempt fails it goes back to `queued` ("not sent yet") instead of `failed`, and is retried later (see `operation.resume`). Chat messages set it; time-bound sends (call signaling) must not. |
| `message.receive` | — | receive-side helper used by the transport; not called by shells. |

The `event.wait`/`event.list` methods below are how received messages surface: a
`message.received` event with `data: { message_id, peer_id, conversation,
idempotency_key, text }`. `message_id` is per recipient (a room send makes one
per member); `idempotency_key` is the sender's key for the message, and a room
send gives every member the same base key with a `:<member>` suffix, so apps
use the part before the first `:` to name a message (see
`docs/chat-envelopes.md`). Signaled
blob transfers ride the same path as text with an `IDFON-DATA/1` envelope
(`ticket=`, `size=` lines). Artifacts and selections use the same mechanism:
an `IDFON-ARTIFACT/1` envelope publishes a durable result and an `IDFON-REF/1`
envelope asks about part of one. A message body may carry reply text followed by
one or more trailing `IDFON-*/1` envelopes; see `docs/idfon-artifacts.md`.

### Events

Events are persisted per identity, FIFO, with monotonically increasing
`cur_<20 digits>` cursors, retained up to `EVENT_RETENTION`. The only event
type currently emitted is `message.received`.

| method | params | result |
|---|---|---|
| `event.list` | `{ follow?, after?, type?, peer? }` | `{ events: [Event] }`; Event = `{ event_id, cursor, type, timestamp, identity, data }`. `after` earlier than the oldest retained cursor → `cursor_too_old` error; empty-string `after` is allowed and means "from the start". `follow: true` streams responses as events arrive. |
| `event.wait` | same as `event.list` + `timeout_ms` (default 30000, max 300000) | blocks server-side, returns the first matching event, or `{ events: [] }` on timeout (success — not an error) |

### Operations

Async operations (message sends, transfers) are persisted with ids and
terminal statuses `delivered | failed | expired | cancelled`. A durable send
(the outbox) that cannot be delivered yet rests at `queued`, which is not
terminal: `operation.wait` on it runs to its timeout. Every status change is
an `operation.state_changed` event `{ operation_id, status, message_id }`.

**Outbox retries.** Queued sends are retried at daemon start, whenever a
peer's message arrives (proof it is reachable: its queued sends go), and on
`operation.resume` — which the apps call on their own reachability signals
(foreground, network change, opening the chat). Each retry claims its sends
first, so overlapping triggers never send one message twice. Cancelling a
queued send (`operation.cancel`) withdraws it; nothing left the device.

| method | params | result |
|---|---|---|
| `operation.get` | `{ operation_id, timeout_ms? }` | `{ operation: { status, ... } }` |
| `operation.wait` | `{ operation_id, timeout_ms? }` | polls server-side until a terminal status |
| `operation.cancel` | `{ operation_id }` | `{}` |
| `operation.resume` | `{ target? }` (peer id, name or alias) | `{ resumed }` — retries queued sends now (all peers, or `target`'s); returns how many it claimed |

### Access (capabilities)

Grants live on the granter's daemon and gate that daemon's own side:
`message.send` = this daemon may send to the subject; `message.receive` =
this daemon may receive from the subject. Subjects are peer endpoint ids
(public keys), never names.

| method | params | result |
|---|---|---|
| `access.check` | `{ subject, capability }` | `{ allowed, identity, subject, capability }` |
| `access.grant` | `{ subject, capability }` | `{ grant }` |
| `access.revoke` | `{ subject, capability }` | `{}` |
| `access.ticket` | `{ subject, capabilities: [..], expires_at?, path_scope?, conversation? }` | `{ ticket }` — an `rcan` delegation token (base32), subject-bound; a signature-valid ticket satisfies the receive gate |
| `access.ticket.revoke` | `{ ticket_id }` | `{ ticket_id, issuer, revoked }` |

Capabilities are **open namespaced strings**, not a closed enum: the built-ins
are `message.send`, `message.receive`, `voice.message.send`,
`voice.message.receive`, `live.audio.publish`, `live.audio.subscribe`,
`recording.fetch`, `recording.retain`, and `mcp.transport`, but any dotted name
is accepted, so a provider can define its own without a protocol change.

The daemon enforces one of them on what leaves the device: **`screen.read`**. A
picture of your screen travels as an `IDFON-FILE/1` marked `screen=1`, and
`message.send` refuses it (`capability_denied`) unless the recipient holds
`screen.read`. The apps ask first, and grant it briefly for "Allow once" (with
`expires_at`) or standing for "During calls". `screen.point` records that a peer's
pointer may open by itself during a call with them. The apps check that one.

### Invocation results

One shared vocabulary for the result of an invoked capability:

```json
{ "type": "text", "text": "..." }
{ "type": "render", "view": { } }
{ "type": "blob_ticket", "blob_ticket": "..." }
{ "type": "stream_ticket", "stream_ticket": "..." }
{ "type": "error", "message": "..." }
```

### MCP transport

User-side relay for the MCP transport binding. The daemon stays generic —
ALPN → configured local command, or peer bi-stream → local Unix socket — and
never parses MCP.

| method | params | result |
|---|---|---|
| `mcp.listen` | `{ to }` | `{ socket, peer, direction: "outbound" }` — opens `<data_dir>/mcp/<hash>.sock`, then splices the first local connection to the peer's `idfon/mcp/1` bi-stream. Requires an `mcp.transport` grant for the peer. |
| `mcp.configure` | `{ command }` | `{ command }` — persists the local MCP server command inbound `idfon/mcp/1` streams are spliced to. Empty/absent disables it; `IDFON_MCP_COMMAND` still overrides per process. |

Inbound `idfon/mcp/1` connections are spliced to the configured command
(`IDFON_MCP_COMMAND`, else the `mcp.configure` value), provided the remote
endpoint has an `mcp.transport` grant. `idfon-mcp connect --uds SOCKET` is the
stdio shim. `peer.show` reports `mcp_transport_granted` for the peer.

**Contact ticket (M3).** `idfon-mcp serve --contact` mints an
`McpContactTicket { transport, peer, discover? }` — the dial address, the peer
endpoint id, and a cached `server/discover` the bridge probed from the local
server. `idfon peer add --mcp-ticket <json>` adds the peer offline. The cached
`discover` is a hint only: refresh it with a live `server/discover` (respect
`ttlMs`/`cacheScope`), and treat `serverInfo` as unverified display data.

### Media (resources/blobs)

Content-addressed blobs stored daemon-side; the ticket is a bearer
capability valid immediately after `put` completes (no peer setup needed to
fetch).

| method | params | result |
|---|---|---|
| `blob.put` | `{ resource_id, bytes: [..], append, finish }` | `{ identity, resource_id, size_bytes, content_hash, blob_ticket }` — chunks stage in `data_dir/staging/`, the ticket is the artifact |
| `blob.get` | `{ resource_id, blob_ticket, offset?, length? }` | `{ bytes: [..], total_size }` — chunked fetch; the ticket is required |
| `live.publish` | `{ file, loop?, relay?, name? }` | `{ ticket, id }` — broadcast; ticket is a bearer capability |
| `live.dial` | `{ to, file, relay?, seconds? }` | 1:1 session; blocks until the callee hangs up; no ticket (session-scoped) |
| `live.answer` / `live.subscribe` | — | callee/subscription side |
| `live.list` | — | `{ ids: [..], publishers: [{ id, kind, ticket, file }] }` |
| `live.stop` | `{ id }` | graceful stop |
| `session.list` / `session.start` / `session.stop` | — | session registry |

### Durable files

Pins a file on the edge's always-on store (`docs/durable-files.md`). The
content-addressed URL is the artifact; the free tier is `blob.put`
(a connected-only `BlobTicket`) instead.

| method | params | result |
|---|---|---|
| `file.publish` | `{ path, name?, url?, token?, expires_in?, permanent? }` | `{ url, sha256, size, existed, expires_in, permanent }` — computes SHA-256, skips the upload when the edge already has the object, streams the file, and optionally writes the same bytes under a mutable `name`. User pins default to and are capped at 24h (`expires_in` can shorten); the edge answers `410` after the term. `permanent` requires `IDFON_FILES_OPS_TOKEN` and never expires |

`url` defaults to `IDFON_EDGE_URL`; `token` defaults to `IDFON_FILES_TOKEN`,
then `IDFON_EDGE_TOKEN`. The edge verifies the body against the hash, so the
URL is also the integrity check.

### Policy

| method | params | result |
|---|---|---|
| `policy.set` / `policy.dry_run` | policy document | capability-policy evaluation |

## Versioning discipline

1. Adding an optional param or result field: no version bump; shells must
   ignore unknown fields.
2. Removing/renaming a field, changing a type or semantic: bump
   `PROTOCOL_VERSION` and update every frontend.
3. Every change to this document should come with a `test-cli.sh` (or
   e2e-script) assertion covering the changed surface.
