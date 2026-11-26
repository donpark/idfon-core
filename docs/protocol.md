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
| `daemon.status` | — | `{ ready, daemon, protocol_version, identity: {id, name, active, public_key, endpoint_id}, ticket: [bytes], host, autostarted, clients, build: {image, stale} }` (ticket = endpoint addr ticket, JSON byte array; `host` = the executable's file name, `idfond` or the app hosting the daemon; `autostarted` = spawned by the `idfon` CLI (`IDFON_AUTOSTARTED=1`); `clients` = connected sockets, this one included; `build.stale` = the file holding the daemon code (`build.image`) changed on disk since start, so the CLI may restart an autostarted daemon that nobody else is using) |
| `daemon.shutdown` | — | `{ shutdown: true }` |

### Identity

| method | params | result |
|---|---|---|
One you: identities are tags on contacts now (2026-11-24, `docs/identity.md`).

| method | params | result |
|---|---|---|
| `identity.list` | — | `{ identities: [{id, name, active, public_key, endpoint_id, share?}] }` — one entry, `default`; `share: "ask"` when set (absent = ok) |
| `identity.rename` | `{ name }` | your name (what your card shows) |
| `identity.update` | `{ share }` | `{ identity, share }` — your sharing preference, `"ask"` (Ask Before Sharing My Card) or `"ok"`; your card carries it (see "Introductions") |
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
| `peer.list` | — | `{ peers: [Peer] }`; Peer = `{id, identity, name, aliases, endpoint_id, endpoint_addr, ring_score?, introduced_by?, share?, ...}` |
| `peer.add` | `{ ref, id, name, endpoint_id, endpoint_addr, aliases, ring_score?, mcp_ticket?, contact_ticket?, intro? }` | `{ peer }` |

`introduced_by` = `{ peer_id, name, issued, note?, mutual }`: who introduced
this contact (from a valid `intro` at `peer.add`, or the introduction on an
accepted contact request). `share`: `"ask"` when the contact asked to be asked
before their card is shared (from their card or `IDFON-SHARE/1`); absent = ok.
See "Introductions".

`mcp_ticket` (M3) is an MCP contact ticket; when present it fills any omitted
`id`/`endpoint_id`/`endpoint_addr`/`name` and caches the ticket's
`server/discover` result. See the MCP transport section.
| `peer.update` | same as add, plus `share?` (`"ask"` \| `"ok"`) | `{ peer }` |
| `peer.remove` | `{ ref }` | `{}` |
| `peer.resolve` | `{ ref }` | live channel info |
| `peer.show` / `peer.status` | `{ ref }` | `{ peer }` / connectivity |
| `peer.doorbell` | `{ peer, url? }` | `{ rung }` (devices the edge pushed to) or `{ rung: 0, skipped: "recent" }` — rings the contact's message doorbell (docs/chat-envelopes.md `IDFON-DOORBELL/1`): seals our name and account id to their `doorbell_key` and posts it to `<edge>/<peer>/_push/doorbell` with our own `message.send` statement. At most once per contact every ten minutes (reset when a message to them is delivered). Fails without their key or an edge (`IDFON_EDGE_URL`, or `url`) |

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
| `contact.import` | `{ text, identity? }` | `{ peers: [Peer] }`; upserts each idfon card (aborts on a card that fails validation; idempotent); a card whose `URL` add link carries an `intro` is added with it |

A card (`ContactTicket`) carries `share: "ask"` when its owner set Ask Before
Sharing My Card; absent = ok. Your own card carries your `identity.update`
preference; a peer's card carries the last preference you recorded for them.

#### Introductions

Sharing a contact's card with someone else, signed by you (idfon-home
`docs/introductions.md`). **An introduction grants, adds or accepts
nothing**: the card is the bare card `contact.export {ref}` builds (no
capabilities, tickets, admission codes or ring grants), and the token is
context, never a credential.

**The token** (JSON; the daemon signs and checks it, apps never do):

```json
{ "v": 1,
  "introducer": "<peer id>", "introducer_name": "Don",
  "subject": "<introduced peer id>", "recipient": "<peer id>" | null,
  "issued": 1795575000, "mutual": true, "note": "Stella does our taxes",
  "sig": "<hex ed25519>" }
```

- `sig` is the introducer's ed25519 signature (its peer id is the public
  key) over `idfon-intro/1\n` followed by the compact JSON (serde_json: no
  whitespace) of the other fields in exactly this order: `v`, `introducer`,
  `introducer_name`, `subject`, `recipient`, `issued`, then `mutual` only when
  true and `note` only when present and non-empty. `recipient` is always
  written, `null` when unbound. Example of the signed bytes:
  `idfon-intro/1\n{"v":1,"introducer":"<hex>","introducer_name":"Don","subject":"<hex>","recipient":"<hex>","issued":1795579241,"note":"Stella does our taxes"}`.
  Codec: `crates/idfon-protocol/src/intro.rs`; `idfon_core::sign_introduction`
  and `verify_introduction`.
- Invalid: `v` other than 1, `note` over 280 characters (Unicode scalar
  values), `issued` more than 5 minutes in the future, or a signature that
  does not verify. There is no expiry.
- `recipient` binds the token to the person it was given to; `null` is a
  share with no known recipient (share sheet, QR, file).
- Not transitive: one introducer, one subject, at most one recipient. The
  daemon never forwards or chains a token.

**Where it rides.**

1. The add link: `idfon://add/<base64url>` of `{ "contact": <card>, "intro":
   <token> }` (beside `capability`/`auth` in the same envelope,
   `contact-exchange.md`). The vCard `contact.introduce` returns has the card
   in `X-IDFON-TICKET` and this link in `URL`.
2. `MessageEnvelope.introduction`: the token as JSON text, outside the message
   signature like `trace` (it carries its own). After `peer.add` with a valid
   `intro`, the daemon attaches the token to every message it sends that
   contact (each `message.send`, including a room's direct fan-out; not room
   gossip broadcasts) until the first message from that contact arrives. That
   state is persisted (`pending_introductions`). Older receivers ignore the
   field.

**Valid for me.** A token counts when it verifies, its `subject` is the
expected person, and its `recipient` is `null` or the expected other party:

| where | `subject` | `recipient` must be |
|---|---|---|
| `peer.add { intro }`, `contact.inspect` | the card's owner | `null` or this identity |
| a held contact request | this identity | `null` or the sender |

A token that is not valid is ignored silently: `peer.add` still adds the
contact (without `introduced_by`), a held message is still held (without an
introduction).

| method | params | result |
|---|---|---|
| `contact.introduce` | `{ ref, to?, note?, mutual? }` | `{ link, vcard, intro, share }` — `ref`'s card with a token signed by this identity, bound to `to` (a contact ref, or a 64-hex peer id) when given; `intro` is the token object; `share` is the subject's preference (`"ask"` \| `"ok"`). Only your own contacts can be introduced; `ref` = `to` and a note over 280 characters are refused. Sends nothing: a both-ways introduction is two calls with `mutual: true`, and the app sends the cards and any notice. |
| `contact.inspect` | `{ link }` | `{ contact: { peer_id, name, share }, intro?: { valid, introducer, introducer_name, introducer_is_contact, subject, recipient, addressed_to_me, issued, mutual, note } }` — reads an add link (`idfon://add/…` or `https://idfon.net/go/add/…`; a bare card link has no `intro`). `valid` means it would count for `peer.add` here (above); `addressed_to_me` = `recipient` is this identity; `introducer_is_contact` = the introducer is one of your contacts; `note` is `null` when absent. `introducer_name` is the name the introducer gave itself: show your own name for them when they are a contact. |
| `peer.add` | existing, plus `intro?` (the token object, or its JSON text) | `{ peer }` — with `introduced_by` when `intro` is valid; the token is kept to attach (above). Re-adding without `intro` keeps an earlier `introduced_by`. |
| `peer.update` | existing, plus `share?` | records a contact's preference (from their `IDFON-SHARE/1`) |
| `identity.update` | `{ share }` | your own preference; your card carries it |

Examples (ids shortened):

```json
{"method":"contact.introduce","params":{"ref":"Stella","to":"Bob","note":"Stella does our taxes"}}
→ {"link":"idfon://add/<base64url>","vcard":"BEGIN:VCARD…","share":"ask",
   "intro":{"v":1,"introducer":"<don>","introducer_name":"Don","subject":"<stella>","recipient":"<bob>","issued":1795579241,"note":"Stella does our taxes","sig":"<hex>"}}

{"method":"contact.inspect","params":{"link":"idfon://add/<base64url>"}}
→ {"contact":{"peer_id":"<stella>","name":"Stella","share":"ask"},
   "intro":{"valid":true,"introducer":"<don>","introducer_name":"Don","introducer_is_contact":true,"subject":"<stella>","recipient":"<bob>","addressed_to_me":true,"issued":1795579241,"mutual":false,"note":"Stella does our taxes"}}

{"method":"peer.add","params":{"contact_ticket":{…,"share":"ask"},"intro":{…}}}
→ {"peer":{"id":"<stella>",…,"introduced_by":{"peer_id":"<don>","name":"Don","issued":1795579241,"note":"Stella does our taxes","mutual":false},"share":"ask"}}

{"method":"peer.update","params":{"ref":"Stella","share":"ask"}} → {"peer":{…,"share":"ask"}}
{"method":"identity.update","params":{"share":"ask"}} → {"identity":"default","share":"ask"}
```

The sharing preference and the notice to the person introduced are app
envelopes (`IDFON-SHARE/1`, `IDFON-INTRO/1` in `chat-envelopes.md`); the
daemon does not interpret them. The rules for automation (introductions never
add or accept anyone; only introducers who are your contacts and people
count in a receiver's own rules; agents drop notices and preferences) are in
`identity.md`, "Introductions".

### Messaging

| method | params | result |
|---|---|---|
| `message.send` | `{ to, text, idempotency_key, capability_ticket?, retries?, durable? }` (`capability_ticket` is the holder's rcan token string) | `{ authenticated, message_id, operation_id, status: "queued" }` — returns immediately; delivery proceeds in the daemon (retries per `retries`), confirm via `operation.wait`. Sender needs a `message.send` grant for the peer (own daemon); the receiver needs `message.receive` for the sender. A successful send auto-establishes the local receive side of the reply path. `durable: true` makes it an outbox item: if every attempt fails it goes back to `queued` ("not sent yet") instead of `failed`, and is retried later (see `operation.resume`). Chat messages set it; time-bound sends (call signaling) must not. |
| `message.receive` | — | receive-side helper used by the transport; not called by shells. A sender who isn't a peer is held as a contact request (see "Contact requests"). |

Outbound envelopes may carry `introduction` (an introduction token as JSON
text, unsigned like `trace`): see "Introductions".

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
`cur_<20 digits>` cursors, retained up to `EVENT_RETENTION`. Messages surface
as `message.received`; a message held from a sender who isn't a peer surfaces
as `contact.request` (see "Contact requests").

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

### Contact requests

A verified message from a sender who isn't a peer of the receiving identity is
**held as a contact request** instead of refused, the way "unknown senders"
work in phone messaging. This makes one-way adds work: someone who scanned your
code or got your card adds you and writes; you see a request. It is also how a
group join link reaches the group's owner (`chat-envelopes.md`, "Group links").

- The sender gets an ordinary success acknowledgment (`status: "held"` in the
  `message.receive` result), so their outbox marks the message delivered.
- Only **plain text** is held. A message that is an `IDFON-*/N` envelope (a
  file, call, receipt, card, typing…) is refused as before (`unauthorized`,
  "sender is not a persisted peer").
- One request per sender per receiving identity, persisted with the rest of
  the state. Later messages append to it.
- Bounds: at most 20 messages per sender and 8 KiB of text per message (past
  either, `capability_denied`); at most 64 pending senders per identity (a new
  one drops the oldest request).
- A sender blocked with `request.decline { block: true }` is refused with the
  same `unauthorized` error an unknown sender used to get, and makes no
  request. The block lasts until `request.unblock` or until they are added as
  a peer (`peer.add`, `request.accept`).
- Nothing is accepted automatically, introduced or not.
- **Introductions.** When a held message carries an introduction token
  (`MessageEnvelope.introduction`) that is valid for this identity — it
  verifies, its `subject` is this identity, and its `recipient` is `null` or
  the sender — the request records `introduction { introducer,
  introducer_name, introducer_is_contact, addressed, note?, issued, mutual }`.
  The first valid token wins; invalid ones are dropped silently.
  `addressed` = the token was bound to the sender (`false` for a share with no
  known recipient). `introducer_is_contact` is refreshed on every
  `request.list`. Accepting the request copies it to the new contact's
  `introduced_by`. See "Introductions".

Each held message emits a `contact.request` event with `data: { request_id,
peer_id, endpoint_id, conversation, text, idempotency_key, introduction? }`
(`introduction` is the request's, when it has one).

| method | params | result |
|---|---|---|
| `request.list` | — | `{ requests: [ContactRequest] }`, newest first. ContactRequest = `{ request_id, identity, peer_id, endpoint_id, endpoint_addr, received_at, messages: [{ message_id, text, conversation, idempotency_key, received_at }], introduction? }`; `endpoint_addr` is the sender's dial address (JSON text) as seen on the connection, or `null` |
| `request.accept` | `{ request_id, name?, conversation? }` | `{ peer }` — adds the sender as a peer (named `name`, else a short prefix of their id; dialed at `endpoint_addr`, else by id through discovery), grants `message.send` and `message.receive` for them (scoped to `conversation` when given, like a group member's grants), then emits each held message the grant covers as an ordinary `message.received` event and removes the request. With `conversation`, held messages outside it are dropped |
| `request.decline` | `{ request_id, block? }` | `{ request_id, peer_id, blocked }` — drops the request; `block: true` also refuses the sender's future messages |
| `request.unblock` | `{ peer_id }` | `{ peer_id, unblocked }` |

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
