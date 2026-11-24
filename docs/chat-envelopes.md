# Chat envelopes

The small messages the apps exchange around a conversation: typing, read
receipts, reactions, edits, presence, profile photos, doorbell keys, "free to
talk?" knocks, cards, and group invites. Each one is a whole message by
itself (never mixed with text), sent with `message.send`, or `room.send` in a
group. They are `IDFON-*/1` text envelopes like the others (`reply-blocks.md`):
a first line naming the envelope, then `key=value` lines. A client that does not
know one shows it as text, so an app that sends them must also read them.

## Naming a message

Reactions, edits and receipts point at an earlier message by its **key**: the
sender's `idempotency_key` up to the first `:`. The receiver finds it in the
`message.received` event (`data.idempotency_key`); the sender knows it because it
chose it. `message_id` can't be used for this: a room send creates one message
per member, each with its own id, while all of them carry the same base key
(`<key>:<member>`).

A key names a message within its conversation and author. Clients make keys
unique (the apps use a random UUID: `ios-<uuid>`, `mac-<uuid>`).

## Envelopes

### Typing — `IDFON-TYPING/1`

```
IDFON-TYPING/1
state=on
```

`state=on` while the person is writing (sent at most every few seconds), `off`
when they stop or clear the composer. Not stored, not durable, no retries. The
receiver shows "typing…" until `off`, a message from them arrives, or about
6 seconds pass. It ignores one that is more than 10 seconds old when it arrives.

### Read receipt — `IDFON-READ/1`

```
IDFON-READ/1
ref=<key>
```

"I have seen your messages up to and including `ref`." Sent in 1:1 chats only,
when the chat is on screen and its latest message from the other person
changes. The receiver marks that message and every earlier one it sent in the
chat as read. Read receipts are a person's choice: the apps have a setting to
stop sending them.

### Reaction — `IDFON-REACT/1`

```
IDFON-REACT/1
ref=<key>
emoji=👍
```

One reaction per person per message: a new one replaces theirs, and an empty
`emoji=` removes it. Durable (outbox).

### Edit — `IDFON-EDIT/1`

```
IDFON-EDIT/1
ref=<key>
text=<base64 of the new UTF-8 text>
```

Replaces the text of the sender's own earlier message (base64 so the new text
can never look like an envelope). The receiver applies it only when the
referenced message's author is the envelope's sender, and marks the message
"Edited". Durable (outbox).

### Presence — `IDFON-PRESENCE/1`

```
IDFON-PRESENCE/1
state=online
```

"Online" (never "last seen"). An app sends `online` when it comes to the
front and every couple of minutes while it stays there, and `offline` when it
leaves. It sends only to the contacts allowed to see it
(`visibility.audience { aspect: "presence" }`, see `identity.md`), with one
attempt and never queued. The receiver shows someone as online for a few
minutes after their last `online`, or after any message from them. A
contact who just came online may get an `online` back, so both see each other.

### Profile photo — `IDFON-PROFILE/1`

```
IDFON-PROFILE/1
photo=<base64 of a small square JPEG>
```

Your photo, sent to the contacts allowed to see it
(`visibility.audience { aspect: "photo" }`) when it changes, when they become
allowed, and once to new contacts. An empty `photo=` removes it: it's sent to
anyone who had it and no longer may. Durable (outbox).

### Doorbell key — `IDFON-DOORBELL/1`

```
IDFON-DOORBELL/1
key=<base64 of a 32-byte X25519 public key>
```

The key a contact uses to ring your message doorbell. There is no mailbox:
a message waits in its sender's outbox until you are reachable. While it waits,
the sender's app asks the edge to send you one alert push ("New message") so
your phone wakes up and comes online. The push carries only the sender's name and
account id, sealed to this key, so the edge and Apple see neither of them.
The phone's notification extension opens it to show who it is from.

- Sealing: HPKE (RFC 9180) with X25519, HKDF-SHA256 and ChaCha20-Poly1305,
  info `idfon-doorbell/1`. The sealed blob is the 32-byte encapsulated key
  followed by the ciphertext, base64. The plaintext is the JSON
  `{"n": "<name, at most 64 chars>", "i": "<account id>"}`.
- An app sends its key once to each contact, and again when the key changes.
  Durable (outbox). A receiver keeps the latest key per contact and ignores a
  `key` that is not a valid X25519 public key.
- A sender rings a contact's doorbell at most once every ten minutes while
  messages wait, and not at all without their key. The edge route and its rate
  limits are in idfon-edge (`/_push/doorbell`).
- The woken phone tells the sender it is here (`IDFON-PRESENCE/1`). The
  sender's outbox then delivers, and the real message's notification replaces
  the doorbell's.

### Knock — `IDFON-KNOCK/1`

```
IDFON-KNOCK/1
id=<unique id>
answer=now
```

"Free to talk?": a one-tap question before a call. Without `answer` it is the
question. The answer repeats the question's `id`, with one of these values:

- `now`: call me now.
- `soon`: in about ten minutes. The asker's app reminds them then.
- `text`: I'd rather text.

Durable (outbox). A receiver ignores an `answer` it does not know. The apps show
the question as a card with the three answers (and as a notification whose
actions answer it), and the answer as a card. A `now` answer has a Call action.

### Card — `IDFON-UI/1`

```
IDFON-UI/1
payload=<base64 of JSON>
```

A small piece of interactive UI sent by a peer or an agent. It shows in the
conversation as a row with a title and up to a few actions.

```json
{
  "kind": "poll",
  "id": "<unique card id>",
  "title": "Lunch?",
  "subtitle": "Pick one",
  "glyph": "fork.knife",
  "actions": [{"id": "pizza", "title": "Pizza"}, {"id": "no", "title": "Not today", "role": "destructive"}],
  "detailURL": "https://…",
  "data": {"any": "string pairs"},
  "fields": [{"id": "name", "label": "Name", "placeholder": "Your name", "value": ""}],
  "slot": "sheet"
}
```

- `kind` and `id` are required. Everything else is optional.
- `glyph` is an SF Symbol name.
- `role` is `destructive` or absent.
- `detailURL` is an http(s) page the card opens full screen.
- `data` holds kind-specific strings.
- `fields` makes the card a form. The apps open it in a sheet, and what the
  person types goes back with the action they pick (`values=` below). `value`
  is filled in to start with.
- `slot: "sheet"` is for calls: while the receiver is in a call with the sender
  and lets them show things during calls, the card opens over the screen by
  itself (the call companion). Otherwise it waits in the conversation like any
  card.
- A card sent again with the same `kind` and `id` replaces the earlier one, so a
  sender can change a card in place (a companion that moves to its next step).
- A client draws the kinds it knows with their own renderer. Every other kind
  still shows, as the generic card: title, subtitle, glyph and actions. A client
  that cannot act on cards may show only the title.

### Card answer — `IDFON-UI-ACTION/1`

```
IDFON-UI-ACTION/1
card=<card id>
action=<action id>
values=<base64 of a JSON object>
```

The action a person chose on a card. It is sent back to whoever sent the card,
and it is never shown as a message. Durable (outbox).

- `values` is present only when the card had fields and something was typed:
  `{"<field id>": "<text>"}`.
- `card=compose` is a request with no card before it: the person picked one of
  the sender's advertised actions from the + menu (the `actions` list in a
  contact's `idfon.json`, see idfon-sdk `docs/voice-agent.md`). The contact
  answers with a card.

### Group invite — `IDFON-GROUP/1`

```
IDFON-GROUP/1
payload=<base64 of JSON>
```

The whole group as it now is, sent 1:1 to each member whenever a group is
created or changes (room membership is local to each daemon, see
`chatrooms.md`). Sent scoped to the room (`conversation` = the room id), so
members who are not each other's contacts can still be told. Durable.

```json
{
  "id": "<room id>",
  "name": "Weekend",
  "icon": "🍝",
  "repo": "<optional repository link>",
  "kind": "group",
  "members": [
    {"id": "<account id>", "name": "Ann", "endpointId": "<endpoint id>", "endpointAddr": "<address JSON>"}
  ]
}
```

- `members` lists everyone, the sender included. A sender who is leaving sends
  it without themselves; a member left out by someone else was removed.
- A receiver accepts a new group only from an unblocked contact (or from the
  person whose group link it opened, see "Group links"), and a change only
  from a current member.
- A member the receiver doesn't know is added as a peer with message grants
  scoped to that room (`access.grant` with `conversation`), and is not shown as
  a contact.
- `kind` is `group`, or `project` when the group has an icon or repository.

## Group links

A group link lets someone who isn't a contact yet ask to join a group. It is
an app convention on top of contact requests (`protocol.md`, "Contact
requests"); the daemon knows nothing about it.

### The link

```
https://idfon.net/go/join/<payload>
idfon://join/<payload>
```

`<payload>` is base64url (no padding) of:

```json
{"v": 1, "room": "<room id>", "name": "Weekend", "contact": "idfon://add/<card>", "token": "<hex>"}
```

- `contact` is the inviter's card link (`contact-exchange.md`), so the joiner
  can add them.
- `token` is 16 random bytes as hex. The inviter's app makes it the first time
  it shares a link to that group and keeps it; the token travels only inside
  the link and the join request below. Resetting the link makes a new one, and
  links with the old token stop working. It is the app's check, not a daemon
  capability.

### Joining

The joiner's app asks first, then:

1. Adds the inviter as a peer from `contact`, unless they already are one. An
   inviter known only this way stays out of the contact list, like a group
   member.
2. Sends the inviter one plain-text message whose **last line** is the join
   line:

   ```
   Ann would like to join “Weekend”.
   idfon-join: <room id> <token> <joiner's name>
   ```

   The first line is for people: an app that doesn't know group links shows
   it as an ordinary request. Only the last line counts. The name is the rest
   of that line after the token (newlines in it become spaces).
3. Remembers that it asked this inviter for this room, and accepts that one
   group invite (`IDFON-GROUP/1` for that room, from that inviter) even though
   the inviter isn't a regular contact. Any other group from them is still
   ignored.

**Why plain text.** The joiner isn't a peer of the inviter's identity yet. From
an unknown sender, the inviter's daemon holds only plain text, as a contact
request, and refuses every `IDFON-*` envelope. So the request can't be an
envelope; it is a sentence with a machine-readable last line.

### Letting them in

The inviter's app reads held requests (`request.list`, on a `contact.request`
event). It lets one in without asking only when a held text ends in a join line
whose token is that group's current token on this device. Then it:

- accepts the request scoped to the group: `request.accept { request_id, name:
  <joiner's name>, conversation: <room id> }`. The joiner gets message grants
  for that room only, like a member met through a group, and stays out of the
  contact list. Held messages outside the room are dropped, the join text
  included, so it never becomes a chat message.
- adds them to the room and sends the group to every member (`IDFON-GROUP/1`,
  above), which is how the joiner learns they're in.

A join line with a wrong or old token is an ordinary request, left for the
person to accept, delete or block. A join from someone who is already a
contact arrives as a normal `message.received` with no conversation; with a
valid token the app adds them the same way and doesn't show the text.
