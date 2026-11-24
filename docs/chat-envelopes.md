# Chat envelopes

The small messages the apps exchange around a conversation: typing, read
receipts, reactions, edits, and group invites. Each one is a whole message by
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
- A receiver accepts a new group only from an unblocked contact, and a change
  only from a current member.
- A member the receiver doesn't know is added as a peer with message grants
  scoped to that room (`access.grant` with `conversation`), and is not shown as
  a contact.
- `kind` is `group`, or `project` when the group has an icon or repository.
