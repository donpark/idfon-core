# Identity: one you, with tags

**Decided 2026-11-24.** The daemon has one identity (`default`): one account
key, one endpoint, one card. What used to be separate identities (Work /
Personal / Private, D4/D20) are **tags on contacts**. A tag doesn't make a
separate you. It decides what the contacts who carry it can see of you.

## Tags

A contact has zero or more tags (`Peer.tags`): Work, Family, anything. The
apps show them as chips on the person screen and as list filters.

- `peer.add { …, tags? }` sets them on a new contact.
- `peer.update { ref, tags? | add_tag? | remove_tag? }` changes them: `tags`
  replaces the set; the other two change one. Tags compare case-insensitively.
- `tag.list` → `{ tags: [{ name, hide_presence, hide_photo, count }] }`: every
  tag in use or with a rule. `count` is the number of contacts carrying it.
- `tag.set { name, hide_presence?, hide_photo? }` creates a tag or changes its
  rule. `tag.rename { name, to }` and `tag.delete { name }` apply to the rule and
  to every contact.

## What a contact can see

Two aspects so far: **presence** ("Online", never "last seen"; D-4) and your
**profile photo**. For one contact and one aspect, the first rule that applies
wins:

1. The contact's own override: `peer.update { ref, visibility: { presence?,
   photo? } }`. A `null` field clears that override; `visibility: null` clears
   them all.
2. Any of the contact's tags hides it (`hide_presence` / `hide_photo`).
3. Your default: `visibility.set { presence?, photo? }`. `visibility.get` reads
   it. Both aspects are shown unless turned off.

`visibility.audience { aspect }` → `{ peers: [id] }` lists who may see an
aspect. The apps send presence and the photo only to that list, leaving out
blocked contacts. They are app-to-app envelopes, so the sending app is where
the rule is enforced.

## Adding someone one way

Adding a contact from their card or code works without them adding you back.
Your first message reaches them as a **contact request** (held, not shown as a
chat), and they accept or decline it. Accepting adds you as their contact and
delivers what you wrote. See protocol.md, "Contact requests".

## Introductions

You can share a contact's card with someone else as an introduction: the bare
card plus a token signed by you, naming who introduced whom to whom
(`contact.introduce`; the token, where it rides and how it is checked are in
protocol.md, "Introductions"; the notices in chat-envelopes.md). The person
introduced sees "introduced by Don" on your recipient's contact request.

- **Ask Before Sharing My Card** is `identity.update { share: "ask" }`. Your
  card carries it and `IDFON-SHARE/1` tells your contacts. It is advisory: a
  sharer's app asks first, and nothing can enforce it.
- **An introduction grants, adds or accepts nothing.** `peer.add` and
  `request.accept` stay a person's decision; a held request with an
  introduction is still only a request.
- **Not transitive.** A token names one introducer, one subject and at most
  one recipient, and is never forwarded or chained. Passing a card on is a new
  introduction, by someone who has the contact.
- **Only people you know count.** Wherever a receiver's own rules use
  introductions (ring rules, request sorting, a future auto-accept), only
  introducers who are your contacts and people (not agents, not unknown ids)
  count, and you can mute an introducer. The daemon reports
  `introducer_is_contact`; the person/agent check and muting are the app's.
- **Bounded and quiet.** Requests stay bounded (64 senders, 20 messages). At
  most 20 notices a day from one introducer are shown; more collapse into one
  line. Agents drop notices and preferences, so two agents cannot loop on
  them.

## Identity methods

- `identity.list` lists the one identity. `identity.rename { name }` changes
  your name, which your card shows. `identity.update { share }` sets Ask
  Before Sharing My Card (`"ask"` | `"ok"`).
- `identity.create` and `identity.delete` are refused. `identity.use` accepts
  only `default`.

## Migration

On load, a store from before this change collapses once
(`single_identity_v1`):

- **Contacts:** each non-default identity's contacts move to `default`, tagged
  with that identity's name. A contact under both is merged. A name clash gets
  the tag as a suffix ("Bob (Private)").
- **Everything else:** its grants, rooms, events, operations, policies,
  sessions and MCP records move to `default`, and each old identity becomes a
  tag rule with nothing hidden.
- **Keys:** the old identity's key files stay on disk but no longer bind. A
  contact who knew you only through that identity reaches an endpoint that's
  gone, and must add you again. Stores no longer get the Work / Personal /
  Private presets.
