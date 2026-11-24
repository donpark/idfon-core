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

## Identity methods

- `identity.list` lists the one identity. `identity.rename { name }` changes
  your name, which your card shows.
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
