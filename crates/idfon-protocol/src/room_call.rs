//! Group-call signaling on a room's channel (idfon-home docs/group-live-chat.md, G1).
//!
//! A group call is a live session inside a room, at most one per room, named
//! by `call_id`. Members announce themselves with `IDFON-LIVE/1` controls that
//! carry a `room=` line:
//!
//! ```text
//! IDFON-LIVE/1
//! action=join            (join | leave | full)
//! room=<room id>
//! call_id=<session id>
//! inc=<n>                (the sender's incarnation: new after a restart)
//! seq=<n>                (per sender and incarnation, increasing: stale and
//!                         duplicate controls are dropped)
//! since=<ms>             (when the sender joined: the cap's order)
//! ticket=<broadcast ticket>
//! addr=<base64 of the dial address JSON>
//! audio_codec=liquid
//! reply=1                (a roster reply or refresh, which is answered only by
//!                         members who did not know the sender)
//! auto_close=1           (the session's auto-close, set by whoever started it)
//! ```
//!
//! The rules, each checked by the simulator (`tests/room_call_sim.rs`) under
//! latency, reordering, loss, duplicates, partitions and crashes:
//!
//! - **Join** goes to the room. Every joined member answers the joiner
//!   directly with its own join (`reply=1`), so a late joiner learns the
//!   roster without a roster service.
//! - **Leave** goes to the room. Losing a member's media transport counts as
//!   one locally.
//! - **Order.** Each sender's controls carry an increasing `seq`; a receiver
//!   drops anything not newer than the last it saw from that sender, so a
//!   join delayed behind its own leave cannot resurrect the member. `seq`
//!   compares only within one incarnation (`inc`): a member that restarts,
//!   even with its clock set back, starts a new one and is heard at once.
//! - **Refresh and expiry.** A joined member re-sends its join every
//!   [`REFRESH_MS`] as a reply; whoever did not know it answers. Members not
//!   heard for [`EXPIRE_MS`] are dropped. Together they heal lost joins and
//!   leaves, partitions, and crashes nobody noticed.
//! - **Glare.** Sessions started at once resolve to the lexically smallest
//!   `call_id`; members of the other one re-join under it.
//! - **Cap: 12 people, by join time.** Every member orders the call by
//!   (`since`, peer id), the same everywhere, and only the first 12 are in.
//!   One ranked out withdraws and is told why (`full`, also sent as advice).
//! - **Auto-close** is a room setting: on, the session ends when the last
//!   member leaves; off, it stays open empty and a later join re-enters the
//!   same `call_id`. Room settings are not shared between members yet (that
//!   needs room admins, docs/room-admins.md), so the member who starts a
//!   session stamps its setting on the session and every join carries it.
//!
//! [`RoomCall`] is the per-room state machine. It does no I/O and reads no
//! clock: the caller passes the time, checks that a control's sender belongs
//! to the room, feeds it in, calls [`RoomCall::tick`] about once a second, and
//! carries out the returned [`Effect`]s.

use std::collections::BTreeMap;

use base64::Engine as _;
use serde::{Deserialize, Serialize};

pub const LIVE_PREFIX: &str = "IDFON-LIVE/1\n";

/// People a call holds at most, including this member.
pub const MAX_CALL_MEMBERS: usize = 12;
/// How often a joined member re-announces itself.
pub const REFRESH_MS: u64 = 10_000;
/// How long a member may go unheard before it is dropped.
pub const EXPIRE_MS: u64 = 35_000;
/// How long a sender's last `seq` is kept after it is gone.
const SEEN_KEEP_MS: u64 = 10 * 60_000;

/// What a room-call control says.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoomCallAction {
    Join,
    Leave,
    Full,
}

/// One `IDFON-LIVE/1` control with a `room=` line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomCallControl {
    pub action: RoomCallAction,
    pub room: String,
    pub call_id: String,
    /// The sender's incarnation (0: none).
    pub inc: u64,
    /// The sender's sequence number within its incarnation (0: none, accepted always).
    pub seq: u64,
    /// When the sender joined, in ms (`join` only).
    pub since: Option<u64>,
    /// The sender's broadcast ticket (`join` only).
    pub ticket: Option<String>,
    /// The sender's dial address JSON (`join` only), decoded from base64.
    pub addr: Option<String>,
    pub audio_codec: Option<String>,
    /// A roster reply or refresh: answered only by members who did not know the sender.
    pub reply: bool,
    /// The session's auto-close (`join` only).
    pub auto_close: Option<bool>,
}

impl RoomCallControl {
    /// A control with only its action, room, session and sequence number.
    pub fn bare(action: RoomCallAction, room: &str, call_id: &str, seq: u64) -> Self {
        Self {
            action,
            room: room.into(),
            call_id: call_id.into(),
            inc: 0,
            seq,
            since: None,
            ticket: None,
            addr: None,
            audio_codec: None,
            reply: false,
            auto_close: None,
        }
    }

    /// The wire text.
    pub fn encode(&self) -> String {
        let action = match self.action {
            RoomCallAction::Join => "join",
            RoomCallAction::Leave => "leave",
            RoomCallAction::Full => "full",
        };
        let mut text = format!("{LIVE_PREFIX}action={action}\nroom={}\ncall_id={}", self.room, self.call_id);
        if self.inc != 0 {
            text.push_str(&format!("\ninc={}", self.inc));
        }
        if self.seq != 0 {
            text.push_str(&format!("\nseq={}", self.seq));
        }
        if let Some(since) = self.since {
            text.push_str(&format!("\nsince={since}"));
        }
        if let Some(ticket) = &self.ticket {
            text.push_str(&format!("\nticket={ticket}"));
        }
        if let Some(addr) = self.addr.as_ref().filter(|addr| !addr.is_empty()) {
            let addr = base64::engine::general_purpose::STANDARD.encode(addr);
            text.push_str(&format!("\naddr={addr}"));
        }
        if let Some(codec) = &self.audio_codec {
            text.push_str(&format!("\naudio_codec={codec}"));
        }
        if self.reply {
            text.push_str("\nreply=1");
        }
        if let Some(auto_close) = self.auto_close {
            text.push_str(if auto_close { "\nauto_close=1" } else { "\nauto_close=0" });
        }
        text
    }

    /// Parse a room-call control. `None` for anything else, including 1:1
    /// `IDFON-LIVE/1` controls (no `room=` line).
    pub fn decode(text: &str) -> Option<Self> {
        let body = text.strip_prefix(LIVE_PREFIX)?;
        let mut fields = BTreeMap::new();
        for line in body.lines() {
            if let Some((key, value)) = line.split_once('=') {
                fields.insert(key.trim(), value.trim());
            }
        }
        let action = match *fields.get("action")? {
            "join" => RoomCallAction::Join,
            "leave" => RoomCallAction::Leave,
            "full" => RoomCallAction::Full,
            _ => return None,
        };
        let room = fields.get("room").filter(|room| !room.is_empty())?.to_string();
        let call_id = fields.get("call_id").filter(|id| !id.is_empty())?.to_string();
        let ticket = fields.get("ticket").filter(|t| !t.is_empty()).map(|t| t.to_string());
        let addr = fields.get("addr").and_then(|addr| {
            let bytes = base64::engine::general_purpose::STANDARD.decode(addr).ok()?;
            String::from_utf8(bytes).ok()
        });
        if action == RoomCallAction::Join && ticket.is_none() {
            return None;
        }
        Some(Self {
            action,
            room,
            call_id,
            inc: fields.get("inc").and_then(|v| v.parse().ok()).unwrap_or(0),
            seq: fields.get("seq").and_then(|v| v.parse().ok()).unwrap_or(0),
            since: fields.get("since").and_then(|v| v.parse().ok()),
            ticket,
            addr,
            audio_codec: fields.get("audio_codec").map(|c| c.to_string()),
            reply: fields.get("reply").is_some_and(|r| *r == "1"),
            auto_close: fields.get("auto_close").map(|v| *v == "1"),
        })
    }
}

/// Whether `text` is a room-call control (cheap check before decoding).
pub fn is_room_call_control(text: &str) -> bool {
    text.starts_with(LIVE_PREFIX) && text.lines().any(|line| line.starts_with("room="))
}

/// A remote member of the call: what subscribing to it needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallMember {
    pub peer: String,
    pub ticket: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audio_codec: Option<String>,
    /// When it joined, in ms: its place in the cap's order.
    #[serde(default)]
    pub since: u64,
}

/// This member's own media, given when it joins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalJoin {
    pub ticket: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audio_codec: Option<String>,
}

/// What the caller must do after feeding the state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Send this text to the whole room.
    SendRoom(String),
    /// Send this text to one peer directly.
    SendDirect { peer: String, text: String },
    /// Start playing this member (this side is in the call).
    Subscribe(CallMember),
    /// Stop playing this member.
    Unsubscribe(String),
    /// The session ended (auto-close, last member gone).
    Ended,
}

/// Why this side is not in the call although it asked to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Refusal {
    /// The call already had 12 people who joined earlier.
    Full,
}

/// The last control seen from a sender.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Seen {
    inc: u64,
    seq: u64,
    at_ms: u64,
}

/// One room's call session, as this member sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomCall {
    pub room: String,
    /// This member's peer id, as others see it: the cap's tie-break.
    #[serde(default)]
    pub me: String,
    /// The live session, or `None` when no call runs in the room.
    pub call_id: Option<String>,
    /// This member's media while it is in the call.
    pub joined: Option<LocalJoin>,
    /// When this member joined, in ms.
    #[serde(default)]
    pub since: u64,
    /// Remote members in the call, by peer id. Tracked even while this side is
    /// not in the call, so the app can show "call in progress (3)".
    pub members: BTreeMap<String, CallMember>,
    /// The current session's auto-close: the starter's room setting.
    pub auto_close: bool,
    /// This member's room setting, used when it starts a session.
    pub room_auto_close: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refused: Option<Refusal>,
    /// Refused for a full call but still wanting in: joins by itself when a
    /// seat opens (until this side leaves).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting: Option<LocalJoin>,
    #[serde(default)]
    seen: BTreeMap<String, Seen>,
    #[serde(default)]
    next_seq: u64,
    /// This instance's incarnation: the clock at its first control (0 until then).
    #[serde(default)]
    inc: u64,
    #[serde(default)]
    last_refresh_ms: u64,
    #[serde(default, skip)]
    last_withdrawn: Option<LocalJoin>,
}

impl RoomCall {
    pub fn new(room: impl Into<String>, me: impl Into<String>, auto_close: bool) -> Self {
        Self {
            room: room.into(),
            me: me.into(),
            call_id: None,
            joined: None,
            since: 0,
            members: BTreeMap::new(),
            auto_close,
            room_auto_close: auto_close,
            refused: None,
            waiting: None,
            seen: BTreeMap::new(),
            next_seq: 0,
            inc: 0,
            last_refresh_ms: 0,
            last_withdrawn: None,
        }
    }

    /// People in the call, this side included when it is in.
    pub fn people(&self) -> usize {
        self.members.len() + usize::from(self.joined.is_some())
    }

    /// The next sequence number: increasing, and past anything this sender
    /// used before a restart (it starts from the clock).
    fn stamp(&mut self, now_ms: u64) -> u64 {
        if self.inc == 0 {
            // Mixed with the peer id so two members starting in the same ms differ.
            let salt = self.me.bytes().fold(0u64, |h, b| h.wrapping_mul(31).wrapping_add(u64::from(b)));
            self.inc = (now_ms ^ salt.rotate_left(17)).max(1);
        }
        self.next_seq = (self.next_seq + 1).max(now_ms.saturating_mul(1_000));
        self.next_seq
    }

    fn join_control(&mut self, reply: bool, now_ms: u64) -> Option<RoomCallControl> {
        let joined = self.joined.clone()?;
        let call_id = self.call_id.clone()?;
        let seq = self.stamp(now_ms);
        Some(RoomCallControl {
            inc: self.inc,
            seq,
            since: Some(self.since),
            ticket: Some(joined.ticket),
            addr: joined.addr,
            audio_codec: joined.audio_codec,
            reply,
            auto_close: Some(self.auto_close),
            ..RoomCallControl::bare(RoomCallAction::Join, &self.room, &call_id, 0)
        })
    }

    fn control(&mut self, action: RoomCallAction, call_id: &str, now_ms: u64) -> RoomCallControl {
        let seq = self.stamp(now_ms);
        RoomCallControl { inc: self.inc, ..RoomCallControl::bare(action, &self.room, call_id, seq) }
    }

    /// This side joins at `now_ms`, starting a session with `new_call_id` if
    /// none runs.
    pub fn join(&mut self, local: LocalJoin, now_ms: u64, new_call_id: impl FnOnce() -> String) -> Vec<Effect> {
        if self.joined.is_some() {
            return Vec::new();
        }
        // Everyone known joined earlier: with 12 already in, there is no seat.
        // Wait for one.
        if self.members.len() >= MAX_CALL_MEMBERS {
            self.refused = Some(Refusal::Full);
            self.waiting = Some(local);
            return Vec::new();
        }
        self.refused = None;
        self.waiting = None;
        if self.call_id.is_none() {
            self.call_id = Some(new_call_id());
            self.auto_close = self.room_auto_close;
        }
        self.joined = Some(local);
        self.since = now_ms;
        self.last_refresh_ms = now_ms;
        let mut effects: Vec<Effect> = self.members.values().cloned().map(Effect::Subscribe).collect();
        if let Some(control) = self.join_control(false, now_ms) {
            effects.push(Effect::SendRoom(control.encode()));
        }
        effects
    }

    /// This side leaves. The session goes on for the others.
    pub fn leave(&mut self, now_ms: u64) -> Vec<Effect> {
        self.waiting = None;
        let mut effects = self.withdraw(now_ms);
        effects.extend(self.end_if_empty());
        effects
    }

    /// Leave without ending the session: the roster stays known. What this
    /// side had joined with is kept in `last_withdrawn` (for waiting).
    fn withdraw(&mut self, now_ms: u64) -> Vec<Effect> {
        let Some(local) = self.joined.take() else {
            return Vec::new();
        };
        self.last_withdrawn = Some(local);
        let mut effects: Vec<Effect> = self.members.keys().cloned().map(Effect::Unsubscribe).collect();
        if let Some(call_id) = self.call_id.clone() {
            effects.push(Effect::SendRoom(self.control(RoomCallAction::Leave, &call_id, now_ms).encode()));
        }
        effects
    }

    /// A member's media transport is gone (every consumer of that peer
    /// closed): drop it here. If it is still alive, its next refresh brings it back.
    pub fn peer_lost(&mut self, peer: &str, _now_ms: u64) -> Vec<Effect> {
        let mut effects = self.remove(peer);
        effects.extend(self.end_if_empty());
        effects
    }

    fn remove(&mut self, peer: &str) -> Vec<Effect> {
        if self.members.remove(peer).is_some() && self.joined.is_some() {
            vec![Effect::Unsubscribe(peer.to_string())]
        } else {
            Vec::new()
        }
    }

    /// Time passes: refresh this member's join, expire members not heard from.
    /// Call about once a second.
    pub fn tick(&mut self, now_ms: u64) -> Vec<Effect> {
        let mut effects = Vec::new();
        let stale: Vec<String> = self
            .members
            .keys()
            .filter(|peer| {
                self.seen
                    .get(*peer)
                    .is_none_or(|seen| now_ms.saturating_sub(seen.at_ms) > EXPIRE_MS)
            })
            .cloned()
            .collect();
        for peer in stale {
            effects.extend(self.remove(&peer));
        }
        let members = &self.members;
        self.seen
            .retain(|peer, seen| members.contains_key(peer) || now_ms.saturating_sub(seen.at_ms) < SEEN_KEEP_MS);
        // A seat opened for a refused member still waiting: join now (ranked
        // by this new join time).
        if self.joined.is_none() && self.members.len() < MAX_CALL_MEMBERS {
            if let Some(local) = self.waiting.take() {
                let call_id = self.call_id.clone();
                let me = self.me.clone();
                effects.extend(self.join(local, now_ms, move || call_id.unwrap_or_else(|| format!("call-{me}-{now_ms}"))));
            }
        }
        if self.joined.is_some() && now_ms.saturating_sub(self.last_refresh_ms) >= REFRESH_MS {
            self.last_refresh_ms = now_ms;
            if let Some(control) = self.join_control(true, now_ms) {
                effects.push(Effect::SendRoom(control.encode()));
            }
        }
        effects.extend(self.end_if_empty());
        effects
    }

    /// A control from `sender` at `now_ms`. The caller has already checked
    /// that `sender` belongs to this room; anything for another room, and
    /// anything not newer than the last control from `sender`, is ignored.
    pub fn receive(&mut self, sender: &str, control: &RoomCallControl, now_ms: u64) -> Vec<Effect> {
        if control.room != self.room {
            return Vec::new();
        }
        if control.seq != 0 {
            let stale = self
                .seen
                .get(sender)
                .is_some_and(|seen| seen.inc == control.inc && seen.seq >= control.seq);
            if stale {
                return Vec::new();
            }
            self.seen.insert(sender.to_string(), Seen { inc: control.inc, seq: control.seq, at_ms: now_ms });
        } else {
            let (inc, seq) = self.seen.get(sender).map_or((0, 0), |seen| (seen.inc, seen.seq));
            self.seen.insert(sender.to_string(), Seen { inc, seq, at_ms: now_ms });
        }
        match control.action {
            RoomCallAction::Join => self.receive_join(sender, control, now_ms),
            RoomCallAction::Leave => {
                let mut effects = self.remove(sender);
                effects.extend(self.end_if_empty());
                effects
            }
            RoomCallAction::Full => {
                // Advice: re-rank with what is known; withdraw only if ranked out.
                if self.joined.is_none() || self.call_id.as_deref() != Some(control.call_id.as_str()) {
                    return Vec::new();
                }
                self.enforce_cap(now_ms)
            }
        }
    }

    fn receive_join(&mut self, sender: &str, control: &RoomCallControl, now_ms: u64) -> Vec<Effect> {
        let mut effects = Vec::new();
        let adopt = |call: &mut Self| {
            call.call_id = Some(control.call_id.clone());
            if let Some(auto_close) = control.auto_close {
                call.auto_close = auto_close;
            }
        };
        match self.call_id.clone() {
            None => adopt(self),
            Some(mine) if mine == control.call_id => {}
            Some(mine) if control.call_id < mine => {
                // Glare, and theirs wins: move to it and re-announce.
                if self.joined.is_some() {
                    effects.extend(self.members.keys().cloned().map(Effect::Unsubscribe));
                }
                self.members.clear();
                adopt(self);
                if let Some(join) = self.join_control(false, now_ms) {
                    effects.push(Effect::SendRoom(join.encode()));
                }
            }
            Some(_) => {
                // Glare, and ours wins: tell the joiner, who will move to ours.
                if let Some(join) = self.join_control(true, now_ms) {
                    effects.push(Effect::SendDirect {
                        peer: sender.to_string(),
                        text: join.encode(),
                    });
                }
                return effects;
            }
        }
        let member = CallMember {
            peer: sender.to_string(),
            ticket: control.ticket.clone().unwrap_or_default(),
            addr: control.addr.clone(),
            audio_codec: control.audio_codec.clone(),
            since: control.since.unwrap_or(now_ms),
        };
        let known = self.members.get(sender).cloned();
        let new = known.is_none();
        match known {
            Some(known) if known == member => {}
            Some(_) => {
                // Re-joined with new media: play the new broadcast.
                if self.joined.is_some() {
                    effects.push(Effect::Unsubscribe(sender.to_string()));
                    effects.push(Effect::Subscribe(member.clone()));
                }
                self.members.insert(sender.to_string(), member);
            }
            None => {
                if self.joined.is_some() {
                    effects.push(Effect::Subscribe(member.clone()));
                }
                self.members.insert(sender.to_string(), member);
            }
        }
        let cap = self.enforce_cap(now_ms);
        let still_member = self.members.contains_key(sender);
        effects.extend(cap);
        // Answer an announcement, or anyone who did not know us yet (a refresh
        // from a member we had missed): it learns us back.
        if still_member && (!control.reply || new) {
            if let Some(join) = self.join_control(true, now_ms) {
                effects.push(Effect::SendDirect {
                    peer: sender.to_string(),
                    text: join.encode(),
                });
            }
        }
        effects
    }

    /// Keep only the first 12 people by (join time, peer id). This side
    /// withdraws if ranked out; members ranked out are dropped and told.
    fn enforce_cap(&mut self, now_ms: u64) -> Vec<Effect> {
        let mut order: Vec<(u64, String)> =
            self.members.values().map(|member| (member.since, member.peer.clone())).collect();
        if self.joined.is_some() {
            order.push((self.since, self.me.clone()));
        }
        if order.len() <= MAX_CALL_MEMBERS {
            return Vec::new();
        }
        order.sort();
        let mut effects = Vec::new();
        let out: Vec<String> = order[MAX_CALL_MEMBERS..].iter().map(|(_, peer)| peer.clone()).collect();
        for peer in out {
            if peer == self.me {
                effects.extend(self.withdraw(now_ms));
                self.refused = Some(Refusal::Full);
                self.waiting = self.last_withdrawn.take();
                continue;
            }
            effects.extend(self.remove(&peer));
            if self.joined.is_some() {
                if let Some(call_id) = self.call_id.clone() {
                    let full = self.control(RoomCallAction::Full, &call_id, now_ms);
                    effects.push(Effect::SendDirect { peer, text: full.encode() });
                }
            }
        }
        effects
    }

    fn end_if_empty(&mut self) -> Vec<Effect> {
        if self.auto_close && self.joined.is_none() && self.members.is_empty() && self.call_id.is_some() {
            self.call_id = None;
            self.auto_close = self.room_auto_close;
            return vec![Effect::Ended];
        }
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controls_roundtrip_and_ignore_one_to_one_invites() {
        let control = RoomCallControl {
            action: RoomCallAction::Join,
            room: "r_abc".into(),
            call_id: "c1".into(),
            inc: 7,
            seq: 42,
            since: Some(1_000),
            ticket: Some("ilv1:x".into()),
            addr: Some("{\"id\":\"x\"}".into()),
            audio_codec: Some("liquid".into()),
            reply: true,
            auto_close: Some(false),
        };
        let text = control.encode();
        assert!(is_room_call_control(&text));
        assert_eq!(RoomCallControl::decode(&text), Some(control));
        let one_to_one = "IDFON-LIVE/1\naction=start\nticket=ilv1:x";
        assert!(!is_room_call_control(one_to_one));
        assert_eq!(RoomCallControl::decode(one_to_one), None);
        // A join without a ticket is malformed.
        assert_eq!(RoomCallControl::decode("IDFON-LIVE/1\naction=join\nroom=r\ncall_id=c"), None);
    }

    fn local(name: &str) -> LocalJoin {
        LocalJoin { ticket: format!("ilv1:{name}"), addr: None, audio_codec: None }
    }

    fn sent(effects: &[Effect]) -> RoomCallControl {
        effects
            .iter()
            .find_map(|effect| match effect {
                Effect::SendRoom(text) => RoomCallControl::decode(text),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no room send in {effects:?}"))
    }

    #[test]
    fn a_join_delayed_behind_its_leave_is_ignored() {
        let mut b = RoomCall::new("r", "me", true);
        let mut a = RoomCall::new("r", "me", true);
        let join = sent(&a.join(local("a"), 1_000, || "c1".into()));
        let leave = sent(&a.leave(2_000));
        b.receive("a", &leave, 2_100);
        b.receive("a", &join, 2_200);
        assert!(b.members.is_empty(), "the stale join stays dead");
    }

    #[test]
    fn a_silent_member_expires_and_a_refresh_brings_it_back() {
        let mut b = RoomCall::new("r", "me", false);
        let mut a = RoomCall::new("r", "me", false);
        let join = sent(&a.join(local("a"), 0, || "c1".into()));
        b.receive("a", &join, 10);
        assert_eq!(b.members.len(), 1);
        b.tick(EXPIRE_MS + 100);
        assert!(b.members.is_empty(), "expired");
        let refresh = sent(&a.tick(EXPIRE_MS + 200));
        b.receive("a", &refresh, EXPIRE_MS + 300);
        assert_eq!(b.members.len(), 1, "back on its refresh");
    }

    #[test]
    fn the_cap_keeps_the_twelve_earliest() {
        let mut me = RoomCall::new("r", "me", true);
        me.join(local("me"), 5_000, || "c1".into());
        for i in 0..12u64 {
            let control = RoomCallControl {
                since: Some(if i < 11 { i } else { 9_000 }),
                ticket: Some(format!("ilv1:{i}")),
                ..RoomCallControl::bare(RoomCallAction::Join, "r", "c1", i + 1)
            };
            me.receive(&format!("p{i:02}"), &control, 6_000);
        }
        // 11 joined before me, one after: I am 12th, p11 is out.
        assert!(me.joined.is_some());
        assert_eq!(me.members.len(), 11);
        assert!(!me.members.contains_key("p11"));
        // An earlier joiner arrives late: now I am 13th and withdraw.
        let early = RoomCallControl {
            since: Some(100),
            ticket: Some("ilv1:early".into()),
            ..RoomCallControl::bare(RoomCallAction::Join, "r", "c1", 1)
        };
        me.receive("early", &early, 6_100);
        assert!(me.joined.is_none());
        assert_eq!(me.refused, Some(Refusal::Full));
    }
}
