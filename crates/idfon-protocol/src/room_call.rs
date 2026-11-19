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
//! ticket=<broadcast ticket>
//! addr=<base64 of the dial address JSON>
//! audio_codec=liquid
//! reply=1                (a direct roster reply, not an announcement)
//! auto_close=1           (the session's auto-close, set by whoever started it)
//! ```
//!
//! - A `join` goes to the room. Every joined member answers the joiner
//!   directly with its own `join` (`reply=1`), so a late joiner learns the
//!   roster without a roster service.
//! - A `leave` goes to the room. Losing a member's transport counts as one.
//! - Glare: two sessions started at once resolve to the lexically smallest
//!   `call_id`; members of the other one re-join under it.
//! - Cap: 12 people. A member that already sees 12 answers a newcomer with
//!   `full`, and the newcomer withdraws; the refusal is visible, not silent.
//!   Two people joining as the 12th and 13th at the same moment can be judged
//!   differently by different members; acceptable for v1.
//! - Auto-close is a room setting: with it on, the session ends when the last
//!   member leaves; with it off, the session stays open empty and a later
//!   join re-enters the same `call_id`. Room settings are not shared between
//!   members yet (that needs room admins, docs/room-admins.md), so the member
//!   who starts a session stamps its room's setting on the session and every
//!   join carries it; members follow the session's value.
//!
//! [`RoomCall`] is the per-room state machine. It does no I/O: the caller
//! checks that a control's sender belongs to the room, feeds it in, and
//! carries out the returned [`Effect`]s (send to the room or one peer,
//! subscribe or drop a member's media).

use std::collections::BTreeMap;

use base64::Engine as _;
use serde::{Deserialize, Serialize};

pub const LIVE_PREFIX: &str = "IDFON-LIVE/1\n";

/// People a call holds at most, including this member.
pub const MAX_CALL_MEMBERS: usize = 12;

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
    /// The sender's broadcast ticket (`join` only).
    pub ticket: Option<String>,
    /// The sender's dial address JSON (`join` only), decoded from base64.
    pub addr: Option<String>,
    pub audio_codec: Option<String>,
    /// A direct roster reply to a joiner, which must not be answered again.
    pub reply: bool,
    /// The session's auto-close (`join` only).
    pub auto_close: Option<bool>,
}

impl RoomCallControl {
    /// The wire text.
    pub fn encode(&self) -> String {
        let action = match self.action {
            RoomCallAction::Join => "join",
            RoomCallAction::Leave => "leave",
            RoomCallAction::Full => "full",
        };
        let mut text = format!("{LIVE_PREFIX}action={action}\nroom={}\ncall_id={}", self.room, self.call_id);
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
    /// The call already had 12 people.
    Full,
}

/// One room's call session, as this member sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomCall {
    pub room: String,
    /// The live session, or `None` when no call runs in the room.
    pub call_id: Option<String>,
    /// This member's media while it is in the call.
    pub joined: Option<LocalJoin>,
    /// Remote members, by peer id. Tracked even while this side is not in
    /// the call, so the app can show "call in progress (3)".
    pub members: BTreeMap<String, CallMember>,
    /// The current session's auto-close: the starter's room setting.
    pub auto_close: bool,
    /// This member's room setting, used when it starts a session.
    pub room_auto_close: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refused: Option<Refusal>,
}

impl RoomCall {
    pub fn new(room: impl Into<String>, auto_close: bool) -> Self {
        Self {
            room: room.into(),
            call_id: None,
            joined: None,
            members: BTreeMap::new(),
            auto_close,
            room_auto_close: auto_close,
            refused: None,
        }
    }

    /// People in the call, this side included when it is in.
    pub fn people(&self) -> usize {
        self.members.len() + usize::from(self.joined.is_some())
    }

    fn join_control(&self, reply: bool) -> Option<RoomCallControl> {
        let joined = self.joined.as_ref()?;
        Some(RoomCallControl {
            action: RoomCallAction::Join,
            room: self.room.clone(),
            call_id: self.call_id.clone()?,
            ticket: Some(joined.ticket.clone()),
            addr: joined.addr.clone(),
            audio_codec: joined.audio_codec.clone(),
            reply,
            auto_close: Some(self.auto_close),
        })
    }

    /// This side joins, starting a session with `new_call_id` if none runs.
    pub fn join(&mut self, local: LocalJoin, new_call_id: impl FnOnce() -> String) -> Vec<Effect> {
        if self.joined.is_some() {
            return Vec::new();
        }
        if self.members.len() >= MAX_CALL_MEMBERS {
            self.refused = Some(Refusal::Full);
            return Vec::new();
        }
        self.refused = None;
        if self.call_id.is_none() {
            self.call_id = Some(new_call_id());
            self.auto_close = self.room_auto_close;
        }
        self.joined = Some(local);
        let mut effects: Vec<Effect> = self.members.values().cloned().map(Effect::Subscribe).collect();
        if let Some(control) = self.join_control(false) {
            effects.push(Effect::SendRoom(control.encode()));
        }
        effects
    }

    /// This side leaves. The session goes on for the others.
    pub fn leave(&mut self) -> Vec<Effect> {
        let Some(_) = self.joined.take() else {
            return Vec::new();
        };
        let mut effects: Vec<Effect> = self.members.keys().cloned().map(Effect::Unsubscribe).collect();
        if let Some(call_id) = self.call_id.clone() {
            effects.push(Effect::SendRoom(
                RoomCallControl {
                    action: RoomCallAction::Leave,
                    room: self.room.clone(),
                    call_id,
                    ticket: None,
                    addr: None,
                    audio_codec: None,
                    reply: false,
                    auto_close: None,
                }
                .encode(),
            ));
        }
        effects.extend(self.end_if_empty());
        effects
    }

    /// A member's transport is gone (every consumer of that peer closed): a leave.
    pub fn peer_lost(&mut self, peer: &str) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.members.remove(peer).is_some() && self.joined.is_some() {
            effects.push(Effect::Unsubscribe(peer.to_string()));
        }
        effects.extend(self.end_if_empty());
        effects
    }

    /// A control from `sender`. The caller has already checked that `sender`
    /// belongs to this room; anything for another room is ignored.
    pub fn receive(&mut self, sender: &str, control: &RoomCallControl) -> Vec<Effect> {
        if control.room != self.room {
            return Vec::new();
        }
        match control.action {
            RoomCallAction::Join => self.receive_join(sender, control),
            RoomCallAction::Leave => {
                if self.call_id.as_deref() == Some(control.call_id.as_str()) {
                    self.peer_lost(sender)
                } else {
                    Vec::new()
                }
            }
            RoomCallAction::Full => {
                if self.joined.is_none() || self.call_id.as_deref() != Some(control.call_id.as_str()) {
                    return Vec::new();
                }
                let mut effects = self.leave();
                self.refused = Some(Refusal::Full);
                // The roster stays known; the session is not ours to end.
                effects.retain(|effect| *effect != Effect::Ended);
                effects
            }
        }
    }

    fn receive_join(&mut self, sender: &str, control: &RoomCallControl) -> Vec<Effect> {
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
                effects.extend(self.members.keys().filter(|_| self.joined.is_some()).cloned().map(Effect::Unsubscribe));
                self.members.clear();
                adopt(self);
                if let Some(join) = self.join_control(false) {
                    effects.push(Effect::SendRoom(join.encode()));
                }
            }
            Some(_) => {
                // Glare, and ours wins: tell the joiner, who will move to ours.
                if let Some(join) = self.join_control(true) {
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
        };
        match self.members.get(sender) {
            Some(known) if *known == member => {}
            Some(_) => {
                // Re-joined with new media: play the new broadcast.
                if self.joined.is_some() {
                    effects.push(Effect::Unsubscribe(sender.to_string()));
                    effects.push(Effect::Subscribe(member.clone()));
                }
                self.members.insert(sender.to_string(), member);
            }
            None => {
                if self.people() >= MAX_CALL_MEMBERS {
                    if self.joined.is_some() {
                        effects.push(Effect::SendDirect {
                            peer: sender.to_string(),
                            text: RoomCallControl {
                                action: RoomCallAction::Full,
                                room: self.room.clone(),
                                call_id: control.call_id.clone(),
                                ticket: None,
                                addr: None,
                                audio_codec: None,
                                reply: false,
                                auto_close: None,
                            }
                            .encode(),
                        });
                    }
                    return effects;
                }
                if self.joined.is_some() {
                    effects.push(Effect::Subscribe(member.clone()));
                }
                self.members.insert(sender.to_string(), member);
            }
        }
        if !control.reply {
            if let Some(join) = self.join_control(true) {
                effects.push(Effect::SendDirect {
                    peer: sender.to_string(),
                    text: join.encode(),
                });
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

    fn local(name: &str) -> LocalJoin {
        LocalJoin {
            ticket: format!("ilv1:{name}"),
            addr: Some(format!("{{\"id\":\"{name}\"}}")),
            audio_codec: Some("liquid".into()),
        }
    }

    /// A tiny in-memory room: every member's effects are delivered to the
    /// others, so the tests exercise the protocol end to end.
    struct Room {
        calls: BTreeMap<String, RoomCall>,
        playing: BTreeMap<String, Vec<String>>,
    }

    impl Room {
        fn new(members: &[&str], auto_close: bool) -> Self {
            Self {
                calls: members.iter().map(|m| (m.to_string(), RoomCall::new("r_test", auto_close))).collect(),
                playing: members.iter().map(|m| (m.to_string(), Vec::new())).collect(),
            }
        }

        fn run(&mut self, from: &str, effects: Vec<Effect>) {
            let mut queue: Vec<(String, Effect)> = effects.into_iter().map(|e| (from.to_string(), e)).collect();
            while !queue.is_empty() {
                let (who, effect) = queue.remove(0);
                match effect {
                    Effect::SendRoom(text) => {
                        let control = RoomCallControl::decode(&text).unwrap();
                        let others: Vec<String> = self.calls.keys().filter(|k| **k != who).cloned().collect();
                        for other in others {
                            let out = self.calls.get_mut(&other).unwrap().receive(&who, &control);
                            queue.extend(out.into_iter().map(|e| (other.clone(), e)));
                        }
                    }
                    Effect::SendDirect { peer, text } => {
                        let control = RoomCallControl::decode(&text).unwrap();
                        let out = self.calls.get_mut(&peer).unwrap().receive(&who, &control);
                        queue.extend(out.into_iter().map(|e| (peer.clone(), e)));
                    }
                    Effect::Subscribe(member) => self.playing.get_mut(&who).unwrap().push(member.peer),
                    Effect::Unsubscribe(peer) => self.playing.get_mut(&who).unwrap().retain(|p| *p != peer),
                    Effect::Ended => {}
                }
            }
        }

        fn join(&mut self, who: &str, call_id: &str) {
            let id = call_id.to_string();
            let effects = self.calls.get_mut(who).unwrap().join(local(who), || id);
            self.run(who, effects);
        }

        fn leave(&mut self, who: &str) {
            let effects = self.calls.get_mut(who).unwrap().leave();
            self.run(who, effects);
        }

        fn playing(&self, who: &str) -> Vec<String> {
            let mut p = self.playing[who].clone();
            p.sort();
            p
        }
    }

    #[test]
    fn controls_roundtrip_and_ignore_one_to_one_invites() {
        let control = RoomCallControl {
            action: RoomCallAction::Join,
            room: "r_abc".into(),
            call_id: "c1".into(),
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

    #[test]
    fn late_joiners_learn_the_roster_and_everyone_hears_everyone() {
        let mut room = Room::new(&["a", "b", "c", "d"], true);
        room.join("a", "c-a");
        room.join("b", "c-b");
        room.join("c", "c-c");
        for who in ["a", "b", "c"] {
            assert_eq!(room.calls[who].call_id.as_deref(), Some("c-a"), "{who} is in a's session");
        }
        assert_eq!(room.playing("a"), vec!["b", "c"]);
        assert_eq!(room.playing("b"), vec!["a", "c"]);
        assert_eq!(room.playing("c"), vec!["a", "b"]);
        // d is not in the call but sees it.
        assert_eq!(room.calls["d"].members.len(), 3);
        assert!(room.playing("d").is_empty());
    }

    #[test]
    fn a_leave_drops_the_member_and_auto_close_ends_the_empty_session() {
        let mut room = Room::new(&["a", "b", "c"], true);
        room.join("a", "c1");
        room.join("b", "c2");
        room.leave("b");
        assert_eq!(room.playing("a"), Vec::<String>::new());
        assert_eq!(room.calls["c"].members.len(), 1);
        room.leave("a");
        for who in ["a", "b", "c"] {
            assert_eq!(room.calls[who].call_id, None, "{who}: session closed");
        }
    }

    #[test]
    fn without_auto_close_an_empty_session_stays_open_and_is_re_entered() {
        let mut room = Room::new(&["a", "b"], false);
        room.join("a", "c1");
        room.leave("a");
        assert_eq!(room.calls["b"].call_id.as_deref(), Some("c1"));
        room.join("b", "c-new");
        assert_eq!(room.calls["b"].call_id.as_deref(), Some("c1"), "re-enters the same session");
    }

    #[test]
    fn members_follow_the_starters_auto_close() {
        // a's room says keep open; b's says close. a starts, so the session stays open.
        let mut room = Room::new(&["a", "b"], true);
        room.calls.get_mut("a").unwrap().room_auto_close = false;
        room.join("a", "c1");
        room.join("b", "c1");
        assert!(!room.calls["b"].auto_close, "b follows the session");
        room.leave("a");
        room.leave("b");
        assert_eq!(room.calls["b"].call_id.as_deref(), Some("c1"), "still open, empty");
    }

    #[test]
    fn glare_resolves_to_the_smallest_call_id() {
        let mut room = Room::new(&["a", "b", "c"], true);
        // a and b start at once: neither has seen the other's join.
        let ea = room.calls.get_mut("a").unwrap().join(local("a"), || "c-zzz".into());
        let eb = room.calls.get_mut("b").unwrap().join(local("b"), || "c-aaa".into());
        room.run("a", ea);
        room.run("b", eb);
        room.join("c", "c-mmm");
        for who in ["a", "b", "c"] {
            assert_eq!(room.calls[who].call_id.as_deref(), Some("c-aaa"), "{who}");
        }
        assert_eq!(room.playing("a"), vec!["b", "c"]);
        assert_eq!(room.playing("b"), vec!["a", "c"]);
        assert_eq!(room.playing("c"), vec!["a", "b"]);
    }

    #[test]
    fn the_thirteenth_person_is_refused_visibly() {
        let names: Vec<String> = (0..13).map(|i| format!("m{i:02}")).collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let mut room = Room::new(&refs, true);
        for name in &refs[..12] {
            room.join(name, "c1");
        }
        assert!(refs[..12].iter().all(|n| room.calls[*n].people() == 12));
        room.join("m12", "c1");
        let late = &room.calls["m12"];
        assert_eq!(late.refused, Some(Refusal::Full));
        assert!(late.joined.is_none(), "withdrew");
        assert!(room.playing("m12").is_empty());
        assert!(refs[..12].iter().all(|n| room.calls[*n].people() == 12), "nobody plays the 13th");
        assert!(refs[..12].iter().all(|n| !room.playing(n).contains(&"m12".to_string())));
        // A newcomer that already sees 12 is refused without announcing anything.
        let mut full = RoomCall::new("r_test", true);
        full.call_id = Some("c1".into());
        for name in &refs[..12] {
            full.members.insert(name.to_string(), CallMember { peer: name.to_string(), ticket: "t".into(), addr: None, audio_codec: None });
        }
        assert!(full.join(local("x"), || "c9".into()).is_empty());
        assert_eq!(full.refused, Some(Refusal::Full));
    }

    #[test]
    fn a_lost_transport_counts_as_a_leave() {
        let mut room = Room::new(&["a", "b"], true);
        room.join("a", "c1");
        room.join("b", "c1");
        let effects = room.calls.get_mut("a").unwrap().peer_lost("b");
        assert_eq!(effects, vec![Effect::Unsubscribe("b".into())]);
    }

    #[test]
    fn a_rejoin_with_new_media_replays_it() {
        let mut call = RoomCall::new("r", true);
        call.join(local("me"), || "c1".into());
        let mut control = RoomCallControl {
            action: RoomCallAction::Join,
            room: "r".into(),
            call_id: "c1".into(),
            ticket: Some("ilv1:old".into()),
            addr: None,
            audio_codec: None,
            reply: true,
            auto_close: None,
        };
        call.receive("b", &control);
        control.ticket = Some("ilv1:new".into());
        let effects = call.receive("b", &control);
        assert_eq!(effects[0], Effect::Unsubscribe("b".into()));
        assert!(matches!(&effects[1], Effect::Subscribe(m) if m.ticket == "ilv1:new"));
    }
}
