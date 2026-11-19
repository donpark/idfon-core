//! Group-call signaling in the daemon (idfon-home docs/group-live-chat.md, G1).
//!
//! One [`RoomCall`] state machine per (identity, room), fed two ways:
//! - room controls (`IDFON-LIVE/1` with `room=`) received on the room's
//!   channel, from senders that belong to the room ([`on_received`]);
//! - this member's own `room.call.join` / `room.call.leave` RPCs.
//!
//! The state machine's sends go out as ordinary room or direct messages with
//! `conversation` = the room id. Received controls arrive on a path that has no
//! transport (the receiver dispatches over a fake one), so their replies wait
//! in an outbox that drains on the live transport, recorded when the identity
//! comes online. Media is the app's: every change is published as a
//! `room.call.updated` event carrying the roster, and the app subscribes to or
//! drops members from it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use idfon_protocol::room_call::{
    is_room_call_control, Effect, LocalJoin, RoomCall, RoomCallControl,
};
use idfon_protocol::{ErrorCode, Request, Response, PROTOCOL_VERSION};

use super::{
    error_response, identity_id_of, record_state_event, request_text, room_matches, room_send,
    send_message, success, transport_not_ready, Store, TransportMode,
};

/// A send the state machine asked for: (identity, room id, effect).
type Outbound = (String, String, Effect);

/// The store's group-call state. Runtime only: a restart forgets it, and
/// members re-announce when they re-join.
#[derive(Default)]
pub(super) struct RoomCalls {
    /// Calls by `"{identity}:{room id}"`.
    calls: HashMap<String, RoomCall>,
    /// Sends waiting for a transport.
    outbox: Vec<Outbound>,
    /// The transport the outbox drains on, set when an identity comes online.
    live: Option<Arc<TransportMode>>,
}

impl std::fmt::Debug for RoomCalls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoomCalls")
            .field("calls", &self.calls.len())
            .field("outbox", &self.outbox.len())
            .finish()
    }
}

/// The transport the outbox drains on. Set when an identity comes online.
pub(super) fn set_live_transport(store: &Arc<Mutex<Store>>, transport: Arc<TransportMode>) {
    store.lock().expect("store mutex poisoned").room_calls.live = Some(transport);
}

fn key(identity: &str, room_id: &str) -> String {
    format!("{identity}:{room_id}")
}

fn unique_key(prefix: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("{prefix}-{nanos:x}-{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

fn new_call_id() -> String {
    unique_key("call")
}

/// Wall clock in ms: the state machine's time (join order, refresh, expiry).
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

/// This identity's peer id as other members see it (the sender id on its
/// messages): the cap's tie-break.
fn self_peer(state: &Store, identity: &str) -> String {
    state
        .identities
        .iter()
        .find(|candidate| candidate.id == identity)
        .and_then(|candidate| candidate.public_key.clone())
        .unwrap_or_else(|| identity.to_string())
}

/// The room's call, created on first use.
fn call_for<'a>(state: &'a mut Store, identity: &str, room: &idfon_protocol::Room) -> &'a mut RoomCall {
    let me = self_peer(state, identity);
    state
        .room_calls
        .calls
        .entry(key(identity, &room.id))
        .or_insert_with(|| RoomCall::new(room.id.clone(), me, room.call_auto_close()))
}

/// The event payload: the roster and this side's place in it.
fn call_json(call: &RoomCall, ended: bool) -> serde_json::Value {
    serde_json::json!({
        "room": call.room,
        "call_id": call.call_id,
        "joined": call.joined.is_some(),
        "people": call.people(),
        "members": call.members.values().collect::<Vec<_>>(),
        "auto_close": call.auto_close,
        "refused": call.refused,
        "waiting": call.waiting.is_some(),
        "ended": ended,
    })
}

/// Queue the sends, and publish the new state as `room.call.updated` when
/// what the app sees changed (`before`: the call's JSON before the change).
fn apply(state: &mut Store, identity: &str, call: &RoomCall, effects: Vec<Effect>, before: Option<serde_json::Value>) {
    let ended = effects.iter().any(|effect| *effect == Effect::Ended);
    for effect in effects {
        if matches!(effect, Effect::SendRoom(_) | Effect::SendDirect { .. }) {
            state.room_calls.outbox.push((identity.to_string(), call.room.clone(), effect));
        }
    }
    let after = call_json(call, ended);
    if before.as_ref() != Some(&after) {
        record_state_event(state, identity, "room.call.updated", after);
    }
}

/// Run every call's clock: refreshes and expiry. Called about once a second.
pub(super) fn tick_all(store: &Arc<Mutex<Store>>) {
    let now = now_ms();
    let any = {
        let mut state = store.lock().expect("store mutex poisoned");
        let keys: Vec<String> = state.room_calls.calls.keys().cloned().collect();
        let mut any = false;
        for key in keys {
            let Some(call) = state.room_calls.calls.get_mut(&key) else { continue };
            let before = call_json(call, false);
            let effects = call.tick(now);
            if effects.is_empty() {
                continue;
            }
            any = true;
            let call = call.clone();
            let identity = key.split_once(':').map(|(identity, _)| identity.to_string()).unwrap_or_default();
            apply(&mut state, &identity, &call, effects, Some(before));
        }
        any
    };
    if any {
        flush_live(store);
    }
}

/// A received message, under the store lock. Feeds room controls from room
/// members to the room's call; ignores everything else. Returns whether the
/// message was a room control.
pub(super) fn on_received(
    state: &mut Store,
    identity: &str,
    sender: &str,
    conversation: Option<&str>,
    text: &str,
) -> bool {
    if !is_room_call_control(text) {
        return false;
    }
    let Some(control) = RoomCallControl::decode(text) else {
        return true;
    };
    let Some(room) = state
        .rooms
        .iter()
        .find(|room| room.identity == identity && room.id == control.room)
        .cloned()
    else {
        return true;
    };
    // The control must travel on its own room's channel, from one of its members.
    if conversation != Some(room.id.as_str()) || !room.members.iter().any(|member| member == sender) {
        tracing::warn!(target: "idfon.daemon", room = %room.id, sender = %sender, "room call control from outside the room; ignored");
        return true;
    }
    let call = call_for(state, identity, &room);
    let before = call_json(call, false);
    let effects = call.receive(sender, &control, now_ms());
    let call = call.clone();
    tracing::info!(target: "idfon.daemon", room = %room.id, sender = %sender, action = ?control.action, people = call.people(), "room call control");
    apply(state, identity, &call, effects, Some(before));
    true
}

/// Send what the outbox holds on the live transport, if it is up. Call
/// outside the store lock.
pub(super) fn flush_live(store: &Arc<Mutex<Store>>) {
    let transport = store.lock().expect("store mutex poisoned").room_calls.live.clone();
    if let Some(transport) = transport {
        flush(store, &transport);
    }
}

/// Send what the outbox holds on `transport`. Call outside the store lock.
pub(super) fn flush(store: &Arc<Mutex<Store>>, transport: &Arc<TransportMode>) {
    let pending: Vec<Outbound> =
        std::mem::take(&mut store.lock().expect("store mutex poisoned").room_calls.outbox);
    for (identity, room_id, effect) in pending {
        let (method, params) = match effect {
            Effect::SendRoom(text) => (
                "room.send",
                serde_json::json!({
                    "identity": identity,
                    "room": room_id,
                    "text": text,
                    "idempotency_key": unique_key("roomcall"),
                }),
            ),
            Effect::SendDirect { peer, text } => (
                "message.send",
                serde_json::json!({
                    "identity": identity,
                    "to": peer,
                    "text": text,
                    "conversation": room_id,
                    "idempotency_key": unique_key("roomcall"),
                }),
            ),
            _ => continue,
        };
        let request = Request {
            version: PROTOCOL_VERSION,
            id: unique_key("roomcall-req"),
            method: method.into(),
            params,
        };
        let response = if method == "room.send" {
            room_send(&request, store, Some(transport))
        } else {
            send_message(&request, store, Some(transport))
        };
        if !response.ok {
            tracing::warn!(target: "idfon.daemon", room = %room_id, method, "room call send failed");
        }
    }
}

/// Resolve the request's room for its identity: (identity id, room).
fn find_room(
    request: &Request,
    state: &Store,
) -> Result<(String, idfon_protocol::Room), Response> {
    let identity_ref = request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let identity_id = identity_id_of(state, &identity_ref);
    let room_ref = request_text(&request.params, "room").unwrap_or_default();
    state
        .rooms
        .iter()
        .find(|room| room.identity == identity_id && room_matches(room, &room_ref))
        .cloned()
        .map(|room| (identity_id, room))
        .ok_or_else(|| {
            error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                format!("room not found: {room_ref}"),
                false,
            )
        })
}

/// `room.call.join { room, ticket, addr?, audio_codec? }`: join the room's call
/// (starting one if none runs) with this member's published broadcast. Returns
/// the call; `refused: "full"` when it already holds 12 people.
pub(super) fn join(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
) -> Response {
    let Some(transport) = transport else {
        return transport_not_ready(request);
    };
    let Some(ticket) = request_text(&request.params, "ticket") else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "ticket is required".into(),
            false,
        );
    };
    let local = LocalJoin {
        ticket,
        addr: request_text(&request.params, "addr"),
        audio_codec: request_text(&request.params, "audio_codec"),
    };
    let call = {
        let mut state = store.lock().expect("store mutex poisoned");
        let (identity, room) = match find_room(request, &state) {
            Ok(found) => found,
            Err(response) => return response,
        };
        let call = call_for(&mut state, &identity, &room);
        call.room_auto_close = room.call_auto_close();
        let before = call_json(call, false);
        let effects = call.join(local, now_ms(), new_call_id);
        let call = call.clone();
        apply(&mut state, &identity, &call, effects, Some(before));
        let dir = state.data_dir.clone();
        let _ = state.save(&dir);
        call
    };
    flush(store, transport);
    success(request, serde_json::json!({ "call": call_json(&call, false) }))
}

/// `room.call.leave { room }`: leave the room's call. The session goes on for
/// the others (or ends, with auto-close, if this was the last member).
pub(super) fn leave(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
) -> Response {
    let Some(transport) = transport else {
        return transport_not_ready(request);
    };
    let call = {
        let mut state = store.lock().expect("store mutex poisoned");
        let (identity, room) = match find_room(request, &state) {
            Ok(found) => found,
            Err(response) => return response,
        };
        let Some(call) = state.room_calls.calls.get_mut(&key(&identity, &room.id)) else {
            return success(request, serde_json::json!({ "call": null }));
        };
        let before = call_json(call, false);
        let effects = call.leave(now_ms());
        let call = call.clone();
        apply(&mut state, &identity, &call, effects, Some(before));
        let dir = state.data_dir.clone();
        let _ = state.save(&dir);
        call
    };
    flush(store, transport);
    success(request, serde_json::json!({ "call": call_json(&call, call.call_id.is_none()) }))
}

/// `room.call.peer_lost { room, peer }`: the app lost a member's media
/// transport; counts as that member leaving.
pub(super) fn peer_lost(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let peer = request_text(&request.params, "peer").unwrap_or_default();
    let mut state = store.lock().expect("store mutex poisoned");
    let (identity, room) = match find_room(request, &state) {
        Ok(found) => found,
        Err(response) => return response,
    };
    let Some(call) = state.room_calls.calls.get_mut(&key(&identity, &room.id)) else {
        return success(request, serde_json::json!({ "call": null }));
    };
    let before = call_json(call, false);
    let effects = call.peer_lost(&peer, now_ms());
    let call = call.clone();
    apply(&mut state, &identity, &call, effects, Some(before));
    drop(state);
    flush_live(store);
    success(request, serde_json::json!({ "call": call_json(&call, call.call_id.is_none()) }))
}

/// `room.call.status { room }`: the room's call as this member sees it, or null.
pub(super) fn status(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let state = store.lock().expect("store mutex poisoned");
    let (identity, room) = match find_room(request, &state) {
        Ok(found) => found,
        Err(response) => return response,
    };
    let call = state
        .room_calls
        .calls
        .get(&key(&identity, &room.id))
        .map(|call| call_json(call, false));
    success(request, serde_json::json!({ "call": call }))
}

