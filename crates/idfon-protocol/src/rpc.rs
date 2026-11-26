//! Daemon RPC method names and typed request params.
//!
//! Method names follow one rule: every method is `noun.verb`, with a
//! singular noun (`peer.list`, `event.wait`, `blob.put`). Names that predate
//! the rule are still accepted: [`canonical_method`] maps each one to its
//! canonical name. The daemon calls it before dispatch, so either spelling
//! works on any protocol version.
//!
//! Each params struct implements [`Method`], which ties the struct to its
//! method name. Clients build requests with [`Request::call`] instead of
//! hand-written JSON. Absent `Option` fields are omitted, not sent as
//! `null` (an explicit `null` would defeat the daemon's identity defaulting).
//!
//! A peer argument is called `peer` in Rust. On the wire it still uses the
//! older key each method has always read (`ref`, `to`, `subject`, `from`),
//! so new clients keep working with older daemons. The daemon also accepts
//! `peer` for all of these methods ([`peer_param_key`]).

use serde::{Deserialize, Serialize};

use crate::{ContactTicket as ContactCard, DeliveryPolicy, Request, PROTOCOL_VERSION};

/// A typed params struct for one daemon method.
pub trait Method: Serialize {
    /// The canonical method name.
    const NAME: &'static str;
}

impl Request {
    /// Builds a request for `method`, with the params serialized from it.
    pub fn call<M: Method>(id: impl Into<String>, method: &M) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            id: id.into(),
            method: M::NAME.into(),
            params: serde_json::to_value(method).unwrap_or_else(|_| serde_json::json!({})),
        }
    }
}

/// Legacy method names and the canonical name each one maps to.
pub const METHOD_ALIASES: &[(&str, &str)] = &[
    ("status", "daemon.status"),
    ("context", "daemon.status"),
    ("identities", "identity.list"),
    ("identities.compact", "identity.list.compact"),
    ("peers", "peer.list"),
    ("peers.compact", "peer.list.compact"),
    ("events", "event.list"),
    ("events.compact", "event.list.compact"),
    ("events.export", "event.export"),
    ("events.sync", "event.sync"),
    ("events.merge", "event.merge"),
    ("wait", "event.wait"),
    ("capability.ticket", "access.ticket"),
    ("capability.ticket.revoke", "access.ticket.revoke"),
    ("media.resource.put", "blob.put"),
    ("media.resource.fetch", "blob.get"),
    ("media.live.publish", "live.publish"),
    ("media.live.dial", "live.dial"),
    ("media.live.answer", "live.answer"),
    ("media.live.stop", "live.stop"),
    ("media.live.subscribe", "live.subscribe"),
    ("media.live.publishers", "live.list"),
    ("media.session.start", "session.start"),
    ("media.session.stop", "session.stop"),
    ("media.sessions", "session.list"),
    ("files.publish", "file.publish"),
];

/// Returns the canonical name for `method`: the mapped name for a legacy
/// alias, else `method` unchanged.
pub fn canonical_method(method: &str) -> &str {
    METHOD_ALIASES
        .iter()
        .find(|(legacy, _)| *legacy == method)
        .map_or(method, |(_, canonical)| canonical)
}

/// For a canonical method that takes a peer argument, returns the wire key
/// its handler reads. The daemon copies a `peer` param into this key when
/// the key is absent.
pub fn peer_param_key(method: &str) -> Option<&'static str> {
    Some(match method {
        "peer.add" | "peer.update" | "peer.remove" | "peer.resolve" | "peer.show"
        | "peer.status" | "peer.device.add" | "peer.device.update" | "peer.device.remove"
        | "web.url" | "contact.introduce" => "ref",
        "message.send" | "mcp.listen" | "live.dial" => "to",
        "access.check" | "access.grant" | "access.revoke" | "access.ticket" => "subject",
        "live.answer" => "from",
        _ => return None,
    })
}

/// Rewrites a request in place to its canonical form: the canonical method
/// name, and a `peer` param copied into the method's legacy peer key.
/// Idempotent.
pub fn normalize_request(request: &mut Request) {
    let canonical = canonical_method(&request.method);
    if canonical != request.method {
        request.method = canonical.to_owned();
    }
    let Some(key) = peer_param_key(&request.method) else {
        return;
    };
    let Some(params) = request.params.as_object_mut() else {
        return;
    };
    let present = |value: Option<&serde_json::Value>| value.is_some_and(|v| !v.is_null());
    if !present(params.get(key)) && present(params.get("peer")) {
        let peer = params["peer"].clone();
        params.insert(key.to_owned(), peer);
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

macro_rules! methods {
    ($($name:literal => $ty:ident { $($body:tt)* })*) => {
        $(
            #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
            pub struct $ty { $($body)* }
            impl Method for $ty {
                const NAME: &'static str = $name;
            }
        )*

        /// Every canonical method that has a typed params struct.
        pub const TYPED_METHODS: &[&str] = &[$($name),*];
    };
}

methods! {
    // Daemon
    "daemon.status" => DaemonStatus {}
    "daemon.shutdown" => DaemonShutdown {}

    // Identity: one you (identities became tags, 2026-11-24).
    "identity.list" => IdentityList {}
    "identity.rename" => IdentityRename {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub identity: Option<String>,
        pub name: String,
    }
    "identity.update" => IdentityUpdate {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub identity: Option<String>,
        /// Ask before sharing my card: `"ask"` or `"ok"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub share: Option<String>,
    }

    // Tags and what contacts can see (docs/identity.md)
    "tag.list" => TagList {}
    "tag.set" => TagSet {
        pub name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub hide_presence: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub hide_photo: Option<bool>,
    }
    "tag.rename" => TagRename { pub name: String, pub to: String }
    "tag.delete" => TagDelete { pub name: String }
    "visibility.get" => VisibilityGet {}
    "visibility.set" => VisibilitySet {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub presence: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub photo: Option<bool>,
    }
    "visibility.audience" => VisibilityAudience { pub aspect: String }

    // Peers
    "peer.list" => PeerList {}
    "peer.add" => PeerAdd {
        #[serde(rename = "ref", alias = "peer", default, skip_serializing_if = "Option::is_none")]
        pub peer: Option<String>,
        /// Stable account id.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub endpoint_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub endpoint_addr: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub aliases: Option<Vec<String>>,
        /// Legacy; maps to `ring_score` (`auto` | `bar` | `call_kit`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub call_mode: Option<String>,
        /// MCP contact ticket (JSON text).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub mcp_ticket: Option<String>,
        /// Contact ticket from a vCard.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub contact_ticket: Option<ContactCard>,
    }
    "peer.update" => PeerUpdate {
        #[serde(rename = "ref", alias = "peer")]
        pub peer: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub endpoint_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub endpoint_addr: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub aliases: Option<Vec<String>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub call_mode: Option<String>,
        /// Replace the contact's tags.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub tags: Option<Vec<String>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub add_tag: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub remove_tag: Option<String>,
        /// The contact's sharing preference (`IDFON-SHARE/1`): `"ask"` or `"ok"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub share: Option<String>,
    }
    "peer.remove" => PeerRemove {
        #[serde(rename = "ref", alias = "peer")]
        pub peer: String,
    }
    "peer.resolve" => PeerResolve {
        #[serde(rename = "ref", alias = "peer")]
        pub peer: String,
    }
    "peer.show" => PeerShow {
        #[serde(rename = "ref", alias = "peer")]
        pub peer: String,
    }
    "peer.status" => PeerStatus {
        #[serde(rename = "ref", alias = "peer")]
        pub peer: String,
    }
    "peer.device.add" => PeerDeviceAdd {
        #[serde(rename = "ref", alias = "peer")]
        pub peer: String,
        pub endpoint_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub endpoint_addr: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub label: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub device_class: Option<String>,
        #[serde(default)]
        pub capabilities: Vec<String>,
    }
    "peer.device.update" => PeerDeviceUpdate {
        #[serde(rename = "ref", alias = "peer")]
        pub peer: String,
        pub endpoint_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub endpoint_addr: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub label: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub device_class: Option<String>,
        #[serde(default)]
        pub capabilities: Vec<String>,
    }
    "peer.device.remove" => PeerDeviceRemove {
        #[serde(rename = "ref", alias = "peer")]
        pub peer: String,
        pub endpoint_id: String,
    }

    // Contact cards
    "contact.ticket" => ContactTicket {}
    // Introductions (docs/protocol.md "Introductions").
    "contact.introduce" => ContactIntroduce {
        /// The contact whose card is shared.
        #[serde(rename = "ref", alias = "peer")]
        pub peer: String,
        /// Bind the token to this person (a contact, or a peer id).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub to: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub note: Option<String>,
        /// Both-ways introduction.
        #[serde(default, skip_serializing_if = "is_false")]
        pub mutual: bool,
    }
    "contact.inspect" => ContactInspect { pub link: String }

    // Contact requests: held messages from senders who aren't peers
    // (docs/protocol.md "Contact requests").
    "request.list" => RequestList {}
    "request.accept" => RequestAccept {
        pub request_id: String,
        /// The new contact's name; a short id when absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        /// Admit the sender to this conversation (a room) only.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub conversation: Option<String>,
    }
    "request.decline" => RequestDecline {
        pub request_id: String,
        /// Also refuse the sender's future messages.
        #[serde(default, skip_serializing_if = "is_false")]
        pub block: bool,
    }
    "request.unblock" => RequestUnblock { pub peer_id: String }

    // Messaging
    "message.send" => MessageSend {
        #[serde(rename = "to", alias = "peer")]
        pub peer: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub text: Option<String>,
        pub idempotency_key: String,
        /// Room topic this message threads into.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub conversation: Option<String>,
        /// Unsigned per-turn caller context.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub context: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub capability_ticket: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub retries: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub delivery: Option<DeliveryPolicy>,
        /// Keep it in the outbox until delivered (retried when the peer is
        /// back), instead of failing once retries run out.
        #[serde(default, skip_serializing_if = "is_false")]
        pub durable: bool,
    }
    // Ring a contact's message doorbell: one "New message" push that wakes
    // their phone for what waits in the outbox (`IDFON-DOORBELL/1`).
    "peer.doorbell" => PeerDoorbell { pub peer: String }

    // Rooms
    "room.create" => RoomCreate {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        pub members: Vec<String>,
        /// `false` keeps a group call open when its last member leaves.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub call_auto_close: Option<bool>,
    }
    "room.list" => RoomList {}
    "room.send" => RoomSend {
        pub room: String,
        pub text: String,
        pub idempotency_key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub capability_ticket: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub retries: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub delivery: Option<DeliveryPolicy>,
    }
    "room.leave" => RoomLeave { pub room: String }
    "room.call.join" => RoomCallJoin {
        pub room: String,
        /// This member's published broadcast ticket.
        pub ticket: String,
        /// This member's dial address (JSON text).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub addr: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub audio_codec: Option<String>,
    }
    "room.call.leave" => RoomCallLeave { pub room: String }
    "room.call.status" => RoomCallStatus { pub room: String }

    // Events
    "event.list" => EventList {
        #[serde(default, skip_serializing_if = "is_false")]
        pub follow: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub after: Option<String>,
        #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
        pub event_type: Option<String>,
        /// Filter: only events from this peer.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub peer: Option<String>,
    }
    "event.wait" => EventWait {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub after: Option<String>,
        #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
        pub event_type: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub peer: Option<String>,
        /// Default 30000, max 300000.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub timeout_ms: Option<u64>,
    }

    // Operations
    "operation.get" => OperationGet {
        pub operation_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub timeout_ms: Option<u64>,
    }
    "operation.wait" => OperationWait {
        pub operation_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub timeout_ms: Option<u64>,
    }
    "operation.cancel" => OperationCancel { pub operation_id: String }

    // Access
    "access.check" => AccessCheck {
        #[serde(rename = "subject", alias = "peer", default, skip_serializing_if = "Option::is_none")]
        pub peer: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub capability: Option<String>,
    }
    "access.grant" => AccessGrant {
        #[serde(rename = "subject", alias = "peer")]
        pub peer: String,
        pub capability: String,
    }
    "access.revoke" => AccessRevoke {
        #[serde(rename = "subject", alias = "peer")]
        pub peer: String,
        pub capability: String,
    }
    "access.ticket" => AccessTicket {
        #[serde(rename = "subject", alias = "peer")]
        pub peer: String,
        pub capabilities: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub expires_at: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub conversation: Option<String>,
        /// Resource path prefix, e.g. `/fs/public`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub path_scope: Option<String>,
    }

    // MCP transport
    "mcp.listen" => McpListen {
        #[serde(rename = "to", alias = "peer")]
        pub peer: String,
    }
    "mcp.configure" => McpConfigure { pub command: String }

    // Blobs
    "blob.put" => BlobPut {
        pub resource_id: String,
        pub bytes: Vec<u8>,
        #[serde(default)]
        pub append: bool,
        #[serde(default)]
        pub finish: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub mime: Option<String>,
    }
    "blob.get" => BlobGet {
        pub resource_id: String,
        pub blob_ticket: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub offset: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub length: Option<u64>,
    }

    // Live streams
    "live.publish" => LivePublish {
        pub file: String,
        #[serde(rename = "loop", default, skip_serializing_if = "is_false")]
        pub loop_playback: bool,
        #[serde(default, skip_serializing_if = "is_false")]
        pub video: bool,
        pub relay: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
    }
    "live.dial" => LiveDial {
        #[serde(rename = "to", alias = "peer")]
        pub peer: String,
        pub file: String,
        #[serde(default, skip_serializing_if = "is_false")]
        pub video: bool,
        pub relay: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub seconds: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub delivery: Option<DeliveryPolicy>,
    }
    "live.answer" => LiveAnswer {
        /// Only answer this peer.
        #[serde(rename = "from", alias = "peer", default, skip_serializing_if = "Option::is_none")]
        pub peer: Option<String>,
        pub out: String,
        #[serde(default, skip_serializing_if = "is_false")]
        pub video: bool,
        pub relay: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub quality: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub seconds: Option<u64>,
        /// Seconds to wait for a caller.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub wait: Option<u64>,
    }
    "live.subscribe" => LiveSubscribe {
        pub ticket: String,
        pub out: String,
        #[serde(default, skip_serializing_if = "is_false")]
        pub video: bool,
        pub relay: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub quality: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub seconds: Option<u64>,
    }
    "live.stop" => LiveStop { pub id: String }
    "live.list" => LiveList {}

    // Durable files
    "file.publish" => FilePublish {
        /// Absolute path readable by the daemon.
        pub path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub token: Option<String>,
        /// Seconds.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub expires_in: Option<u64>,
        #[serde(default, skip_serializing_if = "is_false")]
        pub permanent: bool,
    }

    // Gateway, provider, web
    "gateway.start" => GatewayStart {
        /// `auto`, `direct` or `edge`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub prefer: Option<String>,
    }
    "gateway.stop" => GatewayStop {}
    "provider.start" => ProviderStart { pub root: String }
    "provider.stop" => ProviderStop {}
    "web.url" => WebUrl {
        #[serde(rename = "ref", alias = "peer")]
        pub peer: String,
        pub path: String,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aliases_map_to_canonical_names() {
        assert_eq!(canonical_method("peers"), "peer.list");
        assert_eq!(canonical_method("wait"), "event.wait");
        assert_eq!(canonical_method("media.resource.put"), "blob.put");
        assert_eq!(canonical_method("peer.add"), "peer.add");
        assert_eq!(canonical_method("no.such"), "no.such");
    }

    #[test]
    fn canonical_names_are_not_aliases_and_follow_noun_verb() {
        for (legacy, canonical) in METHOD_ALIASES {
            assert_ne!(legacy, canonical);
            assert_eq!(canonical_method(canonical), *canonical, "{canonical} is itself an alias");
        }
        for name in TYPED_METHODS {
            assert_eq!(canonical_method(name), *name, "{name} is a legacy alias");
            assert!(name.contains('.'), "{name} is not noun.verb");
        }
    }

    #[test]
    fn absent_options_are_omitted_not_null() {
        let request = Request::call("t", &PeerAdd {
            peer: Some("alice".into()),
            ..Default::default()
        });
        assert_eq!(request.method, "peer.add");
        assert_eq!(request.params, serde_json::json!({"ref": "alice"}));
    }

    #[test]
    fn peer_uses_the_legacy_wire_key_and_accepts_peer() {
        let send = MessageSend {
            peer: "bob".into(),
            idempotency_key: "k".into(),
            ..Default::default()
        };
        let value = serde_json::to_value(&send).unwrap();
        assert_eq!(value["to"], "bob");
        let parsed: MessageSend =
            serde_json::from_value(serde_json::json!({"peer": "bob", "idempotency_key": "k"}))
                .unwrap();
        assert_eq!(parsed, send);
    }

    #[test]
    fn normalize_renames_method_and_copies_peer() {
        let mut request = Request {
            version: PROTOCOL_VERSION,
            id: "t".into(),
            method: "capability.ticket".into(),
            params: serde_json::json!({"peer": "carol", "capabilities": []}),
        };
        normalize_request(&mut request);
        assert_eq!(request.method, "access.ticket");
        assert_eq!(request.params["subject"], "carol");

        // An explicit legacy key wins.
        let mut request = Request {
            version: PROTOCOL_VERSION,
            id: "t".into(),
            method: "peer.show".into(),
            params: serde_json::json!({"peer": "x", "ref": "y"}),
        };
        normalize_request(&mut request);
        assert_eq!(request.params["ref"], "y");

        // event.list uses `peer` as its own filter: untouched.
        let mut request = Request {
            version: PROTOCOL_VERSION,
            id: "t".into(),
            method: "events".into(),
            params: serde_json::json!({"peer": "x"}),
        };
        normalize_request(&mut request);
        assert_eq!(request.method, "event.list");
        assert_eq!(request.params, serde_json::json!({"peer": "x"}));
    }

    #[test]
    fn delivery_and_loop_serialize_like_the_old_cli() {
        let publish = LivePublish {
            file: "a.wav".into(),
            loop_playback: true,
            relay: true,
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&publish).unwrap(),
            serde_json::json!({"file": "a.wav", "loop": true, "relay": true})
        );
        let dial = LiveDial {
            peer: "p".into(),
            file: "a.wav".into(),
            relay: true,
            delivery: Some(DeliveryPolicy::default()),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&dial).unwrap()["delivery"],
            serde_json::json!({"mode": "failover"})
        );
    }
}
