#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
    sync::{Arc, Mutex, OnceLock, RwLock},
    time::Duration,
};

mod blob;
mod build_info;
mod diag_views;
mod introductions;
mod live;
mod mcp;
mod relay_auth;
mod room_calls;

use axum::{
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::{IntoResponse, Response as AxumResponse},
    routing::get,
    Router as AxumRouter,
};
use futures_util::StreamExt;
use idfon_core::transport::{
    room_topic, FakeTransport, IrohTransport, MessageTransport, SideChannelGuard, SYNC_ALPN,
};
use idfon_gateway::{
    issue_scoped_token, resource_scope_path, AccountResolver, Authorizer, Backend, Caller,
    Config as GatewayConfig, EdgeBackend, FallbackBackend, GatewayError, GatewayHandle,
    IrohBackend, Resource, StaticToken,
};
use idfon_h3::{serve_router, H3Server, RemoteId};
use idfon_media::service::MediaService;
use idfon_protocol::{
    encode_json, validate_request, ApiError, ErrorCode, Identity, Request, Response, ResponseBody,
    PROTOCOL_VERSION,
};
use iroh::endpoint::Connection;
use iroh::EndpointAddr;
use iroh_gossip::{Event as GossipEvent, GossipSender};
use rusqlite::OptionalExtension;
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::mpsc,
};

const DEFAULT_TRANSPORT: &str = "iroh";
const EVENT_RETENTION: usize = 1000;
// Per-response cap for events.compact: the app's host-result budget is 256 KiB,
// so a full-history response eventually exceeds it and wedges delivery (the
// client's error path never advances its cursor). The client drains the rest
// via its cursor on the next 1s poll.
const COMPACT_EVENTS_MAX_BYTES: usize = 128 * 1024;
static SYNC_GUARDS: OnceLock<Mutex<Vec<SideChannelGuard>>> = OnceLock::new();
const MAX_RESOURCE_BYTES: usize = 512 * 1024;
/// Cap on the unsigned per-turn `context` a sender may attach. Pushed by the
/// UI for active caller-side state; receivers treat it as untrusted data.
const MAX_CONTEXT_CHARS: usize = 4096;
static MEDIA_SERVICE: OnceLock<MediaService> = OnceLock::new();
struct GossipTopicHandle {
    sender: GossipSender,
    task: tokio::task::JoinHandle<()>,
}

static GOSSIP_TOPICS: OnceLock<Mutex<HashMap<String, GossipTopicHandle>>> = OnceLock::new();
const GOSSIP_ROOM_MEMBER_THRESHOLD: usize = 5;

fn gossip_topics() -> &'static Mutex<HashMap<String, GossipTopicHandle>> {
    GOSSIP_TOPICS.get_or_init(|| Mutex::new(HashMap::new()))
}

enum TransportMode {
    Fake(FakeTransport),
    Iroh(Arc<idfon_core::transport::TransportManager>),
}

impl TransportMode {
    async fn new(name: &str, key: Option<[u8; 32]>, port: Option<u16>) -> io::Result<Self> {
        match name {
            "fake" => Ok(Self::Fake(FakeTransport::default())),
            "iroh" => idfon_core::transport::TransportManager::bind(key, port)
                .await
                .map(Arc::new)
                .map(Self::Iroh)
                .map_err(io::Error::other),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "transport must be fake or iroh",
            )),
        }
    }

    fn endpoint_ticket_for(&self, identity: &str) -> Option<Vec<u8>> {
        match self {
            Self::Fake(fake) => fake.endpoint_ticket(identity),
            Self::Iroh(transport) => transport.endpoint_ticket(identity),
        }
    }

    /// Sync accessor for an identity's iroh transport (blocking resolve on a
    /// throwaway runtime, same pattern as `ensure_identity`).
    fn current_transport(
        &self,
        identity: &str,
    ) -> Option<Arc<idfon_core::transport::IrohTransport>> {
        let Self::Iroh(manager) = self else {
            return None;
        };
        let manager = manager.clone();
        let identity = identity.to_string();
        std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    let runtime = tokio::runtime::Runtime::new().ok()?;
                    runtime.block_on(manager.current(&identity))
                })
                .join()
                .ok()?
        })
    }

    /// Async accessor for an identity's iroh transport, for handlers that
    /// already run on the daemon runtime (unlike [`Self::current_transport`]).
    async fn current_transport_async(
        &self,
        identity: &str,
    ) -> Option<Arc<idfon_core::transport::IrohTransport>> {
        match self {
            Self::Iroh(manager) => manager.current(identity).await,
            Self::Fake(_) => None,
        }
    }

    async fn add_identity(&self, identity: &str, key: [u8; 32]) -> io::Result<String> {
        let Self::Iroh(manager) = self else {
            // The fake transport binds no endpoints (tests, `--transport
            // fake`); every identity being live at startup (D20) is a no-op
            // for it, not an error.
            return Ok(String::new());
        };
        manager
            .add_identity(identity, key)
            .await
            .map_err(io::Error::other)
    }

    fn ensure_identity(&self, identity: &str, key: [u8; 32]) -> io::Result<Option<String>> {
        let Self::Iroh(manager) = self else {
            return Ok(None);
        };
        if let Some(endpoint_id) = manager.endpoint_id(identity) {
            return Ok(Some(endpoint_id));
        }
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = tokio::runtime::Runtime::new()?;
                    runtime
                        .block_on(manager.add_identity(identity, key))
                        .map(Some)
                        .map_err(io::Error::other)
                })
                .join()
                .map_err(|_| io::Error::other("identity bind worker panicked"))?
        })
    }

    fn send(
        &self,
        identity: &str,
        peer: &idfon_protocol::Peer,
        message: &idfon_protocol::MessageEnvelope,
        policy: Option<&idfon_protocol::DeliveryPolicy>,
    ) -> io::Result<idfon_protocol::MessageAck> {
        // The fake transport dials a fixed placeholder; the real one tries
        // each device of the peer's account until one acknowledges.
        if matches!(self, Self::Fake(_)) {
            // The peer's first device when it has one, so tests can route by
            // target; otherwise the placeholder.
            let target = peer
                .dial_targets_with(policy)
                .into_iter()
                .next()
                .map(|(_, address)| address)
                .unwrap_or_else(|| {
                    "0000000000000000000000000000000000000000000000000000000000000000".to_string()
                });
            return self.send_to(identity, &target, message);
        }
        let targets = peer.dial_targets_with(policy);
        deliver(policy, targets, |endpoint_id, address| {
            tracing::info!(target: "idfon.daemon", identity = %identity, peer = %peer.id, device = %endpoint_id, message_id = %message.message_id, trace = ?message.trace, telemetry = ?message.telemetry, target_bytes = address.len(), "transport send attempt");
            let result = self.send_to(identity, address, message);
            match &result {
                Ok(ack) => tracing::info!(target: "idfon.daemon", identity = %identity, peer = %peer.id, device = %endpoint_id, message_id = %message.message_id, trace = ?message.trace, status = ?ack.status, "transport send acknowledged"),
                Err(error) => tracing::error!(target: "idfon.daemon", identity = %identity, peer = %peer.id, device = %endpoint_id, message_id = %message.message_id, trace = ?message.trace, error = %error, "transport send failed"),
            }
            result
        })
    }

    /// One dial attempt against a single serialized `EndpointAddr`.
    fn send_to(
        &self,
        identity: &str,
        address: &str,
        message: &idfon_protocol::MessageEnvelope,
    ) -> io::Result<idfon_protocol::MessageAck> {
        let target = serde_json::from_str(address)
            .map_err(|error| io::Error::other(format!("invalid endpoint address: {error}")))?;
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = tokio::runtime::Runtime::new()?;
                    match self {
                        Self::Fake(transport) => runtime.block_on(transport.send(&target, message)),
                        Self::Iroh(transport) => {
                            runtime.block_on(transport.send(identity, &target, message))
                        }
                    }
                    .map_err(io::Error::other)
                })
                .join()
                .map_err(|_| io::Error::other("transport worker panicked"))?
        })
    }
}

/// The device-delivery loop (`docs/multi-device.md`): no policy or `One`
/// dials the first target only; `Failover` walks the device list until one
/// acknowledges; `All` delivers to every device. Extracted so the policy
/// semantics are host-testable.
fn deliver(
    policy: Option<&idfon_protocol::DeliveryPolicy>,
    mut targets: Vec<(String, String)>,
    mut send_to: impl FnMut(&str, &str) -> io::Result<idfon_protocol::MessageAck>,
) -> io::Result<idfon_protocol::MessageAck> {
    let mode = policy.map(|policy| &policy.mode);
    if matches!(mode, Some(idfon_protocol::DeliveryMode::One) | None) {
        targets.truncate(1);
    }
    if targets.is_empty() {
        return Err(io::Error::other("peer has no endpoint address"));
    }
    let all = matches!(mode, Some(idfon_protocol::DeliveryMode::All));
    let mut last_error = None;
    let mut last_ack = None;
    for (endpoint_id, address) in targets {
        match send_to(&endpoint_id, &address) {
            Ok(ack) => {
                if !all {
                    return Ok(ack);
                }
                last_ack = Some(ack);
            }
            Err(error) => {
                last_error = Some(error);
                // Failover keeps walking the device list; only `One` (a
                // single target anyway) stops at the first failure. Breaking
                // here used to disable failover entirely (C17).
                if matches!(mode, Some(idfon_protocol::DeliveryMode::One)) {
                    break;
                }
            }
        }
    }
    match (last_ack, last_error) {
        (Some(ack), _) => Ok(ack),
        (None, Some(error)) => Err(error),
        (None, None) => Err(io::Error::other("peer has no endpoint address")),
    }
}

/// Identity (account signing) key on disk. Plain files in the profile dir —
/// no store involved — so endpoint binding can start before the store loads.
fn identity_key_in(data_dir: &Path, identity_id: &str) -> io::Result<idfon_core::IdentityKey> {
    let path = data_dir.join(format!("identity-{identity_id}.key"));
    let value = std::fs::read_to_string(path)?;
    idfon_core::IdentityKey::decode(value.trim())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid identity key"))
}

/// The transport (endpoint) key for an identity (see `Store::endpoint_key`):
/// a dedicated `endpoint-<id>.key` when the account/endpoint split created
/// one, the identity key otherwise.
fn endpoint_key_in(data_dir: &Path, identity_id: &str) -> io::Result<idfon_core::IdentityKey> {
    let path = data_dir.join(format!("endpoint-{identity_id}.key"));
    match std::fs::read_to_string(&path) {
        Ok(value) => idfon_core::IdentityKey::decode(value.trim())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid endpoint key")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            identity_key_in(data_dir, identity_id)
        }
        Err(error) => Err(error),
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Store {
    identities: Vec<Identity>,
    peers: Vec<idfon_protocol::Peer>,
    #[serde(default)]
    operations: Vec<idfon_protocol::Operation>,
    #[serde(default)]
    events: Vec<idfon_protocol::Event>,
    #[serde(default)]
    messages: Vec<idfon_protocol::MessageEnvelope>,
    #[serde(default)]
    grants: Vec<idfon_protocol::CapabilityGrant>,
    /// Local rooms (topic id + member list); see `docs/chatrooms.md`.
    #[serde(default)]
    rooms: Vec<idfon_protocol::Room>,
    #[serde(default)]
    policies: Vec<idfon_protocol::LocalPolicy>,
    #[serde(default)]
    revoked_tickets: Vec<String>,
    /// Held messages from senders who aren't peers (docs/protocol.md
    /// "Contact requests"), oldest first.
    #[serde(default)]
    contact_requests: Vec<idfon_protocol::ContactRequest>,
    /// Senders whose request was declined with `block`: refused without a
    /// request while they aren't peers.
    #[serde(default)]
    blocked_senders: Vec<BlockedSender>,
    /// Introduction tokens attached to messages to a contact until a message
    /// from them arrives (docs/protocol.md "Introductions").
    #[serde(default)]
    pending_introductions: Vec<introductions::PendingIntroduction>,
    /// Endpoint ids this daemon admitted at the relay's auth service
    /// (`relay_auth`); revocation is the set difference against live
    /// peers/identities (idfon-edge/docs/relay-auth.md §6).
    #[serde(default)]
    relay_admitted: Vec<String>,
    #[serde(default)]
    mcp_discovery: Vec<idfon_protocol::McpDiscoveryRecord>,
    /// Local MCP server command the inbound `idfon/mcp/1` relay splices to
    /// (set by `mcp.configure`; `IDFON_MCP_COMMAND` overrides).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mcp_command: Option<String>,
    #[serde(default)]
    sessions: Vec<idfon_protocol::MediaSession>,
    /// Legacy one-shot marker of the D4 preset identities (Work / Personal /
    /// Private). Presets are no longer created (identities are tags,
    /// 2026-11-24); kept so old stores still parse.
    #[serde(default)]
    presets_v1: bool,
    /// One-shot marker: every identity collapsed into `default`, the others
    /// becoming tags (see `ensure_single_identity`).
    #[serde(default)]
    single_identity_v1: bool,
    /// What contacts with each tag can't see of you (docs/identity.md).
    #[serde(default)]
    tag_rules: Vec<idfon_protocol::TagRule>,
    /// What contacts see of you unless a tag or their own override says
    /// otherwise (`None` = shown).
    #[serde(default)]
    visibility: idfon_protocol::Visibility,
    /// One-shot marker: stored `call_mode: bar` values from before the field
    /// became optional were the old implicit default, not a choice; they are
    /// cleared once so each contact kind gets the shell's default (see
    /// `ensure_call_mode_auto`).
    #[serde(default)]
    call_mode_auto_v1: bool,
    /// One-shot marker: legacy `call_mode` values are folded into
    /// `ring_score` once (see `ensure_ring_scores`).
    #[serde(default)]
    ring_score_v1: bool,
    #[serde(skip)]
    data_dir: PathBuf,
    /// Group-call sessions (G1): runtime only, rebuilt as members re-announce.
    #[serde(skip)]
    room_calls: room_calls::RoomCalls,
}

/// A sender blocked from making contact requests to `identity`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BlockedSender {
    identity: String,
    peer_id: String,
    blocked_at: String,
}

impl Store {
    fn db(data_dir: &Path) -> io::Result<rusqlite::Connection> {
        std::fs::create_dir_all(data_dir)?;
        let connection =
            rusqlite::Connection::open(data_dir.join("state.db")).map_err(io::Error::other)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(io::Error::other)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(io::Error::other)?;
        connection
            .execute_batch("CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK (id = 1), json TEXT NOT NULL);")
            .map_err(io::Error::other)?;
        Ok(connection)
    }

    fn load(data_dir: &Path) -> io::Result<Self> {
        if let Ok(connection) = Self::db(data_dir) {
            let snapshot = connection
                .query_row("SELECT json FROM state WHERE id = 1", [], |row| {
                    row.get::<_, String>(0)
                })
                .optional()
                .map_err(io::Error::other)?;
            if let Some(snapshot) = snapshot {
                let mut store: Self = serde_json::from_str(&snapshot).map_err(io::Error::other)?;
                store.data_dir = data_dir.to_path_buf();
                store.ensure_single_identity(data_dir)?;
                store.ensure_identity_keys(data_dir)?;
                store.ensure_call_mode_auto(data_dir)?;
                store.ensure_ring_scores(data_dir)?;
                return Ok(store);
            }
        }
        let path = data_dir.join("state.json");
        match std::fs::read(&path) {
            Ok(bytes) => {
                let mut store: Self = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                store.data_dir = data_dir.to_path_buf();
                store.ensure_single_identity(data_dir)?;
                store.ensure_identity_keys(data_dir)?;
                store.ensure_call_mode_auto(data_dir)?;
                store.ensure_ring_scores(data_dir)?;
                Ok(store)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let store = Self {
                    identities: vec![Identity {
                        id: "default".into(),
                        name: "Default".into(),
                        endpoint_id: None,
                        public_key: None,
                        active: true,
                        share: None,
                    }],
                    peers: Vec::new(),
                    operations: Vec::new(),
                    events: Vec::new(),
                    messages: Vec::new(),
                    grants: Vec::new(),
                    rooms: Vec::new(),
                    policies: Vec::new(),
                    revoked_tickets: Vec::new(),
                    contact_requests: Vec::new(),
                    blocked_senders: Vec::new(),
                    pending_introductions: Vec::new(),
                    relay_admitted: Vec::new(),
                    mcp_discovery: Vec::new(),
                    mcp_command: None,
                    sessions: Vec::new(),
                    presets_v1: true,
                    single_identity_v1: true,
                    tag_rules: Vec::new(),
                    visibility: Default::default(),
                    // A fresh store has no legacy implicit `bar` to clear.
                    call_mode_auto_v1: true,
                    ring_score_v1: true,
                    data_dir: data_dir.to_path_buf(),
                    room_calls: Default::default(),
                };
                let mut store = store;
                store.ensure_identity_keys(data_dir)?;
                store.save(data_dir)?;
                Ok(store)
            }
            Err(error) => Err(error),
        }
    }

    /// 2026-11-17: `call_mode` used to be stored as `bar` for every peer that
    /// never chose (the serde default). It is optional now, and unset means
    /// the shell's default for the contact's kind (people → System call), so
    /// clear those implicit values once. Explicit `call_kit` choices stay. An
    /// explicit `bar` chosen before this cannot be told apart and is reset
    /// too; the user can set it again.
    fn ensure_call_mode_auto(&mut self, data_dir: &Path) -> io::Result<()> {
        if self.call_mode_auto_v1 {
            return Ok(());
        }
        self.call_mode_auto_v1 = true;
        for peer in &mut self.peers {
            if peer.call_mode == Some(idfon_protocol::IncomingCallMode::Bar) {
                peer.call_mode = None;
            }
        }
        self.save(data_dir)
    }

    /// 2026-11-17: per-contact handling became a ring score. Fold any legacy
    /// `call_mode` left after `ensure_call_mode_auto` into `ring_score`
    /// (`call_kit` → the person default, `bar` → the agent default) and drop
    /// it. An existing `ring_score` wins.
    fn ensure_ring_scores(&mut self, data_dir: &Path) -> io::Result<()> {
        if self.ring_score_v1 {
            return Ok(());
        }
        self.ring_score_v1 = true;
        for peer in &mut self.peers {
            if let Some(mode) = peer.call_mode.take() {
                peer.ring_score.get_or_insert(mode.ring_score());
            }
        }
        self.save(data_dir)
    }

    /// 2026-11-24 (Don): identities are tags that decide what a contact can
    /// see of you, not separate selves. Collapse every identity into
    /// `default` — one endpoint. Each other identity's contacts move to
    /// `default` tagged with that identity's name (merged when the same
    /// contact was under both), and its grants, rooms, events, operations,
    /// policies and sessions follow. Its key files stay on disk but no longer
    /// bind, so contacts who knew you only through that identity must add
    /// you again (docs/identity.md; no users yet).
    fn ensure_single_identity(&mut self, data_dir: &Path) -> io::Result<()> {
        if self.single_identity_v1 {
            return Ok(());
        }
        self.single_identity_v1 = true;
        self.presets_v1 = true;
        if !self.identities.iter().any(|identity| identity.id == "default") {
            return self.save(data_dir);
        }
        let others: Vec<(String, String)> = self
            .identities
            .iter()
            .filter(|identity| identity.id != "default")
            .map(|identity| (identity.id.clone(), identity.name.clone()))
            .collect();
        for (id, tag) in &others {
            let moving: Vec<idfon_protocol::Peer> =
                self.peers.iter().filter(|peer| &peer.identity == id).cloned().collect();
            self.peers.retain(|peer| &peer.identity != id);
            for mut peer in moving {
                if let Some(existing) = self
                    .peers
                    .iter_mut()
                    .find(|other| other.identity == "default" && other.id == peer.id)
                {
                    add_tag(&mut existing.tags, tag);
                    continue;
                }
                peer.identity = "default".into();
                add_tag(&mut peer.tags, tag);
                if self.peers.iter().any(|other| other.identity == "default" && other.name == peer.name) {
                    peer.name = format!("{} ({tag})", peer.name);
                }
                self.peers.push(peer);
            }
            for grant in &mut self.grants {
                if &grant.identity == id {
                    grant.identity = "default".into();
                }
            }
            let default_rooms: Vec<String> = self
                .rooms
                .iter()
                .filter(|room| room.identity == "default")
                .map(|room| room.id.clone())
                .collect();
            self.rooms
                .retain(|room| !(&room.identity == id && default_rooms.contains(&room.id)));
            for room in &mut self.rooms {
                if &room.identity == id {
                    room.identity = "default".into();
                }
            }
            for event in &mut self.events {
                if &event.identity == id {
                    event.identity = "default".into();
                }
            }
            for operation in &mut self.operations {
                if &operation.identity == id {
                    operation.identity = "default".into();
                }
            }
            for policy in &mut self.policies {
                if &policy.identity == id {
                    policy.identity = "default".into();
                }
            }
            for session in &mut self.sessions {
                if &session.identity == id {
                    session.identity = "default".into();
                }
            }
            for record in &mut self.mcp_discovery {
                if &record.identity == id {
                    record.identity = "default".into();
                }
            }
            if !self.tag_rules.iter().any(|rule| rule.name.eq_ignore_ascii_case(tag)) {
                self.tag_rules.push(idfon_protocol::TagRule {
                    name: tag.clone(),
                    hide_presence: false,
                    hide_photo: false,
                });
            }
        }
        self.identities.retain(|identity| identity.id == "default");
        for identity in &mut self.identities {
            identity.active = true;
        }
        self.save(data_dir)
    }

    fn ensure_identity_keys(&mut self, data_dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(data_dir)?;
        for identity in &mut self.identities {
            let path = data_dir.join(format!("identity-{}.key", identity.id));
            let key = match std::fs::read_to_string(&path) {
                Ok(value) => idfon_core::IdentityKey::decode(value.trim()).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid identity key")
                })?,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    let key = idfon_core::IdentityKey::generate();
                    let value = key.encode();
                    std::fs::write(&path, value)?;
                    #[cfg(unix)]
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
                    key
                }
                Err(error) => return Err(error),
            };
            identity.public_key = Some(key.peer_id());
        }
        // Backfill the derived account handle for peers stored before it existed.
        for peer in &mut self.peers {
            ensure_account_alias(peer);
        }
        self.save(data_dir)
    }

    fn identity_key(&self, identity_id: &str) -> io::Result<idfon_core::IdentityKey> {
        identity_key_in(&self.data_dir, identity_id)
    }

    /// The transport (endpoint) key for an identity. Identities created since
    /// the account/endpoint split get a dedicated `endpoint-<id>.key` so the
    /// identity key stays signing-only; identities created before it fall back
    /// to the identity key, because switching their endpoint id would break
    /// every peer record that already points at it.
    fn endpoint_key(&self, identity_id: &str) -> io::Result<idfon_core::IdentityKey> {
        endpoint_key_in(&self.data_dir, identity_id)
    }

    /// The inbound MCP relay command: `IDFON_MCP_COMMAND` (per-process, keeps
    /// the C ABI unchanged) overrides the persisted `mcp.configure` value.
    fn configured_mcp_command(&self) -> Option<String> {
        idfon_config::get("IDFON_MCP_COMMAND")
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .or_else(|| {
                self.mcp_command
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
            })
    }

    fn save(&self, data_dir: &Path) -> io::Result<()> {
        let snapshot = serde_json::to_string(self).map_err(io::Error::other)?;
        let connection = Self::db(data_dir)?;
        connection
            .execute(
                "INSERT INTO state (id, json) VALUES (1, ?1) ON CONFLICT(id) DO UPDATE SET json = excluded.json",
                rusqlite::params![snapshot],
            )
            .map_err(io::Error::other)?;
        // Keep the JSON snapshot as a human-readable export during migration.
        let bytes = serde_json::to_vec_pretty(self).map_err(io::Error::other)?;
        let temporary = data_dir.join("state.json.tmp");
        std::fs::write(&temporary, bytes)?;
        std::fs::rename(temporary, data_dir.join("state.json"))
    }
}

/// Daemon configuration for [`run`]/[`run_blocking`]. `transport` defaults
/// to "iroh" when empty/None; the only other valid value is "fake".
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub socket: PathBuf,
    pub data_dir: PathBuf,
    pub transport: Option<String>,
    /// Exit gracefully after this long with zero connected clients.
    /// None (default) = run until ctrl_c or `daemon.shutdown`.
    pub idle_exit: Option<Duration>,
}

/// The feature set for in-process callers: the store the moment it opens,
/// the transport once the endpoints are bound, and the daemon's runtime —
/// request handling runs on it so spawned work (send delivery, gateways)
/// lands on the long-lived runtime, never on a caller's throwaway one.
#[derive(Clone)]
struct Live {
    store: Arc<Mutex<Store>>,
    transport: Option<Arc<TransportMode>>,
    runtime: tokio::runtime::Handle,
}

static LIVE: RwLock<Option<Live>> = RwLock::new(None);

/// The active identity's transport of the daemon running in this process,
/// once its endpoints are bound; `None` without one (or with the fake
/// transport). In-process packagings (iOS, and the mac app once it hosts the
/// daemon) put call media on this endpoint rather than one of their own: a
/// webhook-mode relay admits only known endpoint ids (idfon-edge
/// docs/relay-auth.md), and the identity endpoint is one.
pub async fn in_process_transport() -> Option<Arc<IrohTransport>> {
    let live = LIVE.read().ok()?.clone()?;
    let transport = live.transport?;
    let identity = active_identity(&live.store);
    transport.current_transport_async(&identity).await
}

/// Registers [`LIVE`] while a `run` is active and clears it on exit (panic
/// included), so `request_json` never serves a stopped runtime.
struct LiveGuard;

impl LiveGuard {
    fn install(store: Arc<Mutex<Store>>) -> Self {
        *LIVE.write().expect("live lock poisoned") = Some(Live {
            store,
            transport: None,
            runtime: tokio::runtime::Handle::current(),
        });
        Self
    }

    fn bind(&self, transport: Arc<TransportMode>) {
        if let Some(live) = LIVE.write().expect("live lock poisoned").as_mut() {
            live.transport = Some(transport);
        }
    }
}

impl Drop for LiveGuard {
    fn drop(&mut self) {
        *LIVE.write().expect("live lock poisoned") = None;
    }
}

/// In-process entry into the feature functions, for packagings that embed
/// them (the iOS app links this crate and calls directly — no socket, no
/// separate lifecycle). Same Request/Response contract as the RPC bridge
/// (`idfon-client`), so a target build only chooses its call path. Store-only
/// requests are served as soon as the store is open — display data never
/// waits for endpoints to bind. Returns `ErrorKind::NotConnected` before
/// that (callers retry, exactly like the bridge's connect error). Returns
/// the response frame plus `response.ok` (binary `*.compact` payloads cannot
/// fail and report `true` — the `idfon-client` convention). Must not be
/// called from inside an async runtime.
pub fn request_json(payload: &[u8]) -> io::Result<(Vec<u8>, bool)> {
    let body = match serde_json::from_slice::<Request>(payload) {
        Ok(request) => {
            let live = LIVE.read().expect("live lock poisoned").clone();
            let Some(live) = live else {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "feature set not started",
                ));
            };
            let mut session_identity = active_identity(&live.store);
            live.runtime.block_on(handle_request(
                request,
                &live.store,
                live.transport.as_ref(),
                &mut session_identity,
            ))?
        }
        Err(error) => encode_json(&error_response(
            "unknown".into(),
            "protocol",
            ErrorCode::InvalidJson,
            error.to_string(),
            false,
        ))
        .map_err(io::Error::other)?,
    };
    if body.len() > idfon_protocol::MAX_FRAME_BYTES {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame too large"));
    }
    // Same convention as `idfon-client`: a frame that is not a protocol
    // Response is a `*.compact` binary payload, which cannot fail.
    let ok = serde_json::from_slice::<Response>(&body)
        .map(|response| response.ok)
        .unwrap_or(true);
    Ok((body, ok))
}

/// Blocking entry point: builds its own tokio runtime and runs the daemon
/// until ctrl_c or an error. Used by the thin `idfond` binary through the
/// dylib's `idfon_daemon_run` C ABI.
pub fn run_blocking(config: DaemonConfig) -> io::Result<()> {
    // Install the shared subscriber unless the embedding app already did
    // (first call wins), so a CLI-spawned daemon's tracing reaches a log file
    // instead of the discarded stderr. `IDFON_LOG_FILE` picks the sink.
    idfon_telemetry::init("idfond", "info");
    // IDFON_IDLE_EXIT_SECS lets the thin `idfond` binary / CLI-spawned
    // daemons opt into idle shutdown without touching the C ABI. "0" = off.
    let env_idle_exit = idfon_config::get("IDFON_IDLE_EXIT_SECS")
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|&secs| secs > 0)
        .map(Duration::from_secs);
    let idle_exit = config
        .idle_exit
        .or(env_idle_exit)
        .filter(|duration| !duration.is_zero());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(io::Error::other)?;
    runtime.block_on(run(DaemonConfig {
        idle_exit,
        ..config
    }))
}

pub async fn run(config: DaemonConfig) -> io::Result<()> {
    let DaemonConfig {
        socket,
        data_dir,
        transport,
        idle_exit,
    } = config;
    let transport = transport.unwrap_or_else(|| DEFAULT_TRANSPORT.into());
    if transport != "fake" && transport != "iroh" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--transport must be fake or iroh",
        ));
    }
    let _data_lock = DataLock::acquire(&data_dir)?;
    build_info::record_start();
    // Chunked-upload staging is transient by definition; a crash can leave an
    // accumulator behind, so sweep it on every boot.
    let _ = std::fs::remove_dir_all(data_dir.join("staging"));
    // Keep the default identity's endpoint address stable across restarts so
    // paired apps do not need re-pairing. Extra identities stay ephemeral
    // (see TransportManager); a taken port falls back to ephemeral too.
    // The IDFON_ENDPOINT_PORT setting wins; else a port derived from the
    // data dir (stable per profile, different across profiles).
    let endpoint_port = idfon_config::get("IDFON_ENDPOINT_PORT")
        .and_then(|value| value.trim().parse::<u16>().ok())
        .or_else(|| {
            let hash = data_dir
                .to_string_lossy()
                .bytes()
                .fold(2_166_136_261u32, |acc, byte| {
                    (acc ^ byte as u32).wrapping_mul(16_777_619)
                });
            Some(59_000 + (hash % 900) as u16)
        });
    let store = Arc::new(Mutex::new(Store::load(&data_dir)?));
    // The feature set serves in-process callers the moment the store is open:
    // local reads (contacts, identities, rooms) never wait on endpoint binds.
    let live = LiveGuard::install(Arc::clone(&store));
    let media_service = Arc::new(MediaService::default());
    let _ = MEDIA_SERVICE.set((*media_service).clone());
    let default_endpoint_key = store
        .lock()
        .expect("store mutex poisoned")
        .endpoint_key("default")?;
    // Bind endpoints ASAP — concurrent with whatever the first requests
    // need, not serialized before them (the key material comes from the
    // profile files the store just loaded).
    let bind_name = transport.clone();
    let bound = tokio::spawn(async move {
        TransportMode::new(
            &bind_name,
            Some(default_endpoint_key.secret_bytes()),
            endpoint_port,
        )
        .await
    });
    let transport = Arc::new(bound.await.map_err(io::Error::other)??);
    live.bind(Arc::clone(&transport));
    // The endpoint key is separate from the identity (account) signing key for
    // identities created since the split; older ones fall back to the identity
    // key so their endpoint id — and peers' records — do not change.
    let bound_keys: Vec<(String, [u8; 32])> = {
        let state = store.lock().expect("store mutex poisoned");
        // D20: every identity is live at once — all endpoints bind at startup
        // (`IDFON_LAZY_IDENTITIES` no longer gates this).
        state
            .identities
            .iter()
            .filter_map(|identity| {
                state
                    .endpoint_key(&identity.id)
                    .ok()
                    .map(|key| (identity.id.clone(), key.secret_bytes()))
            })
            .collect()
    };
    for (identity, key) in bound_keys.iter().filter(|(id, _)| id != "default") {
        transport.add_identity(identity, *key).await?;
    }
    if let TransportMode::Iroh(manager) = transport.as_ref() {
        let mut state = store.lock().expect("store mutex poisoned");
        for identity in &mut state.identities {
            if let Some(endpoint_id) = manager.endpoint_id(&identity.id) {
                identity.endpoint_id = Some(endpoint_id);
            }
        }
        let data_dir = state.data_dir.clone();
        state.save(&data_dir)?;
    }
    // Per-identity inbound work (message receiver, MCP relay, gossip, rooms)
    // is wired for every identity: all are live at once (D20). The relay
    // reads its command per connection (env overrides the persisted
    // `mcp.configure` value), so runtime configuration needs no
    // re-registration. The env var keeps the C ABI unchanged (same precedent
    // as IDFON_IDLE_EXIT_SECS).
    for (identity, _) in &bound_keys {
        wire_identity(Arc::clone(&transport), Arc::clone(&store), identity.clone());
        wire_sync_identity(Arc::clone(&transport), Arc::clone(&store), identity.clone()).await;
    }
    resume_pending_operations(Arc::clone(&store), Arc::clone(&transport));
    // Relay auto-enrollment (idfon-edge/docs/relay-auth.md §6): keeps every identity and
    // peer device endpoint admitted at the relay's auth service. No-op unless
    // IDFON_RELAY_ENROLL_URL is set.
    relay_auth::spawn(Arc::clone(&store));
    prepare_socket(&socket)?;
    if let Some(parent) = socket.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let listener = UnixListener::bind(&socket)?;
    #[cfg(unix)]
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    let _cleanup = SocketCleanup(socket.clone());

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let connections = Arc::new(AtomicUsize::new(0));
    if let Some(idle_exit) = idle_exit {
        let connections = Arc::clone(&connections);
        let shutdown = Arc::clone(&shutdown);
        tokio::spawn(async move {
            let mut idle_since: Option<tokio::time::Instant> = None;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if connections.load(Ordering::Relaxed) > 0 {
                    idle_since = None;
                } else {
                    let since = *idle_since.get_or_insert_with(tokio::time::Instant::now);
                    if since.elapsed() >= idle_exit {
                        tracing::info!(target: "idfon.daemon", idle_exit = ?idle_exit, "no connected clients; shutting down");
                        shutdown.notify_waiters();
                        return;
                    }
                }
            }
        });
    }
    tokio::select! {
        result = accept_loop(listener, store, transport, Arc::clone(&shutdown), Arc::clone(&connections)) => result,
        _ = shutdown.notified() => Ok(()),
        result = tokio::signal::ctrl_c() => result.map_err(io::Error::other),
    }
}

async fn accept_loop(
    listener: UnixListener,
    store: Arc<Mutex<Store>>,
    transport: Arc<TransportMode>,
    shutdown: Arc<tokio::sync::Notify>,
    connections: Arc<AtomicUsize>,
) -> io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let store = Arc::clone(&store);
        let transport = Arc::clone(&transport);
        let shutdown = Arc::clone(&shutdown);
        let connections = Arc::clone(&connections);
        connections.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            // Counts as an active client for the whole connection, which is
            // what long-poll (`wait`) and `events --follow` hold open.
            let _conn = ConnGuard(connections);
            let _client = build_info::ClientGuard::new();
            if let Err(error) = serve(stream, store, transport, shutdown).await {
                tracing::warn!(target: "idfon.daemon", error = %error, "client error");
            }
        });
    }
}

struct ConnGuard(Arc<AtomicUsize>);

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn serve(
    mut stream: UnixStream,
    store: Arc<Mutex<Store>>,
    transport: Arc<TransportMode>,
    shutdown: Arc<tokio::sync::Notify>,
) -> io::Result<()> {
    let mut session_identity = active_identity(&store);
    loop {
        let Some(frame) = read_frame(&mut stream).await? else {
            return Ok(());
        };
        let mut request = match serde_json::from_slice::<Request>(&frame) {
            Ok(request) => request,
            Err(error) => {
                let response = error_response(
                    "unknown".into(),
                    "protocol",
                    ErrorCode::InvalidJson,
                    error.to_string(),
                    false,
                );
                write_frame(
                    &mut stream,
                    &encode_json(&response).map_err(io::Error::other)?,
                )
                .await?;
                continue;
            }
        };
        with_session_identity(&mut request, &session_identity);
        if request.method == "event.list"
            && request
                .params
                .get("follow")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
        {
            let mut follow_request = request;
            loop {
                let response =
                    dispatch_with_transport(follow_request.clone(), &store, Some(&transport));
                if let ResponseBody::Success { result, .. } = &response.body {
                    if let Some(last) = result
                        .get("events")
                        .and_then(serde_json::Value::as_array)
                        .and_then(|events| events.last())
                        .and_then(|event| event.get("cursor"))
                        .cloned()
                    {
                        if let Some(params) = follow_request.params.as_object_mut() {
                            params.insert("after".into(), last);
                        }
                    }
                }
                write_frame(
                    &mut stream,
                    &encode_json(&response).map_err(io::Error::other)?,
                )
                .await?;
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        }
        if request.method == "daemon.shutdown" {
            let response = success(&request, serde_json::json!({"shutdown": true}));
            write_frame(
                &mut stream,
                &encode_json(&response).map_err(io::Error::other)?,
            )
            .await?;
            shutdown.notify_waiters();
            return Ok(());
        }
        let payload =
            handle_request(request, &store, Some(&transport), &mut session_identity).await?;
        write_frame(&mut stream, &payload).await?
    }
}

/// Per-request canonicalization (`rpc::normalize_request`: legacy method
/// names and the `peer` param) and identity defaulting: an explicit
/// `identity` param wins,
/// otherwise the session identity fills both (`__identity` carries the
/// resolved value for handlers). Idempotent.
fn with_session_identity(request: &mut Request, session_identity: &str) {
    idfon_protocol::rpc::normalize_request(request);
    if let Some(params) = request.params.as_object_mut() {
        for key in ["identity", "__identity"] {
            // An explicit `null` counts as absent, like a missing key.
            if params.get(key).is_none_or(serde_json::Value::is_null) {
                params.insert(
                    key.into(),
                    serde_json::Value::String(session_identity.to_owned()),
                );
            }
        }
    }
}

/// The persisted active identity (`identity use` survives restarts, so every
/// connection — and every in-process request — starts from it); an explicit
/// `--identity` param still overrides per request.
fn active_identity(store: &Arc<Mutex<Store>>) -> String {
    let state = store.lock().expect("store mutex poisoned");
    state
        .identities
        .iter()
        .find(|identity| identity.active)
        .map(|identity| identity.id.clone())
        .unwrap_or_else(|| String::from("default"))
}

/// One request through the feature pipeline — identity defaulting, the
/// `wait` long-poll, the compact payloads, the gateway/provider/web methods,
/// and `dispatch_with_transport` for everything else — returning the encoded
/// response frame. This is the whole feature layer, shared by the two
/// packagings: `idfond` serves it over the unix socket (`serve`) and an
/// embedder calls it in-process (`request_json`). `transport` is optional by
/// design: store-only functions run without one, so local data is never
/// gated on endpoints binding. `events --follow` (multi-frame stream) and
/// `daemon.shutdown` (connection teardown) stay in `serve` — they act on the
/// connection, not on the request.
async fn handle_request(
    mut request: Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
    session_identity: &mut String,
) -> io::Result<Vec<u8>> {
    with_session_identity(&mut request, session_identity);
    if request.method == "event.wait" {
        let timeout = request
            .params
            .get("timeout_ms")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(30_000)
            .min(300_000);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(timeout);
        loop {
            let response = dispatch_with_transport(request.clone(), store, transport);
            let found = matches!(&response.body, ResponseBody::Success { result, .. } if result.get("events").and_then(serde_json::Value::as_array).is_some_and(|events| !events.is_empty()));
            if found || tokio::time::Instant::now() >= deadline {
                return encode_json(&response).map_err(io::Error::other);
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
    if request.method == "peer.list.compact" {
        return Ok(compact_peers(
            store,
            request_text(&request.params, "identity").as_deref(),
        ));
    }
    if request.method == "identity.list.compact" {
        return Ok(compact_identities(store));
    }
    if request.method == "event.list.compact" {
        let after = request_text(&request.params, "after");
        return Ok(compact_events(
            store,
            after.as_deref(),
            request_text(&request.params, "identity").as_deref(),
        ));
    }
    if request.method == "gateway.start" {
        let identity = request_text(&request.params, "identity")
            .unwrap_or_else(|| session_identity.clone());
        let response = gateway_start(&request, store, transport, &identity).await;
        return encode_json(&response).map_err(io::Error::other);
    }
    if request.method == "gateway.stop" {
        let identity = request_text(&request.params, "identity")
            .unwrap_or_else(|| session_identity.clone());
        return encode_json(&gateway_stop(&request, &identity)).map_err(io::Error::other);
    }
    if request.method == "web.url" {
        return encode_json(&web_url(&request)).map_err(io::Error::other);
    }
    if request.method == "file.publish" {
        return encode_json(&files_publish(&request).await).map_err(io::Error::other);
    }
    if request.method == "provider.start" {
        let identity = request_text(&request.params, "identity")
            .unwrap_or_else(|| session_identity.clone());
        let response = provider_start(&request, store, transport, &identity).await;
        return encode_json(&response).map_err(io::Error::other);
    }
    if request.method == "provider.stop" {
        let identity = request_text(&request.params, "identity")
            .unwrap_or_else(|| session_identity.clone());
        return encode_json(&provider_stop(&request, &identity)).map_err(io::Error::other);
    }
    let response = dispatch_with_transport(request.clone(), store, transport);
    if request.method == "identity.use" && response.ok {
        if let Some(identity) = request_text(&request.params, "name") {
            *session_identity = identity;
        }
    }
    encode_json(&response).map_err(io::Error::other)
}

async fn wire_sync_identity(
    transport: Arc<TransportMode>,
    store: Arc<Mutex<Store>>,
    identity: String,
) {
    let TransportMode::Iroh(manager) = transport.as_ref() else {
        return;
    };
    let Some(endpoint) = manager.current(&identity).await else {
        return;
    };
    let (tx, mut rx) = mpsc::channel::<Connection>(16);
    let guard = endpoint.add_side_channel(SYNC_ALPN, tx);
    SYNC_GUARDS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .expect("sync guards poisoned")
        .push(guard);
    tokio::spawn(async move {
        while let Some(connection) = rx.recv().await {
            let store = Arc::clone(&store);
            let identity = identity.clone();
            tokio::spawn(async move {
                let result = async {
                    let remote = connection.remote_id().to_string();
                    let (mut send, mut recv) =
                        connection.accept_bi().await.map_err(io::Error::other)?;
                    let bytes = recv
                        .read_to_end(1024 * 1024)
                        .await
                        .map_err(io::Error::other)?;
                    let envelope: idfon_protocol::StateSyncEnvelope =
                        serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                    idfon_core::verify_state_sync(&envelope).map_err(io::Error::other)?;
                    let accepted = {
                        let state = store.lock().expect("store mutex poisoned");
                        state.peers.iter().any(|peer| {
                            peer.identity == identity
                                && peer.id == envelope.account_id
                                && peer.knows_endpoint(&remote)
                        })
                    };
                    if !accepted {
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "sync endpoint is not an enrolled account device",
                        ));
                    }
                    let added = merge_state_events(&store, &envelope)?;
                    let response = serde_json::to_vec(&serde_json::json!({"added": added}))
                        .map_err(io::Error::other)?;
                    send.write_all(&response).await.map_err(io::Error::other)?;
                    send.finish().map_err(io::Error::other)?;
                    Ok::<(), io::Error>(())
                }
                .await;
                if let Err(error) = result {
                    connection.close(1u32.into(), error.to_string().as_bytes());
                }
            });
        }
    });
}

fn merge_state_events(
    store: &Arc<Mutex<Store>>,
    envelope: &idfon_protocol::StateSyncEnvelope,
) -> io::Result<usize> {
    let mut state = store.lock().expect("store mutex poisoned");
    let mut added = 0;
    for event in &envelope.events {
        if state
            .events
            .iter()
            .any(|existing| existing.event_id == event.event_id)
        {
            continue;
        }
        apply_state_event(&mut state, event)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid state event"))?;
        state.events.push(event.clone());
        added += 1;
    }
    if added > 0 {
        state.events.sort_by(|a, b| a.event_id.cmp(&b.event_id));
    }
    let data_dir = state.data_dir.clone();
    state.save(&data_dir)?;
    if added > 0 {
        // Synced peer/identity changes can add or drop endpoints to admit
        // (idfon-edge/docs/relay-auth.md §6).
        relay_auth::poke();
    }
    Ok(added)
}

/// How a send ends: delivered, or — when every attempt failed — back in the
/// outbox (`Queued`, "not sent yet") for a durable send, `Failed` otherwise.
fn final_send_status(delivered: bool, durable: bool) -> idfon_protocol::OperationStatus {
    match (delivered, durable) {
        (true, _) => idfon_protocol::OperationStatus::Delivered,
        (false, true) => idfon_protocol::OperationStatus::Queued,
        (false, false) => idfon_protocol::OperationStatus::Failed,
    }
}

/// Startup: retry everything a previous process left queued or mid-send.
fn resume_pending_operations(store: Arc<Mutex<Store>>, transport: Arc<TransportMode>) {
    resume_queued(&store, &transport, None, true, 4);
}

/// Re-attempt the outbox: queued sends (only `target`'s when given; `stale`
/// also takes sends a previous process left `Transmitting`). Each one is
/// claimed (→ `Transmitting`) under the store lock before any send, so
/// overlapping triggers — app foreground, a message from the peer, a path
/// change — never send one message twice. Returns how many were claimed.
fn resume_queued(
    store: &Arc<Mutex<Store>>,
    transport: &Arc<TransportMode>,
    target: Option<&str>,
    stale: bool,
    attempts: usize,
) -> usize {
    let claimed = {
        let mut state = store.lock().expect("store mutex poisoned");
        let ids: Vec<(String, String)> = state
            .operations
            .iter()
            .filter(|operation| {
                let waiting = operation.status == idfon_protocol::OperationStatus::Queued
                    || (stale && operation.status == idfon_protocol::OperationStatus::Transmitting);
                waiting
                    && operation.outbound.is_some()
                    && target.is_none_or(|target| operation.target.as_deref() == Some(target))
            })
            .map(|operation| (operation.operation_id.clone(), operation.identity.clone()))
            .collect();
        let mut claimed = Vec::new();
        for (operation_id, identity) in ids {
            set_operation_status(
                &mut state,
                &operation_id,
                &identity,
                idfon_protocol::OperationStatus::Transmitting,
            );
            if let Some(operation) = state
                .operations
                .iter()
                .find(|operation| operation.operation_id == operation_id && operation.identity == identity)
            {
                claimed.push(operation.clone());
            }
        }
        if !claimed.is_empty() {
            let data_dir = state.data_dir.clone();
            let _ = state.save(&data_dir);
        }
        claimed
    };
    let count = claimed.len();
    for operation in claimed {
        let store = Arc::clone(store);
        let transport = Arc::clone(transport);
        tokio::spawn(async move {
            let Some(envelope) = operation.outbound.clone() else {
                return;
            };
            let peer = {
                let state = store.lock().expect("store mutex poisoned");
                operation
                    .target
                    .as_deref()
                    .and_then(|target| {
                        state
                            .peers
                            .iter()
                            .find(|peer| peer.identity == operation.identity && peer.id == target)
                    })
                    .cloned()
            };
            // The peer is gone (deleted): nothing will ever deliver this.
            let Some(peer) = peer else {
                let _ = update_operation(
                    &store,
                    &operation.operation_id,
                    &operation.identity,
                    idfon_protocol::OperationStatus::Failed,
                );
                return;
            };
            let mut result = Err(io::Error::other("no transport attempt"));
            for attempt in 0..attempts.max(1) {
                if is_cancelled(&store, &operation.operation_id, &operation.identity) {
                    return;
                }
                let transport = Arc::clone(&transport);
                let identity = operation.identity.clone();
                let peer = peer.clone();
                let envelope = envelope.clone();
                let policy = operation.delivery.clone();
                result = tokio::task::spawn_blocking(move || {
                    transport.send(&identity, &peer, &envelope, policy.as_ref())
                })
                .await
                .unwrap_or_else(|error| Err(io::Error::other(error)));
                if result.is_ok() {
                    break;
                }
                if attempt + 1 < attempts {
                    tokio::time::sleep(std::time::Duration::from_millis(500 << attempt.min(3))).await;
                }
            }
            let status = final_send_status(result.is_ok(), operation.durable);
            tracing::info!(target: "idfon.daemon", identity = %operation.identity, operation_id = %operation.operation_id, status = ?status, "outbox resume finished");
            let _ = update_operation(&store, &operation.operation_id, &operation.identity, status);
        });
    }
    count
}

/// `operation.resume` — retry the outbox now (all peers, or `target`'s). The
/// app calls it on its own reachability signals (foreground, network change,
/// opening a chat); the daemon also retries a peer's sends when that peer
/// messages us.
fn operation_resume(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
) -> Response {
    let Some(transport) = transport else {
        return transport_not_ready(request);
    };
    let target = request_text(&request.params, "target").map(|target| {
        let state = store.lock().expect("store mutex poisoned");
        state
            .peers
            .iter()
            .find(|peer| peer.id == target || peer.name == target || peer.aliases.iter().any(|alias| alias == &target))
            .map(|peer| peer.id.clone())
            .unwrap_or(target)
    });
    let resumed = resume_queued(store, transport, target.as_deref(), false, 2);
    success(request, serde_json::json!({"resumed": resumed}))
}

fn spawn_receiver(
    manager: Arc<idfon_core::transport::TransportManager>,
    store: Arc<Mutex<Store>>,
    identity: String,
    transport: Arc<TransportMode>,
) {
    tokio::spawn(async move {
        let receiver_identity = identity.clone();
        let result = manager
            .serve_with_remote_addr(&identity, move |message, remote| {
                let store = Arc::clone(&store);
                let identity = receiver_identity.clone();
                let transport = Arc::clone(&transport);
                async move {
                    // A message from a peer proves it is reachable: retry
                    // whatever is waiting for it in the outbox.
                    let sender = message.sender.peer_id.clone();
                    // The connection's view of the sender's address, kept
                    // with a contact request so accepting can dial back. Only
                    // when the authenticated remote is the claimed endpoint.
                    let remote_addr = (remote.id.to_string() == message.sender.endpoint_id)
                        .then(|| serde_json::to_string(&remote).ok())
                        .flatten();
                    let mut params = serde_json::to_value(message).map_err(|error| {
                        idfon_core::transport::TransportError::Failed(error.to_string())
                    })?;
                    params["identity"] = serde_json::Value::String(identity);
                    if let Some(remote_addr) = remote_addr {
                        params[REMOTE_ADDR_PARAM] = serde_json::Value::String(remote_addr);
                    }
                    let request = Request {
                        version: PROTOCOL_VERSION,
                        id: format!(
                            "transport-{}",
                            params["message_id"].as_str().unwrap_or("unknown")
                        ),
                        method: "message.receive".into(),
                        params,
                    };
                    let response = dispatch(request, &store);
                    if response.ok && !sender.is_empty() {
                        resume_queued(&store, &transport, Some(&sender), false, 2);
                    }
                    match response.body {
                        ResponseBody::Success { result, .. } => Ok(idfon_protocol::MessageAck {
                            message_id: result["message_id"].as_str().unwrap_or_default().into(),
                            status: idfon_protocol::AckStatus::Accepted,
                        }),
                        ResponseBody::Failure { error, .. } => {
                            Err(idfon_core::transport::TransportError::Failed(error.message))
                        }
                    }
                }
            })
            .await;
        if let Err(error) = result {
            tracing::warn!(target: "idfon.daemon", identity = %identity, error = %error, "iroh receiver stopped");
        }
    });
}

/// Wires the per-identity background work for an already-bound transport:
/// message receiver, MCP inbound relay, gossip, and rooms. Called at startup
/// for the bound set and from `identity.use` when a lazily-bound identity
/// comes online; a no-op for the fake transport.
fn wire_identity(transport: Arc<TransportMode>, store: Arc<Mutex<Store>>, identity: String) {
    let TransportMode::Iroh(manager) = transport.as_ref() else {
        return;
    };
    room_calls::set_live_transport(&store, Arc::clone(&transport));
    // Group-call clock: refreshes and expiry, one ticker per daemon.
    static ROOM_CALL_TICKER: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if !ROOM_CALL_TICKER.swap(true, std::sync::atomic::Ordering::Relaxed) {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                interval.tick().await;
                let store = Arc::clone(&store);
                let _ = tokio::task::spawn_blocking(move || room_calls::tick_all(&store)).await;
            }
        });
    }
    let manager = Arc::clone(manager);
    spawn_receiver(Arc::clone(&manager), Arc::clone(&store), identity.clone(), Arc::clone(&transport));
    let mcp_manager = Arc::clone(&manager);
    let mcp_store = Arc::clone(&store);
    let mcp_identity = identity.clone();
    tokio::spawn(async move {
        mcp::spawn_inbound(mcp_manager, mcp_store, mcp_identity).await;
    });
    let gossip_manager = Arc::clone(&manager);
    tokio::spawn(async move {
        if let Some(current) = gossip_manager.current(&identity).await {
            current.enable_gossip();
        }
        let rooms = store
            .lock()
            .expect("store mutex poisoned")
            .rooms
            .iter()
            .filter(|room| room.identity == identity)
            .cloned()
            .collect::<Vec<_>>();
        for room in rooms {
            let store = Arc::clone(&store);
            let transport = Arc::clone(&transport);
            tokio::spawn(async move {
                join_gossip_room(room.identity, room.id, room.members, store, transport).await;
            });
        }
    });
}

/// Resolves the request's `identity` param to the stored identity id (same
/// lookup as message.send; falls back to the raw ref).
fn resolved_identity_id(request: &Request, store: &Arc<Mutex<Store>>) -> String {
    let identity_ref =
        request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let state = store.lock().expect("store mutex poisoned");
    state
        .identities
        .iter()
        .find(|identity| identity.id == identity_ref || identity.name == identity_ref)
        .map(|identity| identity.id.clone())
        .unwrap_or(identity_ref)
}

/// Resolves the dial target peer (MessageSend grant enforced, matching the
/// message path) and hands the serialized endpoint address to the live dial
/// handler.
fn live_dial_dispatch(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(to) = request_text(&request.params, "to").filter(|value| !value.is_empty()) else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "to is required (peer reference)".into(),
            false,
        );
    };
    let state = store.lock().expect("store mutex poisoned");
    let identity_id = send_identity_id(&state, request, &to);
    let Some(peer) = state.peers.iter().find(|peer| {
        peer.identity == identity_id
            && (peer.id == to
                || peer.name == to
                || peer.aliases.iter().any(|alias| alias == &to)
                || peer.knows_endpoint(&to))
    }) else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "peer not found".into(),
            false,
        );
    };
    let has_grant = state.grants.iter().any(|grant| {
        grant.identity == identity_id
            && grant.subject == peer.id
            && grant.capability == idfon_protocol::Capability::MessageSend
            && grant.revoked_at.is_none()
            && grant
                .expires_at
                .as_deref()
                .is_none_or(|expires| expires > now().as_str())
    });
    if !has_grant {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::CapabilityDenied,
            "media.live.dial requires a message.send grant to the peer".into(),
            false,
        );
    }
    let delivery = request
        .params
        .get("delivery")
        .cloned()
        .and_then(|value| serde_json::from_value::<idfon_protocol::DeliveryPolicy>(value).ok());
    if matches!(
        delivery.as_ref().map(|policy| &policy.mode),
        Some(idfon_protocol::DeliveryMode::All)
    ) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "live calls require one endpoint or failover, not fan-out".into(),
            false,
        );
    }
    let targets = peer.dial_targets_with(delivery.as_ref());
    if targets.is_empty() {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "peer has no endpoint address".into(),
            false,
        );
    }
    drop(state);
    let mut last = None;
    for (_, address) in targets {
        let response = live::live_dial(request, &address);
        if response.ok {
            return response;
        }
        last = Some(response);
    }
    last.expect("dial targets is non-empty")
}

fn contact_ticket_value(
    state: &Store,
    transport: Option<&Arc<TransportMode>>,
    identity_id: &str,
) -> Option<idfon_protocol::ContactTicket> {
    let identity = state
        .identities
        .iter()
        .find(|identity| identity.id == identity_id)?;
    let endpoint_id = identity.endpoint_id.clone()?;
    let endpoint_addr = String::from_utf8(transport?.endpoint_ticket_for(identity_id)?).ok()?;
    Some(idfon_protocol::ContactTicket {
        version: 1,
        account_id: identity.public_key.clone()?,
        endpoint_id,
        endpoint_addr,
        label: Some(identity.name.clone()),
        device_class: None,
        capabilities: vec!["chat".into(), "live_audio".into(), "live_video".into()],
        share: identity.share.clone(),
    })
}

/// A saved peer's bare card (what `contact.export {ref}` and
/// `contact.introduce` share): id, address, name and the peer's sharing
/// preference; never capabilities. `None` without a dial address.
fn peer_card(peer: &idfon_protocol::Peer) -> Option<idfon_protocol::ContactTicket> {
    Some(idfon_protocol::ContactTicket {
        version: 1,
        account_id: peer.id.clone(),
        endpoint_id: peer.endpoint_id.clone()?,
        endpoint_addr: peer.endpoint_addr.clone()?,
        label: Some(peer.name.clone()),
        device_class: None,
        capabilities: vec![],
        share: peer.share.clone(),
    })
}

/// `contact.export`: a vCard 3.0 card carrying this identity's contact ticket,
/// or a saved peer's (`ref`). The card embeds the ticket twice —
/// `X-IDFON-TICKET` and the `idfon://add/<base64url>` deep link that survives
/// an AirDrop round-trip through the Contacts app (docs/contact-exchange.md).
fn contact_export(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
) -> Response {
    let state = store.lock().expect("store mutex poisoned");
    let ticket = match request_text(&request.params, "ref") {
        None => {
            // An identity's own card: `identity` names the line by id or name
            // (like `status`), defaulting to `default`.
            let identity_ref =
                request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
            let bound = state
                .identities
                .iter()
                .find(|identity| {
                    identity.id == identity_ref || identity.name == identity_ref
                })
                .and_then(|identity| contact_ticket_value(&state, transport, &identity.id));
            match bound {
                Some(ticket) => ticket,
                None => {
                    return error_response(
                        request.id.clone(),
                        &request.method,
                        ErrorCode::InvalidRequest,
                        "identity has no bound endpoint".into(),
                        false,
                    )
                }
            }
        }
        Some(reference) => {
            let peer = state.peers.iter().find(|peer| {
                peer.id == reference
                    || peer.name == reference
                    || peer.aliases.iter().any(|alias| *alias == reference)
                    || peer.knows_endpoint(&reference)
            });
            let Some(card) = peer.and_then(peer_card) else {
                return error_response(
                    request.id.clone(),
                    &request.method,
                    ErrorCode::InvalidRequest,
                    "peer has no endpoint address".into(),
                    false,
                );
            };
            card
        }
    };
    drop(state);
    success(
        request,
        serde_json::json!({
            "vcard": idfon_protocol::vcf::contact_ticket_vcard(&ticket),
            "link": idfon_protocol::vcf::contact_ticket_link(&ticket),
        }),
    )
}

/// `contact.import`: adds every idfon card in a vCard file as a peer (the
/// existing `peer.add` upsert per card; a card that fails validation aborts
/// the import, and re-importing is idempotent). Cards without a ticket are
/// skipped by the parser.
fn contact_import(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
) -> Response {
    let Some(text) = request_text(&request.params, "text") else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "text is required".into(),
            false,
        );
    };
    let links = match idfon_protocol::vcf::vcard_add_links(&text) {
        Ok(links) => links,
        Err(message) => {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                message,
                false,
            )
        }
    };
    let mut peers = Vec::new();
    for link in links {
        let mut params = serde_json::json!({"contact_ticket": link.contact});
        if let Some(intro) = link.intro {
            params["intro"] = serde_json::to_value(intro).expect("introduction serializes");
        }
        if let Some(identity) = request_text(&request.params, "identity") {
            params["identity"] = identity.into();
        }
        let call = Request {
            version: PROTOCOL_VERSION,
            id: request.id.clone(),
            method: "peer.add".into(),
            params,
        };
        let response = peer_add(&call, store, transport);
        let ResponseBody::Success { result, .. } = response.body else {
            return response;
        };
        peers.push(result["peer"].clone());
    }
    success(request, serde_json::json!({"peers": peers}))
}

fn contact_ticket(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
) -> Response {
    let Some(transport) = transport else {
        return transport_not_ready(request);
    };
    let state = store.lock().expect("store mutex poisoned");
    let identity = request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    match contact_ticket_value(&state, Some(transport), &identity) {
        Some(ticket) => success(
            request,
            serde_json::to_value(ticket).expect("contact ticket serializes"),
        ),
        None => error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "identity has no bound endpoint".into(),
            false,
        ),
    }
}

/// One loopback gateway per identity, started on demand by `gateway.start`.
struct GatewayInstance {
    addr: std::net::SocketAddr,
    token: String,
    /// Routing mode the instance was built with (`direct`/`edge`/`auto`).
    prefer: String,
    // Dropped (and thus aborted) with the registry entry.
    _handle: GatewayHandle,
}

static GATEWAYS: OnceLock<Mutex<HashMap<String, GatewayInstance>>> = OnceLock::new();

fn gateways() -> &'static Mutex<HashMap<String, GatewayInstance>> {
    GATEWAYS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Direct-vs-edge routing for the loopback gateway. `Auto` is direct-first with
/// an edge fallback when `IDFON_EDGE_URL` is set (P2 hybrid).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Prefer {
    Direct,
    Edge,
    Auto,
}

impl Prefer {
    fn parse(value: Option<&str>) -> Self {
        match value.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            Some("direct") => Self::Direct,
            Some("edge") => Self::Edge,
            _ => Self::Auto,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Edge => "edge",
            Self::Auto => "auto",
        }
    }
}

/// The gateway's backend, chosen by [`Prefer`]. A concrete enum because
/// `Backend`'s `async fn` is not object-safe.
enum GatewayBackend {
    Direct(IrohBackend),
    Edge(EdgeBackend),
    Auto(FallbackBackend<IrohBackend, EdgeBackend>),
}

impl Backend for GatewayBackend {
    async fn fetch(
        &self,
        caller: &Caller,
        account: &str,
        path: &str,
    ) -> Result<Resource, GatewayError> {
        match self {
            Self::Direct(backend) => backend.fetch(caller, account, path).await,
            Self::Edge(backend) => backend.fetch(caller, account, path).await,
            Self::Auto(backend) => backend.fetch(caller, account, path).await,
        }
    }
}

/// Edge base URL + opaque credentials from the environment. An empty URL means
/// no edge leg.
fn edge_config() -> (Option<String>, Vec<(String, String)>) {
    let base = idfon_config::get("IDFON_EDGE_URL");
    let mut headers = Vec::new();
    if let Some(token) = idfon_config::get("IDFON_EDGE_TOKEN") {
        headers.push(("Authorization".to_owned(), format!("Bearer {token}")));
    }
    if let Some(ticket) = idfon_config::get("IDFON_EDGE_TICKET") {
        headers.push(("x-idfon-ticket".to_owned(), ticket));
    }
    (base, headers)
}

/// Builds the gateway backend for `prefer`. `Edge` without `IDFON_EDGE_URL` is
/// an operator error; `Auto` degrades to direct.
fn gateway_backend(direct: IrohBackend, prefer: Prefer) -> Result<GatewayBackend, String> {
    let (base, headers) = edge_config();
    match prefer {
        Prefer::Direct => Ok(GatewayBackend::Direct(direct)),
        Prefer::Edge => {
            let base = base.ok_or("--prefer edge requires IDFON_EDGE_URL")?;
            let edge = EdgeBackend::new(base, headers).map_err(|error| error.to_string())?;
            Ok(GatewayBackend::Edge(edge))
        }
        Prefer::Auto => match base {
            Some(base) => {
                let edge = EdgeBackend::new(base, headers).map_err(|error| error.to_string())?;
                Ok(GatewayBackend::Auto(FallbackBackend::new(direct, edge)))
            }
            None => Ok(GatewayBackend::Direct(direct)),
        },
    }
}

/// Resolves an `idfon://` account ref against the peer store, exactly as
/// `peer.show` and message send do: id, name, alias, or endpoint id. Uses the
/// peer's dial target, so multi-device routing is preserved.
struct PeerStoreResolver {
    store: Arc<Mutex<Store>>,
    identity: String,
}

impl AccountResolver for PeerStoreResolver {
    fn resolve(&self, account: &str) -> Option<EndpointAddr> {
        let state = self.store.lock().expect("store mutex poisoned");
        let peer = state.peers.iter().find(|peer| {
            peer.identity == self.identity
                && (peer.id == account
                    || peer.name == account
                    || peer.aliases.iter().any(|alias| alias == account)
                    || peer.knows_endpoint(account))
        })?;
        let (_, address) = peer.dial_targets().into_iter().next()?;
        serde_json::from_str(&address).ok()
    }
}

/// The loopback gateway is bearer-token gated; the token is the boundary and is
/// only handed out over the 0600 daemon socket.
struct TokenOnly;

impl Authorizer for TokenOnly {
    fn authorize(&self, caller: &Caller, account: &str, path: &str) -> bool {
        caller
            .scope
            .as_ref()
            .is_none_or(|scope| scope.allows(account, path))
    }
}

/// Binds a loopback gateway for `identity` using its live iroh transport.
///
/// ponytail: the endpoint is captured at start, so `transport.rebind` leaves it
/// stale; restart the gateway after a rebind once anything rebinds in practice.
async fn start_gateway(
    store: Arc<Mutex<Store>>,
    identity: &str,
    transport: &Arc<IrohTransport>,
    prefer: Prefer,
) -> io::Result<GatewayInstance> {
    let direct = IrohBackend::new(
        transport,
        PeerStoreResolver {
            store,
            identity: identity.to_owned(),
        },
    )
    .map_err(io::Error::other)?;
    let backend = gateway_backend(direct, prefer).map_err(io::Error::other)?;
    let token = idfon_core::IdentityKey::generate().encode();
    let handle = idfon_gateway::serve(
        GatewayConfig {
            auth: Some(std::sync::Arc::new(StaticToken::new(token.clone()))),
            strict_origin: true,
            ..GatewayConfig::default()
        },
        backend,
        TokenOnly,
    )
    .await?;
    Ok(GatewayInstance {
        addr: handle.local_addr(),
        token,
        prefer: prefer.as_str().to_owned(),
        _handle: handle,
    })
}

fn scoped_gateway_token(secret: &str, account: &str, path: &str) -> Option<String> {
    let path_prefix = resource_scope_path(path)?;
    let expires_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs()
        .saturating_add(3600);
    issue_scoped_token(secret, account, &path_prefix, expires_at)
}

fn gateway_value(
    identity: &str,
    addr: std::net::SocketAddr,
    token: &str,
    prefer: &str,
) -> serde_json::Value {
    serde_json::json!({
        "identity": identity,
        "addr": addr.to_string(),
        "url": format!("http://127.0.0.1:{}/", addr.port()),
        "token": token,
        "prefer": prefer,
    })
}

/// Idempotently starts the identity's gateway and returns its address + token.
async fn gateway_start(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
    identity: &str,
) -> Response {
    let Some(transport) = transport else {
        return transport_not_ready(request);
    };
    let scoped_to = match (
        request_text(&request.params, "account"),
        request_text(&request.params, "path"),
    ) {
        (None, None) => None,
        (Some(account), Some(path)) if !account.is_empty() => Some((account, path)),
        _ => {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                "gateway.start account and path must be supplied together".into(),
                false,
            )
        }
    };
    if scoped_to
        .as_ref()
        .is_some_and(|(_, path)| resource_scope_path(path).is_none())
    {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "gateway.start path must be a safe absolute resource path".into(),
            false,
        );
    }
    let prefer = Prefer::parse(request_text(&request.params, "prefer").as_deref());
    if let Some(instance) = gateways()
        .lock()
        .expect("gateway registry poisoned")
        .get(identity)
    {
        if instance.prefer == prefer.as_str() {
            let token = scoped_to.as_ref().map_or_else(
                || Some(instance.token.clone()),
                |(account, path)| scoped_gateway_token(&instance.token, account, path),
            );
            let Some(token) = token else {
                return error_response(
                    request.id.clone(),
                    &request.method,
                    ErrorCode::InvalidRequest,
                    "could not issue a scoped gateway credential".into(),
                    false,
                );
            };
            return success(
                request,
                gateway_value(identity, instance.addr, &token, &instance.prefer),
            );
        }
    }
    // A different preference rebuilds the backend; drop the stale instance.
    gateways()
        .lock()
        .expect("gateway registry poisoned")
        .remove(identity);
    let Some(iroh) = transport.current_transport_async(identity).await else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            format!("identity {identity} has no iroh transport"),
            false,
        );
    };
    match start_gateway(Arc::clone(store), identity, &iroh, prefer).await {
        Ok(instance) => {
            let token = scoped_to.as_ref().map_or_else(
                || Some(instance.token.clone()),
                |(account, path)| scoped_gateway_token(&instance.token, account, path),
            );
            let Some(token) = token else {
                return error_response(
                    request.id.clone(),
                    &request.method,
                    ErrorCode::InvalidRequest,
                    "could not issue a scoped gateway credential".into(),
                    false,
                );
            };
            let value = gateway_value(identity, instance.addr, &token, &instance.prefer);
            gateways()
                .lock()
                .expect("gateway registry poisoned")
                .insert(identity.to_owned(), instance);
            success(request, value)
        }
        Err(error) => error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            format!("gateway.start failed: {error}"),
            false,
        ),
    }
}

fn gateway_stop(request: &Request, identity: &str) -> Response {
    let stopped = gateways()
        .lock()
        .expect("gateway registry poisoned")
        .remove(identity)
        .is_some();
    success(
        request,
        serde_json::json!({"identity": identity, "stopped": stopped}),
    )
}

/// `web.url { ref, path }` — the public edge URL for a resource, or an error
/// when no edge is configured (`IDFON_EDGE_URL`).
fn web_url(request: &Request) -> Response {
    let reference = request_text(&request.params, "ref").unwrap_or_default();
    let path = request_text(&request.params, "path").unwrap_or_default();
    match edge_url(edge_config().0, &reference, &path) {
        Ok(url) => success(request, serde_json::json!({"url": url})),
        Err(message) => error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            message,
            false,
        ),
    }
}

/// `files.publish { path, name?, url?, token? }` — pin a file on the edge's
/// durable store (`docs/durable-files.md`) and return its content-addressed URL.
/// The free tier is `idfon put` (a connected-only `BlobTicket`), not this path.
async fn files_publish(request: &Request) -> Response {
    match publish_file(request).await {
        Ok(result) => success(request, result),
        Err(message) => error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            message,
            false,
        ),
    }
}

async fn publish_file(request: &Request) -> Result<serde_json::Value, String> {
    let path = request_text(&request.params, "path").ok_or("path is required")?;
    let name = request_text(&request.params, "name").filter(|name| !name.is_empty());
    // Rental term, in seconds; the edge clamps it to its configured cap.
    let expires_in = request
        .params
        .get("expires_in")
        .and_then(serde_json::Value::as_u64)
        .filter(|seconds| *seconds > 0);
    let (configured, _) = edge_config();
    let base = request_text(&request.params, "url")
        .or(configured)
        .ok_or("no edge configured (pass url or set IDFON_EDGE_URL)")?
        .trim_end_matches('/')
        .to_owned();
    // Permanent pins are an operations privilege: they need the ops token.
    let permanent = request
        .params
        .get("permanent")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let token = match request_text(&request.params, "token") {
        Some(token) => token,
        None if permanent => env_value("IDFON_FILES_OPS_TOKEN")
            .ok_or("no operations token (set IDFON_FILES_OPS_TOKEN)")?,
        None => env_value("IDFON_FILES_TOKEN")
            .or_else(|| env_value("IDFON_EDGE_TOKEN"))
            .ok_or("no upload token (pass token or set IDFON_FILES_TOKEN)")?,
    };

    let (sha256, size) = hash_file(&path)
        .await
        .map_err(|error| format!("reading {path}: {error}"))?;
    // The content URL sits under the name's directory so a bundle can list
    // relative `sha256/<hash>` paths.
    let content_path = content_path(name.as_deref(), &sha256);

    let client = reqwest::Client::new();
    let uploaded = client
        .head(format!("{base}{content_path}"))
        .bearer_auth(&token)
        .send()
        .await
        .map_err(|error| format!("edge unreachable: {error}"))?;
    let existed = uploaded.status().is_success();
    if !existed {
        upload(
            &client,
            &base,
            &content_path,
            &token,
            &path,
            size,
            expires_in,
            permanent,
        )
        .await?;
    }
    if let Some(name) = &name {
        upload(
            &client,
            &base,
            &format!("/files/{name}"),
            &token,
            &path,
            size,
            expires_in,
            permanent,
        )
        .await?;
    }
    Ok(serde_json::json!({
        "url": format!("{base}{content_path}"),
        "sha256": sha256,
        "size": size,
        "existed": existed,
        "expires_in": expires_in,
        "permanent": permanent,
    }))
}

/// The content URL sits under the name's directory, so a bundle published by
/// name can list relative `sha256/<hash>` paths.
fn content_path(name: Option<&str>, sha256: &str) -> String {
    match name.and_then(|name| name.rsplit_once('/')) {
        Some((dir, _)) if !dir.is_empty() => format!("/files/{dir}/sha256/{sha256}"),
        _ => format!("/files/sha256/{sha256}"),
    }
}

async fn upload(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    token: &str,
    file: &str,
    size: u64,
    expires_in: Option<u64>,
    permanent: bool,
) -> Result<(), String> {
    let body = tokio::fs::File::open(file)
        .await
        .map_err(|error| format!("reading {file}: {error}"))?;
    let mut request = client
        .put(format!("{base}{path}"))
        .bearer_auth(token)
        .header(reqwest::header::CONTENT_LENGTH, size);
    if let Some(seconds) = expires_in {
        request = request.header("x-idfon-expires-in", seconds);
    }
    if permanent {
        request = request.header("x-idfon-pin", "permanent");
    }
    let response = request
        .body(reqwest::Body::from(body))
        .send()
        .await
        .map_err(|error| format!("uploading to {base}{path}: {error}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("edge returned {status}: {}", body.trim()));
    }
    Ok(())
}

async fn hash_file(path: &str) -> std::io::Result<(String, u64)> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1 << 20];
    let mut size = 0u64;
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size += read as u64;
    }
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    Ok((hex, size))
}

/// A setting: env var, else `~/.idfon/config.toml` (see `idfon_config`).
fn env_value(name: &str) -> Option<String> {
    idfon_config::get(name)
}

/// The public edge URL for a resource. A real endpoint id on an `https://`
/// domain uses the wildcard host form `<z-base-32>.<domain><path>` (the 64-hex
/// id exceeds the 63-octet DNS label limit); aliases and IP/port bases use
/// `<base>/<ref><path>`. Errors on a missing base or an empty ref.
fn edge_url(base: Option<String>, reference: &str, path: &str) -> Result<String, String> {
    let base = base.ok_or("no edge configured (set IDFON_EDGE_URL on the daemon)")?;
    if reference.trim().is_empty() {
        return Err("ref is required".to_owned());
    }
    let path = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };
    let reference = reference.trim();

    if let Some(id) = idfon_core::parse_endpoint_ref(reference) {
        if let Some(host) = wildcard_host(&base) {
            return Ok(format!(
                "https://{}.{}{}",
                idfon_core::endpoint_ref(&id),
                host,
                path
            ));
        }
    }
    Ok(format!(
        "{}/{}{}",
        base.trim_end_matches('/'),
        reference,
        path
    ))
}

/// The host of an `https://` base that can take a wildcard subdomain
/// (`*.<host>`), or `None` for an IP / `localhost` / explicit-port base.
fn wildcard_host(base: &str) -> Option<&str> {
    let host = base.strip_prefix("https://")?.split('/').next()?;
    if host.is_empty() || host.contains(':') || host == "localhost" || !host.contains('.') {
        return None;
    }
    let labels: Vec<&str> = host.split('.').collect();
    if labels.len() == 4 && labels.iter().all(|label| label.parse::<u8>().is_ok()) {
        return None;
    }
    Some(host)
}

/// Capability a caller must hold to read this identity's media resources.
const CAPABILITY_RESOURCE_READ: &str = "resource.read";

/// Per-identity HTTP/3 provider over the local media-resource store.
struct ProviderInstance {
    // Dropping the server aborts it and unregisters the ALPN forwarder.
    _h3: H3Server,
}

static PROVIDERS: OnceLock<Mutex<HashMap<String, ProviderInstance>>> = OnceLock::new();

fn providers() -> &'static Mutex<HashMap<String, ProviderInstance>> {
    PROVIDERS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Clone)]
struct ProviderState {
    store: Arc<Mutex<Store>>,
    identity: String,
    /// This identity's peer id (public key), the issuer of resource tickets.
    peer_id: Option<String>,
    /// Live, user-visible directory served in place. The daemon keeps no copy
    /// and no inventory: a path is resolved on request, so a rename or delete on
    /// disk is reflected immediately.
    root: PathBuf,
}

/// Authorizes the caller. A caller-presented `x-idfon-ticket` is a resource
/// ticket issued by this peer (bearer) or by a peer this peer granted
/// `resource.read` (a delegate, e.g. the edge) (P3); otherwise fall back to the
/// QUIC peer + `resource.read` grant (P0/P1).
fn authorize_resource_read(
    state: &ProviderState,
    remote: &str,
    path: &str,
    ticket: Option<&str>,
) -> Result<(), AxumResponse> {
    if let Some(raw) = ticket {
        return if ticket_authorizes(state, raw, path) {
            Ok(())
        } else {
            Err((StatusCode::FORBIDDEN, "resource ticket not authorized").into_response())
        };
    }
    let store = state.store.lock().expect("store mutex poisoned");
    let Some(peer) = store
        .peers
        .iter()
        .find(|peer| peer.identity == state.identity && peer.knows_endpoint(remote))
    else {
        return Err((StatusCode::FORBIDDEN, "unknown caller").into_response());
    };
    if !has_grant(
        &store,
        &state.identity,
        &peer.id,
        &idfon_protocol::Capability::new(CAPABILITY_RESOURCE_READ),
    ) {
        return Err((StatusCode::FORBIDDEN, "resource.read not granted").into_response());
    }
    Ok(())
}

/// True when `raw` is a valid, unexpired `resource.read` ticket whose path
/// prefix covers `path`, issued either by this peer (bearer) or by a peer this
/// peer granted `resource.read` (a delegate, e.g. the edge).
fn ticket_authorizes(state: &ProviderState, raw: &str, path: &str) -> bool {
    // Decoding verifies the issuer's signature.
    let Ok(ticket) = idfon_core::CapabilityTicket::decode(raw.trim()) else {
        return false;
    };
    if Some(ticket.issuer().as_str()) != state.peer_id.as_deref() {
        // Not a self-issued bearer ticket: the issuer must hold the matching
        // grant. The peer, not the forwarding transport, stays the authority.
        let store = state.store.lock().expect("store mutex poisoned");
        if !has_grant(
            &store,
            &state.identity,
            &ticket.issuer(),
            &idfon_protocol::Capability::new(CAPABILITY_RESOURCE_READ),
        ) {
            return false;
        }
    }
    if !ticket
        .capabilities()
        .contains(&idfon_protocol::Capability::new(CAPABILITY_RESOURCE_READ))
    {
        return false;
    }
    if ticket.is_expired(now().parse::<u64>().unwrap_or(0)) {
        return false;
    }
    if let Some(scope) = ticket.path_scope() {
        if !idfon_core::path_in_scope(path, scope) {
            return false;
        }
    }
    true
}


/// `<data_dir>/staging/<identity>/<id>` for a validated relative `id`. This is
/// the transient accumulator for a chunked `media.resource.put`: the bytes are
/// published to the blob store and the ticket is the artifact, so there is no
/// durable resource directory. Staging is swept on startup and pruned by age;
/// `None` when the id is not a safe relative path.
fn staging_file(data_dir: &Path, identity: &str, id: &str) -> Option<PathBuf> {
    Some(
        data_dir
            .join("staging")
            .join(idfon_core::path::safe_relative_path(identity)?)
            .join(idfon_core::path::safe_relative_path(id)?),
    )
}

/// Removes staging files older than an hour. A single-chunk put has no `finish`
/// to consume its staging copy, so leaked accumulators are bounded by age.
fn prune_staging(root: &Path) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            prune_staging(&path);
            continue;
        }
        if let Ok(modified) = entry.metadata().and_then(|meta| meta.modified()) {
            if modified
                .elapsed()
                .map(|age| age.as_secs() > 3600)
                .unwrap_or(false)
            {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
}

/// `GET /fs/<path>` — reads a file from the identity's live shared root for a
/// granted caller. The root is served in place: no copy, no registry, so a
/// rename or delete on disk shows up on the next request. The path is validated
/// lexically and re-checked after canonicalization so a symlink cannot escape.
async fn provider_resource_get(
    State(state): State<ProviderState>,
    RemoteId(remote): RemoteId,
    headers: axum::http::HeaderMap,
    AxumPath(path): AxumPath<String>,
) -> AxumResponse {
    let ticket = headers
        .get("x-idfon-ticket")
        .and_then(|value| value.to_str().ok());
    let request_path = format!("/fs/{path}");
    if let Err(response) =
        authorize_resource_read(&state, &remote.to_string(), &request_path, ticket)
    {
        return response;
    }
    let Some(rel) = idfon_core::path::safe_relative_path(&path) else {
        return (StatusCode::BAD_REQUEST, "invalid path").into_response();
    };
    let Ok(root) = std::fs::canonicalize(&state.root) else {
        return (StatusCode::NOT_FOUND, "shared root unavailable").into_response();
    };
    let Ok(target) = std::fs::canonicalize(root.join(rel)) else {
        return (StatusCode::NOT_FOUND, "no such file").into_response();
    };
    if !target.starts_with(&root) {
        return (StatusCode::FORBIDDEN, "path escapes the shared root").into_response();
    }
    let Ok(bytes) = std::fs::read(&target) else {
        return (StatusCode::NOT_FOUND, "no such file").into_response();
    };
    let mime = idfon_core::path::mime_for(&target);
    ([(axum::http::header::CONTENT_TYPE, mime)], bytes).into_response()
}

fn provider_router(state: ProviderState) -> AxumRouter {
    AxumRouter::new()
        .route("/fs/{*path}", get(provider_resource_get))
        .with_state(state)
}

/// Idempotently starts the identity's H3 resource provider.
async fn provider_start(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
    identity: &str,
) -> Response {
    let Some(transport) = transport else {
        return transport_not_ready(request);
    };
    if providers()
        .lock()
        .expect("provider registry poisoned")
        .contains_key(identity)
    {
        return success(
            request,
            serde_json::json!({"identity": identity, "serving": true}),
        );
    }
    let Some(root) = request_text(&request.params, "root") else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "root is required (the user-visible directory to serve)".into(),
            false,
        );
    };
    let root = match std::fs::canonicalize(&root) {
        Ok(path) if path.is_dir() => path,
        _ => {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                format!("root is not a directory: {root}"),
                false,
            )
        }
    };
    let Some(iroh) = transport.current_transport_async(identity).await else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            format!("identity {identity} has no iroh transport"),
            false,
        );
    };
    let served = root.display().to_string();
    let peer_id = {
        let state = store.lock().expect("store mutex poisoned");
        state
            .identities
            .iter()
            .find(|candidate| candidate.id == identity || candidate.name == identity)
            .and_then(|candidate| candidate.public_key.clone())
    };
    let h3 = serve_router(
        &iroh,
        provider_router(ProviderState {
            store: Arc::clone(store),
            identity: identity.to_owned(),
            peer_id,
            root,
        }),
    );
    providers()
        .lock()
        .expect("provider registry poisoned")
        .insert(identity.to_owned(), ProviderInstance { _h3: h3 });
    success(
        request,
        serde_json::json!({"identity": identity, "serving": true, "root": served}),
    )
}

fn provider_stop(request: &Request, identity: &str) -> Response {
    let stopped = providers()
        .lock()
        .expect("provider registry poisoned")
        .remove(identity)
        .is_some();
    success(
        request,
        serde_json::json!({"identity": identity, "stopped": stopped}),
    )
}

/// Mutations that change which endpoints the relay should admit
/// (`relay_auth`, idfon-edge/docs/relay-auth.md §6); they poke the enrollment loop.
const RELAY_ENROLL_MUTATIONS: &[&str] = &[
    "peer.add",
    "request.accept",
    "peer.update",
    "peer.device.add",
    "peer.device.update",
    "peer.device.remove",
    "peer.remove",
    "identity.create",
    "identity.delete",
    "event.merge",
];

fn dispatch(request: Request, store: &Arc<Mutex<Store>>) -> Response {
    let transport = Arc::new(TransportMode::Fake(FakeTransport::default()));
    dispatch_with_transport(request, store, Some(&transport))
}

/// Transport-dependent methods fail closed, retryably, while the endpoints
/// are still binding — and before touching any state (a send must never
/// record an operation that is doomed to fail). Store-only functions never
/// take this path, so local data is available the moment the store opens.
fn transport_not_ready(request: &Request) -> Response {
    error_response(
        request.id.clone(),
        &request.method,
        ErrorCode::Internal,
        "transport is still starting".into(),
        true,
    )
}

fn dispatch_with_transport(
    mut request: Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
) -> Response {
    idfon_protocol::rpc::normalize_request(&mut request);
    if let Err(error) = validate_request(&request) {
        let (code, message) = match error {
            idfon_protocol::ProtocolError::InvalidVersion(version) => (
                ErrorCode::InvalidVersion,
                format!("unsupported protocol version {version}"),
            ),
            idfon_protocol::ProtocolError::FrameTooLarge => {
                (ErrorCode::FrameTooLarge, "frame is too large".into())
            }
        };
        return error_response(request.id, &request.method, code, message, false);
    }

    let response = match request.method.as_str() {
        "message.receive" => receive_message(&request, store),
        "message.send" => send_message(&request, store, transport),
        "room.create" => room_create(&request, store, transport),
        "room.join" => room_join(&request, store, transport),
        "room.list" => room_list(&request, store),
        "room.send" => room_send(&request, store, transport),
        "room.leave" => room_leave(&request, store),
        "room.update" => room_update(&request, store, transport),
        "room.call.join" => room_calls::join(&request, store, transport),
        "room.call.leave" => room_calls::leave(&request, store, transport),
        "room.call.status" => room_calls::status(&request, store),
        "room.call.peer_lost" => room_calls::peer_lost(&request, store),
        "operation.get" => operation_get(&request, store),
        "operation.wait" => operation_wait(&request, store),
        "operation.cancel" => operation_cancel(&request, store),
        "operation.resume" => operation_resume(&request, store, transport),
        "event.list" => events(&request, store),
        "event.export" => events_export(&request, store),
        "event.sync" => events_sync(&request, store, transport),
        "event.merge" => events_merge(&request, store),
        "event.wait" => wait_event(&request, store),
        "contact.ticket" => contact_ticket(&request, store, transport),
        "contact.export" => contact_export(&request, store, transport),
        "contact.import" => contact_import(&request, store, transport),
        "contact.introduce" => introductions::contact_introduce(&request, store),
        "contact.inspect" => introductions::contact_inspect(&request, store),
        "request.list" => request_list(&request, store),
        "request.accept" => request_accept(&request, store),
        "request.decline" => request_decline(&request, store),
        "request.unblock" => request_unblock(&request, store),
        "daemon.status" => {
            let state = store.lock().expect("store mutex poisoned");
            let identity_ref =
                request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
            let identity = state
                .identities
                .iter()
                .find(|identity| identity.id == identity_ref || identity.name == identity_ref);
            let identity_id = identity
                .map(|identity| identity.id.as_str())
                .unwrap_or("default");
            success(
                &request,
                {
                    let mut status = serde_json::json!({
                        "identity": identity,
                        "daemon": "idfond",
                        "ready": true,
                        "protocol_version": PROTOCOL_VERSION,
                        "ticket": transport
                            .and_then(|transport| transport.endpoint_ticket_for(identity_id))
                            .unwrap_or_default(),
                        "contact_ticket": contact_ticket_value(&state, transport, identity_id),
                    });
                    if let (Some(status), serde_json::Value::Object(build)) =
                        (status.as_object_mut(), build_info::status_fields())
                    {
                        status.extend(build);
                    }
                    status
                },
            )
        }
        "identity.list" => {
            let state = store.lock().expect("store mutex poisoned");
            success(
                &request,
                serde_json::json!({"identities": state.identities}),
            )
        }
        "identity.list.compact" => success(&request, serde_json::json!(compact_identities(store))),
        "peer.list" => {
            let state = store.lock().expect("store mutex poisoned");
            let identity = request_text(&request.params, "identity");
            let peers: Vec<_> = state
                .peers
                .iter()
                .filter(|peer| {
                    identity
                        .as_deref()
                        .is_none_or(|value| peer.identity == value)
                })
                .collect();
            success(&request, serde_json::json!({"peers": peers}))
        }
        "peer.add" => peer_add(&request, store, transport),
        "peer.update" => peer_update(&request, store),
        "tag.list" => tag_list(&request, store),
        "tag.set" => tag_set(&request, store),
        "tag.rename" => tag_rename(&request, store),
        "tag.delete" => tag_delete(&request, store),
        "visibility.get" => {
            let state = store.lock().expect("store mutex poisoned");
            success(&request, serde_json::json!({"visibility": visibility_view(&state.visibility)}))
        }
        "visibility.set" => visibility_set(&request, store),
        "visibility.audience" => visibility_audience(&request, store),
        "peer.device.add" => peer_device_add(&request, store),
        "peer.device.update" => peer_device_update(&request, store),
        "peer.device.remove" => peer_device_remove(&request, store),
        "peer.remove" => peer_remove(&request, store),
        "peer.show" | "peer.status" => {
            let reference = request
                .params
                .get("ref")
                .and_then(serde_json::Value::as_str);
            let state = store.lock().expect("store mutex poisoned");
            let peer = state.peers.iter().find(|peer| {
                reference.is_some_and(|reference| {
                    peer.id == reference
                        || peer.name == reference
                        || peer.aliases.iter().any(|alias| alias == reference)
                        || peer.knows_endpoint(reference)
                })
            });
            match peer {
                Some(peer) => {
                    let discovery = state
                        .mcp_discovery
                        .iter()
                        .find(|record| {
                            record.identity == peer.identity && record.peer_id == peer.id
                        })
                        .map(|record| record.discover.clone());
                    let mcp_transport = has_grant(
                        &state,
                        &peer.identity,
                        &peer.id,
                        &idfon_protocol::Capability::McpTransport,
                    );
                    success(
                        &request,
                        serde_json::json!({
                            "peer": peer,
                            "status": "known",
                            "mcp_discovery": discovery,
                            "mcp_transport_granted": mcp_transport,
                        }),
                    )
                }
                None => error_response(
                    request.id,
                    &request.method,
                    ErrorCode::InvalidRequest,
                    "peer not found".into(),
                    false,
                ),
            }
        }
        "mcp.listen" => mcp::listen(&request, store, transport),
        "mcp.configure" => mcp_configure(&request, store),
        "access.grant" => access_grant(&request, store),
        "access.ticket" => capability_ticket_issue(&request, store),
        "access.ticket.revoke" => capability_ticket_revoke(&request, store),
        "access.revoke" => access_revoke(&request, store),
        "access.check" => access_check(&request, store),
        "session.start" => media_session_start(&request, store),
        "session.stop" => media_session_stop(&request, store),
        "blob.put" => media_resource_put(&request, store, transport),
        "blob.get" => media_resource_fetch(&request, store, transport),
        "session.list" => media_sessions(&request, store),
        "live.publish" => live::live_publish(&request),
        "live.dial" => live_dial_dispatch(&request, store),
        "live.answer" => {
            live::live_answer(&request, transport, &resolved_identity_id(&request, store))
        }
        "live.stop" => live::live_stop(&request),
        "live.subscribe" => live::live_subscribe(&request),
        "live.list" => live::live_publishers(&request),
        "policy.set" => policy_set(&request, store),
        "policy.dry_run" => policy_dry_run(&request, store),
        // Identities are tags now (2026-11-24, docs/identity.md): there is
        // one you, with one endpoint.
        "identity.create" => error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "there is one identity now; tag contacts instead (tag.set, peer.update tags)".into(),
            false,
        ),
        "identity.rename" => identity_rename(&request, store),
        "identity.update" => introductions::identity_update(&request, store),
        "identity.delete" => error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "the only identity can't be deleted; delete all data instead".into(),
            false,
        ),
        "identity.use" => {
            let name = request
                .params
                .get("name")
                .and_then(serde_json::Value::as_str);
            let mut state = store.lock().expect("store mutex poisoned");
            let found = state.identities.iter().any(|identity| {
                name.is_some_and(|name| identity.id == name || identity.name == name)
            });
            if !found {
                return error_response(
                    request.id,
                    &request.method,
                    ErrorCode::InvalidRequest,
                    "identity not found".into(),
                    false,
                );
            }
            let identity_id = state
                .identities
                .iter()
                .find(|identity| {
                    identity.id == name.unwrap_or_default()
                        || identity.name == name.unwrap_or_default()
                })
                .map(|identity| identity.id.clone())
                .unwrap();
            for identity in &mut state.identities {
                identity.active = identity.id == identity_id;
            }
            let key = match state.endpoint_key(&identity_id) {
                Ok(key) => key,
                Err(error) => {
                    return error_response(
                        request.id,
                        &request.method,
                        ErrorCode::Internal,
                        error.to_string(),
                        false,
                    )
                }
            };
            let Some(transport) = transport else {
                return transport_not_ready(&request);
            };
            let endpoint_id = match transport
                .ensure_identity(&identity_id, key.secret_bytes())
            {
                Ok(endpoint_id) => endpoint_id,
                Err(error) => {
                    return error_response(
                        request.id,
                        &request.method,
                        ErrorCode::Internal,
                        error.to_string(),
                        true,
                    )
                }
            };
            if let Some(endpoint_id) = endpoint_id {
                if let Some(identity) = state
                    .identities
                    .iter_mut()
                    .find(|identity| identity.id == identity_id)
                {
                    identity.endpoint_id = Some(endpoint_id);
                }
            }
            let data_dir = state.data_dir.clone();
            if let Err(error) = state.save(&data_dir) {
                return error_response(
                    request.id,
                    &request.method,
                    ErrorCode::Internal,
                    error.to_string(),
                    true,
                );
            }
            // wire_identity locks the store itself (room_calls::
            // set_live_transport); holding it here deadlocked the daemon.
            drop(state);
            // On lazy (mobile) startup this is the first bind for the
            // identity; wire the receiver/relay/gossip/rooms startup skipped.
            wire_identity(
                Arc::clone(transport),
                Arc::clone(store),
                identity_id.clone(),
            );
            success(
                &request,
                serde_json::json!({"identity": name, "active": true}),
            )
        }
        "peer.resolve" => {
            let reference = request
                .params
                .get("ref")
                .and_then(serde_json::Value::as_str);
            let state = store.lock().expect("store mutex poisoned");
            let identity = request_text(&request.params, "identity");
            let matches: Vec<_> = state
                .peers
                .iter()
                .filter(|peer| {
                    identity
                        .as_deref()
                        .is_none_or(|value| peer.identity == value)
                })
                .filter(|peer| {
                    reference.is_some_and(|reference| {
                        peer.id == reference
                            || peer.name == reference
                            || peer.aliases.iter().any(|alias| alias == reference)
                            || peer.knows_endpoint(reference)
                    })
                })
                .collect();
            match matches.as_slice() {
                [peer] => success(&request, serde_json::json!({"peer": peer})),
                [] => error_response(
                    request.id,
                    &request.method,
                    ErrorCode::InvalidRequest,
                    "peer not found".into(),
                    false,
                ),
                _ => error_response(
                    request.id,
                    &request.method,
                    ErrorCode::AmbiguousPeer,
                    "peer reference is ambiguous".into(),
                    false,
                ),
            }
        }
        _ => error_response(
            request.id,
            &request.method,
            ErrorCode::UnknownMethod,
            format!("unknown method '{}'", request.method),
            false,
        ),
    };
    // Relay auto-enrollment: re-sync admission after mutations. Wakes coalesce
    // in the maintenance task, so this stays cheap on hot paths.
    if matches!(&response.body, ResponseBody::Success { .. })
        && RELAY_ENROLL_MUTATIONS.contains(&request.method.as_str())
    {
        relay_auth::poke();
    }
    response
}

fn request_text(params: &serde_json::Value, name: &str) -> Option<String> {
    if let Some(value) = params.get(name).and_then(serde_json::Value::as_str) {
        return Some(value.to_owned());
    }
    let bytes = params
        .get(&format!("{name}_bytes"))
        .and_then(serde_json::Value::as_array)?;
    let mut output = String::with_capacity(bytes.len());
    for byte in bytes {
        output.push(byte.as_u64()?.try_into().ok().and_then(char::from_u32)?);
    }
    Some(output)
}

/// 128-bit random room id (`r_` + 32 hex). Reuses the identity generator for OS
/// randomness rather than taking on an RNG dependency.
fn random_room_id() -> String {
    let key = idfon_core::IdentityKey::generate();
    format!("r_{}", &key.peer_id()[..32])
}

/// A list of tag names from `params[key]`, trimmed and deduplicated.
fn request_tags(params: &serde_json::Value, key: &str) -> Vec<String> {
    let mut tags = Vec::new();
    for tag in params
        .get(key)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
    {
        add_tag(&mut tags, tag);
    }
    tags
}

/// The grant that lets a peer receive pictures of your screen.
const SCREEN_READ: &str = "screen.read";

/// A screen capture on the wire: a file envelope marked `screen=1`.
fn is_screen_capture(text: &str) -> bool {
    text.starts_with("IDFON-FILE/1\n") && text.lines().any(|line| line.trim() == "screen=1")
}

/// Add `tag` once (case-insensitive).
fn add_tag(tags: &mut Vec<String>, tag: &str) {
    let tag = tag.trim();
    if !tag.is_empty() && !tags.iter().any(|existing| existing.eq_ignore_ascii_case(tag)) {
        tags.push(tag.to_owned());
    }
}

/// Whether `peer` may see `aspect` ("presence" | "photo") of you: their own
/// override, else hidden when any of their tags hides it, else the default
/// (shown unless turned off). docs/identity.md.
fn peer_sees(state: &Store, peer: &idfon_protocol::Peer, aspect: &str) -> bool {
    if let Some(choice) = peer.visibility.as_ref().and_then(|v| v.choice(aspect)) {
        return choice;
    }
    let hidden = peer.tags.iter().any(|tag| {
        state
            .tag_rules
            .iter()
            .any(|rule| rule.name.eq_ignore_ascii_case(tag) && rule.hides(aspect))
    });
    !hidden && state.visibility.choice(aspect).unwrap_or(true)
}

fn identity_id_of(state: &Store, identity_ref: &str) -> String {
    state
        .identities
        .iter()
        .find(|identity| identity.id == identity_ref || identity.name == identity_ref)
        .map(|identity| identity.id.clone())
        .unwrap_or_else(|| identity_ref.to_owned())
}

/// D20: a send rides the relationship's line. An explicit `identity` param
/// wins; otherwise the target peer's stored identity routes the send —
/// identity is fixed at contact-add (§11), so the peer record carries it.
/// Ambiguous or unknown targets fall back to `default` (pass `identity`
/// explicitly when one ref names contacts under several identities — A9).
fn send_identity_id(state: &Store, request: &Request, to: &str) -> String {
    if let Some(identity_ref) = request_text(&request.params, "identity") {
        return identity_id_of(state, &identity_ref);
    }
    let mut matches = state.peers.iter().filter(|peer| {
        peer.id == to
            || peer.name == to
            || peer.aliases.iter().any(|alias| alias == to)
            || peer.knows_endpoint(to)
    });
    match (matches.next(), matches.next()) {
        (Some(peer), None) => peer.identity.clone(),
        _ => "default".into(),
    }
}

/// Ensures a peer carries its derived account handle (`blake3(peer.id)`) as an
/// alias, so `idfon://<handle>` resolves without the handle being carried in
/// tickets. Idempotent; never removes caller aliases — a 64-hex alias may be a
/// device endpoint id, which `ref` matching also accepts.
fn ensure_account_alias(peer: &mut idfon_protocol::Peer) {
    if peer.id.is_empty() {
        return;
    }
    let handle = idfon_core::account_alias(&peer.id);
    if !peer.aliases.iter().any(|alias| alias == &handle) {
        peer.aliases.push(handle);
    }
}

/// Resolve a peer ref (id, name, or alias) to a peer id. Unknown refs are kept
/// verbatim so a peer id that is not yet in the peer list still works.
fn resolve_peer_id(state: &Store, identity_id: &str, peer_ref: &str) -> String {
    state
        .peers
        .iter()
        .find(|peer| {
            peer.identity == identity_id
                && (peer.id == peer_ref
                    || peer.name == peer_ref
                    || peer.aliases.iter().any(|alias| alias == peer_ref))
        })
        .map(|peer| peer.id.clone())
        .unwrap_or_else(|| peer_ref.to_owned())
}

fn room_matches(room: &idfon_protocol::Room, room_ref: &str) -> bool {
    room.id == room_ref || room.name.as_deref() == Some(room_ref)
}

/// Join a room's gossip topic and feed received signed envelopes through the
/// ordinary message receiver. A missing/unjoined topic is intentionally not an
/// error: room.send falls back to direct fan-out.
async fn join_gossip_room(
    identity: String,
    room_id: String,
    members: Vec<String>,
    store: Arc<Mutex<Store>>,
    transport: Arc<TransportMode>,
) {
    let TransportMode::Iroh(manager) = transport.as_ref() else {
        return;
    };
    let key = format!("{identity}:{room_id}");
    if gossip_topics()
        .lock()
        .expect("gossip topics poisoned")
        .contains_key(&key)
    {
        return;
    }
    let Some(iroh) = manager.current(&identity).await else {
        return;
    };
    let service = iroh.enable_gossip();
    let bootstrap = {
        let state = store.lock().expect("store mutex poisoned");
        members
            .iter()
            .filter_map(|member| {
                state
                    .peers
                    .iter()
                    .find(|peer| peer.identity == identity && peer.id == *member)
                    .and_then(|peer| peer.endpoint_id.as_deref())
                    .and_then(|id| id.parse().ok())
            })
            .collect()
    };
    let topic = match service.subscribe(room_topic(&room_id), bootstrap).await {
        Ok(topic) => topic,
        Err(error) => {
            tracing::warn!(target: "idfon.daemon", room = %room_id, error = %error, "gossip room join failed");
            return;
        }
    };
    let (sender, mut receiver) = topic.split();
    let task = tokio::spawn(async move {
        while let Some(Ok(event)) = receiver.next().await {
            let GossipEvent::Received(message) = event else {
                continue;
            };
            let Ok(mut params) = serde_json::from_slice::<serde_json::Value>(&message.content)
            else {
                continue;
            };
            let Some(object) = params.as_object_mut() else {
                continue;
            };
            object.insert(
                "identity".into(),
                serde_json::Value::String(identity.clone()),
            );
            let request = Request {
                version: PROTOCOL_VERSION,
                id: format!("gossip-{}", now()),
                method: "message.receive".into(),
                params,
            };
            let _ = receive_message(&request, &store);
        }
    });
    gossip_topics()
        .lock()
        .expect("gossip topics poisoned")
        .insert(key, GossipTopicHandle { sender, task });
}

fn gossip_room_send(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: &Arc<TransportMode>,
) -> Option<Response> {
    let identity_ref =
        request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let room_ref = request_text(&request.params, "room")?;
    let text = request_text(&request.params, "text")?;
    let key = request_text(&request.params, "idempotency_key")?;
    if tokio::runtime::Handle::try_current().is_err() {
        return None;
    }
    let (identity_id, room_id) = {
        let state = store.lock().expect("store mutex poisoned");
        let identity_id = identity_id_of(&state, &identity_ref);
        let room_id = state
            .rooms
            .iter()
            .find(|room| room.identity == identity_id && room_matches(room, &room_ref))
            .map(|room| room.id.clone())?;
        (identity_id, room_id)
    };
    let sender = gossip_topics()
        .lock()
        .expect("gossip topics poisoned")
        .get(&format!("{identity_id}:{room_id}"))
        .map(|topic| topic.sender.clone())?;
    let capability_ticket = request
        .params
        .get("capability_ticket")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok());
    let (operation, envelope, members) = {
        let mut state = store.lock().expect("store mutex poisoned");
        let identity = state
            .identities
            .iter()
            .find(|identity| identity.id == identity_id || identity.name == identity_id)?
            .clone();
        let room = state
            .rooms
            .iter()
            .find(|room| room.identity == identity.id && room_matches(room, &room_ref))?
            .clone();
        if room.members.len() < GOSSIP_ROOM_MEMBER_THRESHOLD {
            return None;
        }
        if room.members.iter().any(|member| {
            !state.grants.iter().any(|grant| {
                grant.identity == identity.id
                    && grant.subject == *member
                    && grant.capability == idfon_protocol::Capability::MessageSend
                    && grant.revoked_at.is_none()
                    && grant
                        .expires_at
                        .as_deref()
                        .is_none_or(|expires| expires > now().as_str())
            })
        }) {
            return None;
        }
        let fingerprint =
            serde_json::to_string(&serde_json::json!({"room": room.id, "text": text})).ok()?;
        if let Some(existing) = state.operations.iter().find(|operation| {
            operation.target.as_deref() == Some(room.id.as_str())
                && operation.idempotency_key.as_deref() == Some(&key)
        }) {
            return Some(success(
                request,
                serde_json::json!({"room": room.id, "operation_ids": [existing.operation_id], "transport": "gossip", "status": existing.status}),
            ));
        }
        let signing_key = state.identity_key(&identity.id).ok()?;
        let message_id = format!("msg_{}", state.operations.len() + 1);
        let mut envelope = idfon_core::sign_message_with_ticket(
            &signing_key,
            identity.endpoint_id.unwrap_or_default(),
            message_id.clone(),
            idfon_protocol::MessageContent::Text { text },
            &key,
            Some(room.id.clone()),
            capability_ticket,
        )
        .ok()?;
        envelope.trace = Some(idfon_core::new_traceparent());
        envelope.telemetry = Some(idfon_telemetry::mode().to_string());
        let timestamp = now();
        let operation = idfon_protocol::Operation {
            identity: identity.id,
            operation_id: format!("op_{message_id}"),
            method: request.method.clone(),
            status: idfon_protocol::OperationStatus::Queued,
            target: Some(room.id),
            request_fingerprint: Some(fingerprint),
            created_at: timestamp.clone(),
            updated_at: timestamp,
            idempotency_key: Some(key),
            message_id: Some(message_id),
            outbound: Some(envelope.clone()),
            delivery: None,
            durable: false,
        };
        state.operations.push(operation.clone());
        let _ = state.save(&state.data_dir);
        (operation, envelope, room.members)
    };
    let operation_id = operation.operation_id.clone();
    let identity = operation.identity.clone();
    let worker_store = Arc::clone(store);
    let worker_transport = Arc::clone(transport);
    tokio::spawn(async move {
        let result = sender
            .broadcast(serde_json::to_vec(&envelope).unwrap_or_default().into())
            .await;
        let status = if result.is_ok() {
            idfon_protocol::OperationStatus::Delivered
        } else {
            let peers = {
                let state = worker_store.lock().expect("store mutex poisoned");
                members
                    .iter()
                    .filter_map(|member| {
                        state.peers.iter().find(|peer| peer.id == *member).cloned()
                    })
                    .collect::<Vec<_>>()
            };
            let transport = Arc::clone(&worker_transport);
            let envelope = envelope.clone();
            let identity = identity.clone();
            match tokio::task::spawn_blocking(move || {
                peers
                    .iter()
                    .all(|peer| transport.send(&identity, peer, &envelope, None).is_ok())
            })
            .await
            {
                Ok(true) => idfon_protocol::OperationStatus::Delivered,
                _ => idfon_protocol::OperationStatus::Failed,
            }
        };
        let _ = tokio::task::spawn_blocking(move || {
            update_operation(&worker_store, &operation_id, &identity, status)
        })
        .await;
    });
    Some(success(
        request,
        serde_json::json!({"room": operation.target, "operation_ids": [operation.operation_id], "transport": "gossip", "status": "queued"}),
    ))
}

/// `room.create { id?, name?, members? }`. `id` defaults to a random topic id;
/// members are stored as **resolved peer ids** so a rename cannot drop one.
/// Membership is local state — nothing here is shared or authoritative.
fn room_create(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
) -> Response {
    let Some(transport) = transport else {
        return transport_not_ready(request);
    };
    let identity_ref =
        request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let id = request_text(&request.params, "id").unwrap_or_else(random_room_id);
    let name = request_text(&request.params, "name");
    // P6 project metadata: type + icon + repo (D5 keeps "task" out of here).
    let kind = request_text(&request.params, "kind").unwrap_or_else(|| "project".into());
    let icon = request_text(&request.params, "icon");
    let repo = request_text(&request.params, "repo");
    let call_auto_close = request.params.get("call_auto_close").and_then(serde_json::Value::as_bool);
    let member_refs: Vec<String> = request
        .params
        .get("members")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let mut state = store.lock().expect("store mutex poisoned");
    let identity_id = identity_id_of(&state, &identity_ref);
    let members: Vec<String> = member_refs
        .iter()
        .map(|member_ref| resolve_peer_id(&state, &identity_id, member_ref))
        .collect();
    if !state
        .identities
        .iter()
        .any(|identity| identity.id == identity_id)
    {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "no active identity".into(),
            false,
        );
    }
    if state
        .rooms
        .iter()
        .any(|room| room.identity == identity_id && room_matches(room, &id))
    {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            format!("room already exists: {id}"),
            false,
        );
    }
    let room = idfon_protocol::Room {
        id,
        identity: identity_id,
        name,
        members,
        kind,
        icon,
        repo,
        call_auto_close,
    };
    state.rooms.push(room.clone());
    record_state_event(
        &mut state,
        &room.identity,
        "room.created",
        serde_json::json!({"room": room}),
    );
    let dir = state.data_dir.clone();
    let _ = state.save(&dir);
    let identity = room.identity.clone();
    let members = room.members.clone();
    let room_id = room.id.clone();
    let store = Arc::clone(store);
    let transport = Arc::clone(transport);
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move {
            join_gossip_room(identity, room_id, members, store, transport).await;
        });
    }
    success(request, serde_json::json!({ "room": room }))
}

/// `room.join { room, name?, members? }` — imports a room id into local state.
/// Membership remains local because the current invite is intentionally just an
/// opaque room id; callers provide the peers they want to fan out to.
fn room_join(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
) -> Response {
    let Some(transport) = transport else {
        return transport_not_ready(request);
    };
    let room_id = match request_text(&request.params, "room") {
        Some(room) if !room.is_empty() => room,
        _ => {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                "room is required".into(),
                false,
            )
        }
    };
    let identity_ref =
        request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let identity_id = {
        let state = store.lock().expect("store mutex poisoned");
        identity_id_of(&state, &identity_ref)
    };
    {
        let state = store.lock().expect("store mutex poisoned");
        if let Some(existing) = state
            .rooms
            .iter()
            .find(|room| room.identity == identity_id && room.id == room_id)
        {
            return success(request, serde_json::json!({ "room": existing }));
        }
    }
    let mut params = request.params.clone();
    let Some(object) = params.as_object_mut() else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "room params must be an object".into(),
            false,
        );
    };
    object.insert("id".into(), serde_json::Value::String(room_id));
    let create = Request {
        method: "room.create".into(),
        params,
        ..request.clone()
    };
    let response = room_create(&create, store, Some(transport));
    match response.body {
        ResponseBody::Success { result, .. } => success(request, result),
        ResponseBody::Failure { error, .. } => error_response(
            request.id.clone(),
            &request.method,
            error.code,
            error.message,
            false,
        ),
    }
}

/// `room.list` — rooms for the calling identity.
fn room_list(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let identity_ref =
        request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let state = store.lock().expect("store mutex poisoned");
    let identity_id = identity_id_of(&state, &identity_ref);
    let rooms: Vec<idfon_protocol::Room> = state
        .rooms
        .iter()
        .filter(|room| room.identity == identity_id)
        .cloned()
        .collect();
    success(request, serde_json::json!({ "rooms": rooms }))
}

/// `room.update { room, name?, members?, icon?, repo?, kind? }` — change a
/// room's local record: the given fields replace the stored ones (`members`
/// replaces the whole list). Membership stays local (docs/chatrooms.md);
/// apps tell the other members with their own invite message. A changed
/// member list re-joins the gossip topic with the new peers.
fn room_update(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
) -> Response {
    let room_ref = request_text(&request.params, "room").unwrap_or_default();
    let identity_ref =
        request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let member_refs: Option<Vec<String>> = request
        .params
        .get("members")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        });
    let mut state = store.lock().expect("store mutex poisoned");
    let identity_id = identity_id_of(&state, &identity_ref);
    let members = member_refs.map(|refs| {
        refs.iter()
            .map(|member_ref| resolve_peer_id(&state, &identity_id, member_ref))
            .collect::<Vec<_>>()
    });
    let Some(room) = state
        .rooms
        .iter_mut()
        .find(|room| room.identity == identity_id && room_matches(room, &room_ref))
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            format!("room not found: {room_ref}"),
            false,
        );
    };
    if let Some(name) = request_text(&request.params, "name") {
        room.name = (!name.is_empty()).then_some(name);
    }
    if let Some(icon) = request_text(&request.params, "icon") {
        room.icon = (!icon.is_empty()).then_some(icon);
    }
    if let Some(repo) = request_text(&request.params, "repo") {
        room.repo = (!repo.is_empty()).then_some(repo);
    }
    if let Some(kind) = request_text(&request.params, "kind").filter(|kind| !kind.is_empty()) {
        room.kind = kind;
    }
    let members_changed = members.as_ref().is_some_and(|members| members != &room.members);
    if let Some(members) = members {
        room.members = members;
    }
    let room = room.clone();
    record_state_event(
        &mut state,
        &room.identity,
        "room.updated",
        serde_json::json!({"room": room}),
    );
    let dir = state.data_dir.clone();
    let _ = state.save(&dir);
    drop(state);
    if members_changed {
        let key = format!("{}:{}", room.identity, room.id);
        if let Some(topic) = gossip_topics()
            .lock()
            .expect("gossip topics poisoned")
            .remove(&key)
        {
            topic.task.abort();
        }
        if let (Some(transport), Ok(handle)) = (transport, tokio::runtime::Handle::try_current()) {
            let (identity, room_id, members) = (room.identity.clone(), room.id.clone(), room.members.clone());
            let store = Arc::clone(store);
            let transport = Arc::clone(transport);
            handle.spawn(async move {
                join_gossip_room(identity, room_id, members, store, transport).await;
            });
        }
    }
    success(request, serde_json::json!({ "room": room }))
}

/// `room.leave { room }` — local only; the other members are not told.
fn room_leave(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let room_ref = request_text(&request.params, "room").unwrap_or_default();
    let identity_ref =
        request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let mut state = store.lock().expect("store mutex poisoned");
    let identity_id = identity_id_of(&state, &identity_ref);
    let topic_room_id = state
        .rooms
        .iter()
        .find(|room| room.identity == identity_id && room_matches(room, &room_ref))
        .map(|room| room.id.clone());
    let before = state.rooms.len();
    state
        .rooms
        .retain(|room| !(room.identity == identity_id && room_matches(room, &room_ref)));
    let removed = before != state.rooms.len();
    let dir = state.data_dir.clone();
    let _ = state.save(&dir);
    drop(state);
    if let Some(room_id) = topic_room_id {
        let key = format!("{identity_id}:{room_id}");
        if let Some(topic) = gossip_topics()
            .lock()
            .expect("gossip topics poisoned")
            .remove(&key)
        {
            topic.task.abort();
        }
    }
    success(request, serde_json::json!({ "removed": removed }))
}

/// `room.send { room, text, idempotency_key, capability_ticket?, retries? }`
///
/// Fan-out: one ordinary `message.send` per member carrying the same
/// `conversation`, so grants, tickets, retries and operations behave exactly as
/// they do for a 1:1 send. The per-member idempotency key is derived
/// deterministically from the caller's base key (`{base}:{member}`), so a
/// retried `room.send` dedupes per recipient instead of all-or-nothing.
fn room_send(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
) -> Response {
    let Some(transport) = transport else {
        return transport_not_ready(request);
    };
    let room_ref = request_text(&request.params, "room");
    let text = request_text(&request.params, "text");
    let base_key = request_text(&request.params, "idempotency_key");
    let identity_ref =
        request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    if room_ref.is_none() || text.is_none() || base_key.is_none() {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "room, text and idempotency_key are required".into(),
            false,
        );
    }
    let room_ref = room_ref.unwrap();
    let text = text.unwrap();
    let base_key = base_key.unwrap();
    let room = {
        let state = store.lock().expect("store mutex poisoned");
        let identity_id = identity_id_of(&state, &identity_ref);
        state
            .rooms
            .iter()
            .find(|room| room.identity == identity_id && room_matches(room, &room_ref))
            .cloned()
    };
    let room = match room {
        Some(room) => room,
        None => {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                format!("room not found: {room_ref}"),
                false,
            )
        }
    };
    if let Some(response) = gossip_room_send(request, store, transport) {
        return response;
    }
    let capability_ticket = request.params.get("capability_ticket").cloned();
    let delivery = request.params.get("delivery").cloned();
    let retries = request.params.get("retries").cloned();
    let mut operation_ids: Vec<String> = Vec::new();
    let mut failures: Vec<serde_json::Value> = Vec::new();
    for member in &room.members {
        let mut params = serde_json::json!({
            "identity": room.identity,
            "to": member,
            "text": text,
            "idempotency_key": format!("{base_key}:{member}"),
            "conversation": room.id,
        });
        if let Some(ticket) = &capability_ticket {
            params["capability_ticket"] = ticket.clone();
        }
        if let Some(delivery) = &delivery {
            params["delivery"] = delivery.clone();
        }
        if let Some(retries) = &retries {
            params["retries"] = retries.clone();
        }
        let sub_request = Request {
            version: PROTOCOL_VERSION,
            id: format!("{}-{member}", request.id),
            method: "message.send".into(),
            params,
        };
        match send_message(&sub_request, store, Some(transport)).body {
            ResponseBody::Success { result, .. } => {
                if let Some(operation_id) = result
                    .get("operation_id")
                    .and_then(serde_json::Value::as_str)
                {
                    operation_ids.push(operation_id.to_owned());
                }
            }
            ResponseBody::Failure { error, .. } => failures.push(serde_json::json!({
                "peer": member,
                "code": format!("{:?}", error.code),
                "message": error.message,
            })),
        }
    }
    success(
        request,
        serde_json::json!({
            "room": room.id,
            "operation_ids": operation_ids,
            "failures": failures,
        }),
    )
}

fn send_message(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
) -> Response {
    // Fail closed before recording anything: a send queued with no transport
    // would end up marked Failed instead of waiting for the bind to finish.
    let Some(transport) = transport else {
        return transport_not_ready(request);
    };
    let to = request_text(&request.params, "to");
    let identity_id = match &to {
        Some(target) => {
            let state = store.lock().expect("store mutex poisoned");
            send_identity_id(&state, request, target)
        }
        None => "default".into(),
    };
    let text = request_text(&request.params, "text");
    let key = request_text(&request.params, "idempotency_key");
    let conversation = request
        .params
        .get("conversation")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let capability_ticket = request
        .params
        .get("capability_ticket")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok());
    let delivery_policy = request
        .params
        .get("delivery")
        .cloned()
        .and_then(|value| serde_json::from_value::<idfon_protocol::DeliveryPolicy>(value).ok());
    // Outbox semantics (Operation::durable): undeliverable stays Queued.
    let durable = request
        .params
        .get("durable")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let retries = request
        .params
        .get("retries")
        .and_then(serde_json::Value::as_u64)
        // A push-woken callee's transport needs seconds to come up (its
        // endpoint binds before it starts accepting), so a single attempt
        // fails within ~0.25s of the wake — the call invite never arrives and
        // an answered call can never connect (the #51 wake legs). Default to
        // a few backoff-paced retries.
        .unwrap_or(4)
        .min(5) as usize;
    if to.is_none()
        || text.is_none()
        || key.is_none()
        || text.as_deref().is_some_and(str::is_empty)
        || key.as_deref().is_some_and(str::is_empty)
    {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "text is required (use --text, --file, or --stream)".into(),
            false,
        );
    }
    let to = to.unwrap();
    let text = text.unwrap();
    let key = key.unwrap();
    let fingerprint = serde_json::to_string(
        &serde_json::json!({"to": to, "text": text, "conversation": conversation}),
    )
    .unwrap();
    // Diagnostics pass through here on their way to the monitor: keep a copy
    // as queryable views (`docs/mcp-control.md` slice 1). Best-effort.
    let mut state = store.lock().expect("store mutex poisoned");
    crate::diag_views::record(&state.data_dir, &text);
    let identity = match state
        .identities
        .iter()
        .find(|identity| identity.id == identity_id || identity.name == identity_id)
        .cloned()
    {
        Some(identity) => identity,
        None => {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                "no active identity".into(),
                false,
            )
        }
    };
    let peer = match state.peers.iter().find(|peer| {
        peer.identity == identity.id
            && (peer.id == to || peer.name == to || peer.aliases.iter().any(|alias| alias == &to))
    }) {
        Some(peer) => peer.clone(),
        None => {
            tracing::warn!(target: "idfon.daemon", identity = %identity.id, target_peer = %to, "message send peer lookup failed");
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                "peer not found".into(),
                false,
            );
        }
    };
    tracing::info!(target: "idfon.daemon", identity = %identity.id, target_peer = %to, peer = %peer.id, endpoint_id = ?peer.endpoint_id, endpoint_addr_len = ?peer.endpoint_addr.as_ref().map(String::len), key = %key, "message send peer resolved");
    let is_self = identity
        .public_key
        .as_deref()
        .is_some_and(|public_key| public_key == peer.id)
        || identity
            .endpoint_id
            .as_deref()
            .is_some_and(|endpoint_id| peer.endpoint_id.as_deref() == Some(endpoint_id));
    if is_self {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "cannot send a message to the sending identity".into(),
            false,
        );
    }
    if !has_grant_for_conversation(
        &state,
        &identity.id,
        &peer.id,
        &idfon_protocol::Capability::MessageSend,
        conversation.as_deref(),
    ) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::CapabilityDenied,
            "message.send capability denied".into(),
            false,
        );
    }
    // A picture of your screen (an `IDFON-FILE/1` marked `screen=1`) leaves
    // only for a peer you let see it: the `screen.read` grant (the app makes
    // a short one for "Allow once"). The app asks; the daemon holds the line
    // even if the app gets it wrong (idfon-home docs/ios-ui-audit.md §5.4).
    if is_screen_capture(&text)
        && !has_grant(&state, &identity.id, &peer.id, &idfon_protocol::Capability::new(SCREEN_READ))
    {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::CapabilityDenied,
            "screen.read capability denied".into(),
            false,
        );
    }
    // A successful send also establishes the local receive side of the reply path.
    if !state.grants.iter().any(|grant| {
        grant.identity == identity.id
            && grant.subject == peer.id
            && grant.capability == idfon_protocol::Capability::MessageReceive
    }) {
        state.grants.push(idfon_protocol::CapabilityGrant {
            capability: idfon_protocol::Capability::MessageReceive,
            identity: identity.id.clone(),
            subject: peer.id.clone(),
            conversation: None,
            active_at: "0".into(),
            expires_at: None,
            revision: 1,
            revoked_at: None,
        });
    }
    if let Some(existing) = state.operations.iter().find(|operation| {
        operation.target.as_deref() == Some(peer.id.as_str())
            && operation.idempotency_key.as_deref() == Some(&key)
    }) {
        if existing.request_fingerprint.as_deref() != Some(fingerprint.as_str()) {
            // Log it: clients reuse keys after restarts and a silent conflict
            // looks like a vanished message (found via a stuck video call).
            tracing::warn!(target: "idfon.daemon", identity = %identity.id, key = %key, existing_operation = %existing.operation_id, "message send idempotency conflict");
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::IdempotencyKeyConflict,
                "idempotency key was reused with different content".into(),
                false,
            );
        }
        return success(
            request,
            serde_json::json!({"operation_id": existing.operation_id, "status": existing.status}),
        );
    }
    let signing_key = match state.identity_key(&identity.id) {
        Ok(key) => key,
        Err(error) => {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::Internal,
                error.to_string(),
                false,
            )
        }
    };
    let message_id = format!("msg_{}", state.operations.len() + 1);
    let mut envelope = match idfon_core::sign_message_with_ticket(
        &signing_key,
        identity.endpoint_id.unwrap_or_default(),
        message_id.clone(),
        idfon_protocol::MessageContent::Text { text: text.into() },
        &key,
        conversation,
        capability_ticket,
    ) {
        Ok(envelope) => envelope,
        Err(error) => {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::Internal,
                error.to_string(),
                false,
            )
        }
    };
    // Correlation id for the logical send. A caller-supplied `trace` wins;
    // otherwise mint one so every outbound flow is joinable end to end.
    envelope.trace =
        Some(request_text(&request.params, "trace").unwrap_or_else(idfon_core::new_traceparent));
    // Advertise this process's telemetry participation alongside the trace.
    envelope.telemetry = Some(idfon_telemetry::mode().to_string());
    // Caller-pushed per-turn context (e.g. the active per-contact speech
    // settings). Unsigned like `trace`; bounded so it cannot bloat the envelope.
    envelope.context = request_text(&request.params, "context")
        .filter(|value| !value.is_empty())
        .map(|value| value.chars().take(MAX_CONTEXT_CHARS).collect());
    // Added with an introduction and nothing heard from them yet: tell them
    // who introduced us (docs/protocol.md "Introductions").
    envelope.introduction = introductions::pending_token(&state, &identity.id, &peer.id);
    let timestamp = now();
    let operation = idfon_protocol::Operation {
        identity: identity.id.clone(),
        operation_id: format!("op_{message_id}"),
        method: request.method.clone(),
        status: idfon_protocol::OperationStatus::Queued,
        target: Some(peer.id.clone()),
        request_fingerprint: Some(fingerprint),
        created_at: timestamp.clone(),
        updated_at: timestamp,
        idempotency_key: Some(key.into()),
        message_id: Some(envelope.message_id.clone()),
        outbound: Some(envelope.clone()),
        delivery: delivery_policy.clone(),
        durable,
    };
    state.operations.push(operation.clone());
    let result = state.save(&state.data_dir);
    drop(state);
    if let Err(error) = result {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    tracing::info!(
        target: "idfon.daemon",
        identity = %operation.identity,
        operation_id = %operation.operation_id,
        message_id = %operation.message_id.as_deref().unwrap_or(""),
        peer = %peer.id,
        trace = ?envelope.trace,
        "message send queued"
    );
    let operation_id = operation.operation_id.clone();
    let response_message_id = envelope.message_id.clone();
    let worker_store = Arc::clone(store);
    let worker_transport = Arc::clone(transport);
    let worker_envelope = envelope.clone();
    let worker_peer = peer.clone();
    let worker_policy = delivery_policy.clone();
    let worker_identity = identity.id.clone();
    tokio::spawn(async move {
        let transition = tokio::task::spawn_blocking({
            let store = Arc::clone(&worker_store);
            let operation_id = operation_id.clone();
            let identity = worker_identity.clone();
            move || {
                update_operation(
                    &store,
                    &operation_id,
                    &identity,
                    idfon_protocol::OperationStatus::Transmitting,
                )
            }
        })
        .await;
        if transition.is_err() || transition.ok().and_then(Result::err).is_some() {
            return;
        }
        let mut delivery = Err(io::Error::other("no transport attempt"));
        for attempt in 0..=retries {
            tracing::info!(target: "idfon.daemon", identity = %worker_identity, operation_id = %operation_id, message_id = %worker_envelope.message_id, attempt, "message send transport attempt");
            if is_cancelled(&worker_store, &operation_id, &worker_identity) {
                let _ = update_operation(
                    &worker_store,
                    &operation_id,
                    &worker_identity,
                    idfon_protocol::OperationStatus::Cancelled,
                );
                return;
            }
            let transport = Arc::clone(&worker_transport);
            let peer = worker_peer.clone();
            let envelope = worker_envelope.clone();
            let identity = worker_identity.clone();
            let policy = worker_policy.clone();
            delivery = match tokio::task::spawn_blocking(move || {
                transport.send(&identity, &peer, &envelope, policy.as_ref())
            })
            .await
            {
                Ok(result) => result,
                Err(error) => Err(io::Error::other(error)),
            };
            match &delivery {
                Ok(ack) => {
                    tracing::info!(target: "idfon.daemon", identity = %worker_identity, operation_id = %operation_id, message_id = %worker_envelope.message_id, status = ?ack.status, "message send delivered")
                }
                Err(error) => {
                    tracing::error!(target: "idfon.daemon", identity = %worker_identity, operation_id = %operation_id, message_id = %worker_envelope.message_id, error = %error, "message send attempt failed")
                }
            }
            if delivery.is_ok() {
                break;
            }
            if attempt < retries {
                // Backoff: the peer may just be waking (transport cold).
                tokio::time::sleep(std::time::Duration::from_millis(500 << attempt.min(3))).await;
            }
        }
        let status = final_send_status(delivery.is_ok(), durable);
        tracing::info!(
            target: "idfon.daemon",
            identity = %worker_identity,
            operation_id = %operation_id,
            message_id = %worker_envelope.message_id,
            status = ?status,
            "message send finished"
        );
        let identity = worker_identity.clone();
        let _ = tokio::task::spawn_blocking(move || {
            update_operation(&worker_store, &operation_id, &identity, status)
        })
        .await;
    });
    success(
        request,
        serde_json::json!({"operation_id": operation.operation_id, "message_id": response_message_id, "status": "queued", "authenticated": true}),
    )
}

fn is_cancelled(store: &Arc<Mutex<Store>>, operation_id: &str, identity: &str) -> bool {
    store
        .lock()
        .expect("store mutex poisoned")
        .operations
        .iter()
        .any(|operation| {
            operation.operation_id == operation_id
                && operation.identity == identity
                && operation.status == idfon_protocol::OperationStatus::Cancelled
        })
}

fn operation_cancel(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(id) = request
        .params
        .get("operation_id")
        .and_then(serde_json::Value::as_str)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "operation_id is required".into(),
            false,
        );
    };
    let mut state = store.lock().expect("store mutex poisoned");
    let Some(operation) = state.operations.iter_mut().find(|operation| {
        operation.operation_id == id
            && request_text(&request.params, "identity")
                .is_none_or(|identity| operation.identity == identity)
    }) else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "operation not found".into(),
            false,
        );
    };
    if matches!(
        operation.status,
        idfon_protocol::OperationStatus::Delivered
            | idfon_protocol::OperationStatus::Failed
            | idfon_protocol::OperationStatus::Expired
    ) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "operation is already terminal".into(),
            false,
        );
    }
    operation.status = idfon_protocol::OperationStatus::Cancelled;
    operation.updated_at = now();
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(
        request,
        serde_json::json!({"operation_id": id, "status": "cancelled"}),
    )
}

fn update_operation(
    store: &Arc<Mutex<Store>>,
    operation_id: &str,
    identity: &str,
    status: idfon_protocol::OperationStatus,
) -> io::Result<()> {
    let mut state = store.lock().expect("store mutex poisoned");
    set_operation_status(&mut state, operation_id, identity, status);
    let data_dir = state.data_dir.clone();
    state.save(&data_dir)
}

/// The status change and its `operation.state_changed` event, under a held
/// store lock (no save).
fn set_operation_status(
    state: &mut Store,
    operation_id: &str,
    identity: &str,
    status: idfon_protocol::OperationStatus,
) {
    let Some(operation) = state
        .operations
        .iter_mut()
        .find(|operation| operation.operation_id == operation_id && operation.identity == identity)
    else {
        return;
    };
    tracing::info!(target: "idfon.daemon", identity = %identity, operation_id = %operation_id, message_id = ?operation.message_id, status = ?status, "operation state changed");
    operation.status = status.clone();
    operation.updated_at = now();
    let message_id = operation.message_id.clone();
    let event_number = next_event_number(&state.events);
    let cursor = format!("cur_{event_number:020}");
    state.events.push(idfon_protocol::Event {
        event_id: format!("evt_{operation_id}_{event_number}"),
        cursor,
        r#type: "operation.state_changed".into(),
        timestamp: now(),
        identity: "default".into(),
        data: serde_json::json!({"operation_id": operation_id, "status": status, "message_id": message_id}),
    });
    if state.events.len() > EVENT_RETENTION {
        let excess = state.events.len() - EVENT_RETENTION;
        state.events.drain(..excess);
    }
}

fn receive_message(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let envelope =
        match serde_json::from_value::<idfon_protocol::MessageEnvelope>(request.params.clone()) {
            Ok(envelope) => envelope,
            Err(error) => {
                return error_response(
                    request.id.clone(),
                    &request.method,
                    ErrorCode::InvalidRequest,
                    format!("invalid message envelope: {error}"),
                    false,
                )
            }
        };
    if let Err(error) = idfon_core::verify_message(&envelope) {
        tracing::error!(
            target: "idfon.daemon",
            message_id = %envelope.message_id,
            sender = %envelope.sender.peer_id,
            error = %error,
            "message receive authentication failed"
        );
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Unauthorized,
            error.to_string(),
            false,
        );
    }
    let state = store.lock().expect("store mutex poisoned");
    let receiving_identity =
        request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    tracing::info!(target: "idfon.daemon", identity = %receiving_identity, message_id = %envelope.message_id, sender = %envelope.sender.peer_id, sender_endpoint = %envelope.sender.endpoint_id, trace = ?envelope.trace, telemetry = ?envelope.telemetry, "message receive authenticated");
    if let Some(token) = &envelope.capability_ticket {
        let Ok(ticket) = idfon_core::CapabilityTicket::decode(token) else {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::CapabilityDenied,
                "invalid or expired capability ticket".into(),
                false,
            );
        };
        if ticket.issuer()
            != state
                .identities
                .iter()
                .find(|identity| identity.id == receiving_identity)
                .and_then(|identity| identity.public_key.clone())
                .unwrap_or_default()
            || ticket
                .subject()
                .as_deref()
                .is_some_and(|subject| subject != envelope.sender.peer_id)
            || ticket
                .conversation()
                .is_some_and(|scope| Some(scope) != envelope.conversation.as_deref())
            || !ticket
                .capabilities()
                .contains(&idfon_protocol::Capability::MessageReceive)
            || state
                .revoked_tickets
                .iter()
                .any(|id| id == ticket.ticket_id())
            || ticket.is_expired(now().parse::<u64>().unwrap_or(0))
        {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::CapabilityDenied,
                "invalid or expired capability ticket".into(),
                false,
            );
        }
    }
    // Split identities (#56): a peer record may be keyed on either the
    // sender's identity key (`sender.peer_id` — what everything downstream is
    // attributed to) or its iroh endpoint id (`sender.endpoint_id`), because
    // pairing by endpoint ref records the latter while messages always sign
    // with the former. Attribution and grants stay keyed on the
    // signature-verified signer, so accepting either key only widens "is a
    // known record" — never whose conversation a message lands in.
    //
    // An exact signer match is sufficient on its own: the signature is the
    // authentication, and the endpoint claim is routing metadata that can lag
    // reality (a lost endpoint key silently falls back to the identity key, so
    // the claim no longer matches the record's known endpoint — the fourth
    // #56 bite, which cost a wake-on-call E2E leg). The endpoint conjunct
    // remains for the endpoint-keyed match, where it is the only binding.
    let known_peer = state.peers.iter().any(|peer| {
        peer.identity == receiving_identity
            && (peer.id == envelope.sender.peer_id
                || (!envelope.sender.endpoint_id.is_empty()
                    && peer.id == envelope.sender.endpoint_id
                    && peer.knows_endpoint(&envelope.sender.endpoint_id)))
    });
    // A verified ticket (issuer = this identity, unexpired, unrevoked,
    // subject-bound, contains message.receive) is the sender's standing
    // authorization: it satisfies the gate even when the grant has not been
    // materialized yet — always the case on a fresh cross-daemon pairing.
    let allowed = envelope.capability_ticket.is_some()
        || has_grant_for_conversation(
            &state,
            &receiving_identity,
            &envelope.sender.peer_id,
            &idfon_protocol::Capability::MessageReceive,
            envelope.conversation.as_deref(),
        );
    drop(state);
    if !known_peer {
        // Not a contact: hold plain text as a contact request instead of
        // refusing it (docs/protocol.md "Contact requests").
        return hold_contact_request(request, store, &receiving_identity, &envelope);
    }
    if !allowed {
        tracing::warn!(target: "idfon.daemon", identity = %receiving_identity, message_id = %envelope.message_id, sender = %envelope.sender.peer_id, trace = ?envelope.trace, "message receive rejected: capability denied");
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::CapabilityDenied,
            "sender lacks message.receive capability".into(),
            false,
        );
    }
    tracing::info!(
        target: "idfon.daemon",
        identity = %receiving_identity,
        message_id = %envelope.message_id,
        sender = %envelope.sender.peer_id,
        trace = ?envelope.trace,
        telemetry = ?envelope.telemetry,
        "message receive accepted"
    );
    let mut state = store.lock().expect("store mutex poisoned");
    // A message from them: they know who we are, so stop attaching the
    // introduction (persisted now, since a duplicate returns early).
    if introductions::clear_pending(&mut state, &receiving_identity, &envelope.sender.peer_id) {
        let data_dir = state.data_dir.clone();
        if let Err(error) = state.save(&data_dir) {
            tracing::warn!(target: "idfon.daemon", error = %error, "could not persist the cleared introduction");
        }
    }
    // Receiving an authorized message establishes the reciprocal reply path.
    // Do not resurrect a grant that was explicitly revoked or expired.
    if !state.grants.iter().any(|grant| {
        grant.identity == receiving_identity
            && grant.subject == envelope.sender.peer_id
            && grant.capability == idfon_protocol::Capability::MessageSend
    }) {
        state.grants.push(idfon_protocol::CapabilityGrant {
            capability: idfon_protocol::Capability::MessageSend,
            identity: receiving_identity.clone(),
            subject: envelope.sender.peer_id.clone(),
            conversation: envelope.conversation.clone(),
            active_at: "0".into(),
            expires_at: None,
            revision: 1,
            revoked_at: None,
        });
    }
    // The sender presented this identity's verified ticket: materialize its
    // capabilities as grants (message.receive, live.audio.subscribe, …) so
    // replies and live sessions work across daemons, where pairing creates
    // no grants (no shared store). Same dedup rule as above.
    if let Some(ticket) = envelope
        .capability_ticket
        .as_deref()
        .and_then(|token| idfon_core::CapabilityTicket::decode(token).ok())
    {
        for capability in ticket.capabilities() {
            if !state.grants.iter().any(|grant| {
                grant.identity == receiving_identity
                    && grant.subject == envelope.sender.peer_id
                    && grant.capability == *capability
            }) {
                state.grants.push(idfon_protocol::CapabilityGrant {
                    capability: capability.clone(),
                    identity: receiving_identity.clone(),
                    subject: envelope.sender.peer_id.clone(),
                    conversation: envelope.conversation.clone(),
                    active_at: "0".into(),
                    expires_at: None,
                    revision: 1,
                    revoked_at: None,
                });
            }
        }
    }
    if let Some(existing) = state.messages.iter().find(|message| {
        message.sender.peer_id == envelope.sender.peer_id
            && message.idempotency_key == envelope.idempotency_key
            && message.conversation == envelope.conversation
    }) {
        if existing.content != envelope.content {
            tracing::warn!(target: "idfon.daemon", identity = %receiving_identity, message_id = %envelope.message_id, sender = %envelope.sender.peer_id, "message receive rejected: duplicate key with different content");
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::IdempotencyKeyConflict,
                "idempotency key was reused with different content".into(),
                false,
            );
        }
        tracing::info!(
            target: "idfon.daemon",
            identity = %receiving_identity,
            message_id = %existing.message_id,
            sender = %envelope.sender.peer_id,
            trace = ?envelope.trace,
            "message receive duplicate"
        );
        return success(
            request,
            serde_json::json!({
                "message_id": existing.message_id,
                "status": "duplicate",
            }),
        );
    }
    let now = now();
    let operation = idfon_protocol::Operation {
        identity: receiving_identity.clone(),
        operation_id: format!("op_{}", envelope.message_id),
        method: request.method.clone(),
        status: idfon_protocol::OperationStatus::Delivered,
        target: Some(envelope.sender.peer_id.clone()),
        request_fingerprint: None,
        created_at: now.clone(),
        updated_at: now.clone(),
        idempotency_key: Some(envelope.idempotency_key.clone()),
        message_id: Some(envelope.message_id.clone()),
        outbound: None,
        delivery: None,
        durable: false,
    };
    state.messages.push(envelope.clone());
    state.operations.push(operation.clone());
    let cursor = format!("cur_{:020}", next_event_number(&state.events));
    let identity = request_text(&request.params, "identity").unwrap_or_else(|| {
        state
            .identities
            .iter()
            .find(|identity| identity.active)
            .map(|identity| identity.id.clone())
            .unwrap_or_default()
    });
    state.events.push(idfon_protocol::Event {
        event_id: format!("evt_{}", envelope.message_id),
        cursor,
        r#type: "message.received".into(),
        timestamp: now,
        identity,
        // `idempotency_key` is the sender's key for this message: unlike
        // `message_id` (per recipient), a room send carries one base key to
        // every member, so apps use it to name a message (reactions, edits,
        // receipts: docs/chat-envelopes.md).
        data: serde_json::json!({"message_id": envelope.message_id, "peer_id": envelope.sender.peer_id, "conversation": envelope.conversation, "idempotency_key": envelope.idempotency_key, "text": match &envelope.content { idfon_protocol::MessageContent::Text { text } => text }}),
    });
    if state.events.len() > EVENT_RETENTION {
        let excess = state.events.len() - EVENT_RETENTION;
        state.events.drain(..excess);
    }
    // Group-call signaling (G1): room controls update the room's call.
    let idfon_protocol::MessageContent::Text { text } = &envelope.content;
    let room_control = room_calls::on_received(
        &mut state,
        &receiving_identity,
        &envelope.sender.peer_id,
        envelope.conversation.as_deref(),
        text,
    );
    let result = state.save(&state.data_dir);
    drop(state);
    if room_control {
        room_calls::flush_live(store);
    }
    if let Err(error) = result {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(
        request,
        serde_json::json!({
            "message_id": operation.message_id,
            "operation_id": operation.operation_id,
            "status": "delivered",
        }),
    )
}

/// Param the transport receiver adds to `message.receive`: the sender's dial
/// address as the connection saw it (JSON text). Kept with a contact request.
const REMOTE_ADDR_PARAM: &str = "remote_endpoint_addr";
/// Contact-request bounds (docs/protocol.md "Contact requests"): pending
/// senders per identity (the oldest is dropped), messages held per sender,
/// and bytes per held text.
const MAX_REQUEST_SENDERS: usize = 64;
const MAX_REQUEST_MESSAGES: usize = 20;
const MAX_REQUEST_TEXT_BYTES: usize = 8 * 1024;

/// An `IDFON-*/N` envelope (a file, call, receipt, card…): the first line
/// names it. Never held as a contact request.
fn is_envelope(text: &str) -> bool {
    let first = text.lines().next().unwrap_or_default().trim_end();
    first
        .strip_prefix("IDFON-")
        .and_then(|rest| rest.split_once('/'))
        .is_some_and(|(name, version)| {
            !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '-')
                && !version.is_empty()
                && version.chars().all(|c| c.is_ascii_digit())
        })
}

/// The refusal an unknown sender got before contact requests, unchanged so a
/// blocked sender can't tell they are blocked.
fn refuse_unknown_sender(
    request: &Request,
    identity: &str,
    envelope: &idfon_protocol::MessageEnvelope,
    reason: &str,
) -> Response {
    tracing::warn!(target: "idfon.daemon", identity = %identity, message_id = %envelope.message_id, sender = %envelope.sender.peer_id, sender_endpoint = %envelope.sender.endpoint_id, trace = ?envelope.trace, reason, "message receive rejected: unknown peer");
    error_response(
        request.id.clone(),
        &request.method,
        ErrorCode::Unauthorized,
        "sender is not a persisted peer".into(),
        false,
    )
}

fn contact_request_id(identity: &str, peer_id: &str, message_id: &str) -> String {
    let mut hasher = Sha256::new();
    for part in [identity, peer_id, message_id] {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    let digest = hasher.finalize();
    let hex: String = digest[..8].iter().map(|byte| format!("{byte:02x}")).collect();
    format!("req_{hex}")
}

fn push_event(
    state: &mut Store,
    identity: &str,
    event_id: String,
    event_type: &str,
    data: serde_json::Value,
) {
    let cursor = format!("cur_{:020}", next_event_number(&state.events));
    state.events.push(idfon_protocol::Event {
        event_id,
        cursor,
        r#type: event_type.into(),
        timestamp: now(),
        identity: identity.into(),
        data,
    });
    if state.events.len() > EVENT_RETENTION {
        let excess = state.events.len() - EVENT_RETENTION;
        state.events.drain(..excess);
    }
}

/// A verified message from a sender who isn't a peer: hold plain text as a
/// contact request and acknowledge it, so the sender's outbox marks it
/// delivered. Envelopes (files and the like), blocked senders and messages
/// past the bounds are refused.
fn hold_contact_request(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    identity: &str,
    envelope: &idfon_protocol::MessageEnvelope,
) -> Response {
    let idfon_protocol::MessageContent::Text { text } = &envelope.content;
    let sender = &envelope.sender.peer_id;
    let mut state = store.lock().expect("store mutex poisoned");
    if state
        .blocked_senders
        .iter()
        .any(|blocked| blocked.identity == identity && &blocked.peer_id == sender)
    {
        return refuse_unknown_sender(request, identity, envelope, "blocked");
    }
    if is_envelope(text) {
        return refuse_unknown_sender(request, identity, envelope, "not text");
    }
    let refuse = |message: &str| {
        tracing::warn!(target: "idfon.daemon", identity = %identity, message_id = %envelope.message_id, sender = %sender, reason = message, "contact request refused");
        error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::CapabilityDenied,
            message.into(),
            false,
        )
    };
    if text.len() > MAX_REQUEST_TEXT_BYTES {
        return refuse("message is too long for a contact request");
    }
    // Who introduced them, when the message carries a token valid for this
    // identity and this sender; anything else is dropped silently. It never
    // accepts anything (docs/protocol.md "Introductions").
    let introduction = envelope
        .introduction
        .as_deref()
        .and_then(|token| introductions::request_introduction(&state, identity, sender, token));
    let position = state
        .contact_requests
        .iter()
        .position(|held| held.identity == identity && &held.peer_id == sender);
    if let Some(index) = position {
        let held = &state.contact_requests[index];
        if let Some(existing) = held.messages.iter().find(|message| {
            message.idempotency_key == envelope.idempotency_key
                && message.conversation == envelope.conversation
        }) {
            if &existing.text != text {
                return error_response(
                    request.id.clone(),
                    &request.method,
                    ErrorCode::IdempotencyKeyConflict,
                    "idempotency key was reused with different content".into(),
                    false,
                );
            }
            return success(
                request,
                serde_json::json!({"message_id": existing.message_id, "status": "duplicate"}),
            );
        }
        if held.messages.len() >= MAX_REQUEST_MESSAGES {
            return refuse("contact request is full");
        }
    }
    let received_at = now();
    let request_id = match position {
        Some(index) => state.contact_requests[index].request_id.clone(),
        None => {
            while state
                .contact_requests
                .iter()
                .filter(|held| held.identity == identity)
                .count()
                >= MAX_REQUEST_SENDERS
            {
                let oldest = state
                    .contact_requests
                    .iter()
                    .position(|held| held.identity == identity)
                    .expect("counted above");
                let dropped = state.contact_requests.remove(oldest);
                tracing::info!(target: "idfon.daemon", identity = %identity, request_id = %dropped.request_id, "contact request dropped: too many pending");
            }
            let request_id = contact_request_id(identity, sender, &envelope.message_id);
            state.contact_requests.push(idfon_protocol::ContactRequest {
                request_id: request_id.clone(),
                identity: identity.into(),
                peer_id: sender.clone(),
                endpoint_id: envelope.sender.endpoint_id.clone(),
                endpoint_addr: None,
                received_at: received_at.clone(),
                messages: Vec::new(),
                introduction: None,
            });
            request_id
        }
    };
    let held = state
        .contact_requests
        .iter_mut()
        .find(|held| held.request_id == request_id)
        .expect("held above");
    // The latest claim and address win: they are what a reply should dial.
    if !envelope.sender.endpoint_id.is_empty() {
        held.endpoint_id = envelope.sender.endpoint_id.clone();
    }
    // Only an address for the signed endpoint claim: room gossip hands raw
    // JSON to `message.receive`, so a relayer could add this param.
    if let Some(address) = request_text(&request.params, REMOTE_ADDR_PARAM).filter(|address| {
        serde_json::from_str::<EndpointAddr>(address)
            .is_ok_and(|parsed| parsed.id.to_string() == envelope.sender.endpoint_id)
    }) {
        held.endpoint_addr = Some(address);
    }
    held.messages.push(idfon_protocol::HeldMessage {
        message_id: envelope.message_id.clone(),
        text: text.clone(),
        conversation: envelope.conversation.clone(),
        idempotency_key: envelope.idempotency_key.clone(),
        received_at,
    });
    // The first valid introduction is kept.
    if held.introduction.is_none() {
        held.introduction = introduction;
    }
    let endpoint_id = held.endpoint_id.clone();
    let mut data = serde_json::json!({
        "request_id": request_id,
        "peer_id": sender,
        "endpoint_id": endpoint_id,
        "conversation": envelope.conversation,
        "text": text,
        "idempotency_key": envelope.idempotency_key,
    });
    if let Some(introduction) = &held.introduction {
        data["introduction"] = serde_json::to_value(introduction).expect("introduction serializes");
    }
    push_event(
        &mut state,
        identity,
        format!("evt_req_{}", envelope.message_id),
        "contact.request",
        data,
    );
    tracing::info!(target: "idfon.daemon", identity = %identity, message_id = %envelope.message_id, sender = %sender, request_id = %request_id, "message held as contact request");
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(
        request,
        serde_json::json!({
            "message_id": envelope.message_id,
            "status": "held",
            "request_id": request_id,
        }),
    )
}

/// `request.list` — pending contact requests, newest first.
fn request_list(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let identity = request_text(&request.params, "identity");
    let state = store.lock().expect("store mutex poisoned");
    let requests: Vec<_> = state
        .contact_requests
        .iter()
        .rev()
        .filter(|held| identity.as_deref().is_none_or(|value| held.identity == value))
        .cloned()
        .map(|mut held| {
            introductions::refresh_request(&state, &mut held);
            held
        })
        .collect();
    success(request, serde_json::json!({"requests": requests}))
}

/// A short, unused contact name for an accepted sender: a prefix of its id.
fn short_peer_name(state: &Store, identity: &str, peer_id: &str) -> String {
    let taken = |name: &str| {
        state
            .peers
            .iter()
            .any(|peer| peer.identity == identity && peer.name == name)
    };
    let chars: Vec<char> = peer_id.chars().collect();
    (8..chars.len())
        .step_by(4)
        .map(|length| chars[..length].iter().collect::<String>())
        .find(|name| !taken(name))
        .unwrap_or_else(|| peer_id.to_owned())
}

/// `request.accept` — make the sender a peer, grant `message.send` and
/// `message.receive` (scoped to `conversation` when given), then deliver the
/// held messages the grant covers as ordinary `message.received` events.
fn request_accept(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let invalid = |message: &str| {
        error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            message.into(),
            false,
        )
    };
    let Some(request_id) = request_text(&request.params, "request_id") else {
        return invalid("request_id is required");
    };
    let name = request_text(&request.params, "name").filter(|value| !value.trim().is_empty());
    let conversation =
        request_text(&request.params, "conversation").filter(|value| !value.is_empty());
    let mut state = store.lock().expect("store mutex poisoned");
    let Some(index) = state
        .contact_requests
        .iter()
        .position(|held| held.request_id == request_id)
    else {
        return invalid("contact request not found");
    };
    let held = state.contact_requests[index].clone();
    let identity = held.identity.clone();
    let existing = state
        .peers
        .iter()
        .find(|peer| peer.identity == identity && peer.id == held.peer_id)
        .cloned();
    let peer = match existing {
        // Added some other way since: keep that record as it is.
        Some(peer) => peer,
        None => {
            let name = match name {
                Some(name) => {
                    if state
                        .peers
                        .iter()
                        .any(|peer| peer.identity == identity && peer.name == name)
                    {
                        return invalid("peer name already exists");
                    }
                    name
                }
                None => short_peer_name(&state, &identity, &held.peer_id),
            };
            // No address from the connection (the message came over a room's
            // gossip): an id-only address, which iroh resolves by discovery.
            let endpoint_addr = held.endpoint_addr.clone().or_else(|| {
                held.endpoint_id
                    .parse::<iroh::EndpointId>()
                    .ok()
                    .and_then(|id| serde_json::to_string(&EndpointAddr::new(id)).ok())
            });
            let mut peer = idfon_protocol::Peer {
                id: held.peer_id.clone(),
                identity: identity.clone(),
                name,
                endpoint_id: (!held.endpoint_id.is_empty()).then(|| held.endpoint_id.clone()),
                endpoint_addr,
                devices: Vec::new(),
                aliases: Vec::new(),
                ring_score: None,
                call_mode: None,
                pinned: false,
                instant_call: false,
                tags: Vec::new(),
                visibility: None,
                // Accepting a request that carried an introduction keeps it
                // on the new contact.
                introduced_by: held.introduction.as_ref().map(idfon_protocol::IntroducedBy::from),
                share: None,
            };
            ensure_account_alias(&mut peer);
            state.peers.push(peer.clone());
            record_state_event(
                &mut state,
                &identity,
                "peer.created",
                serde_json::json!({"peer": peer}),
            );
            peer
        }
    };
    state
        .blocked_senders
        .retain(|blocked| !(blocked.identity == identity && blocked.peer_id == peer.id));
    for capability in [
        idfon_protocol::Capability::MessageSend,
        idfon_protocol::Capability::MessageReceive,
    ] {
        if state.grants.iter().any(|grant| {
            grant.identity == identity
                && grant.subject == peer.id
                && grant.capability == capability
                && grant.conversation == conversation
                && grant.revoked_at.is_none()
        }) {
            continue;
        }
        let revision = state
            .grants
            .iter()
            .filter(|grant| {
                grant.identity == identity
                    && grant.subject == peer.id
                    && grant.capability == capability
            })
            .map(|grant| grant.revision)
            .max()
            .unwrap_or(0)
            + 1;
        let grant = idfon_protocol::CapabilityGrant {
            capability,
            identity: identity.clone(),
            subject: peer.id.clone(),
            conversation: conversation.clone(),
            active_at: "0".into(),
            expires_at: None,
            revision,
            revoked_at: None,
        };
        state.grants.push(grant.clone());
        record_state_event(
            &mut state,
            &identity,
            "grant.created",
            serde_json::json!({"grant": grant}),
        );
    }
    // Held messages outside an accepted conversation stay undelivered: the
    // sender is admitted to that conversation only.
    for message in held.messages.iter().filter(|message| {
        conversation.is_none() || message.conversation == conversation
    }) {
        if state.messages.iter().any(|known| {
            known.sender.peer_id == held.peer_id
                && known.idempotency_key == message.idempotency_key
                && known.conversation == message.conversation
        }) {
            continue;
        }
        // `messages` is the receive dedup index; the signature was verified
        // when the message was held.
        state.messages.push(idfon_protocol::MessageEnvelope {
            message_id: message.message_id.clone(),
            sender: idfon_protocol::PeerAuth {
                peer_id: held.peer_id.clone(),
                endpoint_id: held.endpoint_id.clone(),
                signature: String::new(),
            },
            content: idfon_protocol::MessageContent::Text {
                text: message.text.clone(),
            },
            idempotency_key: message.idempotency_key.clone(),
            capability_ticket: None,
            conversation: message.conversation.clone(),
            trace: None,
            telemetry: None,
            context: None,
            introduction: None,
        });
        push_event(
            &mut state,
            &identity,
            format!("evt_{}", message.message_id),
            "message.received",
            serde_json::json!({
                "message_id": message.message_id,
                "peer_id": held.peer_id,
                "conversation": message.conversation,
                "idempotency_key": message.idempotency_key,
                "text": message.text,
            }),
        );
    }
    state.contact_requests.remove(index);
    tracing::info!(target: "idfon.daemon", identity = %identity, request_id = %request_id, peer = %peer.id, conversation = ?conversation, "contact request accepted");
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(request, serde_json::json!({"peer": peer}))
}

/// `request.decline` — drop a contact request; `block` also refuses the
/// sender's future messages (until `request.unblock` or they become a peer).
fn request_decline(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(request_id) = request_text(&request.params, "request_id") else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "request_id is required".into(),
            false,
        );
    };
    let block = request
        .params
        .get("block")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let mut state = store.lock().expect("store mutex poisoned");
    let Some(index) = state
        .contact_requests
        .iter()
        .position(|held| held.request_id == request_id)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "contact request not found".into(),
            false,
        );
    };
    let held = state.contact_requests.remove(index);
    if block
        && !state
            .blocked_senders
            .iter()
            .any(|blocked| blocked.identity == held.identity && blocked.peer_id == held.peer_id)
    {
        state.blocked_senders.push(BlockedSender {
            identity: held.identity.clone(),
            peer_id: held.peer_id.clone(),
            blocked_at: now(),
        });
    }
    tracing::info!(target: "idfon.daemon", identity = %held.identity, request_id = %request_id, block, "contact request declined");
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(
        request,
        serde_json::json!({"request_id": request_id, "peer_id": held.peer_id, "blocked": block}),
    )
}

/// `request.unblock` — let a blocked sender make contact requests again.
fn request_unblock(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(peer_id) = request_text(&request.params, "peer_id") else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "peer_id is required".into(),
            false,
        );
    };
    let identity = request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let mut state = store.lock().expect("store mutex poisoned");
    let before = state.blocked_senders.len();
    state
        .blocked_senders
        .retain(|blocked| !(blocked.identity == identity && blocked.peer_id == peer_id));
    let unblocked = state.blocked_senders.len() != before;
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(
        request,
        serde_json::json!({"peer_id": peer_id, "unblocked": unblocked}),
    )
}

fn operation_wait(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let id = request
        .params
        .get("operation_id")
        .and_then(serde_json::Value::as_str);
    let Some(id) = id else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "operation_id is required".into(),
            false,
        );
    };
    let state = store.lock().expect("store mutex poisoned");
    match state.operations.iter().find(|operation| {
        operation.operation_id == id
            && request_text(&request.params, "identity")
                .is_none_or(|identity| operation.identity == identity)
    }) {
        Some(operation) => success(request, serde_json::json!({"operation": operation})),
        None => error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "operation not found".into(),
            false,
        ),
    }
}

fn capability(value: &str) -> Option<idfon_protocol::Capability> {
    (!value.is_empty()).then(|| idfon_protocol::Capability::new(value))
}

/// `mcp.configure` — persist the local command the inbound `idfon/mcp/1`
/// relay splices to. An empty/absent command disables it; `IDFON_MCP_COMMAND`
/// still overrides per process. The relay is registered at startup, so no
/// transport work is needed here: the next inbound connection reads the new
/// value.
fn mcp_configure(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let command = request
        .params
        .get("command")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let mut state = store.lock().expect("store mutex poisoned");
    state.mcp_command = command;
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    let effective = state.configured_mcp_command();
    success(request, serde_json::json!({"command": effective}))
}

/// One grant predicate shared by `access.check` and the MCP relay: an active,
/// unexpired, unrevoked grant of `capability` from `identity` to `subject`.
fn has_grant(
    state: &Store,
    identity: &str,
    subject: &str,
    capability: &idfon_protocol::Capability,
) -> bool {
    has_grant_for_conversation(state, identity, subject, capability, None)
}

fn has_grant_for_conversation(
    state: &Store,
    identity: &str,
    subject: &str,
    capability: &idfon_protocol::Capability,
    conversation: Option<&str>,
) -> bool {
    state.grants.iter().any(|grant| {
        grant.identity == identity
            && grant.subject == subject
            && &grant.capability == capability
            && grant
                .conversation
                .as_deref()
                .is_none_or(|scope| Some(scope) == conversation)
            && grant.revoked_at.is_none()
            && grant
                .expires_at
                .as_deref()
                .is_none_or(|expires| expires > now().as_str())
    })
}

fn capability_ticket_issue(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let identity = request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let subject = request_text(&request.params, "subject");
    let capabilities = request
        .params
        .get("capabilities")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .filter_map(|value| capability(value))
                .collect()
        })
        .unwrap_or_else(|| vec![idfon_protocol::Capability::MessageReceive]);
    let ticket_id =
        request_text(&request.params, "ticket_id").unwrap_or_else(|| format!("ticket-{}", now()));
    let expires_at = request_text(&request.params, "expires_at");
    let state = store.lock().expect("store mutex poisoned");
    let key = state
        .identity_key(&identity)
        .map_err(|error| error.to_string());
    let Ok(key) = key else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "identity not found".into(),
            false,
        );
    };
    let conversation = request_text(&request.params, "conversation");
    let path_scope = request_text(&request.params, "path_scope");
    let Ok(ticket) = idfon_core::TicketSpec {
        subject: subject,
        conversation: conversation,
        capabilities: capabilities,
        expires_at: expires_at,
        ticket_id: ticket_id.into(),
        voice: None,
        path_scope: path_scope,
    }
    .issue(&key) else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "invalid subject or expiry".into(),
            false,
        );
    };
    success(request, serde_json::json!({"ticket": ticket}))
}

fn capability_ticket_revoke(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(ticket_id) = request_text(&request.params, "ticket_id") else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "ticket_id is required".into(),
            false,
        );
    };
    let identity = request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let mut state = store.lock().expect("store mutex poisoned");
    let issuer = state
        .identities
        .iter()
        .find(|item| item.id == identity || item.name == identity)
        .and_then(|item| item.public_key.clone())
        .unwrap_or_default();
    if issuer.is_empty() {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "identity not found".into(),
            false,
        );
    }
    if !state.revoked_tickets.contains(&ticket_id) {
        state.revoked_tickets.push(ticket_id.clone());
    }
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(
        request,
        serde_json::json!({"ticket_id": ticket_id, "issuer": issuer, "revoked": true}),
    )
}

fn access_grant(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(identity) = request
        .params
        .get("identity")
        .and_then(serde_json::Value::as_str)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "identity is required".into(),
            false,
        );
    };
    let Some(subject) = request
        .params
        .get("subject")
        .and_then(serde_json::Value::as_str)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "subject is required".into(),
            false,
        );
    };
    let Some(value) = request
        .params
        .get("capability")
        .and_then(serde_json::Value::as_str)
        .and_then(capability)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "unknown capability".into(),
            false,
        );
    };
    let mut state = store.lock().expect("store mutex poisoned");
    let revision = state
        .grants
        .iter()
        .filter(|grant| {
            grant.identity == identity && grant.subject == subject && grant.capability == value
        })
        .map(|grant| grant.revision)
        .max()
        .unwrap_or(0)
        + 1;
    let grant = idfon_protocol::CapabilityGrant {
        capability: value,
        identity: identity.into(),
        subject: subject.into(),
        conversation: request
            .params
            .get("conversation")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        active_at: request
            .params
            .get("active_at")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("0")
            .into(),
        expires_at: request
            .params
            .get("expires_at")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        revision,
        revoked_at: None,
    };
    state.grants.push(grant.clone());
    record_state_event(
        &mut state,
        identity,
        "grant.created",
        serde_json::json!({"grant": grant}),
    );
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(request, serde_json::json!({"grant": grant}))
}

fn access_revoke(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(identity) = request
        .params
        .get("identity")
        .and_then(serde_json::Value::as_str)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "identity is required".into(),
            false,
        );
    };
    let Some(subject) = request
        .params
        .get("subject")
        .and_then(serde_json::Value::as_str)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "subject is required".into(),
            false,
        );
    };
    let Some(value) = request
        .params
        .get("capability")
        .and_then(serde_json::Value::as_str)
        .and_then(capability)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "unknown capability".into(),
            false,
        );
    };
    let mut state = store.lock().expect("store mutex poisoned");
    let mut changed = false;
    for grant in &mut state.grants {
        if grant.identity == identity
            && grant.subject == subject
            && grant.capability == value
            && grant.revoked_at.is_none()
        {
            grant.revoked_at = Some(now());
            changed = true;
        }
    }
    if !changed {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "grant not found".into(),
            false,
        );
    }
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(request, serde_json::json!({"revoked": true}))
}

fn access_check(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(identity) = request
        .params
        .get("identity")
        .and_then(serde_json::Value::as_str)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "identity is required".into(),
            false,
        );
    };
    let Some(subject) = request
        .params
        .get("subject")
        .and_then(serde_json::Value::as_str)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "subject is required".into(),
            false,
        );
    };
    let Some(value) = request
        .params
        .get("capability")
        .and_then(serde_json::Value::as_str)
        .and_then(capability)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "unknown capability".into(),
            false,
        );
    };
    let state = store.lock().expect("store mutex poisoned");
    let allowed = has_grant(&state, identity, subject, &value);
    success(
        request,
        serde_json::json!({"allowed": allowed, "identity": identity, "subject": subject, "capability": value}),
    )
}

fn media_capability(kind: &idfon_protocol::MediaKind) -> idfon_protocol::Capability {
    match kind {
        idfon_protocol::MediaKind::File => idfon_protocol::Capability::RecordingFetch,
        idfon_protocol::MediaKind::Recording => idfon_protocol::Capability::RecordingFetch,
        idfon_protocol::MediaKind::LiveAudio => idfon_protocol::Capability::LiveAudioSubscribe,
        idfon_protocol::MediaKind::LiveVideo => idfon_protocol::Capability::LiveAudioSubscribe,
    }
}

fn media_session_start(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(identity_ref) = request_text(&request.params, "identity") else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "identity is required".into(),
            false,
        );
    };
    let Some(peer) = request_text(&request.params, "peer") else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "peer is required".into(),
            false,
        );
    };
    let Some(kind) = request
        .params
        .get("kind")
        .and_then(|value| serde_json::from_value(value.clone()).ok())
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "valid media kind is required".into(),
            false,
        );
    };
    let capability = media_capability(&kind);
    tracing::info!(
        target: "idfon.daemon",
        identity = %identity_ref,
        peer = %peer,
        kind = ?kind,
        capability = ?capability,
        "media session start requested"
    );
    let mut state = store.lock().expect("store mutex poisoned");
    let Some(identity) = state
        .identities
        .iter()
        .find(|candidate| candidate.id == identity_ref || candidate.name == identity_ref)
        .map(|candidate| candidate.id.clone())
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "identity not found".into(),
            false,
        );
    };
    // ponytail: recording/file sessions still ride on a MessageSend grant;
    // live audio now requires an explicit LiveAudioSubscribe grant created at
    // pairing time — the callee's Answer button remains the consent gate.
    let granted = |cap: &idfon_protocol::Capability| {
        state.grants.iter().any(|grant| {
            grant.identity == identity
                && grant.subject == peer
                && grant.capability == *cap
                && grant.revoked_at.is_none()
                && grant
                    .expires_at
                    .as_deref()
                    .is_none_or(|expires| expires > now().as_str())
        })
    };
    let allowed = if capability == idfon_protocol::Capability::LiveAudioSubscribe {
        granted(&capability)
    } else {
        granted(&capability) || granted(&idfon_protocol::Capability::MessageSend)
    };
    if !allowed {
        tracing::warn!(target: "idfon.daemon", identity = %identity, peer = %peer, capability = ?capability, "media session start rejected: capability denied");
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::CapabilityDenied,
            "media capability denied".into(),
            false,
        );
    }
    let session = idfon_protocol::MediaSession {
        session_id: format!("session_{}", state.sessions.len() + 1),
        identity: identity.clone(),
        peer: peer.clone(),
        conversation: request
            .params
            .get("conversation")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        kind,
        capability,
        active: true,
        created_at: now(),
    };
    let media_handle = MEDIA_SERVICE.get().map(|service| {
        service.start(
            session.session_id.clone(),
            session.identity.clone(),
            session.peer.clone(),
            session.kind.clone(),
            session.capability.clone(),
        )
    });
    let mode = request
        .params
        .get("mode")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("record");
    tracing::info!(
        target: "idfon.daemon",
        session_id = %session.session_id,
        identity = %session.identity,
        peer = %session.peer,
        mode = %mode,
        "media session authorized"
    );
    let live_ticket = if session.kind == idfon_protocol::MediaKind::LiveAudio && mode == "publish" {
        let Some(handle) = media_handle.clone() else {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::Internal,
                "media service unavailable".into(),
                true,
            );
        };
        match std::thread::spawn(move || {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(handle.start_publisher())
        })
        .join()
        {
            Ok(Ok(ticket)) => Some(ticket),
            Ok(Err(error)) => {
                return error_response(
                    request.id.clone(),
                    &request.method,
                    ErrorCode::Internal,
                    error.to_string(),
                    true,
                )
            }
            Err(_) => {
                return error_response(
                    request.id.clone(),
                    &request.method,
                    ErrorCode::Internal,
                    "media worker panicked".into(),
                    true,
                )
            }
        }
    } else if session.kind == idfon_protocol::MediaKind::LiveAudio && mode == "subscribe" {
        let Some(ticket) = request
            .params
            .get("ticket")
            .and_then(serde_json::Value::as_str)
        else {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                "ticket is required".into(),
                false,
            );
        };
        let Some(handle) = media_handle else {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::Internal,
                "media service unavailable".into(),
                true,
            );
        };
        let ticket = ticket.to_owned();
        match std::thread::spawn(move || {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(handle.start_subscriber(&ticket))
        })
        .join()
        {
            Ok(Ok(())) => None,
            Ok(Err(error)) => {
                return error_response(
                    request.id.clone(),
                    &request.method,
                    ErrorCode::Internal,
                    error.to_string(),
                    true,
                )
            }
            Err(_) => {
                return error_response(
                    request.id.clone(),
                    &request.method,
                    ErrorCode::Internal,
                    "media worker panicked".into(),
                    true,
                )
            }
        }
    } else {
        None
    };
    state.sessions.push(session.clone());
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    tracing::info!(
        target: "idfon.daemon",
        session_id = %session.session_id,
        live_ticket_len = ?live_ticket.as_ref().map(String::len),
        "media session started"
    );
    success(
        request,
        serde_json::json!({"session": session, "live_ticket": live_ticket}),
    )
}

fn media_session_stop(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(id) = request
        .params
        .get("session_id")
        .and_then(serde_json::Value::as_str)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "session_id is required".into(),
            false,
        );
    };
    let identity = request_text(&request.params, "identity");
    let mut state = store.lock().expect("store mutex poisoned");
    let Some(session) = state.sessions.iter_mut().find(|session| {
        session.session_id == id
            && identity
                .as_deref()
                .is_none_or(|value| session.identity == value)
    }) else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "session not found".into(),
            false,
        );
    };
    tracing::info!(
        target: "idfon.daemon",
        identity = %session.identity,
        session_id = %session.session_id,
        peer = %session.peer,
        "media session stopping"
    );
    session.active = false;
    if let Some(service) = MEDIA_SERVICE.get() {
        if let Ok(handle) = service.get(id) {
            let _ = std::thread::spawn(move || {
                tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(handle.stop_publisher())
            })
            .join();
        }
        let _ = service.stop(id);
    }
    tracing::info!(
        target: "idfon.daemon",
        identity = %session.identity,
        session_id = %session.session_id,
        "media session stopped"
    );
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(
        request,
        serde_json::json!({"session_id": id, "active": false}),
    )
}

fn media_resource_put(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
) -> Response {
    let Some(id) = request
        .params
        .get("resource_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "resource_id is required".into(),
            false,
        );
    };
    let identity = request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let resource_path = {
        let state = store.lock().expect("store mutex poisoned");
        match staging_file(&state.data_dir, &identity, id) {
            Some(path) => path,
            None => {
                return error_response(
                    request.id.clone(),
                    &request.method,
                    ErrorCode::InvalidRequest,
                    "invalid resource_id".into(),
                    false,
                )
            }
        }
    };
    // Chunked mode: `append: true` appends to the stored resource; `finish:
    // true` publishes the stored resource as a blob and returns its ticket.
    // Together they move payloads larger than one IPC frame.
    if request
        .params
        .get("finish")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return media_resource_finish(request, &resource_path, &identity, id, store, transport);
    }
    let Some(data) = decode_resource_bytes(request) else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "bytes are required".into(),
            false,
        );
    };
    if let Some(response) = reject_oversize(request, data.len()) {
        return response;
    }
    if request
        .params
        .get("append")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return media_resource_append(request, &resource_path, &identity, id, &data);
    }
    let state = store.lock().expect("store mutex poisoned");
    prune_staging(&state.data_dir.join("staging"));
    let Some(path) = staging_file(&state.data_dir, &identity, id) else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "invalid resource_id".into(),
            false,
        );
    };
    if let Some(parent) = path.parent() {
        if let Err(error) = std::fs::create_dir_all(parent) {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::Internal,
                error.to_string(),
                true,
            );
        }
    }
    if let Err(error) = std::fs::write(&path, &data) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    // Served on the identity's own endpoint (relay-admitted), not a blob one.
    let identity_transport =
        transport.and_then(|transport| transport.current_transport(&identity));
    let blob_ticket = match blob::run_put(state.data_dir.join("blobs"), data.clone(), identity_transport) {
        Ok((ticket, _)) => ticket.to_string(),
        Err(error) => {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::Internal,
                error.to_string(),
                true,
            )
        }
    };
    success(
        request,
        serde_json::json!({"identity": identity, "resource_id": id, "size_bytes": data.len(), "content_hash": blake3::hash(&data).to_hex().to_string(), "blob_ticket": blob_ticket}),
    )
}

fn media_resource_append(
    request: &Request,
    path: &std::path::Path,
    identity: &str,
    id: &str,
    bytes: &[u8],
) -> Response {
    if let Some(parent) = path.parent() {
        if let Err(error) = std::fs::create_dir_all(parent) {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::Internal,
                error.to_string(),
                true,
            );
        }
    }
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        Ok(mut file) => {
            if let Err(error) = std::io::Write::write_all(&mut file, bytes) {
                return error_response(
                    request.id.clone(),
                    &request.method,
                    ErrorCode::Internal,
                    error.to_string(),
                    true,
                );
            }
            let size_bytes = file.metadata().map(|meta| meta.len()).unwrap_or(0);
            success(
                request,
                serde_json::json!({"identity": identity, "resource_id": id, "size_bytes": size_bytes}),
            )
        }
        Err(error) => error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        ),
    }
}

fn media_resource_finish(
    request: &Request,
    path: &std::path::Path,
    identity: &str,
    id: &str,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
) -> Response {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                "resource not found".into(),
                false,
            );
        }
        Err(error) => {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::Internal,
                error.to_string(),
                true,
            );
        }
    };
    let data_dir = store.lock().expect("store mutex poisoned").data_dir.clone();
    // Served on the identity's own endpoint (relay-admitted), not a blob one.
    let identity_transport =
        transport.and_then(|transport| transport.current_transport(identity));
    let blob_ticket = match blob::run_put(data_dir.join("blobs"), bytes.clone(), identity_transport) {
        Ok((ticket, _)) => ticket.to_string(),
        Err(error) => {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::Internal,
                error.to_string(),
                true,
            );
        }
    };
    // The staging copy is transient; the ticket is the artifact now.
    let _ = std::fs::remove_file(path);
    success(
        request,
        serde_json::json!({"identity": identity, "resource_id": id, "size_bytes": bytes.len(), "content_hash": blake3::hash(&bytes).to_hex().to_string(), "blob_ticket": blob_ticket}),
    )
}

/// Decodes the `bytes` JSON array into a `Vec<u8>`, rejecting non-byte values.
fn decode_resource_bytes(request: &Request) -> Option<Vec<u8>> {
    let array = request
        .params
        .get("bytes")
        .and_then(serde_json::Value::as_array)?;
    let mut data = Vec::with_capacity(array.len());
    for byte in array {
        data.push(byte.as_u64().and_then(|value| u8::try_from(value).ok())?);
    }
    Some(data)
}

fn reject_oversize(request: &Request, size: usize) -> Option<Response> {
    if size > MAX_RESOURCE_BYTES {
        return Some(error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::FrameTooLarge,
            "resource is too large".into(),
            false,
        ));
    }
    None
}

fn media_resource_fetch(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
) -> Response {
    let Some(id) = request
        .params
        .get("resource_id")
        .and_then(serde_json::Value::as_str)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "resource_id is required".into(),
            false,
        );
    };
    // Only the path is wanted, and the guard must not outlive it: `run_fetch`
    // blocks for the whole transfer (up to its network timeouts), so holding
    // the daemon's store lock across it would stall every other IPC request.
    let data_dir = {
        let state = store.lock().expect("store mutex poisoned");
        state.data_dir.clone()
    };
    let bytes = if let Some(ticket) = request
        .params
        .get("blob_ticket")
        .and_then(serde_json::Value::as_str)
    {
        // Dial from the requesting identity's endpoint (relay-admitted).
        let identity =
            request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
        let endpoint = transport
            .and_then(|transport| transport.current_transport(&identity))
            .map(|transport| transport.endpoint().clone());
        match blob::run_fetch(ticket.into(), data_dir.join("blobs-fetched"), endpoint) {
            Ok(path) => std::fs::read(path),
            Err(error) => {
                return error_response(
                    request.id.clone(),
                    &request.method,
                    ErrorCode::PeerOffline,
                    error.to_string(),
                    true,
                )
            }
        }
    } else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "blob_ticket is required".into(),
            false,
        );
    };
    match bytes {
        Ok(bytes) => {
            let total_size = bytes.len();
            // Chunked reads: `offset`/`length` slice the payload so responses
            // stay under the IPC frame limit. `run_fetch` caches the transfer as
            // a content-verified `{hash}.blob` under blobs-fetched, so the first
            // chunk downloads and later chunks are local reads.
            // ponytail: slice-after-full-download; range requests on the
            // blob protocol would avoid the double transfer for big blobs.
            let offset = request
                .params
                .get("offset")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0) as usize;
            let length = request
                .params
                .get("length")
                .and_then(serde_json::Value::as_u64)
                .map(|value| value as usize);
            let slice = if offset == 0 && length.is_none() {
                &bytes[..]
            } else if offset > total_size {
                return error_response(
                    request.id.clone(),
                    &request.method,
                    ErrorCode::InvalidRequest,
                    "offset is beyond the resource size".into(),
                    false,
                );
            } else {
                let end = length
                    .map(|len| (offset + len).min(total_size))
                    .unwrap_or(total_size);
                &bytes[offset..end]
            };
            success(
                request,
                serde_json::json!({"resource_id": id, "bytes": slice, "total_size": total_size}),
            )
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "resource not found".into(),
            false,
        ),
        Err(error) => error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        ),
    }
}

fn media_sessions(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let state = store.lock().expect("store mutex poisoned");
    let identity = request_text(&request.params, "identity");
    let sessions: Vec<_> = state
        .sessions
        .iter()
        .filter(|session| {
            identity
                .as_deref()
                .is_none_or(|value| session.identity == value)
        })
        .collect();
    success(request, serde_json::json!({"sessions": sessions}))
}

fn policy_set(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let required = ["id", "identity", "subject", "mode", "delivery"];
    if required.iter().any(|field| {
        request
            .params
            .get(*field)
            .and_then(serde_json::Value::as_str)
            .is_none()
    }) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "id, identity, subject, mode, and delivery are required".into(),
            false,
        );
    }
    let start = request
        .params
        .get("schedule_start")
        .and_then(serde_json::Value::as_u64)
        .map(|value| value as u8);
    let end = request
        .params
        .get("schedule_end")
        .and_then(serde_json::Value::as_u64)
        .map(|value| value as u8);
    if start.is_some_and(|value| value > 23)
        || end.is_some_and(|value| value > 24)
        || start.is_some() != end.is_some()
        || start.zip(end).is_some_and(|(start, end)| start >= end)
    {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "schedule must be a valid start/end hour window".into(),
            false,
        );
    }
    let mut state = store.lock().expect("store mutex poisoned");
    let id = request.params["id"].as_str().unwrap();
    let revision = state
        .policies
        .iter()
        .filter(|policy| policy.id == id)
        .map(|policy| policy.revision)
        .max()
        .unwrap_or(0)
        + 1;
    let policy = idfon_protocol::LocalPolicy {
        id: id.into(),
        identity: request.params["identity"].as_str().unwrap().into(),
        subject: request.params["subject"].as_str().unwrap().into(),
        mode: request.params["mode"].as_str().unwrap().into(),
        delivery: request.params["delivery"].as_str().unwrap().into(),
        notify: request
            .params
            .get("notify")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true),
        auto_accept: request
            .params
            .get("auto_accept")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        interrupt: request
            .params
            .get("interrupt")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        record: request
            .params
            .get("record")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        expires_at: request
            .params
            .get("expires_at")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        schedule_start: start,
        schedule_end: end,
        revision,
    };
    state.policies.retain(|existing| existing.id != policy.id);
    state.policies.push(policy.clone());
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(request, serde_json::json!({"policy": policy}))
}

fn policy_dry_run(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let peer = request
        .params
        .get("peer")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let mode = request
        .params
        .get("mode")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("text");
    let state = store.lock().expect("store mutex poisoned");
    let policy = state
        .policies
        .iter()
        .find(|policy| policy.subject == peer && policy.mode == mode);
    let hour = request
        .params
        .get("hour")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0) as u8;
    let decision = policy
        .filter(|policy| {
            policy
                .expires_at
                .as_deref()
                .is_none_or(|expires| expires > now().as_str())
        })
        .filter(|policy| {
            policy
                .schedule_start
                .zip(policy.schedule_end)
                .is_none_or(|(start, end)| hour >= start && hour < end)
        })
        .map(|policy| {
            if policy.auto_accept {
                "accept"
            } else if policy.notify {
                "notify"
            } else {
                "queue"
            }
        })
        .unwrap_or("reject");
    success(
        request,
        serde_json::json!({"decision": decision, "peer": peer, "mode": mode}),
    )
}

/// D20 management: rename an identity's display name. The id — and with it
/// every endpoint key, peer record, and paired ticket — never changes.
fn identity_rename(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(name) = request
        .params
        .get("name")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "name is required".into(),
            false,
        );
    };
    let identity_ref = request_text(&request.params, "identity").unwrap_or_default();
    let mut state = store.lock().expect("store mutex poisoned");
    let Some(index) = state
        .identities
        .iter()
        .position(|identity| identity.id == identity_ref || identity.name == identity_ref)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "unknown identity".into(),
            false,
        );
    };
    if state
        .identities
        .iter()
        .enumerate()
        .any(|(other, identity)| other != index && (identity.name == name || identity.id == name))
    {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "identity already exists".into(),
            false,
        );
    }
    state.identities[index].name = name.to_owned();
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(
        request,
        serde_json::json!({"identity": state.identities[index].id, "name": name}),
    )
}

/// The sender's ring score from `peer.add`/`peer.update`: `Ok(None)` when
/// absent (leave it alone), `Ok(Some(None))` for `"auto"` (clear: the shell's
/// default for the contact's kind), `Ok(Some(Some(score)))` otherwise.
/// `ring_score` is an integer `0..=100`. The legacy `call_mode` param is still
/// accepted (`bar`/`call_kit`/`auto`) and mapped to a score.
fn requested_ring_score(request: &Request) -> Result<Option<Option<u8>>, Response> {
    let invalid = |message: &str| {
        error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            message.into(),
            false,
        )
    };
    let param = |key: &str| request.params.get(key).filter(|value| !value.is_null());
    if let Some(value) = param("ring_score") {
        if value.as_str() == Some("auto") {
            return Ok(Some(None));
        }
        return match value.as_u64() {
            Some(score) if score <= u64::from(idfon_protocol::ring::MAX) => Ok(Some(Some(score as u8))),
            _ => Err(invalid("ring_score must be an integer 0..=100 or \"auto\"")),
        };
    }
    let Some(value) = param("call_mode") else {
        return Ok(None);
    };
    if value.as_str() == Some("auto") {
        return Ok(Some(None));
    }
    serde_json::from_value::<idfon_protocol::IncomingCallMode>(value.clone())
        .map(|mode| Some(Some(mode.ring_score())))
        .map_err(|_| invalid("call_mode must be \"auto\", \"bar\" or \"call_kit\""))
}

/// Parses an M3 contact ticket passed as a JSON object or a JSON string.
fn parse_contact_ticket(
    value: &serde_json::Value,
) -> Result<idfon_protocol::McpContactTicket, String> {
    match value {
        serde_json::Value::String(text) => {
            serde_json::from_str(text).map_err(|error| error.to_string())
        }
        other => serde_json::from_value(other.clone()).map_err(|error| error.to_string()),
    }
}

fn peer_add(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
) -> Response {
    // An M3 contact ticket carries the dial address, endpoint id, and an
    // optional cached `server/discover`; it fills any omitted fields.
    let device_ticket = request
        .params
        .get("contact_ticket")
        .filter(|value| !value.is_null())
        .and_then(|value| match value {
            serde_json::Value::String(text) => {
                serde_json::from_str::<idfon_protocol::ContactTicket>(text).ok()
            }
            other => serde_json::from_value::<idfon_protocol::ContactTicket>(other.clone()).ok(),
        });
    let contact = match request
        .params
        .get("mcp_ticket")
        .filter(|value| !value.is_null())
    {
        Some(value) => match parse_contact_ticket(value) {
            Ok(contact) => Some(contact),
            Err(message) => {
                return error_response(
                    request.id.clone(),
                    &request.method,
                    ErrorCode::InvalidRequest,
                    format!("invalid mcp ticket: {message}"),
                    false,
                )
            }
        },
        None => None,
    };
    let id = match request
        .params
        .get("id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
    {
        Some(id) => id,
        None => match (&device_ticket, &contact) {
            (Some(ticket), _) => ticket.account_id.clone(),
            (_, Some(contact)) => contact.peer.account_id.clone(),
            (_, None) => {
                return error_response(
                    request.id.clone(),
                    &request.method,
                    ErrorCode::InvalidRequest,
                    "id is required".into(),
                    false,
                )
            }
        },
    };
    let name = request
        .params
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            contact
                .as_ref()
                .and_then(|contact| contact.peer.name.clone())
        })
        // A ContactTicket's `label` is the name the sharer chose (vCard import
        // folds FN into it), so cards name themselves.
        .or_else(|| device_ticket.as_ref().and_then(|ticket| ticket.label.clone()))
        .unwrap_or_else(|| id.clone());
    let identity = request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let ring_score = match requested_ring_score(request) {
        Ok(score) => score.flatten(),
        Err(response) => return response,
    };
    // An introduction token from the add link (object or JSON text). One that
    // does not parse, or is not valid for this add, is ignored: the contact
    // is still added (docs/protocol.md "Introductions").
    let intro = request
        .params
        .get("intro")
        .filter(|value| !value.is_null())
        .and_then(|value| idfon_protocol::Introduction::from_value(value).ok());
    let mut state = store.lock().expect("store mutex poisoned");
    if state
        .peers
        .iter()
        .any(|peer| peer.identity == identity && peer.id != id && peer.name == name)
    {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "peer name already exists".into(),
            false,
        );
    }
    let mut peer = idfon_protocol::Peer {
        id: id.into(),
        identity,
        name,
        endpoint_id: request
            .params
            .get("endpoint_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                device_ticket
                    .as_ref()
                    .map(|ticket| ticket.endpoint_id.clone())
            })
            .or_else(|| {
                contact
                    .as_ref()
                    .map(|contact| contact.peer.endpoint_id.clone())
            }),
        endpoint_addr: request
            .params
            .get("endpoint_addr")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                device_ticket
                    .as_ref()
                    .map(|ticket| ticket.endpoint_addr.clone())
            })
            .or_else(|| contact.as_ref().map(|contact| contact.transport.clone())),
        devices: request
            .params
            .get("devices")
            .and_then(serde_json::Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(|value| serde_json::from_value(value.clone()).ok())
                    .collect()
            })
            .unwrap_or_default(),
        aliases: request
            .params
            .get("aliases")
            .and_then(serde_json::Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default(),
        ring_score,
        call_mode: None,
        pinned: false,
        instant_call: false,
        tags: request_tags(&request.params, "tags"),
        visibility: None,
        introduced_by: None,
        share: None,
    };
    // Re-adding keeps who introduced them; a card says their sharing
    // preference (absent = ok), otherwise the last known one stays.
    let existing = state
        .peers
        .iter()
        .find(|candidate| candidate.identity == peer.identity && candidate.id == peer.id)
        .cloned();
    peer.share = match &device_ticket {
        Some(ticket) => ticket.share.clone().filter(|share| share == idfon_protocol::intro::SHARE_ASK),
        None => existing.as_ref().and_then(|existing| existing.share.clone()),
    };
    peer.introduced_by = existing.and_then(|existing| existing.introduced_by);
    let me = introductions::own_peer_id(&state, &peer.identity).unwrap_or_default();
    if let Some(intro) = intro.filter(|intro| introductions::is_valid_for(intro, &peer.id, &me)) {
        peer.introduced_by = Some(idfon_protocol::IntroducedBy::from(&intro));
        introductions::set_pending(&mut state, &peer.identity.clone(), &peer.id.clone(), &intro);
    }
    // Contact metadata (§19 model deltas): Favorites pin (D7) and the
    // instant-call flag (§5.1), settable at add time like `call_mode`.
    if let Some(pinned) = request
        .params
        .get("pinned")
        .and_then(serde_json::Value::as_bool)
    {
        peer.pinned = pinned;
    }
    if let Some(instant_call) = request
        .params
        .get("instant_call")
        .and_then(serde_json::Value::as_bool)
    {
        peer.instant_call = instant_call;
    }
    ensure_account_alias(&mut peer);
    // Adding someone deliberately overrides an earlier block of their requests.
    state
        .blocked_senders
        .retain(|blocked| !(blocked.identity == peer.identity && blocked.peer_id == peer.id));
    state
        .peers
        .retain(|candidate| !(candidate.identity == peer.identity && candidate.id == peer.id));
    state.peers.push(peer.clone());
    record_state_event(
        &mut state,
        &peer.identity,
        "peer.created",
        serde_json::json!({"peer": peer}),
    );
    if let Some(discover) = contact.and_then(|contact| contact.discover) {
        state
            .mcp_discovery
            .retain(|record| !(record.identity == peer.identity && record.peer_id == peer.id));
        state
            .mcp_discovery
            .push(idfon_protocol::McpDiscoveryRecord {
                peer_id: peer.id.clone(),
                identity: peer.identity.clone(),
                discover,
            });
    }
    if let Some(remote) = state
        .identities
        .iter()
        .find(|candidate| candidate.endpoint_id.as_deref() == peer.endpoint_id.as_deref())
        .cloned()
    {
        if let Some(source) = state
            .identities
            .iter()
            .find(|candidate| candidate.id == peer.identity)
            .cloned()
        {
            let reciprocal = idfon_protocol::Peer {
                id: source.public_key.clone().unwrap_or_default(),
                identity: remote.id.clone(),
                name: source.name.clone(),
                endpoint_id: source.endpoint_id.clone(),
                endpoint_addr: transport
                    .and_then(|transport| transport.endpoint_ticket_for(&source.id))
                    .and_then(|bytes| String::from_utf8(bytes).ok()),
                devices: Vec::new(),
                aliases: Vec::new(),
                ring_score: None,
                call_mode: None,
                pinned: false,
                instant_call: false,
                tags: Vec::new(),
                visibility: None,
                introduced_by: None,
                share: None,
            };
            if !reciprocal.id.is_empty() {
                state.peers.retain(|candidate| {
                    !(candidate.identity == reciprocal.identity && candidate.id == reciprocal.id)
                });
                state.peers.push(reciprocal);
            }
            for (grant_identity, subject, capability) in [
                (
                    peer.identity.clone(),
                    remote.public_key.clone().unwrap_or_default(),
                    idfon_protocol::Capability::MessageSend,
                ),
                (
                    remote.id.clone(),
                    source.public_key.clone().unwrap_or_default(),
                    idfon_protocol::Capability::MessageReceive,
                ),
                (
                    peer.identity.clone(),
                    remote.public_key.clone().unwrap_or_default(),
                    idfon_protocol::Capability::LiveAudioSubscribe,
                ),
                (
                    remote.id.clone(),
                    source.public_key.clone().unwrap_or_default(),
                    idfon_protocol::Capability::LiveAudioSubscribe,
                ),
            ] {
                if !state.grants.iter().any(|grant| {
                    grant.identity == grant_identity
                        && grant.subject == subject
                        && grant.capability == capability
                        && grant.revoked_at.is_none()
                }) {
                    state.grants.push(idfon_protocol::CapabilityGrant {
                        capability,
                        identity: grant_identity,
                        subject,
                        conversation: None,
                        active_at: "0".into(),
                        expires_at: None,
                        revision: 1,
                        revoked_at: None,
                    });
                }
            }
        }
    }
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(request, serde_json::json!({"peer": peer}))
}

/// `tag.list` — every tag in use or with a rule: `{name, hide_presence,
/// hide_photo, count}` (count = contacts with it).
fn tag_list(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let state = store.lock().expect("store mutex poisoned");
    let mut names: Vec<String> = state.tag_rules.iter().map(|rule| rule.name.clone()).collect();
    for peer in &state.peers {
        for tag in &peer.tags {
            add_tag(&mut names, tag);
        }
    }
    let tags: Vec<serde_json::Value> = names
        .iter()
        .map(|name| {
            let rule = state.tag_rules.iter().find(|rule| rule.name.eq_ignore_ascii_case(name));
            let count = state
                .peers
                .iter()
                .filter(|peer| peer.tags.iter().any(|tag| tag.eq_ignore_ascii_case(name)))
                .count();
            serde_json::json!({
                "name": name,
                "hide_presence": rule.is_some_and(|rule| rule.hide_presence),
                "hide_photo": rule.is_some_and(|rule| rule.hide_photo),
                "count": count,
            })
        })
        .collect();
    success(request, serde_json::json!({"tags": tags}))
}

/// `tag.set { name, hide_presence?, hide_photo? }` — create a tag or change
/// what its contacts can't see.
fn tag_set(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(name) = request_text(&request.params, "name").map(|name| name.trim().to_owned()).filter(|name| !name.is_empty()) else {
        return error_response(request.id.clone(), &request.method, ErrorCode::InvalidRequest, "name is required".into(), false);
    };
    let mut state = store.lock().expect("store mutex poisoned");
    let index = match state.tag_rules.iter().position(|rule| rule.name.eq_ignore_ascii_case(&name)) {
        Some(index) => index,
        None => {
            state.tag_rules.push(idfon_protocol::TagRule { name: name.clone(), hide_presence: false, hide_photo: false });
            state.tag_rules.len() - 1
        }
    };
    if let Some(hide) = request.params.get("hide_presence").and_then(serde_json::Value::as_bool) {
        state.tag_rules[index].hide_presence = hide;
    }
    if let Some(hide) = request.params.get("hide_photo").and_then(serde_json::Value::as_bool) {
        state.tag_rules[index].hide_photo = hide;
    }
    let rule = state.tag_rules[index].clone();
    save_or_error(request, &mut state, serde_json::json!({"tag": rule}))
}

/// `tag.rename { name, to }` — on the rule and on every contact.
fn tag_rename(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let (Some(name), Some(to)) = (
        request_text(&request.params, "name"),
        request_text(&request.params, "to").map(|to| to.trim().to_owned()).filter(|to| !to.is_empty()),
    ) else {
        return error_response(request.id.clone(), &request.method, ErrorCode::InvalidRequest, "name and to are required".into(), false);
    };
    let mut state = store.lock().expect("store mutex poisoned");
    if !name.eq_ignore_ascii_case(&to) && state.tag_rules.iter().any(|rule| rule.name.eq_ignore_ascii_case(&to)) {
        return error_response(request.id.clone(), &request.method, ErrorCode::InvalidRequest, format!("a tag named {to} exists"), false);
    }
    for rule in &mut state.tag_rules {
        if rule.name.eq_ignore_ascii_case(&name) {
            rule.name = to.clone();
        }
    }
    for peer in &mut state.peers {
        for tag in &mut peer.tags {
            if tag.eq_ignore_ascii_case(&name) {
                *tag = to.clone();
            }
        }
    }
    save_or_error(request, &mut state, serde_json::json!({"renamed": true}))
}

/// `tag.delete { name }` — the rule, and the tag on every contact.
fn tag_delete(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(name) = request_text(&request.params, "name") else {
        return error_response(request.id.clone(), &request.method, ErrorCode::InvalidRequest, "name is required".into(), false);
    };
    let mut state = store.lock().expect("store mutex poisoned");
    state.tag_rules.retain(|rule| !rule.name.eq_ignore_ascii_case(&name));
    for peer in &mut state.peers {
        peer.tags.retain(|tag| !tag.eq_ignore_ascii_case(&name));
    }
    save_or_error(request, &mut state, serde_json::json!({"deleted": true}))
}

/// The defaults as the apps read them: every aspect resolved (shown unless
/// turned off).
fn visibility_view(visibility: &idfon_protocol::Visibility) -> serde_json::Value {
    serde_json::json!({
        "presence": visibility.presence.unwrap_or(true),
        "photo": visibility.photo.unwrap_or(true),
    })
}

/// `visibility.set { presence?, photo? }` — what contacts see of you unless
/// a tag or their own override says otherwise (D-4: "a toggle hides it").
fn visibility_set(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let mut state = store.lock().expect("store mutex poisoned");
    if let Some(value) = request.params.get("presence").and_then(serde_json::Value::as_bool) {
        state.visibility.presence = Some(value);
    }
    if let Some(value) = request.params.get("photo").and_then(serde_json::Value::as_bool) {
        state.visibility.photo = Some(value);
    }
    let view = visibility_view(&state.visibility);
    save_or_error(request, &mut state, serde_json::json!({"visibility": view}))
}

/// `visibility.audience { aspect }` — the contacts who may see `aspect`
/// ("presence" | "photo") of you: who the apps send presence or the photo
/// to. Blocked contacts are the apps' to leave out.
fn visibility_audience(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let aspect = request_text(&request.params, "aspect").unwrap_or_default();
    if aspect != "presence" && aspect != "photo" {
        return error_response(request.id.clone(), &request.method, ErrorCode::InvalidRequest, "aspect must be presence or photo".into(), false);
    }
    let state = store.lock().expect("store mutex poisoned");
    let peers: Vec<&str> = state
        .peers
        .iter()
        .filter(|peer| peer_sees(&state, peer, &aspect))
        .map(|peer| peer.id.as_str())
        .collect();
    success(request, serde_json::json!({"aspect": aspect, "peers": peers}))
}

/// Save the store and answer `result`, or answer the save error.
fn save_or_error(request: &Request, state: &mut Store, result: serde_json::Value) -> Response {
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(request.id.clone(), &request.method, ErrorCode::Internal, error.to_string(), true);
    }
    success(request, result)
}

fn peer_update(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(reference) = request
        .params
        .get("ref")
        .and_then(serde_json::Value::as_str)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "ref is required".into(),
            false,
        );
    };
    let identity = request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let mut state = store.lock().expect("store mutex poisoned");
    let Some(peer) = state.peers.iter_mut().find(|peer| {
        peer.identity == identity
            && (peer.id == reference
                || peer.name == reference
                || peer.aliases.iter().any(|alias| alias == reference))
    }) else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "peer not found".into(),
            false,
        );
    };
    // The contact's sharing preference, from their `IDFON-SHARE/1`.
    if let Some(value) = request_text(&request.params, "share") {
        match idfon_protocol::intro::parse_share(&value) {
            Ok(share) => peer.share = share,
            Err(message) => {
                return error_response(
                    request.id.clone(),
                    &request.method,
                    ErrorCode::InvalidRequest,
                    message,
                    false,
                )
            }
        }
    }
    if let Some(name) = request
        .params
        .get("name")
        .and_then(serde_json::Value::as_str)
    {
        if name.is_empty() {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                "name cannot be empty".into(),
                false,
            );
        }
        peer.name = name.into();
    }
    if let Some(endpoint_id) = request
        .params
        .get("endpoint_id")
        .and_then(serde_json::Value::as_str)
    {
        peer.endpoint_id = Some(endpoint_id.into());
    }
    if let Some(endpoint_addr) = request
        .params
        .get("endpoint_addr")
        .and_then(serde_json::Value::as_str)
    {
        if serde_json::from_str::<serde_json::Value>(endpoint_addr).is_err() {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                "endpoint_addr must be valid JSON".into(),
                false,
            );
        }
        peer.endpoint_addr = Some(endpoint_addr.into());
    }
    if let Some(pinned) = request
        .params
        .get("pinned")
        .and_then(serde_json::Value::as_bool)
    {
        peer.pinned = pinned;
    }
    if let Some(instant_call) = request
        .params
        .get("instant_call")
        .and_then(serde_json::Value::as_bool)
    {
        peer.instant_call = instant_call;
    }
    // Tags (docs/identity.md): `tags` replaces the set; `add_tag` /
    // `remove_tag` change one.
    if request.params.get("tags").is_some_and(serde_json::Value::is_array) {
        peer.tags = request_tags(&request.params, "tags");
    }
    if let Some(tag) = request_text(&request.params, "add_tag") {
        add_tag(&mut peer.tags, &tag);
    }
    if let Some(tag) = request_text(&request.params, "remove_tag") {
        peer.tags.retain(|existing| !existing.eq_ignore_ascii_case(&tag));
    }
    // `visibility`: this contact's overrides ({presence?, photo?}; a null
    // field clears that override, a null object clears them all).
    match request.params.get("visibility") {
        Some(serde_json::Value::Null) => peer.visibility = None,
        Some(serde_json::Value::Object(fields)) => {
            let mut visibility = peer.visibility.clone().unwrap_or_default();
            for (aspect, value) in fields {
                let choice = value.as_bool();
                match aspect.as_str() {
                    "presence" => visibility.presence = choice,
                    "photo" => visibility.photo = choice,
                    _ => {}
                }
            }
            peer.visibility = (visibility != idfon_protocol::Visibility::default()).then_some(visibility);
        }
        _ => {}
    }
    if let Some(devices) = request
        .params
        .get("devices")
        .and_then(serde_json::Value::as_array)
    {
        peer.devices = devices
            .iter()
            .filter_map(|value| serde_json::from_value(value.clone()).ok())
            .collect();
    }
    if let Some(aliases) = request
        .params
        .get("aliases")
        .and_then(serde_json::Value::as_array)
    {
        peer.aliases = aliases
            .iter()
            .filter_map(serde_json::Value::as_str)
            .map(str::to_owned)
            .collect();
    }
    let ring_score = match requested_ring_score(request) {
        Ok(score) => score,
        Err(response) => return response,
    };
    if let Some(ring_score) = ring_score {
        peer.ring_score = ring_score;
    }
    ensure_account_alias(peer);
    let updated = peer.clone();
    record_state_event(
        &mut state,
        &updated.identity,
        "peer.updated",
        serde_json::json!({"peer": updated}),
    );
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(request, serde_json::json!({"peer": updated}))
}

fn peer_device_from_params(request: &Request) -> Result<idfon_protocol::PeerDevice, String> {
    let endpoint_id = request
        .params
        .get("endpoint_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "endpoint_id is required".to_string())?
        .to_owned();
    let endpoint_addr = request
        .params
        .get("endpoint_addr")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    if let Some(address) = endpoint_addr.as_deref() {
        serde_json::from_str::<serde_json::Value>(address)
            .map_err(|_| "endpoint_addr must be valid JSON".to_string())?;
    }
    let label = request
        .params
        .get("label")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let device_class = request
        .params
        .get("device_class")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let capabilities = request
        .params
        .get("capabilities")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    Ok(idfon_protocol::PeerDevice {
        endpoint_id,
        endpoint_addr,
        label,
        device_class,
        capabilities,
    })
}

fn peer_device_add(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(reference) = request_text(&request.params, "ref") else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "ref is required".into(),
            false,
        );
    };
    let identity = request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let device = match peer_device_from_params(request) {
        Ok(device) => device,
        Err(message) => {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                message,
                false,
            )
        }
    };
    let mut state = store.lock().expect("store mutex poisoned");
    let Some(peer) = state.peers.iter_mut().find(|peer| {
        peer.identity == identity
            && (peer.id == reference
                || peer.name == reference
                || peer.aliases.iter().any(|alias| alias == &reference))
    }) else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "peer not found".into(),
            false,
        );
    };
    if peer.knows_endpoint(&device.endpoint_id) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "endpoint already belongs to peer".into(),
            false,
        );
    }
    peer.devices.push(device);
    let updated = peer.clone();
    record_state_event(
        &mut state,
        &updated.identity,
        "peer.device_added",
        serde_json::json!({"peer": updated}),
    );
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(request, serde_json::json!({"peer": updated}))
}

fn peer_device_update(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(reference) = request_text(&request.params, "ref") else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "ref is required".into(),
            false,
        );
    };
    let Some(endpoint_id) = request_text(&request.params, "endpoint_id") else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "endpoint_id is required".into(),
            false,
        );
    };
    let identity = request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let replacement = match peer_device_from_params(request) {
        Ok(device) => device,
        Err(message) => {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                message,
                false,
            )
        }
    };
    let mut state = store.lock().expect("store mutex poisoned");
    let Some(peer) = state.peers.iter_mut().find(|peer| {
        peer.identity == identity
            && (peer.id == reference
                || peer.name == reference
                || peer.aliases.iter().any(|alias| alias == &reference))
    }) else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "peer not found".into(),
            false,
        );
    };
    let Some(device) = peer
        .devices
        .iter_mut()
        .find(|device| device.endpoint_id == endpoint_id)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "device not found".into(),
            false,
        );
    };
    *device = replacement;
    let updated = peer.clone();
    record_state_event(
        &mut state,
        &updated.identity,
        "peer.device_updated",
        serde_json::json!({"peer": updated}),
    );
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(request, serde_json::json!({"peer": updated}))
}

fn peer_device_remove(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(reference) = request_text(&request.params, "ref") else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "ref is required".into(),
            false,
        );
    };
    let Some(endpoint_id) = request_text(&request.params, "endpoint_id") else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "endpoint_id is required".into(),
            false,
        );
    };
    let identity = request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let mut state = store.lock().expect("store mutex poisoned");
    let Some(peer) = state.peers.iter_mut().find(|peer| {
        peer.identity == identity
            && (peer.id == reference
                || peer.name == reference
                || peer.aliases.iter().any(|alias| alias == &reference))
    }) else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "peer not found".into(),
            false,
        );
    };
    let before = peer.devices.len();
    peer.devices
        .retain(|device| device.endpoint_id != endpoint_id);
    if peer.devices.len() == before {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "device not found".into(),
            false,
        );
    }
    let updated = peer.clone();
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(request, serde_json::json!({"peer": updated}))
}

fn peer_remove(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(reference) = request
        .params
        .get("ref")
        .and_then(serde_json::Value::as_str)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "ref is required".into(),
            false,
        );
    };
    let identity = request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let mut state = store.lock().expect("store mutex poisoned");
    let before = state.peers.len();
    let removed = state
        .peers
        .iter()
        .find(|peer| {
            peer.identity == identity
                && (peer.id == reference
                    || peer.name == reference
                    || peer.aliases.iter().any(|alias| alias == reference))
        })
        .cloned();
    state.peers.retain(|peer| {
        !(peer.identity == identity
            && (peer.id == reference
                || peer.name == reference
                || peer.aliases.iter().any(|alias| alias == reference)))
    });
    if state.peers.len() == before {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "peer not found".into(),
            false,
        );
    }
    if let Some(peer) = removed {
        // A removed contact's introduction is not attached to a later re-add.
        introductions::clear_pending(&mut state, &identity, &peer.id);
        record_state_event(
            &mut state,
            &identity,
            "peer.removed",
            serde_json::json!({"peer": peer}),
        );
    }
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(request, serde_json::json!({"removed": reference}))
}

fn operation_get(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let id = request
        .params
        .get("operation_id")
        .and_then(serde_json::Value::as_str);
    let state = store.lock().expect("store mutex poisoned");
    match id.and_then(|id| {
        state
            .operations
            .iter()
            .find(|operation| operation.operation_id == id)
    }) {
        Some(operation) => success(request, serde_json::json!({"operation": operation})),
        None => error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "operation not found".into(),
            false,
        ),
    }
}

fn wait_event(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let mut response = events(request, store);
    if let ResponseBody::Success { ref mut result, .. } = response.body {
        if let Some(events) = result
            .get_mut("events")
            .and_then(serde_json::Value::as_array_mut)
        {
            events.truncate(1);
        }
    }
    response
}

fn compact_identities(store: &Arc<Mutex<Store>>) -> Vec<u8> {
    let state = store.lock().expect("store mutex poisoned");
    let mut payload = Vec::new();
    payload.extend_from_slice(&(state.identities.len() as u16).to_be_bytes());
    for identity in &state.identities {
        let name = identity.name.as_bytes();
        if name.len() > u16::MAX as usize {
            return Vec::new();
        }
        payload.extend_from_slice(&(name.len() as u16).to_be_bytes());
        payload.extend_from_slice(name);
        payload.push(if identity.active { 1 } else { 0 });
    }
    payload
}

fn compact_peers(store: &Arc<Mutex<Store>>, identity: Option<&str>) -> Vec<u8> {
    let state = store.lock().expect("store mutex poisoned");
    let identity_id = identity.and_then(|value| {
        state
            .identities
            .iter()
            .find(|item| item.id == value || item.name == value)
            .map(|item| item.id.as_str())
    });
    let peers: Vec<_> = state
        .peers
        .iter()
        .filter(|peer| identity_id.is_none_or(|value| peer.identity == value))
        .collect();
    let mut payload = Vec::new();
    payload.extend_from_slice(&(peers.len() as u16).to_be_bytes());
    for peer in peers {
        let name = peer.name.as_bytes();
        let endpoint = peer.endpoint_id.as_deref().unwrap_or("").as_bytes();
        if name.len() > u16::MAX as usize || endpoint.len() > u16::MAX as usize {
            continue;
        }
        payload.extend_from_slice(&(name.len() as u16).to_be_bytes());
        payload.extend_from_slice(name);
        payload.extend_from_slice(&(endpoint.len() as u16).to_be_bytes());
        payload.extend_from_slice(endpoint);
    }
    payload
}

fn compact_events(
    store: &Arc<Mutex<Store>>,
    after: Option<&str>,
    identity: Option<&str>,
) -> Vec<u8> {
    let state = store.lock().expect("store mutex poisoned");
    let identity_id = identity.and_then(|value| {
        state
            .identities
            .iter()
            .find(|item| item.id == value || item.name == value)
            .map(|item| item.id.as_str())
    });
    let events: Vec<_> = state
        .events
        .iter()
        .filter(|event| identity_id.is_none_or(|value| event.identity == value))
        .filter(|event| after.is_none_or(|cursor| event.cursor.as_str() > cursor))
        .collect();
    let mut payload = Vec::new();
    payload.extend_from_slice(&[0, 0]); // event count, patched below
    let mut count: usize = 0;
    for event in events {
        let fields = [
            event.cursor.as_bytes(),
            event.r#type.as_bytes(),
            event
                .data
                .get("peer_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .as_bytes(),
            event
                .data
                .get("message_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .as_bytes(),
            event
                .data
                .get("text")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .as_bytes(),
            // 6th field (epoch seconds): lets the GUI surface only fresh
            // invites from the initial drain instead of replaying calls from
            // past sessions (app relaunches reset the GUI cursor).
            event.timestamp.as_bytes(),
        ];
        if fields.iter().any(|value| value.len() > u16::MAX as usize) {
            // ponytail: skipped rather than wedging the stream; pathological
            // input only — the log makes it observable if it ever fires.
            tracing::warn!(
                target: "idfon.daemon",
                event_id = %event.event_id,
                identity = %event.identity,
                "events.compact skipping oversized event"
            );
            continue;
        }
        let needed: usize = fields.iter().map(|value| value.len() + 2).sum();
        // Always include at least one event so a page boundary can't loop forever.
        if count > 0 && payload.len() + needed > COMPACT_EVENTS_MAX_BYTES {
            break;
        }
        for value in fields {
            payload.extend_from_slice(&(value.len() as u16).to_be_bytes());
            payload.extend_from_slice(value);
        }
        count += 1;
    }
    payload[0..2].copy_from_slice(&(count as u16).to_be_bytes());
    payload
}

fn events_sync(
    request: &Request,
    store: &Arc<Mutex<Store>>,
    transport: Option<&Arc<TransportMode>>,
) -> Response {
    let Some(transport) = transport else {
        return transport_not_ready(request);
    };
    let identity_ref =
        request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let target = request_text(&request.params, "to");
    let Some(target) = target else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "to is required".into(),
            false,
        );
    };
    let (identity, peer, envelope) = {
        let state = store.lock().expect("store mutex poisoned");
        let identity = identity_id_of(&state, &identity_ref);
        let Some(identity_data) = state.identities.iter().find(|item| item.id == identity) else {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                "identity not found".into(),
                false,
            );
        };
        let Some(peer) = state
            .peers
            .iter()
            .find(|peer| {
                peer.identity == identity
                    && (peer.id == target || peer.name == target || peer.knows_endpoint(&target))
            })
            .cloned()
        else {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                "peer not found".into(),
                false,
            );
        };
        let Some(account_id) = identity_data.public_key.clone() else {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                "identity has no account key".into(),
                false,
            );
        };
        let key = match state.identity_key(&identity) {
            Ok(key) => key,
            Err(error) => {
                return error_response(
                    request.id.clone(),
                    &request.method,
                    ErrorCode::Internal,
                    error.to_string(),
                    false,
                )
            }
        };
        let events = state.events.clone();
        let batch_id = format!("{}-{}", identity, events.len());
        (
            identity,
            peer,
            idfon_core::sign_state_sync(&key, account_id, batch_id, events),
        )
    };
    let Some(address) = peer
        .dial_targets()
        .into_iter()
        .next()
        .map(|(_, address)| address)
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "peer has no endpoint address".into(),
            false,
        );
    };
    let Some(endpoint) = transport.current_transport(&identity) else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "no transport for identity".into(),
            false,
        );
    };
    let result = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let runtime = tokio::runtime::Runtime::new().map_err(io::Error::other)?;
                runtime.block_on(async {
                    let target = serde_json::from_str(&address).map_err(io::Error::other)?;
                    let (_connection, mut send, mut recv) = endpoint
                        .open_bi_stream(&target, SYNC_ALPN)
                        .await
                        .map_err(io::Error::other)?;
                    let bytes = serde_json::to_vec(&envelope).map_err(io::Error::other)?;
                    send.write_all(&bytes).await.map_err(io::Error::other)?;
                    send.finish().map_err(io::Error::other)?;
                    recv.read_to_end(1024 * 1024)
                        .await
                        .map_err(io::Error::other)
                })
            })
            .join()
            .map_err(|_| io::Error::other("sync worker panicked"))
    })
    .and_then(|value| value);
    match result {
        Ok(bytes) => success(
            request,
            serde_json::json!({"synced": true, "response": serde_json::from_slice::<serde_json::Value>(&bytes).unwrap_or_default()}),
        ),
        Err(error) => error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        ),
    }
}

fn events_export(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let identity_ref =
        request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let after = request_text(&request.params, "after");
    let state = store.lock().expect("store mutex poisoned");
    let identity_id = identity_id_of(&state, &identity_ref);
    let Some(account_id) = state
        .identities
        .iter()
        .find(|item| item.id == identity_id)
        .and_then(|item| item.public_key.clone())
    else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "identity has no account key".into(),
            false,
        );
    };
    let events = state
        .events
        .iter()
        .filter(|event| {
            after
                .as_deref()
                .is_none_or(|cursor| event.cursor.as_str() > cursor)
        })
        .cloned()
        .collect::<Vec<_>>();
    let key = match state.identity_key(&identity_id) {
        Ok(key) => key,
        Err(error) => {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::Internal,
                error.to_string(),
                false,
            )
        }
    };
    let envelope = idfon_core::sign_state_sync(
        &key,
        account_id,
        format!("{identity_id}-{}", state.events.len()),
        events,
    );
    success(
        request,
        serde_json::to_value(envelope).expect("sync envelope serializes"),
    )
}

fn events_merge(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(value) = request.params.get("envelope") else {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::InvalidRequest,
            "envelope is required".into(),
            false,
        );
    };
    let envelope = match serde_json::from_value::<idfon_protocol::StateSyncEnvelope>(value.clone())
    {
        Ok(envelope) => envelope,
        Err(error) => {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::InvalidRequest,
                format!("invalid sync envelope: {error}"),
                false,
            )
        }
    };
    if let Err(error) = idfon_core::verify_state_sync(&envelope) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Unauthorized,
            error.to_string(),
            false,
        );
    }
    let incoming = envelope.events;
    let mut state = store.lock().expect("store mutex poisoned");
    let mut added = 0usize;
    for event in incoming {
        if state
            .events
            .iter()
            .any(|existing| existing.event_id == event.event_id)
        {
            continue;
        }
        if let Err(response) = apply_state_event(&mut state, &event) {
            return response;
        }
        state.events.push(event);
        added += 1;
    }
    state.events.sort_by(|a, b| a.event_id.cmp(&b.event_id));
    let data_dir = state.data_dir.clone();
    if let Err(error) = state.save(&data_dir) {
        return error_response(
            request.id.clone(),
            &request.method,
            ErrorCode::Internal,
            error.to_string(),
            true,
        );
    }
    success(
        request,
        serde_json::json!({"added": added, "total": state.events.len()}),
    )
}

fn apply_state_event(state: &mut Store, event: &idfon_protocol::Event) -> Result<(), Response> {
    let payload = &event.data;
    match event.r#type.as_str() {
        "identity.created" => {
            let identity: Identity =
                serde_json::from_value(payload["identity"].clone()).map_err(|_| {
                    error_response(
                        "merge".into(),
                        "events.merge",
                        ErrorCode::InvalidRequest,
                        "invalid identity event".into(),
                        false,
                    )
                })?;
            if !state.identities.iter().any(|item| item.id == identity.id) {
                state.identities.push(identity);
            }
        }
        "peer.created"
        | "peer.updated"
        | "peer.device_added"
        | "peer.device_updated"
        | "peer.device_removed" => {
            let peer: idfon_protocol::Peer = serde_json::from_value(payload["peer"].clone())
                .map_err(|_| {
                    error_response(
                        "merge".into(),
                        "events.merge",
                        ErrorCode::InvalidRequest,
                        "invalid peer event".into(),
                        false,
                    )
                })?;
            state
                .peers
                .retain(|item| !(item.identity == peer.identity && item.id == peer.id));
            state.peers.push(peer);
        }
        "peer.removed" => {
            let peer: idfon_protocol::Peer = serde_json::from_value(payload["peer"].clone())
                .map_err(|_| {
                    error_response(
                        "merge".into(),
                        "events.merge",
                        ErrorCode::InvalidRequest,
                        "invalid peer event".into(),
                        false,
                    )
                })?;
            state
                .peers
                .retain(|item| !(item.identity == peer.identity && item.id == peer.id));
        }
        "grant.created" => {
            let grant: idfon_protocol::CapabilityGrant =
                serde_json::from_value(payload["grant"].clone()).map_err(|_| {
                    error_response(
                        "merge".into(),
                        "events.merge",
                        ErrorCode::InvalidRequest,
                        "invalid grant event".into(),
                        false,
                    )
                })?;
            if !state.grants.iter().any(|item| {
                item.identity == grant.identity
                    && item.subject == grant.subject
                    && item.capability == grant.capability
                    && item.conversation == grant.conversation
            }) {
                state.grants.push(grant);
            }
        }
        "room.created" => {
            let room: idfon_protocol::Room = serde_json::from_value(payload["room"].clone())
                .map_err(|_| {
                    error_response(
                        "merge".into(),
                        "events.merge",
                        ErrorCode::InvalidRequest,
                        "invalid room event".into(),
                        false,
                    )
                })?;
            if !state
                .rooms
                .iter()
                .any(|item| item.identity == room.identity && item.id == room.id)
            {
                state.rooms.push(room);
            }
        }
        _ => {
            return Err(error_response(
                "merge".into(),
                "events.merge",
                ErrorCode::InvalidRequest,
                format!("unsupported state event: {}", event.r#type),
                false,
            ))
        }
    }
    Ok(())
}

fn events(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let after = request
        .params
        .get("after")
        .and_then(serde_json::Value::as_str);
    let kind = request
        .params
        .get("type")
        .and_then(serde_json::Value::as_str);
    let peer = request
        .params
        .get("peer")
        .and_then(serde_json::Value::as_str);
    let state = store.lock().expect("store mutex poisoned");
    if let (Some(after), Some(oldest)) = (after, state.events.first()) {
        if !after.is_empty() && after < oldest.cursor.as_str() {
            return error_response(
                request.id.clone(),
                &request.method,
                ErrorCode::CursorTooOld,
                "event cursor is older than retained history".into(),
                false,
            );
        }
    }
    let identity = request_text(&request.params, "identity");
    let mut events: Vec<_> = state
        .events
        .iter()
        .rev()
        .take(EVENT_RETENTION)
        .filter(|event| {
            identity
                .as_deref()
                .is_none_or(|value| event.identity == value)
        })
        .filter(|event| {
            after.is_none_or(|cursor| event.cursor.as_str() > cursor)
                && kind.is_none_or(|value| event.r#type == value)
                && peer.is_none_or(|value| {
                    event
                        .data
                        .get("peer_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(value)
                })
        })
        .cloned()
        .collect();
    // Cursor order (oldest first): `wait` truncates to one event and the
    // shells advance their cursor per delivered event, so a newest-first page
    // strands every older event in the same burst (a hangup stop or an invite
    // lost behind a newer message). `events --follow` also advances by
    // `events.last()`, which is only correct in this order.
    events.reverse();
    success(request, serde_json::json!({"events": events}))
}

fn now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

fn next_event_number(events: &[idfon_protocol::Event]) -> u64 {
    events
        .last()
        .and_then(|event| event.cursor.strip_prefix("cur_"))
        .and_then(|number| number.parse::<u64>().ok())
        .unwrap_or(0)
        .saturating_add(1)
}

fn record_state_event(
    state: &mut Store,
    identity: &str,
    event_type: &str,
    data: serde_json::Value,
) {
    let number = next_event_number(&state.events);
    state.events.push(idfon_protocol::Event {
        event_id: format!("state-{event_type}-{number}"),
        cursor: format!("cur_{number:020}"),
        r#type: event_type.into(),
        timestamp: now(),
        identity: identity.into(),
        data,
    });
    if state.events.len() > EVENT_RETENTION {
        let excess = state.events.len() - EVENT_RETENTION;
        state.events.drain(..excess);
    }
}

fn success(request: &Request, result: serde_json::Value) -> Response {
    Response {
        version: PROTOCOL_VERSION,
        id: request.id.clone(),
        ok: true,
        body: ResponseBody::Success {
            operation: request.method.clone(),
            result,
        },
    }
}

fn error_response(
    id: String,
    operation: &str,
    code: ErrorCode,
    message: String,
    retryable: bool,
) -> Response {
    Response {
        version: PROTOCOL_VERSION,
        id,
        ok: false,
        body: ResponseBody::Failure {
            operation: operation.into(),
            error: ApiError {
                code,
                message,
                retryable,
                retry_after_ms: None,
                next: Vec::new(),
            },
        },
    }
}

async fn read_frame(stream: &mut UnixStream) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0; 4];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let length = u32::from_be_bytes(header) as usize;
    if length > idfon_protocol::MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut frame = vec![0; length];
    stream.read_exact(&mut frame).await?;
    Ok(Some(frame))
}

async fn write_frame(stream: &mut UnixStream, payload: &[u8]) -> io::Result<()> {
    if payload.len() > idfon_protocol::MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(payload).await
}

fn prepare_socket(socket: &Path) -> io::Result<()> {
    if !socket.exists() {
        return Ok(());
    }
    match std::os::unix::net::UnixStream::connect(socket) {
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "another idfond is already using this socket",
        )),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
            ) =>
        {
            // Stale socket: remove it (remove_file works on unix sockets too;
            // Path::is_file() is false for sockets, don't use it here).
            std::fs::remove_file(socket)
        }
        Err(error) => Err(error),
    }
}

/// The store's single-host lock: an exclusive advisory file lock that dies
/// with the process. The previous pid-file scheme needed stale-recovery
/// heuristics (a `kill(pid, 0)` probe) that mis-fired on pid reuse and had a
/// create/write crash window; a kernel lock has no stale state at all. The
/// file is never deleted: unlink + recreate would let two hosts lock
/// different inodes.
struct DataLock(std::fs::File);

impl DataLock {
    fn acquire(data_dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join("state.lock");
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    format!("data directory is already locked: {}", path.display()),
                ));
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(error),
        }
        use std::io::Write;
        let _ = writeln!(&file, "{}", std::process::id()); // holder pid, for `ps`
        Ok(Self(file))
    }
}

impl Drop for DataLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

struct SocketCleanup(PathBuf);

impl Drop for SocketCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn path_scope_matches_whole_segments() {
        assert!(idfon_core::path_in_scope("/fs/public", "/fs/public"));
        assert!(idfon_core::path_in_scope("/fs/public/a.txt", "/fs/public"));
        assert!(idfon_core::path_in_scope("/fs/public/a.txt", "/fs/public/"));
        assert!(!idfon_core::path_in_scope("/fs/publicx/secret", "/fs/public"));
        assert!(!idfon_core::path_in_scope("/fs/publi", "/fs/public"));
        assert!(!idfon_core::path_in_scope("/fs/other", "/fs/public"));
        assert!(idfon_core::path_in_scope("/fs/anything", "/"));
    }
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Local reads run with no transport at all: display data (contacts,
    /// identities, rooms) is available the moment the store opens, never
    /// gated on endpoints binding.
    #[test]
    fn store_reads_serve_without_a_transport() {
        let dir = temp_dir("reads-without-transport");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let response = dispatch_with_transport(
            Request {
                version: PROTOCOL_VERSION,
                id: "peers".into(),
                method: "peers".into(),
                params: serde_json::json!({}),
            },
            &store,
            None,
        );
        assert!(response.ok);
    }

    /// A send before the transport binds fails closed, retryably, and records
    /// nothing: a queued operation would end up marked Failed for good
    /// instead of waiting for the bind to finish.
    #[test]
    fn delivery_policy_walks_the_device_list() {
        use idfon_protocol::{AckStatus, DeliveryMode, DeliveryPolicy, MessageAck};
        let targets = || {
            vec![
                ("dev-a".to_owned(), "addr-a".to_owned()),
                ("dev-b".to_owned(), "addr-b".to_owned()),
            ]
        };
        let ok = |id: &str| {
            Ok(MessageAck {
                message_id: id.to_owned(),
                status: AckStatus::Accepted,
            })
        };

        // Failover (the call path): a dead first device must not stop the
        // second from ringing (C17).
        let policy = DeliveryPolicy {
            mode: DeliveryMode::Failover,
            ..Default::default()
        };
        let mut tried = Vec::new();
        let result = deliver(Some(&policy), targets(), |device, _addr| {
            tried.push(device.to_owned());
            if device == "dev-a" {
                Err(io::Error::other("connect timed out"))
            } else {
                ok("m1")
            }
        });
        assert!(result.is_ok(), "failover must try the next device");
        assert_eq!(tried, ["dev-a", "dev-b"]);

        // One: the first target only.
        let policy = DeliveryPolicy {
            mode: DeliveryMode::One,
            ..Default::default()
        };
        let mut tried = Vec::new();
        let result = deliver(Some(&policy), targets(), |device, _addr| {
            tried.push(device.to_owned());
            Err(io::Error::other("connect timed out"))
        });
        assert!(result.is_err());
        assert_eq!(tried, ["dev-a"]);

        // All: every device; any ack wins.
        let policy = DeliveryPolicy {
            mode: DeliveryMode::All,
            ..Default::default()
        };
        let mut tried = Vec::new();
        let result = deliver(Some(&policy), targets(), |device, _addr| {
            tried.push(device.to_owned());
            if device == "dev-a" {
                Err(io::Error::other("connect timed out"))
            } else {
                ok("m1")
            }
        });
        assert!(result.is_ok());
        assert_eq!(tried, ["dev-a", "dev-b"]);

        // Failover with every device dead: fail.
        let policy = DeliveryPolicy {
            mode: DeliveryMode::Failover,
            ..Default::default()
        };
        assert!(deliver(Some(&policy), targets(), |_, _| Err(io::Error::other("down"))).is_err());
        // No policy keeps the classic single-target default.
        let mut tried = Vec::new();
        assert!(deliver(None, targets(), |device, _addr| {
            tried.push(device.to_owned());
            ok("m1")
        })
        .is_ok());
        assert_eq!(tried, ["dev-a"]);
    }

    #[test]
    fn sends_fail_closed_before_the_transport_binds() {
        let dir = temp_dir("send-without-transport");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let response = dispatch_with_transport(
            Request {
                version: PROTOCOL_VERSION,
                id: "send".into(),
                method: "message.send".into(),
                params: serde_json::json!({"to": "nobody", "text": "hi", "idempotency_key": "k1"}),
            },
            &store,
            None,
        );
        assert!(!response.ok);
        assert!(matches!(
            &response.body,
            ResponseBody::Failure { error, .. } if error.retryable
        ));
        assert!(store.lock().unwrap().operations.is_empty());
    }

    /// The shared feature pipeline serves reads with no transport installed:
    /// the embedded packaging paints local data before the endpoints finish
    /// binding, through exactly the code the socket packaging runs.
    #[test]
    fn handle_request_serves_reads_before_the_transport_binds() {
        let dir = temp_dir("handle-request");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let frame = runtime
            .block_on(handle_request(
                Request {
                    version: PROTOCOL_VERSION,
                    id: "t".into(),
                    method: "peers".into(),
                    params: serde_json::json!({}),
                },
                &store,
                None,
                &mut String::from("default"),
            ))
            .unwrap();
        let response: Response = serde_json::from_slice(&frame).unwrap();
        assert!(response.ok);
    }

    /// Canonical names, legacy aliases, the `peer` param and a `null`
    /// identity all reach the same handlers.
    #[test]
    fn canonical_and_legacy_method_names_are_equivalent() {
        use idfon_protocol::rpc;
        let dir = temp_dir("canonical-names");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let call = |request: Request| -> Response {
            let frame = runtime
                .block_on(handle_request(
                    request,
                    &store,
                    None,
                    &mut String::from("default"),
                ))
                .unwrap();
            serde_json::from_slice(&frame).unwrap()
        };
        let add = call(Request::call(
            "a",
            &rpc::PeerAdd {
                peer: Some("bob".into()),
                id: Some("bob".into()),
                name: Some("Bob".into()),
                endpoint_id: Some("ep-bob".into()),
                ..Default::default()
            },
        ));
        assert!(add.ok, "{add:?}");
        for method in ["peers", "peer.list"] {
            let response = call(Request {
                version: PROTOCOL_VERSION,
                id: "l".into(),
                method: method.into(),
                params: serde_json::json!({"identity": null}),
            });
            let ResponseBody::Success { operation, result } = &response.body else {
                panic!("{method}: {response:?}");
            };
            assert_eq!(operation, "peer.list");
            assert_eq!(result["peers"][0]["name"], "Bob", "{method}");
        }
        let show = call(Request {
            version: PROTOCOL_VERSION,
            id: "s".into(),
            method: "peer.show".into(),
            params: serde_json::json!({"peer": "Bob"}),
        });
        assert!(show.ok, "{show:?}");
    }

    fn test_peer(id: &str, identity: &str, name: &str) -> idfon_protocol::Peer {
        idfon_protocol::Peer {
            id: id.into(),
            identity: identity.into(),
            name: name.into(),
            endpoint_id: None,
            endpoint_addr: None,
            devices: Vec::new(),
            aliases: vec![],
            ring_score: None,
            call_mode: None,
            pinned: false,
            instant_call: false,
            tags: Vec::new(),
            visibility: None,
            introduced_by: None,
            share: None,
        }
    }

    #[test]
    fn a_fresh_store_has_one_identity() {
        let dir = temp_dir("one-identity");
        let store = Store::load(&dir).unwrap();
        assert_eq!(store.identities.len(), 1, "no Work/Personal/Private presets");
        assert_eq!(store.identities[0].id, "default");
        assert!(store.identities[0].active);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn old_identities_collapse_into_tags() {
        let dir = temp_dir("collapse");
        let mut store = Store::load(&dir).unwrap();
        // An old store: the D4 presets, each with contacts.
        store.single_identity_v1 = false;
        store.identities[0].name = "Personal".into();
        for name in ["Work", "Private"] {
            store.identities.push(Identity {
                id: name.into(),
                name: name.into(),
                endpoint_id: None,
                public_key: None,
                active: false,
                share: None,
            });
        }
        store.peers.push(test_peer("alice", "default", "Alice"));
        store.peers.push(test_peer("alice", "Work", "Alice")); // the same contact, twice
        store.peers.push(test_peer("bob", "Work", "Bob"));
        store.peers.push(test_peer("bob2", "Private", "Bob")); // a name clash
        store.grants.push(idfon_protocol::CapabilityGrant {
            capability: idfon_protocol::Capability::MessageSend,
            identity: "Work".into(),
            subject: "bob".into(),
            conversation: None,
            active_at: "0".into(),
            expires_at: None,
            revision: 0,
            revoked_at: None,
        });
        store.save(&dir).unwrap();

        let store = Store::load(&dir).unwrap();
        assert_eq!(store.identities.len(), 1);
        assert!(store.peers.iter().all(|peer| peer.identity == "default"));
        let alice: Vec<_> = store.peers.iter().filter(|peer| peer.id == "alice").collect();
        assert_eq!(alice.len(), 1, "merged");
        assert_eq!(alice[0].tags, vec!["Work".to_string()]);
        let bob = store.peers.iter().find(|peer| peer.id == "bob").unwrap();
        assert_eq!(bob.tags, vec!["Work".to_string()]);
        let bob2 = store.peers.iter().find(|peer| peer.id == "bob2").unwrap();
        assert_eq!(bob2.name, "Bob (Private)");
        assert_eq!(bob2.tags, vec!["Private".to_string()]);
        assert!(store.grants.iter().all(|grant| grant.identity == "default"));
        assert!(store.tag_rules.iter().any(|rule| rule.name == "Work"));
        assert!(store.tag_rules.iter().any(|rule| rule.name == "Private"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn creating_or_deleting_an_identity_is_refused() {
        let dir = temp_dir("identity-refused");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let call = |method: &str| {
            dispatch(
                Request {
                    version: PROTOCOL_VERSION,
                    id: "c".into(),
                    method: method.into(),
                    params: serde_json::json!({"name": "travel"}),
                },
                &store,
            )
        };
        assert!(!call("identity.create").ok);
        assert!(!call("identity.delete").ok);
        assert_eq!(store.lock().unwrap().identities.len(), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn tags_decide_what_contacts_see() {
        let dir = temp_dir("tags");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        {
            let mut state = store.lock().unwrap();
            for (id, name) in [("a", "Ann"), ("b", "Ben"), ("c", "Cy")] {
                state.peers.push(test_peer(id, "default", name));
            }
        }
        let call = |method: &str, params: serde_json::Value| {
            dispatch(
                Request { version: PROTOCOL_VERSION, id: "t".into(), method: method.into(), params },
                &store,
            )
        };
        let result = |response: Response| match response.body {
            ResponseBody::Success { result, .. } => result,
            ResponseBody::Failure { error, .. } => panic!("{error:?}"),
        };
        let audience = |aspect: &str| -> Vec<String> {
            let value = result(call("visibility.audience", serde_json::json!({"aspect": aspect})));
            serde_json::from_value(value["peers"].clone()).unwrap()
        };
        assert!(call("peer.update", serde_json::json!({"ref": "a", "add_tag": "Work"})).ok);
        assert!(call("peer.update", serde_json::json!({"ref": "b", "tags": ["work", "Family"]})).ok);
        // Work hides presence: Ann and Ben (any tag hiding it) can't see it.
        assert!(call("tag.set", serde_json::json!({"name": "Work", "hide_presence": true})).ok);
        assert_eq!(audience("presence"), vec!["c"]);
        assert_eq!(audience("photo"), vec!["a", "b", "c"]);
        // A contact's own override wins over its tags.
        assert!(call("peer.update", serde_json::json!({"ref": "b", "visibility": {"presence": true}})).ok);
        assert_eq!(audience("presence"), vec!["b", "c"]);
        // The default (D-4 "a toggle hides it") hides from everyone else.
        assert!(call("visibility.set", serde_json::json!({"presence": false})).ok);
        assert_eq!(audience("presence"), vec!["b"]);
        let tags = result(call("tag.list", serde_json::json!({})));
        let work = tags["tags"].as_array().unwrap().iter().find(|tag| tag["name"] == "Work").unwrap();
        assert_eq!(work["count"], 2);
        assert_eq!(work["hide_presence"], true);
        assert!(call("tag.rename", serde_json::json!({"name": "Work", "to": "Office"})).ok);
        assert!(store.lock().unwrap().peers.iter().find(|peer| peer.id == "a").unwrap().tags == vec!["Office".to_string()]);
        assert!(call("tag.delete", serde_json::json!({"name": "Office"})).ok);
        assert!(store.lock().unwrap().peers.iter().all(|peer| !peer.tags.iter().any(|tag| tag == "Office")));
        assert!(call("peer.update", serde_json::json!({"ref": "b", "remove_tag": "family", "visibility": null})).ok);
        let state = store.lock().unwrap();
        let ben = state.peers.iter().find(|peer| peer.id == "b").unwrap();
        assert!(ben.tags.is_empty() && ben.visibility.is_none());
        drop(state);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn identity_rename_moves_the_display_name_only() {
        let dir = temp_dir("identity-rename");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let rename = |identity: &str, name: &str| {
            dispatch(
                Request {
                    version: PROTOCOL_VERSION,
                    id: "rename".into(),
                    method: "identity.rename".into(),
                    params: serde_json::json!({"identity": identity, "name": name}),
                },
                &store,
            )
        };
        assert!(rename("default", "Home").ok);
        assert_eq!(store.lock().unwrap().identities[0].name, "Home");
        assert_eq!(store.lock().unwrap().identities[0].id, "default");
    }

    #[test]
    fn peer_update_toggles_contact_flags() {
        let dir = temp_dir("peer-flags");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        {
            let mut state = store.lock().unwrap();
            state.peers.push(idfon_protocol::Peer {
                id: "p1".into(),
                identity: "default".into(),
                name: "Alice".into(),
                endpoint_id: None,
                endpoint_addr: None,
                devices: Vec::new(),
                aliases: vec![],
                ring_score: None,
                call_mode: None,
                pinned: false,
                instant_call: false,
                tags: Vec::new(),
                visibility: None,
                introduced_by: None,
                share: None,
            });
        }
        let update = |params: serde_json::Value| {
            dispatch(
                Request {
                    version: PROTOCOL_VERSION,
                    id: "update".into(),
                    method: "peer.update".into(),
                    params,
                },
                &store,
            )
        };
        // §19 model deltas: Favorites pin (D7) + instant-call flag (§5.1).
        assert!(update(serde_json::json!({"ref": "p1", "pinned": true, "instant_call": true})).ok);
        {
            let state = store.lock().unwrap();
            let peer = state.peers.iter().find(|peer| peer.id == "p1").unwrap();
            assert!(peer.pinned && peer.instant_call);
        }
        assert!(update(serde_json::json!({"ref": "p1", "pinned": false})).ok);
        assert!(!store.lock().unwrap().peers[0].pinned);
    }

    #[test]
    fn room_create_carries_project_metadata() {
        let dir = temp_dir("room-metadata");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let create = |id: &str, params: serde_json::Value| {
            dispatch(
                Request {
                    version: PROTOCOL_VERSION,
                    id: id.into(),
                    method: "room.create".into(),
                    params,
                },
                &store,
            )
        };
        // P6: type + icon + repo ride the room model.
        assert!(
            create(
                "meta",
                serde_json::json!({
                    "id": "r_meta", "name": "idfon", "kind": "project",
                    "icon": "🚀", "repo": "github.com/donpark/idfon",
                })
            )
            .ok
        );
        // Legacy continuity: an untyped room is a project (the room list was
        // always the project list).
        assert!(create("plain", serde_json::json!({"id": "r_plain"})).ok);
        let state = store.lock().unwrap();
        let meta = state.rooms.iter().find(|room| room.id == "r_meta").unwrap();
        assert_eq!(meta.kind, "project");
        assert_eq!(meta.icon.as_deref(), Some("🚀"));
        assert_eq!(meta.repo.as_deref(), Some("github.com/donpark/idfon"));
        let plain = state
            .rooms
            .iter()
            .find(|room| room.id == "r_plain")
            .unwrap();
        assert_eq!(plain.kind, "project");
    }

    #[test]
    fn publish_content_path_follows_the_name_directory() {
        assert_eq!(content_path(None, "ab"), "/files/sha256/ab");
        assert_eq!(
            content_path(Some("phonon-31m/bundle.json"), "ab"),
            "/files/phonon-31m/sha256/ab"
        );
        assert_eq!(content_path(Some("bundle.json"), "ab"), "/files/sha256/ab");
    }

    #[test]
    fn edge_url_composes_the_public_url() {
        assert_eq!(
            edge_url(Some("https://idfon.net".into()), "abc", "fs/x").unwrap(),
            "https://idfon.net/abc/fs/x"
        );
        assert_eq!(
            edge_url(Some("https://idfon.net/".into()), " abc ", "/fs/x").unwrap(),
            "https://idfon.net/abc/fs/x"
        );
        assert!(edge_url(None, "abc", "/x").is_err());
        assert!(edge_url(Some("https://idfon.net".into()), "", "/x").is_err());
    }

    #[test]
    fn edge_url_uses_z32_host_form_for_endpoint_ids() {
        let key = idfon_core::IdentityKey::generate();
        let id: iroh::EndpointId = key.peer_id().parse().unwrap();
        let z = idfon_core::endpoint_ref(&id);
        assert_eq!(z.len(), 52);
        assert_eq!(
            edge_url(Some("https://idfon.net".into()), &id.to_string(), "fs/x").unwrap(),
            format!("https://{z}.idfon.net/fs/x")
        );
        // An IP/port base cannot take a wildcard subdomain: path form.
        assert_eq!(
            edge_url(
                Some("http://127.0.0.1:8080".into()),
                &id.to_string(),
                "fs/x"
            )
            .unwrap(),
            format!("http://127.0.0.1:8080/{}/fs/x", id)
        );
    }

    #[test]
    fn resource_ticket_authorizes_issuer_capability_and_path() {
        let dir = temp_dir("resource-ticket");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let owner_key = idfon_core::IdentityKey::generate();
        let owner = owner_key.peer_id();
        let state = ProviderState {
            store,
            identity: "default".into(),
            peer_id: Some(owner.clone()),
            root: dir,
        };
        let expiry = (now().parse::<u64>().unwrap() + 3600).to_string();
        let read = idfon_protocol::Capability::new(CAPABILITY_RESOURCE_READ);

        let scoped = idfon_core::TicketSpec {
            subject: None,
            capabilities: vec![read.clone()],
            expires_at: Some(expiry.clone()),
            ticket_id: "t-scoped".into(),
            path_scope: Some("/fs/public".into()),
            ..Default::default()
        }
        .issue(&owner_key).unwrap();
        let raw = scoped.token();
        assert!(ticket_authorizes(&state, &raw, "/fs/public/x"));
        assert!(!ticket_authorizes(&state, &raw, "/fs/private/x"));

        // A ticket from a peer with no grant is refused...
        let delegate_key = idfon_core::IdentityKey::generate();
        let delegate_id = delegate_key.peer_id();
        let delegated = idfon_core::TicketSpec {
            subject: None,
            capabilities: vec![read.clone()],
            expires_at: Some(expiry.clone()),
            ticket_id: "t-delegate".into(),
            ..Default::default()
        }
        .issue(&delegate_key).unwrap();
        let delegated_raw = delegated.token();
        assert!(!ticket_authorizes(&state, &delegated_raw, "/fs/x"));

        // ...but granting the issuer `resource.read` authorizes it, and revoking
        // the grant removes that (the peer, not the forwarding transport, is the
        // authority).
        state
            .store
            .lock()
            .unwrap()
            .grants
            .push(idfon_protocol::CapabilityGrant {
                capability: read.clone(),
                identity: "default".into(),
                subject: delegate_id.clone(),
                conversation: None,
                active_at: "0".into(),
                expires_at: None,
                revision: 1,
                revoked_at: None,
            });
        assert!(ticket_authorizes(&state, &delegated_raw, "/fs/x"));
        for grant in &mut state.store.lock().unwrap().grants {
            if grant.subject == delegate_id {
                grant.revoked_at = Some(now());
            }
        }
        assert!(!ticket_authorizes(&state, &delegated_raw, "/fs/x"));

        // Missing capability and expiry both fail.
        let wrong = idfon_core::TicketSpec {
            subject: None,
            capabilities: vec![idfon_protocol::Capability::new("message.receive")],
            expires_at: Some(expiry),
            ticket_id: "t-wrong".into(),
            ..Default::default()
        }
        .issue(&owner_key).unwrap();
        assert!(!ticket_authorizes(
            &state,
            &wrong.token(),
            "/fs/x"
        ));

        let expired = idfon_core::TicketSpec {
            subject: None,
            capabilities: vec![read],
            expires_at: Some("1".to_owned()),
            ticket_id: "t-expired".into(),
            ..Default::default()
        }
        .issue(&owner_key).unwrap();
        assert!(!ticket_authorizes(
            &state,
            &expired.token(),
            "/fs/x"
        ));
    }

    fn temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "idfond-{name}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    /// `message.send` stamps the caller-pushed `context` onto the outbound
    /// envelope, and leaves it unset when the caller did not supply one.
    #[tokio::test]
    async fn message_send_stamps_context_on_the_envelope() {
        let dir = temp_dir("send-context");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let transport = Arc::new(TransportMode::new("fake", None, None).await.unwrap());
        let key = idfon_core::IdentityKey::generate();
        let peer_id = key.peer_id();
        {
            let mut state = store.lock().unwrap();
            state.identities.push(Identity {
                id: "default".into(),
                name: "default".into(),
                endpoint_id: None,
                public_key: None,
                active: false,
                share: None,
            });
            let data_dir = state.data_dir.clone();
            state.ensure_identity_keys(&data_dir).unwrap();
            state.peers.push(idfon_protocol::Peer {
                id: peer_id.clone(),
                identity: "default".into(),
                name: "Alice".into(),
                endpoint_id: Some("ep".into()),
                endpoint_addr: None,
                devices: Vec::new(),
                aliases: vec![],
                ring_score: None,
                call_mode: None,
                pinned: false,
                instant_call: false,
                tags: Vec::new(),
                visibility: None,
                introduced_by: None,
                share: None,
            });
            state.grants.push(idfon_protocol::CapabilityGrant {
                capability: idfon_protocol::Capability::MessageSend,
                identity: "default".into(),
                subject: peer_id.clone(),
                conversation: None,
                active_at: "0".into(),
                expires_at: None,
                revision: 1,
                revoked_at: None,
            });
        }
        let send = |key: &str, to: &str, context: Option<&str>| {
            let mut params = serde_json::json!({
                "identity": "default",
                "to": to,
                "text": "hi",
                "idempotency_key": key,
            });
            if let Some(context) = context {
                params["context"] = serde_json::Value::from(context);
            }
            let request = Request {
                version: PROTOCOL_VERSION,
                id: "test".into(),
                method: "message.send".into(),
                params,
            };
            send_message(&request, &store, Some(&transport))
        };
        assert!(send("k1", &peer_id, Some("Client STT=Apple Built-in")).ok);
        {
            let state = store.lock().unwrap();
            let envelope = state.operations.last().unwrap().outbound.as_ref().unwrap();
            assert_eq!(
                envelope.context.as_deref(),
                Some("Client STT=Apple Built-in")
            );
        }
        assert!(send("k2", &peer_id, None).ok);
        let state = store.lock().unwrap();
        assert!(state
            .operations
            .last()
            .unwrap()
            .outbound
            .as_ref()
            .unwrap()
            .context
            .is_none());
    }

    async fn http_get_with_credential(
        addr: std::net::SocketAddr,
        path: &str,
        credential: &str,
        cookie: bool,
        host_port: u16,
    ) -> (u16, String) {
        let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
        let auth = if cookie {
            format!("Cookie: idfon_gateway={credential}\r\n")
        } else {
            format!("Authorization: Bearer {credential}\r\n")
        };
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: localhost:{host_port}\r\n{auth}Connection: close\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).await.expect("write");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.expect("read");
        let text = String::from_utf8_lossy(&raw).into_owned();
        let status = text
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse().ok())
            .unwrap_or(0);
        let body = text
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.to_owned())
            .unwrap_or_default();
        (status, body)
    }

    async fn http_get(addr: std::net::SocketAddr, path: &str, token: &str) -> (u16, String) {
        http_get_with_credential(addr, path, token, false, addr.port()).await
    }

    /// End-to-end: the daemon's embedded gateway resolves an `idfon://` account
    /// handle through the peer store and fetches the peer's resource over H3.
    #[tokio::test]
    async fn gateway_resolves_account_handle_and_fetches_over_h3() {
        let dir = temp_dir("gateway-h3");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let transport = Arc::new(TransportMode::new("iroh", Some([21; 32]), None).await.unwrap());

        // The peer: a real folder provider served over H3.
        let root = temp_dir("gateway-h3-root");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("hello.txt"), "hello from peer").unwrap();
        let server = Arc::new(IrohTransport::builder().secret_key([22; 32]).bind().await.unwrap());
        let _ = tokio::time::timeout(Duration::from_secs(5), server.endpoint().online()).await;
        let router = idfon_mcp::fs::router(&root, "acct-1").unwrap();
        let _h3 = idfon_h3::serve_router(&server, router);
        let accept_transport = Arc::clone(&server);
        let accept = tokio::spawn(async move {
            let _ = accept_transport
                .serve(|_| async {
                    Err::<idfon_protocol::MessageAck, idfon_core::transport::TransportError>(
                        idfon_core::transport::TransportError::Failed("message path unused".into()),
                    )
                })
                .await;
        });

        // A known contact whose account handle is what the URL will carry.
        let account_id = "acct-1";
        {
            let mut state = store.lock().unwrap();
            let mut peer = idfon_protocol::Peer {
                id: account_id.into(),
                identity: "default".into(),
                name: "Alice".into(),
                endpoint_id: Some(server.endpoint().id().to_string()),
                endpoint_addr: Some(serde_json::to_string(&server.endpoint().addr()).unwrap()),
                devices: Vec::new(),
                aliases: Vec::new(),
                ring_score: None,
                call_mode: None,
                pinned: false,
                instant_call: false,
                tags: Vec::new(),
                visibility: None,
                introduced_by: None,
                share: None,
            };
            ensure_account_alias(&mut peer);
            state.peers.push(peer);
        }

        let request = Request {
            version: PROTOCOL_VERSION,
            id: "gateway-start".into(),
            method: "gateway.start".into(),
            params: serde_json::json!({"identity": "default"}),
        };
        let response = gateway_start(&request, &store, Some(&transport), "default").await;
        let (addr, token) = match response.body {
            ResponseBody::Success { result, .. } => (
                result["addr"].as_str().unwrap().parse().unwrap(),
                result["token"].as_str().unwrap().to_owned(),
            ),
            ResponseBody::Failure { error, .. } => {
                panic!("gateway.start failed: {}", error.message)
            }
        };

        let handle = idfon_core::account_alias(account_id);
        let (status, body) = http_get(addr, &format!("/{handle}/fs/hello.txt"), &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, "hello from peer");

        // Unknown refs never reach the peer.
        let (status, _) = http_get(addr, "/0000/fs/hello.txt", &token).await;
        assert_eq!(status, 404);

        // The bearer token remains available only to the trusted native client.
        let (status, _) = http_get(addr, &format!("/{handle}/fs/hello.txt"), "wrong").await;
        assert_eq!(status, 401);

        // Web pages receive a signed credential scoped to one peer and the
        // containing directory, not the identity-wide gateway token.
        let scoped_request = Request {
            version: PROTOCOL_VERSION,
            id: "gateway-scoped".into(),
            method: "gateway.start".into(),
            params: serde_json::json!({
                "identity": "default",
                "account": handle.clone(),
                "path": "/fs/hello.txt",
            }),
        };
        let scoped_response = gateway_start(&scoped_request, &store, Some(&transport), "default").await;
        let scoped = match scoped_response.body {
            ResponseBody::Success { result, .. } => result["token"].as_str().unwrap().to_owned(),
            ResponseBody::Failure { error, .. } => panic!("scoped gateway.start failed: {}", error.message),
        };
        let (status, body) = http_get_with_credential(
            addr,
            &format!("/{handle}/fs/hello.txt"),
            &scoped,
            true,
            addr.port(),
        )
        .await;
        assert_eq!((status, body.as_str()), (200, "hello from peer"));
        let (status, _) = http_get_with_credential(
            addr,
            "/another-peer/fs/hello.txt",
            &scoped,
            true,
            addr.port(),
        )
        .await;
        assert_eq!(status, 403, "a leaked page credential cannot read another peer");
        let (status, _) = http_get_with_credential(
            addr,
            &format!("/{handle}/private/secret.txt"),
            &scoped,
            true,
            addr.port(),
        )
        .await;
        assert_eq!(status, 403, "a page credential cannot escape its directory");
        let (status, _) = http_get_with_credential(
            addr,
            &format!("/{handle}/fs/hello.txt"),
            &scoped,
            true,
            addr.port().wrapping_add(1),
        )
        .await;
        assert_eq!(status, 403, "alternate Host port is rejected");

        // The identity-wide secret is never accepted from a page URL/cookie.
        let (status, _) = http_get_with_credential(
            addr,
            &format!("/{handle}/fs/hello.txt"),
            &token,
            true,
            addr.port(),
        )
        .await;
        assert_eq!(status, 401);

        gateways().lock().unwrap().remove("default");
        accept.abort();
        drop(_h3);
        server.endpoint().close().await;
        std::fs::remove_dir_all(dir).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    /// The daemon's H3 provider serves media resources only to a caller whose
    /// endpoint maps to a peer holding the `resource.read` grant.
    #[tokio::test]
    async fn provider_serves_a_live_shared_root_to_a_granted_peer() {
        let dir = temp_dir("provider-grant");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let transport = Arc::new(TransportMode::new("iroh", Some([31; 32]), None).await.unwrap());
        let caller = IrohTransport::builder().secret_key([32; 32]).bind().await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), caller.endpoint().online()).await;

        // The user's live shared directory, served in place (no copy, no record).
        let shared = dir.join("shared");
        std::fs::create_dir_all(shared.join("session_1/chart")).unwrap();
        std::fs::write(shared.join("notes.txt"), "secret bytes").unwrap();
        std::fs::write(shared.join("session_1/chart/artifact.html"), "<h1>hi</h1>").unwrap();

        {
            let mut state = store.lock().unwrap();
            state.peers.push(idfon_protocol::Peer {
                id: "caller".into(),
                identity: "default".into(),
                name: "Caller".into(),
                endpoint_id: Some(caller.endpoint().id().to_string()),
                endpoint_addr: Some(serde_json::to_string(&caller.endpoint().addr()).unwrap()),
                devices: Vec::new(),
                aliases: Vec::new(),
                ring_score: None,
                call_mode: None,
                pinned: false,
                instant_call: false,
                tags: Vec::new(),
                visibility: None,
                introduced_by: None,
                share: None,
            });
            state.grants.push(idfon_protocol::CapabilityGrant {
                capability: idfon_protocol::Capability::new("resource.read"),
                identity: "default".into(),
                subject: "caller".into(),
                conversation: None,
                active_at: "0".into(),
                expires_at: None,
                revision: 1,
                revoked_at: None,
            });
        }

        // The provider router registers a side channel; dispatching needs the
        // transport's accept loop running.
        let serving = transport
            .current_transport_async("default")
            .await
            .expect("default transport bound");
        let server_addr = serving.endpoint().addr();
        let accept_transport = Arc::clone(&serving);
        let accept = tokio::spawn(async move {
            let _ = accept_transport
                .serve(|_| async {
                    Err::<idfon_protocol::MessageAck, idfon_core::transport::TransportError>(
                        idfon_core::transport::TransportError::Failed("unused".into()),
                    )
                })
                .await;
        });

        let request = Request {
            version: PROTOCOL_VERSION,
            id: "provider-start".into(),
            method: "provider.start".into(),
            params: serde_json::json!({"identity": "default", "root": shared.display().to_string()}),
        };
        assert!(
            provider_start(&request, &store, Some(&transport), "default")
                .await
                .ok
        );

        let client = idfon_h3::H3Client::new(&caller).unwrap();
        client.add_address(&server_addr);

        let response = client
            .get(&server_addr, "/fs/notes.txt")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.headers.get("content-type").unwrap(), "text/plain");
        assert_eq!(response.bytes().await.unwrap(), &b"secret bytes"[..]);

        let response = client
            .get(&server_addr, "/fs/session_1/chart/artifact.html")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.headers.get("content-type").unwrap(), "text/html");

        // Served in place: a delete shows up on the next request...
        std::fs::remove_file(shared.join("notes.txt")).unwrap();
        let response = client
            .get(&server_addr, "/fs/notes.txt")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status, 404);

        // ...and so does a rename (old path gone, new path live).
        std::fs::rename(
            shared.join("session_1/chart/artifact.html"),
            shared.join("session_1/chart/renamed.html"),
        )
        .unwrap();
        let response = client
            .get(&server_addr, "/fs/session_1/chart/artifact.html")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status, 404);
        let response = client
            .get(&server_addr, "/fs/session_1/chart/renamed.html")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status, 200);

        // Revoking the grant closes the provider without a restart.
        store.lock().unwrap().grants.clear();
        let response = client
            .get(&server_addr, "/fs/session_1/chart/renamed.html")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status, 403);

        providers().lock().unwrap().remove("default");
        accept.abort();
        caller.endpoint().close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The full remote-view path: a producer writes a path-named artifact
    /// through the real `media.resource.put` (what the agent bridge does), and a
    /// granted peer fetches it through the loopback gateway by account handle —
    /// no registration step anywhere.
    #[tokio::test]
    async fn peer_fetches_a_shared_file_through_the_gateway() {
        let producer_dir = temp_dir("artifact-producer");
        let producer_store = Arc::new(Mutex::new(Store::load(&producer_dir).unwrap()));
        let producer_transport =
            Arc::new(TransportMode::new("iroh", Some([41; 32]), None).await.unwrap());
        let consumer_transport = IrohTransport::builder().secret_key([42; 32]).bind().await.unwrap();
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            consumer_transport.endpoint().online(),
        )
        .await;

        // This test gets its own identity so its global PROVIDERS entry cannot
        // collide with the other provider test when the suite runs in parallel.
        producer_transport
            .add_identity("e2e", [43; 32])
            .await
            .unwrap();
        let producer = producer_transport
            .current_transport_async("e2e")
            .await
            .expect("producer transport bound");
        let producer_addr = producer.endpoint().addr();
        {
            let mut state = producer_store.lock().unwrap();
            state.peers.push(idfon_protocol::Peer {
                id: "consumer".into(),
                identity: "e2e".into(),
                name: "Consumer".into(),
                endpoint_id: Some(consumer_transport.endpoint().id().to_string()),
                endpoint_addr: Some(
                    serde_json::to_string(&consumer_transport.endpoint().addr()).unwrap(),
                ),
                devices: Vec::new(),
                aliases: Vec::new(),
                ring_score: None,
                call_mode: None,
                pinned: false,
                instant_call: false,
                tags: Vec::new(),
                visibility: None,
                introduced_by: None,
                share: None,
            });
            state.grants.push(idfon_protocol::CapabilityGrant {
                capability: idfon_protocol::Capability::new("resource.read"),
                identity: "e2e".into(),
                subject: "consumer".into(),
                conversation: None,
                active_at: "0".into(),
                expires_at: None,
                revision: 1,
                revoked_at: None,
            });
        }

        let accept_transport = Arc::clone(&producer);
        let accept = tokio::spawn(async move {
            let _ = accept_transport
                .serve(|_| async {
                    Err::<idfon_protocol::MessageAck, idfon_core::transport::TransportError>(
                        idfon_core::transport::TransportError::Failed("unused".into()),
                    )
                })
                .await;
        });
        // The user (or producer) saves a file into the user-visible shared dir;
        // the daemon serves it in place.
        let shared = producer_dir.join("shared");
        std::fs::create_dir_all(shared.join("session_1/chart")).unwrap();
        std::fs::write(shared.join("session_1/chart/result.html"), "<h1>chart</h1>").unwrap();

        let start = Request {
            version: PROTOCOL_VERSION,
            id: "provider-start".into(),
            method: "provider.start".into(),
            params: serde_json::json!({"identity": "e2e", "root": shared.display().to_string()}),
        };
        assert!(
            provider_start(&start, &producer_store, Some(&producer_transport), "e2e")
                .await
                .ok
        );

        // The consumer resolves the producer's account handle to its endpoint,
        // then fetches through the local gateway over H3.
        let consumer_dir = temp_dir("artifact-consumer");
        let consumer_store = Arc::new(Mutex::new(Store::load(&consumer_dir).unwrap()));
        let account_id = "producer-acct";
        {
            let mut state = consumer_store.lock().unwrap();
            let mut peer = idfon_protocol::Peer {
                id: account_id.into(),
                identity: "default".into(),
                name: "Producer".into(),
                endpoint_id: Some(producer.endpoint().id().to_string()),
                endpoint_addr: Some(serde_json::to_string(&producer_addr).unwrap()),
                devices: Vec::new(),
                aliases: Vec::new(),
                ring_score: None,
                call_mode: None,
                pinned: false,
                instant_call: false,
                tags: Vec::new(),
                visibility: None,
                introduced_by: None,
                share: None,
            };
            ensure_account_alias(&mut peer);
            state.peers.push(peer);
        }
        let backend = IrohBackend::new(
            &consumer_transport,
            PeerStoreResolver {
                store: Arc::clone(&consumer_store),
                identity: "default".into(),
            },
        )
        .unwrap();
        let handle = idfon_gateway::serve(
            GatewayConfig {
                auth: Some(std::sync::Arc::new(StaticToken::new("tok"))),
                ..GatewayConfig::default()
            },
            backend,
            TokenOnly,
        )
        .await
        .unwrap();

        let account = idfon_core::account_alias(account_id);
        let (status, body) = http_get(
            handle.local_addr(),
            &format!("/{account}/fs/session_1/chart/result.html"),
            "tok",
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body, "<h1>chart</h1>");

        providers().lock().unwrap().remove("e2e");
        accept.abort();
        drop(handle);
        consumer_transport.endpoint().close().await;
        std::fs::remove_dir_all(producer_dir).unwrap();
        std::fs::remove_dir_all(consumer_dir).unwrap();
    }

    #[test]
    fn staging_file_rejects_traversal() {
        let root = PathBuf::from("/tmp/idfon-resource-root");
        assert_eq!(
            staging_file(&root, "default", "session/a.txt"),
            Some(root.join("staging/default/session/a.txt"))
        );
        assert!(staging_file(&root, "default", "../secret").is_none());
        assert!(staging_file(&root, "../default", "a.txt").is_none());
    }

    #[test]
    fn event_cursor_keeps_advancing_after_retention_fills() {
        let event = |number: u64| idfon_protocol::Event {
            event_id: format!("event-{number}"),
            cursor: format!("cur_{number:020}"),
            r#type: "test".into(),
            timestamp: "0".into(),
            identity: "default".into(),
            data: serde_json::json!({}),
        };
        let mut events: Vec<_> = (1..=EVENT_RETENTION as u64).map(event).collect();
        assert_eq!(next_event_number(&events), EVENT_RETENTION as u64 + 1);
        events.push(event(EVENT_RETENTION as u64 + 1));
        events.drain(..1);
        assert_eq!(events.len(), EVENT_RETENTION);
        assert_eq!(next_event_number(&events), EVENT_RETENTION as u64 + 2);
    }

    #[test]
    fn capability_ticket_issue_and_revoke_persist() {
        let dir = temp_dir("capability-ticket");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let subject = idfon_core::IdentityKey::generate().peer_id();
        let issue = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "issue".into(),
                method: "capability.ticket".into(),
                params: serde_json::json!({"identity":"default", "subject": subject, "capabilities":["message.receive"], "ticket_id":"ticket-1", "expires_at":"2099-01-01T00:00:00Z"}),
            },
            &store,
        );
        assert!(issue.ok);
        let ticket = match issue.body {
            ResponseBody::Success { result, .. } => result["ticket"].clone(),
            ResponseBody::Failure { .. } => panic!("ticket issuance failed"),
        };
        let ticket: idfon_core::CapabilityTicket = serde_json::from_value(ticket).unwrap();
        assert_eq!(ticket.subject(), Some(subject.clone()));
        assert_eq!(ticket.ticket_id(), "ticket-1");
        let revoke = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "revoke".into(),
                method: "capability.ticket.revoke".into(),
                params: serde_json::json!({"identity":"default", "ticket_id":"ticket-1"}),
            },
            &store,
        );
        assert!(revoke.ok);
        assert!(Store::load(&dir)
            .unwrap()
            .revoked_tickets
            .iter()
            .any(|id| id == "ticket-1"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn authenticated_message_is_accepted_and_tampering_rejected() {
        let dir = temp_dir("auth");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let key = idfon_core::IdentityKey::generate();
        let message = idfon_core::sign_message(
            &key,
            "endpoint-a",
            "msg-1",
            idfon_protocol::MessageContent::Text {
                text: "hello".into(),
            },
            "retry-1",
            None,
        )
        .unwrap();
        let peer_id = key.peer_id();
        let mut state = store.lock().unwrap();
        state.peers.push(idfon_protocol::Peer {
            id: peer_id.clone(),
            identity: "default".into(),
            name: "Alice".into(),
            endpoint_id: Some("endpoint-a".into()),
            endpoint_addr: None,
            devices: Vec::new(),
            aliases: Vec::new(),
            ring_score: None,
            call_mode: None,
            pinned: false,
            instant_call: false,
            tags: Vec::new(),
            visibility: None,
            introduced_by: None,
            share: None,
        });
        state.grants.push(idfon_protocol::CapabilityGrant {
            capability: idfon_protocol::Capability::MessageReceive,
            identity: "default".into(),
            subject: peer_id.clone(),
            conversation: None,
            active_at: "0".into(),
            expires_at: None,
            revision: 1,
            revoked_at: None,
        });
        drop(state);
        let request = Request {
            version: PROTOCOL_VERSION,
            id: "test".into(),
            method: "message.receive".into(),
            params: serde_json::to_value(&message).unwrap(),
        };
        assert!(dispatch(request.clone(), &store).ok);
        assert!(store.lock().unwrap().grants.iter().any(|grant| {
            grant.identity == "default"
                && grant.subject == peer_id
                && grant.capability == idfon_protocol::Capability::MessageSend
        }));
        // The event names the sender's key, which apps use to refer to it.
        assert!(store.lock().unwrap().events.iter().any(|event| {
            event.r#type == "message.received" && event.data["idempotency_key"] == "retry-1"
        }));

        let mut tampered = message;
        tampered.content = idfon_protocol::MessageContent::Text {
            text: "changed".into(),
        };
        let response = dispatch(
            Request {
                params: serde_json::to_value(tampered).unwrap(),
                ..request
            },
            &store,
        );
        assert!(matches!(
            response.body,
            ResponseBody::Failure {
                error: ApiError {
                    code: ErrorCode::Unauthorized,
                    ..
                },
                ..
            }
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A signed `message.receive` for the contact-request tests.
    fn signed_text(
        key: &idfon_core::IdentityKey,
        message_id: &str,
        text: &str,
        idempotency_key: &str,
        conversation: Option<&str>,
    ) -> Request {
        let message = idfon_core::sign_message(
            key,
            endpoint_of(key).to_string(),
            message_id,
            idfon_protocol::MessageContent::Text { text: text.into() },
            idempotency_key,
            conversation.map(str::to_owned),
        )
        .unwrap();
        let mut params = serde_json::to_value(&message).unwrap();
        params["identity"] = "default".into();
        Request {
            version: PROTOCOL_VERSION,
            id: message_id.into(),
            method: "message.receive".into(),
            params,
        }
    }

    /// The test sender's endpoint id (derived from its key, as on a device
    /// without a separate endpoint key).
    fn endpoint_of(key: &idfon_core::IdentityKey) -> iroh::EndpointId {
        iroh::SecretKey::from_bytes(&key.to_bytes()).public()
    }

    fn rpc_call(store: &Arc<Mutex<Store>>, method: &str, params: serde_json::Value) -> Response {
        dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: method.into(),
                method: method.into(),
                params,
            },
            store,
        )
    }

    fn result_of(response: &Response) -> serde_json::Value {
        match &response.body {
            ResponseBody::Success { result, .. } => result.clone(),
            ResponseBody::Failure { error, .. } => panic!("unexpected failure: {error:?}"),
        }
    }

    fn events_of(store: &Arc<Mutex<Store>>, event_type: &str) -> Vec<serde_json::Value> {
        store
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|event| event.r#type == event_type)
            .map(|event| event.data.clone())
            .collect()
    }

    #[test]
    fn unknown_sender_text_becomes_a_contact_request() {
        let dir = temp_dir("request-held");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let sender = idfon_core::IdentityKey::generate();
        let address = serde_json::to_string(&EndpointAddr::new(endpoint_of(&sender))).unwrap();
        let mut first = signed_text(&sender, "msg-1", "hi, it's me", "k1", None);
        first.params[REMOTE_ADDR_PARAM] = address.clone().into();
        let response = dispatch(first, &store);
        assert!(response.ok, "held, not refused: the sender's outbox marks it delivered");
        assert_eq!(result_of(&response)["status"], "held");
        // An address for some other endpoint is ignored (room gossip passes
        // raw JSON, so the param can't be trusted on its own).
        let mut second = signed_text(&sender, "msg-2", "are you there?", "k2", None);
        let other = idfon_core::IdentityKey::generate();
        second.params[REMOTE_ADDR_PARAM] =
            serde_json::to_string(&EndpointAddr::new(endpoint_of(&other))).unwrap().into();
        assert!(dispatch(second, &store).ok);
        // A resend of the same key is a duplicate, not a third message.
        assert_eq!(
            result_of(&dispatch(signed_text(&sender, "msg-1b", "hi, it's me", "k1", None), &store))["status"],
            "duplicate"
        );

        let requests = result_of(&rpc_call(&store, "request.list", serde_json::json!({})))["requests"].clone();
        assert_eq!(requests.as_array().unwrap().len(), 1, "one request per sender");
        let held = &requests[0];
        assert_eq!(held["peer_id"], sender.peer_id());
        assert_eq!(held["endpoint_id"], endpoint_of(&sender).to_string());
        assert_eq!(held["endpoint_addr"], address);
        assert_eq!(held["messages"].as_array().unwrap().len(), 2);
        assert_eq!(held["messages"][1]["text"], "are you there?");
        assert_eq!(held["messages"][1]["idempotency_key"], "k2");

        let events = events_of(&store, "contact.request");
        assert_eq!(events.len(), 2, "one event per held message");
        assert_eq!(events[0]["request_id"], held["request_id"]);
        assert_eq!(events[0]["text"], "hi, it's me");
        assert_eq!(events[0]["idempotency_key"], "k1");
        assert!(events_of(&store, "message.received").is_empty());
        assert!(store.lock().unwrap().peers.is_empty(), "nothing is auto-accepted");

        // Persisted with the rest of the state.
        drop(store);
        let reloaded = Store::load(&dir).unwrap();
        assert_eq!(reloaded.contact_requests.len(), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn accepting_a_request_delivers_it_and_later_messages_pass() {
        let dir = temp_dir("request-accept");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let sender = idfon_core::IdentityKey::generate();
        assert!(dispatch(signed_text(&sender, "msg-1", "hello", "k1", None), &store).ok);
        let request_id = events_of(&store, "contact.request")[0]["request_id"].clone();

        let response = rpc_call(
            &store,
            "request.accept",
            serde_json::json!({"request_id": request_id, "name": "Sam"}),
        );
        let peer = result_of(&response)["peer"].clone();
        assert_eq!(peer["id"], sender.peer_id());
        assert_eq!(peer["name"], "Sam");
        assert_eq!(peer["endpoint_id"], endpoint_of(&sender).to_string());
        // No address from a connection: dialed by id (discovery).
        let address: EndpointAddr =
            serde_json::from_str(peer["endpoint_addr"].as_str().unwrap()).unwrap();
        assert_eq!(address.id, endpoint_of(&sender));

        let received = events_of(&store, "message.received");
        assert_eq!(received.len(), 1);
        assert_eq!(received[0]["text"], "hello");
        assert_eq!(received[0]["idempotency_key"], "k1");
        assert_eq!(received[0]["peer_id"], sender.peer_id());
        assert!(result_of(&rpc_call(&store, "request.list", serde_json::json!({})))["requests"]
            .as_array()
            .unwrap()
            .is_empty());
        for capability in [
            idfon_protocol::Capability::MessageSend,
            idfon_protocol::Capability::MessageReceive,
        ] {
            assert!(has_grant(&store.lock().unwrap(), "default", &sender.peer_id(), &capability));
        }

        // Later messages pass directly, and a resend of the held one is a duplicate.
        let later = dispatch(signed_text(&sender, "msg-2", "thanks", "k2", None), &store);
        assert_eq!(result_of(&later)["status"], "delivered");
        assert_eq!(events_of(&store, "message.received").len(), 2);
        assert_eq!(
            result_of(&dispatch(signed_text(&sender, "msg-1", "hello", "k1", None), &store))["status"],
            "duplicate"
        );
        assert!(events_of(&store, "contact.request").len() == 1);

        // Unnamed accepts get a short id.
        let other = idfon_core::IdentityKey::generate();
        assert!(dispatch(signed_text(&other, "msg-3", "hey", "k3", None), &store).ok);
        let request_id = events_of(&store, "contact.request")[1]["request_id"].clone();
        let peer = result_of(&rpc_call(&store, "request.accept", serde_json::json!({"request_id": request_id})))["peer"].clone();
        assert!(other.peer_id().starts_with(peer["name"].as_str().unwrap()));
        assert!(peer["name"].as_str().unwrap().len() < other.peer_id().len());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn accepting_with_a_conversation_admits_only_that_conversation() {
        let dir = temp_dir("request-scoped");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let joiner = idfon_core::IdentityKey::generate();
        assert!(dispatch(signed_text(&joiner, "msg-1", "joining", "k1", Some("room-1")), &store).ok);
        assert!(dispatch(signed_text(&joiner, "msg-2", "and a DM", "k2", None), &store).ok);
        let request_id = events_of(&store, "contact.request")[0]["request_id"].clone();
        assert_eq!(events_of(&store, "contact.request")[0]["conversation"], "room-1");

        assert!(rpc_call(
            &store,
            "request.accept",
            serde_json::json!({"request_id": request_id, "conversation": "room-1"}),
        )
        .ok);
        let received = events_of(&store, "message.received");
        assert_eq!(received.len(), 1, "only the room message is delivered");
        assert_eq!(received[0]["conversation"], "room-1");
        {
            let state = store.lock().unwrap();
            let receive = idfon_protocol::Capability::MessageReceive;
            assert!(has_grant_for_conversation(&state, "default", &joiner.peer_id(), &receive, Some("room-1")));
            assert!(!has_grant_for_conversation(&state, "default", &joiner.peer_id(), &receive, None));
            assert!(!has_grant_for_conversation(&state, "default", &joiner.peer_id(), &receive, Some("room-2")));
        }
        assert!(dispatch(signed_text(&joiner, "msg-3", "in the room", "k3", Some("room-1")), &store).ok);
        let outside = dispatch(signed_text(&joiner, "msg-4", "elsewhere", "k4", None), &store);
        assert!(matches!(
            outside.body,
            ResponseBody::Failure { error: ApiError { code: ErrorCode::CapabilityDenied, .. }, .. }
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn declining_with_block_refuses_later_messages() {
        let dir = temp_dir("request-block");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let sender = idfon_core::IdentityKey::generate();
        assert!(dispatch(signed_text(&sender, "msg-1", "buy now", "k1", None), &store).ok);
        let request_id = events_of(&store, "contact.request")[0]["request_id"].clone();
        let declined = rpc_call(
            &store,
            "request.decline",
            serde_json::json!({"request_id": request_id, "block": true}),
        );
        assert_eq!(result_of(&declined)["blocked"], true);

        let refused = dispatch(signed_text(&sender, "msg-2", "buy now!!", "k2", None), &store);
        assert!(matches!(
            refused.body,
            ResponseBody::Failure { error: ApiError { code: ErrorCode::Unauthorized, .. }, .. }
        ));
        assert!(store.lock().unwrap().contact_requests.is_empty());
        assert_eq!(events_of(&store, "contact.request").len(), 1, "no request for a blocked sender");
        // Blocks persist.
        assert_eq!(Store::load(&dir).unwrap().blocked_senders.len(), 1);

        let unblocked = rpc_call(&store, "request.unblock", serde_json::json!({"peer_id": sender.peer_id()}));
        assert_eq!(result_of(&unblocked)["unblocked"], true);
        assert!(dispatch(signed_text(&sender, "msg-3", "sorry", "k3", None), &store).ok);
        assert_eq!(store.lock().unwrap().contact_requests.len(), 1);

        // A plain decline drops the request without blocking.
        let request_id = store.lock().unwrap().contact_requests[0].request_id.clone();
        assert!(rpc_call(&store, "request.decline", serde_json::json!({"request_id": request_id})).ok);
        assert!(store.lock().unwrap().blocked_senders.is_empty());
        assert!(dispatch(signed_text(&sender, "msg-4", "again", "k4", None), &store).ok);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn files_and_envelopes_from_unknown_senders_are_refused() {
        let dir = temp_dir("request-file");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let sender = idfon_core::IdentityKey::generate();
        for (index, text) in [
            "IDFON-FILE/1\nname=photo.png\nticket=t",
            "IDFON-TYPING/1\nstate=on",
            "IDFON-UI-ACTION/1\ncard=c\naction=a",
        ]
        .into_iter()
        .enumerate()
        {
            let response = dispatch(
                signed_text(&sender, &format!("msg-{index}"), text, &format!("k{index}"), None),
                &store,
            );
            assert!(matches!(
                response.body,
                ResponseBody::Failure { error: ApiError { code: ErrorCode::Unauthorized, .. }, .. }
            ), "{text}");
        }
        assert!(store.lock().unwrap().contact_requests.is_empty());
        assert!(!is_envelope("IDFON is great"));
        assert!(!is_envelope("IDFON-FILE/one"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn contact_request_bounds_hold() {
        let dir = temp_dir("request-caps");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let sender = idfon_core::IdentityKey::generate();
        let too_long = "x".repeat(MAX_REQUEST_TEXT_BYTES + 1);
        assert!(!dispatch(signed_text(&sender, "msg-long", &too_long, "k-long", None), &store).ok);
        let at_limit = "x".repeat(MAX_REQUEST_TEXT_BYTES);
        assert!(dispatch(signed_text(&sender, "msg-limit", &at_limit, "k-limit", None), &store).ok);
        for index in 1..MAX_REQUEST_MESSAGES {
            assert!(dispatch(
                signed_text(&sender, &format!("msg-{index}"), "more", &format!("k{index}"), None),
                &store
            )
            .ok);
        }
        let over = dispatch(signed_text(&sender, "msg-over", "one more", "k-over", None), &store);
        assert!(!over.ok, "at most {MAX_REQUEST_MESSAGES} messages per sender");
        assert_eq!(store.lock().unwrap().contact_requests[0].messages.len(), MAX_REQUEST_MESSAGES);

        // More senders than the cap: the oldest request is dropped.
        let first_id = store.lock().unwrap().contact_requests[0].request_id.clone();
        for index in 0..MAX_REQUEST_SENDERS {
            let other = idfon_core::IdentityKey::generate();
            assert!(dispatch(signed_text(&other, &format!("m-{index}"), "hi", "k", None), &store).ok);
        }
        let state = store.lock().unwrap();
        assert_eq!(state.contact_requests.len(), MAX_REQUEST_SENDERS);
        assert!(!state.contact_requests.iter().any(|held| held.request_id == first_id));
        drop(state);
        // Newest first.
        let requests = result_of(&rpc_call(&store, "request.list", serde_json::json!({})))["requests"].clone();
        assert_eq!(requests[0]["messages"][0]["message_id"], format!("m-{}", MAX_REQUEST_SENDERS - 1));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn verified_ticket_bootstraps_grants_without_stored_grants() {
        let dir = temp_dir("ticket-bootstrap");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let sender_key = idfon_core::IdentityKey::generate();
        let receiver_key = store.lock().unwrap().identity_key("default").unwrap();
        let sender_id = sender_key.peer_id();
        // Cross-daemon pairing: peer exists, but no grants at all.
        store.lock().unwrap().peers.push(idfon_protocol::Peer {
            id: sender_id.clone(),
            identity: "default".into(),
            name: "Alice".into(),
            endpoint_id: Some("endpoint-a".into()),
            endpoint_addr: None,
            devices: Vec::new(),
            aliases: Vec::new(),
            ring_score: None,
            call_mode: None,
            pinned: false,
            instant_call: false,
            tags: Vec::new(),
            visibility: None,
            introduced_by: None,
            share: None,
        });
        let ticket = idfon_core::TicketSpec {
            subject: None,
            capabilities: vec![
                idfon_protocol::Capability::MessageReceive,
                idfon_protocol::Capability::LiveAudioSubscribe,
            ],
            expires_at: None,
            ticket_id: "tk-bootstrap".into(),
            ..Default::default()
        }
        .issue(&receiver_key).unwrap();
        let message = idfon_core::sign_message_with_ticket(
            &sender_key,
            "endpoint-a",
            "msg-boot",
            idfon_protocol::MessageContent::Text {
                text: "hello".into(),
            },
            "key-boot",
            None,
            Some(ticket),
        )
        .unwrap();
        let response = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "boot".into(),
                method: "message.receive".into(),
                params: serde_json::to_value(&message).unwrap(),
            },
            &store,
        );
        assert!(response.ok, "verified ticket must authorize first delivery");
        let state = store.lock().unwrap();
        for capability in [
            idfon_protocol::Capability::MessageReceive,
            idfon_protocol::Capability::LiveAudioSubscribe,
        ] {
            assert!(
                state.grants.iter().any(|grant| {
                    grant.identity == "default"
                        && grant.subject == sender_id
                        && grant.capability == capability
                }),
                "missing derived grant {capability:?}"
            );
        }
        assert!(
            state.grants.iter().any(|grant| {
                grant.identity == "default"
                    && grant.subject == sender_id
                    && grant.capability == idfon_protocol::Capability::MessageSend
            }),
            "reciprocal reply grant missing"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn endpoint_keyed_record_passes_for_the_signed_identity() {
        let dir = temp_dir("split-identity");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let sender_key = idfon_core::IdentityKey::generate();
        let receiver_key = store.lock().unwrap().identity_key("default").unwrap();
        let sender_id = sender_key.peer_id();
        // Pairing by endpoint ref keys the record on the endpoint id (#56):
        // it can never match `sender.peer_id`, only the signer's claimed
        // endpoint — the split-identity pairing that died at the gate.
        store.lock().unwrap().peers.push(idfon_protocol::Peer {
            id: "endpoint-a".into(),
            identity: "default".into(),
            name: "MacApp".into(),
            endpoint_id: Some("endpoint-a".into()),
            endpoint_addr: None,
            devices: Vec::new(),
            aliases: Vec::new(),
            ring_score: None,
            call_mode: None,
            pinned: false,
            instant_call: false,
            tags: Vec::new(),
            visibility: None,
            introduced_by: None,
            share: None,
        });
        let ticket = idfon_core::TicketSpec {
            subject: None,
            capabilities: vec![idfon_protocol::Capability::MessageReceive],
            expires_at: None,
            ticket_id: "tk-split".into(),
            ..Default::default()
        }
        .issue(&receiver_key).unwrap();
        let message = idfon_core::sign_message_with_ticket(
            &sender_key,
            "endpoint-a",
            "msg-split",
            idfon_protocol::MessageContent::Text {
                text: "hello".into(),
            },
            "key-split",
            None,
            Some(ticket),
        )
        .unwrap();
        let response = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "split".into(),
                method: "message.receive".into(),
                params: serde_json::to_value(&message).unwrap(),
            },
            &store,
        );
        assert!(
            response.ok,
            "endpoint-keyed record must accept the signature-verified identity"
        );
        // Attribution stays on the signer, never on the matched record.
        assert!(store.lock().unwrap().grants.iter().any(|grant| {
            grant.identity == "default"
                && grant.subject == sender_id
                && grant.capability == idfon_protocol::Capability::MessageSend
        }));

        // A stranger claiming the same endpoint gains nothing without a
        // ticket or grant of their own.
        let stranger_key = idfon_core::IdentityKey::generate();
        let stranger = idfon_core::sign_message(
            &stranger_key,
            "endpoint-a",
            "msg-spoof",
            idfon_protocol::MessageContent::Text {
                text: "spoof".into(),
            },
            "key-spoof",
            None,
        )
        .unwrap();
        let response = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "spoof".into(),
                method: "message.receive".into(),
                params: serde_json::to_value(stranger).unwrap(),
            },
            &store,
        );
        assert!(
            !response.ok,
            "an unvouched signer must not pass on the endpoint claim"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn signer_match_survives_endpoint_drift() {
        // A lost endpoint key silently falls back to the identity key, so the
        // sender's endpoint claim stops matching the record's known endpoint
        // (the fourth #56 bite: the wake-on-call E2E invite died at the gate
        // as "unknown peer" despite an exact signer match). The signature is
        // the authentication; the endpoint claim must not veto it.
        let dir = temp_dir("endpoint-drift");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let sender_key = idfon_core::IdentityKey::generate();
        let receiver_key = store.lock().unwrap().identity_key("default").unwrap();
        let sender_id = sender_key.peer_id();
        store.lock().unwrap().peers.push(idfon_protocol::Peer {
            id: sender_id.clone(),
            identity: "default".into(),
            name: "MacApp".into(),
            endpoint_id: Some("endpoint-a".into()),
            endpoint_addr: None,
            devices: Vec::new(),
            aliases: Vec::new(),
            ring_score: None,
            call_mode: None,
            pinned: false,
            instant_call: false,
            tags: Vec::new(),
            visibility: None,
            introduced_by: None,
            share: None,
        });
        let ticket = idfon_core::TicketSpec {
            subject: None,
            capabilities: vec![idfon_protocol::Capability::MessageReceive],
            expires_at: None,
            ticket_id: "tk-drift".into(),
            ..Default::default()
        }
        .issue(&receiver_key).unwrap();
        let message = idfon_core::sign_message_with_ticket(
            &sender_key,
            "endpoint-b", // drifted claim: not the record's endpoint
            "msg-drift",
            idfon_protocol::MessageContent::Text {
                text: "hello".into(),
            },
            "key-drift",
            None,
            Some(ticket),
        )
        .unwrap();
        let response = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "drift".into(),
                method: "message.receive".into(),
                params: serde_json::to_value(message).unwrap(),
            },
            &store,
        );
        assert!(
            response.ok,
            "an exact signer match must not be vetoed by a drifted endpoint claim"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn capability_ticket_scope_expiry_and_issuer_are_enforced() {
        let dir = temp_dir("capability-policy");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let sender_key = idfon_core::IdentityKey::generate();
        let receiver_key = store.lock().unwrap().identity_key("default").unwrap();
        let sender_id = sender_key.peer_id();
        let mut state = store.lock().unwrap();
        state.peers.push(idfon_protocol::Peer {
            id: sender_id.clone(),
            identity: "default".into(),
            name: "Alice".into(),
            endpoint_id: Some("endpoint-a".into()),
            endpoint_addr: None,
            devices: Vec::new(),
            aliases: Vec::new(),
            ring_score: None,
            call_mode: None,
            pinned: false,
            instant_call: false,
            tags: Vec::new(),
            visibility: None,
            introduced_by: None,
            share: None,
        });
        state.grants.push(idfon_protocol::CapabilityGrant {
            capability: idfon_protocol::Capability::MessageReceive,
            identity: "default".into(),
            subject: sender_id.clone(),
            conversation: None,
            active_at: "0".into(),
            expires_at: None,
            revision: 1,
            revoked_at: None,
        });
        drop(state);
        let rejected = |ticket: idfon_core::CapabilityTicket, message_id: &str| {
            let message = idfon_core::sign_message_with_ticket(
                &sender_key,
                "endpoint-a",
                message_id,
                idfon_protocol::MessageContent::Text {
                    text: "hello".into(),
                },
                message_id,
                None,
                Some(ticket),
            )
            .unwrap();
            let response = dispatch(
                Request {
                    version: PROTOCOL_VERSION,
                    id: message_id.into(),
                    method: "message.receive".into(),
                    params: serde_json::to_value(message).unwrap(),
                },
                &store,
            );
            assert!(matches!(
                response.body,
                ResponseBody::Failure {
                    error: ApiError {
                        code: ErrorCode::CapabilityDenied,
                        ..
                    },
                    ..
                }
            ));
        };
        let other_id = idfon_core::IdentityKey::generate().peer_id();
        rejected(
            idfon_core::TicketSpec {
                subject: Some(other_id),
                capabilities: vec![idfon_protocol::Capability::MessageReceive],
                expires_at: None,
                ticket_id: "ticket-subject".into(),
                ..Default::default()
            }
            .issue(&receiver_key).unwrap(),
            "message-subject",
        );
        rejected(
            idfon_core::TicketSpec {
                subject: Some(sender_id.clone()),
                capabilities: vec![idfon_protocol::Capability::MessageReceive],
                expires_at: Some("2000-01-01T00:00:00Z".into()),
                ticket_id: "ticket-expired".into(),
                ..Default::default()
            }
            .issue(&receiver_key).unwrap(),
            "message-expired",
        );
        rejected(
            idfon_core::TicketSpec {
                subject: Some(sender_id),
                capabilities: vec![idfon_protocol::Capability::MessageReceive],
                expires_at: None,
                ticket_id: "ticket-issuer".into(),
                ..Default::default()
            }
            .issue(&sender_key).unwrap(),
            "message-issuer",
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn duplicate_message_is_idempotent_and_conflicting_retry_is_rejected() {
        let dir = temp_dir("idempotency");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let key = idfon_core::IdentityKey::generate();
        let peer_id = key.peer_id();
        let mut state = store.lock().unwrap();
        state.peers.push(idfon_protocol::Peer {
            id: peer_id.clone(),
            identity: "default".into(),
            name: "Alice".into(),
            endpoint_id: Some("ep".into()),
            endpoint_addr: None,
            devices: Vec::new(),
            aliases: vec![],
            ring_score: None,
            call_mode: None,
            pinned: false,
            instant_call: false,
            tags: Vec::new(),
            visibility: None,
            introduced_by: None,
            share: None,
        });
        state.grants.push(idfon_protocol::CapabilityGrant {
            capability: idfon_protocol::Capability::MessageReceive,
            identity: "default".into(),
            subject: peer_id.clone(),
            conversation: None,
            active_at: "0".into(),
            expires_at: None,
            revision: 1,
            revoked_at: None,
        });
        drop(state);
        let signed = |text: &str, conversation: Option<&str>| {
            idfon_core::sign_message_with_ticket(
                &key,
                "ep",
                "msg-1",
                idfon_protocol::MessageContent::Text { text: text.into() },
                "key-1",
                conversation.map(str::to_owned),
                None,
            )
            .unwrap()
        };
        let request = |message| Request {
            version: PROTOCOL_VERSION,
            id: "test".into(),
            method: "message.receive".into(),
            params: serde_json::to_value(message).unwrap(),
        };
        assert!(dispatch(request(signed("hello", Some("chat-ab"))), &store).ok);
        let duplicate = dispatch(request(signed("hello", Some("chat-ab"))), &store);
        assert!(duplicate.ok);
        let other_conversation =
            dispatch(request(signed("different chat", Some("chat-cd"))), &store);
        assert!(other_conversation.ok);
        let conflict = dispatch(request(signed("changed", Some("chat-ab"))), &store);
        assert!(matches!(
            conflict.body,
            ResponseBody::Failure {
                error: ApiError {
                    code: ErrorCode::IdempotencyKeyConflict,
                    ..
                },
                ..
            }
        ));
        let reloaded = Store::load(&dir).unwrap();
        assert_eq!(reloaded.messages.len(), 2);
        assert_eq!(reloaded.events.len(), 2);
        assert_eq!(reloaded.operations.len(), 2);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn queued_outbound_operation_round_trips_for_restart() {
        let dir = temp_dir("outbound-operation");
        let mut state = Store::load(&dir).unwrap();
        let key = idfon_core::IdentityKey::generate();
        let envelope = idfon_core::sign_message(
            &key,
            "endpoint-a",
            "msg-restart",
            idfon_protocol::MessageContent::Text {
                text: "queued".into(),
            },
            "retry-restart",
            Some("chat-ab".into()),
        )
        .unwrap();
        state.operations.push(idfon_protocol::Operation {
            identity: "default".into(),
            operation_id: "op-restart".into(),
            method: "message.send".into(),
            status: idfon_protocol::OperationStatus::Queued,
            target: Some("alice-account".into()),
            request_fingerprint: Some("fingerprint".into()),
            created_at: now(),
            updated_at: now(),
            idempotency_key: Some("retry-restart".into()),
            message_id: Some("msg-restart".into()),
            outbound: Some(envelope),
            delivery: Some(idfon_protocol::DeliveryPolicy {
                mode: idfon_protocol::DeliveryMode::All,
                device_class: Some("mobile".into()),
                ..Default::default()
            }),
            durable: false,
        });
        state.save(&dir).unwrap();
        let restored = Store::load(&dir).unwrap();
        let operation = &restored.operations[0];
        assert_eq!(operation.status, idfon_protocol::OperationStatus::Queued);
        assert_eq!(
            operation.outbound.as_ref().unwrap().message_id,
            "msg-restart"
        );
        assert_eq!(
            operation.outbound.as_ref().unwrap().conversation.as_deref(),
            Some("chat-ab")
        );
        assert_eq!(
            operation.delivery.as_ref().unwrap().mode,
            idfon_protocol::DeliveryMode::All
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn conversation_scoped_grants_do_not_cross_conversations() {
        let dir = temp_dir("conversation-grants");
        let store = Store::load(&dir).unwrap();
        let grant = idfon_protocol::CapabilityGrant {
            capability: idfon_protocol::Capability::MessageReceive,
            identity: "default".into(),
            subject: "alice-account".into(),
            conversation: Some("chat-ab".into()),
            active_at: "0".into(),
            expires_at: None,
            revision: 1,
            revoked_at: None,
        };
        let mut state = store;
        state.grants.push(grant);
        assert!(has_grant_for_conversation(
            &state,
            "default",
            "alice-account",
            &idfon_protocol::Capability::MessageReceive,
            Some("chat-ab"),
        ));
        assert!(!has_grant_for_conversation(
            &state,
            "default",
            "alice-account",
            &idfon_protocol::Capability::MessageReceive,
            Some("chat-cd"),
        ));
        assert!(!has_grant_for_conversation(
            &state,
            "default",
            "alice-account",
            &idfon_protocol::Capability::MessageReceive,
            None,
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn peer_mutations_persist() {
        let dir = temp_dir("management");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let add = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "add".into(),
                method: "peer.add".into(),
                params: serde_json::json!({"id": "peer-1", "name": "Alice", "aliases": ["alice@work"], "tags": ["Work"]}),
            },
            &store,
        );
        assert!(add.ok, "{add:?}");
        let state = Store::load(&dir).unwrap();
        assert_eq!(state.identities.len(), 1);
        assert!(state.peers[0].aliases.iter().any(|alias| alias == "alice@work"));
        assert!(state.peers[0].aliases.contains(&idfon_core::account_alias("peer-1")));
        assert_eq!(state.peers[0].tags, vec!["Work".to_string()]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn contact_exchange_round_trips_through_vcard() {
        let dir = temp_dir("contact-exchange");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let send = |id: &str, method: &str, params: serde_json::Value| {
            dispatch(
                Request {
                    version: PROTOCOL_VERSION,
                    id: id.into(),
                    method: method.into(),
                    params,
                },
                &store,
            )
        };
        assert!(
            send(
                "add",
                "peer.add",
                serde_json::json!({"id": "bob", "name": "Bob", "endpoint_id": "ep1", "endpoint_addr": "{\"node_id\":\"bob\"}"}),
            )
            .ok
        );
        // A saved peer exports as a card (deep link + X property).
        let exported = send("exp", "contact.export", serde_json::json!({"ref": "Bob"}));
        let ResponseBody::Success { result, .. } = exported.body else {
            panic!("export failed")
        };
        let vcard = result["vcard"].as_str().unwrap().to_string();
        assert!(result["link"]
            .as_str()
            .unwrap()
            .starts_with("idfon://add/"));
        assert!(vcard.contains("URL:idfon://add/"));
        assert!(vcard.contains("X-IDFON-TICKET"));
        // The card imports back with its label as the name (peer.add falls
        // back to the ContactTicket label) — no name param passed.
        assert!(send("rm", "peer.remove", serde_json::json!({"ref": "bob"})).ok);
        let imported = send("imp", "contact.import", serde_json::json!({"text": vcard}));
        let ResponseBody::Success { result, .. } = imported.body else {
            panic!("import failed")
        };
        assert_eq!(result["peers"][0]["name"], "Bob");
        assert_eq!(result["peers"][0]["endpoint_id"], "ep1");
        // Re-import is an idempotent upsert.
        assert!(
            send("imp2", "contact.import", serde_json::json!({"text": vcard})).ok
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn contact_export_own_card_round_trips_through_vcard() {
        let dir = temp_dir("contact-export-own");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        // Bind the default identity (endpoint id + public key) and give the
        // fake transport a ticket for it — the wiring `contact.export` needs.
        let bound_name;
        {
            let mut state = store.lock().unwrap();
            let identity = state
                .identities
                .iter_mut()
                .find(|identity| identity.id == "default")
                .unwrap();
            identity.endpoint_id = Some("ep-me".into());
            identity.public_key = Some("pk-me".into());
            bound_name = identity.name.clone();
        }
        let fake = FakeTransport::default();
        fake.set_endpoint_ticket("default", br#"{"node_id":"me"}"#.to_vec());
        let transport = Arc::new(TransportMode::Fake(fake));
        let send = |id: &str, method: &str, params: serde_json::Value| {
            dispatch_with_transport(
                Request {
                    version: PROTOCOL_VERSION,
                    id: id.into(),
                    method: method.into(),
                    params,
                },
                &store,
                Some(&transport),
            )
        };
        // An unbound/unbound-referenced identity fails closed.
        assert!(!send(
            "exp-missing",
            "contact.export",
            serde_json::json!({"identity": "nope"})
        )
        .ok);
        // The identity's own card exports (default identity, and by name).
        let exported = send("exp", "contact.export", serde_json::json!({}));
        let ResponseBody::Success { result, .. } = exported.body else {
            panic!("own-card export failed")
        };
        let vcard = result["vcard"].as_str().unwrap().to_string();
        assert!(vcard.contains("URL:idfon://add/"));
        assert!(result["link"]
            .as_str()
            .unwrap()
            .starts_with("idfon://add/"));
        assert!(
            send("exp-named", "contact.export", serde_json::json!({"identity": bound_name}))
                .ok
        );
        // ... and imports back as a peer under the sharer's name.
        let imported = send("imp", "contact.import", serde_json::json!({"text": vcard}));
        let ResponseBody::Success { result, .. } = imported.body else {
            panic!("import failed")
        };
        assert_eq!(result["peers"][0]["id"], "pk-me");
        assert_eq!(result["peers"][0]["endpoint_id"], "ep-me");
        assert_eq!(result["peers"][0]["name"], bound_name.as_str());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn contact_import_rejects_foreign_vcards() {
        let dir = temp_dir("contact-import-foreign");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let imported = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "imp".into(),
                method: "contact.import".into(),
                params: serde_json::json!({"text": "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:No Ticket\r\nEND:VCARD\r\n"}),
            },
            &store,
        );
        assert!(!imported.ok);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn peer_carries_its_derived_account_handle() {
        let dir = temp_dir("account-handle");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let add = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "add".into(),
                method: "peer.add".into(),
                params: serde_json::json!({"id": "peer-1", "name": "Alice"}),
            },
            &store,
        );
        assert!(add.ok);
        let handle = idfon_core::account_alias("peer-1");
        let state = store.lock().unwrap();
        let peer = state.peers.iter().find(|peer| peer.id == "peer-1").unwrap();
        assert!(peer.aliases.contains(&handle));
        // The handle resolves like any other ref, without being carried.
        assert_eq!(resolve_peer_id(&state, "default", &handle), "peer-1");
        drop(state);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn load_backfills_the_derived_account_handle() {
        let dir = temp_dir("account-handle-backfill");
        {
            let mut state = Store::load(&dir).unwrap();
            state.peers.push(idfon_protocol::Peer {
                id: "peer-1".into(),
                identity: "default".into(),
                name: "Alice".into(),
                endpoint_id: None,
                endpoint_addr: None,
                devices: Vec::new(),
                aliases: Vec::new(),
                ring_score: None,
                call_mode: None,
                pinned: false,
                instant_call: false,
                tags: Vec::new(),
                visibility: None,
                introduced_by: None,
                share: None,
            });
            let data_dir = state.data_dir.clone();
            state.save(&data_dir).unwrap();
        }
        let state = Store::load(&dir).unwrap();
        let peer = state.peers.iter().find(|peer| peer.id == "peer-1").unwrap();
        assert!(peer.aliases.contains(&idfon_core::account_alias("peer-1")));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn peer_device_operations_are_incremental_and_persistent() {
        let dir = temp_dir("peer-devices");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let add = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "peer".into(),
                method: "peer.add".into(),
                params: serde_json::json!({
                    "id": "alice-account",
                    "name": "Alice",
                    "identity": "default",
                    "endpoint_id": "alice-mac",
                    "endpoint_addr": "{\"id\":\"alice-mac\"}"
                }),
            },
            &store,
        );
        assert!(add.ok);

        let device = |id: &str, address: &str| Request {
            version: PROTOCOL_VERSION,
            id: id.into(),
            method: "peer.device.add".into(),
            params: serde_json::json!({
                "ref": "alice-account",
                "endpoint_id": id,
                "endpoint_addr": address,
                "label": "Alice phone",
                "device_class": "mobile",
                "capabilities": ["chat", "callkit"]
            }),
        };
        assert!(dispatch(device("alice-iphone", "{\"id\":\"alice-iphone\"}"), &store).ok);
        let duplicate = dispatch(device("alice-iphone", "{}"), &store);
        assert!(!duplicate.ok);

        let update = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "update".into(),
                method: "peer.device.update".into(),
                params: serde_json::json!({
                    "ref": "alice-account",
                    "endpoint_id": "alice-iphone",
                    "endpoint_addr": "{\"id\":\"alice-iphone\",\"relay\":true}",
                    "label": "Alice iPhone",
                    "device_class": "mobile",
                    "capabilities": ["chat", "callkit", "push"]
                }),
            },
            &store,
        );
        assert!(update.ok);
        {
            let state = store.lock().unwrap();
            let peer = &state.peers[0];
            assert_eq!(peer.devices.len(), 1);
            assert_eq!(peer.devices[0].label.as_deref(), Some("Alice iPhone"));
            assert_eq!(peer.devices[0].capabilities, ["chat", "callkit", "push"]);
        }

        let remove = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "remove".into(),
                method: "peer.device.remove".into(),
                params: serde_json::json!({
                    "ref": "alice-account",
                    "endpoint_id": "alice-iphone"
                }),
            },
            &store,
        );
        assert!(remove.ok);
        assert!(store.lock().unwrap().peers[0].devices.is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn peer_ring_score_defaults_persists_updates_and_rejects_invalid() {
        let dir = temp_dir("ring-score");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let send = |id: &str, method: &str, params: serde_json::Value| {
            dispatch(
                Request {
                    version: PROTOCOL_VERSION,
                    id: id.into(),
                    method: method.into(),
                    params,
                },
                &store,
            )
        };
        let stored = |id: &str| {
            Store::load(&dir).unwrap().peers.iter().find(|peer| peer.id == id).unwrap().ring_score
        };

        // absent -> unset (the shell's per-kind default)
        assert!(send("add-1", "peer.add", serde_json::json!({"id": "peer-1", "name": "Alice"})).ok);
        assert_eq!(stored("peer-1"), None);

        // explicit score is persisted and surfaced by `peers`
        let added = send(
            "add-2",
            "peer.add",
            serde_json::json!({"id": "peer-2", "name": "Bob", "ring_score": 90}),
        );
        assert!(added.ok);
        let ResponseBody::Success { result, .. } = &added.body else {
            panic!("peer.add failed")
        };
        assert_eq!(result["peer"]["ring_score"], serde_json::json!(90));
        assert!(result["peer"].get("call_mode").is_none(), "the legacy field is never written");
        assert_eq!(stored("peer-2"), Some(90));

        // update changes it; an update without it leaves it alone
        assert!(send("upd-1", "peer.update", serde_json::json!({"ref": "peer-2", "ring_score": 20})).ok);
        assert_eq!(stored("peer-2"), Some(20));
        assert!(send("upd-2", "peer.update", serde_json::json!({"ref": "peer-2", "name": "Bobby"})).ok);
        assert_eq!(stored("peer-2"), Some(20));

        // "auto" clears it back to the per-kind default
        assert!(send("upd-auto", "peer.update", serde_json::json!({"ref": "peer-2", "ring_score": "auto"})).ok);
        assert_eq!(stored("peer-2"), None);

        // the legacy call_mode param still works, mapped to a score
        assert!(send("upd-legacy", "peer.update", serde_json::json!({"ref": "peer-2", "call_mode": "call_kit"})).ok);
        assert_eq!(stored("peer-2"), Some(idfon_protocol::ring::DEFAULT_PERSON));

        // invalid values are rejected, not defaulted
        for response in [
            send("bad-1", "peer.add", serde_json::json!({"id": "peer-3", "name": "Carol", "ring_score": 101})),
            send("bad-2", "peer.update", serde_json::json!({"ref": "peer-2", "ring_score": "loud"})),
            send("bad-3", "peer.update", serde_json::json!({"ref": "peer-2", "ring_score": -1})),
            send("bad-4", "peer.update", serde_json::json!({"ref": "peer-2", "call_mode": "telephone"})),
        ] {
            assert!(matches!(
                response.body,
                ResponseBody::Failure { error: ApiError { code: ErrorCode::InvalidRequest, .. }, .. }
            ));
        }
    }

    #[test]
    fn legacy_call_modes_fold_into_ring_scores_once() {
        // A store written after #217 (implicit `bar` already cleared).
        let store = legacy_store_with_call_modes("ring-score-migrate", true);
        let score = |id: &str| store.peers.iter().find(|p| p.id == id).unwrap().ring_score;
        assert_eq!(score("p-bar"), Some(idfon_protocol::ring::DEFAULT_AGENT));
        assert_eq!(score("p-kit"), Some(idfon_protocol::ring::DEFAULT_PERSON));
        assert_eq!(score("p-none"), None);
        assert!(store.peers.iter().all(|p| p.call_mode.is_none()), "legacy field dropped");
        assert!(store.ring_score_v1);
    }

    #[test]
    fn pre_217_implicit_bar_becomes_unset_not_the_agent_score() {
        // Every pre-#217 peer carried an implicit `bar`: it was never a choice.
        let store = legacy_store_with_call_modes("ring-score-pre217", false);
        let score = |id: &str| store.peers.iter().find(|p| p.id == id).unwrap().ring_score;
        assert_eq!(score("p-bar"), None);
        assert_eq!(score("p-kit"), Some(idfon_protocol::ring::DEFAULT_PERSON));
    }

    fn legacy_store_with_call_modes(name: &str, auto_v1: bool) -> Store {
        use idfon_protocol::IncomingCallMode;
        let dir = temp_dir(name);
        {
            let mut store = Store::load(&dir).unwrap();
            for (id, mode) in [
                ("p-bar", Some(IncomingCallMode::Bar)),
                ("p-kit", Some(IncomingCallMode::CallKit)),
                ("p-none", None),
            ] {
                store.peers.push(idfon_protocol::Peer {
                    id: id.into(),
                    identity: "default".into(),
                    name: id.into(),
                    endpoint_id: None,
                    endpoint_addr: None,
                    devices: Vec::new(),
                    aliases: Vec::new(),
                    ring_score: None,
                    call_mode: mode,
                    pinned: false,
                    instant_call: false,
                    tags: Vec::new(),
                    visibility: None,
                    introduced_by: None,
                    share: None,
                });
            }
            store.save(&dir).unwrap();
        }
        // `call_mode` is never serialized, so write the legacy JSON by hand:
        // a store from before scores, with implicit `bar` already cleared.
        let snapshot = {
            let store = Store::load(&dir).unwrap();
            let mut json = serde_json::to_value(&store).unwrap();
            for peer in json["peers"].as_array_mut().unwrap() {
                match peer["id"].as_str().unwrap() {
                    "p-bar" => peer["call_mode"] = "bar".into(),
                    "p-kit" => peer["call_mode"] = "call_kit".into(),
                    _ => {}
                }
            }
            json["ring_score_v1"] = false.into();
            json["call_mode_auto_v1"] = auto_v1.into();
            json.to_string()
        };
        Store::db(&dir)
            .unwrap()
            .execute("UPDATE state SET json = ?1 WHERE id = 1", rusqlite::params![snapshot])
            .unwrap();
        Store::load(&dir).unwrap()
    }

    #[test]
    fn sending_to_own_identity_is_rejected() {
        let dir = temp_dir("self-send");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let self_id = {
            let mut state = store.lock().unwrap();
            state.identities[0].endpoint_id = Some("self-endpoint".into());
            let identity = state.identities[0].clone();
            state.peers.push(idfon_protocol::Peer {
                id: identity.public_key.clone().unwrap(),
                identity: identity.id,
                name: "Self".into(),
                endpoint_id: Some("self-endpoint".into()),
                endpoint_addr: Some("{}".into()),
                devices: Vec::new(),
                aliases: Vec::new(),
                ring_score: None,
                call_mode: None,
                pinned: false,
                instant_call: false,
                tags: Vec::new(),
                visibility: None,
                introduced_by: None,
                share: None,
            });
            identity.public_key.unwrap()
        };
        let response = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "self-send".into(),
                method: "message.send".into(),
                params: serde_json::json!({"identity": "default", "to": self_id, "text": "hello", "idempotency_key": "self-send"}),
            },
            &store,
        );
        assert!(matches!(
            response.body,
            ResponseBody::Failure {
                error: ApiError {
                    code: ErrorCode::InvalidRequest,
                    ..
                },
                ..
            }
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn live_audio_session_requires_subscribe_grant() {
        let dir = temp_dir("live-grant");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap())) as Arc<Mutex<Store>>;
        let start = |store: &Arc<Mutex<Store>>, id: &str| {
            dispatch(
                Request {
                    version: PROTOCOL_VERSION,
                    id: id.into(),
                    method: "media.session.start".into(),
                    params: serde_json::json!({"identity": "default", "peer": "peer-1", "kind": "live_audio"}),
                },
                store,
            )
        };
        // A MessageSend grant alone no longer implies live-audio permission.
        {
            let mut state = store.lock().unwrap();
            state.grants.push(idfon_protocol::CapabilityGrant {
                capability: idfon_protocol::Capability::MessageSend,
                identity: "default".into(),
                subject: "peer-1".into(),
                conversation: None,
                active_at: "0".into(),
                expires_at: None,
                revision: 1,
                revoked_at: None,
            });
        }
        let denied = start(&store, "denied");
        assert!(matches!(
            denied.body,
            ResponseBody::Failure {
                error: ApiError {
                    code: ErrorCode::CapabilityDenied,
                    ..
                },
                ..
            }
        ));
        // Pairing-time LiveAudioSubscribe grant unlocks the session.
        store
            .lock()
            .unwrap()
            .grants
            .push(idfon_protocol::CapabilityGrant {
                capability: idfon_protocol::Capability::LiveAudioSubscribe,
                identity: "default".into(),
                subject: "peer-1".into(),
                conversation: None,
                active_at: "0".into(),
                expires_at: None,
                revision: 1,
                revoked_at: None,
            });
        assert!(start(&store, "allowed").ok);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn operation_cancel_marks_queued_work_cancelled() {
        let dir = temp_dir("cancel");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        store
            .lock()
            .unwrap()
            .operations
            .push(idfon_protocol::Operation {
                identity: "default".into(),
                operation_id: "op_cancel".into(),
                method: "message.send".into(),
                status: idfon_protocol::OperationStatus::Queued,
                target: None,
                request_fingerprint: None,
                created_at: now(),
                updated_at: now(),
                idempotency_key: Some("key".into()),
                message_id: Some("msg".into()),
                outbound: None,
                delivery: None,
                durable: false,
            });
        let response = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "test".into(),
                method: "operation.cancel".into(),
                params: serde_json::json!({"operation_id": "op_cancel"}),
            },
            &store,
        );
        assert!(response.ok);
        assert_eq!(
            store.lock().unwrap().operations[0].status,
            idfon_protocol::OperationStatus::Cancelled
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn event_queries_filter_and_reject_old_cursors() {
        let dir = temp_dir("events");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        {
            let mut state = store.lock().unwrap();
            state.events = vec![idfon_protocol::Event {
                event_id: "evt-2".into(),
                cursor: "cur_00000000000000000002".into(),
                r#type: "message.received".into(),
                timestamp: now(),
                identity: "default".into(),
                data: serde_json::json!({"peer_id": "alice"}),
            }];
        }
        let filtered = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "test".into(),
                method: "events".into(),
                params: serde_json::json!({"after": "cur_00000000000000000002", "type": "message.received", "peer": "alice"}),
            },
            &store,
        );
        assert!(filtered.ok);
        let old = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "test".into(),
                method: "events".into(),
                params: serde_json::json!({"after": "cur_00000000000000000001"}),
            },
            &store,
        );
        assert!(matches!(
            old.body,
            ResponseBody::Failure {
                error: ApiError {
                    code: ErrorCode::CursorTooOld,
                    ..
                },
                ..
            }
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn events_merge_applies_peer_snapshot_and_deduplicates() {
        let dir = temp_dir("events-merge");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let peer = idfon_protocol::Peer {
            id: "alice".into(),
            identity: "default".into(),
            name: "Alice".into(),
            endpoint_id: Some("alice-phone".into()),
            endpoint_addr: Some("{\"id\":\"alice-phone\"}".into()),
            devices: Vec::new(),
            aliases: Vec::new(),
            ring_score: None,
            call_mode: None,
            pinned: false,
            instant_call: false,
            tags: Vec::new(),
            visibility: None,
            introduced_by: None,
            share: None,
        };
        let event = idfon_protocol::Event {
            event_id: "peer-event-1".into(),
            cursor: "remote-1".into(),
            r#type: "peer.created".into(),
            timestamp: "1".into(),
            identity: "default".into(),
            data: serde_json::json!({"peer": peer}),
        };
        let key = idfon_core::IdentityKey::generate();
        let envelope = idfon_core::sign_state_sync(
            &key,
            key.peer_id(),
            "batch-1".into(),
            vec![event],
        );
        let request = || Request {
            version: PROTOCOL_VERSION,
            id: "merge".into(),
            method: "events.merge".into(),
            params: serde_json::json!({"envelope": envelope}),
        };
        assert!(dispatch(request(), &store).ok);
        assert!(dispatch(request(), &store).ok);
        assert_eq!(store.lock().unwrap().peers.len(), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// `events` must answer in cursor order (oldest first): `wait` truncates
    /// to one event and the shells advance their cursor per delivered event,
    /// so a newest-first page stranded every older event in the same burst
    /// (#51 E2E: a ring invite lost behind a newer message — "it's not
    /// ringing"; and a hangup stop lost behind its own invite).
    #[test]
    fn events_page_is_oldest_first_so_wait_truncation_cannot_strand() {
        let dir = temp_dir("events-order");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        {
            let mut state = store.lock().unwrap();
            for index in 0..3 {
                state.events.push(idfon_protocol::Event {
                    event_id: format!("evt-{index}"),
                    cursor: format!("cur_{index:020}"),
                    r#type: "message.received".into(),
                    timestamp: now(),
                    identity: "default".into(),
                    data: serde_json::json!({
                        "text": format!("m{index}"),
                        "peer_id": "p",
                        "message_id": format!("m{index}")
                    }),
                });
            }
        }
        let request = Request {
            version: PROTOCOL_VERSION,
            id: "order".into(),
            method: "events".into(),
            params: serde_json::json!({}),
        };
        let response = events(&request, &store);
        let ResponseBody::Success { result, .. } = response.body else {
            panic!("events failed");
        };
        let cursors: Vec<String> = result["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["cursor"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            cursors,
            vec![
                format!("cur_{:020}", 0),
                format!("cur_{:020}", 1),
                format!("cur_{:020}", 2)
            ]
        );
        // The `wait` long-poll truncates to one: it must keep the oldest
        // pending event, never the newest.
        let response = wait_event(&request, &store);
        let ResponseBody::Success { result, .. } = response.body else {
            panic!("wait failed");
        };
        let list = result["events"].as_array().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0]["cursor"].as_str().unwrap(), format!("cur_{:020}", 0));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn compact_events_pages_large_histories() {
        fn read_u16_field(payload: &[u8], offset: &mut usize) -> Vec<u8> {
            let length = payload[*offset] as usize * 256 + payload[*offset + 1] as usize;
            *offset += 2;
            let value = payload[*offset..*offset + length].to_vec();
            *offset += length;
            value
        }
        let dir = temp_dir("compact-pages");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        {
            let mut state = store.lock().unwrap();
            for index in 0..20 {
                state.events.push(idfon_protocol::Event {
                    event_id: format!("evt-{index}"),
                    cursor: format!("cur_{index:020}"),
                    r#type: "message.received".into(),
                    timestamp: now(),
                    identity: "default".into(),
                    data: serde_json::json!({"text": "x".repeat(16 * 1024)}),
                });
            }
        }
        let mut after: Option<String> = None;
        let mut seen: Vec<String> = Vec::new();
        loop {
            let payload = compact_events(&store, after.as_deref(), None);
            assert!(payload.len() >= 2);
            assert!(payload.len() <= COMPACT_EVENTS_MAX_BYTES);
            let count = u16::from_be_bytes([payload[0], payload[1]]) as usize;
            let mut offset = 2;
            let mut cursors: Vec<String> = Vec::new();
            for _ in 0..count {
                cursors.push(String::from_utf8(read_u16_field(&payload, &mut offset)).unwrap());
                let _kind = read_u16_field(&payload, &mut offset);
                let _peer = read_u16_field(&payload, &mut offset);
                let _message = read_u16_field(&payload, &mut offset);
                let _text = read_u16_field(&payload, &mut offset);
                let _timestamp = read_u16_field(&payload, &mut offset);
            }
            assert_eq!(offset, payload.len());
            if count == 0 {
                break;
            }
            assert!(
                seen.last().is_none_or(|last| cursors[0] > *last),
                "pages overlap or regress"
            );
            seen.extend(cursors);
            after = Some(seen.last().unwrap().clone());
            assert!(seen.len() <= 20, "paging did not terminate");
        }
        assert_eq!(seen.len(), 20);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn media_resource_put_publishes_a_ticket() {
        let dir = temp_dir("resource");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let put = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "put".into(),
                method: "media.resource.put".into(),
                params: serde_json::json!({"resource_id": "res-1", "bytes": [1, 2, 3]}),
            },
            &store,
        );
        match put.body {
            ResponseBody::Success { result, .. } => {
                assert_eq!(result["size_bytes"], 3);
                assert!(result["blob_ticket"]
                    .as_str()
                    .is_some_and(|ticket| ticket.starts_with("blob")));
            }
            _ => panic!("resource put failed"),
        }
        // The ticket is the only way to the bytes; a bare resource_id is refused.
        let fetch = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "fetch".into(),
                method: "media.resource.fetch".into(),
                params: serde_json::json!({"resource_id": "res-1"}),
            },
            &store,
        );
        assert!(matches!(
            fetch.body,
            ResponseBody::Failure {
                error: ApiError {
                    code: ErrorCode::InvalidRequest,
                    ..
                },
                ..
            }
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn chunked_put_finishes_with_a_ticket() {
        let dir = temp_dir("chunked");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        for chunk in [[1u8, 2, 3], [4, 5, 6]] {
            let append = dispatch(
                Request {
                    version: PROTOCOL_VERSION,
                    id: "append".into(),
                    method: "media.resource.put".into(),
                    params: serde_json::json!({"resource_id": "res-c", "bytes": chunk, "append": true}),
                },
                &store,
            );
            assert!(append.ok, "append failed: {append:?}");
        }
        let finish = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "finish".into(),
                method: "media.resource.put".into(),
                params: serde_json::json!({"resource_id": "res-c", "finish": true}),
            },
            &store,
        );
        assert!(finish.ok, "finish failed: {finish:?}");
        // The finish step published a blob for the concatenated resource.
        match &finish.body {
            ResponseBody::Success { result, .. } => {
                assert_eq!(result["size_bytes"], 6);
                assert_eq!(
                    result["content_hash"],
                    blake3::hash(&[1u8, 2, 3, 4, 5, 6]).to_hex().to_string()
                );
                assert!(result["blob_ticket"]
                    .as_str()
                    .is_some_and(|ticket| ticket.starts_with("blob")));
            }
            _ => panic!("finish did not succeed"),
        }
        // The staging accumulator is transient: consumed by the finish above.
        assert!(!dir.join("staging/default/res-c").exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn idle_exit_shuts_down_daemon_with_no_clients() {
        let dir = temp_dir("idle-exit");
        let socket = dir.join("idfond.sock");
        let handle = tokio::spawn(run(DaemonConfig {
            socket: socket.clone(),
            data_dir: dir.clone(),
            transport: Some("fake".into()),
            idle_exit: Some(Duration::from_millis(300)),
        }));
        let result = tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_ok(), "daemon run failed: {result:?}");
        assert!(!socket.exists(), "socket not cleaned up on idle exit");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn daemon_status_says_which_build_runs_and_who_started_it() {
        let dir = temp_dir("status-build");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        build_info::record_start();
        let response = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "status".into(),
                method: "daemon.status".into(),
                params: serde_json::json!({}),
            },
            &store,
        );
        let ResponseBody::Success { result, .. } = response.body else {
            panic!("daemon.status failed");
        };
        assert_eq!(result["daemon"], "idfond");
        assert!(result["host"].as_str().is_some_and(|host| !host.is_empty()), "{result}");
        assert_eq!(result["autostarted"], false);
        assert!(result["clients"].is_u64());
        assert_eq!(result["build"]["stale"], false);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_identity_predating_the_split_keeps_one_key() {
        let dir = temp_dir("endpoint-key");
        let state = Store::load(&dir).unwrap();
        assert_eq!(
            state.identity_key("default").unwrap().peer_id(),
            state.endpoint_key("default").unwrap().peer_id()
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn data_lock_prevents_two_daemons_from_sharing_state() {
        let dir = temp_dir("lock");
        let first = DataLock::acquire(&dir).unwrap();
        let error = DataLock::acquire(&dir).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
        drop(first);
        let second = DataLock::acquire(&dir).unwrap();
        drop(second);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn data_lock_ignores_a_stale_lock_file() {
        let dir = temp_dir("lock-stale");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("state.lock"), "999999999\n").unwrap();
        let lock = DataLock::acquire(&dir).unwrap();
        drop(lock);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn embedded_sqlite_snapshot_survives_json_removal() {
        let dir = temp_dir("sqlite-snapshot");
        let store = Store::load(&dir).unwrap();
        store.save(&dir).unwrap();
        assert!(dir.join("state.db").is_file());
        std::fs::remove_file(dir.join("state.json")).unwrap();
        let restored = Store::load(&dir).unwrap();
        assert_eq!(restored.identities.len(), 1); // one you (identities are tags)
        assert_eq!(restored.identities[0].id, "default");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn first_load_creates_default_identity_and_persists_it() {
        let dir = temp_dir("identity");
        let store = Store::load(&dir).unwrap();
        assert_eq!(store.identities.len(), 1); // one you (identities are tags)
        assert_eq!(store.identities[0].id, "default");
        assert!(store.identities[0].public_key.is_some());
        assert!(dir.join("state.json").is_file());
        assert!(dir.join("identity-default.key").is_file());

        let reloaded = Store::load(&dir).unwrap();
        assert_eq!(reloaded.identities, store.identities);
        assert!(reloaded.peers.is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn peer_resolution_matches_name_alias_and_endpoint() {
        let dir = temp_dir("resolve");
        let mut store = Store::load(&dir).unwrap();
        store.peers.push(idfon_protocol::Peer {
            id: "peer-1".into(),
            identity: "default".into(),
            name: "Alice".into(),
            endpoint_id: Some("ep-1".into()),
            endpoint_addr: None,
            devices: Vec::new(),
            aliases: vec!["alice@work".into()],
            ring_score: None,
            call_mode: None,
            pinned: false,
            instant_call: false,
            tags: Vec::new(),
            visibility: None,
            introduced_by: None,
            share: None,
        });
        store.save(&dir).unwrap();
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));

        for reference in ["peer-1", "Alice", "alice@work", "ep-1"] {
            let response = dispatch(
                Request {
                    version: PROTOCOL_VERSION,
                    id: "test".into(),
                    method: "peer.resolve".into(),
                    params: serde_json::json!({"ref": reference}),
                },
                &store,
            );
            assert!(response.ok, "resolution failed for {reference}");
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn peer_resolution_reports_ambiguity() {
        let dir = temp_dir("ambiguous");
        let mut store = Store::load(&dir).unwrap();
        for id in ["peer-1", "peer-2"] {
            store.peers.push(idfon_protocol::Peer {
                id: id.into(),
                identity: "default".into(),
                name: "same".into(),
                endpoint_id: None,
                endpoint_addr: None,
                devices: Vec::new(),
                aliases: Vec::new(),
                ring_score: None,
                call_mode: None,
                pinned: false,
                instant_call: false,
                tags: Vec::new(),
                visibility: None,
                introduced_by: None,
                share: None,
            });
        }
        store.save(&dir).unwrap();
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let response = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "test".into(),
                method: "peer.resolve".into(),
                params: serde_json::json!({"ref": "same"}),
            },
            &store,
        );
        assert!(!response.ok);
        assert!(matches!(
            response.body,
            ResponseBody::Failure {
                error: ApiError {
                    code: ErrorCode::AmbiguousPeer,
                    ..
                },
                ..
            }
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn room_lifecycle_persists_and_retries_dedupe_per_member() {
        let dir = temp_dir("room-lifecycle");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        {
            let mut state = store.lock().unwrap();
            state.peers.push(idfon_protocol::Peer {
                id: "peer-b".into(),
                identity: "default".into(),
                name: "Bob".into(),
                endpoint_id: Some("endpoint-b".into()),
                endpoint_addr: Some("{}".into()),
                devices: Vec::new(),
                aliases: Vec::new(),
                ring_score: None,
                call_mode: None,
                pinned: false,
                instant_call: false,
                tags: Vec::new(),
                visibility: None,
                introduced_by: None,
                share: None,
            });
            state.grants.push(idfon_protocol::CapabilityGrant {
                capability: idfon_protocol::Capability::MessageSend,
                identity: "default".into(),
                subject: "peer-b".into(),
                conversation: None,
                active_at: "0".into(),
                expires_at: None,
                revision: 1,
                revoked_at: None,
            });
        }
        let create = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "create".into(),
                method: "room.create".into(),
                params: serde_json::json!({"id":"r_test","name":"design","members":["Bob"]}),
            },
            &store,
        );
        let result = |response: Response| match response.body {
            ResponseBody::Success { result, .. } => result,
            ResponseBody::Failure { error, .. } => panic!("request failed: {}", error.message),
        };
        assert!(create.ok);
        assert_eq!(result(create)["room"]["members"][0], "peer-b");

        let reloaded = Store::load(&dir).unwrap();
        assert_eq!(reloaded.rooms[0].id, "r_test");
        assert_eq!(reloaded.rooms[0].members, ["peer-b"]);

        let send = |id: &str| {
            dispatch(
                Request {
                    version: PROTOCOL_VERSION,
                    id: id.into(),
                    method: "room.send".into(),
                    params: serde_json::json!({"room":"design","text":"hello","idempotency_key":"room-key"}),
                },
                &store,
            )
        };
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (first, second) = runtime.block_on(async { (send("send-1"), send("send-2")) });
        assert!(first.ok && second.ok);
        assert_eq!(
            result(first)["operation_ids"],
            result(second)["operation_ids"]
        );
        drop(runtime);

        let leave = dispatch(
            Request {
                version: PROTOCOL_VERSION,
                id: "leave".into(),
                method: "room.leave".into(),
                params: serde_json::json!({"room":"design"}),
            },
            &store,
        );
        assert_eq!(result(leave)["removed"], true);
        assert!(Store::load(&dir).unwrap().rooms.is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The outbox: a durable send to an unreachable peer is "not sent yet"
    /// (Queued), not Failed; `operation.resume` delivers it once the peer is
    /// reachable, exactly once. A plain send still fails as before.
    #[test]
    fn durable_sends_wait_in_the_outbox_until_the_peer_is_reachable() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _entered = runtime.enter();
        let settle = || runtime.block_on(tokio::time::sleep(std::time::Duration::from_millis(600)));
        let dir = temp_dir("outbox");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let fake = FakeTransport::default();
        let transport = Arc::new(TransportMode::Fake(fake.clone()));
        // Bob: a real endpoint id, but no address yet — every dial fails.
        let bob_dir = temp_dir("outbox-bob");
        let (bob, bob_endpoint) = {
            let bob_store = Store::load(&bob_dir).unwrap();
            let endpoint_key = bob_store.endpoint_key("default").unwrap();
            let endpoint = iroh::SecretKey::from_bytes(&endpoint_key.to_bytes()).public();
            (bob_store.identity_key("default").unwrap().peer_id(), endpoint)
        };
        {
            let mut state = store.lock().unwrap();
            state.peers.push(idfon_protocol::Peer {
                id: bob.clone(),
                identity: "default".into(),
                name: "bob".into(),
                endpoint_id: None,
                endpoint_addr: None,
                devices: Vec::new(),
                aliases: Vec::new(),
                ring_score: None,
                call_mode: None,
                pinned: false,
                instant_call: false,
                tags: Vec::new(),
                visibility: None,
                introduced_by: None,
                share: None,
            });
            state.grants.push(idfon_protocol::CapabilityGrant {
                capability: idfon_protocol::Capability::MessageSend,
                identity: "default".into(),
                subject: bob.clone(),
                conversation: None,
                active_at: "0".into(),
                expires_at: None,
                revision: 1,
                revoked_at: None,
            });
        }
        let rpc = |method: &str, params: serde_json::Value| -> serde_json::Value {
            let response = dispatch_with_transport(
                Request { version: PROTOCOL_VERSION, id: method.into(), method: method.into(), params },
                &store,
                Some(&transport),
            );
            match response.body {
                ResponseBody::Success { result, .. } => result,
                ResponseBody::Failure { error, .. } => panic!("{method} failed: {}", error.message),
            }
        };
        let status = |operation_id: &str| -> idfon_protocol::OperationStatus {
            let state = store.lock().unwrap();
            state.operations.iter().find(|op| op.operation_id == operation_id).unwrap().status.clone()
        };

        let waiting = rpc("message.send", serde_json::json!({
            "to": bob, "text": "see you at 6", "idempotency_key": "k-durable", "durable": true, "retries": 0}));
        let plain = rpc("message.send", serde_json::json!({
            "to": bob, "text": "ping", "idempotency_key": "k-plain", "retries": 0}));
        let waiting_id = waiting["operation_id"].as_str().unwrap().to_owned();
        let plain_id = plain["operation_id"].as_str().unwrap().to_owned();
        settle();
        assert_eq!(status(&waiting_id), idfon_protocol::OperationStatus::Queued, "durable: not sent yet");
        assert_eq!(status(&plain_id), idfon_protocol::OperationStatus::Failed, "plain: fails as before");
        assert!(fake.messages().is_empty());
        // Still unreachable: a resume tries and puts it back in the outbox.
        assert_eq!(rpc("operation.resume", serde_json::json!({"target": "bob"}))["resumed"], 1);
        runtime.block_on(tokio::time::sleep(std::time::Duration::from_millis(1200)));
        assert_eq!(status(&waiting_id), idfon_protocol::OperationStatus::Queued);

        // Bob becomes reachable; two overlapping triggers deliver it once.
        {
            let mut state = store.lock().unwrap();
            state.peers[0].endpoint_id = Some(bob_endpoint.to_string());
            state.peers[0].endpoint_addr =
                Some(serde_json::to_string(&EndpointAddr::new(bob_endpoint)).unwrap());
        }
        assert_eq!(rpc("operation.resume", serde_json::json!({})), serde_json::json!({"resumed": 1}));
        assert_eq!(rpc("operation.resume", serde_json::json!({})), serde_json::json!({"resumed": 0}), "claimed once");
        settle();
        assert_eq!(status(&waiting_id), idfon_protocol::OperationStatus::Delivered);
        assert_eq!(status(&plain_id), idfon_protocol::OperationStatus::Failed, "a failed plain send is not resumed");
        let sent = fake.messages();
        assert_eq!(sent.len(), 1);
        assert!(matches!(&sent[0].content, idfon_protocol::MessageContent::Text { text } if text == "see you at 6"));
        // The status changes are events the app can follow, with the message id.
        let state = store.lock().unwrap();
        assert!(state.events.iter().any(|event| event.r#type == "operation.state_changed"
            && event.data["operation_id"] == waiting_id.as_str()
            && event.data["status"] == "delivered"
            && event.data["message_id"].is_string()));
        drop(state);
        std::fs::remove_dir_all(dir).unwrap();
        std::fs::remove_dir_all(bob_dir).unwrap();
    }

    /// A queued send that is cancelled (deleted before it was sent) is never
    /// resumed.
    #[test]
    fn cancelled_outbox_items_are_not_resumed() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _entered = runtime.enter();
        let dir = temp_dir("outbox-cancel");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let transport = Arc::new(TransportMode::Fake(FakeTransport::default()));
        let key = idfon_core::IdentityKey::generate();
        let envelope = idfon_core::sign_message(
            &key, "endpoint-a", "msg-1",
            idfon_protocol::MessageContent::Text { text: "draft".into() }, "k-1", None,
        ).unwrap();
        store.lock().unwrap().operations.push(idfon_protocol::Operation {
            identity: "default".into(),
            operation_id: "op-1".into(),
            method: "message.send".into(),
            status: idfon_protocol::OperationStatus::Cancelled,
            target: Some("bob".into()),
            request_fingerprint: None,
            created_at: now(),
            updated_at: now(),
            idempotency_key: Some("k-1".into()),
            message_id: Some("msg-1".into()),
            outbound: Some(envelope),
            delivery: None,
            durable: true,
        });
        assert_eq!(resume_queued(&store, &transport, None, true, 1), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// §5.4: a screen capture leaves only for a peer holding `screen.read`
    /// (ungranted capability envelopes are refused by the daemon itself).
    #[test]
    fn screen_captures_need_the_screen_read_grant() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _entered = runtime.enter();
        let dir = temp_dir("screen-grant");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        {
            let mut state = store.lock().unwrap();
            let mut bob = test_peer("bob", "default", "Bob");
            bob.endpoint_id = Some("bob-endpoint".into());
            state.peers.push(bob);
            state.grants.push(idfon_protocol::CapabilityGrant {
                capability: idfon_protocol::Capability::MessageSend,
                identity: "default".into(),
                subject: "bob".into(),
                conversation: None,
                active_at: "0".into(),
                expires_at: None,
                revision: 1,
                revoked_at: None,
            });
        }
        let transport = Arc::new(TransportMode::Fake(FakeTransport::default()));
        let rpc = |method: &str, params: serde_json::Value| -> Response {
            dispatch_with_transport(
                Request { version: PROTOCOL_VERSION, id: method.into(), method: method.into(), params },
                &store,
                Some(&transport),
            )
        };
        let capture = "IDFON-FILE/1\nname=screenshot.png\nticket=t\nscreen=1";
        let send = |text: &str, key: &str| rpc("message.send", serde_json::json!({"to": "bob", "text": text, "idempotency_key": key, "retries": 0}));
        let denied = send(capture, "k1");
        assert!(!denied.ok, "no screen.read grant: refused");
        assert!(format!("{denied:?}").contains("screen.read"));
        assert!(send("IDFON-FILE/1\nname=photo.png\nticket=t", "k2").ok, "an ordinary file needs no screen grant");
        assert!(rpc("access.grant", serde_json::json!({"identity": "default", "subject": "bob", "capability": "screen.read"})).ok);
        assert!(send(capture, "k3").ok, "granted: it goes");
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// `room.update` replaces the given fields; members resolve like
    /// `room.create`; an unknown room is an error.
    #[test]
    fn room_update_changes_name_and_members() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _entered = runtime.enter();
        let dir = temp_dir("room-update");
        let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
        let transport = Arc::new(TransportMode::Fake(FakeTransport::default()));
        let rpc = |method: &str, params: serde_json::Value| -> Response {
            dispatch_with_transport(
                Request { version: PROTOCOL_VERSION, id: method.into(), method: method.into(), params },
                &store,
                Some(&transport),
            )
        };
        let created = rpc("room.create", serde_json::json!({"id": "r_test", "name": "Weekend", "kind": "group", "members": ["alice"]}));
        assert!(created.ok, "{created:?}");
        let updated = rpc("room.update", serde_json::json!({"room": "r_test", "name": "Weekend plans", "members": ["alice", "bob"], "repo": "github.com/x/y"}));
        let ResponseBody::Success { result, .. } = updated.body else { panic!("update failed") };
        assert_eq!(result["room"]["name"], "Weekend plans");
        assert_eq!(result["room"]["members"], serde_json::json!(["alice", "bob"]));
        assert_eq!(result["room"]["repo"], "github.com/x/y");
        assert_eq!(result["room"]["kind"], "group", "untouched fields stay");
        let state = store.lock().unwrap();
        assert_eq!(state.rooms[0].members, vec!["alice".to_string(), "bob".to_string()]);
        assert!(state.events.iter().any(|event| event.r#type == "room.updated"));
        drop(state);
        assert!(!rpc("room.update", serde_json::json!({"room": "r_missing", "name": "x"})).ok);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// G1: group-call signaling across daemons. Each store's sends are routed
    /// to the target store as `message.receive`, and the receiver's replies
    /// drain from its outbox, as the live transport would send them.
    #[test]
    fn room_call_signaling_across_daemons() {
        // Sends deliver on spawned tasks.
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _entered = runtime.enter();
        struct Node {
            store: Arc<Mutex<Store>>,
            fake: FakeTransport,
            transport: Arc<TransportMode>,
            peer_id: String,
            endpoint: String,
            dir: PathBuf,
        }
        let node = |name: &str| {
            let dir = temp_dir(name);
            let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
            let (peer_id, endpoint) = {
                let state = store.lock().unwrap();
                let peer_id = state.identity_key("default").unwrap().peer_id();
                let endpoint_key = state.endpoint_key("default").unwrap();
                let endpoint = iroh::SecretKey::from_bytes(&endpoint_key.to_bytes()).public();
                (peer_id, endpoint.to_string())
            };
            let fake = FakeTransport::default();
            let transport = Arc::new(TransportMode::Fake(fake.clone()));
            Node { store, fake, transport, peer_id, endpoint, dir }
        };
        let nodes: Vec<Node> = ["call-a", "call-b", "call-c", "call-d"].iter().map(|n| node(n)).collect();
        // Everyone knows everyone; a, b and c share room r_call; d is outside it.
        for me in &nodes {
            let mut state = me.store.lock().unwrap();
            for other in nodes.iter().filter(|o| o.peer_id != me.peer_id) {
                let endpoint: iroh::EndpointId = other.endpoint.parse().unwrap();
                state.peers.push(idfon_protocol::Peer {
                    id: other.peer_id.clone(),
                    identity: "default".into(),
                    name: other.peer_id[..8].to_string(),
                    endpoint_id: Some(other.endpoint.clone()),
                    endpoint_addr: Some(serde_json::to_string(&EndpointAddr::new(endpoint)).unwrap()),
                    devices: Vec::new(),
                    aliases: Vec::new(),
                    ring_score: None,
                    call_mode: None,
                    pinned: false,
                    instant_call: false,
                    tags: Vec::new(),
                    visibility: None,
                    introduced_by: None,
                    share: None,
                });
                for capability in [idfon_protocol::Capability::MessageSend, idfon_protocol::Capability::MessageReceive] {
                    state.grants.push(idfon_protocol::CapabilityGrant {
                        capability,
                        identity: "default".into(),
                        subject: other.peer_id.clone(),
                        conversation: None,
                        active_at: "0".into(),
                        expires_at: None,
                        revision: 1,
                        revoked_at: None,
                    });
                }
            }
        }
        let rpc = |node: &Node, method: &str, params: serde_json::Value| -> serde_json::Value {
            let response = dispatch_with_transport(
                Request { version: PROTOCOL_VERSION, id: method.into(), method: method.into(), params },
                &node.store,
                Some(&node.transport),
            );
            match response.body {
                ResponseBody::Success { result, .. } => result,
                ResponseBody::Failure { error, .. } => panic!("{method} failed: {}", error.message),
            }
        };
        let room_members = |me: usize, members: &[usize]| {
            let ids: Vec<String> = members.iter().filter(|m| **m != me).map(|m| nodes[*m].peer_id.clone()).collect();
            rpc(&nodes[me], "room.create", serde_json::json!({"id": "r_call", "members": ids}));
        };
        for me in 0..3 {
            room_members(me, &[0, 1, 2]);
        }
        // d's own view of "r_call" includes a, but a's room does not include d.
        room_members(3, &[0, 3]);
        // Deliver every pending send until the network is quiet.
        let route = || {
            let mut quiet = 0;
            for _round in 0..200 {
                std::thread::sleep(std::time::Duration::from_millis(25));
                let mut moved = false;
                for from in &nodes {
                    for (target, envelope) in from.fake.take_sent_to() {
                        let to = nodes.iter().find(|n| n.endpoint == target).expect("routed to a known node");
                        moved = true;
                        let mut params = serde_json::to_value(&envelope).unwrap();
                        params["identity"] = "default".into();
                        let response = dispatch_with_transport(
                            Request { version: PROTOCOL_VERSION, id: "rx".into(), method: "message.receive".into(), params },
                            &to.store,
                            Some(&to.transport),
                        );
                        assert!(response.ok, "receive failed: {response:?}");
                        room_calls::flush(&to.store, &to.transport);
                    }
                }
                quiet = if moved { 0 } else { quiet + 1 };
                if quiet >= 8 {
                    return;
                }
            }
            panic!("signaling did not settle");
        };
        let status = |me: usize| rpc(&nodes[me], "room.call.status", serde_json::json!({"room": "r_call"}))["call"].clone();
        let members_of = |me: usize| -> Vec<String> {
            let mut peers: Vec<String> = status(me)["members"]
                .as_array()
                .map(|m| m.iter().map(|m| m["peer"].as_str().unwrap().to_string()).collect())
                .unwrap_or_default();
            peers.sort();
            peers
        };
        let ids = |list: &[usize]| -> Vec<String> {
            let mut v: Vec<String> = list.iter().map(|i| nodes[*i].peer_id.clone()).collect();
            v.sort();
            v
        };
        let join = |me: usize| {
            rpc(&nodes[me], "room.call.join", serde_json::json!({
                "room": "r_call", "ticket": format!("ilv1:{me}"), "addr": "{}", "audio_codec": "liquid"
            }))
        };

        join(0);
        route();
        join(1);
        route();
        join(2);
        route();
        let call_id = status(0)["call_id"].clone();
        assert!(call_id.is_string());
        for me in 0..3 {
            assert_eq!(status(me)["call_id"], call_id, "node {me} in one session");
            assert_eq!(status(me)["people"], 3, "node {me}");
            let others: Vec<usize> = (0..3).filter(|o| *o != me).collect();
            assert_eq!(members_of(me), ids(&others), "node {me} hears the other two");
        }
        // The app learns the roster from events.
        let events = rpc(&nodes[2], "events", serde_json::json!({"type": "room.call.updated"}));
        assert!(!events["events"].as_array().unwrap().is_empty());

        // d is not a member of a's room: its join is ignored.
        join(3);
        route();
        assert_eq!(status(0)["people"], 3, "a ignores a join from outside its room");
        assert!(!members_of(0).contains(&nodes[3].peer_id));

        // b leaves; a and c drop it.
        rpc(&nodes[1], "room.call.leave", serde_json::json!({"room": "r_call"}));
        route();
        assert_eq!(members_of(0), ids(&[2]));
        assert_eq!(members_of(2), ids(&[0]));
        // a and c leave: auto-close ends the session for everyone.
        rpc(&nodes[0], "room.call.leave", serde_json::json!({"room": "r_call"}));
        route();
        rpc(&nodes[2], "room.call.leave", serde_json::json!({"room": "r_call"}));
        route();
        for me in 0..3 {
            assert!(status(me)["call_id"].is_null(), "node {me}: session closed");
        }
        for node in nodes {
            let _ = std::fs::remove_dir_all(node.dir);
        }
    }


    /// One daemon for the introduction tests: its own store, identity key
    /// and fake transport (sends are routed by hand between stores).
    struct IntroNode {
        store: Arc<Mutex<Store>>,
        fake: FakeTransport,
        transport: Arc<TransportMode>,
        peer_id: String,
        endpoint: String,
        dir: PathBuf,
    }

    impl IntroNode {
        fn new(name: &str) -> Self {
            let dir = temp_dir(name);
            let store = Arc::new(Mutex::new(Store::load(&dir).unwrap()));
            let fake = FakeTransport::default();
            let (peer_id, endpoint) = {
                let mut state = store.lock().unwrap();
                let peer_id = state.identity_key("default").unwrap().peer_id();
                let endpoint_key = state.endpoint_key("default").unwrap();
                let endpoint = iroh::SecretKey::from_bytes(&endpoint_key.to_bytes()).public();
                state.identities[0].endpoint_id = Some(endpoint.to_string());
                state.identities[0].name = name.into();
                fake.set_endpoint_ticket("default", serde_json::to_vec(&EndpointAddr::new(endpoint)).unwrap());
                (peer_id, endpoint.to_string())
            };
            let transport = Arc::new(TransportMode::Fake(fake.clone()));
            Self { store, fake, transport, peer_id, endpoint, dir }
        }

        fn call(&self, method: &str, params: serde_json::Value) -> Response {
            dispatch_with_transport(
                Request { version: PROTOCOL_VERSION, id: method.into(), method: method.into(), params },
                &self.store,
                Some(&self.transport),
            )
        }

        fn rpc(&self, method: &str, params: serde_json::Value) -> serde_json::Value {
            match self.call(method, params).body {
                ResponseBody::Success { result, .. } => result,
                ResponseBody::Failure { error, .. } => panic!("{method} failed: {}", error.message),
            }
        }

        fn error(&self, method: &str, params: serde_json::Value) -> String {
            match self.call(method, params).body {
                ResponseBody::Success { result, .. } => panic!("{method} succeeded: {result}"),
                ResponseBody::Failure { error, .. } => error.message,
            }
        }

        /// A contact record for `other`, dialable (so its card exports).
        fn know(&self, other: &IntroNode, name: &str) {
            let endpoint: iroh::EndpointId = other.endpoint.parse().unwrap();
            let mut peer = test_peer(&other.peer_id, "default", name);
            peer.endpoint_id = Some(other.endpoint.clone());
            peer.endpoint_addr = Some(serde_json::to_string(&EndpointAddr::new(endpoint)).unwrap());
            self.store.lock().unwrap().peers.push(peer);
        }

        fn peer(&self, id: &str) -> idfon_protocol::Peer {
            self.store.lock().unwrap().peers.iter().find(|peer| peer.id == id).cloned().unwrap()
        }
    }

    impl Drop for IntroNode {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// `contact.introduce`, `contact.inspect`, `peer.add { intro }`,
    /// `peer.update { share }` and `identity.update { share }` (docs/protocol.md
    /// "Introductions").
    #[test]
    fn introduction_rpcs() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _entered = runtime.enter();
        let don = IntroNode::new("intro-don");
        let bob = IntroNode::new("intro-bob");
        let stella = IntroNode::new("intro-stella");
        let carol = IntroNode::new("intro-carol");
        don.know(&bob, "Bob");
        don.know(&stella, "Stella");
        bob.know(&don, "Don");

        // Only your own contacts, with a sane note and target.
        assert!(don.error("contact.introduce", serde_json::json!({"ref": "Nobody"})).contains("only your own contacts"));
        assert!(don.error("contact.introduce", serde_json::json!({"ref": "Stella", "note": "x".repeat(281)})).contains("280"));
        assert!(don.error("contact.introduce", serde_json::json!({"ref": "Stella", "to": "Nobody"})).contains("not a contact"));
        assert!(don.error("contact.introduce", serde_json::json!({"ref": "Stella", "to": "Stella"})).contains("themselves"));

        // Stella asked to be asked (her IDFON-SHARE/1, recorded by the app).
        assert!(don.error("peer.update", serde_json::json!({"ref": "Stella", "share": "maybe"})).contains("share must be"));
        assert_eq!(don.rpc("peer.update", serde_json::json!({"ref": "Stella", "share": "ask"}))["peer"]["share"], "ask");

        let shared = don.rpc("contact.introduce", serde_json::json!({
            "ref": "Stella", "to": "Bob", "note": "Stella does our taxes", "mutual": true}));
        assert_eq!(shared["share"], "ask", "the subject's preference");
        let intro = &shared["intro"];
        assert_eq!(intro["v"], 1);
        assert_eq!(intro["introducer"], don.peer_id.as_str());
        assert_eq!(intro["introducer_name"], "intro-don");
        assert_eq!(intro["subject"], stella.peer_id.as_str());
        assert_eq!(intro["recipient"], bob.peer_id.as_str());
        assert_eq!(intro["mutual"], true);
        assert_eq!(intro["note"], "Stella does our taxes");
        // The link is the add envelope { contact, intro }, base64url.
        let link = shared["link"].as_str().unwrap();
        assert!(link.starts_with("idfon://add/"));
        let envelope = serde_json::to_value(idfon_protocol::vcf::add_link_from_str(link).unwrap()).unwrap();
        assert_eq!(&envelope["intro"], intro);
        assert_eq!(envelope["contact"]["account_id"], stella.peer_id.as_str());
        assert_eq!(envelope["contact"]["label"], "Stella");
        assert_eq!(envelope["contact"]["share"], "ask");
        assert!(envelope["contact"].get("capabilities").is_none(), "no access travels");
        // The vCard carries the card and the same link.
        let vcard = shared["vcard"].as_str().unwrap().replace("\r\n ", "");
        assert!(vcard.contains(&format!("URL:{link}")));
        assert!(vcard.contains("X-IDFON-TICKET:"));

        // Inspect, as Bob (the recipient): valid and addressed to him.
        let seen = bob.rpc("contact.inspect", serde_json::json!({"link": link}));
        assert_eq!(seen["contact"], serde_json::json!({"peer_id": stella.peer_id, "name": "Stella", "share": "ask"}));
        assert_eq!(seen["intro"]["valid"], true);
        assert_eq!(seen["intro"]["addressed_to_me"], true);
        assert_eq!(seen["intro"]["introducer_is_contact"], true);
        assert_eq!(seen["intro"]["introducer_name"], "intro-don");
        assert_eq!(seen["intro"]["note"], "Stella does our taxes");
        assert_eq!(seen["intro"]["mutual"], true);
        // The universal-link form reads the same.
        let universal = link.replace("idfon://add/", "https://idfon.net/go/add/");
        assert_eq!(bob.rpc("contact.inspect", serde_json::json!({"link": universal})), seen);
        // As Carol (a forwarded link): not hers, not valid, Don unknown.
        let forwarded = carol.rpc("contact.inspect", serde_json::json!({"link": link}));
        assert_eq!(forwarded["intro"]["valid"], false);
        assert_eq!(forwarded["intro"]["addressed_to_me"], false);
        assert_eq!(forwarded["intro"]["introducer_is_contact"], false);
        // Unbound (share sheet): valid for anyone, addressed to no one.
        let open = don.rpc("contact.introduce", serde_json::json!({"ref": "Stella"}));
        assert!(open["intro"]["recipient"].is_null());
        assert!(open["intro"].get("mutual").is_none() && open["intro"].get("note").is_none());
        let open_seen = carol.rpc("contact.inspect", serde_json::json!({"link": open["link"]}));
        assert_eq!(open_seen["intro"]["valid"], true);
        assert_eq!(open_seen["intro"]["addressed_to_me"], false);
        // Tampered: the note changed after signing.
        let mut tampered = envelope.clone();
        tampered["intro"]["note"] = "Stella does our books".into();
        let tampered_link = idfon_protocol::vcf::add_link(&serde_json::from_value(tampered).unwrap());
        assert_eq!(bob.rpc("contact.inspect", serde_json::json!({"link": tampered_link}))["intro"]["valid"], false);
        // A plain card link has no intro; garbage is refused.
        let plain = don.rpc("contact.export", serde_json::json!({"ref": "Stella"}));
        assert!(bob.rpc("contact.inspect", serde_json::json!({"link": plain["link"]})).get("intro").is_none());
        assert!(bob.error("contact.inspect", serde_json::json!({"link": "idfon://add/!!"})).contains("invalid"));

        // peer.add with the intro: Bob records who introduced Stella and keeps
        // the token to attach; the card's preference comes along.
        let card = &envelope["contact"];
        let added = bob.rpc("peer.add", serde_json::json!({"contact_ticket": card, "intro": intro}))["peer"].clone();
        assert_eq!(added["share"], "ask");
        assert_eq!(added["introduced_by"], serde_json::json!({
            "peer_id": don.peer_id, "name": "intro-don", "issued": intro["issued"],
            "note": "Stella does our taxes", "mutual": true}));
        assert!(introductions::pending_token(&bob.store.lock().unwrap(), "default", &stella.peer_id).is_some());
        // Re-adding without an intro keeps it.
        let readded = bob.rpc("peer.add", serde_json::json!({"contact_ticket": card}))["peer"].clone();
        assert_eq!(readded["introduced_by"]["peer_id"], don.peer_id.as_str());
        // Carol adds from the forwarded link: added, but the intro is ignored.
        let carols = carol.rpc("peer.add", serde_json::json!({"contact_ticket": card, "intro": serde_json::to_string(intro).unwrap()}))["peer"].clone();
        assert_eq!(carols["id"], stella.peer_id.as_str());
        assert!(carols.get("introduced_by").is_none());
        assert!(carol.store.lock().unwrap().pending_introductions.is_empty());
        // peer.update share=ok clears it.
        assert!(bob.rpc("peer.update", serde_json::json!({"ref": "Stella", "share": "ok"}))["peer"].get("share").is_none());

        // identity.update { share }: your own card carries it.
        assert!(don.error("identity.update", serde_json::json!({})).contains("share is required"));
        assert!(don.error("identity.update", serde_json::json!({"share": "never"})).contains("share must be"));
        assert_eq!(don.rpc("identity.update", serde_json::json!({"share": "ask"})), serde_json::json!({"identity": "default", "share": "ask"}));
        assert_eq!(don.rpc("identity.list", serde_json::json!({}))["identities"][0]["share"], "ask");
        let own = don.rpc("contact.export", serde_json::json!({}));
        let own_card = idfon_protocol::vcf::contact_ticket_from_link(own["link"].as_str().unwrap()).unwrap();
        assert_eq!(own_card.share.as_deref(), Some("ask"));
        assert_eq!(don.rpc("identity.update", serde_json::json!({"share": "ok"}))["share"], "ok");
        let own = don.rpc("contact.export", serde_json::json!({}));
        assert_eq!(idfon_protocol::vcf::contact_ticket_from_link(own["link"].as_str().unwrap()).unwrap().share, None);
        // Persisted.
        assert!(Store::load(&don.dir).unwrap().identities[0].share.is_none());
        don.rpc("identity.update", serde_json::json!({"share": "ask"}));
        assert_eq!(Store::load(&don.dir).unwrap().identities[0].share.as_deref(), Some("ask"));
    }

    /// A introduces C to B; B adds C with the intro and writes; C's held
    /// request shows the introduction from A (a contact of C's). The token
    /// rides B's messages to C until C's first message reaches B.
    #[test]
    fn an_introduction_rides_the_first_messages_to_the_person_introduced() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _entered = runtime.enter();
        let a = IntroNode::new("intro-a");
        let b = IntroNode::new("intro-b");
        let c = IntroNode::new("intro-c");
        let d = IntroNode::new("intro-d");
        a.know(&b, "Bob");
        a.know(&c, "Carol");
        c.know(&a, "Ann");
        // Deliver every send, as `message.receive` on the target's store.
        let deliver = |from: &IntroNode, nodes: &[&IntroNode]| -> Vec<(String, Response)> {
            let mut delivered = Vec::new();
            for _ in 0..80 {
                std::thread::sleep(std::time::Duration::from_millis(25));
                let sent = from.fake.take_sent_to();
                if sent.is_empty() && !delivered.is_empty() {
                    break;
                }
                for (target, envelope) in sent {
                    let to = nodes.iter().find(|node| node.endpoint == target).expect("a known node");
                    let mut params = serde_json::to_value(&envelope).unwrap();
                    params["identity"] = "default".into();
                    let introduction = envelope.introduction.clone().unwrap_or_default();
                    delivered.push((introduction, to.call("message.receive", params)));
                }
            }
            delivered
        };

        let shared = a.rpc("contact.introduce", serde_json::json!({"ref": "Carol", "to": "Bob", "note": "she fixes bikes"}));
        // B adds C from the link's vCard (contact.import reads the intro from
        // its URL), grants chat like the app does, and writes.
        let imported = b.rpc("contact.import", serde_json::json!({"text": shared["vcard"]}))["peers"][0].clone();
        assert_eq!(imported["id"], c.peer_id.as_str());
        assert_eq!(imported["introduced_by"]["peer_id"], a.peer_id.as_str());
        b.rpc("access.grant", serde_json::json!({"identity": "default", "subject": c.peer_id, "capability": "message.send"}));
        b.rpc("message.send", serde_json::json!({"to": c.peer_id, "text": "hi, Ann said to write", "idempotency_key": "k1", "retries": 0}));
        b.rpc("message.send", serde_json::json!({"to": c.peer_id, "text": "about the bike", "idempotency_key": "k2", "retries": 0}));
        let delivered = deliver(&b, &[&c]);
        assert_eq!(delivered.len(), 2);
        for (introduction, response) in &delivered {
            assert_eq!(introduction, &shared["intro"].to_string(), "the token rides every first message");
            assert!(response.ok, "{response:?}");
        }

        // C sees a held request, introduced by A, who is C's contact.
        let requests = c.rpc("request.list", serde_json::json!({}))["requests"].clone();
        assert_eq!(requests.as_array().unwrap().len(), 1);
        let request = &requests[0];
        assert_eq!(request["peer_id"], b.peer_id.as_str());
        assert_eq!(request["messages"].as_array().unwrap().len(), 2);
        assert_eq!(request["introduction"], serde_json::json!({
            "introducer": a.peer_id, "introducer_name": "intro-a", "introducer_is_contact": true,
            "addressed": true, "note": "she fixes bikes", "issued": shared["intro"]["issued"], "mutual": false}));
        let events = c.rpc("event.list", serde_json::json!({"type": "contact.request"}))["events"].clone();
        assert!(events.as_array().unwrap().iter().all(|event| event["data"]["introduction"]["introducer"] == a.peer_id.as_str()));
        // Nothing was accepted or granted.
        assert!(c.store.lock().unwrap().peers.iter().all(|peer| peer.id != b.peer_id));
        // C forgetting A: the request still shows the introduction, unvouched.
        c.store.lock().unwrap().peers.retain(|peer| peer.id != a.peer_id);
        assert_eq!(c.rpc("request.list", serde_json::json!({}))["requests"][0]["introduction"]["introducer_is_contact"], false);
        c.know(&a, "Ann");

        // D replays A's token (bound to B) on its own message: dropped.
        d.know(&c, "Carol");
        d.rpc("access.grant", serde_json::json!({"identity": "default", "subject": c.peer_id, "capability": "message.send"}));
        d.store.lock().unwrap().pending_introductions.push(introductions::PendingIntroduction {
            identity: "default".into(),
            peer_id: c.peer_id.clone(),
            token: shared["intro"].to_string(),
        });
        d.rpc("message.send", serde_json::json!({"to": c.peer_id, "text": "me too", "idempotency_key": "k1", "retries": 0}));
        let replayed = deliver(&d, &[&c]);
        assert!(replayed[0].1.ok, "the message is still held");
        let requests = c.rpc("request.list", serde_json::json!({}))["requests"].clone();
        let from_d = requests.as_array().unwrap().iter().find(|request| request["peer_id"] == d.peer_id.as_str()).unwrap();
        assert!(from_d.get("introduction").is_none(), "a token not bound to the sender is dropped");

        // C accepts B (a person's decision) and replies; the new contact keeps
        // who introduced them.
        let request_id = request["request_id"].as_str().unwrap();
        let accepted = c.rpc("request.accept", serde_json::json!({"request_id": request_id, "name": "Bob"}))["peer"].clone();
        assert_eq!(accepted["introduced_by"]["peer_id"], a.peer_id.as_str());
        c.rpc("message.send", serde_json::json!({"to": b.peer_id, "text": "hi Bob", "idempotency_key": "r1", "retries": 0}));
        let replies = deliver(&c, &[&b]);
        assert!(replies[0].1.ok, "{:?}", replies[0].1);
        assert!(replies[0].0.is_empty(), "C was added without an introduction");
        // B heard from C: no more token, and that is persisted.
        assert!(introductions::pending_token(&b.store.lock().unwrap(), "default", &c.peer_id).is_none());
        assert!(Store::load(&b.dir).unwrap().pending_introductions.is_empty());
        b.rpc("message.send", serde_json::json!({"to": c.peer_id, "text": "great", "idempotency_key": "k3", "retries": 0}));
        let later = deliver(&b, &[&c]);
        assert!(later[0].0.is_empty(), "no introduction after hearing back");
        assert!(later[0].1.ok);
        assert_eq!(b.peer(&c.peer_id).introduced_by.unwrap().peer_id, a.peer_id, "the record stays");
    }
}
