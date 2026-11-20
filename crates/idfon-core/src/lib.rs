//! Domain security helpers kept independent from transport details.

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use getrandom::{rand_core::UnwrapErr, SysRng};
use idfon_protocol::{Capability, MessageContent, MessageEnvelope, PeerAuth, VoiceRoute};
use rcan::{Capability as RcanCapability, Delegation, Expires};
use serde::Serialize;
use thiserror::Error;

pub mod alpn;
pub mod path;
// Gossip + tokio I/O are native-only; the wasm build (Cloudflare Worker) uses
// only the security helpers in this file.
#[cfg(not(target_family = "wasm"))]
pub mod relay;
#[cfg(not(target_family = "wasm"))]
pub mod transport;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("invalid peer ID encoding")]
    InvalidPeerId,
    #[error("invalid signature encoding")]
    InvalidSignature,
    #[error("message authentication failed")]
    VerificationFailed,
    #[error("message authentication serialization failed")]
    Serialization,
}



/// A peer's identity: the Ed25519 key it signs with. The hex of its public
/// half is the peer id (`PeerAuth.peer_id`, ticket subjects and issuers),
/// and its secret bytes are the iroh endpoint key. Every public API that
/// needs "who am I" takes `&IdentityKey`; it derefs to the signing key.
#[derive(Clone, PartialEq, Eq)]
pub struct IdentityKey(SigningKey);

impl IdentityKey {
    /// A fresh random identity. Keep it in secure storage.
    pub fn generate() -> Self {
        Self(SigningKey::generate(&mut UnwrapErr(SysRng)))
    }

    pub fn from_bytes(secret: [u8; 32]) -> Self {
        Self(SigningKey::from_bytes(&secret))
    }

    /// From the 64-hex secret written by [`IdentityKey::encode`].
    pub fn decode(hex: &str) -> Option<Self> {
        decode_fixed::<32>(hex).map(Self::from_bytes)
    }

    /// The secret as 64 hex chars (the key-file format).
    pub fn encode(&self) -> String {
        encode_hex(&self.0.to_bytes())
    }

    /// The secret bytes (also the iroh endpoint secret key).
    pub fn secret_bytes(&self) -> [u8; 32] {
        self.0.to_bytes()
    }

    /// The public key as 64 hex chars: this identity's peer id.
    pub fn peer_id(&self) -> String {
        encode_hex(self.0.verifying_key().as_bytes())
    }

    pub fn signing_key(&self) -> &SigningKey {
        &self.0
    }
}

impl std::ops::Deref for IdentityKey {
    type Target = SigningKey;

    fn deref(&self) -> &SigningKey {
        &self.0
    }
}

impl From<SigningKey> for IdentityKey {
    fn from(key: SigningKey) -> Self {
        Self(key)
    }
}

impl std::fmt::Debug for IdentityKey {
    /// Never prints the secret.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("IdentityKey").field(&self.peer_id()).finish()
    }
}

/// Mint a W3C-traceparent-shaped correlation id (`00-<32hex>-<16hex>-01`).
///
/// Not cryptographically meaningful — the trace is an unsigned convenience id,
/// never an authority (see `docs/observability.md`). Unique per call across
/// processes via time + pid + a process-local counter, spread by blake3.
pub fn new_traceparent() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let digest = blake3::hash(format!("{}:{}:{}", nanos, std::process::id(), seq).as_bytes());
    let bytes = digest.as_bytes();
    format!(
        "00-{}-{}-01",
        encode_hex(&bytes[..16]),
        encode_hex(&bytes[16..24])
    )
}

/// Stable, non-revealing account handle: `blake3(account_id)` in lowercase hex.
///
/// Every contact already carries `account_id`, so the handle is derivable rather
/// than carried: `idfon://<handle>` addresses the account without exposing the
/// account key. It is 32 bytes / 64 hex — the same shape as a peer id — which is
/// fine because refs are resolved by matching a field set, not by dispatching on
/// shape (see the daemon's `resolve_peer_id`).
pub fn account_alias(account_id: &str) -> String {
    encode_hex(blake3::hash(account_id.as_bytes()).as_bytes())
}

/// The short, DNS-safe host form of an endpoint id: z-base-32 (52 chars), the
/// encoding pkarr/iroh use for endpoint ids in DNS names. The 64-char hex form
/// exceeds the 63-octet DNS label limit, so `<ref>` hostnames use this.
pub fn endpoint_ref(id: &iroh::EndpointId) -> String {
    id.to_z32()
}

/// Parses an endpoint reference: 64-char hex, or 52-char z-base-32
/// (case-insensitive, since DNS is). Length disambiguates the alphabets; other
/// forms (aliases) return `None`.
pub fn parse_endpoint_ref(value: &str) -> Option<iroh::EndpointId> {
    let value = value.trim();
    match value.len() {
        64 => value.parse::<iroh::EndpointId>().ok(),
        52 => iroh::EndpointId::from_z32(&value.to_ascii_lowercase()).ok(),
        _ => value.parse::<iroh::EndpointId>().ok(),
    }
}




/// Signs a logical message. The endpoint ID is included to bind the proof to
/// the endpoint that claims to represent the peer.
pub fn sign_message(
    key: &IdentityKey,
    endpoint_id: impl Into<String>,
    message_id: impl Into<String>,
    content: MessageContent,
    idempotency_key: impl Into<String>,
    conversation: Option<String>,
) -> Result<MessageEnvelope, AuthError> {
    sign_message_with_ticket(
        key,
        endpoint_id,
        message_id,
        content,
        idempotency_key,
        conversation,
        None,
    )
}

pub fn sign_message_with_ticket(
    key: &IdentityKey,
    endpoint_id: impl Into<String>,
    message_id: impl Into<String>,
    content: MessageContent,
    idempotency_key: impl Into<String>,
    conversation: Option<String>,
    capability_ticket: Option<CapabilityTicket>,
) -> Result<MessageEnvelope, AuthError> {
    let sender = PeerAuth {
        peer_id: key.peer_id(),
        endpoint_id: endpoint_id.into(),
        signature: String::new(),
    };
    let unsigned = MessageEnvelope {
        message_id: message_id.into(),
        sender: sender.clone(),
        content,
        idempotency_key: idempotency_key.into(),
        capability_ticket: capability_ticket.map(|ticket| ticket.token()),
        conversation,
        trace: None,
        telemetry: None,
        context: None,
    };
    let signature = key.sign(&auth_bytes(&unsigned)?);
    Ok(MessageEnvelope {
        sender: PeerAuth {
            signature: encode_hex(&signature.to_bytes()),
            ..sender
        },
        ..unsigned
    })
}

/// The signed document inside a capability ticket: everything the issuer
/// vouches for. The `rcan` envelope carries the issuer, the subject (as the
/// audience) and the expiry.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct TicketDoc {
    capabilities: Vec<Capability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    conversation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    path_scope: Option<String>,
    ticket_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    voice: Option<VoiceRoute>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    relay: Option<String>,
    /// Public/bearer ticket: issued to nobody, presented by anyone. The rcan
    /// audience is then the issuer itself; holders must opt in to admit it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    bearer: bool,
}

impl TicketDoc {
    /// True when this document grants everything `other` asks for. Used by
    /// `rcan` to keep a re-delegation from widening the original grant.
    fn covers(&self, other: &Self) -> bool {
        (self.bearer || !other.bearer)
            && self.ticket_id == other.ticket_id
            && other
                .capabilities
                .iter()
                .all(|wanted| self.capabilities.contains(wanted))
            && (self.conversation.is_none() || self.conversation == other.conversation)
            && (self.relay.is_none() || self.relay == other.relay)
            && match (&self.path_scope, &other.path_scope) {
                (None, _) => true,
                (Some(mine), Some(theirs)) => path_in_scope(theirs, mine),
                (Some(_), None) => false,
            }
    }
}

/// `path` is `scope` itself or below it, on a segment boundary: `/fs/public`
/// covers `/fs/public/a` but not `/fs/publicx`. The one rule for a ticket's
/// `path_scope`, both when serving and when delegating.
pub fn path_in_scope(path: &str, scope: &str) -> bool {
    let scope = scope.trim_end_matches('/');
    scope.is_empty()
        || path == scope
        || path
            .strip_prefix(scope)
            .is_some_and(|rest| rest.starts_with('/'))
}

/// The capability bytes rcan signs and carries: the ticket document as JSON
/// text, verbatim. Nothing canonicalizes or re-encodes it, so the signed bytes
/// and the decoded fields cannot drift apart.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct DocText(String);

impl RcanCapability for DocText {
    fn permits(&self, other: &Self) -> bool {
        match (
            serde_json::from_str::<TicketDoc>(&self.0),
            serde_json::from_str::<TicketDoc>(&other.0),
        ) {
            (Ok(mine), Ok(theirs)) => mine.covers(&theirs),
            _ => false,
        }
    }
}

/// A signed capability ticket: an `rcan` delegation whose capability is the
/// issuer's document. Because the envelope carries issuer, audience and expiry,
/// a holder can re-delegate a narrower copy (`rcan::Delegation::delegate`)
/// without being the issuer.
///
/// There is no way to obtain one that is not signature-checked: mint it, or
/// [`CapabilityTicket::decode`], which verifies as it parses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityTicket {
    delegation: Delegation<DocText>,
    doc: TicketDoc,
}

impl CapabilityTicket {
    fn from_delegation(delegation: Delegation<DocText>) -> Result<Self, AuthError> {
        let doc = serde_json::from_str(delegation.capability().0.as_str())
            .map_err(|_| AuthError::Serialization)?;
        Ok(Self { delegation, doc })
    }

    /// Decodes a ticket token. `rcan` verifies the issuer's signature while
    /// deserializing, so a returned ticket is authentic; the document shape is
    /// validated here.
    pub fn decode(token: &str) -> Result<Self, AuthError> {
        let delegation = Delegation::<DocText>::decode_string(token)
            .map_err(|_| AuthError::VerificationFailed)?;
        Self::from_delegation(delegation)
    }

    /// The wire form: a base32 `rcan` delegation token.
    pub fn token(&self) -> String {
        self.delegation.encode_string()
    }

    /// The issuer's peer id (lowercase hex).
    pub fn issuer(&self) -> String {
        encode_hex(self.delegation.issuer().as_bytes())
    }

    /// The subject endpoint id, or `None` for a public/bearer ticket.
    pub fn subject(&self) -> Option<String> {
        (!self.doc.bearer).then(|| encode_hex(self.delegation.audience().as_bytes()))
    }

    pub fn capabilities(&self) -> &[Capability] {
        &self.doc.capabilities
    }

    pub fn conversation(&self) -> Option<&str> {
        self.doc.conversation.as_deref()
    }

    pub fn path_scope(&self) -> Option<&str> {
        self.doc.path_scope.as_deref()
    }

    pub fn ticket_id(&self) -> &str {
        &self.doc.ticket_id
    }

    pub fn voice(&self) -> Option<&VoiceRoute> {
        self.doc.voice.as_ref()
    }

    pub fn relay(&self) -> Option<&str> {
        self.doc.relay.as_deref()
    }

    pub fn bearer(&self) -> bool {
        self.doc.bearer
    }

    /// Expiry as epoch seconds; `None` = never expires.
    pub fn expires_at(&self) -> Option<u64> {
        match self.delegation.expires() {
            Expires::Never => None,
            Expires::At(seconds) => Some(*seconds),
        }
    }

    pub fn is_expired(&self, now_epoch: u64) -> bool {
        self.expires_at().is_some_and(|seconds| seconds <= now_epoch)
    }

    /// The issuer's ed25519 signature over the delegation, hex.
    pub fn signature_hex(&self) -> String {
        encode_hex(&self.delegation.signature().to_bytes())
    }
}

/// A ticket serializes as its `rcan` token, and only a signature-valid token
/// deserializes, so a decoded ticket is always authentic.
impl Serialize for CapabilityTicket {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.token())
    }
}

impl<'de> serde::Deserialize<'de> for CapabilityTicket {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let token = String::deserialize(deserializer)?;
        CapabilityTicket::decode(&token).map_err(serde::de::Error::custom)
    }
}

/// Mints a ticket. `subject = None` makes it a public/bearer ticket (audience =
/// the issuer, `bearer` set in the signed document).
fn sign_ticket(
    key: &IdentityKey,
    subject: Option<String>,
    expires_at: Option<String>,
    mut doc: TicketDoc,
) -> Result<CapabilityTicket, AuthError> {
    doc.bearer = subject.is_none();
    let audience = match subject.as_deref() {
        Some(subject) => decode_fixed::<32>(subject)
            .and_then(|bytes| VerifyingKey::from_bytes(&bytes).ok())
            .ok_or(AuthError::InvalidPeerId)?,
        None => key.verifying_key(),
    };
    let expires = match expires_at.as_deref() {
        Some(value) => Expires::At(expiry_epoch(value).ok_or(AuthError::Serialization)?),
        None => Expires::Never,
    };
    let json = serde_json::to_string(&doc).map_err(|_| AuthError::Serialization)?;
    CapabilityTicket::from_delegation(Delegation::issue(key, audience, &DocText(json), expires))
}

/// What a capability ticket grants, to whom, for how long. The one way to
/// issue a ticket:
///
/// ```ignore
/// let ticket = TicketSpec::new("t-1")
///     .subject(peer_id)
///     .grant(Capability::MessageReceive)
///     .expires_at("2026-12-31T23:59:59Z")
///     .issue(&key)?;
/// ```
#[derive(Debug, Clone, Default)]
pub struct TicketSpec {
    /// The peer the ticket is bound to; `None` = a public (bearer) ticket.
    pub subject: Option<String>,
    /// Limit to one conversation (room).
    pub conversation: Option<String>,
    pub capabilities: Vec<Capability>,
    /// RFC 3339; `None` = never expires.
    pub expires_at: Option<String>,
    pub ticket_id: String,
    /// How the issuer wants voice carried (signed into the ticket).
    pub voice: Option<VoiceRoute>,
    /// Resource path prefix (e.g. `/fs/public`); signed, cannot be widened.
    pub path_scope: Option<String>,
}

impl TicketSpec {
    pub fn new(ticket_id: impl Into<String>) -> Self {
        Self {
            ticket_id: ticket_id.into(),
            ..Self::default()
        }
    }

    pub fn subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }

    pub fn conversation(mut self, conversation: impl Into<String>) -> Self {
        self.conversation = Some(conversation.into());
        self
    }

    pub fn grant(mut self, capability: Capability) -> Self {
        self.capabilities.push(capability);
        self
    }

    pub fn expires_at(mut self, expires_at: impl Into<String>) -> Self {
        self.expires_at = Some(expires_at.into());
        self
    }

    pub fn voice(mut self, voice: VoiceRoute) -> Self {
        self.voice = Some(voice);
        self
    }

    pub fn path_scope(mut self, path_scope: impl Into<String>) -> Self {
        self.path_scope = Some(path_scope.into());
        self
    }

    /// Sign the ticket with the issuer's key.
    pub fn issue(self, key: &IdentityKey) -> Result<CapabilityTicket, AuthError> {
        sign_ticket(
            key,
            self.subject,
            self.expires_at,
            TicketDoc {
                capabilities: self.capabilities,
                conversation: self.conversation,
                path_scope: self.path_scope,
                ticket_id: self.ticket_id,
                voice: self.voice,
                relay: None,
                bearer: false,
            },
        )
    }
}

/// The signed payload of a relay reachability grant (a "claiming" ticket): the
/// relay URL the issuer vouches for and the revocation id. The `rcan` envelope
/// carries the issuer, the subject endpoint as the audience, and the expiry.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RelayGrant {
    /// Relay URL the issuer claims for the subject's reachability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay: Option<String>,
    /// Revocation id: the relay cascades a revoke to tokens derived from it.
    pub ticket_id: String,
}

impl RcanCapability for RelayGrant {
    /// A grant permits use of its own relay (or any relay when unbound) under
    /// the same revocation id.
    fn permits(&self, other: &Self) -> bool {
        self.ticket_id == other.ticket_id && (self.relay.is_none() || self.relay == other.relay)
    }
}

/// Mints a relay reachability grant as an `rcan` token: the issuer's identity
/// key binds the **subject** endpoint id (the token's audience) to the issuer's
/// **relay** URL with an expiry. The relay's auth service (`idfon-edge/deploy/relay-auth`)
/// admits the subject on the strength of the signature; `Delegation::delegate`
/// lets an admitted caller re-delegate a narrower grant downstream.
pub fn issue_relay_grant(
    key: &IdentityKey,
    subject_endpoint_id: impl Into<String>,
    relay_url: impl Into<String>,
    expires_at: Option<String>,
    ticket_id: impl Into<String>,
) -> Result<String, AuthError> {
    let subject = subject_endpoint_id.into();
    let audience = decode_fixed::<32>(&subject)
        .and_then(|bytes| VerifyingKey::from_bytes(&bytes).ok())
        .ok_or(AuthError::InvalidPeerId)?;
    let expires = match expires_at.as_deref() {
        Some(value) => Expires::At(expiry_epoch(value).ok_or(AuthError::Serialization)?),
        None => Expires::Never,
    };
    let grant = RelayGrant {
        relay: Some(relay_url.into()),
        ticket_id: ticket_id.into(),
    };
    Ok(Delegation::issue(key, audience, &grant, expires).encode_string())
}

/// Decodes a relay grant token and verifies its signature. Expiry, issuer trust
/// and revocation are the relay's call (see `idfon-edge/docs/relay-auth.md`).
pub fn verify_relay_grant(token: &str) -> Result<RelayGrant, AuthError> {
    let delegation: Delegation<RelayGrant> =
        Delegation::decode_string(token).map_err(|_| AuthError::VerificationFailed)?;
    Ok(delegation.capability())
}

/// The exact bytes an owner-bootstrap proof signs
/// (`idfon-edge/deploy/relay-auth/server.mjs` `OWNER_PROOF_PREFIX`), pinned by the
/// golden vector below and `test.mjs`: identity, endpoint, nonce and the
/// relay-auth **origin** the daemon talked to
/// (`https://relay.example.com:9443`, as `URL.origin` serializes it). Binding
/// the origin means a service the daemon was pointed at cannot pass a real
/// relay's challenge through and collect a signature that relay accepts.
pub fn relay_owner_proof_bytes(identity: &str, endpoint: &str, nonce: &str, origin: &str) -> Vec<u8> {
    format!("idfon-relay-owner/2|{identity}|{endpoint}|{nonce}|{origin}").into_bytes()
}

/// Signs the owner proof with the **identity** key (never the endpoint key).
pub fn sign_relay_owner_proof(
    key: &IdentityKey,
    identity: &str,
    endpoint: &str,
    nonce: &str,
    origin: &str,
) -> String {
    encode_hex(
        &key.sign(&relay_owner_proof_bytes(identity, endpoint, nonce, origin))
            .to_bytes(),
    )
}

/// True when `expires_at` is at or before `now_epoch`. `expires_at` may be
/// epoch seconds or RFC 3339; an unparseable value counts as expired.
pub fn expiry_passed(expires_at: &str, now_epoch: u64) -> bool {
    expiry_epoch(expires_at).is_none_or(|seconds| seconds <= now_epoch)
}

/// Parses an expiry into epoch seconds: plain epoch seconds or RFC 3339.
/// `None` when unparseable (callers treat that as expired).
fn expiry_epoch(value: &str) -> Option<u64> {
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(seconds);
    }
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|when| when.timestamp().max(0) as u64)
}

pub fn sign_state_sync(
    key: &IdentityKey,
    account_id: String,
    batch_id: String,
    events: Vec<idfon_protocol::Event>,
) -> idfon_protocol::StateSyncEnvelope {
    let mut envelope = idfon_protocol::StateSyncEnvelope {
        account_id,
        batch_id,
        events,
        signature: String::new(),
    };
    envelope.signature = encode_hex(&key.sign(&state_sync_bytes(&envelope)).to_bytes());
    envelope
}

pub fn verify_state_sync(envelope: &idfon_protocol::StateSyncEnvelope) -> Result<(), AuthError> {
    let public = decode_fixed::<32>(&envelope.account_id).ok_or(AuthError::InvalidPeerId)?;
    let key = VerifyingKey::from_bytes(&public).map_err(|_| AuthError::InvalidPeerId)?;
    let signature = decode_fixed::<64>(&envelope.signature)
        .ok_or(AuthError::InvalidSignature)
        .and_then(|bytes| {
            Signature::from_bytes(&bytes)
                .try_into()
                .map_err(|_| AuthError::InvalidSignature)
        })?;
    key.verify(&state_sync_bytes(envelope), &signature)
        .map_err(|_| AuthError::VerificationFailed)
}

fn state_sync_bytes(envelope: &idfon_protocol::StateSyncEnvelope) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "account_id": envelope.account_id,
        "batch_id": envelope.batch_id,
        "events": envelope.events,
    }))
    .expect("state sync serialization")
}

pub fn verify_message(message: &MessageEnvelope) -> Result<(), AuthError> {
    let public = decode_fixed::<32>(&message.sender.peer_id).ok_or(AuthError::InvalidPeerId)?;
    let verifying_key = VerifyingKey::from_bytes(&public).map_err(|_| AuthError::InvalidPeerId)?;
    let signature = decode_fixed::<64>(&message.sender.signature)
        .ok_or(AuthError::InvalidSignature)
        .and_then(|bytes| {
            Signature::from_bytes(&bytes)
                .try_into()
                .map_err(|_| AuthError::InvalidSignature)
        })?;
    verifying_key
        .verify(&auth_bytes(message)?, &signature)
        .map_err(|_| AuthError::VerificationFailed)
}

#[derive(Serialize)]
struct AuthPayload<'a> {
    message_id: &'a str,
    peer_id: &'a str,
    endpoint_id: &'a str,
    content: &'a MessageContent,
    idempotency_key: &'a str,
    capability_ticket: &'a Option<String>,
    conversation: &'a Option<String>,
}

fn auth_bytes(message: &MessageEnvelope) -> Result<Vec<u8>, AuthError> {
    serde_json::to_vec(&AuthPayload {
        message_id: &message.message_id,
        peer_id: &message.sender.peer_id,
        endpoint_id: &message.sender.endpoint_id,
        content: &message.content,
        idempotency_key: &message.idempotency_key,
        capability_ticket: &message.capability_ticket,
        conversation: &message.conversation,
    })
    .map_err(|_| AuthError::Serialization)
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0xf) as usize] as char);
    }
    output
}

fn decode_fixed<const N: usize>(value: &str) -> Option<[u8; N]> {
    if value.len() != N * 2 {
        return None;
    }
    let mut bytes = [0; N];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_digit(pair[0])? << 4) | hex_digit(pair[1])?;
    }
    Some(bytes)
}

fn hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticket_spec_issues_what_it_says() {
        let issuer = IdentityKey::generate();
        let holder = IdentityKey::generate();
        let ticket = TicketSpec::new("t-spec")
            .subject(holder.peer_id())
            .grant(Capability::MessageReceive)
            .conversation("room-1")
            .path_scope("/fs/public")
            .expires_at("2099-01-01T00:00:00Z")
            .issue(&issuer)
            .unwrap();
        let ticket = CapabilityTicket::decode(&ticket.token()).unwrap();
        assert_eq!(ticket.issuer(), issuer.peer_id());
        assert_eq!(ticket.subject(), Some(holder.peer_id()));
        assert_eq!(ticket.capabilities(), &[Capability::MessageReceive]);
        assert_eq!(ticket.conversation(), Some("room-1"));
        assert_eq!(ticket.path_scope(), Some("/fs/public"));
        assert_eq!(ticket.ticket_id(), "t-spec");
        assert!(ticket.expires_at().is_some());
        assert!(!ticket.bearer());
    }
    use idfon_protocol::VoiceMode;

    #[test]
    fn endpoint_ref_round_trips_hex_and_z32() {
        let key = IdentityKey::generate();
        let id: iroh::EndpointId = key.peer_id().parse().unwrap();
        let hex = id.to_string();
        assert_eq!(hex.len(), 64);
        assert_eq!(parse_endpoint_ref(&hex), Some(id));

        let short = endpoint_ref(&id);
        assert_eq!(short.len(), 52, "z-base-32 of a 32-byte id is 52 chars");
        assert!(short.len() < 63, "must fit a DNS label");
        assert_eq!(parse_endpoint_ref(&short), Some(id));
        // DNS is case-insensitive; z-base-32 decoding is not.
        assert_eq!(parse_endpoint_ref(&short.to_uppercase()), Some(id));
        assert_eq!(parse_endpoint_ref("not-a-ref"), None);
    }

    #[test]
    fn signed_message_verifies_and_tampering_fails() {
        let key = IdentityKey::generate();
        let mut message = sign_message(
            &key,
            "endpoint-a",
            "msg-1",
            MessageContent::Text {
                text: "hello".into(),
            },
            "retry-1",
            None,
        )
        .unwrap();
        assert_eq!(verify_message(&message), Ok(()));
        message.content = MessageContent::Text {
            text: "tampered".into(),
        };
        assert_eq!(verify_message(&message), Err(AuthError::VerificationFailed));
    }

    #[test]
    fn capability_ticket_round_trips_and_tampering_fails() {
        let key = IdentityKey::generate();
        let subject = IdentityKey::generate().peer_id();
        let ticket = TicketSpec {
            subject: Some(subject.clone()),
            capabilities: vec![Capability::MessageReceive],
            expires_at: Some("2099-01-01T00:00:00Z".into()),
            ticket_id: "ticket-1".into(),
            ..Default::default()
        }
        .issue(&key)
        .expect("mints");
        assert_eq!(ticket.issuer(), key.peer_id());
        assert_eq!(ticket.subject(), Some(subject));
        assert_eq!(ticket.ticket_id(), "ticket-1");
        assert!(!ticket.bearer());
        assert!(!ticket.is_expired(1));

        // The token is the wire form; decoding it reproduces the ticket.
        let token = ticket.token();
        assert_eq!(CapabilityTicket::decode(&token).expect("verifies"), ticket);

        // Flipping a signature character fails closed.
        let mut forged = token.clone();
        let last = forged.pop().expect("non-empty");
        forged.push(if last == 'a' { 'b' } else { 'a' });
        assert_eq!(
            CapabilityTicket::decode(&forged).unwrap_err(),
            AuthError::VerificationFailed
        );

        // Garbage never decodes into a ticket.
        assert!(CapabilityTicket::decode("not-a-token").is_err());
    }

    #[test]
    fn bearer_ticket_has_no_subject_and_still_verifies() {
        let key = IdentityKey::generate();
        let ticket = TicketSpec {
            subject: None,
            capabilities: vec![Capability::MessageReceive],
            expires_at: None,
            ticket_id: "t-public".into(),
            ..Default::default()
        }
        .issue(&key)
        .expect("mints");
        assert!(ticket.bearer());
        assert_eq!(ticket.subject(), None);
        assert_eq!(ticket.expires_at(), None);
        let decoded = CapabilityTicket::decode(&ticket.token()).expect("verifies");
        assert!(decoded.bearer());
        assert_eq!(decoded.subject(), None);
    }

    #[test]
    fn voice_route_is_signed_into_the_ticket() {
        let key = IdentityKey::generate();
        let subject = IdentityKey::generate().peer_id();
        let voice = VoiceRoute {
            mode: VoiceMode::NativeDuplex,
            audio: Some("pcm24k".into()),
            model: Some("openai/gpt-live-1".into()),
            delegate: None,
            stt: None,
            tts: None,
        };
        let ticket = TicketSpec {
            subject: Some(subject.clone()),
            conversation: None,
            capabilities: vec![Capability::MessageReceive],
            expires_at: None,
            ticket_id: "t-voice".into(),
            voice: Some(voice.clone()),
            path_scope: None,
        }
        .issue(&key)
        .expect("mints");
        assert_eq!(ticket.voice(), Some(&voice));
        // A different voice route is a different signed document.
        let plain = TicketSpec {
            subject: Some(subject),
            conversation: None,
            capabilities: vec![Capability::MessageReceive],
            expires_at: None,
            ticket_id: "t-voice".into(),
            voice: None,
            path_scope: None,
        }
        .issue(&key)
        .expect("mints");
        assert_eq!(plain.voice(), None);
        assert_ne!(ticket.token(), plain.token());
    }

    #[test]
    fn a_holder_can_redelegate_but_not_widen() {
        use rcan::Authorizer;

        let owner = IdentityKey::generate();
        let holder = IdentityKey::generate();
        let caller = IdentityKey::generate();
        let ticket = TicketSpec {
            subject: Some(holder.peer_id()),
            capabilities: vec![Capability::MessageReceive, Capability::LiveAudioSubscribe],
            expires_at: None,
            ticket_id: "t-redelegate".into(),
            path_scope: Some("/fs".into()),
            ..Default::default()
        }
        .issue(&owner)
        .expect("mints");

        let doc = |capabilities: Vec<Capability>, path_scope: &str| TicketDoc {
            capabilities,
            conversation: None,
            path_scope: Some(path_scope.into()),
            ticket_id: "t-redelegate".into(),
            voice: None,
            relay: None,
            bearer: false,
        };
        // The holder re-delegates a narrower slice under the owner's root.
        let narrower = doc(vec![Capability::MessageReceive], "/fs/public");
        let delegated = Delegation::delegate(
            &holder,
            caller.verifying_key(),
            owner.verifying_key(),
            &DocText(serde_json::to_string(&narrower).unwrap()),
            Expires::Never,
        );
        let authorizer = Authorizer::new(owner.verifying_key());
        authorizer
            .check_invocation_from(
                caller.verifying_key(),
                DocText(serde_json::to_string(&narrower).unwrap()),
                &[&ticket.delegation, &delegated],
            )
            .expect("a narrower re-delegation is accepted");

        // A wider path or an extra capability is refused by the chain check.
        for widened in [
            doc(vec![Capability::MessageReceive], "/other"),
            doc(
                vec![Capability::MessageReceive, Capability::McpTransport],
                "/fs/public",
            ),
        ] {
            let bad = Delegation::delegate(
                &holder,
                caller.verifying_key(),
                owner.verifying_key(),
                &DocText(serde_json::to_string(&widened).unwrap()),
                Expires::Never,
            );
            assert!(
                authorizer
                    .check_invocation_from(
                        caller.verifying_key(),
                        DocText(serde_json::to_string(&widened).unwrap()),
                        &[&ticket.delegation, &bad],
                    )
                    .is_err(),
                "widening must not verify"
            );
        }
    }

    #[test]
    fn expiry_accepts_epoch_and_rfc3339() {
        assert!(expiry_passed("1", 100));
        assert!(!expiry_passed("200", 100));
        assert!(expiry_passed("1970-01-01T00:00:01Z", 100));
        assert!(!expiry_passed("2099-01-01T00:00:00Z", 100));
        // Unparseable is treated as expired.
        assert!(expiry_passed("soon", 100));
    }

    #[test]
    fn state_sync_signature_covers_batch_and_events() {
        let key = IdentityKey::generate();
        let event = idfon_protocol::Event {
            event_id: "event-1".into(),
            cursor: "cursor-1".into(),
            r#type: "peer.created".into(),
            timestamp: "1".into(),
            identity: "default".into(),
            data: serde_json::json!({"peer_id": "alice"}),
        };
        let mut envelope = sign_state_sync(&key, key.peer_id(), "batch-1".into(), vec![event]);
        assert_eq!(verify_state_sync(&envelope), Ok(()));
        envelope.batch_id = "batch-2".into();
        assert_eq!(
            verify_state_sync(&envelope),
            Err(AuthError::VerificationFailed)
        );
    }

    #[test]
    fn endpoint_binding_is_authenticated() {
        let key = IdentityKey::generate();
        let mut message = sign_message(
            &key,
            "endpoint-a",
            "msg-1",
            MessageContent::Text {
                text: "hello".into(),
            },
            "retry-1",
            None,
        )
        .unwrap();
        message.sender.endpoint_id = "endpoint-b".into();
        assert_eq!(verify_message(&message), Err(AuthError::VerificationFailed));
    }

    #[test]
    fn relay_grant_golden_vector() {
        // Cross-language contract: idfon-edge/deploy/relay-auth/test.mjs verifies the same
        // token with node:crypto. ed25519 is deterministic, so any drift in the
        // rcan payload encoding breaks one side.
        let key = IdentityKey::decode(&"07".repeat(32)).expect("fixed key");
        let token = issue_relay_grant(
            &key,
            "11".repeat(32),
            "https://relay.example.com:8443",
            Some("2030-01-01T00:00:00Z".into()),
            "ticket-golden-1",
        )
        .expect("grant mints");
        assert_eq!(token, "alveu3dd4kofecv66vihwezoyx4zkr3wv27l464siipou2iui3jcyeirceirceirceirceirceirceirceirceirceirceirceirceiraaaybmppqydtaai6nb2hi4dthixs64tfnrqxsltfpbqw24dmmuxgg33nhi4dinbtb52gsy3lmv2c2z3pnrsgk3rnghzoj6qqzxlkmpjupwriwbdvgefknpmbgov2ivvzvg3xrbc3z47n6p2urqcex3my2uuyjupmowk5agtgdencpsaatz26drfwkgxfboya");
        let grant = verify_relay_grant(&token).expect("grant verifies");
        assert_eq!(grant.relay.as_deref(), Some("https://relay.example.com:8443"));
        assert_eq!(grant.ticket_id, "ticket-golden-1");
    }

    #[test]
    fn relay_owner_proof_golden_vector() {
        // Cross-language contract with relay-auth (test.mjs verifies the same vector).
        let key = IdentityKey::decode(&"07".repeat(32)).expect("fixed key");
        let identity = key.peer_id();
        let endpoint = "11".repeat(32);
        let nonce = "00112233445566778899aabbccddeeff";
        let origin = "https://relay.example.com:9443";
        assert_eq!(
            relay_owner_proof_bytes(&identity, &endpoint, nonce, origin),
            format!("idfon-relay-owner/2|{identity}|{endpoint}|{nonce}|{origin}").into_bytes()
        );
        let signature = sign_relay_owner_proof(&key, &identity, &endpoint, nonce, origin);
        assert_eq!(signature, "30d079eeaecf63f325e1973ecf2a3d2b04c1a5d9afee607b6e934c34af7a9883f1a352b851ad4159b82cbb0701823adabc192cb097ee9993eba6c1d0357e6003");
    }

    #[test]
    fn trace_is_unsigned_and_does_not_break_verification() {
        let key = IdentityKey::generate();
        let mut message = sign_message(
            &key,
            "endpoint-a",
            "msg-trace",
            MessageContent::Text {
                text: "hello".into(),
            },
            "retry-trace",
            None,
        )
        .unwrap();
        assert!(verify_message(&message).is_ok());
        // Attaching or replacing a trace must not invalidate the signature:
        // it is a convenience correlation id, not authenticated content.
        message.trace = Some(new_traceparent());
        assert!(verify_message(&message).is_ok());
        // `telemetry` is likewise an unsigned, best-effort advertisement.
        message.telemetry = Some("inject".into());
        assert!(verify_message(&message).is_ok());
        assert_eq!(new_traceparent().len(), 55);
        assert_ne!(new_traceparent(), new_traceparent());
    }
}
