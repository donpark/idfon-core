//! The message doorbell from the daemon (docs/chat-envelopes.md
//! `IDFON-DOORBELL/1`): a contact's phone that is asleep gets one alert push,
//! "New message", so it wakes and comes online for what waits in our outbox.
//! The push carries only our name and account id, sealed to the contact's
//! doorbell key, so the edge and Apple see neither.
//!
//! The apps ring from their own outbox; this is the same doorbell for every
//! other client (the `idfon` CLI, agents): the daemon keeps each contact's
//! key from their `IDFON-DOORBELL/1`, and `peer.doorbell` rings it.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use hpke::{
    aead::ChaCha20Poly1305, kdf::HkdfSha256, kem::X25519HkdfSha256, Deserializable, OpModeS,
    Serializable,
};
use idfon_protocol::{ErrorCode, Request, Response};

use crate::{error_response, request_text, success, Store};

/// HPKE `info` for doorbells; the apps use the same.
const INFO: &[u8] = b"idfon-doorbell/1";
const PREFIX: &str = "IDFON-DOORBELL/1\n";
/// At most one ring per contact this often while messages wait (the apps'
/// spacing; the edge also limits rings per caller).
const SPACING: Duration = Duration::from_secs(10 * 60);

/// The key in an `IDFON-DOORBELL/1` envelope (base64 of 32 bytes that make an
/// X25519 public key), or `None` for any other text or an invalid key.
pub fn key_in(text: &str) -> Option<String> {
    let rest = text.strip_prefix(PREFIX)?;
    let key = rest.lines().find_map(|line| line.strip_prefix("key="))?.trim();
    let bytes = STANDARD.decode(key).ok()?;
    <X25519HkdfSha256 as hpke::Kem>::PublicKey::from_bytes(&bytes).ok()?;
    Some(STANDARD.encode(bytes))
}

/// A received message from `sender`: keep their doorbell key if it is one.
/// Returns whether the store changed (the caller saves it).
pub fn on_received(state: &mut Store, identity: &str, sender: &str, text: &str) -> bool {
    let Some(key) = key_in(text) else {
        return false;
    };
    let Some(peer) = state
        .peers
        .iter_mut()
        .find(|peer| peer.identity == identity && peer.id == sender)
    else {
        return false;
    };
    if peer.doorbell_key.as_deref() == Some(key.as_str()) {
        return false;
    }
    peer.doorbell_key = Some(key);
    true
}

/// HPKE-seals `{"n": name, "i": account}` to `recipient` (base64 X25519
/// public key): the encapsulated key, then the ciphertext, base64.
pub fn seal(name: &str, account: &str, recipient: &str) -> Result<String, String> {
    let bytes = STANDARD.decode(recipient).map_err(|_| "doorbell key is not base64")?;
    let key = <X25519HkdfSha256 as hpke::Kem>::PublicKey::from_bytes(&bytes)
        .map_err(|_| "doorbell key is not an X25519 public key")?;
    let name: String = name.chars().take(64).collect();
    let plaintext = serde_json::to_vec(&serde_json::json!({"n": name, "i": account}))
        .map_err(|error| error.to_string())?;
    let (encapsulated, ciphertext) = hpke::single_shot_seal::<ChaCha20Poly1305, HkdfSha256, X25519HkdfSha256>(
        &OpModeS::Base,
        &key,
        INFO,
        &plaintext,
        b"",
    )
    .map_err(|error| format!("sealing failed: {error:?}"))?;
    let mut sealed = encapsulated.to_bytes().to_vec();
    sealed.extend_from_slice(&ciphertext);
    Ok(STANDARD.encode(sealed))
}

/// Opens a sealed doorbell with the recipient's private key: (name, account).
/// The phone's notification extension does this; kept here for tests.
#[cfg(test)]
pub fn open(sealed: &str, private_key: &[u8]) -> Option<(String, String)> {
    let data = STANDARD.decode(sealed).ok()?;
    if data.len() <= 32 {
        return None;
    }
    let (encapsulated, ciphertext) = data.split_at(32);
    let key = <X25519HkdfSha256 as hpke::Kem>::PrivateKey::from_bytes(private_key).ok()?;
    let encapsulated = <X25519HkdfSha256 as hpke::Kem>::EncappedKey::from_bytes(encapsulated).ok()?;
    let plain = hpke::single_shot_open::<ChaCha20Poly1305, HkdfSha256, X25519HkdfSha256>(
        &hpke::OpModeR::Base,
        &key,
        &encapsulated,
        INFO,
        ciphertext,
        b"",
    )
    .ok()?;
    let object: serde_json::Value = serde_json::from_slice(&plain).ok()?;
    Some((object["n"].as_str()?.to_owned(), object["i"].as_str()?.to_owned()))
}

/// When each (identity, peer) doorbell last rang.
fn last_rung() -> &'static Mutex<HashMap<(String, String), Instant>> {
    static LAST: OnceLock<Mutex<HashMap<(String, String), Instant>>> = OnceLock::new();
    LAST.get_or_init(|| Mutex::new(HashMap::new()))
}

/// What a ring needs, read from the store before any await.
struct Ring {
    identity: String,
    peer_id: String,
    key: String,
    name: String,
    account: String,
}

/// `peer.doorbell { peer, identity?, url? }`: ring the contact's doorbell, so their
/// sleeping phone wakes and comes online for what waits in our outbox.
/// Answers `{rung}` (devices the edge pushed to; 0 = none registered), or
/// `{rung: 0, skipped: "recent"}` within ten minutes of the last ring. Fails
/// without their key (they never sent one: an older app, or not a phone) or
/// without an edge (`IDFON_EDGE_URL`).
pub async fn ring_request(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let fail = |code: ErrorCode, message: String| error_response(request.id.clone(), &request.method, code, message, false);
    let ring = {
        let state = store.lock().expect("store mutex poisoned");
        let identity_ref = request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
        let Some(identity) = state
            .identities
            .iter()
            .find(|identity| identity.id == identity_ref || identity.name == identity_ref)
        else {
            return fail(ErrorCode::InvalidRequest, "unknown identity".into());
        };
        let Some(reference) = request_text(&request.params, "peer") else {
            return fail(ErrorCode::InvalidRequest, "peer is required".into());
        };
        let Some(peer) = state.peers.iter().find(|peer| {
            peer.identity == identity.id
                && (peer.id == reference || peer.name == reference || peer.aliases.iter().any(|alias| alias == &reference))
        }) else {
            return fail(ErrorCode::InvalidRequest, "peer not found".into());
        };
        let Some(key) = peer.doorbell_key.clone() else {
            return fail(
                ErrorCode::InvalidRequest,
                format!("{} has no doorbell key: their idfon app sends one once it has you as a contact", peer.name),
            );
        };
        let Some(account) = identity.public_key.clone() else {
            return fail(ErrorCode::Internal, "this identity has no account id".into());
        };
        Ring { identity: identity.id.clone(), peer_id: peer.id.clone(), key, name: identity.name.clone(), account }
    };
    let slot = (ring.identity.clone(), ring.peer_id.clone());
    if last_rung().lock().expect("doorbell mutex").get(&slot).is_some_and(|at| at.elapsed() < SPACING) {
        return success(request, serde_json::json!({"rung": 0, "skipped": "recent"}));
    }
    // `url` overrides the edge, as for `file.publish`.
    let base = request_text(&request.params, "url").or_else(|| crate::edge_config().0);
    let url = match crate::edge_url(base, &ring.peer_id, "/_push/doorbell") {
        Ok(url) => url,
        Err(message) => return fail(ErrorCode::InvalidRequest, message),
    };
    let sealed = match seal(&ring.name, &ring.account, &ring.key) {
        Ok(sealed) => sealed,
        Err(message) => return fail(ErrorCode::Internal, message),
    };
    // Our own statement (no subject, message.send), as the apps present when
    // the contact gave them no ring grant; the edge caps these per ref.
    let statement = crate::capability_ticket_issue(
        &Request {
            version: idfon_protocol::PROTOCOL_VERSION,
            id: "doorbell-statement".into(),
            method: "access.ticket".into(),
            params: serde_json::json!({"identity": ring.identity, "capabilities": ["message.send"]}),
        },
        store,
    );
    let ticket = match &statement.body {
        idfon_protocol::ResponseBody::Success { result, .. } => result["ticket"].as_str().map(str::to_owned),
        _ => None,
    };
    let Some(ticket) = ticket else {
        return fail(ErrorCode::Internal, "could not issue the statement the edge asks for".into());
    };
    let caller = idfon_core::CapabilityTicket::decode(&ticket)
        .map(|ticket| ticket.issuer())
        .unwrap_or_else(|_| ring.account.clone());
    let payload = serde_json::json!({"caller_id": caller, "sealed": sealed}).to_string();
    let response = reqwest::Client::new()
        .post(&url)
        .header("x-idfon-ticket", &ticket)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(payload)
        .timeout(Duration::from_secs(15))
        .send()
        .await;
    let response = match response {
        Ok(response) => response,
        Err(error) => return fail(ErrorCode::PeerOffline, format!("the edge didn't answer: {error}")),
    };
    let status = response.status();
    let body: serde_json::Value = response
        .bytes()
        .await
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default();
    if !status.is_success() {
        let reason = body["error"].as_str().map(str::to_owned).unwrap_or_else(|| status.to_string());
        return fail(ErrorCode::PeerOffline, format!("the edge refused the doorbell: {reason}"));
    }
    last_rung().lock().expect("doorbell mutex").insert(slot, Instant::now());
    let rung = body["rung"].as_u64().unwrap_or(0);
    tracing::info!(target: "idfon.daemon", peer = %ring.peer_id, rung, "doorbell rang");
    success(request, serde_json::json!({"rung": rung}))
}

/// A delivered message to `peer`: the next wait may ring again (the apps do
/// the same).
pub fn delivered(identity: &str, peer: &str) {
    last_rung()
        .lock()
        .expect("doorbell mutex")
        .remove(&(identity.to_owned(), peer.to_owned()));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keypair() -> (Vec<u8>, String) {
        let (private, public) = <X25519HkdfSha256 as hpke::Kem>::derive_keypair(b"idfon doorbell test key material");
        (private.to_bytes().to_vec(), STANDARD.encode(public.to_bytes()))
    }

    #[test]
    fn seal_opens_with_the_recipient_key() {
        let (private, public) = keypair();
        let sealed = seal("Ann Example", "acct-1", &public).unwrap();
        assert_eq!(open(&sealed, &private), Some(("Ann Example".into(), "acct-1".into())));
    }

    #[test]
    fn names_are_cut_to_64_characters() {
        let (private, public) = keypair();
        let sealed = seal(&"x".repeat(100), "acct-1", &public).unwrap();
        assert_eq!(open(&sealed, &private).unwrap().0.len(), 64);
    }

    /// Sealed by the iPhone's code (CryptoKit, `Doorbell.seal`): the two
    /// sides speak the same HPKE.
    #[test]
    fn opens_what_cryptokit_sealed() {
        let private = STANDARD.decode(CRYPTOKIT_PRIVATE).unwrap();
        assert_eq!(open(CRYPTOKIT_SEALED, &private), Some(("Ann".into(), "acct-cryptokit".into())));
    }

    #[test]
    fn key_envelopes_parse_and_bad_keys_do_not() {
        let (_, public) = keypair();
        assert_eq!(key_in(&format!("IDFON-DOORBELL/1\nkey={public}")), Some(public));
        assert_eq!(key_in("IDFON-DOORBELL/1\nkey=abc"), None);
        assert_eq!(key_in("hello"), None);
    }

    fn store_with_phone(key: Option<String>) -> (std::path::PathBuf, Arc<Mutex<Store>>, String) {
        let dir = std::env::temp_dir().join(format!(
            "idfond-doorbell-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let mut state = Store::load(&dir).unwrap();
        let phone = "1".repeat(64);
        let mut peer: idfon_protocol::Peer = serde_json::from_value(serde_json::json!({
            "id": phone, "identity": "default", "name": "phone", "endpoint_id": null, "aliases": []
        }))
        .unwrap();
        peer.doorbell_key = key;
        state.peers.push(peer);
        (dir, Arc::new(Mutex::new(state)), phone)
    }

    #[test]
    fn a_received_key_is_kept_on_the_contact() {
        let (_, public) = keypair();
        let (dir, store, phone) = store_with_phone(None);
        let mut state = store.lock().unwrap();
        assert!(on_received(&mut state, "default", &phone, &format!("IDFON-DOORBELL/1\nkey={public}")));
        assert_eq!(state.peers[0].doorbell_key.as_deref(), Some(public.as_str()));
        assert!(!on_received(&mut state, "default", &phone, &format!("IDFON-DOORBELL/1\nkey={public}")), "same key");
        assert!(!on_received(&mut state, "default", &"2".repeat(64), &format!("IDFON-DOORBELL/1\nkey={public}")), "not a contact");
        drop(state);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Rings a local stand-in for the edge: the route, the statement, and a
    /// sealed name the phone can open; then the ten-minute spacing.
    #[tokio::test]
    async fn rings_the_edge_with_a_sealed_name() {
        let (private, public) = keypair();
        let (dir, store, phone) = store_with_phone(Some(public));
        let seen: Arc<Mutex<Vec<(String, String, serde_json::Value)>>> = Arc::default();
        let recorder = Arc::clone(&seen);
        let app = axum::Router::new().fallback(move |uri: axum::http::Uri, headers: axum::http::HeaderMap, body: String| {
            let recorder = Arc::clone(&recorder);
            async move {
                let ticket = headers.get("x-idfon-ticket").and_then(|v| v.to_str().ok()).unwrap_or_default().to_owned();
                recorder.lock().unwrap().push((uri.path().to_owned(), ticket, serde_json::from_str(&body).unwrap_or_default()));
                axum::Json(serde_json::json!({"rung": 1}))
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let request = |peer: &str| Request {
            version: idfon_protocol::PROTOCOL_VERSION,
            id: "ring".into(),
            method: "peer.doorbell".into(),
            params: serde_json::json!({"peer": peer, "url": base, "identity": "default"}),
        };
        let response = ring_request(&request("phone"), &store).await;
        let idfon_protocol::ResponseBody::Success { result, .. } = &response.body else {
            panic!("ring failed: {response:?}");
        };
        assert_eq!(result["rung"], 1);
        let (path, ticket, body) = seen.lock().unwrap()[0].clone();
        assert_eq!(path, format!("/{phone}/_push/doorbell"));
        let issuer = idfon_core::CapabilityTicket::decode(&ticket).unwrap().issuer();
        assert_eq!(body["caller_id"], issuer.as_str());
        let (name, account) = open(body["sealed"].as_str().unwrap(), &private).unwrap();
        let state = store.lock().unwrap();
        let me = state.identities.iter().find(|identity| identity.id == "default").unwrap();
        assert_eq!((name.as_str(), Some(account.as_str())), (me.name.as_str(), me.public_key.as_deref()));
        drop(state);
        let again = ring_request(&request("phone"), &store).await;
        let idfon_protocol::ResponseBody::Success { result, .. } = &again.body else { panic!("{again:?}") };
        assert_eq!(result["skipped"], "recent");
        assert_eq!(seen.lock().unwrap().len(), 1, "no second push within ten minutes");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn without_their_key_there_is_no_doorbell() {
        let (dir, store, _) = store_with_phone(None);
        let response = ring_request(
            &Request {
                version: idfon_protocol::PROTOCOL_VERSION,
                id: "ring".into(),
                method: "peer.doorbell".into(),
                params: serde_json::json!({"peer": "phone", "url": "http://127.0.0.1:9", "identity": "default"}),
            },
            &store,
        )
        .await;
        let idfon_protocol::ResponseBody::Failure { error, .. } = &response.body else { panic!("{response:?}") };
        assert!(error.message.contains("no doorbell key"), "{}", error.message);
        let _ = std::fs::remove_dir_all(dir);
    }

    const CRYPTOKIT_PRIVATE: &str = "QEMAV69vtcRZhY7Gqe+UlXQumOJXprkRw+WO8+jmjEA=";
    const CRYPTOKIT_SEALED: &str = "1r5eE8YBEKBVB3j0u21vRf52TaoIlDmYFtixc5NRUgcnAYrOfXi3ZEK5rrxornRIrS/0DhMCKmg5elDQf6MCr/j+CPTFNuTyoeFt09y4gAc=";
}
