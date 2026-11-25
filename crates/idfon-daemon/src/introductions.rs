//! Introductions in the daemon (docs/protocol.md "Introductions"): sharing a
//! contact's card signed by this identity, reading one, the sharing
//! preference, and the token that rides on the first messages to the person
//! introduced. An introduction grants, adds or accepts nothing.

use std::sync::{Arc, Mutex};

use idfon_protocol::intro::{parse_share, share_label, MAX_INTRO_NOTE_CHARS};
use idfon_protocol::{AddLink, ErrorCode, Introduction, Request, RequestIntroduction, Response};

use super::{error_response, now, request_text, success, Store};

/// An introduction token this identity attaches to every message it sends a
/// contact until a message from that contact arrives (persisted).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct PendingIntroduction {
    pub identity: String,
    pub peer_id: String,
    /// The token as JSON text, sent as `MessageEnvelope.introduction`.
    pub token: String,
}

pub(crate) fn epoch_now() -> u64 {
    now().parse().unwrap_or(0)
}

/// This identity's peer id (its public key, hex).
pub(crate) fn own_peer_id(state: &Store, identity: &str) -> Option<String> {
    state
        .identities
        .iter()
        .find(|candidate| candidate.id == identity)
        .and_then(|candidate| candidate.public_key.clone())
}

/// A token that counts for this identity: it verifies, names `subject`, and
/// is unbound or bound to `recipient`.
pub(crate) fn is_valid_for(intro: &Introduction, subject: &str, recipient: &str) -> bool {
    idfon_core::verify_introduction(intro, epoch_now()).is_ok()
        && intro.subject == subject
        && intro
            .recipient
            .as_deref()
            .is_none_or(|bound| bound == recipient)
}

fn is_contact(state: &Store, identity: &str, peer_id: &str) -> bool {
    state
        .peers
        .iter()
        .any(|peer| peer.identity == identity && peer.id == peer_id)
}

/// The token to attach to a message to `peer_id`, if one is pending.
pub(crate) fn pending_token(state: &Store, identity: &str, peer_id: &str) -> Option<String> {
    state
        .pending_introductions
        .iter()
        .find(|pending| pending.identity == identity && pending.peer_id == peer_id)
        .map(|pending| pending.token.clone())
}

/// A message from `peer_id` arrived: stop attaching. True when something was
/// removed (the caller saves).
pub(crate) fn clear_pending(state: &mut Store, identity: &str, peer_id: &str) -> bool {
    let before = state.pending_introductions.len();
    state
        .pending_introductions
        .retain(|pending| !(pending.identity == identity && pending.peer_id == peer_id));
    state.pending_introductions.len() != before
}

/// Keeps `intro` to attach to messages to `peer_id` (replacing an older one).
pub(crate) fn set_pending(state: &mut Store, identity: &str, peer_id: &str, intro: &Introduction) {
    clear_pending(state, identity, peer_id);
    state.pending_introductions.push(PendingIntroduction {
        identity: identity.into(),
        peer_id: peer_id.into(),
        token: intro.to_json(),
    });
}

/// The introduction a held contact request records: the token in a message
/// from `sender` to this identity, when it is valid for them. Invalid tokens
/// are `None` (dropped silently).
pub(crate) fn request_introduction(
    state: &Store,
    identity: &str,
    sender: &str,
    token: &str,
) -> Option<RequestIntroduction> {
    let intro = Introduction::from_json(token).ok()?;
    let me = own_peer_id(state, identity)?;
    if !is_valid_for(&intro, &me, sender) {
        tracing::info!(target: "idfon.daemon", identity = %identity, sender = %sender, "introduction dropped: not valid for this request");
        return None;
    }
    Some(RequestIntroduction {
        introducer_is_contact: is_contact(state, identity, &intro.introducer),
        addressed: intro.recipient.is_some(),
        introducer: intro.introducer,
        introducer_name: intro.introducer_name,
        note: intro.note.filter(|note| !note.is_empty()),
        issued: intro.issued,
        mutual: intro.mutual,
    })
}

/// `request.list` reports whether the introducer is a contact now.
pub(crate) fn refresh_request(state: &Store, request: &mut idfon_protocol::ContactRequest) {
    if let Some(intro) = request.introduction.as_mut() {
        intro.introducer_is_contact = is_contact(state, &request.identity, &intro.introducer);
    }
}

fn invalid(request: &Request, message: &str) -> Response {
    error_response(
        request.id.clone(),
        &request.method,
        ErrorCode::InvalidRequest,
        message.into(),
        false,
    )
}

fn find_peer<'a>(
    state: &'a Store,
    identity: &str,
    reference: &str,
) -> Option<&'a idfon_protocol::Peer> {
    state.peers.iter().find(|peer| {
        peer.identity == identity
            && (peer.id == reference
                || peer.name == reference
                || peer.aliases.iter().any(|alias| alias == reference)
                || peer.knows_endpoint(reference))
    })
}

fn is_peer_id(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// `contact.introduce { ref, to?, note?, mutual? }` → `{ link, vcard, intro,
/// share }`: `ref`'s bare card (as `contact.export {ref}` builds it) with a
/// token signed by this identity, bound to `to` when given. Only this
/// identity's own contacts can be introduced. Sends nothing.
pub(crate) fn contact_introduce(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(reference) = request_text(&request.params, "ref") else {
        return invalid(request, "ref is required");
    };
    let identity = request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let note = request_text(&request.params, "note").filter(|note| !note.is_empty());
    if note
        .as_deref()
        .is_some_and(|note| note.chars().count() > MAX_INTRO_NOTE_CHARS)
    {
        return invalid(request, "note is longer than 280 characters");
    }
    let mutual = request
        .params
        .get("mutual")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let state = store.lock().expect("store mutex poisoned");
    let Some(subject) = find_peer(&state, &identity, &reference) else {
        return invalid(request, "only your own contacts can be introduced");
    };
    let Some(card) = super::peer_card(subject) else {
        return invalid(request, "peer has no endpoint address");
    };
    let recipient = match request_text(&request.params, "to") {
        None => None,
        Some(to) => match find_peer(&state, &identity, &to) {
            Some(peer) => Some(peer.id.clone()),
            None if is_peer_id(&to) => Some(to.to_ascii_lowercase()),
            None => return invalid(request, "to is not a contact or a peer id"),
        },
    };
    if recipient.as_deref() == Some(subject.id.as_str()) {
        return invalid(request, "cannot introduce a contact to themselves");
    }
    let Some(me) = state
        .identities
        .iter()
        .find(|candidate| candidate.id == identity || candidate.name == identity)
    else {
        return invalid(request, "unknown identity");
    };
    let key = match state.identity_key(&me.id) {
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
    let intro = match idfon_core::sign_introduction(
        &key,
        me.name.clone(),
        subject.id.clone(),
        recipient,
        epoch_now(),
        mutual,
        note,
    ) {
        Ok(intro) => intro,
        Err(error) => return invalid(request, &error.to_string()),
    };
    let share = share_label(subject.share.as_deref());
    drop(state);
    let link = AddLink {
        contact: card,
        intro: Some(intro.clone()),
    };
    success(
        request,
        serde_json::json!({
            "link": idfon_protocol::vcf::add_link(&link),
            "vcard": idfon_protocol::vcf::add_link_vcard(&link),
            "intro": intro,
            "share": share,
        }),
    )
}

/// `contact.inspect { link }` → `{ contact: { peer_id, name, share }, intro? }`:
/// what an add link carries, with the introduction checked for this identity.
/// `intro.valid` means it verifies, names the card's owner, and is unbound or
/// bound to this identity: exactly when `peer.add` would record it.
pub(crate) fn contact_inspect(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(text) = request_text(&request.params, "link") else {
        return invalid(request, "link is required");
    };
    let link = match idfon_protocol::vcf::add_link_from_str(&text) {
        Ok(link) => link,
        Err(message) => return invalid(request, &message),
    };
    let identity = request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let state = store.lock().expect("store mutex poisoned");
    let me = own_peer_id(&state, &identity).unwrap_or_default();
    let card = &link.contact;
    let mut result = serde_json::json!({
        "contact": {
            "peer_id": card.account_id,
            "name": card.label.clone().filter(|name| !name.is_empty()).unwrap_or_else(|| card.account_id.clone()),
            "share": share_label(card.share.as_deref()),
        },
    });
    if let Some(intro) = &link.intro {
        result["intro"] = serde_json::json!({
            "valid": is_valid_for(intro, &card.account_id, &me),
            "introducer": intro.introducer,
            "introducer_name": intro.introducer_name,
            "introducer_is_contact": is_contact(&state, &identity, &intro.introducer),
            "subject": intro.subject,
            "recipient": intro.recipient,
            "addressed_to_me": intro.recipient.as_deref() == Some(me.as_str()),
            "issued": intro.issued,
            "mutual": intro.mutual,
            "note": intro.note,
        });
    }
    success(request, result)
}

/// `identity.update { share }` → `{ identity, share }`: your own sharing
/// preference (`"ask"` | `"ok"`); your card carries it.
pub(crate) fn identity_update(request: &Request, store: &Arc<Mutex<Store>>) -> Response {
    let Some(value) = request_text(&request.params, "share") else {
        return invalid(request, "share is required");
    };
    let share = match parse_share(&value) {
        Ok(share) => share,
        Err(message) => return invalid(request, &message),
    };
    let identity_ref = request_text(&request.params, "identity").unwrap_or_else(|| "default".into());
    let mut state = store.lock().expect("store mutex poisoned");
    let Some(index) = state
        .identities
        .iter()
        .position(|identity| identity.id == identity_ref || identity.name == identity_ref)
    else {
        return invalid(request, "unknown identity");
    };
    state.identities[index].share = share;
    let id = state.identities[index].id.clone();
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
        serde_json::json!({"identity": id, "share": share_label(state.identities[index].share.as_deref())}),
    )
}
