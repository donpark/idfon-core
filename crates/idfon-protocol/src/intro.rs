//! Introductions: one person shares a contact's card with someone else,
//! signed by the sharer (idfon-home `docs/introductions.md`; the wire rules
//! are in this repo's `docs/protocol.md` "Introductions"). An introduction
//! grants nothing: it is context, never a credential.
//!
//! The token is JSON. Its `sig` signs [`INTRO_DOMAIN`] followed by the
//! compact JSON of every other field in declaration order (see
//! [`Introduction::signing_bytes`]). Signing and verification live in
//! `idfon-core` (`sign_introduction`, `verify_introduction`), which holds the
//! ed25519 keys.

use serde::{Deserialize, Serialize};

use crate::ContactTicket;

/// The domain prefix of the signed bytes.
pub const INTRO_DOMAIN: &str = "idfon-intro/1\n";
/// The token version this build writes and accepts.
pub const INTRO_VERSION: u8 = 1;
/// The longest note, in characters (Unicode scalar values).
pub const MAX_INTRO_NOTE_CHARS: usize = 280;
/// How far in the future `issued` may be (clock skew), in seconds.
pub const MAX_INTRO_FUTURE_SECS: u64 = 5 * 60;

fn is_false(value: &bool) -> bool {
    !*value
}

/// An introduction token. Field order is the signing order: do not reorder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Introduction {
    pub v: u8,
    /// The sharer's peer id (hex ed25519 public key): the signing key.
    pub introducer: String,
    /// The sharer's name as they call themselves (unverified display text).
    pub introducer_name: String,
    /// The introduced person's peer id: whose card this is.
    pub subject: String,
    /// The person it was given to, or `None` (written as `null`) for a share
    /// with no known recipient (share sheet, QR, file).
    pub recipient: Option<String>,
    /// Epoch seconds.
    pub issued: u64,
    /// Both-ways introduction: each side got the other's card. Written only
    /// when true.
    #[serde(default, skip_serializing_if = "is_false")]
    pub mutual: bool,
    /// Written only when present (non-empty).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// The introducer's ed25519 signature over [`Self::signing_bytes`], hex.
    #[serde(default)]
    pub sig: String,
}

/// The signed fields, borrowed in signing order (everything but `sig`).
#[derive(Serialize)]
struct Signed<'a> {
    v: u8,
    introducer: &'a str,
    introducer_name: &'a str,
    subject: &'a str,
    recipient: &'a Option<String>,
    issued: u64,
    #[serde(skip_serializing_if = "is_false")]
    mutual: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    note: &'a Option<String>,
}

/// Why a token is not a valid introduction.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IntroError {
    #[error("introduction is not valid JSON: {0}")]
    Json(String),
    #[error("unsupported introduction version")]
    Version,
    #[error("introduction note is longer than {MAX_INTRO_NOTE_CHARS} characters")]
    NoteTooLong,
    #[error("introduction is issued in the future")]
    Future,
    #[error("invalid introducer key")]
    Introducer,
    #[error("invalid introduction signature encoding")]
    SignatureEncoding,
    #[error("introduction signature does not verify")]
    Signature,
}

impl Introduction {
    /// The bytes `sig` signs: `idfon-intro/1\n` + the compact JSON (serde_json,
    /// no whitespace) of `v, introducer, introducer_name, subject, recipient,
    /// issued[, mutual][, note]`. `recipient` is always present (`null` when
    /// unbound); `mutual` only when true; `note` only when present.
    pub fn signing_bytes(&self) -> Vec<u8> {
        let note = self.note.clone().filter(|note| !note.is_empty());
        let mut bytes = INTRO_DOMAIN.as_bytes().to_vec();
        bytes.extend(
            serde_json::to_vec(&Signed {
                v: self.v,
                introducer: &self.introducer,
                introducer_name: &self.introducer_name,
                subject: &self.subject,
                recipient: &self.recipient,
                issued: self.issued,
                mutual: self.mutual,
                note: &note,
            })
            .expect("introduction serializes"),
        );
        bytes
    }

    /// The checks that need no key: version, note length, and `issued` at
    /// most [`MAX_INTRO_FUTURE_SECS`] past `now` (epoch seconds). There is no
    /// expiry.
    pub fn check_shape(&self, now: u64) -> Result<(), IntroError> {
        if self.v != INTRO_VERSION {
            return Err(IntroError::Version);
        }
        if self
            .note
            .as_deref()
            .is_some_and(|note| note.chars().count() > MAX_INTRO_NOTE_CHARS)
        {
            return Err(IntroError::NoteTooLong);
        }
        if self.issued > now.saturating_add(MAX_INTRO_FUTURE_SECS) {
            return Err(IntroError::Future);
        }
        Ok(())
    }

    /// Parses a token from JSON text (the `MessageEnvelope.introduction` form).
    pub fn from_json(text: &str) -> Result<Self, IntroError> {
        serde_json::from_str(text).map_err(|error| IntroError::Json(error.to_string()))
    }

    /// Parses a token passed as a JSON object or as JSON text (RPC params).
    pub fn from_value(value: &serde_json::Value) -> Result<Self, IntroError> {
        match value {
            serde_json::Value::String(text) => Self::from_json(text),
            other => serde_json::from_value(other.clone())
                .map_err(|error| IntroError::Json(error.to_string())),
        }
    }

    /// The token as compact JSON text.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("introduction serializes")
    }
}

/// Who introduced a contact (`Peer.introduced_by`), from the token it was
/// added with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntroducedBy {
    pub peer_id: String,
    pub name: String,
    pub issued: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(default)]
    pub mutual: bool,
}

impl From<&Introduction> for IntroducedBy {
    fn from(intro: &Introduction) -> Self {
        Self {
            peer_id: intro.introducer.clone(),
            name: intro.introducer_name.clone(),
            issued: intro.issued,
            note: intro.note.clone().filter(|note| !note.is_empty()),
            mutual: intro.mutual,
        }
    }
}

impl From<&RequestIntroduction> for IntroducedBy {
    fn from(intro: &RequestIntroduction) -> Self {
        Self {
            peer_id: intro.introducer.clone(),
            name: intro.introducer_name.clone(),
            issued: intro.issued,
            note: intro.note.clone(),
            mutual: intro.mutual,
        }
    }
}

/// The introduction on a held contact request (`ContactRequest.introduction`):
/// the sender's first messages carried a valid token naming this identity as
/// the subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestIntroduction {
    pub introducer: String,
    pub introducer_name: String,
    /// The introducer is one of this identity's contacts (evaluated when
    /// read: `request.list` refreshes it).
    pub introducer_is_contact: bool,
    /// The token was bound to the sender (`recipient` set). False for a share
    /// with no known recipient ("shared by Don").
    pub addressed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    pub issued: u64,
    #[serde(default)]
    pub mutual: bool,
}

/// The add-link envelope (`idfon://add/<base64url>`): the card plus what
/// rides beside it. `capability` and `auth` are the station/protected-contact
/// members (docs/contact-exchange.md); this build reads `intro`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AddLink {
    pub contact: ContactTicket,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intro: Option<Introduction>,
}

/// The `share` value of a card, peer or identity: `"ask"` (ask before sharing
/// my card) or absent (ok).
pub const SHARE_ASK: &str = "ask";
/// The explicit "ok" spelling RPCs accept and report.
pub const SHARE_OK: &str = "ok";

/// Parses a `share` param: `"ask"` → `Some("ask")`, `"ok"` → `None` (absent
/// means ok). Anything else is an error.
pub fn parse_share(value: &str) -> Result<Option<String>, String> {
    match value {
        SHARE_ASK => Ok(Some(SHARE_ASK.into())),
        SHARE_OK => Ok(None),
        _ => Err("share must be \"ask\" or \"ok\"".into()),
    }
}

/// The reported spelling of a stored `share`: `"ask"` or `"ok"`.
pub fn share_label(share: Option<&str>) -> &'static str {
    if share == Some(SHARE_ASK) {
        SHARE_ASK
    } else {
        SHARE_OK
    }
}
