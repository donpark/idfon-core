//! Minimal vCard 3.0 wrapper for contact-ticket exchange.
//!
//! A card is `FN`/`N` plus the ticket twice: `X-IDFON-TICKET` (ticket JSON,
//! vCard text escaping + line folding) and `URL:idfon://add/<base64url>` (the
//! deep link that survives a round-trip through the Contacts app — AirDrop
//! hands `.vcf` to Contacts, which may strip unknown X- properties but keeps
//! standard `URL` fields, where the user taps it and the `idfon://` scheme
//! opens the app). Cards without a ticket in either slot are skipped on
//! import, so a plain Address Book export imports cleanly.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

use crate::{AddLink, ContactTicket, Introduction};

/// Deep-link shape for the tap-through ticket; must match the iOS/macOS
/// `idfon://` handler (docs/idfon-gateway.md).
const TICKET_URL_PREFIX: &str = "idfon://add/";
/// The universal-link form of the same link (`https://idfon.net/go/<rest>`
/// opens as `idfon://<rest>` in both apps).
const UNIVERSAL_URL_PREFIX: &str = "https://idfon.net/go/add/";

/// Escapes a vCard text value (RFC 6350 §3.4). `\r` is dropped: values hold
/// `\n` line breaks only.
fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            ';' => out.push_str("\\;"),
            ',' => out.push_str("\\,"),
            '\n' => out.push_str("\\n"),
            '\r' => {}
            ch => out.push(ch),
        }
    }
    out
}

/// Reverses [`escape`]; `\n`/`\N` becomes a line break.
fn unescape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') | Some('N') => out.push('\n'),
            Some(other) => out.push(other),
            None => {}
        }
    }
    out
}

/// Folds a content line to 75 octets (RFC 6350); continuations start with a
/// single space. Splits on character boundaries so UTF-8 names survive.
fn fold(line: &str) -> String {
    let mut out = String::new();
    let mut rest = line;
    let mut limit = 75;
    while rest.len() > limit {
        let mut split = limit;
        while !rest.is_char_boundary(split) {
            split -= 1;
        }
        out.push_str(&rest[..split]);
        out.push_str("\r\n ");
        rest = &rest[split..];
        limit = 74;
    }
    out.push_str(rest);
    out
}

fn link_for_json(json: &str) -> String {
    format!("{TICKET_URL_PREFIX}{}", URL_SAFE_NO_PAD.encode(json.as_bytes()))
}

/// The `idfon://add/<base64url>` deep link for a ticket — the same URL the
/// card carries in its `URL` property.
pub fn contact_ticket_link(ticket: &ContactTicket) -> String {
    link_for_json(&serde_json::to_string(ticket).expect("contact ticket serializes"))
}

/// The add link for a card with an introduction: the envelope
/// `{"contact": <card>, "intro": <token>}`, base64url, as
/// `idfon://add/<base64url>` (docs/contact-exchange.md).
pub fn add_link(link: &AddLink) -> String {
    link_for_json(&serde_json::to_string(link).expect("add link serializes"))
}

/// Parses an `idfon://add/<base64url>` deep link back into its ticket. The
/// payload may be the bare ticket or an add envelope (`{ "contact": … }`).
pub fn contact_ticket_from_link(link: &str) -> Result<ContactTicket, String> {
    add_link_from_str(link).map(|link| link.contact)
}

/// Parses an add link (`idfon://add/<base64url>` or its universal form
/// `https://idfon.net/go/add/<base64url>`): a bare contact ticket, or an
/// envelope whose `contact` is the ticket and whose optional `intro` is an
/// introduction token. An `intro` that does not parse is dropped (it is
/// context, never needed to add the contact); validity is checked by the
/// daemon.
pub fn add_link_from_str(link: &str) -> Result<AddLink, String> {
    let link = link.trim();
    let encoded = link
        .strip_prefix(TICKET_URL_PREFIX)
        .or_else(|| link.strip_prefix(UNIVERSAL_URL_PREFIX))
        .ok_or_else(|| format!("expected {TICKET_URL_PREFIX}<base64url>"))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|error| format!("invalid link encoding: {error}"))?;
    add_link_from_json(&bytes)
}

fn add_link_from_json(bytes: &[u8]) -> Result<AddLink, String> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|error| format!("invalid contact ticket: {error}"))?;
    let (contact, intro) = match value.get("contact") {
        Some(contact) => (contact.clone(), value.get("intro").cloned()),
        None => (value, None),
    };
    let contact = serde_json::from_value(contact)
        .map_err(|error| format!("invalid contact ticket: {error}"))?;
    let intro = intro
        .filter(|intro| !intro.is_null())
        .and_then(|intro| Introduction::from_value(&intro).ok());
    Ok(AddLink { contact, intro })
}

/// Wraps a contact ticket in a vCard 3.0 card (CRLF line endings).
pub fn contact_ticket_vcard(ticket: &ContactTicket) -> String {
    let json = serde_json::to_string(ticket).expect("contact ticket serializes");
    vcard_with_url(ticket, &json, &link_for_json(&json))
}

/// A vCard for an add link with an introduction: `X-IDFON-TICKET` is the
/// card, `URL` is the full add link (card + intro).
pub fn add_link_vcard(link: &AddLink) -> String {
    let json = serde_json::to_string(&link.contact).expect("contact ticket serializes");
    vcard_with_url(&link.contact, &json, &add_link(link))
}

fn vcard_with_url(ticket: &ContactTicket, json: &str, url: &str) -> String {
    let name = ticket
        .label
        .as_deref()
        .filter(|name| !name.is_empty())
        .unwrap_or(&ticket.account_id);
    let url = url.to_string();
    let mut out = String::from("BEGIN:VCARD\r\nVERSION:3.0\r\n");
    for (key, value) in [
        ("FN", escape(name)),
        ("N", format!(";{};;;", escape(name))),
        ("URL", url),
        ("X-IDFON-TICKET", escape(json)),
    ] {
        out.push_str(&fold(&format!("{key}:{value}")));
        out.push_str("\r\n");
    }
    out.push_str("END:VCARD\r\n");
    out
}

/// Extracts every idfon contact ticket in a vCard file, one per card. `FN`
/// (the name the sharer chose) becomes the ticket label. Errors only when no
/// card carries a ticket.
pub fn vcard_contact_tickets(text: &str) -> Result<Vec<ContactTicket>, String> {
    vcard_add_links(text).map(|links| links.into_iter().map(|link| link.contact).collect())
}

/// Like [`vcard_contact_tickets`], with each card's introduction (from the
/// `URL` add link) when it carries one.
pub fn vcard_add_links(text: &str) -> Result<Vec<AddLink>, String> {
    let mut links = Vec::new();
    for lines in cards(text) {
        let from_url = url_json(&lines).and_then(|json| add_link_from_json(json.as_bytes()).ok());
        let mut link = match property(&lines, "X-IDFON-TICKET") {
            Some(json) => AddLink {
                contact: serde_json::from_str(&unescape(&json))
                    .map_err(|error| format!("invalid X-IDFON-TICKET: {error}"))?,
                intro: from_url.and_then(|link| link.intro),
            },
            None => match url_json(&lines) {
                Some(json) => add_link_from_json(json.as_bytes())
                    .map_err(|error| format!("invalid X-IDFON-TICKET: {error}"))?,
                None => continue,
            },
        };
        link.contact.label = property(&lines, "FN")
            .map(|value| unescape(&value))
            .filter(|name| !name.is_empty())
            .or(link.contact.label);
        links.push(link);
    }
    if links.is_empty() {
        return Err("no X-IDFON-TICKET in vcard".into());
    }
    Ok(links)
}

/// Unfolds physical lines and groups them into BEGIN:VCARD…END:VCARD blocks.
/// Lines outside cards are ignored.
fn cards(text: &str) -> Vec<Vec<String>> {
    let mut blocks = Vec::new();
    let mut current: Option<Vec<String>> = None;
    for raw in text.split('\n') {
        let line = raw.strip_suffix('\r').unwrap_or(raw);
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some(last) = current.as_mut().and_then(|lines| lines.last_mut()) {
                last.push_str(&line[1..]);
            }
            continue;
        }
        if line.eq_ignore_ascii_case("BEGIN:VCARD") {
            current = Some(Vec::new());
        } else if line.eq_ignore_ascii_case("END:VCARD") {
            if let Some(lines) = current.take() {
                blocks.push(lines);
            }
        } else if let Some(lines) = current.as_mut() {
            lines.push(line.to_string());
        }
    }
    blocks
}

/// The JSON in the `idfon://add/<base64url>` URL: the ticket, or an add
/// envelope. Used when the card went through the Contacts app (unknown X-
/// properties may be stripped there; standard `URL` fields survive), and for
/// the introduction beside the card. The URL prefix is matched exactly: we
/// only ever hand-emit lowercase scheme/verb.
fn url_json(lines: &[String]) -> Option<String> {
    let encoded = property(lines, "URL")?.strip_prefix(TICKET_URL_PREFIX)?.to_string();
    let bytes = URL_SAFE_NO_PAD.decode(unescape(&encoded)).ok()?;
    String::from_utf8(bytes).ok()
}

/// First value of a property in a card (property names are case-insensitive).
fn property(lines: &[String], name: &str) -> Option<String> {
    lines.iter().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        let key = key.split(';').next().unwrap_or_default();
        key.eq_ignore_ascii_case(name)
            .then(|| value.to_string())
    })
}
