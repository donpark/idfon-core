use idfon_protocol::vcf::{contact_ticket_vcard, vcard_contact_tickets};
use idfon_protocol::ContactTicket;

fn ticket() -> ContactTicket {
    ContactTicket {
        version: 1,
        account_id: "account-1".into(),
        endpoint_id: "endpoint-1".into(),
        // Long enough to force folded lines.
        endpoint_addr: format!(
            r#"{{"node_id":"abc","relay_url":"https://relay.example.org","direct_addresses":["192.0.2.1:1234","[2001:db8::1]:1234"],"secret":"{}"}}"#,
            "x".repeat(200)
        ),
        label: Some("Ada; Lovelace, Jr\nEsq".into()),
        device_class: Some("phone".into()),
        capabilities: vec!["chat".into(), "live_audio".into()],
        share: None,
    }
}

#[test]
fn round_trips_folding_escaping_and_json() {
    let original = ticket();
    let vcard = contact_ticket_vcard(&original);
    let parsed = vcard_contact_tickets(&vcard).expect("round trip");
    assert_eq!(parsed, vec![original]);
}

#[test]
fn folds_long_lines_under_75_octets() {
    let vcard = contact_ticket_vcard(&ticket());
    assert!(vcard.split("\r\n").all(|line| {
        line.starts_with(' ') || line.len() <= 75
    }));
}

#[test]
fn imports_many_cards_and_skips_foreign_ones() {
    let mut text = contact_ticket_vcard(&ticket());
    text.push_str("BEGIN:VCARD\r\nVERSION:3.0\r\nFN:No Ticket\r\nN:;No Ticket;;;\r\nEND:VCARD\r\n");
    let mut second = ticket();
    second.account_id = "account-2".into();
    text.push_str(&contact_ticket_vcard(&second));
    let parsed = vcard_contact_tickets(&text).expect("mixed import");
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0].account_id, "account-1");
    assert_eq!(parsed[1].account_id, "account-2");
}

#[test]
fn export_falls_back_to_the_account_id_for_the_name() {
    let mut card = ticket();
    card.label = None;
    assert!(contact_ticket_vcard(&card).contains("FN:account-1"));
}

#[test]
fn fn_wins_over_the_ticket_label_on_import() {
    let mut card = ticket();
    card.label = Some("Old Name".into());
    let vcard = contact_ticket_vcard(&card).replace("FN:Old Name", "FN:New Name");
    let parsed = vcard_contact_tickets(&vcard).expect("renamed card");
    assert_eq!(parsed[0].label.as_deref(), Some("New Name"));
}

#[test]
fn rejects_a_file_without_idfon_cards() {
    let text = "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:No Ticket\r\nEND:VCARD\r\n";
    assert!(vcard_contact_tickets(text).is_err());
}

/// Simulates an AirDrop -> Contacts round-trip: the app drops the unknown X-
/// property (with its folded continuation lines) but keeps standard fields.
fn contacts_strips_x_property(card: &str) -> String {
    let mut kept = Vec::new();
    let mut dropping = false;
    for line in card.split("\r\n") {
        if line.starts_with(' ') {
            if !dropping {
                kept.push(line);
            }
        } else {
            dropping = line.starts_with("X-IDFON-TICKET:");
            if !dropping {
                kept.push(line);
            }
        }
    }
    kept.join("\r\n")
}

#[test]
fn export_carries_the_deep_link() {
    assert!(contact_ticket_vcard(&ticket()).contains("URL:idfon://add/"));
}

#[test]
fn import_survives_contacts_stripping_the_x_property() {
    let stripped = contacts_strips_x_property(&contact_ticket_vcard(&ticket()));
    assert!(!stripped.contains("X-IDFON-TICKET"));
    let parsed = vcard_contact_tickets(&stripped).expect("URL-only card");
    assert_eq!(parsed, vec![ticket()]);
}

#[test]
fn url_only_card_still_takes_the_name_from_fn() {
    let mut card = ticket();
    card.label = Some("Old Name".into());
    let stripped = contacts_strips_x_property(&contact_ticket_vcard(&card))
        .replace("FN:Old Name", "FN:New Name");
    let parsed = vcard_contact_tickets(&stripped).expect("renamed URL card");
    assert_eq!(parsed[0].label.as_deref(), Some("New Name"));
}

#[test]
fn garbage_in_the_url_slot_is_skipped() {
    let text = "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Bad\r\nURL:idfon://add/not-base64!!\r\nEND:VCARD\r\n";
    assert!(vcard_contact_tickets(text).is_err());
}

fn intro() -> idfon_protocol::Introduction {
    idfon_protocol::Introduction {
        v: 1,
        introducer: "a".repeat(64),
        introducer_name: "Don".into(),
        subject: "account-1".into(),
        recipient: None,
        issued: 1_795_575_000,
        mutual: false,
        note: Some("Stella does our taxes".into()),
        sig: "0".repeat(128),
    }
}

fn add_link_value() -> idfon_protocol::AddLink {
    idfon_protocol::AddLink { contact: ticket(), intro: Some(intro()) }
}

#[test]
fn the_add_link_is_the_contact_and_intro_envelope_in_base64url() {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    let link = idfon_protocol::vcf::add_link(&add_link_value());
    let payload = link.strip_prefix("idfon://add/").expect("idfon://add/ link");
    let json: serde_json::Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).unwrap()).unwrap();
    assert_eq!(json["contact"], serde_json::to_value(ticket()).unwrap());
    assert_eq!(json["intro"], serde_json::to_value(intro()).unwrap());
    assert_eq!(json.as_object().unwrap().len(), 2);
    // Parses back; the bare-ticket readers take the card out of it.
    assert_eq!(idfon_protocol::vcf::add_link_from_str(&link).unwrap(), add_link_value());
    assert_eq!(idfon_protocol::vcf::contact_ticket_from_link(&link).unwrap(), ticket());
    // A bare card link (contact.export) has no intro.
    let bare = idfon_protocol::vcf::contact_ticket_link(&ticket());
    assert_eq!(idfon_protocol::vcf::add_link_from_str(&bare).unwrap().intro, None);
    // An intro that is not a token is dropped; the card still adds.
    let junk = format!(
        "idfon://add/{}",
        URL_SAFE_NO_PAD.encode(serde_json::json!({"contact": ticket(), "intro": 7}).to_string())
    );
    assert_eq!(idfon_protocol::vcf::add_link_from_str(&junk).unwrap().intro, None);
}

#[test]
fn an_intro_vcard_carries_the_card_and_the_full_link() {
    let card = idfon_protocol::vcf::add_link_vcard(&add_link_value());
    let unfolded = card.replace("\r\n ", "");
    assert!(unfolded.contains(&format!("URL:{}", idfon_protocol::vcf::add_link(&add_link_value()))));
    let links = idfon_protocol::vcf::vcard_add_links(&card).unwrap();
    assert_eq!(links, vec![add_link_value()]);
    // Through Contacts (X- property stripped): the URL alone gives both.
    let stripped = contacts_strips_x_property(&card);
    assert!(!stripped.contains("X-IDFON-TICKET"));
    assert_eq!(idfon_protocol::vcf::vcard_add_links(&stripped).unwrap(), vec![add_link_value()]);
    assert_eq!(vcard_contact_tickets(&stripped).unwrap(), vec![ticket()]);
}

#[test]
fn the_card_carries_share_ask_and_leaves_ok_out() {
    let mut card = ticket();
    assert!(!serde_json::to_string(&card).unwrap().contains("share"));
    card.share = Some("ask".into());
    assert!(serde_json::to_string(&card).unwrap().contains(r#""share":"ask""#));
    assert_eq!(vcard_contact_tickets(&contact_ticket_vcard(&card)).unwrap(), vec![card]);
}
