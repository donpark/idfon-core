//! What a voice says out of a reply: the `IDFON-SPEAK/1` text when present,
//! else the prose before the first envelope. One implementation for every
//! speaking path (the voice agent, the eve bridge's live commentary), so they
//! cannot disagree about what is read aloud (docs/reply-blocks.md).
//!
//! An envelope header is any line `IDFON-<KIND>/1`. That is deliberately
//! wider than [`crate::blocks::ENVELOPE_HEADERS`]: an envelope kind a speaker
//! does not know must still never be read aloud.

/// True for an envelope header line (`IDFON-<KIND>/1`).
pub fn is_envelope_header(line: &str) -> bool {
    let line = line.trim_end();
    line.starts_with("IDFON-") && line.ends_with("/1")
}

/// Drop trailing `IDFON-*/1` envelope blocks before speaking.
pub fn strip_envelopes(text: &str) -> String {
    let mut out = String::new();
    for line in text.lines() {
        let trimmed = line.trim_end();
        if is_envelope_header(trimmed) {
            break;
        }
        out.push_str(line);
        out.push('\n');
    }
    out.trim().to_string()
}

/// Text a voice agent should actually speak: the `IDFON-SPEAK/1` envelope
/// text(s) when present, else the reply with envelopes stripped. The tool lets
/// the model send the spoken form separately, so artifacts and display text are
/// never read aloud.
pub fn speak_text(text: &str) -> String {
    let mut spoken: Vec<String> = Vec::new();
    let mut in_speak = false;
    let mut body = String::new();
    for line in text.lines() {
        let trimmed = line.trim_end();
        if is_envelope_header(trimmed) {
            if in_speak {
                push_speak_json(&body, &mut spoken);
                body.clear();
            }
            in_speak = trimmed == "IDFON-SPEAK/1";
            continue;
        }
        if in_speak {
            body.push_str(line);
            body.push('\n');
        }
    }
    if in_speak {
        push_speak_json(&body, &mut spoken);
    }
    if spoken.is_empty() {
        strip_envelopes(text)
    } else {
        spoken.join(" ")
    }
}

fn push_speak_json(body: &str, out: &mut Vec<String>) {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(body.trim()) {
        if let Some(text) = value.get("text").and_then(serde_json::Value::as_str) {
            let text = text.trim();
            if !text.is_empty() {
                out.push(text.to_string());
            }
        }
    }
}

/// Text a voice agent should actually speak, mid-generation. The streaming
/// twin of [`speak_text`] for the speech-unit `partial` field: tolerant of a
/// half-written message. An incomplete `IDFON-SPEAK/1` block contributes the
/// decoded prefix of its `text` value (a terminated literal and an
/// unterminated one decode to the same prefix); with no speak block started
/// yet the result is the prose before any envelope. On a complete message
/// this equals [`speak_text`].
pub fn speak_text_partial(text: &str) -> String {
    let mut spoken: Vec<String> = Vec::new();
    let mut in_speak = false;
    let mut body = String::new();
    for line in text.lines() {
        let trimmed = line.trim_end();
        if is_envelope_header(trimmed) {
            if in_speak {
                push_speak_json_partial(&body, &mut spoken);
                body.clear();
            }
            in_speak = trimmed == "IDFON-SPEAK/1";
            continue;
        }
        if in_speak {
            body.push_str(line);
            body.push('\n');
        }
    }
    if in_speak {
        push_speak_json_partial(&body, &mut spoken);
    }
    if spoken.is_empty() {
        body_prefix(text)
    } else {
        spoken.join(" ")
    }
}

/// The prose before any envelope block. A trailing `IDFON-` fragment is held
/// back: it may still become an envelope marker (the stripper's rule).
fn body_prefix(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(index) = rest.find('\n') {
        let (line, tail) = rest.split_at(index + 1);
        if is_envelope_header(line) {
            return out;
        }
        out.push_str(line);
        rest = tail;
    }
    if !rest.starts_with("IDFON-") {
        out.push_str(rest);
    }
    out
}

fn push_speak_json_partial(body: &str, out: &mut Vec<String>) {
    if let Some(text) = json_text_prefix(body) {
        let text = text.trim();
        if !text.is_empty() {
            out.push(text.to_string());
        }
    }
}

/// The decoded prefix of the `"text"` string value in a JSON object that may
/// still be half-written. Scans the string literal with JSON escapes; stops
/// at the closing quote or end of input, which yields the same prefix.
/// Shortcut: the first `"text"` occurrence wins (a prose copy of the key
/// inside another value would confuse it).
fn json_text_prefix(body: &str) -> Option<String> {
    let at = body.find("\"text\"")? + "\"text\"".len();
    let rest = body[at..].trim_start();
    let rest = rest.strip_prefix(':')?.trim_start();
    let rest = rest.strip_prefix('"')?;
    let bytes = rest.as_bytes();
    let mut out = String::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if i + 1 < bytes.len() => {
                match bytes[i + 1] {
                    b'"' => { out.push('"'); i += 2; }
                    b'\\' => { out.push('\\'); i += 2; }
                    b'/' => { out.push('/'); i += 2; }
                    b'n' => { out.push('\n'); i += 2; }
                    b't' => { out.push('\t'); i += 2; }
                    b'r' => { out.push('\r'); i += 2; }
                    b'u' if i + 6 <= bytes.len() => {
                        if let Some(c) = u32::from_str_radix(&rest[i + 2..i + 6], 16).ok().and_then(char::from_u32) {
                            out.push(c);
                        }
                        i += 6;
                    }
                    _ => { i += 1; }
                }
            }
            b'"' => break,
            _ => {
                let ch = rest[i..].chars().next()?;
                out.push(ch);
                i += ch.len_utf8();
            }
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn speaks_the_speak_envelope_else_the_prose() {
        assert_eq!(speak_text("Hi.\nIDFON-SPEAK/1\n{\"text\":\"Hello there.\"}"), "Hello there.");
        assert_eq!(speak_text("Here you go.\nIDFON-DATA/1\nticket=x\nsize=1"), "Here you go.");
        // Unknown envelope kinds are never read aloud either.
        assert_eq!(strip_envelopes("Done.\nIDFON-FUTURE-THING/1\n{}"), "Done.");
    }

    #[test]
    fn partial_matches_complete_on_a_finished_reply() {
        let reply = "One sec.\nIDFON-SPEAK/1\n{\"text\":\"All set.\"}";
        assert_eq!(speak_text_partial(reply), speak_text(reply));
        assert_eq!(speak_text_partial("IDFON-SPEAK/1\n{\"text\": \"Let me ch"), "Let me ch");
    }

    #[test]
    fn header_rule() {
        assert!(is_envelope_header("IDFON-CARD-REQUEST/1\r"));
        assert!(!is_envelope_header("IDFON-SPEAK/2"));
        assert!(!is_envelope_header("hello IDFON-SPEAK/1"));
    }
}
