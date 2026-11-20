//! Every ALPN idfon speaks, in one place.
//!
//! The form is `idfon/<protocol>/<version>`. Two older protocols use a
//! different spelling on the wire (`idfon-health/1`, `idfon-chat/1`); their
//! bytes cannot change without breaking deployed apps, so they are listed
//! here under their own names and must not be copied as a pattern.
//!
//! Crates re-export the constant they serve (e.g. `idfon_h1::ALPN`), so code
//! written against those names keeps working.

/// Signed message frames (the daemon's message path).
pub const MESSAGE: &[u8] = b"idfon/message/1";
/// Account state sync between one account's devices.
pub const SYNC: &[u8] = b"idfon/sync/1";
/// MCP transport binding (`docs/mcp-transport.md`).
pub const MCP: &[u8] = b"idfon/mcp/1";
/// HTTP/1.1 over iroh (`idfon-h1`).
pub const HTTP1: &[u8] = b"idfon/http1/1";
/// HTTP/3 over iroh (`idfon-h3`).
pub const HTTP3: &[u8] = b"idfon/http3/1";
/// WebSocket-shaped message channels (`idfon-channel`).
pub const CHANNEL: &[u8] = b"idfon/channel/1";
/// Connection health probes. Legacy spelling: do not copy.
pub const HEALTH: &[u8] = b"idfon-health/1";
/// FFI chat example. Legacy spelling: do not copy.
pub const CHAT: &[u8] = b"idfon-chat/1";

/// Every registered ALPN, for diagnostics and duplicate checks.
pub const ALL: &[&[u8]] = &[MESSAGE, SYNC, MCP, HTTP1, HTTP3, CHANNEL, HEALTH, CHAT];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alpns_are_unique_and_follow_the_form() {
        for (i, a) in ALL.iter().enumerate() {
            for b in &ALL[i + 1..] {
                assert_ne!(a, b);
            }
            let text = std::str::from_utf8(a).unwrap();
            let legacy = *a == HEALTH || *a == CHAT;
            assert!(legacy || (text.starts_with("idfon/") && text.matches('/').count() == 2), "{text}");
        }
    }
}
