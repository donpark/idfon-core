//! Thin `idfond` launcher. All daemon logic lives in the shared
//! `libiroh_c_ffi.dylib` (entry: `idfon_daemon_run`, implemented in
//! `crates/iroh-c-ffi/src/daemon.rs` over the `idfon-daemon` crate);
//! this binary only parses arguments, picks paths, and calls into the dylib.

use std::ffi::CString;
use std::os::raw::c_char;

use clap::Parser;
use idfon_config::paths;

extern "C" {
    fn idfon_daemon_run(socket_path: *const c_char, data_dir: *const c_char, transport: *const c_char) -> i32;
    fn idfon_relay_settings_set(urls: *const c_char, token: *const c_char, only: bool);
}

/// The idfon daemon. Usually started for you by `idfon` or the apps.
#[derive(Parser)]
#[command(name = "idfond", version)]
struct Args {
    /// Profile: picks both the socket and the data dir (default: IDFON_PROFILE)
    #[arg(long)]
    profile: Option<String>,
    /// Socket path (default: /tmp/idfon[-PROFILE]/idfond.sock)
    #[arg(long)]
    socket: Option<String>,
    /// Data dir for identities and peers (default: the profile's durable dir;
    /// see `idfon config path`)
    #[arg(long = "data-dir")]
    data_dir: Option<String>,
    /// Transport: iroh (default) or fake (tests)
    #[arg(long)]
    transport: Option<String>,
    /// Relay URLs for this run (beats config and IDFON_RELAY_URLS)
    #[arg(long = "relay-urls")]
    relay_urls: Option<String>,
    /// Relay bearer token for this run
    #[arg(long = "relay-token")]
    relay_token: Option<String>,
    /// Relay only for this run (no direct IP paths)
    #[arg(long = "relay-only")]
    relay_only: bool,
}

fn main() {
    let args = Args::parse();
    let profile = paths::profile(args.profile.as_deref());
    let socket = args
        .socket
        .unwrap_or_else(|| paths::socket(profile.as_deref()).display().to_string());
    let data_dir = match args.data_dir {
        Some(dir) => dir,
        None => {
            let data = paths::data_dir(profile.as_deref());
            // One-time move of a pre-move profile out of /tmp, which macOS
            // clears: the identity keys must survive a reboot.
            let legacy = paths::legacy_data_dir(profile.as_deref());
            match paths::migrate_legacy_data(&legacy, &data) {
                Ok(0) => {}
                Ok(copied) => eprintln!(
                    "idfond: migrated {copied} items from {} to {}",
                    legacy.display(),
                    data.display()
                ),
                Err(error) => eprintln!("idfond: migration from {} failed: {error}", legacy.display()),
            }
            data.display().to_string()
        }
    };
    let socket_z = cstring(&socket, "--socket");
    let data_z = cstring(&data_dir, "--data-dir");
    let transport_z = args.transport.as_deref().map(|value| cstring(value, "--transport"));
    let relay_urls_z = args.relay_urls.as_deref().map(|value| cstring(value, "--relay-urls"));
    let relay_token_z = args.relay_token.as_deref().map(|value| cstring(value, "--relay-token"));
    if relay_urls_z.is_some() || relay_token_z.is_some() || args.relay_only {
        let empty = cstring("", "--relay");
        let as_ptr = |value: &Option<CString>| {
            value.as_ref().map_or(empty.as_ptr(), |value| value.as_ptr())
        };
        unsafe {
            idfon_relay_settings_set(as_ptr(&relay_urls_z), as_ptr(&relay_token_z), args.relay_only);
        }
    }
    let code = unsafe {
        idfon_daemon_run(
            socket_z.as_ptr(),
            data_z.as_ptr(),
            transport_z.as_ref().map_or(std::ptr::null(), |value| value.as_ptr()),
        )
    };
    if code != 0 {
        // The daemon core prints the error itself before returning nonzero.
        std::process::exit(1);
    }
}

fn cstring(value: &str, flag: &str) -> CString {
    CString::new(value).unwrap_or_else(|_| {
        eprintln!("idfond: {flag} must not contain NUL bytes");
        std::process::exit(2);
    })
}
