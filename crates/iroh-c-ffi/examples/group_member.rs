//! A headless group-call member, for `scripts/group-call-e2e.sh`.
//!
//! Drives the same FFI the apps use, with its own daemon for signaling:
//! publishes a tone, joins the room's call through `idfon room call join`,
//! follows `room call status` and keeps the mix matched to the roster
//! (`media_live_add_member` / `media_live_remove_member`), and records the mix
//! from the playback callback. At the end it reports which tones it heard.
//!
//! ```text
//! group_member --idfon <cli> --socket <sock> --room <room> --hz 440 \
//!   --seconds 12 --expect 660,990 [--leave-after 6]
//! ```
//!
//! Prints one JSON line: `{"heard": {...}, "members_max": n, ...}`; exits 1
//! when an expected tone is missing from the last two seconds of the mix (or,
//! with `--expect-absent`, a tone that should have left is still there).

use std::ffi::CString;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use iroh_c_ffi::media;

static MIX: Mutex<Vec<f32>> = Mutex::new(Vec::new());

unsafe extern "C" fn collect(
    _: *const std::ffi::c_void,
    pcm: *const u8,
    len: usize,
    _rate: u32,
    channels: u32,
    _: u64,
) {
    let bytes = unsafe { std::slice::from_raw_parts(pcm, len) };
    let samples: Vec<f32> =
        bytes.chunks_exact(4).map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]])).collect();
    let mut mix = MIX.lock().unwrap();
    // Fold to mono for the tone check.
    let width = channels.max(1) as usize;
    mix.extend(samples.chunks_exact(width).map(|f| f.iter().sum::<f32>() / width as f32));
}

/// Power of `hz` in mono 48 kHz `samples` (Goertzel), normalized by length.
fn tone_power(samples: &[f32], hz: f32) -> f32 {
    let coeff = 2.0 * (2.0 * std::f32::consts::PI * hz / 48_000.0).cos();
    let (mut s1, mut s2) = (0.0f32, 0.0f32);
    for &x in samples {
        let s0 = x + coeff * s1 - s2;
        s2 = s1;
        s1 = s0;
    }
    (s1 * s1 + s2 * s2 - coeff * s1 * s2) / (samples.len().max(1) as f32).powi(2)
}

struct Args {
    idfon: String,
    socket: String,
    room: String,
    hz: f32,
    seconds: u64,
    expect: Vec<f32>,
    expect_absent: Vec<f32>,
    leave_after: Option<u64>,
    codec: String,
}

fn args() -> Args {
    let mut it = std::env::args().skip(1);
    let mut args = Args {
        idfon: "idfon".into(),
        socket: String::new(),
        room: String::new(),
        hz: 440.0,
        seconds: 12,
        expect: Vec::new(),
        expect_absent: Vec::new(),
        leave_after: None,
        codec: "opus".into(),
    };
    let list = |v: String| v.split(',').filter(|s| !s.is_empty()).map(|s| s.parse().unwrap()).collect();
    while let Some(flag) = it.next() {
        let value = it.next().unwrap_or_default();
        match flag.as_str() {
            "--idfon" => args.idfon = value,
            "--socket" => args.socket = value,
            "--room" => args.room = value,
            "--hz" => args.hz = value.parse().unwrap(),
            "--seconds" => args.seconds = value.parse().unwrap(),
            "--expect" => args.expect = list(value),
            "--expect-absent" => args.expect_absent = list(value),
            "--leave-after" => args.leave_after = Some(value.parse().unwrap()),
            "--codec" => args.codec = value,
            other => panic!("unknown flag {other}"),
        }
    }
    args
}

fn cli(args: &Args, rest: &[&str]) -> serde_json::Value {
    let out = Command::new(&args.idfon)
        .args(["--socket", &args.socket])
        .args(rest)
        .arg("--json")
        .output()
        .expect("run idfon");
    serde_json::from_slice(&out.stdout).unwrap_or(serde_json::Value::Null)
}

fn take_string(ptr: safer_ffi::prelude::char_p::Box) -> String {
    ptr.to_str().to_string()
}

fn main() {
    let args = args();
    media::media_audio_set_playback_cb(std::ptr::null(), collect);

    // Publish a tone through the push source, as the apps push their mic.
    let source = CString::new("push").unwrap();
    let codec = CString::new(args.codec.clone()).unwrap();
    let ticket = take_string(media::media_live_start_with_profile(
        1,
        0,
        source.as_c_str().into(),
        codec.as_c_str().into(),
        24_000,
        1,
    ));
    assert!(!ticket.is_empty(), "publish failed: {}", take_string(media::media_live_last_error()));
    let stop = Arc::new(AtomicBool::new(false));
    let pusher = {
        let stop = stop.clone();
        let hz = args.hz;
        std::thread::spawn(move || {
            let started = Instant::now();
            let mut n = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let frame: Vec<f32> = (0..480)
                    .map(|i| (2.0 * std::f32::consts::PI * hz * (n + i) as f32 / 24_000.0).sin() * 0.25)
                    .collect();
                media::media_audio_push_frames(frame.as_ptr(), 480, 1);
                n += 480;
                // Real time: 20 ms per push, against the start, not the last push.
                let due = started + Duration::from_micros(n as u64 * 1_000_000 / 24_000);
                if let Some(wait) = due.checked_duration_since(Instant::now()) {
                    std::thread::sleep(wait);
                }
            }
        })
    };
    let addr = take_string(media::media_live_endpoint_addr());
    let joined = cli(
        &args,
        &["room", "call", "join", &args.room, "--ticket", &ticket, "--addr", &addr, "--audio-codec", &args.codec],
    );
    eprintln!("[group_member {}] joined: {}", args.hz, joined["result"]["call"]["people"]);

    // Follow the roster: add new members, drop ones that left.
    let mut playing: std::collections::BTreeMap<String, String> = Default::default();
    let mut members_max = 0usize;
    let started = Instant::now();
    let mut left = false;
    while started.elapsed() < Duration::from_secs(args.seconds) {
        if let Some(after) = args.leave_after {
            if !left && started.elapsed() >= Duration::from_secs(after) {
                cli(&args, &["room", "call", "leave", &args.room]);
                media::media_live_unsubscribe();
                media::media_live_stop();
                stop.store(true, Ordering::Relaxed);
                left = true;
                eprintln!("[group_member {}] left", args.hz);
            }
        }
        if !left {
            let status = cli(&args, &["room", "call", "status", &args.room]);
            let roster: std::collections::BTreeMap<String, (String, String)> = status["result"]["call"]["members"]
                .as_array()
                .map(|members| {
                    members
                        .iter()
                        .filter_map(|m| {
                            Some((
                                m["peer"].as_str()?.to_string(),
                                (m["ticket"].as_str()?.to_string(), m["addr"].as_str().unwrap_or("").to_string()),
                            ))
                        })
                        .collect()
                })
                .unwrap_or_default();
            members_max = members_max.max(roster.len());
            for (peer, ticket) in playing.clone() {
                if roster.get(&peer).map(|(t, _)| t) != Some(&ticket) {
                    let t = CString::new(ticket).unwrap();
                    media::media_live_remove_member(t.as_c_str().into());
                    playing.remove(&peer);
                    eprintln!("[group_member {}] member left {}", args.hz, &peer[..8]);
                }
            }
            for (peer, (ticket, addr)) in &roster {
                if playing.contains_key(peer) {
                    continue;
                }
                let t = CString::new(ticket.clone()).unwrap();
                let a = CString::new(addr.clone()).unwrap();
                let result = media::media_live_add_member(t.as_c_str().into(), a.as_c_str().into());
                eprintln!("[group_member {}] member joined {} result={result}", args.hz, &peer[..8]);
                if result == 0 || result == 2 {
                    playing.insert(peer.clone(), ticket.clone());
                }
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    // Judge the last two seconds of the mix.
    let mix = MIX.lock().unwrap().clone();
    let tail = &mix[mix.len().saturating_sub(96_000)..];
    let heard = |hz: f32| tone_power(tail, hz) > 1e-4;
    let mut report = serde_json::Map::new();
    for hz in args.expect.iter().chain(&args.expect_absent) {
        report.insert(format!("{hz}"), serde_json::json!(heard(*hz)));
    }
    let ok = args.expect.iter().all(|hz| heard(*hz)) && args.expect_absent.iter().all(|hz| !heard(*hz));
    println!(
        "{}",
        serde_json::json!({
            "hz": args.hz, "heard": report, "members_max": members_max,
            "mix_samples": mix.len(), "left": left, "ok": ok,
        })
    );
    if !left {
        cli(&args, &["room", "call", "leave", &args.room]);
        media::media_live_unsubscribe();
        media::media_live_stop();
        stop.store(true, Ordering::Relaxed);
    }
    let _ = pusher.join();
    std::process::exit(if ok { 0 } else { 1 });
}
