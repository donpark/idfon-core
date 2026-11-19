//! 1:1 audio on one machine on the upstream iroh-live stack: a file publisher
//! (`LivePublisher`) and a ticket subscriber (`listen_to_wav`).
//!
//! Relay on: a ticket carries only the endpoint id, and with the relay off
//! nothing resolved it here (DNS has no record; mDNS did not answer within the
//! dial window in this environment). With the relay on, the id resolves
//! through n0's relay and DNS. Needs internet access.

use std::path::Path;

fn write_tone(path: &Path, seconds: f32) {
    let rate = 48_000u32;
    let samples: Vec<i16> = (0..(rate as f32 * seconds) as usize)
        .map(|i| {
            let t = i as f32 / rate as f32;
            ((t * 440.0 * std::f32::consts::TAU).sin() * 0.5 * 32767.0) as i16
        })
        .collect();
    let data_len = (samples.len() * 2) as u32;
    let mut wav = Vec::with_capacity(44 + data_len as usize);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36 + data_len).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&rate.to_le_bytes());
    wav.extend_from_slice(&(rate * 2).to_le_bytes());
    wav.extend_from_slice(&2u16.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_len.to_le_bytes());
    for sample in samples {
        wav.extend_from_slice(&sample.to_le_bytes());
    }
    std::fs::write(path, wav).unwrap();
}

fn rms_of_wav(path: &Path) -> f64 {
    let bytes = std::fs::read(path).unwrap();
    let samples: Vec<f64> = bytes[44..]
        .chunks_exact(2)
        .map(|pair| f64::from(i16::from_le_bytes([pair[0], pair[1]])) / 32768.0)
        .collect();
    if samples.is_empty() {
        return 0.0;
    }
    (samples.iter().map(|s| s * s).sum::<f64>() / samples.len() as f64).sqrt()
}

#[test]
fn file_publisher_reaches_ticket_subscriber_over_loopback() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_default())
        .with_test_writer()
        .try_init();
    let dir = std::env::temp_dir().join(format!("idfon-loopback-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let tone = dir.join("tone.wav");
    let out = dir.join("heard.wav");
    write_tone(&tone, 2.0);

    let (publisher, ticket) =
        idfon_media::live::LivePublisher::start(&tone, true, "loopback", true).unwrap();
    assert!(ticket.starts_with("ilv1"), "ticket = {ticket}");
    let stats = idfon_media::live::listen_to_wav(&ticket, &out, 4, true);
    publisher.stop();
    let stats = stats.unwrap();
    let rms = rms_of_wav(&out);
    println!("stats = {stats:?}, rms = {rms:.3}");
    assert!(stats.packets > 0, "no audio frames arrived: {stats:?}");
    assert!(stats.duration_ms >= 1_000, "too little audio: {stats:?}");
    // A 0.5-amplitude sine has RMS ~0.35; Opus and resampling keep it close.
    assert!(rms > 0.1, "decoded audio is silent: rms = {rms}");
}
