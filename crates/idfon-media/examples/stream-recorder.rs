//! Record decoded audio from an iroh-live broadcast ticket to
//! `stream-recording.wav` (48 kHz mono), through `idfon_media::live`.

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let ticket = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("usage: stream-recorder <TICKET> [SECONDS]"))?;
    let seconds = args.next().map(|s| s.parse()).transpose()?.unwrap_or(15);
    let stats = idfon_media::live::listen_to_wav(
        &ticket,
        std::path::Path::new("stream-recording.wav"),
        seconds,
        true,
    )?;
    println!("{stats:?}");
    Ok(())
}
