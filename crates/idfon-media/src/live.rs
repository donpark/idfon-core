//! Headless live-audio publish/subscribe without a capture device.
//!
//! The publisher streams a media file (symphonia decodes, Opus encodes) over
//! iroh-live; the subscriber decodes a remote broadcast to a 48 kHz mono WAV
//! plus per-packet arrival timings. Both are sync entry points safe to call
//! from non-async contexts: they run their async work on a fresh thread with
//! a local runtime, like the daemon's blob path.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use iroh::protocol::{ProtocolHandler as _, Router};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use iroh_live::layout::LiveLayout;
use iroh_live::media::{audio_encoding, LocalBroadcast, RemoteBroadcast, SubscriptionExt as _};
use iroh_live::{Audience, BroadcastTicket, Node, Reach};
use moq_audio::decode::{Consumer as AudioConsumer, Options as AudioDecodeOptions, Read as AudioRead};
use moq_audio::Format;

use crate::seam::AudioCapture;
use crate::seam::FileAudioCapture;

/// Broadcast name a 1:1 call publishes under the caller's layout root
/// (`live/<caller id>/call`), the same convention as `iroh_live::calls`.
pub const CALL_NAME: &str = "call";

/// A node mounted on its own router: publishes and accepts subscribers.
pub(crate) struct MountedNode {
    pub(crate) node: Node,
    pub(crate) router: Router,
    pub(crate) layout: LiveLayout,
}

impl MountedNode {
    pub(crate) fn spawn(endpoint: Endpoint) -> Self {
        let layout = LiveLayout::default();
        let node = Node::new(endpoint.clone(), layout.config());
        let router = node.mount(Router::builder(endpoint)).spawn();
        Self { node, router, layout }
    }

    pub(crate) fn ticket(&self, name: &str) -> anyhow::Result<String> {
        Ok(self.layout.ticket(self.node.id(), name)?.to_string())
    }

    /// Shuts the node down with the router, then closes the endpoint.
    pub(crate) async fn shutdown(&self) {
        let _ = self.router.shutdown().await;
    }
}

/// Mounts `node` on every MoQ ALPN of the daemon's transport as side channels,
/// for as long as the returned guards live.
pub(crate) fn mount_side_channels(
    transport: &idfon_core::transport::IrohTransport,
    node: &Node,
) -> Vec<idfon_core::transport::SideChannelGuard> {
    iroh_live::node::alpns()
        .into_iter()
        .map(|alpn| {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<iroh::endpoint::Connection>(8);
            let guard = transport.add_side_channel(alpn, tx);
            let node = node.clone();
            tokio::spawn(async move {
                while let Some(connection) = rx.recv().await {
                    let node = node.clone();
                    tokio::spawn(async move {
                        let _ = node.accept(connection).await;
                    });
                }
            });
            guard
        })
        .collect()
}

/// A running file-source publisher. Holds the live endpoint, router, and
/// broadcast; the tokio runtime is owned by this struct (a runtime dropped
/// after publishing tears down the endpoint/router tasks and leaves the
/// broadcast dead). Call [`LivePublisher::stop`] for graceful shutdown.
pub struct LivePublisher {
    runtime: tokio::runtime::Runtime,
    task: tokio::task::JoinHandle<()>,
    live: Arc<MountedNode>,
}

impl LivePublisher {
    /// Publishes `path` (WAV/MP3/FLAC) as a live broadcast. `loop_playback`
    /// repeats the file indefinitely; otherwise the broadcast ends when the
    /// file does (the publisher stays reachable until stopped). With
    /// `relay` set, the endpoint uses n0's public relays as a fallback
    /// transport; without it, subscribers must reach the endpoint directly
    /// (loopback/LAN) and no relay traffic is generated at all.
    pub fn start(
        path: &Path,
        loop_playback: bool,
        name: &str,
        relay: bool,
    ) -> anyhow::Result<(Self, String)> {
        // The daemon dispatches on tokio workers; runtime creation and
        // block_on must happen on a fresh thread (see blob.rs).
        let path = path.to_path_buf();
        let name = name.to_string();
        let handle = std::thread::spawn(move || {
            Self::start_blocking(path, loop_playback, name, relay)
        });
        handle.join().map_err(|_| anyhow::anyhow!("publisher thread panicked"))?
    }

    fn start_blocking(
        path: PathBuf,
        loop_playback: bool,
        name: String,
        relay: bool,
    ) -> anyhow::Result<(Self, String)> {
        let runtime = tokio::runtime::Runtime::new()?;
        let (tx, rx) = std::sync::mpsc::channel();
        // The task owns the node AND LocalBroadcast and holds them across the
        // pending().await below: dropping the broadcast tears down its
        // producer/catalog, so it must outlive the publish call.
        let task = runtime.spawn(async move {
            let fail = |err: String| {
                let _ = tx.send(Err(err));
            };
            let endpoint = match build_endpoint(relay).await {
                Ok(endpoint) => endpoint,
                Err(err) => return fail(format!("endpoint: {err:#}")),
            };
            let live = Arc::new(MountedNode::spawn(endpoint));
            let broadcast: LocalBroadcast = match live.layout.publish(&live.node, &name) {
                Ok(broadcast) => broadcast,
                Err(err) => return fail(format!("publish: {err:#}")),
            };
            let capture = match FileAudioCapture::open(&path, loop_playback).await {
                Ok(capture) => capture,
                Err(err) => return fail(format!("audio file source: {err:#}")),
            };
            if let Err(err) =
                broadcast.set_audio(Box::new(capture).into_source(), audio_encoding::voice())
            {
                return fail(format!("set audio: {err:#}"));
            }
            let ticket = match live.ticket(&name) {
                Ok(ticket) => ticket,
                Err(err) => return fail(format!("ticket: {err:#}")),
            };
            let _ = tx.send(Ok((live.clone(), ticket)));
            let _broadcast = broadcast;
            // Hold the broadcast open until stop()/shutdown.
            std::future::pending::<()>().await;
        });
        let (live, ticket) = rx
            .recv()
            .map_err(|_| anyhow::anyhow!("publisher task died before answering"))?
            .map_err(|err| anyhow::anyhow!(err))?;
        Ok((Self { runtime, task, live }, ticket))
    }

    /// Graceful shutdown: keeps the endpoint alive until the router drains.
    pub fn stop(self) {
        let _ = std::thread::spawn(move || {
            let live = self.live.clone();
            self.runtime.block_on(async move { live.shutdown().await });
            self.task.abort();
            self.runtime.shutdown_timeout(Duration::from_secs(1));
        })
        .join();
    }
}


/// Result of a [`listen_to_wav`] capture.
#[derive(Debug, Clone)]
pub struct ListenStats {
    /// Decoded audio duration in milliseconds.
    pub duration_ms: u64,
    /// Encoded packets received.
    pub packets: usize,
    /// Standard deviation of packet-arrival intervals (wall clock).
    pub arrival_jitter_ms: f64,
    /// Wall-clock (ms since epoch) when capture started, for latency analysis.
    pub wall_ms: u128,
    /// Time from capture start to an established subscribe session.
    pub subscribe_ms: u64,
    /// Time from capture start to the first packet (startup latency;
    /// UX-facing "how long until audio flows").
    pub startup_ms: u64,
    /// Largest interval between consecutive packets (a stall would show up
    /// here even when the average jitter is tiny).
    pub max_gap_ms: u64,
    /// Arrival gaps longer than 100 ms (would drain any small play buffer).
    pub stalls_over_100ms: u32,
    /// Estimated missing packets from pts-timeline holes.
    pub missing_packets: u32,
    /// Smallest prebuffer that would have played the whole capture without
    /// an underrun, given the observed arrival schedule (ms).
    pub prebuffer_ms: u64,
}

pub(crate) async fn build_endpoint(relay: bool) -> anyhow::Result<Endpoint> {
    // N0 preset: n0 public relays as fallback transport + DNS discovery.
    // N0DisableRelay: no relay transport at all — direct connections only
    // (loopback/LAN tests, and keeps traffic off the rate-limited public
    // relays); DNS address lookup still resolves direct addresses.
    // With relay on, the map follows the resolved relay selection — a
    // self-hosted relay replaces the N0 fallback (docs/relay-config.md).
    let builder = if relay {
        // BBR3 for media publishers, as iroh-live's `MoqPreset` sets.
        idfon_core::relay::endpoint_builder()
            .transport_config(iroh_live::Congestion::default().transport_config())
    } else {
        // iroh-live's preset (BBR3, DNS lookup) plus mDNS, which is what
        // resolves a ticket's bare endpoint id on loopback/LAN with no relay;
        // the relay transport itself stays off.
        iroh_live::EndpointConfig::default()
            .apply(iroh::endpoint::Builder::empty())
            .await
            .relay_mode(iroh::RelayMode::Disabled)
    };
    Ok(builder.bind().await?)
}

/// Subscribes to a live ticket and records up to `seconds` of audio to a
/// 16-bit PCM mono 48 kHz WAV. Retries the initial subscribe: the publisher
/// may not have announced its catalog yet.
pub fn listen_to_wav(
    ticket: &str,
    out: &Path,
    seconds: u64,
    relay: bool,
) -> anyhow::Result<ListenStats> {
    let ticket = ticket.to_string();
    let out = out.to_path_buf();
    let handle = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(listen_wav(&ticket, &out, seconds, relay))
    });
    handle.join().map_err(|_| anyhow::anyhow!("listener thread panicked"))?
}

/// Dials `peer_addr` (a serde_json-serialized iroh `EndpointAddr`, as stored
/// in the daemon's peer registry) and publishes `path`'s audio on the
/// session only. Unlike [`LivePublisher`] there is no ticket: the session
/// is the capability, so no third party can subscribe. Blocks until the
/// peer hangs up (its subscribe session closes) or `seconds` elapses
/// (`None` = wait for hangup). Returns wall-clock time held open.
pub fn stream_to_peer(
    path: &Path,
    loop_playback: bool,
    peer_addr: &str,
    seconds: Option<u64>,
    relay: bool,
) -> anyhow::Result<u64> {
    let path = path.to_path_buf();
    let peer_addr = peer_addr.to_string();
    let handle = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(stream_peer(&path, loop_playback, &peer_addr, seconds, relay))
    });
    handle.join().map_err(|_| anyhow::anyhow!("caller thread panicked"))?
}

async fn stream_peer(
    path_arg: &Path,
    loop_playback: bool,
    peer_addr: &str,
    seconds: Option<u64>,
    relay: bool,
) -> anyhow::Result<u64> {
    let addr: EndpointAddr = serde_json::from_str(peer_addr)
        .map_err(|err| anyhow::anyhow!("invalid peer endpoint address: {err}"))?;
    let peer = addr.id;
    let endpoint = build_endpoint(relay).await?;
    let live = MountedNode::spawn(endpoint);
    // The session is the capability: the call broadcast is visible to the
    // dialed peer only.
    let path = live.layout.path(live.node.id(), CALL_NAME)?;
    let (_publication, broadcast) =
        live.node
            .publish_with(path, Audience::peers([peer]), |producer| {
                LocalBroadcast::try_from(producer.clone())
            })?;
    let capture = FileAudioCapture::open(path_arg, loop_playback).await?;
    broadcast.set_audio(Box::new(capture).into_source(), audio_encoding::voice())?;
    let session = live
        .node
        .connect(addr)
        .await
        .map_err(|err| anyhow::anyhow!("dial call: {err}"))?;
    let base = Instant::now();
    // The receiver controls the call length: it hangs up when its capture
    // window ends, which closes the session and releases us. `seconds` is a
    // safety cap for callers nobody hangs up on.
    match seconds {
        Some(cap) => tokio::time::sleep(Duration::from_secs(cap)).await,
        None => {
            session.closed().await;
        }
    }
    let held_ms = base.elapsed().as_millis() as u64;
    drop(broadcast);
    live.shutdown().await;
    Ok(held_ms)
}

/// Waits up to `wait` seconds for an inbound 1:1 call on the daemon's
/// transport endpoint (the MoQ ALPN side-channel is registered here for the
/// duration), accepts it, and records the caller's audio to a WAV file,
/// ending when the caller's broadcast ends or `seconds` elapse. With `from`
/// set, calls from any other endpoint are rejected.
pub fn answer_to_wav(
    transport: std::sync::Arc<idfon_core::transport::IrohTransport>,
    out: &Path,
    seconds: u64,
    wait: u64,
    from: Option<&str>,
) -> anyhow::Result<ListenStats> {
    let out = out.to_path_buf();
    let from = from.map(str::to_string);
    let handle = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(answer_wav(&transport, &out, seconds, wait, from.as_deref()))
    });
    handle.join().map_err(|_| anyhow::anyhow!("answer thread panicked"))?
}

async fn answer_wav(
    transport: &idfon_core::transport::IrohTransport,
    out: &Path,
    seconds: u64,
    wait: u64,
    from: Option<&str>,
) -> anyhow::Result<ListenStats> {
    let from_id: Option<EndpointId> = match from {
        Some(id) => Some(id.parse().map_err(|err| anyhow::anyhow!("invalid --from endpoint id: {err}"))?),
        None => None,
    };
    // Mount a node on the daemon endpoint (side channels on the MoQ ALPNs)
    // and take the caller's call broadcast over its session.
    let layout = LiveLayout::default();
    let node = Node::new(transport.endpoint().clone(), layout.config());
    let side = mount_side_channels(transport, &node);
    let mut accepted = node.accepted();
    let session = tokio::time::timeout(Duration::from_secs(wait), async {
        use n0_future::StreamExt as _;
        loop {
            let Some(session) = accepted.next().await else {
                anyhow::bail!("live transport shut down");
            };
            match from_id {
                Some(want) if session.peer() != want => continue,
                _ => break Ok::<_, anyhow::Error>(session),
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("no incoming call within {wait}s"))??;
    let caller = session.peer();
    let path = layout.path(caller, CALL_NAME)?;
    let subscription = tokio::time::timeout(
        Duration::from_secs(10),
        node.subscribe(path, Reach::Direct(caller)),
    )
    .await
    .map_err(|_| anyhow::anyhow!("caller never published its call broadcast"))?
    .map_err(|err| anyhow::anyhow!("accept call: {err}"))?;
    let remote = subscription.remote_broadcast();
    let stats = record_remote(&remote, &subscription.broadcast(), out, seconds, 0, true).await;
    drop(subscription);
    // Hang up: closing the session releases the caller.
    session.connection().close(0u32.into(), b"hangup");
    node.shutdown().await;
    drop(side);
    stats
}

/// Records `broadcast`'s best audio rendition to a 16-bit PCM mono 48 kHz
/// WAV, ending when the broadcast ends or `seconds` elapse. Shared by the
/// ticket path and the 1:1 answer path.
async fn record_remote(
    remote: &RemoteBroadcast,
    consumer: &iroh_live::moq_net::broadcast::Consumer,
    out: &Path,
    seconds: u64,
    subscribe_ms: u64,
    hangup_ends: bool,
) -> anyhow::Result<ListenStats> {
    let base = Instant::now();
    let catalog = remote
        .catalog_within(Duration::from_secs(10))
        .await
        .map_err(|err| anyhow::anyhow!("broadcast catalog: {err}"))?;
    let (audio, config) = catalog
        .audio
        .ranked()
        .next()
        .ok_or_else(|| anyhow::anyhow!("broadcast has no audio track"))?;
    let mut decode_config = AudioDecodeOptions::default();
    decode_config.output.format = Format::F32;
    decode_config.output.sample_rate = Some(48_000);
    decode_config.output.layout = Some(moq_audio::Layout::Mono);
    // TODO(iroh-live-next): fork. The fork's decode-side `conceal` (FEC from
    // the next packet) is gone; upstream conceals lost Opus packets itself
    // (PLC), and `missing_packets` still counts pts holes.
    let mut decoder = AudioConsumer::new(consumer, config, audio.clone(), decode_config).await?;

    let start = Instant::now();
    let wall_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let mut samples: Vec<f32> = Vec::new();
    let mut arrivals: Vec<u128> = Vec::new();
    let mut pts: Vec<u128> = Vec::new();

    while start.elapsed() < Duration::from_secs(seconds) {
        let remaining = Duration::from_secs(seconds).saturating_sub(start.elapsed());
        let read = match tokio::time::timeout(remaining, decoder.read()).await {
            Err(_) => break,
            Ok(read) => read,
        };
        let frame = match read {
            Ok(Some(AudioRead::Frame(frame))) => frame,
            Ok(Some(_)) => continue,
            Ok(None) => break,
            // On a 1:1 call the peer hanging up is a normal end, not an error.
            Err(_) if hangup_ends => break,
            Err(err) => return Err(err.into()),
        };
        arrivals.push(base.elapsed().as_millis());
        pts.push(frame.timestamp.as_millis().unwrap_or(0));
        for chunk in frame.data.chunks_exact(4) {
            samples.push(f32::from_le_bytes(chunk.try_into().unwrap()));
        }
    }

    write_wav(out, &samples)?;
    let intervals: Vec<u128> = arrivals.windows(2).map(|w| w[1] - w[0]).collect();
    let jitter = if intervals.len() > 1 {
        let mean = intervals.iter().sum::<u128>() as f64 / intervals.len() as f64;
        (intervals
            .iter()
            .map(|i| (*i as f64 - mean).powi(2))
            .sum::<f64>()
            / intervals.len() as f64)
            .sqrt()
    } else {
        0.0
    };

    // UX metrics from the arrival schedule and publisher timeline.
    let max_gap_ms = intervals.iter().max().copied().unwrap_or(0) as u64;
    let stalls = intervals.iter().filter(|i| **i > 100).count() as u32;

    // Missing packets: pts deltas much larger than the median pacing step.
    let mut pts_deltas: Vec<u128> = pts.windows(2).map(|w| w[1] - w[0]).collect();
    pts_deltas.sort_unstable();
    let step = pts_deltas.get(pts_deltas.len() / 2).copied().unwrap_or(0);
    let missing = if step > 0 {
        pts.windows(2)
            .filter(|w| w[1] > w[0] + step + step / 2)
            .map(|w| ((w[1] - w[0]) as f64 / step as f64).round() as u32 - 1)
            .sum::<u32>()
    } else {
        0
    };

    // Minimum prebuffer to play the capture without an underrun: replay
    // starts when the first packet arrives and advances at the publisher's
    // pacing; any packet arriving after its play time forces a stall.
    let a0 = arrivals.first().copied().unwrap_or(0);
    let p0 = pts.first().copied().unwrap_or(0);
    let prebuffer = arrivals
        .iter()
        .zip(pts.iter())
        .map(|(a, p)| a.saturating_sub(a0 + p.saturating_sub(p0)))
        .max()
        .unwrap_or(0) as u64;

    Ok(ListenStats {
        duration_ms: samples.len() as u64 / 48,
        packets: arrivals.len(),
        arrival_jitter_ms: (jitter * 10.0).round() / 10.0,
        wall_ms,
        subscribe_ms,
        startup_ms: arrivals.first().copied().unwrap_or(0).saturating_sub(subscribe_ms as u128) as u64,
        max_gap_ms,
        stalls_over_100ms: stalls,
        missing_packets: missing,
        prebuffer_ms: prebuffer,
    })
}

async fn listen_wav(
    ticket: &str,
    out: &PathBuf,
    seconds: u64,
    relay: bool,
) -> anyhow::Result<ListenStats> {
    let parsed: BroadcastTicket = ticket.parse()?;
    let local_endpoint = build_endpoint(relay).await?;
    // A viewer only dials: no router, no layout.
    let live = Node::new(local_endpoint.clone(), iroh_live::NodeConfig::default());
    let base = Instant::now();
    let mut last_err = String::new();
    // A subscribe to a broadcast that never got announced blocks forever
    // inside iroh, so every attempt is bounded by a timeout; the outer
    // retries cover a publisher whose catalog announce hasn't landed yet.
    const SUBSCRIBE_ATTEMPTS: u32 = 3;
    const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(8);
    let sub = {
        let mut result = None;
        for _attempt in 0..SUBSCRIBE_ATTEMPTS {
            match tokio::time::timeout(SUBSCRIBE_TIMEOUT, live.subscribe_ticket(&parsed))
            .await
            {
                Ok(Ok(sub)) => {
                    result = Some(sub);
                    break;
                }
                Ok(Err(e)) => {
                    last_err = format!("{e:#}");
                }
                Err(_) => {
                    last_err =
                        "timed out waiting for the broadcast to be announced".into();
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        result.ok_or_else(|| anyhow::anyhow!("subscribe failed after retries: {last_err}"))?
    };
    let subscribe_ms = base.elapsed().as_millis() as u64;
    let remote = sub.remote_broadcast();
    let stats = record_remote(&remote, &sub.broadcast(), out, seconds, subscribe_ms, false).await;
    drop(sub);
    live.shutdown().await;
    local_endpoint.close().await;
    stats
}

/// Minimal 16-bit PCM mono 48 kHz WAV writer.
fn write_wav(path: &Path, samples: &[f32]) -> anyhow::Result<()> {
    use std::io::Write;
    let mut file = std::io::BufWriter::new(std::fs::File::create(path)?);
    let data_len = (samples.len() * 2) as u32;
    file.write_all(b"RIFF")?;
    file.write_all(&(36 + data_len).to_le_bytes())?;
    file.write_all(b"WAVEfmt ")?;
    file.write_all(&16u32.to_le_bytes())?;
    file.write_all(&1u16.to_le_bytes())?; // PCM
    file.write_all(&1u16.to_le_bytes())?; // mono
    file.write_all(&48000u32.to_le_bytes())?;
    file.write_all(&96000u32.to_le_bytes())?;
    file.write_all(&2u16.to_le_bytes())?;
    file.write_all(&16u16.to_le_bytes())?;
    file.write_all(b"data")?;
    file.write_all(&data_len.to_le_bytes())?;
    for s in samples {
        file.write_all(&((s.clamp(-1.0, 1.0) * 32767.0) as i16).to_le_bytes())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wav_header_is_wellformed() {
        let dir = std::env::temp_dir().join("idfon-live-wav-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("out.wav");
        write_wav(&path, &[0.0, 0.5, -0.5]).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(&bytes[0..4], b"RIFF");
        assert_eq!(&bytes[8..12], b"WAVE");
        assert_eq!(bytes.len(), 44 + 6);
        // sample count from the data header
        assert_eq!(u32::from_le_bytes(bytes[40..44].try_into().unwrap()), 6);
    }

    #[test]
    fn jitter_is_zero_for_perfect_pacing() {
        // mirrors the interval math in listen_wav
        let arrivals = vec![0u128, 20, 40, 60];
        let intervals: Vec<u128> = arrivals.windows(2).map(|w| w[1] - w[0]).collect();
        let mean = intervals.iter().sum::<u128>() as f64 / intervals.len() as f64;
        let jitter = (intervals
            .iter()
            .map(|i| (*i as f64 - mean).powi(2))
            .sum::<f64>()
            / intervals.len() as f64)
            .sqrt();
        assert!(jitter.abs() < 1e-9);
    }
}
