//! Current MoQ video integration.
//!
//! Pre-encoded Annex-B H.264 uses `FileVideoCapture` and is published as-is
//! (`LocalBroadcast::set_encoded_video`: one rendition, no transcode);
//! MP4/MKV/TS/FLV inputs use the `moq_mux::import::Container` path.

use std::path::Path;

use iroh_live::layout::LiveLayout;
use iroh_live::media::{LocalBroadcast, SubscriptionExt as _};
use iroh_live::{Audience, BroadcastTicket, Node, NodeConfig, Reach};
use moq_video::encode::{Config as EncodeConfig, Encoder};

use crate::live::{build_endpoint, mount_side_channels, MountedNode, CALL_NAME};
use crate::seam::FileVideoCapture;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quality { Low, Mid, High, Highest }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoPreset { P180, P360, P720, P1080 }

impl std::str::FromStr for VideoPreset {
    type Err = ();
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "180p" | "p180" => Ok(Self::P180),
            "360p" | "p360" => Ok(Self::P360),
            "720p" | "p720" => Ok(Self::P720),
            "1080p" | "p1080" => Ok(Self::P1080),
            _ => Err(()),
        }
    }
}

pub struct VideoPublisher {
    runtime: tokio::runtime::Runtime,
    live: MountedNode,
    broadcast: Option<LocalBroadcast>,
    catalog: Option<moq_mux::catalog::Producer>,
    import_task: Option<tokio::task::JoinHandle<()>>,
}

impl VideoPublisher {
    pub fn start(
        path: &Path,
        name: &str,
        relay: bool,
        presets: Vec<VideoPreset>,
    ) -> anyhow::Result<(Self, String)> {
        let path = path.to_path_buf();
        let name = name.to_owned();
        let presets = if presets.is_empty() { vec![VideoPreset::P720] } else { presets };
        let runtime = tokio::runtime::Runtime::new()?;
        let (live, broadcast, catalog, import_task, ticket) = runtime.block_on(async move {
            let endpoint = build_endpoint(relay).await?;
            let live = MountedNode::spawn(endpoint);
            let extension = path.extension().and_then(|value| value.to_str()).unwrap_or_default().to_ascii_lowercase();
            if matches!(extension.as_str(), "mp4" | "m4v" | "mov" | "cmfv" | "mkv" | "webm" | "ts" | "m2ts" | "flv") {
                let broadcast_path = live.layout.path(live.node.id(), &name)?;
                let (_publication, mut raw) = live
                    .node
                    .publish_with(broadcast_path, Audience::Granted, |producer| Ok::<_, std::convert::Infallible>(producer.clone()))?;
                let catalog = moq_mux::catalog::Producer::new(&mut raw, moq_mux::catalog::Config::default())?;
                let reserve = catalog.reserve();
                let data = std::fs::read(&path)?;
                let first_chunk_len = data.len().min(256 * 1024);
                let format = match extension.as_str() {
                    "mkv" | "webm" => moq_mux::import::ContainerFormat::Mkv,
                    "ts" | "m2ts" => moq_mux::import::ContainerFormat::Ts,
                    "flv" => moq_mux::import::ContainerFormat::Flv,
                    _ => moq_mux::import::ContainerFormat::Fmp4,
                };
                let init = moq_mux::import::ContainerInit::new(format, data[..first_chunk_len].to_vec());
                let mut container = moq_mux::import::Container::new(raw, reserve, &init)?;
                let ticket = live.ticket(&name)?;
                let import_task = tokio::spawn(async move {
                    for chunk in data[first_chunk_len..].chunks(256 * 1024) {
                        if container.decode(chunk).is_err() { return; }
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                    let _ = container.finish();
                });
                Ok::<_, anyhow::Error>((live, None, Some(catalog), Some(import_task), ticket))
            } else {
                let broadcast: LocalBroadcast = live.layout.publish(&live.node, &name)?;
                // Pre-encoded H.264 goes out as its one rendition; the preset
                // ladder needs a decoded source (not wired for files yet).
                let _ = &presets;
                broadcast.set_encoded_video(FileVideoCapture::annexb(&path)?.into_annexb_stream())?;
                let ticket = live.ticket(&name)?;
                Ok::<_, anyhow::Error>((live, Some(broadcast), None, None, ticket))
            }
        })?;
        Ok((Self { runtime, live, broadcast, catalog, import_task }, ticket))
    }

    pub fn stop(self) {
        if let Some(task) = self.import_task { task.abort(); }
        drop(self.broadcast);
        drop(self.catalog);
        self.runtime.block_on(self.live.shutdown());
    }
}

pub fn listen_to_h264(
    ticket: &str,
    out: &Path,
    seconds: u64,
    _relay: bool,
    quality: Option<Quality>,
) -> anyhow::Result<VideoStats> {
    let ticket = ticket.parse::<BroadcastTicket>()?;
    let out = out.to_path_buf();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async move {
            let endpoint = iroh_live::EndpointConfig::from_env()?.bind().await?;
            let node = Node::new(endpoint.clone(), NodeConfig::default());
            let subscription = node.subscribe_ticket(&ticket).await?;
            let wanted = quality.map(|quality| match quality {
                Quality::Low => "180p",
                Quality::Mid => "360p",
                Quality::High => "720p",
                Quality::Highest => "1080p",
            });
            let result = record_video(&subscription, wanted, &out, seconds).await;
            drop(subscription);
            node.shutdown().await;
            endpoint.close().await;
            result
        })
    }).join().map_err(|_| anyhow::anyhow!("video listener thread panicked"))?
}

pub fn dial_to_peer(
    path: &Path,
    peer_addr: &str,
    seconds: Option<u64>,
    relay: bool,
) -> anyhow::Result<u64> {
    let path = path.to_path_buf();
    let peer_addr = peer_addr.to_owned();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async move {
            let addr: iroh::EndpointAddr = serde_json::from_str(&peer_addr)?;
            let peer = addr.id;
            let endpoint = build_endpoint(relay).await?;
            let live = MountedNode::spawn(endpoint);
            let call_path = live.layout.path(live.node.id(), CALL_NAME)?;
            let (_publication, broadcast) = live.node.publish_with(
                call_path,
                Audience::peers([peer]),
                |producer| LocalBroadcast::try_from(producer.clone()),
            )?;
            broadcast.set_encoded_video(FileVideoCapture::annexb(&path)?.into_annexb_stream())?;
            let session = live.node.connect(addr).await.map_err(|err| anyhow::anyhow!("dial: {err}"))?;
            let started = std::time::Instant::now();
            match seconds {
                Some(seconds) => tokio::time::sleep(std::time::Duration::from_secs(seconds)).await,
                None => { session.closed().await; }
            }
            let elapsed = started.elapsed().as_millis() as u64;
            drop(broadcast);
            live.shutdown().await;
            Ok(elapsed)
        })
    }).join().map_err(|_| anyhow::anyhow!("video dial thread panicked"))?
}

pub fn answer_to_h264(
    transport: std::sync::Arc<idfon_core::transport::IrohTransport>,
    out: &Path,
    seconds: u64,
    wait: u64,
    callers: &[String],
    _quality: Option<Quality>,
) -> anyhow::Result<VideoStats> {
    let out = out.to_path_buf();
    let callers = crate::live::parse_callers(callers)?;
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async move {
            let layout = LiveLayout::default();
            let node = Node::new(transport.endpoint().clone(), layout.config());
            let side = mount_side_channels(&transport, &node);
            let mut accepted = node.accepted();
            let session = tokio::time::timeout(std::time::Duration::from_secs(wait), async {
                use n0_future::StreamExt as _;
                loop {
                    let Some(session) = accepted.next().await else { anyhow::bail!("live transport shut down"); };
                    if callers.contains(&session.peer()) { break Ok::<_, anyhow::Error>(session); }
                }
            }).await.map_err(|_| anyhow::anyhow!("no incoming call within {wait}s"))??;
            let caller = session.peer();
            let subscription = node
                .subscribe(layout.path(caller, CALL_NAME)?, Reach::Direct(caller))
                .await
                .map_err(|err| anyhow::anyhow!("accept: {err}"))?;
            let result = record_video(&subscription, None, &out, seconds).await;
            drop(subscription);
            session.connection().close(0u32.into(), b"hangup");
            node.shutdown().await;
            drop(side);
            result
        })
    }).join().map_err(|_| anyhow::anyhow!("video answer thread panicked"))?
}

/// Decodes the subscription's video (rendition `wanted` when the catalog has
/// it, else the publisher's first) and re-encodes it to an Annex-B file.
async fn record_video(
    subscription: &iroh_live::Subscription,
    wanted: Option<&str>,
    out: &Path,
    seconds: u64,
) -> anyhow::Result<VideoStats> {
    let catalog = subscription
        .remote_broadcast()
        .catalog_within(std::time::Duration::from_secs(10))
        .await
        .map_err(|err| anyhow::anyhow!("video catalog: {err}"))?;
    let (name, config) = wanted
        .and_then(|name| catalog.video.renditions.get_key_value(name))
        .or_else(|| catalog.video.ranked().next())
        .ok_or_else(|| anyhow::anyhow!("broadcast has no video track"))?;
    let mut consumer = moq_video::decode::Consumer::new(
        &subscription.broadcast(),
        config,
        name.clone(),
        moq_video::decode::Options::new(),
    )
    .await?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(seconds);
    let mut encoder: Option<Encoder> = None;
    let mut bytes = Vec::new();
    let mut frames = 0;
    loop {
        let read = match tokio::time::timeout_at(deadline, consumer.read()).await {
            Err(_) => break,
            Ok(read) => read?,
        };
        let frame = match read {
            Some(moq_video::decode::Read::Frame(frame)) => frame,
            Some(_) => continue,
            None => break,
        };
        if encoder.is_none() {
            let size = frame.size();
            let rate = moq_video::FrameRate::new(30, 1).map_err(|err| anyhow::anyhow!("{err}"))?;
            encoder = Some(Encoder::new(&EncodeConfig::new(size.width, size.height, rate))?);
        }
        let encoder = encoder.as_mut().expect("set above");
        encoder.encode(&frame)?;
        while let Some(unit) = encoder.receive()? {
            bytes.extend_from_slice(&unit.payload);
        }
        frames += 1;
    }
    std::fs::write(out, &bytes)?;
    Ok(VideoStats { duration_ms: seconds * 1000, packets: frames, subscribe_ms: 0, out: out.display().to_string(), frames, bytes: bytes.len() as u64 })
}

#[derive(Debug, Clone, Default)]
pub struct VideoStats {
    pub duration_ms: u64,
    pub packets: usize,
    pub subscribe_ms: u64,
    pub out: String,
    pub frames: usize,
    pub bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_presets() {
        assert_eq!("180p".parse(), Ok(VideoPreset::P180));
        assert_eq!("p1080".parse(), Ok(VideoPreset::P1080));
        // An unknown preset is the error case, not a supported one.
        assert!("1440p".parse::<VideoPreset>().is_err());
    }
}
