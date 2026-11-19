//! Current native media bridge built on the current iroh-live/moq-media APIs.
//!
//! The old iroh-live media backend was intentionally removed. Native shells push
//! frames/samples into the current source model; capture and decode ownership
//! stays in moq-audio/moq-video.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    ffi::c_void,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    sync::{Arc, Mutex},
};

use iroh::{protocol::Router, Endpoint, EndpointAddr, EndpointId};
use iroh_blobs::{store::fs::FsStore, ticket::BlobTicket, BlobsProtocol, ALPN as BLOBS_ALPN};
use iroh_live::layout::LiveLayout;
use iroh_live::media::{
    AudioSource, LocalBroadcast, SubscriptionExt as _, VideoEncoding, VideoFormat, VideoRendition, VideoSource,
};
use iroh_live::{BroadcastTicket, Node, Reach};
use moq_audio::encode::{Codec as AudioCodec, Config as AudioOptions};
use moq_audio::{Format, Frame as AudioFrame};
use moq_video::{Frame as VideoFrame, Size, Surface};
use n0_future::{boxed::BoxStream, stream::unfold};
use safer_ffi::prelude::*;

use idfon_media::seam::{
    audio_frames, video_frames, AudioCapture, AudioInput, AudioPlayback, DeviceAudioCapture,
    FileAudioCapture, PacedPlayback,
};

use crate::util::tokio_executor;

const AUDIO_CAPACITY: usize = 48_000 * 2;
static AUDIO_QUEUE: Mutex<Option<Arc<Mutex<VecDeque<f32>>>>> = Mutex::new(None);
static AUDIO_QUEUE_RATE: AtomicUsize = AtomicUsize::new(48_000);
/// Interleaved channel count the live publisher was started with. Shell pushes
/// must match it: the encoder's PCM layout is fixed when the track is created.
static AUDIO_PUSH_CHANNELS: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(1);
static AUDIO_SAMPLES_PUSHED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static AUDIO_SAMPLES_NONZERO: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static AUDIO_FRAMES_ENCODED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static AUDIO_FRAMES_DECODED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static AUDIO_QUEUE_UNDERRUN_SAMPLES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static AUDIO_QUEUE_OVERFLOW_SAMPLES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Shared monotonic origin for the live audio and video track timestamps.
static LIVE_MEDIA_EPOCH: Mutex<Option<std::time::Instant>> = Mutex::new(None);
static VIDEO_PTS_CLOCK: Mutex<VideoPtsClock> = Mutex::new(VideoPtsClock {
    capture_origin_ms: None,
    last_capture_ms: None,
    media_origin_us: 0,
});
static LAST_ERROR: Mutex<Option<String>> = Mutex::new(None);
static MEDIA_SCOPE: Mutex<Option<PathBuf>> = Mutex::new(None);
static LIVE_CONFIG: Mutex<Option<(bool, bool, bool, Option<PathBuf>, AudioCodec, u32, u32)>> =
    Mutex::new(None);
static AUDIO_ENABLED: AtomicBool = AtomicBool::new(true);
static VIDEO_ENABLED: AtomicBool = AtomicBool::new(false);

/// Audio samples the playout could not deliver in time, for health checks.
pub(crate) fn underruns() -> u64 {
    AUDIO_QUEUE_UNDERRUN_SAMPLES.load(Ordering::Relaxed)
}

/// Whether the live session carries video, for health-check tips.
pub(crate) fn video_enabled() -> bool {
    VIDEO_ENABLED.load(Ordering::Relaxed)
}
static RECORDING: Mutex<Option<Recording>> = Mutex::new(None);
static RECORDING_DURATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static BLOB_PROVIDER: Mutex<Option<BlobProvider>> = Mutex::new(None);
static VIDEO_QUEUE: Mutex<Option<(u32, u32, Vec<u8>, u64)>> = Mutex::new(None);
static VIDEO_FRAMES_PUSHED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static VIDEO_FRAMES_CONSUMED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static PLAYBACK: Mutex<Option<Playback>> = Mutex::new(None);
/// The looping call tone (donpark/idfon#15): its own playback thread so a
/// tone stop never disturbs a recording playback and vice versa.
static TONE: Mutex<Option<Playback>> = Mutex::new(None);
static PLAYBACK_CONTROL: Mutex<Option<moq_audio::playback::Control>> = Mutex::new(None);
static PLAYBACK_ENGINE: Mutex<Option<moq_audio::playback::Engine>> = Mutex::new(None);
static INPUT_DEVICE: Mutex<Option<String>> = Mutex::new(None);
static OUTPUT_DEVICE: Mutex<Option<String>> = Mutex::new(None);
static VOLUME: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(100);
static BITRATE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(36_000);

/// The deep redundancy (DRED) cover, matching the decode `conceal` bound: a
/// burst this long recovers instead of concealing.
const DRED_COVER: std::time::Duration = std::time::Duration::from_millis(120);

/// Redundancy modes for [`redundancy_for`] and `media_audio_set_redundancy`:
/// docs/media-adaptation.md §D7.1's `off | inband | dred | transport`, plus
/// `auto` as the default the ladder does not override.
mod redundancy {
    /// Derive from the call shape (see [`super::redundancy_for`]).
    pub const AUTO: u8 = 0;
    /// Show holes as sent.
    pub const OFF: u8 = 1;
    /// Opus in-band FEC (LBRR).
    pub const INBAND: u8 = 2;
    /// Opus deep redundancy (DRED).
    pub const DRED: u8 = 3;
    /// Was the codec-independent parity sidecar; now an alias for [`DRED`], which
    /// the recovering decoder covers bursts with.
    pub const TRANSPORT: u8 = 4;
}
static REDUNDANCY: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(redundancy::AUTO);

/// The redundancy mode in effect for a call: an explicit override from
/// `media_audio_set_redundancy`, else the automatic §D7.1 split — DRED for
/// quality calls (stereo, and the audio of any video call: video is the shed
/// rung, its audio must survive it), LBRR for mono voice, nothing for
/// non-Opus.
fn redundancy_for(override_mode: u8, codec: AudioCodec, channels: u32, video: bool) -> u8 {
    match override_mode {
        redundancy::AUTO => {
            if codec != AudioCodec::Opus {
                redundancy::OFF
            } else if channels > 1 || video {
                redundancy::DRED
            } else {
                redundancy::INBAND
            }
        }
        mode => mode,
    }
}

/// The call's audio encoding with the redundancy mode applied — the one place
/// both construction sites (`start_live` and the track rebuild) mirror.
fn call_options(
    codec: AudioCodec,
    sample_rate: u32,
    channels: u32,
    video: bool,
) -> anyhow::Result<AudioOptions> {
    let layout = moq_audio::Layout::from_channels(channels)
        .map_err(|err| anyhow::anyhow!("audio channels {channels}: {err}"))?;
    let mut options = AudioOptions::new(sample_rate, layout);
    options.codec = codec;
    let mode = redundancy_for(REDUNDANCY.load(Ordering::Relaxed), codec, channels, video);
    // BITRATE is per channel: stereo speech wants ~64 kbps at the 32 kbps
    // default (music wants `media_audio_set_bitrate`). DRED and the stereo
    // image both need the 48 kbps per channel floor (docs/media-adaptation.md
    // §D7/D7.1); the floor stays so the rates match the fork's once it returns.
    let per_channel = BITRATE.load(Ordering::Relaxed).max(
        if codec == AudioCodec::Opus && (channels > 1 || mode == redundancy::DRED) {
            48_000
        } else {
            0
        },
    );
    options.bitrate = (codec == AudioCodec::Opus)
        .then(|| moq_net::bandwidth::Rate::from_bps(per_channel * u64::from(channels)));
    // Voice: VoIP coding with discontinuous transmission, so silence costs
    // ~nothing on the wire (the old `dtx` knob). Stereo is music mode.
    if codec == AudioCodec::Opus {
        options.kind = Some(if channels > 1 {
            moq_audio::encode::Kind::Music
        } else {
            moq_audio::encode::Kind::Voice
        });
    }
    // §D7.1's split: `inband` is Opus LBRR (mono voice), `dred` the stereo-safe deep
    // redundancy (needs the 48 kbps per channel floor above). The receiver rebuilds lost
    // packets from either through moq-audio's recovering decode (`recover: Some(true)`).
    // `transport` (the codec-independent parity sidecar) is gone with the old fork: the
    // recovering decoder covers single holes from LBRR and bursts from DRED, so it maps
    // to DRED.
    if codec == AudioCodec::Opus {
        options.fec = mode == redundancy::INBAND;
        options.dred =
            matches!(mode, redundancy::DRED | redundancy::TRANSPORT).then_some(DRED_COVER);
    }
    if codec == AudioCodec::Liquid {
        // Fixed 80 ms frames; the codec carries its own redundancy, not the encoder knobs.
        options.frame_duration = std::time::Duration::from_millis(80);
    }
    Ok(options)
}

/// The codec the call's invite names (`audio_codec=`): the rendition a subscriber prefers when the
/// publisher offers several. Set by the shell before subscribing; cleared on unsubscribe.
static AUDIO_PREFERENCE: Mutex<Option<String>> = Mutex::new(None);

/// Prefer `codec` (`liquid`, `opus`, ...; empty clears) among a publisher's audio renditions for the
/// next and current subscription. Returns 0.
#[ffi_export]
pub fn media_live_set_audio_preference(codec: char_p::Ref<'_>) -> u8 {
    let codec = codec.to_str().trim();
    *AUDIO_PREFERENCE
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) =
        (!codec.is_empty()).then(|| codec.to_string());
    0
}

/// The catalog's audio renditions, for crate::audio_rendition.
fn audio_renditions(catalog: &iroh_live::media::Catalog) -> Vec<crate::audio_rendition::Rendition> {
    crate::audio_rendition::in_publisher_order(
        catalog
            .audio
            .renditions
            .iter()
            .map(|(name, config)| crate::audio_rendition::Rendition {
                name: name.clone(),
                codec: config.codec.to_string(),
                bitrate: config.bitrate,
            })
            .collect(),
    )
}

/// The codec of the audio track being decoded, for the call's status line.
static INCOMING_AUDIO_CODEC: Mutex<Option<String>> = Mutex::new(None);

/// A human status for the live call, for checking the adaptive heuristics rather than reading IDs: one line
/// for the incoming video (rendition, resolution, frame rate, hardware or software decode, the rendition's
/// bitrate) and one for the call (audio codecs each way, direct or relay, round trip, loss, the delivery-rate
/// estimate adaptation acts on). Empty when nothing is live. E.g.
/// `high 720p · 30 fps · HW decode · 1.5 Mbps` / `LiquidCodec · direct · 10 ms · 0% loss · 4.8 Mbps avail`.
#[ffi_export]
pub fn media_live_summary() -> char_p::Box {
    fn rate(bps: u64) -> String {
        if bps >= 1_000_000 {
            format!("{:.1} Mbps", bps as f64 / 1e6)
        } else {
            format!("{} kbps", (bps + 500) / 1_000)
        }
    }
    // Line 1: the incoming video — what the adaptive ladder picked and how it is arriving.
    let mut video: Vec<String> = Vec::new();
    if let Some(v) = crate::video::incoming_video() {
        let p = format!("{}p", v.width.min(v.height));
        video.push(match v.rendition {
            Some(name) => format!("{name} {p}"),
            None => p,
        });
        if let Some(fps) = v.fps {
            video.push(format!("{} fps", fps.round() as u32));
        }
        match v.hardware {
            Some(true) => video.push("HW decode".into()),
            Some(false) => video.push("SW decode".into()),
            None => {}
        }
        if let Some(bps) = v.bitrate {
            video.push(rate(bps));
        }
    }
    // Line 2: audio codecs each way, then the path and the signals adaptation acts on.
    let mut call: Vec<String> = Vec::new();
    let codec_name = |codec: &str| match codec {
        "liquid" => "LiquidCodec".to_string(),
        "opus" => "Opus".to_string(),
        "pcm" => "PCM".to_string(),
        other => other.to_string(),
    };
    let incoming = INCOMING_AUDIO_CODEC
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    let outgoing = LIVE_CONFIG
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_ref()
        .map(|c| c.4.to_string());
    match (incoming, outgoing) {
        (Some(i), Some(o)) if i == o => call.push(codec_name(&i)),
        (Some(i), Some(o)) => call.push(format!("{} ↓ {} ↑", codec_name(&i), codec_name(&o))),
        (Some(i), None) => call.push(codec_name(&i)),
        (None, Some(o)) => call.push(codec_name(&o)),
        (None, None) => {}
    }
    if let Some(net) = crate::health::network_summary() {
        call.push(if net.relay {
            "relay".into()
        } else {
            "direct".into()
        });
        if let Some(rtt) = net.rtt_ms {
            call.push(format!("{rtt} ms"));
        }
        call.push(format!("{:.0}% loss", net.loss_pct));
        if let Some(bps) = net.delivery_bps {
            call.push(format!("{} avail", rate(bps)));
        }
    }
    [video.join(" · "), call.join(" · ")]
        .into_iter()
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
        .try_into()
        .unwrap()
}

/// Playout floor for LiquidCodec tracks: one 80 ms packet plus 40 ms of arrival slack.
const LIQUID_PLAYOUT_FLOOR_MS: u64 = 120;

/// The call's audio renditions: the chosen codec first, plus an alternate so
/// each subscriber can pick (and switch mid-call) by what it can decode and
/// what its network affords.
/// Voice-agent PCM stays single. Each rendition is a track named by its
/// position and codec (`rendition_name`); one source feeds every encoder,
/// resampled per rendition by moq-audio.
fn call_renditions(
    codec: AudioCodec,
    sample_rate: u32,
    channels: u32,
    video: bool,
) -> anyhow::Result<Vec<AudioOptions>> {
    let mut renditions = vec![call_options(codec, sample_rate, channels, video)?];
    match codec {
        AudioCodec::Liquid => renditions.push(call_options(
            AudioCodec::Opus,
            sample_rate,
            channels,
            video,
        )?),
        AudioCodec::Opus if moq_audio::external::liquid_available() => {
            renditions.push(call_options(
                AudioCodec::Liquid,
                moq_audio::external::liquid::SAMPLE_RATE,
                1,
                video,
            )?)
        }
        _ => {}
    }
    Ok(renditions)
}

/// The track name of the `index`th call rendition: `{index}.{codec}` (`0.opus`, `1.liquid`).
///
/// The catalog lists renditions in a map keyed by name, so the publisher's order (its chosen codec
/// first) survives only in the names; a subscriber sorts by name to recover it.
fn rendition_name(index: usize, options: &AudioOptions) -> String {
    let codec = iroh_live::media::AudioRendition::from_config(options.clone()).name;
    format!("{index}.{codec}")
}

fn audio_renditions_of(
    renditions: Vec<AudioOptions>,
) -> Vec<iroh_live::media::AudioRendition> {
    renditions
        .into_iter()
        .enumerate()
        .map(|(index, options)| {
            iroh_live::media::AudioRendition::new(rendition_name(index, &options), options)
        })
        .collect()
}

/// The Opus rendition's track name, its configured bitrate and its channel count, for audio ABR.
fn opus_abr_target(renditions: &[AudioOptions]) -> Option<(String, u64, u32)> {
    renditions
        .iter()
        .enumerate()
        .find(|(_, o)| o.codec == AudioCodec::Opus)
        .map(|(index, opus)| {
            (
                rendition_name(index, opus),
                opus.bitrate.as_ref().map_or(0, |rate| rate.as_bps()),
                opus.layout.channels(),
            )
        })
}

/// Stored as `usize` so the function pointer survives a static `Mutex`.
type AudioPlaybackFn = unsafe extern "C" fn(*const c_void, *const u8, usize, u32, u32, u64);

/// Optional shell-owned audio playback. When absent, decoded audio goes to the
/// bundled moq playback device; when set, the shell gets interleaved f32 PCM.
static AUDIO_PLAYBACK_CB: Mutex<Option<(usize, AudioPlaybackFn)>> = Mutex::new(None);

/// Register a shell audio renderer before `media_live_subscribe`.
///
/// The callback runs on the seam's pacing thread and is handed a borrowed
/// interleaved f32 PCM buffer valid only for the duration of the call; copy
/// before returning. Frames are paced through `seam::PacedPlayback`'s jitter
/// buffer, so `pts_us` is always 0 (in-order playout carries no per-frame
/// clock).
#[ffi_export]
pub fn media_audio_set_playback_cb(
    ctx: *const c_void,
    cb: unsafe extern "C" fn(*const c_void, *const u8, usize, u32, u32, u64),
) -> u8 {
    *AUDIO_PLAYBACK_CB.lock().unwrap() = Some((ctx as usize, cb));
    MIXER.lock().unwrap_or_else(|poison| poison.into_inner()).take();
    0
}

#[ffi_export]
pub fn media_audio_clear_playback_cb() {
    *AUDIO_PLAYBACK_CB.lock().unwrap() = None;
    // The mixer calls the old callback; it must not outlive it.
    MIXER.lock().unwrap_or_else(|poison| poison.into_inner()).take();
}

/// Sums the listen legs for the shell callback (crate::mixer). Started by the
/// first leg, dropped once no leg feeds it.
static MIXER: Mutex<Option<crate::mixer::Mixer>> = Mutex::new(None);

/// Members added with `media_live_add_member` (group call, G0). Their legs skip
/// the A/V anchor (group video is later work, and one anchor serves one
/// picture) and write per-member call captures.
static GROUP_PEERS: Mutex<Option<HashSet<EndpointId>>> = Mutex::new(None);

/// Remote members a call holds at most: 12 people including this one.
pub(crate) const MAX_REMOTE_MEMBERS: usize = 11;

fn is_group_peer(peer: &EndpointId) -> bool {
    GROUP_PEERS
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_ref()
        .is_some_and(|peers| peers.contains(peer))
}

/// A mixer input for one leg, starting the mixer on the shell callback if none runs.
fn mixer_member(ctx: usize, cb: AudioPlaybackFn, channels: u32) -> crate::mixer::MemberInput {
    let mut slot = MIXER.lock().unwrap_or_else(|poison| poison.into_inner());
    let mixer = slot.get_or_insert_with(|| {
        crate::mixer::Mixer::start(Box::new(move |pcm: &[f32], channels: u32| {
            // SAFETY: the shell copies before returning (documented contract); the
            // mixer is dropped before the callback is cleared or replaced.
            unsafe {
                (cb)(
                    ctx as *const c_void,
                    pcm.as_ptr() as *const u8,
                    std::mem::size_of_val(pcm),
                    crate::mixer::RATE,
                    channels,
                    0,
                )
            }
        }))
    });
    mixer.add(channels)
}

/// Drop the mixer once no leg feeds it.
fn release_mixer_if_idle() {
    let mut slot = MIXER.lock().unwrap_or_else(|poison| poison.into_inner());
    if slot.as_ref().is_some_and(|mixer| mixer.member_count() == 0) {
        slot.take();
    }
}

/// One call's media plane: one endpoint and node, shared by the published leg
/// and every subscription, so a peer's publish and subscribe ride one session.
///
/// The publication is the call's one broadcast; `subscribers` holds a listen
/// leg per remote member, keyed by endpoint id. The 1:1 FFI drives a session
/// with one remote member; a group call (docs/group-live-chat.md, G0) adds
/// members to the same map. Created by whichever leg starts first, shut down
/// when the last leg ends.
struct CallSession {
    endpoint: Endpoint,
    node: Node,
    router: Router,
    publication: Option<Publication>,
    subscribers: HashMap<EndpointId, Subscriber>,
}

/// The call's published broadcast and the ticket that names it.
struct Publication {
    broadcast: LocalBroadcast,
    ticket: String,
}

/// The live call's node and published broadcast, cloned out of the lock, for
/// crate::abr. `None` without a call or before it publishes.
pub(crate) fn call_publication() -> Option<(Node, LocalBroadcast)> {
    let session = SESSION.lock().unwrap_or_else(|poison| poison.into_inner());
    let session = session.as_ref()?;
    let publication = session.publication.as_ref()?;
    Some((session.node.clone(), publication.broadcast.clone()))
}

/// Whether a call session exists, published or not.
pub(crate) fn call_live() -> bool {
    SESSION
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .is_some()
}

/// What a listen leg needs from the session, cloned out of the lock.
#[derive(Clone)]
pub(crate) struct SessionHandle {
    pub(crate) endpoint: Endpoint,
    pub(crate) node: Node,
}

static SESSION: Mutex<Option<CallSession>> = Mutex::new(None);

/// The call session's endpoint and node, creating the session if no leg runs yet.
/// Runs on the shared executor's runtime (the node's tasks live there).
pub(crate) async fn session_handle() -> anyhow::Result<SessionHandle> {
    if let Some(session) = SESSION
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_ref()
    {
        return Ok(SessionHandle {
            endpoint: session.endpoint.clone(),
            node: session.node.clone(),
        });
    }
    let endpoint = bind_call_endpoint().await?;
    let layout = LiveLayout::default();
    let node = Node::new(endpoint.clone(), layout.config());
    // Accept side: MoQ for the peer's subscriptions plus the health ALPN
    // (docs/media-adaptation.md §B2.2), on the session's own endpoint.
    let router = crate::health::serve(node.mount(Router::builder(endpoint.clone()))).spawn();
    let mut slot = SESSION.lock().unwrap_or_else(|poison| poison.into_inner());
    if let Some(session) = slot.as_ref() {
        // Another leg won the race: keep its session, drop ours.
        let handle = SessionHandle {
            endpoint: session.endpoint.clone(),
            node: session.node.clone(),
        };
        drop(slot);
        let _ = router.shutdown().await;
        return Ok(handle);
    }
    tracing::info!(target: "idfon.media", id = %endpoint.id().fmt_short(), "call session started");
    *slot = Some(CallSession {
        endpoint: endpoint.clone(),
        node: node.clone(),
        router,
        publication: None,
        subscribers: HashMap::new(),
    });
    Ok(SessionHandle { endpoint, node })
}

/// The call endpoint: idfon's relay selection (docs/relay-config.md, so a
/// self-hosted relay replaces n0's rate-limited public ones — media-adaptation
/// D6) with iroh-live's media congestion control (BBR3).
async fn bind_call_endpoint() -> anyhow::Result<Endpoint> {
    Ok(idfon_core::relay::endpoint_builder()
        .transport_config(iroh_live::Congestion::default().transport_config())
        .bind()
        .await?)
}

/// Shut the session down once no leg uses it. Call outside the session lock.
fn release_session_if_idle() {
    let idle = {
        let mut slot = SESSION.lock().unwrap_or_else(|poison| poison.into_inner());
        match slot.as_ref() {
            Some(session) if session.publication.is_none() && session.subscribers.is_empty() => {
                slot.take()
            }
            _ => None,
        }
    };
    if let Some(session) = idle {
        tracing::info!(target: "idfon.media", "call session shut down");
        tokio_executor(async move {
            let _ = session.router.shutdown().await;
        });
    }
}

/// One listen leg: the thread that subscribes, decodes and plays one remote member.
struct Subscriber {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}
struct Recording {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}
struct BlobProvider {
    endpoint: Endpoint,
    store: FsStore,
    _router: Router,
}
struct Playback {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

pub(crate) fn media_base() -> PathBuf {
    std::env::var_os("IDFON_MEDIA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("idfon"))
}

pub(crate) fn media_path(name: &str) -> PathBuf {
    let root = MEDIA_SCOPE
        .lock()
        .ok()
        .and_then(|scope| scope.clone())
        .unwrap_or_else(media_base);
    root.join(name)
}

/// Resolve a caller-supplied scope to a directory. A bare conversation id is
/// not a path: `create_dir_all` on it would drop a directory into the process
/// cwd, so only an absolute scope is honored as-is and anything else is placed
/// under the media root.
fn scoped_media_dir(scope: &Path) -> PathBuf {
    let relative = scope.is_relative();
    if relative {
        let name = scope.file_name().unwrap_or_else(|| scope.as_os_str());
        media_base().join(name)
    } else {
        scope.to_path_buf()
    }
}

fn audio_queue() -> Arc<Mutex<VecDeque<f32>>> {
    let mut slot = AUDIO_QUEUE.lock().expect("audio queue poisoned");
    slot.get_or_insert_with(|| Arc::new(Mutex::new(VecDeque::with_capacity(AUDIO_CAPACITY))))
        .clone()
}

/// Largest single capture push seen, so the trim target fits the device.
static AUDIO_MAX_BURST: AtomicUsize = AtomicUsize::new(0);

fn media_pts_us(epoch: std::time::Instant, now: std::time::Instant) -> u64 {
    now.saturating_duration_since(epoch)
        .as_micros()
        .min(u128::from(u64::MAX)) as u64
}

fn live_media_epoch() -> Option<std::time::Instant> {
    *LIVE_MEDIA_EPOCH
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

#[derive(Default)]
struct VideoPtsClock {
    capture_origin_ms: Option<u64>,
    last_capture_ms: Option<u64>,
    media_origin_us: u64,
}

impl VideoPtsClock {
    fn map(
        &mut self,
        capture_pts_ms: u64,
        epoch: std::time::Instant,
        now: std::time::Instant,
    ) -> u64 {
        if self.capture_origin_ms.is_none()
            || self
                .last_capture_ms
                .is_some_and(|last| capture_pts_ms < last)
        {
            self.capture_origin_ms = Some(capture_pts_ms);
            self.media_origin_us = media_pts_us(epoch, now);
        }
        self.last_capture_ms = Some(capture_pts_ms);
        let capture_origin_ms = self.capture_origin_ms.unwrap_or(capture_pts_ms);
        self.media_origin_us.saturating_add(
            capture_pts_ms
                .saturating_sub(capture_origin_ms)
                .saturating_mul(1_000),
        )
    }
}

fn audio_stream(
    queue: Arc<Mutex<VecDeque<f32>>>,
    sample_rate: u32,
    channels: u32,
) -> BoxStream<AudioFrame> {
    let frame_samples = sample_rate as usize / 50 * channels as usize;
    let epoch = live_media_epoch().unwrap_or_else(std::time::Instant::now);
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + std::time::Duration::from_millis(20),
        std::time::Duration::from_millis(20),
    );
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    Box::pin(unfold(
        (queue, ticker, None::<std::time::Instant>),
        move |(queue, mut ticker, last_tick)| async move {
            ticker.tick().await;
            let now = std::time::Instant::now();
            let tick_gap_us = last_tick
                .map(|last| now.duration_since(last).as_micros())
                .unwrap_or(20_000);
            let mut data = vec![0.0f32; frame_samples];
            let (missing, depth) = if AUDIO_ENABLED.load(Ordering::Relaxed) {
                let mut q = queue.lock().ok()?;
                let available = q.len().min(frame_samples);
                for sample in data.iter_mut() {
                    *sample = q.pop_front().unwrap_or(0.0);
                }
                (frame_samples - available, q.len())
            } else {
                if let Ok(mut q) = queue.lock() {
                    q.clear();
                }
                (0, 0)
            };
            if missing > 0 {
                AUDIO_QUEUE_UNDERRUN_SAMPLES.fetch_add(missing as u64, Ordering::Relaxed);
            }
            let encoded = AUDIO_FRAMES_ENCODED.fetch_add(1, Ordering::Relaxed) + 1;
            if encoded <= 3 || encoded % 50 == 0 || missing > 0 || tick_gap_us > 30_000 {
                eprintln!(
                    "[media] audio frame={} rate={} queue={} underflow={} samples tick_gap={}us",
                    encoded, sample_rate, depth, missing, tick_gap_us
                );
            }
            let bytes: Vec<u8> = data.into_iter().flat_map(f32::to_le_bytes).collect();
            let pts = media_pts_us(epoch, now);
            let timestamp = moq_net::Timestamp::from_micros(pts).ok()?;
            Some((
                AudioFrame::new(bytes::Bytes::from(bytes), timestamp),
                (queue, ticker, Some(now)),
            ))
        },
    ))
}

fn video_stream() -> BoxStream<VideoFrame> {
    Box::pin(unfold((), |_| async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            if !VIDEO_ENABLED.load(Ordering::Relaxed) {
                continue;
            }
            let Some((width, height, pixels, pts)) = VIDEO_QUEUE.lock().ok()?.take() else {
                continue;
            };
            VIDEO_FRAMES_CONSUMED.fetch_add(1, Ordering::Relaxed);
            let size = Size { width, height };
            let surface = Surface::rgba(&pixels, size).ok()?;
            let timestamp = moq_net::Timestamp::from_micros(pts).ok()?;
            return Some((VideoFrame::new(surface, timestamp), ()));
        }
    }))
}

/// Platform capture adapter: PCM pushed by the shell via `media_audio_push_samples`.
struct ShellAudioCapture {
    sample_rate: u32,
    channels: u32,
}

impl AudioCapture for ShellAudioCapture {
    fn into_source(self: Box<Self>) -> AudioSource {
        audio_frames(
            AudioInput {
                format: Format::F32,
                sample_rate: self.sample_rate,
                channels: self.channels,
            },
            audio_stream(audio_queue(), self.sample_rate, self.channels),
        )
    }
}

/// Platform capture adapter: BGRA frames pushed by the shell via `media_video_push_frame`
/// (`Idfon-Bridging.h`; CoreVideo's native capture order). The declared format
/// is the top rung's portrait size at 30 fps; frames of another size are scaled.
fn shell_video_source() -> VideoSource {
    let format = VideoFormat {
        size: Size::new(720, 1_280),
        rate: moq_video::FrameRate::new(30, 1).expect("30 fps is a valid rate"),
    };
    video_frames(format, video_stream())
}

/// Bundled fallback: decoded audio to the platform output device via moq.
struct DevicePlayback {
    engine: moq_audio::playback::Engine,
    sink: moq_audio::playback::Sink,
    control: moq_audio::playback::Control,
    input: AudioInput,
}

impl DevicePlayback {
    async fn open(device: Option<String>, input: AudioInput) -> anyhow::Result<Self> {
        let engine = moq_audio::playback::Engine::open({
            let mut config = moq_audio::playback::Config::default();
            config.device = device;
            config
        })
        .await
        .map_err(|err| anyhow::anyhow!("playback engine: {err}"))?;
        Self::on_engine(engine, input)
    }

    /// A sink on an engine that is already open: the engine mixes it with its other sinks.
    fn on_engine(engine: moq_audio::playback::Engine, input: AudioInput) -> anyhow::Result<Self> {
        let mut spec = moq_audio::playback::Input::default();
        spec.format = input.format;
        spec.sample_rate = input.sample_rate;
        spec.layout = moq_audio::Layout::from_channels(input.channels)
            .map_err(|err| anyhow::anyhow!("playback channels {}: {err}", input.channels))?;
        let sink = engine
            .sink(spec)
            .map_err(|err| anyhow::anyhow!("playback sink: {err}"))?;
        let control = sink.control();
        Ok(Self {
            engine,
            sink,
            control,
            input,
        })
    }

    fn engine(&self) -> moq_audio::playback::Engine {
        self.engine.clone()
    }

    fn control(&self) -> moq_audio::playback::Control {
        self.control.clone()
    }
}

impl AudioPlayback for DevicePlayback {
    fn input(&self) -> AudioInput {
        self.input
    }

    fn write(&mut self, pcm: &[u8], _pts_us: u64) -> anyhow::Result<()> {
        self.sink
            .write(pcm)
            .map(drop)
            .map_err(|err| anyhow::anyhow!("playback write: {err}"))
    }

    fn buffered(&self) -> std::time::Duration {
        self.sink.buffered()
    }

    fn set_volume(&mut self, volume: f32) {
        self.control.set_volume(volume);
    }
}

/// Every rung keeps motion: a still rung reads as a frozen call, so capacity
/// pressure removes pixels (down a rung) or the picture, never the cadence.
fn live_video_renditions() -> VideoEncoding {
    VideoEncoding::ladder(live_video_rungs())
}

fn live_video_rungs() -> Vec<VideoRendition> {
    let rung = |name: &str, width: u32, height: u32, bps: u64| {
        let mut rendition = VideoRendition::new(name).with_size(Size::new(width, height));
        rendition.config.bitrate = Some(moq_net::bandwidth::Rate::from_bps(bps));
        rendition
    };
    vec![
        rung("low", 240, 426, 250_000),
        rung("medium", 480, 854, 800_000),
        rung("high", 720, 1_280, 2_000_000),
    ]
}

fn start_live(
    audio: bool,
    video: bool,
    push_audio: bool,
    file_audio: Option<PathBuf>,
    codec: AudioCodec,
    sample_rate: u32,
    channels: u32,
) -> anyhow::Result<String> {
    anyhow::ensure!(
        (8_000..=48_000).contains(&sample_rate) && sample_rate % 50 == 0,
        "unsupported live audio sample rate: {sample_rate}"
    );
    anyhow::ensure!(
        (1..=2).contains(&channels),
        "unsupported live audio channel count: {channels}"
    );
    AUDIO_PUSH_CHANNELS.store(channels, Ordering::Relaxed);
    AUDIO_ENABLED.store(true, Ordering::Relaxed);
    AUDIO_QUEUE_RATE.store(sample_rate as usize, Ordering::Relaxed);
    AUDIO_QUEUE_UNDERRUN_SAMPLES.store(0, Ordering::Relaxed);
    AUDIO_QUEUE_OVERFLOW_SAMPLES.store(0, Ordering::Relaxed);
    AUDIO_SAMPLES_PUSHED.store(0, Ordering::Relaxed);
    AUDIO_SAMPLES_NONZERO.store(0, Ordering::Relaxed);
    AUDIO_FRAMES_ENCODED.store(0, Ordering::Relaxed);
    if push_audio {
        // Capture starts before the publisher so permission/device setup can
        // overlap network startup. Discard that startup backlog rather than
        // making the peer hear up to two seconds of stale audio.
        if let Ok(mut queue) = AUDIO_QUEUE.lock() {
            *queue = None;
        }
    }
    if !audio && !video {
        anyhow::bail!("at least one media track is required");
    }
    tracing::info!(target: "idfon.media", audio, video, "live publisher start entered");
    let result = tokio_executor(async move {
        let setup_started = std::time::Instant::now();
        tracing::info!(target: "idfon.media", "live endpoint setup begin");
        let session = session_handle().await?;
        tracing::info!(target: "idfon.media", elapsed_ms = setup_started.elapsed().as_millis() as u64, "live endpoint setup returned");
        // Start a shared near-zero media timeline only after endpoint setup;
        // MoQ consumers expect track PTS to begin near the start of publish.
        *LIVE_MEDIA_EPOCH
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(std::time::Instant::now());
        *VIDEO_PTS_CLOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = VideoPtsClock::default();
        let name = format!("idfon-live-{}", std::process::id());
        let layout = LiveLayout::default();
        // A restart (device switch) replaces the publication at the same path:
        // drop the old broadcast first so the path is free.
        let replaced = SESSION
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_mut()
            .and_then(|session| session.publication.take());
        drop(replaced);
        tracing::info!(target: "idfon.media", "live broadcast creation begin");
        let broadcast: LocalBroadcast = layout.publish(&session.node, &name)?;
        tracing::info!(target: "idfon.media", "live broadcast created");
        if audio {
            let renditions = call_renditions(codec, sample_rate, channels, video)?;
            // Audio ABR (docs/media-adaptation.md §C5): the Opus rendition follows the
            // sessions' send estimates (crate::abr). Started after the audio is set.
            let abr = opus_abr_target(&renditions);
            // Capture device is the default live source; shell-pushed PCM
            // remains available through `media_audio_push_samples`.
            let capture: Box<dyn AudioCapture> = if push_audio {
                Box::new(ShellAudioCapture {
                    sample_rate,
                    channels,
                })
            } else if let Some(path) = file_audio {
                Box::new(FileAudioCapture::open(&path, false).await?)
            } else {
                Box::new(DeviceAudioCapture::new(
                    INPUT_DEVICE.lock().unwrap().clone(),
                ))
            };
            broadcast.set_audio_renditions(capture.into_source(), audio_renditions_of(renditions))?;
            if let Some((rendition, total, channels)) = abr {
                crate::abr::start(rendition, total, channels);
            }
        }
        if video {
            VIDEO_ENABLED.store(true, Ordering::Relaxed);
            broadcast.set_video(shell_video_source(), live_video_renditions())?;
        }
        tracing::info!(target: "idfon.media", "live track setup complete; creating ticket");
        let ticket = layout.ticket(session.node.id(), &name)?.to_string();
        let mut slot = SESSION.lock().unwrap_or_else(|poison| poison.into_inner());
        let Some(call) = slot.as_mut() else {
            anyhow::bail!("call session ended during publish setup");
        };
        call.publication = Some(Publication {
            broadcast,
            ticket: ticket.clone(),
        });
        anyhow::Ok(ticket)
    });
    tracing::info!(target: "idfon.media", success = result.is_ok(), "live publisher start returned");
    match result {
        Ok(ticket) => {
            *LAST_ERROR.lock().expect("error poisoned") = None;
            Ok(ticket)
        }
        Err(err) => {
            *LIVE_MEDIA_EPOCH
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = None;
            *VIDEO_PTS_CLOCK
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = VideoPtsClock::default();
            *LAST_ERROR.lock().expect("error poisoned") = Some(format!("{err:#}"));
            Err(err)
        }
    }
}

#[ffi_export]
pub fn media_live_start(audio: u8, video: u8) -> char_p::Box {
    media_live_start_inner(
        audio != 0,
        video != 0,
        false,
        None,
        AudioCodec::Opus,
        48_000,
        1,
    )
}

fn media_live_start_inner(
    audio: bool,
    video: bool,
    push_audio: bool,
    file_audio: Option<PathBuf>,
    codec: AudioCodec,
    sample_rate: u32,
    channels: u32,
) -> char_p::Box {
    match start_live(
        audio,
        video,
        push_audio,
        file_audio.clone(),
        codec,
        sample_rate,
        channels,
    ) {
        Ok(ticket) => {
            *LIVE_CONFIG.lock().unwrap() =
                Some((audio, video, push_audio, file_audio, codec, sample_rate, channels));
            ticket.try_into().unwrap()
        }
        Err(_) => String::new().try_into().unwrap(),
    }
}

#[ffi_export]
pub fn media_live_start_with_source(audio: u8, video: u8, source: char_p::Ref<'_>) -> char_p::Box {
    let source = source.to_str();
    let file = source
        .strip_prefix("file:")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from);
    if source != "mic" && source != "push" && file.is_none() {
        *LAST_ERROR.lock().unwrap() = Some(format!("unknown live source: {source}"));
        return String::new().try_into().unwrap();
    }
    media_live_start_inner(
        audio != 0,
        video != 0,
        source == "push",
        file,
        AudioCodec::Opus,
        48_000,
        1,
    )
}

fn supported_audio_profile(codec: AudioCodec, sample_rate: u32) -> bool {
    matches!(
        (codec, sample_rate),
        (AudioCodec::Opus, 48_000)
            | (AudioCodec::Opus, 24_000)
            | (AudioCodec::Pcm, 24_000)
            | (AudioCodec::Liquid, 24_000)
    )
}

#[ffi_export]
pub fn media_live_start_with_profile(
    audio: u8,
    video: u8,
    source: char_p::Ref<'_>,
    codec: char_p::Ref<'_>,
    sample_rate: u32,
    channels: u32,
) -> char_p::Box {
    let codec = match codec.to_str() {
        "opus" => AudioCodec::Opus,
        "pcm" => AudioCodec::Pcm,
        "liquid" => AudioCodec::Liquid,
        other => {
            *LAST_ERROR.lock().unwrap() = Some(format!("unknown live audio codec: {other}"));
            return String::new().try_into().unwrap();
        }
    };
    if !supported_audio_profile(codec, sample_rate) {
        *LAST_ERROR.lock().unwrap() = Some(format!(
            "unsupported live audio profile: {codec} at {sample_rate} Hz"
        ));
        return String::new().try_into().unwrap();
    }
    if channels != 1 && channels != 2 {
        *LAST_ERROR.lock().unwrap() =
            Some(format!("unsupported live audio channel count: {channels}"));
        return String::new().try_into().unwrap();
    }
    if codec == AudioCodec::Pcm && channels != 1 {
        *LAST_ERROR.lock().unwrap() = Some("pcm24k voice-agent calls are mono".into());
        return String::new().try_into().unwrap();
    }
    if codec == AudioCodec::Liquid && channels != 1 {
        *LAST_ERROR.lock().unwrap() = Some("liquid calls are mono".into());
        return String::new().try_into().unwrap();
    }
    if codec == AudioCodec::Liquid && !moq_audio::external::liquid_available() {
        *LAST_ERROR.lock().unwrap() = Some(
            "liquid codec unavailable: no implementation registered (media_liquid_register)".into(),
        );
        return String::new().try_into().unwrap();
    }
    let source = source.to_str();
    let file = source
        .strip_prefix("file:")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from);
    if source != "mic" && source != "push" && file.is_none() {
        *LAST_ERROR.lock().unwrap() = Some(format!("unknown live source: {source}"));
        return String::new().try_into().unwrap();
    }
    media_live_start_inner(
        audio != 0,
        video != 0,
        source == "push",
        file,
        codec,
        sample_rate,
        channels,
    )
}

#[ffi_export]
pub fn media_audio_push_samples(pcm: *const f32, samples: usize) {
    media_audio_push_frames(pcm, samples, 1)
}

/// Stereo-aware push: `frames` frames of `channels` interleaved f32 samples.
/// The channel count must match the live publisher's
/// (`media_live_start_with_profile`); mismatched pushes are dropped so a mono
/// meter cannot corrupt a stereo call's channel alignment.
#[ffi_export]
pub fn media_audio_push_frames(pcm: *const f32, frames: usize, channels: u32) {
    if pcm.is_null() || frames == 0 {
        return;
    }
    let expected = AUDIO_PUSH_CHANNELS.load(Ordering::Relaxed);
    if channels != expected {
        eprintln!(
            "[media] audio push with {channels} channels dropped (publisher runs {expected})"
        );
        return;
    }
    let samples = frames * channels as usize;
    let rate = AUDIO_QUEUE_RATE.load(Ordering::Relaxed) as u64;
    let pushed = AUDIO_SAMPLES_PUSHED.fetch_add(samples as u64, Ordering::Relaxed) + samples as u64;
    if pushed <= 2_000 || pushed % rate < samples as u64 {
        eprintln!(
            "[media] audio samples pushed total={} rate={}",
            pushed, rate
        );
    }
    let input = unsafe { std::slice::from_raw_parts(pcm, samples) };
    let nonzero = input.iter().filter(|sample| sample.abs() > 0.0001).count() as u64;
    AUDIO_SAMPLES_NONZERO.fetch_add(nonzero, Ordering::Relaxed);
    if pushed <= 2_000 || pushed % rate < samples as u64 {
        let peak = input
            .iter()
            .fold(0.0f32, |peak, sample| peak.max(sample.abs()));
        let rms = (input.iter().map(|sample| sample * sample).sum::<f32>() / samples as f32).sqrt();
        eprintln!(
            "[media] pushed audio samples={} nonzero={} rms={:.5} peak={:.5}",
            samples, nonzero, rms, peak
        );
    }
    let queue = audio_queue();
    let result = queue.lock();
    if let Ok(mut q) = result {
        q.extend(input.iter().copied());
        // Trim here, on the capture callback: this runs in real time even when
        // the encoder pump is starved by network backpressure, so backlog
        // cannot ratchet up to the queue capacity while the pump is blocked.
        // (Queue depth IS latency: capture pushes at the selected sample rate.)
        let burst = AUDIO_MAX_BURST
            .fetch_max(samples, Ordering::Relaxed)
            .max(samples);
        let target = (burst * 2).max(rate as usize * 40 / 1_000);
        let excess = q.len().saturating_sub(target);
        // Trim whole frames only: one stray sample dropped from an interleaved
        // stream swaps the channels of every later sample on stereo.
        let channels = AUDIO_PUSH_CHANNELS.load(Ordering::Relaxed).max(1) as usize;
        let excess = excess - excess % channels;
        if excess > 0 {
            AUDIO_QUEUE_OVERFLOW_SAMPLES.fetch_add(excess as u64, Ordering::Relaxed);
            for _ in 0..excess {
                q.pop_front();
            }
        }
    }
}

#[ffi_export]
pub fn media_live_set_audio_enabled(enabled: u8) -> u8 {
    AUDIO_ENABLED.store(enabled != 0, Ordering::Relaxed);
    0
}
#[ffi_export]
pub fn media_live_set_video_enabled(enabled: u8) -> u8 {
    VIDEO_ENABLED.store(enabled != 0, Ordering::Relaxed);
    // Drop the frame parked while the camera was on: re-enabling would
    // otherwise encode it with a timestamp behind everything already sent.
    if enabled == 0 {
        if let Ok(mut slot) = VIDEO_QUEUE.lock() {
            *slot = None;
        }
    }
    eprintln!(
        "[media] video enabled={} pushed={} consumed={}",
        enabled,
        VIDEO_FRAMES_PUSHED.load(Ordering::Relaxed),
        VIDEO_FRAMES_CONSUMED.load(Ordering::Relaxed)
    );
    0
}
#[ffi_export]
pub fn media_live_last_error() -> char_p::Box {
    LAST_ERROR
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_default()
        .try_into()
        .unwrap()
}

/// The live session's own dial address as `EndpointAddr` JSON — what the far
/// side merges into the ticket (`media_live_subscribe_with_addrs`) via the
/// envelope's `addr` line (C20). A live ticket is endpoint-id-only and a fresh
/// endpoint's pkarr record is not resolvable yet at call-setup time, so
/// id-only dials race discovery and time out; `Endpoint::addr()` carries the
/// relay and current direct addresses, which dial without any lookup. Empty
/// when no live session is running.
#[ffi_export]
pub fn media_live_endpoint_addr() -> char_p::Box {
    let endpoint = SESSION
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_ref()
        .filter(|session| session.publication.is_some())
        .map(|session| session.endpoint.clone());
    let json = endpoint
        .map(|endpoint| serde_json::to_string(&addr_with_relay(&endpoint)).unwrap_or_default())
        .unwrap_or_default();
    if !json.is_empty() {
        // Logged so device traces show whether the addr carried a Relay
        // entry (the reachability floor when direct addresses are stale).
        tracing::info!(target: "idfon.media", addr = %json, "live endpoint addr");
    }
    json.try_into().unwrap()
}
/// How long [`addr_with_relay`] waits for a fresh endpoint to pick its home relay.
const HOME_RELAY_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

/// The endpoint's address, waiting up to [`HOME_RELAY_WAIT`] for it to carry a relay URL.
///
/// A call endpoint is fresh, and its home relay arrives a few hundred ms after it binds. An invite
/// sent before then carries only direct addresses, so when the network changes (Wi-Fi to cellular)
/// the far side has no way back: the 2026-11-18 two-iPhone test lost a call for good that way. The
/// wait is a poll, because the shells call this from plain threads. A relay-less setup (relay off)
/// returns after the wait with direct addresses only.
fn addr_with_relay(endpoint: &Endpoint) -> EndpointAddr {
    let started = std::time::Instant::now();
    loop {
        let addr = endpoint.addr();
        if addr.relay_urls().next().is_some() || started.elapsed() >= HOME_RELAY_WAIT {
            if addr.relay_urls().next().is_none() {
                tracing::warn!(target: "idfon.media", waited_ms = started.elapsed().as_millis() as u64,
                    "live endpoint addr has no relay yet; the far side can only dial direct addresses");
            }
            return addr;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

#[ffi_export]
pub fn media_live_stop() {
    // Fold the per-call report before the counters reset (health §B2.2).
    crate::health::finish_call_sampling();
    crate::abr::stop();
    VIDEO_ENABLED.store(false, Ordering::Relaxed);
    let underruns = AUDIO_QUEUE_UNDERRUN_SAMPLES.swap(0, Ordering::Relaxed);
    let overflows = AUDIO_QUEUE_OVERFLOW_SAMPLES.swap(0, Ordering::Relaxed);
    if underruns > 0 || overflows > 0 {
        eprintln!(
            "[media] audio queue gaps: underrun_samples={} overflow_samples={}",
            underruns, overflows
        );
    }
    let publication = SESSION
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_mut()
        .and_then(|session| session.publication.take());
    if let Some(publication) = publication {
        // Closing (and dropping) a broadcast spawns its teardown, which needs a
        // Tokio context; the shells call this from bare threads.
        let _runtime = crate::util::TOKIO_EXECUTOR.enter();
        publication.broadcast.close();
        tracing::info!(target: "idfon.media", ticket = %publication.ticket, "live publication stopped");
        drop(publication);
    }
    release_session_if_idle();
    *LIVE_CONFIG.lock().unwrap() = None;
    if let Ok(mut frame) = VIDEO_QUEUE.lock() {
        *frame = None;
    }
    if let Ok(mut queue) = AUDIO_QUEUE.lock() {
        *queue = None;
    }
    *LIVE_MEDIA_EPOCH
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = None;
    *VIDEO_PTS_CLOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = VideoPtsClock::default();
}
// --- Listen-leg state (docs/media-adaptation.md §C2) ----------------------

/// What the listening leg is doing, for the shell's call UI. The media plane
/// heals itself; this is how the UI stops claiming "playing" while it is
/// silent (the 2026-11-04 Wi-Fi→cellular run: a 6 s hole with the bar still
/// animating). States are the wire values of [`media_live_state`].
const SUB_IDLE: u8 = 0;
const SUB_CONNECTING: u8 = 1;
const SUB_PLAYING: u8 = 2;
const SUB_RECONNECTING: u8 = 3;
/// The retry loop gave up (a hard transport error, not an early track end).
/// The call can still be up with dead air until the shell rebuilds the leg.
const SUB_FAILED: u8 = 4;

static SUB_STATE: AtomicU8 = AtomicU8::new(SUB_IDLE);
/// Set by the shell (foreground return) to shorten the stall window and skip
/// the retry backoff; cleared as soon as a frame arrives again.
static SUB_KICK: AtomicBool = AtomicBool::new(false);
/// When the OS last reported an interface change (NWPathMonitor). For a short
/// grace window the stall watchdog waits 500 ms instead of 2 s, so a path
/// switch is caught as it happens rather than after the old path has been
/// silent for 2 s.
static PATH_CHANGED_AT: Mutex<Option<std::time::Instant>> = Mutex::new(None);

fn set_sub_state(state: u8) {
    let previous = SUB_STATE.swap(state, Ordering::Relaxed);
    if previous != state {
        tracing::info!(target: "quality", from = previous, to = state, "subscribe state");
        eprintln!("[media] subscribe state {previous} -> {state}");
    }
}

/// What the listening leg is doing: 0 idle, 1 connecting, 2 playing,
/// 3 reconnecting, 4 failed. Cheap; safe to poll from the call UI.
#[ffi_export]
pub fn media_live_state() -> u8 {
    SUB_STATE.load(Ordering::Relaxed)
}

/// The shell returned to the foreground: answer a stalled subscription now
/// instead of at the end of its backoff. No-op when nothing is subscribed.
#[ffi_export]
pub fn media_live_reconnect() {
    SUB_KICK.store(true, Ordering::Relaxed);
}

/// The OS reports a network interface change. Shorten the stall watchdog for
/// a grace window; if frames keep flowing this costs nothing.
#[ffi_export]
pub fn media_live_path_changed() {
    *PATH_CHANGED_AT.lock().unwrap_or_else(|poison| poison.into_inner()) =
        Some(std::time::Instant::now());
}

/// How long a silent subscription is tolerated before it is rebuilt.
///
/// 1.5 s right after a path change or a foreground kick (someone already
/// knows the network moved; 500 ms ended attempts during ordinary LiquidCodec
/// jitter in the round-2 device test, 2026-11-18); otherwise the wait depends on the path: a
/// **direct** path going quiet is usually a cell handover that comes back in
/// ~2 s, and rebuilding costs another second on top (2026-11-04 drive: two
/// ~3 s holes, ~2 s of each the blackhole itself), so wait 4 s for it. A
/// relay path has no such resume behavior — 2 s.
///
/// A selected path that closes does not end the attempt: iroh migrates the
/// connection to another path by itself. It only decides, at a stall, whether
/// to redial (see `subscribe_decode_once`).
/// The stall window without the post-path-change shortening: 4 s direct, 2 s relay.
fn normal_stall_window(relay: bool) -> std::time::Duration {
    if relay {
        std::time::Duration::from_secs(2)
    } else {
        std::time::Duration::from_secs(4)
    }
}

fn stall_window(relay: bool) -> std::time::Duration {
    let recent_path = PATH_CHANGED_AT
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .map(|at| at.elapsed() < std::time::Duration::from_secs(15))
        .unwrap_or(false);
    if recent_path || SUB_KICK.load(Ordering::Relaxed) {
        std::time::Duration::from_millis(1_500)
    } else if relay {
        std::time::Duration::from_secs(2)
    } else {
        std::time::Duration::from_secs(4)
    }
}

/// A contact's stored `EndpointAddr` for the ticket's publisher. Best-effort:
/// a bad or mismatched address blob is ignored (the dial then falls back to
/// discovery, as before).
fn known_addr(ticket: &BroadcastTicket, addrs_json: &str) -> Option<EndpointAddr> {
    let addr = serde_json::from_str::<EndpointAddr>(addrs_json).ok()?;
    (addr.id == ticket.peer() && !addr.addrs.is_empty()).then_some(addr)
}

#[ffi_export]
pub fn media_live_subscribe(ticket: char_p::Ref<'_>) -> u8 {
    subscribe_inner(ticket.to_str(), None)
}

/// Subscribe with the peer's known addresses merged in (#157).
///
/// A serialized live ticket carries the endpoint id only — `LiveTicket`'s
/// wire form is `iroh-live:<base64 id>/<name>` — so every dial resolves the
/// publisher through pkarr/DNS. That is what failed with "No addressing
/// information available" while interfaces flipped (2026-11-04 drive), and it
/// is why first dials can take 10-20 s. The shell already holds the contact's
/// `endpoint_addr` from pairing, so it passes it here; when the ids match and
/// the ticket has no addresses of its own, they are used directly.
#[ffi_export]
pub fn media_live_subscribe_with_addrs(ticket: char_p::Ref<'_>, addrs: char_p::Ref<'_>) -> u8 {
    subscribe_inner(ticket.to_str(), Some(addrs.to_str()))
}

fn subscribe_inner(ticket: &str, addrs: Option<&str>) -> u8 {
    media_live_unsubscribe();
    SUB_KICK.store(false, Ordering::Relaxed);
    set_sub_state(SUB_CONNECTING);
    let Ok(ticket) = ticket.parse::<BroadcastTicket>() else {
        return 1;
    };
    // A ticket names the publisher's id only. The shell holds the contact's
    // dial address (the invite's `addr=`), which dials without discovery.
    let addr = addrs
        .filter(|addrs| !addrs.is_empty())
        .and_then(|addrs| known_addr(&ticket, addrs));
    let session = match tokio_executor(session_handle()) {
        Ok(session) => session,
        Err(err) => {
            set_sub_state(SUB_FAILED);
            *LAST_ERROR.lock().expect("error poisoned") = Some(format!("{err:#}"));
            return 1;
        }
    };
    start_listen_leg(session, ticket, addr)
}

/// Start a listen leg for `ticket`'s publisher on `session`'s node and add it
/// to the call session's member map. Returns 0, or 1 when no session holds it.
fn start_listen_leg(session: SessionHandle, ticket: BroadcastTicket, addr: Option<EndpointAddr>) -> u8 {
    let peer = ticket.peer();
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    // A dedicated thread drives the leg on the shared executor's runtime, so
    // the session's node and this subscription share one reactor.
    let runtime = crate::util::TOKIO_EXECUTOR.handle().clone();
    let handle = std::thread::spawn(move || {
        let result = runtime.block_on(async move {
            // Attempt loop: the subscriber sometimes sees the remote track end
            // mid-call (observed on iOS ~20s in: mux consumer returned Ok(None)
            // while the holder kept publishing — suspected relay/route boundary).
            // A silent early end means dead air until hangup; treat it as
            // transient and re-subscribe while the call is still active.
            let mut samples: Vec<f32> = Vec::new();
            let mut decoded_channels = 1u32;
            let mut timing = String::from("frame,arrival_us,arrival_gap_us,pts_us,media_gap_us,samples,sink_buffered_ms\n");
            let mut decoded_total = 0usize;
            let capture_started = std::time::Instant::now();
            let mut attempt = 0u32;
            // Stalls since the last attempt that delivered audio. The first
            // resubscribes on the same connection; a second in a row redials.
            let mut stalls_in_row = 0u32;
            let result: anyhow::Result<()>;
            loop {
                if thread_stop.load(Ordering::Relaxed) {
                    result = Ok(());
                    break;
                }
                attempt += 1;
                if attempt > 1 {
                    set_sub_state(SUB_RECONNECTING);
                }
                let res = subscribe_decode_once(
                    session.clone(),
                    ticket.clone(),
                    addr.clone(),
                    &thread_stop,
                    &mut samples,
                    &mut timing,
                    &mut decoded_total,
                    capture_started,
                    attempt,
                    &mut decoded_channels,
                    &mut stalls_in_row,
                )
                .await;
                match res {
                    Ok(_ended_by_stop) => {
                        result = Ok(());
                        break;
                    }
                    Err(e) => {
                        // Every failed attempt while the call is up is
                        // transient. An early track end is the publisher
                        // restarting; a dial/setup failure is the network
                        // still settling — the drive run of 2026-11-04 died
                        // for good on "No addressing information available"
                        // when the phone's network flipped (Wi-Fi returning)
                        // mid-redial, and sat at "Audio lost" for 4.5 min.
                        // Retry both; only a stop ends the loop.
                        let backoff = std::time::Duration::from_millis(
                            (100u64 << attempt.min(4)).min(1_000),
                        );
                        if e.downcast_ref::<SubscribeEndError>().is_none() {
                            tracing::warn!(attempt, "subscribe attempt failed: {e:#}; retrying");
                        }
                        eprintln!(
                            "[media] subscribe attempt={attempt} failed decoded={decoded_total} frames; resubscribing in {}ms: {e:#}",
                            backoff.as_millis()
                        );
                        // The shell can cut the wait short (foreground return):
                        // poll instead of one long sleep.
                        let deadline = tokio::time::Instant::now() + backoff;
                        while tokio::time::Instant::now() < deadline {
                            if SUB_KICK.load(Ordering::Relaxed) {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                    }
                }
            }
            // The endpoint is the call session's: it outlives this leg and
            // shuts down with the session (`release_session_if_idle`).
            // A group member's capture is named for it, so legs don't overwrite each other.
            let suffix = if is_group_peer(&peer) {
                format!("-{}", peer.fmt_short())
            } else {
                String::new()
            };
            let received_wav = media_path(&format!("received{suffix}.wav"));
            let received_timing = media_path(&format!("received{suffix}-timing.csv"));
            write_wav(&received_wav, &samples, decoded_channels)?;
            std::fs::write(&received_timing, timing)?;
            eprintln!("[media] live audio captures wav={} timing={}", received_wav.display(), received_timing.display());
            archive_call_capture(&received_wav, &received_timing);
            PLAYBACK_CONTROL.lock().unwrap().take();
            release_mixer_if_idle();
            result
        });
        if let Err(err) = result {
            set_sub_state(SUB_FAILED);
            tracing::error!("live subscribe failed: {err:#}");
            eprintln!("live subscribe failed: {err:#}");
        }
    });
    let subscriber = Subscriber {
        stop,
        handle: Some(handle),
    };
    let mut slot = SESSION.lock().unwrap_or_else(|poison| poison.into_inner());
    match slot.as_mut() {
        Some(call) => {
            call.subscribers.insert(peer, subscriber);
        }
        None => {
            // The session ended under us (a concurrent stop): end the leg.
            drop(slot);
            stop_subscriber(subscriber);
            set_sub_state(SUB_IDLE);
            return 1;
        }
    }
    0
}

/// Stop one listen leg and wait for its thread (which writes the call capture).
fn stop_subscriber(mut subscriber: Subscriber) {
    subscriber.stop.store(true, Ordering::Relaxed);
    if let Some(handle) = subscriber.handle.take() {
        let _ = handle.join();
    }
}

/// Why one subscribe attempt ended.
#[derive(Debug)]
enum SubscribeEndError {
    #[allow(dead_code)]
    ByStop,
    Transient,
}

impl std::fmt::Display for SubscribeEndError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ByStop => write!(f, "stopped by caller"),
            Self::Transient => write!(f, "track ended early (transient)"),
        }
    }
}

impl std::error::Error for SubscribeEndError {}

/// Phone-side makeup gain for live-call playback.
///
/// Historically this compensated for `.playAndRecord` + `.voiceChat` routing
/// playback through VoiceProcessingIO, which drops output gain with no public
/// API to adjust it (StackOverflow 17528057 / 13502293); the 2026-09-25 device
/// fix measured +12 dB (docs/troubleshooting.md, "iOS live-call audio too
/// quiet"). Playback now renders on the shells' AVAudioEngine bus at normal
/// level (verified 2026-10-07), so the default is unity. Keep the knob for
/// devices/paths that still end up quiet: override with `IDFON_PLAYBACK_GAIN_DB`.
const PLAYBACK_GAIN_DB_DEFAULT: f32 = 0.0;

fn playback_gain() -> f32 {
    // VoiceProcessingIO's output drop is iOS-only; leave other platforms at
    // unity (macOS has the media volume slider to compensate).
    if !cfg!(target_os = "ios") {
        return 1.0;
    }
    static GAIN: std::sync::OnceLock<f32> = std::sync::OnceLock::new();
    *GAIN.get_or_init(|| {
        let db = std::env::var("IDFON_PLAYBACK_GAIN_DB")
            .ok()
            .and_then(|value| value.trim().parse::<f32>().ok())
            .filter(|value| value.is_finite())
            .unwrap_or(PLAYBACK_GAIN_DB_DEFAULT);
        10f32.powf(db / 20.0)
    })
}

/// Apply [`playback_gain`] to interleaved f32 PCM bytes, soft-clipping with
/// `tanh`. Returns the input unchanged at unity gain.
fn apply_playback_gain(data: &[u8]) -> Vec<u8> {
    let gain = playback_gain();
    if gain == 1.0 {
        return data.to_vec();
    }
    let mut out = Vec::with_capacity(data.len());
    for chunk in data.chunks_exact(4) {
        let sample = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        out.extend_from_slice(&(sample * gain).tanh().to_le_bytes());
    }
    out
}

/// One subscribe-and-decode attempt: connect, wait for the audio catalog,
/// decode into `samples`/`timing` until the track ends or `stop` is set.
/// Returns Ok(true) when the stop flag ended it, Ok(false) on an early track
/// end (caller decides whether to retry), Err on transport/setup failure.
async fn subscribe_decode_once(
    session: SessionHandle,
    ticket: BroadcastTicket,
    addr: Option<EndpointAddr>,
    stop: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    samples: &mut Vec<f32>,
    timing: &mut String,
    decoded_total: &mut usize,
    capture_started: std::time::Instant,
    attempt: u32,
    channels_out: &mut u32,
    stalls_in_row: &mut u32,
) -> anyhow::Result<bool> {
    // The endpoint is the session's, not the attempt's: a retry after a path
    // change dials from a warm endpoint instead of bootstrapping one
    // (docs/media-adaptation.md §C2, the 4.2 s hole).
    // The peer session this attempt used. An attempt that ends early closes it, so the retry
    // redials: `Node::connect` hands back an existing session as it is, and a session whose path
    // died (Wi-Fi to cellular) stays open until QUIC's idle timeout, so retries would otherwise
    // keep subscribing over the dead one (iroh-live upgrade, two-iPhone test 2026-11-18).
    let mut used_session: Option<iroh_live::Session> = None;
    // Set when this attempt should close its connection so the retry redials.
    // Both call directions share the connection, so closing it also cuts the
    // peer's leg; the round-2 device test (2026-11-18) showed each phone's
    // close ending the other's attempt, back and forth. Only a stall with no
    // selected path left, or a second stall in a row, closes it now.
    let mut redial = false;
    let attempt_started = std::time::Instant::now();
    let res = async {
        // Dial the known address first: a bare-id dial waits on discovery,
        // which failed outright with the relay off (iroh-live upgrade, step 1).
        if let Some(addr) = addr {
            tracing::info!(
                target: "idfon.media",
                attempt,
                peer = %addr.id.fmt_short(),
                relays = addr.relay_urls().count(),
                ips = addr.ip_addrs().count(),
                "subscribe dial"
            );
            let dialed =
                tokio::time::timeout(std::time::Duration::from_secs(8), session.node.connect(addr))
                    .await
                    .map_err(|_| anyhow::anyhow!("dial timed out"))??;
            used_session = Some(dialed);
        }
        let subscription = session
            .node
            .subscribe(ticket.path().clone(), Reach::Direct(ticket.peer()))
            .await?;
        if let Some(serving) = subscription.session() {
            used_session = Some(serving);
        }
        // `path_down`: the selected path closed and no other path has been
        // selected since. It does not end the attempt (iroh opens, closes and
        // migrates paths routinely); at a stall it means the connection is
        // likely dead, so redial at once instead of resubscribing over it.
        let path_down = std::sync::Arc::new(AtomicBool::new(false));
        let peer_conn = subscription
            .session()
            .map(|peer_session| peer_session.connection().clone());
        if let Some(conn) = peer_conn.clone() {
            let flag = path_down.clone();
            let watch_stop = stop.clone();
            let mut selected = conn
                .paths()
                .iter()
                .find(|path| path.is_selected())
                .map(|path| path.id());
            tokio::spawn(async move {
                use n0_future::StreamExt;
                let mut events = conn.path_events();
                while let Some(event) = events.next().await {
                    // Stop with the call, or once this attempt is gone: a
                    // resubscribe on the same connection runs its own watcher.
                    if watch_stop.load(Ordering::Relaxed) || std::sync::Arc::strong_count(&flag) == 1 {
                        break;
                    }
                    match event {
                        iroh::endpoint::PathEvent::Selected { id, .. } => {
                            tracing::info!(target: "idfon.media", path = ?id, "path selected");
                            selected = Some(id);
                            flag.store(false, Ordering::Relaxed);
                        }
                        iroh::endpoint::PathEvent::Closed { id, .. } if selected == Some(id) => {
                            tracing::info!(target: "idfon.media", path = ?id, "selected path closed");
                            selected = None;
                            flag.store(true, Ordering::Relaxed);
                        }
                        iroh::endpoint::PathEvent::Lagged { .. } => {
                            tracing::info!(target: "idfon.media", "path events lagged");
                        }
                        _ => {}
                    }
                }
            });
        }
        let broadcast = subscription.broadcast();
        let remote = subscription.remote_broadcast();
        let signals = crate::health::signals_of(&subscription);
        // Health checks sample this session's downlink (docs/media-adaptation.md §B2.2).
        if let Some(conn) = peer_conn.as_ref() {
            crate::health::attach(&session.endpoint, conn, signals.clone());
        }
        // The publisher writes an empty catalog first and adds the audio
        // rendition once its encoder is probed, so the first snapshot a
        // fast subscriber sees may not carry it yet: wait for it.
        let mut catalog_watch = remote.catalog();
        let catalog = loop {
            if stop.load(Ordering::Relaxed) {
                return Ok(true);
            }
            if let Some(catalog) = catalog_watch
                .borrow()
                .as_ref()
                .filter(|catalog| !catalog.audio.renditions.is_empty())
                .cloned()
            {
                break catalog;
            }
            let _ = tokio::time::timeout(
                std::time::Duration::from_millis(100),
                catalog_watch.changed(),
            )
            .await;
        };
        // A publisher may offer the same audio in several codecs (LiquidCodec + Opus); the subscriber
        // picks, and can switch mid-call (crate::audio_rendition).
        let preferred = AUDIO_PREFERENCE
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        let mut policy = crate::audio_rendition::Policy::new();
        let renditions = audio_renditions(&catalog);
        let Some((chosen, reason)) = policy
            .choose_with_reason(
                &renditions,
                preferred.as_deref(),
                crate::audio_rendition::decodable,
            )
            .map(|(chosen, reason)| (chosen.clone(), reason))
        else {
            anyhow::bail!("broadcast has no audio track this side can decode");
        };
        tracing::info!(
            target: "idfon.media",
            offered = ?renditions.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
            preference = preferred.as_deref().unwrap_or("none"),
            chosen = %chosen.name,
            %reason,
            attempt,
            "audio rendition chosen"
        );
        let config = catalog
            .audio
            .renditions
            .get(&chosen.name)
            .ok_or_else(|| anyhow::anyhow!("audio catalog missing"))?
            .clone();
        let name = chosen.name.clone();
        let mut current = chosen;
        let liquid_track = current.codec == "liquid";
        *INCOMING_AUDIO_CODEC
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(current.codec.clone());
        let mut output = moq_audio::decode::Output::default();
        output.format = Format::F32;
        output.sample_rate = Some(48_000);
        // Native channel count from the catalog (mono or stereo): the shell
        // renders stereo natively — old mono publishers decode as mono. With several
        // renditions the layout is pinned to the first one's, so a switch keeps feeding
        // playback the same channel count (moq-audio remixes).
        output.layout = if renditions.len() > 1 {
            moq_audio::Layout::from_channels(config.channel_count).ok()
        } else {
            None
        };
        let mut decode = moq_audio::decode::Options::default();
        decode.output = output;
        // Keep a bounded playout budget so a late group is not discarded
        // immediately. This is the decoder's jitter tolerance; it must sit at
        // or above the playout target ceiling (`seam::PLAYOUT_TARGET_MAX`),
        // or a 100–250 ms burst is dropped on arrival and becomes a hole the
        // jitter buffer can never smooth over. (The old `max_age`.)
        decode.delay.max = Some(std::time::Duration::from_millis(250));
        // Live playout rebuilds lost packets from the next packet's in-band FEC copy or
        // DRED, concealing what neither covers (moq-audio's recovering decode; LiquidCodec
        // recovers from its redundant copy by default). Holes past the codec's burst bound
        // still read as gaps.
        decode.recover = Some(true);
        let mut consumer =
            moq_audio::decode::Consumer::new(&broadcast, &config, name, decode.clone()).await?;
        let input = AudioInput {
            format: Format::F32,
            sample_rate: 48_000,
            channels: consumer.layout().channels(),
        };
        *channels_out = input.channels;
        crate::health::set_channels(input.channels as u64);
        // Shell playback renders remote call audio on the app's playback bus,
        // which is the VoiceProcessingIO echo reference (docs/audio-media.md);
        // the bundled device engine is the fallback when no callback is set.
        // LiquidCodec packets carry 80 ms each (and recovering a lost one needs
        // the next): hold a packet plus slack, not Opus's 60 ms.
        let playout_floor =
            liquid_track.then_some(std::time::Duration::from_millis(LIQUID_PLAYOUT_FLOOR_MS));
        let mut floor_handle: Option<idfon_media::seam::PlayoutFloor> = None;
        let mut playback: Box<dyn AudioPlayback> = match *AUDIO_PLAYBACK_CB.lock().unwrap() {
            Some((ctx, cb)) => {
                // Every leg feeds the mixer, which hands the shell one stream
                // (a 1:1 call is a mix of one).
                let member = mixer_member(ctx, cb, input.channels);
                let channels = input.channels;
                let paced = PacedPlayback::new(
                    input,
                    Box::new(move |pcm| {
                        let samples: Vec<f32> = pcm
                            .chunks_exact(4)
                            .map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
                            .collect();
                        member.push(&samples, channels);
                    }),
                );
                if let Some(floor) = playout_floor {
                    paced.set_target_floor(floor);
                }
                floor_handle = Some(paced.floor_handle());
                Box::new(paced)
            }
            None => {
                // Legs share one engine, which mixes their sinks.
                let shared = PLAYBACK_ENGINE.lock().unwrap().clone();
                let device_playback = match shared {
                    Some(engine) => DevicePlayback::on_engine(engine, input)?,
                    None => {
                        let device = OUTPUT_DEVICE.lock().unwrap().clone();
                        DevicePlayback::open(device, input).await?
                    }
                };
                PLAYBACK_ENGINE
                    .lock()
                    .unwrap()
                    .replace(device_playback.engine());
                let control = device_playback.control();
                control.set_volume(VOLUME.load(Ordering::Relaxed) as f32 / 100.0);
                *PLAYBACK_CONTROL.lock().unwrap() = Some(control);
                Box::new(device_playback)
            }
        };
        // A/V sync: video presents against the audio playout clock (av.rs).
        // Only the 1:1 leg drives it; group members skip it.
        let anchors_video = !is_group_peer(&ticket.peer());
        if anchors_video {
            crate::av::reset();
        }
        // Match PacedPlayback's target so startup buffering is one hold, not
        // a second delay before the jitter buffer can start draining.
        let startup_prebuffer_frames: usize = (playout_floor
            .map_or(idfon_media::seam::PLAYOUT_TARGET_MS, |floor| {
                floor.as_millis() as u64
            }) as usize
            * 48_000)
            / 1_000;
        let mut startup = Vec::new();
        let mut started = false;
        let mut last_arrival = None;
        let mut expected_pts_us: Option<u128> = None;
        // The holder publishes continuous frames (silence on underflow), so a
        // multi-second frame gap while the call is active means the
        // subscription stalled (observed on iOS: the publisher's serve stream
        // died without SUBSCRIBE_END, and consumer.read() then never returns
        // — neither frames nor a track end). Treat it like an early end.
        let mut last_frame = std::time::Instant::now();
        let mut first_frame_seen = false;
        let mut last_choice = std::time::Instant::now();
        type PendingSwitch = std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = anyhow::Result<(
                            crate::audio_rendition::Rendition,
                            moq_audio::decode::Consumer,
                        )>,
                    > + Send,
            >,
        >;
        let mut pending_switch: Option<PendingSwitch> = None;
        // The current consumer's loss counters already folded into the health record.
        let mut losses_seen = moq_audio::decode::Losses::default();
        loop {
            if stop.load(Ordering::Relaxed) {
                if !started && !startup.is_empty() {
                    let _ = playback.write(&apply_playback_gain(&startup), 0);
                }
                return Ok(true);
            }
            let relay = subscription
                .serving_link()
                .is_some_and(|link| link.sample.relayed);
            // A fresh attempt gets the full window before its first frame: the 500 ms window after a
            // path change is for noticing a stream that went quiet, and a resubscribe needs longer
            // than that to deliver anything, so it used to end every retry before audio could arrive.
            let window = if first_frame_seen {
                stall_window(relay)
            } else {
                stall_window(relay).max(normal_stall_window(relay))
            };
            if last_frame.elapsed() >= window {
                *stalls_in_row += 1;
                let no_path = path_down.load(Ordering::Relaxed);
                let conn_closed = peer_conn.as_ref().is_some_and(|conn| conn.close_reason().is_some());
                redial = no_path || conn_closed || *stalls_in_row >= 2;
                tracing::warn!(
                    attempt,
                    decoded = *decoded_total,
                    media_secs = samples.len() as f64 / (48_000.0 * input.channels as f64),
                    stall_secs = last_frame.elapsed().as_secs_f64(),
                    stalls_in_row = *stalls_in_row,
                    no_path,
                    conn_closed,
                    redial,
                    "subscribe stalled: no frames within the window; treating as early track end"
                );
                set_sub_state(SUB_RECONNECTING);
                return Err(anyhow::anyhow!(SubscribeEndError::Transient));
            }
            // Rendition choice: re-checked every 500 ms against the catalog, what this side can
            // decode now, and the network. A change opens the new track alongside the old one.
            if pending_switch.is_none()
                && last_choice.elapsed() >= std::time::Duration::from_millis(500)
            {
                last_choice = std::time::Instant::now();
                {
                    let signals = signals.borrow();
                    policy.sample(last_choice, signals.loss_rate, signals.delivery_bps);
                }
                let Some(catalog) = remote.catalog().borrow().clone() else {
                    continue;
                };
                let renditions = audio_renditions(&catalog);
                let want = policy
                    .choose(
                        &renditions,
                        preferred.as_deref(),
                        crate::audio_rendition::decodable,
                    )
                    .filter(|want| want.name != current.name)
                    .cloned();
                let want_config = want
                    .as_ref()
                    .and_then(|want| catalog.audio.renditions.get(&want.name).cloned());
                if let (Some(want), Some(want_config)) = (want, want_config) {
                    tracing::info!(target: "idfon.media", from = %current.codec, to = %want.codec,
                        constrained = policy.constrained(), "audio rendition switch: opening");
                    let bc = broadcast.clone();
                    let decode = decode.clone();
                    pending_switch = Some(Box::pin(async move {
                        let consumer = moq_audio::decode::Consumer::new(
                            &bc,
                            &want_config,
                            want.name.clone(),
                            decode,
                        )
                        .await?;
                        anyhow::Ok((want, consumer))
                    }));
                }
            }
            let frame = tokio::select! {
                ready = async { pending_switch.as_mut().expect("guarded").await }, if pending_switch.is_some() => {
                    pending_switch = None;
                    match ready {
                        Ok((want, mut next)) => {
                            // Cut over at the stream position: drop what the old track already
                            // played, trim an overlapping frame, accept a small gap.
                            match tokio::time::timeout(std::time::Duration::from_millis(1_000), next_frame(&mut next)).await {
                                Ok(Ok(Some(mut first))) => {
                                    if let Some(expected) = expected_pts_us {
                                        let pts = first.timestamp.as_micros().unwrap_or(0);
                                        if pts < expected {
                                            let skip = ((expected - pts) * 48_000 / 1_000_000) as usize
                                                * input.channels as usize * 4;
                                            first.data = first.data.slice(skip.min(first.data.len())..);
                                            first.timestamp = moq_net::Timestamp::from_micros(expected as u64)
                                                .unwrap_or(first.timestamp);
                                        }
                                    }
                                    tracing::info!(target: "idfon.media", from = %current.codec, to = %want.codec,
                                        "audio rendition switch: cut over");
                                    if let Some(floor) = floor_handle.as_ref() {
                                        floor.set(if want.codec == "liquid" {
                                            std::time::Duration::from_millis(LIQUID_PLAYOUT_FLOOR_MS)
                                        } else {
                                            std::time::Duration::ZERO
                                        });
                                    }
                                    *INCOMING_AUDIO_CODEC.lock().unwrap_or_else(|poison| poison.into_inner()) =
                                        Some(want.codec.clone());
                                    consumer = next;
                                    losses_seen = moq_audio::decode::Losses::default();
                                    current = want;
                                    if first.data.is_empty() { continue; }
                                    Some(first)
                                }
                                _ => {
                                    tracing::warn!(target: "idfon.media", to = %want.codec,
                                        "audio rendition switch: new track gave no audio; staying");
                                    continue;
                                }
                            }
                        }
                        Err(err) => {
                            tracing::warn!(target: "idfon.media", error = %err, "audio rendition switch failed; staying");
                            continue;
                        }
                    }
                }
                read = tokio::time::timeout(std::time::Duration::from_millis(200), next_frame(&mut consumer)) => match read {
                    Ok(frame) => frame?,
                    Err(_) => continue,
                },
            };
            let Some(frame) = frame else {
                // Track ended. Normal only if the caller stopped it; otherwise
                // the holder is still publishing and this is a transient break.
                let media_secs = samples.len() as f64 / (48_000.0 * input.channels as f64);
                tracing::warn!(
                    attempt,
                    decoded = decoded_total,
                    media_secs,
                    wall_secs = capture_started.elapsed().as_secs_f64(),
                    "subscribe track ended early"
                );
                return Err(anyhow::anyhow!(SubscribeEndError::Transient));
            };
            last_frame = std::time::Instant::now();
            if !first_frame_seen {
                first_frame_seen = true;
                *stalls_in_row = 0;
                tracing::info!(
                    target: "idfon.media",
                    attempt,
                    elapsed_ms = attempt_started.elapsed().as_millis() as u64,
                    codec = %current.codec,
                    "first audio frame of attempt"
                );
            }
            // Frames flow again: the foreground kick has done its job.
            SUB_KICK.store(false, Ordering::Relaxed);
            let decoded = AUDIO_FRAMES_DECODED.fetch_add(1, Ordering::Relaxed) + 1;
            *decoded_total = decoded as usize;
            let arrived = std::time::Instant::now();
            let arrival_us = capture_started.elapsed().as_micros();
            let arrival_gap_us = last_arrival
                .map(|last: std::time::Instant| arrived.duration_since(last).as_micros())
                .unwrap_or(0);
            let pts_us = frame.timestamp.as_micros().unwrap_or(0);
            let media_gap_us =
                expected_pts_us.map(|expected| pts_us.saturating_sub(expected)).unwrap_or(0);
            let frame_samples = frame.data.len() / 4;
            let pts_gap_us = frame_pts_gap_us(frame.data.as_ref(), input.channels);
            expected_pts_us = Some(pts_us + pts_gap_us);
            last_arrival = Some(arrived);
            // Per-call quality record: one fold tick per decoded frame (§C1).
            crate::health::note_frame(pts_us as u64, pts_gap_us as u64);
            let losses = consumer.losses();
            crate::health::note_losses(
                losses.recovered.saturating_sub(losses_seen.recovered),
                losses.concealed.saturating_sub(losses_seen.concealed),
            );
            losses_seen = losses;
            if decoded <= 3 || decoded % 100 == 0 || arrival_gap_us > 30_000 || media_gap_us > 1_000 {
                eprintln!("[media] audio frame decoded #{} arrival_gap={}us media_gap={}us samples={}", decoded, arrival_gap_us, media_gap_us, frame_samples);
            }
            timing.push_str(&format!("{decoded},{arrival_us},{arrival_gap_us},{pts_us},{media_gap_us},{frame_samples},{:.2}\n", playback.buffered().as_secs_f64() * 1_000.0));
            if !started {
                startup.extend_from_slice(&frame.data);
                if startup.len() / 4 < startup_prebuffer_frames * input.channels as usize {
                    continue;
                }
                let batch_us = (startup.len() as u128 / 4 / input.channels.max(1) as u128)
                    * 1_000_000
                    / input.sample_rate.max(1) as u128;
                playback.write(&apply_playback_gain(&startup), pts_us as u64)?;
                // The batch plays as one block starting at pts_us - batch_us.
                if anchors_video {
                    crate::av::anchor_audio(
                        pts_us.saturating_sub(batch_us) as u64,
                        playback.buffered(),
                    );
                }
                startup.clear();
                started = true;
            } else {
                // NetEq-style shrink: the sink parks ahead-writes and never
                // drains back on its own, so discard down to the playout
                // target instead of ratcheting delay after every stall.
                // Trim only genuine overflow: above the adaptive target
                // ceiling plus a burst. A tighter cap (80 ms until
                // 2026-11-04) pulled the buffer back under the cellular
                // arrival bursts it had just absorbed, which clicked.
                const SINK_TARGET_LATENCY: f64 = 0.40;
                let buffered = playback.buffered().as_secs_f64();
                let mut data = frame.data.as_ref();
                let mut trim_samples = 0usize;
                if buffered > SINK_TARGET_LATENCY && data.len() >= 4 {
                    // Drop whole frames only: a half frame would swap the
                    // channels of every later sample on a stereo call.
                    let mut excess_samples = (((buffered - SINK_TARGET_LATENCY)
                        * 48_000.0) as usize)
                        .min(data.len() / 4);
                    excess_samples -= excess_samples % input.channels as usize;
                    trim_samples = excess_samples;
                    data = &data[excess_samples * 4..];
                }
                // Anchored at the pts of what is actually written: trimmed
                // front samples moved the playout start later.
                let written_pts_us = pts_us
                    + (trim_samples as u128 / input.channels.max(1) as u128) * 1_000_000
                        / input.sample_rate.max(1) as u128;
                playback.write(&apply_playback_gain(data), pts_us as u64)?;
                if anchors_video {
                    crate::av::anchor_audio(written_pts_us as u64, playback.buffered());
                }
            }
            // Audio has reached the sink: the call is audible again.
            set_sub_state(SUB_PLAYING);
            for chunk in frame.data.chunks_exact(4) {
                samples.push(f32::from_le_bytes(chunk.try_into().unwrap()));
            }
        }
    };
    let result = res.await;
    if result.is_err() && redial {
        if let Some(peer_session) = used_session.take() {
            tracing::info!(
                target: "idfon.media",
                attempt,
                peer = %peer_session.peer().fmt_short(),
                elapsed_ms = attempt_started.elapsed().as_millis() as u64,
                "subscribe attempt ended early; closing its session so the retry redials"
            );
            peer_session.close(moq_net::Error::Cancel);
            peer_session
                .connection()
                .close(0u32.into(), b"subscribe stalled; redialing");
        }
    } else if result.is_err() {
        tracing::info!(
            target: "idfon.media",
            attempt,
            elapsed_ms = attempt_started.elapsed().as_millis() as u64,
            "subscribe attempt ended early; resubscribing on the same session"
        );
    }
    result
}

/// The next decoded frame, skipping the track-boundary events (`Read::Gap`,
/// `Read::End`) the old consumer folded into its frame stream. `None` when the
/// track ends.
async fn next_frame(
    consumer: &mut moq_audio::decode::Consumer,
) -> Result<Option<AudioFrame>, moq_audio::Error> {
    loop {
        match consumer.read().await? {
            Some(moq_audio::decode::Read::Frame(frame)) => return Ok(Some(frame)),
            Some(_) => continue,
            None => return Ok(None),
        }
    }
}

/// Expected pts gap in µs for one frame: f32 samples at 48 kHz over `channels`.
fn frame_pts_gap_us(data: &[u8], channels: u32) -> u128 {
    (data.len() / 4) as u128 * 1_000_000 / (48_000 * channels as u128)
}

/// Calls whose received audio is kept in `<media_base>/call-captures/`.
const KEPT_CALL_CAPTURES: usize = 5;

/// Copy the call's `received.wav` and `received-timing.csv` to
/// `call-captures/<unix_ms>-received.{wav,csv}`, and keep only the newest
/// `KEPT_CALL_CAPTURES` calls. `received.wav` is overwritten by the next call;
/// a tester who restarts the app to reset state still has the earlier calls.
/// Best effort: a failed copy only logs.
fn archive_call_capture(wav: &std::path::Path, timing: &std::path::Path) {
    let dir = media_base().join("call-captures");
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let result = (|| -> std::io::Result<()> {
        std::fs::create_dir_all(&dir)?;
        std::fs::copy(wav, dir.join(format!("{stamp:013}-received.wav")))?;
        std::fs::copy(timing, dir.join(format!("{stamp:013}-received-timing.csv")))?;
        Ok(())
    })();
    if let Err(err) = result {
        eprintln!("[media] call capture archive failed: {err}");
        return;
    }
    prune_call_captures(&dir, KEPT_CALL_CAPTURES);
}

/// Remove all but the newest `keep` calls in `dir`. A call is the files that
/// share a `<unix_ms>-` prefix; the zero-padded stamp sorts by name.
fn prune_call_captures(dir: &std::path::Path, keep: usize) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    let mut by_call: std::collections::BTreeMap<String, Vec<PathBuf>> = Default::default();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some((stamp, _)) = name.split_once('-') {
            if !stamp.is_empty() && stamp.bytes().all(|b| b.is_ascii_digit()) {
                by_call.entry(stamp.to_owned()).or_default().push(entry.path());
            }
        }
    }
    let excess = by_call.len().saturating_sub(keep);
    for files in by_call.into_values().take(excess) {
        for file in files {
            let _ = std::fs::remove_file(file);
        }
    }
}

fn write_wav(path: &std::path::Path, samples: &[f32], channels: u32) -> anyhow::Result<()> {
    write_wav_at(path, samples, channels, 48_000)
}

fn write_wav_at(
    path: &std::path::Path,
    samples: &[f32],
    channels: u32,
    sample_rate: u32,
) -> anyhow::Result<()> {
    std::fs::create_dir_all(path.parent().unwrap_or(std::path::Path::new(".")))?;
    let mut bytes = Vec::with_capacity(44 + samples.len() * 2);
    let data_len = (samples.len() * 2) as u32;
    bytes.extend_from_slice(b"RIFF");
    bytes.extend_from_slice(&(36 + data_len).to_le_bytes());
    bytes.extend_from_slice(b"WAVEfmt ");
    bytes.extend_from_slice(&16u32.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&(channels as u16).to_le_bytes());
    bytes.extend_from_slice(&sample_rate.to_le_bytes());
    bytes.extend_from_slice(&(sample_rate * channels * 2).to_le_bytes());
    bytes.extend_from_slice(&((channels * 2) as u16).to_le_bytes());
    bytes.extend_from_slice(&16u16.to_le_bytes());
    bytes.extend_from_slice(b"data");
    bytes.extend_from_slice(&data_len.to_le_bytes());
    for sample in samples {
        bytes.extend_from_slice(&((sample.clamp(-1.0, 1.0) * 32767.0) as i16).to_le_bytes());
    }
    std::fs::write(path, bytes)?;
    Ok(())
}

/// The layout a WAV header declares: playback must honor it instead of
/// assuming mono 48 kHz s16 (stereo memos and the shells' 16 kHz memos both
/// arrive through the same file). Chunk walk, so extra chunks (e.g. `fact`)
/// are skipped and `data` may precede `fmt `; a `data` length that overshoots
/// the buffer is clamped so truncated captures still play what they have.
struct WavFormat {
    sample_rate: u32,
    channels: u32,
    bits_per_sample: u16,
    data_offset: usize,
    data_len: usize,
}

fn wav_format(data: &[u8]) -> Option<WavFormat> {
    if data.len() < 44 || &data[0..4] != b"RIFF" || &data[8..12] != b"WAVE" {
        return None;
    }
    let mut format: Option<(u32, u32, u16)> = None;
    let mut payload: Option<(usize, usize)> = None;
    let mut pos = 12;
    while pos + 8 <= data.len() {
        let id = &data[pos..pos + 4];
        let len = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().ok()?) as usize;
        let body = pos + 8;
        if id == b"fmt " {
            if len < 16 || body + 16 > data.len() {
                return None;
            }
            format = Some((
                u32::from_le_bytes(data[body + 4..body + 8].try_into().ok()?),
                u16::from_le_bytes(data[body + 2..body + 4].try_into().ok()?) as u32,
                u16::from_le_bytes(data[body + 14..body + 16].try_into().ok()?),
            ));
        } else if id == b"data" {
            payload = Some((body, len.min(data.len() - body)));
        } else if body.checked_add(len)? > data.len() {
            return None;
        }
        pos = body.checked_add(len)? + (len & 1);
    }
    let (sample_rate, channels, bits_per_sample) = format?;
    let (data_offset, data_len) = payload?;
    Some(WavFormat {
        sample_rate,
        channels,
        bits_per_sample,
        data_offset,
        data_len,
    })
}

#[ffi_export]
pub fn media_live_unsubscribe() {
    media_recording_stop_playback();
    *INCOMING_AUDIO_CODEC
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = None;
    *AUDIO_PREFERENCE
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = None;
    // Every listen leg: the 1:1 shell has one; a group call holds one per member.
    let subscribers: Vec<Subscriber> = SESSION
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_mut()
        .map(|session| session.subscribers.drain().map(|(_, sub)| sub).collect())
        .unwrap_or_default();
    for subscriber in subscribers {
        stop_subscriber(subscriber);
    }
    GROUP_PEERS.lock().unwrap_or_else(|poison| poison.into_inner()).take();
    release_mixer_if_idle();
    release_session_if_idle();
    SUB_KICK.store(false, Ordering::Relaxed);
    set_sub_state(SUB_IDLE);
}

/// Add one remote member to the call (group call, docs/group-live-chat.md G0):
/// subscribe to `ticket`'s publisher without ending the other legs. `addrs` is
/// the member's dial address JSON, as for `media_live_subscribe_with_addrs`
/// (empty when unknown). Every member's audio plays through one mix.
///
/// Returns 0 on success, 1 for a bad ticket or no session, 2 when the member is
/// already in the call, 3 when the call is full (12 people).
#[ffi_export]
pub fn media_live_add_member(ticket: char_p::Ref<'_>, addrs: char_p::Ref<'_>) -> u8 {
    let Ok(ticket) = ticket.to_str().parse::<BroadcastTicket>() else {
        return 1;
    };
    let peer = ticket.peer();
    {
        let slot = SESSION.lock().unwrap_or_else(|poison| poison.into_inner());
        if let Some(session) = slot.as_ref() {
            if session.subscribers.contains_key(&peer) {
                return 2;
            }
            if session.subscribers.len() >= MAX_REMOTE_MEMBERS {
                return 3;
            }
        }
    }
    let addrs = addrs.to_str();
    let addr = (!addrs.is_empty())
        .then(|| known_addr(&ticket, addrs))
        .flatten();
    let session = match tokio_executor(session_handle()) {
        Ok(session) => session,
        Err(err) => {
            *LAST_ERROR.lock().expect("error poisoned") = Some(format!("{err:#}"));
            return 1;
        }
    };
    GROUP_PEERS
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .get_or_insert_with(HashSet::new)
        .insert(peer);
    tracing::info!(target: "idfon.media", peer = %peer.fmt_short(), "group member added");
    if SUB_STATE.load(Ordering::Relaxed) == SUB_IDLE {
        set_sub_state(SUB_CONNECTING);
    }
    start_listen_leg(session, ticket, addr)
}

/// Remove one remote member (its endpoint id, hex) from the call: stop its
/// leg and drop it from the mix. The others keep playing. Returns 0, or 1 when
/// no such member is in the call.
#[ffi_export]
pub fn media_live_remove_member(peer: char_p::Ref<'_>) -> u8 {
    let Ok(peer) = peer.to_str().parse::<EndpointId>() else {
        return 1;
    };
    let subscriber = SESSION
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_mut()
        .and_then(|session| session.subscribers.remove(&peer));
    let Some(subscriber) = subscriber else {
        return 1;
    };
    stop_subscriber(subscriber);
    if let Some(peers) = GROUP_PEERS
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_mut()
    {
        peers.remove(&peer);
    }
    tracing::info!(target: "idfon.media", peer = %peer.fmt_short(), "group member removed");
    release_mixer_if_idle();
    release_session_if_idle();
    0
}

/// How many remote members the call holds (listen legs).
#[ffi_export]
pub fn media_live_member_count() -> u32 {
    SESSION
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_ref()
        .map_or(0, |session| session.subscribers.len() as u32)
}
/// CoreVideo capture is BGRA; `Surface::rgba` reads red first. Feeding BGRA
/// straight in swaps red and blue, which encodes as blue skin on the wire.
fn bgra_to_rgba(pixels: &mut [u8]) {
    for px in pixels.chunks_exact_mut(4) {
        px.swap(0, 2);
    }
}

#[ffi_export]
pub fn media_video_push_frame(data: *const u8, len: usize, width: u32, height: u32, pts_ms: u64) {
    if data.is_null() || width == 0 || height == 0 || len < width as usize * height as usize * 4 {
        return;
    }
    // Camera PTS uses CoreMedia's clock; calibrate its origin to the call's
    // monotonic audio timeline, then preserve the camera's capture spacing.
    let now = std::time::Instant::now();
    let pts_us = live_media_epoch().map_or_else(
        || pts_ms.saturating_mul(1_000),
        |epoch| {
            VIDEO_PTS_CLOCK
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .map(pts_ms, epoch, now)
        },
    );
    let mut pixels =
        unsafe { std::slice::from_raw_parts(data, width as usize * height as usize * 4) }.to_vec();
    bgra_to_rgba(&mut pixels);
    let count = VIDEO_FRAMES_PUSHED.fetch_add(1, Ordering::Relaxed) + 1;
    if count <= 3 || count % 150 == 0 {
        eprintln!(
            "[media] video frame pushed #{} {}x{} enabled={}",
            count,
            width,
            height,
            VIDEO_ENABLED.load(Ordering::Relaxed)
        );
    }
    if let Ok(mut slot) = VIDEO_QUEUE.lock() {
        *slot = Some((width, height, pixels, pts_us));
    }
}

#[ffi_export]
pub fn media_audio_start() -> u8 {
    AUDIO_ENABLED.store(true, Ordering::Relaxed);
    0
}
#[ffi_export]
pub fn media_audio_stop() {
    AUDIO_ENABLED.store(false, Ordering::Relaxed);
}
#[ffi_export]
pub fn media_audio_active() -> u8 {
    u8::from(
        SESSION
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .is_some_and(|session| session.publication.is_some() || !session.subscribers.is_empty())
            || RECORDING.lock().unwrap().is_some()
            || PLAYBACK.lock().unwrap().is_some(),
    )
}
#[ffi_export]
pub fn media_audio_probe(_duration_ms: u64) -> usize {
    audio_queue()
        .lock()
        .map(|queue| queue.iter().filter(|sample| sample.abs() > 0.001).count())
        .unwrap_or(0)
}
#[ffi_export]
pub fn media_audio_input_count() -> usize {
    tokio_executor(async {
        moq_audio::capture::devices()
            .await
            .map(|devices| devices.len())
            .unwrap_or(0)
    })
}
#[ffi_export]
pub fn media_audio_output_count() -> usize {
    tokio_executor(async {
        moq_audio::playback::devices()
            .await
            .map(|devices| devices.len())
            .unwrap_or(0)
    })
}
#[ffi_export]
pub fn media_audio_set_volume(percent: u8) -> u8 {
    let value = percent.min(100);
    VOLUME.store(value, Ordering::Relaxed);
    if let Some(control) = PLAYBACK_CONTROL.lock().unwrap().as_ref() {
        control.set_volume(value as f32 / 100.0);
    }
    0
}
#[ffi_export]
pub fn media_audio_switch_input(device: char_p::Ref<'_>) -> u8 {
    let id = device.to_str().to_owned();
    let found = tokio_executor(async {
        moq_audio::capture::devices()
            .await
            .map(|devices| devices.into_iter().any(|item| item.id == id))
            .unwrap_or(false)
    });
    if !found {
        return 1;
    }
    *INPUT_DEVICE.lock().unwrap() = Some(id);
    let config = LIVE_CONFIG.lock().unwrap().clone();
    if let Some((audio, video, push, file, codec, sample_rate, channels)) = config {
        media_live_stop();
        if start_live(audio, video, push, file.clone(), codec, sample_rate, channels).is_ok() {
            *LIVE_CONFIG.lock().unwrap() =
                Some((audio, video, push, file, codec, sample_rate, channels));
        }
    }
    0
}
#[ffi_export]
pub fn media_audio_switch_output(device: char_p::Ref<'_>) -> u8 {
    let id = device.to_str().to_owned();
    let found = tokio_executor(async {
        moq_audio::playback::devices()
            .await
            .map(|devices| devices.into_iter().any(|item| item.id == id))
            .unwrap_or(false)
    });
    if !found {
        return 1;
    }
    *OUTPUT_DEVICE.lock().unwrap() = Some(id.clone());
    if let Some(engine) = PLAYBACK_ENGINE.lock().unwrap().as_ref() {
        let mut config = moq_audio::playback::Config::default();
        config.device = Some(id);
        if tokio_executor(engine.switch(config)).is_err() {
            return 1;
        }
    }
    0
}
#[ffi_export]
pub fn media_audio_set_bitrate(bitrate: u32) -> u8 {
    if !(8_000..=510_000).contains(&bitrate) {
        return 1;
    }
    BITRATE.store(bitrate as u64, Ordering::Relaxed);
    0
}

/// Flip in-band FEC on the live audio track (docs/media-adaptation.md §B2.4).
///
/// The degradation ladder's stereo step calls this: stereo on is quality
/// mode (FEC off, the LBRR copy would couple the channels), auto stereo-off
/// is resilience mode (FEC on, the measured recovery win is mono speech).
/// `fec` on selects the `inband` redundancy mode; off returns to the
/// automatic split ([`redundancy_for`]). Returns 0 on success, 1 without a
/// live audio track.
#[ffi_export]
pub fn media_audio_set_fec(fec: u8) -> u8 {
    REDUNDANCY.store(
        if fec != 0 {
            redundancy::INBAND
        } else {
            redundancy::AUTO
        },
        Ordering::Relaxed,
    );
    rebuild_audio_track()
}

/// Set the redundancy mode for the live audio track and every call after it
/// (docs/media-adaptation.md §D7.1). Modes: `0` auto — DRED for stereo and
/// for the audio of any video call (video is the shed rung, its audio must
/// survive it), LBRR for mono voice; `1` off (holes as sent); `2` inband
/// (Opus LBRR); `3` dred (Opus deep redundancy); `4` transport (formerly the
/// parity sidecar; now the same as dred). Returns 0 on
/// success, 1 without a live audio track (the mode still applies to the next
/// call), 2 for an unknown mode.
#[ffi_export]
pub fn media_audio_set_redundancy(mode: u8) -> u8 {
    if mode > redundancy::TRANSPORT {
        return 2;
    }
    REDUNDANCY.store(mode, Ordering::Relaxed);
    rebuild_audio_track()
}

/// Replace the live audio track with one built from the current
/// [`call_options`]. Replacing restarts the encoder, which the ladder's
/// channel change costs anyway. Runs inside the shared executor: the shells
/// call the FFI entries from bare threads (Swift `Task.detached`), where tokio
/// timers panic with "there is no reactor running" — `into_source` builds a
/// time-based stream. Returns 0 on success, 1 without a live audio track.
fn rebuild_audio_track() -> u8 {
    tokio_executor(async move {
        let setup = LIVE_CONFIG.lock().unwrap().clone();
        let Some((audio, video, push_audio, file_audio, codec, sample_rate, channels)) = setup else {
            return 1;
        };
        if !audio {
            return 1;
        }
        let Ok(renditions) = call_renditions(codec, sample_rate, channels, video) else {
            return 1;
        };
        // The shell queue is shared across replaces, so a push source keeps
        // whatever it holds; a device source re-opens.
        let capture: Box<dyn AudioCapture> = if push_audio {
            Box::new(ShellAudioCapture {
                sample_rate,
                channels,
            })
        } else if let Some(path) = file_audio {
            match FileAudioCapture::open(&path, false).await {
                Ok(capture) => Box::new(capture),
                Err(_) => return 1,
            }
        } else {
            Box::new(DeviceAudioCapture::new(
                INPUT_DEVICE.lock().unwrap().clone(),
            ))
        };
        let source = capture.into_source();
        let abr = opus_abr_target(&renditions);
        let session = SESSION.lock().unwrap_or_else(|poison| poison.into_inner());
        let Some(publication) = session.as_ref().and_then(|session| session.publication.as_ref())
        else {
            return 1;
        };
        if publication
            .broadcast
            .set_audio_renditions(source, audio_renditions_of(renditions))
            .is_err()
        {
            return 1;
        }
        drop(session);
        // The replaced encoders start at their configured rates: restart ABR from there.
        match abr {
            Some((rendition, total, channels)) => {
                crate::abr::start(rendition, total, channels);
            }
            None => crate::abr::stop(),
        }
        0
    })
}
#[ffi_export]
pub fn media_recording_start() -> u8 {
    media_recording_stop();
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    let handle = std::thread::spawn(move || {
        let result = tokio_executor(async move {
            // The microphone straight to PCM: no encode and no loopback publish
            // (the old path published Opus to itself and decoded it back).
            let mut config = moq_audio::capture::Config::default();
            config.source =
                moq_audio::capture::Source::Microphone(INPUT_DEVICE.lock().unwrap().clone());
            let producer = moq_audio::capture::Producer::open(config).await?;
            let mut reader = producer.consume();
            let input = reader.input().clone();
            let channels = input.layout.channels();
            let sample_rate = input.sample_rate;
            let mut capture = Box::pin(producer.run());
            let mut samples = Vec::new();
            let started = std::time::Instant::now();
            let mut poll = tokio::time::interval(std::time::Duration::from_millis(50));
            loop {
                tokio::select! {
                    result = &mut capture => {
                        if let Err(err) = result {
                            eprintln!("local recording capture ended: {err}");
                        }
                        break;
                    }
                    _ = poll.tick() => {
                        if thread_stop.load(Ordering::Relaxed) { break; }
                    }
                    buffer = reader.read() => {
                        let Ok(buffer) = buffer else { break };
                        if thread_stop.load(Ordering::Relaxed) { break; }
                        samples.extend_from_slice(&buffer.data);
                    }
                }
            }
            write_wav_at(&media_path("recording.wav"), &samples, channels, sample_rate)?;
            RECORDING_DURATION.store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
            anyhow::Ok(())
        });
        if let Err(err) = result {
            eprintln!("local recording failed: {err:#}");
        }
    });
    *RECORDING.lock().unwrap() = Some(Recording {
        stop,
        handle: Some(handle),
    });
    0
}
#[ffi_export]
pub fn media_recording_stop() -> u8 {
    if let Some(mut recording) = RECORDING.lock().unwrap().take() {
        recording.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = recording.handle.take() {
            let _ = handle.join();
        }
        0
    } else {
        1
    }
}
#[ffi_export]
pub fn media_recording_duration_ms() -> u64 {
    RECORDING_DURATION.load(Ordering::Relaxed)
}
#[ffi_export]
pub fn media_recording_persist(ticket: char_p::Ref<'_>) -> u8 {
    let Ok(ticket) = ticket.to_str().parse::<BlobTicket>() else {
        return 1;
    };
    let result = tokio_executor(async {
        let endpoint = idfon_core::relay::endpoint_builder().bind().await?;
        let root = media_path("persisted-blobs");
        let store = FsStore::load(&root).await?;
        store
            .downloader(&endpoint)
            .download(ticket.hash(), Some(ticket.addr().id))
            .await?;
        let output = media_path("fetched-blobs");
        std::fs::create_dir_all(&output)?;
        store
            .blobs()
            .export(ticket.hash(), output.join("received.wav"))
            .await?;
        endpoint.close().await;
        anyhow::Ok(())
    });
    u8::from(result.is_err())
}
#[ffi_export]
pub fn media_recording_play(ticket: char_p::Ref<'_>) -> u8 {
    media_recording_stop_playback();
    let path = if ticket.to_str().parse::<BlobTicket>().is_ok() {
        media_path("fetched-blobs").join("received.wav")
    } else {
        media_path("recording.wav")
    };
    if !path.exists() {
        return 1;
    }
    let Ok(data) = std::fs::read(&path) else {
        return 1;
    };
    let Some(format) = wav_format(&data) else {
        return 1;
    };
    if format.bits_per_sample != 16 {
        return 1;
    }
    let samples = data[format.data_offset..format.data_offset + format.data_len].to_vec();
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    let handle = std::thread::spawn(move || {
        let result = tokio_executor(async {
            let mut playback_config = moq_audio::playback::Config::default();
            playback_config.device = OUTPUT_DEVICE.lock().unwrap().clone();
            let engine = moq_audio::playback::Engine::open(playback_config).await?;
            let mut input = moq_audio::playback::Input::default();
            input.format = Format::S16;
            input.sample_rate = format.sample_rate;
            input.layout = moq_audio::Layout::from_channels(format.channels)
                .map_err(|err| anyhow::anyhow!("playback channels {}: {err}", format.channels))?;
            let mut sink = engine.sink(input)?;
            let control = sink.control();
            control.set_volume(VOLUME.load(Ordering::Relaxed) as f32 / 100.0);
            *PLAYBACK_CONTROL.lock().unwrap() = Some(control);
            // 20 ms of interleaved s16 at the declared rate and channel count.
            let chunk_bytes = (format.sample_rate / 50) as usize * format.channels as usize * 2;
            for chunk in samples.chunks(chunk_bytes) {
                if thread_stop.load(Ordering::Relaxed) {
                    break;
                }
                let _ = sink.write(chunk)?;
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            anyhow::Ok(())
        });
        if let Err(err) = result {
            eprintln!("playback failed: {err:#}");
        }
    });
    *PLAYBACK.lock().unwrap() = Some(Playback {
        stop,
        handle: Some(handle),
    });
    0
}
#[ffi_export]
pub fn media_recording_stop_playback() {
    PLAYBACK_CONTROL.lock().unwrap().take();
    PLAYBACK_ENGINE.lock().unwrap().take();
    if let Some(mut playback) = PLAYBACK.lock().unwrap().take() {
        playback.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = playback.handle.take() {
            let _ = handle.join();
        }
    }
}

/// In-app call tones (donpark/idfon#15, native shell): a small tone
/// generator writing synthesized PCM through the same device playback the
/// recordings use — no assets ship. `kind`: 0 stop, 1 ringback (loop),
/// 2 ringtone (loop), 3 answered (one-shot), 4 ended (one-shot). Every call
/// silences the previous tone first.
#[ffi_export]
pub fn media_call_tone(kind: u8) -> u8 {
    if let Some(mut tone) = TONE.lock().unwrap().take() {
        tone.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = tone.handle.take() {
            let _ = handle.join();
        }
    }
    if kind == 0 {
        return 0;
    }
    if kind > 4 {
        return 1;
    }
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    let handle = std::thread::spawn(move || {
        let result = tokio_executor(async {
            let mut playback_config = moq_audio::playback::Config::default();
            playback_config.device = OUTPUT_DEVICE.lock().unwrap().clone();
            let engine = moq_audio::playback::Engine::open(playback_config).await?;
            let mut input = moq_audio::playback::Input::default();
            input.format = Format::S16;
            input.sample_rate = 48_000;
            input.layout = moq_audio::Layout::Mono;
            let mut sink = engine.sink(input)?;
            let control = sink.control();
            control.set_volume(VOLUME.load(Ordering::Relaxed) as f32 / 100.0);
            let pattern = tone_pcm(kind);
            loop {
                for chunk in pattern.chunks(1_920) {
                    if thread_stop.load(Ordering::Relaxed) {
                        return anyhow::Ok(());
                    }
                    let _ = sink.write(chunk)?;
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                if kind >= 3 {
                    return anyhow::Ok(()); // one-shot
                }
            }
        });
        if let Err(err) = result {
            eprintln!("call tone failed: {err:#}");
        }
    });
    *TONE.lock().unwrap() = Some(Playback {
        stop,
        handle: Some(handle),
    });
    0
}

/// One cadence of a call tone as S16 mono PCM at 48 kHz. Loops repeat the
/// cadence; the short edge fade keeps the loop click-free.
fn tone_pcm(kind: u8) -> Vec<u8> {
    const SAMPLE_RATE: f64 = 48_000.0;
    // (frequency Hz, seconds); 0 Hz is silence.
    let segments: &[(f64, f64)] = match kind {
        1 => &[(440.0, 0.6), (0.0, 0.6)],                // ringback
        2 => &[(800.0, 0.35), (640.0, 0.35), (0.0, 1.1)], // ringtone
        3 => &[(660.0, 0.12)],                            // answered
        _ => &[(440.0, 0.12), (330.0, 0.12)],             // ended
    };
    let mut pcm = Vec::new();
    for &(freq, secs) in segments {
        let frames = (SAMPLE_RATE * secs) as usize;
        for frame in 0..frames {
            let t = frame as f64 / SAMPLE_RATE;
            let edge = (frame.min(frames - frame) as f64 / 1_200.0).min(1.0); // ~25 ms fade
            let sample = (2.0 * std::f64::consts::PI * freq * t).sin() * 0.25 * edge;
            pcm.extend_from_slice(&((sample * i16::MAX as f64) as i16).to_le_bytes());
        }
    }
    pcm
}
#[ffi_export]
pub fn media_live_recording_samples() -> usize {
    let path = {
        let received = media_path("received.wav");
        if received.exists() {
            received
        } else {
            media_path("recording.wav")
        }
    };
    std::fs::read(path)
        .ok()
        .and_then(|data| wav_format(&data))
        // frames, not interleaved values: the waveform is a time axis
        .map(|format| format.data_len / 2 / format.channels.max(1) as usize)
        .unwrap_or(0)
}

/// Channel count the local recording's WAV header declares (1 when there is
/// no readable recording) — the voice-message envelope's `channels=` line.
#[ffi_export]
pub fn media_recording_channels() -> u32 {
    std::fs::read(media_path("recording.wav"))
        .ok()
        .and_then(|data| wav_format(&data))
        .map(|format| format.channels)
        .unwrap_or(1)
}
#[ffi_export]
pub fn media_live_recording_store() -> char_p::Box {
    let result = tokio_executor(async {
        let mut guard = BLOB_PROVIDER
            .lock()
            .map_err(|_| anyhow::anyhow!("blob provider poisoned"))?;
        if guard.is_none() {
            let endpoint = idfon_core::relay::endpoint_builder().bind().await?;
            let store = FsStore::load(media_path("blobs")).await?;
            let router = Router::builder(endpoint.clone())
                .accept(BLOBS_ALPN, BlobsProtocol::new(store.as_ref(), None))
                .spawn();
            *guard = Some(BlobProvider {
                endpoint,
                store,
                _router: router,
            });
        }
        let provider = guard.as_ref().unwrap();
        let path = {
            let local = media_path("recording.wav");
            if local.exists() {
                local
            } else {
                media_path("received.wav")
            }
        };
        anyhow::ensure!(path.exists(), "no recording available");
        let content = provider.store.add_path(&path).await?;
        Ok::<String, anyhow::Error>(format!(
            "{}\n{}",
            RECORDING_DURATION.load(Ordering::Relaxed),
            BlobTicket::new(provider.endpoint.addr(), content.hash, content.format)
        ))
    });
    result.unwrap_or_default().try_into().unwrap()
}
#[ffi_export]
pub fn media_blob_fetch(ticket: char_p::Ref<'_>) -> u8 {
    let Ok(ticket) = ticket.to_str().parse::<BlobTicket>() else {
        return 1;
    };
    let result = tokio_executor(async {
        let endpoint = idfon_core::relay::endpoint_builder().bind().await?;
        let root = media_path("fetched-blobs");
        let store = FsStore::load(&root).await?;
        store
            .downloader(&endpoint)
            .download(ticket.hash(), Some(ticket.addr().id))
            .await?;
        store
            .blobs()
            .export(ticket.hash(), root.join("received.wav"))
            .await?;
        endpoint.close().await;
        anyhow::Ok(())
    });
    u8::from(result.is_err())
}
#[ffi_export]
pub fn media_set_scope(scope: char_p::Ref<'_>) -> u8 {
    let path = scoped_media_dir(Path::new(scope.to_str()));
    if std::fs::create_dir_all(&path).is_err() {
        return 1;
    }
    *MEDIA_SCOPE.lock().unwrap() = Some(path);
    0
}
#[ffi_export]
pub fn media_emergency_stop() {
    media_live_stop();
}
#[ffi_export]
pub fn media_shutdown() {
    media_live_unsubscribe();
    media_recording_stop_playback();
    media_recording_stop();
    media_live_stop();
    if let Some(provider) = BLOB_PROVIDER.lock().unwrap().take() {
        tokio_executor(async {
            provider.endpoint.close().await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::AudioCodec;

    /// A listener on its own endpoint and node, as the far phone would be. The
    /// call session is per process, so an in-process listener on the session's
    /// node would read the publication locally, not over the network.
    fn remote_listener() -> super::SessionHandle {
        super::tokio_executor(async {
            let endpoint = super::bind_call_endpoint().await.expect("listener endpoint");
            // The layout grant, as a real session: each peer may publish under its own id.
            let node = super::Node::new(endpoint.clone(), super::LiveLayout::default().config());
            super::SessionHandle { endpoint, node }
        })
    }

    /// The in-process publisher's dial address, as the invite's `addr=` carries it, plus
    /// loopback on the same ports. A freshly built test binary may lack macOS
    /// local-network permission, which blocks the LAN addresses and sends every call
    /// through a public relay (slow and rate-limited); loopback is exempt.
    fn publisher_addr() -> iroh::EndpointAddr {
        let json = super::media_live_endpoint_addr().to_str().to_string();
        let mut value: serde_json::Value = serde_json::from_str(&json).expect("publisher addr");
        if let Some(addrs) = value.get_mut("addrs").and_then(|addrs| addrs.as_array_mut()) {
            let loopback: Vec<serde_json::Value> = addrs
                .iter()
                .filter_map(|addr| addr.get("Ip")?.as_str()?.parse::<std::net::SocketAddr>().ok())
                .map(|socket| {
                    let ip: std::net::IpAddr = if socket.is_ipv4() {
                        std::net::Ipv4Addr::LOCALHOST.into()
                    } else {
                        std::net::Ipv6Addr::LOCALHOST.into()
                    };
                    serde_json::json!({ "Ip": std::net::SocketAddr::new(ip, socket.port()).to_string() })
                })
                .collect();
            addrs.extend(loopback);
        }
        serde_json::from_value(value).expect("publisher addr")
    }

    #[test]
    fn liquid_profile_is_refused_without_an_implementation() {
        let _guard = crate::util::MEDIA_TEST_GUARD
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        crate::liquid::media_liquid_unregister();
        let refused =
            super::media_live_start_with_profile(1, 0, c"push".into(), c"liquid".into(), 24_000, 1);
        assert!(refused.to_str().is_empty(), "liquid must be refused");
        assert_eq!(crate::liquid::media_liquid_available(), 0);
    }

    #[test]
    fn the_video_ladder_keeps_motion_at_every_rung() {
        let ladder = super::live_video_rungs();
        assert_eq!(ladder.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
                   ["low", "medium", "high"]);
        let bps = |i: usize| ladder[i].config.bitrate.map(|rate| rate.as_bps());
        assert_eq!(bps(0), Some(250_000));
        assert_eq!(bps(1), Some(800_000));
        assert_eq!(bps(2), Some(2_000_000));
    }

    #[test]
    fn video_pts_map_to_audio_epoch_and_keep_capture_spacing() {
        let epoch = std::time::Instant::now();
        let mut clock = super::VideoPtsClock::default();
        let first = clock.map(9_000_000, epoch, epoch + std::time::Duration::from_secs(2));
        let next = clock.map(9_000_033, epoch, epoch + std::time::Duration::from_secs(3));
        let restarted = clock.map(0, epoch, epoch + std::time::Duration::from_secs(4));
        assert_eq!(first, 2_000_000);
        assert_eq!(next, 2_033_000);
        assert_eq!(restarted, 4_000_000);
    }

    #[test]
    fn pushed_capture_bytes_are_reordered_bgra_to_rgba() {
        // CoreVideo hands the shells B,G,R,A; the encoder wants R,G,B,A.
        let mut pixels = [10u8, 20, 30, 255, 1, 2, 3, 4];
        super::bgra_to_rgba(&mut pixels);
        assert_eq!(pixels, [30, 20, 10, 255, 3, 2, 1, 4]);
    }

    #[test]
    fn voice_agent_profiles_are_supported_but_other_rates_are_not() {
        assert!(super::supported_audio_profile(AudioCodec::Opus, 48_000));
        assert!(super::supported_audio_profile(AudioCodec::Pcm, 24_000));
        // The agent plane runs at 24 kHz: Opus there is the per-contact
        // alternative to raw PCM (384 kbps -> ~24-32 kbps, plus FEC/DRED).
        assert!(super::supported_audio_profile(AudioCodec::Opus, 24_000));
        assert!(!super::supported_audio_profile(AudioCodec::Pcm, 48_000));
        assert!(!super::supported_audio_profile(AudioCodec::Opus, 16_000));
    }

    #[test]
    fn call_captures_keep_the_newest_calls() {
        let dir = std::env::temp_dir().join(format!("idfon-capture-prune-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for stamp in 1..=7u64 {
            for suffix in ["received.wav", "received-timing.csv"] {
                std::fs::write(dir.join(format!("{stamp:013}-{suffix}")), b"x").unwrap();
            }
        }
        std::fs::write(dir.join("notes.txt"), b"x").unwrap();
        super::prune_call_captures(&dir, 5);
        let mut left: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        assert_eq!(left.len(), 11);
        assert!(left[0].starts_with("0000000000003-"));
        assert!(left.contains(&"notes.txt".to_owned()));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn media_path_is_stable() {
        assert!(super::media_path("x").ends_with("x"));
    }

    /// The stall watchdog is 4 s direct / 2 s relay normally and 1.5 s after the shell asks for
    /// an immediate answer (foreground return) or the OS reports a path
    /// change. That shortening is what turns a 6 s transition hole into ~2 s.
    #[test]
    fn foreground_kick_and_path_change_shorten_the_stall_window() {
        let _guard = crate::util::MEDIA_TEST_GUARD
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        *super::PATH_CHANGED_AT.lock().unwrap() = None;
        super::SUB_KICK.store(false, std::sync::atomic::Ordering::Relaxed);
        // A direct path going quiet is usually a cell handover that resumes
        // on its own; a relay path has no such behavior.
        assert_eq!(super::stall_window(false), std::time::Duration::from_secs(4));
        assert_eq!(super::stall_window(true), std::time::Duration::from_secs(2));

        super::media_live_reconnect();
        assert_eq!(super::stall_window(false), std::time::Duration::from_millis(1_500));
        assert_eq!(super::stall_window(true), std::time::Duration::from_millis(1_500));
        super::SUB_KICK.store(false, std::sync::atomic::Ordering::Relaxed);

        super::media_live_path_changed();
        assert_eq!(super::stall_window(false), std::time::Duration::from_millis(1_500));
        assert_eq!(super::stall_window(true), std::time::Duration::from_millis(1_500));

        // The state the shell polls maps to the documented wire values.
        super::set_sub_state(super::SUB_RECONNECTING);
        assert_eq!(super::media_live_state(), 3);
        super::set_sub_state(super::SUB_IDLE);
        assert_eq!(super::media_live_state(), 0);
        *super::PATH_CHANGED_AT.lock().unwrap() = None;
    }

    #[test]
    fn known_addr_needs_a_matching_endpoint_id() {
        // C20: only the publisher's own live addresses may be dialed for a
        // ticket. The contact's stored (daemon-endpoint) addresses never match
        // a per-call live endpoint and must be ignored.
        let id = "0000000000000000000000000000000000000000000000000000000000000000";
        let peer: iroh::EndpointId = id.parse().expect("id");
        let ticket = super::BroadcastTicket::at(peer, moq_net::Path::new("live/x/t").expect("path").to_owned());
        let own = format!(r#"{{"id":"{id}","addrs":[{{"Ip":"127.0.0.1:1"}}]}}"#);
        let other = own.replace(id, "1111111111111111111111111111111111111111111111111111111111111111");
        let empty = format!(r#"{{"id":"{id}","addrs":[]}}"#);

        assert!(super::known_addr(&ticket, &own).is_some(), "own addresses are used");
        assert!(super::known_addr(&ticket, &other).is_none(), "foreign addresses are ignored");
        assert!(super::known_addr(&ticket, "not json").is_none(), "garbage is ignored");
        assert!(super::known_addr(&ticket, &empty).is_none(), "no addresses, nothing to dial");
    }

    #[test]
    fn frame_pts_gap_scales_with_channels() {
        // 20 ms @ 48 kHz: 960 mono samples, 1920 interleaved stereo samples.
        assert_eq!(super::frame_pts_gap_us(&vec![0u8; 960 * 4], 1), 20_000);
        assert_eq!(super::frame_pts_gap_us(&vec![0u8; 960 * 8], 2), 20_000);
    }

    /// `media_audio_set_fec` runs on bare threads (the shells call it from
    /// Swift `Task.detached`); its track rebuild must not need a caller tokio
    /// context — `into_source` builds a time-based stream and used to panic
    /// there ("there is no reactor running"), killing every call at publish.
    #[test]
    fn media_audio_set_fec_works_on_a_bare_thread() {
        let _guard = crate::util::MEDIA_TEST_GUARD
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let ticket =
            super::media_live_start_inner(true, false, true, None, AudioCodec::Opus, 48_000, 1);
        assert!(!ticket.to_str().is_empty(), "publish");
        // This test thread has no tokio runtime, exactly like the call site.
        assert_eq!(super::media_audio_set_fec(0), 0);
        assert_eq!(super::media_audio_set_fec(1), 0);
        assert_eq!(super::media_audio_set_redundancy(super::redundancy::DRED), 0);
        assert_eq!(super::media_audio_set_redundancy(super::redundancy::TRANSPORT), 0);
        assert_eq!(super::media_audio_set_redundancy(9), 2);
        // Leaves the mode on auto for the other tests.
        assert_eq!(super::media_audio_set_fec(0), 0);
        super::media_live_stop();
    }

    /// The §D7.1 automatic split: DRED for stereo and for the audio of any
    /// video call (video is the shed rung, its audio must survive it), LBRR
    /// for mono voice, nothing for non-Opus. Explicit modes override it.
    #[test]
    fn redundancy_derives_from_the_call_shape() {
        use super::redundancy;
        let derive = super::redundancy_for;
        assert_eq!(derive(redundancy::AUTO, AudioCodec::Opus, 1, false), redundancy::INBAND);
        assert_eq!(derive(redundancy::AUTO, AudioCodec::Opus, 2, false), redundancy::DRED);
        assert_eq!(derive(redundancy::AUTO, AudioCodec::Opus, 1, true), redundancy::DRED);
        assert_eq!(derive(redundancy::AUTO, AudioCodec::Pcm, 1, false), redundancy::OFF);
        assert_eq!(derive(redundancy::TRANSPORT, AudioCodec::Opus, 1, false), redundancy::TRANSPORT);
        assert_eq!(derive(redundancy::OFF, AudioCodec::Opus, 2, true), redundancy::OFF);
    }

    #[test]
    fn stereo_wav_header_declares_two_channels() {
        let path = std::env::temp_dir().join(format!("idfon-stereo-{}.wav", std::process::id()));
        super::write_wav(&path, &[0.0, 0.0, 0.5, -0.5], 2).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).ok();
        assert_eq!(u16::from_le_bytes([bytes[22], bytes[23]]), 2); // channels
        assert_eq!(u32::from_le_bytes(bytes[28..32].try_into().unwrap()), 192_000); // byte rate
        assert_eq!(u16::from_le_bytes([bytes[32], bytes[33]]), 4); // block align
    }

    #[test]
    fn wav_format_reads_the_declared_layout() {
        // write_wav's own stereo output.
        let path = std::env::temp_dir().join(format!("idfon-wavfmt-{}.wav", std::process::id()));
        super::write_wav(&path, &[0.0; 960 * 2], 2).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).ok();
        let format = super::wav_format(&bytes).unwrap();
        assert_eq!(format.channels, 2);
        assert_eq!(format.sample_rate, 48_000);
        assert_eq!(format.bits_per_sample, 16);
        assert_eq!(format.data_len, 960 * 2 * 2);
        // The shells' memos: canonical 16 kHz mono s16 (AVAudioFile output
        // shape) must parse the same way — playback honors it.
        let mut memo = Vec::new();
        memo.extend_from_slice(b"RIFF");
        memo.extend_from_slice(&36u32.to_le_bytes());
        memo.extend_from_slice(b"WAVEfmt ");
        memo.extend_from_slice(&16u32.to_le_bytes());
        memo.extend_from_slice(&1u16.to_le_bytes());
        memo.extend_from_slice(&1u16.to_le_bytes());
        memo.extend_from_slice(&16_000u32.to_le_bytes());
        memo.extend_from_slice(&32_000u32.to_le_bytes());
        memo.extend_from_slice(&2u16.to_le_bytes());
        memo.extend_from_slice(&16u16.to_le_bytes());
        memo.extend_from_slice(b"data");
        memo.extend_from_slice(&4u32.to_le_bytes());
        memo.extend_from_slice(&[0, 0, 0, 0]);
        let format = super::wav_format(&memo).unwrap();
        assert_eq!(format.channels, 1);
        assert_eq!(format.sample_rate, 16_000);
        assert_eq!(format.data_offset, 44);
        assert_eq!(format.data_len, 4);
        assert!(super::wav_format(&[0u8; 44]).is_none());
    }

    #[test]
    fn relative_scope_stays_out_of_cwd() {
        let dir = super::scoped_media_dir(std::path::Path::new("peer-abc"));
        assert!(dir.is_absolute(), "bare scope must not be cwd-relative: {dir:?}");
        assert_eq!(dir.file_name().unwrap(), "peer-abc");
        // An absolute scope is used verbatim (the mac app passes one).
        let abs = super::scoped_media_dir(std::path::Path::new("/var/tmp/x"));
        assert_eq!(abs, std::path::PathBuf::from("/var/tmp/x"));
    }

    /// The health card's source of truth (docs/media-adaptation.md §C1): two
    /// in-process endpoints — publish through `start_live`, subscribe through
    /// the entry the shells call — yield a readable record *mid-call*, and
    /// `media_live_stop` folds it into the report `/health-check` files under
    /// the peer. Also pins the read-and-clear hand-off: one record ships once.
    #[test]
    fn live_call_serves_a_health_record_midcall_and_at_hangup() {
        let _guard = crate::util::MEDIA_TEST_GUARD
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());

        unsafe extern "C" fn discard(
            _: *const std::ffi::c_void,
            _: *const u8,
            _: usize,
            _: u32,
            _: u32,
            _: u64,
        ) {
        }
        assert_eq!(
            super::media_audio_set_playback_cb(std::ptr::null(), discard),
            0
        );

        let ticket_text =
            super::start_live(true, false, false, None, AudioCodec::Opus, 48_000, 1)
                .expect("publish");
        // An id-only ticket resolves through discovery; talk to the publisher
        // directly, the way the invite's `addr=` does, from a separate node.
        let ticket: super::BroadcastTicket = ticket_text.parse().expect("ticket");
        assert_eq!(
            super::start_listen_leg(remote_listener(), ticket, Some(publisher_addr())),
            0,
            "subscribe"
        );

        // Push from the subscriber's first decoded frame on (the pump pads silence until
        // then): a subscribe through a relay can take seconds, and a push that started at
        // publish would be over before the record had anything to fold.
        let decoded_before = super::AUDIO_FRAMES_DECODED.load(std::sync::atomic::Ordering::Relaxed);
        std::thread::spawn(move || {
            let waited = std::time::Instant::now();
            while super::AUDIO_FRAMES_DECODED.load(std::sync::atomic::Ordering::Relaxed)
                == decoded_before
                && waited.elapsed() < std::time::Duration::from_secs(20)
            {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            for _ in 0..150u32 {
                let frame = vec![0.05f32; 960];
                super::media_audio_push_frames(frame.as_ptr(), 960, 1);
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        });

        // Playback starts once the decoder has frames; the record has something
        // to fold from that point (deadlock guard, not a latency budget).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(25);
        while super::media_live_state() != super::SUB_PLAYING
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert_eq!(
            super::media_live_state(),
            super::SUB_PLAYING,
            "subscriber never started playing"
        );
        // The sampler ticks twice a second: one tick with audio in it.
        std::thread::sleep(std::time::Duration::from_millis(1_500));

        let live_json = crate::health::media_health_live_report().to_str().to_string();
        let live: idfon_media::health::CallRecord =
            serde_json::from_str(&live_json).expect("mid-call record JSON");
        assert!(live.duration_ms > 0, "mid-call duration: {live_json}");
        assert!(!live.timeline.is_empty(), "mid-call timeline: {live_json}");
        assert!(live.audio.frames > 0, "mid-call audio stats: {live_json}");
        assert!(
            !live.path_switches.is_empty(),
            "the path the call started on: {live_json}"
        );

        super::media_live_stop();
        let ended_json = crate::health::media_health_last_report().to_str().to_string();
        let ended: idfon_media::health::CallRecord =
            serde_json::from_str(&ended_json).expect("hangup record JSON");
        assert!(ended.duration_ms >= live.duration_ms, "{ended_json}");
        assert!(
            !ended.timeline.is_empty() && ended.audio.frames > 0,
            "{ended_json}"
        );
        assert!(
            crate::health::media_health_live_report().to_str().is_empty(),
            "no sampling runs after the stop"
        );
        assert!(
            crate::health::media_health_last_report().to_str().is_empty(),
            "the record ships exactly once"
        );

        super::media_live_unsubscribe();
        super::media_audio_clear_playback_cb();
    }

    /// The stereo E2E without hardware (docs/stereo-calls.md): a shell-pushed
    /// L=440 / R=880 Hz tone through the real MoQ publish → decode path must
    /// arrive with channel separation intact. Direct addressing keeps it
    /// deterministic — a LiveTicket is id-only and resolves via pkarr/mDNS.
    #[test]
    fn stereo_tone_roundtrip_keeps_channel_separation() {
        use std::f32::consts::PI;
        use std::sync::atomic::Ordering;

        let _guard = crate::util::MEDIA_TEST_GUARD
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());

        // No speaker in CI: the shell render callback is a no-op sink.
        unsafe extern "C" fn discard(
            _: *const std::ffi::c_void,
            _: *const u8,
            _: usize,
            _: u32,
            _: u32,
            _: u64,
        ) {
        }
        assert_eq!(
            super::media_audio_set_playback_cb(std::ptr::null(), discard),
            0
        );
        let ticket_text =
            super::start_live(true, false, true, None, AudioCodec::Opus, 48_000, 2).expect("publish");
        let ticket: super::BroadcastTicket = ticket_text.parse().expect("ticket");
        // Talk to the in-process publisher's address directly, from a separate node.
        let addr = publisher_addr();
        let listener = remote_listener();

        let pusher = std::thread::spawn(move || {
            for frame_index in 0..120u32 {
                let mut frame = vec![0f32; 960 * 2];
                for (n, pair) in frame.chunks_exact_mut(2).enumerate() {
                    let t = (frame_index as usize * 960 + n) as f32 / 48_000.0;
                    pair[0] = (2.0 * PI * 440.0 * t).sin() * 0.5;
                    pair[1] = (2.0 * PI * 880.0 * t).sin() * 0.5;
                }
                super::media_audio_push_frames(frame.as_ptr(), 960, 2);
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        });

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let watchdog = {
            let stop = stop.clone();
            std::thread::spawn(move || {
                // Deadlock guard, not a latency budget: the round trip must
                // finish eventually, however loaded the machine is. What the
                // test asserts is the captured audio, not the wall clock.
                for _ in 0..150 {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                stop.store(true, Ordering::Relaxed);
            })
        };
        let mut samples = Vec::new();
        let mut timing = String::new();
        let mut decoded_total = 0;
        let mut channels = 0u32;
        let ended = super::tokio_executor(async {
            super::subscribe_decode_once(
                listener.clone(),
                ticket,
                Some(addr),
                &stop,
                &mut samples,
                &mut timing,
                &mut decoded_total,
                std::time::Instant::now(),
                1,
                &mut channels,
                &mut 0u32,
            )
            .await
        });
        pusher.join().unwrap();
        stop.store(true, Ordering::Relaxed);
        watchdog.join().unwrap();
        super::media_live_stop();
        super::media_audio_clear_playback_cb();
        super::tokio_executor(async move {
            listener.node.shutdown().await;
            listener.endpoint.close().await;
        });
        assert!(ended.expect("decode"), "watchdog should end the decode");
        assert_eq!(channels, 2, "stereo catalog must decode as stereo");
        assert!(samples.len() > 48_000, "capture too short: {}", samples.len());

        // Single-frequency correlation per channel, skipping the encoder's
        // first 0.2 s. 440 Hz must sit in ch0, 880 Hz in ch1 — and stay out
        // of the other one.
        let start = (48_000 / 5) as usize * 2;
        let magnitude = |channel: usize, freq: f32| -> f64 {
            let mut re = 0.0f64;
            let mut im = 0.0f64;
            let mut n = 0;
            while start + channel + 2 * n < samples.len() {
                let t = n as f64 / 48_000.0;
                let x = samples[start + channel + 2 * n] as f64;
                let phase = 2.0 * std::f64::consts::PI * freq as f64 * t;
                re += x * phase.cos();
                im += x * phase.sin();
                n += 1;
            }
            (re * re + im * im).sqrt()
        };
        let low = magnitude(0, 440.0);
        let high = magnitude(1, 880.0);
        eprintln!(
            "DIAG samples={} ch0: 440={low:.1} 880={:.1} | ch1: 440={:.1} 880={high:.1}",
            samples.len(),
            magnitude(0, 880.0),
            magnitude(1, 440.0),
        );
        assert!(low > 5.0 * magnitude(0, 880.0), "ch0 must carry 440 Hz");
        assert!(high > 5.0 * magnitude(1, 440.0), "ch1 must carry 880 Hz");
        assert!(low > 5.0 * magnitude(1, 440.0), "440 Hz must stay out of ch1");
        assert!(high > 5.0 * magnitude(0, 880.0), "880 Hz must stay out of ch0");
    }

    /// LiquidCodec through the real MoQ publish → decode path, with a stand-in codec registered through the same C
    /// callbacks the shell uses: a packet is the frame's mean, decoding yields a constant frame of it. Checks the
    /// 80 ms framing, the catalog, the decoder, and that the profile is refused before registration.
    #[test]
    fn liquid_profile_roundtrips_through_the_registered_codec() {
        use std::ffi::c_void;
        use std::sync::atomic::Ordering;

        let _guard = crate::util::MEDIA_TEST_GUARD
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());

        crate::liquid::media_liquid_unregister();
        let refused =
            super::media_live_start_with_profile(1, 0, c"push".into(), c"liquid".into(), 24_000, 1);
        assert!(
            refused.to_str().is_empty(),
            "liquid must be refused with no codec registered"
        );

        unsafe extern "C" fn new(_: *const c_void) -> *const c_void {
            Box::into_raw(Box::new(0f32)) as *const c_void
        }
        unsafe extern "C" fn free(h: *const c_void) {
            drop(unsafe { Box::from_raw(h as *mut f32) });
        }
        unsafe extern "C" fn encode(
            _: *const c_void,
            pcm: *const f32,
            n: usize,
            out: *mut u8,
            cap: usize,
        ) -> i32 {
            assert_eq!(n, 1920);
            assert!(cap >= 4);
            let pcm = unsafe { std::slice::from_raw_parts(pcm, n) };
            let mean = pcm.iter().sum::<f32>() / n as f32;
            unsafe { std::ptr::copy_nonoverlapping(mean.to_le_bytes().as_ptr(), out, 4) };
            4
        }
        unsafe extern "C" fn decode(
            h: *const c_void,
            p: *const u8,
            len: usize,
            out: *mut f32,
            cap: usize,
        ) -> i32 {
            assert_eq!(len, 4);
            let mut b = [0u8; 4];
            unsafe { std::ptr::copy_nonoverlapping(p, b.as_mut_ptr(), 4) };
            let v = f32::from_le_bytes(b);
            unsafe { *(h as *mut f32) = v };
            let out = unsafe { std::slice::from_raw_parts_mut(out, cap) };
            out[..1920].fill(v);
            1920
        }
        unsafe extern "C" fn recover(
            h: *const c_void,
            _: *const u8,
            _: usize,
            _: u32,
            out: *mut f32,
            cap: usize,
        ) -> i32 {
            let v = unsafe { *(h as *const f32) };
            let out = unsafe { std::slice::from_raw_parts_mut(out, cap) };
            out[..1920].fill(v);
            1920
        }
        unsafe extern "C" fn discard(
            _: *const c_void,
            _: *const u8,
            _: usize,
            _: u32,
            _: u32,
            _: u64,
        ) {
        }
        crate::liquid::media_liquid_register(
            std::ptr::null(),
            new,
            encode,
            free,
            new,
            decode,
            recover,
            free,
            400,
        );
        assert_eq!(crate::liquid::media_liquid_available(), 1);
        assert_eq!(
            super::media_audio_set_playback_cb(std::ptr::null(), discard),
            0
        );

        let ticket_text = super::start_live(true, false, true, None, AudioCodec::Liquid, 24_000, 1)
            .expect("publish liquid");
        let ticket: super::BroadcastTicket = ticket_text.parse().expect("ticket");
        let addr = publisher_addr();
        let listener = remote_listener();

        // 0.25 for the whole push, in 20 ms chunks as the shell does, a little faster than real time so the
        // 20 ms pump never pads a chunk with silence (the capture-side trim bounds the backlog).
        let decoded_before = super::AUDIO_FRAMES_DECODED.load(Ordering::Relaxed);
        let pusher = std::thread::spawn(move || {
            // Push only once the subscriber decodes (the pump's silence padding), so the whole
            // signal reaches it: a live track doesn't replay what was sent before the subscribe,
            // and a slow first subscribe (cold caches) otherwise misses most of a 2.8 s push.
            let waited = std::time::Instant::now();
            while super::AUDIO_FRAMES_DECODED.load(Ordering::Relaxed) == decoded_before
                && waited.elapsed() < std::time::Duration::from_secs(8)
            {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            // 80 ms bursts every 70 ms: the capture-side trim then keeps ~160 ms of slack for the pump.
            for _ in 0..40u32 {
                let frame = vec![0.25f32; 1920];
                super::media_audio_push_frames(frame.as_ptr(), 1920, 1);
                std::thread::sleep(std::time::Duration::from_millis(70));
            }
        });
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let watchdog = {
            let stop = stop.clone();
            std::thread::spawn(move || {
                for _ in 0..120 {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                stop.store(true, Ordering::Relaxed);
            })
        };
        let mut samples = Vec::new();
        let mut timing = String::new();
        let mut decoded_total = 0;
        let mut channels = 0u32;
        let ended = super::tokio_executor(async {
            super::subscribe_decode_once(
                listener.clone(),
                ticket,
                Some(addr),
                &stop,
                &mut samples,
                &mut timing,
                &mut decoded_total,
                std::time::Instant::now(),
                1,
                &mut channels,
                &mut 0u32,
            )
            .await
        });
        pusher.join().unwrap();
        stop.store(true, Ordering::Relaxed);
        watchdog.join().unwrap();
        super::media_live_stop();
        super::media_audio_clear_playback_cb();
        crate::liquid::media_liquid_unregister();
        super::tokio_executor(async move {
            listener.node.shutdown().await;
            listener.endpoint.close().await;
        });
        assert!(ended.expect("decode"), "watchdog should end the decode");
        assert_eq!(channels, 1, "liquid decodes mono");
        // Output is 48 kHz (resampled from the codec's 24 kHz). Skip the start (silence, then the resampler's ramp).
        assert!(
            samples.len() > 48_000,
            "capture too short: {}",
            samples.len()
        );
        // The push lasts 3 s of a ~12 s capture: measure 0.5-2.5 s after the signal starts.
        let first = samples
            .iter()
            .position(|s| s.abs() > 1e-3)
            .expect("decoded signal");
        let tail = &samples[first + 24_000..first + 120_000];
        let mean = tail.iter().sum::<f32>() / tail.len() as f32;
        let at_level =
            tail.iter().filter(|s| (**s - 0.25).abs() < 0.01).count() as f32 / tail.len() as f32;
        eprintln!(
            "DIAG at_level={at_level:.3} underrun_samples={} overflow_samples={}",
            super::AUDIO_QUEUE_UNDERRUN_SAMPLES.load(Ordering::Relaxed),
            super::AUDIO_QUEUE_OVERFLOW_SAMPLES.load(Ordering::Relaxed)
        );
        let nonzero = samples.iter().filter(|s| s.abs() > 1e-4).count();
        eprintln!(
            "DIAG liquid samples={} nonzero={nonzero} decoded_total={decoded_total} pushed={} encoded={} steady mean={mean:.4}\n{timing}",
            samples.len(),
            super::AUDIO_SAMPLES_PUSHED.load(Ordering::Relaxed),
            super::AUDIO_FRAMES_ENCODED.load(Ordering::Relaxed),
        );
        assert!(
            (mean - 0.25).abs() < 0.01,
            "steady level should be 0.25, got {mean}"
        );
        assert!(
            at_level > 0.95,
            "nearly every steady sample should decode at 0.25, got {at_level}"
        );
    }

    /// Two audio renditions through the real MoQ path: the publisher offers LiquidCodec (a stand-in that
    /// carries each frame's RMS) and Opus; the subscriber prefers LiquidCodec, then — when the codec goes
    /// away mid-call — switches to Opus on its own, without a gap the listener would hear.
    #[test]
    fn audio_renditions_switch_mid_call_without_a_gap() {
        use std::ffi::c_void;
        use std::sync::atomic::Ordering;

        let _guard = crate::util::MEDIA_TEST_GUARD
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());

        unsafe extern "C" fn new(_: *const c_void) -> *const c_void {
            Box::into_raw(Box::new(0f32)) as *const c_void
        }
        unsafe extern "C" fn free(h: *const c_void) {
            drop(unsafe { Box::from_raw(h as *mut f32) });
        }
        unsafe extern "C" fn encode(
            _: *const c_void,
            pcm: *const f32,
            n: usize,
            out: *mut u8,
            _: usize,
        ) -> i32 {
            let pcm = unsafe { std::slice::from_raw_parts(pcm, n) };
            let rms = (pcm.iter().map(|s| s * s).sum::<f32>() / n as f32).sqrt();
            unsafe { std::ptr::copy_nonoverlapping(rms.to_le_bytes().as_ptr(), out, 4) };
            4
        }
        unsafe extern "C" fn decode(
            h: *const c_void,
            p: *const u8,
            _: usize,
            out: *mut f32,
            cap: usize,
        ) -> i32 {
            let mut b = [0u8; 4];
            unsafe { std::ptr::copy_nonoverlapping(p, b.as_mut_ptr(), 4) };
            let v = f32::from_le_bytes(b);
            unsafe { *(h as *mut f32) = v };
            let out = unsafe { std::slice::from_raw_parts_mut(out, cap) };
            out[..1920].fill(v);
            1920
        }
        unsafe extern "C" fn recover(
            h: *const c_void,
            _: *const u8,
            _: usize,
            _: u32,
            out: *mut f32,
            cap: usize,
        ) -> i32 {
            let v = unsafe { *(h as *const f32) };
            let out = unsafe { std::slice::from_raw_parts_mut(out, cap) };
            out[..1920].fill(v);
            1920
        }
        unsafe extern "C" fn discard(
            _: *const c_void,
            _: *const u8,
            _: usize,
            _: u32,
            _: u32,
            _: u64,
        ) {
        }
        crate::liquid::media_liquid_register(
            std::ptr::null(),
            new,
            encode,
            free,
            new,
            decode,
            recover,
            free,
            400,
        );
        assert_eq!(
            super::media_audio_set_playback_cb(std::ptr::null(), discard),
            0
        );
        super::media_live_set_audio_preference(c"liquid".into());

        let ticket_text = super::start_live(true, false, true, None, AudioCodec::Liquid, 24_000, 1)
            .expect("publish");
        let ticket: super::BroadcastTicket = ticket_text.parse().expect("ticket");
        let addr = publisher_addr();
        let listener = remote_listener();
        let status = super::SESSION
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .publication
            .as_ref()
            .unwrap()
            .broadcast
            .status();
        let published: Vec<String> = status.borrow().renditions.keys().cloned().collect();
        assert_eq!(
            published,
            vec!["0.liquid".to_string(), "1.opus".to_string()],
            "both renditions published, named in publisher order"
        );

        // Every timing below runs from the subscriber's first decoded frame, not from the
        // publish: a subscribe through a relay takes seconds (a fresh test binary may lack
        // macOS local-network permission), and a fixed schedule would then unregister the
        // codec before the subscriber ever chose it.
        let decoded_before = super::AUDIO_FRAMES_DECODED.load(Ordering::Relaxed);
        let decoding = move || {
            let waited = std::time::Instant::now();
            while super::AUDIO_FRAMES_DECODED.load(Ordering::Relaxed) == decoded_before
                && waited.elapsed() < std::time::Duration::from_secs(10)
            {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        };
        // A 440 Hz tone at RMS ~0.177, in 80 ms bursts a little faster than real time.
        let pusher = std::thread::spawn(move || {
            decoding();
            for burst in 0..90u32 {
                let frame: Vec<f32> = (0..1920)
                    .map(|n| {
                        let t = (burst as usize * 1920 + n) as f32 / 24_000.0;
                        (2.0 * std::f32::consts::PI * 440.0 * t).sin() * 0.25
                    })
                    .collect();
                super::media_audio_push_frames(frame.as_ptr(), 1920, 1);
                std::thread::sleep(std::time::Duration::from_millis(70));
            }
        });
        // LiquidCodec goes away 3 s in: the subscriber must move to Opus by itself.
        let unregister = std::thread::spawn(move || {
            decoding();
            std::thread::sleep(std::time::Duration::from_secs(3));
            crate::liquid::media_liquid_unregister();
        });
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let watchdog = {
            let stop = stop.clone();
            std::thread::spawn(move || {
                // 7 s of decoding: past the switch at 3 s, inside the 6.3 s push plus slack,
                // before a silent (DTX) tail could read as a stall.
                decoding();
                for _ in 0..70 {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                stop.store(true, Ordering::Relaxed);
            })
        };
        let (mut samples, mut timing, mut decoded_total, mut channels) =
            (Vec::new(), String::new(), 0, 0u32);
        let mut codec_seen = Vec::new();
        let ended = super::tokio_executor(async {
            let mut stalls = 0u32;
            let decode = super::subscribe_decode_once(
                listener.clone(),
                ticket,
                Some(addr),
                &stop,
                &mut samples,
                &mut timing,
                &mut decoded_total,
                std::time::Instant::now(),
                1,
                &mut channels,
                &mut stalls,
            );
            tokio::pin!(decode);
            loop {
                tokio::select! {
                    ended = &mut decode => break ended,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {
                        let codec = super::INCOMING_AUDIO_CODEC.lock().unwrap().clone();
                        if codec_seen.last() != Some(&codec) { codec_seen.push(codec); }
                    }
                }
            }
        });
        pusher.join().unwrap();
        unregister.join().unwrap();
        stop.store(true, Ordering::Relaxed);
        watchdog.join().unwrap();
        super::media_live_stop();
        super::media_audio_clear_playback_cb();
        super::media_live_set_audio_preference(c"".into());
        super::tokio_executor(async move {
            listener.node.shutdown().await;
            listener.endpoint.close().await;
        });
        assert!(ended.expect("decode"), "watchdog should end the decode");
        let codecs: Vec<String> = codec_seen.into_iter().flatten().collect();
        eprintln!("DIAG codecs over time: {codecs:?}");
        assert_eq!(
            codecs.first().map(String::as_str),
            Some("liquid"),
            "starts on the preferred codec"
        );
        assert_eq!(
            codecs.last().map(String::as_str),
            Some("opus"),
            "switches when LiquidCodec goes away"
        );

        // Level per 100 ms window (48 kHz mono) across the tone: no audible hole at the switch.
        let first = samples.iter().position(|s| s.abs() > 1e-3).expect("signal");
        let last = samples
            .iter()
            .rposition(|s| s.abs() > 1e-3)
            .expect("signal");
        let windows: Vec<f32> = samples[first + 4_800..last.saturating_sub(4_800)]
            .chunks(4_800)
            .map(|w| (w.iter().map(|s| s * s).sum::<f32>() / w.len() as f32).sqrt())
            .collect();
        let quiet = windows.iter().filter(|rms| **rms < 0.05).count();
        eprintln!(
            "DIAG windows={} quiet={quiet} min_rms={:.3}",
            windows.len(),
            windows.iter().cloned().fold(f32::MAX, f32::min)
        );
        assert!(
            windows.len() > 30,
            "tone too short: {} windows",
            windows.len()
        );
        assert!(
            quiet <= 1,
            "{quiet} silent 100 ms windows around the switch"
        );
    }

    /// A tone pusher for the e2e tests: `bursts` 80 ms frames of 24 kHz mono at RMS ~0.177, a little
    /// faster than real time, starting once `start` returns.
    fn push_tone(bursts: u32, start: impl FnOnce() + Send + 'static) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            start();
            for burst in 0..bursts {
                let frame: Vec<f32> = (0..1920)
                    .map(|n| {
                        let t = (burst as usize * 1920 + n) as f32 / 24_000.0;
                        (2.0 * std::f32::consts::PI * 440.0 * t).sin() * 0.25
                    })
                    .collect();
                super::media_audio_push_frames(frame.as_ptr(), 1920, 1);
                std::thread::sleep(std::time::Duration::from_millis(70));
            }
        })
    }

    /// A LiquidCodec stand-in that encodes nothing and decodes a constant: enough to be offered.
    fn register_stand_in_liquid() {
        use std::ffi::c_void;
        unsafe extern "C" fn new(_: *const c_void) -> *const c_void {
            Box::into_raw(Box::new(0f32)) as *const c_void
        }
        unsafe extern "C" fn free(h: *const c_void) {
            drop(unsafe { Box::from_raw(h as *mut f32) });
        }
        unsafe extern "C" fn encode(_: *const c_void, _: *const f32, _: usize, out: *mut u8, _: usize) -> i32 {
            unsafe { *out = 1 };
            1
        }
        unsafe extern "C" fn decode(_: *const c_void, _: *const u8, _: usize, out: *mut f32, cap: usize) -> i32 {
            let out = unsafe { std::slice::from_raw_parts_mut(out, cap) };
            out[..1920].fill(0.1);
            1920
        }
        unsafe extern "C" fn recover(
            _: *const c_void,
            _: *const u8,
            _: usize,
            _: u32,
            out: *mut f32,
            cap: usize,
        ) -> i32 {
            let out = unsafe { std::slice::from_raw_parts_mut(out, cap) };
            out[..1920].fill(0.1);
            1920
        }
        crate::liquid::media_liquid_register(std::ptr::null(), new, encode, free, new, decode, recover, free, 400);
    }

    /// The two-iPhone test of 2026-11-18 played LiquidCodec on a call whose publisher offered
    /// `[opus, liquid]`: the catalog map lists "liquid" first. With no preference the subscriber
    /// must play the publisher's first rendition.
    #[test]
    fn subscriber_plays_the_publishers_first_rendition_without_a_preference() {
        use std::sync::atomic::Ordering;
        let _guard = crate::util::MEDIA_TEST_GUARD
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        unsafe extern "C" fn discard(
            _: *const std::ffi::c_void,
            _: *const u8,
            _: usize,
            _: u32,
            _: u32,
            _: u64,
        ) {
        }
        register_stand_in_liquid();
        assert_eq!(super::media_audio_set_playback_cb(std::ptr::null(), discard), 0);
        super::media_live_set_audio_preference(c"".into());

        let ticket_text = super::start_live(true, false, true, None, AudioCodec::Opus, 24_000, 1)
            .expect("publish");
        let ticket: super::BroadcastTicket = ticket_text.parse().expect("ticket");
        let addr = publisher_addr();
        let listener = remote_listener();
        let published: Vec<String> = super::SESSION
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .publication
            .as_ref()
            .unwrap()
            .broadcast
            .status()
            .borrow()
            .renditions
            .keys()
            .cloned()
            .collect();
        assert_eq!(published, vec!["0.opus".to_string(), "1.liquid".to_string()]);

        let decoded_before = super::AUDIO_FRAMES_DECODED.load(Ordering::Relaxed);
        let decoding = move || {
            let waited = std::time::Instant::now();
            while super::AUDIO_FRAMES_DECODED.load(Ordering::Relaxed) == decoded_before
                && waited.elapsed() < std::time::Duration::from_secs(10)
            {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        };
        let pusher = push_tone(60, || {});
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let watchdog = {
            let stop = stop.clone();
            std::thread::spawn(move || {
                decoding();
                std::thread::sleep(std::time::Duration::from_secs(2));
                stop.store(true, Ordering::Relaxed);
            })
        };
        let (mut samples, mut timing, mut decoded_total, mut channels) =
            (Vec::new(), String::new(), 0, 0u32);
        let ended = super::tokio_executor(super::subscribe_decode_once(
            listener.clone(),
            ticket,
            Some(addr),
            &stop,
            &mut samples,
            &mut timing,
            &mut decoded_total,
            std::time::Instant::now(),
            1,
            &mut channels,
            &mut 0u32,
        ));
        let codec = super::INCOMING_AUDIO_CODEC.lock().unwrap().clone();
        pusher.join().unwrap();
        watchdog.join().unwrap();
        super::media_live_stop();
        super::media_audio_clear_playback_cb();
        crate::liquid::media_liquid_unregister();
        super::tokio_executor(async move {
            listener.node.shutdown().await;
            listener.endpoint.close().await;
        });
        assert!(ended.expect("decode"));
        assert_eq!(codec.as_deref(), Some("opus"), "plays the publisher's first rendition");
        // Opus decodes in 20 ms frames (960 samples at 48 kHz); LiquidCodec in 80 ms ones.
        let sizes: std::collections::BTreeSet<String> =
            timing.lines().skip(1).filter_map(|l| l.split(',').nth(5)).map(str::to_string).collect();
        eprintln!("DIAG decoded frame sizes: {sizes:?}");
        assert!(sizes.contains("960"), "20 ms Opus frames, got {sizes:?}");
    }

    /// The handoff bug of 2026-11-18: after the phone's path died, every retry reused the dead
    /// session and the call never came back. Closing the session from the publisher's side must end
    /// the attempt, and the retry must redial and play again.
    #[test]
    fn subscriber_recovers_after_its_session_is_closed() {
        use std::sync::atomic::Ordering;
        let _guard = crate::util::MEDIA_TEST_GUARD
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        unsafe extern "C" fn discard(
            _: *const std::ffi::c_void,
            _: *const u8,
            _: usize,
            _: u32,
            _: u32,
            _: u64,
        ) {
        }
        assert_eq!(super::media_audio_set_playback_cb(std::ptr::null(), discard), 0);
        super::media_live_set_audio_preference(c"".into());
        let ticket_text = super::start_live(true, false, true, None, AudioCodec::Opus, 24_000, 1)
            .expect("publish");
        let ticket: super::BroadcastTicket = ticket_text.parse().expect("ticket");
        let addr = publisher_addr();
        let listener = remote_listener();
        let listener_id = listener.endpoint.id();
        let publisher_node = super::SESSION.lock().unwrap().as_ref().unwrap().node.clone();

        let decoded = || super::AUDIO_FRAMES_DECODED.load(Ordering::Relaxed);
        let start = decoded();
        let pusher = push_tone(130, move || {
            let waited = std::time::Instant::now();
            while decoded() == start && waited.elapsed() < std::time::Duration::from_secs(10) {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        });
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let recovered_ms = std::sync::Arc::new(std::sync::Mutex::new(None::<u128>));
        let killer = {
            let stop = stop.clone();
            let recovered_ms = recovered_ms.clone();
            std::thread::spawn(move || {
                let waited = std::time::Instant::now();
                while decoded() < start + 25 && waited.elapsed() < std::time::Duration::from_secs(10) {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                // Close the listener's session from the publisher's side, as a dead path would.
                let closed_at = super::tokio_executor(async move {
                    let sessions = { use iroh::Watcher as _; publisher_node.sessions().get() };
                    for session in sessions.iter().filter(|s| s.peer() == listener_id) {
                        session.connection().close(0u32.into(), b"test: path died");
                    }
                    std::time::Instant::now()
                });
                std::thread::sleep(std::time::Duration::from_millis(300));
                let before = decoded();
                while decoded() < before + 5 && closed_at.elapsed() < std::time::Duration::from_secs(8) {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                if decoded() >= before + 5 {
                    *recovered_ms.lock().unwrap() = Some(closed_at.elapsed().as_millis());
                }
                std::thread::sleep(std::time::Duration::from_millis(500));
                stop.store(true, Ordering::Relaxed);
            })
        };
        let attempts = super::tokio_executor({
            let stop = stop.clone();
            let listener = listener.clone();
            async move {
                let (mut samples, mut timing, mut decoded_total, mut channels) =
                    (Vec::new(), String::new(), 0, 0u32);
                let mut attempt = 0u32;
                loop {
                    attempt += 1;
                    match super::subscribe_decode_once(
                        listener.clone(),
                        ticket.clone(),
                        Some(addr.clone()),
                        &stop,
                        &mut samples,
                        &mut timing,
                        &mut decoded_total,
                        std::time::Instant::now(),
                        attempt,
                        &mut channels,
                        &mut 0u32,
                    )
                    .await
                    {
                        Ok(_) => break attempt,
                        Err(_) if attempt < 10 => {
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await
                        }
                        Err(err) => panic!("no recovery after {attempt} attempts: {err:#}"),
                    }
                }
            }
        });
        pusher.join().unwrap();
        killer.join().unwrap();
        super::media_live_stop();
        super::media_audio_clear_playback_cb();
        super::tokio_executor(async move {
            listener.node.shutdown().await;
            listener.endpoint.close().await;
        });
        let recovered = *recovered_ms.lock().unwrap();
        eprintln!("DIAG attempts={attempts} recovered_after_ms={recovered:?}");
        assert!(attempts >= 2, "the close should end the first attempt");
        let recovered = recovered.expect("audio came back after the session closed");
        assert!(recovered < 6_000, "recovery took {recovered} ms");
    }


    /// Mixed shell output for the group test.
    static GROUP_MIX: std::sync::Mutex<Vec<f32>> = std::sync::Mutex::new(Vec::new());

    /// Power of `hz` in mono 48 kHz `samples` (Goertzel), normalized by length.
    fn tone_power(samples: &[f32], hz: f32) -> f32 {
        let coeff = 2.0 * (2.0 * std::f32::consts::PI * hz / 48_000.0).cos();
        let (mut s1, mut s2) = (0.0f32, 0.0f32);
        for &x in samples {
            let s0 = x + coeff * s1 - s2;
            s2 = s1;
            s1 = s0;
        }
        (s1 * s1 + s2 * s2 - coeff * s1 * s2) / (samples.len() as f32).powi(2)
    }

    /// A remote member publishing a continuous tone, as a far phone would.
    struct TonePublisher {
        endpoint: iroh::Endpoint,
        node: super::Node,
        router: iroh::protocol::Router,
        _broadcast: super::LocalBroadcast,
        ticket: String,
        addr: String,
    }

    fn tone_publisher(hz: f32) -> TonePublisher {
        super::tokio_executor(async move {
            let endpoint = super::bind_call_endpoint().await.expect("publisher endpoint");
            let layout = super::LiveLayout::default();
            let node = super::Node::new(endpoint.clone(), layout.config());
            let router = node.mount(iroh::protocol::Router::builder(endpoint.clone())).spawn();
            let broadcast: super::LocalBroadcast = layout.publish(&node, "tone").expect("publish");
            let renditions = super::call_renditions(AudioCodec::Opus, 48_000, 1, false).expect("renditions");
            broadcast
                .set_audio_renditions(
                    super::AudioSource::tone(hz, moq_audio::Layout::from_channels(1).unwrap()).expect("tone"),
                    super::audio_renditions_of(renditions),
                )
                .expect("audio");
            let ticket = layout.ticket(node.id(), "tone").expect("ticket").to_string();
            // Loopback on the bound ports: a fresh test binary may lack local-network permission.
            let mut addr = endpoint.addr();
            for socket in endpoint.bound_sockets() {
                let ip: std::net::IpAddr = if socket.is_ipv4() {
                    std::net::Ipv4Addr::LOCALHOST.into()
                } else {
                    std::net::Ipv6Addr::LOCALHOST.into()
                };
                addr.addrs.insert(iroh::TransportAddr::Ip(std::net::SocketAddr::new(ip, socket.port())));
            }
            let addr = serde_json::to_string(&addr).expect("addr json");
            TonePublisher { endpoint, node, router, _broadcast: broadcast, ticket, addr }
        })
    }

    /// G0: three remote members in one call. Every member is heard in the one mixed
    /// stream the shell gets, and a member removed mid-call leaves the mix while the
    /// others keep playing.
    #[test]
    fn group_call_mixes_every_member_and_drops_a_removed_one() {
        let _guard = crate::util::MEDIA_TEST_GUARD
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        unsafe extern "C" fn collect(
            _: *const std::ffi::c_void,
            pcm: *const u8,
            len: usize,
            rate: u32,
            channels: u32,
            _: u64,
        ) {
            assert_eq!((rate, channels), (48_000, 1), "one mono 48 kHz stream");
            let bytes = unsafe { std::slice::from_raw_parts(pcm, len) };
            GROUP_MIX.lock().unwrap().extend(
                bytes.chunks_exact(4).map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]])),
            );
        }
        assert_eq!(super::media_audio_set_playback_cb(std::ptr::null(), collect), 0);
        super::media_live_set_audio_preference(c"".into());
        GROUP_MIX.lock().unwrap().clear();

        let tones = [440.0f32, 660.0, 990.0];
        let members: Vec<TonePublisher> = tones.iter().map(|&hz| tone_publisher(hz)).collect();
        for member in &members {
            let ticket = std::ffi::CString::new(member.ticket.clone()).unwrap();
            let addr = std::ffi::CString::new(member.addr.clone()).unwrap();
            assert_eq!(super::media_live_add_member(ticket.as_c_str().into(), addr.as_c_str().into()), 0);
        }
        // Adding a member twice is refused.
        let again = std::ffi::CString::new(members[0].ticket.clone()).unwrap();
        assert_eq!(super::media_live_add_member(again.as_c_str().into(), c"".into()), 2);
        assert_eq!(super::media_live_member_count(), 3);

        // Wait for every member's tone, then judge a fresh 1.5 s window.
        let heard = |window: &[f32], hz: f32| tone_power(window, hz) > 1e-4;
        let waited = std::time::Instant::now();
        loop {
            std::thread::sleep(std::time::Duration::from_millis(250));
            let mix = GROUP_MIX.lock().unwrap();
            let tail = &mix[mix.len().saturating_sub(24_000)..];
            if tail.len() == 24_000 && tones.iter().all(|&hz| heard(tail, hz)) {
                break;
            }
            assert!(waited.elapsed() < std::time::Duration::from_secs(15), "not every member was heard");
        }
        GROUP_MIX.lock().unwrap().clear();
        std::thread::sleep(std::time::Duration::from_millis(1_500));
        let all = GROUP_MIX.lock().unwrap().clone();
        let powers: Vec<f32> = tones.iter().map(|&hz| tone_power(&all, hz)).collect();
        eprintln!("DIAG all members: samples={} powers={powers:?}", all.len());
        assert!(all.len() >= 48_000, "the mix kept flowing: {} samples", all.len());
        assert!(tones.iter().all(|&hz| heard(&all, hz)), "every member in the mix: {powers:?}");

        // The 660 Hz member leaves.
        let leaving = members[1].node.id().to_string();
        let leaving = std::ffi::CString::new(leaving).unwrap();
        assert_eq!(super::media_live_remove_member(leaving.as_c_str().into()), 0);
        assert_eq!(super::media_live_member_count(), 2);
        std::thread::sleep(std::time::Duration::from_millis(300));
        GROUP_MIX.lock().unwrap().clear();
        std::thread::sleep(std::time::Duration::from_millis(1_500));
        let rest = GROUP_MIX.lock().unwrap().clone();
        let powers: Vec<f32> = tones.iter().map(|&hz| tone_power(&rest, hz)).collect();
        eprintln!("DIAG after one left: samples={} powers={powers:?}", rest.len());
        assert!(heard(&rest, 440.0) && heard(&rest, 990.0), "the others keep playing: {powers:?}");
        assert!(!heard(&rest, 660.0), "the removed member left the mix: {powers:?}");

        super::media_live_unsubscribe();
        assert_eq!(super::media_live_member_count(), 0);
        assert!(super::MIXER.lock().unwrap().is_none(), "the mixer stops with the last leg");
        super::media_audio_clear_playback_cb();
        for member in members {
            super::tokio_executor(async move {
                let _ = member.router.shutdown().await;
                member.node.shutdown().await;
                member.endpoint.close().await;
            });
        }
    }

}
