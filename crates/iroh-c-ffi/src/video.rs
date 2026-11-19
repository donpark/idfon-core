//! Current decoded-video FFI bridge.

use iroh_live::media::{PlayerConfig, SubscriptionExt as _, IMMEDIATE_LATENCY};
use iroh_live::{BroadcastTicket, Reach};
use safer_ffi::prelude::*;
use std::{
    ffi::c_void,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant},
};

use moq_video::Frame as VideoFrame;

use crate::media::media_path;

static VIDEO: Mutex<Option<VideoSession>> = Mutex::new(None);
static VIDEO_ERROR: OnceLock<Mutex<String>> = OnceLock::new();
static VIDEO_FRAMES_PRESENTED: AtomicU64 = AtomicU64::new(0);
static VIDEO_LAST_FRAME_AT: Mutex<Option<Instant>> = Mutex::new(None);
static VIDEO_RENDITION: Mutex<Option<String>> = Mutex::new(None);
/// What the call's status line shows about incoming video: the decoder in use, the last frame's size, and the
/// presented frame rate over the last second.
static VIDEO_INFO: Mutex<VideoInfo> = Mutex::new(VideoInfo::new());

struct VideoInfo {
    rendition: Option<String>,
    bitrate: Option<u64>,
    decoder: Option<String>,
    size: Option<(u32, u32)>,
    window_start: Option<Instant>,
    window_frames: u32,
    fps: Option<f32>,
}

impl VideoInfo {
    const fn new() -> Self {
        Self {
            rendition: None,
            bitrate: None,
            decoder: None,
            size: None,
            window_start: None,
            window_frames: 0,
            fps: None,
        }
    }

    fn frame(&mut self, size: (u32, u32)) {
        self.size = Some(size);
        let now = Instant::now();
        let start = *self.window_start.get_or_insert(now);
        self.window_frames += 1;
        let elapsed = now.duration_since(start).as_secs_f32();
        if elapsed >= 1.0 {
            self.fps = Some(self.window_frames as f32 / elapsed);
            self.window_start = Some(now);
            self.window_frames = 0;
        }
    }
}

/// Incoming video for the status line, while frames are arriving.
pub(crate) struct IncomingVideo {
    pub rendition: Option<String>,
    pub width: u32,
    pub height: u32,
    pub fps: Option<f32>,
    pub hardware: Option<bool>,
    pub bitrate: Option<u64>,
}

pub(crate) fn incoming_video() -> Option<IncomingVideo> {
    if last_frame_age_ms().is_none_or(|age| age > 2_000) {
        return None;
    }
    let info = VIDEO_INFO
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let (width, height) = info.size?;
    Some(IncomingVideo {
        rendition: info.rendition.clone(),
        width,
        height,
        fps: info.fps,
        hardware: info.decoder.as_deref().map(|name| name != "openh264"),
        bitrate: info.bitrate,
    })
}

pub(crate) fn frames_presented() -> u64 {
    VIDEO_FRAMES_PRESENTED.load(Ordering::Relaxed)
}

pub(crate) fn current_rendition() -> Option<String> {
    VIDEO_RENDITION
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
}

pub(crate) fn last_frame_age_ms() -> Option<u64> {
    VIDEO_LAST_FRAME_AT
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_ref()
        .map(|last| last.elapsed().as_millis() as u64)
}

fn record_presented_frame(size: (u32, u32)) {
    VIDEO_INFO
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .frame(size);
    VIDEO_FRAMES_PRESENTED.fetch_add(1, Ordering::Relaxed);
    *VIDEO_LAST_FRAME_AT.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(Instant::now());
}

/// Stored as `usize` so the function pointer survives a static `Mutex`.
type RenderFn = unsafe extern "C" fn(*const c_void, *const u8, usize, u32, u32, u64);

/// Optional shell-owned in-memory renderer. When absent, decoded frames fall
/// back to the on-disk `video-frame.jpg` artifact.
static VIDEO_RENDER_CB: Mutex<Option<(usize, RenderFn)>> = Mutex::new(None);

/// Register an in-memory video renderer before `media_video_start`.
///
/// The callback runs on a Rust media thread and is handed a borrowed RGBA
/// buffer valid only for the duration of the call; copy before returning.
/// `len == 0` signals "no live frame" (peer camera off / track ended).
#[ffi_export]
pub fn media_video_set_render_cb(
    ctx: *const c_void,
    cb: unsafe extern "C" fn(*const c_void, *const u8, usize, u32, u32, u64),
) -> u8 {
    *VIDEO_RENDER_CB.lock().unwrap() = Some((ctx as usize, cb));
    0
}

#[ffi_export]
pub fn media_video_clear_render_cb() {
    *VIDEO_RENDER_CB.lock().unwrap() = None;
}

/// How long the frame artifact survives without a new decoded frame. The
/// publisher keeps its video track open while the camera is off (the encoder
/// idles), so silence is "peer camera off": remove the artifact so the shell
/// clears the stale picture instead of presenting it as live.
const FRAME_IDLE: Duration = Duration::from_secs(2);
/// How often the loop re-checks the stop flag while waiting on the peer.
const STOP_POLL: Duration = Duration::from_millis(100);

fn video_error(message: impl Into<String>) {
    if let Ok(mut error) = VIDEO_ERROR.get_or_init(|| Mutex::new(String::new())).lock() {
        *error = message.into();
    }
}

struct VideoSession {
    stop: Arc<AtomicBool>,
}

#[ffi_export]
pub fn media_video_start(ticket: char_p::Ref<'_>) -> char_p::Box {
    media_video_stop();
    video_error(""); // a stale error from the previous session must not fail this one
    let ticket = ticket.to_str().to_owned();
    let frame_path = media_path("video-frame.jpg");
    let temp_path = media_path("video-frame.jpg.tmp");
    let _ = std::fs::remove_file(&frame_path);
    let _ = std::fs::remove_file(&temp_path);
    let render: Box<dyn VideoRender> = match *VIDEO_RENDER_CB.lock().unwrap() {
        Some((ctx, cb)) => Box::new(CallbackRender { ctx, cb }),
        None => Box::new(DiskRender::new(&frame_path, &temp_path)),
    };
    *VIDEO_LAST_FRAME_AT.lock().unwrap_or_else(|poison| poison.into_inner()) = None;
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    let return_path = frame_path.to_string_lossy().into_owned();
    // A dedicated thread drives the loop on the shared executor's runtime, so
    // the call session's node and this subscription share one reactor.
    let runtime = crate::util::TOKIO_EXECUTOR.handle().clone();
    std::thread::spawn(move || {
        runtime.block_on(video_loop(&ticket, render, thread_stop));
    });
    *VIDEO.lock().unwrap() = Some(VideoSession { stop });
    return_path.try_into().unwrap()
}

#[ffi_export]
pub fn media_video_last_error() -> char_p::Box {
    VIDEO_ERROR
        .get_or_init(|| Mutex::new(String::new()))
        .lock()
        .map(|error| error.clone())
        .unwrap_or_default()
        .try_into()
        .unwrap()
}

#[ffi_export]
pub fn media_video_stop() {
    *VIDEO_RENDITION.lock().unwrap_or_else(|poison| poison.into_inner()) = None;
    *VIDEO_LAST_FRAME_AT.lock().unwrap_or_else(|poison| poison.into_inner()) = None;
    if let Some(session) = VIDEO.lock().unwrap().take() {
        session.stop.store(true, Ordering::Relaxed);
    }
}

/// Where decoded frames go. Frames arrive shared (`Arc`) from the player, so a
/// renderer borrows them; `idfon_media::seam::VideoRender` takes them by value.
trait VideoRender: Send + Sync {
    fn present(&self, frame: &VideoFrame);
    /// No live frames are available any more (peer camera off / track ended).
    fn clear(&self) {}
}

/// Disk artifact renderer: the fallback when no shell callback is registered.
struct DiskRender {
    frame_path: PathBuf,
    temp_path: PathBuf,
}

impl DiskRender {
    fn new(frame_path: &Path, temp_path: &Path) -> Self {
        Self {
            frame_path: frame_path.to_path_buf(),
            temp_path: temp_path.to_path_buf(),
        }
    }
}

impl VideoRender for DiskRender {
    fn present(&self, frame: &VideoFrame) {
        let Ok(rgba) = frame.surface.to_rgba(&moq_video::convert::Config::default()) else {
            return;
        };
        let mut jpeg = Vec::new();
        let encoder = jpeg_encoder::Encoder::new(&mut jpeg, 80);
        if encoder
            .encode(
                rgba.data(),
                rgba.width() as u16,
                rgba.height() as u16,
                jpeg_encoder::ColorType::Rgba,
            )
            .is_ok()
        {
            if std::fs::write(&self.temp_path, &jpeg).is_ok() {
                let _ = std::fs::rename(&self.temp_path, &self.frame_path);
            }
        } else {
            video_error("video JPEG encode failed");
        }
    }

    fn clear(&self) {
        let _ = std::fs::remove_file(&self.frame_path);
    }
}

/// In-memory renderer: hands decoded RGBA frames straight to the shell.
struct CallbackRender {
    ctx: usize,
    cb: RenderFn,
}

impl VideoRender for CallbackRender {
    fn present(&self, frame: &VideoFrame) {
        let Ok(rgba) = frame.surface.to_rgba(&moq_video::convert::Config::default()) else {
            return;
        };
        let data = rgba.data();
        let pts_ms = frame.timestamp.as_millis().unwrap_or(0) as u64;
        unsafe {
            (self.cb)(
                self.ctx as *const c_void,
                data.as_ptr(),
                data.len(),
                rgba.width(),
                rgba.height(),
                pts_ms,
            );
        }
    }

    fn clear(&self) {
        unsafe { (self.cb)(self.ctx as *const c_void, std::ptr::null(), 0, 0, 0, 0) };
    }
}

async fn video_loop(ticket: &str, render: Box<dyn VideoRender>, stop: Arc<AtomicBool>) {
    let ticket = match ticket.parse::<BroadcastTicket>() {
        Ok(ticket) => ticket,
        Err(error) => {
            video_error(format!("ticket parse failed: {error}"));
            return;
        }
    };
    // The call session's node: the audio leg's subscription already holds a
    // session to this peer, so the video subscription rides it.
    let session = match crate::media::session_handle().await {
        Ok(session) => session,
        Err(error) => {
            video_error(format!("live init failed: {error:#}"));
            return;
        }
    };
    let subscription = match session
        .node
        .subscribe(ticket.path().clone(), Reach::Direct(ticket.peer()))
        .await
    {
        Ok(subscription) => subscription,
        Err(error) => {
            video_error(format!("video subscribe failed: {error}"));
            return;
        }
    };
    let remote = subscription.remote_broadcast();
    // The publisher registers its video rendition only once the first camera
    // frame reaches the encoder, and a mic-first call may not switch the camera
    // on for a while. The first catalog is therefore audio-only or empty; wait
    // for a rendition instead of failing on that snapshot.
    let mut catalog = remote.catalog();
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        if catalog
            .borrow()
            .as_ref()
            .is_some_and(|catalog| !catalog.video.renditions.is_empty())
        {
            break;
        }
        let _ = tokio::time::timeout(STOP_POLL, catalog.changed()).await;
    }
    // The player picks and adapts the rendition from the serving link (upstream
    // `moq-ext-adaptive`, which replaces the hand-picked middle rung). Audio
    // stays with the audio leg; video presents on arrival here and against the
    // audio playout clock below (av.rs).
    let player = match remote.play(PlayerConfig::default().with_latency(IMMEDIATE_LATENCY)) {
        Ok(player) => player,
        Err(error) => {
            video_error(format!("video player failed: {error}"));
            return;
        }
    };
    let status = player.status();
    // Admission must measure the video leg's actual delivery, not only audio
    // goodput; attach this subscription after the audio leg so it is sampled.
    if let Some(peer_session) = subscription.session() {
        crate::health::attach_video(
            &session.endpoint,
            peer_session.connection(),
            crate::health::signals_of(&subscription),
        );
    }
    let mut frames = player.video_frames();
    let mut stop_poll = tokio::time::interval(STOP_POLL);
    while !stop.load(Ordering::Relaxed) {
        // `VideoFrames::next` is a cancel-safe latest-frame slot, so bounding it
        // keeps hangup prompt and lets an idle track clear the picture.
        let frame = tokio::select! {
            _ = stop_poll.tick() => {
                if stop.load(Ordering::Relaxed) { break; }
                continue;
            }
            frame = tokio::time::timeout(FRAME_IDLE, frames.next()) => match frame {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(_) => {
                    render.clear();
                    continue;
                }
            }
        };
        let (rendition, decoder) = {
            let status = status.borrow();
            (status.rendition.clone(), status.decoder.clone())
        };
        *VIDEO_RENDITION
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = rendition.clone();
        {
            let mut info = VIDEO_INFO
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if info.rendition != rendition {
                info.bitrate = rendition.as_ref().and_then(|name| {
                    remote
                        .catalog()
                        .borrow()
                        .as_ref()
                        .and_then(|catalog| catalog.video.renditions.get(name))
                        .and_then(|config| config.bitrate)
                });
                info.rendition = rendition;
            }
            info.decoder = decoder;
        }
        let size = (frame.surface.width(), frame.surface.height());
        let pts_us = frame.timestamp.as_micros().unwrap_or(0) as u64;
        // Present against the audio playout clock: wait out an early frame,
        // drop one too late to show (av.rs). No audio leg = present on arrival.
        match crate::av::decide(pts_us) {
            crate::av::Decision::Present => {
                render.present(&frame);
                record_presented_frame(size);
            }
            crate::av::Decision::Wait(due) => {
                tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await;
                render.present(&frame);
                record_presented_frame(size);
            }
            crate::av::Decision::Drop => {}
        }
    }
    // Track ended or the session stopped: nothing is live any more.
    render.clear();
    drop(player);
    drop(subscription);
}
