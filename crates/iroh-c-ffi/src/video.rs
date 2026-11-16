//! Current decoded-video FFI bridge.

use iroh_live::{ticket::LiveTicket, Live};
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

use idfon_media::seam::VideoRender;
use moq_video::Frame as VideoFrame;

use crate::media::media_path;

static VIDEO: Mutex<Option<VideoSession>> = Mutex::new(None);
static VIDEO_ERROR: OnceLock<Mutex<String>> = OnceLock::new();
static VIDEO_FRAMES_PRESENTED: AtomicU64 = AtomicU64::new(0);
static VIDEO_LAST_FRAME_AT: Mutex<Option<Instant>> = Mutex::new(None);
static VIDEO_RENDITION: Mutex<Option<String>> = Mutex::new(None);

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

fn record_presented_frame() {
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
const PHOTO_FRAME_IDLE: Duration = Duration::from_secs(11);
/// How often the loop re-checks the stop flag while waiting on the peer.
const STOP_POLL: Duration = Duration::from_millis(100);

fn video_error(message: impl Into<String>) {
    if let Ok(mut error) = VIDEO_ERROR.get_or_init(|| Mutex::new(String::new())).lock() {
        *error = message.into();
    }
}

struct VideoSession {
    stop: Arc<AtomicBool>,
    photo_mode: tokio::sync::watch::Sender<bool>,
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
    let (photo_mode_tx, photo_mode_rx) = tokio::sync::watch::channel(false);
    let return_path = frame_path.to_string_lossy().into_owned();
    std::thread::spawn(move || {
        let Ok(runtime) = tokio::runtime::Runtime::new() else {
            video_error("video runtime creation failed");
            return;
        };
        runtime.block_on(video_loop(&ticket, render, thread_stop, photo_mode_rx));
    });
    *VIDEO.lock().unwrap() = Some(VideoSession { stop, photo_mode: photo_mode_tx });
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

#[ffi_export]
pub fn media_video_set_photo_mode(enabled: u8) -> u8 {
    let session = VIDEO.lock().unwrap_or_else(|poison| poison.into_inner());
    let Some(session) = session.as_ref() else { return 1 };
    session.photo_mode.send_replace(enabled != 0);
    0
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
    fn present(&self, frame: VideoFrame) {
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
    fn present(&self, frame: VideoFrame) {
        let Ok(rgba) = frame.surface.to_rgba(&moq_video::convert::Config::default()) else {
            return;
        };
        let data = rgba.data();
        let pts_ms = frame.timestamp.as_millis() as u64;
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

async fn video_loop(
    ticket: &str,
    render: Box<dyn VideoRender>,
    stop: Arc<AtomicBool>,
    mut photo_mode: tokio::sync::watch::Receiver<bool>,
) {
    let ticket = match ticket.parse::<LiveTicket>() {
        Ok(ticket) => ticket,
        Err(error) => {
            video_error(format!("ticket parse failed: {error}"));
            return;
        }
    };
    let live = match Live::from_env().await {
        Ok(live) => live.spawn(),
        Err(error) => {
            video_error(format!("live init failed: {error}"));
            return;
        }
    };
    let subscription = match live
        .subscribe(ticket.endpoint, &ticket.broadcast_name)
        .await
    {
        Ok(subscription) => subscription,
        Err(error) => {
            video_error(format!("video subscribe failed: {error}"));
            return;
        }
    };
    let broadcast = subscription.broadcast();
    // The publisher registers its video rendition only once the first camera
    // frame reaches the encoder (moq-media `VideoSource::Frames` derives the
    // catalog geometry from it), and a mic-first call may not switch the camera
    // on for a while. The first catalog is therefore audio-only or empty; wait
    // for the rendition instead of failing on that snapshot.
    while !broadcast.has_video() {
        if stop.load(Ordering::Relaxed) {
            live.shutdown().await;
            return;
        }
        tokio::time::sleep(STOP_POLL).await;
    }
    // Never start at `best_video()`: on a cellular path that would ask for
    // 720p before the receiver has measured whether the link can carry it.
    // Start at the lowest rung (the still-update profile) and let the existing
    // adaptation loop earn upgrades from delivery/goodput evidence.
    let first_rendition = moq_media::adaptive::rank_renditions(broadcast.catalog().video())
        .last()
        .map(|rendition| rendition.name.clone());
    let Some(first_rendition) = first_rendition else {
        video_error("video catalog has no renditions");
        live.shutdown().await;
        return;
    };
    let track = match broadcast.video_rendition(&first_rendition).await {
        Ok(track) => track,
        Err(error) => {
            video_error(format!("video track failed: {error}"));
            live.shutdown().await;
            return;
        }
    };
    track.enable_adaptation(subscription.signals().clone());
    *VIDEO_RENDITION.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(track.rendition());
    // Admission must measure the video leg's actual delivery, not only audio
    // goodput; attach this subscription after the audio leg so it is sampled.
    crate::health::attach_video(&live, subscription.session().conn(), subscription.signals().clone());
    let mut stop_poll = tokio::time::interval(STOP_POLL);
    while !stop.load(Ordering::Relaxed) {
        // Photo mode retains the last still through the capacity-aware update
        // interval; ordinary video clears promptly when its stream goes quiet.
        let idle = if *photo_mode.borrow() { PHOTO_FRAME_IDLE } else { FRAME_IDLE };
        let frame = tokio::select! {
            _ = stop_poll.tick() => {
                if stop.load(Ordering::Relaxed) { break; }
                continue;
            }
            changed = photo_mode.changed() => {
                if changed.is_err() { break; }
                continue;
            }
            frame = tokio::time::timeout(idle, track.recv()) => match frame {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(_) => {
                    render.clear();
                    continue;
                }
            }
        };
        *VIDEO_RENDITION.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(track.rendition());
        // Present against the audio playout clock: wait out an early frame,
        // drop one too late to show (av.rs). No audio leg = present on arrival.
        match crate::av::decide(frame.timestamp.as_micros() as u64) {
            crate::av::Decision::Present => {
                render.present(frame);
                record_presented_frame();
            }
            crate::av::Decision::Wait(due) => {
                tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await;
                render.present(frame);
                record_presented_frame();
            }
            crate::av::Decision::Drop => {}
        }
    }
    // Track ended or the session stopped: nothing is live any more.
    render.clear();
    live.shutdown().await;
}
