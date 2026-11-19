//! Group-call mixer (docs/group-live-chat.md, G0).
//!
//! Each listen leg paces its member's audio through its own jitter buffer
//! (`seam::PacedPlayback`) and pushes the result here instead of to the shell.
//! One thread sums the members every 10 ms and hands the shell a single
//! stream, so the shell's playback callback keeps seeing one producer at one
//! channel count, however many members talk.
//!
//! With one member there is nothing to mix: its audio passes straight to the
//! shell on the leg's own pacing thread, so a 1:1 call pays none of the mix's
//! prebuffer (two 20 ms chunks plus up to a 10 ms tick). The mix takes over
//! when a second member joins, and hands back when one is left.
//!
//! The device fallback needs none of this: moq's playback `Engine` already
//! mixes every sink opened on it, so legs share one engine instead.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Output rate. Every leg decodes to 48 kHz f32.
pub(crate) const RATE: u32 = 48_000;
const TICK: Duration = Duration::from_millis(10);
const TICK_FRAMES: usize = (RATE as usize) / 100;
/// A member starts contributing once it holds this much, or two of its
/// chunks, whichever is more, so a leg that emits 20 ms chunks every 20 ms
/// does not alternate between data and silence on a 10 ms tick.
const MIN_PREBUFFER_FRAMES: usize = TICK_FRAMES;
/// Past this a member's queue drops its oldest audio: the leg's clock runs
/// ahead of the mixer's, and latency must not grow without bound.
const MAX_QUEUE_FRAMES: usize = (RATE as usize) / 10;

/// Where the mix goes: interleaved f32 at [`RATE`] with `channels` channels.
pub(crate) type Sink = Box<dyn Fn(&[f32], u32) + Send + Sync>;

struct Member {
    queue: VecDeque<f32>,
    started: bool,
    largest_chunk: usize,
}

struct Inner {
    members: Mutex<HashMap<u64, Member>>,
    /// Output channels, fixed by the first member (1 or 2).
    channels: Mutex<Option<u32>>,
    stop: AtomicBool,
    next_id: AtomicU64,
    sink: Sink,
}

/// The running mixer. Dropping it stops the thread.
pub(crate) struct Mixer {
    inner: Arc<Inner>,
    thread: Option<std::thread::JoinHandle<()>>,
}

/// One member's input. Dropping it removes the member from the mix.
pub(crate) struct MemberInput {
    id: u64,
    inner: Arc<Inner>,
}

impl Mixer {
    pub(crate) fn start(sink: Sink) -> Self {
        let inner = Arc::new(Inner {
            members: Mutex::new(HashMap::new()),
            channels: Mutex::new(None),
            stop: AtomicBool::new(false),
            next_id: AtomicU64::new(1),
            sink,
        });
        let thread_inner = inner.clone();
        let thread = std::thread::Builder::new()
            .name("idfon-mixer".into())
            .spawn(move || run(thread_inner))
            .expect("spawn mixer thread");
        Self {
            inner,
            thread: Some(thread),
        }
    }

    /// Add a member whose audio arrives with `channels` channels.
    pub(crate) fn add(&self, channels: u32) -> MemberInput {
        self.inner
            .channels
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get_or_insert(channels.clamp(1, 2));
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner
            .members
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(
                id,
                Member {
                    queue: VecDeque::new(),
                    started: false,
                    largest_chunk: 0,
                },
            );
        MemberInput {
            id,
            inner: self.inner.clone(),
        }
    }

    pub(crate) fn member_count(&self) -> usize {
        self.inner
            .members
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .len()
    }
}

impl Drop for Mixer {
    fn drop(&mut self) {
        self.inner.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl MemberInput {
    /// Queue interleaved f32 `pcm` with `channels` channels, converting to the
    /// mix's channel count.
    pub(crate) fn push(&self, pcm: &[f32], channels: u32) {
        let out = self
            .inner
            .channels
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .unwrap_or(1);
        let mut members = self
            .inner
            .members
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let solo = members.len() == 1;
        let Some(member) = members.get_mut(&self.id) else {
            return;
        };
        let channels = channels.max(1) as usize;
        let out = out as usize;
        if solo {
            // Pass through: drop anything queued while others were in the mix.
            member.queue.clear();
            member.started = false;
            drop(members);
            if channels == out {
                (self.inner.sink)(pcm, out as u32);
            } else {
                let mut converted = Vec::with_capacity(pcm.len() / channels * out);
                for frame in pcm.chunks_exact(channels) {
                    match (channels, out) {
                        (1, 2) => converted.extend([frame[0], frame[0]]),
                        (_, 1) => converted.push(frame.iter().sum::<f32>() / channels as f32),
                        _ => converted.extend_from_slice(&frame[..2]),
                    }
                }
                (self.inner.sink)(&converted, out as u32);
            }
            return;
        }
        let frames = pcm.len() / channels;
        member.largest_chunk = member.largest_chunk.max(frames);
        for frame in pcm.chunks_exact(channels) {
            match (channels, out) {
                (a, b) if a == b => member.queue.extend(frame.iter().copied()),
                (1, 2) => {
                    member.queue.push_back(frame[0]);
                    member.queue.push_back(frame[0]);
                }
                // Down to mono: average the channels.
                (_, 1) => member
                    .queue
                    .push_back(frame.iter().sum::<f32>() / channels as f32),
                // More than stereo in: keep the first two.
                _ => member.queue.extend(frame[..2].iter().copied()),
            }
        }
        let max = MAX_QUEUE_FRAMES * out;
        if member.queue.len() > max {
            let excess = member.queue.len() - max;
            member.queue.drain(..excess);
        }
    }
}

impl Drop for MemberInput {
    fn drop(&mut self) {
        self.inner
            .members
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(&self.id);
    }
}

fn run(inner: Arc<Inner>) {
    let mut next = Instant::now() + TICK;
    let mut mix: Vec<f32> = Vec::new();
    while !inner.stop.load(Ordering::Relaxed) {
        let now = Instant::now();
        if next > now {
            std::thread::sleep(next - now);
        }
        next += TICK;
        // A long stall (debugger, suspended app): do not burst to catch up.
        if Instant::now() > next + TICK * 5 {
            next = Instant::now() + TICK;
        }
        let Some(channels) = *inner
            .channels
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
        else {
            continue;
        };
        let width = channels as usize;
        mix.clear();
        mix.resize(TICK_FRAMES * width, 0.0);
        let mut any = false;
        {
            let mut members = inner
                .members
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            // One member passes through on its own thread (see `push`).
            if members.len() < 2 {
                continue;
            }
            for member in members.values_mut() {
                if !member.started {
                    let prebuffer = MIN_PREBUFFER_FRAMES.max(member.largest_chunk * 2) * width;
                    if member.queue.len() < prebuffer {
                        continue;
                    }
                    member.started = true;
                }
                if member.queue.is_empty() {
                    // Ran dry: rebuild the prebuffer before contributing again.
                    member.started = false;
                    continue;
                }
                any = true;
                let take = member.queue.len().min(mix.len());
                for (slot, sample) in mix.iter_mut().zip(member.queue.drain(..take)) {
                    *slot += sample;
                }
            }
        }
        if any {
            for sample in mix.iter_mut() {
                // Soft clip: several loud members must not wrap.
                if sample.abs() > 0.9 {
                    *sample = sample.signum() * (0.9 + (sample.abs() - 0.9).tanh() * 0.1);
                }
            }
        }
        (inner.sink)(&mix, channels);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect() -> (Sink, Arc<Mutex<Vec<f32>>>) {
        let out = Arc::new(Mutex::new(Vec::new()));
        let sink_out = out.clone();
        (
            Box::new(move |pcm: &[f32], _channels: u32| {
                sink_out.lock().unwrap().extend_from_slice(pcm)
            }),
            out,
        )
    }

    #[test]
    fn two_members_sum_and_a_removed_member_leaves_the_mix() {
        let (sink, out) = collect();
        let mixer = Mixer::start(sink);
        let a = mixer.add(1);
        let b = mixer.add(1);
        // 100 ms (the queue cap) of constant 0.25 from each, in 20 ms chunks.
        for _ in 0..5 {
            a.push(&[0.25; 960], 1);
            b.push(&[0.25; 960], 1);
        }
        std::thread::sleep(Duration::from_millis(50));
        let peak = out.lock().unwrap().iter().cloned().fold(0.0f32, f32::max);
        assert!((peak - 0.5).abs() < 1e-3, "summed level {peak}");
        drop(b);
        assert_eq!(mixer.member_count(), 1);
        // Alone again: its audio passes straight through.
        out.lock().unwrap().clear();
        a.push(&[0.25; 960], 1);
        let after = out.lock().unwrap().iter().cloned().fold(0.0f32, f32::max);
        assert!((after - 0.25).abs() < 1e-3, "one member left, level {after}");
    }

    #[test]
    fn stereo_member_folds_into_a_mono_mix() {
        let (sink, out) = collect();
        let mixer = Mixer::start(sink);
        let mono = mixer.add(1);
        let stereo = mixer.add(2);
        let frame: Vec<f32> = [0.2f32, 0.4].repeat(960);
        // The mix's channel count stays the first member's after it leaves.
        drop(mono);
        stereo.push(&frame, 2);
        assert_eq!(out.lock().unwrap().len(), 960, "folded to mono");
        let peak = out.lock().unwrap().iter().cloned().fold(0.0f32, f32::max);
        assert!((peak - 0.3).abs() < 1e-3, "averaged to {peak}");
    }

    #[test]
    fn a_lone_member_passes_straight_through() {
        let (sink, out) = collect();
        let mixer = Mixer::start(sink);
        let a = mixer.add(1);
        a.push(&[0.25; 960], 1);
        // Delivered at once, on the pushing thread: no prebuffer, no tick.
        assert_eq!(out.lock().unwrap().len(), 960);
        assert!(mixer.inner.members.lock().unwrap()[&a.id].queue.is_empty());
    }

    #[test]
    fn a_member_queue_is_capped() {
        let (sink, _out) = collect();
        let mixer = Mixer::start(sink);
        let a = mixer.add(1);
        let _b = mixer.add(1);
        for _ in 0..100 {
            a.push(&[0.1; 960], 1);
        }
        let len = mixer.inner.members.lock().unwrap()[&a.id].queue.len();
        assert!(len <= MAX_QUEUE_FRAMES, "queue {len}");
    }
}
