//! Audio ABR: retune the live Opus encoder from the connection's send
//! estimate (docs/media-adaptation.md §C5).
//!
//! The mic audio publishes on the call's sessions, and each session's
//! congestion controller knows what the uplink can carry:
//! [`Session::send_bandwidth`](moq_net::Session::send_bandwidth) — the same
//! estimate moq-net mirrors to subscribers as `delivery_bps`. The house rate
//! policy ([`moq_mux::rate`], "every sender backs off the same way") turns
//! estimates into targets.
//!
//! The encoder starts at its configured rate and the estimate only takes it
//! down or back up within the voice band: 8–64 kbps per channel, never above
//! configured. A caller that configured more (music via
//! `media_audio_set_bitrate`) made that choice explicitly; ABR stands down
//! rather than fight it.
//!
//! The publisher retunes one rendition through
//! `LocalBroadcast::set_audio_bitrate` (the iroh-live fork,
//! `audio-renditions-next`). With several peers reading the call, the lowest
//! target wins: the broadcast is one encoding for all of them.

use std::collections::HashMap;
use std::sync::Mutex;

use iroh::EndpointId;
use iroh::Watcher as _;
use moq_mux::rate::{Control, Policy, Target};
use moq_net::bandwidth::Rate;
use moq_net::time::Instant;

/// The voice band `Producer::set_bitrate` is clamped to, in bits per second
/// per channel. The floor is where speech stops being worth sending; the
/// ceiling is where it stops getting better (and matches the floor of
/// `media_audio_set_bitrate`, which is per-channel too).
const BAND_MIN: u64 = 8_000;
const BAND_MAX: u64 = 64_000;

/// How often the driver looks for new sessions to follow.
const SESSION_POLL: std::time::Duration = std::time::Duration::from_millis(500);

/// The running driver; dropping it (call end, or a new call) stops it.
static DRIVER: Mutex<Option<tokio::task::JoinHandle<()>>> = Mutex::new(None);

/// Start driving rendition `rendition` of the call's broadcast from the send
/// estimates of the call's sessions. Returns whether ABR runs: `false` when the
/// configured rate is outside the voice band (the caller's explicit choice
/// stands).
///
/// The driver holds neither the node nor the broadcast: it looks the call up
/// at each step (`crate::media::call_publication`), so a hung-up call's path is
/// free at once, and it ends when no call is live.
pub(crate) fn start(rendition: String, configured_bps: u64, channels: u32) -> bool {
    stop();
    let Some(policy) = voice_policy(configured_bps, channels) else {
        return false;
    };
    let task = crate::util::TOKIO_EXECUTOR.spawn(async move {
        let targets: std::sync::Arc<Mutex<HashMap<EndpointId, u64>>> = Default::default();
        let applied = std::sync::Arc::new(Mutex::new(configured_bps));
        let mut followed: Vec<iroh_live::Session> = Vec::new();
        loop {
            let Some(sessions) = crate::media::call_publication()
                .map(|(node, _)| node.sessions().get())
            else {
                // Not published yet (the call is starting), or the call is gone.
                if !crate::media::call_live() {
                    break;
                }
                tokio::time::sleep(SESSION_POLL).await;
                continue;
            };
            followed.retain(|session| sessions.contains(session));
            for session in sessions {
                if followed.contains(&session) {
                    continue;
                }
                followed.push(session.clone());
                let Some(mut send) = session.moq().send_bandwidth() else {
                    continue;
                };
                let (policy, targets, applied, rendition) = (
                    policy.clone(),
                    targets.clone(),
                    applied.clone(),
                    rendition.clone(),
                );
                tokio::spawn(async move {
                    let peer = session.peer();
                    let mut rate = Control::new(policy);
                    let apply = |target: Option<Target>| {
                        let Some(target) = target else { return };
                        let bps = match target {
                            Target::Enabled(rate) => rate.as_bps(),
                            Target::Disabled => return,
                        };
                        let mut targets = targets.lock().unwrap_or_else(|p| p.into_inner());
                        targets.insert(peer, bps);
                        let lowest = targets.values().copied().min().unwrap_or(bps);
                        drop(targets);
                        let mut applied = applied.lock().unwrap_or_else(|p| p.into_inner());
                        if *applied == lowest {
                            return;
                        }
                        let Some((_, broadcast)) = crate::media::call_publication() else {
                            return;
                        };
                        match broadcast.set_audio_bitrate(&rendition, Rate::from_bps(lowest)) {
                            Ok(()) => {
                                tracing::debug!(target: "idfon.media", bps = lowest, "audio ABR retune");
                                *applied = lowest;
                            }
                            Err(err) => {
                                tracing::debug!(target: "idfon.media", error = %err, "audio ABR retune refused");
                            }
                        }
                    };
                    apply(rate.update(send.peek(), Instant::now()));
                    loop {
                        tokio::select! {
                            _ = session.closed() => break,
                            changed = kio::wait(|waiter| send.poll_changed(waiter)) => {
                                let Ok(estimate) = changed else { break };
                                apply(rate.update(estimate, Instant::now()));
                            }
                        }
                    }
                    targets.lock().unwrap_or_else(|p| p.into_inner()).remove(&peer);
                });
            }
            tokio::time::sleep(SESSION_POLL).await;
        }
    });
    *DRIVER.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(task);
    true
}

/// Call end: stop the driver. Its per-session followers end as the sessions close.
pub(crate) fn stop() {
    if let Some(task) = DRIVER.lock().unwrap_or_else(|poison| poison.into_inner()).take() {
        task.abort();
    }
}

/// The rate policy for a call configured at `configured_bps` total over
/// `channels` channels: within the voice band per channel, capped at the
/// configured rate (an optimistic estimate is not a reason to send more than
/// was asked for). `None` = stand down, leave the encoder alone.
fn voice_policy(configured_bps: u64, channels: u32) -> Option<Policy> {
    let channels = u64::from(channels.max(1));
    let per_channel = configured_bps / channels;
    if !(BAND_MIN..=BAND_MAX).contains(&per_channel) {
        return None;
    }
    let mut policy = Policy::new(Rate::from_bps(configured_bps));
    policy.min = Rate::from_bps(BAND_MIN * channels);
    Some(policy)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn voice_policy_clamps_the_voice_band_per_channel() {
        // Mono voice at the 36 kbps default: floor 8k, ceiling = configured.
        let policy = voice_policy(36_000, 1).unwrap();
        assert_eq!(policy.max.as_bps(), 36_000);
        assert_eq!(policy.min.as_bps(), 8_000);

        // Stereo voice at 48 kbps/channel: both ends scale with channels.
        let policy = voice_policy(96_000, 2).unwrap();
        assert_eq!(policy.max.as_bps(), 96_000);
        assert_eq!(policy.min.as_bps(), 16_000);
    }

    #[test]
    fn voice_policy_stands_down_outside_the_band() {
        assert!(voice_policy(1_020_000, 2).is_none(), "music rates are the caller's choice");
        assert!(voice_policy(4_000, 1).is_none(), "below the band, keep the configured rate");
    }

    #[test]
    fn estimates_never_take_the_target_above_configured() {
        let mut rate = Control::new(voice_policy(36_000, 1).unwrap());
        assert_eq!(
            rate.update(Some(Rate::from_bps(10_000_000)), Instant::now()),
            None,
            "an optimistic estimate changes nothing"
        );
        let bps = |target: moq_mux::rate::Target| match target {
            moq_mux::rate::Target::Enabled(rate) => rate.as_bps(),
            moq_mux::rate::Target::Disabled => 0,
        };
        assert!(bps(rate.target()) <= 36_000);
        // A starved uplink takes it down at once, into the band.
        let target = bps(rate.update(Some(Rate::from_bps(12_000)), Instant::now()).unwrap());
        assert!(
            (8_000..=36_000).contains(&target),
            "target {target} left the voice band"
        );
    }
}
