//! Audio ABR: retune the live Opus encoder from the connection's send
//! estimate (docs/media-adaptation.md §C5).
//!
//! The mic audio publishes on the session the peer dialed in on, and that
//! session's congestion controller knows what the uplink can carry:
//! [`Session::send_bandwidth`](moq_net::Session::send_bandwidth) — the same
//! estimate moq-net mirrors to subscribers as `delivery_bps`. The house rate
//! policy ([`moq_video::encode::rate`], "every sender backs off the same way")
//! turns estimates into targets, and a [`BitrateControl`] hands them to the
//! encoder the publish task owns (see `Options::bitrate_control` in moq-audio).
//!
//! The encoder starts at its configured rate and the estimate only takes it
//! down or back up within the voice band: 8–64 kbps per channel, never above
//! configured. A caller that configured more (music via
//! `media_audio_set_bitrate`) made that choice explicitly; ABR stands down
//! rather than fight it.

//!
//! TODO(iroh-live-next): fork. `moq-ext-publisher` owns the encoder and exposes
//! no bitrate control, and moq-audio's `BitrateControl` was fork-only, so the
//! driver is off: [`start`] only reports whether ABR would run. The policy and
//! its tests stay for when a control returns (upstream or the fork, step 3/4).

#[cfg(test)]
use moq_net::time::Instant;

#[cfg(test)]
use moq_mux::rate::Control;
use moq_mux::rate::Policy;
use moq_net::bandwidth::Rate;

/// The voice band `Producer::set_bitrate` is clamped to, in bits per second
/// per channel. The floor is where speech stops being worth sending; the
/// ceiling is where it stops getting better (and matches the floor of
/// `media_audio_set_bitrate`, which is per-channel too).
const BAND_MIN: u64 = 8_000;
const BAND_MAX: u64 = 64_000;

/// Would drive the publish encoder from the send estimate. Returns whether the
/// configured rate is inside the voice band (ABR would run); nothing drives the
/// encoder on this branch.
pub(crate) fn start(configured_bps: u64, channels: u32) -> bool {
    let wanted = voice_policy(configured_bps, channels).is_some();
    if wanted {
        tracing::info!(target: "idfon.media", configured_bps, channels,
            "audio ABR unavailable on upstream iroh-live; the encoder keeps its configured rate");
    }
    wanted
}

/// Call end. Nothing to forget while the driver is off.
pub(crate) fn stop() {}

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
