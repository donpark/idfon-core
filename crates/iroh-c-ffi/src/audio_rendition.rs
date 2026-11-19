//! Which audio rendition a subscriber plays, when a publisher offers several (moq-media
//! `AudioPublisher::set_renditions`: the same audio as, say, LiquidCodec and Opus).
//!
//! Like the video ladder, the choice is the subscriber's:
//! 1. the **preferred** codec — the one the call's invite names (`audio_codec=`), i.e. what the caller
//!    chose — when this side can decode it;
//! 2. otherwise the first rendition this side can decode (LiquidCodec only once its implementation is
//!    registered, so a call starts on Opus and moves to LiquidCodec when it is ready);
//! 3. under a **constrained** network (sustained loss or a low delivery rate), the cheapest decodable
//!    rendition, until the network has been good for a while.
//!
//! Pure policy, no I/O: the decode loop feeds it the catalog and network samples and acts on the answer.

use std::time::{Duration, Instant};

/// One audio rendition as the catalog describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Rendition {
    pub name: String,
    pub codec: String,
    /// Bits per second, when the catalog says.
    pub bitrate: Option<u64>,
}

/// Whether this side can decode `codec` right now.
pub(crate) fn decodable(codec: &str) -> bool {
    match codec {
        "liquid" => crate::liquid::liquid_available(),
        "opus" | "pcm" => true,
        _ => false,
    }
}

/// Network constraint with hysteresis: down after two bad samples in a row, back up only after
/// `RECOVER` of good ones, so a flapping link does not flip codecs.
#[derive(Debug)]
pub(crate) struct Policy {
    bad_streak: u32,
    constrained_since: Option<Instant>,
    good_since: Option<Instant>,
}

impl Policy {
    /// Loss above this is constrained.
    const LOSS: f64 = 0.05;
    /// A delivery-rate estimate below this (bits/s) is constrained.
    const DELIVERY_BPS: u64 = 64_000;
    /// Good samples needed before leaving the constrained state.
    const RECOVER: Duration = Duration::from_secs(10);

    pub fn new() -> Self {
        Self {
            bad_streak: 0,
            constrained_since: None,
            good_since: None,
        }
    }

    /// Feed one network sample (`loss` as a fraction, `delivery_bps` when estimated).
    pub fn sample(&mut self, now: Instant, loss: f64, delivery_bps: Option<u64>) {
        let bad = loss > Self::LOSS
            || delivery_bps.is_some_and(|bps| bps > 0 && bps < Self::DELIVERY_BPS);
        if bad {
            self.good_since = None;
            self.bad_streak += 1;
            if self.bad_streak >= 2 && self.constrained_since.is_none() {
                self.constrained_since = Some(now);
            }
        } else {
            self.bad_streak = 0;
            let good_since = *self.good_since.get_or_insert(now);
            if self.constrained_since.is_some() && now.duration_since(good_since) >= Self::RECOVER {
                self.constrained_since = None;
            }
        }
    }

    pub fn constrained(&self) -> bool {
        self.constrained_since.is_some()
    }

    /// The rendition to play: see the module docs. `None` when nothing is decodable.
    pub fn choose<'a>(
        &self,
        renditions: &'a [Rendition],
        preferred: Option<&str>,
        can_decode: impl Fn(&str) -> bool,
    ) -> Option<&'a Rendition> {
        let usable: Vec<&Rendition> = renditions.iter().filter(|r| can_decode(&r.codec)).collect();
        if self.constrained() {
            return usable
                .iter()
                .copied()
                .min_by_key(|r| r.bitrate.unwrap_or(u64::MAX));
        }
        preferred
            .and_then(|codec| usable.iter().copied().find(|r| r.codec == codec))
            .or_else(|| usable.first().copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(name: &str, codec: &str, bitrate: u64) -> Rendition {
        Rendition {
            name: name.into(),
            codec: codec.into(),
            bitrate: Some(bitrate),
        }
    }

    fn ladder() -> Vec<Rendition> {
        vec![r("0.opus", "opus", 32_000), r("1.liquid", "liquid", 3_400)]
    }

    #[test]
    fn follows_the_preference_when_decodable() {
        let p = Policy::new();
        assert_eq!(
            p.choose(&ladder(), Some("liquid"), |_| true).unwrap().codec,
            "liquid"
        );
        assert_eq!(
            p.choose(&ladder(), Some("opus"), |_| true).unwrap().codec,
            "opus"
        );
    }

    #[test]
    fn falls_back_until_the_preferred_codec_can_decode() {
        let p = Policy::new();
        let no_liquid = |c: &str| c != "liquid";
        assert_eq!(
            p.choose(&ladder(), Some("liquid"), no_liquid)
                .unwrap()
                .codec,
            "opus"
        );
        assert!(p
            .choose(&[r("0.liquid", "liquid", 3_400)], Some("liquid"), no_liquid)
            .is_none());
    }

    #[test]
    fn a_constrained_network_takes_the_cheapest_and_recovers_with_hysteresis() {
        let mut p = Policy::new();
        let t0 = Instant::now();
        p.sample(t0, 0.10, None);
        assert!(!p.constrained(), "one bad sample is not enough");
        p.sample(t0 + Duration::from_millis(500), 0.0, Some(40_000));
        assert!(p.constrained());
        assert_eq!(
            p.choose(&ladder(), Some("opus"), |_| true).unwrap().codec,
            "liquid"
        );
        p.sample(t0 + Duration::from_secs(1), 0.0, Some(2_000_000));
        p.sample(t0 + Duration::from_secs(6), 0.0, Some(2_000_000));
        assert!(p.constrained(), "recovery needs 10 s of good samples");
        p.sample(t0 + Duration::from_secs(12), 0.0, Some(2_000_000));
        assert!(!p.constrained());
        assert_eq!(
            p.choose(&ladder(), Some("opus"), |_| true).unwrap().codec,
            "opus"
        );
    }
}
