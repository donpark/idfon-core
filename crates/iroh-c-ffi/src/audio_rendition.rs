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

/// Sorts catalog renditions back into the publisher's order.
///
/// The catalog keys renditions by track name in a map, which loses the publisher's order; call
/// renditions are named `{index}.{codec}` (`0.opus`, `1.liquid`), so the index prefix restores it.
/// Names without a numeric prefix (a publisher that names tracks after the codec) sort after the
/// indexed ones, by name.
pub(crate) fn in_publisher_order(mut renditions: Vec<Rendition>) -> Vec<Rendition> {
    fn index(name: &str) -> Option<u32> {
        name.split_once('.').and_then(|(index, _)| index.parse().ok())
    }
    renditions.sort_by(|a, b| {
        (index(&a.name).is_none(), index(&a.name), &a.name)
            .cmp(&(index(&b.name).is_none(), index(&b.name), &b.name))
    });
    renditions
}

/// Why [`Policy::choose_with_reason`] picked a rendition, for the call log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reason {
    /// The network is constrained: the cheapest decodable rendition.
    Constrained,
    /// The invite's codec, decodable here.
    Preferred,
    /// The publisher's first decodable rendition.
    First,
}

impl std::fmt::Display for Reason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Constrained => "constrained",
            Self::Preferred => "preferred",
            Self::First => "first",
        })
    }
}

/// Whether this side can decode `codec` right now.
pub(crate) fn decodable(codec: &str) -> bool {
    match codec {
        "liquid" => moq_audio::external::liquid_available(),
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
        self.choose_with_reason(renditions, preferred, can_decode)
            .map(|(rendition, _)| rendition)
    }

    /// As [`choose`](Self::choose), also saying why. `renditions` must be in publisher order
    /// ([`in_publisher_order`]).
    pub fn choose_with_reason<'a>(
        &self,
        renditions: &'a [Rendition],
        preferred: Option<&str>,
        can_decode: impl Fn(&str) -> bool,
    ) -> Option<(&'a Rendition, Reason)> {
        let usable: Vec<&Rendition> = renditions.iter().filter(|r| can_decode(&r.codec)).collect();
        if self.constrained() {
            return usable
                .iter()
                .copied()
                .min_by_key(|r| r.bitrate.unwrap_or(u64::MAX))
                .map(|r| (r, Reason::Constrained));
        }
        preferred
            .and_then(|codec| usable.iter().copied().find(|r| r.codec == codec))
            .map(|r| (r, Reason::Preferred))
            .or_else(|| usable.first().copied().map(|r| (r, Reason::First)))
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

    #[test]
    fn publisher_order_survives_the_catalog_map() {
        // A catalog map iterates by name: "liquid" before "opus". The index prefix restores the
        // publisher's order, so with no preference the publisher's first codec plays.
        let from_map = vec![r("1.liquid", "liquid", 3_400), r("0.opus", "opus", 32_000)];
        let ordered = in_publisher_order(from_map);
        assert_eq!(ordered[0].name, "0.opus");
        let p = Policy::new();
        let (chosen, reason) = p.choose_with_reason(&ordered, None, |_| true).unwrap();
        assert_eq!((chosen.codec.as_str(), reason), ("opus", Reason::First));
        let (chosen, reason) = p.choose_with_reason(&ordered, Some("liquid"), |_| true).unwrap();
        assert_eq!((chosen.codec.as_str(), reason), ("liquid", Reason::Preferred));
    }

    #[test]
    fn indexed_names_sort_numerically_and_before_bare_names() {
        let ordered = in_publisher_order(vec![
            r("opus", "opus", 1),
            r("10.pcm", "pcm", 1),
            r("2.liquid", "liquid", 1),
            r("0.opus", "opus", 1),
        ]);
        let names: Vec<&str> = ordered.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["0.opus", "2.liquid", "10.pcm", "opus"]);
    }
}
