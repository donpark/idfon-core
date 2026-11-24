# The media forks (`donpark/moq`, `donpark/iroh-live`)

Status: **current** (2026-11-24, after the iroh-live upgrade, donpark/idfon#221).
The media stack comes from two forks pinned by rev in the root `Cargo.toml`.
This doc says why we fork, what each fork carries, and how to move a pin.

## Pins

| Fork | Branch | Pinned rev | Upstream base |
| --- | --- | --- | --- |
| `donpark/moq` | `liquid-codec-next` | `14598b93c` | `moq-dev/moq` `main` at `4a6f52248` (the rev iroh-live pins) |
| `donpark/iroh-live` | `audio-renditions-next` | `fbdcbdda` | `n0-computer/iroh-live` `main` at `60a3fcf` |

Crate manifests name the **upstream** git URLs and revs (`moq-dev/moq@4a6f52248`,
`n0-computer/iroh-live@60a3fcf`); the root `[patch."https://github.com/moq-dev/moq"]`
and `[patch."https://github.com/n0-computer/iroh-live"]` tables redirect every
crate of each family to the fork. Upstream `main` is the only line we track; the
old `release`-line branches (`loss-recovery-release`, `liquid-codec`,
`audio-renditions` off iroh-live `dd80d44`) are retired.

## What the moq fork carries

On top of `4a6f52248` (`git log 4a6f52248..14598b93c` in `~/dev/idfon/moq-upstream`):

1. **libopus 1.6.1 via `opusic-sys`** instead of upstream's `unsafe-libopus`
   (c2rust 1.3.1). Adds DRED and deep PLC.
2. **Opt-in loss recovery.** A decoder backend reports its `Recovery`
   (look-ahead, longest burst, default on/off) and rebuilds a lost packet from
   the one after the hole: in-band FEC (LBRR) for the frame right before it,
   DRED for the rest of the burst, then concealment. A consumer that recovers
   (`Options::recover` or the codec default) holds the look-ahead as playout
   delay and keeps a recovered hole on a continuous timeline; only an unfilled
   hole resets.
3. **A working in-band FEC knob** on the encoder (sets the expected loss rate
   too; without it libopus writes no LBRR). Policy: FEC is mono-only, see
   `media-adaptation.md` §B2.4/§D7.
4. **LiquidCodec** — `AudioCodec::Liquid` with an application-registered
   implementation (`moq_audio::external`), 80 ms packets, and voice encodes that
   withhold silence. See `liquidcodec.md` in idfon-home.
5. A log line saying whether the VideoToolbox encoder runs on hardware.

Upstream `main` already gates VideoToolbox on `cfg(apple)` (macOS and iOS), so
the iOS hardware-encoder patch the old fork needed is gone. Whether iOS now
encodes on hardware in a real call is not yet confirmed on a device
(donpark/idfon#208).

## What the iroh-live fork carries

Publisher-ordered audio renditions and the player's rendition choice (switch
codecs mid-call), live bitrate retune (what ABR drives, `iroh-c-ffi/src/abr.rs`),
and mDNS through `iroh-mdns-address-lookup` (`iroh-mdns-peer-lookup` declares no
license).

## Upstreaming

None of this is upstream yet: no PR is open on `moq-dev/moq` or for the
audio renditions on `n0-computer/iroh-live`. Tracked in donpark/idfon#96.

## Moving a pin

In the moq clone (`~/dev/idfon/moq-upstream`, remotes `upstream` and `mine`):

1. Pick the moq rev the target iroh-live `main` pins, and rebase
   `liquid-codec-next` onto it.
2. `cargo test -p moq-audio` (includes the recovery tests).
3. `git push mine liquid-codec-next`.

Rebase `audio-renditions-next` onto the target iroh-live rev the same way and
push it. Then in idfon-core:

4. Update the upstream revs in the crate manifests (grep for `moq-dev/moq` and
   `n0-computer/iroh-live`) and the fork revs in the root `[patch]` tables.
5. `cargo check --workspace --all-targets`, then the media tests
   (`idfon-media`, `iroh-c-ffi`), then rebuild the apps' vendored libs.

## Rules

- **One source per family.** Every moq crate in the graph must resolve to the
  same fork rev, and every iroh-live crate likewise; a crate left on upstream or
  crates.io splits `moq_net`/`moq_mux` types into incompatible instances. When a
  new crate appears in either family, add it to the `[patch]` table.
- **Move moq and iroh-live together.** The moq rev must be the one the iroh-live
  rev builds against.
- Keep the git pins (reproducible builds); do not track branch heads.
