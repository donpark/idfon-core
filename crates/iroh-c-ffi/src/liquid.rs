//! LiquidCodec bridge: the shell (Swift, Core ML) registers C callbacks and this
//! module installs them as moq-audio's external LiquidCodec implementation
//! (`moq_audio::external`).
//!
//! TODO(iroh-live-next): fork. Upstream moq-audio has no external codec seam, so
//! on this branch registration is accepted and ignored: `media_liquid_available`
//! reports 0 and a `liquid` profile is refused. Step 3 (the rebased moq fork)
//! restores the implementation (git history: this file before the port). With nothing registered, a `liquid` profile is refused
//! at start, and a `liquid` track fails to open its decoder.
//!
//! Contract (one packet per 80 ms frame, see `moq_audio::external::liquid`):
//! - `enc_new(ctx)` opens an encoder stream; `enc_encode(enc, pcm, 1920, out, cap)`
//!   writes one packet and returns its length (negative on error).
//! - `dec_new(ctx)` opens a decoder stream; `dec_decode(dec, packet, len, out, 1920)`
//!   writes 1920 samples and returns 1920 (negative on error);
//!   `dec_recover(dec, next, next_len, frames_to_next, out, 1920)` does the same for a
//!   lost packet (`next` may be null).
//! - `enc_free` / `dec_free` release a stream. Callbacks may run on any thread, one
//!   call at a time per stream.

use std::ffi::c_void;

use safer_ffi::prelude::*;

/// Whether a LiquidCodec implementation can be used. Always false until the moq fork returns.
pub(crate) fn liquid_available() -> bool {
    false
}

/// Register the shell's LiquidCodec implementation (see the module docs for the
/// callback contract). `bitrate` is what the catalog advertises, in bits per
/// second. Replaces any earlier registration; returns 0.
#[ffi_export]
pub fn media_liquid_register(
    ctx: *const c_void,
    enc_new: unsafe extern "C" fn(*const c_void) -> *const c_void,
    enc_encode: unsafe extern "C" fn(*const c_void, *const f32, usize, *mut u8, usize) -> i32,
    enc_free: unsafe extern "C" fn(*const c_void),
    dec_new: unsafe extern "C" fn(*const c_void) -> *const c_void,
    dec_decode: unsafe extern "C" fn(*const c_void, *const u8, usize, *mut f32, usize) -> i32,
    dec_recover: unsafe extern "C" fn(*const c_void, *const u8, usize, u32, *mut f32, usize) -> i32,
    dec_free: unsafe extern "C" fn(*const c_void),
    bitrate: u32,
) -> u8 {
    // TODO(iroh-live-next): fork. Nothing to install the callbacks into.
    let _ = (
        ctx, enc_new, enc_encode, enc_free, dec_new, dec_decode, dec_recover, dec_free, bitrate,
    );
    tracing::warn!(target: "idfon.media", "liquid codec registration ignored: no external codec seam upstream");
    0
}

/// Remove the shell's LiquidCodec implementation. Open streams keep theirs.
#[ffi_export]
pub fn media_liquid_unregister() {}

/// 1 when a LiquidCodec implementation is registered, else 0.
#[ffi_export]
pub fn media_liquid_available() -> u8 {
    u8::from(liquid_available())
}
