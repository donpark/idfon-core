//! LiquidCodec bridge: the shell (Swift, Core ML) registers C callbacks and this
//! module installs them as moq-audio's external LiquidCodec implementation
//! (`moq_audio::external`). With nothing registered, a `liquid` profile is refused
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
use std::sync::Arc;

use moq_audio::external::{self, liquid, ExternalCodec, ExternalDecoder, ExternalEncoder};
use safer_ffi::prelude::*;

type NewFn = unsafe extern "C" fn(*const c_void) -> *const c_void;
type FreeFn = unsafe extern "C" fn(*const c_void);
type EncodeFn = unsafe extern "C" fn(*const c_void, *const f32, usize, *mut u8, usize) -> i32;
type DecodeFn = unsafe extern "C" fn(*const c_void, *const u8, usize, *mut f32, usize) -> i32;
type RecoverFn = unsafe extern "C" fn(*const c_void, *const u8, usize, u32, *mut f32, usize) -> i32;

/// Largest packet a stream may write (a frame plus redundancy, with headroom).
const MAX_PACKET: usize = 256;

#[derive(Clone, Copy)]
struct Callbacks {
    ctx: usize,
    enc_new: NewFn,
    enc_encode: EncodeFn,
    enc_free: FreeFn,
    dec_new: NewFn,
    dec_decode: DecodeFn,
    dec_recover: RecoverFn,
    dec_free: FreeFn,
    bitrate: u64,
}

struct Codec(Callbacks);

struct Handle {
    ptr: usize,
    free: FreeFn,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // SAFETY: `ptr` came from the matching `*_new` and is released once.
        unsafe { (self.free)(self.ptr as *const c_void) }
    }
}

fn open(cb: &Callbacks, new: NewFn, free: FreeFn, what: &str) -> Result<Handle, String> {
    // SAFETY: the shell registered `new` for this purpose; ctx is its own pointer.
    let ptr = unsafe { new(cb.ctx as *const c_void) };
    if ptr.is_null() {
        return Err(format!("shell refused to open a liquid {what}"));
    }
    Ok(Handle {
        ptr: ptr as usize,
        free,
    })
}

struct Encoder {
    cb: Callbacks,
    handle: Handle,
}

impl ExternalEncoder for Encoder {
    fn encode(&mut self, pcm: &[f32]) -> Result<Vec<u8>, String> {
        let mut out = vec![0u8; MAX_PACKET];
        // SAFETY: buffers are valid for the lengths passed for the duration of the call.
        let n = unsafe {
            (self.cb.enc_encode)(
                self.handle.ptr as *const c_void,
                pcm.as_ptr(),
                pcm.len(),
                out.as_mut_ptr(),
                out.len(),
            )
        };
        if n <= 0 || n as usize > out.len() {
            return Err(format!("shell encode returned {n}"));
        }
        out.truncate(n as usize);
        Ok(out)
    }

    fn reset(&mut self) {
        if let Ok(handle) = open(&self.cb, self.cb.enc_new, self.cb.enc_free, "encoder") {
            self.handle = handle;
        }
    }

    fn bitrate(&self) -> u64 {
        self.cb.bitrate
    }
}

struct Decoder {
    cb: Callbacks,
    handle: Handle,
}

impl Decoder {
    fn frame(n: i32, out: Vec<f32>) -> Result<Vec<f32>, String> {
        if n as usize != liquid::FRAME_SIZE {
            return Err(format!("shell decode returned {n}"));
        }
        Ok(out)
    }
}

impl ExternalDecoder for Decoder {
    fn decode(&mut self, packet: &[u8]) -> Result<Vec<f32>, String> {
        let mut out = vec![0f32; liquid::FRAME_SIZE];
        // SAFETY: as above.
        let n = unsafe {
            (self.cb.dec_decode)(
                self.handle.ptr as *const c_void,
                packet.as_ptr(),
                packet.len(),
                out.as_mut_ptr(),
                out.len(),
            )
        };
        Self::frame(n, out)
    }

    fn recover(&mut self, next: Option<&[u8]>, frames_to_next: usize) -> Result<Vec<f32>, String> {
        let mut out = vec![0f32; liquid::FRAME_SIZE];
        let (ptr, len) = next.map_or((std::ptr::null(), 0), |p| (p.as_ptr(), p.len()));
        // SAFETY: as above; a null `next` is part of the contract.
        let n = unsafe {
            (self.cb.dec_recover)(
                self.handle.ptr as *const c_void,
                ptr,
                len,
                frames_to_next.min(u32::MAX as usize) as u32,
                out.as_mut_ptr(),
                out.len(),
            )
        };
        Self::frame(n, out)
    }

    fn reset(&mut self) {
        if let Ok(handle) = open(&self.cb, self.cb.dec_new, self.cb.dec_free, "decoder") {
            self.handle = handle;
        }
    }
}

impl ExternalCodec for Codec {
    fn encoder(&self) -> Result<Box<dyn ExternalEncoder>, String> {
        let handle = open(&self.0, self.0.enc_new, self.0.enc_free, "encoder")?;
        Ok(Box::new(Encoder { cb: self.0, handle }))
    }

    fn decoder(&self) -> Result<Box<dyn ExternalDecoder>, String> {
        let handle = open(&self.0, self.0.dec_new, self.0.dec_free, "decoder")?;
        Ok(Box::new(Decoder { cb: self.0, handle }))
    }
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
    external::register_liquid(Some(Arc::new(Codec(Callbacks {
        ctx: ctx as usize,
        enc_new,
        enc_encode,
        enc_free,
        dec_new,
        dec_decode,
        dec_recover,
        dec_free,
        bitrate: u64::from(bitrate),
    }))));
    0
}

/// Remove the shell's LiquidCodec implementation. Open streams keep theirs.
#[ffi_export]
pub fn media_liquid_unregister() {
    external::register_liquid(None);
}

/// 1 when a LiquidCodec implementation is registered, else 0.
#[ffi_export]
pub fn media_liquid_available() -> u8 {
    u8::from(external::liquid_available())
}
