import CoreML
import Foundation

/// The seam between the Core ML codec and the media stack's C ABI (`media_liquid_register` in
/// crates/iroh-c-ffi/src/liquid.rs). This type owns the loaded models and hands out the C callback set; the app
/// only forwards `callbacks` to the FFI, so iOS and macOS share everything but that one call.
///
/// Every callback drains its own autorelease pool: they run on threads the media stack creates, which have none,
/// and Core ML autoreleases per prediction (measured: ~54 KB per frame never freed without the pool).
///
/// Contract (mirrors moq-audio's `external` module): one packet per 80 ms frame. An encoder stream takes exactly
/// 1920 mono samples at 24 kHz per call and writes one packet; a decoder stream writes exactly 1920 samples per
/// packet, or per lost packet via `recover`. Streams are called one at a time each, from any thread.
public final class LiquidCallCodec: @unchecked Sendable {
    public let models: LiquidModels
    public let silence: [Int32]
    /// Packets carry the previous frame too, so one lost packet is rebuilt from the next (3.4 kbps instead of 1.7).
    public let redundancy: Bool

    public init(models: LiquidModels, redundancy: Bool = true) throws {
        self.models = models
        self.silence = try models.silenceCodes()
        self.redundancy = redundancy
    }

    /// Load the models in `directory` (the Neural Engine compiles them on first use: seconds, then cached).
    public static func load(from directory: URL, redundancy: Bool = true) throws -> LiquidCallCodec {
        try LiquidCallCodec(models: LiquidModels(directory: directory), redundancy: redundancy)
    }

    /// The bitrate the catalog advertises, in bits per second.
    public var bitrate: UInt32 { UInt32((redundancy ? 2 : 1) * CodePacker.bytesPerFrame * 8 * 1000 / 80) }

    /// The C callback set for `media_liquid_register`. `context` stays valid while this object is alive: keep a
    /// strong reference for as long as the registration stands.
    public var callbacks: Callbacks {
        Callbacks(context: UnsafeRawPointer(Unmanaged.passUnretained(self).toOpaque()), bitrate: bitrate)
    }

    public struct Callbacks {
        public let context: UnsafeRawPointer
        public let bitrate: UInt32
        public let encoderNew: @convention(c) (UnsafeRawPointer?) -> UnsafeRawPointer? = { ctx in
            guard let ctx else { return nil }
            let codec = Unmanaged<LiquidCallCodec>.fromOpaque(ctx).takeUnretainedValue()
            guard let encoder = try? LiquidPacketEncoder(models: codec.models, redundancy: codec.redundancy)
            else { return nil }
            return UnsafeRawPointer(Unmanaged.passRetained(EncoderStream(encoder)).toOpaque())
        }
        public let encode: @convention(c) (
            UnsafeRawPointer?, UnsafePointer<Float>?, Int, UnsafeMutablePointer<UInt8>?, Int
        ) -> Int32 = { handle, pcm, count, out, capacity in
            autoreleasepool {
                guard let handle, let pcm, let out, count == LiquidCodec.frameSamples else { return -1 }
                let stream = Unmanaged<EncoderStream>.fromOpaque(handle).takeUnretainedValue()
                guard let packet = try? stream.encoder.encode(Array(UnsafeBufferPointer(start: pcm, count: count))),
                      packet.count <= capacity
                else { return -1 }
                packet.withUnsafeBufferPointer { out.update(from: $0.baseAddress!, count: packet.count) }
                return Int32(packet.count)
            }
        }
        public let encoderFree: @convention(c) (UnsafeRawPointer?) -> Void = { handle in
            guard let handle else { return }
            Unmanaged<EncoderStream>.fromOpaque(handle).release()
        }
        public let decoderNew: @convention(c) (UnsafeRawPointer?) -> UnsafeRawPointer? = { ctx in
            guard let ctx else { return nil }
            let codec = Unmanaged<LiquidCallCodec>.fromOpaque(ctx).takeUnretainedValue()
            guard let decoder = try? LiquidPacketDecoder(models: codec.models, silence: codec.silence) else { return nil }
            return UnsafeRawPointer(Unmanaged.passRetained(DecoderStream(decoder)).toOpaque())
        }
        public let decode: @convention(c) (
            UnsafeRawPointer?, UnsafePointer<UInt8>?, Int, UnsafeMutablePointer<Float>?, Int
        ) -> Int32 = { handle, packet, length, out, capacity in
            autoreleasepool {
                guard let handle, let packet, let out else { return -1 }
                let stream = Unmanaged<DecoderStream>.fromOpaque(handle).takeUnretainedValue()
                guard let pcm = try? stream.decoder.decode(Array(UnsafeBufferPointer(start: packet, count: length)))
                else { return -1 }
                return Callbacks.write(pcm, to: out, capacity: capacity)
            }
        }
        public let recover: @convention(c) (
            UnsafeRawPointer?, UnsafePointer<UInt8>?, Int, UInt32, UnsafeMutablePointer<Float>?, Int
        ) -> Int32 = { handle, next, length, framesToNext, out, capacity in
            autoreleasepool {
                guard let handle, let out else { return -1 }
                let stream = Unmanaged<DecoderStream>.fromOpaque(handle).takeUnretainedValue()
                let bytes = next.map { Array(UnsafeBufferPointer(start: $0, count: length)) }
                guard let pcm = try? stream.decoder.recover(next: bytes, framesToNext: Int(framesToNext)) else { return -1 }
                return Callbacks.write(pcm, to: out, capacity: capacity)
            }
        }
        public let decoderFree: @convention(c) (UnsafeRawPointer?) -> Void = { handle in
            guard let handle else { return }
            Unmanaged<DecoderStream>.fromOpaque(handle).release()
        }

        static func write(_ pcm: [Float], to out: UnsafeMutablePointer<Float>, capacity: Int) -> Int32 {
            guard pcm.count <= capacity else { return -1 }
            pcm.withUnsafeBufferPointer { out.update(from: $0.baseAddress!, count: pcm.count) }
            return Int32(pcm.count)
        }
    }

    final class EncoderStream {
        let encoder: LiquidPacketEncoder
        init(_ encoder: LiquidPacketEncoder) { self.encoder = encoder }
    }

    final class DecoderStream {
        let decoder: LiquidPacketDecoder
        init(_ decoder: LiquidPacketDecoder) { self.decoder = decoder }
    }
}
