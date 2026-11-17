import Accelerate
@preconcurrency import CoreML
import Foundation

/// Streaming LiquidCodec (24 kHz mono speech, 12.5 Hz frames, 12 FSQ codes per frame, ~1.65 kbps).
///
/// Encoder: Core ML, sees the last `encoderWindow` frames of audio plus 20 ms look-ahead, returns the newest frame's
/// FSQ latents; FSQ rounding runs here. Decoder: Core ML, sees the last `decoderContext` frames of codes, returns the
/// newest frame's 6 hidden rows; lin + iSTFT + overlap-add run here in fp32. Both models are fixed-shape and
/// stateless, so they run on the Neural Engine.
public enum LiquidCodec {
    public static let sampleRate = 24_000
    public static let frameSamples = 1920
    public static let lookahead = 480
    public static let codebooks = 12
    public static let encoderWindow = 8
    public static let decoderContext = 24
    static let hop = 320
    static let nFFT = 1280
    static let bins = nFFT / 2 + 1  // 641
    static let hidden = 512
}

// MARK: - FSQ

enum FSQ {
    static let levels: [Int32] = [8, 7, 6, 6]
    static let basis: [Int32] = [1, 8, 56, 336]
    static let codebookSize: Int32 = 8 * 7 * 6 * 6  // 2016

    /// 48 latents -> 12 codes; == LiquidCodecQuantizer.encode.
    static func codes(latents: UnsafePointer<Float>) -> [Int32] {
        var out = [Int32](repeating: 0, count: 12)
        for q in 0..<12 {
            var index: Int32 = 0
            for d in 0..<4 {
                let l = Double(levels[d])
                let halfLevels = (l - 1) * (1 + 1e-3) / 2
                let offset = levels[d] % 2 == 0 ? 0.5 : 0.0
                let shift = atanh(offset / halfLevels)
                let bounded = tanh(Double(latents[q * 4 + d]) + shift) * halfLevels - offset
                let level = Int32(bounded.rounded(.toNearestOrEven)) + levels[d] / 2
                index += level * basis[d]
            }
            out[q] = index
        }
        return out
    }

    /// 12 codes -> 48 latents in [-1, 1].
    static func latents(codes: [Int32], into out: UnsafeMutablePointer<Float>) {
        for q in 0..<12 {
            for d in 0..<4 {
                let half = levels[d] / 2
                out[q * 4 + d] = Float((codes[q] / basis[d]) % levels[d] - half) / Float(half)
            }
        }
    }
}

// MARK: - Bit packing (12 x 11 bits = 17 bytes per frame)

public enum CodePacker {
    public static let bytesPerFrame = 17

    public static func pack(_ codes: [Int32]) -> [UInt8] {
        var out = [UInt8](repeating: 0, count: bytesPerFrame)
        var bit = 0
        for c in codes {
            for b in 0..<11 where (c >> b) & 1 == 1 {
                out[(bit + b) / 8] |= 1 << ((bit + b) % 8)
            }
            bit += 11
        }
        return out
    }

    public static func unpack(_ bytes: [UInt8]) -> [Int32] {
        var out = [Int32](repeating: 0, count: 12)
        var bit = 0
        for q in 0..<12 {
            var c: Int32 = 0
            for b in 0..<11 where (bytes[(bit + b) / 8] >> ((bit + b) % 8)) & 1 == 1 {
                c |= 1 << b
            }
            out[q] = c
            bit += 11
        }
        return out
    }
}

// MARK: - Models

public struct LiquidModels: @unchecked Sendable {
    let encoder: MLModel
    let decoder: MLModel
    let linWeight: [Float]  // 1282 x 512 row-major
    let linBias: [Float]  // 1282

    public init(directory: URL, computeUnits: MLComputeUnits = .cpuAndNeuralEngine) throws {
        let config = MLModelConfiguration()
        config.computeUnits = computeUnits
        encoder = try MLModel(
            contentsOf: directory.appendingPathComponent("LiquidEncoder-W\(LiquidCodec.encoderWindow).mlmodelc"),
            configuration: config)
        decoder = try MLModel(
            contentsOf: directory.appendingPathComponent("LiquidDecoder-C\(LiquidCodec.decoderContext).mlmodelc"),
            configuration: config)
        let data = try Data(contentsOf: directory.appendingPathComponent("synth_head.f32.bin"))
        let rows = 2 * LiquidCodec.bins
        let floats = data.withUnsafeBytes { Array($0.bindMemory(to: Float.self)) }
        precondition(floats.count == rows * LiquidCodec.hidden + rows, "synth_head.f32.bin has the wrong size")
        linWeight = Array(floats[0..<(rows * LiquidCodec.hidden)])
        linBias = Array(floats[(rows * LiquidCodec.hidden)...])
    }
}

// MARK: - Streaming encoder

public final class StreamingEncoder {
    private let model: MLModel
    private let input: MLMultiArray
    private let windowSamples = LiquidCodec.encoderWindow * LiquidCodec.frameSamples + 2 * LiquidCodec.lookahead
    /// Samples not yet consumed; starts with a silent history so the first frame has a full window.
    private var buffer: [Float]
    public private(set) var lastPredictSeconds: Double = 0

    /// `extraDelay` samples of extra silent history shift every frame that much later in the input; with
    /// `lookahead` (480) a push of exactly one frame returns exactly one frame of codes.
    public init(models: LiquidModels, extraDelay: Int = 0) throws {
        model = models.encoder
        input = try MLMultiArray(shape: [1, NSNumber(value: windowSamples)], dataType: .float32)
        buffer = [Float](
            repeating: 0, count: windowSamples - LiquidCodec.frameSamples - LiquidCodec.lookahead + extraDelay)
    }

    /// Feed 24 kHz mono samples; returns the codes of every frame whose 20 ms look-ahead has now arrived.
    public func push(_ samples: [Float]) throws -> [[Int32]] {
        buffer.append(contentsOf: samples)
        var frames: [[Int32]] = []
        while buffer.count >= windowSamples {
            frames.append(try encodeWindow(buffer, from: 0))
            buffer.removeFirst(LiquidCodec.frameSamples)
        }
        return frames
    }

    /// Pads the tail with silence and flushes the remaining (partial) frame.
    public func finish() throws -> [[Int32]] {
        let pending = buffer.count - (windowSamples - LiquidCodec.frameSamples - LiquidCodec.lookahead)
        guard pending > 0 else { return [] }
        let frames = (pending + LiquidCodec.frameSamples - 1) / LiquidCodec.frameSamples
        let target = windowSamples + (frames - 1) * LiquidCodec.frameSamples
        return try push([Float](repeating: 0, count: target - buffer.count))
    }

    private func encodeWindow(_ samples: [Float], from start: Int) throws -> [Int32] {
        let dst = input.dataPointer.bindMemory(to: Float.self, capacity: windowSamples)
        samples.withUnsafeBufferPointer { dst.update(from: $0.baseAddress! + start, count: windowSamples) }
        let t0 = CFAbsoluteTimeGetCurrent()
        let out = try model.prediction(from: MLDictionaryFeatureProvider(dictionary: ["audio": input]))
        lastPredictSeconds = CFAbsoluteTimeGetCurrent() - t0
        let latents = out.featureValue(for: "latents")!.multiArrayValue!
        return try Self.floats(latents, count: 48).withUnsafeBufferPointer { FSQ.codes(latents: $0.baseAddress!) }
    }

    /// Reads an output multiarray as fp32 in logical (row-major) order, whatever its storage type and strides.
    static func floats(_ array: MLMultiArray, count: Int) throws -> [Float] {
        precondition(array.count == count)
        let shape = array.shape.map(\.intValue)
        let strides = array.strides.map(\.intValue)
        var out = [Float](repeating: 0, count: count)
        func offset(_ i: Int) -> Int {
            var rem = i, off = 0
            for d in stride(from: shape.count - 1, through: 0, by: -1) {
                off += (rem % shape[d]) * strides[d]
                rem /= shape[d]
            }
            return off
        }
        switch array.dataType {
        case .float32:
            let p = array.dataPointer.bindMemory(to: Float.self, capacity: 1)
            for i in 0..<count { out[i] = p[offset(i)] }
        case .float16:
            let p = array.dataPointer.bindMemory(to: Float16.self, capacity: 1)
            for i in 0..<count { out[i] = Float(p[offset(i)]) }
        default:
            for i in 0..<count { out[i] = array[i].floatValue }
        }
        return out
    }
}

// MARK: - Streaming decoder

public final class StreamingDecoder {
    private let model: MLModel
    private let input: MLMultiArray
    private let synth: Synthesizer
    /// The last `decoderContext` frames of latents, oldest first; starts with the codes of silence.
    private var context: [Float]
    public private(set) var lastPredictSeconds: Double = 0

    /// `trimStart: false` keeps the first 480 samples, so every push returns exactly one frame (1920 samples) and
    /// the output runs 480 samples late.
    public init(models: LiquidModels, silence: [Int32], trimStart: Bool = true) throws {
        model = models.decoder
        let c = LiquidCodec.decoderContext
        input = try MLMultiArray(shape: [1, NSNumber(value: c), 48], dataType: .float32)
        synth = Synthesizer(weight: models.linWeight, bias: models.linBias, trimStart: trimStart)
        var one = [Float](repeating: 0, count: 48)
        one.withUnsafeMutableBufferPointer { FSQ.latents(codes: silence, into: $0.baseAddress!) }
        context = Array((0..<c).map { _ in one }.joined())
    }

    /// The codes of the most recent frame pushed.
    public private(set) var lastCodes: [Int32] = []

    /// Decode one frame of codes; returns the 24 kHz samples that are now final (1920 per frame in steady state).
    public func push(_ codes: [Int32]) throws -> [Float] {
        lastCodes = codes
        context.removeFirst(48)
        var latents = [Float](repeating: 0, count: 48)
        latents.withUnsafeMutableBufferPointer { FSQ.latents(codes: codes, into: $0.baseAddress!) }
        context.append(contentsOf: latents)
        let dst = input.dataPointer.bindMemory(to: Float.self, capacity: context.count)
        context.withUnsafeBufferPointer { dst.update(from: $0.baseAddress!, count: context.count) }
        let t0 = CFAbsoluteTimeGetCurrent()
        let out = try model.prediction(from: MLDictionaryFeatureProvider(dictionary: ["latents": input]))
        lastPredictSeconds = CFAbsoluteTimeGetCurrent() - t0
        let hidden = try StreamingEncoder.floats(out.featureValue(for: "hidden")!.multiArrayValue!, count: 6 * 512)
        return synth.push(hidden: hidden, finalize: false)
    }

    /// Flushes the held-back overlap-add tail at the end of a stream.
    public func finish() -> [Float] { synth.flush() }
}

/// lin -> polar -> irfft -> Hann window -> streaming overlap-add (== LiquidCodecModel.decode with a cache).
final class Synthesizer {
    static let trim = 480
    static let tail = 960
    private let weight: [Float]
    private let bias: [Float]
    private let window: [Float]
    private let windowSquared: [Float]
    private let dft: vDSP_DFT_Setup
    private var tailAudio = [Float](repeating: 0, count: tail)
    private var tailEnvelope = [Float](repeating: 0, count: tail)
    private var started = false
    private var scale: Float = 1

    init(weight: [Float], bias: [Float], trimStart: Bool = true) {
        started = !trimStart
        self.weight = weight
        self.bias = bias
        let n = LiquidCodec.nFFT
        window = (0..<n).map { 0.5 - 0.5 * cos(2 * Float.pi * Float($0) / Float(n)) }  // periodic Hann
        windowSquared = window.map { $0 * $0 }
        dft = vDSP_DFT_zrop_CreateSetup(nil, vDSP_Length(n), .INVERSE)!
        // Calibrate vDSP's inverse scaling against irfft(norm="backward"): DC = 1 must give 1/N everywhere.
        var re = [Float](repeating: 0, count: n / 2), im = re
        re[0] = 1
        scale = (1 / Float(n)) / irfft(re: re, im: im)[0]
    }

    deinit { vDSP_DFT_DestroySetup(dft) }

    private func irfft(re: [Float], im: [Float]) -> [Float] {
        let n = LiquidCodec.nFFT
        var outRe = [Float](repeating: 0, count: n / 2), outIm = outRe
        vDSP_DFT_Execute(dft, re, im, &outRe, &outIm)
        var out = [Float](repeating: 0, count: n)
        for i in 0..<(n / 2) {  // even/odd interleave
            out[2 * i] = outRe[i]
            out[2 * i + 1] = outIm[i]
        }
        return out
    }

    func push(hidden: [Float], finalize: Bool) -> [Float] {
        let rows = 6, n = LiquidCodec.nFFT, bins = LiquidCodec.bins, hop = LiquidCodec.hop
        // y = hidden (6x512) * W^T (512x1282) + b
        var y = [Float](repeating: 0, count: rows * 2 * bins)
        for r in 0..<rows { for c in 0..<(2 * bins) { y[r * 2 * bins + c] = bias[c] } }
        cblas_sgemm(
            CblasRowMajor, CblasNoTrans, CblasTrans, Int32(rows), Int32(2 * bins), Int32(LiquidCodec.hidden), 1,
            hidden, Int32(LiquidCodec.hidden), weight, Int32(LiquidCodec.hidden), 1, &y, Int32(2 * bins))
        let length = (rows - 1) * hop + n
        var audio = [Float](repeating: 0, count: length)
        var envelope = [Float](repeating: 0, count: length)
        var re = [Float](repeating: 0, count: n / 2), im = re
        for r in 0..<rows {
            let base = r * 2 * bins
            func bin(_ k: Int) -> (Float, Float) {
                let mag = exp(y[base + k])
                let angle = y[base + bins + k]
                return (mag * cos(angle), mag * sin(angle))
            }
            // vDSP packed format: re[0] = DC, im[0] = Nyquist (both real parts).
            re[0] = bin(0).0
            im[0] = bin(n / 2).0
            for k in 1..<(n / 2) { (re[k], im[k]) = bin(k) }
            let frame = irfft(re: re, im: im)
            for j in 0..<n {
                audio[r * hop + j] += frame[j] * scale * window[j]
                envelope[r * hop + j] += windowSquared[j]
            }
        }
        return finish(audio: &audio, envelope: &envelope, finalize: finalize)
    }

    func flush() -> [Float] {
        guard started else { return [] }
        var audio = [Float](repeating: 0, count: Self.tail)
        var envelope = audio
        return finish(audio: &audio, envelope: &envelope, finalize: true)
    }

    private func finish(audio: inout [Float], envelope: inout [Float], finalize: Bool) -> [Float] {
        for i in 0..<Self.tail {
            audio[i] += tailAudio[i]
            envelope[i] += tailEnvelope[i]
        }
        let lo = started ? 0 : Self.trim
        started = true
        var hi = audio.count
        if finalize {
            hi -= Self.trim
            tailAudio = [Float](repeating: 0, count: Self.tail)
            tailEnvelope = tailAudio
        } else {
            tailAudio = Array(audio[(hi - Self.tail)..<hi])
            tailEnvelope = Array(envelope[(hi - Self.tail)..<hi])
            hi -= Self.tail
        }
        guard hi > lo else { return [] }
        return (lo..<hi).map { audio[$0] / envelope[$0] }
    }
}

// MARK: - Convenience

public extension LiquidModels {
    /// The codes the encoder gives digital silence; the decoder's initial context.
    func silenceCodes() throws -> [Int32] {
        let encoder = try StreamingEncoder(models: self)
        let codes = try encoder.push([Float](repeating: 0, count: LiquidCodec.frameSamples + LiquidCodec.lookahead)).first!
        _ = try StreamingDecoder(models: self, silence: codes).push(codes)  // warm the decoder too (first call is slow)
        return codes
    }
}

// MARK: - File round trip

public struct RoundTripResult {
    public var audio: [Float]
    public var encodeMs: [Double]
    public var decodeMs: [Double]
    public var bytes: Int
}

public extension LiquidModels {
    /// Streams `audio` (24 kHz mono) through encoder -> 17-byte packets -> decoder in 10 ms chunks, as a capture
    /// callback would, timing each model call.
    func roundTrip(_ audio: [Float], silence: [Int32]) throws -> RoundTripResult {
        let encoder = try StreamingEncoder(models: self)
        let decoder = try StreamingDecoder(models: self, silence: silence)
        var result = RoundTripResult(audio: [], encodeMs: [], decodeMs: [], bytes: 0)
        func send(_ frames: [[Int32]]) throws {
            for codes in frames {
                result.encodeMs.append(encoder.lastPredictSeconds * 1000)
                let packet = CodePacker.pack(codes)
                result.bytes += packet.count
                result.audio += try decoder.push(CodePacker.unpack(packet))
                result.decodeMs.append(decoder.lastPredictSeconds * 1000)
            }
        }
        var i = 0
        while i < audio.count {
            let j = min(audio.count, i + 240)
            try send(encoder.push(Array(audio[i..<j])))
            i = j
        }
        try send(encoder.finish())
        result.audio += decoder.finish()
        result.audio = Array(result.audio.prefix(audio.count))
        return result
    }
}


// MARK: - Packets (one 80 ms frame per packet, for a transport)

/// Packet format: 17 bytes of the frame's codes, optionally followed by 17 bytes of the previous frame's codes
/// (redundancy, so a receiver one frame behind can rebuild a single lost packet).
public final class LiquidPacketEncoder {
    private let encoder: StreamingEncoder
    private let redundancy: Bool
    private var previous: [UInt8]?
    public var lastPredictSeconds: Double { encoder.lastPredictSeconds }

    public init(models: LiquidModels, redundancy: Bool) throws {
        encoder = try StreamingEncoder(models: models, extraDelay: LiquidCodec.lookahead)
        self.redundancy = redundancy
    }

    /// Exactly one frame (1920 samples, 24 kHz mono) in, one packet out. Packets code the input 480 samples late.
    public func encode(_ pcm: [Float]) throws -> [UInt8] {
        precondition(pcm.count == LiquidCodec.frameSamples)
        guard let codes = try encoder.push(pcm).first else { throw LiquidError.noFrame }
        let packet = CodePacker.pack(codes)
        defer { previous = packet }
        guard redundancy else { return packet }
        return packet + (previous ?? packet)
    }

    public var bitrate: Int { (redundancy ? 2 : 1) * CodePacker.bytesPerFrame * 8 * 1000 / 80 }
}

public final class LiquidPacketDecoder {
    private let decoder: StreamingDecoder
    private let silence: [Int32]
    public var lastPredictSeconds: Double { decoder.lastPredictSeconds }

    public init(models: LiquidModels, silence: [Int32]) throws {
        decoder = try StreamingDecoder(models: models, silence: silence, trimStart: false)
        self.silence = silence
    }

    /// One packet in, exactly one frame (1920 samples) out, 480 samples late.
    public func decode(_ packet: [UInt8]) throws -> [Float] {
        guard packet.count >= CodePacker.bytesPerFrame else { throw LiquidError.badPacket(packet.count) }
        return try decoder.push(CodePacker.unpack(Array(packet.prefix(CodePacker.bytesPerFrame))))
    }

    /// A frame for a lost packet: the redundant copy `next` carries when it is the very next packet, else the
    /// previous frame's codes again (or silence before any).
    public func recover(next: [UInt8]?, framesToNext: Int) throws -> [Float] {
        let n = CodePacker.bytesPerFrame
        if framesToNext == 1, let next, next.count >= 2 * n {
            return try decoder.push(CodePacker.unpack(Array(next[n..<(2 * n)])))
        }
        return try decoder.push(decoder.lastCodes.isEmpty ? silence : decoder.lastCodes)
    }
}

public enum LiquidError: Error {
    case noFrame
    case badPacket(Int)
}
