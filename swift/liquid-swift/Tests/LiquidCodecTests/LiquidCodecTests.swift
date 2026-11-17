import Foundation
import XCTest

@testable import LiquidCodec

final class LiquidCodecTests: XCTestCase {
    func testPackerRoundTripsEveryCodeValue() {
        for start in stride(from: Int32(0), to: 2016, by: 12) {
            let codes = (0..<12).map { min(2015, start + Int32($0)) }
            let packet = CodePacker.pack(codes)
            XCTAssertEqual(packet.count, 17)
            XCTAssertEqual(CodePacker.unpack(packet), codes)
        }
    }

    func testFsqLatentsRoundTripToCodes() {
        let codes: [Int32] = [0, 1, 7, 8, 55, 56, 335, 336, 1000, 2015, 42, 1234]
        var latents = [Float](repeating: 0, count: 48)
        latents.withUnsafeMutableBufferPointer { FSQ.latents(codes: codes, into: $0.baseAddress!) }
        // Latents are exact level centers in [-1, 1]; scaled back to pre-tanh space they re-quantize to the codes.
        let preTanh = (0..<48).map { i -> Float in
            let level = Double(FSQ.levels[i % 4])
            let half = Double(FSQ.levels[i % 4] / 2)
            let halfLevels = (level - 1) * (1 + 1e-3) / 2
            let offset = FSQ.levels[i % 4] % 2 == 0 ? 0.5 : 0.0
            let bounded = Double(latents[i]) * half
            return Float(atanh((bounded + offset) / halfLevels) - atanh(offset / halfLevels))
        }
        XCTAssertEqual(preTanh.withUnsafeBufferPointer { FSQ.codes(latents: $0.baseAddress!) }, codes)
    }

    /// The C callback set, driven the way crates/iroh-c-ffi/src/liquid.rs drives it. Needs the models:
    /// LIQUID_MODELS=<dir with the .mlmodelc files> swift test
    func testCallbacksRoundTripAndRecover() throws {
        guard let dir = ProcessInfo.processInfo.environment["LIQUID_MODELS"] else {
            throw XCTSkip("set LIQUID_MODELS to the models directory")
        }
        let codec = try LiquidCallCodec.load(from: URL(fileURLWithPath: dir))
        let cb = codec.callbacks
        XCTAssertEqual(cb.bitrate, 3400)
        let enc = try XCTUnwrap(cb.encoderNew(cb.context))
        let dec = try XCTUnwrap(cb.decoderNew(cb.context))
        defer {
            cb.encoderFree(enc)
            cb.decoderFree(dec)
        }
        var packets: [[UInt8]] = []
        for k in 0..<10 {
            let pcm = (0..<1920).map { n in Float(sin(Double(k * 1920 + n) * 2 * .pi * 220 / 24000) * 0.3) }
            var out = [UInt8](repeating: 0, count: 256)
            let n = pcm.withUnsafeBufferPointer { p in
                out.withUnsafeMutableBufferPointer { cb.encode(enc, p.baseAddress, 1920, $0.baseAddress, 256) }
            }
            XCTAssertEqual(n, 34)
            packets.append(Array(out.prefix(Int(n))))
        }
        var energy: Float = 0
        for (k, packet) in packets.enumerated() {
            var pcm = [Float](repeating: 0, count: 1920)
            let n: Int32
            if k == 5 {  // lost: rebuilt from packet 6's redundant copy
                n = packets[6].withUnsafeBufferPointer { p in
                    pcm.withUnsafeMutableBufferPointer { cb.recover(dec, p.baseAddress, p.count, 1, $0.baseAddress, 1920) }
                }
            } else {
                n = packet.withUnsafeBufferPointer { p in
                    pcm.withUnsafeMutableBufferPointer { cb.decode(dec, p.baseAddress, p.count, $0.baseAddress, 1920) }
                }
            }
            XCTAssertEqual(n, 1920)
            if k >= 3 { energy += pcm.reduce(0) { $0 + $1 * $1 } }
        }
        XCTAssertGreaterThan(energy, 10, "a 220 Hz tone should decode to audible output")
    }
}

final class LiquidModelStoreTests: XCTestCase {
    /// Installs from the live edge pack into a temp dir, then loads it. Network + 150 MB: opt in with
    /// LIQUID_PACK_TEST=1 (LIQUID_PACK_URL overrides the pack).
    func testInstallFromPackThenServiceLoads() async throws {
        guard ProcessInfo.processInfo.environment["LIQUID_PACK_TEST"] == "1" else {
            throw XCTSkip("set LIQUID_PACK_TEST=1 to download the pack")
        }
        let base = ProcessInfo.processInfo.environment["LIQUID_PACK_URL"].flatMap(URL.init(string:))
            ?? LiquidModelStore.defaultPackURL
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent("liquid-pack-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: dir) }
        let states = StateLog()
        let service = LiquidCodecService(base: base, directory: dir, onReady: { _ in }, onState: { states.append($0) })
        let codec = try await service.ready()
        XCTAssertTrue(LiquidModelStore.isInstalled(at: dir))
        XCTAssertNotNil(LiquidModelStore.installedManifest(at: dir))
        XCTAssertEqual(codec.bitrate, 3400)
        let seen = states.all
        XCTAssertTrue(seen.contains { if case .downloading = $0 { return true } else { return false } })
        XCTAssertEqual(seen.last, .ready)
        // A second install is a no-op (current manifest, sizes match).
        let t0 = Date()
        try await LiquidModelStore.install(from: base, to: dir)
        XCTAssertLessThan(Date().timeIntervalSince(t0), 5)
    }
}

final class StateLog: @unchecked Sendable {
    private let lock = NSLock()
    private var states: [LiquidCodecService.State] = []
    func append(_ s: LiquidCodecService.State) { lock.lock(); states.append(s); lock.unlock() }
    var all: [LiquidCodecService.State] { lock.lock(); defer { lock.unlock() }; return states }
}
