import CryptoKit
import Foundation

/// Where the models live, whether they are complete, and how they get there: the content-addressed pack on the
/// idfon edge (`<base>/pack.json` plus `<base>/sha256/<hash>`, the PhononPack format). One place for the file list, so install and load agree.
public enum LiquidModelStore {
    /// The models `LiquidModels` loads; everything else in the pack (license, notice) is installed alongside.
    public static let files = ["LiquidEncoder-W8.mlmodelc", "LiquidDecoder-C24.mlmodelc", "synth_head.f32.bin"]

    /// The BunnyCDN-fronted pack (`https://idfon.net/models/liquidcodec/` is the origin).
    public static let defaultPackURL = URL(string: "https://files.idfon.net/models/liquidcodec/")!

    /// Application Support/LiquidCodec.
    public static var defaultDirectory: URL {
        FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("LiquidCodec", isDirectory: true)
    }

    public static func isInstalled(at directory: URL = defaultDirectory) -> Bool {
        files.allSatisfy { FileManager.default.fileExists(atPath: directory.appendingPathComponent($0).path) }
    }

    public struct Manifest: Codable, Sendable {
        public struct File: Codable, Sendable {
            public let path: String
            public let size: Int
            public let sha256: String
        }
        public let built: Int64
        public let files: [File]
        public var totalBytes: Int { files.reduce(0) { $0 + $1.size } }
    }

    public struct Failure: LocalizedError {
        public let message: String
        public var errorDescription: String? { message }
    }

    /// Install (or update) the pack `base` serves at `directory`. Already current → returns at once. Otherwise each
    /// file is fetched by hash, its size and SHA-256 verified, and the finished tree swapped in whole, so a crash or
    /// a failed fetch never leaves a half pack where `LiquidModels` would load it. Offline with a complete install
    /// is not an error. `progress` gets the fraction of bytes in hand (0...1).
    public static func install(
        from base: URL = defaultPackURL, to directory: URL = defaultDirectory,
        progress: @Sendable (Double) -> Void = { _ in }
    ) async throws {
        let fm = FileManager.default
        let want: Manifest
        do {
            let (data, response) = try await URLSession.shared.data(from: base.appendingPathComponent("pack.json"))
            try check(response, "pack.json")
            want = try JSONDecoder().decode(Manifest.self, from: data)
        } catch {
            if isInstalled(at: directory) { return }
            throw error
        }
        if installedManifest(at: directory)?.built == want.built, sizesMatch(want, at: directory) {
            progress(1)
            return
        }

        let staging = directory.deletingLastPathComponent()
            .appendingPathComponent("\(directory.lastPathComponent).partial-\(want.built)", isDirectory: true)
        try fm.createDirectory(at: staging, withIntermediateDirectories: true)
        var done = 0
        for file in want.files {
            let dest = staging.appendingPathComponent(file.path)
            // A staged file from an interrupted attempt counts when it verifies.
            if !(try matches(dest, file)) {
                let (tmp, response) = try await URLSession.shared.download(
                    from: base.appendingPathComponent("sha256/\(file.sha256)"))
                try check(response, file.path)
                try fm.createDirectory(at: dest.deletingLastPathComponent(), withIntermediateDirectories: true)
                try? fm.removeItem(at: dest)
                try fm.moveItem(at: tmp, to: dest)
                guard try matches(dest, file) else {
                    try? fm.removeItem(at: dest)
                    throw Failure(message: "\(file.path): size or SHA-256 mismatch")
                }
            }
            done += file.size
            progress(Double(done) / Double(max(want.totalBytes, 1)))
        }
        try JSONEncoder().encode(want).write(to: staging.appendingPathComponent("pack.json"))

        // Swap: the old tree (if any) moves aside first, so `directory` is always either complete or absent.
        let retired = directory.deletingLastPathComponent()
            .appendingPathComponent("\(directory.lastPathComponent).old-\(UUID().uuidString)", isDirectory: true)
        if fm.fileExists(atPath: directory.path) { try fm.moveItem(at: directory, to: retired) }
        try fm.moveItem(at: staging, to: directory)
        try? fm.removeItem(at: retired)
    }

    public static func installedManifest(at directory: URL = defaultDirectory) -> Manifest? {
        guard let data = try? Data(contentsOf: directory.appendingPathComponent("pack.json")) else { return nil }
        return try? JSONDecoder().decode(Manifest.self, from: data)
    }

    private static func sizesMatch(_ manifest: Manifest, at directory: URL) -> Bool {
        manifest.files.allSatisfy { file in
            let attrs = try? FileManager.default.attributesOfItem(atPath: directory.appendingPathComponent(file.path).path)
            return (attrs?[.size] as? Int) == file.size
        }
    }

    private static func matches(_ url: URL, _ file: Manifest.File) throws -> Bool {
        guard let attrs = try? FileManager.default.attributesOfItem(atPath: url.path),
              (attrs[.size] as? Int) == file.size
        else { return false }
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }
        var hasher = SHA256()
        while let chunk = try handle.read(upToCount: 1 << 20), !chunk.isEmpty { hasher.update(data: chunk) }
        return hasher.finalize().map { String(format: "%02x", $0) }.joined() == file.sha256
    }

    private static func check(_ response: URLResponse, _ what: String) throws {
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let code = (response as? HTTPURLResponse)?.statusCode ?? -1
            throw Failure(message: "\(what): HTTP \(code)")
        }
    }
}
