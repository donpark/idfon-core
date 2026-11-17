import Foundation

/// The codec's lifecycle in an app, shared by iOS and macOS: absent → downloading → loading → ready (or failed).
///
/// Policy (idfon-home/docs/liquidcodec.md): the codec is made ready as soon as it is *needed* (a contact uses it,
/// an incoming call carries it, or it is the default), not when a call starts; a call that finds it still on the
/// way waits for it (`ready()`, no timeout). Only a failure (download, verification, Core ML load) falls back.
/// Once loaded, `onReady` hands the codec to the app, which registers its callbacks with the media stack.
public actor LiquidCodecService {
    public enum State: Sendable, Equatable {
        case absent
        case downloading(Double)
        case loading
        case ready
        case failed(String)
    }

    public private(set) var state: State
    private var codec: LiquidCallCodec?
    private var task: Task<LiquidCallCodec, Error>?
    private let base: URL
    private let directory: URL
    private let onReady: @Sendable (LiquidCallCodec) async -> Void
    private let onState: @Sendable (State) -> Void

    /// - Parameters:
    ///   - onReady: called once per successful load (register the callbacks here; keep `codec` alive).
    ///   - onState: every state change, for UI; may be called from any task.
    public init(
        base: URL = LiquidModelStore.defaultPackURL, directory: URL = LiquidModelStore.defaultDirectory,
        onReady: @escaping @Sendable (LiquidCallCodec) async -> Void,
        onState: @escaping @Sendable (State) -> Void = { _ in }
    ) {
        self.base = base
        self.directory = directory
        self.onReady = onReady
        self.onState = onState
        self.state = .absent
    }

    /// Start making the codec ready (download if needed, then load) without waiting. Idempotent; a failed attempt
    /// is retried by the next call.
    public func prepare() {
        _ = start()
    }

    /// Wait until the codec is ready, starting the work if nobody has. Throws only on a real failure.
    @discardableResult
    public func ready() async throws -> LiquidCallCodec {
        try await start().value
    }

    public var isReady: Bool { codec != nil }

    private func start() -> Task<LiquidCallCodec, Error> {
        if let task { return task }
        let base = base, directory = directory
        let task = Task<LiquidCallCodec, Error> {
            do {
                if !LiquidModelStore.isInstalled(at: directory) {
                    set(.downloading(0))
                    try await LiquidModelStore.install(from: base, to: directory) { fraction in
                        Task { await self.set(.downloading(fraction)) }
                    }
                } else {
                    // Installed: load first (calls must not wait on the network), then refresh in the background.
                    Task.detached(priority: .background) { try? await LiquidModelStore.install(from: base, to: directory) }
                }
                set(.loading)
                let loaded = try await Task.detached(priority: .userInitiated) {
                    try LiquidCallCodec.load(from: directory)
                }.value
                codec = loaded
                await onReady(loaded)
                set(.ready)
                return loaded
            } catch {
                set(.failed(error.localizedDescription))
                self.task = nil  // let the next prepare/ready retry
                throw error
            }
        }
        self.task = task
        return task
    }

    private func set(_ new: State) {
        // Progress reports land out of order across tasks; never move backwards within a download.
        if case .downloading(let old) = state, case .downloading(let next) = new, next < old { return }
        if state == .ready, case .downloading = new { return }
        state = new
        onState(new)
    }
}
