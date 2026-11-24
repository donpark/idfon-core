#if !canImport(CoreAI)
import Foundation
import TygerVoice

/// Stand-in where the CoreAI framework does not exist (the iOS Simulator
/// SDK). Nothing is ever loadable, so callers fall through to their other
/// turn tiers exactly as on an OS without the Core AI model downloaded.
@available(macOS 27.0, iOS 27.0, *)
public final class CoreAITurnModel: TurnModel {
    public enum LoadError: Error { case unavailable }

    public let usesFloat16 = false
    public let stepModelName = ""
    public let directory: URL

    public convenience init() throws { throw LoadError.unavailable }
    public init(directory: URL) throws { throw LoadError.unavailable }

    public static func present(_ url: URL) -> Bool { false }
    public static func loadable(in directory: URL) -> Bool { false }
    public static func studentStep(in directory: URL) -> Bool { false }
    public static func hasTraits(in directory: URL) -> Bool { false }

    public func reset() {}
    public func encode(_ window: [Float]) -> [[Float]] { [] }
    public func step(hu: [Float], ha: [Float], actDb: Double, prevState: Int) -> [Float] { [] }
}
#endif
