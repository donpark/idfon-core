// swift-tools-version: 5.9
import PackageDescription

// LiquidCodec (24 kHz neural speech codec, 80 ms frames) on Core ML, shared by the iOS and macOS apps.
let package = Package(
    name: "LiquidCodec",
    platforms: [.macOS("15.0"), .iOS("18.0")],
    products: [
        .library(name: "LiquidCodec", targets: ["LiquidCodec"]),
    ],
    targets: [
        .target(name: "LiquidCodec", path: "Sources/LiquidCodec"),
        .testTarget(name: "LiquidCodecTests", dependencies: ["LiquidCodec"], path: "Tests/LiquidCodecTests"),
    ]
)
