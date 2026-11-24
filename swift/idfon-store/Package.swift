// swift-tools-version: 5.9
import PackageDescription

// IdfonStore: the apps' on-device message log in SQLite (system SQLite, no
// dependencies), shared by the iOS and macOS apps.
let package = Package(
    name: "IdfonStore",
    platforms: [.macOS("15.0"), .iOS("18.0")],
    products: [
        .library(name: "IdfonStore", targets: ["IdfonStore"]),
    ],
    targets: [
        .target(name: "IdfonStore", path: "Sources/IdfonStore"),
        .testTarget(name: "IdfonStoreTests", dependencies: ["IdfonStore"], path: "Tests/IdfonStoreTests"),
    ]
)
