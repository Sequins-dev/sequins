// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "SequinsData",
    platforms: [
        .macOS(.v15),
        .iOS(.v18)
    ],
    products: [
        .library(
            name: "SequinsData",
            targets: ["SequinsData"]
        ),
    ],
    dependencies: [
        .package(path: "../arrow-swift"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.6.0"),
        .package(url: "https://github.com/apple/swift-distributed-tracing.git", from: "1.1.0"),
    ],
    targets: [
        .binaryTarget(
            name: "SequinsFFI",
            path: "SequinsFFI.xcframework"
        ),
        .target(
            name: "SequinsData",
            dependencies: [
                "SequinsFFI",
                .product(name: "Arrow", package: "arrow-swift"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Tracing", package: "swift-distributed-tracing"),
            ],
            swiftSettings: [.swiftLanguageMode(.v5)],
            linkerSettings: [
                .linkedFramework("SystemConfiguration"),
                .linkedFramework("CoreFoundation"),
                .linkedFramework("Security"),
                .linkedLibrary("bz2")
            ]
        ),
        .testTarget(
            name: "SequinsDataTests",
            dependencies: ["SequinsData"],
            swiftSettings: [.swiftLanguageMode(.v5)]
        ),
    ]
)
