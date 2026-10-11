// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "SequinsOtel",
    platforms: [
        .macOS(.v13),
        .iOS(.v16),
    ],
    products: [
        .library(name: "SequinsOtel", targets: ["SequinsOtel"]),
    ],
    dependencies: [
        .package(url: "https://github.com/swift-otel/swift-otel.git", from: "0.9.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.6.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", from: "2.5.0"),
    ],
    targets: [
        .target(
            name: "SequinsOtel",
            dependencies: [
                .product(name: "OTel", package: "swift-otel"),
                .product(name: "OTLPHTTPExporter", package: "swift-otel"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Metrics", package: "swift-metrics"),
            ]
        ),
        .testTarget(
            name: "SequinsOtelTests",
            dependencies: ["SequinsOtel"]
        ),
    ]
)
