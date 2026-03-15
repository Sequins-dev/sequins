// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "Sequins",
    platforms: [
        .macOS(.v15)
    ],
    products: [
        .executable(
            name: "Sequins",
            targets: ["Sequins"]
        )
    ],
    dependencies: [
        .package(path: "SequinsData")
    ],
    targets: [
        .executableTarget(
            name: "Sequins",
            dependencies: ["SequinsData"],
            path: "Sequins",
            exclude: ["Assets.xcassets"],
            swiftSettings: [.swiftLanguageMode(.v5)]
        )
    ]
)
