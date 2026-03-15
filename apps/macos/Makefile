.PHONY: build build-release test clean generate build-app run-app test-ui

# =============================================================================
# Swift Package Manager Commands (for unit tests and CI)
# =============================================================================
# Use xcrun to ensure we're using Xcode's Swift toolchain (not Swiftly)
# -DSPM_BUILD skips SwiftUI previews which don't work in SPM builds

build:
	xcrun swift build -Xswiftc -DSPM_BUILD

build-release:
	xcrun swift build -c release -Xswiftc -DSPM_BUILD

test:
	xcrun swift test -Xswiftc -DSPM_BUILD

clean:
	rm -rf .build
	rm -rf *.xcodeproj

# Default target
.DEFAULT_GOAL := build

# =============================================================================
# XcodeGen Commands (for macOS app builds)
# =============================================================================
# XcodeGen generates .xcodeproj from project.yml (plain-text configuration)
# This allows proper macOS app bundles with Info.plist, entitlements, etc.
# The generated .xcodeproj is gitignored - regenerate with `make generate`

generate:
	xcodegen generate

build-app: generate
	xcodebuild -scheme Sequins -configuration Debug build

build-app-release: generate
	xcodebuild -scheme Sequins -configuration Release build

run-app: build-app
	@APP_PATH=$$(xcodebuild -scheme Sequins -configuration Debug -showBuildSettings 2>/dev/null | grep -m1 "BUILT_PRODUCTS_DIR" | awk '{print $$3}'); \
	open "$$APP_PATH/Sequins.app"

# =============================================================================
# UI Tests (XCUITest)
# =============================================================================
# UI tests require a running application and CANNOT run via `swift test`.
# They must be run through xcodebuild with the generated project.

test-ui: generate
	xcodebuild -scheme Sequins -destination 'platform=macOS' test

# =============================================================================
# Full Test Suite
# =============================================================================
test-all: test test-ui
