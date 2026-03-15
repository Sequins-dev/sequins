.PHONY: build test clean xcframework run swift-build swift-test swift-generate swift-build-app swift-run-app icons

SWIFT_PACKAGE_PATH := apps/macos/SequinsData

# Standard Rust commands
build:
	cargo build

test:
	cargo test

clean:
	cargo clean
	rm -rf target/SequinsFFI.xcframework target/xcframework target/frameworks

xcframework:
	@echo "Building xcframework..."
	@rm -rf target/SequinsFFI.xcframework target/xcframework target/frameworks
	@rm -rf $(SWIFT_PACKAGE_PATH)/SequinsFFI.xcframework
	MACOSX_DEPLOYMENT_TARGET=14.0 cargo build \
		--package sequins-ffi \
		--target aarch64-apple-darwin \
		--target x86_64-apple-darwin
	@mkdir -p target/xcframework/libs
	lipo -create \
		target/aarch64-apple-darwin/debug/libsequins_ffi.a \
		target/x86_64-apple-darwin/debug/libsequins_ffi.a \
		-output target/xcframework/libs/libsequins_ffi.a
	xcrun xcodebuild -create-xcframework \
		-library target/xcframework/libs/libsequins_ffi.a \
		-headers crates/sequins-ffi/include \
		-output target/SequinsFFI.xcframework
	@cp -R target/SequinsFFI.xcframework $(SWIFT_PACKAGE_PATH)/
	@echo "XCFramework built and installed to $(SWIFT_PACKAGE_PATH)"

swift-build:
	$(MAKE) -C apps/macos build

swift-test:
	$(MAKE) -C apps/macos test

ICON_SVG    := apps/macos/Sequins/Assets.xcassets/AppIcon.appiconset/logo.svg
ICON_DIR    := apps/macos/Sequins/Assets.xcassets/AppIcon.appiconset
MENUBAR_DIR := apps/macos/Sequins/Assets.xcassets/MenuBarIcon.imageset

icons:
	rsvg-convert -w 16   -h 16   $(ICON_SVG) -o $(ICON_DIR)/icon_16x16.png
	rsvg-convert -w 32   -h 32   $(ICON_SVG) -o $(ICON_DIR)/icon_32x32.png
	rsvg-convert -w 64   -h 64   $(ICON_SVG) -o $(ICON_DIR)/icon_64x64.png
	rsvg-convert -w 128  -h 128  $(ICON_SVG) -o $(ICON_DIR)/icon_128x128.png
	rsvg-convert -w 256  -h 256  $(ICON_SVG) -o $(ICON_DIR)/icon_256x256.png
	rsvg-convert -w 512  -h 512  $(ICON_SVG) -o $(ICON_DIR)/icon_512x512.png
	rsvg-convert -w 1024 -h 1024 $(ICON_SVG) -o $(ICON_DIR)/icon_1024x1024.png
	rsvg-convert -w 18   -h 18   $(ICON_SVG) -o $(MENUBAR_DIR)/menubar_icon.png
	rsvg-convert -w 36   -h 36   $(ICON_SVG) -o $(MENUBAR_DIR)/menubar_icon@2x.png

swift-generate: icons
	$(MAKE) -C apps/macos generate

swift-build-app: xcframework
	$(MAKE) -C apps/macos build-app

swift-run-app: xcframework
	$(MAKE) -C apps/macos run-app
