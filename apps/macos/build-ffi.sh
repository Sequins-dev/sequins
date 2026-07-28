#!/usr/bin/env bash
#
# Rebuild the SequinsFFI.xcframework consumed by the SequinsData SPM package.
#
# Builds the `sequins-ffi` staticlib for both macOS architectures, lipos them
# into a universal `libsequins_ffi.a`, and refreshes the xcframework slice's
# library + headers (the cbindgen build script regenerates `sequins.h` on build).
#
# Run from anywhere; paths are resolved relative to the repo.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
XCFRAMEWORK="$SCRIPT_DIR/SequinsData/SequinsFFI.xcframework"
SLICE="$XCFRAMEWORK/macos-arm64_x86_64"
PROFILE="${1:-release}"

if [[ "$PROFILE" == "release" ]]; then
  CARGO_PROFILE_FLAG="--release"
  TARGET_SUBDIR="release"
else
  CARGO_PROFILE_FLAG=""
  TARGET_SUBDIR="debug"
fi

echo "==> Building sequins-ffi ($PROFILE) for aarch64 + x86_64…"
cd "$REPO_ROOT"
cargo build -p sequins-ffi $CARGO_PROFILE_FLAG --target aarch64-apple-darwin
cargo build -p sequins-ffi $CARGO_PROFILE_FLAG --target x86_64-apple-darwin

ARM64_LIB="$REPO_ROOT/target/aarch64-apple-darwin/$TARGET_SUBDIR/libsequins_ffi.a"
X86_64_LIB="$REPO_ROOT/target/x86_64-apple-darwin/$TARGET_SUBDIR/libsequins_ffi.a"

echo "==> Creating universal static library…"
mkdir -p "$SLICE/Headers"
lipo -create "$ARM64_LIB" "$X86_64_LIB" -output "$SLICE/libsequins_ffi.a"

echo "==> Refreshing headers…"
cp "$REPO_ROOT/crates/sequins-ffi/include/sequins.h" "$SLICE/Headers/sequins.h"
cp "$REPO_ROOT/crates/sequins-ffi/include/module.modulemap" "$SLICE/Headers/module.modulemap"

echo "==> Done. xcframework refreshed at:"
echo "    $XCFRAMEWORK"
lipo -info "$SLICE/libsequins_ffi.a"
