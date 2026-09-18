#!/bin/sh
# Builds the muser binaries that carry the MelonDMA RDMA bulk lane and signs
# them for MelonDMA's DriverKit user client.
#
#   MELONDMA_DEXT_DIR=~/src/MelonDMA/dev/src/dext scripts/build_melon_rdma.sh
#
# Both binaries open the NIC: `muser` receives, `muser-remote-qualify` is what
# `muser node smoke` runs. Each needs, and each has lost in the past, three
# things that do not show up until the first handoff: the user-client
# entitlement naming the driver's *current* bundle id, a trusted signing
# identity (an ad-hoc signature is refused with kIOReturnNotPermitted), and an
# LC_RPATH to the shim. This script checks all three before calling it done.
set -eu

: "${MELONDMA_DEXT_DIR:?point MELONDMA_DEXT_DIR at the src/dext of a MelonDMA checkout}"
sign_id="${MUSER_SIGN_ID:-Apple Development}"
entitlements="$MELONDMA_DEXT_DIR/tools/reinit.entitlements"
[ -f "$entitlements" ] || { echo "no user-client entitlements at $entitlements" >&2; exit 1; }
[ -f "$MELONDMA_DEXT_DIR/build/libibverbs.dylib" ] || {
    echo "no MelonDMA shim at $MELONDMA_DEXT_DIR/build/libibverbs.dylib — build MelonDMA first" >&2
    exit 1
}
key='Print :com.apple.developer.driverkit.userclient-access:0'
bundle_id=$(/usr/libexec/PlistBuddy -c "$key" "$entitlements")

cd "$(dirname "$0")/.."
export MELONDMA_DEXT_DIR
cargo build --release --features melon-rdma -p muser-server
cargo build --release --features "melon-rdma,metal" -p muser-bench --bin muser-remote-qualify

for binary in target/release/muser target/release/muser-remote-qualify; do
    codesign --force --options runtime --timestamp=none --sign "$sign_id" \
        --entitlements "$entitlements" "$binary"
    codesign -d --entitlements - "$binary" 2>/dev/null | grep -q "$bundle_id" || {
        echo "$binary is not signed for $bundle_id" >&2
        exit 1
    }
    otool -l "$binary" | grep -q "path $MELONDMA_DEXT_DIR/build" || {
        echo "$binary has no LC_RPATH to $MELONDMA_DEXT_DIR/build" >&2
        exit 1
    }
done

# The entitlement only opens a driver that answers to the same bundle id.
if ! systemextensionsctl list 2>/dev/null | grep -q "$bundle_id.*activated enabled"; then
    echo "warning: no activated system extension answers to $bundle_id; the binaries" >&2
    echo "         are signed for it, but RDMA will be refused until that driver runs" >&2
fi
echo "built and signed for $bundle_id: target/release/muser target/release/muser-remote-qualify"
