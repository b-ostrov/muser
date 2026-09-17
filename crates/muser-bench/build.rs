// Same job as crates/muser-server/build.rs: this package's binaries link
// MelonDMA's libibverbs shim through muser-cluster's melon-rdma feature and
// need their own LC_RPATH to load it at runtime.
//
// A build script's `rustc-link-arg` applies only to the binaries of its own
// package, so muser-cluster's copy does not reach muser-remote-qualify -- the
// binary `muser node smoke` actually runs. Without this it links fine and then
// aborts under the accelerator wrapper with "Library not loaded:
// @rpath/libibverbs.dylib", which surfaces to the operator as an exit-status
// mismatch against the retained receipt rather than as a missing library.
//
// A stock build (no feature) returns immediately and needs no MelonDMA
// checkout, exactly as muser-cluster's build script does.
fn main() {
    println!("cargo:rerun-if-env-changed=MELONDMA_DEXT_DIR");
    if std::env::var("CARGO_FEATURE_MELON_RDMA").is_err() {
        return;
    }
    if std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default() != "macos" {
        return;
    }
    let dext_dir = std::env::var("MELONDMA_DEXT_DIR").expect(
        "MELONDMA_DEXT_DIR must point at a MelonDMA checkout's src/dext \
         (for its libibverbs_compat shim) to build the melon-rdma feature on macOS",
    );
    println!("cargo:rustc-link-arg=-Wl,-rpath,{dext_dir}/build");
}
