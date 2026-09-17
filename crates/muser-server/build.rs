// The `muser` binary links MelonDMA's libibverbs shim through muser-cluster's
// melon-rdma feature, and needs its own LC_RPATH to load it.
//
// muser-cluster's build script already emits this rpath, but a build script's
// `rustc-link-arg` applies only to the binaries of its own package -- it does
// not reach a dependent's binary. Without this, `muser` built with
// `--features melon-rdma` links fine and then dies at exec with
// "Library not loaded: @rpath/libibverbs.dylib". The shim is not installed
// into a system library directory; it lives in a MelonDMA checkout, so the
// path has to be recorded at build time for runtime too.
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
