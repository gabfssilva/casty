//! ld-27037 (Xcode 27) leaves the LINKEDIT string pool misaligned in the old dyld-info format when the count of
//! indirect symbols is odd, and dyld then refuses the library. Chained fixups are the format dyld reads on every
//! macOS the wheel targets, and are not subject to it.

fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-link-arg=-Wl,-fixup_chains");
    }
}
