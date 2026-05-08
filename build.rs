// Capture the short git SHA at build time so `labctl --version` can identify
// the exact commit a binary was built from. Falls back to "unknown" outside a
// git checkout (e.g. release tarballs).
fn main() {
    let sha = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| {
            o.status.success().then(|| {
                String::from_utf8_lossy(&o.stdout).trim().to_string()
            })
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=LABCTL_GIT_SHA={sha}");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/heads");
}
