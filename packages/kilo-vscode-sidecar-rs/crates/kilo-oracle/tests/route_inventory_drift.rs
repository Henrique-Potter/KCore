//! Route-inventory drift gate.
//!
//! Per migration plan **Issue-ready work breakdown #45**:
//!
//! > Add `route_inventory` PR check that fails if
//! > `docs/route-inventory.md` is unchanged when route handlers move.
//!
//! Implementation: hash the source bytes of every file under
//! `crates/kilo-server/src/routes/` and the registered routes inside
//! `crates/kilo-server/src/http/mod.rs`, then compare against a stored
//! snapshot under `tests/route_inventory_snapshot.txt`. Anyone adding,
//! removing, or signature-changing a route must regenerate the snapshot
//! AND update `docs/route-inventory.md` in the same commit. The mismatch
//! makes the omission impossible to ship without a deliberate
//! acknowledgement.
//!
//! Regenerate the snapshot:
//!
//! ```bash
//! cargo test -p kilo-oracle --test route_inventory_drift -- --nocapture \
//!     2>&1 | grep "Update snapshot to:" | tail -n1 \
//!     | sed 's/Update snapshot to: //' \
//!     > crates/kilo-oracle/tests/route_inventory_snapshot.txt
//! ```
//!
//! Or just run the test, copy the printed hash, and overwrite the file.

use sha2::{Digest, Sha256};
use std::{fs, path::PathBuf};

fn repo_root() -> PathBuf {
    let manifest = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn sidecar_root() -> PathBuf {
    repo_root().join("packages").join("kilo-vscode-sidecar-rs")
}

#[test]
fn route_handlers_match_inventory_snapshot() {
    let mut hasher = Sha256::new();

    // Hash every routes/*.rs file body. Order is deterministic.
    let routes_dir = sidecar_root()
        .join("crates")
        .join("kilo-server")
        .join("src")
        .join("routes");
    let mut entries: Vec<PathBuf> = fs::read_dir(&routes_dir)
        .expect("routes/ exists")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("rs"))
        .collect();
    entries.sort();
    for path in &entries {
        let body = fs::read(path).expect("readable route file");
        hasher.update(path.file_name().unwrap().to_string_lossy().as_bytes());
        hasher.update(b"\0");
        hasher.update(&body);
        hasher.update(b"\0");
    }

    // Hash the router-registration body of `http/mod.rs`. We hash the
    // whole file rather than parse — any change to route registration
    // (routes added, removed, or method-changed) flips the hash.
    let http_mod = sidecar_root()
        .join("crates")
        .join("kilo-server")
        .join("src")
        .join("http")
        .join("mod.rs");
    let body = fs::read(&http_mod).expect("readable http/mod.rs");
    hasher.update(b"http/mod.rs\0");
    hasher.update(&body);

    let actual = hex_encode(&hasher.finalize());

    let snapshot_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("route_inventory_snapshot.txt");

    let expected = fs::read_to_string(&snapshot_path)
        .map(|s| s.trim().to_string())
        .unwrap_or_default();

    if expected != actual {
        let inventory = sidecar_root().join("docs").join("route-inventory.md");
        eprintln!("\n--- Route inventory drift ---");
        eprintln!(
            "Stored snapshot:   {}",
            expected
                .is_empty()
                .then(|| "<missing>")
                .unwrap_or(&expected)
        );
        eprintln!("Current snapshot:  {actual}");
        eprintln!("Update snapshot to: {actual}");
        eprintln!("\nIf you intentionally changed routes:");
        eprintln!("  1. Update {}", inventory.display());
        eprintln!(
            "  2. Overwrite {} with the current snapshot above.",
            snapshot_path.display()
        );
        eprintln!(
            "  3. Re-run `cargo test -p kilo-oracle --test inventory_parity` to confirm SDK parity.",
        );
        panic!("route_inventory_drift: snapshot mismatch — see stderr for remediation");
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}
