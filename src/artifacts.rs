use std::path::Path;

use anyhow::{Context, Result};
use serde_json::json;

use crate::{
    store::{ArtifactRow, Store},
    util,
};

/// Register a directory or file that already exists on disk as an artifact.
///
/// External artifacts deliberately skip the deep ``dir_content_hash`` walk
/// that internal (labctl-produced) artifacts use. Hashing every byte of a
/// multi-TB raw-data directory would take hours and isn't useful here:
/// the user is asserting "this path is the artifact" — labctl just needs
/// a stable, idempotent dedup key. The canonical path itself is exactly
/// that. Re-registering the same path yields the same artifact id; moving
/// the data to a new path and re-registering produces a fresh entry.
pub fn register_external(
    store: &mut Store,
    alias: &str,
    path: &Path,
    kind: &str,
) -> Result<ArtifactRow> {
    let path = path
        .canonicalize()
        .with_context(|| format!("external artifact path does not exist: {}", path.display()))?;
    let content_hash = util::sha256_bytes(path.display().to_string().as_bytes());
    let artifact = store.insert_artifact(
        kind,
        &path,
        &content_hash,
        None,
        &json!({
            "external": true,
            "alias": alias,
            "path": path,
            "hash_strategy": "canonical_path",
        }),
    )?;
    store.set_alias(alias, &artifact.id)?;
    Ok(artifact)
}
