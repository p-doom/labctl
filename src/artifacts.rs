use std::path::Path;

use anyhow::{Context, Result};
use serde_json::json;

use crate::store::{ArtifactRow, Store};

/// Register a directory or file that already exists on disk as an artifact.
///
/// External artifacts skip every form of byte-walking: the user is
/// asserting "this path is the artifact" and labctl uses the canonical
/// path itself as identity. Re-registering the same path yields the
/// same artifact id (path-canonical id derivation lives in
/// `Store::insert_artifact`); moving the data to a new path and
/// re-registering produces a fresh entry.
pub fn register_external(
    store: &Store,
    alias: &str,
    path: &Path,
    kind: &str,
) -> Result<ArtifactRow> {
    let path = path
        .canonicalize()
        .with_context(|| format!("external artifact path does not exist: {}", path.display()))?;
    let artifact = store.insert_artifact(
        kind,
        &path,
        None,
        &json!({
            "external": true,
            "alias": alias,
            "path": path,
        }),
    )?;
    store.set_alias(alias, &artifact.id)?;
    Ok(artifact)
}

