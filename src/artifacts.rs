use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::json;

use crate::{
    config::ClusterConfig,
    fs_layout::{self, AliasTargetSidecar, ArtifactSidecar},
    remote,
    store::{self, ArtifactRow, Store},
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

/// Report from `import_from_cluster`. Surfaced as JSON to the CLI so
/// scripts can grep it.
#[derive(Debug, Serialize)]
pub struct ImportReport {
    pub artifact_id: String,
    pub local_alias: String,
    pub kind: String,
    pub content_hash: String,
    pub local_path: PathBuf,
    pub bytes_copied: bool,
    pub dedup_hit: bool,
    pub foreign_cluster: String,
    pub foreign_artifact_path: PathBuf,
}

/// Import an artifact identified by `foreign_alias` on `foreign_cluster`
/// into the local cluster's registry. Reads the foreign alias + meta
/// sidecars over SSH, rsyncs the bytes into the local artifact_root
/// (when `copy_bytes`), and inserts a local artifact row that
/// preserves the foreign content hash (so re-importing the same bytes
/// dedupes) and records lineage via metadata. Local alias resolves to
/// the imported artifact.
///
/// Failures are loud: foreign alias missing, no [remote] on the
/// foreign cluster, kind not configured locally, local alias already
/// in use, content_hash mismatch on a dedup hit. The command is
/// intended for human-driven use after consulting the foreign UI; the
/// errors point at the right next step rather than papering over.
pub fn import_from_cluster(
    local_cluster: &ClusterConfig,
    local_store: &mut Store,
    foreign_cluster: &ClusterConfig,
    foreign_alias: &str,
    local_alias: Option<&str>,
    copy_bytes: bool,
) -> Result<ImportReport> {
    let remote = foreign_cluster.remote.as_ref().with_context(|| {
        format!(
            "foreign cluster {:?} has no [remote] section in its cluster.toml — \
             add ssh_alias (preferred) or host/ssh_user so cross-cluster ops \
             know how to reach it",
            foreign_cluster.name
        )
    })?;

    // 1. Resolve the foreign alias.
    let target_path =
        fs_layout::alias_target(&foreign_cluster.filesystem.runs_base, foreign_alias);
    let target: AliasTargetSidecar = remote::read_json(remote, &target_path)
        .with_context(|| {
            format!(
                "foreign alias {foreign_alias:?} not found on cluster {:?} \
                 (looked at {})",
                foreign_cluster.name,
                target_path.display()
            )
        })?;

    // 2. Read the artifact's metadata sidecar.
    let foreign_artifact_path = target.artifact_path.clone();
    let foreign_meta_path = foreign_artifact_path.join(fs_layout::ARTIFACT_META);
    let foreign_meta: ArtifactSidecar =
        remote::read_json(remote, &foreign_meta_path).with_context(|| {
            format!(
                "foreign artifact metadata at {} on cluster {:?}",
                foreign_meta_path.display(),
                foreign_cluster.name
            )
        })?;

    // 3. Locate this kind's root on the local cluster.
    let local_root = local_cluster
        .filesystem
        .artifact_roots
        .get(&foreign_meta.kind)
        .with_context(|| {
            format!(
                "local cluster {:?} has no [filesystem.artifact_roots].{kind} entry; \
                 can't accept imports of this kind",
                local_cluster.name,
                kind = foreign_meta.kind,
            )
        })?;

    // 4. Compute the local destination. The imported artifact becomes
    //    `<local_root>/<local_user>/<final_alias>`; whatever directory
    //    structure was under the foreign artifact path is preserved by
    //    rsync underneath. Local alias defaults to the foreign one.
    let final_alias = local_alias.unwrap_or(foreign_alias);
    let local_user = store::current_user()?;
    let local_dst = fs_layout::artifact_dir(local_root, &local_user, final_alias);

    // 5. If the local registry already has an artifact with this hash,
    //    don't re-copy bytes — return the existing one (idempotent
    //    re-import). This is the same dedup the internal pipeline gets
    //    for free via `Store::insert_artifact`'s hash check; we surface
    //    it explicitly here so the CLI report can tell the user.
    if let Some(existing) =
        local_store.find_artifact_by_hash(&foreign_meta.kind, &foreign_meta.content_hash)?
    {
        // Honor the alias request even on a dedup hit — they may have
        // imported under a different local alias previously.
        local_store.set_alias(final_alias, &existing.id)?;
        return Ok(ImportReport {
            artifact_id: existing.id.clone(),
            local_alias: final_alias.to_string(),
            kind: foreign_meta.kind.clone(),
            content_hash: foreign_meta.content_hash.clone(),
            local_path: existing.path.clone(),
            bytes_copied: false,
            dedup_hit: true,
            foreign_cluster: foreign_cluster.name.clone(),
            foreign_artifact_path,
        });
    }

    // 6. Refuse to overwrite an existing local path that's bytes-only
    //    (not yet registered). insert_artifact would happily decompose
    //    the path and write its .meta.json, but the user almost
    //    certainly meant to import to a fresh destination.
    if local_dst.exists() {
        bail!(
            "local destination already exists: {} \
             (not a registry hit — bytes are there but no .meta.json declares an \
              artifact with this content_hash). Pick a different --as <alias> \
              or clean up the path manually.",
            local_dst.display()
        );
    }

    // 7. Copy bytes from the foreign cluster.
    let bytes_copied = if copy_bytes {
        remote::rsync_dir(remote, &foreign_artifact_path, &local_dst)?;
        true
    } else {
        // --no-copy: stub-only registration. The user is asserting
        // they'll provide bytes out-of-band (manual rsync, shared mount
        // appearing later, etc.). Create the empty dir so the path
        // exists for insert_artifact's decompose step.
        std::fs::create_dir_all(&local_dst).with_context(|| {
            format!("failed to mkdir stub local destination {}", local_dst.display())
        })?;
        false
    };

    // 8. Register on the local cluster. Preserve content_hash from
    //    foreign so identity is consistent across clusters; augment
    //    metadata with import provenance so the lineage UI can show
    //    where this came from.
    let mut metadata = foreign_meta.metadata.clone();
    let already_imported = metadata.get("imported_from").is_some();
    metadata["imported_from"] = json!({
        "cluster": foreign_cluster.name,
        "artifact_id": foreign_meta.id,
        "alias": foreign_alias,
        "path": foreign_artifact_path,
        "producer_run_id": foreign_meta.producer_run_id,
        "bytes_copied": bytes_copied,
        "chained": already_imported,
    });

    let local_artifact = local_store.insert_artifact(
        &foreign_meta.kind,
        &local_dst,
        &foreign_meta.content_hash,
        None, // no local producer — the artifact came from outside
        &metadata,
    )?;
    local_store.set_alias(final_alias, &local_artifact.id)?;

    Ok(ImportReport {
        artifact_id: local_artifact.id.clone(),
        local_alias: final_alias.to_string(),
        kind: foreign_meta.kind,
        content_hash: foreign_meta.content_hash,
        local_path: local_dst,
        bytes_copied,
        dedup_hit: false,
        foreign_cluster: foreign_cluster.name.clone(),
        foreign_artifact_path,
    })
}
