//! Administrative subcommands. Currently just `add-user`; the module
//! exists as the home for whatever lifecycle (key rotation, role
//! pruning, etc.) lands next.
//!
//! `add-user` does the work the deploy doc used to ask the operator to
//! do by hand with `psql`: insert the labctl-side row, create the PG
//! role with LOGIN + the full GRANT set, and materialise the per-user
//! directory tree under `runs_base` and each artifact root. The three
//! steps are sequenced so a partial failure is recoverable on rerun:
//!   1. PG INSERT into `users` (the labctl-side row). If this fails
//!      (e.g. row already exists), nothing else fires.
//!   2. PG role + GRANTs. Idempotent — the DO block checks pg_roles
//!      first. Failures here usually mean the connecting user lacks
//!      CREATEROLE; the labctl-side row is rolled back via the
//!      surrounding transaction.
//!   3. FS dirs. Independent of PG: `create_dir_all` is idempotent,
//!      we apply the cluster's shared-group perms if configured.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::{config::ClusterConfig, fs_layout, store::Store, util};

#[derive(Debug, Serialize)]
pub struct AddUserReport {
    pub name: String,
    pub registered: bool,
    pub pg_role_created: bool,
    pub dirs_created: Vec<PathBuf>,
}

pub async fn add_user(
    cluster: &ClusterConfig,
    store: &Store,
    name: &str,
    want_pg_role: bool,
    want_dirs: bool,
) -> Result<AddUserReport> {
    // Strict validation: every downstream step interpolates `name`
    // either into a path segment (FS layout) or into a SQL identifier
    // (via PG's `format('... %I ...')` — itself safe, but pinning the
    // shape here gives a single rejection point with a clear message).
    validate_admin_name(name)?;
    fs_layout::validate_user(name)?;

    let now = util::now_ts();
    let registered = store
        .insert_user(name, now)
        .await
        .with_context(|| format!("registering user {name:?} in PG"))?;

    let pg_role_created = if want_pg_role {
        store
            .ensure_pg_role(name)
            .await
            .with_context(|| format!("creating PG role {name:?}"))?
    } else {
        false
    };

    let dirs_created = if want_dirs {
        create_user_dirs(cluster, name)?
    } else {
        Vec::new()
    };

    Ok(AddUserReport {
        name: name.to_string(),
        registered,
        pg_role_created,
        dirs_created,
    })
}

/// Stricter than `fs_layout::validate_user`: bounds the charset to
/// `[A-Za-z0-9._-]` so the name is safe to interpolate into a SQL
/// identifier even before PG's `quote_ident` (defence in depth).
fn validate_admin_name(name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("user name must not be empty");
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        bail!(
            "user name {name:?} contains characters outside [A-Za-z0-9._-]; \
             matches neither a Unix username nor a PG identifier"
        );
    }
    Ok(())
}

/// Per-user dir tree: `<runs_base>/runs/<user>/` plus
/// `<artifact_root>/<user>/` for every kind. Mirrors the layout `init`
/// builds for the bootstrapping user. Idempotent on rerun. Shared-group
/// perms (from `[filesystem].shared_group`) are applied to any newly-
/// created leaf so peer users can read across the tree.
fn create_user_dirs(cluster: &ClusterConfig, name: &str) -> Result<Vec<PathBuf>> {
    let mut targets: Vec<PathBuf> = vec![fs_layout::user_runs_dir(
        &cluster.filesystem.runs_base,
        name,
    )];
    for root in cluster.filesystem.artifact_roots.values() {
        targets.push(root.join(name));
    }
    targets.dedup();

    let mut created: Vec<PathBuf> = Vec::new();
    for t in &targets {
        ensure_dir(t, cluster.filesystem.shared_group.as_deref())
            .with_context(|| format!("create_user_dirs: {}", t.display()))?;
        created.push(t.clone());
    }
    Ok(created)
}

fn ensure_dir(path: &Path, shared_group: Option<&str>) -> Result<()> {
    std::fs::create_dir_all(path).with_context(|| format!("create_dir_all {}", path.display()))?;
    if let Some(group) = shared_group {
        fs_layout::apply_shared_perms(path, group)
            .with_context(|| format!("apply shared-group perms ({group}) to {}", path.display()))?;
    }
    Ok(())
}
