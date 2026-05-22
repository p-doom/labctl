# Postgres deployment

Postgres is the source of truth for runs, artifacts, aliases, eval
requests, pipelines, and events. Artifact bytes live at the producer-
written path under `<artifact_root>/<user>/<alias>/`; provenance
bundles continue to live in a content-addressed tree
(`_provenance_objects/`) under `runs_base`. Only the indexable
metadata is in PG.

This doc covers the **user-installed, single-instance** PG deployment
under the labctl admin's user account, suitable for a lab-team
cluster where:

- Cluster admins won't (or shouldn't need to) install PG system-wide.
- One designated user runs the PG instance; other users connect as PG
  roles.
- The host running labctl services (`labctl-ui.service`,
  `labctl-agent.service`) also hosts PG — currently `hai-login2`.

For larger deployments (cluster-admin-managed PG, multi-host service
federation), the same schema + clients apply; only the install
procedure differs.

## Substrate

Cluster: `haicore.berlin` (Helmholtz Munich)
Host: `hai-login2`
PG version: 16.4 (module `PostgreSQL/16.4-GCCcore-13.3.0`)
Data dir: `/fast/project/HFMI_SynergyUnit/p-doom_shared/labctl/postgres/data`
Run dir (Unix socket): `/fast/project/.../postgres/run`
Log dir: `/fast/project/.../postgres/log`

NFS-fsync caveat: the data dir is on NFS. fsync over NFS is
theoretically dicier than local-disk fsync (the NFS client may report
success before the server has durably written). For a single-instance
PG with one writing host, the documented "SQLite over NFS" corruption
modes (lock contention across hosts) do not apply. Acceptable for
this scale; revisit if disk-backed local scratch becomes available.

## Initial install

```bash
module load PostgreSQL/16.4-GCCcore-13.3.0

PGROOT=/fast/project/HFMI_SynergyUnit/p-doom_shared/labctl/postgres
mkdir -p "$PGROOT/data" "$PGROOT/run" "$PGROOT/log"

initdb -D "$PGROOT/data" \
    --auth=peer --auth-host=scram-sha-256 \
    --encoding=UTF8 --locale=C --username=franz.srambical
```

## Configuration

Append to `$PGROOT/data/postgresql.conf`:

```
include 'postgresql.conf.local'
```

Write `$PGROOT/data/postgresql.conf.local`:

```ini
listen_addresses = '127.0.0.1,10.86.2.252'   # localhost + hai-login2 internal IP
port = 5432
unix_socket_directories = '/fast/project/HFMI_SynergyUnit/p-doom_shared/labctl/postgres/run'
max_connections = 100
shared_buffers = 256MB
fsync = on
wal_level = replica
logging_collector = on
log_directory = '/fast/project/HFMI_SynergyUnit/p-doom_shared/labctl/postgres/log'
log_filename = 'postgresql-%Y-%m-%d.log'
log_line_prefix = '%t [%p] %u@%d '
```

Write `$PGROOT/data/pg_hba.conf`:

```
# labctl PG instance auth
local   all   all                  peer
host    all   all   127.0.0.1/32   scram-sha-256
host    all   all   ::1/128        scram-sha-256
```

## systemd-user unit

Write `~/.config/systemd/user/labctl-postgres.service`:

```ini
[Unit]
Description=labctl Postgres instance
After=network.target
# Data dir lives on NFS shared across all login nodes. Only one host
# may run PG against it at a time — multi-host postmaster.pid contention
# was observed when this unit was enabled on hai-login{1,3}.
ConditionHost=hai-login2.haicore.berlin

[Service]
Type=forking
Environment=PGDATA=/fast/project/HFMI_SynergyUnit/p-doom_shared/labctl/postgres/data
ExecStart=/fast/service/apps/software/PostgreSQL/16.4-GCCcore-13.3.0/bin/pg_ctl -D ${PGDATA} -l /fast/project/HFMI_SynergyUnit/p-doom_shared/labctl/postgres/log/server.log -w start
ExecStop=/fast/service/apps/software/PostgreSQL/16.4-GCCcore-13.3.0/bin/pg_ctl -D ${PGDATA} -m fast stop
ExecReload=/fast/service/apps/software/PostgreSQL/16.4-GCCcore-13.3.0/bin/pg_ctl -D ${PGDATA} reload
TimeoutSec=120
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
```

Then:

```bash
systemctl --user daemon-reload
systemctl --user enable --now labctl-postgres.service
```

`pg_ctl` exit-code semantics + `Type=forking` give systemd a reliable
ready signal without needing the `notify` PG build flavor (the
cluster's PG module lacks libsystemd integration).

**Critical gotcha — `ConditionHost`.** The data dir lives on NFS and is
visible from every login node. systemd-user units, once `enable`d on
one login node, *autostart on every other login node the user logs
into*. Without the host-pinning condition, multiple postmasters race
for `postmaster.pid` over NFS — each kicks the other out with
`performing immediate shutdown because data directory lock file is
invalid`, observable as a ~30-second restart cycle and intermittent
client connection failures with "No such file or directory" or "Stale
file handle" on the socket. Always include `ConditionHost=` with the
FQDN of the designated PG host. To recover after the fact, stop +
disable the unit on every non-designated host:

```bash
for h in hai-login1 hai-login3; do
    ssh "$h" "systemctl --user stop labctl-postgres.service \
              && systemctl --user disable labctl-postgres.service"
done
```

## Schema

Migrations live in `migrations/` and are applied automatically by
`PgStore::connect` via `sqlx::migrate!`. Every labctl process applies
any pending migrations as part of its startup; a failure aborts
`connect()` (no half-applied schema is ever observable).

On a brand-new instance you only need to create the empty database;
the first agent or CLI invocation runs the migrations:

```bash
psql -h "$PGROOT/run" -d postgres -c 'CREATE DATABASE labctl;'
# next labctl invocation drives the migrations
labctl status    # or any subcommand that opens PgStore
```

## First-time cutover from an existing labctl deployment

If you already have an old labctl instance running (a PG database
created when only `0001` existed, populated by the legacy importer
or by hand), do **not** point the new binary at it directly: the new
constraints (FKs in 0002, FKs to `users` in 0003) will fail to apply
mid-`ALTER TABLE` against rows the old code never validated. The
cleanest path is a full re-import on a fresh DB:

```bash
# 1. stop both services so nothing writes during the cutover.
systemctl --user stop labctl-ui.service labctl-agent.service

# 2. drop + recreate; sqlx::migrate! will lay down 0001+0002+0003 fresh.
psql -h "$PGROOT/run" -d postgres -c 'DROP DATABASE labctl;'
psql -h "$PGROOT/run" -d postgres -c 'CREATE DATABASE labctl;'

# 3. let any labctl subcommand run migrations on the empty DB.
labctl status

# 4. re-import the FS sidecars under the new constrained schema.
module load PostgreSQL/16.4-GCCcore-13.3.0
python3 scripts/import-to-pg.py --cluster ~/.config/labctl/cluster.toml

# 5. restart services.
systemctl --user start labctl-agent.service labctl-ui.service
```

The importer is single-transaction (`BEGIN; TRUNCATE … CASCADE;
\copy …; COMMIT;`), so the DEFERRABLE FKs in 0002 validate only at
commit. `users` is loaded first; every downstream `submitted_by` /
`"user"` reference resolves immediately.

## Importing existing FS-truth into PG

The cutover described above uses the same script the original
substrate import used:

```bash
module load PostgreSQL/16.4-GCCcore-13.3.0
python3 scripts/import-to-pg.py \
    --cluster ~/.config/labctl/cluster.toml \
    [--dry-run]
```

The importer walks `<runs_base>/runs/<user>/<run_id>/.lab/*.json`,
the legacy `<artifact_roots>/<kind>/_objects/<prefix>/<hash>/.meta.json`
trees written by pre-c1d31e8 runs, `<runs_base>/aliases/`,
`<runs_base>/pipelines/`, `<runs_base>/eval_state/`, and
`<runs_base>/events/*.jsonl`, emits per-table TSV files (including a
derived `users.tsv` built from the distinct `submitted_by` / `"user"`
values it observes), then `\copy`s them into PG in one transaction.
Artifacts written post-c1d31e8 land at their producer path via the
normal `insert_artifact` path at run-import time, not through the
`_objects/` walk.

The original `events_id_seq` is restored via `setval` so subsequent
`INSERT INTO events` doesn't collide with imported event ids.

## Backup

Daily `pg_dump` via cron:

```cron
0 2 * * *  module load PostgreSQL/16.4-GCCcore-13.3.0 && pg_dump -h $PGROOT/run labctl | zstd > $PGROOT/backup/labctl-$(date +\%Y\%m\%d).sql.zst
```

Combined with the NFS-side artifact bytes (under
`<artifact_root>/<user>/<alias>/`) and provenance bundles
(content-addressed under `_provenance_objects/`, inherently restorable from each
other or from a peer cluster), this is the disaster-recovery story.

## Connecting from labctl

Configured per-user in `cluster.toml`:

```toml
[postgres]
host = "/fast/project/HFMI_SynergyUnit/p-doom_shared/labctl/postgres/run"  # Unix socket
port = 5432
database = "labctl"
# user defaults to $USER
```

The Unix socket path enables peer authentication on the PG host
(`hai-login2`). For agent / CLI invocations on other login nodes (or
in containers where the socket path is unavailable), set `host` to a
TCP target and provide credentials via `~/.pgpass` or env vars.

## Multi-user rollout

Use the `labctl admin add-user` subcommand, run by whoever owns the
PG database (the user who ran `initdb` — on the canonical lab setup,
that's the same user who hosts `labctl-postgres.service`):

```bash
labctl admin add-user alice
```

In one transaction's worth of bookkeeping, the command:

  * Inserts the labctl-side row in the `users` table (FK target for
    every per-user column).
  * Creates the PG role with LOGIN and grants the full table /
    sequence / default-privilege set on `public` (idempotent: a
    pre-existing role is detected and only the GRANTs re-apply).
  * Materialises the per-user FS dirs: `<runs_base>/runs/<alice>/`,
    `<artifact_root>/<alice>/`, `<artifact_root>/aliases/<alice>/`
    for every artifact root, honouring `[filesystem].shared_group`
    perms if configured.

Flags: `--no-pg-role` (the role already exists / is provisioned
out-of-band), `--no-create-dirs` (FS lives elsewhere or has already
been arranged).

After `add-user` returns, the new collaborator copies a `cluster.toml`
pointing at this PG instance (`labctl init --join <path>` is the
intended path) and starts using labctl. Their PG connection uses the
matching role; peer auth on the Unix socket works when they're on
the PG host, password / `~/.pgpass` everywhere else.
