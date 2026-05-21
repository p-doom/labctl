# Postgres deployment

labctl is moving from filesystem-as-truth + in-memory SQLite cache to
**Postgres-as-truth** for runs / artifacts / events / pipelines. The
artifact bytes and provenance bundles continue to live in
content-addressed FS trees (`_objects/`, `_provenance_objects/`); only
the indexable metadata moves to PG.

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

## Schema

Applied via `migrations/0001_initial_schema.sql` (in this repo). Run
once after `CREATE DATABASE labctl`:

```bash
psql -h "$PGROOT/run" -d postgres -c 'CREATE DATABASE labctl;'
psql -h "$PGROOT/run" -d labctl -f migrations/0001_initial_schema.sql
```

In the long-running setup, schema migrations land via `sqlx-cli` from
the labctl crate at agent / CLI startup.

## Importing existing FS-truth into PG

One-shot, idempotent (`TRUNCATE … RESTART IDENTITY` then `\copy`):

```bash
module load PostgreSQL/16.4-GCCcore-13.3.0
python3 scripts/import-to-pg.py \
    --cluster ~/.config/labctl/cluster.toml \
    [--dry-run]
```

The importer walks `<runs_base>/runs/<user>/<run_id>/.lab/*.json`,
`<artifact_roots>/<kind>/_objects/<prefix>/<hash>/.meta.json`,
`<runs_base>/aliases/`, `<runs_base>/pipelines/`,
`<runs_base>/eval_state/`, and `<runs_base>/events/*.jsonl`, emits
per-table TSV files, then `\copy`s them into PG. Currently imports
~8.4k rows across 10 tables in seconds.

The original `events_id_seq` is restored via `setval` so subsequent
`INSERT INTO events` doesn't collide with imported event ids.

## Backup

Daily `pg_dump` via cron:

```cron
0 2 * * *  module load PostgreSQL/16.4-GCCcore-13.3.0 && pg_dump -h $PGROOT/run labctl | zstd > $PGROOT/backup/labctl-$(date +\%Y\%m\%d).sql.zst
```

Combined with the NFS-side artifact bytes and provenance bundles
(which are content-addressed and inherently restorable from each
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

Not yet automated. To grant another user write access:

```sql
CREATE ROLE alice WITH LOGIN;
GRANT ALL ON SCHEMA public TO alice;
GRANT ALL ON ALL TABLES IN SCHEMA public TO alice;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO alice;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO alice;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO alice;
```

Future `labctl admin add-user <name>` should automate this.
