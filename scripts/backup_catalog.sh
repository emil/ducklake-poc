#!/usr/bin/env bash
# Back up the DuckLake catalog (the Postgres database) with pg_dump.
#
# Output: $BACKUP_DIR/<database>_<UTC timestamp>.tgz, for example
#   ducklake_catalog_20261012T101500Z.tgz
# The archive holds one plain SQL dump: <database>_<timestamp>.sql
#
# The catalog holds the DuckLake metadata, wave_manifest and ingest_queue.
# It does NOT hold the Parquet files. Back up data/lake (and data/raw, if
# you need it) separately. A catalog restored without its files points to
# files that do not exist.
#
# Environment:
#   BACKUP_DIR    (required) directory for the archives. Created if missing.
#   PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
#                 same variables and defaults as src/ducklake_poc/config.py.
#   PG_CONTAINER  docker container to use when pg_dump is not on PATH
#                 (default: ducklake_postgres, from docker-compose.yml).
#
# Restore:
#   tar -xzOf <archive>.tgz | psql -h HOST -U USER -d DB
#   The dump uses --clean --if-exists, so it replaces the existing objects.
#   Stop ducklake-ingest-worker and ducklake-api before you restore.

set -euo pipefail

if [[ -z "${BACKUP_DIR:-}" ]]; then
    echo "error: BACKUP_DIR is not set" >&2
    exit 2
fi

PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-ducklake_catalog}"
PG_USER="${PG_USER:-ducklake}"
PG_PASSWORD="${PG_PASSWORD:-ducklake}"
PG_CONTAINER="${PG_CONTAINER:-ducklake_postgres}"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
name="${PG_DB}_${timestamp}"
archive="${BACKUP_DIR}/${name}.tgz"

mkdir -p "${BACKUP_DIR}"
work="$(mktemp -d "${BACKUP_DIR}/.${name}.XXXXXX")"
trap 'rm -rf "${work}"' EXIT

dump_args=(--username "${PG_USER}" --dbname "${PG_DB}"
           --format=plain --clean --if-exists --no-owner)

if command -v pg_dump >/dev/null 2>&1; then
    PGPASSWORD="${PG_PASSWORD}" pg_dump --host "${PG_HOST}" --port "${PG_PORT}" \
        "${dump_args[@]}" > "${work}/${name}.sql"
elif command -v docker >/dev/null 2>&1; then
    # No client tools on the host: run pg_dump inside the postgres container.
    # This also guarantees that the client and server versions match.
    docker exec -e PGPASSWORD="${PG_PASSWORD}" "${PG_CONTAINER}" \
        pg_dump "${dump_args[@]}" > "${work}/${name}.sql"
else
    echo "error: neither pg_dump nor docker is available" >&2
    exit 1
fi

# Write to a temporary name and rename, so that an archive that exists is complete.
tar -C "${work}" -czf "${work}/${name}.tgz" "${name}.sql"
mv "${work}/${name}.tgz" "${archive}"

echo "${archive}"
