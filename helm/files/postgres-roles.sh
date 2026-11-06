#!/usr/bin/env bash
# Keeps SRW's application role apart from the bootstrap superuser on the bundled
# Postgres StatefulSets (app, pgvector, audit). Operator view: helm/README.md,
# "Bundled Postgres roles".
#
# End state on every server:
#   * POSTGRES_USER (`postgres`) is the bootstrap superuser (OID 10). Its
#     password lives in the dedicated superuser Secret, which no app pod reads.
#   * SRW_APP_USER (`srw`) is LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
#     NOREPLICATION NOBYPASSRLS and owns SRW_APP_DB and every schema object in
#     it. Extensions stay owned by the superuser, as on CloudNativePG.
#   * SRW_DB_EXTENSIONS exist in SRW_APP_DB, created by the superuser before
#     the orchestrator's migrations run (`vector` is not a trusted extension).
#
# One script, two callers:
#   initdb   the image's docker-entrypoint runs it from
#            /docker-entrypoint-initdb.d on a fresh volume, with its own
#            socket-only temporary server up. It creates the app role, the
#            database and the extensions.
#   upgrade  the StatefulSet's `postgres-roles` init container runs it on every
#            pod start. A fresh volume is left to initdb. Otherwise it starts a
#            socket-only temporary server, converts a legacy volume (one where
#            the app role IS the bootstrap superuser), re-applies the end state
#            and stops the server again.
#
# Why a legacy volume needs more than ALTER ROLE srw NOSUPERUSER: there the app
# role is OID 10. PostgreSQL 16 refuses to demote it ("The bootstrap user must
# have the SUPERUSER attribute"), and on 15, where it is allowed, the demoted
# role would still own pg_catalog, every built-in function and the template
# databases. So the conversion keeps OID 10 as the superuser under the new name
# and hands SRW's objects to a new, ordinary role that then takes the app name:
#   1. as OID 10: create the ordinary role `srw_role_split_owner` and the
#      helper superuser `srw_role_split_admin` (no password; it can only log in
#      over this pod's Unix socket);
#   2. as OID 10, in SRW_APP_DB, one transaction: every schema, relation,
#      routine and type OID 10 owns there, and the database itself, go to
#      `srw_role_split_owner` (REASSIGN OWNED refuses the bootstrap role);
#   3. as the helper, one transaction: rename OID 10 to POSTGRES_USER, rename
#      `srw_role_split_owner` to SRW_APP_USER, set both passwords;
#   4. as POSTGRES_USER: drop the helper, then the ordinary end-state pass.
# Every step is idempotent and the state is read from the catalog, so a run
# killed anywhere is finished by the next pod start. OID 10 stays a superuser
# throughout, and its password is either the old app password (until step 3
# commits) or the superuser Secret's (after), besides the Unix socket that the
# image's pg_hba.conf trusts.
#
# Passwords reach psql through the environment (\getenv), never argv.
set -Eeuo pipefail

log() { printf 'postgres-roles: %s\n' "$*"; }
die() {
    printf 'postgres-roles: ERROR: %s\n' "$*" >&2
    exit 1
}

PENDING_OWNER=srw_role_split_owner
SPLIT_ADMIN=srw_role_split_admin

# psql over the pod-local Unix socket. Empty PGHOST/PGHOSTADDR force the
# socket the way docker-entrypoint.sh's docker_process_sql does.
sql() {
    local user=$1 db=$2
    shift 2
    PGHOST='' PGHOSTADDR='' psql -X -q -v ON_ERROR_STOP=1 --no-password \
        --username "$user" --dbname "$db" "$@"
}

check_inputs() {
    : "${POSTGRES_USER:?POSTGRES_USER (the bootstrap superuser) is not set}"
    : "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD (the superuser password) is not set}"
    : "${SRW_APP_USER:?SRW_APP_USER is not set}"
    : "${SRW_APP_PASSWORD:?SRW_APP_PASSWORD is not set}"
    : "${SRW_APP_DB:?SRW_APP_DB is not set}"
    local name
    for name in "$POSTGRES_USER" "$SRW_APP_USER"; do
        [[ $name =~ ^[a-z_][a-z0-9_]{0,62}$ ]] ||
            die "role name '$name' must match ^[a-z_][a-z0-9_]{0,62}\$"
    done
    [[ $POSTGRES_USER != "$SRW_APP_USER" ]] ||
        die "the application role and the superuser are both '$SRW_APP_USER'; they must differ"
    case "$SRW_APP_USER" in
    "$PENDING_OWNER" | "$SPLIT_ADMIN") die "'$SRW_APP_USER' is reserved for the role split" ;;
    esac
}

# Name of the bootstrap superuser (OID 10). The socket is trusted, so any
# existing role can ask; the two candidates are the only names it can have.
bootstrap_role() {
    local candidate name
    for candidate in "$POSTGRES_USER" "$SRW_APP_USER"; do
        if name="$(sql "$candidate" postgres -At \
            -c 'SELECT rolname FROM pg_catalog.pg_roles WHERE oid = 10' 2>/dev/null)"; then
            printf '%s' "$name"
            return 0
        fi
    done
    return 1
}

# psql interpolates :'variables' in scripts, not in -c strings.
role_exists() { # <connect-as> <role>
    [[ "$(sql "$1" postgres -At -v role="$2" \
        <<<"SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname = :'role'")" == 1 ]]
}

database_exists() { # <connect-as> <database>
    [[ "$(sql "$1" postgres -At -v db="$2" \
        <<<"SELECT count(*) FROM pg_catalog.pg_database WHERE datname = :'db'")" == 1 ]]
}

# Steps 1-3 above. Runs only while OID 10 is still named SRW_APP_USER.
convert_legacy() {
    log "converting: '$SRW_APP_USER' is the bootstrap superuser; it becomes '$POSTGRES_USER' and a new ordinary '$SRW_APP_USER' takes over $SRW_APP_DB"
    if role_exists "$SRW_APP_USER" "$POSTGRES_USER"; then
        die "a role named '$POSTGRES_USER' already exists beside the bootstrap superuser '$SRW_APP_USER'; rename or drop it (kubectl exec into this pod, psql -U $SRW_APP_USER) and restart the pod"
    fi
    split_create_roles
    split_hand_over
    split_rename
    log "converted: '$POSTGRES_USER' is the bootstrap superuser, '$SRW_APP_USER' is an ordinary role"
}

# Step 1, as OID 10. The new owner stays NOLOGIN until step 3 gives it the
# app password.
split_create_roles() {
    sql "$SRW_APP_USER" postgres <<'SQL'
SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'srw_role_split_owner') AS missing \gset
\if :missing
CREATE ROLE srw_role_split_owner WITH NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
\endif
SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'srw_role_split_admin') AS missing \gset
\if :missing
CREATE ROLE srw_role_split_admin WITH LOGIN SUPERUSER;
\endif
SQL
}

# Step 2, as OID 10, one transaction. Skipped when the database does not exist
# yet: there is nothing to hand over.
split_hand_over() {
    if ! database_exists "$SRW_APP_USER" "$SRW_APP_DB"; then
        log "database '$SRW_APP_DB' does not exist yet; nothing to hand over"
        return 0
    fi
    sql "$SRW_APP_USER" "$SRW_APP_DB" <<'SQL'
BEGIN;
DO $transfer$
DECLARE
    target CONSTANT name := 'srw_role_split_owner';
    r record;
    moved integer := 0;
BEGIN
    -- Schemas. `public` too when OID 10 owns it (databases from before
    -- PostgreSQL 15); from 15 on it belongs to pg_database_owner and follows
    -- the database's owner by itself.
    FOR r IN
        SELECT n.nspname
        FROM pg_catalog.pg_namespace n
        WHERE n.nspowner = 10
          AND n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema'
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                          WHERE d.classid = 'pg_catalog.pg_namespace'::regclass
                            AND d.objid = n.oid AND d.deptype = 'e')
    LOOP
        EXECUTE format('ALTER SCHEMA %I OWNER TO %I', r.nspname, target);
        moved := moved + 1;
    END LOOP;

    -- Relations. Each partition is its own relation and is listed itself.
    -- Indexes, TOAST tables and sequences owned by a column (serial,
    -- identity) follow their table.
    FOR r IN
        SELECT c.oid::regclass AS rel, c.relkind
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relowner = 10
          AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
          AND n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema'
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                          WHERE d.classid = 'pg_catalog.pg_class'::regclass
                            AND d.objid = c.oid AND d.deptype = 'e')
          AND NOT (c.relkind = 'S' AND EXISTS (
                SELECT 1 FROM pg_catalog.pg_depend d
                WHERE d.classid = 'pg_catalog.pg_class'::regclass
                  AND d.objid = c.oid
                  AND d.refclassid = 'pg_catalog.pg_class'::regclass
                  AND d.deptype IN ('a', 'i')))
        ORDER BY c.oid
    LOOP
        EXECUTE format('ALTER %s %s OWNER TO %I',
            CASE r.relkind
                WHEN 'v' THEN 'VIEW'
                WHEN 'm' THEN 'MATERIALIZED VIEW'
                WHEN 'f' THEN 'FOREIGN TABLE'
                WHEN 'S' THEN 'SEQUENCE'
                ELSE 'TABLE'
            END, r.rel, target);
        moved := moved + 1;
    END LOOP;

    -- Functions, procedures and aggregates.
    FOR r IN
        SELECT p.oid::regprocedure AS routine
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE p.proowner = 10
          AND n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema'
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                          WHERE d.classid = 'pg_catalog.pg_proc'::regclass
                            AND d.objid = p.oid AND d.deptype = 'e')
    LOOP
        EXECUTE format('ALTER ROUTINE %s OWNER TO %I', r.routine, target);
        moved := moved + 1;
    END LOOP;

    -- Enums, domains, ranges and their multiranges, standalone composites
    -- and base types. An array type follows its element and a table's row
    -- type its table; a multirange does not follow its range (PostgreSQL 15).
    FOR r IN
        SELECT t.oid::regtype AS typ, t.typtype
        FROM pg_catalog.pg_type t
        JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typowner = 10
          AND t.typtype IN ('b', 'c', 'd', 'e', 'r', 'm')
          AND n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema'
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_type e WHERE e.typarray = t.oid)
          AND (t.typtype <> 'c' OR EXISTS (SELECT 1 FROM pg_catalog.pg_class c
                                          WHERE c.oid = t.typrelid AND c.relkind = 'c'))
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                          WHERE d.classid = 'pg_catalog.pg_type'::regclass
                            AND d.objid = t.oid AND d.deptype = 'e')
    LOOP
        EXECUTE format('ALTER %s %s OWNER TO %I',
            CASE WHEN r.typtype = 'd' THEN 'DOMAIN' ELSE 'TYPE' END, r.typ, target);
        moved := moved + 1;
    END LOOP;

    -- Extended statistics (CREATE STATISTICS).
    FOR r IN
        SELECT format('%I.%I', n.nspname, s.stxname) AS stat
        FROM pg_catalog.pg_statistic_ext s
        JOIN pg_catalog.pg_namespace n ON n.oid = s.stxnamespace
        WHERE s.stxowner = 10
    LOOP
        EXECUTE format('ALTER STATISTICS %s OWNER TO %I', r.stat, target);
        moved := moved + 1;
    END LOOP;

    EXECUTE format('ALTER DATABASE %I OWNER TO %I', current_database(), target);
    RAISE NOTICE 'postgres-roles: % objects in % now belong to the application role',
        moved, current_database();

    -- Kinds SRW's migrations never create. Left with the superuser, and named,
    -- so an operator who made one by hand knows to look.
    FOR r IN
        SELECT kind, count(*) AS n FROM (
            SELECT 'operator' AS kind FROM pg_catalog.pg_operator o
                WHERE o.oprowner = 10 AND o.oid >= 16384
                  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d WHERE d.classid = 'pg_catalog.pg_operator'::regclass AND d.objid = o.oid AND d.deptype = 'e')
            UNION ALL SELECT 'operator class' FROM pg_catalog.pg_opclass o
                WHERE o.opcowner = 10 AND o.oid >= 16384
                  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d WHERE d.classid = 'pg_catalog.pg_opclass'::regclass AND d.objid = o.oid AND d.deptype = 'e')
            UNION ALL SELECT 'operator family' FROM pg_catalog.pg_opfamily o
                WHERE o.opfowner = 10 AND o.oid >= 16384
                  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d WHERE d.classid = 'pg_catalog.pg_opfamily'::regclass AND d.objid = o.oid AND d.deptype = 'e')
            UNION ALL SELECT 'collation' FROM pg_catalog.pg_collation WHERE collowner = 10 AND oid >= 16384
            UNION ALL SELECT 'conversion' FROM pg_catalog.pg_conversion WHERE conowner = 10 AND oid >= 16384
            UNION ALL SELECT 'text search configuration' FROM pg_catalog.pg_ts_config WHERE cfgowner = 10 AND oid >= 16384
            UNION ALL SELECT 'text search dictionary' FROM pg_catalog.pg_ts_dict WHERE dictowner = 10 AND oid >= 16384
            UNION ALL SELECT 'foreign server' FROM pg_catalog.pg_foreign_server WHERE srvowner = 10
            UNION ALL SELECT 'publication' FROM pg_catalog.pg_publication WHERE pubowner = 10
            UNION ALL SELECT 'event trigger' FROM pg_catalog.pg_event_trigger WHERE evtowner = 10
            UNION ALL SELECT 'large object' FROM pg_catalog.pg_largeobject_metadata WHERE lomowner = 10
            UNION ALL SELECT 'default privileges entry' FROM pg_catalog.pg_default_acl WHERE defaclrole = 10
        ) leftovers GROUP BY kind
    LOOP
        RAISE WARNING 'postgres-roles: % object(s) of kind "%" in % stay owned by the superuser',
            r.n, r.kind, current_database();
    END LOOP;
END
$transfer$;
COMMIT;
SQL
}

# Step 3, one transaction, from a session that is not OID 10: a role cannot
# rename itself.
split_rename() {
    sql "$SPLIT_ADMIN" postgres <<'SQL'
\getenv super_user POSTGRES_USER
\getenv super_password POSTGRES_PASSWORD
\getenv app_user SRW_APP_USER
\getenv app_password SRW_APP_PASSWORD
SELECT count(*) > 0 AS has_settings FROM pg_catalog.pg_db_role_setting WHERE setrole = 10 \gset
\if :has_settings
\warn 'postgres-roles: per-role settings (ALTER ROLE ... SET) stay with the superuser; re-apply any the application needs to the new application role'
\endif
BEGIN;
ALTER ROLE :"app_user" RENAME TO :"super_user";
ALTER ROLE srw_role_split_owner RENAME TO :"app_user";
ALTER ROLE :"app_user" WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
    PASSWORD :'app_password';
ALTER ROLE :"super_user" WITH SUPERUSER LOGIN PASSWORD :'super_password';
COMMIT;
SQL
}

# The end state, applied on every start. Idempotent; as POSTGRES_USER.
ensure() {
    sql "$POSTGRES_USER" postgres <<'SQL'
\getenv super_user POSTGRES_USER
\getenv super_password POSTGRES_PASSWORD
\getenv app_user SRW_APP_USER
\getenv app_password SRW_APP_PASSWORD
\getenv app_db SRW_APP_DB
BEGIN;
SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'app_user') AS missing \gset
\if :missing
CREATE ROLE :"app_user" WITH LOGIN;
\endif
-- Both passwords follow their Secrets: rotating one is a Secret edit and a
-- restart of this pod.
ALTER ROLE :"app_user" WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
    PASSWORD :'app_password';
ALTER ROLE :"super_user" WITH SUPERUSER LOGIN PASSWORD :'super_password';
DROP ROLE IF EXISTS srw_role_split_admin;
COMMIT;
SELECT format('CREATE DATABASE %I OWNER %I', :'app_db', :'app_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datname = :'app_db') \gexec
SELECT format('ALTER DATABASE %I OWNER TO %I', :'app_db', :'app_user')
FROM pg_catalog.pg_database
WHERE datname = :'app_db'
  AND datdba <> (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = :'app_user') \gexec
SQL

    # The privileged step migrations cannot do as the application role. Their
    # own CREATE EXTENSION IF NOT EXISTS then finds the extension and stops.
    if [[ -n "${SRW_DB_EXTENSIONS// /}" ]]; then
        sql "$POSTGRES_USER" "$SRW_APP_DB" <<'SQL'
\getenv extensions SRW_DB_EXTENSIONS
SELECT format('CREATE EXTENSION IF NOT EXISTS %I WITH SCHEMA public', extension)
FROM unnest(string_to_array(btrim(:'extensions'), ' ')) AS extension
WHERE extension <> '' \gexec
SQL
    fi
    log "ready: '$SRW_APP_USER' owns $SRW_APP_DB without superuser rights; extensions: ${SRW_DB_EXTENSIONS:-none}"
}

reconcile() {
    local boot
    boot="$(bootstrap_role)" ||
        die "cannot connect as '$POSTGRES_USER' or '$SRW_APP_USER' over the local socket"
    if [[ $boot == "$SRW_APP_USER" ]]; then
        convert_legacy
    elif [[ $boot != "$POSTGRES_USER" ]]; then
        die "the bootstrap superuser is '$boot', expected '$POSTGRES_USER' or '$SRW_APP_USER'"
    fi
    ensure
}

upgrade() {
    if [[ ! -s "$PGDATA/PG_VERSION" ]]; then
        log "fresh volume: the image's initdb creates the roles"
        return 0
    fi
    # Socket only: no client can connect while roles and owners change.
    # max_locks_per_transaction: step 2 locks every relation and index of the
    # database in one transaction.
    pg_ctl -D "$PGDATA" -w -t "${SRW_DB_ROLES_START_TIMEOUT:-3600}" \
        -o "-c listen_addresses='' -c max_locks_per_transaction=512" start
    trap 'pg_ctl -D "$PGDATA" -m fast -w stop || true' EXIT
    reconcile
    trap - EXIT
    pg_ctl -D "$PGDATA" -m fast -w -t 600 stop
}

main() {
    local mode=${1:-initdb}
    check_inputs
    case "$mode" in
    upgrade)
        if [[ "$(id -u)" == 0 ]]; then
            # pg_ctl refuses root; drop privileges the way docker-entrypoint.sh does.
            if command -v gosu >/dev/null; then exec gosu postgres bash "${BASH_SOURCE[0]}" "$@"; fi
            if command -v su-exec >/dev/null; then exec su-exec postgres bash "${BASH_SOURCE[0]}" "$@"; fi
            die "running as root without gosu or su-exec to drop to the postgres user"
        fi
        upgrade
        ;;
    initdb) reconcile ;;
    *) die "unknown mode '$mode' (expected initdb or upgrade)" ;;
    esac
}

# docker-entrypoint.sh executes an executable *.sh from initdb.d and sources
# any other, with its own arguments in "$@"; both mean initdb. Tests set
# SRW_DB_ROLES_SOURCE_ONLY to load the functions without running anything.
if [[ -z "${SRW_DB_ROLES_SOURCE_ONLY:-}" ]]; then
    if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
        main "$@"
    else
        main initdb
    fi
fi
