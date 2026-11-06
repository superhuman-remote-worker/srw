"""helm/files/postgres-roles.sh against real PostgreSQL 15 (pgvector) and 16.

The bundled StatefulSets used to run SRW's role as the image's bootstrap
superuser (OID 10). The script makes `postgres` that superuser and SRW's role an
ordinary owner: on a fresh volume at initdb, and on an existing one by
handing every object to a new ordinary role and renaming both, because OID 10
cannot be demoted on 16 and would still own the catalog on 15.

Each case runs the script inside the database container over the Unix socket,
as the image's initdb and the chart's init container do. The init container's
own pg_ctl wrapper (``upgrade`` mode) needs a stopped server, so it is covered
by the podman and k3d runs recorded in the commit, not here.

Issue: knowledge-base/knowledge/issues/bundled_postgres_app_role_is_superuser.md
"""

from __future__ import annotations

import io
import tarfile
from pathlib import Path

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "helm" / "files" / "postgres-roles.sh"

APP_DB = "appdb"
APP_PASSWORD = "app-password-after-split"
LEGACY_PASSWORD = "legacy-app-password"
SUPER_PASSWORD = "superuser-password-after-split"

# Every ownable kind the hand-over has a branch for, created by the legacy
# superuser exactly as SRW's migrations (and hand edits) would have.
LEGACY_OBJECTS = """
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE SCHEMA extra;
CREATE TABLE extra.ident (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, s serial, note text);
CREATE TABLE public.plain (id uuid DEFAULT uuid_generate_v4() PRIMARY KEY);
CREATE SEQUENCE extra.standalone_seq;
CREATE VIEW extra.v AS SELECT id FROM extra.ident;
CREATE MATERIALIZED VIEW extra.mv AS SELECT id FROM extra.ident;
CREATE DOMAIN extra.positive AS integer CHECK (VALUE > 0);
CREATE TYPE extra.pair AS (a int, b int);
CREATE TYPE extra.mood AS ENUM ('ok', 'meh');
CREATE TYPE extra.floatrange AS RANGE (subtype = float8);
CREATE FUNCTION extra.add(a int, b int) RETURNS int LANGUAGE sql AS 'SELECT a + b';
CREATE FUNCTION extra.definer() RETURNS bigint LANGUAGE sql SECURITY DEFINER
    AS 'SELECT count(*) FROM extra.ident';
CREATE AGGREGATE extra.mysum(int) (sfunc = extra.add, stype = int, initcond = '0');
CREATE PROCEDURE extra.noop() LANGUAGE sql AS 'SELECT 1';
CREATE TABLE extra.parted (k int, v text) PARTITION BY RANGE (k);
CREATE TABLE extra.parted_1 PARTITION OF extra.parted FOR VALUES FROM (0) TO (10);
CREATE TABLE extra.parted_2 PARTITION OF extra.parted FOR VALUES FROM (10) TO (20);
CREATE STATISTICS extra.st ON k, v FROM extra.parted_1;
"""

# Objects in the application's schemas the superuser still owns, other than
# extension members and the array types of extension types.
SUPERUSER_LEFTOVERS = """
WITH ext AS (SELECT classid, objid FROM pg_depend WHERE deptype = 'e'),
app_ns AS (SELECT oid FROM pg_namespace
           WHERE nspname NOT LIKE 'pg\\_%' AND nspname <> 'information_schema')
SELECT 'schema ' || nspname FROM pg_namespace
    WHERE nspowner = 10 AND oid IN (SELECT oid FROM app_ns)
UNION ALL SELECT 'relation ' || c.oid::regclass::text FROM pg_class c
    WHERE relowner = 10 AND relnamespace IN (SELECT oid FROM app_ns)
      AND NOT EXISTS (SELECT 1 FROM ext WHERE classid = 'pg_class'::regclass AND objid = c.oid)
UNION ALL SELECT 'routine ' || p.oid::regprocedure::text FROM pg_proc p
    WHERE proowner = 10 AND pronamespace IN (SELECT oid FROM app_ns)
      AND NOT EXISTS (SELECT 1 FROM ext WHERE classid = 'pg_proc'::regclass AND objid = p.oid)
UNION ALL SELECT 'type ' || t.oid::regtype::text FROM pg_type t
    WHERE typowner = 10 AND typnamespace IN (SELECT oid FROM app_ns)
      AND NOT EXISTS (SELECT 1 FROM ext WHERE classid = 'pg_type'::regclass AND objid = t.oid)
      AND NOT EXISTS (SELECT 1 FROM pg_type e JOIN ext ON ext.classid = 'pg_type'::regclass
                      AND ext.objid = e.oid WHERE e.typarray = t.oid)
UNION ALL SELECT 'statistics ' || stxname FROM pg_statistic_ext WHERE stxowner = 10
"""

# A fingerprint of roles and ownership, to prove a second run changes nothing.
CATALOG_FINGERPRINT = """
SELECT string_agg(item, ',' ORDER BY item) FROM (
    SELECT rolname || ':' || oid || ':' || rolsuper || rolcreaterole || rolcreatedb
           || rolreplication || rolbypassrls || rolcanlogin AS item
        FROM pg_roles WHERE rolname !~ '^pg_'
    UNION ALL SELECT 'db:' || datname || ':' || datdba FROM pg_database
    UNION ALL SELECT 'rel:' || relowner || ':' || count(*) FROM pg_class GROUP BY relowner
    UNION ALL SELECT 'proc:' || proowner || ':' || count(*) FROM pg_proc GROUP BY proowner
    UNION ALL SELECT 'type:' || typowner || ':' || count(*) FROM pg_type GROUP BY typowner
    UNION ALL SELECT 'ext:' || extname || ':' || extowner FROM pg_extension
) items
"""

IMAGES = {
    # The app and pgvector StatefulSets' image, and the audit one's.
    "pg15-pgvector": ("pgvector/pgvector:pg15", "uuid-ossp vector pg_trgm"),
    "pg16": ("postgres:16", "uuid-ossp btree_gist"),
}


def _start(image: str, user: str, password: str, dbname: str) -> PostgresContainer:
    container = PostgresContainer(image, username=user, password=password, dbname=dbname)
    try:
        container.start()
    except Exception as exc:  # pragma: no cover - depends on the host
        pytest.skip(f"local Postgres container unavailable: {exc}")
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as tar:
        data = SCRIPT.read_bytes()
        info = tarfile.TarInfo("postgres-roles.sh")
        info.size = len(data)
        info.mode = 0o555
        tar.addfile(info, io.BytesIO(data))
    container.get_wrapped_container().put_archive("/tmp", archive.getvalue())
    return container


def _script(container: PostgresContainer, extensions: str, *steps: str) -> tuple[int, str]:
    """Run the script as the image's initdb does: as postgres, server up.

    With ``steps``, source it and call only those functions, which leaves the
    database exactly where a pod killed after them would.
    """
    environment = {
        "POSTGRES_USER": "postgres",
        "POSTGRES_PASSWORD": SUPER_PASSWORD,
        "SRW_APP_USER": "srw",
        "SRW_APP_PASSWORD": APP_PASSWORD,
        "SRW_APP_DB": APP_DB,
        "SRW_DB_EXTENSIONS": extensions,
    }
    if steps:
        environment["SRW_DB_ROLES_SOURCE_ONLY"] = "1"
        command = [
            "bash",
            "-c",
            "set -Eeuo pipefail; . /tmp/postgres-roles.sh; check_inputs; " + "; ".join(steps),
        ]
    else:
        command = ["bash", "/tmp/postgres-roles.sh", "initdb"]
    result = container.get_wrapped_container().exec_run(
        command, user="postgres", environment=environment
    )
    return result.exit_code, result.output.decode()


async def _connect(container: PostgresContainer, user: str, password: str, database: str):
    return await asyncpg.connect(
        host=container.get_container_host_ip(),
        port=int(container.get_exposed_port(5432)),
        user=user,
        password=password,
        database=database,
    )


async def _assert_split(container: PostgresContainer, extensions: str) -> str:
    """The end state; returns the catalog fingerprint."""
    admin = await _connect(container, "postgres", SUPER_PASSWORD, APP_DB)
    try:
        roles = {
            row["rolname"]: row
            for row in await admin.fetch(
                "SELECT oid, rolname, rolsuper, rolcreaterole, rolcreatedb, rolreplication,"
                " rolbypassrls, rolcanlogin FROM pg_roles WHERE rolname !~ '^pg_'"
            )
        }
        assert set(roles) == {"postgres", "srw"}, "the split's helper roles must be gone"
        assert roles["postgres"]["oid"] == 10 and roles["postgres"]["rolsuper"]
        app = roles["srw"]
        assert app["rolcanlogin"]
        assert not any(
            app[attribute]
            for attribute in (
                "rolsuper",
                "rolcreaterole",
                "rolcreatedb",
                "rolreplication",
                "rolbypassrls",
            )
        )
        assert (
            await admin.fetchval(
                "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = $1", APP_DB
            )
            == "srw"
        )
        installed = {
            row["extname"]: row["owner"]
            for row in await admin.fetch(
                "SELECT extname, pg_get_userbyid(extowner) AS owner FROM pg_extension"
            )
        }
        for extension in extensions.split():
            assert installed.get(extension) == "postgres", extension
        assert [row[0] for row in await admin.fetch(SUPERUSER_LEFTOVERS)] == []
        fingerprint = await admin.fetchval(CATALOG_FINGERPRINT)
    finally:
        await admin.close()

    owner = await _connect(container, "srw", APP_PASSWORD, APP_DB)
    try:
        assert not await owner.fetchval(
            "SELECT rolsuper FROM pg_roles WHERE rolname = current_user"
        )
        # What migrations do as the owner, after the split.
        async with owner.transaction():
            for extension in extensions.split():
                await owner.execute(f'CREATE EXTENSION IF NOT EXISTS "{extension}"')
            await owner.execute("CREATE TABLE post_split (id int)")
            await owner.execute("DROP TABLE post_split")
            if await owner.fetchval("SELECT to_regclass('extra.ident') IS NOT NULL"):
                await owner.execute("ALTER TABLE extra.ident ADD COLUMN post_split int")
                await owner.execute("CREATE INDEX ON extra.ident (post_split)")
                await owner.execute(
                    "CREATE TABLE extra.parted_3 PARTITION OF extra.parted"
                    " FOR VALUES FROM (20) TO (30)"
                )
                await owner.execute(
                    "CREATE OR REPLACE FUNCTION extra.add(a int, b int) RETURNS int"
                    " LANGUAGE sql AS 'SELECT a + b + 0'"
                )
                assert await owner.fetchval("SELECT extra.definer()") == 0
            raise _Rollback
    except _Rollback:
        pass
    finally:
        await owner.close()

    # The superuser-only escape hatches are closed for the application role.
    owner = await _connect(container, "srw", APP_PASSWORD, APP_DB)
    try:
        for statement in (
            "COPY (SELECT 1) TO PROGRAM 'true'",
            "ALTER SYSTEM SET work_mem = '8MB'",
            "CREATE ROLE escalate LOGIN",
            "CREATE DATABASE escalate",
        ):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await owner.execute(statement)
    finally:
        await owner.close()

    with pytest.raises(asyncpg.InvalidPasswordError):
        await _connect(container, "srw", LEGACY_PASSWORD, APP_DB)
    return fingerprint


class _Rollback(Exception):
    pass


@pytest.mark.parametrize("image_key", sorted(IMAGES))
@pytest.mark.parametrize(
    "stop_after",
    [
        pytest.param((), id="uninterrupted"),
        pytest.param(("split_create_roles",), id="killed-after-step-1"),
        pytest.param(("split_create_roles", "split_hand_over"), id="killed-after-step-2"),
        pytest.param(
            ("split_create_roles", "split_hand_over", "split_rename"),
            id="killed-after-step-3",
        ),
    ],
)
@pytest.mark.asyncio
async def test_legacy_volume_converts_and_resumes(image_key, stop_after):
    image, extensions = IMAGES[image_key]
    container = _start(image, "srw", LEGACY_PASSWORD, APP_DB)
    try:
        legacy = await _connect(container, "srw", LEGACY_PASSWORD, APP_DB)
        try:
            assert await legacy.fetchval("SELECT rolsuper FROM pg_roles WHERE oid = 10")
            assert await legacy.fetchval("SELECT rolname FROM pg_roles WHERE oid = 10") == "srw"
            await legacy.execute(LEGACY_OBJECTS)
        finally:
            await legacy.close()

        if stop_after:
            code, output = _script(container, extensions, *stop_after)
            assert code == 0, output

        code, output = _script(container, extensions)
        assert code == 0, output
        assert "ready:" in output
        first = await _assert_split(container, extensions)

        # Every pod start runs it again: nothing may change.
        code, output = _script(container, extensions)
        assert code == 0, output
        assert "converting" not in output
        assert await _assert_split(container, extensions) == first
    finally:
        container.stop()


@pytest.mark.parametrize("image_key", sorted(IMAGES))
@pytest.mark.asyncio
async def test_fresh_volume_creates_ordinary_owner_and_extensions(image_key):
    """The initdb path: the image made `postgres` the superuser, nothing else."""
    image, extensions = IMAGES[image_key]
    container = _start(image, "postgres", SUPER_PASSWORD, "postgres")
    try:
        code, output = _script(container, extensions)
        assert code == 0, output
        first = await _assert_split(container, extensions)
        code, output = _script(container, extensions)
        assert code == 0, output
        assert await _assert_split(container, extensions) == first
    finally:
        container.stop()


@pytest.mark.asyncio
async def test_refuses_a_second_role_named_like_the_superuser():
    """A hand-made `postgres` beside a legacy OID 10 stops the pod, loudly."""
    image, extensions = IMAGES["pg16"]
    container = _start(image, "srw", LEGACY_PASSWORD, APP_DB)
    try:
        legacy = await _connect(container, "srw", LEGACY_PASSWORD, APP_DB)
        try:
            await legacy.execute("CREATE ROLE postgres LOGIN")
        finally:
            await legacy.close()
        code, output = _script(container, extensions)
        assert code != 0
        assert "already exists beside the bootstrap superuser" in output
        legacy = await _connect(container, "srw", LEGACY_PASSWORD, APP_DB)
        try:
            # Nothing changed: the legacy role is still the superuser.
            assert await legacy.fetchval("SELECT rolname FROM pg_roles WHERE oid = 10") == "srw"
        finally:
            await legacy.close()
    finally:
        container.stop()
