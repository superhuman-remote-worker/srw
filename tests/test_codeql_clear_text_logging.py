"""Secret values must never reach a log record.

Covers the sites CodeQL flags under ``py/clear-text-logging-sensitive-data``.
Each test asserts two things at once: the secret literal is ABSENT from every
emitted record, and the useful context (host, database name, provider name,
key count, slot, VM name, job id) is still PRESENT. A fix that simply
deletes the message would fail these as surely as one that leaks.

DSN parsing is tested separately from diagnostics. Logs identify databases
through independent configuration, and never copy a credential-bearing DSN.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# A password containing the characters that break naive DSN slicing.
AWKWARD_PASSWORD = "p/a@ss:w0rd"
SECRET_PASSWORD = "sup3rs3cret"
SAFE_DSN = f"postgresql://srw_user:{SECRET_PASSWORD}@db-host:5432/srwdb"
# The legacy DATABASE_URL fallback is operator-supplied and NOT url-quoted.
UNQUOTED_DSN = f"postgresql://srw_user:{AWKWARD_PASSWORD}@db-host:5432/srwdb"
# Same, with no trailing database path. This is the shape where slicing a DSN on
# "/" emits a password fragment as the "database name": with no path segment to
# land on, the slice lands inside the credentials instead.
UNQUOTED_DSN_NO_PATH = f"postgresql://srw_user:{AWKWARD_PASSWORD}@db-host:5432"


def _messages(caplog) -> str:
    """All emitted records, rendered exactly as a handler would format them."""
    return "\n".join(r.getMessage() for r in caplog.records)


# =============================================================================
# The shared DSN sanitiser
# =============================================================================


class TestDescribePostgresDsn:
    def test_returns_host_port_and_database(self):
        from shared.db_url import describe_postgres_dsn

        parts = describe_postgres_dsn(SAFE_DSN)

        assert parts["host"] == "db-host"
        assert parts["port"] == "5432"
        assert parts["database"] == "srwdb"

    def test_never_returns_the_password(self):
        from shared.db_url import describe_postgres_dsn

        parts = describe_postgres_dsn(SAFE_DSN)

        assert SECRET_PASSWORD not in repr(parts)
        assert not any("pass" in key.lower() for key in parts)

    def test_unquoted_fallback_password_never_leaks_into_the_database_name(self):
        """``split("/")[-1]`` on this DSN emits a password fragment."""
        from shared.db_url import describe_postgres_dsn

        parts = describe_postgres_dsn(UNQUOTED_DSN)

        assert parts["database"] == "srwdb"
        assert AWKWARD_PASSWORD not in repr(parts)
        for fragment in AWKWARD_PASSWORD.split("/"):
            assert fragment not in parts["database"]

    def test_strips_query_parameters_from_the_database_name(self):
        from shared.db_url import describe_postgres_dsn

        parts = describe_postgres_dsn(f"{SAFE_DSN}?sslmode=verify-full")

        assert parts["database"] == "srwdb"

    def test_tolerates_a_dsn_it_cannot_parse(self):
        from shared.db_url import describe_postgres_dsn

        parts = describe_postgres_dsn("")

        assert set(parts) == {"host", "port", "database"}
        assert SECRET_PASSWORD not in repr(parts)


# =============================================================================
# src/shared/db_url.py — the credential-bearing parts stay available to callers
# =============================================================================


class TestBuildPostgresUrlUnchanged:
    def test_dsn_still_carries_credentials_for_connecting(self, monkeypatch):
        """The sanitiser must not change what connects — only what is logged."""
        from shared.db_url import build_postgres_url

        monkeypatch.setenv("SRWTEST_USER", "srw_user")
        monkeypatch.setenv("SRWTEST_PASSWORD", SECRET_PASSWORD)
        monkeypatch.setenv("SRWTEST_HOST", "db-host")
        monkeypatch.setenv("SRWTEST_DB", "srwdb")

        assert SECRET_PASSWORD in build_postgres_url("SRWTEST")


# =============================================================================
# src/orchestrator/init.py
# =============================================================================


class TestInitPostgresLogging:
    @pytest.mark.asyncio
    async def test_database_name_logged_without_the_dsn_password(
        self, monkeypatch, caplog
    ):
        from orchestrator import init as init_mod

        monkeypatch.setenv("POSTGRES_USER", "srw_user")
        monkeypatch.setenv("POSTGRES_PASSWORD", SECRET_PASSWORD)
        monkeypatch.setenv("POSTGRES_HOST", "db-host")
        monkeypatch.setenv("POSTGRES_DB", "srwdb")

        fake_db = MagicMock()
        fake_db.create_database_if_not_exists = AsyncMock(return_value=True)
        fake_db.connect = AsyncMock(side_effect=RuntimeError("stop after logging"))
        fake_db.close = AsyncMock()

        with caplog.at_level(logging.INFO, logger=init_mod.logger.name):
            with patch(
                "orchestrator.database.postgres.PostgresDB", return_value=fake_db
            ):
                await init_mod.init_postgres()

        emitted = _messages(caplog)
        assert SECRET_PASSWORD not in emitted
        assert "srwdb" in emitted

    def test_pg_dump_progress_logs_database_without_the_password(
        self, monkeypatch, caplog, tmp_path
    ):
        from orchestrator import init as init_mod

        monkeypatch.setenv("POSTGRES_USER", "srw_user")
        monkeypatch.setenv("POSTGRES_PASSWORD", SECRET_PASSWORD)
        monkeypatch.setenv("POSTGRES_HOST", "db-host")
        monkeypatch.setenv("POSTGRES_DB", "srwdb")

        with caplog.at_level(logging.INFO, logger=init_mod.logger.name):
            with patch("subprocess.run", return_value=MagicMock(returncode=0)):
                init_mod.backup_postgres(tmp_path / "backup.dump")

        emitted = _messages(caplog)
        assert SECRET_PASSWORD not in emitted
        assert "srwdb" in emitted


# =============================================================================
# src/orchestrator/seed/llm_config.py
# =============================================================================


class TestSeedLlmConfigLogging:
    @pytest.mark.asyncio
    async def test_malformed_entry_never_prints_its_inline_api_key(self, caplog):
        """The one unambiguous clear-text leak: ``%r`` of the whole entry."""
        from orchestrator.seed import llm_config

        db = MagicMock()
        db.list_system_api_keys = AsyncMock(return_value=[])
        report = llm_config.SeedReport()

        with caplog.at_level(logging.DEBUG, logger=llm_config.logger.name):
            await llm_config._seed_api_keys(
                db,
                [{"apiKey": "sk-live-LEAKED-KEY-0123456789", "label": "oops"}],
                report,
            )

        emitted = _messages(caplog)
        assert "sk-live-LEAKED-KEY-0123456789" not in emitted
        assert "provider" in emitted.lower()

    @pytest.mark.asyncio
    async def test_endpoint_entry_never_prints_its_inline_api_key(self, caplog):
        from orchestrator.seed import llm_config

        db = MagicMock()
        db.list_system_llm_endpoints = AsyncMock(return_value=[])
        report = llm_config.SeedReport()

        with caplog.at_level(logging.DEBUG, logger=llm_config.logger.name):
            await llm_config._seed_endpoints(
                db, [{"apiKey": "sk-live-ENDPOINT-KEY-987654", "models": []}], report
            )

        assert "sk-live-ENDPOINT-KEY-987654" not in _messages(caplog)

    @pytest.mark.asyncio
    async def test_seeding_a_key_logs_a_constant_outcome_and_preserves_provider(
        self, caplog
    ):
        from orchestrator.seed import llm_config

        db = MagicMock()
        db.list_system_api_keys = AsyncMock(return_value=[])
        db.upsert_system_api_key = AsyncMock(return_value={"id": "1"})
        report = llm_config.SeedReport()

        with caplog.at_level(logging.DEBUG, logger=llm_config.logger.name):
            await llm_config._seed_api_keys(
                db,
                [{"provider": "openai", "apiKey": "sk-live-SEEDED-KEY-424242"}],
                report,
            )

        emitted = _messages(caplog)
        assert "sk-live-SEEDED-KEY-424242" not in emitted
        assert "openai" not in emitted
        assert "seeded system API-key entry" in emitted
        assert report.api_keys_seeded == ["openai"]
        assert db.upsert_system_api_key.await_args.kwargs["provider"] == "openai"


# =============================================================================
# src/orchestrator/database/postgres.py
# =============================================================================


class TestPostgresDbLogging:
    @pytest.mark.asyncio
    async def test_create_database_logs_name_without_the_dsn_password(self, caplog):
        from orchestrator.database import postgres as pg_mod

        db = pg_mod.PostgresDB(SAFE_DSN)
        conn = MagicMock()
        conn.fetchval = AsyncMock(return_value=None)
        conn.execute = AsyncMock()
        conn.close = AsyncMock()

        with caplog.at_level(logging.DEBUG, logger=pg_mod.logger.name):
            with patch.object(pg_mod.asyncpg, "connect", AsyncMock(return_value=conn)):
                created = await db.create_database_if_not_exists()

        assert created is True
        emitted = _messages(caplog)
        assert SECRET_PASSWORD not in emitted
        assert "configured database" in emitted

    @pytest.mark.asyncio
    async def test_unquoted_fallback_dsn_never_leaks_a_password_fragment(self, caplog):
        from orchestrator.database import postgres as pg_mod

        db = pg_mod.PostgresDB(UNQUOTED_DSN_NO_PATH)
        conn = MagicMock()
        conn.fetchval = AsyncMock(return_value=1)
        conn.execute = AsyncMock()
        conn.close = AsyncMock()

        with caplog.at_level(logging.DEBUG, logger=pg_mod.logger.name):
            with patch.object(pg_mod.asyncpg, "connect", AsyncMock(return_value=conn)):
                # No database path to parse: the DSN is rejected rather than
                # treating a fragment of the password as the database name.
                with pytest.raises(RuntimeError, match="database name"):
                    await db.create_database_if_not_exists()

        emitted = _messages(caplog)
        assert AWKWARD_PASSWORD not in emitted
        assert "a@ss:w0rd" not in emitted


# =============================================================================
# src/shared/runtime/llm/key_ring.py
# =============================================================================


class TestKeyRingLogging:
    def test_initialisation_logs_provider_and_count_not_the_keys(self, caplog):
        from shared.runtime.llm import key_ring as key_ring_mod

        keys = ["sk-first-key-abcdefghijklmnop", "sk-second-key-qrstuvwxyz012345"]

        with caplog.at_level(logging.DEBUG, logger=key_ring_mod.logger.name):
            key_ring_mod.KeyRing(keys, provider="anthropic")

        emitted = _messages(caplog)
        for key in keys:
            assert key not in emitted
        assert "anthropic" in emitted
        assert "2" in emitted

    def test_single_key_mode_does_not_emit_the_key(self, caplog):
        from shared.runtime.llm import key_ring as key_ring_mod

        with caplog.at_level(logging.DEBUG, logger=key_ring_mod.logger.name):
            key_ring_mod.KeyRing(["sk-only-key-abcdefghijklmnopqr"], provider="openai")

        emitted = _messages(caplog)
        assert "sk-only-key-abcdefghijklmnopqr" not in emitted
        assert "openai" in emitted

    def test_mask_key_does_not_disclose_most_of_a_short_key(self):
        from shared.runtime.llm.key_ring import _mask_key

        assert "shortk" not in _mask_key("shortkey")
        assert _mask_key("shortkey").endswith("...")


# =============================================================================
# src/shared/runtime/core/loader.py
# =============================================================================


class TestLoaderLogging:
    def test_dropped_override_keys_are_named_without_their_values(self, caplog):
        from shared.runtime.core import loader as loader_mod

        override = {
            f"{loader_mod.LOADER_OWNED_KEY_PREFIX}api_key": "sk-live-OVERRIDE-99887766",
            "temperature": 0.5,
        }

        with caplog.at_level(logging.DEBUG, logger=loader_mod.logger.name):
            cleaned = loader_mod.strip_loader_owned_keys(override)

        emitted = _messages(caplog)
        assert "sk-live-OVERRIDE-99887766" not in emitted
        assert "api_key" in emitted
        assert "temperature" in cleaned


# =============================================================================
# src/vm_controller/controller.py
# =============================================================================


class TestVmControllerLogging:
    @pytest.mark.asyncio
    async def test_rootdisk_logs_use_the_job_derived_name(self, caplog):
        from vm_controller import controller as controller_mod

        auth_key = "tskey-auth-SECRET-VALUE-0123456789"
        job_id = "11111111-2222-4333-8444-555555555555"
        manifest = {
            "spec": {
                "dataVolumeTemplates": [
                    {
                        "metadata": {"name": controller_mod._rootdisk_name(job_id)},
                        "spec": {"source": {"pvc": {"name": "golden"}}},
                    }
                ],
                "template": {"spec": {"authKey": auth_key}},
            }
        }

        ctrl = controller_mod.VMController.__new__(controller_mod.VMController)
        ctrl._list_workspace_cleanup_carriers = AsyncMock(return_value=())
        dv = {"status": {"phase": "Succeeded"}}
        ctrl._get_dv = AsyncMock(return_value=dv)
        ctrl._exact_rootdisk_identity = AsyncMock(
            return_value=(dv, {}, "dv-uid", "pvc-uid")
        )
        ctrl._active_recovery_pins = AsyncMock(return_value=())
        ctrl._acquire_workspace_cleanup_reservation = AsyncMock(
            return_value={"admission_id": "cleanup-admission"}
        )

        with caplog.at_level(logging.DEBUG, logger=controller_mod.log.name):
            name = await ctrl._ensure_rootdisk(manifest, job_id)

        emitted = _messages(caplog)
        assert auth_key not in emitted
        assert name == controller_mod._rootdisk_name(job_id)
        assert job_id in emitted


@pytest.mark.parametrize(
    "dsn, expected",
    [
        ("postgresql://user@localhost/srwdb?password=p@ss/LEAKEDSECRET", "srwdb"),
        (
            "postgresql://user:pw@localhost/srwdb?application_name=admin@example.com",
            "srwdb",
        ),
        ("postgresql://user:pw@localhost/srw@prod", "srw@prod"),
        ("postgresql://user:pw@localhost/srw%20db", "srw db"),
        ("postgresql://user:pw@localhost?dbname=query_db", "query_db"),
        ("postgresql://user:pw@localhost?database=query_db", "query_db"),
        ("postgresql://user:pw@localhost?dbname=first&database=second", "first"),
    ],
)
def test_dsn_metadata_respects_component_boundaries(dsn, expected):
    from shared.db_url import describe_postgres_dsn

    parts = describe_postgres_dsn(dsn)
    assert parts["database"] == expected
    assert parts["host"] == "localhost"
    assert "LEAKEDSECRET" not in repr(parts)


def test_malformed_ipv6_description_is_empty():
    from shared.db_url import describe_postgres_dsn

    assert describe_postgres_dsn("postgresql://user:pw@[bad/db") == {
        "host": "",
        "port": "",
        "database": "",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dsn, expected",
    [
        ("postgresql://user@localhost/srwdb?password=p@ss/LEAKEDSECRET", "srwdb"),
        ("postgresql://user:pw@localhost/srw@prod", "srw@prod"),
        ("postgresql://user:pw@localhost/srw%22db?sslmode=require", 'srw"db'),
        ("postgresql://user:pw@localhost?dbname=query_db", "query_db"),
    ],
)
async def test_database_creation_uses_driver_database_not_log_label(
    dsn, expected, caplog
):
    from orchestrator.database import postgres as pg_mod

    db = pg_mod.PostgresDB(dsn)
    conn = MagicMock()
    conn.fetchval = AsyncMock(return_value=None)
    conn.execute = AsyncMock()
    conn.close = AsyncMock()
    with caplog.at_level(logging.INFO, logger=pg_mod.logger.name):
        with patch.object(
            pg_mod.asyncpg, "connect", AsyncMock(return_value=conn)
        ) as connect:
            assert await db.create_database_if_not_exists()
    conn.fetchval.assert_awaited_once_with(
        "SELECT 1 FROM pg_database WHERE datname = $1", expected
    )
    conn.execute.assert_awaited_once_with(
        'CREATE DATABASE "' + expected.replace('"', '""') + '"'
    )
    assert connect.call_args.kwargs["database"] == "postgres"
    assert "LEAKEDSECRET" not in _messages(caplog)
    assert "database" in _messages(caplog).lower()


def test_key_ring_logs_slots_without_any_key_prefix(caplog):
    from shared.runtime.llm.key_ring import KeyRing

    with caplog.at_level(logging.DEBUG):
        ring = KeyRing(["PRIVATEA-key-one", "PRIVATEB-key-two"], provider="openai")
        assert ring.rotate() == "PRIVATEB-key-two"
    text = _messages(caplog)
    assert "PRIVATEA" not in text and "PRIVATEB" not in text
    assert "openai" in text and "slot" in text


@pytest.mark.asyncio
async def test_malformed_seed_field_names_cannot_leak_secrets(caplog):
    from orchestrator.seed.llm_config import _seed_api_keys, SeedReport

    db = MagicMock()
    db.list_system_api_keys = AsyncMock(return_value=[])
    await _seed_api_keys(
        db, [{"apiKey": "SECRET-VALUE", "SECRET-FIELD": True}], SeedReport()
    )
    assert "SECRET" not in _messages(caplog)
    assert "apiKey" in _messages(caplog)


def test_missing_cloud_secret_warning_keeps_context_without_custom_reference(caplog):
    from orchestrator.services.cloud.config import warn_main_cloud_missing_secret_config

    secret_reference = "VAULT_REF_WITH_SECRET_VALUE"
    with caplog.at_level(logging.WARNING):
        warn_main_cloud_missing_secret_config(
            [{"field": "agent_password", "env_var": secret_reference}],
            logger=logging.getLogger("test.main-cloud"),
        )

    emitted = _messages(caplog)
    assert secret_reference not in emitted
    assert "active main cloud backend" in emitted
    assert "required secret environment configuration" in emitted
    assert "Helm/Vault" in emitted


@pytest.mark.asyncio
async def test_init_fallback_logs_configuration_source_not_query_password(
    monkeypatch, caplog
):
    from orchestrator import init as init_mod

    monkeypatch.delenv("POSTGRES_USER", raising=False)
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    monkeypatch.setenv(
        "DATABASE_URL", "postgresql://user@localhost/srwdb?password=p@ss/LEAKEDSECRET"
    )
    db = MagicMock()
    db.create_database_if_not_exists = AsyncMock(return_value=True)
    db.connect = AsyncMock(side_effect=RuntimeError("stop"))
    db.close = AsyncMock()
    with caplog.at_level(logging.INFO):
        with patch("orchestrator.database.postgres.PostgresDB", return_value=db):
            await init_mod.init_postgres()
    assert "LEAKEDSECRET" not in _messages(caplog)
    assert "DATABASE_URL" in _messages(caplog)
