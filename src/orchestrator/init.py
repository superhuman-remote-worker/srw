#!/usr/bin/env python3
"""Database initialization for the orchestrator.

This module provides database initialization functionality for:
- PostgreSQL App DB (jobs, agents, requirements, datasources, builder)
- PostgreSQL Vector DB (citations, sources, memories, knowledge_index)

Can be used standalone or imported by the root init.py.

Usage:
    # Initialize databases (idempotent)
    python -m orchestrator.init

    # Force reset (delete all data, recreate)
    python -m orchestrator.init --force-reset

    # Skip specific databases
    python -m orchestrator.init --skip-postgres

    # Verify connectivity only
    python -m orchestrator.init --verify

    # Backup/restore
    python -m orchestrator.init --backup /path/to/backup
    python -m orchestrator.init --restore /path/to/backup
"""

import argparse
import asyncio
import hashlib
import logging
import os
import secrets
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import yaml

# Resource paths are relative to the editable application checkout.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ORCHESTRATOR_DIR = Path(__file__).parent

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for initialization."""
    if verbose and not os.getenv("DEBUG_ALL"):
        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        for namespace in ("__main__", "orchestrator", "shared"):
            logging.getLogger(namespace).setLevel(logging.DEBUG)
    else:
        level = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(
            level=level,
            format="%(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


# =============================================================================
# PostgreSQL Initialization
# =============================================================================


def get_postgres_connection_string() -> str:
    """Get PostgreSQL connection string from environment.

    Prefers the discrete-parts layout (POSTGRES_USER/PASSWORD/HOST/PORT/DB)
    so passwords with ``/`` and other DSN-significant characters get URL-
    quoted correctly. Falls back to ``$DATABASE_URL`` for legacy stacks.
    """
    from shared.db_url import build_postgres_url

    return (
        build_postgres_url(
            "POSTGRES",
            fallback_env="DATABASE_URL",
            default_host="localhost",
            default_db="srw",
        )
        or "postgresql://srw:srw_password@localhost:5432/srw"
    )


def _parse_connection_string(connection_string: str) -> dict:
    """Parse connection string into components for pg_dump/pg_restore."""
    parsed = urlparse(connection_string)
    return {
        "host": parsed.hostname or "localhost",
        "port": str(parsed.port or 5432),
        "user": parsed.username or "srw",
        "password": parsed.password or "",
        "database": parsed.path.lstrip("/").split("?")[0] or "srw",
    }


def _database_log_label(*, vector: bool = False) -> str:
    """Read diagnostic identity independently of a credential-bearing DSN."""
    if vector:
        if os.getenv("VECTOR_POSTGRES_USER") and os.getenv("VECTOR_POSTGRES_PASSWORD"):
            return os.getenv("VECTOR_POSTGRES_DB") or "vector database"
        return "vector database configured via VECTOR_DB_URL"
    if os.getenv("POSTGRES_USER") and os.getenv("POSTGRES_PASSWORD"):
        return os.getenv("POSTGRES_DB") or "srw"
    return "application database configured via DATABASE_URL"


async def init_postgres(force_reset: bool = False) -> bool:
    """Initialize PostgreSQL database.

    Uses the PostgresDB class from orchestrator.database.postgres for:
    - Database creation (if not exists)
    - Schema application (idempotent)
    - Schema reset (if force_reset=True)
    - Schema verification

    Args:
        force_reset: If True, drop all tables and recreate schema.

    Returns:
        True if initialization successful, False otherwise.
    """
    try:
        from orchestrator.database.postgres import PostgresDB
    except ImportError as e:
        logger.error(f"  Could not import PostgresDB: {e}")
        return False

    connection_string = get_postgres_connection_string()
    db_name = _database_log_label()
    logger.info("  Database: %s", db_name)

    db = PostgresDB(connection_string)

    try:
        # Create database if it doesn't exist
        try:
            created = await db.create_database_if_not_exists()
            if created:
                logger.info(f"  Created database: {db_name}")
        except Exception as e:
            logger.warning(f"  Could not check/create database: {e}")

        # Connect to the database
        await db.connect()

        # Reset schema if requested
        if force_reset:
            logger.info("  Resetting schema (dropping all tables)...")
            await db.reset_schema()
        else:
            # Apply pending migrations (idempotent — no-op if up to date)
            await db.apply_migrations()
            logger.info("  Applied pending migrations")

        # Bootstrap the two stable platform defaults from bundled YAML.  The
        # seed helper is insert-only: after first boot, the DB copies and the
        # operator-selected pointers are authoritative and are never reset by an
        # image upgrade.
        from orchestrator.services.default_experts import seed_managed_default_experts

        managed_defaults = await seed_managed_default_experts(
            db, PROJECT_ROOT / "config"
        )
        logger.info(
            "  Ensured managed expert defaults: worker=%s session=%s",
            managed_defaults.get("worker"),
            managed_defaults.get("session"),
        )

        # Verify tables exist
        logger.info("  Verifying tables:")
        table_status = await db.verify_schema()
        all_exist = True
        for table, exists in table_status.items():
            status = "ok" if exists else "MISSING"
            logger.info(f"    {table}: {status}")
            if not exists:
                all_exist = False

        # Backfill: encrypt any plaintext credentials JSONB values left over
        # from before the encryption-at-rest migration. Idempotent — no-op
        # on already-encrypted rows.
        await _backfill_encrypt_datasource_credentials(db)

        # Seed default datasources from environment variables
        await _seed_default_datasources(db)

        # Seed system_api_keys from SEED_*_API_KEY env vars (idempotent;
        # mirrors the helm seeder Job)
        await _seed_llm_keys_from_env(db)

        # Seed the subscription-proxy system endpoint when the proxy URL is set
        # (idempotent; matched by its stable transport marker)
        await _seed_subscription_proxy_endpoint(db)

        # Promote the legacy deployment-wide Tavily key into the model catalog.
        # This must precede any bundled SearXNG seed so upgrades retain Tavily
        # as primary and can use SearXNG only as a fallback.
        if await ensure_tavily_search_endpoint(db):
            logger.info("  Registered Tavily search provider from TAVILY_API_KEY")

        # The bundled keyless provider fills only an empty primary/fallback.
        # Keep this immediately after Tavily: upgrades with a legacy Tavily key
        # must retain Tavily as primary and place SearXNG in fallback.
        if await ensure_searxng_search_endpoint(db):
            logger.info("  Registered bundled SearXNG search provider")

        # Auto-wire the ElevenLabs TTS provider when ELEVENLABS_API_KEY is set
        # (idempotent; env is the source of truth for the key)
        if await ensure_elevenlabs_tts_endpoint(db):
            logger.info("  Registered ElevenLabs TTS provider from ELEVENLABS_API_KEY")

        # Seed the models catalog from the helm seed payload — same source
        # the helm Job consumes. helm/values.yaml's llm.seed.systemModels[]
        # is the only source; the legacy config/models.yaml bridge was
        # removed in chunk 7 of the catalog migration.
        await _seed_models_from_helm(db)

        # One-shot migration: promote system-scoped user_llm_endpoint_models
        # rows to the catalog and drop the legacy table. No-op once the
        # table is gone.
        await _migrate_endpoint_models_to_catalog(db)

        # When OpenRouter is the seeded gateway, insert convenience catalog
        # rows for the auxiliary + embedding routes that would otherwise be
        # empty (idempotent; resolver picks first-enabled-alphabetical at
        # call time)
        await _apply_openrouter_defaults(db)

        # Backfill Nextcloud cloud folders for existing projects
        await _backfill_cloud_folders(db)

        # Seed default projects for users without one
        # (handles JIT-provisioned users who lack a default project)
        await _seed_default_projects(db)

        # PR 3: one-shot bump for the operator's default project to
        # network_tier='home-allowed'. Runs every boot but only flips
        # the row when it's still at the migration default — see fn doc.
        await _seed_operator_network_tier(db)

        # Enforce NOT NULL constraint on default_project_id
        # (applied after seeding so existing users have projects first)
        await _enforce_default_project_constraint(db)

        # Migrate orphan jobs to default projects
        await _migrate_orphan_jobs(db)

        # Seed admin MCP token (after users and projects are set up)
        await _seed_admin_mcp_token(db)

        return all_exist

    except Exception as e:
        logger.error(f"  PostgreSQL initialization failed: {e}")
        return False

    finally:
        await db.close()


# =============================================================================
# Vector DB Initialization
# =============================================================================


def get_vector_connection_string() -> Optional[str]:
    """Get Vector DB connection string from environment.

    Returns None when neither the discrete-parts layout
    (VECTOR_POSTGRES_USER/PASSWORD/HOST/PORT/DB) nor the legacy
    VECTOR_DB_URL is set (single-DB mode).
    """
    from shared.db_url import build_postgres_url

    return build_postgres_url("VECTOR_POSTGRES", fallback_env="VECTOR_DB_URL")


async def init_vector_db(force_reset: bool = False) -> bool:
    """Initialize the Vector DB (citations, memories + knowledge_index).

    Requires VECTOR_DB_URL to be set.

    Args:
        force_reset: If True, drop all tables and recreate.

    Returns:
        True if successful, False on error.
    """
    vector_url = get_vector_connection_string()
    if not vector_url:
        logger.warning("  VECTOR_DB_URL not set — vector DB will not be initialized")
        return False

    try:
        from orchestrator.database.postgres import (
            MIGRATIONS_VECTOR_DIR,
            PostgresDB,
            VECTOR_REQUIRED_TABLES,
        )
    except ImportError as e:
        logger.error(f"  Could not import PostgresDB: {e}")
        return False

    db_name = _database_log_label(vector=True)
    logger.info("  Database: %s", db_name)

    db = PostgresDB(vector_url, migrations_dir=MIGRATIONS_VECTOR_DIR)

    try:
        # Create database if it doesn't exist
        try:
            created = await db.create_database_if_not_exists()
            if created:
                logger.info(f"  Created database: {db_name}")
        except Exception as e:
            logger.warning(f"  Could not check/create database: {e}")

        await db.connect()

        if force_reset:
            logger.info("  Resetting vector schema (dropping all tables)...")
            await db.reset_schema()
        else:
            # Apply the vector migration chain from zero. The runner is
            # idempotent and records schema_migrations; this replaces the frozen
            # vector_schema.sql, which drifted from the migrations and left the
            # vector DB missing halfvec/HNSW/bitemporal/TTL objects. (The frozen
            # file stays in the tree — tests still load it directly.)
            await db.apply_migrations()
        logger.info("  Vector migrations applied")

        # Verify tables
        logger.info("  Verifying vector tables:")
        async with db.acquire() as conn:
            for table in VECTOR_REQUIRED_TABLES:
                exists = await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                    "WHERE table_name = $1)",
                    table,
                )
                status = "ok" if exists else "MISSING"
                logger.info(f"    {table}: {status}")
                if not exists:
                    return False

        return True

    except Exception as e:
        logger.error(f"  Vector DB initialization failed: {e}")
        return False

    finally:
        await db.close()


async def verify_vector_db() -> dict:
    """Verify Vector DB connectivity and tables.

    Returns:
        Dict with 'configured', 'connected', 'tables', and any error info.
    """
    vector_url = get_vector_connection_string()
    if not vector_url:
        return {"configured": False}

    try:
        from orchestrator.database.postgres import PostgresDB, VECTOR_REQUIRED_TABLES
    except ImportError:
        return {
            "configured": True,
            "connected": False,
            "error": "asyncpg not installed",
        }

    db = PostgresDB(vector_url)

    try:
        await db.connect()
        tables = {}
        async with db.acquire() as conn:
            for table in VECTOR_REQUIRED_TABLES:
                exists = await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                    "WHERE table_name = $1)",
                    table,
                )
                tables[table] = exists
        return {
            "configured": True,
            "connected": True,
            "tables": tables,
            "all_tables_exist": all(tables.values()),
        }
    except Exception as e:
        return {"configured": True, "connected": False, "error": str(e)}
    finally:
        if db.is_connected:
            await db.close()


def backup_vector_db(backup_file: Path) -> bool:
    """Backup Vector DB using pg_dump.

    Args:
        backup_file: Path to write the backup file.

    Returns:
        True if successful, False otherwise.
    """
    vector_url = get_vector_connection_string()
    if not vector_url:
        logger.info("  Vector DB not configured (VECTOR_DB_URL not set)")
        return False

    params = _parse_connection_string(vector_url)

    cmd = [
        "pg_dump",
        "-h",
        params["host"],
        "-p",
        params["port"],
        "-U",
        params["user"],
        "-d",
        params["database"],
        "-F",
        "c",  # Custom format (compressed)
        "-f",
        str(backup_file),
    ]

    env = os.environ.copy()
    env["PGPASSWORD"] = params["password"]

    logger.info(
        "  Running pg_dump for vector database: %s",
        _database_log_label(vector=True),
    )

    try:
        subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
        logger.info(f"  Backup created: {backup_file}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"  pg_dump failed: {e.stderr}")
        return False
    except FileNotFoundError:
        logger.error("  pg_dump not found. Is PostgreSQL client installed?")
        return False


def restore_vector_db(backup_file: Path) -> bool:
    """Restore Vector DB from pg_dump backup.

    Args:
        backup_file: Path to the backup file.

    Returns:
        True if successful, False otherwise.
    """
    if not backup_file.exists():
        logger.error(f"  Backup file not found: {backup_file}")
        return False

    vector_url = get_vector_connection_string()
    if not vector_url:
        logger.info("  Vector DB not configured (VECTOR_DB_URL not set)")
        return False

    params = _parse_connection_string(vector_url)

    env = os.environ.copy()
    env["PGPASSWORD"] = params["password"]

    cmd = [
        "pg_restore",
        "-h",
        params["host"],
        "-p",
        params["port"],
        "-U",
        params["user"],
        "-d",
        params["database"],
        "--clean",
        "--if-exists",
        str(backup_file),
    ]

    logger.info(
        "  Running pg_restore for vector database: %s",
        _database_log_label(vector=True),
    )

    try:
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if result.returncode != 0 and "error" in result.stderr.lower():
            logger.warning(f"  pg_restore warnings: {result.stderr[:200]}")
        logger.info(f"  Restore completed from: {backup_file}")
        return True
    except FileNotFoundError:
        logger.error("  pg_restore not found. Is PostgreSQL client installed?")
        return False


async def verify_postgres() -> dict:
    """Verify PostgreSQL connectivity and schema.

    Returns:
        Dict with 'connected', 'tables', and any error info.
    """
    try:
        from orchestrator.database.postgres import PostgresDB
    except ImportError:
        return {"connected": False, "error": "asyncpg not installed"}

    connection_string = get_postgres_connection_string()
    db = PostgresDB(connection_string)

    try:
        await db.connect()
        tables = await db.verify_schema()
        return {
            "connected": True,
            "tables": tables,
            "all_tables_exist": all(tables.values()),
        }
    except Exception as e:
        return {"connected": False, "error": str(e)}
    finally:
        if db.is_connected:
            await db.close()


def backup_postgres(backup_file: Path) -> bool:
    """Backup PostgreSQL using pg_dump.

    Args:
        backup_file: Path to write the backup file.

    Returns:
        True if successful, False otherwise.
    """
    connection_string = get_postgres_connection_string()
    params = _parse_connection_string(connection_string)

    cmd = [
        "pg_dump",
        "-h",
        params["host"],
        "-p",
        params["port"],
        "-U",
        params["user"],
        "-d",
        params["database"],
        "-F",
        "c",  # Custom format (compressed)
        "-f",
        str(backup_file),
    ]

    env = os.environ.copy()
    env["PGPASSWORD"] = params["password"]

    logger.info(
        "  Running pg_dump for database: %s",
        _database_log_label(),
    )

    try:
        subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
        logger.info(f"  Backup created: {backup_file}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"  pg_dump failed: {e.stderr}")
        return False
    except FileNotFoundError:
        logger.error("  pg_dump not found. Is PostgreSQL client installed?")
        return False


def restore_postgres(backup_file: Path) -> bool:
    """Restore PostgreSQL from pg_dump backup.

    Args:
        backup_file: Path to the backup file.

    Returns:
        True if successful, False otherwise.
    """
    if not backup_file.exists():
        logger.error(f"  Backup file not found: {backup_file}")
        return False

    connection_string = get_postgres_connection_string()
    params = _parse_connection_string(connection_string)

    env = os.environ.copy()
    env["PGPASSWORD"] = params["password"]

    # Clear database first
    logger.info(
        "  Clearing database: %s",
        _database_log_label(),
    )
    try:
        asyncio.run(_reset_postgres_schema())
    except Exception as e:
        logger.warning(f"  Could not clear database: {e}")

    cmd = [
        "pg_restore",
        "-h",
        params["host"],
        "-p",
        params["port"],
        "-U",
        params["user"],
        "-d",
        params["database"],
        "--clean",
        "--if-exists",
        str(backup_file),
    ]

    logger.info(
        "  Running pg_restore for database: %s",
        _database_log_label(),
    )

    try:
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if result.returncode != 0 and "error" in result.stderr.lower():
            logger.warning(f"  pg_restore warnings: {result.stderr[:200]}")
        logger.info(f"  Restore completed from: {backup_file}")
        return True
    except FileNotFoundError:
        logger.error("  pg_restore not found. Is PostgreSQL client installed?")
        return False


async def _reset_postgres_schema():
    """Helper to reset PostgreSQL schema."""
    from orchestrator.database.postgres import PostgresDB

    connection_string = get_postgres_connection_string()
    db = PostgresDB(connection_string)
    try:
        await db.connect()
        await db.reset_schema()
    finally:
        await db.close()


# =============================================================================
# Datasource Credentials Encryption Backfill
# =============================================================================


async def _backfill_encrypt_datasource_credentials(db) -> None:
    """Encrypt any legacy plaintext credentials in the datasources table.

    Idempotent: rows already encrypted (v1 ciphertext) or empty are skipped.
    Logged so operators can confirm the one-shot migration ran on first boot
    after the encryption-at-rest change.
    """
    try:
        counts = await db.backfill_encrypt_datasource_credentials()
    except Exception as exc:
        logger.error("  Backfill of datasource credentials failed: %s", exc)
        return

    encrypted = counts["encrypted"]
    skipped = counts["skipped"]
    errors = counts["errors"]

    if encrypted > 0:
        logger.info(
            "  Encrypted %d legacy plaintext datasource credentials "
            "(%d skipped, %d errors)",
            encrypted,
            skipped,
            errors,
        )
    elif errors > 0:
        logger.warning(
            "  Datasource credentials backfill: %d errors (%d skipped)",
            errors,
            skipped,
        )
    else:
        logger.debug(
            "  Datasource credentials backfill: nothing to encrypt (%d skipped)",
            skipped,
        )


# =============================================================================
# Default Datasource Seeding
# =============================================================================


async def _upsert_seeded_datasource(
    db,
    drivers,
    *,
    name: str,
    ds_type: str,
    connection_url: str,
    credentials: dict | None = None,
) -> bool:
    """Validate a seeded row with its connector driver, then upsert it.

    ``False`` (and an error in the log) when the driver refuses it.
    """
    from orchestrator.services.connector_drivers.platform import (
        validate_platform_connector,
    )

    try:
        normalized = await validate_platform_connector(
            drivers,
            ds_type,
            name=name,
            connection_url=connection_url,
            credentials=credentials,
        )
    except ValueError as exc:
        logger.error("    Default datasource %s (%s) refused: %s", ds_type, name, exc)
        return False
    await db.upsert_default_datasource(
        name=name,
        ds_type=ds_type,
        connection_url=normalized.connection_url,
        credentials=normalized.credentials,
    )
    return True


async def _seed_default_datasources(db) -> None:
    """Seed global datasources from DEFAULT_DS_* environment variables.

    Creates or updates global datasources (job_id=NULL) based on env vars.
    See knowledge-base/knowledge/datasources.md for the full design.

    Supported env var patterns:
        DEFAULT_DS_POSTGRESQL_URL, DEFAULT_DS_POSTGRESQL_NAME, DEFAULT_DS_POSTGRESQL_READ_ONLY
        DEFAULT_DS_NEO4J_URL, DEFAULT_DS_NEO4J_USERNAME, DEFAULT_DS_NEO4J_PASSWORD, DEFAULT_DS_NEO4J_NAME, DEFAULT_DS_NEO4J_READ_ONLY
        DEFAULT_DS_MONGODB_URL, DEFAULT_DS_MONGODB_NAME, DEFAULT_DS_MONGODB_READ_ONLY

    Each row goes through its connector driver's validation first, as a
    create would; a refused one is logged and not seeded.
    """
    from orchestrator.services.connector_drivers import builtin_connector_drivers

    drivers = builtin_connector_drivers()
    seeded = 0

    # PostgreSQL datasource
    pg_url = os.getenv("DEFAULT_DS_POSTGRESQL_URL")
    if pg_url:
        name = os.getenv("DEFAULT_DS_POSTGRESQL_NAME", "Default PostgreSQL")
        if await _upsert_seeded_datasource(
            db,
            drivers,
            name=name,
            ds_type="postgresql",
            connection_url=pg_url,
        ):
            logger.info(f"    Seeded default datasource: postgresql ({name})")
            seeded += 1

    # Neo4j datasource
    neo4j_url = os.getenv("DEFAULT_DS_NEO4J_URL")
    if neo4j_url:
        name = os.getenv("DEFAULT_DS_NEO4J_NAME", "Default Neo4j")
        credentials = {}
        username = os.getenv("DEFAULT_DS_NEO4J_USERNAME")
        password = os.getenv("DEFAULT_DS_NEO4J_PASSWORD")
        if username:
            credentials["username"] = username
        if password:
            credentials["password"] = password
        if await _upsert_seeded_datasource(
            db,
            drivers,
            name=name,
            ds_type="neo4j",
            connection_url=neo4j_url,
            credentials=credentials if credentials else None,
        ):
            logger.info(f"    Seeded default datasource: neo4j ({name})")
            seeded += 1

    # MongoDB datasource
    mongo_url = os.getenv("DEFAULT_DS_MONGODB_URL")
    if mongo_url:
        name = os.getenv("DEFAULT_DS_MONGODB_NAME", "Default MongoDB")
        if await _upsert_seeded_datasource(
            db,
            drivers,
            name=name,
            ds_type="mongodb",
            connection_url=mongo_url,
        ):
            logger.info(f"    Seeded default datasource: mongodb ({name})")
            seeded += 1

    # WebDAV datasource (Nextcloud or any WebDAV server)
    webdav_url = os.getenv("DEFAULT_DS_WEBDAV_URL")
    if webdav_url:
        name = os.getenv("DEFAULT_DS_WEBDAV_NAME", "Default WebDAV")
        credentials = {}
        username = os.getenv("DEFAULT_DS_WEBDAV_USERNAME")
        password = os.getenv("DEFAULT_DS_WEBDAV_PASSWORD")
        if username:
            credentials["username"] = username
        if password:
            credentials["password"] = password
        if await _upsert_seeded_datasource(
            db,
            drivers,
            name=name,
            ds_type="webdav",
            connection_url=webdav_url,
            credentials=credentials if credentials else None,
        ):
            logger.info(f"    Seeded default datasource: webdav ({name})")
            seeded += 1

    if seeded > 0:
        logger.info(f"  Seeded {seeded} default datasource(s)")
    else:
        logger.info(
            "  No default datasources configured (DEFAULT_DS_* env vars not set)"
        )


# =============================================================================
# LLM API Key Seeding (mirrors helm/templates/orchestrator/llm-seed-job.yaml)
# =============================================================================


# Providers seeded from SEED_*_API_KEY env vars. Stays in sync with
# orchestrator.main.VALID_SYSTEM_API_KEY_PROVIDERS minus the legacy
# "vision" slot (vision keys ride along on a per-endpoint inline api_key
# now). Inlined here rather than imported from main.py because importing
# main.py at runtime triggers a fresh ``load_dotenv()`` that would undo
# any test-time ``monkeypatch.delenv`` calls.
_SEEDABLE_PROVIDERS = (
    "openai",
    "anthropic",
    "google",
    "groq",
    "openrouter",
)


async def _seed_llm_keys_from_env(db) -> None:
    """Seed system_api_keys from SEED_<PROVIDER>_API_KEY env vars on first boot.

    Mirrors the helm seeder Job so local dev with ``python init.py`` and a
    ``.env`` file gets the same insert-only behavior. After first run,
    rotating keys via Admin → Providers is never clobbered — re-runs report
    all entries as "skipped" and exit clean.

    Env var shape: ``SEED_OPENAI_API_KEY``, ``SEED_ANTHROPIC_API_KEY``, …
    one per provider in ``_SEEDABLE_PROVIDERS``.
    """
    try:
        from orchestrator.seed.llm_config import seed as llm_seed
    except ImportError as e:
        logger.warning(f"  Could not import seed.llm_config: {e}")
        return

    entries = []
    for provider in _SEEDABLE_PROVIDERS:
        env_name = f"SEED_{provider.upper()}_API_KEY"
        if os.environ.get(env_name):
            entries.append(
                {
                    "provider": provider,
                    "apiKeyEnv": env_name,
                    "label": f"Seeded from {env_name} at init.",
                }
            )

    if not entries:
        logger.info("  No SEED_*_API_KEY env vars set — skipping LLM key seed")
        return

    report = await llm_seed(db, {"systemApiKeys": entries})
    # Report the providers via the literal tuple this function seeds from, so
    # no string that passed through the seeder's key handling reaches the log.
    seeded = [p for p in _SEEDABLE_PROVIDERS if p in set(report.api_keys_seeded)]
    skipped = [p for p in _SEEDABLE_PROVIDERS if p in set(report.api_keys_skipped)]
    if seeded:
        logger.info("  Seeded system_api_keys: %s", ", ".join(seeded))
    if skipped:
        logger.info(
            "  Skipped existing system_api_keys: %s",
            ", ".join(skipped),
        )


from orchestrator.seed.llm_config import (  # noqa: E402, F401
    CODEX_PROXY_ENDPOINT_LABEL,  # re-exported: tests reference init_mod.CODEX_PROXY_ENDPOINT_LABEL
    SUBSCRIPTION_PROXY_ENDPOINT_LABEL,
    ensure_codex_proxy_endpoint,
    ensure_elevenlabs_tts_endpoint,
    ensure_searxng_search_endpoint,
    ensure_subscription_proxy_endpoint,
    ensure_tavily_search_endpoint,
)


async def _seed_subscription_proxy_endpoint(db) -> None:
    """Boot-time seed for the system-scoped subscription-proxy llm_endpoints row.

    Skips when neither ``SUBSCRIPTION_PROXY_URL`` nor the legacy
    ``CODEX_PROXY_URL`` is set, so a fresh stack with no proxy configured
    doesn't carry a dangling transport row. Runtime paths (an OAuth callback,
    the availability probe) call
    :func:`ensure_subscription_proxy_endpoint` directly with a fallback URL so
    a user who connects a subscription via the cockpit gets wired up without
    having to set the env var.
    """
    proxy_url = os.environ.get("SUBSCRIPTION_PROXY_URL") or os.environ.get(
        "CODEX_PROXY_URL"
    )
    if not proxy_url:
        logger.info(
            "  SUBSCRIPTION_PROXY_URL/CODEX_PROXY_URL not set — "
            "skipping subscription-proxy endpoint seed"
        )
        return

    created = await ensure_subscription_proxy_endpoint(db, proxy_url=proxy_url)
    if created:
        logger.info(f"  Seeded subscription-proxy endpoint at {proxy_url}")
    else:
        logger.info("  subscription-proxy endpoint already present — leaving untouched")


# Back-compat alias for callers/tests written against the Codex-only name.
_seed_codex_proxy_endpoint = _seed_subscription_proxy_endpoint


async def _seed_models_from_helm(db) -> None:
    """Seed the ``models`` catalog from the helm seed payload on first boot.

    Reads ``helm/values.yaml``'s ``llm.seed.systemModels[]`` block (the same
    block the helm post-install Job consumes via the rendered ConfigMap) and
    delegates to :func:`orchestrator.seed.llm_config.seed`. Each entry becomes
    one ``(model, capability)`` catalog row with ``provider_kind='system'``,
    inserted via ``ON CONFLICT DO NOTHING`` so admin edits made via the
    Cockpit are never clobbered. The sibling ``llm.seed.defaults`` map rides
    along: it pins ``llm.default_<kind>_model`` only for kinds that have no
    pin yet, and only to models present in the catalog.

    Rows whose ``provider`` has no ``system_api_keys`` entry yet are skipped
    by the seeder — they would not be reachable. Bare-metal devs running
    ``python init.py`` get the same path as the helm Job; the helm chart
    values are the single source of truth for which catalog rows ship by
    default.
    """
    helm_values_path = Path(__file__).resolve().parents[2] / "helm" / "values.yaml"
    system_models: list[dict] = []
    defaults: dict = {}

    if helm_values_path.exists():
        try:
            helm_raw = yaml.safe_load(helm_values_path.read_text()) or {}
        except yaml.YAMLError as e:
            logger.warning(f"  Could not parse {helm_values_path}: {e}")
            helm_raw = {}
        seed_block = (helm_raw.get("llm") or {}).get("seed") or {}
        system_models = list(seed_block.get("systemModels") or [])
        defaults = dict(seed_block.get("defaults") or {})

    if not system_models and not defaults:
        logger.info(
            "  Catalog seed skipped — no helm.llm.seed.systemModels or defaults entries"
        )
        return

    try:
        from orchestrator.seed.llm_config import seed as llm_seed
    except ImportError as e:
        logger.warning(f"  Could not import seed.llm_config: {e}")
        return

    # Only systemModels + defaults here. Endpoint seeding is owned by
    # _seed_codex_proxy_endpoint (init.py) and the helm post-install Job.
    report = await llm_seed(db, {"systemModels": system_models, "defaults": defaults})

    if report.models_seeded:
        logger.info(f"  Seeded {len(report.models_seeded)} catalog rows from helm seed")
    if report.models_skipped:
        # skipped covers both "already present" and "provider not configured" —
        # the per-entry reason is in the seeder's own log lines above.
        logger.info(
            f"  Skipped {len(report.models_skipped)} catalog rows "
            "(already present or provider not seeded)"
        )
    if report.defaults_seeded:
        logger.info(
            f"  Pinned {len(report.defaults_seeded)} default model(s) from helm seed"
        )
    if report.defaults_skipped:
        logger.info(
            f"  Skipped {len(report.defaults_skipped)} default pin(s) "
            "(already pinned or model not in the catalog)"
        )


async def _migrate_endpoint_models_to_catalog(db) -> None:
    """One-shot migration: promote system-scoped ``user_llm_endpoint_models``
    rows into the catalog and drop the legacy table.

    Idempotent — bails out cleanly when the table no longer exists. User-
    scoped rows (``llm_endpoints.user_id IS NOT NULL``) are *not*
    migrated; the dual-scope complexity isn't worth carrying forward, and
    user-side authoring was unused in practice (per design doc §Decisions).
    """
    async with db.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT to_regclass('public.user_llm_endpoint_models') IS NOT NULL"
        )
        if not exists:
            return

        rows = await conn.fetch(
            """
            SELECT m.endpoint_id, m.model_id, m.display_name, m.family,
                   m.context_window, m.reasoning_level, m.enabled, m.capability
              FROM user_llm_endpoint_models m
              JOIN llm_endpoints e ON e.id = m.endpoint_id
             WHERE e.user_id IS NULL
            """
        )

    promoted = 0
    skipped_unsupported_capability = 0
    for row in rows:
        capability = (row["capability"] or "chat").lower()
        if capability not in (
            "chat",
            "auxiliary",
            "embedding",
            "vision",
            "whisper",
            "tts",
        ):
            skipped_unsupported_capability += 1
            continue
        from shared.runtime.core.model_registry import family_of  # local import: src/*
        # path may not be on sys.path until runtime is fully wired

        # Forward the singular capability — the accessor's canonicalizer
        # auto-expands 'chat' to ['chat','auxiliary'] (matches the operator
        # intent on the v1 endpoint table, which never knew about array
        # shapes). Other singular values land as singletons.
        result = await db.create_model(
            provider_kind="endpoint",
            provider_ref=str(row["endpoint_id"]),
            model_id=row["model_id"],
            display_label=row["display_name"],
            capability=capability,
            family=row["family"] or family_of(row["model_id"]),
            context_window=row["context_window"],
            reasoning_level=row["reasoning_level"],
            enabled=bool(row["enabled"]),
            seeded_from="migration:user_llm_endpoint_models",
            on_conflict_do_nothing=True,
        )
        if result is not None:
            promoted += 1

    async with db.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS user_llm_endpoint_models CASCADE")

    logger.info(
        "  Migrated user_llm_endpoint_models → models: %d promoted, "
        "%d skipped (unsupported v1 capability); table dropped",
        promoted,
        skipped_unsupported_capability,
    )


async def _apply_openrouter_defaults(db) -> None:
    """When OpenRouter is the seeded gateway, insert convenience catalog rows
    for the OpenRouter-routed auxiliary and embedding models.

    Rationale: OpenRouter is a single-key gateway for many provider stacks.
    When an admin only seeds an OpenRouter key, the auxiliary and embedding
    slots would otherwise be empty and dependent features (memory
    extraction, recall search) would fail at first use. Inserting these
    rows ensures the catalog has at least one entry per dependent capability.

    The default-model resolver's "first-enabled-alphabetical" fallback then
    handles which one gets used. Admin edits/disables survive subsequent
    boots via ``ON CONFLICT DO NOTHING``.
    """
    openrouter_key = await db.get_system_api_key("openrouter")
    if not openrouter_key:
        return

    convenience_rows = [
        {
            "provider_kind": "system",
            "provider_ref": "openrouter",
            "model_id": "openrouter/google/gemini-2.5-flash",
            "display_label": "Gemini 2.5 Flash (OpenRouter)",
            "capabilities": ["auxiliary"],
            "family": "gemini",
            "seeded_from": "helm:openrouter-defaults",
        },
        {
            "provider_kind": "system",
            "provider_ref": "openrouter",
            "model_id": "openrouter/openai/text-embedding-3-large",
            "display_label": "text-embedding-3-large (OpenRouter)",
            "capabilities": ["embedding"],
            "family": "default",
            "seeded_from": "helm:openrouter-defaults",
        },
    ]

    inserted = 0
    for row in convenience_rows:
        result = await db.create_model(**row, on_conflict_do_nothing=True)
        if result is not None:
            inserted += 1
            logger.info(
                f"  Inserted openrouter convenience row: {row['model_id']} "
                f"({row['capabilities']})"
            )
    if not inserted:
        logger.info("  OpenRouter convenience catalog rows already present — no-op")


# =============================================================================
# Cloud Folder Backfill
# =============================================================================


async def _backfill_cloud_folders(db) -> None:
    """Create main-cloud project folders for projects that predate the feature.

    Idempotent — skips projects whose main_cloud_folder_handle (or the legacy
    nextcloud_folder_id) is already populated.
    """
    from orchestrator.services.cloud import build_backend

    backend = build_backend()
    if not await backend.ensure_initialized():
        logger.info("  Cloud folder backfill skipped — main cloud not available")
        return

    backfilled = 0
    async with db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, name FROM projects
            WHERE main_cloud_folder_handle IS NULL
              AND nextcloud_folder_id IS NULL
              AND is_default = FALSE
              AND status = 'active'
            """
        )

    for row in rows:
        project_id = str(row["id"])
        project_name = row["name"]
        group_name = f"project-{project_id}"

        try:
            await backend.ensure_group(group_name)
            folder_handle = await backend.ensure_project_folder(
                project_name=project_name,
                group_id=group_name,
            )
            if folder_handle is not None:
                legacy_folder_id = backend.legacy_folder_id(folder_handle)
                await db.update_project(
                    project_id,
                    main_cloud_backend=backend.backend_id,
                    main_cloud_folder_handle=folder_handle.to_db(),
                    nextcloud_folder_id=legacy_folder_id,
                )

                # The project working folder is intentionally NOT attached as a
                # `webdav` datasource — it is cloned into job/session workspaces
                # (Mode-A baseline / `projects/` sync mount), so a datasource here
                # would double-expose the same files through the webdav_* tools.
                # See knowledge-base/knowledge/issues/main_cloud.md (Issue 1 / Issue 8).

                backfilled += 1
                logger.info(
                    f"    Backfilled cloud folder for project '{project_name}' "
                    f"(handle={folder_handle.native_id})"
                )
        except Exception as e:
            logger.warning(
                f"    Failed to backfill cloud folder for project '{project_name}': {e}"
            )

    await backend.close()

    if backfilled > 0:
        logger.info(f"  Backfilled {backfilled} project cloud folder(s)")
    else:
        logger.info("  No projects need cloud folder backfill")


# =============================================================================
# Default Project Seeding
# =============================================================================


async def _seed_default_projects(db) -> None:
    """Create default projects for users that don't have one.

    Queries users where default_project_id IS NULL, then creates a personal
    default project for each with owner membership. Managed knowledge-vault
    provisioning is handled by the orchestrator; projects no longer receive a
    shared jobs repository.
    """
    try:
        async with db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, display_name FROM users WHERE default_project_id IS NULL"
            )

        if not rows:
            logger.info("  All users have default projects")
            return

        seeded = 0
        for row in rows:
            try:
                await db.create_default_project_for_user(
                    str(row["id"]), row["display_name"]
                )
                seeded += 1
            except Exception as e:
                logger.warning(
                    f"  Failed to create default project for user {row['display_name']}: {e}"
                )

        if seeded > 0:
            logger.info(f"  Seeded {seeded} default project(s)")

    except Exception as e:
        logger.warning(f"  Default project seeding failed: {e}")


async def _seed_operator_network_tier(db) -> None:
    """Bump the homelab operator's default project to home-allowed.

    Reads OPERATOR_HOME_ALLOWED_EMAIL from env. When set, finds the
    matching user's default project and flips its network_tier from
    'internet-only' (the migration default) to 'home-allowed'. The
    UPDATE is gated on the current value being 'internet-only' so the
    seeder is idempotent and never overwrites an operator who's later
    moved their project to a different tier through the cockpit.

    Silent no-op when the env var is empty (SaaS deployments) or when
    the user/project doesn't exist yet (e.g. first boot before
    Keycloak has JIT-provisioned the user — a later boot picks it up).
    """
    operator_email = os.environ.get("OPERATOR_HOME_ALLOWED_EMAIL", "").strip()
    if not operator_email:
        return

    try:
        async with db.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE projects
                SET network_tier = 'home-allowed'
                WHERE id = (
                    SELECT default_project_id FROM users WHERE email = $1
                )
                AND network_tier = 'internet-only'
                """,
                operator_email,
            )
        # asyncpg returns 'UPDATE <n>'; trailing token is the row count.
        if isinstance(result, str) and result.startswith("UPDATE "):
            count = result.rsplit(" ", 1)[-1]
            if count != "0":
                logger.info(
                    f"  Operator project network_tier set to 'home-allowed' "
                    f"(user={operator_email})"
                )
    except Exception as e:
        logger.warning(f"  Operator network_tier seed failed: {e}")


async def _enforce_default_project_constraint(db) -> None:
    """Add CHECK constraint ensuring every user has a default project.

    Uses ADD CONSTRAINT ... CHECK (default_project_id IS NOT NULL).
    Idempotent — skips if the constraint already exists.
    Must run after _seed_default_projects to avoid constraint violations.
    """
    try:
        async with db.acquire() as conn:
            await conn.execute("""
                DO $$ BEGIN
                    ALTER TABLE users ADD CONSTRAINT users_default_project_required
                        CHECK (default_project_id IS NOT NULL);
                EXCEPTION WHEN duplicate_object THEN null;
                END $$;
            """)
        logger.info("  Default project constraint enforced on users table")
    except Exception as e:
        logger.warning(f"  Failed to enforce default project constraint: {e}")


async def _seed_admin_mcp_token(db) -> None:
    """Generate a root MCP token for the admin user if none exists.

    When MCP_DEV_TOKEN is set in the environment, uses that fixed value
    instead of generating a random token. This keeps the token stable
    across database resets so .mcp.json never goes stale during local
    development.

    Env vars:
        MCP_DEV_TOKEN: Fixed token value (must start with "srw_").
                       When set, this exact token is inserted on every init.
                       When unset, a random token is generated once.
    """
    try:
        admin = await db.get_admin_user()
        if not admin:
            return

        dev_token = os.environ.get("MCP_DEV_TOKEN", "").strip()

        # Check if admin already has an MCP token
        existing = await db.list_mcp_tokens(str(admin["id"]))
        if existing:
            if dev_token:
                # Dev token configured — ensure it matches what's in the DB.
                # If the hash differs (e.g. env var changed), replace it.
                expected_hash = hashlib.sha256(dev_token.encode()).hexdigest()
                current_hash_matches = False
                for tok in existing:
                    stored = await db.get_mcp_token_by_hash(expected_hash)
                    if stored:
                        current_hash_matches = True
                        break
                if current_hash_matches:
                    logger.info("  Admin MCP dev token already exists and matches")
                    return
                # Hash mismatch — revoke old tokens and insert the new dev token
                for tok in existing:
                    await db.revoke_mcp_token(str(tok["id"]), str(admin["id"]))
                logger.info("  Replaced admin MCP token with updated dev token")
            else:
                logger.info("  Admin MCP token already exists")
                return

        # Use fixed dev token or generate a random one
        if dev_token:
            if not dev_token.startswith("srw_"):
                logger.warning("  MCP_DEV_TOKEN must start with 'srw_' — ignoring")
                return
            token = dev_token
        else:
            token = "srw_" + secrets.token_urlsafe(32)

        token_hash = hashlib.sha256(token.encode()).hexdigest()
        token_prefix = token[:12]

        await db.create_mcp_token(
            user_id=str(admin["id"]),
            name="Root (auto-generated)",
            token_hash=token_hash,
            token_prefix=token_prefix,
            scope="all",
        )

        if dev_token:
            logger.info("  Admin MCP token seeded from MCP_DEV_TOKEN (scope: all)")
        else:
            logger.info("  ============================================")
            logger.info("  Admin MCP Token (scope: all):")
            logger.info(f"  {token}")
            logger.info("  Save this token — it will not be shown again.")
            logger.info("  ============================================")
    except Exception as e:
        logger.warning(f"  Admin MCP token seeding failed: {e}")


async def _migrate_orphan_jobs(db) -> None:
    """Assign orphan jobs (project_id IS NULL) to their user's default project.

    For jobs with a user_id, uses the user's default_project_id.
    For jobs without a user_id, assigns to the first available default project
    as a fallback.
    """
    try:
        async with db.acquire() as conn:
            # Jobs with a user_id: assign to that user's default project
            result = await conn.execute("""
                UPDATE jobs j
                SET project_id = u.default_project_id
                FROM users u
                WHERE j.user_id = u.id
                  AND j.project_id IS NULL
                  AND u.default_project_id IS NOT NULL
            """)
            user_count = int(result.split()[-1]) if result else 0

            # Jobs without user_id: assign to first default project as fallback
            fallback_count = 0
            orphan_count = await conn.fetchval(
                "SELECT COUNT(*) FROM jobs WHERE project_id IS NULL"
            )
            if orphan_count and orphan_count > 0:
                fallback_project = await conn.fetchval(
                    "SELECT id FROM projects WHERE is_default = TRUE LIMIT 1"
                )
                if fallback_project:
                    result = await conn.execute(
                        "UPDATE jobs SET project_id = $1 WHERE project_id IS NULL",
                        fallback_project,
                    )
                    fallback_count = int(result.split()[-1]) if result else 0

        total = user_count + fallback_count
        if total > 0:
            logger.info(
                f"  Migrated {total} orphan job(s) to default projects "
                f"({user_count} by user, {fallback_count} fallback)"
            )
        else:
            logger.info("  No orphan jobs to migrate")

    except Exception as e:
        logger.warning(f"  Orphan job migration failed: {e}")


# =============================================================================
# Neo4j Initialization (System Knowledge Base)
# =============================================================================

NEO4J_SCHEMA_QUERIES = [
    # Unique constraint: note slug per project
    "CREATE CONSTRAINT note_id_unique IF NOT EXISTS FOR (n:Note) REQUIRE (n.project_id, n.id) IS UNIQUE",
    # Indexes for common lookups
    "CREATE INDEX note_project IF NOT EXISTS FOR (n:Note) ON (n.project_id)",
    "CREATE INDEX note_type IF NOT EXISTS FOR (n:Note) ON (n.project_id, n.type)",
    "CREATE INDEX note_status IF NOT EXISTS FOR (n:Note) ON (n.project_id, n.status)",
    "CREATE INDEX tag_project IF NOT EXISTS FOR (t:Tag) ON (t.project_id)",
    "CREATE INDEX keyword_project IF NOT EXISTS FOR (k:Keyword) ON (k.project_id)",
]


def get_neo4j_config() -> Optional[dict]:
    """Get system Neo4j connection config from environment.

    Returns:
        Dict with uri, username, password if configured, else None.
    """
    uri = os.getenv("NEO4J_URL")
    if not uri:
        return None
    return {
        "uri": uri,
        "username": os.getenv("NEO4J_USERNAME", "neo4j"),
        "password": os.getenv("NEO4J_PASSWORD", ""),
    }


def init_neo4j(force_reset: bool = False) -> bool:
    """Initialize Neo4j schema for the knowledge base.

    Creates constraints and indexes for Note, Tag, and Keyword nodes.
    Idempotent — all statements use IF NOT EXISTS.

    Args:
        force_reset: If True, delete all knowledge nodes before recreating schema.

    Returns:
        True if successful or Neo4j not configured, False on error.
    """
    config = get_neo4j_config()
    if not config:
        logger.info("  Neo4j not configured (NEO4J_URL not set)")
        logger.info("  Skipping Neo4j initialization (optional for knowledge base)")
        return True

    try:
        from shared.runtime.database.neo4j_db import Neo4jDB
    except ImportError as e:
        logger.warning(f"  Could not import Neo4jDB: {e}")
        return True  # Non-fatal — KB is optional

    db = Neo4jDB(**config)
    if not db.connect():
        logger.warning("  Could not connect to Neo4j — knowledge base unavailable")
        return True  # Non-fatal

    try:
        if force_reset:
            logger.info("  Resetting Neo4j knowledge data...")
            db.execute_write("MATCH (n:Note) DETACH DELETE n")
            db.execute_write("MATCH (t:Tag) DETACH DELETE t")
            db.execute_write("MATCH (k:Keyword) DETACH DELETE k")
            logger.info("  Cleared all knowledge nodes")

        logger.info("  Applying Neo4j schema (constraints + indexes)...")
        for query in NEO4J_SCHEMA_QUERIES:
            try:
                db.execute_write(query)
            except Exception as e:
                # Some Neo4j versions handle IF NOT EXISTS differently
                logger.debug(f"  Schema query note: {e}")

        logger.info("  Neo4j schema applied successfully")
        return True

    except Exception as e:
        logger.warning(f"  Neo4j initialization error: {e}")
        return True  # Non-fatal

    finally:
        db.close()


def verify_neo4j() -> dict:
    """Verify Neo4j connectivity and schema.

    Returns:
        Dict with connection status and schema info.
    """
    config = get_neo4j_config()
    if not config:
        return {"connected": False, "configured": False}

    try:
        from shared.runtime.database.neo4j_db import Neo4jDB
    except ImportError:
        return {
            "connected": False,
            "configured": True,
            "error": "neo4j driver not installed",
        }

    db = Neo4jDB(**config)
    if not db.connect():
        return {"connected": False, "configured": True, "error": "connection failed"}

    try:
        schema = db.get_schema()
        return {
            "connected": True,
            "configured": True,
            "node_labels": schema.get("node_labels", []),
            "relationship_types": schema.get("relationship_types", []),
        }
    except Exception as e:
        return {"connected": False, "configured": True, "error": str(e)}
    finally:
        db.close()


# =============================================================================
# Uploads Directory Management
# =============================================================================


def _resolve_uploads_dir() -> Path:
    """Resolve uploads directory, respecting WORKSPACE_PATH env var."""
    env_path = os.getenv("WORKSPACE_PATH")
    if env_path:
        return Path(env_path) / "uploads"
    return Path(__file__).resolve().parents[2] / "workspace" / "uploads"


UPLOADS_DIR = _resolve_uploads_dir()


def get_uploads_path() -> Path:
    """Get the uploads directory path."""
    return UPLOADS_DIR


def init_uploads() -> bool:
    """Initialize uploads directory."""
    uploads_path = get_uploads_path()
    try:
        uploads_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"  Uploads path: {uploads_path}")
        return True
    except Exception as e:
        logger.error(f"  Failed to initialize uploads: {e}")
        return False


def cleanup_uploads() -> bool:
    """Clean up all uploads."""
    uploads_path = get_uploads_path()
    if not uploads_path.exists():
        return True

    removed = 0
    for item in uploads_path.iterdir():
        try:
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
            removed += 1
        except Exception as e:
            logger.warning(f"  Failed to remove {item}: {e}")

    logger.info(f"  Removed {removed} upload(s)")
    return True


def verify_uploads() -> dict:
    """Verify uploads directory and return stats."""
    uploads_path = get_uploads_path()
    result = {
        "path": str(uploads_path),
        "exists": uploads_path.exists(),
        "upload_count": 0,
        "total_files": 0,
        "total_size_bytes": 0,
    }

    if not uploads_path.exists():
        return result

    # Count uploads (directories with metadata.json)
    for upload_dir in uploads_path.iterdir():
        if upload_dir.is_dir() and (upload_dir / "metadata.json").exists():
            result["upload_count"] += 1
            for f in upload_dir.iterdir():
                if f.is_file() and f.name != "metadata.json":
                    result["total_files"] += 1
                    result["total_size_bytes"] += f.stat().st_size

    return result


def backup_uploads(backup_dir: Path) -> bool:
    """Backup uploads directory."""
    uploads_path = get_uploads_path()
    if not uploads_path.exists() or not any(uploads_path.iterdir()):
        logger.info("  No uploads to backup")
        return True

    dest = backup_dir / "uploads"
    try:
        shutil.copytree(uploads_path, dest)
        upload_count = len([d for d in dest.iterdir() if d.is_dir()])
        logger.info(f"  Backed up {upload_count} upload(s)")
        return True
    except Exception as e:
        logger.error(f"  Uploads backup failed: {e}")
        return False


def restore_uploads(backup_dir: Path) -> bool:
    """Restore uploads from backup."""
    src = backup_dir / "uploads"
    if not src.exists():
        logger.info("  No uploads backup found")
        return True

    uploads_path = get_uploads_path()
    try:
        # Clear existing
        if uploads_path.exists():
            shutil.rmtree(uploads_path)

        shutil.copytree(src, uploads_path)
        upload_count = len([d for d in uploads_path.iterdir() if d.is_dir()])
        logger.info(f"  Restored {upload_count} upload(s)")
        return True
    except Exception as e:
        logger.error(f"  Uploads restore failed: {e}")
        return False


# =============================================================================
# Combined Operations
# =============================================================================


async def init_databases(
    force_reset: bool = False,
    skip_postgres: bool = False,
) -> bool:
    """Initialize all databases.

    Args:
        force_reset: If True, drop and recreate all data.
        skip_postgres: Skip PostgreSQL initialization.

    Returns:
        True if all enabled databases initialized successfully.
    """
    success = True

    if not skip_postgres:
        logger.info("")
        logger.info("Initializing PostgreSQL...")
        if not await init_postgres(force_reset):
            success = False
            logger.error("PostgreSQL initialization failed")

    # Vector DB (memories + knowledge_index) — skip if not configured
    if not skip_postgres:
        logger.info("")
        logger.info("Initializing Vector DB...")
        if not await init_vector_db(force_reset):
            logger.warning(
                "Vector DB initialization had issues (non-fatal in single-DB mode)"
            )

    # Neo4j (system knowledge base) — non-fatal if not configured
    logger.info("")
    logger.info("Initializing Neo4j (knowledge base)...")
    init_neo4j(force_reset)

    return success


async def verify_databases(
    skip_postgres: bool = False,
) -> dict:
    """Verify all database connections.

    Args:
        skip_postgres: Skip PostgreSQL verification.

    Returns:
        Dict with status for each database.
    """
    result = {}

    if not skip_postgres:
        result["postgres"] = await verify_postgres()
        result["vector_db"] = await verify_vector_db()

    result["neo4j"] = verify_neo4j()

    return result


def backup_databases(backup_dir: Path) -> dict:
    """Backup all databases.

    Args:
        backup_dir: Directory to store backups.

    Returns:
        Dict with backup status for each database.
    """
    backup_dir.mkdir(parents=True, exist_ok=True)
    result = {}

    # Backup PostgreSQL
    postgres_file = backup_dir / "postgres.dump"
    result["postgres"] = {
        "file": "postgres.dump",
        "success": backup_postgres(postgres_file),
    }

    return result


def restore_databases(backup_dir: Path) -> dict:
    """Restore all databases from backup.

    Args:
        backup_dir: Directory containing backups.

    Returns:
        Dict with restore status for each database.
    """
    result = {}

    # Restore PostgreSQL
    postgres_file = backup_dir / "postgres.dump"
    if postgres_file.exists():
        result["postgres"] = {
            "file": "postgres.dump",
            "success": restore_postgres(postgres_file),
        }
    else:
        logger.info("  No PostgreSQL backup found")
        result["postgres"] = {"success": True, "skipped": True}

    return result


# =============================================================================
# CLI
# =============================================================================


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Initialize orchestrator databases.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m orchestrator.init                     # Initialize databases
  python -m orchestrator.init --force-reset       # Reset and reinitialize
  python -m orchestrator.init --verify            # Just verify connectivity
  python -m orchestrator.init --backup ./backup   # Create backup
  python -m orchestrator.init --restore ./backup  # Restore from backup
        """,
    )
    parser.add_argument(
        "--force-reset",
        action="store_true",
        help="Delete all data and recreate databases (WARNING: deletes all data)",
    )
    parser.add_argument(
        "--skip-postgres",
        action="store_true",
        help="Skip PostgreSQL initialization",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Only verify database connectivity, don't initialize",
    )
    parser.add_argument(
        "--backup",
        metavar="PATH",
        help="Create backup to specified directory",
    )
    parser.add_argument(
        "--restore",
        metavar="PATH",
        help="Restore from backup directory",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output",
    )
    return parser.parse_args()


async def main_async(args: argparse.Namespace) -> int:
    """Async main function."""
    # Verify mode
    if args.verify:
        logger.info("")
        logger.info("=" * 60)
        logger.info("Database Connectivity Verification")
        logger.info("=" * 60)
        logger.info("")

        result = await verify_databases(args.skip_postgres)

        if "postgres" in result:
            pg = result["postgres"]
            if pg.get("connected"):
                logger.info("PostgreSQL: connected")
                if not pg.get("all_tables_exist"):
                    logger.warning("  Some tables missing - run init to create them")
            else:
                logger.warning(
                    f"PostgreSQL: not connected ({pg.get('error', 'unknown error')})"
                )

        return 0

    # Backup mode
    if args.backup:
        backup_dir = Path(args.backup)
        logger.info("")
        logger.info("=" * 60)
        logger.info("Database Backup")
        logger.info("=" * 60)
        logger.info(f"Backup directory: {backup_dir}")
        logger.info("")

        result = backup_databases(backup_dir)

        success = all(r.get("success", False) for r in result.values())
        return 0 if success else 1

    # Restore mode
    if args.restore:
        backup_dir = Path(args.restore)
        if not backup_dir.exists():
            logger.error(f"Backup directory not found: {backup_dir}")
            return 1

        logger.info("")
        logger.info("=" * 60)
        logger.info("Database Restore")
        logger.info("=" * 60)
        logger.info(f"Restoring from: {backup_dir}")
        logger.info("")

        result = restore_databases(backup_dir)

        success = all(r.get("success", False) for r in result.values())
        return 0 if success else 1

    # Initialize mode
    logger.info("")
    logger.info("=" * 60)
    logger.info("Orchestrator Database Initialization")
    logger.info("=" * 60)

    if args.force_reset:
        logger.warning("WARNING: Force reset mode - all data will be deleted!")

    success = await init_databases(
        force_reset=args.force_reset,
        skip_postgres=args.skip_postgres,
    )

    logger.info("")
    logger.info("=" * 60)
    if success:
        logger.info("Database Initialization Complete!")
    else:
        logger.error("Database Initialization Failed!")
    logger.info("=" * 60)

    return 0 if success else 1


def main() -> int:
    """Main entry point."""
    args = parse_args()
    setup_logging(args.verbose)

    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\nInitialization cancelled by user.")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
