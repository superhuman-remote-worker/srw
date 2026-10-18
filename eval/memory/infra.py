"""Infrastructure builders for the memory eval harness.

Everything here mirrors a production construction site so the harness
measures the real system:

- vector DB: agent.database.postgres_db.PostgresDB (pgvector codec) against
  a dedicated eval database, schema applied via the production migrations
  runner (orchestrator.database.migrate) over migrations/vector/.
- embeddings: the real EmbeddingService singleton (EMBEDDING_* env vars),
  dimension-probed up front so a misconfigured endpoint aborts the run
  instead of corrupting it.
- auxiliary LLM: the persistent_app session-scoped rebuild
  (resolve_model_settings → LLMConfig → create_llm → AuxiliaryLLM).
- extraction prompt: the persistent_session prompt-matrix resolution.
- MemoryManager: persistent_session._setup_memory's construction, with
  retrieval_timeout=None — production bounds store calls at 5 s and
  *skips* injection on timeout; in an eval that silent skip would corrupt
  metrics, so infra slowness fails loud instead.
"""

import logging
import re
import uuid
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlsplit, urlunsplit

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
VECTOR_MIGRATIONS_DIR = (
    REPO_ROOT / "src" / "orchestrator" / "database" / "migrations" / "vector"
)

#: Cluster pgvector reached over `scripts/port-forward-dbs.sh` (VECTOR_PG_PORT),
#: eval-dedicated database name. Override with EVAL_VECTOR_DSN.
DEFAULT_DSN = "postgresql://srw:srw_password@localhost:5433/srw_eval"

_EVAL_NS = uuid.uuid5(uuid.NAMESPACE_URL, "srw://eval/memory")


def project_uuid(run_id: str, question_id: str) -> uuid.UUID:
    """Deterministic per-(run, question) project scope.

    One LongMemEval question = one project: its haystack sessions share
    memories exactly like jobs/threads of one project do in production.
    The run_id keeps repeat runs from dedup-merging into each other's rows.
    """
    return uuid.uuid5(_EVAL_NS, f"{run_id}/{question_id}")


def session_uuid(run_id: str, question_id: str, session_id: str) -> uuid.UUID:
    """Deterministic per-haystack-session job scope (= memory provenance)."""
    return uuid.uuid5(_EVAL_NS, f"{run_id}/{question_id}/{session_id}")


def _split_dsn(dsn: str) -> tuple:
    parts = urlsplit(dsn)
    dbname = parts.path.lstrip("/")
    if not re.fullmatch(r"[A-Za-z0-9_]+", dbname or ""):
        raise ValueError(f"DSN must end in a simple database name, got {dbname!r}")
    return parts, dbname


async def ensure_database(dsn: str) -> None:
    """Create the eval database (and the vector extension) if missing.

    Connects to the server's maintenance database with the same
    credentials, so the DSN's role must be allowed to CREATE DATABASE and
    CREATE EXTENSION (use the pgvector cluster's superuser/owner role).
    """
    import asyncpg

    parts, dbname = _split_dsn(dsn)
    maint_dsn = urlunsplit(parts._replace(path="/postgres"))

    conn = await asyncpg.connect(maint_dsn)
    try:
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", dbname
        )
        if not exists:
            await conn.execute(f'CREATE DATABASE "{dbname}"')
            logger.info("Created eval database %s", dbname)
    finally:
        await conn.close()

    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    finally:
        await conn.close()


async def apply_migrations(dsn: str) -> None:
    """Apply migrations/vector/ with the production runner (idempotent)."""
    import asyncpg

    from orchestrator.database.migrate import run_migrations

    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    try:
        await run_migrations(pool, VECTOR_MIGRATIONS_DIR)
    finally:
        await pool.close()


async def connect_vector_db(dsn: str) -> Any:
    """Agent-side PostgresDB (registers the pgvector codec per connection)."""
    from agent.database.postgres_db import PostgresDB

    db = PostgresDB(connection_string=dsn, min_connections=1, max_connections=8)
    await db.connect()
    return db


async def require_embedding_service() -> Any:
    """The production embedding singleton, dimension-verified or abort.

    Every metric downstream depends on real embeddings — a missing key or
    wrong-dimension endpoint must kill the run here, not produce rows.
    """
    from shared.runtime.services.embedding_service import get_embedding_service

    service = get_embedding_service()
    if not service.api_key:
        raise RuntimeError(
            "No embedding API key (EMBEDDING_API_KEY / provider key) — "
            "set the EMBEDDING_* environment, see eval/memory/README.md"
        )
    ok = await service.verify_dimensions()
    if ok is False or service.degraded_reason:
        raise RuntimeError(
            f"Embedding endpoint degraded: {service.degraded_reason or 'dimension mismatch'}"
        )
    if ok is None:
        logger.warning(
            "Embedding dimension probe inconclusive — continuing, but a "
            "misconfigured endpoint will fail on first store"
        )
    return service


def build_auxiliary_llm(config: Any) -> Any:
    """Session-scoped AuxiliaryLLM, exactly like persistent_app's rebuild."""
    from shared.runtime.core.loader import LLMConfig, create_llm, resolve_model_settings
    from shared.runtime.services.auxiliary import AuxiliaryLLM

    aux_cfg = config.auxiliary
    if not aux_cfg.model:
        raise RuntimeError(
            "config.auxiliary.model is empty — seam-mode arms need an "
            "extraction model (set it via the arm's auxiliary: block)"
        )
    model_settings = resolve_model_settings(aux_cfg.model, config._deployment_dir)
    aux_llm_config = LLMConfig(
        model=aux_cfg.model,
        base_url=aux_cfg.base_url,
        api_key=aux_cfg.api_key,
        temperature=aux_cfg.temperature,
        top_p=model_settings.get("top_p"),
        top_k=model_settings.get("top_k"),
        model_max_context_tokens=model_settings.get("model_max_context_tokens"),
        max_retries=1,
    )
    inner = create_llm(aux_llm_config, config.limits)
    return AuxiliaryLLM(
        llm=inner,
        max_iterations=aux_cfg.max_iterations,
        timeout=aux_cfg.timeout,
    )


def resolve_extraction_prompt(config: Any) -> str:
    """Prompt-matrix resolution, mirroring persistent_session's resolver.

    Unlike production (which degrades to "" with a warning), an eval run
    with an empty extraction prompt would silently measure the wrong
    system — so this raises.
    """
    from shared.runtime.core.loader import load_auxiliary_prompt

    aux_model = (
        config.auxiliary.model
        or config.llm.get_phase_config("summarization").model
        or config.llm.model
    )
    prompt = load_auxiliary_prompt(config, "memory_extraction", model=aux_model)
    if not prompt:
        raise RuntimeError("memory_extraction prompt resolved empty")
    return prompt


def make_recall_store(
    db: Any,
    embedding_service: Any,
    config: Any,
    job_id: uuid.UUID,
    project_id: uuid.UUID,
) -> Any:
    """Scope-bound RecallStore (persistent_session._setup_memory shape)."""
    from shared.runtime.services.recall_store import RecallStore

    return RecallStore(
        db=db,
        embedding_service=embedding_service,
        job_id=job_id,
        config=config.memory,
        agent_id=config.agent_id,
        project_id=project_id,
        project_ids=[project_id],
    )


def build_manager(
    config: Any,
    *,
    recall_store: Any,
    auxiliary_llm: Optional[Any],
    extraction_prompt: str,
    job_id: uuid.UUID,
    project_id: uuid.UUID,
    knowledge_store: Optional[Any] = None,
) -> Any:
    """Bind the arm's memory.pipeline exactly like a persistent session.

    knowledge_store defaults to None — LongMemEval exercises no kb_write,
    so the kb_notes retriever binds inert (same as a session without
    Neo4j). retrieval_timeout=None per the module docstring.
    """
    from agent.services.memory import MemoryManager, MemoryRuntime
    from shared.runtime.services.memory.ingestion import maybe_attach_ingestion_verdict

    # Ingestion verdicts + bi-temporal supersede (overhaul Phase 4) — attach to
    # the store the capture writers use, so a seam ingest exercises supersede.
    maybe_attach_ingestion_verdict(recall_store, auxiliary_llm, config.memory)

    return MemoryManager.from_config(
        config.memory,
        MemoryRuntime(
            recall_store=recall_store,
            knowledge_store=knowledge_store,
            auxiliary_llm=auxiliary_llm,
            memory_config=config.memory,
            auxiliary_config=config.auxiliary,
            extraction_prompt=extraction_prompt,
            job_id=str(job_id),
            project_id=str(project_id),
            project_ids=[str(project_id)],
            retrieval_timeout=None,
        ),
    )
