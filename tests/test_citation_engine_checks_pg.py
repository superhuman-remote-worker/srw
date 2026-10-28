"""Citation checks, live events and withdrawal on a real vector store.

Citation engine overhaul WP1/WP2 (knowledge-base/knowledge/features/
citation_engine_overhaul.md): a quote that is not in the source fails at once
and never reaches the LLM; an edit is checked again; with no verifier, or a
verifier that fails, a citation reads "not checked" instead of pending
forever; a withdrawn citation keeps its record. Every change is announced to
the session (``citation.created`` before the tool returns, then
``citation.updated`` / ``citation.verdict``) with the row the cockpit reads.

Runs the real vector migrations on a disposable pgvector container.
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest
import pytest_asyncio

from orchestrator.database.migrate import run_migrations

VECTOR_MIGRATIONS = (
    Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/vector"
)

SOURCE_TEXT = (
    "Section 3. Companies must retain accounting records for ten years. "
    "The regulation applies to all books and journals."
)


@pytest.fixture(scope="module")
def vector_dsn():
    testcontainers = pytest.importorskip("testcontainers.postgres")
    container = testcontainers.PostgresContainer("pgvector/pgvector:pg16")
    try:
        container.start()
    except Exception as exc:
        pytest.skip(f"no container runtime for the citation engine test: {exc}")
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql://", 1
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def migrated(vector_dsn):
    pool = await asyncpg.create_pool(vector_dsn, min_size=1, max_size=2)
    try:
        await run_migrations(pool, VECTOR_MIGRATIONS)
    finally:
        await pool.close()
    return vector_dsn


@pytest_asyncio.fixture
async def vector_db(migrated):
    from agent.database.postgres_db import PostgresDB

    db = PostgresDB(connection_string=migrated)
    await db.connect()
    try:
        yield db
    finally:
        await db.close()


def _verdict_aux(verified: bool = True, matched_text=None):
    from shared.runtime.services.auxiliary import CitationVerdict

    aux = MagicMock()
    aux.chain = AsyncMock(
        return_value=CitationVerdict(
            verified=verified,
            similarity_score=0.9 if verified else 0.2,
            matched_text=matched_text,
            reasoning="supports the claim" if verified else "says something else",
        )
    )
    aux.health = MagicMock()
    return aux


def _engine(vector_db, *, aux=None, events=None):
    from agent.citation_engine import CitationContext, CitationEngine

    ctx = CitationContext(session_id=str(uuid.uuid4()), agent_id="pg-test")
    return CitationEngine(
        db=vector_db,
        context=ctx,
        verify_aux=aux,
        verify_prompt="verify" if aux is not None else None,
        on_event=(lambda method, params: events.append((method, params)))
        if events is not None
        else None,
    )


async def _source(engine, text=SOURCE_TEXT):
    # Unique bytes per test, so content-hash dedup never shares rows.
    return await engine.add_custom_source(
        name="GoBD note", content=f"{text} marker={uuid.uuid4()}"
    )


async def _cite(engine, source, quote, claim="Records are kept ten years"):
    return await engine.cite_doc(
        claim=claim,
        source_id=source.id,
        quote_context=quote,
        locator={"page": 1},
        verbatim_quote=quote,
    )


@pytest.mark.asyncio
async def test_found_quote_is_pending_announced_then_verified(vector_db):
    events: list = []
    aux = _verdict_aux(verified=True)
    engine = _engine(vector_db, aux=aux, events=events)
    source = await _source(engine)

    result = await _cite(engine, source, "must retain accounting records for ten years")

    assert result.verification_status.value == "pending"
    # Announced before the tool returned, with the row the cockpit reads.
    method, params = events[0]
    assert method == "citation.created"
    row = params["citation"]
    assert row["id"] == result.citation_id
    assert row["verification_status"] == "pending"
    assert row["source_name"] == "GoBD note"
    assert row["matched_text"].startswith("must retain")
    assert row["withdrawn_at"] is None

    await engine.await_pending_verifications(timeout=10)
    aux.chain.assert_awaited_once()
    cit = await engine.get_citation(result.citation_id)
    assert cit.verification_status.value == "verified"
    assert cit.verification_reason is None
    # The LLM named no passage, so the first check's match is kept.
    assert cit.matched_location["method"] == "exact"
    assert events[-1][0] == "citation.verdict"
    assert events[-1][1]["verification_status"] == "verified"


@pytest.mark.asyncio
async def test_made_up_quote_fails_at_once_without_the_llm(vector_db):
    events: list = []
    aux = _verdict_aux(verified=True)
    engine = _engine(vector_db, aux=aux, events=events)
    source = await _source(engine)

    result = await _cite(engine, source, "records must be destroyed after one year")

    assert result.verification_status.value == "failed"
    assert "do not appear" in result.verification_notes
    await engine.await_pending_verifications(timeout=1)
    aux.chain.assert_not_awaited()
    cit = await engine.get_citation(result.citation_id)
    assert cit.verification_reason == "quote_not_found"
    assert events[0][1]["citation"]["verification_reason"] == "quote_not_found"


@pytest.mark.asyncio
async def test_llm_rejection_records_not_supported(vector_db):
    engine = _engine(vector_db, aux=_verdict_aux(verified=False))
    source = await _source(engine)
    result = await _cite(engine, source, "must retain accounting records")
    await engine.await_pending_verifications(timeout=10)
    cit = await engine.get_citation(result.citation_id)
    assert cit.verification_status.value == "failed"
    assert cit.verification_reason == "not_supported"
    assert cit.verification_notes == "says something else"


@pytest.mark.asyncio
async def test_without_a_verifier_a_citation_is_not_checked(vector_db):
    engine = _engine(vector_db, aux=None)
    source = await _source(engine)
    result = await _cite(engine, source, "must retain accounting records")
    assert result.verification_status.value == "unverified"
    cit = await engine.get_citation(result.citation_id)
    assert cit.verification_reason == "not_checked"
    assert "quote was found" in cit.verification_notes


@pytest.mark.asyncio
async def test_a_failing_verifier_ends_pending_as_not_checked(vector_db):
    aux = MagicMock()
    aux.chain = AsyncMock(side_effect=RuntimeError("provider down"))
    aux.health = MagicMock()
    events: list = []
    engine = _engine(vector_db, aux=aux, events=events)
    source = await _source(engine)
    result = await _cite(engine, source, "must retain accounting records")
    await engine.await_pending_verifications(timeout=10)
    cit = await engine.get_citation(result.citation_id)
    assert cit.verification_status.value == "unverified"
    assert cit.verification_reason == "not_checked"
    assert events[-1][0] == "citation.verdict"
    assert events[-1][1]["verification_status"] == "unverified"


@pytest.mark.asyncio
async def test_edit_is_checked_again(vector_db):
    events: list = []
    aux = _verdict_aux(verified=True)
    engine = _engine(vector_db, aux=aux, events=events)
    source = await _source(engine)
    result = await _cite(engine, source, "records must be destroyed after one year")
    assert result.verification_status.value == "failed"

    await engine.edit_citation(
        result.citation_id, verbatim_quote="must retain accounting records"
    )
    assert ("citation.updated", result.citation_id) in [
        (m, p["citation_id"]) for m, p in events
    ]
    await engine.await_pending_verifications(timeout=10)
    aux.chain.assert_awaited_once()
    cit = await engine.get_citation(result.citation_id)
    assert cit.verification_status.value == "verified"

    # An edit to another made-up quote fails again at once.
    await engine.edit_citation(
        result.citation_id, verbatim_quote="no such words here at all"
    )
    cit = await engine.get_citation(result.citation_id)
    assert cit.verification_status.value == "failed"
    assert cit.verification_reason == "quote_not_found"
    assert cit.similarity_score is None


@pytest.mark.asyncio
async def test_withdrawal_keeps_the_record(vector_db):
    events: list = []
    engine = _engine(vector_db, aux=None, events=events)
    source = await _source(engine)
    result = await _cite(engine, source, "records must be destroyed after one year")

    with pytest.raises(ValueError):
        await engine.withdraw_citation(result.citation_id, "  ")
    withdrawn = await engine.withdraw_citation(
        result.citation_id, "no source says this"
    )
    assert withdrawn.withdrawn_at is not None
    assert withdrawn.withdrawn_reason == "no source says this"
    # The verdict stays; nothing is deleted.
    assert withdrawn.verification_status.value == "failed"
    assert events[-1][0] == "citation.updated"
    assert events[-1][1]["citation"]["withdrawn_reason"] == "no source says this"

    # Idempotent: the first reason is kept.
    again = await engine.withdraw_citation(result.citation_id, "other")
    assert again.withdrawn_reason == "no source says this"

    failed = await engine.list_citations(verification_status="failed")
    assert [c.id for c in failed] == [result.citation_id]
    assert (
        await engine.list_citations(
            verification_status="failed", include_withdrawn=False
        )
        == []
    )
    with pytest.raises(ValueError, match="withdrawn"):
        await engine.edit_citation(result.citation_id, claim="new claim")


@pytest.mark.asyncio
async def test_withdraw_is_scoped_to_the_job(vector_db):
    engine = _engine(vector_db, aux=None)
    source = await _source(engine)
    result = await _cite(engine, source, "must retain accounting records")
    other = _engine(vector_db, aux=None)
    with pytest.raises(ValueError, match="not found"):
        await other.withdraw_citation(result.citation_id, "not mine")


@pytest.mark.asyncio
async def test_aclose_stops_events(vector_db):
    events: list = []
    started = asyncio.Event()

    async def _blocked(*_args, **_kwargs):
        started.set()
        await asyncio.Event().wait()

    aux = MagicMock()
    aux.chain = _blocked
    aux.health = MagicMock()
    engine = _engine(vector_db, aux=aux, events=events)
    source = await _source(engine)
    await _cite(engine, source, "must retain accounting records")
    await started.wait()
    await engine.aclose(timeout=1.0)
    assert [m for m, _ in events] == ["citation.created"]


class _Workspace:
    def __init__(self, files):
        self.files = dict(files)

    def read_file(self, path):
        return self.files[path]

    def write_file(self, path, content):
        self.files[path] = content


@pytest.mark.asyncio
async def test_delivery_appends_footnotes_in_first_mention_order(vector_db):
    from agent.citation_engine.delivery import deliver_documents

    engine = _engine(vector_db, aux=None)
    source = await _source(engine)
    good = await _cite(engine, source, "must retain accounting records")
    bad = await _cite(engine, source, "records must be destroyed after one year")
    unused = await _cite(engine, source, "applies to all books and journals")
    doc = (
        f"# Report\n\nSecond first [^c{bad.citation_id}]. Then [^c{good.citation_id}], "
        f"again [^c{bad.citation_id}]. Code `[^c{unused.citation_id}]` is literal.\n"
    )
    ws = _Workspace({"output/report.md": doc, "output/data.csv": "a,b"})

    result = await deliver_documents(
        engine, ws, ["output/report.md", "output/data.csv"], verify_timeout=1
    )

    text = ws.files["output/report.md"]
    defs = [line for line in text.splitlines() if line.startswith("[^c")]
    assert [d.split(":")[0] for d in defs] == [
        f"[^c{bad.citation_id}]",
        f"[^c{good.citation_id}]",
    ]
    assert "**Unverified:** the quoted words are not in the source." in defs[0]
    assert "(Not checked.)" in defs[1]
    assert ws.files["output/data.csv"] == "a,b"
    assert result["files"] == {"output/report.md": 2}
    report = result["citations"]
    assert report["total"] == 3
    assert report["unverified_count"] == 1
    assert report["unverified"][0]["handle"] == f"[^c{bad.citation_id}]"

    # Idempotent: a second delivery leaves the file as it is.
    await deliver_documents(engine, ws, ["output/report.md"], verify_timeout=1)
    assert ws.files["output/report.md"] == text


@pytest.mark.asyncio
async def test_two_units_citing_the_same_bytes_keep_their_own_names(vector_db):
    """D1: the second registrant no longer sees the first one's file name."""
    content = f"Shared annual report text, retain records ten years. {uuid.uuid4()}"
    first = _engine(vector_db, aux=None)
    second = _engine(vector_db, aux=None)
    a = await first.add_custom_source(name="alice-private-notes.txt", content=content)
    b = await second.add_custom_source(name="report-2025.txt", content=content)
    assert a.id == b.id  # content is stored once

    assert (await first.get_source(a.id)).name == "alice-private-notes.txt"
    assert (await second.get_source(b.id)).name == "report-2025.txt"
    assert [s.name for s in await second.list_sources()] == ["report-2025.txt"]

    events: list = []
    second._on_event = lambda method, params: events.append(params)
    await _cite(second, b, "retain records ten years")
    assert events[0]["citation"]["source_name"] == "report-2025.txt"


@pytest.mark.asyncio
async def test_deleting_a_session_removes_its_citations_and_orphan_sources(vector_db):
    session = _engine(vector_db, aux=None)
    other = _engine(vector_db, aux=None)
    own = await _source(session)
    shared_text = f"Shared bytes. {uuid.uuid4()}"
    shared = await session.add_custom_source(name="s.txt", content=shared_text)
    await other.add_custom_source(name="o.txt", content=shared_text)
    await _cite(session, own, "must retain accounting records")
    thread = session.context.session_id

    async with vector_db.acquire() as conn:
        await conn.execute(
            "INSERT INTO memories (job_id, content) VALUES ($1::uuid, 'keep me')",
            thread,
        )
        await conn.execute("SELECT retire_session_citations($1::uuid)", thread)
        assert not await conn.fetchval(
            "SELECT count(*) FROM citations WHERE job_id = $1::uuid", thread
        )
        assert not await conn.fetchval(
            "SELECT count(*) FROM sources WHERE id = $1", own.id
        )
        # Still linked by the other unit, so kept.
        assert await conn.fetchval(
            "SELECT count(*) FROM sources WHERE id = $1", shared.id
        )
        # Session memories belong to the project and stay.
        assert await conn.fetchval(
            "SELECT count(*) FROM memories WHERE job_id = $1::uuid", thread
        )
