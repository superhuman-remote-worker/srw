"""Offline tests for the memory eval harness (eval/memory/).

Everything here runs without a database, embedding endpoint, or LLM —
infra-touching pieces (stores, manager) are replaced by fakes injected
through HarnessHandles' factory seams. The committed fixture
eval/memory/fixtures/tiny_longmemeval.json uses the real LongMemEval
schema, so the loader tests double as schema documentation.
"""

import json
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from eval.memory import infra
from eval.memory.arms import ArmSpec, IngestionOptions, _apply_auxiliary_overrides
from eval.memory.contradiction import (
    classify_answer,
    load_probe_meta,
    retrieval_outcome,
    summarize_probe,
)
from eval.memory.datasets import (
    LMESession,
    LMETurn,
    load_longmemeval,
    subset_questions,
)
from eval.memory.ingest import (
    EMBED_TOKEN_CAP,
    VERBATIM_MAX_CHARS,
    HarnessHandles,
    PrimedEmbedding,
    _verbatim_round_texts,
    ingest_question,
)
from eval.memory.lifecycle import (
    classify_case,
    load_lifecycle_meta,
    summarize,
)
from eval.memory.judge import (
    JUDGE_MAX_TOKENS,
    LLMRoute,
    anscheck_prompt,
    calibrate,
    judge_row,
    parse_verdict,
    reader_messages,
    summarize_judgements,
)
from eval.memory.metrics import (
    aggregate,
    collapse_to_sessions,
    coverage_at_k,
    first_hit_rank,
    ndcg_at_k,
    question_metrics,
    recall_at_k,
)
from eval.memory.query import answer_retrieval, session_ranking
from eval.memory.report import render_comparison, render_markdown
from eval.memory.run import _existing_question_ids
from shared.runtime.core.loader import QueryConfig
from agent.services.memory.types import AssembleStats, InjectionBlock, MemoryPayload

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = REPO_ROOT / "eval" / "memory" / "fixtures" / "tiny_longmemeval.json"
PROBE_FIXTURE = REPO_ROOT / "eval" / "memory" / "fixtures" / "contradiction_probe.json"
LIFECYCLE_FIXTURE = REPO_ROOT / "eval" / "memory" / "fixtures" / "lifecycle_probe.json"
ARMS_DIR = REPO_ROOT / "eval" / "memory" / "arms"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeDB:
    """Just enough of PostgresDB for provenance + count queries."""

    def __init__(self):
        self.rows = []  # dicts: id, job_id, project_id

    async def fetch(self, query, project):
        return [r for r in self.rows if r["project_id"] == project]

    async def fetchval(self, query, project):
        return len([r for r in self.rows if r["project_id"] == project])


class FakeStore:
    """Records store() kwargs; mirrors rows into the FakeDB."""

    def __init__(self, job_id, project_id, db):
        self.job_id = job_id
        self.project_id = project_id
        self.db = db
        self.stored = []

    async def store(self, **kwargs):
        self.stored.append(kwargs)
        mem_id = uuid.uuid4()
        self.db.rows.append(
            {"id": mem_id, "job_id": self.job_id, "project_id": self.project_id}
        )
        return mem_id


class FakeManager:
    """Records capture()/assemble() traffic; returns a canned payload."""

    def __init__(self, store, job_id, project_id, payload=None):
        self.store = store
        self.job_id = job_id
        self.project_id = project_id
        self.captures = []
        self.assembles = []
        self.payload = payload or MemoryPayload(blocks=[], stats=AssembleStats())

    async def capture(self, event):
        self.captures.append(event)

    async def assemble(self, req):
        self.assembles.append(req)
        return self.payload


def make_handles(db=None, payload=None, run_id="testrun"):
    """HarnessHandles wired entirely with fakes."""
    db = db if db is not None else FakeDB()
    stores = []
    managers = []

    def store_factory(job_id, project_id):
        store = FakeStore(job_id, project_id, db)
        stores.append(store)
        return store

    def manager_factory(store, job_id, project_id):
        manager = FakeManager(store, job_id, project_id, payload=payload)
        managers.append(manager)
        return manager

    handles = HarnessHandles(
        config=SimpleNamespace(
            llm=SimpleNamespace(model="test-model"),
            # Real MemoryConfig shape for query formation (digest off, as
            # the production default) — _form_query_text reads it.
            memory=SimpleNamespace(query=QueryConfig()),
        ),
        run_id=run_id,
        db=db,
        store_factory=store_factory,
        manager_factory=manager_factory,
    )
    return handles, stores, managers


# ---------------------------------------------------------------------------
# Dataset loading
# ---------------------------------------------------------------------------


class TestDatasetLoader:
    def test_fixture_loads(self):
        questions = load_longmemeval(str(FIXTURE))
        assert [q.question_id for q in questions] == [
            "tiny_single_1",
            "tiny_multi_1",
            "tiny_cat_1_abs",
        ]
        single = questions[0]
        assert single.question_type == "single-session-user"
        assert len(single.sessions) == 3
        assert single.evidence_session_ids == frozenset({"s2_dog"})
        assert not single.is_abstention
        assert single.sessions[0].date.startswith("2023/05/01")

        multi = questions[1]
        assert multi.evidence_session_ids == frozenset({"s1_trip_book", "s3_trip_move"})

        abstention = questions[2]
        assert abstention.is_abstention
        assert abstention.evidence_session_ids == frozenset()

    def test_has_answer_turn_labels(self):
        questions = load_longmemeval(str(FIXTURE))
        dog_session = questions[0].sessions[1]
        assert dog_session.turns[0].has_answer
        assert not dog_session.turns[2].has_answer

    def test_rounds_pairing(self):
        session = LMESession(
            session_id="s",
            turns=[
                LMETurn(role="assistant", content="hello first"),
                LMETurn(role="user", content="u1"),
                LMETurn(role="assistant", content="a1"),
                LMETurn(role="user", content="trailing"),
            ],
        )
        rounds = list(session.rounds())
        # assistant-first pairs with an empty user; trailing user yields (u, None)
        assert rounds[0][0].content == "" and rounds[0][1].content == "hello first"
        assert rounds[1][0].content == "u1" and rounds[1][1].content == "a1"
        assert rounds[2][0].content == "trailing" and rounds[2][1] is None

    def test_session_count_mismatch_raises(self, tmp_path):
        broken = json.loads(FIXTURE.read_text())
        broken[0]["haystack_session_ids"] = broken[0]["haystack_session_ids"][:-1]
        path = tmp_path / "broken.json"
        path.write_text(json.dumps(broken))
        with pytest.raises(ValueError, match="session ids"):
            load_longmemeval(str(path))


class TestSubset:
    def test_stratified_limit_covers_types(self):
        questions = load_longmemeval(str(FIXTURE))
        picked = subset_questions(questions, limit=2, seed=1)
        assert len(picked) == 2
        assert {q.question_type for q in picked} == {
            "single-session-user",
            "multi-session",
        }

    def test_deterministic(self):
        questions = load_longmemeval(str(FIXTURE))
        a = [q.question_id for q in subset_questions(questions, limit=2, seed=7)]
        b = [q.question_id for q in subset_questions(questions, limit=2, seed=7)]
        assert a == b

    def test_ids_filter(self):
        questions = load_longmemeval(str(FIXTURE))
        picked = subset_questions(questions, ids=["tiny_multi_1"])
        assert [q.question_id for q in picked] == ["tiny_multi_1"]
        with pytest.raises(ValueError, match="not in dataset"):
            subset_questions(questions, ids=["nope"])

    def test_type_filter(self):
        questions = load_longmemeval(str(FIXTURE))
        picked = subset_questions(questions, types=["multi-session"])
        assert [q.question_id for q in picked] == ["tiny_multi_1"]


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


class TestMetrics:
    def test_collapse(self):
        assert collapse_to_sessions(["a", "b", "a", "c", "b"]) == ["a", "b", "c"]

    def test_recall(self):
        evidence = frozenset({"b"})
        assert recall_at_k(["a", "b", "c"], evidence, 1) == 0.0
        assert recall_at_k(["a", "b", "c"], evidence, 3) == 1.0
        assert recall_at_k([], evidence, 5) == 0.0
        assert recall_at_k(["a"], frozenset(), 5) is None

    def test_coverage(self):
        evidence = frozenset({"b", "d"})
        assert coverage_at_k(["b", "a", "c"], evidence, 3) == 0.5
        assert coverage_at_k(["b", "a", "d"], evidence, 3) == 1.0

    def test_ndcg(self):
        evidence = frozenset({"b"})
        # hit at rank 2: dcg = 1/log2(3); idcg = 1/log2(2) = 1
        assert ndcg_at_k(["a", "b", "c"], evidence, 3) == pytest.approx(
            0.6309, abs=1e-3
        )
        two = frozenset({"b", "d"})
        # hits at ranks 1+3 vs ideal ranks 1+2
        expected = (1.0 + 1.0 / 2.0) / (1.0 + 0.6309)
        assert ndcg_at_k(["b", "a", "d"], two, 3) == pytest.approx(expected, abs=1e-3)
        assert ndcg_at_k(["a"], frozenset(), 3) is None

    def test_first_hit(self):
        assert first_hit_rank(["a", "b"], frozenset({"b"})) == 2
        assert first_hit_rank(["a"], frozenset({"b"})) is None
        assert first_hit_rank(["a"], frozenset()) is None

    def test_question_metrics_keys(self):
        out = question_metrics(["a", "b"], frozenset({"b"}), ks=[1, 2])
        assert set(out) == {
            "recall@1",
            "coverage@1",
            "ndcg@1",
            "recall@2",
            "coverage@2",
            "ndcg@2",
            "first_hit_rank",
        }

    def test_aggregate(self):
        rows = [
            {
                "question_type": "multi-session",
                "metrics": {"recall@5": 1.0},
                "cost": {"tokens_injected": 100},
            },
            {
                "question_type": "multi-session",
                "metrics": {"recall@5": 0.0},
                "cost": {"tokens_injected": 300},
            },
            {
                "question_type": "single-session-user",
                "is_abstention": True,
                "metrics": {"recall@5": None},
                "cost": {"tokens_injected": 50},
            },
        ]
        summary = aggregate(rows)
        assert summary["questions"] == 3
        assert summary["abstentions"] == 1
        assert summary["overall"]["recall@5"] == 0.5  # None excluded
        assert summary["overall"]["scored_questions"] == 2
        assert summary["cost"]["tokens_injected"] == 150.0  # all rows
        assert summary["by_type"]["multi-session"]["questions"] == 2


# ---------------------------------------------------------------------------
# Scope uuids
# ---------------------------------------------------------------------------


class TestScopeUuids:
    def test_deterministic_and_distinct(self):
        assert infra.project_uuid("r1", "q1") == infra.project_uuid("r1", "q1")
        assert infra.project_uuid("r1", "q1") != infra.project_uuid("r2", "q1")
        assert infra.project_uuid("r1", "q1") != infra.project_uuid("r1", "q2")
        s1 = infra.session_uuid("r1", "q1", "sA")
        assert s1 == infra.session_uuid("r1", "q1", "sA")
        assert s1 != infra.session_uuid("r1", "q1", "sB")
        assert s1 != infra.project_uuid("r1", "q1")


# ---------------------------------------------------------------------------
# Ingestion drivers
# ---------------------------------------------------------------------------


class TestSeamIngest:
    @pytest.fixture
    def question(self):
        return load_longmemeval(str(FIXTURE))[0]  # 3 sessions: 2+2+1 rounds

    @pytest.mark.asyncio
    async def test_event_sequence(self, question):
        handles, stores, managers = make_handles()
        arm = ArmSpec(name="t", ingestion=IngestionOptions(mode="seam"))
        result = await ingest_question(question, arm, handles)

        assert result.sessions == 3
        assert result.turns == 5
        assert result.assembles == 5  # read path once per round
        assert len(managers) == 3

        for manager, rounds in zip(managers, (2, 2, 1)):
            kinds = [e.kind for e in manager.captures]
            assert kinds == ["turn_end"] * rounds + ["session_end"]
            turn_counts = [e.turn_count for e in manager.captures[:-1]]
            assert turn_counts == list(range(1, rounds + 1))
            # message lists grow by 2 per round (Human + AI), snapshots stable
            lengths = [len(e.messages) for e in manager.captures[:-1]]
            assert lengths == [2 * (i + 1) for i in range(rounds)]

    @pytest.mark.asyncio
    async def test_read_path_query_is_last_user_message(self, question):
        handles, _, managers = make_handles()
        arm = ArmSpec(name="t", ingestion=IngestionOptions(mode="seam"))
        await ingest_question(question, arm, handles)
        first = managers[0].assembles[0]
        assert first.query_text == question.sessions[0].turns[0].content

    @pytest.mark.asyncio
    async def test_read_path_digest_flag_switches_query_formation(self, question):
        # memory.query.digest=True (e.g. via a config_overrides arm) routes
        # per-turn queries through the unified digest — later assembles see
        # a windowed conversation, not just the last user message.
        handles, _, managers = make_handles()
        handles.config.memory.query = QueryConfig(digest=True, digest_window=4)
        arm = ArmSpec(name="t", ingestion=IngestionOptions(mode="seam"))
        await ingest_question(question, arm, handles)
        manager = managers[0]
        last = manager.assembles[-1]
        rounds = list(question.sessions[0].rounds())
        # The current user message is always the digest's focus line...
        assert rounds[-1][0].content[:80] in last.query_text
        if len(rounds) > 1:
            # ...and with history the window adds prior turns (legacy
            # formation would be the bare last user message).
            assert "\n" in last.query_text
            assert last.query_text != rounds[-1][0].content

    @pytest.mark.asyncio
    async def test_read_path_off(self, question):
        handles, _, managers = make_handles()
        arm = ArmSpec(
            name="t",
            ingestion=IngestionOptions(mode="seam", read_path_per_turn=False),
        )
        result = await ingest_question(question, arm, handles)
        assert result.assembles == 0
        assert all(not m.assembles for m in managers)

    @pytest.mark.asyncio
    async def test_scoping(self, question):
        handles, _, managers = make_handles()
        arm = ArmSpec(name="t", ingestion=IngestionOptions(mode="seam"))
        await ingest_question(question, arm, handles)
        projects = {m.project_id for m in managers}
        assert len(projects) == 1
        jobs = [m.job_id for m in managers]
        assert len(set(jobs)) == 3
        assert projects.pop() not in jobs

    @pytest.mark.asyncio
    async def test_date_prefix(self, question):
        handles, _, managers = make_handles()
        arm = ArmSpec(
            name="t",
            ingestion=IngestionOptions(mode="seam", date_prefix=True),
        )
        await ingest_question(question, arm, handles)
        first_event = managers[0].captures[0]
        assert first_event.messages[0].content.startswith("[Session date: 2023/05/01")
        # only the session's first user message is prefixed
        second_event = managers[0].captures[1]
        assert second_event.messages[2].content == question.sessions[0].turns[2].content

    @pytest.mark.asyncio
    async def test_assemble_errors_surface(self, question):
        payload = MemoryPayload(
            blocks=[], stats=AssembleStats(errors=["retriever:x: Boom: y"])
        )
        handles, _, _ = make_handles(payload=payload)
        arm = ArmSpec(name="t", ingestion=IngestionOptions(mode="seam"))
        result = await ingest_question(question, arm, handles)
        assert result.errors
        assert "Boom" in result.errors[0]


class TestVerbatimIngest:
    @pytest.mark.asyncio
    async def test_store_kwargs(self):
        question = load_longmemeval(str(FIXTURE))[0]
        handles, stores, managers = make_handles()
        arm = ArmSpec(
            name="t",
            ingestion=IngestionOptions(mode="verbatim", verbatim_importance=0.7),
        )
        result = await ingest_question(question, arm, handles)

        assert not managers  # no manager in verbatim mode
        assert result.turns == 5
        assert result.stored_memories == 5  # FakeDB count

        first = stores[0].stored[0]
        assert "remaining_turns" not in first
        assert first["importance"] == 0.7
        assert first["source"] == "observer"
        assert "User: Can you help me plan" in first["content"]
        assert "Assistant: Sure!" in first["content"]
        assert first["source_turn_start"] == 0
        assert first["source_turn_end"] == 1

    @pytest.mark.asyncio
    async def test_date_prefix(self):
        question = load_longmemeval(str(FIXTURE))[0]
        handles, stores, _ = make_handles()
        arm = ArmSpec(
            name="t",
            ingestion=IngestionOptions(mode="verbatim", date_prefix=True),
        )
        await ingest_question(question, arm, handles)
        assert "[Session date: 2023/05/01" in stores[0].stored[0]["content"]
        assert "[Session date" not in stores[0].stored[1]["content"]


class FakeEmbeddingService:
    """Counts single vs batch calls; batch vectors are index-tagged."""

    model = "fake-embed"

    def __init__(self):
        self.embed_calls = []
        self.batch_calls = []

    async def embed(self, text):
        self.embed_calls.append(text)
        return [-1.0]

    async def embed_batch(self, texts):
        self.batch_calls.append(list(texts))
        base = sum(len(c) for c in self.batch_calls[:-1])
        return [[float(base + i)] for i in range(len(texts))]


class TestPrimedEmbedding:
    @pytest.mark.asyncio
    async def test_primed_hits_skip_single_calls(self):
        inner = FakeEmbeddingService()
        primed = PrimedEmbedding(inner, batch_size=2)
        await primed.prime(["a", "b", "c"])

        assert [len(c) for c in inner.batch_calls] == [2, 1]  # chunked
        assert await primed.embed("a") == [0.0]
        assert await primed.embed("c") == [2.0]  # vector follows the text
        assert inner.embed_calls == []  # never fell back

    @pytest.mark.asyncio
    async def test_miss_falls_back_to_inner(self):
        inner = FakeEmbeddingService()
        primed = PrimedEmbedding(inner)
        await primed.prime(["a"])
        assert await primed.embed("unprimed") == [-1.0]
        assert inner.embed_calls == ["unprimed"]

    @pytest.mark.asyncio
    async def test_prime_dedups_and_delegates_attrs(self):
        inner = FakeEmbeddingService()
        primed = PrimedEmbedding(inner, batch_size=10)
        await primed.prime(["a", "a", "b"])
        assert inner.batch_calls == [["a", "b"]]
        await primed.prime(["b", "c"])  # already-cached text not re-sent
        assert inner.batch_calls[1] == ["c"]
        assert primed.model == "fake-embed"

    @pytest.mark.asyncio
    async def test_verbatim_ingest_primes_exactly_the_stored_texts(self):
        question = load_longmemeval(str(FIXTURE))[0]
        handles, stores, _ = make_handles()
        embedding = FakeEmbeddingService()
        handles.embedding = embedding
        arm = ArmSpec(
            name="t",
            ingestion=IngestionOptions(mode="verbatim", date_prefix=True),
        )
        await ingest_question(question, arm, handles)

        primed_texts = [t for chunk in embedding.batch_calls for t in chunk]
        stored_texts = [s["content"] for store in stores for s in store.stored]
        assert primed_texts == stored_texts  # incl. first-round date prefix
        expected = [t for s in question.sessions for t in _verbatim_round_texts(s, arm)]
        assert stored_texts == expected

    @pytest.mark.asyncio
    async def test_seam_mode_never_batch_primes(self):
        question = load_longmemeval(str(FIXTURE))[0]
        handles, _, managers = make_handles()
        embedding = FakeEmbeddingService()
        handles.embedding = embedding
        arm = ArmSpec(name="t", ingestion=IngestionOptions(mode="seam"))
        await ingest_question(question, arm, handles)
        assert embedding.batch_calls == []
        assert managers  # went through the manager path


# ---------------------------------------------------------------------------
# Query / provenance
# ---------------------------------------------------------------------------


def _payload_for(records):
    """MemoryPayload with one memory block whose items carry record ids."""
    return MemoryPayload(
        blocks=[
            InjectionBlock(
                kind="memory",
                items=[{"record_id": str(rid), "token_count": 10} for rid in records],
            )
        ],
        stats=AssembleStats(tokens_injected=30, candidates_total=3, latency_ms=1.5),
    )


class TestSessionRanking:
    def test_collapse_and_skip_unresolvable(self):
        r1, r2, r3 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        job_a, job_b = uuid.uuid4(), uuid.uuid4()
        payload = _payload_for([r1, r2, r3, uuid.uuid4()])
        record_to_job = {str(r1): job_a, str(r2): job_b, str(r3): job_a}
        ranked = session_ranking(payload, record_to_job, {job_a: "sA", job_b: "sB"})
        assert ranked == ["sA", "sB"]


class TestAnswerRetrieval:
    @pytest.mark.asyncio
    async def test_end_to_end_with_fakes(self):
        question = load_longmemeval(str(FIXTURE))[0]  # evidence: s2_dog
        run_id = "testrun"
        job_workout = infra.session_uuid(run_id, question.question_id, "s3_workout")
        job_dog = infra.session_uuid(run_id, question.question_id, "s2_dog")
        project = infra.project_uuid(run_id, question.question_id)

        rec_workout, rec_dog = uuid.uuid4(), uuid.uuid4()
        db = FakeDB()
        db.rows = [
            {"id": rec_workout, "job_id": job_workout, "project_id": project},
            {"id": rec_dog, "job_id": job_dog, "project_id": project},
        ]
        payload = _payload_for([rec_workout, rec_dog])
        handles, _, managers = make_handles(db=db, payload=payload, run_id=run_id)
        arm = ArmSpec(name="t", ks=[1, 3])

        row = await answer_retrieval(question, arm, handles)

        assert managers[-1].assembles[0].query_text == question.question
        assert row["ranked_sessions"] == ["s3_workout", "s2_dog"]
        assert row["metrics"]["recall@1"] == 0.0
        assert row["metrics"]["recall@3"] == 1.0
        assert row["metrics"]["first_hit_rank"] == 2.0
        assert row["cost"]["memories_injected"] == 2
        assert row["cost"]["tokens_injected"] == 30
        assert row["items"][1]["session"] == "s2_dog"

    @pytest.mark.asyncio
    async def test_abstention_metrics_are_none(self):
        question = load_longmemeval(str(FIXTURE))[2]
        handles, _, _ = make_handles()
        arm = ArmSpec(name="t", ks=[5])
        row = await answer_retrieval(question, arm, handles)
        assert row["is_abstention"]
        assert row["metrics"]["recall@5"] is None


# ---------------------------------------------------------------------------
# Arms
# ---------------------------------------------------------------------------


class TestArms:
    def test_committed_arm_files_parse(self):
        flat = ArmSpec.from_file(str(ARMS_DIR / "flat_verbatim.yaml"))
        assert flat.ingestion.mode == "verbatim"
        assert flat.config_overrides["memory"]["dedup_threshold"] == 1.01

        current = ArmSpec.from_file(str(ARMS_DIR / "persistent_current.yaml"))
        assert current.ingestion.mode == "seam"
        assert current.ingestion.read_path_per_turn
        assert current.ks == [1, 3, 5, 10]

    def test_invalid_mode_raises(self):
        with pytest.raises(ValueError, match="ingestion.mode"):
            IngestionOptions(mode="bogus")

    def test_build_agent_config_flat(self):
        from eval.memory.arms import build_agent_config

        arm = ArmSpec.from_file(str(ARMS_DIR / "flat_verbatim.yaml"))
        config = build_agent_config(arm)
        assert config.memory.enabled
        assert config.memory.manager_enabled
        assert config.memory.dedup_threshold == 1.01
        assert "recall_hybrid" in config.memory.pipeline.retrievers

    def test_build_agent_config_persistent_writers(self):
        from eval.memory.arms import build_agent_config

        arm = ArmSpec.from_file(str(ARMS_DIR / "persistent_current.yaml"))
        config = build_agent_config(arm)
        assert config.memory.pipeline.writers == [
            "persistent_interval_extractor",
            "pre_compaction_extractor",
            "teardown_extractor",
        ]

    def test_seam_arm_with_no_writers_raises(self):
        from eval.memory.arms import build_agent_config

        arm = ArmSpec(
            name="t",
            config_overrides={"memory": {"pipeline": {"writers": []}}},
            ingestion=IngestionOptions(mode="seam"),
        )
        with pytest.raises(ValueError, match="store nothing"):
            build_agent_config(arm)

    def test_auxiliary_overrides(self, monkeypatch):
        config = SimpleNamespace(
            auxiliary=SimpleNamespace(
                model="m0", base_url=None, api_key=None, timeout=120
            )
        )
        monkeypatch.setenv("EVAL_TEST_KEY", "sk-test")
        _apply_auxiliary_overrides(
            config,
            {"model": "m1", "base_url": "http://x", "api_key_env": "EVAL_TEST_KEY"},
        )
        assert config.auxiliary.model == "m1"
        assert config.auxiliary.base_url == "http://x"
        assert config.auxiliary.api_key == "sk-test"

    def test_auxiliary_missing_env_raises(self, monkeypatch):
        monkeypatch.delenv("EVAL_TEST_KEY_MISSING", raising=False)
        config = SimpleNamespace(auxiliary=SimpleNamespace(api_key=None))
        with pytest.raises(ValueError, match="EVAL_TEST_KEY_MISSING"):
            _apply_auxiliary_overrides(config, {"api_key_env": "EVAL_TEST_KEY_MISSING"})


# ---------------------------------------------------------------------------
# Runner plumbing + reports
# ---------------------------------------------------------------------------


class TestRunnerPlumbing:
    def test_existing_question_ids(self, tmp_path):
        path = tmp_path / "results.jsonl"
        path.write_text(
            json.dumps({"question_id": "a"})
            + "\n"
            + "not json\n"
            + json.dumps({"question_id": "b"})
            + "\n"
        )
        assert _existing_question_ids(path) == {"a", "b"}

    def test_existing_question_ids_missing_file(self, tmp_path):
        assert _existing_question_ids(tmp_path / "nope.jsonl") == set()


class TestReport:
    def _summary(self, recall):
        return {
            "questions": 4,
            "abstentions": 1,
            "overall": {"recall@5": recall, "ndcg@5": 0.5, "scored_questions": 3},
            "cost": {"tokens_injected": 123.0},
            "by_type": {
                "multi-session": {"recall@5": recall, "ndcg@5": 0.5, "questions": 4}
            },
            "arm": {"name": "armX"},
        }

    def test_render_markdown(self):
        text = render_markdown(self._summary(0.75))
        assert "**overall**" in text
        assert "0.750" in text
        assert "multi-session" in text
        assert "tokens_injected: 123.0" in text

    def test_render_comparison(self):
        text = render_comparison(self._summary(0.5), self._summary(0.75))
        assert "+0.250" in text
        assert "armX" in text


# ---------------------------------------------------------------------------
# End-task judge
# ---------------------------------------------------------------------------


def fake_chat_client(responses):
    """OpenAI-shaped async client returning canned responses in order."""
    remaining = list(responses)
    calls = []

    async def create(**kwargs):
        calls.append(kwargs)
        content = remaining.pop(0)
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=content))]
        )

    client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )
    return client, calls


class TestJudge:
    def test_anscheck_prompt_selects_paper_templates(self):
        p = anscheck_prompt("multi-session", "Q?", "A", "R", abstention=False)
        assert "contains the correct answer" in p and "Q?" in p and "R" in p
        p = anscheck_prompt("temporal-reasoning", "Q?", "A", "R", abstention=False)
        assert "off-by-one errors" in p
        p = anscheck_prompt("knowledge-update", "Q?", "A", "R", abstention=False)
        assert "updated answer" in p
        p = anscheck_prompt(
            "single-session-preference", "Q?", "A", "R", abstention=False
        )
        assert "Rubric: A" in p
        # abstention overrides the type-specific template
        p = anscheck_prompt("multi-session", "Q?", "expl", "R", abstention=True)
        assert "unanswerable" in p and "Explanation: expl" in p
        with pytest.raises(ValueError):
            anscheck_prompt("nope", "Q?", "A", "R", abstention=False)

    def test_parse_verdict(self):
        assert parse_verdict("Yes.")
        assert parse_verdict("YES — the response is correct")
        assert not parse_verdict("No")
        assert not parse_verdict("incorrect")
        # reasoning models: only the post-think tail counts
        assert not parse_verdict("<think>could be yes, hmm</think>No")
        assert parse_verdict("<think>checking... it is wrong? no — right</think>Yes")
        assert not parse_verdict("<thinking>yes yes yes</thinking>no")

    def test_reader_messages(self):
        row = {
            "question": "What did I adopt?",
            "question_date": "2023/06/01",
            "injected_context": "## Memories\n- adopted a dog",
        }
        msgs = reader_messages(row)
        assert msgs[0]["role"] == "system"
        assert "adopted a dog" in msgs[0]["content"]
        assert msgs[1]["content"].startswith("(Current date: 2023/06/01)")

        empty = reader_messages({"question": "Q?", "injected_context": ""})
        assert "(no memories retrieved)" in empty[0]["content"]
        assert empty[1]["content"] == "Q?"

    @pytest.mark.asyncio
    async def test_judge_row_wires_reader_into_judge(self):
        row = {
            "question_id": "q1",
            "question_type": "single-session-user",
            "is_abstention": False,
            "question": "What pet?",
            "answer": "a dog",
            "question_date": "",
            "injected_context": "- user adopted a dog named Rex",
        }
        reader_client, reader_calls = fake_chat_client(["You adopted a dog."])
        judge_client, judge_calls = fake_chat_client(["yes"])
        reader = LLMRoute(model="reader-m")
        judge = LLMRoute(model="judge-m")

        out = await judge_row(row, reader_client, judge_client, reader, judge)

        assert out["correct"] is True
        assert out["hypothesis"] == "You adopted a dog."
        # reader saw the injected context; judge saw the reader's answer
        assert "adopted a dog named Rex" in reader_calls[0]["messages"][0]["content"]
        judge_prompt = judge_calls[0]["messages"][0]["content"]
        assert "You adopted a dog." in judge_prompt and "What pet?" in judge_prompt
        # paper protocol keeps temperature 0; max_tokens is raised so
        # reasoning judges can think before the verdict token
        assert judge_calls[0]["max_tokens"] == JUDGE_MAX_TOKENS
        assert judge_calls[0]["temperature"] == 0.0

    def test_summarize_judgements(self):
        rows = [
            {"question_type": "multi-session", "is_abstention": False, "correct": True},
            {
                "question_type": "multi-session",
                "is_abstention": False,
                "correct": False,
            },
            {
                "question_type": "knowledge-update",
                "is_abstention": True,
                "correct": True,
            },
        ]
        s = summarize_judgements(rows)
        assert s["questions"] == 3
        assert s["accuracy"] == round(2 / 3, 4)
        assert s["accuracy_answerable"] == 0.5
        assert s["abstention_score"] == 1.0
        assert s["by_type"]["multi-session"]["n"] == 2

    def test_calibrate(self):
        judgements = [
            {"question_id": "a", "correct": True},
            {"question_id": "b", "correct": False},
            {"question_id": "c", "correct": True},
        ]
        labels = {"a": True, "b": True, "x": False}
        out = calibrate(judgements, labels)
        assert out["overlap"] == 2
        assert out["agreement"] == 0.5
        assert out["disagreements"] == ["b"]

    def test_route_from_env_fallback(self, monkeypatch):
        monkeypatch.setenv("EVAL_AUX_MODEL", "aux-m")
        monkeypatch.setenv("EVAL_AUX_BASE_URL", "https://aux")
        monkeypatch.setenv("EVAL_AUX_API_KEY", "k")
        monkeypatch.delenv("EVAL_READER_MODEL", raising=False)
        route = LLMRoute.from_env("reader")
        assert route.model == "aux-m" and route.base_url == "https://aux"

        monkeypatch.setenv("EVAL_READER_MODEL", "reader-m")
        assert LLMRoute.from_env("reader").model == "reader-m"

        monkeypatch.delenv("EVAL_AUX_MODEL", raising=False)
        monkeypatch.delenv("EVAL_JUDGE_MODEL", raising=False)
        with pytest.raises(RuntimeError, match="EVAL_JUDGE_MODEL"):
            LLMRoute.from_env("judge")


# ---------------------------------------------------------------------------
# Contradiction-survival probe
# ---------------------------------------------------------------------------


class TestContradiction:
    def test_fixture_loads_through_both_loaders(self):
        questions = load_longmemeval(str(PROBE_FIXTURE))
        meta = load_probe_meta(str(PROBE_FIXTURE))
        assert len(questions) == len(meta) == 8
        for q in questions:
            probe = meta[q.question_id]
            haystack = {s.session_id for s in q.sessions}
            assert probe["update_session_id"] in haystack
            assert probe["original_session_id"] in haystack
            assert q.evidence_session_ids == {probe["update_session_id"]}
            # the values actually appear in their sessions' content
            by_id = {s.session_id: s for s in q.sessions}
            original_text = " ".join(
                t.content for t in by_id[probe["original_session_id"]].turns
            )
            update_text = " ".join(
                t.content for t in by_id[probe["update_session_id"]].turns
            )
            assert probe["stale_value"].lower() in original_text.lower()
            assert probe["current_value"].lower() in update_text.lower()

    def test_load_probe_meta_missing_key_raises(self, tmp_path):
        broken = json.loads(PROBE_FIXTURE.read_text())
        del broken[0]["probe"]["stale_value"]
        path = tmp_path / "broken.json"
        path.write_text(json.dumps(broken))
        with pytest.raises(ValueError, match="stale_value"):
            load_probe_meta(str(path))

    def test_classify_answer(self):
        probe = {
            "current_value": "red Tesla Model 3",
            "stale_value": "blue Honda Civic",
        }
        assert classify_answer("You drive a red tesla model 3.", probe) == "current"
        # mentioning both counts as current (knowledge-update convention)
        assert (
            classify_answer(
                "You used to drive a blue Honda Civic, now a red Tesla Model 3.",
                probe,
            )
            == "current"
        )
        assert classify_answer("A blue Honda Civic, I believe.", probe) == "stale"
        assert classify_answer("I do not have that information.", probe) == "miss"

    def test_retrieval_outcome(self):
        probe = {
            "update_session_id": "u",
            "original_session_id": "o",
            "current_value": "x",
            "stale_value": "y",
        }
        row = {"ranked_sessions": ["a", "u", "o"]}
        out = retrieval_outcome(row, probe)
        assert out["update_rank"] == 2 and out["original_rank"] == 3
        assert out["update_above_original"] is True

        out = retrieval_outcome({"ranked_sessions": ["o", "a"]}, probe)
        assert out["update_rank"] is None and out["update_injected"] is False
        assert out["update_above_original"] is False

        # update present, original absent -> above by definition
        out = retrieval_outcome({"ranked_sessions": ["u"]}, probe)
        assert out["update_above_original"] is True

    def test_summarize_probe(self):
        rows = [
            {
                "update_injected": True,
                "original_injected": True,
                "update_above_original": True,
                "reader_class": "current",
            },
            {
                "update_injected": True,
                "original_injected": False,
                "update_above_original": True,
                "reader_class": "stale",
            },
            {
                "update_injected": False,
                "original_injected": True,
                "update_above_original": False,
            },
        ]
        s = summarize_probe(rows)
        assert s["questions"] == 3
        assert s["update_injected"] == round(2 / 3, 4)
        assert s["update_above_original"] == round(2 / 3, 4)
        assert s["reader"]["n"] == 2
        assert s["reader"]["current_rate"] == 0.5
        assert s["reader"]["stale_rate"] == 0.5
        assert s["reader"]["miss_rate"] == 0.0


class TestVerbatimTruncation:
    def test_oversized_round_capped_consistently(self):
        big = "x" * (VERBATIM_MAX_CHARS + 5000)
        session = LMESession(
            session_id="s",
            turns=[
                LMETurn(role="user", content=big),
                LMETurn(role="assistant", content="ok"),
                LMETurn(role="user", content="small"),
                LMETurn(role="assistant", content="fine"),
            ],
        )
        arm = ArmSpec(name="t", ingestion=IngestionOptions(mode="verbatim"))
        texts = _verbatim_round_texts(session, arm)
        # "x"*N compresses to few cl100k tokens, so only the char cap fires
        assert len(texts[0]) == VERBATIM_MAX_CHARS + len(" …[truncated]")
        assert texts[0].endswith("…[truncated]")
        assert texts[1] == "User: small\nAssistant: fine"  # small rounds untouched

    def test_token_dense_round_capped_below_char_limit(self):
        # token-dense content (digit soup ≈ 1 token/char-ish in cl100k)
        # stays under the char cap but must still be token-truncated
        big = " ".join(str(i % 97) for i in range(7500))  # ~22k chars
        assert len(big) < VERBATIM_MAX_CHARS
        session = LMESession(
            session_id="s",
            turns=[
                LMETurn(role="user", content=big),
                LMETurn(role="assistant", content="ok"),
            ],
        )
        arm = ArmSpec(name="t", ingestion=IngestionOptions(mode="verbatim"))
        text = _verbatim_round_texts(session, arm)[0]
        assert text.endswith("…[truncated]")
        from eval.memory.ingest import _encoding

        n = len(_encoding().encode(text, disallowed_special=()))
        assert n <= EMBED_TOKEN_CAP + 8  # cap + marker tokens


class TestExtractionPromptOverride:
    def test_arm_field_parses(self, tmp_path):
        arm_file = tmp_path / "arm.yaml"
        arm_file.write_text(
            "name: t\nextraction_prompt_file: config/prompts/"
            "memory_extraction_prompt_complete.txt\n"
        )
        arm = ArmSpec.from_file(str(arm_file))
        assert arm.extraction_prompt_file == (
            "config/prompts/memory_extraction_prompt_complete.txt"
        )
        assert ArmSpec(name="x").extraction_prompt_file is None

    def test_variant_prompt_committed_and_nonempty(self):
        text = (
            REPO_ROOT / "config" / "prompts" / "memory_extraction_prompt_complete.txt"
        ).read_text(encoding="utf-8")
        # the structural contract the extractor parser relies on
        for marker in (
            "content",
            "summary",
            "keywords",
            "importance",
            "retrieval_messages",
            "factual",
            "procedural",
            "error_solution",
            "vocabulary",
            "relational",
        ):
            assert marker in text, f"variant prompt missing field/type {marker}"
        # the intervention itself
        assert "Completeness over precision" in text
        assert "Events that happened" in text


class TestLifecycleScorer:
    """Bi-temporal supersede scorer (eval/memory/lifecycle.py)."""

    @staticmethod
    def _rows(*pairs):
        """(text, retired) tuples -> the {text, retired} rows classify_case wants.

        Text is lowercased here exactly as _fetch_case_rows does from the DB.
        """
        return [{"text": t.lower(), "retired": r} for t, r in pairs]

    def test_fixture_loads_through_both_loaders(self):
        questions = load_longmemeval(str(LIFECYCLE_FIXTURE))
        meta = load_lifecycle_meta(str(LIFECYCLE_FIXTURE))
        assert len(questions) == len(meta) >= 15
        raw = {
            inst["question_id"]: inst
            for inst in json.loads(LIFECYCLE_FIXTURE.read_text(encoding="utf-8"))
        }
        for q in questions:
            block = meta[q.question_id]
            cat = block["category"]
            blob = " ".join(t.content for s in q.sessions for t in s.turns).lower()
            # every asserted value actually occurs in some turn
            for v in block["expect_valid"] + block["expect_retired"]:
                assert v.lower() in blob, f"{q.question_id}: {v!r} not in any turn"
            # only should-retire categories name retired values; and they ship a
            # probe block so contradiction.py can still score them
            if cat in ("update", "update_chain"):
                assert block["expect_retired"], q.question_id
                assert "probe" in raw[q.question_id], q.question_id
            else:
                assert not block["expect_retired"], q.question_id

    def test_update_pass(self):
        rows = self._rows(
            ("user drives a red Tesla Model 3", False),
            ("user drives a blue Honda Civic", True),
        )
        v = classify_case(
            "update", rows, ["red Tesla Model 3"], ["blue Honda Civic"], []
        )
        assert v["status"] == "pass"
        assert all(x["outcome"] == "ok" for x in v["values"])

    def test_false_retire_is_a_fault(self):
        # a coexisting fact that should have survived was wrongly retired
        rows = self._rows(
            ("owns a Tesla Model 3", False),
            ("owns a Honda motorcycle", True),  # retired -> over-retire bug
        )
        v = classify_case(
            "coexist", rows, ["Tesla Model 3", "Honda motorcycle"], [], []
        )
        assert v["status"] == "fail"
        outcomes = {x["value"]: x["outcome"] for x in v["values"]}
        assert outcomes["Honda motorcycle"] == "false_retired"
        assert outcomes["Tesla Model 3"] == "ok"

    def test_missed_retire_is_a_fault(self):
        # stale value still served by a valid row -> supersede never fired
        rows = self._rows(
            ("default backend is OpenCloud", False),
            ("default backend is Nextcloud", False),
        )
        v = classify_case("update", rows, ["OpenCloud"], ["Nextcloud"], [])
        assert v["status"] == "fail"
        assert v["values"][-1]["outcome"] == "missed_retire"

    def test_chain_retires_every_hop(self):
        rows = self._rows(
            ("orchestrator on port 8085", False),
            ("orchestrator on port 8080", True),
            ("orchestrator on port 8083", True),
        )
        v = classify_case("update_chain", rows, ["8085"], ["8080", "8083"], [])
        assert v["status"] == "pass"

    def test_noop_duplicate_is_a_fault(self):
        rows = self._rows(
            ("works at Northwind Robotics", False),
            ("employed by Northwind Robotics", False),  # twin -> NOOP failed
        )
        v = classify_case(
            "noop", rows, ["Northwind Robotics"], [], ["Northwind Robotics"]
        )
        assert v["status"] == "fail"
        assert v["values"][-1]["outcome"] == "duplicate"

    def test_noop_single_row_passes(self):
        rows = self._rows(("works at Northwind Robotics", False))
        v = classify_case(
            "noop", rows, ["Northwind Robotics"], [], ["Northwind Robotics"]
        )
        assert v["status"] == "pass"

    def test_not_extracted_is_incomplete_not_fault(self):
        rows = self._rows(("something unrelated", False))
        v = classify_case("coexist", rows, ["Spanish", "Japanese"], [], [])
        assert v["status"] == "incomplete"
        assert all(x["outcome"] == "not_extracted" for x in v["values"])

    def test_no_rows(self):
        v = classify_case("update", [], ["X"], ["Y"], [])
        assert v["status"] == "no_rows"

    def test_summarize_rates_and_denominators(self):
        coexist = classify_case(
            "coexist",
            self._rows(("has X here", False), ("has Y here", True)),
            ["X", "Y"],
            [],
            [],
        )  # X ok, Y false_retired
        upd = classify_case(
            "update",
            self._rows(("now C", False), ("old S", True)),
            ["C"],
            ["S"],
            [],
        )  # both ok
        gap = classify_case(
            "coexist", self._rows(("noise", False)), ["Z"], [], []
        )  # Z not_extracted
        s = summarize([coexist, upd, gap])
        assert s["cases"] == 3
        assert s["failed"] == 1 and s["passed"] == 1 and s["incomplete"] == 1
        # X, Y, C are assessable survivors; Z is excluded (not extracted)
        assert s["survive_assessable_n"] == 3
        assert s["false_retired_n"] == 1
        assert s["false_retire_rate"] == round(1 / 3, 4)
        assert s["missed_retire_rate"] == 0.0  # S correctly retired
        assert s["not_extracted_n"] == 1
        assert s["by_category"]["coexist"] == {
            "pass": 0,
            "fail": 1,
            "incomplete": 1,
            "no_rows": 0,
        }
