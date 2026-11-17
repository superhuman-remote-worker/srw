"""Tests for closed-vocabulary tool parameters.

Regression cover for the bug in
``knowledge-base/knowledge/issues/agent_tool_fixed_vocabularies_invisible_to_model.md``: tool
parameters backed by a fixed set of values (Postgres enums, ``NOTE_TYPES``,
``_AUTONOMY_VALUES``) were typed as bare ``str`` and documented only in
docstring prose. Every citation tool is registered ``defer_to_workspace=True``,
so ``apply_description_overrides`` replaces that docstring with a one-line
blurb before the tool is bound — the vocabulary reached the model nowhere, and
an agent burned nine calls guessing ``extraction_method``.

Three things are asserted here, and each one can actually fail:

1. **Drift** — the ``Literal`` sets match the enums/frozensets they mirror.
   The ``kb_write`` docstring listed nine of ten ``NOTE_TYPES`` for long enough
   that nobody noticed ``datasource`` was missing; prose cannot be tested, a
   ``Literal`` can.
2. **Constraint** — the bound ``args_schema`` actually carries ``enum``.
3. **Survival** — the constraint is still there *after* the deferral override
   runs. This is the load-bearing one: it is the only thing that distinguishes
   a real fix from documentation that gets stripped again.

Note ``tests/test_graph.py::TestEditCitationTool`` mocks the engine wholesale,
so it structurally cannot catch any of this.
"""

from typing import get_args
from unittest.mock import AsyncMock, MagicMock

import pytest


# =============================================================================
# 1. DRIFT — the Literal sets must match their single sources of truth
# =============================================================================


class TestVocabularyDrift:
    """Each Literal is spelled out in a tool signature; assert it stays in sync.

    Spelling them literally (rather than deriving from the enum) keeps them
    valid static annotations and keeps the tool modules importable when the
    engine is absent — these tests are the price of that choice.
    """

    def test_citation_literals_match_engine_enums(self):
        from shared.runtime.citation_engine.models import (
            AnnotationType,
            Confidence,
            ExtractionMethod,
            SourceType,
            VerificationStatus,
        )
        from agent.tools.citation.sources import (
            AnnotationTypeValue,
            ConfidenceValue,
            ExtractionMethodValue,
            SourceTypeValue,
            VerificationStatusValue,
        )

        pairs = [
            (ExtractionMethodValue, ExtractionMethod),
            (ConfidenceValue, Confidence),
            (VerificationStatusValue, VerificationStatus),
            (SourceTypeValue, SourceType),
            (AnnotationTypeValue, AnnotationType),
        ]
        for literal, enum in pairs:
            assert set(get_args(literal)) == {m.value for m in enum}, (
                f"{enum.__name__} drifted from its tool-schema Literal"
            )

    def test_kb_literals_match_knowledge_graph_vocabularies(self):
        """The 'datasource' omission this catches was live in the docstring."""
        from shared.runtime.services.knowledge_graph import (
            CONFIDENCE_LEVELS,
            NOTE_STATUSES,
            NOTE_TYPES,
        )
        from agent.tools.knowledge.knowledge_tools import (
            NoteConfidenceValue,
            NoteStatusValue,
            NoteTypeValue,
        )

        assert set(get_args(NoteTypeValue)) == set(NOTE_TYPES)
        assert set(get_args(NoteStatusValue)) == set(NOTE_STATUSES)
        assert set(get_args(NoteConfidenceValue)) == set(CONFIDENCE_LEVELS)

    def test_autonomy_literal_matches_autonomy_values(self):
        """propose_automation.autonomy named its values nowhere the model could
        see — not in the type, not even in prose. Only in _AUTONOMY_VALUES,
        surfaced solely in a post-hoc rejection."""
        from agent.tools.orchestrator.workflows import (
            _AUTONOMY_VALUES,
            create_workflow_tools,
        )

        tools = {t.name: t for t in create_workflow_tools(MagicMock())}
        enum = _enum_of(tools["propose_automation"], "autonomy")
        assert enum is not None
        assert set(enum) == set(_AUTONOMY_VALUES)

    def test_phases_is_a_boolean_on_both_creation_tools(self):
        """create_job (the shared descriptor behind the agent, officer and MCP
        tools) and MCP create_project_job take ``phases`` as an optional
        boolean, the type shared.job_phases reads; no spelled-out vocabulary
        of words can drift from it."""
        import ast
        import inspect
        from pathlib import Path

        from shared.orch_surface.jobs import get_descriptor

        handler = get_descriptor("create_job").handler
        hint = inspect.get_annotations(handler, eval_str=True)["phases"]
        assert set(get_args(hint)) == {bool, type(None)}

        # The MCP module is read, not imported: FastMCP wraps the function.
        server = Path(__file__).resolve().parents[1] / "src/mcp_server/server.py"
        (function,) = [
            node
            for node in ast.walk(ast.parse(server.read_text(encoding="utf-8")))
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name == "create_project_job"
        ]
        (argument,) = [a for a in function.args.args if a.arg == "phases"]
        assert ast.unparse(argument.annotation) == "bool | None"

    def test_backlog_ticket_types_are_in_every_vocabulary(self):
        """A ticket type missing from any one site silently degrades: the tool
        rejects it, or kb_reindex rewrites it to 'learning' on the next pass."""
        from orchestrator.services.kb_reindex import VALID_NOTE_TYPES
        from shared.runtime.services.knowledge_graph import NOTE_TYPES
        from agent.tools.knowledge.knowledge_tools import NoteTypeValue

        for ticket_type in ("feature", "issue", "idea"):
            assert ticket_type in NOTE_TYPES
            assert ticket_type in set(get_args(NoteTypeValue))
            assert ticket_type in VALID_NOTE_TYPES

    def test_priority_literal_matches_priority_ranks(self):
        """A set-of-keys comparison alone would still pass if the rank
        numbers were scrambled (e.g. high=2, low=0), silently reversing the
        backlog's sort order. Pin the actual word -> rank mapping."""
        from shared.runtime.services.knowledge_graph import PRIORITY_RANKS
        from agent.tools.knowledge.knowledge_tools import PriorityValue

        assert set(get_args(PriorityValue)) == set(PRIORITY_RANKS)
        assert PRIORITY_RANKS == {"high": 0, "normal": 1, "low": 2}

    def test_backlog_vocab_copies_match_their_canonical_sources(self):
        """M1: the orchestrator cannot import src/ at runtime (no agent deps
        in that image), so orchestrator/services/project_backlog.py and
        kb_reindex.py each hand-duplicate a copy of the priority maps /
        ticket-type tuple canonical in knowledge_graph.py / knowledge_tools.py
        -- that duplication is deliberate and the copies must STAY copies,
        but a test (unlike the orchestrator at runtime) CAN import both
        sides and pin them in sync. They are all in sync today; this only
        fails if a future edit touches one side and not its mirror."""
        from orchestrator.services.kb_reindex import PRIORITY_RANKS as reindex_ranks
        from orchestrator.services.project_backlog import BACKLOG_NOTE_TYPES
        from orchestrator.services.project_backlog import (
            PRIORITY_WORDS as backlog_words,
        )
        from shared.runtime.services.knowledge_graph import (
            PRIORITY_RANKS as canonical_ranks,
            PRIORITY_WORDS as canonical_words,
        )
        from agent.tools.knowledge.knowledge_tools import _TICKET_TYPES

        assert backlog_words == canonical_words
        assert reindex_ranks == canonical_ranks
        assert set(BACKLOG_NOTE_TYPES) == set(_TICKET_TYPES)


# =============================================================================
# 2 + 3. CONSTRAINT and SURVIVAL — the schema carries enum, and keeps it
# =============================================================================


def _enum_of(tool, param):
    """Return the JSON-schema enum for ``param``, or None if unconstrained.

    Handles both the bare form (``{"enum": [...]}``) and the Optional form,
    which pydantic renders as ``{"anyOf": [{"enum": [...]}, {"type": "null"}]}``.
    """
    prop = tool.args_schema.model_json_schema()["properties"].get(param)
    assert prop is not None, f"{tool.name}.{param} is absent from the schema"
    if "enum" in prop:
        return prop["enum"]
    return next((b.get("enum") for b in prop.get("anyOf", []) if b.get("enum")), None)


@pytest.fixture
def citation_tools():
    from agent.tools.citation.sources import create_source_tools

    return {t.name: t for t in create_source_tools(MagicMock())}


class TestSchemaConstraints:
    """The vocabulary must be in the args_schema, not only in prose."""

    @pytest.mark.parametrize(
        "tool_name,param,expected",
        [
            (
                "cite_web",
                "extraction_method",
                {"direct_quote", "paraphrase", "inference", "aggregation", "negative"},
            ),
            ("cite_web", "confidence", {"high", "medium", "low"}),
            (
                "cite_document",
                "extraction_method",
                {"direct_quote", "paraphrase", "inference", "aggregation", "negative"},
            ),
            ("cite_document", "confidence", {"high", "medium", "low"}),
            (
                "edit_citation",
                "extraction_method",
                {"direct_quote", "paraphrase", "inference", "aggregation", "negative"},
            ),
            ("edit_citation", "confidence", {"high", "medium", "low"}),
            # 'unverified' was missing from the docstring while being a valid
            # enum member — an agent could not learn it existed.
            (
                "list_citations",
                "status",
                {"pending", "verified", "failed", "unverified"},
            ),
            ("tag_source", "action", {"add", "remove"}),
            (
                "annotate_source",
                "type",
                {"note", "highlight", "summary", "question", "critique"},
            ),
            ("search_library", "mode", {"hybrid", "keyword", "semantic"}),
            ("search_library", "scope", {"content", "annotations", "all"}),
            (
                "search_library",
                "source_type",
                {"document", "website", "database", "custom"},
            ),
            (
                "generate_bibliography",
                "style",
                {"bibtex", "harvard", "ieee", "apa", "inline"},
            ),
        ],
    )
    def test_citation_tool_params_are_constrained(
        self, citation_tools, tool_name, param, expected
    ):
        enum = _enum_of(citation_tools[tool_name], param)
        assert enum is not None, (
            f"{tool_name}.{param} accepts free text — the model cannot see the "
            f"vocabulary (its docstring is stripped by defer_to_workspace)"
        )
        assert set(enum) == expected


class TestOtherToolConstraints:
    """The same defect class outside the citation tools."""

    def test_kb_tool_params_are_constrained(self):
        """kb_write.type is the highest-risk instance: a param literally named
        'type', ten values, and a docstring that listed nine of them."""
        import inspect

        from agent.tools.knowledge.knowledge_tools import create_kb_tools

        sig = inspect.signature(create_kb_tools)
        tools = {
            t.name: t for t in create_kb_tools(*[MagicMock()] * len(sig.parameters))
        }
        assert "datasource" in _enum_of(tools["kb_write"], "type")
        assert set(_enum_of(tools["kb_write"], "confidence")) == {
            "high",
            "medium",
            "low",
        }
        assert set(_enum_of(tools["kb_update"], "status")) == {
            "active",
            "resolved",
            "superseded",
            "archived",
        }
        assert "datasource" in _enum_of(tools["kb_list"], "type")

    def test_create_job_phases_is_a_boolean(self):
        """The agent and officer create_job carries ``phases`` as a boolean in
        its schema, not as free text described only in its docstring."""
        from agent.tools.context import ToolContext
        from agent.tools.orchestrator.jobs import create_orchestrator_tools

        tools = {t.name: t for t in create_orchestrator_tools(ToolContext())}
        prop = tools["create_job"].args_schema.model_json_schema()["properties"][
            "phases"
        ]
        assert {branch.get("type") for branch in prop["anyOf"]} == {
            "boolean",
            "null",
        }
        assert prop.get("default") is None

    def test_task_add_priority_is_constrained(self):
        """Silently downgraded anything unrecognised to 'medium', with no word
        of it in the confirmation."""
        from agent.tools.core.session_task_tools import create_session_task_tools

        tools = {t.name: t for t in create_session_task_tools(MagicMock())}
        assert set(_enum_of(tools["task_add"], "priority")) == {
            "high",
            "medium",
            "low",
        }

    def test_edit_file_position_is_constrained(self):
        from agent.tools.workspace.files import create_file_tools

        ctx = MagicMock()
        ctx.get_config.side_effect = lambda k, *a: {
            "max_read_words": 50000,
            "model_max_context_tokens": 128000,
        }.get(k, (a[0] if a else None))
        tools = {t.name: t for t in create_file_tools(ctx)}
        assert set(_enum_of(tools["edit_file"], "position")) == {"start", "end"}


class TestTagSourceAction:
    """tag_source fell through to 'add' for any value that wasn't exactly
    'remove' — so action='Remove' silently added the tags it was asked to
    remove and reported "Added". Worse than a loud enum error: wrong data,
    confident output, no error anywhere.
    """

    @pytest.fixture
    def tag_tool(self):
        from agent.tools.citation.sources import create_source_tools

        ctx = MagicMock()
        engine = MagicMock()
        engine.tag_source = AsyncMock(return_value=["a"])
        engine.remove_tags = AsyncMock(return_value=[])
        ctx.get_citation_engine.return_value = engine
        tools = {t.name: t for t in create_source_tools(ctx)}
        return tools["tag_source"], engine

    @pytest.mark.asyncio
    async def test_remove_removes(self, tag_tool):
        tool, engine = tag_tool
        result = await tool.ainvoke({"source_id": 1, "tags": "x", "action": "remove"})
        engine.remove_tags.assert_awaited_once()
        engine.tag_source.assert_not_awaited()
        assert "Removed" in result

    @pytest.mark.asyncio
    async def test_schema_rejects_near_miss_before_it_runs(self, tag_tool):
        """'Remove' is now unrepresentable: pydantic rejects it at the tool
        boundary, so the body never runs and nothing is mutated. This is the
        layer that protects the model."""
        tool, engine = tag_tool
        with pytest.raises(Exception, match="add|remove"):
            await tool.ainvoke({"source_id": 1, "tags": "x", "action": "Remove"})
        engine.tag_source.assert_not_awaited()
        engine.remove_tags.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_body_still_refuses_unknown_action(self, tag_tool):
        """Defense in depth for callers that bypass schema validation: the body
        must error rather than fall through to 'add'."""
        tool, engine = tag_tool
        result = await tool.coroutine(source_id=1, tags="x", action="Remove")
        engine.tag_source.assert_not_awaited()
        assert "error" in result.lower()


class TestDeferralSurvival:
    """The whole fix rests on this: the override must not strip the enum.

    ``apply_description_overrides`` swaps the docstring for a one-line
    ``short_description`` on every ``defer_to_workspace`` tool. It does that via
    ``model_copy(update={"description": ...})``, which leaves ``args_schema``
    alone — so a Literal survives where prose does not. If that ever changes,
    this test fails and the bug is back.
    """

    def test_edit_citation_is_actually_deferred(self):
        """Guards the premise: if it stops being deferred, the below is moot."""
        from agent.tools.registry import TOOL_REGISTRY

        meta = TOOL_REGISTRY["edit_citation"]
        assert meta.get("defer_to_workspace") is True
        assert meta.get("short_description")

    def test_enum_survives_description_override(self, citation_tools):
        from agent.tools.description_manager import apply_description_overrides

        before = citation_tools["edit_citation"]
        full_description = before.description

        after = {
            t.name: t
            for t in apply_description_overrides(list(citation_tools.values()))
        }["edit_citation"]

        # The override must have actually run, or this proves nothing.
        assert after.description != full_description
        assert "extraction_method" not in after.description

        # ...and yet the vocabulary is still enforced, because it rides the
        # schema rather than the prose.
        assert set(_enum_of(after, "extraction_method")) == {
            "direct_quote",
            "paraphrase",
            "inference",
            "aggregation",
            "negative",
        }
        assert set(_enum_of(after, "confidence")) == {"high", "medium", "low"}


# =============================================================================
# Engine-side validation — fails fast, before Postgres
# =============================================================================


class TestEditCitationValidation:
    """edit_citation must reject bad enums itself, like create_citation does.

    Previously the value flowed into the ``$N::extraction_method`` cast and
    Postgres raised ``InvalidTextRepresentationError`` — not a ``ValueError``,
    so it bypassed the tool's curated handler and surfaced as a raw driver
    string naming no valid values. That is what left the agent guessing.
    """

    @pytest.fixture
    def engine(self):
        from agent.citation_engine.engine import CitationEngine

        return CitationEngine(db=AsyncMock())

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "bad_value",
        [
            "Tavily extract from raw GitHub document",  # the reported value
            "web_extract",
            "direct",  # guessed six times in one parallel batch
        ],
    )
    async def test_rejects_bad_extraction_method_without_touching_db(
        self, engine, bad_value
    ):
        with pytest.raises(ValueError) as exc:
            await engine.edit_citation(citation_id=1, extraction_method=bad_value)

        # The message must name the valid values — it is the agent's only
        # recovery hint, and its absence is what caused the guessing.
        for member in ("direct_quote", "paraphrase", "inference", "aggregation"):
            assert member in str(exc.value)

        # Fail fast: no DB roundtrip at all.
        engine.db.fetchrow.assert_not_called()
        engine.db.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_rejects_bad_confidence_without_touching_db(self, engine):
        with pytest.raises(ValueError, match="high"):
            await engine.edit_citation(citation_id=1, confidence="very sure")
        engine.db.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_valid_values_pass_validation(self, engine):
        """A good value must reach the ownership guard, not be rejected early."""
        engine.db.fetchrow = AsyncMock(return_value=None)  # -> "not found"
        with pytest.raises(ValueError, match="not found"):
            await engine.edit_citation(
                citation_id=1, extraction_method="aggregation", confidence="medium"
            )
        engine.db.fetchrow.assert_called_once()


# =============================================================================
# Helpers behind the fix
# =============================================================================


class TestErrorHumanizer:
    """Last-resort net: name the values even if a raw DB enum error escapes."""

    def test_translates_enum_error_into_actionable_message(self):
        from agent.tools.citation.sources import _humanize_db_enum_error

        msg = _humanize_db_enum_error(
            Exception('invalid input value for enum extraction_method: "direct"')
        )
        assert msg is not None
        assert "direct_quote" in msg and "paraphrase" in msg

    def test_returns_none_for_unrelated_errors(self):
        from agent.tools.citation.sources import _humanize_db_enum_error

        assert _humanize_db_enum_error(Exception("connection reset")) is None

    def test_returns_none_for_unknown_enum(self):
        from agent.tools.citation.sources import _humanize_db_enum_error

        assert (
            _humanize_db_enum_error(
                Exception('invalid input value for enum some_other: "x"')
            )
            is None
        )


class TestVerbatimGating:
    """P3's lever: only a claimed direct quote is filed as a verbatim quote.

    The verifier's word-for-word check is triggered by the presence of
    verbatim_quote. The create tools used to hardcode
    extraction_method='direct_quote' and populate verbatim_quote regardless of
    what the text actually was, so a paraphrase was checked as a quote and
    failed — a failure the agent had no way to avoid or repair.
    """

    def test_direct_quote_is_filed_as_verbatim(self):
        from agent.tools.citation.sources import _verbatim_or_none

        assert _verbatim_or_none("exact text", "direct_quote") == "exact text"

    @pytest.mark.parametrize(
        "method", ["paraphrase", "inference", "aggregation", "negative"]
    )
    def test_non_quote_methods_file_no_verbatim(self, method):
        from agent.tools.citation.sources import _verbatim_or_none

        assert _verbatim_or_none("a rewording of the source", method) is None

    def test_overlong_quote_is_still_treated_as_context(self):
        """Pre-existing behaviour, preserved: a huge 'quote' is not a quote."""
        from agent.tools.citation.sources import _verbatim_or_none

        assert _verbatim_or_none("x" * 1500, "direct_quote") is None
