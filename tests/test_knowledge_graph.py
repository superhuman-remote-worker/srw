"""Tests for src/services/knowledge_graph.py.

Covers section 10 of persistent_agent_tests.md:
  10.1  Constants
  10.2  slugify()
  10.3  KnowledgeGraphDB.__init__()
  10.4  connect() / close() / is_connected
  10.5  create_note()
  10.6  read_note()
  10.7  list_notes()
  10.8  update_note()
  10.9  get_related()
  10.10 get_contradictions()
  10.11 get_provenance()
  10.12 get_unanswered()
  10.13 get_all_notes_for_export()
  10.14 delete_project_knowledge()
  10.15 get_note_content_hash()
"""

import hashlib
from unittest.mock import MagicMock, patch

import pytest

from shared.runtime.services.knowledge_graph import (
    KnowledgeGraphDB,
    slugify,
    NOTE_TYPES,
    NOTE_STATUSES,
    CONFIDENCE_LEVELS,
    RELATIONSHIP_TYPES,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_kg():
    """Create a KnowledgeGraphDB with mocked Neo4jDB."""
    with patch("shared.runtime.services.knowledge_graph.Neo4jDB") as MockDB:
        mock_db = MagicMock()
        MockDB.return_value = mock_db
        kg = KnowledgeGraphDB(uri="bolt://test:7687", username="u", password="p")
    return kg, mock_db


# =============================================================================
# 10.1: Constants
# =============================================================================


class TestConstants:
    """Tests for module-level constant sets."""

    def test_note_types_count(self):
        assert len(NOTE_TYPES) == 15

    def test_note_types_members(self):
        expected = {
            "goal",
            "plan",
            "decision",
            "learning",
            "code",
            "source",
            "question",
            "state",
            "retrospective",
            "datasource",
            "feature",
            "issue",
            "idea",
            # Officer types (centurion.md §5, vector/0015)
            "charter",
            "report",
        }
        assert NOTE_TYPES == expected

    def test_note_statuses_count(self):
        assert len(NOTE_STATUSES) == 4

    def test_note_statuses_members(self):
        assert NOTE_STATUSES == {"active", "resolved", "superseded", "archived"}

    def test_confidence_levels_count(self):
        assert len(CONFIDENCE_LEVELS) == 3

    def test_confidence_levels_members(self):
        assert CONFIDENCE_LEVELS == {"high", "medium", "low"}

    def test_relationship_types_count(self):
        assert len(RELATIONSHIP_TYPES) == 8

    def test_relationship_types_members(self):
        expected = {
            "REFERENCES",
            "DERIVED_FROM",
            "SUPPORTS",
            "CONTRADICTS",
            "ANSWERS",
            "DEPENDS_ON",
            "SUPERSEDES",
            "IMPLEMENTS",
        }
        assert RELATIONSHIP_TYPES == expected

    def test_constants_are_frozensets(self):
        assert isinstance(NOTE_TYPES, frozenset)
        assert isinstance(NOTE_STATUSES, frozenset)
        assert isinstance(CONFIDENCE_LEVELS, frozenset)
        assert isinstance(RELATIONSHIP_TYPES, frozenset)


# =============================================================================
# 10.2: slugify()
# =============================================================================


class TestSlugify:
    """Tests for the slugify() function."""

    def test_lowercases_input(self):
        assert slugify("Chose JWT") == "chose-jwt"

    def test_replaces_spaces_with_hyphens(self):
        assert slugify("hello world test") == "hello-world-test"

    def test_strips_underscores(self):
        # Underscores are stripped by the non-alphanumeric regex before hyphen conversion
        assert slugify("hello_world") == "helloworld"

    def test_strips_non_alphanumeric(self):
        assert slugify("auth (v2)!") == "auth-v2"

    def test_collapses_multiple_hyphens(self):
        assert slugify("a---b") == "a-b"

    def test_strips_leading_trailing_hyphens(self):
        assert slugify("-test-") == "test"

    def test_truncates_to_max_length(self):
        long_title = "a" * 100
        result = slugify(long_title, max_length=80)
        assert len(result) == 80

    def test_empty_title_after_stripping(self):
        assert slugify("!!!") == ""

    def test_empty_string(self):
        assert slugify("") == ""


# =============================================================================
# 10.3: KnowledgeGraphDB.__init__()
# =============================================================================


class TestInit:
    """Tests for constructor / environment variable reading."""

    @patch.dict("os.environ", {}, clear=True)
    def test_defaults_when_no_env(self):
        kg, _ = _make_kg()
        # Constructor params override env, but let's test env defaults
        with patch("shared.runtime.services.knowledge_graph.Neo4jDB") as MockDB:
            KnowledgeGraphDB()
            MockDB.assert_called_once_with("bolt://localhost:7687", "neo4j", "")

    @patch.dict(
        "os.environ",
        {
            "NEO4J_URL": "bolt://custom:7687",
            "NEO4J_USERNAME": "admin",
            "NEO4J_PASSWORD": "secret",
        },
    )
    def test_reads_env_vars(self):
        with patch("shared.runtime.services.knowledge_graph.Neo4jDB") as MockDB:
            KnowledgeGraphDB()
            MockDB.assert_called_once_with("bolt://custom:7687", "admin", "secret")

    def test_constructor_params_override_env(self):
        with patch("shared.runtime.services.knowledge_graph.Neo4jDB") as MockDB:
            with patch.dict("os.environ", {"NEO4J_URL": "bolt://env:7687"}):
                KnowledgeGraphDB(uri="bolt://param:7687")
                MockDB.assert_called_once()
                assert MockDB.call_args[0][0] == "bolt://param:7687"


# =============================================================================
# 10.4: connect() / close() / is_connected
# =============================================================================


class TestConnectionMethods:
    """Tests for connect/close/is_connected delegation."""

    def test_connect_delegates(self):
        kg, mock_db = _make_kg()
        mock_db.connect.return_value = True
        assert kg.connect() is True
        mock_db.connect.assert_called_once()

    def test_connect_returns_false(self):
        kg, mock_db = _make_kg()
        mock_db.connect.return_value = False
        assert kg.connect() is False

    def test_close_delegates(self):
        kg, mock_db = _make_kg()
        kg.close()
        mock_db.close.assert_called_once()

    def test_is_connected_delegates(self):
        kg, mock_db = _make_kg()
        type(mock_db).is_connected = property(lambda self: True)
        assert kg.is_connected is True


# =============================================================================
# 10.5: create_note()
# =============================================================================


class TestCreateNote:
    """Tests for create_note() method."""

    def test_invalid_note_type_raises(self):
        kg, _ = _make_kg()
        with pytest.raises(ValueError, match="Invalid note_type"):
            kg.create_note("proj-1", "Title", "invalid_type", "content")

    def test_invalid_confidence_raises(self):
        kg, _ = _make_kg()
        with pytest.raises(ValueError, match="Invalid confidence"):
            kg.create_note("proj-1", "Title", "decision", "content", confidence="ultra")

    def test_none_confidence_is_valid(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []  # no slug collision
        result = kg.create_note(
            "proj-1", "Title", "decision", "content", confidence=None
        )
        assert isinstance(result, str)

    def test_generates_slug_from_title(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        result = kg.create_note("proj-1", "Chose JWT", "decision", "content")
        assert result == "chose-jwt"

    def test_fallback_slug_for_empty_title(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        result = kg.create_note("proj-1", "!!!", "decision", "content")
        assert result.startswith("note-")
        assert len(result) > 5

    def test_appends_suffix_on_collision(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = [{"n.id": "chose-jwt"}]  # collision
        result = kg.create_note("proj-1", "Chose JWT", "decision", "content")
        assert result.startswith("chose-jwt-")
        assert len(result) > len("chose-jwt-")

    def test_collision_suffix_is_deterministic(self):
        # Same title + same content must converge to the SAME suffixed slug on
        # repeat, instead of forking with a random suffix each time.
        kg, mock_db = _make_kg()
        mock_db.execute_query.side_effect = [
            [{"n.id": "chose-jwt"}],  # base slug collides
            [],  # suffixed slug is free
            [],  # no note was waiting to link to it
        ]
        r1 = kg.create_note("proj-1", "Chose JWT", "decision", "same-body")

        kg2, mock_db2 = _make_kg()
        mock_db2.execute_query.side_effect = [
            [{"n.id": "chose-jwt"}],
            [],
            [],
        ]
        r2 = kg2.create_note("proj-1", "Chose JWT", "decision", "same-body")

        assert r1.startswith("chose-jwt-")
        assert r1 == r2  # deterministic across runs

    def test_collision_different_content_diverges(self):
        # Same title, different content → different suffix (both notes kept).
        kg, mock_db = _make_kg()
        mock_db.execute_query.side_effect = [[{"n.id": "t"}], [], []]
        r1 = kg.create_note("proj-1", "T", "decision", "body-A")
        kg2, mock_db2 = _make_kg()
        mock_db2.execute_query.side_effect = [[{"n.id": "t"}], [], []]
        r2 = kg2.create_note("proj-1", "T", "decision", "body-B")
        assert r1 != r2

    def test_collision_idempotent_when_suffixed_slug_exists(self):
        # If the deterministic suffixed slug already exists (same title+content
        # written before), return it — do NOT create a duplicate node.
        kg, mock_db = _make_kg()
        mock_db.execute_query.side_effect = [
            [{"n.id": "chose-jwt"}],  # base slug collides
            [{"n.id": "chose-jwt-deadbeef"}],  # suffixed slug already present
        ]
        result = kg.create_note("proj-1", "Chose JWT", "decision", "same-body")
        assert result.startswith("chose-jwt-")
        mock_db.execute_write.assert_not_called()  # idempotent — no new node

    def test_creates_note_node(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.create_note("proj-1", "Title", "decision", "content")
        mock_db.execute_write.assert_called()
        # First execute_write creates the Note node
        first_call = mock_db.execute_write.call_args_list[0]
        assert "CREATE (n:Note" in first_call[0][0]

    def test_creates_tags(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.create_note(
            "proj-1", "Title", "decision", "content", tags=["Auth", "Security"]
        )
        # Note creation + 2 tag creations = 3 execute_write calls
        tag_calls = [
            c for c in mock_db.execute_write.call_args_list if "TAGGED" in c[0][0]
        ]
        assert len(tag_calls) == 2
        # Tags should be lowercased
        assert tag_calls[0][0][1]["tag"] == "auth"
        assert tag_calls[1][0][1]["tag"] == "security"

    def test_creates_keywords(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.create_note(
            "proj-1", "Title", "decision", "content", keywords=["JWT", "OAuth"]
        )
        kw_calls = [
            c for c in mock_db.execute_write.call_args_list if "HAS_KEYWORD" in c[0][0]
        ]
        assert len(kw_calls) == 2
        assert kw_calls[0][0][1]["kw"] == "jwt"

    def test_creates_relationships(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.create_note(
            "proj-1",
            "Title",
            "decision",
            "content",
            links=[{"target": "other-note", "type": "SUPPORTS"}],
        )
        rel_calls = [
            c for c in mock_db.execute_write.call_args_list if "SUPPORTS" in c[0][0]
        ]
        assert len(rel_calls) == 1

    def test_skips_links_with_missing_target(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.create_note(
            "proj-1", "Title", "decision", "content", links=[{"type": "SUPPORTS"}]
        )  # no target
        # Only the Note creation call, no relationship calls
        rel_calls = [
            c for c in mock_db.execute_write.call_args_list if "SUPPORTS" in c[0][0]
        ]
        assert len(rel_calls) == 0

    def test_skips_links_with_invalid_rel_type(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.create_note(
            "proj-1",
            "Title",
            "decision",
            "content",
            links=[{"target": "other", "type": "INVALID"}],
        )
        rel_calls = [
            c for c in mock_db.execute_write.call_args_list if "INVALID" in c[0][0]
        ]
        assert len(rel_calls) == 0

    def test_defaults_link_type_to_references(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.create_note(
            "proj-1", "Title", "decision", "content", links=[{"target": "other-note"}]
        )  # no type
        rel_calls = [
            c for c in mock_db.execute_write.call_args_list if "REFERENCES" in c[0][0]
        ]
        assert len(rel_calls) == 1

    def test_returns_slug(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        result = kg.create_note("proj-1", "My Note", "learning", "content")
        assert result == "my-note"

    def test_a_link_records_its_target_while_it_does_not_exist(self):
        # One write per link, not a MATCH that skips a missing target: the
        # write makes the edge or keeps the link pending on the source.
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.create_note(
            "proj-1",
            "Title",
            "decision",
            "content",
            links=[{"target": "later", "type": "REFERENCES"}],
        )
        (call,) = [
            c for c in mock_db.execute_write.call_args_list if "REFERENCES" in c[0][0]
        ]
        query, params = call[0]
        assert "OPTIONAL MATCH (b:Note" in query
        assert "pending_links" in query
        assert params["entry"] == "REFERENCES|later"

    def test_creating_a_note_links_the_notes_that_waited_for_it(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.side_effect = [
            [],  # slug is free
            [{"src": "early", "entries": ["SUPPORTS|chose-jwt"]}],
        ]
        kg.create_note("proj-1", "Chose JWT", "decision", "content")
        lookup = mock_db.execute_query.call_args_list[1][0]
        assert lookup[1] == {"pid": "proj-1", "suffix": "|chose-jwt"}
        (call,) = [
            c for c in mock_db.execute_write.call_args_list if "SUPPORTS" in c[0][0]
        ]
        assert call[0][1]["src"] == "early"
        assert call[0][1]["tgt"] == "chose-jwt"

    def test_a_target_that_only_ends_in_the_new_id_is_not_linked(self):
        # "x|chose-jwt" ends with "|chose-jwt" but names another note.
        kg, mock_db = _make_kg()
        mock_db.execute_query.side_effect = [
            [],
            [{"src": "early", "entries": ["SUPPORTS|x|chose-jwt", "BOGUS|chose-jwt"]}],
        ]
        kg.create_note("proj-1", "Chose JWT", "decision", "content")
        assert not [
            c
            for c in mock_db.execute_write.call_args_list
            if "SUPPORTS" in c[0][0] or "BOGUS" in c[0][0]
        ]


# =============================================================================
# 10.6: read_note()
# =============================================================================


class TestReadNote:
    """Tests for read_note() method."""

    def test_returns_none_when_not_found(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        assert kg.read_note("proj-1", "nonexistent") is None

    def test_returns_dict_with_properties(self):
        kg, mock_db = _make_kg()
        mock_node = MagicMock()
        mock_node.items.return_value = [("id", "note-1"), ("title", "Test")]
        mock_db.execute_query.side_effect = [
            [{"n": mock_node, "tags": ["auth"], "keywords": ["jwt"]}],  # main query
            [
                {"rel_type": "SUPPORTS", "target_id": "other", "target_title": "Other"}
            ],  # outgoing
            [
                {
                    "rel_type": "DERIVED_FROM",
                    "source_id": "parent",
                    "source_title": "Parent",
                }
            ],  # incoming
        ]
        result = kg.read_note("proj-1", "note-1")
        assert result is not None
        assert result["id"] == "note-1"
        assert result["tags"] == ["auth"]
        assert result["keywords"] == ["jwt"]

    def test_handles_dict_node(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.side_effect = [
            [{"n": {"id": "note-1", "title": "Test"}, "tags": [], "keywords": []}],
            [],  # outgoing
            [],  # incoming
        ]
        result = kg.read_note("proj-1", "note-1")
        assert result["id"] == "note-1"

    def test_pending_links_come_back_as_relationships_without_an_edge(self):
        kg, mock_db = _make_kg()
        node = {"id": "note-1", "pending_links": ["REFERENCES|later", "bad"]}
        mock_db.execute_query.side_effect = [
            [{"n": node, "tags": [], "keywords": []}],
            [],
            [],
        ]
        result = kg.read_note("proj-1", "note-1")
        assert result["pending_relationships"] == [
            {"type": "REFERENCES", "target": "later"}
        ]
        assert "pending_links" not in result

    def test_empty_props_for_unknown_node_type(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.side_effect = [
            [{"n": 42, "tags": [], "keywords": []}],  # node is int
            [],
            [],
        ]
        result = kg.read_note("proj-1", "note-1")
        assert result is not None
        assert "tags" in result

    def test_relationships_structure(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.side_effect = [
            [{"n": {"id": "n1"}, "tags": [], "keywords": []}],
            [{"rel_type": "SUPPORTS", "target_id": "n2", "target_title": "Note 2"}],
            [],
        ]
        result = kg.read_note("proj-1", "n1")
        assert len(result["relationships"]) == 1
        rel = result["relationships"][0]
        assert rel["type"] == "SUPPORTS"
        assert rel["target"] == "n2"
        assert rel["target_title"] == "Note 2"

    def test_incoming_relationships_structure(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.side_effect = [
            [{"n": {"id": "n1"}, "tags": [], "keywords": []}],
            [],
            [{"rel_type": "DERIVED_FROM", "source_id": "n0", "source_title": "Root"}],
        ]
        result = kg.read_note("proj-1", "n1")
        assert len(result["incoming_relationships"]) == 1
        inc = result["incoming_relationships"][0]
        assert inc["type"] == "DERIVED_FROM"
        assert inc["source"] == "n0"


# =============================================================================
# 10.7: list_notes()
# =============================================================================


class TestListNotes:
    """Tests for list_notes() method."""

    def test_returns_all_notes_for_project(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = [
            {"id": "n1", "title": "A"},
            {"id": "n2", "title": "B"},
        ]
        result = kg.list_notes("proj-1")
        assert len(result) == 2

    def test_filters_by_note_type(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.list_notes("proj-1", note_type="decision")
        query = mock_db.execute_query.call_args[0][0]
        assert "n.type = $type" in query

    def test_filters_by_status(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.list_notes("proj-1", status="active")
        query = mock_db.execute_query.call_args[0][0]
        assert "n.status = $status" in query

    def test_filters_by_job_id(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.list_notes("proj-1", job_id="job-1")
        query = mock_db.execute_query.call_args[0][0]
        assert "n.job_id = $job_id" in query

    def test_filters_by_tag(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.list_notes("proj-1", tag="Auth")
        query = mock_db.execute_query.call_args[0][0]
        assert "TAGGED" in query
        params = mock_db.execute_query.call_args[0][1]
        assert params["tag"] == "auth"  # lowercased

    def test_multiple_filters_combined(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.list_notes("proj-1", note_type="decision", status="active")
        query = mock_db.execute_query.call_args[0][0]
        assert "n.type = $type" in query
        assert "n.status = $status" in query

    def test_ordered_by_modified_desc(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.list_notes("proj-1")
        query = mock_db.execute_query.call_args[0][0]
        assert "ORDER BY n.modified DESC" in query

    def test_capped_at_limit(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.list_notes("proj-1", limit=10)
        params = mock_db.execute_query.call_args[0][1]
        assert params["limit"] == 10

    def test_default_limit_is_50(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.list_notes("proj-1")
        params = mock_db.execute_query.call_args[0][1]
        assert params["limit"] == 50


# =============================================================================
# 10.8: update_note()
# =============================================================================


class TestUpdateNote:
    """Tests for update_note() method."""

    def test_returns_false_when_not_found(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        assert kg.update_note("proj-1", "nonexistent") is False

    def test_always_updates_modified(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = [{"n.id": "n1"}]
        kg.update_note("proj-1", "n1")
        write_query = mock_db.execute_write.call_args[0][0]
        assert "n.modified = datetime($now)" in write_query

    def test_content_replaces_entirely(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = [{"n.id": "n1"}]
        kg.update_note("proj-1", "n1", content="new content")
        write_query = mock_db.execute_write.call_args_list[0][0][0]
        assert "n.content = $content" in write_query

    def test_append_concatenates(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = [{"n.id": "n1"}]
        kg.update_note("proj-1", "n1", append="extra")
        write_query = mock_db.execute_write.call_args_list[0][0][0]
        assert "n.content = n.content + $append" in write_query
        params = mock_db.execute_write.call_args_list[0][0][1]
        assert params["append"] == "\n\nextra"

    def test_content_takes_priority_over_append(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = [{"n.id": "n1"}]
        kg.update_note("proj-1", "n1", content="new", append="extra")
        write_query = mock_db.execute_write.call_args_list[0][0][0]
        assert "n.content = $content" in write_query
        assert "n.content + $append" not in write_query

    def test_invalid_status_raises(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = [{"n.id": "n1"}]
        with pytest.raises(ValueError, match="Invalid status"):
            kg.update_note("proj-1", "n1", status="invalid")

    def test_invalid_confidence_raises(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = [{"n.id": "n1"}]
        with pytest.raises(ValueError, match="Invalid confidence"):
            kg.update_note("proj-1", "n1", confidence="ultra")

    def test_creates_new_tags(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = [{"n.id": "n1"}]
        kg.update_note("proj-1", "n1", add_tags=["NewTag"])
        tag_calls = [
            c for c in mock_db.execute_write.call_args_list if "TAGGED" in c[0][0]
        ]
        assert len(tag_calls) == 1
        assert tag_calls[0][0][1]["tag"] == "newtag"

    def test_creates_new_relationships(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = [{"n.id": "n1"}]
        kg.update_note("proj-1", "n1", add_links=[{"target": "n2", "type": "SUPPORTS"}])
        rel_calls = [
            c for c in mock_db.execute_write.call_args_list if "SUPPORTS" in c[0][0]
        ]
        assert len(rel_calls) == 1

    def test_skips_links_missing_target(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = [{"n.id": "n1"}]
        kg.update_note("proj-1", "n1", add_links=[{"type": "SUPPORTS"}])
        rel_calls = [
            c for c in mock_db.execute_write.call_args_list if "SUPPORTS" in c[0][0]
        ]
        assert len(rel_calls) == 0

    def test_skips_links_invalid_rel_type(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = [{"n.id": "n1"}]
        kg.update_note("proj-1", "n1", add_links=[{"target": "n2", "type": "INVALID"}])
        rel_calls = [
            c for c in mock_db.execute_write.call_args_list if "INVALID" in c[0][0]
        ]
        assert len(rel_calls) == 0

    def test_returns_true_on_success(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = [{"n.id": "n1"}]
        assert kg.update_note("proj-1", "n1", content="x") is True


# =============================================================================
# 10.9: get_related()
# =============================================================================


class TestGetRelated:
    """Tests for get_related() method."""

    def test_caps_max_hops_at_3(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.get_related("proj-1", "n1", max_hops=10)
        query = mock_db.execute_query.call_args[0][0]
        assert "*1..3" in query

    def test_excludes_starting_note(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.get_related("proj-1", "n1")
        query = mock_db.execute_query.call_args[0][0]
        assert "related.id <> $nid" in query

    def test_returns_results(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = [
            {
                "id": "n2",
                "title": "Note 2",
                "type": "learning",
                "status": "active",
                "distance": 1,
                "rel_types": ["SUPPORTS"],
            }
        ]
        result = kg.get_related("proj-1", "n1")
        assert len(result) == 1
        assert result[0]["id"] == "n2"

    def test_returns_empty_when_no_related(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        assert kg.get_related("proj-1", "n1") == []


# =============================================================================
# 10.10: get_contradictions()
# =============================================================================


class TestGetContradictions:
    """Tests for get_contradictions() method."""

    def test_filters_active_notes_only(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.get_contradictions("proj-1")
        query = mock_db.execute_query.call_args[0][0]
        assert "a.status = 'active'" in query
        assert "b.status = 'active'" in query

    def test_returns_contradiction_pairs(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = [
            {"note_a": "n1", "title_a": "A", "note_b": "n2", "title_b": "B"}
        ]
        result = kg.get_contradictions("proj-1")
        assert len(result) == 1
        assert result[0]["note_a"] == "n1"
        assert result[0]["note_b"] == "n2"

    def test_returns_empty_when_none(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        assert kg.get_contradictions("proj-1") == []


# =============================================================================
# 10.11: get_provenance()
# =============================================================================


class TestGetProvenance:
    """Tests for get_provenance() method."""

    def test_follows_derived_from_only(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.get_provenance("proj-1", "n1")
        query = mock_db.execute_query.call_args[0][0]
        assert "DERIVED_FROM" in query

    def test_caps_max_depth_at_10(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.get_provenance("proj-1", "n1", max_depth=20)
        query = mock_db.execute_query.call_args[0][0]
        assert "*1..10" in query

    def test_excludes_starting_note(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.get_provenance("proj-1", "n1")
        query = mock_db.execute_query.call_args[0][0]
        assert "node.id <> $nid" in query

    def test_ordered_by_depth(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.get_provenance("proj-1", "n1")
        query = mock_db.execute_query.call_args[0][0]
        assert "ORDER BY depth" in query


# =============================================================================
# 10.12: get_unanswered()
# =============================================================================


class TestGetUnanswered:
    """Tests for get_unanswered() method."""

    def test_filters_question_type(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.get_unanswered("proj-1")
        query = mock_db.execute_query.call_args[0][0]
        assert "type: 'question'" in query

    def test_filters_active_status(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.get_unanswered("proj-1")
        query = mock_db.execute_query.call_args[0][0]
        assert "status: 'active'" in query

    def test_excludes_outgoing_answers(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.get_unanswered("proj-1")
        query = mock_db.execute_query.call_args[0][0]
        assert "NOT (q)-[:ANSWERS]->()" in query

    def test_excludes_incoming_answers(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = []
        kg.get_unanswered("proj-1")
        query = mock_db.execute_query.call_args[0][0]
        assert "NOT ()-[:ANSWERS]->(q)" in query

    def test_returns_expected_fields(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.return_value = [
            {
                "id": "q1",
                "title": "Why?",
                "content": "Details",
                "created": "2026-01-01",
                "job_id": "j1",
            }
        ]
        result = kg.get_unanswered("proj-1")
        assert result[0]["id"] == "q1"
        assert result[0]["title"] == "Why?"


# =============================================================================
# 10.13: get_all_notes_for_export()
# =============================================================================


class TestGetAllNotesForExport:
    """Tests for get_all_notes_for_export() method."""

    def test_returns_all_notes(self):
        kg, mock_db = _make_kg()
        mock_node = MagicMock()
        mock_node.items.return_value = [("id", "n1"), ("title", "A")]
        mock_db.execute_query.side_effect = [
            [{"n": mock_node, "tags": ["t1"], "keywords": ["k1"]}],
            [{"rel_type": "SUPPORTS", "target_id": "n2"}],  # rels for n1
        ]
        result = kg.get_all_notes_for_export("proj-1")
        assert len(result) == 1
        assert result[0]["id"] == "n1"
        assert result[0]["tags"] == ["t1"]
        assert result[0]["keywords"] == ["k1"]

    def test_handles_dict_nodes(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.side_effect = [
            [{"n": {"id": "n1", "title": "A"}, "tags": [], "keywords": []}],
            [],  # rels
        ]
        result = kg.get_all_notes_for_export("proj-1")
        assert len(result) == 1
        assert result[0]["id"] == "n1"

    def test_skips_non_node_non_dict(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.side_effect = [
            [{"n": 42, "tags": [], "keywords": []}],  # int node
        ]
        result = kg.get_all_notes_for_export("proj-1")
        assert len(result) == 0

    def test_includes_relationships(self):
        kg, mock_db = _make_kg()
        mock_db.execute_query.side_effect = [
            [{"n": {"id": "n1"}, "tags": [], "keywords": []}],
            [{"rel_type": "REFERENCES", "target_id": "n2"}],
        ]
        result = kg.get_all_notes_for_export("proj-1")
        assert len(result[0]["relationships"]) == 1
        assert result[0]["relationships"][0]["type"] == "REFERENCES"


# =============================================================================
# 10.14: delete_project_knowledge()
# =============================================================================


class TestDeleteProjectKnowledge:
    """Tests for delete_project_knowledge() method."""

    def test_uses_detach_delete(self):
        kg, mock_db = _make_kg()
        mock_db.execute_write.return_value = [{"cnt": 5}]
        kg.delete_project_knowledge("proj-1")
        query = mock_db.execute_write.call_args[0][0]
        assert "DETACH DELETE" in query

    def test_matches_by_project_id(self):
        kg, mock_db = _make_kg()
        mock_db.execute_write.return_value = [{"cnt": 0}]
        kg.delete_project_knowledge("proj-1")
        query = mock_db.execute_write.call_args[0][0]
        assert "project_id: $pid" in query

    def test_returns_deleted_count(self):
        kg, mock_db = _make_kg()
        mock_db.execute_write.return_value = [{"cnt": 7}]
        assert kg.delete_project_knowledge("proj-1") == 7

    def test_returns_zero_when_empty(self):
        kg, mock_db = _make_kg()
        mock_db.execute_write.return_value = []
        assert kg.delete_project_knowledge("proj-1") == 0


# =============================================================================
# 10.15: get_note_content_hash()
# =============================================================================


class TestGetNoteContentHash:
    """Tests for get_note_content_hash() method."""

    def test_returns_sha256_hex(self):
        kg, _ = _make_kg()
        content = "test content"
        expected = hashlib.sha256(content.encode()).hexdigest()
        assert kg.get_note_content_hash(content) == expected

    def test_different_content_different_hash(self):
        kg, _ = _make_kg()
        h1 = kg.get_note_content_hash("abc")
        h2 = kg.get_note_content_hash("def")
        assert h1 != h2

    def test_same_content_same_hash(self):
        kg, _ = _make_kg()
        h1 = kg.get_note_content_hash("hello")
        h2 = kg.get_note_content_hash("hello")
        assert h1 == h2
