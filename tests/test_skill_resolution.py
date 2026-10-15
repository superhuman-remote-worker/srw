"""Pure skill-menu resolution + fencing (Agent Skills, Slice 2).

Mirrors the experts precedence model (src/core/expert_resolution.py) but the
menu keeps ALL names (bundled is the floor) instead of picking one winner.
"""

import pytest

from shared.runtime.core.skill_resolution import (
    APP_GUIDE_BREAK_GLASS_ENV,
    APP_GUIDE_LOADER_TOOL,
    APP_GUIDE_SKILL,
    add_default_canvas_skill,
    add_persistent_system_skills,
    app_guide_break_glass_disabled,
    app_guide_health_snapshot,
    is_reserved_system_skill_name,
    managed_product_guide_turn_boundary,
    resolve_skill_menu,
    scope_skills_for_tools,
    skill_bundle_digest,
    skill_files_to_workspace,
)
from shared.runtime.core.expert_resolution import fence_skills_menu

U = "11111111-1111-1111-1111-111111111111"


def _row(name, *, owner_id=None, is_global=False, created_at="2026-01-01", **extra):
    return {
        "name": name,
        "description": f"desc-{name}",
        "owner_id": owner_id,
        "is_global": is_global,
        "created_at": created_at,
        **extra,
    }


def test_menu_keeps_bundled_floor():
    rows = [_row("a", _source="bundled")]
    menu = resolve_skill_menu(rows, user_id=U, project_ids=set())
    assert [m["name"] for m in menu] == ["a"]
    assert menu[0]["_source"] == "bundled"


def test_owner_shadows_bundled_same_name():
    rows = [
        _row("a", _source="bundled"),
        _row("a", owner_id=U, _source="user"),
    ]
    menu = resolve_skill_menu(rows, user_id=U, project_ids=set())
    assert len(menu) == 1
    assert menu[0]["_source"] == "user"  # owner (tier 3) wins


def test_global_shadows_bundled_but_not_owner():
    rows = [
        _row("a", _source="bundled"),
        _row("a", is_global=True, _source="global"),
        _row("a", owner_id=U, _source="user"),
    ]
    menu = resolve_skill_menu(rows, user_id=U, project_ids=set())
    assert len(menu) == 1 and menu[0]["_source"] == "user"


def test_menu_is_sorted_by_name_deterministic():
    rows = [_row("zeta"), _row("alpha"), _row("mid", owner_id=U)]
    names = [m["name"] for m in resolve_skill_menu(rows, user_id=U, project_ids=set())]
    assert names == ["alpha", "mid", "zeta"]


def test_files_to_workspace_prefixes_skill_dir():
    out = skill_files_to_workspace({"hello": {"SKILL.md": "x", "references/g.md": "y"}})
    assert out == {
        "skills/hello/SKILL.md": "x",
        "skills/hello/references/g.md": "y",
    }


def test_canvas_skill_requires_use_skill_and_file_set_capability():
    skills = {
        "menu": [
            {"name": "present-with-canvas"},
            {"name": "ordinary-skill"},
        ],
        "files": {
            "present-with-canvas": {"SKILL.md": "canvas"},
            "ordinary-skill": {"SKILL.md": "ordinary"},
        },
    }

    scoped = scope_skills_for_tools(skills, ["use_skill"])
    assert [item["name"] for item in scoped["menu"]] == ["ordinary-skill"]
    assert set(scoped["files"]) == {"ordinary-skill"}

    # Without use_skill nothing is offered at all (skills_tool_category.md D2).
    for tools in ([], ["set_canvas"]):
        scoped = scope_skills_for_tools(skills, tools)
        assert scoped["menu"] == []
        assert scoped["files"] == {}

    scoped = scope_skills_for_tools(skills, ["use_skill", "set_canvas"])
    assert [item["name"] for item in scoped["menu"]] == [
        "present-with-canvas",
        "ordinary-skill",
    ]
    assert set(scoped["files"]) == {"present-with-canvas", "ordinary-skill"}
    # The helper is pure so backend re-scoping can restore the skill later.
    assert "present-with-canvas" in skills["files"]


def test_default_canvas_skill_is_the_only_catalog_floor_when_db_payload_is_empty():
    catalog = add_default_canvas_skill({})

    assert [item["name"] for item in catalog["menu"]] == ["present-with-canvas"]
    assert set(catalog["files"]) == {"present-with-canvas"}
    assert "Present With Canvas" in catalog["files"]["present-with-canvas"]["SKILL.md"]


def test_default_canvas_skill_does_not_override_resolved_replacement():
    replacement = {
        "menu": [{"name": "present-with-canvas", "description": "custom"}],
        "files": {"present-with-canvas": {"SKILL.md": "custom body"}},
    }

    assert add_default_canvas_skill(replacement) == replacement


def test_persistent_system_floor_replaces_app_guide_shadow_with_running_bundle():
    replacement = {
        "menu": [
            {"name": APP_GUIDE_SKILL, "description": "untrusted replacement"},
            {"name": "ordinary-skill", "description": "keep"},
        ],
        "files": {
            APP_GUIDE_SKILL: {"SKILL.md": "MUTABLE-OR-STALE"},
            "ordinary-skill": {"SKILL.md": "ordinary"},
        },
    }

    catalog = add_persistent_system_skills(replacement)

    app_entry = next(
        item for item in catalog["menu"] if item["name"] == APP_GUIDE_SKILL
    )
    assert app_entry["system_managed"] is True
    assert app_entry["loader_tool"] == APP_GUIDE_LOADER_TOOL
    assert "MUTABLE-OR-STALE" not in catalog["files"][APP_GUIDE_SKILL]["SKILL.md"]
    assert app_entry["bundle_digest"] == skill_bundle_digest(
        catalog["files"][APP_GUIDE_SKILL]
    )
    assert catalog["files"]["ordinary-skill"]["SKILL.md"] == "ordinary"


def test_persistent_system_floor_refreshes_app_guide_digest(tmp_path):
    root = tmp_path / "skills"
    app_dir = root / APP_GUIDE_SKILL
    app_dir.mkdir(parents=True)
    skill_md = app_dir / "SKILL.md"
    skill_md.write_text(
        "---\nname: app-guide\ndescription: current\n---\nVERSION ONE\n",
        encoding="utf-8",
    )

    first = add_persistent_system_skills({}, skills_root=root)
    first_entry = next(
        item for item in first["menu"] if item["name"] == APP_GUIDE_SKILL
    )

    skill_md.write_text(
        "---\nname: app-guide\ndescription: current\n---\nVERSION TWO\n",
        encoding="utf-8",
    )
    second = add_persistent_system_skills(first, skills_root=root)
    second_entry = next(
        item for item in second["menu"] if item["name"] == APP_GUIDE_SKILL
    )

    assert first_entry["bundle_digest"] != second_entry["bundle_digest"]
    assert "VERSION TWO" in second["files"][APP_GUIDE_SKILL]["SKILL.md"]


def test_persistent_system_floor_never_falls_back_to_replacement_if_bundle_missing(
    tmp_path,
):
    replacement = {
        "menu": [{"name": APP_GUIDE_SKILL, "description": "replacement"}],
        "files": {APP_GUIDE_SKILL: {"SKILL.md": "UNTRUSTED FALLBACK"}},
    }

    catalog = add_persistent_system_skills(
        replacement,
        skills_root=tmp_path / "missing-skills",
    )

    assert all(item.get("name") != APP_GUIDE_SKILL for item in catalog["menu"])
    assert APP_GUIDE_SKILL not in catalog["files"]


@pytest.mark.parametrize("value", ["1", "TRUE", " yes ", "On"])
def test_app_guide_break_glass_uses_one_bounded_truthy_policy(value):
    assert app_guide_break_glass_disabled({APP_GUIDE_BREAK_GLASS_ENV: value})


@pytest.mark.parametrize("value", ["", "0", "false", "no", "off", "unexpected"])
def test_app_guide_break_glass_defaults_and_false_values_stay_enabled(value):
    assert not app_guide_break_glass_disabled({APP_GUIDE_BREAK_GLASS_ENV: value})


def test_break_glass_removes_untrusted_guide_but_preserves_other_catalog_floors(
    monkeypatch,
):
    replacement = {
        "menu": [
            {"name": APP_GUIDE_SKILL, "description": "untrusted"},
            {"name": "ordinary-skill", "description": "keep"},
        ],
        "files": {
            APP_GUIDE_SKILL: {"SKILL.md": "UNTRUSTED"},
            "ordinary-skill": {"SKILL.md": "ordinary"},
        },
    }
    monkeypatch.setenv(APP_GUIDE_BREAK_GLASS_ENV, "true")

    catalog = add_persistent_system_skills(replacement)

    names = {item["name"] for item in catalog["menu"]}
    assert APP_GUIDE_SKILL not in names
    assert names == {"ordinary-skill", "present-with-canvas"}
    assert APP_GUIDE_SKILL not in catalog["files"]
    assert catalog["files"]["ordinary-skill"]["SKILL.md"] == "ordinary"


def test_reenable_rebind_restores_current_managed_digest(monkeypatch):
    replacement = {
        "menu": [{"name": APP_GUIDE_SKILL, "description": "stale"}],
        "files": {APP_GUIDE_SKILL: {"SKILL.md": "STALE"}},
    }
    monkeypatch.setenv(APP_GUIDE_BREAK_GLASS_ENV, "true")
    disabled = add_persistent_system_skills(replacement)
    assert APP_GUIDE_SKILL not in disabled["files"]

    monkeypatch.delenv(APP_GUIDE_BREAK_GLASS_ENV)
    restored = add_persistent_system_skills(disabled)
    entry = next(item for item in restored["menu"] if item["name"] == APP_GUIDE_SKILL)

    assert entry["system_managed"] is True
    assert entry["bundle_digest"] == skill_bundle_digest(
        restored["files"][APP_GUIDE_SKILL]
    )
    assert "STALE" not in restored["files"][APP_GUIDE_SKILL]["SKILL.md"]


def test_app_guide_health_snapshot_is_bounded_and_distinguishes_failure_modes(
    tmp_path,
):
    assert app_guide_health_snapshot(environ={}) == {"state": "ready"}
    assert app_guide_health_snapshot(environ={APP_GUIDE_BREAK_GLASS_ENV: "true"}) == {
        "state": "disabled",
        "reason": "operator_break_glass",
    }
    assert app_guide_health_snapshot(
        skills_root=tmp_path / "missing",
        environ={},
    ) == {"state": "unavailable", "reason": "bundle_unavailable"}
    assert app_guide_health_snapshot(
        reader_available=False,
        environ={},
    ) == {"state": "unavailable", "reason": "reader_unavailable"}


def test_app_guide_scope_requires_its_dedicated_reader():
    skills = {
        "menu": [
            {"name": APP_GUIDE_SKILL},
            {"name": "ordinary-skill"},
        ],
        "files": {
            APP_GUIDE_SKILL: {"SKILL.md": "guide"},
            "ordinary-skill": {"SKILL.md": "ordinary"},
        },
    }

    without_reader = scope_skills_for_tools(skills, ["use_skill"])
    assert [item["name"] for item in without_reader["menu"]] == ["ordinary-skill"]
    assert set(without_reader["files"]) == {"ordinary-skill"}

    with_reader = scope_skills_for_tools(skills, [APP_GUIDE_LOADER_TOOL, "use_skill"])
    assert [item["name"] for item in with_reader["menu"]] == [
        APP_GUIDE_SKILL,
        "ordinary-skill",
    ]
    assert set(with_reader["files"]) == {APP_GUIDE_SKILL, "ordinary-skill"}

    # The guide has its own reader, so it stays when use_skill does not.
    reader_only = scope_skills_for_tools(skills, [APP_GUIDE_LOADER_TOOL])
    assert [item["name"] for item in reader_only["menu"]] == [APP_GUIDE_SKILL]
    assert set(reader_only["files"]) == {APP_GUIDE_SKILL}


def test_app_guide_turn_boundary_requires_trusted_live_reader():
    catalog = add_persistent_system_skills({})
    boundary = managed_product_guide_turn_boundary(
        catalog,
        [APP_GUIDE_LOADER_TOOL],
    )

    entry = next(item for item in catalog["menu"] if item["name"] == APP_GUIDE_SKILL)
    assert "<managed_product_guide_turn_boundary" in boundary
    assert entry["bundle_digest"] in boundary
    assert "not current SRW product documentation" in boundary
    assert "must call `read_product_guide` now" in boundary
    assert "topic only from the actual user request" in boundary
    assert "start with `index` when uncertain" in boundary
    assert managed_product_guide_turn_boundary(catalog, ["read_file"]) == ""

    spoof = {
        "menu": [
            {
                "name": APP_GUIDE_SKILL,
                "system_managed": False,
                "loader_tool": APP_GUIDE_LOADER_TOOL,
            }
        ]
    }
    assert (
        managed_product_guide_turn_boundary(
            spoof,
            [APP_GUIDE_LOADER_TOOL],
        )
        == ""
    )


def test_app_guide_name_is_reserved_for_the_running_product():
    assert is_reserved_system_skill_name(APP_GUIDE_SKILL)
    assert not is_reserved_system_skill_name("my-app-guide-extension")


def test_fence_skills_menu_empty_is_blank():
    assert fence_skills_menu([]) == ""


def test_fence_skills_menu_wraps_and_strips_braces():
    out = fence_skills_menu([{"name": "a", "description": "use {when} ok"}])
    assert "<available_skills" in out and "</available_skills>" in out
    assert "- a: use when ok" in out  # braces stripped
    assert "{" not in out and "}" not in out


def test_fence_skills_menu_names_managed_app_guide_reader():
    out = fence_skills_menu(
        [
            {
                "name": APP_GUIDE_SKILL,
                "description": "Current product help",
                "system_managed": True,
                "loader_tool": APP_GUIDE_LOADER_TOOL,
            }
        ]
    )

    assert (
        "- app-guide [load with read_product_guide(topic_id)]: Current product help"
        in out
    )


# --- Slice 3: bound skills are removed from the model-invoked catalog ---

from shared.runtime.core.skill_resolution import filter_bound_skills  # noqa: E402


def test_filter_removes_bound_skill_from_menu_and_files():
    blob = {
        "agent": {
            "instruction_files": [
                {"skill": "todo-guide", "trigger": "before_tool:next_phase_todos"},
                {"file": "x.md", "trigger": "phase:tactical"},
            ]
        },
        "skills": {
            "menu": [{"name": "todo-guide"}, {"name": "free-skill"}],
            "files": {"todo-guide": {"SKILL.md": "x"}, "free-skill": {"SKILL.md": "y"}},
        },
    }
    filter_bound_skills(blob)
    assert [m["name"] for m in blob["skills"]["menu"]] == ["free-skill"]
    assert set(blob["skills"]["files"]) == {"free-skill"}


def test_filter_noop_without_skills_or_bindings():
    assert filter_bound_skills({"agent": {}}) == {"agent": {}}
    blob = {
        "agent": {"instruction_files": []},
        "skills": {"menu": [{"name": "a"}], "files": {}},
    }
    filter_bound_skills(blob)
    assert [m["name"] for m in blob["skills"]["menu"]] == [
        "a"
    ]  # nothing bound → unchanged
