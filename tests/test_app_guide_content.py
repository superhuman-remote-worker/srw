"""Focused content contracts for the managed SRW product guide.

These checks do not attempt to prove every sentence from implementation. They
pin the safety/actionability boundaries whose drift would make the guide teach
users an unsafe or impossible workflow, and force a coverage decision when a
live session control group gains a tool.
"""

from __future__ import annotations

from shared.runtime.core.srw_manifest_config import (
    srw_config_fragment as _srw_config_fragment,
)

from pathlib import Path
from typing import get_args

import yaml

from orchestrator.services.canvas import CanvasRenderer
from orchestrator.services.cloud_staging import select_protected_mount
from orchestrator.services.default_experts import MANAGED_SEEDS
from orchestrator.services.project_loops import (
    LOOP_ANALYSIS_ROLES,
    LOOP_CAMPAIGN_BUDGET_RESERVE,
    LOOP_CAMPAIGN_DEFAULT_CAPS,
)
from orchestrator.services.shared_browser_canvas import BrowserCapabilityReason
from shared.runtime.core.capability_grants import CATALOG
from shared.runtime.core.session_tool_overrides import SESSION_TOOL_OVERRIDE_NAMES
from agent.tools.canvas import CANVAS_TOOLS_METADATA
from agent.tools.research.browser_direct import BROWSER_DIRECT_TOOLS_METADATA

_ROOT = Path(__file__).resolve().parents[1]
_GUIDE = _ROOT / "config" / "skills" / "app-guide"


def _focused_topic(topic: str) -> tuple[dict, str]:
    text = (_GUIDE / "references" / f"{topic}.md").read_text(encoding="utf-8")
    metadata_text, body = text.removeprefix("---\n").split("\n---\n", 1)
    return yaml.safe_load(metadata_text), " ".join(body.lower().split())


def test_guide_requires_outcome_level_coverage_and_honest_combination_gaps():
    text = (_GUIDE / "SKILL.md").read_text(encoding="utf-8")
    _, body = text.removeprefix("---\n").split("\n---\n", 1)
    body = " ".join(body.lower().split())

    assert "match the user's requested **outcome**, not just nouns" in body
    assert (
        "documented components are not proof that srw supports combining them" in body
    )
    assert "state an explicit guide gap for the built-in route" in body
    assert "continue with step 6" in body
    assert "do not load the nearest topic to manufacture a supported" in body
    assert "it is not a catch-all place to search for a feature absent" in body
    assert "enterprise identity administration terms such as sso, scim, saml" in body
    assert "are not project-group or datasource workflows" in body
    assert "identify the administrator or integration check needed" in body
    assert "route to `memory-and-knowledge`" in body
    assert "the currently proven path requires a **container workspace**" in body
    assert "virtual and none cannot host it" in body
    assert "the guide does not document this exact workflow" in body
    assert "is covered by the `automations` limitation row" in body
    assert "load `automations` and explain the connector boundary" in body
    assert "connector, prompt, expert, or permission documented separately" in body


def test_guide_separates_stable_how_to_from_dynamic_capability_checks():
    text = (_GUIDE / "SKILL.md").read_text(encoding="utf-8")
    _, body = text.removeprefix("---\n").split("\n---\n", 1)
    body = " ".join(body.lower().split())

    assert "after reading the focused guide, call `get_product_capabilities`" in body
    assert "do not call it for stable concepts" in body
    assert "copy only the exact relevant `capability_ids`" in body
    assert "product-guide topic ids and capability-tool topic ids are different" in body
    assert "call the capability tool without filters" in body
    assert "the live registry does not cover it and treat it as unknown" in body
    assert "keep stable guide instructions available" in body


def test_guide_preserves_partial_unknown_mixed_and_operation_boundaries():
    text = (_GUIDE / "SKILL.md").read_text(encoding="utf-8")
    _, body = text.removeprefix("---\n").split("\n---\n", 1)
    body = " ".join(body.lower().split())

    assert "top-level `status` as authoritative" in body
    assert "current capability state comes from the same-turn" in body
    for state in (
        "`unknown`",
        "`no_opinion`",
        "`degraded`",
        "`needs_attachment`",
        "`needs_upgrade`",
        "`not_ready`",
    ):
        assert state in body
    assert "`partial` result" in body and "`truncated=true`" in body
    assert "`product.mixed_build=true`" in body
    assert "`product.mixed_build=false` means only that known observed" in body
    assert "advisory snapshot at `evaluated_at`, never authorization" in body
    assert "a visible operation may still be offered as an attempt" in body
    assert "call the exact operation tool currently visible" in body
    assert "action-time checks, which vary by tool" in body
    assert "do not claim that an already-bound call re-fetched" in body
    assert "never pass the capability result to an operation as authority" in body


def test_focused_guides_keep_metadata_contracts():
    expected = {
        "overview": {
            "guide_id": "product.overview",
            "content_type": "explanation",
            "capability_ids": {
                "jobs.create",
                "sessions.permission-mode",
                "projects.manage",
            },
            "journey_ids": {"product.first-run"},
        },
        "sessions": {
            "guide_id": "sessions.use",
            "content_type": "how_to",
            "capability_ids": {
                "sessions.permission-mode",
                "workspaces.select",
            },
            "journey_ids": {"sessions.start", "sessions.resume"},
        },
        "jobs": {
            "guide_id": "jobs.run",
            "content_type": "how_to",
            "capability_ids": {"jobs.create", "jobs.review"},
            "journey_ids": {"jobs.create", "jobs.monitor", "jobs.review"},
        },
        "automations": {
            "guide_id": "automations.schedule",
            "content_type": "how_to",
            "capability_ids": {"automations.manage"},
            "journey_ids": {"automations.create"},
        },
        "fleet-and-delegation": {
            "guide_id": "sessions.delegate",
            "content_type": "how_to",
            "capability_ids": {
                "sessions.delegate",
                "jobs.create",
                "jobs.review",
            },
            "journey_ids": {"sessions.delegate-job"},
        },
        "canvas-and-browser": {
            "guide_id": "canvas.present-and-browse",
            "content_type": "how_to",
            "capability_ids": {
                "canvas.files",
                "canvas.browser",
                "workspaces.select",
            },
            "journey_ids": {"canvas.present-file", "canvas.share-browser"},
        },
        "experts": {
            "guide_id": "experts.choose-and-customize",
            "content_type": "reference",
            "capability_ids": {"experts.select", "experts.manage"},
            "journey_ids": {"experts.choose", "experts.customize"},
        },
        "projects-and-loops": {
            "guide_id": "projects.organize",
            "content_type": "how_to",
            "capability_ids": {"projects.manage"},
            "journey_ids": {"projects.create", "projects.configure"},
        },
        "permissions-and-availability": {
            "guide_id": "sessions.permissions-and-workspaces",
            "content_type": "how_to",
            "capability_ids": {
                "sessions.permission-mode",
                "workspaces.select",
            },
            "journey_ids": {
                "sessions.configure-permissions",
                "sessions.choose-workspace",
            },
        },
        "project-loops": {
            "guide_id": "projects.loops.run",
            "content_type": "how_to",
            "capability_ids": {"projects.loops"},
            "journey_ids": {
                "projects.loops.start",
                "projects.loops.monitor",
            },
        },
        "protected-cloud": {
            "guide_id": "sessions.protected-cloud",
            "content_type": "how_to",
            "capability_ids": {"sessions.protected-cloud"},
            "journey_ids": {
                "sessions.protected-cloud.create",
                "sessions.protected-cloud.review",
            },
        },
        "memory-and-knowledge": {
            "guide_id": "memory-and-knowledge.understand",
            "content_type": "explanation",
            "capability_ids": {"memory.recall", "projects.knowledge"},
            "journey_ids": {
                "memory.configure-scope",
                "projects.knowledge.browse",
                "projects.knowledge.record",
            },
        },
        "files-and-integrations": {
            "guide_id": "workspaces.files-and-results",
            "content_type": "explanation",
            "capability_ids": {"workspaces.select"},
            "journey_ids": {"workspaces.find-results"},
        },
    }

    for topic, wanted in expected.items():
        metadata, _ = _focused_topic(topic)
        assert metadata["guide_id"] == wanted["guide_id"]
        assert metadata["content_type"] == wanted["content_type"]
        assert set(metadata["capability_ids"]) == wanted["capability_ids"]
        assert set(metadata["journey_ids"]) == wanted["journey_ids"]


def test_jobs_guide_keeps_current_workspace_review_and_pause_boundaries():
    _, body = _focused_topic("jobs")

    assert "loading this app guide does not grant those tools" in body
    assert "worker expert; session experts are not eligible for jobs" in body
    for label in (
        "**container**",
        "**vm (qemu)**",
        "**virtual (cloud files)**",
        "**none (no workspace)**",
    ):
        assert label in body
    assert "virtual keeps file tools but disables shell, browser, and git" in body
    assert "none also disables file tools" in body
    assert "autonomy controls normal phase-boundary check-ins" in body
    assert "**partial** — freezes at phase boundaries and at completion" in body
    assert "**guided** — freezes after every tactical phase" in body
    assert "some pause reasons are retried or redispatched automatically" in body
    assert "instead of assuming capacity is the cause" in body
    assert "do not promise this review mode for every project" in body
    assert "critic review and the scholar research pre-pass are configurable" in body
    assert "neither should be described as running for every job" in body


def test_sessions_guide_requires_current_capability_qualification():
    _, body = _focused_topic("sessions")

    assert "possible session capabilities, not proof" in body
    assert "current workspace" in body and "selected tools" in body
    assert "effective grants" in body and "deployment configuration" in body
    assert "use `get_product_capabilities` without filters" in body
    assert "exact operation tool" in body


def test_experts_guide_covers_current_bundled_roster_and_selection_rules():
    _, body = _focused_topic("experts")

    for config_path in sorted((_ROOT / "config" / "experts").glob("*/config.yaml")):
        config = _srw_config_fragment(
            yaml.safe_load(config_path.read_text(encoding="utf-8"))
        )
        expert_type = (
            "session" if config.get("$extends") == "session_base" else "worker"
        )
        row = f"| **{config['display_name'].lower()}** | {expert_type} |"
        assert row in body, config_path.parent.name

    assert {seed["directory"] for seed in MANAGED_SEEDS} == {
        "assistant",
        "general-worker",
    }
    assert "initial application worker default" in body
    assert "initial application session default" in body
    assert "explicitly selected for this run" in body
    assert "selected project's default" in body
    assert "your personal default" in body
    assert "the application default" in body
    assert "it is immutable after creation" in body
    assert "bundled disk experts are read-only" in body
    assert "duplicate" in body and "owned copy" in body
    assert "cannot be deleted while jobs, sessions, automations" in body
    assert "this static guide cannot inspect that state" in body


def test_memory_guide_separates_compaction_recall_native_kb_and_external_okf():
    _, body = _focused_topic("memory-and-knowledge")

    assert "context compaction is not long-term memory" in body
    assert "it is not by itself a promise" in body
    assert "when memory is enabled" in body
    assert "failures can degrade the feature" in body
    assert "a run configured to require memory may pause or fail loudly" in body
    assert "**share memories across jobs** / `project_scoped` enabled" in body
    assert (
        "without a project scope, memory stays with the current job or session" in body
    )
    assert "memory has no general cockpit browsing/editor surface" in body
    assert (
        "the current knowledge tab does not provide a free-form create/content editor"
        in body
    )
    assert "verify that the session actually has knowledge tools" in body
    assert "additional, reusable, read-only library" in body
    assert "does not merge it into the writable native project knowledge base" in body
    assert "do not use memory as the only record of something critical" in body


def test_email_guide_requires_both_scope_and_explicit_attachment():
    _, body = _focused_topic("datasources-email")

    assert "folder allowlist" in body
    assert "leaving this empty shares the whole mailbox" in body
    assert "it does **not** attach the connector" in body
    assert "finish by selecting it for this session" in body


def test_okf_guide_separates_source_validation_from_index_readiness():
    _, body = _focused_topic("datasources-okf")

    assert "a complete setup has two separate checks" in body
    assert "test connection" in body and "validates the source" in body
    assert "index state must reach **ready**" in body
    assert "does not mean that every note is searchable yet" in body
    assert "initial indexing starts in the background" in body


def test_automation_guide_keeps_current_safety_and_actionability_boundaries():
    _, body = _focused_topic("automations")

    assert "current built-in automation surface is schedule-only" in body
    assert "related features are not proof of a supported combined workflow" in body
    assert "scheduled work that reads a mailbox or database" in body
    assert "automation-fired jobs attach no connectors" in body
    assert "do not invent a connector attachment through project linkage" in body
    assert "run now" in body and "does not move the next scheduled run" in body
    assert "automation-fired jobs do not attach connectors" in body
    assert "automatically pauses the automation" in body
    assert "catchup window" in body and "does not backfill every missed" in body
    assert "scheduled delivery is at-least-once" in body
    assert "pinned by its stable id" in body
    assert "unpinned `worker_base` resolves" in body
    assert (
        "default session group cannot save, enable, run, pause, edit, or delete" in body
    )
    assert "app guide itself does not grant workflow tools" in body


def test_fleet_guide_keeps_current_scope_and_fallback_boundaries():
    _, body = _focused_topic("fleet-and-delegation")

    assert "fleet management" in body
    assert "jobs → new job" in body
    assert "normally inherits this session's selected connectors" in body
    assert "app guide does not grant fleet management" in body
    assert "parallel only when fleet capacity permits" in body
    assert "three simultaneous starts are not guaranteed" in body
    assert "does not delete jobs" in body
    assert "does not" in body and "continuously monitor a job" in body
    assert "delegation" in body and "different ways srw can split work" in body
    assert "non-admin users need the delegation grant" in body


def test_fleet_guide_covers_published_evidence():
    """E5 (officer_supervision_surface §3.3): the three bounded evidence reads
    have explicit prose, framed as judgment material rather than file access,
    with the availability-honesty rule (`unavailable` is never `empty`)."""
    _, body = _focused_topic("fleet-and-delegation")

    for tool_name in (
        "get_job_completion_report",
        "list_job_evidence",
        "read_job_evidence",
    ):
        assert f"`{tool_name}`" in body, tool_name
    assert "published completion evidence" in body
    assert "pinned at completion" in body
    assert "judgment material, not a live file browser" in body
    assert "a source that could not be reached, not an empty result" in body


def test_jobs_guide_reads_liveness_instead_of_fabricated_progress():
    """E5 (officer_supervision_surface §5): progress is server-computed
    liveness; no percent/ETA is fabricated; `suspected_stuck` prompts
    investigation and `unavailable` is never presented as inactivity."""
    _, body = _focused_topic("jobs")

    assert "liveness**, not a percentage" in body
    for state in (
        "`active`",
        "`waiting`",
        "`paused`",
        "`suspected_stuck`",
        "`unavailable`",
        "`terminal`",
    ):
        assert state in body, state
    assert "reasons and a last-activity time" in body
    assert "`progress_percent` field is honestly `null`" in body
    assert "does not fabricate a percent or an eta" in body
    assert '`suspected_stuck` means "investigate"' in body
    assert 'never present it as "no activity"' in body


def test_canvas_guide_keeps_renderer_handoff_and_availability_boundaries():
    _, body = _focused_topic("canvas-and-browser")

    assert "canvas is not another place where srw stores a copy" in body
    assert "svg, pdf, mermaid" in body and "are not supported" in body
    assert "scripts, forms, animations" in body
    assert "html-interactive" in body and "`auto` never selects it" in body
    assert "opaque, no-network sandbox" in body
    assert "every css, script, image, and font" in body
    assert "live preview" in body and "deployment enables" in body
    assert "closing the pane is local" in body
    assert "does not delete the file" in body
    assert "take control" in body and "release control" in body
    assert "required workspace" in body and "container workspace" in body
    assert "user_is_driving" in body
    assert "same dom, cookies, and authenticated state" in body
    assert "closing the canvas pane merely detaches your viewer" in body
    assert "not available on the jobs view" in body
    assert "do not promise vm support" in body
    assert "shared browser is off on this server" in body
    assert "intentionally hidden" in body
    assert "app guide itself grants neither" in body


def test_permissions_guide_keeps_current_policy_and_workspace_boundaries():
    _, body = _focused_topic("permissions-and-availability")

    assert "does not itself inspect current flags" in body
    assert "uses `get_product_capabilities`" in body
    assert "advisory, time-stamped observation, not authorization" in body
    assert "supervised" in body and "before every tool call" in body
    assert "auto-accept" in body
    for tool_name in ("run_command", "shell_execute", "shell_read"):
        assert tool_name in body
    assert "autonomous" in body and "without per-call approval" in body
    assert "permission mode does not add tools" in body
    assert "built-in non-admin ceiling allows auto-accept" in body
    assert "autonomous needs an explicit grant" in body
    assert "global, project, and user scope" in body
    assert "most restrictive result wins" in body
    assert "installation default is container for jobs and virtual for sessions" in body
    for tier in ("virtual", "container", "none", "vm"):
        assert f"| **{tier}**" in body
    assert "workspace changes are upgrade-only" in body
    for group in (
        "browser",
        "shell",
        "canvas",
        "job control",
        "job inspection",
        "experts & skills",
        "automations & loops",
    ):
        assert f"**{group}**" in body
    assert "admin → grants" in body
    assert "do not recommend switching to autonomous" in body


def test_project_loop_guide_keeps_current_budget_control_and_action_boundaries():
    _, body = _focused_topic("project-loops")

    assert "projects → choose a project → loop" in body
    assert "experimental, unattended feature" in body
    assert "**standard** or **campaign**" in body
    assert "at least one is required" in body
    assert "one completed stage/turn" in body
    assert "several jobs while spending one iteration" in body
    assert "does **not** stop the loop automatically" in body
    assert "only when every member fails" in body
    assert "exactly one critic as its own stage" in body
    assert "file no campaign" in body and "falls back" in body
    assert "close the campaign, not the overall project loop" in body
    for outcome in ("ship", "extend", "kill"):
        assert f"**{outcome}**" in body
    assert "**pause** is graceful" in body
    assert "**stop** is permanent" in body
    assert "default group cannot start, pause, resume, stop, or reconfigure" in body
    assert "app guide itself grants no loop tools" in body


def test_project_loop_guide_covers_current_parallel_roles_and_campaign_defaults():
    _, body = _focused_topic("project-loops")

    for role in LOOP_ANALYSIS_ROLES:
        assert f"**{role}**" in body, role
    assert (
        f"up to **{LOOP_CAMPAIGN_DEFAULT_CAPS['max_stages']} stages** by default"
        in body
    )
    assert f"reserves {LOOP_CAMPAIGN_BUDGET_RESERVE} remaining iterations" in body
    assert (
        f"extended {LOOP_CAMPAIGN_DEFAULT_CAPS['max_extensions']} times by default"
        in body
    )
    assert (
        f"{LOOP_CAMPAIGN_DEFAULT_CAPS['abort_failures']} consecutive failed "
        "campaign members abort it" in body
    )


def test_protected_cloud_guide_keeps_current_eligibility_review_and_safety_boundaries():
    _, body = _focused_topic("protected-cloud")

    assert "ordinary writable cloud mount is **live**" in body
    assert "read-only view" in body and "private staging layer" in body
    assert "non-default nextcloud" in body
    assert "uses the **container** workspace" in body
    for unsupported in (
        "personal/default project",
        "opencloud",
        "google drive",
        "microsoft 365",
        "virtual",
        "none",
        "vm",
    ):
        assert unsupported in body
    assert "creation-time choice" in body
    assert "cannot be switched during a session" in body
    assert "cloud changes (n)" in body
    assert "**apply to cloud**" in body and "**reject all**" in body
    assert "whole-diff in v1" in body
    assert "agent cannot apply or reject" in body
    assert "reject is permanent" in body
    assert "fail-closed" in body
    assert "never falls back to a live writable mount" in body
    assert "exact staged epoch" in body
    assert "changed a touched cloud path externally" in body
    assert "partial apply" in body and "safe to repeat" in body
    assert "badge appears only when the staged count is greater than zero" in body
    assert "do not look for a force button" in body


def test_protected_cloud_guide_matches_the_current_mount_selection_rule():
    _, body = _focused_topic("protected-cloud")
    rows = [
        {
            "id": "personal-home",
            "mount_kind": "project_default",
            "backend_id": "nextcloud",
            "cloud_handle": "home",
        },
        {
            "id": "nc-first",
            "mount_kind": "project",
            "backend_id": "nextcloud",
            "cloud_handle": "nc-1",
        },
        {
            "id": "nc-second",
            "mount_kind": "project",
            "backend_id": "nextcloud",
            "cloud_handle": "nc-2",
        },
    ]

    assert select_protected_mount(rows)["id"] == "nc-first"
    assert "first eligible nextcloud project mount" in body


def test_permissions_guide_covers_the_current_grant_catalog():
    _, body = _focused_topic("permissions-and-availability")

    assert CATALOG["permission_mode"]["default"] == "auto_accept"
    for key in CATALOG:
        assert f"`{key}`" in body, key


def test_canvas_and_direct_browser_tool_inventories_have_guide_coverage():
    canvas_coverage = {
        "canvas-and-browser": {
            "get_canvas",
            "set_canvas",
            "clear_canvas",
        }
    }
    browser_coverage = {
        "canvas-and-browser": {
            "browser_navigate",
            "browser_snapshot",
            "browser_click",
            "browser_type",
            "browser_select",
            "browser_scroll",
            "browser_screenshot",
            "browser_back",
            "browser_close",
        }
    }
    renderer_coverage = {
        "canvas-and-browser": {
            "auto",
            "markdown",
            "text",
            "html",
            "html-interactive",
            "image",
            "office",
        }
    }

    canvas_names = set().union(*canvas_coverage.values())
    assert canvas_names == set(SESSION_TOOL_OVERRIDE_NAMES["canvas"])
    assert canvas_names == set(CANVAS_TOOLS_METADATA)
    assert set().union(*browser_coverage.values()) == set(BROWSER_DIRECT_TOOLS_METADATA)
    # The guide may describe a schema-advertised opt-in renderer before every
    # supported deployment has it; every renderer in this build still needs an
    # explicit coverage decision.
    assert set(get_args(CanvasRenderer)) <= set().union(*renderer_coverage.values())


def test_canvas_guide_covers_every_shared_browser_reason_code():
    _, body = _focused_topic("canvas-and-browser")

    for reason in get_args(BrowserCapabilityReason):
        assert f"`{reason}`" in body, reason


def test_every_live_workflow_tool_has_an_explicit_guide_topic():
    coverage = {
        "automations": {
            "list_automations",
            "get_automation",
            "list_automation_runs",
            "propose_automation",
        },
        "project-loops": {
            "get_project_loop",
            "list_project_loop_jobs",
            "explain_project_loop",
        },
    }

    assert set().union(*coverage.values()) == set(
        SESSION_TOOL_OVERRIDE_NAMES["workflows"]
    )


def test_every_selectable_live_fleet_tool_has_an_explicit_guide_topic():
    coverage = {
        "fleet-and-delegation": {
            "get_session_context",
            "create_job",
            "list_jobs",
            "get_job",
            "get_job_file",
            "list_job_files",
            # E4/E5 (officer_supervision_surface): the bounded evidence reads'
            # guide prose lives in fleet-and-delegation ("What Fleet Management
            # can do"), pinned by test_fleet_guide_covers_published_evidence.
            "get_job_completion_report",
            "list_job_evidence",
            "read_job_evidence",
            "approve_job",
            "resume_job_with_feedback",
            "cancel_job",
            "pause_job",
            "get_current_project",
            "list_project_jobs",
            "list_project_repositories",
            "get_default_project_repository",
        }
    }

    assert set().union(*coverage.values()) == set().union(
        SESSION_TOOL_OVERRIDE_NAMES["orchestrator"],
        SESSION_TOOL_OVERRIDE_NAMES["job_control"],
        SESSION_TOOL_OVERRIDE_NAMES["job_inspection"],
    )
