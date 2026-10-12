"""Legacy notification producer manifest — the "one fewer system" gate.

Re-runs ``scripts/check_notification_producers.py`` against the live
orchestrator tree and asserts the result matches the committed manifest at
``policy/notification_producers.txt``. Modelled on ``test_endpoint_inventory``.

The unified notification system (knowledge-base/knowledge/features/
unified_notification_system.md §6) was done when this manifest became empty
(slice 3, 2026-08-26). It must stay empty: the manifest reflects the code,
and the legacy call-site count is zero. Every notification goes through
``notification_service.record()``; a message-ledger ``log_message(direction=
"outbound")`` write is allowed only under a ``# notification-ledger:`` line
saying why it is a ledger row and not a feed write.
"""

import difflib
import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "check_notification_producers.py"
MANIFEST = REPO_ROOT / "policy" / "notification_producers.txt"


def _load_script():
    spec = importlib.util.spec_from_file_location(
        "check_notification_producers", SCRIPT
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_notification_producers"] = module
    spec.loader.exec_module(module)
    return module


def _manifest_sites() -> list[str]:
    return [
        line
        for line in MANIFEST.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]


def test_empty_source_discovery_is_an_error(monkeypatch):
    script = _load_script()
    monkeypatch.setattr(script, "_roots", lambda: [])
    with pytest.raises(RuntimeError, match="No Python sources"):
        script.collect_sites()


def test_application_composition_package_is_scanned(tmp_path, monkeypatch):
    """main.py shrinks to an entrypoint; its code moves into application/."""
    script = _load_script()
    (tmp_path / "application" / "nested").mkdir(parents=True)
    for relative in (
        "main.py",
        "application/__init__.py",
        "application/routes.py",
        "application/nested/lifespan.py",
    ):
        (tmp_path / relative).write_text("")
    monkeypatch.setattr(script, "ORCHESTRATOR", tmp_path)
    assert script._roots() == [
        tmp_path / "main.py",
        tmp_path / "application" / "__init__.py",
        tmp_path / "application" / "nested" / "lifespan.py",
        tmp_path / "application" / "routes.py",
    ]


def test_manifest_matches_code():
    """The committed manifest must reflect the current tree."""
    script = _load_script()
    rendered = script.render_manifest(script.collect_sites())
    on_disk = MANIFEST.read_text()
    if rendered != on_disk:
        diff = "\n".join(
            difflib.unified_diff(
                on_disk.splitlines(),
                rendered.splitlines(),
                fromfile=str(MANIFEST.relative_to(REPO_ROOT)),
                tofile="<regenerated>",
                lineterm="",
            )
        )
        pytest.fail(
            "Legacy notification producer manifest is stale. Run:\n\n"
            "    python scripts/check_notification_producers.py --write\n\n"
            "and commit the result. Diff:\n\n" + diff
        )


def test_no_legacy_call_sites_remain():
    """One fewer system (§6): the legacy paths are gone and stay gone.

    A new caller of a retired path is a regression, not a feature. Record it
    through ``notification_service.record()`` instead — every category,
    action and delivery rule lives in
    ``src/orchestrator/services/notification_catalog.py``. An outbound
    ``log_message`` that really is a message-ledger write (not a feed write)
    is excused by a ``# notification-ledger: <reason>`` line above the call.
    """
    script = _load_script()
    current = script.collect_sites()
    assert current == [], (
        "Legacy notification call sites reappeared: "
        + ", ".join(f"{s.file}:{s.qualname} ({s.kind})" for s in current)
        + ". Use notification_service.record(); do not add callers of "
        "dispatch()/notify_*()/log_message outbound without a ledger note/"
        "the officer digest ring/the quiet-hours queue/thread_notifications."
    )
    assert _manifest_sites() == []


def test_retired_symbols_do_not_exist():
    """The legacy fan-out is deleted, not merely unused."""
    from orchestrator.services import notification_service as svc_mod

    service = svc_mod.NotificationService
    for name in (
        "dispatch",
        "dispatch_digest",
        "notify_review_returned_to_manual",
        "notify_automation_auto_disabled",
        "notify_admins_user_registered",
        "_queue_notification",
        "_broadcast_sse",
    ):
        assert not hasattr(service, name), f"NotificationService.{name} came back"
    from orchestrator.database.postgres import PostgresDB

    for name in (
        "queue_notification",
        "claim_pending_notifications",
        "get_users_exiting_quiet_hours",
        "get_user_notifications",
        "get_unread_count",
        "mark_notification_read",
        "log_project_loop_message_once",
    ):
        assert not hasattr(PostgresDB, name), f"PostgresDB.{name} came back"
