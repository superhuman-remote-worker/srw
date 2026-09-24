#!/usr/bin/env python3
"""Inventory every call site of a LEGACY notification path.

The unified notification system (knowledge-base/knowledge/features/
unified_notification_system.md, §6 "one fewer system") retires the paths
below in favour of ``notification_service.record()``. This script enumerates
their remaining call sites so CI can prove the number only ever goes down —
slice 3 drives it to zero and the test then asserts an empty manifest.

Output: one line per site in stable sort order, deliberately WITHOUT line
numbers so an unrelated edit does not churn the manifest::

    <relative file>  <enclosing qualname>  <kind>  #<ordinal within qualname>

Kinds:
  dispatch        notification_service.dispatch(...) / notifier.dispatch(...)
                  — the legacy email+webhook fan-out with no durable row
  notify_freeze   _notify_operator_freeze(...) callers that still ride dispatch
                  (none after slice 1; kept so a regression is visible)
  notify_helper   notify_review_returned_to_manual / notify_automation_auto_disabled
                  / notify_admins_user_registered
  log_outbound    <db>.log_message(..., direction="outbound", ...) — message_log
                  doubling as the feed. A ``# notification-ledger: <reason>``
                  comment on the line above the call marks an audit-ledger write
                  that stays by design and is excluded.
  loop_once       log_project_loop_message_once(...)
  feed_frame      notification_feed.broadcast(...) of a FEED frame type (the
                  session.* / user_registered / reply_delivered frames are not
                  feed items and are ignored)
  digest_ring     merge_thread_officer_state(..., {"digest": ...})
  queue_row       queue_notification(...) — the quiet-hours queue
  headless_email  send_permission_pending_email(...)

The CI snapshot lives at policy/notification_producers.txt; see
tests/test_notification_producer_manifest.py.
"""

from __future__ import annotations

import argparse
import ast
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ORCHESTRATOR = REPO_ROOT / "src" / "orchestrator"
MANIFEST = REPO_ROOT / "policy" / "notification_producers.txt"

FEED_FRAME_TYPES = frozenset(
    {
        "new_message",
        "loop_user_question",
        "loop_campaign_disposition",
        "loop_campaign_review_skipped",
        "loop_cooldown_park",
        "review_returned_to_manual",
        "automation_auto_disabled",
    }
)
NOTIFY_HELPERS = frozenset(
    {
        "notify_review_returned_to_manual",
        "notify_automation_auto_disabled",
        "notify_admins_user_registered",
    }
)
LEDGER_MARKER = "# notification-ledger:"


def _roots() -> list[Path]:
    # main.py is only the entrypoint once the application composition package
    # (orchestrator/application/) owns what used to live in it; scan both.
    roots = [ORCHESTRATOR / "main.py"]
    roots.extend(sorted((ORCHESTRATOR / "application").rglob("*.py")))
    for sub in ("services", "database", "security"):
        roots.extend(sorted((ORCHESTRATOR / sub).glob("*.py")))
    return roots


@dataclass(frozen=True)
class Site:
    file: str
    qualname: str
    kind: str
    ordinal: int

    def render(self) -> str:
        return f"{self.file}  {self.qualname}  {self.kind}  #{self.ordinal}"


def _terminal_name(node: ast.expr | None) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _keyword(call: ast.Call, name: str) -> ast.expr | None:
    for kw in call.keywords:
        if kw.arg == name:
            return kw.value
    return None


def _const(node: ast.expr | None) -> object:
    return node.value if isinstance(node, ast.Constant) else None


def _dict_has_key(node: ast.expr, key: str) -> bool:
    if not isinstance(node, ast.Dict):
        return False
    return any(_const(k) == key for k in node.keys if k is not None)


def classify(call: ast.Call, qualname: str) -> str | None:
    """Return the legacy kind of this call, or None if it is not one."""
    callee = _terminal_name(call.func)
    receiver = (
        _terminal_name(call.func.value)
        if isinstance(call.func, ast.Attribute)
        else None
    )
    bare = isinstance(call.func, ast.Name)

    if callee == "dispatch" and receiver and "notif" in receiver.lower():
        return "dispatch"
    if callee == "_notify_operator_freeze" and bare:
        # Migrated in slice 1: it records. A *new* caller that passes no
        # dedup_key would fail at runtime; the manifest tracks the old shape
        # only, which is the call without the keyword.
        if _keyword(call, "dedup_key") is None:
            return "notify_freeze"
        return None
    if callee in NOTIFY_HELPERS:
        return "notify_helper"
    if callee == "log_message" and _const(_keyword(call, "direction")) == "outbound":
        return "log_outbound"
    if callee == "log_project_loop_message_once":
        return "loop_once"
    if callee == "broadcast" and receiver and "notification_feed" in receiver:
        event_type = _keyword(call, "event_type")
        value = _const(event_type)
        if value in FEED_FRAME_TYPES:
            return "feed_frame"
        # The project-loop notifier passes its frame type as a variable; it
        # is the one dynamic feed broadcast (main.py::_notify_loop_event).
        if value is None and event_type is not None and "loop" in qualname:
            return "feed_frame"
        return None
    if callee == "merge_thread_officer_state":
        for arg in list(call.args) + [kw.value for kw in call.keywords]:
            if _dict_has_key(arg, "digest"):
                return "digest_ring"
        return None
    if callee == "queue_notification":
        return "queue_row"
    if callee == "send_permission_pending_email":
        return "headless_email"
    return None


class _Visitor(ast.NodeVisitor):
    def __init__(self, rel_path: str, source_lines: list[str]) -> None:
        self.rel_path = rel_path
        self.source_lines = source_lines
        self.stack: list[str] = []
        self.sites: list[tuple[str, str]] = []

    def _qualname(self) -> str:
        return ".".join(self.stack) or "<module>"

    def _enter(self, node: ast.AST) -> None:
        self.stack.append(getattr(node, "name", "?"))
        self.generic_visit(node)
        self.stack.pop()

    visit_FunctionDef = _enter
    visit_AsyncFunctionDef = _enter
    visit_ClassDef = _enter

    def _ledger_annotated(self, call: ast.Call) -> bool:
        idx = call.lineno - 2  # the line above the call, 0-based
        while idx >= 0:
            line = self.source_lines[idx].strip()
            if not line:
                idx -= 1
                continue
            if not line.startswith("#"):
                return False
            if line.startswith(LEDGER_MARKER):
                return True
            idx -= 1
        return False

    def visit_Call(self, node: ast.Call) -> None:
        kind = classify(node, self._qualname())
        if kind and not (kind == "log_outbound" and self._ledger_annotated(node)):
            self.sites.append((self._qualname(), kind))
        self.generic_visit(node)


def collect_sites() -> list[Site]:
    sites: list[Site] = []
    paths = _roots()
    if not paths:
        raise RuntimeError("No Python sources discovered for the policy inventory")
    for path in paths:
        source = path.read_text()
        tree = ast.parse(source, filename=str(path))
        visitor = _Visitor(str(path.relative_to(REPO_ROOT)), source.splitlines())
        visitor.visit(tree)
        counters: dict[tuple[str, str], int] = {}
        for qualname, kind in visitor.sites:
            key = (qualname, kind)
            counters[key] = counters.get(key, 0) + 1
            sites.append(Site(visitor.rel_path, qualname, kind, counters[key]))
    sites.sort(key=lambda s: (s.file, s.qualname, s.kind, s.ordinal))
    return sites


def render_manifest(sites: list[Site]) -> str:
    header = (
        "# legacy notification producers — generated by scripts/check_notification_producers.py\n"
        "# DO NOT EDIT BY HAND. Regenerate with `python scripts/check_notification_producers.py --write`.\n"
        "#\n"
        "# Every line is one call site of a path the unified notification system retires\n"
        "# (knowledge-base/knowledge/features/unified_notification_system.md §6). The CI test\n"
        "# fails if this list grows; slice 3 drives it to zero and then asserts it stays empty.\n"
        "#\n"
        "# <file>  <enclosing qualname>  <kind>  #<ordinal>\n"
    )
    body = "\n".join(s.render() for s in sites)
    return header + ("\n" + body + "\n" if body else "")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="Write the manifest")
    parser.add_argument(
        "--check", action="store_true", help="Exit non-zero if the manifest is stale"
    )
    args = parser.parse_args()

    sites = collect_sites()
    rendered = render_manifest(sites)

    if args.write:
        MANIFEST.parent.mkdir(parents=True, exist_ok=True)
        MANIFEST.write_text(rendered)
        print(f"wrote {len(sites)} legacy sites to {MANIFEST.relative_to(REPO_ROOT)}")
        return 0
    if args.check:
        if not MANIFEST.exists():
            print(f"ERROR: {MANIFEST} missing — run with --write", file=sys.stderr)
            return 2
        if MANIFEST.read_text() != rendered:
            print(
                f"ERROR: {MANIFEST.relative_to(REPO_ROOT)} is stale.\n"
                "Run `python scripts/check_notification_producers.py --write` and commit.",
                file=sys.stderr,
            )
            return 1
        print(f"OK: {len(sites)} legacy notification sites, manifest fresh.")
        return 0
    sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    sys.exit(main())
