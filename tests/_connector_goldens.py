"""Golden-file plumbing and canonical rows for the connector behaviour pins.

Slice D1a of ``connector_drivers.md`` moves every connector type out of
type-branching code into one driver per type. Step 0 pins what the 13 types
do today, so each move is a refactor the goldens can prove, not a rewrite they
have to trust. The pins live in ``tests/fixtures/connector_goldens/*.json``,
one file per concern, written by the ``tests/test_connector_goldens_*.py``
modules:

* ``api.json`` — create, update and delete: status, response body and every
  store call with its normalized arguments, including each 400/403/409 detail;
* ``probe.json`` — ``POST /api/datasources/{id}/test`` for success and each
  failure class, with the driver/library seam mocked;
* ``payload.json`` — the datasources payload the agent receives, compared
  byte for byte as compact JSON (key order included);
* ``tools.json`` — tool categories for mixed connector sets, plus the type
  inventory each surface hardcodes;
* ``facts.json`` — the README.md connector lines the agent writes and the
  project KB note the orchestrator projects for each connector.

**They pin today's behaviour, defects included.** A case that pins a known
defect carries ``pinned_defect`` (in the case definition and so in the
golden). The commit that fixes it changes that golden entry on purpose.

**Regenerate** after a deliberate behaviour change, then review the fixture
diff::

    UPDATE_CONNECTOR_GOLDENS=1 python -m pytest tests/test_connector_goldens_*.py

A regeneration run rewrites only the entries of the cases that ran, keeps the
rest, and orders the file by each module's case list, so ``-k`` is safe. It
must run serially: xdist workers would each write a partial file.

**Normalisation.** The fake stores return fixed ids and timestamps, so only
one thing varies between runs: the 12-hex ``error_ref`` a failed probe or a
500 carries. :func:`normalise` replaces it with ``<error_ref>`` after checking
its shape.
"""

from __future__ import annotations

import copy
import difflib
import json
import os
import re
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

GOLDEN_DIR = Path(__file__).parent / "fixtures" / "connector_goldens"
UPDATE = os.environ.get("UPDATE_CONNECTOR_GOLDENS") == "1"
REGENERATE_HINT = (
    "If the change is deliberate, regenerate with "
    "UPDATE_CONNECTOR_GOLDENS=1 python -m pytest tests/test_connector_goldens_*.py "
    "and let the fixture diff carry the intent in review."
)

USER_ID = "00000000-0000-0000-0000-0000000000c1"
USER = {"id": USER_ID, "is_admin": False}
PROJECT_ID = "00000000-0000-0000-0000-0000000000b1"
OTHER_PROJECT_ID = "00000000-0000-0000-0000-0000000000b2"
DATASOURCE_ID = "00000000-0000-0000-0000-0000000000d1"
FIXED_TS = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

#: A real, unencrypted Ed25519 key (private bytes 00..1f, check ints fixed by
#: pasting it once). C1 parses every SSH key to fingerprint it for the
#: workspace ssh-agent and refuses one it cannot parse, so a key that is only
#: structurally valid no longer stands in for a usable one.
SSH_PRIVATE_KEY = (
    "-----BEGIN OPENSSH PRIVATE KEY-----\n"
    "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtz\n"
    "c2gtZWQyNTUxOQAAACADoQe/884Qvh1w3RjnS8CZZ+TWMJulDV8d3IZkElUxuAAA\n"
    "AIjyQbTU8kG01AAAAAtzc2gtZWQyNTUxOQAAACADoQe/884Qvh1w3RjnS8CZZ+TW\n"
    "MJulDV8d3IZkElUxuAAAAEAAAQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0e\n"
    "HwOhB7/zzhC+HXDdGOdLwJln5NYwm6UNXx3chmQSVTG4AAAAAAECAwQF\n"
    "-----END OPENSSH PRIVATE KEY-----\n"
)
SSH_PUBLIC_KEY = (
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAOhB7/zzhC+HXDdGOdLwJln5NYwm6UNXx3chmQSVTG4 "
    "golden@test\n"
)

_ERROR_REF = re.compile(r"^[0-9a-f]{12}$")
_ERROR_REF_IN_TEXT = re.compile(r"error_ref=([0-9a-f]{12})")


def normalise(value: Any) -> Any:
    """JSON-round-trip ``value`` and replace each ``error_ref`` with a marker.

    The round trip turns tuples into lists and datetimes into ISO strings
    (``default=str``), which is what the wire carries anyway.
    """
    plain = json.loads(json.dumps(value, default=str))
    return _replace_error_refs(plain)


def _replace_error_refs(value: Any) -> Any:
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            if key == "error_ref" and isinstance(item, str):
                assert _ERROR_REF.match(item), f"unexpected error_ref shape {item!r}"
                out[key] = "<error_ref>"
            else:
                out[key] = _replace_error_refs(item)
        return out
    if isinstance(value, list):
        return [_replace_error_refs(item) for item in value]
    if isinstance(value, str):
        return _ERROR_REF_IN_TEXT.sub("error_ref=<error_ref>", value)
    return value


def _pretty(value: Any, *, sort_keys: bool) -> list[str]:
    return json.dumps(
        value, indent=2, sort_keys=sort_keys, ensure_ascii=False
    ).splitlines()


def wire_bytes(value: Any) -> str:
    """Compact JSON with insertion-ordered keys: the bytes the agent receives."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


class Golden:
    """One golden file, keyed by case id, ordered by the owning module's cases."""

    def __init__(self, name: str, case_ids: Iterable[str]) -> None:
        if UPDATE and os.environ.get("PYTEST_XDIST_WORKER"):
            pytest.fail("Regenerate connector goldens serially (without -n).")
        self.name = name
        self.path = GOLDEN_DIR / f"{name}.json"
        self.case_ids = list(case_ids)
        self.recorded: dict[str, Any] = {}
        self.cases: dict[str, Any] = (
            json.loads(self.path.read_text()) if self.path.exists() else {}
        )

    def check(self, case_id: str, actual: Any, *, exact_bytes: bool = False) -> None:
        """Compare ``actual`` with the golden entry (or record it when updating).

        ``exact_bytes`` compares the compact JSON serialisation, so key order
        is part of the contract; otherwise the comparison is structural.
        """
        actual = normalise(actual)
        if UPDATE:
            self.recorded[case_id] = actual
            return
        if case_id not in self.cases:
            pytest.fail(
                f"{self.path.name} has no entry for {case_id!r}. {REGENERATE_HINT}"
            )
        expected = self.cases[case_id]
        if exact_bytes:
            if wire_bytes(actual) == wire_bytes(expected):
                return
        elif actual == expected:
            return
        diff = "\n".join(
            difflib.unified_diff(
                _pretty(expected, sort_keys=not exact_bytes),
                _pretty(actual, sort_keys=not exact_bytes),
                fromfile=f"golden {self.name}/{case_id}",
                tofile="actual",
                lineterm="",
            )
        )
        pytest.fail(
            f"Connector behaviour changed for {self.name}/{case_id}.\n{diff}\n"
            f"{REGENERATE_HINT}"
        )

    def assert_covers_cases(self) -> None:
        """A golden entry per case, and no entry for a case that is gone."""
        if UPDATE:
            return
        missing = [case for case in self.case_ids if case not in self.cases]
        stale = [case for case in self.cases if case not in self.case_ids]
        assert not missing and not stale, (
            f"{self.path.name} drifted from its cases: missing={missing} "
            f"stale={stale}. {REGENERATE_HINT}"
        )

    def flush(self) -> None:
        """Write recorded entries, keeping unrecorded ones, in case order."""
        if not UPDATE or not self.recorded:
            return
        merged = {
            case: self.recorded[case] if case in self.recorded else self.cases[case]
            for case in self.case_ids
            if case in self.recorded or case in self.cases
        }
        GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(merged, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )


# =============================================================================
# Canonical resolved rows
# =============================================================================
#
# One row per connector kind, shaped like ``resolve_datasources_for_job``
# returns it (decrypted credentials, parsed config, ``project_read_only``
# from the project link). Kinds split two types by variant: ``repository``
# (token, SSH key), ``mcp`` (remote, stdio) and ``kb`` (external, a
# project's native KB). Every secret is a fake.

_ROW_IDS = {
    "generic": "d5000001-0000-0000-0000-000000000001",
    "credentials": "d5000002-0000-0000-0000-000000000002",
    "generic_file": "d5000003-0000-0000-0000-000000000003",
    "kubeconfig": "d5000004-0000-0000-0000-000000000004",
    "ssh_key": "d5000005-0000-0000-0000-000000000005",
    "repository_token": "d5000006-0000-0000-0000-000000000006",
    "repository_ssh": "d5000007-0000-0000-0000-000000000007",
    "postgresql": "d5000008-0000-0000-0000-000000000008",
    "neo4j": "d5000009-0000-0000-0000-000000000009",
    "mongodb": "d500000a-0000-0000-0000-00000000000a",
    "webdav": "d500000b-0000-0000-0000-00000000000b",
    "email": "d500000c-0000-0000-0000-00000000000c",
    "kb": "d500000d-0000-0000-0000-00000000000d",
    "kb_native": "d500000e-0000-0000-0000-00000000000e",
    "mcp_remote": "d500000f-0000-0000-0000-00000000000f",
    "mcp_stdio": "d5000010-0000-0000-0000-000000000010",
}

KUBECONFIG_YAML = (
    "apiVersion: v1\n"
    "kind: Config\n"
    "clusters:\n"
    "- name: staging\n"
    "  cluster: {server: 'https://k8s.example.test:6443'}\n"
    "contexts:\n"
    "- name: staging\n"
    "  context: {cluster: staging, user: ci}\n"
    "current-context: staging\n"
    "users:\n"
    "- name: ci\n"
    "  user: {token: kube-secret}\n"
)

_KIND_FIELDS: dict[str, dict[str, Any]] = {
    "generic": {
        "name": "Billing API",
        "description": "Billing REST API",
        "type": "generic",
        "connection_url": "https://billing.example.test/api",
        "credentials": {"env_vars": {"BILLING_TOKEN": "billing-secret"}},
        "config": {},
        "cli_hint": 'curl -H "Authorization: Bearer $BILLING_TOKEN" $BILLING_URL',
    },
    "credentials": {
        "name": "Vendor login",
        "description": "Vendor portal account",
        "type": "credentials",
        "connection_url": None,
        "credentials": {
            "env_vars": {"VENDOR_USER": "alice", "VENDOR_PASSWORD": "vendor-secret"}
        },
        "config": {},
    },
    "generic_file": {
        "name": "Service account",
        "description": "GCP service account",
        "type": "generic_file",
        "connection_url": None,
        "credentials": {
            "files": [
                {
                    "name": "sa.json",
                    "contents": '{"type": "service_account"}',
                    "target_path": "/home/srw/.config/gcloud/sa.json",
                    "mode": "0600",
                    "env_var": "GOOGLE_APPLICATION_CREDENTIALS",
                },
                {
                    "name": "ca.pem",
                    "contents": "-----BEGIN CERTIFICATE-----\nZ29sZGVu\n-----END CERTIFICATE-----\n",
                    "target_path": "/home/srw/.config/gcloud/ca.pem",
                    "mode": "0644",
                },
            ]
        },
        "config": {},
    },
    "kubeconfig": {
        "name": "Staging Cluster",
        "description": "Staging Kubernetes",
        "type": "kubeconfig",
        "connection_url": None,
        "credentials": {
            "files": [
                {
                    "name": "staging-cluster.yaml",
                    "contents": KUBECONFIG_YAML,
                    "target_path": "/home/srw/.kube/configs/staging-cluster.yaml",
                    "mode": "0600",
                }
            ]
        },
        "config": {},
    },
    "ssh_key": {
        "name": "Deploy Key",
        "description": "Bastion access",
        "type": "ssh_key",
        "connection_url": None,
        "credentials": {
            "files": [
                {
                    "name": "deploy-key",
                    "contents": SSH_PRIVATE_KEY,
                    "target_path": "/home/srw/.ssh/deploy-key",
                    "mode": "0600",
                },
                {
                    "name": "deploy-key.pub",
                    "contents": SSH_PUBLIC_KEY,
                    "target_path": "/home/srw/.ssh/deploy-key.pub",
                    "mode": "0644",
                },
            ]
        },
        "config": {},
    },
    "repository_token": {
        "name": "Widgets",
        "description": "Widgets service",
        "type": "repository",
        "connection_url": "https://github.com/acme/widgets.git",
        "credentials": {"auth_method": "token", "token": "ghp_widgets-secret"},
        "config": {"forge": "github"},
        "default_branch": "main",
    },
    "repository_ssh": {
        "name": "Gadgets",
        "description": "Gadgets service",
        "type": "repository",
        "connection_url": "ssh://git@git.example.test:2222/acme/gadgets.git",
        "credentials": {"auth_method": "ssh", "ssh_key": SSH_PRIVATE_KEY},
        "config": {"forge": "gitea"},
        "default_branch": "develop",
    },
    "postgresql": {
        "name": "Orders DB",
        "description": "Order history",
        "type": "postgresql",
        "connection_url": "postgresql://orders:pg-secret@db.example.test:5432/orders",
        "credentials": {},
        "config": {},
    },
    "neo4j": {
        "name": "Supply Graph",
        "description": "Supplier graph",
        "type": "neo4j",
        "connection_url": "bolt://neo4j.example.test:7687",
        "credentials": {"username": "graph", "password": "neo-secret"},
        "config": {},
    },
    "mongodb": {
        "name": "Events",
        "description": "Event store",
        "type": "mongodb",
        "connection_url": "mongodb://events:mongo-secret@mongo.example.test:27017/events",
        "credentials": {},
        "config": {},
    },
    "webdav": {
        "name": "Team files",
        "description": "Shared drive",
        "type": "webdav",
        "connection_url": "https://cloud.example.test/remote.php/dav/files/alice",
        "credentials": {"username": "alice", "password": "dav-secret"},
        "config": {},
    },
    "email": {
        "name": "Support inbox",
        "description": "Customer support mailbox",
        "type": "email",
        "connection_url": None,
        "credentials": {
            "backend": "imap_smtp",
            "username": "support@example.test",
            "password": "mail-secret",
            "imap": {"host": "imap.example.test", "port": 993, "security": "ssl"},
            "smtp": {"host": "smtp.example.test", "port": 465, "security": "ssl"},
        },
        "config": {
            "access": "draft",
            "folders": ["INBOX", "Support"],
            "drafts_folder": "Drafts",
            "from_address": "support@example.test",
            "recipient_allowlist": [],
            "unattended_send": False,
        },
    },
    "kb": {
        "name": "Handbook",
        "description": "Company handbook",
        "type": "kb",
        "connection_url": "https://git.example.test/acme/handbook.git",
        "credentials": {"auth_method": "token", "token": "kb-secret"},
        "config": {"root_path": "vault", "forge": "gitea"},
        "default_branch": "main",
    },
    "kb_native": {
        "name": "Project knowledge",
        "description": "This project's own knowledge base",
        "type": "kb",
        "connection_url": None,
        "credentials": {},
        "config": {"root_path": "", "native_project_id": PROJECT_ID},
    },
    "mcp_remote": {
        "name": "Docs MCP",
        "description": "Documentation search",
        "type": "mcp",
        "connection_url": "https://mcp.example.test/mcp",
        "credentials": {
            "transport": "http",
            "auth": {"type": "bearer", "token": "mcp-secret"},
        },
        "config": {},
    },
    "mcp_stdio": {
        "name": "Local MCP",
        "description": "Local tool server",
        "type": "mcp",
        "connection_url": None,
        "credentials": {
            "transport": "stdio",
            "command": "npx",
            "args": ["-y", "@acme/mcp"],
            "env": {"ACME_KEY": "stdio-secret"},
        },
        "config": {},
    },
}

#: Every connector kind, in catalog order with variants next to their type.
KINDS: tuple[str, ...] = tuple(_KIND_FIELDS)


def resolved_row(kind: str, *, project_read_only: Any = False, **over: Any) -> dict:
    """A deep-copied resolved row for ``kind``; ``over`` replaces fields."""
    row: dict[str, Any] = {
        "id": _ROW_IDS[kind],
        "name": None,
        "description": None,
        "type": None,
        "connection_url": None,
        "credentials": {},
        "config": {},
        "cli_hint": None,
        "default_branch": None,
        "read_only": None,
        "created_by": USER_ID,
        "is_global": False,
        "scope_mode": "all",
        "auto_attach": False,
        "policy_revision": 1,
        "created_at": FIXED_TS,
        "updated_at": FIXED_TS,
        "project_read_only": project_read_only,
    }
    row.update(copy.deepcopy(_KIND_FIELDS[kind]))
    row.update(copy.deepcopy(over))
    return row


def all_rows(*, project_read_only: Any = False) -> list[dict]:
    """One resolved row per kind, in ``KINDS`` order."""
    return [resolved_row(kind, project_read_only=project_read_only) for kind in KINDS]
