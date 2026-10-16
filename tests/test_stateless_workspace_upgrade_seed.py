"""The stateless claimant's upgrade copy (C1): source, exclusions, Git, marker."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from agent.api import workspace_upgrade_seed as wus
from agent.core.backends.virtual import VirtualWorkspaceBackend
from shared.runtime.core.backends.object_store import InMemoryObjectStore
from tests._fs_backend import FilesystemTestBackend

THREAD_ID = "11111111-2222-4333-8444-555555555555"
SEED_FROM = {"backend": "virtual", "prefix": f"threads/{THREAD_ID}/", "binding": None}
SPEC = {
    "version": 1,
    "seed_from": SEED_FROM,
    "workspace": {
        "backend": "virtual",
        "mounts": [
            {
                "name": "workspace",
                "rclone_spec": {"type": "memory"},
                "prefix": f"threads/{THREAD_ID}/",
            }
        ],
    },
}


@pytest.fixture(autouse=True)
def _isolated_seed_results(monkeypatch):
    """Copies made here never leak a receipt count into another test file."""
    monkeypatch.setattr(wus, "_SEED_RESULTS", {})


@pytest.mark.parametrize(
    "rel,targets,synced",
    [
        ("notes/a.md", [], False),
        ("cloud/a.md", ["cloud"], True),
        ("cloudy/a.md", ["cloud"], False),
        ("projects/x/a.md", ["projects/x"], True),
        ("a.md", [""], True),
        (".git/HEAD", [""], False),
    ],
)
def test_cloud_synced_path(rel, targets, synced):
    assert wus.cloud_synced_path(rel, targets) is synced


class _ShellBackend(FilesystemTestBackend):
    """A container-like test backend; ``shell = False`` makes it a lite one."""

    shell = True
    supports_shell = property(lambda self: self.shell)


def _session(tmp_path, *, git_active=True, commit_ok=True, mounts=()):
    git = MagicMock(is_active=git_active)
    git.commit = MagicMock(return_value=commit_ok)
    git.get_current_commit = MagicMock(return_value="a" * 40)
    backend = _ShellBackend(tmp_path)
    return SimpleNamespace(
        workspace_manager=SimpleNamespace(backend=backend, git_manager=git),
        workspace_sync=(
            SimpleNamespace(mounts=[SimpleNamespace(target_path=t) for t in mounts])
            if mounts
            else None
        ),
        postgres_conn=SimpleNamespace(
            record_turn_commit=AsyncMock(),
            clear_workspace_upgrade_seed=AsyncMock(return_value=True),
        ),
    )


@pytest.fixture
def source(monkeypatch):
    store = InMemoryObjectStore()
    src = VirtualWorkspaceBackend(store, prefix=f"threads/{THREAD_ID}/")
    src.write_file("notes/a.md", "carried")
    src.write_file("cloud/b.md", "cloud copy")
    monkeypatch.setattr(
        "agent.core.backends.factory.create_lite_backend", lambda *_a, **_k: src
    )
    return src


@pytest.mark.asyncio
async def test_seed_copies_commits_records_and_clears_in_order(tmp_path, source):
    session = _session(tmp_path, mounts=["cloud"])

    files = await wus.seed_upgraded_workspace(
        session, SPEC, thread_id=THREAD_ID, lease_token=7
    )

    assert files == 1
    dst = session.workspace_manager.backend
    assert dst.read_file("notes/a.md") == "carried"
    assert not dst.exists("cloud/b.md")
    session.workspace_manager.git_manager.commit.assert_called_once()
    session.postgres_conn.record_turn_commit.assert_awaited_once_with(
        THREAD_ID, "a" * 40
    )
    session.postgres_conn.clear_workspace_upgrade_seed.assert_awaited_once_with(
        THREAD_ID, expected_seed_from=SEED_FROM
    )
    assert wus.take_seed_result(THREAD_ID, 7) == 1
    assert wus.take_seed_result(THREAD_ID, 7) is None


@pytest.mark.asyncio
async def test_the_shared_copy_commits_and_records_but_leaves_the_marker(
    tmp_path, source
):
    """C2's VM recovery copy reuses this step; each caller owns its marker."""
    session = _session(tmp_path)

    files = await wus.copy_lite_workspace_and_commit(
        session,
        SPEC["workspace"],
        thread_id=THREAD_ID,
        exclude=lambda rel: wus.cloud_synced_path(rel, ["cloud"]),
    )

    assert files == 1
    dst = session.workspace_manager.backend
    assert dst.read_file("notes/a.md") == "carried"
    assert not dst.exists("cloud/b.md")
    session.workspace_manager.git_manager.commit.assert_called_once()
    session.postgres_conn.record_turn_commit.assert_awaited_once_with(
        THREAD_ID, "a" * 40
    )
    session.postgres_conn.clear_workspace_upgrade_seed.assert_not_awaited()
    assert wus.take_seed_result(THREAD_ID, 7) is None


@pytest.mark.asyncio
async def test_a_failed_commit_keeps_the_marker(tmp_path, source):
    """/undo after the upgrade must not delete carried files: without a
    recorded post-copy commit the copy is retried, never half-finished."""
    session = _session(tmp_path, commit_ok=False)
    with pytest.raises(wus.WorkspaceUpgradeSeedError):
        await wus.seed_upgraded_workspace(
            session, SPEC, thread_id=THREAD_ID, lease_token=7
        )
    session.postgres_conn.clear_workspace_upgrade_seed.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_none_source_copies_nothing_but_clears(tmp_path):
    session = _session(tmp_path)
    spec = {
        "version": 1,
        "seed_from": {"backend": "none", "prefix": "threads/t/", "binding": None},
        "workspace": {"backend": "none"},
    }
    assert (
        await wus.seed_upgraded_workspace(
            session, spec, thread_id=THREAD_ID, lease_token=3
        )
        == 0
    )
    session.postgres_conn.clear_workspace_upgrade_seed.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spec",
    [
        {"version": 2, "seed_from": SEED_FROM, "workspace": {"backend": "virtual"}},
        {"version": 1, "seed_from": SEED_FROM, "workspace": {"backend": "sandbox"}},
        {"version": 1, "workspace": {"backend": "none"}},
    ],
)
async def test_an_unknown_seed_spec_is_refused(tmp_path, spec):
    with pytest.raises(wus.WorkspaceUpgradeSeedError):
        await wus.seed_upgraded_workspace(
            _session(tmp_path), spec, thread_id=THREAD_ID, lease_token=1
        )


@pytest.mark.asyncio
async def test_a_lite_attach_is_refused(tmp_path, source):
    session = _session(tmp_path)
    session.workspace_manager.backend.shell = False
    with pytest.raises(wus.WorkspaceUpgradeSeedError):
        await wus.seed_upgraded_workspace(
            session, SPEC, thread_id=THREAD_ID, lease_token=1
        )


def test_bundle_key_is_read_beside_attach():
    assert wus.claim_bundle_workspace_upgrade_seed({"attach": {}}) is None
    assert (
        wus.claim_bundle_workspace_upgrade_seed({"workspace_upgrade_seed": SPEC})
        == SPEC
    )


@pytest.mark.asyncio
async def test_claim_bundle_requests_send_the_seed_capability():
    """Review Focus 5: a new executor declares it can seed."""
    import httpx

    from agent.api.orchestrator_client import OrchestratorClient

    seen = []

    def handler(request):
        seen.append(request.url.params.get("capabilities"))
        return httpx.Response(200, json={"attach": {}})

    client = OrchestratorClient("http://test", "127.0.0.1", 8000, "worker", "default")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as transport:
        client._client = transport
        await client.get_claim_bundle("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", 7)
    assert seen == ["workspace_upgrade_seed.v1"]
